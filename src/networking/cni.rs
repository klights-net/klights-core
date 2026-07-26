//! In-process CNI bridge networking for klights pods.
//!
//! Implements the daemon side of the klights CNI flow.
//! containerd executes the `klights-cni` shim from the klights-managed CNI bin
//! directory, and the shim forwards ADD/DEL over Unix-socket RPC to this
//! in-process rtnetlink implementation.
//!
//! Public API:
//! - [`add`] — wire up a new pod sandbox (create veth, assign IP, configure pod netns)
//! - [`del`] — tear down a pod sandbox (delete veth, release IP record)
use anyhow::{Context, Result};
use async_trait::async_trait;
use futures::stream::TryStreamExt;
use netlink_packet_route::link::InfoData;
use netlink_packet_route::link::InfoKind;
use netlink_packet_route::link::InfoVeth;
use netlink_packet_route::link::{LinkAttribute, LinkFlag, LinkMessage};
use netlink_packet_route::neighbour::{NeighbourAddress, NeighbourAttribute, NeighbourMessage};
use netlink_packet_route::{AddressFamily, route::RouteType};
use nix::sched::{CloneFlags, setns};
use std::collections::HashSet;
use std::net::Ipv4Addr;
use std::os::fd::AsFd;
use std::os::unix::io::AsRawFd;
use std::str::FromStr;
use std::sync::{Arc, Mutex, Weak};

use klights_network_api::{PodNetwork, PodNetworkAssignmentKey, PodNetworkAssignmentPublisher};
use klights_node_store::{
    CacheNetworkError, PodIpamStore, PodNetworkAllocationRequest, PodNetworkCache, PodRuntimeStore,
    SandboxKey,
};

use super::types::BridgeName;
use klights_types::{NodeName, PodSubnet};

#[derive(Default)]
pub(crate) struct SandboxOperationLocks {
    gates: Arc<Mutex<std::collections::HashMap<String, Weak<tokio::sync::Mutex<()>>>>>,
}

pub(crate) struct SandboxOperationGuard {
    sandbox_id: String,
    gates: Arc<Mutex<std::collections::HashMap<String, Weak<tokio::sync::Mutex<()>>>>>,
    _gate: tokio::sync::OwnedMutexGuard<()>,
}

impl SandboxOperationLocks {
    pub(crate) async fn acquire(&self, sandbox_id: &str) -> SandboxOperationGuard {
        let gate = {
            let mut gates = self.gates.lock().unwrap();
            gates.retain(|_, gate| gate.strong_count() > 0);
            if let Some(gate) = gates.get(sandbox_id).and_then(Weak::upgrade) {
                gate
            } else {
                let gate = Arc::new(tokio::sync::Mutex::new(()));
                gates.insert(sandbox_id.to_string(), Arc::downgrade(&gate));
                gate
            }
        };
        let guard = gate.lock_owned().await;
        SandboxOperationGuard {
            sandbox_id: sandbox_id.to_string(),
            gates: self.gates.clone(),
            _gate: guard,
        }
    }

    #[cfg(test)]
    fn retained_key_count(&self) -> usize {
        self.gates.lock().unwrap().len()
    }
}

impl Drop for SandboxOperationGuard {
    fn drop(&mut self) {
        let mut gates = self.gates.lock().unwrap();
        if gates
            .get(&self.sandbox_id)
            .is_some_and(|gate| gate.strong_count() == 1)
        {
            gates.remove(&self.sandbox_id);
        }
    }
}

enum ExistingAllocation {
    Valid { ip: Ipv4Addr },
    Stale { reason: String },
}

#[async_trait]
trait NetnsInspector: Send + Sync {
    async fn inspect(
        &self,
        task_supervisor: &klights_supervisor::TaskSupervisor,
        netns_setns_path: &str,
        pod_ip: Ipv4Addr,
        prefix: u8,
        gateway: Ipv4Addr,
    ) -> Result<()>;
}

struct RealNetnsInspector;

#[async_trait]
impl NetnsInspector for RealNetnsInspector {
    async fn inspect(
        &self,
        task_supervisor: &klights_supervisor::TaskSupervisor,
        netns_setns_path: &str,
        pod_ip: Ipv4Addr,
        prefix: u8,
        gateway: Ipv4Addr,
    ) -> Result<()> {
        let netns_path = netns_setns_path.to_string();
        task_supervisor
            .run_blocking(
                klights_supervisor::TaskCategory::Network,
                "cni_validate_pod_netns_state",
                move || validate_pod_netns_state(&netns_path, pod_ip, prefix, gateway),
            )
            .await
            .context("blocking pod netns validation failed")?
    }
}

/// Set up in-process bridge networking for a new pod sandbox.
///
/// Steps:
/// 1. Ensure the klights bridge exists with the right IP and MTU.
/// 2. Allocate a pod IP from the IPAM (SQLite-backed MAX+1 counter).
/// 3. Create a veth pair; attach the host side to the bridge.
/// 4. Move the pod side into the sandbox netns.
/// 5. Inside the pod netns: rename to `eth0`, assign IP, add default route, bring up lo.
/// 6. Record the allocation in `pod_networks`.
///
/// For host-network pods, returns immediately with the host IP (no veth, no allocation).
pub struct CniAddArgs<'a> {
    pub cache: &'a dyn PodNetworkCache,
    pub ipam: &'a dyn PodIpamStore,
    pub runtime: &'a dyn PodRuntimeStore,
    pub assignment_publisher: &'a dyn PodNetworkAssignmentPublisher,
    pub handle: &'a rtnetlink::Handle,
    pub sandbox_id: &'a str,
    pub pod: klights_types::PodIdentity,
    pub bridge_name: &'a BridgeName,
    pub bridge_idx: u32,
    pub netns_setns_path: &'a str,
    pub netns_record_path: &'a str,
    pub pod_subnet: &'a PodSubnet,
    pub pod_link_mtu: u32,
    pub host_network: bool,
    pub host_ip: &'a str,
    pub _node_name: &'a NodeName,
    pub task_supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
}

pub async fn add(args: CniAddArgs<'_>) -> Result<PodNetwork> {
    let CniAddArgs {
        cache,
        ipam,
        runtime,
        assignment_publisher,
        handle,
        sandbox_id,
        pod,
        bridge_name,
        bridge_idx,
        netns_setns_path,
        netns_record_path,
        pod_subnet,
        pod_link_mtu,
        host_network,
        host_ip,
        _node_name,
        task_supervisor,
    } = args;
    let file_process = klights_supervisor::FileProcessExecutor::new(task_supervisor.clone());
    let namespace = pod.namespace.as_str();
    let pod_name = pod.name.as_str();
    let pod_uid = pod.uid.as_str();
    if host_network {
        return Ok(PodNetwork::new(std::net::IpAddr::V4(
            Ipv4Addr::from_str(host_ip).unwrap_or(Ipv4Addr::UNSPECIFIED),
        )));
    }
    let assignment_key = PodNetworkAssignmentKey::try_new(sandbox_id, namespace, pod_name, pod_uid)
        .map_err(anyhow::Error::new)?;

    let bridge_name = bridge_name.as_str();

    // Subnet parameters from the typed primitive — no string re-parsing.
    let subnet_base = pod_subnet.base();
    let prefix_len = pod_subnet.prefix();
    let bridge_ip = pod_subnet.bridge_ip();
    let subnet_size = pod_subnet.size();
    let uid_hex = pod_uid.replace('-', "");
    let uid_chars = &uid_hex[..uid_hex.len().min(11)];
    let veth_host = format!("veth{}", uid_chars);
    let veth_pod_temp = format!("vpod{}", uid_chars);
    let allocation_request = || {
        PodNetworkAllocationRequest::try_new(
            sandbox_id,
            pod.clone(),
            subnet_base,
            subnet_size,
            &veth_host,
            netns_record_path,
        )
    };

    if let Some(endpoint) = cache
        .get_network_for_sandbox(SandboxKey::try_new(sandbox_id)?)
        .await
        .context("Failed to check existing pod network allocation")?
    {
        let validated = ipam
            .reserve_ip_and_insert_network(allocation_request()?)
            .await
            .context("existing pod network allocation identity validation failed")?;
        if validated.ip_addr() != endpoint.ip_addr() {
            anyhow::bail!(
                "existing allocation identity validation returned IP {} instead of {}",
                validated.ip_addr(),
                endpoint.ip_addr()
            );
        }
        let pod_ip = Ipv4Addr::from_str(endpoint.ip_addr()).context("Invalid recorded pod IP")?;
        let allocation = validate_existing_allocation(ValidateExistingAllocationArgs {
            handle,
            bridge_name,
            recorded_netns_path: endpoint.netns_path(),
            current_netns_record_path: netns_record_path,
            veth_host: endpoint.veth_host(),
            pod_ip,
            prefix: prefix_len,
            gateway: bridge_ip,
            netns_setns_path,
            inspector: &RealNetnsInspector,
            task_supervisor: task_supervisor.as_ref(),
            file_process: &file_process,
        })
        .await
        .with_context(|| format!("Failed to validate existing allocation for {}", sandbox_id))?;

        match allocation {
            ExistingAllocation::Valid { ip } => {
                tracing::debug!(
                    "cni::add {}: reusing existing allocation ip={} veth_host={}",
                    sandbox_id,
                    ip,
                    endpoint.veth_host()
                );
                assignment_publisher.publish_assignment(&assignment_key);
                return Ok(PodNetwork::new(std::net::IpAddr::V4(ip)));
            }
            ExistingAllocation::Stale { reason } => {
                tracing::warn!(
                    "cni::add {}: existing allocation is stale ({}), rebuilding",
                    sandbox_id,
                    reason
                );
                remove_stale_allocation(cache, handle, allocation_request()?, endpoint.veth_host())
                    .await?;
            }
        }
    }

    // Reserve before any veth/netns mutation. A concurrent conflicting ADD
    // loses at the durable identity CAS and cannot disturb the winner.
    let (ip_addr_str, _ip_int) = allocate_ip_with_reclaim(
        cache,
        ipam,
        runtime,
        sandbox_id,
        &pod,
        subnet_base,
        subnet_size,
        &veth_host,
        netns_record_path,
    )
    .await
    .context("Atomic IPAM allocation failed")?;
    let pod_ip = Ipv4Addr::from_str(&ip_addr_str).context("Invalid allocated IP")?;

    let setup_result: Result<(String, Ipv4Addr)> = async {
        if let Ok(existing) = crate::networking::get_link_index(handle, &veth_host).await {
            handle
                .link()
                .del(existing)
                .execute()
                .await
                .with_context(|| format!("Failed to delete stale veth {}", veth_host))?;
        }

        // Open the target netns fd before creating the veth pair.
        // In rootless mode (user namespace), rtnetlink RTM_SETLINK +
        // IFLA_NET_NS_FD fails with EPERM. Creating the peer directly in the
        // target netns works reliably.
        let netns_open_key = netns_setns_path.to_string();
        let netns_path_for_open = netns_setns_path.to_string();
        let netns_file = file_process
            .run_blocking_file_keyed("cni_open_sandbox_netns", netns_open_key, move || {
                open_netns_file_blocking(netns_path_for_open)
            })
            .await
            .with_context(|| format!("Failed to open sandbox netns {}", netns_setns_path))?;
        let netns_fd_raw = netns_file.as_raw_fd();

        create_veth_pair_with_peer_in_netns(handle, &veth_host, &veth_pod_temp, netns_fd_raw)
            .await
            .with_context(|| {
                format!(
                    "Failed to create veth pair {}/{} in target netns",
                    veth_host, veth_pod_temp
                )
            })?;
        drop(netns_file);

        flush_host_neighbour(handle, bridge_idx, pod_ip, "add-before-reuse").await;

        // Get host-side interface index (peer is already in the pod netns)
        let veth_host_idx = crate::networking::get_link_index(handle, &veth_host)
            .await
            .with_context(|| format!("veth_host {} not found after creation", veth_host))?;

        // Set host veth MTU, attach to bridge, bring up
        handle
            .link()
            .set(veth_host_idx)
            .mtu(pod_link_mtu)
            .execute()
            .await
            .context("Failed to set veth_host MTU")?;
        handle
            .link()
            .set(veth_host_idx)
            .controller(bridge_idx)
            .execute()
            .await
            .context("Failed to attach veth_host to bridge")?;

        // Enable hairpin_mode on the bridge port so pod-to-self via ClusterIP
        // works. Without this the bridge drops frames that DNAT sends back out
        // the same port they arrived on (hairpin forwarding).
        let hairpin_path = format!("/sys/class/net/{}/brport/hairpin_mode", veth_host);
        if let Err(e) = crate::utils::write_file_async(&file_process, &hairpin_path, b"1").await {
            tracing::warn!(
                "Failed to set hairpin_mode on {}: {} (continuing anyway)",
                veth_host,
                e
            );
        }

        handle
            .link()
            .set(veth_host_idx)
            .up()
            .execute()
            .await
            .context("Failed to bring up veth_host")?;

        // Configure pod netns in a blocking thread (setns is per-thread)
        let netns_path_owned = netns_setns_path.to_string();
        let veth_pod_temp_owned = veth_pod_temp.clone();
        task_supervisor
            .run_blocking(
                klights_supervisor::TaskCategory::Network,
                "cni_configure_pod_netns",
                move || {
                    configure_pod_netns(
                        &netns_path_owned,
                        &veth_pod_temp_owned,
                        pod_ip,
                        prefix_len,
                        bridge_ip,
                        pod_link_mtu,
                    )
                },
            )
            .await
            .context("blocking pod netns configuration failed")?
            .context("Failed to configure pod netns")?;

        Ok((ip_addr_str, pod_ip))
    }
    .await;

    let completed_setup = match setup_result {
        Ok(result) => Ok(result),
        Err(e) => {
            cleanup_host_veth(handle, &veth_host).await;
            let _ = cache.delete_network_if_matches(allocation_request()?).await;
            Err(e)
        }
    };
    let (ip_addr_str, pod_ip) =
        complete_persisted_assignment(completed_setup, assignment_publisher, &assignment_key)?;

    tracing::info!(
        "cni::add {}/{}: ip={} veth_host={}",
        namespace,
        pod_name,
        ip_addr_str,
        veth_host
    );

    Ok(PodNetwork::new(std::net::IpAddr::V4(pod_ip)))
}

/// Tear down the pod network allocation for a sandbox.
///
/// Deletes the host-side veth (kernel auto-removes the pod side) and removes
/// the `pod_networks` record. Idempotent: missing veth or record is a warning.
pub async fn del(
    cache: &dyn PodNetworkCache,
    handle: &rtnetlink::Handle,
    sandbox_id: &str,
    bridge_idx: u32,
) -> Result<()> {
    let record = cache
        .get_network_for_sandbox(SandboxKey::try_new(sandbox_id)?)
        .await
        .context("Failed to look up pod_networks")?;

    let endpoint = match record {
        Some(r) => r,
        None => {
            tracing::debug!(
                "cni::del {}: no pod_networks record (host-network or already deleted)",
                sandbox_id
            );
            return Ok(());
        }
    };
    let (ip_addr, veth_host, _netns_path) = endpoint.into_parts();
    if let Ok(pod_ip) = Ipv4Addr::from_str(&ip_addr) {
        flush_host_neighbour(handle, bridge_idx, pod_ip, "del-before-release").await;
    }

    let mut veth_delete_failed = false;
    // Delete host veth — kernel removes pod side automatically
    match crate::networking::get_link_index(handle, &veth_host).await {
        Ok(idx) => {
            if let Err(e) = handle.link().del(idx).execute().await {
                tracing::warn!(
                    "cni::del {}: failed to delete veth {}: {}",
                    sandbox_id,
                    veth_host,
                    e
                );
                veth_delete_failed = true;
            }
        }
        Err(_) => {
            tracing::warn!(
                "cni::del {}: veth {} not found (already deleted?)",
                sandbox_id,
                veth_host
            );
        }
    }

    if veth_delete_failed {
        anyhow::bail!(
            "cni::del {}: veth delete failed; keeping pod_networks row for retry",
            sandbox_id
        );
    }

    cache
        .delete_network_for_sandbox(SandboxKey::try_new(sandbox_id)?)
        .await
        .context("Failed to delete pod_networks record")?;

    tracing::info!(
        "cni::del {}: released ip={} veth_host={}",
        sandbox_id,
        ip_addr,
        veth_host
    );
    Ok(())
}

fn neighbour_delete_message(bridge_idx: u32, pod_ip: Ipv4Addr) -> NeighbourMessage {
    let mut message = NeighbourMessage::default();
    message.header.family = AddressFamily::Inet;
    message.header.ifindex = bridge_idx;
    message.header.kind = RouteType::Unspec;
    message
        .attributes
        .push(NeighbourAttribute::Destination(NeighbourAddress::Inet(
            pod_ip,
        )));
    message
}

async fn flush_host_neighbour(
    handle: &rtnetlink::Handle,
    bridge_idx: u32,
    pod_ip: Ipv4Addr,
    reason: &str,
) {
    match handle
        .neighbours()
        .del(neighbour_delete_message(bridge_idx, pod_ip))
        .execute()
        .await
    {
        Ok(()) => tracing::debug!("cni::{reason}: flushed host neighbour cache for {pod_ip}"),
        Err(err) => tracing::debug!(
            "cni::{reason}: no host neighbour cache entry flushed for {pod_ip}: {err}"
        ),
    }
}

async fn cleanup_host_veth(handle: &rtnetlink::Handle, veth_host: &str) {
    if let Ok(idx) = crate::networking::get_link_index(handle, veth_host).await
        && let Err(e) = handle.link().del(idx).execute().await
    {
        tracing::warn!("cni: failed to rollback veth {}: {}", veth_host, e);
    }
}

#[allow(clippy::too_many_arguments)]
async fn allocate_ip_with_reclaim(
    cache: &dyn PodNetworkCache,
    ipam: &dyn PodIpamStore,
    runtime: &dyn PodRuntimeStore,
    sandbox_id: &str,
    pod: &klights_types::PodIdentity,
    subnet_base: u32,
    subnet_size: u32,
    veth_host: &str,
    netns_record_path: &str,
) -> Result<(String, u32)> {
    let request = || {
        PodNetworkAllocationRequest::try_new(
            sandbox_id,
            pod.clone(),
            subnet_base,
            subnet_size,
            veth_host,
            netns_record_path,
        )
    };
    let first = ipam.reserve_ip_and_insert_network(request()?).await;

    match first {
        Ok(alloc) => Ok(alloc.into_parts()),
        Err(e) => {
            if !matches!(e, CacheNetworkError::AddressExhausted { .. }) {
                return Err(anyhow::Error::new(e));
            }

            let live_sandboxes: HashSet<String> = runtime
                .list_pod_runtime()
                .await
                .unwrap_or_default()
                .into_iter()
                .filter_map(|record| record.sandbox_id().map(str::to_owned))
                .collect();

            let mut reclaimed = 0usize;
            if let Ok(assignments) = cache.list_network_assignments().await {
                for assignment in assignments {
                    let stale = assignment.request().clone();
                    if !live_sandboxes.contains(stale.sandbox_id())
                        && cache
                            .delete_network_if_matches(stale)
                            .await
                            .unwrap_or(false)
                    {
                        reclaimed += 1;
                    }
                }
            }
            if reclaimed > 0 {
                tracing::warn!(
                    reclaimed,
                    "cni::add: reclaimed stale pod_network IPAM rows after exhaustion"
                );
            }

            ipam.reserve_ip_and_insert_network(request()?)
                .await
                .map(|allocation| allocation.into_parts())
                .map_err(anyhow::Error::new)
        }
    }
}

fn complete_persisted_assignment<T>(
    result: Result<T>,
    assignment_publisher: &dyn PodNetworkAssignmentPublisher,
    assignment_key: &PodNetworkAssignmentKey,
) -> Result<T> {
    let value = result?;
    assignment_publisher.publish_assignment(assignment_key);
    Ok(value)
}

// Create a veth pair where the peer side is created directly inside the target
// network namespace. This avoids the separate RTM_SETLINK + IFLA_NET_NS_FD move
// that fails with EPERM inside a rootless user namespace.
//
// The peer LinkMessage carries IFLA_NET_NS_FD so the kernel places it into
// the target netns at creation time (RTM_NEWLINK).
async fn create_veth_pair_with_peer_in_netns(
    handle: &rtnetlink::Handle,
    veth_host_name: &str,
    veth_pod_name: &str,
    netns_fd: std::os::unix::io::RawFd,
) -> Result<()> {
    // Build the peer LinkMessage with NetNsFd so it is created inside the pod netns.
    // In rtnetlink's veth() helper convention: the first arg to veth() is the
    // peer name (goes into InfoVeth::Peer), the second arg is the main link name.
    // We replicate that but add NetNsFd to the peer so the kernel places it into
    // the target netns at creation time.
    let mut peer = LinkMessage::default();
    peer.attributes
        .push(LinkAttribute::IfName(veth_pod_name.to_string()));
    peer.attributes.push(LinkAttribute::NetNsFd(netns_fd));
    let link_info_data = InfoData::Veth(InfoVeth::Peer(peer));

    // Build the link info NLA (replicates the private link_info() method).
    let link_info_nlas = vec![
        netlink_packet_route::link::LinkInfo::Kind(InfoKind::Veth),
        netlink_packet_route::link::LinkInfo::Data(link_info_data),
    ];

    let mut req = handle.link().add();
    req.message_mut()
        .attributes
        .push(LinkAttribute::IfName(veth_host_name.to_string()));
    req.message_mut()
        .attributes
        .push(LinkAttribute::LinkInfo(link_info_nlas));
    // Bring the host-side link up (replicates the private up() method).
    req.message_mut().header.flags.push(LinkFlag::Up);
    req.message_mut().header.change_mask.push(LinkFlag::Up);

    req.execute().await.with_context(|| {
        format!(
            "rtnetlink RTM_NEWLINK veth pair {}/{} with peer in netns",
            veth_host_name, veth_pod_name
        )
    })?;
    Ok(())
}

fn run_command_output(args: &[&str], context: &str) -> Result<String> {
    let result = std::process::Command::new("ip")
        .args(args)
        .output()
        .with_context(|| format!("failed to run ip {context}"))?;

    if !result.status.success() {
        let stderr = String::from_utf8_lossy(&result.stderr).trim().to_string();
        return Err(anyhow::anyhow!("ip {context} failed: {stderr}"));
    }

    Ok(String::from_utf8_lossy(&result.stdout).to_string())
}

// Configure the pod-side interface inside the sandbox netns.
// Called from spawn_blocking while this thread is temporarily moved into pod netns.
fn configure_pod_netns(
    netns_path: &str,
    veth_temp_name: &str,
    pod_ip: Ipv4Addr,
    prefix_len: u8,
    gateway: Ipv4Addr,
    pod_link_mtu: u32,
) -> Result<()> {
    use std::os::unix::io::AsFd;

    // Save host netns
    let host_netns =
        std::fs::File::open("/proc/self/ns/net").context("Failed to open host netns")?;

    // Enter pod netns
    let pod_netns = std::fs::File::open(netns_path).context("Failed to open pod netns")?;
    setns(pod_netns.as_fd(), CloneFlags::CLONE_NEWNET).context("Failed to setns into pod netns")?;
    drop(pod_netns);

    let result: Result<()> = (|| {
        let mut netns_socket =
            super::netns_sync::new_route_socket().context("Failed to create netlink socket")?;

        let pod_idx = super::netns_sync::link_index_by_name(&mut netns_socket, veth_temp_name)?;
        super::netns_sync::link_rename(&mut netns_socket, pod_idx, "eth0")?;
        super::netns_sync::link_set_mtu(&mut netns_socket, pod_idx, pod_link_mtu)?;

        super::netns_sync::addr_add_v4(&mut netns_socket, pod_idx, pod_ip, prefix_len)?;

        super::netns_sync::link_up(&mut netns_socket, pod_idx)?;
        let loopback_idx = super::netns_sync::link_index_by_name(&mut netns_socket, "lo")?;
        super::netns_sync::link_up(&mut netns_socket, loopback_idx)?;

        super::netns_sync::route_add_default_v4(&mut netns_socket, gateway, pod_idx)
    })();

    let restore = setns(host_netns.as_fd(), CloneFlags::CLONE_NEWNET);
    drop(host_netns);
    restore_host_netns_or_abort(result, restore)
}

// Centralises the "failed to restore host netns → process-fatal" policy.
//
// A spawn_blocking worker that stays in the pod netns after returning would
// silently route later work into the wrong namespace. There is no safe way to
// recover: abort is the only correct response.
//
// The inner `_with_policy` variant accepts a caller-supplied diverging
// function so the abort branch can be exercised in unit tests without killing
// the test binary.
fn restore_host_netns_or_abort(result: Result<()>, restore: nix::Result<()>) -> Result<()> {
    restore_host_netns_or_abort_with_policy(result, restore, || std::process::abort())
}

fn restore_host_netns_or_abort_with_policy(
    result: Result<()>,
    restore: nix::Result<()>,
    on_restore_fail: impl Fn(),
) -> Result<()> {
    if let Err(e) = restore {
        tracing::error!("CRITICAL: failed to restore host netns: {e}");
        on_restore_fail();
        // on_restore_fail must diverge (abort or panic); any return is a bug.
        unreachable!("on_restore_fail returned without diverging")
    }
    result
}

fn is_interface_not_found_error(err: &anyhow::Error) -> bool {
    err.to_string().contains("Interface '") && err.to_string().contains("' not found")
}

struct ValidateExistingAllocationArgs<'a> {
    handle: &'a rtnetlink::Handle,
    bridge_name: &'a str,
    recorded_netns_path: &'a str,
    current_netns_record_path: &'a str,
    veth_host: &'a str,
    pod_ip: Ipv4Addr,
    prefix: u8,
    gateway: Ipv4Addr,
    netns_setns_path: &'a str,
    inspector: &'a dyn NetnsInspector,
    task_supervisor: &'a klights_supervisor::TaskSupervisor,
    file_process: &'a klights_supervisor::FileProcessExecutor,
}

async fn validate_existing_allocation(
    args: ValidateExistingAllocationArgs<'_>,
) -> Result<ExistingAllocation> {
    let ValidateExistingAllocationArgs {
        handle,
        bridge_name,
        recorded_netns_path,
        current_netns_record_path,
        veth_host,
        pod_ip,
        prefix,
        gateway,
        netns_setns_path,
        inspector,
        task_supervisor,
        file_process,
    } = args;
    if recorded_netns_path.starts_with("/proc/self/fd/") {
        return Ok(ExistingAllocation::Stale {
            reason: format!(
                "recorded netns path {} is ephemeral fd path",
                recorded_netns_path
            ),
        });
    }
    if recorded_netns_path != current_netns_record_path {
        return Ok(ExistingAllocation::Stale {
            reason: format!(
                "recorded netns path {} differs from current {}",
                recorded_netns_path, current_netns_record_path
            ),
        });
    }

    let veth_idx = match crate::networking::get_link_index(handle, veth_host).await {
        Ok(idx) => idx,
        Err(e) if is_interface_not_found_error(&e) => {
            return Ok(ExistingAllocation::Stale {
                reason: "recorded host veth does not exist".to_string(),
            });
        }
        Err(e) => return Err(e).context("failed to look up host veth"),
    };

    let bridge_idx = crate::networking::get_link_index(handle, bridge_name)
        .await
        .ok();
    let mut links = handle.link().get().match_index(veth_idx).execute();
    let veth = links
        .try_next()
        .await
        .context("failed to inspect host veth link attributes")?
        .ok_or_else(|| anyhow::anyhow!("host veth {} disappeared during validation", veth_host))?;

    let mut controller = None;
    for attr in &veth.attributes {
        if let LinkAttribute::Controller(idx) = attr {
            controller = Some(*idx);
        }
    }
    if let Some(expected_bridge_idx) = bridge_idx {
        match controller {
            Some(idx) if idx == expected_bridge_idx => {}
            Some(idx) => {
                return Ok(ExistingAllocation::Stale {
                    reason: format!(
                        "host veth {} attached to controller {} instead of bridge {}",
                        veth_host, idx, expected_bridge_idx
                    ),
                });
            }
            None => {
                return Ok(ExistingAllocation::Stale {
                    reason: format!(
                        "host veth {} has no bridge controller attachment",
                        veth_host
                    ),
                });
            }
        }
    }

    if !veth.header.flags.contains(&LinkFlag::Up) {
        return Ok(ExistingAllocation::Stale {
            reason: format!("host veth {} is not UP", veth_host),
        });
    }

    validate_existing_allocation_netns(
        inspector,
        task_supervisor,
        file_process,
        netns_setns_path,
        pod_ip,
        prefix,
        gateway,
    )
    .await
}

async fn validate_existing_allocation_netns(
    inspector: &dyn NetnsInspector,
    task_supervisor: &klights_supervisor::TaskSupervisor,
    file_process: &klights_supervisor::FileProcessExecutor,
    netns_setns_path: &str,
    pod_ip: Ipv4Addr,
    prefix: u8,
    gateway: Ipv4Addr,
) -> Result<ExistingAllocation> {
    let path = netns_setns_path.to_string();
    let netns_usable = file_process
        .run_blocking_file_keyed(
            "cni_check_netns_path_usable",
            netns_setns_path.to_string(),
            move || Ok(netns_path_usable_blocking(path)),
        )
        .await?;
    if !netns_usable {
        tracing::debug!(
            "cni::add: netns path {} unavailable, validating only host-side state for duplicate ADD",
            netns_setns_path
        );
        return Ok(ExistingAllocation::Valid { ip: pod_ip });
    }

    match inspector
        .inspect(task_supervisor, netns_setns_path, pod_ip, prefix, gateway)
        .await
    {
        Ok(()) => Ok(ExistingAllocation::Valid { ip: pod_ip }),
        Err(e) => Ok(ExistingAllocation::Stale {
            reason: format!("pod netns validation failed: {e:#}"),
        }),
    }
}

async fn remove_stale_allocation(
    cache: &dyn PodNetworkCache,
    handle: &rtnetlink::Handle,
    identity: PodNetworkAllocationRequest,
    veth_host: &str,
) -> Result<()> {
    let sandbox_id = identity.sandbox_id().to_string();
    // The caller holds the instance-owned sandbox-operation guard. Keep the
    // durable identity row in place while deleting its datapath so no
    // same-sandbox rebuild can reserve and recreate a veth that this cleanup
    // then mistakes for the stale link.
    if let Ok(veth_idx) = crate::networking::get_link_index(handle, veth_host).await {
        handle
            .link()
            .del(veth_idx)
            .execute()
            .await
            .with_context(|| format!("Failed to delete stale veth {}", veth_host))?;
    }
    if !cache
        .delete_network_if_matches(identity)
        .await
        .context("Failed to conditionally delete stale pod_networks allocation")?
    {
        anyhow::bail!(
            "stale allocation identity changed concurrently for sandbox {}",
            sandbox_id
        );
    }
    Ok(())
}

fn validate_pod_netns_state(
    netns_setns_path: &str,
    pod_ip: Ipv4Addr,
    prefix: u8,
    gateway: Ipv4Addr,
) -> Result<()> {
    let host_netns =
        std::fs::File::open("/proc/self/ns/net").context("Failed to open host netns handle")?;
    let pod_netns = std::fs::File::open(netns_setns_path)
        .with_context(|| format!("Failed to open pod netns {}", netns_setns_path))?;

    setns(pod_netns.as_fd(), CloneFlags::CLONE_NEWNET).with_context(|| {
        format!(
            "Failed to setns into pod netns for validation {}",
            netns_setns_path
        )
    })?;
    drop(pod_netns);

    let result: Result<()> = (|| {
        let ip_output = run_command_output(
            &["-4", "-o", "addr", "show", "dev", "eth0"],
            "addr show eth0",
        )?;
        let expected_addr = format!(" {}/{}", pod_ip, prefix);
        if !ip_output.contains(&expected_addr) {
            anyhow::bail!(
                "eth0 is missing expected address {}/{} in pod netns",
                pod_ip,
                prefix
            );
        }

        let lo_output = run_command_output(&["-o", "link", "show", "dev", "lo"], "link show lo")?;
        if !lo_output.contains(" UP ") {
            anyhow::bail!("lo is not UP in pod netns");
        }

        let eth_output =
            run_command_output(&["-o", "link", "show", "dev", "eth0"], "link show eth0")?;
        if !eth_output.contains(" UP ") {
            anyhow::bail!("eth0 is not UP in pod netns");
        }

        let route_output =
            run_command_output(&["-4", "route", "show", "default"], "route show default")?;
        let expected = format!("default via {} dev eth0", gateway);
        if !route_output.contains(&expected) {
            anyhow::bail!("missing expected default route: {expected}");
        }

        Ok(())
    })();

    let restore = setns(host_netns.as_fd(), CloneFlags::CLONE_NEWNET);
    drop(host_netns);
    restore_host_netns_or_abort(result, restore)
}

fn open_netns_file_blocking(path: String) -> Result<std::fs::File> {
    std::fs::File::open(&path).with_context(|| format!("Failed to open netns path {}", path))
}

fn netns_path_usable_blocking(path: String) -> bool {
    std::fs::File::open(path).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;

    struct FakeNetnsInspector {
        result: Result<()>,
    }

    #[async_trait]
    impl NetnsInspector for FakeNetnsInspector {
        async fn inspect(
            &self,
            _task_supervisor: &klights_supervisor::TaskSupervisor,
            _netns_setns_path: &str,
            _pod_ip: Ipv4Addr,
            _prefix: u8,
            _gateway: Ipv4Addr,
        ) -> Result<()> {
            self.result
                .as_ref()
                .map(|_| ())
                .map_err(|e| anyhow!("{e:#}"))
        }
    }

    fn test_task_supervisor() -> klights_supervisor::TaskSupervisor {
        klights_supervisor::TaskSupervisor::new(klights_supervisor::TaskCategoryConfig::default())
    }

    struct EmptyNetworkCache;

    impl klights_node_store::PodNetworkCache for EmptyNetworkCache {
        fn get_network_for_uid(
            &self,
            _pod_uid: klights_node_store::PodUidKey,
        ) -> klights_node_store::CacheNetworkFuture<
            '_,
            Option<klights_node_store::PodNetworkEndpoint>,
        > {
            Box::pin(async { Ok(None) })
        }

        fn get_network_for_pod(
            &self,
            _pod: klights_types::PodIdentity,
        ) -> klights_node_store::CacheNetworkFuture<
            '_,
            Option<klights_node_store::PodNetworkEndpoint>,
        > {
            Box::pin(async { Ok(None) })
        }

        fn get_network_for_sandbox(
            &self,
            _sandbox_id: klights_node_store::SandboxKey,
        ) -> klights_node_store::CacheNetworkFuture<
            '_,
            Option<klights_node_store::PodNetworkEndpoint>,
        > {
            Box::pin(async { Ok(None) })
        }

        fn get_network_for_assignment(
            &self,
            _sandbox_id: klights_node_store::SandboxKey,
            _pod: klights_types::PodIdentity,
        ) -> klights_node_store::CacheNetworkFuture<
            '_,
            Option<klights_node_store::PodNetworkEndpoint>,
        > {
            Box::pin(async { Ok(None) })
        }

        fn delete_network_for_sandbox(
            &self,
            _sandbox_id: klights_node_store::SandboxKey,
        ) -> klights_node_store::CacheNetworkFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }

        fn delete_network_if_matches(
            &self,
            _request: klights_node_store::PodNetworkAllocationRequest,
        ) -> klights_node_store::CacheNetworkFuture<'_, bool> {
            Box::pin(async { Ok(false) })
        }

        fn list_network_assignments(
            &self,
        ) -> klights_node_store::CacheNetworkFuture<
            '_,
            Vec<klights_node_store::PodNetworkAssignmentSnapshot>,
        > {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    struct PostSnapshotWinnerCache {
        stale: klights_node_store::PodNetworkAllocationRequest,
        winner: klights_node_store::PodNetworkAllocationRequest,
        current: std::sync::Mutex<klights_node_store::PodNetworkAllocationRequest>,
        winner_deleted: std::sync::atomic::AtomicBool,
    }

    impl PostSnapshotWinnerCache {
        fn new(
            stale: klights_node_store::PodNetworkAllocationRequest,
            winner: klights_node_store::PodNetworkAllocationRequest,
        ) -> Self {
            Self {
                current: std::sync::Mutex::new(stale.clone()),
                stale,
                winner,
                winner_deleted: std::sync::atomic::AtomicBool::new(false),
            }
        }
    }

    impl klights_node_store::PodNetworkCache for PostSnapshotWinnerCache {
        fn get_network_for_uid(
            &self,
            _pod_uid: klights_node_store::PodUidKey,
        ) -> klights_node_store::CacheNetworkFuture<
            '_,
            Option<klights_node_store::PodNetworkEndpoint>,
        > {
            Box::pin(async { Ok(None) })
        }

        fn get_network_for_pod(
            &self,
            _pod: klights_types::PodIdentity,
        ) -> klights_node_store::CacheNetworkFuture<
            '_,
            Option<klights_node_store::PodNetworkEndpoint>,
        > {
            Box::pin(async { Ok(None) })
        }

        fn get_network_for_sandbox(
            &self,
            _sandbox_id: klights_node_store::SandboxKey,
        ) -> klights_node_store::CacheNetworkFuture<
            '_,
            Option<klights_node_store::PodNetworkEndpoint>,
        > {
            Box::pin(async { Ok(None) })
        }

        fn get_network_for_assignment(
            &self,
            _sandbox_id: klights_node_store::SandboxKey,
            _pod: klights_types::PodIdentity,
        ) -> klights_node_store::CacheNetworkFuture<
            '_,
            Option<klights_node_store::PodNetworkEndpoint>,
        > {
            Box::pin(async { Ok(None) })
        }

        fn delete_network_for_sandbox(
            &self,
            _sandbox_id: klights_node_store::SandboxKey,
        ) -> klights_node_store::CacheNetworkFuture<'_, ()> {
            Box::pin(async move {
                let current = self.current.lock().unwrap();
                if *current == self.winner {
                    self.winner_deleted
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                }
                Ok(())
            })
        }

        fn delete_network_if_matches(
            &self,
            request: klights_node_store::PodNetworkAllocationRequest,
        ) -> klights_node_store::CacheNetworkFuture<'_, bool> {
            Box::pin(async move {
                let mut current = self.current.lock().unwrap();
                if *current == request {
                    *current = self.winner.clone();
                    Ok(true)
                } else {
                    Ok(false)
                }
            })
        }

        fn list_network_assignments(
            &self,
        ) -> klights_node_store::CacheNetworkFuture<
            '_,
            Vec<klights_node_store::PodNetworkAssignmentSnapshot>,
        > {
            Box::pin(async move {
                assert_eq!(*self.current.lock().unwrap(), self.stale);
                let allocation = klights_node_store::PodNetworkAllocation::try_new(
                    Ipv4Addr::from(self.stale.subnet_base_int() + 2).to_string(),
                    self.stale.subnet_base_int() + 2,
                )?;
                let snapshot = klights_node_store::PodNetworkAssignmentSnapshot::try_new(
                    self.stale.clone(),
                    allocation,
                )?;
                *self.current.lock().unwrap() = self.winner.clone();
                Ok(vec![snapshot])
            })
        }
    }

    struct ExhaustOnceIpam {
        calls: std::sync::atomic::AtomicUsize,
        base: u32,
    }

    impl klights_node_store::PodIpamStore for ExhaustOnceIpam {
        fn reserve_ip_and_insert_network(
            &self,
            _request: klights_node_store::PodNetworkAllocationRequest,
        ) -> klights_node_store::CacheNetworkFuture<'_, klights_node_store::PodNetworkAllocation>
        {
            Box::pin(async move {
                if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    Err(klights_node_store::CacheNetworkError::AddressExhausted {
                        subnet_base_int: self.base,
                        subnet_size: 4,
                    })
                } else {
                    klights_node_store::PodNetworkAllocation::try_new(
                        Ipv4Addr::from(self.base + 2).to_string(),
                        self.base + 2,
                    )
                }
            })
        }
    }

    struct EmptyRuntimeStore;

    impl klights_node_store::PodRuntimeStore for EmptyRuntimeStore {
        fn admit_pod_runtime(
            &self,
            _admission: klights_node_store::PodRuntimeAdmission,
        ) -> klights_node_store::RuntimeWorkFuture<'_, ()> {
            Box::pin(async { unreachable!("CNI only lists runtime rows") })
        }

        fn record_owned_sandbox(
            &self,
            _sandbox: klights_node_store::OwnedPodSandbox,
        ) -> klights_node_store::RuntimeWorkFuture<'_, ()> {
            Box::pin(async { unreachable!("CNI only lists runtime rows") })
        }

        fn record_cgroup(
            &self,
            _cgroup: klights_node_store::PodRuntimeCgroup,
        ) -> klights_node_store::RuntimeWorkFuture<'_, ()> {
            Box::pin(async { unreachable!("CNI only lists runtime rows") })
        }

        fn delete_pod_runtime_for_uid(
            &self,
            _pod_uid: klights_node_store::RuntimePodUid,
        ) -> klights_node_store::RuntimeWorkFuture<'_, ()> {
            Box::pin(async { unreachable!("CNI only lists runtime rows") })
        }

        fn get_pod_runtime(
            &self,
            _pod_uid: klights_node_store::RuntimePodUid,
        ) -> klights_node_store::RuntimeWorkFuture<'_, Option<klights_node_store::PodRuntimeRecord>>
        {
            Box::pin(async { unreachable!("CNI only lists runtime rows") })
        }

        fn list_pod_runtime(
            &self,
        ) -> klights_node_store::RuntimeWorkFuture<'_, Vec<klights_node_store::PodRuntimeRecord>>
        {
            Box::pin(async { Ok(Vec::new()) })
        }

        fn list_pod_runtime_by_namespace(
            &self,
            _namespace: klights_node_store::RuntimeNamespace,
        ) -> klights_node_store::RuntimeWorkFuture<'_, Vec<klights_node_store::PodRuntimeRecord>>
        {
            Box::pin(async { unreachable!("CNI only lists all runtime rows") })
        }
    }

    struct OrderedIpam {
        order: std::sync::Arc<std::sync::Mutex<Vec<&'static str>>>,
        fail: bool,
    }

    impl klights_node_store::PodIpamStore for OrderedIpam {
        fn reserve_ip_and_insert_network(
            &self,
            _request: klights_node_store::PodNetworkAllocationRequest,
        ) -> klights_node_store::CacheNetworkFuture<'_, klights_node_store::PodNetworkAllocation>
        {
            Box::pin(async move {
                self.order.lock().unwrap().push("persist");
                if self.fail {
                    Err(klights_node_store::CacheNetworkError::persistence_failed(
                        "insert failed",
                    ))
                } else {
                    klights_node_store::PodNetworkAllocation::try_new(
                        "10.42.0.2",
                        u32::from(Ipv4Addr::new(10, 42, 0, 2)),
                    )
                }
            })
        }
    }

    struct OrderedPublisher {
        order: std::sync::Arc<std::sync::Mutex<Vec<&'static str>>>,
    }

    impl klights_network_api::PodNetworkAssignmentPublisher for OrderedPublisher {
        fn publish_assignment(&self, _key: &PodNetworkAssignmentKey) {
            self.order.lock().unwrap().push("publish");
        }
    }

    #[tokio::test]
    async fn fresh_allocation_publishes_once_only_after_persistence() {
        let order = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let ipam = OrderedIpam {
            order: order.clone(),
            fail: false,
        };
        let publisher = OrderedPublisher {
            order: order.clone(),
        };
        let key =
            PodNetworkAssignmentKey::try_new("sandbox-a", "default", "pod-a", "uid-a").unwrap();
        let pod = klights_types::PodIdentity::new("default", "pod-a", "uid-a");

        let allocation = allocate_ip_with_reclaim(
            &EmptyNetworkCache,
            &ipam,
            &EmptyRuntimeStore,
            "sandbox-a",
            &pod,
            u32::from(Ipv4Addr::new(10, 42, 0, 0)),
            256,
            "veth-a",
            "/run/netns/a",
        )
        .await;
        complete_persisted_assignment(allocation, &publisher, &key).unwrap();

        assert_eq!(*order.lock().unwrap(), ["persist", "publish"]);
    }

    #[tokio::test]
    async fn failed_allocation_does_not_publish() {
        let order = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let ipam = OrderedIpam {
            order: order.clone(),
            fail: true,
        };
        let publisher = OrderedPublisher {
            order: order.clone(),
        };
        let key =
            PodNetworkAssignmentKey::try_new("sandbox-a", "default", "pod-a", "uid-a").unwrap();
        let pod = klights_types::PodIdentity::new("default", "pod-a", "uid-a");

        let allocation = allocate_ip_with_reclaim(
            &EmptyNetworkCache,
            &ipam,
            &EmptyRuntimeStore,
            "sandbox-a",
            &pod,
            u32::from(Ipv4Addr::new(10, 42, 0, 0)),
            256,
            "veth-a",
            "/run/netns/a",
        )
        .await;
        assert!(complete_persisted_assignment(allocation, &publisher, &key).is_err());

        assert_eq!(*order.lock().unwrap(), ["persist"]);
    }

    #[tokio::test]
    async fn exhaustion_reclaim_does_not_delete_a_post_snapshot_winner() {
        let base = u32::from(Ipv4Addr::new(10, 42, 88, 0));
        let stale = PodNetworkAllocationRequest::try_new(
            "sandbox-reused",
            klights_types::PodIdentity::new("default", "stale", "uid-stale"),
            base,
            4,
            "veth-stale",
            "/run/netns/stale",
        )
        .unwrap();
        let winner = PodNetworkAllocationRequest::try_new(
            "sandbox-reused",
            klights_types::PodIdentity::new("default", "winner", "uid-winner"),
            base + 4,
            4,
            "veth-winner",
            "/run/netns/winner",
        )
        .unwrap();
        let cache = PostSnapshotWinnerCache::new(stale, winner);
        let ipam = ExhaustOnceIpam {
            calls: std::sync::atomic::AtomicUsize::new(0),
            base,
        };
        let requested = klights_types::PodIdentity::new("default", "new", "uid-new");

        allocate_ip_with_reclaim(
            &cache,
            &ipam,
            &EmptyRuntimeStore,
            "sandbox-new",
            &requested,
            base,
            4,
            "veth-new",
            "/run/netns/new",
        )
        .await
        .unwrap();

        assert!(
            !cache
                .winner_deleted
                .load(std::sync::atomic::Ordering::SeqCst),
            "reclaim must compare the immutable stale snapshot and preserve a post-snapshot winner"
        );
    }

    #[tokio::test]
    async fn same_sandbox_serialization_keeps_row_and_datapath_cleanup_before_rebuild() {
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        enum Assignment {
            Stale,
            Winner,
        }

        let operations = std::sync::Arc::new(tokio::sync::Mutex::new((
            Some(Assignment::Stale),
            Some(Assignment::Stale),
        )));
        let locks = std::sync::Arc::new(SandboxOperationLocks::default());
        let stale_guard = locks.acquire("sandbox-reused").await;
        let waiter = {
            let locks = locks.clone();
            let operations = operations.clone();
            tokio::spawn(async move {
                let _winner_guard = locks.acquire("sandbox-reused").await;
                let mut operations = operations.lock().await;
                assert_eq!(
                    *operations,
                    (None, None),
                    "a same-sandbox rebuild must not reserve until stale datapath and row are gone"
                );
                *operations = (Some(Assignment::Winner), Some(Assignment::Winner));
            })
        };

        {
            let mut operations = operations.lock().await;
            operations.1 = None;
            assert_eq!(
                *operations,
                (Some(Assignment::Stale), None),
                "stale datapath must be removed while its identity row still excludes a winner"
            );
            operations.0 = None;
        }
        drop(stale_guard);
        waiter.await.unwrap();

        assert_eq!(
            *operations.lock().await,
            (Some(Assignment::Winner), Some(Assignment::Winner)),
            "late stale cleanup must not delete the rebuilt winner's datapath"
        );
        assert_eq!(
            locks.retained_key_count(),
            0,
            "instance-owned keyed serialization must release inactive sandbox keys"
        );
    }

    #[tokio::test]
    async fn real_adapter_exhaustion_reclaims_stale_row_and_retries_once() {
        let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let backend = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:cni-real-adapter-stale-reclaim",
        )
        .await
        .unwrap();
        let adapter =
            crate::datastore::node_local::network_adapter::NodeLocalNetworkAdapter::new(backend);
        let base = u32::from(Ipv4Addr::new(10, 42, 91, 0));
        let stale_pod = klights_types::PodIdentity::new("default", "stale", "uid-stale");
        let stale = adapter
            .reserve_ip_and_insert_network(
                PodNetworkAllocationRequest::try_new(
                    "sandbox-stale",
                    stale_pod,
                    base,
                    4,
                    "veth-stale",
                    "/run/netns/stale",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(stale.ip_int(), base + 2);

        let winner_pod = klights_types::PodIdentity::new("default", "winner", "uid-winner");
        let winner = allocate_ip_with_reclaim(
            adapter.as_ref(),
            adapter.as_ref(),
            adapter.as_ref(),
            "sandbox-winner",
            &winner_pod,
            base,
            4,
            "veth-winner",
            "/run/netns/winner",
        )
        .await
        .expect("typed exhaustion must trigger stale-row reclaim and one retry");
        assert_eq!(winner.1, base + 2);
        assert!(
            adapter
                .get_network_for_sandbox(SandboxKey::try_new("sandbox-stale").unwrap())
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            adapter
                .get_network_for_sandbox(SandboxKey::try_new("sandbox-winner").unwrap())
                .await
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_neighbour_delete_message_targets_bridge_and_pod_ip() {
        let pod_ip = Ipv4Addr::new(10, 43, 0, 223);
        let msg = neighbour_delete_message(42, pod_ip);

        assert_eq!(msg.header.ifindex, 42);
        assert_eq!(msg.header.family, netlink_packet_route::AddressFamily::Inet);
        assert!(msg.attributes.iter().any(|attr| matches!(
            attr,
            netlink_packet_route::neighbour::NeighbourAttribute::Destination(
                netlink_packet_route::neighbour::NeighbourAddress::Inet(ip)
            ) if *ip == pod_ip
        )));
    }

    #[test]
    fn test_veth_name_deterministic() {
        let uid = "12345678-abcd-ef00-1234-567890abcdef";
        let uid_hex = uid.replace('-', "");
        let uid_chars = &uid_hex[..uid_hex.len().min(11)];
        let veth_host = format!("veth{}", uid_chars);
        assert_eq!(veth_host, "veth12345678abc");
        assert!(
            veth_host.len() <= 15,
            "veth name must be ≤15 chars for kernel"
        );
    }

    #[test]
    fn test_veth_name_short_uid() {
        let uid = "abc-def";
        let uid_hex = uid.replace('-', "");
        let uid_chars = &uid_hex[..uid_hex.len().min(11)];
        let veth_host = format!("veth{}", uid_chars);
        assert_eq!(veth_host, "vethabcdef");
        assert!(veth_host.len() <= 15);
    }

    #[tokio::test]
    async fn test_validate_existing_allocation_netns_unavailable_path_uses_host_only_validation() {
        let inspector = FakeNetnsInspector {
            result: Err(anyhow!("should not run")),
        };

        let res = validate_existing_allocation_netns(
            &inspector,
            &test_task_supervisor(),
            &crate::kubelet::file_blocking::test_file_process_executor(),
            "/definitely/missing/netns/path",
            Ipv4Addr::new(10, 43, 0, 10),
            24,
            Ipv4Addr::new(10, 43, 0, 1),
        )
        .await
        .expect("validation result");

        match res {
            ExistingAllocation::Valid { ip } => assert_eq!(ip, Ipv4Addr::new(10, 43, 0, 10)),
            ExistingAllocation::Stale { reason } => panic!("unexpected stale allocation: {reason}"),
        }
    }

    #[tokio::test]
    async fn test_validate_existing_allocation_netns_inspector_success_returns_valid() {
        let inspector = FakeNetnsInspector { result: Ok(()) };
        let res = validate_existing_allocation_netns(
            &inspector,
            &test_task_supervisor(),
            &crate::kubelet::file_blocking::test_file_process_executor(),
            "/proc/self/ns/net",
            Ipv4Addr::new(10, 43, 0, 11),
            24,
            Ipv4Addr::new(10, 43, 0, 1),
        )
        .await
        .expect("validation result");

        match res {
            ExistingAllocation::Valid { ip } => assert_eq!(ip, Ipv4Addr::new(10, 43, 0, 11)),
            ExistingAllocation::Stale { reason } => panic!("unexpected stale allocation: {reason}"),
        }
    }

    #[tokio::test]
    async fn test_validate_existing_allocation_netns_inspector_failure_returns_stale() {
        let inspector = FakeNetnsInspector {
            result: Err(anyhow!("missing default route")),
        };
        let res = validate_existing_allocation_netns(
            &inspector,
            &test_task_supervisor(),
            &crate::kubelet::file_blocking::test_file_process_executor(),
            "/proc/self/ns/net",
            Ipv4Addr::new(10, 43, 0, 12),
            24,
            Ipv4Addr::new(10, 43, 0, 1),
        )
        .await
        .expect("validation result");

        match res {
            ExistingAllocation::Stale { reason } => {
                assert!(
                    reason.contains("missing default route"),
                    "stale reason should include inspector error, got: {reason}"
                );
            }
            ExistingAllocation::Valid { ip } => panic!("unexpected valid allocation: {ip}"),
        }
    }

    #[test]
    fn test_restore_host_netns_or_abort_with_policy_op_ok_restore_ok() {
        let result =
            restore_host_netns_or_abort_with_policy(Ok(()), Ok(()), || panic!("abort called"));
        assert!(result.is_ok());
    }

    #[test]
    fn test_restore_host_netns_or_abort_with_policy_op_err_restore_ok_propagates_error() {
        let result = restore_host_netns_or_abort_with_policy(
            Err(anyhow!("operation failed")),
            Ok(()),
            || panic!("abort called"),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("operation failed"));
    }

    #[test]
    #[should_panic(expected = "netns restore failed — abort")]
    fn test_restore_host_netns_or_abort_with_policy_restore_fail_calls_policy() {
        let _ =
            restore_host_netns_or_abort_with_policy(Ok(()), Err(nix::errno::Errno::EPERM), || {
                panic!("netns restore failed — abort")
            });
    }

    #[test]
    #[should_panic(expected = "netns restore failed — abort")]
    fn test_restore_host_netns_or_abort_with_policy_abort_wins_over_op_error() {
        let _ = restore_host_netns_or_abort_with_policy(
            Err(anyhow!("operation also failed")),
            Err(nix::errno::Errno::EPERM),
            || panic!("netns restore failed — abort"),
        );
    }
}
