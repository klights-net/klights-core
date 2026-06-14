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

use super::types::{BridgeName, NodeName, PodSubnet};
use crate::datastore::node_local::NodeLocalBackend;
use crate::datastore::{
    DatastoreBackend, PodNetworkAllocationLink, PodNetworkAllocationPod,
    PodNetworkAllocationRequest, PodNetworkAllocationSubnet, PodNetworkEndpoint,
};

/// Result of a successful [`add`] call.
pub struct PodNetwork {
    pub ip_addr: std::net::IpAddr,
}

#[async_trait]
pub trait CniStore: Send + Sync {
    async fn get_network_for_sandbox(&self, sandbox_id: &str)
    -> Result<Option<PodNetworkEndpoint>>;
    async fn delete_network_for_sandbox(&self, sandbox_id: &str) -> Result<()>;
    async fn reserve_ip_and_insert_network(
        &self,
        sandbox_id: &str,
        pod: &crate::pod_identity::PodIdentity,
        subnet_base_int: u32,
        subnet_size: u32,
        veth_host: &str,
        netns_path: &str,
    ) -> Result<(String, u32)>;
    async fn live_sandbox_ids(&self) -> Result<HashSet<String>>;
    async fn network_sandbox_ids(&self) -> Result<Vec<String>>;
}

#[async_trait]
impl CniStore for dyn DatastoreBackend {
    async fn get_network_for_sandbox(
        &self,
        sandbox_id: &str,
    ) -> Result<Option<PodNetworkEndpoint>> {
        self.get_pod_network(sandbox_id).await
    }

    async fn delete_network_for_sandbox(&self, sandbox_id: &str) -> Result<()> {
        self.delete_pod_network(sandbox_id).await
    }

    async fn reserve_ip_and_insert_network(
        &self,
        sandbox_id: &str,
        pod: &crate::pod_identity::PodIdentity,
        subnet_base_int: u32,
        subnet_size: u32,
        veth_host: &str,
        netns_path: &str,
    ) -> Result<(String, u32)> {
        self.ipam_allocate_and_record_pod_network(
            sandbox_id,
            pod,
            subnet_base_int,
            subnet_size,
            veth_host,
            netns_path,
        )
        .await
    }

    async fn live_sandbox_ids(&self) -> Result<HashSet<String>> {
        Ok(self
            .list_sandboxes()
            .await?
            .into_iter()
            .map(|sandbox| sandbox.sandbox_id)
            .collect())
    }

    async fn network_sandbox_ids(&self) -> Result<Vec<String>> {
        self.list_pod_network_sandbox_ids().await
    }
}

#[async_trait]
impl CniStore for dyn NodeLocalBackend {
    async fn get_network_for_sandbox(
        &self,
        sandbox_id: &str,
    ) -> Result<Option<PodNetworkEndpoint>> {
        NodeLocalBackend::get_network_for_sandbox(self, sandbox_id).await
    }

    async fn delete_network_for_sandbox(&self, sandbox_id: &str) -> Result<()> {
        NodeLocalBackend::delete_network_for_sandbox(self, sandbox_id).await
    }

    async fn reserve_ip_and_insert_network(
        &self,
        sandbox_id: &str,
        pod: &crate::pod_identity::PodIdentity,
        subnet_base_int: u32,
        subnet_size: u32,
        veth_host: &str,
        netns_path: &str,
    ) -> Result<(String, u32)> {
        NodeLocalBackend::reserve_ip_and_insert_network(
            self,
            PodNetworkAllocationRequest::new(
                sandbox_id,
                PodNetworkAllocationPod::new(&pod.namespace, &pod.name, &pod.uid),
                PodNetworkAllocationSubnet::new(subnet_base_int, subnet_size),
                PodNetworkAllocationLink::new(veth_host, netns_path),
            ),
        )
        .await
    }

    async fn live_sandbox_ids(&self) -> Result<HashSet<String>> {
        Ok(self
            .list_pod_runtime()
            .await?
            .into_iter()
            .filter_map(|runtime| runtime.sandbox_id)
            .collect())
    }

    async fn network_sandbox_ids(&self) -> Result<Vec<String>> {
        NodeLocalBackend::list_networks(self).await
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
        task_supervisor: &crate::task_supervisor::TaskSupervisor,
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
        task_supervisor: &crate::task_supervisor::TaskSupervisor,
        netns_setns_path: &str,
        pod_ip: Ipv4Addr,
        prefix: u8,
        gateway: Ipv4Addr,
    ) -> Result<()> {
        let netns_path = netns_setns_path.to_string();
        task_supervisor
            .run_blocking(
                crate::task_supervisor::TaskCategory::Network,
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
pub struct CniAddArgs<'a, S: CniStore + ?Sized + 'a> {
    pub store: &'a S,
    pub handle: &'a rtnetlink::Handle,
    pub sandbox_id: &'a str,
    pub pod: crate::pod_identity::PodIdentity,
    pub bridge_name: &'a BridgeName,
    pub bridge_idx: u32,
    pub netns_setns_path: &'a str,
    pub netns_record_path: &'a str,
    pub pod_subnet: &'a PodSubnet,
    pub pod_link_mtu: u32,
    pub host_network: bool,
    pub host_ip: &'a str,
    pub _node_name: &'a NodeName,
    pub task_supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
}

pub async fn add<S: CniStore + ?Sized>(args: CniAddArgs<'_, S>) -> Result<PodNetwork> {
    let CniAddArgs {
        store,
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
    let namespace = pod.namespace.as_str();
    let pod_name = pod.name.as_str();
    let pod_uid = pod.uid.as_str();
    if host_network {
        return Ok(PodNetwork {
            ip_addr: std::net::IpAddr::V4(
                Ipv4Addr::from_str(host_ip).unwrap_or(Ipv4Addr::UNSPECIFIED),
            ),
        });
    }

    let bridge_name = bridge_name.as_str();

    // Subnet parameters from the typed primitive — no string re-parsing.
    let subnet_base = pod_subnet.base();
    let prefix_len = pod_subnet.prefix();
    let bridge_ip = pod_subnet.bridge_ip();
    let subnet_size = pod_subnet.size();

    if let Some(endpoint) = store
        .get_network_for_sandbox(sandbox_id)
        .await
        .context("Failed to check existing pod network allocation")?
    {
        let pod_ip = Ipv4Addr::from_str(&endpoint.ip_addr).context("Invalid recorded pod IP")?;
        let allocation = validate_existing_allocation(ValidateExistingAllocationArgs {
            handle,
            bridge_name,
            recorded_netns_path: &endpoint.netns_path,
            current_netns_record_path: netns_record_path,
            veth_host: &endpoint.veth_host,
            pod_ip,
            prefix: prefix_len,
            gateway: bridge_ip,
            netns_setns_path,
            inspector: &RealNetnsInspector,
            task_supervisor: task_supervisor.as_ref(),
        })
        .await
        .with_context(|| format!("Failed to validate existing allocation for {}", sandbox_id))?;

        match allocation {
            ExistingAllocation::Valid { ip } => {
                tracing::debug!(
                    "cni::add {}: reusing existing allocation ip={} veth_host={}",
                    sandbox_id,
                    ip,
                    endpoint.veth_host
                );
                publish_pod_network_assignment(sandbox_id, namespace, pod_name, pod_uid).await;
                return Ok(PodNetwork {
                    ip_addr: std::net::IpAddr::V4(ip),
                });
            }
            ExistingAllocation::Stale { reason } => {
                tracing::warn!(
                    "cni::add {}: existing allocation is stale ({}), rebuilding",
                    sandbox_id,
                    reason
                );
                remove_stale_allocation(store, handle, sandbox_id, &endpoint.veth_host).await?;
            }
        }
    }

    // Deterministic veth name from pod_uid (max 15 chars for kernel)
    let uid_hex = pod_uid.replace('-', "");
    let uid_chars = &uid_hex[..uid_hex.len().min(11)];
    let veth_host = format!("veth{}", uid_chars);
    let veth_pod_temp = format!("vpod{}", uid_chars);

    if let Ok(existing) = crate::networking::get_link_index(handle, &veth_host).await {
        handle
            .link()
            .del(existing)
            .execute()
            .await
            .with_context(|| format!("Failed to delete stale veth {}", veth_host))?;
    }

    // Open the target netns fd before creating the veth pair.
    // In rootless mode (user namespace), rtnetlink RTM_SETLINK + IFLA_NET_NS_FD
    // fails with EPERM. Creating the veth with the peer directly in the target
    // netns (RTM_NEWLINK + IFLA_NET_NS_FD on the peer) works reliably.
    let netns_open_key = netns_setns_path.to_string();
    let netns_path_for_open = netns_setns_path.to_string();
    let netns_file = crate::kubelet::file_blocking::run_blocking_file_keyed(
        "cni_open_sandbox_netns",
        netns_open_key,
        move || open_netns_file_blocking(netns_path_for_open),
    )
    .await
    .with_context(|| format!("Failed to open sandbox netns {}", netns_setns_path))?;
    let netns_fd_raw = netns_file.as_raw_fd();

    // Create veth pair with peer directly in the target netns.
    // This avoids a separate setns_by_fd move which fails with EPERM in rootless.
    create_veth_pair_with_peer_in_netns(handle, &veth_host, &veth_pod_temp, netns_fd_raw)
        .await
        .with_context(|| {
            format!(
                "Failed to create veth pair {}/{} in target netns",
                veth_host, veth_pod_temp
            )
        })?;
    drop(netns_file);

    let setup_result: Result<(String, Ipv4Addr)> = async {
        // Atomically reserve IP + record pod_networks row before netns setup.
        let (ip_addr_str, _ip_int) = allocate_ip_with_reclaim(
            store,
            sandbox_id,
            &pod,
            subnet_base,
            subnet_size,
            &veth_host,
            netns_record_path,
        )
        .await
        .context("Atomic IPAM allocation failed")?;
        publish_pod_network_assignment(sandbox_id, namespace, pod_name, pod_uid).await;
        let pod_ip = Ipv4Addr::from_str(&ip_addr_str).context("Invalid allocated IP")?;
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
        if let Err(e) = crate::utils::write_file_async(&hairpin_path, b"1").await {
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
                crate::task_supervisor::TaskCategory::Network,
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

    let (ip_addr_str, pod_ip) = match setup_result {
        Ok(result) => result,
        Err(e) => {
            cleanup_host_veth(handle, &veth_host).await;
            let _ = store.delete_network_for_sandbox(sandbox_id).await;
            return Err(e);
        }
    };

    tracing::info!(
        "cni::add {}/{}: ip={} veth_host={}",
        namespace,
        pod_name,
        ip_addr_str,
        veth_host
    );

    Ok(PodNetwork {
        ip_addr: std::net::IpAddr::V4(pod_ip),
    })
}

/// Tear down the pod network allocation for a sandbox.
///
/// Deletes the host-side veth (kernel auto-removes the pod side) and removes
/// the `pod_networks` record. Idempotent: missing veth or record is a warning.
pub async fn del<S: CniStore + ?Sized>(
    store: &S,
    handle: &rtnetlink::Handle,
    sandbox_id: &str,
    bridge_idx: u32,
) -> Result<()> {
    let record = store
        .get_network_for_sandbox(sandbox_id)
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
    let crate::datastore::PodNetworkEndpoint {
        ip_addr,
        veth_host,
        netns_path: _netns_path,
    } = endpoint;
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

    store
        .delete_network_for_sandbox(sandbox_id)
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

async fn publish_pod_network_assignment(
    sandbox_id: &str,
    namespace: &str,
    pod_name: &str,
    pod_uid: &str,
) {
    let key = crate::networking::pod_network_events::PodNetworkKey::new(
        sandbox_id, namespace, pod_name, pod_uid,
    );
    crate::networking::global_pod_network_events()
        .publish_assignment(&key)
        .await;
}

async fn allocate_ip_with_reclaim<S: CniStore + ?Sized>(
    store: &S,
    sandbox_id: &str,
    pod: &crate::pod_identity::PodIdentity,
    subnet_base: u32,
    subnet_size: u32,
    veth_host: &str,
    netns_record_path: &str,
) -> Result<(String, u32)> {
    let first = store
        .reserve_ip_and_insert_network(
            sandbox_id,
            pod,
            subnet_base,
            subnet_size,
            veth_host,
            netns_record_path,
        )
        .await;

    match first {
        Ok(alloc) => Ok(alloc),
        Err(e) => {
            if !format!("{e:#}").contains("no free IPs in pod subnet") {
                return Err(e);
            }

            let live_sandboxes = store.live_sandbox_ids().await.unwrap_or_default();

            let mut reclaimed = 0usize;
            if let Ok(ids) = store.network_sandbox_ids().await {
                for sid in ids {
                    if !live_sandboxes.contains(&sid)
                        && store.delete_network_for_sandbox(&sid).await.is_ok()
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

            store
                .reserve_ip_and_insert_network(
                    sandbox_id,
                    pod,
                    subnet_base,
                    subnet_size,
                    veth_host,
                    netns_record_path,
                )
                .await
        }
    }
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
    task_supervisor: &'a crate::task_supervisor::TaskSupervisor,
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
        netns_setns_path,
        pod_ip,
        prefix,
        gateway,
    )
    .await
}

async fn validate_existing_allocation_netns(
    inspector: &dyn NetnsInspector,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
    netns_setns_path: &str,
    pod_ip: Ipv4Addr,
    prefix: u8,
    gateway: Ipv4Addr,
) -> Result<ExistingAllocation> {
    let path = netns_setns_path.to_string();
    let netns_usable = crate::kubelet::file_blocking::run_blocking_file_keyed(
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

async fn remove_stale_allocation<S: CniStore + ?Sized>(
    store: &S,
    handle: &rtnetlink::Handle,
    sandbox_id: &str,
    veth_host: &str,
) -> Result<()> {
    if let Ok(veth_idx) = crate::networking::get_link_index(handle, veth_host).await {
        handle
            .link()
            .del(veth_idx)
            .execute()
            .await
            .with_context(|| format!("Failed to delete stale veth {}", veth_host))?;
    }
    store
        .delete_network_for_sandbox(sandbox_id)
        .await
        .context("Failed to delete stale pod_networks allocation")?;
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
            _task_supervisor: &crate::task_supervisor::TaskSupervisor,
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

    fn test_task_supervisor() -> crate::task_supervisor::TaskSupervisor {
        crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        )
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
