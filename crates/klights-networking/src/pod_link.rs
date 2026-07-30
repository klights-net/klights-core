//! Low-level root-mode veth, netns and node-local IPAM operations.
//!
//! The Phase 13D CNI adapter owns command sequencing, publication and
//! rollback.  These helpers contain only the concrete datapath operations it
//! invokes.

use std::collections::HashSet;
use std::net::Ipv4Addr;
use std::os::fd::AsFd;

use anyhow::{Context, Result};
use klights_node_store::{
    CacheNetworkError, PodIpamStore, PodNetworkAllocationRequest, PodNetworkCache, PodRuntimeStore,
};
use netlink_packet_route::link::{
    InfoData, InfoKind, InfoVeth, LinkAttribute, LinkFlag, LinkInfo, LinkMessage,
};
use nix::sched::{CloneFlags, setns};

#[allow(clippy::too_many_arguments)]
pub async fn allocate_ip_with_reclaim(
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
    match ipam.reserve_ip_and_insert_network(request()?).await {
        Ok(allocation) => Ok(allocation.into_parts()),
        Err(error) => {
            if !matches!(error, CacheNetworkError::AddressExhausted { .. }) {
                return Err(anyhow::Error::new(error));
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

pub async fn create_veth_pair_with_peer_in_netns(
    handle: &rtnetlink::Handle,
    veth_host_name: &str,
    veth_pod_name: &str,
    netns_fd: std::os::unix::io::RawFd,
) -> Result<()> {
    let mut peer = LinkMessage::default();
    peer.attributes
        .push(LinkAttribute::IfName(veth_pod_name.to_string()));
    peer.attributes.push(LinkAttribute::NetNsFd(netns_fd));
    let link_info = vec![
        LinkInfo::Kind(InfoKind::Veth),
        LinkInfo::Data(InfoData::Veth(InfoVeth::Peer(peer))),
    ];

    let mut request = handle.link().add();
    request
        .message_mut()
        .attributes
        .push(LinkAttribute::IfName(veth_host_name.to_string()));
    request
        .message_mut()
        .attributes
        .push(LinkAttribute::LinkInfo(link_info));
    request.message_mut().header.flags.push(LinkFlag::Up);
    request.message_mut().header.change_mask.push(LinkFlag::Up);
    request.execute().await.with_context(|| {
        format!(
            "rtnetlink RTM_NEWLINK veth pair {veth_host_name}/{veth_pod_name} \
             with peer in netns"
        )
    })
}

pub fn configure_pod_netns(
    netns_path: &str,
    veth_temp_name: &str,
    pod_ip: Ipv4Addr,
    prefix_len: u8,
    gateway: Ipv4Addr,
    pod_link_mtu: u32,
) -> Result<()> {
    let host_netns =
        std::fs::File::open("/proc/self/ns/net").context("Failed to open host netns")?;
    let pod_netns = std::fs::File::open(netns_path).context("Failed to open pod netns")?;
    setns(pod_netns.as_fd(), CloneFlags::CLONE_NEWNET).context("Failed to setns into pod netns")?;
    drop(pod_netns);

    let result = (|| {
        let mut socket =
            crate::netns_sync::new_route_socket().context("Failed to create netlink socket")?;
        let pod_index = crate::netns_sync::link_index_by_name(&mut socket, veth_temp_name)?;
        crate::netns_sync::link_rename(&mut socket, pod_index, "eth0")?;
        crate::netns_sync::link_set_mtu(&mut socket, pod_index, pod_link_mtu)?;
        crate::netns_sync::addr_add_v4(&mut socket, pod_index, pod_ip, prefix_len)?;
        crate::netns_sync::link_up(&mut socket, pod_index)?;
        let loopback_index = crate::netns_sync::link_index_by_name(&mut socket, "lo")?;
        crate::netns_sync::link_up(&mut socket, loopback_index)?;
        crate::netns_sync::route_add_default_v4(&mut socket, gateway, pod_index)
    })();

    let restore = setns(host_netns.as_fd(), CloneFlags::CLONE_NEWNET);
    drop(host_netns);
    restore_host_netns_or_abort(result, restore)
}

pub fn validate_pod_netns_state(
    netns_setns_path: &str,
    pod_ip: Ipv4Addr,
    prefix: u8,
    gateway: Ipv4Addr,
) -> Result<()> {
    let host_netns =
        std::fs::File::open("/proc/self/ns/net").context("Failed to open host netns handle")?;
    let pod_netns = std::fs::File::open(netns_setns_path)
        .with_context(|| format!("Failed to open pod netns {netns_setns_path}"))?;
    setns(pod_netns.as_fd(), CloneFlags::CLONE_NEWNET).with_context(|| {
        format!("Failed to setns into pod netns for validation {netns_setns_path}")
    })?;
    drop(pod_netns);

    let result = (|| {
        let ip_output = run_command_output(
            &["-4", "-o", "addr", "show", "dev", "eth0"],
            "addr show eth0",
        )?;
        let expected_address = format!(" {pod_ip}/{prefix}");
        if !ip_output.contains(&expected_address) {
            anyhow::bail!("eth0 is missing expected address {pod_ip}/{prefix} in pod netns");
        }

        let loopback = run_command_output(&["-o", "link", "show", "dev", "lo"], "link show lo")?;
        if !loopback.contains(" UP ") {
            anyhow::bail!("lo is not UP in pod netns");
        }
        let ethernet =
            run_command_output(&["-o", "link", "show", "dev", "eth0"], "link show eth0")?;
        if !ethernet.contains(" UP ") {
            anyhow::bail!("eth0 is not UP in pod netns");
        }
        let routes = run_command_output(&["-4", "route", "show", "default"], "route show default")?;
        let expected_route = format!("default via {gateway} dev eth0");
        if !routes.contains(&expected_route) {
            anyhow::bail!("missing expected default route: {expected_route}");
        }
        Ok(())
    })();

    let restore = setns(host_netns.as_fd(), CloneFlags::CLONE_NEWNET);
    drop(host_netns);
    restore_host_netns_or_abort(result, restore)
}

fn run_command_output(args: &[&str], context: &str) -> Result<String> {
    let result = std::process::Command::new("ip")
        .args(args)
        .output()
        .with_context(|| format!("failed to run ip {context}"))?;
    if !result.status.success() {
        let stderr = String::from_utf8_lossy(&result.stderr).trim().to_string();
        anyhow::bail!("ip {context} failed: {stderr}");
    }
    Ok(String::from_utf8_lossy(&result.stdout).to_string())
}

fn restore_host_netns_or_abort(result: Result<()>, restore: nix::Result<()>) -> Result<()> {
    restore_host_netns_or_abort_with_policy(result, restore, || std::process::abort())
}

pub(crate) fn restore_host_netns_or_abort_with_policy(
    result: Result<()>,
    restore: nix::Result<()>,
    on_restore_fail: impl Fn(),
) -> Result<()> {
    if let Err(error) = restore {
        tracing::error!("CRITICAL: failed to restore host netns: {error}");
        on_restore_fail();
        unreachable!("on_restore_fail returned without diverging")
    }
    result
}

pub fn open_netns_file_blocking(path: String) -> Result<std::fs::File> {
    std::fs::File::open(&path).with_context(|| format!("Failed to open netns path {path}"))
}

pub fn netns_path_usable_blocking(path: String) -> bool {
    std::fs::File::open(path).is_ok()
}
