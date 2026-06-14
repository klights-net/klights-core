//! VXLAN FDB (Forwarding Database) and route management for klights peers.
//!
//! For each peer node recorded in `node_subnets`, klights installs:
//!   - One permanent bridge FDB entry on `klights.vxlan`:
//!     `lladdr=peer.vtep_mac  dst=peer.node_ip  dev=klights.vxlan  permanent`
//!     This tells the kernel to encapsulate frames destined for `peer.vtep_mac`
//!     in a VXLAN UDP packet to `peer.node_ip`.
//!   - One route for the peer's pod subnet via `klights.vxlan`:
//!     `ip route add <peer.subnet> via <peer.vtep_ip> dev klights.vxlan`
//!     This makes pod traffic to the remote subnet exit through the VXLAN device.
//!
//! Both operations are idempotent (existing entries are ignored / replaced).
//! Driven by `reconcile_peers` which is called at startup and whenever a Node
//! change event is observed.

use anyhow::{Context, Result};
use netlink_packet_route::AddressFamily;
use netlink_packet_route::neighbour::{NeighbourFlag, NeighbourState};
use netlink_packet_route::route::{
    RouteAttribute, RouteMessage, RouteProtocol, RouteScope, RouteType,
};
use std::net::IpAddr;

use crate::datastore::{DatastoreBackend, NodeSubnet};

/// Apply FDB entry + route for a single peer node.
///
/// Idempotent: if the entry already exists the kernel returns EEXIST which
/// we silently ignore.
pub async fn apply_peer_subnet(
    handle: &rtnetlink::Handle,
    vxlan_idx: u32,
    peer: &NodeSubnet,
) -> Result<()> {
    let Some(vtep_mac) = peer.vtep_mac else {
        tracing::debug!(
            "vxlan_fdb: skipping peer {} — vtep_mac not yet known",
            peer.node_name
        );
        return Ok(());
    };
    let vtep_mac_bytes = vtep_mac.bytes();
    let node_ip = peer.node_ip;
    let vtep_ip = peer.vtep_ip;

    // ---- FDB entry: lladdr=vtep_mac dst=node_ip dev=klights.vxlan permanent ----
    let fdb_result = handle
        .neighbours()
        .add_bridge(vxlan_idx, &vtep_mac_bytes)
        .state(NeighbourState::Permanent)
        .flags(vec![NeighbourFlag::Own]) // NTF_SELF — applies to the VXLAN device itself
        .destination(IpAddr::V4(node_ip))
        .execute()
        .await;

    if let Err(e) = &fdb_result
        && !crate::networking::is_nl_eexist_error(e)
    {
        fdb_result.with_context(|| {
            format!(
                "Failed to add FDB entry for peer {} (mac={} dst={})",
                peer.node_name, vtep_mac, node_ip
            )
        })?;
    }

    // ---- L3 neighbour: peer VTEP IP -> peer VTEP MAC on klights.vxlan ----
    // The route below points at peer.vtep_ip as gateway; without a permanent
    // neighbour entry the kernel may ARP over a VXLAN device and blackhole.
    handle
        .neighbours()
        .add(vxlan_idx, IpAddr::V4(vtep_ip))
        .link_local_address(&vtep_mac_bytes)
        .state(NeighbourState::Permanent)
        .replace()
        .execute()
        .await
        .with_context(|| {
            format!(
                "Failed to add L3 neighbour for peer {} ({} -> {})",
                peer.node_name, vtep_ip, vtep_mac
            )
        })?;

    // ---- Route: <peer_subnet> via <vtep_ip> dev klights.vxlan ----
    let subnet_ip = peer.subnet.base_ip();
    let prefix_len = peer.subnet.prefix();

    let route_result = handle
        .route()
        .add()
        .v4()
        .destination_prefix(subnet_ip, prefix_len)
        .output_interface(vxlan_idx)
        .gateway(vtep_ip)
        .execute()
        .await;

    if let Err(e) = &route_result
        && !crate::networking::is_nl_eexist_error(e)
    {
        route_result.with_context(|| {
            format!(
                "Failed to add route for peer {} subnet {} via {}",
                peer.node_name, peer.subnet, vtep_ip
            )
        })?;
    }

    tracing::info!(
        "vxlan_fdb: applied peer {} subnet={} vtep_mac={} node_ip={}",
        peer.node_name,
        peer.subnet,
        vtep_mac,
        node_ip,
    );
    Ok(())
}

/// Remove FDB entry + route for a departing peer node.
///
/// Errors from missing entries are logged as warnings and not propagated.
pub async fn remove_peer_subnet(
    handle: &rtnetlink::Handle,
    vxlan_idx: u32,
    peer: &NodeSubnet,
) -> Result<()> {
    let subnet_ip = peer.subnet.base_ip();
    let prefix_len = peer.subnet.prefix();
    let vtep_ip = peer.vtep_ip;
    let node_ip = peer.node_ip;

    // ---- Remove route ----
    {
        let mut route_msg = RouteMessage::default();
        route_msg.header.address_family = AddressFamily::Inet;
        route_msg.header.destination_prefix_length = prefix_len;
        route_msg.header.protocol = RouteProtocol::Static;
        route_msg.header.scope = RouteScope::Universe;
        route_msg.header.kind = RouteType::Unicast;
        route_msg.attributes.push(RouteAttribute::Destination(
            netlink_packet_route::route::RouteAddress::Inet(subnet_ip),
        ));
        route_msg.attributes.push(RouteAttribute::Oif(vxlan_idx));
        route_msg.attributes.push(RouteAttribute::Gateway(
            netlink_packet_route::route::RouteAddress::Inet(vtep_ip),
        ));

        if let Err(e) = handle.route().del(route_msg).execute().await {
            tracing::warn!(
                "vxlan_fdb: failed to remove route for peer {} subnet {}: {}",
                peer.node_name,
                peer.subnet,
                e
            );
        }
    }

    // ---- Remove L3 neighbour ----
    {
        use netlink_packet_route::neighbour::{
            NeighbourAddress, NeighbourAttribute, NeighbourMessage,
        };
        let mut msg = NeighbourMessage::default();
        msg.header.family = AddressFamily::Inet;
        msg.header.ifindex = vxlan_idx;
        msg.header.state = NeighbourState::Permanent;
        msg.attributes
            .push(NeighbourAttribute::Destination(NeighbourAddress::Inet(
                vtep_ip,
            )));

        if let Err(e) = handle.neighbours().del(msg).execute().await {
            tracing::warn!(
                "vxlan_fdb: failed to remove L3 neighbour for peer {}: {}",
                peer.node_name,
                e
            );
        }
    }

    // ---- Remove FDB entry ----
    if let Some(vtep_mac) = peer.vtep_mac {
        use netlink_packet_route::neighbour::{
            NeighbourAddress, NeighbourAttribute, NeighbourMessage,
        };
        let mut msg = NeighbourMessage::default();
        msg.header.family = AddressFamily::Bridge;
        msg.header.ifindex = vxlan_idx;
        msg.header.state = NeighbourState::Permanent;
        msg.attributes.push(NeighbourAttribute::LinkLocalAddress(
            vtep_mac.bytes().to_vec(),
        ));
        msg.attributes
            .push(NeighbourAttribute::Destination(NeighbourAddress::Inet(
                node_ip,
            )));

        if let Err(e) = handle.neighbours().del(msg).execute().await {
            tracing::warn!(
                "vxlan_fdb: failed to remove FDB entry for peer {}: {}",
                peer.node_name,
                e
            );
        }
    }

    tracing::info!("vxlan_fdb: removed peer {}", peer.node_name);
    Ok(())
}

/// Reconcile FDB + routes for all known peers in `node_subnets`.
///
/// Called at startup and on each Node add/update event. Installs entries for
/// all peers that have a non-empty `vtep_mac`. Existing entries are idempotent.
pub async fn reconcile_peers(
    handle: &rtnetlink::Handle,
    db: &dyn DatastoreBackend,
    my_node_name: &str,
    vxlan_device: &str,
) -> Result<()> {
    let peers = db
        .list_peer_subnets(my_node_name)
        .await
        .context("Failed to list peer subnets")?;

    if peers.is_empty() {
        tracing::debug!("vxlan_fdb: no peers to reconcile");
        return Ok(());
    }

    let vxlan_idx = crate::networking::get_link_index(handle, vxlan_device)
        .await
        .with_context(|| format!("{} not found during FDB reconcile", vxlan_device))?;

    for peer in &peers {
        if let Err(e) = apply_peer_subnet(handle, vxlan_idx, peer).await {
            tracing::warn!("vxlan_fdb: failed to apply peer {}: {}", peer.node_name, e);
        }
    }

    tracing::info!("vxlan_fdb: reconciled {} peer(s)", peers.len());
    Ok(())
}
