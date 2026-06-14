//! Rootless / hybrid network surface.
//!
//! The live bridge/veth/IPAM CNI datapath lives on `RootlessNetworkPlane`.
//! This module keeps the lightweight hybrid peer metadata stub used by focused
//! unit tests while service routing and pod-endpoint resolution remain shared
//! across modes.
//!
//! Service routing and pod-endpoint resolution are reused unchanged —
//! `NftServiceRouter` and `SqlitePodEndpointResolver` work in both
//! modes.

pub mod pasta;

use anyhow::Result;
use async_trait::async_trait;

use crate::networking::peer_router::PeerRouter;
use crate::networking::types::NodeEndpoint;

/// Rootless `PeerRouter`.
///
/// NodeSubnet peer events are metadata for the rootless path: actual
/// cross-node pod routing is reconciled from `pod_endpoints` into nft DNAT
/// on root nodes and the bypass4netns endpoint map on rootless nodes. This
/// router therefore accepts both endpoint variants without programming VXLAN
/// state, keeping the shared peer-watch controller convergent in hybrid mode.
pub struct Bypass4NetnsPeerRouter;

impl Bypass4NetnsPeerRouter {
    pub fn stub() -> Self {
        Self
    }
}

#[async_trait]
impl PeerRouter for Bypass4NetnsPeerRouter {
    async fn apply_peer_endpoint(&self, peer: &NodeEndpoint) -> Result<()> {
        tracing::debug!(?peer, "rootless peer metadata observed");
        Ok(())
    }

    async fn remove_peer_endpoint(&self, peer: &NodeEndpoint) -> Result<()> {
        tracing::debug!(?peer, "rootless peer metadata removed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    // The "no Phase-2-stub strings" invariant is enforced by the base-repo
    // source guard run by `./build.sh`.

    #[tokio::test]
    async fn test_rootless_peer_router_accepts_hybrid_peer_endpoints_without_vxlan() {
        use crate::datastore::NodeSubnet;
        use crate::networking::types::{HostPortRange, NodeName, PodSubnet, VtepMac};
        let router = Bypass4NetnsPeerRouter::stub();

        let vxlan_peer = NodeEndpoint::Vxlan(NodeSubnet {
            node_name: NodeName::parse("peer").unwrap(),
            subnet: PodSubnet::parse("10.42.7.0/24").unwrap(),
            subnet_base_int: 0,
            vtep_ip: Ipv4Addr::new(10, 42, 7, 0),
            vtep_mac: Some(VtepMac::parse("aa:bb:cc:11:22:33").unwrap()),
            node_ip: Ipv4Addr::new(192, 168, 1, 5),
            mode: crate::controllers::annotations::NodePeerMode::Root,
            hostport_range: None,
        });
        router
            .apply_peer_endpoint(&vxlan_peer)
            .await
            .expect("rootless router should ignore VXLAN peer metadata without failing");

        let rootless_peer = NodeEndpoint::Rootless {
            node_ip: std::net::IpAddr::V4(Ipv4Addr::new(192, 168, 1, 9)),
            hostport_range: HostPortRange {
                start: 31000,
                end: 31999,
            },
        };
        router
            .remove_peer_endpoint(&rootless_peer)
            .await
            .expect("rootless router should accept rootless peer metadata");
    }
}
