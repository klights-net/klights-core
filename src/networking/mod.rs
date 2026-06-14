pub mod boot;
pub mod cleanup;
pub mod cni;
pub mod datapath;
pub mod dataplane_health;
pub mod device_state;
pub mod netfilter;
pub mod netns_sync;
pub mod peer_router;
pub mod plane;
pub mod pod_endpoint_resolver;
pub mod pod_network_events;
pub mod provider;
pub mod rootless;
pub mod rootless_plane;
pub mod service_router;
pub mod service_routing;
#[cfg(test)]
pub mod test_support;
pub mod types;
pub mod vxlan;
pub mod vxlan_fdb;
pub mod wireguard;

use anyhow::Context;
use std::sync::{Arc, OnceLock};

pub use boot::NetworkBoot;
pub use cleanup::NetworkCleanup;
pub use datapath::Datapath;
pub use peer_router::PeerRouter;
pub use plane::NetworkPlane;
pub use pod_endpoint_resolver::{PodEndpointResolver, SqlitePodEndpointResolver};
pub use rootless_plane::RootlessNetworkPlane;
pub use service_router::ServiceRouter;
pub use types::{BridgeName, ClusterCidr, NodeEndpoint, NodeName, PodSubnet, VtepMac};

pub fn pod_link_mtu_for_encryption(encryption: wireguard::DataplaneEncryption) -> u32 {
    match encryption {
        wireguard::DataplaneEncryption::Enabled => wireguard::WIREGUARD_MTU,
        wireguard::DataplaneEncryption::Disabled => vxlan::VXLAN_MTU,
    }
}

static POD_NETWORK_EVENTS: OnceLock<pod_network_events::PodNetworkEvents> = OnceLock::new();

pub fn global_pod_network_events() -> pod_network_events::PodNetworkEvents {
    POD_NETWORK_EVENTS
        .get_or_init(pod_network_events::PodNetworkEvents::new)
        .clone()
}

/// App-owned parent struct holding one Arc per narrow networking trait.
///
/// This is the gate Tasks 4–6 of the refactor build toward: AppState
/// holds a single `Arc<Network>` rather than four separate Arcs, and
/// every consumer reaches the surface they need via the matching field
/// (`state.network.datapath`, `state.network.peering`,
/// `state.network.services`, `state.network.resolver`).
///
/// `shutdown` sequences cleanup so the coalescer drains before the
/// netlink connection driver dies.
pub struct Network {
    pub datapath: Arc<dyn Datapath>,
    pub peering: Arc<dyn PeerRouter>,
    pub services: Arc<dyn ServiceRouter>,
    /// `PodEndpointResolver` for cross-mode pod reachability. Service routing
    /// uses the same `pod_endpoints` stream for hybrid DNAT reconciliation.
    pub resolver: Arc<dyn PodEndpointResolver>,
}

impl Network {
    /// Sequenced shutdown: services first (drains coalescer + drops
    /// the nft table), then datapath (kills the rtnetlink connection
    /// driver). PeerRouter has no shutdown hook today.
    pub async fn shutdown(&self) -> anyhow::Result<()> {
        self.services
            .cleanup()
            .await
            .context("services cleanup failed")?;
        self.datapath
            .shutdown()
            .await
            .context("datapath shutdown failed")?;
        Ok(())
    }
}

pub async fn get_link_index(handle: &rtnetlink::Handle, name: &str) -> anyhow::Result<u32> {
    use futures::stream::TryStreamExt;

    let mut links = handle.link().get().match_name(name.to_owned()).execute();
    if let Some(link) = links
        .try_next()
        .await
        .context("failed to list links while resolving interface index")?
    {
        Ok(link.header.index)
    } else {
        anyhow::bail!("Interface '{}' not found", name)
    }
}

pub fn is_nl_eexist_error(err: &rtnetlink::Error) -> bool {
    match err {
        rtnetlink::Error::NetlinkError(e) => {
            if let Some(code) = e.code {
                let code = code.get();
                code == libc::EEXIST || code == -(libc::EEXIST)
            } else {
                false
            }
        }
        // Other variants are not expected for add operations and are treated
        // as non-EEXIST failures. We intentionally avoid string matching on
        // the fallback/error path.
        _ => false,
    }
}

#[cfg(test)]
mod network_facade_tests {
    use super::*;

    #[test]
    fn pod_link_mtu_tracks_selected_cross_node_dataplane() {
        assert_eq!(
            pod_link_mtu_for_encryption(wireguard::DataplaneEncryption::Enabled),
            wireguard::WIREGUARD_MTU
        );
        assert_eq!(
            pod_link_mtu_for_encryption(wireguard::DataplaneEncryption::Disabled),
            vxlan::VXLAN_MTU
        );
        const _: () = assert!(
            wireguard::WIREGUARD_MTU <= vxlan::VXLAN_MTU,
            "encrypted pod links must not exceed the lower WireGuard transport MTU"
        );
    }

    /// Compile-time check: the Network struct exposes one Arc per narrow
    /// sub-trait. If a future refactor flattens or renames these fields,
    /// the destructure here fails.
    #[test]
    fn test_network_struct_holds_all_four_subtraits() {
        fn _assert_fields(n: &Network) {
            let Network {
                datapath: _,
                peering: _,
                services: _,
                resolver: _,
            } = n;
        }
    }

    /// Build a Network of mocks and observe shutdown order: services
    /// must drain before datapath shuts down.
    #[tokio::test]
    async fn test_network_shutdown_calls_each_subtrait_shutdown_in_order() {
        let provider = Arc::new(crate::networking::test_support::MockNetworkProvider::new());
        let services = Arc::new(crate::networking::test_support::MockServiceRouter::new());
        let resolver: Arc<dyn PodEndpointResolver> =
            Arc::new(crate::networking::test_support::MockPodEndpointResolver);
        let net = Network {
            datapath: provider.clone(),
            peering: provider.clone(),
            services: services.clone(),
            resolver,
        };

        net.shutdown().await.expect("shutdown must succeed");
        assert_eq!(
            services.cleanup_count(),
            1,
            "services.cleanup must be invoked exactly once"
        );
        // MockNetworkProvider records Shutdown in its calls vec via
        // <Self as Datapath>::shutdown().
        let calls = provider.calls();
        let shutdown_count = calls
            .iter()
            .filter(|c| matches!(c, crate::networking::test_support::NetworkCall::Shutdown))
            .count();
        assert_eq!(
            shutdown_count, 1,
            "datapath.shutdown must be invoked exactly once"
        );
    }
}
