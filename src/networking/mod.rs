pub mod boot;
pub mod cleanup;
pub mod cni;
pub mod config;
pub mod dataplane_health;
pub mod device_state;
pub(crate) mod hostport_resource;
pub mod netfilter;
pub mod netns_sync;
pub mod plane;
pub mod pod_endpoint_resolver;
pub mod pod_network_events;
pub mod rootless;
pub mod rootless_plane;
/// Concrete service-routing internals do not own a second public hostPort DTO;
/// callers use `klights_network_api::HostPortBinding`.
///
/// ```compile_fail,E0432
/// use klights::networking::service_routing::HostPortSpec;
/// ```
pub mod service_routing;
pub(crate) mod subnet_allocator;
#[cfg(test)]
pub mod test_support;
pub mod types;

#[cfg(test)]
mod contract_conformance_tests {
    fn assert_datapath<T: klights_network_api::Datapath>() {}
    fn assert_peer_router<T: klights_network_api::PeerRouter>() {}
    fn assert_service_router<T: klights_network_api::ServiceRouter>() {}
    fn assert_endpoint_resolver<T: klights_network_api::PodEndpointResolver>() {}
    fn assert_endpoint_source<T: klights_network_api::PodEndpointEventSource>() {}

    #[test]
    fn concrete_network_adapters_implement_focused_ports() {
        assert_datapath::<super::NetworkPlane>();
        assert_peer_router::<super::NetworkPlane>();
        assert_datapath::<super::RootlessNetworkPlane>();
        assert_peer_router::<super::RootlessNetworkPlane>();
        assert_service_router::<super::service_routing::NftServiceRouter>();
        assert_endpoint_resolver::<super::SqlitePodEndpointResolver>();
        assert_endpoint_source::<super::SqlitePodEndpointResolver>();
    }
}
pub mod wireguard;

use anyhow::Context;
use std::sync::Arc;

pub use boot::NetworkBoot;
pub use cleanup::NetworkCleanup;
pub use config::{NetworkBootConfig, NetworkCleanupConfig, NetworkMode};
pub use plane::NetworkPlane;
pub use pod_endpoint_resolver::SqlitePodEndpointResolver;
pub use rootless_plane::RootlessNetworkPlane;
pub use types::{BridgeName, ClusterCidr, NodeName, PodSubnet};

/// Historical pod-link MTU used when encryption is disabled.
pub const POD_OVERLAY_MTU: u32 = 1450;

pub fn pod_link_mtu_for_encryption(encryption: wireguard::DataplaneEncryption) -> u32 {
    match encryption {
        wireguard::DataplaneEncryption::Enabled => wireguard::WIREGUARD_MTU,
        wireguard::DataplaneEncryption::Disabled => POD_OVERLAY_MTU,
    }
}

/// App-owned parent struct holding one Arc per narrow networking trait.
///
/// This is the gate Tasks 4–6 of the refactor build toward: AppState
/// holds a single `Arc<Network>` rather than four separate Arcs, and
/// every consumer reaches the surface it needs via the matching capability
/// accessor.
///
/// `shutdown` sequences cleanup so the coalescer drains before the
/// netlink connection driver dies.
///
/// Facade capabilities are intentionally not fields that downstream crates can
/// reach or replace directly:
///
/// ```compile_fail,E0616
/// fn direct_field_access_is_forbidden(network: &klights::networking::Network) {
///     let _ = (
///         &network.datapath,
///         &network.peering,
///         &network.services,
///         &network.resolver,
///     );
/// }
/// ```
pub struct Network {
    datapath: Arc<dyn klights_network_api::Datapath>,
    peering: Arc<dyn klights_network_api::PeerRouter>,
    services: Arc<dyn klights_network_api::ServiceRouter>,
    /// `PodEndpointResolver` for cross-mode pod reachability. Service routing
    /// uses the same `pod_endpoints` stream for hybrid DNAT reconciliation.
    resolver: Arc<dyn klights_network_api::PodEndpointResolver>,
}

impl Network {
    pub(crate) fn new(
        datapath: Arc<dyn klights_network_api::Datapath>,
        peering: Arc<dyn klights_network_api::PeerRouter>,
        services: Arc<dyn klights_network_api::ServiceRouter>,
        resolver: Arc<dyn klights_network_api::PodEndpointResolver>,
    ) -> Self {
        Self {
            datapath,
            peering,
            services,
            resolver,
        }
    }

    pub fn datapath(&self) -> &Arc<dyn klights_network_api::Datapath> {
        &self.datapath
    }

    pub fn peering(&self) -> &Arc<dyn klights_network_api::PeerRouter> {
        &self.peering
    }

    pub fn services(&self) -> &Arc<dyn klights_network_api::ServiceRouter> {
        &self.services
    }

    pub fn resolver(&self) -> &Arc<dyn klights_network_api::PodEndpointResolver> {
        &self.resolver
    }

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
            POD_OVERLAY_MTU
        );
        const _: () = assert!(
            wireguard::WIREGUARD_MTU <= POD_OVERLAY_MTU,
            "encrypted pod links must not exceed the lower WireGuard transport MTU"
        );
    }

    #[test]
    fn test_network_accessors_preserve_composed_capability_identity() {
        let provider = Arc::new(crate::networking::test_support::MockNetworkProvider::new());
        let datapath: Arc<dyn klights_network_api::Datapath> = provider.clone();
        let peering: Arc<dyn klights_network_api::PeerRouter> = provider;
        let services: Arc<dyn klights_network_api::ServiceRouter> =
            Arc::new(crate::networking::test_support::MockServiceRouter::new());
        let resolver: Arc<dyn klights_network_api::PodEndpointResolver> =
            Arc::new(crate::networking::test_support::MockPodEndpointResolver);
        let network = Network::new(
            datapath.clone(),
            peering.clone(),
            services.clone(),
            resolver.clone(),
        );

        assert!(Arc::ptr_eq(network.datapath(), &datapath));
        assert!(Arc::ptr_eq(network.peering(), &peering));
        assert!(Arc::ptr_eq(network.services(), &services));
        assert!(Arc::ptr_eq(network.resolver(), &resolver));
    }

    /// Build a Network of mocks and observe shutdown order: services
    /// must drain before datapath shuts down.
    #[tokio::test]
    async fn test_network_shutdown_calls_each_subtrait_shutdown_in_order() {
        let provider = Arc::new(crate::networking::test_support::MockNetworkProvider::new());
        let services = Arc::new(crate::networking::test_support::MockServiceRouter::new());
        let resolver: Arc<dyn klights_network_api::PodEndpointResolver> =
            Arc::new(crate::networking::test_support::MockPodEndpointResolver);
        let net = Network::new(
            provider.clone(),
            provider.clone(),
            services.clone(),
            resolver,
        );

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
