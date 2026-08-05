pub mod boot;
pub mod plane;
#[cfg(test)]
#[path = "service_routing/tests.rs"]
mod service_routing_adapter_tests;
#[cfg(any(test, feature = "integration-test-harness"))]
pub mod test_support;

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
        assert_peer_router::<klights_networking::RootPeerDataplane>();
        assert_datapath::<klights_networking::rootless::RootlessNetworkPlane>();
        assert_peer_router::<klights_networking::rootless::RootlessNetworkPlane>();
        assert_service_router::<klights_networking::service_routing::NftServiceRouter>();
        assert_endpoint_resolver::<klights_networking::StorePodEndpointResolver>();
        assert_endpoint_source::<klights_networking::StorePodEndpointResolver>();
    }
}

pub use boot::NetworkBoot;
pub use klights_networking::{
    BridgeName, Network, NetworkBootConfig, NetworkCleanup, NetworkCleanupConfig, NetworkMode,
    pod_link_mtu_for_encryption,
};
pub use plane::NetworkPlane;

#[cfg(test)]
mod network_facade_tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn pod_link_mtu_tracks_selected_cross_node_dataplane() {
        assert_eq!(
            pod_link_mtu_for_encryption(
                klights_networking::wireguard::DataplaneEncryption::Enabled
            ),
            klights_networking::wireguard::WIREGUARD_MTU
        );
        assert_eq!(
            pod_link_mtu_for_encryption(
                klights_networking::wireguard::DataplaneEncryption::Disabled
            ),
            klights_networking::POD_OVERLAY_MTU
        );
        const _: () = assert!(
            klights_networking::wireguard::WIREGUARD_MTU <= klights_networking::POD_OVERLAY_MTU,
            "encrypted pod links must not exceed the lower WireGuard transport MTU"
        );
    }

    #[test]
    fn test_network_accessors_preserve_composed_capability_identity() {
        let provider =
            Arc::new(crate::bootstrap::networking::test_support::MockNetworkProvider::new());
        let datapath: Arc<dyn klights_network_api::Datapath> = provider.clone();
        let peering: Arc<dyn klights_network_api::PeerRouter> = provider;
        let services: Arc<dyn klights_network_api::ServiceRouter> =
            Arc::new(crate::bootstrap::networking::test_support::MockServiceRouter::new());
        let resolver: Arc<dyn klights_network_api::PodEndpointResolver> =
            Arc::new(crate::bootstrap::networking::test_support::MockPodEndpointResolver);
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
        let provider =
            Arc::new(crate::bootstrap::networking::test_support::MockNetworkProvider::new());
        let services =
            Arc::new(crate::bootstrap::networking::test_support::MockServiceRouter::new());
        let resolver: Arc<dyn klights_network_api::PodEndpointResolver> =
            Arc::new(crate::bootstrap::networking::test_support::MockPodEndpointResolver);
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
        let calls = provider.calls();
        let shutdown_count = calls
            .iter()
            .filter(|call| {
                matches!(
                    call,
                    crate::bootstrap::networking::test_support::NetworkCall::Shutdown
                )
            })
            .count();
        assert_eq!(
            shutdown_count, 1,
            "datapath.shutdown must be invoked exactly once"
        );
    }
}
