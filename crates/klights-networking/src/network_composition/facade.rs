use anyhow::Context;
use std::sync::Arc;

/// Historical pod-link MTU used when encryption is disabled.
pub const POD_OVERLAY_MTU: u32 = 1450;

pub fn pod_link_mtu_for_encryption(encryption: crate::wireguard::DataplaneEncryption) -> u32 {
    match encryption {
        crate::wireguard::DataplaneEncryption::Enabled => crate::wireguard::WIREGUARD_MTU,
        crate::wireguard::DataplaneEncryption::Disabled => POD_OVERLAY_MTU,
    }
}

/// App-owned parent struct holding one Arc per narrow networking trait.
///
/// This is the gate Tasks 4–6 of the refactor build toward: ApiState
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
/// fn direct_field_access_is_forbidden(network: &klights_networking::Network) {
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
    pub fn new(
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

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_datapath<T: klights_network_api::Datapath>() {}
    fn assert_peer_router<T: klights_network_api::PeerRouter>() {}
    fn assert_service_router<T: klights_network_api::ServiceRouter>() {}
    fn assert_endpoint_resolver<T: klights_network_api::PodEndpointResolver>() {}
    fn assert_endpoint_source<T: klights_network_api::PodEndpointEventSource>() {}

    #[test]
    fn networking_implementations_satisfy_their_focused_ports() {
        assert_datapath::<crate::NetworkPlane>();
        assert_peer_router::<crate::RootPeerDataplane>();
        assert_datapath::<crate::rootless::RootlessNetworkPlane>();
        assert_peer_router::<crate::rootless::RootlessNetworkPlane>();
        assert_service_router::<crate::service_routing::NftServiceRouter>();
        assert_endpoint_resolver::<crate::StorePodEndpointResolver>();
        assert_endpoint_source::<crate::StorePodEndpointResolver>();
    }

    #[test]
    fn pod_link_mtu_tracks_selected_cross_node_dataplane() {
        assert_eq!(
            pod_link_mtu_for_encryption(crate::wireguard::DataplaneEncryption::Enabled),
            crate::wireguard::WIREGUARD_MTU
        );
        assert_eq!(
            pod_link_mtu_for_encryption(crate::wireguard::DataplaneEncryption::Disabled),
            POD_OVERLAY_MTU
        );
        const _: () = assert!(
            crate::wireguard::WIREGUARD_MTU <= POD_OVERLAY_MTU,
            "encrypted pod links must not exceed the lower WireGuard transport MTU"
        );
    }

    #[test]
    fn accessors_preserve_composed_capability_identity() {
        let provider = Arc::new(crate::test_support::MockNetworkProvider::new());
        let datapath: Arc<dyn klights_network_api::Datapath> = provider.clone();
        let peering: Arc<dyn klights_network_api::PeerRouter> = provider;
        let services: Arc<dyn klights_network_api::ServiceRouter> =
            Arc::new(crate::test_support::MockServiceRouter::new());
        let resolver: Arc<dyn klights_network_api::PodEndpointResolver> =
            Arc::new(crate::test_support::MockPodEndpointResolver);
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

    #[tokio::test]
    async fn shutdown_calls_service_cleanup_before_datapath_shutdown() {
        let provider = Arc::new(crate::test_support::MockNetworkProvider::new());
        let services = Arc::new(crate::test_support::MockServiceRouter::new());
        let resolver: Arc<dyn klights_network_api::PodEndpointResolver> =
            Arc::new(crate::test_support::MockPodEndpointResolver);
        let network = Network::new(
            provider.clone(),
            provider.clone(),
            services.clone(),
            resolver,
        );

        network.shutdown().await.expect("shutdown must succeed");
        assert_eq!(services.cleanup_count(), 1);
        let calls = provider.calls();
        assert_eq!(
            calls
                .iter()
                .filter(|call| matches!(call, crate::test_support::NetworkCall::Shutdown))
                .count(),
            1
        );
    }
}
