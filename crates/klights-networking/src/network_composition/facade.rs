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
