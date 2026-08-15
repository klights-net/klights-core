//! Root composition adapters for the focused networking contract.
//!
//! Each networking consumer receives a separately erased focused capability
//! and cannot recover unrelated leader operations.

use std::sync::Arc;

pub(crate) struct ApiServiceRoutingSyncAdapter {
    services: Arc<dyn klights_network_api::ServiceRouter>,
}

impl ApiServiceRoutingSyncAdapter {
    pub(crate) fn new(services: Arc<dyn klights_network_api::ServiceRouter>) -> Self {
        Self { services }
    }
}

impl klights_reconcile_api::ServiceRoutingSync for ApiServiceRoutingSyncAdapter {
    fn request_service_routing_sync(
        &self,
    ) -> Result<(), klights_reconcile_api::ReconcileSinkError> {
        self.services.request_services_sync().map_err(|error| {
            klights_reconcile_api::ReconcileSinkError::unavailable(error.to_string())
        })
    }
}

pub(crate) fn network_mode(mode: &crate::bootstrap::NodeMode) -> klights_networking::NetworkMode {
    match mode {
        crate::bootstrap::NodeMode::Root => klights_networking::NetworkMode::Root,
        crate::bootstrap::NodeMode::Rootless { .. } => klights_networking::NetworkMode::Rootless,
    }
}

pub(crate) fn cleanup_config(
    mode: &crate::bootstrap::NodeMode,
    config: &crate::KlightsConfig,
) -> anyhow::Result<klights_networking::NetworkCleanupConfig> {
    klights_networking::NetworkCleanupConfig::try_new(
        network_mode(mode),
        config.bridge_name.clone(),
        config.wireguard_device.clone(),
        config.containerd_namespace.clone(),
        matches!(
            mode,
            crate::bootstrap::NodeMode::Rootless {
                rootlesskit_pid,
                ..
            } if *rootlesskit_pid != 0
        ),
    )
    .map_err(anyhow::Error::msg)
}
