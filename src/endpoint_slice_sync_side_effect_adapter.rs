use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use klights_network_api::ServiceRouter;
use serde_json::Value;

use crate::side_effects::SideEffect;

/// Syncs nft service rules after EndpointSlice create/update.
///
/// Holds an optional `ServiceRouter` so test fixtures that have no live
/// nft instance can still register the side-effect (the apply path
/// becomes a no-op when `services` is None).
struct EndpointSliceSyncEffect {
    services: Option<Arc<dyn ServiceRouter>>,
}

#[async_trait]
impl SideEffect for EndpointSliceSyncEffect {
    fn name(&self) -> &'static str {
        "endpoint_slice_sync"
    }

    async fn apply(&self, _resource: &Value) -> Result<()> {
        if let Some(services) = &self.services {
            services.request_services_sync()?;
        }
        Ok(())
    }
}

pub(crate) fn effect(services: Option<Arc<dyn ServiceRouter>>) -> Arc<dyn SideEffect> {
    Arc::new(EndpointSliceSyncEffect { services })
}
