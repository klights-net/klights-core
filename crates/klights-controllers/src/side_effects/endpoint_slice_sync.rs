//! Side effect to sync service rules after EndpointSlice changes.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use klights_network_api::ServiceRouter;
use serde_json::Value;

use super::SideEffect;

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

pub fn effect(services: Option<Arc<dyn ServiceRouter>>) -> Arc<dyn SideEffect> {
    Arc::new(EndpointSliceSyncEffect { services })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn endpoint_slice_sync_without_router_is_a_named_noop() {
        let effect = effect(None);
        assert_eq!(effect.name(), "endpoint_slice_sync");
        effect
            .apply(&serde_json::json!({
                "apiVersion": "discovery.k8s.io/v1",
                "kind": "EndpointSlice"
            }))
            .await
            .unwrap();
    }
}
