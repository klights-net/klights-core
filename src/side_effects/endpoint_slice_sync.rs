//! Side effect to sync service rules after EndpointSlice changes.

use super::SideEffect;
use crate::datastore::DatastoreBackend;
use crate::networking::ServiceRouter;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;

/// Syncs nft service rules after EndpointSlice create/update.
///
/// Holds an optional `ServiceRouter` so test fixtures that have no live
/// nft instance can still register the side-effect (the apply path
/// becomes a no-op when `services` is None).
pub struct EndpointSliceSyncEffect {
    services: Option<Arc<dyn ServiceRouter>>,
}

#[async_trait]
impl SideEffect for EndpointSliceSyncEffect {
    fn name(&self) -> &'static str {
        "endpoint_slice_sync"
    }

    async fn apply(&self, _resource: &Value, _db: &dyn DatastoreBackend) -> Result<()> {
        if let Some(services) = &self.services {
            services.request_services_sync();
        }
        Ok(())
    }
}

/// Create an EndpointSliceSyncEffect instance.
pub fn endpoint_slice_sync(services: Option<Arc<dyn ServiceRouter>>) -> Arc<dyn SideEffect> {
    Arc::new(EndpointSliceSyncEffect { services })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_endpoint_slice_sync_name() {
        let effect = endpoint_slice_sync(None);
        assert_eq!(effect.name(), "endpoint_slice_sync");
    }
}
