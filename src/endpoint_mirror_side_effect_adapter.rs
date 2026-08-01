use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use crate::datastore::DatastoreHandle;
use crate::side_effects::endpoint_mirror::EndpointMirrorStore;
use klights_controllers::endpoints;
use klights_controllers::side_effects::SideEffect;

/// Mirrors manually-created/updated Endpoints to EndpointSlices.
struct EndpointMirrorEffect {
    db: DatastoreHandle,
    identity: Arc<dyn klights_controllers::ControllerIdentityGenerator>,
}

#[async_trait]
impl SideEffect for EndpointMirrorEffect {
    fn name(&self) -> &'static str {
        "endpoint_mirror"
    }

    async fn apply(&self, resource: &Value) -> Result<()> {
        self.mirror_endpoints(resource).await
    }

    async fn apply_delete(&self, resource: &Value) -> Result<()> {
        self.delete_mirrored_endpointslice(resource).await
    }
}

#[async_trait]
impl EndpointMirrorStore for EndpointMirrorEffect {
    async fn mirror_endpoints(&self, resource: &Value) -> Result<()> {
        endpoints::mirror_endpoints_to_endpointslice_at(
            self.db.as_ref(),
            resource,
            chrono::Utc::now(),
            self.identity.as_ref(),
        )
        .await
    }

    async fn delete_mirrored_endpointslice(&self, resource: &Value) -> Result<()> {
        endpoints::delete_mirrored_endpointslice_for_endpoints(self.db.as_ref(), resource).await
    }
}

pub(crate) fn effect(
    db: DatastoreHandle,
    identity: Arc<dyn klights_controllers::ControllerIdentityGenerator>,
) -> Arc<dyn SideEffect> {
    Arc::new(EndpointMirrorEffect { db, identity })
}
