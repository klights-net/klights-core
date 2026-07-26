use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use crate::controllers::endpoints;
use crate::datastore::{DatastoreBackend, DatastoreHandle};
use crate::side_effects::SideEffect;
use crate::side_effects::endpoint_mirror::EndpointMirrorStore;

/// Mirrors manually-created/updated Endpoints to EndpointSlices.
struct EndpointMirrorEffect {
    db: DatastoreHandle,
}

#[async_trait]
impl SideEffect for EndpointMirrorEffect {
    fn name(&self) -> &'static str {
        "endpoint_mirror"
    }

    async fn apply(&self, resource: &Value) -> Result<()> {
        self.db.mirror_endpoints(resource).await
    }

    async fn apply_delete(&self, resource: &Value) -> Result<()> {
        self.db.delete_mirrored_endpointslice(resource).await
    }
}

#[async_trait]
impl EndpointMirrorStore for dyn DatastoreBackend + '_ {
    async fn mirror_endpoints(&self, resource: &Value) -> Result<()> {
        endpoints::mirror_endpoints_to_endpointslice(self, resource).await
    }

    async fn delete_mirrored_endpointslice(&self, resource: &Value) -> Result<()> {
        endpoints::delete_mirrored_endpointslice_for_endpoints(self, resource).await
    }
}

pub(crate) fn effect(db: DatastoreHandle) -> Arc<dyn SideEffect> {
    Arc::new(EndpointMirrorEffect { db })
}
