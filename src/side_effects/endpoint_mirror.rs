//! Side effect to mirror Endpoints to EndpointSlices.

use super::SideEffect;
use crate::controllers::endpoints;
use crate::datastore::DatastoreBackend;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;

/// Mirrors manually-created/updated Endpoints to EndpointSlices.
pub struct EndpointMirrorEffect;

#[async_trait]
impl SideEffect for EndpointMirrorEffect {
    fn name(&self) -> &'static str {
        "endpoint_mirror"
    }

    async fn apply(&self, resource: &Value, db: &dyn DatastoreBackend) -> Result<()> {
        endpoints::mirror_endpoints_to_endpointslice(db, resource).await
    }

    async fn apply_delete(&self, resource: &Value, db: &dyn DatastoreBackend) -> Result<()> {
        endpoints::delete_mirrored_endpointslice_for_endpoints(db, resource).await
    }
}

/// Create an EndpointMirrorEffect instance.
pub fn endpoint_mirror() -> Arc<dyn SideEffect> {
    Arc::new(EndpointMirrorEffect)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_endpoint_mirror_name() {
        let effect = endpoint_mirror();
        assert_eq!(effect.name(), "endpoint_mirror");
    }

    #[tokio::test]
    async fn endpoint_mirror_delete_hook_removes_mirrored_slice() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("default"),
            "manual-mirror",
            serde_json::json!({
                "apiVersion": "discovery.k8s.io/v1",
                "kind": "EndpointSlice",
                "metadata": {
                    "namespace": "default",
                    "name": "manual-mirror",
                    "labels": {
                        "endpointslice.kubernetes.io/managed-by": "endpointslicemirroring-controller.k8s.io"
                    }
                },
                "addressType": "IPv4",
                "endpoints": [],
                "ports": []
            }),
        )
        .await
        .expect("create mirror slice");
        let endpoints = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {
                "namespace": "default",
                "name": "manual"
            }
        });

        endpoint_mirror()
            .apply_delete(&endpoints, &db)
            .await
            .expect("delete hook");

        assert!(
            db.get_resource(
                "discovery.k8s.io/v1",
                "EndpointSlice",
                Some("default"),
                "manual-mirror",
            )
            .await
            .expect("get mirror")
            .is_none()
        );
    }
}
