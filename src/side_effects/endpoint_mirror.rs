//! Side effect to mirror Endpoints to EndpointSlices.

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

#[async_trait]
pub(crate) trait EndpointMirrorStore: Send + Sync {
    async fn mirror_endpoints(&self, resource: &Value) -> Result<()>;
    async fn delete_mirrored_endpointslice(&self, resource: &Value) -> Result<()>;
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_endpoint_mirror_name() {
        let (_db, db_handle) = crate::datastore::test_support::in_memory_with_handle().await;
        let effect = crate::endpoint_mirror_side_effect_adapter::effect(
            db_handle,
            crate::controllers::test_utils::deterministic_controller_identity(),
        );
        assert_eq!(effect.name(), "endpoint_mirror");
    }

    #[tokio::test]
    async fn endpoint_mirror_delete_hook_removes_mirrored_slice() {
        let db = crate::datastore::test_support::in_memory().await;
        let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());
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

        crate::endpoint_mirror_side_effect_adapter::effect(
            db_handle,
            crate::controllers::test_utils::deterministic_controller_identity(),
        )
        .apply_delete(&endpoints)
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
