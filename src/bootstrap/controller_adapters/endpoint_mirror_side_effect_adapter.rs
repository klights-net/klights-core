use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use klights_controllers::endpoints;
use klights_controllers::side_effects::endpoint_mirror::EndpointMirrorStore;

struct RootEndpointMirrorStore {
    store: Arc<super::controller_runtime_adapter::RootControllerLeaderPort>,
    identity: Arc<dyn klights_controllers::ControllerIdentityGenerator>,
}

#[async_trait]
impl EndpointMirrorStore for RootEndpointMirrorStore {
    async fn mirror_endpoints(&self, resource: &Value) -> Result<()> {
        endpoints::mirror_endpoints_to_endpointslice_at(
            self.store.as_ref(),
            resource,
            chrono::Utc::now(),
            self.identity.as_ref(),
        )
        .await
    }

    async fn delete_mirrored_endpointslice(&self, resource: &Value) -> Result<()> {
        endpoints::delete_mirrored_endpointslice_for_endpoints(self.store.as_ref(), resource).await
    }
}

pub(crate) fn port(
    store: Arc<super::controller_runtime_adapter::RootControllerLeaderPort>,
    identity: Arc<dyn klights_controllers::ControllerIdentityGenerator>,
) -> Arc<dyn EndpointMirrorStore> {
    Arc::new(RootEndpointMirrorStore { store, identity })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn endpoint_mirror_delete_hook_removes_mirrored_slice() {
        let db = crate::bootstrap::composition::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
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
            "metadata": {"namespace": "default", "name": "manual"}
        });
        let ports =
            crate::bootstrap::composition::cluster_store::selector::sqlite_opened_passive_store(
                &db,
            );

        klights_controllers::side_effects::endpoint_mirror::effect(port(
            Arc::new(crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort::new_for_test(
                ports.applied_outbox,
                std::sync::Arc::new(db.clone()),
                ports.read_ports.resource_reads(),
                ports.ownership_reads,
            )),
            crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity(),
        ))
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
