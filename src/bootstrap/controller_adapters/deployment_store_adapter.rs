use async_trait::async_trait;
use klights_cluster_core::{PatchKind, Resource, ResourcePreconditions};
use klights_cluster_store::ResourceListOptions;
use klights_reconcile_api::ControllerStoreResult as Result;

use crate::bootstrap::controller_adapters::controller_store_error_adapter::map_controller_store_error;
use crate::datastore::{DatastoreBackend, ResourcePatchRequest};
use klights_controllers::deployment::{DeploymentPodMutation, DeploymentStore};

#[async_trait]
impl DeploymentPodMutation
    for crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerPodPort
{
    async fn merge_deployment_pod_labels(
        &self,
        namespace: &str,
        name: &str,
        labels: Vec<(String, String)>,
    ) -> Result<Resource> {
        self.merge_controller_pod_labels(namespace, name, None, labels)
            .await
            .map_err(map_controller_store_error)
    }
}

#[async_trait]
impl DeploymentStore for dyn DatastoreBackend + '_ {
    async fn list_replicasets(&self, namespace: &str) -> Result<Vec<Resource>> {
        Ok(self
            .list_resources(
                "apps/v1",
                "ReplicaSet",
                Some(namespace),
                ResourceListOptions::all(),
            )
            .await
            .map_err(map_controller_store_error)?
            .items)
    }

    async fn create_replicaset(
        &self,
        namespace: &str,
        name: &str,
        replicaset: serde_json::Value,
    ) -> Result<Resource> {
        self.create_resource("apps/v1", "ReplicaSet", Some(namespace), name, replicaset)
            .await
            .map_err(map_controller_store_error)
    }

    async fn patch_replicaset_scale(
        &self,
        namespace: &str,
        name: &str,
        patch: serde_json::Value,
        expected_uid: String,
    ) -> Result<Option<Resource>> {
        self.patch_resource_latest_with_preconditions(
            "apps/v1",
            "ReplicaSet",
            Some(namespace),
            name,
            ResourcePatchRequest::new(
                PatchKind::Merge,
                patch,
                ResourcePreconditions::uid(expected_uid),
            ),
        )
        .await
        .map_err(map_controller_store_error)
    }

    async fn update_deployment_status(
        &self,
        resource: &Resource,
        status: serde_json::Value,
    ) -> Result<()> {
        klights_controllers::common::write_status_for_resource(self, resource, &status)
            .await
            .map(|_| ())
            .map_err(map_controller_store_error)
    }
}
