use async_trait::async_trait;
use klights_cluster_core::{PatchKind, Resource, ResourcePreconditions};
use klights_reconcile_api::ControllerStoreResult as Result;

use crate::controller_store_error_adapter::map_controller_store_error;
use crate::controllers::deployment::{DeploymentPodMutation, DeploymentStore};
use crate::datastore::{DatastoreBackend, ResourceListQuery, ResourcePatchRequest};
use crate::kubelet::pod_repository::PodObjectWriter;

#[async_trait]
impl<T> DeploymentPodMutation for T
where
    T: PodObjectWriter + Send + Sync + ?Sized,
{
    async fn merge_deployment_pod_labels(
        &self,
        namespace: &str,
        name: &str,
        labels: Vec<(String, String)>,
    ) -> Result<Resource> {
        PodObjectWriter::merge_pod_labels(self, namespace, name, labels)
            .await
            .map_err(map_controller_store_error)
    }
}

#[async_trait]
impl<T> DeploymentStore for T
where
    T: DatastoreBackend + klights_controllers::gc::GcResourceStore + Send + Sync + ?Sized,
{
    async fn list_replicasets(&self, namespace: &str) -> Result<Vec<Resource>> {
        Ok(self
            .list_resources(
                "apps/v1",
                "ReplicaSet",
                Some(namespace),
                ResourceListQuery::all(),
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
        crate::controllers::common::write_status_for_resource(self, resource, &status)
            .await
            .map(|_| ())
            .map_err(map_controller_store_error)
    }
}
