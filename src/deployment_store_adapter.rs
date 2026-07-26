use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::{PatchKind, Resource, ResourcePreconditions};

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
        PodObjectWriter::merge_pod_labels(self, namespace, name, labels).await
    }
}

#[async_trait]
impl<T> DeploymentStore for T
where
    T: DatastoreBackend + Send + Sync + ?Sized,
{
    async fn list_replicasets(&self, namespace: &str) -> Result<Vec<Resource>> {
        Ok(self
            .list_resources(
                "apps/v1",
                "ReplicaSet",
                Some(namespace),
                ResourceListQuery::all(),
            )
            .await?
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
    }

    async fn update_deployment_status(
        &self,
        resource: &Resource,
        status: serde_json::Value,
    ) -> Result<()> {
        crate::controllers::common::write_status_for_resource(self, resource, &status)
            .await
            .map(|_| ())
    }
}
