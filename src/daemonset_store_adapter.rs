use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;

use crate::controllers::daemonset::{DaemonSetPodMutation, DaemonSetStore};
use crate::datastore::{DatastoreBackend, ResourceListQuery};
use crate::kubelet::pod_repository::PodObjectWriter;

#[async_trait]
impl<T> DaemonSetPodMutation for T
where
    T: PodObjectWriter + Send + Sync + ?Sized,
{
    async fn create_daemonset_pod(
        &self,
        namespace: &str,
        name: &str,
        node_name: &str,
        pod: serde_json::Value,
    ) -> Result<Resource> {
        PodObjectWriter::create_controller_pod(self, namespace, name, node_name, pod).await
    }
}

#[async_trait]
impl<T> DaemonSetStore for T
where
    T: DatastoreBackend + Send + Sync + ?Sized,
{
    async fn list_controller_revisions(&self, namespace: &str) -> Result<Vec<Resource>> {
        Ok(self
            .list_resources(
                "apps/v1",
                "ControllerRevision",
                Some(namespace),
                ResourceListQuery::all(),
            )
            .await?
            .items)
    }

    async fn create_controller_revision(
        &self,
        namespace: &str,
        name: &str,
        revision: serde_json::Value,
    ) -> Result<Resource> {
        self.create_resource(
            "apps/v1",
            "ControllerRevision",
            Some(namespace),
            name,
            revision,
        )
        .await
    }

    async fn list_nodes(&self) -> Result<Vec<Resource>> {
        Ok(self
            .list_resources("v1", "Node", None, ResourceListQuery::all())
            .await?
            .items)
    }

    async fn update_daemonset_status(
        &self,
        resource: &Resource,
        status: serde_json::Value,
    ) -> Result<()> {
        crate::controllers::common::write_status_for_resource(self, resource, &status)
            .await
            .map(|_| ())
    }
}
