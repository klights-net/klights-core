use async_trait::async_trait;
use klights_cluster_core::Resource;
use klights_reconcile_api::ControllerStoreResult as Result;

use crate::bootstrap::controller_adapters::controller_store_error_adapter::map_controller_store_error;
use crate::datastore::{DatastoreBackend, ResourceListQuery};
use crate::kubelet::pod_repository::PodObjectWriter;
use klights_controllers::daemonset::{DaemonSetPodMutation, DaemonSetStore};

#[async_trait]
impl DaemonSetPodMutation
    for crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerPodPort
{
    async fn create_daemonset_pod(
        &self,
        namespace: &str,
        name: &str,
        node_name: &str,
        pod: serde_json::Value,
    ) -> Result<Resource> {
        PodObjectWriter::create_controller_pod(self, namespace, name, node_name, pod)
            .await
            .map_err(map_controller_store_error)
    }
}

#[async_trait]
impl DaemonSetPodMutation for dyn PodObjectWriter + '_ {
    async fn create_daemonset_pod(
        &self,
        namespace: &str,
        name: &str,
        node_name: &str,
        pod: serde_json::Value,
    ) -> Result<Resource> {
        PodObjectWriter::create_controller_pod(self, namespace, name, node_name, pod)
            .await
            .map_err(map_controller_store_error)
    }
}

#[async_trait]
impl DaemonSetPodMutation for crate::kubelet::pod_repository::PodRepository {
    async fn create_daemonset_pod(
        &self,
        namespace: &str,
        name: &str,
        node_name: &str,
        pod: serde_json::Value,
    ) -> Result<Resource> {
        PodObjectWriter::create_controller_pod(self, namespace, name, node_name, pod)
            .await
            .map_err(map_controller_store_error)
    }
}

#[async_trait]
impl DaemonSetStore for dyn DatastoreBackend + '_ {
    async fn list_controller_revisions(&self, namespace: &str) -> Result<Vec<Resource>> {
        Ok(self
            .list_resources(
                "apps/v1",
                "ControllerRevision",
                Some(namespace),
                ResourceListQuery::all(),
            )
            .await
            .map_err(map_controller_store_error)?
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
        .map_err(map_controller_store_error)
    }

    async fn list_nodes(&self) -> Result<Vec<Resource>> {
        Ok(self
            .list_resources("v1", "Node", None, ResourceListQuery::all())
            .await
            .map_err(map_controller_store_error)?
            .items)
    }

    async fn update_daemonset_status(
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
