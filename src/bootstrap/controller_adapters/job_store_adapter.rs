use async_trait::async_trait;
use klights_cluster_core::Resource;
use klights_reconcile_api::ControllerStoreResult as Result;

use crate::bootstrap::controller_adapters::controller_store_error_adapter::map_controller_store_error;
use crate::datastore::DatastoreBackend;
use crate::kubelet::pod_repository::PodObjectWriter;
use klights_controllers::job::{JobPodMutation, JobStore};

#[async_trait]
impl JobPodMutation
    for crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerPodPort
{
    async fn create_job_pod(
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

    async fn replace_job_pod_owner_references(
        &self,
        namespace: &str,
        name: &str,
        owner_references: Vec<serde_json::Value>,
    ) -> Result<Resource> {
        PodObjectWriter::update_pod_owner_references(self, namespace, name, owner_references)
            .await
            .map_err(map_controller_store_error)
    }
}

#[async_trait]
impl JobPodMutation for dyn PodObjectWriter + '_ {
    async fn create_job_pod(
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

    async fn replace_job_pod_owner_references(
        &self,
        namespace: &str,
        name: &str,
        owner_references: Vec<serde_json::Value>,
    ) -> Result<Resource> {
        PodObjectWriter::update_pod_owner_references(self, namespace, name, owner_references)
            .await
            .map_err(map_controller_store_error)
    }
}

#[async_trait]
impl JobPodMutation for crate::kubelet::pod_repository::PodRepository {
    async fn create_job_pod(
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

    async fn replace_job_pod_owner_references(
        &self,
        namespace: &str,
        name: &str,
        owner_references: Vec<serde_json::Value>,
    ) -> Result<Resource> {
        PodObjectWriter::update_pod_owner_references(self, namespace, name, owner_references)
            .await
            .map_err(map_controller_store_error)
    }
}

#[async_trait]
impl JobStore for dyn DatastoreBackend + '_ {
    async fn get_job(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        DatastoreBackend::get_resource(self, "batch/v1", "Job", Some(namespace), name)
            .await
            .map_err(map_controller_store_error)
    }

    async fn update_job_status(
        &self,
        resource: &Resource,
        status: serde_json::Value,
    ) -> Result<Resource> {
        klights_controllers::common::write_status_for_resource(self, resource, &status)
            .await
            .map_err(map_controller_store_error)
    }
}
