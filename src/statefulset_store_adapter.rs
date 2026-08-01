use async_trait::async_trait;
use klights_cluster_core::Resource;
use klights_reconcile_api::ControllerStoreResult as Result;

use crate::controller_store_error_adapter::map_controller_store_error;
use crate::datastore::DatastoreBackend;
use crate::kubelet::pod_repository::PodObjectWriter;
use klights_controllers::statefulset::{StatefulSetPodMutation, StatefulSetStore};

#[async_trait]
impl StatefulSetPodMutation for crate::controller_runtime_adapter::RootControllerPodPort {
    async fn create_statefulset_pod(
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
impl StatefulSetPodMutation for dyn PodObjectWriter + '_ {
    async fn create_statefulset_pod(
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
impl StatefulSetPodMutation for crate::kubelet::pod_repository::PodRepository {
    async fn create_statefulset_pod(
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
impl StatefulSetStore for dyn DatastoreBackend + '_ {
    async fn get_statefulset(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        DatastoreBackend::get_resource(self, "apps/v1", "StatefulSet", Some(namespace), name)
            .await
            .map_err(map_controller_store_error)
    }

    async fn update_statefulset_status(
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
