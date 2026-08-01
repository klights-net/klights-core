use async_trait::async_trait;
use klights_cluster_core::Resource;
use klights_reconcile_api::ControllerStoreResult as Result;

use crate::controller_store_error_adapter::map_controller_store_error;
use crate::controllers::replicaset::{ReplicaSetPodMutation, ReplicaSetStore};
use crate::datastore::DatastoreBackend;
use crate::kubelet::pod_repository::PodObjectWriter;

#[async_trait]
impl<T> ReplicaSetPodMutation for T
where
    T: PodObjectWriter + Send + Sync + ?Sized,
{
    async fn create_replicaset_pod(
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

    async fn replace_replicaset_pod_owner_references(
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
impl<T> ReplicaSetStore for T
where
    T: DatastoreBackend + klights_controllers::gc::GcResourceStore + Send + Sync + ?Sized,
{
    async fn get_replicaset(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        DatastoreBackend::get_resource(self, "apps/v1", "ReplicaSet", Some(namespace), name)
            .await
            .map_err(map_controller_store_error)
    }

    async fn update_replicaset_status(
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
