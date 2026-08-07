use async_trait::async_trait;
use klights_cluster_core::Resource;
use klights_reconcile_api::ControllerStoreResult as Result;

use crate::bootstrap::controller_adapters::controller_store_error_adapter::map_controller_store_error;
use crate::datastore::{DatastoreBackend, ResourceListQuery};
use klights_controllers::replicationcontroller::{
    ReplicationControllerPodMutation, ReplicationControllerStore,
};

#[async_trait]
impl ReplicationControllerPodMutation
    for crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerPodPort
{
    async fn create_replication_controller_pod(
        &self,
        namespace: &str,
        name: &str,
        _node_name: &str,
        pod: serde_json::Value,
    ) -> Result<Resource> {
        self.create_controller_pod(namespace, name, pod)
            .await
            .map_err(map_controller_store_error)
    }

    async fn replace_replication_controller_pod_owner_references(
        &self,
        namespace: &str,
        name: &str,
        owner_references: Vec<serde_json::Value>,
    ) -> Result<Resource> {
        self.replace_controller_owner_references(namespace, name, None, owner_references)
            .await
            .map_err(map_controller_store_error)
    }
}

#[async_trait]
impl ReplicationControllerStore for dyn DatastoreBackend + '_ {
    async fn get_replication_controller(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Resource>> {
        DatastoreBackend::get_resource(self, "v1", "ReplicationController", Some(namespace), name)
            .await
            .map_err(map_controller_store_error)
    }

    async fn list_resource_quotas(&self, namespace: &str) -> Result<Vec<Resource>> {
        Ok(self
            .list_resources(
                "v1",
                "ResourceQuota",
                Some(namespace),
                ResourceListQuery::all(),
            )
            .await
            .map_err(map_controller_store_error)?
            .items)
    }

    async fn update_replication_controller_status(
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
