use async_trait::async_trait;
use klights_cluster_core::{Resource, ResourceBatchOperation, ResourcePreconditions};
use klights_cluster_store::ResourceListOptions;
use klights_reconcile_api::ControllerStoreResult as Result;
use serde_json::Value;

use crate::bootstrap::controller_adapters::controller_store_error_adapter::map_controller_store_error;
use crate::datastore::DatastoreBackend;
use klights_controllers::endpoints::EndpointReconcileStore;

#[async_trait]
impl EndpointReconcileStore for dyn DatastoreBackend + '_ {
    async fn endpoint_namespace_is_terminating(&self, namespace: &str) -> Result<bool> {
        let Some(resource) = self
            .get_namespace(namespace)
            .await
            .map_err(map_controller_store_error)?
        else {
            return Ok(false);
        };
        Ok(resource
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(Value::as_str)
            .is_some())
    }

    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        DatastoreBackend::get_resource(self, api_version, kind, namespace, name)
            .await
            .map_err(map_controller_store_error)
    }

    async fn list_service_endpoint_slices(
        &self,
        namespace: &str,
        service_name: &str,
    ) -> Result<Vec<Resource>> {
        Ok(self
            .list_resources(
                "discovery.k8s.io/v1",
                "EndpointSlice",
                Some(namespace),
                ResourceListOptions::new(
                    Some(&format!("kubernetes.io/service-name={service_name}")),
                    None,
                    None,
                    None,
                ),
            )
            .await
            .map_err(map_controller_store_error)?
            .items)
    }

    async fn create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
    ) -> Result<Resource> {
        DatastoreBackend::create_resource(self, api_version, kind, namespace, name, data)
            .await
            .map_err(map_controller_store_error)
    }

    async fn update_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        DatastoreBackend::update_resource_with_preconditions(
            self,
            api_version,
            kind,
            namespace,
            name,
            data,
            preconditions,
        )
        .await
        .map_err(map_controller_store_error)
    }

    async fn delete_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> Result<()> {
        DatastoreBackend::delete_resource_with_preconditions(
            self,
            api_version,
            kind,
            namespace,
            name,
            preconditions,
        )
        .await
        .map_err(map_controller_store_error)
    }

    async fn apply_resource_batch(&self, operations: Vec<ResourceBatchOperation>) -> Result<()> {
        DatastoreBackend::apply_resource_batch(self, operations)
            .await
            .map_err(map_controller_store_error)
    }
}
