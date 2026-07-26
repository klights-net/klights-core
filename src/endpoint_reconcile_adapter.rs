use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::{Resource, ResourceBatchOperation, ResourcePreconditions};
use serde_json::Value;

use crate::controllers::endpoints::EndpointReconcileStore;
use crate::datastore::{DatastoreBackend, ResourceListQuery};

#[async_trait]
impl<T> EndpointReconcileStore for T
where
    T: DatastoreBackend + Send + Sync + ?Sized,
{
    async fn endpoint_namespace_is_terminating(&self, namespace: &str) -> Result<bool> {
        let Some(resource) = self.get_namespace(namespace).await? else {
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
        DatastoreBackend::get_resource(self, api_version, kind, namespace, name).await
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
                ResourceListQuery::new(
                    Some(&format!("kubernetes.io/service-name={service_name}")),
                    None,
                    None,
                    None,
                ),
            )
            .await?
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
        DatastoreBackend::create_resource(self, api_version, kind, namespace, name, data).await
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
    }

    async fn apply_resource_batch(&self, operations: Vec<ResourceBatchOperation>) -> Result<()> {
        DatastoreBackend::apply_resource_batch(self, operations).await
    }

    fn endpoint_store_error_is_conflict(&self, error: &anyhow::Error) -> bool {
        crate::datastore::errors::is_conflict_error(error)
    }

    fn endpoint_store_error_is_already_exists(&self, error: &anyhow::Error) -> bool {
        crate::datastore::errors::is_conflict_error(error) || error.to_string().contains("exists")
    }
}
