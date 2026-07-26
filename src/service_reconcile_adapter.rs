use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::{Resource, ResourcePreconditions};
use serde_json::Value;

use crate::controllers::service::ServiceReconcileStore;
use crate::datastore::{DatastoreBackend, ResourceListQuery};

#[async_trait]
impl<T> ServiceReconcileStore for T
where
    T: DatastoreBackend + Send + Sync + ?Sized,
{
    async fn list_services(&self) -> Result<Vec<Resource>> {
        Ok(self
            .list_resources("v1", "Service", None, ResourceListQuery::all())
            .await?
            .items)
    }

    async fn get_service(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        DatastoreBackend::get_resource(self, "v1", "Service", Some(namespace), name).await
    }

    async fn update_service(
        &self,
        namespace: &str,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        DatastoreBackend::update_resource_with_preconditions(
            self,
            "v1",
            "Service",
            Some(namespace),
            name,
            data,
            preconditions,
        )
        .await
    }

    fn service_store_error_is_conflict(&self, error: &anyhow::Error) -> bool {
        crate::datastore::errors::is_conflict_error(error)
    }
}
