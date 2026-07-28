use async_trait::async_trait;
use klights_cluster_core::{Resource, ResourcePreconditions};
use klights_reconcile_api::ControllerStoreResult as Result;
use serde_json::Value;

use crate::controller_store_error_adapter::map_controller_store_error;
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
            .await
            .map_err(map_controller_store_error)?
            .items)
    }

    async fn get_service(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        DatastoreBackend::get_resource(self, "v1", "Service", Some(namespace), name)
            .await
            .map_err(map_controller_store_error)
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
        .map_err(map_controller_store_error)
    }
}
