use async_trait::async_trait;
use klights_cluster_core::{Resource, ResourcePreconditions};
use klights_cluster_store::{
    ClusterResourceMutation, ClusterResourceRead, ResourceCollectionScope, ResourceGetRequest,
    ResourceListQuery, ResourceListRead, ResourceListRequest,
};
use klights_reconcile_api::ControllerStoreResult as Result;
use serde_json::Value;

use crate::bootstrap::controller_adapters::controller_store_error_adapter::map_controller_store_error;
use klights_controllers::service::ServiceReconcileStore;

pub(crate) struct FocusedServiceReconcileStore {
    resource_reads: std::sync::Arc<dyn ClusterResourceRead>,
    resource_mutations: std::sync::Arc<dyn ClusterResourceMutation>,
}

impl FocusedServiceReconcileStore {
    pub(crate) fn new(
        resource_reads: std::sync::Arc<dyn ClusterResourceRead>,
        resource_mutations: std::sync::Arc<dyn ClusterResourceMutation>,
    ) -> Self {
        Self {
            resource_reads,
            resource_mutations,
        }
    }
}

#[async_trait]
impl ServiceReconcileStore for FocusedServiceReconcileStore {
    async fn list_services(&self) -> Result<Vec<Resource>> {
        match self
            .resource_reads
            .list_resources(ResourceListRequest::new(
                "v1",
                "Service",
                ResourceCollectionScope::AllNamespaces,
                ResourceListQuery::all(),
            ))
            .await
            .map_err(|error| map_controller_store_error(error.into()))?
        {
            ResourceListRead::Current(page) | ResourceListRead::Historical(page) => {
                Ok(page.into_items())
            }
            ResourceListRead::Expired {
                requested,
                oldest_available,
                ..
            } => Err(klights_reconcile_api::ControllerStoreError::unavailable(
                format!(
                    "Service LIST at resourceVersion {requested} expired before {oldest_available}"
                ),
            )),
        }
    }

    async fn get_service(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        self.resource_reads
            .get_resource(ResourceGetRequest::new(
                "v1",
                "Service",
                Some(namespace.to_string()),
                name,
            ))
            .await
            .map_err(|error| map_controller_store_error(error.into()))
    }

    async fn update_service(
        &self,
        namespace: &str,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        self.resource_mutations
            .update_resource_with_preconditions(
                "v1",
                "Service",
                Some(namespace),
                name,
                data,
                preconditions,
            )
            .await
            .map_err(|error| map_controller_store_error(error.into()))
    }
}
