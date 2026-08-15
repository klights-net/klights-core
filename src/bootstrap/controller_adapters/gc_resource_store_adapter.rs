use async_trait::async_trait;
use klights_cluster_core::{Resource, ResourcePreconditions};
use klights_cluster_store::{
    ClusterOwnershipRead, ClusterResourceMutation, ClusterResourceRead, OwnerNameKindRequest,
    OwnerUidRequest, ResourceCollectionScope, ResourceGetRequest, ResourceListQuery,
    ResourceListRead, ResourceListRequest,
};
use klights_reconcile_api::ControllerStoreResult as Result;

use crate::bootstrap::controller_adapters::controller_store_error_adapter::map_controller_store_error;
use klights_controllers::gc::GcResourceStore;

struct BorrowedGcResourceStore<'a> {
    resource_reads: &'a dyn ClusterResourceRead,
    resource_mutations: &'a dyn ClusterResourceMutation,
    ownership_reads: &'a dyn ClusterOwnershipRead,
}

pub(crate) fn borrowed_store<'a>(
    resource_reads: &'a dyn ClusterResourceRead,
    resource_mutations: &'a dyn ClusterResourceMutation,
    ownership_reads: &'a dyn ClusterOwnershipRead,
) -> impl GcResourceStore + 'a {
    BorrowedGcResourceStore {
        resource_reads,
        resource_mutations,
        ownership_reads,
    }
}

#[async_trait]
impl GcResourceStore for BorrowedGcResourceStore<'_> {
    async fn list_custom_resource_definitions(&self) -> Result<Vec<Resource>> {
        match self
            .resource_reads
            .list_resources(ResourceListRequest::new(
                "apiextensions.k8s.io/v1",
                "CustomResourceDefinition",
                ResourceCollectionScope::Cluster,
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
                    "CustomResourceDefinition LIST at resourceVersion {requested} expired before {oldest_available}"
                ),
            )),
        }
    }

    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        self.resource_reads
            .get_resource(ResourceGetRequest::new(
                api_version,
                kind,
                namespace.map(ToOwned::to_owned),
                name,
            ))
            .await
            .map_err(|error| map_controller_store_error(error.into()))
    }

    async fn update_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        self.resource_mutations
            .update_resource_with_preconditions(
                api_version,
                kind,
                namespace,
                name,
                data,
                preconditions,
            )
            .await
            .map_err(|error| map_controller_store_error(error.into()))
    }

    async fn update_main_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        self.resource_mutations
            .update_main_resource_with_preconditions(
                api_version,
                kind,
                namespace,
                name,
                data,
                preconditions,
            )
            .await
            .map_err(|error| map_controller_store_error(error.into()))
    }

    async fn find_owned_resources(
        &self,
        owner_uid: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        let request = OwnerUidRequest::try_new(owner_uid, namespace.map(ToOwned::to_owned))
            .map_err(|error| map_controller_store_error(error.into()))?;
        self.ownership_reads
            .find_owned_resources(request)
            .await
            .map_err(|error| map_controller_store_error(error.into()))
    }

    async fn find_owned_by_name_kind_empty_uid(
        &self,
        owner_api_version: &str,
        owner_name: &str,
        owner_kind: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        let request = OwnerNameKindRequest::try_new(
            owner_api_version,
            owner_name,
            owner_kind,
            namespace.map(ToOwned::to_owned),
        )
        .map_err(|error| map_controller_store_error(error.into()))?;
        self.ownership_reads
            .find_owned_by_name_kind_empty_uid(request)
            .await
            .map_err(|error| map_controller_store_error(error.into()))
    }
}
