use async_trait::async_trait;
use klights_cluster_core::{Resource, command::StorageCommand};
use klights_cluster_store::{
    ClusterOwnershipRead, ClusterResourceMutation, ClusterResourceRead, ResourceCollectionScope,
    ResourceListQuery, ResourceListRead, ResourceListRequest,
};
use klights_controllers::side_effects::applied_pod::{
    AppliedPodPdbStore, AppliedPodSideEffectError, AppliedPodSideEffectSinks,
    AppliedPodSideEffectStores,
};
use klights_reconcile_api::ControllerStoreResult;

use crate::bootstrap::controller_adapters::controller_store_error_adapter::map_controller_store_error;

struct BorrowedAppliedPodPdbStore<'a> {
    resource_reads: &'a dyn ClusterResourceRead,
}

#[async_trait]
impl AppliedPodPdbStore for BorrowedAppliedPodPdbStore<'_> {
    async fn list_pod_disruption_budgets(
        &self,
        namespace: &str,
    ) -> ControllerStoreResult<Vec<Resource>> {
        match self
            .resource_reads
            .list_resources(ResourceListRequest::new(
                "policy/v1",
                "PodDisruptionBudget",
                ResourceCollectionScope::Namespace(namespace.to_string()),
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
                    "PodDisruptionBudget LIST at resourceVersion {requested} expired before {oldest_available}"
                ),
            )),
        }
    }
}

pub(crate) async fn handle_applied_pod_side_effects(
    sinks: AppliedPodSideEffectSinks<'_>,
    command: &StorageCommand,
    resource: Option<&Resource>,
    pod_endpoint_effect: klights_cluster_core::PodEndpointEffect,
    resource_reads: &dyn ClusterResourceRead,
    resource_mutations: &dyn ClusterResourceMutation,
    ownership_reads: &dyn ClusterOwnershipRead,
) -> Result<(), AppliedPodSideEffectError> {
    let workload_store =
        crate::bootstrap::controller_adapters::workload_pod_side_effect_adapter::borrowed_store(
            resource_reads,
        );
    let job_store = crate::bootstrap::controller_adapters::job_side_effect_adapter::borrowed_store(
        resource_reads,
    );
    let service_store =
        crate::bootstrap::controller_adapters::service_pod_side_effect_adapter::borrowed_store(
            resource_reads,
        );
    let gc_store = crate::bootstrap::controller_adapters::gc_resource_store_adapter::borrowed_store(
        resource_reads,
        resource_mutations,
        ownership_reads,
    );
    let pdb_store = BorrowedAppliedPodPdbStore { resource_reads };
    let stores = AppliedPodSideEffectStores::new(
        &service_store,
        &workload_store,
        &job_store,
        &pdb_store,
        &gc_store,
    );
    klights_controllers::side_effects::applied_pod::handle_applied_pod_side_effects(
        stores,
        sinks,
        command,
        resource,
        pod_endpoint_effect,
    )
    .await
}
