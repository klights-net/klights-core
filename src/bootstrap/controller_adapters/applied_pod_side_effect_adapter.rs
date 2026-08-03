use async_trait::async_trait;
use klights_cluster_core::{Resource, command::StorageCommand};
use klights_controllers::side_effects::applied_pod::{
    AppliedPodPdbStore, AppliedPodSideEffectError, AppliedPodSideEffectSinks,
    AppliedPodSideEffectStores,
};
use klights_reconcile_api::ControllerStoreResult;

use crate::bootstrap::controller_adapters::controller_store_error_adapter::map_controller_store_error;
use crate::datastore::{DatastoreBackend, ResourceListQuery};

struct BorrowedAppliedPodPdbStore<'a> {
    db: &'a dyn DatastoreBackend,
}

#[async_trait]
impl AppliedPodPdbStore for BorrowedAppliedPodPdbStore<'_> {
    async fn list_pod_disruption_budgets(
        &self,
        namespace: &str,
    ) -> ControllerStoreResult<Vec<Resource>> {
        Ok(self
            .db
            .list_resources(
                "policy/v1",
                "PodDisruptionBudget",
                Some(namespace),
                ResourceListQuery::all(),
            )
            .await
            .map_err(map_controller_store_error)?
            .items)
    }
}

pub(crate) async fn handle_applied_pod_side_effects(
    sinks: AppliedPodSideEffectSinks<'_>,
    command: &StorageCommand,
    resource: Option<&Resource>,
    pod_endpoint_effect: klights_cluster_core::PodEndpointEffect,
    db: &dyn DatastoreBackend,
) -> Result<(), AppliedPodSideEffectError> {
    let workload_store =
        crate::bootstrap::controller_adapters::workload_pod_side_effect_adapter::borrowed_store(db);
    let job_store =
        crate::bootstrap::controller_adapters::job_side_effect_adapter::borrowed_store(db);
    let service_store =
        crate::bootstrap::controller_adapters::service_pod_side_effect_adapter::borrowed_store(db);
    let gc_store =
        crate::bootstrap::controller_adapters::gc_resource_store_adapter::borrowed_store(db);
    let pdb_store = BorrowedAppliedPodPdbStore { db };
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
