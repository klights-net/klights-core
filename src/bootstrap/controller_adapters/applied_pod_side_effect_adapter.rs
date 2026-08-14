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

#[cfg(test)]
pub(crate) async fn handle_applied_pod_side_effects_for_test(
    sinks: AppliedPodSideEffectSinks<'_>,
    command: &StorageCommand,
    resource: Option<&Resource>,
    pod_endpoint_effect: klights_cluster_core::PodEndpointEffect,
    db: crate::datastore::DatastoreHandle,
) -> Result<(), AppliedPodSideEffectError> {
    let workload_store =
        crate::bootstrap::controller_adapters::workload_pod_side_effect_adapter::port_for_test(
            db.clone(),
        );
    let job_store =
        crate::bootstrap::controller_adapters::job_side_effect_adapter::port_for_test(db.clone());
    struct DirectService<'a>(&'a dyn crate::datastore::DatastoreBackend);
    #[async_trait]
    impl klights_controllers::side_effects::service_pod::ServicePodStore for DirectService<'_> {
        async fn load_service_endpoint_state(
            &self,
            namespace: &str,
        ) -> anyhow::Result<klights_controllers::side_effects::service_pod::ServiceEndpointState>
        {
            Ok(
                klights_controllers::side_effects::service_pod::ServiceEndpointState {
                    services: self
                        .0
                        .list_resources(
                            "v1",
                            "Service",
                            Some(namespace),
                            klights_cluster_store::ResourceListOptions::all(),
                        )
                        .await?
                        .items,
                    endpoints: self
                        .0
                        .list_resources(
                            "v1",
                            "Endpoints",
                            Some(namespace),
                            klights_cluster_store::ResourceListOptions::all(),
                        )
                        .await?
                        .items,
                    endpoint_slices: self
                        .0
                        .list_resources(
                            "discovery.k8s.io/v1",
                            "EndpointSlice",
                            Some(namespace),
                            klights_cluster_store::ResourceListOptions::all(),
                        )
                        .await?
                        .items,
                },
            )
        }
    }
    struct DirectGc<'a>(&'a dyn crate::datastore::DatastoreBackend);
    #[async_trait]
    impl klights_controllers::gc::GcResourceStore for DirectGc<'_> {
        async fn list_custom_resource_definitions(&self) -> ControllerStoreResult<Vec<Resource>> {
            self.0
                .list_resources(
                    "apiextensions.k8s.io/v1",
                    "CustomResourceDefinition",
                    None,
                    klights_cluster_store::ResourceListOptions::all(),
                )
                .await
                .map(|p| p.items)
                .map_err(map_controller_store_error)
        }
        async fn get_resource(
            &self,
            a: &str,
            k: &str,
            n: Option<&str>,
            name: &str,
        ) -> ControllerStoreResult<Option<Resource>> {
            self.0
                .get_resource(a, k, n, name)
                .await
                .map_err(map_controller_store_error)
        }
        async fn update_resource_with_preconditions(
            &self,
            a: &str,
            k: &str,
            n: Option<&str>,
            name: &str,
            data: serde_json::Value,
            p: klights_cluster_core::ResourcePreconditions,
        ) -> ControllerStoreResult<Resource> {
            self.0
                .update_resource_with_preconditions(a, k, n, name, data, p)
                .await
                .map_err(map_controller_store_error)
        }
        async fn update_main_resource_with_preconditions(
            &self,
            a: &str,
            k: &str,
            n: Option<&str>,
            name: &str,
            data: serde_json::Value,
            p: klights_cluster_core::ResourcePreconditions,
        ) -> ControllerStoreResult<Resource> {
            self.0
                .update_main_resource_with_preconditions(a, k, n, name, data, p)
                .await
                .map_err(map_controller_store_error)
        }
        async fn find_owned_resources(
            &self,
            uid: &str,
            n: Option<&str>,
        ) -> ControllerStoreResult<Vec<Resource>> {
            self.0
                .find_owned_resources(uid, n)
                .await
                .map_err(map_controller_store_error)
        }
        async fn find_owned_by_name_kind_empty_uid(
            &self,
            a: &str,
            name: &str,
            k: &str,
            n: Option<&str>,
        ) -> ControllerStoreResult<Vec<Resource>> {
            self.0
                .find_owned_by_name_kind_empty_uid(a, name, k, n)
                .await
                .map_err(map_controller_store_error)
        }
    }
    let service_store = DirectService(db.as_ref());
    let gc_store = DirectGc(db.as_ref());
    struct DirectPdb<'a>(&'a dyn crate::datastore::DatastoreBackend);
    #[async_trait]
    impl AppliedPodPdbStore for DirectPdb<'_> {
        async fn list_pod_disruption_budgets(
            &self,
            namespace: &str,
        ) -> ControllerStoreResult<Vec<Resource>> {
            self.0
                .list_resources(
                    "policy/v1",
                    "PodDisruptionBudget",
                    Some(namespace),
                    klights_cluster_store::ResourceListOptions::all(),
                )
                .await
                .map(|page| page.items)
                .map_err(map_controller_store_error)
        }
    }
    let pdb_store = DirectPdb(db.as_ref());
    let stores = AppliedPodSideEffectStores::new(
        &service_store,
        workload_store.as_ref(),
        job_store.as_ref(),
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
