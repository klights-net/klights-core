//! Composition-root wiring for Pod repository API and reconcile adapters.

use std::sync::Arc;

use crate::control_plane::client::LeaderApiClient;
use crate::datastore::DatastoreHandle;
use crate::kubelet::pod_repository::{
    PodRepository, PodRepositoryAdapterDependencies, PodRepositoryAdapterFactory,
    PodRepositoryAdapters, PodRepositoryDeliveryDependencies, PodRepositoryNetworkDependencies,
    PodRepositoryRuntimeDependencies,
};
use crate::pod_api_service::{PodApiService, PodApiServiceDependencies};
use crate::side_effects::{SideEffectMetrics, SideEffectRegistry};
use klights_supervisor::TaskSupervisor;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PodSchedulingMode {
    InlineSingleNode,
    DeferredMultiNodeLeader,
}

#[derive(Clone)]
pub struct PodRepositoryBuildConfig {
    pub db: DatastoreHandle,
    pub supervisor: Arc<TaskSupervisor>,
    pub side_effects: Arc<SideEffectRegistry>,
    pub metrics: Arc<SideEffectMetrics>,
    pub pod_network_cache: Arc<dyn klights_node_store::PodNetworkCache>,
    pub assignment_waiter: Arc<dyn klights_network_api::PodNetworkAssignmentWaiter>,
    pub scheduling_mode: PodSchedulingMode,
    pub outbox: Option<Arc<crate::kubelet::outbox::Outbox>>,
    pub cluster_api: Option<Arc<dyn LeaderApiClient>>,
}

struct RootPodRepositoryAdapterFactory {
    db: DatastoreHandle,
    side_effects: Arc<SideEffectRegistry>,
    metrics: Arc<SideEffectMetrics>,
}

impl PodRepositoryAdapterFactory for RootPodRepositoryAdapterFactory {
    fn build(&self, dependencies: PodRepositoryAdapterDependencies) -> PodRepositoryAdapters {
        let pod_reconcile = Arc::new(crate::pod_reconcile_adapter::PodReconcileAdapter::new(
            self.db.clone(),
            self.side_effects.controller_dispatcher_slot(),
            self.metrics.clone(),
            self.side_effects.clone(),
            dependencies.store.clone(),
        ));
        let subresource = Arc::new(crate::pod_subresource_service::PodSubresourceService::new(
            dependencies.store.clone(),
            dependencies.status_only.clone(),
            self.side_effects.controller_dispatcher_slot(),
        ));
        let api = Arc::new(PodApiService::new(PodApiServiceDependencies {
            store: dependencies.store,
            status_only: dependencies.status_only,
            db: self.db.clone(),
            supervisor: dependencies.supervisor,
            delete_coordinator: dependencies.delete_coordinator,
            gc_reconcile: pod_reconcile.clone(),
            service_reconcile: pod_reconcile.clone(),
            side_effects: self.side_effects.clone(),
            metrics: self.metrics.clone(),
        }));
        PodRepositoryAdapters {
            api: api.clone(),
            subresource,
            gc_delete: api.clone(),
            gc_reconcile: pod_reconcile.clone(),
            pdb_reconcile: pod_reconcile.clone(),
            namespace_termination: pod_reconcile.clone(),
            mutation_reconcile: pod_reconcile,
            #[cfg(test)]
            api_for_test: api,
        }
    }
}

pub(crate) fn build_pod_repository_parts(
    config: PodRepositoryBuildConfig,
    leadership: Option<tokio::sync::watch::Receiver<bool>>,
) -> crate::kubelet::pod_repository::facade::PodRepositoryParts {
    let PodRepositoryBuildConfig {
        db,
        supervisor,
        side_effects,
        metrics,
        pod_network_cache,
        assignment_waiter,
        scheduling_mode,
        outbox,
        cluster_api,
    } = config;
    let _ = scheduling_mode;
    PodRepository::build_parts_with_adapters(
        db.clone(),
        PodRepositoryRuntimeDependencies {
            supervisor,
            metrics: metrics.clone(),
        },
        PodRepositoryNetworkDependencies {
            pod_network_cache,
            assignment_waiter,
        },
        PodRepositoryDeliveryDependencies {
            outbox,
            cluster_api,
        },
        leadership,
        Arc::new(RootPodRepositoryAdapterFactory {
            db,
            side_effects: side_effects.clone(),
            metrics: metrics.clone(),
        }),
    )
}
