use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::bootstrap::controller_adapters::controller_store_error_adapter::map_controller_store_error;
use crate::datastore::{DatastoreBackend, DatastoreHandle, ResourceListQuery};
use crate::kubelet::pod_lifecycle_core::message::PodLifecycleKey;
use crate::kubelet::pod_lifecycle_router::{
    OrphanReason, PodLifecycleRouter, enqueue_orphan_finalize,
};
use klights_cluster_core::Resource;
use klights_controllers::node_lifecycle::{
    NodeLifecyclePodStore, NodeLifecycleStore, NodeLostPodLifecycleSink,
};

#[cfg(test)]
#[path = "../../controller_policy_tests/node_lifecycle.rs"]
mod policy_tests;

trait DatastoreNodeLifecycleAccess {
    fn datastore(&self) -> &dyn DatastoreBackend;
}

impl DatastoreNodeLifecycleAccess for DatastoreHandle {
    fn datastore(&self) -> &dyn DatastoreBackend {
        self.as_ref()
    }
}

impl DatastoreNodeLifecycleAccess for &dyn DatastoreBackend {
    fn datastore(&self) -> &dyn DatastoreBackend {
        *self
    }
}

struct DatastoreNodeLifecycleStore<T> {
    inner: T,
}

impl<T> DatastoreNodeLifecycleStore<T> {
    fn new(inner: T) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<T> NodeLifecycleStore for DatastoreNodeLifecycleStore<T>
where
    T: DatastoreNodeLifecycleAccess + Send + Sync,
{
    async fn list_nodes(&self) -> klights_reconcile_api::ControllerStoreResult<Vec<Resource>> {
        Ok(self
            .inner
            .datastore()
            .list_resources("v1", "Node", None, ResourceListQuery::all())
            .await
            .map_err(map_controller_store_error)?
            .items)
    }

    async fn list_node_leases(
        &self,
    ) -> klights_reconcile_api::ControllerStoreResult<Vec<Resource>> {
        Ok(self
            .inner
            .datastore()
            .list_resources(
                "coordination.k8s.io/v1",
                "Lease",
                Some("kube-node-lease"),
                ResourceListQuery::all(),
            )
            .await
            .map_err(map_controller_store_error)?
            .items)
    }
}

#[cfg(test)]
fn borrowed_store(db: &dyn DatastoreBackend) -> DatastoreNodeLifecycleStore<&dyn DatastoreBackend> {
    DatastoreNodeLifecycleStore::new(db)
}

struct NodeLostPodLifecycleAdapter {
    inner: Arc<PodLifecycleRouter>,
}

#[async_trait]
impl NodeLostPodLifecycleSink for NodeLostPodLifecycleAdapter {
    async fn enqueue_node_lost_cleanup(
        &self,
        pod: Resource,
    ) -> klights_reconcile_api::ControllerStoreResult<()> {
        let namespace = pod.namespace.as_deref().unwrap_or("default");
        enqueue_orphan_finalize(
            self.inner.as_ref(),
            PodLifecycleKey::new(namespace, &pod.name, &pod.uid),
            OrphanReason::NodeLost,
        )
        .await
        .map_err(|error| {
            klights_reconcile_api::ControllerStoreError::unavailable(error.to_string())
        })
    }
}

pub(crate) struct NodeLifecycleControllerDependencies {
    pub(crate) store: DatastoreHandle,
    pub(crate) pods: Arc<dyn NodeLifecyclePodStore>,
    pub(crate) pod_mutations: Arc<dyn klights_reconcile_api::PodMutationReconcileSink>,
    pub(crate) pod_lifecycle: Arc<PodLifecycleRouter>,
    pub(crate) lease_observations: Arc<klights_controllers::node_lease::NodeLeaseTracker>,
    pub(crate) supervisor: Arc<klights_supervisor::TaskSupervisor>,
    pub(crate) node_status: Arc<dyn klights_leader_api::LeaderNodeLifecycleStatus>,
    pub(crate) watch: Arc<dyn klights_leader_api::LeaderWatch>,
    pub(crate) coordination: Arc<dyn klights_leader_api::ControllerCoordination>,
    pub(crate) pod_eviction_grace: std::time::Duration,
}

pub(crate) async fn run(
    dependencies: NodeLifecycleControllerDependencies,
    cancel: CancellationToken,
) {
    let NodeLifecycleControllerDependencies {
        store,
        pods,
        pod_mutations,
        pod_lifecycle,
        lease_observations,
        supervisor,
        node_status,
        watch,
        coordination,
        pod_eviction_grace,
    } = dependencies;
    let store = DatastoreNodeLifecycleStore::new(store);
    let pod_lifecycle = NodeLostPodLifecycleAdapter {
        inner: pod_lifecycle,
    };
    let clock = klights_supervisor::SystemWallClock;
    let runtime = klights_controllers::node_lifecycle::NodeLifecycleRuntimeDependencies::new(
        &store,
        pods.as_ref(),
        pod_mutations.as_ref(),
        &pod_lifecycle,
        lease_observations.as_ref(),
        supervisor.as_ref(),
        node_status.as_ref(),
        watch.as_ref(),
        &clock,
        pod_eviction_grace,
    );
    klights_controllers::node_lifecycle::run_node_lifecycle_controller(
        runtime,
        coordination,
        cancel,
    )
    .await;
}
