use klights_cluster_store::{
    ClusterResourceRead, ResourceCollectionScope, ResourceListQuery, ResourceListRead,
    ResourceListRequest,
};
use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::bootstrap::controller_adapters::controller_store_error_adapter::map_controller_store_error;
use klights_cluster_core::Resource;
use klights_controllers::node_lifecycle::{
    NodeLifecyclePodStore, NodeLifecycleStore, NodeLostPodLifecycleSink,
};
use klights_kubelet::pod_lifecycle_core::message::PodLifecycleKey;
use klights_kubelet::pod_lifecycle_router::{
    OrphanReason, PodLifecycleRouter, enqueue_orphan_finalize,
};

struct DatastoreNodeLifecycleStore {
    resource_reads: Arc<dyn ClusterResourceRead>,
}

#[async_trait]
impl NodeLifecycleStore for DatastoreNodeLifecycleStore {
    async fn list_nodes(&self) -> klights_reconcile_api::ControllerStoreResult<Vec<Resource>> {
        match self
            .resource_reads
            .list_resources(ResourceListRequest::new(
                "v1",
                "Node",
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
                    "Node LIST at resourceVersion {requested} expired before {oldest_available}"
                ),
            )),
        }
    }

    async fn list_node_leases(
        &self,
    ) -> klights_reconcile_api::ControllerStoreResult<Vec<Resource>> {
        match self
            .resource_reads
            .list_resources(ResourceListRequest::new(
                "coordination.k8s.io/v1",
                "Lease",
                ResourceCollectionScope::Namespace("kube-node-lease".to_string()),
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
                    "Lease LIST at resourceVersion {requested} expired before {oldest_available}"
                ),
            )),
        }
    }
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
    pub(crate) resource_reads: Arc<dyn ClusterResourceRead>,
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
        resource_reads,
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
    let store = DatastoreNodeLifecycleStore { resource_reads };
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
