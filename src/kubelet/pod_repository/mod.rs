//! `PodRepository` — single production boundary for `v1/Pod` persistence.
//!
//! The repository owns kubelet lifecycle, workload-controller, accounting-
//! controller, API pod subresource, AND the main API pod create / update /
//! patch / delete / list paths. `("v1","Pod",...)` does not appear as a
//! `DatastoreBackend` argument outside
//! [`klights_kubelet::pod_repository::store::PodStore`].
//!
//! Internal services depend on `Arc<PodStore>` rather than
//! `DatastoreHandle`, which localizes the pod-shaped DB boundary to a
//! single file. Network-runtime tables (`pod_network`, `sandbox`) and
//! [`klights_network_api::Datapath::cni_add`] /
//! [`klights_network_api::Datapath::cni_del`] calls remain with their existing owners and
//! are not policed by this boundary.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
#[cfg(any(test, feature = "pod-repository-test-support"))]
use tokio::sync::broadcast;

#[cfg(test)]
use crate::datastore::DatastoreHandle;
use crate::kubelet::pod_runtime::deletion_finalizer::PodDeletionFinalizer;
use crate::kubelet::pod_runtime::service::PodDeletionFinalizeResult;
use klights_cluster_core::Resource;
#[cfg(test)]
use klights_controllers::side_effects::SideEffectMetrics;
#[cfg(test)]
use klights_controllers::side_effects::SideEffectRegistry;
use klights_leader_api::LeaderResourceQuery;
use klights_leader_api::{ResourceGetRequest, ResourceListRequest, ResourceQueryConsistency};
use klights_pod_api::PodRepositoryError;
use klights_reconcile_api::{GcPodDeleteRequest, GcPodDeleteSink};
use klights_supervisor::TaskSupervisor;
use klights_types::ResourceKey;
#[cfg(any(test, feature = "pod-repository-test-support"))]
use klights_watch::WatchEvent;

#[cfg(test)]
mod workqueue_tests;

fn resource_list_from_leader(
    result: klights_leader_api::ResourceListResult,
) -> std::result::Result<klights_pod_api::PodListResult, PodRepositoryError> {
    let (items, resource_version, _position, continue_token, remaining_item_count) =
        result.into_parts();
    klights_pod_api::PodListResult::try_new(
        items,
        resource_version,
        continue_token,
        remaining_item_count,
    )
}

#[cfg(test)]
struct TestDatastorePodNetworkCache {
    node_local: Option<std::sync::Arc<crate::datastore::node_local::NodeLocalStores>>,
}

#[cfg(test)]
pub(crate) fn test_pod_network_cache(
    node_local: std::sync::Arc<crate::datastore::node_local::NodeLocalStores>,
) -> Arc<dyn klights_node_store::PodNetworkCache> {
    Arc::new(TestDatastorePodNetworkCache {
        node_local: Some(node_local),
    })
}

#[cfg(test)]
pub(crate) fn empty_test_pod_network_cache() -> Arc<dyn klights_node_store::PodNetworkCache> {
    Arc::new(TestDatastorePodNetworkCache { node_local: None })
}

#[cfg(test)]
pub(crate) fn test_assignment_bus() -> Arc<klights_networking::PodNetworkAssignmentBus> {
    Arc::new(klights_networking::PodNetworkAssignmentBus::new())
}

#[cfg(test)]
pub(crate) async fn test_node_local_store(
    supervisor: Arc<TaskSupervisor>,
) -> std::sync::Arc<crate::datastore::node_local::NodeLocalStores> {
    std::sync::Arc::new(
        crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:pod-repository-network-test",
        )
        .await
        .expect("open node-local test store"),
    )
}

#[cfg(test)]
pub(crate) fn pod_repository_for_test(
    db: &crate::datastore::sqlite::Datastore,
) -> Arc<PodRepository> {
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let metrics = SideEffectMetrics::new();
    let side_effects = Arc::new(SideEffectRegistry::new());
    let db_handle: DatastoreHandle = Arc::new(db.clone());
    Arc::new(PodRepository::new(
        db_handle,
        supervisor,
        side_effects,
        metrics,
    ))
}

#[cfg(test)]
impl klights_node_store::PodNetworkCache for TestDatastorePodNetworkCache {
    fn get_network_for_uid(
        &self,
        pod_uid: klights_node_store::PodUidKey,
    ) -> klights_node_store::CacheNetworkFuture<'_, Option<klights_node_store::PodNetworkEndpoint>>
    {
        match &self.node_local {
            Some(node_local) => klights_node_store::PodNetworkCache::get_network_for_uid(
                node_local.as_ref(),
                pod_uid,
            ),
            None => Box::pin(async { Ok(None) }),
        }
    }

    fn get_network_for_pod(
        &self,
        pod: klights_types::PodIdentity,
    ) -> klights_node_store::CacheNetworkFuture<'_, Option<klights_node_store::PodNetworkEndpoint>>
    {
        match &self.node_local {
            Some(node_local) => {
                klights_node_store::PodNetworkCache::get_network_for_pod(node_local.as_ref(), pod)
            }
            None => Box::pin(async { Ok(None) }),
        }
    }

    fn get_network_for_sandbox(
        &self,
        sandbox_id: klights_node_store::SandboxKey,
    ) -> klights_node_store::CacheNetworkFuture<'_, Option<klights_node_store::PodNetworkEndpoint>>
    {
        match &self.node_local {
            Some(node_local) => klights_node_store::PodNetworkCache::get_network_for_sandbox(
                node_local.as_ref(),
                sandbox_id,
            ),
            None => Box::pin(async { Ok(None) }),
        }
    }

    fn get_network_for_assignment(
        &self,
        sandbox_id: klights_node_store::SandboxKey,
        pod: klights_types::PodIdentity,
    ) -> klights_node_store::CacheNetworkFuture<'_, Option<klights_node_store::PodNetworkEndpoint>>
    {
        match &self.node_local {
            Some(node_local) => klights_node_store::PodNetworkCache::get_network_for_assignment(
                node_local.as_ref(),
                sandbox_id,
                pod,
            ),
            None => Box::pin(async { Ok(None) }),
        }
    }

    fn delete_network_for_sandbox(
        &self,
        sandbox_id: klights_node_store::SandboxKey,
    ) -> klights_node_store::CacheNetworkFuture<'_, ()> {
        match &self.node_local {
            Some(node_local) => klights_node_store::PodNetworkCache::delete_network_for_sandbox(
                node_local.as_ref(),
                sandbox_id,
            ),
            None => Box::pin(async { Ok(()) }),
        }
    }

    fn delete_network_if_matches(
        &self,
        request: klights_node_store::PodNetworkAllocationRequest,
    ) -> klights_node_store::CacheNetworkFuture<'_, bool> {
        match &self.node_local {
            Some(node_local) => klights_node_store::PodNetworkCache::delete_network_if_matches(
                node_local.as_ref(),
                request,
            ),
            None => Box::pin(async { Ok(false) }),
        }
    }

    fn list_network_assignments(
        &self,
    ) -> klights_node_store::CacheNetworkFuture<
        '_,
        Vec<klights_node_store::PodNetworkAssignmentSnapshot>,
    > {
        match &self.node_local {
            Some(node_local) => {
                klights_node_store::PodNetworkCache::list_network_assignments(node_local.as_ref())
            }
            None => Box::pin(async { Ok(Vec::new()) }),
        }
    }
}

pub mod background;
pub mod delete_coordinator;
pub mod facade;
pub mod watch;

#[cfg(test)]
pub(crate) use crate::pod_repository_composition::PodRepositoryBuildConfig;
#[cfg(test)]
pub(crate) use crate::pod_repository_composition::PodSchedulingMode;
use klights_kubelet::pod_repository::{
    PodNetworkAssignmentQuery, PodNetworkAssignmentRequest, PodStatusUpdate, RuntimeReconcileStatus,
};

#[cfg(test)]
#[async_trait]
pub(crate) trait PodQueryTestExt: klights_pod_api::PodQuery {
    async fn get_pod(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        klights_pod_api::PodQuery::get_pod(
            self,
            klights_pod_api::PodGetRequest::try_by_name(namespace, name)?,
        )
        .await
        .map_err(Into::into)
    }
}

#[cfg(test)]
impl<T> PodQueryTestExt for T where T: klights_pod_api::PodQuery + ?Sized {}

use background::PodRepositoryBackground;
use delete_coordinator::PodDeleteCoordinator;
use klights_kubelet::pod_repository::status;
use klights_kubelet::pod_repository::store::PodStore;
use klights_kubelet::pod_repository::workqueue::PodWorkqueue;
use klights_reconcile_api::{PodEvictionAdmissionSink, PodGcReconcileSink, PodPdbReconcileSink};
use watch::PodWatchService;

pub(crate) struct PodRepositoryAdapterDependencies {
    pub store: Arc<PodStore>,
    pub supervisor: Arc<TaskSupervisor>,
    pub delete_coordinator: Arc<PodDeleteCoordinator>,
}

pub(crate) struct PodRepositoryRuntimeDependencies {
    pub supervisor: Arc<TaskSupervisor>,
    pub metrics: Arc<dyn klights_reconcile_api::ReconcileFailureMetrics>,
    pub wall_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
}

pub(crate) struct PodRepositoryCoreDependencies {
    pub store: Arc<PodStore>,
    pub status_persistence: Arc<dyn klights_pod_api::PodStatusPersistence>,
    pub metadata_persistence: Arc<dyn klights_pod_api::PodPersistence>,
    pub workqueue: Arc<PodWorkqueue>,
}

pub(crate) struct PodRepositoryNetworkDependencies {
    pub assignment_query: Arc<dyn PodNetworkAssignmentQuery>,
    pub host_ip: klights_kubelet::context::HostIpState,
}

pub(crate) struct PodRepositoryDeliveryDependencies {
    pub outbox: Option<Arc<klights_kubelet::outbox::Outbox>>,
    pub cluster_api: Option<Arc<dyn LeaderResourceQuery>>,
    pub remote_delivery_required: bool,
    pub bound_pod_finalization: Arc<dyn klights_pod_api::BoundPodFinalization>,
    #[cfg(feature = "pod-repository-test-support")]
    pub test_local_bound_finalization: Option<Arc<dyn klights_pod_api::BoundPodFinalization>>,
}

pub(crate) struct PodRepositoryAdapters {
    pub gc_delete: Arc<dyn GcPodDeleteSink>,
    pub gc_reconcile: Arc<dyn PodGcReconcileSink>,
    pub pdb_reconcile: Arc<dyn PodPdbReconcileSink>,
    pub eviction_admission: Arc<dyn PodEvictionAdmissionSink>,
    pub namespace_bootstrap: Arc<dyn klights_reconcile_api::NamespaceBootstrapSink>,
    pub namespace_termination: Arc<dyn klights_reconcile_api::NamespaceTerminationSink>,
    pub mutation_reconcile: Arc<dyn klights_reconcile_api::PodMutationReconcileSink>,
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    pub test_api: Option<Arc<dyn klights_pod_api::PodApiMutation>>,
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    pub test_subresource: Option<Arc<dyn klights_pod_api::PodSubresourceMutation>>,
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    pub test_scheduling: Option<Arc<dyn klights_pod_api::PodScheduling>>,
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
pub trait PodWatchSource: Send + Sync {
    fn subscribe_pod_watch(&self) -> broadcast::Receiver<WatchEvent>;
}

/// Eight-trait pod persistence repository. Constructed once at process
/// startup by `ApiState`, then shared by every consumer behind narrow
/// trait references.
pub struct PodRepository {
    store: Arc<PodStore>,
    status: status::PodStatusService,
    metadata: klights_kubelet::pod_repository::PodMetadataService,
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    test_subresource: Option<Arc<dyn klights_pod_api::PodSubresourceMutation>>,
    network_svc: Arc<dyn PodNetworkAssignmentQuery>,
    _watch: PodWatchService,
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    test_api: Option<Arc<dyn klights_pod_api::PodApiMutation>>,
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    test_scheduling: Option<Arc<dyn klights_pod_api::PodScheduling>>,
    gc_delete: Arc<dyn GcPodDeleteSink>,
    eviction_admission: Arc<dyn PodEvictionAdmissionSink>,
    namespace_bootstrap: Arc<dyn klights_reconcile_api::NamespaceBootstrapSink>,
    workqueue: Arc<PodWorkqueue>,
    namespace_termination: Arc<dyn klights_reconcile_api::NamespaceTerminationSink>,
    mutation_reconcile: Arc<dyn klights_reconcile_api::PodMutationReconcileSink>,
    supervisor: Arc<TaskSupervisor>,
    outbox: Option<Arc<klights_kubelet::outbox::Outbox>>,
    cluster_api: Option<Arc<dyn LeaderResourceQuery>>,
    host_ip: klights_kubelet::context::HostIpState,
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    deletion_finalizer: Arc<dyn PodDeletionFinalizer>,
    #[cfg(feature = "pod-repository-test-support")]
    test_local_bound_finalization: Option<Arc<dyn klights_pod_api::BoundPodFinalization>>,
}

impl PodRepository {
    #[cfg(test)]
    pub(crate) async fn test_create_pod(
        &self,
        namespace: &str,
        name: &str,
        _node_name: &str,
        body: Value,
    ) -> Result<Resource> {
        let created = klights_pod_api::PodApiMutation::create_pod(
            self,
            klights_pod_api::PodApiCreateRequest {
                namespace: namespace.to_string(),
                body,
                dry_run: false,
            },
        )
        .await?;
        created
            .resource
            .ok_or_else(|| anyhow::anyhow!("test Pod {namespace}/{name} create returned dry-run"))
    }

    #[cfg(test)]
    pub(crate) async fn test_get_pod_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> Result<Option<Resource>> {
        klights_pod_api::PodQuery::get_pod(
            self,
            klights_pod_api::PodGetRequest::try_by_identity(klights_types::PodIdentity::new(
                namespace, name, uid,
            ))?,
        )
        .await
        .map_err(Into::into)
    }

    #[cfg(feature = "pod-repository-test-support")]
    pub(crate) async fn integration_seed_pod(
        &self,
        namespace: &str,
        name: &str,
        pod: Value,
    ) -> Result<Resource> {
        self.store.create(namespace, name, pod).await
    }

    #[cfg(feature = "pod-repository-test-support")]
    pub(crate) async fn integration_read_pod(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Resource>> {
        self.store.get(namespace, name).await
    }

    #[cfg(feature = "pod-repository-test-support")]
    pub(crate) async fn integration_update_pod(
        &self,
        namespace: &str,
        name: &str,
        pod: Value,
        expected_resource_version: i64,
    ) -> Result<Resource> {
        self.store
            .update(namespace, name, pod, expected_resource_version)
            .await
    }

    #[cfg(feature = "pod-repository-test-support")]
    pub(crate) async fn integration_update_pod_status(
        &self,
        namespace: &str,
        name: &str,
        status: Value,
        expected_resource_version: Option<i64>,
    ) -> Result<Resource> {
        self.store
            .integration_update_status(namespace, name, status, expected_resource_version)
            .await
    }

    #[cfg(feature = "pod-repository-test-support")]
    pub(crate) async fn integration_finalize_bound_pod(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> Result<klights_pod_api::BoundPodFinalizationOutcome> {
        let request = klights_pod_api::BoundPodFinalizationRequest::try_new(
            klights_types::PodIdentity::new(namespace, name, uid),
        )?;
        self.test_local_bound_finalization
            .as_ref()
            .expect("test-support root repository requires local bound finalization")
            .finalize_bound_pod(request)
            .await
            .map_err(anyhow::Error::new)
    }

    #[cfg(feature = "pod-repository-test-support")]
    pub(crate) fn integration_has_deferred_runtime_for_uid(&self, pod_uid: &str) -> bool {
        self.status.has_deferred_runtime_for_uid(pod_uid)
    }

    #[cfg(test)]
    pub(crate) fn test_root_api_services(
        &self,
    ) -> (
        Arc<dyn klights_pod_api::PodApiMutation>,
        Arc<dyn klights_pod_api::PodSubresourceMutation>,
    ) {
        (
            self.test_api
                .clone()
                .expect("test repository requires a root Pod API adapter"),
            self.test_subresource
                .clone()
                .expect("test repository requires a root Pod subresource adapter"),
        )
    }

    pub(crate) fn mutation_reconcile_port(
        &self,
    ) -> Arc<dyn klights_reconcile_api::PodMutationReconcileSink> {
        self.mutation_reconcile.clone()
    }

    pub(crate) fn eviction_admission_port(
        &self,
    ) -> Arc<dyn klights_reconcile_api::PodEvictionAdmissionSink> {
        self.eviction_admission.clone()
    }

    pub(crate) fn namespace_bootstrap_port(
        &self,
    ) -> Arc<dyn klights_reconcile_api::NamespaceBootstrapSink> {
        self.namespace_bootstrap.clone()
    }
}

#[derive(Clone)]
struct PodDeletionFinalizerDependencies {
    store: Arc<PodStore>,
    gc_pod_delete_sink: Arc<dyn GcPodDeleteSink>,
    gc_reconcile: Arc<dyn PodGcReconcileSink>,
    pdb_reconcile: Arc<dyn PodPdbReconcileSink>,
    namespace_termination: Arc<dyn klights_reconcile_api::NamespaceTerminationSink>,
    cluster_api: Option<Arc<dyn LeaderResourceQuery>>,
    outbox: Option<Arc<klights_kubelet::outbox::Outbox>>,
    remote_delivery_required: bool,
    bound_pod_finalization: Arc<dyn klights_pod_api::BoundPodFinalization>,
    mutation_reconcile: Arc<dyn klights_reconcile_api::PodMutationReconcileSink>,
    metrics: Arc<dyn klights_reconcile_api::ReconcileFailureMetrics>,
    supervisor: Arc<TaskSupervisor>,
    deferred_runtime: status::DeferredRuntimeReducerHandle,
}

/// Finalizer decorator that releases repository-private deferred runtime state
/// only after the actor-owned deletion boundary reports a terminal outcome.
/// Pending finalizers and errors retain the observation for the actor retry.
pub(crate) struct DeferredRuntimeCleanupFinalizer {
    inner: Arc<dyn PodDeletionFinalizer>,
    deferred_runtime: status::DeferredRuntimeReducerHandle,
}

impl DeferredRuntimeCleanupFinalizer {
    pub(crate) fn new(
        inner: Arc<dyn PodDeletionFinalizer>,
        deferred_runtime: status::DeferredRuntimeReducerHandle,
    ) -> Self {
        Self {
            inner,
            deferred_runtime,
        }
    }
}

fn compose_pod_deletion_finalizer(
    dependencies: PodDeletionFinalizerDependencies,
) -> Arc<dyn PodDeletionFinalizer> {
    let runtime_deletion_finalizer =
        crate::kubelet::pod_runtime::deletion_finalizer::compose_real_pod_deletion_finalizer(
            crate::kubelet::pod_runtime::deletion_finalizer::RealPodDeletionFinalizerDependencies {
                pod_query: dependencies.store,
                gc_pod_delete_sink: dependencies.gc_pod_delete_sink,
                gc_reconcile: dependencies.gc_reconcile,
                pdb_reconcile: dependencies.pdb_reconcile,
                namespace_termination: dependencies.namespace_termination,
                cluster_api: dependencies.cluster_api,
                outbox: dependencies
                    .outbox
                    .map(|outbox| outbox as Arc<dyn klights_leader_api::NodeOutbox>),
                remote_delivery_required: dependencies.remote_delivery_required,
                bound_pod_finalization: dependencies.bound_pod_finalization,
                mutation_reconcile: dependencies.mutation_reconcile,
                metrics: dependencies.metrics,
                supervisor: dependencies.supervisor,
            },
        );
    Arc::new(DeferredRuntimeCleanupFinalizer::new(
        runtime_deletion_finalizer,
        dependencies.deferred_runtime,
    ))
}

#[async_trait]
impl PodDeletionFinalizer for DeferredRuntimeCleanupFinalizer {
    async fn finalize_after_actor_cleanup(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
    ) -> Result<PodDeletionFinalizeResult> {
        let result = self.inner.finalize_after_actor_cleanup(key).await?;
        if matches!(
            result,
            PodDeletionFinalizeResult::DeletedOrAlreadyGone | PodDeletionFinalizeResult::Queued
        ) {
            self.deferred_runtime.forget(&key.uid);
        }
        Ok(result)
    }
}

impl PodRepository {
    #[cfg(test)]
    pub fn new(
        db: DatastoreHandle,
        supervisor: Arc<TaskSupervisor>,
        side_effects: Arc<SideEffectRegistry>,
        metrics: Arc<SideEffectMetrics>,
    ) -> Self {
        Self::new_with_scheduling_mode(
            db,
            supervisor,
            side_effects,
            metrics,
            PodSchedulingMode::InlineSingleNode,
        )
    }

    pub fn sandbox_gc_dirty_counter(&self) -> Arc<std::sync::atomic::AtomicUsize> {
        self.store.sandbox_gc_dirty_counter()
    }

    pub(crate) fn host_ip_state(&self) -> klights_kubelet::context::HostIpState {
        self.host_ip.clone()
    }

    pub fn outbox(&self) -> Option<&klights_kubelet::outbox::Outbox> {
        self.outbox.as_deref()
    }

    #[cfg(test)]
    pub fn new_with_scheduling_mode(
        db: DatastoreHandle,
        supervisor: Arc<TaskSupervisor>,
        side_effects: Arc<SideEffectRegistry>,
        metrics: Arc<SideEffectMetrics>,
        scheduling_mode: PodSchedulingMode,
    ) -> Self {
        Self::new_with_scheduling_mode_and_outbox(
            db,
            supervisor,
            side_effects,
            metrics,
            scheduling_mode,
            None,
        )
    }

    #[cfg(test)]
    pub fn new_with_scheduling_mode_and_outbox(
        db: DatastoreHandle,
        supervisor: Arc<TaskSupervisor>,
        side_effects: Arc<SideEffectRegistry>,
        metrics: Arc<SideEffectMetrics>,
        scheduling_mode: PodSchedulingMode,
        outbox: Option<Arc<klights_kubelet::node_outbox::Outbox>>,
    ) -> Self {
        let network_cache = empty_test_pod_network_cache();
        let assignment_bus = test_assignment_bus();
        Self::new_with_network_events(
            db,
            supervisor,
            side_effects,
            metrics,
            network_cache,
            assignment_bus,
            scheduling_mode,
            outbox,
        )
    }

    #[cfg(test)]
    pub fn new_with_scheduling_mode_outbox_and_cluster_api(
        db: DatastoreHandle,
        supervisor: Arc<TaskSupervisor>,
        side_effects: Arc<SideEffectRegistry>,
        metrics: Arc<SideEffectMetrics>,
        scheduling_mode: PodSchedulingMode,
        outbox: Option<Arc<klights_kubelet::node_outbox::Outbox>>,
        cluster_api: Arc<dyn LeaderResourceQuery>,
    ) -> Self {
        let pod_network_cache = empty_test_pod_network_cache();
        let assignment_waiter = test_assignment_bus();
        Self::new_with_network_events_and_cluster_api(PodRepositoryBuildConfig {
            db,
            pod_workqueue_store: None,
            supervisor,
            side_effects,
            metrics,
            pod_network_cache,
            assignment_waiter,
            scheduling_mode,
            outbox,
            cluster_api: Some(cluster_api),
            remote_delivery_required: true,
            controller_identity: crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity(
            ),
            #[cfg(not(test))]
            api_identity: Arc::new(crate::bootstrap::controller_adapters::system_identity_adapter::SystemIdentityGenerator),
            scheduler_bind_gate: None,
            #[cfg(not(test))]
            gc_coordination: Arc::new(klights_controllers::ControllerCoordination::new()),
        })
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_network_events(
        db: DatastoreHandle,
        supervisor: Arc<TaskSupervisor>,
        side_effects: Arc<SideEffectRegistry>,
        metrics: Arc<SideEffectMetrics>,
        pod_network_cache: Arc<dyn klights_node_store::PodNetworkCache>,
        assignment_waiter: Arc<dyn klights_network_api::PodNetworkAssignmentWaiter>,
        scheduling_mode: PodSchedulingMode,
        outbox: Option<Arc<klights_kubelet::node_outbox::Outbox>>,
    ) -> Self {
        Self::new_with_network_events_and_cluster_api(PodRepositoryBuildConfig {
            db,
            pod_workqueue_store: None,
            supervisor,
            side_effects,
            metrics,
            pod_network_cache,
            assignment_waiter,
            scheduling_mode,
            outbox,
            cluster_api: None,
            remote_delivery_required: false,
            controller_identity: crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity(
            ),
            #[cfg(not(test))]
            api_identity: Arc::new(crate::bootstrap::controller_adapters::system_identity_adapter::SystemIdentityGenerator),
            scheduler_bind_gate: None,
            #[cfg(not(test))]
            gc_coordination: Arc::new(klights_controllers::ControllerCoordination::new()),
        })
    }

    #[cfg(test)]
    fn new_with_network_events_and_cluster_api(config: PodRepositoryBuildConfig) -> Self {
        let parts = Self::build_parts(config);
        // Ordinary constructors retain lazy queue startup on first enqueue.
        parts.repository
    }

    /// Build `PodRepository` and its deferred-startup services without
    /// calling `workqueue.start()`. The returned `PodRepositoryBackground`
    /// must be started after lifecycle wiring is complete (Task 4.2).
    #[cfg(test)]
    pub fn build_parts(config: PodRepositoryBuildConfig) -> facade::PodRepositoryParts {
        crate::pod_repository_composition::build_pod_repository_parts(config, None).repository_parts
    }

    pub(crate) fn build_parts_with_adapters(
        core: PodRepositoryCoreDependencies,
        runtime: PodRepositoryRuntimeDependencies,
        network: PodRepositoryNetworkDependencies,
        delivery: PodRepositoryDeliveryDependencies,
        adapters: PodRepositoryAdapters,
    ) -> facade::PodRepositoryParts {
        let PodRepositoryCoreDependencies {
            store,
            status_persistence,
            metadata_persistence,
            workqueue,
        } = core;
        let PodRepositoryRuntimeDependencies {
            supervisor,
            metrics,
            wall_clock,
        } = runtime;
        let PodRepositoryNetworkDependencies {
            assignment_query,
            host_ip,
        } = network;
        let PodRepositoryDeliveryDependencies {
            outbox,
            cluster_api,
            remote_delivery_required,
            bound_pod_finalization,
            #[cfg(feature = "pod-repository-test-support")]
            test_local_bound_finalization,
        } = delivery;
        workqueue.set_namespace_termination_sink(adapters.namespace_termination.clone());
        let gc_reconcile = adapters.gc_reconcile;
        let pdb_reconcile = adapters.pdb_reconcile;
        let eviction_admission = adapters.eviction_admission;
        let namespace_bootstrap = adapters.namespace_bootstrap;
        let namespace_termination = adapters.namespace_termination;
        let mutation_reconcile = adapters.mutation_reconcile;
        let status = status::PodStatusService::new(status::PodStatusServiceDependencies {
            pod_query: store.clone(),
            status_persistence,
            mutation_reconcile: mutation_reconcile.clone(),
            outbox: outbox.clone(),
            remote_delivery_required,
            cluster_api: cluster_api.clone(),
            host_ip: host_ip.clone(),
            wall_clock: wall_clock.clone(),
        });
        let metadata = klights_kubelet::pod_repository::PodMetadataService::new(
            klights_kubelet::pod_repository::PodMetadataDependencies {
                persistence: metadata_persistence,
                outbox: outbox.clone(),
                remote_delivery_required,
                mutation_reconcile: mutation_reconcile.clone(),
                wall_clock: wall_clock.clone(),
            },
        );
        let watch = PodWatchService::new(store.clone());
        let gc_pod_delete_sink = adapters.gc_delete.clone();
        workqueue.set_remote_pod_delete_resignal_sink(Arc::downgrade(&gc_pod_delete_sink));

        let deletion_finalizer_dependencies = PodDeletionFinalizerDependencies {
            store: store.clone(),
            gc_pod_delete_sink,
            gc_reconcile,
            pdb_reconcile: pdb_reconcile.clone(),
            namespace_termination: namespace_termination.clone(),
            cluster_api: cluster_api.clone(),
            outbox: outbox.clone(),
            remote_delivery_required,
            bound_pod_finalization: bound_pod_finalization.clone(),
            mutation_reconcile: mutation_reconcile.clone(),
            metrics: metrics.clone(),
            supervisor: supervisor.clone(),
            deferred_runtime: status.deferred_runtime_handle(),
        };
        #[cfg(any(test, feature = "pod-repository-test-support"))]
        let deletion_finalizer =
            compose_pod_deletion_finalizer(deletion_finalizer_dependencies.clone());

        let repository = Self {
            store,
            status,
            metadata,
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            test_subresource: adapters.test_subresource,
            network_svc: assignment_query,
            _watch: watch,
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            test_api: adapters.test_api,
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            test_scheduling: adapters.test_scheduling,
            gc_delete: adapters.gc_delete,
            eviction_admission,
            namespace_bootstrap,
            workqueue: workqueue.clone(),
            namespace_termination: namespace_termination.clone(),
            mutation_reconcile,
            supervisor,
            outbox,
            cluster_api,
            host_ip,
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            deletion_finalizer,
            #[cfg(feature = "pod-repository-test-support")]
            test_local_bound_finalization,
        };
        let background = PodRepositoryBackground::new(workqueue);
        facade::PodRepositoryParts::new(repository, background, deletion_finalizer_dependencies)
    }

    #[cfg(any(test, feature = "pod-repository-test-support"))]
    pub async fn finalize_pod_deletion_after_actor_cleanup(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
    ) -> Result<bool> {
        let key = crate::kubelet::pod_runtime::service::PodRuntimeKey::new(ns, name, uid);
        match self
            .deletion_finalizer
            .finalize_after_actor_cleanup(&key)
            .await?
        {
            PodDeletionFinalizeResult::DeletedOrAlreadyGone => Ok(true),
            PodDeletionFinalizeResult::Queued => Ok(false),
            PodDeletionFinalizeResult::FinalizersPending => Ok(false),
        }
    }

    #[cfg(any(test, feature = "pod-repository-test-support"))]
    pub fn deletion_finalizer(
        &self,
    ) -> Arc<dyn crate::kubelet::pod_runtime::deletion_finalizer::PodDeletionFinalizer> {
        self.deletion_finalizer.clone()
    }

    pub async fn enqueue_actor_deletes_for_terminating_namespace(
        &self,
        namespace: &str,
    ) -> Result<()> {
        self.workqueue
            .enqueue_actor_deletes_for_terminating_namespace(namespace)
            .await
    }

    pub fn set_pod_lifecycle_router_for_node(
        &self,
        router: Arc<klights_kubelet::pod_lifecycle_router::PodLifecycleRouter>,
        local_node_name: String,
    ) {
        self.workqueue
            .set_lifecycle_router_for_node(router, local_node_name);
    }

    pub async fn enqueue_namespace_termination(
        &self,
        namespace: String,
        uid: String,
    ) -> Result<()> {
        self.workqueue
            .enqueue_namespace_termination(namespace, uid)
            .await
    }

    /// Spawn async namespace-termination reconciliation after a pod status or
    /// metadata write.
    ///
    /// Both operations are derived-state maintenance that must not block
    /// the caller (kubelet status writer, controller pod writer). The
    /// spawned task runs on the TaskSupervisor under `Background` so it
    /// is visible on the admin diagnostics API and participates in
    /// graceful shutdown.
    async fn spawn_post_write_maintenance(&self, namespace: &str) {
        let namespace_termination = self.namespace_termination.clone();
        let ns = namespace.to_string();
        let _ = self
            .supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Background,
                format!("post_write_maintenance/{ns}"),
                async move {
                    if let Err(err) = namespace_termination
                        .reconcile_namespace_termination(
                            klights_reconcile_api::NamespaceTerminationRequest {
                                namespace: ns.clone(),
                                expected_uid: None,
                            },
                        )
                        .await
                    {
                        tracing::warn!(
                            namespace = %ns,
                            error = ?err,
                            "post-write namespace termination reconcile failed"
                        );
                    }
                },
            )
            .await;
    }

    async fn finish_status_write(
        &self,
        namespace: &str,
        result: status::PodStatusWriteResult,
        context: &'static str,
    ) -> Resource {
        let resource = result.resource;
        if result.changed {
            if result.endpoint_state_changed {
                let _ = self
                    .mutation_reconcile
                    .reconcile_pod_mutation(
                        klights_reconcile_api::PodMutationReconcileRequest::RunHooks {
                            pod: resource.clone(),
                            named_hook: Some("pdb_reconcile"),
                            context,
                        },
                    )
                    .await;
            }
            self.spawn_post_write_maintenance(namespace).await;
        }
        resource
    }

    #[cfg(any(test, feature = "pod-repository-test-support"))]
    pub async fn schedule_pending_pod(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Resource>> {
        self.test_scheduling
            .as_deref()
            .expect("test scheduler requires the neutral Pod scheduling port")
            .schedule_pending_pod(namespace.to_string(), name.to_string())
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))
    }

    #[cfg(any(test, feature = "pod-repository-test-support"))]
    pub async fn bind_pod_from_api(
        &self,
        namespace: &str,
        name: &str,
        binding: serde_json::Value,
        dry_run: bool,
    ) -> Result<()> {
        self.test_api
            .as_deref()
            .expect("test bind requires the neutral Pod API port")
            .bind_pod(klights_pod_api::PodBindingRequest {
                namespace: namespace.to_string(),
                name: name.to_string(),
                binding,
                dry_run,
            })
            .await
            .map_err(|error| anyhow::anyhow!("{error:?}"))
    }

    #[cfg(any(test, feature = "pod-repository-test-support"))]
    pub async fn schedule_all_unbound_pods(&self) -> Result<()> {
        self.test_scheduling
            .as_deref()
            .expect("test scheduler requires the neutral Pod scheduling port")
            .schedule_all_unbound_pods()
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))
    }

    /// Enqueue the owning Job for asynchronous reconciliation after a pod
    /// reaches a terminal phase or is marked failed.
    ///
    /// This replaces the old synchronous `reconcile_job_for_pod_owner` path
    /// that called `klights_controllers::job::reconcile_job()` inline, blocking the
    /// pod watcher. The async enqueue gives the Job controller exponential
    /// backoff retry and keeps the watcher responsive.
    ///
    /// No-op when the pod has no Job owner or when the controller dispatcher
    /// is not yet bound.
    pub async fn enqueue_job_reconcile_for_pod(&self, pod: &Value) {
        if let Err(err) = self
            .mutation_reconcile
            .reconcile_pod_mutation(
                klights_reconcile_api::PodMutationReconcileRequest::EnqueueJobOwner {
                    pod: Resource::from_data_lossy(Arc::new(pod.clone())),
                },
            )
            .await
        {
            tracing::warn!(error = %err, "failed to enqueue Job reconcile for terminal Pod");
        }
    }
}

impl PodRepository {
    /// Overlay the node-local status checkpoint onto a worker fresh read so the
    /// worker observes its OWN just-written status (read-your-own-write).
    ///
    /// A worker's status writes propagate to the leader asynchronously through
    /// the outbox. Under real inter-node latency a plain leader read-back races
    /// ahead of that write landing and returns a stale phase, which made
    /// `finalize_startup` loop on `Unconfirmed` (and similarly stalled the
    /// deletion confirm path), slowing status convergence and foreground-GC
    /// deletion to the point of conformance-test timeout on a two-VM cluster.
    ///
    /// This is the same merge the status read path already performs
    /// (`PodStatusService::read_current_pod`). The checkpoint only ever reflects
    /// state the worker itself authored and self-clears once the leader catches
    /// up, so it can never surface more than the worker already knows. Only used
    /// on the worker (cluster_api set); the leader reads the cluster store
    /// directly and needs no overlay.
    async fn overlay_local_status_checkpoint(
        &self,
        pod: Option<Resource>,
    ) -> Result<Option<Resource>> {
        match (pod, &self.outbox) {
            (Some(pod), Some(outbox)) => Ok(Some(outbox.merge_pod_status_checkpoint(pod).await?)),
            (other, _) => Ok(other),
        }
    }
}

impl klights_pod_api::PodQuery for PodRepository {
    fn get_pod(
        &self,
        request: klights_pod_api::PodGetRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Option<Resource>> {
        Box::pin(async move {
            let pod = if let Some(cluster_api) = &self.cluster_api {
                let pod = cluster_api
                    .get_resource(
                        ResourceGetRequest::try_new(
                            pod_resource_key(request.namespace(), request.name()),
                            ResourceQueryConsistency::LeaderFresh,
                        )
                        .map_err(|error| PodRepositoryError::unavailable(error.to_string()))?,
                    )
                    .await
                    .map_err(|error| PodRepositoryError::unavailable(error.to_string()))?;
                self.overlay_local_status_checkpoint(pod)
                    .await
                    .map_err(|error| PodRepositoryError::unavailable(error.to_string()))?
            } else {
                self.store
                    .get(request.namespace(), request.name())
                    .await
                    .map_err(|error| PodRepositoryError::unavailable(error.to_string()))?
            };
            Ok(match request.uid() {
                Some(uid) => pod.filter(|pod| pod.uid == uid),
                None => pod,
            })
        })
    }

    fn list_pods(
        &self,
        request: klights_pod_api::PodListRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodListResult> {
        Box::pin(async move {
            if let Some(cluster_api) = &self.cluster_api {
                let list = cluster_api
                    .list_resources(
                        ResourceListRequest::try_new(
                            "v1",
                            "Pod",
                            request.namespace().map(str::to_string),
                            request.label_selector().map(str::to_string),
                            request.field_selector().map(str::to_string),
                            request.limit(),
                            request.continue_token().map(str::to_string),
                            ResourceQueryConsistency::LeaderFresh,
                        )
                        .map_err(|error| PodRepositoryError::unavailable(error.to_string()))?,
                    )
                    .await
                    .map_err(|error| PodRepositoryError::unavailable(error.to_string()))?;
                return resource_list_from_leader(list);
            }
            self.store
                .list(
                    request.namespace(),
                    request.label_selector(),
                    request.field_selector(),
                    request.limit(),
                    request.continue_token(),
                )
                .await
                .map_err(|error| PodRepositoryError::unavailable(error.to_string()))
        })
    }

    fn list_pods_by_owner_uid(
        &self,
        request: klights_pod_api::PodOwnerListRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Vec<Resource>> {
        Box::pin(async move {
            if self.cluster_api.is_some() {
                let pods = klights_pod_api::PodQuery::list_pods(
                    self,
                    klights_pod_api::PodListRequest::try_new(
                        Some(request.namespace().to_string()),
                        None,
                        None,
                        None,
                        None,
                    )?,
                )
                .await?;
                return Ok(pods
                    .into_parts()
                    .0
                    .into_iter()
                    .filter(|pod| pod_has_owner_uid(&pod.data, request.owner_uid()))
                    .collect());
            }
            self.store
                .list_by_owner(request.namespace(), request.owner_uid())
                .await
                .map_err(|error| PodRepositoryError::unavailable(error.to_string()))
        })
    }
}

impl klights_pod_api::PodUpdate for PodRepository {
    fn update_pod(
        &self,
        request: klights_pod_api::PodUpdateRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Resource> {
        self.metadata.update_pod_from(self, request)
    }
}

impl klights_pod_api::PodSnapshotQuery for PodRepository {
    fn snapshot_pods(
        &self,
        request: klights_pod_api::PodSnapshotListRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodSnapshotListOutcome> {
        Box::pin(async move {
            self.store
                .snapshot_list(request)
                .await
                .map_err(|error| PodRepositoryError::unavailable(error.to_string()))
        })
    }
}

impl klights_reconcile_api::NamespaceTerminationQueueSink for PodRepository {
    fn enqueue_namespace_termination(
        &self,
        namespace: String,
        uid: String,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'_> {
        Box::pin(async move {
            self.workqueue
                .enqueue_namespace_termination(namespace, uid)
                .await
                .map_err(|error| {
                    klights_reconcile_api::ReconcileSinkError::unavailable(error.to_string())
                })
        })
    }
}

fn pod_resource_key(ns: &str, name: &str) -> ResourceKey {
    ResourceKey {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some(ns.to_string()),
        name: name.to_string(),
    }
}

fn pod_has_owner_uid(pod: &Value, owner_uid: &str) -> bool {
    pod.pointer("/metadata/ownerReferences")
        .and_then(|owners| owners.as_array())
        .is_some_and(|owners| {
            owners
                .iter()
                .any(|owner| owner.get("uid").and_then(|uid| uid.as_str()) == Some(owner_uid))
        })
}

#[async_trait]
impl status::PodStatusWriter for PodRepository {
    async fn set_pod_status(
        &self,
        ns: &str,
        name: &str,
        update: PodStatusUpdate,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        let result = self
            .status
            .set_pod_status(ns, name, &update, expected_rv)
            .await?;
        Ok(self.finish_status_write(ns, result, "pod_status_set").await)
    }

    async fn set_pod_status_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        update: PodStatusUpdate,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        let result = self
            .status
            .set_pod_status_for_uid(ns, name, pod_uid, update, expected_rv)
            .await?;
        Ok(self
            .finish_status_write(ns, result, "pod_status_set_uid")
            .await)
    }

    async fn apply_runtime_reconcile_status(
        &self,
        ns: &str,
        name: &str,
        update: RuntimeReconcileStatus,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        let result = self
            .status
            .apply_runtime_reconcile_status(ns, name, update, expected_rv)
            .await?;
        Ok(self
            .finish_status_write(ns, result, "pod_runtime_reconcile_status")
            .await)
    }

    async fn apply_runtime_reconcile_status_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        update: RuntimeReconcileStatus,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        let result = self
            .status
            .apply_runtime_reconcile_status_for_uid(ns, name, pod_uid, update, expected_rv)
            .await?;
        Ok(self
            .finish_status_write(ns, result, "pod_runtime_reconcile_status_uid")
            .await)
    }

    async fn mark_start_pending_for_retry_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        error_message: &str,
    ) -> Result<Resource> {
        let result = self
            .status
            .mark_start_pending_for_retry_for_uid(ns, name, pod_uid, error_message)
            .await?;
        Ok(self
            .finish_status_write(ns, result, "pod_start_pending_retry")
            .await)
    }

    async fn set_probe_readiness(
        &self,
        ns: &str,
        name: &str,
        container_name: &str,
        ready: bool,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        let result = self
            .status
            .set_probe_readiness(ns, name, container_name, ready, expected_rv)
            .await?;
        Ok(self
            .finish_status_write(ns, result, "pod_probe_readiness")
            .await)
    }

    async fn set_probe_readiness_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        container_name: &str,
        ready: bool,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        let result = self
            .status
            .set_probe_readiness_for_uid(ns, name, pod_uid, container_name, ready, expected_rv)
            .await?;
        Ok(self
            .finish_status_write(ns, result, "pod_probe_readiness_uid")
            .await)
    }
    async fn set_deadline_exceeded(
        &self,
        ns: &str,
        name: &str,
        message: String,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        let result = self
            .status
            .set_deadline_exceeded(ns, name, message, expected_rv)
            .await?;
        Ok(self
            .finish_status_write(ns, result, "pod_deadline_exceeded")
            .await)
    }

    async fn set_deadline_exceeded_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        message: String,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        let result = self
            .status
            .set_deadline_exceeded_for_uid(ns, name, pod_uid, message, expected_rv)
            .await?;
        Ok(self
            .finish_status_write(ns, result, "pod_deadline_exceeded_uid")
            .await)
    }
    async fn apply_ephemeral_container_statuses(
        &self,
        ns: &str,
        name: &str,
        statuses: Vec<Value>,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        let result = self
            .status
            .apply_ephemeral_container_statuses(ns, name, statuses, expected_rv)
            .await?;
        Ok(self
            .finish_status_write(ns, result, "pod_ephemeral_container_statuses")
            .await)
    }

    async fn apply_ephemeral_container_statuses_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        statuses: Vec<Value>,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        let result = self
            .status
            .apply_ephemeral_container_statuses_for_uid(ns, name, pod_uid, statuses, expected_rv)
            .await?;
        Ok(self
            .finish_status_write(ns, result, "pod_ephemeral_container_statuses_uid")
            .await)
    }
    async fn note_container_restart(
        &self,
        ns: &str,
        name: &str,
        container_name: &str,
        terminated: Value,
        expected_rv: Option<i64>,
    ) -> Result<Option<Resource>> {
        let updated = self
            .status
            .note_container_restart(ns, name, container_name, terminated, expected_rv)
            .await?;
        if updated.is_some() {
            self.spawn_post_write_maintenance(ns).await;
        }
        Ok(updated)
    }

    async fn note_container_restart_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        container_name: &str,
        terminated: Value,
        expected_rv: Option<i64>,
    ) -> Result<Option<Resource>> {
        let updated = self
            .status
            .note_container_restart_for_uid(
                ns,
                name,
                pod_uid,
                container_name,
                terminated,
                expected_rv,
            )
            .await?;
        if updated.is_some() {
            self.spawn_post_write_maintenance(ns).await;
        }
        Ok(updated)
    }
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
impl klights_pod_api::PodSubresourceMutation for PodRepository {
    fn replace_status(
        &self,
        request: klights_pod_api::PodStatusReplaceRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Resource> {
        Box::pin(async move {
            let updated = self
                .test_subresource
                .as_deref()
                .expect("test status replace requires the canonical Pod subresource port")
                .replace_status(request)
                .await?;
            let _ = self
                .mutation_reconcile
                .reconcile_pod_mutation(
                    klights_reconcile_api::PodMutationReconcileRequest::RunHooks {
                        pod: updated.clone(),
                        named_hook: None,
                        context: "pod_status_subresource_replace",
                    },
                )
                .await;
            Ok(updated)
        })
    }

    fn patch_status(
        &self,
        request: klights_pod_api::PodStatusPatchRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Resource> {
        Box::pin(async move {
            let updated = self
                .test_subresource
                .as_deref()
                .expect("test status patch requires the canonical Pod subresource port")
                .patch_status(request)
                .await?;
            let _ = self
                .mutation_reconcile
                .reconcile_pod_mutation(
                    klights_reconcile_api::PodMutationReconcileRequest::RunHooks {
                        pod: updated.clone(),
                        named_hook: None,
                        context: "pod_status_subresource_patch",
                    },
                )
                .await;
            Ok(updated)
        })
    }

    fn update_ephemeral_containers(
        &self,
        request: klights_pod_api::PodEphemeralContainersRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Resource> {
        self.test_subresource
            .as_deref()
            .expect("test ephemeral-container update requires the canonical Pod subresource port")
            .update_ephemeral_containers(request)
    }
}

impl PodNetworkAssignmentQuery for PodRepository {
    fn read_pod_network_assignment(
        &self,
        request: PodNetworkAssignmentRequest,
    ) -> klights_kubelet::pod_repository::PodNetworkAssignmentFuture<'_> {
        self.network_svc.read_pod_network_assignment(request)
    }
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
impl klights_pod_api::PodApiMutation for PodRepository {
    fn create_pod(
        &self,
        request: klights_pod_api::PodApiCreateRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodApiCreateResult> {
        self.test_api
            .as_deref()
            .expect("test create requires the canonical Pod API port")
            .create_pod(request)
    }

    fn update_pod(
        &self,
        request: klights_pod_api::PodApiUpdateRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodApiWriteOutcome> {
        self.test_api
            .as_deref()
            .expect("test update requires the canonical Pod API port")
            .update_pod(request)
    }

    fn patch_pod(
        &self,
        request: klights_pod_api::PodApiPatchRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodApiWriteOutcome> {
        self.test_api
            .as_deref()
            .expect("test patch requires the canonical Pod API port")
            .patch_pod(request)
    }

    fn delete_pod(
        &self,
        request: klights_pod_api::PodApiDeleteRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodApiDeleteOutcome> {
        self.test_api
            .as_deref()
            .expect("test delete requires the canonical Pod API port")
            .delete_pod(request)
    }

    fn delete_collection_pods(
        &self,
        request: klights_pod_api::PodApiDeleteCollectionRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, ()> {
        self.test_api
            .as_deref()
            .expect("test collection delete requires the canonical Pod API port")
            .delete_collection_pods(request)
    }

    fn bind_pod(
        &self,
        request: klights_pod_api::PodBindingRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, ()> {
        self.test_api
            .as_deref()
            .expect("test bind requires the canonical Pod API port")
            .bind_pod(request)
    }
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
impl PodWatchSource for PodRepository {
    fn subscribe_pod_watch(&self) -> broadcast::Receiver<WatchEvent> {
        self.store.subscribe_watch()
    }
}

impl GcPodDeleteSink for PodRepository {
    fn request_gc_pod_delete(
        &self,
        request: GcPodDeleteRequest,
    ) -> klights_reconcile_api::GcPodDeleteFuture<'_> {
        self.gc_delete.request_gc_pod_delete(request)
    }
}
