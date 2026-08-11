//! Composition-root wiring for Pod repository API and reconcile adapters.
//!
//! E6: the former root aggregate and broad parts facade are gone. This module
//! owns the flat focused-capability construction result of
//! focused capabilities plus every root-owned adapter that the aggregate
//! previously hid behind its trait impls: the live `RootPodQueryWriter`,
//! the root `PodUpdate`/`PodStatusWriter`/deletion-finalizer wrappers, and
//! the namespace-termination queue sink over the workqueue.

use std::sync::Arc;

use crate::datastore::DatastoreHandle;
use klights_kubelet::pod_repository::background::PodRepositoryBackground;
use klights_kubelet::pod_repository::delete_coordinator::PodDeleteCoordinator;
use klights_kubelet::pod_repository::store::PodStore;
use klights_kubelet::pod_repository::workqueue::{
    PodWorkqueue, PodWorkqueueEntry, PodWorkqueueKind, PodWorkqueuePersistence,
};

impl klights_cluster_store::PodUidPreconditionRead for dyn crate::datastore::DatastoreBackend + '_ {
    fn read_pod_uid_precondition(
        &self,
        request: klights_cluster_store::PodUidPreconditionRequest,
    ) -> klights_cluster_store::PodUidPreconditionFuture<'_> {
        Box::pin(async move {
            let live = self
                .get_resource("v1", "Pod", Some(request.namespace()), request.name())
                .await
                .map_err(|error| {
                    klights_cluster_store::PodUidPreconditionError::retryable(error.to_string())
                })?;
            Ok(match live {
                None => klights_cluster_store::PodUidPreconditionState::Missing,
                Some(pod) if pod.uid == request.expected_uid() => {
                    klights_cluster_store::PodUidPreconditionState::Matches
                }
                Some(pod) => klights_cluster_store::PodUidPreconditionState::Mismatch {
                    actual_uid: pod.uid,
                },
            })
        })
    }
}
use k8s_native_service::{
    PodApiService, PodApiServiceDependencies, PodNativeOrchestration,
    PodNativeOrchestrationDependencies, PodSubresourceService,
};
use klights_controllers::side_effects::SideEffectMetrics;
use klights_controllers::side_effects::SideEffectRegistry;
use klights_leader_api::LeaderResourceQuery;
use klights_pod_api::PodRepositoryError;
use klights_reconcile_api::GcPodDeleteSink;
use klights_supervisor::TaskSupervisor;
use klights_types::{PodIdentity, ResourceKey};

#[cfg(test)]
mod workqueue_tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PodSchedulingMode {
    InlineSingleNode,
    DeferredMultiNodeLeader,
}

#[derive(Clone)]
pub(crate) struct PodRepositoryBuildConfig {
    pub db: DatastoreHandle,
    pub pod_workqueue_store: Option<Arc<dyn klights_node_store::PodWorkqueueStore>>,
    pub supervisor: Arc<TaskSupervisor>,
    pub side_effects: Arc<SideEffectRegistry>,
    pub metrics: Arc<SideEffectMetrics>,
    pub pod_network_cache: Arc<dyn klights_node_store::PodNetworkCache>,
    pub assignment_waiter: Arc<dyn klights_network_api::PodNetworkAssignmentWaiter>,
    pub scheduling_mode: PodSchedulingMode,
    pub outbox: Option<Arc<klights_kubelet::node_outbox::Outbox>>,
    pub cluster_api: Option<Arc<dyn LeaderResourceQuery>>,
    pub remote_delivery_required: bool,
    pub controller_identity: Arc<dyn klights_controllers::ControllerIdentityGenerator>,
    #[cfg(not(test))]
    pub api_identity: Arc<dyn k8s_native_service::ApiIdentityGenerator>,
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    pub(crate) scheduler_bind_gate: Option<
        Arc<crate::bootstrap::composition_adapters::pod_native_adapter::SchedulerBindGateForTest>,
    >,
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    pub(crate) post_write_maintenance_notify: Option<Arc<tokio::sync::Notify>>,
    #[cfg(not(test))]
    pub gc_coordination: Arc<dyn klights_reconcile_api::GcForegroundDeleteCoordination>,
}

#[derive(Clone)]
pub(crate) struct WorkerPodRepositoryBuildConfig {
    pub resource_query: Arc<dyn LeaderResourceQuery>,
    pub pod_workqueue_store: Arc<dyn klights_node_store::PodWorkqueueStore>,
    pub supervisor: Arc<TaskSupervisor>,
    pub metrics: Arc<SideEffectMetrics>,
    pub pod_network_cache: Arc<dyn klights_node_store::PodNetworkCache>,
    pub assignment_waiter: Arc<dyn klights_network_api::PodNetworkAssignmentWaiter>,
    pub outbox: Arc<klights_kubelet::node_outbox::Outbox>,
}

pub(crate) struct PodRepositoryAdapterDependencies {
    pub store: Arc<PodStore>,
    pub supervisor: Arc<TaskSupervisor>,
    pub deletion: Arc<dyn klights_pod_api::PodDeleteOrchestration>,
}

pub(crate) struct PodRepositoryRuntimeDependencies {
    pub supervisor: Arc<TaskSupervisor>,
    pub metrics: Arc<dyn klights_reconcile_api::ReconcileFailureMetrics>,
    pub wall_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    pub post_write_maintenance_notify: Option<Arc<tokio::sync::Notify>>,
}

pub(crate) struct PodRepositoryCoreDependencies {
    pub store: Arc<PodStore>,
    pub status_persistence: Arc<dyn klights_pod_api::PodStatusPersistence>,
    pub metadata_persistence: Arc<dyn klights_pod_api::PodPersistence>,
    pub workqueue: Arc<PodWorkqueue>,
}

pub(crate) struct PodRepositoryNetworkDependencies {
    pub assignment_query: Arc<dyn klights_kubelet::pod_repository::PodNetworkAssignmentQuery>,
    pub host_ip: klights_kubelet::context::HostIpState,
}

pub(crate) struct PodRepositoryDeliveryDependencies {
    pub outbox: Option<Arc<klights_kubelet::outbox::Outbox>>,
    pub cluster_api: Option<Arc<dyn LeaderResourceQuery>>,
    pub remote_delivery_required: bool,
    pub bound_pod_finalization: Arc<dyn klights_pod_api::BoundPodFinalization>,
}

pub(crate) struct PodRepositoryAdapters {
    pub gc_delete: Arc<dyn GcPodDeleteSink>,
    pub gc_reconcile: Arc<dyn klights_reconcile_api::PodGcReconcileSink>,
    pub pdb_reconcile: Arc<dyn klights_reconcile_api::PodPdbReconcileSink>,
    pub eviction_admission: Arc<dyn klights_reconcile_api::PodEvictionAdmissionSink>,
    pub namespace_bootstrap: Arc<dyn klights_reconcile_api::NamespaceBootstrapSink>,
    pub namespace_termination: Arc<dyn klights_reconcile_api::NamespaceTerminationSink>,
    pub mutation_reconcile: Arc<dyn klights_reconcile_api::PodMutationReconcileSink>,
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    pub test_api: Option<Arc<dyn klights_pod_api::PodApiMutation>>,
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    pub test_subresource: Option<Arc<dyn klights_pod_api::PodSubresourceMutation>>,
}

/// Optional API-facing services emitted by the leader composition. Workers
/// deliberately pass the empty value because they have no leader-owned API
/// surface.
struct PodRepositoryApiServices {
    api: Option<Arc<PodApiService>>,
    subresource: Option<Arc<PodSubresourceService>>,
    scheduling: Option<Arc<dyn klights_pod_api::PodScheduling>>,
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
#[allow(dead_code)]
pub(crate) trait PodWatchSource: Send + Sync {
    fn subscribe_pod_watch(&self) -> tokio::sync::broadcast::Receiver<klights_watch::WatchEvent>;
}

struct RootPodRepositoryComposition {
    db: DatastoreHandle,
    resource_query: Arc<dyn LeaderResourceQuery>,
    side_effects: Arc<SideEffectRegistry>,
    metrics: Arc<SideEffectMetrics>,
    gc_coordination: Arc<dyn klights_reconcile_api::GcForegroundDeleteCoordination>,
    wall_clock: Arc<dyn klights_supervisor::WallClock>,
    controller_identity: Arc<dyn klights_controllers::ControllerIdentityGenerator>,
    api_identity: Arc<dyn k8s_native_service::ApiIdentityGenerator>,
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    scheduler_bind_gate: Option<
        Arc<crate::bootstrap::composition_adapters::pod_native_adapter::SchedulerBindGateForTest>,
    >,
}

/// Anonymous focused handoff used only at construction boundaries.  Callers
/// must destructure this tuple immediately and retain individual ports; no
/// named aggregate is allowed to escape into a subsystem or fixture.
#[cfg(any(test, feature = "pod-repository-test-support"))]
type PodRepositoryConstructionResult = (
    Arc<dyn klights_pod_api::PodQuery>,
    Arc<dyn klights_pod_api::PodSnapshotQuery>,
    Arc<dyn klights_pod_api::PodUpdate>,
    Arc<dyn klights_kubelet::pod_repository::status::PodStatusWriter>,
    Arc<PodWorkqueue>,
    Arc<dyn klights_kubelet::pod_repository::PodNetworkAssignmentQuery>,
    klights_kubelet::context::HostIpState,
    PodRepositoryBackground,
    Arc<dyn klights_kubelet::pod_deletion_finalizer::PodDeletionFinalizer>,
    Arc<std::sync::atomic::AtomicUsize>,
    Arc<dyn klights_reconcile_api::PodMutationReconcileSink>,
    Arc<dyn GcPodDeleteSink>,
    Arc<dyn klights_reconcile_api::PodEvictionAdmissionSink>,
    Arc<dyn klights_reconcile_api::NamespaceBootstrapSink>,
    Arc<dyn klights_reconcile_api::NamespaceTerminationQueueSink>,
    Option<Arc<PodApiService>>,
    Option<Arc<PodSubresourceService>>,
    Option<Arc<dyn klights_pod_api::PodScheduling>>,
    Arc<dyn PodWatchSource>,
    Arc<dyn klights_pod_api::BoundPodFinalization>,
    klights_kubelet::pod_repository::status::DeferredRuntimeReducerHandle,
    Option<Arc<dyn klights_pod_api::PodApiMutation>>,
    Option<Arc<dyn klights_pod_api::PodSubresourceMutation>>,
);

#[cfg(not(any(test, feature = "pod-repository-test-support")))]
type PodRepositoryConstructionResult = (
    Arc<dyn klights_pod_api::PodQuery>,
    Arc<dyn klights_pod_api::PodSnapshotQuery>,
    Arc<dyn klights_pod_api::PodUpdate>,
    Arc<dyn klights_kubelet::pod_repository::status::PodStatusWriter>,
    Arc<PodWorkqueue>,
    Arc<dyn klights_kubelet::pod_repository::PodNetworkAssignmentQuery>,
    klights_kubelet::context::HostIpState,
    PodRepositoryBackground,
    Arc<dyn klights_kubelet::pod_deletion_finalizer::PodDeletionFinalizer>,
    Arc<std::sync::atomic::AtomicUsize>,
    Arc<dyn klights_reconcile_api::PodMutationReconcileSink>,
    Arc<dyn GcPodDeleteSink>,
    Arc<dyn klights_reconcile_api::PodEvictionAdmissionSink>,
    Arc<dyn klights_reconcile_api::NamespaceBootstrapSink>,
    Arc<dyn klights_reconcile_api::NamespaceTerminationQueueSink>,
    Option<Arc<PodApiService>>,
    Option<Arc<PodSubresourceService>>,
    Option<Arc<dyn klights_pod_api::PodScheduling>>,
);

#[derive(Clone)]
struct RootPodWorkqueuePersistence {
    node_local: Option<Arc<dyn klights_node_store::PodWorkqueueStore>>,
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    wall_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    test_rows: Arc<std::sync::Mutex<Vec<RootInMemoryPodWorkqueueEntry>>>,
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    test_next_id: Arc<std::sync::atomic::AtomicI64>,
}

#[cfg(test)]
pub(crate) fn test_workqueue_persistence(
    node_local: Arc<dyn klights_node_store::PodWorkqueueStore>,
    wall_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
) -> impl PodWorkqueuePersistence + 'static {
    RootPodWorkqueuePersistence {
        node_local: Some(node_local),
        wall_clock,
        test_rows: Arc::new(std::sync::Mutex::new(Vec::new())),
        test_next_id: Arc::new(std::sync::atomic::AtomicI64::new(1)),
    }
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
#[derive(Clone)]
struct RootInMemoryPodWorkqueueEntry {
    id: i64,
    kind: PodWorkqueueKind,
    namespace: String,
    name: String,
    uid: String,
    payload: serde_json::Value,
    attempt_count: i64,
    next_attempt_at_ms: i64,
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
fn in_memory_row_matches_token(
    row: &RootInMemoryPodWorkqueueEntry,
    token: &klights_node_store::PodWorkqueueLeaseToken,
) -> bool {
    if row.id != token.id().get() || row.next_attempt_at_ms != token.leased_next_due_ms().get() {
        return false;
    }
    match token.identity() {
        klights_node_store::PodWorkIdentity::Pod(pod) => {
            row.kind == PodWorkqueueKind::Pod
                && row.namespace == pod.namespace
                && row.name == pod.name
                && row.uid == pod.uid
        }
        klights_node_store::PodWorkIdentity::Namespace { name, uid } => {
            row.kind == PodWorkqueueKind::Namespace
                && row.namespace.is_empty()
                && row.name == *name
                && row.uid == *uid
        }
    }
}

struct WorkerPodPersistence {
    resource_query: Arc<dyn LeaderResourceQuery>,
}

struct WorkerPodAdapters;

fn worker_reconcile_ok() -> klights_reconcile_api::ReconcileSinkFuture<'static> {
    Box::pin(async { Ok(()) })
}

impl klights_reconcile_api::PodMutationReconcileSink for WorkerPodAdapters {
    fn reconcile_pod_mutation(
        &self,
        _request: klights_reconcile_api::PodMutationReconcileRequest,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'_> {
        worker_reconcile_ok()
    }
}

impl klights_reconcile_api::PodGcReconcileSink for WorkerPodAdapters {
    fn reconcile_owner_references<'a>(
        &'a self,
        _pod: klights_cluster_core::Resource,
        _pod_delete_sink: &'a dyn klights_reconcile_api::GcPodDeleteSink,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'a> {
        worker_reconcile_ok()
    }

    fn cascade_delete_dependents<'a>(
        &'a self,
        _owner: klights_types::PodIdentity,
        _pod_delete_sink: &'a dyn klights_reconcile_api::GcPodDeleteSink,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'a> {
        worker_reconcile_ok()
    }

    fn finalize_foreground_owners<'a>(
        &'a self,
        _deleted_dependent: klights_cluster_core::Resource,
        _pod_delete_sink: &'a dyn klights_reconcile_api::GcPodDeleteSink,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'a> {
        worker_reconcile_ok()
    }
}

impl klights_reconcile_api::PodPdbReconcileSink for WorkerPodAdapters {
    fn reconcile_namespace_pdbs(
        &self,
        _namespace: String,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'_> {
        worker_reconcile_ok()
    }
}

impl klights_reconcile_api::PodEvictionAdmissionSink for WorkerPodAdapters {
    fn admit_pod_eviction(
        &self,
        _request: klights_reconcile_api::PodEvictionAdmissionRequest,
    ) -> klights_reconcile_api::PodEvictionAdmissionFuture<'_> {
        Box::pin(async { Ok(klights_reconcile_api::PodEvictionAdmissionOutcome::Allowed) })
    }
}

impl klights_reconcile_api::NamespaceTerminationSink for WorkerPodAdapters {
    fn reconcile_namespace_termination(
        &self,
        _request: klights_reconcile_api::NamespaceTerminationRequest,
    ) -> klights_reconcile_api::NamespaceTerminationFuture<'_> {
        Box::pin(async { Ok(klights_reconcile_api::NamespaceTerminationOutcome::Finalized) })
    }
}

impl klights_reconcile_api::NamespaceBootstrapSink for WorkerPodAdapters {
    fn create_default_service_account(
        &self,
        _namespace: String,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'_> {
        worker_reconcile_ok()
    }

    fn create_root_ca_config_map(
        &self,
        _namespace: String,
        _ca_certificate: String,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'_> {
        worker_reconcile_ok()
    }
}

impl WorkerPodAdapters {
    fn build(
        dependencies: PodRepositoryAdapterDependencies,
        gc_delete: Arc<dyn klights_reconcile_api::GcPodDeleteSink>,
    ) -> PodRepositoryAdapters {
        let adapter = Arc::new(WorkerPodAdapters);
        let _ = dependencies;
        PodRepositoryAdapters {
            gc_delete,
            gc_reconcile: adapter.clone(),
            pdb_reconcile: adapter.clone(),
            eviction_admission: adapter.clone(),
            namespace_bootstrap: adapter.clone(),
            namespace_termination: adapter.clone(),
            mutation_reconcile: adapter,
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            test_api: None,
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            test_subresource: None,
        }
    }
}

fn worker_persistence_unavailable(operation: &str) -> klights_pod_api::PodRepositoryError {
    klights_pod_api::PodRepositoryError::unavailable(format!(
        "worker Pod persistence cannot perform leader-owned {operation}"
    ))
}

impl klights_pod_api::PodRepositoryReadPersistence for WorkerPodPersistence {
    fn get_persisted_pod(
        &self,
        request: klights_pod_api::PodRepositoryGetRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Option<klights_cluster_core::Resource>> {
        Box::pin(async move {
            self.resource_query
                .get_resource(
                    klights_leader_api::ResourceGetRequest::try_new(
                        klights_types::ResourceKey::new(
                            "v1",
                            "Pod",
                            Some(request.namespace),
                            request.name,
                        ),
                        klights_leader_api::ResourceQueryConsistency::Cached,
                    )
                    .map_err(|error| {
                        klights_pod_api::PodRepositoryError::internal(error.to_string())
                    })?,
                )
                .await
                .map_err(|error| {
                    klights_pod_api::PodRepositoryError::unavailable(error.to_string())
                })
        })
    }

    fn list_persisted_pods(
        &self,
        request: klights_pod_api::PodRepositoryListRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodListResult> {
        Box::pin(async move {
            let result = self
                .resource_query
                .list_resources(
                    klights_leader_api::ResourceListRequest::try_new(
                        "v1",
                        "Pod",
                        request.namespace,
                        request.label_selector,
                        request.field_selector,
                        request.limit,
                        request.continue_token,
                        klights_leader_api::ResourceQueryConsistency::Cached,
                    )
                    .map_err(|error| {
                        klights_pod_api::PodRepositoryError::internal(error.to_string())
                    })?,
                )
                .await
                .map_err(|error| {
                    klights_pod_api::PodRepositoryError::unavailable(error.to_string())
                })?;
            let (items, resource_version, _, continue_token, remaining_item_count) =
                result.into_parts();
            klights_pod_api::PodListResult::try_new(
                items,
                resource_version,
                continue_token,
                remaining_item_count,
            )
        })
    }

    fn snapshot_persisted_pods(
        &self,
        request: klights_pod_api::PodSnapshotListRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodSnapshotListOutcome> {
        Box::pin(async move {
            let list = request.list;
            let result = self
                .list_persisted_pods(klights_pod_api::PodRepositoryListRequest {
                    namespace: list.namespace().map(str::to_string),
                    label_selector: list.label_selector().map(str::to_string),
                    field_selector: list.field_selector().map(str::to_string),
                    limit: list.limit(),
                    continue_token: list.continue_token().map(str::to_string),
                })
                .await?;
            if result.resource_version() < request.snapshot_resource_version {
                return Ok(klights_pod_api::PodSnapshotListOutcome::Current);
            }
            Ok(klights_pod_api::PodSnapshotListOutcome::List(result))
        })
    }

    fn list_persisted_pods_by_owner(
        &self,
        request: klights_pod_api::PodRepositoryOwnerListRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Vec<klights_cluster_core::Resource>> {
        Box::pin(async move {
            let pods = self
                .list_persisted_pods(klights_pod_api::PodRepositoryListRequest {
                    namespace: Some(request.namespace),
                    label_selector: None,
                    field_selector: None,
                    limit: None,
                    continue_token: None,
                })
                .await?;
            Ok(pods
                .into_parts()
                .0
                .into_iter()
                .filter(|pod| {
                    pod.data
                        .pointer("/metadata/ownerReferences")
                        .and_then(serde_json::Value::as_array)
                        .is_some_and(|owners| {
                            owners.iter().any(|owner| {
                                owner.get("uid").and_then(serde_json::Value::as_str)
                                    == Some(request.owner_uid.as_str())
                            })
                        })
                })
                .collect())
        })
    }
}

impl klights_pod_api::PodPersistence for WorkerPodPersistence {
    fn create_pod(
        &self,
        _request: klights_pod_api::PodPersistenceCreateRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async { Err(worker_persistence_unavailable("create")) })
    }

    fn replace_pod(
        &self,
        _request: klights_pod_api::PodPersistenceReplaceRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async { Err(worker_persistence_unavailable("replace")) })
    }

    fn replace_pod_including_status(
        &self,
        _request: klights_pod_api::PodPersistenceReplaceRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async { Err(worker_persistence_unavailable("scheduler replace")) })
    }

    fn patch_pod_metadata(
        &self,
        _request: klights_pod_api::PodMetadataPatchRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async { Err(worker_persistence_unavailable("metadata patch")) })
    }
}

impl klights_pod_api::PodStatusPersistence for WorkerPodPersistence {
    fn write_pod_status(
        &self,
        _request: klights_pod_api::PodStatusWriteRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async { Err(worker_persistence_unavailable("status write")) })
    }
}

impl klights_pod_api::PodRepositoryWritePersistence for WorkerPodPersistence {
    fn create_persisted_pod(
        &self,
        _request: klights_pod_api::PodRepositoryCreateRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async { Err(worker_persistence_unavailable("create")) })
    }

    fn replace_persisted_pod(
        &self,
        _request: klights_pod_api::PodRepositoryReplaceRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async { Err(worker_persistence_unavailable("update")) })
    }

    fn patch_persisted_pod(
        &self,
        _request: klights_pod_api::PodRepositoryPatchRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Option<klights_cluster_core::Resource>> {
        Box::pin(async { Err(worker_persistence_unavailable("patch")) })
    }

    fn write_persisted_pod_status(
        &self,
        _request: klights_pod_api::PodRepositoryStatusRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async { Err(worker_persistence_unavailable("status update")) })
    }

    fn log_persisted_pod_status_noop(&self, request: klights_pod_api::PodRepositoryStatusNoop<'_>) {
        tracing::debug!(
            namespace = request.namespace,
            name = request.name,
            resource_version = request.resource.resource_version,
            "worker Pod status write was already current"
        );
    }
}

pub(crate) fn new_pod_store(db: DatastoreHandle) -> PodStore {
    crate::bootstrap::composition_adapters::pod_repository_persistence_adapter::new_store(db)
}

fn legacy_workqueue_kind(kind: PodWorkqueueKind) -> klights_node_store::PodWorkqueueKind {
    match kind {
        PodWorkqueueKind::Pod => klights_node_store::PodWorkqueueKind::Pod,
        PodWorkqueueKind::Namespace => klights_node_store::PodWorkqueueKind::Namespace,
    }
}

fn focused_workqueue_entry(
    lease: klights_node_store::PodWorkqueueLease,
) -> anyhow::Result<PodWorkqueueEntry> {
    let (row, lease_token) = lease.into_parts();
    let (id, identity, payload, attempt_count, _next_due_ms) = row.into_parts();
    let (kind, pod) = identity.into_persisted();
    Ok(PodWorkqueueEntry {
        id: id.get(),
        kind: match kind {
            klights_node_store::PodWorkqueueKind::Pod => PodWorkqueueKind::Pod,
            klights_node_store::PodWorkqueueKind::Namespace => PodWorkqueueKind::Namespace,
        },
        namespace: pod.namespace,
        name: pod.name,
        uid: pod.uid,
        payload: serde_json::from_slice(&payload)?,
        attempt_count,
        lease_token,
    })
}

#[async_trait::async_trait]
impl PodWorkqueuePersistence for RootPodWorkqueuePersistence {
    async fn enqueue(
        &self,
        kind: PodWorkqueueKind,
        pod: &PodIdentity,
        payload: serde_json::Value,
        attempt_count: i64,
        min_delay_ms: i64,
        last_error: Option<&str>,
    ) -> anyhow::Result<()> {
        if let Some(node_local) = &self.node_local {
            let identity = match legacy_workqueue_kind(kind) {
                klights_node_store::PodWorkqueueKind::Pod => {
                    klights_node_store::PodWorkIdentity::try_pod(pod.clone())?
                }
                klights_node_store::PodWorkqueueKind::Namespace => {
                    klights_node_store::PodWorkIdentity::try_namespace(&pod.name, &pod.uid)?
                }
            };
            let entry = klights_node_store::PodWorkqueueEnqueue::try_new(
                identity,
                serde_json::to_vec(&payload)?,
                attempt_count,
                min_delay_ms,
                last_error.map(str::to_string),
            )?;
            return node_local
                .enqueue_work(entry)
                .await
                .map_err(anyhow::Error::from);
        }
        #[cfg(not(any(test, feature = "pod-repository-test-support")))]
        return Err(anyhow::anyhow!(
            "production Pod workqueue persistence requires node-local storage"
        ));
        #[cfg(any(test, feature = "pod-repository-test-support"))]
        {
            let id = self
                .test_next_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let now_ms = self.wall_clock.now_ms();
            let floor = now_ms
                .checked_add(min_delay_ms)
                .ok_or_else(|| anyhow::anyhow!("workqueue enqueue due time overflow"))?;
            let mut rows = self.test_rows.lock().unwrap();
            let tail_next =
                rows.iter()
                    .map(|row| row.next_attempt_at_ms)
                    .max()
                    .map_or(Ok(floor), |tail| {
                        tail.checked_add(1)
                            .map(|next| floor.max(next))
                            .ok_or_else(|| anyhow::anyhow!("workqueue enqueue tail overflow"))
                    })?;
            rows.push(RootInMemoryPodWorkqueueEntry {
                id,
                kind,
                namespace: pod.namespace.clone(),
                name: pod.name.clone(),
                uid: pod.uid.clone(),
                payload,
                attempt_count,
                next_attempt_at_ms: tail_next,
            });
            let _ = last_error;
            Ok(())
        }
    }

    async fn ensure_absent(
        &self,
        kind: PodWorkqueueKind,
        pod: &PodIdentity,
        payload: serde_json::Value,
        attempt_count: i64,
        min_delay_ms: i64,
        last_error: Option<&str>,
    ) -> anyhow::Result<bool> {
        if let Some(node_local) = &self.node_local {
            let identity = match legacy_workqueue_kind(kind) {
                klights_node_store::PodWorkqueueKind::Pod => {
                    klights_node_store::PodWorkIdentity::try_pod(pod.clone())?
                }
                klights_node_store::PodWorkqueueKind::Namespace => {
                    klights_node_store::PodWorkIdentity::try_namespace(&pod.name, &pod.uid)?
                }
            };
            let entry = klights_node_store::PodWorkqueueEnqueue::try_new(
                identity,
                serde_json::to_vec(&payload)?,
                attempt_count,
                min_delay_ms,
                last_error.map(str::to_string),
            )?;
            return node_local
                .ensure_work_if_absent(entry)
                .await
                .map_err(anyhow::Error::from);
        }
        #[cfg(not(any(test, feature = "pod-repository-test-support")))]
        return Err(anyhow::anyhow!(
            "production Pod workqueue persistence requires node-local storage"
        ));
        #[cfg(any(test, feature = "pod-repository-test-support"))]
        {
            let now_ms = self.wall_clock.now_ms();
            let mut rows = self.test_rows.lock().unwrap();
            if rows.iter().any(|row| {
                row.kind == kind
                    && row.namespace == pod.namespace
                    && row.name == pod.name
                    && row.uid == pod.uid
            }) {
                return Ok(false);
            }
            let floor = now_ms
                .checked_add(min_delay_ms)
                .ok_or_else(|| anyhow::anyhow!("workqueue ensure due time overflow"))?;
            let tail_next =
                rows.iter()
                    .map(|row| row.next_attempt_at_ms)
                    .max()
                    .map_or(Ok(floor), |tail| {
                        tail.checked_add(1)
                            .map(|next| floor.max(next))
                            .ok_or_else(|| anyhow::anyhow!("workqueue ensure tail overflow"))
                    })?;
            let id = self
                .test_next_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            rows.push(RootInMemoryPodWorkqueueEntry {
                id,
                kind,
                namespace: pod.namespace.clone(),
                name: pod.name.clone(),
                uid: pod.uid.clone(),
                payload,
                attempt_count,
                next_attempt_at_ms: tail_next,
            });
            let _ = last_error;
            Ok(true)
        }
    }

    async fn peek_next_due(&self) -> anyhow::Result<Option<i64>> {
        if let Some(node_local) = &self.node_local {
            return node_local
                .peek_next_due_ms()
                .await
                .map_err(anyhow::Error::from);
        }
        #[cfg(not(any(test, feature = "pod-repository-test-support")))]
        return Err(anyhow::anyhow!(
            "production Pod workqueue persistence requires node-local storage"
        ));
        #[cfg(any(test, feature = "pod-repository-test-support"))]
        {
            Ok(self
                .test_rows
                .lock()
                .unwrap()
                .iter()
                .map(|row| row.next_attempt_at_ms)
                .min())
        }
    }

    async fn claim_due(
        &self,
        now_ms: i64,
        lease_duration_ms: i64,
    ) -> anyhow::Result<Option<PodWorkqueueEntry>> {
        if let Some(node_local) = &self.node_local {
            return node_local
                .claim_due_work_with_lease(klights_node_store::PodWorkqueueClaimRequest::try_new(
                    now_ms,
                    lease_duration_ms,
                )?)
                .await?
                .map(focused_workqueue_entry)
                .transpose();
        }
        #[cfg(not(any(test, feature = "pod-repository-test-support")))]
        return Err(anyhow::anyhow!(
            "production Pod workqueue persistence requires node-local storage"
        ));
        #[cfg(any(test, feature = "pod-repository-test-support"))]
        {
            let mut rows = self.test_rows.lock().unwrap();
            let candidate = rows
                .iter()
                .enumerate()
                .filter(|(_, row)| row.next_attempt_at_ms <= now_ms)
                .min_by_key(|(_, row)| (row.next_attempt_at_ms, row.id))
                .map(|(index, _)| index);
            Ok(candidate.map(|index| {
                let row = &mut rows[index];
                let lease_deadline_ms = now_ms
                    .checked_add(lease_duration_ms)
                    .expect("validated workqueue lease must not overflow");
                row.next_attempt_at_ms = lease_deadline_ms;
                let identity = match legacy_workqueue_kind(row.kind) {
                    klights_node_store::PodWorkqueueKind::Pod => {
                        klights_node_store::PodWorkIdentity::try_pod(PodIdentity::new(
                            &row.namespace,
                            &row.name,
                            &row.uid,
                        ))
                    }
                    klights_node_store::PodWorkqueueKind::Namespace => {
                        klights_node_store::PodWorkIdentity::try_namespace(&row.name, &row.uid)
                    }
                }
                .expect("in-memory workqueue row identity was validated on enqueue");
                PodWorkqueueEntry {
                    id: row.id,
                    kind: row.kind,
                    namespace: row.namespace.clone(),
                    name: row.name.clone(),
                    uid: row.uid.clone(),
                    payload: row.payload.clone(),
                    attempt_count: row.attempt_count,
                    lease_token: klights_node_store::PodWorkqueueLeaseToken::try_new(
                        row.id,
                        identity,
                        lease_deadline_ms,
                    )
                    .expect("in-memory lease values were validated"),
                }
            }))
        }
    }

    async fn acknowledge(
        &self,
        token: klights_node_store::PodWorkqueueLeaseToken,
    ) -> anyhow::Result<klights_node_store::PodWorkqueueMutationOutcome> {
        if let Some(node_local) = &self.node_local {
            return node_local
                .acknowledge_work(token)
                .await
                .map_err(anyhow::Error::from);
        }
        #[cfg(not(any(test, feature = "pod-repository-test-support")))]
        return Err(anyhow::anyhow!(
            "production Pod workqueue persistence requires node-local storage"
        ));
        #[cfg(any(test, feature = "pod-repository-test-support"))]
        {
            let mut rows = self.test_rows.lock().unwrap();
            let before = rows.len();
            rows.retain(|row| !in_memory_row_matches_token(row, &token));
            Ok(if rows.len() < before {
                klights_node_store::PodWorkqueueMutationOutcome::Applied
            } else {
                klights_node_store::PodWorkqueueMutationOutcome::Stale
            })
        }
    }

    async fn requeue(
        &self,
        row: PodWorkqueueEntry,
        attempt_count: i64,
        min_delay_ms: i64,
        error: &str,
    ) -> anyhow::Result<klights_node_store::PodWorkqueueMutationOutcome> {
        if let Some(node_local) = &self.node_local {
            let request = klights_node_store::PodWorkqueueRequeue::try_new(
                row.lease_token,
                serde_json::to_vec(&row.payload)?,
                attempt_count,
                min_delay_ms,
                Some(error.to_string()),
            )?;
            return node_local
                .requeue_work(request)
                .await
                .map_err(anyhow::Error::from);
        }
        #[cfg(not(any(test, feature = "pod-repository-test-support")))]
        return Err(anyhow::anyhow!(
            "production Pod workqueue persistence requires node-local storage"
        ));
        #[cfg(any(test, feature = "pod-repository-test-support"))]
        {
            let mut rows = self.test_rows.lock().unwrap();
            let Some(stored) = rows
                .iter_mut()
                .find(|stored| in_memory_row_matches_token(stored, &row.lease_token))
            else {
                return Ok(klights_node_store::PodWorkqueueMutationOutcome::Stale);
            };
            let due = self
                .wall_clock
                .now_ms()
                .checked_add(min_delay_ms)
                .ok_or_else(|| anyhow::anyhow!("workqueue requeue due time overflow"))?;
            stored.payload = row.payload;
            stored.attempt_count = attempt_count;
            stored.next_attempt_at_ms = if due == row.lease_token.leased_next_due_ms().get() {
                due.checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("workqueue requeue due time overflow"))?
            } else {
                due
            };
            Ok(klights_node_store::PodWorkqueueMutationOutcome::Applied)
        }
    }
}

// ── Root-owned focused capabilities (E6) ──
//
// The former aggregate implemented these traits by holding every service; the
// root composition now exposes equivalent focused adapters that wrap exactly
// the services they need.

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

pub(crate) fn pod_resource_key(ns: &str, name: &str) -> ResourceKey {
    ResourceKey {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some(ns.to_string()),
        name: name.to_string(),
    }
}

pub(crate) fn pod_has_owner_uid(pod: &serde_json::Value, owner_uid: &str) -> bool {
    pod.pointer("/metadata/ownerReferences")
        .and_then(|owners| owners.as_array())
        .is_some_and(|owners| {
            owners
                .iter()
                .any(|owner| owner.get("uid").and_then(|uid| uid.as_str()) == Some(owner_uid))
        })
}

/// Root live Pod query: leader-fresh reads through the cluster API when a
/// worker, direct store reads on the leader, with the worker's node-local
/// status checkpoint overlaid for its own just-written status.
pub(crate) struct RootPodQueryWriter {
    store: Arc<PodStore>,
    cluster_api: Option<Arc<dyn LeaderResourceQuery>>,
    outbox: Option<Arc<klights_kubelet::outbox::Outbox>>,
}

impl RootPodQueryWriter {
    pub(crate) fn new(
        store: Arc<PodStore>,
        cluster_api: Option<Arc<dyn LeaderResourceQuery>>,
        outbox: Option<Arc<klights_kubelet::outbox::Outbox>>,
    ) -> Self {
        Self {
            store,
            cluster_api,
            outbox,
        }
    }

    /// Overlay the node-local status checkpoint onto a worker fresh read so the
    /// worker observes its OWN just-written status (read-your-own-write).
    /// The checkpoint only ever reflects state the worker itself
    /// authored and self-clears once the leader catches up.
    async fn overlay_local_status_checkpoint(
        &self,
        pod: Option<klights_cluster_core::Resource>,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        match (pod, &self.outbox) {
            (Some(pod), Some(outbox)) => Ok(Some(outbox.merge_pod_status_checkpoint(pod).await?)),
            (other, _) => Ok(other),
        }
    }
}

impl klights_pod_api::PodQuery for RootPodQueryWriter {
    fn get_pod(
        &self,
        request: klights_pod_api::PodGetRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Option<klights_cluster_core::Resource>> {
        Box::pin(async move {
            let pod = if let Some(cluster_api) = &self.cluster_api {
                let pod = cluster_api
                    .get_resource(
                        klights_leader_api::ResourceGetRequest::try_new(
                            pod_resource_key(request.namespace(), request.name()),
                            klights_leader_api::ResourceQueryConsistency::LeaderFresh,
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
                        klights_leader_api::ResourceListRequest::try_new(
                            "v1",
                            "Pod",
                            request.namespace().map(str::to_string),
                            request.label_selector().map(str::to_string),
                            request.field_selector().map(str::to_string),
                            request.limit(),
                            request.continue_token().map(str::to_string),
                            klights_leader_api::ResourceQueryConsistency::LeaderFresh,
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
    ) -> klights_pod_api::PodRepositoryFuture<'_, Vec<klights_cluster_core::Resource>> {
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

impl klights_pod_api::PodSnapshotQuery for RootPodQueryWriter {
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

#[cfg(any(test, feature = "pod-repository-test-support"))]
impl PodWatchSource for RootPodQueryWriter {
    fn subscribe_pod_watch(&self) -> tokio::sync::broadcast::Receiver<klights_watch::WatchEvent> {
        self.store.subscribe_watch()
    }
}

// Insert for `PodUpdate` and the root status writer: metadata writes need
// the composed root query for their recompute-readback, and the status
// adapter owns the post-write finish hooks (PDB reconcile + namespace
// maintenance) that the aggregate previously ran in `finish_status_write`.
pub(crate) struct RootPodMetadataWriter {
    metadata: klights_kubelet::pod_repository::PodMetadataService,
    query: Arc<dyn klights_pod_api::PodQuery>,
}

impl klights_pod_api::PodUpdate for RootPodMetadataWriter {
    fn update_pod(
        &self,
        request: klights_pod_api::PodUpdateRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_cluster_core::Resource> {
        self.metadata.update_pod_from(self.query.as_ref(), request)
    }
}

pub(crate) struct RootPodStatusWriterAdapter {
    status: klights_kubelet::pod_repository::status::PodStatusService,
    mutation_reconcile: Arc<dyn klights_reconcile_api::PodMutationReconcileSink>,
    namespace_termination: Arc<dyn klights_reconcile_api::NamespaceTerminationSink>,
    supervisor: Arc<TaskSupervisor>,
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    post_write_maintenance_notify: Option<Arc<tokio::sync::Notify>>,
}

impl RootPodStatusWriterAdapter {
    /// Spawn async namespace-termination reconciliation after a pod status or
    /// metadata write. Derived-state maintenance must not block the caller
    /// (kubelet status writer, controller pod writer); the spawned task runs
    /// on the TaskSupervisor under `Background` so it is visible on the admin
    /// diagnostics API and participates in graceful shutdown.
    async fn spawn_post_write_maintenance(&self, namespace: &str) {
        let namespace_termination = self.namespace_termination.clone();
        let ns = namespace.to_string();
        #[cfg(any(test, feature = "pod-repository-test-support"))]
        let completion = self.post_write_maintenance_notify.clone();
        let spawn_result = self
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
                    #[cfg(any(test, feature = "pod-repository-test-support"))]
                    if let Some(completion) = completion {
                        completion.notify_one();
                    }
                },
            )
            .await;
        #[cfg(any(test, feature = "pod-repository-test-support"))]
        if spawn_result.is_err()
            && let Some(completion) = self.post_write_maintenance_notify.as_ref()
        {
            completion.notify_one();
        }
        #[cfg(not(any(test, feature = "pod-repository-test-support")))]
        let _ = spawn_result;
    }

    async fn finish_status_write(
        &self,
        namespace: &str,
        result: klights_kubelet::pod_repository::status::PodStatusWriteResult,
        context: &'static str,
    ) -> klights_cluster_core::Resource {
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
}

#[async_trait::async_trait]
impl klights_kubelet::pod_repository::status::PodStatusWriter for RootPodStatusWriterAdapter {
    async fn set_pod_status(
        &self,
        ns: &str,
        name: &str,
        update: klights_kubelet::pod_repository::PodStatusUpdate,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
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
        update: klights_kubelet::pod_repository::PodStatusUpdate,
        expected_rp: Option<i64>,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        let result = self
            .status
            .set_pod_status_for_uid(ns, name, pod_uid, update, expected_rp)
            .await?;
        Ok(self
            .finish_status_write(ns, result, "pod_status_set_uid")
            .await)
    }

    async fn apply_runtime_reconcile_status(
        &self,
        ns: &str,
        name: &str,
        update: klights_kubelet::pod_repository::RuntimeReconcileStatus,
        expected_rp: Option<i64>,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        let result = self
            .status
            .apply_runtime_reconcile_status(ns, name, update, expected_rp)
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
        update: klights_kubelet::pod_repository::RuntimeReconcileStatus,
        expected_rp: Option<i64>,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        let result = self
            .status
            .apply_runtime_reconcile_status_for_uid(ns, name, pod_uid, update, expected_rp)
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
    ) -> anyhow::Result<klights_cluster_core::Resource> {
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
        expected_rp: Option<i64>,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        let result = self
            .status
            .set_probe_readiness(ns, name, container_name, ready, expected_rp)
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
        expected_rp: Option<i64>,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        let result = self
            .status
            .set_probe_readiness_for_uid(ns, name, pod_uid, container_name, ready, expected_rp)
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
        expected_rp: Option<i64>,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        let result = self
            .status
            .set_deadline_exceeded(ns, name, message, expected_rp)
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
        expected_rp: Option<i64>,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        let result = self
            .status
            .set_deadline_exceeded_for_uid(ns, name, pod_uid, message, expected_rp)
            .await?;
        Ok(self
            .finish_status_write(ns, result, "pod_deadline_exceeded_uid")
            .await)
    }

    async fn apply_ephemeral_container_statuses(
        &self,
        ns: &str,
        name: &str,
        statuses: Vec<serde_json::Value>,
        expected_rp: Option<i64>,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        let result = self
            .status
            .apply_ephemeral_container_statuses(ns, name, statuses, expected_rp)
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
        statuses: Vec<serde_json::Value>,
        expected_rp: Option<i64>,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        let result = self
            .status
            .apply_ephemeral_container_statuses_for_uid(ns, name, pod_uid, statuses, expected_rp)
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
        terminated: serde_json::Value,
        expected_rp: Option<i64>,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        let updated = self
            .status
            .note_container_restart(ns, name, container_name, terminated, expected_rp)
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
        terminated: serde_json::Value,
        expected_rp: Option<i64>,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        let updated = self
            .status
            .note_container_restart_for_uid(
                ns,
                name,
                pod_uid,
                container_name,
                terminated,
                expected_rp,
            )
            .await?;
        if updated.is_some() {
            self.spawn_post_write_maintenance(ns).await;
        }
        Ok(updated)
    }
}

/// Trait-object across the workqueue's durable namespace-termination
/// enqueue capability (previously exposed as an aggregate trait impl).
pub(crate) struct RootNamespaceTerminationQueue {
    workqueue: Arc<PodWorkqueue>,
}

impl klights_reconcile_api::NamespaceTerminationSink for RootNamespaceTerminationQueue {
    fn reconcile_namespace_termination(
        &self,
        request: klights_reconcile_api::NamespaceTerminationRequest,
    ) -> klights_reconcile_api::NamespaceTerminationFuture<'_> {
        Box::pin(async move {
            self.workqueue
                .enqueue_namespace_termination(
                    request.namespace,
                    request.expected_uid.unwrap_or_default(),
                )
                .await
                .map_err(|error| {
                    klights_reconcile_api::ReconcileSinkError::unavailable(error.to_string())
                })?;
            Ok(klights_reconcile_api::NamespaceTerminationOutcome::StillPending)
        })
    }
}

impl klights_reconcile_api::NamespaceTerminationQueueSink for RootNamespaceTerminationQueue {
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

#[derive(Clone)]
pub(crate) struct PodDeletionFinalizerDependencies {
    pub store: Arc<PodStore>,
    pub gc_pod_delete_sink: Arc<dyn GcPodDeleteSink>,
    pub gc_reconcile: Arc<dyn klights_reconcile_api::PodGcReconcileSink>,
    pub pdb_reconcile: Arc<dyn klights_reconcile_api::PodPdbReconcileSink>,
    pub namespace_termination: Arc<dyn klights_reconcile_api::NamespaceTerminationSink>,
    pub cluster_api: Option<Arc<dyn LeaderResourceQuery>>,
    pub outbox: Option<Arc<klights_kubelet::outbox::Outbox>>,
    pub remote_delivery_required: bool,
    pub bound_pod_finalization: Arc<dyn klights_pod_api::BoundPodFinalization>,
    pub mutation_reconcile: Arc<dyn klights_reconcile_api::PodMutationReconcileSink>,
    pub metrics: Arc<dyn klights_reconcile_api::ReconcileFailureMetrics>,
    pub supervisor: Arc<TaskSupervisor>,
    pub deferred_runtime: klights_kubelet::pod_repository::status::DeferredRuntimeReducerHandle,
}

/// Finalizer decorator that releases repository-private deferred runtime state
/// only after the actor-owned deletion boundary reports a terminal outcome.
pub(crate) struct DeferredRuntimeCleanupFinalizer {
    inner: Arc<dyn klights_kubelet::pod_deletion_finalizer::PodDeletionFinalizer>,
    deferred_runtime: klights_kubelet::pod_repository::status::DeferredRuntimeReducerHandle,
}

impl DeferredRuntimeCleanupFinalizer {
    pub(crate) fn new(
        inner: Arc<dyn klights_kubelet::pod_deletion_finalizer::PodDeletionFinalizer>,
        deferred_runtime: klights_kubelet::pod_repository::status::DeferredRuntimeReducerHandle,
    ) -> Self {
        Self {
            inner,
            deferred_runtime,
        }
    }
}

pub(crate) fn compose_pod_deletion_finalizer(
    dependencies: PodDeletionFinalizerDependencies,
) -> Arc<dyn klights_kubelet::pod_deletion_finalizer::PodDeletionFinalizer> {
    let runtime_deletion_finalizer =
        klights_kubelet::pod_deletion_finalizer::compose_real_pod_deletion_finalizer(
            klights_kubelet::pod_deletion_finalizer::RealPodDeletionFinalizerDependencies {
                pod_query: dependencies.store,
                gc_pod_delete_sink: dependencies.gc_pod_delete_sink,
                gc_reconcile: dependencies.gc_reconcile,
                pdb_reconcile: dependencies.pdb_reconcile,
                namespace_termination: dependencies.namespace_termination.clone(),
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

#[async_trait::async_trait]
impl klights_kubelet::pod_deletion_finalizer::PodDeletionFinalizer
    for DeferredRuntimeCleanupFinalizer
{
    async fn finalize_after_actor_cleanup(
        &self,
        key: &klights_kubelet::runtime_types::PodRuntimeKey,
    ) -> anyhow::Result<klights_kubelet::runtime_types::PodDeletionFinalizeResult> {
        let result = self.inner.finalize_after_actor_cleanup(key).await?;
        if matches!(
            result,
            klights_kubelet::runtime_types::PodDeletionFinalizeResult::DeletedOrAlreadyGone
                | klights_kubelet::runtime_types::PodDeletionFinalizeResult::Queued
        ) {
            self.deferred_runtime.forget(&key.uid);
        }
        Ok(result)
    }
}

type RootPodAdapterBuild = (
    PodRepositoryAdapters,
    Arc<PodApiService>,
    Arc<PodSubresourceService>,
    Arc<dyn klights_pod_api::PodScheduling>,
    Arc<dyn klights_pod_api::PodPersistence>,
    Arc<dyn klights_pod_api::PodStatusPersistence>,
);

impl RootPodRepositoryComposition {
    fn build(&self, dependencies: PodRepositoryAdapterDependencies) -> RootPodAdapterBuild {
        let pod_reconcile = Arc::new(
            crate::bootstrap::controller_adapters::pod_reconcile_adapter::PodReconcileAdapter::new_with_coordination(
                self.db.clone(),
                self.side_effects.controller_dispatcher_slot(),
                self.metrics.clone(),
                self.side_effects.clone(),
                dependencies.store.clone(),
                self.gc_coordination.clone(),
                self.controller_identity.clone(),
            ),
        );
        let native =
            crate::bootstrap::composition_adapters::pod_native_adapter::RootPodNativeAdapter::new(
                dependencies.store.clone(),
                self.db.clone(),
                self.wall_clock.clone(),
                #[cfg(any(test, feature = "pod-repository-test-support"))]
                self.scheduler_bind_gate.clone(),
            );
        let pod_query: Arc<dyn klights_pod_api::PodQuery> = native.clone();
        let persistence: Arc<dyn klights_pod_api::PodPersistence> = native.clone();
        let status_persistence: Arc<dyn klights_pod_api::PodStatusPersistence> = native.clone();
        let deletion: Arc<dyn klights_pod_api::PodDeleteOrchestration> = dependencies.deletion;
        let event_sink: Arc<dyn klights_pod_api::PodControlPlaneEventSink> = native.clone();
        let placement: Arc<dyn klights_pod_api::PodPlacement> =
            Arc::new(klights_controllers::scheduler::SchedulerPlacement::new());
        let mutation_effects: Arc<dyn klights_reconcile_api::ResourceMutationEffectsPort> =
            klights_controllers::side_effects::ResourceMutationEffects::new(
                self.side_effects.clone(),
                self.metrics.clone(),
            );
        let subresource = Arc::new(PodSubresourceService::new(
            pod_query.clone(),
            persistence.clone(),
            status_persistence.clone(),
            pod_reconcile.clone(),
        ));
        let native_orchestration = Arc::new(PodNativeOrchestration::new(
            PodNativeOrchestrationDependencies {
                identity: self.api_identity.clone(),
                pod_query: pod_query.clone(),
                persistence: persistence.clone(),
                deletion: deletion.clone(),
                admission_resources: native.clone(),
                spec_validation: native.clone(),
                admission: crate::bootstrap::composition_adapters::resource_admission_adapter::ResourceAdmissionAdapter::new(
                    self.api_identity.clone(),
                    self.db.clone(),
                ),
                resource_query: self.resource_query.clone(),
                quota_runtime:
                    crate::bootstrap::controller_adapters::resource_quota_admission_adapter::ResourceQuotaAdmissionAdapter::new(
                        self.db.clone(),
                    ),
                supervisor: dependencies.supervisor.clone(),
                gc_reconcile: pod_reconcile.clone(),
                service_reconcile: pod_reconcile.clone(),
                mutation_reconcile: pod_reconcile.clone(),
                metrics: self.metrics.clone(),
                wall_clock: self.wall_clock.clone(),
            },
        ));
        let mutation: Arc<dyn klights_pod_api::PodApiMutation> = native_orchestration.clone();
        let gc_delete: Arc<dyn klights_reconcile_api::GcPodDeleteSink> =
            native_orchestration.clone();
        let api = Arc::new(PodApiService::new(PodApiServiceDependencies {
            mutation,
            gc_delete,
        }));
        let scheduling: Arc<dyn klights_pod_api::PodScheduling> =
            klights_controllers::scheduler::SchedulerService::new(
                klights_controllers::scheduler::SchedulerServiceDependencies {
                    pod_query,
                    persistence: persistence.clone(),
                    status_persistence: status_persistence.clone(),
                    deletion,
                    event_sink,
                    placement,
                    resource_query: self.resource_query.clone(),
                    supervisor: dependencies.supervisor,
                    mutation_effects,
                    wall_clock: self.wall_clock.clone(),
                },
            );
        let adapters = PodRepositoryAdapters {
            gc_delete: api.clone(),
            gc_reconcile: pod_reconcile.clone(),
            pdb_reconcile: pod_reconcile.clone(),
            eviction_admission: pod_reconcile.clone(),
            namespace_bootstrap: pod_reconcile.clone(),
            namespace_termination: pod_reconcile.clone(),
            mutation_reconcile: pod_reconcile,
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            test_api: Some(api.clone()),
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            test_subresource: Some(subresource.clone()),
        };
        (
            adapters,
            api,
            subresource,
            scheduling,
            native,
            status_persistence,
        )
    }
}

#[cfg(not(any(test, feature = "pod-repository-test-support")))]
pub(crate) fn build_pod_repository_parts(
    config: PodRepositoryBuildConfig,
    leader_coordination: Option<Arc<dyn klights_leader_api::ControllerCoordination>>,
) -> PodRepositoryConstructionResult {
    build_pod_repository_parts_inner(config, leader_coordination, None)
}

#[cfg(feature = "pod-repository-test-support")]
pub(crate) fn build_integration_pod_repository_parts(
    config: PodRepositoryBuildConfig,
    resource_query: Arc<dyn LeaderResourceQuery>,
) -> PodRepositoryConstructionResult {
    build_pod_repository_parts_inner(
        config,
        None,
        Some(resource_query),
        Some((
            Arc::new(
                crate::bootstrap::controller_adapters::system_identity_adapter::SystemIdentityGenerator,
            ),
            Arc::new(klights_controllers::ControllerCoordination::new()),
        )),
    )
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
pub(crate) fn build_pod_repository_parts(
    config: PodRepositoryBuildConfig,
    leader_coordination: Option<Arc<dyn klights_leader_api::ControllerCoordination>>,
) -> PodRepositoryConstructionResult {
    build_pod_repository_parts_inner(config, leader_coordination, None, None)
}

fn build_pod_repository_parts_inner(
    config: PodRepositoryBuildConfig,
    leader_coordination: Option<Arc<dyn klights_leader_api::ControllerCoordination>>,
    resource_query_override: Option<Arc<dyn LeaderResourceQuery>>,
    #[cfg(any(test, feature = "pod-repository-test-support"))] test_support: Option<(
        Arc<dyn k8s_native_service::ApiIdentityGenerator>,
        Arc<dyn klights_reconcile_api::GcForegroundDeleteCoordination>,
    )>,
) -> PodRepositoryConstructionResult {
    let PodRepositoryBuildConfig {
        db,
        pod_workqueue_store,
        supervisor,
        side_effects,
        metrics,
        pod_network_cache,
        assignment_waiter,
        scheduling_mode,
        outbox,
        cluster_api,
        remote_delivery_required,
        controller_identity,
        #[cfg(not(test))]
        api_identity,
        #[cfg(any(test, feature = "pod-repository-test-support"))]
        scheduler_bind_gate,
        #[cfg(any(test, feature = "pod-repository-test-support"))]
        post_write_maintenance_notify,
        #[cfg(not(test))]
        gc_coordination,
    } = config;
    #[cfg(test)]
    let (api_identity, gc_coordination) = test_support.unwrap_or_else(|| {
        (
            Arc::new(
                crate::bootstrap::controller_adapters::system_identity_adapter::SystemIdentityGenerator,
            ) as Arc<dyn k8s_native_service::ApiIdentityGenerator>,
            Arc::new(klights_controllers::ControllerCoordination::new())
                as Arc<dyn klights_reconcile_api::GcForegroundDeleteCoordination>,
        )
    });
    #[cfg(all(feature = "pod-repository-test-support", not(test)))]
    let _ = test_support;
    let _ = scheduling_mode;
    #[cfg(not(any(test, feature = "pod-repository-test-support")))]
    let resource_query = resource_query_override
        .or_else(|| cluster_api.clone())
        .expect("production Pod repository requires a leader resource query");
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    let _ = resource_query_override;
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    let resource_query = cluster_api.clone().unwrap_or_else(|| {
        Arc::new(crate::control_plane::client::local::LocalApiClient::new(
            db.clone(),
            "local-node".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ))
    });
    let wall_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock> =
        Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock);
    let persistence_parts =
        crate::bootstrap::composition_adapters::pod_repository_persistence_adapter::new_root_parts(
            db.clone(),
            wall_clock.clone(),
        );
    let store = persistence_parts.store.clone();
    let local_bound_finalization = persistence_parts.bound_finalization;
    let unscheduled_deletion = persistence_parts.unscheduled_deletion;
    let workqueue_persistence = RootPodWorkqueuePersistence {
        node_local: pod_workqueue_store,
        #[cfg(any(test, feature = "pod-repository-test-support"))]
        wall_clock: wall_clock.clone(),
        #[cfg(any(test, feature = "pod-repository-test-support"))]
        test_rows: Arc::new(std::sync::Mutex::new(Vec::new())),
        #[cfg(any(test, feature = "pod-repository-test-support"))]
        test_next_id: Arc::new(std::sync::atomic::AtomicI64::new(1)),
    };
    let workqueue = if let Some(leader_coordination) = leader_coordination {
        PodWorkqueue::new_leader(
            store.clone(),
            workqueue_persistence,
            supervisor.clone(),
            metrics.clone(),
            unscheduled_deletion,
            leader_coordination,
            wall_clock.clone(),
        )
    } else {
        PodWorkqueue::new(
            store.clone(),
            workqueue_persistence,
            supervisor.clone(),
            metrics.clone(),
            wall_clock.clone(),
        )
    };
    let delete_coordinator = Arc::new(PodDeleteCoordinator::new(
        store.clone(),
        workqueue.clone(),
        supervisor.clone(),
        metrics.clone(),
        wall_clock.clone(),
    ));
    let (adapters, api, subresource, scheduling, metadata_persistence, status_persistence) =
        RootPodRepositoryComposition {
            db: db.clone(),
            resource_query,
            side_effects: side_effects.clone(),
            metrics: metrics.clone(),
            gc_coordination,
            wall_clock: Arc::new(klights_supervisor::SystemWallClock),
            controller_identity,
            api_identity,
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            scheduler_bind_gate,
        }
        .build(PodRepositoryAdapterDependencies {
            store: store.clone(),
            supervisor: supervisor.clone(),
            deletion: delete_coordinator,
        });
    let delivery_outbox = outbox.map(|outbox| outbox as Arc<dyn klights_leader_api::NodeOutbox>);
    let bound_pod_finalization =
        crate::bootstrap::composition_adapters::bound_pod_finalization_adapter::new_for_root(
            store.clone(),
            local_bound_finalization,
            cluster_api.clone(),
            delivery_outbox.clone(),
            remote_delivery_required,
            wall_clock.clone(),
        );
    let host_ip = klights_kubelet::context::HostIpState::default();
    let assignment_query = klights_kubelet::pod_repository::pod_network_assignment_query(
        pod_network_cache,
        supervisor.clone(),
        assignment_waiter,
        host_ip.clone(),
    );
    assemble_pod_services(
        PodRepositoryCoreDependencies {
            status_persistence,
            metadata_persistence,
            store,
            workqueue,
        },
        PodRepositoryRuntimeDependencies {
            supervisor,
            metrics: metrics.clone(),
            wall_clock,
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            post_write_maintenance_notify,
        },
        PodRepositoryNetworkDependencies {
            assignment_query,
            host_ip,
        },
        PodRepositoryDeliveryDependencies {
            outbox: delivery_outbox,
            cluster_api,
            remote_delivery_required,
            bound_pod_finalization,
        },
        adapters,
        PodRepositoryApiServices {
            api: Some(api),
            subresource: Some(subresource),
            scheduling: Some(scheduling),
        },
    )
}

pub(crate) fn build_worker_pod_repository_parts(
    config: WorkerPodRepositoryBuildConfig,
) -> PodRepositoryConstructionResult {
    let wall_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock> =
        Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock);
    let worker_persistence = Arc::new(WorkerPodPersistence {
        resource_query: config.resource_query.clone(),
    });
    let store = Arc::new(PodStore::from_persistence(
        worker_persistence.clone(),
        worker_persistence.clone(),
        Arc::new(std::sync::atomic::AtomicUsize::new(1)),
    ));
    let workqueue = PodWorkqueue::new(
        store.clone(),
        RootPodWorkqueuePersistence {
            node_local: Some(config.pod_workqueue_store),
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            wall_clock: wall_clock.clone(),
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            test_rows: Arc::new(std::sync::Mutex::new(Vec::new())),
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            test_next_id: Arc::new(std::sync::atomic::AtomicI64::new(1)),
        },
        config.supervisor.clone(),
        config.metrics.clone(),
        wall_clock.clone(),
    );
    let outbox: Arc<dyn klights_leader_api::NodeOutbox> = config.outbox;
    let root_deletion = crate::bootstrap::composition_adapters::bound_pod_finalization_adapter::RootBoundPodFinalization::new(
        store.clone(),
        None,
        Some(config.resource_query.clone()),
        Some(outbox.clone()),
        true,
        wall_clock.clone(),
    );
    let adapters = WorkerPodAdapters::build(
        PodRepositoryAdapterDependencies {
            store: store.clone(),
            supervisor: config.supervisor.clone(),
            deletion: Arc::new(PodDeleteCoordinator::new(
                store.clone(),
                workqueue.clone(),
                config.supervisor.clone(),
                config.metrics.clone(),
                wall_clock.clone(),
            )),
        },
        root_deletion.clone(),
    );
    let bound_pod_finalization: Arc<dyn klights_pod_api::BoundPodFinalization> = root_deletion;
    let host_ip = klights_kubelet::context::HostIpState::default();
    let assignment_query = klights_kubelet::pod_repository::pod_network_assignment_query(
        config.pod_network_cache,
        config.supervisor.clone(),
        config.assignment_waiter,
        host_ip.clone(),
    );
    assemble_pod_services(
        PodRepositoryCoreDependencies {
            status_persistence: worker_persistence.clone(),
            metadata_persistence: worker_persistence,
            store,
            workqueue,
        },
        PodRepositoryRuntimeDependencies {
            supervisor: config.supervisor,
            metrics: config.metrics,
            wall_clock,
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            post_write_maintenance_notify: None,
        },
        PodRepositoryNetworkDependencies {
            assignment_query,
            host_ip,
        },
        PodRepositoryDeliveryDependencies {
            outbox: Some(outbox),
            cluster_api: Some(config.resource_query),
            remote_delivery_required: true,
            bound_pod_finalization,
        },
        adapters,
        PodRepositoryApiServices {
            api: None,
            subresource: None,
            scheduling: None,
        },
    )
}

/// Assemble the flat focused parts from the core/runtime/network/delivery
/// dependencies and the API services produced by the root composition. The
/// worker flow passes `None` for the API services (no leader-owned API
/// surface on a worker).
fn assemble_pod_services(
    core: PodRepositoryCoreDependencies,
    runtime: PodRepositoryRuntimeDependencies,
    network: PodRepositoryNetworkDependencies,
    delivery: PodRepositoryDeliveryDependencies,
    adapters: PodRepositoryAdapters,
    services: PodRepositoryApiServices,
) -> PodRepositoryConstructionResult {
    let PodRepositoryApiServices {
        api,
        subresource,
        scheduling,
    } = services;
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
        #[cfg(any(test, feature = "pod-repository-test-support"))]
        post_write_maintenance_notify,
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
    } = delivery;
    workqueue.set_namespace_termination_sink(adapters.namespace_termination.clone());
    let gc_reconcile = adapters.gc_reconcile;
    let pdb_reconcile = adapters.pdb_reconcile;
    let eviction_admission = adapters.eviction_admission;
    let namespace_bootstrap = adapters.namespace_bootstrap;
    let namespace_termination = adapters.namespace_termination;
    let mutation_reconcile = adapters.mutation_reconcile;
    let status = klights_kubelet::pod_repository::status::PodStatusService::new(
        klights_kubelet::pod_repository::status::PodStatusServiceDependencies {
            pod_query: store.clone(),
            status_persistence,
            mutation_reconcile: mutation_reconcile.clone(),
            outbox: outbox.clone(),
            remote_delivery_required,
            cluster_api: cluster_api.clone(),
            host_ip: host_ip.clone(),
            wall_clock: wall_clock.clone(),
        },
    );
    let metadata = klights_kubelet::pod_repository::PodMetadataService::new(
        klights_kubelet::pod_repository::PodMetadataDependencies {
            persistence: metadata_persistence,
            outbox: outbox.clone(),
            remote_delivery_required,
            mutation_reconcile: mutation_reconcile.clone(),
            wall_clock: wall_clock.clone(),
        },
    );
    let gc_pod_delete_sink = adapters.gc_delete.clone();
    workqueue.set_remote_pod_delete_resignal_sink(Arc::downgrade(&gc_pod_delete_sink));

    let namespace_termination_queue = Arc::new(RootNamespaceTerminationQueue {
        workqueue: workqueue.clone(),
    });
    let deferred_runtime = status.deferred_runtime_handle();
    let deletion_finalizer = compose_pod_deletion_finalizer(PodDeletionFinalizerDependencies {
        store: store.clone(),
        gc_pod_delete_sink: gc_pod_delete_sink.clone(),
        gc_reconcile: gc_reconcile.clone(),
        pdb_reconcile: pdb_reconcile.clone(),
        // Actor finalization performs post-write namespace maintenance through
        // the original root sink. The durable queue is only the workqueue
        // execution boundary; adapting it here would erase an absent
        // expected UID into an empty string.
        namespace_termination: namespace_termination.clone(),
        cluster_api: cluster_api.clone(),
        outbox: outbox.clone(),
        remote_delivery_required,
        bound_pod_finalization: bound_pod_finalization.clone(),
        mutation_reconcile: mutation_reconcile.clone(),
        metrics: metrics.clone(),
        supervisor: supervisor.clone(),
        deferred_runtime: deferred_runtime.clone(),
    });

    let query_writer = Arc::new(RootPodQueryWriter::new(
        store.clone(),
        cluster_api.clone(),
        outbox.clone(),
    ));
    let pod_query: Arc<dyn klights_pod_api::PodQuery> = query_writer.clone();
    let pod_snapshot: Arc<dyn klights_pod_api::PodSnapshotQuery> = query_writer.clone();
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    let watch_source: Arc<dyn PodWatchSource> = query_writer.clone();
    let pod_update = Arc::new(RootPodMetadataWriter {
        metadata,
        query: pod_query.clone(),
    });
    let pod_status_writer = Arc::new(RootPodStatusWriterAdapter {
        status,
        mutation_reconcile: mutation_reconcile.clone(),
        namespace_termination: namespace_termination.clone(),
        supervisor: supervisor.clone(),
        #[cfg(any(test, feature = "pod-repository-test-support"))]
        post_write_maintenance_notify,
    });
    let background = PodRepositoryBackground::new(workqueue.clone());
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    {
        (
            pod_query,
            pod_snapshot,
            pod_update,
            pod_status_writer as Arc<dyn klights_kubelet::pod_repository::status::PodStatusWriter>,
            workqueue,
            assignment_query,
            host_ip,
            background,
            deletion_finalizer,
            store.sandbox_gc_dirty_counter(),
            mutation_reconcile,
            gc_pod_delete_sink,
            eviction_admission,
            namespace_bootstrap,
            namespace_termination_queue,
            api,
            subresource,
            scheduling,
            watch_source,
            bound_pod_finalization,
            deferred_runtime,
            adapters.test_api,
            adapters.test_subresource,
        )
    }
    #[cfg(not(any(test, feature = "pod-repository-test-support")))]
    {
        (
            pod_query,
            pod_snapshot,
            pod_update,
            pod_status_writer as Arc<dyn klights_kubelet::pod_repository::status::PodStatusWriter>,
            workqueue,
            assignment_query,
            host_ip,
            background,
            deletion_finalizer,
            store.sandbox_gc_dirty_counter(),
            mutation_reconcile,
            gc_pod_delete_sink,
            eviction_admission,
            namespace_bootstrap,
            namespace_termination_queue,
            api,
            subresource,
            scheduling,
        )
    }
}

// ── E6: test-support helpers moved from the deleted root pod-repository
// module (cfg(test) only; integration-test support lives in
// `pod_repository_composition_test_support.rs`).

#[cfg(test)]
struct TestDatastorePodNetworkCache {
    network: Option<Arc<dyn klights_node_store::PodNetworkCache>>,
}

#[cfg(test)]
pub(crate) fn empty_test_pod_network_cache() -> Arc<dyn klights_node_store::PodNetworkCache> {
    Arc::new(TestDatastorePodNetworkCache { network: None })
}

#[cfg(test)]
pub(crate) fn test_assignment_bus() -> Arc<klights_networking::PodNetworkAssignmentBus> {
    Arc::new(klights_networking::PodNetworkAssignmentBus::new())
}

#[cfg(test)]
pub(crate) async fn test_node_local_store(
    supervisor: Arc<TaskSupervisor>,
) -> std::sync::Arc<crate::bootstrap::node_store::NodeLocalStores> {
    std::sync::Arc::new(
        crate::bootstrap::node_store::open_node_local(
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
impl klights_node_store::PodNetworkCache for TestDatastorePodNetworkCache {
    fn get_network_for_uid(
        &self,
        pod_uid: klights_node_store::PodUidKey,
    ) -> klights_node_store::CacheNetworkFuture<'_, Option<klights_node_store::PodNetworkEndpoint>>
    {
        match &self.network {
            Some(network) => {
                klights_node_store::PodNetworkCache::get_network_for_uid(network.as_ref(), pod_uid)
            }
            None => Box::pin(async { Ok(None) }),
        }
    }

    fn get_network_for_pod(
        &self,
        pod: klights_types::PodIdentity,
    ) -> klights_node_store::CacheNetworkFuture<'_, Option<klights_node_store::PodNetworkEndpoint>>
    {
        match &self.network {
            Some(network) => {
                klights_node_store::PodNetworkCache::get_network_for_pod(network.as_ref(), pod)
            }
            None => Box::pin(async { Ok(None) }),
        }
    }

    fn get_network_for_sandbox(
        &self,
        sandbox_id: klights_node_store::SandboxKey,
    ) -> klights_node_store::CacheNetworkFuture<'_, Option<klights_node_store::PodNetworkEndpoint>>
    {
        match &self.network {
            Some(network) => klights_node_store::PodNetworkCache::get_network_for_sandbox(
                network.as_ref(),
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
        match &self.network {
            Some(network) => klights_node_store::PodNetworkCache::get_network_for_assignment(
                network.as_ref(),
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
        match &self.network {
            Some(network) => klights_node_store::PodNetworkCache::delete_network_for_sandbox(
                network.as_ref(),
                sandbox_id,
            ),
            None => Box::pin(async { Ok(()) }),
        }
    }

    fn delete_network_if_matches(
        &self,
        request: klights_node_store::PodNetworkAllocationRequest,
    ) -> klights_node_store::CacheNetworkFuture<'_, bool> {
        match &self.network {
            Some(network) => klights_node_store::PodNetworkCache::delete_network_if_matches(
                network.as_ref(),
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
        match &self.network {
            Some(network) => {
                klights_node_store::PodNetworkCache::list_network_assignments(network.as_ref())
            }
            None => Box::pin(async { Ok(Vec::new()) }),
        }
    }
}
