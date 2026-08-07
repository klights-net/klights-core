//! Composition-root wiring for Pod repository API and reconcile adapters.

use std::sync::Arc;

use crate::datastore::DatastoreHandle;
use crate::kubelet::pod_repository::delete_coordinator::PodDeleteCoordinator;
use crate::kubelet::pod_repository::store::PodStore;
use crate::kubelet::pod_repository::workqueue::PodWorkqueue;
use crate::kubelet::pod_repository::{
    PodRepository, PodRepositoryAdapterDependencies, PodRepositoryAdapters,
    PodRepositoryCoreDependencies, PodRepositoryDeliveryDependencies,
    PodRepositoryNetworkDependencies, PodRepositoryRuntimeDependencies,
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
use klights_supervisor::TaskSupervisor;
use klights_types::PodIdentity;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PodSchedulingMode {
    InlineSingleNode,
    DeferredMultiNodeLeader,
}

#[derive(Clone)]
pub struct PodRepositoryBuildConfig {
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
    pub controller_identity: Arc<dyn klights_controllers::ControllerIdentityGenerator>,
    #[cfg(not(test))]
    pub api_identity: Arc<dyn k8s_native_service::ApiIdentityGenerator>,
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    pub(crate) scheduler_bind_gate: Option<
        Arc<crate::bootstrap::composition_adapters::pod_native_adapter::SchedulerBindGateForTest>,
    >,
    #[cfg(not(test))]
    pub gc_coordination: Arc<dyn klights_reconcile_api::GcForegroundDeleteCoordination>,
}

#[derive(Clone)]
pub struct WorkerPodRepositoryBuildConfig {
    pub resource_query: Arc<dyn LeaderResourceQuery>,
    pub pod_workqueue_store: Arc<dyn klights_node_store::PodWorkqueueStore>,
    pub supervisor: Arc<TaskSupervisor>,
    pub metrics: Arc<SideEffectMetrics>,
    pub pod_network_cache: Arc<dyn klights_node_store::PodNetworkCache>,
    pub assignment_waiter: Arc<dyn klights_network_api::PodNetworkAssignmentWaiter>,
    pub outbox: Arc<klights_kubelet::node_outbox::Outbox>,
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

pub(crate) struct RootPodRepositoryParts {
    pub repository_parts: crate::kubelet::pod_repository::facade::PodRepositoryParts,
    pub api: Arc<PodApiService>,
    pub subresource: Arc<PodSubresourceService>,
    pub scheduling: Arc<dyn klights_pod_api::PodScheduling>,
}

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

#[cfg(any(test, feature = "pod-repository-test-support"))]
#[derive(Clone)]
struct RootInMemoryPodWorkqueueEntry {
    id: i64,
    kind: crate::kubelet::pod_repository::workqueue::PodWorkqueueKind,
    namespace: String,
    name: String,
    uid: String,
    payload: serde_json::Value,
    attempt_count: i64,
    next_attempt_at_ms: i64,
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
        dependencies: crate::kubelet::pod_repository::PodRepositoryAdapterDependencies,
        gc_delete: Arc<dyn klights_reconcile_api::GcPodDeleteSink>,
    ) -> crate::kubelet::pod_repository::PodRepositoryAdapters {
        let adapter = Arc::new(WorkerPodAdapters);
        let _ = dependencies;
        crate::kubelet::pod_repository::PodRepositoryAdapters {
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
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            test_scheduling: None,
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

fn legacy_workqueue_kind(
    kind: crate::kubelet::pod_repository::workqueue::PodWorkqueueKind,
) -> klights_node_store::PodWorkqueueKind {
    match kind {
        crate::kubelet::pod_repository::workqueue::PodWorkqueueKind::Pod => {
            klights_node_store::PodWorkqueueKind::Pod
        }
        crate::kubelet::pod_repository::workqueue::PodWorkqueueKind::Namespace => {
            klights_node_store::PodWorkqueueKind::Namespace
        }
    }
}

fn focused_workqueue_entry(
    row: klights_node_store::PodWorkqueueEntry,
) -> anyhow::Result<crate::kubelet::pod_repository::workqueue::PodWorkqueueEntry> {
    let (id, identity, payload, attempt_count, _next_due_ms) = row.into_parts();
    let (kind, pod) = identity.into_persisted();
    Ok(
        crate::kubelet::pod_repository::workqueue::PodWorkqueueEntry {
            id: id.get(),
            kind: match kind {
                klights_node_store::PodWorkqueueKind::Pod => {
                    crate::kubelet::pod_repository::workqueue::PodWorkqueueKind::Pod
                }
                klights_node_store::PodWorkqueueKind::Namespace => {
                    crate::kubelet::pod_repository::workqueue::PodWorkqueueKind::Namespace
                }
            },
            namespace: pod.namespace,
            name: pod.name,
            uid: pod.uid,
            payload: serde_json::from_slice(&payload)?,
            attempt_count,
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            next_attempt_at_ms: _next_due_ms.get(),
        },
    )
}

#[async_trait::async_trait]
impl crate::kubelet::pod_repository::workqueue::PodWorkqueuePersistence
    for RootPodWorkqueuePersistence
{
    async fn enqueue(
        &self,
        kind: crate::kubelet::pod_repository::workqueue::PodWorkqueueKind,
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
            self.test_rows
                .lock()
                .unwrap()
                .push(RootInMemoryPodWorkqueueEntry {
                    id,
                    kind,
                    namespace: pod.namespace.clone(),
                    name: pod.name.clone(),
                    uid: pod.uid.clone(),
                    payload,
                    attempt_count,
                    next_attempt_at_ms: now_ms.saturating_add(min_delay_ms),
                });
            let _ = last_error;
            Ok(())
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
    ) -> anyhow::Result<Option<crate::kubelet::pod_repository::workqueue::PodWorkqueueEntry>> {
        if let Some(node_local) = &self.node_local {
            return node_local
                .claim_due_work(klights_node_store::DueTimeMs::try_new(now_ms)?)
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
                let row = rows.remove(index);
                crate::kubelet::pod_repository::workqueue::PodWorkqueueEntry {
                    id: row.id,
                    kind: row.kind,
                    namespace: row.namespace,
                    name: row.name,
                    uid: row.uid,
                    payload: row.payload,
                    attempt_count: row.attempt_count,
                    #[cfg(any(test, feature = "pod-repository-test-support"))]
                    next_attempt_at_ms: row.next_attempt_at_ms,
                }
            }))
        }
    }

    async fn complete(&self, _id: i64) -> anyhow::Result<()> {
        if self.node_local.is_some() {
            // The durable port atomically removes the row when it is claimed,
            // preserving the existing node.db claim semantics.
            return Ok(());
        }
        #[cfg(not(any(test, feature = "pod-repository-test-support")))]
        return Err(anyhow::anyhow!(
            "production Pod workqueue persistence requires node-local storage"
        ));
        #[cfg(any(test, feature = "pod-repository-test-support"))]
        {
            self.test_rows.lock().unwrap().retain(|row| row.id != _id);
            Ok(())
        }
    }

    async fn record_failure(
        &self,
        row: crate::kubelet::pod_repository::workqueue::PodWorkqueueEntry,
        min_delay_ms: i64,
        error: &str,
    ) -> anyhow::Result<()> {
        let pod = PodIdentity::new(&row.namespace, &row.name, &row.uid);
        self.enqueue(
            row.kind,
            &pod,
            row.payload,
            row.attempt_count.saturating_add(1),
            min_delay_ms,
            Some(error),
        )
        .await
    }

    async fn dead_letter(&self, id: i64, error: &str) -> anyhow::Result<()> {
        let _ = error;
        self.complete(id).await
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
                dependencies.delete_coordinator.clone(),
                self.db.clone(),
                self.wall_clock.clone(),
                #[cfg(any(test, feature = "pod-repository-test-support"))]
                self.scheduler_bind_gate.clone(),
            );
        let pod_query: Arc<dyn klights_pod_api::PodQuery> = native.clone();
        let persistence: Arc<dyn klights_pod_api::PodPersistence> = native.clone();
        let status_persistence: Arc<dyn klights_pod_api::PodStatusPersistence> = native.clone();
        let deletion: Arc<dyn klights_pod_api::PodDeleteOrchestration> = native.clone();
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
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            test_scheduling: Some(scheduling.clone()),
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
) -> RootPodRepositoryParts {
    build_pod_repository_parts_inner(config, leader_coordination, None)
}

#[cfg(any())]
pub(crate) fn build_integration_pod_repository_parts(
    config: PodRepositoryBuildConfig,
    resource_query: Arc<dyn LeaderResourceQuery>,
) -> RootPodRepositoryParts {
    build_pod_repository_parts_inner(config, None, Some(resource_query))
}

#[cfg(feature = "pod-repository-test-support")]
pub(crate) fn build_integration_pod_repository_parts(
    config: PodRepositoryBuildConfig,
    resource_query: Arc<dyn LeaderResourceQuery>,
) -> RootPodRepositoryParts {
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
) -> RootPodRepositoryParts {
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
) -> RootPodRepositoryParts {
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
        controller_identity,
        #[cfg(not(test))]
        api_identity,
        #[cfg(any(test, feature = "pod-repository-test-support"))]
        scheduler_bind_gate,
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
    #[cfg(feature = "pod-repository-test-support")]
    let test_local_bound_finalization = persistence_parts.test_local_bound_finalization;
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
            delete_coordinator,
        });
    let delivery_outbox = outbox.map(|outbox| outbox as Arc<dyn klights_leader_api::NodeOutbox>);
    let bound_pod_finalization =
        crate::bootstrap::composition_adapters::bound_pod_finalization_adapter::new_for_root(
            store.clone(),
            local_bound_finalization,
            cluster_api.clone(),
            delivery_outbox.clone(),
            wall_clock.clone(),
        );
    let host_ip = klights_kubelet::context::HostIpState::default();
    let assignment_query = klights_kubelet::pod_repository::pod_network_assignment_query(
        pod_network_cache,
        supervisor.clone(),
        assignment_waiter,
        host_ip.clone(),
    );
    let repository_parts = PodRepository::build_parts_with_adapters(
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
        },
        PodRepositoryNetworkDependencies {
            assignment_query,
            host_ip,
        },
        PodRepositoryDeliveryDependencies {
            outbox: delivery_outbox,
            cluster_api,
            remote_metadata_delivery_required: false,
            bound_pod_finalization,
            #[cfg(feature = "pod-repository-test-support")]
            test_local_bound_finalization: Some(test_local_bound_finalization),
        },
        adapters,
    );
    RootPodRepositoryParts {
        repository_parts,
        api,
        subresource,
        scheduling,
    }
}

pub(crate) fn build_worker_pod_repository_parts(
    config: WorkerPodRepositoryBuildConfig,
) -> crate::kubelet::pod_repository::facade::PodRepositoryParts {
    let wall_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock> =
        Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock);
    let worker_persistence = Arc::new(WorkerPodPersistence {
        resource_query: config.resource_query.clone(),
    });
    let store = Arc::new(PodStore::from_persistence(
        worker_persistence.clone(),
        worker_persistence.clone(),
        wall_clock.clone(),
        Arc::new(std::sync::atomic::AtomicUsize::new(1)),
        #[cfg(any(test, feature = "pod-repository-test-support"))]
        None,
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
        wall_clock.clone(),
    );
    let adapters = WorkerPodAdapters::build(
        PodRepositoryAdapterDependencies {
            store: store.clone(),
            supervisor: config.supervisor.clone(),
            delete_coordinator: Arc::new(PodDeleteCoordinator::new(
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
    PodRepository::build_parts_with_adapters(
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
        },
        PodRepositoryNetworkDependencies {
            assignment_query,
            host_ip,
        },
        PodRepositoryDeliveryDependencies {
            outbox: Some(outbox),
            cluster_api: Some(config.resource_query),
            remote_metadata_delivery_required: true,
            bound_pod_finalization,
            #[cfg(feature = "pod-repository-test-support")]
            test_local_bound_finalization: None,
        },
        adapters,
    )
}
