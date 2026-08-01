//! Composition-root wiring for Pod repository API and reconcile adapters.

use std::sync::Arc;

impl From<crate::api::pod_repository_ports::ApiPodList> for crate::datastore::ResourceList {
    fn from(list: crate::api::pod_repository_ports::ApiPodList) -> Self {
        Self {
            items: list.items,
            resource_version: list.resource_version,
            watch_replay_position: None,
            continue_token: list.continue_token,
            remaining_item_count: list.remaining_item_count,
        }
    }
}

use crate::datastore::DatastoreHandle;
use crate::kubelet::pod_repository::delete_coordinator::PodDeleteCoordinator;
use crate::kubelet::pod_repository::state_only_writer::{StateOnlyWriter, StatusOnlyWriterService};
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
use crate::pod_api_service::{PodApiService, PodApiServiceDependencies};
use crate::pod_native_orchestration::{PodNativeOrchestration, PodNativeOrchestrationDependencies};
use crate::side_effects::SideEffectMetrics;
use crate::side_effects::SideEffectRegistry;
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
    #[cfg(test)]
    pub(crate) scheduler_bind_gate:
        Option<Arc<crate::pod_native_orchestration::SchedulerBindGateForTest>>,
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
    #[cfg(test)]
    scheduler_bind_gate: Option<Arc<crate::pod_native_orchestration::SchedulerBindGateForTest>>,
}

pub(crate) struct RootPodRepositoryParts {
    pub repository_parts: crate::kubelet::pod_repository::facade::PodRepositoryParts,
    pub api: Arc<PodApiService>,
    pub subresource: Arc<crate::pod_subresource_service::PodSubresourceService>,
    pub scheduling: Arc<dyn klights_pod_api::PodScheduling>,
}

#[derive(Clone)]
struct RootPodWorkqueuePersistence {
    node_local: Option<Arc<dyn klights_node_store::PodWorkqueueStore>>,
    #[cfg(test)]
    wall_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
    #[cfg(test)]
    test_rows: Arc<std::sync::Mutex<Vec<crate::datastore::node_local::PodWorkqueueEntry>>>,
    #[cfg(test)]
    test_next_id: Arc<std::sync::atomic::AtomicI64>,
}

struct RootPodPersistence {
    db: DatastoreHandle,
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
            #[cfg(test)]
            test_api: None,
            #[cfg(test)]
            test_subresource: None,
            #[cfg(test)]
            test_scheduling: None,
            #[cfg(test)]
            test_mark_terminating: None,
        }
    }
}

fn worker_persistence_unavailable(operation: &str) -> anyhow::Error {
    anyhow::Error::new(klights_pod_api::PodRepositoryError::unavailable(format!(
        "worker Pod persistence cannot perform leader-owned {operation}"
    )))
}

#[async_trait::async_trait]
impl crate::kubelet::pod_repository::store::PodPersistence for WorkerPodPersistence {
    async fn get(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        self.resource_query
            .get_resource(klights_leader_api::ResourceGetRequest::try_new(
                klights_types::ResourceKey::new("v1", "Pod", Some(namespace.to_string()), name),
                klights_leader_api::ResourceQueryConsistency::Cached,
            )?)
            .await
            .map_err(anyhow::Error::new)
    }

    async fn list(
        &self,
        namespace: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        limit: Option<i64>,
        continue_token: Option<&str>,
    ) -> anyhow::Result<crate::kubelet::pod_repository::PodResourceList> {
        let result = self
            .resource_query
            .list_resources(klights_leader_api::ResourceListRequest::try_new(
                "v1",
                "Pod",
                namespace.map(str::to_owned),
                label_selector.map(str::to_owned),
                field_selector.map(str::to_owned),
                limit,
                continue_token.map(str::to_owned),
                klights_leader_api::ResourceQueryConsistency::Cached,
            )?)
            .await
            .map_err(anyhow::Error::new)?;
        let (items, resource_version, _, continue_token, remaining_item_count) =
            result.into_parts();
        Ok(crate::kubelet::pod_repository::PodResourceList {
            items,
            resource_version,
            continue_token,
            remaining_item_count,
        })
    }

    async fn snapshot_list(
        &self,
        request: klights_pod_api::PodSnapshotListRequest,
    ) -> anyhow::Result<klights_pod_api::PodSnapshotListOutcome> {
        let list = request.list;
        let result = self
            .list(
                list.namespace(),
                list.label_selector(),
                list.field_selector(),
                list.limit(),
                list.continue_token(),
            )
            .await?;
        if result.resource_version < request.snapshot_resource_version {
            return Ok(klights_pod_api::PodSnapshotListOutcome::Current);
        }
        Ok(klights_pod_api::PodSnapshotListOutcome::List(
            klights_pod_api::PodListResult::try_new(
                result.items,
                result.resource_version,
                result.continue_token,
                result.remaining_item_count,
            )?,
        ))
    }

    async fn list_by_owner(
        &self,
        namespace: &str,
        owner_uid: &str,
    ) -> anyhow::Result<Vec<klights_cluster_core::Resource>> {
        Ok(self
            .list(Some(namespace), None, None, None, None)
            .await?
            .items
            .into_iter()
            .filter(|pod| {
                pod.data
                    .pointer("/metadata/ownerReferences")
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(|owners| {
                        owners.iter().any(|owner| {
                            owner.get("uid").and_then(serde_json::Value::as_str) == Some(owner_uid)
                        })
                    })
            })
            .collect())
    }

    async fn create(
        &self,
        _namespace: &str,
        _name: &str,
        _body: serde_json::Value,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        Err(worker_persistence_unavailable("create"))
    }

    async fn update(
        &self,
        _namespace: &str,
        _name: &str,
        _body: serde_json::Value,
        _preconditions: klights_cluster_core::ResourcePreconditions,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        Err(worker_persistence_unavailable("update"))
    }

    async fn patch_latest(
        &self,
        _namespace: &str,
        _name: &str,
        _patch_kind: crate::datastore::PatchKind,
        _patch: serde_json::Value,
        _preconditions: klights_cluster_core::ResourcePreconditions,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        Err(worker_persistence_unavailable("patch"))
    }

    async fn update_status(
        &self,
        _namespace: &str,
        _name: &str,
        _status: serde_json::Value,
        _preconditions: klights_cluster_core::ResourcePreconditions,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        Err(worker_persistence_unavailable("status update"))
    }

    async fn delete(
        &self,
        _namespace: &str,
        _name: &str,
        _preconditions: klights_cluster_core::ResourcePreconditions,
    ) -> anyhow::Result<crate::kubelet::pod_repository::store::PodDeleteCasOutcome> {
        Err(worker_persistence_unavailable("delete"))
    }

    fn log_status_noop(
        &self,
        namespace: &str,
        name: &str,
        resource: &klights_cluster_core::Resource,
    ) {
        tracing::debug!(
            namespace,
            name,
            resource_version = resource.resource_version,
            "worker Pod status write was already current"
        );
    }
}

pub(crate) fn new_pod_store(
    db: DatastoreHandle,
) -> crate::kubelet::pod_repository::store::PodStore {
    crate::kubelet::pod_repository::store::PodStore::from_persistence(
        Arc::new(RootPodPersistence { db }),
        Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
    )
}

fn pod_persistence_error(error: anyhow::Error, namespace: &str, name: &str) -> anyhow::Error {
    if let Some(error) = error.downcast_ref::<klights_cluster_core::StorageMutationError>() {
        use klights_cluster_core::StorageCommandRejectionCode;

        let message = error.message().to_string();
        return match error.rejection_code() {
            Some(StorageCommandRejectionCode::AlreadyExists) => {
                anyhow::Error::new(klights_pod_api::PodRepositoryError::already_exists(message))
            }
            Some(StorageCommandRejectionCode::Conflict) => {
                anyhow::Error::new(klights_pod_api::PodRepositoryError::conflict(message))
            }
            Some(StorageCommandRejectionCode::NotFound) => anyhow::Error::new(
                klights_pod_api::PodRepositoryError::not_found(namespace, name),
            ),
            Some(StorageCommandRejectionCode::InvalidCommit) => {
                anyhow::Error::new(klights_pod_api::PodRepositoryError::internal(message))
            }
            None => anyhow::Error::new(klights_pod_api::PodRepositoryError::unavailable(message)),
        };
    }
    if let Some(error) = error.downcast_ref::<klights_cluster_datastore::errors::DatastoreError>() {
        return match error {
            klights_cluster_datastore::errors::DatastoreError::AlreadyExists { message } => {
                anyhow::Error::new(klights_pod_api::PodRepositoryError::already_exists(message))
            }
            klights_cluster_datastore::errors::DatastoreError::Conflict { message } => {
                anyhow::Error::new(klights_pod_api::PodRepositoryError::conflict(message))
            }
            klights_cluster_datastore::errors::DatastoreError::NotFound { .. } => {
                anyhow::Error::new(klights_pod_api::PodRepositoryError::not_found(
                    namespace, name,
                ))
            }
        };
    }
    anyhow::Error::new(klights_pod_api::PodRepositoryError::unavailable(
        error.to_string(),
    ))
}

#[async_trait::async_trait]
impl crate::kubelet::pod_repository::store::PodPersistence for RootPodPersistence {
    async fn get(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        self.db
            .get_resource("v1", "Pod", Some(namespace), name)
            .await
            .map_err(|error| pod_persistence_error(error, namespace, name))
    }

    async fn list(
        &self,
        namespace: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        limit: Option<i64>,
        continue_token: Option<&str>,
    ) -> anyhow::Result<crate::kubelet::pod_repository::PodResourceList> {
        let list = self
            .db
            .list_resources(
                "v1",
                "Pod",
                namespace,
                crate::datastore::ResourceListQuery::new(
                    label_selector,
                    field_selector,
                    limit,
                    continue_token,
                ),
            )
            .await
            .map_err(|error| {
                pod_persistence_error(error, namespace.unwrap_or_default(), "Pod list")
            })?;
        Ok(crate::kubelet::pod_repository::PodResourceList {
            items: list.items,
            resource_version: list.resource_version,
            continue_token: list.continue_token,
            remaining_item_count: list.remaining_item_count,
        })
    }

    async fn snapshot_list(
        &self,
        request: klights_pod_api::PodSnapshotListRequest,
    ) -> anyhow::Result<klights_pod_api::PodSnapshotListOutcome> {
        let list = request.list;
        let snapshot = self
            .db
            .snapshot_resources_at_rv(
                "v1",
                "Pod",
                list.namespace(),
                crate::datastore::ResourceListQuery::new(
                    list.label_selector(),
                    list.field_selector(),
                    list.limit(),
                    list.continue_token(),
                ),
                request.snapshot_resource_version,
            )
            .await?;
        Ok(match snapshot {
            crate::datastore::SnapshotAtRv::List(list) => {
                klights_pod_api::PodSnapshotListOutcome::List(
                    klights_pod_api::PodListResult::try_new(
                        list.items,
                        list.resource_version,
                        list.continue_token,
                        list.remaining_item_count,
                    )?,
                )
            }
            crate::datastore::SnapshotAtRv::Current => {
                klights_pod_api::PodSnapshotListOutcome::Current
            }
            crate::datastore::SnapshotAtRv::Expired => {
                klights_pod_api::PodSnapshotListOutcome::Expired
            }
        })
    }

    async fn list_by_owner(
        &self,
        namespace: &str,
        owner_uid: &str,
    ) -> anyhow::Result<Vec<klights_cluster_core::Resource>> {
        self.db
            .list_resources_by_owner_uid("v1", "Pod", Some(namespace), owner_uid)
            .await
            .map_err(|error| pod_persistence_error(error, namespace, "Pod owner list"))
    }

    async fn create(
        &self,
        namespace: &str,
        name: &str,
        body: serde_json::Value,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        self.db
            .create_resource("v1", "Pod", Some(namespace), name, body)
            .await
            .map_err(|error| pod_persistence_error(error, namespace, name))
    }

    async fn update(
        &self,
        namespace: &str,
        name: &str,
        body: serde_json::Value,
        preconditions: klights_cluster_core::ResourcePreconditions,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        self.db
            .update_resource_with_preconditions(
                "v1",
                "Pod",
                Some(namespace),
                name,
                body,
                preconditions,
            )
            .await
            .map_err(|error| pod_persistence_error(error, namespace, name))
    }

    async fn patch_latest(
        &self,
        namespace: &str,
        name: &str,
        patch_kind: klights_cluster_core::PatchKind,
        patch: serde_json::Value,
        preconditions: klights_cluster_core::ResourcePreconditions,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        self.db
            .patch_resource_latest_with_preconditions(
                "v1",
                "Pod",
                Some(namespace),
                name,
                crate::datastore::ResourcePatchRequest::new(patch_kind, patch, preconditions),
            )
            .await
            .map_err(|error| pod_persistence_error(error, namespace, name))
    }

    async fn update_status(
        &self,
        namespace: &str,
        name: &str,
        status: serde_json::Value,
        preconditions: klights_cluster_core::ResourcePreconditions,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        self.db
            .update_status_only_with_preconditions(
                "v1",
                "Pod",
                Some(namespace),
                name,
                status,
                preconditions,
            )
            .await
            .map_err(|error| pod_persistence_error(error, namespace, name))
    }

    async fn delete(
        &self,
        namespace: &str,
        name: &str,
        preconditions: klights_cluster_core::ResourcePreconditions,
    ) -> anyhow::Result<crate::kubelet::pod_repository::store::PodDeleteCasOutcome> {
        match self
            .db
            .delete_resource_with_preconditions("v1", "Pod", Some(namespace), name, preconditions)
            .await
        {
            Ok(()) => Ok(crate::kubelet::pod_repository::store::PodDeleteCasOutcome::Removed),
            Err(error) if klights_cluster_datastore::errors::is_conflict_error(&error) => {
                Ok(crate::kubelet::pod_repository::store::PodDeleteCasOutcome::Conflict)
            }
            Err(error)
                if error
                    .downcast_ref::<klights_cluster_datastore::errors::DatastoreError>()
                    .is_some_and(|error| {
                        matches!(
                            error,
                            klights_cluster_datastore::errors::DatastoreError::NotFound { .. }
                        )
                    }) =>
            {
                Ok(crate::kubelet::pod_repository::store::PodDeleteCasOutcome::Gone)
            }
            Err(error) => Err(pod_persistence_error(error, namespace, name)),
        }
    }

    fn log_status_noop(
        &self,
        namespace: &str,
        name: &str,
        resource: &klights_cluster_core::Resource,
    ) {
        crate::resource_write_diagnostics::log_noop_resource_write(
            crate::resource_write_diagnostics::NoopResourceWrite {
                operation: "pod_store_update_status",
                api_version: "v1",
                kind: "Pod",
                namespace: Some(namespace),
                name,
                uid: &resource.uid,
                resource_version: resource.resource_version,
                reason: "pod status unchanged",
            },
        );
    }

    #[cfg(test)]
    fn subscribe_watch(&self) -> tokio::sync::broadcast::Receiver<crate::watch::WatchEvent> {
        self.db
            .subscribe_watch(klights_watch::WatchTopic::new("v1", "Pod"))
    }

    #[cfg(test)]
    fn legacy_db(&self) -> DatastoreHandle {
        self.db.clone()
    }
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
            #[cfg(test)]
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
        #[cfg(not(test))]
        return Err(anyhow::anyhow!(
            "production Pod workqueue persistence requires node-local storage"
        ));
        #[cfg(test)]
        {
            let id = self
                .test_next_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let now_ms = self.wall_clock.now_ms();
            self.test_rows
                .lock()
                .unwrap()
                .push(crate::datastore::node_local::PodWorkqueueEntry {
                    id,
                    kind: match legacy_workqueue_kind(kind) {
                        klights_node_store::PodWorkqueueKind::Pod => {
                            crate::datastore::node_local::PodWorkqueueKind::Pod
                        }
                        klights_node_store::PodWorkqueueKind::Namespace => {
                            crate::datastore::node_local::PodWorkqueueKind::Namespace
                        }
                    },
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
        #[cfg(not(test))]
        return Err(anyhow::anyhow!(
            "production Pod workqueue persistence requires node-local storage"
        ));
        #[cfg(test)]
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
        #[cfg(not(test))]
        return Err(anyhow::anyhow!(
            "production Pod workqueue persistence requires node-local storage"
        ));
        #[cfg(test)]
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
                    kind: match row.kind {
                        crate::datastore::node_local::PodWorkqueueKind::Pod => {
                            crate::kubelet::pod_repository::workqueue::PodWorkqueueKind::Pod
                        }
                        crate::datastore::node_local::PodWorkqueueKind::Namespace => {
                            crate::kubelet::pod_repository::workqueue::PodWorkqueueKind::Namespace
                        }
                    },
                    namespace: row.namespace,
                    name: row.name,
                    uid: row.uid,
                    payload: row.payload,
                    attempt_count: row.attempt_count,
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
        #[cfg(not(test))]
        return Err(anyhow::anyhow!(
            "production Pod workqueue persistence requires node-local storage"
        ));
        #[cfg(test)]
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

impl RootPodRepositoryComposition {
    fn build(
        &self,
        dependencies: PodRepositoryAdapterDependencies,
    ) -> (
        PodRepositoryAdapters,
        Arc<PodApiService>,
        Arc<crate::pod_subresource_service::PodSubresourceService>,
        Arc<dyn klights_pod_api::PodScheduling>,
    ) {
        let pod_reconcile = Arc::new(
            crate::pod_reconcile_adapter::PodReconcileAdapter::new_with_coordination(
                self.db.clone(),
                self.side_effects.controller_dispatcher_slot(),
                self.metrics.clone(),
                self.side_effects.clone(),
                dependencies.store.clone(),
                self.gc_coordination.clone(),
            ),
        );
        let native = crate::pod_native_adapter::RootPodNativeAdapter::new(
            dependencies.store.clone(),
            dependencies.status_only.clone(),
            dependencies.delete_coordinator.clone(),
            self.db.clone(),
            self.wall_clock.clone(),
        );
        let pod_query: Arc<dyn klights_pod_api::PodQuery> = native.clone();
        let persistence: Arc<dyn klights_pod_api::PodPersistence> = native.clone();
        let status_persistence: Arc<dyn klights_pod_api::PodStatusPersistence> = native.clone();
        let subresource = Arc::new(crate::pod_subresource_service::PodSubresourceService::new(
            pod_query.clone(),
            persistence.clone(),
            status_persistence.clone(),
            pod_reconcile.clone(),
        ));
        let native_orchestration = Arc::new(PodNativeOrchestration::new(
            PodNativeOrchestrationDependencies {
                pod_query,
                persistence,
                status_persistence,
                deletion: native.clone(),
                event_sink: native.clone(),
                placement: native.clone(),
                admission_resources: native.clone(),
                spec_validation: native,
                admission: crate::resource_admission_adapter::ResourceAdmissionAdapter::new(
                    self.db.clone(),
                ),
                resource_query: self.resource_query.clone(),
                quota_runtime:
                    crate::resource_quota_admission_adapter::ResourceQuotaAdmissionAdapter::new(
                        self.db.clone(),
                    ),
                supervisor: dependencies.supervisor,
                gc_reconcile: pod_reconcile.clone(),
                service_reconcile: pod_reconcile.clone(),
                mutation_effects:
                    crate::resource_mutation_effects_adapter::ResourceMutationEffectsAdapter::new(
                        self.side_effects.clone(),
                        self.metrics.clone(),
                    ),
                metrics: self.metrics.clone(),
                wall_clock: self.wall_clock.clone(),
            },
        ));
        #[cfg(test)]
        if let Some(gate) = self.scheduler_bind_gate.clone() {
            native_orchestration.set_scheduler_bind_gate_for_test(gate);
        }
        let mutation: Arc<dyn klights_pod_api::PodApiMutation> = native_orchestration.clone();
        let gc_delete: Arc<dyn klights_reconcile_api::GcPodDeleteSink> =
            native_orchestration.clone();
        let api = Arc::new(PodApiService::new(PodApiServiceDependencies {
            mutation,
            gc_delete,
        }));
        let scheduling: Arc<dyn klights_pod_api::PodScheduling> =
            crate::pod_scheduler_service::PodSchedulerService::new(native_orchestration.clone());
        #[cfg(test)]
        let mark_terminating: Arc<dyn klights_pod_api::PodMarkTerminating> = native_orchestration;
        let adapters = PodRepositoryAdapters {
            gc_delete: api.clone(),
            gc_reconcile: pod_reconcile.clone(),
            pdb_reconcile: pod_reconcile.clone(),
            eviction_admission: pod_reconcile.clone(),
            namespace_bootstrap: pod_reconcile.clone(),
            namespace_termination: pod_reconcile.clone(),
            mutation_reconcile: pod_reconcile,
            #[cfg(test)]
            test_api: Some(api.clone()),
            #[cfg(test)]
            test_subresource: Some(subresource.clone()),
            #[cfg(test)]
            test_scheduling: Some(scheduling.clone()),
            #[cfg(test)]
            test_mark_terminating: Some(mark_terminating),
        };
        (adapters, api, subresource, scheduling)
    }
}

pub(crate) fn build_pod_repository_parts(
    config: PodRepositoryBuildConfig,
    leader_coordination: Option<Arc<dyn klights_leader_api::ControllerCoordination>>,
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
        #[cfg(test)]
        scheduler_bind_gate,
        #[cfg(not(test))]
        gc_coordination,
    } = config;
    #[cfg(test)]
    let gc_coordination: Arc<dyn klights_reconcile_api::GcForegroundDeleteCoordination> =
        Arc::new(crate::controllers::ControllerCoordination::new());
    let _ = scheduling_mode;
    #[cfg(not(test))]
    let resource_query = cluster_api
        .clone()
        .expect("production Pod repository requires a leader resource query");
    #[cfg(test)]
    let resource_query = cluster_api.clone().unwrap_or_else(|| {
        Arc::new(crate::control_plane::client::local::LocalApiClient::new(
            db.clone(),
            "local-node".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ))
    });
    let wall_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock> =
        Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock);
    let store = Arc::new(PodStore::from_persistence(
        Arc::new(RootPodPersistence { db: db.clone() }),
        wall_clock.clone(),
    ));
    let workqueue_persistence = RootPodWorkqueuePersistence {
        node_local: pod_workqueue_store,
        #[cfg(test)]
        wall_clock: wall_clock.clone(),
        #[cfg(test)]
        test_rows: Arc::new(std::sync::Mutex::new(Vec::new())),
        #[cfg(test)]
        test_next_id: Arc::new(std::sync::atomic::AtomicI64::new(1)),
    };
    let workqueue = if let Some(leader_coordination) = leader_coordination {
        PodWorkqueue::new_leader(
            store.clone(),
            workqueue_persistence,
            supervisor.clone(),
            metrics.clone(),
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
    let status_only: Arc<dyn StateOnlyWriter> =
        Arc::new(StatusOnlyWriterService::new(store.clone()));
    let delete_coordinator = Arc::new(PodDeleteCoordinator::new(
        store.clone(),
        workqueue.clone(),
        supervisor.clone(),
        metrics.clone(),
        wall_clock.clone(),
    ));
    let (adapters, api, subresource, scheduling) = RootPodRepositoryComposition {
        db: db.clone(),
        resource_query,
        side_effects: side_effects.clone(),
        metrics: metrics.clone(),
        gc_coordination,
        wall_clock: Arc::new(klights_supervisor::SystemWallClock),
        #[cfg(test)]
        scheduler_bind_gate,
    }
    .build(PodRepositoryAdapterDependencies {
        store: store.clone(),
        status_only: status_only.clone(),
        supervisor: supervisor.clone(),
        delete_coordinator,
    });
    let delivery_outbox = outbox.map(|outbox| outbox as Arc<dyn klights_leader_api::NodeOutbox>);
    let bound_pod_finalization = crate::bound_pod_finalization_adapter::new_for_root(
        store.clone(),
        cluster_api.clone(),
        delivery_outbox.clone(),
        wall_clock.clone(),
    );
    let repository_parts = PodRepository::build_parts_with_adapters(
        PodRepositoryCoreDependencies {
            store,
            status_only,
            workqueue,
        },
        PodRepositoryRuntimeDependencies {
            supervisor,
            metrics: metrics.clone(),
            wall_clock,
        },
        PodRepositoryNetworkDependencies {
            pod_network_cache,
            assignment_waiter,
        },
        PodRepositoryDeliveryDependencies {
            outbox: delivery_outbox,
            cluster_api,
            bound_pod_finalization,
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
    let store = Arc::new(PodStore::from_persistence(
        Arc::new(WorkerPodPersistence {
            resource_query: config.resource_query.clone(),
        }),
        wall_clock.clone(),
    ));
    let workqueue = PodWorkqueue::new(
        store.clone(),
        RootPodWorkqueuePersistence {
            node_local: Some(config.pod_workqueue_store),
            #[cfg(test)]
            wall_clock: wall_clock.clone(),
            #[cfg(test)]
            test_rows: Arc::new(std::sync::Mutex::new(Vec::new())),
            #[cfg(test)]
            test_next_id: Arc::new(std::sync::atomic::AtomicI64::new(1)),
        },
        config.supervisor.clone(),
        config.metrics.clone(),
        wall_clock.clone(),
    );
    let status_only: Arc<dyn StateOnlyWriter> =
        Arc::new(StatusOnlyWriterService::new(store.clone()));
    let outbox: Arc<dyn klights_leader_api::NodeOutbox> = config.outbox;
    let root_deletion = crate::bound_pod_finalization_adapter::RootBoundPodFinalization::new(
        store.clone(),
        Some(config.resource_query.clone()),
        Some(outbox.clone()),
        wall_clock.clone(),
    );
    let adapters = WorkerPodAdapters::build(
        PodRepositoryAdapterDependencies {
            store: store.clone(),
            status_only: status_only.clone(),
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
    PodRepository::build_parts_with_adapters(
        PodRepositoryCoreDependencies {
            store,
            status_only,
            workqueue,
        },
        PodRepositoryRuntimeDependencies {
            supervisor: config.supervisor,
            metrics: config.metrics,
            wall_clock,
        },
        PodRepositoryNetworkDependencies {
            pod_network_cache: config.pod_network_cache,
            assignment_waiter: config.assignment_waiter,
        },
        PodRepositoryDeliveryDependencies {
            outbox: Some(outbox),
            cluster_api: Some(config.resource_query),
            bound_pod_finalization,
        },
        adapters,
    )
}

#[cfg(test)]
mod pod_persistence_error_tests {
    use klights_cluster_core::{StorageCommandRejectionCode, StorageMutationError};
    use klights_pod_api::PodRepositoryError;

    use super::pod_persistence_error;

    #[test]
    fn neutral_already_exists_rejection_preserves_pod_repository_category() {
        let mapped = pod_persistence_error(
            StorageMutationError::rejected(
                StorageCommandRejectionCode::AlreadyExists,
                "Resource already exists (409 Conflict)",
            )
            .into(),
            "default",
            "duplicate",
        );

        assert!(matches!(
            mapped.downcast_ref::<PodRepositoryError>(),
            Some(PodRepositoryError::AlreadyExists { .. })
        ));
    }

    #[test]
    fn neutral_conflict_rejection_preserves_pod_repository_category() {
        let mapped = pod_persistence_error(
            StorageMutationError::rejected(
                StorageCommandRejectionCode::Conflict,
                "resourceVersion precondition failed (409 Conflict)",
            )
            .into(),
            "default",
            "stale",
        );

        assert!(matches!(
            mapped.downcast_ref::<PodRepositoryError>(),
            Some(PodRepositoryError::Conflict { .. })
        ));
    }
}
