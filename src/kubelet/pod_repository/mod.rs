//! `PodRepository` — single production boundary for `v1/Pod` persistence.
//!
//! The repository owns kubelet lifecycle, workload-controller, accounting-
//! controller, API pod subresource, AND the main API pod create / update /
//! patch / delete / list paths. `("v1","Pod",...)` does not appear as a
//! `DatastoreBackend` argument outside [`store::PodStore`].
//!
//! Internal services depend on `Arc<PodStore>` rather than
//! `DatastoreHandle`, which localizes the pod-shaped DB boundary to a
//! single file. Network-runtime tables (`pod_network`, `sandbox`) and
//! `NetworkProvider::cni_*` calls remain with their existing owners and
//! are not policed by this boundary.

use std::sync::Arc;

use crate::kubelet::pod_repository::api::PodSchedulingMode;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
#[cfg(test)]
use tokio::sync::broadcast;

use crate::api::{AppError, DeleteOptions};
use crate::control_plane::client::{LeaderApiClient, ListRequest, ResourceKey};
use crate::controllers::gc::GcPodDeleteSink;
use crate::datastore::{DatastoreHandle, Resource, ResourceList};
use crate::kubelet::pod_runtime::deletion_finalizer::PodDeletionFinalizer;
use crate::kubelet::pod_runtime::service::PodDeletionFinalizeResult;
use crate::side_effects::{SideEffectMetrics, SideEffectRegistry};
use crate::task_supervisor::TaskSupervisor;
#[cfg(test)]
use crate::watch::WatchEvent;

pub mod api;
pub mod background;
pub mod facade;
pub mod network;
pub mod objects;
pub mod state_only_writer;
pub mod status;
pub mod store;
pub mod subresource;
pub mod types;
pub mod watch;
pub mod workqueue;

#[cfg(test)]
mod tests;

pub use api::PodRepositoryBuildConfig;
pub use types::{
    PodApiCreateRequest, PodApiCreateResult, PodApiDeleteOutcome, PodApiUpdateOutcome,
    PodNetworkAssignment, PodStatusPatchType, PodStatusUpdate, RuntimeReconcileStatus,
    content_type_to_patch_type,
};

use api::{PodApiService, PodApiServiceDependencies};
use background::PodRepositoryBackground;
use network::PodNetworkService;
use objects::PodObjectService;
use state_only_writer::StatusOnlyWriterService;
use status::PodStatusService;
use store::PodStore;
use subresource::PodSubresourceService;
use watch::PodWatchService;
use workqueue::PodWorkqueue;

#[async_trait]
pub trait PodReader: Send + Sync {
    async fn get_pod(&self, ns: &str, name: &str) -> Result<Option<Resource>>;
    async fn get_pod_for_uid(&self, ns: &str, name: &str, uid: &str) -> Result<Option<Resource>>;
    async fn list_pods(
        &self,
        ns: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        limit: Option<i64>,
        continue_token: Option<&str>,
    ) -> Result<ResourceList>;
    async fn list_pods_by_owner_uid(&self, ns: &str, owner_uid: &str) -> Result<Vec<Resource>>;
}

#[async_trait]
pub trait PodStatusWriter: Send + Sync {
    /// LEGACY: prefer `set_pod_status_for_uid`. This variant does not
    /// gate the write on pod UID. Production code MUST NOT use this —
    /// stale events for a deleted pod can fold into a same-name
    /// recreated pod and corrupt its status. Retained for legacy test
    /// scaffolding that doesn't have a stable UID at construction time.
    async fn set_pod_status(
        &self,
        ns: &str,
        name: &str,
        update: PodStatusUpdate,
        expected_rv: Option<i64>,
    ) -> Result<Resource>;

    /// UID-bound status write. All production callers MUST use this.
    async fn set_pod_status_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        update: PodStatusUpdate,
        expected_rv: Option<i64>,
    ) -> Result<Resource>;

    /// LEGACY no-UID variant. Tests call this. Production MUST use
    /// `apply_runtime_reconcile_status_for_uid` to gate stale CRI
    /// events.
    async fn apply_runtime_reconcile_status(
        &self,
        ns: &str,
        name: &str,
        update: RuntimeReconcileStatus,
        expected_rv: Option<i64>,
    ) -> Result<Resource>;

    async fn apply_runtime_reconcile_status_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        update: RuntimeReconcileStatus,
        expected_rv: Option<i64>,
    ) -> Result<Resource>;

    /// UID-bound retry-status write for retryable StartPod failures
    /// (image pull, CNI readiness, transient CRI connectivity).
    ///
    /// Writes `containerStatuses[].state.waiting.reason` (escalating
    /// `ErrImagePull` → `ImagePullBackOff` for repeated pull failures) and
    /// `waiting.message` (the underlying error). Phase stays `Pending` so
    /// controller-owned pods are not counted as terminal.
    async fn mark_start_pending_for_retry_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        error_message: &str,
    ) -> Result<Resource>;

    /// LEGACY no-UID variant. Production MUST use
    /// `set_probe_readiness_for_uid`.
    async fn set_probe_readiness(
        &self,
        ns: &str,
        name: &str,
        container_name: &str,
        ready: bool,
        expected_rv: Option<i64>,
    ) -> Result<Resource>;

    async fn set_probe_readiness_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        container_name: &str,
        ready: bool,
        expected_rv: Option<i64>,
    ) -> Result<Resource>;

    /// LEGACY no-UID variant. Production MUST use
    /// `set_deadline_exceeded_for_uid`.
    async fn set_deadline_exceeded(
        &self,
        ns: &str,
        name: &str,
        message: String,
        expected_rv: Option<i64>,
    ) -> Result<Resource>;

    async fn set_deadline_exceeded_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        message: String,
        expected_rv: Option<i64>,
    ) -> Result<Resource>;

    /// Replace `status.ephemeralContainerStatuses` with the given slice
    /// while preserving the rest of `status`. Used by the runtime
    /// reconciler when CRI reports state for `kubectl debug`'s ephemeral
    /// containers. LEGACY no-UID; production uses `_for_uid` variant.
    async fn apply_ephemeral_container_statuses(
        &self,
        ns: &str,
        name: &str,
        statuses: Vec<Value>,
        expected_rv: Option<i64>,
    ) -> Result<Resource>;

    async fn apply_ephemeral_container_statuses_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        statuses: Vec<Value>,
        expected_rv: Option<i64>,
    ) -> Result<Resource>;

    /// Bump `containerStatuses[name=container].restartCount` by 1 and
    /// stamp `lastState` with the supplied terminated descriptor.
    /// Returns `Ok(None)` if the container is not yet present in
    /// `containerStatuses` (next runtime reconcile will create it).
    /// LEGACY no-UID; production uses `note_container_restart_for_uid`.
    async fn note_container_restart(
        &self,
        ns: &str,
        name: &str,
        container_name: &str,
        terminated: Value,
        expected_rv: Option<i64>,
    ) -> Result<Option<Resource>>;

    async fn note_container_restart_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        container_name: &str,
        terminated: Value,
        expected_rv: Option<i64>,
    ) -> Result<Option<Resource>>;
}

#[async_trait]
pub trait PodMetadataWriter: Send + Sync {
    async fn record_sandbox_id(&self, ns: &str, name: &str, sandbox_id: &str) -> Result<Resource>;

    async fn record_sandbox_id_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        sandbox_id: &str,
    ) -> Result<Resource>;
}

#[async_trait]
pub trait PodObjectWriter: Send + Sync {
    /// Controller-driven Pod create. Internally delegates to
    /// `PodApiWriter::api_create_pod` with `run_admission=true,
    /// dry_run=false`.
    async fn create_controller_pod(
        &self,
        ns: &str,
        name: &str,
        node_name: &str,
        pod: Value,
    ) -> Result<Resource>;

    async fn delete_pod(&self, ns: &str, name: &str) -> Result<()>;

    async fn update_pod_owner_references(
        &self,
        ns: &str,
        name: &str,
        owner_refs: Vec<Value>,
    ) -> Result<Resource>;

    /// UID-gated variant: fails if the live Pod UID does not match
    /// `expected_uid`, protecting same-name replacements.
    async fn update_pod_owner_references_for_uid(
        &self,
        ns: &str,
        name: &str,
        expected_uid: &str,
        owner_refs: Vec<Value>,
    ) -> Result<Resource> {
        let _ = expected_uid;
        self.update_pod_owner_references(ns, name, owner_refs).await
    }

    async fn merge_pod_labels(
        &self,
        ns: &str,
        name: &str,
        labels: Vec<(String, String)>,
    ) -> Result<Resource>;

    /// UID-gated variant: fails if the live Pod UID does not match
    /// `expected_uid`, protecting same-name replacements.
    async fn merge_pod_labels_for_uid(
        &self,
        ns: &str,
        name: &str,
        expected_uid: &str,
        labels: Vec<(String, String)>,
    ) -> Result<Resource> {
        let _ = expected_uid;
        self.merge_pod_labels(ns, name, labels).await
    }
}

#[async_trait]
pub trait PodSubresourceWriter: Send + Sync {
    /// PUT `/api/v1/.../pods/{name}/status`
    async fn replace_status_from_api(
        &self,
        ns: &str,
        name: &str,
        status: Value,
        expected_rv: i64,
    ) -> Result<Resource>;

    /// UID-gated variant for same-name replacement protection.
    async fn replace_status_from_api_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        status: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        let _ = pod_uid;
        self.replace_status_from_api(ns, name, status, expected_rv)
            .await
    }

    /// PATCH `/api/v1/.../pods/{name}/status` — `patch_type` carries the
    /// request content type.
    async fn patch_status_from_api(
        &self,
        ns: &str,
        name: &str,
        patch: Value,
        patch_type: PodStatusPatchType,
        expected_rv: i64,
    ) -> Result<Resource>;

    /// UID-gated variant for same-name replacement protection.
    async fn patch_status_from_api_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        patch: Value,
        patch_type: PodStatusPatchType,
        expected_rv: i64,
    ) -> Result<Resource> {
        let _ = pod_uid;
        self.patch_status_from_api(ns, name, patch, patch_type, expected_rv)
            .await
    }

    /// PATCH `/api/v1/.../pods/{name}/ephemeralcontainers`
    async fn update_ephemeral_containers(
        &self,
        ns: &str,
        name: &str,
        containers: Vec<Value>,
        expected_rv: i64,
    ) -> Result<Resource>;

    /// UID-gated variant for same-name replacement protection.
    async fn update_ephemeral_containers_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        containers: Vec<Value>,
        expected_rv: i64,
    ) -> Result<Resource> {
        let _ = pod_uid;
        self.update_ephemeral_containers(ns, name, containers, expected_rv)
            .await
    }
}

#[async_trait]
pub trait PodNetworkReader: Send + Sync {
    /// Read the assignment CRI/CNI produced. `host_network=true` returns
    /// the host IP in both fields. Otherwise reads the `pod_network` row
    /// written by the klights CNI shim during containerd `RunPodSandbox`,
    /// waiting on the CNI assignment notification when the row is not visible
    /// on the first read.
    async fn read_pod_network_assignment(
        &self,
        sandbox_id: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        host_network: bool,
    ) -> Result<PodNetworkAssignment>;
}

#[cfg(test)]
pub trait PodWatchSource: Send + Sync {
    fn subscribe_pod_watch(&self) -> broadcast::Receiver<WatchEvent>;
}

#[async_trait]
pub trait PodApiWriter: Send + Sync {
    async fn api_create_pod(
        &self,
        request: PodApiCreateRequest,
    ) -> std::result::Result<PodApiCreateResult, AppError>;

    async fn api_update_pod(
        &self,
        ns: &str,
        name: &str,
        body: Value,
        current: Resource,
        dry_run: bool,
    ) -> std::result::Result<PodApiUpdateOutcome, AppError>;

    async fn api_patch_pod(
        &self,
        ns: &str,
        name: &str,
        patch: Value,
        patch_type: PodStatusPatchType,
        dry_run: bool,
    ) -> std::result::Result<PodApiUpdateOutcome, AppError>;

    async fn api_delete_pod(
        &self,
        ns: &str,
        name: &str,
        options: DeleteOptions,
        dry_run: bool,
    ) -> std::result::Result<PodApiDeleteOutcome, AppError>;

    async fn api_delete_collection_pods(
        &self,
        ns: &str,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        dry_run: bool,
    ) -> std::result::Result<(), AppError>;
}

/// Eight-trait pod persistence repository. Constructed once at process
/// startup by `AppState`, then shared by every consumer behind narrow
/// trait references.
pub struct PodRepository {
    store: Arc<PodStore>,
    status: PodStatusService,
    objects: PodObjectService,
    subresource: PodSubresourceService,
    network_svc: PodNetworkService,
    _watch: PodWatchService,
    api: Arc<PodApiService>,
    workqueue: Arc<PodWorkqueue>,
    side_effects: Arc<SideEffectRegistry>,
    metrics: Arc<SideEffectMetrics>,
    supervisor: Arc<TaskSupervisor>,
    outbox: Option<Arc<crate::kubelet::outbox::Outbox>>,
    cluster_api: Option<Arc<dyn LeaderApiClient>>,
    deletion_finalizer:
        Arc<crate::kubelet::pod_runtime::deletion_finalizer::RealPodDeletionFinalizer>,
}

fn ensure_pod_uid_matches(data: &Value, expected_uid: &str, ns: &str, name: &str) -> Result<()> {
    let live_uid = data
        .pointer("/metadata/uid")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    if live_uid == expected_uid {
        return Ok(());
    }

    Err(anyhow::anyhow!(
        "Pod {}/{} UID mismatch: expected {}, found {}",
        ns,
        name,
        expected_uid,
        if live_uid.is_empty() {
            "<empty>"
        } else {
            live_uid
        }
    ))
}

impl PodRepository {
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
        self.store.sandbox_gc_dirty.clone()
    }

    pub fn outbox(&self) -> Option<&crate::kubelet::outbox::Outbox> {
        self.outbox.as_deref()
    }

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

    pub fn new_with_scheduling_mode_and_outbox(
        db: DatastoreHandle,
        supervisor: Arc<TaskSupervisor>,
        side_effects: Arc<SideEffectRegistry>,
        metrics: Arc<SideEffectMetrics>,
        scheduling_mode: PodSchedulingMode,
        outbox: Option<Arc<crate::kubelet::outbox::Outbox>>,
    ) -> Self {
        Self::new_with_network_events(
            db,
            supervisor,
            side_effects,
            metrics,
            crate::networking::global_pod_network_events(),
            scheduling_mode,
            outbox,
        )
    }

    pub fn new_with_scheduling_mode_outbox_and_cluster_api(
        db: DatastoreHandle,
        supervisor: Arc<TaskSupervisor>,
        side_effects: Arc<SideEffectRegistry>,
        metrics: Arc<SideEffectMetrics>,
        scheduling_mode: PodSchedulingMode,
        outbox: Option<Arc<crate::kubelet::outbox::Outbox>>,
        cluster_api: Arc<dyn LeaderApiClient>,
    ) -> Self {
        Self::new_with_network_events_and_cluster_api(PodRepositoryBuildConfig {
            db,
            supervisor,
            side_effects,
            metrics,
            network_events: crate::networking::global_pod_network_events(),
            scheduling_mode,
            outbox,
            cluster_api: Some(cluster_api),
        })
    }

    pub fn new_with_network_events(
        db: DatastoreHandle,
        supervisor: Arc<TaskSupervisor>,
        side_effects: Arc<SideEffectRegistry>,
        metrics: Arc<SideEffectMetrics>,
        network_events: crate::networking::pod_network_events::PodNetworkEvents,
        scheduling_mode: PodSchedulingMode,
        outbox: Option<Arc<crate::kubelet::outbox::Outbox>>,
    ) -> Self {
        Self::new_with_network_events_and_cluster_api(PodRepositoryBuildConfig {
            db,
            supervisor,
            side_effects,
            metrics,
            network_events,
            scheduling_mode,
            outbox,
            cluster_api: None,
        })
    }

    fn new_with_network_events_and_cluster_api(config: PodRepositoryBuildConfig) -> Self {
        let parts = Self::build_parts(config);
        parts.background.start();
        parts.repository
    }

    /// Build `PodRepository` and its deferred-startup services without
    /// calling `workqueue.start()`. The returned `PodRepositoryBackground`
    /// must be started after lifecycle wiring is complete (Task 4.2).
    pub fn build_parts(config: PodRepositoryBuildConfig) -> facade::PodRepositoryParts {
        let PodRepositoryBuildConfig {
            db,
            supervisor,
            side_effects,
            metrics,
            network_events,
            scheduling_mode: _scheduling_mode,
            outbox,
            cluster_api,
        } = config;
        let store = Arc::new(PodStore::new(db.clone()));
        let workqueue = PodWorkqueue::new(
            store.clone(),
            db.clone(),
            supervisor.clone(),
            metrics.clone(),
        );
        let status_only = Arc::new(StatusOnlyWriterService::new(store.clone()));
        let api = Arc::new(PodApiService::new(PodApiServiceDependencies {
            store: store.clone(),
            status_only: status_only.clone(),
            db: db.clone(),
            supervisor: supervisor.clone(),
            workqueue: workqueue.clone(),
            side_effects: side_effects.clone(),
            metrics: metrics.clone(),
            outbox: outbox.clone(),
        }));
        let status = PodStatusService::new(
            store.clone(),
            status_only.clone(),
            side_effects.controller_dispatcher_slot(),
            outbox.clone(),
            cluster_api.clone(),
        );
        let objects = PodObjectService::new(
            store.clone(),
            api.clone(),
            side_effects.controller_dispatcher_slot(),
            outbox.clone(),
            cluster_api.clone(),
        );
        let subresource = PodSubresourceService::new(
            store.clone(),
            status_only,
            side_effects.controller_dispatcher_slot(),
        );
        let network_svc = PodNetworkService::new(db, supervisor.clone(), network_events);
        let watch = PodWatchService::new(store.clone());
        let gc_pod_delete_sink: Arc<dyn crate::controllers::gc::GcPodDeleteSink> = api.clone();

        let deletion_finalizer = Arc::new(
            crate::kubelet::pod_runtime::deletion_finalizer::RealPodDeletionFinalizer::new(
                store.clone(),
                gc_pod_delete_sink,
                cluster_api.clone(),
                outbox.clone(),
                side_effects.clone(),
                metrics.clone(),
                supervisor.clone(),
            ),
        );

        let repository = Self {
            store,
            status,
            objects,
            subresource,
            network_svc,
            _watch: watch,
            api,
            workqueue: workqueue.clone(),
            side_effects,
            metrics,
            supervisor,
            outbox,
            cluster_api,
            deletion_finalizer,
        };
        let background = PodRepositoryBackground::new(workqueue);
        facade::PodRepositoryParts {
            repository,
            background,
        }
    }

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
            PodDeletionFinalizeResult::FinalizersPending => Ok(false),
        }
    }

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
        router: Arc<crate::kubelet::pod_lifecycle_router::PodLifecycleRouter>,
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

    /// Spawn async PDB + namespace-termination reconciliation after a
    /// pod status or metadata write.
    ///
    /// Both operations are derived-state maintenance that must not block
    /// the caller (kubelet status writer, controller pod writer). The
    /// spawned task runs on the TaskSupervisor under `Background` so it
    /// is visible on the admin diagnostics API and participates in
    /// graceful shutdown.
    async fn spawn_post_write_maintenance(&self, namespace: &str) {
        let db = self.store.db().clone();
        let pod_reader: Arc<dyn crate::kubelet::pod_repository::PodReader> = self.store.clone();
        let metrics = self.metrics.clone();
        let ns = namespace.to_string();
        let _ = self
            .supervisor
            .spawn_async(
                crate::task_supervisor::TaskCategory::Background,
                format!("post_write_maintenance/{ns}"),
                async move {
                    crate::controllers::pdb::reconcile_pdbs_for_namespace(
                        db.as_ref(),
                        pod_reader.as_ref(),
                        &ns,
                    )
                    .await;
                    if let Err(err) = crate::api::reconcile_namespace_termination(
                        db.as_ref(),
                        &ns,
                        metrics.as_ref(),
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

    pub async fn schedule_pending_pod(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Resource>> {
        self.api
            .schedule_pending_pod(namespace, name)
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))
    }

    pub async fn schedule_all_unbound_pods(&self) -> Result<()> {
        self.api
            .schedule_all_unbound_pods()
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))
    }

    #[cfg(test)]
    fn set_scheduler_bind_gate_for_test(&self, gate: Arc<api::SchedulerBindGateForTest>) {
        self.api.set_scheduler_bind_gate_for_test(gate);
    }

    /// Enqueue the owning Job for asynchronous reconciliation after a pod
    /// reaches a terminal phase or is marked failed.
    ///
    /// This replaces the old synchronous `reconcile_job_for_pod_owner` path
    /// that called `controllers::job::reconcile_job()` inline, blocking the
    /// pod watcher. The async enqueue gives the Job controller exponential
    /// backoff retry and keeps the watcher responsive.
    ///
    /// No-op when the pod has no Job owner or when the controller dispatcher
    /// is not yet bound.
    pub async fn enqueue_job_reconcile_for_pod(&self, pod: &Value) {
        let Some(owners) = pod
            .pointer("/metadata/ownerReferences")
            .and_then(|arr| arr.as_array())
        else {
            return;
        };

        let Some(owner) = owners
            .iter()
            .find(|r| r.get("kind").and_then(|k| k.as_str()) == Some("Job"))
        else {
            return;
        };

        let Some(job_name) = owner.get("name").and_then(|n| n.as_str()) else {
            return;
        };
        let Some(namespace) = pod.pointer("/metadata/namespace").and_then(|n| n.as_str()) else {
            return;
        };

        let dispatcher = self.side_effects.controller_dispatcher_slot();
        let Some(dispatcher) = dispatcher.get() else {
            return;
        };

        dispatcher
            .enqueue_reconcile_key(crate::controllers::workqueue::ReconcileKey::namespaced(
                "batch/v1", "Job", namespace, job_name,
            ))
            .await;
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

#[async_trait]
impl PodReader for PodRepository {
    async fn get_pod(&self, ns: &str, name: &str) -> Result<Option<Resource>> {
        if let Some(cluster_api) = &self.cluster_api {
            // Kubelet lifecycle and probe decisions need the current single-pod
            // status. A stale worker informer-cache hit can keep probes behind
            // the startup initialDelay gate after the container is already
            // Running, so use the internal fresh read path here, then overlay
            // the node-local checkpoint so the worker reads its own writes.
            let pod = cluster_api
                .get_resource_fresh(pod_resource_key(ns, name))
                .await?;
            return self.overlay_local_status_checkpoint(pod).await;
        }
        self.store.get(ns, name).await
    }

    async fn get_pod_for_uid(&self, ns: &str, name: &str, uid: &str) -> Result<Option<Resource>> {
        if let Some(cluster_api) = &self.cluster_api {
            let pod = cluster_api
                .get_resource_fresh(pod_resource_key(ns, name))
                .await?
                .filter(|pod| pod.uid == uid);
            return self.overlay_local_status_checkpoint(pod).await;
        }
        Ok(self.store.get(ns, name).await?.filter(|pod| pod.uid == uid))
    }

    async fn list_pods(
        &self,
        ns: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        limit: Option<i64>,
        continue_token: Option<&str>,
    ) -> Result<ResourceList> {
        if let Some(cluster_api) = &self.cluster_api {
            return cluster_api
                .list_resources_fresh(ListRequest {
                    api_version: "v1".to_string(),
                    kind: "Pod".to_string(),
                    namespace: ns.map(str::to_string),
                    label_selector: label_selector.map(str::to_string),
                    field_selector: field_selector.map(str::to_string),
                    limit,
                    continue_token: continue_token.map(str::to_string),
                })
                .await;
        }
        self.store
            .list(ns, label_selector, field_selector, limit, continue_token)
            .await
    }
    async fn list_pods_by_owner_uid(&self, ns: &str, owner_uid: &str) -> Result<Vec<Resource>> {
        if self.cluster_api.is_some() {
            let pods = self.list_pods(Some(ns), None, None, None, None).await?;
            return Ok(pods
                .items
                .into_iter()
                .filter(|pod| pod_has_owner_uid(&pod.data, owner_uid))
                .collect());
        }
        self.store.list_by_owner(ns, owner_uid).await
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

pub fn current_epoch_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

#[async_trait]
impl PodStatusWriter for PodRepository {
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
        if result.changed {
            self.spawn_post_write_maintenance(ns).await;
        }
        Ok(result.resource)
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
        if result.changed {
            self.spawn_post_write_maintenance(ns).await;
        }
        Ok(result.resource)
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
        if result.changed {
            self.spawn_post_write_maintenance(ns).await;
        }
        Ok(result.resource)
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
        if result.changed {
            self.spawn_post_write_maintenance(ns).await;
        }
        Ok(result.resource)
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
        if result.changed {
            self.spawn_post_write_maintenance(ns).await;
        }
        Ok(result.resource)
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
        if result.changed {
            self.spawn_post_write_maintenance(ns).await;
        }
        Ok(result.resource)
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
        if result.changed {
            self.spawn_post_write_maintenance(ns).await;
        }
        Ok(result.resource)
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
        if result.changed {
            self.spawn_post_write_maintenance(ns).await;
        }
        Ok(result.resource)
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
        if result.changed {
            self.spawn_post_write_maintenance(ns).await;
        }
        Ok(result.resource)
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
        if result.changed {
            self.spawn_post_write_maintenance(ns).await;
        }
        Ok(result.resource)
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
        if result.changed {
            self.spawn_post_write_maintenance(ns).await;
        }
        Ok(result.resource)
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

#[async_trait]
impl PodMetadataWriter for PodRepository {
    async fn record_sandbox_id(&self, ns: &str, name: &str, sandbox_id: &str) -> Result<Resource> {
        let updated = self.objects.record_sandbox_id(ns, name, sandbox_id).await?;
        self.spawn_post_write_maintenance(ns).await;
        Ok(updated)
    }

    async fn record_sandbox_id_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        sandbox_id: &str,
    ) -> Result<Resource> {
        let updated = self
            .objects
            .record_sandbox_id_for_uid(ns, name, pod_uid, sandbox_id)
            .await?;
        self.spawn_post_write_maintenance(ns).await;
        Ok(updated)
    }
}

#[async_trait]
impl PodObjectWriter for PodRepository {
    async fn create_controller_pod(
        &self,
        ns: &str,
        name: &str,
        node_name: &str,
        pod: Value,
    ) -> Result<Resource> {
        let created = self
            .objects
            .create_controller_pod(ns, name, node_name, pod)
            .await?;
        self.spawn_post_write_maintenance(ns).await;
        Ok(created)
    }
    async fn delete_pod(&self, ns: &str, name: &str) -> Result<()> {
        let outcome = self
            .api
            .api_delete_pod(ns, name, DeleteOptions::default(), false)
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        if let PodApiDeleteOutcome::GracefulSet(resource) = outcome {
            crate::side_effects::run_hooks_logged(
                &self.side_effects,
                &resource.data,
                self.store.db().as_ref(),
                &self.metrics,
                "pod_object_mark_terminating",
            )
            .await;
        }

        Ok(())
    }
    async fn update_pod_owner_references(
        &self,
        ns: &str,
        name: &str,
        owner_refs: Vec<Value>,
    ) -> Result<Resource> {
        let updated = self
            .objects
            .update_pod_owner_references(ns, name, owner_refs)
            .await?;
        self.spawn_post_write_maintenance(ns).await;
        Ok(updated)
    }

    async fn update_pod_owner_references_for_uid(
        &self,
        ns: &str,
        name: &str,
        expected_uid: &str,
        owner_refs: Vec<Value>,
    ) -> Result<Resource> {
        let updated = self
            .objects
            .update_pod_owner_references_for_uid(ns, name, expected_uid, owner_refs)
            .await?;
        self.spawn_post_write_maintenance(ns).await;
        Ok(updated)
    }

    async fn merge_pod_labels(
        &self,
        ns: &str,
        name: &str,
        labels: Vec<(String, String)>,
    ) -> Result<Resource> {
        let updated = self.objects.merge_pod_labels(ns, name, labels).await?;
        self.spawn_post_write_maintenance(ns).await;
        Ok(updated)
    }

    async fn merge_pod_labels_for_uid(
        &self,
        ns: &str,
        name: &str,
        expected_uid: &str,
        labels: Vec<(String, String)>,
    ) -> Result<Resource> {
        let updated = self
            .objects
            .merge_pod_labels_for_uid(ns, name, expected_uid, labels)
            .await?;
        self.spawn_post_write_maintenance(ns).await;
        Ok(updated)
    }
}

#[async_trait]
impl PodSubresourceWriter for PodRepository {
    async fn replace_status_from_api(
        &self,
        ns: &str,
        name: &str,
        status: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        let updated = self
            .subresource
            .replace_status_from_api(ns, name, status, expected_rv)
            .await?;
        crate::side_effects::run_hooks_logged(
            &self.side_effects,
            &updated.data,
            self.store.db().as_ref(),
            &self.metrics,
            "pod_status_subresource_replace",
        )
        .await;
        Ok(updated)
    }
    async fn replace_status_from_api_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        status: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        let updated = self
            .subresource
            .replace_status_from_api_for_uid(ns, name, pod_uid, status, expected_rv)
            .await?;
        crate::side_effects::run_hooks_logged(
            &self.side_effects,
            &updated.data,
            self.store.db().as_ref(),
            &self.metrics,
            "pod_status_subresource_replace",
        )
        .await;
        Ok(updated)
    }
    async fn patch_status_from_api(
        &self,
        ns: &str,
        name: &str,
        patch: Value,
        patch_type: PodStatusPatchType,
        expected_rv: i64,
    ) -> Result<Resource> {
        let updated = self
            .subresource
            .patch_status_from_api(ns, name, patch, patch_type, expected_rv)
            .await?;
        crate::side_effects::run_hooks_logged(
            &self.side_effects,
            &updated.data,
            self.store.db().as_ref(),
            &self.metrics,
            "pod_status_subresource_patch",
        )
        .await;
        Ok(updated)
    }
    async fn update_ephemeral_containers(
        &self,
        ns: &str,
        name: &str,
        containers: Vec<Value>,
        expected_rv: i64,
    ) -> Result<Resource> {
        self.subresource
            .update_ephemeral_containers(ns, name, containers, expected_rv)
            .await
    }
}

#[async_trait]
impl PodNetworkReader for PodRepository {
    async fn read_pod_network_assignment(
        &self,
        sandbox_id: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        host_network: bool,
    ) -> Result<PodNetworkAssignment> {
        self.network_svc
            .read_pod_network_assignment(sandbox_id, namespace, pod_name, pod_uid, host_network)
            .await
    }
}

#[cfg(test)]
impl PodWatchSource for PodRepository {
    fn subscribe_pod_watch(&self) -> broadcast::Receiver<WatchEvent> {
        self.store.subscribe_watch()
    }
}

#[async_trait]
#[allow(clippy::todo)]
impl PodApiWriter for PodRepository {
    async fn api_create_pod(
        &self,
        request: PodApiCreateRequest,
    ) -> std::result::Result<PodApiCreateResult, AppError> {
        self.api.api_create_pod(request).await
    }
    async fn api_update_pod(
        &self,
        ns: &str,
        name: &str,
        body: Value,
        current: Resource,
        dry_run: bool,
    ) -> std::result::Result<PodApiUpdateOutcome, AppError> {
        self.api
            .api_update_pod(ns, name, body, current, dry_run)
            .await
    }
    async fn api_patch_pod(
        &self,
        ns: &str,
        name: &str,
        patch: Value,
        patch_type: PodStatusPatchType,
        dry_run: bool,
    ) -> std::result::Result<PodApiUpdateOutcome, AppError> {
        self.api
            .api_patch_pod(ns, name, patch, patch_type, dry_run)
            .await
    }
    async fn api_delete_pod(
        &self,
        ns: &str,
        name: &str,
        options: DeleteOptions,
        dry_run: bool,
    ) -> std::result::Result<PodApiDeleteOutcome, AppError> {
        self.api.api_delete_pod(ns, name, options, dry_run).await
    }
    async fn api_delete_collection_pods(
        &self,
        ns: &str,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        dry_run: bool,
    ) -> std::result::Result<(), AppError> {
        self.api
            .api_delete_collection_pods(ns, label_selector, field_selector, dry_run)
            .await
    }
}

#[async_trait]
impl GcPodDeleteSink for PodRepository {
    async fn request_gc_pod_delete(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<()> {
        let options = DeleteOptions::with_uid_precondition(uid);
        match self
            .api
            .api_delete_pod_for_gc(namespace, name, options, false)
            .await
        {
            Ok(_outcome) => Ok(()),
            Err(e) => Err(anyhow::anyhow!("{e:?}")),
        }
    }
}
