//! Controller dispatcher for unified reconciliation
//!
//! This module provides a centralized dispatcher that routes resource reconciliation
//! to the appropriate controller based on the resource's apiVersion and kind.
//!
//! Mutation handlers should call [`ControllerDispatcher::enqueue`] — never the
//! synchronous [`ControllerDispatcher::reconcile`] — so the HTTP response does
//! not block on reconcile latency and so failures get exponential-backoff
//! retried via the workqueue (P0-LEAK-02). The synchronous form is kept
//! public only for tests that exercise individual controllers.
//!
//! The dispatcher exchanges [`DatastoreHandle`] (`Arc<dyn DatastoreBackend>`)
//! across its public surface — never a concrete `Datastore` — so the
//! datastore backend stays pluggable behind the trait boundary.

use crate::controller::{Context, Controller};
use crate::controllers::{
    daemonset_controller::DaemonSetController,
    deployment_controller::DeploymentController,
    job_controller::JobController,
    pdb_controller::PDBController,
    pvc_controller::PVCController,
    replicaset_controller::ReplicaSetController,
    replication_controller_runner::ReplicationControllerController,
    service::{NodePortAllocator, ServiceIpam},
    service_controller::ServiceController,
    statefulset_controller::StatefulSetController,
    workqueue::{
        Key, MAX_RETRY_ATTEMPTS, ReconcileKey, WorkQueue, backoff_for, controller_kind_static,
    },
};
use crate::datastore::DatastoreHandle;
use crate::kubelet::pod_repository::PodRepository;
use anyhow::{Context as _, Result};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};

/// Controller dispatcher that routes resources to the appropriate controller.
///
/// Holds a [`WorkQueue`] for asynchronous reconciliation: producers (mutation
/// handlers, watch listeners) call [`enqueue`](Self::enqueue); the worker spawned
/// by [`run_worker`](Self::run_worker) drains the queue and dispatches each key
/// to the appropriate controller, fetching the freshest resource state from the
/// datastore at dispatch time. Failures are re-enqueued with exponential backoff.
///
/// `worker_running` flips to true when [`run_worker`] starts. Until then,
/// [`enqueue`] falls through to a synchronous [`reconcile`] using the
/// [`DatastoreHandle`] supplied via [`set_sync_context`](Self::set_sync_context)
/// — preserves the synchronous post-create assertions in unit tests that
/// drive HTTP handlers without a running worker.
pub struct ControllerDispatcher {
    controllers: HashMap<(&'static str, &'static str), Arc<dyn Controller>>,
    queue: WorkQueue,
    retry_count: Arc<Mutex<HashMap<Key, u32>>>,
    worker_running: Arc<std::sync::atomic::AtomicBool>,
    /// Datastore handle + node_name captured by the running worker, used by
    /// [`enqueue`] when no worker is registered (test mode) so the call still
    /// has somewhere to dispatch synchronously.
    sync_ctx: Arc<Mutex<Option<(DatastoreHandle, String)>>>,
    /// Service router shared across all dispatched controllers via
    /// [`Context::services`]. Set by bootstrap before the worker starts;
    /// `None` in tests that exercise non-Service controllers without a
    /// live router.
    services: Arc<Mutex<Option<Arc<dyn crate::networking::ServiceRouter>>>>,
    /// Pod repository shared across all dispatched controllers via
    /// [`Context::pod_repository`]. Set by bootstrap before the worker
    /// starts; required by the Deployment and ReplicaSet controllers
    /// (they fail-fast at reconcile time if it is missing).
    pod_repository: Arc<Mutex<Option<Arc<PodRepository>>>>,
    active_reconciles: Arc<Mutex<ActiveReconciles>>,
    active_reconciles_changed: Arc<Notify>,
}

#[derive(Default)]
struct ActiveReconciles {
    in_flight: HashSet<Key>,
    pending_followup: HashSet<Key>,
}

impl ControllerDispatcher {
    /// Create a new controller dispatcher with all available controllers
    #[cfg(test)]
    pub fn new(service_ipam: Arc<ServiceIpam>) -> Self {
        Self::with_task_supervisor(
            service_ipam,
            Arc::new(crate::task_supervisor::TaskSupervisor::new(
                crate::task_supervisor::TaskCategoryConfig::default(),
            )),
        )
    }

    pub fn with_task_supervisor(
        service_ipam: Arc<ServiceIpam>,
        task_supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
    ) -> Self {
        Self::new_with_nodeport(
            service_ipam,
            Arc::new(NodePortAllocator::new()),
            task_supervisor,
            None,
        )
    }

    pub fn new_with_nodeport(
        service_ipam: Arc<ServiceIpam>,
        nodeport_alloc: Arc<NodePortAllocator>,
        task_supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
        csr_signer: Option<Arc<dyn crate::auth::csr_signer::CsrSigner>>,
    ) -> Self {
        let mut controllers: HashMap<(&'static str, &'static str), Arc<dyn Controller>> =
            HashMap::new();

        // Register all controllers by (apiVersion, kind)
        controllers.insert(
            ("apps/v1", "Deployment"),
            Arc::new(DeploymentController) as Arc<dyn Controller>,
        );
        controllers.insert(
            ("apps/v1", "ReplicaSet"),
            Arc::new(ReplicaSetController) as Arc<dyn Controller>,
        );
        controllers.insert(
            ("apps/v1", "StatefulSet"),
            Arc::new(StatefulSetController) as Arc<dyn Controller>,
        );
        controllers.insert(
            ("apps/v1", "DaemonSet"),
            Arc::new(DaemonSetController) as Arc<dyn Controller>,
        );
        controllers.insert(
            ("batch/v1", "Job"),
            Arc::new(JobController) as Arc<dyn Controller>,
        );
        controllers.insert(
            ("v1", "Service"),
            Arc::new(ServiceController {
                service_ipam: service_ipam.clone(),
                nodeport_alloc: nodeport_alloc.clone(),
            }) as Arc<dyn Controller>,
        );
        controllers.insert(
            ("v1", "PersistentVolumeClaim"),
            Arc::new(PVCController) as Arc<dyn Controller>,
        );
        controllers.insert(
            ("v1", "ReplicationController"),
            Arc::new(ReplicationControllerController) as Arc<dyn Controller>,
        );
        controllers.insert(
            ("policy/v1", "PodDisruptionBudget"),
            Arc::new(PDBController) as Arc<dyn Controller>,
        );

        // CSR signer controller — only registered when a signer is available
        if let Some(signer) = csr_signer {
            controllers.insert(
                ("certificates.k8s.io/v1", "CertificateSigningRequest"),
                Arc::new(crate::controllers::csr_signer::CsrSignerController::new(
                    signer,
                )) as Arc<dyn Controller>,
            );
        }

        Self {
            controllers,
            queue: WorkQueue::with_task_supervisor(task_supervisor),
            retry_count: Arc::new(Mutex::new(HashMap::new())),
            worker_running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            sync_ctx: Arc::new(Mutex::new(None)),
            services: Arc::new(Mutex::new(None)),
            pod_repository: Arc::new(Mutex::new(None)),
            active_reconciles: Arc::new(Mutex::new(ActiveReconciles::default())),
            active_reconciles_changed: Arc::new(Notify::new()),
        }
    }

    /// Attach a live ServiceRouter so controllers (notably ServiceController)
    /// can request immediate nft sync via `Context::services`. Must be
    /// called by bootstrap before [`run_worker`] starts.
    pub async fn set_services(&self, services: Arc<dyn crate::networking::ServiceRouter>) {
        *self.services.lock().await = Some(services);
    }

    async fn current_services(&self) -> Option<Arc<dyn crate::networking::ServiceRouter>> {
        self.services.lock().await.clone()
    }

    /// Attach the process-wide `PodRepository` so workload controllers
    /// (Deployment, ReplicaSet) can read/write pod objects through the
    /// repository. Bootstrap must call this before [`run_worker`] starts;
    /// test fixtures that drive these controllers must call it before
    /// invoking [`reconcile`].
    pub async fn set_pod_repository(&self, pod_repository: Arc<PodRepository>) {
        *self.pod_repository.lock().await = Some(pod_repository);
    }

    pub async fn current_pod_repository(&self) -> Option<Arc<PodRepository>> {
        self.pod_repository.lock().await.clone()
    }

    /// Extract the workqueue key from a resource and enqueue it. Producers
    /// (mutation handlers, watch listeners) call this instead of `reconcile`
    /// so the HTTP path returns immediately and reconcile failures get
    /// exponential-backoff retried by the worker.
    ///
    /// When no worker is running (typical in unit tests), this falls through
    /// to a synchronous `reconcile` so test fixtures that don't spawn the
    /// worker still observe the post-mutation reconcile side effects.
    pub async fn enqueue(&self, resource: &Value) {
        if !self
            .worker_running
            .load(std::sync::atomic::Ordering::Acquire)
        {
            // No worker — dispatch synchronously so callers (tests that don't
            // start a worker) still see the reconcile happen.
            let ctx = self.sync_ctx.lock().await;
            if let Some((ref db_handle, ref node_name)) = *ctx
                && let Err(e) = self.reconcile(resource, db_handle, node_name).await
            {
                tracing::warn!("Synchronous fallback reconcile failed: {}", e);
            }
            return;
        }
        if let Some(key) = key_for_value(resource) {
            self.queue.add(key).await;
        }
    }

    /// Enqueue a specific controller reconcile key without the synchronous
    /// fallback used by HTTP handler tests. Side effects use this path so they
    /// never run controller reconciliation inline with the mutating request.
    pub async fn enqueue_reconcile_key(&self, key: ReconcileKey) {
        self.queue.add(key.into()).await;
    }

    pub async fn enqueue_controller_owner_for_pod(&self, pod: &Value) {
        let Some(owner_refs) = pod
            .pointer("/metadata/ownerReferences")
            .and_then(|value| value.as_array())
        else {
            return;
        };
        let namespace = pod
            .pointer("/metadata/namespace")
            .and_then(|value| value.as_str())
            .unwrap_or("default");

        for owner_ref in owner_refs {
            if owner_ref
                .get("controller")
                .and_then(|value| value.as_bool())
                != Some(true)
            {
                continue;
            }
            let api_version = owner_ref
                .get("apiVersion")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            let kind = owner_ref
                .get("kind")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            let owner_name = owner_ref
                .get("name")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            let Some((static_api, static_kind)) = controller_kind_static(api_version, kind) else {
                continue;
            };
            self.enqueue_reconcile_key(ReconcileKey::namespaced(
                static_api,
                static_kind,
                namespace,
                owner_name,
            ))
            .await;
        }
    }

    pub async fn pending_reconcile_keys(&self) -> Vec<ReconcileKey> {
        self.queue.ready_keys_snapshot().await
    }

    #[cfg(test)]
    pub async fn queued_reconcile_keys_for_test(&self) -> Vec<ReconcileKey> {
        self.queue.ready_keys().await
    }

    #[cfg(test)]
    pub async fn take_reconcile_key_for_test(&self) -> ReconcileKey {
        self.queue.take().await.into()
    }

    /// Configure the synchronous-fallback context for [`enqueue`] when no
    /// worker is running. Tests that exercise HTTP handlers should call this
    /// so the post-mutation reconcile lands.
    #[cfg(test)]
    pub async fn set_sync_context(&self, db_handle: DatastoreHandle, node_name: String) {
        *self.sync_ctx.lock().await = Some((db_handle, node_name));
    }

    #[cfg(test)]
    fn replace_controller_for_test(
        &mut self,
        api_version: &'static str,
        kind: &'static str,
        controller: Arc<dyn Controller>,
    ) {
        self.controllers.insert((api_version, kind), controller);
    }

    /// Run the worker loop until `cancel` fires. Drains the workqueue: for
    /// each key, fetches the latest resource from the datastore (the
    /// resource may have changed since the producer enqueued it) and
    /// dispatches to the matching controller. Reconcile failures are
    /// re-enqueued with exponential backoff up to [`MAX_RETRY_ATTEMPTS`];
    /// persistent failures surface via `tracing::error!` and the key is
    /// dropped (the next mutation/watch event will re-enqueue it).
    pub async fn run_worker(
        self: Arc<Self>,
        db_handle: DatastoreHandle,
        node_name: String,
        cancel: tokio_util::sync::CancellationToken,
    ) {
        self.run_worker_pool(1, db_handle, node_name, cancel).await;
    }

    pub async fn run_worker_pool(
        self: Arc<Self>,
        worker_count: usize,
        db_handle: DatastoreHandle,
        node_name: String,
        cancel: tokio_util::sync::CancellationToken,
    ) {
        // Mark the worker as running so subsequent `enqueue` calls go through
        // the queue instead of falling back to synchronous reconcile.
        self.worker_running
            .store(true, std::sync::atomic::Ordering::Release);
        let worker_count = worker_count.max(1);
        tracing::info!(
            workers = worker_count,
            "Controller workqueue worker pool started"
        );
        let workers = (0..worker_count).map(|worker_id| {
            let dispatcher = self.clone();
            let db_handle = db_handle.clone();
            let node_name = node_name.clone();
            let cancel = cancel.clone();
            async move {
                dispatcher
                    .run_worker_loop(worker_id, db_handle, node_name, cancel)
                    .await;
            }
        });
        futures::future::join_all(workers).await;
        self.worker_running
            .store(false, std::sync::atomic::Ordering::Release);
    }

    async fn run_worker_loop(
        self: Arc<Self>,
        worker_id: usize,
        db_handle: DatastoreHandle,
        node_name: String,
        cancel: tokio_util::sync::CancellationToken,
    ) {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::info!(worker_id, "Controller workqueue worker shutting down");
                    return;
                }
                key = self.queue.take() => {
                    if !self.begin_key_dispatch(&key).await {
                        continue;
                    }
                    self.dispatch_key(&key, &db_handle, &node_name).await;
                    self.finish_key_dispatch(key).await;
                }
            }
        }
    }

    async fn begin_key_dispatch(&self, key: &Key) -> bool {
        let mut active = self.active_reconciles.lock().await;
        if active.in_flight.contains(key) {
            active.pending_followup.insert(key.clone());
            return false;
        }
        active.in_flight.insert(key.clone());
        true
    }

    async fn wait_for_key_dispatch_slot(&self, key: &Key) {
        loop {
            let notified = self.active_reconciles_changed.notified();
            {
                let mut active = self.active_reconciles.lock().await;
                if !active.in_flight.contains(key) {
                    active.in_flight.insert(key.clone());
                    return;
                }
            }
            notified.await;
        }
    }

    async fn finish_key_dispatch(&self, key: Key) {
        let should_requeue = {
            let mut active = self.active_reconciles.lock().await;
            active.in_flight.remove(&key);
            active.pending_followup.remove(&key)
        };
        self.active_reconciles_changed.notify_waiters();
        if should_requeue {
            self.queue.add(key).await;
        }
    }

    async fn dispatch_key(&self, key: &Key, db_handle: &DatastoreHandle, node_name: &str) {
        // Fetch the freshest version of the resource. If it's gone (deleted
        // between enqueue and dispatch), there is nothing to reconcile and we
        // also clear any retry counter for the key.
        let namespace = key.namespace.clone();
        let resource = match db_handle
            .get_resource(key.api_version, key.kind, namespace.as_deref(), &key.name)
            .await
        {
            Ok(Some(r)) => r,
            Ok(None) => {
                self.retry_count.lock().await.remove(key);
                return;
            }
            Err(e) => {
                tracing::warn!(
                    workqueue_key = %key,
                    error = %e,
                    "workqueue: failed to fetch resource; will retry"
                );
                self.requeue_with_backoff(key.clone()).await;
                return;
            }
        };

        let value = crate::api::inject_resource_version(resource.data, resource.resource_version);
        match self.reconcile_unlocked(&value, db_handle, node_name).await {
            Ok(()) => {
                self.retry_count.lock().await.remove(key);
                if let Err(e) = self
                    .schedule_finished_job_ttl_requeue_if_needed(key, db_handle)
                    .await
                {
                    tracing::warn!(
                        workqueue_key = %key,
                        error = %e,
                        "workqueue: failed to schedule Job ttlSecondsAfterFinished cleanup"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    workqueue_key = %key,
                    error = %e,
                    "workqueue: reconcile failed; will retry with backoff"
                );
                self.requeue_with_backoff(key.clone()).await;
            }
        }
    }

    async fn schedule_finished_job_ttl_requeue_if_needed(
        &self,
        key: &Key,
        db_handle: &DatastoreHandle,
    ) -> Result<()> {
        if key.api_version != "batch/v1" || key.kind != "Job" {
            return Ok(());
        }

        let Some(resource) = db_handle
            .get_resource(
                key.api_version,
                key.kind,
                key.namespace.as_deref(),
                &key.name,
            )
            .await?
        else {
            return Ok(());
        };

        if resource
            .data
            .pointer("/metadata/deletionTimestamp")
            .is_some()
        {
            return Ok(());
        }

        let Some(delay) = crate::controllers::job::job_ttl_cleanup_delay(&resource.data)? else {
            return Ok(());
        };

        if delay.is_zero() {
            self.queue.add(key.clone()).await;
        } else {
            self.queue.add_after(key.clone(), delay).await;
        }
        Ok(())
    }

    async fn requeue_with_backoff(&self, key: Key) {
        let mut counts = self.retry_count.lock().await;
        let attempt = counts.entry(key.clone()).or_insert(0);
        if *attempt >= MAX_RETRY_ATTEMPTS {
            tracing::error!(
                workqueue_key = %key,
                attempts = *attempt,
                "workqueue: dropping key after MAX_RETRY_ATTEMPTS — \
                 will only retry on next mutation or watch event"
            );
            counts.remove(&key);
            return;
        }
        let backoff = backoff_for(*attempt);
        *attempt += 1;
        drop(counts);
        self.queue.add_after(key, backoff).await;
    }

    /// Reconcile a resource by dispatching to the appropriate controller.
    ///
    /// # Arguments
    ///
    /// * `resource` - The resource to reconcile (must have apiVersion and kind)
    /// * `db_handle` - Datastore handle (`Arc<dyn DatastoreBackend>`)
    /// * `node_name` - Node name for pod scheduling
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if reconciliation succeeded or no controller is registered
    /// for this resource type. Returns `Err` if reconciliation failed.
    pub async fn reconcile(
        &self,
        resource: &Value,
        db_handle: &DatastoreHandle,
        node_name: &str,
    ) -> Result<()> {
        let Some(key) = key_for_value(resource) else {
            return self
                .reconcile_unlocked(resource, db_handle, node_name)
                .await;
        };
        self.wait_for_key_dispatch_slot(&key).await;
        let result = self
            .reconcile_unlocked(resource, db_handle, node_name)
            .await;
        self.finish_key_dispatch(key).await;
        result
    }

    async fn reconcile_unlocked(
        &self,
        resource: &Value,
        db_handle: &DatastoreHandle,
        node_name: &str,
    ) -> Result<()> {
        let api_version = resource
            .get("apiVersion")
            .and_then(|v| v.as_str())
            .context("Missing apiVersion in resource")?;
        let kind = resource
            .get("kind")
            .and_then(|v| v.as_str())
            .context("Missing kind in resource")?;

        if let Some(namespace) = resource
            .pointer("/metadata/namespace")
            .and_then(|v| v.as_str())
            .filter(|namespace| !namespace.is_empty())
            && namespace_is_terminating(db_handle, namespace).await?
        {
            tracing::debug!(
                api_version,
                kind,
                namespace,
                name = resource
                    .pointer("/metadata/name")
                    .and_then(|v| v.as_str())
                    .unwrap_or(""),
                "controller reconcile skipped for terminating namespace"
            );
            return Ok(());
        }

        // Look up controller for this resource type
        if let Some(controller) = self.controllers.get(&(api_version, kind)) {
            let ctx = match self.current_services().await {
                Some(services) => {
                    Context::with_services(db_handle.clone(), node_name.to_string(), services)
                }
                None => Context::new(db_handle.clone(), node_name.to_string()),
            };
            let ctx = match self.current_pod_repository().await {
                Some(pod_repository) => ctx.with_pod_repository(pod_repository),
                None => ctx,
            };
            controller.reconcile(resource.clone(), ctx).await?;
        }
        // If no controller is registered, that's fine - not all resources need reconciliation

        Ok(())
    }
}

async fn namespace_is_terminating(db_handle: &DatastoreHandle, namespace: &str) -> Result<bool> {
    let Some(ns) = db_handle.get_namespace(namespace).await? else {
        return Ok(false);
    };
    Ok(ns
        .data
        .pointer("/metadata/deletionTimestamp")
        .and_then(|v| v.as_str())
        .is_some())
}

/// Build a workqueue [`Key`] from a resource JSON Value. Returns `None` when
/// the resource is missing the fields needed to identify it (apiVersion, kind,
/// name) — such resources are not enqueueable.
fn key_for_value(resource: &Value) -> Option<Key> {
    let api_version = resource.get("apiVersion").and_then(|v| v.as_str())?;
    let kind = resource.get("kind").and_then(|v| v.as_str())?;
    let (api_version, kind) = controller_kind_static(api_version, kind)?;
    let name = resource
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())?;
    let namespace = resource
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str());
    Some(match namespace {
        Some(namespace) if !namespace.is_empty() => {
            ReconcileKey::namespaced(api_version, kind, namespace, name).into()
        }
        _ => ReconcileKey::cluster(api_version, kind, name).into(),
    })
}

#[cfg(test)]
impl Default for ControllerDispatcher {
    fn default() -> Self {
        // Default ServiceIpam with standard service CIDR
        let service_ipam = Arc::new(ServiceIpam::new("10.43.128.0/17"));
        Self::with_task_supervisor(
            service_ipam,
            Arc::new(crate::task_supervisor::TaskSupervisor::new(
                crate::task_supervisor::TaskCategoryConfig::default(),
            )),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datastore::sqlite::Datastore;
    use async_trait::async_trait;
    use serde_json::json;
    use tokio::sync::Notify;

    fn handle_for(db: Datastore) -> DatastoreHandle {
        Arc::new(db)
    }

    struct BlockingController {
        started: Arc<Notify>,
        release: Arc<Notify>,
    }

    #[async_trait]
    impl Controller for BlockingController {
        fn name(&self) -> &'static str {
            "blocking"
        }

        async fn reconcile(&self, _resource: Value, _ctx: Context) -> Result<()> {
            self.started.notify_waiters();
            self.release.notified().await;
            Ok(())
        }
    }

    struct SignalController {
        called: Arc<Notify>,
    }

    #[async_trait]
    impl Controller for SignalController {
        fn name(&self) -> &'static str {
            "signal"
        }

        async fn reconcile(&self, _resource: Value, _ctx: Context) -> Result<()> {
            self.called.notify_waiters();
            Ok(())
        }
    }

    struct SerialProbeController {
        calls: Arc<std::sync::atomic::AtomicUsize>,
        active: Arc<std::sync::atomic::AtomicUsize>,
        overlapped: Arc<std::sync::atomic::AtomicBool>,
        first_started: Arc<Notify>,
        second_started: Arc<Notify>,
        release_first: Arc<Notify>,
    }

    #[async_trait]
    impl Controller for SerialProbeController {
        fn name(&self) -> &'static str {
            "serial-probe"
        }

        async fn reconcile(&self, _resource: Value, _ctx: Context) -> Result<()> {
            if self
                .active
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                != 0
            {
                self.overlapped
                    .store(true, std::sync::atomic::Ordering::SeqCst);
            }
            let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            if call == 1 {
                self.first_started.notify_waiters();
                self.release_first.notified().await;
            } else {
                self.second_started.notify_waiters();
            }
            self.active
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn direct_reconcile_is_serialized_with_worker_for_same_key() {
        let db = crate::datastore::test_support::in_memory().await;
        let service = db
            .create_resource(
                "v1",
                "Service",
                Some("default"),
                "same",
                json!({
                    "apiVersion": "v1",
                    "kind": "Service",
                    "metadata": {"namespace": "default", "name": "same", "uid": "svc-uid"},
                    "spec": {"ports": [{"port": 80}]}
                }),
            )
            .await
            .expect("create service");
        let resource = crate::api::inject_resource_version(service.data, service.resource_version);

        let service_ipam = Arc::new(ServiceIpam::new("10.43.128.0/17"));
        let mut dispatcher = ControllerDispatcher::new(service_ipam);
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let overlapped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let first_started = Arc::new(Notify::new());
        let second_started = Arc::new(Notify::new());
        let release_first = Arc::new(Notify::new());
        dispatcher.replace_controller_for_test(
            "v1",
            "Service",
            Arc::new(SerialProbeController {
                calls: calls.clone(),
                active: active.clone(),
                overlapped: overlapped.clone(),
                first_started: first_started.clone(),
                second_started: second_started.clone(),
                release_first: release_first.clone(),
            }),
        );
        let dispatcher = Arc::new(dispatcher);
        let db_handle = handle_for(db);
        let cancel = tokio_util::sync::CancellationToken::new();
        let worker = tokio::spawn({
            let dispatcher = dispatcher.clone();
            let db_handle = db_handle.clone();
            let cancel = cancel.clone();
            async move {
                dispatcher
                    .run_worker_pool(2, db_handle, "test-node".to_string(), cancel)
                    .await;
            }
        });

        dispatcher
            .enqueue_reconcile_key(ReconcileKey::namespaced("v1", "Service", "default", "same"))
            .await;
        tokio::time::timeout(std::time::Duration::from_secs(1), first_started.notified())
            .await
            .expect("worker reconcile should start");

        let direct = tokio::spawn({
            let dispatcher = dispatcher.clone();
            let db_handle = db_handle.clone();
            let resource = resource.clone();
            async move {
                dispatcher
                    .reconcile(&resource, &db_handle, "test-node")
                    .await
                    .expect("direct reconcile should complete")
            }
        });
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(100),
                second_started.notified()
            )
            .await
            .is_err(),
            "direct reconcile must not enter the same Service key while worker reconcile is active"
        );

        release_first.notify_waiters();
        direct.await.expect("direct reconcile task should join");
        assert!(
            !overlapped.load(std::sync::atomic::Ordering::SeqCst),
            "same-key reconciles must not overlap"
        );
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "worker reconcile and direct reconcile should both run, but serially"
        );

        cancel.cancel();
        worker.await.expect("worker pool task should join");
    }

    #[tokio::test]
    async fn worker_pool_dispatches_service_while_other_controller_is_blocked() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "slow",
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"namespace": "default", "name": "slow", "uid": "slow-uid"},
                "spec": {}
            }),
        )
        .await
        .expect("create slow deployment");
        db.create_resource(
            "v1",
            "Service",
            Some("default"),
            "fast",
            json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"namespace": "default", "name": "fast", "uid": "fast-uid"},
                "spec": {"ports": [{"port": 80}]}
            }),
        )
        .await
        .expect("create fast service");

        let service_ipam = Arc::new(ServiceIpam::new("10.43.128.0/17"));
        let mut dispatcher = ControllerDispatcher::new(service_ipam);
        let slow_started = Arc::new(Notify::new());
        let slow_release = Arc::new(Notify::new());
        let fast_called = Arc::new(Notify::new());
        dispatcher.replace_controller_for_test(
            "apps/v1",
            "Deployment",
            Arc::new(BlockingController {
                started: slow_started.clone(),
                release: slow_release.clone(),
            }),
        );
        dispatcher.replace_controller_for_test(
            "v1",
            "Service",
            Arc::new(SignalController {
                called: fast_called.clone(),
            }),
        );
        let dispatcher = Arc::new(dispatcher);
        let cancel = tokio_util::sync::CancellationToken::new();
        let worker = tokio::spawn({
            let dispatcher = dispatcher.clone();
            let db_handle = handle_for(db);
            let cancel = cancel.clone();
            async move {
                dispatcher
                    .run_worker_pool(2, db_handle, "test-node".to_string(), cancel)
                    .await;
            }
        });

        dispatcher
            .enqueue_reconcile_key(ReconcileKey::namespaced(
                "apps/v1",
                "Deployment",
                "default",
                "slow",
            ))
            .await;
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            slow_started.notified(),
        )
        .await
        .expect("slow reconcile should start");

        dispatcher
            .enqueue_reconcile_key(ReconcileKey::namespaced("v1", "Service", "default", "fast"))
            .await;
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            fast_called.notified(),
        )
        .await
        .expect("Service reconcile must dispatch while another worker is blocked");

        cancel.cancel();
        slow_release.notify_waiters();
        worker.await.expect("worker pool task should join");
    }

    #[tokio::test]
    async fn test_dispatcher_routes_deployment_to_deployment_controller() {
        let db = crate::datastore::test_support::in_memory().await;
        let service_ipam = Arc::new(ServiceIpam::new("10.43.128.0/17"));
        let dispatcher = ControllerDispatcher::new(service_ipam);
        dispatcher
            .set_pod_repository(crate::controllers::test_utils::pod_repository_for_test(&db))
            .await;

        let deployment = db
            .create_resource(
                "apps/v1",
                "Deployment",
                Some("default"),
                "nginx",
                json!({
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "metadata": {
                        "name": "nginx",
                        "namespace": "default",
                        "uid": "deploy-123"
                    },
                    "spec": {
                        "replicas": 1,
                        "selector": {"matchLabels": {"app": "nginx"}},
                        "template": {
                            "metadata": {"labels": {"app": "nginx"}},
                            "spec": {"containers": [{"name": "nginx", "image": "nginx:1.25"}]}
                        }
                    }
                }),
            )
            .await
            .unwrap();

        let resource =
            crate::api::inject_resource_version(deployment.data, deployment.resource_version);
        let db_handle = handle_for(db);
        let result = dispatcher
            .reconcile(&resource, &db_handle, "test-node")
            .await;
        assert!(result.is_ok());

        // Verify ReplicaSet was created — query through the trait.
        let rs_list = db_handle
            .list_resources(
                "apps/v1",
                "ReplicaSet",
                Some("default"),
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .unwrap();
        assert_eq!(rs_list.items.len(), 1);
    }

    #[tokio::test]
    async fn test_dispatcher_skips_namespaced_controller_when_namespace_terminating() {
        let (db, db_handle) = crate::datastore::test_support::in_memory_with_handle().await;
        db.create_namespace(
            "terminating-jobs",
            json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": "terminating-jobs",
                    "uid": "terminating-ns-uid",
                    "deletionTimestamp": crate::utils::k8s_timestamp(),
                },
                "spec": {"finalizers": ["kubernetes"]},
                "status": {"phase": "Terminating"}
            }),
        )
        .await
        .unwrap();
        let job = db
            .create_resource(
                "batch/v1",
                "Job",
                Some("terminating-jobs"),
                "cleanup-race",
                json!({
                    "apiVersion": "batch/v1",
                    "kind": "Job",
                    "metadata": {
                        "name": "cleanup-race",
                        "namespace": "terminating-jobs",
                        "uid": "job-cleanup-race"
                    },
                    "spec": {
                        "completions": 1,
                        "parallelism": 1,
                        "template": {
                            "metadata": {"labels": {"job": "cleanup-race"}},
                            "spec": {
                                "containers": [{"name": "pause", "image": "registry.k8s.io/pause:3.10.1"}],
                                "restartPolicy": "Never"
                            }
                        }
                    }
                }),
            )
            .await
            .unwrap();

        let service_ipam = Arc::new(ServiceIpam::new("10.43.128.0/17"));
        let dispatcher = ControllerDispatcher::new(service_ipam);
        dispatcher
            .set_pod_repository(crate::controllers::test_utils::pod_repository_for_test(&db))
            .await;

        let resource = crate::api::inject_resource_version(job.data, job.resource_version);
        dispatcher
            .reconcile(&resource, &db_handle, "test-node")
            .await
            .unwrap();

        let pods = db
            .list_resources(
                "v1",
                "Pod",
                Some("terminating-jobs"),
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .unwrap();
        assert_eq!(
            pods.items.len(),
            0,
            "controllers must not create replacement pods inside a terminating namespace"
        );
    }

    #[tokio::test]
    async fn test_dispatcher_ignores_unknown_resource_types() {
        let db = crate::datastore::test_support::in_memory().await;
        let service_ipam = Arc::new(ServiceIpam::new("10.43.128.0/17"));
        let dispatcher = ControllerDispatcher::new(service_ipam);

        let unknown = json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "test", "namespace": "default"}
        });

        let db_handle = handle_for(db);
        let result = dispatcher
            .reconcile(&unknown, &db_handle, "test-node")
            .await;
        assert!(result.is_ok());
    }

    #[test]
    fn endpoints_api_objects_are_not_controller_dispatch_keys() {
        assert_eq!(
            controller_kind_static("v1", "Endpoints"),
            None,
            "real Endpoints API objects are mirrored by side effects; they must not be routed to the Service-shaped EndpointsController"
        );
    }

    #[tokio::test]
    async fn test_dispatcher_returns_error_for_missing_api_version() {
        let db = crate::datastore::test_support::in_memory().await;
        let service_ipam = Arc::new(ServiceIpam::new("10.43.128.0/17"));
        let dispatcher = ControllerDispatcher::new(service_ipam);

        let bad_resource = json!({
            "kind": "Deployment",
            "metadata": {"name": "test"}
        });

        let db_handle = handle_for(db);
        let result = dispatcher
            .reconcile(&bad_resource, &db_handle, "test-node")
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("apiVersion"));
    }

    #[tokio::test]
    async fn test_dispatcher_returns_error_for_missing_kind() {
        let db = crate::datastore::test_support::in_memory().await;
        let service_ipam = Arc::new(ServiceIpam::new("10.43.128.0/17"));
        let dispatcher = ControllerDispatcher::new(service_ipam);

        let bad_resource = json!({
            "apiVersion": "apps/v1",
            "metadata": {"name": "test"}
        });

        let db_handle = handle_for(db);
        let result = dispatcher
            .reconcile(&bad_resource, &db_handle, "test-node")
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("kind"));
    }

    #[tokio::test]
    async fn test_set_sync_context_accepts_datastore_handle() {
        // Compile-time + runtime guard: set_sync_context must accept a
        // DatastoreHandle (Arc<dyn DatastoreBackend>) without requiring a
        // concrete Datastore at the call site.
        let db = crate::datastore::test_support::in_memory().await;
        let service_ipam = Arc::new(ServiceIpam::new("10.43.128.0/17"));
        let dispatcher = ControllerDispatcher::new(service_ipam);

        let db_handle: DatastoreHandle = Arc::new(db);
        dispatcher
            .set_sync_context(db_handle, "test-node".to_string())
            .await;
    }

    #[tokio::test]
    async fn test_finished_job_with_future_ttl_is_requeued_for_cleanup_deadline() {
        let db = crate::datastore::test_support::in_memory().await;
        let db_handle = handle_for(db.clone());
        let service_ipam = Arc::new(ServiceIpam::new("10.43.128.0/17"));
        let task_supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let dispatcher =
            ControllerDispatcher::with_task_supervisor(service_ipam, task_supervisor.clone());
        dispatcher
            .set_pod_repository(crate::controllers::test_utils::pod_repository_for_test(&db))
            .await;

        let finish_time = (chrono::Utc::now() - chrono::Duration::milliseconds(500))
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        db.create_resource(
            "batch/v1",
            "Job",
            Some("default"),
            "ttl-delayed-job",
            json!({
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {
                    "name": "ttl-delayed-job",
                    "namespace": "default",
                    "uid": "uid-ttl-delayed-job"
                },
                "spec": {
                    "ttlSecondsAfterFinished": 1,
                    "completions": 1,
                    "parallelism": 1,
                    "template": {
                        "spec": {
                            "containers": [{"name": "worker", "image": "busybox"}],
                            "restartPolicy": "Never"
                        }
                    }
                },
                "status": {
                    "conditions": [{
                        "type": "Complete",
                        "status": "True",
                        "lastTransitionTime": finish_time
                    }],
                    "succeeded": 1,
                    "completionTime": finish_time
                }
            }),
        )
        .await
        .unwrap();
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "ttl-delayed-job-pod",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "ttl-delayed-job-pod",
                    "namespace": "default",
                    "uid": "uid-ttl-delayed-job-pod",
                    "ownerReferences": [{
                        "apiVersion": "batch/v1",
                        "kind": "Job",
                        "name": "ttl-delayed-job",
                        "uid": "uid-ttl-delayed-job",
                        "controller": true
                    }]
                },
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                },
                "status": {"phase": "Succeeded"}
            }),
        )
        .await
        .unwrap();

        let key = Key::new("batch/v1", "Job", "default", "ttl-delayed-job");
        dispatcher.dispatch_key(&key, &db_handle, "test-node").await;

        assert!(
            dispatcher.queued_reconcile_keys_for_test().await.is_empty(),
            "future ttlSecondsAfterFinished must arm a timer instead of immediately requeueing"
        );
        let active_timers =
            task_supervisor.active_tasks(Some(crate::task_supervisor::TaskCategory::Timer));
        assert!(
            active_timers
                .iter()
                .any(|task| task.name == "workqueue_add_after"),
            "future ttlSecondsAfterFinished must use the supervised workqueue timer"
        );

        let requeued = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            dispatcher.take_reconcile_key_for_test(),
        )
        .await
        .expect("finished Job should be requeued when ttlSecondsAfterFinished expires");
        assert_eq!(requeued.api_version, "batch/v1");
        assert_eq!(requeued.kind, "Job");
        assert_eq!(requeued.namespace.as_deref(), Some("default"));
        assert_eq!(requeued.name, "ttl-delayed-job");
    }

    #[tokio::test]
    async fn test_deleting_finished_job_with_expired_ttl_is_not_requeued() {
        let db = crate::datastore::test_support::in_memory().await;
        let db_handle = handle_for(db.clone());
        let service_ipam = Arc::new(ServiceIpam::new("10.43.128.0/17"));
        let task_supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let dispatcher =
            ControllerDispatcher::with_task_supervisor(service_ipam, task_supervisor.clone());
        dispatcher
            .set_pod_repository(crate::controllers::test_utils::pod_repository_for_test(&db))
            .await;

        let finish_time = (chrono::Utc::now() - chrono::Duration::seconds(30))
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        db.create_resource(
            "batch/v1",
            "Job",
            Some("default"),
            "ttl-deleting-job",
            json!({
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {
                    "name": "ttl-deleting-job",
                    "namespace": "default",
                    "uid": "uid-ttl-deleting-job",
                    "deletionTimestamp": crate::utils::k8s_timestamp(),
                    "finalizers": ["foregroundDeletion"]
                },
                "spec": {
                    "ttlSecondsAfterFinished": 0,
                    "completions": 1,
                    "parallelism": 1,
                    "template": {
                        "spec": {
                            "containers": [{"name": "worker", "image": "busybox"}],
                            "restartPolicy": "Never"
                        }
                    }
                },
                "status": {
                    "conditions": [{
                        "type": "Complete",
                        "status": "True",
                        "lastTransitionTime": finish_time
                    }],
                    "succeeded": 1,
                    "completionTime": finish_time
                }
            }),
        )
        .await
        .unwrap();

        let key = Key::new("batch/v1", "Job", "default", "ttl-deleting-job");
        dispatcher.dispatch_key(&key, &db_handle, "test-node").await;

        assert!(
            dispatcher.queued_reconcile_keys_for_test().await.is_empty(),
            "TTL scheduler must not immediately requeue once foreground Job deletion has started"
        );
        let active_timers =
            task_supervisor.active_tasks(Some(crate::task_supervisor::TaskCategory::Timer));
        assert!(
            !active_timers
                .iter()
                .any(|task| task.name == "workqueue_add_after"),
            "TTL scheduler must not arm a delayed workqueue timer once foreground Job deletion has started"
        );
    }
}
