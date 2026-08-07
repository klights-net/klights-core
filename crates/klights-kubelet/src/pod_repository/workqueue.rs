//! Durable retry queue for deferred pod and namespace delete work.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Result;
use klights_leader_api::{ControllerCoordination, ControllerLease, ControllerScope};
use klights_pod_api::{
    PodGetRequest, PodLifecycleWakeup, PodLifecycleWakeupRequest, PodListRequest, PodQuery,
    UnscheduledPodDeletion, UnscheduledPodDeletionOutcome, UnscheduledPodDeletionRequest,
};
use klights_reconcile_api::{
    GcPodDeleteRequest, GcPodDeleteSink, NamespaceTerminationOutcome, NamespaceTerminationRequest,
    NamespaceTerminationSink,
};
use serde_json::{Map, Value, json};
use tokio::sync::Notify;

use klights_node_store::{PodWorkqueueLeaseToken, PodWorkqueueMutationOutcome};
use klights_reconcile_api::ReconcileFailureMetrics;
use klights_supervisor::{TaskCategory, TaskSupervisor};
use klights_types::PodIdentity;

const MAX_ATTEMPTS: i64 = 720;
const MIN_DELAY_MS: i64 = 5_000;
const POD_DELETE_TARGET_NODE_PAYLOAD_KEY: &str = "target_node";
const POD_DELETE_LAST_RESIGNAL_MS_PAYLOAD_KEY: &str = "last_resignal_ms";
const REMOTE_POD_DELETE_RESIGNAL_MIN_INTERVAL_MS: i64 = 30_000;
const WORK_LEASE_MS: i64 = 30_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PodWorkqueueKind {
    Pod,
    Namespace,
}

#[derive(Clone, Debug)]
pub struct PodWorkqueueEntry {
    pub id: i64,
    pub kind: PodWorkqueueKind,
    pub namespace: String,
    pub name: String,
    pub uid: String,
    pub payload: Value,
    pub attempt_count: i64,
    pub lease_token: PodWorkqueueLeaseToken,
}

#[async_trait::async_trait]
pub trait PodWorkqueuePersistence: Send + Sync {
    async fn enqueue(
        &self,
        kind: PodWorkqueueKind,
        pod: &PodIdentity,
        payload: Value,
        attempt_count: i64,
        min_delay_ms: i64,
        last_error: Option<&str>,
    ) -> Result<()>;
    async fn peek_next_due(&self) -> Result<Option<i64>>;
    async fn claim_due(
        &self,
        now_ms: i64,
        lease_duration_ms: i64,
    ) -> Result<Option<PodWorkqueueEntry>>;
    async fn acknowledge(
        &self,
        token: PodWorkqueueLeaseToken,
    ) -> Result<PodWorkqueueMutationOutcome>;
    async fn requeue(
        &self,
        row: PodWorkqueueEntry,
        attempt_count: i64,
        min_delay_ms: i64,
        error: &str,
    ) -> Result<PodWorkqueueMutationOutcome>;
}

pub struct PodWorkqueue {
    pod_query: Arc<dyn PodQuery>,
    unscheduled_deletion: Option<Arc<dyn UnscheduledPodDeletion>>,
    leader_coordination: Option<Arc<dyn ControllerCoordination>>,
    persistence: Arc<dyn PodWorkqueuePersistence>,
    supervisor: Arc<TaskSupervisor>,
    metrics: Arc<dyn ReconcileFailureMetrics>,
    wall_clock: Arc<dyn crate::runtime_clock::RuntimeClock>,
    wake: Arc<Notify>,
    lifecycle_router: std::sync::Mutex<Option<Arc<dyn PodLifecycleWakeup>>>,
    local_node_name: std::sync::Mutex<Option<String>>,
    remote_pod_delete_resignal_sink: std::sync::Mutex<Option<std::sync::Weak<dyn GcPodDeleteSink>>>,
    namespace_termination: std::sync::Mutex<Option<Arc<dyn NamespaceTerminationSink>>>,
    reconciler_started: AtomicBool,
    /// Set to true when `start()` is called. Enables Task 4.1 tests to
    /// verify that `build_parts` defers startup to `PodRepositoryBackground`.
    start_called: AtomicBool,
}

impl PodWorkqueue {
    pub fn new(
        pod_query: Arc<dyn PodQuery>,
        persistence: impl PodWorkqueuePersistence + 'static,
        supervisor: Arc<TaskSupervisor>,
        metrics: Arc<dyn ReconcileFailureMetrics>,
        wall_clock: Arc<dyn crate::runtime_clock::RuntimeClock>,
    ) -> Arc<Self> {
        Self::new_with_unscheduled_deletion(
            pod_query,
            persistence,
            supervisor,
            metrics,
            wall_clock,
            None,
            None,
        )
    }

    pub fn new_leader(
        pod_query: Arc<dyn PodQuery>,
        persistence: impl PodWorkqueuePersistence + 'static,
        supervisor: Arc<TaskSupervisor>,
        metrics: Arc<dyn ReconcileFailureMetrics>,
        unscheduled_deletion: Arc<dyn UnscheduledPodDeletion>,
        leader_coordination: Arc<dyn ControllerCoordination>,
        wall_clock: Arc<dyn crate::runtime_clock::RuntimeClock>,
    ) -> Arc<Self> {
        Self::new_with_unscheduled_deletion(
            pod_query,
            persistence,
            supervisor,
            metrics,
            wall_clock,
            Some(unscheduled_deletion),
            Some(leader_coordination),
        )
    }

    fn new_with_unscheduled_deletion(
        pod_query: Arc<dyn PodQuery>,
        persistence: impl PodWorkqueuePersistence + 'static,
        supervisor: Arc<TaskSupervisor>,
        metrics: Arc<dyn ReconcileFailureMetrics>,
        wall_clock: Arc<dyn crate::runtime_clock::RuntimeClock>,
        unscheduled_deletion: Option<Arc<dyn UnscheduledPodDeletion>>,
        leader_coordination: Option<Arc<dyn ControllerCoordination>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            pod_query,
            unscheduled_deletion,
            leader_coordination,
            persistence: Arc::new(persistence),
            supervisor,
            metrics,
            wall_clock,
            wake: Arc::new(Notify::new()),
            lifecycle_router: std::sync::Mutex::new(None),
            local_node_name: std::sync::Mutex::new(None),
            remote_pod_delete_resignal_sink: std::sync::Mutex::new(None),
            namespace_termination: std::sync::Mutex::new(None),
            reconciler_started: AtomicBool::new(false),
            start_called: AtomicBool::new(false),
        })
    }

    pub fn set_namespace_termination_sink(&self, sink: Arc<dyn NamespaceTerminationSink>) {
        *self.namespace_termination.lock().unwrap() = Some(sink);
    }

    pub async fn start(self: &Arc<Self>) -> Result<()> {
        self.start_called.store(true, Ordering::Release);
        if self.leader_coordination.is_some() {
            self.ensure_reconciler_started().await?;
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn start_called(&self) -> bool {
        self.start_called.load(Ordering::Acquire)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn current_test_leader_lease(&self) -> Option<ControllerLease> {
        self.leader_coordination.as_ref().map(|coordination| {
            coordination
                .try_acquire(ControllerScope::Cluster)
                .expect("test leader coordination must be authoritative")
        })
    }

    pub fn set_lifecycle_router_for_node<Route>(&self, router: Arc<Route>, local_node_name: String)
    where
        Route: crate::pod_repository::PodLifecycleRouteSink + 'static,
    {
        let route: Arc<dyn crate::pod_repository::PodLifecycleRouteSink> = router;
        let wakeup: Arc<dyn PodLifecycleWakeup> =
            Arc::new(crate::pod_repository::PodLifecycleWakeupService::new(route));
        *self.lifecycle_router.lock().unwrap() = Some(wakeup);
        *self.local_node_name.lock().unwrap() = Some(local_node_name);
    }

    pub fn set_remote_pod_delete_resignal_sink(&self, sink: std::sync::Weak<dyn GcPodDeleteSink>) {
        *self.remote_pod_delete_resignal_sink.lock().unwrap() = Some(sink);
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn set_remote_pod_delete_resignal_sink_for_tests(&self, sink: Arc<dyn GcPodDeleteSink>) {
        self.set_remote_pod_delete_resignal_sink(Arc::downgrade(&sink));
    }

    #[cfg(any(test, feature = "test-support"))]
    pub async fn enqueue_deferred_delete(
        self: &Arc<Self>,
        ns: String,
        name: String,
        uid: String,
        run_after: Duration,
    ) -> Result<()> {
        self.enqueue_deferred_delete_with_target_node(ns, name, uid, run_after, None)
            .await
    }

    pub async fn enqueue_deferred_delete_with_target_node(
        self: &Arc<Self>,
        ns: String,
        name: String,
        uid: String,
        run_after: Duration,
        target_node: Option<String>,
    ) -> Result<()> {
        self.ensure_reconciler_started().await?;
        self.enqueue_deferred_delete_row_with_target_node(ns, name, uid, run_after, target_node)
            .await?;
        self.wake.notify_one();
        Ok(())
    }

    pub async fn enqueue_deferred_delete_row_with_target_node(
        &self,
        ns: String,
        name: String,
        uid: String,
        run_after: Duration,
        target_node: Option<String>,
    ) -> Result<()> {
        let delay_ms = run_after.as_millis().min(i64::MAX as u128) as i64;
        let pod = PodIdentity::new(&ns, &name, &uid);
        let payload = pod_delete_target_payload(target_node.as_deref());
        self.persistence
            .enqueue(PodWorkqueueKind::Pod, &pod, payload, 0, delay_ms, None)
            .await?;
        Ok(())
    }

    /// Enqueue a namespace termination attempt onto the durable
    /// pod_workqueue. The reconciler loop picks it up immediately
    /// (notify_one), runs `run_namespace_termination`, and on Err
    /// re-schedules with `MIN_DELAY_MS` backoff up to `MAX_ATTEMPTS`.
    /// Pod work is never subject to that namespace-only attempt ceiling.
    /// Each retry is short-lived so the PodDeleteWorkqueue slot
    /// churns naturally between many concurrent namespace deletes.
    pub async fn enqueue_namespace_termination(
        self: &Arc<Self>,
        namespace: String,
        uid: String,
    ) -> Result<()> {
        self.ensure_reconciler_started().await?;
        let pod = PodIdentity::new("", &namespace, &uid);
        self.persistence
            .enqueue(PodWorkqueueKind::Namespace, &pod, json!({}), 0, 0, None)
            .await?;
        self.wake.notify_one();
        Ok(())
    }

    pub async fn enqueue_actor_deletes_for_terminating_namespace(
        self: &Arc<Self>,
        namespace: &str,
    ) -> Result<()> {
        self.ensure_reconciler_started().await?;
        self.enqueue_actor_deletes_for_terminating_namespace_pods(namespace)
            .await
    }

    async fn ensure_reconciler_started(self: &Arc<Self>) -> Result<()> {
        if self.reconciler_started.load(Ordering::Relaxed) {
            return Ok(());
        }
        if self
            .reconciler_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Ok(());
        }

        let this = self.clone();
        self.supervisor
            .spawn_async(
                TaskCategory::Background,
                "pod_workqueue_reconciler",
                async move { this.reconciler_loop().await },
            )
            .await
            .map_err(|e| anyhow::anyhow!("failed to spawn pod_workqueue reconciler: {e:?}"))?;
        Ok(())
    }

    #[cfg(any(test, feature = "test-support"))]
    pub async fn ensure_reconciler_started_for_tests(self: &Arc<Self>) -> Result<()> {
        self.ensure_reconciler_started().await
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn set_local_node_name_for_tests(&self, local_node_name: Option<String>) {
        *self.local_node_name.lock().unwrap() = local_node_name;
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn supervisor_for_tests(&self) -> Arc<TaskSupervisor> {
        self.supervisor.clone()
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn notify_for_tests(&self) {
        self.wake.notify_one();
    }

    async fn reconciler_loop(self: Arc<Self>) {
        let cancel = self.supervisor.root_cancellation_token();
        let mut leader_lease: Option<ControllerLease> = None;
        let mut scan_on_gain = false;
        loop {
            if cancel.is_cancelled() {
                return;
            }
            if let Some(coordination) = self.leader_coordination.as_ref()
                && leader_lease.is_none()
            {
                leader_lease = Some(tokio::select! {
                    result = coordination.acquire(ControllerScope::Cluster) => {
                        match result {
                            Ok(lease) => lease,
                            Err(error) => {
                                tracing::warn!(%error, "pod_workqueue: leader coordination closed");
                                return;
                            }
                        }
                    }
                    _ = cancel.cancelled() => return,
                });
                scan_on_gain = true;
            }
            if self.leader_coordination.is_some() && scan_on_gain {
                if let Err(error) = self
                    .enqueue_terminating_unbound_pods_on_leadership_gain()
                    .await
                {
                    tracing::warn!(%error, "pod_workqueue: leadership handoff discovery failed");
                    tokio::select! {
                        _ = self.supervisor.sleep(
                            "pod_workqueue_leadership_handoff_retry",
                            Duration::from_millis(250),
                        ) => {}
                        _ = coordination_revoked(
                            &self.leader_coordination,
                            &leader_lease,
                        ) => {
                            leader_lease = None;
                        }
                        _ = cancel.cancelled() => return,
                    }
                    scan_on_gain = leader_lease.is_some();
                    continue;
                }
                scan_on_gain = false;
            }
            let next_due = match self.persistence.peek_next_due().await {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!(error = %e, "pod_workqueue: peek_next_due failed");
                    tokio::select! {
                        _ = self.supervisor.sleep(
                            "pod_workqueue_reconciler_error_backoff",
                            Duration::from_millis(250),
                        ) => {}
                        _ = cancel.cancelled() => return,
                    }
                    continue;
                }
            };

            match next_due {
                None => {
                    tokio::select! {
                        _ = self.wake.notified() => {}
                        _ = coordination_revoked(
                            &self.leader_coordination,
                            &leader_lease,
                        ) => {
                            leader_lease = None;
                        }
                        _ = cancel.cancelled() => return,
                    }
                    continue;
                }
                Some(ts) => {
                    let now = self.wall_clock.now_ms();
                    if ts > now {
                        let sleep_for = Duration::from_millis((ts - now) as u64);
                        tokio::select! {
                            _ = self.supervisor.sleep("pod_workqueue_sleep_until_due", sleep_for) => {}
                            _ = self.wake.notified() => continue,
                            _ = coordination_revoked(
                                &self.leader_coordination,
                                &leader_lease,
                            ) => {
                                leader_lease = None;
                                continue;
                            }
                            _ = cancel.cancelled() => return,
                        }
                    }
                }
            }

            // Claim the next due entry first so we can route to the right
            // task category. Pod-delete work runs on PodDeleteWorkqueue (slot
            // limit gates concurrent pod cleanup); namespace-termination
            // runs on Background (unlimited) so a slow ns retry cannot
            // block pod cleanup, and many concurrent ns deletes can each
            // make progress without serializing through the limit.
            let row = match self
                .persistence
                .claim_due(self.wall_clock.now_ms(), WORK_LEASE_MS)
                .await
            {
                Ok(Some(row)) => row,
                Ok(None) => continue,
                Err(e) => {
                    tracing::error!(error = %e, "pod_workqueue: claim_due failed");
                    tokio::select! {
                        _ = self.supervisor.sleep(
                            "pod_workqueue_claim_error_backoff",
                            Duration::from_millis(250),
                        ) => {}
                        _ = coordination_revoked(
                            &self.leader_coordination,
                            &leader_lease,
                        ) => leader_lease = None,
                        _ = cancel.cancelled() => return,
                    }
                    continue;
                }
            };
            if !coordination_is_current(&self.leader_coordination, &leader_lease) {
                self.park_claimed_row(row, "leadership lost before deferred delete")
                    .await;
                continue;
            }

            let category = match row.kind {
                PodWorkqueueKind::Pod => TaskCategory::PodDeleteWorkqueue,
                PodWorkqueueKind::Namespace => TaskCategory::Background,
            };

            // For PodDeleteWorkqueue (limit-bounded) only: wait for a free
            // slot before spawning. Background is unlimited so no wait.
            if matches!(row.kind, PodWorkqueueKind::Pod)
                && !self.supervisor.is_category_free(category)
            {
                let free = self.supervisor.category_free_notify(category);
                enum WaitOutcome {
                    Ready,
                    Wake,
                    Revoked,
                    Cancelled,
                    LeaseDeadline,
                }
                let lease_remaining_ms = row
                    .lease_token
                    .leased_next_due_ms()
                    .get()
                    .saturating_sub(self.wall_clock.now_ms())
                    .max(0) as u64;
                let outcome = tokio::select! {
                    _ = free.notified() => WaitOutcome::Ready,
                    _ = self.wake.notified() => WaitOutcome::Wake,
                    _ = coordination_revoked(
                        &self.leader_coordination,
                        &leader_lease,
                    ) => WaitOutcome::Revoked,
                    _ = cancel.cancelled() => WaitOutcome::Cancelled,
                    _ = self.supervisor.sleep(
                        "pod_workqueue_claim_lease_deadline",
                        Duration::from_millis(lease_remaining_ms),
                    ) => WaitOutcome::LeaseDeadline,
                };
                match outcome {
                    WaitOutcome::Ready => {}
                    WaitOutcome::Wake => {
                        self.park_claimed_row(row, "deferred delete category wait interrupted")
                            .await;
                        continue;
                    }
                    WaitOutcome::Revoked => {
                        leader_lease = None;
                        self.park_claimed_row(row, "leadership lost during category wait")
                            .await;
                        continue;
                    }
                    WaitOutcome::Cancelled => {
                        self.park_claimed_row(row, "shutdown during deferred delete category wait")
                            .await;
                        return;
                    }
                    WaitOutcome::LeaseDeadline => {
                        self.park_claimed_row(row, "deferred delete claim lease reached deadline")
                            .await;
                        continue;
                    }
                }
            }

            let this = self.clone();
            let task_lease = leader_lease.clone();
            let task_row = row.clone();
            if let Err(error) = self
                .supervisor
                .spawn_async(category, "pod_workqueue_retry", async move {
                    this.run_retry(task_row, task_lease).await;
                })
                .await
            {
                tracing::warn!(?error, "pod_workqueue: retry spawn refused");
                self.park_claimed_row(row, "deferred delete retry spawn refused")
                    .await;
            }
        }
    }

    async fn run_retry(
        self: Arc<Self>,
        mut row: PodWorkqueueEntry,
        leader_lease: Option<ControllerLease>,
    ) {
        let _work_id = row.id;
        let target_node = row
            .payload
            .get(POD_DELETE_TARGET_NODE_PAYLOAD_KEY)
            .and_then(|value| value.as_str())
            .map(ToString::to_string);
        let retry_now_ms = self.wall_clock.now_ms();
        let result = {
            let operation = async {
                match row.kind {
                    PodWorkqueueKind::Pod => {
                        self.run_pod_delete_full_with_target_node_and_payload(
                            row.namespace.clone(),
                            row.name.clone(),
                            row.uid.clone(),
                            target_node,
                            &mut row.payload,
                            retry_now_ms,
                        )
                        .await
                    }
                    PodWorkqueueKind::Namespace => {
                        self.run_namespace_termination(row.name.clone(), row.uid.clone())
                            .await
                    }
                }
            };
            match (&self.leader_coordination, leader_lease.clone()) {
                (Some(coordination), Some(lease)) => {
                    klights_leader_api::scope_controller_lease(
                        coordination.clone(),
                        lease,
                        operation,
                    )
                    .await
                }
                (None, None) => operation.await,
                _ => Err(anyhow::anyhow!(
                    "deferred delete lacks a matching controller lease"
                )),
            }
        };

        if !coordination_is_current(&self.leader_coordination, &leader_lease) {
            self.park_claimed_row(row, "leadership lost during deferred delete")
                .await;
            return;
        }
        if result.is_ok() {
            match self.persistence.acknowledge(row.lease_token).await {
                Ok(PodWorkqueueMutationOutcome::Applied) => {}
                Ok(PodWorkqueueMutationOutcome::Stale) => {
                    tracing::debug!(
                        "pod_workqueue: success acknowledge lost stale lease ownership"
                    );
                }
                Err(error) => {
                    tracing::error!(%error, "pod_workqueue: success acknowledge failed");
                }
            }
            return;
        }

        let err = result.expect_err("error is present");
        if row.kind == PodWorkqueueKind::Namespace && row.attempt_count >= MAX_ATTEMPTS {
            match self.persistence.acknowledge(row.lease_token).await {
                Ok(PodWorkqueueMutationOutcome::Applied | PodWorkqueueMutationOutcome::Stale) => {}
                Err(error) => {
                    tracing::error!(%error, "pod_workqueue: namespace dead-letter acknowledge failed");
                    return;
                }
            }
            self.bump_dead_letter_metric(row.kind);
            tracing::error!(
                kind = ?row.kind,
                namespace = %row.namespace,
                name = %row.name,
                attempts = row.attempt_count,
                error = %err,
                "pod_workqueue: dead-letter after max attempts"
            );
            return;
        }

        let next_attempt = match row.attempt_count.checked_add(1) {
            Some(attempt) => attempt,
            None => {
                tracing::error!("pod_workqueue: attempt count overflow; parking without increment");
                row.attempt_count
            }
        };
        match self
            .persistence
            .requeue(row, next_attempt, MIN_DELAY_MS, &format!("{err:#}"))
            .await
        {
            Ok(PodWorkqueueMutationOutcome::Applied) => {}
            Ok(PodWorkqueueMutationOutcome::Stale) => {
                tracing::debug!("pod_workqueue: retry requeue lost stale lease ownership");
                return;
            }
            Err(enq_err) => {
                tracing::error!(error = %enq_err, "pod_workqueue: record_failure failed");
                return;
            }
        }
        self.wake.notify_one();
    }

    #[cfg(any(test, feature = "test-support"))]
    pub async fn run_retry_for_tests(
        self: Arc<Self>,
        row: PodWorkqueueEntry,
        leader_lease: Option<ControllerLease>,
    ) {
        self.run_retry(row, leader_lease).await;
    }

    async fn park_claimed_row(&self, row: PodWorkqueueEntry, reason: &str) {
        let attempt_count = row.attempt_count;
        match self
            .persistence
            .requeue(row, attempt_count, 0, reason)
            .await
        {
            Ok(PodWorkqueueMutationOutcome::Applied) => self.wake.notify_one(),
            Ok(PodWorkqueueMutationOutcome::Stale) => {}
            Err(error) => {
                tracing::error!(%error, "pod_workqueue: failed to park work after leadership loss");
            }
        }
    }

    async fn enqueue_terminating_unbound_pods_on_leadership_gain(&self) -> Result<()> {
        let pods = self
            .pod_query
            .list_pods(PodListRequest::try_new(None, None, None, None, None)?)
            .await?;
        for pod in pods.into_parts().0 {
            let terminating = pod
                .data
                .pointer("/metadata/deletionTimestamp")
                .and_then(Value::as_str)
                .is_some_and(|value| !value.is_empty());
            let unbound = pod
                .data
                .pointer("/spec/nodeName")
                .and_then(Value::as_str)
                .is_none_or(|value| value.trim().is_empty());
            if terminating && unbound && !pod.uid.is_empty() {
                self.enqueue_deferred_delete_row_with_target_node(
                    pod.namespace.unwrap_or_default(),
                    pod.name,
                    pod.uid,
                    Duration::ZERO,
                    None,
                )
                .await?;
            }
        }
        self.wake.notify_one();
        Ok(())
    }

    #[cfg(any(test, feature = "test-support"))]
    pub async fn run_pod_delete_full_with_target_node_for_tests(
        &self,
        ns: String,
        name: String,
        uid: String,
        target_node: Option<String>,
    ) -> Result<()> {
        let mut payload = pod_delete_target_payload(target_node.as_deref());
        self.run_pod_delete_full_with_target_node_and_payload(
            ns,
            name,
            uid,
            target_node,
            &mut payload,
            self.wall_clock.now_ms(),
        )
        .await
    }

    async fn run_pod_delete_full_with_target_node_and_payload(
        &self,
        ns: String,
        name: String,
        uid: String,
        target_node: Option<String>,
        payload: &mut Value,
        retry_now_ms: i64,
    ) -> Result<()> {
        let pod_before_delete = self
            .pod_query
            .get_pod(PodGetRequest::try_by_name(&ns, &name)?)
            .await?;
        match pod_before_delete {
            Some(_resource) if uid.is_empty() => {
                anyhow::bail!(
                    "pod deferred delete missing UID for live Pod {}/{}; refusing name-only delete",
                    ns,
                    name
                );
            }
            Some(resource) if resource.uid == uid => {
                let resource = if resource
                    .data
                    .pointer("/spec/nodeName")
                    .and_then(|node| node.as_str())
                    .is_none_or(|node| node.trim().is_empty())
                {
                    let unscheduled_deletion = self.unscheduled_deletion.as_ref().ok_or_else(|| {
                        anyhow::anyhow!(
                            "pod deferred delete requires leader unscheduled-delete capability for {}/{} uid {}",
                            ns,
                            name,
                            uid
                        )
                    })?;
                    let request = UnscheduledPodDeletionRequest::try_new(
                        PodIdentity::new(&ns, &name, &uid),
                        resource.resource_version,
                    )
                    .map_err(|error| {
                        anyhow::anyhow!("invalid unscheduled Pod delete request: {error}")
                    })?;
                    match unscheduled_deletion
                        .delete_unscheduled_pod(request)
                        .await
                        .map_err(|error| {
                            anyhow::anyhow!("unscheduled Pod delete failed: {error}")
                        })? {
                        UnscheduledPodDeletionOutcome::Removed => return Ok(()),
                        UnscheduledPodDeletionOutcome::FinalizersPending => {
                            anyhow::bail!(
                                "unscheduled pod {}/{} uid {} awaiting finalizer removal",
                                ns,
                                name,
                                uid
                            );
                        }
                        UnscheduledPodDeletionOutcome::DeferToActor => {
                            // The capability observed a bound Pod. Re-read so
                            // the lifecycle actor receives the current row
                            // only if the queued UID still owns the slot.
                            match self
                                .pod_query
                                .get_pod(PodGetRequest::try_by_name(&ns, &name)?)
                                .await?
                            {
                                Some(fresh) if fresh.uid == uid => fresh,
                                _ => return Ok(()),
                            }
                        }
                        UnscheduledPodDeletionOutcome::Retry => {
                            anyhow::bail!(
                                "unscheduled pod {}/{} uid {} changed during delete CAS; retrying from a fresh observation",
                                ns,
                                name,
                                uid
                            );
                        }
                    }
                } else {
                    resource
                };
                if !self.should_process_deferred_pod_delete_for_target(
                    "pod deferred delete is not targeted to this node",
                    &ns,
                    &name,
                    &uid,
                    target_node.as_deref(),
                ) {
                    if remote_pod_delete_resignal_due(payload, retry_now_ms) {
                        self.resignal_remote_pod_delete(&ns, &name, &uid).await?;
                    } else {
                        tracing::debug!(
                            namespace = %ns,
                            pod = %name,
                            uid = %uid,
                            "remote pod delete re-signal throttled"
                        );
                    }
                    anyhow::bail!(
                        "pod deferred delete for remote pod {}/{} uid {} awaiting actor-owned finalization on target node",
                        ns,
                        name,
                        uid
                    );
                }
                if !self.should_process_local_pod_delete(
                    "pod deferred delete skipped local actor wake for non-local Pod",
                    &ns,
                    &name,
                    &uid,
                    &resource.data,
                ) {
                    anyhow::bail!(
                        "pod deferred delete awaiting local actor for {}/{} uid {}",
                        ns,
                        name,
                        uid
                    );
                }
                self.wake_local_actor_for_pod_delete(&ns, &name, &uid, resource)
                    .await?;
                anyhow::bail!(
                    "pod deferred delete waiting for kubelet cleanup for {}/{} uid {}",
                    ns,
                    name,
                    uid
                );
            }
            Some(resource) => {
                tracing::warn!(
                    namespace = %ns,
                    pod = %name,
                    queued_uid = %uid,
                    live_uid = %resource.uid,
                    "pod deferred delete ignored stale UID because a replacement Pod exists"
                );
            }
            None => {}
        }

        Ok(())
    }

    #[cfg(any(test, feature = "test-support"))]
    pub async fn run_pod_delete_full_with_target_node_and_payload_for_tests(
        &self,
        ns: String,
        name: String,
        uid: String,
        target_node: Option<String>,
        payload: &mut Value,
        retry_now_ms: i64,
    ) -> Result<()> {
        self.run_pod_delete_full_with_target_node_and_payload(
            ns,
            name,
            uid,
            target_node,
            payload,
            retry_now_ms,
        )
        .await
    }

    async fn resignal_remote_pod_delete(&self, ns: &str, name: &str, uid: &str) -> Result<()> {
        let sink = self
            .remote_pod_delete_resignal_sink
            .lock()
            .unwrap()
            .as_ref()
            .and_then(std::sync::Weak::upgrade);
        let Some(sink) = sink else {
            tracing::debug!(
                namespace = %ns,
                pod = %name,
                uid = %uid,
                "remote pod delete retry has no GC re-signal sink"
            );
            return Ok(());
        };
        sink.request_gc_pod_delete(GcPodDeleteRequest::new(PodIdentity::new(ns, name, uid)))
            .await
            .map_err(Into::into)
    }

    fn live_pod_belongs_to_local_node(&self, pod: &serde_json::Value) -> bool {
        let Some(local_node_name) = self.local_node_name.lock().unwrap().clone() else {
            tracing::debug!(
                pod_node = %self.pod_node_name_for_log(pod),
                "pod deferred delete skipped local actor wake; local node name is unknown",
            );
            return false;
        };
        let Some(pod_node_name) = pod
            .pointer("/spec/nodeName")
            .and_then(|node| node.as_str())
            .filter(|node| !node.trim().is_empty())
        else {
            return true;
        };
        pod_node_name == local_node_name
    }

    fn local_node_name_for_log(&self) -> String {
        self.local_node_name
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| "<unset>".to_string())
    }

    fn pod_node_name_for_log(&self, pod: &serde_json::Value) -> String {
        pod.pointer("/spec/nodeName")
            .and_then(|node| node.as_str())
            .unwrap_or("<unscheduled>")
            .to_string()
    }

    fn should_process_deferred_pod_delete_for_target(
        &self,
        skip_message: &str,
        namespace: &str,
        pod_name: &str,
        uid: &str,
        target_node: Option<&str>,
    ) -> bool {
        let Some(target_node) = target_node else {
            return true;
        };
        let Some(local_node_name) = self.local_node_name.lock().unwrap().clone() else {
            tracing::debug!(
                namespace = %namespace,
                pod = %pod_name,
                uid = %uid,
                local_node = "unset",
                target_node = %target_node,
                "{}", skip_message
            );
            return false;
        };

        if local_node_name == target_node {
            return true;
        }

        tracing::debug!(
            namespace = %namespace,
            pod = %pod_name,
            uid = %uid,
            target_node = %target_node,
            local_node = %local_node_name,
            "{}", skip_message
        );
        false
    }

    fn should_process_local_pod_delete(
        &self,
        skip_message: &str,
        namespace: &str,
        pod_name: &str,
        uid: &str,
        pod: &serde_json::Value,
    ) -> bool {
        if self.live_pod_belongs_to_local_node(pod) {
            return true;
        }

        tracing::debug!(
            namespace = %namespace,
            pod = %pod_name,
            uid = %uid,
            local_node = %self.local_node_name_for_log(),
            pod_node = %self.pod_node_name_for_log(pod),
            "{}", skip_message
        );
        false
    }

    async fn wake_local_actor_for_pod_delete(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        resource: klights_cluster_core::Resource,
    ) -> Result<()> {
        let Some(router) = self.lifecycle_router.lock().unwrap().clone() else {
            tracing::warn!(
                namespace = %ns,
                pod = %name,
                uid = %uid,
                "pod deferred delete cannot wake actor because lifecycle router is not configured"
            );
            return Ok(());
        };
        let request =
            PodLifecycleWakeupRequest::try_from_pod(PodIdentity::new(ns, name, uid), resource)
                .map_err(|err| anyhow::anyhow!("invalid pod deferred delete actor wake: {err}"))?;
        router
            .wake_pod_lifecycle(request)
            .await
            .map_err(|err| anyhow::anyhow!("pod deferred delete actor wake failed: {err}"))
    }

    async fn run_namespace_termination(
        self: &Arc<Self>,
        namespace: String,
        uid: String,
    ) -> Result<()> {
        // Use the outcome-returning variant. Returning Err on StillPending
        // engages the workqueue's existing 5s-backoff retry path
        // (MIN_DELAY_MS) up to MAX_ATTEMPTS=720 (~1h ceiling) and then
        // dead-letters. Each task is short-lived, so the limit-1
        // PodDeleteWorkqueue slot churns naturally and many concurrent
        // namespace deletes serialize without one delete holding the slot.
        let sink = self
            .namespace_termination
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("namespace termination sink is not configured"))?;
        let outcome = sink
            .reconcile_namespace_termination(NamespaceTerminationRequest {
                namespace: namespace.clone(),
                expected_uid: Some(uid),
            })
            .await;
        match outcome {
            Ok(NamespaceTerminationOutcome::Finalized) => Ok(()),
            Ok(NamespaceTerminationOutcome::StillPending) => {
                self.enqueue_actor_deletes_for_terminating_namespace_pods(&namespace)
                    .await?;
                Err(anyhow::anyhow!(
                    "namespace {} still terminating; will retry",
                    namespace
                ))
            }
            Err(e) => Err(anyhow::anyhow!("namespace termination failed: {:?}", e)),
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub async fn run_namespace_termination_for_tests(
        self: &Arc<Self>,
        namespace: String,
        uid: String,
    ) -> Result<()> {
        self.run_namespace_termination(namespace, uid).await
    }

    async fn enqueue_actor_deletes_for_terminating_namespace_pods(
        self: &Arc<Self>,
        namespace: &str,
    ) -> Result<()> {
        let pods = self
            .pod_query
            .list_pods(PodListRequest::try_new(
                Some(namespace.to_string()),
                None,
                None,
                None,
                None,
            )?)
            .await?;
        let mut enqueued_any = false;
        for resource in pods.into_parts().0 {
            if resource
                .data
                .pointer("/metadata/deletionTimestamp")
                .and_then(Value::as_str)
                .is_none()
            {
                continue;
            }
            if resource.uid.is_empty() {
                tracing::warn!(
                    namespace = %namespace,
                    pod = %resource.name,
                    "namespace termination cannot enqueue actor-owned Pod delete without UID"
                );
                continue;
            }
            let target_node = resource
                .data
                .pointer("/spec/nodeName")
                .and_then(Value::as_str)
                .filter(|node| !node.trim().is_empty())
                .map(ToString::to_string);
            self.enqueue_deferred_delete_row_with_target_node(
                namespace.to_string(),
                resource.name,
                resource.uid,
                Duration::ZERO,
                target_node,
            )
            .await?;
            enqueued_any = true;
        }
        if enqueued_any {
            self.wake.notify_one();
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-support"))]
    pub async fn enqueue_actor_deletes_for_terminating_namespace_pods_for_tests(
        self: &Arc<Self>,
        namespace: &str,
    ) -> Result<()> {
        self.enqueue_actor_deletes_for_terminating_namespace_pods(namespace)
            .await
    }

    fn bump_dead_letter_metric(&self, kind: PodWorkqueueKind) {
        match kind {
            PodWorkqueueKind::Pod => {
                self.metrics.record_cascade_delete_failure();
            }
            PodWorkqueueKind::Namespace => {
                self.metrics.record_namespace_delete_failure();
            }
        }
    }
}

fn coordination_is_current(
    coordination: &Option<Arc<dyn ControllerCoordination>>,
    lease: &Option<ControllerLease>,
) -> bool {
    match (coordination, lease) {
        (Some(coordination), Some(lease)) => coordination.validate(lease).is_ok(),
        (None, None) => true,
        _ => false,
    }
}

async fn coordination_revoked(
    coordination: &Option<Arc<dyn ControllerCoordination>>,
    lease: &Option<ControllerLease>,
) {
    match (coordination, lease) {
        (Some(coordination), Some(lease)) => coordination.wait_for_revocation(lease).await,
        _ => std::future::pending().await,
    }
}

fn pod_delete_target_payload(target_node: Option<&str>) -> Value {
    let mut payload = Map::new();
    if let Some(target_node) = target_node.filter(|node| !node.trim().is_empty()) {
        payload.insert(
            POD_DELETE_TARGET_NODE_PAYLOAD_KEY.to_string(),
            Value::String(target_node.to_string()),
        );
    }
    Value::Object(payload)
}

fn remote_pod_delete_resignal_due(payload: &mut Value, now_ms: i64) -> bool {
    let Some(payload) = payload.as_object_mut() else {
        return true;
    };
    let last_resignal_ms = payload
        .get(POD_DELETE_LAST_RESIGNAL_MS_PAYLOAD_KEY)
        .and_then(|value| value.as_i64());
    if let Some(last_resignal_ms) = last_resignal_ms
        && now_ms.saturating_sub(last_resignal_ms) < REMOTE_POD_DELETE_RESIGNAL_MIN_INTERVAL_MS
    {
        return false;
    }
    payload.insert(
        POD_DELETE_LAST_RESIGNAL_MS_PAYLOAD_KEY.to_string(),
        Value::Number(now_ms.into()),
    );
    true
}
