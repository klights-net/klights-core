//! Durable retry queue for deferred pod and namespace delete work.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Result;
use serde_json::{Map, Value, json};
use tokio::sync::Notify;

use crate::datastore::{DatastoreHandle, PodWorkqueueEntry, PodWorkqueueKind};
use crate::kubelet::pod_lifecycle_core::message::{LifecycleMessage, PodLifecycleKey};
use crate::kubelet::pod_lifecycle_router::PodLifecycleRouter;
use crate::pod_identity::PodIdentity;
use crate::side_effects::SideEffectMetrics;
use crate::task_supervisor::{TaskCategory, TaskSupervisor};

use super::store::{PodStore, UnscheduledPodDeleteOutcome};
const MAX_ATTEMPTS: i64 = 720;
const MIN_DELAY_MS: i64 = 5_000;
const POD_DELETE_TARGET_NODE_PAYLOAD_KEY: &str = "target_node";
const POD_DELETE_LAST_RESIGNAL_MS_PAYLOAD_KEY: &str = "last_resignal_ms";
const REMOTE_POD_DELETE_RESIGNAL_MIN_INTERVAL_MS: i64 = 30_000;

pub struct PodWorkqueue {
    store: Arc<PodStore>,
    db: DatastoreHandle,
    supervisor: Arc<TaskSupervisor>,
    metrics: Arc<SideEffectMetrics>,
    wake: Arc<Notify>,
    lifecycle_router: std::sync::Mutex<Option<Arc<PodLifecycleRouter>>>,
    local_node_name: std::sync::Mutex<Option<String>>,
    remote_pod_delete_resignal_sink:
        std::sync::Mutex<Option<std::sync::Weak<dyn crate::controllers::gc::GcPodDeleteSink>>>,
    reconciler_started: AtomicBool,
    /// Set to true when `start()` is called. Enables Task 4.1 tests to
    /// verify that `build_parts` defers startup to `PodRepositoryBackground`.
    start_called: AtomicBool,
}

impl PodWorkqueue {
    pub fn new(
        store: Arc<PodStore>,
        db: DatastoreHandle,
        supervisor: Arc<TaskSupervisor>,
        metrics: Arc<SideEffectMetrics>,
    ) -> Arc<Self> {
        Arc::new(Self {
            store,
            db,
            supervisor,
            metrics,
            wake: Arc::new(Notify::new()),
            lifecycle_router: std::sync::Mutex::new(None),
            local_node_name: std::sync::Mutex::new(None),
            remote_pod_delete_resignal_sink: std::sync::Mutex::new(None),
            reconciler_started: AtomicBool::new(false),
            start_called: AtomicBool::new(false),
        })
    }

    pub(super) fn start(self: &Arc<Self>) {
        self.start_called.store(true, Ordering::Release);
        // Reconciler starts lazily on first enqueue from async callers.
    }

    #[cfg(test)]
    pub(super) fn start_called(&self) -> bool {
        self.start_called.load(Ordering::Acquire)
    }

    pub(super) fn set_lifecycle_router_for_node(
        &self,
        router: Arc<PodLifecycleRouter>,
        local_node_name: String,
    ) {
        *self.lifecycle_router.lock().unwrap() = Some(router);
        *self.local_node_name.lock().unwrap() = Some(local_node_name);
    }

    pub(super) fn set_remote_pod_delete_resignal_sink(
        &self,
        sink: std::sync::Weak<dyn crate::controllers::gc::GcPodDeleteSink>,
    ) {
        *self.remote_pod_delete_resignal_sink.lock().unwrap() = Some(sink);
    }

    #[cfg(test)]
    fn set_remote_pod_delete_resignal_sink_for_tests(
        &self,
        sink: Arc<dyn crate::controllers::gc::GcPodDeleteSink>,
    ) {
        self.set_remote_pod_delete_resignal_sink(Arc::downgrade(&sink));
    }

    #[cfg(test)]
    pub(super) async fn enqueue_deferred_delete(
        self: &Arc<Self>,
        ns: String,
        name: String,
        uid: String,
        run_after: Duration,
    ) -> Result<()> {
        self.enqueue_deferred_delete_with_target_node(ns, name, uid, run_after, None)
            .await
    }

    pub(super) async fn enqueue_deferred_delete_with_target_node(
        self: &Arc<Self>,
        ns: String,
        name: String,
        uid: String,
        run_after: Duration,
        target_node: Option<String>,
    ) -> Result<()> {
        self.ensure_reconciler_started().await?;
        let delay_ms = run_after.as_millis().min(i64::MAX as u128) as i64;
        let pod = PodIdentity::new(&ns, &name, &uid);
        let payload = pod_delete_target_payload(target_node.as_deref());
        self.db
            .pod_workqueue_enqueue(PodWorkqueueKind::Pod, &pod, payload, 0, delay_ms, None)
            .await?;
        self.wake.notify_one();
        Ok(())
    }

    /// Enqueue a namespace termination attempt onto the durable
    /// pod_workqueue. The reconciler loop picks it up immediately
    /// (notify_one), runs `run_namespace_termination`, and on Err
    /// re-schedules with `MIN_DELAY_MS` backoff up to `MAX_ATTEMPTS`.
    /// Each retry is short-lived so the PodDeleteWorkqueue slot
    /// churns naturally between many concurrent namespace deletes.
    pub(super) async fn enqueue_namespace_termination(
        self: &Arc<Self>,
        namespace: String,
        uid: String,
    ) -> Result<()> {
        self.ensure_reconciler_started().await?;
        let pod = PodIdentity::new("", &namespace, &uid);
        self.db
            .pod_workqueue_enqueue(PodWorkqueueKind::Namespace, &pod, json!({}), 0, 0, None)
            .await?;
        self.wake.notify_one();
        Ok(())
    }

    pub(super) async fn enqueue_actor_deletes_for_terminating_namespace(
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

    async fn reconciler_loop(self: Arc<Self>) {
        let cancel = self.supervisor.root_cancellation_token();
        loop {
            let next_due = match self.db.pod_workqueue_peek_next_due().await {
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
                        _ = cancel.cancelled() => return,
                    }
                    continue;
                }
                Some(ts) => {
                    let now = now_ms();
                    if ts > now {
                        let sleep_for = Duration::from_millis((ts - now) as u64);
                        tokio::select! {
                            _ = self.supervisor.sleep("pod_workqueue_sleep_until_due", sleep_for) => {}
                            _ = self.wake.notified() => continue,
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
            let row = match self.db.pod_workqueue_claim_due(now_ms()).await {
                Ok(Some(row)) => row,
                Ok(None) => continue,
                Err(e) => {
                    tracing::error!(error = %e, "pod_workqueue: claim_due failed");
                    continue;
                }
            };

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
                tokio::select! {
                    _ = free.notified() => {}
                    _ = self.wake.notified() => {}
                    _ = cancel.cancelled() => return,
                }
            }

            let this = self.clone();
            let _ = self
                .supervisor
                .spawn_async(category, "pod_workqueue_retry", async move {
                    this.run_retry(row).await;
                })
                .await;
        }
    }

    async fn run_retry(self: Arc<Self>, mut row: PodWorkqueueEntry) {
        let target_node = row
            .payload
            .get(POD_DELETE_TARGET_NODE_PAYLOAD_KEY)
            .and_then(|value| value.as_str())
            .map(ToString::to_string);
        let retry_now_ms = now_ms();
        let result = match row.kind {
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
        };

        if result.is_ok() {
            let _ = self.db.pod_workqueue_complete(row.id).await;
            return;
        }

        let err = result.expect_err("error is present");
        if row.attempt_count >= MAX_ATTEMPTS {
            let _ = self
                .db
                .pod_workqueue_dead_letter(row.id, &format!("{err:#}"))
                .await;
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

        if let Err(enq_err) = self
            .db
            .pod_workqueue_record_failure(row, MIN_DELAY_MS, &format!("{err:#}"))
            .await
        {
            tracing::error!(error = %enq_err, "pod_workqueue: record_failure failed");
            return;
        }
        self.wake.notify_one();
    }

    #[cfg(test)]
    async fn run_pod_delete_full_with_target_node(
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
            now_ms(),
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
        let pod_before_delete = self.store.get(&ns, &name).await?;
        match pod_before_delete {
            Some(_resource) if uid.is_empty() => {
                anyhow::bail!(
                    "pod deferred delete missing UID for live Pod {}/{}; refusing name-only delete",
                    ns,
                    name
                );
            }
            Some(resource) if resource.uid == uid => {
                // HR#11 exception: a Pod never picked up by a kubelet
                // (spec.nodeName empty) has no lifecycle actor to finalize it,
                // so its row — and namespace — would linger forever. The
                // leader removes the row directly, atomically confirming no
                // kubelet has claimed it. Once a kubelet owns the Pod, only
                // the actor may remove the row (the actor-wake path below).
                let resource = if pod_target_node_from_pod_data(&resource.data).is_some() {
                    resource
                } else {
                    match self
                        .store
                        .delete_unscheduled_with_uid(&ns, &name, &uid)
                        .await?
                    {
                        UnscheduledPodDeleteOutcome::Removed => return Ok(()),
                        UnscheduledPodDeleteOutcome::FinalizersPending => {
                            anyhow::bail!(
                                "unscheduled pod {}/{} uid {} awaiting finalizer removal",
                                ns,
                                name,
                                uid
                            );
                        }
                        UnscheduledPodDeleteOutcome::DeferToActor => {
                            // A bind raced the atomic delete; re-read so the
                            // node-targeted actor path below routes to the
                            // now-assigned node.
                            match self.store.get(&ns, &name).await? {
                                Some(fresh) if fresh.uid == uid => fresh,
                                _ => return Ok(()),
                            }
                        }
                    }
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
                self.wake_local_actor_for_pod_delete(&ns, &name, &uid, &resource)
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
        sink.request_gc_pod_delete(ns, name, uid).await
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
        resource: &crate::datastore::Resource,
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
        router
            .route(LifecycleMessage::WatchModified {
                key: PodLifecycleKey::new(ns, name, uid),
                resource_version: Some(resource.resource_version),
                pod: std::sync::Arc::unwrap_or_clone(resource.data.clone()),
            })
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
        let outcome = crate::api::reconcile_namespace_termination_for_uid_with_outcome(
            self.db.as_ref(),
            &namespace,
            &uid,
            &self.metrics,
        )
        .await;
        match outcome {
            Ok(crate::api::NamespaceTerminationOutcome::Finalized) => Ok(()),
            Ok(crate::api::NamespaceTerminationOutcome::StillPending) => {
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

    async fn enqueue_actor_deletes_for_terminating_namespace_pods(
        &self,
        namespace: &str,
    ) -> Result<()> {
        let pods = self
            .store
            .list(Some(namespace), None, None, None, None)
            .await?;
        let mut enqueued_any = false;
        for resource in pods.items {
            let target_node = pod_target_node_from_pod_data(&resource.data);
            if resource
                .data
                .pointer("/metadata/deletionTimestamp")
                .and_then(|value| value.as_str())
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

            // Namespace termination can mark unscheduled Pods terminating; no
            // node-scoped Pod watcher will see those events. Queue the same
            // UID-bound actor wake used by Pod DELETE so final row removal
            // remains actor-owned.
            let pod = PodIdentity::new(namespace, &resource.name, &resource.uid);
            self.db
                .pod_workqueue_enqueue(
                    PodWorkqueueKind::Pod,
                    &pod,
                    pod_delete_target_payload(target_node.as_deref()),
                    0,
                    0,
                    None,
                )
                .await?;
            enqueued_any = true;
        }
        if enqueued_any {
            self.wake.notify_one();
        }
        Ok(())
    }

    fn bump_dead_letter_metric(&self, kind: PodWorkqueueKind) {
        match kind {
            PodWorkqueueKind::Pod => {
                self.metrics
                    .cascade_delete_failures_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            PodWorkqueueKind::Namespace => {
                self.metrics
                    .namespace_delete_failures_total
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn pod_target_node_from_pod_data(pod: &Value) -> Option<String> {
    pod.pointer("/spec/nodeName")
        .and_then(|node| node.as_str())
        .filter(|node| !node.trim().is_empty())
        .map(ToString::to_string)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kubelet::pod_lifecycle_core::action::PodAction;
    use crate::kubelet::pod_lifecycle_core::message::{LifecycleMessage, PodLifecycleWorkKind};
    use crate::kubelet::pod_lifecycle_router::LifecycleReplyHandle;
    use crate::kubelet::pod_lifecycle_router::executor::{ExecutorError, PodWorkExecutor};

    #[derive(Default)]
    struct RecordingGcPodDeleteSink {
        calls: tokio::sync::Mutex<Vec<(String, String, String)>>,
    }

    impl RecordingGcPodDeleteSink {
        async fn calls(&self) -> Vec<(String, String, String)> {
            self.calls.lock().await.clone()
        }
    }

    #[async_trait::async_trait]
    impl crate::controllers::gc::GcPodDeleteSink for RecordingGcPodDeleteSink {
        async fn request_gc_pod_delete(
            &self,
            namespace: &str,
            name: &str,
            uid: &str,
        ) -> Result<()> {
            self.calls.lock().await.push((
                namespace.to_string(),
                name.to_string(),
                uid.to_string(),
            ));
            Ok(())
        }
    }

    struct WakeRecordingExecutor {
        stop_seen: tokio::sync::Notify,
    }

    #[async_trait::async_trait]
    impl PodWorkExecutor for WakeRecordingExecutor {
        async fn dispatch(
            &self,
            action: PodAction,
            reply_to: LifecycleReplyHandle,
        ) -> Result<(), ExecutorError> {
            match action {
                PodAction::ReconcileCriLeftovers {
                    key, operation_id, ..
                } => {
                    let _ = reply_to
                        .route(LifecycleMessage::PodWorkCompleted {
                            key,
                            operation_id,
                            kind: PodLifecycleWorkKind::ReconcileCriLeftovers,
                            sandbox_id: None,
                        })
                        .await;
                }
                PodAction::StopPod { key, .. } if key.uid == "uid-old" => {
                    self.stop_seen.notify_waiters();
                }
                _ => {}
            }
            Ok(())
        }
    }

    async fn test_workqueue() -> (Arc<PodWorkqueue>, DatastoreHandle) {
        let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
        let store = Arc::new(PodStore::new(db.clone()));
        let supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let metrics = SideEffectMetrics::new();
        let workqueue = PodWorkqueue::new(store, db.clone(), supervisor, metrics);
        *workqueue.local_node_name.lock().unwrap() = Some("node-a".to_string());
        (workqueue, db)
    }

    fn test_router(
        supervisor: &Arc<crate::task_supervisor::TaskSupervisor>,
        executor: Arc<dyn PodWorkExecutor>,
    ) -> Arc<crate::kubelet::pod_lifecycle_router::PodLifecycleRouter> {
        let registry = Arc::new(
            crate::kubelet::pod_lifecycle_actor::registry::PodLifecycleRegistry::new(
                supervisor.clone(),
                crate::kubelet::pod_lifecycle_actor::config::PodLifecycleConcurrencyConfig::production_default(),
                Arc::new(std::sync::Mutex::new(executor.clone())),
            ),
        );
        Arc::new(
            crate::kubelet::pod_lifecycle_router::PodLifecycleRouter::new_actor_with_executor(
                registry, executor,
            ),
        )
    }

    fn pod_with_uid_on_node(
        name: &str,
        uid: &str,
        deleting: bool,
        node_name: &str,
    ) -> serde_json::Value {
        let mut pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": name,
                "uid": uid
            },
            "spec": {
                "nodeName": node_name,
                "containers": [{"name": "app", "image": "busybox"}]
            }
        });
        if deleting {
            pod["metadata"]["deletionTimestamp"] = json!("2026-05-13T00:00:00Z");
            pod["metadata"]["deletionGracePeriodSeconds"] = json!(0);
        }
        pod
    }

    fn pod_with_uid(name: &str, uid: &str, deleting: bool) -> serde_json::Value {
        pod_with_uid_on_node(name, uid, deleting, "node-a")
    }

    fn unscheduled_pod_with_uid(name: &str, uid: &str, deleting: bool) -> serde_json::Value {
        let mut pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": name,
                "uid": uid
            },
            "spec": {
                "containers": [{"name": "app", "image": "busybox"}]
            }
        });
        if deleting {
            pod["metadata"]["deletionTimestamp"] = json!("2026-05-13T00:00:00Z");
            pod["metadata"]["deletionGracePeriodSeconds"] = json!(0);
        }
        pod
    }

    #[tokio::test]
    async fn deferred_pod_delete_removes_unscheduled_terminating_pod_directly() {
        // HR#11 exception: an unscheduled Pod (never bound to a node) has no
        // kubelet actor; the leader removes the row directly.
        let (workqueue, db) = test_workqueue().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "unscheduled",
            unscheduled_pod_with_uid("unscheduled", "uid-unsched", true),
        )
        .await
        .unwrap();

        workqueue
            .run_pod_delete_full_with_target_node(
                "default".to_string(),
                "unscheduled".to_string(),
                "uid-unsched".to_string(),
                None,
            )
            .await
            .expect("unscheduled terminating Pod delete must succeed without an actor");

        assert!(
            db.get_resource("v1", "Pod", Some("default"), "unscheduled")
                .await
                .unwrap()
                .is_none(),
            "unscheduled terminating Pod row must be removed so its namespace can finalize"
        );
    }

    #[tokio::test]
    async fn deferred_pod_delete_waits_for_kubelet_cleanup_while_uid_still_exists() {
        let (workqueue, db) = test_workqueue().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "same-name",
            pod_with_uid("same-name", "uid-old", true),
        )
        .await
        .unwrap();

        let err = workqueue
            .run_pod_delete_full_with_target_node(
                "default".to_string(),
                "same-name".to_string(),
                "uid-old".to_string(),
                Some("node-a".to_string()),
            )
            .await
            .expect_err("deferred workqueue must retry while kubelet-owned Pod cleanup is pending");

        assert!(
            err.to_string().contains("waiting for kubelet cleanup"),
            "unexpected error: {err:#}"
        );
        let live = db
            .get_resource("v1", "Pod", Some("default"), "same-name")
            .await
            .unwrap()
            .expect(
                "terminating Pod must remain until actor finalization confirms runtime cleanup",
            );
        assert_eq!(live.uid, "uid-old");
    }

    #[tokio::test]
    async fn deferred_pod_delete_wakes_local_actor_for_live_uid() {
        let (workqueue, db) = test_workqueue().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "same-name",
            pod_with_uid_on_node("same-name", "uid-old", true, "node-a"),
        )
        .await
        .unwrap();

        let supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let executor = Arc::new(WakeRecordingExecutor {
            stop_seen: tokio::sync::Notify::new(),
        });
        let router = test_router(&supervisor, executor.clone());
        workqueue.set_lifecycle_router_for_node(router, "node-a".to_string());

        let err = workqueue
            .run_pod_delete_full_with_target_node(
                "default".to_string(),
                "same-name".to_string(),
                "uid-old".to_string(),
                Some("node-a".to_string()),
            )
            .await
            .expect_err("same-UID live Pod should keep durable reminder until actor deletes row");
        assert!(
            err.to_string().contains("waiting for kubelet cleanup"),
            "unexpected error: {err:#}"
        );

        tokio::time::timeout(Duration::from_secs(1), executor.stop_seen.notified())
            .await
            .expect("deferred delete should wake the local lifecycle actor");
    }

    #[tokio::test]
    async fn terminating_pod_is_finalized_even_when_live_watch_event_is_dropped() {
        let (workqueue, db) = test_workqueue().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "watch-dropped",
            pod_with_uid_on_node("watch-dropped", "uid-old", true, "node-a"),
        )
        .await
        .unwrap();

        let supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let executor = Arc::new(WakeRecordingExecutor {
            stop_seen: tokio::sync::Notify::new(),
        });
        let router = test_router(&supervisor, executor.clone());
        workqueue.set_lifecycle_router_for_node(router, "node-a".to_string());

        workqueue
            .run_pod_delete_full_with_target_node(
                "default".to_string(),
                "watch-dropped".to_string(),
                "uid-old".to_string(),
                Some("node-a".to_string()),
            )
            .await
            .expect_err("durable reminder must retry while actor-owned cleanup is pending");
        tokio::time::timeout(Duration::from_secs(1), executor.stop_seen.notified())
            .await
            .expect("durable workqueue reminder should wake the actor without a live watch event");

        workqueue
            .store
            .delete_with_uid("default", "watch-dropped", "uid-old")
            .await
            .expect("actor-owned UID finalization should remove the row");
        workqueue
            .run_pod_delete_full_with_target_node(
                "default".to_string(),
                "watch-dropped".to_string(),
                "uid-old".to_string(),
                Some("node-a".to_string()),
            )
            .await
            .expect("once actor finalization removed the row, the reminder completes");
        assert!(
            db.get_resource("v1", "Pod", Some("default"), "watch-dropped")
                .await
                .unwrap()
                .is_none(),
            "Pod row must be gone only after actor-owned UID finalization"
        );
    }

    #[tokio::test]
    async fn deferred_pod_delete_is_uid_bound_and_preserves_replacement_pod() {
        let (workqueue, db) = test_workqueue().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "same-name",
            pod_with_uid("same-name", "uid-new", false),
        )
        .await
        .unwrap();

        workqueue
            .run_pod_delete_full_with_target_node(
                "default".to_string(),
                "same-name".to_string(),
                "uid-old".to_string(),
                Some("node-a".to_string()),
            )
            .await
            .expect(
                "stale deferred delete for old UID should complete without touching replacement",
            );

        let live = db
            .get_resource("v1", "Pod", Some("default"), "same-name")
            .await
            .unwrap()
            .expect("replacement Pod must not be deleted by stale deferred work");
        assert_eq!(live.uid, "uid-new");
    }

    #[tokio::test]
    async fn deferred_pod_delete_waits_for_remote_actor_owned_finalization() {
        // Remote-targeted deferred delete must not hard-delete the Pod row from
        // the leader. The worker's Pod watch/actor owns finalization; the
        // leader-side workqueue only keeps a UID-bound reminder alive until
        // that actor-owned finalization removes the row.
        let (workqueue, db) = test_workqueue().await;

        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "remote-pod",
            pod_with_uid_on_node("remote-pod", "uid-old", true, "node-b"),
        )
        .await
        .unwrap();
        let supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let executor = Arc::new(WakeRecordingExecutor {
            stop_seen: tokio::sync::Notify::new(),
        });
        let router = test_router(&supervisor, executor.clone());
        workqueue.set_lifecycle_router_for_node(router, "node-a".to_string());

        let err = workqueue
            .run_pod_delete_full_with_target_node(
                "default".to_string(),
                "remote-pod".to_string(),
                "uid-old".to_string(),
                Some("node-b".to_string()),
            )
            .await
            .expect_err("remote-targeted delete must keep retrying until the row is removed");
        assert!(
            err.to_string()
                .contains("awaiting actor-owned finalization"),
            "unexpected error: {err:#}"
        );

        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(100),
                executor.stop_seen.notified(),
            )
            .await
            .is_err(),
            "remote-targeted delete should not wake local actor"
        );
    }

    #[tokio::test]
    async fn remote_pod_workqueue_resignals_gc_delete_on_retry() {
        let (workqueue, db) = test_workqueue().await;

        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "remote-pod",
            pod_with_uid_on_node("remote-pod", "uid-old", true, "node-b"),
        )
        .await
        .unwrap();

        let sink = Arc::new(RecordingGcPodDeleteSink::default());
        workqueue.set_remote_pod_delete_resignal_sink_for_tests(sink.clone());

        let err = workqueue
            .run_pod_delete_full_with_target_node(
                "default".to_string(),
                "remote-pod".to_string(),
                "uid-old".to_string(),
                Some("node-b".to_string()),
            )
            .await
            .expect_err("remote Pod should keep retrying until actor removes row");

        assert!(
            err.to_string()
                .contains("awaiting actor-owned finalization"),
            "unexpected error: {err:#}"
        );
        assert_eq!(
            sink.calls().await,
            vec![(
                "default".to_string(),
                "remote-pod".to_string(),
                "uid-old".to_string()
            )],
            "remote retry must re-signal the UID-bound GC delete path"
        );
    }

    #[tokio::test]
    async fn remote_pod_resignal_throttled_to_every_30_seconds() {
        let (workqueue, db) = test_workqueue().await;

        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "remote-pod",
            pod_with_uid_on_node("remote-pod", "uid-old", true, "node-b"),
        )
        .await
        .unwrap();

        let sink = Arc::new(RecordingGcPodDeleteSink::default());
        workqueue.set_remote_pod_delete_resignal_sink_for_tests(sink.clone());
        let mut payload = pod_delete_target_payload(Some("node-b"));

        workqueue
            .run_pod_delete_full_with_target_node_and_payload(
                "default".to_string(),
                "remote-pod".to_string(),
                "uid-old".to_string(),
                Some("node-b".to_string()),
                &mut payload,
                100_000,
            )
            .await
            .expect_err("remote Pod should retry after re-signal");
        assert_eq!(sink.calls().await.len(), 1);

        workqueue
            .run_pod_delete_full_with_target_node_and_payload(
                "default".to_string(),
                "remote-pod".to_string(),
                "uid-old".to_string(),
                Some("node-b".to_string()),
                &mut payload,
                110_000,
            )
            .await
            .expect_err("remote Pod should retry while re-signal is throttled");
        assert_eq!(
            sink.calls().await.len(),
            1,
            "second retry inside throttle window must not re-signal"
        );

        workqueue
            .run_pod_delete_full_with_target_node_and_payload(
                "default".to_string(),
                "remote-pod".to_string(),
                "uid-old".to_string(),
                Some("node-b".to_string()),
                &mut payload,
                130_000,
            )
            .await
            .expect_err("remote Pod should retry after throttle window re-signal");
        assert_eq!(
            sink.calls().await.len(),
            2,
            "retry at the 30s boundary should re-signal again"
        );
    }

    #[tokio::test]
    async fn remote_pod_without_deletion_timestamp_is_marked_on_first_retry() {
        let (workqueue, db) = test_workqueue().await;

        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "remote-pod",
            pod_with_uid_on_node("remote-pod", "uid-old", false, "node-b"),
        )
        .await
        .unwrap();

        let sink = Arc::new(RecordingGcPodDeleteSink::default());
        workqueue.set_remote_pod_delete_resignal_sink_for_tests(sink.clone());

        workqueue
            .run_pod_delete_full_with_target_node(
                "default".to_string(),
                "remote-pod".to_string(),
                "uid-old".to_string(),
                Some("node-b".to_string()),
            )
            .await
            .expect_err("remote Pod should retry until target actor finalizes it");

        assert_eq!(
            sink.calls().await,
            vec![(
                "default".to_string(),
                "remote-pod".to_string(),
                "uid-old".to_string()
            )],
            "first remote retry must request the UID-bound delete mark even before deletionTimestamp is present"
        );
    }

    #[tokio::test]
    async fn remote_pod_resignal_is_uid_bound_and_self_extinguishes_when_row_removed() {
        let (workqueue, db) = test_workqueue().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "remote-pod",
            pod_with_uid_on_node("remote-pod", "uid-new", true, "node-b"),
        )
        .await
        .unwrap();

        let sink = Arc::new(RecordingGcPodDeleteSink::default());
        workqueue.set_remote_pod_delete_resignal_sink_for_tests(sink.clone());

        workqueue
            .run_pod_delete_full_with_target_node(
                "default".to_string(),
                "remote-pod".to_string(),
                "uid-old".to_string(),
                Some("node-b".to_string()),
            )
            .await
            .expect("stale UID reminder must complete without touching replacement");
        assert!(
            sink.calls().await.is_empty(),
            "stale UID must not re-signal deletion for a replacement Pod"
        );

        db.delete_resource("v1", "Pod", Some("default"), "remote-pod")
            .await
            .unwrap();
        workqueue
            .run_pod_delete_full_with_target_node(
                "default".to_string(),
                "remote-pod".to_string(),
                "uid-old".to_string(),
                Some("node-b".to_string()),
            )
            .await
            .expect("missing row means actor-owned finalization completed");
        assert!(
            sink.calls().await.is_empty(),
            "removed row should self-extinguish without re-signaling"
        );
    }

    #[tokio::test]
    async fn deferred_pod_delete_for_remote_pod_completes_once_row_removed() {
        // Durability backstop: the remote deferred delete keeps the workqueue
        // entry alive (Err -> retry) while the Pod row still exists, and
        // completes (Ok) only once the actor-owned path has removed the row.
        let (workqueue, db) = test_workqueue().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "remote-pod",
            pod_with_uid_on_node("remote-pod", "uid-old", true, "node-b"),
        )
        .await
        .unwrap();

        // Row still present: must bail to retry (not complete).
        workqueue
            .run_pod_delete_full_with_target_node(
                "default".to_string(),
                "remote-pod".to_string(),
                "uid-old".to_string(),
                Some("node-b".to_string()),
            )
            .await
            .expect_err("must retry while the remote Pod row still exists");

        // Simulate the row finally being removed by the remote actor. The next
        // retry must complete cleanly.
        db.delete_resource("v1", "Pod", Some("default"), "remote-pod")
            .await
            .unwrap();

        workqueue
            .run_pod_delete_full_with_target_node(
                "default".to_string(),
                "remote-pod".to_string(),
                "uid-old".to_string(),
                Some("node-b".to_string()),
            )
            .await
            .expect("once the remote Pod row is gone the deferred delete completes");
    }

    #[tokio::test]
    async fn deferred_pod_delete_waits_when_local_node_unknown_with_target() {
        // When local node is unknown but a target is specified, the deferred
        // delete still must not fall back to a leader-side hard-delete. It
        // keeps retrying until the target actor removes the row.
        let (workqueue, db) = test_workqueue().await;
        *workqueue.local_node_name.lock().unwrap() = None;

        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "orphaned-pod",
            pod_with_uid_on_node("orphaned-pod", "uid-old", true, "node-a"),
        )
        .await
        .unwrap();

        let err = workqueue
            .run_pod_delete_full_with_target_node(
                "default".to_string(),
                "orphaned-pod".to_string(),
                "uid-old".to_string(),
                Some("node-a".to_string()),
            )
            .await
            .expect_err("deferred delete with unknown local node must retry until the row is gone");
        assert!(
            err.to_string()
                .contains("awaiting actor-owned finalization"),
            "unexpected error: {err:#}"
        );
    }

    #[tokio::test]
    async fn deferred_pod_delete_skips_if_local_node_unknown_when_pod_has_node_name() {
        let (workqueue, db) = test_workqueue().await;
        *workqueue.local_node_name.lock().unwrap() = None;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "orphaned-pod",
            pod_with_uid_on_node("orphaned-pod", "uid-old", true, "node-a"),
        )
        .await
        .unwrap();

        let err = workqueue
            .run_pod_delete_full_with_target_node(
                "default".to_string(),
                "orphaned-pod".to_string(),
                "uid-old".to_string(),
                None,
            )
            .await
            .expect_err("deferred delete should not run without local node identity");
        assert!(
            err.to_string().contains("local"),
            "unexpected error: {err:#}"
        );
    }

    #[tokio::test]
    async fn namespace_termination_enqueues_all_terminating_pods() {
        // Regression: namespace termination must enqueue workqueue entries
        // for ALL terminating pods, including those on remote nodes. Remote
        // entries are actor-owned reminders, not leader hard-deletes.
        let (workqueue, db) = test_workqueue().await;
        let supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let executor = Arc::new(WakeRecordingExecutor {
            stop_seen: tokio::sync::Notify::new(),
        });
        let router = test_router(&supervisor, executor.clone());
        workqueue.set_lifecycle_router_for_node(router, "node-a".to_string());

        db.create_resource(
            "v1",
            "Pod",
            Some("terminating-ns"),
            "local-pod",
            pod_with_uid_on_node("local-pod", "local-uid", true, "node-a"),
        )
        .await
        .unwrap();
        db.create_resource(
            "v1",
            "Pod",
            Some("terminating-ns"),
            "remote-pod",
            pod_with_uid_on_node("remote-pod", "remote-uid", true, "node-b"),
        )
        .await
        .unwrap();
        workqueue
            .enqueue_actor_deletes_for_terminating_namespace("terminating-ns")
            .await
            .unwrap();

        let mut claimed_rows = Vec::new();
        loop {
            let row = db.pod_workqueue_claim_due(i64::MAX).await.unwrap();
            if let Some(row) = row {
                claimed_rows.push(row);
                continue;
            }
            break;
        }

        assert_eq!(
            claimed_rows.len(),
            2,
            "namespace termination should enqueue workqueue entries for both local and remote pods"
        );
        let names: std::collections::HashSet<&str> =
            claimed_rows.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains("local-pod"));
        assert!(names.contains("remote-pod"));
    }

    #[tokio::test]
    async fn enqueue_deferred_delete_records_uid_bound_retry_row() {
        let (workqueue, db) = test_workqueue().await;
        let before = now_ms();

        workqueue
            .enqueue_deferred_delete(
                "default".to_string(),
                "same-name".to_string(),
                "uid-old".to_string(),
                Duration::from_millis(50),
            )
            .await
            .unwrap();

        let due = db
            .pod_workqueue_peek_next_due()
            .await
            .unwrap()
            .expect("deferred delete must be recorded in the durable workqueue");
        assert!(
            due >= before + 40,
            "deferred delete should not be due before the requested delay"
        );
        let row = db.pod_workqueue_claim_due(due).await.unwrap().unwrap();
        assert_eq!(row.kind, PodWorkqueueKind::Pod);
        assert_eq!(row.namespace, "default");
        assert_eq!(row.name, "same-name");
        assert_eq!(row.uid, "uid-old");
        assert!(
            row.payload
                .get(POD_DELETE_TARGET_NODE_PAYLOAD_KEY)
                .is_none()
                || row
                    .payload
                    .get(POD_DELETE_TARGET_NODE_PAYLOAD_KEY)
                    .is_some_and(|value| value.is_null()),
            "default deferred delete should not set a target node"
        );
    }

    #[tokio::test]
    async fn enqueue_deferred_delete_with_target_node_records_target_in_payload() {
        let (workqueue, db) = test_workqueue().await;
        let before = now_ms();

        workqueue
            .enqueue_deferred_delete_with_target_node(
                "default".to_string(),
                "same-name".to_string(),
                "uid-old".to_string(),
                Duration::from_millis(50),
                Some("node-a".to_string()),
            )
            .await
            .unwrap();

        let due = db
            .pod_workqueue_peek_next_due()
            .await
            .unwrap()
            .expect("deferred delete must be recorded in the durable workqueue");
        assert!(
            due >= before + 40,
            "deferred delete should not be due before the requested delay"
        );
        let row = db.pod_workqueue_claim_due(due).await.unwrap().unwrap();
        assert_eq!(row.kind, PodWorkqueueKind::Pod);
        assert_eq!(row.namespace, "default");
        assert_eq!(row.name, "same-name");
        assert_eq!(row.uid, "uid-old");
        assert_eq!(
            row.payload
                .get(POD_DELETE_TARGET_NODE_PAYLOAD_KEY)
                .and_then(|value| value.as_str()),
            Some("node-a")
        );
    }

    #[tokio::test]
    async fn deferred_pod_delete_with_remote_target_retries_without_local_actor_or_outbox() {
        // Regression test: a deferred delete for a pod on a remote node
        // must not be silently dropped, must not wake the local actor, and
        // must not hard-delete through a leader-side outbox.
        let (workqueue, db) = test_workqueue().await;

        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "remote-pod",
            pod_with_uid_on_node("remote-pod", "uid-old", true, "node-b"),
        )
        .await
        .unwrap();

        let supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let executor = Arc::new(WakeRecordingExecutor {
            stop_seen: tokio::sync::Notify::new(),
        });
        let router = test_router(&supervisor, executor.clone());
        workqueue.set_lifecycle_router_for_node(router, "node-a".to_string());

        db.pod_workqueue_enqueue(
            crate::datastore::PodWorkqueueKind::Pod,
            &crate::pod_identity::PodIdentity::new("default", "remote-pod", "uid-old"),
            json!({"target_node": "node-b"}),
            0,
            0,
            None,
        )
        .await
        .unwrap();

        let due = db.pod_workqueue_peek_next_due().await.unwrap().unwrap();
        let row = db.pod_workqueue_claim_due(due).await.unwrap().unwrap();
        workqueue.clone().run_retry(row).await;

        // The local actor must NOT be woken for a remote pod.
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(100),
                executor.stop_seen.notified(),
            )
            .await
            .is_err(),
            "remote-targeted delete should not wake local actor"
        );

        // The pod row still exists, so the workqueue entry must be RE-ENQUEUED
        // for retry rather than completed. The target worker's actor is the
        // only terminal delete owner for a picked-up Pod.
        assert!(
            db.pod_workqueue_claim_due(i64::MAX)
                .await
                .unwrap()
                .is_some(),
            "remote-targeted deferred delete must retry until the cluster row is removed"
        );
    }

    #[tokio::test]
    async fn worker_finalizes_pod_from_durable_intent_without_restart() {
        let (workqueue, db) = test_workqueue().await;
        *workqueue.local_node_name.lock().unwrap() = Some("node-b".to_string());
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "worker-pod",
            pod_with_uid_on_node("worker-pod", "uid-old", true, "node-b"),
        )
        .await
        .unwrap();

        let supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let executor = Arc::new(WakeRecordingExecutor {
            stop_seen: tokio::sync::Notify::new(),
        });
        let router = test_router(&supervisor, executor.clone());
        workqueue.set_lifecycle_router_for_node(router, "node-b".to_string());

        db.pod_workqueue_enqueue(
            crate::datastore::PodWorkqueueKind::Pod,
            &crate::pod_identity::PodIdentity::new("default", "worker-pod", "uid-old"),
            json!({"target_node": "node-b"}),
            0,
            0,
            None,
        )
        .await
        .unwrap();

        let due = db.pod_workqueue_peek_next_due().await.unwrap().unwrap();
        let row = db.pod_workqueue_claim_due(due).await.unwrap().unwrap();
        workqueue
            .run_pod_delete_full_with_target_node(
                row.namespace.clone(),
                row.name.clone(),
                row.uid.clone(),
                row.payload
                    .get(POD_DELETE_TARGET_NODE_PAYLOAD_KEY)
                    .and_then(|value| value.as_str())
                    .map(ToString::to_string),
            )
            .await
            .expect_err("durable intent should retry until actor-owned cleanup removes the row");
        tokio::time::timeout(Duration::from_secs(1), executor.stop_seen.notified())
            .await
            .expect("running worker must consume durable intent and wake its actor");

        workqueue
            .store
            .delete_with_uid("default", "worker-pod", "uid-old")
            .await
            .expect("worker actor-owned finalization should remove the row");
        workqueue
            .run_pod_delete_full_with_target_node(
                "default".to_string(),
                "worker-pod".to_string(),
                "uid-old".to_string(),
                Some("node-b".to_string()),
            )
            .await
            .expect("durable intent must self-extinguish once actor finalization removed the row");
    }

    #[tokio::test]
    async fn enqueue_deferred_delete_does_not_skip_remote_target() {
        // Regression test: enqueue_deferred_delete_with_target_node must
        // NOT silently drop entries for remote-targeted pods.
        let (workqueue, db) = test_workqueue().await;

        workqueue
            .enqueue_deferred_delete_with_target_node(
                "default".to_string(),
                "remote-pod".to_string(),
                "uid-old".to_string(),
                Duration::from_millis(50),
                Some("node-b".to_string()),
            )
            .await
            .unwrap();

        let due = db.pod_workqueue_peek_next_due().await.unwrap();
        assert!(
            due.is_some(),
            "remote-targeted deferred delete must be enqueued in the workqueue"
        );
        let row = db.pod_workqueue_claim_due(i64::MAX).await.unwrap().unwrap();
        assert_eq!(row.namespace, "default");
        assert_eq!(row.name, "remote-pod");
        assert_eq!(row.uid, "uid-old");
        assert_eq!(
            row.payload
                .get(POD_DELETE_TARGET_NODE_PAYLOAD_KEY)
                .and_then(|value| value.as_str()),
            Some("node-b")
        );
    }

    #[tokio::test]
    async fn namespace_termination_enqueues_uid_bound_delete_for_unscheduled_pod() {
        let (workqueue, db) = test_workqueue().await;
        db.create_namespace(
            "terminating-ns",
            json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": "terminating-ns", "uid": "ns-uid"},
                "spec": {"finalizers": ["kubernetes"]},
                "status": {"phase": "Active"}
            }),
        )
        .await
        .unwrap();
        let ns = db
            .get_namespace("terminating-ns")
            .await
            .unwrap()
            .expect("namespace exists");
        let mut ns_data = std::sync::Arc::unwrap_or_clone(ns.data);
        ns_data["metadata"]["deletionTimestamp"] = json!("2026-05-16T00:00:00Z");
        ns_data["status"]["phase"] = json!("Terminating");
        db.update_namespace("terminating-ns", ns_data, ns.resource_version)
            .await
            .unwrap();
        db.create_resource(
            "v1",
            "Pod",
            Some("terminating-ns"),
            "unscheduled",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "terminating-ns",
                    "name": "unscheduled",
                    "uid": "pod-uid"
                },
                "spec": {
                    "containers": [{"name": "pause", "image": "registry.k8s.io/pause:3.10.1"}]
                },
                "status": {"phase": "Pending"}
            }),
        )
        .await
        .unwrap();

        let err = workqueue
            .run_namespace_termination("terminating-ns".to_string(), "ns-uid".to_string())
            .await
            .expect_err("namespace should stay pending until actor-owned pod deletion finalizes");
        assert!(
            err.to_string().contains("still terminating"),
            "unexpected namespace retry error: {err:#}"
        );

        let row = db
            .pod_workqueue_claim_due(now_ms() + 1_000)
            .await
            .unwrap()
            .expect("namespace termination must enqueue actor-owned Pod delete work");
        assert_eq!(row.kind, PodWorkqueueKind::Pod);
        assert_eq!(row.namespace, "terminating-ns");
        assert_eq!(row.name, "unscheduled");
        assert_eq!(row.uid, "pod-uid");

        let pod = db
            .get_resource("v1", "Pod", Some("terminating-ns"), "unscheduled")
            .await
            .unwrap()
            .expect("Pod row must remain until actor finalization");
        assert!(
            pod.data
                .pointer("/metadata/deletionTimestamp")
                .and_then(|value| value.as_str())
                .is_some(),
            "namespace termination should only mark the Pod terminating"
        );
    }

    #[tokio::test]
    async fn remote_pod_with_finalizers_is_not_hard_deleted_by_leader_workqueue() {
        // Regression: namespace deletion should NOT hard-delete a remote pod
        // that has remaining finalizers. Remote picked-up Pods remain
        // actor-owned regardless of finalizer state.
        //
        // Upstream test: [sig-api-machinery] OrderedNamespaceDeletion
        // "namespace deletion should delete pod first" — the test creates a
        // pod with a custom finalizer, deletes the namespace, and expects the
        // pod to still exist (with deletionTimestamp) while the ConfigMap
        // does NOT have deletionTimestamp.
        let (workqueue, db) = test_workqueue().await;

        let mut remote_pod = pod_with_uid_on_node("finalizer-pod", "uid-f", true, "node-b");
        remote_pod["metadata"]["finalizers"] = json!(["test-finalizer"]);
        db.create_resource(
            "v1",
            "Pod",
            Some("terminating-ns"),
            "finalizer-pod",
            remote_pod,
        )
        .await
        .unwrap();

        let supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let executor = Arc::new(WakeRecordingExecutor {
            stop_seen: tokio::sync::Notify::new(),
        });
        let router = test_router(&supervisor, executor.clone());
        workqueue.set_lifecycle_router_for_node(router, "node-a".to_string());

        // Enqueue as if namespace termination did it.
        db.pod_workqueue_enqueue(
            crate::datastore::PodWorkqueueKind::Pod,
            &crate::pod_identity::PodIdentity::new("terminating-ns", "finalizer-pod", "uid-f"),
            json!({"target_node": "node-b"}),
            0,
            0,
            None,
        )
        .await
        .unwrap();

        let due = db.pod_workqueue_peek_next_due().await.unwrap().unwrap();
        let row = db.pod_workqueue_claim_due(due).await.unwrap().unwrap();
        workqueue.clone().run_retry(row).await;

        // The pod must STILL exist in the datastore — remote leader workqueue
        // retries are actor wakeup/reminder state only.
        let pod = db
            .get_resource("v1", "Pod", Some("terminating-ns"), "finalizer-pod")
            .await
            .unwrap();
        assert!(
            pod.is_some(),
            "remote pod with finalizers must NOT be hard-deleted by leader workqueue"
        );
        let pod_data = pod.unwrap();
        assert!(
            pod_data.data.pointer("/metadata/finalizers").is_some(),
            "pod must still have its finalizers"
        );

        // The workqueue entry must be re-enqueued for retry (not completed),
        // because the pod still has finalizers.
        let retry_row = db.pod_workqueue_claim_due(i64::MAX).await.unwrap();
        assert!(
            retry_row.is_some(),
            "remote pod with finalizers must be re-enqueued for retry"
        );
    }

    /// The reconciler loop must respond to root cancellation at every wait
    /// point — bare `wake.notified().await`, sleep-until-due, and
    /// category-free-wait. Without cancellation branches, shutdown can
    /// be delayed by the sleep duration.
    #[tokio::test]
    async fn reconciler_exits_on_root_cancellation() {
        let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
        let store = Arc::new(PodStore::new(db.clone()));
        let supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let cancel = supervisor.root_cancellation_token();
        let metrics = SideEffectMetrics::new();
        let workqueue = PodWorkqueue::new(store, db.clone(), supervisor.clone(), metrics);

        // Enqueue a deferred delete to trigger reconciler start.
        // The reconciler will fail to process it (no lifecycle_router set)
        // and loop with error backoff, which is sufficient for testing
        // cancellation responsiveness.
        workqueue
            .enqueue_deferred_delete(
                "default".to_string(),
                "test-pod".to_string(),
                "uid-1".to_string(),
                Duration::from_millis(5000),
            )
            .await
            .unwrap();

        // Wait for the reconciler task to appear.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if supervisor
                .active_tasks(Some(TaskCategory::Background))
                .iter()
                .any(|t| t.name == "pod_workqueue_reconciler")
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "reconciler task did not appear"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Cancel root and verify the reconciler exits quickly.
        cancel.cancel();
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            if supervisor
                .active_tasks(Some(TaskCategory::Background))
                .iter()
                .all(|t| t.name != "pod_workqueue_reconciler")
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "reconciler did not exit within 3s of cancellation"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}
