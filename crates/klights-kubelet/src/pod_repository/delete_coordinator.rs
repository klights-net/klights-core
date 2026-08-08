use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use klights_pod_api::{
    PodActorFinalizeRequest, PodDeleteMarkOutcome, PodDeleteMarkRequest, PodDeleteOrchestration,
    PodMarkedRetryRequest, PodRepositoryError, PodRepositoryFuture,
};
use serde_json::Value;

use crate::pod_repository::delete_deadline::{
    PodDeleteDeadlineDisposition, has_nonempty_pod_deletion_timestamp, plan_pod_delete_deadline,
};
use klights_cluster_core::{Resource, ResourcePreconditions};
use klights_reconcile_api::ReconcileFailureMetrics;
use klights_supervisor::TaskSupervisor;

use crate::pod_repository::store::PodStore;
use crate::pod_repository::workqueue::PodWorkqueue;

const MAX_DELETE_CONFLICT_RETRIES: u32 = 8;

#[async_trait]
trait PodDeleteStorePort: Send + Sync {
    async fn get(&self, ns: &str, name: &str) -> Result<Option<Resource>>;

    async fn mark_deleting_at_resource_version(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        body: Value,
        expected_rv: i64,
    ) -> Result<Resource>;
}

#[async_trait]
impl PodDeleteStorePort for PodStore {
    async fn get(&self, ns: &str, name: &str) -> Result<Option<Resource>> {
        PodStore::get(self, ns, name).await
    }

    async fn mark_deleting_at_resource_version(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        body: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        PodStore::mark_deleting_at_resource_version(self, ns, name, uid, body, expected_rv).await
    }
}

#[async_trait]
trait PodDeleteQueuePort: Send + Sync {
    async fn enqueue_deferred_delete_with_target_node(
        &self,
        ns: String,
        name: String,
        uid: String,
        run_after: Duration,
        target_node: Option<String>,
    ) -> Result<()>;

    async fn ensure_deferred_delete_with_target_node(
        &self,
        ns: String,
        name: String,
        uid: String,
        run_after: Duration,
        target_node: Option<String>,
    ) -> Result<bool>;
}

struct WorkqueuePodDeleteQueue {
    workqueue: Arc<PodWorkqueue>,
}

#[async_trait]
impl PodDeleteQueuePort for WorkqueuePodDeleteQueue {
    async fn enqueue_deferred_delete_with_target_node(
        &self,
        ns: String,
        name: String,
        uid: String,
        run_after: Duration,
        target_node: Option<String>,
    ) -> Result<()> {
        self.workqueue
            .enqueue_deferred_delete_with_target_node(ns, name, uid, run_after, target_node)
            .await
    }

    async fn ensure_deferred_delete_with_target_node(
        &self,
        ns: String,
        name: String,
        uid: String,
        run_after: Duration,
        target_node: Option<String>,
    ) -> Result<bool> {
        self.workqueue
            .ensure_deferred_delete_with_target_node(ns, name, uid, run_after, target_node)
            .await
    }
}

#[async_trait]
trait PodDeleteSleeperPort: Send + Sync {
    async fn sleep(&self, name: &'static str, duration: Duration);
}

struct SupervisorPodDeleteSleeper {
    supervisor: Arc<TaskSupervisor>,
}

#[async_trait]
impl PodDeleteSleeperPort for SupervisorPodDeleteSleeper {
    async fn sleep(&self, name: &'static str, duration: Duration) {
        let _ = self.supervisor.sleep(name, duration).await;
    }
}

pub struct PodDeleteCoordinator {
    store: Arc<dyn PodDeleteStorePort>,
    queue: Arc<dyn PodDeleteQueuePort>,
    sleeper: Arc<dyn PodDeleteSleeperPort>,
    metrics: Arc<dyn ReconcileFailureMetrics>,
    wall_clock: Arc<dyn crate::runtime_clock::RuntimeClock>,
}

impl PodDeleteCoordinator {
    pub fn new(
        store: Arc<PodStore>,
        workqueue: Arc<PodWorkqueue>,
        supervisor: Arc<TaskSupervisor>,
        metrics: Arc<dyn ReconcileFailureMetrics>,
        wall_clock: Arc<dyn crate::runtime_clock::RuntimeClock>,
    ) -> Self {
        Self::new_with_ports(
            store,
            Arc::new(WorkqueuePodDeleteQueue { workqueue }),
            Arc::new(SupervisorPodDeleteSleeper { supervisor }),
            metrics,
            wall_clock,
        )
    }

    fn new_with_ports(
        store: Arc<dyn PodDeleteStorePort>,
        queue: Arc<dyn PodDeleteQueuePort>,
        sleeper: Arc<dyn PodDeleteSleeperPort>,
        metrics: Arc<dyn ReconcileFailureMetrics>,
        wall_clock: Arc<dyn crate::runtime_clock::RuntimeClock>,
    ) -> Self {
        Self {
            store,
            queue,
            sleeper,
            metrics,
            wall_clock,
        }
    }

    fn dry_run_delete_body(
        &self,
        resource: &Resource,
        requested_grace_period_seconds: Option<i64>,
    ) -> Result<Value, PodRepositoryError> {
        let mut data = plan_pod_delete_deadline(
            &resource.data,
            requested_grace_period_seconds,
            self.wall_clock.now_utc(),
        )
        .map(|plan| plan.body)
        .map_err(|error| PodRepositoryError::internal(error.to_string()))?;
        if let Some(obj) = data.as_object_mut()
            && let Some(meta) = obj.get_mut("metadata").and_then(|m| m.as_object_mut())
        {
            meta.insert(
                "resourceVersion".to_string(),
                Value::String(resource.resource_version.to_string()),
            );
        }
        Ok(data)
    }

    async fn enqueue_actor_finalize_if_ready_inner(
        &self,
        ns: &str,
        name: &str,
        resource: &Resource,
    ) -> Result<(), PodRepositoryError> {
        if !has_nonempty_pod_deletion_timestamp(&resource.data)
            || resource
                .data
                .pointer("/metadata/finalizers")
                .and_then(|finalizers| finalizers.as_array())
                .is_some_and(|finalizers| !finalizers.is_empty())
        {
            return Ok(());
        }

        if let Err(err) = self
            .enqueue_marked_pod_retry(
                ns.to_string(),
                name.to_string(),
                resource.uid.clone(),
                Duration::ZERO,
                &resource.data,
            )
            .await
        {
            self.metrics.record_cascade_delete_failure();
            tracing::error!(
                namespace = %ns,
                name = %name,
                uid = %resource.uid,
                error = %err,
                "failed to enqueue actor finalization after pod finalizers drained"
            );
            return Err(PodRepositoryError::internal(format!(
                "failed to enqueue actor finalization for {ns}/{name} uid {}: {err:#}",
                resource.uid
            )));
        }
        Ok(())
    }

    async fn mark_and_queue_api_delete(
        &self,
        ns: &str,
        name: &str,
        requested_grace_period_seconds: Option<i64>,
        delete_preconditions: &ResourcePreconditions,
        initial_resource: Resource,
    ) -> Result<PodDeleteMarkOutcome, PodRepositoryError> {
        let mut current = initial_resource;
        let mut attempt = 0u32;
        let operation_now = self.wall_clock.now_utc();

        let (updated, previous, plan) = loop {
            let delete_base = if delete_preconditions.resource_version.is_some() {
                current.clone()
            } else {
                self.store
                    .get(ns, name)
                    .await
                    .map_err(|error| map_store_error(error, ns, name))?
                    .ok_or_else(|| PodRepositoryError::not_found(ns, name))?
            };
            ensure_resource_preconditions_match(&delete_base, delete_preconditions)?;
            let plan = plan_pod_delete_deadline(
                &delete_base.data,
                requested_grace_period_seconds,
                operation_now,
            )
            .map_err(|error| PodRepositoryError::internal(error.to_string()))?;
            debug_assert!(
                !requested_grace_period_seconds.is_some_and(|requested| {
                    delete_base
                        .data
                        .pointer("/metadata/deletionGracePeriodSeconds")
                        .and_then(Value::as_i64)
                        .is_some_and(|existing| requested >= 0 && requested < existing)
                }) || plan.disposition == PodDeleteDeadlineDisposition::Shorten,
                "a shorter explicit Pod grace must produce a shortening plan"
            );
            if plan.disposition == PodDeleteDeadlineDisposition::Unchanged {
                break (delete_base.clone(), delete_base, plan);
            }
            let mark_result = self
                .store
                .mark_deleting_at_resource_version(
                    ns,
                    name,
                    &delete_base.uid,
                    plan.body.clone(),
                    delete_base.resource_version,
                )
                .await;

            match mark_result {
                Ok(updated) => {
                    break (updated, delete_base, plan);
                }
                Err(e)
                    if is_repository_conflict(&e) && attempt + 1 < MAX_DELETE_CONFLICT_RETRIES =>
                {
                    let backoff_ms = std::cmp::min(20u64.saturating_mul(1u64 << attempt), 250);
                    self.sleeper
                        .sleep(
                            "pod_delete_conflict_retry_backoff",
                            Duration::from_millis(backoff_ms),
                        )
                        .await;
                    current = self
                        .store
                        .get(ns, name)
                        .await
                        .map_err(|error| map_store_error(error, ns, name))?
                        .ok_or_else(|| PodRepositoryError::not_found(ns, name))?;
                    attempt += 1;
                    continue;
                }
                Err(error) => return Err(map_store_error(error, ns, name)),
            }
        };

        let uid = updated.uid.clone();
        let queue_result = if plan.disposition == PodDeleteDeadlineDisposition::Unchanged {
            self.ensure_marked_pod_retry(
                ns.to_string(),
                name.to_string(),
                uid.clone(),
                plan.remaining_delay,
                &updated.data,
            )
            .await
            .map(|_| ())
        } else if plan.queue_actor_reminder {
            self.enqueue_marked_pod_retry(
                ns.to_string(),
                name.to_string(),
                uid.clone(),
                plan.remaining_delay,
                &updated.data,
            )
            .await
        } else {
            Ok(())
        };
        if let Err(e) = queue_result {
            self.metrics.record_cascade_delete_failure();
            tracing::error!(
                namespace = %ns,
                name = %name,
                uid = %uid,
                error = %e,
                "failed to enqueue pod deferred delete"
            );
            return Err(PodRepositoryError::internal(format!(
                "failed to enqueue pod deferred delete for {ns}/{name} uid {uid}: {e:#}"
            )));
        }

        Ok(PodDeleteMarkOutcome {
            updated,
            previous,
            uid,
            changed: plan.disposition != PodDeleteDeadlineDisposition::Unchanged,
        })
    }

    async fn ensure_marked_pod_retry(
        &self,
        ns: String,
        name: String,
        uid: String,
        run_after: Duration,
        pod_data: &Value,
    ) -> Result<bool> {
        self.queue
            .ensure_deferred_delete_with_target_node(
                ns,
                name,
                uid,
                run_after,
                pod_target_node_from_pod_data(pod_data),
            )
            .await
    }

    async fn enqueue_marked_pod_retry(
        &self,
        ns: String,
        name: String,
        uid: String,
        run_after: Duration,
        pod_data: &Value,
    ) -> Result<()> {
        self.queue
            .enqueue_deferred_delete_with_target_node(
                ns,
                name,
                uid,
                run_after,
                pod_target_node_from_pod_data(pod_data),
            )
            .await
    }
}

impl PodDeleteOrchestration for PodDeleteCoordinator {
    fn preview_delete(
        &self,
        resource: &Resource,
        requested_grace_period_seconds: Option<i64>,
    ) -> Result<Value, PodRepositoryError> {
        self.dry_run_delete_body(resource, requested_grace_period_seconds)
    }

    fn mark_and_queue_delete(
        &self,
        request: PodDeleteMarkRequest,
    ) -> PodRepositoryFuture<'_, PodDeleteMarkOutcome> {
        Box::pin(async move {
            self.mark_and_queue_api_delete(
                &request.namespace,
                &request.name,
                request.requested_grace_period_seconds,
                &request.preconditions,
                request.initial_resource,
            )
            .await
        })
    }

    fn enqueue_actor_finalize_if_ready(
        &self,
        request: PodActorFinalizeRequest,
    ) -> PodRepositoryFuture<'_, ()> {
        Box::pin(async move {
            self.enqueue_actor_finalize_if_ready_inner(
                &request.namespace,
                &request.name,
                &request.resource,
            )
            .await
        })
    }

    fn enqueue_marked_retry(&self, request: PodMarkedRetryRequest) -> PodRepositoryFuture<'_, ()> {
        Box::pin(async move {
            self.enqueue_marked_pod_retry(
                request.namespace,
                request.name,
                request.uid,
                request.run_after,
                &request.pod_data,
            )
            .await
            .map_err(|error| PodRepositoryError::internal(error.to_string()))
        })
    }
}

fn ensure_resource_preconditions_match(
    resource: &Resource,
    preconditions: &ResourcePreconditions,
) -> Result<(), PodRepositoryError> {
    if let Some(expected_uid) = preconditions.uid.as_deref()
        && resource.uid != expected_uid
    {
        return Err(PodRepositoryError::conflict("UID precondition failed"));
    }

    if let Some(expected_rv) = preconditions.resource_version
        && resource.resource_version != expected_rv
    {
        return Err(PodRepositoryError::conflict(format!(
            "resourceVersion precondition failed: expected {expected_rv} got {}",
            resource.resource_version
        )));
    }

    Ok(())
}

fn map_store_error(error: anyhow::Error, namespace: &str, name: &str) -> PodRepositoryError {
    match error.downcast::<PodRepositoryError>() {
        Ok(error) => error,
        Err(error) => {
            let _ = (namespace, name);
            PodRepositoryError::internal(error.to_string())
        }
    }
}

fn is_repository_conflict(error: &anyhow::Error) -> bool {
    matches!(
        error.downcast_ref::<PodRepositoryError>(),
        Some(PodRepositoryError::Conflict { .. })
    )
}

fn pod_target_node_from_pod_data(pod: &Value) -> Option<String> {
    pod.pointer("/spec/nodeName")
        .and_then(|node| node.as_str())
        .filter(|node| !node.trim().is_empty())
        .map(ToString::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Mutex;

    #[derive(Default)]
    struct RecordingMetrics {
        cascade_delete_failures_total: AtomicUsize,
        namespace_delete_failures_total: AtomicUsize,
    }

    impl RecordingMetrics {
        fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }
    }

    impl ReconcileFailureMetrics for RecordingMetrics {
        fn record_cascade_delete_failure(&self) {
            self.cascade_delete_failures_total
                .fetch_add(1, Ordering::SeqCst);
        }

        fn record_namespace_delete_failure(&self) {
            self.namespace_delete_failures_total
                .fetch_add(1, Ordering::SeqCst);
        }
    }

    struct FakeDeleteStore {
        current: Mutex<Resource>,
        writes: AtomicUsize,
        conflicts_remaining: AtomicUsize,
    }

    impl FakeDeleteStore {
        fn new(resource: Resource) -> Self {
            Self {
                current: Mutex::new(resource),
                writes: AtomicUsize::new(0),
                conflicts_remaining: AtomicUsize::new(0),
            }
        }

        fn with_conflicts(resource: Resource, conflicts: usize) -> Self {
            Self {
                current: Mutex::new(resource),
                writes: AtomicUsize::new(0),
                conflicts_remaining: AtomicUsize::new(conflicts),
            }
        }
    }

    #[async_trait]
    impl PodDeleteStorePort for FakeDeleteStore {
        async fn get(&self, _ns: &str, _name: &str) -> Result<Option<Resource>> {
            Ok(Some(self.current.lock().await.clone()))
        }

        async fn mark_deleting_at_resource_version(
            &self,
            _ns: &str,
            _name: &str,
            _uid: &str,
            body: Value,
            _expected_rv: i64,
        ) -> Result<Resource> {
            if self
                .conflicts_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(anyhow::Error::new(PodRepositoryError::conflict(
                    "injected delete CAS conflict",
                )));
            }
            self.writes.fetch_add(1, Ordering::SeqCst);
            let mut guard = self.current.lock().await;
            let mut updated = guard.clone();
            updated.resource_version += 1;
            updated.data = Arc::new(body);
            *guard = updated.clone();
            Ok(updated)
        }
    }

    struct FailingDeleteQueue {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl PodDeleteQueuePort for FailingDeleteQueue {
        async fn enqueue_deferred_delete_with_target_node(
            &self,
            _ns: String,
            _name: String,
            _uid: String,
            _run_after: Duration,
            _target_node: Option<String>,
        ) -> Result<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(anyhow::anyhow!("node-local workqueue write failed"))
        }

        async fn ensure_deferred_delete_with_target_node(
            &self,
            _ns: String,
            _name: String,
            _uid: String,
            _run_after: Duration,
            _target_node: Option<String>,
        ) -> Result<bool> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(anyhow::anyhow!("node-local workqueue ensure failed"))
        }
    }

    struct FailOnceDeleteQueue {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl PodDeleteQueuePort for FailOnceDeleteQueue {
        async fn enqueue_deferred_delete_with_target_node(
            &self,
            _ns: String,
            _name: String,
            _uid: String,
            _run_after: Duration,
            _target_node: Option<String>,
        ) -> Result<()> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                Err(anyhow::anyhow!("injected first reminder failure"))
            } else {
                Ok(())
            }
        }

        async fn ensure_deferred_delete_with_target_node(
            &self,
            _ns: String,
            _name: String,
            _uid: String,
            _run_after: Duration,
            _target_node: Option<String>,
        ) -> Result<bool> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                Err(anyhow::anyhow!("injected first reminder failure"))
            } else {
                Ok(true)
            }
        }
    }

    struct NoopDeleteSleeper;

    #[async_trait]
    impl PodDeleteSleeperPort for NoopDeleteSleeper {
        async fn sleep(&self, _name: &'static str, _duration: Duration) {}
    }

    struct RecordingDeleteQueue {
        delays: Mutex<Vec<Duration>>,
        targets: Mutex<Vec<Option<String>>>,
        existing: std::sync::atomic::AtomicBool,
    }

    impl Default for RecordingDeleteQueue {
        fn default() -> Self {
            Self {
                delays: Mutex::new(Vec::new()),
                targets: Mutex::new(Vec::new()),
                existing: std::sync::atomic::AtomicBool::new(false),
            }
        }
    }

    impl RecordingDeleteQueue {
        fn with_existing() -> Self {
            Self {
                delays: Mutex::new(Vec::new()),
                targets: Mutex::new(Vec::new()),
                existing: std::sync::atomic::AtomicBool::new(true),
            }
        }
    }

    #[async_trait]
    impl PodDeleteQueuePort for RecordingDeleteQueue {
        async fn enqueue_deferred_delete_with_target_node(
            &self,
            _ns: String,
            _name: String,
            _uid: String,
            run_after: Duration,
            target_node: Option<String>,
        ) -> Result<()> {
            self.delays.lock().await.push(run_after);
            self.targets.lock().await.push(target_node);
            self.existing.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn ensure_deferred_delete_with_target_node(
            &self,
            _ns: String,
            _name: String,
            _uid: String,
            run_after: Duration,
            target_node: Option<String>,
        ) -> Result<bool> {
            if self
                .existing
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                self.delays.lock().await.push(run_after);
                self.targets.lock().await.push(target_node);
                Ok(true)
            } else {
                Ok(false)
            }
        }
    }

    struct FixedDeleteClock;

    impl crate::runtime_clock::RuntimeClock for FixedDeleteClock {
        fn now_ms(&self) -> i64 {
            1_786_190_400_000
        }
    }

    #[derive(Default)]
    struct AdvancingDeleteClock {
        calls: AtomicUsize,
    }

    impl crate::runtime_clock::RuntimeClock for AdvancingDeleteClock {
        fn now_ms(&self) -> i64 {
            1_786_190_400_000 + self.calls.fetch_add(1, Ordering::SeqCst) as i64 * 10_000
        }
    }

    fn pod_resource() -> Resource {
        Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "pod-a".to_string(),
            uid: "uid-a".to_string(),
            resource_version: 7,
            data: Arc::new(json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "pod-a",
                    "namespace": "default",
                    "uid": "uid-a"
                },
                "spec": {
                    "nodeName": "node-a",
                    "terminationGracePeriodSeconds": 0,
                    "containers": [{"name": "app", "image": "busybox"}]
                }
            })),
        }
    }

    #[test]
    fn delete_precondition_errors_preserve_uid_and_resource_version_conflicts() {
        let resource = pod_resource();
        let cases = [
            (
                ResourcePreconditions {
                    uid: Some("other-uid".to_string()),
                    resource_version: None,
                },
                "UID precondition failed",
            ),
            (
                ResourcePreconditions {
                    uid: None,
                    resource_version: Some(8),
                },
                "resourceVersion precondition failed: expected 8 got 7",
            ),
        ];

        for (preconditions, expected_message) in cases {
            let error = ensure_resource_preconditions_match(&resource, &preconditions)
                .expect_err("mismatched Pod delete precondition must conflict");
            assert_eq!(
                error,
                PodRepositoryError::conflict(expected_message),
                "precondition {preconditions:?}"
            );
        }
    }

    #[test]
    fn dry_run_delete_uses_the_absolute_future_deadline() {
        let coordinator = PodDeleteCoordinator::new_with_ports(
            Arc::new(FakeDeleteStore::new(pod_resource())),
            Arc::new(RecordingDeleteQueue::default()),
            Arc::new(NoopDeleteSleeper),
            RecordingMetrics::new(),
            Arc::new(FixedDeleteClock),
        );
        let body = coordinator
            .dry_run_delete_body(&pod_resource(), Some(5))
            .expect("valid dry-run delete plan");
        assert_eq!(
            body.pointer("/metadata/deletionTimestamp")
                .and_then(Value::as_str),
            Some("2026-08-08T12:00:05.000000000Z")
        );
        assert_eq!(
            body.pointer("/metadata/deletionGracePeriodSeconds")
                .and_then(Value::as_i64),
            Some(5)
        );
    }

    #[tokio::test]
    async fn repeated_equal_delete_is_a_zero_write_zero_queue_noop() {
        let mut resource = pod_resource();
        let mut body = (*resource.data).clone();
        body["metadata"]["deletionTimestamp"] = json!("2026-08-08T12:00:30.000000000Z");
        body["metadata"]["deletionGracePeriodSeconds"] = json!(30);
        resource.data = Arc::new(body);
        let store = Arc::new(FakeDeleteStore::new(resource.clone()));
        let queue = Arc::new(RecordingDeleteQueue::with_existing());
        let coordinator = PodDeleteCoordinator::new_with_ports(
            store.clone(),
            queue.clone(),
            Arc::new(NoopDeleteSleeper),
            RecordingMetrics::new(),
            Arc::new(FixedDeleteClock),
        );

        let outcome = coordinator
            .mark_and_queue_api_delete(
                "default",
                "pod-a",
                Some(30),
                &ResourcePreconditions::default(),
                resource.clone(),
            )
            .await
            .expect("repeated delete");

        assert_eq!(outcome.updated.resource_version, resource.resource_version);
        assert!(!outcome.changed);
        assert_eq!(store.writes.load(Ordering::SeqCst), 0);
        assert!(queue.delays.lock().await.is_empty());
    }

    #[tokio::test]
    async fn terminating_pod_without_positive_stored_grace_queues_immediately_without_rewrite() {
        for stored_grace in [None, Some(0)] {
            let mut resource = pod_resource();
            let mut body = (*resource.data).clone();
            body["metadata"]["deletionTimestamp"] = json!("2026-08-08T12:00:30.000000000Z");
            if let Some(stored_grace) = stored_grace {
                body["metadata"]["deletionGracePeriodSeconds"] = json!(stored_grace);
            }
            resource.data = Arc::new(body);
            let store = Arc::new(FakeDeleteStore::new(resource.clone()));
            let queue = Arc::new(RecordingDeleteQueue::default());
            let coordinator = PodDeleteCoordinator::new_with_ports(
                store.clone(),
                queue.clone(),
                Arc::new(NoopDeleteSleeper),
                RecordingMetrics::new(),
                Arc::new(FixedDeleteClock),
            );

            let outcome = coordinator
                .mark_and_queue_api_delete(
                    "default",
                    "pod-a",
                    None,
                    &ResourcePreconditions::default(),
                    resource,
                )
                .await
                .expect("existing terminating Pod reminder");

            assert!(!outcome.changed, "stored grace {stored_grace:?}");
            assert_eq!(store.writes.load(Ordering::SeqCst), 0);
            assert_eq!(
                queue.delays.lock().await.as_slice(),
                &[Duration::ZERO],
                "stored grace {stored_grace:?}"
            );
        }
    }

    #[tokio::test]
    async fn failed_delete_precondition_has_zero_writes_and_zero_queue_effects() {
        let store = Arc::new(FakeDeleteStore::new(pod_resource()));
        let queue = Arc::new(RecordingDeleteQueue::default());
        let coordinator = PodDeleteCoordinator::new_with_ports(
            store.clone(),
            queue.clone(),
            Arc::new(NoopDeleteSleeper),
            RecordingMetrics::new(),
            Arc::new(FixedDeleteClock),
        );

        let error = coordinator
            .mark_and_queue_api_delete(
                "default",
                "pod-a",
                Some(5),
                &ResourcePreconditions {
                    uid: Some("wrong-uid".to_string()),
                    resource_version: None,
                },
                pod_resource(),
            )
            .await
            .expect_err("UID precondition must fail");

        assert_eq!(
            error,
            PodRepositoryError::conflict("UID precondition failed")
        );
        assert_eq!(store.writes.load(Ordering::SeqCst), 0);
        assert!(queue.delays.lock().await.is_empty());
    }

    #[tokio::test]
    async fn dry_run_matches_real_delete_body_without_writes_or_queue_effects() {
        let store = Arc::new(FakeDeleteStore::new(pod_resource()));
        let queue = Arc::new(RecordingDeleteQueue::default());
        let coordinator = PodDeleteCoordinator::new_with_ports(
            store.clone(),
            queue.clone(),
            Arc::new(NoopDeleteSleeper),
            RecordingMetrics::new(),
            Arc::new(FixedDeleteClock),
        );

        let mut dry_run = coordinator
            .dry_run_delete_body(&pod_resource(), Some(5))
            .expect("valid dry-run delete plan");
        assert_eq!(store.writes.load(Ordering::SeqCst), 0);
        assert!(queue.delays.lock().await.is_empty());

        let outcome = coordinator
            .mark_and_queue_api_delete(
                "default",
                "pod-a",
                Some(5),
                &ResourcePreconditions::default(),
                pod_resource(),
            )
            .await
            .expect("real delete");
        dry_run["metadata"]
            .as_object_mut()
            .expect("metadata object")
            .remove("resourceVersion");
        assert_eq!(dry_run, *outcome.updated.data);
        assert_eq!(store.writes.load(Ordering::SeqCst), 1);
        assert_eq!(
            queue.delays.lock().await.as_slice(),
            &[Duration::from_secs(5)]
        );
    }

    #[tokio::test]
    async fn shorter_delete_recomputes_from_original_start_and_queues_remaining_deadline() {
        let mut resource = pod_resource();
        let mut body = (*resource.data).clone();
        body["metadata"]["deletionTimestamp"] = json!("2026-08-08T12:00:30.000000000Z");
        body["metadata"]["deletionGracePeriodSeconds"] = json!(30);
        resource.data = Arc::new(body);
        let store = Arc::new(FakeDeleteStore::new(resource.clone()));
        let queue = Arc::new(RecordingDeleteQueue::default());
        let coordinator = PodDeleteCoordinator::new_with_ports(
            store.clone(),
            queue.clone(),
            Arc::new(NoopDeleteSleeper),
            RecordingMetrics::new(),
            Arc::new(FixedDeleteClock),
        );

        let outcome = coordinator
            .mark_and_queue_api_delete(
                "default",
                "pod-a",
                Some(5),
                &ResourcePreconditions::default(),
                resource,
            )
            .await
            .expect("shorter delete");

        assert!(outcome.changed);
        assert_eq!(store.writes.load(Ordering::SeqCst), 1);
        assert_eq!(
            outcome
                .updated
                .data
                .pointer("/metadata/deletionTimestamp")
                .and_then(Value::as_str),
            Some("2026-08-08T12:00:05.000000000Z")
        );
        assert_eq!(
            queue.delays.lock().await.as_slice(),
            &[Duration::from_secs(5)]
        );
    }

    #[tokio::test]
    async fn delete_cas_retries_reuse_one_operation_clock_sample() {
        let store = Arc::new(FakeDeleteStore::with_conflicts(pod_resource(), 1));
        let queue = Arc::new(RecordingDeleteQueue::default());
        let clock = Arc::new(AdvancingDeleteClock::default());
        let coordinator = PodDeleteCoordinator::new_with_ports(
            store,
            queue.clone(),
            Arc::new(NoopDeleteSleeper),
            RecordingMetrics::new(),
            clock.clone(),
        );

        let outcome = coordinator
            .mark_and_queue_api_delete(
                "default",
                "pod-a",
                Some(5),
                &ResourcePreconditions::default(),
                pod_resource(),
            )
            .await
            .expect("delete after one CAS conflict");

        assert_eq!(clock.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            outcome
                .updated
                .data
                .pointer("/metadata/deletionTimestamp")
                .and_then(Value::as_str),
            Some("2026-08-08T12:00:05.000000000Z")
        );
        assert_eq!(
            queue.delays.lock().await.as_slice(),
            &[Duration::from_secs(5)]
        );
    }

    #[test]
    fn store_errors_keep_kubernetes_conflict_not_found_and_internal_categories() {
        let cases = [
            (
                anyhow::Error::new(PodRepositoryError::conflict("stale resource version")),
                PodRepositoryError::conflict("stale resource version"),
            ),
            (
                anyhow::Error::new(PodRepositoryError::not_found("default", "pod-a")),
                PodRepositoryError::not_found("default", "pod-a"),
            ),
            (
                anyhow::anyhow!("node-local store unavailable"),
                PodRepositoryError::internal("node-local store unavailable"),
            ),
        ];

        for (error, expected) in cases {
            assert_eq!(map_store_error(error, "default", "pod-a"), expected);
        }
    }

    #[tokio::test]
    async fn api_delete_mark_returns_error_when_durable_retry_enqueue_fails() {
        let store = Arc::new(FakeDeleteStore::new(pod_resource()));
        let queue = Arc::new(FailingDeleteQueue {
            calls: AtomicUsize::new(0),
        });
        let metrics = RecordingMetrics::new();
        let coordinator = PodDeleteCoordinator::new_with_ports(
            store.clone(),
            queue.clone(),
            Arc::new(NoopDeleteSleeper),
            metrics.clone(),
            Arc::new(crate::runtime_clock::SystemRuntimeClock),
        );

        let err = coordinator
            .mark_and_queue_api_delete(
                "default",
                "pod-a",
                None,
                &ResourcePreconditions::default(),
                pod_resource(),
            )
            .await
            .expect_err("enqueue failure must fail the API delete");

        assert!(matches!(err, PodRepositoryError::Internal { .. }));
        assert_eq!(queue.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            metrics
                .cascade_delete_failures_total
                .load(Ordering::Relaxed),
            1
        );
        assert!(
            store
                .current
                .lock()
                .await
                .data
                .pointer("/metadata/deletionTimestamp")
                .is_some(),
            "pod may be marked, but the caller must see failure and can retry durable enqueue"
        );
    }

    #[tokio::test]
    async fn queue_failure_then_duplicate_delete_restores_missing_uid_reminder() {
        let mut initial = pod_resource();
        let mut initial_body = (*initial.data).clone();
        initial_body["spec"]["terminationGracePeriodSeconds"] = json!(30);
        initial.data = Arc::new(initial_body);
        let store = Arc::new(FakeDeleteStore::new(initial.clone()));
        let queue = Arc::new(FailOnceDeleteQueue {
            calls: AtomicUsize::new(0),
        });
        let coordinator = PodDeleteCoordinator::new_with_ports(
            store.clone(),
            queue.clone(),
            Arc::new(NoopDeleteSleeper),
            RecordingMetrics::new(),
            Arc::new(FixedDeleteClock),
        );

        coordinator
            .mark_and_queue_api_delete(
                "default",
                "pod-a",
                None,
                &ResourcePreconditions::default(),
                initial,
            )
            .await
            .expect_err("first reminder write must fail after the delete mark commits");
        let marked = store.current.lock().await.clone();
        let marked_rv = marked.resource_version;

        let duplicate = coordinator
            .mark_and_queue_api_delete(
                "default",
                "pod-a",
                None,
                &ResourcePreconditions::default(),
                marked,
            )
            .await
            .expect("duplicate delete must restore an absent durable reminder");

        assert!(!duplicate.changed);
        assert_eq!(duplicate.updated.resource_version, marked_rv);
        assert_eq!(store.writes.load(Ordering::SeqCst), 1);
        assert_eq!(queue.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn finalizer_drain_enqueue_failure_is_returned_for_retry() {
        let mut resource = pod_resource();
        let mut body = (*resource.data).clone();
        body["metadata"]["deletionTimestamp"] = json!("2026-08-08T12:00:30.000000000Z");
        body["metadata"]["finalizers"] = json!([]);
        resource.data = Arc::new(body);
        let queue = Arc::new(FailingDeleteQueue {
            calls: AtomicUsize::new(0),
        });
        let metrics = RecordingMetrics::new();
        let coordinator = PodDeleteCoordinator::new_with_ports(
            Arc::new(FakeDeleteStore::new(resource.clone())),
            queue.clone(),
            Arc::new(NoopDeleteSleeper),
            metrics.clone(),
            Arc::new(FixedDeleteClock),
        );

        let error = coordinator
            .enqueue_actor_finalize_if_ready_inner("default", "pod-a", &resource)
            .await
            .expect_err("finalizer-drain queue failure must reach the API retry path");
        assert!(matches!(error, PodRepositoryError::Internal { .. }));
        assert_eq!(queue.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            metrics
                .cascade_delete_failures_total
                .load(Ordering::Relaxed),
            1
        );
    }

    #[tokio::test]
    async fn malformed_deletion_metadata_fails_closed_identically_for_dry_run_and_live() {
        let mut resource = pod_resource();
        let mut body = (*resource.data).clone();
        body["metadata"]["deletionTimestamp"] = json!({"malformed": true});
        resource.data = Arc::new(body);
        let store = Arc::new(FakeDeleteStore::new(resource.clone()));
        let queue = Arc::new(RecordingDeleteQueue::default());
        let coordinator = PodDeleteCoordinator::new_with_ports(
            store.clone(),
            queue.clone(),
            Arc::new(NoopDeleteSleeper),
            RecordingMetrics::new(),
            Arc::new(FixedDeleteClock),
        );

        let dry_error = coordinator
            .dry_run_delete_body(&resource, None)
            .expect_err("dry-run malformed deadline must fail closed");
        let live_error = coordinator
            .mark_and_queue_api_delete(
                "default",
                "pod-a",
                None,
                &ResourcePreconditions::default(),
                resource,
            )
            .await
            .expect_err("live malformed deadline must fail closed");
        assert_eq!(dry_error, live_error);
        assert_eq!(store.writes.load(Ordering::SeqCst), 0);
        assert!(queue.delays.lock().await.is_empty());
    }

    #[tokio::test]
    async fn null_deletion_timestamp_does_not_enqueue_actor_finalization() {
        let mut resource = pod_resource();
        let mut body = (*resource.data).clone();
        body["metadata"]["deletionTimestamp"] = Value::Null;
        body["metadata"]["finalizers"] = json!([]);
        resource.data = Arc::new(body);
        let queue = Arc::new(FailingDeleteQueue {
            calls: AtomicUsize::new(0),
        });
        let coordinator = PodDeleteCoordinator::new_with_ports(
            Arc::new(FakeDeleteStore::new(resource.clone())),
            queue.clone(),
            Arc::new(NoopDeleteSleeper),
            RecordingMetrics::new(),
            Arc::new(FixedDeleteClock),
        );

        coordinator
            .enqueue_actor_finalize_if_ready_inner("default", "pod-a", &resource)
            .await
            .expect("null timestamp is not terminating and must be ignored");
        assert_eq!(queue.calls.load(Ordering::SeqCst), 0);
    }

    struct PausedBindingDeleteStore {
        current: Mutex<Resource>,
        mark_entered: tokio::sync::Notify,
        release_mark: tokio::sync::Notify,
        first_mark: std::sync::atomic::AtomicBool,
    }

    #[async_trait]
    impl PodDeleteStorePort for PausedBindingDeleteStore {
        async fn get(&self, _ns: &str, _name: &str) -> Result<Option<Resource>> {
            Ok(Some(self.current.lock().await.clone()))
        }

        async fn mark_deleting_at_resource_version(
            &self,
            _ns: &str,
            _name: &str,
            uid: &str,
            body: Value,
            expected_rv: i64,
        ) -> Result<Resource> {
            if self.first_mark.swap(false, Ordering::SeqCst) {
                self.mark_entered.notify_one();
                self.release_mark.notified().await;
            }
            let mut current = self.current.lock().await;
            if current.uid != uid || current.resource_version != expected_rv {
                return Err(anyhow::Error::new(PodRepositoryError::conflict(
                    "scheduler bind won delete CAS",
                )));
            }
            let mut updated = current.clone();
            updated.resource_version += 1;
            updated.data = Arc::new(body);
            *current = updated.clone();
            Ok(updated)
        }
    }

    #[tokio::test]
    async fn paused_api_delete_retries_after_scheduler_bind_and_targets_winning_node() {
        let mut initial = pod_resource();
        let mut body = (*initial.data).clone();
        body["spec"]["nodeName"] = json!("");
        body["spec"]["terminationGracePeriodSeconds"] = json!(30);
        initial.data = Arc::new(body);
        let store = Arc::new(PausedBindingDeleteStore {
            current: Mutex::new(initial.clone()),
            mark_entered: tokio::sync::Notify::new(),
            release_mark: tokio::sync::Notify::new(),
            first_mark: std::sync::atomic::AtomicBool::new(true),
        });
        let queue = Arc::new(RecordingDeleteQueue::default());
        let coordinator = Arc::new(PodDeleteCoordinator::new_with_ports(
            store.clone(),
            queue.clone(),
            Arc::new(NoopDeleteSleeper),
            RecordingMetrics::new(),
            Arc::new(FixedDeleteClock),
        ));
        let entered = store.mark_entered.notified();
        let delete = {
            let coordinator = coordinator.clone();
            tokio::spawn(async move {
                coordinator
                    .mark_and_queue_api_delete(
                        "default",
                        "pod-a",
                        None,
                        &ResourcePreconditions::default(),
                        initial,
                    )
                    .await
            })
        };
        entered.await;
        {
            let mut current = store.current.lock().await;
            let mut bound = (*current.data).clone();
            bound["spec"]["nodeName"] = json!("node-bound-by-scheduler");
            current.resource_version += 1;
            current.data = Arc::new(bound);
        }
        store.release_mark.notify_one();

        let outcome = delete.await.unwrap().expect("delete retry after bind");
        assert!(outcome.changed);
        assert_eq!(outcome.uid, "uid-a");
        assert_eq!(
            outcome
                .updated
                .data
                .pointer("/spec/nodeName")
                .and_then(Value::as_str),
            Some("node-bound-by-scheduler")
        );
        assert!(has_nonempty_pod_deletion_timestamp(&outcome.updated.data));
        assert_eq!(
            queue.targets.lock().await.as_slice(),
            &[Some("node-bound-by-scheduler".to_string())]
        );
        assert_eq!(store.current.lock().await.uid, "uid-a");
    }
}
