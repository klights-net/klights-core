use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use klights_pod_api::PodRepositoryError;
use serde_json::Value;

use klights_cluster_core::{Resource, ResourcePreconditions};
use klights_kubelet::pod_repository::delete_deadline::{
    PodDeleteDeadlineDisposition, plan_pod_delete_deadline,
};
use klights_reconcile_api::ReconcileFailureMetrics;
use klights_supervisor::TaskSupervisor;

use klights_kubelet::pod_repository::store::PodStore;
use klights_kubelet::pod_repository::workqueue::PodWorkqueue;

const MAX_DELETE_CONFLICT_RETRIES: u32 = 8;

#[derive(Debug)]
pub(crate) struct PodDeleteMarkOutcome {
    pub updated: Resource,
    pub previous: Resource,
    pub uid: String,
    pub changed: bool,
}

#[async_trait]
pub(crate) trait PodDeleteStorePort: Send + Sync {
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
pub(crate) trait PodDeleteQueuePort: Send + Sync {
    async fn enqueue_deferred_delete_with_target_node(
        &self,
        ns: String,
        name: String,
        uid: String,
        run_after: Duration,
        target_node: Option<String>,
    ) -> Result<()>;
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
}

#[async_trait]
pub(crate) trait PodDeleteSleeperPort: Send + Sync {
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
    wall_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
}

impl PodDeleteCoordinator {
    pub(crate) fn new(
        store: Arc<PodStore>,
        workqueue: Arc<PodWorkqueue>,
        supervisor: Arc<TaskSupervisor>,
        metrics: Arc<dyn ReconcileFailureMetrics>,
        wall_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
    ) -> Self {
        Self::new_with_ports(
            store,
            Arc::new(WorkqueuePodDeleteQueue { workqueue }),
            Arc::new(SupervisorPodDeleteSleeper { supervisor }),
            metrics,
            wall_clock,
        )
    }

    pub(crate) fn new_with_ports(
        store: Arc<dyn PodDeleteStorePort>,
        queue: Arc<dyn PodDeleteQueuePort>,
        sleeper: Arc<dyn PodDeleteSleeperPort>,
        metrics: Arc<dyn ReconcileFailureMetrics>,
        wall_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
    ) -> Self {
        Self {
            store,
            queue,
            sleeper,
            metrics,
            wall_clock,
        }
    }

    pub(crate) fn dry_run_delete_body(
        &self,
        resource: &Resource,
        requested_grace_period_seconds: Option<i64>,
    ) -> Value {
        let mut data = plan_pod_delete_deadline(
            &resource.data,
            requested_grace_period_seconds,
            self.wall_clock.now_utc(),
        )
        .map(|plan| plan.body)
        .unwrap_or_else(|_| (*resource.data).clone());
        if let Some(obj) = data.as_object_mut()
            && let Some(meta) = obj.get_mut("metadata").and_then(|m| m.as_object_mut())
        {
            meta.insert(
                "resourceVersion".to_string(),
                Value::String(resource.resource_version.to_string()),
            );
        }
        data
    }

    pub(crate) async fn enqueue_actor_finalize_if_ready(
        &self,
        ns: &str,
        name: &str,
        resource: &Resource,
    ) {
        if resource
            .data
            .pointer("/metadata/deletionTimestamp")
            .is_none()
            || resource
                .data
                .pointer("/metadata/finalizers")
                .and_then(|finalizers| finalizers.as_array())
                .is_some_and(|finalizers| !finalizers.is_empty())
        {
            return;
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
        }
    }

    pub(crate) async fn mark_and_queue_api_delete(
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
        if plan.queue_actor_reminder
            && let Err(e) = self
                .enqueue_marked_pod_retry(
                    ns.to_string(),
                    name.to_string(),
                    uid.clone(),
                    plan.remaining_delay,
                    &updated.data,
                )
                .await
        {
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

    pub(crate) async fn enqueue_marked_pod_retry(
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

pub(super) fn pod_target_node_from_pod_data(pod: &Value) -> Option<String> {
    pod.pointer("/spec/nodeName")
        .and_then(|node| node.as_str())
        .filter(|node| !node.trim().is_empty())
        .map(ToString::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use klights_controllers::side_effects::SideEffectMetrics;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Mutex;

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
    }

    struct NoopDeleteSleeper;

    #[async_trait]
    impl PodDeleteSleeperPort for NoopDeleteSleeper {
        async fn sleep(&self, _name: &'static str, _duration: Duration) {}
    }

    #[derive(Default)]
    struct RecordingDeleteQueue {
        delays: Mutex<Vec<Duration>>,
    }

    #[async_trait]
    impl PodDeleteQueuePort for RecordingDeleteQueue {
        async fn enqueue_deferred_delete_with_target_node(
            &self,
            _ns: String,
            _name: String,
            _uid: String,
            run_after: Duration,
            _target_node: Option<String>,
        ) -> Result<()> {
            self.delays.lock().await.push(run_after);
            Ok(())
        }
    }

    struct FixedDeleteClock;

    impl klights_kubelet::runtime_clock::RuntimeClock for FixedDeleteClock {
        fn now_ms(&self) -> i64 {
            1_786_190_400_000
        }
    }

    #[derive(Default)]
    struct AdvancingDeleteClock {
        calls: AtomicUsize,
    }

    impl klights_kubelet::runtime_clock::RuntimeClock for AdvancingDeleteClock {
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
            SideEffectMetrics::new(),
            Arc::new(FixedDeleteClock),
        );
        let body = coordinator.dry_run_delete_body(&pod_resource(), Some(5));
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
        let queue = Arc::new(RecordingDeleteQueue::default());
        let coordinator = PodDeleteCoordinator::new_with_ports(
            store.clone(),
            queue.clone(),
            Arc::new(NoopDeleteSleeper),
            SideEffectMetrics::new(),
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
                SideEffectMetrics::new(),
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
            SideEffectMetrics::new(),
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
            SideEffectMetrics::new(),
            Arc::new(FixedDeleteClock),
        );

        let mut dry_run = coordinator.dry_run_delete_body(&pod_resource(), Some(5));
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
            SideEffectMetrics::new(),
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
            SideEffectMetrics::new(),
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
        let metrics = SideEffectMetrics::new();
        let coordinator = PodDeleteCoordinator::new_with_ports(
            store.clone(),
            queue.clone(),
            Arc::new(NoopDeleteSleeper),
            metrics.clone(),
            Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
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
}
