use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use klights_pod_api::PodRepositoryError;
use serde_json::{Value, json};

use klights_cluster_core::{Resource, ResourcePreconditions};
use klights_reconcile_api::ReconcileFailureMetrics;
use klights_supervisor::TaskSupervisor;

use super::store::PodStore;
use super::workqueue::PodWorkqueue;

const MAX_DELETE_CONFLICT_RETRIES: u32 = 8;

#[derive(Debug)]
pub(crate) struct PodDeleteMarkOutcome {
    pub updated: Resource,
    pub previous: Resource,
    pub uid: String,
}

#[async_trait]
pub(crate) trait PodDeleteStorePort: Send + Sync {
    async fn get(&self, ns: &str, name: &str) -> Result<Option<Resource>>;

    async fn mark_deleting_latest(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        body: &Value,
    ) -> Result<Resource>;

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

    async fn mark_deleting_latest(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        body: &Value,
    ) -> Result<Resource> {
        PodStore::mark_deleting_latest(self, ns, name, uid, body).await
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

    async fn enqueue_namespace_termination_pod(
        &self,
        ns: String,
        name: String,
        uid: String,
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

    async fn enqueue_namespace_termination_pod(
        &self,
        ns: String,
        name: String,
        uid: String,
        target_node: Option<String>,
    ) -> Result<()> {
        self.workqueue
            .enqueue_deferred_delete_row_with_target_node(
                ns,
                name,
                uid,
                Duration::ZERO,
                target_node,
            )
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
        let grace_period_seconds =
            pod_delete_grace_period_seconds(&resource.data, requested_grace_period_seconds);
        let mut data = pod_data_with_deletion_metadata(
            &resource.data,
            grace_period_seconds,
            self.wall_clock.now_utc(),
        );
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

        let (updated, previous, grace_period_seconds) = loop {
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
            let grace_period_seconds =
                pod_delete_grace_period_seconds(&delete_base.data, requested_grace_period_seconds);
            let data = pod_data_with_deletion_metadata(
                &delete_base.data,
                grace_period_seconds,
                self.wall_clock.now_utc(),
            );
            let mark_result = if delete_preconditions.resource_version.is_some() {
                self.store
                    .mark_deleting_at_resource_version(
                        ns,
                        name,
                        &delete_base.uid,
                        data,
                        delete_base.resource_version,
                    )
                    .await
            } else {
                self.store
                    .mark_deleting_latest(ns, name, &delete_base.uid, &data)
                    .await
            };

            match mark_result {
                Ok(updated) => {
                    break (updated, delete_base, grace_period_seconds);
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

        let uid = updated
            .data
            .get("metadata")
            .and_then(|m| m.get("uid"))
            .and_then(|u| u.as_str())
            .unwrap_or("")
            .to_string();
        let deferred_delete_delay = Duration::from_secs(grace_period_seconds as u64);
        if let Err(e) = self
            .enqueue_marked_pod_retry(
                ns.to_string(),
                name.to_string(),
                uid.clone(),
                deferred_delete_delay,
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

    pub(super) async fn enqueue_terminating_namespace_pod(
        &self,
        namespace: &str,
        resource: &Resource,
    ) -> Result<bool> {
        if resource
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|value| value.as_str())
            .is_none()
        {
            return Ok(false);
        }
        if resource.uid.is_empty() {
            tracing::warn!(
                namespace = %namespace,
                pod = %resource.name,
                "namespace termination cannot enqueue actor-owned Pod delete without UID"
            );
            return Ok(false);
        }

        self.queue
            .enqueue_namespace_termination_pod(
                namespace.to_string(),
                resource.name.clone(),
                resource.uid.clone(),
                pod_target_node_from_pod_data(&resource.data),
            )
            .await?;
        Ok(true)
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

pub(super) fn pod_data_with_deletion_metadata(
    data: &Value,
    grace_period_seconds: i64,
    operation_now: chrono::DateTime<chrono::Utc>,
) -> Value {
    let mut data = data.clone();
    if let Some(meta) = data.get_mut("metadata").and_then(|m| m.as_object_mut())
        && meta
            .get("deletionTimestamp")
            .is_none_or(|timestamp| timestamp.is_null())
    {
        meta.insert(
            "deletionTimestamp".to_string(),
            Value::String(klights_cluster_core::k8s_time::format_legacy_timestamp(
                operation_now,
            )),
        );
        meta.insert(
            "deletionGracePeriodSeconds".to_string(),
            json!(grace_period_seconds),
        );
    }
    if data
        .pointer("/metadata/deletionTimestamp")
        .is_some_and(|timestamp| !timestamp.is_null())
    {
        let transition_time =
            klights_cluster_core::k8s_time::format_legacy_timestamp(operation_now);
        klights_types::mark_terminating_pod_unready_at(&mut data, &transition_time);
    }
    data
}

pub(super) fn pod_delete_grace_period_seconds(
    data: &Value,
    requested_grace_period_seconds: Option<i64>,
) -> i64 {
    requested_grace_period_seconds
        .or_else(|| {
            data.pointer("/spec/terminationGracePeriodSeconds")
                .and_then(|value| value.as_i64())
        })
        .unwrap_or(30)
        .max(0)
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Mutex;

    struct FakeDeleteStore {
        current: Mutex<Resource>,
    }

    impl FakeDeleteStore {
        fn new(resource: Resource) -> Self {
            Self {
                current: Mutex::new(resource),
            }
        }
    }

    #[async_trait]
    impl PodDeleteStorePort for FakeDeleteStore {
        async fn get(&self, _ns: &str, _name: &str) -> Result<Option<Resource>> {
            Ok(Some(self.current.lock().await.clone()))
        }

        async fn mark_deleting_latest(
            &self,
            _ns: &str,
            _name: &str,
            _uid: &str,
            body: &Value,
        ) -> Result<Resource> {
            let mut guard = self.current.lock().await;
            let mut updated = guard.clone();
            updated.resource_version += 1;
            updated.data = Arc::new(body.clone());
            *guard = updated.clone();
            Ok(updated)
        }

        async fn mark_deleting_at_resource_version(
            &self,
            ns: &str,
            name: &str,
            uid: &str,
            body: Value,
            _expected_rv: i64,
        ) -> Result<Resource> {
            self.mark_deleting_latest(ns, name, uid, &body).await
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

        async fn enqueue_namespace_termination_pod(
            &self,
            _ns: String,
            _name: String,
            _uid: String,
            _target_node: Option<String>,
        ) -> Result<()> {
            unreachable!("not used by this test")
        }
    }

    struct NoopDeleteSleeper;

    #[async_trait]
    impl PodDeleteSleeperPort for NoopDeleteSleeper {
        async fn sleep(&self, _name: &'static str, _duration: Duration) {}
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
    fn delete_grace_policy_prefers_request_then_spec_then_default_and_clamps_zero() {
        let mut pod = (*pod_resource().data).clone();
        let cases = [(Some(5), 5), (Some(-1), 0), (None, 0)];
        for (requested, expected) in cases {
            assert_eq!(
                pod_delete_grace_period_seconds(&pod, requested),
                expected,
                "requested grace {requested:?}"
            );
        }

        pod.pointer_mut("/spec/terminationGracePeriodSeconds")
            .expect("fixture termination grace")
            .take();
        assert_eq!(pod_delete_grace_period_seconds(&pod, None), 30);
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
