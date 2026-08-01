// These tests hold TEST_ENV_LOCK across awaits on purpose: the guard
// serializes env-var mutation for the whole test body, so dropping it
// before the awaited reconcile would reintroduce the cross-test env race.
#![allow(clippy::await_holding_lock)]
use chrono::{TimeZone, Utc};
use klights_controllers::node_lifecycle::*;
use klights_leader_api::{ResourceEvent, WatchEventType};
use serde_json::json;

struct TestNodeLifecycleStatus<'a>(&'a dyn crate::datastore::DatastoreBackend);

impl klights_leader_api::LeaderNodeLifecycleStatus for TestNodeLifecycleStatus<'_> {
    fn submit_node_lifecycle_status(
        &self,
        request: klights_leader_api::NodeLifecycleStatusRequest,
    ) -> klights_leader_api::NodeLifecycleStatusFuture<
        '_,
        klights_leader_api::NodeLifecycleStatusResult,
    > {
        Box::pin(async move {
            let klights_cluster_core::StorageCommand::UpdateStatus {
                api_version,
                kind,
                namespace,
                name,
                status,
                preconditions,
                ..
            } = request.into_command()
            else {
                unreachable!("lifecycle request must be UpdateStatus")
            };
            let resource = self
                .0
                .update_status_only_with_preconditions(
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                    status,
                    preconditions,
                )
                .await
                .map_err(|error| {
                    klights_leader_api::NodeLifecycleStatusError::apply_failed(error.to_string())
                })?;
            Ok(klights_leader_api::NodeLifecycleStatusResult::Updated {
                resource_version: resource.resource_version,
            })
        })
    }
}

fn test_pod_store(
    db: &crate::datastore::sqlite::Datastore,
) -> crate::controller_runtime_adapter::RootControllerPodPort {
    crate::controller_runtime_adapter::RootControllerPodPort::new_for_test(
        crate::controllers::test_utils::pod_repository_for_test(db),
    )
}

fn eviction_grace() -> std::time::Duration {
    let seconds = std::env::var("KLIGHTS_NODE_NOT_READY_POD_EVICTION_GRACE_SECONDS")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value >= 0)
        .unwrap_or(0);
    std::time::Duration::from_secs(seconds as u64)
}

async fn reconcile_node_lifecycle_once_with_tracker_for_test(
    db: &crate::datastore::sqlite::Datastore,
    tracker: &klights_controllers::node_lease::NodeLeaseTracker,
    now: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<Option<std::time::Duration>> {
    let pods = test_pod_store(db);
    reconcile_node_lifecycle_once_with_tracker(
        db as &dyn crate::datastore::DatastoreBackend,
        &TestNodeLifecycleStatus(db),
        &pods,
        tracker,
        now,
        NodeLifecyclePodActions {
            mutation_reconcile: None,
            lifecycle: None,
            eviction_grace: eviction_grace(),
        },
    )
    .await
}

async fn reconcile_node_lifecycle_once(
    db: &crate::datastore::sqlite::Datastore,
    now: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<Option<std::time::Duration>> {
    let tracker = klights_controllers::node_lease::NodeLeaseTracker::new_at(now);
    refresh_node_lease_tracker_from_cluster_leases(
        db as &dyn crate::datastore::DatastoreBackend,
        &tracker,
    )
    .await?;
    reconcile_node_lifecycle_once_with_tracker_for_test(db, &tracker, now).await
}

async fn reconcile_node_lifecycle_once_after_startup(
    db: &crate::datastore::sqlite::Datastore,
    now: chrono::DateTime<chrono::Utc>,
    _startup_resource_version: i64,
) -> anyhow::Result<Option<std::time::Duration>> {
    reconcile_node_lifecycle_once_with_tracker_for_test(
        db,
        &klights_controllers::node_lease::NodeLeaseTracker::new_at(now),
        now,
    )
    .await
}

fn resource_event(event_type: WatchEventType, value: serde_json::Value) -> ResourceEvent {
    let resource =
        klights_cluster_core::Resource::try_from_data(std::sync::Arc::new(value)).unwrap();
    ResourceEvent::try_new(event_type, resource, None).unwrap()
}

/// Test-only env var guard: sets a var for the test's duration and
/// restores the prior value on drop. Use under `crate::TEST_ENV_LOCK`.
struct EnvVarGuard {
    name: &'static str,
    previous: Option<std::ffi::OsString>,
}
impl EnvVarGuard {
    fn set(name: &'static str, value: &str) -> Self {
        let previous = std::env::var_os(name);
        unsafe { std::env::set_var(name, value) };
        Self { name, previous }
    }
    fn remove(name: &'static str) -> Self {
        let previous = std::env::var_os(name);
        unsafe { std::env::remove_var(name) };
        Self { name, previous }
    }
}
impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match &self.previous {
            Some(v) => unsafe { std::env::set_var(self.name, v) },
            None => unsafe { std::env::remove_var(self.name) },
        }
    }
}

fn test_lifecycle_router() -> (
    std::sync::Arc<crate::kubelet::pod_lifecycle_router::PodLifecycleRouter>,
    std::sync::Arc<crate::kubelet::pod_lifecycle_router::executor::RecordingExecutor>,
) {
    let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let recorder = crate::kubelet::pod_lifecycle_router::executor::RecordingExecutor::new();
    let executor: std::sync::Arc<
        dyn crate::kubelet::pod_lifecycle_router::executor::PodWorkExecutor,
    > = recorder.clone();
    let registry = std::sync::Arc::new(
            crate::kubelet::pod_lifecycle_actor::registry::PodLifecycleRegistry::new(
                supervisor,
                crate::kubelet::pod_lifecycle_actor::config::PodLifecycleConcurrencyConfig::production_default(),
                std::sync::Arc::new(std::sync::Mutex::new(executor)),
            ),
        );
    (
        std::sync::Arc::new(
            crate::kubelet::pod_lifecycle_router::PodLifecycleRouter::new_actor_with_executor(
                registry,
                recorder.clone(),
            ),
        ),
        recorder,
    )
}

struct TestNodeLostSink(std::sync::Arc<crate::kubelet::pod_lifecycle_router::PodLifecycleRouter>);

#[async_trait::async_trait]
impl klights_controllers::node_lifecycle::NodeLostPodLifecycleSink for TestNodeLostSink {
    async fn enqueue_node_lost_cleanup(
        &self,
        pod: klights_cluster_core::Resource,
    ) -> klights_reconcile_api::ControllerStoreResult<()> {
        use crate::kubelet::pod_lifecycle_router::{OrphanReason, enqueue_orphan_finalize};

        enqueue_orphan_finalize(
            self.0.as_ref(),
            crate::kubelet::pod_lifecycle_core::message::PodLifecycleKey::new(
                pod.namespace.as_deref().unwrap_or("default"),
                &pod.name,
                &pod.uid,
            ),
            OrphanReason::NodeLost,
        )
        .await
        .map_err(|error| {
            klights_reconcile_api::ControllerStoreError::unavailable(error.to_string())
        })
    }
}

#[tokio::test]
async fn track_lease_from_event_updates_tracker() {
    let tracker = klights_controllers::node_lease::NodeLeaseTracker::new_at(
        Utc.with_ymd_and_hms(2026, 5, 13, 6, 30, 0).unwrap(),
    );
    let event = resource_event(
        WatchEventType::Added,
        json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": "worker-a",
                "namespace": "kube-node-lease",
                "resourceVersion": "1"
            },
            "spec": {
                "holderIdentity": "worker-a",
                "leaseDurationSeconds": 40,
                "renewTime": "2026-05-13T06:34:15.000000Z"
            }
        }),
    );

    klights_controllers::node_lifecycle::track_lease_from_event(&event, &tracker)
        .await
        .expect("event should refresh local lease tracker");

    let tracked = tracker.deadline_for_node("worker-a").await.observed;
    assert!(tracked.is_some());
    assert_eq!(
        tracked.as_ref().map(|obs| obs.renew_time.to_string()),
        Some("2026-05-13 06:34:15 UTC".to_string())
    );
}

#[tokio::test]
async fn track_lease_from_event_ignores_deleted_lease() {
    let tracker = klights_controllers::node_lease::NodeLeaseTracker::new_at(
        Utc.with_ymd_and_hms(2026, 5, 13, 6, 30, 0).unwrap(),
    );
    let event = resource_event(
        WatchEventType::Deleted,
        json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": "worker-a",
                "namespace": "kube-node-lease",
            },
            "spec": {
                "holderIdentity": "worker-a",
                "leaseDurationSeconds": 40,
                "renewTime": "2026-05-13T06:34:15.000000Z"
            }
        }),
    );

    klights_controllers::node_lifecycle::track_lease_from_event(&event, &tracker)
        .await
        .expect("deleted events should be ignored");
    assert!(
        tracker
            .deadline_for_node("worker-a")
            .await
            .observed
            .is_none()
    );
}

#[tokio::test]
async fn stale_node_lease_marks_ready_unknown() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Node",
        None,
        "worker-a",
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a"},
            "status": {
                "conditions": [
                    {
                        "type": "Ready",
                        "status": "True",
                        "reason": "KubeletReady",
                        "message": "klights is ready",
                        "lastHeartbeatTime": "2026-05-13T06:34:14Z",
                        "lastTransitionTime": "2026-05-13T06:34:14Z"
                    },
                    {
                        "type": "MemoryPressure",
                        "status": "False",
                        "reason": "KubeletHasSufficientMemory",
                        "message": "kubelet has sufficient memory available",
                        "lastHeartbeatTime": "2026-05-13T06:34:14Z",
                        "lastTransitionTime": "2026-05-13T06:34:14Z"
                    }
                ]
            }
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "coordination.k8s.io/v1",
        "Lease",
        Some("kube-node-lease"),
        "worker-a",
        json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {"name": "worker-a", "namespace": "kube-node-lease"},
            "spec": {
                "holderIdentity": "worker-a",
                "leaseDurationSeconds": 40,
                "renewTime": "2026-05-13T06:34:15.000000Z"
            }
        }),
    )
    .await
    .unwrap();

    let next =
        reconcile_node_lifecycle_once(&db, Utc.with_ymd_and_hms(2026, 5, 13, 6, 34, 56).unwrap())
            .await
            .unwrap();

    let node = db
        .get_resource("v1", "Node", None, "worker-a")
        .await
        .unwrap()
        .unwrap();
    let ready = node.data["status"]["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|condition| condition["type"] == "Ready")
        .unwrap();
    assert_eq!(ready["status"], "Unknown");
    assert_eq!(ready["reason"], "NodeStatusUnknown");
    assert_eq!(ready["message"], "Kubelet stopped posting node status.");
    assert!(
        ready.get("lastHeartbeatTime").is_none(),
        "leader must not persist the churny lastHeartbeatTime field"
    );
    assert_eq!(
        ready["lastTransitionTime"], "2026-05-13T06:34:56Z",
        "transition time records when the leader observed the stale node"
    );
    assert!(
        next.is_none(),
        "already-stale nodes should not schedule a hot retry after being marked Unknown"
    );
}

#[tokio::test]
async fn stale_node_lease_marks_bound_pods_unknown() {
    // This test verifies the Unknown projection in the window before
    // cleanup, so it pins a non-zero eviction grace (the default is now 0
    // = immediate cleanup; see default_zero_grace_cleans_stale_node_pod).
    let _env_lock = crate::TEST_ENV_LOCK.lock().unwrap();
    let _grace = EnvVarGuard::set("KLIGHTS_NODE_NOT_READY_POD_EVICTION_GRACE_SECONDS", "30");
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Node",
        None,
        "worker-a",
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a"},
            "status": {
                "conditions": [{
                    "type": "Ready",
                    "status": "True",
                    "reason": "KubeletReady",
                    "message": "klights is ready",
                    "lastHeartbeatTime": "2026-05-13T06:34:14Z",
                    "lastTransitionTime": "2026-05-13T06:34:14Z"
                }]
            }
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "coordination.k8s.io/v1",
        "Lease",
        Some("kube-node-lease"),
        "worker-a",
        json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {"name": "worker-a", "namespace": "kube-node-lease"},
            "spec": {
                "holderIdentity": "worker-a",
                "leaseDurationSeconds": 40,
                "renewTime": "2026-05-13T06:34:15.000000Z"
            }
        }),
    )
    .await
    .unwrap();
    seed_running_pod_on_node(&db, "worker-pod", "worker-pod-uid", "worker-a").await;
    seed_running_pod_on_node(&db, "other-pod", "other-pod-uid", "worker-b").await;

    reconcile_node_lifecycle_once(&db, Utc.with_ymd_and_hms(2026, 5, 13, 6, 34, 56).unwrap())
        .await
        .unwrap();

    let worker_pod = db
        .get_resource("v1", "Pod", Some("default"), "worker-pod")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(worker_pod.data["status"]["phase"], "Unknown");
    let ready = pod_condition(&worker_pod.data, "Ready");
    assert_eq!(ready["status"], "Unknown");
    assert_eq!(ready["reason"], "NodeStatusUnknown");
    assert_eq!(
        ready["message"], "Kubelet stopped posting node status.",
        "pod Unknown reason should explain that the node heartbeat went stale"
    );
    let containers_ready = pod_condition(&worker_pod.data, "ContainersReady");
    assert_eq!(containers_ready["status"], "Unknown");
    assert_eq!(containers_ready["reason"], "NodeStatusUnknown");
    assert_eq!(
        worker_pod.data["status"]["containerStatuses"][0]["ready"], true,
        "node-lifecycle Unknown projection must preserve the worker's last known container readiness"
    );
    assert_eq!(
        worker_pod.data["status"]["containerStatuses"][0]["started"], true,
        "node-lifecycle Unknown projection must preserve the worker's last known container start state"
    );
    assert!(
        worker_pod
            .data
            .pointer("/metadata/deletionTimestamp")
            .is_none(),
        "stale-node pods must stay Unknown during the pod eviction grace period"
    );

    let other_pod = db
        .get_resource("v1", "Pod", Some("default"), "other-pod")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        other_pod.data["status"]["phase"], "Running",
        "only pods bound to the stale node should be projected Unknown"
    );
}

#[tokio::test]
async fn fresh_node_status_heartbeat_prevents_stale_lease_pod_cleanup() {
    let _env_lock = crate::TEST_ENV_LOCK.lock().unwrap();
    let _grace = EnvVarGuard::remove("KLIGHTS_NODE_NOT_READY_POD_EVICTION_GRACE_SECONDS");
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Node",
        None,
        "worker-a",
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a"},
            "status": {
                "conditions": [{
                    "type": "Ready",
                    "status": "True",
                    "reason": "KubeletReady",
                    "message": "klights is ready",
                    "lastHeartbeatTime": "2026-05-13T06:34:55Z",
                    "lastTransitionTime": "2026-05-13T06:34:14Z"
                }]
            }
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "coordination.k8s.io/v1",
        "Lease",
        Some("kube-node-lease"),
        "worker-a",
        json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {"name": "worker-a", "namespace": "kube-node-lease"},
            "spec": {
                "holderIdentity": "worker-a",
                "leaseDurationSeconds": 40,
                "renewTime": "2026-05-13T06:34:15.000000Z"
            }
        }),
    )
    .await
    .unwrap();
    seed_running_pod_on_node(&db, "worker-pod", "worker-pod-uid", "worker-a").await;

    let next =
        reconcile_node_lifecycle_once(&db, Utc.with_ymd_and_hms(2026, 5, 13, 6, 34, 56).unwrap())
            .await
            .unwrap();

    let node = db
        .get_resource("v1", "Node", None, "worker-a")
        .await
        .unwrap()
        .unwrap();
    let ready = node.data["status"]["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|condition| condition["type"] == "Ready")
        .unwrap();
    assert_eq!(
        ready["status"], "True",
        "fresh Node status heartbeat must prevent stale-lease Unknown projection"
    );
    let pod = db
        .get_resource("v1", "Pod", Some("default"), "worker-pod")
        .await
        .unwrap()
        .expect("fresh node status heartbeat must preserve running pod");
    assert_eq!(pod.data["status"]["phase"], "Running");
    assert!(
        next.is_some(),
        "controller should sleep until the fresh node-status heartbeat deadline"
    );
}

#[tokio::test]
async fn default_zero_grace_marks_stale_node_pod_node_lost_immediately() {
    // With the default eviction grace (0), a pod on a confirmed-stale node
    // is marked NodeLost in the same reconcile pass. Actor finalization owns
    // the eventual API row removal.
    let _env_lock = crate::TEST_ENV_LOCK.lock().unwrap();
    let _grace = EnvVarGuard::remove("KLIGHTS_NODE_NOT_READY_POD_EVICTION_GRACE_SECONDS");
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Node",
        None,
        "worker-a",
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a"},
            "status": {
                "conditions": [{
                    "type": "Ready",
                    "status": "True",
                    "reason": "KubeletReady",
                    "message": "klights is ready",
                    "lastHeartbeatTime": "2026-05-13T06:34:14Z",
                    "lastTransitionTime": "2026-05-13T06:34:14Z"
                }]
            }
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "coordination.k8s.io/v1",
        "Lease",
        Some("kube-node-lease"),
        "worker-a",
        json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {"name": "worker-a", "namespace": "kube-node-lease"},
            "spec": {
                "holderIdentity": "worker-a",
                "leaseDurationSeconds": 40,
                "renewTime": "2026-05-13T06:34:15.000000Z"
            }
        }),
    )
    .await
    .unwrap();
    seed_running_pod_on_node(&db, "worker-pod", "worker-pod-uid", "worker-a").await;

    reconcile_node_lifecycle_once(&db, Utc.with_ymd_and_hms(2026, 5, 13, 6, 34, 56).unwrap())
        .await
        .unwrap();

    let pod = db
        .get_resource("v1", "Pod", Some("default"), "worker-pod")
        .await
        .unwrap()
        .expect("controller must preserve the API row for actor-owned finalization");
    assert_eq!(pod.data["status"]["phase"], "Failed");
    assert_eq!(pod.data["status"]["reason"], "NodeLost");
}

#[tokio::test]
async fn stale_node_lease_marks_unknown_bound_pods_node_lost_after_grace() {
    // Exercises the within-grace -> after-grace staging, so it pins a
    // non-zero eviction grace (the default is now 0 = immediate cleanup).
    let _env_lock = crate::TEST_ENV_LOCK.lock().unwrap();
    let _grace = EnvVarGuard::set("KLIGHTS_NODE_NOT_READY_POD_EVICTION_GRACE_SECONDS", "30");
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Node",
        None,
        "worker-a",
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a"},
            "status": {
                "conditions": [{
                    "type": "Ready",
                    "status": "True",
                    "reason": "KubeletReady",
                    "message": "klights is ready",
                    "lastHeartbeatTime": "2026-05-13T06:34:14Z",
                    "lastTransitionTime": "2026-05-13T06:34:14Z"
                }]
            }
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "coordination.k8s.io/v1",
        "Lease",
        Some("kube-node-lease"),
        "worker-a",
        json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {"name": "worker-a", "namespace": "kube-node-lease"},
            "spec": {
                "holderIdentity": "worker-a",
                "leaseDurationSeconds": 40,
                "renewTime": "2026-05-13T06:34:15.000000Z"
            }
        }),
    )
    .await
    .unwrap();
    seed_running_pod_on_node(&db, "worker-pod", "worker-pod-uid", "worker-a").await;

    reconcile_node_lifecycle_once(&db, Utc.with_ymd_and_hms(2026, 5, 13, 6, 34, 56).unwrap())
        .await
        .unwrap();
    let within_grace = db
        .get_resource("v1", "Pod", Some("default"), "worker-pod")
        .await
        .unwrap()
        .unwrap();
    assert!(
        within_grace
            .data
            .pointer("/metadata/deletionTimestamp")
            .is_none(),
        "pod should not terminate before the stale-node eviction grace period"
    );

    reconcile_node_lifecycle_once(&db, Utc.with_ymd_and_hms(2026, 5, 13, 6, 35, 26).unwrap())
        .await
        .unwrap();
    let after_grace = db
        .get_resource("v1", "Pod", Some("default"), "worker-pod")
        .await
        .unwrap()
        .expect("controller must preserve the API row for actor-owned finalization");
    assert_eq!(after_grace.data["metadata"]["uid"], "worker-pod-uid");
    assert_eq!(after_grace.data["spec"]["nodeName"], "worker-a");
    assert_eq!(after_grace.data["status"]["phase"], "Failed");
    assert_eq!(after_grace.data["status"]["reason"], "NodeLost");
    assert!(
        after_grace
            .data
            .pointer("/metadata/deletionTimestamp")
            .is_none(),
        "NodeLost cleanup must not synthesize a controller-side delete mark"
    );
}

#[tokio::test]
async fn deleted_node_event_marks_bound_pods_node_lost_and_wakes_actor() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repository = crate::controllers::test_utils::pod_repository_for_test(&db);
    let (router, recorder) = test_lifecycle_router();
    let pods =
        crate::controller_runtime_adapter::RootControllerPodPort::new_for_test(pod_repository);
    let lifecycle = TestNodeLostSink(router);
    seed_running_pod_on_node(&db, "fake-node-pod", "fake-node-pod-uid", "e2e-fake-node").await;
    seed_running_pod_on_node(&db, "real-node-pod", "real-node-pod-uid", "real-node").await;

    let event = resource_event(
        WatchEventType::Deleted,
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "e2e-fake-node"}
        }),
    );

    let cleaned = klights_controllers::node_lifecycle::cleanup_pods_bound_to_deleted_node_event(
        &db as &dyn crate::datastore::DatastoreBackend,
        &pods,
        None,
        Some(&lifecycle),
        &event,
        Utc::now(),
    )
    .await
    .expect("deleted Node cleanup must succeed");

    assert!(cleaned, "deleted Node event should be handled");
    let fake_node_pod = db
        .get_resource("v1", "Pod", Some("default"), "fake-node-pod")
        .await
        .unwrap()
        .expect("controller must leave picked-up Pods for actor-owned finalization");
    assert_eq!(fake_node_pod.data["status"]["phase"], "Failed");
    assert_eq!(fake_node_pod.data["status"]["reason"], "NodeLost");
    assert!(
        db.get_resource("v1", "Pod", Some("default"), "real-node-pod")
            .await
            .unwrap()
            .is_some(),
        "cleanup must be scoped to the deleted Node name"
    );

    for _ in 0..1000 {
        if recorder.action_count() > 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    let actions = recorder.take_actions();
    assert!(
        actions.iter().any(|action| {
            matches!(
                action,
                crate::kubelet::pod_lifecycle_core::action::PodAction::StopPod {
                    key,
                    ..
                } if key.namespace == "default"
                    && key.name == "fake-node-pod"
                    && key.uid == "fake-node-pod-uid"
            )
        }),
        "deleted-node cleanup must wake the UID-bound lifecycle actor: {actions:?}"
    );
}

#[tokio::test]
async fn node_lost_cleanup_enqueues_owning_replicaset_after_node_lost_mark() {
    // Uses the staged within-grace -> after-grace timing, so it pins a
    // non-zero eviction grace (the default is now 0 = immediate cleanup).
    let _env_lock = crate::TEST_ENV_LOCK.lock().unwrap();
    let _grace = EnvVarGuard::set("KLIGHTS_NODE_NOT_READY_POD_EVICTION_GRACE_SECONDS", "30");
    let state = crate::api::test_support::build_test_app_state().await;
    state
        .resource_mutation()
        .db
        .create_resource(
            "v1",
            "Node",
            None,
            "worker-a",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "worker-a"},
                "status": {
                    "conditions": [{
                        "type": "Ready",
                        "status": "True",
                        "reason": "KubeletReady",
                        "message": "klights is ready",
                        "lastHeartbeatTime": "2026-05-13T06:34:14Z",
                        "lastTransitionTime": "2026-05-13T06:34:14Z"
                    }]
                }
            }),
        )
        .await
        .unwrap();
    state
        .controller_reconcile()
        .node_lease_tracker
        .record_from_lease_object(
            "worker-a",
            &json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": "worker-a", "namespace": "kube-node-lease"},
                "spec": {
                    "holderIdentity": "worker-a",
                    "leaseDurationSeconds": 40,
                    "renewTime": "2026-05-13T06:34:15.000000Z"
                }
            }),
        )
        .await
        .unwrap();
    state
            .resource_mutation().db
            .create_resource(
                "apps/v1",
                "ReplicaSet",
                Some("default"),
                "owned-rs",
                json!({
                    "apiVersion": "apps/v1",
                    "kind": "ReplicaSet",
                    "metadata": {
                        "name": "owned-rs",
                        "namespace": "default",
                        "uid": "owned-rs-uid"
                    },
                    "spec": {
                        "replicas": 1,
                        "selector": {"matchLabels": {"app": "lost"}},
                        "template": {
                            "metadata": {"labels": {"app": "lost"}},
                            "spec": {"containers": [{"name": "app", "image": "registry.k8s.io/pause:3.10"}]}
                        }
                    }
                }),
            )
            .await
            .unwrap();
    state
        .resource_mutation()
        .db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "lost-pod",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "lost-pod",
                    "uid": "lost-pod-uid",
                    "creationTimestamp": "2026-05-13T06:30:00Z",
                    "labels": {"app": "lost"},
                    "ownerReferences": [{
                        "apiVersion": "apps/v1",
                        "kind": "ReplicaSet",
                        "name": "owned-rs",
                        "uid": "owned-rs-uid",
                        "controller": true
                    }]
                },
                "spec": {
                    "nodeName": "worker-a",
                    "containers": [{"name": "app", "image": "registry.k8s.io/pause:3.10"}]
                },
                "status": {
                    "phase": "Running",
                    "conditions": [
                        {
                            "type": "ContainersReady",
                            "status": "True",
                            "lastTransitionTime": "2026-05-13T06:30:10Z"
                        },
                        {
                            "type": "Ready",
                            "status": "True",
                            "lastTransitionTime": "2026-05-13T06:30:10Z"
                        }
                    ]
                }
            }),
        )
        .await
        .unwrap();

    let pods = crate::controller_runtime_adapter::RootControllerPodPort::new_for_test(
        state.resource_mutation().pod_repository.clone(),
    );
    klights_controllers::node_lifecycle::reconcile_node_lifecycle_once_with_tracker(
        state.resource_mutation().db.as_ref() as &dyn crate::datastore::DatastoreBackend,
        &TestNodeLifecycleStatus(state.resource_mutation().db.as_ref()),
        &pods,
        state.controller_reconcile().node_lease_tracker.as_ref(),
        Utc.with_ymd_and_hms(2026, 5, 13, 6, 34, 56).unwrap(),
        klights_controllers::node_lifecycle::NodeLifecyclePodActions {
            mutation_reconcile: Some(
                state
                    .resource_mutation()
                    .pod_repository
                    .mutation_reconcile_port()
                    .as_ref(),
            ),
            lifecycle: None,
            eviction_grace: std::time::Duration::ZERO,
        },
    )
    .await
    .unwrap();
    let pre_cleanup_keys = state
        .controller_reconcile()
        .controller_dispatcher
        .pending_reconcile_keys()
        .await;
    for _ in 0..pre_cleanup_keys.len() {
        let _ = state
            .controller_reconcile()
            .controller_dispatcher
            .take_reconcile_key_for_test()
            .await;
    }
    klights_controllers::node_lifecycle::reconcile_node_lifecycle_once_with_tracker(
        state.resource_mutation().db.as_ref() as &dyn crate::datastore::DatastoreBackend,
        &TestNodeLifecycleStatus(state.resource_mutation().db.as_ref()),
        &pods,
        state.controller_reconcile().node_lease_tracker.as_ref(),
        Utc.with_ymd_and_hms(2026, 5, 13, 6, 35, 26).unwrap(),
        klights_controllers::node_lifecycle::NodeLifecyclePodActions {
            mutation_reconcile: Some(
                state
                    .resource_mutation()
                    .pod_repository
                    .mutation_reconcile_port()
                    .as_ref(),
            ),
            lifecycle: None,
            eviction_grace: std::time::Duration::ZERO,
        },
    )
    .await
    .unwrap();

    let lost_pod = state
        .resource_mutation()
        .db
        .get_resource("v1", "Pod", Some("default"), "lost-pod")
        .await
        .unwrap()
        .expect("controller must preserve the Pod row for actor-owned finalization");
    assert_eq!(lost_pod.data["status"]["phase"], "Failed");
    assert_eq!(lost_pod.data["status"]["reason"], "NodeLost");
    let keys = state
        .controller_reconcile()
        .controller_dispatcher
        .pending_reconcile_keys()
        .await;
    assert!(
        keys.iter().any(|key| {
            key.api_version() == "apps/v1"
                && key.kind() == "ReplicaSet"
                && key.namespace() == Some("default")
                && key.name() == "owned-rs"
        }),
        "NodeLost cleanup must enqueue the owning ReplicaSet so it can reschedule"
    );
}

#[test]
fn node_lifecycle_retry_delay_increases_linearly_and_caps_at_sixty_seconds() {
    let expected = [
        (0, 5),
        (1, 10),
        (2, 15),
        (3, 20),
        (4, 25),
        (5, 30),
        (6, 35),
        (7, 40),
        (8, 45),
        (9, 50),
        (10, 55),
        (11, 60),
        (12, 60),
        (99, 60),
    ];
    for (attempt, seconds) in expected {
        assert_eq!(
            klights_controllers::node_lifecycle::node_lifecycle_retry_delay(attempt),
            std::time::Duration::from_secs(seconds),
            "attempt {attempt}"
        );
    }
}

async fn seed_running_pod_on_node(
    db: &crate::datastore::sqlite::Datastore,
    name: &str,
    uid: &str,
    node_name: &str,
) {
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        name,
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": name,
                "uid": uid,
                "creationTimestamp": "2026-05-13T06:30:00Z"
            },
            "spec": {
                "nodeName": node_name,
                "containers": [{"name": "app", "image": "registry.k8s.io/pause:3.10"}]
            },
            "status": {
                "phase": "Running",
                "podIP": "10.42.0.10",
                "hostIP": "192.0.2.10",
                "conditions": [
                    {
                        "type": "PodScheduled",
                        "status": "True",
                        "lastTransitionTime": "2026-05-13T06:30:00Z"
                    },
                    {
                        "type": "Initialized",
                        "status": "True",
                        "lastTransitionTime": "2026-05-13T06:30:00Z"
                    },
                    {
                        "type": "ContainersReady",
                        "status": "True",
                        "lastTransitionTime": "2026-05-13T06:30:10Z"
                    },
                    {
                        "type": "Ready",
                        "status": "True",
                        "lastTransitionTime": "2026-05-13T06:30:10Z"
                    }
                ],
                "containerStatuses": [{
                    "name": "app",
                    "ready": true,
                    "started": true,
                    "restartCount": 0,
                    "state": {"running": {"startedAt": "2026-05-13T06:30:10Z"}}
                }]
            }
        }),
    )
    .await
    .unwrap();
}

fn pod_condition<'a>(pod: &'a serde_json::Value, condition_type: &str) -> &'a serde_json::Value {
    pod["status"]["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|condition| condition["type"] == condition_type)
        .unwrap()
}

#[tokio::test]
async fn memory_lease_tracker_writes_node_status_only_after_deadline_expires() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Node",
        None,
        "worker-a",
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a"},
            "status": {
                "conditions": [{
                    "type": "Ready",
                    "status": "True",
                    "reason": "KubeletReady",
                    "message": "klights is ready",
                    "lastHeartbeatTime": "2026-05-13T06:35:09Z",
                    "lastTransitionTime": "2026-05-13T06:35:09Z"
                }]
            }
        }),
    )
    .await
    .unwrap();
    let tracker = klights_controllers::node_lease::NodeLeaseTracker::new_at(
        Utc.with_ymd_and_hms(2026, 5, 13, 6, 35, 0).unwrap(),
    );
    tracker
        .record_from_lease_object(
            "worker-a",
            &json!({
                "metadata": {"name": "worker-a", "namespace": "kube-node-lease"},
                "spec": {
                    "leaseDurationSeconds": 20,
                    "renewTime": "2026-05-13T06:35:10.000000Z"
                }
            }),
        )
        .await
        .unwrap();
    let rv_before_fresh = db.get_current_resource_version().await.unwrap();

    let next = reconcile_node_lifecycle_once_with_tracker_for_test(
        &db,
        &tracker,
        Utc.with_ymd_and_hms(2026, 5, 13, 6, 35, 20).unwrap(),
    )
    .await
    .unwrap();

    assert_eq!(
        db.get_current_resource_version().await.unwrap(),
        rv_before_fresh,
        "fresh in-memory heartbeats must not write cluster.db while Node status is unchanged"
    );
    assert_eq!(next, Some(std::time::Duration::from_secs(10)));

    reconcile_node_lifecycle_once_with_tracker_for_test(
        &db,
        &tracker,
        Utc.with_ymd_and_hms(2026, 5, 13, 6, 35, 34).unwrap(),
    )
    .await
    .unwrap();

    let node = db
        .get_resource("v1", "Node", None, "worker-a")
        .await
        .unwrap()
        .unwrap();
    let ready = node.data["status"]["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|condition| condition["type"] == "Ready")
        .unwrap();
    assert_eq!(ready["status"], "Unknown");
    assert_eq!(ready["reason"], "NodeStatusUnknown");
    assert!(
        node.resource_version > rv_before_fresh,
        "cluster.db should change only for the offline transition"
    );
}

#[tokio::test]
async fn newly_promoted_leader_grace_reset_prevents_mass_eviction() {
    // A long-running node that just became leader starts with an empty
    // in-memory tracker and an old startup_time. Without the promotion
    // grace-reset (T8) every unobserved node would look stale and be
    // evicted. With it, they get a fresh window and stay Ready.
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Node",
        None,
        "worker-a",
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a"},
            "status": {
                "conditions": [{
                    "type": "Ready",
                    "status": "True",
                    "reason": "KubeletReady",
                    "message": "klights is ready",
                    "lastHeartbeatTime": "2026-05-13T06:00:00Z",
                    "lastTransitionTime": "2026-05-13T06:00:00Z"
                }]
            }
        }),
    )
    .await
    .unwrap();
    seed_running_pod_on_node(&db, "worker-pod", "worker-pod-uid", "worker-a").await;

    let old_start = Utc.with_ymd_and_hms(2026, 5, 13, 6, 0, 0).unwrap();
    let tracker = klights_controllers::node_lease::NodeLeaseTracker::new_at(old_start);
    let now = Utc.with_ymd_and_hms(2026, 5, 13, 7, 0, 0).unwrap();

    // Precondition: without a reset the unobserved node already looks stale.
    assert!(
        tracker.deadline_for_node("worker-a").await.deadline <= now,
        "precondition: an old startup_time makes the unobserved node look stale"
    );

    // Promotion grace-reset, then reconcile.
    tracker.reset_grace_window(now).await;
    reconcile_node_lifecycle_once_with_tracker_for_test(&db, &tracker, now)
        .await
        .unwrap();

    let node = db
        .get_resource("v1", "Node", None, "worker-a")
        .await
        .unwrap()
        .unwrap();
    let ready = node.data["status"]["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|condition| condition["type"] == "Ready")
        .unwrap();
    assert_eq!(
        ready["status"], "True",
        "grace reset must keep unobserved nodes Ready right after promotion"
    );
    assert!(
        db.get_resource("v1", "Pod", Some("default"), "worker-pod")
            .await
            .unwrap()
            .is_some(),
        "grace reset must prevent eviction of pods on unobserved nodes after promotion"
    );
}

#[tokio::test]
async fn fresh_lease_promotes_startup_unknown_node_ready() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Node",
        None,
        "worker-a",
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a"},
            "status": {
                "conditions": [{
                    "type": "Ready",
                    "status": "Unknown",
                    "reason": "NodeStatusUnknown",
                    "message": "Kubelet stopped posting node status.",
                    "lastHeartbeatTime": "2026-05-13T06:34:14Z",
                    "lastTransitionTime": "2026-05-13T06:35:00Z"
                }]
            }
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "coordination.k8s.io/v1",
        "Lease",
        Some("kube-node-lease"),
        "worker-a",
        json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {"name": "worker-a", "namespace": "kube-node-lease"},
            "spec": {
                "holderIdentity": "worker-a",
                "leaseDurationSeconds": 120,
                "renewTime": "2026-05-13T06:35:10.000000Z"
            }
        }),
    )
    .await
    .unwrap();

    let next =
        reconcile_node_lifecycle_once(&db, Utc.with_ymd_and_hms(2026, 5, 13, 6, 35, 11).unwrap())
            .await
            .unwrap();

    let node = db
        .get_resource("v1", "Node", None, "worker-a")
        .await
        .unwrap()
        .unwrap();
    let ready = node.data["status"]["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|condition| condition["type"] == "Ready")
        .unwrap();
    assert_eq!(ready["status"], "True");
    assert_eq!(ready["reason"], "KubeletReady");
    assert!(
        ready.get("lastHeartbeatTime").is_none(),
        "Ready transition must not persist lastHeartbeatTime"
    );
    assert_eq!(ready["lastTransitionTime"], "2026-05-13T06:35:11Z");
    assert_eq!(
        next,
        Some(std::time::Duration::from_secs(
            (klights_controllers::node_lease::DEFAULT_NODE_LEASE_GRACE_SECONDS - 1) as u64
        ))
    );
}

#[tokio::test]
async fn node_status_transitions_do_not_store_last_heartbeat_time() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Node",
        None,
        "worker-a",
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a"},
            "status": {
                "conditions": [{
                    "type": "Ready",
                    "status": "True",
                    "reason": "KubeletReady",
                    "message": "klights is ready",
                    "lastTransitionTime": "2026-05-13T06:34:14Z"
                }]
            }
        }),
    )
    .await
    .unwrap();
    let tracker = klights_controllers::node_lease::NodeLeaseTracker::new_at(
        Utc.with_ymd_and_hms(2026, 5, 13, 6, 34, 0).unwrap(),
    );
    tracker
        .record_from_lease_object(
            "worker-a",
            &json!({
                "metadata": {"name": "worker-a", "namespace": "kube-node-lease"},
                "spec": {
                    "holderIdentity": "worker-a",
                    "leaseDurationSeconds": 10,
                    "renewTime": "2026-05-13T06:34:15.000000Z"
                }
            }),
        )
        .await
        .unwrap();

    reconcile_node_lifecycle_once_with_tracker_for_test(
        &db,
        &tracker,
        Utc.with_ymd_and_hms(2026, 5, 13, 6, 34, 26).unwrap(),
    )
    .await
    .unwrap();
    let unknown_node = db
        .get_resource("v1", "Node", None, "worker-a")
        .await
        .unwrap()
        .unwrap();
    let unknown_ready = unknown_node.data["status"]["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|condition| condition["type"] == "Ready")
        .unwrap();
    assert_eq!(unknown_ready["status"], "Unknown");
    assert!(
        unknown_ready.get("lastHeartbeatTime").is_none(),
        "Unknown transition must not persist lastHeartbeatTime"
    );

    tracker
        .record_from_lease_object(
            "worker-a",
            &json!({
                "metadata": {"name": "worker-a", "namespace": "kube-node-lease"},
                "spec": {
                    "holderIdentity": "worker-a",
                    "leaseDurationSeconds": 10,
                    "renewTime": "2026-05-13T06:34:30.000000Z"
                }
            }),
        )
        .await
        .unwrap();
    reconcile_node_lifecycle_once_with_tracker_for_test(
        &db,
        &tracker,
        Utc.with_ymd_and_hms(2026, 5, 13, 6, 34, 31).unwrap(),
    )
    .await
    .unwrap();
    let ready_node = db
        .get_resource("v1", "Node", None, "worker-a")
        .await
        .unwrap()
        .unwrap();
    let ready = ready_node.data["status"]["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|condition| condition["type"] == "Ready")
        .unwrap();
    assert_eq!(ready["status"], "True");
    assert!(
        ready.get("lastHeartbeatTime").is_none(),
        "Ready transition must not persist lastHeartbeatTime"
    );
}

#[tokio::test]
async fn fresh_lease_reconciles_unknown_bound_pods_from_cluster_status_after_worker_replay() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Node",
        None,
        "worker-a",
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a"},
            "status": {
                "conditions": [{
                    "type": "Ready",
                    "status": "Unknown",
                    "reason": "NodeStatusUnknown",
                    "message": "Kubelet stopped posting node status.",
                    "lastHeartbeatTime": "2026-05-13T06:34:14Z",
                    "lastTransitionTime": "2026-05-13T06:35:00Z"
                }]
            }
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "coordination.k8s.io/v1",
        "Lease",
        Some("kube-node-lease"),
        "worker-a",
        json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {"name": "worker-a", "namespace": "kube-node-lease"},
            "spec": {
                "holderIdentity": "worker-a",
                "leaseDurationSeconds": 120,
                "renewTime": "2026-05-13T06:35:10.000000Z"
            }
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "worker-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "worker-pod",
                "uid": "worker-pod-uid",
                "creationTimestamp": "2026-05-13T06:30:00Z"
            },
            "spec": {
                "nodeName": "worker-a",
                "containers": [{"name": "app", "image": "registry.k8s.io/pause:3.10"}]
            },
            "status": {
                "phase": "Unknown",
                "podIP": "10.42.0.10",
                "hostIP": "192.0.2.10",
                "conditions": [
                    {
                        "type": "PodScheduled",
                        "status": "True",
                        "lastTransitionTime": "2026-05-13T06:30:00Z"
                    },
                    {
                        "type": "Initialized",
                        "status": "True",
                        "lastTransitionTime": "2026-05-13T06:30:00Z"
                    },
                    {
                        "type": "ContainersReady",
                        "status": "Unknown",
                        "reason": "NodeStatusUnknown",
                        "message": "Kubelet stopped posting node status.",
                        "lastTransitionTime": "2026-05-13T06:35:00Z"
                    },
                    {
                        "type": "Ready",
                        "status": "Unknown",
                        "reason": "NodeStatusUnknown",
                        "message": "Kubelet stopped posting node status.",
                        "lastTransitionTime": "2026-05-13T06:35:00Z"
                    }
                ],
                "containerStatuses": [{
                    "name": "app",
                    "containerID": "containerd://worker-pod-app",
                    "ready": false,
                    "started": false,
                    "restartCount": 0,
                    "state": {"running": {"startedAt": "2026-05-13T06:30:10Z"}}
                }]
            }
        }),
    )
    .await
    .unwrap();

    reconcile_node_lifecycle_once(&db, Utc.with_ymd_and_hms(2026, 5, 13, 6, 35, 11).unwrap())
        .await
        .unwrap();

    let pod = db
        .get_resource("v1", "Pod", Some("default"), "worker-pod")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pod.data["status"]["phase"], "Running");
    assert_eq!(pod.data["status"]["podIP"], "10.42.0.10");
    assert_eq!(pod.data["status"]["hostIP"], "192.0.2.10");
    assert_eq!(pod.data["status"]["containerStatuses"][0]["ready"], true);
    assert_eq!(pod.data["status"]["containerStatuses"][0]["started"], true);
    assert_eq!(
        pod.data["status"]["containerStatuses"][0]["containerID"],
        "containerd://worker-pod-app"
    );
    assert_eq!(
        pod.data["status"]["containerStatuses"][0]["state"]["running"]["startedAt"],
        "2026-05-13T06:30:10Z"
    );

    let ready = pod_condition(&pod.data, "Ready");
    assert_eq!(ready["status"], "True");
    assert!(ready.get("reason").is_none());
    assert!(ready.get("message").is_none());
    let containers_ready = pod_condition(&pod.data, "ContainersReady");
    assert_eq!(containers_ready["status"], "True");
    assert!(containers_ready.get("reason").is_none());
    assert!(containers_ready.get("message").is_none());
}

#[tokio::test]
async fn fresh_lease_reconciles_unknown_pods_when_node_status_refresh_already_marked_ready() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Node",
        None,
        "worker-a",
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a"},
            "status": {
                "conditions": [{
                    "type": "Ready",
                    "status": "True",
                    "reason": "KubeletReady",
                    "message": "klights is ready",
                    "lastHeartbeatTime": "2026-05-13T06:35:08Z",
                    "lastTransitionTime": "2026-05-13T06:35:08Z"
                }]
            }
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "coordination.k8s.io/v1",
        "Lease",
        Some("kube-node-lease"),
        "worker-a",
        json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {"name": "worker-a", "namespace": "kube-node-lease"},
            "spec": {
                "holderIdentity": "worker-a",
                "leaseDurationSeconds": 120,
                "renewTime": "2026-05-13T06:35:10.000000Z"
            }
        }),
    )
    .await
    .unwrap();
    seed_running_pod_on_node(&db, "worker-pod", "worker-pod-uid", "worker-a").await;
    let pod = db
        .get_resource("v1", "Pod", Some("default"), "worker-pod")
        .await
        .unwrap()
        .unwrap();
    let mut pod_data = pod.data.as_ref().clone();
    pod_data["status"]["phase"] = json!("Unknown");
    pod_data["status"]["reason"] = json!("NodeStatusUnknown");
    for condition in pod_data["status"]["conditions"]
        .as_array_mut()
        .expect("Pod conditions")
    {
        if matches!(
            condition["type"].as_str(),
            Some("Ready" | "ContainersReady")
        ) {
            condition["status"] = json!("Unknown");
            condition["reason"] = json!("NodeStatusUnknown");
            condition["message"] = json!("Kubelet stopped posting node status.");
            condition["lastTransitionTime"] = json!("2026-05-13T06:35:00Z");
        }
    }
    db.update_status_only_with_preconditions(
        "v1",
        "Pod",
        Some("default"),
        "worker-pod",
        pod_data["status"].clone(),
        klights_cluster_core::ResourcePreconditions::from_resource(&pod),
    )
    .await
    .unwrap();

    reconcile_node_lifecycle_once(&db, Utc.with_ymd_and_hms(2026, 5, 13, 6, 35, 11).unwrap())
        .await
        .unwrap();

    let pod = db
        .get_resource("v1", "Pod", Some("default"), "worker-pod")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        pod.data["status"]["phase"], "Running",
        "fresh Lease should trigger NodeResourceReconcile even if a Node status refresh already flipped Ready"
    );
    assert_eq!(pod_condition(&pod.data, "Ready")["status"], "True");
    assert_eq!(
        pod_condition(&pod.data, "ContainersReady")["status"],
        "True"
    );
}

#[tokio::test]
async fn leader_startup_ignores_preexisting_fresh_lease_until_renewed() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Node",
        None,
        "worker-a",
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a"},
            "status": {
                "conditions": [{
                    "type": "Ready",
                    "status": "Unknown",
                    "reason": "NodeStatusUnknown",
                    "message": "Kubelet stopped posting node status.",
                    "lastHeartbeatTime": "2026-05-13T06:34:14Z",
                    "lastTransitionTime": "2026-05-13T06:35:00Z"
                }]
            }
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "coordination.k8s.io/v1",
        "Lease",
        Some("kube-node-lease"),
        "worker-a",
        json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {"name": "worker-a", "namespace": "kube-node-lease"},
            "spec": {
                "holderIdentity": "worker-a",
                "leaseDurationSeconds": 120,
                "renewTime": "2026-05-13T06:35:10.000000Z"
            }
        }),
    )
    .await
    .unwrap();
    let startup_rv = db.get_current_resource_version().await.unwrap();

    let next = reconcile_node_lifecycle_once_after_startup(
        &db,
        Utc.with_ymd_and_hms(2026, 5, 13, 6, 35, 11).unwrap(),
        startup_rv,
    )
    .await
    .unwrap();

    let node = db
        .get_resource("v1", "Node", None, "worker-a")
        .await
        .unwrap()
        .unwrap();
    let ready = node.data["status"]["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|condition| condition["type"] == "Ready")
        .unwrap();
    assert_eq!(
        ready["status"], "Unknown",
        "a persisted pre-start Lease is not proof that the worker synced with this leader"
    );
    assert_eq!(
        next,
        Some(std::time::Duration::from_secs(
            klights_cluster_core::DEFAULT_NODE_LEASE_DURATION_SECONDS as u64
        )),
        "already-unknown pre-start leases should wait through startup grace for the worker's next heartbeat"
    );
}
