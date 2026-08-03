use crate::node_lease::{DEFAULT_NODE_LEASE_GRACE_SECONDS, NodeLeaseTracker};
use crate::node_lifecycle::*;
use chrono::{TimeZone, Utc};
use klights_cluster_core::{Resource, ResourcePreconditions, StorageCommand};
use klights_leader_api::{ResourceEvent, WatchEventType};
use klights_reconcile_api::{ControllerStoreError, ControllerStoreResult};
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

type ResourceKey = (String, String, Option<String>, String);

#[derive(Clone, Default)]
struct MemoryNodeLifecycleRuntime {
    resources: Arc<Mutex<BTreeMap<ResourceKey, Resource>>>,
    next_resource_version: Arc<AtomicI64>,
}

impl MemoryNodeLifecycleRuntime {
    fn key(api_version: &str, kind: &str, namespace: Option<&str>, name: &str) -> ResourceKey {
        (
            api_version.to_string(),
            kind.to_string(),
            namespace.map(str::to_string),
            name.to_string(),
        )
    }

    fn store(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        mut value: serde_json::Value,
    ) -> Resource {
        let resource_version = self.next_resource_version.fetch_add(1, Ordering::Relaxed) + 1;
        if value
            .pointer("/metadata/uid")
            .and_then(|value| value.as_str())
            .is_none_or(str::is_empty)
        {
            value["metadata"]["uid"] = json!(format!("{kind}-{name}-uid"));
        }
        value["metadata"]["resourceVersion"] = json!(resource_version.to_string());
        let resource = Resource::try_from_data(Arc::new(value)).expect("valid lifecycle resource");
        self.resources.lock().unwrap().insert(
            Self::key(api_version, kind, namespace, name),
            resource.clone(),
        );
        resource
    }

    async fn create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
    ) -> ControllerStoreResult<Resource> {
        Ok(self.store(api_version, kind, namespace, name, value))
    }

    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        Ok(self
            .resources
            .lock()
            .unwrap()
            .get(&Self::key(api_version, kind, namespace, name))
            .cloned())
    }

    async fn get_current_resource_version(&self) -> ControllerStoreResult<i64> {
        Ok(self.next_resource_version.load(Ordering::Relaxed))
    }

    async fn update_status_only_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> ControllerStoreResult<Resource> {
        let current = self
            .get_resource(api_version, kind, namespace, name)
            .await?
            .expect("lifecycle status target");
        if preconditions.resource_version != Some(current.resource_version)
            || preconditions.uid.as_deref() != Some(current.uid.as_str())
        {
            return Err(ControllerStoreError::conflict("stale lifecycle status"));
        }
        let mut value = (*current.data).clone();
        value["status"] = status;
        Ok(self.store(api_version, kind, namespace, name, value))
    }
}

#[async_trait::async_trait]
impl NodeLifecycleStore for MemoryNodeLifecycleRuntime {
    async fn list_nodes(&self) -> ControllerStoreResult<Vec<Resource>> {
        Ok(self
            .resources
            .lock()
            .unwrap()
            .iter()
            .filter(|((api_version, kind, namespace, _), _)| {
                api_version == "v1" && kind == "Node" && namespace.is_none()
            })
            .map(|(_, resource)| resource.clone())
            .collect())
    }

    async fn list_node_leases(&self) -> ControllerStoreResult<Vec<Resource>> {
        Ok(self
            .resources
            .lock()
            .unwrap()
            .iter()
            .filter(|((api_version, kind, namespace, _), _)| {
                api_version == "coordination.k8s.io/v1"
                    && kind == "Lease"
                    && namespace.as_deref() == Some("kube-node-lease")
            })
            .map(|(_, resource)| resource.clone())
            .collect())
    }
}

#[async_trait::async_trait]
impl NodeLifecyclePodStore for MemoryNodeLifecycleRuntime {
    async fn list_pods_bound_to_node(
        &self,
        node_name: &str,
    ) -> ControllerStoreResult<Vec<Resource>> {
        Ok(self
            .resources
            .lock()
            .unwrap()
            .iter()
            .filter(|((api_version, kind, _, _), resource)| {
                api_version == "v1"
                    && kind == "Pod"
                    && resource
                        .data
                        .pointer("/spec/nodeName")
                        .and_then(|v| v.as_str())
                        == Some(node_name)
            })
            .map(|(_, resource)| resource.clone())
            .collect())
    }

    async fn replace_pod_status_for_uid(
        &self,
        pod: &Resource,
        status: serde_json::Value,
    ) -> ControllerStoreResult<Resource> {
        self.update_status_only_with_preconditions(
            "v1",
            "Pod",
            pod.namespace.as_deref(),
            &pod.name,
            status,
            ResourcePreconditions::from_resource(pod),
        )
        .await
    }
}

struct TestNodeLifecycleStatus<'a>(&'a MemoryNodeLifecycleRuntime);

impl klights_leader_api::LeaderNodeLifecycleStatus for TestNodeLifecycleStatus<'_> {
    fn submit_node_lifecycle_status(
        &self,
        request: klights_leader_api::NodeLifecycleStatusRequest,
    ) -> klights_leader_api::NodeLifecycleStatusFuture<
        '_,
        klights_leader_api::NodeLifecycleStatusResult,
    > {
        Box::pin(async move {
            let StorageCommand::UpdateStatus {
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

async fn reconcile_node_lifecycle_once_with_tracker_and_grace(
    db: &MemoryNodeLifecycleRuntime,
    tracker: &NodeLeaseTracker,
    now: chrono::DateTime<chrono::Utc>,
    eviction_grace: std::time::Duration,
) -> anyhow::Result<Option<std::time::Duration>> {
    reconcile_node_lifecycle_once_with_tracker(
        db,
        &TestNodeLifecycleStatus(db),
        db,
        tracker,
        now,
        NodeLifecyclePodActions {
            mutation_reconcile: None,
            lifecycle: None,
            eviction_grace,
        },
    )
    .await
}

async fn reconcile_node_lifecycle_once_with_tracker_for_test(
    db: &MemoryNodeLifecycleRuntime,
    tracker: &NodeLeaseTracker,
    now: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<Option<std::time::Duration>> {
    reconcile_node_lifecycle_once_with_tracker_and_grace(
        db,
        tracker,
        now,
        std::time::Duration::ZERO,
    )
    .await
}

async fn reconcile_node_lifecycle_once_with_grace(
    db: &MemoryNodeLifecycleRuntime,
    now: chrono::DateTime<chrono::Utc>,
    eviction_grace: std::time::Duration,
) -> anyhow::Result<Option<std::time::Duration>> {
    let tracker = NodeLeaseTracker::new_at(now);
    refresh_node_lease_tracker_from_cluster_leases(db, &tracker).await?;
    reconcile_node_lifecycle_once_with_tracker_and_grace(db, &tracker, now, eviction_grace).await
}

async fn reconcile_node_lifecycle_once(
    db: &MemoryNodeLifecycleRuntime,
    now: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<Option<std::time::Duration>> {
    reconcile_node_lifecycle_once_with_grace(db, now, std::time::Duration::ZERO).await
}

async fn reconcile_node_lifecycle_once_after_startup(
    db: &MemoryNodeLifecycleRuntime,
    now: chrono::DateTime<chrono::Utc>,
    _startup_resource_version: i64,
) -> anyhow::Result<Option<std::time::Duration>> {
    reconcile_node_lifecycle_once_with_tracker_for_test(db, &NodeLeaseTracker::new_at(now), now)
        .await
}

fn resource_event(event_type: WatchEventType, value: serde_json::Value) -> ResourceEvent {
    let resource = Resource::try_from_data(Arc::new(value)).unwrap();
    ResourceEvent::try_new(event_type, resource, None).unwrap()
}

#[derive(Default)]
struct RecordingNodeLostSink(Mutex<Vec<(String, String, String)>>);

#[async_trait::async_trait]
impl NodeLostPodLifecycleSink for RecordingNodeLostSink {
    async fn enqueue_node_lost_cleanup(&self, pod: Resource) -> ControllerStoreResult<()> {
        self.0.lock().unwrap().push((
            pod.namespace.as_deref().unwrap_or("default").to_string(),
            pod.name.clone(),
            pod.uid.clone(),
        ));
        Ok(())
    }
}

#[derive(Default)]
struct RecordingPodMutationSink(Mutex<Vec<klights_reconcile_api::PodMutationReconcileRequest>>);

impl klights_reconcile_api::PodMutationReconcileSink for RecordingPodMutationSink {
    fn reconcile_pod_mutation(
        &self,
        request: klights_reconcile_api::PodMutationReconcileRequest,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'_> {
        self.0.lock().unwrap().push(request);
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn track_lease_from_event_updates_tracker() {
    let tracker = NodeLeaseTracker::new_at(Utc.with_ymd_and_hms(2026, 5, 13, 6, 30, 0).unwrap());
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

    track_lease_from_event(&event, &tracker)
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
    let tracker = NodeLeaseTracker::new_at(Utc.with_ymd_and_hms(2026, 5, 13, 6, 30, 0).unwrap());
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

    track_lease_from_event(&event, &tracker)
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
    let db = MemoryNodeLifecycleRuntime::default();
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
    let db = MemoryNodeLifecycleRuntime::default();
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

    reconcile_node_lifecycle_once_with_grace(
        &db,
        Utc.with_ymd_and_hms(2026, 5, 13, 6, 34, 56).unwrap(),
        std::time::Duration::from_secs(30),
    )
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
    let db = MemoryNodeLifecycleRuntime::default();
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
    let db = MemoryNodeLifecycleRuntime::default();
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
    let db = MemoryNodeLifecycleRuntime::default();
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

    reconcile_node_lifecycle_once_with_grace(
        &db,
        Utc.with_ymd_and_hms(2026, 5, 13, 6, 34, 56).unwrap(),
        std::time::Duration::from_secs(30),
    )
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

    reconcile_node_lifecycle_once_with_grace(
        &db,
        Utc.with_ymd_and_hms(2026, 5, 13, 6, 35, 26).unwrap(),
        std::time::Duration::from_secs(30),
    )
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
    let db = MemoryNodeLifecycleRuntime::default();
    let lifecycle = RecordingNodeLostSink::default();
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

    let cleaned = cleanup_pods_bound_to_deleted_node_event(
        &db,
        &db,
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

    assert!(
        lifecycle.0.lock().unwrap().iter().any(|key| key
            == &(
                "default".to_string(),
                "fake-node-pod".to_string(),
                "fake-node-pod-uid".to_string(),
            )),
        "deleted-node cleanup must wake the UID-bound lifecycle actor"
    );
}

#[tokio::test]
async fn node_lost_cleanup_enqueues_owning_replicaset_after_node_lost_mark() {
    // Uses the staged within-grace -> after-grace timing, so it pins a
    // non-zero eviction grace (the default is now 0 = immediate cleanup).
    let db = MemoryNodeLifecycleRuntime::default();
    let mutation_reconcile = RecordingPodMutationSink::default();
    let tracker = NodeLeaseTracker::new_at(Utc.with_ymd_and_hms(2026, 5, 13, 6, 34, 14).unwrap());

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
    tracker
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
    db.create_resource(
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
    db.create_resource(
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

    reconcile_node_lifecycle_once_with_tracker(
        &db,
        &TestNodeLifecycleStatus(&db),
        &db,
        &tracker,
        Utc.with_ymd_and_hms(2026, 5, 13, 6, 34, 56).unwrap(),
        NodeLifecyclePodActions {
            mutation_reconcile: Some(&mutation_reconcile),
            lifecycle: None,
            eviction_grace: std::time::Duration::ZERO,
        },
    )
    .await
    .unwrap();

    let lost_pod = db
        .get_resource("v1", "Pod", Some("default"), "lost-pod")
        .await
        .unwrap()
        .expect("controller must preserve the Pod row for actor-owned finalization");
    assert_eq!(lost_pod.data["status"]["phase"], "Failed");
    assert_eq!(lost_pod.data["status"]["reason"], "NodeLost");
    let requests = mutation_reconcile.0.lock().unwrap();
    assert!(
        requests.iter().any(|request| {
            let klights_reconcile_api::PodMutationReconcileRequest::RunHooks { pod, .. } = request
            else {
                return false;
            };
            pod.data
                .pointer("/metadata/ownerReferences/0/name")
                .and_then(|value| value.as_str())
                == Some("owned-rs")
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
            node_lifecycle_retry_delay(attempt),
            std::time::Duration::from_secs(seconds),
            "attempt {attempt}"
        );
    }
}

async fn seed_running_pod_on_node(
    db: &MemoryNodeLifecycleRuntime,
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
    let db = MemoryNodeLifecycleRuntime::default();
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
    let tracker = NodeLeaseTracker::new_at(Utc.with_ymd_and_hms(2026, 5, 13, 6, 35, 0).unwrap());
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
    let db = MemoryNodeLifecycleRuntime::default();
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
    let tracker = NodeLeaseTracker::new_at(old_start);
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
    let db = MemoryNodeLifecycleRuntime::default();
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
            (DEFAULT_NODE_LEASE_GRACE_SECONDS - 1) as u64
        ))
    );
}

#[tokio::test]
async fn node_status_transitions_do_not_store_last_heartbeat_time() {
    let db = MemoryNodeLifecycleRuntime::default();
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
    let tracker = NodeLeaseTracker::new_at(Utc.with_ymd_and_hms(2026, 5, 13, 6, 34, 0).unwrap());
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
    let db = MemoryNodeLifecycleRuntime::default();
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
    let db = MemoryNodeLifecycleRuntime::default();
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
    let db = MemoryNodeLifecycleRuntime::default();
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
