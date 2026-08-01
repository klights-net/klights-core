use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use crate::node_lease::{DEFAULT_NODE_LEASE_GRACE_SECONDS, NodeLeaseObservation, NodeLeaseTracker};
use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use klights_cluster_core::k8s_time::format_time as k8s_time_format;
use klights_cluster_core::{Resource, ResourcePreconditions, StorageCommand};
use klights_leader_api::{ResourceEvent, WatchEventType};
use klights_reconcile_api::ControllerStoreResult;
use serde_json::{Value, json};

const POD_CLEANUP_REASON_NODE_LOST: &str = "NodeLost";

#[async_trait]
pub trait NodeLifecycleStore: Send + Sync {
    async fn list_nodes(&self) -> ControllerStoreResult<Vec<Resource>>;
    async fn list_node_leases(&self) -> ControllerStoreResult<Vec<Resource>>;
}

#[async_trait]
pub trait NodeLifecyclePodStore: Send + Sync {
    async fn list_pods_bound_to_node(
        &self,
        node_name: &str,
    ) -> ControllerStoreResult<Vec<Resource>>;
    async fn replace_pod_status_for_uid(
        &self,
        pod: &Resource,
        status: Value,
    ) -> ControllerStoreResult<Resource>;
}

#[async_trait]
pub trait NodeLostPodLifecycleSink: Send + Sync {
    async fn enqueue_node_lost_cleanup(&self, pod: Resource) -> ControllerStoreResult<()>;
}

const NODE_STATUS_UNKNOWN_REASON: &str = "NodeStatusUnknown";
const NODE_STATUS_UNKNOWN_MESSAGE: &str = "Kubelet stopped posting node status.";
const NODE_READY_REASON: &str = "KubeletReady";
const NODE_READY_MESSAGE: &str = "klights is ready";
// Default 0: once a node is confirmed Unknown (after ~24s of confirmed lease
// silence, T3), its pods are marked Unknown and cleaned up immediately — no
// extra wait. Cleanup still flows through the UID-bound actor finalization
// (HR #11). Operators can restore a delay via the env var above.
//
// Deliberate deviations from upstream: (a) ignores per-pod tolerationSeconds
// for node.kubernetes.io/unreachable (acceptable because eviction only fires
// after confirmed silence, not on a transient blip); (b) a partitioned-but-
// alive node could have its pods rescheduled while it still runs them until it
// sheds leadership/membership — mitigated by the 24s detection, not eliminated.

#[derive(Clone, Copy)]
pub struct NodeLifecyclePodActions<'a> {
    pub mutation_reconcile: Option<&'a dyn klights_reconcile_api::PodMutationReconcileSink>,
    pub lifecycle: Option<&'a dyn NodeLostPodLifecycleSink>,
    pub eviction_grace: Duration,
}

impl NodeLifecyclePodActions<'_> {}

pub async fn reconcile_node_lifecycle_once_with_tracker(
    db: &(impl NodeLifecycleStore + ?Sized),
    node_status: &dyn klights_leader_api::LeaderNodeLifecycleStatus,
    pod_repository: &(impl NodeLifecyclePodStore + ?Sized),
    node_lease_tracker: &NodeLeaseTracker,
    now: DateTime<Utc>,
    pod_actions: NodeLifecyclePodActions<'_>,
) -> Result<Option<Duration>> {
    let NodeLifecyclePodActions {
        mutation_reconcile: side_effects,
        lifecycle: pod_lifecycle_router,
        eviction_grace: pod_eviction_grace,
    } = pod_actions;
    let nodes = db.list_nodes().await?;
    let mut next_deadline: Option<Duration> = None;

    for node in nodes {
        let lease_deadline = node_lease_tracker.deadline_for_node(&node.name).await;
        let deadline = lease_deadline.deadline;
        let observed = lease_deadline.observed.as_ref();

        let mut data = Arc::unwrap_or_clone(node.data.clone());
        let lease_fresh = deadline > now;
        let status_heartbeat_deadline = ready_status_heartbeat_deadline(&data);
        let status_heartbeat_fresh =
            status_heartbeat_deadline.is_some_and(|deadline| deadline > now);
        let stale = !lease_fresh && !status_heartbeat_fresh;
        let mut should_reconcile_ready_resources = false;

        let changed = if stale {
            mark_node_ready_unknown(&mut data, now)
        } else {
            if lease_fresh && let Ok(remaining) = deadline.signed_duration_since(now).to_std() {
                next_deadline =
                    Some(next_deadline.map_or(remaining, |current| current.min(remaining)));
            }
            if let Some(deadline) = status_heartbeat_deadline
                && deadline > now
                && let Ok(remaining) = deadline.signed_duration_since(now).to_std()
            {
                next_deadline =
                    Some(next_deadline.map_or(remaining, |current| current.min(remaining)));
            }
            let ready_transition = lease_fresh
                .then_some(observed)
                .flatten()
                .map(|lease| mark_node_ready_from_fresh_observation(&mut data, lease, now))
                .unwrap_or(false);
            should_reconcile_ready_resources = (lease_fresh && observed.is_some()
                || status_heartbeat_fresh)
                && (ready_transition || node_ready_condition_true(&data));
            ready_transition
        };

        if changed {
            update_node_status(node_status, &node, data).await?;
        }
        if stale {
            merge_deadline(
                &mut next_deadline,
                mark_pods_unknown_on_node(
                    db,
                    pod_repository,
                    side_effects,
                    pod_lifecycle_router,
                    &node.name,
                    pod_eviction_grace,
                    now,
                )
                .await?,
            );
        } else if should_reconcile_ready_resources {
            reconcile_node_resources_after_ready(pod_repository, &node.name, now).await?;
        }
    }

    Ok(next_deadline)
}

pub async fn cleanup_pods_bound_to_deleted_node_event(
    db: &(impl NodeLifecycleStore + ?Sized),
    pod_repository: &(impl NodeLifecyclePodStore + ?Sized),
    side_effects: Option<&dyn klights_reconcile_api::PodMutationReconcileSink>,
    pod_lifecycle_router: Option<&dyn NodeLostPodLifecycleSink>,
    event: &ResourceEvent,
    now: DateTime<Utc>,
) -> Result<bool> {
    let Some(node_name) = deleted_node_name(event) else {
        return Ok(false);
    };
    cleanup_pods_bound_to_deleted_node(
        db,
        pod_repository,
        side_effects,
        pod_lifecycle_router,
        node_name,
        now,
    )
    .await?;
    Ok(true)
}

fn deleted_node_name(event: &ResourceEvent) -> Option<&str> {
    if event.event_type() != WatchEventType::Deleted {
        return None;
    }
    if event.resource().api_version != "v1" || event.resource().kind != "Node" {
        return None;
    }
    event
        .resource()
        .data
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .filter(|name| !name.trim().is_empty())
}

async fn cleanup_pods_bound_to_deleted_node(
    db: &(impl NodeLifecycleStore + ?Sized),
    pod_repository: &(impl NodeLifecyclePodStore + ?Sized),
    side_effects: Option<&dyn klights_reconcile_api::PodMutationReconcileSink>,
    pod_lifecycle_router: Option<&dyn NodeLostPodLifecycleSink>,
    node_name: &str,
    now: DateTime<Utc>,
) -> Result<()> {
    let pods = pod_repository.list_pods_bound_to_node(node_name).await?;
    for pod in pods {
        mark_pod_node_lost_and_enqueue_actor_cleanup(
            db,
            pod_repository,
            side_effects,
            pod_lifecycle_router,
            node_name,
            pod,
            now,
        )
        .await?;
    }
    Ok(())
}

pub async fn refresh_node_lease_tracker_from_cluster_leases(
    db: &(impl NodeLifecycleStore + ?Sized),
    tracker: &NodeLeaseTracker,
) -> Result<()> {
    let leases = db.list_node_leases().await?;

    for lease in leases {
        if let Err(err) = tracker
            .record_from_lease_object(&lease.name, lease.data.as_ref())
            .await
        {
            tracing::warn!(
                node_name = %lease.name,
                "node_lifecycle: failed to seed lease tracker from persisted lease: {err:#}"
            );
        }
    }

    Ok(())
}

pub async fn track_lease_from_event(
    event: &ResourceEvent,
    tracker: &NodeLeaseTracker,
) -> Result<()> {
    if event.event_type() == WatchEventType::Bookmark
        || event.event_type() == WatchEventType::Deleted
    {
        return Ok(());
    }

    if event.resource().kind != "Lease" {
        return Ok(());
    }
    if event
        .resource()
        .data
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        != Some("kube-node-lease")
    {
        return Ok(());
    }

    let node_name = event
        .resource()
        .data
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if node_name.is_empty() {
        return Ok(());
    }

    tracker
        .record_from_lease_object(node_name, event.resource().data.as_ref())
        .await
        .map(|_| ())
}

pub fn node_lifecycle_retry_delay(attempt: u32) -> Duration {
    let steps = attempt.saturating_add(1).min(12);
    Duration::from_secs((steps * 5) as u64)
}

async fn update_node_status(
    node_status: &dyn klights_leader_api::LeaderNodeLifecycleStatus,
    node: &Resource,
    data: Value,
) -> Result<()> {
    let status = data.get("status").cloned().unwrap_or_else(|| json!({}));
    let request =
        klights_leader_api::NodeLifecycleStatusRequest::try_new(StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Node".to_string(),
            namespace: None,
            name: node.name.clone(),
            status,
            expected_rv: Some(node.resource_version),
            preconditions: ResourcePreconditions::uid_and_resource_version(
                node.uid.clone(),
                node.resource_version,
            ),
            observed_status_stamp: None,
        })?;
    node_status.submit_node_lifecycle_status(request).await?;
    Ok(())
}

async fn mark_pods_unknown_on_node(
    db: &(impl NodeLifecycleStore + ?Sized),
    pod_repository: &(impl NodeLifecyclePodStore + ?Sized),
    side_effects: Option<&dyn klights_reconcile_api::PodMutationReconcileSink>,
    pod_lifecycle_router: Option<&dyn NodeLostPodLifecycleSink>,
    node_name: &str,
    pod_eviction_grace: Duration,
    now: DateTime<Utc>,
) -> Result<Option<Duration>> {
    let pods = pod_repository.list_pods_bound_to_node(node_name).await?;
    let mut next_deadline = None;
    for pod in pods {
        let mut data = Arc::unwrap_or_clone(pod.data.clone());
        let status_changed = mark_pod_status_unknown(&mut data, now);
        if pod.data.pointer("/metadata/deletionTimestamp").is_none() {
            match stale_node_pod_terminal_deadline(&data, pod_eviction_grace) {
                Some(deadline) if deadline <= now => {
                    mark_pod_node_lost_and_enqueue_actor_cleanup(
                        db,
                        pod_repository,
                        side_effects,
                        pod_lifecycle_router,
                        node_name,
                        pod,
                        now,
                    )
                    .await?;
                    continue;
                }
                Some(deadline) => {
                    if let Ok(remaining) = deadline.signed_duration_since(now).to_std() {
                        merge_deadline(&mut next_deadline, Some(remaining));
                    }
                }
                None => {}
            }
        }
        if status_changed {
            let status = data.get("status").cloned().unwrap_or_else(|| json!({}));
            pod_repository
                .replace_pod_status_for_uid(&pod, status)
                .await?;
        }
    }
    Ok(next_deadline)
}

async fn mark_pod_node_lost_and_enqueue_actor_cleanup(
    _db: &(impl NodeLifecycleStore + ?Sized),
    pod_repository: &(impl NodeLifecyclePodStore + ?Sized),
    side_effects: Option<&dyn klights_reconcile_api::PodMutationReconcileSink>,
    pod_lifecycle_router: Option<&dyn NodeLostPodLifecycleSink>,
    node_name: &str,
    pod: Resource,
    now: DateTime<Utc>,
) -> Result<()> {
    let namespace = pod.namespace.as_deref().unwrap_or("default");
    let mut data = Arc::unwrap_or_clone(pod.data.clone());
    mark_pod_status_node_lost(&mut data, now);
    let status = data.get("status").cloned().unwrap_or_else(|| json!({}));
    pod_repository
        .replace_pod_status_for_uid(&pod, status)
        .await
        .with_context(|| {
            format!(
                "mark NodeLost Pod status for {}/{} uid={}",
                namespace, pod.name, pod.uid
            )
        })?;

    run_node_lost_pod_cleanup_side_effects(side_effects, &data).await?;

    if let Some(lifecycle_sink) = pod_lifecycle_router {
        lifecycle_sink
            .enqueue_node_lost_cleanup(pod.clone())
            .await
            .with_context(|| {
                format!(
                    "enqueue actor-owned NodeLost cleanup for {}/{} uid={}",
                    namespace, pod.name, pod.uid
                )
            })?;
    } else {
        tracing::debug!(
            node = node_name,
            namespace,
            pod = %pod.name,
            uid = %pod.uid,
            "NodeLost Pod marked without local lifecycle router; watch delivery or later startup will drive actor cleanup"
        );
    }

    Ok(())
}

async fn run_node_lost_pod_cleanup_side_effects(
    mutation_reconcile: Option<&dyn klights_reconcile_api::PodMutationReconcileSink>,
    pod_data: &Value,
) -> Result<()> {
    let Some(mutation_reconcile) = mutation_reconcile else {
        return Ok(());
    };
    let pod = Resource::try_from_data(std::sync::Arc::new(pod_data.clone()))
        .map_err(|error| anyhow::anyhow!("invalid cleaned-up Pod identity: {error}"))?;
    mutation_reconcile
        .reconcile_pod_mutation(
            klights_reconcile_api::PodMutationReconcileRequest::ServicesAfterDelete {
                deleted: pod.clone(),
            },
        )
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    mutation_reconcile
        .reconcile_pod_mutation(
            klights_reconcile_api::PodMutationReconcileRequest::RunHooks {
                pod,
                named_hook: None,
                context: "node_lost_pod_cleanup",
            },
        )
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    Ok(())
}

fn merge_deadline(next_deadline: &mut Option<Duration>, candidate: Option<Duration>) {
    if let Some(candidate) = candidate {
        *next_deadline = Some(next_deadline.map_or(candidate, |current| current.min(candidate)));
    }
}

async fn reconcile_node_resources_after_ready(
    pod_repository: &(impl NodeLifecyclePodStore + ?Sized),
    node_name: &str,
    now: DateTime<Utc>,
) -> Result<()> {
    reconcile_unknown_pods_after_node_ready(pod_repository, node_name, now).await
}

async fn reconcile_unknown_pods_after_node_ready(
    pod_repository: &(impl NodeLifecyclePodStore + ?Sized),
    node_name: &str,
    now: DateTime<Utc>,
) -> Result<()> {
    let pods = pod_repository.list_pods_bound_to_node(node_name).await?;
    for pod in pods {
        let mut data = Arc::unwrap_or_clone(pod.data.clone());
        if !restore_pod_status_after_node_ready(&mut data, now) {
            continue;
        }
        let status = data.get("status").cloned().unwrap_or_else(|| json!({}));
        pod_repository
            .replace_pod_status_for_uid(&pod, status)
            .await?;
    }
    Ok(())
}

pub fn node_lifecycle_event(event: &ResourceEvent) -> bool {
    if event.event_type() == WatchEventType::Bookmark {
        return false;
    }
    match (
        event.resource().api_version.as_str(),
        event.resource().kind.as_str(),
    ) {
        ("v1", "Node") => true,
        ("coordination.k8s.io/v1", "Lease") => {
            event
                .resource()
                .data
                .pointer("/metadata/namespace")
                .and_then(|v| v.as_str())
                == Some("kube-node-lease")
        }
        _ => false,
    }
}

fn mark_node_ready_unknown(node: &mut Value, now: DateTime<Utc>) -> bool {
    let now = k8s_time_format(now);
    let Some(conditions) = node_conditions_mut(node) else {
        return false;
    };
    if let Some(condition) = conditions.iter_mut().find(|c| c["type"] == "Ready") {
        if condition.get("status").and_then(|v| v.as_str()) == Some("Unknown")
            && condition.get("reason").and_then(|v| v.as_str()) == Some(NODE_STATUS_UNKNOWN_REASON)
        {
            return false;
        }
        let previous = condition.clone();
        condition["status"] = json!("Unknown");
        condition["reason"] = json!(NODE_STATUS_UNKNOWN_REASON);
        condition["message"] = json!(NODE_STATUS_UNKNOWN_MESSAGE);
        remove_condition_field(condition, "lastHeartbeatTime");
        crate::common::preserve_condition_transition_time(condition, Some(&previous), &now);
        return true;
    }

    let mut condition = json!({
        "type": "Ready",
        "status": "Unknown",
        "reason": NODE_STATUS_UNKNOWN_REASON,
        "message": NODE_STATUS_UNKNOWN_MESSAGE
    });
    crate::common::preserve_condition_transition_time(&mut condition, None, &now);
    conditions.push(condition);
    true
}

fn mark_pod_status_unknown(pod: &mut Value, now: DateTime<Utc>) -> bool {
    if matches!(
        pod.pointer("/status/phase").and_then(|v| v.as_str()),
        Some("Succeeded" | "Failed")
    ) {
        return false;
    }

    let now = k8s_time_format(now);
    let status = ensure_object_field(pod, "status");
    let mut changed = false;
    if status.get("phase").and_then(|v| v.as_str()) != Some("Unknown") {
        status.insert("phase".to_string(), json!("Unknown"));
        changed = true;
    }

    let conditions = status
        .entry("conditions".to_string())
        .or_insert_with(|| json!([]));
    if !conditions.is_array() {
        *conditions = json!([]);
        changed = true;
    }
    let Some(conditions) = conditions.as_array_mut() else {
        return changed;
    };
    for condition_type in ["ContainersReady", "Ready"] {
        if mark_pod_condition_unknown(conditions, condition_type, &now) {
            changed = true;
        }
    }
    changed
}

fn ensure_object_field<'a>(
    value: &'a mut Value,
    field: &str,
) -> &'a mut serde_json::Map<String, Value> {
    if !value.is_object() {
        *value = json!({});
    }
    let obj = value.as_object_mut().expect("object ensured");
    let entry = obj.entry(field.to_string()).or_insert_with(|| json!({}));
    if !entry.is_object() {
        *entry = json!({});
    }
    entry.as_object_mut().expect("object field ensured")
}

fn mark_pod_condition_unknown(
    conditions: &mut Vec<Value>,
    condition_type: &str,
    now: &str,
) -> bool {
    if let Some(condition) = conditions
        .iter_mut()
        .find(|condition| condition.get("type").and_then(|v| v.as_str()) == Some(condition_type))
    {
        if condition.get("status").and_then(|v| v.as_str()) == Some("Unknown")
            && condition.get("reason").and_then(|v| v.as_str()) == Some(NODE_STATUS_UNKNOWN_REASON)
        {
            return false;
        }
        let previous = condition.clone();
        condition["status"] = json!("Unknown");
        condition["reason"] = json!(NODE_STATUS_UNKNOWN_REASON);
        condition["message"] = json!(NODE_STATUS_UNKNOWN_MESSAGE);
        crate::common::preserve_condition_transition_time(condition, Some(&previous), now);
        return true;
    }

    let mut condition = json!({
        "type": condition_type,
        "status": "Unknown",
        "reason": NODE_STATUS_UNKNOWN_REASON,
        "message": NODE_STATUS_UNKNOWN_MESSAGE
    });
    crate::common::preserve_condition_transition_time(&mut condition, None, now);
    conditions.push(condition);
    true
}

fn restore_pod_status_after_node_ready(pod: &mut Value, now: DateTime<Utc>) -> bool {
    if matches!(
        pod.pointer("/status/phase").and_then(|v| v.as_str()),
        Some("Succeeded" | "Failed")
    ) {
        return false;
    }
    if !pod_status_has_node_unknown_projection(pod) {
        return false;
    }
    let Some(restored_phase) = infer_pod_phase_from_cluster_status(pod) else {
        return false;
    };

    let now = k8s_time_format(now);
    let existing_conditions = pod
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let readiness_probe_containers = container_names_with_probe(pod, "readinessProbe");
    let startup_probe_containers = container_names_with_probe(pod, "startupProbe");

    let status = ensure_object_field(pod, "status");
    let mut changed = false;
    if status.get("phase").and_then(|v| v.as_str()) != Some(restored_phase.as_str()) {
        status.insert("phase".to_string(), Value::String(restored_phase.clone()));
        changed = true;
    }
    if restore_running_container_readiness(
        status,
        &readiness_probe_containers,
        &startup_probe_containers,
    ) {
        changed = true;
    }

    let all_containers_ready = app_container_statuses_all_ready(status);
    let containers_ready_status = if all_containers_ready {
        "True"
    } else {
        "False"
    };
    let ready_status = if restored_phase == "Running" && all_containers_ready {
        "True"
    } else {
        "False"
    };
    if upsert_reconciled_pod_condition(
        status,
        &existing_conditions,
        "ContainersReady",
        containers_ready_status,
        &now,
    ) {
        changed = true;
    }
    if upsert_reconciled_pod_condition(status, &existing_conditions, "Ready", ready_status, &now) {
        changed = true;
    }
    changed
}

fn mark_pod_status_node_lost(pod: &mut Value, now: DateTime<Utc>) {
    let _ = mark_pod_status_unknown(pod, now);
    let status = ensure_object_field(pod, "status");
    status.insert("phase".to_string(), json!("Failed"));
    status.insert("reason".to_string(), json!(POD_CLEANUP_REASON_NODE_LOST));
    status.insert(
        "message".to_string(),
        json!("Pod was terminated because its Node was lost."),
    );
}

fn pod_status_has_node_unknown_projection(pod: &Value) -> bool {
    pod.pointer("/status/phase").and_then(|v| v.as_str()) == Some("Unknown")
        || ["ContainersReady", "Ready"].iter().any(|condition_type| {
            pod.pointer("/status/conditions")
                .and_then(|v| v.as_array())
                .into_iter()
                .flatten()
                .any(|condition| {
                    condition.get("type").and_then(|v| v.as_str()) == Some(*condition_type)
                        && condition.get("status").and_then(|v| v.as_str()) == Some("Unknown")
                        && condition.get("reason").and_then(|v| v.as_str())
                            == Some(NODE_STATUS_UNKNOWN_REASON)
                })
        })
}

fn stale_node_pod_terminal_deadline(
    pod: &Value,
    pod_eviction_grace: Duration,
) -> Option<DateTime<Utc>> {
    let seconds = i64::try_from(pod_eviction_grace.as_secs()).unwrap_or(i64::MAX);
    node_unknown_transition_time(pod)
        .map(|transition_time| transition_time + chrono::Duration::seconds(seconds))
}

fn node_unknown_transition_time(pod: &Value) -> Option<DateTime<Utc>> {
    pod.pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .and_then(|conditions| {
            conditions.iter().find_map(|condition| {
                if condition.get("type").and_then(|v| v.as_str()) != Some("Ready")
                    || condition.get("status").and_then(|v| v.as_str()) != Some("Unknown")
                    || condition.get("reason").and_then(|v| v.as_str())
                        != Some(NODE_STATUS_UNKNOWN_REASON)
                {
                    return None;
                }
                condition
                    .get("lastTransitionTime")
                    .and_then(|v| v.as_str())
                    .and_then(|raw| chrono::DateTime::parse_from_rfc3339(raw).ok())
                    .map(|dt| dt.with_timezone(&Utc))
            })
        })
}

fn infer_pod_phase_from_cluster_status(pod: &Value) -> Option<String> {
    let statuses = pod
        .pointer("/status/containerStatuses")
        .and_then(|v| v.as_array())?;
    if statuses.is_empty() {
        return None;
    }

    let mut any_running = false;
    let mut all_terminated = true;
    let mut any_terminated_nonzero = false;
    let mut all_terminated_zero = true;
    for status in statuses {
        if status.pointer("/state/running").is_some() {
            any_running = true;
            all_terminated = false;
            all_terminated_zero = false;
            continue;
        }
        let Some(exit_code) = status
            .pointer("/state/terminated/exitCode")
            .and_then(value_as_i64)
        else {
            all_terminated = false;
            all_terminated_zero = false;
            continue;
        };
        if exit_code != 0 {
            any_terminated_nonzero = true;
            all_terminated_zero = false;
        }
    }

    if any_running {
        return Some("Running".to_string());
    }

    let restart_policy = pod
        .pointer("/spec/restartPolicy")
        .and_then(|v| v.as_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("Always");
    if all_terminated {
        if restart_policy == "Always" || (restart_policy == "OnFailure" && any_terminated_nonzero) {
            return Some("Running".to_string());
        }
        if all_terminated_zero && matches!(restart_policy, "Never" | "OnFailure") {
            return Some("Succeeded".to_string());
        }
        if any_terminated_nonzero && restart_policy == "Never" {
            return Some("Failed".to_string());
        }
    }

    Some("Pending".to_string())
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
}

fn container_names_with_probe(pod: &Value, probe_field: &str) -> HashSet<String> {
    pod.pointer("/spec/containers")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
        .filter(|container| container.get(probe_field).is_some())
        .filter_map(|container| {
            container
                .get("name")
                .and_then(|name| name.as_str())
                .map(str::to_string)
        })
        .collect()
}

fn restore_running_container_readiness(
    status: &mut serde_json::Map<String, Value>,
    readiness_probe_containers: &HashSet<String>,
    startup_probe_containers: &HashSet<String>,
) -> bool {
    let Some(statuses) = status
        .get_mut("containerStatuses")
        .and_then(|v| v.as_array_mut())
    else {
        return false;
    };
    let mut changed = false;
    for item in statuses {
        if item.pointer("/state/running").is_none() {
            continue;
        }
        let Some(name) = item
            .get("name")
            .and_then(|v| v.as_str())
            .map(str::to_string)
        else {
            continue;
        };
        if !readiness_probe_containers.contains(&name)
            && item.get("ready").and_then(|v| v.as_bool()) != Some(true)
        {
            item["ready"] = json!(true);
            changed = true;
        }
        if !startup_probe_containers.contains(&name)
            && item.get("started").is_some()
            && item.get("started").and_then(|v| v.as_bool()) != Some(true)
        {
            item["started"] = json!(true);
            changed = true;
        }
    }
    changed
}

fn app_container_statuses_all_ready(status: &serde_json::Map<String, Value>) -> bool {
    status
        .get("containerStatuses")
        .and_then(|v| v.as_array())
        .is_some_and(|statuses| {
            !statuses.is_empty()
                && statuses.iter().all(|status| {
                    status.get("ready").and_then(|ready| ready.as_bool()) == Some(true)
                })
        })
}

fn upsert_reconciled_pod_condition(
    status: &mut serde_json::Map<String, Value>,
    existing_conditions: &[Value],
    condition_type: &str,
    condition_status: &str,
    now: &str,
) -> bool {
    let conditions = status
        .entry("conditions".to_string())
        .or_insert_with(|| Value::Array(Vec::new()));
    let mut changed = false;
    if !conditions.is_array() {
        *conditions = Value::Array(Vec::new());
        changed = true;
    }
    let Some(conditions) = conditions.as_array_mut() else {
        return changed;
    };
    let previous = crate::common::condition_by_type(existing_conditions, condition_type);
    if let Some(condition) = conditions
        .iter_mut()
        .find(|condition| condition.get("type").and_then(|v| v.as_str()) == Some(condition_type))
    {
        if set_condition_string_field(condition, "status", condition_status) {
            changed = true;
        }
        if remove_condition_field(condition, "reason") {
            changed = true;
        }
        if remove_condition_field(condition, "message") {
            changed = true;
        }
        let previous_transition = condition.get("lastTransitionTime").cloned();
        crate::common::preserve_condition_transition_time(condition, previous, now);
        if condition.get("lastTransitionTime") != previous_transition.as_ref() {
            changed = true;
        }
        return changed;
    }

    let mut condition = json!({
        "type": condition_type,
        "status": condition_status
    });
    crate::common::preserve_condition_transition_time(&mut condition, previous, now);
    conditions.push(condition);
    true
}

fn set_condition_string_field(condition: &mut Value, field: &str, value: &str) -> bool {
    if !condition.is_object() {
        *condition = json!({});
    }
    if condition.get(field).and_then(|v| v.as_str()) == Some(value) {
        return false;
    }
    condition[field] = json!(value);
    true
}

fn remove_condition_field(condition: &mut Value, field: &str) -> bool {
    let Some(obj) = condition.as_object_mut() else {
        return false;
    };
    obj.remove(field).is_some()
}

fn mark_node_ready_from_fresh_observation(
    node: &mut Value,
    _lease: &NodeLeaseObservation,
    now: DateTime<Utc>,
) -> bool {
    if network_unavailable(node) {
        return false;
    }

    let transition_time = k8s_time_format(now);
    let Some(conditions) = node_conditions_mut(node) else {
        return false;
    };
    if let Some(condition) = conditions.iter_mut().find(|c| c["type"] == "Ready") {
        if condition.get("status").and_then(|v| v.as_str()) == Some("True") {
            return false;
        }
        if condition.get("status").and_then(|v| v.as_str()) != Some("Unknown")
            || condition.get("reason").and_then(|v| v.as_str()) != Some(NODE_STATUS_UNKNOWN_REASON)
        {
            return false;
        }
        let previous = condition.clone();
        condition["status"] = json!("True");
        condition["reason"] = json!(NODE_READY_REASON);
        condition["message"] = json!(NODE_READY_MESSAGE);
        remove_condition_field(condition, "lastHeartbeatTime");
        crate::common::preserve_condition_transition_time(
            condition,
            Some(&previous),
            &transition_time,
        );
        return true;
    }

    let mut condition = json!({
        "type": "Ready",
        "status": "True",
        "reason": NODE_READY_REASON,
        "message": NODE_READY_MESSAGE
    });
    crate::common::preserve_condition_transition_time(&mut condition, None, &transition_time);
    conditions.push(condition);
    true
}

fn network_unavailable(node: &Value) -> bool {
    node.pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
        .any(|condition| {
            condition.get("type").and_then(|v| v.as_str()) == Some("NetworkUnavailable")
                && condition.get("status").and_then(|v| v.as_str()) == Some("True")
        })
}

fn node_conditions_mut(node: &mut Value) -> Option<&mut Vec<Value>> {
    let node_obj = node.as_object_mut()?;
    let status = node_obj.entry("status").or_insert_with(|| json!({}));
    if !status.is_object() {
        *status = json!({});
    }
    let status_obj = status.as_object_mut()?;
    let conditions = status_obj.entry("conditions").or_insert_with(|| json!([]));
    if !conditions.is_array() {
        *conditions = json!([]);
    }
    conditions.as_array_mut()
}

fn node_ready_condition_true(node: &Value) -> bool {
    node.pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
        .any(|condition| {
            condition.get("type").and_then(|v| v.as_str()) == Some("Ready")
                && condition.get("status").and_then(|v| v.as_str()) == Some("True")
        })
}

fn ready_status_heartbeat_deadline(node: &Value) -> Option<DateTime<Utc>> {
    let heartbeat = node
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
        .find(|condition| {
            condition.get("type").and_then(|v| v.as_str()) == Some("Ready")
                && condition.get("status").and_then(|v| v.as_str()) == Some("True")
        })?
        .get("lastHeartbeatTime")
        .and_then(|v| v.as_str())
        .and_then(|raw| chrono::DateTime::parse_from_rfc3339(raw).ok())
        .map(|dt| dt.with_timezone(&Utc))?;
    Some(heartbeat + chrono::Duration::seconds(DEFAULT_NODE_LEASE_GRACE_SECONDS))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn retry_delay_is_bounded_and_deterministic() {
        assert_eq!(node_lifecycle_retry_delay(0), Duration::from_secs(5));
        assert_eq!(node_lifecycle_retry_delay(5), Duration::from_secs(30));
        assert_eq!(node_lifecycle_retry_delay(11), Duration::from_secs(60));
        assert_eq!(
            node_lifecycle_retry_delay(u32::MAX),
            Duration::from_secs(60)
        );
    }

    #[test]
    fn ready_unknown_transition_is_idempotent_and_uses_injected_time() {
        let mut node = json!({
            "status": {"conditions": [{
                "type": "Ready",
                "status": "True",
                "reason": "KubeletReady",
                "message": "ready",
                "lastHeartbeatTime": "2026-01-01T00:00:00Z",
                "lastTransitionTime": "2026-01-01T00:00:00Z"
            }]}
        });
        let now = Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();

        assert!(mark_node_ready_unknown(&mut node, now));
        let ready = &node["status"]["conditions"][0];
        assert_eq!(ready["status"], "Unknown");
        assert_eq!(ready["reason"], NODE_STATUS_UNKNOWN_REASON);
        assert!(ready.get("lastHeartbeatTime").is_none());
        assert_eq!(ready["lastTransitionTime"], "2026-01-02T03:04:05Z");
        assert!(!mark_node_ready_unknown(&mut node, now));
    }
}
