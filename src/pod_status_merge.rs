//! Centralized Pod status merge policy.
//!
//! Multiple write paths (kubelet runtime emission, leader outbox/raft status
//! apply, replicated apply, and status subresource writes) historically each
//! carried ad hoc protections for status races against newer Pod state. This
//! module owns the single DRY policy they all route through so stale
//! `ContainerCreating` snapshots cannot clobber confirmed runtime state and
//! scheduler-owned conditions (e.g. `DisruptionTarget`) survive a kubelet
//! status rewrite.

use serde_json::Value;

/// Originator of a Pod `.status` write. Controls which merge rules apply.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PodStatusOwner {
    /// Status computed and forwarded by the kubelet runtime (outbox/raft apply,
    /// stamped replicated apply, runtime emission). Terminal/confirmed state must
    /// be preserved over a stale snapshot; scheduler conditions are preserved.
    KubeletRuntime,
    /// Status written by the scheduler (e.g. conditions, nomination). Only
    /// scheduler-owned conditions are preserved; no terminal-state rewrite.
    Scheduler,
    /// Status written by an API client through the `/status` subresource. The
    /// caller is authoritative over `phase` and container state — the kubelet
    /// terminal-preservation rewrite must NOT apply. Scheduler conditions preserved.
    ApiStatusSubresource,
    /// Status applied by a raft replication or leader-direct path without an
    /// outbox stamp (no kubelet terminal-state guarantee). Scheduler conditions
    /// are preserved; terminal-state rewrite is NOT applied.
    ReplicatedApply,
}

/// Narrow set of status fields each owner may update. Unused fields are
/// left `None`; merge applies only the fields present.
#[derive(Debug, Default)]
pub struct PodStatusPatch {
    /// Override the Pod `phase` field.
    pub phase: Option<String>,
    /// Conditions the owner wants to set (merged by `type` key).
    pub conditions: Option<Vec<Value>>,
}

/// Merge policy applied to an incoming Pod `.status` before it replaces the
/// current `.status`.
///
/// - Always: preserve scheduler-owned conditions (`DisruptionTarget`, ...) by
///   `type` so a kubelet status snapshot that omits them cannot drop them.
/// - Only for [`PodStatusOwner::KubeletRuntime`]: preserve terminal or
///   confirmed runtime container state over a stale `waiting` snapshot.
///
/// `current` is the full live Pod object (`{apiVersion, kind, metadata, spec,
/// status}`) and `incoming_status` is the bare `.status` object about to be
/// written. The rewrite is in-place on `incoming_status`.
pub fn merge_pod_status_for_update(
    api_version: &str,
    kind: &str,
    current: &Value,
    incoming_status: &mut Value,
    owner: PodStatusOwner,
) {
    if api_version != "v1" || kind != "Pod" {
        return;
    }
    preserve_non_kubelet_conditions(current, incoming_status);
    if owner == PodStatusOwner::KubeletRuntime {
        preserve_terminal_or_confirmed_runtime_state(current, incoming_status);
    }
}

/// Preserve scheduler/controller-owned Pod conditions that the kubelet does
/// not rebuild. A kubelet status snapshot omits conditions like
/// `DisruptionTarget` (set by the scheduler on preemption); carrying that
/// snapshot forward must not drop them. Conditions already present in the
/// incoming status (matched by `type`) are left untouched.
fn preserve_non_kubelet_conditions(current: &Value, incoming_status: &mut Value) {
    let Some(existing_conditions) = current
        .pointer("/status/conditions")
        .and_then(|conditions| conditions.as_array())
    else {
        return;
    };
    let preservable: Vec<Value> = existing_conditions
        .iter()
        .filter(|condition| {
            condition
                .get("type")
                .and_then(|value| value.as_str())
                .is_some_and(|condition_type| !is_kubelet_rebuilt_pod_condition(condition_type))
        })
        .cloned()
        .collect();
    if preservable.is_empty() {
        return;
    }

    let Some(status_obj) = incoming_status.as_object_mut() else {
        return;
    };
    let conditions = status_obj
        .entry("conditions".to_string())
        .or_insert_with(|| Value::Array(Vec::new()));
    if !conditions.is_array() {
        *conditions = Value::Array(Vec::new());
    }
    let Some(conditions) = conditions.as_array_mut() else {
        return;
    };

    for condition in preservable {
        let Some(condition_type) = condition.get("type").and_then(|value| value.as_str()) else {
            continue;
        };
        if conditions.iter().any(|existing| {
            existing.get("type").and_then(|value| value.as_str()) == Some(condition_type)
        }) {
            continue;
        }
        conditions.push(condition);
    }
}

/// Kubelet-rebuilt conditions are always re-emitted by the kubelet status
/// snapshot, so they are never candidates for preservation.
fn is_kubelet_rebuilt_pod_condition(condition_type: &str) -> bool {
    matches!(
        condition_type,
        "PodScheduled" | "Initialized" | "ContainersReady" | "Ready"
    )
}

/// Preserve terminal or confirmed runtime container state over a stale kubelet
/// `waiting` snapshot. The kubelet status pipeline is eventually consistent
/// and a `ContainerCreating` snapshot can be retried after the container has
/// already started or terminated; this rewrite keeps the newer observed state.
///
/// Rules (see `klights-core/fixnow.md` Task 2 Step 3):
/// - Only compare container statuses sharing the same `name`.
/// - Treat identity as the same when `containerID` matches and is non-empty.
/// - If both statuses have `restartCount`, require equality before preserving
///   a terminal state (so a real restart is not masked).
/// - Preserve a current `terminated` state over incoming `waiting.ContainerCreating`.
/// - Preserve a current `running` state over incoming `waiting.ContainerCreating`.
/// - Do NOT preserve terminal state over an incoming status with a higher
///   `restartCount` (that is a genuine new incarnation).
/// - If the current phase is terminal (`Succeeded`/`Failed`) and the incoming
///   phase is not terminal for the same containers, keep the current phase and
///   current container statuses.
fn preserve_terminal_or_confirmed_runtime_state(current: &Value, incoming_status: &mut Value) {
    let current_phase = current
        .pointer("/status/phase")
        .and_then(|value| value.as_str());
    let current_terminal = matches!(current_phase, Some("Succeeded") | Some("Failed"));

    let Some(current_statuses) = current
        .pointer("/status/containerStatuses")
        .and_then(|value| value.as_array())
    else {
        return;
    };

    // Index the current statuses by container name for O(n) matching.
    let current_by_name: std::collections::HashMap<&str, &Value> = current_statuses
        .iter()
        .filter_map(|status| {
            status
                .get("name")
                .and_then(|value| value.as_str())
                .map(|name| (name, status))
        })
        .collect();
    if current_by_name.is_empty() {
        return;
    }

    let Some(incoming_phase_value) = incoming_status.get("phase").cloned() else {
        return;
    };
    let incoming_phase = incoming_phase_value.as_str();
    let incoming_terminal = matches!(incoming_phase, Some("Succeeded") | Some("Failed"));

    // Terminal-phase preservation: current terminal, incoming not terminal for
    // the same containers → keep current phase and current container statuses.
    if current_terminal
        && !incoming_terminal
        && shares_any_container(incoming_status, &current_by_name)
    {
        if let Some(phase) = current_phase {
            incoming_status["phase"] = Value::String(phase.to_string());
        }
        let preserved: Vec<Value> = current_statuses
            .iter()
            .filter(|status| {
                status
                    .get("name")
                    .and_then(|value| value.as_str())
                    .is_some_and(|name| current_by_name.contains_key(name))
            })
            .cloned()
            .collect();
        incoming_status["containerStatuses"] = Value::Array(preserved);
        return;
    }

    // Per-container state preservation against an incoming `waiting` snapshot.
    let Some(incoming_statuses) = incoming_status
        .get_mut("containerStatuses")
        .and_then(|value| value.as_array_mut())
    else {
        return;
    };
    for incoming in incoming_statuses.iter_mut() {
        let Some(name) = incoming.get("name").and_then(|value| value.as_str()) else {
            continue;
        };
        let Some(current) = current_by_name.get(name) else {
            continue;
        };
        // Same incarnation (containerID) is required before preserving: a new
        // containerID means a fresh container, not a stale snapshot.
        if !same_container_incarnation(current, incoming) {
            continue;
        }
        // restartCount must match (when both present) so a genuine restart with
        // a higher count is not masked as a stale snapshot.
        if let (Some(current_count), Some(incoming_count)) = (
            current.get("restartCount").and_then(|v| v.as_i64()),
            incoming.get("restartCount").and_then(|v| v.as_i64()),
        ) && current_count != incoming_count
        {
            // An incoming higher restartCount is a real new restart — do
            // not preserve the older terminal/running state over it.
            continue;
        }
        let incoming_waiting = incoming
            .pointer("/state/waiting")
            .and_then(|value| value.as_object());
        let incoming_is_container_creating = incoming_waiting
            .and_then(|waiting| waiting.get("reason"))
            .and_then(|value| value.as_str())
            == Some("ContainerCreating");
        if !incoming_is_container_creating {
            continue;
        }
        let current_has_terminated = current.pointer("/state/terminated").is_some();
        let current_has_running = current.pointer("/state/running").is_some();
        if (current_has_terminated || current_has_running)
            && let Some(state) = current.get("state").cloned()
        {
            incoming["state"] = state;
        }
    }
}

/// Whether `current` and `incoming` refer to the same container incarnation.
/// Same when both omit `containerID` (unambiguous snapshot pairing is by name),
/// or when the non-empty `containerID` values match.
fn same_container_incarnation(current: &Value, incoming: &Value) -> bool {
    let current_id = current
        .get("containerID")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    let incoming_id = incoming
        .get("containerID")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    if current_id.is_empty() && incoming_id.is_empty() {
        return true;
    }
    !current_id.is_empty() && current_id == incoming_id
}

/// Whether the incoming status shares at least one container name with the
/// current status — used to gate terminal-phase preservation to the same Pod
/// incarnation rather than a wholesale replacement.
fn shares_any_container(
    incoming_status: &Value,
    current_by_name: &std::collections::HashMap<&str, &Value>,
) -> bool {
    incoming_status
        .pointer("/containerStatuses")
        .and_then(|value| value.as_array())
        .is_some_and(|statuses| {
            statuses.iter().any(|status| {
                status
                    .get("name")
                    .and_then(|value| value.as_str())
                    .is_some_and(|name| current_by_name.contains_key(name))
            })
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn kubelet_status_preserves_scheduler_owned_disruption_target() {
        let current = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "status": {
                "phase": "Running",
                "conditions": [
                    {"type": "PodScheduled", "status": "True"},
                    {"type": "Initialized", "status": "True"},
                    {"type": "ContainersReady", "status": "True"},
                    {"type": "Ready", "status": "True"},
                    {"type": "DisruptionTarget", "status": "True", "reason": "PreemptionByScheduler"}
                ]
            }
        });
        let mut incoming = json!({
            "phase": "Running",
            "conditions": [
                {"type": "PodScheduled", "status": "True"},
                {"type": "Initialized", "status": "True"},
                {"type": "ContainersReady", "status": "True"},
                {"type": "Ready", "status": "True"}
            ]
        });
        merge_pod_status_for_update(
            "v1",
            "Pod",
            &current,
            &mut incoming,
            PodStatusOwner::KubeletRuntime,
        );
        assert!(
            incoming
                .pointer("/conditions")
                .and_then(|value| value.as_array())
                .unwrap()
                .iter()
                .any(
                    |condition| condition.pointer("/type").and_then(|value| value.as_str())
                        == Some("DisruptionTarget")
                ),
            "kubelet status must preserve scheduler-owned DisruptionTarget: {incoming:?}"
        );
    }

    #[test]
    fn kubelet_waiting_snapshot_cannot_regress_running_or_terminal_container() {
        let current = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "status": {
                "phase": "Succeeded",
                "containerStatuses": [{
                    "name": "app",
                    "containerID": "containerd://ctr-1",
                    "restartCount": 0,
                    "ready": false,
                    "started": true,
                    "state": {"terminated": {"exitCode": 0, "reason": "Completed"}}
                }]
            }
        });
        let mut incoming = json!({
            "phase": "Pending",
            "containerStatuses": [{
                "name": "app",
                "containerID": "containerd://ctr-1",
                "restartCount": 0,
                "ready": false,
                "started": false,
                "state": {"waiting": {"reason": "ContainerCreating"}}
            }]
        });
        merge_pod_status_for_update(
            "v1",
            "Pod",
            &current,
            &mut incoming,
            PodStatusOwner::KubeletRuntime,
        );
        assert_eq!(incoming.pointer("/phase"), Some(&json!("Succeeded")));
        assert_eq!(
            incoming.pointer("/containerStatuses/0/state/terminated/exitCode"),
            Some(&json!(0))
        );
        assert!(
            incoming
                .pointer("/containerStatuses/0/state/waiting")
                .is_none()
        );
    }

    #[test]
    fn user_status_subresource_does_not_get_kubelet_terminal_rewrite() {
        let current = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "status": {
                "phase": "Succeeded",
                "containerStatuses": [{
                    "name": "app",
                    "containerID": "containerd://ctr-1",
                    "restartCount": 0,
                    "state": {"terminated": {"exitCode": 0}}
                }]
            }
        });
        let mut incoming = json!({"phase": "Running"});
        merge_pod_status_for_update(
            "v1",
            "Pod",
            &current,
            &mut incoming,
            PodStatusOwner::ApiStatusSubresource,
        );
        assert_eq!(incoming.pointer("/phase"), Some(&json!("Running")));
    }

    // ── Task 8 typed-ownership regression tests ────────────────────

    #[test]
    fn scheduler_disruption_target_survives_worker_status_apply() {
        let current = json!({
            "apiVersion": "v1", "kind": "Pod",
            "status": {
                "phase": "Running",
                "conditions": [
                    {"type": "Ready", "status": "True"},
                    {"type": "DisruptionTarget", "status": "True", "reason": "PreemptionByScheduler"}
                ]
            }
        });
        let mut incoming = json!({
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "True"}]
        });
        merge_pod_status_for_update(
            "v1",
            "Pod",
            &current,
            &mut incoming,
            PodStatusOwner::KubeletRuntime,
        );
        let conditions = incoming
            .pointer("/conditions")
            .and_then(|v| v.as_array())
            .unwrap();
        assert!(
            conditions
                .iter()
                .any(|c| c.get("type").and_then(|v| v.as_str()) == Some("DisruptionTarget")),
            "DisruptionTarget must survive KubeletRuntime (worker) apply: {incoming:?}"
        );
    }

    #[test]
    fn scheduler_disruption_target_survives_leader_direct_status_apply() {
        let current = json!({
            "apiVersion": "v1", "kind": "Pod",
            "status": {
                "conditions": [
                    {"type": "Ready", "status": "True"},
                    {"type": "DisruptionTarget", "status": "True", "reason": "PreemptionByScheduler"}
                ]
            }
        });
        let mut incoming = json!({
            "conditions": [{"type": "Ready", "status": "True"}]
        });
        merge_pod_status_for_update(
            "v1",
            "Pod",
            &current,
            &mut incoming,
            PodStatusOwner::ReplicatedApply,
        );
        let conditions = incoming
            .pointer("/conditions")
            .and_then(|v| v.as_array())
            .unwrap();
        assert!(
            conditions
                .iter()
                .any(|c| c.get("type").and_then(|v| v.as_str()) == Some("DisruptionTarget")),
            "DisruptionTarget must survive ReplicatedApply (leader-direct) apply: {incoming:?}"
        );
    }

    #[test]
    fn api_status_subresource_can_replace_user_owned_status_fields() {
        let current = json!({
            "apiVersion": "v1", "kind": "Pod",
            "status": {
                "phase": "Succeeded",
                "containerStatuses": [{
                    "name": "app",
                    "containerID": "containerd://ctr-1",
                    "restartCount": 0,
                    "state": {"terminated": {"exitCode": 0}}
                }]
            }
        });
        let mut incoming = json!({"phase": "Failed"});
        merge_pod_status_for_update(
            "v1",
            "Pod",
            &current,
            &mut incoming,
            PodStatusOwner::ApiStatusSubresource,
        );
        assert_eq!(
            incoming.pointer("/phase"),
            Some(&json!("Failed")),
            "ApiStatusSubresource must not rewrite phase to Succeeded: {incoming:?}"
        );
    }

    #[test]
    fn kubelet_runtime_status_cannot_drop_scheduler_owned_conditions() {
        let current = json!({
            "apiVersion": "v1", "kind": "Pod",
            "status": {
                "conditions": [
                    {"type": "PodScheduled", "status": "True"},
                    {"type": "DisruptionTarget", "status": "True"}
                ]
            }
        });
        let mut incoming = json!({
            "conditions": [{"type": "PodScheduled", "status": "True"}]
        });
        merge_pod_status_for_update(
            "v1",
            "Pod",
            &current,
            &mut incoming,
            PodStatusOwner::KubeletRuntime,
        );
        let conditions = incoming
            .pointer("/conditions")
            .and_then(|v| v.as_array())
            .unwrap();
        assert!(
            conditions
                .iter()
                .any(|c| c.get("type").and_then(|v| v.as_str()) == Some("DisruptionTarget")),
            "KubeletRuntime must not drop DisruptionTarget: {incoming:?}"
        );
    }

    #[test]
    fn terminal_container_state_does_not_regress_to_container_creating() {
        let current = json!({
            "apiVersion": "v1", "kind": "Pod",
            "status": {
                "phase": "Succeeded",
                "containerStatuses": [{
                    "name": "worker",
                    "containerID": "containerd://abc",
                    "restartCount": 0,
                    "state": {"terminated": {"exitCode": 0, "reason": "Completed"}}
                }]
            }
        });
        let mut incoming = json!({
            "phase": "Pending",
            "containerStatuses": [{
                "name": "worker",
                "containerID": "containerd://abc",
                "restartCount": 0,
                "state": {"waiting": {"reason": "ContainerCreating"}}
            }]
        });
        merge_pod_status_for_update(
            "v1",
            "Pod",
            &current,
            &mut incoming,
            PodStatusOwner::KubeletRuntime,
        );
        assert!(
            incoming
                .pointer("/containerStatuses/0/state/terminated")
                .is_some(),
            "terminal container state must not regress to ContainerCreating: {incoming:?}"
        );
        assert!(
            incoming
                .pointer("/containerStatuses/0/state/waiting")
                .is_none(),
            "waiting state must be replaced by preserved terminal state: {incoming:?}"
        );
    }

    #[test]
    fn pod_status_merge_json_and_protobuf_paths_match() {
        // Both JSON and protobuf raft-apply paths produce a serde_json::Value and
        // call merge_pod_status_for_update. Verify the merge function is pure and
        // deterministic: same input Value → same output regardless of decode path.
        let current = json!({
            "apiVersion": "v1", "kind": "Pod",
            "status": {
                "conditions": [
                    {"type": "DisruptionTarget", "status": "True", "reason": "PreemptionByScheduler"}
                ]
            }
        });
        // "JSON path": incoming status built from JSON literal (kubectl/API path)
        let mut incoming_json = json!({"conditions": []});
        merge_pod_status_for_update(
            "v1",
            "Pod",
            &current,
            &mut incoming_json,
            PodStatusOwner::KubeletRuntime,
        );

        // "Protobuf path": same incoming status decoded from a JSON string
        // (simulates the serde_json::Value produced by pb_pod_status_to_json)
        let mut incoming_proto: serde_json::Value =
            serde_json::from_str(r#"{"conditions":[]}"#).unwrap();
        merge_pod_status_for_update(
            "v1",
            "Pod",
            &current,
            &mut incoming_proto,
            PodStatusOwner::KubeletRuntime,
        );

        assert_eq!(
            incoming_json, incoming_proto,
            "JSON and protobuf apply paths must produce identical merge results"
        );
    }
}
