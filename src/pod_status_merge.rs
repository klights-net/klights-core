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

/// Originator of a Pod `.status` write. The merge policy is stricter for the
/// kubelet runtime path than for a user-driven `/status` subresource write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PodStatusUpdateSource {
    /// Status computed and forwarded by the kubelet runtime (outbox/raft apply,
    /// replicated apply, runtime emission). Terminal/confirmed state must be
    /// preserved over a stale snapshot.
    KubeletRuntime,
    /// Status written by an API client through the `/status` subresource. The
    /// caller is authoritative over `phase` and container state — the kubelet
    /// terminal-preservation rewrite must NOT apply.
    UserStatusSubresource,
}

/// Merge policy applied to an incoming Pod `.status` before it replaces the
/// current `.status`.
///
/// - Always: preserve scheduler-owned conditions (`DisruptionTarget`, ...) by
///   `type` so a kubelet status snapshot that omits them cannot drop them.
/// - Only for [`PodStatusUpdateSource::KubeletRuntime`]: preserve terminal or
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
    source: PodStatusUpdateSource,
) {
    if api_version != "v1" || kind != "Pod" {
        return;
    }
    preserve_non_kubelet_conditions(current, incoming_status);
    if source == PodStatusUpdateSource::KubeletRuntime {
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
            PodStatusUpdateSource::KubeletRuntime,
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
            PodStatusUpdateSource::KubeletRuntime,
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
            PodStatusUpdateSource::UserStatusSubresource,
        );
        assert_eq!(incoming.pointer("/phase"), Some(&json!("Running")));
    }
}
