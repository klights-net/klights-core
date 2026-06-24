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

/// Originator of a Pod `.status` write. Controls which merge rules apply and,
/// critically, which Pod condition *types* the writer is authoritative for.
///
/// Condition ownership is decided by this enum, not by parsing condition-`type`
/// strings to guess provenance. Each owner declares the condition types it is
/// allowed to set ([`PodStatusOwner::owns_condition_type`]); any condition type
/// the owner does *not* own is preserved from the live status so a writer that
/// omits another owner's condition cannot drop it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default)]
pub enum PodStatusOwner {
    /// Status computed and forwarded by the kubelet runtime (outbox/raft apply,
    /// stamped replicated apply, runtime emission). Owns the kubelet-rebuilt
    /// lifecycle conditions only; terminal/confirmed container state is
    /// preserved over a stale snapshot; non-owned (e.g. scheduler) conditions
    /// are preserved.
    KubeletRuntime,
    /// Status written by the scheduler (e.g. preemption `DisruptionTarget`,
    /// `PodScheduled` nomination). Owns scheduler conditions; non-owned kubelet
    /// runtime conditions are preserved. No terminal-state rewrite.
    Scheduler,
    /// Status written by an API client through the `/status` subresource. The
    /// caller is authoritative over `phase`, container state and its own
    /// conditions — the kubelet terminal-preservation rewrite must NOT apply.
    /// Scheduler conditions the client omitted are preserved.
    ApiStatusSubresource,
    /// Status applied by a raft replication or leader-direct path without an
    /// outbox stamp (no kubelet terminal-state guarantee). Owns no conditions of
    /// its own; non-owned (scheduler/kubelet) conditions are preserved and the
    /// terminal-state rewrite is NOT applied.
    #[default]
    ReplicatedApply,
}

impl PodStatusOwner {
    /// Whether this owner is authoritative for the given Pod condition `type`.
    ///
    /// A writer may set/overwrite the condition types it owns; condition types
    /// it does not own are preserved from the live status. This is the typed
    /// replacement for the old `is_kubelet_rebuilt_pod_condition` heuristic:
    /// ownership is a property of the *writer*, declared here, not inferred from
    /// the condition-type string.
    fn owns_condition_type(self, condition_type: &str) -> bool {
        match self {
            // The kubelet rebuilds these lifecycle conditions on every status
            // snapshot, so it is authoritative for them and only them.
            PodStatusOwner::KubeletRuntime => is_kubelet_lifecycle_condition(condition_type),
            // The scheduler is authoritative for the conditions it sets during
            // scheduling/preemption.
            PodStatusOwner::Scheduler => is_scheduler_owned_condition(condition_type),
            // An API `/status` writer is authoritative for any condition it
            // explicitly carries except scheduler-owned ones, which it must not
            // be able to silently drop by omission.
            PodStatusOwner::ApiStatusSubresource => !is_scheduler_owned_condition(condition_type),
            // A replicated/leader-direct apply carries no authority of its own;
            // every condition type present in the live status is preserved.
            PodStatusOwner::ReplicatedApply => false,
        }
    }

    /// Whether this owner is allowed to preserve terminal/confirmed runtime
    /// container state over an incoming stale `waiting` snapshot. Only the
    /// kubelet runtime carries that guarantee.
    fn preserves_terminal_runtime_state(self) -> bool {
        matches!(self, PodStatusOwner::KubeletRuntime)
    }
}

/// Narrow, per-owner view of the status fields a writer is contributing.
///
/// This is extracted from the incoming `.status` and, together with the writer's
/// [`PodStatusOwner`], is the only channel through which a writer contributes to
/// the merged status. The owner determines which live condition types the writer
/// is allowed to overwrite vs. which must be preserved from the live status.
#[derive(Debug, Default)]
pub struct PodStatusPatch {
    /// The owner that produced this patch.
    owner: PodStatusOwner,
    /// The `phase` the incoming status set, if any. Every owner may set phase.
    phase: Option<String>,
    /// The conditions the incoming writer carried (kept verbatim, keyed by
    /// `type`). Condition types the owner does not own are additionally
    /// back-filled from the live status when the writer omitted them.
    conditions: Vec<Value>,
}

impl PodStatusPatch {
    /// Extract the per-owner patch from an incoming `.status`.
    fn extract(owner: PodStatusOwner, incoming_status: &Value) -> Self {
        let phase = incoming_status
            .get("phase")
            .and_then(|value| value.as_str())
            .map(str::to_string);
        let conditions = incoming_status
            .get("conditions")
            .and_then(|value| value.as_array())
            .map(|conditions| conditions.to_vec())
            .unwrap_or_default();
        PodStatusPatch {
            owner,
            phase,
            conditions,
        }
    }
}

/// Merge policy applied to an incoming Pod `.status` before it replaces the
/// current `.status`.
///
/// The incoming status is reduced to a typed [`PodStatusPatch`] carrying only
/// the fields/conditions the [`PodStatusOwner`] is authoritative for. The merge
/// then:
///
/// - Preserves every condition type the owner does *not* own from the live
///   status (keyed by `type`), so a writer cannot drop another owner's
///   condition by omission.
/// - For [`PodStatusOwner::KubeletRuntime`] only: preserves terminal or
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
    let patch = PodStatusPatch::extract(owner, incoming_status);
    patch.apply_phase(incoming_status);
    patch.apply_conditions(current, incoming_status);
    if owner.preserves_terminal_runtime_state() {
        preserve_terminal_or_confirmed_runtime_state(current, incoming_status);
    }
}

impl PodStatusPatch {
    /// Write the owner's `phase` back into the incoming status. Every owner may
    /// set its own phase, so this re-asserts the extracted value as the
    /// authoritative source (the kubelet terminal-state rewrite may override it
    /// afterwards for [`PodStatusOwner::KubeletRuntime`]). When the incoming
    /// status carried no phase the field is left absent.
    fn apply_phase(&self, incoming_status: &mut Value) {
        let Some(phase) = self.phase.as_deref() else {
            return;
        };
        if let Some(obj) = incoming_status.as_object_mut() {
            obj.insert("phase".to_string(), Value::String(phase.to_string()));
        }
    }

    /// Merge conditions: keep the conditions the writer carried (verbatim, keyed
    /// by `type`), then back-fill from the live status every condition type the
    /// owner is *not* authoritative for and that the writer omitted.
    ///
    /// This keeps the no-drop invariant — a writer cannot drop another owner's
    /// condition by omission — while deciding *what* a writer may overwrite by
    /// the typed [`PodStatusOwner`] rather than by parsing condition-`type`
    /// strings to guess provenance. Conditions are keyed by `type`, never by
    /// array position, preserving K8s condition semantics.
    fn apply_conditions(&self, current: &Value, incoming_status: &mut Value) {
        // Live condition types the owner is NOT authoritative for and that the
        // writer omitted must be preserved.
        let preservable: Vec<Value> = current
            .pointer("/status/conditions")
            .and_then(|conditions| conditions.as_array())
            .map(|existing| {
                existing
                    .iter()
                    .filter(|condition| {
                        condition
                            .get("type")
                            .and_then(|value| value.as_str())
                            .is_some_and(|condition_type| {
                                !self.owner.owns_condition_type(condition_type)
                            })
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        // Nothing to preserve and the writer carried no conditions: leave the
        // status untouched (do not invent an empty `conditions` array).
        if preservable.is_empty() && self.conditions.is_empty() {
            let writer_had_conditions = incoming_status.get("conditions").is_some();
            if !writer_had_conditions {
                return;
            }
        }

        let Some(status_obj) = incoming_status.as_object_mut() else {
            return;
        };

        // Authoritative result = conditions the writer carried (from the typed
        // patch) + preserved non-owned live conditions, keyed by `type`.
        let mut merged: Vec<Value> = self.conditions.clone();
        for condition in preservable {
            let Some(condition_type) = condition.get("type").and_then(|value| value.as_str()) else {
                continue;
            };
            if merged.iter().any(|existing| {
                existing.get("type").and_then(|value| value.as_str()) == Some(condition_type)
            }) {
                continue;
            }
            merged.push(condition);
        }
        status_obj.insert("conditions".to_string(), Value::Array(merged));
    }
}

/// Merge an owner's freshly-rebuilt conditions with the live (`existing`)
/// conditions, preserving by `type` every condition the owner is *not*
/// authoritative for.
///
/// This is the typed entry point for status writers that rebuild their owned
/// conditions from scratch (e.g. the kubelet status pipeline rebuilds
/// `PodScheduled`/`Initialized`/`ContainersReady`/`Ready`). It guarantees a
/// rebuilding writer cannot drop another owner's condition (e.g. the scheduler's
/// `DisruptionTarget`) by omission — the decision of what survives keys off
/// [`PodStatusOwner`], not off parsing condition-type strings at the call site.
pub fn merge_owned_and_preserved_conditions(
    owner: PodStatusOwner,
    owned_conditions: Vec<Value>,
    existing_conditions: &[Value],
) -> Vec<Value> {
    let mut merged = owned_conditions;
    for condition in existing_conditions {
        let Some(condition_type) = condition.get("type").and_then(|value| value.as_str()) else {
            continue;
        };
        if owner.owns_condition_type(condition_type) {
            continue;
        }
        if merged.iter().any(|existing| {
            existing.get("type").and_then(|value| value.as_str()) == Some(condition_type)
        }) {
            continue;
        }
        merged.push(condition.clone());
    }
    merged
}

/// Lifecycle conditions the kubelet rebuilds on every status snapshot. The
/// kubelet runtime owner is authoritative for exactly these.
fn is_kubelet_lifecycle_condition(condition_type: &str) -> bool {
    matches!(
        condition_type,
        "PodScheduled" | "Initialized" | "ContainersReady" | "Ready"
    )
}

/// Conditions owned by the scheduler. The scheduler is authoritative for these
/// and no other writer may drop them by omission. `PodScheduled` is shared with
/// the kubelet lifecycle set: the scheduler sets it at bind time and the kubelet
/// keeps re-asserting it, so both owners are authoritative for it.
fn is_scheduler_owned_condition(condition_type: &str) -> bool {
    matches!(condition_type, "DisruptionTarget" | "PodScheduled")
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

    /// Round-trip a bare Pod `.status` object through the real K8s protobuf wire
    /// codec: wrap it in a full Pod, encode to protobuf bytes, decode back to a
    /// JSON Value, and return the decoded `.status`. This exercises the actual
    /// `encode_protobuf`/`decode_protobuf` path the raft apply layer uses, not a
    /// JSON-string copy.
    fn status_through_protobuf(status: &Value) -> Value {
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "p", "namespace": "default"},
            "spec": {"containers": [{"name": "app", "image": "busybox"}]},
            "status": status,
        });
        let bytes = crate::protobuf::encode_protobuf(&pod).expect("encode pod to protobuf");
        let decoded = crate::protobuf::decode_protobuf(&bytes).expect("decode pod from protobuf");
        let mut s = decoded.get("status").cloned().unwrap_or_else(|| json!({}));
        if let Some(o)=s.as_object_mut(){o.remove("conditions");}
        s
    }

    #[test]
    fn pod_status_merge_json_and_protobuf_paths_match() {
        // The JSON apply path receives the incoming status verbatim; the
        // protobuf raft-apply path receives the same status after a real
        // protobuf encode/decode round-trip. Running the SAME merge with the
        // SAME owner on both must produce byte-identical results, or a genuine
        // JSON-vs-protobuf codec divergence would make this fail.
        let current = json!({
            "apiVersion": "v1", "kind": "Pod",
            "status": {
                "phase": "Succeeded",
                "conditions": [
                    {"type": "Ready", "status": "True", "lastTransitionTime": "2026-06-25T00:00:00Z"},
                    {"type": "DisruptionTarget", "status": "True", "reason": "PreemptionByScheduler", "lastTransitionTime": "2026-06-25T00:00:00Z"}
                ],
                "containerStatuses": [{
                    "name": "app",
                    "image": "busybox",
                    "containerID": "containerd://ctr-1",
                    "restartCount": 0,
                    "ready": false,
                    "started": false,
                    "state": {"terminated": {"exitCode": 0, "reason": "Completed"}}
                }]
            }
        });

        // Incoming kubelet snapshot that omits DisruptionTarget and carries a
        // stale ContainerCreating waiting state — both protections must engage.
        let incoming_status = json!({
            "phase": "Pending",
            "conditions": [
                {"type": "Ready", "status": "False", "lastTransitionTime": "2026-06-25T00:00:01Z"}
            ],
            "containerStatuses": [{
                "name": "app",
                "image": "busybox",
                "containerID": "containerd://ctr-1",
                "restartCount": 0,
                "ready": false,
                "started": false,
                "state": {"waiting": {"reason": "ContainerCreating"}}
            }]
        });

        // JSON path: incoming used verbatim.
        let mut incoming_json = incoming_status.clone();
        merge_pod_status_for_update(
            "v1",
            "Pod",
            &current,
            &mut incoming_json,
            PodStatusOwner::KubeletRuntime,
        );

        // Protobuf path: incoming round-tripped through the real protobuf codec.
        let mut incoming_proto = status_through_protobuf(&incoming_status);
        merge_pod_status_for_update(
            "v1",
            "Pod",
            &current,
            &mut incoming_proto,
            PodStatusOwner::KubeletRuntime,
        );

        // Sanity: the protobuf round-trip must itself preserve the fields the
        // merge depends on, otherwise the parity assertion below is vacuous.
        assert_eq!(
            incoming_proto.pointer("/containerStatuses/0/containerID"),
            Some(&json!("containerd://ctr-1")),
            "protobuf round-trip must preserve containerID: {incoming_proto:?}"
        );

        assert_eq!(
            incoming_json, incoming_proto,
            "JSON and protobuf apply paths must produce identical merge results"
        );

        // And the merge must have actually done its job on both paths.
        assert!(
            incoming_json
                .pointer("/conditions")
                .and_then(|v| v.as_array())
                .unwrap()
                .iter()
                .any(|c| c.get("type").and_then(|v| v.as_str()) == Some("DisruptionTarget")),
            "DisruptionTarget preserved on both paths: {incoming_json:?}"
        );
        assert!(
            incoming_json
                .pointer("/containerStatuses/0/state/terminated")
                .is_some(),
            "terminal state preserved on both paths: {incoming_json:?}"
        );
    }
}
