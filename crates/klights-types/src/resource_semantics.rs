use serde_json::Value;

pub fn has_builtin_status_subresource(api_version: &str, kind: &str) -> bool {
    matches!(
        (api_version, kind),
        (
            "admissionregistration.k8s.io/v1",
            "MutatingWebhookConfiguration"
        ) | (
            "admissionregistration.k8s.io/v1",
            "ValidatingWebhookConfiguration"
        ) | (
            "admissionregistration.k8s.io/v1",
            "ValidatingAdmissionPolicy"
        ) | (
            "admissionregistration.k8s.io/v1",
            "ValidatingAdmissionPolicyBinding"
        ) | ("apiextensions.k8s.io/v1", "CustomResourceDefinition")
            | ("apiregistration.k8s.io/v1", "APIService")
            | ("apps/v1", "DaemonSet")
            | ("apps/v1", "Deployment")
            | ("apps/v1", "ReplicaSet")
            | ("apps/v1", "StatefulSet")
            | ("autoscaling/v1", "HorizontalPodAutoscaler")
            | ("autoscaling/v2", "HorizontalPodAutoscaler")
            | ("batch/v1", "CronJob")
            | ("batch/v1", "Job")
            | ("certificates.k8s.io/v1", "CertificateSigningRequest")
            | ("flowcontrol.apiserver.k8s.io/v1", "FlowSchema")
            | (
                "flowcontrol.apiserver.k8s.io/v1",
                "PriorityLevelConfiguration"
            )
            | ("networking.k8s.io/v1", "Ingress")
            | ("policy/v1", "PodDisruptionBudget")
            | ("storage.k8s.io/v1", "CSINode")
            | ("storage.k8s.io/v1", "VolumeAttachment")
            | ("v1", "Node")
            | ("v1", "Namespace")
            | ("v1", "PersistentVolume")
            | ("v1", "PersistentVolumeClaim")
            | ("v1", "Pod")
            | ("v1", "ReplicationController")
            | ("v1", "ResourceQuota")
            | ("v1", "Service")
    )
}

/// Main-resource writes must not mutate `.status` for built-in resources
/// that expose a status subresource. The status endpoint owns that field.
///
/// For Pods the live status is preserved verbatim, but any scheduler-owned
/// condition the main write was itself setting (e.g. `DisruptionTarget` from
/// scheduler preemption) is folded back in through the central Pod status
/// merge. Without this, a leader-side preemption `UpdateResource` replicated
/// through raft would have its `DisruptionTarget` condition stripped whenever a
/// newer kubelet status snapshot landed on the live row first — the very race
/// the central merge exists to close.
pub fn preserve_status_subresource_on_main_update(
    api_version: &str,
    kind: &str,
    current: &Value,
    proposed: &mut Value,
) {
    if !has_builtin_status_subresource(api_version, kind) {
        return;
    }

    if !proposed.is_object() {
        return;
    }
    let terminating_transition_time = (api_version == "v1"
        && kind == "Pod"
        && proposed
            .pointer("/metadata/deletionTimestamp")
            .is_some_and(|timestamp| !timestamp.is_null()))
    .then(|| pod_delete_mark_transition_time(proposed))
    .flatten()
    .map(str::to_string);
    if let Some(mut status) = current.get("status").cloned() {
        // Carry scheduler-owned Pod conditions (DisruptionTarget, ...) that the
        // main write was setting into the preserved live status. The central
        // merge treats `proposed` as the source of non-kubelet conditions to
        // preserve and `status` (the live snapshot) as the incoming target, so
        // a preemption termination's DisruptionTarget survives even when the
        // live kubelet status omits it. The `UserStatusSubresource` source is
        // used intentionally: only condition preservation applies, not the
        // kubelet terminal-state rewrite, since this is a main-resource update
        // preserving the authoritative live status — not a kubelet snapshot.
        crate::pod_status_merge::merge_pod_status_for_update(
            api_version,
            kind,
            proposed,
            &mut status,
            crate::pod_status_merge::PodStatusOwner::ApiStatusSubresource,
        );
        carry_scheduler_bind_pod_scheduled_condition(
            api_version,
            kind,
            current,
            proposed,
            &mut status,
        );
        if let Some(obj) = proposed.as_object_mut() {
            obj.insert("status".to_string(), status);
        }
        if let Some(transition_time) = terminating_transition_time.as_deref() {
            mark_terminating_pod_unready_at(proposed, transition_time);
        }
    } else if let Some(obj) = proposed.as_object_mut() {
        obj.remove("status");
    }
}

fn carry_scheduler_bind_pod_scheduled_condition(
    api_version: &str,
    kind: &str,
    current: &Value,
    proposed: &Value,
    status: &mut Value,
) {
    if api_version != "v1" || kind != "Pod" {
        return;
    }
    if !is_pod_bind_transition(current, proposed) {
        return;
    }
    let Some(proposed_condition) = pod_condition(proposed, "PodScheduled") else {
        return;
    };
    if proposed_condition.get("status").and_then(Value::as_str) != Some("True") {
        return;
    }
    upsert_pod_condition(status, proposed_condition.clone());
}

fn is_pod_bind_transition(current: &Value, proposed: &Value) -> bool {
    pod_node_name(current).is_none() && pod_node_name(proposed).is_some()
}

fn pod_node_name(pod: &Value) -> Option<&str> {
    pod.pointer("/spec/nodeName")
        .and_then(Value::as_str)
        .filter(|node_name| !node_name.is_empty())
}

fn pod_condition<'a>(pod: &'a Value, condition_type: &str) -> Option<&'a Value> {
    pod.pointer("/status/conditions")
        .and_then(Value::as_array)
        .and_then(|conditions| {
            conditions.iter().find(|condition| {
                condition.get("type").and_then(Value::as_str) == Some(condition_type)
            })
        })
}

fn upsert_pod_condition(status: &mut Value, condition: Value) {
    let Some(status_object) = status.as_object_mut() else {
        return;
    };
    let condition_type = condition
        .get("type")
        .and_then(Value::as_str)
        .map(str::to_string);
    let Some(condition_type) = condition_type else {
        return;
    };
    let conditions = status_object
        .entry("conditions".to_string())
        .or_insert_with(|| Value::Array(Vec::new()));
    if !conditions.is_array() {
        *conditions = Value::Array(Vec::new());
    }
    let Some(conditions) = conditions.as_array_mut() else {
        return;
    };
    conditions.retain(|existing| {
        existing.get("type").and_then(Value::as_str) != Some(condition_type.as_str())
    });
    conditions.push(condition);
}

pub fn is_pod_delete_mark_patch(api_version: &str, kind: &str, patch: &Value) -> bool {
    if api_version != "v1" || kind != "Pod" {
        return false;
    }
    let Some(patch_obj) = patch.as_object() else {
        return false;
    };
    if !patch_obj
        .keys()
        .all(|key| matches!(key.as_str(), "metadata" | "status"))
    {
        return false;
    }
    let Some(metadata) = patch_obj.get("metadata").and_then(Value::as_object) else {
        return false;
    };
    if metadata
        .get("deletionTimestamp")
        .is_none_or(|timestamp| timestamp.is_null())
    {
        return false;
    }
    metadata.keys().all(|key| {
        matches!(
            key.as_str(),
            "deletionTimestamp" | "deletionGracePeriodSeconds" | "generation"
        )
    })
}

pub fn pod_delete_mark_transition_time(patch: &Value) -> Option<&str> {
    patch
        .pointer("/status/conditions")
        .and_then(Value::as_array)
        .and_then(|conditions| {
            conditions.iter().find(|condition| {
                condition.pointer("/type").and_then(Value::as_str) == Some("Ready")
            })
        })
        .and_then(|condition| condition.pointer("/lastTransitionTime"))
        .and_then(Value::as_str)
}

pub fn is_zero_grace_pod_delete_mark_patch(api_version: &str, kind: &str, patch: &Value) -> bool {
    if !is_pod_delete_mark_patch(api_version, kind, patch) {
        return false;
    }
    patch
        .pointer("/metadata/deletionGracePeriodSeconds")
        .and_then(Value::as_i64)
        == Some(0)
}

pub fn pod_delete_mark_patch_without_status(patch: &Value) -> Value {
    let mut patch = patch.clone();
    if let Some(patch_obj) = patch.as_object_mut() {
        patch_obj.remove("status");
    }
    patch
}

pub fn mark_terminating_pod_unready_at(data: &mut Value, now: &str) {
    let Some(status) = data
        .get_mut("status")
        .and_then(|value| value.as_object_mut())
    else {
        return;
    };

    for status_list_name in ["containerStatuses", "initContainerStatuses"] {
        if let Some(statuses) = status
            .get_mut(status_list_name)
            .and_then(|value| value.as_array_mut())
        {
            for container_status in statuses {
                if let Some(container_status) = container_status.as_object_mut() {
                    container_status.insert("ready".to_string(), serde_json::json!(false));
                }
            }
        }
    }

    let conditions = status
        .entry("conditions".to_string())
        .or_insert_with(|| serde_json::json!([]));
    if !conditions.is_array() {
        *conditions = serde_json::json!([]);
    }
    let Some(conditions) = conditions.as_array_mut() else {
        return;
    };
    for condition_type in ["Ready", "ContainersReady"] {
        upsert_terminating_readiness_condition(conditions, condition_type, now);
    }
}

fn upsert_terminating_readiness_condition(
    conditions: &mut Vec<Value>,
    condition_type: &str,
    now: &str,
) {
    if let Some(condition) = conditions.iter_mut().find(|condition| {
        condition.pointer("/type").and_then(|value| value.as_str()) == Some(condition_type)
    }) && let Some(condition) = condition.as_object_mut()
    {
        let status_changed =
            condition.get("status").and_then(|value| value.as_str()) != Some("False");
        condition.insert("status".to_string(), serde_json::json!("False"));
        condition.insert("reason".to_string(), serde_json::json!("PodTerminating"));
        condition.insert(
            "message".to_string(),
            serde_json::json!("Pod is terminating"),
        );
        if status_changed || !condition.contains_key("lastTransitionTime") {
            condition.insert("lastTransitionTime".to_string(), serde_json::json!(now));
        }
        return;
    }

    conditions.push(serde_json::json!({
        "type": condition_type,
        "status": "False",
        "lastTransitionTime": now,
        "reason": "PodTerminating",
        "message": "Pod is terminating"
    }));
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // T1 red test: privilege boundary — a main-resource update that carries
    // status.podIP/podIPs/hostIP/hostIPs in the request body must NOT leak
    // those fields into the merged status when the live row has none.
    // The request body's .status is a proposed mutation; the live row's
    // .status is authoritative. Since the live row has no IP fields and
    // the merge's back-fill is gated on the owner (ApiStatusSubresource
    // does not back-fill), the merged status must be IP-free.
    //
    // This is the canonical privilege-boundary test: pods and pods/status
    // are separately authorized in RBAC, and status.podIP feeds
    // Endpoints/EndpointSlice reconciliation. A regression here is a
    // privilege-boundary failure, not just a wire deviation.
    #[test]
    fn main_update_status_pod_ip_does_not_leak_when_live_row_has_no_ip() {
        // Live row: Pod exists, no status at all.
        let current = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "test", "namespace": "default", "uid": "uid-1"},
            "spec": {"nodeName": "node-1"}
        });
        // Request body: main-resource update that mistakenly carries
        // status.podIP (a privilege-boundary violation if it leaks).
        let mut proposed = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "test", "namespace": "default", "uid": "uid-1"},
            "spec": {"nodeName": "node-1"},
            "status": {
                "podIP": "10.50.1.20",
                "podIPs": [{"ip": "10.50.1.20"}],
                "hostIP": "10.99.0.14",
                "hostIPs": [{"ip": "10.99.0.14"}]
            }
        });

        preserve_status_subresource_on_main_update("v1", "Pod", &current, &mut proposed);

        let merged_status = proposed.get("status");
        assert!(
            merged_status.is_none() || merged_status.is_some_and(|s| s.get("podIP").is_none()),
            "main-resource update must not inject status.podIP when live row has none: {proposed:?}"
        );
        assert!(
            merged_status.is_none() || merged_status.is_some_and(|s| s.get("podIPs").is_none()),
            "main-resource update must not inject status.podIPs when live row has none: {proposed:?}"
        );
        assert!(
            merged_status.is_none() || merged_status.is_some_and(|s| s.get("hostIP").is_none()),
            "main-resource update must not inject status.hostIP when live row has none: {proposed:?}"
        );
        assert!(
            merged_status.is_none() || merged_status.is_some_and(|s| s.get("hostIPs").is_none()),
            "main-resource update must not inject status.hostIPs when live row has none: {proposed:?}"
        );
    }

    // Same as above, but the live row has DIFFERENT IP values. The request
    // body's values must not overwrite the live row's.
    #[test]
    fn main_update_status_pod_ip_does_not_overwrite_live_row_ip() {
        let current = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "test", "namespace": "default", "uid": "uid-1"},
            "spec": {"nodeName": "node-1"},
            "status": {
                "podIP": "10.50.1.10",
                "podIPs": [{"ip": "10.50.1.10"}],
                "hostIP": "10.99.0.10",
                "hostIPs": [{"ip": "10.99.0.10"}]
            }
        });
        let mut proposed = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "test", "namespace": "default", "uid": "uid-1"},
            "spec": {"nodeName": "node-1"},
            "status": {
                "podIP": "10.50.1.99",
                "podIPs": [{"ip": "10.50.1.99"}],
                "hostIP": "10.99.0.99",
                "hostIPs": [{"ip": "10.99.0.99"}]
            }
        });

        preserve_status_subresource_on_main_update("v1", "Pod", &current, &mut proposed);

        // The merged status must carry the LIVE values, not the request body's.
        assert_eq!(
            proposed.pointer("/status/podIP"),
            Some(&json!("10.50.1.10")),
            "live podIP must not be overwritten by main-resource request body: {proposed:?}"
        );
        assert_eq!(
            proposed.pointer("/status/podIPs/0/ip"),
            Some(&json!("10.50.1.10")),
            "live podIPs must not be overwritten by main-resource request body: {proposed:?}"
        );
        assert_eq!(
            proposed.pointer("/status/hostIP"),
            Some(&json!("10.99.0.10")),
            "live hostIP must not be overwritten by main-resource request body: {proposed:?}"
        );
        assert_eq!(
            proposed.pointer("/status/hostIPs/0/ip"),
            Some(&json!("10.99.0.10")),
            "live hostIPs must not be overwritten by main-resource request body: {proposed:?}"
        );
    }

    // T1 red test: DisruptionTarget carry-back. The existing canonical
    // coverage at lib.rs:250 and lib.rs:289 stays green; this Pod-status
    // variant is local to resource_semantics.rs so the carry-back is
    // tested at the same call site that implements it.
    #[test]
    fn main_update_preserves_disruption_target_condition() {
        // Live row already has DisruptionTarget (set by scheduler preemption).
        let current = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "test", "namespace": "default", "uid": "uid-1"},
            "spec": {"nodeName": "node-1"},
            "status": {
                "phase": "Running",
                "conditions": [
                    {"type": "Ready", "status": "True"},
                    {"type": "DisruptionTarget", "status": "True", "reason": "PreemptionByScheduler"}
                ]
            }
        });
        // Main-resource update that does NOT carry DisruptionTarget.
        let mut proposed = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "test", "namespace": "default", "uid": "uid-1"},
            "spec": {"nodeName": "node-1"}
        });

        preserve_status_subresource_on_main_update("v1", "Pod", &current, &mut proposed);

        let conditions = proposed
            .pointer("/status/conditions")
            .and_then(|v| v.as_array())
            .expect("merged status must have conditions");
        assert!(
            conditions
                .iter()
                .any(|c| { c.get("type").and_then(|v| v.as_str()) == Some("DisruptionTarget") }),
            "DisruptionTarget must survive main-resource update: {proposed:?}"
        );
    }

    // T1 red test: Planned-delete readiness carry-back.
    // Reuse the existing canonical test from lib.rs but exercise it at
    // the resource_semantics call site to verify the merge + mark path.
    #[test]
    fn main_update_planned_delete_preserves_readiness_conditions() {
        let current = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "test", "namespace": "default", "uid": "uid-1"},
            "spec": {"nodeName": "node-1"},
            "status": {
                "phase": "Running",
                "conditions": [{"type": "Ready", "status": "True"}],
                "containerStatuses": [{"name": "app", "ready": true}]
            }
        });
        let mut proposed = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "test",
                "namespace": "default",
                "uid": "uid-1",
                "deletionTimestamp": "2026-07-15T00:00:30Z",
                "deletionGracePeriodSeconds": 30
            },
            "spec": {"nodeName": "node-1"},
            "status": {"conditions": [{
                "type": "Ready",
                "status": "False",
                "reason": "PodTerminating",
                "lastTransitionTime": "2026-07-15T00:00:00Z"
            }]}
        });

        preserve_status_subresource_on_main_update("v1", "Pod", &current, &mut proposed);

        assert_eq!(proposed.pointer("/status/phase"), Some(&json!("Running")));
        assert_eq!(
            proposed.pointer("/status/conditions/0/status"),
            Some(&json!("False")),
            "planned-delete must set Ready=False"
        );
        assert_eq!(
            proposed.pointer("/status/containerStatuses/0/ready"),
            Some(&json!(false)),
            "planned-delete must mark containers unready"
        );
    }
}
