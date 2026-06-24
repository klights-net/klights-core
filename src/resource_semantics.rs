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
        if let Some(obj) = proposed.as_object_mut() {
            obj.insert("status".to_string(), status);
        }
    } else if let Some(obj) = proposed.as_object_mut() {
        obj.remove("status");
    }
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
            "deletionTimestamp" | "deletionGracePeriodSeconds"
        )
    })
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

pub fn mark_terminating_pod_unready(data: &mut Value) {
    let now = crate::utils::k8s_timestamp();
    mark_terminating_pod_unready_at(data, &now);
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
