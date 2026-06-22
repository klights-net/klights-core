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
pub fn preserve_status_subresource_on_main_update(
    api_version: &str,
    kind: &str,
    current: &Value,
    proposed: &mut Value,
) {
    if !has_builtin_status_subresource(api_version, kind) {
        return;
    }

    let Some(obj) = proposed.as_object_mut() else {
        return;
    };
    if let Some(status) = current.get("status").cloned() {
        obj.insert("status".to_string(), status);
    } else {
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

/// Delegate to the central Pod status merge policy.
///
/// All kubelet-status write paths (raft apply, cluster-replace merge,
/// replicated apply, sqlite/redb appliers) historically called this helper
/// for the ad hoc condition-preservation. They now route through the single
/// DRY policy in [`crate::pod_status_merge`], which additionally preserves
/// terminal/confirmed runtime state over a stale `ContainerCreating`
/// snapshot — this thin wrapper keeps the existing call sites working while
/// consolidating the rules in one place.
pub fn preserve_non_kubelet_pod_conditions_on_kubelet_status_update(
    api_version: &str,
    kind: &str,
    current: &Value,
    status: &mut Value,
) {
    crate::pod_status_merge::merge_pod_status_for_update(
        api_version,
        kind,
        current,
        status,
        crate::pod_status_merge::PodStatusUpdateSource::KubeletRuntime,
    );
}
