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

pub fn preserve_non_kubelet_pod_conditions_on_kubelet_status_update(
    api_version: &str,
    kind: &str,
    current: &Value,
    status: &mut Value,
) {
    if api_version != "v1" || kind != "Pod" {
        return;
    }

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

    let Some(status_obj) = status.as_object_mut() else {
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

fn is_kubelet_rebuilt_pod_condition(condition_type: &str) -> bool {
    matches!(
        condition_type,
        "PodScheduled" | "Initialized" | "ContainersReady" | "Ready"
    )
}
