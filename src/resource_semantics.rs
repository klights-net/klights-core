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
