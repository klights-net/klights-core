/// Determine if a resource kind is namespace-scoped or cluster-scoped.
/// Returns true for namespaced resources, false for cluster-scoped.
pub(super) fn is_namespaced(kind: &str) -> bool {
    !matches!(
        kind,
        "APIService"
            | "CSIDriver"
            | "CSINode"
            | "CertificateSigningRequest"
            | "ClusterRole"
            | "ClusterRoleBinding"
            | "CustomResourceDefinition"
            | "FlowSchema"
            | "IPAddress"
            | "IngressClass"
            | "MutatingWebhookConfiguration"
            | "Namespace"
            | "Node"
            | "PersistentVolume"
            | "PriorityClass"
            | "PriorityLevelConfiguration"
            | "RuntimeClass"
            | "ServiceCIDR"
            | "StorageClass"
            | "ValidatingAdmissionPolicy"
            | "ValidatingAdmissionPolicyBinding"
            | "ValidatingWebhookConfiguration"
            | "VolumeAttachment"
    )
}

pub(super) fn is_builtin_api_version(api_version: &str) -> bool {
    matches!(
        api_version,
        "v1" | "apps/v1"
            | "autoscaling/v1"
            | "autoscaling/v2"
            | "batch/v1"
            | "certificates.k8s.io/v1"
            | "coordination.k8s.io/v1"
            | "discovery.k8s.io/v1"
            | "events.k8s.io/v1"
            | "networking.k8s.io/v1"
            | "node.k8s.io/v1"
            | "policy/v1"
            | "rbac.authorization.k8s.io/v1"
            | "scheduling.k8s.io/v1"
            | "storage.k8s.io/v1"
            | "authentication.k8s.io/v1"
            | "authorization.k8s.io/v1"
            | "admissionregistration.k8s.io/v1"
            | "apiregistration.k8s.io/v1"
            | "apiextensions.k8s.io/v1"
            | "flowcontrol.apiserver.k8s.io/v1"
    )
}

/// Dynamic/custom resources can be either cluster-scoped or namespaced depending on CRD scope.
/// When namespace is None we must not blindly coerce to namespaced default.
pub(super) fn is_dynamic_custom_resource(api_version: &str, kind: &str) -> bool {
    !is_builtin_api_version(api_version) && !matches!(kind, "Namespace" | "Event")
}

pub(super) fn use_namespaced_table(
    api_version: &str,
    kind: &str,
    namespace: &Option<&str>,
) -> bool {
    if is_dynamic_custom_resource(api_version, kind) {
        return namespace.is_some();
    }
    is_namespaced(kind)
}

#[cfg(test)]
mod tests {
    use super::is_namespaced;

    #[test]
    fn generated_cluster_resources_are_cluster_scoped() {
        for kind in [
            "APIService",
            "CSIDriver",
            "CSINode",
            "CertificateSigningRequest",
            "ClusterRole",
            "ClusterRoleBinding",
            "CustomResourceDefinition",
            "FlowSchema",
            "IPAddress",
            "IngressClass",
            "MutatingWebhookConfiguration",
            "Namespace",
            "Node",
            "PersistentVolume",
            "PriorityClass",
            "PriorityLevelConfiguration",
            "RuntimeClass",
            "ServiceCIDR",
            "StorageClass",
            "ValidatingAdmissionPolicy",
            "ValidatingAdmissionPolicyBinding",
            "ValidatingWebhookConfiguration",
            "VolumeAttachment",
        ] {
            assert!(!is_namespaced(kind), "{kind} must be cluster-scoped");
        }
    }
}
