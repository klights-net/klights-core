//! Typed authorization request attributes.
//!
//! Represents a Kubernetes authorization decision request as immutable typed
//! fields. Handlers build this through a shared helper; the authorizer chain
//! evaluates it without knowing HTTP details.

/// Whether this is a resource or non-resource request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RequestKind {
    Resource,
    NonResource,
}

/// Immutable Kubernetes authorization attributes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorizationRequest {
    pub kind: RequestKind,
    pub verb: String,
    pub api_group: Option<String>,
    pub api_version: Option<String>,
    pub resource: Option<String>,
    pub subresource: Option<String>,
    pub namespace: Option<String>,
    pub name: Option<String>,
    pub non_resource_url: Option<String>,
    pub field_selector: Option<String>,
    pub label_selector: Option<String>,
    pub resource_request: bool,
}

impl AuthorizationRequest {
    /// Build a resource authorization request.
    pub fn resource(
        verb: &str,
        api_group: &str,
        api_version: &str,
        resource: &str,
        subresource: Option<&str>,
        namespace: Option<&str>,
        name: Option<&str>,
    ) -> Self {
        Self {
            kind: RequestKind::Resource,
            verb: verb.to_string(),
            api_group: if api_group.is_empty() {
                None
            } else {
                Some(api_group.to_string())
            },
            api_version: if api_version.is_empty() {
                None
            } else {
                Some(api_version.to_string())
            },
            resource: Some(resource.to_string()),
            subresource: subresource.map(|s| s.to_string()),
            namespace: namespace.map(|s| s.to_string()),
            name: name.map(|s| s.to_string()),
            non_resource_url: None,
            field_selector: None,
            label_selector: None,
            resource_request: true,
        }
    }

    /// Build a non-resource URL authorization request.
    pub fn non_resource(verb: &str, url: &str) -> Self {
        Self {
            kind: RequestKind::NonResource,
            verb: verb.to_string(),
            api_group: None,
            api_version: None,
            resource: None,
            subresource: None,
            namespace: None,
            name: None,
            non_resource_url: Some(url.to_string()),
            field_selector: None,
            label_selector: None,
            resource_request: false,
        }
    }

    /// Attach a field selector (used for RBAC resourceNames and node list/watch).
    pub fn with_field_selector(mut self, selector: Option<String>) -> Self {
        self.field_selector = selector;
        self
    }

    /// Attach a label selector.
    pub fn with_label_selector(mut self, selector: Option<String>) -> Self {
        self.label_selector = selector;
        self
    }
}

/// Map a Kubernetes Kind string (e.g. "Pod", "Deployment") to its URL
/// resource name (e.g. "pods", "deployments"). This is needed because
/// generated handlers receive the `kind` from route registration but RBAC
/// rules use lowercase plural URL names.
pub fn kind_to_resource_name(kind: &str) -> &str {
    match kind {
        // core/v1
        "Pod" => "pods",
        "Node" => "nodes",
        "Service" => "services",
        "ConfigMap" => "configmaps",
        "Secret" => "secrets",
        "Namespace" => "namespaces",
        "Event" => "events",
        "ServiceAccount" => "serviceaccounts",
        "PersistentVolume" => "persistentvolumes",
        "PersistentVolumeClaim" => "persistentvolumeclaims",
        "Endpoints" => "endpoints",
        "PodTemplate" => "podtemplates",
        "ReplicationController" => "replicationcontrollers",
        "ResourceQuota" => "resourcequotas",
        "LimitRange" => "limitranges",
        "PodBinding" => "podbindings",
        // apps/v1
        "Deployment" => "deployments",
        "ReplicaSet" => "replicasets",
        "StatefulSet" => "statefulsets",
        "DaemonSet" => "daemonsets",
        "ControllerRevision" => "controllerrevisions",
        // batch/v1
        "Job" => "jobs",
        "CronJob" => "cronjobs",
        // certificates.k8s.io/v1
        "CertificateSigningRequest" => "certificatesigningrequests",
        // rbac.authorization.k8s.io/v1
        "ClusterRole" => "clusterroles",
        "ClusterRoleBinding" => "clusterrolebindings",
        "Role" => "roles",
        "RoleBinding" => "rolebindings",
        // coordination.k8s.io/v1
        "Lease" => "leases",
        // networking.k8s.io/v1
        "Ingress" => "ingresses",
        "IngressClass" => "ingressclasses",
        "NetworkPolicy" => "networkpolicies",
        // discovery.k8s.io/v1
        "EndpointSlice" => "endpointslices",
        // policy/v1
        "PodDisruptionBudget" => "poddisruptionbudgets",
        // autoscaling
        "HorizontalPodAutoscaler" => "horizontalpodautoscalers",
        // storage.k8s.io
        "CSIDriver" => "csidrivers",
        "CSINode" => "csinodes",
        "CSIStorageCapacity" => "csistoragecapacities",
        "StorageClass" => "storageclasses",
        "VolumeAttachment" => "volumeattachments",
        // apiextensions.k8s.io/v1
        "CustomResourceDefinition" => "customresourcedefinitions",
        // apiregistration.k8s.io/v1
        "APIService" => "apiservices",
        // node.k8s.io/v1
        "RuntimeClass" => "runtimeclasses",
        // authorization.k8s.io/v1
        "SubjectAccessReview" => "subjectaccessreviews",
        "SelfSubjectAccessReview" => "selfsubjectaccessreviews",
        "LocalSubjectAccessReview" => "localsubjectaccessreviews",
        "SelfSubjectRulesReview" => "selfsubjectrulesreviews",
        "TokenReview" => "tokenreviews",
        // flowcontrol.apiserver.k8s.io/v1
        "FlowSchema" => "flowschemas",
        "PriorityLevelConfiguration" => "prioritylevelconfigurations",
        // admissionregistration.k8s.io/v1
        "ValidatingAdmissionPolicy" => "validatingadmissionpolicies",
        "ValidatingAdmissionPolicyBinding" => "validatingadmissionpolicybindings",
        "ValidatingWebhookConfiguration" => "validatingwebhookconfigurations",
        "MutatingWebhookConfiguration" => "mutatingwebhookconfigurations",
        // events.k8s.io/v1
        // Event already mapped to "events" above
        // scheduling/v1
        "PriorityClass" => "priorityclasses",
        // Eviction subresource
        "Eviction" => "pods", // Eviction targets pods
        // networking.k8s.io/v1alpha1
        "ServiceCIDR" => "servicecidrs",
        "IPAddress" => "ipaddresses",
        // TokenRequest subresource
        "TokenRequest" => "serviceaccounts", // TokenRequest targets serviceaccounts
        // Fallback: lowercase + pluralize heuristically for unknown kinds
        _ => kind,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_request_has_correct_fields() {
        let req = AuthorizationRequest::resource(
            "create",
            "certificates.k8s.io",
            "v1",
            "certificatesigningrequests",
            None,
            None,
            None,
        );
        assert_eq!(req.kind, RequestKind::Resource);
        assert_eq!(req.verb, "create");
        assert_eq!(req.api_group.as_deref(), Some("certificates.k8s.io"));
        assert_eq!(req.resource.as_deref(), Some("certificatesigningrequests"));
        assert!(req.subresource.is_none());
        assert!(req.namespace.is_none());
        assert!(req.name.is_none());
        assert!(req.resource_request);
    }

    #[test]
    fn resource_request_with_subresource() {
        let req = AuthorizationRequest::resource(
            "update",
            "",
            "v1",
            "pods",
            Some("status"),
            Some("default"),
            Some("my-pod"),
        );
        assert_eq!(req.subresource.as_deref(), Some("status"));
        assert_eq!(req.namespace.as_deref(), Some("default"));
        assert_eq!(req.name.as_deref(), Some("my-pod"));
        assert_eq!(req.api_group, None);
    }

    #[test]
    fn non_resource_request_url() {
        let req = AuthorizationRequest::non_resource("get", "/api/v1");
        assert_eq!(req.kind, RequestKind::NonResource);
        assert_eq!(req.verb, "get");
        assert_eq!(req.non_resource_url.as_deref(), Some("/api/v1"));
        assert!(!req.resource_request);
    }

    #[test]
    fn field_selector_preserved() {
        let req =
            AuthorizationRequest::resource("list", "", "v1", "pods", None, Some("default"), None)
                .with_field_selector(Some("spec.nodeName=tokyo".to_string()));
        assert_eq!(req.field_selector.as_deref(), Some("spec.nodeName=tokyo"));
    }

    // --- Kind-to-resource name mapping tests (Phase 2A Step 1) ---

    #[test]
    fn kind_to_resource_name_maps_core_v1_kinds() {
        let cases = [
            ("Pod", "pods"),
            ("Node", "nodes"),
            ("Service", "services"),
            ("ConfigMap", "configmaps"),
            ("Secret", "secrets"),
            ("Namespace", "namespaces"),
            ("Event", "events"),
            ("ServiceAccount", "serviceaccounts"),
            ("PersistentVolume", "persistentvolumes"),
            ("PersistentVolumeClaim", "persistentvolumeclaims"),
            ("Endpoints", "endpoints"),
            ("ReplicationController", "replicationcontrollers"),
            ("ResourceQuota", "resourcequotas"),
            ("LimitRange", "limitranges"),
        ];
        for (kind, expected) in &cases {
            assert_eq!(
                kind_to_resource_name(kind),
                *expected,
                "kind_to_resource_name({kind}) should be {expected}"
            );
        }
    }

    #[test]
    fn kind_to_resource_name_maps_apps_v1_kinds() {
        let cases = [
            ("Deployment", "deployments"),
            ("ReplicaSet", "replicasets"),
            ("StatefulSet", "statefulsets"),
            ("DaemonSet", "daemonsets"),
            ("ControllerRevision", "controllerrevisions"),
        ];
        for (kind, expected) in &cases {
            assert_eq!(
                kind_to_resource_name(kind),
                *expected,
                "kind_to_resource_name({kind}) should be {expected}"
            );
        }
    }

    #[test]
    fn kind_to_resource_name_maps_rbac_csr_and_other_group_kinds() {
        let cases = [
            ("CertificateSigningRequest", "certificatesigningrequests"),
            ("ClusterRole", "clusterroles"),
            ("ClusterRoleBinding", "clusterrolebindings"),
            ("Role", "roles"),
            ("RoleBinding", "rolebindings"),
            ("Lease", "leases"),
            ("Ingress", "ingresses"),
            ("NetworkPolicy", "networkpolicies"),
            ("EndpointSlice", "endpointslices"),
            ("PodDisruptionBudget", "poddisruptionbudgets"),
            ("HorizontalPodAutoscaler", "horizontalpodautoscalers"),
            ("CustomResourceDefinition", "customresourcedefinitions"),
            ("APIService", "apiservices"),
            ("StorageClass", "storageclasses"),
            ("CSINode", "csinodes"),
            ("CSIDriver", "csidrivers"),
            ("VolumeAttachment", "volumeattachments"),
        ];
        for (kind, expected) in &cases {
            assert_eq!(
                kind_to_resource_name(kind),
                *expected,
                "kind_to_resource_name({kind}) should be {expected}"
            );
        }
    }

    #[test]
    fn kind_to_resource_name_unknown_kind_passes_through() {
        // Unknown kinds are passed through unchanged so custom resources
        // (which use lowercase plural names as kind in some configs) still work.
        assert_eq!(kind_to_resource_name("Widget"), "Widget");
    }
}
