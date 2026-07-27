//! Shared domain types for klights.

pub mod field_selector;
pub mod ip;
pub mod json_patch;
pub mod label_selector;
pub mod network;
pub mod pod_status_merge;
pub mod quantity;
pub mod resource_semantics;
pub mod rtt_estimator;

use std::fmt;

/// A client certificate already accepted by a TLS transport.
///
/// This transport credential is shared by the API and internal RPC adapters;
/// auth policy derives identities from the DER bytes through focused ports.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TlsClientCertificate(pub Vec<u8>);

/// Common name used by the extension API server request-header client.
pub const APISERVICE_PROXY_COMMON_NAME: &str = "system:klights:apiservice-proxy";

pub const DEFAULT_SERVICE_ACCOUNT_TOKEN_EXPIRATION_SECONDS: i64 = 3_600;
pub const MIN_SERVICE_ACCOUNT_TOKEN_EXPIRATION_SECONDS: i64 = 600;
pub const MAX_SERVICE_ACCOUNT_TOKEN_EXPIRATION_SECONDS: i64 = 365 * 24 * 3_600;

pub fn normalize_service_account_token_expiration_seconds(requested: Option<i64>) -> i64 {
    requested
        .unwrap_or(DEFAULT_SERVICE_ACCOUNT_TOKEN_EXPIRATION_SECONDS)
        .clamp(
            MIN_SERVICE_ACCOUNT_TOKEN_EXPIRATION_SECONDS,
            MAX_SERVICE_ACCOUNT_TOKEN_EXPIRATION_SECONDS,
        )
}

pub use field_selector::{
    FieldRequirement, FieldSelector, FieldSelectorOperator, FieldSelectorParseError,
    default_field_value, resolve_field_value,
};
pub use ip::first_usable_ipv4;
pub use json_patch::apply_merge_patch;
pub use label_selector::{
    LabelRequirement, LabelSelector, LabelSelectorParseError, parse_label_selector, split_selector,
};
pub use network::{
    ClusterCidr, DATAPLANE_ENCRYPTION_ANNOTATION, DATAPLANE_ENDPOINT_ANNOTATION,
    DATAPLANE_MODE_ANNOTATION, DATAPLANE_PORT_ANNOTATION, DATAPLANE_PUBLIC_KEY_ANNOTATION,
    HostPortRange, NodeName, NodePeerMode, NodePeerModeParseError, PodHostPortProtocol,
    PodHostPortSpec, PodSubnet, parse_node_peer_mode, pod_host_port_specs,
    set_node_dataplane_annotations,
};
pub use pod_status_merge::{
    PodStatusOwner, PodStatusPatch, merge_owned_and_preserved_conditions,
    merge_pod_status_for_update,
};
pub use quantity::{
    calculate_pod_effective_resource_for_key, format_cpu_milli, format_memory_bytes,
    format_resource_quantity, is_binary_quantity_resource, parse_cpu_milli,
    parse_decimal_si_quantity, parse_memory_bytes, parse_resource_quantity,
};
pub use resource_semantics::{
    has_builtin_status_subresource, is_pod_delete_mark_patch, is_zero_grace_pod_delete_mark_patch,
    mark_terminating_pod_unready_at, pod_delete_mark_patch_without_status,
    preserve_status_subresource_on_main_update,
};
pub use rtt_estimator::{RTT_DEFAULT_MS, RttEstimator};

/// API resource identity used by leader queries and API mutations.
///
/// `api_version` deliberately retains Kubernetes' current combined
/// group/version representation (for example `v1` or `apps/v1`). Packet 3.1 is
/// a behavior-preserving ownership move, so construction preserves the input
/// strings exactly instead of introducing new validation policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourceKey {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
}

impl ResourceKey {
    pub fn new(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        namespace: Option<String>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            api_version: api_version.into(),
            kind: kind.into(),
            namespace,
            name: name.into(),
        }
    }
}

/// UID-qualified Pod identity shared across service and adapter boundaries.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PodIdentity {
    pub namespace: String,
    pub name: String,
    pub uid: String,
}

impl PodIdentity {
    pub fn new(namespace: &str, name: &str, uid: &str) -> Self {
        Self {
            namespace: namespace.to_string(),
            name: name.to_string(),
            uid: uid.to_string(),
        }
    }
}

impl fmt::Display for PodIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{} uid={}", self.namespace, self.name, self.uid)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use serde_json::json;

    use super::{
        PodIdentity, PodStatusOwner, ResourceKey, apply_merge_patch,
        is_zero_grace_pod_delete_mark_patch, mark_terminating_pod_unready_at,
        merge_pod_status_for_update,
    };

    #[test]
    fn resource_key_preserves_current_identity_strings() {
        let core = ResourceKey::new("v1", "Pod", Some("default".to_string()), "web");
        assert_eq!(core.api_version, "v1");
        assert_eq!(core.kind, "Pod");
        assert_eq!(core.namespace.as_deref(), Some("default"));
        assert_eq!(core.name, "web");

        let grouped = ResourceKey::new("apps/v1", "Deployment", None, "controller");
        assert_eq!(
            grouped,
            ResourceKey {
                api_version: "apps/v1".to_string(),
                kind: "Deployment".to_string(),
                namespace: None,
                name: "controller".to_string(),
            }
        );

        let arbitrary = ResourceKey::new("group/version/raw", "kind/raw", None, "");
        assert_eq!(arbitrary.api_version, "group/version/raw");
        assert_eq!(arbitrary.kind, "kind/raw");
        assert_eq!(arbitrary.name, "");
        assert_ne!(
            ResourceKey::new("v1", "Namespace", None, "same"),
            ResourceKey::new("v1", "Namespace", Some(String::new()), "same")
        );
    }

    #[test]
    fn pod_identity_new_preserves_fields() {
        let identity = PodIdentity::new("ns1", "podA", "uid-xyz");
        assert_eq!(identity.namespace, "ns1");
        assert_eq!(identity.name, "podA");
        assert_eq!(identity.uid, "uid-xyz");
    }

    #[test]
    fn pod_identity_display_format_is_stable() {
        let identity = PodIdentity::new("kube-system", "coredns", "uid-1");
        assert_eq!(identity.to_string(), "kube-system/coredns uid=uid-1");
    }

    #[test]
    fn pod_identity_equality_is_structural() {
        assert_eq!(
            PodIdentity::new("a", "b", "c"),
            PodIdentity::new("a", "b", "c"),
        );
        assert_ne!(
            PodIdentity::new("a", "b", "c"),
            PodIdentity::new("a", "b", "d"),
        );
    }

    #[test]
    fn pod_identity_uid_participates_in_hash_identity() {
        let first = PodIdentity::new("kube-system", "coredns", "uid-1");
        let replacement = PodIdentity::new("kube-system", "coredns", "uid-2");

        let identities = HashSet::from([first.clone(), replacement.clone()]);
        assert_eq!(identities.len(), 2);
        assert!(identities.contains(&first));
        assert!(identities.contains(&replacement));
    }

    #[test]
    fn canonical_merge_patch_contract_is_infallible() {
        let mut resource = json!({"metadata": {"labels": {"keep": "yes", "drop": "yes"}}});
        apply_merge_patch(
            &mut resource,
            &json!({"metadata": {"labels": {"drop": null, "new": "yes"}}}),
        );
        assert_eq!(
            resource,
            json!({"metadata": {"labels": {"keep": "yes", "new": "yes"}}})
        );
    }

    #[test]
    fn canonical_pod_delete_and_readiness_contract_is_stable() {
        let patch = json!({
            "metadata": {
                "deletionTimestamp": "2026-07-15T00:00:00Z",
                "deletionGracePeriodSeconds": 0
            },
            "status": {"phase": "Running"}
        });
        assert!(is_zero_grace_pod_delete_mark_patch("v1", "Pod", &patch));

        let mut pod = json!({"status": {"conditions": [], "containerStatuses": [{"ready": true}]}});
        mark_terminating_pod_unready_at(&mut pod, "2026-07-15T00:00:00Z");
        assert_eq!(
            pod.pointer("/status/containerStatuses/0/ready"),
            Some(&json!(false))
        );
        assert_eq!(
            pod.pointer("/status/conditions/0/lastTransitionTime"),
            Some(&json!("2026-07-15T00:00:00Z"))
        );
    }

    #[test]
    fn canonical_pod_status_owner_preserves_scheduler_condition() {
        let current = json!({"status": {"conditions": [
            {"type": "Ready", "status": "True"},
            {"type": "DisruptionTarget", "status": "True"}
        ]}});
        let mut incoming = json!({"conditions": [{"type": "Ready", "status": "False"}]});
        merge_pod_status_for_update(
            "v1",
            "Pod",
            &current,
            &mut incoming,
            PodStatusOwner::KubeletRuntime,
        );
        assert!(incoming["conditions"].as_array().is_some_and(|conditions| {
            conditions.iter().any(|condition| {
                condition.get("type").and_then(|value| value.as_str()) == Some("DisruptionTarget")
            })
        }));
    }
}
