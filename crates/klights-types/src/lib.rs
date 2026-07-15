//! Shared domain types for klights.

pub mod label_selector;
pub mod quantity;

use std::fmt;

pub use label_selector::{
    LabelRequirement, LabelSelector, LabelSelectorParseError, parse_label_selector, split_selector,
};
pub use quantity::{
    format_cpu_milli, format_memory_bytes, format_resource_quantity, is_binary_quantity_resource,
    parse_cpu_milli, parse_decimal_si_quantity, parse_memory_bytes, parse_resource_quantity,
};

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

    use super::{PodIdentity, ResourceKey};

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
}
