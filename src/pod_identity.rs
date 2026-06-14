//! Pod identity triple: (namespace, name, uid).
//!
//! Replaces the ad-hoc `(namespace: &str, pod_name: &str, pod_uid: &str)`
//! arg triple that appears in many function signatures across the kubelet,
//! datastore, and networking layers. Using a single struct makes call sites
//! explicit and removes the silent-misorder risk of three same-typed
//! positional args.

use std::fmt;

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
    use super::*;

    #[test]
    fn new_preserves_fields() {
        let id = PodIdentity::new("ns1", "podA", "uid-xyz");
        assert_eq!(id.namespace, "ns1");
        assert_eq!(id.name, "podA");
        assert_eq!(id.uid, "uid-xyz");
    }

    #[test]
    fn display_format_is_stable() {
        let id = PodIdentity::new("kube-system", "coredns", "uid-1");
        assert_eq!(format!("{}", id), "kube-system/coredns uid=uid-1");
    }

    #[test]
    fn equality_is_structural() {
        assert_eq!(
            PodIdentity::new("a", "b", "c"),
            PodIdentity::new("a", "b", "c"),
        );
        assert_ne!(
            PodIdentity::new("a", "b", "c"),
            PodIdentity::new("a", "b", "d"),
        );
    }
}
