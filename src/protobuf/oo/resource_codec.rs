//! Object-safe protobuf codec boundary for Kubernetes resources.
//!
//! All protobuf encode/decode dispatch flows through this registry. The
//! per-resource conversion helpers remain regular functions, but they are
//! implementation details owned by codec objects instead of a parallel
//! free-function dispatch path.

use serde_json::Value;

/// Object-safe trait for encoding/decoding resources in one or more API groups.
///
/// Encode and decode flow through the same object, making the dispatch layer
/// mockable in tests.
///
/// Each codec declares the `(api_version_prefix, kind)` pairs it handles.
/// The api_version_prefix is the API group/version prefix (e.g. `"rbac.authorization.k8s.io"`
/// or `""` for the core group). The registry matches on the full pair to prevent
/// same-kind resources in different API groups from routing to the wrong codec.
pub trait ResourceProtoCodec: Send + Sync {
    /// Return `(api_version_prefix, kind)` pairs this codec handles.
    /// `api_version_prefix` is matched against the leading portion of the
    /// resource's `apiVersion` (the group prefix before `/v1`).
    fn entry_keys(&self) -> &'static [(&'static str, &'static str)];

    /// Decode protobuf bytes into a JSON Value.
    fn decode_to_json(&self, api_version: &str, kind: &str, data: &[u8]) -> anyhow::Result<Value>;

    /// Encode a JSON Value into protobuf bytes.
    fn encode_from_json(
        &self,
        api_version: &str,
        kind: &str,
        value: &Value,
    ) -> anyhow::Result<Vec<u8>>;

    fn handles(&self, api_version: &str, kind: &str) -> bool {
        let prefix = OoCodecRegistry::api_group_prefix(api_version);
        self.entry_keys()
            .iter()
            .any(|(api_prefix, k)| *api_prefix == prefix && *k == kind)
    }
}

/// A registry of OO codecs for Kubernetes resources.
///
/// Entries are keyed by `(api_version_prefix, kind)` — both must match for
/// dispatch. This prevents same-kind resources in different API groups from
/// routing to the wrong codec.
pub struct OoCodecRegistry {
    codecs: Vec<Box<dyn ResourceProtoCodec>>,
}

impl OoCodecRegistry {
    pub fn new(codecs: Vec<Box<dyn ResourceProtoCodec>>) -> Self {
        Self { codecs }
    }

    #[cfg(test)]
    pub fn empty() -> Self {
        Self { codecs: vec![] }
    }

    /// Extract the API group prefix from an apiVersion string.
    /// `"rbac.authorization.k8s.io/v1"` → `"rbac.authorization.k8s.io"`
    /// `"v1"` → `""`
    pub fn api_group_prefix(api_version: &str) -> &str {
        match api_version.rsplit_once('/') {
            Some((group, _)) => group,
            None => "",
        }
    }

    /// Look up the codec that handles a given (apiVersion, kind) pair.
    pub fn lookup(&self, api_version: &str, kind: &str) -> Option<&dyn ResourceProtoCodec> {
        self.codecs
            .iter()
            .find(|c| c.handles(api_version, kind))
            .map(|c| c.as_ref())
    }

    /// Decode protobuf bytes for a kind, returning JSON or an error.
    pub fn decode(&self, api_version: &str, kind: &str, data: &[u8]) -> anyhow::Result<Value> {
        match self.lookup(api_version, kind) {
            Some(codec) => codec.decode_to_json(api_version, kind, data),
            None => anyhow::bail!("no OO codec for {api_version}/{kind}"),
        }
    }

    /// Encode JSON for a kind into protobuf bytes, or error.
    pub fn encode(&self, api_version: &str, kind: &str, value: &Value) -> anyhow::Result<Vec<u8>> {
        match self.lookup(api_version, kind) {
            Some(codec) => codec.encode_from_json(api_version, kind, value),
            None => anyhow::bail!("no OO codec for {api_version}/{kind}"),
        }
    }

    /// Check if any codec in this registry handles the given (apiVersion, kind) pair.
    pub fn handles(&self, api_version: &str, kind: &str) -> bool {
        self.codecs.iter().any(|c| c.handles(api_version, kind))
    }
}

/// Global singleton OO codec registry for all protobuf-supported resources.
///
/// Lazy-initialized on first access.
pub fn global_oo_registry() -> &'static OoCodecRegistry {
    use std::sync::OnceLock;
    static REGISTRY: OnceLock<OoCodecRegistry> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        OoCodecRegistry::new(vec![
            Box::new(super::RbacV1Codec),
            Box::new(super::CertificatesV1Codec),
            Box::new(super::AuthorizationV1Codec),
            Box::new(super::FlowcontrolV1Codec),
            Box::new(super::BuiltinResourceCodec),
        ])
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A mock codec for testing the registry dispatch.
    struct MockCodec {
        entries: &'static [(&'static str, &'static str)],
    }

    impl ResourceProtoCodec for MockCodec {
        fn entry_keys(&self) -> &'static [(&'static str, &'static str)] {
            self.entries
        }

        fn decode_to_json(
            &self,
            _api_version: &str,
            _kind: &str,
            _data: &[u8],
        ) -> anyhow::Result<Value> {
            Ok(json!({"decoded": true, "source": "mock"}))
        }

        fn encode_from_json(
            &self,
            _api_version: &str,
            _kind: &str,
            _value: &Value,
        ) -> anyhow::Result<Vec<u8>> {
            Ok(b"mock-encoded".to_vec())
        }
    }

    #[test]
    fn api_group_prefix_extraction() {
        assert_eq!(OoCodecRegistry::api_group_prefix("v1"), "");
        assert_eq!(
            OoCodecRegistry::api_group_prefix("rbac.authorization.k8s.io/v1"),
            "rbac.authorization.k8s.io"
        );
        assert_eq!(
            OoCodecRegistry::api_group_prefix("certificates.k8s.io/v1"),
            "certificates.k8s.io"
        );
        assert_eq!(OoCodecRegistry::api_group_prefix("apps/v1"), "apps");
    }

    #[test]
    fn empty_registry_returns_none() {
        let registry = OoCodecRegistry::empty();
        assert!(
            registry
                .lookup("rbac.authorization.k8s.io/v1", "ClusterRole")
                .is_none()
        );
    }

    #[test]
    fn registry_dispatches_to_matching_api_version_and_kind() {
        let codec = MockCodec {
            entries: &[
                ("rbac.authorization.k8s.io", "ClusterRole"),
                ("rbac.authorization.k8s.io", "ClusterRoleBinding"),
            ],
        };
        let registry = OoCodecRegistry::new(vec![Box::new(codec)]);
        assert!(
            registry
                .lookup("rbac.authorization.k8s.io/v1", "ClusterRole")
                .is_some()
        );
        assert!(
            registry
                .lookup("rbac.authorization.k8s.io/v1", "ClusterRoleBinding")
                .is_some()
        );
        // Same kind, different API group → not found
        assert!(registry.lookup("v1", "ClusterRole").is_none());
        assert!(registry.lookup("v1", "Pod").is_none());
    }

    #[test]
    fn registry_rejects_same_kind_in_wrong_api_group() {
        // Phase 2B: same kind name in different API groups must not collide.
        // This test verifies the registry distinguishes by (apiVersion, kind).
        let rbac_codec = MockCodec {
            entries: &[("rbac.authorization.k8s.io", "ClusterRole")],
        };
        let fake_codec = MockCodec {
            entries: &[("fake.io", "ClusterRole")],
        };
        let registry = OoCodecRegistry::new(vec![Box::new(rbac_codec), Box::new(fake_codec)]);

        // rbac.authorization.k8s.io/v1 → rbac_codec
        let result = registry
            .decode("rbac.authorization.k8s.io/v1", "ClusterRole", b"rbac-data")
            .unwrap();
        assert_eq!(result["decoded"], true);

        // fake.io/v1 → fake_codec
        let result = registry
            .decode("fake.io/v1", "ClusterRole", b"fake-data")
            .unwrap();
        assert_eq!(result["decoded"], true);

        // v1 (core) ClusterRole → not found (no codec registered for core "ClusterRole")
        assert!(registry.lookup("v1", "ClusterRole").is_none());
    }

    #[test]
    fn registry_decode_routes_to_correct_codec() {
        let rbac_codec = MockCodec {
            entries: &[("rbac.authorization.k8s.io", "ClusterRole")],
        };
        let csr_codec = MockCodec {
            entries: &[("certificates.k8s.io", "CertificateSigningRequest")],
        };
        let registry = OoCodecRegistry::new(vec![Box::new(rbac_codec), Box::new(csr_codec)]);

        let result = registry
            .decode("rbac.authorization.k8s.io/v1", "ClusterRole", b"anything")
            .unwrap();
        assert_eq!(result["decoded"], true);
        assert_eq!(result["source"], "mock");
    }

    #[test]
    fn registry_encode_routes_to_correct_codec() {
        let codec = MockCodec {
            entries: &[("certificates.k8s.io", "CertificateSigningRequest")],
        };
        let registry = OoCodecRegistry::new(vec![Box::new(codec)]);

        let result = registry
            .encode(
                "certificates.k8s.io/v1",
                "CertificateSigningRequest",
                &json!({"test": true}),
            )
            .unwrap();
        assert_eq!(result, b"mock-encoded");
    }

    #[test]
    fn registry_handles_returns_true_for_known_pair() {
        let codec = MockCodec {
            entries: &[("rbac.authorization.k8s.io", "ClusterRole")],
        };
        let registry = OoCodecRegistry::new(vec![Box::new(codec)]);
        assert!(registry.handles("rbac.authorization.k8s.io/v1", "ClusterRole"));
        // Same kind name in wrong API group
        assert!(!registry.handles("v1", "ClusterRole"));
        // Different kind
        assert!(!registry.handles("rbac.authorization.k8s.io/v1", "Pod"));
        assert!(!registry.handles("v1", "Pod"));
    }

    #[test]
    fn unknown_kind_decode_returns_error() {
        let registry = OoCodecRegistry::empty();
        let result = registry.decode("v1", "Pod", b"data");
        assert!(result.is_err());
    }

    #[test]
    fn unknown_kind_encode_returns_error() {
        let registry = OoCodecRegistry::empty();
        let result = registry.encode("v1", "Pod", &json!({}));
        assert!(result.is_err());
    }

    #[test]
    fn core_api_v1_matches_empty_prefix() {
        let codec = MockCodec {
            entries: &[("", "Pod")],
        };
        let registry = OoCodecRegistry::new(vec![Box::new(codec)]);
        assert!(registry.handles("v1", "Pod"));
        assert!(!registry.handles("apps/v1", "Pod"));
    }

    #[test]
    fn global_registry_handles_all_existing_builtin_protobuf_groups() {
        let registry = global_oo_registry();
        for (api_version, kind) in [
            ("v1", "Pod"),
            ("v1", "PodList"),
            ("apps/v1", "Deployment"),
            ("apps/v1", "ControllerRevisionList"),
            ("batch/v1", "CronJob"),
            ("authentication.k8s.io/v1", "TokenReview"),
            ("apiregistration.k8s.io/v1", "APIService"),
            ("apiextensions.k8s.io/v1", "CustomResourceDefinition"),
            ("coordination.k8s.io/v1", "Lease"),
            ("scheduling.k8s.io/v1", "PriorityClass"),
            ("storage.k8s.io/v1", "VolumeAttachment"),
            ("node.k8s.io/v1", "RuntimeClass"),
            ("policy/v1", "PodDisruptionBudget"),
            ("autoscaling/v1", "Scale"),
            ("discovery.k8s.io/v1", "EndpointSlice"),
            ("flowcontrol.apiserver.k8s.io/v1", "FlowSchema"),
            ("networking.k8s.io/v1", "NetworkPolicy"),
            (
                "admissionregistration.k8s.io/v1",
                "ValidatingWebhookConfiguration",
            ),
        ] {
            assert!(
                registry.handles(api_version, kind),
                "global OO registry must handle {api_version}/{kind}"
            );
        }
    }

    #[test]
    fn flowcontrol_resources_are_not_owned_by_builtin_bucket() {
        let registry = global_oo_registry();
        for kind in [
            "FlowSchema",
            "PriorityLevelConfiguration",
            "FlowSchemaList",
            "PriorityLevelConfigurationList",
        ] {
            assert!(
                registry.handles("flowcontrol.apiserver.k8s.io/v1", kind),
                "global OO registry must keep handling flowcontrol {kind}"
            );
            assert!(
                !super::super::BuiltinResourceCodec
                    .handles("flowcontrol.apiserver.k8s.io/v1", kind),
                "flowcontrol {kind} must be served by a dedicated ResourceProtoCodec"
            );
        }
    }
}
