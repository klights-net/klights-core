//! Transport-neutral resource fixtures for native-service consumer tests.
//!
//! Fixtures in this module may use only native-service ports and domain values.
//! Concrete datastore, watch-bus, bootstrap, and root composition types belong
//! to their canonical owners and are injected by the caller when required.

/// Canonical CRD registry setup for resource/discovery fixtures.
pub async fn register_crd(
    registry: &klights_leader_api::CrdRegistry,
    value: &serde_json::Value,
) -> Result<(), String> {
    crate::discovery::register_crd_from_value(registry, value).await
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn crd_fixture_uses_the_native_discovery_owner() {
        let registry = klights_leader_api::CrdRegistry::new();
        super::register_crd(
            &registry,
            &serde_json::json!({
                "apiVersion": "apiextensions.k8s.io/v1",
                "kind": "CustomResourceDefinition",
                "metadata": {"name": "widgets.example.test"},
                "spec": {
                    "group": "example.test",
                    "names": {"kind": "Widget", "plural": "widgets"},
                    "scope": "Namespaced",
                    "versions": [{"name": "v1", "served": true, "storage": true}]
                }
            }),
        )
        .await
        .expect("register CRD through native discovery support");
        assert!(
            registry
                .get("example.test", "v1", "widgets")
                .await
                .is_some()
        );
    }
}
