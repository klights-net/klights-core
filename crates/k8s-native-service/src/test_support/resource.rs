//! Transport-neutral resource fixtures for native-service consumer tests.
//!
//! Fixtures in this module may use only native-service ports and domain values.
//! Concrete datastore, watch-bus, bootstrap, and root composition types belong
//! to their canonical owners and are injected by the caller when required.

use std::sync::Arc;

/// Swappable API orchestration metrics port for native-service fixtures.
///
/// The fixture owns only an erased `NodeMetrics` port. It exposes neither a
/// node runtime, transport, router, nor inner trait object.
pub struct NodeMetricsFixture {
    inner: std::sync::RwLock<Arc<dyn klights_node_api::NodeMetrics>>,
}

struct UnavailableNodeMetrics;

impl NodeMetricsFixture {
    pub fn new() -> Self {
        Self {
            inner: std::sync::RwLock::new(Arc::new(UnavailableNodeMetrics)),
        }
    }

    pub fn replace(&self, metrics: Arc<dyn klights_node_api::NodeMetrics>) {
        *self.inner.write().expect("node metrics fixture lock") = metrics;
    }
}

impl Default for NodeMetricsFixture {
    fn default() -> Self {
        Self::new()
    }
}

impl klights_node_api::NodeMetrics for NodeMetricsFixture {
    fn collect_metrics(
        &self,
        request: klights_node_api::NodeMetricsRequest,
    ) -> klights_node_api::NodeMetricsFuture<'_, klights_node_api::NodeMetricsResult> {
        let metrics = self
            .inner
            .read()
            .expect("node metrics fixture lock")
            .clone();
        Box::pin(async move { metrics.collect_metrics(request).await })
    }
}

impl klights_node_api::NodeMetrics for UnavailableNodeMetrics {
    fn collect_metrics(
        &self,
        _request: klights_node_api::NodeMetricsRequest,
    ) -> klights_node_api::NodeMetricsFuture<'_, klights_node_api::NodeMetricsResult> {
        Box::pin(async {
            Err(klights_node_api::NodeMetricsError::unavailable(
                "node metrics are not configured for the native-service fixture",
            ))
        })
    }
}

/// Canonical CRD registry setup for resource/discovery fixtures.
pub async fn register_crd(
    registry: &klights_leader_api::CrdRegistry,
    value: &serde_json::Value,
) -> Result<(), String> {
    crate::discovery::register_crd_from_value(registry, value).await
}

#[cfg(test)]
mod tests {
    use super::NodeMetricsFixture;

    #[test]
    fn p12_2d_node_metrics_fixture_is_a_narrow_api_orchestration_owner() {
        fn accepts_fixture(_: Option<NodeMetricsFixture>) {}
        accepts_fixture(None);
    }

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
