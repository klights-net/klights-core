//! Side effect to request namespace termination after Pod mutations.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use klights_reconcile_api::{NamespaceTerminationRequest, NamespaceTerminationSink};
use serde_json::Value;

use super::SideEffect;

struct NamespaceTerminationEffect {
    reconciliation: Arc<dyn NamespaceTerminationSink>,
}

#[async_trait]
impl SideEffect for NamespaceTerminationEffect {
    fn name(&self) -> &'static str {
        "namespace_termination"
    }

    async fn apply(&self, resource: &Value) -> Result<()> {
        let namespace = resource
            .pointer("/metadata/namespace")
            .and_then(Value::as_str)
            .unwrap_or("");
        if namespace.is_empty() {
            return Ok(());
        }
        self.reconciliation
            .reconcile_namespace_termination(NamespaceTerminationRequest {
                namespace: namespace.to_string(),
                expected_uid: None,
            })
            .await
            .map(|_| ())
            .map_err(|error| anyhow::anyhow!("namespace termination failed: {error:?}"))
    }
}

pub fn effect(reconciliation: Arc<dyn NamespaceTerminationSink>) -> Arc<dyn SideEffect> {
    Arc::new(NamespaceTerminationEffect { reconciliation })
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use klights_reconcile_api::{NamespaceTerminationFuture, NamespaceTerminationOutcome};

    use super::*;

    struct RecordingTermination {
        requests: Mutex<Vec<NamespaceTerminationRequest>>,
    }

    impl NamespaceTerminationSink for RecordingTermination {
        fn reconcile_namespace_termination(
            &self,
            request: NamespaceTerminationRequest,
        ) -> NamespaceTerminationFuture<'_> {
            self.requests.lock().unwrap().push(request);
            Box::pin(async { Ok(NamespaceTerminationOutcome::Finalized) })
        }
    }

    #[tokio::test]
    async fn namespaced_pod_requests_unqualified_namespace_reconcile() {
        let sink = Arc::new(RecordingTermination {
            requests: Mutex::new(Vec::new()),
        });
        let effect = effect(sink.clone());

        effect
            .apply(&serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"namespace": "work", "name": "pod"}
            }))
            .await
            .unwrap();

        assert_eq!(effect.name(), "namespace_termination");
        assert_eq!(
            *sink.requests.lock().unwrap(),
            vec![NamespaceTerminationRequest {
                namespace: "work".to_string(),
                expected_uid: None,
            }]
        );
    }
}
