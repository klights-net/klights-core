use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;

use crate::datastore::{DatastoreBackend, DatastoreHandle};
use crate::side_effects::SideEffect;
use crate::side_effects::SideEffectMetrics;

struct NamespaceTerminationEffect {
    db: DatastoreHandle,
    metrics: Arc<SideEffectMetrics>,
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
        reconcile(self.db.as_ref(), namespace, &self.metrics).await
    }
}

pub(crate) fn effect(db: DatastoreHandle, metrics: Arc<SideEffectMetrics>) -> Arc<dyn SideEffect> {
    Arc::new(NamespaceTerminationEffect { db, metrics })
}

pub(crate) async fn reconcile(
    db: &dyn DatastoreBackend,
    namespace: &str,
    metrics: &SideEffectMetrics,
) -> Result<()> {
    crate::api::reconcile_namespace_termination(db, namespace, metrics)
        .await
        .map_err(|error| anyhow::anyhow!("namespace termination failed: {error:?}"))
}
