use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;

use crate::datastore::DatastoreHandle;
use klights_controllers::side_effects::SideEffect;
use klights_controllers::side_effects::SideEffectMetrics;

struct NamespaceTerminationEffect {
    store: Arc<dyn klights_reconcile_api::NamespaceLifecycleStore>,
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
        reconcile(self.store.as_ref(), namespace, &self.metrics).await
    }
}

pub(crate) fn effect(db: DatastoreHandle, metrics: Arc<SideEffectMetrics>) -> Arc<dyn SideEffect> {
    #[cfg(not(test))]
    let store = crate::api_state_adapter::RootNamespaceTerminationStore::new(db);
    #[cfg(test)]
    let store = crate::api_state_adapter::RootNamespaceTerminationStore::new(db);
    Arc::new(NamespaceTerminationEffect { store, metrics })
}

pub(crate) async fn reconcile(
    store: &dyn klights_reconcile_api::NamespaceLifecycleStore,
    namespace: &str,
    metrics: &SideEffectMetrics,
) -> Result<()> {
    k8s_native_service::reconcile_namespace_termination_at(
        store,
        namespace,
        metrics,
        klights_supervisor::SystemWallClock::now_utc(),
    )
    .await
    .map_err(|error| anyhow::anyhow!("namespace termination failed: {error:?}"))
}
