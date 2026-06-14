use std::sync::Arc;

use crate::datastore::DatastoreHandle;
use crate::kubelet::pod_runtime::service::PodRuntimeKey;

/// Kubelet Pod event emission port.
#[async_trait::async_trait]
pub trait PodEventSink: Send + Sync {
    /// Emit a pod event.
    async fn emit_pod_event(
        &self,
        key: &PodRuntimeKey,
        event_type: &str, // "Normal" or "Warning"
        reason: &str,     // "Scheduled" | "Pulling" | "Pulled" | "Failed" | ...
        message: &str,
        reporting_component: &str,
        node_name: &str,
    ) -> anyhow::Result<()>;
}

// --- Production adapter ---

/// Production event sink that routes through the outbox-aware event helper.
pub struct RealPodEventSink {
    outbox: Option<Arc<crate::kubelet::outbox::Outbox>>,
    datastore: DatastoreHandle,
}

impl RealPodEventSink {
    pub fn new(
        outbox: Option<Arc<crate::kubelet::outbox::Outbox>>,
        datastore: DatastoreHandle,
    ) -> Self {
        Self { outbox, datastore }
    }
}

#[async_trait::async_trait]
impl PodEventSink for RealPodEventSink {
    async fn emit_pod_event(
        &self,
        key: &PodRuntimeKey,
        event_type: &str,
        reason: &str,
        message: &str,
        reporting_component: &str,
        node_name: &str,
    ) -> anyhow::Result<()> {
        let pod = serde_json::json!({
            "metadata": {
                "namespace": key.namespace,
                "name": key.name,
                "uid": key.uid,
            },
        });
        crate::kubelet::events::emit_pod_event_with_outbox(
            self.datastore.as_ref(),
            self.outbox.as_deref(),
            crate::kubelet::events::PodEventRecord {
                pod: &pod,
                reason,
                message,
                event_type,
                reporting_component,
                reporting_instance: node_name,
            },
        )
        .await?;
        Ok(())
    }
}
