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
