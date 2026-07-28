use crate::kubelet::pod_runtime::service::PodRuntimeKey;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PodEventSinkError {
    #[error("Pod event delivery unavailable: {message}")]
    Unavailable { message: String },
}

impl PodEventSinkError {
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable {
            message: message.into(),
        }
    }
}

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
    ) -> Result<(), PodEventSinkError>;
}
