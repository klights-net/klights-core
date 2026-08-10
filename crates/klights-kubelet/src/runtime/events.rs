use crate::runtime_types::PodRuntimeKey;

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

#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::Mutex;

    // --- MockPodEventSink ---

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct MockPodEvent {
        pub(crate) namespace: String,
        pub(crate) name: String,
        pub(crate) uid: String,
        pub(crate) event_type: String,
        pub(crate) reason: String,
        pub(crate) message: String,
        pub(crate) reporting_component: String,
        pub(crate) node_name: String,
    }

    pub(crate) struct MockPodEventSink {
        events: Mutex<Vec<MockPodEvent>>,
    }

    impl Default for MockPodEventSink {
        fn default() -> Self {
            Self::new()
        }
    }

    impl MockPodEventSink {
        pub(crate) fn new() -> Self {
            Self {
                events: Mutex::new(Vec::new()),
            }
        }

        #[allow(dead_code)]
        pub(crate) fn clear_events(&self) {
            self.events.lock().unwrap().clear();
        }

        pub(crate) fn recorded_events(&self) -> Vec<MockPodEvent> {
            self.events.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl crate::runtime::events::PodEventSink for MockPodEventSink {
        async fn emit_pod_event(
            &self,
            key: &crate::runtime_types::PodRuntimeKey,
            event_type: &str,
            reason: &str,
            message: &str,
            reporting_component: &str,
            node_name: &str,
        ) -> Result<(), crate::runtime::events::PodEventSinkError> {
            self.events.lock().unwrap().push(MockPodEvent {
                namespace: key.namespace.clone(),
                name: key.name.clone(),
                uid: key.uid.clone(),
                event_type: event_type.to_string(),
                reason: reason.to_string(),
                message: message.to_string(),
                reporting_component: reporting_component.to_string(),
                node_name: node_name.to_string(),
            });
            Ok(())
        }
    }
}
