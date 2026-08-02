//! Transport-neutral node outbox diagnostics and dead-letter administration.

use std::fmt;
use std::future::Future;
use std::pin::Pin;

#[derive(Clone, Debug, PartialEq)]
pub struct NodeOutboxStatus {
    pub pending: i64,
    pub oldest_age_seconds: f64,
    pub dispatch_total: u64,
    pub dispatch_errors_total: u64,
    pub dead_letter_total: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeDeadLetter {
    pub id: i64,
    pub original_id: i64,
    pub client_id: String,
    pub idempotency_key: String,
    pub enqueued_ms: i64,
    pub subject_key: String,
    pub subject_api_version: String,
    pub subject_kind: String,
    pub subject_namespace: Option<String>,
    pub subject_name: String,
    pub subject_uid: Option<String>,
    pub pod_uid: String,
    pub operation: String,
    pub stream_id: i64,
    pub stream_seq: i64,
    pub payload_proto: Vec<u8>,
    pub attempts: i64,
    pub last_error: String,
    pub moved_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeAdminError {
    message: String,
}

impl NodeAdminError {
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for NodeAdminError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for NodeAdminError {}

pub type NodeAdminFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, NodeAdminError>> + Send + 'a>>;

pub trait NodeOutboxDiagnostics: Send + Sync {
    fn outbox_status(&self) -> NodeAdminFuture<'_, NodeOutboxStatus>;
}

pub trait NodeDeadLetterAdmin: Send + Sync {
    fn list_dead_letters(&self) -> NodeAdminFuture<'_, Vec<NodeDeadLetter>>;
    fn replay_dead_letter(&self, id: i64) -> NodeAdminFuture<'_, bool>;
    fn delete_dead_letter(&self, id: i64) -> NodeAdminFuture<'_, bool>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_object_safe(
        _diagnostics: &dyn NodeOutboxDiagnostics,
        _admin: &dyn NodeDeadLetterAdmin,
    ) {
    }

    #[test]
    fn node_admin_capabilities_are_object_safe() {
        let _ = assert_object_safe;
    }
}
