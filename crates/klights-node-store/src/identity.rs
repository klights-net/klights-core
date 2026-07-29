//! Focused node-local identity and metadata persistence port.
//!
//! Implementations own only node.db identity values and small node-local
//! metadata. Cluster resources, watches, delivery queues, and runtime state are
//! deliberately outside this capability.

use std::fmt;
use std::future::Future;
use std::pin::Pin;

#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeIdentityError {
    PersistenceFailed {
        operation: &'static str,
        message: String,
    },
    Retryable {
        operation: &'static str,
        message: String,
    },
    Timeout,
    Cancelled,
}

impl NodeIdentityError {
    pub fn persistence_failed(operation: &'static str, message: impl Into<String>) -> Self {
        Self::PersistenceFailed {
            operation,
            message: message.into(),
        }
    }
}

impl fmt::Display for NodeIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PersistenceFailed { message, .. } => formatter.write_str(message),
            Self::Retryable { operation, message } => {
                write!(formatter, "{operation}: {message}")
            }
            Self::Timeout => formatter.write_str("node identity operation timed out"),
            Self::Cancelled => formatter.write_str("node identity operation was cancelled"),
        }
    }
}

impl std::error::Error for NodeIdentityError {}

pub type NodeIdentityFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, NodeIdentityError>> + Send + 'a>>;

pub trait NodeIdentity: Send + Sync {
    fn close(&self) {}

    fn backend_name(&self) -> &'static str;

    fn ensure_node_identity<'a>(
        &'a self,
        cluster_id: &'a str,
        node_uid: &'a str,
    ) -> NodeIdentityFuture<'a, ()>;

    fn get_node_meta<'a>(&'a self, key: &'a str) -> NodeIdentityFuture<'a, Option<String>>;

    fn set_node_meta<'a>(&'a self, key: &'a str, value: &'a str) -> NodeIdentityFuture<'a, ()>;
}
