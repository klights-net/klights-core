//! Typed failures for public cluster-persistence ports.
//!
//! The ports in this crate are implemented by backend adapters.  Their errors
//! must retain a stable semantic category while preserving the backend,
//! operation, and original failure for callers that need diagnostics.

use std::{error::Error, fmt};

/// Stable semantic category for a cluster-persistence port failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterStoreErrorKind {
    InvalidRequest,
    NotFound,
    Conflict,
    Unsupported,
    CorruptData,
    Persistence,
    Retryable,
}

impl fmt::Display for ClusterStoreErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::InvalidRequest => "invalid request",
            Self::NotFound => "not found",
            Self::Conflict => "conflict",
            Self::Unsupported => "unsupported",
            Self::CorruptData => "corrupt data",
            Self::Persistence => "persistence",
            Self::Retryable => "retryable",
        };
        formatter.write_str(label)
    }
}

/// Backend boundary that reported a cluster-persistence port failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistenceBackend {
    Sqlite,
    Redb,
    Root,
    Replication,
}

impl fmt::Display for PersistenceBackend {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::Sqlite => "sqlite",
            Self::Redb => "redb",
            Self::Root => "root",
            Self::Replication => "replication",
        };
        formatter.write_str(label)
    }
}

/// Failure returned by a public cluster-persistence port.
///
/// `operation` is intentionally a static adapter-owned label.  It identifies
/// the boundary that failed without accepting arbitrary caller-controlled
/// strings into the persistence contract.
#[derive(Debug)]
pub struct ClusterStoreError {
    kind: ClusterStoreErrorKind,
    backend: Option<PersistenceBackend>,
    operation: &'static str,
    message: String,
    source: Option<Box<dyn Error + Send + Sync + 'static>>,
}

impl ClusterStoreError {
    /// Wrap an adapter failure without losing its original error chain.
    pub fn adapter_failure_boxed(
        kind: ClusterStoreErrorKind,
        backend: PersistenceBackend,
        operation: &'static str,
        source: Box<dyn Error + Send + Sync + 'static>,
    ) -> Self {
        Self {
            kind,
            backend: Some(backend),
            operation,
            message: source.to_string(),
            source: Some(source),
        }
    }

    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self::message(
            ClusterStoreErrorKind::InvalidRequest,
            "request validation",
            message,
        )
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::message(
            ClusterStoreErrorKind::Unsupported,
            "capability check",
            message,
        )
    }

    pub fn corrupt_data(message: impl Into<String>) -> Self {
        Self::message(
            ClusterStoreErrorKind::CorruptData,
            "data validation",
            message,
        )
    }

    pub fn persistence(message: impl Into<String>) -> Self {
        Self::message(ClusterStoreErrorKind::Persistence, "persistence", message)
    }

    pub fn kind(&self) -> ClusterStoreErrorKind {
        self.kind
    }

    pub fn backend(&self) -> Option<PersistenceBackend> {
        self.backend
    }

    pub fn operation(&self) -> &'static str {
        self.operation
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self.kind, ClusterStoreErrorKind::Retryable)
    }

    fn message(
        kind: ClusterStoreErrorKind,
        operation: &'static str,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            backend: None,
            operation,
            message: message.into(),
            source: None,
        }
    }
}

impl fmt::Display for ClusterStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.backend {
            Some(backend) => write!(
                formatter,
                "cluster store {} error in {} {}: {}",
                backend, self.operation, self.kind, self.message
            ),
            None => write!(
                formatter,
                "cluster store {} error in {}: {}",
                self.kind, self.operation, self.message
            ),
        }
    }
}

impl Error for ClusterStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref().map(|source| source as _)
    }
}

/// Standard result returned by public cluster-persistence ports.
pub type ClusterStoreResult<T> = std::result::Result<T, ClusterStoreError>;
