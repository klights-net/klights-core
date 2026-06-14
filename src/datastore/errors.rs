//! Shared datastore error types at the trait boundary.
//!
//! `OpenError` is exposed by the opener layer (DSB-02) for failures that
//! prevent a datastore from being opened at all — schema mismatch,
//! corruption, or file-permission issues.
//!
//! `BackendError` lands when the first non-SQLite backend or replicated
//! command layer requires unified error reporting at the trait surface.

use std::path::PathBuf;

/// Errors that can occur when opening a datastore connection.
///
/// These are fatal startup errors — the operator must take explicit action
/// (delete the DB, fix permissions, etc.) before the process can run.
#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    /// The schema fingerprint in the DB doesn't match the binary's schema.
    ///
    /// This means the schema has changed since the DB was created. Until
    /// development, the operator action is "delete the DB and restart".
    #[error("schema fingerprint mismatch at {path}: expected {expected}, got {actual}\n{hint}")]
    SchemaMismatch {
        /// Path to the database file.
        path: String,
        /// The fingerprint this binary expects.
        expected: String,
        /// The fingerprint stored in the DB.
        actual: String,
        /// Human-readable hint for the operator.
        hint: String,
    },

    /// The database file is corrupted and `PRAGMA integrity_check` failed.
    ///
    /// SQLite cannot recover from corruption automatically. The operator
    /// must restore from backup or start fresh.
    #[error("database corruption detected at {path}: {details}")]
    Corrupt { path: String, details: String },

    /// Filesystem or permission error when accessing the database file.
    #[error("filesystem error accessing {path}: {source}")]
    Filesystem {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// The parent directory has permissions wider than 0700.
    ///
    /// This is a security check — the opener refuses to use a DB whose parent
    /// is readable/writable by non-root users.
    #[error("parent directory {0} has permissions wider than 0700")]
    ParentPermissionsTooWide(PathBuf),

    /// Raft mode was requested but the selected backend does not implement
    /// DatastoreSnapshotter.
    #[error(
        "ReplicationMode::Raft requires a snapshot-capable backend. \
         Backend '{backend}' does not support snapshot/restore. \
         See DatastoreSnapshotter (DSB-R-09a)."
    )]
    RaftRequiresSnapshotter { backend: String },
}

impl OpenError {
    /// Return a path hint for error reporting.
    pub fn path_hint(&self) -> String {
        match self {
            OpenError::SchemaMismatch { path, .. } => path.clone(),
            OpenError::Corrupt { path, .. } => path.clone(),
            OpenError::Filesystem { path, .. } => path.display().to_string(),
            OpenError::ParentPermissionsTooWide(p) => p.display().to_string(),
            OpenError::RaftRequiresSnapshotter { .. } => String::new(),
        }
    }
}

/// Runtime datastore errors that higher layers need to handle consistently
/// across backends.
#[derive(Debug, thiserror::Error)]
pub enum DatastoreError {
    /// Optimistic-concurrency conflict. Maps to Kubernetes HTTP 409 Conflict.
    #[error("{message} (409 Conflict)")]
    Conflict { message: String },

    /// Requested object was not found. Maps to Kubernetes HTTP 404 NotFound.
    #[error("{message}")]
    NotFound { message: String },
}

impl DatastoreError {
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::Conflict {
            message: message.into(),
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound {
            message: message.into(),
        }
    }

    pub fn is_conflict(&self) -> bool {
        matches!(self, Self::Conflict { .. })
    }
}

/// Return true when an anyhow error represents a datastore conflict.
pub fn is_conflict_error(err: &anyhow::Error) -> bool {
    if err
        .downcast_ref::<DatastoreError>()
        .is_some_and(DatastoreError::is_conflict)
    {
        return true;
    }

    let lower = format!("{err:#}").to_ascii_lowercase();
    lower.contains("409 conflict")
        || lower.contains("version conflict")
        || lower.contains("rv conflict")
}
