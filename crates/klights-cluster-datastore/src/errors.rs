//! Shared datastore error types at the trait boundary.
//!
//! `OpenError` is exposed by the opener layer (DSB-02) for failures that
//! prevent a datastore from being opened at all — schema mismatch,
//! corruption, or file-permission issues.
//!
//! `BackendError` lands when the first non-SQLite backend or replicated
//! command layer requires unified error reporting at the trait surface.

use std::path::PathBuf;

use klights_cluster_store::{ClusterStoreError, ClusterStoreErrorKind, PersistenceBackend};

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
}

impl OpenError {
    /// Return a path hint for error reporting.
    pub fn path_hint(&self) -> String {
        match self {
            OpenError::SchemaMismatch { path, .. } => path.clone(),
            OpenError::Corrupt { path, .. } => path.clone(),
            OpenError::Filesystem { path, .. } => path.display().to_string(),
            OpenError::ParentPermissionsTooWide(p) => p.display().to_string(),
        }
    }
}

impl From<klights_supervisor::SqliteOpenError> for OpenError {
    fn from(error: klights_supervisor::SqliteOpenError) -> Self {
        match error {
            klights_supervisor::SqliteOpenError::Corrupt { path, details } => {
                Self::Corrupt { path, details }
            }
        }
    }
}

/// Bridge private datastore adapter errors into the supervised DB call result.
impl From<OpenError> for tokio_rusqlite::Error {
    fn from(error: OpenError) -> Self {
        Self::Other(Box::new(error))
    }
}

/// Runtime datastore errors that higher layers need to handle consistently
/// across backends.
#[derive(Debug, thiserror::Error)]
pub enum DatastoreError {
    /// Create collided with an existing Kubernetes identity.
    #[error("{message} (409 Conflict)")]
    AlreadyExists { message: String },

    /// Optimistic-concurrency conflict. Maps to Kubernetes HTTP 409 Conflict.
    #[error("{message} (409 Conflict)")]
    Conflict { message: String },

    /// Requested object was not found. Maps to Kubernetes HTTP 404 NotFound.
    #[error("{message}")]
    NotFound { message: String },
}

impl DatastoreError {
    pub fn already_exists(message: impl Into<String>) -> Self {
        Self::AlreadyExists {
            message: message.into(),
        }
    }

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

/// Map a concrete datastore failure at a public persistence-port boundary.
///
/// This belongs beside [`DatastoreError`] so root composition can preserve
/// semantic classifications without importing concrete datastore error types.
pub fn cluster_store_adapter_error(
    error: anyhow::Error,
    backend: PersistenceBackend,
    operation: &'static str,
) -> ClusterStoreError {
    let error = match error.downcast::<ClusterStoreError>() {
        Ok(error) => {
            let kind = error.kind();
            return ClusterStoreError::adapter_failure_boxed(
                kind,
                backend,
                operation,
                Box::new(error),
            );
        }
        Err(error) => error,
    };
    let kind = match error.downcast_ref::<DatastoreError>() {
        Some(DatastoreError::AlreadyExists { .. } | DatastoreError::Conflict { .. }) => {
            ClusterStoreErrorKind::Conflict
        }
        Some(DatastoreError::NotFound { .. }) => ClusterStoreErrorKind::NotFound,
        None => ClusterStoreErrorKind::Persistence,
    };
    ClusterStoreError::adapter_failure_boxed(kind, backend, operation, error.into_boxed_dyn_error())
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

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::*;

    #[test]
    fn compatibility_adapter_preserves_an_inner_typed_cluster_store_error() {
        let inner = ClusterStoreError::adapter_failure_boxed(
            ClusterStoreErrorKind::Conflict,
            PersistenceBackend::Sqlite,
            "focused persistence port",
            Box::new(std::io::Error::other("unique identity collision")),
        );
        let error = cluster_store_adapter_error(
            anyhow::Error::new(inner),
            PersistenceBackend::Root,
            "root datastore compatibility adapter",
        );

        assert_eq!(error.kind(), ClusterStoreErrorKind::Conflict);
        assert_eq!(error.backend(), Some(PersistenceBackend::Root));
        assert_eq!(error.operation(), "root datastore compatibility adapter");
        let source = error.source().expect("typed error must remain the source");
        let inner = source
            .downcast_ref::<ClusterStoreError>()
            .expect("source must remain the typed cluster-store error");
        assert_eq!(inner.kind(), ClusterStoreErrorKind::Conflict);
        assert_eq!(inner.backend(), Some(PersistenceBackend::Sqlite));
    }
}
