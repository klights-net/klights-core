//! Backend-neutral snapshot contracts and capture fencing.

use crate::ClusterStoreResult;
use async_trait::async_trait;
use klights_cluster_core::command::{COMMAND_CODEC_VERSION, supports_command_codec_version};
use serde::{Deserialize, Serialize};

/// Opaque exclusive guard acquired by root orchestration before passive
/// backend capture pins its consistent read view.
pub struct SnapshotExclusiveFence {
    _guard: Box<dyn Send + Sync>,
}

impl SnapshotExclusiveFence {
    pub fn new<T>(guard: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        Self {
            _guard: Box::new(guard),
        }
    }
}

/// Opaque shared guard held by state-machine mutations while capture or
/// authoritative install owns the exclusive side.
pub struct SnapshotMutationFence {
    _guard: Box<dyn Send + Sync>,
}

impl SnapshotMutationFence {
    pub fn new<T>(guard: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        Self {
            _guard: Box::new(guard),
        }
    }
}

/// Focused backend lifecycle and snapshot-fence port.
#[async_trait]
pub trait BackendLifecycleStore: Send + Sync {
    async fn acquire_snapshot_exclusive_fence(
        &self,
    ) -> ClusterStoreResult<Option<SnapshotExclusiveFence>>;
    async fn acquire_snapshot_mutation_fence(
        &self,
    ) -> ClusterStoreResult<Option<SnapshotMutationFence>>;
    fn close(&self);
}

/// Versioned snapshot of cluster-replicated backend state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotEnvelope {
    pub backend_kind: String,
    pub schema_fingerprint: String,
    pub codec_version: u32,
    pub last_applied_rv: i64,
    pub last_applied_command_id: Option<String>,
    pub tables: Vec<SnapshotTable>,
}

/// One table worth of snapshot data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotTable {
    pub name: String,
    pub entries: Vec<SnapshotEntry>,
}

/// One persisted snapshot row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// Typed backend-envelope validation errors.
#[derive(Debug, Clone, thiserror::Error)]
pub enum SnapshotRestoreError {
    #[error(
        "snapshot backend '{snapshot_backend}' does not match target backend '{target_backend}'"
    )]
    BackendMismatch {
        snapshot_backend: String,
        target_backend: String,
    },
    #[error(
        "snapshot schema fingerprint '{snapshot_fingerprint}' does not match target '{target_fingerprint}'"
    )]
    SchemaMismatch {
        snapshot_fingerprint: String,
        target_fingerprint: String,
    },
    #[error("snapshot codec version {snapshot_version} does not match binary {target_version}")]
    CodecVersionMismatch {
        snapshot_version: u32,
        target_version: u32,
    },
    #[error("snapshot data error: {0}")]
    Data(String),
}

/// Focused backend snapshot/restore port.
#[async_trait]
pub trait DatastoreSnapshotter: Send + Sync {
    fn backend_kind(&self) -> &'static str;
    fn schema_fingerprint(&self) -> String;
    async fn snapshot(&self, fence: SnapshotExclusiveFence)
    -> ClusterStoreResult<SnapshotEnvelope>;
    async fn restore(
        &self,
        envelope: &SnapshotEnvelope,
        fence: SnapshotExclusiveFence,
    ) -> ClusterStoreResult<()>;

    fn validate_envelope(
        &self,
        envelope: &SnapshotEnvelope,
    ) -> std::result::Result<(), SnapshotRestoreError> {
        if envelope.backend_kind != self.backend_kind() {
            return Err(SnapshotRestoreError::BackendMismatch {
                snapshot_backend: envelope.backend_kind.clone(),
                target_backend: self.backend_kind().to_string(),
            });
        }
        if envelope.schema_fingerprint != self.schema_fingerprint() {
            return Err(SnapshotRestoreError::SchemaMismatch {
                snapshot_fingerprint: envelope.schema_fingerprint.clone(),
                target_fingerprint: self.schema_fingerprint(),
            });
        }
        if !supports_command_codec_version(envelope.codec_version) {
            return Err(SnapshotRestoreError::CodecVersionMismatch {
                snapshot_version: envelope.codec_version,
                target_version: COMMAND_CODEC_VERSION,
            });
        }
        Ok(())
    }
}

/// Stable fingerprint of a backend's cluster-replicated table inventory.
pub fn compute_schema_fingerprint(table_names: &[&str]) -> String {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use sha2::{Digest, Sha256};

    let mut sorted = table_names.to_vec();
    sorted.sort();
    let hash = Sha256::digest(sorted.join("\n").as_bytes());
    URL_SAFE_NO_PAD.encode(&hash[..8])
}

#[cfg(test)]
mod tests {
    use super::compute_schema_fingerprint;
    use crate::{ClusterStoreError, ClusterStoreErrorKind, PersistenceBackend};

    #[test]
    fn adapter_failures_keep_owned_kind_backend_operation_and_source() {
        let error = ClusterStoreError::adapter_failure_boxed(
            ClusterStoreErrorKind::Conflict,
            PersistenceBackend::Sqlite,
            "focused persistence port",
            Box::new(std::io::Error::other("unique identity collision")),
        );

        assert_eq!(error.kind(), ClusterStoreErrorKind::Conflict);
        assert_eq!(error.backend(), Some(PersistenceBackend::Sqlite));
        assert_eq!(error.operation(), "focused persistence port");
        assert!(std::error::Error::source(&error).is_some());
        assert!(!error.is_retryable());
    }

    #[test]
    fn schema_fingerprint_is_order_independent_and_inventory_sensitive() {
        assert_eq!(
            compute_schema_fingerprint(&["a", "b", "c"]),
            compute_schema_fingerprint(&["c", "b", "a"])
        );
        assert_ne!(
            compute_schema_fingerprint(&["a", "b"]),
            compute_schema_fingerprint(&["a", "b", "c"])
        );
    }
}
