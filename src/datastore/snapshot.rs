//! Datastore snapshot / restore trait for Raft log compaction (DSB-R-09a).
//!
//! Every backend that can participate in Raft mode must implement
//! `DatastoreSnapshotter`.  Snapshots capture `ClusterReplicated` tables
//! plus metadata (resourceVersion, last applied command id); `NodeLocal`
//! tables are excluded from cluster snapshots.
//!
//! The envelope is self-describing (backend kind, schema fingerprint,
//! codec version) so restore can reject mismatches with typed errors.

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::datastore::command::COMMAND_CODEC_VERSION;

// ---------------------------------------------------------------------------
// Snapshot envelope types
// ---------------------------------------------------------------------------

/// Versioned snapshot of cluster-replicated state.
///
/// Self-describing: restore validates the envelope metadata before
/// touching the destination database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotEnvelope {
    /// Backend kind this snapshot was produced by (e.g. "redb", "sqlite").
    pub backend_kind: String,
    /// Schema fingerprint of the producing backend.  Must match the
    /// restore target's fingerprint.
    pub schema_fingerprint: String,
    /// `StorageCommand` codec version at snapshot time.
    pub codec_version: u32,
    /// Last applied resourceVersion.
    pub last_applied_rv: i64,
    /// Last applied command id, if any command has been applied.
    pub last_applied_command_id: Option<String>,
    /// Per-table payloads.
    pub tables: Vec<SnapshotTable>,
}

/// One table worth of snapshot data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotTable {
    pub name: String,
    pub entries: Vec<SnapshotEntry>,
}

/// One row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// Typed restore errors.
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

// ---------------------------------------------------------------------------
// DatastoreSnapshotter trait
// ---------------------------------------------------------------------------

/// Trait for backends that support snapshot/restore.
///
/// In Raft mode, the leader periodically snapshots cluster state and
/// streams the envelope to followers via InstallSnapshot RPC.  Followers
/// restore into an empty database and resume applying commands from the
/// snapshot's `last_applied_rv`.
///
/// Backends that do not implement this trait are rejected at startup
/// when `ReplicationMode::Raft` is requested.
#[async_trait]
pub trait DatastoreSnapshotter: Send + Sync {
    /// Backend kind identifier string (e.g. "redb", "sqlite").
    fn backend_kind(&self) -> &'static str;

    /// Schema fingerprint for mismatched-envelope detection.
    fn schema_fingerprint(&self) -> String;

    /// Produce a cluster-state snapshot from the current database state.
    async fn snapshot(&self) -> Result<SnapshotEnvelope>;

    /// Restore cluster state from a snapshot envelope into a **fresh,
    /// empty** database.  The target database must have no prior data.
    async fn restore(&self, envelope: &SnapshotEnvelope) -> Result<()>;

    /// Validate the envelope against this backend without performing
    /// a restore.  Returns an error on backend/schema/codec mismatch.
    fn validate_envelope(&self, envelope: &SnapshotEnvelope) -> Result<(), SnapshotRestoreError> {
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
        if envelope.codec_version != COMMAND_CODEC_VERSION {
            return Err(SnapshotRestoreError::CodecVersionMismatch {
                snapshot_version: envelope.codec_version,
                target_version: COMMAND_CODEC_VERSION,
            });
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Snapshot helpers
// ---------------------------------------------------------------------------

/// Compute a stable schema fingerprint from the list of table names.
///
/// The fingerprint is a hex-encoded SHA-256 of the sorted, newline-separated
/// table names.  This is simple but sufficient — if a table is added, removed,
/// or renamed, the fingerprint changes and restore rejects the mismatch.
pub fn compute_schema_fingerprint(table_names: &[&str]) -> String {
    use sha2::{Digest, Sha256};
    let mut sorted: Vec<&str> = table_names.to_vec();
    sorted.sort();
    let joined = sorted.join("\n");
    let hash = Sha256::digest(joined.as_bytes());
    // Use base64-url-safe encoding for compact, safe output.
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    URL_SAFE_NO_PAD.encode(&hash[..8])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_fingerprint_stable() {
        let f1 = compute_schema_fingerprint(&["a", "b", "c"]);
        let f2 = compute_schema_fingerprint(&["c", "b", "a"]);
        assert_eq!(f1, f2, "fingerprint must be order-independent");
    }

    #[test]
    fn schema_fingerprint_changes_on_different_tables() {
        let f1 = compute_schema_fingerprint(&["a", "b"]);
        let f2 = compute_schema_fingerprint(&["a", "b", "c"]);
        assert_ne!(f1, f2);
    }
}
