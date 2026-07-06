//! Phase 3 Raft snapshot envelope and builder.
//!
//! openraft drives `RaftSnapshotBuilder::build_snapshot` on the leader
//! (and on followers that fall too far behind log retention) to package
//! the current state-machine view into a single transferable blob. The
//! follower receives the bytes via `RaftStateMachine::install_snapshot`
//! and atomically replays them, then resumes the log from the snapshot's
//! `last_log_id`.
//!
//! The on-the-wire payload reuses the existing
//! `replication::snapshot::generate_snapshot` helper that already powers
//! the Phase 2 replica join path, so leader and follower share one
//! source of truth for "what makes up a cluster snapshot".

use std::io::Cursor;
use std::io::Write;
use std::sync::Arc;

use anyhow::Result;
use openraft::storage::RaftSnapshotBuilder;
use openraft::{
    AnyError, LogId, Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership,
};
use serde::{Deserialize, Serialize};

use crate::datastore::DatastoreBackend;
use crate::datastore::raft::types::{NodeId, TypeConfig};

/// Self-describing snapshot envelope. Carries the `last_applied`
/// log-id, the membership configuration, and an ordered list of
/// `LogApplyCommit` rows that, when replayed via
/// `DatastoreBackend::apply_log_apply_commit`, reconstruct the cluster
/// data state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RaftSnapshotData {
    pub last_applied: Option<LogId<NodeId>>,
    pub membership: StoredMembership<NodeId, openraft::BasicNode>,
    #[serde(default)]
    pub current_rv: i64,
    pub commits: Vec<crate::log_apply::LogApplyCommit>,
}

impl RaftSnapshotData {
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>> {
        crate::datastore::raft::compressed::encode(serde_json::to_vec(self)?.as_slice())
    }

    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(
            crate::datastore::raft::compressed::decode(bytes)?.as_slice(),
        )?)
    }

    pub fn snapshot_id(&self) -> String {
        snapshot_id_for(self.last_applied)
    }

    pub async fn serialize_from_backend_to_cursor(
        db: &dyn DatastoreBackend,
        last_applied: Option<LogId<NodeId>>,
        membership: &StoredMembership<NodeId, openraft::BasicNode>,
    ) -> Result<Cursor<Vec<u8>>> {
        // P2 (memory-improvement.md): stream the JSON through a zstd encoder
        // so the uncompressed JSON is never fully materialized in memory.
        // The old code wrote ALL commits to a raw Cursor<Vec<u8>> (hundreds
        // of MiB under e2e churn) then compressed the whole Vec in one shot.
        // This stream-compresses incrementally: each JSON write goes through
        // the zstd encoder, which flushes compressed blocks to `framed` as
        // they fill. Peak memory is O(zstd window size + page size), not
        // O(total snapshot size).
        let mut framed = Vec::new();
        // Always emit TAG_ZSTD — the streaming encoder can't know the final
        // size to decide RAW fallback, and JSON always compresses well.
        framed.push(crate::datastore::raft::compressed::TAG_ZSTD);
        let mut encoder = zstd::Encoder::new(&mut framed, 3)?;
        // Write the JSON envelope through the encoder.
        encoder.write_all(b"{\"last_applied\":")?;
        serde_json::to_writer(&mut encoder, &last_applied)?;
        encoder.write_all(b",\"membership\":")?;
        serde_json::to_writer(&mut encoder, membership)?;
        // Stream the snapshot commits BEFORE reading the leader current_rv.
        // Reading current_rv first opens a TOCTOU window: commits applied
        // during the many streaming awaits below would land in the snapshot
        // with a resourceVersion higher than the already-captured current_rv,
        // producing an internally inconsistent snapshot that a follower
        // rejects in `replace_resource_state_in_conn` ("snapshot entry
        // resourceVersion N is ahead of leader current_rv M"), permanently
        // breaking raft catch-up. Because every emitted commit was already
        // applied to the leader store (and its metadata.resource_version
        // advanced atomically in the same transaction) before we read its
        // rows, reading current_rv last guarantees it is >= the maximum
        // emitted resourceVersion. The JSON key order is irrelevant to serde
        // deserialization, so emitting `commits` before `current_rv` is safe.
        encoder.write_all(b",\"commits\":")?;
        crate::replication::snapshot::write_snapshot_commits_json_array(db, 0, &mut encoder)
            .await?;
        let current_rv = db.get_current_resource_version().await?;
        encoder.write_all(b",\"current_rv\":")?;
        serde_json::to_writer(&mut encoder, &current_rv)?;
        encoder.write_all(b"}")?;
        // Finish the zstd stream — flushes remaining compressed data to `framed`.
        encoder.finish()?;
        Ok(Cursor::new(framed))
    }
}

pub fn snapshot_id_for(last_applied: Option<LogId<NodeId>>) -> String {
    match last_applied {
        Some(id) => format!("raft-snapshot-t{}-i{}", id.leader_id.term, id.index),
        None => "raft-snapshot-empty".to_string(),
    }
}

fn snapshot_write_err<E: std::fmt::Display>(e: E) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::write_snapshot(None, AnyError::error(e.to_string())),
    }
}

/// Real snapshot builder used by `SqliteRaftStateMachine::get_snapshot_builder`.
/// Owns the cluster backend handle plus a snapshot of the engine's
/// `last_applied` / `membership` at build-request time so the produced
/// `SnapshotMeta` is consistent with the bytes it carries.
#[derive(Clone)]
pub struct SqliteRaftSnapshotBuilder {
    pub(crate) backend: Arc<dyn DatastoreBackend>,
    pub(crate) last_applied: Option<LogId<NodeId>>,
    pub(crate) membership: StoredMembership<NodeId, openraft::BasicNode>,
}

impl RaftSnapshotBuilder<TypeConfig> for SqliteRaftSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> std::result::Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let snapshot = RaftSnapshotData::serialize_from_backend_to_cursor(
            self.backend.as_ref(),
            self.last_applied,
            &self.membership,
        )
        .await
        .map_err(snapshot_write_err)?;
        let meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.membership.clone(),
            snapshot_id: snapshot_id_for(self.last_applied),
        };
        Ok(Snapshot {
            meta,
            snapshot: Box::new(snapshot),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datastore::test_support;

    /// P2 (memory): serialize_from_backend_to_cursor must produce a valid
    /// zstd-framed payload that round-trips through deserialize_from_bytes.
    /// The streaming path always emits TAG_ZSTD (no RAW fallback), because
    /// the streaming encoder can't know the final size to decide fallback.
    #[tokio::test]
    async fn streaming_snapshot_round_trips_and_is_zstd_framed() {
        let db = test_support::in_memory().await;
        crate::controllers::namespace::init_default_namespaces(&db)
            .await
            .unwrap();
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm-stream-test",
            serde_json::json!({"metadata": {"name": "cm-stream-test"}}),
        )
        .await
        .unwrap();

        let membership = StoredMembership::<NodeId, openraft::BasicNode>::default();
        let cursor = RaftSnapshotData::serialize_from_backend_to_cursor(&db, None, &membership)
            .await
            .unwrap();

        let framed = cursor.into_inner();
        assert_eq!(
            framed[0],
            crate::datastore::raft::compressed::TAG_ZSTD,
            "streaming snapshot must be zstd-framed (P2)"
        );

        let decoded = RaftSnapshotData::deserialize_from_bytes(&framed).unwrap();
        assert_eq!(
            decoded.current_rv,
            db.get_current_resource_version().await.unwrap()
        );
        assert!(!decoded.commits.is_empty(), "snapshot must contain commits");
        assert!(
            decoded.commits.iter().any(|c| c.mutations.iter().any(|m| {
                matches!(m, crate::log_apply::LogApplyMutation::PutResource(row)
                    if row.name == "cm-stream-test")
            })),
            "snapshot must contain the ConfigMap"
        );
    }
}
