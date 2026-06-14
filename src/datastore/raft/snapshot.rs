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
        Ok(serde_json::to_vec(self)?)
    }

    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }

    pub fn snapshot_id(&self) -> String {
        snapshot_id_for(self.last_applied)
    }

    pub async fn serialize_from_backend_to_cursor(
        db: &dyn DatastoreBackend,
        last_applied: Option<LogId<NodeId>>,
        membership: &StoredMembership<NodeId, openraft::BasicNode>,
        current_rv: i64,
    ) -> Result<Cursor<Vec<u8>>> {
        let mut cursor = Cursor::new(Vec::new());
        cursor.write_all(b"{\"last_applied\":")?;
        serde_json::to_writer(&mut cursor, &last_applied)?;
        cursor.write_all(b",\"membership\":")?;
        serde_json::to_writer(&mut cursor, membership)?;
        cursor.write_all(b",\"current_rv\":")?;
        serde_json::to_writer(&mut cursor, &current_rv)?;
        cursor.write_all(b",\"commits\":")?;
        crate::replication::snapshot::write_snapshot_commits_json_array(db, 0, &mut cursor).await?;
        cursor.write_all(b"}")?;
        cursor.set_position(0);
        Ok(cursor)
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

fn snapshot_read_err<E: std::fmt::Display>(e: E) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::read_snapshot(None, AnyError::error(e.to_string())),
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
        let current_rv = self
            .backend
            .get_current_resource_version()
            .await
            .map_err(snapshot_read_err)?;
        let snapshot = RaftSnapshotData::serialize_from_backend_to_cursor(
            self.backend.as_ref(),
            self.last_applied,
            &self.membership,
            current_rv,
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
