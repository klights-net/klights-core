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
    /// Persisted assignment protocol. Missing from legacy envelopes means the
    /// historical leader-assigned RV behavior.
    #[serde(
        default,
        skip_serializing_if = "crate::datastore::resource_version_assignment::SnapshotAssignmentMode::is_absent_legacy"
    )]
    pub resource_version_assignment_mode:
        crate::datastore::resource_version_assignment::SnapshotAssignmentMode,
    /// Durable watch-log allocator boundary. `None` identifies snapshots
    /// written by peers predating apply-order event IDs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watch_event_high_water: Option<i64>,
    /// Authoritative watch-compaction boundaries. `None` is reserved for
    /// snapshots from peers predating replay-floor transfer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watch_replay_floors: Option<Vec<crate::datastore::WatchReplayFloor>>,
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
        Self::serialize_from_backend_to_cursor_inner(db, last_applied, membership).await
    }

    async fn serialize_from_backend_to_cursor_inner(
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
        // Freeze the apply-order boundary before the first streamed page.
        // Concurrent rows are replayed from the Raft log after this snapshot;
        // certifying them in the snapshot cursor would skip them on failover.
        let snapshot_position = db.current_watch_replay_position().await?;
        // The backend fence keeps resources, history, outbox state, allocator,
        // and replay floors mutation-stable across every awaited page. Emit
        // commits first and serialize the counters afterward; JSON key order
        // is irrelevant to serde deserialization.
        encoder.write_all(b",\"commits\":")?;
        crate::replication::snapshot::write_snapshot_commits_json_array_through_event_id(
            db,
            0,
            snapshot_position.event_id,
            &mut encoder,
        )
        .await?;
        let replay_position = db.current_watch_replay_position().await?;
        let meta_store = crate::datastore::DatastoreBackendMetaStore::new(db);
        let resource_version_assignment_mode =
            crate::datastore::resource_version_assignment::read_resource_version_assignment_mode(
                &meta_store,
            )
            .await?;
        let floors = normalize_snapshot_floors(
            db.list_watch_replay_floors().await?,
            snapshot_position.event_id,
            replay_position.resource_version,
        );
        encoder.write_all(b",\"current_rv\":")?;
        serde_json::to_writer(&mut encoder, &replay_position.resource_version)?;
        encoder.write_all(b",\"resource_version_assignment_mode\":")?;
        serde_json::to_writer(
            &mut encoder,
            &crate::datastore::resource_version_assignment::SnapshotAssignmentMode::explicit(
                resource_version_assignment_mode,
            ),
        )?;
        encoder.write_all(b",\"watch_event_high_water\":")?;
        serde_json::to_writer(&mut encoder, &snapshot_position.event_id)?;
        encoder.write_all(b",\"watch_replay_floors\":")?;
        serde_json::to_writer(&mut encoder, &floors)?;
        encoder.write_all(b"}")?;
        // Finish the zstd stream — flushes remaining compressed data to `framed`.
        encoder.finish()?;
        Ok(Cursor::new(framed))
    }
}

fn normalize_snapshot_floors(
    mut floors: Vec<crate::datastore::WatchReplayFloor>,
    high_water_event_id: i64,
    current_resource_version: i64,
) -> Vec<crate::datastore::WatchReplayFloor> {
    for floor in &mut floors {
        if floor.position_is_exact && floor.floor_event_id > high_water_event_id {
            // GC may advance after the boundary was captured. Relative to this
            // snapshot every older cursor is compacted while the boundary
            // itself remains a valid empty replay position.
            floor.floor_event_id = high_water_event_id;
            floor.floor_resource_version = current_resource_version;
        } else {
            floor.floor_resource_version =
                floor.floor_resource_version.min(current_resource_version);
        }
    }
    floors
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
    pub(crate) node_local: Arc<crate::datastore::node_local::SqliteNodeLocalDb>,
}

impl RaftSnapshotBuilder<TypeConfig> for SqliteRaftSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> std::result::Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let _snapshot_fence = self
            .backend
            .acquire_snapshot_exclusive_fence()
            .await
            .map_err(snapshot_write_err)?;
        let last_applied = self
            .node_local
            .raft_meta_get("last_applied")
            .await
            .map_err(snapshot_write_err)?
            .map(|bytes| serde_json::from_slice(&bytes))
            .transpose()
            .map_err(snapshot_write_err)?;
        let membership = self
            .node_local
            .raft_meta_get("last_membership")
            .await
            .map_err(snapshot_write_err)?
            .map(|bytes| serde_json::from_slice(&bytes))
            .transpose()
            .map_err(snapshot_write_err)?
            .unwrap_or_default();
        let snapshot = RaftSnapshotData::serialize_from_backend_to_cursor_inner(
            self.backend.as_ref(),
            last_applied,
            &membership,
        )
        .await
        .map_err(snapshot_write_err)?;
        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership: membership,
            snapshot_id: snapshot_id_for(last_applied),
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
        crate::controllers::namespace::init_default_namespaces(
            &crate::kubelet::file_blocking::test_file_process_executor(),
            &db,
        )
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
        assert_eq!(
            decoded.watch_event_high_water,
            Some(db.current_watch_replay_position().await.unwrap().event_id)
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

    #[tokio::test]
    async fn snapshot_serialization_preserves_v1_assignment_mode() {
        let db = test_support::in_memory().await;
        crate::datastore::resource_version_assignment::write_resource_version_assignment_mode(
            &db,
            crate::log_apply::ResourceVersionAssignment::CommittedApplyV1,
        )
        .await
        .unwrap();
        let membership = StoredMembership::<NodeId, openraft::BasicNode>::default();
        let snapshot = RaftSnapshotData::serialize_from_backend_to_cursor(&db, None, &membership)
            .await
            .unwrap();
        let decoded = RaftSnapshotData::deserialize_from_bytes(&snapshot.into_inner()).unwrap();
        assert_eq!(
            decoded.resource_version_assignment_mode,
            crate::datastore::resource_version_assignment::SnapshotAssignmentMode::Explicit(
                crate::log_apply::ResourceVersionAssignment::CommittedApplyV1
            )
        );

        let restored = test_support::in_memory().await;
        crate::datastore::resource_version_assignment::write_resource_version_assignment_mode(
            &restored,
            crate::log_apply::ResourceVersionAssignment::CommittedApplyV1,
        )
        .await
        .unwrap();
        assert_eq!(
            crate::datastore::resource_version_assignment::read_resource_version_assignment_mode(
                &restored
            )
            .await
            .unwrap(),
            crate::log_apply::ResourceVersionAssignment::CommittedApplyV1
        );
    }

    #[test]
    fn legacy_snapshot_without_watch_allocator_remains_decodable() {
        let legacy = serde_json::json!({
            "last_applied": null,
            "membership": StoredMembership::<NodeId, openraft::BasicNode>::default(),
            "current_rv": 7,
            "commits": []
        });
        let framed = crate::datastore::raft::compressed::encode(
            serde_json::to_vec(&legacy).unwrap().as_slice(),
        )
        .unwrap();

        let decoded = RaftSnapshotData::deserialize_from_bytes(&framed).unwrap();
        assert_eq!(decoded.current_rv, 7);
        assert_eq!(decoded.watch_event_high_water, None);
        assert_eq!(decoded.watch_replay_floors, None);
        assert_eq!(
            decoded.resource_version_assignment_mode,
            crate::datastore::resource_version_assignment::SnapshotAssignmentMode::AbsentLegacySnapshot
        );
    }
}
