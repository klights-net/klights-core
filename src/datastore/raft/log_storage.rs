//! Phase 3 Raft log storage backed by the node-local SQLite database.
//!
//! Implements openraft 0.9 `RaftLogStorage` + `RaftLogReader` (storage-v2).
//! Each log entry is serialized (serde_json) into the `raft_log_entries`
//! table; vote and last-purged log id live as singleton rows in
//! `raft_meta`.
//!
//! Truncation is the critical primitive that resolves the Phase 2
//! log-divergence bug: when a follower receives an `AppendEntries` that
//! conflicts with its local tail, openraft calls `truncate(log_id)` and
//! this impl deletes the divergent rows so the new history can be
//! appended cleanly.

use std::fmt::Debug;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use klights_node_store::{
    EncodedRaftLogEntry, OpaqueRaftBytes, RaftLogBatch, RaftLogCoordinate, RaftLogDurability,
    RaftLogRange, RaftPurgeRequest,
};
use openraft::AnyError;
use openraft::storage::{LogFlushed, LogState, RaftLogStorage};
use openraft::{LogId, RaftLogReader, StorageError, StorageIOError, Vote};

use super::types::{NodeId, TypeConfig};

#[derive(Clone)]
pub struct SqliteRaftLogStorage {
    durability: Arc<dyn RaftLogDurability>,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
}

impl SqliteRaftLogStorage {
    pub fn new(
        durability: Arc<dyn RaftLogDurability>,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Self {
        Self {
            durability,
            supervisor,
        }
    }
}

fn ioerr_read(e: impl std::fmt::Display) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::read_logs(AnyError::error(e.to_string())),
    }
}

fn ioerr_write(e: impl std::fmt::Display) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::write_logs(AnyError::error(e.to_string())),
    }
}

fn ioerr_read_vote(e: impl std::fmt::Display) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::read_vote(AnyError::error(e.to_string())),
    }
}

fn ioerr_write_vote(e: impl std::fmt::Display) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::write_vote(AnyError::error(e.to_string())),
    }
}

fn range_bounds(
    range: impl RangeBounds<u64>,
) -> Result<RaftLogRange, klights_node_store::RaftDurabilityError> {
    let empty_after_max =
        matches!(range.start_bound(), Bound::Excluded(value) if *value == u64::MAX);
    let start = match range.start_bound() {
        Bound::Included(s) => *s,
        Bound::Excluded(s) => s.checked_add(1).unwrap_or(u64::MAX),
        Bound::Unbounded => 0,
    };
    let mut end = match range.end_bound() {
        Bound::Included(e) => e.checked_add(1),
        Bound::Excluded(e) => Some(*e),
        Bound::Unbounded => None,
    };
    if empty_after_max {
        end = Some(u64::MAX);
    }
    RaftLogRange::new(start, end)
}

impl RaftLogReader<TypeConfig> for SqliteRaftLogStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<openraft::Entry<TypeConfig>>, StorageError<NodeId>> {
        let entries = self
            .durability
            .read_log_range(range_bounds(range).map_err(ioerr_read)?)
            .await
            .map_err(ioerr_read)?;
        self.supervisor
            .run_blocking(
                klights_supervisor::TaskCategory::Others,
                "raft-log-entry-batch-decode",
                move || -> anyhow::Result<Vec<openraft::Entry<TypeConfig>>> {
                    let mut out = Vec::with_capacity(entries.len());
                    for encoded in entries {
                        let (coordinate, blob) = encoded.into_parts();
                        let entry: openraft::Entry<TypeConfig> =
                            serde_json::from_slice(blob.as_slice())?;
                        anyhow::ensure!(
                            entry.log_id.index == coordinate.index()
                                && entry.log_id.leader_id.term == coordinate.term()
                                && entry.log_id.leader_id.voted_for().unwrap_or_default()
                                    == coordinate.leader_node_id(),
                            "persisted Raft coordinate does not match entry bytes"
                        );
                        out.push(entry);
                    }
                    Ok(out)
                },
            )
            .await
            .map_err(ioerr_read)?
            .map_err(ioerr_read)
    }
}

impl RaftLogStorage<TypeConfig> for SqliteRaftLogStorage {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let (last_entry, encoded_last_purged) = self
            .durability
            .load_log_state()
            .await
            .map_err(ioerr_read)?
            .into_parts();
        let last_purged = match encoded_last_purged {
            Some(bytes) => serde_json::from_slice(bytes.as_slice()).map_err(ioerr_read)?,
            None => None,
        };
        let last_log_id = match last_entry {
            Some(coordinate) => {
                let leader_id =
                    openraft::LeaderId::new(coordinate.term(), coordinate.leader_node_id());
                Some(LogId::new(leader_id, coordinate.index()))
            }
            None => last_purged,
        };
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let bytes = serde_json::to_vec(vote).map_err(ioerr_write_vote)?;
        self.durability
            .store_vote(OpaqueRaftBytes::new(bytes))
            .await
            .map_err(ioerr_write_vote)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        match self.durability.load_vote().await.map_err(ioerr_read_vote)? {
            Some(bytes) => Ok(Some(
                serde_json::from_slice(bytes.as_slice()).map_err(ioerr_read_vote)?,
            )),
            None => Ok(None),
        }
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let bytes = serde_json::to_vec(&committed).map_err(ioerr_write)?;
        self.durability
            .store_committed(OpaqueRaftBytes::new(bytes))
            .await
            .map_err(ioerr_write)
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        match self.durability.load_committed().await.map_err(ioerr_read)? {
            Some(bytes) => Ok(serde_json::from_slice(bytes.as_slice()).map_err(ioerr_read)?),
            None => Ok(None),
        }
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let entries = entries.into_iter().collect::<Vec<_>>();
        let encoded_entries = self
            .supervisor
            .run_blocking(
                klights_supervisor::TaskCategory::Others,
                "raft-log-entry-batch-encode",
                move || -> anyhow::Result<Vec<EncodedRaftLogEntry>> {
                    entries
                        .into_iter()
                        .map(|entry| {
                            let coordinate = RaftLogCoordinate::new(
                                entry.log_id.index,
                                entry.log_id.leader_id.term,
                                entry.log_id.leader_id.voted_for().unwrap_or_default(),
                            );
                            Ok(EncodedRaftLogEntry::new(
                                coordinate,
                                OpaqueRaftBytes::new(serde_json::to_vec(&entry)?),
                            ))
                        })
                        .collect()
                },
            )
            .await
            .map_err(ioerr_write)?
            .map_err(ioerr_write)?;
        let batch = RaftLogBatch::new(encoded_entries).map_err(ioerr_write)?;
        self.durability
            .append_log_entries(batch)
            .await
            .map_err(ioerr_write)?;
        // SQLite db_call writes commit synchronously; on return the data
        // is durable so we can report flush completion immediately.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.durability
            .truncate_log_from(log_id.index)
            .await
            .map_err(ioerr_write)
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let bytes = serde_json::to_vec(&Some(log_id)).map_err(ioerr_write)?;
        self.durability
            .purge_log_through(RaftPurgeRequest::new(
                RaftLogCoordinate::new(
                    log_id.index,
                    log_id.leader_id.term,
                    log_id.leader_id.voted_for().unwrap_or_default(),
                ),
                OpaqueRaftBytes::new(bytes),
            ))
            .await
            .map_err(ioerr_write)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datastore::node_local::sqlite::SqliteNodeLocalDb;
    use crate::datastore::raft::types::{NodeId, StorageCommandPayload};
    use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};
    use openraft::{Entry, EntryPayload, LeaderId};

    fn entry_for(index: u64, term: u64, leader_node: NodeId, payload: &[u8]) -> Entry<TypeConfig> {
        Entry {
            log_id: LogId::new(LeaderId::new(term, leader_node), index),
            payload: EntryPayload::Normal(StorageCommandPayload::from_bytes(payload.to_vec())),
        }
    }

    async fn fresh_storage() -> SqliteRaftLogStorage {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let executor = klights_node_datastore::open::open_with_opts(
            klights_node_datastore::open::in_memory_opts(),
            supervisor.clone(),
            "sqlite:raft-log-test",
        )
        .await
        .expect("open node-local executor");
        let nl: Arc<dyn RaftLogDurability> =
            Arc::new(SqliteNodeLocalDb::from_executor(executor).expect("create node-local db"));
        SqliteRaftLogStorage::new(nl, supervisor)
    }

    async fn append_one(s: &SqliteRaftLogStorage, e: &Entry<TypeConfig>) {
        let encoded = EncodedRaftLogEntry::new(
            RaftLogCoordinate::new(
                e.log_id.index,
                e.log_id.leader_id.term,
                e.log_id.leader_id.voted_for().unwrap_or_default(),
            ),
            OpaqueRaftBytes::new(serde_json::to_vec(e).unwrap()),
        );
        s.durability
            .append_log_entries(RaftLogBatch::new(vec![encoded]).unwrap())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn append_then_read_back_roundtrip() {
        let mut s = fresh_storage().await;
        let entries = vec![
            entry_for(1, 1, 10, b"a"),
            entry_for(2, 1, 10, b"b"),
            entry_for(3, 1, 10, b"c"),
        ];
        for e in &entries {
            append_one(&s, e).await;
        }
        let state = s.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id.unwrap().index, 3);
        assert!(state.last_purged_log_id.is_none());
        let got = s.try_get_log_entries(1..4).await.unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].log_id.index, 1);
        assert_eq!(got[2].log_id.index, 3);
    }

    #[test]
    fn range_after_max_is_empty_instead_of_becoming_unbounded() {
        let range = range_bounds((Bound::Excluded(u64::MAX), Bound::Unbounded)).unwrap();
        assert_eq!(range.start_inclusive(), u64::MAX);
        assert_eq!(range.end_exclusive(), Some(u64::MAX));
    }

    #[tokio::test]
    async fn truncate_removes_divergent_tail() {
        let mut s = fresh_storage().await;
        for i in 1..=5 {
            append_one(&s, &entry_for(i, 1, 10, b"x")).await;
        }
        s.truncate(LogId::new(LeaderId::new(1, 10), 3))
            .await
            .unwrap();
        let got = s.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].log_id.index, 1);
        assert_eq!(got[1].log_id.index, 2);
    }

    #[tokio::test]
    async fn purge_removes_prefix_and_updates_last_purged() {
        let mut s = fresh_storage().await;
        for i in 1..=5 {
            append_one(&s, &entry_for(i, 1, 10, b"x")).await;
        }
        s.purge(LogId::new(LeaderId::new(1, 10), 3)).await.unwrap();
        let state = s.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.unwrap().index, 3);
        assert_eq!(state.last_log_id.unwrap().index, 5);
        let got = s.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].log_id.index, 4);
    }

    #[tokio::test]
    async fn vote_round_trips() {
        let mut s = fresh_storage().await;
        assert!(s.read_vote().await.unwrap().is_none());
        let v = Vote::new(7, 10);
        s.save_vote(&v).await.unwrap();
        assert_eq!(s.read_vote().await.unwrap().unwrap(), v);
    }

    #[tokio::test]
    async fn committed_round_trips() {
        let mut s = fresh_storage().await;
        assert!(s.read_committed().await.unwrap().is_none());
        let id = LogId::new(LeaderId::new(2, 10), 42);
        s.save_committed(Some(id)).await.unwrap();
        assert_eq!(s.read_committed().await.unwrap().unwrap(), id);
    }
}
