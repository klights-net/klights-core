//! OpenRaft log storage backed by the focused node-local durability port.
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

use crate::types::{NodeId, TypeConfig};

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

    #[test]
    fn range_after_max_is_empty_instead_of_becoming_unbounded() {
        let range = range_bounds((Bound::Excluded(u64::MAX), Bound::Unbounded)).unwrap();
        assert_eq!(range.start_inclusive(), u64::MAX);
        assert_eq!(range.end_exclusive(), Some(u64::MAX));
    }
}
