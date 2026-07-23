use klights_node_store::{
    EncodedRaftAppliedState, EncodedRaftLogEntry, EncodedRaftLogState, OpaqueRaftBytes,
    RaftAppliedStateDurability, RaftAppliedStateWrite, RaftDurabilityError, RaftDurabilityFuture,
    RaftLogBatch, RaftLogCoordinate, RaftLogDurability, RaftLogRange, RaftPurgeRequest,
};
use rusqlite::OptionalExtension;

use super::{SqliteNodeLocalDb, queries};

const VOTE: &str = "vote";
const COMMITTED: &str = "committed";
const LAST_PURGED: &str = "last_purged_log_id";
const LAST_APPLIED: &str = "last_applied";
const LAST_MEMBERSHIP: &str = "last_membership";

fn persistence(operation: &'static str, error: impl std::fmt::Display) -> RaftDurabilityError {
    RaftDurabilityError::persistence_failed(operation, error.to_string())
}

fn bit_pattern_i64(value: u64) -> i64 {
    // Preserve the historical SQLite bit representation. In particular,
    // hashed node IDs legitimately use the upper half of u64 and round-trip
    // through a signed INTEGER via the inverse `as u64` conversion.
    value as i64
}

fn index_i64(field: &'static str, value: u64) -> Result<i64, RaftDurabilityError> {
    i64::try_from(value).map_err(|_| RaftDurabilityError::InvalidInput {
        field,
        message: "Raft log index exceeds the SQLite INTEGER domain".to_string(),
    })
}

fn map_db_error(operation: &'static str, error: tokio_rusqlite::Error) -> RaftDurabilityError {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    while let Some(current) = source {
        if let Some(error) = current.downcast_ref::<RaftDurabilityError>() {
            return error.clone();
        }
        source = current.source();
    }
    persistence(operation, error)
}

fn decode_index(value: i64) -> rusqlite::Result<u64> {
    u64::try_from(value).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Integer,
            Box::new(RaftDurabilityError::corrupt_data(
                "raft_log.index",
                "stored index is negative",
            )),
        )
    })
}

impl RaftLogDurability for SqliteNodeLocalDb {
    fn read_log_range(
        &self,
        range: RaftLogRange,
    ) -> RaftDurabilityFuture<'_, Vec<EncodedRaftLogEntry>> {
        Box::pin(async move {
            let start = index_i64("start_inclusive", range.start_inclusive())?;
            let end = range
                .end_exclusive()
                .map(|value| index_i64("end_exclusive", value))
                .transpose()?;
            self.db_call("node_local:raft_log_read", move |conn| {
                let map_row = |row: &rusqlite::Row<'_>| {
                    Ok(EncodedRaftLogEntry::new(
                        RaftLogCoordinate::new(
                            decode_index(row.get::<_, i64>(0)?)?,
                            row.get::<_, i64>(1)? as u64,
                            row.get::<_, i64>(2)? as u64,
                        ),
                        OpaqueRaftBytes::new(row.get(3)?),
                    ))
                };
                let values = if let Some(end) = end {
                    conn.prepare(queries::RAFT_LOG_GET_RANGE)?
                        .query_map(rusqlite::params![start, end], map_row)?
                        .collect::<rusqlite::Result<Vec<_>>>()?
                } else {
                    conn.prepare(queries::RAFT_LOG_GET_RANGE_UNBOUNDED)?
                        .query_map(rusqlite::params![start], map_row)?
                        .collect::<rusqlite::Result<Vec<_>>>()?
                };
                Ok(values)
            })
            .await
            .map_err(|error| map_db_error("read_log_range", error))
        })
    }

    fn load_log_state(&self) -> RaftDurabilityFuture<'_, EncodedRaftLogState> {
        Box::pin(async move {
            self.db_call("node_local:raft_log_state", move |conn| {
                let last = conn
                    .query_row(queries::RAFT_LOG_LAST, [], |row| {
                        Ok(RaftLogCoordinate::new(
                            decode_index(row.get::<_, i64>(0)?)?,
                            row.get::<_, i64>(1)? as u64,
                            row.get::<_, i64>(2)? as u64,
                        ))
                    })
                    .optional()?;
                let purged = conn
                    .query_row(queries::RAFT_META_GET, [LAST_PURGED], |row| {
                        row.get::<_, Vec<u8>>(0)
                    })
                    .optional()?
                    .map(OpaqueRaftBytes::new);
                Ok(EncodedRaftLogState::new(last, purged))
            })
            .await
            .map_err(|error| map_db_error("load_log_state", error))
        })
    }

    fn append_log_entries(&self, entries: RaftLogBatch) -> RaftDurabilityFuture<'_, ()> {
        Box::pin(async move {
            let entries = entries
                .into_vec()
                .into_iter()
                .map(|entry| {
                    let (coordinate, payload) = entry.into_parts();
                    Ok::<_, RaftDurabilityError>((
                        index_i64("entries.index", coordinate.index())?,
                        bit_pattern_i64(coordinate.term()),
                        bit_pattern_i64(coordinate.leader_node_id()),
                        payload.into_vec(),
                    ))
                })
                .collect::<Result<Vec<_>, RaftDurabilityError>>()?;
            self.db_call("node_local:raft_log_append_batch", move |conn| {
                let tx = conn.transaction()?;
                for (index, term, leader, payload) in entries {
                    tx.execute(
                        queries::RAFT_LOG_INSERT,
                        rusqlite::params![index, term, leader, payload],
                    )?;
                }
                tx.commit()?;
                Ok(())
            })
            .await
            .map_err(|error| persistence("append_log_entries", error))
        })
    }

    fn truncate_log_from(&self, from_inclusive: u64) -> RaftDurabilityFuture<'_, ()> {
        Box::pin(async move {
            let from = index_i64("from_inclusive", from_inclusive)?;
            self.db_call("node_local:raft_log_truncate", move |conn| {
                conn.execute(queries::RAFT_LOG_TRUNCATE_FROM, [from])?;
                Ok(())
            })
            .await
            .map_err(|error| persistence("truncate_log_from", error))
        })
    }

    fn purge_log_through(&self, request: RaftPurgeRequest) -> RaftDurabilityFuture<'_, ()> {
        Box::pin(async move {
            let (through, encoded) = request.into_parts();
            let through = index_i64("through.index", through.index())?;
            let encoded = encoded.into_vec();
            self.db_call("node_local:raft_log_purge", move |conn| {
                let tx = conn.transaction()?;
                tx.execute(queries::RAFT_LOG_PURGE_UPTO, [through])?;
                tx.execute(
                    queries::RAFT_META_SET,
                    rusqlite::params![LAST_PURGED, encoded],
                )?;
                tx.commit()?;
                Ok(())
            })
            .await
            .map_err(|error| persistence("purge_log_through", error))
        })
    }

    fn load_vote(&self) -> RaftDurabilityFuture<'_, Option<OpaqueRaftBytes>> {
        read_meta(self, VOTE, "load_vote")
    }

    fn store_vote(&self, value: OpaqueRaftBytes) -> RaftDurabilityFuture<'_, ()> {
        write_meta(self, VOTE, value, "store_vote")
    }

    fn load_committed(&self) -> RaftDurabilityFuture<'_, Option<OpaqueRaftBytes>> {
        read_meta(self, COMMITTED, "load_committed")
    }

    fn store_committed(&self, value: OpaqueRaftBytes) -> RaftDurabilityFuture<'_, ()> {
        write_meta(self, COMMITTED, value, "store_committed")
    }
}

impl RaftAppliedStateDurability for SqliteNodeLocalDb {
    fn load_applied_state(&self) -> RaftDurabilityFuture<'_, EncodedRaftAppliedState> {
        Box::pin(async move {
            self.db_call("node_local:raft_applied_state", move |conn| {
                let read = |key| {
                    conn.query_row(queries::RAFT_META_GET, [key], |row| {
                        row.get::<_, Vec<u8>>(0)
                    })
                    .optional()
                };
                Ok(EncodedRaftAppliedState::new(
                    read(LAST_APPLIED)?.map(OpaqueRaftBytes::new),
                    read(LAST_MEMBERSHIP)?.map(OpaqueRaftBytes::new),
                ))
            })
            .await
            .map_err(|error| persistence("load_applied_state", error))
        })
    }

    fn store_applied_state(&self, state: RaftAppliedStateWrite) -> RaftDurabilityFuture<'_, ()> {
        Box::pin(async move {
            let (last, membership) = state.into_parts();
            self.db_call("node_local:raft_applied_state_store", move |conn| {
                let tx = conn.transaction()?;
                if let Some(value) = last {
                    tx.execute(
                        queries::RAFT_META_SET,
                        rusqlite::params![LAST_APPLIED, value.into_vec()],
                    )?;
                }
                if let Some(value) = membership {
                    tx.execute(
                        queries::RAFT_META_SET,
                        rusqlite::params![LAST_MEMBERSHIP, value.into_vec()],
                    )?;
                }
                tx.commit()?;
                Ok(())
            })
            .await
            .map_err(|error| persistence("store_applied_state", error))
        })
    }
}

fn read_meta<'a>(
    db: &'a SqliteNodeLocalDb,
    key: &'static str,
    operation: &'static str,
) -> RaftDurabilityFuture<'a, Option<OpaqueRaftBytes>> {
    Box::pin(async move {
        db.db_call("node_local:raft_meta_read", move |conn| {
            conn.query_row(queries::RAFT_META_GET, [key], |row| {
                row.get::<_, Vec<u8>>(0)
            })
            .optional()
            .map_err(Into::into)
        })
        .await
        .map(|value| value.map(OpaqueRaftBytes::new))
        .map_err(|error| persistence(operation, error))
    })
}

fn write_meta<'a>(
    db: &'a SqliteNodeLocalDb,
    key: &'static str,
    value: OpaqueRaftBytes,
    operation: &'static str,
) -> RaftDurabilityFuture<'a, ()> {
    Box::pin(async move {
        let value = value.into_vec();
        db.db_call("node_local:raft_meta_write", move |conn| {
            conn.execute(queries::RAFT_META_SET, rusqlite::params![key, value])?;
            Ok(())
        })
        .await
        .map_err(|error| persistence(operation, error))
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};

    use super::*;
    use crate::datastore::sqlite::{DbExecutor, opener};

    async fn fresh() -> SqliteNodeLocalDb {
        let executor = DbExecutor::open_with_opts(
            opener::OpenOpts::node_in_memory(),
            Arc::new(TaskSupervisor::new(TaskCategoryConfig::default())),
            "sqlite:raft-durability-test",
        )
        .await
        .unwrap();
        SqliteNodeLocalDb::from_executor(executor).unwrap()
    }

    #[tokio::test]
    async fn empty_snapshot_preserves_absent_last_applied_row() {
        let db = fresh().await;
        db.store_applied_state(RaftAppliedStateWrite::new(
            None,
            Some(OpaqueRaftBytes::new(b"membership".to_vec())),
        ))
        .await
        .unwrap();

        let state = db.load_applied_state().await.unwrap().into_parts();
        assert!(state.0.is_none(), "empty snapshot must not store JSON null");
        assert_eq!(state.1.unwrap().as_slice(), b"membership");
    }

    #[tokio::test]
    async fn log_batch_and_purge_are_byte_exact() {
        let db = fresh().await;
        let entries = vec![
            EncodedRaftLogEntry::new(
                RaftLogCoordinate::new(1, 2, 3),
                OpaqueRaftBytes::new(vec![0, 1, 0]),
            ),
            EncodedRaftLogEntry::new(
                RaftLogCoordinate::new(3, 4, u64::MAX),
                OpaqueRaftBytes::new(vec![9, 0, 8]),
            ),
        ];
        db.append_log_entries(RaftLogBatch::new(entries).unwrap())
            .await
            .unwrap();
        let read = db
            .read_log_range(RaftLogRange::new(1, None).unwrap())
            .await
            .unwrap();
        assert_eq!(read[1].coordinate(), RaftLogCoordinate::new(3, 4, u64::MAX));
        assert_eq!(read[0].payload().as_slice(), [0, 1, 0]);

        let purged = OpaqueRaftBytes::new(vec![7, 0, 7]);
        db.purge_log_through(RaftPurgeRequest::new(
            RaftLogCoordinate::new(1, 2, 3),
            purged.clone(),
        ))
        .await
        .unwrap();
        let (last, stored_purged) = db.load_log_state().await.unwrap().into_parts();
        assert_eq!(last.unwrap().index(), 3);
        assert_eq!(stored_purged.unwrap(), purged);
    }

    #[tokio::test]
    async fn log_mutations_reject_indices_above_sqlite_integer_domain() {
        let db = fresh().await;
        let too_large = i64::MAX as u64 + 1;
        assert!(matches!(
            db.read_log_range(RaftLogRange::new(too_large, None).unwrap())
                .await,
            Err(RaftDurabilityError::InvalidInput {
                field: "start_inclusive",
                ..
            })
        ));
        assert!(matches!(
            db.read_log_range(RaftLogRange::new(0, Some(too_large)).unwrap())
                .await,
            Err(RaftDurabilityError::InvalidInput {
                field: "end_exclusive",
                ..
            })
        ));
        let batch = RaftLogBatch::new(vec![EncodedRaftLogEntry::new(
            RaftLogCoordinate::new(too_large, u64::MAX, u64::MAX),
            OpaqueRaftBytes::new(vec![1]),
        )])
        .unwrap();
        assert!(matches!(
            db.append_log_entries(batch).await,
            Err(RaftDurabilityError::InvalidInput {
                field: "entries.index",
                ..
            })
        ));
        assert!(matches!(
            db.truncate_log_from(too_large).await,
            Err(RaftDurabilityError::InvalidInput {
                field: "from_inclusive",
                ..
            })
        ));
        assert!(matches!(
            db.purge_log_through(RaftPurgeRequest::new(
                RaftLogCoordinate::new(too_large, 1, 1),
                OpaqueRaftBytes::new(vec![1]),
            ))
            .await,
            Err(RaftDurabilityError::InvalidInput {
                field: "through.index",
                ..
            })
        ));
    }
}
