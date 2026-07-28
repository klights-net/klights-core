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
const STORAGE_INCARNATION: &str = "storage_incarnation";
const STORAGE_LOG_HIGH_WATERMARK: &str = "storage_log_high_watermark";
const STORAGE_LOG_HIGH_TERM: &str = "storage_log_high_term";
const STORAGE_LOG_HIGH_LEADER: &str = "storage_log_high_leader";

fn advance_storage_log_attestation(
    tx: &rusqlite::Transaction<'_>,
    coordinate: RaftLogCoordinate,
) -> rusqlite::Result<()> {
    let index = index_i64("storage_attestation.index", coordinate.index())
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let current = tx
        .query_row(
            queries::RAFT_META_GET,
            [STORAGE_LOG_HIGH_WATERMARK],
            |row| row.get::<_, i64>(0),
        )
        .optional()?;
    if current.is_none_or(|current| index > current) {
        for (key, value) in [
            (STORAGE_LOG_HIGH_WATERMARK, index),
            (STORAGE_LOG_HIGH_TERM, bit_pattern_i64(coordinate.term())),
            (
                STORAGE_LOG_HIGH_LEADER,
                bit_pattern_i64(coordinate.leader_node_id()),
            ),
        ] {
            tx.execute(queries::RAFT_META_SET, rusqlite::params![key, value])?;
        }
    }
    Ok(())
}

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
            let high_watermark = entries.iter().max_by_key(|entry| entry.0).map(|entry| {
                RaftLogCoordinate::new(entry.0 as u64, entry.1 as u64, entry.2 as u64)
            });
            self.db_call("node_local:raft_log_append_batch", move |conn| {
                let tx = conn.transaction()?;
                for (index, term, leader, payload) in entries {
                    tx.execute(
                        queries::RAFT_LOG_INSERT,
                        rusqlite::params![index, term, leader, payload],
                    )?;
                }
                if let Some(high_watermark) = high_watermark {
                    advance_storage_log_attestation(&tx, high_watermark)?;
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
            let through_coordinate = through;
            let through = index_i64("through.index", through_coordinate.index())?;
            let encoded = encoded.into_vec();
            self.db_call("node_local:raft_log_purge", move |conn| {
                let tx = conn.transaction()?;
                tx.execute(queries::RAFT_LOG_PURGE_UPTO, [through])?;
                tx.execute(
                    queries::RAFT_META_SET,
                    rusqlite::params![LAST_PURGED, encoded],
                )?;
                advance_storage_log_attestation(&tx, through_coordinate)?;
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

    fn load_or_create_storage_incarnation(&self) -> RaftDurabilityFuture<'_, String> {
        Box::pin(async move {
            let generated = uuid::Uuid::new_v4().to_string().into_bytes();
            self.db_call("node_local:raft_storage_incarnation", move |conn| {
                let tx = conn.transaction()?;
                tx.execute(
                    "INSERT OR IGNORE INTO raft_meta (key, value) VALUES (?1, ?2)",
                    rusqlite::params![STORAGE_INCARNATION, generated],
                )?;
                let value = tx.query_row(queries::RAFT_META_GET, [STORAGE_INCARNATION], |row| {
                    row.get::<_, Vec<u8>>(0)
                })?;
                tx.commit()?;
                Ok(String::from_utf8(value).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Blob,
                        Box::new(error),
                    )
                })?)
            })
            .await
            .map_err(|error| map_db_error("load_or_create_storage_incarnation", error))
        })
    }

    fn load_storage_log_attestation(&self) -> RaftDurabilityFuture<'_, Option<RaftLogCoordinate>> {
        Box::pin(async move {
            self.db_call("node_local:raft_storage_log_high_watermark", move |conn| {
                let tx = conn.transaction()?;
                let current_index = tx
                    .query_row(
                        queries::RAFT_META_GET,
                        [STORAGE_LOG_HIGH_WATERMARK],
                        |row| row.get::<_, i64>(0),
                    )
                    .optional()?;
                let coordinate = match current_index {
                    Some(index) => {
                        let term =
                            tx.query_row(queries::RAFT_META_GET, [STORAGE_LOG_HIGH_TERM], |row| {
                                row.get::<_, i64>(0)
                            })?;
                        let leader = tx.query_row(
                            queries::RAFT_META_GET,
                            [STORAGE_LOG_HIGH_LEADER],
                            |row| row.get::<_, i64>(0),
                        )?;
                        Some(RaftLogCoordinate::new(
                            decode_index(index)?,
                            term as u64,
                            leader as u64,
                        ))
                    }
                    None => {
                        let coordinate = tx
                            .query_row(queries::RAFT_LOG_LAST, [], |row| {
                                Ok(RaftLogCoordinate::new(
                                    decode_index(row.get::<_, i64>(0)?)?,
                                    row.get::<_, i64>(1)? as u64,
                                    row.get::<_, i64>(2)? as u64,
                                ))
                            })
                            .optional()?;
                        if let Some(coordinate) = coordinate {
                            advance_storage_log_attestation(&tx, coordinate)?;
                        }
                        coordinate
                    }
                };
                tx.commit()?;
                Ok(coordinate)
            })
            .await
            .map_err(|error| map_db_error("load_storage_log_high_watermark", error))
        })
    }

    fn load_storage_current_boundary(&self) -> RaftDurabilityFuture<'_, Option<RaftLogCoordinate>> {
        Box::pin(async move {
            self.db_call("node_local:raft_storage_current_boundary", move |conn| {
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
                    .map(|encoded| {
                        serde_json::from_slice::<openraft::LogId<u64>>(&encoded)
                            .map(|log_id| {
                                RaftLogCoordinate::new(
                                    log_id.index,
                                    log_id.leader_id.term,
                                    log_id.leader_id.node_id,
                                )
                            })
                            .map_err(|error| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    0,
                                    rusqlite::types::Type::Blob,
                                    Box::new(error),
                                )
                            })
                    })
                    .transpose()?;
                let snapshot_anchor = conn
                    .query_row(queries::RAFT_META_GET, [LAST_APPLIED], |row| {
                        row.get::<_, Vec<u8>>(0)
                    })
                    .optional()?
                    .map(|encoded| {
                        serde_json::from_slice::<Option<openraft::LogId<u64>>>(&encoded)
                            .map(|log_id| {
                                log_id.map(|log_id| {
                                    RaftLogCoordinate::new(
                                        log_id.index,
                                        log_id.leader_id.term,
                                        log_id.leader_id.node_id,
                                    )
                                })
                            })
                            .map_err(|error| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    0,
                                    rusqlite::types::Type::Blob,
                                    Box::new(error),
                                )
                            })
                    })
                    .transpose()?
                    .flatten();
                Ok([last, purged, snapshot_anchor]
                    .into_iter()
                    .flatten()
                    .max_by_key(|coordinate| coordinate.index()))
            })
            .await
            .map_err(|error| map_db_error("load_storage_current_boundary", error))
        })
    }

    fn reset_orphaned_learner_log(&self) -> RaftDurabilityFuture<'_, bool> {
        Box::pin(async move {
            let replacement_incarnation = uuid::Uuid::new_v4().to_string().into_bytes();
            self.db_call("node_local:raft_learner_orphan_reset", move |conn| {
                let tx = conn.transaction()?;
                let first_index = tx
                    .query_row(queries::RAFT_LOG_FIRST_INDEX, [], |row| {
                        decode_index(row.get::<_, i64>(0)?)
                    })
                    .optional()?;
                let has_last_purged = tx
                    .query_row(queries::RAFT_META_GET, [LAST_PURGED], |_| Ok(()))
                    .optional()?
                    .is_some();
                let has_last_applied = tx
                    .query_row(queries::RAFT_META_GET, [LAST_APPLIED], |_| Ok(()))
                    .optional()?
                    .is_some();
                let orphaned = first_index.is_some_and(|index| index > 0)
                    && !has_last_purged
                    && !has_last_applied;
                if orphaned {
                    tx.execute("DELETE FROM raft_log_entries", [])?;
                    tx.execute(queries::RAFT_META_DELETE_RECOVERABLE_STATE, [])?;
                    tx.execute(
                        queries::RAFT_META_SET,
                        rusqlite::params![STORAGE_INCARNATION, replacement_incarnation],
                    )?;
                }
                tx.commit()?;
                Ok(orphaned)
            })
            .await
            .map_err(|error| map_db_error("reset_orphaned_learner_log", error))
        })
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
            let last = last.map(OpaqueRaftBytes::into_vec);
            let last_coordinate = last
                .as_deref()
                .map(serde_json::from_slice::<Option<openraft::LogId<u64>>>)
                .transpose()
                .map_err(|error| persistence("decode applied LogId", error))?
                .flatten()
                .map(|log_id| {
                    RaftLogCoordinate::new(
                        log_id.index,
                        log_id.leader_id.term,
                        log_id.leader_id.node_id,
                    )
                });
            self.db_call("node_local:raft_applied_state_store", move |conn| {
                let tx = conn.transaction()?;
                if let Some(value) = last {
                    tx.execute(
                        queries::RAFT_META_SET,
                        rusqlite::params![LAST_APPLIED, value],
                    )?;
                }
                if let Some(coordinate) = last_coordinate {
                    advance_storage_log_attestation(&tx, coordinate)?;
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

    async fn fresh() -> SqliteNodeLocalDb {
        let executor = crate::datastore::node_local::sqlite::open::open_with_opts(
            crate::datastore::node_local::sqlite::open::in_memory_opts(),
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
    async fn learner_recovery_resets_orphaned_suffix_and_committed_but_preserves_vote() {
        let db = fresh().await;
        let old_incarnation = db.load_or_create_storage_incarnation().await.unwrap();
        db.append_log_entries(
            RaftLogBatch::new(vec![
                EncodedRaftLogEntry::new(
                    RaftLogCoordinate::new(124_022, 1, 7),
                    OpaqueRaftBytes::new(b"first-orphan".to_vec()),
                ),
                EncodedRaftLogEntry::new(
                    RaftLogCoordinate::new(128_367, 1, 7),
                    OpaqueRaftBytes::new(b"last-orphan".to_vec()),
                ),
            ])
            .unwrap(),
        )
        .await
        .unwrap();
        let vote = OpaqueRaftBytes::new(b"durable-vote".to_vec());
        db.store_vote(vote.clone()).await.unwrap();
        db.store_committed(OpaqueRaftBytes::new(b"orphan-commit".to_vec()))
            .await
            .unwrap();

        assert!(
            db.reset_orphaned_learner_log().await.unwrap(),
            "an unanchored non-zero suffix cannot be replayed and must be reacquired"
        );

        let (last, purged) = db.load_log_state().await.unwrap().into_parts();
        assert_eq!(last, None);
        assert_eq!(purged, None);
        assert_eq!(db.load_committed().await.unwrap(), None);
        assert_eq!(db.load_vote().await.unwrap(), Some(vote));
        assert_ne!(
            db.load_or_create_storage_incarnation().await.unwrap(),
            old_incarnation,
            "discarding an orphaned suffix must create a new admission identity"
        );
        assert_eq!(db.load_storage_log_attestation().await.unwrap(), None);
    }

    #[tokio::test]
    async fn raft_storage_incarnation_is_stable_for_the_lifetime_of_node_db() {
        let db = fresh().await;

        let first = db.load_or_create_storage_incarnation().await.unwrap();
        let reopened = db.load_or_create_storage_incarnation().await.unwrap();

        assert_eq!(reopened, first);
        assert_eq!(
            uuid::Uuid::parse_str(&first).unwrap().get_version(),
            Some(uuid::Version::Random)
        );
    }

    #[tokio::test]
    async fn raft_storage_log_high_watermark_never_moves_back_on_truncate() {
        let db = fresh().await;
        db.append_log_entries(
            RaftLogBatch::new(vec![EncodedRaftLogEntry::new(
                RaftLogCoordinate::new(41, 1, 7),
                OpaqueRaftBytes::new(b"entry".to_vec()),
            )])
            .unwrap(),
        )
        .await
        .unwrap();
        db.truncate_log_from(41).await.unwrap();

        assert_eq!(
            db.load_storage_log_attestation()
                .await
                .unwrap()
                .unwrap()
                .index(),
            41
        );
    }

    #[tokio::test]
    async fn snapshot_applied_log_id_advances_highwater_and_current_boundary() {
        let db = fresh().await;
        let snapshot_log = openraft::LogId::new(openraft::LeaderId::new(7, 11), 93);
        db.store_applied_state(RaftAppliedStateWrite::new(
            Some(OpaqueRaftBytes::new(
                serde_json::to_vec(&Some(snapshot_log)).unwrap(),
            )),
            None,
        ))
        .await
        .unwrap();

        let high = db.load_storage_log_attestation().await.unwrap().unwrap();
        let boundary = db.load_storage_current_boundary().await.unwrap().unwrap();
        assert_eq!(
            (high.index(), high.term(), high.leader_node_id()),
            (93, 7, 11)
        );
        assert_eq!(boundary, high);
    }

    #[tokio::test]
    async fn learner_recovery_never_discards_snapshot_anchored_state() {
        let db = fresh().await;
        db.append_log_entries(
            RaftLogBatch::new(vec![EncodedRaftLogEntry::new(
                RaftLogCoordinate::new(124_022, 1, 7),
                OpaqueRaftBytes::new(b"anchored".to_vec()),
            )])
            .unwrap(),
        )
        .await
        .unwrap();
        db.store_applied_state(RaftAppliedStateWrite::new(
            Some(OpaqueRaftBytes::new(
                serde_json::to_vec(&Some(openraft::LogId::new(
                    openraft::LeaderId::new(1, 7),
                    124_022,
                )))
                .unwrap(),
            )),
            None,
        ))
        .await
        .unwrap();

        assert!(!db.reset_orphaned_learner_log().await.unwrap());
        let (last, _) = db.load_log_state().await.unwrap().into_parts();
        assert_eq!(last.unwrap().index(), 124_022);
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
