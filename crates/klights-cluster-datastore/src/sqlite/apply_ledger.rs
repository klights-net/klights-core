//! Read-only SQLite view of durable committed-apply ledger state.

use klights_cluster_core::{LogApplyAppliedOutboxRow, OutboxStreamWatermark, WatchReplayPosition};
use klights_cluster_store::{AppliedOutboxLookup, CommittedApplyFuture, DurableApplyLedgerRead};
use klights_supervisor::DbExecutor;
use rusqlite::OptionalExtension;

use super::live_apply::map_committed_apply_error;
use super::mutation_queries;

/// SQLite owner of the read-only durable committed-apply ledger capability.
#[derive(Clone)]
pub struct SqliteApplyLedgerRead {
    executor: DbExecutor,
}

impl SqliteApplyLedgerRead {
    pub fn new(executor: DbExecutor) -> Self {
        Self { executor }
    }
}

impl DurableApplyLedgerRead for SqliteApplyLedgerRead {
    fn current_apply_position(&self) -> CommittedApplyFuture<'_, WatchReplayPosition> {
        Box::pin(async move {
            self.executor
                .call_raw("read_apply_position", |connection| {
                    let transaction = connection.transaction()?;
                    let raw_resource_version: String = transaction.query_row(
                        "SELECT value FROM metadata WHERE key = 'resource_version'",
                        [],
                        |row| row.get(0),
                    )?;
                    let resource_version = raw_resource_version.parse::<i64>().map_err(|_| {
                        super::live_apply::other_error(format!(
                            "invalid resource_version metadata {raw_resource_version:?}"
                        ))
                    })?;
                    let event_id = transaction.query_row(
                        "SELECT COALESCE((SELECT seq FROM sqlite_sequence \
                         WHERE name = 'watch_events'), 0)",
                        [],
                        |row| row.get(0),
                    )?;
                    transaction.commit()?;
                    Ok(WatchReplayPosition {
                        resource_version,
                        event_id,
                        resource_version_filter_through_event_id: 0,
                    })
                })
                .await
                .map_err(anyhow::Error::new)
                .map_err(map_committed_apply_error)
        })
    }

    fn get_applied_outbox(
        &self,
        lookup: AppliedOutboxLookup,
    ) -> CommittedApplyFuture<'_, Option<LogApplyAppliedOutboxRow>> {
        Box::pin(async move {
            let idempotency_key = lookup.into_idempotency_key();
            self.executor
                .call_raw("read_applied_outbox", move |connection| {
                    connection
                        .query_row(
                            mutation_queries::APPLIED_OUTBOX_GET,
                            [idempotency_key],
                            |row| {
                                Ok(LogApplyAppliedOutboxRow {
                                    idempotency_key: row.get(0)?,
                                    subject_key: row.get(1)?,
                                    operation: row.get(2)?,
                                    first_seen_ms: row.get(3)?,
                                    applied_rv: row.get(4)?,
                                    result_proto: row.get(5)?,
                                    status_stamp: row.get(6)?,
                                })
                            },
                        )
                        .optional()
                        .map_err(tokio_rusqlite::Error::from)
                })
                .await
                .map_err(anyhow::Error::new)
                .map_err(map_committed_apply_error)
        })
    }

    fn list_outbox_watermarks(&self) -> CommittedApplyFuture<'_, Vec<OutboxStreamWatermark>> {
        Box::pin(async move {
            self.executor
                .call_raw("list_outbox_watermarks", |connection| {
                    let mut statement = connection.prepare(
                        "SELECT client_id, stream_id, last_seq \
                         FROM outbox_stream_watermarks \
                         ORDER BY client_id ASC, stream_id ASC",
                    )?;
                    let rows = statement.query_map([], |row| {
                        Ok(OutboxStreamWatermark {
                            client_id: row.get(0)?,
                            stream_id: row.get(1)?,
                            stream_seq: row.get(2)?,
                        })
                    })?;
                    rows.collect::<rusqlite::Result<Vec<_>>>()
                        .map_err(tokio_rusqlite::Error::from)
                })
                .await
                .map_err(anyhow::Error::new)
                .map_err(map_committed_apply_error)
        })
    }
}
