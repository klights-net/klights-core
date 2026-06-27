use super::super::queries;
use crate::log_apply::LogApplyAppliedOutboxRow;

pub(in crate::datastore::sqlite) struct OutboxLedgerStateApplier<'tx, 'conn> {
    tx: &'tx rusqlite::Transaction<'conn>,
}

impl<'tx, 'conn> OutboxLedgerStateApplier<'tx, 'conn> {
    pub(super) fn new(tx: &'tx rusqlite::Transaction<'conn>) -> Self {
        Self { tx }
    }

    pub(in crate::datastore::sqlite) fn put_applied_outbox(
        &self,
        row: LogApplyAppliedOutboxRow,
    ) -> tokio_rusqlite::Result<()> {
        self.tx.execute(
            queries::APPLIED_OUTBOX_UPSERT_EXACT,
            rusqlite::params![
                row.idempotency_key,
                row.subject_key,
                row.operation,
                row.first_seen_ms,
                row.applied_rv,
                row.result_proto,
                row.status_stamp
            ],
        )?;
        Ok(())
    }

    pub(in crate::datastore::sqlite) fn delete_applied_outbox(
        &self,
        idempotency_key: String,
    ) -> tokio_rusqlite::Result<()> {
        self.tx.execute(
            queries::APPLIED_OUTBOX_DELETE_BY_KEY,
            rusqlite::params![idempotency_key],
        )?;
        Ok(())
    }

    pub(in crate::datastore::sqlite) fn gc_applied_outbox(
        &self,
        cutoff_ms: i64,
    ) -> tokio_rusqlite::Result<()> {
        self.tx.execute(
            queries::APPLIED_OUTBOX_DELETE_EXPIRED,
            rusqlite::params![cutoff_ms],
        )?;
        Ok(())
    }
}
