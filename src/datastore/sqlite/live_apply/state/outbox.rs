use super::super::queries;
use klights_cluster_core::LogApplyAppliedOutboxRow;

pub(super) struct OutboxLedgerStateApplier<'tx, 'conn> {
    tx: &'tx rusqlite::Transaction<'conn>,
}

impl<'tx, 'conn> OutboxLedgerStateApplier<'tx, 'conn> {
    pub(super) fn new(tx: &'tx rusqlite::Transaction<'conn>) -> Self {
        Self { tx }
    }

    pub(super) fn put_applied_outbox(
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

    pub(super) fn delete_applied_outbox(
        &self,
        idempotency_key: String,
    ) -> tokio_rusqlite::Result<()> {
        self.tx.execute(
            queries::APPLIED_OUTBOX_DELETE_BY_KEY,
            rusqlite::params![idempotency_key],
        )?;
        Ok(())
    }

    pub(super) fn gc_applied_outbox(&self, cutoff_ms: i64) -> tokio_rusqlite::Result<()> {
        self.tx.execute(
            queries::APPLIED_OUTBOX_DELETE_EXPIRED,
            rusqlite::params![cutoff_ms],
        )?;
        Ok(())
    }
}
