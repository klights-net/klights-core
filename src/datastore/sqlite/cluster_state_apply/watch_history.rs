use super::super::crud::helpers::{
    WatchEventInsert, WatchEventPayload, insert_watch_event_in_conn,
};
use super::super::{create_staged_post_commit, gc::gc_watch_events_in_tx};
use klights_cluster_core::LogApplyWatchEventRow;

pub(in crate::datastore::sqlite) struct WatchHistoryStateApplier<'tx, 'conn> {
    tx: &'tx rusqlite::Transaction<'conn>,
}

impl<'tx, 'conn> WatchHistoryStateApplier<'tx, 'conn> {
    pub(super) fn new(tx: &'tx rusqlite::Transaction<'conn>) -> Self {
        Self { tx }
    }

    pub(in crate::datastore::sqlite) fn apply_put_watch_event(
        &self,
        row: LogApplyWatchEventRow,
    ) -> tokio_rusqlite::Result<klights_cluster_store::StagedPostCommit> {
        let data_bytes = serde_json::to_vec(&row.data)
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?;
        insert_watch_event_in_conn(
            self.tx,
            WatchEventInsert::preserve_committed_payload(
                row.event_id,
                WatchEventPayload {
                    api_version: &row.api_version,
                    kind: &row.kind,
                    namespace: row.namespace.as_deref(),
                    name: &row.name,
                    resource_version: row.resource_version,
                    event_type: &row.event_type,
                    data: &data_bytes,
                },
            ),
        )?;
        Ok(create_staged_post_commit(
            &row.api_version,
            &row.kind,
            row.namespace.as_deref(),
            &row.name,
            row.resource_version,
            &row.event_type,
            row.data,
        ))
    }

    pub(in crate::datastore::sqlite) fn apply_gc_watch_events(
        &self,
        max_rows: i64,
        batch_cap: i64,
    ) -> tokio_rusqlite::Result<()> {
        let removed = gc_watch_events_in_tx(self.tx, max_rows, batch_cap)?;
        if removed > 0 {
            let _ = self.tx.execute("PRAGMA incremental_vacuum(1000)", []);
        }
        Ok(())
    }
}
