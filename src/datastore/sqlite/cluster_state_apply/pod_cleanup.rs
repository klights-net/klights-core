use super::super::queries;
use crate::log_apply::{LogApplyPodCleanupIntentKey, LogApplyPodCleanupIntentRow};

pub(in crate::datastore::sqlite) struct PodCleanupStateApplier<'tx, 'conn> {
    tx: &'tx rusqlite::Transaction<'conn>,
}

impl<'tx, 'conn> PodCleanupStateApplier<'tx, 'conn> {
    pub(super) fn new(tx: &'tx rusqlite::Transaction<'conn>) -> Self {
        Self { tx }
    }

    pub(in crate::datastore::sqlite) fn put_pod_cleanup_intent(
        &self,
        row: LogApplyPodCleanupIntentRow,
    ) -> tokio_rusqlite::Result<()> {
        let pod_data = serde_json::to_vec(&row.pod_data)
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?;
        self.tx.execute(
            queries::POD_CLEANUP_INTENT_UPSERT,
            rusqlite::params![
                row.node_name,
                row.namespace,
                row.pod_name,
                row.pod_uid,
                row.reason,
                row.resource_version,
                row.created_at_ms,
                pod_data
            ],
        )?;
        Ok(())
    }

    pub(in crate::datastore::sqlite) fn delete_pod_cleanup_intent(
        &self,
        key: LogApplyPodCleanupIntentKey,
    ) -> tokio_rusqlite::Result<()> {
        self.tx.execute(
            queries::POD_CLEANUP_INTENT_DELETE,
            rusqlite::params![
                key.node_name,
                key.namespace,
                key.pod_name,
                key.pod_uid,
                key.reason
            ],
        )?;
        Ok(())
    }

    pub(in crate::datastore::sqlite) fn delete_pod_cleanup_intents_for_node(
        &self,
        node_name: String,
    ) -> tokio_rusqlite::Result<()> {
        self.tx.execute(
            queries::POD_CLEANUP_INTENTS_DELETE_BY_NODE,
            rusqlite::params![node_name],
        )?;
        Ok(())
    }
}
