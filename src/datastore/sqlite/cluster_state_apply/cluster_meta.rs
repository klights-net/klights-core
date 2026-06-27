use super::super::queries;

pub(in crate::datastore::sqlite) struct ClusterMetaStateApplier<'tx, 'conn> {
    tx: &'tx rusqlite::Transaction<'conn>,
}

impl<'tx, 'conn> ClusterMetaStateApplier<'tx, 'conn> {
    pub(super) fn new(tx: &'tx rusqlite::Transaction<'conn>) -> Self {
        Self { tx }
    }

    pub(in crate::datastore::sqlite) fn put_klights_meta(
        &self,
        key: String,
        value: String,
    ) -> tokio_rusqlite::Result<()> {
        self.tx.execute(
            queries::UPSERT_KLIGHTS_META,
            rusqlite::params![&key, &value],
        )?;
        Ok(())
    }
}
