use super::super::mutation_queries;

pub(super) struct ClusterMetaStateApplier<'tx, 'conn> {
    tx: &'tx rusqlite::Transaction<'conn>,
}

impl<'tx, 'conn> ClusterMetaStateApplier<'tx, 'conn> {
    pub(super) fn new(tx: &'tx rusqlite::Transaction<'conn>) -> Self {
        Self { tx }
    }

    pub(super) fn put_klights_meta(
        &self,
        key: String,
        value: String,
    ) -> klights_supervisor::DbClosureResult<()> {
        self.tx.execute(
            mutation_queries::UPSERT_KLIGHTS_META,
            rusqlite::params![&key, &value],
        )?;
        Ok(())
    }
}
