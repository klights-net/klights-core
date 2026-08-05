use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;

use crate::datastore::{DatastoreBackend, DatastoreHandle, ResourceListQuery};
use klights_controllers::side_effects::job::JobSideEffectStore;

struct BorrowedJobSideEffectStore<'a> {
    db: &'a dyn DatastoreBackend,
}

struct OwnedJobSideEffectStore {
    db: DatastoreHandle,
}

pub(crate) fn borrowed_store(db: &dyn DatastoreBackend) -> impl JobSideEffectStore + '_ {
    BorrowedJobSideEffectStore { db }
}

#[async_trait]
impl JobSideEffectStore for BorrowedJobSideEffectStore<'_> {
    async fn list_jobs(&self, namespace: &str) -> Result<Vec<Resource>> {
        self.db
            .list_resources("batch/v1", "Job", Some(namespace), ResourceListQuery::all())
            .await
            .map(|listing| listing.items)
    }
}

#[async_trait]
impl JobSideEffectStore for OwnedJobSideEffectStore {
    async fn list_jobs(&self, namespace: &str) -> Result<Vec<Resource>> {
        borrowed_store(self.db.as_ref()).list_jobs(namespace).await
    }
}

pub(crate) fn port(db: DatastoreHandle) -> Arc<dyn JobSideEffectStore> {
    Arc::new(OwnedJobSideEffectStore { db })
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_job_reconcile_name() {
        let (_db, db_handle) =
            crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
        let effect = klights_controllers::side_effects::job::effect(
            super::port(db_handle),
            klights_controllers::side_effects::ControllerDispatcherSlot::new(),
        );
        assert_eq!(effect.name(), "job_reconcile");
    }
}
