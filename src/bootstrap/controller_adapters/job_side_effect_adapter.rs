use klights_cluster_store::{
    ClusterResourceRead, ResourceCollectionScope, ResourceListQuery, ResourceListRead,
    ResourceListRequest,
};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;

#[cfg(test)]
use crate::datastore::DatastoreHandle;
use klights_controllers::side_effects::job::JobSideEffectStore;

struct BorrowedJobSideEffectStore<'a> {
    resource_reads: &'a dyn ClusterResourceRead,
}

struct OwnedJobSideEffectStore {
    resource_reads: Arc<dyn ClusterResourceRead>,
}

pub(crate) fn borrowed_store(
    resource_reads: &dyn ClusterResourceRead,
) -> impl JobSideEffectStore + '_ {
    BorrowedJobSideEffectStore { resource_reads }
}

#[async_trait]
impl JobSideEffectStore for BorrowedJobSideEffectStore<'_> {
    async fn list_jobs(&self, namespace: &str) -> Result<Vec<Resource>> {
        match self
            .resource_reads
            .list_resources(ResourceListRequest::new(
                "batch/v1",
                "Job",
                ResourceCollectionScope::Namespace(namespace.to_string()),
                ResourceListQuery::all(),
            ))
            .await?
        {
            ResourceListRead::Current(page) | ResourceListRead::Historical(page) => {
                Ok(page.into_items())
            }
            ResourceListRead::Expired {
                requested,
                oldest_available,
                ..
            } => anyhow::bail!(
                "batch/v1/Job LIST at resourceVersion {requested} expired before {oldest_available}"
            ),
        }
    }
}

#[async_trait]
impl JobSideEffectStore for OwnedJobSideEffectStore {
    async fn list_jobs(&self, namespace: &str) -> Result<Vec<Resource>> {
        match self
            .resource_reads
            .list_resources(ResourceListRequest::new(
                "batch/v1",
                "Job",
                ResourceCollectionScope::Namespace(namespace.to_string()),
                ResourceListQuery::all(),
            ))
            .await?
        {
            ResourceListRead::Current(page) | ResourceListRead::Historical(page) => {
                Ok(page.into_items())
            }
            ResourceListRead::Expired {
                requested,
                oldest_available,
                ..
            } => anyhow::bail!(
                "Job LIST at resourceVersion {requested} expired before {oldest_available}"
            ),
        }
    }
}

pub(crate) fn port(resource_reads: Arc<dyn ClusterResourceRead>) -> Arc<dyn JobSideEffectStore> {
    Arc::new(OwnedJobSideEffectStore { resource_reads })
}

#[cfg(test)]
struct DirectJobSideEffectStore {
    db: DatastoreHandle,
}
#[cfg(test)]
#[async_trait]
impl JobSideEffectStore for DirectJobSideEffectStore {
    async fn list_jobs(&self, namespace: &str) -> Result<Vec<Resource>> {
        self.db
            .list_resources(
                "batch/v1",
                "Job",
                Some(namespace),
                klights_cluster_store::ResourceListOptions::all(),
            )
            .await
            .map(|page| page.items)
    }
}
#[cfg(test)]
pub(crate) fn port_for_test(db: DatastoreHandle) -> Arc<dyn JobSideEffectStore> {
    Arc::new(DirectJobSideEffectStore { db })
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_job_reconcile_name() {
        let (db, _db_handle) =
            crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
        let effect = klights_controllers::side_effects::job::effect(
            super::port(db.focused_read_store()),
            klights_controllers::side_effects::ControllerDispatcherSlot::new(),
        );
        assert_eq!(effect.name(), "job_reconcile");
    }
}
