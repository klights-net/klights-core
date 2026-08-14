use klights_cluster_store::ResourceListOptions;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;

use crate::datastore::{DatastoreBackend, DatastoreHandle};
use klights_controllers::side_effects::workload_pod::WorkloadPodStore;

struct BorrowedWorkloadPodStore<'a> {
    db: &'a dyn DatastoreBackend,
}

struct OwnedWorkloadPodStore {
    db: DatastoreHandle,
}

pub(crate) fn borrowed_store(db: &dyn DatastoreBackend) -> impl WorkloadPodStore + '_ {
    BorrowedWorkloadPodStore { db }
}

#[async_trait]
impl WorkloadPodStore for BorrowedWorkloadPodStore<'_> {
    async fn get_replica_set(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        self.db
            .get_resource("apps/v1", "ReplicaSet", Some(namespace), name)
            .await
    }

    async fn list_replica_sets(&self, namespace: &str) -> Result<Vec<Resource>> {
        self.db
            .list_resources(
                "apps/v1",
                "ReplicaSet",
                Some(namespace),
                ResourceListOptions::all(),
            )
            .await
            .map(|listing| listing.items)
    }

    async fn list_replication_controllers(&self, namespace: &str) -> Result<Vec<Resource>> {
        self.db
            .list_resources(
                "v1",
                "ReplicationController",
                Some(namespace),
                ResourceListOptions::all(),
            )
            .await
            .map(|listing| listing.items)
    }
}

#[async_trait]
impl WorkloadPodStore for OwnedWorkloadPodStore {
    async fn get_replica_set(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        borrowed_store(self.db.as_ref())
            .get_replica_set(namespace, name)
            .await
    }

    async fn list_replica_sets(&self, namespace: &str) -> Result<Vec<Resource>> {
        borrowed_store(self.db.as_ref())
            .list_replica_sets(namespace)
            .await
    }

    async fn list_replication_controllers(&self, namespace: &str) -> Result<Vec<Resource>> {
        borrowed_store(self.db.as_ref())
            .list_replication_controllers(namespace)
            .await
    }
}

pub(crate) fn port(db: DatastoreHandle) -> Arc<dyn WorkloadPodStore> {
    Arc::new(OwnedWorkloadPodStore { db })
}
