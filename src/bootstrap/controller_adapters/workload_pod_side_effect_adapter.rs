use klights_cluster_store::{
    ClusterResourceRead, ResourceCollectionScope, ResourceGetRequest, ResourceListQuery,
    ResourceListRead, ResourceListRequest,
};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;

#[cfg(test)]
use crate::datastore::DatastoreHandle;
use klights_controllers::side_effects::workload_pod::WorkloadPodStore;

struct BorrowedWorkloadPodStore<'a> {
    resource_reads: &'a dyn ClusterResourceRead,
}

struct OwnedWorkloadPodStore {
    resource_reads: Arc<dyn ClusterResourceRead>,
}

pub(crate) fn borrowed_store(
    resource_reads: &dyn ClusterResourceRead,
) -> impl WorkloadPodStore + '_ {
    BorrowedWorkloadPodStore { resource_reads }
}

#[async_trait]
impl WorkloadPodStore for BorrowedWorkloadPodStore<'_> {
    async fn get_replica_set(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        self.resource_reads
            .get_resource(ResourceGetRequest::new(
                "apps/v1",
                "ReplicaSet",
                Some(namespace.to_string()),
                name,
            ))
            .await
            .map_err(Into::into)
    }

    async fn list_replica_sets(&self, namespace: &str) -> Result<Vec<Resource>> {
        list_resources(self.resource_reads, "apps/v1", "ReplicaSet", namespace).await
    }

    async fn list_replication_controllers(&self, namespace: &str) -> Result<Vec<Resource>> {
        list_resources(
            self.resource_reads,
            "v1",
            "ReplicationController",
            namespace,
        )
        .await
    }
}

async fn list_resources(
    resource_reads: &dyn ClusterResourceRead,
    api_version: &str,
    kind: &str,
    namespace: &str,
) -> Result<Vec<Resource>> {
    match resource_reads
        .list_resources(ResourceListRequest::new(
            api_version,
            kind,
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
            "{api_version}/{kind} LIST at resourceVersion {requested} expired before {oldest_available}"
        ),
    }
}

#[async_trait]
impl WorkloadPodStore for OwnedWorkloadPodStore {
    async fn get_replica_set(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        self.resource_reads
            .get_resource(ResourceGetRequest::new(
                "apps/v1",
                "ReplicaSet",
                Some(namespace.to_string()),
                name,
            ))
            .await
            .map_err(Into::into)
    }

    async fn list_replica_sets(&self, namespace: &str) -> Result<Vec<Resource>> {
        list_resources(
            self.resource_reads.as_ref(),
            "apps/v1",
            "ReplicaSet",
            namespace,
        )
        .await
    }

    async fn list_replication_controllers(&self, namespace: &str) -> Result<Vec<Resource>> {
        list_resources(
            self.resource_reads.as_ref(),
            "v1",
            "ReplicationController",
            namespace,
        )
        .await
    }
}

pub(crate) fn port(resource_reads: Arc<dyn ClusterResourceRead>) -> Arc<dyn WorkloadPodStore> {
    Arc::new(OwnedWorkloadPodStore { resource_reads })
}

#[cfg(test)]
struct DirectWorkloadPodStore {
    db: DatastoreHandle,
}
#[cfg(test)]
#[async_trait]
impl WorkloadPodStore for DirectWorkloadPodStore {
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
                klights_cluster_store::ResourceListOptions::all(),
            )
            .await
            .map(|page| page.items)
    }
    async fn list_replication_controllers(&self, namespace: &str) -> Result<Vec<Resource>> {
        self.db
            .list_resources(
                "v1",
                "ReplicationController",
                Some(namespace),
                klights_cluster_store::ResourceListOptions::all(),
            )
            .await
            .map(|page| page.items)
    }
}
#[cfg(test)]
pub(crate) fn port_for_test(db: DatastoreHandle) -> Arc<dyn WorkloadPodStore> {
    Arc::new(DirectWorkloadPodStore { db })
}
