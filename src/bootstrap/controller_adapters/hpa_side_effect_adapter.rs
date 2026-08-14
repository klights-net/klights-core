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
use klights_controllers::side_effects::hpa::HpaSideEffectStore;

struct RootHpaSideEffectStore {
    resource_reads: Arc<dyn ClusterResourceRead>,
}

#[async_trait]
impl HpaSideEffectStore for RootHpaSideEffectStore {
    async fn list_hpas(&self, api_version: &'static str, namespace: &str) -> Result<Vec<Resource>> {
        match self
            .resource_reads
            .list_resources(ResourceListRequest::new(
                api_version,
                "HorizontalPodAutoscaler",
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
                "HPA LIST at resourceVersion {requested} expired before {oldest_available}"
            ),
        }
    }
}

pub(crate) fn port(resource_reads: Arc<dyn ClusterResourceRead>) -> Arc<dyn HpaSideEffectStore> {
    Arc::new(RootHpaSideEffectStore { resource_reads })
}

#[cfg(test)]
struct DirectHpaSideEffectStore {
    db: DatastoreHandle,
}
#[cfg(test)]
#[async_trait]
impl HpaSideEffectStore for DirectHpaSideEffectStore {
    async fn list_hpas(&self, api_version: &'static str, namespace: &str) -> Result<Vec<Resource>> {
        self.db
            .list_resources(
                api_version,
                "HorizontalPodAutoscaler",
                Some(namespace),
                klights_cluster_store::ResourceListOptions::all(),
            )
            .await
            .map(|page| page.items)
    }
}
#[cfg(test)]
pub(crate) fn port_for_test(db: DatastoreHandle) -> Arc<dyn HpaSideEffectStore> {
    Arc::new(DirectHpaSideEffectStore { db })
}
