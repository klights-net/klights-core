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
use klights_controllers::side_effects::apiservice::ApiServiceSideEffectStore;

struct RootApiServiceSideEffectStore {
    resource_reads: Arc<dyn ClusterResourceRead>,
}

#[async_trait]
impl ApiServiceSideEffectStore for RootApiServiceSideEffectStore {
    async fn list_apiservices(&self) -> Result<Vec<Resource>> {
        match self
            .resource_reads
            .list_resources(ResourceListRequest::new(
                "apiregistration.k8s.io/v1",
                "APIService",
                ResourceCollectionScope::Cluster,
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
                "APIService LIST at resourceVersion {requested} expired before {oldest_available}"
            ),
        }
    }
}

pub(crate) fn port(
    resource_reads: Arc<dyn ClusterResourceRead>,
) -> Arc<dyn ApiServiceSideEffectStore> {
    Arc::new(RootApiServiceSideEffectStore { resource_reads })
}

#[cfg(test)]
struct DirectApiServiceSideEffectStore {
    db: DatastoreHandle,
}
#[cfg(test)]
#[async_trait]
impl ApiServiceSideEffectStore for DirectApiServiceSideEffectStore {
    async fn list_apiservices(&self) -> Result<Vec<Resource>> {
        self.db
            .list_resources(
                "apiregistration.k8s.io/v1",
                "APIService",
                None,
                klights_cluster_store::ResourceListOptions::all(),
            )
            .await
            .map(|page| page.items)
    }
}
#[cfg(test)]
pub(crate) fn port_for_test(db: DatastoreHandle) -> Arc<dyn ApiServiceSideEffectStore> {
    Arc::new(DirectApiServiceSideEffectStore { db })
}
