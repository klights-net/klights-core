use klights_cluster_store::ResourceListOptions;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;

use crate::datastore::DatastoreHandle;
use klights_controllers::side_effects::apiservice::ApiServiceSideEffectStore;

struct RootApiServiceSideEffectStore {
    db: DatastoreHandle,
}

#[async_trait]
impl ApiServiceSideEffectStore for RootApiServiceSideEffectStore {
    async fn list_apiservices(&self) -> Result<Vec<Resource>> {
        self.db
            .list_resources(
                "apiregistration.k8s.io/v1",
                "APIService",
                None,
                ResourceListOptions::all(),
            )
            .await
            .map(|listing| listing.items)
    }
}

pub(crate) fn port(db: DatastoreHandle) -> Arc<dyn ApiServiceSideEffectStore> {
    Arc::new(RootApiServiceSideEffectStore { db })
}
