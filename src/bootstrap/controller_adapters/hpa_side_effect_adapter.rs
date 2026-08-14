use klights_cluster_store::ResourceListOptions;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;

use crate::datastore::DatastoreHandle;
use klights_controllers::side_effects::hpa::HpaSideEffectStore;

struct RootHpaSideEffectStore {
    db: DatastoreHandle,
}

#[async_trait]
impl HpaSideEffectStore for RootHpaSideEffectStore {
    async fn list_hpas(&self, api_version: &'static str, namespace: &str) -> Result<Vec<Resource>> {
        self.db
            .list_resources(
                api_version,
                "HorizontalPodAutoscaler",
                Some(namespace),
                ResourceListOptions::all(),
            )
            .await
            .map(|listing| listing.items)
    }
}

pub(crate) fn port(db: DatastoreHandle) -> Arc<dyn HpaSideEffectStore> {
    Arc::new(RootHpaSideEffectStore { db })
}
