use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::controllers::crd::{CrdRegistryEventStream, CrdRegistryReader, CrdRegistryRuntime};
use crate::datastore::sqlite::DatastoreWatchReplaySource;
use crate::datastore::{DatastoreBackend, DatastoreHandle, WatchTarget};
use crate::watch::{
    SignalWatchCursor, WatchCursorError, WatchDeliveryScope, WatchEvent, WindowPolicy,
};
use klights_watch::WatchTopic;

struct LeaderCrdRegistryRuntime {
    db: DatastoreHandle,
}

struct LeaderCrdRegistryEventStream {
    cursor: SignalWatchCursor<DatastoreWatchReplaySource>,
}

#[async_trait]
impl<T> CrdRegistryReader for T
where
    T: DatastoreBackend + ?Sized,
{
    async fn list_crd_values(&self) -> Result<Vec<serde_json::Value>> {
        self.list_resources(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .map(|listing| {
            listing
                .items
                .into_iter()
                .map(|resource| Arc::unwrap_or_clone(resource.data))
                .collect()
        })
    }
}

#[async_trait]
impl CrdRegistryReader for LeaderCrdRegistryRuntime {
    async fn list_crd_values(&self) -> Result<Vec<serde_json::Value>> {
        CrdRegistryReader::list_crd_values(self.db.as_ref()).await
    }
}

#[async_trait]
impl CrdRegistryRuntime for LeaderCrdRegistryRuntime {
    async fn subscribe_crd_events(&self) -> Result<Box<dyn CrdRegistryEventStream>> {
        let topic = WatchTopic::new("apiextensions.k8s.io/v1", "CustomResourceDefinition");
        let start_rv = self.db.get_current_resource_version().await?;
        let cursor = SignalWatchCursor::new(
            self.db.subscribe_watch_signals(topic.clone()),
            DatastoreWatchReplaySource::new(
                Arc::new(crate::datastore::DatastoreBackendWatchStore::new(
                    self.db.clone(),
                )),
                vec![WatchTarget::cluster(
                    "apiextensions.k8s.io/v1",
                    "CustomResourceDefinition",
                )],
            ),
            topic,
            WatchDeliveryScope::Cluster,
            start_rv,
            WindowPolicy::default_watch_delivery(),
        );
        Ok(Box::new(LeaderCrdRegistryEventStream { cursor }))
    }
}

#[async_trait]
impl CrdRegistryEventStream for LeaderCrdRegistryEventStream {
    async fn prime_replay(&mut self) -> std::result::Result<(), WatchCursorError> {
        self.cursor.prime_replay_or_expired().await.map(|_| ())
    }

    async fn next_event(&mut self) -> std::result::Result<WatchEvent, WatchCursorError> {
        self.cursor.next_event().await
    }
}

pub(crate) fn new_runtime(db: DatastoreHandle) -> Arc<dyn CrdRegistryRuntime> {
    Arc::new(LeaderCrdRegistryRuntime { db })
}
