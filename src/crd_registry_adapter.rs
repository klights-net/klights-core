use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::controllers::crd::{CrdRegistryReader, CrdRegistryRuntime, CrdRegistryWatchSession};
use crate::datastore::{DatastoreBackend, DatastoreHandle};
use klights_leader_api::{LeaderWatch, LeaderWatchError, WatchRequest};

struct LeaderCrdRegistryRuntime {
    db: DatastoreHandle,
    positioned_watch: klights_watch::PositionedWatchService,
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
    async fn open_crd_watch(
        &self,
    ) -> std::result::Result<CrdRegistryWatchSession, LeaderWatchError> {
        let listing = self
            .db
            .list_resources(
                "apiextensions.k8s.io/v1",
                "CustomResourceDefinition",
                None,
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .map_err(|error| LeaderWatchError::unavailable(error.to_string()))?;
        let request = WatchRequest::try_new(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            None,
            None,
            Some(listing.resource_version),
            listing.watch_replay_position,
        )?;
        let events = self.positioned_watch.watch_resources(request).await?;
        Ok(CrdRegistryWatchSession {
            initial_values: listing
                .items
                .into_iter()
                .map(|resource| Arc::unwrap_or_clone(resource.data))
                .collect(),
            events,
        })
    }
}

pub(crate) fn new_runtime(
    db: DatastoreHandle,
    watch_signals: Arc<dyn klights_watch::WatchSignalSubscribe>,
) -> Arc<dyn CrdRegistryRuntime> {
    let positioned_watch = crate::control_plane::client::local::datastore_positioned_watch_service(
        db.clone(),
        watch_signals,
    );
    Arc::new(LeaderCrdRegistryRuntime {
        db,
        positioned_watch,
    })
}
