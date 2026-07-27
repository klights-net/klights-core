use klights_leader_api::{LeaderWatch, LeaderWatchFuture, WatchRequest};

use crate::datastore::DatastoreBackendWatchStore;

impl LeaderWatch for DatastoreBackendWatchStore {
    fn watch_resources(&self, request: WatchRequest) -> LeaderWatchFuture<'_> {
        let positioned =
            crate::control_plane::client::local::datastore_positioned_watch_service(self.db());
        Box::pin(async move { LeaderWatch::watch_resources(&positioned, request).await })
    }
}
