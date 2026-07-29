#![cfg(test)]

use klights_leader_api::{LeaderWatch, LeaderWatchFuture, WatchRequest};

use crate::datastore::DatastoreBackendWatchStore;

impl LeaderWatch for DatastoreBackendWatchStore {
    fn watch_resources(&self, request: WatchRequest) -> LeaderWatchFuture<'_> {
        let positioned = crate::positioned_watch_adapter::for_test(self.db());
        Box::pin(async move { LeaderWatch::watch_resources(&positioned, request).await })
    }
}
