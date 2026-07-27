#![cfg(test)]

use klights_leader_api::{LeaderWatch, LeaderWatchFuture, WatchRequest};

use crate::datastore::DatastoreBackendWatchStore;

impl LeaderWatch for DatastoreBackendWatchStore {
    fn watch_resources(&self, request: WatchRequest) -> LeaderWatchFuture<'_> {
        let sink = self.db().commit_observation_sink();
        let signals = sink
            .as_any()
            .downcast_ref::<crate::watch_commit_observation_adapter::WatchCommitObservationSink>()
            .expect("test datastore watch sink")
            .signal_source();
        let positioned = crate::control_plane::client::local::datastore_positioned_watch_service(
            self.db(),
            signals,
        );
        Box::pin(async move { LeaderWatch::watch_resources(&positioned, request).await })
    }
}
