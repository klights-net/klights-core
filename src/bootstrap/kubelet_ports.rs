use std::sync::Arc;

use crate::datastore::sqlite::DatastoreWatchReplaySource;
use crate::datastore::{DatastoreHandle, WatchTarget};
use crate::kubelet::pod_watch_source::{BoxedWatchReplaySource, PodWatchSource};
use crate::watch::{WatchSignal, WatchTopic};

pub struct DatastorePodWatchSource {
    db: DatastoreHandle,
}

impl DatastorePodWatchSource {
    pub fn new(db: DatastoreHandle) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl PodWatchSource for DatastorePodWatchSource {
    fn subscribe_watch_signals(
        &self,
        topic: WatchTopic,
    ) -> tokio::sync::broadcast::Receiver<WatchSignal> {
        self.db.subscribe_watch_signals(topic)
    }

    fn replay_source(&self, targets: Vec<WatchTarget>) -> BoxedWatchReplaySource {
        BoxedWatchReplaySource::new(Arc::new(DatastoreWatchReplaySource::new(
            self.db.clone(),
            targets,
        )))
    }

    async fn current_resource_version(&self) -> anyhow::Result<i64> {
        self.db.get_current_resource_version().await
    }
}
