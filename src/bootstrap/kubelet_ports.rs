use std::sync::Arc;

use crate::datastore::sqlite::DatastoreWatchReplaySource;
use crate::datastore::{CurrentResourceVersionStore, WatchStore, WatchTarget};
use crate::kubelet::pod_watch_source::{BoxedWatchReplaySource, PodWatchSource};
use crate::node_heartbeat::NodeHeartbeatWatchSource;
use klights_watch::WatchTopic;

pub struct DatastorePodWatchSource {
    watch_store: Arc<dyn WatchStore>,
    resource_versions: Arc<dyn CurrentResourceVersionStore>,
}

impl DatastorePodWatchSource {
    pub fn new<T>(store: Arc<T>) -> Self
    where
        T: WatchStore + CurrentResourceVersionStore + 'static,
    {
        Self {
            watch_store: store.clone(),
            resource_versions: store,
        }
    }
}

#[async_trait::async_trait]
impl PodWatchSource for DatastorePodWatchSource {
    fn subscribe_watch_signals(&self, topic: WatchTopic) -> klights_watch::WatchSignalReceiver {
        self.watch_store.subscribe_watch_signals(topic)
    }

    fn replay_source(&self, targets: Vec<WatchTarget>) -> BoxedWatchReplaySource {
        BoxedWatchReplaySource::new(Arc::new(DatastoreWatchReplaySource::new(
            self.watch_store.clone(),
            targets,
        )))
    }

    async fn current_resource_version(&self) -> anyhow::Result<i64> {
        self.resource_versions.get_current_resource_version().await
    }
}

#[async_trait::async_trait]
impl NodeHeartbeatWatchSource for DatastorePodWatchSource {
    fn subscribe_watch_signals(&self, topic: WatchTopic) -> klights_watch::WatchSignalReceiver {
        self.watch_store.subscribe_watch_signals(topic)
    }

    fn replay_source(&self, targets: Vec<WatchTarget>) -> BoxedWatchReplaySource {
        BoxedWatchReplaySource::new(Arc::new(DatastoreWatchReplaySource::new(
            self.watch_store.clone(),
            targets,
        )))
    }

    async fn current_resource_version(&self) -> anyhow::Result<i64> {
        self.resource_versions.get_current_resource_version().await
    }
}
