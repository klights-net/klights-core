use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::controllers::scheduler::SchedulerRuntime;
use crate::datastore::DatastoreHandle;
use crate::kubelet::pod_repository::PodRepository;

pub(crate) struct LeaderSchedulerRuntime {
    db: DatastoreHandle,
    pods: Arc<PodRepository>,
}

impl LeaderSchedulerRuntime {
    pub(crate) fn new(db: DatastoreHandle, pods: Arc<PodRepository>) -> Self {
        Self { db, pods }
    }
}

#[async_trait]
impl SchedulerRuntime for LeaderSchedulerRuntime {
    fn subscribe_signals(&self) -> klights_watch::WatchSignalReceiver {
        klights_watch::WatchSignalReceiver::new(
            [
                klights_watch::WatchTopic::new("v1", "Pod"),
                klights_watch::WatchTopic::new("v1", "Node"),
            ]
            .into_iter()
            .map(|topic| self.db.subscribe_watch_signals(topic))
            .collect(),
        )
    }

    async fn current_resource_version(&self) -> Result<i64> {
        self.db.get_current_resource_version().await
    }

    async fn schedule_all_unbound_pods(&self) -> Result<()> {
        self.pods.schedule_all_unbound_pods().await
    }
}
