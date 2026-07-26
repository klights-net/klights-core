use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::controller_dispatcher::ControllerDispatcher;
use crate::controllers::cronjob_scheduler::{
    CronJobScheduler, CronJobSchedulerRuntime, CronJobWatch,
};
use crate::datastore::sqlite::DatastoreWatchReplaySource;
use crate::datastore::{DatastoreHandle, WatchTarget};
use crate::watch::{
    SignalWatchCursor, WatchCursorError, WatchDeliveryScope, WatchEvent, WindowPolicy,
};
use klights_supervisor::TaskSupervisor;
use klights_watch::WatchTopic;

struct LeaderCronJobSchedulerRuntime {
    db: DatastoreHandle,
    dispatcher: Arc<ControllerDispatcher>,
}

struct LeaderCronJobWatch {
    cursor: SignalWatchCursor<DatastoreWatchReplaySource>,
}

#[async_trait]
impl CronJobWatch for LeaderCronJobWatch {
    async fn next_event(&mut self) -> std::result::Result<WatchEvent, WatchCursorError> {
        self.cursor.next_event().await
    }
}

#[async_trait]
impl CronJobSchedulerRuntime for LeaderCronJobSchedulerRuntime {
    async fn list_cronjobs(&self) -> Result<Vec<klights_cluster_core::Resource>> {
        self.db
            .list_resources(
                "batch/v1",
                "CronJob",
                None,
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .map(|listing| listing.items)
    }

    async fn reconcile_cronjob(&self, resource: &klights_cluster_core::Resource) -> Result<()> {
        crate::controllers::cronjob::reconcile_cronjob_one(
            self.db.as_ref(),
            Some(self.dispatcher.as_ref()),
            &resource.data,
            resource.resource_version,
        )
        .await
    }

    async fn subscribe_watch(&self) -> Result<Box<dyn CronJobWatch>> {
        let topic = WatchTopic::new("batch/v1", "CronJob");
        let accepted_rv = self.db.get_current_resource_version().await?;
        let cursor = SignalWatchCursor::new(
            self.db.subscribe_watch_signals(topic.clone()),
            DatastoreWatchReplaySource::new(
                Arc::new(crate::datastore::DatastoreBackendWatchStore::new(
                    self.db.clone(),
                )),
                vec![WatchTarget::namespaced("batch/v1", "CronJob")],
            ),
            topic,
            WatchDeliveryScope::NamespacedAll,
            accepted_rv,
            WindowPolicy::default_watch_delivery(),
        );
        Ok(Box::new(LeaderCronJobWatch { cursor }))
    }
}

pub(crate) fn new_leader_scheduler(
    db: DatastoreHandle,
    dispatcher: Arc<ControllerDispatcher>,
    supervisor: Arc<TaskSupervisor>,
) -> Arc<CronJobScheduler> {
    CronJobScheduler::new(
        Arc::new(LeaderCronJobSchedulerRuntime { db, dispatcher }),
        supervisor,
    )
}
