use std::sync::Arc;

use async_trait::async_trait;

use crate::controllers::ControllerDispatcher;
use crate::controllers::cronjob_scheduler::{
    CronJobScheduler, CronJobSchedulerRuntime, CronJobSchedulerRuntimeError, CronJobWatchSession,
};
use crate::datastore::DatastoreHandle;
use klights_leader_api::{LeaderWatch, LeaderWatchError, WatchRequest};
use klights_supervisor::TaskSupervisor;

struct LeaderCronJobSchedulerRuntime {
    db: DatastoreHandle,
    dispatcher: Arc<ControllerDispatcher>,
    positioned_watch: klights_watch::PositionedWatchService,
}

#[async_trait]
impl CronJobSchedulerRuntime for LeaderCronJobSchedulerRuntime {
    fn wall_time(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }

    async fn list_cronjobs(
        &self,
    ) -> std::result::Result<Vec<klights_cluster_core::Resource>, CronJobSchedulerRuntimeError>
    {
        self.db
            .list_resources(
                "batch/v1",
                "CronJob",
                None,
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .map(|listing| listing.items)
            .map_err(|error| CronJobSchedulerRuntimeError::query_unavailable(error.to_string()))
    }

    async fn reconcile_cronjob(
        &self,
        resource: &klights_cluster_core::Resource,
    ) -> std::result::Result<(), CronJobSchedulerRuntimeError> {
        klights_leader_api::validate_controller_lease_if_scoped()
            .map_err(|error| CronJobSchedulerRuntimeError::reconcile_failed(error.to_string()))?;
        crate::controllers::cronjob::reconcile_cronjob_one_at(
            self.db.as_ref(),
            Some(self.dispatcher.as_ref()),
            &resource.data,
            resource.resource_version,
            self.wall_time(),
        )
        .await
        .map_err(|error| CronJobSchedulerRuntimeError::reconcile_failed(error.to_string()))
    }

    async fn open_watch(&self) -> std::result::Result<CronJobWatchSession, LeaderWatchError> {
        let listing = self
            .db
            .list_resources(
                "batch/v1",
                "CronJob",
                None,
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .map_err(|error| LeaderWatchError::unavailable(error.to_string()))?;
        let request = WatchRequest::try_new(
            "batch/v1",
            "CronJob",
            None,
            None,
            None,
            Some(listing.resource_version),
            listing.watch_replay_position,
        )?;
        let events = self.positioned_watch.watch_resources(request).await?;
        Ok(CronJobWatchSession {
            initial_resources: listing.items,
            events,
        })
    }
}

pub(crate) fn new_leader_scheduler(
    db: DatastoreHandle,
    positioned_watch: klights_watch::PositionedWatchService,
    dispatcher: Arc<ControllerDispatcher>,
    supervisor: Arc<TaskSupervisor>,
) -> Arc<CronJobScheduler> {
    CronJobScheduler::new(
        Arc::new(LeaderCronJobSchedulerRuntime {
            positioned_watch,
            db,
            dispatcher,
        }),
        supervisor,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt as _;
    use serde_json::json;

    #[tokio::test]
    async fn positioned_watch_uses_exact_initial_snapshot_handoff() {
        let (db, db_handle) = crate::datastore::test_support::in_memory_with_handle().await;
        let passive_reads = crate::datastore::test_support::sqlite_passive_read_ports(&db);
        db.create_resource(
            "batch/v1",
            "CronJob",
            Some("default"),
            "first",
            json!({
                "apiVersion": "batch/v1",
                "kind": "CronJob",
                "metadata": {"name": "first", "namespace": "default"},
                "spec": {"schedule": "0 * * * *", "suspend": true}
            }),
        )
        .await
        .unwrap();
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let dispatcher = Arc::new(
            crate::controllers::ControllerDispatcher::with_task_supervisor(
                Arc::new(crate::controllers::service::ServiceIpam::new(
                    "10.43.128.0/17",
                )),
                supervisor,
            ),
        );
        let runtime = LeaderCronJobSchedulerRuntime {
            positioned_watch: crate::positioned_watch_adapter::for_test(
                &passive_reads,
                db_handle.clone(),
            ),
            db: db_handle,
            dispatcher,
        };

        let mut session = runtime.open_watch().await.unwrap();
        let accepted = session
            .events
            .accepted_cursor()
            .and_then(|cursor| cursor.replay_position())
            .expect("local positioned watch exposes the exact accepted event cursor");
        assert_eq!(session.initial_resources.len(), 1);
        assert_eq!(session.initial_resources[0].name, "first");

        db.create_resource(
            "batch/v1",
            "CronJob",
            Some("default"),
            "second",
            json!({
                "apiVersion": "batch/v1",
                "kind": "CronJob",
                "metadata": {"name": "second", "namespace": "default"},
                "spec": {"schedule": "0 * * * *", "suspend": true}
            }),
        )
        .await
        .unwrap();
        let delivered = session.events.next().await.unwrap().unwrap();
        let delivered_position = delivered
            .resume_position()
            .expect("delivered watch event carries the durable event cursor");
        assert!(accepted.permits_successor(delivered_position));
        assert_eq!(delivered.resource().name, "second");
    }
}
