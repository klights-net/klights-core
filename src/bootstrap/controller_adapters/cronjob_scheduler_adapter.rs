use std::sync::Arc;

use async_trait::async_trait;

use klights_controllers::ControllerDispatcher;
use klights_controllers::cronjob_scheduler::{
    CronJobScheduler, CronJobSchedulerRuntime, CronJobSchedulerRuntimeError, CronJobWatchSession,
};
use klights_leader_api::{LeaderWatch, LeaderWatchError, WatchRequest};
use klights_supervisor::TaskSupervisor;

struct LeaderCronJobSchedulerRuntime {
    resource_reads: Arc<dyn klights_cluster_store::ClusterResourceRead>,
    store: Arc<dyn klights_controllers::cronjob::CronJobStore>,
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
        let listing = self
            .resource_reads
            .list_resources(klights_cluster_store::ResourceListRequest::new(
                "batch/v1",
                "CronJob",
                klights_cluster_store::ResourceCollectionScope::AllNamespaces,
                klights_cluster_store::ResourceListQuery::all(),
            ))
            .await
            .map_err(|error| CronJobSchedulerRuntimeError::query_unavailable(error.to_string()))?;
        match listing {
            klights_cluster_store::ResourceListRead::Current(page)
            | klights_cluster_store::ResourceListRead::Historical(page) => Ok(page.into_items()),
            klights_cluster_store::ResourceListRead::Expired {
                requested,
                oldest_available,
                ..
            } => Err(CronJobSchedulerRuntimeError::query_unavailable(format!(
                "CronJob LIST at resourceVersion {requested} expired before {oldest_available}"
            ))),
        }
    }

    async fn reconcile_cronjob(
        &self,
        resource: &klights_cluster_core::Resource,
    ) -> std::result::Result<(), CronJobSchedulerRuntimeError> {
        klights_leader_api::validate_controller_lease_if_scoped()
            .map_err(|error| CronJobSchedulerRuntimeError::reconcile_failed(error.to_string()))?;
        klights_controllers::cronjob::reconcile_cronjob_one_at(
            self.store.as_ref(),
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
            .resource_reads
            .list_resources(klights_cluster_store::ResourceListRequest::new(
                "batch/v1",
                "CronJob",
                klights_cluster_store::ResourceCollectionScope::AllNamespaces,
                klights_cluster_store::ResourceListQuery::all(),
            ))
            .await
            .map_err(|error| LeaderWatchError::unavailable(error.to_string()))?;
        let page = match listing {
            klights_cluster_store::ResourceListRead::Current(page)
            | klights_cluster_store::ResourceListRead::Historical(page) => page,
            klights_cluster_store::ResourceListRead::Expired {
                requested,
                oldest_available,
                ..
            } => {
                return Err(LeaderWatchError::unavailable(format!(
                    "CronJob LIST at resourceVersion {requested} expired before {oldest_available}"
                )));
            }
        };
        let request = WatchRequest::try_new(
            "batch/v1",
            "CronJob",
            None,
            None,
            None,
            Some(page.snapshot().resource_version()),
            Some(page.snapshot().position()),
        )?;
        let events = self.positioned_watch.watch_resources(request).await?;
        Ok(CronJobWatchSession {
            initial_resources: page.into_items(),
            events,
        })
    }
}

pub(crate) fn new_leader_scheduler(
    resource_reads: Arc<dyn klights_cluster_store::ClusterResourceRead>,
    store: Arc<dyn klights_controllers::cronjob::CronJobStore>,
    positioned_watch: klights_watch::PositionedWatchService,
    dispatcher: Arc<ControllerDispatcher>,
    supervisor: Arc<TaskSupervisor>,
) -> Arc<CronJobScheduler> {
    CronJobScheduler::new(
        Arc::new(LeaderCronJobSchedulerRuntime {
            positioned_watch,
            resource_reads,
            store,
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
        let db = klights_cluster_datastore::sqlite::embedded::Datastore::new_in_memory()
            .await
            .unwrap();
        let passive_reads =
            crate::bootstrap::cluster_store::selector::sqlite_passive_read_ports(&db);
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
        let dispatcher =
            crate::bootstrap::controller_adapters::controller_runtime_adapter::dispatcher_for_test(
                &db,
                Arc::new(klights_controllers::service::ServiceIpam::new(
                    "10.43.128.0/17",
                )),
            );
        let ports = crate::bootstrap::cluster_store::selector::sqlite_opened_passive_store(&db);
        let runtime = LeaderCronJobSchedulerRuntime {
            positioned_watch:
                crate::bootstrap::composition_adapters::positioned_watch_adapter::for_test(
                    &passive_reads,
                    &db,
                ),
            resource_reads: passive_reads.resource_reads(),
            store: Arc::new(
                crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort::new_for_test(
                    ports.applied_outbox,
                    ports.committed_apply,
                    ports.read_ports.resource_reads(),
                    ports.ownership_reads,
                ),
            ),
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
