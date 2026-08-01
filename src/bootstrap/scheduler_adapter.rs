use std::sync::Arc;

use async_trait::async_trait;
use klights_reconcile_api::{ControllerStoreError, ControllerStoreResult};

use klights_controllers::scheduler::SchedulerRuntime;
use klights_leader_api::{LeaderWatch, LeaderWatchError, WatchRequest, WatchStream};

pub(crate) struct LeaderSchedulerRuntime {
    pods: Arc<dyn klights_pod_api::PodScheduling>,
    positioned_watch: klights_watch::PositionedWatchService,
}

impl LeaderSchedulerRuntime {
    pub(crate) fn new(
        positioned_watch: klights_watch::PositionedWatchService,
        pods: Arc<dyn klights_pod_api::PodScheduling>,
    ) -> Self {
        Self {
            positioned_watch,
            pods,
        }
    }
}

#[async_trait]
impl SchedulerRuntime for LeaderSchedulerRuntime {
    async fn open_watch_sessions(&self) -> std::result::Result<Vec<WatchStream>, LeaderWatchError> {
        let mut sessions = Vec::with_capacity(2);
        for (api_version, kind) in [("v1", "Pod"), ("v1", "Node")] {
            let request = WatchRequest::try_new(api_version, kind, None, None, None, None, None)?;
            sessions.push(self.positioned_watch.watch_resources(request).await?);
        }
        Ok(sessions)
    }

    async fn schedule_all_unbound_pods(&self) -> ControllerStoreResult<()> {
        klights_leader_api::validate_controller_lease_if_scoped().map_err(|error| {
            ControllerStoreError::unavailable(format!(
                "controller authority rejected effect: {error}"
            ))
        })?;
        self.pods
            .schedule_all_unbound_pods()
            .await
            .map_err(|error| {
                ControllerStoreError::unavailable(format!("schedule unbound Pods failed: {error}"))
            })
    }
}
