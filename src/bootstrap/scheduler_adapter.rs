use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::controllers::scheduler::SchedulerRuntime;
use crate::datastore::DatastoreHandle;
use klights_leader_api::{LeaderWatch, LeaderWatchError, WatchRequest, WatchStream};

pub(crate) struct LeaderSchedulerRuntime {
    db: DatastoreHandle,
    pods: Arc<crate::pod_api_service::PodApiService>,
}

impl LeaderSchedulerRuntime {
    pub(crate) fn new(
        db: DatastoreHandle,
        pods: Arc<crate::pod_api_service::PodApiService>,
    ) -> Self {
        Self { db, pods }
    }
}

#[async_trait]
impl SchedulerRuntime for LeaderSchedulerRuntime {
    async fn open_watch_sessions(&self) -> std::result::Result<Vec<WatchStream>, LeaderWatchError> {
        let positioned = crate::control_plane::client::local::datastore_positioned_watch_service(
            self.db.clone(),
        );
        let mut sessions = Vec::with_capacity(2);
        for (api_version, kind) in [("v1", "Pod"), ("v1", "Node")] {
            let request = WatchRequest::try_new(api_version, kind, None, None, None, None, None)?;
            sessions.push(positioned.watch_resources(request).await?);
        }
        Ok(sessions)
    }

    async fn schedule_all_unbound_pods(&self) -> Result<()> {
        self.pods
            .schedule_all_unbound_pods()
            .await
            .map_err(|error| anyhow::anyhow!("{error:?}"))
    }
}
