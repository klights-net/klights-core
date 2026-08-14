//! Root composition adapters for the passive Redb cluster datastore.

use anyhow::Result;
use klights_cluster_datastore::redb::embedded::RedbDatastore as PassiveRedbDatastore;
use klights_supervisor::TaskSupervisor;
use std::sync::Arc;

mod backend_impl;
mod snapshot;

/// Root composition identity around the destination-owned Redb store.
#[derive(Clone)]
pub struct RedbDatastore(PassiveRedbDatastore);

impl std::ops::Deref for RedbDatastore {
    type Target = PassiveRedbDatastore;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl RedbDatastore {
    pub async fn new_persistent_with_sink(
        path: &std::path::Path,
        supervisor: Arc<TaskSupervisor>,
        #[cfg(test)] commit_sink: Arc<dyn crate::datastore::CommitObservationSink>,
        wall_clock: Arc<dyn klights_supervisor::WallClock>,
    ) -> Result<Self> {
        #[cfg(test)]
        let passive = PassiveRedbDatastore::new_persistent_with_sink(
            path,
            supervisor,
            commit_sink,
            wall_clock,
        )
        .await?;
        #[cfg(not(test))]
        let passive = PassiveRedbDatastore::new_persistent(path, supervisor, wall_clock).await?;
        Ok(Self(passive))
    }

    pub async fn new_in_memory_with_supervisor_and_sink(
        supervisor: Arc<TaskSupervisor>,
        #[cfg(test)] commit_sink: Arc<dyn crate::datastore::CommitObservationSink>,
        wall_clock: Arc<dyn klights_supervisor::WallClock>,
    ) -> Result<Self> {
        #[cfg(test)]
        let passive = PassiveRedbDatastore::new_in_memory_with_supervisor_and_sink(
            supervisor,
            commit_sink,
            wall_clock,
        )
        .await?;
        #[cfg(not(test))]
        let passive =
            PassiveRedbDatastore::new_in_memory_with_supervisor(supervisor, wall_clock).await?;
        Ok(Self(passive))
    }

    #[cfg(test)]
    pub async fn new_in_memory() -> Result<Self> {
        Self::new_in_memory_with_supervisor_and_sink(
            Arc::new(TaskSupervisor::new(Default::default())),
            crate::bootstrap::watch_commit_wiring::new_sink(),
            Arc::new(klights_supervisor::SystemWallClock),
        )
        .await
    }
}
