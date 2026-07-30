//! `RedbDatastore` — redb backend composed from focused domain stores.
//!
//! Production composes `SequencedDatastore` above this passive backend; `RedbDatastore` implements
//! `DatastoreBackend` by delegating to composed stores. Legacy local
//! `StorageCommand` apply support is test-only cleanup debt.

use std::sync::Arc;

#[cfg(test)]
use crate::datastore::CommitObservationSink;
use anyhow::{Result, anyhow};
use klights_supervisor::TaskSupervisor;

pub mod advance;
#[cfg(test)]
mod applier;
mod backend_impl;
pub mod network;
pub mod snapshot;
mod snapshot_capture;
pub mod watch;

pub mod crud {
    //! Resource and namespace CRUD stores.
    pub mod namespaces;
    pub mod resources;
}

#[cfg(test)]
mod tests;

use advance::RedbRvStore;
use crud::namespaces::RedbNamespaceStore;
use crud::resources::RedbResourceStore;
use klights_cluster_datastore::redb::RedbAccessor;
use klights_cluster_datastore::redb::RedbOpenOpts;
use klights_cluster_datastore::redb::RedbReadStore;
use klights_cluster_datastore::redb::live_committed_apply::RedbLiveCommittedApplyStore;
use klights_cluster_datastore::redb::recovery::RedbRecoveryStore;
use network::RedbNetworkStore;
use watch::RedbWatchStore;

/// Redb-backed datastore composed from focused domain stores.
///
/// Each store owns its data access logic and can be tested independently.
/// The `DatastoreBackend` impl delegates to these stores.
pub struct RedbDatastore {
    pub accessor: Arc<RedbAccessor>,
    #[cfg(test)]
    commit_sink: Arc<dyn CommitObservationSink>,
    resources: RedbResourceStore,
    namespaces: RedbNamespaceStore,
    watch_store: RedbWatchStore,
    network: RedbNetworkStore,
    live_committed_apply: RedbLiveCommittedApplyStore,
    recovery: RedbRecoveryStore,
    read_store: Arc<RedbReadStore>,
    rv_store: RedbRvStore,
    snapshot_sessions: Arc<tokio::sync::Semaphore>,
    wall_clock: Arc<dyn klights_supervisor::WallClock>,
}

impl Clone for RedbDatastore {
    fn clone(&self) -> Self {
        Self::from_accessor(
            self.accessor.clone(),
            #[cfg(test)]
            self.commit_sink.clone(),
            self.snapshot_sessions.clone(),
            self.wall_clock.clone(),
        )
    }
}

impl RedbDatastore {
    fn from_accessor(
        accessor: Arc<RedbAccessor>,
        #[cfg(test)] commit_sink: Arc<dyn CommitObservationSink>,
        snapshot_sessions: Arc<tokio::sync::Semaphore>,
        wall_clock: Arc<dyn klights_supervisor::WallClock>,
    ) -> Self {
        let resources = RedbResourceStore::new(accessor.clone(), wall_clock.clone());
        let namespaces = RedbNamespaceStore::new(accessor.clone());
        let watch_store = RedbWatchStore::new(accessor.clone());
        let network = RedbNetworkStore::new(accessor.clone());
        let live_committed_apply = RedbLiveCommittedApplyStore::new(accessor.clone());
        let recovery = RedbRecoveryStore::new(accessor.clone(), snapshot_sessions.clone());
        let read_store = Arc::new(RedbReadStore::new(accessor.clone()));
        Self {
            resources,
            namespaces,
            watch_store,
            network,
            live_committed_apply,
            recovery,
            read_store,
            rv_store: RedbRvStore::new(accessor.clone()),
            accessor,
            #[cfg(test)]
            commit_sink,
            snapshot_sessions,
            wall_clock,
        }
    }

    pub async fn new_persistent_with_sink(
        path: &std::path::Path,
        supervisor: Arc<TaskSupervisor>,
        #[cfg(test)] commit_sink: Arc<dyn CommitObservationSink>,
        wall_clock: Arc<dyn klights_supervisor::WallClock>,
    ) -> Result<Self> {
        let path = if path.extension().is_none() {
            path.join("redb").join("cluster.redb")
        } else {
            path.to_path_buf()
        };
        let db = klights_cluster_datastore::redb::open_persistent(
            supervisor.as_ref(),
            RedbOpenOpts {
                path,
                cache_size: 40 * 1024 * 1024,
            },
        )
        .await
        .map_err(|e| anyhow!("failed to open redb datastore: {e}"))?;
        let accessor = Arc::new(RedbAccessor::new(Arc::new(db), supervisor));
        Ok(Self::from_accessor(
            accessor,
            #[cfg(test)]
            commit_sink,
            Arc::new(tokio::sync::Semaphore::new(1)),
            wall_clock,
        ))
    }

    /// Production in-memory constructor with an explicit task supervisor.
    pub async fn new_in_memory_with_supervisor_and_sink(
        supervisor: Arc<TaskSupervisor>,
        #[cfg(test)] commit_sink: Arc<dyn CommitObservationSink>,
        wall_clock: Arc<dyn klights_supervisor::WallClock>,
    ) -> Result<Self> {
        let db = klights_cluster_datastore::redb::open_in_memory(supervisor.as_ref()).await?;
        let accessor = Arc::new(RedbAccessor::new(Arc::new(db), supervisor));
        Ok(Self::from_accessor(
            accessor,
            #[cfg(test)]
            commit_sink,
            Arc::new(tokio::sync::Semaphore::new(1)),
            wall_clock,
        ))
    }

    #[cfg(test)]
    pub async fn new_persistent(
        path: &std::path::Path,
        supervisor: Arc<TaskSupervisor>,
    ) -> Result<Self> {
        Self::new_persistent_with_sink(
            path,
            supervisor,
            crate::watch_commit_observation_adapter::new_sink(),
            Arc::new(klights_supervisor::SystemWallClock),
        )
        .await
    }

    #[cfg(test)]
    pub async fn new_in_memory_with_supervisor(supervisor: Arc<TaskSupervisor>) -> Result<Self> {
        Self::new_in_memory_with_supervisor_and_sink(
            supervisor,
            crate::watch_commit_observation_adapter::new_sink(),
            Arc::new(klights_supervisor::SystemWallClock),
        )
        .await
    }

    #[cfg(test)]
    pub async fn new_in_memory() -> Result<Self> {
        let supervisor = Arc::new(TaskSupervisor::new(Default::default()));
        let db = klights_cluster_datastore::redb::open_in_memory(supervisor.as_ref()).await?;
        let accessor = Arc::new(RedbAccessor::new(Arc::new(db), supervisor));
        Ok(Self::from_accessor(
            accessor,
            crate::watch_commit_observation_adapter::new_sink(),
            Arc::new(tokio::sync::Semaphore::new(1)),
            Arc::new(klights_supervisor::SystemWallClock),
        ))
    }

    fn finish_post_commit<T>(
        &self,
        (result, pending): (T, Option<klights_cluster_store::StagedPostCommit>),
    ) -> T {
        #[cfg(not(test))]
        let _ = pending;
        #[cfg(test)]
        if let Some(pending) = pending {
            crate::datastore::sqlite::publish_pending(pending, self.commit_sink.as_ref());
        }
        result
    }

    pub(crate) fn focused_read_store(&self) -> Arc<RedbReadStore> {
        self.read_store.clone()
    }

    #[cfg(test)]
    pub(crate) fn passive_read_ports_for_test(
        &self,
    ) -> crate::datastore::selector::PassiveReadPorts {
        let focused_reads = self.focused_read_store();
        crate::datastore::selector::PassiveReadPorts::new(
            focused_reads.clone(),
            focused_reads.clone(),
            focused_reads,
        )
    }

    pub(crate) fn focused_committed_apply(
        &self,
    ) -> Arc<dyn klights_cluster_store::PrivilegedCommittedRaftApply> {
        Arc::new(self.live_committed_apply.clone())
    }
}
