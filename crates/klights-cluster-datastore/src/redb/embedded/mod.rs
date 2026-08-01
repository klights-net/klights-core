//! `RedbDatastore` — redb backend composed from focused domain stores.
//!
//! Production composes `SequencedDatastore` above this passive backend; `RedbDatastore` implements
//! `DatastoreBackend` by delegating to composed stores. Legacy local
//! `StorageCommand` apply support is test-only cleanup debt.

use std::sync::Arc;

mod backend_impl;

use anyhow::{Result, anyhow};
#[cfg(any(test, feature = "test-support"))]
use klights_cluster_store::CommitObservationSink;
use klights_supervisor::TaskSupervisor;

pub mod advance;
#[cfg(test)]
mod applier;
pub mod network;
pub mod snapshot;
mod snapshot_capture;
pub mod watch;

pub mod crud {
    //! Resource and namespace CRUD stores.
    pub mod namespaces;
    pub mod resources;
}

use crate::redb::RedbAccessor;
use crate::redb::RedbOpenOpts;
use crate::redb::RedbReadStore;
use crate::redb::live_committed_apply::RedbLiveCommittedApplyStore;
use crate::redb::recovery::RedbRecoveryStore;
use advance::RedbRvStore;
use crud::namespaces::RedbNamespaceStore;
use crud::resources::RedbResourceStore;
use network::RedbNetworkStore;
use watch::RedbWatchStore;

/// Redb-backed datastore composed from focused domain stores.
///
/// Each store owns its data access logic and can be tested independently.
/// The `DatastoreBackend` impl delegates to these stores.
pub struct RedbDatastore {
    pub accessor: Arc<RedbAccessor>,
    #[cfg(any(test, feature = "test-support"))]
    commit_sink: Option<Arc<dyn CommitObservationSink>>,
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
            #[cfg(any(test, feature = "test-support"))]
            self.commit_sink.clone(),
            self.snapshot_sessions.clone(),
            self.wall_clock.clone(),
        )
    }
}

impl RedbDatastore {
    fn from_accessor(
        accessor: Arc<RedbAccessor>,
        #[cfg(any(test, feature = "test-support"))] commit_sink: Option<
            Arc<dyn CommitObservationSink>,
        >,
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
            #[cfg(any(test, feature = "test-support"))]
            commit_sink,
            snapshot_sessions,
            wall_clock,
        }
    }

    async fn new_persistent_inner(
        path: &std::path::Path,
        supervisor: Arc<TaskSupervisor>,
        #[cfg(any(test, feature = "test-support"))] commit_sink: Option<
            Arc<dyn CommitObservationSink>,
        >,
        wall_clock: Arc<dyn klights_supervisor::WallClock>,
    ) -> Result<Self> {
        let path = if path.extension().is_none() {
            path.join("redb").join("cluster.redb")
        } else {
            path.to_path_buf()
        };
        let db = crate::redb::open_persistent(
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
            #[cfg(any(test, feature = "test-support"))]
            commit_sink,
            Arc::new(tokio::sync::Semaphore::new(1)),
            wall_clock,
        ))
    }

    async fn new_in_memory_with_supervisor_inner(
        supervisor: Arc<TaskSupervisor>,
        #[cfg(any(test, feature = "test-support"))] commit_sink: Option<
            Arc<dyn CommitObservationSink>,
        >,
        wall_clock: Arc<dyn klights_supervisor::WallClock>,
    ) -> Result<Self> {
        let db = crate::redb::open_in_memory(supervisor.as_ref()).await?;
        let accessor = Arc::new(RedbAccessor::new(Arc::new(db), supervisor));
        Ok(Self::from_accessor(
            accessor,
            #[cfg(any(test, feature = "test-support"))]
            commit_sink,
            Arc::new(tokio::sync::Semaphore::new(1)),
            wall_clock,
        ))
    }

    /// Open a persistent datastore without installing test observation hooks.
    pub async fn new_persistent(
        path: &std::path::Path,
        supervisor: Arc<TaskSupervisor>,
        wall_clock: Arc<dyn klights_supervisor::WallClock>,
    ) -> Result<Self> {
        Self::new_persistent_inner(
            path,
            supervisor,
            #[cfg(any(test, feature = "test-support"))]
            None,
            wall_clock,
        )
        .await
    }

    /// Open an in-memory datastore without installing test observation hooks.
    pub async fn new_in_memory_with_supervisor(
        supervisor: Arc<TaskSupervisor>,
        wall_clock: Arc<dyn klights_supervisor::WallClock>,
    ) -> Result<Self> {
        Self::new_in_memory_with_supervisor_inner(
            supervisor,
            #[cfg(any(test, feature = "test-support"))]
            None,
            wall_clock,
        )
        .await
    }

    /// Open a persistent datastore with a test commit-observation hook.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn new_persistent_with_sink(
        path: &std::path::Path,
        supervisor: Arc<TaskSupervisor>,
        commit_sink: Arc<dyn CommitObservationSink>,
        wall_clock: Arc<dyn klights_supervisor::WallClock>,
    ) -> Result<Self> {
        Self::new_persistent_inner(path, supervisor, Some(commit_sink), wall_clock).await
    }

    /// Open an in-memory datastore with a test commit-observation hook.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn new_in_memory_with_supervisor_and_sink(
        supervisor: Arc<TaskSupervisor>,
        commit_sink: Arc<dyn CommitObservationSink>,
        wall_clock: Arc<dyn klights_supervisor::WallClock>,
    ) -> Result<Self> {
        Self::new_in_memory_with_supervisor_inner(supervisor, Some(commit_sink), wall_clock).await
    }

    #[cfg(test)]
    pub async fn new_in_memory() -> Result<Self> {
        let supervisor = Arc::new(TaskSupervisor::new(Default::default()));
        let db = crate::redb::open_in_memory(supervisor.as_ref()).await?;
        let accessor = Arc::new(RedbAccessor::new(Arc::new(db), supervisor));
        Ok(Self::from_accessor(
            accessor,
            Some(crate::test_fixtures::commit_observation::new_sink()),
            Arc::new(tokio::sync::Semaphore::new(1)),
            Arc::new(klights_supervisor::SystemWallClock),
        ))
    }

    fn finish_post_commit<T>(
        &self,
        (result, pending): (T, Option<klights_cluster_store::StagedPostCommit>),
    ) -> T {
        #[cfg(not(any(test, feature = "test-support")))]
        let _ = pending;
        #[cfg(any(test, feature = "test-support"))]
        if let Some(pending) = pending
            && let Some(commit_sink) = self.commit_sink.as_deref()
        {
            crate::sqlite::embedded::publish_pending(pending, commit_sink);
        }
        result
    }

    pub fn focused_read_store(&self) -> Arc<RedbReadStore> {
        self.read_store.clone()
    }

    pub fn focused_committed_apply(
        &self,
    ) -> Arc<dyn klights_cluster_store::PrivilegedCommittedRaftApply> {
        Arc::new(self.live_committed_apply.clone())
    }

    pub fn focused_recovery_store(&self) -> Arc<RedbRecoveryStore> {
        Arc::new(self.recovery.clone())
    }
}
