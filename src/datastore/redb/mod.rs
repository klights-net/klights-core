//! `RedbDatastore` — redb backend composed from focused domain stores.
//!
//! Production composes `SequencedDatastore` above this passive backend; `RedbDatastore` implements
//! `DatastoreBackend` by delegating to composed stores. Legacy local
//! `StorageCommand` apply support is test-only cleanup debt.

use std::sync::Arc;

use crate::datastore::CommitObservationSink;
use anyhow::{Result, anyhow};
use klights_supervisor::TaskSupervisor;

pub mod accessor;
pub mod advance;
#[cfg(test)]
mod applier;
mod backend_impl;
mod helpers;
pub mod key_codec;
pub mod meta;
pub mod network;
pub mod open_boundary;
pub mod opener;
mod position_membership;
mod replay_floor;
pub mod snapshot;
mod snapshot_capture;
pub mod tables;
pub mod watch;

pub mod crud {
    //! Resource and namespace CRUD stores.
    pub mod namespaces;
    pub mod resources;
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub use open_boundary::open_persistent_blocking as open;
pub use opener::RedbOpenOpts;

use accessor::RedbAccessor;
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
    commit_sink: Arc<dyn CommitObservationSink>,
    resources: RedbResourceStore,
    namespaces: RedbNamespaceStore,
    watch_store: RedbWatchStore,
    network: RedbNetworkStore,
    rv_store: RedbRvStore,
    snapshot_sessions: Arc<tokio::sync::Semaphore>,
}

impl Clone for RedbDatastore {
    fn clone(&self) -> Self {
        Self::from_accessor(
            self.accessor.clone(),
            self.commit_sink.clone(),
            self.snapshot_sessions.clone(),
        )
    }
}

impl RedbDatastore {
    fn from_accessor(
        accessor: Arc<RedbAccessor>,
        commit_sink: Arc<dyn CommitObservationSink>,
        snapshot_sessions: Arc<tokio::sync::Semaphore>,
    ) -> Self {
        Self {
            resources: RedbResourceStore::new(accessor.clone(), commit_sink.clone()),
            namespaces: RedbNamespaceStore::new(accessor.clone(), commit_sink.clone()),
            watch_store: RedbWatchStore::new(accessor.clone()),
            network: RedbNetworkStore::new(accessor.clone()),
            rv_store: RedbRvStore::new(accessor.clone()),
            accessor,
            commit_sink,
            snapshot_sessions,
        }
    }

    pub async fn new_persistent_with_sink(
        path: &std::path::Path,
        supervisor: Arc<TaskSupervisor>,
        commit_sink: Arc<dyn CommitObservationSink>,
    ) -> Result<Self> {
        let path = if path.extension().is_none() {
            path.join("redb").join("cluster.redb")
        } else {
            path.to_path_buf()
        };
        let db = open_boundary::open_persistent(
            supervisor.as_ref(),
            opener::RedbOpenOpts {
                path,
                cache_size: 40 * 1024 * 1024,
            },
        )
        .await
        .map_err(|e| anyhow!("failed to open redb datastore: {e}"))?;
        let accessor = Arc::new(RedbAccessor::new(Arc::new(db), supervisor));
        Ok(Self::from_accessor(
            accessor,
            commit_sink,
            Arc::new(tokio::sync::Semaphore::new(1)),
        ))
    }

    /// Production in-memory constructor with an explicit task supervisor.
    pub async fn new_in_memory_with_supervisor_and_sink(
        supervisor: Arc<TaskSupervisor>,
        commit_sink: Arc<dyn CommitObservationSink>,
    ) -> Result<Self> {
        let db = open_boundary::open_in_memory(supervisor.as_ref()).await?;
        let accessor = Arc::new(RedbAccessor::new(Arc::new(db), supervisor));
        Ok(Self::from_accessor(
            accessor,
            commit_sink,
            Arc::new(tokio::sync::Semaphore::new(1)),
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
        )
        .await
    }

    #[cfg(test)]
    pub async fn new_in_memory_with_supervisor(supervisor: Arc<TaskSupervisor>) -> Result<Self> {
        Self::new_in_memory_with_supervisor_and_sink(
            supervisor,
            crate::watch_commit_observation_adapter::new_sink(),
        )
        .await
    }

    #[cfg(test)]
    pub async fn new_in_memory() -> Result<Self> {
        let db = open_boundary::open_in_memory_blocking()?;
        let supervisor = Arc::new(TaskSupervisor::new(Default::default()));
        let accessor = Arc::new(RedbAccessor::new(Arc::new(db), supervisor));
        Ok(Self::from_accessor(
            accessor,
            crate::watch_commit_observation_adapter::new_sink(),
            Arc::new(tokio::sync::Semaphore::new(1)),
        ))
    }
}
