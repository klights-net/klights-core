//! `RedbDatastore` — redb backend composed from focused domain stores.
//!
//! Production uses `ReplicatedDatastore`; `RedbDatastore` implements
//! `DatastoreBackend` by delegating to composed stores. Legacy local
//! `StorageCommand` apply support is test-only cleanup debt.

use std::sync::Arc;

use anyhow::{Result, anyhow};
use tokio::sync::broadcast;

use crate::datastore::types::*;
use crate::task_supervisor::TaskSupervisor;
use crate::watch::WatchBus;

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
pub mod pod_slot;
pub mod sandbox;
pub mod snapshot;
pub mod tables;
pub mod watch;
pub mod workqueue;

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
use pod_slot::RedbPodSlotStore;
use sandbox::RedbSandboxStore;
use watch::RedbWatchStore;
use workqueue::RedbWorkqueueStore;

const POD_ENDPOINT_CHANNEL_BOUND: usize = 4_096;
const POD_SLOT_ADMISSION_CHANNEL_BOUND: usize = 4_096;

/// Redb-backed datastore composed from focused domain stores.
///
/// Each store owns its data access logic and can be tested independently.
/// The `DatastoreBackend` impl delegates to these stores.
pub struct RedbDatastore {
    pub accessor: Arc<RedbAccessor>,
    watch_bus: Arc<WatchBus>,
    resources: RedbResourceStore,
    namespaces: RedbNamespaceStore,
    watch_store: RedbWatchStore,
    pod_slots: RedbPodSlotStore,
    sandboxes: RedbSandboxStore,
    network: RedbNetworkStore,
    workqueue: RedbWorkqueueStore,
    rv_store: RedbRvStore,
    pod_endpoint_tx: broadcast::Sender<PodEndpointEvent>,
    pod_slot_admission_tx: broadcast::Sender<PodSlotAdmissionEvent>,
}

impl Clone for RedbDatastore {
    fn clone(&self) -> Self {
        Self::from_accessor(
            self.accessor.clone(),
            self.watch_bus.clone(),
            self.pod_endpoint_tx.clone(),
            self.pod_slot_admission_tx.clone(),
        )
    }
}

impl RedbDatastore {
    fn from_accessor(
        accessor: Arc<RedbAccessor>,
        watch_bus: Arc<WatchBus>,
        pod_endpoint_tx: broadcast::Sender<PodEndpointEvent>,
        pod_slot_admission_tx: broadcast::Sender<PodSlotAdmissionEvent>,
    ) -> Self {
        Self {
            resources: RedbResourceStore::new(accessor.clone(), watch_bus.clone()),
            namespaces: RedbNamespaceStore::new(accessor.clone(), watch_bus.clone()),
            watch_store: RedbWatchStore::new(accessor.clone()),
            pod_slots: RedbPodSlotStore::new(accessor.clone(), pod_slot_admission_tx.clone()),
            sandboxes: RedbSandboxStore::new(accessor.clone()),
            network: RedbNetworkStore::new(accessor.clone(), pod_endpoint_tx.clone()),
            workqueue: RedbWorkqueueStore::new(accessor.clone()),
            rv_store: RedbRvStore::new(accessor.clone()),
            accessor,
            watch_bus,
            pod_endpoint_tx,
            pod_slot_admission_tx,
        }
    }

    pub async fn new_persistent(
        path: &std::path::Path,
        supervisor: Arc<TaskSupervisor>,
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
        let (pod_endpoint_tx, _) = broadcast::channel(POD_ENDPOINT_CHANNEL_BOUND);
        let (pod_slot_admission_tx, _) = broadcast::channel(POD_SLOT_ADMISSION_CHANNEL_BOUND);
        let accessor = Arc::new(RedbAccessor::new(Arc::new(db), supervisor));
        Ok(Self::from_accessor(
            accessor,
            Arc::new(WatchBus::new(1024)),
            pod_endpoint_tx,
            pod_slot_admission_tx,
        ))
    }

    /// Production in-memory constructor with an explicit task supervisor.
    pub async fn new_in_memory_with_supervisor(supervisor: Arc<TaskSupervisor>) -> Result<Self> {
        let db = open_boundary::open_in_memory(supervisor.as_ref()).await?;
        let (pod_endpoint_tx, _) = broadcast::channel(POD_ENDPOINT_CHANNEL_BOUND);
        let (pod_slot_admission_tx, _) = broadcast::channel(POD_SLOT_ADMISSION_CHANNEL_BOUND);
        let accessor = Arc::new(RedbAccessor::new(Arc::new(db), supervisor));
        Ok(Self::from_accessor(
            accessor,
            Arc::new(WatchBus::new(1024)),
            pod_endpoint_tx,
            pod_slot_admission_tx,
        ))
    }

    #[cfg(test)]
    pub async fn new_in_memory() -> Result<Self> {
        let db = open_boundary::open_in_memory_blocking()?;
        let (pod_endpoint_tx, _) = broadcast::channel(POD_ENDPOINT_CHANNEL_BOUND);
        let (pod_slot_admission_tx, _) = broadcast::channel(POD_SLOT_ADMISSION_CHANNEL_BOUND);
        let supervisor = Arc::new(TaskSupervisor::new(Default::default()));
        let accessor = Arc::new(RedbAccessor::new(Arc::new(db), supervisor));
        Ok(Self::from_accessor(
            accessor,
            Arc::new(WatchBus::new(1024)),
            pod_endpoint_tx,
            pod_slot_admission_tx,
        ))
    }
}
