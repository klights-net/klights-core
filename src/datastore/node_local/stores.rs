//! Root composition for focused node-local persistence ports.
//!
//! This bundle never crosses into feature implementations. Bootstrap selects
//! the concrete node database once, then hands each consumer only the focused
//! `klights-node-store` capability it needs.

use std::sync::Arc;

use anyhow::Result;
use klights_node_datastore::{
    SqliteNodeIdentity, SqliteNodeNetworkStateStore, SqliteRaftDurability, SqliteRuntimeWorkStore,
    delivery::SqliteDeliveryStore,
};
use klights_node_store::{
    DeadLetterStore, NodeIdentity, OutboxDispatcherStore, OutboxProducerStore,
    OutboxStatusStampStore, PodEndpointStore, PodEndpointStoreEventSource, PodIpamStore,
    PodNetworkCache, PodRuntimeStore, PodSlotAdmissionEventSource, PodSlotAdmissionStore,
    PodStatusCheckpointStore, PodWorkqueueStore, RaftAppliedStatePersistence, RaftLogPersistence,
    RuntimeObservationCheckpointStore,
};
use klights_supervisor::{DbExecutor, WallClock};

#[derive(Clone)]
pub(crate) struct NodeLocalStores {
    identity: Arc<SqliteNodeIdentity>,
    raft_persistence: Arc<SqliteRaftDurability>,
    delivery: Arc<SqliteDeliveryStore>,
    network: Arc<SqliteNodeNetworkStateStore>,
    runtime_work: Arc<SqliteRuntimeWorkStore>,
    #[cfg(test)]
    executor: DbExecutor,
}

impl NodeLocalStores {
    pub(crate) fn from_executor_with_clock(
        executor: DbExecutor,
        wall_clock: Arc<dyn WallClock>,
    ) -> Result<Self> {
        Ok(Self {
            identity: Arc::new(SqliteNodeIdentity::new(executor.clone())),
            raft_persistence: Arc::new(SqliteRaftDurability::new(executor.clone())),
            delivery: Arc::new(SqliteDeliveryStore::new(
                executor.clone(),
                wall_clock.clone(),
            )),
            network: Arc::new(SqliteNodeNetworkStateStore::new(
                executor.clone(),
                wall_clock.clone(),
            )),
            runtime_work: Arc::new(SqliteRuntimeWorkStore::new(executor.clone(), wall_clock)),
            #[cfg(test)]
            executor,
        })
    }

    #[cfg(test)]
    pub(crate) fn from_executor(executor: DbExecutor) -> Result<Self> {
        Self::from_executor_with_clock(executor, Arc::new(klights_supervisor::SystemWallClock))
    }

    pub(crate) fn identity(&self) -> Arc<dyn NodeIdentity> {
        self.identity.clone()
    }

    pub(crate) fn raft_log_persistence(&self) -> Arc<dyn RaftLogPersistence> {
        self.raft_persistence.clone()
    }

    pub(crate) fn raft_applied_state_persistence(&self) -> Arc<dyn RaftAppliedStatePersistence> {
        self.raft_persistence.clone()
    }

    pub(crate) fn outbox_producer(&self) -> Arc<dyn OutboxProducerStore> {
        self.delivery.clone()
    }

    pub(crate) fn outbox_dispatcher(&self) -> Arc<dyn OutboxDispatcherStore> {
        self.delivery.clone()
    }

    pub(crate) fn outbox_status_stamps(&self) -> Arc<dyn OutboxStatusStampStore> {
        self.delivery.clone()
    }

    pub(crate) fn dead_letters(&self) -> Arc<dyn DeadLetterStore> {
        self.delivery.clone()
    }

    pub(crate) fn pod_status_checkpoints(&self) -> Arc<dyn PodStatusCheckpointStore> {
        self.delivery.clone()
    }

    pub(crate) fn runtime_observation_checkpoints(
        &self,
    ) -> Arc<dyn RuntimeObservationCheckpointStore> {
        self.delivery.clone()
    }

    pub(crate) fn pod_network_cache(&self) -> Arc<dyn PodNetworkCache> {
        self.network.clone()
    }

    pub(crate) fn pod_ipam(&self) -> Arc<dyn PodIpamStore> {
        self.network.clone()
    }

    pub(crate) fn pod_endpoints(&self) -> Arc<dyn PodEndpointStore> {
        self.network.clone()
    }

    pub(crate) fn pod_endpoint_events(&self) -> Arc<dyn PodEndpointStoreEventSource> {
        self.network.clone()
    }

    pub(crate) fn pod_runtime(&self) -> Arc<dyn PodRuntimeStore> {
        self.runtime_work.clone()
    }

    pub(crate) fn pod_workqueue(&self) -> Arc<dyn PodWorkqueueStore> {
        self.runtime_work.clone()
    }

    pub(crate) fn pod_slots(&self) -> Arc<dyn PodSlotAdmissionStore> {
        self.runtime_work.clone()
    }

    pub(crate) fn pod_slot_events(&self) -> Arc<dyn PodSlotAdmissionEventSource> {
        self.runtime_work.clone()
    }

    #[cfg(test)]
    pub(crate) fn identity_ref(&self) -> &SqliteNodeIdentity {
        &self.identity
    }

    #[cfg(test)]
    pub(crate) fn delivery_ref(&self) -> &SqliteDeliveryStore {
        &self.delivery
    }

    #[cfg(test)]
    pub(crate) fn network_state_ref(&self) -> &SqliteNodeNetworkStateStore {
        &self.network
    }

    #[cfg(test)]
    pub(crate) fn runtime_work_ref(&self) -> &SqliteRuntimeWorkStore {
        &self.runtime_work
    }

    #[cfg(test)]
    pub(crate) fn raft_persistence_ref(&self) -> &SqliteRaftDurability {
        &self.raft_persistence
    }

    #[cfg(test)]
    pub(crate) fn executor_for_test(&self) -> DbExecutor {
        self.executor.clone()
    }
}
