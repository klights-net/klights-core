//! Worker-local leader-backed resource store capabilities.
//!
//! The worker store is a kubelet-owned composition of focused leader and
//! node-local ports.  It intentionally has no dependency on the root crate or
//! its cluster datastore traits.

use std::sync::atomic::AtomicI64;
use std::sync::{Arc, Mutex};

#[cfg(any(test, feature = "test-support"))]
use anyhow::Result;
use klights_leader_api::{LeaderWatch, WatchRequest};
#[cfg(any(test, feature = "test-support"))]
use klights_watch::WatchTransitionProjector;
use klights_watch::{WatchBus, WatchSignal, WatchTopic};

use crate::pod_lifecycle_router::PodLifecycleRouter;

pub mod cache;
pub mod ports;
pub mod reflector;
pub mod watch;

pub use cache::{WorkerListPage, WorkerResourceList};

use watch::WorkerWatchHistory;

/// Worker-local event source used by kubelet and node-discovery consumers.
pub trait WorkerWatchEvents: Send + Sync {
    fn subscribe_signals(&self, topic: WatchTopic) -> klights_watch::WatchSignalReceiver;
    #[cfg(any(test, feature = "test-support"))]
    fn subscribe(
        &self,
        topic: WatchTopic,
    ) -> tokio::sync::broadcast::Receiver<klights_watch::WatchEvent>;
    fn publish_signal(&self, signal: WatchSignal);
    #[cfg(any(test, feature = "test-support"))]
    fn publish(&self, event: klights_watch::WatchEvent);
}

/// In-memory event bus for worker-local mirrored events.
pub struct WorkerWatchBus {
    bus: WatchBus,
}

impl WorkerWatchBus {
    pub fn new() -> Self {
        Self {
            bus: WatchBus::new(1024),
        }
    }
}

impl Default for WorkerWatchBus {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerWatchEvents for WorkerWatchBus {
    fn subscribe_signals(&self, topic: WatchTopic) -> klights_watch::WatchSignalReceiver {
        self.bus.subscribe_signals(topic)
    }

    #[cfg(any(test, feature = "test-support"))]
    fn subscribe(
        &self,
        topic: WatchTopic,
    ) -> tokio::sync::broadcast::Receiver<klights_watch::WatchEvent> {
        self.bus.subscribe(topic)
    }

    fn publish_signal(&self, signal: WatchSignal) {
        self.bus.publish_signal(signal);
    }

    #[cfg(any(test, feature = "test-support"))]
    fn publish(&self, event: klights_watch::WatchEvent) {
        self.bus.publish(event);
    }
}

/// Focused worker-store inputs.  Construction accepts the individual
/// capabilities directly; no datastore or API-state umbrella crosses this
/// boundary.
pub struct WorkerStorePorts {
    pub resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    pub leader_watch: Arc<dyn LeaderWatch>,
    pub subnet_allocation: Arc<dyn klights_leader_api::LeaderNodeSubnetAllocation>,
    pub network_topology: Arc<dyn klights_leader_api::LeaderNetworkTopologyQuery>,
    pub cleanup_intents: Arc<dyn klights_leader_api::LeaderPodCleanupIntents>,
    pub watch_events: Arc<dyn WorkerWatchEvents>,
}

/// Worker-local mirror/cache facade used by kubelet bootstrap.
pub struct WorkerStoreAdapter {
    pub(crate) resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    pub(crate) leader_watch: Arc<dyn LeaderWatch>,
    pub(crate) subnet_allocation: Arc<dyn klights_leader_api::LeaderNodeSubnetAllocation>,
    pub(crate) network_topology: Arc<dyn klights_leader_api::LeaderNetworkTopologyQuery>,
    pub(crate) cleanup_intents: Arc<dyn klights_leader_api::LeaderPodCleanupIntents>,
    pub(crate) transition_projectors: Arc<dyn klights_watch::WatchTransitionProjectorFactory>,
    pub(crate) watch_events: Arc<dyn WorkerWatchEvents>,
    pub(crate) node_name: String,
    pub(crate) current_rv: AtomicI64,
    pub(crate) event_history: Mutex<WorkerWatchHistory>,
    pub(crate) next_event_id: AtomicI64,
    pub(crate) pod_lifecycle_router: Mutex<Option<Arc<PodLifecycleRouter>>>,
}

impl LeaderWatch for WorkerStoreAdapter {
    fn watch_resources(&self, request: WatchRequest) -> klights_leader_api::LeaderWatchFuture<'_> {
        self.leader_watch.watch_resources(request)
    }
}

impl WorkerStoreAdapter {
    /// Compose a worker store from focused leader capabilities.
    pub fn from_focused_ports(ports: WorkerStorePorts, node_name: String) -> Self {
        Self {
            resource_query: ports.resource_query,
            leader_watch: ports.leader_watch,
            subnet_allocation: ports.subnet_allocation,
            network_topology: ports.network_topology,
            cleanup_intents: ports.cleanup_intents,
            transition_projectors: Arc::new(klights_watch::SelectorWatchTransitionProjectors),
            watch_events: ports.watch_events,
            node_name,
            current_rv: AtomicI64::new(0),
            event_history: Mutex::new(WorkerWatchHistory::default()),
            next_event_id: AtomicI64::new(1),
            pod_lifecycle_router: Mutex::new(None),
        }
    }

    /// Test-only convenience construction from one client implementing the
    /// complete focused leader capability set.
    #[cfg(any(test, feature = "test-support"))]
    pub fn new<T>(cluster_api: Arc<T>, node_name: String) -> Self
    where
        T: klights_leader_api::LeaderResourceQuery
            + LeaderWatch
            + klights_leader_api::LeaderNodeSubnetAllocation
            + klights_leader_api::LeaderNetworkTopologyQuery
            + klights_leader_api::LeaderPodCleanupIntents
            + Send
            + Sync
            + 'static,
    {
        Self::from_focused_ports(
            WorkerStorePorts {
                resource_query: cluster_api.clone(),
                leader_watch: cluster_api.clone(),
                subnet_allocation: cluster_api.clone(),
                network_topology: cluster_api.clone(),
                cleanup_intents: cluster_api,
                watch_events: Arc::new(WorkerWatchBus::new()),
            },
            node_name,
        )
    }

    pub fn set_pod_lifecycle_router(&self, router: Arc<PodLifecycleRouter>) {
        *self
            .pod_lifecycle_router
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(router);
    }

    pub fn watch_signals(&self, topic: WatchTopic) -> klights_watch::WatchSignalReceiver {
        self.watch_events.subscribe_signals(topic)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn resource_query_for_test(&self) -> &dyn klights_leader_api::LeaderResourceQuery {
        self.resource_query.as_ref()
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn transition_projector_for_test(
        &self,
        request: &WatchRequest,
    ) -> Result<Box<dyn WatchTransitionProjector>> {
        self.transition_projectors
            .projector(request)
            .map_err(anyhow::Error::new)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn watch_topic(
        &self,
        topic: WatchTopic,
    ) -> tokio::sync::broadcast::Receiver<klights_watch::WatchEvent> {
        self.watch_events.subscribe(topic)
    }
}
