//! Worker-local leader-backed resource store capabilities.
//!
//! The worker store is a kubelet-owned composition of focused leader and
//! node-local ports.  It intentionally has no dependency on the root crate or
//! its cluster datastore traits.

use std::collections::HashMap;
use std::sync::atomic::AtomicI64;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use klights_cluster_core::Resource;
use klights_leader_api::{
    LeaderWatch, LeaderWatchError, ResourceEvent, WatchEventType, WatchRequest,
};
use klights_types::{FieldSelector, LabelSelector};
use klights_watch::{
    PreparedWatchTransition, WatchBus, WatchSignal, WatchTopic, WatchTransitionProjector,
    WatchTransitionProjectorFactory,
};

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

struct WorkerWatchTransitionProjector {
    membership: WorkerSelectorMembership,
}

impl WatchTransitionProjector for WorkerWatchTransitionProjector {
    fn replace(&mut self, resources: &[klights_cluster_core::Resource]) {
        self.membership.replace(resources);
    }

    fn prepare(&self, event: ResourceEvent) -> Result<PreparedWatchTransition, LeaderWatchError> {
        let pending = self.membership.prepare(event)?;
        Ok(PreparedWatchTransition::new(
            pending.event().cloned(),
            pending,
        ))
    }

    fn commit(&mut self, prepared: PreparedWatchTransition) -> Result<(), LeaderWatchError> {
        self.membership
            .commit(prepared.into_token::<PendingWorkerSelectorTransition>()?);
        Ok(())
    }
}

#[derive(Default)]
struct WorkerWatchTransitionProjectors;

impl WatchTransitionProjectorFactory for WorkerWatchTransitionProjectors {
    fn projector(
        &self,
        request: &WatchRequest,
    ) -> Result<Box<dyn WatchTransitionProjector>, LeaderWatchError> {
        Ok(Box::new(WorkerWatchTransitionProjector {
            membership: WorkerSelectorMembership::try_new(request)?,
        }))
    }
}

/// Selector membership used by the worker mirror.  It deliberately lives in
/// the worker crate instead of enabling `klights-watch`'s session feature:
/// that feature owns the leader-side durable datastore session and must not be
/// pulled across the worker boundary.
struct WorkerSelectorMembership {
    filter: WorkerWatchFilter,
    membership: HashMap<WorkerSelectorKey, Resource>,
}

impl WorkerSelectorMembership {
    fn try_new(request: &WatchRequest) -> Result<Self, LeaderWatchError> {
        Ok(Self {
            filter: WorkerWatchFilter::try_new(request)?,
            membership: HashMap::new(),
        })
    }

    fn replace(&mut self, resources: &[Resource]) {
        self.membership.clear();
        self.membership.extend(
            resources
                .iter()
                .cloned()
                .map(|resource| (WorkerSelectorKey::from_resource(&resource), resource)),
        );
    }

    fn prepare(
        &self,
        event: ResourceEvent,
    ) -> Result<PendingWorkerSelectorTransition, LeaderWatchError> {
        if matches!(
            event.event_type(),
            WatchEventType::Bookmark | WatchEventType::Error
        ) {
            return Ok(PendingWorkerSelectorTransition {
                event: Some(event),
                mutation: WorkerSelectorMutation::None,
            });
        }

        let key = WorkerSelectorKey::from_resource(event.resource());
        let prior = self.membership.get(&key).cloned();
        let was_member = prior.is_some();
        let matches = self.filter.matches(event.resource());
        let position = event.resume_position();
        let event_type = event.event_type();
        let current = event.resource().clone();
        let (event, mutation) = match event_type {
            WatchEventType::Deleted => {
                let mutation = if was_member {
                    WorkerSelectorMutation::Remove(key)
                } else {
                    WorkerSelectorMutation::None
                };
                ((was_member || matches).then_some(event), mutation)
            }
            WatchEventType::Added | WatchEventType::Modified if matches => {
                let event = if was_member || event_type == WatchEventType::Added {
                    Some(event)
                } else {
                    Some(ResourceEvent::try_new(
                        WatchEventType::Added,
                        current.clone(),
                        position,
                    )?)
                };
                (event, WorkerSelectorMutation::Upsert(key, current))
            }
            WatchEventType::Added | WatchEventType::Modified if was_member => {
                let event = ResourceEvent::try_new(
                    WatchEventType::Deleted,
                    prior.expect("membership was checked"),
                    position,
                )?;
                (Some(event), WorkerSelectorMutation::Remove(key))
            }
            WatchEventType::Added | WatchEventType::Modified => {
                (None, WorkerSelectorMutation::None)
            }
            WatchEventType::Bookmark | WatchEventType::Error => unreachable!(),
        };
        Ok(PendingWorkerSelectorTransition { event, mutation })
    }

    fn commit(&mut self, pending: PendingWorkerSelectorTransition) {
        match pending.mutation {
            WorkerSelectorMutation::None => {}
            WorkerSelectorMutation::Upsert(key, resource) => {
                self.membership.insert(key, resource);
            }
            WorkerSelectorMutation::Remove(key) => {
                self.membership.remove(&key);
            }
        }
    }
}

struct PendingWorkerSelectorTransition {
    event: Option<ResourceEvent>,
    mutation: WorkerSelectorMutation,
}

impl PendingWorkerSelectorTransition {
    fn event(&self) -> Option<&ResourceEvent> {
        self.event.as_ref()
    }
}

enum WorkerSelectorMutation {
    None,
    Upsert(WorkerSelectorKey, Resource),
    Remove(WorkerSelectorKey),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct WorkerSelectorKey {
    api_version: String,
    kind: String,
    namespace: Option<String>,
    name: String,
}

impl WorkerSelectorKey {
    fn from_resource(resource: &Resource) -> Self {
        Self {
            api_version: resource.api_version.clone(),
            kind: resource.kind.clone(),
            namespace: resource.namespace.clone(),
            name: resource.name.clone(),
        }
    }
}

struct WorkerWatchFilter {
    api_version: String,
    kind: String,
    namespace: Option<String>,
    label_selector: Option<LabelSelector>,
    field_selector: Option<FieldSelector>,
}

impl WorkerWatchFilter {
    fn try_new(request: &WatchRequest) -> Result<Self, LeaderWatchError> {
        let label_selector = request
            .label_selector()
            .filter(|selector| !selector.trim().is_empty())
            .map(LabelSelector::parse)
            .transpose()
            .map_err(|error| {
                LeaderWatchError::invalid_request("watch.label_selector", error.to_string())
            })?;
        let field_selector = request
            .field_selector()
            .filter(|selector| !selector.trim().is_empty())
            .map(FieldSelector::parse)
            .transpose()
            .map_err(|error| {
                LeaderWatchError::invalid_request("watch.field_selector", error.to_string())
            })?;
        Ok(Self {
            api_version: request.api_version().to_string(),
            kind: request.kind().to_string(),
            namespace: request.namespace().map(str::to_owned),
            label_selector,
            field_selector,
        })
    }

    fn matches(&self, resource: &Resource) -> bool {
        resource.api_version == self.api_version
            && resource.kind == self.kind
            && self
                .namespace
                .as_deref()
                .is_none_or(|namespace| resource.namespace.as_deref() == Some(namespace))
            && self
                .label_selector
                .as_ref()
                .is_none_or(|selector| selector.matches_resource(&resource.data))
            && self.field_selector.as_ref().is_none_or(|selector| {
                selector.matches_resource_with_identity(
                    &resource.api_version,
                    &resource.kind,
                    &resource.data,
                )
            })
    }
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
            transition_projectors: Arc::new(WorkerWatchTransitionProjectors),
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
