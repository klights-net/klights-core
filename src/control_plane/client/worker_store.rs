use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use serde_json::Value;
#[cfg(test)]
use tokio::sync::broadcast;

use crate::control_plane::client::{
    legacy_dataplane, legacy_list_response, legacy_node_subnet, legacy_watch_event,
};
use crate::datastore::{
    CatchUpResource, ListPageRequest, PositionedWatchEvent, PositionedWatchReplay,
    PositionedWatchReplayRead, Resource, ResourceList, ResourcePreconditions, WatchReplayPosition,
    WatchStore, WatchTarget, WatchTargetScope,
};
use klights_cluster_core::LogApplyPodCleanupIntentRow;
use klights_cluster_store::{ReplayAvailability, ReplayRetentionBoundary};
use klights_kubelet::pod_lifecycle_core::message::{LifecycleMessage, PodLifecycleKey};
use klights_kubelet::pod_lifecycle_router::PodLifecycleRouter;
use klights_leader_api::{
    LeaderWatch, LeaderWatchError, NodeDataplaneQuery, NodeSubnetAllocationRequest,
    NodeSubnetQuery, PeerSubnetsQuery, PodCleanupIntentAckRequest, PodCleanupIntentListRequest,
    ResourceGetRequest, ResourceListRequest, ResourceQueryConsistency, WatchRequest,
    WatchResumeCursor,
};
use klights_types::ResourceKey;
use klights_watch::{EventType, WatchBus, WatchEvent};
use klights_watch::{WatchSignal, WatchTopic};

const WORKER_WATCH_EVENT_HISTORY_CAPACITY: usize = 32_768;

fn legacy_pod_cleanup_intent(
    intent: klights_leader_api::PodCleanupIntent,
) -> LogApplyPodCleanupIntentRow {
    let (node_name, namespace, pod_name, pod_uid, reason, resource_version, created_at_ms, pod) =
        intent.into_parts();
    let pod_data = Arc::try_unwrap(pod.data).unwrap_or_else(|shared| (*shared).clone());
    LogApplyPodCleanupIntentRow {
        node_name,
        namespace,
        pod_name,
        pod_uid,
        reason,
        resource_version,
        created_at_ms,
        pod_data,
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ReflectedResourceKey {
    api_version: String,
    kind: String,
    namespace: Option<String>,
    name: String,
}

#[derive(Clone, Debug)]
struct ReflectedResource {
    uid: String,
    object: Arc<Value>,
}

#[derive(Default)]
struct ReflectorState {
    resources: HashMap<ReflectedResourceKey, ReflectedResource>,
}

struct PendingReflectorSnapshot {
    replacement: HashMap<ReflectedResourceKey, ReflectedResource>,
    events: Vec<WatchEvent>,
}

impl ReflectorState {
    fn prepare_snapshot(
        &self,
        resources: Vec<Resource>,
        snapshot_rv: i64,
    ) -> PendingReflectorSnapshot {
        let mut replacement = HashMap::with_capacity(resources.len());
        for resource in resources {
            replacement.insert(
                ReflectedResourceKey {
                    api_version: resource.api_version,
                    kind: resource.kind,
                    namespace: resource.namespace,
                    name: resource.name,
                },
                ReflectedResource {
                    uid: resource.uid,
                    object: resource.data,
                },
            );
        }

        let mut events = Vec::new();
        for (key, previous) in &self.resources {
            match replacement.get(key) {
                None => events.push(WatchEvent {
                    event_type: EventType::Deleted,
                    object: object_at_resource_version(&previous.object, snapshot_rv),
                    encoded_payload: None,
                }),
                Some(current) if current.uid != previous.uid => {
                    events.push(WatchEvent {
                        event_type: EventType::Deleted,
                        object: object_at_resource_version(&previous.object, snapshot_rv),
                        encoded_payload: None,
                    });
                    events.push(WatchEvent {
                        event_type: EventType::Added,
                        object: current.object.clone(),
                        encoded_payload: None,
                    });
                }
                Some(current) if current.object != previous.object => events.push(WatchEvent {
                    event_type: EventType::Modified,
                    object: current.object.clone(),
                    encoded_payload: None,
                }),
                Some(_) => {}
            }
        }
        for (key, current) in &replacement {
            if !self.resources.contains_key(key) {
                events.push(WatchEvent {
                    event_type: EventType::Added,
                    object: current.object.clone(),
                    encoded_payload: None,
                });
            }
        }
        events.sort_unstable_by(|left, right| {
            reflected_event_order_key(left).cmp(&reflected_event_order_key(right))
        });
        PendingReflectorSnapshot {
            replacement,
            events,
        }
    }

    fn commit_snapshot(&mut self, replacement: HashMap<ReflectedResourceKey, ReflectedResource>) {
        self.resources = replacement;
    }

    #[cfg(test)]
    fn replace_snapshot(&mut self, resources: Vec<Resource>, snapshot_rv: i64) -> Vec<WatchEvent> {
        let pending = self.prepare_snapshot(resources, snapshot_rv);
        let PendingReflectorSnapshot {
            replacement,
            events,
        } = pending;
        self.commit_snapshot(replacement);
        events
    }

    fn observe(&mut self, event: &WatchEvent) {
        let Some(key) = reflected_resource_key(&event.object) else {
            return;
        };
        match event.event_type {
            EventType::Added | EventType::Modified => {
                self.resources.insert(
                    key,
                    ReflectedResource {
                        uid: event
                            .object
                            .pointer("/metadata/uid")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string(),
                        object: event.object.clone(),
                    },
                );
            }
            EventType::Deleted => {
                let event_uid = event
                    .object
                    .pointer("/metadata/uid")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if self
                    .resources
                    .get(&key)
                    .is_some_and(|current| current.uid == event_uid)
                {
                    self.resources.remove(&key);
                }
            }
            EventType::Bookmark | EventType::Error => {}
        }
    }
}

fn reflected_event_order_key(event: &WatchEvent) -> (&str, &str, Option<&str>, &str, u8) {
    let object = event.object.as_ref();
    let event_rank = match event.event_type {
        EventType::Deleted => 0,
        EventType::Added => 1,
        EventType::Modified => 2,
        EventType::Bookmark => 3,
        EventType::Error => 4,
    };
    (
        object
            .get("apiVersion")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        object
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        object
            .pointer("/metadata/namespace")
            .and_then(Value::as_str),
        object
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        event_rank,
    )
}

fn reflected_resource_key(object: &Value) -> Option<ReflectedResourceKey> {
    Some(ReflectedResourceKey {
        api_version: object.get("apiVersion")?.as_str()?.to_string(),
        kind: object.get("kind")?.as_str()?.to_string(),
        namespace: object
            .pointer("/metadata/namespace")
            .and_then(Value::as_str)
            .map(str::to_string),
        name: object.pointer("/metadata/name")?.as_str()?.to_string(),
    })
}

fn object_at_resource_version(object: &Arc<Value>, resource_version: i64) -> Arc<Value> {
    let mut object = object.as_ref().clone();
    if let Some(metadata) = object.get_mut("metadata").and_then(Value::as_object_mut) {
        metadata.insert(
            "resourceVersion".to_string(),
            Value::String(resource_version.to_string()),
        );
    }
    Arc::new(object)
}

#[derive(Default)]
struct WorkerWatchHistory {
    events: VecDeque<(i64, WatchEvent)>,
    floors: HashMap<(WatchTopic, Option<String>), Vec<ReplayRetentionBoundary>>,
}

fn worker_replay_boundaries(
    history: &WorkerWatchHistory,
    target: &WatchTarget,
) -> Vec<ReplayRetentionBoundary> {
    let topic = WatchTopic::new(&target.api_version, &target.kind);
    history
        .floors
        .iter()
        .filter(|((floor_topic, namespace), _)| {
            if floor_topic != &topic {
                return false;
            }
            match &target.scope {
                WatchTargetScope::Cluster => namespace.is_none(),
                WatchTargetScope::Namespaced(Some(want)) => {
                    namespace.as_deref() == Some(want.as_str())
                }
                WatchTargetScope::Namespaced(None) => namespace.is_some(),
            }
        })
        .flat_map(|(_, boundaries)| boundaries.iter().copied())
        .collect()
}

/// Worker-local compatibility store for legacy kubelet call sites.
///
/// This type deliberately does not open or own `cluster.db`. Cluster resource
/// reads are served through the focused leader query/cache port;
/// node-local runtime/network rows are served through focused node-store ports.
pub trait WorkerWatchEvents: Send + Sync {
    fn subscribe_signals(&self, topic: WatchTopic) -> klights_watch::WatchSignalReceiver;
    #[cfg(test)]
    fn subscribe(&self, topic: WatchTopic) -> broadcast::Receiver<WatchEvent>;
    fn publish_signal(&self, signal: WatchSignal);
    #[cfg(test)]
    fn publish(&self, event: WatchEvent);
}

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

    #[cfg(test)]
    fn subscribe(&self, topic: WatchTopic) -> broadcast::Receiver<WatchEvent> {
        self.bus.subscribe(topic)
    }

    fn publish_signal(&self, signal: WatchSignal) {
        self.bus.publish_signal(signal);
    }

    #[cfg(test)]
    fn publish(&self, event: WatchEvent) {
        self.bus.publish(event);
    }
}

pub struct WorkerStoreAdapter {
    resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    leader_watch: Arc<dyn LeaderWatch>,
    subnet_allocation: Arc<dyn klights_leader_api::LeaderNodeSubnetAllocation>,
    network_topology: Arc<dyn klights_leader_api::LeaderNetworkTopologyQuery>,
    cleanup_intents: Arc<dyn klights_leader_api::LeaderPodCleanupIntents>,
    transition_projectors: Arc<dyn klights_watch::WatchTransitionProjectorFactory>,
    watch_events: Arc<dyn WorkerWatchEvents>,
    node_name: String,
    current_rv: AtomicI64,
    event_history: Mutex<WorkerWatchHistory>,
    next_event_id: AtomicI64,
    pod_lifecycle_router: Mutex<Option<Arc<PodLifecycleRouter>>>,
}

pub(crate) struct WorkerStorePorts {
    pub(crate) resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    pub(crate) leader_watch: Arc<dyn LeaderWatch>,
    pub(crate) subnet_allocation: Arc<dyn klights_leader_api::LeaderNodeSubnetAllocation>,
    pub(crate) network_topology: Arc<dyn klights_leader_api::LeaderNetworkTopologyQuery>,
    pub(crate) cleanup_intents: Arc<dyn klights_leader_api::LeaderPodCleanupIntents>,
    pub(crate) transition_projectors: Arc<dyn klights_watch::WatchTransitionProjectorFactory>,
    pub(crate) watch_events: Arc<dyn WorkerWatchEvents>,
}

impl LeaderWatch for WorkerStoreAdapter {
    fn watch_resources(&self, request: WatchRequest) -> klights_leader_api::LeaderWatchFuture<'_> {
        self.leader_watch.watch_resources(request)
    }
}

impl WorkerStoreAdapter {
    #[cfg(test)]
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
        Self::from_ports(
            WorkerStorePorts {
                resource_query: cluster_api.clone(),
                leader_watch: cluster_api.clone(),
                subnet_allocation: cluster_api.clone(),
                network_topology: cluster_api.clone(),
                cleanup_intents: cluster_api,
                transition_projectors: Arc::new(
                    crate::bootstrap::composition_adapters::remote_informer_cache_adapter::WatchCacheAdapter::new(),
                ),
                watch_events: Arc::new(WorkerWatchBus::new()),
            },
            node_name,
        )
    }

    pub(crate) fn from_ports(ports: WorkerStorePorts, node_name: String) -> Self {
        Self {
            resource_query: ports.resource_query,
            leader_watch: ports.leader_watch,
            subnet_allocation: ports.subnet_allocation,
            network_topology: ports.network_topology,
            cleanup_intents: ports.cleanup_intents,
            transition_projectors: ports.transition_projectors,
            watch_events: ports.watch_events,
            node_name,
            current_rv: AtomicI64::new(0),
            event_history: Mutex::new(WorkerWatchHistory::default()),
            next_event_id: AtomicI64::new(1),
            pod_lifecycle_router: Mutex::new(None),
        }
    }

    pub fn set_pod_lifecycle_router(&self, router: Arc<PodLifecycleRouter>) {
        *self
            .pod_lifecycle_router
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(router);
    }

    pub async fn start_watch_mirrors(
        self: &Arc<Self>,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<Vec<klights_supervisor::SupervisedJoinHandle<()>>> {
        let mut handles = Vec::new();
        for req in self.worker_watch_requests() {
            let this = self.clone();
            let cancel = cancel.clone();
            let spawn_supervisor = supervisor.clone();
            let mirror_supervisor = supervisor.clone();
            handles.push(
                spawn_supervisor
                    .spawn_async(
                        klights_supervisor::TaskCategory::Network,
                        "worker_store_watch_mirror",
                        async move {
                            this.run_watch_mirror(req, mirror_supervisor, cancel).await;
                        },
                    )
                    .await?,
            );
        }
        Ok(handles)
    }

    pub fn watch_signals(&self, topic: WatchTopic) -> klights_watch::WatchSignalReceiver {
        self.watch_events.subscribe_signals(topic)
    }

    #[cfg(test)]
    pub fn watch_topic(&self, topic: WatchTopic) -> broadcast::Receiver<WatchEvent> {
        self.watch_events.subscribe(topic)
    }

    fn worker_watch_requests(&self) -> Vec<WatchRequest> {
        let mut reqs = vec![
            WatchRequest::try_new(
                "v1",
                "Pod",
                None,
                None,
                Some(format!("spec.nodeName={}", self.node_name)),
                None,
                None,
            )
            .expect("worker Pod watch identity is valid"),
        ];
        for (api_version, kind, namespace) in [
            ("v1", "Namespace", None),
            ("v1", "ConfigMap", None),
            ("v1", "Secret", None),
            ("v1", "PersistentVolumeClaim", None),
            ("v1", "PersistentVolume", None),
            ("v1", "Node", None),
            ("coordination.k8s.io/v1", "Lease", Some("kube-node-lease")),
        ] {
            reqs.push(
                WatchRequest::try_new(
                    api_version,
                    kind,
                    namespace.map(str::to_string),
                    None,
                    None,
                    None,
                    None,
                )
                .expect("worker mirror watch identity is valid"),
            );
        }
        reqs
    }

    async fn run_watch_mirror(
        self: Arc<Self>,
        req: WatchRequest,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
        cancel: tokio_util::sync::CancellationToken,
    ) {
        let mut next_resource_version = req.start_resource_version();
        let mut next_watch_replay_position = req.start_watch_replay_position();
        let mut state = ReflectorState::default();
        let mut selector_membership = self
            .transition_projectors
            .projector(&req)
            .expect("worker mirror selector was validated by WatchRequest");
        // Consecutive failed reconnects; reset to 0 once the stream delivers an
        // event (progress). Drives the shared exponential reconnect backoff so
        // a sustained leader/WAN outage cannot become a fixed-interval
        // reconnect storm across every watch scope.
        let mut reconnect_attempt: u32 = 0;
        let mut immediate_expiry_relist_available = true;
        loop {
            if cancel.is_cancelled() {
                return;
            }
            if next_resource_version.is_none() {
                match self
                    .reconcile_watch_snapshot(&req, &mut state, selector_membership.as_mut())
                    .await
                {
                    Ok((resource_version, watch_replay_position)) => {
                        next_resource_version = Some(resource_version);
                        next_watch_replay_position = watch_replay_position;
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "worker store watch mirror initial list failed");
                        if !sleep_before_watch_mirror_reconnect(
                            &supervisor,
                            &cancel,
                            reconnect_attempt,
                        )
                        .await
                        {
                            return;
                        }
                        reconnect_attempt = reconnect_attempt.saturating_add(1);
                        continue;
                    }
                }
            }

            let watch_req = req
                .clone()
                .with_resume_cursor(
                    WatchResumeCursor::try_new(next_resource_version, next_watch_replay_position)
                        .expect("worker mirror cursor remains valid"),
                )
                .expect("worker mirror request remains valid");
            match self.leader_watch.watch_resources(watch_req).await {
                Ok(mut stream) => {
                    use futures::StreamExt;
                    let mut relist_required = false;
                    loop {
                        tokio::select! {
                            _ = cancel.cancelled() => return,
                            event = stream.next() => {
                                match event {
                                    Some(Ok(event)) => {
                                        let delivered = event.clone();
                                        let event_rv = delivered.resource().resource_version;
                                        let mut applied_cursor = WatchResumeCursor::try_new(
                                            next_resource_version,
                                            next_watch_replay_position,
                                        )
                                        .expect("worker mirror cursor remains valid");
                                        if let Err(err) =
                                            applied_cursor.advance_after_apply(&delivered)
                                        {
                                            tracing::warn!(error = %err, "worker mirror cursor rejected event before apply");
                                            break;
                                        }
                                        let pending_transition =
                                            match selector_membership.prepare(event) {
                                                Ok(pending) => pending,
                                                Err(err) => {
                                                    tracing::warn!(error = %err, "worker mirror selector rejected event before apply");
                                                    break;
                                                }
                                        };
                                        let Some(transitioned) = pending_transition.event() else {
                                            if let Err(err) =
                                                selector_membership.commit(pending_transition)
                                            {
                                                tracing::warn!(error = %err, "worker mirror selector could not commit filtered event");
                                                break;
                                            }
                                            if event_rv > 0 {
                                                self.observe_rv(event_rv);
                                            }
                                            next_resource_version =
                                                applied_cursor.resource_version();
                                            next_watch_replay_position =
                                                applied_cursor.replay_position();
                                            reconnect_attempt = 0;
                                            immediate_expiry_relist_available = true;
                                            continue;
                                        };
                                        let transitioned = legacy_watch_event(transitioned);
                                        let transitioned = match self
                                            .publish_watch_from_mirror(transitioned)
                                            .await
                                        {
                                            Ok(event) => event,
                                            Err(err) => {
                                                tracing::warn!(
                                                    error = %err,
                                                    "worker store watch mirror could not apply event; reconnecting from last applied position"
                                                );
                                                break;
                                            }
                                        };
                                        state.observe(&transitioned);
                                        if let Err(err) =
                                            selector_membership.commit(pending_transition)
                                        {
                                            tracing::warn!(error = %err, "worker mirror selector could not commit applied event");
                                            break;
                                        }
                                        if event_rv > 0 {
                                            self.observe_rv(event_rv);
                                        }
                                        next_resource_version = applied_cursor.resource_version();
                                        next_watch_replay_position = applied_cursor.replay_position();
                                        reconnect_attempt = 0;
                                        immediate_expiry_relist_available = true;
                                    }
                                    Some(Err(err)) => {
                                        if is_watch_window_expired(&err) {
                                            // Replay-window expiration: the
                                            // leader GC'd past our resume
                                            // bookmark and the in-scope events
                                            // in the gap are gone. Relist from
                                            // a fresh snapshot once immediately
                                            // instead of retrying the stale
                                            // bookmark, then use reconnect
                                            // backoff if the leader keeps
                                            // declaring the fresh handoff
                                            // expired.
                                            tracing::info!(
                                                error = %err,
                                                "worker store watch mirror replay window expired; relisting"
                                            );
                                            next_resource_version = None;
                                            next_watch_replay_position = None;
                                            if immediate_expiry_relist_available {
                                                immediate_expiry_relist_available = false;
                                                reconnect_attempt = 0;
                                                relist_required = true;
                                            }
                                            break;
                                        }
                                        tracing::warn!(error = %err, "worker store watch mirror failed");
                                        break;
                                    }
                                    None => break,
                                }
                            }
                        }
                    }
                    // A relist is recovery, not a retry: skip the reconnect
                    // backoff and immediately re-enter the outer loop, where
                    // next_resource_version == None triggers a fresh LIST.
                    if relist_required {
                        continue;
                    }
                }
                Err(err) => {
                    if is_watch_window_expired(&err) {
                        tracing::info!(
                            error = %err,
                            "worker store watch mirror open replay window expired; relisting"
                        );
                        next_resource_version = None;
                        next_watch_replay_position = None;
                        if immediate_expiry_relist_available {
                            immediate_expiry_relist_available = false;
                            reconnect_attempt = 0;
                            continue;
                        }
                    }
                    tracing::warn!(error = %err, "worker store watch mirror could not open stream");
                }
            }
            if !sleep_before_watch_mirror_reconnect(&supervisor, &cancel, reconnect_attempt).await {
                return;
            }
            reconnect_attempt = reconnect_attempt.saturating_add(1);
        }
    }

    async fn reconcile_watch_snapshot(
        &self,
        req: &WatchRequest,
        state: &mut ReflectorState,
        selector_membership: &mut dyn klights_watch::WatchTransitionProjector,
    ) -> Result<(i64, Option<WatchReplayPosition>)> {
        let list = self
            .resource_query
            .list_resources(ResourceListRequest::try_new(
                req.api_version().to_string(),
                req.kind().to_string(),
                req.namespace().map(str::to_owned),
                req.label_selector().map(str::to_owned),
                req.field_selector().map(str::to_owned),
                None,
                None,
                ResourceQueryConsistency::LeaderFresh,
            )?)
            .await?;
        let list = legacy_list_response(list);
        let resource_version = list.resource_version;
        let PendingReflectorSnapshot {
            replacement,
            events,
        } = state.prepare_snapshot(list.items.clone(), resource_version);
        for event in events {
            self.publish_watch_from_mirror(event).await?;
        }
        state.commit_snapshot(replacement);
        selector_membership.replace(&list.items);
        self.observe_rv(resource_version);
        Ok((resource_version, list.watch_replay_position))
    }

    async fn publish_watch_from_mirror(&self, event: WatchEvent) -> Result<WatchEvent> {
        let lifecycle_message = self.local_pod_lifecycle_message(&event);
        if let Some(message) = lifecycle_message {
            let router = self
                .pod_lifecycle_router
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
                .ok_or_else(|| {
                    anyhow!(
                        "worker store mirror saw local Pod before lifecycle router was configured"
                    )
                })?;
            router.route(message).await.map_err(|err| {
                anyhow!("worker store mirror failed to route local Pod to lifecycle actor: {err}")
            })?;
        }
        self.publish_watch(event.clone());
        Ok(event)
    }

    fn publish_watch(&self, event: WatchEvent) {
        if let Some(rv) = event
            .object
            .pointer("/metadata/resourceVersion")
            .and_then(|rv| rv.as_i64().or_else(|| rv.as_str()?.parse::<i64>().ok()))
        {
            self.observe_rv(rv);
        }
        self.record_watch_event(event.clone());
        if let Some(signal) = WatchSignal::from_event(&event) {
            self.watch_events.publish_signal(signal);
        }
        #[cfg(test)]
        self.watch_events.publish(event);
    }

    fn local_pod_lifecycle_message(&self, event: &WatchEvent) -> Option<LifecycleMessage> {
        let pod = event.object.as_ref();
        if pod.get("apiVersion").and_then(|value| value.as_str()) != Some("v1")
            || pod.get("kind").and_then(|value| value.as_str()) != Some("Pod")
        {
            return None;
        }
        if pod
            .pointer("/spec/nodeName")
            .and_then(|value| value.as_str())
            != Some(self.node_name.as_str())
        {
            return None;
        }
        let namespace = pod
            .pointer("/metadata/namespace")
            .and_then(|value| value.as_str())?;
        let name = pod
            .pointer("/metadata/name")
            .and_then(|value| value.as_str())?;
        let uid = pod
            .pointer("/metadata/uid")
            .and_then(|value| value.as_str())
            .filter(|uid| !uid.trim().is_empty())?;
        let key = PodLifecycleKey::new(namespace, name, uid);
        let resource_version = event.resource_version();
        match event.event_type {
            EventType::Added => Some(LifecycleMessage::WatchAdded {
                key,
                resource_version,
                pod: pod.clone(),
            }),
            EventType::Modified => Some(LifecycleMessage::WatchModified {
                key,
                resource_version,
                pod: pod.clone(),
            }),
            EventType::Deleted => Some(LifecycleMessage::WatchDeleted {
                key,
                resource_version,
                pod: pod.clone(),
            }),
            EventType::Bookmark | EventType::Error => None,
        }
    }

    fn record_watch_event(&self, event: WatchEvent) {
        if event.resource_version().is_none() {
            return;
        }
        let mut history = self
            .event_history
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let event_id = self.next_event_id.fetch_add(1, Ordering::Relaxed);
        history.events.push_back((event_id, event));
        while history.events.len() > WORKER_WATCH_EVENT_HISTORY_CAPACITY {
            if let Some((removed_id, removed)) = history.events.pop_front()
                && let Some(topic) = watch_event_topic(&removed)
            {
                let namespace = removed
                    .object
                    .pointer("/metadata/namespace")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                if let Some(resource_version) = removed.resource_version() {
                    let floor = history.floors.entry((topic, namespace)).or_default();
                    ReplayRetentionBoundary::retain_exact(
                        floor,
                        WatchReplayPosition {
                            resource_version,
                            event_id: removed_id,
                            resource_version_filter_through_event_id: 0,
                        },
                    );
                }
            }
        }
    }

    fn historical_watch_events_since(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
    ) -> Vec<CatchUpResource> {
        let history = self
            .event_history
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        history
            .events
            .iter()
            .filter_map(|(_, event)| {
                let rv = event.resource_version()?;
                if rv <= since_rv || !watch_event_matches_targets(event, targets) {
                    return None;
                }
                catchup_resource_from_watch_event(event)
            })
            .collect()
    }

    fn is_pod_resource(api_version: &str, kind: &str) -> bool {
        api_version == "v1" && kind == "Pod"
    }

    fn pod_belongs_to_local_node(&self, resource: &Resource) -> bool {
        resource
            .data
            .pointer("/spec/nodeName")
            .and_then(|node| node.as_str())
            .is_some_and(|node| node == self.node_name)
    }

    fn local_pod_field_selector(&self, field_selector: Option<&str>) -> String {
        let local_selector = format!("spec.nodeName={}", self.node_name);
        match field_selector
            .map(str::trim)
            .filter(|selector| !selector.is_empty())
        {
            Some(selector)
                if selector
                    .split(',')
                    .any(|part| part.trim() == local_selector) =>
            {
                selector.to_string()
            }
            Some(selector) => format!("{selector},{local_selector}"),
            None => local_selector,
        }
    }

    fn observe_rv(&self, rv: i64) {
        let mut current = self.current_rv.load(Ordering::Relaxed);
        while rv > current {
            match self.current_rv.compare_exchange_weak(
                current,
                rv,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    fn unsupported<T>(&self, operation: &str) -> Result<T> {
        Err(anyhow!(
            "worker-local store does not support direct cluster datastore operation {operation}"
        ))
    }
}

async fn sleep_before_watch_mirror_reconnect(
    supervisor: &klights_supervisor::TaskSupervisor,
    cancel: &tokio_util::sync::CancellationToken,
    attempt: u32,
) -> bool {
    tokio::select! {
        _ = cancel.cancelled() => false,
        _ = supervisor.sleep(
            "worker_store_watch_mirror_reconnect",
            klights_supervisor::reconnect_backoff::delay(attempt),
        ) => true,
    }
}

/// True when a worker watch-stream error is a replay-window expiration
/// (the Kubernetes "too old resource version" / HTTP 410 contract). The leader
/// returns a typed WatchResources marker when the durable `watch_events` window
/// no longer covers the worker's resume bookmark; the reflector must relist
/// from a fresh snapshot rather than retry the stale bookmark, which would loop
/// on the same expiration.
///
/// The tonic::Status is carried as the error source by the gRPC client (see
/// `watch_resources_rpc`), so walk the anyhow chain to find and inspect it.
fn is_watch_window_expired(err: &LeaderWatchError) -> bool {
    matches!(err, LeaderWatchError::ReplayExpired { .. })
}

fn watch_event_topic(event: &WatchEvent) -> Option<WatchTopic> {
    Some(WatchTopic::new(
        event.object.get("apiVersion")?.as_str()?,
        event.object.get("kind")?.as_str()?,
    ))
}

fn watch_event_matches_targets(event: &WatchEvent, targets: &[WatchTarget]) -> bool {
    let Some(api_version) = event
        .object
        .get("apiVersion")
        .and_then(|value| value.as_str())
    else {
        return false;
    };
    let Some(kind) = event.object.get("kind").and_then(|value| value.as_str()) else {
        return false;
    };
    let namespace = event
        .object
        .pointer("/metadata/namespace")
        .and_then(|value| value.as_str());

    targets.iter().any(|target| {
        if target.api_version != api_version || target.kind != kind {
            return false;
        }
        match &target.scope {
            WatchTargetScope::Cluster => namespace.is_none(),
            WatchTargetScope::Namespaced(Some(target_ns)) => namespace == Some(target_ns.as_str()),
            WatchTargetScope::Namespaced(None) => namespace.is_some(),
        }
    })
}

fn worker_replay_event_follows_position(
    position: WatchReplayPosition,
    event_id: i64,
    event: &WatchEvent,
) -> bool {
    event
        .resource_version()
        .is_some_and(|resource_version| !position.represents_event(event_id, resource_version))
}

fn catchup_resource_from_watch_event(event: &WatchEvent) -> Option<CatchUpResource> {
    let api_version = event.object.get("apiVersion")?.as_str()?.to_string();
    let kind = event.object.get("kind")?.as_str()?.to_string();
    let metadata = event.object.get("metadata")?;
    let name = metadata.get("name")?.as_str()?.to_string();
    let namespace = metadata
        .get("namespace")
        .and_then(|value| value.as_str())
        .map(str::to_string);
    let uid = metadata
        .get("uid")
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .to_string();
    let resource_version = event.resource_version()?;

    Some(CatchUpResource {
        resource: Resource {
            id: 0,
            api_version,
            kind,
            namespace,
            name,
            uid,
            resource_version,
            data: event.object.clone(),
        },
        event_type: std::borrow::Cow::Owned(event.event_type.to_string()),
    })
}

#[async_trait]
impl crate::datastore::CurrentResourceVersionStore for WorkerStoreAdapter {
    async fn get_current_resource_version(&self) -> Result<i64> {
        Ok(self.current_rv.load(Ordering::Relaxed))
    }
}

#[async_trait]
impl WatchStore for WorkerStoreAdapter {
    #[cfg(test)]
    fn subscribe_watch(&self, topic: WatchTopic) -> broadcast::Receiver<klights_watch::WatchEvent> {
        self.watch_events.subscribe(topic)
    }

    async fn list_watch_events_since(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        Ok(self.historical_watch_events_since(targets, since_rv))
    }

    async fn list_watch_events_after_position_checked_bounded(
        &self,
        targets: &[WatchTarget],
        position: WatchReplayPosition,
        limit: std::num::NonZeroUsize,
    ) -> Result<PositionedWatchReplayRead<CatchUpResource>> {
        let high_water_event_id = self.next_event_id.load(Ordering::Relaxed).saturating_sub(1);
        let history = self
            .event_history
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if targets.iter().any(|target| {
            ReplayRetentionBoundary::classify_all(
                worker_replay_boundaries(&history, target),
                position,
            ) == ReplayAvailability::Expired
        }) {
            return Ok(PositionedWatchReplayRead::Expired);
        }
        let events: Vec<_> = history
            .events
            .iter()
            .filter(|(event_id, event)| {
                worker_replay_event_follows_position(position, *event_id, event)
            })
            .filter(|(_, event)| watch_event_matches_targets(event, targets))
            .filter_map(|(event_id, event)| {
                let resource_version = event.resource_version()?;
                Some(PositionedWatchEvent {
                    position: WatchReplayPosition {
                        resource_version,
                        event_id: *event_id,
                        resource_version_filter_through_event_id: 0,
                    },
                    event: catchup_resource_from_watch_event(event)?,
                })
            })
            .take(limit.get())
            .collect();
        let next_position =
            WatchReplayPosition::after_page(position, &events, high_water_event_id, limit);
        Ok(PositionedWatchReplayRead::Events(PositionedWatchReplay {
            events,
            next_position,
        }))
    }
}

impl klights_watch::WatchSignalSubscribe for WorkerStoreAdapter {
    fn subscribe(&self, topic: WatchTopic) -> klights_watch::WatchSignalReceiver {
        self.watch_events.subscribe_signals(topic)
    }
}

#[async_trait]
impl crate::datastore::ResourceStore for WorkerStoreAdapter {
    async fn create_resource(
        &self,
        _api_version: &str,
        _kind: &str,
        _namespace: Option<&str>,
        _name: &str,
        _data: Value,
    ) -> Result<Resource> {
        self.unsupported("create_resource")
    }

    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        let key = ResourceKey {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
        };
        let resource = self
            .resource_query
            .get_resource(ResourceGetRequest::try_new(
                key,
                ResourceQueryConsistency::Cached,
            )?)
            .await?;
        if Self::is_pod_resource(api_version, kind)
            && resource
                .as_ref()
                .is_some_and(|resource| !self.pod_belongs_to_local_node(resource))
        {
            return Ok(None);
        }
        if let Some(resource) = &resource {
            self.observe_rv(resource.resource_version);
        }
        Ok(resource)
    }

    async fn delete_resource(
        &self,
        _api_version: &str,
        _kind: &str,
        _namespace: Option<&str>,
        _name: &str,
    ) -> Result<()> {
        self.unsupported("delete_resource")
    }

    async fn delete_resource_with_preconditions(
        &self,
        _api_version: &str,
        _kind: &str,
        _namespace: Option<&str>,
        _name: &str,
        _preconditions: ResourcePreconditions,
    ) -> Result<()> {
        self.unsupported("delete_resource_with_preconditions")
    }

    async fn update_resource(
        &self,
        _api_version: &str,
        _kind: &str,
        _namespace: Option<&str>,
        _name: &str,
        _data: Value,
        _expected_rv: i64,
    ) -> Result<Resource> {
        self.unsupported("update_resource")
    }

    async fn update_resource_with_preconditions(
        &self,
        _api_version: &str,
        _kind: &str,
        _namespace: Option<&str>,
        _name: &str,
        _data: Value,
        _preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        self.unsupported("update_resource_with_preconditions")
    }
}

#[async_trait]
impl crate::datastore::ResourceListStore for WorkerStoreAdapter {
    async fn list_resources_page(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList> {
        let field_selector = if Self::is_pod_resource(api_version, kind) {
            Some(self.local_pod_field_selector(field_selector))
        } else {
            field_selector.map(str::to_string)
        };
        let list = self
            .resource_query
            .list_resources(ResourceListRequest::try_new(
                api_version,
                kind,
                namespace.map(str::to_string),
                label_selector.map(str::to_string),
                field_selector,
                None,
                None,
                ResourceQueryConsistency::Cached,
            )?)
            .await?;
        let mut list = legacy_list_response(list);
        self.observe_rv(list.resource_version);
        if page.limit().is_some() || page.continue_token().is_some() {
            list.items.sort_by(|a, b| a.name.cmp(&b.name));
            list = page.apply_to_sorted_resource_list(list);
        }
        Ok(list)
    }

    async fn list_resource_keys_for_scope(
        &self,
        api_version: String,
        kind: String,
        namespaced: bool,
    ) -> Result<Vec<(Option<String>, String)>> {
        let list = crate::datastore::ResourceListStore::list_resources_page(
            self,
            &api_version,
            &kind,
            None,
            None,
            None,
            ListPageRequest::unbounded(),
        )
        .await?;
        Ok(list
            .items
            .into_iter()
            .map(|resource| {
                (
                    namespaced.then_some(resource.namespace).flatten(),
                    resource.name,
                )
            })
            .collect())
    }
}

#[async_trait]
impl crate::datastore::ReplicationStore for WorkerStoreAdapter {
    async fn replace_replicated_resource_state(
        &self,
        _entries: Vec<klights_cluster_core::SnapshotRestoreOperation>,
        _current_rv: i64,
        _watch_event_high_water: Option<i64>,
        _watch_replay_floors: Option<Vec<crate::datastore::WatchReplayFloor>>,
        _metadata: Option<crate::datastore::ReplicatedSnapshotMetadata>,
    ) -> Result<()> {
        self.unsupported("replace_replicated_resource_state")
    }

    async fn apply_log_apply_commit(
        &self,
        _commit: klights_cluster_core::LogApplyCommit,
    ) -> Result<()> {
        self.unsupported("apply_log_apply_commit")
    }

    async fn apply_raft_log_apply_commit(
        &self,
        _commit: klights_cluster_core::LogApplyCommit,
    ) -> Result<klights_cluster_store::StorageCommandResult> {
        self.unsupported("apply_raft_log_apply_commit")
    }

    async fn apply_raft_log_apply_commit_receipt(
        &self,
        _commit: klights_cluster_core::LogApplyCommit,
    ) -> Result<klights_cluster_store::CommittedRaftApplyReceipt> {
        self.unsupported("apply_raft_log_apply_commit_receipt")
    }

    #[cfg(any(test, feature = "pod-repository-test-support"))]
    async fn apply_replicated_create_resource(
        &self,
        _api_version: &str,
        _kind: &str,
        _namespace: Option<&str>,
        _name: &str,
        _data: Value,
        _options: crate::datastore::ReplicatedCreateOptions,
    ) -> Result<Resource> {
        self.unsupported("apply_replicated_create_resource")
    }
}

#[async_trait]
impl crate::datastore::NamespaceContentStore for WorkerStoreAdapter {
    async fn list_namespace_resources(&self, _namespace: &str) -> Result<Vec<Resource>> {
        Ok(Vec::new())
    }

    async fn list_namespace_resources_of_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>> {
        Ok(crate::datastore::ResourceListStore::list_resources_page(
            self,
            "v1",
            kind,
            Some(namespace),
            None,
            None,
            ListPageRequest::unbounded(),
        )
        .await?
        .items)
    }

    async fn list_namespace_resources_excluding_kind(
        &self,
        _namespace: &str,
        _kind: &str,
    ) -> Result<Vec<Resource>> {
        Ok(Vec::new())
    }

    async fn count_namespace_resources(&self, namespace: &str) -> Result<i64> {
        Ok(
            crate::datastore::NamespaceContentStore::list_namespace_resources(self, namespace)
                .await?
                .len() as i64,
        )
    }
}

#[async_trait]
impl crate::datastore::OwnershipStore for WorkerStoreAdapter {
    async fn find_owned_resources(
        &self,
        owner_uid: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        let _ = (owner_uid, namespace);
        Ok(Vec::new())
    }

    async fn list_resources_by_owner_uid(
        &self,
        _api_version: &str,
        _kind: &str,
        _namespace: Option<&str>,
        _owner_uid: &str,
    ) -> Result<Vec<Resource>> {
        Ok(Vec::new())
    }

    async fn find_owned_by_name_kind_empty_uid(
        &self,
        _owner_api_version: &str,
        _owner_name: &str,
        _owner_kind: &str,
        _namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        Ok(Vec::new())
    }
}

#[async_trait]
impl crate::datastore::StatusStore for WorkerStoreAdapter {
    async fn update_status_only(
        &self,
        _api_version: &str,
        _kind: &str,
        _namespace: Option<&str>,
        _name: &str,
        _status: Value,
        _expected_rv: Option<i64>,
    ) -> Result<Resource> {
        self.unsupported("update_status_only")
    }

    async fn update_status_only_with_preconditions(
        &self,
        _api_version: &str,
        _kind: &str,
        _namespace: Option<&str>,
        _name: &str,
        _status: Value,
        _preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        self.unsupported("update_status_only_with_preconditions")
    }
}

#[async_trait]
impl crate::datastore::NetworkMetadataStore for WorkerStoreAdapter {
    async fn allocate_node_subnet(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<klights_cluster_store::StoredNodeSubnet> {
        let request = NodeSubnetAllocationRequest::try_new(node_name, cluster_cidr, node_ip)
            .map_err(anyhow::Error::new)?;
        let result = self
            .subnet_allocation
            .allocate_node_subnet(request)
            .await
            .map_err(anyhow::Error::new)?;
        legacy_node_subnet(result.into_subnet()).map_err(anyhow::Error::new)
    }

    async fn update_node_peer_attributes(
        &self,
        _node_name: &str,
        _mode: klights_controllers::annotations::NodePeerMode,
        _hostport_range: Option<klights_types::HostPortRange>,
    ) -> Result<()> {
        Ok(())
    }

    async fn update_node_dataplane(
        &self,
        _metadata: klights_cluster_store::DataplanePeerMetadata,
    ) -> Result<()> {
        self.unsupported("update_node_dataplane")
    }

    async fn get_node_dataplane(
        &self,
        node_name: &str,
    ) -> Result<Option<klights_cluster_store::DataplanePeerMetadata>> {
        let request = NodeDataplaneQuery::try_new(node_name).map_err(anyhow::Error::new)?;
        self.network_topology
            .get_node_dataplane(request)
            .await
            .map_err(anyhow::Error::new)?
            .into_option()
            .map(legacy_dataplane)
            .transpose()
            .map_err(anyhow::Error::new)
    }

    async fn get_node_subnet(
        &self,
        node_name: &str,
    ) -> Result<Option<klights_cluster_store::StoredNodeSubnet>> {
        let request = NodeSubnetQuery::try_new(node_name).map_err(anyhow::Error::new)?;
        self.network_topology
            .get_node_subnet(request)
            .await
            .map_err(anyhow::Error::new)?
            .into_option()
            .map(legacy_node_subnet)
            .transpose()
            .map_err(anyhow::Error::new)
    }

    async fn list_peer_subnets(
        &self,
        request: klights_cluster_store::PeerTopologyRequest,
    ) -> Result<Vec<klights_cluster_store::StoredNodeSubnet>> {
        let excluded_node_name = request.excluded_node_name().ok_or_else(|| {
            anyhow!("worker topology transport does not expose an all-subnets snapshot query")
        })?;
        let request =
            PeerSubnetsQuery::try_new(excluded_node_name.as_str()).map_err(anyhow::Error::new)?;
        self.network_topology
            .list_peer_subnets(request)
            .await
            .map_err(anyhow::Error::new)?
            .into_vec()
            .into_iter()
            .map(legacy_node_subnet)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(anyhow::Error::new)
    }

    async fn delete_node_subnet(&self, _node_name: &str) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl crate::datastore::PodCleanupStore for WorkerStoreAdapter {
    async fn move_pod_to_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        let _ = (node_name, namespace, pod_name, pod_uid, reason);
        self.unsupported("move_pod_to_cleanup_intent")
    }

    async fn list_pod_cleanup_intents_for_node(
        &self,
        node_name: &str,
    ) -> Result<Vec<LogApplyPodCleanupIntentRow>> {
        self.cleanup_intents
            .list_pod_cleanup_intents(
                PodCleanupIntentListRequest::try_new(node_name).map_err(anyhow::Error::new)?,
            )
            .await
            .map(|intents| intents.into_iter().map(legacy_pod_cleanup_intent).collect())
            .map_err(anyhow::Error::new)
    }

    async fn delete_pod_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        self.cleanup_intents
            .acknowledge_pod_cleanup_intent(
                PodCleanupIntentAckRequest::try_new(
                    node_name, namespace, pod_name, pod_uid, reason,
                )
                .map_err(anyhow::Error::new)?,
            )
            .await
            .map_err(anyhow::Error::new)
    }

    async fn delete_pod_cleanup_intents_for_node(&self, node_name: &str) -> Result<()> {
        let _ = node_name;
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/worker.rs"]
mod tests;
