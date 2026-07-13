use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::broadcast;

use crate::control_plane::client::{LeaderApiClient, ListRequest, ResourceKey, WatchRequest};
#[cfg(test)]
use crate::datastore::command::{CommandMeta, StorageCommand};
use crate::datastore::node_local::NodeLocalHandle;
use crate::datastore::replay_retention::{ReplayAvailability, ReplayRetentionBoundary};
use crate::datastore::{
    AppliedOutboxRecord, CatchUpResource, DatastoreBackend, ListPageRequest, NodeSubnet, PatchKind,
    PodCleanupIntent, PodEndpointEvent, PodEndpointRow, PodNetworkAllocationLink,
    PodNetworkAllocationPod, PodNetworkAllocationRequest, PodNetworkAllocationSubnet,
    PodNetworkEndpoint, PodSlotAdmissionEvent, PodSlotAdmissionResult, PodWorkqueueEntry,
    PodWorkqueueKind, PositionedWatchEvent, PositionedWatchReplay, PositionedWatchReplayRead,
    RawWatchEvent, Resource, ResourceList, ResourcePatchRequest, ResourcePreconditions, SandboxRef,
    SnapshotAtRv, WatchReplayPosition, WatchReplayRead, WatchStore, WatchTarget, WatchTargetScope,
};
use crate::kubelet::pod_lifecycle_core::message::{LifecycleMessage, PodLifecycleKey};
use crate::kubelet::pod_lifecycle_router::PodLifecycleRouter;
use crate::watch::{EventType, WatchBus, WatchEvent, WatchSignal, WatchTopic};

const WORKER_WATCH_EVENT_HISTORY_CAPACITY: usize = 32_768;

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
/// reads are served through the node/worker cache exposed by `LeaderApiClient`;
/// node-local runtime/network rows are served through `NodeLocalBackend`.
pub struct WorkerStoreAdapter {
    cluster_api: Arc<dyn LeaderApiClient>,
    node_local: NodeLocalHandle,
    watch_bus: Arc<WatchBus>,
    node_name: String,
    current_rv: AtomicI64,
    event_history: Mutex<WorkerWatchHistory>,
    next_event_id: AtomicI64,
    pod_lifecycle_router: Mutex<Option<Arc<PodLifecycleRouter>>>,
}

impl WorkerStoreAdapter {
    pub fn new(
        cluster_api: Arc<dyn LeaderApiClient>,
        node_local: NodeLocalHandle,
        node_name: String,
    ) -> Self {
        Self {
            cluster_api,
            node_local,
            watch_bus: Arc::new(WatchBus::new(1024)),
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
        supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<Vec<crate::task_supervisor::SupervisedJoinHandle<()>>> {
        let mut handles = Vec::new();
        for req in self.worker_watch_requests() {
            let this = self.clone();
            let cancel = cancel.clone();
            let spawn_supervisor = supervisor.clone();
            let mirror_supervisor = supervisor.clone();
            handles.push(
                spawn_supervisor
                    .spawn_async(
                        crate::task_supervisor::TaskCategory::Network,
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

    pub fn watch_signals(&self, topic: WatchTopic) -> broadcast::Receiver<WatchSignal> {
        self.watch_bus.subscribe_signals(topic)
    }

    #[cfg(test)]
    pub fn watch_topic(&self, topic: WatchTopic) -> broadcast::Receiver<WatchEvent> {
        self.watch_bus.subscribe(topic)
    }

    fn worker_watch_requests(&self) -> Vec<WatchRequest> {
        let mut reqs = vec![WatchRequest {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: None,
            label_selector: None,
            field_selector: Some(format!("spec.nodeName={}", self.node_name)),
            start_resource_version: None,
            start_watch_replay_position: None,
        }];
        for (api_version, kind, namespace) in [
            ("v1", "Namespace", None),
            ("v1", "ConfigMap", None),
            ("v1", "Secret", None),
            ("v1", "PersistentVolumeClaim", None),
            ("v1", "PersistentVolume", None),
            ("v1", "Node", None),
            ("coordination.k8s.io/v1", "Lease", Some("kube-node-lease")),
        ] {
            reqs.push(WatchRequest {
                api_version: api_version.to_string(),
                kind: kind.to_string(),
                namespace: namespace.map(str::to_string),
                label_selector: None,
                field_selector: None,
                start_resource_version: None,
                start_watch_replay_position: None,
            });
        }
        reqs
    }

    async fn run_watch_mirror(
        self: Arc<Self>,
        req: WatchRequest,
        supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
        cancel: tokio_util::sync::CancellationToken,
    ) {
        let mut next_resource_version = req.start_resource_version;
        let mut next_watch_replay_position = req.start_watch_replay_position;
        let mut state = ReflectorState::default();
        let mut selector_membership = crate::watch::SelectorMembership::default();
        let has_selector = super::watch_request_has_selector(&req);
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
                    .reconcile_watch_snapshot(
                        &req,
                        &mut state,
                        has_selector.then_some(&mut selector_membership),
                    )
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

            let mut watch_req = req.clone();
            watch_req.start_resource_version = next_resource_version;
            watch_req.start_watch_replay_position = next_watch_replay_position;
            let selector_req = watch_req.clone();
            match self.cluster_api.watch_resources(watch_req).await {
                Ok(mut stream) => {
                    use futures::StreamExt;
                    let mut relist_required = false;
                    loop {
                        tokio::select! {
                            _ = cancel.cancelled() => return,
                            event = stream.next() => {
                                match event {
                                    Some(Ok(event)) => {
                                        reconnect_attempt = 0;
                                        immediate_expiry_relist_available = true;
                                        let event_rv = event.event.resource_version();
                                        let resume_position = event.resume_position;
                                        let matches = super::watch_request_matches_event(
                                            &selector_req,
                                            &event.event,
                                        );
                                        let (transitioned, membership_mutation) = if has_selector {
                                            let (event, mutation) = selector_membership
                                                .prepare_transition(event.event, matches)
                                                .into_parts();
                                            (event, Some(mutation))
                                        } else {
                                            (matches.then_some(event.event), None)
                                        };
                                        let Some(transitioned) = transitioned
                                        else {
                                            if let Some(mutation) = membership_mutation {
                                                selector_membership.commit(mutation);
                                            }
                                            if let Some(rv) = event_rv {
                                                self.observe_rv(rv);
                                            }
                                            super::advance_watch_resume_after_apply(
                                                &mut next_resource_version,
                                                &mut next_watch_replay_position,
                                                event_rv,
                                                resume_position,
                                            );
                                            continue;
                                        };
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
                                        if let Some(mutation) = membership_mutation {
                                            selector_membership.commit(mutation);
                                        }
                                        if let Some(rv) = event_rv {
                                            self.observe_rv(rv);
                                        }
                                        super::advance_watch_resume_after_apply(
                                            &mut next_resource_version,
                                            &mut next_watch_replay_position,
                                            event_rv,
                                            resume_position,
                                        );
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
        selector_membership: Option<&mut crate::watch::SelectorMembership>,
    ) -> Result<(i64, Option<WatchReplayPosition>)> {
        let list = self
            .cluster_api
            .list_resources_fresh(ListRequest {
                api_version: req.api_version.clone(),
                kind: req.kind.clone(),
                namespace: req.namespace.clone(),
                label_selector: req.label_selector.clone(),
                field_selector: req.field_selector.clone(),
                limit: None,
                continue_token: None,
            })
            .await?;
        let resource_version = list.resource_version;
        let pending_membership = selector_membership.as_ref().map(|_| {
            let mut membership = crate::watch::SelectorMembership::default();
            membership.replace_from_resources(&list.items);
            membership
        });
        let PendingReflectorSnapshot {
            replacement,
            events,
        } = state.prepare_snapshot(list.items, resource_version);
        for event in events {
            self.publish_watch_from_mirror(event).await?;
        }
        state.commit_snapshot(replacement);
        if let (Some(selector_membership), Some(pending_membership)) =
            (selector_membership, pending_membership)
        {
            *selector_membership = pending_membership;
        }
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
            self.watch_bus.publish_signal(signal);
        }
        #[cfg(test)]
        self.watch_bus.publish(event);
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

    fn snapshot_replay_event_type(since_rv: i64) -> &'static str {
        if since_rv > 0 { "MODIFIED" } else { "ADDED" }
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
    supervisor: &crate::task_supervisor::TaskSupervisor,
    cancel: &tokio_util::sync::CancellationToken,
    attempt: u32,
) -> bool {
    tokio::select! {
        _ = cancel.cancelled() => false,
        _ = supervisor.sleep(
            "worker_store_watch_mirror_reconnect",
            crate::utils::watch_reconnect_delay(attempt),
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
fn is_watch_window_expired(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<tonic::Status>()
            .is_some_and(crate::replication::grpc::is_watch_replay_expired_status)
    })
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
    event.resource_version().is_some_and(|resource_version| {
        !crate::datastore::position_membership::position_represents(
            position,
            event_id,
            resource_version,
        )
    })
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
    fn subscribe_watch_signals(&self, topic: WatchTopic) -> broadcast::Receiver<WatchSignal> {
        self.watch_bus.subscribe_signals(topic)
    }

    #[cfg(test)]
    fn subscribe_watch(&self, topic: WatchTopic) -> broadcast::Receiver<crate::watch::WatchEvent> {
        self.watch_bus.subscribe(topic)
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

#[async_trait]
impl DatastoreBackend for WorkerStoreAdapter {
    fn close(&self) {
        self.node_local.close();
    }

    fn subscribe_watch_signals(&self, topic: WatchTopic) -> broadcast::Receiver<WatchSignal> {
        self.watch_bus.subscribe_signals(topic)
    }

    #[cfg(test)]
    fn subscribe_watch(&self, topic: WatchTopic) -> broadcast::Receiver<crate::watch::WatchEvent> {
        self.watch_bus.subscribe(topic)
    }

    #[cfg(test)]
    fn subscribe_watch_many(&self, topics: Vec<WatchTopic>) -> crate::watch::WatchReceiver {
        self.watch_bus.subscribe_many(topics)
    }

    #[cfg(test)]
    fn broadcast_watch_event(&self, pending: crate::datastore::PendingWatchEvent) {
        self.publish_watch(pending.event);
    }

    async fn apply_raft_log_apply_commit(
        &self,
        _commit: crate::log_apply::LogApplyCommit,
    ) -> Result<crate::datastore::raft::types::StorageCommandResult> {
        self.unsupported("apply_raft_log_apply_commit")
    }

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
        let resource = self.cluster_api.get_resource(key).await?;
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
        // Fetch the full scope from the leader and paginate locally. The worker
        // cache (and the leader datastore) own the authoritative collection, but
        // pagination must be applied exactly once: passing limit/continue_token
        // to the leader *and* re-applying ListPageRequest here would let the
        // local pass clear the leader-provided continue_token (the leader has
        // already truncated to the limit), silently dropping the rest of the
        // collection from a worker LIST. The node/worker cache also returns
        // items in arbitrary (hash-map) order, so sort by name before applying
        // the page so name-based continuation is deterministic and matches the
        // leader's ordering.
        let mut list = self
            .cluster_api
            .list_resources(ListRequest {
                api_version: api_version.to_string(),
                kind: kind.to_string(),
                namespace: namespace.map(str::to_string),
                label_selector: label_selector.map(str::to_string),
                field_selector,
                limit: None,
                continue_token: None,
            })
            .await?;
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
        let list = self
            .list_resources(
                &api_version,
                &kind,
                None,
                crate::datastore::ResourceListQuery::all(),
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

    async fn get_current_resource_version(&self) -> Result<i64> {
        Ok(self.current_rv.load(Ordering::Relaxed))
    }

    async fn create_namespace(&self, _name: &str, _data: Value) -> Result<Resource> {
        self.unsupported("create_namespace")
    }

    async fn get_namespace(&self, name: &str) -> Result<Option<Resource>> {
        let key = ResourceKey {
            api_version: "v1".to_string(),
            kind: "Namespace".to_string(),
            namespace: None,
            name: name.to_string(),
        };
        let resource = self.cluster_api.get_resource_fresh(key).await?;
        if let Some(resource) = &resource {
            self.observe_rv(resource.resource_version);
        }
        Ok(resource)
    }

    async fn list_namespaces_page(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList> {
        self.list_resources_page(
            "v1",
            "Namespace",
            None,
            label_selector,
            field_selector,
            page,
        )
        .await
    }

    async fn update_namespace(
        &self,
        _name: &str,
        _data: Value,
        _expected_rv: i64,
    ) -> Result<Resource> {
        self.unsupported("update_namespace")
    }

    async fn delete_namespace_contents(&self, _name: &str) -> Result<()> {
        self.unsupported("delete_namespace_contents")
    }

    async fn delete_namespace(&self, _name: &str) -> Result<()> {
        self.unsupported("delete_namespace")
    }

    async fn pod_workqueue_enqueue(
        &self,
        kind: PodWorkqueueKind,
        pod: &crate::pod_identity::PodIdentity,
        payload: Value,
        attempt_count: i64,
        min_delay_ms: i64,
        last_error: Option<&str>,
    ) -> Result<()> {
        self.node_local
            .enqueue_workqueue(kind, pod, payload, attempt_count, min_delay_ms, last_error)
            .await
    }

    async fn pod_workqueue_peek_next_due(&self) -> Result<Option<i64>> {
        self.node_local.peek_workqueue_next_due().await
    }

    async fn pod_workqueue_claim_due(&self, now_ms: i64) -> Result<Option<PodWorkqueueEntry>> {
        self.node_local.claim_workqueue_due(now_ms).await
    }

    async fn pod_workqueue_complete(&self, id: i64) -> Result<()> {
        self.node_local.complete_workqueue(id).await
    }

    async fn pod_workqueue_record_failure(
        &self,
        row: PodWorkqueueEntry,
        min_delay_ms: i64,
        error: &str,
    ) -> Result<()> {
        let pod = crate::pod_identity::PodIdentity::new(&row.namespace, &row.name, &row.uid);
        self.node_local
            .enqueue_workqueue(
                row.kind,
                &pod,
                row.payload,
                row.attempt_count.saturating_add(1),
                min_delay_ms,
                Some(error),
            )
            .await
    }

    async fn pod_workqueue_dead_letter(&self, id: i64, _error: &str) -> Result<()> {
        self.node_local.complete_workqueue(id).await
    }

    async fn record_sandbox(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        sandbox_id: &str,
    ) -> Result<()> {
        self.node_local
            .admit_pod_runtime(pod_uid, namespace, pod_name, &self.node_name)
            .await?;
        self.node_local.record_sandbox(pod_uid, sandbox_id).await
    }

    async fn get_sandbox(&self, namespace: &str, pod_name: &str) -> Result<Option<String>> {
        Ok(self
            .node_local
            .list_pod_runtime_by_namespace(namespace)
            .await?
            .into_iter()
            .find(|row| row.pod_name == pod_name)
            .and_then(|row| row.sandbox_id))
    }

    async fn get_sandbox_for_uid(
        &self,
        _namespace: &str,
        _pod_name: &str,
        pod_uid: &str,
    ) -> Result<Option<String>> {
        Ok(self
            .node_local
            .get_pod_runtime(pod_uid)
            .await?
            .and_then(|row| row.sandbox_id))
    }

    async fn delete_sandbox(&self, namespace: &str, pod_name: &str) -> Result<()> {
        for row in self
            .node_local
            .list_pod_runtime_by_namespace(namespace)
            .await?
            .into_iter()
            .filter(|row| row.pod_name == pod_name)
        {
            self.node_local
                .delete_pod_runtime_for_uid(&row.pod_uid)
                .await?;
        }
        Ok(())
    }

    async fn delete_sandbox_for_uid(
        &self,
        _namespace: &str,
        _pod_name: &str,
        pod_uid: &str,
        _sandbox_id: &str,
    ) -> Result<()> {
        self.node_local.delete_pod_runtime_for_uid(pod_uid).await
    }

    async fn delete_pod_network(&self, sandbox_id: &str) -> Result<()> {
        self.node_local.delete_network_for_sandbox(sandbox_id).await
    }

    async fn find_owned_resources(
        &self,
        _owner_uid: &str,
        _namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
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

    async fn list_cluster_resources_modified_since(
        &self,
        api_version: &str,
        kind: &str,
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        let list = self
            .list_resources(
                api_version,
                kind,
                None,
                crate::datastore::ResourceListQuery::all(),
            )
            .await?;
        let event_type = Self::snapshot_replay_event_type(since_rv);
        Ok(list
            .items
            .into_iter()
            .filter(|resource| resource.resource_version > since_rv)
            .map(|resource| CatchUpResource {
                resource,
                event_type: std::borrow::Cow::Borrowed(event_type),
            })
            .collect())
    }

    async fn list_cluster_resources(&self) -> Result<Vec<Resource>> {
        self.unsupported("list_cluster_resources")
    }

    async fn list_resources_modified_since(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        let list = self
            .list_resources(
                api_version,
                kind,
                namespace,
                crate::datastore::ResourceListQuery::all(),
            )
            .await?;
        let event_type = Self::snapshot_replay_event_type(since_rv);
        Ok(list
            .items
            .into_iter()
            .filter(|resource| resource.resource_version > since_rv)
            .map(|resource| CatchUpResource {
                resource,
                event_type: std::borrow::Cow::Borrowed(event_type),
            })
            .collect())
    }

    async fn advance_resource_version_after(&self, min_rv: i64) -> Result<i64> {
        self.observe_rv(min_rv);
        Ok(self.current_rv.load(Ordering::Relaxed))
    }

    async fn list_namespace_resources(&self, _namespace: &str) -> Result<Vec<Resource>> {
        Ok(Vec::new())
    }

    async fn list_namespace_resources_of_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>> {
        Ok(self
            .list_resources(
                "v1",
                kind,
                Some(namespace),
                crate::datastore::ResourceListQuery::all(),
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
        Ok(self.list_namespace_resources(namespace).await?.len() as i64)
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

    async fn current_watch_replay_position(&self) -> Result<WatchReplayPosition> {
        Ok(WatchReplayPosition {
            resource_version: self.current_rv.load(Ordering::Relaxed),
            event_id: self.next_event_id.load(Ordering::Relaxed).saturating_sub(1),
            resource_version_filter_through_event_id: 0,
        })
    }

    async fn snapshot_resources_at_position(
        &self,
        targets: &[WatchTarget],
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        position: WatchReplayPosition,
    ) -> Result<SnapshotAtRv> {
        let high_water_event_id = self.next_event_id.load(Ordering::Relaxed).saturating_sub(1);
        if position.event_id > high_water_event_id
            || position.resource_version_filter_through_event_id > high_water_event_id
            || (position.event_id == 0
                && position.resource_version_filter_through_event_id == 0
                && position.resource_version > self.current_rv.load(Ordering::Relaxed))
        {
            return Ok(SnapshotAtRv::Expired);
        }

        let history = self
            .event_history
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for target in targets {
            let boundaries = worker_replay_boundaries(&history, target);
            if ReplayRetentionBoundary::classify_all(boundaries.iter().copied(), position)
                == ReplayAvailability::Expired
                || !boundaries.is_empty()
            {
                return Ok(SnapshotAtRv::Expired);
            }
        }

        let mut resources = HashMap::<ReflectedResourceKey, Resource>::new();
        for (event_id, event) in &history.events {
            if !watch_event_matches_targets(event, targets) {
                continue;
            }
            let Some(resource_version) = event.resource_version() else {
                return Ok(SnapshotAtRv::Expired);
            };
            if !crate::datastore::position_membership::position_represents(
                position,
                *event_id,
                resource_version,
            ) {
                continue;
            }
            let Some(resource) =
                catchup_resource_from_watch_event(event).map(|catchup| catchup.resource)
            else {
                return Ok(SnapshotAtRv::Expired);
            };
            let key = ReflectedResourceKey {
                api_version: resource.api_version.clone(),
                kind: resource.kind.clone(),
                namespace: resource.namespace.clone(),
                name: resource.name.clone(),
            };
            if event.event_type == EventType::Deleted {
                resources.remove(&key);
            } else {
                resources.insert(key, resource);
            }
        }
        drop(history);

        let mut items = crate::datastore::position_membership::apply_membership_selectors(
            resources.into_values().collect(),
            label_selector,
            field_selector,
        )?;
        crate::datastore::position_membership::sort_for_watch_targets(&mut items, targets);
        Ok(SnapshotAtRv::List(ResourceList {
            items,
            resource_version: position.resource_version,
            watch_replay_position: Some(position),
            continue_token: None,
            remaining_item_count: None,
        }))
    }

    async fn list_raw_watch_events_since_checked_bounded(
        &self,
        _targets: &[WatchTarget],
        _since_rv: i64,
        _limit: std::num::NonZeroUsize,
    ) -> Result<WatchReplayRead<RawWatchEvent>> {
        self.unsupported("list_raw_watch_events_since_checked_bounded")
    }

    async fn list_all_watch_events_since(&self, _since_rv: i64) -> Result<Vec<CatchUpResource>> {
        Ok(Vec::new())
    }

    async fn list_all_watch_events_since_paged(
        &self,
        _since_rv: i64,
        _after_resource_version: i64,
        _after_id: i64,
        _limit: std::num::NonZeroUsize,
    ) -> Result<Vec<(i64, CatchUpResource)>> {
        Ok(Vec::new())
    }

    async fn list_deleted_watch_events_since(
        &self,
        _since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        Ok(Vec::new())
    }

    async fn allocate_node_subnet(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<NodeSubnet> {
        self.cluster_api
            .allocate_node_subnet(node_name, cluster_cidr, node_ip)
            .await
    }

    async fn update_node_peer_attributes(
        &self,
        _node_name: &str,
        _mode: crate::controllers::annotations::NodePeerMode,
        _hostport_range: Option<crate::networking::types::HostPortRange>,
    ) -> Result<()> {
        Ok(())
    }

    async fn update_node_dataplane(
        &self,
        _metadata: crate::networking::wireguard::DataplanePeerMetadata,
    ) -> Result<()> {
        self.unsupported("update_node_dataplane")
    }

    async fn get_node_dataplane(
        &self,
        node_name: &str,
    ) -> Result<Option<crate::networking::wireguard::DataplanePeerMetadata>> {
        self.cluster_api.get_node_dataplane(node_name).await
    }

    async fn list_pod_cleanup_intents_for_node(
        &self,
        node_name: &str,
    ) -> Result<Vec<PodCleanupIntent>> {
        self.cluster_api
            .list_pod_cleanup_intents_for_node(node_name)
            .await
    }

    async fn delete_pod_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        self.cluster_api
            .delete_pod_cleanup_intent(node_name, namespace, pod_name, pod_uid, reason)
            .await
    }

    async fn get_node_subnet(&self, node_name: &str) -> Result<Option<NodeSubnet>> {
        self.cluster_api.get_node_subnet(node_name).await
    }

    async fn list_peer_subnets(&self, my_node_name: &str) -> Result<Vec<NodeSubnet>> {
        self.cluster_api.list_peer_subnets(my_node_name).await
    }

    async fn delete_node_subnet(&self, _node_name: &str) -> Result<()> {
        Ok(())
    }

    async fn pod_slot_try_admit(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        node_name: &str,
    ) -> Result<PodSlotAdmissionResult> {
        self.node_local
            .admit_pod_runtime(pod_uid, namespace, pod_name, node_name)
            .await?;
        Ok(PodSlotAdmissionResult::Admitted {
            resource_version: 0,
        })
    }

    async fn pod_slot_mark_terminating(
        &self,
        _namespace: &str,
        _pod_name: &str,
        _pod_uid: &str,
        _node_name: &str,
    ) -> Result<()> {
        Ok(())
    }

    async fn pod_slot_clear_if_uid(
        &self,
        _namespace: &str,
        _pod_name: &str,
        pod_uid: &str,
        _node_name: &str,
    ) -> Result<()> {
        self.node_local.delete_pod_runtime_for_uid(pod_uid).await
    }

    fn subscribe_pod_slot_admissions(&self) -> broadcast::Receiver<PodSlotAdmissionEvent> {
        self.node_local.subscribe_pod_slot_admissions()
    }

    async fn patch_resource_latest(
        &self,
        _api_version: &str,
        _kind: &str,
        _namespace: Option<&str>,
        _name: &str,
        _patch_kind: PatchKind,
        _patch: Value,
    ) -> Result<Option<Resource>> {
        self.unsupported("patch_resource_latest")
    }

    async fn patch_resource_latest_with_preconditions(
        &self,
        _api_version: &str,
        _kind: &str,
        _namespace: Option<&str>,
        _name: &str,
        _request: ResourcePatchRequest,
    ) -> Result<Option<Resource>> {
        self.unsupported("patch_resource_latest_with_preconditions")
    }

    async fn get_pod_network(&self, sandbox_id: &str) -> Result<Option<PodNetworkEndpoint>> {
        self.node_local.get_network_for_sandbox(sandbox_id).await
    }

    async fn get_pod_network_for_pod(
        &self,
        _namespace: &str,
        _pod_name: &str,
        pod_uid: &str,
    ) -> Result<Option<PodNetworkEndpoint>> {
        self.node_local.get_network_for_uid(pod_uid).await
    }

    async fn ipam_allocate_and_record_pod_network(
        &self,
        sandbox_id: &str,
        pod: &crate::pod_identity::PodIdentity,
        subnet_base_int: u32,
        subnet_size: u32,
        veth_host: &str,
        netns_path: &str,
    ) -> Result<(String, u32)> {
        self.node_local
            .reserve_ip_and_insert_network(PodNetworkAllocationRequest::new(
                sandbox_id,
                PodNetworkAllocationPod::new(&pod.namespace, &pod.name, &pod.uid),
                PodNetworkAllocationSubnet::new(subnet_base_int, subnet_size),
                PodNetworkAllocationLink::new(veth_host, netns_path),
            ))
            .await
    }

    async fn list_sandboxes(&self) -> Result<Vec<SandboxRef>> {
        Ok(self
            .node_local
            .list_pod_runtime()
            .await?
            .into_iter()
            .filter_map(|row| {
                Some(SandboxRef {
                    namespace: row.namespace,
                    pod_name: row.pod_name,
                    pod_uid: row.pod_uid,
                    sandbox_id: row.sandbox_id?,
                })
            })
            .collect())
    }

    async fn list_pod_network_sandbox_ids(&self) -> Result<Vec<String>> {
        self.node_local.list_networks().await
    }

    async fn watch_events_gc_prunable_count(
        &self,
        _max_rows: i64,
        _batch_cap: i64,
    ) -> Result<usize> {
        Ok(0)
    }

    async fn gc_watch_events(&self, _max_rows: i64, _batch_cap: i64) -> Result<usize> {
        Ok(0)
    }

    async fn pod_endpoint_get_by_pod_ip(
        &self,
        pod_ip: std::net::Ipv4Addr,
    ) -> Result<Option<PodEndpointRow>> {
        self.node_local.get_endpoint_by_pod_ip(pod_ip).await
    }

    async fn pod_endpoint_list_all(&self) -> Result<Vec<PodEndpointRow>> {
        self.node_local.list_endpoints_all().await
    }

    fn subscribe_pod_endpoints(&self) -> broadcast::Receiver<PodEndpointEvent> {
        self.node_local.subscribe_pod_endpoints()
    }

    async fn get_klights_meta(&self, key: &str) -> Result<Option<String>> {
        self.node_local.get_node_meta(key).await
    }

    async fn set_klights_meta(&self, key: &str, value: &str) -> Result<()> {
        self.node_local.set_node_meta(key, value).await
    }

    async fn get_applied_outbox(
        &self,
        _idempotency_key: &str,
    ) -> Result<Option<AppliedOutboxRecord>> {
        Ok(None)
    }

    async fn insert_applied_outbox(&self, _record: AppliedOutboxRecord) -> Result<bool> {
        Ok(false)
    }

    async fn apply_outbox_transactionally(
        &self,
        _idempotency_key: &str,
        _operation: &str,
        _payload: &[u8],
        _authoring_node: &str,
    ) -> std::result::Result<
        crate::kubelet::outbox::OutboxApplyResult,
        crate::kubelet::outbox::OutboxApplyError,
    > {
        Err(crate::kubelet::outbox::OutboxApplyError::Retryable(
            "worker-local store does not support leader-side outbox apply".to_string(),
        ))
    }

    async fn build_log_apply_commit_for_outbox(
        &self,
        _idempotency_key: &str,
        _operation: &str,
        _payload: &[u8],
        _authoring_node: &str,
    ) -> std::result::Result<
        crate::datastore::sqlite::BuildOutboxOutcome,
        crate::kubelet::outbox::OutboxApplyError,
    > {
        Err(crate::kubelet::outbox::OutboxApplyError::Retryable(
            "worker-local store does not support leader-side outbox build".to_string(),
        ))
    }

    async fn gc_applied_outbox(&self, _now_ms: i64, _ttl_ms: i64) -> Result<usize> {
        Ok(0)
    }

    async fn applied_outbox_gc_prunable_count(&self, _cutoff_ms: i64) -> Result<usize> {
        Ok(0)
    }

    /// TO-BE-CLEANUP: legacy replicated StorageCommand apply test support.
    #[cfg(test)]
    async fn apply_replicated_command(
        &self,
        _command: StorageCommand,
        _meta: CommandMeta,
    ) -> Result<()> {
        self.unsupported("apply_replicated_command")
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
        let resource = self.cluster_api.get_resource(key).await?;
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
        let mut list = self
            .cluster_api
            .list_resources(ListRequest {
                api_version: api_version.to_string(),
                kind: kind.to_string(),
                namespace: namespace.map(str::to_string),
                label_selector: label_selector.map(str::to_string),
                field_selector,
                limit: None,
                continue_token: None,
            })
            .await?;
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
    #[cfg(test)]
    async fn apply_replicated_command(
        &self,
        _command: StorageCommand,
        _meta: CommandMeta,
    ) -> Result<()> {
        self.unsupported("apply_replicated_command")
    }

    async fn replace_replicated_resource_state(
        &self,
        _entries: Vec<crate::log_apply::LogApplyCommit>,
        _current_rv: i64,
        _watch_event_high_water: Option<i64>,
        _watch_replay_floors: Option<Vec<crate::datastore::WatchReplayFloor>>,
        _metadata: Option<crate::datastore::ReplicatedSnapshotMetadata>,
    ) -> Result<()> {
        self.unsupported("replace_replicated_resource_state")
    }

    async fn apply_log_apply_commit(
        &self,
        _commit: crate::log_apply::LogApplyCommit,
    ) -> Result<()> {
        self.unsupported("apply_log_apply_commit")
    }

    async fn apply_raft_log_apply_commit(
        &self,
        _commit: crate::log_apply::LogApplyCommit,
    ) -> Result<crate::datastore::raft::types::StorageCommandResult> {
        self.unsupported("apply_raft_log_apply_commit")
    }

    async fn current_log_apply_index(&self) -> Result<i64> {
        Ok(0)
    }

    #[cfg(test)]
    async fn apply_replicated_create_resource(
        &self,
        _api_version: &str,
        _kind: &str,
        _namespace: Option<&str>,
        _name: &str,
        _data: Value,
        _options: crate::datastore::types::ReplicatedCreateOptions,
    ) -> Result<Resource> {
        self.unsupported("apply_replicated_create_resource")
    }
}

#[async_trait]
impl crate::datastore::PodWorkqueueStore for WorkerStoreAdapter {
    async fn pod_workqueue_enqueue(
        &self,
        kind: PodWorkqueueKind,
        pod: &crate::pod_identity::PodIdentity,
        payload: Value,
        attempt_count: i64,
        min_delay_ms: i64,
        last_error: Option<&str>,
    ) -> Result<()> {
        self.node_local
            .enqueue_workqueue(kind, pod, payload, attempt_count, min_delay_ms, last_error)
            .await
    }

    async fn pod_workqueue_peek_next_due(&self) -> Result<Option<i64>> {
        self.node_local.peek_workqueue_next_due().await
    }

    async fn pod_workqueue_claim_due(&self, now_ms: i64) -> Result<Option<PodWorkqueueEntry>> {
        self.node_local.claim_workqueue_due(now_ms).await
    }

    async fn pod_workqueue_complete(&self, id: i64) -> Result<()> {
        self.node_local.complete_workqueue(id).await
    }

    async fn pod_workqueue_record_failure(
        &self,
        row: PodWorkqueueEntry,
        min_delay_ms: i64,
        error: &str,
    ) -> Result<()> {
        let pod = crate::pod_identity::PodIdentity::new(&row.namespace, &row.name, &row.uid);
        self.node_local
            .enqueue_workqueue(
                row.kind,
                &pod,
                row.payload,
                row.attempt_count.saturating_add(1),
                min_delay_ms,
                Some(error),
            )
            .await
    }

    async fn pod_workqueue_dead_letter(&self, id: i64, _error: &str) -> Result<()> {
        self.node_local.complete_workqueue(id).await
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
impl crate::datastore::MetaStore for WorkerStoreAdapter {
    async fn get_klights_meta(&self, key: &str) -> Result<Option<String>> {
        self.node_local.get_node_meta(key).await
    }

    async fn set_klights_meta(&self, key: &str, value: &str) -> Result<()> {
        self.node_local.set_node_meta(key, value).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::client::local::LocalApiClient;
    use crate::control_plane::client::{
        CacheScope, ConfigMap, Node, Pod, ResourceEvent, Secret, WatchStream,
    };
    use crate::datastore::DatastoreBackend;
    use crate::kubelet::pod_lifecycle_router::{
        PodLifecycleDiagnostics, PodLifecycleRouteBackend, PodLifecycleRouteError,
        PodLifecycleRouteMode,
    };
    use crate::task_supervisor::{TaskCategoryConfig, TaskSupervisor};
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn worker_replay_position_filter_matches_shared_position_semantics() {
        struct Case {
            name: &'static str,
            position: WatchReplayPosition,
            event_id: i64,
            resource_version: Option<i64>,
            expected: bool,
        }

        let cases = [
            Case {
                name: "scalar filters older rv",
                position: WatchReplayPosition::from_resource_version(50),
                event_id: 11,
                resource_version: Some(40),
                expected: false,
            },
            Case {
                name: "scalar includes newer rv",
                position: WatchReplayPosition::from_resource_version(50),
                event_id: 11,
                resource_version: Some(51),
                expected: true,
            },
            Case {
                name: "exact includes later lower rv",
                position: WatchReplayPosition {
                    resource_version: 50,
                    event_id: 10,
                    resource_version_filter_through_event_id: 0,
                },
                event_id: 11,
                resource_version: Some(40),
                expected: true,
            },
            Case {
                name: "composite filters lower rv through anchor",
                position: WatchReplayPosition::from_resource_version_through_event_id(50, 12),
                event_id: 11,
                resource_version: Some(40),
                expected: false,
            },
            Case {
                name: "composite includes lower rv after anchor",
                position: WatchReplayPosition::from_resource_version_through_event_id(50, 12),
                event_id: 13,
                resource_version: Some(40),
                expected: true,
            },
            Case {
                name: "missing rv fails closed",
                position: WatchReplayPosition::default(),
                event_id: 1,
                resource_version: None,
                expected: false,
            },
        ];

        for case in cases {
            let event = WatchEvent {
                event_type: EventType::Modified,
                object: Arc::new(serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {
                        "namespace": "default",
                        "name": case.name,
                        "resourceVersion": case.resource_version.map(|rv| rv.to_string()),
                    }
                })),
                encoded_payload: None,
            };
            assert_eq!(
                worker_replay_event_follows_position(case.position, case.event_id, &event),
                case.expected,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn worker_retention_boundary_keeps_newer_position_available() {
        let topic = WatchTopic::new("v1", "ConfigMap");
        let mut history = WorkerWatchHistory::default();
        ReplayRetentionBoundary::retain_exact(
            history
                .floors
                .entry((topic.clone(), Some("default".into())))
                .or_default(),
            WatchReplayPosition {
                resource_version: 10,
                event_id: 40,
                resource_version_filter_through_event_id: 0,
            },
        );
        let target = WatchTarget {
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            scope: WatchTargetScope::Namespaced(Some("default".into())),
        };

        assert_eq!(
            ReplayRetentionBoundary::classify_all(
                worker_replay_boundaries(&history, &target),
                WatchReplayPosition {
                    resource_version: 10,
                    event_id: 40,
                    resource_version_filter_through_event_id: 0,
                },
            ),
            ReplayAvailability::Available
        );
    }

    struct FailingPodLifecycleBackend {
        remaining_failures: AtomicUsize,
        route_attempts: AtomicUsize,
    }

    impl FailingPodLifecycleBackend {
        fn new(failures: usize) -> Self {
            Self {
                remaining_failures: AtomicUsize::new(failures),
                route_attempts: AtomicUsize::new(0),
            }
        }

        fn route_attempts(&self) -> usize {
            self.route_attempts.load(Ordering::Acquire)
        }
    }

    fn configure_successful_pod_router(adapter: &WorkerStoreAdapter) {
        adapter.set_pod_lifecycle_router(Arc::new(PodLifecycleRouter::new_test_backend(Arc::new(
            FailingPodLifecycleBackend::new(0),
        ))));
    }

    #[async_trait]
    impl PodLifecycleRouteBackend for FailingPodLifecycleBackend {
        async fn route(
            &self,
            _message: LifecycleMessage,
        ) -> std::result::Result<(), PodLifecycleRouteError> {
            self.route_attempts.fetch_add(1, Ordering::AcqRel);
            if self
                .remaining_failures
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                Err(PodLifecycleRouteError::SendError(
                    "injected worker mirror route failure".to_string(),
                ))
            } else {
                Ok(())
            }
        }

        fn try_route_nonblocking(&self, _message: LifecycleMessage) {}

        fn mode(&self) -> PodLifecycleRouteMode {
            PodLifecycleRouteMode::Actor
        }

        async fn remove_pod_state(&self, _key: &PodLifecycleKey) -> bool {
            false
        }

        async fn diagnostics(&self) -> PodLifecycleDiagnostics {
            PodLifecycleDiagnostics {
                mode: PodLifecycleRouteMode::Actor,
                actor_states: Vec::new(),
                recent_trace: Vec::new(),
                active_pod_count: 0,
            }
        }

        async fn active_pod_count(&self) -> usize {
            0
        }
    }

    #[tokio::test]
    async fn failed_local_pod_route_is_not_published_by_worker_mirror() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:worker-store-route-apply-gate-test",
        )
        .await
        .expect("open node-local");
        let adapter = WorkerStoreAdapter::new(
            Arc::new(HandoffLeaderApi),
            node_local,
            "worker-a".to_string(),
        );
        let backend = Arc::new(FailingPodLifecycleBackend::new(1));
        adapter.set_pod_lifecycle_router(Arc::new(PodLifecycleRouter::new_test_backend(
            backend.clone(),
        )));
        let mut watch = adapter.watch_topic(WatchTopic::new("v1", "Pod"));

        let result = adapter
            .publish_watch_from_mirror(WatchEvent::added(serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "must-replay",
                    "uid": "uid-must-replay",
                    "resourceVersion": "42"
                },
                "spec": {"nodeName": "worker-a"}
            })))
            .await;

        assert!(
            result.is_err(),
            "the lifecycle routing failure must propagate"
        );
        assert!(
            watch.try_recv().is_err(),
            "a Pod event whose lifecycle route failed must not be locally published"
        );
        assert_eq!(
            adapter.current_rv.load(Ordering::Acquire),
            0,
            "a failed route must not advance worker mirror state"
        );
        assert_eq!(backend.route_attempts(), 1);
    }

    #[tokio::test]
    async fn failed_snapshot_pod_route_retries_without_committing_reflector_or_membership() {
        let cluster_db = crate::datastore::test_support::in_memory().await;
        cluster_db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "snapshot-replay",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "snapshot-replay",
                        "uid": "uid-snapshot-replay"
                    },
                    "spec": {
                        "nodeName": "worker-a",
                        "containers": [{"name": "app", "image": "busybox"}]
                    }
                }),
            )
            .await
            .expect("create snapshot Pod");
        let cluster_api = Arc::new(LocalApiClient::new(
            Arc::new(cluster_db),
            "worker-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ));
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:worker-store-snapshot-apply-gate-test",
        )
        .await
        .expect("open node-local");
        let adapter = WorkerStoreAdapter::new(cluster_api, node_local, "worker-a".to_string());
        let backend = Arc::new(FailingPodLifecycleBackend::new(1));
        adapter.set_pod_lifecycle_router(Arc::new(PodLifecycleRouter::new_test_backend(
            backend.clone(),
        )));
        let req = WatchRequest {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: None,
            label_selector: None,
            field_selector: Some("spec.nodeName=worker-a".to_string()),
            start_resource_version: None,
            start_watch_replay_position: None,
        };
        let mut state = ReflectorState::default();
        let mut membership = crate::watch::SelectorMembership::default();
        let mut watch = adapter.watch_topic(WatchTopic::new("v1", "Pod"));

        let first = adapter
            .reconcile_watch_snapshot(&req, &mut state, Some(&mut membership))
            .await;
        assert!(
            first.is_err(),
            "the initial snapshot route failure must propagate"
        );
        assert!(
            state.resources.is_empty(),
            "reflector state must remain uncommitted"
        );
        assert_eq!(
            membership.len(),
            0,
            "selector membership must remain uncommitted"
        );
        assert_eq!(adapter.current_rv.load(Ordering::Acquire), 0);
        assert!(
            watch.try_recv().is_err(),
            "failed snapshot must not publish"
        );

        adapter
            .reconcile_watch_snapshot(&req, &mut state, Some(&mut membership))
            .await
            .expect("the same snapshot must replay after the route recovers");
        let replayed = watch.try_recv().expect("replayed snapshot event");
        assert_eq!(replayed.event_type, EventType::Added);
        assert_eq!(
            replayed
                .object
                .pointer("/metadata/name")
                .and_then(Value::as_str),
            Some("snapshot-replay")
        );
        assert_eq!(state.resources.len(), 1);
        assert_eq!(membership.len(), 1);
        assert_eq!(
            backend.route_attempts(),
            2,
            "the failed initial-list event must be routed again on snapshot retry"
        );
    }

    #[test]
    fn is_watch_window_expired_requires_typed_replay_marker() {
        let marked = crate::replication::grpc::watch_replay_expired_status(41, "expired");
        let err = anyhow::Error::from(marked).context("gRPC WatchResources stream failed");
        assert!(
            is_watch_window_expired(&err),
            "typed replay-expired status must trigger a relist"
        );

        let wrapped = crate::replication::grpc::watch_replay_expired_status(42, "expired");
        let err = anyhow::Error::from(wrapped)
            .context("inner wrapper")
            .context("gRPC WatchResources stream failed");
        assert!(
            is_watch_window_expired(&err),
            "typed replay-expired status must be detected through wrappers"
        );

        let legacy = tonic::Status::out_of_range(
            "WatchResources replay window expired: resume rv 42 requires relist",
        );
        let err = anyhow::Error::from(legacy).context("gRPC WatchResources stream failed");
        assert!(
            is_watch_window_expired(&err),
            "exact legacy replay-expired status from pre-marker leaders must trigger a relist"
        );

        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert(
            crate::replication::grpc::WATCH_REPLAY_EXPIRED_REASON_METADATA_KEY,
            tonic::metadata::MetadataValue::from_static(
                crate::replication::grpc::WATCH_REPLAY_EXPIRED_REASON,
            ),
        );
        let malformed_marker = tonic::Status::with_details_and_metadata(
            tonic::Code::OutOfRange,
            "expired",
            bytes::Bytes::from_static(b"not protobuf"),
            metadata,
        );

        let cases = [
            (
                tonic::Status::out_of_range("expired but unmarked"),
                "unmarked OutOfRange",
            ),
            (
                tonic::Status::out_of_range("message exceeds configured maximum size"),
                "message-size OutOfRange",
            ),
            (
                tonic::Status::unavailable("transport gone"),
                "non-OutOfRange gRPC error",
            ),
            (malformed_marker, "malformed replay-expired details"),
        ];
        for (status, name) in cases {
            let err = anyhow::Error::from(status).context("gRPC WatchResources stream failed");
            assert!(
                !is_watch_window_expired(&err),
                "{name} must not trigger a relist"
            );
        }

        assert!(
            !is_watch_window_expired(&anyhow!("some other failure")),
            "non-tonic errors must not trigger a relist"
        );
    }

    fn reflected_resource(name: &str, uid: &str, rv: i64) -> Resource {
        Resource {
            id: rv,
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: name.to_string(),
            uid: uid.to_string(),
            resource_version: rv,
            data: Arc::new(serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "namespace": "default",
                    "name": name,
                    "uid": uid,
                    "resourceVersion": rv.to_string()
                }
            })),
        }
    }

    #[test]
    fn reflector_relist_diff_synthesizes_missed_delete_at_snapshot_rv() {
        let mut state = ReflectorState::default();
        let initial = state.replace_snapshot(vec![reflected_resource("removed", "uid-1", 41)], 41);
        assert_eq!(initial.len(), 1);
        assert_eq!(initial[0].event_type, EventType::Added);

        let replacement = state.replace_snapshot(Vec::new(), 52);
        assert_eq!(replacement.len(), 1);
        assert_eq!(replacement[0].event_type, EventType::Deleted);
        assert_eq!(replacement[0].resource_version(), Some(52));
        assert_eq!(
            replacement[0]
                .object
                .pointer("/metadata/name")
                .and_then(Value::as_str),
            Some("removed")
        );
    }

    #[test]
    fn reflector_snapshot_keeps_distinct_objects_with_the_same_rv() {
        let mut state = ReflectorState::default();
        let events = state.replace_snapshot(
            vec![
                reflected_resource("first", "uid-first", 41),
                reflected_resource("second", "uid-second", 41),
            ],
            41,
        );

        assert_eq!(events.len(), 2);
        assert_eq!(
            events
                .iter()
                .map(|event| event
                    .object
                    .pointer("/metadata/name")
                    .and_then(Value::as_str)
                    .unwrap())
                .collect::<std::collections::HashSet<_>>(),
            std::collections::HashSet::from(["first", "second"])
        );
    }

    #[test]
    fn reflector_snapshot_diff_order_is_stable_by_resource_key() {
        let mut state = ReflectorState::default();
        let names = [
            "hotel", "alpha", "golf", "bravo", "foxtrot", "charlie", "echo", "delta",
        ];
        let events = state.replace_snapshot(
            names
                .iter()
                .map(|name| reflected_resource(name, &format!("uid-{name}"), 41))
                .collect(),
            41,
        );

        assert_eq!(
            events
                .iter()
                .map(|event| event.object["metadata"]["name"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec![
                "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"
            ],
            "authoritative relist diffs must be deterministic for replay and tests"
        );
    }

    #[test]
    fn reflector_relist_replaces_same_name_uid_with_delete_then_add() {
        let mut state = ReflectorState::default();
        state.replace_snapshot(vec![reflected_resource("same-name", "uid-old", 41)], 41);

        let replacement =
            state.replace_snapshot(vec![reflected_resource("same-name", "uid-new", 52)], 52);

        assert_eq!(
            replacement
                .iter()
                .map(|event| event.event_type)
                .collect::<Vec<_>>(),
            vec![EventType::Deleted, EventType::Added]
        );
        assert_eq!(
            replacement[0]
                .object
                .pointer("/metadata/uid")
                .and_then(Value::as_str),
            Some("uid-old")
        );
        assert_eq!(
            replacement[1]
                .object
                .pointer("/metadata/uid")
                .and_then(Value::as_str),
            Some("uid-new")
        );
    }

    #[test]
    fn reflector_relist_marks_same_uid_changes_modified_and_ignores_unchanged_objects() {
        let mut state = ReflectorState::default();
        let initial = reflected_resource("updated", "uid-stable", 41);
        state.replace_snapshot(vec![initial.clone()], 41);

        assert!(state.replace_snapshot(vec![initial], 41).is_empty());

        let mut updated = reflected_resource("updated", "uid-stable", 52);
        Arc::make_mut(&mut updated.data)
            .as_object_mut()
            .unwrap()
            .insert("data".to_string(), serde_json::json!({"key": "new"}));
        let events = state.replace_snapshot(vec![updated], 52);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, EventType::Modified);
        assert_eq!(events[0].resource_version(), Some(52));
    }

    #[derive(Default)]
    struct HandoffLeaderApi;

    #[async_trait]
    impl LeaderApiClient for HandoffLeaderApi {
        async fn get_resource(&self, key: ResourceKey) -> Result<Option<Resource>> {
            if key.api_version == "v1" && key.kind == "Namespace" && key.name == "fresh-events" {
                return Ok(None);
            }
            if key.api_version == "v1" && key.kind == "Pod" && key.name == "cached-deleted" {
                return Ok(Some(Resource {
                    id: 1,
                    api_version: "v1".to_string(),
                    kind: "Pod".to_string(),
                    namespace: Some("default".to_string()),
                    name: "cached-deleted".to_string(),
                    uid: "uid-cached".to_string(),
                    resource_version: 12,
                    data: Arc::new(serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {
                            "namespace": "default",
                            "name": "cached-deleted",
                            "uid": "uid-cached",
                            "resourceVersion": "12"
                        },
                        "spec": {
                            "nodeName": "worker-a",
                            "containers": [{"name": "app", "image": "nginx"}]
                        }
                    })),
                }));
            }
            unreachable!("handoff test does not use get_resource for {key:?}")
        }

        async fn get_resource_fresh(&self, key: ResourceKey) -> Result<Option<Resource>> {
            if key.api_version == "v1" && key.kind == "Namespace" && key.name == "fresh-events" {
                return Ok(Some(Resource {
                    id: 2,
                    api_version: "v1".to_string(),
                    kind: "Namespace".to_string(),
                    namespace: None,
                    name: "fresh-events".to_string(),
                    uid: "uid-fresh-events".to_string(),
                    resource_version: 13,
                    data: Arc::new(serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "Namespace",
                        "metadata": {
                            "name": "fresh-events",
                            "uid": "uid-fresh-events",
                            "resourceVersion": "13"
                        },
                        "status": {"phase": "Active"}
                    })),
                }));
            }
            if key.api_version == "v1" && key.kind == "Pod" && key.name == "cached-deleted" {
                return Ok(None);
            }
            self.get_resource(key).await
        }

        async fn list_resources(&self, req: ListRequest) -> Result<ResourceList> {
            let resource_version = if req.api_version == "v1" && req.kind == "Pod" {
                assert_eq!(
                    req.field_selector.as_deref(),
                    Some("spec.nodeName=worker-a")
                );
                41
            } else {
                0
            };
            Ok(ResourceList {
                items: Vec::new(),
                resource_version,
                watch_replay_position: (resource_version > 0).then_some(WatchReplayPosition {
                    resource_version,
                    event_id: 91,
                    resource_version_filter_through_event_id: 0,
                }),
                continue_token: None,
                remaining_item_count: None,
            })
        }

        async fn watch_resources(&self, req: WatchRequest) -> Result<WatchStream<ResourceEvent>> {
            if req.api_version == "v1" && req.kind == "Pod" {
                assert_eq!(req.start_resource_version, Some(41));
                assert_eq!(
                    req.start_watch_replay_position,
                    Some(WatchReplayPosition {
                        resource_version: 41,
                        event_id: 91,
                        resource_version_filter_through_event_id: 0,
                    })
                );
                let event = ResourceEvent {
                    event: WatchEvent::modified(serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {
                            "namespace": "default",
                            "name": "bound-during-handoff",
                            "uid": "uid-handoff",
                            "resourceVersion": "42"
                        },
                        "spec": {
                            "nodeName": "worker-a",
                            "containers": [{"name": "app", "image": "nginx"}]
                        },
                        "status": {"phase": "Pending"}
                    })),
                    resume_position: Some(WatchReplayPosition {
                        resource_version: 42,
                        event_id: 92,
                        resource_version_filter_through_event_id: 0,
                    }),
                };
                return Ok(Box::pin(futures::stream::once(async move { Ok(event) })));
            }
            Ok(Box::pin(futures::stream::pending()))
        }

        async fn wait_cache_ready(&self, _scope: CacheScope) -> Result<()> {
            Ok(())
        }

        async fn get_pod(&self, _ns: &str, _name: &str) -> Result<Option<Pod>> {
            unreachable!("handoff test does not use get_pod")
        }

        async fn get_pod_for_uid(&self, _ns: &str, _name: &str, _uid: &str) -> Result<Option<Pod>> {
            unreachable!("handoff test does not use get_pod_for_uid")
        }

        async fn watch_pods_on_node(&self, _node_name: &str) -> Result<WatchStream<Pod>> {
            unreachable!("handoff test does not use watch_pods_on_node")
        }

        async fn list_pods_on_node(&self, _node_name: &str) -> Result<Vec<Pod>> {
            unreachable!("handoff test does not use list_pods_on_node")
        }

        async fn get_configmap(&self, _ns: &str, _name: &str) -> Result<Option<ConfigMap>> {
            unreachable!("handoff test does not use get_configmap")
        }

        async fn get_secret(&self, _ns: &str, _name: &str) -> Result<Option<Secret>> {
            unreachable!("handoff test does not use get_secret")
        }

        async fn get_node(&self, _name: &str) -> Result<Node> {
            unreachable!("handoff test does not use get_node")
        }

        async fn watch_node(&self, _name: &str) -> Result<WatchStream<Node>> {
            unreachable!("handoff test does not use watch_node")
        }

        async fn allocate_node_subnet(
            &self,
            _node_name: &str,
            _cluster_cidr: &str,
            _node_ip: &str,
        ) -> Result<NodeSubnet> {
            unreachable!("handoff test does not use allocate_node_subnet")
        }

        async fn get_node_subnet(&self, _node_name: &str) -> Result<Option<NodeSubnet>> {
            unreachable!("handoff test does not use get_node_subnet")
        }

        async fn list_peer_subnets(&self, _my_node_name: &str) -> Result<Vec<NodeSubnet>> {
            unreachable!("handoff test does not use list_peer_subnets")
        }

        async fn get_node_dataplane(
            &self,
            _node_name: &str,
        ) -> Result<Option<crate::networking::wireguard::DataplanePeerMetadata>> {
            unreachable!("handoff test does not use get_node_dataplane")
        }

        async fn list_pod_cleanup_intents_for_node(
            &self,
            _node_name: &str,
        ) -> Result<Vec<PodCleanupIntent>> {
            unreachable!("handoff test does not use list_pod_cleanup_intents_for_node")
        }

        async fn delete_pod_cleanup_intent(
            &self,
            _node_name: &str,
            _namespace: &str,
            _pod_name: &str,
            _pod_uid: &str,
            _reason: &str,
        ) -> Result<()> {
            unreachable!("handoff test does not use delete_pod_cleanup_intent")
        }

        async fn apply_outbox(
            &self,
            _idempotency_key: &str,
            _operation: crate::kubelet::outbox::payload::OutboxOperation,
            _payload: bytes::Bytes,
            _client_id: &str,
            _stream_id: i64,
            _stream_seq: i64,
        ) -> std::result::Result<
            crate::kubelet::outbox::OutboxApplyResult,
            crate::kubelet::outbox::OutboxApplyError,
        > {
            unreachable!("handoff test does not use apply_outbox")
        }
    }

    #[tokio::test]
    async fn failed_pod_route_reconnects_and_replays_from_prior_exact_position() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor.clone(),
            None,
            "sqlite:worker-store-route-replay-position-test",
        )
        .await
        .expect("open node-local");
        let adapter = Arc::new(WorkerStoreAdapter::new(
            Arc::new(HandoffLeaderApi),
            node_local,
            "worker-a".to_string(),
        ));
        let backend = Arc::new(FailingPodLifecycleBackend::new(1));
        adapter.set_pod_lifecycle_router(Arc::new(PodLifecycleRouter::new_test_backend(
            backend.clone(),
        )));
        let mut watch = adapter.watch_topic(WatchTopic::new("v1", "Pod"));
        let cancel = tokio_util::sync::CancellationToken::new();
        let driver_adapter = adapter.clone();
        let driver_supervisor = supervisor.clone();
        let driver_cancel = cancel.clone();
        let handle = supervisor
            .spawn_async(
                crate::task_supervisor::TaskCategory::Network,
                "worker_store_route_replay_position_test",
                async move {
                    driver_adapter
                        .run_watch_mirror(
                            WatchRequest {
                                api_version: "v1".to_string(),
                                kind: "Pod".to_string(),
                                namespace: None,
                                label_selector: None,
                                field_selector: Some("spec.nodeName=worker-a".to_string()),
                                start_resource_version: None,
                                start_watch_replay_position: None,
                            },
                            driver_supervisor,
                            driver_cancel,
                        )
                        .await;
                },
            )
            .await
            .expect("spawn mirror driver");

        let replayed = tokio::time::timeout(std::time::Duration::from_secs(3), watch.recv())
            .await
            .expect("failed route should reconnect and replay")
            .expect("worker Pod watch remains open");
        assert_eq!(
            replayed
                .object
                .pointer("/metadata/name")
                .and_then(Value::as_str),
            Some("bound-during-handoff")
        );
        assert!(
            backend.route_attempts() >= 2,
            "the event must be routed once unsuccessfully, then replayed and routed again"
        );

        cancel.cancel();
        let _ = handle.join().await;
    }

    #[derive(Clone, Copy)]
    enum OpenExpiryMode {
        TypedOnce,
        TypedAlways,
        UnmarkedOnce,
    }

    struct OpenExpiredThenRelistLeaderApi {
        list_count: AtomicUsize,
        watch_count: AtomicUsize,
        watch_attempted: tokio::sync::Notify,
        expiry_mode: OpenExpiryMode,
    }

    impl OpenExpiredThenRelistLeaderApi {
        fn typed_expiry() -> Self {
            Self {
                list_count: AtomicUsize::new(0),
                watch_count: AtomicUsize::new(0),
                watch_attempted: tokio::sync::Notify::new(),
                expiry_mode: OpenExpiryMode::TypedOnce,
            }
        }

        fn repeated_typed_expiry() -> Self {
            Self {
                list_count: AtomicUsize::new(0),
                watch_count: AtomicUsize::new(0),
                watch_attempted: tokio::sync::Notify::new(),
                expiry_mode: OpenExpiryMode::TypedAlways,
            }
        }

        fn unmarked_out_of_range() -> Self {
            Self {
                list_count: AtomicUsize::new(0),
                watch_count: AtomicUsize::new(0),
                watch_attempted: tokio::sync::Notify::new(),
                expiry_mode: OpenExpiryMode::UnmarkedOnce,
            }
        }

        async fn wait_for_watch_attempts(&self, expected: usize) {
            while self.watch_count.load(Ordering::SeqCst) < expected {
                self.watch_attempted.notified().await;
            }
        }
    }

    #[async_trait]
    impl LeaderApiClient for OpenExpiredThenRelistLeaderApi {
        async fn get_resource(&self, key: ResourceKey) -> Result<Option<Resource>> {
            HandoffLeaderApi.get_resource(key).await
        }

        async fn get_resource_fresh(&self, key: ResourceKey) -> Result<Option<Resource>> {
            HandoffLeaderApi.get_resource_fresh(key).await
        }

        async fn list_resources(&self, req: ListRequest) -> Result<ResourceList> {
            if req.api_version != "v1" || req.kind != "Pod" {
                return Ok(ResourceList {
                    items: Vec::new(),
                    resource_version: 0,
                    watch_replay_position: None,
                    continue_token: None,
                    remaining_item_count: None,
                });
            }
            assert_eq!(
                req.field_selector.as_deref(),
                Some("spec.nodeName=worker-a")
            );
            let attempt = self.list_count.fetch_add(1, Ordering::SeqCst);
            let (name, uid, resource_version) = if attempt == 0 {
                ("removed-before-relist", "uid-removed", 41)
            } else {
                ("scheduled-after-relist", "uid-after-relist", 52)
            };
            let items = vec![Resource {
                id: 1,
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: name.to_string(),
                uid: uid.to_string(),
                resource_version,
                data: Arc::new(serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": name,
                        "uid": uid,
                        "resourceVersion": resource_version.to_string()
                    },
                    "spec": {
                        "nodeName": "worker-a",
                        "containers": [{"name": "app", "image": "busybox"}]
                    },
                    "status": {"phase": "Pending"}
                })),
            }];
            Ok(ResourceList {
                items,
                resource_version: if attempt == 0 { 41 } else { 52 },
                watch_replay_position: None,
                continue_token: None,
                remaining_item_count: None,
            })
        }

        async fn watch_resources(&self, req: WatchRequest) -> Result<WatchStream<ResourceEvent>> {
            if req.api_version != "v1" || req.kind != "Pod" {
                return Ok(Box::pin(futures::stream::pending()));
            }
            assert_eq!(
                req.field_selector.as_deref(),
                Some("spec.nodeName=worker-a")
            );
            let attempt = self.watch_count.fetch_add(1, Ordering::SeqCst);
            self.watch_attempted.notify_waiters();
            let should_expire =
                attempt == 0 || matches!(self.expiry_mode, OpenExpiryMode::TypedAlways);
            if should_expire {
                let expected_rv = if attempt == 0 { 41 } else { 52 };
                assert_eq!(req.start_resource_version, Some(expected_rv));
                let status = match self.expiry_mode {
                    OpenExpiryMode::TypedOnce | OpenExpiryMode::TypedAlways => {
                        crate::replication::grpc::watch_replay_expired_status(
                            expected_rv,
                            format!(
                                "WatchResources replay window expired: resume rv {expected_rv} requires relist"
                            ),
                        )
                    }
                    OpenExpiryMode::UnmarkedOnce => {
                        tonic::Status::out_of_range("message exceeds configured maximum size")
                    }
                };
                return Err(anyhow::Error::from(status).context("gRPC WatchResources failed"));
            }
            assert_eq!(
                req.start_resource_version,
                Some(match self.expiry_mode {
                    OpenExpiryMode::TypedOnce | OpenExpiryMode::TypedAlways => 52,
                    OpenExpiryMode::UnmarkedOnce => 41,
                })
            );
            Ok(Box::pin(futures::stream::pending()))
        }

        async fn wait_cache_ready(&self, scope: CacheScope) -> Result<()> {
            HandoffLeaderApi.wait_cache_ready(scope).await
        }

        async fn get_pod(&self, ns: &str, name: &str) -> Result<Option<Pod>> {
            HandoffLeaderApi.get_pod(ns, name).await
        }

        async fn get_pod_for_uid(&self, ns: &str, name: &str, uid: &str) -> Result<Option<Pod>> {
            HandoffLeaderApi.get_pod_for_uid(ns, name, uid).await
        }

        async fn watch_pods_on_node(&self, node_name: &str) -> Result<WatchStream<Pod>> {
            HandoffLeaderApi.watch_pods_on_node(node_name).await
        }

        async fn list_pods_on_node(&self, node_name: &str) -> Result<Vec<Pod>> {
            HandoffLeaderApi.list_pods_on_node(node_name).await
        }

        async fn get_configmap(&self, ns: &str, name: &str) -> Result<Option<ConfigMap>> {
            HandoffLeaderApi.get_configmap(ns, name).await
        }

        async fn get_secret(&self, ns: &str, name: &str) -> Result<Option<Secret>> {
            HandoffLeaderApi.get_secret(ns, name).await
        }

        async fn get_node(&self, name: &str) -> Result<Node> {
            HandoffLeaderApi.get_node(name).await
        }

        async fn watch_node(&self, name: &str) -> Result<WatchStream<Node>> {
            HandoffLeaderApi.watch_node(name).await
        }

        async fn allocate_node_subnet(
            &self,
            node_name: &str,
            cluster_cidr: &str,
            node_ip: &str,
        ) -> Result<NodeSubnet> {
            HandoffLeaderApi
                .allocate_node_subnet(node_name, cluster_cidr, node_ip)
                .await
        }

        async fn get_node_subnet(&self, node_name: &str) -> Result<Option<NodeSubnet>> {
            HandoffLeaderApi.get_node_subnet(node_name).await
        }

        async fn list_peer_subnets(&self, my_node_name: &str) -> Result<Vec<NodeSubnet>> {
            HandoffLeaderApi.list_peer_subnets(my_node_name).await
        }

        async fn get_node_dataplane(
            &self,
            node_name: &str,
        ) -> Result<Option<crate::networking::wireguard::DataplanePeerMetadata>> {
            HandoffLeaderApi.get_node_dataplane(node_name).await
        }

        async fn list_pod_cleanup_intents_for_node(
            &self,
            node_name: &str,
        ) -> Result<Vec<PodCleanupIntent>> {
            HandoffLeaderApi
                .list_pod_cleanup_intents_for_node(node_name)
                .await
        }

        async fn delete_pod_cleanup_intent(
            &self,
            node_name: &str,
            namespace: &str,
            pod_name: &str,
            pod_uid: &str,
            reason: &str,
        ) -> Result<()> {
            HandoffLeaderApi
                .delete_pod_cleanup_intent(node_name, namespace, pod_name, pod_uid, reason)
                .await
        }

        async fn apply_outbox(
            &self,
            idempotency_key: &str,
            operation: crate::kubelet::outbox::payload::OutboxOperation,
            payload: bytes::Bytes,
            client_id: &str,
            stream_id: i64,
            stream_seq: i64,
        ) -> std::result::Result<
            crate::kubelet::outbox::OutboxApplyResult,
            crate::kubelet::outbox::OutboxApplyError,
        > {
            HandoffLeaderApi
                .apply_outbox(
                    idempotency_key,
                    operation,
                    payload,
                    client_id,
                    stream_id,
                    stream_seq,
                )
                .await
        }
    }

    #[tokio::test]
    async fn worker_pod_get_uses_worker_cache_not_fresh_leader_state() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:worker-store-pod-get-fresh-test",
        )
        .await
        .expect("open node-local");
        let adapter = WorkerStoreAdapter::new(
            Arc::new(HandoffLeaderApi),
            node_local,
            "worker-a".to_string(),
        );

        let pod = adapter
            .get_resource("v1", "Pod", Some("default"), "cached-deleted")
            .await
            .expect("fresh pod get should succeed");

        assert_eq!(
            pod.as_ref().map(|resource| resource.uid.as_str()),
            Some("uid-cached"),
            "worker pod get must read the worker cache and avoid a fresh leader unary read"
        );
    }

    #[tokio::test]
    async fn worker_store_pod_events_use_fresh_namespace_state_before_outbox_enqueue() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:worker-store-event-namespace-fresh-test",
        )
        .await
        .expect("open node-local");
        let adapter = WorkerStoreAdapter::new(
            Arc::new(HandoffLeaderApi),
            node_local.clone(),
            "worker-a".to_string(),
        );
        let outbox = crate::kubelet::outbox::Outbox::new(node_local.clone());
        let pod = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "fresh-events",
                "name": "sysctl-pod",
                "uid": "uid-sysctl-pod"
            },
            "spec": {
                "nodeName": "worker-a",
                "containers": [{"name": "test-container", "image": "busybox"}]
            }
        });

        crate::kubelet::events::emit_pod_event_with_outbox(
            &adapter,
            Some(&outbox),
            crate::kubelet::events::PodEventRecord {
                pod: &pod,
                reason: "Started",
                message: "Started container test-container",
                event_type: "Normal",
                reporting_component: "klights-kubelet",
                reporting_instance: "worker-a",
            },
        )
        .await
        .expect("worker-store event emission should enqueue event");

        let row = node_local
            .claim_next_due_outbox(i64::MAX / 2, 1_000, "event-test")
            .await
            .expect("claim outbox")
            .expect("event outbox row should be enqueued");
        assert_eq!(row.operation, "EventCreate");
        assert_eq!(row.subject_namespace.as_deref(), Some("fresh-events"));
        assert_eq!(row.subject_kind, "Event");
    }

    #[tokio::test]
    async fn worker_pod_lists_are_constrained_to_local_node() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:worker-store-pod-list-local-node-test",
        )
        .await
        .expect("open node-local");
        let adapter = WorkerStoreAdapter::new(
            Arc::new(HandoffLeaderApi),
            node_local,
            "worker-a".to_string(),
        );

        let list = adapter
            .list_resources_page(
                "v1",
                "Pod",
                Some("default"),
                None,
                None,
                ListPageRequest::unbounded(),
            )
            .await
            .expect("list local pods");

        assert_eq!(list.resource_version, 41);
    }

    #[tokio::test]
    async fn worker_list_page_preserves_continuation_metadata() {
        // Regression: list_resources_page used to pass limit/continue_token to
        // the leader *and* re-apply ListPageRequest locally. The leader-side
        // pagination already truncated the page, so the local re-apply saw a
        // list no longer than the limit and cleared the leader-provided
        // continue_token / remaining_item_count — workers' LIST silently dropped
        // the rest of the collection. Pagination must be applied exactly once.
        let cluster_db = crate::datastore::test_support::in_memory().await;
        for name in ["cm-a", "cm-b", "cm-c"] {
            cluster_db
                .create_resource(
                    "v1",
                    "ConfigMap",
                    Some("default"),
                    name,
                    serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {"namespace": "default", "name": name}
                    }),
                )
                .await
                .expect("create configmap");
        }
        let cluster_api = Arc::new(LocalApiClient::new(
            Arc::new(cluster_db.clone()),
            "worker-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ));
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:worker-store-pagination-test",
        )
        .await
        .expect("open node-local");
        let adapter = WorkerStoreAdapter::new(cluster_api, node_local, "worker-a".to_string());

        let first = adapter
            .list_resources_page(
                "v1",
                "ConfigMap",
                Some("default"),
                None,
                None,
                ListPageRequest::try_new(Some(2), None).expect("page request"),
            )
            .await
            .expect("list first page");
        assert_eq!(
            first
                .items
                .iter()
                .map(|item| item.name.as_str())
                .collect::<Vec<_>>(),
            vec!["cm-a", "cm-b"]
        );
        assert_eq!(
            first.continue_token.as_deref(),
            Some("cm-b"),
            "first page must expose a continue token for the remaining item"
        );
        assert_eq!(first.remaining_item_count, Some(1));

        let second = adapter
            .list_resources_page(
                "v1",
                "ConfigMap",
                Some("default"),
                None,
                None,
                ListPageRequest::try_new(Some(2), first.continue_token.clone())
                    .expect("page request"),
            )
            .await
            .expect("list second page");
        assert_eq!(
            second
                .items
                .iter()
                .map(|item| item.name.as_str())
                .collect::<Vec<_>>(),
            vec!["cm-c"]
        );
        assert!(
            second.continue_token.is_none(),
            "final page must not advertise a continue token"
        );
    }

    #[tokio::test]
    async fn worker_watch_replay_respects_resume_resource_version() {
        let cluster_db = crate::datastore::test_support::in_memory().await;
        for name in ["cm-a", "cm-b", "cm-c"] {
            cluster_db
                .create_resource(
                    "v1",
                    "ConfigMap",
                    Some("default"),
                    name,
                    serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {"namespace": "default", "name": name}
                    }),
                )
                .await
                .expect("create configmap");
        }
        let cluster_api = Arc::new(LocalApiClient::new(
            Arc::new(cluster_db.clone()),
            "worker-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ));
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:worker-store-watch-resume-rv-test",
        )
        .await
        .expect("open node-local");
        let adapter = WorkerStoreAdapter::new(cluster_api, node_local, "worker-a".to_string());
        for (index, name) in ["cm-a", "cm-b", "cm-c"].into_iter().enumerate() {
            adapter.publish_watch(WatchEvent::added(serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "namespace": "default",
                    "name": name,
                    "uid": format!("uid-{name}"),
                    "resourceVersion": (index + 1).to_string()
                }
            })));
        }
        let targets = [WatchTarget::namespaced_in_namespace(
            "v1",
            "ConfigMap",
            "default",
        )];
        let limit = std::num::NonZeroUsize::new(3).expect("non-zero limit");

        let first = crate::datastore::WatchStore::list_watch_events_since_checked_bounded(
            &adapter, &targets, 0, limit,
        )
        .await
        .expect("initial watch replay");
        let crate::datastore::WatchReplayRead::Events(first_events) = first else {
            panic!("worker adapter replay should not expire");
        };
        assert_eq!(first_events.len(), 3);
        let max_rv = first_events
            .iter()
            .map(|event| event.resource.resource_version)
            .max()
            .expect("initial replay should have a max rv");

        let second = crate::datastore::WatchStore::list_watch_events_since_checked_bounded(
            &adapter, &targets, max_rv, limit,
        )
        .await
        .expect("resumed watch replay");
        let crate::datastore::WatchReplayRead::Events(second_events) = second else {
            panic!("worker adapter replay should not expire");
        };
        assert!(
            second_events.is_empty(),
            "resumed worker replay must not return resources at or below the resume RV"
        );
    }

    #[tokio::test]
    async fn worker_snapshot_expires_after_compaction_can_hide_unchanged_live_objects() {
        let cluster_db = crate::datastore::test_support::in_memory().await;
        let cluster_api = Arc::new(LocalApiClient::new(
            Arc::new(cluster_db),
            "worker-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ));
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:worker-store-snapshot-compaction-test",
        )
        .await
        .expect("open node-local");
        let adapter = WorkerStoreAdapter::new(cluster_api, node_local, "worker-a".to_string());
        adapter.publish_watch(WatchEvent::added(serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "namespace": "default",
                "name": "unchanged-live",
                "uid": "uid-unchanged-live",
                "resourceVersion": "1"
            }
        })));

        for index in 0..WORKER_WATCH_EVENT_HISTORY_CAPACITY {
            let rv = index + 2;
            adapter.publish_watch(WatchEvent::added(serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "namespace": "default",
                    "name": format!("noise-{index}"),
                    "uid": format!("uid-noise-{index}"),
                    "resourceVersion": rv.to_string()
                }
            })));
        }

        let target = [WatchTarget::namespaced_in_namespace(
            "v1",
            "ConfigMap",
            "default",
        )];
        let position = adapter
            .current_watch_replay_position()
            .await
            .expect("worker replay position");
        assert!(
            position.event_id > WORKER_WATCH_EVENT_HISTORY_CAPACITY as i64,
            "test must force worker watch-history compaction"
        );
        let snapshot = adapter
            .snapshot_resources_at_position(&target, None, None, position)
            .await
            .expect("worker snapshot");
        assert!(
            matches!(snapshot, SnapshotAtRv::Expired),
            "worker snapshots must fail closed after compaction because unchanged live objects can have no retained establishing event"
        );
    }

    #[tokio::test]
    async fn worker_scalar_watch_replay_never_synthesizes_events_from_live_list_state() {
        let cluster_db = crate::datastore::test_support::in_memory().await;
        cluster_db
            .create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "not-durable-worker-history",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {
                        "namespace": "default",
                        "name": "not-durable-worker-history"
                    }
                }),
            )
            .await
            .expect("create configmap");
        let cluster_api = Arc::new(LocalApiClient::new(
            Arc::new(cluster_db),
            "worker-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ));
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:worker-store-no-scalar-snapshot-replay-test",
        )
        .await
        .expect("open node-local");
        let adapter = WorkerStoreAdapter::new(cluster_api, node_local, "worker-a".to_string());

        let replay = crate::datastore::WatchStore::list_watch_events_since(
            &adapter,
            &[WatchTarget::namespaced_in_namespace(
                "v1",
                "ConfigMap",
                "default",
            )],
            0,
        )
        .await
        .expect("scalar replay");

        assert!(
            replay.is_empty(),
            "worker scalar replay must expose only its local durable mirror history; live LIST synthesis is a second establishment algorithm"
        );
    }

    #[tokio::test]
    async fn worker_watch_replay_preserves_mirrored_delete_events() {
        let cluster_db = crate::datastore::test_support::in_memory().await;
        let cluster_api = Arc::new(LocalApiClient::new(
            Arc::new(cluster_db.clone()),
            "worker-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ));
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:worker-store-watch-delete-replay-test",
        )
        .await
        .expect("open node-local");
        let adapter = WorkerStoreAdapter::new(cluster_api, node_local, "worker-a".to_string());

        crate::datastore::DatastoreBackend::broadcast_watch_event(
            &adapter,
            crate::datastore::create_pending_watch_event(
                "v1",
                "ConfigMap",
                Some("default"),
                "deleted-config",
                42,
                "DELETED",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {
                        "namespace": "default",
                        "name": "deleted-config",
                        "resourceVersion": "41"
                    },
                    "data": {"data-1": "value-1"}
                }),
            ),
        );

        let replay = crate::datastore::WatchStore::list_watch_events_since_checked_bounded(
            &adapter,
            &[WatchTarget::namespaced("v1", "ConfigMap")],
            0,
            std::num::NonZeroUsize::new(8).expect("non-zero limit"),
        )
        .await
        .expect("watch replay should succeed");

        let crate::datastore::WatchReplayRead::Events(events) = replay else {
            panic!("worker adapter replay should not expire");
        };
        assert!(
            events.iter().any(|event| {
                event.event_type.as_ref() == "DELETED"
                    && event.resource.kind == "ConfigMap"
                    && event.resource.name == "deleted-config"
                    && event.resource.resource_version == 42
            }),
            "worker watch replay must preserve mirrored DELETED events because deleted resources are absent from snapshot replay"
        );
    }

    #[tokio::test]
    async fn worker_watch_replay_marks_resumed_bound_pod_snapshot_changes_modified() {
        let cluster_db = crate::datastore::test_support::in_memory().await;
        let created = cluster_db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "deadline-pod",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "deadline-pod",
                        "uid": "uid-deadline"
                    },
                    "spec": {
                        "nodeName": "worker-a",
                        "containers": [{
                            "name": "pause",
                            "image": "registry.k8s.io/pause:3.10"
                        }]
                    }
                }),
            )
            .await
            .expect("create pod");
        let cluster_api = Arc::new(LocalApiClient::new(
            Arc::new(cluster_db.clone()),
            "worker-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ));
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:worker-store-watch-resume-pod-modified-test",
        )
        .await
        .expect("open node-local");
        let adapter = WorkerStoreAdapter::new(cluster_api, node_local, "worker-a".to_string());
        let mut created_event = (*created.data).clone();
        created_event["metadata"]["resourceVersion"] =
            serde_json::json!(created.resource_version.to_string());
        adapter.publish_watch(WatchEvent::added(created_event));
        let targets = [WatchTarget::namespaced_in_namespace("v1", "Pod", "default")];
        let limit = std::num::NonZeroUsize::new(4).expect("non-zero limit");

        let first = crate::datastore::WatchStore::list_watch_events_since_checked_bounded(
            &adapter, &targets, 0, limit,
        )
        .await
        .expect("initial watch replay");
        let crate::datastore::WatchReplayRead::Events(first_events) = first else {
            panic!("worker adapter replay should not expire");
        };
        assert_eq!(first_events.len(), 1);
        assert_eq!(first_events[0].event_type.as_ref(), "ADDED");

        let updated = cluster_db
            .update_resource(
                "v1",
                "Pod",
                Some("default"),
                "deadline-pod",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "deadline-pod",
                        "uid": "uid-deadline"
                    },
                    "spec": {
                        "nodeName": "worker-a",
                        "activeDeadlineSeconds": 1,
                        "containers": [{
                            "name": "pause",
                            "image": "registry.k8s.io/pause:3.10"
                        }]
                    }
                }),
                created.resource_version,
            )
            .await
            .expect("update pod");
        let mut updated_event = (*updated.data).clone();
        updated_event["metadata"]["resourceVersion"] =
            serde_json::json!(updated.resource_version.to_string());
        adapter.publish_watch(WatchEvent::modified(updated_event));

        let resumed = crate::datastore::WatchStore::list_watch_events_since_checked_bounded(
            &adapter,
            &targets,
            created.resource_version,
            limit,
        )
        .await
        .expect("resumed watch replay");
        let crate::datastore::WatchReplayRead::Events(resumed_events) = resumed else {
            panic!("worker adapter replay should not expire");
        };
        assert_eq!(resumed_events.len(), 1);
        assert_eq!(
            resumed_events[0].event_type.as_ref(),
            "MODIFIED",
            "worker snapshot replay after a resume RV must preserve update semantics"
        );
        assert_eq!(
            resumed_events[0]
                .resource
                .data
                .pointer("/spec/activeDeadlineSeconds")
                .and_then(|value| value.as_i64()),
            Some(1)
        );
    }

    #[tokio::test]
    async fn reads_cluster_objects_through_worker_cache_and_runtime_rows_from_node_local() {
        let cluster_db = crate::datastore::test_support::in_memory().await;
        cluster_db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "web",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "web",
                        "uid": "uid-1"
                    },
                    "spec": {
                        "nodeName": "worker-a",
                        "containers": [{"name": "app", "image": "nginx"}]
                    }
                }),
            )
            .await
            .expect("create cluster pod");
        let cluster_api = Arc::new(LocalApiClient::new(
            Arc::new(cluster_db.clone()),
            "worker-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ));
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:worker-store-test",
        )
        .await
        .expect("open node-local");
        let adapter =
            WorkerStoreAdapter::new(cluster_api, node_local.clone(), "worker-a".to_string());

        let pod = adapter
            .get_resource("v1", "Pod", Some("default"), "web")
            .await
            .expect("get pod through leader api")
            .expect("pod exists");
        assert_eq!(pod.uid, "uid-1");

        adapter
            .record_sandbox("default", "web", "uid-1", "sandbox-1")
            .await
            .expect("record sandbox in node-local store");
        assert_eq!(
            adapter
                .get_sandbox_for_uid("default", "web", "uid-1")
                .await
                .expect("read worker sandbox"),
            Some("sandbox-1".to_string())
        );
        assert_eq!(
            cluster_db
                .get_sandbox_for_uid("default", "web", "uid-1")
                .await
                .expect("cluster runtime lookup must stay empty"),
            None,
            "worker runtime rows must not be written to cluster storage"
        );
    }

    #[tokio::test]
    async fn watch_mirror_publishes_existing_node_pods_on_startup() {
        let cluster_db = crate::datastore::test_support::in_memory().await;
        cluster_db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "already-bound",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "already-bound",
                        "uid": "uid-bound"
                    },
                    "spec": {
                        "nodeName": "worker-a",
                        "containers": [{"name": "app", "image": "nginx"}]
                    },
                    "status": {"phase": "Pending"}
                }),
            )
            .await
            .expect("create cluster pod");
        let cluster_api = Arc::new(LocalApiClient::new(
            Arc::new(cluster_db.clone()),
            "worker-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ));
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor.clone(),
            None,
            "sqlite:worker-store-watch-bootstrap-test",
        )
        .await
        .expect("open node-local");
        let adapter = Arc::new(WorkerStoreAdapter::new(
            cluster_api,
            node_local,
            "worker-a".to_string(),
        ));
        configure_successful_pod_router(&adapter);
        let mut watch_rx = adapter.watch_topic(crate::watch::WatchTopic::new("v1", "Pod"));
        let cancel = tokio_util::sync::CancellationToken::new();

        let handles = adapter
            .start_watch_mirrors(supervisor.clone(), cancel.clone())
            .await
            .expect("start watch mirrors");

        let event = tokio::time::timeout(std::time::Duration::from_secs(1), watch_rx.recv())
            .await
            .expect("existing node pod should be replayed into worker watch")
            .expect("watch channel should remain open");
        cancel.cancel();
        for handle in handles {
            let _ = handle.join().await;
        }

        assert_eq!(event.event_type, crate::watch::EventType::Added);
        assert_eq!(
            event
                .object
                .pointer("/metadata/name")
                .and_then(|v| v.as_str()),
            Some("already-bound")
        );
        assert_eq!(
            event
                .object
                .pointer("/spec/nodeName")
                .and_then(|v| v.as_str()),
            Some("worker-a")
        );
    }

    #[tokio::test]
    async fn watch_mirror_publishes_namespace_events_on_startup() {
        let cluster_db = crate::datastore::test_support::in_memory().await;
        cluster_db
            .create_namespace(
                "terminating-ns",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {
                        "name": "terminating-ns",
                        "uid": "ns-uid",
                        "deletionTimestamp": "2026-05-18T20:06:06Z"
                    },
                    "spec": {"finalizers": ["kubernetes"]},
                    "status": {"phase": "Terminating"}
                }),
            )
            .await
            .expect("create terminating namespace");
        let cluster_api = Arc::new(LocalApiClient::new(
            Arc::new(cluster_db.clone()),
            "worker-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ));
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor.clone(),
            None,
            "sqlite:worker-store-namespace-watch-bootstrap-test",
        )
        .await
        .expect("open node-local");
        let adapter = Arc::new(WorkerStoreAdapter::new(
            cluster_api,
            node_local,
            "worker-a".to_string(),
        ));
        let mut watch_rx = adapter.watch_topic(crate::watch::WatchTopic::new("v1", "Namespace"));
        let cancel = tokio_util::sync::CancellationToken::new();

        let handles = adapter
            .start_watch_mirrors(supervisor.clone(), cancel.clone())
            .await
            .expect("start watch mirrors");

        let event = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let event = watch_rx
                    .recv()
                    .await
                    .expect("watch channel should remain open");
                if event.object.get("kind").and_then(|value| value.as_str()) == Some("Namespace") {
                    break event;
                }
            }
        })
        .await
        .expect("terminating namespace should be replayed into worker watch");
        cancel.cancel();
        for handle in handles {
            let _ = handle.join().await;
        }

        assert_eq!(event.event_type, crate::watch::EventType::Added);
        assert_eq!(
            event
                .object
                .pointer("/metadata/name")
                .and_then(|value| value.as_str()),
            Some("terminating-ns")
        );
        assert_eq!(
            event
                .object
                .pointer("/metadata/deletionTimestamp")
                .and_then(|value| value.as_str()),
            Some("2026-05-18T20:06:06Z")
        );
    }

    #[tokio::test]
    async fn watch_mirror_relists_after_open_time_replay_window_expiration() {
        let cluster_api = Arc::new(OpenExpiredThenRelistLeaderApi::typed_expiry());
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor.clone(),
            None,
            "sqlite:worker-store-watch-open-expired-test",
        )
        .await
        .expect("open node-local");
        let adapter = Arc::new(WorkerStoreAdapter::new(
            cluster_api.clone(),
            node_local,
            "worker-a".to_string(),
        ));
        configure_successful_pod_router(&adapter);
        let mut watch_rx = adapter.watch_topic(crate::watch::WatchTopic::new("v1", "Pod"));
        let cancel = tokio_util::sync::CancellationToken::new();

        let handles = adapter
            .start_watch_mirrors(supervisor.clone(), cancel.clone())
            .await
            .expect("start watch mirrors");

        let events = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            let mut events = Vec::new();
            while events.len() < 3 {
                events.push(
                    watch_rx
                        .recv()
                        .await
                        .expect("watch channel should remain open"),
                );
            }
            events
        })
        .await
        .expect("mirror should publish initial and authoritative replacement events");
        cancel.cancel();
        for handle in handles {
            let _ = handle.join().await;
        }

        assert_eq!(
            events
                .iter()
                .map(|event| (
                    event.event_type,
                    event
                        .object
                        .pointer("/metadata/name")
                        .and_then(Value::as_str)
                        .unwrap()
                ))
                .collect::<Vec<_>>(),
            vec![
                (EventType::Added, "removed-before-relist"),
                (EventType::Deleted, "removed-before-relist"),
                (EventType::Added, "scheduled-after-relist"),
            ],
            "expired replay relist must remove objects absent from the authoritative snapshot"
        );
        assert!(
            cluster_api.list_count.load(Ordering::SeqCst) >= 2,
            "open-time typed replay expiry must force a fresh LIST"
        );
    }

    #[tokio::test]
    async fn watch_mirror_unmarked_out_of_range_reconnects_without_relist() {
        let cluster_api = Arc::new(OpenExpiredThenRelistLeaderApi::unmarked_out_of_range());
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor.clone(),
            None,
            "sqlite:worker-store-watch-unmarked-out-of-range-test",
        )
        .await
        .expect("open node-local");
        let adapter = Arc::new(WorkerStoreAdapter::new(
            cluster_api.clone(),
            node_local,
            "worker-a".to_string(),
        ));
        configure_successful_pod_router(&adapter);
        let cancel = tokio_util::sync::CancellationToken::new();
        let driver_adapter = adapter.clone();
        let driver_supervisor = supervisor.clone();
        let driver_cancel = cancel.clone();
        let handle = supervisor
            .spawn_async(
                crate::task_supervisor::TaskCategory::Network,
                "worker_store_watch_unmarked_out_of_range_test",
                async move {
                    driver_adapter
                        .run_watch_mirror(
                            WatchRequest {
                                api_version: "v1".to_string(),
                                kind: "Pod".to_string(),
                                namespace: None,
                                label_selector: None,
                                field_selector: Some("spec.nodeName=worker-a".to_string()),
                                start_resource_version: None,
                                start_watch_replay_position: None,
                            },
                            driver_supervisor,
                            driver_cancel,
                        )
                        .await;
                },
            )
            .await
            .expect("spawn mirror driver");

        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            cluster_api.wait_for_watch_attempts(2),
        )
        .await
        .expect("unmarked OutOfRange should reconnect without requiring a relist");
        cancel.cancel();
        let _ = handle.join().await;

        assert_eq!(
            cluster_api.list_count.load(Ordering::SeqCst),
            1,
            "unmarked OutOfRange must keep the safe resume position and avoid authoritative LIST"
        );
    }

    #[tokio::test]
    async fn watch_mirror_repeated_expiry_backs_off_before_next_relist() {
        let cluster_api = Arc::new(OpenExpiredThenRelistLeaderApi::repeated_typed_expiry());
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor.clone(),
            None,
            "sqlite:worker-store-watch-repeated-expiry-test",
        )
        .await
        .expect("open node-local");
        let adapter = Arc::new(WorkerStoreAdapter::new(
            cluster_api.clone(),
            node_local,
            "worker-a".to_string(),
        ));
        configure_successful_pod_router(&adapter);
        let cancel = tokio_util::sync::CancellationToken::new();
        let driver_adapter = adapter.clone();
        let driver_supervisor = supervisor.clone();
        let driver_cancel = cancel.clone();
        let handle = supervisor
            .spawn_async(
                crate::task_supervisor::TaskCategory::Network,
                "worker_store_watch_repeated_expiry_test",
                async move {
                    driver_adapter
                        .run_watch_mirror(
                            WatchRequest {
                                api_version: "v1".to_string(),
                                kind: "Pod".to_string(),
                                namespace: None,
                                label_selector: None,
                                field_selector: Some("spec.nodeName=worker-a".to_string()),
                                start_resource_version: None,
                                start_watch_replay_position: None,
                            },
                            driver_supervisor,
                            driver_cancel,
                        )
                        .await;
                },
            )
            .await
            .expect("spawn mirror driver");

        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            cluster_api.wait_for_watch_attempts(2),
        )
        .await
        .expect("second typed expiry should be observed");
        assert_eq!(
            cluster_api.list_count.load(Ordering::SeqCst),
            2,
            "first typed expiry should get exactly one immediate relist"
        );

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        cancel.cancel();
        let _ = handle.join().await;

        assert_eq!(
            cluster_api.list_count.load(Ordering::SeqCst),
            2,
            "second consecutive typed expiry must back off instead of immediately relisting again"
        );
    }

    #[tokio::test]
    async fn worker_store_requeues_node_local_pod_workqueue_failures() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:worker-store-workqueue-retry-test",
        )
        .await
        .expect("open node-local");
        let adapter = WorkerStoreAdapter::new(
            Arc::new(HandoffLeaderApi),
            node_local,
            "worker-a".to_string(),
        );

        let pod = crate::pod_identity::PodIdentity::new("default", "stuck", "uid-stuck");
        adapter
            .pod_workqueue_enqueue(
                PodWorkqueueKind::Pod,
                &pod,
                serde_json::json!({"source": "test"}),
                3,
                0,
                None,
            )
            .await
            .expect("enqueue workqueue row");
        let claimed = adapter
            .pod_workqueue_claim_due(i64::MAX)
            .await
            .expect("claim workqueue row")
            .expect("workqueue row exists");

        adapter
            .pod_workqueue_record_failure(claimed, 0, "missed delete")
            .await
            .expect("record worker-local failure");

        let retried = adapter
            .pod_workqueue_claim_due(i64::MAX)
            .await
            .expect("claim retried workqueue row")
            .expect("failure must requeue worker-local pod delete work");
        assert_eq!(retried.kind, PodWorkqueueKind::Pod);
        assert_eq!(retried.namespace, "default");
        assert_eq!(retried.name, "stuck");
        assert_eq!(retried.uid, "uid-stuck");
        assert_eq!(retried.attempt_count, 4);
        assert_eq!(retried.payload, serde_json::json!({"source": "test"}));
    }

    #[tokio::test]
    async fn worker_store_routes_local_pod_watch_to_lifecycle_actor() {
        struct LocalPodLeaderApi;

        #[async_trait]
        impl LeaderApiClient for LocalPodLeaderApi {
            async fn get_resource(&self, key: ResourceKey) -> Result<Option<Resource>> {
                unreachable!("local pod watch test does not use get_resource for {key:?}")
            }

            async fn list_resources(&self, req: ListRequest) -> Result<ResourceList> {
                Ok(ResourceList {
                    items: Vec::new(),
                    resource_version: if req.api_version == "v1" && req.kind == "Pod" {
                        41
                    } else {
                        0
                    },
                    watch_replay_position: None,
                    continue_token: None,
                    remaining_item_count: None,
                })
            }

            async fn watch_resources(
                &self,
                req: WatchRequest,
            ) -> Result<WatchStream<ResourceEvent>> {
                if req.api_version == "v1" && req.kind == "Pod" {
                    if req.start_resource_version != Some(41) {
                        return Ok(Box::pin(futures::stream::pending()));
                    }
                    let events = vec![
                        ResourceEvent {
                            event: WatchEvent::added(serde_json::json!({
                                "apiVersion": "v1",
                                "kind": "Pod",
                                "metadata": {
                                    "namespace": "default",
                                    "name": "startable",
                                    "uid": "uid-startable",
                                    "resourceVersion": "42"
                                },
                                "spec": {
                                    "nodeName": "worker-a",
                                    "containers": [{"name": "app", "image": "busybox"}]
                                },
                                "status": {"phase": "Pending"}
                            })),
                            resume_position: None,
                        },
                        ResourceEvent {
                            event: WatchEvent::modified(serde_json::json!({
                                "apiVersion": "v1",
                                "kind": "Pod",
                                "metadata": {
                                    "namespace": "default",
                                    "name": "terminating",
                                    "uid": "uid-terminating",
                                    "resourceVersion": "43",
                                    "deletionTimestamp": "2026-06-21T02:07:04Z"
                                },
                                "spec": {
                                    "nodeName": "worker-a",
                                    "containers": [{"name": "app", "image": "busybox"}]
                                },
                                "status": {"phase": "Succeeded"}
                            })),
                            resume_position: None,
                        },
                        ResourceEvent {
                            event: WatchEvent::added(serde_json::json!({
                                "apiVersion": "v1",
                                "kind": "Pod",
                                "metadata": {
                                    "namespace": "default",
                                    "name": "moving-away",
                                    "uid": "uid-moving-away",
                                    "resourceVersion": "44"
                                },
                                "spec": {
                                    "nodeName": "worker-a",
                                    "containers": [{"name": "app", "image": "busybox"}]
                                },
                                "status": {"phase": "Running"}
                            })),
                            resume_position: None,
                        },
                        ResourceEvent {
                            event: WatchEvent::modified(serde_json::json!({
                                "apiVersion": "v1",
                                "kind": "Pod",
                                "metadata": {
                                    "namespace": "default",
                                    "name": "moving-away",
                                    "uid": "uid-moving-away",
                                    "resourceVersion": "45"
                                },
                                "spec": {
                                    "nodeName": "worker-b",
                                    "containers": [{"name": "app", "image": "busybox"}]
                                },
                                "status": {"phase": "Running"}
                            })),
                            resume_position: None,
                        },
                    ];
                    return Ok(Box::pin(futures::stream::iter(events.into_iter().map(Ok))));
                }
                Ok(Box::pin(futures::stream::pending()))
            }

            async fn wait_cache_ready(&self, _scope: CacheScope) -> Result<()> {
                Ok(())
            }

            async fn get_pod(&self, _ns: &str, _name: &str) -> Result<Option<Pod>> {
                unreachable!("local pod watch test does not use get_pod")
            }

            async fn get_pod_for_uid(
                &self,
                _ns: &str,
                _name: &str,
                _uid: &str,
            ) -> Result<Option<Pod>> {
                unreachable!("local pod watch test does not use get_pod_for_uid")
            }

            async fn watch_pods_on_node(&self, _node_name: &str) -> Result<WatchStream<Pod>> {
                unreachable!("local pod watch test does not use watch_pods_on_node")
            }

            async fn list_pods_on_node(&self, _node_name: &str) -> Result<Vec<Pod>> {
                unreachable!("local pod watch test does not use list_pods_on_node")
            }

            async fn get_configmap(&self, _ns: &str, _name: &str) -> Result<Option<ConfigMap>> {
                Ok(None)
            }

            async fn get_secret(&self, _ns: &str, _name: &str) -> Result<Option<Secret>> {
                Ok(None)
            }

            async fn get_node(&self, name: &str) -> Result<Node> {
                unreachable!("local pod watch test does not use get_node for {name}")
            }

            async fn watch_node(&self, _name: &str) -> Result<WatchStream<Node>> {
                Ok(Box::pin(futures::stream::pending()))
            }

            async fn allocate_node_subnet(
                &self,
                node_name: &str,
                _cluster_cidr: &str,
                _node_ip: &str,
            ) -> Result<NodeSubnet> {
                unreachable!("local pod watch test does not allocate subnet for {node_name}")
            }

            async fn get_node_subnet(&self, node_name: &str) -> Result<Option<NodeSubnet>> {
                unreachable!("local pod watch test does not get subnet for {node_name}")
            }

            async fn list_peer_subnets(&self, my_node_name: &str) -> Result<Vec<NodeSubnet>> {
                unreachable!("local pod watch test does not list peer subnets for {my_node_name}")
            }

            async fn get_node_dataplane(
                &self,
                node_name: &str,
            ) -> Result<Option<crate::networking::wireguard::DataplanePeerMetadata>> {
                unreachable!("local pod watch test does not get dataplane for {node_name}")
            }

            async fn apply_outbox(
                &self,
                _idempotency_key: &str,
                _operation: crate::kubelet::outbox::payload::OutboxOperation,
                _payload: bytes::Bytes,
                _client_id: &str,
                _stream_id: i64,
                _stream_seq: i64,
            ) -> std::result::Result<
                crate::kubelet::outbox::OutboxApplyResult,
                crate::kubelet::outbox::OutboxApplyError,
            > {
                unreachable!("local pod watch test does not apply outbox")
            }
        }

        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor.clone(),
            None,
            "sqlite:worker-store-terminating-pod-watch-test",
        )
        .await
        .expect("open node-local");
        let adapter = Arc::new(WorkerStoreAdapter::new(
            Arc::new(LocalPodLeaderApi),
            node_local,
            "worker-a".to_string(),
        ));
        let executor = crate::kubelet::pod_lifecycle_router::executor::RecordingExecutor::new();
        let registry = Arc::new(
            crate::kubelet::pod_lifecycle_actor::registry::PodLifecycleRegistry::new(
                supervisor.clone(),
                crate::kubelet::pod_lifecycle_actor::config::PodLifecycleConcurrencyConfig::production_default(),
                Arc::new(std::sync::Mutex::new(
                    executor.clone()
                        as Arc<
                            dyn crate::kubelet::pod_lifecycle_router::executor::PodWorkExecutor,
                        >,
                )),
            ),
        );
        let router = Arc::new(
            crate::kubelet::pod_lifecycle_router::PodLifecycleRouter::new_actor_with_executor(
                registry,
                executor.clone()
                    as Arc<dyn crate::kubelet::pod_lifecycle_router::executor::PodWorkExecutor>,
            ),
        );
        adapter.set_pod_lifecycle_router(router);

        let mut pod_watch = adapter.watch_topic(WatchTopic::new("v1", "Pod"));

        let cancel = tokio_util::sync::CancellationToken::new();
        let handles = adapter
            .start_watch_mirrors(supervisor, cancel.clone())
            .await
            .expect("start watch mirrors");

        let moving_types = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            let mut types = Vec::new();
            while types.len() < 2 {
                let event = pod_watch.recv().await.expect("Pod watch remains open");
                if event
                    .object
                    .pointer("/metadata/name")
                    .and_then(Value::as_str)
                    == Some("moving-away")
                {
                    types.push(event.event_type);
                }
            }
            types
        })
        .await
        .expect("nodeName leave transition should be mirrored");
        assert_eq!(
            moving_types,
            vec![EventType::Added, EventType::Deleted],
            "a Pod leaving spec.nodeName=worker-a must synthesize Deleted on the worker mirror"
        );

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
        let mut observed = Vec::new();
        loop {
            observed.extend(executor.take_actions());
            let start_seen = observed.iter().any(|action| {
                matches!(
                    action,
                    crate::kubelet::pod_lifecycle_core::action::PodAction::StartPod {
                        key, ..
                    }
                    | crate::kubelet::pod_lifecycle_core::action::PodAction::CheckSlotAdmission {
                        key,
                        ..
                    } if key.name == "startable" && key.uid == "uid-startable"
                )
            });
            let stop_seen = observed.iter().any(|action| {
                matches!(
                    action,
                    crate::kubelet::pod_lifecycle_core::action::PodAction::StopPod {
                        key, ..
                    } if key.name == "terminating" && key.uid == "uid-terminating"
                )
            });
            if start_seen && stop_seen {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "local Pod watch events must wake lifecycle actors; observed actions: {observed:?}"
                );
            }
            tokio::task::yield_now().await;
        }
        cancel.cancel();
        for handle in handles {
            let _ = handle.join().await;
        }
    }

    #[tokio::test]
    async fn watch_mirror_replays_pods_bound_between_initial_list_and_watch() {
        let cluster_api = Arc::new(HandoffLeaderApi);
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor.clone(),
            None,
            "sqlite:worker-store-watch-handoff-test",
        )
        .await
        .expect("open node-local");
        let adapter = Arc::new(WorkerStoreAdapter::new(
            cluster_api,
            node_local,
            "worker-a".to_string(),
        ));
        configure_successful_pod_router(&adapter);
        let mut watch_rx = adapter.watch_topic(crate::watch::WatchTopic::new("v1", "Pod"));
        let cancel = tokio_util::sync::CancellationToken::new();

        let handles = adapter
            .start_watch_mirrors(supervisor.clone(), cancel.clone())
            .await
            .expect("start watch mirrors");

        let event = tokio::time::timeout(std::time::Duration::from_secs(1), watch_rx.recv())
            .await
            .expect("pod bound after the initial list should be replayed from list RV")
            .expect("watch channel should remain open");
        cancel.cancel();
        for handle in handles {
            let _ = handle.join().await;
        }

        assert_eq!(
            event.event_type,
            crate::watch::EventType::Added,
            "a Pod entering the worker's nodeName selector after LIST must be ADDED"
        );
        assert_eq!(
            event
                .object
                .pointer("/metadata/name")
                .and_then(|v| v.as_str()),
            Some("bound-during-handoff")
        );
        assert_eq!(
            event
                .object
                .pointer("/metadata/resourceVersion")
                .and_then(|v| v.as_str()),
            Some("42")
        );
    }
}
