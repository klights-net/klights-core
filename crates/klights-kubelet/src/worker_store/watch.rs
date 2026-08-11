//! Worker informer LIST/WATCH mirrors, replay history, and reconnect driver.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use anyhow::{Result, anyhow};
use futures::StreamExt as _;
use klights_cluster_core::{PositionedWatchEvent, Resource, WatchReplayPosition};
use klights_leader_api::{
    LeaderWatchError, ResourceEvent, ResourceListRequest, ResourceQueryConsistency, WatchRequest,
    WatchResumeCursor,
};
use klights_watch::{
    EventType, WatchEvent, WatchSignal, WatchSignalSubscribe, WatchTarget, WatchTargetScope,
    WatchTopic,
};
use serde_json::Value;

use super::WorkerStoreAdapter;
use super::reflector::ReflectorState;

const WORKER_WATCH_EVENT_HISTORY_CAPACITY: usize = 32_768;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerReplayAvailability {
    Available,
    Expired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerReplayBoundary {
    Exact(WatchReplayPosition),
}

impl WorkerReplayBoundary {
    const fn classify(self, cursor: WatchReplayPosition) -> WorkerReplayAvailability {
        match self {
            Self::Exact(floor) => {
                if cursor.event_id > 0 {
                    if cursor.event_id < floor.event_id {
                        WorkerReplayAvailability::Expired
                    } else {
                        WorkerReplayAvailability::Available
                    }
                } else if cursor.resource_version_filter_through_event_id > 0 {
                    if cursor.resource_version < floor.resource_version
                        || cursor.resource_version_filter_through_event_id < floor.event_id
                    {
                        WorkerReplayAvailability::Expired
                    } else {
                        WorkerReplayAvailability::Available
                    }
                } else if cursor.resource_version < floor.resource_version {
                    WorkerReplayAvailability::Expired
                } else {
                    WorkerReplayAvailability::Available
                }
            }
        }
    }

    fn classify_all(
        boundaries: impl IntoIterator<Item = Self>,
        cursor: WatchReplayPosition,
    ) -> WorkerReplayAvailability {
        if boundaries
            .into_iter()
            .any(|boundary| boundary.classify(cursor) == WorkerReplayAvailability::Expired)
        {
            WorkerReplayAvailability::Expired
        } else {
            WorkerReplayAvailability::Available
        }
    }

    fn retain_exact(boundaries: &mut Vec<Self>, candidate: WatchReplayPosition) {
        let mut exact = boundaries
            .iter()
            .copied()
            .map(|boundary| match boundary {
                Self::Exact(position) => position,
            })
            .collect::<Vec<_>>();
        exact.push(candidate);
        let highest_resource_version = *exact
            .iter()
            .max_by_key(|position| position.resource_version)
            .expect("candidate keeps exact boundaries non-empty");
        let highest_event_id = *exact
            .iter()
            .max_by_key(|position| position.event_id)
            .expect("candidate keeps exact boundaries non-empty");
        boundaries.clear();
        boundaries.push(Self::Exact(highest_resource_version));
        if highest_event_id != highest_resource_version {
            boundaries.push(Self::Exact(highest_event_id));
        }
    }
}

#[derive(Default)]
pub(crate) struct WorkerWatchHistory {
    pub(crate) events: VecDeque<(i64, WatchEvent)>,
    floors: HashMap<(WatchTopic, Option<String>), Vec<WorkerReplayBoundary>>,
}

/// Focused worker replay resource.  It carries the post-event object and its
/// Kubernetes event type without exposing root datastore watch DTOs.
#[derive(Clone, Debug)]
pub struct WorkerCatchUpResource {
    pub resource: Resource,
    pub event_type: String,
}

fn worker_replay_boundaries(
    history: &WorkerWatchHistory,
    target: &WatchTarget,
) -> Vec<WorkerReplayBoundary> {
    let topic = WatchTopic::new(target.api_version(), target.kind());
    history
        .floors
        .iter()
        .filter(|((floor_topic, namespace), _)| {
            if floor_topic != &topic {
                return false;
            }
            match target.scope() {
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

impl WorkerStoreAdapter {
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
                                        if let Err(err) = applied_cursor.advance_after_apply(&delivered) {
                                            tracing::warn!(error = %err, "worker mirror cursor rejected event before apply");
                                            break;
                                        }
                                        let pending_transition = match selector_membership.prepare(event) {
                                            Ok(pending) => pending,
                                            Err(err) => {
                                                tracing::warn!(error = %err, "worker mirror selector rejected event before apply");
                                                break;
                                            }
                                        };
                                        let Some(transitioned) = pending_transition.event() else {
                                            if let Err(err) = selector_membership.commit(pending_transition) {
                                                tracing::warn!(error = %err, "worker mirror selector could not commit filtered event");
                                                break;
                                            }
                                            if event_rv > 0 { self.observe_rv(event_rv); }
                                            next_resource_version = applied_cursor.resource_version();
                                            next_watch_replay_position = applied_cursor.replay_position();
                                            reconnect_attempt = 0;
                                            immediate_expiry_relist_available = true;
                                            continue;
                                        };
                                        let transitioned = resource_event_to_watch_event(transitioned);
                                        let transitioned = match self.publish_watch_from_mirror(transitioned).await {
                                            Ok(event) => event,
                                            Err(err) => {
                                                tracing::warn!(error = %err, "worker store watch mirror could not apply event; reconnecting from last applied position");
                                                break;
                                            }
                                        };
                                        state.observe(&transitioned);
                                        if let Err(err) = selector_membership.commit(pending_transition) {
                                            tracing::warn!(error = %err, "worker mirror selector could not commit applied event");
                                            break;
                                        }
                                        if event_rv > 0 { self.observe_rv(event_rv); }
                                        next_resource_version = applied_cursor.resource_version();
                                        next_watch_replay_position = applied_cursor.replay_position();
                                        reconnect_attempt = 0;
                                        immediate_expiry_relist_available = true;
                                    }
                                    Some(Err(err)) => {
                                        if is_watch_window_expired(&err) {
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
                    if relist_required {
                        continue;
                    }
                }
                Err(err) => {
                    if is_watch_window_expired(&err) {
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

    #[cfg(any(test, feature = "test-support"))]
    pub async fn run_watch_mirror_for_test(
        self: Arc<Self>,
        req: WatchRequest,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
        cancel: tokio_util::sync::CancellationToken,
    ) {
        self.run_watch_mirror(req, supervisor, cancel).await;
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
            .await
            .map_err(anyhow::Error::new)?;
        let (items, resource_version, replay_position, _, _) = list.into_parts();
        let pending = state.prepare_snapshot(items.clone(), resource_version);
        for event in pending.events() {
            self.publish_watch_from_mirror(event.clone()).await?;
        }
        pending.commit_into(state);
        selector_membership.replace(&items);
        self.observe_rv(resource_version);
        Ok((resource_version, replay_position))
    }

    #[cfg(any(test, feature = "test-support"))]
    pub async fn reconcile_watch_snapshot_for_test(
        &self,
        req: &WatchRequest,
        state: &mut ReflectorState,
        selector_membership: &mut dyn klights_watch::WatchTransitionProjector,
    ) -> Result<(i64, Option<WatchReplayPosition>)> {
        self.reconcile_watch_snapshot(req, state, selector_membership)
            .await
    }

    pub async fn publish_watch_from_mirror(&self, event: WatchEvent) -> Result<WatchEvent> {
        if let Some(message) = local_pod_lifecycle_message(event.clone(), &self.node_name) {
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

    /// Test-support seam for injecting an already-projected mirror event.
    /// Production callers use the LIST/WATCH driver above.
    #[cfg(any(test, feature = "test-support"))]
    pub fn publish_watch_for_test(&self, event: WatchEvent) {
        self.publish_watch(event);
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
        #[cfg(any(test, feature = "test-support"))]
        self.watch_events.publish(event);
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
                    WorkerReplayBoundary::retain_exact(
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
    ) -> Vec<WorkerCatchUpResource> {
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

    pub fn list_watch_events_since(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
    ) -> Vec<WorkerCatchUpResource> {
        self.historical_watch_events_since(targets, since_rv)
    }

    pub fn list_watch_events_after_position_checked_bounded(
        &self,
        targets: &[WatchTarget],
        position: WatchReplayPosition,
        limit: std::num::NonZeroUsize,
    ) -> klights_watch::PositionedWatchReplayRead<WorkerCatchUpResource> {
        let high_water_event_id = self.next_event_id.load(Ordering::Relaxed).saturating_sub(1);
        let history = self
            .event_history
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if targets.iter().any(|target| {
            WorkerReplayBoundary::classify_all(worker_replay_boundaries(&history, target), position)
                == WorkerReplayAvailability::Expired
        }) {
            return klights_watch::PositionedWatchReplayRead::Expired;
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
        klights_watch::PositionedWatchReplayRead::Events(klights_watch::PositionedWatchReplay::new(
            events,
            next_position,
        ))
    }
}

impl WatchSignalSubscribe for WorkerStoreAdapter {
    fn subscribe(&self, topic: WatchTopic) -> klights_watch::WatchSignalReceiver {
        self.watch_events.subscribe_signals(topic)
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

/// A typed replay-window expiry is the only condition that permits relisting.
pub fn is_watch_window_expired(err: &LeaderWatchError) -> bool {
    matches!(err, LeaderWatchError::ReplayExpired { .. })
}

fn watch_event_topic(event: &WatchEvent) -> Option<WatchTopic> {
    Some(WatchTopic::new(
        event.object.get("apiVersion")?.as_str()?,
        event.object.get("kind")?.as_str()?,
    ))
}

fn watch_event_matches_targets(event: &WatchEvent, targets: &[WatchTarget]) -> bool {
    let Some(api_version) = event.object.get("apiVersion").and_then(Value::as_str) else {
        return false;
    };
    let Some(kind) = event.object.get("kind").and_then(Value::as_str) else {
        return false;
    };
    let namespace = event
        .object
        .pointer("/metadata/namespace")
        .and_then(Value::as_str);

    targets.iter().any(|target| {
        if target.api_version() != api_version || target.kind() != kind {
            return false;
        }
        match target.scope() {
            WatchTargetScope::Cluster => namespace.is_none(),
            WatchTargetScope::Namespaced(Some(target_ns)) => namespace == Some(target_ns.as_str()),
            WatchTargetScope::Namespaced(None) => namespace.is_some(),
        }
    })
}

/// Shared scalar/positioned replay predicate used by the worker history.
pub fn worker_replay_event_follows_position(
    position: WatchReplayPosition,
    event_id: i64,
    event: &WatchEvent,
) -> bool {
    event
        .resource_version()
        .is_some_and(|resource_version| !position.represents_event(event_id, resource_version))
}

fn catchup_resource_from_watch_event(event: &WatchEvent) -> Option<WorkerCatchUpResource> {
    let api_version = event.object.get("apiVersion")?.as_str()?.to_string();
    let kind = event.object.get("kind")?.as_str()?.to_string();
    let metadata = event.object.get("metadata")?;
    let name = metadata.get("name")?.as_str()?.to_string();
    let namespace = metadata
        .get("namespace")
        .and_then(Value::as_str)
        .map(str::to_string);
    let uid = metadata
        .get("uid")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let resource_version = event.resource_version()?;

    Some(WorkerCatchUpResource {
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
        event_type: event.event_type.to_string(),
    })
}

fn resource_event_to_watch_event(event: &ResourceEvent) -> WatchEvent {
    WatchEvent {
        event_type: match event.event_type() {
            klights_leader_api::WatchEventType::Added => EventType::Added,
            klights_leader_api::WatchEventType::Modified => EventType::Modified,
            klights_leader_api::WatchEventType::Deleted => EventType::Deleted,
            klights_leader_api::WatchEventType::Bookmark => EventType::Bookmark,
            klights_leader_api::WatchEventType::Error => EventType::Error,
        },
        object: event.resource().data.clone(),
        encoded_payload: None,
    }
}

fn local_pod_lifecycle_message(
    event: WatchEvent,
    node_name: &str,
) -> Option<crate::pod_lifecycle_core::message::LifecycleMessage> {
    use crate::pod_lifecycle_core::message::{LifecycleMessage, PodLifecycleKey};
    let pod = event.object.as_ref();
    if pod.get("apiVersion").and_then(Value::as_str) != Some("v1")
        || pod.get("kind").and_then(Value::as_str) != Some("Pod")
    {
        return None;
    }
    if pod.pointer("/spec/nodeName").and_then(Value::as_str) != Some(node_name) {
        return None;
    }
    let namespace = pod.pointer("/metadata/namespace").and_then(Value::as_str)?;
    let name = pod.pointer("/metadata/name").and_then(Value::as_str)?;
    let uid = pod
        .pointer("/metadata/uid")
        .and_then(Value::as_str)
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
