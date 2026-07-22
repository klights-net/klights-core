use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::api::AppState;
use crate::datastore::command::StorageCommand;
use crate::datastore::raft::node::RaftNode;
use crate::datastore::sqlite::DatastoreWatchReplaySource;
use crate::datastore::{
    DatastoreBackend, POD_CLEANUP_REASON_NODE_LOST, Resource, ResourcePreconditions, WatchTarget,
};
use crate::kubelet::pod_lifecycle_core::message::PodLifecycleKey;
use crate::kubelet::pod_lifecycle_router::{
    OrphanReason, PodLifecycleRouter, enqueue_orphan_finalize,
};
use crate::kubelet::pod_repository::{PodObjectWriter, PodReader, PodSubresourceWriter};
use crate::node_lease_tracker::{
    DEFAULT_NODE_LEASE_GRACE_SECONDS, NodeLeaseObservation, NodeLeaseTracker,
};
use crate::utils::k8s_time_format;
use crate::watch::{
    EventType, SignalWatchCursor, WatchCursorError, WatchDeliveryScope, WatchEvent, WindowPolicy,
};
use klights_watch::{WatchSignalReceiver, WatchTopic};

#[cfg(test)]
const DEFAULT_NODE_LEASE_DURATION_SECONDS: i64 =
    crate::node_lease_tracker::DEFAULT_NODE_LEASE_DURATION_SECONDS;
const NODE_STATUS_UNKNOWN_REASON: &str = "NodeStatusUnknown";
const NODE_STATUS_UNKNOWN_MESSAGE: &str = "Kubelet stopped posting node status.";
const NODE_READY_REASON: &str = "KubeletReady";
const NODE_READY_MESSAGE: &str = "klights is ready";
const NODE_NOT_READY_POD_EVICTION_GRACE_ENV: &str =
    "KLIGHTS_NODE_NOT_READY_POD_EVICTION_GRACE_SECONDS";
// Default 0: once a node is confirmed Unknown (after ~24s of confirmed lease
// silence, T3), its pods are marked Unknown and cleaned up immediately — no
// extra wait. Cleanup still flows through the UID-bound actor finalization
// (HR #11). Operators can restore a delay via the env var above.
//
// Deliberate deviations from upstream: (a) ignores per-pod tolerationSeconds
// for node.kubernetes.io/unreachable (acceptable because eviction only fires
// after confirmed silence, not on a transient blip); (b) a partitioned-but-
// alive node could have its pods rescheduled while it still runs them until it
// sheds leadership/membership — mitigated by the 24s detection, not eliminated.
const DEFAULT_NODE_NOT_READY_POD_EVICTION_GRACE_SECONDS: i64 = 0;

#[cfg(test)]
struct TestNodeLifecycleStatus<'a>(&'a dyn DatastoreBackend);

#[cfg(test)]
impl klights_leader_api::LeaderNodeLifecycleStatus for TestNodeLifecycleStatus<'_> {
    fn submit_node_lifecycle_status(
        &self,
        request: klights_leader_api::NodeLifecycleStatusRequest,
    ) -> klights_leader_api::NodeLifecycleStatusFuture<
        '_,
        klights_leader_api::NodeLifecycleStatusResult,
    > {
        Box::pin(async move {
            let StorageCommand::UpdateStatus {
                api_version,
                kind,
                namespace,
                name,
                status,
                preconditions,
                ..
            } = request.into_command()
            else {
                unreachable!("validated lifecycle request must be UpdateStatus")
            };
            let resource = self
                .0
                .update_status_only_with_preconditions(
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                    status,
                    preconditions,
                )
                .await
                .map_err(|error| {
                    klights_leader_api::NodeLifecycleStatusError::apply_failed(error.to_string())
                })?;
            Ok(klights_leader_api::NodeLifecycleStatusResult::Updated {
                resource_version: resource.resource_version,
            })
        })
    }
}

pub trait NodeLifecyclePodRepository: PodReader + PodSubresourceWriter + PodObjectWriter {}

impl<T> NodeLifecyclePodRepository for T where
    T: PodReader + PodSubresourceWriter + PodObjectWriter + Send + Sync
{
}

pub async fn mark_all_nodes_unknown_on_startup(
    state: &AppState,
    node_status: &dyn klights_leader_api::LeaderNodeLifecycleStatus,
) -> Result<()> {
    mark_all_nodes_unknown_at_with_pods(
        state.db.as_ref(),
        node_status,
        state.pod_repository.as_ref(),
        Some(state.side_effects.as_ref()),
        state.pod_lifecycle_router.as_deref(),
        Utc::now(),
    )
    .await
}

#[cfg(test)]
pub async fn mark_all_nodes_unknown_at(
    db: &crate::datastore::sqlite::Datastore,
    now: DateTime<Utc>,
) -> Result<()> {
    let pod_repository = crate::controllers::test_utils::pod_repository_for_test(db);
    let node_status = TestNodeLifecycleStatus(db);
    mark_all_nodes_unknown_at_with_pods(db, &node_status, pod_repository.as_ref(), None, None, now)
        .await
}

async fn mark_all_nodes_unknown_at_with_pods(
    db: &dyn DatastoreBackend,
    node_status: &dyn klights_leader_api::LeaderNodeLifecycleStatus,
    pod_repository: &dyn NodeLifecyclePodRepository,
    side_effects: Option<&crate::side_effects::SideEffectRegistry>,
    pod_lifecycle_router: Option<&PodLifecycleRouter>,
    now: DateTime<Utc>,
) -> Result<()> {
    let nodes = db
        .list_resources(
            "v1",
            "Node",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;
    for node in nodes.items {
        let mut data = Arc::unwrap_or_clone(node.data.clone());
        if mark_node_ready_unknown(&mut data, now) {
            update_node_status(node_status, &node, data).await?;
        }
        let _ = mark_pods_unknown_on_node(
            db,
            pod_repository,
            side_effects,
            pod_lifecycle_router,
            &node.name,
            now,
        )
        .await?;
    }
    Ok(())
}

#[cfg(test)]
pub async fn reconcile_node_lifecycle_once(
    db: &crate::datastore::sqlite::Datastore,
    now: DateTime<Utc>,
) -> Result<Option<Duration>> {
    let pod_repository = crate::controllers::test_utils::pod_repository_for_test(db);
    let tracker = node_lease_tracker_from_cluster_leases_for_test(db, now).await?;
    let node_status = TestNodeLifecycleStatus(db);
    reconcile_node_lifecycle_once_with_tracker(
        db,
        &node_status,
        pod_repository.as_ref(),
        &tracker,
        now,
        None,
        None,
    )
    .await
}

#[cfg(test)]
pub async fn reconcile_node_lifecycle_once_after_startup(
    db: &crate::datastore::sqlite::Datastore,
    now: DateTime<Utc>,
    startup_resource_version: i64,
) -> Result<Option<Duration>> {
    let _ = startup_resource_version;
    reconcile_node_lifecycle_once_with_tracker_for_test(
        db,
        &NodeLeaseTracker::new_for_test(now),
        now,
    )
    .await
}

#[cfg(test)]
pub async fn reconcile_node_lifecycle_once_with_tracker_for_test(
    db: &crate::datastore::sqlite::Datastore,
    node_lease_tracker: &NodeLeaseTracker,
    now: DateTime<Utc>,
) -> Result<Option<Duration>> {
    let pod_repository = crate::controllers::test_utils::pod_repository_for_test(db);
    let node_status = TestNodeLifecycleStatus(db);
    reconcile_node_lifecycle_once_with_tracker(
        db,
        &node_status,
        pod_repository.as_ref(),
        node_lease_tracker,
        now,
        None,
        None,
    )
    .await
}

async fn reconcile_node_lifecycle_once_with_tracker(
    db: &dyn DatastoreBackend,
    node_status: &dyn klights_leader_api::LeaderNodeLifecycleStatus,
    pod_repository: &dyn NodeLifecyclePodRepository,
    node_lease_tracker: &NodeLeaseTracker,
    now: DateTime<Utc>,
    side_effects: Option<&crate::side_effects::SideEffectRegistry>,
    pod_lifecycle_router: Option<&PodLifecycleRouter>,
) -> Result<Option<Duration>> {
    let nodes = db
        .list_resources(
            "v1",
            "Node",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;
    let mut next_deadline: Option<Duration> = None;

    for node in nodes.items {
        let lease_deadline = node_lease_tracker.deadline_for_node(&node.name).await;
        let deadline = lease_deadline.deadline;
        let observed = lease_deadline.observed.as_ref();

        let mut data = Arc::unwrap_or_clone(node.data.clone());
        let lease_fresh = deadline > now;
        let status_heartbeat_deadline = ready_status_heartbeat_deadline(&data);
        let status_heartbeat_fresh =
            status_heartbeat_deadline.is_some_and(|deadline| deadline > now);
        let stale = !lease_fresh && !status_heartbeat_fresh;
        let mut should_reconcile_ready_resources = false;

        let changed = if stale {
            mark_node_ready_unknown(&mut data, now)
        } else {
            if lease_fresh && let Ok(remaining) = deadline.signed_duration_since(now).to_std() {
                next_deadline =
                    Some(next_deadline.map_or(remaining, |current| current.min(remaining)));
            }
            if let Some(deadline) = status_heartbeat_deadline
                && deadline > now
                && let Ok(remaining) = deadline.signed_duration_since(now).to_std()
            {
                next_deadline =
                    Some(next_deadline.map_or(remaining, |current| current.min(remaining)));
            }
            let ready_transition = lease_fresh
                .then_some(observed)
                .flatten()
                .map(|lease| mark_node_ready_from_fresh_observation(&mut data, lease, now))
                .unwrap_or(false);
            should_reconcile_ready_resources = (lease_fresh && observed.is_some()
                || status_heartbeat_fresh)
                && (ready_transition || node_ready_condition_true(&data));
            ready_transition
        };

        if changed {
            update_node_status(node_status, &node, data).await?;
        }
        if stale {
            merge_deadline(
                &mut next_deadline,
                mark_pods_unknown_on_node(
                    db,
                    pod_repository,
                    side_effects,
                    pod_lifecycle_router,
                    &node.name,
                    now,
                )
                .await?,
            );
        } else if should_reconcile_ready_resources {
            reconcile_node_resources_after_ready(pod_repository, &node.name, now).await?;
        }
    }

    Ok(next_deadline)
}

async fn cleanup_pods_bound_to_deleted_node_event(
    db: &dyn DatastoreBackend,
    pod_repository: &dyn NodeLifecyclePodRepository,
    side_effects: Option<&crate::side_effects::SideEffectRegistry>,
    pod_lifecycle_router: Option<&PodLifecycleRouter>,
    event: &WatchEvent,
) -> Result<bool> {
    let Some(node_name) = deleted_node_name(event) else {
        return Ok(false);
    };
    cleanup_pods_bound_to_deleted_node(
        db,
        pod_repository,
        side_effects,
        pod_lifecycle_router,
        node_name,
        Utc::now(),
    )
    .await?;
    Ok(true)
}

fn deleted_node_name(event: &WatchEvent) -> Option<&str> {
    if event.event_type != EventType::Deleted {
        return None;
    }
    if event.object.get("apiVersion").and_then(|v| v.as_str()) != Some("v1")
        || event.object.get("kind").and_then(|v| v.as_str()) != Some("Node")
    {
        return None;
    }
    event
        .object
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .filter(|name| !name.trim().is_empty())
}

async fn cleanup_pods_bound_to_deleted_node(
    db: &dyn DatastoreBackend,
    pod_repository: &dyn NodeLifecyclePodRepository,
    side_effects: Option<&crate::side_effects::SideEffectRegistry>,
    pod_lifecycle_router: Option<&PodLifecycleRouter>,
    node_name: &str,
    now: DateTime<Utc>,
) -> Result<()> {
    let field_selector = format!("spec.nodeName={node_name}");
    let pods = pod_repository
        .list_pods(None, None, Some(&field_selector), None, None)
        .await?;
    for pod in pods.items {
        mark_pod_node_lost_and_enqueue_actor_cleanup(
            db,
            pod_repository,
            side_effects,
            pod_lifecycle_router,
            node_name,
            pod,
            now,
        )
        .await?;
    }
    Ok(())
}

#[cfg(test)]
async fn node_lease_tracker_from_cluster_leases_for_test(
    db: &crate::datastore::sqlite::Datastore,
    startup_time: DateTime<Utc>,
) -> Result<NodeLeaseTracker> {
    let tracker = NodeLeaseTracker::new_for_test(startup_time);
    let leases = db
        .list_resources(
            "coordination.k8s.io/v1",
            "Lease",
            Some("kube-node-lease"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;
    for lease in leases.items {
        tracker
            .record_from_lease_object(&lease.name, lease.data.as_ref())
            .await?;
    }
    Ok(tracker)
}

pub async fn run_node_lifecycle_controller(
    state: Arc<AppState>,
    node_status: Arc<dyn klights_leader_api::LeaderNodeLifecycleStatus>,
    cancel: CancellationToken,
    _startup_resource_version: i64,
    mut is_leader_rx: watch::Receiver<bool>,
    _raft_node: Option<Arc<RaftNode>>,
) {
    if let Err(err) = refresh_node_lease_tracker_from_cluster_leases(
        state.db.as_ref(),
        state.node_lease_tracker.as_ref(),
    )
    .await
    {
        tracing::warn!(
            "node_lifecycle: failed to seed node lease tracker from persisted leases: {err:#}"
        );
    }

    if *is_leader_rx.borrow() {
        // Already leader at startup: reset the grace window here too, since the
        // wait-based path below only fires when leadership is acquired by waiting.
        state
            .node_lease_tracker
            .reset_grace_window(Utc::now())
            .await;
    } else if !wait_for_leadership(&state, &cancel, &mut is_leader_rx).await {
        return;
    }

    let db = state.db.clone();
    let watch_topics = vec![
        WatchTopic::new("v1", "Node"),
        WatchTopic::new("coordination.k8s.io/v1", "Lease"),
    ];
    let signal_rx = WatchSignalReceiver::new(
        watch_topics
            .iter()
            .cloned()
            .map(|topic| db.subscribe_watch_signals(topic))
            .collect(),
    );
    let mut cursor = SignalWatchCursor::new_many(
        signal_rx,
        DatastoreWatchReplaySource::new(
            std::sync::Arc::new(crate::datastore::DatastoreBackendWatchStore::new(
                db.clone(),
            )),
            vec![
                WatchTarget::cluster("v1", "Node"),
                WatchTarget::cluster("coordination.k8s.io/v1", "Lease"),
            ],
        ),
        watch_topics,
        WatchDeliveryScope::Cluster,
        db.get_current_resource_version().await.unwrap_or(0),
        WindowPolicy::default_watch_delivery(),
    );
    if let Err(err) = cursor.prime_replay_or_expired().await {
        tracing::warn!(?err, "node_lifecycle: initial replay failed");
    }

    let mut retry_attempt = 0u32;
    'controller: loop {
        if !*is_leader_rx.borrow() {
            tracing::debug!("node_lifecycle: not leader, waiting before reconcile");
            retry_attempt = 0;
            if !wait_for_leadership(&state, &cancel, &mut is_leader_rx).await {
                return;
            }
        }

        let next_deadline = match reconcile_node_lifecycle_once_with_tracker(
            db.as_ref(),
            node_status.as_ref(),
            state.pod_repository.as_ref(),
            state.node_lease_tracker.as_ref(),
            Utc::now(),
            Some(state.side_effects.as_ref()),
            state.pod_lifecycle_router.as_deref(),
        )
        .await
        {
            Ok(next_deadline) => {
                retry_attempt = 0;
                next_deadline
            }
            Err(err) => {
                tracing::warn!("node_lifecycle reconcile failed: {err:#}");
                let attempt = retry_attempt;
                retry_attempt = retry_attempt.saturating_add(1);
                if wait_for_retry(&state, &cancel, attempt).await {
                    continue;
                }
                break;
            }
        };

        let maybe_event = if let Some(delay) = next_deadline {
            tokio::select! {
                _ = cancel.cancelled() => None,
                _ = is_leader_rx.changed() => {
                    if !*is_leader_rx.borrow() {
                        tracing::debug!(
                            "node_lifecycle: relinquished leadership, waiting for re-election"
                        );
                        if !wait_for_leadership(&state, &cancel, &mut is_leader_rx).await {
                            return;
                        }
                    }
                    None
                }
                sleep = state.task_supervisor.sleep("node_lifecycle_lease_deadline", delay) => {
                    if let Err(err) = sleep {
                        tracing::warn!("node_lifecycle deadline timer failed: {err:#}");
                    }
                    None
                }
                _ = state.node_lease_tracker.wait_changed() => None,
                event = cursor.next_event() => {
                    Some(event)
                },
            }
        } else {
            tokio::select! {
                _ = cancel.cancelled() => None,
                _ = is_leader_rx.changed() => {
                    if !*is_leader_rx.borrow() {
                        tracing::debug!(
                            "node_lifecycle: relinquished leadership, waiting for re-election"
                        );
                        if !wait_for_leadership(&state, &cancel, &mut is_leader_rx).await {
                            return;
                        }
                    }
                    None
                }
                _ = state.node_lease_tracker.wait_changed() => None,
                event = cursor.next_event() => {
                    Some(event)
                },
            }
        };
        let watch_result = match maybe_event {
            Some(watch_result) => watch_result,
            None => {
                if cancel.is_cancelled() {
                    break;
                }
                continue;
            }
        };
        match watch_result {
            Ok(event) => {
                if let Err(err) =
                    track_lease_from_event(&event, state.node_lease_tracker.as_ref()).await
                {
                    tracing::warn!(
                        "node_lifecycle: failed to refresh lease from watch event: {err:#}"
                    );
                }
                if deleted_node_name(&event).is_some() {
                    loop {
                        match cleanup_pods_bound_to_deleted_node_event(
                            db.as_ref(),
                            state.pod_repository.as_ref(),
                            Some(state.side_effects.as_ref()),
                            state.pod_lifecycle_router.as_deref(),
                            &event,
                        )
                        .await
                        {
                            Ok(true) => {
                                retry_attempt = 0;
                                continue 'controller;
                            }
                            Ok(false) => break,
                            Err(err) => {
                                tracing::warn!(
                                    "node_lifecycle: failed to cleanup pods for deleted node event: {err:#}"
                                );
                                let attempt = retry_attempt;
                                retry_attempt = retry_attempt.saturating_add(1);
                                if !wait_for_retry(&state, &cancel, attempt).await {
                                    break 'controller;
                                }
                            }
                        }
                    }
                }
                if node_lifecycle_event(&event) {
                    continue;
                }
            }
            Err(WatchCursorError::Closed) => break,
            Err(err) => {
                tracing::warn!("node_lifecycle watch error: {err:#?}");
                let attempt = retry_attempt;
                retry_attempt = retry_attempt.saturating_add(1);
                if !wait_for_retry(&state, &cancel, attempt).await {
                    break;
                }
            }
        }
    }
}

async fn wait_for_leadership(
    state: &AppState,
    cancel: &CancellationToken,
    is_leader_rx: &mut watch::Receiver<bool>,
) -> bool {
    loop {
        if *is_leader_rx.borrow() {
            // (Re)acquired leadership: give every node a fresh startup grace
            // window before this term's first reconcile, so a blind new leader
            // does not mass-evict on stale in-memory deadlines (T8).
            state
                .node_lease_tracker
                .reset_grace_window(Utc::now())
                .await;
            return true;
        }

        tokio::select! {
            _ = cancel.cancelled() => return false,
            changed = is_leader_rx.changed() => {
                if changed.is_err() {
                    return false;
                }
                continue;
            }
        }
    }
}

async fn refresh_node_lease_tracker_from_cluster_leases(
    db: &dyn DatastoreBackend,
    tracker: &NodeLeaseTracker,
) -> Result<()> {
    let leases = db
        .list_resources(
            "coordination.k8s.io/v1",
            "Lease",
            Some("kube-node-lease"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;

    for lease in leases.items {
        if let Err(err) = tracker
            .record_from_lease_object(&lease.name, lease.data.as_ref())
            .await
        {
            tracing::warn!(
                node_name = %lease.name,
                "node_lifecycle: failed to seed lease tracker from persisted lease: {err:#}"
            );
        }
    }

    Ok(())
}

async fn track_lease_from_event(event: &WatchEvent, tracker: &NodeLeaseTracker) -> Result<()> {
    if event.event_type == EventType::Bookmark || event.event_type == EventType::Deleted {
        return Ok(());
    }

    if event.object.get("kind").and_then(|k| k.as_str()) != Some("Lease") {
        return Ok(());
    }
    if event
        .object
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        != Some("kube-node-lease")
    {
        return Ok(());
    }

    let node_name = event
        .object
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if node_name.is_empty() {
        return Ok(());
    }

    tracker
        .record_from_lease_object(node_name, event.object.as_ref())
        .await
        .map(|_| ())
}

fn node_lifecycle_retry_delay(attempt: u32) -> Duration {
    let steps = attempt.saturating_add(1).min(12);
    Duration::from_secs((steps * 5) as u64)
}

async fn wait_for_retry(state: &AppState, cancel: &CancellationToken, attempt: u32) -> bool {
    let delay = node_lifecycle_retry_delay(attempt);
    tokio::select! {
        _ = cancel.cancelled() => false,
        _ = state.task_supervisor.sleep("node_lifecycle_retry", delay) => true,
    }
}

async fn update_node_status(
    node_status: &dyn klights_leader_api::LeaderNodeLifecycleStatus,
    node: &Resource,
    data: Value,
) -> Result<()> {
    let status = data.get("status").cloned().unwrap_or_else(|| json!({}));
    let request =
        klights_leader_api::NodeLifecycleStatusRequest::try_new(StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Node".to_string(),
            namespace: None,
            name: node.name.clone(),
            status,
            expected_rv: Some(node.resource_version),
            preconditions: ResourcePreconditions::uid_and_resource_version(
                node.uid.clone(),
                node.resource_version,
            ),
            observed_status_stamp: None,
        })?;
    node_status.submit_node_lifecycle_status(request).await?;
    Ok(())
}

async fn mark_pods_unknown_on_node(
    db: &dyn DatastoreBackend,
    pod_repository: &dyn NodeLifecyclePodRepository,
    side_effects: Option<&crate::side_effects::SideEffectRegistry>,
    pod_lifecycle_router: Option<&PodLifecycleRouter>,
    node_name: &str,
    now: DateTime<Utc>,
) -> Result<Option<Duration>> {
    let field_selector = format!("spec.nodeName={node_name}");
    let pods = pod_repository
        .list_pods(None, None, Some(&field_selector), None, None)
        .await?;
    let mut next_deadline = None;
    for pod in pods.items {
        let mut data = Arc::unwrap_or_clone(pod.data.clone());
        let status_changed = mark_pod_status_unknown(&mut data, now);
        if pod.data.pointer("/metadata/deletionTimestamp").is_none() {
            match stale_node_pod_terminal_deadline(&data) {
                Some(deadline) if deadline <= now => {
                    mark_pod_node_lost_and_enqueue_actor_cleanup(
                        db,
                        pod_repository,
                        side_effects,
                        pod_lifecycle_router,
                        node_name,
                        pod,
                        now,
                    )
                    .await?;
                    continue;
                }
                Some(deadline) => {
                    if let Ok(remaining) = deadline.signed_duration_since(now).to_std() {
                        merge_deadline(&mut next_deadline, Some(remaining));
                    }
                }
                None => {}
            }
        }
        if status_changed {
            let status = data.get("status").cloned().unwrap_or_else(|| json!({}));
            let namespace = pod.namespace.as_deref().unwrap_or("default");
            pod_repository
                .replace_status_from_api_for_uid(
                    namespace,
                    &pod.name,
                    &pod.uid,
                    status,
                    pod.resource_version,
                )
                .await?;
        }
    }
    Ok(next_deadline)
}

async fn mark_pod_node_lost_and_enqueue_actor_cleanup(
    db: &dyn DatastoreBackend,
    pod_repository: &dyn NodeLifecyclePodRepository,
    side_effects: Option<&crate::side_effects::SideEffectRegistry>,
    pod_lifecycle_router: Option<&PodLifecycleRouter>,
    node_name: &str,
    pod: Resource,
    now: DateTime<Utc>,
) -> Result<()> {
    let namespace = pod.namespace.as_deref().unwrap_or("default");
    let mut data = Arc::unwrap_or_clone(pod.data.clone());
    mark_pod_status_node_lost(&mut data, now);
    let status = data.get("status").cloned().unwrap_or_else(|| json!({}));
    pod_repository
        .replace_status_from_api_for_uid(
            namespace,
            &pod.name,
            &pod.uid,
            status,
            pod.resource_version,
        )
        .await
        .with_context(|| {
            format!(
                "mark NodeLost Pod status for {}/{} uid={}",
                namespace, pod.name, pod.uid
            )
        })?;

    run_node_lost_pod_cleanup_side_effects(db, side_effects, namespace, &pod.name, &pod.uid, &data)
        .await?;

    if let Some(router) = pod_lifecycle_router {
        enqueue_orphan_finalize(
            router,
            PodLifecycleKey::new(namespace, &pod.name, &pod.uid),
            OrphanReason::NodeLost,
        )
        .await
        .with_context(|| {
            format!(
                "enqueue actor-owned NodeLost cleanup for {}/{} uid={}",
                namespace, pod.name, pod.uid
            )
        })?;
    } else {
        tracing::debug!(
            node = node_name,
            namespace,
            pod = %pod.name,
            uid = %pod.uid,
            "NodeLost Pod marked without local lifecycle router; watch delivery or later startup will drive actor cleanup"
        );
    }

    Ok(())
}

async fn run_node_lost_pod_cleanup_side_effects(
    db: &dyn DatastoreBackend,
    side_effects: Option<&crate::side_effects::SideEffectRegistry>,
    namespace: &str,
    pod_name: &str,
    pod_uid: &str,
    pod_data: &Value,
) -> Result<()> {
    let Some(side_effects) = side_effects else {
        return Ok(());
    };
    crate::side_effects::service_pod::enqueue_services_after_pod_delete(
        pod_data,
        db,
        &side_effects.controller_dispatcher_slot(),
    )
    .await
    .with_context(|| {
        format!("enqueue Service reconcile after NodeLost cleanup for {namespace}/{pod_name} uid={pod_uid}")
    })?;
    side_effects
        .run_hooks(pod_data, db)
        .await
        .with_context(|| {
            format!("run NodeLost cleanup hooks for {namespace}/{pod_name} uid={pod_uid}")
        })?;
    Ok(())
}

fn merge_deadline(next_deadline: &mut Option<Duration>, candidate: Option<Duration>) {
    if let Some(candidate) = candidate {
        *next_deadline = Some(next_deadline.map_or(candidate, |current| current.min(candidate)));
    }
}

async fn reconcile_node_resources_after_ready(
    pod_repository: &dyn NodeLifecyclePodRepository,
    node_name: &str,
    now: DateTime<Utc>,
) -> Result<()> {
    reconcile_unknown_pods_after_node_ready(pod_repository, node_name, now).await
}

async fn reconcile_unknown_pods_after_node_ready(
    pod_repository: &dyn NodeLifecyclePodRepository,
    node_name: &str,
    now: DateTime<Utc>,
) -> Result<()> {
    let field_selector = format!("spec.nodeName={node_name}");
    let pods = pod_repository
        .list_pods(None, None, Some(&field_selector), None, None)
        .await?;
    for pod in pods.items {
        let mut data = Arc::unwrap_or_clone(pod.data.clone());
        if !restore_pod_status_after_node_ready(&mut data, now) {
            continue;
        }
        let status = data.get("status").cloned().unwrap_or_else(|| json!({}));
        let namespace = pod.namespace.as_deref().unwrap_or("default");
        pod_repository
            .replace_status_from_api_for_uid(
                namespace,
                &pod.name,
                &pod.uid,
                status,
                pod.resource_version,
            )
            .await?;
    }
    Ok(())
}

fn node_lifecycle_event(event: &WatchEvent) -> bool {
    if event.event_type == EventType::Bookmark {
        return false;
    }
    let kind = event.object.get("kind").and_then(|v| v.as_str());
    let api_version = event.object.get("apiVersion").and_then(|v| v.as_str());
    match (api_version, kind) {
        (Some("v1"), Some("Node")) => true,
        (Some("coordination.k8s.io/v1"), Some("Lease")) => {
            event
                .object
                .pointer("/metadata/namespace")
                .and_then(|v| v.as_str())
                == Some("kube-node-lease")
        }
        _ => false,
    }
}

#[cfg(test)]
fn lease_deadline(lease: &Value) -> Option<DateTime<Utc>> {
    let renew_time = lease
        .pointer("/spec/renewTime")
        .and_then(|v| v.as_str())
        .and_then(|raw| chrono::DateTime::parse_from_rfc3339(raw).ok())
        .map(|dt| dt.with_timezone(&Utc))?;
    let duration_seconds = lease
        .pointer("/spec/leaseDurationSeconds")
        .and_then(|v| v.as_i64())
        .filter(|seconds| *seconds > 0)
        .unwrap_or(DEFAULT_NODE_LEASE_DURATION_SECONDS);
    Some(renew_time + chrono::Duration::seconds(duration_seconds))
}

fn mark_node_ready_unknown(node: &mut Value, now: DateTime<Utc>) -> bool {
    let now = k8s_time_format(now);
    let Some(conditions) = node_conditions_mut(node) else {
        return false;
    };
    if let Some(condition) = conditions.iter_mut().find(|c| c["type"] == "Ready") {
        if condition.get("status").and_then(|v| v.as_str()) == Some("Unknown")
            && condition.get("reason").and_then(|v| v.as_str()) == Some(NODE_STATUS_UNKNOWN_REASON)
        {
            return false;
        }
        let previous = condition.clone();
        condition["status"] = json!("Unknown");
        condition["reason"] = json!(NODE_STATUS_UNKNOWN_REASON);
        condition["message"] = json!(NODE_STATUS_UNKNOWN_MESSAGE);
        remove_condition_field(condition, "lastHeartbeatTime");
        crate::controllers::common::preserve_condition_transition_time(
            condition,
            Some(&previous),
            &now,
        );
        return true;
    }

    let mut condition = json!({
        "type": "Ready",
        "status": "Unknown",
        "reason": NODE_STATUS_UNKNOWN_REASON,
        "message": NODE_STATUS_UNKNOWN_MESSAGE
    });
    crate::controllers::common::preserve_condition_transition_time(&mut condition, None, &now);
    conditions.push(condition);
    true
}

fn mark_pod_status_unknown(pod: &mut Value, now: DateTime<Utc>) -> bool {
    if matches!(
        pod.pointer("/status/phase").and_then(|v| v.as_str()),
        Some("Succeeded" | "Failed")
    ) {
        return false;
    }

    let now = k8s_time_format(now);
    let status = ensure_object_field(pod, "status");
    let mut changed = false;
    if status.get("phase").and_then(|v| v.as_str()) != Some("Unknown") {
        status.insert("phase".to_string(), json!("Unknown"));
        changed = true;
    }

    let conditions = status
        .entry("conditions".to_string())
        .or_insert_with(|| json!([]));
    if !conditions.is_array() {
        *conditions = json!([]);
        changed = true;
    }
    let Some(conditions) = conditions.as_array_mut() else {
        return changed;
    };
    for condition_type in ["ContainersReady", "Ready"] {
        if mark_pod_condition_unknown(conditions, condition_type, &now) {
            changed = true;
        }
    }
    changed
}

fn ensure_object_field<'a>(
    value: &'a mut Value,
    field: &str,
) -> &'a mut serde_json::Map<String, Value> {
    if !value.is_object() {
        *value = json!({});
    }
    let obj = value.as_object_mut().expect("object ensured");
    let entry = obj.entry(field.to_string()).or_insert_with(|| json!({}));
    if !entry.is_object() {
        *entry = json!({});
    }
    entry.as_object_mut().expect("object field ensured")
}

fn mark_pod_condition_unknown(
    conditions: &mut Vec<Value>,
    condition_type: &str,
    now: &str,
) -> bool {
    if let Some(condition) = conditions
        .iter_mut()
        .find(|condition| condition.get("type").and_then(|v| v.as_str()) == Some(condition_type))
    {
        if condition.get("status").and_then(|v| v.as_str()) == Some("Unknown")
            && condition.get("reason").and_then(|v| v.as_str()) == Some(NODE_STATUS_UNKNOWN_REASON)
        {
            return false;
        }
        let previous = condition.clone();
        condition["status"] = json!("Unknown");
        condition["reason"] = json!(NODE_STATUS_UNKNOWN_REASON);
        condition["message"] = json!(NODE_STATUS_UNKNOWN_MESSAGE);
        crate::controllers::common::preserve_condition_transition_time(
            condition,
            Some(&previous),
            now,
        );
        return true;
    }

    let mut condition = json!({
        "type": condition_type,
        "status": "Unknown",
        "reason": NODE_STATUS_UNKNOWN_REASON,
        "message": NODE_STATUS_UNKNOWN_MESSAGE
    });
    crate::controllers::common::preserve_condition_transition_time(&mut condition, None, now);
    conditions.push(condition);
    true
}

fn restore_pod_status_after_node_ready(pod: &mut Value, now: DateTime<Utc>) -> bool {
    if matches!(
        pod.pointer("/status/phase").and_then(|v| v.as_str()),
        Some("Succeeded" | "Failed")
    ) {
        return false;
    }
    if !pod_status_has_node_unknown_projection(pod) {
        return false;
    }
    let Some(restored_phase) = infer_pod_phase_from_cluster_status(pod) else {
        return false;
    };

    let now = k8s_time_format(now);
    let existing_conditions = pod
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let readiness_probe_containers = container_names_with_probe(pod, "readinessProbe");
    let startup_probe_containers = container_names_with_probe(pod, "startupProbe");

    let status = ensure_object_field(pod, "status");
    let mut changed = false;
    if status.get("phase").and_then(|v| v.as_str()) != Some(restored_phase.as_str()) {
        status.insert("phase".to_string(), Value::String(restored_phase.clone()));
        changed = true;
    }
    if restore_running_container_readiness(
        status,
        &readiness_probe_containers,
        &startup_probe_containers,
    ) {
        changed = true;
    }

    let all_containers_ready = app_container_statuses_all_ready(status);
    let containers_ready_status = if all_containers_ready {
        "True"
    } else {
        "False"
    };
    let ready_status = if restored_phase == "Running" && all_containers_ready {
        "True"
    } else {
        "False"
    };
    if upsert_reconciled_pod_condition(
        status,
        &existing_conditions,
        "ContainersReady",
        containers_ready_status,
        &now,
    ) {
        changed = true;
    }
    if upsert_reconciled_pod_condition(status, &existing_conditions, "Ready", ready_status, &now) {
        changed = true;
    }
    changed
}

fn mark_pod_status_node_lost(pod: &mut Value, now: DateTime<Utc>) {
    let _ = mark_pod_status_unknown(pod, now);
    let status = ensure_object_field(pod, "status");
    status.insert("phase".to_string(), json!("Failed"));
    status.insert("reason".to_string(), json!(POD_CLEANUP_REASON_NODE_LOST));
    status.insert(
        "message".to_string(),
        json!("Pod was terminated because its Node was lost."),
    );
}

fn pod_status_has_node_unknown_projection(pod: &Value) -> bool {
    pod.pointer("/status/phase").and_then(|v| v.as_str()) == Some("Unknown")
        || ["ContainersReady", "Ready"].iter().any(|condition_type| {
            pod.pointer("/status/conditions")
                .and_then(|v| v.as_array())
                .into_iter()
                .flatten()
                .any(|condition| {
                    condition.get("type").and_then(|v| v.as_str()) == Some(*condition_type)
                        && condition.get("status").and_then(|v| v.as_str()) == Some("Unknown")
                        && condition.get("reason").and_then(|v| v.as_str())
                            == Some(NODE_STATUS_UNKNOWN_REASON)
                })
        })
}

fn stale_node_pod_terminal_deadline(pod: &Value) -> Option<DateTime<Utc>> {
    node_unknown_transition_time(pod).map(|transition_time| {
        transition_time + chrono::Duration::seconds(node_not_ready_pod_eviction_grace_seconds())
    })
}

fn node_not_ready_pod_eviction_grace_seconds() -> i64 {
    parse_node_not_ready_pod_eviction_grace_seconds(
        std::env::var(NODE_NOT_READY_POD_EVICTION_GRACE_ENV).ok(),
    )
}

fn parse_node_not_ready_pod_eviction_grace_seconds(raw: Option<String>) -> i64 {
    raw.as_deref()
        .and_then(|value| value.trim().parse::<i64>().ok())
        .filter(|seconds| *seconds >= 0)
        .unwrap_or(DEFAULT_NODE_NOT_READY_POD_EVICTION_GRACE_SECONDS)
}

fn node_unknown_transition_time(pod: &Value) -> Option<DateTime<Utc>> {
    pod.pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .and_then(|conditions| {
            conditions.iter().find_map(|condition| {
                if condition.get("type").and_then(|v| v.as_str()) != Some("Ready")
                    || condition.get("status").and_then(|v| v.as_str()) != Some("Unknown")
                    || condition.get("reason").and_then(|v| v.as_str())
                        != Some(NODE_STATUS_UNKNOWN_REASON)
                {
                    return None;
                }
                condition
                    .get("lastTransitionTime")
                    .and_then(|v| v.as_str())
                    .and_then(|raw| chrono::DateTime::parse_from_rfc3339(raw).ok())
                    .map(|dt| dt.with_timezone(&Utc))
            })
        })
}

fn infer_pod_phase_from_cluster_status(pod: &Value) -> Option<String> {
    let statuses = pod
        .pointer("/status/containerStatuses")
        .and_then(|v| v.as_array())?;
    if statuses.is_empty() {
        return None;
    }

    let mut any_running = false;
    let mut all_terminated = true;
    let mut any_terminated_nonzero = false;
    let mut all_terminated_zero = true;
    for status in statuses {
        if status.pointer("/state/running").is_some() {
            any_running = true;
            all_terminated = false;
            all_terminated_zero = false;
            continue;
        }
        let Some(exit_code) = status
            .pointer("/state/terminated/exitCode")
            .and_then(value_as_i64)
        else {
            all_terminated = false;
            all_terminated_zero = false;
            continue;
        };
        if exit_code != 0 {
            any_terminated_nonzero = true;
            all_terminated_zero = false;
        }
    }

    if any_running {
        return Some("Running".to_string());
    }

    let restart_policy = pod
        .pointer("/spec/restartPolicy")
        .and_then(|v| v.as_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("Always");
    if all_terminated {
        if restart_policy == "Always" || (restart_policy == "OnFailure" && any_terminated_nonzero) {
            return Some("Running".to_string());
        }
        if all_terminated_zero && matches!(restart_policy, "Never" | "OnFailure") {
            return Some("Succeeded".to_string());
        }
        if any_terminated_nonzero && restart_policy == "Never" {
            return Some("Failed".to_string());
        }
    }

    Some("Pending".to_string())
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
}

fn container_names_with_probe(pod: &Value, probe_field: &str) -> HashSet<String> {
    pod.pointer("/spec/containers")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
        .filter(|container| container.get(probe_field).is_some())
        .filter_map(|container| {
            container
                .get("name")
                .and_then(|name| name.as_str())
                .map(str::to_string)
        })
        .collect()
}

fn restore_running_container_readiness(
    status: &mut serde_json::Map<String, Value>,
    readiness_probe_containers: &HashSet<String>,
    startup_probe_containers: &HashSet<String>,
) -> bool {
    let Some(statuses) = status
        .get_mut("containerStatuses")
        .and_then(|v| v.as_array_mut())
    else {
        return false;
    };
    let mut changed = false;
    for item in statuses {
        if item.pointer("/state/running").is_none() {
            continue;
        }
        let Some(name) = item
            .get("name")
            .and_then(|v| v.as_str())
            .map(str::to_string)
        else {
            continue;
        };
        if !readiness_probe_containers.contains(&name)
            && item.get("ready").and_then(|v| v.as_bool()) != Some(true)
        {
            item["ready"] = json!(true);
            changed = true;
        }
        if !startup_probe_containers.contains(&name)
            && item.get("started").is_some()
            && item.get("started").and_then(|v| v.as_bool()) != Some(true)
        {
            item["started"] = json!(true);
            changed = true;
        }
    }
    changed
}

fn app_container_statuses_all_ready(status: &serde_json::Map<String, Value>) -> bool {
    status
        .get("containerStatuses")
        .and_then(|v| v.as_array())
        .is_some_and(|statuses| {
            !statuses.is_empty()
                && statuses.iter().all(|status| {
                    status.get("ready").and_then(|ready| ready.as_bool()) == Some(true)
                })
        })
}

fn upsert_reconciled_pod_condition(
    status: &mut serde_json::Map<String, Value>,
    existing_conditions: &[Value],
    condition_type: &str,
    condition_status: &str,
    now: &str,
) -> bool {
    let conditions = status
        .entry("conditions".to_string())
        .or_insert_with(|| Value::Array(Vec::new()));
    let mut changed = false;
    if !conditions.is_array() {
        *conditions = Value::Array(Vec::new());
        changed = true;
    }
    let Some(conditions) = conditions.as_array_mut() else {
        return changed;
    };
    let previous =
        crate::controllers::common::condition_by_type(existing_conditions, condition_type);
    if let Some(condition) = conditions
        .iter_mut()
        .find(|condition| condition.get("type").and_then(|v| v.as_str()) == Some(condition_type))
    {
        if set_condition_string_field(condition, "status", condition_status) {
            changed = true;
        }
        if remove_condition_field(condition, "reason") {
            changed = true;
        }
        if remove_condition_field(condition, "message") {
            changed = true;
        }
        let previous_transition = condition.get("lastTransitionTime").cloned();
        crate::controllers::common::preserve_condition_transition_time(condition, previous, now);
        if condition.get("lastTransitionTime") != previous_transition.as_ref() {
            changed = true;
        }
        return changed;
    }

    let mut condition = json!({
        "type": condition_type,
        "status": condition_status
    });
    crate::controllers::common::preserve_condition_transition_time(&mut condition, previous, now);
    conditions.push(condition);
    true
}

fn set_condition_string_field(condition: &mut Value, field: &str, value: &str) -> bool {
    if !condition.is_object() {
        *condition = json!({});
    }
    if condition.get(field).and_then(|v| v.as_str()) == Some(value) {
        return false;
    }
    condition[field] = json!(value);
    true
}

fn remove_condition_field(condition: &mut Value, field: &str) -> bool {
    let Some(obj) = condition.as_object_mut() else {
        return false;
    };
    obj.remove(field).is_some()
}

fn mark_node_ready_from_fresh_observation(
    node: &mut Value,
    _lease: &NodeLeaseObservation,
    now: DateTime<Utc>,
) -> bool {
    if network_unavailable(node) {
        return false;
    }

    let transition_time = k8s_time_format(now);
    let Some(conditions) = node_conditions_mut(node) else {
        return false;
    };
    if let Some(condition) = conditions.iter_mut().find(|c| c["type"] == "Ready") {
        if condition.get("status").and_then(|v| v.as_str()) == Some("True") {
            return false;
        }
        if condition.get("status").and_then(|v| v.as_str()) != Some("Unknown")
            || condition.get("reason").and_then(|v| v.as_str()) != Some(NODE_STATUS_UNKNOWN_REASON)
        {
            return false;
        }
        let previous = condition.clone();
        condition["status"] = json!("True");
        condition["reason"] = json!(NODE_READY_REASON);
        condition["message"] = json!(NODE_READY_MESSAGE);
        remove_condition_field(condition, "lastHeartbeatTime");
        crate::controllers::common::preserve_condition_transition_time(
            condition,
            Some(&previous),
            &transition_time,
        );
        return true;
    }

    let mut condition = json!({
        "type": "Ready",
        "status": "True",
        "reason": NODE_READY_REASON,
        "message": NODE_READY_MESSAGE
    });
    crate::controllers::common::preserve_condition_transition_time(
        &mut condition,
        None,
        &transition_time,
    );
    conditions.push(condition);
    true
}

fn network_unavailable(node: &Value) -> bool {
    node.pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
        .any(|condition| {
            condition.get("type").and_then(|v| v.as_str()) == Some("NetworkUnavailable")
                && condition.get("status").and_then(|v| v.as_str()) == Some("True")
        })
}

fn node_conditions_mut(node: &mut Value) -> Option<&mut Vec<Value>> {
    let node_obj = node.as_object_mut()?;
    let status = node_obj.entry("status").or_insert_with(|| json!({}));
    if !status.is_object() {
        *status = json!({});
    }
    let status_obj = status.as_object_mut()?;
    let conditions = status_obj.entry("conditions").or_insert_with(|| json!([]));
    if !conditions.is_array() {
        *conditions = json!([]);
    }
    conditions.as_array_mut()
}

fn node_ready_condition_true(node: &Value) -> bool {
    node.pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
        .any(|condition| {
            condition.get("type").and_then(|v| v.as_str()) == Some("Ready")
                && condition.get("status").and_then(|v| v.as_str()) == Some("True")
        })
}

fn ready_status_heartbeat_deadline(node: &Value) -> Option<DateTime<Utc>> {
    let heartbeat = node
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
        .find(|condition| {
            condition.get("type").and_then(|v| v.as_str()) == Some("Ready")
                && condition.get("status").and_then(|v| v.as_str()) == Some("True")
        })?
        .get("lastHeartbeatTime")
        .and_then(|v| v.as_str())
        .and_then(|raw| chrono::DateTime::parse_from_rfc3339(raw).ok())
        .map(|dt| dt.with_timezone(&Utc))?;
    Some(heartbeat + chrono::Duration::seconds(DEFAULT_NODE_LEASE_GRACE_SECONDS))
}

#[cfg(test)]
mod tests {
    // These tests hold TEST_ENV_LOCK across awaits on purpose: the guard
    // serializes env-var mutation for the whole test body, so dropping it
    // before the awaited reconcile would reintroduce the cross-test env race.
    #![allow(clippy::await_holding_lock)]
    use crate::watch::{EventType, WatchEvent};
    use chrono::{TimeZone, Utc};
    use serde_json::json;

    /// Test-only env var guard: sets a var for the test's duration and
    /// restores the prior value on drop. Use under `crate::TEST_ENV_LOCK`.
    struct EnvVarGuard {
        name: &'static str,
        previous: Option<std::ffi::OsString>,
    }
    impl EnvVarGuard {
        fn set(name: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(name);
            unsafe { std::env::set_var(name, value) };
            Self { name, previous }
        }
        fn remove(name: &'static str) -> Self {
            let previous = std::env::var_os(name);
            unsafe { std::env::remove_var(name) };
            Self { name, previous }
        }
    }
    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(v) => unsafe { std::env::set_var(self.name, v) },
                None => unsafe { std::env::remove_var(self.name) },
            }
        }
    }

    fn test_lifecycle_router() -> (
        std::sync::Arc<crate::kubelet::pod_lifecycle_router::PodLifecycleRouter>,
        std::sync::Arc<crate::kubelet::pod_lifecycle_router::executor::RecordingExecutor>,
    ) {
        let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let recorder = crate::kubelet::pod_lifecycle_router::executor::RecordingExecutor::new();
        let executor: std::sync::Arc<
            dyn crate::kubelet::pod_lifecycle_router::executor::PodWorkExecutor,
        > = recorder.clone();
        let registry = std::sync::Arc::new(
            crate::kubelet::pod_lifecycle_actor::registry::PodLifecycleRegistry::new(
                supervisor,
                crate::kubelet::pod_lifecycle_actor::config::PodLifecycleConcurrencyConfig::production_default(),
                std::sync::Arc::new(std::sync::Mutex::new(executor)),
            ),
        );
        (
            std::sync::Arc::new(
                crate::kubelet::pod_lifecycle_router::PodLifecycleRouter::new_actor_with_executor(
                    registry,
                    recorder.clone(),
                ),
            ),
            recorder,
        )
    }

    #[tokio::test]
    async fn track_lease_from_event_updates_tracker() {
        let tracker = crate::node_lease_tracker::NodeLeaseTracker::new_for_test(
            Utc.with_ymd_and_hms(2026, 5, 13, 6, 30, 0).unwrap(),
        );
        let event = WatchEvent::added(json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": "worker-a",
                "namespace": "kube-node-lease",
                "resourceVersion": "1"
            },
            "spec": {
                "holderIdentity": "worker-a",
                "leaseDurationSeconds": 40,
                "renewTime": "2026-05-13T06:34:15.000000Z"
            }
        }));

        super::track_lease_from_event(&event, &tracker)
            .await
            .expect("event should refresh local lease tracker");

        let tracked = tracker.deadline_for_node("worker-a").await.observed;
        assert!(tracked.is_some());
        assert_eq!(
            tracked.as_ref().map(|obs| obs.renew_time.to_string()),
            Some("2026-05-13 06:34:15 UTC".to_string())
        );
    }

    #[tokio::test]
    async fn track_lease_from_event_ignores_deleted_lease() {
        let tracker = crate::node_lease_tracker::NodeLeaseTracker::new_for_test(
            Utc.with_ymd_and_hms(2026, 5, 13, 6, 30, 0).unwrap(),
        );
        let event = WatchEvent {
            event_type: EventType::Deleted,
            object: std::sync::Arc::new(json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {
                    "name": "worker-a",
                    "namespace": "kube-node-lease",
                },
                "spec": {
                    "holderIdentity": "worker-a",
                    "leaseDurationSeconds": 40,
                    "renewTime": "2026-05-13T06:34:15.000000Z"
                }
            })),
            encoded_payload: None,
        };

        super::track_lease_from_event(&event, &tracker)
            .await
            .expect("deleted events should be ignored");
        assert!(
            tracker
                .deadline_for_node("worker-a")
                .await
                .observed
                .is_none()
        );
    }

    #[tokio::test]
    async fn stale_node_lease_marks_ready_unknown() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "worker-a",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "worker-a"},
                "status": {
                    "conditions": [
                        {
                            "type": "Ready",
                            "status": "True",
                            "reason": "KubeletReady",
                            "message": "klights is ready",
                            "lastHeartbeatTime": "2026-05-13T06:34:14Z",
                            "lastTransitionTime": "2026-05-13T06:34:14Z"
                        },
                        {
                            "type": "MemoryPressure",
                            "status": "False",
                            "reason": "KubeletHasSufficientMemory",
                            "message": "kubelet has sufficient memory available",
                            "lastHeartbeatTime": "2026-05-13T06:34:14Z",
                            "lastTransitionTime": "2026-05-13T06:34:14Z"
                        }
                    ]
                }
            }),
        )
        .await
        .unwrap();
        db.create_resource(
            "coordination.k8s.io/v1",
            "Lease",
            Some("kube-node-lease"),
            "worker-a",
            json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": "worker-a", "namespace": "kube-node-lease"},
                "spec": {
                    "holderIdentity": "worker-a",
                    "leaseDurationSeconds": 40,
                    "renewTime": "2026-05-13T06:34:15.000000Z"
                }
            }),
        )
        .await
        .unwrap();

        let next = super::reconcile_node_lifecycle_once(
            &db,
            Utc.with_ymd_and_hms(2026, 5, 13, 6, 34, 56).unwrap(),
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "worker-a")
            .await
            .unwrap()
            .unwrap();
        let ready = node.data["status"]["conditions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|condition| condition["type"] == "Ready")
            .unwrap();
        assert_eq!(ready["status"], "Unknown");
        assert_eq!(ready["reason"], "NodeStatusUnknown");
        assert_eq!(ready["message"], "Kubelet stopped posting node status.");
        assert!(
            ready.get("lastHeartbeatTime").is_none(),
            "leader must not persist the churny lastHeartbeatTime field"
        );
        assert_eq!(
            ready["lastTransitionTime"], "2026-05-13T06:34:56Z",
            "transition time records when the leader observed the stale node"
        );
        assert!(
            next.is_none(),
            "already-stale nodes should not schedule a hot retry after being marked Unknown"
        );
    }

    #[tokio::test]
    async fn stale_node_lease_marks_bound_pods_unknown() {
        // This test verifies the Unknown projection in the window before
        // cleanup, so it pins a non-zero eviction grace (the default is now 0
        // = immediate cleanup; see default_zero_grace_cleans_stale_node_pod).
        let _env_lock = crate::TEST_ENV_LOCK.lock().unwrap();
        let _grace = EnvVarGuard::set("KLIGHTS_NODE_NOT_READY_POD_EVICTION_GRACE_SECONDS", "30");
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "worker-a",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "worker-a"},
                "status": {
                    "conditions": [{
                        "type": "Ready",
                        "status": "True",
                        "reason": "KubeletReady",
                        "message": "klights is ready",
                        "lastHeartbeatTime": "2026-05-13T06:34:14Z",
                        "lastTransitionTime": "2026-05-13T06:34:14Z"
                    }]
                }
            }),
        )
        .await
        .unwrap();
        db.create_resource(
            "coordination.k8s.io/v1",
            "Lease",
            Some("kube-node-lease"),
            "worker-a",
            json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": "worker-a", "namespace": "kube-node-lease"},
                "spec": {
                    "holderIdentity": "worker-a",
                    "leaseDurationSeconds": 40,
                    "renewTime": "2026-05-13T06:34:15.000000Z"
                }
            }),
        )
        .await
        .unwrap();
        seed_running_pod_on_node(&db, "worker-pod", "worker-pod-uid", "worker-a").await;
        seed_running_pod_on_node(&db, "other-pod", "other-pod-uid", "worker-b").await;

        super::reconcile_node_lifecycle_once(
            &db,
            Utc.with_ymd_and_hms(2026, 5, 13, 6, 34, 56).unwrap(),
        )
        .await
        .unwrap();

        let worker_pod = db
            .get_resource("v1", "Pod", Some("default"), "worker-pod")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(worker_pod.data["status"]["phase"], "Unknown");
        let ready = pod_condition(&worker_pod.data, "Ready");
        assert_eq!(ready["status"], "Unknown");
        assert_eq!(ready["reason"], "NodeStatusUnknown");
        assert_eq!(
            ready["message"], "Kubelet stopped posting node status.",
            "pod Unknown reason should explain that the node heartbeat went stale"
        );
        let containers_ready = pod_condition(&worker_pod.data, "ContainersReady");
        assert_eq!(containers_ready["status"], "Unknown");
        assert_eq!(containers_ready["reason"], "NodeStatusUnknown");
        assert_eq!(
            worker_pod.data["status"]["containerStatuses"][0]["ready"], true,
            "node-lifecycle Unknown projection must preserve the worker's last known container readiness"
        );
        assert_eq!(
            worker_pod.data["status"]["containerStatuses"][0]["started"], true,
            "node-lifecycle Unknown projection must preserve the worker's last known container start state"
        );
        assert!(
            worker_pod
                .data
                .pointer("/metadata/deletionTimestamp")
                .is_none(),
            "stale-node pods must stay Unknown during the pod eviction grace period"
        );

        let other_pod = db
            .get_resource("v1", "Pod", Some("default"), "other-pod")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            other_pod.data["status"]["phase"], "Running",
            "only pods bound to the stale node should be projected Unknown"
        );
    }

    #[tokio::test]
    async fn fresh_node_status_heartbeat_prevents_stale_lease_pod_cleanup() {
        let _env_lock = crate::TEST_ENV_LOCK.lock().unwrap();
        let _grace = EnvVarGuard::remove("KLIGHTS_NODE_NOT_READY_POD_EVICTION_GRACE_SECONDS");
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "worker-a",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "worker-a"},
                "status": {
                    "conditions": [{
                        "type": "Ready",
                        "status": "True",
                        "reason": "KubeletReady",
                        "message": "klights is ready",
                        "lastHeartbeatTime": "2026-05-13T06:34:55Z",
                        "lastTransitionTime": "2026-05-13T06:34:14Z"
                    }]
                }
            }),
        )
        .await
        .unwrap();
        db.create_resource(
            "coordination.k8s.io/v1",
            "Lease",
            Some("kube-node-lease"),
            "worker-a",
            json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": "worker-a", "namespace": "kube-node-lease"},
                "spec": {
                    "holderIdentity": "worker-a",
                    "leaseDurationSeconds": 40,
                    "renewTime": "2026-05-13T06:34:15.000000Z"
                }
            }),
        )
        .await
        .unwrap();
        seed_running_pod_on_node(&db, "worker-pod", "worker-pod-uid", "worker-a").await;

        let next = super::reconcile_node_lifecycle_once(
            &db,
            Utc.with_ymd_and_hms(2026, 5, 13, 6, 34, 56).unwrap(),
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "worker-a")
            .await
            .unwrap()
            .unwrap();
        let ready = node.data["status"]["conditions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|condition| condition["type"] == "Ready")
            .unwrap();
        assert_eq!(
            ready["status"], "True",
            "fresh Node status heartbeat must prevent stale-lease Unknown projection"
        );
        let pod = db
            .get_resource("v1", "Pod", Some("default"), "worker-pod")
            .await
            .unwrap()
            .expect("fresh node status heartbeat must preserve running pod");
        assert_eq!(pod.data["status"]["phase"], "Running");
        assert!(
            next.is_some(),
            "controller should sleep until the fresh node-status heartbeat deadline"
        );
    }

    #[tokio::test]
    async fn default_zero_grace_marks_stale_node_pod_node_lost_immediately() {
        // With the default eviction grace (0), a pod on a confirmed-stale node
        // is marked NodeLost in the same reconcile pass. Actor finalization owns
        // the eventual API row removal.
        let _env_lock = crate::TEST_ENV_LOCK.lock().unwrap();
        let _grace = EnvVarGuard::remove("KLIGHTS_NODE_NOT_READY_POD_EVICTION_GRACE_SECONDS");
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "worker-a",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "worker-a"},
                "status": {
                    "conditions": [{
                        "type": "Ready",
                        "status": "True",
                        "reason": "KubeletReady",
                        "message": "klights is ready",
                        "lastHeartbeatTime": "2026-05-13T06:34:14Z",
                        "lastTransitionTime": "2026-05-13T06:34:14Z"
                    }]
                }
            }),
        )
        .await
        .unwrap();
        db.create_resource(
            "coordination.k8s.io/v1",
            "Lease",
            Some("kube-node-lease"),
            "worker-a",
            json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": "worker-a", "namespace": "kube-node-lease"},
                "spec": {
                    "holderIdentity": "worker-a",
                    "leaseDurationSeconds": 40,
                    "renewTime": "2026-05-13T06:34:15.000000Z"
                }
            }),
        )
        .await
        .unwrap();
        seed_running_pod_on_node(&db, "worker-pod", "worker-pod-uid", "worker-a").await;

        super::reconcile_node_lifecycle_once(
            &db,
            Utc.with_ymd_and_hms(2026, 5, 13, 6, 34, 56).unwrap(),
        )
        .await
        .unwrap();

        let pod = db
            .get_resource("v1", "Pod", Some("default"), "worker-pod")
            .await
            .unwrap()
            .expect("controller must preserve the API row for actor-owned finalization");
        assert_eq!(pod.data["status"]["phase"], "Failed");
        assert_eq!(pod.data["status"]["reason"], "NodeLost");
    }

    #[tokio::test]
    async fn stale_node_lease_marks_unknown_bound_pods_node_lost_after_grace() {
        // Exercises the within-grace -> after-grace staging, so it pins a
        // non-zero eviction grace (the default is now 0 = immediate cleanup).
        let _env_lock = crate::TEST_ENV_LOCK.lock().unwrap();
        let _grace = EnvVarGuard::set("KLIGHTS_NODE_NOT_READY_POD_EVICTION_GRACE_SECONDS", "30");
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "worker-a",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "worker-a"},
                "status": {
                    "conditions": [{
                        "type": "Ready",
                        "status": "True",
                        "reason": "KubeletReady",
                        "message": "klights is ready",
                        "lastHeartbeatTime": "2026-05-13T06:34:14Z",
                        "lastTransitionTime": "2026-05-13T06:34:14Z"
                    }]
                }
            }),
        )
        .await
        .unwrap();
        db.create_resource(
            "coordination.k8s.io/v1",
            "Lease",
            Some("kube-node-lease"),
            "worker-a",
            json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": "worker-a", "namespace": "kube-node-lease"},
                "spec": {
                    "holderIdentity": "worker-a",
                    "leaseDurationSeconds": 40,
                    "renewTime": "2026-05-13T06:34:15.000000Z"
                }
            }),
        )
        .await
        .unwrap();
        seed_running_pod_on_node(&db, "worker-pod", "worker-pod-uid", "worker-a").await;

        super::reconcile_node_lifecycle_once(
            &db,
            Utc.with_ymd_and_hms(2026, 5, 13, 6, 34, 56).unwrap(),
        )
        .await
        .unwrap();
        let within_grace = db
            .get_resource("v1", "Pod", Some("default"), "worker-pod")
            .await
            .unwrap()
            .unwrap();
        assert!(
            within_grace
                .data
                .pointer("/metadata/deletionTimestamp")
                .is_none(),
            "pod should not terminate before the stale-node eviction grace period"
        );

        super::reconcile_node_lifecycle_once(
            &db,
            Utc.with_ymd_and_hms(2026, 5, 13, 6, 35, 26).unwrap(),
        )
        .await
        .unwrap();
        let after_grace = db
            .get_resource("v1", "Pod", Some("default"), "worker-pod")
            .await
            .unwrap()
            .expect("controller must preserve the API row for actor-owned finalization");
        assert_eq!(after_grace.data["metadata"]["uid"], "worker-pod-uid");
        assert_eq!(after_grace.data["spec"]["nodeName"], "worker-a");
        assert_eq!(after_grace.data["status"]["phase"], "Failed");
        assert_eq!(after_grace.data["status"]["reason"], "NodeLost");
        assert!(
            after_grace
                .data
                .pointer("/metadata/deletionTimestamp")
                .is_none(),
            "NodeLost cleanup must not synthesize a controller-side delete mark"
        );
    }

    #[tokio::test]
    async fn deleted_node_event_marks_bound_pods_node_lost_and_wakes_actor() {
        let db = crate::datastore::test_support::in_memory().await;
        let pod_repository = crate::controllers::test_utils::pod_repository_for_test(&db);
        let (router, recorder) = test_lifecycle_router();
        seed_running_pod_on_node(&db, "fake-node-pod", "fake-node-pod-uid", "e2e-fake-node").await;
        seed_running_pod_on_node(&db, "real-node-pod", "real-node-pod-uid", "real-node").await;

        let event = WatchEvent::deleted(json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "e2e-fake-node"}
        }));

        let cleaned = super::cleanup_pods_bound_to_deleted_node_event(
            &db,
            pod_repository.as_ref(),
            None,
            Some(router.as_ref()),
            &event,
        )
        .await
        .expect("deleted Node cleanup must succeed");

        assert!(cleaned, "deleted Node event should be handled");
        let fake_node_pod = db
            .get_resource("v1", "Pod", Some("default"), "fake-node-pod")
            .await
            .unwrap()
            .expect("controller must leave picked-up Pods for actor-owned finalization");
        assert_eq!(fake_node_pod.data["status"]["phase"], "Failed");
        assert_eq!(fake_node_pod.data["status"]["reason"], "NodeLost");
        assert!(
            db.get_resource("v1", "Pod", Some("default"), "real-node-pod")
                .await
                .unwrap()
                .is_some(),
            "cleanup must be scoped to the deleted Node name"
        );

        for _ in 0..1000 {
            if recorder.action_count() > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        let actions = recorder.take_actions();
        assert!(
            actions.iter().any(|action| {
                matches!(
                    action,
                    crate::kubelet::pod_lifecycle_core::action::PodAction::StopPod {
                        key,
                        ..
                    } if key.namespace == "default"
                        && key.name == "fake-node-pod"
                        && key.uid == "fake-node-pod-uid"
                )
            }),
            "deleted-node cleanup must wake the UID-bound lifecycle actor: {actions:?}"
        );
    }

    #[tokio::test]
    async fn node_lost_cleanup_enqueues_owning_replicaset_after_node_lost_mark() {
        // Uses the staged within-grace -> after-grace timing, so it pins a
        // non-zero eviction grace (the default is now 0 = immediate cleanup).
        let _env_lock = crate::TEST_ENV_LOCK.lock().unwrap();
        let _grace = EnvVarGuard::set("KLIGHTS_NODE_NOT_READY_POD_EVICTION_GRACE_SECONDS", "30");
        let state = crate::api::test_support::build_test_app_state().await;
        state
            .db
            .create_resource(
                "v1",
                "Node",
                None,
                "worker-a",
                json!({
                    "apiVersion": "v1",
                    "kind": "Node",
                    "metadata": {"name": "worker-a"},
                    "status": {
                        "conditions": [{
                            "type": "Ready",
                            "status": "True",
                            "reason": "KubeletReady",
                            "message": "klights is ready",
                            "lastHeartbeatTime": "2026-05-13T06:34:14Z",
                            "lastTransitionTime": "2026-05-13T06:34:14Z"
                        }]
                    }
                }),
            )
            .await
            .unwrap();
        state
            .node_lease_tracker
            .record_from_lease_object(
                "worker-a",
                &json!({
                    "apiVersion": "coordination.k8s.io/v1",
                    "kind": "Lease",
                    "metadata": {"name": "worker-a", "namespace": "kube-node-lease"},
                    "spec": {
                        "holderIdentity": "worker-a",
                        "leaseDurationSeconds": 40,
                        "renewTime": "2026-05-13T06:34:15.000000Z"
                    }
                }),
            )
            .await
            .unwrap();
        state
            .db
            .create_resource(
                "apps/v1",
                "ReplicaSet",
                Some("default"),
                "owned-rs",
                json!({
                    "apiVersion": "apps/v1",
                    "kind": "ReplicaSet",
                    "metadata": {
                        "name": "owned-rs",
                        "namespace": "default",
                        "uid": "owned-rs-uid"
                    },
                    "spec": {
                        "replicas": 1,
                        "selector": {"matchLabels": {"app": "lost"}},
                        "template": {
                            "metadata": {"labels": {"app": "lost"}},
                            "spec": {"containers": [{"name": "app", "image": "registry.k8s.io/pause:3.10"}]}
                        }
                    }
                }),
            )
            .await
            .unwrap();
        state
            .db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "lost-pod",
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "lost-pod",
                        "uid": "lost-pod-uid",
                        "creationTimestamp": "2026-05-13T06:30:00Z",
                        "labels": {"app": "lost"},
                        "ownerReferences": [{
                            "apiVersion": "apps/v1",
                            "kind": "ReplicaSet",
                            "name": "owned-rs",
                            "uid": "owned-rs-uid",
                            "controller": true
                        }]
                    },
                    "spec": {
                        "nodeName": "worker-a",
                        "containers": [{"name": "app", "image": "registry.k8s.io/pause:3.10"}]
                    },
                    "status": {
                        "phase": "Running",
                        "conditions": [
                            {
                                "type": "ContainersReady",
                                "status": "True",
                                "lastTransitionTime": "2026-05-13T06:30:10Z"
                            },
                            {
                                "type": "Ready",
                                "status": "True",
                                "lastTransitionTime": "2026-05-13T06:30:10Z"
                            }
                        ]
                    }
                }),
            )
            .await
            .unwrap();

        super::reconcile_node_lifecycle_once_with_tracker(
            state.db.as_ref(),
            &super::TestNodeLifecycleStatus(state.db.as_ref()),
            state.pod_repository.as_ref(),
            state.node_lease_tracker.as_ref(),
            Utc.with_ymd_and_hms(2026, 5, 13, 6, 34, 56).unwrap(),
            Some(state.side_effects.as_ref()),
            None,
        )
        .await
        .unwrap();
        let pre_cleanup_keys = state.controller_dispatcher.pending_reconcile_keys().await;
        for _ in 0..pre_cleanup_keys.len() {
            let _ = state
                .controller_dispatcher
                .take_reconcile_key_for_test()
                .await;
        }
        super::reconcile_node_lifecycle_once_with_tracker(
            state.db.as_ref(),
            &super::TestNodeLifecycleStatus(state.db.as_ref()),
            state.pod_repository.as_ref(),
            state.node_lease_tracker.as_ref(),
            Utc.with_ymd_and_hms(2026, 5, 13, 6, 35, 26).unwrap(),
            Some(state.side_effects.as_ref()),
            None,
        )
        .await
        .unwrap();

        let lost_pod = state
            .db
            .get_resource("v1", "Pod", Some("default"), "lost-pod")
            .await
            .unwrap()
            .expect("controller must preserve the Pod row for actor-owned finalization");
        assert_eq!(lost_pod.data["status"]["phase"], "Failed");
        assert_eq!(lost_pod.data["status"]["reason"], "NodeLost");
        let keys = state.controller_dispatcher.pending_reconcile_keys().await;
        assert!(
            keys.iter().any(|key| {
                key.api_version() == "apps/v1"
                    && key.kind() == "ReplicaSet"
                    && key.namespace() == Some("default")
                    && key.name() == "owned-rs"
            }),
            "NodeLost cleanup must enqueue the owning ReplicaSet so it can reschedule"
        );
    }

    #[test]
    fn node_not_ready_pod_eviction_grace_defaults_to_zero() {
        // Default 0 = immediate cleanup once the node is confirmed Unknown.
        assert_eq!(
            super::parse_node_not_ready_pod_eviction_grace_seconds(None),
            0
        );
        assert_eq!(
            super::parse_node_not_ready_pod_eviction_grace_seconds(Some("bad".to_string())),
            0
        );
        assert_eq!(
            super::parse_node_not_ready_pod_eviction_grace_seconds(Some("-1".to_string())),
            0
        );
    }

    #[test]
    fn node_not_ready_pod_eviction_grace_accepts_env_override_value() {
        assert_eq!(
            super::parse_node_not_ready_pod_eviction_grace_seconds(Some("45".to_string())),
            45
        );
        assert_eq!(
            super::parse_node_not_ready_pod_eviction_grace_seconds(Some("0".to_string())),
            0
        );
    }

    #[test]
    fn node_lifecycle_retry_delay_increases_linearly_and_caps_at_sixty_seconds() {
        let expected = [
            (0, 5),
            (1, 10),
            (2, 15),
            (3, 20),
            (4, 25),
            (5, 30),
            (6, 35),
            (7, 40),
            (8, 45),
            (9, 50),
            (10, 55),
            (11, 60),
            (12, 60),
            (99, 60),
        ];
        for (attempt, seconds) in expected {
            assert_eq!(
                super::node_lifecycle_retry_delay(attempt),
                std::time::Duration::from_secs(seconds),
                "attempt {attempt}"
            );
        }
    }

    async fn seed_running_pod_on_node(
        db: &crate::datastore::sqlite::Datastore,
        name: &str,
        uid: &str,
        node_name: &str,
    ) {
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            name,
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": name,
                    "uid": uid,
                    "creationTimestamp": "2026-05-13T06:30:00Z"
                },
                "spec": {
                    "nodeName": node_name,
                    "containers": [{"name": "app", "image": "registry.k8s.io/pause:3.10"}]
                },
                "status": {
                    "phase": "Running",
                    "podIP": "10.42.0.10",
                    "hostIP": "192.0.2.10",
                    "conditions": [
                        {
                            "type": "PodScheduled",
                            "status": "True",
                            "lastTransitionTime": "2026-05-13T06:30:00Z"
                        },
                        {
                            "type": "Initialized",
                            "status": "True",
                            "lastTransitionTime": "2026-05-13T06:30:00Z"
                        },
                        {
                            "type": "ContainersReady",
                            "status": "True",
                            "lastTransitionTime": "2026-05-13T06:30:10Z"
                        },
                        {
                            "type": "Ready",
                            "status": "True",
                            "lastTransitionTime": "2026-05-13T06:30:10Z"
                        }
                    ],
                    "containerStatuses": [{
                        "name": "app",
                        "ready": true,
                        "started": true,
                        "restartCount": 0,
                        "state": {"running": {"startedAt": "2026-05-13T06:30:10Z"}}
                    }]
                }
            }),
        )
        .await
        .unwrap();
    }

    fn pod_condition<'a>(
        pod: &'a serde_json::Value,
        condition_type: &str,
    ) -> &'a serde_json::Value {
        pod["status"]["conditions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|condition| condition["type"] == condition_type)
            .unwrap()
    }

    #[test]
    fn lease_without_duration_uses_kubernetes_node_monitor_grace_period() {
        let deadline = super::lease_deadline(&json!({
            "spec": {
                "renewTime": "2026-05-13T06:34:15.000000Z"
            }
        }))
        .expect("deadline");

        let renew = Utc.with_ymd_and_hms(2026, 5, 13, 6, 34, 15).unwrap();
        assert_eq!(
            deadline,
            renew
                + chrono::Duration::seconds(
                    crate::node_lease_tracker::DEFAULT_NODE_LEASE_DURATION_SECONDS
                ),
            "missing leaseDurationSeconds should fall back to the canonical node-lease duration"
        );
    }

    #[tokio::test]
    async fn memory_lease_tracker_writes_node_status_only_after_deadline_expires() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "worker-a",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "worker-a"},
                "status": {
                    "conditions": [{
                        "type": "Ready",
                        "status": "True",
                        "reason": "KubeletReady",
                        "message": "klights is ready",
                        "lastHeartbeatTime": "2026-05-13T06:35:09Z",
                        "lastTransitionTime": "2026-05-13T06:35:09Z"
                    }]
                }
            }),
        )
        .await
        .unwrap();
        let tracker = crate::node_lease_tracker::NodeLeaseTracker::new_for_test(
            Utc.with_ymd_and_hms(2026, 5, 13, 6, 35, 0).unwrap(),
        );
        tracker
            .record_from_lease_object(
                "worker-a",
                &json!({
                    "metadata": {"name": "worker-a", "namespace": "kube-node-lease"},
                    "spec": {
                        "leaseDurationSeconds": 20,
                        "renewTime": "2026-05-13T06:35:10.000000Z"
                    }
                }),
            )
            .await
            .unwrap();
        let rv_before_fresh = db.get_current_resource_version().await.unwrap();

        let next = super::reconcile_node_lifecycle_once_with_tracker_for_test(
            &db,
            &tracker,
            Utc.with_ymd_and_hms(2026, 5, 13, 6, 35, 20).unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(
            db.get_current_resource_version().await.unwrap(),
            rv_before_fresh,
            "fresh in-memory heartbeats must not write cluster.db while Node status is unchanged"
        );
        assert_eq!(next, Some(std::time::Duration::from_secs(10)));

        super::reconcile_node_lifecycle_once_with_tracker_for_test(
            &db,
            &tracker,
            Utc.with_ymd_and_hms(2026, 5, 13, 6, 35, 34).unwrap(),
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "worker-a")
            .await
            .unwrap()
            .unwrap();
        let ready = node.data["status"]["conditions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|condition| condition["type"] == "Ready")
            .unwrap();
        assert_eq!(ready["status"], "Unknown");
        assert_eq!(ready["reason"], "NodeStatusUnknown");
        assert!(
            node.resource_version > rv_before_fresh,
            "cluster.db should change only for the offline transition"
        );
    }

    #[tokio::test]
    async fn newly_promoted_leader_grace_reset_prevents_mass_eviction() {
        // A long-running node that just became leader starts with an empty
        // in-memory tracker and an old startup_time. Without the promotion
        // grace-reset (T8) every unobserved node would look stale and be
        // evicted. With it, they get a fresh window and stay Ready.
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "worker-a",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "worker-a"},
                "status": {
                    "conditions": [{
                        "type": "Ready",
                        "status": "True",
                        "reason": "KubeletReady",
                        "message": "klights is ready",
                        "lastHeartbeatTime": "2026-05-13T06:00:00Z",
                        "lastTransitionTime": "2026-05-13T06:00:00Z"
                    }]
                }
            }),
        )
        .await
        .unwrap();
        seed_running_pod_on_node(&db, "worker-pod", "worker-pod-uid", "worker-a").await;

        let old_start = Utc.with_ymd_and_hms(2026, 5, 13, 6, 0, 0).unwrap();
        let tracker = crate::node_lease_tracker::NodeLeaseTracker::new_for_test(old_start);
        let now = Utc.with_ymd_and_hms(2026, 5, 13, 7, 0, 0).unwrap();

        // Precondition: without a reset the unobserved node already looks stale.
        assert!(
            tracker.deadline_for_node("worker-a").await.deadline <= now,
            "precondition: an old startup_time makes the unobserved node look stale"
        );

        // Promotion grace-reset, then reconcile.
        tracker.reset_grace_window(now).await;
        super::reconcile_node_lifecycle_once_with_tracker_for_test(&db, &tracker, now)
            .await
            .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "worker-a")
            .await
            .unwrap()
            .unwrap();
        let ready = node.data["status"]["conditions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|condition| condition["type"] == "Ready")
            .unwrap();
        assert_eq!(
            ready["status"], "True",
            "grace reset must keep unobserved nodes Ready right after promotion"
        );
        assert!(
            db.get_resource("v1", "Pod", Some("default"), "worker-pod")
                .await
                .unwrap()
                .is_some(),
            "grace reset must prevent eviction of pods on unobserved nodes after promotion"
        );
    }

    #[tokio::test]
    async fn leader_startup_marks_even_fresh_nodes_unknown() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "worker-a",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "worker-a"},
                "status": {
                    "conditions": [{
                        "type": "Ready",
                        "status": "True",
                        "reason": "KubeletReady",
                        "message": "klights is ready",
                        "lastHeartbeatTime": "2026-05-13T06:34:14Z",
                        "lastTransitionTime": "2026-05-13T06:34:14Z"
                    }]
                }
            }),
        )
        .await
        .unwrap();

        super::mark_all_nodes_unknown_at(
            &db,
            Utc.with_ymd_and_hms(2026, 5, 13, 6, 35, 00).unwrap(),
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "worker-a")
            .await
            .unwrap()
            .unwrap();
        let ready = node.data["status"]["conditions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|condition| condition["type"] == "Ready")
            .unwrap();
        assert_eq!(ready["status"], "Unknown");
        assert_eq!(ready["lastTransitionTime"], "2026-05-13T06:35:00Z");
    }

    #[tokio::test]
    async fn fresh_lease_promotes_startup_unknown_node_ready() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "worker-a",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "worker-a"},
                "status": {
                    "conditions": [{
                        "type": "Ready",
                        "status": "Unknown",
                        "reason": "NodeStatusUnknown",
                        "message": "Kubelet stopped posting node status.",
                        "lastHeartbeatTime": "2026-05-13T06:34:14Z",
                        "lastTransitionTime": "2026-05-13T06:35:00Z"
                    }]
                }
            }),
        )
        .await
        .unwrap();
        db.create_resource(
            "coordination.k8s.io/v1",
            "Lease",
            Some("kube-node-lease"),
            "worker-a",
            json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": "worker-a", "namespace": "kube-node-lease"},
                "spec": {
                    "holderIdentity": "worker-a",
                    "leaseDurationSeconds": 120,
                    "renewTime": "2026-05-13T06:35:10.000000Z"
                }
            }),
        )
        .await
        .unwrap();

        let next = super::reconcile_node_lifecycle_once(
            &db,
            Utc.with_ymd_and_hms(2026, 5, 13, 6, 35, 11).unwrap(),
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "worker-a")
            .await
            .unwrap()
            .unwrap();
        let ready = node.data["status"]["conditions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|condition| condition["type"] == "Ready")
            .unwrap();
        assert_eq!(ready["status"], "True");
        assert_eq!(ready["reason"], "KubeletReady");
        assert!(
            ready.get("lastHeartbeatTime").is_none(),
            "Ready transition must not persist lastHeartbeatTime"
        );
        assert_eq!(ready["lastTransitionTime"], "2026-05-13T06:35:11Z");
        assert_eq!(
            next,
            Some(std::time::Duration::from_secs(
                (crate::node_lease_tracker::DEFAULT_NODE_LEASE_GRACE_SECONDS - 1) as u64
            ))
        );
    }

    #[tokio::test]
    async fn node_status_transitions_do_not_store_last_heartbeat_time() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "worker-a",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "worker-a"},
                "status": {
                    "conditions": [{
                        "type": "Ready",
                        "status": "True",
                        "reason": "KubeletReady",
                        "message": "klights is ready",
                        "lastTransitionTime": "2026-05-13T06:34:14Z"
                    }]
                }
            }),
        )
        .await
        .unwrap();
        let tracker = crate::node_lease_tracker::NodeLeaseTracker::new_for_test(
            Utc.with_ymd_and_hms(2026, 5, 13, 6, 34, 0).unwrap(),
        );
        tracker
            .record_from_lease_object(
                "worker-a",
                &json!({
                    "metadata": {"name": "worker-a", "namespace": "kube-node-lease"},
                    "spec": {
                        "holderIdentity": "worker-a",
                        "leaseDurationSeconds": 10,
                        "renewTime": "2026-05-13T06:34:15.000000Z"
                    }
                }),
            )
            .await
            .unwrap();

        super::reconcile_node_lifecycle_once_with_tracker_for_test(
            &db,
            &tracker,
            Utc.with_ymd_and_hms(2026, 5, 13, 6, 34, 26).unwrap(),
        )
        .await
        .unwrap();
        let unknown_node = db
            .get_resource("v1", "Node", None, "worker-a")
            .await
            .unwrap()
            .unwrap();
        let unknown_ready = unknown_node.data["status"]["conditions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|condition| condition["type"] == "Ready")
            .unwrap();
        assert_eq!(unknown_ready["status"], "Unknown");
        assert!(
            unknown_ready.get("lastHeartbeatTime").is_none(),
            "Unknown transition must not persist lastHeartbeatTime"
        );

        tracker
            .record_from_lease_object(
                "worker-a",
                &json!({
                    "metadata": {"name": "worker-a", "namespace": "kube-node-lease"},
                    "spec": {
                        "holderIdentity": "worker-a",
                        "leaseDurationSeconds": 10,
                        "renewTime": "2026-05-13T06:34:30.000000Z"
                    }
                }),
            )
            .await
            .unwrap();
        super::reconcile_node_lifecycle_once_with_tracker_for_test(
            &db,
            &tracker,
            Utc.with_ymd_and_hms(2026, 5, 13, 6, 34, 31).unwrap(),
        )
        .await
        .unwrap();
        let ready_node = db
            .get_resource("v1", "Node", None, "worker-a")
            .await
            .unwrap()
            .unwrap();
        let ready = ready_node.data["status"]["conditions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|condition| condition["type"] == "Ready")
            .unwrap();
        assert_eq!(ready["status"], "True");
        assert!(
            ready.get("lastHeartbeatTime").is_none(),
            "Ready transition must not persist lastHeartbeatTime"
        );
    }

    #[tokio::test]
    async fn fresh_lease_reconciles_unknown_bound_pods_from_cluster_status_after_worker_replay() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "worker-a",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "worker-a"},
                "status": {
                    "conditions": [{
                        "type": "Ready",
                        "status": "Unknown",
                        "reason": "NodeStatusUnknown",
                        "message": "Kubelet stopped posting node status.",
                        "lastHeartbeatTime": "2026-05-13T06:34:14Z",
                        "lastTransitionTime": "2026-05-13T06:35:00Z"
                    }]
                }
            }),
        )
        .await
        .unwrap();
        db.create_resource(
            "coordination.k8s.io/v1",
            "Lease",
            Some("kube-node-lease"),
            "worker-a",
            json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": "worker-a", "namespace": "kube-node-lease"},
                "spec": {
                    "holderIdentity": "worker-a",
                    "leaseDurationSeconds": 120,
                    "renewTime": "2026-05-13T06:35:10.000000Z"
                }
            }),
        )
        .await
        .unwrap();
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "worker-pod",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "worker-pod",
                    "uid": "worker-pod-uid",
                    "creationTimestamp": "2026-05-13T06:30:00Z"
                },
                "spec": {
                    "nodeName": "worker-a",
                    "containers": [{"name": "app", "image": "registry.k8s.io/pause:3.10"}]
                },
                "status": {
                    "phase": "Unknown",
                    "podIP": "10.42.0.10",
                    "hostIP": "192.0.2.10",
                    "conditions": [
                        {
                            "type": "PodScheduled",
                            "status": "True",
                            "lastTransitionTime": "2026-05-13T06:30:00Z"
                        },
                        {
                            "type": "Initialized",
                            "status": "True",
                            "lastTransitionTime": "2026-05-13T06:30:00Z"
                        },
                        {
                            "type": "ContainersReady",
                            "status": "Unknown",
                            "reason": "NodeStatusUnknown",
                            "message": "Kubelet stopped posting node status.",
                            "lastTransitionTime": "2026-05-13T06:35:00Z"
                        },
                        {
                            "type": "Ready",
                            "status": "Unknown",
                            "reason": "NodeStatusUnknown",
                            "message": "Kubelet stopped posting node status.",
                            "lastTransitionTime": "2026-05-13T06:35:00Z"
                        }
                    ],
                    "containerStatuses": [{
                        "name": "app",
                        "containerID": "containerd://worker-pod-app",
                        "ready": false,
                        "started": false,
                        "restartCount": 0,
                        "state": {"running": {"startedAt": "2026-05-13T06:30:10Z"}}
                    }]
                }
            }),
        )
        .await
        .unwrap();

        super::reconcile_node_lifecycle_once(
            &db,
            Utc.with_ymd_and_hms(2026, 5, 13, 6, 35, 11).unwrap(),
        )
        .await
        .unwrap();

        let pod = db
            .get_resource("v1", "Pod", Some("default"), "worker-pod")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pod.data["status"]["phase"], "Running");
        assert_eq!(pod.data["status"]["podIP"], "10.42.0.10");
        assert_eq!(pod.data["status"]["hostIP"], "192.0.2.10");
        assert_eq!(pod.data["status"]["containerStatuses"][0]["ready"], true);
        assert_eq!(pod.data["status"]["containerStatuses"][0]["started"], true);
        assert_eq!(
            pod.data["status"]["containerStatuses"][0]["containerID"],
            "containerd://worker-pod-app"
        );
        assert_eq!(
            pod.data["status"]["containerStatuses"][0]["state"]["running"]["startedAt"],
            "2026-05-13T06:30:10Z"
        );

        let ready = pod_condition(&pod.data, "Ready");
        assert_eq!(ready["status"], "True");
        assert!(ready.get("reason").is_none());
        assert!(ready.get("message").is_none());
        let containers_ready = pod_condition(&pod.data, "ContainersReady");
        assert_eq!(containers_ready["status"], "True");
        assert!(containers_ready.get("reason").is_none());
        assert!(containers_ready.get("message").is_none());
    }

    #[tokio::test]
    async fn fresh_lease_reconciles_unknown_pods_when_node_status_refresh_already_marked_ready() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "worker-a",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "worker-a"},
                "status": {
                    "conditions": [{
                        "type": "Ready",
                        "status": "True",
                        "reason": "KubeletReady",
                        "message": "klights is ready",
                        "lastHeartbeatTime": "2026-05-13T06:35:08Z",
                        "lastTransitionTime": "2026-05-13T06:35:08Z"
                    }]
                }
            }),
        )
        .await
        .unwrap();
        db.create_resource(
            "coordination.k8s.io/v1",
            "Lease",
            Some("kube-node-lease"),
            "worker-a",
            json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": "worker-a", "namespace": "kube-node-lease"},
                "spec": {
                    "holderIdentity": "worker-a",
                    "leaseDurationSeconds": 120,
                    "renewTime": "2026-05-13T06:35:10.000000Z"
                }
            }),
        )
        .await
        .unwrap();
        seed_running_pod_on_node(&db, "worker-pod", "worker-pod-uid", "worker-a").await;
        let pod = db
            .get_resource("v1", "Pod", Some("default"), "worker-pod")
            .await
            .unwrap()
            .unwrap();
        let mut pod_data = pod.data.as_ref().clone();
        assert!(super::mark_pod_status_unknown(
            &mut pod_data,
            Utc.with_ymd_and_hms(2026, 5, 13, 6, 35, 0).unwrap()
        ));
        db.update_status_only_with_preconditions(
            "v1",
            "Pod",
            Some("default"),
            "worker-pod",
            pod_data["status"].clone(),
            crate::datastore::ResourcePreconditions::from_resource(&pod),
        )
        .await
        .unwrap();

        super::reconcile_node_lifecycle_once(
            &db,
            Utc.with_ymd_and_hms(2026, 5, 13, 6, 35, 11).unwrap(),
        )
        .await
        .unwrap();

        let pod = db
            .get_resource("v1", "Pod", Some("default"), "worker-pod")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            pod.data["status"]["phase"], "Running",
            "fresh Lease should trigger NodeResourceReconcile even if a Node status refresh already flipped Ready"
        );
        assert_eq!(pod_condition(&pod.data, "Ready")["status"], "True");
        assert_eq!(
            pod_condition(&pod.data, "ContainersReady")["status"],
            "True"
        );
    }

    #[tokio::test]
    async fn leader_startup_ignores_preexisting_fresh_lease_until_renewed() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "worker-a",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "worker-a"},
                "status": {
                    "conditions": [{
                        "type": "Ready",
                        "status": "Unknown",
                        "reason": "NodeStatusUnknown",
                        "message": "Kubelet stopped posting node status.",
                        "lastHeartbeatTime": "2026-05-13T06:34:14Z",
                        "lastTransitionTime": "2026-05-13T06:35:00Z"
                    }]
                }
            }),
        )
        .await
        .unwrap();
        db.create_resource(
            "coordination.k8s.io/v1",
            "Lease",
            Some("kube-node-lease"),
            "worker-a",
            json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": "worker-a", "namespace": "kube-node-lease"},
                "spec": {
                    "holderIdentity": "worker-a",
                    "leaseDurationSeconds": 120,
                    "renewTime": "2026-05-13T06:35:10.000000Z"
                }
            }),
        )
        .await
        .unwrap();
        let startup_rv = db.get_current_resource_version().await.unwrap();

        let next = super::reconcile_node_lifecycle_once_after_startup(
            &db,
            Utc.with_ymd_and_hms(2026, 5, 13, 6, 35, 11).unwrap(),
            startup_rv,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "worker-a")
            .await
            .unwrap()
            .unwrap();
        let ready = node.data["status"]["conditions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|condition| condition["type"] == "Ready")
            .unwrap();
        assert_eq!(
            ready["status"], "Unknown",
            "a persisted pre-start Lease is not proof that the worker synced with this leader"
        );
        assert_eq!(
            next,
            Some(std::time::Duration::from_secs(
                crate::node_lease_tracker::DEFAULT_NODE_LEASE_DURATION_SECONDS as u64
            )),
            "already-unknown pre-start leases should wait through startup grace for the worker's next heartbeat"
        );
    }
}
