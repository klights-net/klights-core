use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::api::AppState;
use crate::controllers::node_lifecycle::{
    NodeLifecyclePodStore, NodeLifecycleStore, NodeLostPodLifecycleSink,
};
use crate::datastore::raft::node::RaftNode;
use crate::datastore::sqlite::DatastoreWatchReplaySource;
use crate::datastore::{DatastoreBackend, ResourceListQuery, WatchTarget};
use crate::kubelet::pod_lifecycle_core::message::PodLifecycleKey;
use crate::kubelet::pod_lifecycle_router::{
    OrphanReason, PodLifecycleRouter, enqueue_orphan_finalize,
};
use crate::kubelet::pod_repository::{PodReader, PodSubresourceWriter};
use crate::watch::{SignalWatchCursor, WatchCursorError, WatchDeliveryScope, WindowPolicy};
use klights_cluster_core::Resource;
use klights_watch::{WatchSignalReceiver, WatchTopic};

#[async_trait]
impl<T> NodeLifecycleStore for T
where
    T: DatastoreBackend + Send + Sync + ?Sized,
{
    async fn list_nodes(&self) -> Result<Vec<Resource>> {
        Ok(self
            .list_resources("v1", "Node", None, ResourceListQuery::all())
            .await?
            .items)
    }

    async fn list_node_leases(&self) -> Result<Vec<Resource>> {
        Ok(self
            .list_resources(
                "coordination.k8s.io/v1",
                "Lease",
                Some("kube-node-lease"),
                ResourceListQuery::all(),
            )
            .await?
            .items)
    }
}

#[async_trait]
impl<T> NodeLifecyclePodStore for T
where
    T: PodReader + PodSubresourceWriter + Send + Sync + ?Sized,
{
    async fn list_pods_bound_to_node(&self, node_name: &str) -> Result<Vec<Resource>> {
        let field_selector = format!("spec.nodeName={node_name}");
        Ok(
            PodReader::list_pods(self, None, None, Some(&field_selector), None, None)
                .await?
                .items,
        )
    }

    async fn replace_pod_status_for_uid(
        &self,
        pod: &Resource,
        status: serde_json::Value,
    ) -> Result<Resource> {
        PodSubresourceWriter::replace_status_from_api_for_uid(
            self,
            pod.namespace.as_deref().unwrap_or("default"),
            &pod.name,
            &pod.uid,
            status,
            pod.resource_version,
        )
        .await
    }
}

#[async_trait]
impl NodeLostPodLifecycleSink for PodLifecycleRouter {
    async fn enqueue_node_lost_cleanup(&self, pod: Resource) -> Result<()> {
        let namespace = pod.namespace.as_deref().unwrap_or("default");
        enqueue_orphan_finalize(
            self,
            PodLifecycleKey::new(namespace, &pod.name, &pod.uid),
            OrphanReason::NodeLost,
        )
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))
    }
}

pub(crate) async fn run_node_lifecycle_controller(
    state: Arc<AppState>,
    node_status: Arc<dyn klights_leader_api::LeaderNodeLifecycleStatus>,
    cancel: CancellationToken,
    _startup_resource_version: i64,
    mut is_leader_rx: watch::Receiver<bool>,
    _raft_node: Option<Arc<RaftNode>>,
) {
    if let Err(err) =
        crate::controllers::node_lifecycle::refresh_node_lease_tracker_from_cluster_leases(
            state.resource_mutation().db.as_ref(),
            state.controller_reconcile().node_lease_tracker.as_ref(),
        )
        .await
    {
        tracing::warn!(
            "node_lifecycle: failed to seed node lease tracker from persisted leases: {err:#}"
        );
    }

    if *is_leader_rx.borrow() {
        state
            .controller_reconcile()
            .node_lease_tracker
            .reset_grace_window(Utc::now())
            .await;
    } else if !wait_for_leadership(&state, &cancel, &mut is_leader_rx).await {
        return;
    }

    let db = state.resource_mutation().db.clone();
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
            Arc::new(crate::datastore::DatastoreBackendWatchStore::new(
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

        let next_deadline =
            match crate::controllers::node_lifecycle::reconcile_node_lifecycle_once_with_tracker(
                db.as_ref(),
                node_status.as_ref(),
                state.resource_mutation().pod_repository.as_ref(),
                state.controller_reconcile().node_lease_tracker.as_ref(),
                Utc::now(),
                Some(
                    state
                        .resource_mutation()
                        .pod_repository
                        .mutation_reconcile_port()
                        .as_ref(),
                ),
                state
                    .pod_node_subresources()
                    .pod_lifecycle_router
                    .as_deref()
                    .map(|router| router as &dyn NodeLostPodLifecycleSink),
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
                    if !*is_leader_rx.borrow()
                        && !wait_for_leadership(&state, &cancel, &mut is_leader_rx).await
                    {
                        return;
                    }
                    None
                }
                sleep = state.operational().task_supervisor.sleep("node_lifecycle_lease_deadline", delay) => {
                    if let Err(err) = sleep {
                        tracing::warn!("node_lifecycle deadline timer failed: {err:#}");
                    }
                    None
                }
                _ = state.controller_reconcile().node_lease_tracker.wait_changed() => None,
                event = cursor.next_event() => Some(event),
            }
        } else {
            tokio::select! {
                _ = cancel.cancelled() => None,
                _ = is_leader_rx.changed() => {
                    if !*is_leader_rx.borrow()
                        && !wait_for_leadership(&state, &cancel, &mut is_leader_rx).await
                    {
                        return;
                    }
                    None
                }
                _ = state.controller_reconcile().node_lease_tracker.wait_changed() => None,
                event = cursor.next_event() => Some(event),
            }
        };
        let Some(watch_result) = maybe_event else {
            if cancel.is_cancelled() {
                break;
            }
            continue;
        };

        match watch_result {
            Ok(event) => {
                if let Err(err) = crate::controllers::node_lifecycle::track_lease_from_event(
                    &event,
                    state.controller_reconcile().node_lease_tracker.as_ref(),
                )
                .await
                {
                    tracing::warn!(
                        "node_lifecycle: failed to refresh lease from watch event: {err:#}"
                    );
                }
                if event.event_type == crate::watch::EventType::Deleted
                    && event.object.get("kind").and_then(|value| value.as_str()) == Some("Node")
                {
                    loop {
                        match crate::controllers::node_lifecycle::cleanup_pods_bound_to_deleted_node_event(
                            db.as_ref(),
                            state.resource_mutation().pod_repository.as_ref(),
                            Some(
                                state
                                    .resource_mutation()
                                    .pod_repository
                                    .mutation_reconcile_port()
                                    .as_ref(),
                            ),
                            state
                                .pod_node_subresources()
                                .pod_lifecycle_router
                                .as_deref()
                                .map(|router| router as &dyn NodeLostPodLifecycleSink),
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
                if crate::controllers::node_lifecycle::node_lifecycle_event(&event) {
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
            state
                .controller_reconcile()
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
            }
        }
    }
}

async fn wait_for_retry(state: &AppState, cancel: &CancellationToken, attempt: u32) -> bool {
    let delay = crate::controllers::node_lifecycle::node_lifecycle_retry_delay(attempt);
    tokio::select! {
        _ = cancel.cancelled() => false,
        _ = state.operational().task_supervisor.sleep("node_lifecycle_retry", delay) => true,
    }
}
