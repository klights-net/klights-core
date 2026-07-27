use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use futures::StreamExt as _;
use klights_leader_api::{
    LeaderWatch, LeaderWatchError, WatchEventType, WatchRequest, WatchStream,
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::controllers::node_lifecycle::{
    NodeLifecyclePodStore, NodeLifecycleStore, NodeLostPodLifecycleSink,
};
use crate::datastore::{DatastoreBackend, DatastoreHandle, ResourceListQuery};
use crate::kubelet::pod_lifecycle_core::message::PodLifecycleKey;
use crate::kubelet::pod_lifecycle_router::{
    OrphanReason, PodLifecycleRouter, enqueue_orphan_finalize,
};
use crate::kubelet::pod_repository::{PodReader, PodSubresourceWriter};
use klights_cluster_core::Resource;

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

pub(crate) struct NodeLifecycleControllerDependencies {
    pub(crate) store: DatastoreHandle,
    pub(crate) pods: Arc<dyn NodeLifecyclePodStore>,
    pub(crate) pod_mutations: Arc<dyn klights_reconcile_api::PodMutationReconcileSink>,
    pub(crate) pod_lifecycle: Arc<PodLifecycleRouter>,
    pub(crate) lease_observations: Arc<crate::node_lease_tracker::NodeLeaseTracker>,
    pub(crate) supervisor: Arc<klights_supervisor::TaskSupervisor>,
    pub(crate) node_status: Arc<dyn klights_leader_api::LeaderNodeLifecycleStatus>,
    pub(crate) watch: Arc<dyn LeaderWatch>,
    pub(crate) pod_eviction_grace: std::time::Duration,
}

pub(crate) async fn run_node_lifecycle_controller(
    dependencies: NodeLifecycleControllerDependencies,
    cancel: CancellationToken,
    mut is_leader_rx: watch::Receiver<bool>,
) {
    let NodeLifecycleControllerDependencies {
        store: db,
        pods: pod_repository,
        pod_mutations,
        pod_lifecycle: pod_lifecycle_router,
        lease_observations: node_lease_tracker,
        supervisor: task_supervisor,
        node_status,
        watch,
        pod_eviction_grace,
    } = dependencies;
    let mut events = match open_node_lifecycle_watches(watch.as_ref()).await {
        Ok(events) => events,
        Err(error) => {
            tracing::warn!("node_lifecycle: failed to open positioned watches: {error:#}");
            return;
        }
    };
    if let Err(err) =
        crate::controllers::node_lifecycle::refresh_node_lease_tracker_from_cluster_leases(
            db.as_ref(),
            node_lease_tracker.as_ref(),
        )
        .await
    {
        tracing::warn!(
            "node_lifecycle: failed to seed node lease tracker from persisted leases: {err:#}"
        );
    }

    if *is_leader_rx.borrow() {
        node_lease_tracker.reset_grace_window(Utc::now()).await;
    } else if !wait_for_leadership(node_lease_tracker.as_ref(), &cancel, &mut is_leader_rx).await {
        return;
    }

    let mut retry_attempt = 0u32;
    'controller: loop {
        if !*is_leader_rx.borrow() {
            tracing::debug!("node_lifecycle: not leader, waiting before reconcile");
            retry_attempt = 0;
            if !wait_for_leadership(node_lease_tracker.as_ref(), &cancel, &mut is_leader_rx).await {
                return;
            }
        }

        let next_deadline =
            match crate::controllers::node_lifecycle::reconcile_node_lifecycle_once_with_tracker(
                db.as_ref(),
                node_status.as_ref(),
                pod_repository.as_ref(),
                node_lease_tracker.as_ref(),
                Utc::now(),
                Some(pod_mutations.as_ref()),
                Some(pod_lifecycle_router.as_ref() as &dyn NodeLostPodLifecycleSink),
                pod_eviction_grace,
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
                    if wait_for_retry(task_supervisor.as_ref(), &cancel, attempt).await {
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
                        && !wait_for_leadership(
                            node_lease_tracker.as_ref(),
                            &cancel,
                            &mut is_leader_rx,
                        ).await
                    {
                        return;
                    }
                    None
                }
                sleep = task_supervisor.sleep("node_lifecycle_lease_deadline", delay) => {
                    if let Err(err) = sleep {
                        tracing::warn!("node_lifecycle deadline timer failed: {err:#}");
                    }
                    None
                }
                _ = node_lease_tracker.wait_changed() => None,
                event = events.next() => event,
            }
        } else {
            tokio::select! {
                _ = cancel.cancelled() => None,
                _ = is_leader_rx.changed() => {
                    if !*is_leader_rx.borrow()
                        && !wait_for_leadership(
                            node_lease_tracker.as_ref(),
                            &cancel,
                            &mut is_leader_rx,
                        ).await
                    {
                        return;
                    }
                    None
                }
                _ = node_lease_tracker.wait_changed() => None,
                event = events.next() => event,
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
                    node_lease_tracker.as_ref(),
                )
                .await
                {
                    tracing::warn!(
                        "node_lifecycle: failed to refresh lease from watch event: {err:#}"
                    );
                }
                if event.event_type() == WatchEventType::Deleted && event.resource().kind == "Node"
                {
                    loop {
                        match crate::controllers::node_lifecycle::cleanup_pods_bound_to_deleted_node_event(
                            db.as_ref(),
                            pod_repository.as_ref(),
                            Some(pod_mutations.as_ref()),
                            Some(
                                pod_lifecycle_router.as_ref()
                                    as &dyn NodeLostPodLifecycleSink,
                            ),
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
                                if !wait_for_retry(
                                    task_supervisor.as_ref(),
                                    &cancel,
                                    attempt,
                                )
                                .await
                                {
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
            Err(LeaderWatchError::ReplayExpired { .. }) => {
                tracing::warn!(
                    "node_lifecycle watch replay expired; refreshing lease state and reopening"
                );
                if let Err(error) =
                    crate::controllers::node_lifecycle::refresh_node_lease_tracker_from_cluster_leases(
                        db.as_ref(),
                        node_lease_tracker.as_ref(),
                    )
                    .await
                {
                    tracing::warn!("node_lifecycle lease refresh failed: {error:#}");
                }
                match open_node_lifecycle_watches(watch.as_ref()).await {
                    Ok(reopened) => {
                        events = reopened;
                        continue;
                    }
                    Err(error) => {
                        tracing::warn!("node_lifecycle positioned watch reopen failed: {error:#}");
                    }
                }
            }
            Err(err) => {
                tracing::warn!("node_lifecycle watch error: {err:#?}");
                let attempt = retry_attempt;
                retry_attempt = retry_attempt.saturating_add(1);
                if !wait_for_retry(task_supervisor.as_ref(), &cancel, attempt).await {
                    break;
                }
            }
        }
    }
}

async fn open_node_lifecycle_watches(
    watch: &dyn LeaderWatch,
) -> std::result::Result<futures::stream::SelectAll<WatchStream>, LeaderWatchError> {
    let mut sessions = Vec::with_capacity(2);
    for (api_version, kind, namespace) in [
        ("v1", "Node", None),
        (
            "coordination.k8s.io/v1",
            "Lease",
            Some("kube-node-lease".to_string()),
        ),
    ] {
        let request = WatchRequest::try_new(api_version, kind, namespace, None, None, None, None)?;
        sessions.push(watch.watch_resources(request).await?);
    }
    Ok(futures::stream::select_all(sessions))
}

async fn wait_for_leadership(
    node_lease_tracker: &crate::node_lease_tracker::NodeLeaseTracker,
    cancel: &CancellationToken,
    is_leader_rx: &mut watch::Receiver<bool>,
) -> bool {
    loop {
        if *is_leader_rx.borrow() {
            node_lease_tracker.reset_grace_window(Utc::now()).await;
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

async fn wait_for_retry(
    task_supervisor: &klights_supervisor::TaskSupervisor,
    cancel: &CancellationToken,
    attempt: u32,
) -> bool {
    let delay = crate::controllers::node_lifecycle::node_lifecycle_retry_delay(attempt);
    tokio::select! {
        _ = cancel.cancelled() => false,
        _ = task_supervisor.sleep("node_lifecycle_retry", delay) => true,
    }
}
