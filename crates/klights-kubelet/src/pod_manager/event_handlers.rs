use super::*;
use crate::pod_repository::workqueue::PodWorkqueue;
use crate::pod_watch_source::PodWatchEvent as WatchEvent;
use klights_leader_api::WatchEventType as EventType;

/// Enqueue the owning Job for asynchronous reconciliation after a terminal
/// watch event, through the focused mutation-reconcile sink port rather than
/// the concrete root repository — callers hold only the capability this
/// function actually needs.
pub(super) async fn enqueue_job_reconcile_for_terminal_watch_pod(
    mutation_reconcile: &dyn klights_reconcile_api::PodMutationReconcileSink,
    pod: &Value,
) {
    let phase = pod
        .pointer("/status/phase")
        .and_then(|value| value.as_str());
    if matches!(phase, Some("Succeeded") | Some("Failed"))
        && let Err(err) = mutation_reconcile
            .reconcile_pod_mutation(
                klights_reconcile_api::PodMutationReconcileRequest::EnqueueJobOwner {
                    pod: klights_cluster_core::Resource::from_data_lossy(Arc::new(pod.clone())),
                },
            )
            .await
    {
        tracing::warn!(error = %err, "failed to enqueue Job reconcile for terminal Pod");
    }
}

pub(super) struct WatchEventHandlerContext<'a> {
    pub persistent_volume_event_handler: &'a Arc<dyn PersistentVolumeEventHandler>,
    pub pod_cleanup_intents: &'a Arc<dyn klights_leader_api::LeaderPodCleanupIntents>,
    pub node_name: &'a str,
    // `pod_workqueue` backs the durable namespace-termination enqueue
    // capability (`enqueue_actor_deletes_for_terminating_namespace`);
    // volume-refresh reads use the focused `pod_query` field below, and
    // phase/restart persistence goes through the focused status writer.
    pub pod_workqueue: &'a Arc<PodWorkqueue>,
    pub pod_query: &'a dyn klights_pod_api::PodQuery,
    pub pod_status_writer: &'a dyn crate::pod_repository::status::PodStatusWriter,
    pub mutation_reconcile: &'a dyn klights_reconcile_api::PodMutationReconcileSink,
    pub pod_creation_tracker: &'a PodCreationTracker,
    pub retry_state: &'a PodStartRetryTracker,
    pub pod_lifecycle_state: &'a PodLifecycleStateTracker,
    pub pod_lifecycle_router: std::sync::Arc<crate::pod_lifecycle_router::PodLifecycleRouter>,
    pub task_supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
    pub file_process: klights_supervisor::FileProcessExecutor,
    pub deadline_timers: super::deadline_timers::DeadlineTimerRegistry,
    pub now_unix_seconds: i64,
    pub node_capacity: crate::node_capacity::NodeCapacity,
    pub paths: crate::runtime_paths::KubeletRuntimePaths,
}

pub(super) async fn handle_watch_event(context: WatchEventHandlerContext<'_>, event: WatchEvent) {
    let WatchEventHandlerContext {
        persistent_volume_event_handler,
        pod_cleanup_intents,
        node_name,
        pod_workqueue,
        pod_query,
        pod_status_writer: _pod_status_writer,
        mutation_reconcile,
        pod_creation_tracker,
        retry_state,
        pod_lifecycle_state,
        pod_lifecycle_router,
        task_supervisor,
        file_process,
        deadline_timers,
        now_unix_seconds,
        node_capacity,
        paths,
    } = context;
    // Check event kind and dispatch to appropriate handler
    let event_kind = event
        .object
        .get("kind")
        .and_then(|k| k.as_str())
        .unwrap_or("UNKNOWN");
    let event_name = event
        .object
        .pointer("/metadata/name")
        .and_then(|n| n.as_str())
        .unwrap_or("UNKNOWN");

    // Dispatch to appropriate handler
    if event_kind == "PersistentVolumeClaim" {
        persistent_volume_event_handler
            .handle_pvc_event(&event, event_name)
            .await;
        return;
    }

    if event_kind == "PersistentVolume" {
        persistent_volume_event_handler
            .handle_pv_event(&event, event_name)
            .await;
        return;
    }

    // Handle Secret/ConfigMap watch events — refresh mounted volumes so create,
    // update, and delete changes propagate to optional mounts.
    if (event_kind == "Secret" || event_kind == "ConfigMap")
        && (event.event_type == EventType::Added
            || event.event_type == EventType::Modified
            || event.event_type == EventType::Deleted)
    {
        let event_ns = event
            .object
            .pointer("/metadata/namespace")
            .and_then(|n| n.as_str())
            .unwrap_or("default");
        let volumes_root = paths.volumes_root().to_string_lossy().into_owned();
        let refresh_result = if event.event_type == EventType::Deleted {
            crate::volumes::refresh_secret_configmap_volumes_after_delete(
                &file_process,
                event_kind,
                event_ns,
                event_name,
                &volumes_root,
                pod_query,
            )
            .await
        } else {
            crate::volumes::refresh_secret_configmap_volumes_from_event(
                &file_process,
                event_kind,
                event_ns,
                event_name,
                &event.object,
                &volumes_root,
                pod_query,
            )
            .await
        };
        if let Err(e) = refresh_result {
            tracing::warn!(
                "Failed to refresh {} volume {}/{}: {}",
                event_kind,
                event_ns,
                event_name,
                e
            );
        }
        return;
    }

    if event_kind == "Namespace" {
        handle_namespace_termination_event(pod_workqueue, &event).await;
        return;
    }

    // Handle Pod events
    if event_kind != "Pod" {
        return;
    }

    tracing::info!(
        "Pod watcher received {} event for pod {}",
        event.event_type.as_str(),
        event_name
    );

    // Handle ADDED events (new pods) — start is now owned by the actor/executor.
    // The watcher already routes WatchAdded through the router, which dispatches
    // StartPod to the executor. This handler only does non-start pod work.
    if event.event_type == EventType::Added {
        tracing::debug!("Watch ADDED for pod {} routed through actor", event_name);
    }

    // Handle MODIFIED events (pod status changes).
    // Start reconciliation is now owned by the actor/executor.
    if event.event_type == EventType::Modified {
        schedule_active_deadline_timer_for_modified_pod(
            &event.object,
            now_unix_seconds,
            deadline_timers,
            task_supervisor.clone(),
            pod_lifecycle_router.clone(),
        )
        .await;

        let is_terminating = event
            .object
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str())
            .is_some();
        if is_terminating {
            // Stop/delete is now handled by actor/executor via WatchModified → StopPod.
            // This handler cleans up creation state only.
            if let (Some(namespace), Some(name)) = (
                event
                    .object
                    .get("metadata")
                    .and_then(|m| m.get("namespace"))
                    .and_then(|n| n.as_str()),
                event
                    .object
                    .get("metadata")
                    .and_then(|m| m.get("name"))
                    .and_then(|n| n.as_str()),
            ) && should_clear_pod_creation_inflight(&event.object)
            {
                clear_pod_creation_inflight(pod_creation_tracker, namespace, name).await;
                clear_pod_start_retry_state(retry_state, namespace, name).await;
            }
            return;
        }

        if let (Some(namespace), Some(name)) = (
            event
                .object
                .get("metadata")
                .and_then(|m| m.get("namespace"))
                .and_then(|n| n.as_str()),
            event
                .object
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str()),
        ) && should_clear_pod_creation_inflight(&event.object)
        {
            clear_pod_creation_inflight(pod_creation_tracker, namespace, name).await;
            clear_pod_start_retry_state(retry_state, namespace, name).await;
        }

        // R2g: Ephemeral container reconcile is now owned by the executor
        // via WatchModified → ReconcileEphemeral.
        //
        // Pod→Service reconcile after a pod modification is leader-owned: the
        // pod_repository side-effect path (`enqueue_services_after_pod_update`)
        // and the leader's outbox apply path
        // (`enqueue_forwarded_pod_status_effects` in
        // `replication/grpc/server.rs`) both fire before the watch event that
        // gets us here. Calling endpoint reconcile from kubelet is redundant on
        // the leader and broken on workers (no cluster.db write surface).

        // Terminal Pod watch events are the serialized datastore signal after
        // status writes. Re-enqueue the owning Job from this event so indexed
        // Job status cannot miss a final succeeded/failed index due to races
        // between CRI completion handling and controller queue coalescing.
        enqueue_job_reconcile_for_terminal_watch_pod(mutation_reconcile, &event.object).await;

        // Refresh downwardAPI volumes to reflect metadata changes (labels/annotations)
        let volumes_root = paths.volumes_root().to_string_lossy().into_owned();
        if let Err(e) = crate::volumes::refresh_downward_api_volumes(
            &file_process,
            &event.object,
            &volumes_root,
            node_capacity,
        )
        .await
        {
            tracing::warn!(
                "Failed to refresh downwardAPI volumes after pod modification: {}",
                e
            );
        }
    }

    // Handle DELETED events — stop/delete is now owned by actor/executor.
    if event.event_type == EventType::Deleted
        && let (Some(namespace), Some(name)) = (
            event
                .object
                .get("metadata")
                .and_then(|m| m.get("namespace"))
                .and_then(|n| n.as_str()),
            event
                .object
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str()),
        )
    {
        clear_pod_creation_inflight(pod_creation_tracker, namespace, name).await;
        clear_pod_start_retry_state(retry_state, namespace, name).await;
        if let Some(key) = pod_lifecycle_key_from_pod(&event.object) {
            crate::pod_lifecycle_actor::state::remove_pod_state(pod_lifecycle_state, &key).await;
        }
        let orphan_enqueued = match crate::reconciler::orphan::OrphanScanner::scan_deleted_event(
            pod_lifecycle_router.as_ref(),
            &event,
        )
        .await
        {
            Ok(enqueued) => enqueued,
            Err(err) => {
                tracing::warn!(
                    namespace,
                    pod = name,
                    "failed to enqueue deleted-pod orphan cleanup: {err}"
                );
                false
            }
        };
        if orphan_enqueued
            && let Some(key) = node_lost_cleanup_intent_key_for_deleted_pod(&event, node_name)
        {
            match klights_leader_api::PodCleanupIntentAckRequest::try_new(
                key.node_name.as_str(),
                key.namespace.as_str(),
                key.pod_name.as_str(),
                key.pod_uid.as_str(),
                crate::pod_lifecycle_core::message::POD_CLEANUP_REASON_NODE_LOST,
            ) {
                Ok(request) => {
                    if let Err(err) = pod_cleanup_intents
                        .acknowledge_pod_cleanup_intent(request)
                        .await
                    {
                        tracing::warn!(
                            node = %key.node_name,
                            namespace = %key.namespace,
                            pod = %key.pod_name,
                            uid = %key.pod_uid,
                            error = %err,
                            "failed to acknowledge NodeLost pod cleanup intent after deleted-pod orphan handoff"
                        );
                    }
                }
                Err(err) => tracing::warn!(
                    node = %key.node_name,
                    namespace = %key.namespace,
                    pod = %key.pod_name,
                    uid = %key.pod_uid,
                    error = %err,
                    "refused invalid NodeLost pod cleanup-intent acknowledgement key"
                ),
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NodeLostCleanupIntentKey {
    node_name: String,
    namespace: String,
    pod_name: String,
    pod_uid: String,
}

fn node_lost_cleanup_intent_key_for_deleted_pod(
    event: &WatchEvent,
    local_node_name: &str,
) -> Option<NodeLostCleanupIntentKey> {
    if event.event_type != EventType::Deleted {
        return None;
    }
    if event
        .object
        .pointer("/kind")
        .and_then(|value| value.as_str())
        != Some("Pod")
    {
        return None;
    }
    if event
        .object
        .pointer("/status/phase")
        .and_then(|value| value.as_str())
        != Some("Unknown")
    {
        return None;
    }
    let node_name = event
        .object
        .pointer("/spec/nodeName")
        .and_then(|value| value.as_str())
        .filter(|node| *node == local_node_name)?;
    let namespace = event
        .object
        .pointer("/metadata/namespace")
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())?;
    let pod_name = event
        .object
        .pointer("/metadata/name")
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())?;
    let pod_uid = event
        .object
        .pointer("/metadata/uid")
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())?;
    Some(NodeLostCleanupIntentKey {
        node_name: node_name.to_string(),
        namespace: namespace.to_string(),
        pod_name: pod_name.to_string(),
        pod_uid: pod_uid.to_string(),
    })
}

pub(super) async fn handle_namespace_termination_event(
    pod_workqueue: &Arc<PodWorkqueue>,
    event: &WatchEvent,
) {
    if event.object.get("kind").and_then(|kind| kind.as_str()) != Some("Namespace") {
        return;
    }

    let namespace = match event
        .object
        .pointer("/metadata/name")
        .and_then(|name| name.as_str())
        .filter(|name| !name.trim().is_empty())
    {
        Some(namespace) => namespace,
        None => return,
    };

    if event
        .object
        .pointer("/metadata/deletionTimestamp")
        .and_then(|value| value.as_str())
        .is_none()
    {
        return;
    }

    if let Err(err) = pod_workqueue
        .enqueue_actor_deletes_for_terminating_namespace(namespace)
        .await
    {
        tracing::warn!(
            namespace = %namespace,
            error = %err,
            "namespace termination event failed to enqueue local Pod actor deletes"
        );
    }
}
