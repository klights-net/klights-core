use super::*;
#[cfg(test)]
use crate::kubelet::pod_status_logic::ContainerInfo;

pub(super) async fn enqueue_job_reconcile_for_terminal_watch_pod(
    pod_repo: &Arc<crate::kubelet::pod_repository::PodRepository>,
    pod: &Value,
) {
    let phase = pod
        .pointer("/status/phase")
        .and_then(|value| value.as_str());
    if matches!(phase, Some("Succeeded") | Some("Failed")) {
        pod_repo.enqueue_job_reconcile_for_pod(pod).await;
    }
}

pub(super) struct WatchEventHandlerContext<'a> {
    pub db: &'a dyn DatastoreBackend,
    pub cluster_api: &'a Arc<dyn crate::control_plane::client::LeaderApiClient>,
    pub node_name: &'a str,
    pub containerd_namespace: &'a str,
    pub cluster_reconciliation_enabled: bool,
    pub pod_repo: &'a Arc<crate::kubelet::pod_repository::PodRepository>,
    pub pod_creation_tracker: &'a PodCreationTracker,
    pub retry_state: &'a PodStartRetryTracker,
    pub pod_lifecycle_state: &'a PodLifecycleStateTracker,
    pub pod_lifecycle_router:
        std::sync::Arc<crate::kubelet::pod_lifecycle_router::PodLifecycleRouter>,
    pub task_supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
}

pub(super) async fn handle_watch_event(context: WatchEventHandlerContext<'_>, event: WatchEvent) {
    let WatchEventHandlerContext {
        db,
        cluster_api,
        node_name,
        containerd_namespace,
        cluster_reconciliation_enabled,
        pod_repo,
        pod_creation_tracker,
        retry_state,
        pod_lifecycle_state,
        pod_lifecycle_router,
        task_supervisor,
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
        handle_pvc_event(db, &event, event_name, cluster_reconciliation_enabled).await;
        return;
    }

    if event_kind == "PersistentVolume" {
        handle_pv_event(db, &event, event_name, cluster_reconciliation_enabled).await;
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
        let volumes_root = crate::paths::volumes_root_path(containerd_namespace)
            .to_string_lossy()
            .into_owned();
        let refresh_result = if event.event_type == EventType::Deleted {
            crate::kubelet::volumes::refresh_secret_configmap_volumes_after_delete(
                event_kind,
                event_ns,
                event_name,
                &volumes_root,
                pod_repo.as_ref(),
            )
            .await
        } else {
            crate::kubelet::volumes::refresh_secret_configmap_volumes_from_event(
                event_kind,
                event_ns,
                event_name,
                &event.object,
                &volumes_root,
                pod_repo.as_ref(),
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
        handle_namespace_termination_event(pod_repo, &event).await;
        return;
    }

    // Handle Pod events
    if event_kind != "Pod" {
        return;
    }

    tracing::info!(
        "Pod watcher received {} event for pod {}",
        event.event_type,
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
        enqueue_job_reconcile_for_terminal_watch_pod(pod_repo, &event.object).await;

        // Refresh downwardAPI volumes to reflect metadata changes (labels/annotations)
        let volumes_root = crate::paths::volumes_root_path(containerd_namespace)
            .to_string_lossy()
            .into_owned();
        if let Err(e) =
            crate::kubelet::volumes::refresh_downward_api_volumes(&event.object, &volumes_root)
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
            crate::kubelet::pod_lifecycle_actor::state::remove_pod_state(pod_lifecycle_state, &key)
                .await;
        }
        let orphan_enqueued =
            match crate::kubelet::reconciler::orphan::OrphanScanner::scan_deleted_event(
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
            && let Err(err) = cluster_api
                .delete_pod_cleanup_intent(
                    &key.node_name,
                    &key.namespace,
                    &key.pod_name,
                    &key.pod_uid,
                    crate::datastore::POD_CLEANUP_REASON_NODE_LOST,
                )
                .await
        {
            tracing::warn!(
                node = %key.node_name,
                namespace = %key.namespace,
                pod = %key.pod_name,
                uid = %key.pod_uid,
                error = %err,
                "failed to delete NodeLost pod cleanup intent after deleted-pod orphan enqueue"
            );
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
    pod_repo: &Arc<crate::kubelet::pod_repository::PodRepository>,
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

    if let Err(err) = pod_repo
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

/// Persist a recomputed phase + containerStatuses + conditions for a single pod.
///
/// Pure of CRI: the caller has already collected `container_infos` (state,
/// exit_code, etc.) and computed `new_phase`. This function compares against the
/// existing pod status, skips the DB update when nothing changed, and otherwise
/// writes the new phase, container statuses, and standard pod conditions back
/// to the datastore.
///
/// Extracted from `monitor_running_pods` so the phase-transition path
/// (Running → Succeeded / Failed) can be tested directly with an in-memory
/// `Datastore`.
#[cfg(test)]
pub(super) struct PodPhaseUpdateRequest<'a> {
    pub pod_resource: &'a crate::datastore::Resource,
    pub container_infos: &'a [(String, ContainerInfo)],
    pub restart_counts: &'a std::collections::HashMap<String, i32>,
    pub current_phase: Option<&'a str>,
    pub new_phase: &'a str,
    pub namespace: &'a str,
    pub pod_name: &'a str,
}

#[cfg(test)]
pub(super) async fn apply_pod_phase_update(
    pod_repo: &Arc<crate::kubelet::pod_repository::PodRepository>,
    request: PodPhaseUpdateRequest<'_>,
) {
    let PodPhaseUpdateRequest {
        pod_resource,
        container_infos,
        restart_counts,
        current_phase,
        new_phase,
        namespace,
        pod_name,
    } = request;
    use crate::kubelet::pod_status_builders::build_container_statuses;
    use crate::kubelet::pod_status_logic::extract_ready_containers_from_pod_condition;

    use crate::kubelet::pod_repository::{PodStatusWriter, RuntimeReconcileStatus};
    tracing::info!(
        namespace, pod_name,
        current_phase = current_phase.unwrap_or("None"),
        new_phase,
        uid = %pod_resource.uid,
        pod_ip = ?pod_resource.data.pointer("/status/podIP").and_then(|v| v.as_str()),
        "apply_pod_phase_update called"
    );
    // Build updated containerStatuses
    // Pass readiness probe results: containers with successful readiness probes are marked ready
    let ready_containers = extract_ready_containers_from_pod_condition(&pod_resource.data);
    let mut container_statuses =
        build_container_statuses(container_infos, restart_counts, &ready_containers);
    if container_statuses.is_empty() && has_create_container_config_error_status(&pod_resource.data)
    {
        container_statuses = pod_resource
            .data
            .pointer("/status/containerStatuses")
            .and_then(|s| s.as_array())
            .cloned()
            .unwrap_or_default();
    }

    if matches!(current_phase, Some("Failed" | "Succeeded"))
        && !matches!(new_phase, "Failed" | "Succeeded")
    {
        tracing::debug!(
            "Pod {}/{} is already terminal (phase: {}); ignoring runtime phase {}",
            namespace,
            pod_name,
            current_phase.unwrap_or(""),
            new_phase
        );
        return;
    }

    // Preserve lastState from existing DB status (set by liveness probe restart)
    // build_container_statuses doesn't include lastState, so merge it from DB
    if let Some(existing_statuses) = pod_resource
        .data
        .pointer("/status/containerStatuses")
        .and_then(|s| s.as_array())
    {
        for cs in &mut container_statuses {
            let cs_name = cs.get("name").and_then(|n| n.as_str()).unwrap_or("");
            for existing in existing_statuses {
                if existing.get("name").and_then(|n| n.as_str()) == Some(cs_name) {
                    if let Some(existing_count) =
                        existing.get("restartCount").and_then(|v| v.as_i64())
                        && let Some(obj) = cs.as_object_mut()
                    {
                        let rebuilt_count = obj
                            .get("restartCount")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(0);
                        if existing_count > rebuilt_count {
                            obj.insert(
                                "restartCount".to_string(),
                                serde_json::json!(existing_count),
                            );
                        }
                    }
                    if let Some(last_state) = existing.get("lastState")
                        && let Some(obj) = cs.as_object_mut()
                    {
                        obj.insert("lastState".to_string(), last_state.clone());
                    }
                    preserve_published_container_started_at(cs, existing);
                    break;
                }
            }
        }
    }

    // Status-write dedup intentionally removed (sonobuoy "Container Runtime …
    // should run with the expected status" reproducer): the old gate compared
    // the freshly built status to the stored one and skipped the write when
    // they matched. On a container restart, between the time the prior CRI
    // event wrote `state.terminated` and the time the new container reached
    // `state == 1 (Running)` in CRI, intermediate reconciles could still
    // observe `state == 2` and re-publish the same terminated entry — the
    // dedup then suppressed the eventual terminated→running publish for the
    // new container instance, leaving `ready=false` forever. We now always
    // forward the freshly computed status to the repository writer and let
    // its own per-field diff decide whether to emit a noop watch event.
    tracing::info!(
        target: "klights::pod_status::trace",
        namespace,
        pod_name,
        uid = %pod_resource.uid,
        current_phase = current_phase.unwrap_or("None"),
        new_phase,
        existing_container_states = %container_states_summary(
            pod_resource
                .data
                .get("status")
                .and_then(|s| s.get("containerStatuses"))
                .and_then(|cs| cs.as_array()),
        ),
        new_container_states = %container_states_summary(Some(&container_statuses)),
        "apply_pod_phase_update: forwarding runtime reconcile (dedup disabled)"
    );

    // Best-effort runtime-driven phase reconciliation. The repository
    // overwrites only `phase` and `containerStatuses`, preserving every
    // other status field (`podIP`/`podIPs`/`hostIP`/`hostIPs`/`conditions`/
    // `qosClass`/`initContainerStatuses`) — those belong to writers that
    // own them. On error, log and continue: today this site swallows
    // failures via `let _ = ...`, and the standing Task 16.6 decision is to
    // preserve the infallible-from-caller's-perspective contract here.
    if let Err(e) = pod_repo
        .apply_runtime_reconcile_status_for_uid(
            namespace,
            pod_name,
            &pod_resource.uid,
            RuntimeReconcileStatus {
                phase: new_phase.to_string(),
                container_statuses: container_statuses.clone(),
            },
            None,
        )
        .await
    {
        tracing::warn!(
            "apply_runtime_reconcile_status failed for {}/{}: {}",
            namespace,
            pod_name,
            e
        );
        return;
    }

    // When a pod reaches a terminal phase, enqueue its owning Job for
    // asynchronous reconciliation so status.succeeded/failed counts and
    // conditions are updated promptly. Uses the controller dispatcher
    // workqueue (not inline reconcile) to avoid blocking the watcher.
    if new_phase == "Succeeded" || new_phase == "Failed" {
        pod_repo
            .enqueue_job_reconcile_for_pod(&pod_resource.data)
            .await;
    }
}

#[cfg(test)]
fn has_create_container_config_error_status(pod: &Value) -> bool {
    pod.pointer("/status/containerStatuses")
        .and_then(|s| s.as_array())
        .is_some_and(|statuses| {
            statuses.iter().any(|status| {
                status
                    .pointer("/state/waiting/reason")
                    .and_then(|reason| reason.as_str())
                    == Some("CreateContainerConfigError")
            })
        })
}

#[cfg(test)]
fn preserve_published_container_started_at(
    next: &mut serde_json::Value,
    existing: &serde_json::Value,
) {
    let Some(next_id) = next.get("containerID").and_then(|v| v.as_str()) else {
        return;
    };
    if next_id.is_empty() {
        return;
    }
    if existing.get("containerID").and_then(|v| v.as_str()) != Some(next_id) {
        return;
    }
    let Some(started_at) = existing
        .pointer("/state/running/startedAt")
        .or_else(|| existing.pointer("/state/terminated/startedAt"))
        .cloned()
    else {
        return;
    };
    if let Some(next_started_at) = next.pointer_mut("/state/running/startedAt") {
        *next_started_at = started_at.clone();
    }
    if let Some(next_started_at) = next.pointer_mut("/state/terminated/startedAt") {
        *next_started_at = started_at;
    }
}

/// Render `containerStatuses` as a compact `name=state:rc=N:id=…12` summary
/// for the runtime-reconcile trace logs. Used on both sides of the diff so
/// the log line shows exactly what apply_pod_phase_update is about to
/// forward and what was already on disk.
#[cfg(test)]
fn container_states_summary(statuses: Option<&Vec<serde_json::Value>>) -> String {
    let Some(arr) = statuses else {
        return "<none>".to_string();
    };
    if arr.is_empty() {
        return "<empty>".to_string();
    }
    let mut parts: Vec<String> = Vec::with_capacity(arr.len());
    for cs in arr {
        let name = cs.get("name").and_then(|v| v.as_str()).unwrap_or("?");
        let state_kind = cs
            .get("state")
            .and_then(|s| s.as_object())
            .and_then(|m| m.keys().next().cloned())
            .unwrap_or_else(|| "?".to_string());
        let rc = cs
            .get("restartCount")
            .and_then(|v| v.as_i64())
            .unwrap_or(-1);
        let id = cs
            .get("containerID")
            .and_then(|v| v.as_str())
            .map(|s| s.rsplit('/').next().unwrap_or(s))
            .map(|s| s.chars().take(12).collect::<String>())
            .unwrap_or_default();
        let ready = cs
            .get("ready")
            .and_then(|v| v.as_bool())
            .map(|b| if b { "T" } else { "F" })
            .unwrap_or("?");
        parts.push(format!("{name}={state_kind}:rc={rc}:ready={ready}:id={id}"));
    }
    parts.join(",")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Arc;

    struct EnvVarGuard {
        name: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl EnvVarGuard {
        fn set(name: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
            let previous = std::env::var_os(name);
            unsafe { std::env::set_var(name, value) };
            Self { name, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(previous) = &self.previous {
                unsafe { std::env::set_var(self.name, previous) };
            } else {
                unsafe { std::env::remove_var(self.name) };
            }
        }
    }

    fn fixture_supervisor() -> Arc<crate::task_supervisor::TaskSupervisor> {
        Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ))
    }

    fn fixture_pod_repo(
        db_handle: crate::datastore::DatastoreHandle,
        supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
    ) -> Arc<crate::kubelet::pod_repository::PodRepository> {
        let pod_repo = Arc::new(crate::kubelet::pod_repository::PodRepository::new(
            db_handle,
            supervisor.clone(),
            Arc::new(crate::side_effects::SideEffectRegistry::new()),
            crate::side_effects::SideEffectMetrics::new(),
        ));

        let registry = Arc::new(crate::kubelet::pod_lifecycle_actor::registry::PodLifecycleRegistry::new(
            supervisor,
            crate::kubelet::pod_lifecycle_actor::config::PodLifecycleConcurrencyConfig::production_default(),
            Arc::new(std::sync::Mutex::new(
                Arc::new(
                    crate::kubelet::pod_lifecycle_router::executor::NoopExecutor
                ) as Arc<dyn crate::kubelet::pod_lifecycle_router::executor::PodWorkExecutor>,
            )),
        ));
        let router = Arc::new(
            crate::kubelet::pod_lifecycle_router::PodLifecycleRouter::new_actor_with_executor(
                registry,
                Arc::new(crate::kubelet::pod_lifecycle_router::executor::NoopExecutor),
            ),
        );
        pod_repo.set_pod_lifecycle_router_for_node(router, "worker-a".to_string());
        pod_repo
    }

    #[test]
    fn deleted_unknown_local_pod_maps_to_nodelost_cleanup_intent_key() {
        let event = WatchEvent::deleted(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "kube-system",
                "name": "coredns-old",
                "uid": "uid-old"
            },
            "spec": {"nodeName": "mn-controlplane1"},
            "status": {"phase": "Unknown"}
        }));

        let key = node_lost_cleanup_intent_key_for_deleted_pod(&event, "mn-controlplane1")
            .expect("deleted Unknown pod on this node must map to cleanup intent key");

        assert_eq!(
            key,
            NodeLostCleanupIntentKey {
                node_name: "mn-controlplane1".to_string(),
                namespace: "kube-system".to_string(),
                pod_name: "coredns-old".to_string(),
                pod_uid: "uid-old".to_string(),
            }
        );
    }

    #[test]
    fn deleted_running_or_remote_pod_does_not_map_to_nodelost_cleanup_intent_key() {
        let running = WatchEvent::deleted(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"namespace": "default", "name": "web", "uid": "uid-web"},
            "spec": {"nodeName": "mn-controlplane1"},
            "status": {"phase": "Running"}
        }));
        assert!(
            node_lost_cleanup_intent_key_for_deleted_pod(&running, "mn-controlplane1").is_none()
        );

        let remote = WatchEvent::deleted(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"namespace": "default", "name": "web", "uid": "uid-web"},
            "spec": {"nodeName": "mn-controlplane2"},
            "status": {"phase": "Unknown"}
        }));
        assert!(
            node_lost_cleanup_intent_key_for_deleted_pod(&remote, "mn-controlplane1").is_none()
        );
    }

    #[tokio::test]
    async fn namespace_termination_event_enqueues_actor_delete_for_terminating_local_pod() {
        let (db, db_handle) = crate::datastore::test_support::in_memory_with_handle().await;
        let supervisor = fixture_supervisor();
        let pod_repo = fixture_pod_repo(db_handle.clone(), supervisor);

        db.create_resource(
            "v1",
            "Pod",
            Some("terminating-ns"),
            "left-behind",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "terminating-ns",
                    "name": "left-behind",
                    "uid": "uid-left-behind",
                    "deletionTimestamp": "2026-05-18T20:06:06Z"
                },
                "spec": {
                    "nodeName": "worker-a",
                    "containers": [{"name": "app", "image": "nginx"}]
                },
                "status": {"phase": "Running"}
            }),
        )
        .await
        .expect("create terminating pod");

        handle_namespace_termination_event(
            &pod_repo,
            &WatchEvent::modified(json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": "terminating-ns",
                    "uid": "ns-uid",
                    "deletionTimestamp": "2026-05-18T20:06:06Z"
                },
                "spec": {"finalizers": ["kubernetes"]},
                "status": {"phase": "Terminating"}
            })),
        )
        .await;

        let row = db_handle
            .pod_workqueue_claim_due(i64::MAX)
            .await
            .expect("claim pod workqueue row")
            .expect("namespace termination event must enqueue local actor delete work");
        assert_eq!(row.kind, crate::datastore::PodWorkqueueKind::Pod);
        assert_eq!(row.namespace, "terminating-ns");
        assert_eq!(row.name, "left-behind");
        assert_eq!(row.uid, "uid-left-behind");
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)] // serializes process-global env used to prove the handler ignores it
    async fn configmap_watch_refresh_uses_configured_runtime_namespace_not_global_env() {
        let _env_lock = crate::TEST_ENV_LOCK.lock().unwrap();
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_ns = "event-handler-runtime-ns";
        let wrong_global_ns = "event-handler-wrong-global-ns";
        let _data_root = EnvVarGuard::set("KLIGHTS_DATA_ROOT", temp.path());
        let _runtime_env = EnvVarGuard::set("KLIGHTS_CONTAINERD_NAMESPACE", wrong_global_ns);

        let (db, db_handle) = crate::datastore::test_support::in_memory_with_handle().await;
        let supervisor = fixture_supervisor();
        let pod_repo = fixture_pod_repo(db_handle.clone(), supervisor.clone());
        let cluster_api: Arc<dyn crate::control_plane::client::LeaderApiClient> =
            Arc::new(crate::control_plane::client::local::LocalApiClient::new(
                db_handle.clone(),
                "worker-a".to_string(),
                crate::control_plane::client::local::always_leader_watch(),
            ));
        let pod_creation_tracker: PodCreationTracker =
            Arc::new(tokio::sync::Mutex::new(HashSet::new()));
        let retry_state: PodStartRetryTracker =
            Arc::new(tokio::sync::Mutex::new(PodStartRetryState::new()));
        let pod_lifecycle_state = new_pod_lifecycle_state_tracker();
        let registry = Arc::new(
            crate::kubelet::pod_lifecycle_actor::registry::PodLifecycleRegistry::new(
                supervisor.clone(),
                crate::kubelet::pod_lifecycle_actor::config::PodLifecycleConcurrencyConfig::production_default(),
                Arc::new(std::sync::Mutex::new(
                    Arc::new(crate::kubelet::pod_lifecycle_router::executor::NoopExecutor)
                        as Arc<dyn crate::kubelet::pod_lifecycle_router::executor::PodWorkExecutor>,
                )),
            ),
        );
        let pod_lifecycle_router = Arc::new(
            crate::kubelet::pod_lifecycle_router::PodLifecycleRouter::new_actor_with_executor(
                registry,
                Arc::new(crate::kubelet::pod_lifecycle_router::executor::NoopExecutor),
            ),
        );

        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "cm-pod",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"namespace": "default", "name": "cm-pod", "uid": "uid-cm-pod"},
                "spec": {
                    "nodeName": "worker-a",
                    "containers": [{"name": "app", "image": "registry.k8s.io/e2e-test-images/agnhost:2.40"}],
                    "volumes": [{"name": "config-vol", "configMap": {"name": "my-config"}}]
                },
                "status": {"phase": "Running"}
            }),
        )
        .await
        .expect("create pod");

        let volume_path = crate::paths::volumes_root_path(runtime_ns)
            .join("default_cm-pod/volumes/config-map/config-vol");
        let volume_path = volume_path.to_string_lossy().into_owned();
        std::fs::create_dir_all(&volume_path).expect("create mounted configmap volume");
        std::fs::write(format!("{volume_path}/data-1"), "value-1").expect("seed mounted data");

        handle_watch_event(
            WatchEventHandlerContext {
                db: db_handle.as_ref(),
                cluster_api: &cluster_api,
                node_name: "worker-a",
                containerd_namespace: runtime_ns,
                cluster_reconciliation_enabled: false,
                pod_repo: &pod_repo,
                pod_creation_tracker: &pod_creation_tracker,
                retry_state: &retry_state,
                pod_lifecycle_state: &pod_lifecycle_state,
                pod_lifecycle_router,
                task_supervisor: supervisor,
            },
            WatchEvent::modified(json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"namespace": "default", "name": "my-config"},
                "data": {"data-1": "value-2"}
            })),
        )
        .await;

        let refreshed =
            crate::utils::read_utf8_file(format!("{volume_path}/data-1")).expect("read refresh");
        assert_eq!(refreshed, "value-2");
    }
}
