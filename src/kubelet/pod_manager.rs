use crate::datastore::sqlite::DatastoreWatchReplaySource;
use crate::datastore::{DatastoreBackend, DatastoreHandle, WatchTarget};
#[cfg(test)]
use crate::kubelet::pod_creation_state::PodStartSource;
use crate::kubelet::pod_creation_state::{
    PodCreationTracker, PodStartRetryState, PodStartRetryTracker, clear_pod_creation_inflight,
    should_clear_pod_creation_inflight,
};
use crate::kubelet::pod_lifecycle_actor::message::{LifecycleMessage, PodLifecycleKey};
use crate::kubelet::pod_lifecycle_actor::state::{
    PodLifecycleStateTracker, new_pod_lifecycle_state_tracker,
};
#[cfg(test)]
use crate::kubelet::pod_runtime_state::{PodRuntimeState, StartupDecision, decide_startup_action};
#[cfg(test)]
use crate::kubelet::pod_status_builders::{
    build_container_statuses, build_creation_error_statuses, build_failed_init_container_statuses,
    cri_timestamp_from_ns,
};
#[cfg(test)]
use crate::kubelet::pod_status_logic::{ContainerInfo, compute_pod_phase, should_restart};
use crate::kubelet::pod_watch_handlers::{handle_pv_event, handle_pvc_event};
use crate::watch::{
    EventType, SignalWatchCursor, WatchCursorError, WatchDeliveryScope, WatchEvent,
    WatchEventFilter, WatchSignalReceiver, WatchTopic, WindowPolicy,
};
use anyhow::Result;
#[cfg(test)]
use event_handlers::{PodPhaseUpdateRequest, apply_pod_phase_update};
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::sync::Mutex;
use tokio::sync::mpsc;

/// Cached host IP address (node's real non-loopback IP).
/// Set once during pod watcher startup, used by update_pod_status.
static HOST_IP: OnceLock<String> = OnceLock::new();
type CriEventReceiver = mpsc::Receiver<crate::kubelet::cri_events::KubeletEvent>;

pub mod event_handlers;
mod startup;

mod deadline_timers;
mod event_forwarder;

#[derive(Clone)]
pub struct PodWatcherRuntimePorts {
    cri_runtime: Arc<dyn crate::kubelet::pod_runtime::cri::CriRuntime>,
    container_control: Arc<dyn crate::kubelet::pod_runtime::cri::ContainerRuntimeControl>,
}

impl PodWatcherRuntimePorts {
    pub fn new(
        cri_runtime: Arc<dyn crate::kubelet::pod_runtime::cri::CriRuntime>,
        container_control: Arc<dyn crate::kubelet::pod_runtime::cri::ContainerRuntimeControl>,
    ) -> Self {
        Self {
            cri_runtime,
            container_control,
        }
    }
}

#[derive(Clone)]
struct PodWatcherRuntimeContext {
    db: DatastoreHandle,
    cluster_api: Arc<dyn crate::control_plane::client::LeaderApiClient>,
    node_local: Option<crate::datastore::node_local::NodeLocalHandle>,
    config: Arc<crate::KlightsConfig>,
    task_supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
    pod_repository: Arc<crate::kubelet::pod_repository::PodRepository>,
    pod_lifecycle_router: Arc<crate::kubelet::pod_lifecycle_router::PodLifecycleRouter>,
    cluster_reconciliation_enabled: bool,
    pod_lifecycle_rx: Arc<
        tokio::sync::Mutex<
            Option<tokio::sync::mpsc::Receiver<crate::kubelet::lifecycle::LifecycleCommand>>,
        >,
    >,
    pod_start_retry_state: Option<PodStartRetryTracker>,
}

impl PodWatcherRuntimeContext {
    fn from_app_state(state: &crate::api::AppState) -> Self {
        Self {
            db: state.db.clone(),
            cluster_api: state.cluster_api.clone(),
            node_local: None,
            config: state.config.clone(),
            task_supervisor: state.task_supervisor.clone(),
            pod_repository: state.pod_repository.clone(),
            pod_lifecycle_router: state
                .pod_lifecycle_router
                .clone()
                .expect("pod_lifecycle_router must be set in AppState before run_pod_watcher"),
            cluster_reconciliation_enabled: state
                .is_raft_leader_rx
                .as_ref()
                .is_none_or(|proxy| proxy.is_leader()),
            pod_lifecycle_rx: state
                .pod_lifecycle_rx
                .clone()
                .expect("pod_lifecycle_rx must be set in AppState before run_pod_watcher"),
            pod_start_retry_state: state.pod_start_retry_state.clone(),
        }
    }

    fn from_kubelet_context(
        context: &crate::kubelet::context::KubeletContext,
        db: DatastoreHandle,
    ) -> Self {
        Self {
            db,
            cluster_api: context.cluster_api.clone(),
            node_local: Some(context.node_local.clone()),
            config: context.config.clone(),
            task_supervisor: context.task_supervisor.clone(),
            pod_repository: context.pod_repository.clone(),
            pod_lifecycle_router: context.pod_lifecycle_router.clone(),
            cluster_reconciliation_enabled: false,
            pod_lifecycle_rx: context.pod_lifecycle_rx.clone(),
            pod_start_retry_state: Some(context.pod_start_retry_state.clone()),
        }
    }
}

fn pod_watcher_node_field_selector(node_name: &str) -> String {
    format!("spec.nodeName={node_name}")
}

fn pod_watcher_node_event_filter(node_name: &str) -> WatchEventFilter {
    WatchEventFilter::new().with_field_selector(
        "v1",
        "Pod",
        pod_watcher_node_field_selector(node_name),
    )
}

fn pod_watcher_watch_topics() -> Vec<WatchTopic> {
    vec![
        WatchTopic::new("v1", "Pod"),
        WatchTopic::new("v1", "PersistentVolumeClaim"),
        WatchTopic::new("v1", "PersistentVolume"),
        WatchTopic::new("v1", "Secret"),
        WatchTopic::new("v1", "ConfigMap"),
        WatchTopic::new("v1", "Namespace"),
    ]
}

fn pod_watcher_replay_targets() -> Vec<WatchTarget> {
    vec![
        WatchTarget::namespaced("v1", "Pod"),
        WatchTarget::namespaced("v1", "PersistentVolumeClaim"),
        WatchTarget::cluster("v1", "PersistentVolume"),
        WatchTarget::namespaced("v1", "Secret"),
        WatchTarget::namespaced("v1", "ConfigMap"),
        WatchTarget::cluster("v1", "Namespace"),
    ]
}

#[cfg(test)]
mod watch_topic_tests {
    use super::*;

    #[test]
    fn pod_watcher_live_topics_cover_secret_configmap_refresh_sources() {
        let topics = pod_watcher_watch_topics();
        assert!(
            topics.contains(&crate::watch::WatchTopic::new("v1", "ConfigMap")),
            "ConfigMap watch events must reach the pod watcher so mounted ConfigMap volumes refresh"
        );
        assert!(
            topics.contains(&crate::watch::WatchTopic::new("v1", "Secret")),
            "Secret watch events must reach the pod watcher so mounted Secret volumes refresh"
        );
    }
}

struct PodRecovery<'a> {
    pod_repo: &'a Arc<crate::kubelet::pod_repository::PodRepository>,
    node_name: &'a str,
    retry_state: &'a PodStartRetryTracker,
    pod_lifecycle_router: std::sync::Arc<crate::kubelet::pod_lifecycle_router::PodLifecycleRouter>,
}
async fn spawn_cri_event_forwarder(
    cri: Arc<dyn crate::kubelet::pod_runtime::cri::CriRuntime>,
    cancel_token: tokio_util::sync::CancellationToken,
    task_supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
    lifecycle_tx: Option<
        tokio::sync::mpsc::Sender<crate::kubelet::reconciler::cri_reconnect::CriStreamLifecycle>,
    >,
) -> CriEventReceiver {
    event_forwarder::spawn_cri_event_forwarder(cri, cancel_token, task_supervisor, lifecycle_tx)
        .await
}

pub fn get_cached_host_ip() -> &'static str {
    HOST_IP.get().map(|s| s.as_str()).unwrap_or("127.0.0.1")
}

/// Configuration for pod watcher
#[derive(Clone)]
pub struct PodWatcherConfig {
    pub service_cidr: String,
    pub node_name: String,
    pub containerd_namespace: String,
}

async fn rotate_all_pod_logs(containerd_ns: &str) {
    use crate::kubelet::log_rotation::{get_max_log_files, get_max_log_size};

    let log_root = crate::paths::pod_logs_root_path(containerd_ns);
    crate::kubelet::pod_fs::PodFs::rotate_logs(log_root, get_max_log_size(), get_max_log_files())
        .await;
}

pub async fn run_pod_watcher(
    runtime_ports: PodWatcherRuntimePorts,
    state: std::sync::Arc<crate::api::AppState>,
    cancel_token: tokio_util::sync::CancellationToken,
) {
    run_pod_watcher_with_runtime(
        runtime_ports,
        PodWatcherRuntimeContext::from_app_state(state.as_ref()),
        cancel_token,
    )
    .await;
}

pub async fn run_pod_watcher_with_context(
    runtime_ports: PodWatcherRuntimePorts,
    context: std::sync::Arc<crate::kubelet::context::KubeletContext>,
    db: DatastoreHandle,
    cancel_token: tokio_util::sync::CancellationToken,
) {
    run_pod_watcher_with_runtime(
        runtime_ports,
        PodWatcherRuntimeContext::from_kubelet_context(context.as_ref(), db),
        cancel_token,
    )
    .await;
}

async fn run_pod_watcher_with_runtime(
    runtime_ports: PodWatcherRuntimePorts,
    state: PodWatcherRuntimeContext,
    cancel_token: tokio_util::sync::CancellationToken,
) {
    tracing::info!("Starting pod watcher task");

    let container_control = runtime_ports.container_control.clone();
    let cri_runtime = runtime_ports.cri_runtime.clone();

    let db_handle = state.db.clone();
    let db = db_handle.as_ref();

    // Compute and cache the host IP for pod status from the registered Node
    // InternalIP. Node names are Kubernetes identities, not DNS names.
    let node_ip = crate::kubelet::node_ip::resolve_node_ip_from_leader_api_or_hostname(
        state.cluster_api.as_ref(),
        &state.config.node_name,
    )
    .await;
    let _ = HOST_IP.set(node_ip.clone());
    tracing::info!("Host IP for pod status: {}", node_ip);

    let config = PodWatcherConfig {
        service_cidr: state.config.service_cidr.clone(),
        node_name: state.config.node_name.clone(),
        containerd_namespace: state.config.containerd_namespace.clone(),
    };
    let _service_cidr = config.service_cidr.as_str();

    // Use the configured instance namespace for all host-side pod state.
    // CRI metadata can still surface k8s.io for runtime internals, but the
    // host paths for logs, hosts files, and termination messages must stay in
    // the klights instance namespace.
    let containerd_namespace = config.containerd_namespace.clone();

    let mut lifecycle_rx = state
        .pod_lifecycle_rx
        .lock()
        .await
        .take()
        .expect("pod lifecycle receiver must be set before run_pod_watcher");
    let watch_topics = pod_watcher_watch_topics();
    let signal_rx = WatchSignalReceiver::new(
        watch_topics
            .iter()
            .cloned()
            .map(|topic| state.db.subscribe_watch_signals(topic))
            .collect(),
    );

    // Wait for CNI to be ready before creating any pods.
    // CRI gRPC is ready but the CNI plugin may not have loaded its config yet.
    // Probe by listing sandboxes — this is cheap and forces CNI plugin init.
    {
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(30);
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            match cri_runtime.list_pod_sandboxes(None).await {
                Ok(_) => {
                    tracing::info!(
                        "CNI ready after {} attempts ({:?})",
                        attempt,
                        start.elapsed()
                    );
                    break;
                }
                Err(e) => {
                    if start.elapsed() >= timeout {
                        tracing::warn!(
                            "CNI readiness probe timed out after {}s: {:#}",
                            timeout.as_secs(),
                            e
                        );
                        break;
                    }
                    tracing::debug!("CNI readiness probe attempt {}: {:#}", attempt, e);
                    let _ = state
                        .task_supervisor
                        .sleep(
                            "pod_watcher_cni_readiness_retry",
                            std::time::Duration::from_secs(1),
                        )
                        .await;
                }
            }
        }
    }

    let pod_creation_tracker: PodCreationTracker = Arc::new(Mutex::new(HashSet::new()));
    let pod_start_retry_state: PodStartRetryTracker = state
        .pod_start_retry_state
        .clone()
        .unwrap_or_else(|| Arc::new(Mutex::new(PodStartRetryState::new())));
    let pod_lifecycle_state = new_pod_lifecycle_state_tracker();
    let pod_lifecycle_router = state.pod_lifecycle_router.clone();

    let mut cri_reconnect_lifecycle_tx = None;
    if let Some(node_local) = state.node_local.clone() {
        let reconciler = crate::kubelet::reconciler::startup::StartupReconciler::new(
            config.node_name.clone(),
            config.containerd_namespace.clone(),
            state.cluster_api.clone(),
            node_local.clone(),
            cri_runtime.clone(),
            pod_lifecycle_router.clone(),
        );
        if let Err(err) = reconciler.run_once().await {
            tracing::warn!("startup reconciler failed: {err:#}");
        }
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        cri_reconnect_lifecycle_tx = Some(tx);
        let reconnect = std::sync::Arc::new(
            crate::kubelet::reconciler::cri_reconnect::CriReconnectReconciler::new(
                config.node_name.clone(),
                state.cluster_api.clone(),
                node_local,
                cri_runtime.clone(),
                container_control.clone(),
                pod_lifecycle_router.clone(),
            ),
        );
        let reconnect_cancel = cancel_token.clone();
        if let Err(err) = state
            .task_supervisor
            .spawn_async(
                crate::task_supervisor::TaskCategory::Background,
                "cri_reconnect_reconciler",
                async move {
                    reconnect.run_lifecycle_loop(rx, reconnect_cancel).await;
                },
            )
            .await
        {
            tracing::warn!("failed to spawn CRI reconnect reconciler: {err}");
        }
    }

    let event_filter = pod_watcher_node_event_filter(&config.node_name);
    let mut cursor = SignalWatchCursor::new_many(
        signal_rx,
        DatastoreWatchReplaySource::new(db_handle.clone(), pod_watcher_replay_targets()),
        watch_topics,
        WatchDeliveryScope::All,
        db.get_current_resource_version().await.unwrap_or(0),
        WindowPolicy::default_watch_delivery(),
    );

    {
        let mut pod_recovery = PodRecovery::new(
            &state.pod_repository,
            &config.node_name,
            &pod_start_retry_state,
            pod_lifecycle_router.clone(),
        );
        if let Err(e) = pod_recovery.recover_existing_pods().await {
            tracing::warn!("Boot-time pod recovery failed: {:#}", e);
        }
    }

    // Subscribe to CRI container events in a dedicated task. containerd can keep
    // GetContainerEvents pending until an event exists, so treating "no event
    // yet" as a subscribe timeout loses short-lived container exits. The
    // forwarder owns that long-lived await and buffers lifecycle events into the
    // pod watcher without blocking watch/retry/log-rotation arms.
    let mut cri_event_rx = spawn_cri_event_forwarder(
        cri_runtime.clone(),
        cancel_token.clone(),
        state.task_supervisor.clone(),
        cri_reconnect_lifecycle_tx,
    )
    .await;

    // Supervised periodic trigger for log rotation (60 second interval).
    let (log_rotation_tick_tx, mut log_rotation_tick_rx) = mpsc::channel::<()>(4);
    // JUSTIFY: log rotation is a wall-clock cadence; container log size
    // has no underlying event source, and a "log size grew" trigger
    // would itself require polling.
    if let Err(err) = state
        .task_supervisor
        .spawn_interval(
            "pod_watcher_log_rotation",
            std::time::Duration::from_secs(60),
            move |_| {
                let log_rotation_tick_tx = log_rotation_tick_tx.clone();
                async move {
                    let _ = log_rotation_tick_tx.send(()).await;
                }
            },
        )
        .await
    {
        tracing::warn!("Failed to spawn pod watcher log-rotation timer: {}", err);
    }
    // P0-LEAK-03 plan step 4: `phase_sync_interval` (5s polling) is gone. The
    // race it covered — container exits before update_pod_status("Running") —
    // is fixed by ordering: `create_pod` now marks the pod Running with all
    // containers in Waiting state *before* the start_container pass, so
    // ContainerStoppedEvent always sees a Running pod and `process_event_pod`
    // transitions it to Failed/Succeeded cleanly. The CRI event stream is the
    // sole driver of phase reconciliation; the reconnect arm above keeps it
    // live across containerd hiccups.
    match cursor.prime_replay_or_expired().await {
        Ok(replayed) => {
            tracing::debug!(
                "Pod watcher primed {} replay events before entering live watch",
                replayed
            );
        }
        Err(err) => {
            tracing::warn!(?err, "Pod watcher initial replay failed");
        }
    }

    loop {
        tokio::select! {
            // Handle cancellation signal
            _ = cancel_token.cancelled() => {
                tracing::info!("Pod watcher cancelled, shutting down");
                break;
            }

            // Handle watch events with replay retry
            event_result = cursor.next_event() => {
                let event = match event_result {
                    Ok(event) => event,
                    Err(WatchCursorError::Closed) => {
                        tracing::warn!("Pod watcher signal channel closed");
                        break;
                    }
                    Err(WatchCursorError::Expired) => {
                        tracing::warn!("Pod watcher replay window expired");
                        break;
                    }
                    Err(WatchCursorError::Replay(err)) => {
                        tracing::warn!("Pod watcher replay failed: {err:#}");
                        break;
                    }
                };
                if !event_filter.matches(&event) {
                    continue;
                }
                // Fire-and-forget lifecycle trace message: spawn through the
                // supervisor so actor sends never block event processing.
                // handle_watch_event must always run regardless of actor state.
                if let Some(message) = lifecycle_message_from_watch_event(&event) {
                    let _ = pod_lifecycle_router.route(message).await;
                }
                event_handlers::handle_watch_event(
                    event_handlers::WatchEventHandlerContext {
                        db,
                        cluster_api: &state.cluster_api,
                        node_name: &config.node_name,
                        cluster_reconciliation_enabled: state.cluster_reconciliation_enabled,
                        pod_repo: &state.pod_repository,
                        pod_creation_tracker: &pod_creation_tracker,
                        retry_state: &pod_start_retry_state,
                        pod_lifecycle_state: &pod_lifecycle_state,
                        pod_lifecycle_router: pod_lifecycle_router.clone(),
                        task_supervisor: state.task_supervisor.clone(),
                    },
                    event,
                ).await;
            }

            Some(ev) = cri_event_rx.recv() => {
                tracing::info!(
                    container_id = %&ev.container_id[..12.min(ev.container_id.len())],
                    kind = ev.kind.as_str(),
                    "CRI event received"
                );
                if let Some(key) = pod_lifecycle_key_for_cri_event(
                    container_control.as_ref(),
                    &state.pod_repository,
                    &ev,
                ).await {
                    let _ = pod_lifecycle_router
                        .route(LifecycleMessage::CriEvent {
                            key,
                            container_id: ev.container_id.clone(),
                            kind: ev.kind,
                        })
                        .await;
                }
                // R2f: process_event_pod is now owned by the executor
                // via CriEvent → ReconcileRuntime.
            }

            Some(cmd) = lifecycle_rx.recv() => {
                // R2g: Lifecycle commands route through router → actor → executor.
                // The executor handles all command dispatching.
                if let Some(message) = lifecycle_message_from_command(&state.pod_repository, cmd.clone()).await {
                    let _ = pod_lifecycle_router.route(message).await;
                }
            }

            // Handle log rotation timer tick
            Some(()) = log_rotation_tick_rx.recv() => {
                rotate_all_pod_logs(&containerd_namespace).await;
            }
        }
    }

    tracing::warn!("Pod watcher task ended");
}

fn pod_lifecycle_key_from_pod(pod: &Value) -> Option<PodLifecycleKey> {
    let namespace = pod
        .pointer("/metadata/namespace")
        .and_then(|n| n.as_str())
        .unwrap_or("default");
    let name = pod.pointer("/metadata/name").and_then(|n| n.as_str())?;
    let uid = pod
        .pointer("/metadata/uid")
        .and_then(|uid| uid.as_str())
        .unwrap_or_default();
    Some(PodLifecycleKey::new(namespace, name, uid))
}

fn pod_resource_version(pod: &Value) -> Option<i64> {
    pod.pointer("/metadata/resourceVersion").and_then(|rv| {
        rv.as_i64()
            .or_else(|| rv.as_str().and_then(|s| s.parse::<i64>().ok()))
    })
}

fn lifecycle_message_from_watch_event(event: &WatchEvent) -> Option<LifecycleMessage> {
    if event.object.pointer("/kind").and_then(|kind| kind.as_str()) != Some("Pod") {
        return None;
    }

    let pod = event.object.as_ref();
    let key = pod_lifecycle_key_from_pod(pod)?;
    let resource_version = pod_resource_version(pod);
    let pod = pod.clone();
    match event.event_type {
        EventType::Added => Some(LifecycleMessage::WatchAdded {
            key,
            resource_version,
            pod,
        }),
        EventType::Modified => Some(LifecycleMessage::WatchModified {
            key,
            resource_version,
            pod,
        }),
        EventType::Deleted => Some(LifecycleMessage::WatchDeleted {
            key,
            resource_version,
            pod,
        }),
        EventType::Bookmark | EventType::Error => None,
    }
}

async fn pod_lifecycle_key_for_pod_name(
    pod_repo: &Arc<crate::kubelet::pod_repository::PodRepository>,
    namespace: &str,
    pod_name: &str,
) -> Option<PodLifecycleKey> {
    use crate::kubelet::pod_repository::PodReader;

    match pod_repo.get_pod(namespace, pod_name).await {
        Ok(Some(pod_resource)) => pod_lifecycle_key_from_pod(&pod_resource.data),
        Ok(None) => None,
        Err(err) => {
            tracing::debug!(
                namespace,
                pod = pod_name,
                "failed to read pod for lifecycle actor routing: {err:#}"
            );
            None
        }
    }
}

async fn pod_lifecycle_key_for_cri_event(
    container_control: &dyn crate::kubelet::pod_runtime::cri::ContainerRuntimeControl,
    pod_repo: &Arc<crate::kubelet::pod_repository::PodRepository>,
    event: &crate::kubelet::cri_events::KubeletEvent,
) -> Option<PodLifecycleKey> {
    let resolved = match (event.pod_namespace.as_deref(), event.pod_name.as_deref()) {
        (Some(namespace), Some(name)) => Some((namespace.to_string(), name.to_string())),
        _ => match container_control
            .pod_metadata_for_container(&event.container_id)
            .await
        {
            Ok(resolved) => resolved,
            Err(err) => {
                tracing::debug!(
                    container_id = %event.container_id,
                    "failed to resolve CRI event pod for lifecycle actor routing: {err:#}"
                );
                None
            }
        },
    }?;

    pod_lifecycle_key_for_pod_name(pod_repo, &resolved.0, &resolved.1).await
}

fn lifecycle_command_target(
    command: &crate::kubelet::lifecycle::LifecycleCommand,
) -> (&str, &str, &str) {
    command.target()
}

pub(crate) async fn lifecycle_message_from_command(
    _pod_repo: &Arc<crate::kubelet::pod_repository::PodRepository>,
    command: crate::kubelet::lifecycle::LifecycleCommand,
) -> Option<LifecycleMessage> {
    let (namespace, pod_name, pod_uid) = lifecycle_command_target(&command);
    if pod_uid.is_empty() {
        tracing::warn!(
            namespace,
            pod = pod_name,
            "dropping lifecycle command without pod uid"
        );
        return None;
    }
    let key = PodLifecycleKey::new(namespace, pod_name, pod_uid);
    Some(LifecycleMessage::LifecycleCommand { key, command })
}

async fn clear_pod_start_retry_state(
    retry_state: &PodStartRetryTracker,
    namespace: &str,
    pod_name: &str,
) {
    retry_state.lock().await.clear(namespace, pod_name);
}

#[cfg(test)]
fn parse_deadline_timer_delay_secs(
    pod: &serde_json::Value,
) -> Option<(String, String, u64, String)> {
    deadline_timers::parse_deadline_timer_delay_secs(pod)
}
async fn schedule_active_deadline_timer_for_modified_pod(
    pod: &serde_json::Value,
    task_supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
    pod_lifecycle_router: std::sync::Arc<crate::kubelet::pod_lifecycle_router::PodLifecycleRouter>,
) {
    deadline_timers::schedule_active_deadline_timer_for_modified_pod(
        pod,
        task_supervisor,
        pod_lifecycle_router,
    )
    .await
}
#[cfg(test)]
fn latest_container_infos_by_name(
    all_container_infos: Vec<(String, ContainerInfo, i64)>,
) -> Vec<(String, ContainerInfo)> {
    let mut latest_containers: std::collections::HashMap<String, (ContainerInfo, i64)> =
        std::collections::HashMap::new();
    for (name, info, created_at) in all_container_infos {
        let name_clone = name.clone();
        let order_key = container_attempt_order_key(&info, created_at);
        match latest_containers.get(&name) {
            Some((_, existing_order_key)) if order_key <= *existing_order_key => continue,
            _ => {
                latest_containers.insert(name_clone, (info, order_key));
            }
        }
    }

    latest_containers
        .into_iter()
        .map(|(name, (info, _))| (name, info))
        .collect()
}

#[cfg(test)]
fn container_attempt_order_key(info: &ContainerInfo, created_at: i64) -> i64 {
    created_at.max(info.started_at).max(info.finished_at)
}

#[cfg(test)]
async fn persist_runtime_restart_status(
    pod_repo: &Arc<crate::kubelet::pod_repository::PodRepository>,
    pod_resource: &crate::datastore::Resource,
    namespace: &str,
    pod_name: &str,
    container_name: &str,
    info: &ContainerInfo,
) -> Result<Option<i32>> {
    use crate::kubelet::pod_repository::PodStatusWriter;

    let updated = pod_repo
        .note_container_restart_for_uid(
            namespace,
            pod_name,
            &pod_resource.uid,
            container_name,
            runtime_restart_last_state(info),
            None,
        )
        .await?;
    Ok(updated
        .as_ref()
        .and_then(|resource| container_restart_count(&resource.data, container_name)))
}

#[cfg(test)]
fn runtime_restart_last_state(info: &ContainerInfo) -> Value {
    let reason = if info.exit_code == 0 {
        "Completed"
    } else {
        "Error"
    };
    let mut last_state = serde_json::json!({
        "terminated": {
            "exitCode": info.exit_code,
            "reason": reason,
            "startedAt": cri_timestamp_from_ns(info.started_at),
            "finishedAt": cri_timestamp_from_ns(info.finished_at),
        }
    });
    if !info.termination_message.is_empty()
        && let Some(terminated) = last_state
            .get_mut("terminated")
            .and_then(|value| value.as_object_mut())
    {
        terminated.insert(
            "message".to_string(),
            serde_json::json!(info.termination_message),
        );
    }
    last_state
}

#[cfg(test)]
fn container_restart_count(pod: &Value, container_name: &str) -> Option<i32> {
    pod.pointer("/status/containerStatuses")
        .and_then(|statuses| statuses.as_array())
        .and_then(|statuses| {
            statuses.iter().find(|status| {
                status.get("name").and_then(|value| value.as_str()) == Some(container_name)
            })
        })
        .and_then(|status| status.get("restartCount"))
        .and_then(|count| count.as_i64())
        .and_then(|count| i32::try_from(count).ok())
}

#[cfg(test)]
mod failure_reason_tests;

#[cfg(test)]
mod tests;
