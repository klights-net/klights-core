use crate::pod_creation_state::{
    PodCreationTracker, PodStartRetryTracker, clear_pod_creation_inflight,
    should_clear_pod_creation_inflight,
};
use crate::pod_lifecycle_actor::message::{LifecycleMessage, PodLifecycleKey};
use crate::pod_lifecycle_actor::state::{
    PodLifecycleStateTracker, new_pod_lifecycle_state_tracker,
};
use crate::pod_watch_handlers::PersistentVolumeEventHandler;
use crate::pod_watch_source::{
    PodWatchCheckpoint, PodWatchDisconnect, PodWatchEvent, PodWatchRecoveryPlan, PodWatchSession,
    PodWatchSource, PodWatchStream,
};
use anyhow::Result;
use futures::StreamExt as _;
use klights_leader_api::{LeaderWatchError, WatchEventType};
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc;

type CriEventReceiver = mpsc::Receiver<crate::cri_events::KubeletEvent>;
type ManagerKubeletConfig = crate::context::KubeletConfig<
    crate::log_rotation::LogRotationPolicy,
    crate::node_capacity::NodeCapacity,
    crate::runtime_paths::KubeletRuntimePaths,
>;
type ManagerLifecycleServices = crate::context::LifecycleServices<
    crate::pod_lifecycle_router::PodLifecycleRouter,
    crate::context::PodLifecycleReceiver,
    crate::pod_creation_state::PodStartRetryTracker,
>;
type ManagerStatusDeliveryServices = crate::context::StatusDeliveryServices<crate::outbox::Outbox>;
type ManagerLocalExecutionServices = crate::context::LocalExecutionServices<
    dyn crate::runtime_clock::RuntimeClock,
    ManagerKubeletConfig,
>;
type PodWatchReconnectFuture = std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<PodWatchSession, LeaderWatchError>>
            + Send
            + 'static,
    >,
>;

mod event_handlers;
mod startup;

mod deadline_timers;
mod event_forwarder;

#[derive(Clone)]
pub struct PodWatcherRuntimePorts {
    cri_runtime: Arc<dyn crate::runtime::cri::CriRuntime>,
    container_control: Arc<dyn crate::runtime::cri::ContainerRuntimeControl>,
    cni_readiness: crate::cni_readiness::CniReadiness,
}

impl PodWatcherRuntimePorts {
    pub fn new(
        cri_runtime: Arc<dyn crate::runtime::cri::CriRuntime>,
        container_control: Arc<dyn crate::runtime::cri::ContainerRuntimeControl>,
        cni_readiness: crate::cni_readiness::CniReadiness,
    ) -> Self {
        Self {
            cri_runtime,
            container_control,
            cni_readiness,
        }
    }
}

#[derive(Clone)]
struct PodWatcherRuntimeContext {
    pod_watch_source: Arc<dyn PodWatchSource>,
    lifecycle: ManagerLifecycleServices,
    status_delivery: ManagerStatusDeliveryServices,
    local_execution: ManagerLocalExecutionServices,
    host_ip: crate::context::HostIpState,
    persistent_volume_event_handler: Arc<dyn PersistentVolumeEventHandler>,
    deadline_timers: deadline_timers::DeadlineTimerRegistry,
}

impl PodWatcherRuntimeContext {
    fn new(
        lifecycle: ManagerLifecycleServices,
        status_delivery: ManagerStatusDeliveryServices,
        local_execution: ManagerLocalExecutionServices,
        pod_watch_source: Arc<dyn PodWatchSource>,
        persistent_volume_event_handler: Arc<dyn PersistentVolumeEventHandler>,
    ) -> Self {
        Self {
            pod_watch_source,
            host_ip: lifecycle.host_ip.clone(),
            lifecycle,
            status_delivery,
            local_execution,
            persistent_volume_event_handler,
            deadline_timers: deadline_timers::DeadlineTimerRegistry::default(),
        }
    }
}

fn pod_watcher_node_field_selector(node_name: &str) -> String {
    format!("spec.nodeName={node_name}")
}

#[cfg(test)]
mod watch_topic_tests {
    use super::*;

    #[test]
    fn pod_watcher_node_selector_is_exact() {
        assert_eq!(
            pod_watcher_node_field_selector("worker-a"),
            "spec.nodeName=worker-a"
        );
    }
}

struct PodRecovery<'a> {
    pod_repo: Arc<dyn klights_pod_api::PodQuery>,
    node_name: &'a str,
    retry_state: &'a PodStartRetryTracker,
    pod_lifecycle_router: std::sync::Arc<crate::pod_lifecycle_router::PodLifecycleRouter>,
}
async fn spawn_cri_event_forwarder(
    cri: Arc<dyn crate::runtime::cri::CriRuntime>,
    cancel_token: tokio_util::sync::CancellationToken,
    task_supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
    lifecycle_tx: Option<
        tokio::sync::mpsc::Sender<crate::reconciler::cri_reconnect::CriStreamLifecycle>,
    >,
    wall_clock: Arc<dyn crate::runtime_clock::RuntimeClock>,
) -> CriEventReceiver {
    event_forwarder::spawn_cri_event_forwarder(
        cri,
        cancel_token,
        task_supervisor,
        lifecycle_tx,
        wall_clock,
    )
    .await
}

async fn wait_for_cni_readiness(
    readiness: crate::cni_readiness::CniReadiness,
    cancel_token: tokio_util::sync::CancellationToken,
) -> Result<()> {
    readiness.wait_ready(cancel_token).await
}

fn pod_watch_reconnect_future(
    source: Arc<dyn PodWatchSource>,
    node_name: String,
    recovery: PodWatchRecoveryPlan,
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    attempt: u32,
) -> PodWatchReconnectFuture {
    Box::pin(async move {
        if attempt > 0 {
            let _ = task_supervisor
                .sleep(
                    "pod_manager_watch_reconnect",
                    klights_supervisor::reconnect_backoff::delay(attempt - 1),
                )
                .await;
        }
        source.open_pod_manager_watch(node_name, recovery).await
    })
}

async fn next_pod_watch_event(
    stream: &mut Option<PodWatchStream>,
) -> Option<Result<PodWatchEvent, crate::pod_watch_source::PodWatchStreamError>> {
    match stream {
        Some(stream) => stream.next().await,
        None => std::future::pending().await,
    }
}

async fn await_pod_watch_reconnect(
    reconnect: &mut Option<PodWatchReconnectFuture>,
) -> Result<PodWatchSession, LeaderWatchError> {
    match reconnect {
        Some(reconnect) => reconnect.await,
        None => std::future::pending().await,
    }
}

/// Configuration for pod watcher
#[derive(Clone)]
pub struct PodWatcherConfig {
    pub service_cidr: String,
    pub node_name: String,
    pub containerd_namespace: String,
}

async fn rotate_all_pod_logs(
    file_process: &klights_supervisor::FileProcessExecutor,
    log_root: std::path::PathBuf,
    policy: crate::log_rotation::LogRotationPolicy,
) {
    let key = log_root.to_string_lossy().into_owned();
    let _ = file_process
        .run_blocking_file_keyed("podfs_rotate_logs", key, move || {
            rotate_logs_sync(&log_root, policy.max_size(), policy.max_files());
            Ok(())
        })
        .await;
}

fn rotate_logs_sync(root: &std::path::Path, max_size: u64, max_files: usize) {
    use crate::log_rotation::{RotationPlan, build_rotation_plan};

    let Ok(pod_dirs) = std::fs::read_dir(root) else {
        return;
    };
    for pod_dir in pod_dirs.flatten().filter(|entry| entry.path().is_dir()) {
        let Ok(container_dirs) = std::fs::read_dir(pod_dir.path()) else {
            continue;
        };
        for container_dir in container_dirs
            .flatten()
            .filter(|entry| entry.path().is_dir())
        {
            let log_file = container_dir.path().join("0.log");
            let Ok(metadata) = std::fs::metadata(&log_file) else {
                continue;
            };
            let Some(RotationPlan {
                remove_oldest,
                renames,
                current_to_one,
            }) = build_rotation_plan(&log_file, metadata.len(), max_size, max_files)
            else {
                continue;
            };
            let _ = std::fs::remove_file(remove_oldest);
            for (source, destination) in renames {
                let source: &std::path::Path = source.as_path();
                if source.exists()
                    && let Err(error) = std::fs::rename(source, &destination)
                {
                    tracing::warn!(
                        "Failed to rename {:?} -> {:?}: {error:#}",
                        source,
                        destination
                    );
                }
            }
            let (current, destination) = current_to_one;
            if let Err(error) = std::fs::rename(&current, &destination) {
                tracing::warn!(
                    "Failed to rotate {:?} -> {:?}: {error:#}",
                    current,
                    destination
                );
            }
        }
    }
}

#[cfg(test)]
mod pod_log_rotation_tests {
    use super::rotate_logs_sync;

    fn fixture_log(size: usize) -> (tempfile::TempDir, std::path::PathBuf) {
        let root = tempfile::tempdir().unwrap();
        let container = root.path().join("ns_pod_uid").join("container0");
        std::fs::create_dir_all(&container).unwrap();
        let log = container.join("0.log");
        std::fs::write(&log, vec![0_u8; size]).unwrap();
        (root, log)
    }

    #[test]
    fn test_rotate_logs_rotates_large_files() {
        let (root, log) = fixture_log(11 * 1024 * 1024);
        rotate_logs_sync(root.path(), 10 * 1024 * 1024, 5);
        assert!(!log.exists());
        assert!(log.with_file_name("0.1.log").exists());
    }

    #[test]
    fn test_rotate_logs_skips_under_threshold_files() {
        let (root, log) = fixture_log(1024);
        rotate_logs_sync(root.path(), 10 * 1024 * 1024, 5);
        assert!(log.exists());
        assert!(!log.with_file_name("0.1.log").exists());
    }

    #[test]
    fn test_rotate_logs_chain_deletes_oldest_and_renames_others() {
        let (root, log) = fixture_log(11 * 1024 * 1024);
        let container = log.parent().unwrap();
        for index in 1..=4 {
            std::fs::write(
                container.join(format!("0.{index}.log")),
                format!("log {index}"),
            )
            .unwrap();
        }
        rotate_logs_sync(root.path(), 10 * 1024 * 1024, 5);
        assert_eq!(
            std::fs::read_to_string(container.join("0.4.log")).unwrap(),
            "log 3"
        );
        assert!(!container.join("0.5.log").exists());
    }

    #[test]
    fn test_rotate_logs_handles_missing_log_file() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("ns_pod_uid/container0")).unwrap();
        rotate_logs_sync(root.path(), 10 * 1024 * 1024, 5);
        assert!(!root.path().join("ns_pod_uid/container0/0.1.log").exists());
    }
}

pub async fn run_pod_watcher_with_services(
    runtime_ports: PodWatcherRuntimePorts,
    lifecycle: ManagerLifecycleServices,
    status_delivery: ManagerStatusDeliveryServices,
    local_execution: ManagerLocalExecutionServices,
    pod_watch_source: Arc<dyn PodWatchSource>,
    persistent_volume_event_handler: Arc<dyn PersistentVolumeEventHandler>,
    cancel_token: tokio_util::sync::CancellationToken,
) {
    run_pod_watcher_with_runtime(
        runtime_ports,
        PodWatcherRuntimeContext::new(
            lifecycle,
            status_delivery,
            local_execution,
            pod_watch_source,
            persistent_volume_event_handler,
        ),
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
    let lifecycle = &state.lifecycle;
    let status_delivery = &state.status_delivery;
    let local_execution = &state.local_execution;
    let kubelet_config = &local_execution.config;

    // Compute and cache the host IP for pod status from the registered Node
    // InternalIP. Node names are Kubernetes identities, not DNS names.
    let node_ip = crate::node_ip::resolve_node_ip_from_leader_api_or_hostname(
        status_delivery.resource_query.as_ref(),
        kubelet_config.node_name(),
    )
    .await;
    state.host_ip.publish(node_ip.clone());
    tracing::info!("Host IP for pod status: {}", node_ip);

    let config = PodWatcherConfig {
        service_cidr: kubelet_config.service_cidr().to_string(),
        node_name: kubelet_config.node_name().to_string(),
        containerd_namespace: kubelet_config.containerd_namespace().to_string(),
    };
    let _service_cidr = config.service_cidr.as_str();

    let mut lifecycle_rx = lifecycle
        .pod_lifecycle_rx
        .lock()
        .await
        .take()
        .expect("pod lifecycle receiver must be set before run_pod_watcher");
    if let Err(err) =
        wait_for_cni_readiness(runtime_ports.cni_readiness.clone(), cancel_token.clone()).await
    {
        tracing::warn!("pod watcher exiting before CNI readiness: {err:#}");
        return;
    }

    let pod_creation_tracker: PodCreationTracker = Arc::new(Mutex::new(HashSet::new()));
    let pod_start_retry_state = lifecycle.pod_start_retry_state.clone();
    let pod_lifecycle_state = new_pod_lifecycle_state_tracker();
    let pod_lifecycle_router = lifecycle.pod_lifecycle_router.clone();

    let cri_reconnect_lifecycle_tx = {
        let reconciler = crate::reconciler::startup::StartupReconciler::new(
            config.node_name.clone(),
            kubelet_config.paths().clone(),
            crate::reconciler::startup::StartupDependencies {
                resource_query: status_delivery.resource_query.clone(),
                cache_readiness: status_delivery.cache_readiness.clone(),
                pod_cleanup_intents: status_delivery.pod_cleanup_intents.clone(),
                pod_runtime_store: local_execution.pod_runtime_store.clone(),
                pod_endpoint_store: local_execution.pod_endpoint_store.clone(),
                cri: cri_runtime.clone(),
                router: pod_lifecycle_router.clone(),
                file_process: local_execution.file_process.clone(),
            },
        );
        if let Err(err) = reconciler.run_once().await {
            tracing::warn!("startup reconciler failed: {err:#}");
        }
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let reconnect = std::sync::Arc::new(
            crate::reconciler::cri_reconnect::CriReconnectReconciler::new(
                config.node_name.clone(),
                crate::reconciler::cri_reconnect::CriReconnectDependencies {
                    resource_query: status_delivery.resource_query.clone(),
                    cache_readiness: status_delivery.cache_readiness.clone(),
                    pod_runtime_store: local_execution.pod_runtime_store.clone(),
                    pod_endpoint_store: local_execution.pod_endpoint_store.clone(),
                    cri: cri_runtime.clone(),
                    container_control: container_control.clone(),
                    router: pod_lifecycle_router.clone(),
                    task_supervisor: local_execution.task_supervisor.clone(),
                },
            ),
        );
        let reconnect_cancel = cancel_token.clone();
        if let Err(err) = local_execution
            .task_supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Background,
                "cri_reconnect_reconciler",
                async move {
                    reconnect.run_lifecycle_loop(rx, reconnect_cancel).await;
                },
            )
            .await
        {
            tracing::warn!("failed to spawn CRI reconnect reconciler: {err}");
        }
        Some(tx)
    };

    {
        let mut pod_recovery = PodRecovery::new(
            lifecycle.pod_query.clone(),
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
        local_execution.task_supervisor.clone(),
        cri_reconnect_lifecycle_tx,
        local_execution.wall_clock.clone(),
    )
    .await;

    // Supervised periodic trigger for log rotation (60 second interval).
    let (log_rotation_tick_tx, mut log_rotation_tick_rx) = mpsc::channel::<()>(4);
    // JUSTIFY: log rotation is a wall-clock cadence; container log size
    // has no underlying event source, and a "log size grew" trigger
    // would itself require polling.
    if let Err(err) = local_execution
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
    let mut pod_events = None;
    let mut pod_watch_checkpoint = PodWatchCheckpoint::default();
    let mut pod_watch_reconnect_attempt = 0_u32;
    let mut pod_watch_reconnect = Some(pod_watch_reconnect_future(
        state.pod_watch_source.clone(),
        config.node_name.clone(),
        PodWatchRecoveryPlan::initial(),
        local_execution.task_supervisor.clone(),
        0,
    ));
    loop {
        tokio::select! {
            // Handle cancellation signal
            _ = cancel_token.cancelled() => {
                tracing::info!("Pod watcher cancelled, shutting down");
                break;
            }

            reconnect_result = await_pod_watch_reconnect(&mut pod_watch_reconnect) => {
                pod_watch_reconnect = None;
                match reconnect_result {
                    Ok(session) => {
                        pod_watch_checkpoint = session.checkpoint;
                        pod_events = Some(session.stream);
                        pod_watch_reconnect_attempt = 0;
                        tracing::info!("Pod watcher positioned leader watch established");
                    }
                    Err(error) => {
                        tracing::warn!(%error, "pod watcher failed to open positioned leader watch");
                        pod_watch_reconnect_attempt = pod_watch_reconnect_attempt.saturating_add(1);
                        pod_watch_reconnect = Some(pod_watch_reconnect_future(
                            state.pod_watch_source.clone(),
                            config.node_name.clone(),
                            pod_watch_checkpoint.recovery_plan(PodWatchDisconnect::EndOfStream),
                            local_execution.task_supervisor.clone(),
                            pod_watch_reconnect_attempt,
                        ));
                    }
                }
            }

            // Handle watch events with positioned replay recovery. Reconnect
            // remains one arm of this same loop, so lifecycle and CRI receivers
            // stay owned and active while the watch transport is unavailable.
            event_result = next_pod_watch_event(&mut pod_events) => {
                let event = match event_result {
                    Some(Ok(event)) => event,
                    Some(Err(error)) if matches!(error.source, LeaderWatchError::ReplayExpired { .. }) => {
                        tracing::warn!(scope = ?error.scope, "Pod watcher positioned replay window expired; relisting scope");
                        pod_events = None;
                        pod_watch_reconnect_attempt = pod_watch_reconnect_attempt.saturating_add(1);
                        pod_watch_reconnect = Some(pod_watch_reconnect_future(
                            state.pod_watch_source.clone(),
                            config.node_name.clone(),
                            pod_watch_checkpoint.recovery_plan(
                                PodWatchDisconnect::ReplayExpired(error.scope),
                            ),
                            local_execution.task_supervisor.clone(),
                            pod_watch_reconnect_attempt,
                        ));
                        continue;
                    }
                    Some(Err(error)) => {
                        tracing::warn!(scope = ?error.scope, error = %error.source, "Pod watcher positioned leader watch failed");
                        pod_events = None;
                        pod_watch_reconnect_attempt = pod_watch_reconnect_attempt.saturating_add(1);
                        pod_watch_reconnect = Some(pod_watch_reconnect_future(
                            state.pod_watch_source.clone(),
                            config.node_name.clone(),
                            pod_watch_checkpoint.recovery_plan(
                                PodWatchDisconnect::Failed(error.scope),
                            ),
                            local_execution.task_supervisor.clone(),
                            pod_watch_reconnect_attempt,
                        ));
                        continue;
                    }
                    None => {
                        tracing::warn!("Pod watcher positioned leader watch closed; reconnecting");
                        pod_events = None;
                        pod_watch_reconnect_attempt = pod_watch_reconnect_attempt.saturating_add(1);
                        pod_watch_reconnect = Some(pod_watch_reconnect_future(
                            state.pod_watch_source.clone(),
                            config.node_name.clone(),
                            pod_watch_checkpoint.recovery_plan(PodWatchDisconnect::EndOfStream),
                            local_execution.task_supervisor.clone(),
                            pod_watch_reconnect_attempt,
                        ));
                        continue;
                    }
                };
                let event_scope = event.scope;
                let event_resource_version = event.resource_version().unwrap_or_default();
                let event_resume_position = event.resume_position;
                // Fire-and-forget lifecycle trace message: spawn through the
                // supervisor so actor sends never block event processing.
                // handle_watch_event must always run regardless of actor state.
                if let Some(message) = lifecycle_message_from_watch_event(&event) {
                    let _ = pod_lifecycle_router.route(message).await;
                }
                event_handlers::handle_watch_event(
                    event_handlers::WatchEventHandlerContext {
                        persistent_volume_event_handler: &state.persistent_volume_event_handler,
                        pod_cleanup_intents: &status_delivery.pod_cleanup_intents,
                        node_name: &config.node_name,
                        pod_workqueue: &lifecycle.pod_workqueue,
                        pod_query: lifecycle.pod_query.as_ref(),
                        pod_status_writer: lifecycle.pod_status_writer.as_ref(),
                        mutation_reconcile: lifecycle.pod_mutation_reconcile.as_ref(),
                        pod_creation_tracker: &pod_creation_tracker,
                        retry_state: &pod_start_retry_state,
                        pod_lifecycle_state: &pod_lifecycle_state,
                        pod_lifecycle_router: pod_lifecycle_router.clone(),
                        task_supervisor: local_execution.task_supervisor.clone(),
                        file_process: local_execution.file_process.clone(),
                        deadline_timers: state.deadline_timers.clone(),
                        now_unix_seconds: local_execution
                            .wall_clock
                            .now_ms()
                            .div_euclid(1_000),
                        node_capacity: kubelet_config.node_capacity(),
                        paths: kubelet_config.paths().clone(),
                    },
                    event,
                ).await;
                if let Err(error) = pod_watch_checkpoint.advance_after_apply(
                    event_scope,
                    event_resource_version,
                    event_resume_position,
                ) {
                    tracing::warn!(scope = ?event_scope, %error, "Pod watcher rejected out-of-order positioned event");
                    pod_events = None;
                    pod_watch_reconnect_attempt = pod_watch_reconnect_attempt.saturating_add(1);
                    pod_watch_reconnect = Some(pod_watch_reconnect_future(
                        state.pod_watch_source.clone(),
                        config.node_name.clone(),
                        pod_watch_checkpoint.recovery_plan(
                            PodWatchDisconnect::Failed(event_scope),
                        ),
                        local_execution.task_supervisor.clone(),
                        pod_watch_reconnect_attempt,
                    ));
                }
            }

            Some(ev) = cri_event_rx.recv() => {
                tracing::info!(
                    container_id = %&ev.container_id[..12.min(ev.container_id.len())],
                    kind = ev.kind.as_str(),
                    "CRI event received"
                );
                if let Some(key) = pod_lifecycle_key_for_cri_event(
                    container_control.as_ref(),
                    lifecycle.pod_query.as_ref(),
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
                if let Some(message) = lifecycle_message_from_command(cmd.clone()).await {
                    let _ = pod_lifecycle_router.route(message).await;
                }
            }

            // Handle log rotation timer tick
            Some(()) = log_rotation_tick_rx.recv() => {
                rotate_all_pod_logs(
                    &local_execution.file_process,
                    kubelet_config.paths().pod_logs_root(),
                    kubelet_config.log_rotation(),
                )
                .await;
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

fn lifecycle_message_from_watch_event(event: &PodWatchEvent) -> Option<LifecycleMessage> {
    if event.object.pointer("/kind").and_then(|kind| kind.as_str()) != Some("Pod") {
        return None;
    }

    let pod = event.object.as_ref();
    let key = pod_lifecycle_key_from_pod(pod)?;
    let resource_version = pod_resource_version(pod);
    let pod = pod.clone();
    match event.event_type {
        WatchEventType::Added => Some(LifecycleMessage::WatchAdded {
            key,
            resource_version,
            pod,
        }),
        WatchEventType::Modified => Some(LifecycleMessage::WatchModified {
            key,
            resource_version,
            pod,
        }),
        WatchEventType::Deleted => Some(LifecycleMessage::WatchDeleted {
            key,
            resource_version,
            pod,
        }),
        WatchEventType::Bookmark | WatchEventType::Error => None,
    }
}

async fn pod_lifecycle_key_for_pod_name(
    pod_repo: &dyn klights_pod_api::PodQuery,
    namespace: &str,
    pod_name: &str,
) -> Option<PodLifecycleKey> {
    use klights_pod_api::PodGetRequest;

    match pod_repo
        .get_pod(PodGetRequest::try_by_name(namespace, pod_name).ok()?)
        .await
    {
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
    container_control: &dyn crate::runtime::cri::ContainerRuntimeControl,
    pod_repo: &dyn klights_pod_api::PodQuery,
    event: &crate::cri_events::KubeletEvent,
) -> Option<PodLifecycleKey> {
    let resolved = match (
        event.pod_namespace.as_deref(),
        event.pod_name.as_deref(),
        event.pod_uid.as_deref(),
    ) {
        (Some(namespace), Some(name), Some(uid)) => {
            Some(klights_types::PodIdentity::new(namespace, name, uid))
        }
        _ => match container_control
            .pod_metadata_for_container(&event.container_id)
            .await
        {
            Ok(resolved) => resolved,
            Err(err) => {
                tracing::warn!(
                    container_id = %event.container_id,
                    "failed to resolve CRI event pod for lifecycle actor routing: {err:#}"
                );
                None
            }
        },
    };

    let Some(resolved) = resolved else {
        tracing::warn!(
            container_id = %event.container_id,
            kind = event.kind.as_str(),
            "CRI transition has no UID-qualified Pod identity; lifecycle reconciliation cannot be routed"
        );
        return None;
    };

    let live_key =
        pod_lifecycle_key_for_pod_name(pod_repo, &resolved.namespace, &resolved.name).await?;
    if live_key.uid != resolved.uid {
        tracing::warn!(
            namespace = %resolved.namespace,
            pod = %resolved.name,
            event_uid = %resolved.uid,
            live_uid = %live_key.uid,
            container_id = %event.container_id,
            "rejecting stale CRI transition for same-name replacement Pod"
        );
        return None;
    }

    Some(live_key)
}

fn lifecycle_command_target(command: &crate::lifecycle::LifecycleCommand) -> (&str, &str, &str) {
    command.target()
}

pub(crate) async fn lifecycle_message_from_command(
    command: crate::lifecycle::LifecycleCommand,
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

async fn schedule_active_deadline_timer_for_modified_pod(
    pod: &serde_json::Value,
    now_unix_seconds: i64,
    registry: deadline_timers::DeadlineTimerRegistry,
    task_supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
    pod_lifecycle_router: std::sync::Arc<crate::pod_lifecycle_router::PodLifecycleRouter>,
) {
    deadline_timers::schedule_active_deadline_timer_for_modified_pod(
        pod,
        now_unix_seconds,
        registry,
        task_supervisor,
        pod_lifecycle_router,
    )
    .await
}
#[cfg(test)]
mod cri_event_identity_tests {
    use std::sync::Arc;

    use super::*;
    use crate::runtime::cri::{ContainerRuntimeControl, ContainerRuntimeState};
    use klights_types::PodIdentity;

    struct LivePodQuery(klights_cluster_core::Resource);

    impl klights_pod_api::PodQuery for LivePodQuery {
        fn get_pod(
            &self,
            request: klights_pod_api::PodGetRequest,
        ) -> klights_pod_api::PodRepositoryFuture<'_, Option<klights_cluster_core::Resource>>
        {
            let matches = request.namespace() == self.0.namespace.as_deref().unwrap_or("default")
                && request.name() == self.0.name;
            Box::pin(async move { Ok(matches.then(|| self.0.clone())) })
        }

        fn list_pods(
            &self,
            _request: klights_pod_api::PodListRequest,
        ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodListResult> {
            Box::pin(async { klights_pod_api::PodListResult::try_new(Vec::new(), 0, None, None) })
        }

        fn list_pods_by_owner_uid(
            &self,
            _request: klights_pod_api::PodOwnerListRequest,
        ) -> klights_pod_api::PodRepositoryFuture<'_, Vec<klights_cluster_core::Resource>> {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    struct ContainerIdentity(Option<PodIdentity>);

    #[async_trait::async_trait]
    impl ContainerRuntimeControl for ContainerIdentity {
        async fn list_containers(
            &self,
            _sandbox_id_filter: Option<&str>,
        ) -> anyhow::Result<Vec<(String, ContainerRuntimeState)>> {
            Ok(Vec::new())
        }

        async fn pod_metadata_for_container(
            &self,
            _container_id: &str,
        ) -> anyhow::Result<Option<PodIdentity>> {
            Ok(self.0.clone())
        }
    }

    fn live_pod(uid: &str) -> LivePodQuery {
        LivePodQuery(
            klights_cluster_core::Resource::try_from_data(Arc::new(serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "workloads",
                    "name": "same-name",
                    "uid": uid,
                    "resourceVersion": "7"
                }
            })))
            .expect("valid Pod resource"),
        )
    }

    fn stopped_event(uid: Option<&str>) -> crate::cri_events::KubeletEvent {
        crate::cri_events::KubeletEvent {
            kind: crate::cri_events::KubeletEventKind::Stopped,
            container_id: "gone-container".into(),
            pod_namespace: Some("workloads".into()),
            pod_name: Some("same-name".into()),
            pod_uid: uid.map(str::to_string),
            pod_sandbox_id: None,
            timestamp_ns: 1_777_000_123,
        }
    }

    #[tokio::test]
    async fn carried_uid_routes_when_container_inventory_is_missing() {
        let key = pod_lifecycle_key_for_cri_event(
            &ContainerIdentity(None),
            &live_pod("carried-uid"),
            &stopped_event(Some("carried-uid")),
        )
        .await
        .expect("carried identity routes without container inventory");

        assert_eq!(
            key,
            PodLifecycleKey::new("workloads", "same-name", "carried-uid")
        );
    }

    #[tokio::test]
    async fn stale_carried_uid_cannot_route_to_same_name_replacement() {
        let key = pod_lifecycle_key_for_cri_event(
            &ContainerIdentity(None),
            &live_pod("replacement-uid"),
            &stopped_event(Some("deleted-uid")),
        )
        .await;

        assert!(
            key.is_none(),
            "stale event must not reach replacement actor"
        );
    }

    #[tokio::test]
    async fn typed_inventory_uid_routes_only_matching_live_pod() {
        let mut event = stopped_event(None);
        event.pod_namespace = None;
        event.pod_name = None;
        let identity = PodIdentity::new("workloads", "same-name", "inventory-uid");

        let key = pod_lifecycle_key_for_cri_event(
            &ContainerIdentity(Some(identity)),
            &live_pod("inventory-uid"),
            &event,
        )
        .await
        .expect("typed inventory identity routes matching live pod");

        assert_eq!(key.uid, "inventory-uid");
    }
}

#[cfg(test)]
mod failure_reason_tests;

#[cfg(test)]
mod tests;
