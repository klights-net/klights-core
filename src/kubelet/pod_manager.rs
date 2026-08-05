#[cfg(test)]
use crate::kubelet::pod_watch_handlers::NoopPersistentVolumeEventHandler;
use crate::kubelet::pod_watch_handlers::PersistentVolumeEventHandler;
use crate::kubelet::pod_watch_source::{
    PodWatchCheckpoint, PodWatchDisconnect, PodWatchEvent, PodWatchRecoveryPlan, PodWatchSession,
    PodWatchSource, PodWatchStream,
};
use anyhow::Result;
#[cfg(test)]
use event_handlers::{PodPhaseUpdateRequest, apply_pod_phase_update};
use futures::StreamExt as _;
#[cfg(test)]
use klights_kubelet::pod_creation_state::PodStartRetryState;
#[cfg(test)]
use klights_kubelet::pod_creation_state::PodStartSource;
use klights_kubelet::pod_creation_state::{
    PodCreationTracker, PodStartRetryTracker, clear_pod_creation_inflight,
    should_clear_pod_creation_inflight,
};
use klights_kubelet::pod_lifecycle_actor::message::{LifecycleMessage, PodLifecycleKey};
use klights_kubelet::pod_lifecycle_actor::state::{
    PodLifecycleStateTracker, new_pod_lifecycle_state_tracker,
};
#[cfg(test)]
use klights_kubelet::pod_runtime_state::{PodRuntimeState, StartupDecision, decide_startup_action};
#[cfg(test)]
use klights_kubelet::pod_status_builders::{
    build_container_statuses, build_creation_error_statuses, build_failed_init_container_statuses,
    cri_timestamp_from_ns,
};
#[cfg(test)]
use klights_kubelet::pod_status_logic::{ContainerInfo, compute_pod_phase, should_restart};
use klights_leader_api::{LeaderWatchError, WatchEventType};
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc;

type CriEventReceiver = mpsc::Receiver<klights_kubelet::cri_events::KubeletEvent>;
type PodWatchReconnectFuture = std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<PodWatchSession, LeaderWatchError>>
            + Send
            + 'static,
    >,
>;

pub mod event_handlers;
mod startup;

mod deadline_timers;
mod event_forwarder;

#[derive(Clone)]
pub struct PodWatcherRuntimePorts {
    cri_runtime: Arc<dyn klights_kubelet::runtime::cri::CriRuntime>,
    container_control: Arc<dyn klights_kubelet::runtime::cri::ContainerRuntimeControl>,
    cni_readiness: klights_kubelet::cni_readiness::CniReadiness,
}

impl PodWatcherRuntimePorts {
    pub fn new(
        cri_runtime: Arc<dyn klights_kubelet::runtime::cri::CriRuntime>,
        container_control: Arc<dyn klights_kubelet::runtime::cri::ContainerRuntimeControl>,
        cni_readiness: klights_kubelet::cni_readiness::CniReadiness,
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
    lifecycle: crate::kubelet::context::KubeletLifecycleServices,
    status_delivery: crate::kubelet::context::KubeletStatusDeliveryServices,
    local_execution: crate::kubelet::context::KubeletLocalExecutionServices,
    host_ip: crate::kubelet::context::HostIpState,
    persistent_volume_event_handler: Arc<dyn PersistentVolumeEventHandler>,
    deadline_timers: deadline_timers::DeadlineTimerRegistry,
}

impl PodWatcherRuntimeContext {
    fn new(
        lifecycle: crate::kubelet::context::KubeletLifecycleServices,
        status_delivery: crate::kubelet::context::KubeletStatusDeliveryServices,
        local_execution: crate::kubelet::context::KubeletLocalExecutionServices,
        pod_watch_source: Arc<dyn PodWatchSource>,
        persistent_volume_event_handler: Arc<dyn PersistentVolumeEventHandler>,
    ) -> Self {
        Self {
            pod_watch_source,
            host_ip: lifecycle.pod_repository.host_ip_state(),
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
    pod_repo: &'a Arc<crate::kubelet::pod_repository::PodRepository>,
    node_name: &'a str,
    retry_state: &'a PodStartRetryTracker,
    pod_lifecycle_router: std::sync::Arc<klights_kubelet::pod_lifecycle_router::PodLifecycleRouter>,
}
async fn spawn_cri_event_forwarder(
    cri: Arc<dyn klights_kubelet::runtime::cri::CriRuntime>,
    cancel_token: tokio_util::sync::CancellationToken,
    task_supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
    lifecycle_tx: Option<
        tokio::sync::mpsc::Sender<crate::kubelet::reconciler::cri_reconnect::CriStreamLifecycle>,
    >,
    wall_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
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
    readiness: klights_kubelet::cni_readiness::CniReadiness,
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
) -> Option<Result<PodWatchEvent, crate::kubelet::pod_watch_source::PodWatchStreamError>> {
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
    policy: klights_kubelet::log_rotation::LogRotationPolicy,
) {
    crate::kubelet::pod_fs::PodFs::rotate_logs(
        file_process,
        log_root,
        policy.max_size(),
        policy.max_files(),
    )
    .await;
}

pub(crate) async fn run_pod_watcher_with_services(
    runtime_ports: PodWatcherRuntimePorts,
    lifecycle: crate::kubelet::context::KubeletLifecycleServices,
    status_delivery: crate::kubelet::context::KubeletStatusDeliveryServices,
    local_execution: crate::kubelet::context::KubeletLocalExecutionServices,
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
    let node_ip = klights_kubelet::node_ip::resolve_node_ip_from_leader_api_or_hostname(
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
        let reconciler = crate::kubelet::reconciler::startup::StartupReconciler::new(
            config.node_name.clone(),
            kubelet_config.paths().clone(),
            crate::kubelet::reconciler::startup::StartupDependencies {
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
            crate::kubelet::reconciler::cri_reconnect::CriReconnectReconciler::new(
                config.node_name.clone(),
                crate::kubelet::reconciler::cri_reconnect::CriReconnectDependencies {
                    resource_query: status_delivery.resource_query.clone(),
                    cache_readiness: status_delivery.cache_readiness.clone(),
                    pod_runtime_store: local_execution.pod_runtime_store.clone(),
                    pod_endpoint_store: local_execution.pod_endpoint_store.clone(),
                    cri: cri_runtime.clone(),
                    container_control: container_control.clone(),
                    router: pod_lifecycle_router.clone(),
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
            &lifecycle.pod_repository,
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
                        pod_repo: &lifecycle.pod_repository,
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
                    &lifecycle.pod_repository,
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
                if let Some(message) = lifecycle_message_from_command(&lifecycle.pod_repository, cmd.clone()).await {
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
    container_control: &dyn klights_kubelet::runtime::cri::ContainerRuntimeControl,
    pod_repo: &Arc<crate::kubelet::pod_repository::PodRepository>,
    event: &klights_kubelet::cri_events::KubeletEvent,
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
    command: &klights_kubelet::lifecycle::LifecycleCommand,
) -> (&str, &str, &str) {
    command.target()
}

pub(crate) async fn lifecycle_message_from_command(
    _pod_repo: &Arc<crate::kubelet::pod_repository::PodRepository>,
    command: klights_kubelet::lifecycle::LifecycleCommand,
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
    parse_deadline_timer_delay_secs_at(pod, chrono::Utc::now().timestamp())
}
#[cfg(test)]
fn parse_deadline_timer_delay_secs_at(
    pod: &serde_json::Value,
    now_unix_seconds: i64,
) -> Option<(String, String, u64, String)> {
    deadline_timers::parse_deadline_timer_delay_secs_at(pod, now_unix_seconds)
}
async fn schedule_active_deadline_timer_for_modified_pod(
    pod: &serde_json::Value,
    now_unix_seconds: i64,
    registry: deadline_timers::DeadlineTimerRegistry,
    task_supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
    pod_lifecycle_router: std::sync::Arc<klights_kubelet::pod_lifecycle_router::PodLifecycleRouter>,
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
    pod_resource: &klights_cluster_core::Resource,
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
            "startedAt": cri_timestamp_from_ns(info.started_at, chrono::DateTime::UNIX_EPOCH),
            "finishedAt": cri_timestamp_from_ns(info.finished_at, chrono::DateTime::UNIX_EPOCH),
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
