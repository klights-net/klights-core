use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::kubelet::pod_lifecycle_core::action::PodAction;
use crate::kubelet::pod_lifecycle_core::message::{
    LifecycleMessage, PodLifecycleKey, PodLifecycleWorkFailure, PodLifecycleWorkKind, PodSlotKey,
};
use crate::kubelet::pod_lifecycle_core::state::{FinalizationAction, PodPhase};
use crate::kubelet::pod_lifecycle_core::trace::{LifecycleTraceEntry, LifecycleTraceRing};
use crate::kubelet::pod_lifecycle_router::LifecycleReplyHandle;
use crate::kubelet::pod_lifecycle_router::executor::PodWorkExecutor;
#[cfg(test)]
use crate::task_supervisor::TaskCategoryConfig;
use crate::task_supervisor::{SupervisedJoinHandle, TaskSupervisor};

use super::registry::{
    ActorInstanceToken, PodLifecycleActorRemovalHandle, PodLifecycleActorStateEntry,
};
use super::state::PodLifecycleState;

const DEFAULT_POD_ACTOR_IDLE_GRACE_SECS: u64 = 30;
const KLIGHTS_POD_ACTOR_IDLE_GRACE_SECS: &str = "KLIGHTS_POD_ACTOR_IDLE_GRACE_SECS";

pub fn pod_actor_idle_grace_duration() -> Duration {
    std::env::var(KLIGHTS_POD_ACTOR_IDLE_GRACE_SECS)
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(DEFAULT_POD_ACTOR_IDLE_GRACE_SECS))
}

pub type PodLifecycleActorStateMap = Arc<Mutex<HashMap<PodSlotKey, PodLifecycleActorStateEntry>>>;

pub struct PodLifecycleActorRuntime {
    pub slot: PodSlotKey,
    pub trace: Arc<Mutex<LifecycleTraceRing>>,
    pub actor_state: PodLifecycleActorStateMap,
    pub supervisor: Arc<TaskSupervisor>,
    pub executor_holder: Arc<std::sync::Mutex<Arc<dyn PodWorkExecutor>>>,
    pub reply_handle: LifecycleReplyHandle,
    pub self_removal: PodLifecycleActorRemovalHandle,
    pub shutdown_token: CancellationToken,
    pub instance: ActorInstanceToken,
    pub idle_grace: Duration,
}

fn pod_has_ephemeral_containers(pod: &serde_json::Value) -> bool {
    pod.pointer("/spec/ephemeralContainers")
        .and_then(|v| v.as_array())
        .is_some_and(|containers| !containers.is_empty())
}

fn pod_has_published_startup_runtime_state(pod: &serde_json::Value) -> bool {
    let is_running = pod.pointer("/status/phase").and_then(|v| v.as_str()) == Some("Running");
    if !is_running {
        return false;
    }

    pod.pointer("/status/podIP")
        .and_then(|v| v.as_str())
        .is_some_and(|ip| !ip.trim().is_empty())
        || pod
            .pointer("/status/podIPs/0/ip")
            .and_then(|v| v.as_str())
            .is_some_and(|ip| !ip.trim().is_empty())
}

fn pod_is_node_lost_terminal(pod: &serde_json::Value) -> bool {
    pod.pointer("/status/phase").and_then(|v| v.as_str()) == Some("Failed")
        && pod.pointer("/status/reason").and_then(|v| v.as_str()) == Some("NodeLost")
}

fn pod_running_sandbox_id_for_finalization(pod: &serde_json::Value) -> Option<String> {
    if !pod_has_published_startup_runtime_state(pod) {
        return None;
    }

    pod.pointer("/metadata/annotations/klights.dev~1sandbox-id")
        .and_then(|v| v.as_str())
        .filter(|sandbox_id| !sandbox_id.trim().is_empty())
        .map(str::to_string)
}

fn pod_pending_with_published_runtime_state(pod: &serde_json::Value) -> bool {
    let is_pending = pod.pointer("/status/phase").and_then(|v| v.as_str()) == Some("Pending");
    if !is_pending {
        return false;
    }

    let has_pod_ip = pod
        .pointer("/status/podIP")
        .and_then(|v| v.as_str())
        .is_some_and(|ip| !ip.trim().is_empty());
    if !has_pod_ip {
        return false;
    }

    pod.pointer("/status/containerStatuses")
        .and_then(|v| v.as_array())
        .is_some_and(|statuses| {
            !statuses.is_empty()
                && statuses.iter().all(|status| {
                    status.pointer("/state/running").is_some()
                        && status.get("started").and_then(|v| v.as_bool()) != Some(false)
                })
        })
}

fn pod_create_container_config_error_fingerprint(pod: &serde_json::Value) -> Option<String> {
    let is_pending = pod.pointer("/status/phase").and_then(|v| v.as_str()) == Some("Pending");
    if !is_pending {
        return None;
    }

    let has_config_error = pod
        .pointer("/status/containerStatuses")
        .and_then(|v| v.as_array())
        .is_some_and(|statuses| {
            statuses.iter().any(|status| {
                status
                    .pointer("/state/waiting/reason")
                    .and_then(|v| v.as_str())
                    == Some("CreateContainerConfigError")
            })
        });
    if !has_config_error {
        return None;
    }

    Some(
        serde_json::json!({
            "annotations": pod.pointer("/metadata/annotations").cloned().unwrap_or_default(),
            "labels": pod.pointer("/metadata/labels").cloned().unwrap_or_default(),
            "containers": pod.pointer("/spec/containers").cloned().unwrap_or_default(),
            "initContainers": pod.pointer("/spec/initContainers").cloned().unwrap_or_default(),
            "volumes": pod.pointer("/spec/volumes").cloned().unwrap_or_default(),
        })
        .to_string(),
    )
}

pub struct PodLifecycleActor {
    slot: Option<PodSlotKey>,
    state: PodLifecycleState,
    restart_reconciled: bool,
    pending_restart_admission: Option<LifecycleMessage>,
    last_config_error_retry_fingerprint: Option<String>,
    trace: std::sync::Arc<Mutex<LifecycleTraceRing>>,
    actor_state: Option<PodLifecycleActorStateMap>,
    supervisor: Arc<TaskSupervisor>,
    executor_holder: Arc<std::sync::Mutex<Arc<dyn PodWorkExecutor>>>,
    _reply_handle: LifecycleReplyHandle,
    self_removal: Option<PodLifecycleActorRemovalHandle>,
    shutdown_token: CancellationToken,
    instance: Option<ActorInstanceToken>,
    idle_grace: Duration,
    slot_admission_gate_enabled: bool,
    idle_generation: u64,
    idle_timer_armed_generation: Option<u64>,
    last_key: Option<PodLifecycleKey>,
    #[cfg(test)]
    event_sink: Option<mpsc::Sender<&'static str>>,
    #[cfg(test)]
    uid_mismatch_warnings: Vec<(&'static str, String, String)>,
    #[cfg(test)]
    fail_next_spawn: bool,
}

impl PodLifecycleActor {
    #[cfg(test)]
    pub fn new_with_event_sink_for_test(
        trace_capacity: usize,
        event_sink: mpsc::Sender<&'static str>,
        executor_holder: Arc<std::sync::Mutex<Arc<dyn PodWorkExecutor>>>,
        reply_handle: LifecycleReplyHandle,
    ) -> Self {
        Self {
            slot: None,
            state: PodLifecycleState::new(),
            restart_reconciled: true,
            pending_restart_admission: None,
            last_config_error_retry_fingerprint: None,
            trace: std::sync::Arc::new(Mutex::new(LifecycleTraceRing::new(trace_capacity))),
            actor_state: None,
            supervisor: Arc::new(TaskSupervisor::new(TaskCategoryConfig::default())),
            executor_holder,
            _reply_handle: reply_handle,
            self_removal: None,
            shutdown_token: CancellationToken::new(),
            instance: None,
            idle_grace: pod_actor_idle_grace_duration(),
            slot_admission_gate_enabled: false,
            idle_generation: 0,
            idle_timer_armed_generation: None,
            last_key: None,
            event_sink: Some(event_sink),
            uid_mismatch_warnings: Vec::new(),
            fail_next_spawn: false,
        }
    }

    pub fn new_with_shared_trace_and_state(runtime: PodLifecycleActorRuntime) -> Self {
        Self {
            slot: Some(runtime.slot),
            state: PodLifecycleState::new(),
            restart_reconciled: false,
            pending_restart_admission: None,
            last_config_error_retry_fingerprint: None,
            trace: runtime.trace,
            actor_state: Some(runtime.actor_state),
            supervisor: runtime.supervisor,
            executor_holder: runtime.executor_holder,
            _reply_handle: runtime.reply_handle,
            self_removal: Some(runtime.self_removal),
            shutdown_token: runtime.shutdown_token,
            instance: Some(runtime.instance),
            idle_grace: runtime.idle_grace,
            slot_admission_gate_enabled: true,
            idle_generation: 0,
            idle_timer_armed_generation: None,
            last_key: None,
            #[cfg(test)]
            event_sink: None,
            #[cfg(test)]
            uid_mismatch_warnings: Vec::new(),
            #[cfg(test)]
            fail_next_spawn: false,
        }
    }

    pub async fn run(mut self, mut rx: mpsc::Receiver<LifecycleMessage>) {
        let (completion_tx, mut completion_rx) = mpsc::channel::<LifecycleMessage>(256);
        let shutdown = self.shutdown_token.clone();
        let mut spawned_work = Vec::<SupervisedJoinHandle<()>>::new();

        loop {
            tokio::select! {
                message = rx.recv() => {
                    let Some(message) = message else {
                        break;
                    };
                    let idle_generation = message.idle_grace_generation();
                    if let Some(handle) = self.process_message(message, &completion_tx).await {
                        spawned_work.push(handle);
                    }
                    if self.after_message_processed(
                        idle_generation,
                        rx.is_empty(),
                        completion_rx.is_empty(),
                        &completion_tx,
                    ).await {
                        break;
                    }
                }
                completion = completion_rx.recv() => {
                    let Some(completion) = completion else {
                        break;
                    };
                    let idle_generation = completion.idle_grace_generation();
                    if let Some(handle) = self.process_message(completion, &completion_tx).await {
                        spawned_work.push(handle);
                    }
                    if self.after_message_processed(
                        idle_generation,
                        rx.is_empty(),
                        completion_rx.is_empty(),
                        &completion_tx,
                    ).await {
                        break;
                    }
                }
                _ = shutdown.cancelled() => {
                    self.state.cancel_in_flight();
                    for handle in &spawned_work {
                        handle.abort();
                    }
                    return;
                }
            }
        }

        for handle in spawned_work {
            let _ = handle.join().await;
        }
    }

    fn mailbox_is_empty(actor_empty: bool, completion_empty: bool) -> bool {
        actor_empty && completion_empty
    }

    fn idle_quiescent(&self, actor_empty: bool, completion_empty: bool) -> bool {
        self.self_removal.is_some()
            && self.state.phase == PodPhase::Terminated
            && self.state.active_uid.is_none()
            && self.state.pending_replacement.is_none()
            && self.state.pending_start_pod.is_none()
            && !self.state.slot_admission_waiting
            && self.state.in_flight.is_none()
            && Self::mailbox_is_empty(actor_empty, completion_empty)
    }

    fn invalidate_idle_timer(&mut self) {
        self.idle_generation = self.idle_generation.wrapping_add(1);
        self.idle_timer_armed_generation = None;
    }

    async fn arm_idle_removal_timer(&mut self, completion_tx: &mpsc::Sender<LifecycleMessage>) {
        if self.idle_timer_armed_generation.is_some() {
            return;
        }
        let Some(key) = self.last_key.clone() else {
            return;
        };

        self.idle_generation = self.idle_generation.wrapping_add(1);
        let generation = self.idle_generation;
        self.idle_timer_armed_generation = Some(generation);
        let tx = completion_tx.clone();
        let delay = self.idle_grace;
        if let Err(err) = self
            .supervisor
            .spawn_delay("pod_lifecycle_actor_idle_grace", delay, async move {
                let _ = tx
                    .send(LifecycleMessage::ActorIdleGraceExpired { key, generation })
                    .await;
            })
            .await
        {
            self.idle_timer_armed_generation = None;
            tracing::warn!("failed to arm pod lifecycle actor idle grace timer: {err:#}");
        }
    }

    async fn try_remove_self_if_idle(&self) -> bool {
        let (Some(self_removal), Some(slot), Some(instance)) =
            (&self.self_removal, &self.slot, &self.instance)
        else {
            return false;
        };
        self_removal.try_remove_if_idle(slot, instance).await
    }

    async fn after_message_processed(
        &mut self,
        idle_generation: Option<u64>,
        actor_empty: bool,
        completion_empty: bool,
        completion_tx: &mpsc::Sender<LifecycleMessage>,
    ) -> bool {
        if let Some(generation) = idle_generation {
            if self.idle_timer_armed_generation == Some(generation) {
                self.idle_timer_armed_generation = None;
                if self.idle_quiescent(actor_empty, completion_empty)
                    && self.try_remove_self_if_idle().await
                {
                    return true;
                }
            }
        } else if !self.idle_quiescent(actor_empty, completion_empty) {
            self.invalidate_idle_timer();
            return false;
        }

        if self.idle_quiescent(actor_empty, completion_empty) {
            self.arm_idle_removal_timer(completion_tx).await;
        }
        false
    }

    async fn process_message(
        &mut self,
        message: LifecycleMessage,
        completion_tx: &mpsc::Sender<LifecycleMessage>,
    ) -> Option<SupervisedJoinHandle<()>> {
        let key = message.key().clone();
        if self.slot.is_none() {
            self.slot = Some(PodSlotKey::from(&key));
        }
        self.last_key = Some(key.clone());
        let event_name = message.event_name();
        if let Some(actor_state) = &self.actor_state {
            actor_state.lock().await.insert(
                super::message::PodSlotKey::from(&key),
                PodLifecycleActorStateEntry {
                    uid: key.uid.clone(),
                    state: event_name.to_string(),
                },
            );
        }
        {
            let mut trace = self.trace.lock().await;
            trace.record(LifecycleTraceEntry::new(
                key,
                message.event_name(),
                message.resource_version(),
                message.sandbox_id_hint(),
                "received",
            ));
        }

        let action = self.handle(message);
        self.dispatch_action(action, completion_tx).await
    }

    async fn dispatch_action(
        &mut self,
        action: PodAction,
        completion_tx: &mpsc::Sender<LifecycleMessage>,
    ) -> Option<SupervisedJoinHandle<()>> {
        if action.is_noop() {
            return None;
        }

        let expected = action
            .key()
            .cloned()
            .zip(action.operation_id())
            .zip(action.expected_completion())
            .map(|((key, operation_id), kind)| (key, operation_id, kind));
        let cancel = expected
            .as_ref()
            .and_then(|(key, operation_id, kind)| {
                self.state
                    .in_flight
                    .as_ref()
                    .filter(|work| {
                        work.uid == key.uid
                            && work.operation_id == *operation_id
                            && work.kind == *kind
                    })
                    .map(|work| work.cancel.clone())
            })
            .unwrap_or_else(CancellationToken::new);
        let synth = action.failure_synthesizer();
        let synth_for_task = synth.clone();
        let category = action.task_category();
        let task_name = action.task_name();
        let executor = self.executor_holder.lock().unwrap().clone();
        let reply = LifecycleReplyHandle::direct(completion_tx.clone());
        let task_completion_tx = completion_tx.clone();

        #[cfg(test)]
        if self.fail_next_spawn {
            self.fail_next_spawn = false;
            if let Some((key, operation_id, kind)) = expected.as_ref() {
                self.state
                    .complete_matching_work(&key.uid, *operation_id, *kind);
            }
            if let Some(synth) = synth.as_ref() {
                let _ = completion_tx
                    .send(synth(anyhow::anyhow!(
                        "injected pod lifecycle work spawn failure"
                    )))
                    .await;
            }
            return None;
        }

        match self
            .supervisor
            .spawn_async(category, task_name, async move {
                if let Err(err) = executor.dispatch_with_cancel(action, reply, cancel).await
                    && let Some(synth) = synth_for_task
                {
                    let _ = task_completion_tx.send(synth(err)).await;
                }
            })
            .await
        {
            Ok(handle) => Some(handle),
            Err(err) => {
                if let Some((key, operation_id, kind)) = expected {
                    self.state
                        .complete_matching_work(&key.uid, operation_id, kind);
                }
                if let Some(synth) = synth {
                    let _ = completion_tx.send(synth(err)).await;
                }
                None
            }
        }
    }

    fn active_uid_is(&self, key: &PodLifecycleKey) -> bool {
        self.state.active_uid_matches(&key.uid)
    }

    fn ensure_active_uid(&mut self, key: &PodLifecycleKey) {
        if self.state.active_uid.is_none() {
            self.state.admit_uid(&key.uid);
        }
    }

    fn admit_replacement_uid(&mut self, key: &PodLifecycleKey) {
        self.state.cancel_in_flight();
        self.state.admit_uid(&key.uid);
        self.last_config_error_retry_fingerprint = None;
    }

    fn next_work_operation(&mut self, key: &PodLifecycleKey, kind: PodLifecycleWorkKind) -> u64 {
        let operation_id = self.state.next_operation_id();
        self.state
            .record_in_flight(key.uid.clone(), kind, operation_id);
        operation_id
    }

    fn start_pod_action(
        &mut self,
        key: PodLifecycleKey,
        pod: Option<serde_json::Value>,
    ) -> PodAction {
        let operation_id = self.next_work_operation(&key, PodLifecycleWorkKind::StartPod);
        PodAction::StartPod {
            key,
            pod,
            operation_id,
            permit: None,
        }
    }

    fn check_slot_admission_action(
        &mut self,
        key: PodLifecycleKey,
        pod: serde_json::Value,
        resource_version: Option<i64>,
        start_after_admit: bool,
    ) -> PodAction {
        self.state.update_pending_start_pod_if_newer(
            key.clone(),
            pod.clone(),
            resource_version,
            start_after_admit,
        );
        let operation_id = self.next_work_operation(&key, PodLifecycleWorkKind::CheckSlotAdmission);
        PodAction::CheckSlotAdmission {
            key,
            pod,
            resource_version,
            start_after_admit,
            operation_id,
            permit: None,
        }
    }

    fn stop_pod_action(
        &mut self,
        key: PodLifecycleKey,
        pod: Option<serde_json::Value>,
        sandbox_id: String,
    ) -> PodAction {
        self.state.pending_stop_pod =
            Some(crate::kubelet::pod_lifecycle_core::state::PendingStopPod {
                key: key.clone(),
                pod: pod.clone(),
                sandbox_id: sandbox_id.clone(),
            });
        let operation_id = self.next_work_operation(&key, PodLifecycleWorkKind::StopPod);
        PodAction::StopPod {
            key,
            pod,
            sandbox_id,
            operation_id,
            permit: None,
        }
    }

    fn finalize_pod_deletion_action(&mut self, key: PodLifecycleKey) -> PodAction {
        let operation_id =
            self.next_work_operation(&key, PodLifecycleWorkKind::FinalizePodDeletion);
        PodAction::FinalizePodDeletion {
            key,
            operation_id,
            permit: None,
        }
    }

    fn stop_pod_completed_action(&mut self, key: PodLifecycleKey) -> PodAction {
        self.state.cancel_in_flight();
        self.state.pending_stop_pod = None;
        self.state.drop_pending_ephemeral_reconcile_if_uid(&key.uid);
        self.state.phase = PodPhase::Terminated;
        self.state.active_uid = None;
        self.state.admitted_slot_uid = None;
        self.state.active_sandbox_id = None;
        self.finalize_pod_deletion_action(key)
    }

    fn finalize_startup_action(
        &mut self,
        key: PodLifecycleKey,
        pod: Option<serde_json::Value>,
        sandbox_id: String,
    ) -> PodAction {
        let operation_id = self.next_work_operation(&key, PodLifecycleWorkKind::FinalizeStartup);
        PodAction::FinalizeStartup {
            key,
            pod,
            sandbox_id,
            operation_id,
            permit: None,
        }
    }

    fn reconcile_runtime_action(
        &mut self,
        key: PodLifecycleKey,
        hint: crate::kubelet::pod_runtime::service::RuntimeReconcileHint,
    ) -> PodAction {
        let operation_id = self.next_work_operation(&key, PodLifecycleWorkKind::ReconcileRuntime);
        tracing::info!(
            namespace = %key.namespace, pod = %key.name, uid = %key.uid,
            operation_id, phase = ?self.state.phase,
            hint_container_id = ?hint.container_id,
            "lifecycle-actor: dispatching ReconcileRuntime"
        );
        PodAction::ReconcileRuntime {
            key,
            hint,
            operation_id,
            permit: None,
        }
    }

    fn runtime_reconcile_action_or_defer(
        &mut self,
        key: PodLifecycleKey,
        container_id: Option<&str>,
    ) -> PodAction {
        let in_flight = self.state.in_flight_kind_for_uid(&key.uid);
        let defer = in_flight.is_some()
            || matches!(
                self.state.phase,
                PodPhase::PendingStart | PodPhase::Starting
            );
        tracing::info!(
            namespace = %key.namespace, pod = %key.name, uid = %key.uid,
            phase = ?self.state.phase, ?in_flight, defer, container_id,
            "lifecycle-actor: CRI event → runtime_reconcile_action_or_defer"
        );
        if defer {
            self.state.defer_runtime_reconcile(container_id);
            PodAction::Noop
        } else {
            self.reconcile_runtime_action(
                key,
                crate::kubelet::pod_runtime::service::RuntimeReconcileHint::from_container_id(
                    container_id.unwrap_or(""),
                ),
            )
        }
    }

    fn drain_pending_runtime_reconcile(&mut self, key: PodLifecycleKey) -> PodAction {
        tracing::info!(
            namespace = %key.namespace, pod = %key.name, uid = %key.uid,
            phase = ?self.state.phase,
            pending = self.state.pending_runtime_reconcile,
            in_flight = ?self.state.in_flight,
            "lifecycle-actor: drain_pending_runtime_reconcile"
        );
        if self.state.pending_runtime_reconcile
            && self.state.in_flight.is_none()
            && matches!(self.state.phase, PodPhase::Running)
        {
            let hint = self.state.take_runtime_reconcile_hint();
            self.reconcile_runtime_action(key, hint)
        } else {
            PodAction::Noop
        }
    }

    fn reconcile_deferred_runtime_after_start_failure(
        &mut self,
        key: PodLifecycleKey,
    ) -> PodAction {
        if self.state.pending_runtime_reconcile && self.state.in_flight.is_none() {
            let hint = self.state.take_runtime_reconcile_hint();
            return self.reconcile_runtime_action(key, hint);
        }
        PodAction::Noop
    }

    fn reconcile_cri_leftovers_action(&mut self, key: PodLifecycleKey) -> PodAction {
        let operation_id =
            self.next_work_operation(&key, PodLifecycleWorkKind::ReconcileCriLeftovers);
        PodAction::ReconcileCriLeftovers {
            key,
            operation_id,
            permit: None,
        }
    }

    fn defer_for_restart_reconcile(
        &mut self,
        key: &PodLifecycleKey,
        message: LifecycleMessage,
    ) -> Option<PodAction> {
        if self.restart_reconciled {
            return None;
        }
        self.restart_reconciled = true;
        self.pending_restart_admission = Some(message);
        Some(self.reconcile_cri_leftovers_action(key.clone()))
    }

    fn resume_pending_restart_admission(&mut self) -> PodAction {
        let Some(message) = self.pending_restart_admission.take() else {
            return PodAction::Noop;
        };
        self.handle(message)
    }

    fn make_startable_or_start_pending_snapshot(&mut self, key: PodLifecycleKey) -> PodAction {
        self.state.phase = PodPhase::Created;
        if let Some(pending) = self.state.pending_start_pod.take() {
            self.state.phase = PodPhase::PendingStart;
            self.start_pod_action(key, Some(pending.pod))
        } else {
            PodAction::Noop
        }
    }

    fn start_or_check_slot_admission(
        &mut self,
        key: PodLifecycleKey,
        pod: serde_json::Value,
        resource_version: Option<i64>,
        start_after_admit: bool,
    ) -> PodAction {
        if !self.slot_admission_gate_enabled {
            if start_after_admit {
                return self.start_pod_action(key, Some(pod));
            }
            return PodAction::Noop;
        }

        if self.state.admitted_slot_uid.as_deref() == Some(key.uid.as_str()) {
            self.state.pending_start_pod = None;
            if start_after_admit {
                return self.start_pod_action(key, Some(pod));
            }
            return PodAction::Noop;
        }

        self.state.update_pending_start_pod_if_newer(
            key.clone(),
            pod.clone(),
            resource_version,
            start_after_admit,
        );

        if self.state.slot_admission_waiting
            || self.state.in_flight_kind_for_uid(&key.uid)
                == Some(PodLifecycleWorkKind::CheckSlotAdmission)
        {
            return PodAction::Noop;
        }

        self.check_slot_admission_action(key, pod, resource_version, start_after_admit)
    }

    fn handle_command_action(
        &mut self,
        key: PodLifecycleKey,
        command: crate::kubelet::lifecycle::LifecycleCommand,
    ) -> PodAction {
        let operation_id = self.next_work_operation(&key, PodLifecycleWorkKind::HandleCommand);
        PodAction::HandleCommand {
            key,
            command,
            operation_id,
            permit: None,
        }
    }

    fn warn_uid_mismatch(
        &mut self,
        workflow: &'static str,
        expected_uid: &str,
        key: &PodLifecycleKey,
    ) {
        tracing::warn!(
            namespace = %key.namespace,
            pod = %key.name,
            workflow,
            expected_uid = %expected_uid,
            incoming_uid = %key.uid,
            "lifecycle-actor: UID mismatch in pod workflow; dropping stale message"
        );
        #[cfg(test)]
        self.uid_mismatch_warnings
            .push((workflow, expected_uid.to_string(), key.uid.clone()));
    }

    fn warn_active_uid_mismatch(&mut self, workflow: &'static str, key: &PodLifecycleKey) {
        let Some(active_uid) = self.state.active_uid.clone() else {
            return;
        };
        self.warn_uid_mismatch(workflow, &active_uid, key);
    }

    fn reconcile_ephemeral_action(
        &mut self,
        key: PodLifecycleKey,
        pod: Option<serde_json::Value>,
    ) -> PodAction {
        let operation_id = self.next_work_operation(&key, PodLifecycleWorkKind::ReconcileEphemeral);
        PodAction::ReconcileEphemeral {
            key,
            pod,
            operation_id,
            permit: None,
        }
    }

    fn remember_pending_ephemeral_reconcile(
        &mut self,
        key: PodLifecycleKey,
        pod: serde_json::Value,
        resource_version: Option<i64>,
    ) {
        self.state
            .update_pending_ephemeral_reconcile_if_newer(key, pod, resource_version);
    }

    fn reconcile_ephemeral_action_or_defer(
        &mut self,
        key: PodLifecycleKey,
        pod: serde_json::Value,
        resource_version: Option<i64>,
    ) -> PodAction {
        if self.state.in_flight_kind_for_uid(&key.uid).is_some()
            || !matches!(self.state.phase, PodPhase::Running)
        {
            self.remember_pending_ephemeral_reconcile(key, pod, resource_version);
            PodAction::Noop
        } else {
            self.reconcile_ephemeral_action(key, Some(pod))
        }
    }

    fn drain_pending_ephemeral_reconcile(&mut self, key: PodLifecycleKey) -> PodAction {
        if self.state.in_flight.is_some() || !matches!(self.state.phase, PodPhase::Running) {
            return PodAction::Noop;
        }
        let Some(pending) = self
            .state
            .pending_ephemeral_reconcile
            .as_ref()
            .filter(|pending| pending.key.uid == key.uid)
            .map(|pending| (pending.key.clone(), pending.pod.clone()))
        else {
            return PodAction::Noop;
        };
        self.state.pending_ephemeral_reconcile = None;
        self.reconcile_ephemeral_action(pending.0, Some(pending.1))
    }

    fn drain_pending_runtime_or_ephemeral_reconcile(&mut self, key: PodLifecycleKey) -> PodAction {
        let runtime_action = self.drain_pending_runtime_reconcile(key.clone());
        if runtime_action.is_noop() {
            self.drain_pending_ephemeral_reconcile(key)
        } else {
            runtime_action
        }
    }

    fn drain_pending_startup_finalization_retry(&mut self, key: PodLifecycleKey) -> PodAction {
        if !self.state.pending_startup_finalization_retry {
            return PodAction::Noop;
        }
        if !matches!(self.state.phase, PodPhase::Running)
            || self.state.finalized
            || self.state.in_flight.is_some()
        {
            return PodAction::Noop;
        }
        let Some(sandbox_id) = self.state.sandbox_id.clone().filter(|id| !id.is_empty()) else {
            self.state.pending_startup_finalization_retry = false;
            return PodAction::Noop;
        };
        self.state.pending_startup_finalization_retry = false;
        tracing::info!(
            namespace = %key.namespace,
            pod = %key.name,
            uid = %key.uid,
            sandbox_id = %sandbox_id,
            "lifecycle-actor: retrying startup finalization after deferred Running watch echo"
        );
        self.finalize_startup_action(key, None, sandbox_id)
    }

    fn retry_unconfirmed_startup_finalization_after_runtime_reconcile(
        &mut self,
        key: PodLifecycleKey,
    ) -> PodAction {
        if !matches!(self.state.phase, PodPhase::Running)
            || self.state.finalized
            || self.state.in_flight.is_some()
        {
            return PodAction::Noop;
        }
        let Some(sandbox_id) = self.state.sandbox_id.clone().filter(|id| !id.is_empty()) else {
            return PodAction::Noop;
        };
        self.state.pending_startup_finalization_retry = false;
        tracing::info!(
            namespace = %key.namespace,
            pod = %key.name,
            uid = %key.uid,
            sandbox_id = %sandbox_id,
            "lifecycle-actor: runtime reconcile completed with unconfirmed startup finalization; retrying"
        );
        self.finalize_startup_action(key, None, sandbox_id)
    }

    fn start_retry_action(&mut self, key: PodLifecycleKey) -> PodAction {
        PodAction::ScheduleRetry {
            key,
            delay: self.state.next_start_retry_delay(),
        }
    }

    /// Produce a `ScheduleStartPodRetry` action carrying the underlying
    /// failure message and incremented attempt counter so the executor can
    /// write `ErrImagePull`/`ImagePullBackOff` status and emit a
    /// `Warning Failed` event before scheduling the delay.
    fn start_pod_retry_action(&mut self, key: PodLifecycleKey, error_message: String) -> PodAction {
        let delay = self.state.next_start_retry_delay();
        let attempt = self.state.retry_attempts;
        PodAction::ScheduleStartPodRetry {
            key,
            delay,
            error_message,
            attempt,
        }
    }

    fn completion_matches(
        &mut self,
        key: &PodLifecycleKey,
        operation_id: u64,
        kind: PodLifecycleWorkKind,
    ) -> bool {
        if let Some(work) = self.state.in_flight.as_ref()
            && work.uid != key.uid
        {
            let expected_uid = work.uid.clone();
            self.warn_uid_mismatch("pod_work_result", &expected_uid, key);
            return false;
        }
        self.state
            .complete_matching_work(&key.uid, operation_id, kind)
    }

    #[cfg(test)]
    pub fn handle_for_test(&mut self, message: LifecycleMessage) -> PodAction {
        self.handle(message)
    }

    #[cfg(test)]
    pub fn pending_replacement_uid_for_test(&self) -> Option<&str> {
        self.state
            .pending_replacement
            .as_ref()
            .map(|pending| pending.key.uid.as_str())
    }

    #[cfg(test)]
    pub fn pending_replacement_resource_version_for_test(&self) -> Option<i64> {
        self.state
            .pending_replacement
            .as_ref()
            .and_then(|pending| pending.resource_version)
    }

    #[cfg(test)]
    pub fn pending_replacement_pod_for_test(&self) -> Option<&serde_json::Value> {
        self.state
            .pending_replacement
            .as_ref()
            .map(|pending| &pending.pod)
    }

    #[cfg(test)]
    pub fn in_flight_for_test(&self) -> Option<(&str, PodLifecycleWorkKind, u64)> {
        self.state
            .in_flight
            .as_ref()
            .map(|work| (work.uid.as_str(), work.kind, work.operation_id))
    }

    #[cfg(test)]
    pub fn active_uid_for_test(&self) -> Option<&str> {
        self.state.active_uid.as_deref()
    }

    #[cfg(test)]
    pub fn uid_mismatch_warnings_for_test(&self) -> Vec<(&'static str, String, String)> {
        self.uid_mismatch_warnings.clone()
    }

    #[cfg(test)]
    pub fn clear_uid_mismatch_warnings_for_test(&mut self) {
        self.uid_mismatch_warnings.clear();
    }

    #[cfg(test)]
    pub fn admitted_slot_uid_for_test(&self) -> Option<&str> {
        self.state.admitted_slot_uid.as_deref()
    }

    #[cfg(test)]
    pub fn fail_next_spawn_for_test(&mut self) {
        self.fail_next_spawn = true;
    }

    #[cfg(test)]
    pub fn enable_slot_admission_gate_for_test(&mut self) {
        self.slot_admission_gate_enabled = true;
    }

    /// Pure state-machine decision. Always synchronous.
    fn handle(&mut self, message: LifecycleMessage) -> PodAction {
        #[cfg(test)]
        if let Some(event_sink) = &self.event_sink {
            let _ = event_sink.try_send(message.event_name());
        }

        match message {
            // ── Watch events: update resource version, gate on phase ──
            // Once a watch event has dispatched StartPod, later watch echoes
            // stay idle until PodWorkCompleted or RetryDue advances the
            // state machine. This prevents status writes from re-entering pod
            // startup before the completion message reaches the mailbox.
            LifecycleMessage::WatchAdded {
                key,
                resource_version,
                pod,
            } => {
                let is_terminating = pod.pointer("/metadata/deletionTimestamp").is_some();
                let is_node_lost_terminal = pod_is_node_lost_terminal(&pod);
                if let Some(action) = self.defer_for_restart_reconcile(
                    &key,
                    LifecycleMessage::WatchAdded {
                        key: key.clone(),
                        resource_version,
                        pod: pod.clone(),
                    },
                ) {
                    return action;
                }
                self.state.update_resource_version(resource_version);
                if self.state.active_uid.is_none() {
                    self.state.admit_uid(&key.uid);
                } else if !self.active_uid_is(&key) {
                    self.warn_active_uid_mismatch("watch_added", &key);
                    if self.state.phase == PodPhase::Stopping {
                        self.state
                            .set_pending_replacement(key, pod, resource_version);
                        return PodAction::Noop;
                    }
                    self.admit_replacement_uid(&key);
                }
                if is_terminating || is_node_lost_terminal {
                    self.state.drop_pending_ephemeral_reconcile_if_uid(&key.uid);
                    if self.state.in_flight_kind_for_uid(&key.uid)
                        == Some(PodLifecycleWorkKind::StopPod)
                        || self.state.in_flight_kind_for_uid(&key.uid)
                            == Some(PodLifecycleWorkKind::FinalizePodDeletion)
                    {
                        PodAction::Noop
                    } else if self.state.phase == PodPhase::Terminated {
                        self.finalize_pod_deletion_action(key)
                    } else {
                        if self.state.in_flight_kind_for_uid(&key.uid)
                            == Some(PodLifecycleWorkKind::StartPod)
                        {
                            self.state.cancel_in_flight();
                        }
                        self.state.phase = PodPhase::Stopping;
                        self.stop_pod_action(key, Some(pod), String::new())
                    }
                } else if self.state.phase == PodPhase::Created {
                    self.state.phase = PodPhase::PendingStart;
                    self.start_or_check_slot_admission(key, pod, resource_version, true)
                } else {
                    PodAction::Noop
                }
            }

            LifecycleMessage::WatchModified {
                key,
                ref pod,
                resource_version,
                ..
            } => {
                let is_terminating = pod.pointer("/metadata/deletionTimestamp").is_some();
                let is_node_lost_terminal = pod_is_node_lost_terminal(pod);
                let running_sandbox_id = if is_terminating {
                    None
                } else {
                    pod_running_sandbox_id_for_finalization(pod).or_else(|| {
                        pod_has_published_startup_runtime_state(pod)
                            .then(|| self.state.sandbox_id.clone())
                            .flatten()
                    })
                };
                // Runtime reconcile may publish Running + podIP through the
                // worker overlay without advancing the stored resourceVersion.
                // If startup finalization was previously attempted before that
                // runtime state existed, this equal-RV watch echo is the only
                // signal that can start readiness probes.
                let stale_startup_finalization_retry =
                    running_sandbox_id.as_deref().is_some_and(|sandbox_id| {
                        self.active_uid_is(&key)
                            && !self.state.finalized
                            && self
                                .state
                                .sandbox_id
                                .as_deref()
                                .is_none_or(|current| current == sandbox_id)
                    });
                // A terminating watch event (deletionTimestamp set) must never
                // be dropped as a stale resourceVersion. Worker status echoes
                // advance the actor's last-seen RV with synthetic values that
                // can exceed the real RV at which the leader stamped
                // deletionTimestamp; the real terminating event would then look
                // stale and be silently dropped, leaving the Pod Running long
                // past its grace period (multinode worker-pod GC stall).
                // deletionTimestamp is monotonic and the downstream terminating
                // handling is idempotent (in-flight Stop / FinalizePodDeletion
                // collapse to Noop), so acting on it is always safe.
                let terminating_bypasses_stale_rv =
                    (is_terminating || is_node_lost_terminal) && self.active_uid_is(&key);
                if self.state.is_stale_resource_version(resource_version)
                    && !terminating_bypasses_stale_rv
                    && !stale_startup_finalization_retry
                {
                    return PodAction::Noop;
                }
                if let Some(action) = self.defer_for_restart_reconcile(
                    &key,
                    LifecycleMessage::WatchModified {
                        key: key.clone(),
                        resource_version,
                        pod: pod.clone(),
                    },
                ) {
                    return action;
                }
                self.state.update_resource_version(resource_version);
                let finalizing_after_local_clear =
                    is_terminating && self.state.phase == PodPhase::Terminated;
                if self.state.active_uid.is_none() && !finalizing_after_local_clear {
                    self.state.admit_uid(&key.uid);
                } else if !self.active_uid_is(&key) {
                    self.warn_active_uid_mismatch("watch_modified", &key);
                    if finalizing_after_local_clear {
                        return self.finalize_pod_deletion_action(key);
                    }
                    if self.state.phase == PodPhase::Stopping {
                        let should_store = self
                            .state
                            .pending_replacement
                            .as_ref()
                            .map(|pending| pending.key.uid == key.uid)
                            .unwrap_or(true);
                        if should_store {
                            self.state
                                .set_pending_replacement(key, pod.clone(), resource_version);
                        }
                    }
                    return PodAction::Noop;
                }

                if is_terminating || is_node_lost_terminal {
                    self.state.drop_pending_ephemeral_reconcile_if_uid(&key.uid);
                    if self.state.in_flight_kind_for_uid(&key.uid)
                        == Some(PodLifecycleWorkKind::StopPod)
                        || self.state.in_flight_kind_for_uid(&key.uid)
                            == Some(PodLifecycleWorkKind::FinalizePodDeletion)
                    {
                        PodAction::Noop
                    } else if self.state.phase == PodPhase::Terminated {
                        self.finalize_pod_deletion_action(key)
                    } else {
                        if self.state.in_flight_kind_for_uid(&key.uid)
                            == Some(PodLifecycleWorkKind::StartPod)
                        {
                            self.state.cancel_in_flight();
                        }
                        self.state.phase = PodPhase::Stopping;
                        self.stop_pod_action(key, Some(pod.clone()), String::new())
                    }
                } else if let Some(sandbox_id) = running_sandbox_id {
                    let has_ephemeral = pod_has_ephemeral_containers(pod);
                    let in_flight_kind = self.state.in_flight_kind_for_uid(&key.uid);
                    if self.slot_admission_gate_enabled
                        && self.state.admitted_slot_uid.as_deref() != Some(key.uid.as_str())
                    {
                        return self.start_or_check_slot_admission(
                            key,
                            pod.clone(),
                            resource_version,
                            false,
                        );
                    }
                    self.state.phase = PodPhase::Running;
                    self.state.reset_start_retry_attempts();
                    if in_flight_kind == Some(PodLifecycleWorkKind::StartPod) {
                        self.state.cancel_in_flight();
                    }
                    match self.state.on_started(&sandbox_id) {
                        FinalizationAction::RunFinalizers => {
                            if has_ephemeral {
                                self.remember_pending_ephemeral_reconcile(
                                    key.clone(),
                                    pod.clone(),
                                    resource_version,
                                );
                            }
                            if in_flight_kind.is_some()
                                && in_flight_kind != Some(PodLifecycleWorkKind::StartPod)
                            {
                                if in_flight_kind == Some(PodLifecycleWorkKind::FinalizeStartup) {
                                    self.state.pending_startup_finalization_retry = true;
                                    tracing::info!(
                                        namespace = %key.namespace,
                                        pod = %key.name,
                                        uid = %key.uid,
                                        "lifecycle-actor: Running watch echo arrived during startup finalization; retry pending"
                                    );
                                }
                                PodAction::Noop
                            } else {
                                self.finalize_startup_action(key, Some(pod.clone()), sandbox_id)
                            }
                        }
                        FinalizationAction::AlreadyFinalized => {
                            if has_ephemeral {
                                self.reconcile_ephemeral_action_or_defer(
                                    key,
                                    pod.clone(),
                                    resource_version,
                                )
                            } else {
                                PodAction::Noop
                            }
                        }
                    }
                } else if self.state.phase == PodPhase::Running
                    && pod_pending_with_published_runtime_state(pod)
                {
                    tracing::info!(
                        namespace = %key.namespace,
                        pod = %key.name,
                        uid = %key.uid,
                        in_flight = ?self.state.in_flight_kind_for_uid(&key.uid),
                        "lifecycle-actor: Pending watch echo has podIP and running containers; reconciling runtime status"
                    );
                    self.runtime_reconcile_action_or_defer(key, None)
                } else if self.state.phase == PodPhase::Created {
                    self.state.phase = PodPhase::PendingStart;
                    self.start_or_check_slot_admission(key, pod.clone(), resource_version, true)
                } else if self.state.phase != PodPhase::PendingStart {
                    if let Some(fingerprint) = pod_create_container_config_error_fingerprint(pod) {
                        if self.last_config_error_retry_fingerprint.as_deref()
                            != Some(fingerprint.as_str())
                        {
                            self.last_config_error_retry_fingerprint = Some(fingerprint);
                            self.state.phase = PodPhase::PendingStart;
                            self.start_or_check_slot_admission(
                                key,
                                pod.clone(),
                                resource_version,
                                true,
                            )
                        } else {
                            PodAction::Noop
                        }
                    } else if pod_has_ephemeral_containers(pod) {
                        self.reconcile_ephemeral_action_or_defer(key, pod.clone(), resource_version)
                    } else {
                        PodAction::Noop
                    }
                } else if pod_has_ephemeral_containers(pod) {
                    self.reconcile_ephemeral_action_or_defer(key, pod.clone(), resource_version)
                } else {
                    if self.state.in_flight_kind_for_uid(&key.uid).is_none()
                        && let Some(fingerprint) =
                            pod_create_container_config_error_fingerprint(pod)
                    {
                        let previous = self.last_config_error_retry_fingerprint.as_deref();
                        if previous.is_none() {
                            self.last_config_error_retry_fingerprint = Some(fingerprint);
                            return PodAction::Noop;
                        }
                        if previous.is_some_and(|value| value != fingerprint.as_str()) {
                            self.last_config_error_retry_fingerprint = Some(fingerprint);
                            return self.start_or_check_slot_admission(
                                key,
                                pod.clone(),
                                resource_version,
                                true,
                            );
                        }
                    }
                    if pod
                        .pointer("/spec/nodeName")
                        .and_then(|v| v.as_str())
                        .is_some_and(|node_name| !node_name.trim().is_empty())
                    {
                        self.state.update_pending_start_pod_if_newer(
                            key.clone(),
                            pod.clone(),
                            resource_version,
                            true,
                        );
                    }
                    PodAction::Noop
                }
            }

            LifecycleMessage::WatchDeleted {
                key,
                resource_version,
                pod,
            } => {
                self.state.update_resource_version(resource_version);
                if self.state.active_uid.is_none() {
                    self.state.phase = PodPhase::Terminated;
                    return PodAction::Noop;
                }
                if !self.active_uid_is(&key) {
                    self.warn_active_uid_mismatch("watch_deleted", &key);
                    self.state.drop_pending_replacement_if_uid(&key.uid);
                    return PodAction::Noop;
                }
                if self.state.phase == PodPhase::Terminated
                    || self.state.in_flight_kind_for_uid(&key.uid)
                        == Some(PodLifecycleWorkKind::StopPod)
                {
                    PodAction::Noop
                } else {
                    if self.state.in_flight_kind_for_uid(&key.uid)
                        == Some(PodLifecycleWorkKind::StartPod)
                    {
                        self.state.cancel_in_flight();
                    }
                    self.state.phase = PodPhase::Stopping;
                    self.stop_pod_action(key, Some(pod), String::new())
                }
            }

            // ── Retry / failure ──
            LifecycleMessage::RetryDue { key } => {
                if self.state.phase == PodPhase::Terminated
                    && self.state.in_flight.is_none()
                    && self.state.active_uid.is_none()
                {
                    return self.finalize_pod_deletion_action(key);
                }
                if self.state.phase == PodPhase::Stopping
                    && self.state.in_flight.is_none()
                    && let Some((pending_key, pending_pod, pending_sandbox_id)) = self
                        .state
                        .pending_stop_pod
                        .as_ref()
                        .filter(|pending| pending.key.uid == key.uid)
                        .map(|pending| {
                            (
                                pending.key.clone(),
                                pending.pod.clone(),
                                pending.sandbox_id.clone(),
                            )
                        })
                {
                    return self.stop_pod_action(pending_key, pending_pod, pending_sandbox_id);
                }
                self.ensure_active_uid(&key);
                if !self.active_uid_is(&key) {
                    self.warn_active_uid_mismatch("retry_due", &key);
                    return PodAction::Noop;
                }
                if matches!(self.state.phase, PodPhase::Created | PodPhase::PendingStart) {
                    self.state.phase = PodPhase::PendingStart;
                    if let Some((pending_key, pending_pod, pending_rv, pending_start_after_admit)) =
                        self.state.pending_start_pod.as_ref().map(|pending| {
                            (
                                pending.key.clone(),
                                pending.pod.clone(),
                                pending.resource_version,
                                pending.start_after_admit,
                            )
                        })
                    {
                        self.start_or_check_slot_admission(
                            pending_key,
                            pending_pod,
                            pending_rv,
                            pending_start_after_admit,
                        )
                    } else if self.state.admitted_slot_uid.as_deref() == Some(key.uid.as_str()) {
                        self.start_pod_action(key, None)
                    } else {
                        PodAction::Noop
                    }
                } else {
                    PodAction::Noop
                }
            }

            LifecycleMessage::OrphanFinalize { key, reason } => {
                tracing::warn!(
                    namespace = %key.namespace,
                    pod = %key.name,
                    uid = %key.uid,
                    reason = ?reason,
                    "pod lifecycle actor received orphan finalization request"
                );
                if self.state.in_flight_kind_for_uid(&key.uid)
                    == Some(PodLifecycleWorkKind::StopPod)
                    || self.state.in_flight_kind_for_uid(&key.uid)
                        == Some(PodLifecycleWorkKind::FinalizePodDeletion)
                {
                    return PodAction::Noop;
                }
                if self.state.active_uid.as_deref() != Some(key.uid.as_str()) {
                    self.state.admit_uid(&key.uid);
                }
                self.state.phase = PodPhase::Stopping;
                self.stop_pod_action(key, None, String::new())
            }

            LifecycleMessage::PodWorkFailed {
                key,
                operation_id,
                kind,
                retryable,
                failure,
            } => {
                if !self.completion_matches(&key, operation_id, kind) {
                    return PodAction::Noop;
                }
                let finalizers_pending =
                    matches!(failure, PodLifecycleWorkFailure::FinalizersPending);
                if !finalizers_pending {
                    tracing::warn!(
                        namespace = %key.namespace,
                        pod = %key.name,
                        uid = %key.uid,
                        operation_id,
                        kind = ?kind,
                        retryable,
                        failure = ?failure,
                        "pod lifecycle work failed"
                    );
                }
                if kind == PodLifecycleWorkKind::ReconcileCriLeftovers {
                    tracing::warn!(
                        namespace = %key.namespace,
                        pod = %key.name,
                        uid = %key.uid,
                        "CRI leftover reconcile failed; proceeding with first admission after best-effort delay"
                    );
                    return self.resume_pending_restart_admission();
                }
                match (kind, retryable) {
                    (PodLifecycleWorkKind::StartPod, true) => {
                        let reconcile =
                            self.reconcile_deferred_runtime_after_start_failure(key.clone());
                        if reconcile.is_noop() {
                            // Carry the underlying error message to the
                            // executor so it can write ErrImagePull /
                            // ImagePullBackOff to containerStatuses and emit
                            // a Warning Failed event. Non-Startup failures
                            // (Cancelled, etc.) reach the actor with an
                            // empty message — the executor still surfaces
                            // a generic Failed event.
                            let error_message = match &failure {
                                PodLifecycleWorkFailure::Startup(msg)
                                | PodLifecycleWorkFailure::DispatchFailed(msg) => msg.clone(),
                                PodLifecycleWorkFailure::Cancelled => "cancelled".to_string(),
                                PodLifecycleWorkFailure::ContainerNotFound => {
                                    "container not found".to_string()
                                }
                                PodLifecycleWorkFailure::DeadlineExceeded => {
                                    "deadline exceeded".to_string()
                                }
                                PodLifecycleWorkFailure::Deleted => "pod deleted".to_string(),
                                PodLifecycleWorkFailure::FinalizersPending => {
                                    "finalizers pending".to_string()
                                }
                                PodLifecycleWorkFailure::NotOwned { .. } => {
                                    "pod not owned by local node".to_string()
                                }
                            };
                            self.start_pod_retry_action(key, error_message)
                        } else {
                            reconcile
                        }
                    }
                    (PodLifecycleWorkKind::StartPod, false)
                        if self.state.pending_runtime_reconcile
                            && matches!(failure, PodLifecycleWorkFailure::Startup(_)) =>
                    {
                        self.reconcile_deferred_runtime_after_start_failure(key)
                    }
                    (PodLifecycleWorkKind::StartPod, false)
                        if matches!(
                            failure,
                            PodLifecycleWorkFailure::DispatchFailed(_)
                                | PodLifecycleWorkFailure::Deleted
                                | PodLifecycleWorkFailure::DeadlineExceeded
                        ) =>
                    {
                        self.make_startable_or_start_pending_snapshot(key)
                    }
                    (PodLifecycleWorkKind::StopPod, true) => {
                        self.state.phase = PodPhase::Stopping;
                        PodAction::ScheduleRetry {
                            key,
                            delay: std::time::Duration::from_secs(1),
                        }
                    }
                    (PodLifecycleWorkKind::StopPod, false)
                        if matches!(failure, PodLifecycleWorkFailure::ContainerNotFound) =>
                    {
                        self.stop_pod_completed_action(key)
                    }
                    // HR#11 / P0 StopPod loop: the local node does not own
                    // this Pod (unscheduled or assigned to another node), so
                    // there is no local CRI/CNI/volume state to clean. This is
                    // terminal on the local actor — retrying can never succeed
                    // and would spin the actor forever. The Pod row is owned by
                    // `PodStore::delete_unscheduled_with_uid` (unscheduled) or
                    // the owning node's lifecycle actor. Cancel the in-flight
                    // stop and drop the pending snapshot without finalizing a
                    // hard delete on a row this node does not own.
                    (PodLifecycleWorkKind::StopPod, false)
                        if matches!(failure, PodLifecycleWorkFailure::NotOwned { .. }) =>
                    {
                        self.state.cancel_in_flight();
                        self.state.pending_stop_pod = None;
                        PodAction::Noop
                    }
                    (PodLifecycleWorkKind::StopPod, false) => {
                        self.state.phase = PodPhase::Stopping;
                        PodAction::Noop
                    }
                    (PodLifecycleWorkKind::FinalizePodDeletion, true) => PodAction::ScheduleRetry {
                        key,
                        delay: if finalizers_pending {
                            std::time::Duration::from_secs(5)
                        } else {
                            std::time::Duration::from_secs(1)
                        },
                    },
                    (PodLifecycleWorkKind::FinalizePodDeletion, false) => PodAction::Noop,
                    _ => PodAction::Noop,
                }
            }

            LifecycleMessage::SlotAdmissionGranted {
                key,
                operation_id,
                pod,
                resource_version,
                start_after_admit,
            } => {
                if !self.completion_matches(
                    &key,
                    operation_id,
                    PodLifecycleWorkKind::CheckSlotAdmission,
                ) {
                    return PodAction::Noop;
                }
                self.state.slot_admission_waiting = false;
                self.state.admitted_slot_uid = Some(key.uid.clone());
                self.state.update_resource_version(resource_version);
                self.state.pending_start_pod = None;
                if start_after_admit && self.active_uid_is(&key) {
                    self.state.phase = PodPhase::PendingStart;
                    self.start_pod_action(key, Some(pod))
                } else if start_after_admit {
                    self.warn_active_uid_mismatch("slot_admission_granted", &key);
                    PodAction::Noop
                } else {
                    PodAction::Noop
                }
            }

            LifecycleMessage::SlotAdmissionBlocked {
                key, operation_id, ..
            } => {
                if !self.completion_matches(
                    &key,
                    operation_id,
                    PodLifecycleWorkKind::CheckSlotAdmission,
                ) {
                    return PodAction::Noop;
                }
                self.state.slot_admission_waiting = true;
                PodAction::Noop
            }

            LifecycleMessage::SlotAdmissionWake { key } => {
                let Some(pending) = self.state.pending_start_pod.as_ref() else {
                    return PodAction::Noop;
                };
                if pending.key.uid != key.uid {
                    let expected_uid = pending.key.uid.clone();
                    self.warn_uid_mismatch("slot_admission_wake", &expected_uid, &key);
                    return PodAction::Noop;
                }
                if self.state.in_flight.is_some() {
                    return PodAction::Noop;
                }
                let pending_key = pending.key.clone();
                let pending_pod = pending.pod.clone();
                let pending_rv = pending.resource_version;
                let pending_start_after_admit = pending.start_after_admit;
                self.state.slot_admission_waiting = false;
                self.check_slot_admission_action(
                    pending_key,
                    pending_pod,
                    pending_rv,
                    pending_start_after_admit,
                )
            }

            // ── Completions ──
            LifecycleMessage::PodWorkCompleted {
                key,
                operation_id,
                kind: super::message::PodLifecycleWorkKind::ReconcileCriLeftovers,
                ..
            } => {
                if !self.completion_matches(
                    &key,
                    operation_id,
                    PodLifecycleWorkKind::ReconcileCriLeftovers,
                ) {
                    return PodAction::Noop;
                }
                self.resume_pending_restart_admission()
            }

            LifecycleMessage::PodWorkCompleted {
                key,
                operation_id,
                kind: super::message::PodLifecycleWorkKind::StartPod,
                sandbox_id,
            } => {
                if !self.completion_matches(&key, operation_id, PodLifecycleWorkKind::StartPod) {
                    return PodAction::Noop;
                }
                tracing::info!(
                    namespace = %key.namespace, pod = %key.name, uid = %key.uid,
                    ?sandbox_id,
                    pending_runtime_reconcile = self.state.pending_runtime_reconcile,
                    "lifecycle-actor: StartPod completed → FinalizeStartup"
                );
                self.state.phase = PodPhase::Running;
                // Use sandbox_id from completion when available; fall back to state.
                let sid = sandbox_id
                    .or_else(|| self.state.sandbox_id.clone())
                    .unwrap_or_default();
                if sid.is_empty() {
                    if self.state.pending_runtime_reconcile {
                        tracing::info!(
                            namespace = %key.namespace,
                            pod = %key.name,
                            uid = %key.uid,
                            "lifecycle-actor: StartPod completed without sandbox id; draining deferred runtime reconcile"
                        );
                        let hint = self.state.take_runtime_reconcile_hint();
                        self.state.reset_start_retry_attempts();
                        self.state.pending_start_pod = None;
                        return self.reconcile_runtime_action(key, hint);
                    }
                    // A StartPod can legitimately complete without a sandbox
                    // when the executor skipped an unscheduled pod snapshot.
                    // Keep the actor startable so the scheduler's bind watch
                    // update can dispatch StartPod again.
                    return self.make_startable_or_start_pending_snapshot(key);
                }
                self.state.reset_start_retry_attempts();
                self.state.pending_start_pod = None;
                let action = self.state.on_started(&sid);
                match action {
                    FinalizationAction::RunFinalizers => {
                        self.finalize_startup_action(key, None, sid)
                    }
                    FinalizationAction::AlreadyFinalized => PodAction::Noop,
                }
            }

            LifecycleMessage::PodWorkCompleted {
                key,
                operation_id,
                kind: super::message::PodLifecycleWorkKind::StopPod,
                ..
            } => {
                if !self.completion_matches(&key, operation_id, PodLifecycleWorkKind::StopPod) {
                    return PodAction::Noop;
                }
                self.stop_pod_completed_action(key)
            }

            LifecycleMessage::PodWorkCompleted {
                key,
                operation_id,
                kind: super::message::PodLifecycleWorkKind::FinalizePodDeletion,
                ..
            } => {
                if !self.completion_matches(
                    &key,
                    operation_id,
                    PodLifecycleWorkKind::FinalizePodDeletion,
                ) {
                    return PodAction::Noop;
                }
                if let Some(pending) = self.state.pending_replacement.take() {
                    self.state.admit_uid(&pending.key.uid);
                    self.state.update_resource_version(pending.resource_version);
                    self.state.phase = PodPhase::PendingStart;
                    self.last_config_error_retry_fingerprint = None;
                    self.start_or_check_slot_admission(
                        pending.key,
                        pending.pod,
                        pending.resource_version,
                        true,
                    )
                } else {
                    PodAction::Noop
                }
            }

            LifecycleMessage::PodWorkCompleted {
                key,
                operation_id,
                kind: super::message::PodLifecycleWorkKind::FinalizeStartup,
                sandbox_id,
                ..
            } => {
                if !self.completion_matches(
                    &key,
                    operation_id,
                    PodLifecycleWorkKind::FinalizeStartup,
                ) {
                    return PodAction::Noop;
                }
                if let Some(sandbox_id) = sandbox_id
                    && self.state.sandbox_id.as_deref() == Some(sandbox_id.as_str())
                {
                    self.state.finalized = true;
                    self.state.pending_startup_finalization_retry = false;
                }
                tracing::info!(
                    namespace = %key.namespace, pod = %key.name, uid = %key.uid,
                    phase = ?self.state.phase,
                    pending_runtime_reconcile = self.state.pending_runtime_reconcile,
                    pending_startup_finalization_retry = self.state.pending_startup_finalization_retry,
                    "lifecycle-actor: FinalizeStartup completed"
                );
                // Dispatch any deferred CRI event that arrived during
                // startup finalization (e.g. container stopped/crashed).
                let deferred = self.drain_pending_runtime_or_ephemeral_reconcile(key.clone());
                if deferred.is_noop() {
                    self.drain_pending_startup_finalization_retry(key)
                } else {
                    deferred
                }
            }

            LifecycleMessage::PodWorkCompleted {
                key,
                operation_id,
                kind: super::message::PodLifecycleWorkKind::ReconcileRuntime,
                ..
            } => {
                if !self.completion_matches(
                    &key,
                    operation_id,
                    PodLifecycleWorkKind::ReconcileRuntime,
                ) {
                    return PodAction::Noop;
                }
                if matches!(self.state.phase, PodPhase::PendingStart) {
                    self.start_retry_action(key)
                } else {
                    let deferred = self.drain_pending_runtime_or_ephemeral_reconcile(key.clone());
                    if deferred.is_noop() {
                        self.retry_unconfirmed_startup_finalization_after_runtime_reconcile(key)
                    } else {
                        deferred
                    }
                }
            }

            LifecycleMessage::PodWorkCompleted {
                key,
                operation_id,
                kind: super::message::PodLifecycleWorkKind::ReconcileEphemeral,
                ..
            } => {
                if !self.completion_matches(
                    &key,
                    operation_id,
                    PodLifecycleWorkKind::ReconcileEphemeral,
                ) {
                    return PodAction::Noop;
                }
                self.drain_pending_ephemeral_reconcile(key)
            }

            // Other completion kinds
            LifecycleMessage::PodWorkCompleted {
                key,
                operation_id,
                kind,
                ..
            } => {
                if !self.completion_matches(&key, operation_id, kind) {
                    return PodAction::Noop;
                }
                PodAction::Noop
            }

            // ── Runtime reconcile ──
            LifecycleMessage::CriEvent {
                key, container_id, ..
            } => {
                if self.state.active_uid.is_some() && !self.active_uid_is(&key) {
                    self.warn_active_uid_mismatch("cri_event", &key);
                    return PodAction::Noop;
                }
                self.ensure_active_uid(&key);
                self.runtime_reconcile_action_or_defer(key, Some(container_id.as_str()))
            }
            LifecycleMessage::ActiveDeadlineDue { key } => {
                if self.state.active_uid.is_some() && !self.active_uid_is(&key) {
                    self.warn_active_uid_mismatch("active_deadline_due", &key);
                    return PodAction::Noop;
                }
                self.ensure_active_uid(&key);
                self.runtime_reconcile_action_or_defer(key, None)
            }

            // ── Commands ──
            LifecycleMessage::LifecycleCommand { key, command } => {
                if self.state.active_uid.is_some() && !self.active_uid_is(&key) {
                    self.warn_active_uid_mismatch("lifecycle_command", &key);
                    return PodAction::Noop;
                }
                self.ensure_active_uid(&key);
                self.handle_command_action(key, command)
            }

            // ── Unhandled ──
            LifecycleMessage::NetworkAssigned { .. }
            | LifecycleMessage::ProbeResult { .. }
            | LifecycleMessage::ActorIdleGraceExpired { .. } => PodAction::Noop,
        }
    }
}
