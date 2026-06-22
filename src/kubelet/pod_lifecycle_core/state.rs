//! Shared pod lifecycle state machine — used by both actor and multiplex backends.
//!
//! `PodLifecycleState` tracks pod phase, in-flight work, monotonic operation ids,
//! and sandbox identity so both actor and multiplex can share state-machine logic.

use super::message::{PodLifecycleKey, PodLifecycleWorkKind};

/// Phases a pod transitions through during its lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PodPhase {
    Created,
    PendingStart,
    Starting,
    Running,
    Stopping,
    Terminated,
}

#[derive(Debug)]
pub struct PendingReplacement {
    pub key: PodLifecycleKey,
    pub pod: serde_json::Value,
    pub resource_version: Option<i64>,
}

#[derive(Debug)]
pub struct PendingStartPod {
    pub key: PodLifecycleKey,
    pub pod: serde_json::Value,
    pub resource_version: Option<i64>,
    pub start_after_admit: bool,
}

#[derive(Debug)]
pub struct PendingStopPod {
    pub key: PodLifecycleKey,
    pub pod: Option<serde_json::Value>,
    pub sandbox_id: String,
}

#[derive(Debug)]
pub struct PendingEphemeralReconcile {
    pub key: PodLifecycleKey,
    pub pod: serde_json::Value,
    pub resource_version: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct InFlightWork {
    pub uid: String,
    pub kind: PodLifecycleWorkKind,
    pub operation_id: u64,
    pub cancel: tokio_util::sync::CancellationToken,
}

/// Per-pod lifecycle state shared by actor and multiplex backends.
#[derive(Debug)]
pub struct PodLifecycleState {
    pub phase: PodPhase,
    pub sandbox_id: Option<String>,
    pub active_uid: Option<String>,
    pub admitted_slot_uid: Option<String>,
    pub active_sandbox_id: Option<String>,
    pub pending_replacement: Option<PendingReplacement>,
    pub pending_start_pod: Option<PendingStartPod>,
    pub pending_stop_pod: Option<PendingStopPod>,
    pub pending_ephemeral_reconcile: Option<PendingEphemeralReconcile>,
    pub slot_admission_waiting: bool,
    pub in_flight: Option<InFlightWork>,
    pub finalized: bool,
    /// Monotonic counter for correlating work with completions.
    pub operation_id: u64,
    /// Operation id of the currently in-flight work, if any.
    pub work_in_flight: Option<u64>,
    pub retry_attempts: u32,
    /// Most recent resourceVersion seen from a watch event.
    pub last_resource_version: Option<i64>,
    /// A CRI event arrived while startup/finalization was still in progress.
    /// Runtime reconciliation must run after startup finalizers complete so
    /// fast-exiting containers cannot be hidden by the final Running write.
    pub pending_runtime_reconcile: bool,
    /// Container id carried by the deferred CRI event, if any. Drained into a
    /// `RuntimeReconcileHint` when the deferred reconcile runs so the
    /// reconciler can read the concrete (terminated) container status instead
    /// of synthesizing Pending/ContainerCreating under a lossy listing.
    pub pending_runtime_reconcile_container_id: Option<String>,
    /// A Running watch echo arrived while startup finalization was in flight.
    /// If that in-flight finalizer still cannot confirm probe startup, retry
    /// once with the newer watch state instead of waiting for another event.
    pub pending_startup_finalization_retry: bool,
}

impl Default for PodLifecycleState {
    fn default() -> Self {
        Self {
            phase: PodPhase::Created,
            sandbox_id: None,
            active_uid: None,
            admitted_slot_uid: None,
            active_sandbox_id: None,
            pending_replacement: None,
            pending_start_pod: None,
            pending_stop_pod: None,
            pending_ephemeral_reconcile: None,
            slot_admission_waiting: false,
            in_flight: None,
            finalized: false,
            operation_id: 0,
            work_in_flight: None,
            retry_attempts: 0,
            last_resource_version: None,
            pending_runtime_reconcile: false,
            pending_runtime_reconcile_container_id: None,
            pending_startup_finalization_retry: false,
        }
    }
}

impl PodLifecycleState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Allocate the next monotonic operation id.
    pub fn next_operation_id(&mut self) -> u64 {
        self.operation_id += 1;
        self.operation_id
    }

    pub fn active_uid_matches(&self, uid: &str) -> bool {
        self.active_uid.as_deref() == Some(uid)
    }

    pub fn admit_uid(&mut self, uid: &str) {
        self.active_uid = Some(uid.to_string());
        self.active_sandbox_id = None;
        self.sandbox_id = None;
        self.pending_replacement = None;
        self.pending_start_pod = None;
        self.pending_stop_pod = None;
        self.pending_ephemeral_reconcile = None;
        self.slot_admission_waiting = false;
        self.in_flight = None;
        self.work_in_flight = None;
        self.finalized = false;
        self.retry_attempts = 0;
        self.pending_runtime_reconcile = false;
        self.pending_runtime_reconcile_container_id = None;
        self.pending_startup_finalization_retry = false;
        self.phase = PodPhase::Created;
    }

    pub fn set_pending_start_pod(
        &mut self,
        key: PodLifecycleKey,
        pod: serde_json::Value,
        resource_version: Option<i64>,
        start_after_admit: bool,
    ) {
        self.pending_start_pod = Some(PendingStartPod {
            key,
            pod,
            resource_version,
            start_after_admit,
        });
    }

    pub fn update_pending_start_pod_if_newer(
        &mut self,
        key: PodLifecycleKey,
        pod: serde_json::Value,
        resource_version: Option<i64>,
        start_after_admit: bool,
    ) {
        let should_replace = match &self.pending_start_pod {
            Some(current) if current.key.uid == key.uid => {
                match (current.resource_version, resource_version) {
                    (Some(current_rv), Some(new_rv)) => new_rv >= current_rv,
                    (None, Some(_)) => true,
                    _ => false,
                }
            }
            Some(_) => true,
            None => true,
        };
        if should_replace {
            self.set_pending_start_pod(key, pod, resource_version, start_after_admit);
        }
    }

    pub fn set_pending_replacement(
        &mut self,
        key: PodLifecycleKey,
        pod: serde_json::Value,
        resource_version: Option<i64>,
    ) {
        self.pending_replacement = Some(PendingReplacement {
            key,
            pod,
            resource_version,
        });
    }

    pub fn drop_pending_replacement_if_uid(&mut self, uid: &str) -> bool {
        if self
            .pending_replacement
            .as_ref()
            .is_some_and(|pending| pending.key.uid == uid)
        {
            self.pending_replacement = None;
            return true;
        }
        false
    }

    pub fn update_pending_ephemeral_reconcile_if_newer(
        &mut self,
        key: PodLifecycleKey,
        pod: serde_json::Value,
        resource_version: Option<i64>,
    ) {
        let should_replace = match &self.pending_ephemeral_reconcile {
            Some(current) if current.key.uid == key.uid => {
                match (current.resource_version, resource_version) {
                    (Some(current_rv), Some(new_rv)) => new_rv >= current_rv,
                    (None, Some(_)) => true,
                    (Some(_), None) => false,
                    (None, None) => true,
                }
            }
            Some(_) => true,
            None => true,
        };
        if should_replace {
            self.pending_ephemeral_reconcile = Some(PendingEphemeralReconcile {
                key,
                pod,
                resource_version,
            });
        }
    }

    pub fn drop_pending_ephemeral_reconcile_if_uid(&mut self, uid: &str) -> bool {
        if self
            .pending_ephemeral_reconcile
            .as_ref()
            .is_some_and(|pending| pending.key.uid == uid)
        {
            self.pending_ephemeral_reconcile = None;
            return true;
        }
        false
    }

    pub fn record_in_flight(&mut self, uid: String, kind: PodLifecycleWorkKind, operation_id: u64) {
        self.work_in_flight = Some(operation_id);
        self.in_flight = Some(InFlightWork {
            uid,
            kind,
            operation_id,
            cancel: tokio_util::sync::CancellationToken::new(),
        });
    }

    pub fn in_flight_matches(
        &self,
        uid: &str,
        operation_id: u64,
        kind: PodLifecycleWorkKind,
    ) -> bool {
        self.in_flight.as_ref().is_some_and(|work| {
            work.uid == uid && work.operation_id == operation_id && work.kind == kind
        })
    }

    pub fn in_flight_kind_for_uid(&self, uid: &str) -> Option<PodLifecycleWorkKind> {
        self.in_flight
            .as_ref()
            .filter(|work| work.uid == uid)
            .map(|work| work.kind)
    }

    pub fn clear_in_flight(&mut self) {
        self.work_in_flight = None;
        self.in_flight = None;
    }

    pub fn cancel_in_flight(&mut self) {
        if let Some(work) = self.in_flight.take() {
            work.cancel.cancel();
        }
        self.work_in_flight = None;
    }

    pub fn complete_matching_work(
        &mut self,
        uid: &str,
        operation_id: u64,
        kind: PodLifecycleWorkKind,
    ) -> bool {
        if !self.in_flight_matches(uid, operation_id, kind) {
            return false;
        }
        self.clear_in_flight();
        true
    }

    pub fn next_start_retry_delay(&mut self) -> std::time::Duration {
        self.retry_attempts = self.retry_attempts.saturating_add(1);
        crate::kubelet::pod_creation_state::retry_backoff(self.retry_attempts)
    }

    pub fn reset_start_retry_attempts(&mut self) {
        self.retry_attempts = 0;
    }

    /// Try to mark work as in-flight. Returns the operation id if successful,
    /// `None` if another operation is already in flight.
    pub fn try_start_work(&mut self, _kind: PodLifecycleWorkKind) -> Option<u64> {
        if self.work_in_flight.is_some() || self.in_flight.is_some() {
            return None;
        }
        let id = self.next_operation_id();
        self.work_in_flight = Some(id);
        Some(id)
    }

    /// Mark work as completed. Returns `true` if the operation was the
    /// currently in-flight one (and clears in-flight), `false` if it was
    /// a stale completion.
    pub fn complete_work(&mut self, operation_id: u64, _kind: PodLifecycleWorkKind) -> bool {
        if self.work_in_flight == Some(operation_id) {
            self.clear_in_flight();
            true
        } else {
            false
        }
    }

    /// True when the pod has reached a terminal phase.
    pub fn is_terminal(&self) -> bool {
        matches!(self.phase, PodPhase::Terminated)
    }

    /// Update the last seen resource version. Ignores older versions.
    pub fn update_resource_version(&mut self, rv: Option<i64>) {
        match (self.last_resource_version, rv) {
            (Some(current), Some(new)) if new > current => {
                self.last_resource_version = Some(new);
            }
            (None, some) => {
                self.last_resource_version = some;
            }
            _ => {}
        }
    }

    /// Check if the given resource version is older than the last seen.
    pub fn is_stale_resource_version(&self, rv: Option<i64>) -> bool {
        match (self.last_resource_version, rv) {
            (Some(current), Some(new)) => new <= current,
            _ => false,
        }
    }

    /// Record a sandbox start and return whether finalization should run.
    pub fn on_started(&mut self, sandbox_id: &str) -> FinalizationAction {
        if self.sandbox_id.as_deref() == Some(sandbox_id) && self.finalized {
            return FinalizationAction::AlreadyFinalized;
        }
        self.sandbox_id = Some(sandbox_id.to_string());
        self.active_sandbox_id = Some(sandbox_id.to_string());
        self.finalized = false;
        self.pending_startup_finalization_retry = false;
        FinalizationAction::RunFinalizers
    }

    /// Defer a runtime reconcile that could not run because startup or another
    /// operation is still in flight. `container_id` carries the CRI event's
    /// concrete container id (if any) so the later reconcile can read its
    /// status instead of synthesizing Pending/ContainerCreating under a
    /// lossy sandbox container listing.
    pub fn defer_runtime_reconcile(&mut self, container_id: Option<&str>) {
        self.pending_runtime_reconcile = true;
        // Preserve an earlier hint if the new one is empty — a subsequent
        // CRI event with no container id must not erase a prior one.
        if let Some(id) = container_id.filter(|id| !id.is_empty()) {
            self.pending_runtime_reconcile_container_id = Some(id.to_string());
        }
    }

    /// Drain a deferred runtime reconcile into a `RuntimeReconcileHint`.
    /// Clears both the boolean flag and the carried container id. Returns
    /// `none()` when nothing was deferred.
    pub fn take_runtime_reconcile_hint(
        &mut self,
    ) -> crate::kubelet::pod_runtime::service::RuntimeReconcileHint {
        if !self.pending_runtime_reconcile {
            return crate::kubelet::pod_runtime::service::RuntimeReconcileHint::none();
        }
        self.pending_runtime_reconcile = false;
        let container_id = self.pending_runtime_reconcile_container_id.take();
        match container_id {
            Some(id) => {
                crate::kubelet::pod_runtime::service::RuntimeReconcileHint::from_container_id(id)
            }
            None => crate::kubelet::pod_runtime::service::RuntimeReconcileHint::none(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FinalizationAction {
    RunFinalizers,
    AlreadyFinalized,
}
