//! Definitive `PodAction` enum — the output of pod lifecycle state machines.
//!
//! Both actor and multiplex backends use this type. Permit fields are
//! `Option` so actor mode can construct actions without multiplex permits.
//!
//! Accessor methods (`key()`, `operation_id()`, etc.) are used by the
//! multiplex adapter, trace/diagnostics emitters, and failure synthesis.

use std::sync::Arc;

use super::concurrency::WorkPermit;
use super::message::{
    LifecycleMessage, PodLifecycleKey, PodLifecycleWorkFailure, PodLifecycleWorkKind,
};
use crate::kubelet::lifecycle::LifecycleCommand;
use crate::task_supervisor::TaskCategory;

/// Synthesizes a `PodWorkFailed` message from an executor dispatch error.
/// Shared by actor backend, multiplex adapter, and `PodDemuxEngine::spawn_work`.
pub type FailureSynthesizer = Arc<dyn Fn(anyhow::Error) -> LifecycleMessage + Send + Sync>;

/// Action decided by a pod state machine for a lifecycle message.
#[derive(Debug)]
pub enum PodAction {
    /// Start a pod sandbox and containers.
    StartPod {
        key: PodLifecycleKey,
        /// `Some` = use this snapshot; `None` = re-fetch from datastore (RetryDue path).
        pod: Option<serde_json::Value>,
        operation_id: u64,
        permit: Option<WorkPermit>,
    },
    /// Check cluster-visible slot admission before runtime startup.
    CheckSlotAdmission {
        key: PodLifecycleKey,
        pod: serde_json::Value,
        resource_version: Option<i64>,
        start_after_admit: bool,
        operation_id: u64,
        permit: Option<WorkPermit>,
    },
    /// Stop a running pod sandbox.
    StopPod {
        key: PodLifecycleKey,
        /// `Some` for watch-driven deletes/modifications so hard-deleted pods
        /// can still be cleaned up after their datastore row is gone. `None`
        /// means the executor must re-fetch by key.
        pod: Option<serde_json::Value>,
        sandbox_id: String,
        operation_id: u64,
        permit: Option<WorkPermit>,
    },
    /// Remove the Pod API object after StopPod confirmed runtime cleanup and
    /// the actor cleared its local slot/cache state.
    FinalizePodDeletion {
        key: PodLifecycleKey,
        operation_id: u64,
        permit: Option<WorkPermit>,
    },
    /// Run post-start finalization: probes, endpoints, owner reconcile.
    FinalizeStartup {
        key: PodLifecycleKey,
        pod: Option<serde_json::Value>,
        sandbox_id: String,
        operation_id: u64,
        permit: Option<WorkPermit>,
    },
    /// Reconcile pod runtime state (CRI event response, deadline check).
    ReconcileRuntime {
        key: PodLifecycleKey,
        operation_id: u64,
        permit: Option<WorkPermit>,
    },
    /// Clean same-slot CRI leftovers before admitting a fresh actor UID.
    ReconcileCriLeftovers {
        key: PodLifecycleKey,
        operation_id: u64,
        permit: Option<WorkPermit>,
    },
    /// Handle a probe lifecycle command.
    HandleCommand {
        key: PodLifecycleKey,
        command: LifecycleCommand,
        operation_id: u64,
        permit: Option<WorkPermit>,
    },
    /// Reconcile ephemeral containers for a pod.
    ReconcileEphemeral {
        key: PodLifecycleKey,
        pod: Option<serde_json::Value>,
        operation_id: u64,
        permit: Option<WorkPermit>,
    },
    /// Schedule a retry after a delay (handled by executor via spawn_delay).
    ScheduleRetry {
        key: PodLifecycleKey,
        delay: std::time::Duration,
    },
    /// Schedule a StartPod retry after a delay. Unlike `ScheduleRetry`, the
    /// executor first writes `containerStatuses[].state.waiting.reason` =
    /// `ErrImagePull` / `ImagePullBackOff` (UID-keyed) and emits a
    /// `Warning Failed` Pod event so the failure is visible via
    /// `kubectl get pods` / `kubectl describe`. Phase stays Pending so
    /// controllers don't treat the pod as terminal.
    ScheduleStartPodRetry {
        key: PodLifecycleKey,
        delay: std::time::Duration,
        error_message: String,
        attempt: u32,
    },
    /// No state change or work needed.
    Noop,
}

impl PodAction {
    /// Pod key for routing and diagnostics. `None` only for `Noop`.
    pub fn key(&self) -> Option<&PodLifecycleKey> {
        match self {
            Self::StartPod { key, .. }
            | Self::CheckSlotAdmission { key, .. }
            | Self::StopPod { key, .. }
            | Self::FinalizePodDeletion { key, .. }
            | Self::FinalizeStartup { key, .. }
            | Self::ReconcileRuntime { key, .. }
            | Self::ReconcileCriLeftovers { key, .. }
            | Self::HandleCommand { key, .. }
            | Self::ReconcileEphemeral { key, .. }
            | Self::ScheduleRetry { key, .. }
            | Self::ScheduleStartPodRetry { key, .. } => Some(key),
            Self::Noop => None,
        }
    }

    /// Operation id for completion correlation. `None` for fire-and-forget actions
    /// (`ScheduleRetry`, `ScheduleStartPodRetry`, `Noop`).
    pub fn operation_id(&self) -> Option<u64> {
        match self {
            Self::StartPod { operation_id, .. }
            | Self::CheckSlotAdmission { operation_id, .. }
            | Self::StopPod { operation_id, .. }
            | Self::FinalizePodDeletion { operation_id, .. }
            | Self::FinalizeStartup { operation_id, .. }
            | Self::ReconcileRuntime { operation_id, .. }
            | Self::ReconcileCriLeftovers { operation_id, .. }
            | Self::HandleCommand { operation_id, .. }
            | Self::ReconcileEphemeral { operation_id, .. } => Some(*operation_id),
            Self::ScheduleRetry { .. } | Self::ScheduleStartPodRetry { .. } | Self::Noop => None,
        }
    }

    /// `Some(kind)` if a `PodWorkCompleted` / `PodWorkFailed` message is expected
    /// from the executor. `None` for fire-and-forget actions. Used by the multiplex
    /// adapter to synthesize `PodWorkFailed` when the actor executor returns `Err`
    /// before posting a completion.
    pub fn expected_completion(&self) -> Option<PodLifecycleWorkKind> {
        match self {
            Self::StartPod { .. } => Some(PodLifecycleWorkKind::StartPod),
            Self::CheckSlotAdmission { .. } => Some(PodLifecycleWorkKind::CheckSlotAdmission),
            Self::StopPod { .. } => Some(PodLifecycleWorkKind::StopPod),
            Self::FinalizePodDeletion { .. } => Some(PodLifecycleWorkKind::FinalizePodDeletion),
            Self::FinalizeStartup { .. } => Some(PodLifecycleWorkKind::FinalizeStartup),
            Self::ReconcileRuntime { .. } => Some(PodLifecycleWorkKind::ReconcileRuntime),
            Self::ReconcileCriLeftovers { .. } => Some(PodLifecycleWorkKind::ReconcileCriLeftovers),
            Self::HandleCommand { .. } => Some(PodLifecycleWorkKind::HandleCommand),
            Self::ReconcileEphemeral { .. } => Some(PodLifecycleWorkKind::ReconcileEphemeral),
            Self::ScheduleRetry { .. } | Self::ScheduleStartPodRetry { .. } | Self::Noop => None,
        }
    }

    /// Supervisor task category for this action.
    pub fn task_category(&self) -> TaskCategory {
        match self {
            Self::StartPod { .. }
            | Self::CheckSlotAdmission { .. }
            | Self::StopPod { .. }
            | Self::FinalizePodDeletion { .. }
            | Self::FinalizeStartup { .. }
            | Self::ReconcileRuntime { .. }
            | Self::ReconcileCriLeftovers { .. }
            | Self::HandleCommand { .. }
            | Self::ReconcileEphemeral { .. } => TaskCategory::PodLifecycleWork,
            Self::ScheduleRetry { .. } | Self::ScheduleStartPodRetry { .. } => TaskCategory::Timer,
            Self::Noop => TaskCategory::Background,
        }
    }

    /// Stable static name for tracing / supervisor diagnostics.
    pub fn task_name(&self) -> &'static str {
        match self {
            Self::StartPod { .. } => "executor_start_pod",
            Self::CheckSlotAdmission { .. } => "executor_check_slot_admission",
            Self::StopPod { .. } => "executor_stop_pod",
            Self::FinalizePodDeletion { .. } => "executor_finalize_pod_deletion",
            Self::FinalizeStartup { .. } => "executor_finalize_startup",
            Self::ReconcileRuntime { .. } => "executor_reconcile_runtime",
            Self::ReconcileCriLeftovers { .. } => "executor_reconcile_cri_leftovers",
            Self::HandleCommand { .. } => "executor_handle_command",
            Self::ReconcileEphemeral { .. } => "executor_reconcile_ephemeral",
            Self::ScheduleRetry { .. } => "executor_schedule_retry",
            Self::ScheduleStartPodRetry { .. } => "executor_schedule_start_pod_retry",
            Self::Noop => "executor_noop",
        }
    }

    /// Synthesizer for `PodWorkFailed` on either spawn rejection or
    /// inline dispatch error. Single closure shared by both paths so the
    /// resulting message shape is guaranteed identical. `None` for
    /// actions that do not own an in-flight operation (`ScheduleRetry`,
    /// `ScheduleStartPodRetry`, `Noop`).
    pub fn failure_synthesizer(&self) -> Option<FailureSynthesizer> {
        let kind = self.expected_completion()?;
        let op_id = self.operation_id()?;
        let key = self.key()?.clone();
        Some(Arc::new(move |error: anyhow::Error| {
            LifecycleMessage::PodWorkFailed {
                key: key.clone(),
                operation_id: op_id,
                kind,
                retryable: false,
                failure: PodLifecycleWorkFailure::DispatchFailed(format!("{error:#}")),
            }
        }))
    }

    pub fn is_noop(&self) -> bool {
        matches!(self, PodAction::Noop)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_key() -> PodLifecycleKey {
        PodLifecycleKey::new("default", "test-pod", "uid-1")
    }

    // ── PodAction accessor table-driven tests ──

    #[test]
    fn start_pod_accessors() {
        let action = PodAction::StartPod {
            key: test_key(),
            pod: Some(json!({"kind": "Pod"})),
            operation_id: 42,
            permit: None,
        };
        assert_eq!(action.key().map(|k| &k.name), Some(&"test-pod".to_string()));
        assert_eq!(action.operation_id(), Some(42));
        assert_eq!(
            action.expected_completion(),
            Some(PodLifecycleWorkKind::StartPod)
        );
        assert_eq!(action.task_category(), TaskCategory::PodLifecycleWork);
        assert_eq!(action.task_name(), "executor_start_pod");
        assert!(action.failure_synthesizer().is_some());
    }

    #[test]
    fn stop_pod_accessors() {
        let action = PodAction::StopPod {
            key: test_key(),
            pod: Some(json!({"kind": "Pod"})),
            sandbox_id: "sbox-1".into(),
            operation_id: 43,
            permit: None,
        };
        assert_eq!(action.operation_id(), Some(43));
        assert_eq!(
            action.expected_completion(),
            Some(PodLifecycleWorkKind::StopPod)
        );
        assert_eq!(action.task_category(), TaskCategory::PodLifecycleWork);
        assert_eq!(action.task_name(), "executor_stop_pod");
        assert!(action.failure_synthesizer().is_some());
    }

    #[test]
    fn finalize_pod_deletion_accessors() {
        let action = PodAction::FinalizePodDeletion {
            key: test_key(),
            operation_id: 50,
            permit: None,
        };
        assert_eq!(action.operation_id(), Some(50));
        assert_eq!(
            action.expected_completion(),
            Some(PodLifecycleWorkKind::FinalizePodDeletion)
        );
        assert_eq!(action.task_category(), TaskCategory::PodLifecycleWork);
        assert_eq!(action.task_name(), "executor_finalize_pod_deletion");
        assert!(action.failure_synthesizer().is_some());
    }

    #[test]
    fn finalize_startup_accessors() {
        let action = PodAction::FinalizeStartup {
            key: test_key(),
            pod: Some(json!({"kind": "Pod"})),
            sandbox_id: "sbox-1".into(),
            operation_id: 44,
            permit: None,
        };
        assert_eq!(action.operation_id(), Some(44));
        assert_eq!(
            action.expected_completion(),
            Some(PodLifecycleWorkKind::FinalizeStartup)
        );
        assert_eq!(action.task_category(), TaskCategory::PodLifecycleWork);
        assert!(action.failure_synthesizer().is_some());
    }

    #[test]
    fn reconcile_runtime_accessors() {
        let action = PodAction::ReconcileRuntime {
            key: test_key(),
            operation_id: 45,
            permit: None,
        };
        assert_eq!(action.operation_id(), Some(45));
        assert_eq!(
            action.expected_completion(),
            Some(PodLifecycleWorkKind::ReconcileRuntime)
        );
        assert_eq!(action.task_category(), TaskCategory::PodLifecycleWork);
        assert_eq!(action.task_name(), "executor_reconcile_runtime");
        assert!(action.failure_synthesizer().is_some());
    }

    #[test]
    fn reconcile_cri_leftovers_accessors() {
        let action = PodAction::ReconcileCriLeftovers {
            key: test_key(),
            operation_id: 49,
            permit: None,
        };
        assert_eq!(action.operation_id(), Some(49));
        assert_eq!(
            action.expected_completion(),
            Some(PodLifecycleWorkKind::ReconcileCriLeftovers)
        );
        assert_eq!(action.task_category(), TaskCategory::PodLifecycleWork);
        assert_eq!(action.task_name(), "executor_reconcile_cri_leftovers");
        assert!(action.failure_synthesizer().is_some());
    }

    #[test]
    fn handle_command_accessors() {
        let action = PodAction::HandleCommand {
            key: test_key(),
            command: LifecycleCommand::ReadinessChanged {
                pod_uid: "test-uid".into(),
                namespace: "default".into(),
                pod_name: "p".into(),
                container_name: "c".into(),
                ready: true,
            },
            operation_id: 47,
            permit: None,
        };
        assert_eq!(action.operation_id(), Some(47));
        assert_eq!(
            action.expected_completion(),
            Some(PodLifecycleWorkKind::HandleCommand)
        );
        assert_eq!(action.task_category(), TaskCategory::PodLifecycleWork);
        assert!(action.failure_synthesizer().is_some());
    }

    #[test]
    fn reconcile_ephemeral_accessors() {
        let action = PodAction::ReconcileEphemeral {
            key: test_key(),
            pod: Some(json!({"kind": "Pod"})),
            operation_id: 48,
            permit: None,
        };
        assert_eq!(action.operation_id(), Some(48));
        assert_eq!(
            action.expected_completion(),
            Some(PodLifecycleWorkKind::ReconcileEphemeral)
        );
        assert_eq!(action.task_category(), TaskCategory::PodLifecycleWork);
        assert!(action.failure_synthesizer().is_some());
    }

    #[test]
    fn schedule_retry_accessors() {
        let action = PodAction::ScheduleRetry {
            key: test_key(),
            delay: std::time::Duration::from_secs(5),
        };
        assert!(action.key().is_some());
        assert_eq!(action.operation_id(), None);
        assert_eq!(action.expected_completion(), None);
        assert_eq!(action.task_category(), TaskCategory::Timer);
        assert_eq!(action.task_name(), "executor_schedule_retry");
        assert!(action.failure_synthesizer().is_none());
    }

    #[test]
    fn schedule_start_pod_retry_carries_error_message_and_attempt() {
        let action = PodAction::ScheduleStartPodRetry {
            key: test_key(),
            delay: std::time::Duration::from_secs(4),
            error_message: "Failed to pull image \"x:1\": connection refused".into(),
            attempt: 2,
        };
        assert_eq!(action.key().map(|k| &k.uid), Some(&"uid-1".to_string()));
        assert_eq!(action.operation_id(), None);
        assert_eq!(action.expected_completion(), None);
        assert_eq!(action.task_category(), TaskCategory::Timer);
        assert_eq!(action.task_name(), "executor_schedule_start_pod_retry");
        assert!(action.failure_synthesizer().is_none());
        match action {
            PodAction::ScheduleStartPodRetry {
                delay,
                error_message,
                attempt,
                ..
            } => {
                assert_eq!(delay, std::time::Duration::from_secs(4));
                assert!(error_message.contains("Failed to pull image"));
                assert_eq!(attempt, 2);
            }
            _ => panic!("expected ScheduleStartPodRetry"),
        }
    }

    #[test]
    fn noop_accessors() {
        let action = PodAction::Noop;
        assert_eq!(action.key(), None);
        assert_eq!(action.operation_id(), None);
        assert_eq!(action.expected_completion(), None);
        assert_eq!(action.task_category(), TaskCategory::Background);
        assert_eq!(action.task_name(), "executor_noop");
        assert!(action.failure_synthesizer().is_none());
    }

    // ── failure_synthesizer produces correct PodWorkFailed message ──

    #[test]
    fn failure_synthesizer_produces_dispatch_failed() {
        let action = PodAction::StartPod {
            key: test_key(),
            pod: Some(json!({"kind": "Pod"})),
            operation_id: 99,
            permit: None,
        };
        let synth = action
            .failure_synthesizer()
            .expect("StartPod should have synth");
        let msg = synth(anyhow::anyhow!("test error"));
        match msg {
            LifecycleMessage::PodWorkFailed {
                key,
                operation_id,
                kind,
                retryable,
                failure,
            } => {
                assert_eq!(key.name, "test-pod");
                assert_eq!(operation_id, 99);
                assert_eq!(kind, PodLifecycleWorkKind::StartPod);
                assert!(!retryable);
                assert!(
                    matches!(failure, PodLifecycleWorkFailure::DispatchFailed(ref s) if s.contains("test error"))
                );
            }
            other => panic!("expected PodWorkFailed, got {other:?}"),
        }
    }

    // ── type-shape assertion: permits are Option ──

    #[test]
    fn pod_action_permits_are_option() {
        // Actor-mode: all permits are None
        let _start = PodAction::StartPod {
            key: test_key(),
            pod: Some(json!({"kind": "Pod"})),
            operation_id: 1,
            permit: None,
        };
        let _stop = PodAction::StopPod {
            key: test_key(),
            pod: None,
            sandbox_id: "s".into(),
            operation_id: 1,
            permit: None,
        };

        // Multiplex-mode: permits are Some (compile-only check)
        let wp = WorkPermit { _private: () };
        let _start_mpx = PodAction::StartPod {
            key: test_key(),
            pod: Some(json!({"kind": "Pod"})),
            operation_id: 1,
            permit: Some(wp),
        };
    }
}
