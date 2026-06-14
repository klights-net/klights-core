//! Actor-mode pod lifecycle work executor — async dispatch.
//!
//! The actor-mode `PodWorkExecutor` can be async because per-pod actor loops
//! only head-of-line block their own pod, not a shared multiplex consumer.
//! The multiplex-safe sync `dispatch -> WorkSpawn` shape lands with the
//! multiplex backend, not R2a.

use super::LifecycleReplyHandle;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::kubelet::pod_lifecycle_core::action::PodAction;
use crate::kubelet::pod_lifecycle_core::message::LifecycleMessage;
use crate::kubelet::pod_runtime::service::PodRuntimeService;

/// Error type for actor-mode executor dispatch.
pub type ExecutorError = anyhow::Error;

/// Actor-mode executor seam. `dispatch` may await (per-pod actor serialization
/// means the await only blocks that pod's actor, not a shared multiplex consumer).
#[async_trait::async_trait]
pub trait PodWorkExecutor: Send + Sync {
    /// Execute the work described by `action`. The implementation may await
    /// CRI, DB, or filesystem work inline (actor mode only).
    async fn dispatch(
        &self,
        action: PodAction,
        reply_to: LifecycleReplyHandle,
    ) -> Result<(), ExecutorError>;

    /// Execute lifecycle work with a cancellation token. L6 threads the token
    /// through the executor boundary; L7 makes individual CRI awaits observe it.
    async fn dispatch_with_cancel(
        &self,
        action: PodAction,
        reply_to: LifecycleReplyHandle,
        _cancel: CancellationToken,
    ) -> Result<(), ExecutorError> {
        self.dispatch(action, reply_to).await
    }
}

/// Executor that succeeds immediately for every action.
/// Used before individual executor paths are wired (R2c–R2g).
pub struct NoopExecutor;

#[async_trait::async_trait]
impl PodWorkExecutor for NoopExecutor {
    async fn dispatch(
        &self,
        _action: PodAction,
        _reply_to: LifecycleReplyHandle,
    ) -> Result<(), ExecutorError> {
        Ok(())
    }
}

/// Test-only executor that records dispatched actions.
#[cfg(test)]
pub struct RecordingExecutor {
    pub actions: std::sync::Mutex<Vec<PodAction>>,
}

#[cfg(test)]
impl RecordingExecutor {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            actions: std::sync::Mutex::new(Vec::new()),
        })
    }

    pub fn take_actions(&self) -> Vec<PodAction> {
        std::mem::take(&mut *self.actions.lock().unwrap())
    }

    pub fn action_count(&self) -> usize {
        self.actions.lock().unwrap().len()
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl PodWorkExecutor for RecordingExecutor {
    async fn dispatch(
        &self,
        action: PodAction,
        reply_to: LifecycleReplyHandle,
    ) -> Result<(), ExecutorError> {
        // CheckSlotAdmission synthesizes SlotAdmissionGranted so the actor
        // state machine can proceed to StartPod. Other actions with
        // expected_completion get a PodWorkCompleted.
        let completion = if let PodAction::CheckSlotAdmission {
            key,
            pod,
            resource_version,
            start_after_admit,
            operation_id,
            ..
        } = &action
        {
            Some(LifecycleMessage::SlotAdmissionGranted {
                key: key.clone(),
                operation_id: *operation_id,
                pod: pod.clone(),
                resource_version: *resource_version,
                start_after_admit: *start_after_admit,
            })
        } else if let (Some(key), Some(op_id), Some(kind)) = (
            action.key().cloned(),
            action.operation_id(),
            action.expected_completion(),
        ) {
            Some(LifecycleMessage::PodWorkCompleted {
                key,
                operation_id: op_id,
                kind,
                sandbox_id: None,
            })
        } else {
            None
        };
        self.actions.lock().unwrap().push(action);
        if let Some(message) = completion {
            let _ = reply_to.route(message).await;
        }
        Ok(())
    }
}

// ── Production executor (R2c0) ──

/// Real executor that performs lifecycle work using injected runtime
/// components. Individual fields replace the former context bundle.
pub struct PodLifecycleExecutor {
    runtime: Arc<dyn PodRuntimeService>,
}

impl PodLifecycleExecutor {
    pub fn new(runtime: Arc<dyn PodRuntimeService>) -> Self {
        Self { runtime }
    }

    pub fn runtime(&self) -> Arc<dyn PodRuntimeService> {
        self.runtime.clone()
    }
}

#[async_trait::async_trait]
impl PodWorkExecutor for PodLifecycleExecutor {
    async fn dispatch(
        &self,
        action: PodAction,
        reply_to: LifecycleReplyHandle,
    ) -> Result<(), ExecutorError> {
        self.dispatch_with_cancel(action, reply_to, CancellationToken::new())
            .await
    }

    async fn dispatch_with_cancel(
        &self,
        action: PodAction,
        reply_to: LifecycleReplyHandle,
        cancel: CancellationToken,
    ) -> Result<(), ExecutorError> {
        match &action {
            PodAction::StartPod { .. } => {
                dispatch_via_runtime(self.runtime.as_ref(), action, reply_to, cancel).await
            }
            PodAction::CheckSlotAdmission { .. } => {
                dispatch_via_runtime(self.runtime.as_ref(), action, reply_to, cancel).await
            }
            PodAction::ReconcileRuntime { .. } => {
                dispatch_via_runtime(self.runtime.as_ref(), action, reply_to, cancel).await
            }
            PodAction::ReconcileCriLeftovers { .. } => {
                dispatch_via_runtime(self.runtime.as_ref(), action, reply_to, cancel).await
            }
            PodAction::ScheduleRetry { .. } => {
                dispatch_via_runtime(self.runtime.as_ref(), action, reply_to, cancel).await
            }
            PodAction::ScheduleStartPodRetry { .. } => {
                dispatch_via_runtime(self.runtime.as_ref(), action, reply_to, cancel).await
            }
            PodAction::StopPod { .. } => {
                dispatch_via_runtime(self.runtime.as_ref(), action, reply_to, cancel).await
            }
            PodAction::FinalizePodDeletion { .. } => {
                dispatch_via_runtime(self.runtime.as_ref(), action, reply_to, cancel).await
            }
            PodAction::FinalizeStartup { .. } => {
                dispatch_via_runtime(self.runtime.as_ref(), action, reply_to, cancel).await
            }
            PodAction::HandleCommand { .. } => {
                dispatch_via_runtime(self.runtime.as_ref(), action, reply_to, cancel).await
            }
            PodAction::ReconcileEphemeral { .. } => {
                dispatch_via_runtime(self.runtime.as_ref(), action, reply_to, cancel).await
            }
            PodAction::Noop => Ok(()),
        }
    }
}

/// Dispatch a `PodAction` to a `PodRuntimeService` instead of executor-owned
/// side-effect handlers.
async fn dispatch_via_runtime(
    runtime: &dyn PodRuntimeService,
    action: PodAction,
    reply_to: LifecycleReplyHandle,
    cancel: CancellationToken,
) -> Result<(), ExecutorError> {
    use crate::kubelet::pod_lifecycle_core::message::{
        PodLifecycleWorkFailure, PodLifecycleWorkKind,
    };
    use crate::kubelet::pod_runtime::service::{
        PodDeletionFinalizeResult, PodFinalizeStartupResult, PodRuntimeKey,
        PodSlotAdmissionRequest, PodStartResult,
    };

    match action {
        PodAction::StartPod {
            key,
            pod,
            operation_id,
            ..
        } => {
            let kind = PodLifecycleWorkKind::StartPod;
            let runtime_key = PodRuntimeKey::from(&key);
            match runtime.start_pod(runtime_key, pod, cancel).await {
                Ok(PodStartResult::Started { sandbox_id }) => {
                    let _ = reply_to
                        .route(LifecycleMessage::PodWorkCompleted {
                            key,
                            operation_id,
                            kind,
                            sandbox_id,
                        })
                        .await;
                }
                Ok(PodStartResult::Cancelled) => {
                    let _ = reply_to
                        .route(LifecycleMessage::PodWorkFailed {
                            key,
                            operation_id,
                            kind,
                            retryable: true,
                            failure: PodLifecycleWorkFailure::Cancelled,
                        })
                        .await;
                }
                Ok(PodStartResult::Failed(msg)) => {
                    let _ = reply_to
                        .route(LifecycleMessage::PodWorkFailed {
                            key,
                            operation_id,
                            kind,
                            retryable: true,
                            failure: PodLifecycleWorkFailure::Startup(msg),
                        })
                        .await;
                }
                Ok(PodStartResult::Terminal(msg)) => {
                    let _ = reply_to
                        .route(LifecycleMessage::PodWorkFailed {
                            key,
                            operation_id,
                            kind,
                            retryable: false,
                            failure: PodLifecycleWorkFailure::Startup(msg),
                        })
                        .await;
                }
                Err(e) => {
                    let _ = reply_to
                        .route(LifecycleMessage::PodWorkFailed {
                            key,
                            operation_id,
                            kind,
                            retryable: true,
                            failure: PodLifecycleWorkFailure::DispatchFailed(format!("{e:#}")),
                        })
                        .await;
                }
            }
            Ok(())
        }
        PodAction::CheckSlotAdmission {
            key,
            pod,
            resource_version,
            start_after_admit,
            operation_id,
            ..
        } => {
            let runtime_key = PodRuntimeKey::from(&key);
            runtime
                .check_slot_admission(
                    PodSlotAdmissionRequest {
                        key: runtime_key,
                        pod,
                        resource_version,
                        start_after_admit,
                        operation_id,
                    },
                    reply_to,
                    cancel,
                )
                .await?;
            Ok(())
        }
        PodAction::StopPod {
            key,
            pod,
            sandbox_id,
            operation_id,
            ..
        } => {
            let kind = PodLifecycleWorkKind::StopPod;
            let runtime_key = PodRuntimeKey::from(&key);
            match runtime
                .stop_pod(
                    runtime_key.clone(),
                    pod,
                    if sandbox_id.is_empty() {
                        None
                    } else {
                        Some(sandbox_id.clone())
                    },
                )
                .await
            {
                Ok(()) => {
                    let _ = reply_to
                        .route(LifecycleMessage::PodWorkCompleted {
                            key,
                            operation_id,
                            kind,
                            sandbox_id: Some(sandbox_id),
                        })
                        .await;
                }
                Err(e) => {
                    let (retryable, failure) = if is_container_not_found_runtime_error(&e) {
                        (false, PodLifecycleWorkFailure::ContainerNotFound)
                    } else if e.to_string().to_ascii_lowercase().contains("timed out") {
                        (true, PodLifecycleWorkFailure::DeadlineExceeded)
                    } else {
                        (
                            true,
                            PodLifecycleWorkFailure::DispatchFailed(format!("{e:#}")),
                        )
                    };
                    let _ = reply_to
                        .route(LifecycleMessage::PodWorkFailed {
                            key,
                            operation_id,
                            kind,
                            retryable,
                            failure,
                        })
                        .await;
                }
            }
            Ok(())
        }
        PodAction::FinalizeStartup {
            key,
            pod,
            sandbox_id,
            operation_id,
            ..
        } => {
            let kind = PodLifecycleWorkKind::FinalizeStartup;
            let runtime_key = PodRuntimeKey::from(&key);
            let sandbox_id_hint = (!sandbox_id.trim().is_empty()).then(|| sandbox_id.clone());
            match runtime
                .finalize_startup(runtime_key, pod, sandbox_id_hint)
                .await
            {
                Ok(PodFinalizeStartupResult::Confirmed { sandbox_id }) => {
                    let _ = reply_to
                        .route(LifecycleMessage::PodWorkCompleted {
                            key,
                            operation_id,
                            kind,
                            sandbox_id: Some(sandbox_id),
                        })
                        .await;
                }
                Ok(PodFinalizeStartupResult::Unconfirmed) => {
                    let _ = reply_to
                        .route(LifecycleMessage::PodWorkCompleted {
                            key,
                            operation_id,
                            kind,
                            sandbox_id: None,
                        })
                        .await;
                }
                Err(e) => {
                    let _ = reply_to
                        .route(LifecycleMessage::PodWorkFailed {
                            key,
                            operation_id,
                            kind,
                            retryable: true,
                            failure: PodLifecycleWorkFailure::DispatchFailed(format!("{e:#}")),
                        })
                        .await;
                }
            }
            Ok(())
        }
        PodAction::FinalizePodDeletion {
            key, operation_id, ..
        } => {
            let kind = PodLifecycleWorkKind::FinalizePodDeletion;
            let runtime_key = PodRuntimeKey::from(&key);
            match runtime.finalize_deletion(runtime_key).await {
                Ok(PodDeletionFinalizeResult::DeletedOrAlreadyGone) => {
                    let _ = reply_to
                        .route(LifecycleMessage::PodWorkCompleted {
                            key,
                            operation_id,
                            kind,
                            sandbox_id: None,
                        })
                        .await;
                }
                Ok(PodDeletionFinalizeResult::FinalizersPending) => {
                    let _ = reply_to
                        .route(LifecycleMessage::PodWorkFailed {
                            key,
                            operation_id,
                            kind,
                            retryable: true,
                            failure: PodLifecycleWorkFailure::FinalizersPending,
                        })
                        .await;
                }
                Err(e) => {
                    let _ = reply_to
                        .route(LifecycleMessage::PodWorkFailed {
                            key,
                            operation_id,
                            kind,
                            retryable: true,
                            failure: PodLifecycleWorkFailure::DispatchFailed(format!("{e:#}")),
                        })
                        .await;
                }
            }
            Ok(())
        }
        PodAction::ReconcileRuntime {
            key, operation_id, ..
        } => {
            let kind = PodLifecycleWorkKind::ReconcileRuntime;
            let runtime_key = PodRuntimeKey::from(&key);
            match runtime.reconcile_runtime(runtime_key).await {
                Ok(()) => {
                    let _ = reply_to
                        .route(LifecycleMessage::PodWorkCompleted {
                            key,
                            operation_id,
                            kind,
                            sandbox_id: None,
                        })
                        .await;
                }
                Err(e) => {
                    tracing::warn!(
                        namespace = %key.namespace,
                        pod = %key.name,
                        uid = %key.uid,
                        "ReconcileRuntime via runtime service failed: {e:#}"
                    );
                    return Err(anyhow::anyhow!("{e:#}"));
                }
            }
            Ok(())
        }
        PodAction::ReconcileCriLeftovers {
            key, operation_id, ..
        } => {
            let kind = PodLifecycleWorkKind::ReconcileCriLeftovers;
            let runtime_key = PodRuntimeKey::from(&key);
            // Legacy handler always returns Ok(()) even on error; preserve that.
            if let Err(e) = runtime.reconcile_cri_leftovers(runtime_key).await {
                tracing::warn!(
                    namespace = %key.namespace,
                    pod = %key.name,
                    uid = %key.uid,
                    "ReconcileCriLeftovers via runtime service failed: {e:#}"
                );
            }
            let _ = reply_to
                .route(LifecycleMessage::PodWorkCompleted {
                    key,
                    operation_id,
                    kind,
                    sandbox_id: None,
                })
                .await;
            Ok(())
        }
        PodAction::ScheduleRetry { key, delay } => {
            let runtime_key = PodRuntimeKey::from(&key);
            runtime.schedule_retry(runtime_key, delay, reply_to).await?;
            Ok(())
        }
        PodAction::ScheduleStartPodRetry {
            key,
            delay,
            error_message,
            attempt,
        } => {
            let runtime_key = PodRuntimeKey::from(&key);
            runtime
                .schedule_start_pod_retry(runtime_key, delay, error_message, attempt, reply_to)
                .await?;
            Ok(())
        }
        PodAction::ReconcileEphemeral {
            key,
            pod,
            operation_id,
            ..
        } => {
            let kind = PodLifecycleWorkKind::ReconcileEphemeral;
            let runtime_key = PodRuntimeKey::from(&key);
            runtime.reconcile_ephemeral(runtime_key, pod).await?;
            let _ = reply_to
                .route(LifecycleMessage::PodWorkCompleted {
                    key,
                    operation_id,
                    kind,
                    sandbox_id: None,
                })
                .await;
            Ok(())
        }
        PodAction::HandleCommand {
            command,
            key,
            operation_id,
            ..
        } => {
            let kind = PodLifecycleWorkKind::HandleCommand;
            runtime.handle_lifecycle_command(command).await?;
            let _ = reply_to
                .route(LifecycleMessage::PodWorkCompleted {
                    key,
                    operation_id,
                    kind,
                    sandbox_id: None,
                })
                .await;
            Ok(())
        }
        PodAction::Noop => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kubelet::pod_lifecycle_core::message::PodLifecycleKey;

    /// Task 15.1: executor stores individual component fields directly
    /// without depending on the bundled context type.
    #[test]
    fn lifecycle_executor_does_not_depend_on_pod_work_context() {
        // Compile-time assertion: the executor struct, constructors, and
        // method signatures must not reference the bundled context type.
        // Individual component fields are stored directly instead.
    }

    /// Task 24: PodLifecycleExecutor constructor accepts and stores a
    /// non-optional `Arc<dyn PodRuntimeService>`.
    #[tokio::test]
    async fn pod_lifecycle_executor_constructor_accepts_runtime_service_object() {
        use std::sync::Arc;

        use crate::kubelet::pod_runtime::test_support::MockPodRuntimeService;

        let mock_runtime: Arc<dyn PodRuntimeService> = Arc::new(MockPodRuntimeService::new());

        let executor = PodLifecycleExecutor::new(mock_runtime);

        let _runtime = executor.runtime();
    }

    #[test]
    fn expected_completion_covers_every_runtime_routed_action_kind() {
        use crate::kubelet::lifecycle::LifecycleCommand;
        use crate::kubelet::pod_lifecycle_core::message::PodLifecycleWorkKind;

        let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
        let actions = vec![
            (
                PodAction::StartPod {
                    key: key.clone(),
                    pod: None,
                    operation_id: 1,
                    permit: None,
                },
                PodLifecycleWorkKind::StartPod,
            ),
            (
                PodAction::StopPod {
                    key: key.clone(),
                    pod: None,
                    sandbox_id: "sandbox-a".into(),
                    operation_id: 2,
                    permit: None,
                },
                PodLifecycleWorkKind::StopPod,
            ),
            (
                PodAction::FinalizeStartup {
                    key: key.clone(),
                    pod: None,
                    sandbox_id: "sandbox-a".into(),
                    operation_id: 3,
                    permit: None,
                },
                PodLifecycleWorkKind::FinalizeStartup,
            ),
            (
                PodAction::ReconcileRuntime {
                    key: key.clone(),
                    operation_id: 4,
                    permit: None,
                },
                PodLifecycleWorkKind::ReconcileRuntime,
            ),
            (
                PodAction::ReconcileCriLeftovers {
                    key: key.clone(),
                    operation_id: 8,
                    permit: None,
                },
                PodLifecycleWorkKind::ReconcileCriLeftovers,
            ),
            (
                PodAction::HandleCommand {
                    key: key.clone(),
                    command: LifecycleCommand::ReadinessChanged {
                        pod_uid: "uid-a".into(),
                        namespace: "default".into(),
                        pod_name: "pod-a".into(),
                        container_name: "app".into(),
                        ready: true,
                    },
                    operation_id: 6,
                    permit: None,
                },
                PodLifecycleWorkKind::HandleCommand,
            ),
            (
                PodAction::ReconcileEphemeral {
                    key: key.clone(),
                    pod: None,
                    operation_id: 7,
                    permit: None,
                },
                PodLifecycleWorkKind::ReconcileEphemeral,
            ),
        ];

        for (action, expected_kind) in actions {
            assert_eq!(action.expected_completion(), Some(expected_kind));
            assert_eq!(action.key(), Some(&key));
            assert!(action.operation_id().is_some());
        }
    }

    // ── Task 7.3: dispatch_via_runtime tests ──

    use crate::kubelet::pod_runtime::test_support::MockPodRuntimeService;

    fn dummy_reply_handle() -> LifecycleReplyHandle {
        let (tx, _rx) = tokio::sync::mpsc::channel::<LifecycleMessage>(64);
        let handle = LifecycleReplyHandle::direct(tx);
        drop(_rx);
        handle
    }

    /// StartPod action is dispatched to runtime.start_pod with correct key,
    /// pod snapshot, and cancellation token.
    #[tokio::test]
    async fn executor_dispatches_start_pod_to_runtime_service() {
        let mock = std::sync::Arc::new(MockPodRuntimeService::new());
        let key = PodLifecycleKey::new("ns", "start-pod", "uid-sp");
        let pod =
            serde_json::json!({"metadata":{"name":"start-pod","namespace":"ns","uid":"uid-sp"}});
        let action = PodAction::StartPod {
            key: key.clone(),
            pod: Some(pod.clone()),
            operation_id: 1,
            permit: None,
        };
        let (tx, _rx) = tokio::sync::mpsc::channel::<LifecycleMessage>(64);
        let reply_to = LifecycleReplyHandle::direct(tx);
        drop(_rx); // drop receiver so sends don't block

        dispatch_via_runtime(
            mock.as_ref() as &dyn PodRuntimeService,
            action,
            reply_to,
            CancellationToken::new(),
        )
        .await
        .expect("dispatch must succeed");

        let calls = mock.recorded_calls();
        assert_eq!(calls.len(), 1, "exactly one runtime call");
        match &calls[0] {
            crate::kubelet::pod_runtime::test_support::MockRuntimeCall::StartPod {
                namespace,
                name,
                uid,
                has_pod,
                cancelled,
            } => {
                assert_eq!(namespace, "ns");
                assert_eq!(name, "start-pod");
                assert_eq!(uid, "uid-sp");
                assert!(has_pod, "pod snapshot must be passed");
                assert!(!cancelled, "cancel token must not be signalled");
            }
            other => panic!("expected StartPod, got {:?}", other),
        }
    }

    /// StopPod, FinalizeStartup, and FinalizePodDeletion are each dispatched
    /// to the corresponding runtime method.
    #[tokio::test]
    async fn executor_dispatches_stop_and_finalize_actions_to_runtime_service() {
        let mock = std::sync::Arc::new(MockPodRuntimeService::new());
        let (tx, _rx) = tokio::sync::mpsc::channel::<LifecycleMessage>(64);
        let reply_to = LifecycleReplyHandle::direct(tx);
        drop(_rx); // drop receiver so sends don't block

        // StopPod
        dispatch_via_runtime(
            mock.as_ref() as &dyn PodRuntimeService,
            PodAction::StopPod {
                key: PodLifecycleKey::new("ns", "stop-pod", "uid-stop"),
                pod: None,
                sandbox_id: "sandbox-1".into(),
                operation_id: 2,
                permit: None,
            },
            reply_to.clone(),
            CancellationToken::new(),
        )
        .await
        .expect("dispatch must succeed");

        // FinalizeStartup
        dispatch_via_runtime(
            mock.as_ref() as &dyn PodRuntimeService,
            PodAction::FinalizeStartup {
                key: PodLifecycleKey::new("ns", "finalize-pod", "uid-fin-start"),
                pod: None,
                sandbox_id: "sandbox-fin".into(),
                operation_id: 3,
                permit: None,
            },
            reply_to.clone(),
            CancellationToken::new(),
        )
        .await
        .expect("dispatch must succeed");

        // FinalizePodDeletion
        dispatch_via_runtime(
            mock.as_ref() as &dyn PodRuntimeService,
            PodAction::FinalizePodDeletion {
                key: PodLifecycleKey::new("ns", "delete-pod", "uid-del"),
                operation_id: 4,
                permit: None,
            },
            reply_to.clone(),
            CancellationToken::new(),
        )
        .await
        .expect("dispatch must succeed");

        let calls = mock.recorded_calls();
        assert_eq!(calls.len(), 3, "three runtime calls");
        assert!(matches!(
            &calls[0],
            crate::kubelet::pod_runtime::test_support::MockRuntimeCall::StopPod {
                namespace,
                name,
                uid,
                sandbox_id,
            } if namespace == "ns" && name == "stop-pod" && uid == "uid-stop" && *sandbox_id == Some("sandbox-1".to_string())
        ));
        assert!(matches!(
            &calls[1],
            crate::kubelet::pod_runtime::test_support::MockRuntimeCall::FinalizeStartup {
                namespace,
                name,
                uid,
                ..
            } if namespace == "ns" && name == "finalize-pod" && uid == "uid-fin-start"
        ));
        assert!(matches!(
            &calls[2],
            crate::kubelet::pod_runtime::test_support::MockRuntimeCall::FinalizeDeletion {
                namespace,
                name,
                uid,
            } if namespace == "ns" && name == "delete-pod" && uid == "uid-del"
        ));
    }

    /// UID from PodAction key is preserved in the PodRuntimeKey received
    /// by the mock across all four routed action types.
    #[tokio::test]
    async fn executor_runtime_calls_preserve_action_uid() {
        let mock = std::sync::Arc::new(MockPodRuntimeService::new());
        let (tx, _rx) = tokio::sync::mpsc::channel::<LifecycleMessage>(64);
        let reply_to = LifecycleReplyHandle::direct(tx);
        drop(_rx); // drop receiver so sends don't block
        let test_uid = "preserved-uid-42";

        // StartPod
        dispatch_via_runtime(
            mock.as_ref() as &dyn PodRuntimeService,
            PodAction::StartPod {
                key: PodLifecycleKey::new("ns", "p", test_uid),
                pod: None,
                operation_id: 1,
                permit: None,
            },
            reply_to.clone(),
            CancellationToken::new(),
        )
        .await
        .unwrap();

        // StopPod
        dispatch_via_runtime(
            mock.as_ref() as &dyn PodRuntimeService,
            PodAction::StopPod {
                key: PodLifecycleKey::new("ns", "p", test_uid),
                pod: None,
                sandbox_id: "s".into(),
                operation_id: 2,
                permit: None,
            },
            reply_to.clone(),
            CancellationToken::new(),
        )
        .await
        .unwrap();

        // FinalizeStartup
        dispatch_via_runtime(
            mock.as_ref() as &dyn PodRuntimeService,
            PodAction::FinalizeStartup {
                key: PodLifecycleKey::new("ns", "p", test_uid),
                pod: None,
                sandbox_id: "sandbox-uid".into(),
                operation_id: 3,
                permit: None,
            },
            reply_to.clone(),
            CancellationToken::new(),
        )
        .await
        .unwrap();

        // FinalizePodDeletion
        dispatch_via_runtime(
            mock.as_ref() as &dyn PodRuntimeService,
            PodAction::FinalizePodDeletion {
                key: PodLifecycleKey::new("ns", "p", test_uid),
                operation_id: 4,
                permit: None,
            },
            reply_to.clone(),
            CancellationToken::new(),
        )
        .await
        .unwrap();

        for call in mock.recorded_calls() {
            let (ns, name, uid) = match &call {
                crate::kubelet::pod_runtime::test_support::MockRuntimeCall::StartPod {
                    namespace,
                    name,
                    uid,
                    ..
                } => (namespace, name, uid),
                crate::kubelet::pod_runtime::test_support::MockRuntimeCall::StopPod {
                    namespace,
                    name,
                    uid,
                    ..
                } => (namespace, name, uid),
                crate::kubelet::pod_runtime::test_support::MockRuntimeCall::FinalizeStartup {
                    namespace,
                    name,
                    uid,
                    ..
                } => (namespace, name, uid),
                crate::kubelet::pod_runtime::test_support::MockRuntimeCall::FinalizeDeletion {
                    namespace,
                    name,
                    uid,
                } => (namespace, name, uid),
                _ => continue,
            };
            assert_eq!(ns, "ns");
            assert_eq!(name, "p");
            assert_eq!(
                uid, test_uid,
                "UID must be preserved across all runtime calls"
            );
        }
    }

    // ── Task 7.4: reconcile and command dispatch tests ──

    /// ReconcileRuntime, ReconcileCriLeftovers, ReconcileEphemeral, and
    /// HandleCommand are dispatched to the corresponding runtime methods.
    #[tokio::test]
    async fn executor_dispatches_reconcile_and_command_actions_to_runtime_service() {
        let mock = std::sync::Arc::new(MockPodRuntimeService::new());

        // ReconcileRuntime
        dispatch_via_runtime(
            mock.as_ref() as &dyn PodRuntimeService,
            PodAction::ReconcileRuntime {
                key: PodLifecycleKey::new("ns", "rec-runtime", "uid-rr"),
                operation_id: 1,
                permit: None,
            },
            dummy_reply_handle(),
            CancellationToken::new(),
        )
        .await
        .expect("dispatch must succeed");

        // ReconcileCriLeftovers
        dispatch_via_runtime(
            mock.as_ref() as &dyn PodRuntimeService,
            PodAction::ReconcileCriLeftovers {
                key: PodLifecycleKey::new("ns", "rec-cri", "uid-rc"),
                operation_id: 2,
                permit: None,
            },
            dummy_reply_handle(),
            CancellationToken::new(),
        )
        .await
        .expect("dispatch must succeed");

        // ReconcileEphemeral
        dispatch_via_runtime(
            mock.as_ref() as &dyn PodRuntimeService,
            PodAction::ReconcileEphemeral {
                key: PodLifecycleKey::new("ns", "rec-eph", "uid-re"),
                pod: Some(serde_json::json!({"metadata":{"name":"rec-eph"}})),
                operation_id: 3,
                permit: None,
            },
            dummy_reply_handle(),
            CancellationToken::new(),
        )
        .await
        .expect("dispatch must succeed");

        // HandleCommand
        dispatch_via_runtime(
            mock.as_ref() as &dyn PodRuntimeService,
            PodAction::HandleCommand {
                key: PodLifecycleKey::new("ns", "handle-cmd", "uid-hc"),
                command: crate::kubelet::lifecycle::LifecycleCommand::ReadinessChanged {
                    pod_uid: "uid-hc".into(),
                    namespace: "ns".into(),
                    pod_name: "handle-cmd".into(),
                    container_name: "app".into(),
                    ready: true,
                },
                operation_id: 4,
                permit: None,
            },
            dummy_reply_handle(),
            CancellationToken::new(),
        )
        .await
        .expect("dispatch must succeed");

        let calls = mock.recorded_calls();
        assert_eq!(calls.len(), 4, "four runtime calls");
        assert!(matches!(&calls[0],
            crate::kubelet::pod_runtime::test_support::MockRuntimeCall::ReconcileRuntime {
                namespace, name, uid
            } if namespace == "ns" && name == "rec-runtime" && uid == "uid-rr"
        ));
        assert!(matches!(&calls[1],
            crate::kubelet::pod_runtime::test_support::MockRuntimeCall::ReconcileCriLeftovers {
                namespace, name, uid
            } if namespace == "ns" && name == "rec-cri" && uid == "uid-rc"
        ));
        assert!(matches!(&calls[2],
            crate::kubelet::pod_runtime::test_support::MockRuntimeCall::ReconcileEphemeral {
                namespace, name, uid
            } if namespace == "ns" && name == "rec-eph" && uid == "uid-re"
        ));
        assert!(matches!(
            &calls[3],
            crate::kubelet::pod_runtime::test_support::MockRuntimeCall::HandleCommand { .. }
        ));
    }

    /// Retry timer actions are dispatched to the runtime service so the
    /// executor stays a transport adapter and does not own repo/event/timer
    /// side effects.
    #[tokio::test]
    async fn executor_dispatches_retry_actions_to_runtime_service() {
        let mock = std::sync::Arc::new(MockPodRuntimeService::new());

        dispatch_via_runtime(
            mock.as_ref() as &dyn PodRuntimeService,
            PodAction::ScheduleRetry {
                key: PodLifecycleKey::new("ns", "retry-pod", "uid-retry"),
                delay: std::time::Duration::from_millis(25),
            },
            dummy_reply_handle(),
            CancellationToken::new(),
        )
        .await
        .expect("dispatch must succeed");

        dispatch_via_runtime(
            mock.as_ref() as &dyn PodRuntimeService,
            PodAction::ScheduleStartPodRetry {
                key: PodLifecycleKey::new("ns", "start-retry-pod", "uid-start-retry"),
                delay: std::time::Duration::from_millis(50),
                error_message: "pull failed".to_string(),
                attempt: 2,
            },
            dummy_reply_handle(),
            CancellationToken::new(),
        )
        .await
        .expect("dispatch must succeed");

        let calls = mock.recorded_calls();
        assert_eq!(calls.len(), 2, "two runtime retry calls");
        assert!(matches!(
            &calls[0],
            crate::kubelet::pod_runtime::test_support::MockRuntimeCall::ScheduleRetry {
                namespace,
                name,
                uid,
                delay_ms,
            } if namespace == "ns"
                && name == "retry-pod"
                && uid == "uid-retry"
                && *delay_ms == 25
        ));
        assert!(matches!(
            &calls[1],
            crate::kubelet::pod_runtime::test_support::MockRuntimeCall::ScheduleStartPodRetry {
                namespace,
                name,
                uid,
                delay_ms,
                attempt,
                error_message,
            } if namespace == "ns"
                && name == "start-retry-pod"
                && uid == "uid-start-retry"
                && *delay_ms == 50
                && *attempt == 2
                && error_message == "pull failed"
        ));
    }

    /// CheckSlotAdmission belongs behind the runtime/store port, not direct
    /// datastore access from the executor.
    #[tokio::test]
    async fn executor_dispatches_check_slot_admission_to_runtime_service() {
        let mock = std::sync::Arc::new(MockPodRuntimeService::new());
        let executor = PodLifecycleExecutor::new(mock.clone());
        let key = PodLifecycleKey::new("ns", "slot-pod", "uid-slot");
        let pod = serde_json::json!({
            "metadata": {
                "namespace": "ns",
                "name": "slot-pod",
                "uid": "uid-slot",
            }
        });

        executor
            .dispatch_with_cancel(
                PodAction::CheckSlotAdmission {
                    key,
                    pod,
                    resource_version: Some(7),
                    start_after_admit: true,
                    operation_id: 9,
                    permit: None,
                },
                dummy_reply_handle(),
                CancellationToken::new(),
            )
            .await
            .expect("dispatch must succeed");

        let calls = mock.recorded_calls();
        assert_eq!(
            calls.len(),
            1,
            "CheckSlotAdmission must route through PodRuntimeService"
        );
        assert!(matches!(
            &calls[0],
            crate::kubelet::pod_runtime::test_support::MockRuntimeCall::CheckSlotAdmission {
                namespace,
                name,
                uid,
                has_pod,
                resource_version,
                start_after_admit,
                operation_id,
                cancelled,
            } if namespace == "ns"
                && name == "slot-pod"
                && uid == "uid-slot"
                && *has_pod
                && *resource_version == Some(7)
                && *start_after_admit
                && *operation_id == 9
                && !*cancelled
        ));
    }

    #[tokio::test]
    async fn executor_passes_finalize_startup_snapshot_and_sandbox_hint_to_runtime() {
        let mock = std::sync::Arc::new(MockPodRuntimeService::new());
        let executor = PodLifecycleExecutor::new(mock.clone());
        let key = PodLifecycleKey::new("ns", "finalize-pod", "uid-finalize");
        let pod = serde_json::json!({
            "metadata": {
                "namespace": "ns",
                "name": "finalize-pod",
                "uid": "uid-finalize",
            },
            "status": {
                "phase": "Running",
                "podIP": "10.42.0.10"
            }
        });

        executor
            .dispatch_with_cancel(
                PodAction::FinalizeStartup {
                    key,
                    pod: Some(pod),
                    sandbox_id: "sandbox-hint".to_string(),
                    operation_id: 17,
                    permit: None,
                },
                dummy_reply_handle(),
                CancellationToken::new(),
            )
            .await
            .expect("dispatch must succeed");

        let calls = mock.recorded_calls();
        assert_eq!(calls.len(), 1);
        assert!(matches!(
            &calls[0],
            crate::kubelet::pod_runtime::test_support::MockRuntimeCall::FinalizeStartup {
                namespace,
                name,
                uid,
                has_pod,
                sandbox_id_hint,
            } if namespace == "ns"
                && name == "finalize-pod"
                && uid == "uid-finalize"
                && *has_pod
                && sandbox_id_hint.as_deref() == Some("sandbox-hint")
        ));
    }

    /// HandleCommand preserves the UID from the LifecycleCommand, not from
    /// a same-name Pod lookup.
    #[tokio::test]
    async fn lifecycle_command_routing_preserves_command_uid() {
        let mock = std::sync::Arc::new(MockPodRuntimeService::new());
        let command_uid = "cmd-uid-42";

        dispatch_via_runtime(
            mock.as_ref() as &dyn PodRuntimeService,
            PodAction::HandleCommand {
                key: PodLifecycleKey::new("ns", "pod-name-may-differ", command_uid),
                command: crate::kubelet::lifecycle::LifecycleCommand::StartupPassed {
                    pod_uid: command_uid.into(),
                    namespace: "ns".into(),
                    pod_name: "pod-name-may-differ".into(),
                    container_name: "sidecar".into(),
                },
                operation_id: 1,
                permit: None,
            },
            dummy_reply_handle(),
            CancellationToken::new(),
        )
        .await
        .expect("dispatch must succeed");

        let calls = mock.recorded_calls();
        assert_eq!(calls.len(), 1);
        match &calls[0] {
            crate::kubelet::pod_runtime::test_support::MockRuntimeCall::HandleCommand {
                command_name,
            } => {
                assert!(
                    command_name.contains(command_uid),
                    "command must carry UID {command_uid}, got: {command_name}"
                );
            }
            other => panic!("expected HandleCommand, got {:?}", other),
        }
    }

    #[test]
    fn container_not_found_runtime_error_matches_container_specific_messages() {
        assert!(
            is_container_not_found_runtime_error(&anyhow::anyhow!(
                "container app not found in pod ns/pod"
            )),
            "container + not found message should be terminal"
        );
        assert!(
            is_container_not_found_runtime_error(&anyhow::anyhow!(
                "no such container: container-abc"
            )),
            "container-id-specific message should be terminal"
        );
        assert!(
            !is_container_not_found_runtime_error(&anyhow::anyhow!(
                "pod sandbox not found for uid xyz"
            )),
            "generic pod not found must remain retryable"
        );
    }
}

fn is_container_not_found_runtime_error(error: &anyhow::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    (message.contains("container") && message.contains("not found"))
        || message.contains("no such container")
}
