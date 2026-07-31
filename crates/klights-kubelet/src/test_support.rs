use std::sync::Mutex;

use tokio_util::sync::CancellationToken;

use crate::lifecycle::LifecycleCommand;
use crate::pod_lifecycle_core::message::{LifecycleMessage, PodLifecycleKey};
use crate::pod_lifecycle_router::LifecycleReplyHandle;
use crate::runtime::{
    PodDeletionFinalizeResult, PodFinalizeStartupResult, PodOwnershipError, PodRuntimeKey,
    PodRuntimeService, PodSlotAdmissionRequest, PodStartResult, RuntimeReconcileHint,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MockRuntimeCall {
    StartPod {
        namespace: String,
        name: String,
        uid: String,
        has_pod: bool,
        cancelled: bool,
    },
    StopPod {
        namespace: String,
        name: String,
        uid: String,
        sandbox_id: Option<String>,
    },
    FinalizeStartup {
        namespace: String,
        name: String,
        uid: String,
        has_pod: bool,
        sandbox_id_hint: Option<String>,
    },
    FinalizeDeletion {
        namespace: String,
        name: String,
        uid: String,
    },
    ReconcileRuntime {
        namespace: String,
        name: String,
        uid: String,
        hint_container_ids: Vec<String>,
    },
    ReconcileCriLeftovers {
        namespace: String,
        name: String,
        uid: String,
    },
    ReconcileEphemeral {
        namespace: String,
        name: String,
        uid: String,
    },
    CheckSlotAdmission {
        namespace: String,
        name: String,
        uid: String,
        has_pod: bool,
        resource_version: Option<i64>,
        start_after_admit: bool,
        operation_id: u64,
        cancelled: bool,
    },
    HandleCommand {
        command_name: String,
    },
    ScheduleRetry {
        namespace: String,
        name: String,
        uid: String,
        delay_ms: u128,
    },
    ScheduleStartPodRetry {
        namespace: String,
        name: String,
        uid: String,
        delay_ms: u128,
        attempt: u32,
        error_message: String,
    },
}

pub struct MockPodRuntimeService {
    calls: Mutex<Vec<MockRuntimeCall>>,
    start_result: Mutex<PodStartResult>,
    finalize_startup_result: Mutex<PodFinalizeStartupResult>,
    finalize_result: Mutex<PodDeletionFinalizeResult>,
    fail_method: Mutex<Option<String>>,
    stop_ownership_error: Mutex<Option<PodOwnershipError>>,
    start_pod_cancel: Mutex<Option<CancellationToken>>,
}

impl Default for MockPodRuntimeService {
    fn default() -> Self {
        Self::new()
    }
}

impl MockPodRuntimeService {
    pub fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            start_result: Mutex::new(PodStartResult::Started { sandbox_id: None }),
            finalize_startup_result: Mutex::new(PodFinalizeStartupResult::Unconfirmed),
            finalize_result: Mutex::new(PodDeletionFinalizeResult::DeletedOrAlreadyGone),
            fail_method: Mutex::new(None),
            stop_ownership_error: Mutex::new(None),
            start_pod_cancel: Mutex::new(None),
        }
    }

    pub fn set_finalize_result(&self, result: PodDeletionFinalizeResult) {
        *self.finalize_result.lock().unwrap() = result;
    }

    pub fn set_stop_pod_ownership_error(&self, local_node: &str, target_node: Option<&str>) {
        *self.stop_ownership_error.lock().unwrap() = Some(PodOwnershipError {
            local_node: local_node.to_string(),
            target_node: target_node.map(str::to_string),
        });
    }

    pub fn recorded_calls(&self) -> Vec<MockRuntimeCall> {
        self.calls.lock().unwrap().clone()
    }

    fn check_fail(&self, method: &str) -> anyhow::Result<()> {
        if self.fail_method.lock().unwrap().as_deref() == Some(method) {
            anyhow::bail!("injected failure for: {method}");
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl PodRuntimeService for MockPodRuntimeService {
    async fn start_pod(
        &self,
        key: PodRuntimeKey,
        pod: Option<serde_json::Value>,
        cancel: CancellationToken,
    ) -> anyhow::Result<PodStartResult> {
        self.check_fail("start_pod")?;
        *self.start_pod_cancel.lock().unwrap() = Some(cancel.clone());
        self.calls.lock().unwrap().push(MockRuntimeCall::StartPod {
            namespace: key.namespace,
            name: key.name,
            uid: key.uid,
            has_pod: pod.is_some(),
            cancelled: cancel.is_cancelled(),
        });
        Ok(self.start_result.lock().unwrap().clone())
    }

    async fn stop_pod(
        &self,
        key: PodRuntimeKey,
        _pod: Option<serde_json::Value>,
        sandbox_id: Option<String>,
    ) -> anyhow::Result<()> {
        self.calls.lock().unwrap().push(MockRuntimeCall::StopPod {
            namespace: key.namespace,
            name: key.name,
            uid: key.uid,
            sandbox_id,
        });
        if let Some(error) = self.stop_ownership_error.lock().unwrap().take() {
            return Err(anyhow::Error::new(error));
        }
        self.check_fail("stop_pod")
    }

    async fn finalize_startup(
        &self,
        key: PodRuntimeKey,
        pod: Option<serde_json::Value>,
        sandbox_id_hint: Option<String>,
    ) -> anyhow::Result<PodFinalizeStartupResult> {
        self.check_fail("finalize_startup")?;
        self.calls
            .lock()
            .unwrap()
            .push(MockRuntimeCall::FinalizeStartup {
                namespace: key.namespace,
                name: key.name,
                uid: key.uid,
                has_pod: pod.is_some(),
                sandbox_id_hint,
            });
        Ok(self.finalize_startup_result.lock().unwrap().clone())
    }

    async fn finalize_deletion(
        &self,
        key: PodRuntimeKey,
    ) -> anyhow::Result<PodDeletionFinalizeResult> {
        self.check_fail("finalize_deletion")?;
        self.calls
            .lock()
            .unwrap()
            .push(MockRuntimeCall::FinalizeDeletion {
                namespace: key.namespace,
                name: key.name,
                uid: key.uid,
            });
        Ok(self.finalize_result.lock().unwrap().clone())
    }

    async fn reconcile_runtime(
        &self,
        key: PodRuntimeKey,
        hint: RuntimeReconcileHint,
    ) -> anyhow::Result<()> {
        self.check_fail("reconcile_runtime")?;
        self.calls
            .lock()
            .unwrap()
            .push(MockRuntimeCall::ReconcileRuntime {
                namespace: key.namespace,
                name: key.name,
                uid: key.uid,
                hint_container_ids: hint.container_ids().map(str::to_string).collect(),
            });
        Ok(())
    }

    async fn reconcile_cri_leftovers(&self, key: PodRuntimeKey) -> anyhow::Result<()> {
        self.check_fail("reconcile_cri_leftovers")?;
        self.calls
            .lock()
            .unwrap()
            .push(MockRuntimeCall::ReconcileCriLeftovers {
                namespace: key.namespace,
                name: key.name,
                uid: key.uid,
            });
        Ok(())
    }

    async fn reconcile_ephemeral(
        &self,
        key: PodRuntimeKey,
        _pod: Option<serde_json::Value>,
    ) -> anyhow::Result<()> {
        self.check_fail("reconcile_ephemeral")?;
        self.calls
            .lock()
            .unwrap()
            .push(MockRuntimeCall::ReconcileEphemeral {
                namespace: key.namespace,
                name: key.name,
                uid: key.uid,
            });
        Ok(())
    }

    async fn handle_lifecycle_command(&self, command: LifecycleCommand) -> anyhow::Result<()> {
        self.check_fail("handle_lifecycle_command")?;
        self.calls
            .lock()
            .unwrap()
            .push(MockRuntimeCall::HandleCommand {
                command_name: format!("{command:?}").chars().take(60).collect(),
            });
        Ok(())
    }

    async fn check_slot_admission(
        &self,
        request: PodSlotAdmissionRequest,
        reply_to: LifecycleReplyHandle,
        cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        self.check_fail("check_slot_admission")?;
        let PodSlotAdmissionRequest {
            key,
            pod,
            resource_version,
            start_after_admit,
            operation_id,
        } = request;
        self.calls
            .lock()
            .unwrap()
            .push(MockRuntimeCall::CheckSlotAdmission {
                namespace: key.namespace.clone(),
                name: key.name.clone(),
                uid: key.uid.clone(),
                has_pod: !pod.is_null(),
                resource_version,
                start_after_admit,
                operation_id,
                cancelled: cancel.is_cancelled(),
            });
        let _ = reply_to
            .route(LifecycleMessage::SlotAdmissionGranted {
                key: PodLifecycleKey::new(&key.namespace, &key.name, &key.uid),
                operation_id,
                pod,
                resource_version,
                start_after_admit,
            })
            .await;
        Ok(())
    }

    async fn schedule_retry(
        &self,
        key: PodRuntimeKey,
        delay: std::time::Duration,
        _reply_to: LifecycleReplyHandle,
    ) -> anyhow::Result<()> {
        self.check_fail("schedule_retry")?;
        self.calls
            .lock()
            .unwrap()
            .push(MockRuntimeCall::ScheduleRetry {
                namespace: key.namespace,
                name: key.name,
                uid: key.uid,
                delay_ms: delay.as_millis(),
            });
        Ok(())
    }

    async fn schedule_start_pod_retry(
        &self,
        key: PodRuntimeKey,
        delay: std::time::Duration,
        error_message: String,
        attempt: u32,
        _reply_to: LifecycleReplyHandle,
    ) -> anyhow::Result<()> {
        self.check_fail("schedule_start_pod_retry")?;
        self.calls
            .lock()
            .unwrap()
            .push(MockRuntimeCall::ScheduleStartPodRetry {
                namespace: key.namespace,
                name: key.name,
                uid: key.uid,
                delay_ms: delay.as_millis(),
                attempt,
                error_message,
            });
        Ok(())
    }
}
