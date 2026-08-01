//! Focused runtime port consumed by pod lifecycle actors.

use tokio_util::sync::CancellationToken;

use crate::lifecycle::LifecycleCommand;
use crate::pod_lifecycle_router::LifecycleReplyHandle;

pub mod cri;
pub mod hooks;
pub mod images;
pub mod recovery;
pub mod store;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

pub use crate::runtime_reconcile_hint::RuntimeReconcileHint;
pub use crate::runtime_types::{
    PodDeletionFinalizeResult, PodFinalizeStartupResult, PodOwnershipError, PodRuntimeKey,
    PodStartResult,
};

/// Request object for UID-qualified pod slot admission checks.
#[derive(Clone, Debug)]
pub struct PodSlotAdmissionRequest {
    pub key: PodRuntimeKey,
    pub pod: serde_json::Value,
    pub resource_version: Option<i64>,
    pub start_after_admit: bool,
    pub operation_id: u64,
}

/// Backend-neutral lifecycle runtime port.
///
/// Implementations remain root-composed; the lifecycle crate owns only this
/// focused, UID-bearing interface.
#[async_trait::async_trait]
pub trait PodRuntimeService: Send + Sync {
    async fn start_pod(
        &self,
        key: PodRuntimeKey,
        pod: Option<serde_json::Value>,
        cancel: CancellationToken,
    ) -> anyhow::Result<PodStartResult>;

    async fn stop_pod(
        &self,
        key: PodRuntimeKey,
        pod: Option<serde_json::Value>,
        sandbox_id: Option<String>,
    ) -> anyhow::Result<()>;

    async fn finalize_startup(
        &self,
        key: PodRuntimeKey,
        pod: Option<serde_json::Value>,
        sandbox_id_hint: Option<String>,
    ) -> anyhow::Result<PodFinalizeStartupResult>;

    async fn finalize_deletion(
        &self,
        key: PodRuntimeKey,
    ) -> anyhow::Result<PodDeletionFinalizeResult>;

    async fn reconcile_runtime(
        &self,
        key: PodRuntimeKey,
        hint: RuntimeReconcileHint,
    ) -> anyhow::Result<()>;

    async fn reconcile_cri_leftovers(&self, key: PodRuntimeKey) -> anyhow::Result<()>;

    async fn reconcile_ephemeral(
        &self,
        key: PodRuntimeKey,
        pod: Option<serde_json::Value>,
    ) -> anyhow::Result<()>;

    async fn handle_lifecycle_command(&self, command: LifecycleCommand) -> anyhow::Result<()>;

    async fn check_slot_admission(
        &self,
        request: PodSlotAdmissionRequest,
        reply_to: LifecycleReplyHandle,
        cancel: CancellationToken,
    ) -> anyhow::Result<()>;

    async fn schedule_retry(
        &self,
        key: PodRuntimeKey,
        delay: std::time::Duration,
        reply_to: LifecycleReplyHandle,
    ) -> anyhow::Result<()>;

    async fn schedule_start_pod_retry(
        &self,
        key: PodRuntimeKey,
        delay: std::time::Duration,
        error_message: String,
        attempt: u32,
        reply_to: LifecycleReplyHandle,
    ) -> anyhow::Result<()>;
}

pub(crate) fn retry_backoff(attempts: u32) -> std::time::Duration {
    let seconds = 2_u64
        .saturating_mul(2_u64.saturating_pow(attempts.saturating_sub(1)))
        .min(60);
    std::time::Duration::from_secs(seconds.max(2))
}
