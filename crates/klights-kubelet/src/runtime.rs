//! Focused runtime port consumed by pod lifecycle actors.

use tokio_util::sync::CancellationToken;

use crate::lifecycle::LifecycleCommand;
use crate::pod_lifecycle_router::LifecycleReplyHandle;

pub(crate) mod active_deadline;
pub(crate) mod cluster_policy;
pub mod cri;
pub mod events;
pub(crate) mod filesystem;
pub mod hooks;
pub(crate) mod hostports;
pub mod images;
pub(crate) mod init_container_status;
pub(crate) mod lifecycle_commands;
pub(crate) mod network;
pub(crate) mod orphan_stop;
pub(crate) mod pod_identity;
pub(crate) mod probes;
pub mod recovery;
pub(crate) mod retry;
pub(crate) mod service;
pub(crate) mod slot_admission;
pub(crate) mod startup_finalization;
pub(crate) mod status_emitter;
pub(crate) mod status_helpers;
pub(crate) mod status_projection;
pub mod store;
pub(crate) mod volumes;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

#[cfg(test)]
mod test_repository;
#[cfg(test)]
mod tests;

pub use crate::runtime_reconcile_hint::RuntimeReconcileHint;
pub use crate::runtime_types::{
    PodDeletionFinalizeResult, PodFinalizeStartupResult, PodOwnershipError, PodRuntimeKey,
    PodStartResult,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PodStopMode {
    Graceful,
    Forced,
}

#[derive(Clone, Debug)]
pub struct PodStopRequest {
    pub key: PodRuntimeKey,
    pub pod: Option<serde_json::Value>,
    pub sandbox_id: Option<String>,
    pub deletion_deadline: Option<chrono::DateTime<chrono::Utc>>,
    pub mode: PodStopMode,
    pub operation_id: u64,
    pub cancel: CancellationToken,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PodStopResult {
    Completed,
    Cancelled,
}

pub fn remaining_stop_grace(
    deletion_deadline: Option<chrono::DateTime<chrono::Utc>>,
    now: chrono::DateTime<chrono::Utc>,
    mode: PodStopMode,
) -> std::time::Duration {
    if mode == PodStopMode::Forced {
        return std::time::Duration::ZERO;
    }
    deletion_deadline
        .and_then(|deadline| (deadline - now).to_std().ok())
        .unwrap_or_default()
}

pub fn remaining_stop_grace_seconds(
    deletion_deadline: Option<chrono::DateTime<chrono::Utc>>,
    now: chrono::DateTime<chrono::Utc>,
    mode: PodStopMode,
) -> i64 {
    let remaining = remaining_stop_grace(deletion_deadline, now, mode);
    if remaining.is_zero() {
        return 0;
    }
    let rounded_up = remaining
        .as_secs()
        .saturating_add(u64::from(remaining.subsec_nanos() != 0));
    rounded_up.min(i64::MAX as u64) as i64
}

#[cfg(test)]
mod stop_deadline_tests {
    use super::*;

    #[test]
    fn remaining_grace_rounds_up_and_forced_or_elapsed_are_zero() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-08-08T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let half_second = now + chrono::Duration::milliseconds(500);
        assert_eq!(
            remaining_stop_grace_seconds(Some(half_second), now, PodStopMode::Graceful),
            1
        );
        assert_eq!(
            remaining_stop_grace_seconds(Some(now), now, PodStopMode::Graceful),
            0
        );
        assert_eq!(
            remaining_stop_grace_seconds(Some(half_second), now, PodStopMode::Forced),
            0
        );
        assert_eq!(
            remaining_stop_grace_seconds(None, now, PodStopMode::Graceful),
            0
        );
    }
}

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

    async fn stop_pod(&self, request: PodStopRequest) -> anyhow::Result<PodStopResult>;

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
