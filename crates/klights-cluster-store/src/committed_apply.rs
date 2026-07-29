//! Privileged committed-Raft apply and its durable ledger reads.
//!
//! Normal API/controller command submission deliberately does not appear in
//! this module. The apply capability is reserved for a Raft state machine that
//! already holds a committed canonical [`LogApplyCommit`]. Ledger reads are a
//! separate capability so read-only diagnostics cannot obtain apply rights.

use std::fmt;
use std::future::Future;
use std::pin::Pin;

use klights_cluster_core::{
    CommittedApplyOutcome, LogApplyAppliedOutboxRow, LogApplyCommit, OutboxStreamWatermark,
    PodEndpointEffect, Resource, WatchReplayPosition,
};

/// Persistence failure returned by committed apply or its ledger reads.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommittedApplyError {
    PersistenceFailed { message: String },
    CorruptData { message: String },
    UnsupportedMode { message: String },
    Retryable { message: String },
    Timeout,
    Cancelled,
}

impl CommittedApplyError {
    pub fn persistence_failed(message: impl Into<String>) -> Self {
        Self::PersistenceFailed {
            message: message.into(),
        }
    }
}

impl fmt::Display for CommittedApplyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PersistenceFailed { message }
            | Self::CorruptData { message }
            | Self::UnsupportedMode { message }
            | Self::Retryable { message } => formatter.write_str(message),
            Self::Timeout => formatter.write_str("committed apply timed out"),
            Self::Cancelled => formatter.write_str("committed apply was cancelled"),
        }
    }
}

impl std::error::Error for CommittedApplyError {}

/// Persistence request carrying one already-committed canonical Raft delta.
#[derive(Clone, Debug, PartialEq)]
pub struct CommittedRaftApplyRequest {
    commit: LogApplyCommit,
}

impl CommittedRaftApplyRequest {
    pub fn new(commit: LogApplyCommit) -> Self {
        Self { commit }
    }

    pub const fn commit(&self) -> &LogApplyCommit {
        &self.commit
    }

    pub fn into_commit(self) -> LogApplyCommit {
        self.commit
    }
}

/// Durable state-machine receipt for one committed apply attempt.
///
/// Terminal optimistic-concurrency rejection is a committed state-machine
/// result rather than a persistence failure. It therefore remains in the
/// receipt, preserving learner catch-up and idempotent replay semantics.
#[derive(Clone, Debug)]
pub struct CommittedRaftApplyReceipt {
    outcome: CommittedApplyOutcome,
    returned_resource: Option<Resource>,
    pod_endpoint_effect: PodEndpointEffect,
}

impl CommittedRaftApplyReceipt {
    pub fn new(outcome: CommittedApplyOutcome, pod_endpoint_effect: PodEndpointEffect) -> Self {
        let returned_resource = match &outcome {
            CommittedApplyOutcome::Visible { resource, .. } => resource.clone(),
            _ => None,
        };
        Self {
            outcome,
            returned_resource,
            pod_endpoint_effect,
        }
    }
    pub fn with_returned_resource(mut self, resource: Option<Resource>) -> Self {
        self.returned_resource = resource;
        self
    }
    pub const fn outcome(&self) -> &CommittedApplyOutcome {
        &self.outcome
    }
    pub fn into_outcome(self) -> CommittedApplyOutcome {
        self.outcome
    }
    pub const fn pod_endpoint_effect(&self) -> PodEndpointEffect {
        self.pod_endpoint_effect
    }
    pub fn into_parts(self) -> (CommittedApplyOutcome, Option<Resource>, PodEndpointEffect) {
        (
            self.outcome,
            self.returned_resource,
            self.pod_endpoint_effect,
        )
    }
    pub const fn applied_resource_version(&self) -> Option<i64> {
        match &self.outcome {
            CommittedApplyOutcome::Visible {
                resource_version, ..
            }
            | CommittedApplyOutcome::NoPublicChange {
                resource_version, ..
            } => Some(*resource_version),
            CommittedApplyOutcome::Rejected(_) => None,
            _ => None,
        }
    }
    pub fn terminal_rejection(&self) -> Option<&str> {
        match &self.outcome {
            CommittedApplyOutcome::Rejected(value) => Some(value.message()),
            _ => None,
        }
    }
    pub const fn applied_resource(&self) -> Option<&Resource> {
        self.returned_resource.as_ref()
    }
}

/// Opaque durable idempotency-ledger lookup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppliedOutboxLookup {
    idempotency_key: String,
}

impl AppliedOutboxLookup {
    pub fn new(idempotency_key: impl Into<String>) -> Self {
        Self {
            idempotency_key: idempotency_key.into(),
        }
    }

    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    pub fn into_idempotency_key(self) -> String {
        self.idempotency_key
    }
}

/// Heap-erased future used at the coarse committed-persistence boundary.
pub type CommittedApplyFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, CommittedApplyError>> + Send + 'a>>;

/// Raft-state-machine-only right to persist one committed logical delta.
///
/// Each invocation allocates one future at the coarse consensus/persistence
/// boundary. No per-resource stream or event-loop item is boxed here.
pub trait PrivilegedCommittedRaftApply: Send + Sync {
    fn apply_committed_raft(
        &self,
        request: CommittedRaftApplyRequest,
    ) -> CommittedApplyFuture<'_, CommittedRaftApplyReceipt>;
}

/// Read-only durable state produced by committed apply.
///
/// The canonical cluster-core values retain RV/event-ID, applied-outbox row,
/// status-stamp, and stream-watermark meaning. This capability only describes
/// persistence lookups and exposes no insert, update, delete, apply, submit,
/// or command method.
pub trait DurableApplyLedgerRead: Send + Sync {
    fn current_apply_position(&self) -> CommittedApplyFuture<'_, WatchReplayPosition>;

    fn get_applied_outbox(
        &self,
        lookup: AppliedOutboxLookup,
    ) -> CommittedApplyFuture<'_, Option<LogApplyAppliedOutboxRow>>;

    fn list_outbox_watermarks(&self) -> CommittedApplyFuture<'_, Vec<OutboxStreamWatermark>>;
}
