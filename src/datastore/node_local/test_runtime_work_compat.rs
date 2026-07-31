//! Test-only compatibility DTOs retained for legacy root regression coverage.

use anyhow::{Result, anyhow};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PodSlotAdmissionState {
    Admitted,
    Terminating,
}

impl PodSlotAdmissionState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Admitted => "Admitted",
            Self::Terminating => "Terminating",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "Admitted" => Ok(Self::Admitted),
            "Terminating" => Ok(Self::Terminating),
            other => Err(anyhow!("invalid pod slot admission state {other:?}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PodSlotAdmissionResult {
    Admitted {
        resource_version: i64,
    },
    Blocked {
        blocking_uid: String,
        blocking_node: String,
        state: PodSlotAdmissionState,
        resource_version: i64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PodSlotMutationResult {
    Changed { resource_version: i64 },
    Unchanged { resource_version: i64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PodSlotClearResult {
    Cleared {
        resource_version: i64,
    },
    NotFound,
    UidMismatch {
        blocking_uid: String,
        blocking_node: String,
        state: PodSlotAdmissionState,
        resource_version: i64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PodSlotAdmissionEvent {
    Changed {
        namespace: String,
        pod_name: String,
        pod_uid: String,
        state: PodSlotAdmissionState,
        resource_version: i64,
    },
    Cleared {
        namespace: String,
        pod_name: String,
        pod_uid: String,
        resource_version: i64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SandboxRef {
    pub namespace: String,
    pub pod_name: String,
    pub pod_uid: String,
    pub sandbox_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PodWorkqueueKind {
    Pod,
    Namespace,
}

impl PodWorkqueueKind {
    pub fn as_str(self) -> &'static str {
        match self {
            PodWorkqueueKind::Pod => "pod",
            PodWorkqueueKind::Namespace => "namespace",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "pod" => Ok(Self::Pod),
            "namespace" => Ok(Self::Namespace),
            other => Err(anyhow!("invalid pod_workqueue kind '{}'", other)),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PodWorkqueueEntry {
    pub id: i64,
    pub kind: PodWorkqueueKind,
    pub namespace: String,
    pub name: String,
    pub uid: String,
    pub payload: Value,
    pub attempt_count: i64,
    pub next_attempt_at_ms: i64,
}

#[cfg(test)]
pub use super::sqlite::{
    DeadLetterRow, OutboxInsert, OutboxRow, OutboxStats, PodStatusCheckpoint,
    RuntimeObservationCheckpoint,
};
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodRuntimeOwnershipError {
    Conflict {
        pod_uid: String,
        existing_namespace: String,
        existing_pod_name: String,
        existing_node_name: String,
        existing_sandbox_id: Option<String>,
    },
    Persistence {
        message: String,
    },
}

impl std::fmt::Display for PodRuntimeOwnershipError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Conflict { pod_uid, .. } => {
                write!(
                    formatter,
                    "pod runtime ownership conflict for UID {pod_uid}"
                )
            }
            Self::Persistence { message } => formatter.write_str(message),
        }
    }
}

impl std::error::Error for PodRuntimeOwnershipError {}

/// Durable result of recording one leased outbox delivery failure.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboxFailureDisposition {
    /// The leased row was released with its incremented attempt and backoff.
    RetryScheduled,
    /// The incremented attempt reached the threshold and the row moved atomically.
    DeadLettered,
    /// The row was absent or no longer owned by the supplied lease token.
    LeaseLost,
}

#[cfg(test)]
pub use super::sqlite::DeadLetterTestInsert;

impl super::NodeLocalStores {
    pub async fn enqueue_workqueue(
        &self,
        kind: PodWorkqueueKind,
        pod: &klights_types::PodIdentity,
        payload: serde_json::Value,
        attempt_count: i64,
        min_delay_ms: i64,
        last_error: Option<&str>,
    ) -> anyhow::Result<()> {
        let identity = match kind {
            PodWorkqueueKind::Pod => klights_node_store::PodWorkIdentity::try_pod(pod.clone())?,
            PodWorkqueueKind::Namespace => {
                klights_node_store::PodWorkIdentity::try_namespace(&pod.name, &pod.uid)?
            }
        };
        let entry = klights_node_store::PodWorkqueueEnqueue::try_new(
            identity,
            serde_json::to_vec(&payload)?,
            attempt_count,
            min_delay_ms,
            last_error.map(str::to_string),
        )?;
        self.pod_workqueue()
            .enqueue_work(entry)
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn peek_workqueue_next_due(&self) -> anyhow::Result<Option<i64>> {
        self.pod_workqueue()
            .peek_next_due_ms()
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn claim_workqueue_due(
        &self,
        now_ms: i64,
    ) -> anyhow::Result<Option<PodWorkqueueEntry>> {
        let row = self
            .pod_workqueue()
            .claim_due_work(klights_node_store::DueTimeMs::try_new(now_ms)?)
            .await?;
        row.map(|row| {
            let (id, identity, payload, attempt_count, next_due_ms) = row.into_parts();
            let (kind, pod) = identity.into_persisted();
            Ok(PodWorkqueueEntry {
                id: id.get(),
                kind: match kind {
                    klights_node_store::PodWorkqueueKind::Pod => PodWorkqueueKind::Pod,
                    klights_node_store::PodWorkqueueKind::Namespace => PodWorkqueueKind::Namespace,
                },
                namespace: pod.namespace,
                name: pod.name,
                uid: pod.uid,
                payload: serde_json::from_slice(&payload)?,
                attempt_count,
                next_attempt_at_ms: next_due_ms.get(),
            })
        })
        .transpose()
    }

    pub async fn complete_workqueue(&self, _id: i64) -> anyhow::Result<()> {
        Ok(())
    }
}
