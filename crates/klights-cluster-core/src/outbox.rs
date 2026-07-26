use std::fmt;

use serde_json::Value;

use crate::{
    LogApplyCommit, Resource, ResourceBatchOperation, ResourcePreconditions, StorageCommand,
};

/// Complete operation set persisted in node-local outbox rows and consumed by
/// committed cluster apply. Lease renewal is intentionally included even
/// though it uses a focused leader capability instead of durable delivery.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum OutboxOperation {
    PodStatus,
    RuntimeReconcile,
    ProbeReadiness,
    DeadlineExceeded,
    ContainerStatusSnapshot,
    EphemeralContainerStatuses,
    PodMetadata,
    NodeRegistration,
    NodeDataplane,
    NodeStatus,
    LeaseRenew,
    EventCreate,
}

impl OutboxOperation {
    pub const ALL: [Self; 12] = [
        Self::PodStatus,
        Self::RuntimeReconcile,
        Self::ProbeReadiness,
        Self::DeadlineExceeded,
        Self::ContainerStatusSnapshot,
        Self::EphemeralContainerStatuses,
        Self::PodMetadata,
        Self::NodeRegistration,
        Self::NodeDataplane,
        Self::NodeStatus,
        Self::LeaseRenew,
        Self::EventCreate,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PodStatus => "PodStatus",
            Self::RuntimeReconcile => "RuntimeReconcile",
            Self::ProbeReadiness => "ProbeReadiness",
            Self::DeadlineExceeded => "DeadlineExceeded",
            Self::ContainerStatusSnapshot => "ContainerStatusSnapshot",
            Self::EphemeralContainerStatuses => "EphemeralContainerStatuses",
            Self::PodMetadata => "PodMetadata",
            Self::NodeRegistration => "NodeRegistration",
            Self::NodeDataplane => "NodeDataplane",
            Self::NodeStatus => "NodeStatus",
            Self::LeaseRenew => "LeaseRenew",
            Self::EventCreate => "EventCreate",
        }
    }

    pub const fn subject_api_version_kind(self) -> (&'static str, &'static str) {
        match self {
            Self::NodeRegistration | Self::NodeDataplane | Self::NodeStatus => ("v1", "Node"),
            Self::LeaseRenew => ("coordination.k8s.io/v1", "Lease"),
            Self::EventCreate => ("events.k8s.io/v1", "Event"),
            Self::PodStatus
            | Self::RuntimeReconcile
            | Self::ProbeReadiness
            | Self::DeadlineExceeded
            | Self::ContainerStatusSnapshot
            | Self::EphemeralContainerStatuses
            | Self::PodMetadata => ("v1", "Pod"),
        }
    }

    pub const fn priority(self) -> OutboxPriority {
        match self {
            Self::LeaseRenew => OutboxPriority::Lease,
            Self::NodeStatus => OutboxPriority::NodeHealth,
            Self::EventCreate => OutboxPriority::Diagnostic,
            _ => OutboxPriority::Workload,
        }
    }

    pub const fn is_supersedable_pod_status(self) -> bool {
        matches!(
            self,
            Self::PodStatus
                | Self::RuntimeReconcile
                | Self::ProbeReadiness
                | Self::DeadlineExceeded
                | Self::ContainerStatusSnapshot
                | Self::EphemeralContainerStatuses
        )
    }

    pub const fn uses_per_subject_sequence(self) -> bool {
        !matches!(self, Self::LeaseRenew)
    }
}

impl fmt::Display for OutboxOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl TryFrom<&str> for OutboxOperation {
    type Error = UnknownOutboxOperation;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::ALL
            .into_iter()
            .find(|operation| operation.as_str() == value)
            .ok_or_else(|| UnknownOutboxOperation(value.to_string()))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboxPriority {
    Lease,
    NodeHealth,
    Workload,
    Diagnostic,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnknownOutboxOperation(String);

impl fmt::Display for UnknownOutboxOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "unknown outbox operation: {}", self.0)
    }
}

impl std::error::Error for UnknownOutboxOperation {}

/// Neutral result of authoritative outbox apply.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutboxApplyOutcome {
    Applied { applied_rv: i64 },
    AlreadyApplied { applied_rv: Option<i64> },
}

impl OutboxApplyOutcome {
    pub const fn applied_resource_version(&self) -> Option<i64> {
        match self {
            Self::Applied { applied_rv } => Some(*applied_rv),
            Self::AlreadyApplied { applied_rv } => *applied_rv,
        }
    }
}

/// Neutral authoritative-apply failure. Transport availability, timeout, and
/// authentication failures are leader API concerns and do not belong here.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutboxApplyError {
    Retryable(String),
    ConflictTerminal(String),
    NotFound(String),
    UidMismatch { expected: String, actual: String },
}

impl fmt::Display for OutboxApplyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Retryable(message)
            | Self::ConflictTerminal(message)
            | Self::NotFound(message) => formatter.write_str(message),
            Self::UidMismatch { expected, actual } => {
                write!(
                    formatter,
                    "delivery UID mismatch: expected {expected}, actual {actual}"
                )
            }
        }
    }
}

impl std::error::Error for OutboxApplyError {}

impl OutboxApplyError {
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable(_))
    }

    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::ConflictTerminal(_) | Self::NotFound(_) | Self::UidMismatch { .. }
        )
    }
}

/// Backend-neutral result of materializing one durable outbox command.
///
/// Storage adapters construct this value; the Raft proposer consumes it.
/// Keeping the handoff in cluster-core prevents the consensus layer from
/// depending on a concrete SQLite or redb implementation.
pub enum BuildOutboxOutcome {
    /// The idempotency slot was claimed and the commit must be proposed.
    NeedsPropose {
        commit: LogApplyCommit,
        applied_rv: i64,
        /// A terminal result is returned only after this commit durably
        /// records the ledger and stream watermark.
        terminal_error: Option<OutboxApplyError>,
    },
    /// Lease renewal is handled by its focused leader capability.
    LeaseRenewShortcircuit,
    /// The same idempotency key was already committed.
    AlreadyApplied {
        applied_rv: Option<i64>,
        /// Transactional pre-delete receipt for actor-finalization cascade.
        committed_resource: Option<Resource>,
    },
}

/// Decoded neutral outbox payload. Wire encoding is owned by the internal
/// protobuf boundary rather than the node runtime.
#[derive(Clone, Debug, PartialEq)]
pub struct OutboxPayload {
    pub command: StorageCommand,
}

impl OutboxPayload {
    pub const fn new(command: StorageCommand) -> Self {
        Self { command }
    }

    pub const fn command(&self) -> &StorageCommand {
        &self.command
    }

    pub fn into_command(self) -> StorageCommand {
        self.command
    }
}

/// Stable UID-aware subject key used for outbox dedupe and sequencing.
pub fn subject_key_for_command(command: &StorageCommand) -> String {
    match command {
        StorageCommand::CreateResource {
            api_version,
            kind,
            namespace,
            name,
            data,
        }
        | StorageCommand::UpdateResource {
            api_version,
            kind,
            namespace,
            name,
            data,
            ..
        } => resource_subject_key(api_version, kind, namespace.as_deref(), name, data),
        StorageCommand::UpdateStatus {
            api_version,
            kind,
            namespace,
            name,
            preconditions,
            ..
        }
        | StorageCommand::DeleteResource {
            api_version,
            kind,
            namespace,
            name,
            preconditions,
        }
        | StorageCommand::PatchResource {
            api_version,
            kind,
            namespace,
            name,
            preconditions,
            ..
        } => resource_key_parts(
            api_version,
            kind,
            namespace.as_deref(),
            name,
            preconditions.uid.as_deref(),
        ),
        StorageCommand::CreateNamespace { name, data }
        | StorageCommand::UpdateNamespace { name, data, .. } => {
            resource_subject_key("v1", "Namespace", None, name, data)
        }
        StorageCommand::DeleteNamespace { name }
        | StorageCommand::DeleteNamespaceContents { name } => {
            resource_key_parts("v1", "Namespace", None, name, None)
        }
        StorageCommand::FinalizeBoundPod {
            namespace,
            name,
            pod_uid,
            ..
        } => resource_key_parts("v1", "Pod", Some(namespace), name, Some(pod_uid)),
        StorageCommand::ApplyResourceBatch { operations } => match operations.first() {
            Some(ResourceBatchOperation::Put {
                api_version,
                kind,
                namespace,
                name,
                ..
            })
            | Some(ResourceBatchOperation::Delete {
                api_version,
                kind,
                namespace,
                name,
                ..
            }) => format!(
                "batch:{api_version}/{kind}/{}/{}",
                namespace.as_deref().unwrap_or(""),
                name
            ),
            None => "batch:empty".to_string(),
        },
        other => other.variant_name().to_string(),
    }
}

pub fn pod_target(command: &StorageCommand) -> Option<(&str, &str, &ResourcePreconditions)> {
    match command {
        StorageCommand::UpdateStatus {
            api_version,
            kind,
            namespace: Some(namespace),
            name,
            preconditions,
            ..
        }
        | StorageCommand::UpdateResource {
            api_version,
            kind,
            namespace: Some(namespace),
            name,
            preconditions,
            ..
        }
        | StorageCommand::PatchResource {
            api_version,
            kind,
            namespace: Some(namespace),
            name,
            preconditions,
            ..
        }
        | StorageCommand::DeleteResource {
            api_version,
            kind,
            namespace: Some(namespace),
            name,
            preconditions,
        }
        | StorageCommand::DeleteResourceWithTombstone {
            api_version,
            kind,
            namespace: Some(namespace),
            name,
            preconditions,
            ..
        } if api_version == "v1" && kind == "Pod" => Some((namespace, name, preconditions)),
        _ => None,
    }
}

pub fn classify_apply_error_for_command(
    command: &StorageCommand,
    error: OutboxApplyError,
) -> OutboxApplyError {
    match error {
        OutboxApplyError::Retryable(message) => {
            let lower = message.to_ascii_lowercase();
            if lower.contains("query returned no rows") && pod_target(command).is_some() {
                OutboxApplyError::ConflictTerminal(message)
            } else {
                classify_apply_error(OutboxApplyError::Retryable(message))
            }
        }
        other => other,
    }
}

pub fn classify_apply_error(error: OutboxApplyError) -> OutboxApplyError {
    match error {
        OutboxApplyError::Retryable(message) => {
            let lower = message.to_ascii_lowercase();
            if lower.contains("uid mismatch") || lower.contains("uid precondition failed") {
                OutboxApplyError::UidMismatch {
                    expected: "<unknown>".to_string(),
                    actual: "<unknown>".to_string(),
                }
            } else if lower.contains("not found") {
                OutboxApplyError::NotFound(message)
            } else if lower.contains("conflict") || lower.contains("precondition failed") {
                OutboxApplyError::ConflictTerminal(message)
            } else {
                OutboxApplyError::Retryable(message)
            }
        }
        other => other,
    }
}

fn resource_subject_key(
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    data: &Value,
) -> String {
    resource_key_parts(
        api_version,
        kind,
        namespace,
        name,
        data.pointer("/metadata/uid").and_then(Value::as_str),
    )
}

fn resource_key_parts(
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    uid: Option<&str>,
) -> String {
    let mut key = match namespace {
        Some(namespace) => format!("{api_version}/{kind}/{namespace}/{name}"),
        None => format!("{api_version}/{kind}/{name}"),
    };
    if let Some(uid) = uid.filter(|uid| !uid.is_empty()) {
        key.push('/');
        key.push_str(uid);
    }
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operations_round_trip_every_persisted_wire_name() {
        for operation in OutboxOperation::ALL {
            assert_eq!(OutboxOperation::try_from(operation.as_str()), Ok(operation));
        }
        assert!(OutboxOperation::try_from("unknown").is_err());
    }

    #[test]
    fn subject_key_is_uid_aware() {
        let command = StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web".to_string(),
            status: serde_json::json!({}),
            expected_rv: None,
            preconditions: ResourcePreconditions {
                uid: Some("uid-1".to_string()),
                resource_version: None,
            },
            observed_status_stamp: None,
        };
        assert_eq!(
            subject_key_for_command(&command),
            "v1/Pod/default/web/uid-1"
        );
    }

    #[test]
    fn stale_pod_precondition_failure_is_terminal() {
        let command = StorageCommand::DeleteResource {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web".to_string(),
            preconditions: ResourcePreconditions::default(),
        };
        assert!(matches!(
            classify_apply_error_for_command(
                &command,
                OutboxApplyError::Retryable("Query returned no rows".to_string())
            ),
            OutboxApplyError::ConflictTerminal(_)
        ));
    }
}
