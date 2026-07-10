use std::fmt;

use anyhow::{Result, anyhow};

use crate::datastore::command::{StorageCommand, decode_command_protobuf, encode_command_protobuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

/// Persisted scheduling class for node-local outbox rows. Lower values are
/// more urgent; the stable integer representation is part of node.db schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i64)]
pub enum OutboxPriorityClass {
    Lease = 0,
    NodeHealth = 1,
    Workload = 2,
    Diagnostic = 3,
}

impl OutboxPriorityClass {
    pub const fn persisted_value(self) -> i64 {
        self as i64
    }
}

/// Once diagnostic work reaches this age it joins the workload scheduling
/// class, bounding starvation without delaying leader-health traffic.
pub const OUTBOX_DIAGNOSTIC_AGING_MS: i64 = 30_000;

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

    pub fn as_str(self) -> &'static str {
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

    pub fn subject_api_version_kind(self) -> (&'static str, &'static str) {
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

    pub const fn priority_class(self) -> OutboxPriorityClass {
        match self {
            Self::LeaseRenew => OutboxPriorityClass::Lease,
            Self::NodeStatus => OutboxPriorityClass::NodeHealth,
            Self::EventCreate => OutboxPriorityClass::Diagnostic,
            Self::PodStatus
            | Self::RuntimeReconcile
            | Self::ProbeReadiness
            | Self::DeadlineExceeded
            | Self::ContainerStatusSnapshot
            | Self::EphemeralContainerStatuses
            | Self::PodMetadata
            | Self::NodeRegistration
            | Self::NodeDataplane => OutboxPriorityClass::Workload,
        }
    }

    pub const fn supersedable_pod_status(self) -> bool {
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
}

impl fmt::Display for OutboxOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<&str> for OutboxOperation {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self> {
        match value {
            "PodStatus" => Ok(Self::PodStatus),
            "RuntimeReconcile" => Ok(Self::RuntimeReconcile),
            "ProbeReadiness" => Ok(Self::ProbeReadiness),
            "DeadlineExceeded" => Ok(Self::DeadlineExceeded),
            "ContainerStatusSnapshot" => Ok(Self::ContainerStatusSnapshot),
            "EphemeralContainerStatuses" => Ok(Self::EphemeralContainerStatuses),
            "PodMetadata" => Ok(Self::PodMetadata),
            "NodeRegistration" => Ok(Self::NodeRegistration),
            "NodeDataplane" => Ok(Self::NodeDataplane),
            "NodeStatus" => Ok(Self::NodeStatus),
            "LeaseRenew" => Ok(Self::LeaseRenew),
            "EventCreate" => Ok(Self::EventCreate),
            other => Err(anyhow!("unknown outbox operation: {other}")),
        }
    }
}

#[cfg(test)]
mod classification_tests {
    use super::*;

    #[test]
    fn operation_owns_persisted_priority_and_supersedable_semantics() {
        assert_eq!(
            OutboxOperation::LeaseRenew
                .priority_class()
                .persisted_value(),
            0
        );
        assert_eq!(
            OutboxOperation::NodeStatus
                .priority_class()
                .persisted_value(),
            1
        );
        assert_eq!(
            OutboxOperation::PodStatus
                .priority_class()
                .persisted_value(),
            2
        );
        assert_eq!(
            OutboxOperation::EventCreate
                .priority_class()
                .persisted_value(),
            3
        );

        for operation in [
            OutboxOperation::PodStatus,
            OutboxOperation::RuntimeReconcile,
            OutboxOperation::ProbeReadiness,
            OutboxOperation::DeadlineExceeded,
            OutboxOperation::ContainerStatusSnapshot,
            OutboxOperation::EphemeralContainerStatuses,
        ] {
            assert!(operation.supersedable_pod_status(), "{operation}");
        }
        for operation in [
            OutboxOperation::PodMetadata,
            OutboxOperation::NodeRegistration,
            OutboxOperation::NodeDataplane,
            OutboxOperation::NodeStatus,
            OutboxOperation::LeaseRenew,
            OutboxOperation::EventCreate,
        ] {
            assert!(!operation.supersedable_pod_status(), "{operation}");
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OutboxPayload {
    pub command: StorageCommand,
}

impl OutboxPayload {
    pub fn from_command(command: StorageCommand) -> Self {
        Self { command }
    }

    pub fn encode_protobuf(&self) -> Result<Vec<u8>> {
        encode_command_protobuf(&self.command)
    }

    pub fn decode_protobuf(bytes: &[u8]) -> Result<Self> {
        Ok(Self {
            command: decode_command_protobuf(bytes)?,
        })
    }
}
