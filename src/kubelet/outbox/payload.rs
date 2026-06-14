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

impl OutboxOperation {
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
