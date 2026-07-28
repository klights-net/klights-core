use anyhow::{Result, anyhow};
use serde_json::Value;
use std::net::Ipv4Addr;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PodNetworkAllocationPod<'a> {
    pub namespace: &'a str,
    pub name: &'a str,
    pub uid: &'a str,
}

impl<'a> PodNetworkAllocationPod<'a> {
    pub fn new(namespace: &'a str, name: &'a str, uid: &'a str) -> Self {
        Self {
            namespace,
            name,
            uid,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PodNetworkAllocationSubnet {
    pub base_int: u32,
    pub size: u32,
}

impl PodNetworkAllocationSubnet {
    pub fn new(base_int: u32, size: u32) -> Self {
        Self { base_int, size }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PodNetworkAllocationLink<'a> {
    pub veth_host: &'a str,
    pub netns_path: &'a str,
}

impl<'a> PodNetworkAllocationLink<'a> {
    pub fn new(veth_host: &'a str, netns_path: &'a str) -> Self {
        Self {
            veth_host,
            netns_path,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PodNetworkAllocationRequest<'a> {
    pub sandbox_id: &'a str,
    pub pod: PodNetworkAllocationPod<'a>,
    pub subnet: PodNetworkAllocationSubnet,
    pub link: PodNetworkAllocationLink<'a>,
}

impl<'a> PodNetworkAllocationRequest<'a> {
    pub fn new(
        sandbox_id: &'a str,
        pod: PodNetworkAllocationPod<'a>,
        subnet: PodNetworkAllocationSubnet,
        link: PodNetworkAllocationLink<'a>,
    ) -> Self {
        Self {
            sandbox_id,
            pod,
            subnet,
            link,
        }
    }

    pub fn into_owned(self) -> OwnedPodNetworkAllocationRequest {
        OwnedPodNetworkAllocationRequest {
            sandbox_id: self.sandbox_id.to_string(),
            namespace: self.pod.namespace.to_string(),
            pod_name: self.pod.name.to_string(),
            pod_uid: self.pod.uid.to_string(),
            subnet_base_int: self.subnet.base_int,
            subnet_size: self.subnet.size,
            veth_host: self.link.veth_host.to_string(),
            netns_path: self.link.netns_path.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedPodNetworkAllocationRequest {
    pub sandbox_id: String,
    pub namespace: String,
    pub pod_name: String,
    pub pod_uid: String,
    pub subnet_base_int: u32,
    pub subnet_size: u32,
    pub veth_host: String,
    pub netns_path: String,
}

impl OwnedPodNetworkAllocationRequest {
    pub fn as_borrowed(&self) -> PodNetworkAllocationRequest<'_> {
        PodNetworkAllocationRequest::new(
            &self.sandbox_id,
            PodNetworkAllocationPod::new(&self.namespace, &self.pod_name, &self.pod_uid),
            PodNetworkAllocationSubnet::new(self.subnet_base_int, self.subnet_size),
            PodNetworkAllocationLink::new(&self.veth_host, &self.netns_path),
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PodNetworkEndpoint {
    pub ip_addr: String,
    pub veth_host: String,
    pub netns_path: String,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PodEndpointMode {
    EncryptedDirect,
    Hostport,
}

impl PodEndpointMode {
    pub fn as_str(self) -> &'static str {
        match self {
            PodEndpointMode::EncryptedDirect => "encrypted_direct",
            PodEndpointMode::Hostport => "hostport",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "encrypted_direct" => Ok(PodEndpointMode::EncryptedDirect),
            "hostport" => Ok(PodEndpointMode::Hostport),
            other => Err(anyhow!("unknown pod_endpoint mode: {}", other)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PodEndpointRow {
    pub pod_uid: String,
    pub namespace: String,
    pub pod_name: String,
    pub node_name: String,
    pub mode: PodEndpointMode,
    pub pod_ip: Ipv4Addr,
    pub node_ip: Ipv4Addr,
    pub host_port_tcp: Option<u16>,
    pub host_port_udp: Option<u16>,
    pub generation: i64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PodEndpointEvent {
    Upsert(PodEndpointRow),
    Delete { pod_uid: String, pod_ip: Ipv4Addr },
}

#[cfg(test)]
mod tests {
    use super::PodEndpointMode;

    #[test]
    fn encrypted_direct_is_live_pod_endpoint_label() {
        assert_eq!(
            PodEndpointMode::EncryptedDirect.as_str(),
            "encrypted_direct"
        );
        assert_eq!(
            PodEndpointMode::parse("encrypted_direct").unwrap(),
            PodEndpointMode::EncryptedDirect
        );
        assert!(PodEndpointMode::parse("vxlan").is_err());
        assert_eq!(
            PodEndpointMode::parse("hostport").unwrap(),
            PodEndpointMode::Hostport
        );
    }
}

pub use super::sqlite::{
    DeadLetterRow, OutboxInsert, OutboxRow, OutboxStats, PodRuntimeRow, PodStatusCheckpoint,
    ProbeStateRow, ReplicationCheckpoint, RuntimeObservationCheckpoint,
};

/// Exact durable identity of one node-local pod-network allocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodNetworkAssignmentRow {
    pub sandbox_id: String,
    pub namespace: String,
    pub pod_name: String,
    pub pod_uid: String,
    pub subnet_base_int: u32,
    pub subnet_size: u32,
    pub ip_addr: String,
    pub ip_int: u32,
    pub veth_host: String,
    pub netns_path: String,
}

/// Typed outcome failures from the atomic node-local IPAM reservation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodNetworkReservationError {
    AddressExhausted {
        subnet_base_int: u32,
        subnet_size: u32,
    },
    IdentityConflict {
        sandbox_id: String,
    },
    Persistence {
        message: String,
    },
}

impl std::fmt::Display for PodNetworkReservationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AddressExhausted {
                subnet_base_int,
                subnet_size,
            } => write!(
                formatter,
                "pod address range {subnet_base_int}/{subnet_size} is exhausted"
            ),
            Self::IdentityConflict { sandbox_id } => {
                write!(
                    formatter,
                    "sandbox {sandbox_id} already has a different identity"
                )
            }
            Self::Persistence { message } => formatter.write_str(message),
        }
    }
}

impl std::error::Error for PodNetworkReservationError {}

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
