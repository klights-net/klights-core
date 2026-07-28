use std::fmt;
use std::future::Future;
use std::pin::Pin;

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct RaftStorageLogAttestation {
    pub term: u64,
    pub leader_node_id: u64,
    pub index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftStorageAttestation {
    pub high_watermark: Option<RaftStorageLogAttestation>,
    pub current_boundary: Option<RaftStorageLogAttestation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteNodeMode {
    Root,
    Rootless,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteNodeHostFacts {
    pub cpu_count: u32,
    pub memory_ki: u64,
    pub architecture: String,
    pub operating_system: String,
    pub os_image: String,
    pub kernel_version: String,
    pub container_runtime_version: String,
    pub kubelet_version: String,
    pub git_commit: String,
}

impl RemoteNodeHostFacts {
    pub fn validate(&self) -> Result<(), ControlplaneJoinError> {
        if self.cpu_count == 0 {
            return Err(ControlplaneJoinError::new(
                "node registration cpu_count must be positive",
            ));
        }
        if self.memory_ki == 0 {
            return Err(ControlplaneJoinError::new(
                "node registration memory_ki must be positive",
            ));
        }
        for (field, value, limit) in [
            ("architecture", self.architecture.as_str(), 63),
            ("operating_system", self.operating_system.as_str(), 63),
            ("os_image", self.os_image.as_str(), 256),
            ("kernel_version", self.kernel_version.as_str(), 256),
            (
                "container_runtime_version",
                self.container_runtime_version.as_str(),
                256,
            ),
            ("kubelet_version", self.kubelet_version.as_str(), 256),
            ("git_commit", self.git_commit.as_str(), 128),
        ] {
            if value.trim().is_empty() || value.trim() != value || value.len() > limit {
                return Err(ControlplaneJoinError::new(format!(
                    "node registration {field} must be non-empty canonical text of at most {limit} bytes"
                )));
            }
        }
        if self.operating_system != "linux" {
            return Err(ControlplaneJoinError::new(
                "node registration operating_system must be 'linux'",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteNodeRegistrationSnapshot {
    pub node_mode: RemoteNodeMode,
    pub host: RemoteNodeHostFacts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlplaneJoinRegistrationSnapshot {
    pub node_name: String,
    pub node_internal_ip: String,
    pub as_learner: bool,
    pub storage_incarnation: String,
    pub storage_log_attestation: RaftStorageAttestation,
    pub snapshot: RemoteNodeRegistrationSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlplaneJoinRequest {
    pub node_id: u64,
    pub addr: String,
    pub node_name: String,
    pub as_learner: bool,
    pub storage_incarnation: String,
    pub storage_log_attestation: RaftStorageAttestation,
    pub command_codec_version: u32,
    pub node_internal_ip: Option<String>,
    pub node_registration: Option<RemoteNodeRegistrationSnapshot>,
    /// Rolling-upgrade compatibility for a persisted member whose request
    /// predates the typed snapshot. Never used to synthesize other host facts.
    pub legacy_node_git_commit: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlplaneJoinOutcome {
    Accepted {
        voter_count_after: u32,
        admitted_as_learner: bool,
        ca_cert_pem: String,
        encrypted_ca_key: Vec<u8>,
        ca_key_nonce: [u8; 12],
    },
    RedirectToLeader {
        leader_id: u64,
        leader_addr: String,
    },
    Denied {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlplaneJoinRoute {
    Local,
    Redirect { leader_id: u64, leader_addr: String },
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlplaneJoinAdmissionOutcome {
    pub changed: bool,
    pub voter_count_after: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlplaneJoinError {
    message: String,
}

impl ControlplaneJoinError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for ControlplaneJoinError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ControlplaneJoinError {}

pub type ControlplaneJoinFuture<'a> = Pin<
    Box<dyn Future<Output = Result<ControlplaneJoinOutcome, ControlplaneJoinError>> + Send + 'a>,
>;
pub type ControlplaneMemberQueryFuture<'a> = Pin<Box<dyn Future<Output = bool> + Send + 'a>>;
pub type ControlplaneJoinAdmissionFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<ControlplaneJoinAdmissionOutcome, ControlplaneJoinError>>
            + Send
            + 'a,
    >,
>;
pub type ControlplaneJoinRegistrationFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), ControlplaneJoinError>> + Send + 'a>>;
pub type ControlplaneJoinMetadataFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), ControlplaneJoinError>> + Send + 'a>>;

pub trait ControlplaneJoinHandler: Send + Sync {
    fn join(&self, request: ControlplaneJoinRequest) -> ControlplaneJoinFuture<'_>;
    fn is_controlplane_member<'a>(
        &'a self,
        node_name: &'a str,
    ) -> ControlplaneMemberQueryFuture<'a>;
}

pub trait ControlplaneJoinAuthority: Send + Sync {
    fn route(&self) -> ControlplaneJoinRoute;
}

pub trait ControlplaneMemberQuery: Send + Sync {
    fn is_controlplane_member<'a>(
        &'a self,
        node_name: &'a str,
    ) -> ControlplaneMemberQueryFuture<'a>;
}

pub trait ControlplaneJoinAdmission: Send + Sync {
    fn admit<'a>(
        &'a self,
        request: &'a ControlplaneJoinRequest,
    ) -> ControlplaneJoinAdmissionFuture<'a>;
}

pub trait ControlplaneJoinRegistration: Send + Sync {
    fn register<'a>(
        &'a self,
        request: &'a ControlplaneJoinRequest,
        voter_count_after: u32,
    ) -> ControlplaneJoinRegistrationFuture<'a>;
}

pub trait ControlplaneJoinMetadata: Send + Sync {
    fn refresh<'a>(
        &'a self,
        node_name: &'a str,
        as_learner: bool,
    ) -> ControlplaneJoinMetadataFuture<'a>;
}
