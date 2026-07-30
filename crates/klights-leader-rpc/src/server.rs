use anyhow::{Context, Result, anyhow};
use futures::stream::BoxStream;
use klights_cluster_core::{Resource, ResourcePreconditions, StorageCommand, WatchReplayPosition};
use klights_leader_api::OutboxDeliveryResult;
use klights_node_api::{
    ExecStreamChannel, ExecTerminalError, NodeExecFrame, NodeExecSyncResult, NodeLogEvent,
    NodeLogTerminalError, NodeMetricsContainerSample, NodeMetricsError, NodeMetricsNodeSample,
    NodeMetricsPodSample, NodeMetricsResult,
};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tonic::{Request, Response, Status, metadata::MetadataMap};

use super::ca_files::ControlplaneCaFiles;
use super::ca_files::ReplicationRuntimeFiles;

use crate::protocol::{
    FollowerCompletionContext, FollowerControlMessage, JoinRequest, JoinResponse, JoinRole,
    MetadataResponse, NodeOperationKind, ReplicationEntry, RoutedNodeExecFrame,
    RoutedNodeExecRequest, RoutedNodeExecSyncRequest, RoutedNodeExecSyncResponse,
    RoutedNodeLogEvent, RoutedNodeLogRequest, RoutedNodeMetricsRequest, RoutedNodeMetricsResponse,
};
use crate::{
    JOIN_TOKEN_METADATA_KEY, entry_to_proto, resource_command_request_from_proto,
    watch_replay_position_from_proto, watch_replay_position_to_proto,
};

/// Focused application handler used by the authenticated gRPC transport.
///
/// The transport owns no concrete replication application service. This
/// contract moves with the transport in Phase 12A; the embedded replication
/// adapter remains the implementation owner.
pub trait GrpcRuntimeSupervision: Send + Sync {
    fn task_supervisor(&self) -> Arc<klights_supervisor::TaskSupervisor>;
}

/// Root-provided wall-clock capability for policy decisions made at the
/// authenticated leader RPC boundary.
///
/// The transport owns the skew policy but must not discover wall time itself.
/// This port moves with the reusable leader RPC transport in Phase 12A while
/// runtime composition remains responsible for selecting the system clock.
pub trait GrpcWallClock: Send + Sync {
    fn now(&self) -> chrono::DateTime<chrono::Utc>;
}

impl<F> GrpcWallClock for F
where
    F: Fn() -> chrono::DateTime<chrono::Utc> + Send + Sync,
{
    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        self()
    }
}

#[async_trait::async_trait]
pub trait GrpcBootstrapRuntime: Send + Sync {
    async fn validate_controlplane_bootstrap_token(
        &self,
        token: &str,
    ) -> Result<(), klights_leader_api::BootstrapTokenValidationError>;
    async fn handle_authenticated_join(&self, request: JoinRequest) -> JoinResponse;
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GrpcRuntimeError {
    #[error("{operation} unavailable: {message}")]
    Unavailable {
        operation: &'static str,
        message: String,
    },
}

impl GrpcRuntimeError {
    pub fn unavailable(operation: &'static str, message: impl Into<String>) -> Self {
        Self::Unavailable {
            operation,
            message: message.into(),
        }
    }
}

#[async_trait::async_trait]
pub trait GrpcFollowerSessionRuntime: Send + Sync {
    async fn register_follower(
        &self,
        dataplane: klights_leader_api::NetworkDataplane,
    ) -> (tokio::sync::mpsc::Receiver<FollowerControlMessage>, u64);
    async fn register_stream_follower(
        &self,
        node_name: String,
        session_id: u64,
    ) -> std::result::Result<tokio::sync::mpsc::Receiver<ReplicationEntry>, GrpcRuntimeError>;
    async fn update_follower_ack(&self, node_name: &str, applied_rv: i64);
    async fn unregister_follower(&self, node_name: &str, session_id: u64);
}

#[async_trait::async_trait]
pub trait GrpcFollowerCompletionRuntime: Send + Sync {
    async fn complete_node_exec_sync(
        &self,
        context: FollowerCompletionContext<'_>,
        response: RoutedNodeExecSyncResponse,
    ) -> std::result::Result<(), GrpcRuntimeError>;
    async fn complete_node_log_event(
        &self,
        context: FollowerCompletionContext<'_>,
        response: RoutedNodeLogEvent,
    ) -> std::result::Result<(), GrpcRuntimeError>;
    async fn complete_node_metrics(
        &self,
        context: FollowerCompletionContext<'_>,
        response: RoutedNodeMetricsResponse,
    ) -> std::result::Result<(), GrpcRuntimeError>;
    async fn complete_node_exec_stream_frame(
        &self,
        context: FollowerCompletionContext<'_>,
        response: RoutedNodeExecFrame,
    ) -> std::result::Result<(), GrpcRuntimeError>;
}

#[async_trait::async_trait]
pub trait GrpcMetadataRuntime: Send + Sync {
    async fn handle_metadata(&self) -> MetadataResponse;
    async fn record_observed_peer_endpoint(&self, node_name: &str, endpoint: String);
    async fn observed_peer_endpoint(&self, node_name: &str) -> Option<String>;
}

#[derive(Clone)]
pub struct GrpcReplicationRuntimePorts {
    supervision: Arc<dyn GrpcRuntimeSupervision>,
    bootstrap: Arc<dyn GrpcBootstrapRuntime>,
    follower_sessions: Arc<dyn GrpcFollowerSessionRuntime>,
    follower_completions: Arc<dyn GrpcFollowerCompletionRuntime>,
    metadata: Arc<dyn GrpcMetadataRuntime>,
}

impl GrpcReplicationRuntimePorts {
    pub fn from_shared<T>(shared: Arc<T>) -> Self
    where
        T: GrpcRuntimeSupervision
            + GrpcBootstrapRuntime
            + GrpcFollowerSessionRuntime
            + GrpcFollowerCompletionRuntime
            + GrpcMetadataRuntime
            + 'static,
    {
        Self {
            supervision: shared.clone(),
            bootstrap: shared.clone(),
            follower_sessions: shared.clone(),
            follower_completions: shared.clone(),
            metadata: shared,
        }
    }
}

const MAX_NODE_LEASE_RENEW_TIME_SKEW_SECONDS: i64 = 100;

pub fn validate_join_metadata(
    join: &klights_internal_protobuf::JoinRequest,
) -> Result<klights_leader_api::NetworkDataplane> {
    validate_join_metadata_with_endpoint(join, None)
}

fn require_worker_command_codec_v3(
    join: &klights_internal_protobuf::JoinRequest,
) -> std::result::Result<(), Status> {
    crate::protocol::require_exact_command_codec(join.command_codec_version, "worker")
        .map_err(Status::failed_precondition)
}

fn observed_or_advertised_dataplane_endpoint(
    endpoint_override: Option<IpAddr>,
    advertised_endpoint: &str,
) -> Result<IpAddr> {
    endpoint_override
        .or_else(|| advertised_endpoint.trim().parse().ok())
        .ok_or_else(|| anyhow!("dataplane endpoint must be a canonical IP address"))
}

fn dataplane_port_from_u32(port: u32) -> Result<Option<u16>> {
    if port == 0 {
        Ok(None)
    } else {
        Ok(Some(
            u16::try_from(port).map_err(|_| anyhow!("dataplane port exceeds u16"))?,
        ))
    }
}

fn parse_dataplane_mode(raw: &str) -> Result<klights_leader_api::NetworkNodeMode> {
    match raw {
        "root" => Ok(klights_leader_api::NetworkNodeMode::Root),
        "rootless" => Ok(klights_leader_api::NetworkNodeMode::Rootless),
        other => Err(anyhow!("unsupported dataplane mode {other:?}")),
    }
}

fn parse_dataplane_encryption(raw: &str) -> Result<klights_leader_api::DataplaneEncryption> {
    match raw {
        "" | "enabled" | "wireguard" => Ok(klights_leader_api::DataplaneEncryption::WireGuard),
        "disabled" | "direct" => Ok(klights_leader_api::DataplaneEncryption::Direct),
        other => Err(anyhow!("unsupported dataplane encryption {other:?}")),
    }
}

fn node_subnet_allocation_status(error: klights_leader_api::NodeSubnetAllocationError) -> Status {
    use klights_leader_api::NodeSubnetAllocationError;

    match error {
        NodeSubnetAllocationError::InvalidRequest { .. } => {
            Status::invalid_argument(error.to_string())
        }
        NodeSubnetAllocationError::NotLeader => Status::failed_precondition("not raft leader"),
        NodeSubnetAllocationError::Unauthorized { .. } => {
            Status::permission_denied(error.to_string())
        }
        NodeSubnetAllocationError::Conflict { .. } => Status::already_exists(error.to_string()),
        NodeSubnetAllocationError::Exhausted { .. } => {
            Status::resource_exhausted(error.to_string())
        }
        NodeSubnetAllocationError::Timeout => Status::deadline_exceeded(error.to_string()),
        NodeSubnetAllocationError::Cancelled => Status::cancelled(error.to_string()),
        NodeSubnetAllocationError::AllocationFailed { .. }
        | NodeSubnetAllocationError::CorruptResponse { .. } => Status::internal(error.to_string()),
        NodeSubnetAllocationError::Retryable { .. } => Status::unavailable(error.to_string()),
        _ => Status::internal(error.to_string()),
    }
}

fn validate_node_lease_renew_time_skew(
    renew_time: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> std::result::Result<(), Status> {
    let renew_time = chrono::DateTime::parse_from_rfc3339(renew_time)
        .map_err(|err| Status::invalid_argument(format!("invalid node lease renewTime: {err}")))?
        .with_timezone(&chrono::Utc);
    let skew_seconds = now.signed_duration_since(renew_time).num_seconds().abs();
    if skew_seconds > MAX_NODE_LEASE_RENEW_TIME_SKEW_SECONDS {
        return Err(Status::invalid_argument(format!(
            "node lease renewTime clock skew {skew_seconds}s exceeds {MAX_NODE_LEASE_RENEW_TIME_SKEW_SECONDS}s"
        )));
    }
    Ok(())
}

fn validate_join_metadata_with_endpoint(
    join: &klights_internal_protobuf::JoinRequest,
    endpoint_override: Option<IpAddr>,
) -> Result<klights_leader_api::NetworkDataplane> {
    let mode = parse_dataplane_mode(&join.dataplane_mode)?;
    let encryption = parse_dataplane_encryption(&join.dataplane_encryption)?;
    let port = dataplane_port_from_u32(join.dataplane_port)?;
    let endpoint =
        observed_or_advertised_dataplane_endpoint(endpoint_override, &join.dataplane_endpoint)?;
    let public_key =
        Some(join.dataplane_public_key.clone()).filter(|value| !value.trim().is_empty());
    Ok(klights_leader_api::NetworkDataplane::try_new(
        join.node_name.clone(),
        mode,
        encryption,
        public_key.as_deref(),
        endpoint,
        port,
    )?)
}

fn validate_controlplane_join_dataplane_metadata_with_endpoint(
    join: &klights_internal_protobuf::JoinAsControlplaneRequest,
    endpoint_override: Option<IpAddr>,
) -> Result<klights_leader_api::NetworkDataplane> {
    let mode = parse_dataplane_mode(&join.dataplane_mode)?;
    let encryption = parse_dataplane_encryption(&join.dataplane_encryption)?;
    let port = dataplane_port_from_u32(join.dataplane_port)?;
    let endpoint =
        observed_or_advertised_dataplane_endpoint(endpoint_override, &join.dataplane_endpoint)?;
    let public_key =
        Some(join.dataplane_public_key.clone()).filter(|value| !value.trim().is_empty());
    Ok(klights_leader_api::NetworkDataplane::try_new(
        join.node_name.clone(),
        mode,
        encryption,
        public_key.as_deref(),
        endpoint,
        port,
    )?)
}

pub fn validate_controlplane_node_registration(
    registration: klights_internal_protobuf::NodeRegistrationSnapshot,
) -> Result<klights_leader_api::RemoteNodeRegistrationSnapshot> {
    let node_mode = match registration.node_mode.as_str() {
        "root" => klights_leader_api::RemoteNodeMode::Root,
        "rootless" => klights_leader_api::RemoteNodeMode::Rootless,
        other => return Err(anyhow!("unsupported node registration mode {other:?}")),
    };
    let host = klights_leader_api::RemoteNodeHostFacts {
        cpu_count: registration.cpu_count,
        memory_ki: registration.memory_ki,
        architecture: registration.architecture,
        operating_system: registration.operating_system,
        os_image: registration.os_image,
        kernel_version: registration.kernel_version,
        container_runtime_version: registration.container_runtime_version,
        kubelet_version: registration.kubelet_version,
        git_commit: registration.git_commit,
    };
    host.validate()?;
    Ok(klights_leader_api::RemoteNodeRegistrationSnapshot { node_mode, host })
}

fn uri_host_for_ip(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(ip) => ip.to_string(),
        IpAddr::V6(ip) => format!("[{ip}]"),
    }
}

fn uri_has_explicit_path_or_query(addr: &str) -> bool {
    let Some(authority_start) = addr.find("://").map(|idx| idx + 3) else {
        return false;
    };
    let after_scheme = &addr[authority_start..];
    after_scheme.contains('/') || after_scheme.contains('?')
}

fn raft_addr_with_observed_host(addr: &str, endpoint_override: Option<IpAddr>) -> Result<String> {
    let Some(observed_ip) = endpoint_override else {
        return Ok(addr.to_string());
    };
    let uri = addr
        .parse::<hyper::Uri>()
        .with_context(|| format!("invalid controlplane raft URI '{addr}'"))?;
    let scheme = uri
        .scheme_str()
        .ok_or_else(|| anyhow!("controlplane raft URI has no scheme: {addr}"))?;
    let authority = uri
        .authority()
        .ok_or_else(|| anyhow!("controlplane raft URI has no authority: {addr}"))?;
    let observed_host = uri_host_for_ip(observed_ip);
    let authority = match authority.port_u16() {
        Some(port) => format!("{observed_host}:{port}"),
        None => observed_host,
    };
    let mut path_and_query = uri
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or("");
    if path_and_query == "/" && !uri_has_explicit_path_or_query(addr) {
        path_and_query = "";
    }
    Ok(format!("{scheme}://{authority}{path_and_query}"))
}

pub fn insert_tonic_tcp_connect_info<B>(
    request: &mut hyper::http::Request<B>,
    local_addr: Option<SocketAddr>,
    remote_addr: Option<SocketAddr>,
) {
    request
        .extensions_mut()
        .insert(tonic::transport::server::TcpConnectInfo {
            local_addr,
            remote_addr,
        });
}

/// P0 (memory-improvement.md §10) made the snapshot serve path cache its
/// result as an `Arc<Vec<LogApplyCommit>>`. P1 supersedes that: the serve
/// path now STREAMS the snapshot through a bounded channel and never
/// materializes a `Vec` at all, so no cache is held on the server struct.
/// The `SnapshotCache` type itself is kept (in `snapshot_cache.rs`) for its
/// unit tests and any future callers that want the Arc-sharing semantics.
#[derive(Clone)]
pub struct ReplicationServerPorts {
    resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    resource_command: Arc<dyn klights_leader_api::LeaderResourceCommand>,
    watch: Arc<dyn klights_leader_api::LeaderWatch>,
    projected_token: Arc<dyn klights_leader_api::LeaderAuthenticatedProjectedServiceAccountToken>,
    pod_cleanup: Arc<dyn klights_leader_api::LeaderPodCleanupIntents>,
    node_lease: Arc<dyn klights_leader_api::LeaderNodeLeaseRenewal>,
    node_subnet: Arc<dyn klights_leader_api::LeaderNodeSubnetAllocation>,
    topology_query: Arc<dyn klights_leader_api::LeaderNetworkTopologyQuery>,
    topology_command: Arc<dyn klights_leader_api::LeaderNetworkTopologyCommand>,
    authenticated_outbox: Arc<dyn klights_leader_api::LeaderAuthenticatedOutboxDelivery>,
}

#[derive(Clone, Debug)]
pub struct ReplicationPeerIdentity {
    pub username: String,
    pub groups: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplicationPeerAuthenticationError {
    Rejected { message: String },
    DependencyFailure { message: String },
    InternalFailure { message: String },
}

impl std::fmt::Display for ReplicationPeerAuthenticationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::Rejected { message }
            | Self::DependencyFailure { message }
            | Self::InternalFailure { message } => message,
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for ReplicationPeerAuthenticationError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlplaneCredentialError {
    Rejected { message: String },
    DependencyFailure { message: String },
    InternalFailure { message: String },
}

impl std::fmt::Display for ControlplaneCredentialError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::Rejected { message }
            | Self::DependencyFailure { message }
            | Self::InternalFailure { message } => message,
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for ControlplaneCredentialError {}

/// Consumer-owned authentication port for internal RPC peer certificates.
#[async_trait::async_trait]
pub trait ReplicationPeerAuthenticator: Send + Sync {
    async fn authenticate(
        &self,
        certificate: &klights_types::TlsClientCertificate,
    ) -> Result<ReplicationPeerIdentity, ReplicationPeerAuthenticationError>;
}

/// Consumer-owned certificate/key operation port used during control-plane
/// bootstrap. The replication transport never owns auth policy or key crypto.
#[async_trait::async_trait]
pub trait ControlplaneCredentialIssuer: Send + Sync {
    async fn sign_server_csr(
        &self,
        ca_cert_pem: &str,
        ca_key_pem: &str,
        csr_pem: Vec<u8>,
    ) -> Result<String, ControlplaneCredentialError>;

    async fn encrypt_key_material(
        &self,
        join_token: &str,
        plaintext: &[u8],
    ) -> Result<(Vec<u8>, Vec<u8>), ControlplaneCredentialError>;
}

impl ReplicationServerPorts {
    pub fn from_split<T>(
        shared: Arc<T>,
        resource_command: Arc<dyn klights_leader_api::LeaderResourceCommand>,
        authenticated_outbox: Arc<dyn klights_leader_api::LeaderAuthenticatedOutboxDelivery>,
        projected_token: Arc<
            dyn klights_leader_api::LeaderAuthenticatedProjectedServiceAccountToken,
        >,
    ) -> Self
    where
        T: klights_leader_api::LeaderResourceQuery
            + klights_leader_api::LeaderWatch
            + klights_leader_api::LeaderPodCleanupIntents
            + klights_leader_api::LeaderNodeLeaseRenewal
            + klights_leader_api::LeaderNodeSubnetAllocation
            + klights_leader_api::LeaderNetworkTopologyQuery
            + klights_leader_api::LeaderNetworkTopologyCommand
            + Send
            + Sync
            + 'static,
    {
        Self {
            resource_query: shared.clone(),
            resource_command,
            watch: shared.clone(),
            projected_token,
            pod_cleanup: shared.clone(),
            node_lease: shared.clone(),
            node_subnet: shared.clone(),
            topology_query: shared.clone(),
            topology_command: shared,
            authenticated_outbox,
        }
    }

    pub fn from_shared<T>(
        shared: Arc<T>,
        projected_token: Arc<
            dyn klights_leader_api::LeaderAuthenticatedProjectedServiceAccountToken,
        >,
    ) -> Self
    where
        T: klights_leader_api::LeaderResourceQuery
            + klights_leader_api::LeaderResourceCommand
            + klights_leader_api::LeaderWatch
            + klights_leader_api::LeaderPodCleanupIntents
            + klights_leader_api::LeaderNodeLeaseRenewal
            + klights_leader_api::LeaderNodeSubnetAllocation
            + klights_leader_api::LeaderNetworkTopologyQuery
            + klights_leader_api::LeaderNetworkTopologyCommand
            + klights_leader_api::LeaderAuthenticatedOutboxDelivery
            + Send
            + Sync
            + 'static,
    {
        Self {
            resource_query: shared.clone(),
            resource_command: shared.clone(),
            watch: shared.clone(),
            projected_token,
            pod_cleanup: shared.clone(),
            node_lease: shared.clone(),
            node_subnet: shared.clone(),
            topology_query: shared.clone(),
            topology_command: shared.clone(),
            authenticated_outbox: shared,
        }
    }
}

pub struct GrpcReplicationServer {
    runtime: GrpcReplicationRuntimePorts,
    ports: ReplicationServerPorts,
    wall_clock: Arc<dyn GrpcWallClock>,
    node_self_query: Option<Arc<dyn klights_leader_api::LeaderResourceQuery>>,
    node_self_status: Option<Arc<dyn klights_leader_api::LeaderNodeSelfStatus>>,
    node_lifecycle_status: Option<Arc<dyn klights_leader_api::LeaderNodeLifecycleStatus>>,
    peer_authenticator: Arc<dyn ReplicationPeerAuthenticator>,
    credential_issuer: Arc<dyn ControlplaneCredentialIssuer>,
    /// Phase 3 raft RPC dispatcher. Populated by the leader bootstrap
    /// (P3-11c) when raft mode is wired. When None, the three Raft
    /// RPCs respond with `RaftRpcRouterError::Disabled` so the client
    /// side can translate it into `RPCError::Unreachable`.
    raft_rpc_router: Option<Arc<dyn crate::raft_rpc::RaftRpcRouter>>,
    /// Phase 3 controlplane join handler. Populated alongside
    /// `raft_rpc_router` by the leader bootstrap. When None,
    /// `JoinAsControlplane` is denied with a fixed reason.
    controlplane_join_handler: Option<Arc<dyn klights_leader_api::ControlplaneJoinHandler>>,
    /// Supervised reader for in-band CA distribution/signing material.
    controlplane_ca_files: ControlplaneCaFiles,
    /// Fenced authority for leader-owned worker RPCs.
    authority: Option<Arc<dyn klights_leader_api::LeaderAuthority>>,
    local_node_name: Option<String>,
    /// bug-grpc A1/B2: per-stream watch heartbeat cadence, from the shared
    /// `GrpcTransportPolicy`.
    watch_heartbeat_interval: Duration,
}

impl GrpcReplicationServer {
    fn from_parts(
        runtime: GrpcReplicationRuntimePorts,
        ports: ReplicationServerPorts,
        peer_authenticator: Arc<dyn ReplicationPeerAuthenticator>,
        credential_issuer: Arc<dyn ControlplaneCredentialIssuer>,
        wall_clock: Arc<dyn GrpcWallClock>,
    ) -> Self {
        let controlplane_ca_files = ControlplaneCaFiles::new(runtime.supervision.task_supervisor());
        Self {
            runtime,
            ports,
            wall_clock,
            node_self_query: None,
            node_self_status: None,
            node_lifecycle_status: None,
            peer_authenticator,
            credential_issuer,
            raft_rpc_router: None,
            controlplane_join_handler: None,
            controlplane_ca_files,
            authority: None,
            local_node_name: None,
            watch_heartbeat_interval: Duration::MAX,
        }
    }

    /// bug-grpc A1/B2: override the watch heartbeat cadence from the shared
    /// transport policy (and let tests shrink it to milliseconds).
    pub fn with_watch_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.watch_heartbeat_interval = interval;
        self
    }

    pub fn with_node_query(
        mut self,
        query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    ) -> Self {
        self.node_self_query = Some(query);
        self
    }

    pub fn with_node_self_status(
        mut self,
        status: Arc<dyn klights_leader_api::LeaderNodeSelfStatus>,
    ) -> Self {
        self.node_self_status = Some(status);
        self
    }

    pub fn with_node_lifecycle_status(
        mut self,
        status: Arc<dyn klights_leader_api::LeaderNodeLifecycleStatus>,
    ) -> Self {
        self.node_lifecycle_status = Some(status);
        self
    }

    pub fn new_with_ports(
        runtime: GrpcReplicationRuntimePorts,
        ports: ReplicationServerPorts,
        peer_authenticator: Arc<dyn ReplicationPeerAuthenticator>,
        credential_issuer: Arc<dyn ControlplaneCredentialIssuer>,
        wall_clock: Arc<dyn GrpcWallClock>,
    ) -> Self {
        Self::from_parts(
            runtime,
            ports,
            peer_authenticator,
            credential_issuer,
            wall_clock,
        )
    }

    /// P3-11b: attach a Raft RPC dispatcher so this server can handle
    /// `RaftAppendEntries` / `RaftVote` / `RaftInstallSnapshot` calls
    /// from peer voters. The dispatcher is provided by the leader
    /// bootstrap (P3-11c) when raft mode is wired.
    pub fn with_raft_rpc_router(mut self, router: Arc<dyn crate::raft_rpc::RaftRpcRouter>) -> Self {
        self.raft_rpc_router = Some(router);
        self
    }

    /// P3-11c: attach a `ControlplaneJoinHandler` so this server can
    /// service `JoinAsControlplane` RPCs from peer voters that want to
    /// be added to the cluster via `RaftNode::add_voter`.
    pub fn with_controlplane_join_handler(
        mut self,
        handler: Arc<dyn klights_leader_api::ControlplaneJoinHandler>,
    ) -> Self {
        self.controlplane_join_handler = Some(handler);
        self
    }

    /// Set the runtime paths used to locate CA cert/key files.
    pub fn with_runtime_files(mut self, files: ReplicationRuntimeFiles) -> Self {
        self.controlplane_ca_files.set_files(files);
        self
    }

    async fn service_account_signing_key_pem(&self) -> std::result::Result<String, Status> {
        self.controlplane_ca_files
            .service_account_signing_key_pem()
            .await
    }

    pub fn with_authority(
        mut self,
        authority: Arc<dyn klights_leader_api::LeaderAuthority>,
    ) -> Self {
        self.authority = Some(authority);
        self
    }

    pub fn with_wall_clock(mut self, wall_clock: Arc<dyn GrpcWallClock>) -> Self {
        self.wall_clock = wall_clock;
        self
    }

    pub fn with_projected_token(
        mut self,
        projected_token: Arc<
            dyn klights_leader_api::LeaderAuthenticatedProjectedServiceAccountToken,
        >,
    ) -> Self {
        self.ports.projected_token = projected_token;
        self
    }

    pub fn with_local_node_name(mut self, node_name: impl Into<String>) -> Self {
        let node_name = node_name.into();
        if !node_name.trim().is_empty() {
            self.local_node_name = Some(node_name);
        }
        self
    }

    fn require_raft_leader(&self) -> std::result::Result<(), Status> {
        if let Some(authority) = &self.authority {
            let klights_leader_api::AuthorityRoute::Local(permit) = authority.route() else {
                return Err(Status::failed_precondition("not current leader authority"));
            };
            authority
                .validate(&permit)
                .map_err(|_| Status::failed_precondition("stale leader authority"))?;
        }
        Ok(())
    }

    fn sample_raft_leadership(
        &self,
    ) -> std::result::Result<Option<klights_leader_api::AuthorityPermit>, Status> {
        let Some(authority) = &self.authority else {
            return Ok(None);
        };
        let klights_leader_api::AuthorityRoute::Local(permit) = authority.route() else {
            return Err(Status::failed_precondition("not current leader authority"));
        };
        authority
            .validate(&permit)
            .map_err(|_| Status::failed_precondition("stale leader authority"))?;
        Ok(Some(permit))
    }

    fn require_raft_leadership_unchanged(
        &self,
        permit: Option<&klights_leader_api::AuthorityPermit>,
    ) -> std::result::Result<(), Status> {
        if let (Some(authority), Some(permit)) = (&self.authority, permit) {
            authority.validate(permit).map_err(|_| {
                Status::failed_precondition("leader authority changed during leader-fresh read")
            })?;
        }
        Ok(())
    }

    /// Authenticate a raft consensus RPC (append-entries / vote /
    /// install-snapshot). Raft peers are all control-plane voters. They must
    /// present their node (`system:node:<name>` + `system:nodes`) client
    /// certificate. Bootstrap tokens are only for CSR bootstrap and admin certs
    /// are not raft peer credentials.
    async fn require_raft_peer_auth<T>(
        &self,
        request: &Request<T>,
    ) -> std::result::Result<(), Status> {
        self.require_controlplane_node_auth(request, "raft consensus RPCs")
            .await
    }

    async fn require_controlplane_node_auth<T>(
        &self,
        request: &Request<T>,
        action: &'static str,
    ) -> std::result::Result<(), Status> {
        let Some(cert) = request
            .extensions()
            .get::<klights_types::TlsClientCertificate>()
        else {
            return Err(Status::unauthenticated(format!(
                "{action} require a node client certificate"
            )));
        };
        let identity = authenticate_peer_identity(self.peer_authenticator.as_ref(), cert).await?;
        let _node_name = identity
            .username
            .strip_prefix("system:node:")
            .filter(|name| !name.is_empty())
            .filter(|_| identity.groups.iter().any(|group| group == "system:nodes"))
            .ok_or_else(|| {
                Status::unauthenticated("control-plane certificate must be a node identity")
            })?;

        // A node client certificate is necessary but NOT sufficient: every
        // worker also holds a `system:node:`/`system:nodes` cert signed by the
        // cluster CA. Consensus RPCs (vote / append-entries / install-snapshot)
        // must originate from a control-plane node. Control-plane node certs are
        // minted only through the controlplane-token-gated bootstrap and carry
        // the `system:controlplanes` group in addition to `system:nodes`; a
        // worker's node cert (signed via the Kubernetes CSR API) carries only
        // `system:nodes`. Authorizing on this group — rather than on the local
        // node's raft membership view — stops a worker from driving consensus
        // (e.g. a `vote` with an inflated term forcing the leader to step down)
        // while letting a freshly-joining control-plane authorize immediately,
        // before it has caught up enough to learn cluster membership.
        if !identity
            .groups
            .iter()
            .any(|group| group == "system:controlplanes")
        {
            return Err(Status::permission_denied(format!(
                "{action} require a system:controlplanes node certificate"
            )));
        }
        Ok(())
    }

    async fn require_controlplane_join_token(
        &self,
        metadata: &MetadataMap,
    ) -> std::result::Result<(), Status> {
        let supplied = metadata
            .get(JOIN_TOKEN_METADATA_KEY)
            .ok_or_else(|| Status::unauthenticated("missing replication bootstrap token"))?
            .to_str()
            .map_err(|_| Status::unauthenticated("invalid replication bootstrap token metadata"))?;
        self.runtime
            .bootstrap
            .validate_controlplane_bootstrap_token(supplied)
            .await
        .map_err(|err| {
            tracing::warn!(error = %err, "invalid controlplane bootstrap token for gRPC unary auth");
            Status::unauthenticated(format!("invalid controlplane bootstrap token: {err}"))
        })?;
        Ok(())
    }

    async fn require_steady_state_auth<T>(
        &self,
        request: &Request<T>,
    ) -> std::result::Result<ReplicationPeerIdentity, Status> {
        self.node_client_identity(request).await?.ok_or_else(|| {
            Status::unauthenticated(
                "steady-state replication RPC requires a node client certificate",
            )
        })
    }
    async fn node_client_identity<T>(
        &self,
        request: &Request<T>,
    ) -> std::result::Result<Option<ReplicationPeerIdentity>, Status> {
        let Some(cert) = request
            .extensions()
            .get::<klights_types::TlsClientCertificate>()
        else {
            return Ok(None);
        };
        let identity = authenticate_peer_identity(self.peer_authenticator.as_ref(), cert).await?;
        validate_node_client_identity(&identity, None)?;
        Ok(Some(identity))
    }
}

async fn authenticate_peer_identity(
    authenticator: &dyn ReplicationPeerAuthenticator,
    certificate: &klights_types::TlsClientCertificate,
) -> std::result::Result<ReplicationPeerIdentity, Status> {
    authenticator
        .authenticate(certificate)
        .await
        .map_err(replication_peer_authentication_status)
}

fn replication_peer_authentication_status(error: ReplicationPeerAuthenticationError) -> Status {
    match error {
        ReplicationPeerAuthenticationError::Rejected { message } => {
            Status::unauthenticated(format!("invalid node client certificate: {message}"))
        }
        ReplicationPeerAuthenticationError::DependencyFailure { message } => {
            Status::unavailable(message)
        }
        ReplicationPeerAuthenticationError::InternalFailure { message } => {
            Status::internal(message)
        }
    }
}

fn controlplane_credential_status(
    operation: &'static str,
    error: ControlplaneCredentialError,
) -> Status {
    let message = format!("{operation} failed: {error}");
    match error {
        ControlplaneCredentialError::Rejected { .. } => Status::invalid_argument(message),
        ControlplaneCredentialError::DependencyFailure { .. } => Status::unavailable(message),
        ControlplaneCredentialError::InternalFailure { .. } => Status::internal(message),
    }
}

async fn encrypt_controlplane_key_material(
    issuer: &dyn ControlplaneCredentialIssuer,
    operation: &'static str,
    join_token: &str,
    plaintext: &[u8],
) -> std::result::Result<(Vec<u8>, Vec<u8>), Status> {
    issuer
        .encrypt_key_material(join_token, plaintext)
        .await
        .map_err(|error| controlplane_credential_status(operation, error))
}

fn validate_node_client_identity(
    identity: &ReplicationPeerIdentity,
    expected_node_name: Option<&str>,
) -> std::result::Result<(), Status> {
    let Some(node_name) = identity.username.strip_prefix("system:node:") else {
        return Err(Status::unauthenticated(
            "node client certificate username must use system:node:<node>",
        ));
    };
    if !identity.groups.iter().any(|group| group == "system:nodes") {
        return Err(Status::unauthenticated(
            "node client certificate is missing system:nodes group",
        ));
    }
    if let Some(expected) = expected_node_name
        && node_name != expected
    {
        return Err(Status::unauthenticated(
            "node client certificate username does not match join nodeName",
        ));
    }
    Ok(())
}

/// The authority of a caller to a node-scoped RPC (Kubernetes NodeRestriction).
#[derive(Debug)]
enum CallerAuthority {
    /// Not a `system:nodes` identity (control-plane/admin cert, a non-node cert,
    /// or no cert). Like upstream NodeRestriction — which only constrains the
    /// `system:nodes` group — these callers are not node-bound.
    Unrestricted,
    /// A node identity (`system:node:<name>` + `system:nodes`), constrained to
    /// its own node name.
    Node(String),
}

/// Classify the caller of a node-scoped RPC from its mTLS client certificate.
///
/// NodeRestriction only constrains the `system:nodes` group (matching upstream
/// Kubernetes): a request carrying a `system:node:<name>` certificate is bound
/// to `<name>`, so a compromised worker presenting its own cert cannot claim
/// another node. Control-plane (`system:masters`) certs, other certs, and
/// missing certificates are not node identities and are left unrestricted. The
/// node-scoped RPC handlers call `require_steady_state_auth` before this helper,
/// so token-only/no-cert callers do not reach the unrestricted branch.
fn node_authority_from_identity(identity: &ReplicationPeerIdentity) -> CallerAuthority {
    let is_controlplane = identity
        .groups
        .iter()
        .any(|group| group == "system:controlplanes");
    let is_node = !is_controlplane
        && identity.username.starts_with("system:node:")
        && identity.groups.iter().any(|group| group == "system:nodes");
    if is_node {
        let node = identity
            .username
            .strip_prefix("system:node:")
            .unwrap_or_default()
            .to_string();
        CallerAuthority::Node(node)
    } else {
        CallerAuthority::Unrestricted
    }
}

/// Projected token issuance is stricter than general NodeRestriction: every
/// authenticated node certificate, including a control-plane node certificate,
/// may mint only for the exact node encoded in its `system:node:<name>` CN.
fn exact_projected_token_node_authority(identity: &ReplicationPeerIdentity) -> CallerAuthority {
    let is_node = identity.username.starts_with("system:node:")
        && identity.groups.iter().any(|group| group == "system:nodes");
    if is_node {
        CallerAuthority::Node(
            identity
                .username
                .strip_prefix("system:node:")
                .unwrap_or_default()
                .to_string(),
        )
    } else {
        CallerAuthority::Unrestricted
    }
}

/// Enforce that the caller is permitted to act for `claimed_node`.
fn enforce_node_authority(
    caller: &CallerAuthority,
    claimed_node: &str,
) -> std::result::Result<(), Status> {
    match caller {
        CallerAuthority::Unrestricted => Ok(()),
        CallerAuthority::Node(node) if node == claimed_node => Ok(()),
        CallerAuthority::Node(node) => Err(Status::permission_denied(format!(
            "node \"{node}\" may not act for node \"{claimed_node}\""
        ))),
    }
}

fn apply_outbox_error_response(
    error: klights_leader_api::OutboxDeliveryError,
) -> Response<klights_internal_protobuf::ApplyOutboxResponse> {
    let error = match error {
        klights_leader_api::OutboxDeliveryError::ConflictTerminal(message)
            if message
                .to_ascii_lowercase()
                .contains("uid precondition failed") =>
        {
            klights_leader_api::OutboxDeliveryError::UidMismatch {
                expected: "<unknown>".to_string(),
                actual: "<unknown>".to_string(),
            }
        }
        other => other,
    };
    let error_type = match &error {
        klights_leader_api::OutboxDeliveryError::CodecIncompatible { .. } => "CodecIncompatible",
        klights_leader_api::OutboxDeliveryError::Retryable(_) => "Retryable",
        klights_leader_api::OutboxDeliveryError::NotFound(_) => "NotFound",
        klights_leader_api::OutboxDeliveryError::UidMismatch { .. } => "UidMismatch",
        klights_leader_api::OutboxDeliveryError::ConflictTerminal(_) => "ConflictTerminal",
        klights_leader_api::OutboxDeliveryError::InvalidRequest { .. } => "InvalidRequest",
        klights_leader_api::OutboxDeliveryError::NotLeader => "NotLeader",
        klights_leader_api::OutboxDeliveryError::Timeout => "Timeout",
        klights_leader_api::OutboxDeliveryError::Cancelled => "Cancelled",
        klights_leader_api::OutboxDeliveryError::CorruptResponse { .. } => "CorruptResponse",
        _ => "CorruptResponse",
    };
    Response::new(klights_internal_protobuf::ApplyOutboxResponse {
        already_applied: false,
        applied_rv: 0,
        error: Some(error.to_string()),
        error_type: Some(error_type.to_string()),
    })
}

/// P3-11c: full mount that also wires the Raft RPC dispatcher and the
/// `JoinAsControlplane` handler. Either may be `None`; when both are
/// None this is functionally equivalent to
/// the server without either optional handler.
///
/// bug-grpc A1: `mount_service_full` taking the shared
/// [`GrpcTransportPolicy`]. The server applies the policy's
/// `max_message_bytes` to the tonic service's decode/encode limits (these
/// were previously **unset** server-side — an unbounded request could OOM
/// the leader). The over-limit rejection is exercised by
/// `server_rejects_request_over_policy_message_limit`.
#[allow(clippy::too_many_arguments)]
pub fn mount_service_full_production(
    app: axum::Router,
    runtime: GrpcReplicationRuntimePorts,
    ports: ReplicationServerPorts,
    peer_authenticator: Arc<dyn ReplicationPeerAuthenticator>,
    credential_issuer: Arc<dyn ControlplaneCredentialIssuer>,
    wall_clock: Arc<dyn GrpcWallClock>,
    raft_rpc_router: Option<Arc<dyn crate::raft_rpc::RaftRpcRouter>>,
    controlplane_join_handler: Option<Arc<dyn klights_leader_api::ControlplaneJoinHandler>>,
    runtime_files: ReplicationRuntimeFiles,
    authority: Option<Arc<dyn klights_leader_api::LeaderAuthority>>,
    local_node_name: Option<String>,
    node_self_query: Option<Arc<dyn klights_leader_api::LeaderResourceQuery>>,
    node_self_status: Option<Arc<dyn klights_leader_api::LeaderNodeSelfStatus>>,
    node_lifecycle_status: Option<Arc<dyn klights_leader_api::LeaderNodeLifecycleStatus>>,
    transport_policy: Arc<crate::transport_policy::GrpcTransportPolicy>,
) -> axum::Router {
    let mut grpc = GrpcReplicationServer::new_with_ports(
        runtime,
        ports,
        peer_authenticator,
        credential_issuer,
        wall_clock,
    )
    .with_runtime_files(runtime_files)
    .with_watch_heartbeat_interval(transport_policy.watch_heartbeat_interval);
    if let Some(authority) = authority {
        grpc = grpc.with_authority(authority);
    }
    if let Some(local_node_name) = local_node_name {
        grpc = grpc.with_local_node_name(local_node_name);
    }
    if let Some(query) = node_self_query {
        grpc = grpc.with_node_query(query);
    }
    if let Some(status) = node_self_status {
        grpc = grpc.with_node_self_status(status);
    }
    if let Some(status) = node_lifecycle_status {
        grpc = grpc.with_node_lifecycle_status(status);
    }
    if let Some(router) = raft_rpc_router {
        grpc = grpc.with_raft_rpc_router(router);
    }
    if let Some(handler) = controlplane_join_handler {
        grpc = grpc.with_controlplane_join_handler(handler);
    }
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(klights_internal_protobuf::FILE_DESCRIPTOR_SET)
        .build_v1()
        .expect("failed to build replication gRPC reflection service");
    let max_message_bytes = transport_policy.max_message_bytes;
    let grpc_router = tonic::service::Routes::new(
        klights_internal_protobuf::replication_server::ReplicationServer::new(grpc)
            .max_decoding_message_size(max_message_bytes)
            .max_encoding_message_size(max_message_bytes),
    )
    .add_service(reflection)
    .into_axum_router();
    app.route(
        "/klights.replication.Replication/{*method}",
        axum::routing::any_service(grpc_router.clone()),
    )
    .route(
        "/grpc.reflection.v1.ServerReflection/{*method}",
        axum::routing::any_service(grpc_router.clone()),
    )
    .route(
        "/grpc.reflection.v1alpha.ServerReflection/{*method}",
        axum::routing::any_service(grpc_router),
    )
}

#[cfg(test)]
mod tests;

#[tonic::async_trait]
impl klights_internal_protobuf::replication_server::Replication for GrpcReplicationServer {
    type ConnectStream =
        BoxStream<'static, std::result::Result<klights_internal_protobuf::LeaderMessage, Status>>;
    type WatchResourcesStream =
        BoxStream<'static, std::result::Result<klights_internal_protobuf::WatchEvent, Status>>;

    async fn connect(
        &self,
        request: Request<tonic::Streaming<klights_internal_protobuf::FollowerMessage>>,
    ) -> std::result::Result<Response<Self::ConnectStream>, Status> {
        let remote_addr = request.remote_addr();
        let client_cert_identity = self.node_client_identity(&request).await?;
        let mut inbound = request.into_inner();
        let first = inbound.message().await?.ok_or_else(|| {
            Status::unauthenticated("first replication message must be JoinRequest")
        })?;
        let join = match first.payload {
            Some(klights_internal_protobuf::follower_message::Payload::Join(join)) => join,
            _ => {
                return Err(Status::unauthenticated(
                    "first replication message must be JoinRequest",
                ));
            }
        };
        require_worker_command_codec_v3(&join)?;

        let dataplane =
            validate_join_metadata_with_endpoint(&join, remote_addr.map(|addr| addr.ip()))
                .map_err(|err| Status::invalid_argument(err.to_string()))?;
        self.require_raft_leader()?;
        let role = match klights_internal_protobuf::JoinRole::try_from(join.role)
            .map_err(|_| Status::invalid_argument("unknown join role"))?
        {
            klights_internal_protobuf::JoinRole::Worker => JoinRole::Worker,
            klights_internal_protobuf::JoinRole::Unspecified => {
                return Err(Status::invalid_argument("join role must be WORKER"));
            }
        };
        let node_name = join.node_name.clone();
        let response = match client_cert_identity
            .as_ref()
            .map(|identity| validate_node_client_identity(identity, Some(&node_name)))
            .transpose()
        {
            Ok(Some(())) => {
                self.runtime
                    .bootstrap
                    .handle_authenticated_join(crate::protocol::JoinRequest {
                        token: String::new(),
                        node_name,
                        role,
                    })
                    .await
            }
            Ok(None) => JoinResponse::Rejected {
                reason: "replication stream requires a node client certificate; bootstrap tokens are only valid for CSR bootstrap".into(),
            },
            Err(status) => JoinResponse::Rejected {
                reason: status.message().to_string(),
            },
        };

        let accepted = matches!(response, JoinResponse::Accepted { .. });
        if accepted {
            self.ports
                .topology_command
                .register_node_dataplane(dataplane.clone())
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?;
            if let (Some(query), Some(status)) = (
                self.node_self_query.as_deref(),
                self.node_lifecycle_status.as_deref(),
            ) {
                publish_joining_node_external_ip(query, status, &dataplane)
                    .await
                    .map_err(|err| Status::internal(err.to_string()))?;
            }
        }
        let joined_node_name = dataplane.node_name().to_string();
        let (mut control_rx, follower_session) = if accepted {
            let (rx, session) = self
                .runtime
                .follower_sessions
                .register_follower(dataplane.clone())
                .await;
            (Some(rx), Some(session))
        } else {
            (None, None)
        };
        let first_response =
            join_response_to_proto(self.ports.topology_query.as_ref(), response).await?;
        let follower_sessions = self.runtime.follower_sessions.clone();
        let follower_completions = self.runtime.follower_completions.clone();
        let local_node_name_for_observed_endpoint = self.local_node_name.clone();
        let node_self_query_for_observed_endpoint = self.node_self_query.clone();
        let node_self_status_for_observed_endpoint = self.node_self_status.clone();
        let mut entries = if accepted {
            Some(
                follower_sessions
                    .register_stream_follower(
                        joined_node_name.clone(),
                        follower_session.expect("session must be set when accepted"),
                    )
                    .await
                    .map_err(|err| Status::internal(err.to_string()))?,
            )
        } else {
            None
        };
        let stream = async_stream::stream! {
            yield Ok(klights_internal_protobuf::LeaderMessage {
                payload: Some(klights_internal_protobuf::leader_message::Payload::JoinResponse(first_response)),
            });
            if accepted {
                if let Some(local_node_name) = local_node_name_for_observed_endpoint.as_deref() {
                    let Some(query) = node_self_query_for_observed_endpoint.as_deref() else {
                        yield Err(Status::failed_precondition("local Node query capability is unavailable"));
                        return;
                    };
                    match node_has_external_ip(query, local_node_name).await {
                        Ok(false) => {
                            yield Ok(klights_internal_protobuf::LeaderMessage {
                                payload: Some(
                                    klights_internal_protobuf::leader_message::Payload::ObserveLeaderEndpointRequest(
                                        klights_internal_protobuf::ObserveLeaderEndpointRequest {},
                                    ),
                                ),
                            });
                        }
                        Ok(true) => {}
                        Err(err) => {
                            tracing::warn!(
                                node = %joined_node_name,
                                error = %err,
                                "failed to check local Node ExternalIP before peer observation request"
                            );
                        }
                    }
                }
                let Some(mut entries) = entries.take() else {
                    yield Err(Status::internal("accepted replication stream missing fanout receiver"));
                    return;
                };
                let Some(mut control_rx) = control_rx.take() else {
                    yield Err(Status::internal("accepted replication stream missing control receiver"));
                    return;
                };
                loop {
                    tokio::select! {
                        message = inbound.message() => {
                            let message = match message {
                                Ok(Some(message)) => message,
                                Ok(None) => break,
                                Err(status) => {
                                    yield Err(status);
                                    break;
                                }
                            };
                            match message.payload {
                                // T6: legacy `Forward` payload removed. Workers now
                                // route writes through outbox -> ApplyOutbox RPC.
                                Some(klights_internal_protobuf::follower_message::Payload::Ack(ack)) => {
                                    follower_sessions.update_follower_ack(&joined_node_name, ack.applied_rv).await;
                                }
                                Some(klights_internal_protobuf::follower_message::Payload::NodeExecSyncResponse(response)) => {
                                    if let Err(err) = follower_completions.complete_node_exec_sync(
                                        FollowerCompletionContext::new(
                                            &joined_node_name,
                                            follower_session.expect("accepted stream has a follower session"),
                                            NodeOperationKind::ExecSync,
                                        ),
                                        node_exec_sync_response_from_proto(response),
                                    ).await {
                                        tracing::warn!(node = %joined_node_name, error = %err, "dropped unmatched node exec response");
                                    }
                                }
                                Some(klights_internal_protobuf::follower_message::Payload::PodLogResponse(response)) => {
                                    if let Err(err) = follower_completions.complete_node_log_event(
                                        FollowerCompletionContext::new(
                                            &joined_node_name,
                                            follower_session.expect("accepted stream has a follower session"),
                                            NodeOperationKind::Log,
                                        ),
                                        pod_log_response_from_proto(response),
                                    ).await {
                                        tracing::warn!(node = %joined_node_name, error = %err, "dropped unmatched pod log response");
                                    }
                                }
                                Some(klights_internal_protobuf::follower_message::Payload::NodeMetricsResponse(response)) => {
                                    if let Err(err) = follower_completions.complete_node_metrics(
                                        FollowerCompletionContext::new(
                                            &joined_node_name,
                                            follower_session.expect("accepted stream has a follower session"),
                                            NodeOperationKind::Metrics,
                                        ),
                                        node_metrics_response_from_proto(response),
                                    ).await {
                                        tracing::warn!(node = %joined_node_name, error = %err, "dropped unmatched node metrics response");
                                    }
                                }
                                Some(klights_internal_protobuf::follower_message::Payload::NodeExecStreamFrame(frame)) => {
                                    match node_exec_stream_frame_from_proto(frame) {
                                        Ok(frame) => {
                                            if let Err(err) = follower_completions.complete_node_exec_stream_frame(
                                                FollowerCompletionContext::new(
                                                    &joined_node_name,
                                                    follower_session.expect("accepted stream has a follower session"),
                                                    NodeOperationKind::ExecStream,
                                                ),
                                                frame,
                                            ).await {
                                                tracing::warn!(node = %joined_node_name, error = %err, "dropped unmatched node exec stream frame");
                                            }
                                        }
                                        Err(err) => {
                                            tracing::warn!(node = %joined_node_name, error = %err, "dropped invalid node exec stream frame");
                                        }
                                    }
                                }
                                Some(klights_internal_protobuf::follower_message::Payload::ObservedLeaderEndpoint(observed)) => {
                                    if let (
                                        Some(local_node_name),
                                        Some(node_query),
                                        Some(node_status),
                                    ) = (
                                        local_node_name_for_observed_endpoint.as_deref(),
                                        node_self_query_for_observed_endpoint.as_deref(),
                                        node_self_status_for_observed_endpoint.as_deref(),
                                    )
                                        && let Err(err) = refresh_local_node_external_ip_from_observed_endpoint(
                                            node_query,
                                            node_status,
                                            local_node_name,
                                            &observed.endpoint,
                                        ).await
                                    {
                                        tracing::warn!(
                                            node = %joined_node_name,
                                            endpoint = %observed.endpoint,
                                            error = %err,
                                            "failed to refresh local Node ExternalIP from follower-observed leader endpoint"
                                        );
                                    }
                                }
                                Some(klights_internal_protobuf::follower_message::Payload::Join(_)) | None => {
                                    yield Err(Status::invalid_argument("unexpected JoinRequest after stream start"));
                                    break;
                                }
                            }
                        }
                        control = control_rx.recv() => {
                            let Some(control) = control else {
                                break;
                            };
                            match control {
                                FollowerControlMessage::NodeExecSync(request) => {
                                    yield Ok(klights_internal_protobuf::LeaderMessage {
                                        payload: Some(klights_internal_protobuf::leader_message::Payload::NodeExecSyncRequest(
                                            node_exec_sync_request_to_proto(request),
                                        )),
                                    });
                                }
                                FollowerControlMessage::NodeExec(request) => {
                                    yield Ok(klights_internal_protobuf::LeaderMessage {
                                        payload: Some(klights_internal_protobuf::leader_message::Payload::NodeExecRequest(
                                            node_exec_request_to_proto(request),
                                        )),
                                    });
                                }
                                FollowerControlMessage::NodeExecFrame(frame) => {
                                    yield Ok(klights_internal_protobuf::LeaderMessage {
                                        payload: Some(klights_internal_protobuf::leader_message::Payload::NodeExecStreamFrame(
                                            node_exec_stream_frame_to_proto(frame),
                                        )),
                                    });
                                }
                                FollowerControlMessage::PodLog(request) => {
                                    yield Ok(klights_internal_protobuf::LeaderMessage {
                                        payload: Some(klights_internal_protobuf::leader_message::Payload::PodLogRequest(
                                            pod_log_request_to_proto(request),
                                        )),
                                    });
                                }
                                FollowerControlMessage::NodeMetrics(request) => {
                                    yield Ok(klights_internal_protobuf::LeaderMessage {
                                        payload: Some(klights_internal_protobuf::leader_message::Payload::NodeMetricsRequest(
                                            node_metrics_request_to_proto(request),
                                        )),
                                    });
                                }
                            }
                        }
                        entry = entries.recv() => {
                            let Some(entry) = entry else {
                                break;
                            };
                            let entry = match entry_to_proto(&entry) {
                                Ok(entry) => entry,
                                Err(err) => {
                                    yield Err(Status::internal(err.to_string()));
                                    break;
                                }
                            };
                            yield Ok(klights_internal_protobuf::LeaderMessage {
                                payload: Some(klights_internal_protobuf::leader_message::Payload::StreamItem(
                                    klights_internal_protobuf::StreamItem {
                                        item: Some(klights_internal_protobuf::stream_item::Item::Entry(entry)),
                                    }
                                )),
                            });
                        }
                    }
                }
                if let Some(session) = follower_session {
                    follower_sessions.unregister_follower(&joined_node_name, session).await;
                }
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_metadata(
        &self,
        request: Request<klights_internal_protobuf::MetadataRequest>,
    ) -> std::result::Result<Response<klights_internal_protobuf::MetadataResponse>, Status> {
        self.require_steady_state_auth(&request).await?;
        let metadata = self.runtime.metadata.handle_metadata().await;
        Ok(Response::new(klights_internal_protobuf::MetadataResponse {
            cluster_id: metadata.cluster_id,
            leader_epoch: metadata.leader_epoch,
            current_rv: metadata.current_rv,
            current_log_index: metadata.current_log_index,
            command_codec_version: metadata.command_codec_version,
        }))
    }

    async fn get_resource(
        &self,
        request: Request<klights_internal_protobuf::GetResourceRequest>,
    ) -> std::result::Result<Response<klights_internal_protobuf::GetResourceResponse>, Status> {
        self.require_steady_state_auth(&request).await?;
        let leadership_rx = self.sample_raft_leadership()?;
        let req = request.into_inner();
        let query = klights_leader_api::ResourceGetRequest::try_new(
            klights_types::ResourceKey {
                api_version: req.api_version,
                kind: req.kind,
                namespace: req.namespace,
                name: req.name,
            },
            klights_leader_api::ResourceQueryConsistency::LeaderFresh,
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let resource = self
            .ports
            .resource_query
            .get_resource(query)
            .await
            .map_err(|err| Status::unavailable(err.to_string()))?;
        self.require_raft_leadership_unchanged(leadership_rx.as_ref())?;
        Ok(Response::new(match resource {
            Some(resource) => klights_internal_protobuf::GetResourceResponse {
                found: true,
                resource: Some(resource_to_proto(&resource)),
            },
            None => klights_internal_protobuf::GetResourceResponse {
                found: false,
                resource: None,
            },
        }))
    }

    async fn list_resources(
        &self,
        request: Request<klights_internal_protobuf::ListResourcesRequest>,
    ) -> std::result::Result<Response<klights_internal_protobuf::ListResourcesResponse>, Status>
    {
        self.require_steady_state_auth(&request).await?;
        let leadership_rx = self.sample_raft_leadership()?;
        let req = request.into_inner();
        let query = klights_leader_api::ResourceListRequest::try_new(
            req.api_version,
            req.kind,
            req.namespace,
            req.label_selector,
            req.field_selector,
            req.limit,
            req.continue_token,
            klights_leader_api::ResourceQueryConsistency::LeaderFresh,
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let list = self
            .ports
            .resource_query
            .list_resources(query)
            .await
            .map_err(|err| Status::unavailable(err.to_string()))?;
        self.require_raft_leadership_unchanged(leadership_rx.as_ref())?;
        let (items, resource_version, watch_replay_position, continue_token, remaining_item_count) =
            list.into_parts();
        let items: Vec<klights_internal_protobuf::ResourceObject> =
            items.iter().map(resource_to_proto).collect();
        Ok(Response::new(
            klights_internal_protobuf::ListResourcesResponse {
                total: items.len() as i64,
                items,
                continue_token,
                resource_version,
                remaining_item_count,
                watch_replay_position: watch_replay_position.map(watch_replay_position_to_proto),
            },
        ))
    }

    async fn submit_resource_command(
        &self,
        request: Request<klights_internal_protobuf::SubmitResourceCommandRequest>,
    ) -> std::result::Result<
        Response<klights_internal_protobuf::SubmitResourceCommandResponse>,
        Status,
    > {
        self.require_controlplane_node_auth(&request, "resource command submissions")
            .await?;
        self.require_raft_leader()?;
        let request = request.into_inner();
        if !klights_cluster_core::supports_command_codec_version(request.codec_version) {
            return Err(Status::failed_precondition(format!(
                "resource command codec {} is incompatible with required codec {}",
                request.codec_version,
                klights_cluster_core::COMMAND_CODEC_VERSION
            )));
        }
        let request =
            resource_command_request_from_proto(request).map_err(resource_command_status)?;
        let result = self
            .ports
            .resource_command
            .submit_resource_command(request)
            .await
            .map_err(resource_command_status)?;
        Ok(Response::new(resource_command_result_to_proto(result)))
    }

    async fn watch_resources(
        &self,
        request: Request<klights_internal_protobuf::WatchResourcesRequest>,
    ) -> std::result::Result<Response<Self::WatchResourcesStream>, Status> {
        self.require_steady_state_auth(&request).await?;
        // Issue #4: a worker watch must be served by the current raft leader.
        // Reject establishment on a stale follower so the worker reconnects to
        // the new leader instead of streaming from a deposed node.
        let leadership_rx = self.sample_raft_leadership()?;
        let req = request.into_inner();
        let watch_request = klights_leader_api::WatchRequest::try_new(
            req.api_version.clone(),
            req.kind.clone(),
            req.namespace.clone(),
            req.label_selector.clone(),
            req.field_selector.clone(),
            req.start_resource_version,
            req.start_watch_replay_position
                .as_ref()
                .map(watch_replay_position_from_proto),
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        {
            let positioned_stream = self
                .ports
                .watch
                .watch_resources(watch_request)
                .await
                .map_err(leader_watch_error_to_status)?;
            let accepted_cursor = positioned_stream.accepted_cursor().ok_or_else(|| {
                Status::internal("local positioned watch omitted its accepted session cursor")
            })?;
            self.require_raft_leadership_unchanged(leadership_rx.as_ref())?;
            let supervisor = self.runtime.supervision.task_supervisor();
            let heartbeat_interval = self.watch_heartbeat_interval;
            let authority = self.authority.clone();
            let leader_permit = leadership_rx;
            let mut last_rv = accepted_cursor.resource_version().unwrap_or(0);
            let mut last_position = accepted_cursor
                .replay_position()
                .unwrap_or_else(|| WatchReplayPosition::from_resource_version(last_rv));
            let stream = async_stream::stream! {
                let mut positioned_stream = positioned_stream;
                loop {
                    let next = if let (Some(authority), Some(permit)) =
                        (authority.as_ref(), leader_permit.as_ref())
                    {
                        tokio::select! {
                            biased;
                            _ = authority.wait_for_revocation(permit) => break,
                            result = supervisor.timeout(
                                "grpc_positioned_watch_heartbeat",
                                heartbeat_interval,
                                futures::StreamExt::next(&mut positioned_stream),
                            ) => result,
                        }
                    } else {
                        supervisor
                            .timeout(
                                "grpc_positioned_watch_heartbeat",
                                heartbeat_interval,
                                futures::StreamExt::next(&mut positioned_stream),
                            )
                            .await
                    };
                    let event = match next {
                        Ok(Ok(Some(Ok(event)))) => event,
                        Ok(Ok(Some(Err(error)))) => {
                            yield Err(leader_watch_error_to_status(error));
                            break;
                        }
                        Ok(Ok(None)) | Err(_) => break,
                        Ok(Err(_elapsed)) => {
                            yield Ok(watch_heartbeat_proto(
                                &req.api_version,
                                &req.kind,
                                last_rv,
                                last_position,
                            ));
                            continue;
                        }
                    };
                    if let Some(position) = event.resume_position() {
                        last_position = position;
                    }
                    last_rv = last_rv.max(event.resource().resource_version);
                    yield Ok(klights_internal_protobuf::WatchEvent {
                        event_type: event.event_type().as_str().to_string(),
                        resource: Some(resource_to_proto(event.resource())),
                        resume_position: event
                            .resume_position()
                            .map(watch_replay_position_to_proto),
                    });
                }
            };
            return Ok(Response::new(Box::pin(stream)));
        }
    }

    async fn projected_service_account_token(
        &self,
        request: Request<klights_internal_protobuf::ProjectedServiceAccountTokenRequest>,
    ) -> std::result::Result<
        Response<klights_internal_protobuf::ProjectedServiceAccountTokenResponse>,
        Status,
    > {
        let authenticated_identity = self.require_steady_state_auth(&request).await?;
        self.require_raft_leader()?;
        let caller = exact_projected_token_node_authority(&authenticated_identity);
        let req = request.into_inner();
        let token_request = klights_leader_api::ProjectedServiceAccountTokenRequest::try_new(
            req.namespace,
            req.service_account_name,
            req.audiences,
            req.expiration_seconds,
            req.bound_pod_name.unwrap_or_default(),
            req.bound_pod_uid.unwrap_or_default(),
            req.bound_node_name.unwrap_or_default(),
            req.bound_node_uid,
        )
        .map_err(projected_token_error_to_status)?;
        enforce_node_authority(&caller, token_request.bound_node_name())?;
        let token = self
            .ports
            .projected_token
            .issue_authenticated_projected_service_account_token(token_request)
            .await
            .map_err(projected_token_error_to_status)?;
        Ok(Response::new(
            klights_internal_protobuf::ProjectedServiceAccountTokenResponse {
                token: token.into_token(),
            },
        ))
    }

    async fn apply_outbox(
        &self,
        request: Request<klights_internal_protobuf::ApplyOutboxRequest>,
    ) -> std::result::Result<Response<klights_internal_protobuf::ApplyOutboxResponse>, Status> {
        let authenticated_identity = self.require_steady_state_auth(&request).await?;
        self.require_raft_leader()?;
        let caller = node_authority_from_identity(&authenticated_identity);
        let authenticated_node = authenticated_identity
            .username
            .strip_prefix("system:node:")
            .ok_or_else(|| {
                Status::unauthenticated(
                    "durable outbox delivery requires an authenticated node client certificate",
                )
            })?;
        let req = request.into_inner();
        // NodeRestriction: the legacy wire author must equal the certificate-bound author.
        enforce_node_authority(&caller, &req.authoring_node)?;
        let delivery_operation =
            klights_leader_api::OutboxDeliveryOperation::try_from_wire_name(&req.operation)
                .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let delivery_request = klights_leader_api::OutboxDeliveryRequest::try_new_versioned(
            req.codec_version,
            req.idempotency_key,
            delivery_operation,
            std::sync::Arc::<[u8]>::from(req.payload_proto),
            req.client_id,
            req.stream_id,
            req.stream_seq,
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let request = klights_leader_api::AuthenticatedOutboxDeliveryRequest::try_new(
            authenticated_node,
            delivery_request,
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let result = self
            .ports
            .authenticated_outbox
            .deliver_authenticated_outbox(request)
            .await;
        match result {
            Ok(OutboxDeliveryResult::Applied { applied_rv }) => Ok(Response::new(
                klights_internal_protobuf::ApplyOutboxResponse {
                    already_applied: false,
                    applied_rv,
                    error: None,
                    error_type: None,
                },
            )),
            Ok(OutboxDeliveryResult::AlreadyApplied { applied_rv }) => Ok(Response::new(
                klights_internal_protobuf::ApplyOutboxResponse {
                    already_applied: true,
                    applied_rv: applied_rv.unwrap_or(0),
                    error: None,
                    error_type: None,
                },
            )),
            Err(err) => Ok(apply_outbox_error_response(err)),
        }
    }

    async fn renew_node_lease(
        &self,
        request: Request<klights_internal_protobuf::RenewNodeLeaseRequest>,
    ) -> std::result::Result<Response<klights_internal_protobuf::RenewNodeLeaseResponse>, Status>
    {
        let identity = self.require_steady_state_auth(&request).await?;
        self.require_raft_leader()?;
        let caller = node_authority_from_identity(&identity);
        let req = request.into_inner();
        // NodeRestriction: a node may only renew its own lease.
        enforce_node_authority(&caller, &req.node_name)?;
        if req.lease_duration_seconds <= 0 {
            return Err(Status::invalid_argument(
                "lease_duration_seconds must be positive",
            ));
        }
        validate_node_lease_renew_time_skew(&req.renew_time, self.wall_clock.now())?;
        let renewal = klights_leader_api::NodeLeaseRenewalRequest::try_new(
            req.node_name,
            req.renew_time,
            req.lease_duration_seconds,
        )
        .map_err(|err| Status::invalid_argument(err.to_string()))?;
        self.ports
            .node_lease
            .renew_node_lease(renewal)
            .await
            .map_err(|err| Status::unavailable(err.to_string()))?;
        Ok(Response::new(
            klights_internal_protobuf::RenewNodeLeaseResponse {},
        ))
    }

    async fn allocate_node_subnet(
        &self,
        request: Request<klights_internal_protobuf::AllocateNodeSubnetRequest>,
    ) -> std::result::Result<Response<klights_internal_protobuf::NodeSubnetResponse>, Status> {
        let identity = self.require_steady_state_auth(&request).await?;
        self.require_raft_leader()?;
        let authority = node_authority_from_identity(&identity);
        let req = request.into_inner();
        enforce_node_authority(&authority, &req.node_name)?;
        let focused_request = klights_leader_api::NodeSubnetAllocationRequest::try_new(
            req.node_name,
            req.cluster_cidr,
            &req.node_ip,
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let subnet = self
            .ports
            .node_subnet
            .allocate_node_subnet(focused_request)
            .await
            .map_err(node_subnet_allocation_status)?
            .into_subnet();
        Ok(Response::new(
            klights_internal_protobuf::NodeSubnetResponse {
                subnet: Some(focused_node_subnet_to_proto(subnet)),
            },
        ))
    }

    async fn get_node_subnet(
        &self,
        request: Request<klights_internal_protobuf::GetNodeSubnetRequest>,
    ) -> std::result::Result<Response<klights_internal_protobuf::GetNodeSubnetResponse>, Status>
    {
        self.require_steady_state_auth(&request).await?;
        self.require_raft_leader()?;
        let req = request.into_inner();
        let query = klights_leader_api::NodeSubnetQuery::try_new(req.node_name)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let subnet = self
            .ports
            .topology_query
            .get_node_subnet(query)
            .await
            .map_err(|err| Status::unavailable(err.to_string()))?
            .into_option();
        Ok(Response::new(match subnet {
            Some(subnet) => klights_internal_protobuf::GetNodeSubnetResponse {
                found: true,
                subnet: Some(focused_node_subnet_to_proto(subnet)),
            },
            None => klights_internal_protobuf::GetNodeSubnetResponse {
                found: false,
                subnet: None,
            },
        }))
    }

    async fn list_peer_subnets(
        &self,
        request: Request<klights_internal_protobuf::ListPeerSubnetsRequest>,
    ) -> std::result::Result<Response<klights_internal_protobuf::ListPeerSubnetsResponse>, Status>
    {
        self.require_steady_state_auth(&request).await?;
        self.require_raft_leader()?;
        let req = request.into_inner();
        let query = klights_leader_api::PeerSubnetsQuery::try_new(req.my_node_name)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let items = self
            .ports
            .topology_query
            .list_peer_subnets(query)
            .await
            .map_err(|err| Status::unavailable(err.to_string()))?
            .into_vec()
            .into_iter()
            .map(focused_node_subnet_to_proto)
            .collect();
        Ok(Response::new(
            klights_internal_protobuf::ListPeerSubnetsResponse { items },
        ))
    }

    async fn get_node_dataplane(
        &self,
        request: Request<klights_internal_protobuf::GetNodeDataplaneRequest>,
    ) -> std::result::Result<Response<klights_internal_protobuf::GetNodeDataplaneResponse>, Status>
    {
        self.require_steady_state_auth(&request).await?;
        self.require_raft_leader()?;
        let req = request.into_inner();
        let query = klights_leader_api::NodeDataplaneQuery::try_new(req.node_name)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let metadata = self
            .ports
            .topology_query
            .get_node_dataplane(query)
            .await
            .map_err(|err| Status::unavailable(err.to_string()))?
            .into_option();
        Ok(Response::new(match metadata {
            Some(metadata) => klights_internal_protobuf::GetNodeDataplaneResponse {
                found: true,
                metadata: Some(focused_dataplane_to_proto(metadata)),
            },
            None => klights_internal_protobuf::GetNodeDataplaneResponse {
                found: false,
                metadata: None,
            },
        }))
    }

    async fn observe_peer_endpoint(
        &self,
        request: Request<klights_internal_protobuf::ObservePeerEndpointRequest>,
    ) -> std::result::Result<Response<klights_internal_protobuf::ObservePeerEndpointResponse>, Status>
    {
        let identity = self.require_steady_state_auth(&request).await?;
        let caller = node_authority_from_identity(&identity);
        let observed_endpoint = request.remote_addr().map(|addr| addr.ip().to_string());
        let req = request.into_inner();
        enforce_node_authority(&caller, &req.node_name)?;
        if req.node_name.trim().is_empty() {
            return Err(Status::invalid_argument("node_name is required"));
        }

        if let Some(endpoint) = observed_endpoint {
            self.runtime
                .metadata
                .record_observed_peer_endpoint(&req.node_name, endpoint.clone())
                .await;
            return Ok(Response::new(
                klights_internal_protobuf::ObservePeerEndpointResponse {
                    found: true,
                    endpoint,
                },
            ));
        }

        Ok(Response::new(
            match self
                .runtime
                .metadata
                .observed_peer_endpoint(&req.node_name)
                .await
            {
                Some(endpoint) => klights_internal_protobuf::ObservePeerEndpointResponse {
                    found: true,
                    endpoint,
                },
                None => klights_internal_protobuf::ObservePeerEndpointResponse {
                    found: false,
                    endpoint: String::new(),
                },
            },
        ))
    }

    async fn list_pod_cleanup_intents_for_node(
        &self,
        request: Request<klights_internal_protobuf::ListPodCleanupIntentsForNodeRequest>,
    ) -> std::result::Result<
        Response<klights_internal_protobuf::ListPodCleanupIntentsForNodeResponse>,
        Status,
    > {
        let identity = self.require_steady_state_auth(&request).await?;
        self.require_raft_leader()?;
        let caller = node_authority_from_identity(&identity);
        let req = request.into_inner();
        let request = klights_leader_api::PodCleanupIntentListRequest::try_new(req.node_name)
            .map_err(pod_cleanup_intent_error_to_status)?;
        enforce_node_authority(&caller, request.node_name())?;
        let items = self
            .ports
            .pod_cleanup
            .list_pod_cleanup_intents(request)
            .await
            .map_err(pod_cleanup_intent_error_to_status)?
            .into_iter()
            .map(pod_cleanup_intent_to_proto)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(Response::new(
            klights_internal_protobuf::ListPodCleanupIntentsForNodeResponse { items },
        ))
    }

    async fn delete_pod_cleanup_intent(
        &self,
        request: Request<klights_internal_protobuf::DeletePodCleanupIntentRequest>,
    ) -> std::result::Result<
        Response<klights_internal_protobuf::DeletePodCleanupIntentResponse>,
        Status,
    > {
        let identity = self.require_steady_state_auth(&request).await?;
        self.require_raft_leader()?;
        let caller = node_authority_from_identity(&identity);
        let req = request.into_inner();
        let request = klights_leader_api::PodCleanupIntentAckRequest::try_new(
            req.node_name,
            req.namespace,
            req.pod_name,
            req.pod_uid,
            req.reason,
        )
        .map_err(pod_cleanup_intent_error_to_status)?;
        // NodeRestriction: a node may only clear its own pod cleanup intents.
        enforce_node_authority(&caller, request.node_name())?;
        self.ports
            .pod_cleanup
            .acknowledge_pod_cleanup_intent(request)
            .await
            .map_err(pod_cleanup_intent_error_to_status)?;
        Ok(Response::new(
            klights_internal_protobuf::DeletePodCleanupIntentResponse {},
        ))
    }

    // ── Phase 3 Raft consensus RPCs (P3-11b) ────────────────────────────

    async fn raft_append_entries(
        &self,
        request: Request<klights_internal_protobuf::RaftAppendEntriesRequest>,
    ) -> std::result::Result<Response<klights_internal_protobuf::RaftAppendEntriesResponse>, Status>
    {
        self.require_raft_peer_auth(&request).await?;
        let request = request.into_inner();
        crate::protocol::require_exact_command_codec(
            request.command_codec_version,
            "Raft append-entries peer",
        )
        .map_err(Status::failed_precondition)?;
        let payload = request.payload;
        let receiver: crate::raft_rpc::RaftReceiverAdmission =
            serde_json::from_slice(&request.receiver_admission).map_err(|error| {
                Status::failed_precondition(format!(
                    "invalid exact-v3 Raft receiver admission proof: {error}"
                ))
            })?;
        Ok(Response::new(
            klights_internal_protobuf::RaftAppendEntriesResponse {
                result: Some(
                    match dispatch_raft_rpc(self.raft_rpc_router.as_ref(), |r| {
                        r.append_entries(receiver.clone(), payload.clone())
                    })
                    .await
                    {
                        Ok(bytes) => {
                            klights_internal_protobuf::raft_append_entries_response::Result::Ok(
                                bytes,
                            )
                        }
                        Err(msg) => {
                            klights_internal_protobuf::raft_append_entries_response::Result::Error(
                                msg,
                            )
                        }
                    },
                ),
            },
        ))
    }

    async fn raft_vote(
        &self,
        request: Request<klights_internal_protobuf::RaftVoteRequest>,
    ) -> std::result::Result<Response<klights_internal_protobuf::RaftVoteResponse>, Status> {
        self.require_raft_peer_auth(&request).await?;
        let request = request.into_inner();
        crate::protocol::require_exact_command_codec(
            request.command_codec_version,
            "Raft vote peer",
        )
        .map_err(Status::failed_precondition)?;
        let payload = request.payload;
        let receiver: crate::raft_rpc::RaftReceiverAdmission =
            serde_json::from_slice(&request.receiver_admission).map_err(|error| {
                Status::failed_precondition(format!(
                    "invalid exact-v3 Raft receiver admission proof: {error}"
                ))
            })?;
        Ok(Response::new(klights_internal_protobuf::RaftVoteResponse {
            result: Some(
                match dispatch_raft_rpc(self.raft_rpc_router.as_ref(), |r| {
                    r.vote(receiver.clone(), payload.clone())
                })
                .await
                {
                    Ok(bytes) => klights_internal_protobuf::raft_vote_response::Result::Ok(bytes),
                    Err(msg) => klights_internal_protobuf::raft_vote_response::Result::Error(msg),
                },
            ),
        }))
    }

    async fn raft_install_snapshot(
        &self,
        request: Request<klights_internal_protobuf::RaftInstallSnapshotRequest>,
    ) -> std::result::Result<Response<klights_internal_protobuf::RaftInstallSnapshotResponse>, Status>
    {
        self.require_raft_peer_auth(&request).await?;
        let request = request.into_inner();
        crate::protocol::require_exact_command_codec(
            request.command_codec_version,
            "Raft snapshot peer",
        )
        .map_err(Status::failed_precondition)?;
        let payload = request.payload;
        let receiver: crate::raft_rpc::RaftReceiverAdmission =
            serde_json::from_slice(&request.receiver_admission).map_err(|error| {
                Status::failed_precondition(format!(
                    "invalid exact-v3 Raft receiver admission proof: {error}"
                ))
            })?;
        Ok(Response::new(
            klights_internal_protobuf::RaftInstallSnapshotResponse {
                result: Some(
                    match dispatch_raft_rpc(self.raft_rpc_router.as_ref(), |r| {
                        r.install_snapshot(receiver.clone(), payload.clone())
                    })
                    .await
                    {
                        Ok(bytes) => {
                            klights_internal_protobuf::raft_install_snapshot_response::Result::Ok(
                                bytes,
                            )
                        }
                        Err(msg) => {
                            klights_internal_protobuf::raft_install_snapshot_response::Result::Error(
                                msg,
                            )
                        }
                    },
                ),
            },
        ))
    }

    async fn join_as_controlplane(
        &self,
        request: Request<klights_internal_protobuf::JoinAsControlplaneRequest>,
    ) -> std::result::Result<Response<klights_internal_protobuf::JoinAsControlplaneResponse>, Status>
    {
        let remote_addr = request.remote_addr();
        // Raft voter/learner admission must be authorized by a valid controlplane
        // bootstrap token on first join. A node client cert alone is insufficient:
        // every worker holds a `system:node:`/`system:nodes` cert, and admitting
        // one as a voter (or, ignoring the voter limit, a learner) would hand it
        // the full replicated cluster.db (all Secrets) and quorum influence.
        let controlplane_token_authenticated = self
            .require_controlplane_join_token(request.metadata())
            .await
            .is_ok();
        let client_cert_identity = self.node_client_identity(&request).await?;
        let mut req = request.into_inner();
        let Some(identity) = client_cert_identity.as_ref() else {
            return Err(Status::unauthenticated(
                "JoinAsControlplane requires a node client certificate; bootstrap tokens are only valid for CSR bootstrap",
            ));
        };
        validate_node_client_identity(identity, Some(&req.node_name))?;
        let Some(handler) = self.controlplane_join_handler.as_ref() else {
            return Ok(Response::new(klights_internal_protobuf::JoinAsControlplaneResponse {
                result: Some(klights_internal_protobuf::join_as_controlplane_response::Result::Denied(
                    klights_internal_protobuf::JoinAsControlplaneDenied {
                        reason:
                            "raft mode not enabled on this server (no controlplane join handler)"
                                .to_string(),
                    },
                )),
            }));
        };
        // Authorize: a valid controlplane token (first join) OR an existing
        // controlplane membership (restart/rejoin uses the node cert — the token
        // is short-lived and gone by then; raft membership is the persisted
        // record of "this node is an authorized control plane"). A worker has
        // neither and is rejected here.
        let existing_member = handler.is_controlplane_member(&req.node_name).await;
        if !controlplane_token_authenticated && !existing_member {
            return Err(Status::permission_denied(
                "JoinAsControlplane requires a valid controlplane bootstrap token (first join) or an existing controlplane membership (rejoin)",
            ));
        }
        let expected_node_id = klights_cluster_core::raft_node_id_for_node_name(&req.node_name);
        if req.node_id != expected_node_id {
            return Err(Status::invalid_argument(format!(
                "JoinAsControlplane node_id {} does not match the ID derived from authenticated node name {}",
                req.node_id, req.node_name
            )));
        }
        let node_registration = match req.node_registration.take() {
            Some(registration) => Some(
                validate_controlplane_node_registration(registration)
                    .map_err(|err| Status::invalid_argument(err.to_string()))?,
            ),
            None if existing_member => None,
            None => {
                return Err(Status::invalid_argument(
                    "first-time JoinAsControlplane requires node_registration host facts",
                ));
            }
        };
        let observed_ip = remote_addr.map(|addr| addr.ip());
        let dataplane =
            validate_controlplane_join_dataplane_metadata_with_endpoint(&req, observed_ip)
                .map_err(|err| Status::invalid_argument(err.to_string()))?;
        if let Some(registration) = node_registration.as_ref() {
            let mode_matches = matches!(
                (&registration.node_mode, dataplane.mode()),
                (
                    klights_leader_api::RemoteNodeMode::Root,
                    klights_leader_api::NetworkNodeMode::Root
                ) | (
                    klights_leader_api::RemoteNodeMode::Rootless,
                    klights_leader_api::NetworkNodeMode::Rootless
                )
            );
            if !mode_matches {
                return Err(Status::invalid_argument(
                    "node_registration.node_mode must match dataplane_mode",
                ));
            }
        }
        let raft_addr = raft_addr_with_observed_host(&req.addr, observed_ip)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let node_internal_ip = Some(req.node_internal_ip).filter(|value| !value.trim().is_empty());
        if let Some(internal_ip) = node_internal_ip.as_deref() {
            internal_ip.parse::<IpAddr>().map_err(|err| {
                Status::invalid_argument(format!("invalid node_internal_ip: {err}"))
            })?;
        }
        let storage_incarnation = req.storage_incarnation.trim().to_string();
        if uuid::Uuid::parse_str(&storage_incarnation).is_err() {
            return Err(Status::invalid_argument(
                "JoinAsControlplane requires a valid storage_incarnation UUID",
            ));
        }
        let storage_log_attestation = req.storage_log_attestation.ok_or_else(|| {
            Status::invalid_argument("JoinAsControlplane requires storage_log_attestation")
        })?;
        let map_log_id = |attestation: klights_internal_protobuf::RaftStorageLogId| {
            klights_leader_api::RaftStorageLogAttestation {
                term: attestation.term,
                leader_node_id: attestation.leader_node_id,
                index: attestation.index,
            }
        };
        let storage_log_attestation = klights_leader_api::RaftStorageAttestation {
            high_watermark: storage_log_attestation.high_watermark.map(map_log_id),
            current_boundary: storage_log_attestation.current_boundary.map(map_log_id),
        };
        let outcome = handler
            .join(klights_leader_api::ControlplaneJoinRequest {
                node_id: req.node_id,
                addr: raft_addr,
                node_name: req.node_name,
                as_learner: req.as_learner,
                storage_incarnation,
                storage_log_attestation,
                command_codec_version: req.command_codec_version,
                node_internal_ip,
                node_registration,
                legacy_node_git_commit: Some(req.node_git_commit)
                    .filter(|value| !value.trim().is_empty()),
            })
            .await
            .map_err(|err| Status::internal(format!("raft RPC router dispatch: {err}")))?;
        let result = match outcome {
            klights_leader_api::ControlplaneJoinOutcome::Accepted {
                voter_count_after,
                admitted_as_learner,
                ..
            } => {
                self.ports
                    .topology_command
                    .register_node_dataplane(dataplane)
                    .await
                    .map_err(|err| Status::unavailable(err.to_string()))?;
                let ca_cert_pem = self
                    .controlplane_ca_files
                    .join_response_ca_cert_pem()
                    .await?;
                klights_internal_protobuf::join_as_controlplane_response::Result::Accepted(
                    klights_internal_protobuf::JoinAsControlplaneAccepted {
                        voter_count_after,
                        admitted_as_learner,
                        ca_cert_pem,
                        encrypted_ca_key: Vec::new(),
                        ca_key_nonce: Vec::new(),
                    },
                )
            }
            klights_leader_api::ControlplaneJoinOutcome::RedirectToLeader {
                leader_id,
                leader_addr,
            } => {
                klights_internal_protobuf::join_as_controlplane_response::Result::RedirectToLeader(
                    klights_internal_protobuf::JoinAsControlplaneRedirect {
                        leader_id,
                        leader_addr,
                    },
                )
            }
            klights_leader_api::ControlplaneJoinOutcome::Denied { reason } => {
                klights_internal_protobuf::join_as_controlplane_response::Result::Denied(
                    klights_internal_protobuf::JoinAsControlplaneDenied { reason },
                )
            }
        };
        Ok(Response::new(
            klights_internal_protobuf::JoinAsControlplaneResponse {
                result: Some(result),
            },
        ))
    }

    async fn sign_controlplane_csr(
        &self,
        request: Request<klights_internal_protobuf::SignControlplaneCsrRequest>,
    ) -> std::result::Result<Response<klights_internal_protobuf::SignControlplaneCsrResponse>, Status>
    {
        let join_token = request
            .metadata()
            .get(JOIN_TOKEN_METADATA_KEY)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let token_auth = self
            .require_controlplane_join_token(request.metadata())
            .await;
        // A *valid controlplane* bootstrap token is the only credential that may
        // unlock the cluster CA private key and SA signing key below. Capture it
        // before the auth match consumes `token_auth`. Node-cert auth (used for
        // server-cert renewal by existing nodes) must never leak that material.
        let controlplane_token_authenticated = token_auth.is_ok();
        let client_cert_identity = self.node_client_identity(&request).await?;

        let req = request.into_inner();
        match token_auth {
            Ok(()) => {}
            Err(token_status) => match client_cert_identity.as_ref() {
                Some(identity) => {
                    validate_node_client_identity(identity, Some(&req.node_name))?;
                    // Cert-renewal path: a node client cert alone is NOT enough to
                    // have a CA-trusted server certificate minted. A worker holds
                    // `system:node:`/`system:nodes` too, so identity cannot
                    // distinguish it from a control plane. Only a current raft
                    // member (a node admitted via a controlplane-token-gated
                    // JoinAsControlplane) may renew its server cert this way.
                    // Otherwise a worker could mint a `klights-server` cert with
                    // attacker-chosen SANs and impersonate the API server.
                    let is_member = match self.controlplane_join_handler.as_ref() {
                        Some(handler) => handler.is_controlplane_member(&req.node_name).await,
                        None => false,
                    };
                    if !is_member {
                        return Err(Status::permission_denied(
                            "SignControlplaneCsr node-cert path is restricted to current controlplane members; present a controlplane bootstrap token to join",
                        ));
                    }
                }
                None => return Err(token_status),
            },
        }
        if req.server_csr.is_empty() {
            return Err(Status::invalid_argument("server_csr is required"));
        }

        let ca_cert_pem = self.controlplane_ca_files.signing_ca_cert_pem().await?;
        let ca_key_pem = self.controlplane_ca_files.signing_ca_key_pem().await?;
        let service_account_signing_key_pem = self.service_account_signing_key_pem().await?;

        let signed_server_cert = self
            .credential_issuer
            .sign_server_csr(&ca_cert_pem, &ca_key_pem, req.server_csr)
            .await
            .map_err(|error| match error {
                ControlplaneCredentialError::Rejected { message } => {
                    Status::invalid_argument(format!("CSR signing failed: {message}"))
                }
                ControlplaneCredentialError::DependencyFailure { message } => {
                    Status::unavailable(message)
                }
                ControlplaneCredentialError::InternalFailure { message } => {
                    Status::internal(message)
                }
            })?;

        let (
            encrypted_ca_key,
            ca_key_nonce,
            encrypted_service_account_signing_key,
            service_account_signing_key_nonce,
        ) = if controlplane_token_authenticated && !join_token.is_empty() {
            let (encrypted_ca_key, ca_key_nonce) = encrypt_controlplane_key_material(
                self.credential_issuer.as_ref(),
                "CA key encryption",
                &join_token,
                ca_key_pem.as_bytes(),
            )
            .await?;
            let (encrypted_service_account_signing_key, service_account_signing_key_nonce) =
                encrypt_controlplane_key_material(
                    self.credential_issuer.as_ref(),
                    "ServiceAccount signing key encryption",
                    &join_token,
                    service_account_signing_key_pem.as_bytes(),
                )
                .await?;
            (
                encrypted_ca_key,
                ca_key_nonce,
                encrypted_service_account_signing_key,
                service_account_signing_key_nonce,
            )
        } else {
            (Vec::new(), Vec::new(), Vec::new(), Vec::new())
        };

        tracing::info!(
            node_name = %req.node_name,
            "SignControlplaneCsr: signed server cert for joining controlplane"
        );

        Ok(Response::new(
            klights_internal_protobuf::SignControlplaneCsrResponse {
                signed_server_cert,
                ca_cert_pem,
                encrypted_ca_key,
                ca_key_nonce,
                encrypted_service_account_signing_key,
                service_account_signing_key_nonce,
            },
        ))
    }
}

/// Helper: dispatch one of the three Raft RPCs against the optional
/// router, mapping `Disabled` and dispatch errors into a `String` the
/// proto envelope can carry. The client side translates the `error`
/// arm into `RPCError::Unreachable` (router not installed) or
/// `RPCError::RemoteError` (consensus-layer error).
async fn dispatch_raft_rpc<'a, F, Fut>(
    router: Option<&'a Arc<dyn crate::raft_rpc::RaftRpcRouter>>,
    call: F,
) -> std::result::Result<Vec<u8>, String>
where
    F: FnOnce(&'a Arc<dyn crate::raft_rpc::RaftRpcRouter>) -> Fut,
    Fut: std::future::Future<
            Output = std::result::Result<Vec<u8>, crate::raft_rpc::RaftRpcRouterError>,
        >,
{
    let Some(router) = router else {
        return Err(crate::raft_rpc::RaftRpcRouterError::Disabled.to_string());
    };
    call(router).await.map_err(|err| err.to_string())
}

pub fn resource_to_proto(resource: &Resource) -> klights_internal_protobuf::ResourceObject {
    let mut data = (*resource.data).clone();
    if let Some(root) = data.as_object_mut() {
        root.insert(
            "apiVersion".to_string(),
            serde_json::Value::String(resource.api_version.clone()),
        );
        root.insert(
            "kind".to_string(),
            serde_json::Value::String(resource.kind.clone()),
        );
        let metadata = root
            .entry("metadata".to_string())
            .or_insert_with(|| serde_json::json!({}));
        if let Some(metadata) = metadata.as_object_mut() {
            metadata.insert(
                "name".to_string(),
                serde_json::Value::String(resource.name.clone()),
            );
            match &resource.namespace {
                Some(namespace) => {
                    metadata.insert(
                        "namespace".to_string(),
                        serde_json::Value::String(namespace.clone()),
                    );
                }
                None => {
                    metadata.remove("namespace");
                }
            }
            metadata.insert(
                "uid".to_string(),
                serde_json::Value::String(resource.uid.clone()),
            );
            metadata.insert(
                "resourceVersion".to_string(),
                serde_json::Value::String(resource.resource_version.to_string()),
            );
        }
    }
    klights_internal_protobuf::ResourceObject {
        api_version: resource.api_version.clone(),
        kind: resource.kind.clone(),
        namespace: resource.namespace.clone(),
        name: resource.name.clone(),
        uid: resource.uid.clone(),
        resource_version: resource.resource_version,
        data_json: serde_json::to_vec(&data).unwrap_or_default(),
    }
}

fn resource_command_result_to_proto(
    result: klights_leader_api::ResourceCommandResult,
) -> klights_internal_protobuf::SubmitResourceCommandResponse {
    use klights_internal_protobuf::submit_resource_command_response::Result as WireResult;
    let result = match result {
        klights_leader_api::ResourceCommandResult::Resource(resource) => {
            WireResult::Resource(resource_to_proto(&resource))
        }
        klights_leader_api::ResourceCommandResult::Ack { resource_version } => {
            WireResult::Ack(klights_internal_protobuf::ResourceCommandAck { resource_version })
        }
    };
    klights_internal_protobuf::SubmitResourceCommandResponse {
        result: Some(result),
    }
}

fn resource_command_status(error: klights_leader_api::ResourceCommandError) -> Status {
    use klights_leader_api::ResourceCommandError;
    match error {
        ResourceCommandError::InvalidRequest { .. }
        | ResourceCommandError::UnsupportedCommand { .. } => {
            Status::invalid_argument(error.to_string())
        }
        ResourceCommandError::PodDeletionForbidden | ResourceCommandError::Unauthorized => {
            Status::permission_denied(error.to_string())
        }
        ResourceCommandError::NotLeader => Status::failed_precondition(error.to_string()),
        ResourceCommandError::AlreadyExists { .. } => Status::already_exists(error.to_string()),
        ResourceCommandError::Conflict { .. } => Status::aborted(error.to_string()),
        ResourceCommandError::NotFound { .. } => Status::not_found(error.to_string()),
        ResourceCommandError::Retryable { .. } => Status::unavailable(error.to_string()),
        ResourceCommandError::Timeout => Status::deadline_exceeded(error.to_string()),
        ResourceCommandError::Cancelled => Status::cancelled(error.to_string()),
        ResourceCommandError::SubmissionFailed { .. }
        | ResourceCommandError::CorruptResponse { .. } => Status::internal(error.to_string()),
        _ => Status::internal("unknown resource command error"),
    }
}

fn focused_node_subnet_to_proto(
    subnet: klights_leader_api::NodeSubnet,
) -> klights_internal_protobuf::NodeSubnetObject {
    klights_internal_protobuf::NodeSubnetObject {
        node_name: subnet.node_name().to_string(),
        subnet: subnet.subnet().to_string(),
        subnet_base_int: subnet.subnet_base_int(),
        gateway_ip: subnet.gateway_ip().to_string(),
        node_ip: subnet.node_ip().to_string(),
        mode: match subnet.mode() {
            klights_leader_api::NetworkNodeMode::Root => "root",
            klights_leader_api::NetworkNodeMode::Rootless => "rootless",
        }
        .to_string(),
        hostport_range: subnet.hostport_range().map(|range| range.to_string()),
    }
}

fn focused_dataplane_to_proto(
    metadata: klights_leader_api::NetworkDataplane,
) -> klights_internal_protobuf::DataplaneMetadataObject {
    klights_internal_protobuf::DataplaneMetadataObject {
        node_name: metadata.node_name().to_string(),
        mode: match metadata.mode() {
            klights_leader_api::NetworkNodeMode::Root => "root",
            klights_leader_api::NetworkNodeMode::Rootless => "rootless",
        }
        .to_string(),
        encryption: match metadata.encryption() {
            klights_leader_api::DataplaneEncryption::WireGuard => "enabled",
            klights_leader_api::DataplaneEncryption::Direct => "disabled",
        }
        .to_string(),
        public_key: metadata.public_key().map(str::to_owned),
        endpoint: metadata.endpoint().to_string(),
        port: metadata.port().map(u32::from),
    }
}

fn pod_cleanup_intent_to_proto(
    intent: klights_leader_api::PodCleanupIntent,
) -> std::result::Result<klights_internal_protobuf::PodCleanupIntentObject, Status> {
    let (node_name, namespace, pod_name, pod_uid, reason, resource_version, created_at_ms, pod) =
        intent.into_parts();
    let pod_data_json = serde_json::to_vec(pod.data.as_ref()).map_err(|error| {
        pod_cleanup_intent_error_to_status(
            klights_leader_api::PodCleanupIntentError::corrupt_intent(format!(
                "encode Pod cleanup intent snapshot for {namespace}/{pod_name} uid={pod_uid}: {error}"
            )),
        )
    })?;
    Ok(klights_internal_protobuf::PodCleanupIntentObject {
        node_name,
        namespace,
        pod_name,
        pod_uid,
        reason,
        resource_version,
        created_at_ms,
        pod_data_json,
    })
}

fn projected_token_error_to_status(
    error: klights_leader_api::ProjectedServiceAccountTokenError,
) -> Status {
    use klights_leader_api::ProjectedServiceAccountTokenError as Error;
    let message = error.to_string();
    match error {
        Error::InvalidRequest { .. } => Status::invalid_argument(message),
        Error::NotLeader => Status::failed_precondition("not raft leader"),
        Error::Unauthorized => Status::permission_denied(message),
        Error::BindingMismatch { .. } => Status::aborted(message),
        Error::ServiceAccountNotFound | Error::BoundPodNotFound | Error::BoundNodeNotFound => {
            Status::not_found(message)
        }
        Error::CorruptResource { .. } | Error::CorruptResponse { .. } => Status::data_loss(message),
        Error::SigningFailed { .. } => Status::failed_precondition(message),
        Error::Unavailable { .. } | Error::Transport { .. } => Status::unavailable(message),
        Error::Timeout => Status::deadline_exceeded(message),
        Error::Cancelled => Status::cancelled(message),
        _ => Status::internal(message),
    }
}

fn pod_cleanup_intent_error_to_status(error: klights_leader_api::PodCleanupIntentError) -> Status {
    use klights_leader_api::PodCleanupIntentError as Error;
    let message = error.to_string();
    match error {
        Error::InvalidRequest { .. } => Status::invalid_argument(message),
        Error::NotLeader => Status::failed_precondition("not raft leader"),
        Error::Unauthorized => Status::permission_denied(message),
        Error::CorruptIntent { .. } => Status::data_loss(message),
        Error::Unavailable { .. } | Error::Transport { .. } => Status::unavailable(message),
        Error::Timeout => Status::deadline_exceeded(message),
        Error::Cancelled => Status::cancelled(message),
        _ => Status::internal(message),
    }
}

/// Build a BOOKMARK heartbeat proto event carrying `last_rv` so the worker
/// treats it as both liveness and a resume point. Reuses the normal event
/// proto shape (the client decode requires a `resource`), and the worker's
/// informer cache skips BOOKMARK events rather than materializing them.
fn watch_heartbeat_proto(
    api_version: &str,
    kind: &str,
    last_rv: i64,
    resume_position: WatchReplayPosition,
) -> klights_internal_protobuf::WatchEvent {
    let resource = Resource::from_data_lossy(Arc::new(serde_json::json!({
        "apiVersion": api_version,
        "kind": kind,
        "metadata": {"resourceVersion": last_rv.to_string()}
    })));
    klights_internal_protobuf::WatchEvent {
        event_type: klights_leader_api::WatchEventType::Bookmark
            .as_str()
            .to_string(),
        resource: Some(resource_to_proto(&resource)),
        resume_position: Some(watch_replay_position_to_proto(resume_position)),
    }
}

fn leader_watch_error_to_status(error: klights_leader_api::LeaderWatchError) -> Status {
    match error {
        error @ klights_leader_api::LeaderWatchError::ReplayExpired {
            accepted_resource_version,
        } => crate::watch_replay_expired_status(accepted_resource_version, error.to_string()),
        klights_leader_api::LeaderWatchError::InvalidRequest { .. } => {
            Status::invalid_argument(error.to_string())
        }
        klights_leader_api::LeaderWatchError::Unavailable { .. } => {
            Status::unavailable(error.to_string())
        }
        _ => Status::internal(error.to_string()),
    }
}

pub async fn publish_joining_node_external_ip(
    query: &dyn klights_leader_api::LeaderResourceQuery,
    node_status: &dyn klights_leader_api::LeaderNodeLifecycleStatus,
    dataplane: &klights_leader_api::NetworkDataplane,
) -> Result<()> {
    let get = klights_leader_api::node_get_request(
        dataplane.node_name(),
        klights_leader_api::ResourceQueryConsistency::LeaderFresh,
    )?;
    let Some(resource) = query.get_resource(get).await? else {
        return Ok(());
    };
    let mut data = (*resource.data).clone();
    if !klights_cluster_core::set_node_external_ip(&mut data, &dataplane.endpoint().to_string()) {
        return Ok(());
    }
    let status = data
        .get("status")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let request =
        klights_leader_api::NodeLifecycleStatusRequest::try_new(StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Node".to_string(),
            namespace: None,
            name: dataplane.node_name().to_string(),
            status,
            expected_rv: Some(resource.resource_version),
            preconditions: ResourcePreconditions::uid_and_resource_version(
                resource.uid,
                resource.resource_version,
            ),
            observed_status_stamp: None,
        })?;
    node_status.submit_node_lifecycle_status(request).await?;
    Ok(())
}

pub async fn refresh_local_node_external_ip_from_observed_endpoint(
    query: &dyn klights_leader_api::LeaderResourceQuery,
    node_status: &dyn klights_leader_api::LeaderNodeSelfStatus,
    node_name: &str,
    endpoint: &str,
) -> Result<()> {
    let endpoint = endpoint.trim();
    if endpoint.is_empty() {
        return Ok(());
    }
    let endpoint_ip = endpoint
        .parse::<std::net::IpAddr>()
        .with_context(|| format!("observed leader endpoint must be an IP address: {endpoint}"))?;
    let get = klights_leader_api::node_get_request(
        node_name,
        klights_leader_api::ResourceQueryConsistency::LeaderFresh,
    )?;
    let Some(resource) = query.get_resource(get).await? else {
        return Ok(());
    };
    let mut data = (*resource.data).clone();
    if !klights_cluster_core::set_node_external_ip(&mut data, &endpoint_ip.to_string()) {
        return Ok(());
    }
    let status = data
        .get("status")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let request =
        klights_leader_api::NodeSelfStatusRequest::try_new(StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Node".to_string(),
            namespace: None,
            name: node_name.to_string(),
            status,
            expected_rv: None,
            preconditions: ResourcePreconditions::uid(resource.uid),
            observed_status_stamp: None,
        })?;
    node_status.submit_node_self_status(request).await?;
    Ok(())
}

async fn node_has_external_ip(
    query: &dyn klights_leader_api::LeaderResourceQuery,
    node_name: &str,
) -> Result<bool> {
    let request = klights_leader_api::node_get_request(
        node_name,
        klights_leader_api::ResourceQueryConsistency::LeaderFresh,
    )?;
    let Some(node) = query.get_resource(request).await? else {
        return Ok(false);
    };
    Ok(node
        .data
        .pointer("/status/addresses")
        .and_then(|value| value.as_array())
        .is_some_and(|addresses| {
            addresses.iter().any(|address| {
                address.get("type").and_then(|value| value.as_str()) == Some("ExternalIP")
                    && address
                        .get("address")
                        .and_then(|value| value.as_str())
                        .is_some_and(|value| !value.trim().is_empty())
            })
        }))
}

async fn join_response_to_proto(
    topology: &dyn klights_leader_api::LeaderNetworkTopologyQuery,
    response: JoinResponse,
) -> std::result::Result<klights_internal_protobuf::JoinResponse, Status> {
    match response {
        JoinResponse::Accepted {
            cluster_id,
            leader_epoch,
            current_rv,
        } => {
            let peers = dataplane_peers_from_topology(topology).await?;
            Ok(klights_internal_protobuf::JoinResponse {
                result: Some(klights_internal_protobuf::join_response::Result::Accepted(
                    klights_internal_protobuf::JoinAccepted {
                        cluster_id,
                        leader_epoch,
                        current_rv,
                        peers,
                    },
                )),
            })
        }
        JoinResponse::Rejected { reason } => Ok(klights_internal_protobuf::JoinResponse {
            result: Some(klights_internal_protobuf::join_response::Result::Rejected(
                klights_internal_protobuf::JoinRejected { reason },
            )),
        }),
    }
}

async fn dataplane_peers_from_topology(
    topology: &dyn klights_leader_api::LeaderNetworkTopologyQuery,
) -> std::result::Result<Vec<klights_internal_protobuf::DataplanePeer>, Status> {
    let query = klights_leader_api::PeerSubnetsQuery::try_new("snapshot-peer-list")
        .map_err(|err| Status::internal(err.to_string()))?;
    let mut subnets = topology
        .list_peer_subnets(query)
        .await
        .map_err(|err| Status::unavailable(err.to_string()))?
        .into_vec();
    subnets.sort_by(|a, b| a.node_name().cmp(b.node_name()));

    let mut peers = Vec::with_capacity(subnets.len());
    for subnet in subnets {
        let node_name = subnet.node_name().to_string();
        let query = klights_leader_api::NodeDataplaneQuery::try_new(node_name.clone())
            .map_err(|err| Status::internal(err.to_string()))?;
        let Some(dataplane) = topology
            .get_node_dataplane(query)
            .await
            .map_err(|err| Status::unavailable(err.to_string()))?
            .into_option()
        else {
            continue;
        };
        peers.push(klights_internal_protobuf::DataplanePeer {
            node_name,
            pod_cidr: subnet.subnet().to_string(),
            public_key: dataplane.public_key().unwrap_or_default().to_string(),
            endpoint: dataplane.endpoint().to_string(),
            port: dataplane.port().map(u32::from).unwrap_or_default(),
            mode: match dataplane.mode() {
                klights_leader_api::NetworkNodeMode::Root => "root",
                klights_leader_api::NetworkNodeMode::Rootless => "rootless",
            }
            .to_string(),
            encryption: match dataplane.encryption() {
                klights_leader_api::DataplaneEncryption::WireGuard => "enabled",
                klights_leader_api::DataplaneEncryption::Direct => "disabled",
            }
            .to_string(),
        });
    }
    Ok(peers)
}

// `forwarded_*_to_proto` helpers removed in T6 along with the legacy
// ForwardCommand wire path.

fn node_exec_sync_request_to_proto(
    request: RoutedNodeExecSyncRequest,
) -> klights_internal_protobuf::NodeExecSyncRequest {
    let request_id = request.request_id;
    let (target, command, timeout_seconds) = request.request.into_parts();
    let (node_name, namespace, pod_name, container_id) = target.into_parts();
    klights_internal_protobuf::NodeExecSyncRequest {
        request_id,
        node_name,
        namespace,
        pod_name,
        container_id,
        command,
        timeout_seconds,
    }
}

fn node_exec_sync_response_from_proto(
    response: klights_internal_protobuf::NodeExecSyncResponse,
) -> RoutedNodeExecSyncResponse {
    let result = match response.error {
        Some(error) => NodeExecSyncResult::failed(
            response.stdout,
            response.stderr,
            response.exit_code,
            ExecTerminalError::new(error),
        ),
        None => NodeExecSyncResult::success(response.stdout, response.stderr, response.exit_code),
    };
    RoutedNodeExecSyncResponse {
        request_id: response.request_id,
        result,
    }
}

fn node_exec_request_to_proto(
    request: RoutedNodeExecRequest,
) -> klights_internal_protobuf::NodeExecRequest {
    let request_id = request.request_id;
    let (target, command, options, attach) = request.request.into_parts();
    let (node_name, namespace, pod_name, container_id) = target.into_parts();
    klights_internal_protobuf::NodeExecRequest {
        request_id,
        node_name,
        namespace,
        pod_name,
        container_id,
        command,
        tty: options.tty(),
        stdin: options.stdin(),
        stdout: options.stdout(),
        stderr: options.stderr(),
        attach,
    }
}

fn node_exec_stream_frame_to_proto(
    frame: RoutedNodeExecFrame,
) -> klights_internal_protobuf::NodeExecStreamFrame {
    let (channel, data, fin) = frame.frame.into_parts();
    klights_internal_protobuf::NodeExecStreamFrame {
        request_id: frame.request_id,
        channel: channel.as_wire_name().to_string(),
        data,
        fin,
    }
}

fn node_exec_stream_frame_from_proto(
    frame: klights_internal_protobuf::NodeExecStreamFrame,
) -> Result<RoutedNodeExecFrame> {
    let channel = ExecStreamChannel::try_from_wire_name(&frame.channel)
        .ok_or_else(|| anyhow!("unknown node exec stream channel '{}'", frame.channel))?;
    Ok(RoutedNodeExecFrame {
        request_id: frame.request_id,
        frame: NodeExecFrame::new(channel, frame.data, frame.fin),
    })
}

fn pod_log_request_to_proto(
    routed: RoutedNodeLogRequest,
) -> klights_internal_protobuf::PodLogRequest {
    let (target, options) = routed.request.into_parts();
    let (node_name, namespace, pod_name, pod_uid, container_name) = target.into_parts();
    let (_, tail_lines, timestamps, since_time, since_seconds, limit_bytes, previous) =
        options.into_parts();
    klights_internal_protobuf::PodLogRequest {
        request_id: routed.request_id,
        node_name,
        namespace,
        pod_name,
        pod_uid,
        container_name,
        follow: routed.follow.then(|| "true".to_string()),
        tail_lines: tail_lines.map(|value| value.to_string()),
        timestamps,
        since_time,
        since_seconds,
        limit_bytes: limit_bytes.and_then(|value| i64::try_from(value).ok()),
        previous,
    }
}

fn pod_log_response_from_proto(
    response: klights_internal_protobuf::PodLogResponse,
) -> RoutedNodeLogEvent {
    let event = match response.error {
        Some(error) => NodeLogEvent::failed(response.log_content, NodeLogTerminalError::new(error)),
        None if response.fin => NodeLogEvent::complete(response.log_content),
        None => NodeLogEvent::data(response.log_content),
    };
    RoutedNodeLogEvent {
        request_id: response.request_id,
        event,
    }
}

fn node_metrics_request_to_proto(
    request: RoutedNodeMetricsRequest,
) -> klights_internal_protobuf::NodeMetricsRequest {
    let (target, pod_uids) = request.request.into_parts();
    klights_internal_protobuf::NodeMetricsRequest {
        request_id: request.request_id,
        node_name: target.into_node_name(),
        pod_uids,
    }
}

fn node_metrics_response_from_proto(
    response: klights_internal_protobuf::NodeMetricsResponse,
) -> RoutedNodeMetricsResponse {
    let request_id = response.request_id;
    let node_name = response.node_name;
    let result = match response.error {
        Some(error) => Err(NodeMetricsError::unavailable(error)),
        None => {
            let target = match klights_node_api::NodeMetricsTarget::try_new(node_name.clone()) {
                Ok(target) => target,
                Err(error) => {
                    return RoutedNodeMetricsResponse {
                        request_id,
                        node_name,
                        result: Err(error),
                    };
                }
            };
            let node = response
                .node
                .map(|node| NodeMetricsNodeSample::new(node.cpu_nanos, node.memory_bytes));
            let pods = response
                .pods
                .into_iter()
                .map(|pod| {
                    NodeMetricsPodSample::new(
                        pod.namespace,
                        pod.name,
                        pod.uid,
                        pod.containers
                            .into_iter()
                            .map(|container| {
                                NodeMetricsContainerSample::new(
                                    container.name,
                                    container.cpu_nanos,
                                    container.memory_bytes,
                                )
                            })
                            .collect(),
                    )
                })
                .collect();
            Ok(NodeMetricsResult::new(target, node, pods))
        }
    };
    RoutedNodeMetricsResponse {
        request_id,
        node_name,
        result,
    }
}
