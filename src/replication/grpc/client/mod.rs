use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context as TaskContext, Poll};
#[cfg(test)]
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use futures::StreamExt as _;
use hyper_util::rt::TokioIo;
use klights_cluster_core::Resource;
use klights_leader_api::{
    LeaderOutboxDelivery, LeaderWatchError, NetworkTopologyError, NodeDataplaneQuery,
    NodeDataplaneResult, NodeSubnetAllocationError, NodeSubnetAllocationRequest,
    NodeSubnetAllocationResult, NodeSubnetQuery, NodeSubnetResult, OutboxDeliveryError,
    OutboxDeliveryFuture, OutboxDeliveryRequest, OutboxDeliveryResult, PeerSubnetsQuery,
    PeerSubnetsResult, PodCleanupIntent, PodCleanupIntentAckRequest, PodCleanupIntentError,
    PodCleanupIntentListRequest, ProjectedServiceAccountToken, ProjectedServiceAccountTokenError,
    ProjectedServiceAccountTokenRequest, ResourceCommandError, ResourceCommandRequest,
    ResourceCommandResult, ResourceEvent, ResourceListRequest, ResourceListResult,
    ResourceQueryError, WatchRequest, WatchStream,
};
use klights_node_api::{
    BoundedByteStream, ByteStreamBounds, ByteStreamError, ByteStreamFuture, ExecStreamChannel,
    ExecStreamOptions, ExecTerminalError, NodeExecFrame, NodeExecRequest, NodeExecRuntime,
    NodeExecSyncRequest, NodeExecSyncResult, NodeExecTarget, NodeLogEvent, NodeLogRequest,
    NodeLogResult, NodeLogRuntime, NodeLogSetupError, NodeLogTarget, NodeLogTerminalError,
    NodeMetricsError, NodeMetricsRequest, NodeMetricsRuntime, NodeMetricsTarget,
};
use tokio::sync::{Mutex, mpsc};
use tokio_rustls::rustls::{
    DigitallySignedStruct, Error as TlsError, SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    crypto::{self, CryptoProvider},
    pki_types::{CertificateDer, ServerName, UnixTime},
};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity, Uri};
use tower::Service;

use crate::leader_tls_policy::{LeaderTlsVerificationPolicy, ResolvedLeaderTlsVerification};
use crate::replication::grpc::transport_policy::GrpcTransportPolicy;
use crate::replication::grpc::{
    JOIN_TOKEN_METADATA_KEY, entry_from_proto, resource_command_request_to_proto,
    watch_replay_position_from_proto, watch_replay_position_to_proto,
};
use klights_internal_protobuf::replication_client::ReplicationClient as TonicClient;
use klights_types::ResourceKey;
/// Response from SignControlplaneCsr RPC.
pub struct SignControlplaneCsrResponse {
    pub signed_server_cert: String,
    pub ca_cert_pem: String,
    pub encrypted_ca_key: Vec<u8>,
    pub ca_key_nonce: Vec<u8>,
    pub encrypted_service_account_signing_key: Vec<u8>,
    pub service_account_signing_key_nonce: Vec<u8>,
}

use crate::replication::protocol::{
    JoinResponse, JoinRole, RoutedNodeMetricsRequest, RoutedNodeMetricsResponse, StreamItem,
};
use klights_supervisor::{TaskCategory, TaskSupervisor};

const CONNECT_CHANNEL_CAPACITY: usize = 64;
const STREAM_ITEM_CHANNEL_CAPACITY: usize = 1024;
const NODE_EXEC_STREAM_FRAME_CHANNEL_CAPACITY: usize = 128;
// bug-grpc A1: message-size limits now live on `GrpcTransportPolicy`
// (`max_message_bytes`); the former `MAX_GRPC_MESSAGE_BYTES` constant is
// gone so client, CRI, and server cannot drift.
// `DEFAULT_FORWARD_RESPONSE_TIMEOUT` and `PendingForward` removed in T6.
type StreamItemQueue = Arc<Mutex<mpsc::Receiver<Result<StreamItem>>>>;
type NodeExecRuntimeSlot = Arc<Mutex<Option<Arc<dyn NodeExecRuntime>>>>;
#[derive(Clone)]
struct NodeExecInputRoute {
    sender: mpsc::Sender<NodeExecFrame>,
    cancellation: Arc<CancellationToken>,
}
type NodeExecInputRoutes = Arc<Mutex<std::collections::HashMap<String, NodeExecInputRoute>>>;
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum ActiveRuntimeKind {
    Exec,
    Log,
}
type RuntimeCancellationRoutes =
    Arc<Mutex<std::collections::HashMap<(ActiveRuntimeKind, String), Arc<CancellationToken>>>>;
type NodeLogRuntimeSlot = Arc<Mutex<Option<Arc<dyn NodeLogRuntime>>>>;
type NodeMetricsRuntimeSlot = Arc<Mutex<Option<Arc<dyn NodeMetricsRuntime>>>>;

#[derive(Debug)]
struct SkipCaServerCertVerifier {
    provider: Arc<CryptoProvider>,
}

impl SkipCaServerCertVerifier {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            provider: Arc::new(crypto::ring::default_provider()),
        })
    }
}

impl ServerCertVerifier for SkipCaServerCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, TlsError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[derive(Clone)]
struct ConnectDispatchContext {
    supervisor: Arc<TaskSupervisor>,
    node_exec_runtime: NodeExecRuntimeSlot,
    node_exec_inputs: NodeExecInputRoutes,
    node_stream_cancellations: RuntimeCancellationRoutes,
    node_log_runtime: NodeLogRuntimeSlot,
    node_metrics_runtime: NodeMetricsRuntimeSlot,
    observed_leader_endpoint: Option<String>,
}

fn node_exec_error_frame(message: String) -> NodeExecFrame {
    NodeExecFrame::new(
        ExecStreamChannel::Error,
        serde_json::json!({
            "metadata": {},
            "status": "Failure",
            "message": message,
        })
        .to_string()
        .into_bytes(),
        true,
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinDataplaneMetadata {
    pub public_key: Option<String>,
    pub endpoint: String,
    pub port: Option<u16>,
    pub mode: klights_leader_api::NetworkNodeMode,
    pub encryption: klights_leader_api::DataplaneEncryption,
}

fn dataplane_mode_wire(mode: klights_leader_api::NetworkNodeMode) -> &'static str {
    match mode {
        klights_leader_api::NetworkNodeMode::Root => "root",
        klights_leader_api::NetworkNodeMode::Rootless => "rootless",
    }
}

fn dataplane_encryption_wire(encryption: klights_leader_api::DataplaneEncryption) -> &'static str {
    match encryption {
        klights_leader_api::DataplaneEncryption::WireGuard => "enabled",
        klights_leader_api::DataplaneEncryption::Direct => "disabled",
    }
}

// bug-grpc A1: the default per-call unary deadline (15 s — sized above a
// worst-case slow WAN round-trip but well inside the 60 s outbox lease) now
// lives on `GrpcTransportPolicy::unary_deadline`.

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrpcClientConfig {
    pub leader_endpoint: String,
    pub token: String,
    pub node_name: String,
    /// Worker replication stream role. Control-plane learners join through
    /// JoinAsControlplane with as_learner=true.
    pub role: JoinRole,
    pub dataplane: JoinDataplaneMetadata,
    pub ca_cert_path: Option<PathBuf>,
    pub skip_ca: bool,
    /// Node client certificate PEM for mTLS auth (steady-state).
    /// When set, the gRPC client presents this certificate instead of
    /// attaching the bootstrap token metadata header.
    pub client_cert_pem: Option<String>,
    /// Node client private key PEM paired with `client_cert_pem`.
    pub client_key_pem: Option<String>,
    // `forward_response_timeout` removed in T6 — the legacy ForwardCommand
    // round-trip is gone. Field kept as `_legacy_forward_response_timeout`
    // would only confuse callers; struct shape simplified.
}

pub(crate) trait RegistrationSnapshotView {
    fn remote_snapshot(&self)
    -> crate::replication::grpc::raft_rpc::RemoteNodeRegistrationSnapshot;
}

impl RegistrationSnapshotView
    for crate::replication::grpc::raft_rpc::RemoteNodeRegistrationSnapshot
{
    fn remote_snapshot(
        &self,
    ) -> crate::replication::grpc::raft_rpc::RemoteNodeRegistrationSnapshot {
        self.clone()
    }
}

#[cfg(test)]
impl RegistrationSnapshotView for crate::kubelet::node::NodeRegistrationSnapshot {
    fn remote_snapshot(
        &self,
    ) -> crate::replication::grpc::raft_rpc::RemoteNodeRegistrationSnapshot {
        crate::replication::grpc::raft_rpc::RemoteNodeRegistrationSnapshot {
            node_mode: match self.node_mode {
                klights_network_api::NodePeerMode::Root => {
                    crate::replication::grpc::raft_rpc::RemoteNodeMode::Root
                }
                klights_network_api::NodePeerMode::Rootless => {
                    crate::replication::grpc::raft_rpc::RemoteNodeMode::Rootless
                }
            },
            host: crate::replication::grpc::raft_rpc::RemoteNodeHostFacts {
                cpu_count: self.host.cpu_count,
                memory_ki: self.host.memory_ki,
                architecture: self.host.architecture.clone(),
                operating_system: self.host.operating_system.clone(),
                os_image: self.host.os_image.clone(),
                kernel_version: self.host.kernel_version.clone(),
                container_runtime_version: self.host.container_runtime_version.clone(),
                kubelet_version: self.host.kubelet_version.clone(),
                git_commit: self.host.git_commit.clone(),
            },
        }
    }
}

pub(crate) fn node_registration_to_proto(
    registration: &impl RegistrationSnapshotView,
) -> klights_internal_protobuf::NodeRegistrationSnapshot {
    let registration = registration.remote_snapshot();
    let node_mode = match &registration.node_mode {
        crate::replication::grpc::raft_rpc::RemoteNodeMode::Root => "root",
        crate::replication::grpc::raft_rpc::RemoteNodeMode::Rootless => "rootless",
    };
    klights_internal_protobuf::NodeRegistrationSnapshot {
        cpu_count: registration.host.cpu_count,
        memory_ki: registration.host.memory_ki,
        architecture: registration.host.architecture.clone(),
        operating_system: registration.host.operating_system.clone(),
        os_image: registration.host.os_image.clone(),
        kernel_version: registration.host.kernel_version.clone(),
        container_runtime_version: registration.host.container_runtime_version.clone(),
        kubelet_version: registration.host.kubelet_version.clone(),
        git_commit: registration.host.git_commit.clone(),
        node_mode: node_mode.to_string(),
    }
}

impl GrpcClientConfig {
    async fn leader_tls_verification(
        &self,
        supervisor: &TaskSupervisor,
    ) -> Result<ResolvedLeaderTlsVerification> {
        LeaderTlsVerificationPolicy::new(self.ca_cert_path.clone(), self.skip_ca)
            .resolve(supervisor)
            .await
    }
}

#[derive(Clone)]
pub struct ReplicationGrpcClient {
    config: Arc<GrpcClientConfig>,
    supervisor: Arc<TaskSupervisor>,
    stream: Arc<Mutex<Option<OpenConnectStream>>>,
    join_response: Arc<Mutex<Option<JoinResponse>>>,
    node_exec_runtime: NodeExecRuntimeSlot,
    node_log_runtime: NodeLogRuntimeSlot,
    node_metrics_runtime: NodeMetricsRuntimeSlot,
    /// T2 step 5: list of all known leader endpoints (from --leader).
    /// When the stream fails, the reconnect loop cycles through these
    /// to find a reachable leader instead of retrying the same fixed
    /// endpoint forever.
    all_leader_endpoints: Arc<std::sync::Mutex<Vec<String>>>,
    /// Index into `all_leader_endpoints` of the last tried endpoint.
    endpoint_index: Arc<std::sync::Mutex<usize>>,
    /// T2 step 5: overrides `config.leader_endpoint` when set by
    /// `try_next_endpoint`. On stream failure the reconnect loop
    /// cycles the endpoint; the overridden value is used by
    /// `ensure_joined` for the next connect attempt.
    current_endpoint_override: Arc<std::sync::Mutex<Option<String>>>,
    /// Last remote IP reached by the gRPC transport. This lets a worker
    /// report the leader's observed external IP even when the configured
    /// leader endpoint was a hostname.
    observed_leader_endpoint: Arc<std::sync::Mutex<Option<String>>>,
    /// bug-grpc: purpose-segregated channel lanes. Each [`ChannelLane`]
    /// owns a small pool of independent HTTP/2 connections (one TCP
    /// socket each) to the active leader endpoint, reused round-robin.
    /// Segregating by purpose guarantees a stall on the long-lived
    /// Connect stream (or a backed-up status RPC) cannot head-of-line
    /// block a different class of RPC, and spreading concurrent calls
    /// across N connections eliminates single-connection TCP HOL.
    /// A tonic `Channel` multiplexes requests over its connection and
    /// lazily reconnects on transport loss, so a pooled channel is
    /// reused across calls (no per-call TLS handshake in steady state)
    /// and rebuilt only when the active endpoint changes (failover) or
    /// the lane is explicitly invalidated.
    channel_pools: Arc<Mutex<std::collections::HashMap<ChannelLane, LanePool>>>,
    /// bug-grpc: observability + test seam — number of real channel
    /// builds (TLS handshakes) performed via `channel_to_endpoint`.
    channel_build_count: Arc<std::sync::atomic::AtomicU64>,
    /// T7: per-peer raft transport counters (shared via Arc so test seams
    /// can read them without holding the struct mut).  Monotonic; never
    /// reset.  All three raft RPC paths route through `raft_unary_call`
    /// which increments these on every call.
    raft_timeout_count: Arc<std::sync::atomic::AtomicU64>,
    raft_append_entries_call_count: Arc<std::sync::atomic::AtomicU64>,
    raft_append_entries_byte_count: Arc<std::sync::atomic::AtomicU64>,
    /// bug-grpc A1: the single transport policy object — owns the per-call
    /// unary deadline (the bounded-call self-heal that fixes the partial-loss
    /// "10-minute stable cluster" stall), dial timeouts/keepalives, and the
    /// message-size limits. Constructed once at bootstrap and injected into
    /// every production client.
    policy: Arc<GrpcTransportPolicy>,
}

/// bug-grpc: a class of leader RPCs that must not share an HTTP/2
/// connection with other classes. Each lane keeps its own connection
/// pool so a stall in one class cannot block another.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum ChannelLane {
    /// The long-lived bidi `Connect` stream and `Snapshot` streaming.
    Stream,
    /// Hot, latency-critical worker→leader writes: `apply_outbox`,
    /// `renew_node_lease`.
    Status,
    /// Reads and cold control-plane RPCs: metadata, get/list/watch,
    /// subnet/dataplane reads, projected SA tokens, join/CSR.
    Read,
    /// Raft consensus RPCs (control-plane only): AppendEntries/Vote.
    Raft,
    /// T3: InstallSnapshot gets its own lane so a stalled multi-chunk
    /// snapshot transfer cannot head-of-line-block heartbeats/AppendEntries
    /// multiplexed over the same Raft connection under loss.
    InstallSnapshot,
}

impl ChannelLane {
    /// Number of independent connections this lane keeps to the active
    /// endpoint, sourced from the injected transport policy.
    fn pool_size(self, policy: &GrpcTransportPolicy) -> usize {
        match self {
            ChannelLane::Stream => policy.stream_lane_pool_size,
            ChannelLane::Status => policy.status_lane_pool_size,
            ChannelLane::Read => policy.read_lane_pool_size,
            ChannelLane::Raft => policy.raft_lane_pool_size,
            // T3: IS transfers are large but rare; a single dedicated
            // connection keeps them off the heartbeat/AppendEntries lane.
            ChannelLane::InstallSnapshot => policy.raft_lane_pool_size,
        }
        .max(1)
    }
}

/// bug-grpc: one pooled, reusable set of channels for a single
/// (lane, endpoint). Channels are handed out round-robin via `next`.
struct LanePool {
    endpoint: String,
    channels: Vec<Channel>,
    next: usize,
}

struct OpenConnectStream {
    sender: mpsc::Sender<klights_internal_protobuf::FollowerMessage>,
    stream_items: StreamItemQueue,
}

#[derive(Clone)]
struct ObservedPeerTcpConnector {
    observed_peer_ip: Arc<std::sync::Mutex<Option<String>>>,
}

impl ObservedPeerTcpConnector {
    fn new(observed_peer_ip: Arc<std::sync::Mutex<Option<String>>>) -> Self {
        Self { observed_peer_ip }
    }
}

impl Service<Uri> for ObservedPeerTcpConnector {
    type Response = TokioIo<tokio::net::TcpStream>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = io::Result<Self::Response>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let observed_peer_ip = self.observed_peer_ip.clone();
        let host = uri.host().map(str::to_string);
        let port = uri.port_u16().or_else(|| match uri.scheme_str() {
            Some("http") => Some(80),
            Some("https") => Some(443),
            _ => None,
        });

        Box::pin(async move {
            let host = host.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("leader endpoint has no host: {uri}"),
                )
            })?;
            let port = port.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("leader endpoint has no port: {uri}"),
                )
            })?;
            let stream = tokio::net::TcpStream::connect((host.as_str(), port)).await?;
            stream.set_nodelay(true)?;
            try_set_tcp_congestion_bbr(&stream);
            if let Ok(peer_addr) = stream.peer_addr()
                && let Ok(mut guard) = observed_peer_ip.lock()
            {
                *guard = Some(peer_addr.ip().to_string());
            }
            Ok(TokioIo::new(stream))
        })
    }
}

/// Best-effort: prefer BBR for inter-node gRPC TCP sockets on Linux.
///
/// BBR tolerates the lossy multinode harness (and real WANs) far better than
/// the default CUBIC, whose congestion window collapses under the ~0.5% loss
/// profile and widens the raft-commit window that the OCC refactor narrowed.
/// This must never fail the connection: if the kernel lacks BBR (older kernels,
/// a restricted netns, or a container without the module) `setsockopt` returns
/// `ENOENT`/`ENOPROTOOPT`, swallowed at debug level — startup must not depend on
/// host sysctl state. This mirrors `set_nodelay`-style socket tuning and
/// performs no blocking I/O.
#[cfg(target_os = "linux")]
fn try_set_tcp_congestion_bbr(stream: &tokio::net::TcpStream) {
    use std::os::unix::io::AsRawFd;
    let fd = stream.as_raw_fd();
    let algo = b"bbr";
    // SAFETY: `setsockopt` with `IPPROTO_TCP`/`TCP_CONGESTION` reads a short
    // NUL-free algorithm-name buffer (`b"bbr"`, 3 bytes) of the given length
    // and writes nothing back. `fd` is a live, owned `TcpStream` for the
    // duration of this call, and the static buffer outlives it. This is the
    // same non-blocking kernel path used by socket tuning such as
    // `set_nodelay`; it performs no I/O.
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_CONGESTION,
            algo.as_ptr() as *const libc::c_void,
            algo.len() as libc::socklen_t,
        )
    };
    if rc != 0 {
        tracing::debug!(
            error = %std::io::Error::last_os_error(),
            "could not set TCP_CONGESTION=bbr on inter-node socket; staying on kernel default"
        );
    }
}

/// Non-Linux fallback: BBR socket tuning is Linux-specific; no-op elsewhere.
#[cfg(not(target_os = "linux"))]
fn try_set_tcp_congestion_bbr(_stream: &tokio::net::TcpStream) {}

impl ReplicationGrpcClient {
    pub fn new(
        config: GrpcClientConfig,
        supervisor: Arc<TaskSupervisor>,
        policy: Arc<GrpcTransportPolicy>,
    ) -> Self {
        Self {
            config: Arc::new(config),
            supervisor,
            stream: Arc::new(Mutex::new(None)),
            join_response: Arc::new(Mutex::new(None)),
            node_exec_runtime: Arc::new(Mutex::new(None)),
            node_log_runtime: Arc::new(Mutex::new(None)),
            node_metrics_runtime: Arc::new(Mutex::new(None)),
            all_leader_endpoints: Arc::new(std::sync::Mutex::new(Vec::new())),
            endpoint_index: Arc::new(std::sync::Mutex::new(0)),
            current_endpoint_override: Arc::new(std::sync::Mutex::new(None)),
            observed_leader_endpoint: Arc::new(std::sync::Mutex::new(None)),
            channel_pools: Arc::new(Mutex::new(std::collections::HashMap::new())),
            channel_build_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            raft_timeout_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            raft_append_entries_call_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            raft_append_entries_byte_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            policy,
        }
    }

    /// The transport policy this client was built with.
    pub fn transport_policy(&self) -> &GrpcTransportPolicy {
        &self.policy
    }

    /// Test seam: shrink the unary RPC deadline so timeout behaviour can be
    /// exercised in milliseconds instead of the production 15 s.
    #[cfg(test)]
    pub(crate) fn override_unary_deadline(&mut self, deadline: Duration) {
        let mut policy = *self.policy;
        policy.unary_deadline = deadline;
        self.policy = Arc::new(policy);
    }

    /// Test seam: shrink the Raft unary RPC deadline so timeout behaviour
    /// can be exercised in milliseconds instead of the production value.
    /// bug-grpc T6: the three Raft consensus RPCs now have their own
    /// per-call deadline (`raft_unary_deadline`) so a wedged peer cannot
    /// stall consensus under partial packet loss.
    #[cfg(test)]
    pub(crate) fn override_raft_unary_deadline(&mut self, deadline: Duration) {
        let mut policy = *self.policy;
        policy.raft_unary_deadline = deadline;
        self.policy = Arc::new(policy);
    }

    /// bug-grpc: number of real channel builds (TLS handshakes) so far.
    /// Test seam asserting unary RPCs reuse a cached channel.
    #[cfg(test)]
    pub fn channel_build_count(&self) -> u64 {
        self.channel_build_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// T7: test seam — number of raft RPC deadline-exceeded timeouts recorded
    /// across all three raft RPC methods (AppendEntries, Vote, InstallSnapshot).
    #[cfg(test)]
    pub fn raft_timeout_count(&self) -> u64 {
        self.raft_timeout_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// T7: test seam — number of AppendEntries RPCs dispatched.
    #[cfg(test)]
    pub fn raft_append_entries_call_count(&self) -> u64 {
        self.raft_append_entries_call_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// T7: test seam — total bytes sent in AppendEntries payloads.
    #[cfg(test)]
    pub fn raft_append_entries_byte_count(&self) -> u64 {
        self.raft_append_entries_byte_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// bug-grpc: test seam — the endpoint a lane's pool is currently
    /// built for (None if the lane has never been used / was invalidated).
    #[cfg(test)]
    async fn lane_endpoint(&self, lane: ChannelLane) -> Option<String> {
        self.channel_pools
            .lock()
            .await
            .get(&lane)
            .map(|pool| pool.endpoint.clone())
    }

    /// bug-grpc: test seam — number of pooled connections currently held
    /// for a lane.
    #[cfg(test)]
    async fn lane_pool_len(&self, lane: ChannelLane) -> usize {
        self.channel_pools
            .lock()
            .await
            .get(&lane)
            .map(|pool| pool.channels.len())
            .unwrap_or(0)
    }

    pub fn node_name(&self) -> &str {
        &self.config.node_name
    }

    /// Returns the current leader endpoint, respecting any override
    /// set by `try_next_endpoint`.
    pub fn current_leader_endpoint(&self) -> String {
        if let Ok(guard) = self.current_endpoint_override.lock()
            && let Some(ep) = guard.as_ref()
        {
            return ep.clone();
        }
        self.config.leader_endpoint.clone()
    }

    pub fn set_current_leader_endpoint(&self, endpoint: Option<String>) {
        if let Some(endpoint) = endpoint.as_ref()
            && let Ok(endpoints) = self.all_leader_endpoints.lock()
            && let Some(index) = endpoints.iter().position(|candidate| candidate == endpoint)
            && let Ok(mut guard) = self.endpoint_index.lock()
        {
            *guard = index;
        }
        if let Ok(mut guard) = self.current_endpoint_override.lock() {
            *guard = endpoint;
        }
    }

    pub fn clear_current_leader_endpoint(&self) {
        self.set_current_leader_endpoint(None);
    }

    /// T2 step 5: register all known leader endpoints (from --leader).
    /// The reconnect loop calls [`try_next_endpoint`] after each stream
    /// failure to cycle through the list instead of retrying the same
    /// fixed endpoint.
    pub fn set_all_leader_endpoints(&self, endpoints: Vec<String>) {
        let current = self.current_leader_endpoint();
        if let Some(index) = endpoints.iter().position(|candidate| candidate == &current)
            && let Ok(mut guard) = self.endpoint_index.lock()
        {
            *guard = index;
        }
        if let Ok(mut guard) = self.all_leader_endpoints.lock() {
            *guard = endpoints;
        }
    }

    /// T2 step 5: cycle to the next leader endpoint in the registered
    /// list and set it as the active override. Returns the new endpoint.
    /// If the list is empty or has only one entry, returns the current
    /// config endpoint unchanged.
    pub fn try_next_endpoint(&self) -> String {
        let endpoints = match self.all_leader_endpoints.lock() {
            Ok(guard) => guard.clone(),
            Err(_) => return self.config.leader_endpoint.clone(),
        };
        if endpoints.len() <= 1 {
            return self.current_leader_endpoint();
        }
        let mut idx = self.endpoint_index.lock().unwrap();
        *idx = (*idx + 1) % endpoints.len();
        let next = endpoints[*idx].clone();
        if let Ok(mut guard) = self.current_endpoint_override.lock() {
            *guard = Some(next.clone());
        }
        tracing::info!(
            idx = *idx,
            endpoint = %next,
            "T2 step 5: cycling leader endpoint for reconnect"
        );
        next
    }

    fn leader_endpoint_candidates(&self) -> Vec<String> {
        let current = self.current_leader_endpoint();
        let mut candidates = vec![current.clone()];
        if let Ok(endpoints) = self.all_leader_endpoints.lock() {
            for endpoint in endpoints.iter() {
                if endpoint != &current && !candidates.contains(endpoint) {
                    candidates.push(endpoint.clone());
                }
            }
        }
        candidates
    }

    pub async fn set_node_exec_runtime(&self, runtime: Arc<dyn NodeExecRuntime>) {
        *self.node_exec_runtime.lock().await = Some(runtime);
    }

    pub async fn set_node_log_runtime(&self, handler: Arc<dyn NodeLogRuntime>) {
        *self.node_log_runtime.lock().await = Some(handler);
    }

    pub async fn set_node_metrics_runtime(&self, runtime: Arc<dyn NodeMetricsRuntime>) {
        *self.node_metrics_runtime.lock().await = Some(runtime);
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub fn worker(
        leader_endpoint: String,
        node_name: String,
        token: String,
        dataplane: JoinDataplaneMetadata,
        ca_cert_path: Option<PathBuf>,
        skip_ca: bool,
        supervisor: Arc<TaskSupervisor>,
        policy: Arc<GrpcTransportPolicy>,
    ) -> Self {
        Self::new(
            GrpcClientConfig {
                leader_endpoint,
                token,
                node_name,
                role: JoinRole::Worker,
                dataplane,
                ca_cert_path,
                skip_ca,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor,
            policy,
        )
    }

    pub async fn connect(
        config: GrpcClientConfig,
        supervisor: Arc<TaskSupervisor>,
        policy: Arc<GrpcTransportPolicy>,
    ) -> Result<Self> {
        let client = Self::new(config, supervisor, policy);
        client.ensure_joined().await?;
        Ok(client)
    }

    pub async fn ensure_joined(&self) -> Result<JoinResponse> {
        let mut guard = self.stream.lock().await;
        if guard.is_some() {
            if let Some(response) = self.join_response.lock().await.clone() {
                return Ok(response);
            }
            return Ok(JoinResponse::Accepted {
                cluster_id: String::new(),
                leader_epoch: 0,
                current_rv: 0,
            });
        }
        let (stream, response) = self.open_connect_stream().await?;
        *self.join_response.lock().await = Some(response.clone());
        *guard = Some(stream);
        Ok(response)
    }

    pub async fn metadata(&self) -> Result<crate::replication::protocol::MetadataResponse> {
        let response = self
            .unary_call(
                "grpc_get_metadata",
                ChannelLane::Read,
                |mut client| async move {
                    client
                        .get_metadata(klights_internal_protobuf::MetadataRequest {})
                        .await
                        .map(|r| r.into_inner())
                },
            )
            .await
            .map_err(|err| err.into_anyhow("gRPC GetMetadata failed"))?;
        Ok(crate::replication::protocol::MetadataResponse {
            cluster_id: response.cluster_id,
            leader_epoch: response.leader_epoch,
            current_rv: response.current_rv,
            current_log_index: response.current_log_index,
            command_codec_version: response.command_codec_version,
        })
    }

    pub async fn get_resource_rpc(
        &self,
        key: ResourceKey,
    ) -> std::result::Result<Option<Resource>, ResourceQueryError> {
        let expected_key = key.clone();
        let request = klights_internal_protobuf::GetResourceRequest {
            api_version: key.api_version,
            kind: key.kind,
            namespace: key.namespace,
            name: key.name,
        };
        let response = self
            .unary_call("grpc_get_resource", ChannelLane::Read, move |mut client| {
                let request = request.clone();
                async move { client.get_resource(request).await.map(|r| r.into_inner()) }
            })
            .await
            .map_err(resource_query_error_from_unary)?;
        let resource = resource_from_get_response(response)?;
        if resource.as_ref().is_some_and(|resource| {
            resource.api_version != expected_key.api_version
                || resource.kind != expected_key.kind
                || resource.namespace != expected_key.namespace
                || resource.name != expected_key.name
        }) {
            return Err(ResourceQueryError::corrupt_response(
                "GetResource response identity does not match the requested key",
            ));
        }
        Ok(resource)
    }

    pub async fn list_resources_rpc(
        &self,
        req: ResourceListRequest,
    ) -> std::result::Result<ResourceListResult, ResourceQueryError> {
        let expected_api_version = req.api_version().to_string();
        let expected_kind = req.kind().to_string();
        let expected_namespace = req.namespace().map(str::to_owned);
        let request = klights_internal_protobuf::ListResourcesRequest {
            api_version: expected_api_version.clone(),
            kind: expected_kind.clone(),
            namespace: expected_namespace.clone(),
            label_selector: req.label_selector().map(str::to_owned),
            field_selector: req.field_selector().map(str::to_owned),
            limit: req.limit(),
            continue_token: req.continue_token().map(str::to_owned),
        };
        let response = self
            .unary_call(
                "grpc_list_resources",
                ChannelLane::Read,
                move |mut client| {
                    let request = request.clone();
                    async move { client.list_resources(request).await.map(|r| r.into_inner()) }
                },
            )
            .await
            .map_err(resource_query_error_from_unary)?;
        validate_list_response_metadata(&response)?;
        let items = response
            .items
            .into_iter()
            .map(resource_from_proto)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if items.iter().any(|resource| {
            resource.api_version != expected_api_version
                || resource.kind != expected_kind
                || expected_namespace
                    .as_ref()
                    .is_some_and(|namespace| resource.namespace.as_ref() != Some(namespace))
        }) {
            return Err(ResourceQueryError::corrupt_response(
                "ListResources item identity is outside the requested scope",
            ));
        }
        ResourceListResult::try_new(
            items,
            response.resource_version,
            response
                .watch_replay_position
                .as_ref()
                .map(watch_replay_position_from_proto),
            response.continue_token,
            response.remaining_item_count,
        )
    }

    pub async fn submit_resource_command_rpc(
        &self,
        request: ResourceCommandRequest,
    ) -> std::result::Result<ResourceCommandResult, ResourceCommandError> {
        let request = resource_command_request_to_proto(&request)
            .map_err(|error| ResourceCommandError::submission_failed(error.to_string()))?;
        let response = self
            .unary_call(
                "grpc_submit_resource_command",
                ChannelLane::Status,
                move |mut client| {
                    let request = request.clone();
                    async move {
                        client
                            .submit_resource_command(request)
                            .await
                            .map(|response| response.into_inner())
                    }
                },
            )
            .await
            .map_err(resource_command_rpc_error)?;
        resource_command_result_from_proto(response)
    }

    pub async fn watch_resources_rpc(
        &self,
        req: WatchRequest,
    ) -> std::result::Result<WatchStream, LeaderWatchError> {
        let validation_request = req.clone();
        let request = klights_internal_protobuf::WatchResourcesRequest {
            api_version: req.api_version().to_string(),
            kind: req.kind().to_string(),
            namespace: req.namespace().map(str::to_owned),
            field_selector: req.field_selector().map(str::to_owned),
            start_resource_version: req.start_resource_version(),
            label_selector: req.label_selector().map(str::to_owned),
            start_watch_replay_position: req
                .start_watch_replay_position()
                .map(watch_replay_position_to_proto),
        };
        let response = self
            .streaming_open_call(
                "grpc_watch_resources_open",
                ChannelLane::Read,
                move |mut client| {
                    let request = request.clone();
                    async move { client.watch_resources(request).await }
                },
            )
            .await
            .map_err(watch_rpc_error)?;
        let stream = response.into_inner().map(move |event| {
            event
                .map_err(watch_status_error)
                .and_then(resource_event_from_proto)
                .and_then(|event| {
                    event.validate_for(&validation_request)?;
                    Ok(event)
                })
        });
        Ok(WatchStream::deferred_transport(Box::pin(stream)))
    }

    /// Opens a long-lived streaming RPC through the same bounded candidate
    /// recovery policy for every caller. Candidate endpoints are probes: the
    /// accepted leader hint changes only after the server returns response
    /// headers successfully. This prevents a failed or stale candidate from
    /// poisoning unrelated RPCs while still bounding a connection that keeps
    /// HTTP/2 alive but never opens the stream.
    async fn streaming_open_call<T, F, Fut>(
        &self,
        name: &'static str,
        lane: ChannelLane,
        make_call: F,
    ) -> Result<tonic::Response<T>>
    where
        F: Fn(TonicClient<Channel>) -> Fut,
        Fut: Future<Output = std::result::Result<tonic::Response<T>, tonic::Status>>,
    {
        let mut last_retryable: Option<anyhow::Error> = None;
        for endpoint in self.leader_endpoint_candidates() {
            let client = match self.tonic_client_lane_for_endpoint(lane, &endpoint).await {
                Ok(client) => client,
                Err(err) => {
                    last_retryable = Some(err);
                    continue;
                }
            };
            match self
                .supervisor
                .timeout(name, self.policy.stream_open_deadline, make_call(client))
                .await
            {
                Ok(Ok(Ok(response))) => {
                    self.set_current_leader_endpoint(Some(endpoint));
                    return Ok(response);
                }
                Ok(Ok(Err(status))) if is_not_raft_leader_status(&status) => {
                    last_retryable = Some(anyhow::Error::from(status));
                }
                Ok(Ok(Err(status))) if is_transport_status(&status) => {
                    if self.policy.evict_lane_on_transport_error {
                        self.heal_lane_on_transport(lane, &status).await;
                    }
                    last_retryable = Some(anyhow::Error::from(status));
                }
                Ok(Ok(Err(status))) => return Err(anyhow::Error::from(status)),
                Ok(Err(_elapsed)) => {
                    self.invalidate_lane(lane).await;
                    last_retryable = Some(anyhow!(
                        "{name} deadline exceeded after {:?}",
                        self.policy.stream_open_deadline
                    ));
                }
                Err(err) => return Err(err),
            }
        }
        Err(last_retryable.unwrap_or_else(|| anyhow!("no leader endpoint accepted {name}")))
    }

    pub async fn projected_service_account_token_rpc(
        &self,
        req: ProjectedServiceAccountTokenRequest,
    ) -> std::result::Result<ProjectedServiceAccountToken, ProjectedServiceAccountTokenError> {
        let (
            namespace,
            service_account_name,
            audiences,
            expiration_seconds,
            bound_pod_name,
            bound_pod_uid,
            bound_node_name,
            bound_node_uid,
        ) = req.into_parts();
        let request = klights_internal_protobuf::ProjectedServiceAccountTokenRequest {
            namespace,
            service_account_name,
            audiences,
            expiration_seconds,
            bound_pod_name: Some(bound_pod_name),
            bound_pod_uid: Some(bound_pod_uid),
            bound_node_name: Some(bound_node_name),
            bound_node_uid,
        };
        let response = self
            .unary_call(
                "grpc_projected_service_account_token",
                ChannelLane::Status,
                move |mut client| {
                    let request = request.clone();
                    async move {
                        client
                            .projected_service_account_token(request)
                            .await
                            .map(|r| r.into_inner())
                    }
                },
            )
            .await
            .map_err(projected_token_error_from_unary)?;
        ProjectedServiceAccountToken::try_new(response.token)
    }

    /// bug-grpc A2: the single retry/deadline/failover path for **every**
    /// non-Raft, non-streaming unary worker→leader RPC. Generalizes the loop
    /// that used to live (only) in `apply_outbox_rpc`:
    ///
    /// - **Failover** across [`leader_endpoint_candidates`] — current endpoint
    ///   first, then the rest.
    /// - **Per-call deadline** via `supervisor.timeout(name, policy.unary_deadline, …)`
    ///   so a keepalive-alive but response-wedged connection (the partial-loss
    ///   "stable cluster" stall) aborts instead of blocking forever.
    /// - **Retryable classification**: `not raft leader`
    ///   ([`is_not_raft_leader_status`]) and transport faults
    ///   ([`is_transport_status`]) are retried on the next candidate; the
    ///   transport case (and an elapsed deadline) **evicts only this lane**
    ///   ([`heal_lane_on_transport`] / [`invalidate_lane`]) so the rebuild is
    ///   fresh while sibling lanes keep their warm connections.
    /// - Any other gRPC status is returned as [`UnaryRpcError::Status`]
    ///   (application error, not transport-retryable).
    ///
    /// `make_call` is invoked once per candidate with a fresh lane client; it
    /// must build its own request from owned/cloned data (so it can be called
    /// again on the next candidate) and return the raw tonic call result.
    ///
    /// Raft RPCs and streaming RPCs (`connect`, `snapshot`, `watch_resources`)
    /// are deliberately excluded — they have different lanes/lifecycles.
    async fn unary_call<T, F, Fut>(
        &self,
        name: &'static str,
        lane: ChannelLane,
        make_call: F,
    ) -> std::result::Result<T, UnaryRpcError>
    where
        F: Fn(TonicClient<Channel>) -> Fut,
        Fut: Future<Output = std::result::Result<T, tonic::Status>>,
    {
        let mut last_retryable: Option<String> = None;
        for endpoint in self.leader_endpoint_candidates() {
            self.set_current_leader_endpoint(Some(endpoint.clone()));
            let client = match self.tonic_client_lane_for_endpoint(lane, &endpoint).await {
                Ok(client) => client,
                Err(err) => {
                    last_retryable = Some(err.to_string());
                    continue;
                }
            };
            match self
                .supervisor
                .timeout(name, self.policy.unary_deadline, make_call(client))
                .await
            {
                Ok(Ok(Ok(value))) => return Ok(value),
                Ok(Ok(Err(status))) if is_not_raft_leader_status(&status) => {
                    // Stale leader hint: try the next candidate without
                    // evicting (the connection itself is healthy).
                    last_retryable = Some(status.to_string());
                    continue;
                }
                Ok(Ok(Err(status))) if is_transport_status(&status) => {
                    if self.policy.evict_lane_on_transport_error {
                        self.heal_lane_on_transport(lane, &status).await;
                    }
                    last_retryable = Some(status.to_string());
                    continue;
                }
                Ok(Ok(Err(status))) => return Err(UnaryRpcError::Status(status)),
                Ok(Err(_elapsed)) => {
                    // Per-call deadline elapsed: the connection is wedged.
                    // Evict the lane so the next attempt / durable retry
                    // rebuilds a fresh connection.
                    self.invalidate_lane(lane).await;
                    last_retryable = Some(format!(
                        "{name} deadline exceeded after {:?}",
                        self.policy.unary_deadline
                    ));
                    continue;
                }
                Err(err) => {
                    // Supervisor declined the timer (root shutdown): retry.
                    last_retryable = Some(err.to_string());
                    continue;
                }
            }
        }
        Err(UnaryRpcError::Retryable(last_retryable.unwrap_or_else(
            || format!("no leader endpoint accepted {name}"),
        )))
    }

    /// bug-grpc T6: the raft analogue of [`unary_call`] for the three Raft
    /// consensus RPCs (AppendEntries/Vote/InstallSnapshot). Bounds the call
    /// with `policy.raft_unary_deadline` via the supervised-timeout helper
    /// so a keepalive-alive but response-wedged peer cannot stall consensus
    /// under partial packet loss, and heals the Raft lane on transport
    /// failure or an elapsed deadline so the next attempt rebuilds a fresh
    /// connection.
    ///
    /// Unlike the worker→leader [`unary_call`], raft RPCs address a fixed
    /// peer (not a leader-endpoint failover set) and have no `not raft
    /// leader` retry semantics, so this wrapper does not iterate
    /// [`leader_endpoint_candidates`]. The caller (`GrpcRaftNetwork` in
    /// `datastore::raft::grpc_network`) already owns the per-peer client
    /// lifecycle and openraft's own retry/backoff drives re-sends.
    async fn raft_unary_call<T, F, Fut>(
        &self,
        name: &'static str,
        lane: ChannelLane,
        make_call: F,
    ) -> Result<T>
    where
        F: FnOnce(TonicClient<Channel>) -> Fut,
        Fut: Future<Output = std::result::Result<T, tonic::Status>>,
    {
        let client = self.tonic_client_lane(lane).await?;
        match self
            .supervisor
            .timeout(name, self.policy.raft_unary_deadline, make_call(client))
            .await
        {
            Ok(Ok(Ok(value))) => Ok(value),
            Ok(Ok(Err(status))) if is_transport_status(&status) => {
                self.heal_lane_on_transport(lane, &status).await;
                Err(anyhow::anyhow!("{name} transport failure: {status}"))
            }
            Ok(Ok(Err(status))) => Err(anyhow::anyhow!("{name} failed: {status}")),
            Ok(Err(_elapsed)) => {
                // Per-call deadline elapsed: the connection is wedged. Evict
                // the lane so the next attempt rebuilds a fresh
                // connection against the peer.
                self.raft_timeout_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::warn!(
                    name,
                    deadline_ms = self.policy.raft_unary_deadline.as_millis(),
                    "raft RPC deadline exceeded; invalidating lane"
                );
                self.invalidate_lane(lane).await;
                Err(anyhow::anyhow!(
                    "{name} deadline exceeded after {:?}",
                    self.policy.raft_unary_deadline
                ))
            }
            Err(err) => Err(anyhow::anyhow!("{name} supervisor timeout failed: {err}")),
        }
    }

    pub(crate) async fn apply_outbox_rpc(
        &self,
        request: OutboxDeliveryRequest,
    ) -> std::result::Result<OutboxDeliveryResult, OutboxDeliveryError> {
        // bug-grpc A2: reimplemented on the generic `unary_call` executor.
        // Idempotency key + response decode stay here; the retry/deadline/
        // failover/lane-heal loop is shared.
        let (codec_version, idempotency_key, operation, payload, client_id, stream_id, stream_seq) =
            request.into_parts();
        let operation = operation.as_wire_name().to_string();
        let payload = payload.to_vec();
        let authoring_node = self.node_name().to_string();
        let response = match self
            .unary_call(
                "grpc_apply_outbox",
                ChannelLane::Status,
                move |mut client| {
                    let request = klights_internal_protobuf::ApplyOutboxRequest {
                        idempotency_key: idempotency_key.clone(),
                        operation: operation.clone(),
                        payload_proto: payload.clone(),
                        authoring_node: authoring_node.clone(),
                        client_id: client_id.clone(),
                        stream_id,
                        stream_seq,
                        codec_version,
                    };
                    async move { client.apply_outbox(request).await.map(|r| r.into_inner()) }
                },
            )
            .await
        {
            Ok(response) => response,
            Err(UnaryRpcError::Retryable(message)) => {
                return Err(OutboxDeliveryError::Retryable(message));
            }
            Err(UnaryRpcError::Status(status)) => return Err(outbox_error_from_status(status)),
        };
        decode_apply_outbox_response(response)
    }

    /// Fail startup before this client can submit commands to an older leader.
    pub async fn require_command_codec_v3(&self) -> anyhow::Result<()> {
        let metadata = self.metadata().await?;
        crate::replication::protocol::require_exact_command_codec(
            metadata.command_codec_version,
            "replication leader",
        )
        .map_err(anyhow::Error::msg)
    }

    /// P3-11c: opaque envelope dispatch for the three Raft consensus
    /// RPCs. The payload bytes are the serde-encoded openraft RPC; the
    /// response is either the serde-encoded openraft response (Ok arm)
    /// or a server-side error message (Error arm). Used by
    /// `ReplicationGrpcRaftRpcClient` to implement
    /// `datastore::raft::grpc_network::GrpcRaftRpcClient`.
    pub async fn raft_append_entries_rpc(
        &self,
        receiver: crate::replication::grpc::raft_rpc::RaftReceiverAdmission,
        payload: Vec<u8>,
    ) -> Result<std::result::Result<Vec<u8>, String>> {
        let receiver_admission = serde_json::to_vec(&receiver).map_err(|error| {
            UnaryRpcError::Retryable(format!("encode receiver admission: {error}"))
        })?;
        let byte_len = payload.len() as u64;
        self.raft_append_entries_call_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.raft_append_entries_byte_count
            .fetch_add(byte_len, std::sync::atomic::Ordering::Relaxed);
        let response = self
            .raft_unary_call(
                "grpc_raft_append_entries",
                ChannelLane::Raft,
                move |mut client| async move {
                    client
                        .raft_append_entries(tonic::Request::new(
                            klights_internal_protobuf::RaftAppendEntriesRequest {
                                payload,
                                command_codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
                                receiver_admission,
                            },
                        ))
                        .await
                        .map(|r| r.into_inner())
                },
            )
            .await?;
        Ok(match response.result {
            Some(klights_internal_protobuf::raft_append_entries_response::Result::Ok(bytes)) => {
                Ok(bytes)
            }
            Some(klights_internal_protobuf::raft_append_entries_response::Result::Error(msg)) => {
                Err(msg)
            }
            None => Err("server returned empty RaftAppendEntriesResponse result".to_string()),
        })
    }

    pub async fn raft_vote_rpc(
        &self,
        receiver: crate::replication::grpc::raft_rpc::RaftReceiverAdmission,
        payload: Vec<u8>,
    ) -> Result<std::result::Result<Vec<u8>, String>> {
        let receiver_admission = serde_json::to_vec(&receiver).map_err(|error| {
            UnaryRpcError::Retryable(format!("encode receiver admission: {error}"))
        })?;
        let response = self
            .raft_unary_call(
                "grpc_raft_vote",
                ChannelLane::Raft,
                move |mut client| async move {
                    client
                        .raft_vote(tonic::Request::new(
                            klights_internal_protobuf::RaftVoteRequest {
                                payload,
                                command_codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
                                receiver_admission,
                            },
                        ))
                        .await
                        .map(|r| r.into_inner())
                },
            )
            .await?;
        Ok(match response.result {
            Some(klights_internal_protobuf::raft_vote_response::Result::Ok(bytes)) => Ok(bytes),
            Some(klights_internal_protobuf::raft_vote_response::Result::Error(msg)) => Err(msg),
            None => Err("server returned empty RaftVoteResponse result".to_string()),
        })
    }

    pub async fn raft_install_snapshot_rpc(
        &self,
        receiver: crate::replication::grpc::raft_rpc::RaftReceiverAdmission,
        payload: Vec<u8>,
    ) -> Result<std::result::Result<Vec<u8>, String>> {
        let receiver_admission = serde_json::to_vec(&receiver).map_err(|error| {
            UnaryRpcError::Retryable(format!("encode receiver admission: {error}"))
        })?;
        let response = self
            .raft_unary_call(
                "grpc_raft_install_snapshot",
                ChannelLane::InstallSnapshot,
                move |mut client| async move {
                    client
                        .raft_install_snapshot(tonic::Request::new(
                            klights_internal_protobuf::RaftInstallSnapshotRequest {
                                payload,
                                command_codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
                                receiver_admission,
                            },
                        ))
                        .await
                        .map(|r| r.into_inner())
                },
            )
            .await?;
        Ok(match response.result {
            Some(klights_internal_protobuf::raft_install_snapshot_response::Result::Ok(bytes)) => {
                Ok(bytes)
            }
            Some(klights_internal_protobuf::raft_install_snapshot_response::Result::Error(msg)) => {
                Err(msg)
            }
            None => Err("server returned empty RaftInstallSnapshotResponse result".to_string()),
        })
    }

    /// P3-11c: send `JoinAsControlplane` to this client's leader
    /// endpoint, requesting that the remote leader add (node_id, addr)
    /// as a Raft voter via `RaftNode::add_voter`. Returns the typed
    /// outcome so the caller can drive the redirect-on-not-leader and
    /// retry-on-no-leader paths.
    ///
    /// T1.5.x: `as_learner=true` requests admission as a raft learner
    /// instead — the leader runs `RaftNode::add_learner_only` and the
    /// node serves as a replica without contributing to voter quorum.
    pub async fn join_as_controlplane_rpc(
        &self,
        node_id: u64,
        addr: &str,
        registration: &crate::replication::grpc::raft_rpc::ControlplaneJoinRegistration,
    ) -> Result<crate::replication::grpc::raft_rpc::ControlplaneJoinOutcome> {
        use crate::replication::grpc::raft_rpc::ControlplaneJoinOutcome;
        registration.snapshot.host.validate()?;
        anyhow::ensure!(
            matches!(
                (&registration.snapshot.node_mode, self.config.dataplane.mode),
                (
                    crate::replication::grpc::raft_rpc::RemoteNodeMode::Root,
                    klights_leader_api::NetworkNodeMode::Root
                ) | (
                    crate::replication::grpc::raft_rpc::RemoteNodeMode::Rootless,
                    klights_leader_api::NetworkNodeMode::Rootless
                )
            ),
            "Node registration mode must match JoinAsControlplane dataplane mode"
        );
        let request = klights_internal_protobuf::JoinAsControlplaneRequest {
            node_id,
            addr: addr.to_string(),
            node_name: registration.node_name.clone(),
            as_learner: registration.as_learner,
            dataplane_public_key: self.config.dataplane.public_key.clone().unwrap_or_default(),
            dataplane_endpoint: self.config.dataplane.endpoint.clone(),
            dataplane_port: self.config.dataplane.port.unwrap_or_default() as u32,
            dataplane_mode: dataplane_mode_wire(self.config.dataplane.mode).to_string(),
            dataplane_encryption: dataplane_encryption_wire(self.config.dataplane.encryption)
                .to_string(),
            node_internal_ip: registration.node_internal_ip.clone(),
            node_git_commit: registration.snapshot.host.git_commit.clone(),
            node_registration: Some(node_registration_to_proto(&registration.snapshot)),
            command_codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
            storage_incarnation: registration.storage_incarnation.clone(),
            storage_log_attestation: Some(klights_internal_protobuf::RaftStorageAttestation {
                high_watermark: registration
                    .storage_log_attestation
                    .high_watermark
                    .as_ref()
                    .map(|attestation| klights_internal_protobuf::RaftStorageLogId {
                        term: attestation.term,
                        leader_node_id: attestation.leader_node_id,
                        index: attestation.index,
                    }),
                current_boundary: registration
                    .storage_log_attestation
                    .current_boundary
                    .as_ref()
                    .map(|attestation| klights_internal_protobuf::RaftStorageLogId {
                        term: attestation.term,
                        leader_node_id: attestation.leader_node_id,
                        index: attestation.index,
                    }),
            }),
        };
        let join_token = self.controlplane_join_token_value()?;
        let response = self
            .unary_call(
                "grpc_join_as_controlplane",
                ChannelLane::Read,
                move |mut client| {
                    let request = request.clone();
                    let join_token = join_token.clone();
                    async move {
                        let mut request = tonic::Request::new(request);
                        if let Some(value) = join_token {
                            request
                                .metadata_mut()
                                .insert(JOIN_TOKEN_METADATA_KEY, value);
                        }
                        client
                            .join_as_controlplane(request)
                            .await
                            .map(|r| r.into_inner())
                    }
                },
            )
            .await
            .map_err(|err| err.into_anyhow("gRPC JoinAsControlplane failed"))?;
        let outcome = match response.result {
            Some(klights_internal_protobuf::join_as_controlplane_response::Result::Accepted(
                accepted,
            )) => {
                let ca_key_nonce: [u8; 12] = accepted.ca_key_nonce.try_into().unwrap_or([0u8; 12]);
                ControlplaneJoinOutcome::Accepted {
                    voter_count_after: accepted.voter_count_after,
                    admitted_as_learner: accepted.admitted_as_learner,
                    ca_cert_pem: accepted.ca_cert_pem,
                    encrypted_ca_key: accepted.encrypted_ca_key,
                    ca_key_nonce,
                }
            }
            Some(
                klights_internal_protobuf::join_as_controlplane_response::Result::RedirectToLeader(
                    r,
                ),
            ) => ControlplaneJoinOutcome::RedirectToLeader {
                leader_id: r.leader_id,
                leader_addr: r.leader_addr,
            },
            Some(klights_internal_protobuf::join_as_controlplane_response::Result::Denied(d)) => {
                ControlplaneJoinOutcome::Denied { reason: d.reason }
            }
            None => {
                return Err(anyhow!("JoinAsControlplane response missing result oneof"));
            }
        };
        Ok(outcome)
    }

    /// Send a CSR to the leader for signing. Returns the signed server cert
    /// and encrypted CA material. Called during cert init before the API
    /// server starts, so the joining node has a properly signed server cert.
    pub async fn sign_controlplane_csr_rpc(
        &self,
        node_name: &str,
        server_csr: &[u8],
    ) -> Result<SignControlplaneCsrResponse> {
        let request = klights_internal_protobuf::SignControlplaneCsrRequest {
            node_name: node_name.to_string(),
            server_csr: server_csr.to_vec(),
        };
        let csr_token = self.bootstrap_csr_token_value()?;
        let response = self
            .unary_call(
                "grpc_sign_controlplane_csr",
                ChannelLane::Read,
                move |mut client| {
                    let request = request.clone();
                    let csr_token = csr_token.clone();
                    async move {
                        let mut request = tonic::Request::new(request);
                        if let Some(value) = csr_token {
                            request
                                .metadata_mut()
                                .insert(JOIN_TOKEN_METADATA_KEY, value);
                        }
                        client
                            .sign_controlplane_csr(request)
                            .await
                            .map(|r| r.into_inner())
                    }
                },
            )
            .await
            .map_err(|err| err.into_anyhow("gRPC SignControlplaneCsr failed"))?;
        Ok(SignControlplaneCsrResponse {
            signed_server_cert: response.signed_server_cert,
            ca_cert_pem: response.ca_cert_pem,
            encrypted_ca_key: response.encrypted_ca_key,
            ca_key_nonce: response.ca_key_nonce,
            encrypted_service_account_signing_key: response.encrypted_service_account_signing_key,
            service_account_signing_key_nonce: response.service_account_signing_key_nonce,
        })
    }

    pub async fn renew_node_lease_rpc(
        &self,
        renew_time: &str,
        lease_duration_seconds: i64,
    ) -> Result<()> {
        self.renew_node_lease_focused_rpc(renew_time, lease_duration_seconds)
            .await
            .map_err(anyhow::Error::new)
            .context("gRPC RenewNodeLease failed")
    }

    pub(crate) async fn renew_node_lease_focused_rpc(
        &self,
        renew_time: &str,
        lease_duration_seconds: i64,
    ) -> std::result::Result<(), klights_leader_api::NodeLeaseRenewalError> {
        // bug-grpc A2: Status-lane unary RPC — the same lossy-link wedge as
        // apply_outbox, now bounded by the shared executor's per-call deadline
        // and lane self-heal.
        let node_name = self.node_name().to_string();
        let renew_time = renew_time.to_string();
        self.unary_call(
            "grpc_renew_node_lease",
            ChannelLane::Status,
            move |mut client| {
                let request = klights_internal_protobuf::RenewNodeLeaseRequest {
                    node_name: node_name.clone(),
                    renew_time: renew_time.clone(),
                    lease_duration_seconds,
                };
                async move {
                    client
                        .renew_node_lease(request)
                        .await
                        .map(|r| r.into_inner())
                }
            },
        )
        .await
        .map(|_| ())
        .map_err(node_lease_renewal_error_from_unary)
    }

    pub async fn allocate_node_subnet_rpc(
        &self,
        request: NodeSubnetAllocationRequest,
    ) -> std::result::Result<NodeSubnetAllocationResult, NodeSubnetAllocationError> {
        let expected_node_name = request.node_name().to_string();
        let (node_name, cluster_cidr, node_ip) = request.into_parts();
        let request = klights_internal_protobuf::AllocateNodeSubnetRequest {
            node_name,
            cluster_cidr,
            node_ip: node_ip.to_string(),
        };
        let response = self
            .unary_call(
                "grpc_allocate_node_subnet",
                ChannelLane::Status,
                move |mut client| {
                    let request = request.clone();
                    async move {
                        client
                            .allocate_node_subnet(request)
                            .await
                            .map(|r| r.into_inner())
                    }
                },
            )
            .await
            .map_err(node_subnet_allocation_error_from_unary)?;
        let subnet = response
            .subnet
            .map(node_subnet_from_proto)
            .transpose()
            .map_err(|error| NodeSubnetAllocationError::corrupt_response(error.to_string()))?;
        NodeSubnetAllocationResult::try_from_wire(&expected_node_name, subnet)
    }

    pub async fn get_node_subnet_rpc(
        &self,
        request: NodeSubnetQuery,
    ) -> std::result::Result<NodeSubnetResult, NetworkTopologyError> {
        let node_name = request.into_node_name();
        let request = klights_internal_protobuf::GetNodeSubnetRequest {
            node_name: node_name.clone(),
        };
        let response = self
            .unary_call(
                "grpc_get_node_subnet",
                ChannelLane::Read,
                move |mut client| {
                    let request = request.clone();
                    async move {
                        client
                            .get_node_subnet(request)
                            .await
                            .map(|r| r.into_inner())
                    }
                },
            )
            .await
            .map_err(network_topology_error_from_unary)?;
        let subnet = response.subnet.map(node_subnet_from_proto).transpose()?;
        NodeSubnetResult::try_from_wire(&node_name, response.found, subnet)
    }

    pub async fn list_peer_subnets_rpc(
        &self,
        request: PeerSubnetsQuery,
    ) -> std::result::Result<PeerSubnetsResult, NetworkTopologyError> {
        let my_node_name = request.into_node_name();
        let request = klights_internal_protobuf::ListPeerSubnetsRequest {
            my_node_name: my_node_name.clone(),
        };
        let response = self
            .unary_call(
                "grpc_list_peer_subnets",
                ChannelLane::Read,
                move |mut client| {
                    let request = request.clone();
                    async move {
                        client
                            .list_peer_subnets(request)
                            .await
                            .map(|r| r.into_inner())
                    }
                },
            )
            .await
            .map_err(network_topology_error_from_unary)?;
        let subnets = response
            .items
            .into_iter()
            .map(node_subnet_from_proto)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        PeerSubnetsResult::try_new(&my_node_name, subnets)
    }

    pub async fn get_node_dataplane_rpc(
        &self,
        request: NodeDataplaneQuery,
    ) -> std::result::Result<NodeDataplaneResult, NetworkTopologyError> {
        let node_name = request.into_node_name();
        let request = klights_internal_protobuf::GetNodeDataplaneRequest {
            node_name: node_name.clone(),
        };
        let response = self
            .unary_call(
                "grpc_get_node_dataplane",
                ChannelLane::Read,
                move |mut client| {
                    let request = request.clone();
                    async move {
                        client
                            .get_node_dataplane(request)
                            .await
                            .map(|r| r.into_inner())
                    }
                },
            )
            .await
            .map_err(network_topology_error_from_unary)?;
        let metadata = response
            .metadata
            .map(dataplane_metadata_from_proto)
            .transpose()?;
        NodeDataplaneResult::try_from_wire(&node_name, response.found, metadata)
    }

    pub async fn observe_peer_endpoint_rpc(&self, node_name: &str) -> Result<Option<String>> {
        let request = klights_internal_protobuf::ObservePeerEndpointRequest {
            node_name: node_name.to_string(),
        };
        let response = self
            .unary_call(
                "grpc_observe_peer_endpoint",
                ChannelLane::Read,
                move |mut client| {
                    let request = request.clone();
                    async move {
                        client
                            .observe_peer_endpoint(request)
                            .await
                            .map(|r| r.into_inner())
                    }
                },
            )
            .await
            .map_err(|err| err.into_anyhow("gRPC ObservePeerEndpoint failed"))?;
        Ok(response.found.then_some(response.endpoint))
    }

    pub async fn list_pod_cleanup_intents_for_node_rpc(
        &self,
        request: PodCleanupIntentListRequest,
    ) -> std::result::Result<Vec<PodCleanupIntent>, PodCleanupIntentError> {
        let request = klights_internal_protobuf::ListPodCleanupIntentsForNodeRequest {
            node_name: request.into_node_name(),
        };
        let response = self
            .unary_call(
                "grpc_list_pod_cleanup_intents_for_node",
                ChannelLane::Read,
                move |mut client| {
                    let request = request.clone();
                    async move {
                        client
                            .list_pod_cleanup_intents_for_node(request)
                            .await
                            .map(|r| r.into_inner())
                    }
                },
            )
            .await
            .map_err(pod_cleanup_intent_error_from_unary)?;
        response
            .items
            .into_iter()
            .map(pod_cleanup_intent_from_proto)
            .collect()
    }

    pub async fn delete_pod_cleanup_intent_rpc(
        &self,
        request: PodCleanupIntentAckRequest,
    ) -> std::result::Result<(), PodCleanupIntentError> {
        let (node_name, namespace, pod_name, pod_uid, reason) = request.into_parts();
        let request = klights_internal_protobuf::DeletePodCleanupIntentRequest {
            node_name,
            namespace,
            pod_name,
            pod_uid,
            reason,
        };
        self.unary_call(
            "grpc_delete_pod_cleanup_intent",
            ChannelLane::Status,
            move |mut client| {
                let request = request.clone();
                async move {
                    client
                        .delete_pod_cleanup_intent(request)
                        .await
                        .map(|r| r.into_inner())
                }
            },
        )
        .await
        .map(|_| ())
        .map_err(pod_cleanup_intent_error_from_unary)
    }

    pub async fn stream_next(&self) -> Result<StreamItem> {
        let (_, stream_items) = self.ensure_stream_parts().await?;
        let next = stream_items.lock().await.recv().await;
        match next {
            Some(Ok(item)) => Ok(item),
            Some(Err(err)) => {
                self.clear_stream().await;
                Err(err)
            }
            None => {
                self.clear_stream().await;
                Err(anyhow!("replication stream closed"))
            }
        }
    }

    pub async fn ack(&self, applied_rv: i64) -> Result<()> {
        let (sender, _) = self.ensure_stream_parts().await?;
        if sender
            .send(klights_internal_protobuf::FollowerMessage {
                payload: Some(klights_internal_protobuf::follower_message::Payload::Ack(
                    klights_internal_protobuf::StreamAck { applied_rv },
                )),
            })
            .await
            .is_err()
        {
            self.clear_stream().await;
            return Err(anyhow!("replication stream closed before ACK send"));
        }
        Ok(())
    }

    // `forward_command_with_meta` removed in T6. Workers now route writes
    // through outbox -> ApplyOutbox.

    async fn open_connect_stream(&self) -> Result<(OpenConnectStream, JoinResponse)> {
        // bug-grpc: the long-lived bidi stream gets its own dedicated
        // connection (Stream lane) so it never head-of-line blocks the
        // hot Status RPCs (`apply_outbox`/`renew_node_lease`).
        let mut client = self.tonic_client_lane(ChannelLane::Stream).await?;
        let (sender, mut rx) = mpsc::channel(CONNECT_CHANNEL_CAPACITY);
        sender
            .send(klights_internal_protobuf::FollowerMessage {
                payload: Some(klights_internal_protobuf::follower_message::Payload::Join(
                    self.join_request(),
                )),
            })
            .await
            .map_err(|_| anyhow!("failed to queue initial JoinRequest"))?;
        let outbound = async_stream::stream! {
            while let Some(message) = rx.recv().await {
                yield message;
            }
        };
        let mut inbound = client
            .connect(tonic::Request::new(outbound))
            .await
            .context("gRPC Connect failed")?
            .into_inner();
        let first = inbound
            .message()
            .await
            .context("read gRPC JoinResponse")?
            .ok_or_else(|| anyhow!("leader closed gRPC stream before JoinResponse"))?;
        let response = join_response_from_leader_message(first)?;
        if let JoinResponse::Rejected { reason } = &response {
            return Err(anyhow!("join rejected: {reason}"));
        }
        if let Some(endpoint) = self.observed_leader_endpoint_for_report() {
            sender
                .send(klights_internal_protobuf::FollowerMessage {
                    payload: Some(
                        klights_internal_protobuf::follower_message::Payload::ObservedLeaderEndpoint(
                            klights_internal_protobuf::ObservedLeaderEndpoint { endpoint },
                        ),
                    ),
                })
                .await
                .map_err(|_| anyhow!("failed to queue observed leader endpoint"))?;
        }
        let (stream_tx, stream_rx) = mpsc::channel(STREAM_ITEM_CHANNEL_CAPACITY);
        let dispatch_context = ConnectDispatchContext {
            supervisor: self.supervisor.clone(),
            node_exec_runtime: self.node_exec_runtime.clone(),
            node_exec_inputs: Arc::new(Mutex::new(std::collections::HashMap::new())),
            node_stream_cancellations: Arc::new(Mutex::new(std::collections::HashMap::new())),
            node_log_runtime: self.node_log_runtime.clone(),
            node_metrics_runtime: self.node_metrics_runtime.clone(),
            observed_leader_endpoint: self.observed_leader_endpoint_for_report(),
        };
        self.supervisor
            .spawn_async(
                TaskCategory::Network,
                "grpc_replication_client_reader",
                run_connect_reader(inbound, sender.clone(), stream_tx, dispatch_context),
            )
            .await?;
        Ok((
            OpenConnectStream {
                sender,
                stream_items: Arc::new(Mutex::new(stream_rx)),
            },
            response,
        ))
    }

    async fn ensure_stream_parts(
        &self,
    ) -> Result<(
        mpsc::Sender<klights_internal_protobuf::FollowerMessage>,
        StreamItemQueue,
    )> {
        let mut guard = self.stream.lock().await;
        if guard.is_none() {
            let (stream, response) = self.open_connect_stream().await?;
            *self.join_response.lock().await = Some(response);
            *guard = Some(stream);
        }
        let stream = guard.as_ref().expect("stream set above");
        Ok((stream.sender.clone(), stream.stream_items.clone()))
    }

    async fn clear_stream(&self) {
        *self.stream.lock().await = None;
        // bug-grpc: a dropped stream means only the stream's connection
        // is suspect. Invalidate ONLY the Stream lane so the next
        // `ensure_joined` rebuilds it; the hot Status/Read/Raft lanes
        // must survive a stream flap (invariant §3.2.4).
        self.invalidate_lane(ChannelLane::Stream).await;
    }

    pub async fn reset_stream(&self) {
        self.clear_stream().await;
        *self.join_response.lock().await = None;
    }

    // `forward_response_timeout` removed in T6 along with the legacy
    // ForwardCommand round-trip.

    /// bug-grpc: build a tonic client for `lane` against the active
    /// leader endpoint, iterating failover candidates on transport error.
    async fn tonic_client_lane(&self, lane: ChannelLane) -> Result<TonicClient<Channel>> {
        let channel = self.channel_via_lane(lane).await?;
        Ok(tonic_client_with_limits(
            channel,
            self.policy.max_message_bytes,
        ))
    }

    /// bug-grpc: build a tonic client for `lane` pinned to a specific
    /// endpoint, reusing that endpoint's pooled connections. Used by the
    /// hot-path RPCs that drive their own candidate loop.
    async fn tonic_client_lane_for_endpoint(
        &self,
        lane: ChannelLane,
        endpoint: &str,
    ) -> Result<TonicClient<Channel>> {
        let channel = self.channel_for(lane, endpoint).await?;
        Ok(tonic_client_with_limits(
            channel,
            self.policy.max_message_bytes,
        ))
    }

    /// bug-grpc: resolve a pooled channel for `lane`, iterating failover
    /// candidates (current endpoint first) on transport error. Reuses a
    /// warm pool when one exists for the active endpoint.
    async fn channel_via_lane(&self, lane: ChannelLane) -> Result<Channel> {
        let mut last_error: Option<anyhow::Error> = None;
        for candidate in self.leader_endpoint_candidates() {
            match self.channel_for(lane, &candidate).await {
                Ok(channel) => {
                    self.set_current_leader_endpoint(Some(candidate));
                    return Ok(channel);
                }
                Err(err) => {
                    tracing::warn!(
                        endpoint = %candidate,
                        lane = ?lane,
                        error = %err,
                        "replication gRPC endpoint connect failed; trying next endpoint"
                    );
                    last_error = Some(err);
                    self.try_next_endpoint();
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("no replication leader endpoints configured")))
    }

    /// bug-grpc: reuse-first pooled channel accessor. Returns a warm
    /// round-robined connection when the lane already has a pool for
    /// `endpoint`; otherwise lazily builds `pool_size` independent
    /// connections (outside the lock) and installs them.
    async fn channel_for(&self, lane: ChannelLane, endpoint: &str) -> Result<Channel> {
        // Hot path: warm pool for this endpoint — round-robin, no build.
        {
            let mut pools = self.channel_pools.lock().await;
            if let Some(pool) = pools.get_mut(&lane)
                && pool.endpoint == endpoint
                && !pool.channels.is_empty()
            {
                let channel = pool.channels[pool.next % pool.channels.len()].clone();
                pool.next = pool.next.wrapping_add(1);
                return Ok(channel);
            }
        }
        // Miss (cold lane or endpoint changed): build the pool OUTSIDE
        // the lock, then install. Each build is an independent TCP/TLS
        // connection so concurrent calls spread across them.
        let lane_pool_size = lane.pool_size(&self.policy);
        let mut built = Vec::with_capacity(lane_pool_size);
        let mut last_err: Option<anyhow::Error> = None;
        for _ in 0..lane_pool_size {
            match self.channel_to_endpoint(endpoint).await {
                Ok(channel) => built.push(channel),
                Err(err) => {
                    last_err = Some(err);
                    break;
                }
            }
        }
        if built.is_empty() {
            return Err(last_err.unwrap_or_else(|| anyhow!("no channel built for {endpoint}")));
        }
        let chosen = built[0].clone();
        let mut pools = self.channel_pools.lock().await;
        pools.insert(
            lane,
            LanePool {
                endpoint: endpoint.to_string(),
                channels: built,
                next: 1,
            },
        );
        Ok(chosen)
    }

    /// bug-grpc: drop a lane's pool. The next `channel_for`/`channel_via_lane`
    /// call rebuilds it against the current leader endpoint. Only the
    /// named lane is affected — other lanes keep their warm connections.
    async fn invalidate_lane(&self, lane: ChannelLane) {
        self.channel_pools.lock().await.remove(&lane);
    }

    /// Self-heal a wedged lane after a transport-level RPC failure.
    ///
    /// When the leader *process* restarts (or its connection wedges under
    /// loss), the lane's warm channel pool keeps handing out a dead
    /// `tonic::Channel` — `channel_for` reuses a non-empty pool verbatim
    /// with no health check. Without eviction the worker's watch (Read),
    /// node-lease, and outbox (Status) RPCs spin forever and the node
    /// never rejoins. Evicting the lane on a transport error makes the
    /// next attempt — already driven by the existing reconnect/heartbeat/
    /// dispatch loops — rebuild a fresh connection against the current (or
    /// failover) leader endpoint. This mirrors the raft-transport
    /// self-heal in `datastore::raft::grpc_network` and the Stream-lane
    /// self-heal in `clear_stream`. Application-level errors (`not raft
    /// leader`, `NotFound`, conflicts) must NOT evict.
    async fn heal_lane_on_transport(&self, lane: ChannelLane, status: &tonic::Status) {
        if is_transport_status(status) {
            tracing::warn!(
                lane = ?lane,
                code = ?status.code(),
                message = %status.message(),
                "evicting wedged replication lane after transport error; will rebuild on next RPC"
            );
            self.invalidate_lane(lane).await;
        }
    }

    /// Test accessor: whether a warm channel pool currently exists for a
    /// lane. Used to assert lane eviction (self-heal) in tests.
    #[cfg(test)]
    async fn lane_pool_present_for_test(&self, lane: ChannelLane) -> bool {
        self.channel_pools.lock().await.contains_key(&lane)
    }

    async fn channel_to_endpoint(&self, current: &str) -> Result<Channel> {
        // bug-grpc: count every real channel build (each is a TLS
        // handshake to the leader). Incremented at entry so both the
        // cached unary path and the endpoint-specific probe path
        // (`tonic_client_for_endpoint`) are accounted for.
        self.channel_build_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // T2 step 5: use the current (possibly overridden) leader
        // endpoint so the reconnect loop's endpoint cycling takes
        // effect on the next connection attempt.
        let endpoint = normalized_endpoint(current)?;
        // bug-grpc A1: all dial tunables come from the injected policy.
        let mut builder = self.policy.configure_endpoint(
            Endpoint::from_shared(endpoint.clone())?,
            crate::replication::grpc::transport_policy::ChannelKind::InterNode,
        );
        if endpoint.starts_with("https://") {
            let host = endpoint_host(&endpoint)?;
            // TLS 1.3 only: the leader server rejects TLS 1.2 (see
            // bootstrap::init::tls::serve_https). tonic's ClientTlsConfig
            // does not expose protocol version control, but the server-side
            // restriction ensures only TLS 1.3 is negotiated.
            let mut tls = ClientTlsConfig::new().domain_name(host).assume_http2(true);

            // Attach client certificate identity when available (mTLS).
            if let (Some(cert), Some(key)) =
                (&self.config.client_cert_pem, &self.config.client_key_pem)
            {
                let identity = Identity::from_pem(cert.as_bytes(), key.as_bytes());
                tls = tls.identity(identity);
            }

            match self
                .config
                .leader_tls_verification(self.supervisor.as_ref())
                .await
                .context("read gRPC CA certificate")?
            {
                ResolvedLeaderTlsVerification::SkipCa => {
                    tracing::warn!(
                        leader_endpoint = %endpoint,
                        "skipping TLS CA verification for leader bootstrap connection"
                    );
                    builder =
                        builder.tls_config_with_verifier(tls, SkipCaServerCertVerifier::new())?;
                }
                ResolvedLeaderTlsVerification::CaPem(ca_pem) => {
                    tls = tls.ca_certificate(Certificate::from_pem(ca_pem));
                    builder = builder.tls_config(tls)?;
                }
                ResolvedLeaderTlsVerification::SystemRoots => {
                    tls = tls.with_enabled_roots();
                    builder = builder.tls_config(tls)?;
                }
            }
        }
        builder
            .connect_with_connector(ObservedPeerTcpConnector::new(
                self.observed_leader_endpoint.clone(),
            ))
            .await
            .with_context(|| format!("connect replication leader at {endpoint}"))
    }

    fn join_request(&self) -> klights_internal_protobuf::JoinRequest {
        klights_internal_protobuf::JoinRequest {
            token: String::new(),
            node_name: self.config.node_name.clone(),
            role: match self.config.role {
                JoinRole::Worker => klights_internal_protobuf::JoinRole::Worker as i32,
            },
            dataplane_public_key: self.config.dataplane.public_key.clone().unwrap_or_default(),
            dataplane_endpoint: self.config.dataplane.endpoint.clone(),
            dataplane_port: self.config.dataplane.port.map(u32::from).unwrap_or(0),
            dataplane_mode: dataplane_mode_wire(self.config.dataplane.mode).to_string(),
            dataplane_encryption: dataplane_encryption_wire(self.config.dataplane.encryption)
                .to_string(),
            command_codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
        }
    }

    fn observed_leader_endpoint_for_report(&self) -> Option<String> {
        if let Ok(guard) = self.observed_leader_endpoint.lock()
            && let Some(endpoint) = guard.as_deref()
        {
            return Some(endpoint.to_string());
        }
        None
    }

    fn uses_client_cert_auth(&self) -> bool {
        self.config.client_cert_pem.is_some() && self.config.client_key_pem.is_some()
    }

    #[cfg(test)]
    pub(crate) fn uses_client_cert_auth_for_test(&self) -> bool {
        self.uses_client_cert_auth()
    }

    #[cfg(test)]
    pub(crate) fn dataplane_for_test(&self) -> JoinDataplaneMetadata {
        self.config.dataplane.clone()
    }

    #[cfg(test)]
    fn add_join_token<T>(&self, request: &mut tonic::Request<T>) -> Result<()> {
        let _ = request;
        Ok(())
    }

    /// Attach the controlplane bootstrap token to a `JoinAsControlplane` request.
    ///
    /// Unlike steady-state RPCs (which authenticate purely by node-cert mTLS),
    /// raft voter/learner admission requires a valid controlplane token on the
    /// *first* join — the leader gates `JoinAsControlplane` on it. On restart the
    /// token is gone (`config.token` empty) and the leader instead recognizes the
    /// node by its existing raft membership, so omitting it here is correct.
    /// bug-grpc A2: precompute the controlplane join token metadata value so
    /// the `unary_call` closure (which cannot borrow `self`) can attach it on
    /// each candidate attempt. `None` when no token is configured (rejoin by
    /// node-cert mTLS).
    fn controlplane_join_token_value(&self) -> Result<Option<tonic::metadata::AsciiMetadataValue>> {
        if self.config.token.is_empty() {
            return Ok(None);
        }
        let value = self
            .config
            .token
            .parse()
            .context("controlplane bootstrap token is not valid gRPC metadata")?;
        Ok(Some(value))
    }

    /// bug-grpc A2: precompute the bootstrap token metadata value for the CSR
    /// RPC. `None` when no token is configured or when node-cert mTLS already
    /// authenticates the caller.
    fn bootstrap_csr_token_value(&self) -> Result<Option<tonic::metadata::AsciiMetadataValue>> {
        if self.config.token.is_empty() || self.uses_client_cert_auth() {
            return Ok(None);
        }
        let value = self
            .config
            .token
            .parse()
            .context("bootstrap token is not valid gRPC metadata")?;
        Ok(Some(value))
    }
}

async fn run_connect_reader(
    mut inbound: tonic::codec::Streaming<klights_internal_protobuf::LeaderMessage>,
    outbound: mpsc::Sender<klights_internal_protobuf::FollowerMessage>,
    stream_tx: mpsc::Sender<Result<StreamItem>>,
    context: ConnectDispatchContext,
) {
    let terminal_error = loop {
        match inbound.message().await {
            Ok(Some(message)) => {
                if let Err(err) =
                    dispatch_leader_message(message, &outbound, &stream_tx, &context).await
                {
                    break err;
                }
            }
            Ok(None) => break anyhow!("replication stream closed"),
            Err(status) => break anyhow!("replication stream error: {status}"),
        }
    };

    cancel_all_node_streams(&context).await;
    let _ = stream_tx.send(Err(terminal_error)).await;
}

async fn cancel_all_node_streams(context: &ConnectDispatchContext) {
    let cancellations = {
        let mut routes = context.node_stream_cancellations.lock().await;
        routes.drain().map(|(_, cancel)| cancel).collect::<Vec<_>>()
    };
    for cancel in cancellations {
        cancel.cancel();
    }
    context.node_exec_inputs.lock().await.clear();
}

async fn remove_node_stream_cancellation_if_current(
    routes: &RuntimeCancellationRoutes,
    kind: ActiveRuntimeKind,
    request_id: &str,
    expected: &Arc<CancellationToken>,
) {
    let mut routes = routes.lock().await;
    let key = (kind, request_id.to_string());
    if routes
        .get(&key)
        .is_some_and(|current| Arc::ptr_eq(current, expected))
    {
        routes.remove(&key);
    }
}

async fn remove_node_exec_routes_if_current(
    inputs: &NodeExecInputRoutes,
    cancellations: &RuntimeCancellationRoutes,
    request_id: &str,
    expected: &Arc<CancellationToken>,
) {
    {
        let mut inputs = inputs.lock().await;
        if inputs
            .get(request_id)
            .is_some_and(|route| Arc::ptr_eq(&route.cancellation, expected))
        {
            inputs.remove(request_id);
        }
    }
    remove_node_stream_cancellation_if_current(
        cancellations,
        ActiveRuntimeKind::Exec,
        request_id,
        expected,
    )
    .await;
}

async fn dispatch_leader_message(
    message: klights_internal_protobuf::LeaderMessage,
    outbound: &mpsc::Sender<klights_internal_protobuf::FollowerMessage>,
    stream_tx: &mpsc::Sender<Result<StreamItem>>,
    context: &ConnectDispatchContext,
) -> Result<()> {
    match message.payload {
        Some(klights_internal_protobuf::leader_message::Payload::StreamItem(item)) => {
            let item = stream_item_from_proto(item)?;
            stream_tx
                .send(Ok(item))
                .await
                .map_err(|_| anyhow!("stream item receiver closed"))?;
        }
        // T6: legacy ForwardResponse payload removed.
        Some(klights_internal_protobuf::leader_message::Payload::NodeExecSyncRequest(request)) => {
            let response = handle_node_exec_sync_request(request, &context.node_exec_runtime).await;
            outbound
                .send(klights_internal_protobuf::FollowerMessage {
                    payload: Some(
                        klights_internal_protobuf::follower_message::Payload::NodeExecSyncResponse(
                            response,
                        ),
                    ),
                })
                .await
                .map_err(|_| anyhow!("replication stream closed before node exec response send"))?;
        }
        Some(klights_internal_protobuf::leader_message::Payload::NodeExecRequest(request)) => {
            handle_node_exec_stream_request(
                request,
                outbound,
                context.supervisor.clone(),
                &context.node_exec_runtime,
                &context.node_exec_inputs,
                &context.node_stream_cancellations,
            )
            .await?;
        }
        Some(klights_internal_protobuf::leader_message::Payload::NodeExecStreamFrame(frame)) => {
            let (request_id, frame) = node_exec_stream_frame_from_proto(frame)?;
            tracing::debug!(
                request_id = %request_id,
                channel = frame.channel().as_wire_name(),
                len = frame.data().len(),
                fin = frame.fin(),
                "received node exec stream input frame from leader"
            );
            let route = {
                let routes = context.node_exec_inputs.lock().await;
                routes.get(&request_id).cloned()
            };
            let Some(route) = route else {
                tracing::warn!(
                    request_id = %request_id,
                    "dropped node exec stream input frame for inactive stream"
                );
                return Ok(());
            };
            route
                .sender
                .send(frame)
                .await
                .map_err(|_| anyhow!("node exec stream input receiver closed"))?;
        }
        Some(klights_internal_protobuf::leader_message::Payload::PodLogRequest(request)) => {
            if request.follow.as_deref() == Some("true") {
                handle_pod_log_follow_request(
                    request,
                    &context.node_log_runtime,
                    outbound.clone(),
                    context.supervisor.clone(),
                    context.node_stream_cancellations.clone(),
                )
                .await?;
            } else {
                let response = handle_pod_log_request(request, &context.node_log_runtime).await;
                outbound
                    .send(klights_internal_protobuf::FollowerMessage {
                        payload: Some(
                            klights_internal_protobuf::follower_message::Payload::PodLogResponse(
                                response,
                            ),
                        ),
                    })
                    .await
                    .map_err(|_| {
                        anyhow!("replication stream closed before pod log response send")
                    })?;
            }
        }
        Some(klights_internal_protobuf::leader_message::Payload::NodeMetricsRequest(request)) => {
            let response =
                handle_node_metrics_request(request, &context.node_metrics_runtime).await;
            outbound
                .send(klights_internal_protobuf::FollowerMessage {
                    payload: Some(
                        klights_internal_protobuf::follower_message::Payload::NodeMetricsResponse(
                            response,
                        ),
                    ),
                })
                .await
                .map_err(|_| {
                    anyhow!("replication stream closed before node metrics response send")
                })?;
        }
        Some(klights_internal_protobuf::leader_message::Payload::ObserveLeaderEndpointRequest(
            _,
        )) => {
            if let Some(endpoint) = context.observed_leader_endpoint.as_deref() {
                outbound
                    .send(klights_internal_protobuf::FollowerMessage {
                        payload: Some(
                            klights_internal_protobuf::follower_message::Payload::ObservedLeaderEndpoint(
                                klights_internal_protobuf::ObservedLeaderEndpoint {
                                    endpoint: endpoint.to_string(),
                                },
                            ),
                        ),
                    })
                    .await
                    .map_err(|_| {
                        anyhow!(
                            "replication stream closed before observed leader endpoint response send"
                        )
                    })?;
            }
        }
        Some(klights_internal_protobuf::leader_message::Payload::JoinResponse(response)) => {
            if let Some(klights_internal_protobuf::join_response::Result::Rejected(rejected)) =
                response.result
            {
                return Err(anyhow!("join rejected: {}", rejected.reason));
            }
        }
        None => return Err(anyhow!("empty LeaderMessage")),
    }
    Ok(())
}

async fn handle_node_exec_sync_request(
    request: klights_internal_protobuf::NodeExecSyncRequest,
    handler: &NodeExecRuntimeSlot,
) -> klights_internal_protobuf::NodeExecSyncResponse {
    let request_id = request.request_id.clone();
    let request = match node_exec_sync_request_from_proto(request) {
        Ok(request) => request,
        Err(error) => {
            return klights_internal_protobuf::NodeExecSyncResponse {
                request_id,
                stdout: Vec::new(),
                stderr: Vec::new(),
                exit_code: 126,
                error: Some(error.to_string()),
            };
        }
    };
    let Some(handler) = handler.lock().await.clone() else {
        return klights_internal_protobuf::NodeExecSyncResponse {
            request_id,
            stdout: Vec::new(),
            stderr: Vec::new(),
            exit_code: 126,
            error: Some("node exec handler is not available".to_string()),
        };
    };
    node_exec_sync_response_to_proto(request_id, handler.exec_sync(request).await)
}

struct GrpcNodeExecSession {
    input: Mutex<mpsc::Receiver<NodeExecFrame>>,
    output: mpsc::Sender<NodeExecFrame>,
    cancelled: AtomicBool,
}

impl BoundedByteStream for GrpcNodeExecSession {
    type Frame = NodeExecFrame;

    fn bounds(&self) -> ByteStreamBounds {
        ByteStreamBounds::try_new(
            NODE_EXEC_STREAM_FRAME_CHANNEL_CAPACITY,
            NODE_EXEC_STREAM_FRAME_CHANNEL_CAPACITY,
        )
        .expect("exec stream capacity is a non-zero constant")
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    fn send_frame(&self, frame: NodeExecFrame) -> ByteStreamFuture<'_, ()> {
        Box::pin(async move {
            if self.is_cancelled() {
                return Err(ByteStreamError::cancelled());
            }
            self.output
                .send(frame)
                .await
                .map_err(|_| ByteStreamError::closed("node exec stream output receiver closed"))
        })
    }

    fn recv_frame(&self) -> ByteStreamFuture<'_, Option<NodeExecFrame>> {
        Box::pin(async move {
            if self.is_cancelled() {
                return Err(ByteStreamError::cancelled());
            }
            Ok(self.input.lock().await.recv().await)
        })
    }

    fn cancel(&mut self) -> ByteStreamFuture<'_, ()> {
        Box::pin(async move {
            if !self.cancelled.swap(true, Ordering::AcqRel) {
                self.input.get_mut().close();
            }
            Ok(())
        })
    }
}

async fn handle_node_exec_stream_request(
    request: klights_internal_protobuf::NodeExecRequest,
    outbound: &mpsc::Sender<klights_internal_protobuf::FollowerMessage>,
    supervisor: Arc<TaskSupervisor>,
    handler: &NodeExecRuntimeSlot,
    node_exec_inputs: &NodeExecInputRoutes,
    node_stream_cancellations: &RuntimeCancellationRoutes,
) -> Result<()> {
    let request_id = request.request_id.clone();
    let request = node_exec_request_from_proto(request)?;
    let Some(handler) = handler.lock().await.clone() else {
        send_node_exec_frame_to_leader(
            outbound,
            &request_id,
            node_exec_error_frame("node exec stream handler is not available".to_string()),
        )
        .await?;
        return Ok(());
    };

    let (input_tx, input_rx) = mpsc::channel(NODE_EXEC_STREAM_FRAME_CHANNEL_CAPACITY);
    let runtime_cancel = Arc::new(CancellationToken::new());
    {
        let mut cancellations = node_stream_cancellations.lock().await;
        let key = (ActiveRuntimeKind::Exec, request_id.clone());
        if cancellations.contains_key(&key) {
            return Err(anyhow!(
                "duplicate private node exec stream request id '{request_id}'"
            ));
        }
        cancellations.insert(key, runtime_cancel.clone());
    }
    {
        let mut routes = node_exec_inputs.lock().await;
        if routes.contains_key(&request_id) {
            drop(routes);
            remove_node_stream_cancellation_if_current(
                node_stream_cancellations,
                ActiveRuntimeKind::Exec,
                &request_id,
                &runtime_cancel,
            )
            .await;
            return Err(anyhow!(
                "duplicate private node exec input route id '{request_id}'"
            ));
        }
        routes.insert(
            request_id.clone(),
            NodeExecInputRoute {
                sender: input_tx,
                cancellation: runtime_cancel.clone(),
            },
        );
    }

    let task_request_id = request_id.clone();
    let task_cancel = runtime_cancel.clone();
    let output = outbound.clone();
    let routes = node_exec_inputs.clone();
    let cancellations = node_stream_cancellations.clone();
    tracing::debug!(
        request_id = %request_id,
        stdin = request.options().stdin(),
        stdout = request.options().stdout(),
        stderr = request.options().stderr(),
        tty = request.options().tty(),
        "registered node exec stream input route"
    );
    if let Err(err) = supervisor
        .spawn_async(
            TaskCategory::Network,
            "grpc_node_exec_runtime",
            async move {
                let (output_tx, mut output_rx) =
                    mpsc::channel(NODE_EXEC_STREAM_FRAME_CHANNEL_CAPACITY);
                let session = GrpcNodeExecSession {
                    input: Mutex::new(input_rx),
                    output: output_tx,
                    cancelled: AtomicBool::new(false),
                };
                let handler_task = handler.exec_stream(request, Box::new(session));
                tokio::pin!(handler_task);
                loop {
                    tokio::select! {
                        biased;
                        _ = task_cancel.cancelled() => {
                            break;
                        }
                        _ = &mut handler_task => {
                            while let Some(frame) = output_rx.recv().await {
                                let terminal = frame.is_terminal();
                                if send_node_exec_frame_to_leader_with_cancel(
                                    &output,
                                    &task_request_id,
                                    frame,
                                    &task_cancel,
                                ).await.is_err() || terminal {
                                    break;
                                }
                            }
                            break;
                        }
                        frame = output_rx.recv() => {
                            let Some(frame) = frame else {
                                break;
                            };
                            let terminal = frame.is_terminal();
                            if send_node_exec_frame_to_leader_with_cancel(
                                &output,
                                &task_request_id,
                                frame,
                                &task_cancel,
                            ).await.is_err() || terminal {
                                break;
                            }
                        }
                    }
                }
                remove_node_exec_routes_if_current(
                    &routes,
                    &cancellations,
                    &task_request_id,
                    &task_cancel,
                )
                .await;
            },
        )
        .await
    {
        remove_node_exec_routes_if_current(
            node_exec_inputs,
            node_stream_cancellations,
            &request_id,
            &runtime_cancel,
        )
        .await;
        return Err(err.into());
    }
    Ok(())
}

async fn send_node_exec_frame_to_leader_with_cancel(
    outbound: &mpsc::Sender<klights_internal_protobuf::FollowerMessage>,
    request_id: &str,
    frame: NodeExecFrame,
    cancel: &CancellationToken,
) -> Result<()> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(anyhow!("node exec stream cancelled")),
        result = send_node_exec_frame_to_leader(outbound, request_id, frame) => result,
    }
}

async fn send_node_exec_frame_to_leader(
    outbound: &mpsc::Sender<klights_internal_protobuf::FollowerMessage>,
    request_id: &str,
    frame: NodeExecFrame,
) -> Result<()> {
    outbound
        .send(klights_internal_protobuf::FollowerMessage {
            payload: Some(
                klights_internal_protobuf::follower_message::Payload::NodeExecStreamFrame(
                    node_exec_stream_frame_to_proto(request_id, frame),
                ),
            ),
        })
        .await
        .map_err(|_| anyhow!("replication stream closed before node exec stream frame send"))
}

async fn handle_pod_log_request(
    request: klights_internal_protobuf::PodLogRequest,
    handler: &NodeLogRuntimeSlot,
) -> klights_internal_protobuf::PodLogResponse {
    let request_id = request.request_id.clone();
    let request = match node_log_request_from_proto(request) {
        Ok(request) => request,
        Err(error) => {
            return node_log_error_to_proto(request_id, error.to_string());
        }
    };
    let Some(handler) = handler.lock().await.clone() else {
        return node_log_error_to_proto(request_id, "pod log handler is not available".to_string());
    };
    match handler.read_logs(request).await {
        Ok(result) => node_log_result_to_proto(request_id, result),
        Err(error) => node_log_error_to_proto(request_id, error.to_string()),
    }
}

async fn handle_node_metrics_request(
    request: klights_internal_protobuf::NodeMetricsRequest,
    runtime: &NodeMetricsRuntimeSlot,
) -> klights_internal_protobuf::NodeMetricsResponse {
    let request = match node_metrics_request_from_proto(request) {
        Ok(request) => request,
        Err(response) => return node_metrics_response_to_proto(response),
    };
    let request_id = request.request_id;
    let node_name = request.request.target().node_name().to_string();
    let result = match runtime.lock().await.clone() {
        Some(runtime) => runtime.collect_metrics(request.request).await,
        None => Err(NodeMetricsError::unavailable(
            "node metrics handler is not available",
        )),
    };
    node_metrics_response_to_proto(RoutedNodeMetricsResponse {
        request_id,
        node_name,
        result,
    })
}

async fn handle_pod_log_follow_request(
    request: klights_internal_protobuf::PodLogRequest,
    handler: &NodeLogRuntimeSlot,
    outbound: mpsc::Sender<klights_internal_protobuf::FollowerMessage>,
    supervisor: Arc<TaskSupervisor>,
    node_stream_cancellations: RuntimeCancellationRoutes,
) -> Result<()> {
    let request_id = request.request_id.clone();
    let request = match node_log_request_from_proto(request) {
        Ok(request) => request,
        Err(error) => {
            outbound
                .send(klights_internal_protobuf::FollowerMessage {
                    payload: Some(
                        klights_internal_protobuf::follower_message::Payload::PodLogResponse(
                            node_log_error_to_proto(request_id, error.to_string()),
                        ),
                    ),
                })
                .await
                .map_err(|_| anyhow!("replication stream closed before pod log response send"))?;
            return Ok(());
        }
    };
    let Some(handler) = handler.lock().await.clone() else {
        outbound
            .send(klights_internal_protobuf::FollowerMessage {
                payload: Some(
                    klights_internal_protobuf::follower_message::Payload::PodLogResponse(
                        klights_internal_protobuf::PodLogResponse {
                            request_id,
                            log_content: Vec::new(),
                            error: Some("pod log handler is not available".to_string()),
                            fin: true,
                        },
                    ),
                ),
            })
            .await
            .map_err(|_| anyhow!("replication stream closed before pod log response send"))?;
        return Ok(());
    };

    let runtime_cancel = Arc::new(CancellationToken::new());
    {
        let mut cancellations = node_stream_cancellations.lock().await;
        let key = (ActiveRuntimeKind::Log, request_id.clone());
        if cancellations.contains_key(&key) {
            return Err(anyhow!(
                "duplicate private pod log stream request id '{request_id}'"
            ));
        }
        cancellations.insert(key, runtime_cancel.clone());
    }
    let task_request_id = request_id.clone();
    let task_cancel = runtime_cancel.clone();
    let cancellations = node_stream_cancellations.clone();

    if let Err(error) = supervisor
        .spawn_async(
            TaskCategory::Network,
            "grpc_pod_log_follow_stream",
            async move {
                run_pod_log_follow_task(
                    handler,
                    request,
                    task_request_id.clone(),
                    outbound,
                    task_cancel.clone(),
                )
                .await;
                remove_node_stream_cancellation_if_current(
                    &cancellations,
                    ActiveRuntimeKind::Log,
                    &task_request_id,
                    &task_cancel,
                )
                .await;
            },
        )
        .await
    {
        remove_node_stream_cancellation_if_current(
            &node_stream_cancellations,
            ActiveRuntimeKind::Log,
            &request_id,
            &runtime_cancel,
        )
        .await;
        return Err(error.into());
    }
    Ok(())
}

async fn run_pod_log_follow_task(
    handler: Arc<dyn NodeLogRuntime>,
    request: NodeLogRequest,
    request_id: String,
    outbound: mpsc::Sender<klights_internal_protobuf::FollowerMessage>,
    cancel: Arc<CancellationToken>,
) {
    let open = handler.open_logs(request);
    let mut stream = match tokio::select! {
        biased;
        _ = cancel.cancelled() => return,
        result = open => result,
    } {
        Ok(stream) => stream,
        Err(error) => {
            let response = klights_internal_protobuf::PodLogResponse {
                request_id,
                log_content: Vec::new(),
                error: Some(error.to_string()),
                fin: true,
            };
            let _ = send_pod_log_response_with_cancel(&outbound, response, &cancel).await;
            return;
        }
    };
    loop {
        let event = tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            event = stream.recv_frame() => event,
        };
        match event {
            Ok(Some(event)) => {
                let terminal = event.is_terminal();
                let response = node_log_event_to_proto(request_id.clone(), event);
                if send_pod_log_response_with_cancel(&outbound, response, &cancel)
                    .await
                    .is_err()
                {
                    break;
                }
                if terminal {
                    break;
                }
            }
            Ok(None) => {
                let response = klights_internal_protobuf::PodLogResponse {
                    request_id: request_id.clone(),
                    log_content: Vec::new(),
                    error: None,
                    fin: true,
                };
                let _ = send_pod_log_response_with_cancel(&outbound, response, &cancel).await;
                break;
            }
            Err(error) => {
                let response = node_log_error_to_proto(request_id.clone(), error.to_string());
                let _ = send_pod_log_response_with_cancel(&outbound, response, &cancel).await;
                break;
            }
        }
    }
    let _ = stream.cancel().await;
}

async fn send_pod_log_response_with_cancel(
    outbound: &mpsc::Sender<klights_internal_protobuf::FollowerMessage>,
    response: klights_internal_protobuf::PodLogResponse,
    cancel: &CancellationToken,
) -> Result<()> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(anyhow!("node log stream cancelled")),
        result = outbound.send(klights_internal_protobuf::FollowerMessage {
            payload: Some(klights_internal_protobuf::follower_message::Payload::PodLogResponse(response)),
        }) => result.map_err(|_| anyhow!("replication stream closed before pod log response send")),
    }
}

// `impl CommandForwarder for ReplicationGrpcClient` removed in T6 along
// with the trait itself. Workers now use ApplyOutbox via the new
// LeaderApiClient surface.

fn resource_from_get_response(
    response: klights_internal_protobuf::GetResourceResponse,
) -> std::result::Result<Option<Resource>, ResourceQueryError> {
    match (response.found, response.resource) {
        (true, Some(resource)) => resource_from_proto(resource).map(Some),
        (false, None) => Ok(None),
        (true, None) => Err(ResourceQueryError::corrupt_response(
            "GetResource response marked found but omitted the resource",
        )),
        (false, Some(_)) => Err(ResourceQueryError::corrupt_response(
            "GetResource response carried a resource while marked not found",
        )),
    }
}

fn validate_list_response_metadata(
    response: &klights_internal_protobuf::ListResourcesResponse,
) -> std::result::Result<(), ResourceQueryError> {
    if response.resource_version < 0
        || response.remaining_item_count.is_some_and(|count| count < 0)
        || response.total != response.items.len() as i64
        || response
            .watch_replay_position
            .as_ref()
            .is_some_and(|position| {
                position.resource_version < 0
                    || position.event_id < 0
                    || position.resource_version_filter_through_event_id < 0
                    || position.resource_version != response.resource_version
            })
    {
        return Err(ResourceQueryError::corrupt_response(
            "ListResources response metadata is inconsistent",
        ));
    }
    Ok(())
}

fn resource_from_proto(
    resource: klights_internal_protobuf::ResourceObject,
) -> std::result::Result<Resource, ResourceQueryError> {
    let data: serde_json::Value = serde_json::from_slice(&resource.data_json).map_err(|error| {
        ResourceQueryError::corrupt_response(format!(
            "decode {} {} resource JSON: {error}",
            resource.api_version, resource.kind
        ))
    })?;
    let decoded = Resource::try_from_data(Arc::new(data)).map_err(|error| {
        ResourceQueryError::corrupt_response(format!("invalid resource object identity: {error}"))
    })?;
    if decoded.api_version != resource.api_version
        || decoded.kind != resource.kind
        || decoded.namespace != resource.namespace
        || decoded.name != resource.name
        || decoded.uid != resource.uid
        || decoded.resource_version != resource.resource_version
    {
        return Err(ResourceQueryError::corrupt_response(
            "resource wire identity or resourceVersion does not match its object body",
        ));
    }
    Ok(decoded)
}

fn resource_query_error_from_unary(error: UnaryRpcError) -> ResourceQueryError {
    match error {
        UnaryRpcError::Retryable(message) => ResourceQueryError::retryable(message),
        UnaryRpcError::Status(status) => match status.code() {
            tonic::Code::InvalidArgument => ResourceQueryError::InvalidRequest {
                field: "rpc.request",
                message: status.message().to_string(),
            },
            tonic::Code::DeadlineExceeded => ResourceQueryError::Timeout,
            tonic::Code::Cancelled => ResourceQueryError::Cancelled,
            tonic::Code::Unavailable | tonic::Code::FailedPrecondition => {
                ResourceQueryError::retryable(status.to_string())
            }
            _ => ResourceQueryError::query_failed(status.to_string()),
        },
    }
}

fn resource_command_result_from_proto(
    response: klights_internal_protobuf::SubmitResourceCommandResponse,
) -> std::result::Result<ResourceCommandResult, ResourceCommandError> {
    use klights_internal_protobuf::submit_resource_command_response::Result as WireResult;
    match response.result {
        Some(WireResult::Ack(ack)) => {
            ResourceCommandResult::try_from_response(klights_cluster_core::StorageResponse::Ack {
                resource_version: ack.resource_version,
            })
        }
        Some(WireResult::Resource(wire)) => {
            let data: serde_json::Value =
                serde_json::from_slice(&wire.data_json).map_err(|error| {
                    ResourceCommandError::corrupt_response(format!(
                        "decode resource command response JSON: {error}"
                    ))
                })?;
            let result = ResourceCommandResult::try_from_response(
                klights_cluster_core::StorageResponse::Resource {
                    resource_version: wire.resource_version,
                    data,
                },
            )?;
            let ResourceCommandResult::Resource(resource) = &result else {
                unreachable!("resource response conversion returned a non-resource result")
            };
            if resource.api_version != wire.api_version
                || resource.kind != wire.kind
                || resource.namespace != wire.namespace
                || resource.name != wire.name
                || resource.uid != wire.uid
            {
                return Err(ResourceCommandError::corrupt_response(
                    "resource command wire identity does not match its object",
                ));
            }
            Ok(result)
        }
        None => Err(ResourceCommandError::corrupt_response(
            "resource command response is missing its result",
        )),
    }
}

fn resource_command_rpc_error(error: UnaryRpcError) -> ResourceCommandError {
    match error {
        UnaryRpcError::Retryable(message) => ResourceCommandError::retryable(message),
        UnaryRpcError::Status(status) => {
            let message = status.message().to_string();
            match status.code() {
                tonic::Code::InvalidArgument => ResourceCommandError::InvalidRequest {
                    field: "command",
                    message,
                },
                tonic::Code::PermissionDenied | tonic::Code::Unauthenticated => {
                    ResourceCommandError::Unauthorized
                }
                tonic::Code::FailedPrecondition => ResourceCommandError::NotLeader,
                tonic::Code::AlreadyExists => ResourceCommandError::AlreadyExists { message },
                tonic::Code::Aborted => ResourceCommandError::Conflict { message },
                tonic::Code::NotFound => ResourceCommandError::NotFound { message },
                tonic::Code::DeadlineExceeded => ResourceCommandError::Timeout,
                tonic::Code::Cancelled => ResourceCommandError::Cancelled,
                tonic::Code::Unavailable => ResourceCommandError::retryable(message),
                _ => ResourceCommandError::submission_failed(status.to_string()),
            }
        }
    }
}

fn watch_status_error(status: tonic::Status) -> LeaderWatchError {
    if let Some(accepted_resource_version) =
        crate::replication::grpc::watch_replay_expired_resource_version(&status)
    {
        return LeaderWatchError::ReplayExpired {
            accepted_resource_version,
        };
    }
    match status.code() {
        tonic::Code::Cancelled => LeaderWatchError::Cancelled,
        tonic::Code::DeadlineExceeded => LeaderWatchError::Timeout,
        tonic::Code::Unavailable | tonic::Code::FailedPrecondition => {
            LeaderWatchError::unavailable(status.to_string())
        }
        _ => LeaderWatchError::transport(status.to_string()),
    }
}

fn watch_rpc_error(error: anyhow::Error) -> LeaderWatchError {
    if let Some(status) = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<tonic::Status>())
    {
        return watch_status_error(status.clone());
    }
    LeaderWatchError::transport(format!("gRPC WatchResources failed: {error:#}"))
}

fn resource_event_from_proto(
    event: klights_internal_protobuf::WatchEvent,
) -> std::result::Result<ResourceEvent, LeaderWatchError> {
    let resume_position = event
        .resume_position
        .as_ref()
        .map(watch_replay_position_from_proto);
    let resource = event.resource.ok_or_else(|| {
        LeaderWatchError::malformed_event("WatchResources event missing resource")
    })?;
    let resource = resource_from_proto(resource)
        .map_err(|error| LeaderWatchError::malformed_event(error.to_string()))?;
    ResourceEvent::try_from_wire_type(&event.event_type, resource, resume_position)
}

fn node_subnet_from_proto(
    subnet: klights_internal_protobuf::NodeSubnetObject,
) -> std::result::Result<klights_leader_api::NodeSubnet, NetworkTopologyError> {
    let gateway_ip = subnet.gateway_ip.parse().map_err(|error| {
        NetworkTopologyError::corrupt_response(format!(
            "invalid node subnet gateway IP '{}': {error}",
            subnet.gateway_ip
        ))
    })?;
    let node_ip = subnet.node_ip.parse().map_err(|error| {
        NetworkTopologyError::corrupt_response(format!(
            "invalid node subnet node IP '{}': {error}",
            subnet.node_ip
        ))
    })?;
    let mode = match subnet.mode.as_str() {
        "root" => klights_leader_api::NetworkNodeMode::Root,
        "rootless" => klights_leader_api::NetworkNodeMode::Rootless,
        other => {
            return Err(NetworkTopologyError::corrupt_response(format!(
                "invalid node subnet mode '{other}'"
            )));
        }
    };
    let hostport_range = subnet
        .hostport_range
        .as_deref()
        .map(|raw| {
            let (start, end) = raw.split_once('-').ok_or_else(|| {
                NetworkTopologyError::corrupt_response(format!(
                    "invalid node subnet host-port range '{raw}'"
                ))
            })?;
            let start = start.parse::<u16>().map_err(|error| {
                NetworkTopologyError::corrupt_response(format!(
                    "invalid host-port range start '{start}': {error}"
                ))
            })?;
            let end = end.parse::<u16>().map_err(|error| {
                NetworkTopologyError::corrupt_response(format!(
                    "invalid host-port range end '{end}': {error}"
                ))
            })?;
            klights_leader_api::HostPortRange::try_new(start, end)
        })
        .transpose()?;
    klights_leader_api::NodeSubnet::try_new(
        subnet.node_name,
        subnet.subnet,
        subnet.subnet_base_int,
        gateway_ip,
        node_ip,
        mode,
        hostport_range,
    )
}

fn dataplane_metadata_from_proto(
    metadata: klights_internal_protobuf::DataplaneMetadataObject,
) -> std::result::Result<klights_leader_api::NetworkDataplane, NetworkTopologyError> {
    let port = metadata
        .port
        .map(u16::try_from)
        .transpose()
        .map_err(|_| NetworkTopologyError::corrupt_response("dataplane port exceeds u16"))?;
    let mode = match metadata.mode.as_str() {
        "root" => klights_leader_api::NetworkNodeMode::Root,
        "rootless" => klights_leader_api::NetworkNodeMode::Rootless,
        other => {
            return Err(NetworkTopologyError::corrupt_response(format!(
                "invalid dataplane mode '{other}'"
            )));
        }
    };
    let encryption = match metadata.encryption.as_str() {
        "enabled" => klights_leader_api::DataplaneEncryption::WireGuard,
        "disabled" => klights_leader_api::DataplaneEncryption::Direct,
        other => {
            return Err(NetworkTopologyError::corrupt_response(format!(
                "invalid dataplane encryption '{other}'"
            )));
        }
    };
    let endpoint = metadata.endpoint.parse().map_err(|error| {
        NetworkTopologyError::corrupt_response(format!(
            "invalid dataplane endpoint '{}': {error}",
            metadata.endpoint
        ))
    })?;
    klights_leader_api::NetworkDataplane::try_new(
        metadata.node_name,
        mode,
        encryption,
        metadata.public_key.as_deref(),
        endpoint,
        port,
    )
}

pub(super) fn pod_cleanup_intent_from_proto(
    intent: klights_internal_protobuf::PodCleanupIntentObject,
) -> std::result::Result<PodCleanupIntent, PodCleanupIntentError> {
    let pod_data = serde_json::from_slice(&intent.pod_data_json).map_err(|error| {
        PodCleanupIntentError::corrupt_intent(format!(
            "decode Pod cleanup intent JSON for {}/{} uid={}: {error}",
            intent.namespace, intent.pod_name, intent.pod_uid
        ))
    })?;
    let pod_snapshot = Resource::try_from_data(Arc::new(pod_data)).map_err(|error| {
        PodCleanupIntentError::corrupt_intent(format!(
            "decode Pod cleanup intent identity for {}/{} uid={}: {error}",
            intent.namespace, intent.pod_name, intent.pod_uid
        ))
    })?;
    PodCleanupIntent::try_new(
        intent.node_name,
        intent.namespace,
        intent.pod_name,
        intent.pod_uid,
        intent.reason,
        intent.resource_version,
        intent.created_at_ms,
        pod_snapshot,
    )
}

impl LeaderOutboxDelivery for ReplicationGrpcClient {
    fn deliver_outbox(&self, request: OutboxDeliveryRequest) -> OutboxDeliveryFuture<'_> {
        Box::pin(async move { self.apply_outbox_rpc(request).await })
    }
}

fn decode_apply_outbox_response(
    response: klights_internal_protobuf::ApplyOutboxResponse,
) -> std::result::Result<OutboxDeliveryResult, OutboxDeliveryError> {
    if response.error.is_some() && (response.already_applied || response.applied_rv != 0) {
        return Err(OutboxDeliveryError::corrupt_response(
            "ApplyOutbox response carried both an error and successful apply evidence",
        ));
    }
    match (response.error, response.error_type.as_deref()) {
        (Some(message), error_type) => {
            return Err(outbox_error_from_response(error_type, message));
        }
        (None, Some(error_type)) => {
            return Err(OutboxDeliveryError::corrupt_response(format!(
                "ApplyOutbox success response carried error_type {error_type:?} without an error",
            )));
        }
        (None, None) => {}
    }

    if response.already_applied {
        OutboxDeliveryResult::try_already_applied(
            (response.applied_rv > 0).then_some(response.applied_rv),
        )
    } else {
        OutboxDeliveryResult::try_applied(response.applied_rv).map_err(|error| {
            OutboxDeliveryError::corrupt_response(format!(
                "ApplyOutbox returned an invalid applied resourceVersion: {error}",
            ))
        })
    }
}

fn outbox_error_from_response(error_type: Option<&str>, message: String) -> OutboxDeliveryError {
    match error_type {
        Some("CodecIncompatible") => {
            OutboxDeliveryError::codec_incompatible(0, klights_cluster_core::COMMAND_CODEC_VERSION)
        }
        Some("NotFound") => OutboxDeliveryError::NotFound(message),
        Some("UidMismatch") => OutboxDeliveryError::UidMismatch {
            expected: "<unknown>".to_string(),
            actual: "<unknown>".to_string(),
        },
        Some("ConflictTerminal") => OutboxDeliveryError::ConflictTerminal(message),
        Some("Retryable") => OutboxDeliveryError::Retryable(message),
        Some("InvalidRequest") => OutboxDeliveryError::InvalidRequest {
            field: "server.delivery",
            message,
        },
        Some("NotLeader") => OutboxDeliveryError::NotLeader,
        Some("Timeout") => OutboxDeliveryError::Timeout,
        Some("Cancelled") => OutboxDeliveryError::Cancelled,
        Some("CorruptResponse") => OutboxDeliveryError::CorruptResponse { message },
        Some(error_type) => OutboxDeliveryError::CorruptResponse {
            message: format!("unknown ApplyOutbox error_type {error_type:?}: {message}"),
        },
        None => OutboxDeliveryError::CorruptResponse {
            message: format!("ApplyOutbox error response omitted error_type: {message}"),
        },
    }
}

fn outbox_error_from_status(status: tonic::Status) -> OutboxDeliveryError {
    match status.code() {
        tonic::Code::FailedPrecondition if is_not_raft_leader_status(&status) => {
            OutboxDeliveryError::NotLeader
        }
        tonic::Code::DeadlineExceeded => OutboxDeliveryError::Timeout,
        tonic::Code::Cancelled => OutboxDeliveryError::Cancelled,
        // A transport status does not prove that the leader durably consumed
        // this exact stream position. Terminal decisions are accepted only in
        // the typed response body, which the server emits after its
        // ledger/watermark commit. Keep every other status retryable.
        _ => OutboxDeliveryError::Retryable(status.to_string()),
    }
}

/// bug-grpc A2: outcome of the generic [`ReplicationGrpcClient::unary_call`]
/// executor.
#[derive(Debug)]
pub enum UnaryRpcError {
    /// Every endpoint candidate failed transiently — connect failure,
    /// `not raft leader`, transport wedge, or per-call deadline. The caller's
    /// durable retry (outbox dispatcher, heartbeat loop, reconcile) should
    /// re-attempt; the offending lane has already been evicted where relevant.
    Retryable(String),
    /// The leader returned an application-level gRPC error (`NotFound`,
    /// `InvalidArgument`, `AlreadyExists`, …). Not retryable by the transport
    /// layer; the caller decides how to surface it.
    Status(tonic::Status),
}

impl std::fmt::Display for UnaryRpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnaryRpcError::Retryable(message) => write!(f, "retryable unary RPC error: {message}"),
            UnaryRpcError::Status(status) => write!(f, "{status}"),
        }
    }
}

impl UnaryRpcError {
    /// Map to an `anyhow::Error` with a call-site context string, for the
    /// RPCs whose public signature is `anyhow::Result<T>`.
    fn into_anyhow(self, context: &'static str) -> anyhow::Error {
        anyhow::Error::new(self).context(context)
    }
}

impl std::error::Error for UnaryRpcError {}

fn node_lease_renewal_error_from_unary(
    error: UnaryRpcError,
) -> klights_leader_api::NodeLeaseRenewalError {
    use klights_leader_api::NodeLeaseRenewalError;

    match error {
        UnaryRpcError::Retryable(message)
            if message.to_ascii_lowercase().contains("not raft leader") =>
        {
            NodeLeaseRenewalError::NotLeader
        }
        UnaryRpcError::Retryable(message) => NodeLeaseRenewalError::retryable(message),
        UnaryRpcError::Status(status) => {
            let message = status.message().to_string();
            match status.code() {
                tonic::Code::InvalidArgument => NodeLeaseRenewalError::InvalidRequest {
                    field: "lease",
                    message,
                },
                tonic::Code::Unauthenticated | tonic::Code::PermissionDenied => {
                    NodeLeaseRenewalError::unauthorized(message)
                }
                tonic::Code::FailedPrecondition if is_not_raft_leader_status(&status) => {
                    NodeLeaseRenewalError::NotLeader
                }
                tonic::Code::Unavailable => NodeLeaseRenewalError::unavailable(message),
                tonic::Code::DeadlineExceeded => NodeLeaseRenewalError::Timeout,
                tonic::Code::Cancelled => NodeLeaseRenewalError::Cancelled,
                _ => NodeLeaseRenewalError::retryable(status.to_string()),
            }
        }
    }
}

fn node_subnet_allocation_error_from_unary(error: UnaryRpcError) -> NodeSubnetAllocationError {
    match error {
        UnaryRpcError::Retryable(message)
            if message.to_ascii_lowercase().contains("not raft leader") =>
        {
            NodeSubnetAllocationError::NotLeader
        }
        UnaryRpcError::Retryable(message) => NodeSubnetAllocationError::retryable(message),
        UnaryRpcError::Status(status) => {
            let message = status.message().to_string();
            match status.code() {
                tonic::Code::InvalidArgument => {
                    NodeSubnetAllocationError::invalid_request("request", message)
                }
                tonic::Code::Unauthenticated | tonic::Code::PermissionDenied => {
                    NodeSubnetAllocationError::unauthorized(message)
                }
                tonic::Code::AlreadyExists | tonic::Code::Aborted => {
                    NodeSubnetAllocationError::conflict(message)
                }
                tonic::Code::ResourceExhausted => NodeSubnetAllocationError::exhausted(message),
                tonic::Code::FailedPrecondition
                    if message.to_ascii_lowercase().contains("not raft leader") =>
                {
                    NodeSubnetAllocationError::NotLeader
                }
                tonic::Code::DeadlineExceeded => NodeSubnetAllocationError::Timeout,
                tonic::Code::Cancelled => NodeSubnetAllocationError::Cancelled,
                _ => NodeSubnetAllocationError::allocation_failed(status.to_string()),
            }
        }
    }
}

fn network_topology_error_from_unary(error: UnaryRpcError) -> NetworkTopologyError {
    match error {
        UnaryRpcError::Retryable(message)
            if message.to_ascii_lowercase().contains("not raft leader") =>
        {
            NetworkTopologyError::NotLeader
        }
        UnaryRpcError::Retryable(message) => NetworkTopologyError::retryable(message),
        UnaryRpcError::Status(status) => {
            let message = status.message().to_string();
            match status.code() {
                tonic::Code::InvalidArgument => {
                    NetworkTopologyError::invalid_request("request", message)
                }
                tonic::Code::Unauthenticated | tonic::Code::PermissionDenied => {
                    NetworkTopologyError::unauthorized(message)
                }
                tonic::Code::FailedPrecondition
                    if message.to_ascii_lowercase().contains("not raft leader") =>
                {
                    NetworkTopologyError::NotLeader
                }
                tonic::Code::DeadlineExceeded => NetworkTopologyError::Timeout,
                tonic::Code::Cancelled => NetworkTopologyError::Cancelled,
                _ => NetworkTopologyError::query_failed(status.to_string()),
            }
        }
    }
}

fn projected_token_error_from_unary(error: UnaryRpcError) -> ProjectedServiceAccountTokenError {
    match error {
        UnaryRpcError::Retryable(message)
            if message.to_ascii_lowercase().contains("not raft leader") =>
        {
            ProjectedServiceAccountTokenError::NotLeader
        }
        UnaryRpcError::Retryable(message) => {
            ProjectedServiceAccountTokenError::unavailable(message)
        }
        UnaryRpcError::Status(status) => {
            let message = status.message().to_string();
            match status.code() {
                tonic::Code::Unauthenticated | tonic::Code::PermissionDenied => {
                    ProjectedServiceAccountTokenError::Unauthorized
                }
                tonic::Code::Aborted => {
                    ProjectedServiceAccountTokenError::binding_mismatch(message)
                }
                tonic::Code::NotFound
                    if message.to_ascii_lowercase().contains("serviceaccount") =>
                {
                    ProjectedServiceAccountTokenError::ServiceAccountNotFound
                }
                tonic::Code::NotFound if message.to_ascii_lowercase().contains("pod") => {
                    ProjectedServiceAccountTokenError::BoundPodNotFound
                }
                tonic::Code::NotFound => ProjectedServiceAccountTokenError::BoundNodeNotFound,
                tonic::Code::FailedPrecondition
                    if message.to_ascii_lowercase().contains("not raft leader") =>
                {
                    ProjectedServiceAccountTokenError::NotLeader
                }
                tonic::Code::FailedPrecondition => {
                    ProjectedServiceAccountTokenError::signing_failed(message)
                }
                tonic::Code::DataLoss => {
                    ProjectedServiceAccountTokenError::corrupt_resource(message)
                }
                tonic::Code::DeadlineExceeded => ProjectedServiceAccountTokenError::Timeout,
                tonic::Code::Cancelled => ProjectedServiceAccountTokenError::Cancelled,
                tonic::Code::InvalidArgument => {
                    ProjectedServiceAccountTokenError::corrupt_response(message)
                }
                _ => ProjectedServiceAccountTokenError::transport(status.to_string()),
            }
        }
    }
}

fn pod_cleanup_intent_error_from_unary(error: UnaryRpcError) -> PodCleanupIntentError {
    match error {
        UnaryRpcError::Retryable(message)
            if message.to_ascii_lowercase().contains("not raft leader") =>
        {
            PodCleanupIntentError::NotLeader
        }
        UnaryRpcError::Retryable(message) => PodCleanupIntentError::unavailable(message),
        UnaryRpcError::Status(status) => {
            let message = status.message().to_string();
            match status.code() {
                tonic::Code::Unauthenticated | tonic::Code::PermissionDenied => {
                    PodCleanupIntentError::Unauthorized
                }
                tonic::Code::FailedPrecondition
                    if message.to_ascii_lowercase().contains("not raft leader") =>
                {
                    PodCleanupIntentError::NotLeader
                }
                tonic::Code::DataLoss | tonic::Code::InvalidArgument => {
                    PodCleanupIntentError::corrupt_intent(message)
                }
                tonic::Code::DeadlineExceeded => PodCleanupIntentError::Timeout,
                tonic::Code::Cancelled => PodCleanupIntentError::Cancelled,
                _ => PodCleanupIntentError::transport(status.to_string()),
            }
        }
    }
}

fn is_not_raft_leader_status(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::FailedPrecondition
        && status
            .message()
            .to_ascii_lowercase()
            .contains("not raft leader")
}

/// Whether a gRPC status reflects a transport-level failure (the peer is
/// unreachable / the connection wedged or was reset) rather than an
/// application-level rejection. tonic surfaces a dropped or refused
/// HTTP/2 connection — exactly what a leader restart produces — as
/// `Unavailable` or `Unknown`. Application errors (`FailedPrecondition`
/// such as `not raft leader`, `NotFound`, `AlreadyExists`, `Aborted`,
/// `InvalidArgument`, ...) are deliberately excluded so a healthy
/// connection is never evicted. Kept conservative: over-eviction only
/// costs one pool rebuild; under-eviction reintroduces the wedge.
fn is_transport_status(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable | tonic::Code::Unknown | tonic::Code::Cancelled
    )
}

/// bug-grpc: DRY constructor applying the policy's message-size limits to
/// every tonic client built from a pooled channel.
fn tonic_client_with_limits(channel: Channel, max_message_bytes: usize) -> TonicClient<Channel> {
    TonicClient::new(channel)
        .max_decoding_message_size(max_message_bytes)
        .max_encoding_message_size(max_message_bytes)
}

fn normalized_endpoint(endpoint: &str) -> Result<String> {
    let trimmed = endpoint.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("leader endpoint is empty"));
    }
    if trimmed.contains("://") {
        if trimmed.starts_with("https://")
            || (allow_plaintext_leader_endpoint_for_tests() && trimmed.starts_with("http://"))
        {
            Ok(trimmed.to_string())
        } else {
            Err(anyhow!(
                "leader endpoint must use https://, got '{}'",
                trimmed
            ))
        }
    } else {
        Ok(format!("https://{trimmed}"))
    }
}

#[cfg(test)]
fn allow_plaintext_leader_endpoint_for_tests() -> bool {
    true
}

#[cfg(not(test))]
fn allow_plaintext_leader_endpoint_for_tests() -> bool {
    false
}

fn endpoint_host(endpoint: &str) -> Result<String> {
    let uri = endpoint
        .parse::<hyper::Uri>()
        .with_context(|| format!("invalid leader endpoint URI '{endpoint}'"))?;
    uri.host()
        .map(str::to_string)
        .ok_or_else(|| anyhow!("leader endpoint has no host: {endpoint}"))
}

fn join_response_from_leader_message(
    message: klights_internal_protobuf::LeaderMessage,
) -> Result<JoinResponse> {
    match message.payload {
        Some(klights_internal_protobuf::leader_message::Payload::JoinResponse(response)) => {
            match response.result {
                Some(klights_internal_protobuf::join_response::Result::Accepted(accepted)) => {
                    Ok(JoinResponse::Accepted {
                        cluster_id: accepted.cluster_id,
                        leader_epoch: accepted.leader_epoch,
                        current_rv: accepted.current_rv,
                    })
                }
                Some(klights_internal_protobuf::join_response::Result::Rejected(rejected)) => {
                    Ok(JoinResponse::Rejected {
                        reason: rejected.reason,
                    })
                }
                None => Err(anyhow!("empty JoinResponse")),
            }
        }
        other => Err(anyhow!("expected JoinResponse, got {other:?}")),
    }
}

fn stream_item_from_proto(item: klights_internal_protobuf::StreamItem) -> Result<StreamItem> {
    match item.item {
        Some(klights_internal_protobuf::stream_item::Item::Entry(entry)) => {
            Ok(StreamItem::Entry(Box::new(entry_from_proto(entry)?)))
        }
        Some(klights_internal_protobuf::stream_item::Item::Heartbeat(heartbeat)) => {
            Ok(StreamItem::Heartbeat {
                current_rv: heartbeat.current_rv,
            })
        }
        None => Err(anyhow!("empty StreamItem")),
    }
}

// `forwarded_write_from_response` removed in T6.

fn node_exec_sync_request_from_proto(
    request: klights_internal_protobuf::NodeExecSyncRequest,
) -> Result<NodeExecSyncRequest> {
    let target = NodeExecTarget::try_new(
        request.node_name,
        request.namespace,
        request.pod_name,
        request.container_id,
    )?;
    Ok(NodeExecSyncRequest::try_new(
        target,
        request.command,
        request.timeout_seconds,
    )?)
}

fn node_exec_sync_response_to_proto(
    request_id: String,
    response: NodeExecSyncResult,
) -> klights_internal_protobuf::NodeExecSyncResponse {
    let (stdout, stderr, exit_code, terminal_error) = response.into_parts();
    klights_internal_protobuf::NodeExecSyncResponse {
        request_id,
        stdout,
        stderr,
        exit_code,
        error: terminal_error.map(ExecTerminalError::into_message),
    }
}

fn node_exec_request_from_proto(
    request: klights_internal_protobuf::NodeExecRequest,
) -> Result<NodeExecRequest> {
    let target = NodeExecTarget::try_new(
        request.node_name,
        request.namespace,
        request.pod_name,
        request.container_id,
    )?;
    let options =
        ExecStreamOptions::new(request.stdin, request.stdout, request.stderr, request.tty);
    Ok(if request.attach {
        NodeExecRequest::attach(target, options)
    } else {
        NodeExecRequest::exec(target, request.command, options)
    })
}

fn node_exec_stream_frame_to_proto(
    request_id: &str,
    frame: NodeExecFrame,
) -> klights_internal_protobuf::NodeExecStreamFrame {
    let (channel, data, fin) = frame.into_parts();
    klights_internal_protobuf::NodeExecStreamFrame {
        request_id: request_id.to_string(),
        channel: channel.as_wire_name().to_string(),
        data,
        fin,
    }
}

fn node_exec_stream_frame_from_proto(
    frame: klights_internal_protobuf::NodeExecStreamFrame,
) -> Result<(String, NodeExecFrame)> {
    let channel = ExecStreamChannel::try_from_wire_name(&frame.channel)
        .ok_or_else(|| anyhow!("unknown node exec stream channel '{}'", frame.channel))?;
    Ok((
        frame.request_id,
        NodeExecFrame::new(channel, frame.data, frame.fin),
    ))
}

fn node_log_request_from_proto(
    request: klights_internal_protobuf::PodLogRequest,
) -> std::result::Result<NodeLogRequest, NodeLogSetupError> {
    let target = NodeLogTarget::try_new(
        request.node_name,
        request.namespace,
        request.pod_name,
        request.pod_uid,
        request.container_name,
    )?;
    let options = klights_node_api::NodeLogOptions::new(
        request.follow,
        request.tail_lines.and_then(|value| value.parse().ok()),
        request.timestamps,
        request.since_time,
        request.since_seconds,
        request
            .limit_bytes
            .and_then(|value| usize::try_from(value).ok()),
        request.previous,
    );
    Ok(NodeLogRequest::new(target, options))
}

fn node_log_result_to_proto(
    request_id: String,
    response: NodeLogResult,
) -> klights_internal_protobuf::PodLogResponse {
    let (log_content, terminal_error) = response.into_parts();
    klights_internal_protobuf::PodLogResponse {
        request_id,
        log_content,
        error: terminal_error.map(NodeLogTerminalError::into_message),
        fin: true,
    }
}

fn node_log_event_to_proto(
    request_id: String,
    event: NodeLogEvent,
) -> klights_internal_protobuf::PodLogResponse {
    let (log_content, terminal_error, terminal) = event.into_parts();
    klights_internal_protobuf::PodLogResponse {
        request_id,
        log_content,
        error: terminal_error.map(NodeLogTerminalError::into_message),
        fin: terminal,
    }
}

fn node_log_error_to_proto(
    request_id: String,
    error: String,
) -> klights_internal_protobuf::PodLogResponse {
    klights_internal_protobuf::PodLogResponse {
        request_id,
        log_content: Vec::new(),
        error: Some(error),
        fin: true,
    }
}

fn node_metrics_request_from_proto(
    request: klights_internal_protobuf::NodeMetricsRequest,
) -> std::result::Result<RoutedNodeMetricsRequest, RoutedNodeMetricsResponse> {
    let request_id = request.request_id;
    let node_name = request.node_name;
    let target = match NodeMetricsTarget::try_new(node_name.clone()) {
        Ok(target) => target,
        Err(error) => {
            return Err(RoutedNodeMetricsResponse {
                request_id,
                node_name,
                result: Err(error),
            });
        }
    };
    Ok(RoutedNodeMetricsRequest {
        request_id,
        request: NodeMetricsRequest::new(target, request.pod_uids),
    })
}

fn node_metrics_response_to_proto(
    response: RoutedNodeMetricsResponse,
) -> klights_internal_protobuf::NodeMetricsResponse {
    let (node, pods, error) = match response.result {
        Ok(result) => {
            let (_target, node, pods) = result.into_parts();
            (node, pods, None)
        }
        Err(error) => (None, Vec::new(), Some(error.to_string())),
    };
    klights_internal_protobuf::NodeMetricsResponse {
        request_id: response.request_id,
        node_name: response.node_name,
        node: node.map(|node| klights_internal_protobuf::NodeMetricsNodeSample {
            cpu_nanos: node.cpu_nanos(),
            memory_bytes: node.memory_bytes(),
        }),
        pods: pods
            .into_iter()
            .map(|pod| {
                let (namespace, name, uid, containers) = pod.into_parts();
                klights_internal_protobuf::NodeMetricsPodSample {
                    namespace,
                    name,
                    uid,
                    containers: containers
                        .into_iter()
                        .map(|container| {
                            let (name, cpu_nanos, memory_bytes) = container.into_parts();
                            klights_internal_protobuf::NodeMetricsContainerSample {
                                name,
                                cpu_nanos,
                                memory_bytes,
                            }
                        })
                        .collect(),
                }
            })
            .collect(),
        error,
    }
}

// `forwarded_*_from_proto` helpers removed in T6 along with the
// ForwardedResource / ForwardedNodeSubnet / ForwardedPodSlotAdmission
// proto messages.

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
