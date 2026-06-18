use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
#[cfg(test)]
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{Stream, StreamExt as _};
use hyper_util::rt::TokioIo;
use tokio::sync::{Mutex, mpsc};
use tokio_rustls::rustls::{
    DigitallySignedStruct, Error as TlsError, SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    crypto::{self, CryptoProvider},
    pki_types::{CertificateDer, ServerName, UnixTime},
};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity, Uri};
use tower::Service;

use crate::control_plane::client::{
    ListRequest, ListResponse, ProjectedServiceAccountToken, ProjectedServiceAccountTokenRequest,
    ResourceEvent, ResourceKey, WatchRequest, WatchStream,
};
use crate::datastore::{NodeSubnet, PodCleanupIntent, Resource};
use crate::kubelet::outbox::payload::OutboxOperation;
use crate::kubelet::outbox::{OutboxApplyError, OutboxApplyResult};
use crate::leader_tls_policy::{LeaderTlsVerification, LeaderTlsVerificationPolicy};
use crate::networking::wireguard::{DataplaneEncryption, DataplaneMode, DataplanePeerMetadata};
use crate::replication::grpc::generated::replication_client::ReplicationClient as TonicClient;
use crate::replication::grpc::transport_policy::GrpcTransportPolicy;
use crate::replication::grpc::{
    JOIN_TOKEN_METADATA_KEY, entry_from_proto, generated, log_apply_commit_from_proto,
};
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
    ExecStreamChannel, JoinResponse, JoinRole, NodeExecRequest, NodeExecStreamFrame,
    NodeExecSyncRequest, NodeExecSyncResponse, PodLogRequest, PodLogResponse, StreamItem,
};
use crate::task_supervisor::{TaskCategory, TaskSupervisor};

const CONNECT_CHANNEL_CAPACITY: usize = 64;
const STREAM_ITEM_CHANNEL_CAPACITY: usize = 1024;
const NODE_EXEC_STREAM_FRAME_CHANNEL_CAPACITY: usize = 128;
// bug-grpc A1: message-size limits now live on `GrpcTransportPolicy`
// (`max_message_bytes`); the former `MAX_GRPC_MESSAGE_BYTES` constant is
// gone so client, CRI, and server cannot drift.
// `DEFAULT_FORWARD_RESPONSE_TIMEOUT` and `PendingForward` removed in T6.
type StreamItemQueue = Arc<Mutex<mpsc::Receiver<Result<StreamItem>>>>;
type NodeExecSyncHandlerSlot = Arc<Mutex<Option<Arc<dyn NodeExecSyncHandler>>>>;
type NodeExecStreamHandlerSlot = Arc<Mutex<Option<Arc<dyn NodeExecStreamHandler>>>>;
type NodeExecInputRoutes =
    Arc<Mutex<std::collections::HashMap<String, mpsc::Sender<NodeExecStreamFrame>>>>;
type PodLogHandlerSlot = Arc<Mutex<Option<Arc<dyn PodLogHandler>>>>;

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
    node_exec_sync_handler: NodeExecSyncHandlerSlot,
    node_exec_stream_handler: NodeExecStreamHandlerSlot,
    node_exec_inputs: NodeExecInputRoutes,
    pod_log_handler: PodLogHandlerSlot,
    observed_leader_endpoint: Option<String>,
}

#[async_trait]
pub trait NodeExecSyncHandler: Send + Sync {
    async fn exec_sync(&self, request: NodeExecSyncRequest) -> NodeExecSyncResponse;
}

#[async_trait]
pub trait NodeExecStreamHandler: Send + Sync {
    async fn exec_stream(
        &self,
        request: NodeExecRequest,
        input: mpsc::Receiver<NodeExecStreamFrame>,
        output: mpsc::Sender<NodeExecStreamFrame>,
    );
}

#[async_trait]
pub trait PodLogHandler: Send + Sync {
    async fn get_logs(&self, request: PodLogRequest) -> PodLogResponse;
    fn follow_logs(
        &self,
        request: PodLogRequest,
    ) -> Pin<Box<dyn Stream<Item = PodLogResponse> + Send>>;
}

pub struct CriNodeExecSyncHandler {
    cri: Arc<tokio::sync::Mutex<crate::kubelet::cri::CriClient>>,
    task_supervisor: Arc<TaskSupervisor>,
}

impl CriNodeExecSyncHandler {
    pub fn new(
        cri: Arc<tokio::sync::Mutex<crate::kubelet::cri::CriClient>>,
        task_supervisor: Arc<TaskSupervisor>,
    ) -> Self {
        Self {
            cri,
            task_supervisor,
        }
    }
}

pub struct LocalPodLogHandler {
    containerd_namespace: String,
    task_supervisor: Arc<TaskSupervisor>,
    pod_event_db: Option<crate::datastore::DatastoreHandle>,
}

impl LocalPodLogHandler {
    pub fn new(containerd_namespace: String, task_supervisor: Arc<TaskSupervisor>) -> Self {
        Self {
            containerd_namespace,
            task_supervisor,
            pod_event_db: None,
        }
    }

    pub fn new_with_pod_event_store(
        containerd_namespace: String,
        task_supervisor: Arc<TaskSupervisor>,
        pod_event_db: crate::datastore::DatastoreHandle,
    ) -> Self {
        Self {
            containerd_namespace,
            task_supervisor,
            pod_event_db: Some(pod_event_db),
        }
    }

    fn log_path(&self, request: &PodLogRequest) -> String {
        crate::paths::pod_log_dir_path(
            &self.containerd_namespace,
            &request.namespace,
            &request.pod_name,
            &request.pod_uid,
        )
        .join(&request.container_name)
        .join("0.log")
        .to_string_lossy()
        .into_owned()
    }
}

#[async_trait]
impl PodLogHandler for LocalPodLogHandler {
    async fn get_logs(&self, request: PodLogRequest) -> PodLogResponse {
        let log_path = self.log_path(&request);

        let params = crate::api_pod_subresources::logs::LogQuery {
            container: Some(request.container_name.clone()),
            follow: None,
            tail_lines: request.tail_lines.as_deref().and_then(|t| t.parse().ok()),
            timestamps: request.timestamps.clone(),
            since_seconds: request.since_seconds,
            since_time: request.since_time.clone(),
            limit_bytes: request.limit_bytes.map(|l| l as usize),
            previous: request.previous.clone(),
            insecure_skip_tls_verify_backend: false,
        };

        match crate::api_pod_subresources::logs::build_log_output_bytes(
            &log_path,
            &params,
            self.task_supervisor.as_ref(),
        )
        .await
        {
            Ok(content) => PodLogResponse {
                request_id: request.request_id,
                log_content: content.to_vec(),
                error: None,
                fin: true,
            },
            Err(e) => PodLogResponse {
                request_id: request.request_id,
                log_content: Vec::new(),
                error: Some(format!("{e:?}")),
                fin: true,
            },
        }
    }

    fn follow_logs(
        &self,
        request: PodLogRequest,
    ) -> Pin<Box<dyn Stream<Item = PodLogResponse> + Send>> {
        let request_id = request.request_id.clone();
        if request.previous.as_deref() == Some("true") {
            return Box::pin(futures::stream::once(async move {
                PodLogResponse {
                    request_id,
                    log_content: Vec::new(),
                    error: None,
                    fin: true,
                }
            }));
        }

        let namespace = request.namespace.clone();
        let pod_name = request.pod_name.clone();
        let pod_uid = request.pod_uid.clone();
        let container_name = request.container_name.clone();
        let log_path = self.log_path(&request);
        let params = crate::api_pod_subresources::logs::LogQuery {
            container: Some(request.container_name.clone()),
            follow: request.follow.clone(),
            tail_lines: request
                .tail_lines
                .as_deref()
                .and_then(|s| s.parse::<usize>().ok()),
            timestamps: request.timestamps.clone(),
            since_time: request.since_time.clone(),
            since_seconds: request.since_seconds,
            limit_bytes: request
                .limit_bytes
                .and_then(|limit| usize::try_from(limit).ok()),
            previous: request.previous.clone(),
            insecure_skip_tls_verify_backend: false,
        };
        let byte_stream: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>> =
            if let Some(pod_event_db) = &self.pod_event_db {
                let termination = crate::api_pod_subresources::logs::PodLogFollowTermination::new(
                    pod_event_db.subscribe_watch(crate::watch::WatchTopic::new("v1", "Pod")),
                    namespace,
                    pod_name,
                    pod_uid,
                    container_name,
                    false,
                );
                Box::pin(
                    crate::api_pod_subresources::logs::follow_log_file_with_termination_watch(
                        log_path,
                        params,
                        self.task_supervisor.clone(),
                        termination,
                    ),
                )
            } else {
                Box::pin(
                    crate::api_pod_subresources::logs::follow_log_file_with_initial_query(
                        log_path,
                        params,
                        self.task_supervisor.clone(),
                    ),
                )
            };

        let stream = byte_stream.map(move |item| match item {
            Ok(log_content) => PodLogResponse {
                request_id: request_id.clone(),
                log_content: log_content.to_vec(),
                error: None,
                fin: false,
            },
            Err(err) => PodLogResponse {
                request_id: request_id.clone(),
                log_content: Vec::new(),
                error: Some(err.to_string()),
                fin: true,
            },
        });
        Box::pin(stream)
    }
}

#[async_trait]
impl NodeExecSyncHandler for CriNodeExecSyncHandler {
    async fn exec_sync(&self, request: NodeExecSyncRequest) -> NodeExecSyncResponse {
        let result = {
            let mut cri = self.cri.lock().await;
            crate::api_pod_subresources::exec_sync_with_created_state_retry(
                &mut cri,
                self.task_supervisor.as_ref(),
                &request.container_id,
                &request.command,
                request.timeout_seconds,
            )
            .await
        };
        match result {
            Ok(response) => NodeExecSyncResponse {
                request_id: request.request_id,
                stdout: response.stdout,
                stderr: response.stderr,
                exit_code: response.exit_code,
                error: None,
            },
            Err(err) => NodeExecSyncResponse {
                request_id: request.request_id,
                stdout: Vec::new(),
                stderr: Vec::new(),
                exit_code: 126,
                error: Some(err.to_string()),
            },
        }
    }
}

#[async_trait]
impl NodeExecStreamHandler for CriNodeExecSyncHandler {
    async fn exec_stream(
        &self,
        request: NodeExecRequest,
        input: mpsc::Receiver<NodeExecStreamFrame>,
        output: mpsc::Sender<NodeExecStreamFrame>,
    ) {
        if let Err(err) = run_cri_node_exec_stream(
            self.cri.clone(),
            self.task_supervisor.clone(),
            request.clone(),
            input,
            output.clone(),
        )
        .await
        {
            let _ = output
                .send(node_exec_error_frame(
                    request.request_id,
                    format!("remote node exec failed: {err:#}"),
                ))
                .await;
        }
    }
}

fn node_exec_error_frame(request_id: String, message: String) -> NodeExecStreamFrame {
    NodeExecStreamFrame {
        request_id,
        channel: ExecStreamChannel::Error,
        data: serde_json::json!({
            "metadata": {},
            "status": "Failure",
            "message": message,
        })
        .to_string()
        .into_bytes(),
        fin: true,
    }
}

async fn send_exec_frame(
    output: &mpsc::Sender<NodeExecStreamFrame>,
    frame: NodeExecStreamFrame,
) -> Result<()> {
    output
        .send(frame)
        .await
        .map_err(|_| anyhow!("node exec stream output receiver closed"))
}

async fn run_cri_node_exec_stream(
    cri: Arc<tokio::sync::Mutex<crate::kubelet::cri::CriClient>>,
    task_supervisor: Arc<TaskSupervisor>,
    request: NodeExecRequest,
    mut input: mpsc::Receiver<NodeExecStreamFrame>,
    output: mpsc::Sender<NodeExecStreamFrame>,
) -> Result<()> {
    use crate::spdy::{SpdyExec, SpdyFrame, StreamType};
    use tokio::io::AsyncWriteExt;

    tracing::debug!(
        request_id = %request.request_id,
        container = %request.container_id,
        command = ?request.command,
        stdin = request.stdin,
        stdout = request.stdout,
        stderr = request.stderr,
        tty = request.tty,
        "starting CRI node exec stream"
    );

    let streaming_url = {
        let mut cri_client = cri.lock().await;
        crate::api_pod_subresources::exec_with_created_state_retry(
            &mut cri_client,
            task_supervisor.as_ref(),
            crate::api_pod_subresources::ExecRequest {
                container_id: &request.container_id,
                command: &request.command,
                stream_options: crate::api_pod_subresources::ExecStreamOptions {
                    tty: request.tty,
                    stdin: request.stdin,
                    stdout: request.stdout,
                    stderr: request.stderr && !request.tty,
                },
            },
        )
        .await?
        .url
    };

    let mut containerd_stream = SpdyExec::connect_to_streaming_url(&streaming_url).await?;
    let mut containerd_spdy = SpdyExec::new();

    if request.stdin {
        containerd_spdy
            .write_syn_stream(&mut containerd_stream, 1, StreamType::Stdin)
            .await?;
    }
    if request.stdout {
        containerd_spdy
            .write_syn_stream(&mut containerd_stream, 3, StreamType::Stdout)
            .await?;
    }
    if request.stderr && !request.tty {
        containerd_spdy
            .write_syn_stream(&mut containerd_stream, 5, StreamType::Stderr)
            .await?;
    }
    containerd_spdy
        .write_syn_stream(&mut containerd_stream, 7, StreamType::Error)
        .await?;
    if request.tty {
        containerd_spdy
            .write_syn_stream(&mut containerd_stream, 9, StreamType::Resize)
            .await?;
        let initial_resize = serde_json::json!({"Width": 80, "Height": 24});
        containerd_spdy
            .write_data_frame(
                &mut containerd_stream,
                9,
                initial_resize.to_string().as_bytes(),
                false,
            )
            .await?;
    }

    let mut stdin_closed = !request.stdin;
    let mut input_closed = false;
    loop {
        tokio::select! {
            frame = input.recv(), if !input_closed && (!stdin_closed || request.tty) => {
                match frame {
                    Some(frame) => match frame.channel {
                        ExecStreamChannel::Stdin if request.stdin => {
                            tracing::debug!(
                                request_id = %request.request_id,
                                len = frame.data.len(),
                                fin = frame.fin,
                                "forwarding node exec stdin to containerd"
                            );
                            if !frame.data.is_empty() {
                                containerd_spdy
                                    .write_data_frame(&mut containerd_stream, 1, &frame.data, false)
                                    .await?;
                            }
                            if frame.fin {
                                containerd_spdy
                                    .write_data_frame(&mut containerd_stream, 1, &[], true)
                                    .await?;
                                stdin_closed = true;
                            }
                        }
                        ExecStreamChannel::Resize if request.tty => {
                            tracing::debug!(
                                request_id = %request.request_id,
                                len = frame.data.len(),
                                fin = frame.fin,
                                "forwarding node exec resize to containerd"
                            );
                            if !frame.data.is_empty() {
                                containerd_spdy
                                    .write_data_frame(&mut containerd_stream, 9, &frame.data, false)
                                    .await?;
                            }
                        }
                        _ => {}
                    },
                    None => {
                        if request.stdin && !stdin_closed {
                            let _ = containerd_spdy
                                .write_data_frame(&mut containerd_stream, 1, &[], true)
                                .await;
                        }
                        stdin_closed = true;
                        input_closed = true;
                    }
                }
            }
            frame = containerd_spdy.read_frame(&mut containerd_stream) => {
                match frame? {
                    SpdyFrame::Data { stream_id, data, fin } => {
                        let channel = match stream_id {
                            3 => Some(ExecStreamChannel::Stdout),
                            5 => Some(ExecStreamChannel::Stderr),
                            7 => Some(ExecStreamChannel::Error),
                            _ => None,
                        };
                        if let Some(channel) = channel {
                            let node_frame = NodeExecStreamFrame {
                                request_id: request.request_id.clone(),
                                channel,
                                data,
                                fin,
                            };
                            let is_terminal_error_frame =
                                crate::replication::protocol::node_exec_error_frame_is_terminal(
                                    &node_frame,
                                );
                            tracing::debug!(
                                request_id = %request.request_id,
                                channel = node_frame.channel.as_str(),
                                len = node_frame.data.len(),
                                fin,
                                "forwarding containerd exec frame to leader"
                            );
                            send_exec_frame(&output, node_frame).await?;
                            if is_terminal_error_frame {
                                return Ok(());
                            }
                        }
                    }
                    SpdyFrame::SynReply { .. } => {}
                    SpdyFrame::Ping { id } => {
                        containerd_spdy.write_ping(&mut containerd_stream, id).await?;
                    }
                    SpdyFrame::RstStream { .. } | SpdyFrame::GoAway => break,
                    SpdyFrame::Settings | SpdyFrame::WindowUpdate { .. } | SpdyFrame::Unknown | SpdyFrame::SynStream { .. } => {}
                }
            }
        }
        let _ = containerd_stream.flush().await;
    }

    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinDataplaneMetadata {
    pub public_key: Option<String>,
    pub endpoint: String,
    pub port: Option<u16>,
    pub mode: DataplaneMode,
    pub encryption: DataplaneEncryption,
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

impl GrpcClientConfig {
    fn leader_tls_verification(&self) -> LeaderTlsVerification {
        LeaderTlsVerificationPolicy::new(self.ca_cert_path.clone(), self.skip_ca).verification()
    }
}

#[derive(Clone)]
pub struct ReplicationGrpcClient {
    config: Arc<GrpcClientConfig>,
    supervisor: Arc<TaskSupervisor>,
    stream: Arc<Mutex<Option<OpenConnectStream>>>,
    join_response: Arc<Mutex<Option<JoinResponse>>>,
    node_exec_sync_handler: NodeExecSyncHandlerSlot,
    node_exec_stream_handler: NodeExecStreamHandlerSlot,
    pod_log_handler: PodLogHandlerSlot,
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
    /// Raft consensus RPCs (control-plane only).
    Raft,
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
    sender: mpsc::Sender<generated::FollowerMessage>,
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
            if let Ok(peer_addr) = stream.peer_addr()
                && let Ok(mut guard) = observed_peer_ip.lock()
            {
                *guard = Some(peer_addr.ip().to_string());
            }
            Ok(TokioIo::new(stream))
        })
    }
}

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
            node_exec_sync_handler: Arc::new(Mutex::new(None)),
            node_exec_stream_handler: Arc::new(Mutex::new(None)),
            pod_log_handler: Arc::new(Mutex::new(None)),
            all_leader_endpoints: Arc::new(std::sync::Mutex::new(Vec::new())),
            endpoint_index: Arc::new(std::sync::Mutex::new(0)),
            current_endpoint_override: Arc::new(std::sync::Mutex::new(None)),
            observed_leader_endpoint: Arc::new(std::sync::Mutex::new(None)),
            channel_pools: Arc::new(Mutex::new(std::collections::HashMap::new())),
            channel_build_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
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

    /// bug-grpc: number of real channel builds (TLS handshakes) so far.
    /// Test seam asserting unary RPCs reuse a cached channel.
    #[cfg(test)]
    pub fn channel_build_count(&self) -> u64 {
        self.channel_build_count
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

    pub async fn set_node_exec_sync_handler(&self, handler: Arc<dyn NodeExecSyncHandler>) {
        *self.node_exec_sync_handler.lock().await = Some(handler);
    }

    pub async fn set_node_exec_stream_handler(&self, handler: Arc<dyn NodeExecStreamHandler>) {
        *self.node_exec_stream_handler.lock().await = Some(handler);
    }

    pub async fn set_pod_log_handler(&self, handler: Arc<dyn PodLogHandler>) {
        *self.pod_log_handler.lock().await = Some(handler);
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
                        .get_metadata(generated::MetadataRequest {})
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
        })
    }

    pub async fn cluster_membership(
        &self,
    ) -> Result<crate::control_plane::client::membership::ClusterMembership> {
        let response = self
            .unary_call(
                "grpc_get_cluster_membership",
                ChannelLane::Read,
                |mut client| async move {
                    client
                        .get_cluster_membership(generated::ClusterMembershipRequest {})
                        .await
                        .map(|r| r.into_inner())
                },
            )
            .await
            .map_err(|err| err.into_anyhow("gRPC GetClusterMembership failed"))?;
        Ok(
            crate::control_plane::client::membership::ClusterMembership {
                cluster_id: response.cluster_id,
                voters: response.voters,
                term: response.term,
                leader_hint: (!response.leader_hint.is_empty()).then_some(response.leader_hint),
            },
        )
    }

    pub async fn snapshot(
        &self,
        last_applied_rv: i64,
    ) -> Result<Vec<crate::log_apply::LogApplyCommit>> {
        // bug-grpc: snapshot is a large streaming read — keep it on the
        // Stream lane so it cannot HOL-block hot Status/Read unary RPCs.
        let mut client = self.tonic_client_lane(ChannelLane::Stream).await?;
        let mut request = tonic::Request::new(generated::SnapshotRequest { last_applied_rv });
        self.add_join_token(&mut request)?;
        let mut stream = client
            .snapshot(request)
            .await
            .context("gRPC Snapshot failed")?
            .into_inner();
        let mut entries = Vec::new();
        while let Some(entry) = stream.message().await.context("read snapshot entry")? {
            entries.push(log_apply_commit_from_proto(entry)?);
        }
        Ok(entries)
    }

    pub async fn get_resource_rpc(&self, key: ResourceKey) -> Result<Option<Resource>> {
        let request = generated::GetResourceRequest {
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
            .map_err(|err| err.into_anyhow("gRPC GetResource failed"))?;
        response
            .resource
            .map(resource_from_proto)
            .transpose()
            .map(|resource| if response.found { resource } else { None })
    }

    pub async fn list_resources_rpc(&self, req: ListRequest) -> Result<ListResponse> {
        let request = generated::ListResourcesRequest {
            api_version: req.api_version,
            kind: req.kind,
            namespace: req.namespace,
            label_selector: req.label_selector,
            field_selector: req.field_selector,
            limit: req.limit,
            continue_token: req.continue_token,
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
            .map_err(|err| err.into_anyhow("gRPC ListResources failed"))?;
        let items = response
            .items
            .into_iter()
            .map(resource_from_proto)
            .collect::<Result<Vec<_>>>()?;
        Ok(ListResponse {
            items,
            resource_version: response.resource_version,
            continue_token: response.continue_token,
            remaining_item_count: response.remaining_item_count,
        })
    }

    pub async fn watch_resources_rpc(
        &self,
        req: WatchRequest,
    ) -> Result<WatchStream<ResourceEvent>> {
        let mut client = self.tonic_client().await?;
        let mut request = tonic::Request::new(generated::WatchResourcesRequest {
            api_version: req.api_version,
            kind: req.kind,
            namespace: req.namespace,
            field_selector: req.field_selector,
            start_resource_version: req.start_resource_version.unwrap_or(0),
            label_selector: req.label_selector,
        });
        self.add_join_token(&mut request)?;
        let stream = match client.watch_resources(request).await {
            Ok(stream) => stream,
            Err(status) => {
                // Self-heal: a leader restart wedges the Read-lane warm
                // pool. Evict it so the caller's reconnect loop rebuilds a
                // fresh channel on the next watch attempt.
                self.heal_lane_on_transport(ChannelLane::Read, &status)
                    .await;
                return Err(anyhow::Error::from(status).context("gRPC WatchResources failed"));
            }
        }
        .into_inner()
        .map(|event| {
            event
                // Preserve the tonic::Status as the error source (rather than
                // flattening it into a display string) so the worker reflector
                // can detect a replay-window expiration (Code::OutOfRange) and
                // relist, matching the K8s "too old resource version" contract.
                .map_err(|err| {
                    anyhow::Error::from(err).context("gRPC WatchResources stream failed")
                })
                .and_then(resource_event_from_proto)
        });
        Ok(Box::pin(stream))
    }

    pub async fn projected_service_account_token_rpc(
        &self,
        req: ProjectedServiceAccountTokenRequest,
    ) -> Result<ProjectedServiceAccountToken> {
        let request = generated::ProjectedServiceAccountTokenRequest {
            namespace: req.namespace,
            service_account_name: req.service_account_name,
            audiences: req.audiences,
            expiration_seconds: req.expiration_seconds,
            bound_pod_name: req.bound_pod_name,
            bound_pod_uid: req.bound_pod_uid,
            bound_node_name: req.bound_node_name,
            bound_node_uid: req.bound_node_uid,
        };
        let response = self
            .unary_call(
                "grpc_projected_service_account_token",
                ChannelLane::Read,
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
            .map_err(|err| err.into_anyhow("gRPC ProjectedServiceAccountToken failed"))?;
        Ok(ProjectedServiceAccountToken {
            token: response.token,
        })
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

    pub async fn apply_outbox_rpc(
        &self,
        idempotency_key: &str,
        operation: OutboxOperation,
        payload: Bytes,
    ) -> std::result::Result<OutboxApplyResult, OutboxApplyError> {
        // bug-grpc A2: reimplemented on the generic `unary_call` executor.
        // Idempotency key + response decode stay here; the retry/deadline/
        // failover/lane-heal loop is shared.
        let idempotency_key = idempotency_key.to_string();
        let operation = operation.as_str().to_string();
        let payload = payload.to_vec();
        let authoring_node = self.node_name().to_string();
        let response = match self
            .unary_call(
                "grpc_apply_outbox",
                ChannelLane::Status,
                move |mut client| {
                    let request = generated::ApplyOutboxRequest {
                        idempotency_key: idempotency_key.clone(),
                        operation: operation.clone(),
                        payload_proto: payload.clone(),
                        authoring_node: authoring_node.clone(),
                    };
                    async move { client.apply_outbox(request).await.map(|r| r.into_inner()) }
                },
            )
            .await
        {
            Ok(response) => response,
            Err(UnaryRpcError::Retryable(message)) => {
                return Err(OutboxApplyError::Retryable(message));
            }
            Err(UnaryRpcError::Status(status)) => return Err(outbox_error_from_status(status)),
        };
        if let Some(error) = response.error {
            return Err(outbox_error_from_response(
                response.error_type.as_deref(),
                error,
            ));
        }
        if response.already_applied {
            Ok(OutboxApplyResult::AlreadyApplied {
                applied_rv: Some(response.applied_rv),
            })
        } else {
            Ok(OutboxApplyResult::Applied {
                applied_rv: response.applied_rv,
            })
        }
    }

    /// P3-11c: opaque envelope dispatch for the three Raft consensus
    /// RPCs. The payload bytes are the serde-encoded openraft RPC; the
    /// response is either the serde-encoded openraft response (Ok arm)
    /// or a server-side error message (Error arm). Used by
    /// `ReplicationGrpcRaftRpcClient` to implement
    /// `datastore::raft::grpc_network::GrpcRaftRpcClient`.
    pub async fn raft_append_entries_rpc(
        &self,
        payload: Vec<u8>,
    ) -> Result<std::result::Result<Vec<u8>, String>> {
        let mut client = self.tonic_client_lane(ChannelLane::Raft).await?;
        let mut request = tonic::Request::new(generated::RaftAppendEntriesRequest { payload });
        self.add_join_token(&mut request)?;
        let response = client
            .raft_append_entries(request)
            .await
            .context("gRPC RaftAppendEntries failed")?
            .into_inner();
        Ok(match response.result {
            Some(generated::raft_append_entries_response::Result::Ok(bytes)) => Ok(bytes),
            Some(generated::raft_append_entries_response::Result::Error(msg)) => Err(msg),
            None => Err("server returned empty RaftAppendEntriesResponse result".to_string()),
        })
    }

    pub async fn raft_vote_rpc(
        &self,
        payload: Vec<u8>,
    ) -> Result<std::result::Result<Vec<u8>, String>> {
        let mut client = self.tonic_client_lane(ChannelLane::Raft).await?;
        let mut request = tonic::Request::new(generated::RaftVoteRequest { payload });
        self.add_join_token(&mut request)?;
        let response = client
            .raft_vote(request)
            .await
            .context("gRPC RaftVote failed")?
            .into_inner();
        Ok(match response.result {
            Some(generated::raft_vote_response::Result::Ok(bytes)) => Ok(bytes),
            Some(generated::raft_vote_response::Result::Error(msg)) => Err(msg),
            None => Err("server returned empty RaftVoteResponse result".to_string()),
        })
    }

    pub async fn raft_install_snapshot_rpc(
        &self,
        payload: Vec<u8>,
    ) -> Result<std::result::Result<Vec<u8>, String>> {
        let mut client = self.tonic_client_lane(ChannelLane::Raft).await?;
        let mut request = tonic::Request::new(generated::RaftInstallSnapshotRequest { payload });
        self.add_join_token(&mut request)?;
        let response = client
            .raft_install_snapshot(request)
            .await
            .context("gRPC RaftInstallSnapshot failed")?
            .into_inner();
        Ok(match response.result {
            Some(generated::raft_install_snapshot_response::Result::Ok(bytes)) => Ok(bytes),
            Some(generated::raft_install_snapshot_response::Result::Error(msg)) => Err(msg),
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
        node_name: &str,
        as_learner: bool,
        node_internal_ip: &str,
    ) -> Result<crate::replication::grpc::raft_rpc::ControlplaneJoinOutcome> {
        use crate::replication::grpc::raft_rpc::ControlplaneJoinOutcome;
        let request = generated::JoinAsControlplaneRequest {
            node_id,
            addr: addr.to_string(),
            node_name: node_name.to_string(),
            as_learner,
            dataplane_public_key: self.config.dataplane.public_key.clone().unwrap_or_default(),
            dataplane_endpoint: self.config.dataplane.endpoint.clone(),
            dataplane_port: self.config.dataplane.port.unwrap_or_default() as u32,
            dataplane_mode: self.config.dataplane.mode.as_str().to_string(),
            dataplane_encryption: self.config.dataplane.encryption.as_str().to_string(),
            node_internal_ip: node_internal_ip.to_string(),
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
            Some(generated::join_as_controlplane_response::Result::Accepted(accepted)) => {
                let ca_key_nonce: [u8; 12] = accepted.ca_key_nonce.try_into().unwrap_or([0u8; 12]);
                ControlplaneJoinOutcome::Accepted {
                    voter_count_after: accepted.voter_count_after,
                    admitted_as_learner: accepted.admitted_as_learner,
                    ca_cert_pem: accepted.ca_cert_pem,
                    encrypted_ca_key: accepted.encrypted_ca_key,
                    ca_key_nonce,
                }
            }
            Some(generated::join_as_controlplane_response::Result::RedirectToLeader(r)) => {
                ControlplaneJoinOutcome::RedirectToLeader {
                    leader_id: r.leader_id,
                    leader_addr: r.leader_addr,
                }
            }
            Some(generated::join_as_controlplane_response::Result::Denied(d)) => {
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
        let request = generated::SignControlplaneCsrRequest {
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
        // bug-grpc A2: Status-lane unary RPC — the same lossy-link wedge as
        // apply_outbox, now bounded by the shared executor's per-call deadline
        // and lane self-heal.
        let node_name = self.node_name().to_string();
        let renew_time = renew_time.to_string();
        self.unary_call(
            "grpc_renew_node_lease",
            ChannelLane::Status,
            move |mut client| {
                let request = generated::RenewNodeLeaseRequest {
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
        .map_err(|err| err.into_anyhow("gRPC RenewNodeLease failed"))
    }

    pub async fn allocate_node_subnet_rpc(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<NodeSubnet> {
        let request = generated::AllocateNodeSubnetRequest {
            node_name: node_name.to_string(),
            cluster_cidr: cluster_cidr.to_string(),
            node_ip: node_ip.to_string(),
        };
        let response = self
            .unary_call(
                "grpc_allocate_node_subnet",
                ChannelLane::Read,
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
            .map_err(|err| err.into_anyhow("gRPC AllocateNodeSubnet failed"))?;
        let subnet = response
            .subnet
            .ok_or_else(|| anyhow!("AllocateNodeSubnet response missing subnet"))?;
        node_subnet_from_proto(subnet)
    }

    pub async fn get_node_subnet_rpc(&self, node_name: &str) -> Result<Option<NodeSubnet>> {
        let request = generated::GetNodeSubnetRequest {
            node_name: node_name.to_string(),
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
            .map_err(|err| err.into_anyhow("gRPC GetNodeSubnet failed"))?;
        response
            .subnet
            .map(node_subnet_from_proto)
            .transpose()
            .map(|subnet| if response.found { subnet } else { None })
    }

    pub async fn list_peer_subnets_rpc(&self, my_node_name: &str) -> Result<Vec<NodeSubnet>> {
        let request = generated::ListPeerSubnetsRequest {
            my_node_name: my_node_name.to_string(),
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
            .map_err(|err| err.into_anyhow("gRPC ListPeerSubnets failed"))?;
        response
            .items
            .into_iter()
            .map(node_subnet_from_proto)
            .collect()
    }

    pub async fn get_node_dataplane_rpc(
        &self,
        node_name: &str,
    ) -> Result<Option<DataplanePeerMetadata>> {
        let request = generated::GetNodeDataplaneRequest {
            node_name: node_name.to_string(),
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
            .map_err(|err| err.into_anyhow("gRPC GetNodeDataplane failed"))?;
        response
            .metadata
            .map(dataplane_metadata_from_proto)
            .transpose()
            .map(|metadata| if response.found { metadata } else { None })
    }

    pub async fn observe_peer_endpoint_rpc(&self, node_name: &str) -> Result<Option<String>> {
        let request = generated::ObservePeerEndpointRequest {
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
        node_name: &str,
    ) -> Result<Vec<PodCleanupIntent>> {
        let request = generated::ListPodCleanupIntentsForNodeRequest {
            node_name: node_name.to_string(),
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
            .map_err(|err| err.into_anyhow("gRPC ListPodCleanupIntentsForNode failed"))?;
        response
            .items
            .into_iter()
            .map(pod_cleanup_intent_from_proto)
            .collect()
    }

    pub async fn delete_pod_cleanup_intent_rpc(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        let request = generated::DeletePodCleanupIntentRequest {
            node_name: node_name.to_string(),
            namespace: namespace.to_string(),
            pod_name: pod_name.to_string(),
            pod_uid: pod_uid.to_string(),
            reason: reason.to_string(),
        };
        self.unary_call(
            "grpc_delete_pod_cleanup_intent",
            ChannelLane::Read,
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
        .map_err(|err| err.into_anyhow("gRPC DeletePodCleanupIntent failed"))
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
            .send(generated::FollowerMessage {
                payload: Some(generated::follower_message::Payload::Ack(
                    generated::StreamAck { applied_rv },
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
            .send(generated::FollowerMessage {
                payload: Some(generated::follower_message::Payload::Join(
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
                .send(generated::FollowerMessage {
                    payload: Some(
                        generated::follower_message::Payload::ObservedLeaderEndpoint(
                            generated::ObservedLeaderEndpoint { endpoint },
                        ),
                    ),
                })
                .await
                .map_err(|_| anyhow!("failed to queue observed leader endpoint"))?;
        }
        let (stream_tx, stream_rx) = mpsc::channel(STREAM_ITEM_CHANNEL_CAPACITY);
        let dispatch_context = ConnectDispatchContext {
            supervisor: self.supervisor.clone(),
            node_exec_sync_handler: self.node_exec_sync_handler.clone(),
            node_exec_stream_handler: self.node_exec_stream_handler.clone(),
            node_exec_inputs: Arc::new(Mutex::new(std::collections::HashMap::new())),
            pod_log_handler: self.pod_log_handler.clone(),
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
    ) -> Result<(mpsc::Sender<generated::FollowerMessage>, StreamItemQueue)> {
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

    /// Read-lane tonic client — the default for cold/control-plane and
    /// read RPCs.
    async fn tonic_client(&self) -> Result<TonicClient<Channel>> {
        self.tonic_client_lane(ChannelLane::Read).await
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

            match self.config.leader_tls_verification() {
                LeaderTlsVerification::SkipCa => {
                    tracing::warn!(
                        leader_endpoint = %endpoint,
                        "skipping TLS CA verification for leader bootstrap connection"
                    );
                    builder =
                        builder.tls_config_with_verifier(tls, SkipCaServerCertVerifier::new())?;
                }
                LeaderTlsVerification::CaFile(path) => {
                    let ca_pem = read_ca_pem(path, self.supervisor.as_ref()).await?;
                    tls = tls.ca_certificate(Certificate::from_pem(ca_pem));
                    builder = builder.tls_config(tls)?;
                }
                LeaderTlsVerification::SystemRoots => {
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

    fn join_request(&self) -> generated::JoinRequest {
        generated::JoinRequest {
            token: String::new(),
            node_name: self.config.node_name.clone(),
            role: match self.config.role {
                JoinRole::Worker => generated::JoinRole::Worker as i32,
            },
            dataplane_public_key: self.config.dataplane.public_key.clone().unwrap_or_default(),
            dataplane_endpoint: self.config.dataplane.endpoint.clone(),
            dataplane_port: self.config.dataplane.port.map(u32::from).unwrap_or(0),
            dataplane_mode: self.config.dataplane.mode.as_str().to_string(),
            dataplane_encryption: self.config.dataplane.encryption.as_str().to_string(),
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
    mut inbound: tonic::codec::Streaming<generated::LeaderMessage>,
    outbound: mpsc::Sender<generated::FollowerMessage>,
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

    let _ = stream_tx.send(Err(terminal_error)).await;
}

async fn dispatch_leader_message(
    message: generated::LeaderMessage,
    outbound: &mpsc::Sender<generated::FollowerMessage>,
    stream_tx: &mpsc::Sender<Result<StreamItem>>,
    context: &ConnectDispatchContext,
) -> Result<()> {
    match message.payload {
        Some(generated::leader_message::Payload::StreamItem(item)) => {
            let item = stream_item_from_proto(item)?;
            stream_tx
                .send(Ok(item))
                .await
                .map_err(|_| anyhow!("stream item receiver closed"))?;
        }
        // T6: legacy ForwardResponse payload removed.
        Some(generated::leader_message::Payload::NodeExecSyncRequest(request)) => {
            let response =
                handle_node_exec_sync_request(request, &context.node_exec_sync_handler).await;
            outbound
                .send(generated::FollowerMessage {
                    payload: Some(generated::follower_message::Payload::NodeExecSyncResponse(
                        response,
                    )),
                })
                .await
                .map_err(|_| anyhow!("replication stream closed before node exec response send"))?;
        }
        Some(generated::leader_message::Payload::NodeExecRequest(request)) => {
            handle_node_exec_stream_request(
                request,
                outbound,
                context.supervisor.clone(),
                &context.node_exec_stream_handler,
                &context.node_exec_inputs,
            )
            .await?;
        }
        Some(generated::leader_message::Payload::NodeExecStreamFrame(frame)) => {
            let frame = node_exec_stream_frame_from_proto(frame)?;
            tracing::debug!(
                request_id = %frame.request_id,
                channel = frame.channel.as_str(),
                len = frame.data.len(),
                fin = frame.fin,
                "received node exec stream input frame from leader"
            );
            let route = {
                let routes = context.node_exec_inputs.lock().await;
                routes.get(&frame.request_id).cloned()
            };
            let Some(route) = route else {
                tracing::warn!(
                    request_id = %frame.request_id,
                    "dropped node exec stream input frame for inactive stream"
                );
                return Ok(());
            };
            route
                .send(frame)
                .await
                .map_err(|_| anyhow!("node exec stream input receiver closed"))?;
        }
        Some(generated::leader_message::Payload::PodLogRequest(request)) => {
            if request.follow.as_deref() == Some("true") {
                handle_pod_log_follow_request(
                    request,
                    &context.pod_log_handler,
                    outbound.clone(),
                    context.supervisor.clone(),
                )
                .await?;
            } else {
                let response = handle_pod_log_request(request, &context.pod_log_handler).await;
                outbound
                    .send(generated::FollowerMessage {
                        payload: Some(generated::follower_message::Payload::PodLogResponse(
                            response,
                        )),
                    })
                    .await
                    .map_err(|_| {
                        anyhow!("replication stream closed before pod log response send")
                    })?;
            }
        }
        Some(generated::leader_message::Payload::ObserveLeaderEndpointRequest(_)) => {
            if let Some(endpoint) = context.observed_leader_endpoint.as_deref() {
                outbound
                    .send(generated::FollowerMessage {
                        payload: Some(
                            generated::follower_message::Payload::ObservedLeaderEndpoint(
                                generated::ObservedLeaderEndpoint {
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
        Some(generated::leader_message::Payload::JoinResponse(response)) => {
            if let Some(generated::join_response::Result::Rejected(rejected)) = response.result {
                return Err(anyhow!("join rejected: {}", rejected.reason));
            }
        }
        None => return Err(anyhow!("empty LeaderMessage")),
    }
    Ok(())
}

async fn handle_node_exec_sync_request(
    request: generated::NodeExecSyncRequest,
    handler: &NodeExecSyncHandlerSlot,
) -> generated::NodeExecSyncResponse {
    let request = node_exec_sync_request_from_proto(request);
    let Some(handler) = handler.lock().await.clone() else {
        return generated::NodeExecSyncResponse {
            request_id: request.request_id,
            stdout: Vec::new(),
            stderr: Vec::new(),
            exit_code: 126,
            error: Some("node exec handler is not available".to_string()),
        };
    };
    node_exec_sync_response_to_proto(handler.exec_sync(request).await)
}

async fn handle_node_exec_stream_request(
    request: generated::NodeExecRequest,
    outbound: &mpsc::Sender<generated::FollowerMessage>,
    supervisor: Arc<TaskSupervisor>,
    handler: &NodeExecStreamHandlerSlot,
    node_exec_inputs: &NodeExecInputRoutes,
) -> Result<()> {
    let request = node_exec_request_from_proto(request);
    let Some(handler) = handler.lock().await.clone() else {
        send_node_exec_frame_to_leader(
            outbound,
            node_exec_error_frame(
                request.request_id,
                "node exec stream handler is not available".to_string(),
            ),
        )
        .await?;
        return Ok(());
    };

    let (input_tx, input_rx) = mpsc::channel(NODE_EXEC_STREAM_FRAME_CHANNEL_CAPACITY);
    {
        let mut routes = node_exec_inputs.lock().await;
        routes.insert(request.request_id.clone(), input_tx);
    }

    let request_id = request.request_id.clone();
    let task_request_id = request_id.clone();
    let output = outbound.clone();
    let routes = node_exec_inputs.clone();
    tracing::debug!(
        request_id = %request_id,
        stdin = request.stdin,
        stdout = request.stdout,
        stderr = request.stderr,
        tty = request.tty,
        "registered node exec stream input route"
    );
    if let Err(err) = supervisor
        .spawn_async(
            TaskCategory::Network,
            "grpc_node_exec_stream_handler",
            async move {
                let (output_tx, mut output_rx) =
                    mpsc::channel(NODE_EXEC_STREAM_FRAME_CHANNEL_CAPACITY);
                let handler_task = handler.exec_stream(request, input_rx, output_tx);
                tokio::pin!(handler_task);
                loop {
                    tokio::select! {
                        _ = &mut handler_task => {
                            while let Some(frame) = output_rx.recv().await {
                                if send_node_exec_frame_to_leader(&output, frame).await.is_err() {
                                    break;
                                }
                            }
                            break;
                        }
                        frame = output_rx.recv() => {
                            let Some(frame) = frame else {
                                break;
                            };
                            if send_node_exec_frame_to_leader(&output, frame).await.is_err() {
                                break;
                            }
                        }
                    }
                }
                routes.lock().await.remove(&task_request_id);
            },
        )
        .await
    {
        node_exec_inputs.lock().await.remove(&request_id);
        return Err(err);
    }
    Ok(())
}

async fn send_node_exec_frame_to_leader(
    outbound: &mpsc::Sender<generated::FollowerMessage>,
    frame: NodeExecStreamFrame,
) -> Result<()> {
    outbound
        .send(generated::FollowerMessage {
            payload: Some(generated::follower_message::Payload::NodeExecStreamFrame(
                node_exec_stream_frame_to_proto(frame),
            )),
        })
        .await
        .map_err(|_| anyhow!("replication stream closed before node exec stream frame send"))
}

async fn handle_pod_log_request(
    request: generated::PodLogRequest,
    handler: &PodLogHandlerSlot,
) -> generated::PodLogResponse {
    let request = pod_log_request_from_proto(request);
    let Some(handler) = handler.lock().await.clone() else {
        return generated::PodLogResponse {
            request_id: request.request_id,
            log_content: Vec::new(),
            error: Some("pod log handler is not available".to_string()),
            fin: true,
        };
    };
    pod_log_response_to_proto(handler.get_logs(request).await)
}

async fn handle_pod_log_follow_request(
    request: generated::PodLogRequest,
    handler: &PodLogHandlerSlot,
    outbound: mpsc::Sender<generated::FollowerMessage>,
    supervisor: Arc<TaskSupervisor>,
) -> Result<()> {
    let request = pod_log_request_from_proto(request);
    let request_id = request.request_id.clone();
    let Some(handler) = handler.lock().await.clone() else {
        outbound
            .send(generated::FollowerMessage {
                payload: Some(generated::follower_message::Payload::PodLogResponse(
                    generated::PodLogResponse {
                        request_id,
                        log_content: Vec::new(),
                        error: Some("pod log handler is not available".to_string()),
                        fin: true,
                    },
                )),
            })
            .await
            .map_err(|_| anyhow!("replication stream closed before pod log response send"))?;
        return Ok(());
    };

    supervisor
        .spawn_async(
            TaskCategory::Network,
            "grpc_pod_log_follow_stream",
            async move {
                let mut stream = handler.follow_logs(request);
                while let Some(response) = stream.next().await {
                    let terminal = response.fin || response.error.is_some();
                    if outbound
                        .send(generated::FollowerMessage {
                            payload: Some(generated::follower_message::Payload::PodLogResponse(
                                pod_log_response_to_proto(response),
                            )),
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                    if terminal {
                        return;
                    }
                }

                let _ = outbound
                    .send(generated::FollowerMessage {
                        payload: Some(generated::follower_message::Payload::PodLogResponse(
                            generated::PodLogResponse {
                                request_id,
                                log_content: Vec::new(),
                                error: None,
                                fin: true,
                            },
                        )),
                    })
                    .await;
            },
        )
        .await?;
    Ok(())
}

// `impl CommandForwarder for ReplicationGrpcClient` removed in T6 along
// with the trait itself. Workers now use ApplyOutbox via the new
// LeaderApiClient surface.

fn resource_from_proto(resource: generated::ResourceObject) -> Result<Resource> {
    let data: serde_json::Value =
        serde_json::from_slice(&resource.data_json).with_context(|| {
            format!(
                "decode {} {} resource JSON",
                resource.api_version, resource.kind
            )
        })?;
    Ok(Resource {
        id: 0,
        api_version: resource.api_version,
        kind: resource.kind,
        namespace: resource.namespace,
        name: resource.name,
        uid: resource.uid,
        resource_version: resource.resource_version,
        data: Arc::new(data),
    })
}

fn resource_event_from_proto(event: generated::WatchEvent) -> Result<ResourceEvent> {
    let resource = event
        .resource
        .ok_or_else(|| anyhow!("WatchResources event missing resource"))?;
    let resource = resource_from_proto(resource)?;
    Ok(ResourceEvent {
        event: crate::watch::WatchEvent::from_type(&event.event_type, (*resource.data).clone()),
    })
}

fn node_subnet_from_proto(subnet: generated::NodeSubnetObject) -> Result<NodeSubnet> {
    crate::replication::protocol::ForwardedNodeSubnet {
        node_name: subnet.node_name,
        subnet: subnet.subnet,
        subnet_base_int: subnet.subnet_base_int,
        vtep_ip: subnet.vtep_ip,
        vtep_mac: subnet.vtep_mac,
        node_ip: subnet.node_ip,
        mode: subnet.mode,
        hostport_range: subnet.hostport_range,
    }
    .into_node_subnet()
}

fn dataplane_metadata_from_proto(
    metadata: generated::DataplaneMetadataObject,
) -> Result<DataplanePeerMetadata> {
    let port = metadata
        .port
        .map(u16::try_from)
        .transpose()
        .map_err(|_| anyhow!("dataplane metadata port exceeds u16"))?;
    DataplanePeerMetadata::try_new(
        metadata.node_name,
        DataplaneMode::parse(&metadata.mode)?,
        DataplaneEncryption::parse(Some(&metadata.encryption))?,
        metadata.public_key,
        Some(metadata.endpoint),
        port,
    )
}

fn pod_cleanup_intent_from_proto(
    intent: generated::PodCleanupIntentObject,
) -> Result<PodCleanupIntent> {
    let pod_data = serde_json::from_slice(&intent.pod_data_json).with_context(|| {
        format!(
            "decode pod cleanup intent JSON for {}/{} uid={}",
            intent.namespace, intent.pod_name, intent.pod_uid
        )
    })?;
    Ok(PodCleanupIntent {
        node_name: intent.node_name,
        namespace: intent.namespace,
        pod_name: intent.pod_name,
        pod_uid: intent.pod_uid,
        reason: intent.reason,
        resource_version: intent.resource_version,
        created_at_ms: intent.created_at_ms,
        pod_data,
    })
}

fn outbox_error_from_response(error_type: Option<&str>, message: String) -> OutboxApplyError {
    match error_type {
        Some("NotFound") => OutboxApplyError::NotFound(message),
        Some("UidMismatch") => OutboxApplyError::UidMismatch {
            expected: "<unknown>".to_string(),
            actual: "<unknown>".to_string(),
        },
        Some("ConflictTerminal") => OutboxApplyError::ConflictTerminal(message),
        Some("Retryable") | None => OutboxApplyError::Retryable(message),
        Some(_) => OutboxApplyError::Retryable(message),
    }
}

fn outbox_error_from_status(status: tonic::Status) -> OutboxApplyError {
    let message = status.message().to_string();
    match status.code() {
        tonic::Code::NotFound => OutboxApplyError::NotFound(message),
        tonic::Code::FailedPrecondition if is_not_raft_leader_status(&status) => {
            OutboxApplyError::Retryable(status.to_string())
        }
        tonic::Code::FailedPrecondition
            if message.to_ascii_lowercase().contains("uid mismatch") =>
        {
            OutboxApplyError::UidMismatch {
                expected: "<unknown>".to_string(),
                actual: "<unknown>".to_string(),
            }
        }
        tonic::Code::FailedPrecondition | tonic::Code::AlreadyExists | tonic::Code::Aborted => {
            OutboxApplyError::ConflictTerminal(message)
        }
        _ => OutboxApplyError::Retryable(status.to_string()),
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
        tonic::Code::Unavailable | tonic::Code::Unknown
    )
}

async fn read_ca_pem(path: PathBuf, supervisor: &TaskSupervisor) -> Result<Vec<u8>> {
    use std::fs as blocking_fs;

    let key = path.display().to_string();
    supervisor
        .run_blocking_file_keyed("grpc_client_read_ca_pem", key, move || {
            blocking_fs::read(path)
        })
        .await?
        .context("read gRPC CA certificate")
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

fn join_response_from_leader_message(message: generated::LeaderMessage) -> Result<JoinResponse> {
    match message.payload {
        Some(generated::leader_message::Payload::JoinResponse(response)) => match response.result {
            Some(generated::join_response::Result::Accepted(accepted)) => {
                Ok(JoinResponse::Accepted {
                    cluster_id: accepted.cluster_id,
                    leader_epoch: accepted.leader_epoch,
                    current_rv: accepted.current_rv,
                })
            }
            Some(generated::join_response::Result::Rejected(rejected)) => {
                Ok(JoinResponse::Rejected {
                    reason: rejected.reason,
                })
            }
            None => Err(anyhow!("empty JoinResponse")),
        },
        other => Err(anyhow!("expected JoinResponse, got {other:?}")),
    }
}

fn stream_item_from_proto(item: generated::StreamItem) -> Result<StreamItem> {
    match item.item {
        Some(generated::stream_item::Item::Entry(entry)) => {
            Ok(StreamItem::Entry(Box::new(entry_from_proto(entry)?)))
        }
        Some(generated::stream_item::Item::Heartbeat(heartbeat)) => Ok(StreamItem::Heartbeat {
            current_rv: heartbeat.current_rv,
        }),
        None => Err(anyhow!("empty StreamItem")),
    }
}

// `forwarded_write_from_response` removed in T6.

fn node_exec_sync_request_from_proto(
    request: generated::NodeExecSyncRequest,
) -> NodeExecSyncRequest {
    NodeExecSyncRequest {
        request_id: request.request_id,
        node_name: request.node_name,
        namespace: request.namespace,
        pod_name: request.pod_name,
        container_id: request.container_id,
        command: request.command,
        timeout_seconds: request.timeout_seconds,
    }
}

fn node_exec_sync_response_to_proto(
    response: NodeExecSyncResponse,
) -> generated::NodeExecSyncResponse {
    generated::NodeExecSyncResponse {
        request_id: response.request_id,
        stdout: response.stdout,
        stderr: response.stderr,
        exit_code: response.exit_code,
        error: response.error,
    }
}

fn node_exec_request_from_proto(request: generated::NodeExecRequest) -> NodeExecRequest {
    NodeExecRequest {
        request_id: request.request_id,
        node_name: request.node_name,
        namespace: request.namespace,
        pod_name: request.pod_name,
        container_id: request.container_id,
        command: request.command,
        tty: request.tty,
        stdin: request.stdin,
        stdout: request.stdout,
        stderr: request.stderr,
    }
}

fn node_exec_stream_frame_to_proto(frame: NodeExecStreamFrame) -> generated::NodeExecStreamFrame {
    generated::NodeExecStreamFrame {
        request_id: frame.request_id,
        channel: frame.channel.as_str().to_string(),
        data: frame.data,
        fin: frame.fin,
    }
}

fn node_exec_stream_frame_from_proto(
    frame: generated::NodeExecStreamFrame,
) -> Result<NodeExecStreamFrame> {
    let channel = ExecStreamChannel::parse(&frame.channel)
        .ok_or_else(|| anyhow!("unknown node exec stream channel '{}'", frame.channel))?;
    Ok(NodeExecStreamFrame {
        request_id: frame.request_id,
        channel,
        data: frame.data,
        fin: frame.fin,
    })
}

fn pod_log_request_from_proto(request: generated::PodLogRequest) -> PodLogRequest {
    PodLogRequest {
        request_id: request.request_id,
        node_name: request.node_name,
        namespace: request.namespace,
        pod_name: request.pod_name,
        pod_uid: request.pod_uid,
        container_name: request.container_name,
        follow: request.follow,
        tail_lines: request.tail_lines,
        timestamps: request.timestamps,
        since_time: request.since_time,
        since_seconds: request.since_seconds,
        limit_bytes: request.limit_bytes,
        previous: request.previous,
    }
}

fn pod_log_response_to_proto(response: PodLogResponse) -> generated::PodLogResponse {
    generated::PodLogResponse {
        request_id: response.request_id,
        log_content: response.log_content,
        error: response.error,
        fin: response.fin,
    }
}

// `forwarded_*_from_proto` helpers removed in T6 along with the
// ForwardedResource / ForwardedNodeSubnet / ForwardedPodSlotAdmission
// proto messages.

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
