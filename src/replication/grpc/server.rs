use anyhow::{Context, Result, anyhow};
use futures::stream::BoxStream;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status, metadata::MetadataMap};

use crate::controller_dispatcher::ControllerDispatcher;
use crate::datastore::backend::{DatastoreBackend, DatastoreHandle};
use crate::datastore::sqlite::DatastoreWatchReplaySource;
use crate::datastore::{ResourcePreconditions, WatchReplayPosition, WatchTarget};
use crate::kubelet::pod_repository::store::PodStore;
use crate::metrics::{
    NodeMetricsContainerSample, NodeMetricsPodSample, NodeMetricsRequest, NodeMetricsResponse,
};
use crate::networking::wireguard::{DataplaneEncryption, DataplaneMode, DataplanePeerMetadata};
use crate::replication::grpc::{
    JOIN_TOKEN_METADATA_KEY, entry_to_proto, generated, resource_command_request_from_proto,
    watch_replay_expired_status, watch_replay_position_from_proto, watch_replay_position_to_proto,
};
use crate::replication::protocol::{
    ExecStreamChannel, FollowerControlMessage, JoinResponse, JoinRole, NodeExecRequest,
    NodeExecStreamFrame, NodeExecSyncRequest, NodeExecSyncResponse, PodLogRequest, PodLogResponse,
};
use crate::replication::service::ReplicationService;
use crate::replication::snapshot::SnapshotCommitSink;
use crate::watch::WatchEventSelection;

use super::ca_files::ControlplaneCaFiles;

const MAX_NODE_LEASE_RENEW_TIME_SKEW_SECONDS: i64 = 100;

pub fn validate_join_metadata(join: &generated::JoinRequest) -> Result<DataplanePeerMetadata> {
    validate_join_metadata_with_endpoint(join, None)
}

fn observed_or_advertised_dataplane_endpoint(
    endpoint_override: Option<IpAddr>,
    advertised_endpoint: &str,
) -> Option<String> {
    endpoint_override
        .map(|ip| ip.to_string())
        .or_else(|| Some(advertised_endpoint.to_string()).filter(|value| !value.trim().is_empty()))
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
    join: &generated::JoinRequest,
    endpoint_override: Option<IpAddr>,
) -> Result<DataplanePeerMetadata> {
    let mode = DataplaneMode::parse(&join.dataplane_mode)?;
    let encryption = DataplaneEncryption::parse(Some(&join.dataplane_encryption))?;
    let port = dataplane_port_from_u32(join.dataplane_port)?;
    let endpoint =
        observed_or_advertised_dataplane_endpoint(endpoint_override, &join.dataplane_endpoint);
    DataplanePeerMetadata::try_new(
        join.node_name.clone(),
        mode,
        encryption,
        Some(join.dataplane_public_key.clone()).filter(|value| !value.trim().is_empty()),
        endpoint,
        port,
    )
}

fn validate_controlplane_join_dataplane_metadata_with_endpoint(
    join: &generated::JoinAsControlplaneRequest,
    endpoint_override: Option<IpAddr>,
) -> Result<DataplanePeerMetadata> {
    let mode = DataplaneMode::parse(&join.dataplane_mode)?;
    let encryption = DataplaneEncryption::parse(Some(&join.dataplane_encryption))?;
    let port = dataplane_port_from_u32(join.dataplane_port)?;
    let endpoint =
        observed_or_advertised_dataplane_endpoint(endpoint_override, &join.dataplane_endpoint);
    DataplanePeerMetadata::try_new(
        join.node_name.clone(),
        mode,
        encryption,
        Some(join.dataplane_public_key.clone()).filter(|value| !value.trim().is_empty()),
        endpoint,
        port,
    )
}

fn validate_controlplane_node_registration(
    registration: generated::NodeRegistrationSnapshot,
) -> Result<crate::replication::grpc::raft_rpc::RemoteNodeRegistrationSnapshot> {
    let node_mode =
        crate::controllers::annotations::parse_node_peer_mode(Some(&registration.node_mode))
            .map_err(|err| anyhow!(err))?;
    let host = crate::kubelet::node::NodeRegistrationHostFacts {
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
    Ok(crate::replication::grpc::raft_rpc::RemoteNodeRegistrationSnapshot { node_mode, host })
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
pub struct GrpcReplicationServer {
    service: Arc<ReplicationService>,
    db: DatastoreHandle,
    pod_store: Arc<PodStore>,
    controller_dispatcher: Option<Arc<ControllerDispatcher>>,
    node_lease_tracker: Arc<crate::node_lease_tracker::NodeLeaseTracker>,
    node_self_query: Option<Arc<dyn klights_leader_api::LeaderResourceQuery>>,
    node_self_status: Option<Arc<dyn klights_leader_api::LeaderNodeSelfStatus>>,
    node_lifecycle_status: Option<Arc<dyn klights_leader_api::LeaderNodeLifecycleStatus>>,
    /// Phase 3 raft RPC dispatcher. Populated by the leader bootstrap
    /// (P3-11c) when raft mode is wired. When None, the three Raft
    /// RPCs respond with `RaftRpcRouterError::Disabled` so the client
    /// side can translate it into `RPCError::Unreachable`.
    raft_rpc_router: Option<Arc<dyn crate::replication::grpc::raft_rpc::RaftRpcRouter>>,
    /// Phase 3 controlplane join handler. Populated alongside
    /// `raft_rpc_router` by the leader bootstrap. When None,
    /// `JoinAsControlplane` is denied with a fixed reason.
    controlplane_join_handler:
        Option<Arc<dyn crate::replication::grpc::raft_rpc::ControlplaneJoinHandler>>,
    /// Supervised reader for in-band CA distribution/signing material.
    controlplane_ca_files: ControlplaneCaFiles,
    /// Raft leadership gate for leader-owned worker RPCs. When present,
    /// follower controlplanes must reject writes/control streams instead of
    /// updating follower-local cluster state.
    is_leader_rx: Option<tokio::sync::watch::Receiver<bool>>,
    local_node_name: Option<String>,
    /// bug-grpc A1/B2: per-stream watch heartbeat cadence, from the shared
    /// `GrpcTransportPolicy`.
    watch_heartbeat_interval: Duration,
}

impl GrpcReplicationServer {
    fn from_parts(
        service: Arc<ReplicationService>,
        db: DatastoreHandle,
        controller_dispatcher: Option<Arc<ControllerDispatcher>>,
        node_lease_tracker: Arc<crate::node_lease_tracker::NodeLeaseTracker>,
    ) -> Self {
        let controlplane_ca_files = ControlplaneCaFiles::new(service.task_supervisor());
        let pod_store = Arc::new(PodStore::new(db.clone()));
        Self {
            service,
            db,
            pod_store,
            controller_dispatcher,
            node_lease_tracker,
            node_self_query: None,
            node_self_status: None,
            node_lifecycle_status: None,
            raft_rpc_router: None,
            controlplane_join_handler: None,
            controlplane_ca_files,
            is_leader_rx: None,
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

    pub fn new(service: Arc<ReplicationService>, db: DatastoreHandle) -> Self {
        Self::from_parts(
            service,
            db,
            None,
            Arc::new(crate::node_lease_tracker::NodeLeaseTracker::new()),
        )
    }

    pub fn new_with_controller_dispatcher(
        service: Arc<ReplicationService>,
        db: DatastoreHandle,
        controller_dispatcher: Arc<ControllerDispatcher>,
    ) -> Self {
        Self::from_parts(
            service,
            db,
            Some(controller_dispatcher),
            Arc::new(crate::node_lease_tracker::NodeLeaseTracker::new()),
        )
    }

    pub fn new_with_node_lease_tracker(
        service: Arc<ReplicationService>,
        db: DatastoreHandle,
        node_lease_tracker: Arc<crate::node_lease_tracker::NodeLeaseTracker>,
    ) -> Self {
        Self::from_parts(service, db, None, node_lease_tracker)
    }

    pub fn new_with_controller_dispatcher_and_node_lease_tracker(
        service: Arc<ReplicationService>,
        db: DatastoreHandle,
        controller_dispatcher: Arc<ControllerDispatcher>,
        node_lease_tracker: Arc<crate::node_lease_tracker::NodeLeaseTracker>,
    ) -> Self {
        Self::from_parts(service, db, Some(controller_dispatcher), node_lease_tracker)
    }

    /// P3-11b: attach a Raft RPC dispatcher so this server can handle
    /// `RaftAppendEntries` / `RaftVote` / `RaftInstallSnapshot` calls
    /// from peer voters. The dispatcher is provided by the leader
    /// bootstrap (P3-11c) when raft mode is wired.
    pub fn with_raft_rpc_router(
        mut self,
        router: Arc<dyn crate::replication::grpc::raft_rpc::RaftRpcRouter>,
    ) -> Self {
        self.raft_rpc_router = Some(router);
        self
    }

    /// P3-11c: attach a `ControlplaneJoinHandler` so this server can
    /// service `JoinAsControlplane` RPCs from peer voters that want to
    /// be added to the cluster via `RaftNode::add_voter`.
    pub fn with_controlplane_join_handler(
        mut self,
        handler: Arc<dyn crate::replication::grpc::raft_rpc::ControlplaneJoinHandler>,
    ) -> Self {
        self.controlplane_join_handler = Some(handler);
        self
    }

    /// Set the containerd namespace for locating CA cert/key files.
    pub fn with_namespace(mut self, namespace: &str) -> Self {
        self.controlplane_ca_files.set_namespace(namespace);
        self
    }

    async fn service_account_signing_key_pem(&self) -> std::result::Result<String, Status> {
        let namespace = self.controlplane_ca_files.containerd_namespace()?;
        let supervisor = self.service.task_supervisor();
        crate::auth::read_service_account_signing_key_supervised(namespace, supervisor.as_ref())
            .await
            .map_err(|err| {
                Status::failed_precondition(format!(
                    "ServiceAccount signing key not available: {err:#}"
                ))
            })
    }

    pub fn with_leader_gate(mut self, is_leader_rx: tokio::sync::watch::Receiver<bool>) -> Self {
        self.is_leader_rx = Some(is_leader_rx);
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
        if self.is_leader_rx.as_ref().is_some_and(|rx| !*rx.borrow()) {
            return Err(Status::failed_precondition("not raft leader"));
        }
        Ok(())
    }

    fn sample_raft_leadership(
        &self,
    ) -> std::result::Result<Option<tokio::sync::watch::Receiver<bool>>, Status> {
        let Some(mut leadership_rx) = self.is_leader_rx.clone() else {
            return Ok(None);
        };
        if !*leadership_rx.borrow_and_update() {
            return Err(Status::failed_precondition("not raft leader"));
        }
        Ok(Some(leadership_rx))
    }

    fn require_raft_leadership_unchanged(
        leadership_rx: Option<&tokio::sync::watch::Receiver<bool>>,
    ) -> std::result::Result<(), Status> {
        if leadership_rx.is_some_and(|rx| rx.has_changed().unwrap_or(true)) {
            return Err(Status::failed_precondition(
                "raft leadership changed during leader-fresh read",
            ));
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
            .get::<crate::auth::TlsClientCertificate>()
        else {
            return Err(Status::unauthenticated(format!(
                "{action} require a node client certificate"
            )));
        };
        let user = crate::auth::user_from_cert(&cert.0).map_err(|err| {
            Status::unauthenticated(format!("invalid control-plane node certificate: {err}"))
        })?;
        let identity = crate::auth::AuthenticatedIdentity::client_cert(user.username, user.groups);
        let _node_name = identity
            .username
            .strip_prefix("system:node:")
            .filter(|name| !name.is_empty())
            .filter(|_| {
                identity
                    .groups
                    .iter()
                    .any(|group| group == crate::auth::NODES_GROUP)
            })
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
            .any(|group| group == crate::auth::CONTROLPLANE_NODES_GROUP)
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
        crate::bootstrap::bootstrap_token::validate_bootstrap_token_for_scope(
            self.db.as_ref(),
            supplied,
            crate::bootstrap::bootstrap_token::BootstrapTokenScope::Controlplane,
        )
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
    ) -> std::result::Result<(), Status> {
        if node_client_identity(request)?.is_some() {
            Ok(())
        } else {
            Err(Status::unauthenticated(
                "steady-state replication RPC requires a node client certificate",
            ))
        }
    }
}

fn node_client_identity<T>(
    request: &Request<T>,
) -> std::result::Result<Option<crate::auth::AuthenticatedIdentity>, Status> {
    let Some(cert) = request
        .extensions()
        .get::<crate::auth::TlsClientCertificate>()
    else {
        return Ok(None);
    };
    let user = crate::auth::user_from_cert(&cert.0).map_err(|err| {
        Status::unauthenticated(format!("invalid node client certificate: {err}"))
    })?;
    let identity = crate::auth::AuthenticatedIdentity::client_cert(user.username, user.groups);
    validate_node_client_identity(&identity, None)?;
    Ok(Some(identity))
}

fn validate_node_client_identity(
    identity: &crate::auth::AuthenticatedIdentity,
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
fn caller_node_authority<T>(request: &Request<T>) -> CallerAuthority {
    let Some(cert) = request
        .extensions()
        .get::<crate::auth::TlsClientCertificate>()
    else {
        return CallerAuthority::Unrestricted;
    };
    let Ok(user) = crate::auth::user_from_cert(&cert.0) else {
        return CallerAuthority::Unrestricted;
    };
    let identity = crate::auth::AuthenticatedIdentity::client_cert(user.username, user.groups);
    let is_controlplane = identity
        .groups
        .iter()
        .any(|group| group == crate::auth::CONTROLPLANE_NODES_GROUP);
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

async fn consume_terminal_outbox_sequence_for_rpc(
    db: &dyn crate::datastore::DatastoreBackend,
    idempotency_key: &str,
    operation: crate::kubelet::outbox::payload::OutboxOperation,
    authenticated_node: &str,
    watermark: Option<crate::log_apply::OutboxStreamWatermark>,
) -> std::result::Result<(), Status> {
    crate::control_plane::client::apply::consume_terminal_outbox_sequence(
        db,
        idempotency_key,
        operation,
        authenticated_node,
        watermark,
    )
    .await
    .map_err(|error| {
        Status::unavailable(format!(
            "failed to durably consume terminal outbox sequence: {error}"
        ))
    })
}

fn apply_outbox_error_response(
    error: klights_leader_api::OutboxDeliveryError,
) -> Response<generated::ApplyOutboxResponse> {
    let error_type = match &error {
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
    Response::new(generated::ApplyOutboxResponse {
        already_applied: false,
        applied_rv: 0,
        error: Some(error.to_string()),
        error_type: Some(error_type.to_string()),
    })
}

pub fn mount_service(
    app: axum::Router,
    service: Arc<ReplicationService>,
    db: DatastoreHandle,
    transport_policy: Arc<crate::replication::grpc::transport_policy::GrpcTransportPolicy>,
) -> axum::Router {
    mount_service_with_controller_dispatcher(app, service, db, None, None, transport_policy)
}

pub fn mount_service_with_controller_dispatcher(
    app: axum::Router,
    service: Arc<ReplicationService>,
    db: DatastoreHandle,
    controller_dispatcher: Option<Arc<ControllerDispatcher>>,
    node_lease_tracker: Option<Arc<crate::node_lease_tracker::NodeLeaseTracker>>,
    transport_policy: Arc<crate::replication::grpc::transport_policy::GrpcTransportPolicy>,
) -> axum::Router {
    mount_service_full(
        app,
        service,
        db,
        controller_dispatcher,
        node_lease_tracker,
        None,
        None,
        "",
        None,
        None,
        None,
        None,
        None,
        transport_policy,
    )
}

/// P3-11c: full mount that also wires the Raft RPC dispatcher and the
/// `JoinAsControlplane` handler. Either may be `None`; when both are
/// None this is functionally equivalent to
/// `mount_service_with_controller_dispatcher`.
#[allow(clippy::too_many_arguments)]
pub fn mount_service_full(
    app: axum::Router,
    service: Arc<ReplicationService>,
    db: DatastoreHandle,
    controller_dispatcher: Option<Arc<ControllerDispatcher>>,
    node_lease_tracker: Option<Arc<crate::node_lease_tracker::NodeLeaseTracker>>,
    raft_rpc_router: Option<Arc<dyn crate::replication::grpc::raft_rpc::RaftRpcRouter>>,
    controlplane_join_handler: Option<
        Arc<dyn crate::replication::grpc::raft_rpc::ControlplaneJoinHandler>,
    >,
    containerd_namespace: &str,
    is_leader_rx: Option<tokio::sync::watch::Receiver<bool>>,
    local_node_name: Option<String>,
    node_self_query: Option<Arc<dyn klights_leader_api::LeaderResourceQuery>>,
    node_self_status: Option<Arc<dyn klights_leader_api::LeaderNodeSelfStatus>>,
    node_lifecycle_status: Option<Arc<dyn klights_leader_api::LeaderNodeLifecycleStatus>>,
    transport_policy: Arc<crate::replication::grpc::transport_policy::GrpcTransportPolicy>,
) -> axum::Router {
    mount_service_full_with_policy(
        app,
        service,
        db,
        controller_dispatcher,
        node_lease_tracker,
        raft_rpc_router,
        controlplane_join_handler,
        containerd_namespace,
        is_leader_rx,
        local_node_name,
        node_self_query,
        node_self_status,
        node_lifecycle_status,
        transport_policy,
    )
}

/// bug-grpc A1: `mount_service_full` taking the shared
/// [`GrpcTransportPolicy`]. The server applies the policy's
/// `max_message_bytes` to the tonic service's decode/encode limits (these
/// were previously **unset** server-side — an unbounded request could OOM
/// the leader). The over-limit rejection is exercised by
/// `server_rejects_request_over_policy_message_limit`.
#[allow(clippy::too_many_arguments)]
pub fn mount_service_full_with_policy(
    app: axum::Router,
    service: Arc<ReplicationService>,
    db: DatastoreHandle,
    controller_dispatcher: Option<Arc<ControllerDispatcher>>,
    node_lease_tracker: Option<Arc<crate::node_lease_tracker::NodeLeaseTracker>>,
    raft_rpc_router: Option<Arc<dyn crate::replication::grpc::raft_rpc::RaftRpcRouter>>,
    controlplane_join_handler: Option<
        Arc<dyn crate::replication::grpc::raft_rpc::ControlplaneJoinHandler>,
    >,
    containerd_namespace: &str,
    is_leader_rx: Option<tokio::sync::watch::Receiver<bool>>,
    local_node_name: Option<String>,
    node_self_query: Option<Arc<dyn klights_leader_api::LeaderResourceQuery>>,
    node_self_status: Option<Arc<dyn klights_leader_api::LeaderNodeSelfStatus>>,
    node_lifecycle_status: Option<Arc<dyn klights_leader_api::LeaderNodeLifecycleStatus>>,
    transport_policy: Arc<crate::replication::grpc::transport_policy::GrpcTransportPolicy>,
) -> axum::Router {
    let mut grpc = match (controller_dispatcher, node_lease_tracker) {
        (Some(controller_dispatcher), Some(node_lease_tracker)) => {
            GrpcReplicationServer::new_with_controller_dispatcher_and_node_lease_tracker(
                service,
                db,
                controller_dispatcher,
                node_lease_tracker,
            )
        }
        (Some(controller_dispatcher), None) => {
            GrpcReplicationServer::new_with_controller_dispatcher(
                service,
                db,
                controller_dispatcher,
            )
        }
        (None, Some(node_lease_tracker)) => {
            GrpcReplicationServer::new_with_node_lease_tracker(service, db, node_lease_tracker)
        }
        (None, None) => GrpcReplicationServer::new(service, db),
    };
    grpc = grpc
        .with_namespace(containerd_namespace)
        .with_watch_heartbeat_interval(transport_policy.watch_heartbeat_interval);
    if let Some(is_leader_rx) = is_leader_rx {
        grpc = grpc.with_leader_gate(is_leader_rx);
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
        .register_encoded_file_descriptor_set(crate::replication::grpc::FILE_DESCRIPTOR_SET)
        .build_v1()
        .expect("failed to build replication gRPC reflection service");
    let max_message_bytes = transport_policy.max_message_bytes;
    let grpc_router = tonic::service::Routes::new(
        generated::replication_server::ReplicationServer::new(grpc)
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

#[tonic::async_trait]
impl generated::replication_server::Replication for GrpcReplicationServer {
    type ConnectStream = BoxStream<'static, std::result::Result<generated::LeaderMessage, Status>>;
    type SnapshotStream =
        BoxStream<'static, std::result::Result<generated::ReplicationEntry, Status>>;
    type WatchResourcesStream =
        BoxStream<'static, std::result::Result<generated::WatchEvent, Status>>;

    async fn connect(
        &self,
        request: Request<tonic::Streaming<generated::FollowerMessage>>,
    ) -> std::result::Result<Response<Self::ConnectStream>, Status> {
        let remote_addr = request.remote_addr();
        let client_cert_identity = node_client_identity(&request)?;
        let mut inbound = request.into_inner();
        let first = inbound.message().await?.ok_or_else(|| {
            Status::unauthenticated("first replication message must be JoinRequest")
        })?;
        let join = match first.payload {
            Some(generated::follower_message::Payload::Join(join)) => join,
            _ => {
                return Err(Status::unauthenticated(
                    "first replication message must be JoinRequest",
                ));
            }
        };

        let dataplane =
            validate_join_metadata_with_endpoint(&join, remote_addr.map(|addr| addr.ip()))
                .map_err(|err| Status::invalid_argument(err.to_string()))?;
        self.require_raft_leader()?;
        let role = match generated::JoinRole::try_from(join.role)
            .map_err(|_| Status::invalid_argument("unknown join role"))?
        {
            generated::JoinRole::Worker => JoinRole::Worker,
            generated::JoinRole::Unspecified => {
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
                self.service
                    .handle_authenticated_join(crate::replication::protocol::JoinRequest {
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
            self.db
                .update_node_dataplane(dataplane.clone())
                .await
                .map_err(|err| Status::internal(err.to_string()))?;
            if let (Some(query), Some(status)) = (
                self.node_self_query.as_deref(),
                self.node_lifecycle_status.as_deref(),
            ) {
                refresh_joining_node_from_dataplane(self.db.as_ref(), query, status, &dataplane)
                    .await
                    .map_err(|err| Status::internal(err.to_string()))?;
            } else {
                refresh_node_routing_metadata_from_dataplane(self.db.as_ref(), &dataplane)
                    .await
                    .map_err(|err| Status::internal(err.to_string()))?;
            }
        }
        let joined_node_name = dataplane.node_name.clone();
        let (mut control_rx, follower_session) = if accepted {
            let (rx, session) = self.service.register_follower(dataplane.clone()).await;
            (Some(rx), Some(session))
        } else {
            (None, None)
        };
        let first_response = join_response_to_proto(self.db.as_ref(), response).await?;
        let service = self.service.clone();
        // T6: `db` and `controller_dispatcher` were captured by the
        // legacy Forward handler. The handler is gone; the stream now
        // only relays Ack / NodeExec / PodLog messages, none of which
        // need the leader datastore here. Keep the underscore-bound
        // names so future Raft work has a tap-in point.
        let db_for_observed_endpoint = self.db.clone();
        let local_node_name_for_observed_endpoint = self.local_node_name.clone();
        let node_self_query_for_observed_endpoint = self.node_self_query.clone();
        let node_self_status_for_observed_endpoint = self.node_self_status.clone();
        let _db = self.db.clone();
        let _controller_dispatcher = self.controller_dispatcher.clone();
        let mut entries = if accepted {
            Some(
                service
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
            yield Ok(generated::LeaderMessage {
                payload: Some(generated::leader_message::Payload::JoinResponse(first_response)),
            });
            if accepted {
                if let Some(local_node_name) = local_node_name_for_observed_endpoint.as_deref() {
                    match node_has_external_ip(db_for_observed_endpoint.as_ref(), local_node_name).await {
                        Ok(false) => {
                            yield Ok(generated::LeaderMessage {
                                payload: Some(
                                    generated::leader_message::Payload::ObserveLeaderEndpointRequest(
                                        generated::ObserveLeaderEndpointRequest {},
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
                                Some(generated::follower_message::Payload::Ack(ack)) => {
                                    service.update_follower_ack(&joined_node_name, ack.applied_rv).await;
                                }
                                Some(generated::follower_message::Payload::NodeExecSyncResponse(response)) => {
                                    if let Err(err) = service.complete_node_exec_sync(node_exec_sync_response_from_proto(response)).await {
                                        tracing::warn!(node = %joined_node_name, error = %err, "dropped unmatched node exec response");
                                    }
                                }
                                Some(generated::follower_message::Payload::PodLogResponse(response)) => {
                                    if let Err(err) = service.complete_pod_log(pod_log_response_from_proto(response)).await {
                                        tracing::warn!(node = %joined_node_name, error = %err, "dropped unmatched pod log response");
                                    }
                                }
                                Some(generated::follower_message::Payload::NodeMetricsResponse(response)) => {
                                    if let Err(err) = service.complete_node_metrics(node_metrics_response_from_proto(response)).await {
                                        tracing::warn!(node = %joined_node_name, error = %err, "dropped unmatched node metrics response");
                                    }
                                }
                                Some(generated::follower_message::Payload::NodeExecStreamFrame(frame)) => {
                                    match node_exec_stream_frame_from_proto(frame) {
                                        Ok(frame) => {
                                            if let Err(err) = service.complete_node_exec_stream_frame(frame).await {
                                                tracing::warn!(node = %joined_node_name, error = %err, "dropped unmatched node exec stream frame");
                                            }
                                        }
                                        Err(err) => {
                                            tracing::warn!(node = %joined_node_name, error = %err, "dropped invalid node exec stream frame");
                                        }
                                    }
                                }
                                Some(generated::follower_message::Payload::ObservedLeaderEndpoint(observed)) => {
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
                                Some(generated::follower_message::Payload::Join(_)) | None => {
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
                                    yield Ok(generated::LeaderMessage {
                                        payload: Some(generated::leader_message::Payload::NodeExecSyncRequest(
                                            node_exec_sync_request_to_proto(request),
                                        )),
                                    });
                                }
                                FollowerControlMessage::NodeExec(request) => {
                                    yield Ok(generated::LeaderMessage {
                                        payload: Some(generated::leader_message::Payload::NodeExecRequest(
                                            node_exec_request_to_proto(request),
                                        )),
                                    });
                                }
                                FollowerControlMessage::NodeExecFrame(frame) => {
                                    yield Ok(generated::LeaderMessage {
                                        payload: Some(generated::leader_message::Payload::NodeExecStreamFrame(
                                            node_exec_stream_frame_to_proto(frame),
                                        )),
                                    });
                                }
                                FollowerControlMessage::PodLog(request) => {
                                    yield Ok(generated::LeaderMessage {
                                        payload: Some(generated::leader_message::Payload::PodLogRequest(
                                            pod_log_request_to_proto(request),
                                        )),
                                    });
                                }
                                FollowerControlMessage::NodeMetrics(request) => {
                                    yield Ok(generated::LeaderMessage {
                                        payload: Some(generated::leader_message::Payload::NodeMetricsRequest(
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
                            yield Ok(generated::LeaderMessage {
                                payload: Some(generated::leader_message::Payload::StreamItem(
                                    generated::StreamItem {
                                        item: Some(generated::stream_item::Item::Entry(entry)),
                                    }
                                )),
                            });
                        }
                    }
                }
                if let Some(session) = follower_session {
                    service.unregister_follower(&joined_node_name, session).await;
                }
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }

    async fn snapshot(
        &self,
        request: Request<generated::SnapshotRequest>,
    ) -> std::result::Result<Response<Self::SnapshotStream>, Status> {
        self.require_steady_state_auth(&request).await?;
        let last_applied_rv = request.into_inner().last_applied_rv;
        let db = self.db.clone();

        // memory-improvement.md §10 P1: stream the snapshot straight to the
        // wire instead of materializing it into one `Arc<Vec<LogApplyCommit>>`.
        // The producer drives `emit_snapshot_commits` (which keyset-pages
        // watch_events) and pushes each commit's protobuf into a bounded
        // channel; the stream yields from that channel.
        //
        // The producer and consumer run COOPERATIVELY on this one stream
        // future (no spawn): `select!` races the bounded-channel recv against
        // the producer so that when the channel fills up the producer awaits
        // while the consumer drains — that backpressure is what bounds peak
        // resident memory to O(channel capacity + page size), not O(rows).
        let stream = async_stream::stream! {
            const SNAPSHOT_STREAM_CHANNEL_CAPACITY: usize = 256;
            let (tx, mut rx) = mpsc::channel::<
                std::result::Result<generated::ReplicationEntry, Status>,
            >(SNAPSHOT_STREAM_CHANNEL_CAPACITY);
            let err_tx = tx.clone();
            let producer = async move {
                let mut sink = crate::replication::snapshot_commit_channel_sink::SnapshotCommitChannelSink::new(tx);
                let result = crate::replication::snapshot::stream_snapshot_commits(
                    db.as_ref(),
                    last_applied_rv,
                    &mut sink,
                )
                .await;
                if let Err(err) = result {
                    // Surface the generation error as a terminal stream item,
                    // then close the channel via finish().
                    let _ = err_tx
                        .send(Err(Status::internal(err.to_string())))
                        .await;
                }
                let _ = sink.finish();
            };
            tokio::pin!(producer);
            let mut producer_done = false;
            loop {
                if producer_done {
                    match rx.recv().await {
                        Some(Ok(proto)) => yield Ok(proto),
                        Some(Err(status)) => {
                            yield Err(status);
                            return;
                        }
                        None => return,
                    }
                } else {
                    tokio::select! {
                        biased;
                        item = rx.recv() => match item {
                            Some(Ok(proto)) => yield Ok(proto),
                            Some(Err(status)) => {
                                yield Err(status);
                                return;
                            }
                            None => return,
                        },
                        _ = &mut producer => {
                            producer_done = true;
                        }
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_metadata(
        &self,
        request: Request<generated::MetadataRequest>,
    ) -> std::result::Result<Response<generated::MetadataResponse>, Status> {
        self.require_steady_state_auth(&request).await?;
        let metadata = self.service.handle_metadata().await;
        Ok(Response::new(generated::MetadataResponse {
            cluster_id: metadata.cluster_id,
            leader_epoch: metadata.leader_epoch,
            current_rv: metadata.current_rv,
            current_log_index: metadata.current_log_index,
            supported_features: metadata.supported_features,
        }))
    }

    async fn get_resource(
        &self,
        request: Request<generated::GetResourceRequest>,
    ) -> std::result::Result<Response<generated::GetResourceResponse>, Status> {
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
        let key = query.key();
        let resource = self
            .db
            .get_resource(
                &key.api_version,
                &key.kind,
                key.namespace.as_deref(),
                &key.name,
            )
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        Self::require_raft_leadership_unchanged(leadership_rx.as_ref())?;
        Ok(Response::new(match resource {
            Some(resource) => generated::GetResourceResponse {
                found: true,
                resource: Some(resource_to_proto(&resource)),
            },
            None => generated::GetResourceResponse {
                found: false,
                resource: None,
            },
        }))
    }

    async fn list_resources(
        &self,
        request: Request<generated::ListResourcesRequest>,
    ) -> std::result::Result<Response<generated::ListResourcesResponse>, Status> {
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
            .db
            .list_resources(
                query.api_version(),
                query.kind(),
                query.namespace(),
                crate::datastore::ResourceListQuery::new(
                    query.label_selector(),
                    query.field_selector(),
                    query.limit(),
                    query.continue_token(),
                ),
            )
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        Self::require_raft_leadership_unchanged(leadership_rx.as_ref())?;
        let items: Vec<generated::ResourceObject> =
            list.items.iter().map(resource_to_proto).collect();
        Ok(Response::new(generated::ListResourcesResponse {
            items,
            total: list.items.len() as i64,
            continue_token: list.continue_token,
            resource_version: list.resource_version,
            remaining_item_count: list.remaining_item_count,
            watch_replay_position: list
                .watch_replay_position
                .map(watch_replay_position_to_proto),
        }))
    }

    async fn submit_resource_command(
        &self,
        request: Request<generated::SubmitResourceCommandRequest>,
    ) -> std::result::Result<Response<generated::SubmitResourceCommandResponse>, Status> {
        self.require_controlplane_node_auth(&request, "resource command submissions")
            .await?;
        self.require_raft_leader()?;
        let request = resource_command_request_from_proto(request.into_inner())
            .map_err(resource_command_status)?;
        let result = crate::control_plane::client::local::submit_resource_command_to_store(
            &self.db, request,
        )
        .await
        .map_err(resource_command_status)?;
        Ok(Response::new(resource_command_result_to_proto(result)))
    }

    async fn watch_resources(
        &self,
        request: Request<generated::WatchResourcesRequest>,
    ) -> std::result::Result<Response<Self::WatchResourcesStream>, Status> {
        self.require_steady_state_auth(&request).await?;
        // Issue #4: a worker watch must be served by the current raft leader.
        // Reject establishment on a stale follower so the worker reconnects to
        // the new leader instead of streaming from a deposed node.
        let leadership_rx = self.sample_raft_leadership()?;
        let req = request.into_inner();
        klights_leader_api::WatchRequest::try_new(
            req.api_version.clone(),
            req.kind.clone(),
            req.namespace.clone(),
            req.label_selector.clone(),
            req.field_selector.clone(),
            Some(req.start_resource_version),
            req.start_watch_replay_position
                .as_ref()
                .map(watch_replay_position_from_proto),
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let topic = crate::watch::WatchTopic::new(&req.api_version, &req.kind);
        // For new peers, the exact position came from the same read snapshot as
        // LIST. For legacy scalar-RV peers, capture a durable high-water mark
        // before synchronously subscribing, then filter the pre-anchor prefix
        // by RV. Rows applied after the anchor are replayed by event ID even if
        // their RV is lower than an earlier row.
        let requested_position = req
            .start_watch_replay_position
            .as_ref()
            .map(watch_replay_position_from_proto);
        let replay_position = if let Some(position) = requested_position {
            position
        } else {
            let anchor = self
                .db
                .current_watch_replay_position()
                .await
                .map_err(|err| Status::internal(err.to_string()))?;
            WatchReplayPosition::from_resource_version_through_event_id(
                req.start_resource_version,
                anchor.event_id,
            )
        };
        Self::require_raft_leadership_unchanged(leadership_rx.as_ref())?;
        // No await is permitted between the durable anchor above and this
        // subscription. Replay closes the anchor->subscribe interval.
        let signal_rx = self.db.subscribe_watch_signals(topic.clone());
        let replay_source = DatastoreWatchReplaySource::new(
            std::sync::Arc::new(crate::datastore::DatastoreBackendWatchStore::new(
                self.db.clone(),
            )),
            vec![watch_target_for_request(&req)],
        );
        let scope: crate::watch::WatchDeliveryScope = watch_delivery_scope_for_request(&req);
        let supervisor = self.service.task_supervisor();
        let heartbeat_interval = self.watch_heartbeat_interval;
        // Clone the leadership signal into the stream so the loop can race it
        // against the broadcast recv and terminate promptly on a leadership
        // change. Without this a deposed leader's broadcast goes silent and the
        // worker waits up to its ~60s idle watchdog before reconnecting, reading
        // stale informer-cached state in the window.
        let mut leader_rx = leadership_rx;
        let stream = async_stream::stream! {
            let mut last_rv = req
                .start_resource_version
                .max(replay_position.resource_version);
            let mut cursor = crate::watch::SignalWatchCursor::new_many_at_position(
                signal_rx,
                replay_source,
                vec![topic],
                scope,
                last_rv,
                replay_position,
                crate::watch::WindowPolicy::default_watch_delivery(),
            );
            if let Err(err) = cursor.prime_replay_or_expired().await {
                yield Err(watch_cursor_error_to_status(err, cursor.accepted_rv()));
                return;
            }
            // bug-grpc B2: per-stream heartbeat. The previous code reset the
            // heartbeat deadline on every loop iteration, so continuous
            // *non-matching* broadcast traffic (the global firehose carries
            // every kind) starved a quiet *matching* stream's BOOKMARK — the
            // worker then idle-reconnected every window. Track when THIS stream
            // last yielded (an event or a bookmark) and wait only the remaining
            // time; a filtered-out event does NOT reset the clock, so the
            // bookmark still fires on schedule under unrelated traffic.
            let mut last_yield_at = Instant::now();
            loop {
                let elapsed = last_yield_at.elapsed();
                if elapsed >= heartbeat_interval {
                    yield Ok(watch_heartbeat_proto(
                        &req.api_version,
                        &req.kind,
                        last_rv,
                        cursor.processed_position(),
                    ));
                    last_yield_at = Instant::now();
                    continue;
                }
                let wait = heartbeat_interval - elapsed;
                // broadcast::Receiver::recv is cancel-safe, so dropping it on
                // timeout loses no event. Race it against a leadership-loss
                // signal (issue #4): if this node stops being the raft leader,
                // end the stream so the worker reconnects to the new leader
                // instead of idling on a deposed, silent broadcaster.
                let recv = if let Some(leader_watch) = leader_rx.as_mut() {
                    tokio::select! {
                        biased;
                        _ = watch_leadership_lost(leader_watch) => break,
                        r = supervisor
                            .timeout("grpc_watch_heartbeat", wait, cursor.next_event()) => r,
                    }
                } else {
                    supervisor
                        .timeout("grpc_watch_heartbeat", wait, cursor.next_event())
                        .await
                };
                let event = match recv {
                    Ok(Ok(event)) => event,
                    // Idle past this stream's heartbeat window: emit a liveness
                    // bookmark carrying the cursor so the client resumes
                    // correctly, and reset the per-stream clock.
                    Ok(Err(_elapsed)) => {
                        yield Ok(watch_heartbeat_proto(
                            &req.api_version,
                            &req.kind,
                            last_rv,
                            cursor.processed_position(),
                        ));
                        last_yield_at = Instant::now();
                        continue;
                    }
                    // Supervisor declined the timer (root shutdown): end stream.
                    Err(_shutdown) => break,
                };
                let event = match event {
                    Ok(event) => event,
                    Err(crate::watch::WatchCursorError::Closed) => break,
                    Err(err) => {
                        yield Err(watch_cursor_error_to_status(err, cursor.accepted_rv()));
                        return;
                    }
                };
                if !watch_event_should_stream(&event, &req) {
                    continue;
                }
                let resource = resource_from_event(&event);
                let rv = resource.resource_version;
                let event_type = watch_event_type(&event).to_string();
                yield Ok(generated::WatchEvent {
                    event_type,
                    resource: Some(resource_to_proto(&resource)),
                    resume_position: Some(watch_replay_position_to_proto(cursor.processed_position())),
                });
                if rv > 0 {
                    cursor.accept_event(rv);
                    last_rv = last_rv.max(rv);
                }
                last_yield_at = Instant::now();
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }

    async fn projected_service_account_token(
        &self,
        request: Request<generated::ProjectedServiceAccountTokenRequest>,
    ) -> std::result::Result<Response<generated::ProjectedServiceAccountTokenResponse>, Status>
    {
        self.require_steady_state_auth(&request).await?;
        self.require_raft_leader()?;
        let caller = caller_node_authority(&request);
        let req = request.into_inner();
        let token_request =
            crate::control_plane::client::ProjectedServiceAccountTokenRequest::try_new(
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
        let signing_key_pem = self.service_account_signing_key_pem().await?;
        let token =
            crate::control_plane::service_account_tokens::issue_projected_service_account_token(
                self.db.as_ref(),
                self.pod_store.as_ref(),
                &signing_key_pem,
                &token_request,
            )
            .await
            .map_err(projected_token_error_to_status)?;
        Ok(Response::new(
            generated::ProjectedServiceAccountTokenResponse {
                token: token.into_token(),
            },
        ))
    }

    async fn apply_outbox(
        &self,
        request: Request<generated::ApplyOutboxRequest>,
    ) -> std::result::Result<Response<generated::ApplyOutboxResponse>, Status> {
        self.require_steady_state_auth(&request).await?;
        self.require_raft_leader()?;
        let caller = caller_node_authority(&request);
        let req = request.into_inner();
        let authenticated_node = match &caller {
            CallerAuthority::Node(node_name) => node_name.as_str(),
            CallerAuthority::Unrestricted => {
                return Err(Status::permission_denied(
                    "durable outbox delivery requires an authenticated node client certificate",
                ));
            }
        };
        // NodeRestriction: the legacy wire author must equal the certificate-bound author.
        enforce_node_authority(&caller, &req.authoring_node)?;
        let delivery_operation =
            klights_leader_api::OutboxDeliveryOperation::try_from_wire_name(&req.operation)
                .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let delivery_request = klights_leader_api::OutboxDeliveryRequest::try_new(
            req.idempotency_key,
            delivery_operation,
            std::sync::Arc::<[u8]>::from(req.payload_proto),
            req.client_id,
            req.stream_id,
            req.stream_seq,
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let (idempotency_key, delivery_operation, payload, client_id, stream_id, stream_seq) =
            delivery_request.into_parts();
        let operation = delivery_operation.into();
        let watermark = crate::control_plane::client::apply::outbox_stream_watermark(
            &client_id, stream_id, stream_seq,
        );
        let payload = bytes::Bytes::from_owner(payload);
        let decoded_payload =
            match crate::kubelet::outbox::payload::OutboxPayload::decode_protobuf(&payload) {
                Ok(decoded) => decoded,
                Err(error) => {
                    consume_terminal_outbox_sequence_for_rpc(
                        self.db.as_ref(),
                        &idempotency_key,
                        operation,
                        authenticated_node,
                        watermark.clone(),
                    )
                    .await?;
                    return Ok(apply_outbox_error_response(
                        klights_leader_api::OutboxDeliveryError::invalid(
                            "delivery.payload",
                            format!("invalid outbox payload: {error}"),
                        ),
                    ));
                }
            };
        if let Err(error) = crate::control_plane::client::apply::authorize_outbox_command(
            delivery_operation,
            &decoded_payload.command,
            authenticated_node,
        ) {
            consume_terminal_outbox_sequence_for_rpc(
                self.db.as_ref(),
                &idempotency_key,
                operation,
                authenticated_node,
                watermark.clone(),
            )
            .await?;
            return Ok(apply_outbox_error_response(error));
        }
        if delivery_operation == klights_leader_api::OutboxDeliveryOperation::PodMetadata
            && let Err(error) =
                crate::control_plane::client::apply::authorize_live_pod_metadata_command(
                    self.db.as_ref(),
                    &decoded_payload.command,
                    authenticated_node,
                )
                .await
        {
            if error.is_terminal() {
                consume_terminal_outbox_sequence_for_rpc(
                    self.db.as_ref(),
                    &idempotency_key,
                    operation,
                    authenticated_node,
                    watermark.clone(),
                )
                .await?;
                return Ok(apply_outbox_error_response(error));
            }
            return Err(Status::unavailable(error.to_string()));
        }
        if operation == crate::kubelet::outbox::payload::OutboxOperation::NodeStatus {
            if let Err(error) = klights_leader_api::NodeSelfStatusRequest::validate_command(
                &decoded_payload.command,
            ) {
                consume_terminal_outbox_sequence_for_rpc(
                    self.db.as_ref(),
                    &idempotency_key,
                    operation,
                    authenticated_node,
                    watermark.clone(),
                )
                .await?;
                return Ok(apply_outbox_error_response(
                    klights_leader_api::OutboxDeliveryError::invalid(
                        "delivery.payload",
                        error.to_string(),
                    ),
                ));
            }
            let crate::datastore::command::StorageCommand::UpdateStatus {
                name,
                preconditions,
                ..
            } = &decoded_payload.command
            else {
                unreachable!("NodeSelfStatusRequest validation admits only UpdateStatus")
            };
            if name != authenticated_node {
                consume_terminal_outbox_sequence_for_rpc(
                    self.db.as_ref(),
                    &idempotency_key,
                    operation,
                    authenticated_node,
                    watermark.clone(),
                )
                .await?;
                return Ok(apply_outbox_error_response(
                    klights_leader_api::OutboxDeliveryError::conflict(format!(
                        "node {} cannot publish Node status for {name}",
                        authenticated_node
                    )),
                ));
            }
            let current = self
                .db
                .get_resource("v1", "Node", None, name)
                .await
                .map_err(|error| Status::unavailable(error.to_string()))?;
            let Some(current) = current else {
                consume_terminal_outbox_sequence_for_rpc(
                    self.db.as_ref(),
                    &idempotency_key,
                    operation,
                    authenticated_node,
                    watermark.clone(),
                )
                .await?;
                return Ok(apply_outbox_error_response(
                    klights_leader_api::OutboxDeliveryError::not_found(format!(
                        "v1/Node/{name} not found"
                    )),
                ));
            };
            if preconditions.uid.as_deref() != Some(current.uid.as_str()) {
                consume_terminal_outbox_sequence_for_rpc(
                    self.db.as_ref(),
                    &idempotency_key,
                    operation,
                    authenticated_node,
                    watermark.clone(),
                )
                .await?;
                return Ok(apply_outbox_error_response(
                    klights_leader_api::OutboxDeliveryError::uid_mismatch(
                        preconditions.uid.clone().unwrap_or_default(),
                        current.uid,
                    ),
                ));
            }
        }
        let result =
            crate::control_plane::client::apply::apply_outbox_to_local_leader_with_resource(
                self.db.as_ref(),
                &idempotency_key,
                operation,
                payload,
                authenticated_node,
                watermark,
            )
            .await;
        match result {
            Ok(crate::control_plane::client::apply::LocalOutboxApply {
                result: crate::kubelet::outbox::OutboxApplyResult::Applied { applied_rv },
                resource,
                command: Some(command),
                ..
            }) => {
                crate::control_plane::client::pod_status_side_effects::handle_applied_pod_side_effects(
                    self.controller_dispatcher.as_ref(),
                    &command,
                    resource.as_ref(),
                    self.db.as_ref(),
                )
                .await;
                Ok(Response::new(generated::ApplyOutboxResponse {
                    already_applied: false,
                    applied_rv,
                    error: None,
                    error_type: None,
                }))
            }
            Ok(crate::control_plane::client::apply::LocalOutboxApply {
                result: crate::kubelet::outbox::OutboxApplyResult::Applied { applied_rv },
                ..
            }) => Ok(Response::new(generated::ApplyOutboxResponse {
                already_applied: false,
                applied_rv,
                error: None,
                error_type: None,
            })),
            Ok(crate::control_plane::client::apply::LocalOutboxApply {
                result: crate::kubelet::outbox::OutboxApplyResult::AlreadyApplied { applied_rv },
                ..
            }) => Ok(Response::new(generated::ApplyOutboxResponse {
                already_applied: true,
                applied_rv: applied_rv.unwrap_or(0),
                error: None,
                error_type: None,
            })),
            Err(err) => Ok(apply_outbox_error_response(err)),
        }
    }

    async fn renew_node_lease(
        &self,
        request: Request<generated::RenewNodeLeaseRequest>,
    ) -> std::result::Result<Response<generated::RenewNodeLeaseResponse>, Status> {
        self.require_steady_state_auth(&request).await?;
        self.require_raft_leader()?;
        let caller = caller_node_authority(&request);
        let req = request.into_inner();
        // NodeRestriction: a node may only renew its own lease.
        enforce_node_authority(&caller, &req.node_name)?;
        if req.lease_duration_seconds <= 0 {
            return Err(Status::invalid_argument(
                "lease_duration_seconds must be positive",
            ));
        }
        validate_node_lease_renew_time_skew(&req.renew_time, chrono::Utc::now())?;
        self.node_lease_tracker
            .record_from_lease_object(
                &req.node_name,
                &serde_json::json!({
                    "metadata": {
                        "name": req.node_name,
                        "namespace": "kube-node-lease"
                    },
                    "spec": {
                        "holderIdentity": req.node_name,
                        "leaseDurationSeconds": req.lease_duration_seconds,
                        "renewTime": req.renew_time
                    }
                }),
            )
            .await
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        Ok(Response::new(generated::RenewNodeLeaseResponse {}))
    }

    async fn allocate_node_subnet(
        &self,
        request: Request<generated::AllocateNodeSubnetRequest>,
    ) -> std::result::Result<Response<generated::NodeSubnetResponse>, Status> {
        self.require_steady_state_auth(&request).await?;
        self.require_raft_leader()?;
        let authority = caller_node_authority(&request);
        let req = request.into_inner();
        enforce_node_authority(&authority, &req.node_name)?;
        let focused_request = klights_leader_api::NodeSubnetAllocationRequest::try_new(
            req.node_name,
            req.cluster_cidr,
            &req.node_ip,
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let (node_name, cluster_cidr, node_ip) = focused_request.into_parts();
        let subnet = self
            .db
            .allocate_node_subnet(&node_name, &cluster_cidr, &node_ip.to_string())
            .await
            .map_err(|error| {
                let message = error.to_string();
                if crate::control_plane::client::node_subnet_allocation_is_exhausted(&message) {
                    Status::resource_exhausted(message)
                } else if message.to_ascii_lowercase().contains("conflict") {
                    Status::aborted(message)
                } else {
                    Status::internal(message)
                }
            })?;
        Ok(Response::new(generated::NodeSubnetResponse {
            subnet: Some(node_subnet_to_proto(subnet)?),
        }))
    }

    async fn get_node_subnet(
        &self,
        request: Request<generated::GetNodeSubnetRequest>,
    ) -> std::result::Result<Response<generated::GetNodeSubnetResponse>, Status> {
        self.require_steady_state_auth(&request).await?;
        self.require_raft_leader()?;
        let req = request.into_inner();
        let query = klights_leader_api::NodeSubnetQuery::try_new(req.node_name)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let subnet = self
            .db
            .get_node_subnet(query.node_name())
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(match subnet {
            Some(subnet) => generated::GetNodeSubnetResponse {
                found: true,
                subnet: Some(node_subnet_to_proto(subnet)?),
            },
            None => generated::GetNodeSubnetResponse {
                found: false,
                subnet: None,
            },
        }))
    }

    async fn list_peer_subnets(
        &self,
        request: Request<generated::ListPeerSubnetsRequest>,
    ) -> std::result::Result<Response<generated::ListPeerSubnetsResponse>, Status> {
        self.require_steady_state_auth(&request).await?;
        self.require_raft_leader()?;
        let req = request.into_inner();
        let query = klights_leader_api::PeerSubnetsQuery::try_new(req.my_node_name)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let items = self
            .db
            .list_peer_subnets(query.node_name())
            .await
            .map_err(|err| Status::internal(err.to_string()))?
            .into_iter()
            .map(crate::control_plane::client::focused_node_subnet)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| Status::internal(error.to_string()))?;
        let items = klights_leader_api::PeerSubnetsResult::try_new(query.node_name(), items)
            .map_err(|error| Status::internal(error.to_string()))?
            .into_vec()
            .into_iter()
            .map(focused_node_subnet_to_proto)
            .collect();
        Ok(Response::new(generated::ListPeerSubnetsResponse { items }))
    }

    async fn get_node_dataplane(
        &self,
        request: Request<generated::GetNodeDataplaneRequest>,
    ) -> std::result::Result<Response<generated::GetNodeDataplaneResponse>, Status> {
        self.require_steady_state_auth(&request).await?;
        self.require_raft_leader()?;
        let req = request.into_inner();
        let query = klights_leader_api::NodeDataplaneQuery::try_new(req.node_name)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let metadata = self
            .db
            .get_node_dataplane(query.node_name())
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(match metadata {
            Some(metadata) => generated::GetNodeDataplaneResponse {
                found: true,
                metadata: Some(dataplane_metadata_to_proto(metadata)?),
            },
            None => generated::GetNodeDataplaneResponse {
                found: false,
                metadata: None,
            },
        }))
    }

    async fn observe_peer_endpoint(
        &self,
        request: Request<generated::ObservePeerEndpointRequest>,
    ) -> std::result::Result<Response<generated::ObservePeerEndpointResponse>, Status> {
        self.require_steady_state_auth(&request).await?;
        let caller = caller_node_authority(&request);
        let observed_endpoint = request.remote_addr().map(|addr| addr.ip().to_string());
        let req = request.into_inner();
        enforce_node_authority(&caller, &req.node_name)?;
        if req.node_name.trim().is_empty() {
            return Err(Status::invalid_argument("node_name is required"));
        }

        if let Some(endpoint) = observed_endpoint {
            self.service
                .record_observed_peer_endpoint(&req.node_name, endpoint.clone())
                .await;
            return Ok(Response::new(generated::ObservePeerEndpointResponse {
                found: true,
                endpoint,
            }));
        }

        Ok(Response::new(
            match self.service.observed_peer_endpoint(&req.node_name).await {
                Some(endpoint) => generated::ObservePeerEndpointResponse {
                    found: true,
                    endpoint,
                },
                None => generated::ObservePeerEndpointResponse {
                    found: false,
                    endpoint: String::new(),
                },
            },
        ))
    }

    async fn list_pod_cleanup_intents_for_node(
        &self,
        request: Request<generated::ListPodCleanupIntentsForNodeRequest>,
    ) -> std::result::Result<Response<generated::ListPodCleanupIntentsForNodeResponse>, Status>
    {
        self.require_steady_state_auth(&request).await?;
        self.require_raft_leader()?;
        let caller = caller_node_authority(&request);
        let req = request.into_inner();
        let request =
            crate::control_plane::client::PodCleanupIntentListRequest::try_new(req.node_name)
                .map_err(pod_cleanup_intent_error_to_status)?;
        enforce_node_authority(&caller, request.node_name())?;
        let items = self
            .db
            .list_pod_cleanup_intents_for_node(request.node_name())
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .into_iter()
            .map(crate::control_plane::client::local::focused_pod_cleanup_intent)
            .map(|intent| {
                intent
                    .map_err(pod_cleanup_intent_error_to_status)
                    .and_then(pod_cleanup_intent_to_proto)
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(Response::new(
            generated::ListPodCleanupIntentsForNodeResponse { items },
        ))
    }

    async fn delete_pod_cleanup_intent(
        &self,
        request: Request<generated::DeletePodCleanupIntentRequest>,
    ) -> std::result::Result<Response<generated::DeletePodCleanupIntentResponse>, Status> {
        self.require_steady_state_auth(&request).await?;
        self.require_raft_leader()?;
        let caller = caller_node_authority(&request);
        let req = request.into_inner();
        let request = crate::control_plane::client::PodCleanupIntentAckRequest::try_new(
            req.node_name,
            req.namespace,
            req.pod_name,
            req.pod_uid,
            req.reason,
        )
        .map_err(pod_cleanup_intent_error_to_status)?;
        // NodeRestriction: a node may only clear its own pod cleanup intents.
        enforce_node_authority(&caller, request.node_name())?;
        let (node_name, namespace, pod_name, pod_uid, reason) = request.into_parts();
        self.db
            .delete_pod_cleanup_intent(&node_name, &namespace, &pod_name, &pod_uid, &reason)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(generated::DeletePodCleanupIntentResponse {}))
    }

    // ── Phase 3 Raft consensus RPCs (P3-11b) ────────────────────────────

    async fn raft_append_entries(
        &self,
        request: Request<generated::RaftAppendEntriesRequest>,
    ) -> std::result::Result<Response<generated::RaftAppendEntriesResponse>, Status> {
        self.require_raft_peer_auth(&request).await?;
        let payload = request.into_inner().payload;
        Ok(Response::new(generated::RaftAppendEntriesResponse {
            result: Some(
                match dispatch_raft_rpc(self.raft_rpc_router.as_ref(), |r| {
                    r.append_entries(payload.clone())
                })
                .await
                {
                    Ok(bytes) => generated::raft_append_entries_response::Result::Ok(bytes),
                    Err(msg) => generated::raft_append_entries_response::Result::Error(msg),
                },
            ),
        }))
    }

    async fn raft_vote(
        &self,
        request: Request<generated::RaftVoteRequest>,
    ) -> std::result::Result<Response<generated::RaftVoteResponse>, Status> {
        self.require_raft_peer_auth(&request).await?;
        let payload = request.into_inner().payload;
        Ok(Response::new(generated::RaftVoteResponse {
            result: Some(
                match dispatch_raft_rpc(self.raft_rpc_router.as_ref(), |r| r.vote(payload.clone()))
                    .await
                {
                    Ok(bytes) => generated::raft_vote_response::Result::Ok(bytes),
                    Err(msg) => generated::raft_vote_response::Result::Error(msg),
                },
            ),
        }))
    }

    async fn raft_install_snapshot(
        &self,
        request: Request<generated::RaftInstallSnapshotRequest>,
    ) -> std::result::Result<Response<generated::RaftInstallSnapshotResponse>, Status> {
        self.require_raft_peer_auth(&request).await?;
        let payload = request.into_inner().payload;
        Ok(Response::new(generated::RaftInstallSnapshotResponse {
            result: Some(
                match dispatch_raft_rpc(self.raft_rpc_router.as_ref(), |r| {
                    r.install_snapshot(payload.clone())
                })
                .await
                {
                    Ok(bytes) => generated::raft_install_snapshot_response::Result::Ok(bytes),
                    Err(msg) => generated::raft_install_snapshot_response::Result::Error(msg),
                },
            ),
        }))
    }

    async fn join_as_controlplane(
        &self,
        request: Request<generated::JoinAsControlplaneRequest>,
    ) -> std::result::Result<Response<generated::JoinAsControlplaneResponse>, Status> {
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
        let client_cert_identity = node_client_identity(&request)?;
        let mut req = request.into_inner();
        let Some(identity) = client_cert_identity.as_ref() else {
            return Err(Status::unauthenticated(
                "JoinAsControlplane requires a node client certificate; bootstrap tokens are only valid for CSR bootstrap",
            ));
        };
        validate_node_client_identity(identity, Some(&req.node_name))?;
        let Some(handler) = self.controlplane_join_handler.as_ref() else {
            return Ok(Response::new(generated::JoinAsControlplaneResponse {
                result: Some(generated::join_as_controlplane_response::Result::Denied(
                    generated::JoinAsControlplaneDenied {
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
        let expected_node_id =
            crate::datastore::raft::types::raft_node_id_for_node_name(&req.node_name);
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
                (&registration.node_mode, dataplane.mode),
                (
                    crate::controllers::annotations::NodePeerMode::Root,
                    DataplaneMode::Root
                ) | (
                    crate::controllers::annotations::NodePeerMode::Rootless,
                    DataplaneMode::Rootless
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
        let outcome = handler
            .join(
                crate::replication::grpc::raft_rpc::ControlplaneJoinRequest {
                    node_id: req.node_id,
                    addr: raft_addr,
                    node_name: req.node_name,
                    as_learner: req.as_learner,
                    supported_features: req.supported_features,
                    node_internal_ip,
                    node_registration,
                    legacy_node_git_commit: Some(req.node_git_commit)
                        .filter(|value| !value.trim().is_empty()),
                },
            )
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        let result = match outcome {
            crate::replication::grpc::raft_rpc::ControlplaneJoinOutcome::Accepted {
                voter_count_after,
                admitted_as_learner,
                ..
            } => {
                self.db
                    .update_node_dataplane(dataplane)
                    .await
                    .map_err(|err| Status::internal(err.to_string()))?;
                let ca_cert_pem = self
                    .controlplane_ca_files
                    .join_response_ca_cert_pem()
                    .await?;
                generated::join_as_controlplane_response::Result::Accepted(
                    generated::JoinAsControlplaneAccepted {
                        voter_count_after,
                        admitted_as_learner,
                        ca_cert_pem,
                        encrypted_ca_key: Vec::new(),
                        ca_key_nonce: Vec::new(),
                    },
                )
            }
            crate::replication::grpc::raft_rpc::ControlplaneJoinOutcome::RedirectToLeader {
                leader_id,
                leader_addr,
            } => generated::join_as_controlplane_response::Result::RedirectToLeader(
                generated::JoinAsControlplaneRedirect {
                    leader_id,
                    leader_addr,
                },
            ),
            crate::replication::grpc::raft_rpc::ControlplaneJoinOutcome::Denied { reason } => {
                generated::join_as_controlplane_response::Result::Denied(
                    generated::JoinAsControlplaneDenied { reason },
                )
            }
        };
        Ok(Response::new(generated::JoinAsControlplaneResponse {
            result: Some(result),
        }))
    }

    async fn sign_controlplane_csr(
        &self,
        request: Request<generated::SignControlplaneCsrRequest>,
    ) -> std::result::Result<Response<generated::SignControlplaneCsrResponse>, Status> {
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
        let client_cert_identity = node_client_identity(&request)?;

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

        let signer =
            crate::auth::csr_signer::CaCsrSigner::new(ca_cert_pem.clone(), ca_key_pem.clone());
        use crate::auth::csr_signer::CsrSigner;
        let sign_result = signer
            .sign(crate::auth::csr_signer::SignRequest {
                csr_pem: req.server_csr,
                common_name: "klights-server".to_string(),
                organizations: vec![],
                usages: vec!["server auth".to_string()],
                ttl_seconds: 86400 * 365 * 10,
            })
            .map_err(|e| Status::invalid_argument(format!("CSR signing failed: {e}")))?;

        let (
            encrypted_ca_key,
            ca_key_nonce,
            encrypted_service_account_signing_key,
            service_account_signing_key_nonce,
        ) = if controlplane_token_authenticated && !join_token.is_empty() {
            let (encrypted_ca_key, ca_key_nonce) =
                match crate::auth::ca_transport::encrypt_ca_key(&join_token, ca_key_pem.as_bytes())
                {
                    Ok((ct, nonce)) => (ct, nonce.to_vec()),
                    Err(e) => {
                        return Err(Status::internal(format!("CA key encryption failed: {e}")));
                    }
                };
            let (encrypted_service_account_signing_key, service_account_signing_key_nonce) =
                match crate::auth::ca_transport::encrypt_ca_key(
                    &join_token,
                    service_account_signing_key_pem.as_bytes(),
                ) {
                    Ok((ct, nonce)) => (ct, nonce.to_vec()),
                    Err(e) => {
                        return Err(Status::internal(format!(
                            "ServiceAccount signing key encryption failed: {e}"
                        )));
                    }
                };
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

        Ok(Response::new(generated::SignControlplaneCsrResponse {
            signed_server_cert: sign_result.certificate_pem,
            ca_cert_pem,
            encrypted_ca_key,
            ca_key_nonce,
            encrypted_service_account_signing_key,
            service_account_signing_key_nonce,
        }))
    }
}

/// Helper: dispatch one of the three Raft RPCs against the optional
/// router, mapping `Disabled` and dispatch errors into a `String` the
/// proto envelope can carry. The client side translates the `error`
/// arm into `RPCError::Unreachable` (router not installed) or
/// `RPCError::RemoteError` (consensus-layer error).
async fn dispatch_raft_rpc<'a, F, Fut>(
    router: Option<&'a Arc<dyn crate::replication::grpc::raft_rpc::RaftRpcRouter>>,
    call: F,
) -> std::result::Result<Vec<u8>, String>
where
    F: FnOnce(&'a Arc<dyn crate::replication::grpc::raft_rpc::RaftRpcRouter>) -> Fut,
    Fut: std::future::Future<
            Output = std::result::Result<
                Vec<u8>,
                crate::replication::grpc::raft_rpc::RaftRpcRouterError,
            >,
        >,
{
    let Some(router) = router else {
        return Err(crate::replication::grpc::raft_rpc::RaftRpcRouterError::Disabled.to_string());
    };
    call(router).await.map_err(|err| err.to_string())
}

fn resource_to_proto(resource: &crate::datastore::Resource) -> generated::ResourceObject {
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
    generated::ResourceObject {
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
) -> generated::SubmitResourceCommandResponse {
    use generated::submit_resource_command_response::Result as WireResult;
    let result = match result {
        klights_leader_api::ResourceCommandResult::Resource(resource) => {
            WireResult::Resource(resource_to_proto(&resource))
        }
        klights_leader_api::ResourceCommandResult::Ack { resource_version } => {
            WireResult::Ack(generated::ResourceCommandAck { resource_version })
        }
    };
    generated::SubmitResourceCommandResponse {
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

fn node_subnet_to_proto(
    subnet: crate::datastore::NodeSubnet,
) -> std::result::Result<generated::NodeSubnetObject, Status> {
    crate::control_plane::client::focused_node_subnet(subnet)
        .map(focused_node_subnet_to_proto)
        .map_err(|error| Status::internal(error.to_string()))
}

fn focused_node_subnet_to_proto(
    subnet: klights_leader_api::NodeSubnet,
) -> generated::NodeSubnetObject {
    generated::NodeSubnetObject {
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

fn dataplane_metadata_to_proto(
    metadata: DataplanePeerMetadata,
) -> std::result::Result<generated::DataplaneMetadataObject, Status> {
    let metadata = crate::control_plane::client::focused_dataplane(metadata)
        .map_err(|error| Status::internal(error.to_string()))?;
    Ok(generated::DataplaneMetadataObject {
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
    })
}

fn pod_cleanup_intent_to_proto(
    intent: crate::control_plane::client::PodCleanupIntent,
) -> std::result::Result<generated::PodCleanupIntentObject, Status> {
    let (node_name, namespace, pod_name, pod_uid, reason, resource_version, created_at_ms, pod) =
        intent.into_parts();
    let pod_data_json = serde_json::to_vec(pod.data.as_ref()).map_err(|error| {
        pod_cleanup_intent_error_to_status(
            crate::control_plane::client::PodCleanupIntentError::corrupt_intent(format!(
                "encode Pod cleanup intent snapshot for {namespace}/{pod_name} uid={pod_uid}: {error}"
            )),
        )
    })?;
    Ok(generated::PodCleanupIntentObject {
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
    error: crate::control_plane::client::ProjectedServiceAccountTokenError,
) -> Status {
    use crate::control_plane::client::ProjectedServiceAccountTokenError as Error;
    let message = error.to_string();
    match error {
        Error::InvalidRequest { .. } => Status::invalid_argument(message),
        Error::NotLeader => Status::failed_precondition("not raft leader"),
        Error::Unauthorized | Error::BindingMismatch { .. } => Status::permission_denied(message),
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

fn pod_cleanup_intent_error_to_status(
    error: crate::control_plane::client::PodCleanupIntentError,
) -> Status {
    use crate::control_plane::client::PodCleanupIntentError as Error;
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

fn resource_from_event(event: &crate::watch::WatchEvent) -> crate::datastore::Resource {
    crate::datastore::Resource::from_watch_event_ref(event)
}

fn watch_event_type(event: &crate::watch::WatchEvent) -> &'static str {
    match event.event_type {
        crate::watch::EventType::Added => "ADDED",
        crate::watch::EventType::Modified => "MODIFIED",
        crate::watch::EventType::Deleted => "DELETED",
        crate::watch::EventType::Bookmark => "BOOKMARK",
        crate::watch::EventType::Error => "ERROR",
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
) -> generated::WatchEvent {
    let hb = crate::watch::WatchEvent::bookmark_typed(last_rv, api_version, kind);
    let resource = resource_from_event(&hb);
    generated::WatchEvent {
        event_type: watch_event_type(&hb).to_string(),
        resource: Some(resource_to_proto(&resource)),
        resume_position: Some(watch_replay_position_to_proto(resume_position)),
    }
}

fn watch_cursor_error_to_status(err: crate::watch::WatchCursorError, accepted_rv: i64) -> Status {
    match err {
        crate::watch::WatchCursorError::Expired => watch_replay_expired_status(
            accepted_rv,
            format!(
                "WatchResources replay window expired: resume rv {accepted_rv} requires relist"
            ),
        ),
        crate::watch::WatchCursorError::Replay(err) => {
            Status::internal(format!("replay WatchResources failed: {err}"))
        }
        crate::watch::WatchCursorError::Closed => Status::unavailable("watch stream closed"),
    }
}

fn watch_event_should_stream(
    event: &crate::watch::WatchEvent,
    req: &generated::WatchResourcesRequest,
) -> bool {
    if event.event_type == crate::watch::EventType::Bookmark {
        return true;
    }
    if WatchEventSelection::new(&req.api_version, &req.kind)
        .namespace(req.namespace.as_deref())
        .label_selector(req.label_selector.as_deref())
        .field_selector(req.field_selector.as_deref())
        .matches(event)
    {
        return true;
    }
    event.event_type == crate::watch::EventType::Modified && selector_may_change_membership(req)
}

fn selector_may_change_membership(req: &generated::WatchResourcesRequest) -> bool {
    if req
        .label_selector
        .as_deref()
        .is_some_and(|selector| !selector.trim().is_empty())
    {
        return true;
    }
    let Some(selector) = req
        .field_selector
        .as_deref()
        .filter(|selector| !selector.trim().is_empty())
    else {
        return false;
    };
    selector.split(',').any(|requirement| {
        let (field, operator) = if let Some((field, _)) = requirement.split_once("!=") {
            (field.trim(), "!=")
        } else if let Some((field, _)) = requirement.split_once("==") {
            (field.trim(), "==")
        } else if let Some((field, _)) = requirement.split_once('=') {
            (field.trim(), "=")
        } else {
            (requirement.trim(), "")
        };
        match field {
            "metadata.name" | "metadata.namespace" => false,
            "spec.nodeName" => operator == "!=",
            _ => true,
        }
    })
}

/// Complete on any raft leadership-signal version change (or when its sender is
/// dropped). Even a demote/promote flap invalidates the leader-fresh watch
/// sample. `watch::Receiver::changed` is cancel-safe, so dropping the pending
/// future when the broadcast receive wins loses no transition.
async fn watch_leadership_lost(leader_rx: &mut tokio::sync::watch::Receiver<bool>) {
    if !*leader_rx.borrow() {
        return;
    }
    let _ = leader_rx.changed().await;
}

fn watch_target_for_request(req: &generated::WatchResourcesRequest) -> WatchTarget {
    if let Some(namespace) = req.namespace.as_ref() {
        return WatchTarget::namespaced_in_namespace(
            req.api_version.clone(),
            req.kind.clone(),
            namespace.clone(),
        );
    }
    // Share the canonical scope list with the datastore/API scope logic instead
    // of maintaining a second list here, which drifted out of sync and
    // misclassified cluster-scoped kinds such as CSIDriver, CSINode,
    // VolumeAttachment, IngressClass, IPAddress, ServiceCIDR, and the
    // validating admission policy types as namespaced on the watch replay path.
    if crate::datastore::sqlite::scope::is_namespaced(&req.kind) {
        WatchTarget::namespaced(req.api_version.clone(), req.kind.clone())
    } else {
        WatchTarget::cluster(req.api_version.clone(), req.kind.clone())
    }
}

fn watch_delivery_scope_for_request(
    req: &generated::WatchResourcesRequest,
) -> crate::watch::WatchDeliveryScope {
    if let Some(namespace) = req.namespace.as_ref() {
        return crate::watch::WatchDeliveryScope::Namespaced(namespace.clone());
    }
    if crate::datastore::sqlite::scope::is_namespaced(&req.kind) {
        crate::watch::WatchDeliveryScope::NamespacedAll
    } else {
        crate::watch::WatchDeliveryScope::Cluster
    }
}

async fn refresh_joining_node_from_dataplane(
    db: &dyn DatastoreBackend,
    query: &dyn klights_leader_api::LeaderResourceQuery,
    node_status: &dyn klights_leader_api::LeaderNodeLifecycleStatus,
    dataplane: &DataplanePeerMetadata,
) -> Result<()> {
    refresh_node_routing_metadata_from_dataplane(db, dataplane).await?;
    publish_joining_node_external_ip(query, node_status, dataplane).await
}

async fn refresh_node_routing_metadata_from_dataplane(
    db: &dyn DatastoreBackend,
    dataplane: &DataplanePeerMetadata,
) -> Result<()> {
    // Registration/dataplane projection only: this full-object CAS stamps
    // routing metadata but deliberately does not mutate Node status. ExternalIP
    // is published separately through the exact UID+RV status capability.
    let Some(resource) = db
        .get_resource("v1", "Node", None, &dataplane.node_name)
        .await?
    else {
        return Ok(());
    };
    let mut data = (*resource.data).clone();
    if !crate::kubelet::node::stamp_node_routing_metadata_from_store(
        db,
        &dataplane.node_name,
        &mut data,
    )
    .await?
    {
        return Ok(());
    }
    db.update_resource_with_preconditions(
        "v1",
        "Node",
        None,
        &dataplane.node_name,
        data,
        ResourcePreconditions::from_resource(&resource),
    )
    .await?;
    Ok(())
}

async fn publish_joining_node_external_ip(
    query: &dyn klights_leader_api::LeaderResourceQuery,
    node_status: &dyn klights_leader_api::LeaderNodeLifecycleStatus,
    dataplane: &DataplanePeerMetadata,
) -> Result<()> {
    let get = klights_leader_api::node_get_request(
        &dataplane.node_name,
        klights_leader_api::ResourceQueryConsistency::LeaderFresh,
    )?;
    let Some(resource) = query.get_resource(get).await? else {
        return Ok(());
    };
    let mut data = (*resource.data).clone();
    if !crate::kubelet::node::set_node_external_ip(&mut data, &dataplane.endpoint.to_string()) {
        return Ok(());
    }
    let status = data
        .get("status")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let request = klights_leader_api::NodeLifecycleStatusRequest::try_new(
        crate::datastore::command::StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Node".to_string(),
            namespace: None,
            name: dataplane.node_name.clone(),
            status,
            expected_rv: Some(resource.resource_version),
            preconditions: ResourcePreconditions::uid_and_resource_version(
                resource.uid,
                resource.resource_version,
            ),
            observed_status_stamp: None,
        },
    )?;
    node_status.submit_node_lifecycle_status(request).await?;
    Ok(())
}

async fn refresh_local_node_external_ip_from_observed_endpoint(
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
    crate::kubelet::node::publish_node_external_ip_if_changed(
        query,
        node_status,
        node_name,
        &endpoint_ip.to_string(),
    )
    .await
}

async fn node_has_external_ip(db: &dyn DatastoreBackend, node_name: &str) -> Result<bool> {
    let Some(node) = db.get_resource("v1", "Node", None, node_name).await? else {
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
    db: &dyn DatastoreBackend,
    response: JoinResponse,
) -> std::result::Result<generated::JoinResponse, Status> {
    match response {
        JoinResponse::Accepted {
            cluster_id,
            leader_epoch,
            current_rv,
        } => {
            let peers = dataplane_peers_from_db(db).await?;
            Ok(generated::JoinResponse {
                result: Some(generated::join_response::Result::Accepted(
                    generated::JoinAccepted {
                        cluster_id,
                        leader_epoch,
                        current_rv,
                        peers,
                    },
                )),
            })
        }
        JoinResponse::Rejected { reason } => Ok(generated::JoinResponse {
            result: Some(generated::join_response::Result::Rejected(
                generated::JoinRejected { reason },
            )),
        }),
    }
}

async fn dataplane_peers_from_db(
    db: &dyn DatastoreBackend,
) -> std::result::Result<Vec<generated::DataplanePeer>, Status> {
    let mut subnets = db
        .list_peer_subnets("")
        .await
        .map_err(|err| Status::internal(err.to_string()))?;
    subnets.sort_by(|a, b| a.node_name.as_str().cmp(b.node_name.as_str()));

    let mut peers = Vec::with_capacity(subnets.len());
    for subnet in subnets {
        let node_name = subnet.node_name.to_string();
        let Some(dataplane) = db
            .get_node_dataplane(&node_name)
            .await
            .map_err(|err| Status::internal(err.to_string()))?
        else {
            continue;
        };
        peers.push(generated::DataplanePeer {
            node_name,
            pod_cidr: subnet.subnet.to_string(),
            public_key: dataplane
                .public_key
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
            endpoint: dataplane.endpoint.to_string(),
            port: dataplane.port.map(u32::from).unwrap_or_default(),
            mode: dataplane.mode.as_str().to_string(),
            encryption: dataplane.encryption.as_str().to_string(),
        });
    }
    Ok(peers)
}

// `forwarded_*_to_proto` helpers removed in T6 along with the legacy
// ForwardCommand wire path.

fn node_exec_sync_request_to_proto(request: NodeExecSyncRequest) -> generated::NodeExecSyncRequest {
    generated::NodeExecSyncRequest {
        request_id: request.request_id,
        node_name: request.node_name,
        namespace: request.namespace,
        pod_name: request.pod_name,
        container_id: request.container_id,
        command: request.command,
        timeout_seconds: request.timeout_seconds,
    }
}

fn node_exec_sync_response_from_proto(
    response: generated::NodeExecSyncResponse,
) -> NodeExecSyncResponse {
    NodeExecSyncResponse {
        request_id: response.request_id,
        stdout: response.stdout,
        stderr: response.stderr,
        exit_code: response.exit_code,
        error: response.error,
    }
}

fn node_exec_request_to_proto(request: NodeExecRequest) -> generated::NodeExecRequest {
    generated::NodeExecRequest {
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
        attach: request.attach,
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

fn pod_log_request_to_proto(request: PodLogRequest) -> generated::PodLogRequest {
    generated::PodLogRequest {
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

fn pod_log_response_from_proto(response: generated::PodLogResponse) -> PodLogResponse {
    PodLogResponse {
        request_id: response.request_id,
        log_content: response.log_content,
        error: response.error,
        fin: response.fin,
    }
}

fn node_metrics_request_to_proto(request: NodeMetricsRequest) -> generated::NodeMetricsRequest {
    generated::NodeMetricsRequest {
        request_id: request.request_id,
        node_name: request.node_name,
        pod_uids: request.pod_uids,
    }
}

fn node_metrics_response_from_proto(
    response: generated::NodeMetricsResponse,
) -> NodeMetricsResponse {
    NodeMetricsResponse {
        request_id: response.request_id,
        node_name: response.node_name,
        node: response
            .node
            .map(|node| crate::metrics::NodeMetricsNodeSample {
                cpu_nanos: node.cpu_nanos,
                memory_bytes: node.memory_bytes,
            }),
        pods: response
            .pods
            .into_iter()
            .map(|pod| NodeMetricsPodSample {
                namespace: pod.namespace,
                name: pod.name,
                uid: pod.uid,
                containers: pod
                    .containers
                    .into_iter()
                    .map(|container| NodeMetricsContainerSample {
                        name: container.name,
                        cpu_nanos: container.cpu_nanos,
                        memory_bytes: container.memory_bytes,
                    })
                    .collect(),
            })
            .collect(),
        error: response.error,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::datastore::backend::{DatastoreBackend, DatastoreHandle};
    use crate::datastore::command::{
        COMMAND_CODEC_VERSION, CommandId, CommandMeta, StorageCommand,
    };
    use crate::datastore::types::{ResourcePreconditions, WatchReplayPosition};
    use crate::replication::grpc::generated::replication_client::ReplicationClient;
    use crate::replication::grpc::generated::replication_server::Replication;
    use crate::replication::grpc::raft_rpc::{
        ControlplaneJoinHandler, ControlplaneJoinOutcome, RaftRpcRouterError,
    };
    use crate::replication::grpc::{
        generated::{self, JoinRequest, JoinRole, MetadataRequest, SnapshotRequest},
        server::validate_join_metadata,
    };
    use crate::replication::protocol::ReplicationEntry;
    use crate::replication::service::ReplicationService;
    use crate::task_supervisor::{TaskCategoryConfig, TaskSupervisor};
    use tokio::sync::mpsc;
    use tonic_reflection::pb::v1::{
        ServerReflectionRequest, server_reflection_client::ServerReflectionClient,
        server_reflection_request, server_reflection_response,
    };

    #[test]
    fn resource_proto_body_uses_authoritative_identity_and_resource_version() {
        let resource = crate::datastore::Resource {
            id: 7,
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "canonical".to_string(),
            uid: "uid-canonical".to_string(),
            resource_version: 42,
            data: Arc::new(serde_json::json!({
                "apiVersion": "stale/v1",
                "kind": "Stale",
                "metadata": {
                    "namespace": "stale",
                    "name": "stale",
                    "uid": "uid-stale"
                }
            })),
        };

        let wire = super::resource_to_proto(&resource);
        let body: serde_json::Value =
            serde_json::from_slice(&wire.data_json).expect("resource JSON");
        assert_eq!(body["apiVersion"], "v1");
        assert_eq!(body["kind"], "ConfigMap");
        assert_eq!(body["metadata"]["namespace"], "default");
        assert_eq!(body["metadata"]["name"], "canonical");
        assert_eq!(body["metadata"]["uid"], "uid-canonical");
        assert_eq!(body["metadata"]["resourceVersion"], "42");
    }

    #[derive(Default)]
    struct RecordingNodeLifecycleStatus {
        requests: Mutex<Vec<klights_leader_api::NodeLifecycleStatusRequest>>,
    }

    impl RecordingNodeLifecycleStatus {
        fn take_request(&self) -> klights_leader_api::NodeLifecycleStatusRequest {
            self.requests
                .lock()
                .expect("recording Node lifecycle status mutex poisoned")
                .pop()
                .expect("one Node lifecycle status request")
        }
    }

    impl klights_leader_api::LeaderNodeLifecycleStatus for RecordingNodeLifecycleStatus {
        fn submit_node_lifecycle_status(
            &self,
            request: klights_leader_api::NodeLifecycleStatusRequest,
        ) -> klights_leader_api::NodeLifecycleStatusFuture<
            '_,
            klights_leader_api::NodeLifecycleStatusResult,
        > {
            let resource_version = request.resource_version() + 1;
            self.requests
                .lock()
                .expect("recording Node lifecycle status mutex poisoned")
                .push(request);
            Box::pin(async move {
                Ok(klights_leader_api::NodeLifecycleStatusResult::Updated { resource_version })
            })
        }
    }

    #[test]
    fn node_lease_renew_time_skew_allows_100_seconds_but_rejects_101() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-07-07T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let past_100 = crate::utils::k8s_time_format(now - chrono::Duration::seconds(100));
        let future_100 = crate::utils::k8s_time_format(now + chrono::Duration::seconds(100));
        let past_101 = crate::utils::k8s_time_format(now - chrono::Duration::seconds(101));
        let future_101 = crate::utils::k8s_time_format(now + chrono::Duration::seconds(101));

        super::validate_node_lease_renew_time_skew(&past_100, now)
            .expect("100s past skew is accepted at boundary");
        super::validate_node_lease_renew_time_skew(&future_100, now)
            .expect("100s future skew is accepted at boundary");
        assert_eq!(
            super::validate_node_lease_renew_time_skew(&past_101, now)
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            super::validate_node_lease_renew_time_skew(&future_101, now)
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }

    fn valid_join() -> JoinRequest {
        JoinRequest {
            token: "token".to_string(),
            node_name: "worker-1".to_string(),
            role: JoinRole::Worker as i32,
            dataplane_public_key: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string(),
            dataplane_endpoint: "192.0.2.10".to_string(),
            dataplane_port: 51_820,
            dataplane_mode: "root".to_string(),
            dataplane_encryption: "enabled".to_string(),
        }
    }

    fn watch_request_for_kind(
        kind: &str,
        namespace: Option<&str>,
    ) -> generated::WatchResourcesRequest {
        generated::WatchResourcesRequest {
            api_version: "v1".to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            field_selector: None,
            start_resource_version: 0,
            label_selector: None,
            start_watch_replay_position: None,
        }
    }

    #[test]
    fn watch_target_classifies_cluster_scoped_kinds() {
        use crate::datastore::types::WatchTargetScope;

        // Kinds that the old hand-maintained gRPC list omitted and therefore
        // misclassified as namespaced on the watch replay path.
        for kind in [
            "CSIDriver",
            "CSINode",
            "VolumeAttachment",
            "IngressClass",
            "IPAddress",
            "ServiceCIDR",
            "ValidatingAdmissionPolicy",
            "ValidatingAdmissionPolicyBinding",
            // Kinds the old list already covered must keep classifying correctly.
            "Node",
            "Namespace",
            "ClusterRole",
            "PriorityLevelConfiguration",
        ] {
            let target = super::watch_target_for_request(&watch_request_for_kind(kind, None));
            assert_eq!(
                target.scope,
                WatchTargetScope::Cluster,
                "{kind} must be classified as cluster-scoped"
            );
        }
    }

    #[test]
    fn watch_target_classifies_namespaced_kinds() {
        use crate::datastore::types::WatchTargetScope;

        let target = super::watch_target_for_request(&watch_request_for_kind("ConfigMap", None));
        assert_eq!(target.scope, WatchTargetScope::Namespaced(None));

        let scoped = super::watch_target_for_request(&watch_request_for_kind(
            "ConfigMap",
            Some("kube-system"),
        ));
        assert_eq!(
            scoped.scope,
            WatchTargetScope::Namespaced(Some("kube-system".to_string()))
        );
    }

    #[test]
    fn watch_server_prefilter_forwards_only_possible_selector_transitions() {
        let modified = crate::watch::WatchEvent::modified(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "other",
                "labels": {"app": "other"},
                "resourceVersion": "2"
            },
            "spec": {"nodeName": "worker-b"},
            "status": {"phase": "Pending"}
        }));

        let mut label_req = watch_pods_request();
        label_req.label_selector = Some("app=selected".to_string());
        assert!(super::watch_event_should_stream(&modified, &label_req));

        let mut mutable_field_req = watch_pods_request();
        mutable_field_req.field_selector = Some("status.phase=Running".to_string());
        assert!(super::watch_event_should_stream(
            &modified,
            &mutable_field_req
        ));

        let mut node_req = watch_pods_request();
        node_req.field_selector = Some("spec.nodeName=worker-a".to_string());
        assert!(
            !super::watch_event_should_stream(&modified, &node_req),
            "nonmatching immutable nodeName traffic must stay server-filtered"
        );

        let bound_to_excluded_node = crate::watch::WatchEvent::modified(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "newly-bound",
                "resourceVersion": "3"
            },
            "spec": {"nodeName": "worker-a"}
        }));
        let mut node_not_equal_req = watch_pods_request();
        node_not_equal_req.field_selector = Some("spec.nodeName!=worker-a".to_string());
        assert!(
            super::watch_event_should_stream(&bound_to_excluded_node, &node_not_equal_req),
            "nodeName inequality can leave membership when an unassigned Pod binds"
        );

        let nonmatching_added = crate::watch::WatchEvent::added((*modified.object).clone());
        assert!(
            !super::watch_event_should_stream(&nonmatching_added, &label_req),
            "nonmatching ADDED events cannot be leave transitions"
        );
    }

    async fn create_scoped_token_for_test(
        db: &dyn DatastoreBackend,
        token: &str,
        scope: crate::bootstrap::bootstrap_token::BootstrapTokenScope,
    ) {
        crate::bootstrap::bootstrap_token::create_scoped_bootstrap_token_secret_for_test(
            db, scope, token,
        )
        .await
        .unwrap();
    }

    async fn grpc_test_server_with_signing_ca(
        db: DatastoreHandle,
        namespace: &str,
    ) -> super::GrpcReplicationServer {
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let (ca_cert, ca_key, ca_cert_pem, ca_key_pem) = crate::auth::generate_ca_full().unwrap();
        let ca_cert_path = crate::paths::ca_cert_path(namespace);
        let ca_key_path = crate::paths::ca_key_path(namespace);
        let service_account_key_path = crate::paths::service_account_signing_key_path(namespace);
        std::fs::create_dir_all(ca_cert_path.parent().unwrap()).unwrap();
        std::fs::write(&ca_cert_path, ca_cert_pem).unwrap();
        std::fs::write(&ca_key_path, ca_key_pem).unwrap();
        std::fs::write(&service_account_key_path, "service-account-signing-key").unwrap();
        drop((ca_cert, ca_key));

        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor));
        super::GrpcReplicationServer::new(service, db).with_namespace(namespace)
    }

    fn sample_entry(rv: i64) -> ReplicationEntry {
        ReplicationEntry {
            command: StorageCommand::CreateNamespace {
                name: format!("streamed-{rv}"),
                data: serde_json::json!({"metadata": {"name": format!("streamed-{rv}")}}),
            },
            meta: CommandMeta {
                command_id: CommandId(format!("grpc-server-stream-{rv}")),
                codec_version: COMMAND_CODEC_VERSION,
                resource_version: rv,
                uid: None,
                timestamp_ms: 0,
                authoring_node: "leader".to_string(),
            },
        }
    }

    async fn grpc_test_server(
        db: DatastoreHandle,
    ) -> (String, Arc<ReplicationService>, tokio::task::JoinHandle<()>) {
        grpc_test_server_with_dispatcher(db, None).await
    }

    async fn grpc_test_server_with_dispatcher(
        db: DatastoreHandle,
        controller_dispatcher: Option<Arc<crate::controller_dispatcher::ControllerDispatcher>>,
    ) -> (String, Arc<ReplicationService>, tokio::task::JoinHandle<()>) {
        grpc_test_server_full(db, controller_dispatcher, None).await
    }

    async fn grpc_test_server_full(
        db: DatastoreHandle,
        controller_dispatcher: Option<Arc<crate::controller_dispatcher::ControllerDispatcher>>,
        controlplane_join_handler: Option<Arc<dyn ControlplaneJoinHandler>>,
    ) -> (String, Arc<ReplicationService>, tokio::task::JoinHandle<()>) {
        grpc_test_server_full_with_node_cert(
            db,
            controller_dispatcher,
            controlplane_join_handler,
            None,
        )
        .await
    }

    async fn grpc_test_server_with_node_cert(
        db: DatastoreHandle,
        node_name: &str,
    ) -> (String, Arc<ReplicationService>, tokio::task::JoinHandle<()>) {
        grpc_test_server_full_with_node_cert(db, None, None, Some(node_name.to_string())).await
    }

    async fn grpc_test_server_full_with_node_cert(
        db: DatastoreHandle,
        controller_dispatcher: Option<Arc<crate::controller_dispatcher::ControllerDispatcher>>,
        controlplane_join_handler: Option<Arc<dyn ControlplaneJoinHandler>>,
        injected_node_cert: Option<String>,
    ) -> (String, Arc<ReplicationService>, tokio::task::JoinHandle<()>) {
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor));
        let node_status = Arc::new(crate::control_plane::client::local::LocalApiClient::new(
            db.clone(),
            "test-leader".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ));
        let app = super::mount_service_full(
            axum::Router::new(),
            service.clone(),
            db,
            controller_dispatcher,
            None,
            None,
            controlplane_join_handler,
            "",
            None,
            None,
            Some(node_status.clone()),
            None,
            Some(node_status),
            crate::replication::grpc::transport_policy::GrpcTransportPolicy::shared_default(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            loop {
                let (stream, remote_addr) = match listener.accept().await {
                    Ok(conn) => conn,
                    Err(_) => break,
                };
                let local_addr = stream.local_addr().ok();
                let app = app.clone();
                let injected_node_cert = injected_node_cert.clone();
                tokio::spawn(async move {
                    use tower::ServiceExt;

                    let io = hyper_util::rt::TokioIo::new(stream);
                    let service = hyper::service::service_fn(move |mut req| {
                        if let Some(node_name) = injected_node_cert.as_deref() {
                            req.extensions_mut()
                                .insert(crate::auth::TlsClientCertificate(node_client_cert_der(
                                    node_name,
                                    &["system:nodes"],
                                )));
                        }
                        super::insert_tonic_tcp_connect_info(
                            &mut req,
                            local_addr,
                            Some(remote_addr),
                        );
                        app.clone().oneshot(req)
                    });
                    let _ = hyper_util::server::conn::auto::Builder::new(
                        hyper_util::rt::TokioExecutor::new(),
                    )
                    .serve_connection_with_upgrades(io, service)
                    .await;
                });
            }
        });
        (endpoint, service, handle)
    }

    /// bug-grpc A1/B2: serve the replication gRPC service built with an
    /// explicit [`GrpcTransportPolicy`] so a test can shrink `max_message_bytes`
    /// (decode-limit test) or `watch_heartbeat_interval` (per-stream heartbeat
    /// test). `injected_node_cert` injects a node client cert so handlers that
    /// require steady-state auth (e.g. `watch_resources`) accept the request;
    /// `None` leaves auth to fail (the decode-size check fires first regardless).
    async fn grpc_test_server_with_policy(
        db: DatastoreHandle,
        policy: Arc<crate::replication::grpc::transport_policy::GrpcTransportPolicy>,
        injected_node_cert: Option<&str>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let injected_node_cert = injected_node_cert.map(str::to_string);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor));
        let app = super::mount_service_full_with_policy(
            axum::Router::new(),
            service,
            db,
            None,
            None,
            None,
            None,
            "",
            None,
            None,
            None,
            None,
            None,
            policy,
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            loop {
                let (stream, remote_addr) = match listener.accept().await {
                    Ok(conn) => conn,
                    Err(_) => break,
                };
                let local_addr = stream.local_addr().ok();
                let app = app.clone();
                let injected_node_cert = injected_node_cert.clone();
                tokio::spawn(async move {
                    use tower::ServiceExt;
                    let io = hyper_util::rt::TokioIo::new(stream);
                    let service = hyper::service::service_fn(move |mut req| {
                        if let Some(node_name) = injected_node_cert.as_deref() {
                            req.extensions_mut()
                                .insert(crate::auth::TlsClientCertificate(node_client_cert_der(
                                    node_name,
                                    &["system:nodes"],
                                )));
                        }
                        super::insert_tonic_tcp_connect_info(
                            &mut req,
                            local_addr,
                            Some(remote_addr),
                        );
                        app.clone().oneshot(req)
                    });
                    let _ = hyper_util::server::conn::auto::Builder::new(
                        hyper_util::rt::TokioExecutor::new(),
                    )
                    .serve_connection_with_upgrades(io, service)
                    .await;
                });
            }
        });
        (endpoint, handle)
    }

    /// bug-grpc A1: the server now applies `GrpcTransportPolicy::max_message_bytes`
    /// to the tonic service decode limit (previously unset → unbounded). A
    /// request larger than the configured limit must be rejected at decode,
    /// before the handler runs.
    #[tokio::test]
    async fn server_rejects_request_over_policy_message_limit() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        let policy = crate::replication::grpc::transport_policy::GrpcTransportPolicy {
            max_message_bytes: 1024,
            ..Default::default()
        }
        .shared();
        let (endpoint, handle) = grpc_test_server_with_policy(db, policy, None).await;

        let channel = tonic::transport::Endpoint::from_shared(endpoint)
            .unwrap()
            .connect()
            .await
            .unwrap();
        // Default client encoding limit is unbounded, so the oversized request
        // is sent; the server must reject it on decode.
        let mut client = ReplicationClient::new(channel);
        let oversized = tonic::Request::new(generated::ApplyOutboxRequest {
            idempotency_key: "k".to_string(),
            operation: "create".to_string(),
            payload_proto: vec![0u8; 8 * 1024],
            authoring_node: "worker-1".to_string(),
            client_id: "client".to_string(),
            stream_id: 1,
            stream_seq: 1,
        });
        let result = client.apply_outbox(oversized).await;
        assert!(
            result.is_err(),
            "server must reject a request exceeding the policy message limit, got {result:?}"
        );

        // A small request is not rejected on size grounds (it fails auth /
        // leadership later, but never with an OutOfRange size error).
        let small = tonic::Request::new(generated::ApplyOutboxRequest {
            idempotency_key: "k".to_string(),
            operation: "create".to_string(),
            payload_proto: vec![0u8; 16],
            authoring_node: "worker-1".to_string(),
            client_id: "client".to_string(),
            stream_id: 1,
            stream_seq: 1,
        });
        if let Err(status) = client.apply_outbox(small).await {
            assert_ne!(
                status.code(),
                tonic::Code::OutOfRange,
                "a small request must not be rejected for message size"
            );
        }
        handle.abort();
    }

    /// bug-grpc B2: a quiet *matching* internal watch stream must still emit a
    /// per-stream BOOKMARK heartbeat even while the global broadcast carries a
    /// continuous stream of *non-matching* events. The old code reset the
    /// heartbeat deadline on every loop iteration, so unrelated traffic starved
    /// the bookmark and the worker idle-reconnected every window.
    #[tokio::test]
    async fn watch_stream_emits_bookmark_during_stream_local_silence_under_nonmatching_traffic() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        db.create_namespace("hb", serde_json::json!({"metadata": {"name": "hb"}}))
            .await
            .unwrap();
        let policy = crate::replication::grpc::transport_policy::GrpcTransportPolicy {
            watch_heartbeat_interval: std::time::Duration::from_millis(300),
            ..Default::default()
        }
        .shared();
        let (endpoint, handle) =
            grpc_test_server_with_policy(db.clone(), policy, Some("worker-1")).await;

        let channel = tonic::transport::Endpoint::from_shared(endpoint)
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = ReplicationClient::new(channel);
        let mut watch = client
            .watch_resources(generated::WatchResourcesRequest {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: None,
                field_selector: None,
                start_resource_version: 0,
                label_selector: None,
                start_watch_replay_position: None,
            })
            .await
            .unwrap()
            .into_inner();

        // Continuous NON-matching (Secret) traffic, faster than the heartbeat
        // interval, for the duration of the test.
        let noise_db = db.clone();
        let noise = tokio::spawn(async move {
            for i in 0..60 {
                let name = format!("noise-{i}");
                let _ = noise_db
                    .create_resource(
                        "v1",
                        "Secret",
                        Some("hb"),
                        &name,
                        serde_json::json!({
                            "apiVersion": "v1",
                            "kind": "Secret",
                            "metadata": {"name": name, "namespace": "hb"},
                        }),
                    )
                    .await;
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        });

        // Despite the Secret firehose, the quiet ConfigMap stream must emit a
        // BOOKMARK within a few heartbeat windows.
        let mut saw_bookmark = false;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(std::time::Duration::from_secs(1), watch.message()).await {
                Ok(Ok(Some(event))) => {
                    if event.event_type == "BOOKMARK" {
                        saw_bookmark = true;
                        break;
                    }
                }
                Ok(Ok(None)) | Ok(Err(_)) => break,
                Err(_) => continue,
            }
        }
        noise.abort();
        handle.abort();
        assert!(
            saw_bookmark,
            "a quiet matching watch stream must emit a per-stream BOOKMARK under non-matching traffic"
        );
    }

    /// Worker pod watches are field-selected by `spec.nodeName`. A signal for a
    /// higher-RV non-matching Pod must replay the durable Pod history from the
    /// worker stream's accepted RV, so a lower-RV matching Pod already present
    /// in `watch_events` is delivered instead of being skipped behind the
    /// non-matching high-water mark.
    #[tokio::test]
    async fn watch_stream_replays_lower_matching_pod_on_nonmatching_high_rv_signal() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        db.create_namespace(
            "default",
            serde_json::json!({"metadata": {"name": "default"}}),
        )
        .await
        .unwrap();
        let scheduled_here = db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "scheduled-here",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "scheduled-here",
                        "uid": "uid-here"
                    },
                    "spec": {
                        "nodeName": "worker-1",
                        "containers": [{"name": "app", "image": "pause"}]
                    },
                    "status": {"phase": "Pending"}
                }),
            )
            .await
            .unwrap();
        let other_node = db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "other-node",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "other-node",
                        "uid": "uid-other"
                    },
                    "spec": {
                        "nodeName": "worker-2",
                        "containers": [{"name": "app", "image": "pause"}]
                    },
                    "status": {"phase": "Pending"}
                }),
            )
            .await
            .unwrap();
        assert!(
            other_node.resource_version > scheduled_here.resource_version,
            "test setup requires the nonmatching Pod to carry the higher RV"
        );
        let policy = crate::replication::grpc::transport_policy::GrpcTransportPolicy {
            watch_heartbeat_interval: std::time::Duration::from_secs(30),
            ..Default::default()
        }
        .shared();
        let (endpoint, handle) =
            grpc_test_server_with_policy(db.clone(), policy, Some("worker-1")).await;

        let channel = tonic::transport::Endpoint::from_shared(endpoint)
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = ReplicationClient::new(channel);
        let mut watch = client
            .watch_resources(generated::WatchResourcesRequest {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: None,
                field_selector: Some("spec.nodeName=worker-1".to_string()),
                start_resource_version: 0,
                label_selector: None,
                start_watch_replay_position: None,
            })
            .await
            .unwrap()
            .into_inner();

        db.broadcast_watch_event(crate::datastore::PendingWatchEvent::from_event(
            crate::watch::WatchEvent::modified(serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "other-node",
                    "uid": "uid-other",
                    "resourceVersion": other_node.resource_version.to_string()
                },
                "spec": {
                    "nodeName": "worker-2",
                    "containers": [{"name": "app", "image": "pause"}]
                },
                "status": {"phase": "Pending"}
            })),
        ));

        let event = tokio::time::timeout(std::time::Duration::from_secs(1), watch.message())
            .await
            .expect("matching lower-RV event must not be dropped after higher-RV non-match")
            .expect("watch stream should stay healthy")
            .expect("watch stream should yield the matching event");
        handle.abort();

        assert_eq!(event.event_type, "ADDED");
        let resource = event.resource.expect("watch event should carry a resource");
        assert_eq!(resource.name, "scheduled-here");
        assert_eq!(resource.resource_version, scheduled_here.resource_version);
    }

    // --- watch_resources leadership-termination tests (issue #4) -----------

    async fn grpc_leader_server(
        is_leader: bool,
    ) -> (
        super::GrpcReplicationServer,
        tokio::sync::watch::Sender<bool>,
    ) {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        grpc_leader_server_with_db(db, is_leader).await
    }

    async fn grpc_leader_server_with_db(
        db: DatastoreHandle,
        is_leader: bool,
    ) -> (
        super::GrpcReplicationServer,
        tokio::sync::watch::Sender<bool>,
    ) {
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor));
        let (leader_tx, is_leader_rx) = tokio::sync::watch::channel(is_leader);
        let grpc = super::GrpcReplicationServer::new(service, db).with_leader_gate(is_leader_rx);
        (grpc, leader_tx)
    }

    fn watch_pods_request() -> generated::WatchResourcesRequest {
        generated::WatchResourcesRequest {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: None,
            field_selector: None,
            start_resource_version: 0,
            label_selector: None,
            start_watch_replay_position: None,
        }
    }

    fn watch_configmaps_from_rv(start_resource_version: i64) -> generated::WatchResourcesRequest {
        generated::WatchResourcesRequest {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: None,
            field_selector: None,
            start_resource_version,
            label_selector: None,
            start_watch_replay_position: None,
        }
    }

    fn configmap(name: &str) -> serde_json::Value {
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "namespace": "default",
                "name": name,
            },
            "data": {"key": name},
        })
    }

    async fn configmap_replay_db() -> (DatastoreHandle, i64) {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        db.create_namespace(
            "default",
            serde_json::json!({"metadata": {"name": "default"}}),
        )
        .await
        .unwrap();
        let first = db
            .create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "resume-old",
                configmap("resume-old"),
            )
            .await
            .unwrap();
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "resume-new",
            configmap("resume-new"),
        )
        .await
        .unwrap();
        let resume_rv = (first.resource_version - 1).max(1);
        assert!(
            resume_rv < first.resource_version,
            "test setup must start before the first ConfigMap event"
        );
        (db, resume_rv)
    }

    #[tokio::test]
    async fn watch_resources_replays_positive_resume_rv_through_signal_cursor() {
        use futures::StreamExt;

        let (db, resume_rv) = configmap_replay_db().await;
        let (grpc, _leader_tx) = grpc_leader_server_with_db(db, true).await;
        let mut stream = grpc
            .watch_resources(request_with_node_client_cert(
                watch_configmaps_from_rv(resume_rv),
                "worker-1",
            ))
            .await
            .expect("leader should accept watch")
            .into_inner();

        let event = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
            .await
            .expect("positive-rv watch should replay retained events")
            .expect("watch stream should yield")
            .expect("watch stream should stay healthy");
        assert_eq!(event.event_type, "ADDED");
        let first_resume_position = event
            .resume_position
            .clone()
            .expect("replayed event must carry a resume position");
        assert!(
            first_resume_position.event_id > 0,
            "legacy scalar-RV watches must be upgraded to a composite resume position"
        );
        let resource = event.resource.expect("watch event should carry resource");
        assert_eq!(resource.name, "resume-old");
        assert!(resource.resource_version > resume_rv);

        drop(stream);
        let mut resumed_request = watch_configmaps_from_rv(resource.resource_version);
        resumed_request.start_watch_replay_position = Some(first_resume_position);
        let mut resumed = grpc
            .watch_resources(request_with_node_client_cert(resumed_request, "worker-1"))
            .await
            .expect("composite continuation should open")
            .into_inner();
        let next = tokio::time::timeout(std::time::Duration::from_secs(1), resumed.next())
            .await
            .expect("composite continuation should replay the unread suffix")
            .expect("continuation stream should yield")
            .expect("continuation stream should stay healthy");
        assert_eq!(
            next.resource.expect("event resource").name,
            "resume-new",
            "per-event resume position must neither duplicate nor skip read-ahead events"
        );
    }

    #[tokio::test]
    async fn watch_resources_replays_retained_event_for_legacy_zero_resume_without_new_signal() {
        use futures::StreamExt;

        let (db, _resume_rv) = configmap_replay_db().await;
        let (grpc, _leader_tx) = grpc_leader_server_with_db(db, true).await;
        let mut stream = grpc
            .watch_resources(request_with_node_client_cert(
                watch_configmaps_from_rv(0),
                "worker-1",
            ))
            .await
            .expect("leader should accept legacy zero-rv watch")
            .into_inner();

        let event = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
            .await
            .expect("initial positioned replay must not wait for an unrelated later signal")
            .expect("watch stream should yield")
            .expect("watch stream should stay healthy");
        assert_eq!(event.event_type, "ADDED");
        assert_eq!(
            event.resource.expect("watch event resource").name,
            "resume-old"
        );
        assert!(
            event
                .resume_position
                .expect("new server must upgrade legacy watch to exact position")
                .event_id
                > 0
        );
    }

    #[tokio::test]
    async fn watch_resources_maps_expired_signal_replay_to_out_of_range() {
        use futures::StreamExt;

        let (db, resume_rv) = configmap_replay_db().await;
        db.gc_watch_events(1, 1000)
            .await
            .expect("watch-events gc should run");
        let (grpc, _leader_tx) = grpc_leader_server_with_db(db, true).await;
        let mut stream = grpc
            .watch_resources(request_with_node_client_cert(
                watch_configmaps_from_rv(resume_rv),
                "worker-1",
            ))
            .await
            .expect("leader should accept watch")
            .into_inner();

        let status = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
            .await
            .expect("expired replay should produce a stream error")
            .expect("watch stream should yield an error")
            .expect_err("expired replay must be surfaced as an error");
        assert_eq!(status.code(), tonic::Code::OutOfRange);
        assert!(
            crate::replication::grpc::is_watch_replay_expired_status(&status),
            "status should carry the typed replay-expired marker, got {status:?}"
        );
    }

    #[tokio::test]
    async fn watch_resources_rejects_establishment_when_not_raft_leader() {
        let (grpc, _leader_tx) = grpc_leader_server(false).await;
        let status = match grpc
            .watch_resources(request_with_node_client_cert(
                watch_pods_request(),
                "worker-1",
            ))
            .await
        {
            Ok(_) => panic!("a non-leader must reject watch establishment"),
            Err(status) => status,
        };
        assert_eq!(
            status.code(),
            tonic::Code::FailedPrecondition,
            "establishment on a non-leader must fail with FailedPrecondition"
        );
    }

    #[tokio::test]
    async fn resource_get_and_list_reject_non_leader_and_raw_invalid_requests() {
        let (follower, _leader_tx) = grpc_leader_server(false).await;
        let get = generated::GetResourceRequest {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web".to_string(),
        };
        let list = generated::ListResourcesRequest {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            label_selector: None,
            field_selector: None,
            limit: None,
            continue_token: None,
        };
        assert_eq!(
            follower
                .get_resource(request_with_node_client_cert(get.clone(), "worker-1"))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            follower
                .list_resources(request_with_node_client_cert(list, "worker-1"))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );

        let (leader, _leader_tx) = grpc_leader_server(true).await;
        let mut invalid_get = get;
        invalid_get.api_version.clear();
        assert_eq!(
            leader
                .get_resource(request_with_node_client_cert(invalid_get, "worker-1"))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        let mut invalid_watch = watch_pods_request();
        invalid_watch.start_resource_version = -1;
        assert_eq!(
            leader
                .watch_resources(request_with_node_client_cert(invalid_watch, "worker-1"))
                .await
                .err()
                .expect("negative watch cursor must be rejected")
                .code(),
            tonic::Code::InvalidArgument
        );
    }

    #[tokio::test]
    async fn network_topology_queries_reject_non_leader() {
        let (grpc, _leader_tx) = grpc_leader_server(false).await;
        let rejected = grpc
            .get_node_subnet(request_with_node_client_cert(
                generated::GetNodeSubnetRequest {
                    node_name: "worker-1".to_string(),
                },
                "worker-1",
            ))
            .await
            .expect_err("a non-leader must reject topology queries");
        assert_eq!(rejected.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn node_certificate_may_allocate_subnet_only_for_itself() {
        let (grpc, _leader_tx) = grpc_leader_server(true).await;
        let status = grpc
            .allocate_node_subnet(request_with_node_client_cert(
                generated::AllocateNodeSubnetRequest {
                    node_name: "worker-2".to_string(),
                    cluster_cidr: "10.42.0.0/16".to_string(),
                    node_ip: "192.0.2.22".to_string(),
                },
                "worker-1",
            ))
            .await
            .expect_err("a worker node must not allocate a peer subnet");
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn controlplane_certificate_may_allocate_peer_subnet() {
        let (grpc, _leader_tx) = grpc_leader_server(true).await;
        let response = grpc
            .allocate_node_subnet(request_with_controlplane_client_cert(
                generated::AllocateNodeSubnetRequest {
                    node_name: "worker-2".to_string(),
                    cluster_cidr: "10.42.0.0/16".to_string(),
                    node_ip: "192.0.2.22".to_string(),
                },
                "controlplane-1",
            ))
            .await
            .expect("control-plane authority may allocate a peer subnet")
            .into_inner();
        let subnet = response.subnet.expect("allocation payload");
        assert_eq!(subnet.node_name, "worker-2");
        assert_eq!(subnet.subnet, "10.42.0.0/24");
    }

    #[tokio::test]
    async fn subnet_exhaustion_maps_to_resource_exhausted() {
        let (grpc, _leader_tx) = grpc_leader_server(true).await;
        for node_name in ["worker-1", "worker-2"] {
            let result = grpc
                .allocate_node_subnet(request_with_controlplane_client_cert(
                    generated::AllocateNodeSubnetRequest {
                        node_name: node_name.to_string(),
                        cluster_cidr: "10.42.0.0/24".to_string(),
                        node_ip: "192.0.2.22".to_string(),
                    },
                    "controlplane-1",
                ))
                .await;
            if node_name == "worker-1" {
                result.expect("the only /24 must be allocated");
            } else {
                assert_eq!(
                    result
                        .expect_err("the second allocation must exhaust the CIDR")
                        .code(),
                    tonic::Code::ResourceExhausted
                );
            }
        }
    }

    #[tokio::test]
    async fn watch_resources_terminates_promptly_on_leadership_loss() {
        use futures::StreamExt;
        let (grpc, leader_tx) = grpc_leader_server(true).await;
        let mut stream = match grpc
            .watch_resources(request_with_node_client_cert(
                watch_pods_request(),
                "worker-1",
            ))
            .await
        {
            Ok(response) => response.into_inner(),
            Err(status) => panic!("the leader must accept watch establishment: {status:?}"),
        };

        // Depose this node mid-stream: leadership flips away.
        leader_tx.send(false).expect("leader signal still live");

        // The stream must terminate (None) promptly once leadership is lost,
        // instead of idling up to the ~60s client idle watchdog on a deposed,
        // silent broadcaster. Before the fix the loop had no leadership select
        // and would wait on the broadcast recv indefinitely here.
        match tokio::time::timeout(std::time::Duration::from_secs(2), stream.next()).await {
            Ok(None) => { /* stream ended cleanly on leadership loss */ }
            Ok(Some(Ok(_))) => {
                panic!("stream should terminate on leadership loss, not yield an event")
            }
            Ok(Some(Err(_))) => panic!("stream should end cleanly, not error"),
            Err(_) => panic!("stream did not terminate within 2s of leadership loss"),
        }
    }

    struct AcceptingControlplaneJoinHandler;

    fn test_node_registration_proto(git_commit: &str) -> generated::NodeRegistrationSnapshot {
        generated::NodeRegistrationSnapshot {
            cpu_count: 6,
            memory_ki: 12 * 1024 * 1024,
            architecture: "test-arch".to_string(),
            operating_system: "linux".to_string(),
            os_image: "Test Linux".to_string(),
            kernel_version: "6.1-test".to_string(),
            container_runtime_version: "containerd://1.7.0".to_string(),
            kubelet_version: "v1.34.0-test".to_string(),
            git_commit: git_commit.to_string(),
            node_mode: "root".to_string(),
        }
    }

    #[test]
    fn controlplane_node_registration_rejects_empty_or_invalid_host_facts() {
        let mut cases = Vec::new();
        let mut zero_cpu = test_node_registration_proto("joiner");
        zero_cpu.cpu_count = 0;
        cases.push(("zero-cpu", zero_cpu));
        let mut zero_memory = test_node_registration_proto("joiner");
        zero_memory.memory_ki = 0;
        cases.push(("zero-memory", zero_memory));
        let mut empty_kernel = test_node_registration_proto("joiner");
        empty_kernel.kernel_version.clear();
        cases.push(("empty-kernel", empty_kernel));
        let mut invalid_mode = test_node_registration_proto("joiner");
        invalid_mode.node_mode = "leader-root".to_string();
        cases.push(("invalid-mode", invalid_mode));

        for (name, registration) in cases {
            assert!(
                super::validate_controlplane_node_registration(registration).is_err(),
                "{name} must be rejected"
            );
        }
    }

    #[async_trait::async_trait]
    impl ControlplaneJoinHandler for AcceptingControlplaneJoinHandler {
        async fn join(
            &self,
            request: crate::replication::grpc::raft_rpc::ControlplaneJoinRequest,
        ) -> Result<ControlplaneJoinOutcome, RaftRpcRouterError> {
            Ok(ControlplaneJoinOutcome::Accepted {
                voter_count_after: if request.as_learner { 1 } else { 2 },
                admitted_as_learner: request.as_learner,
                ca_cert_pem: String::new(),
                encrypted_ca_key: Vec::new(),
                ca_key_nonce: [0u8; 12],
            })
        }

        // Permissive test double: treat callers as existing members so node-cert
        // (rejoin) JoinAsControlplane is accepted without a token. Token-gating
        // and non-member rejection are exercised by dedicated handlers/tests.
        async fn is_controlplane_member(&self, _node_name: &str) -> bool {
            true
        }
    }

    /// Test double whose callers are never existing members — exercises the
    /// "worker / first-time caller without a controlplane token is rejected"
    /// path on JoinAsControlplane.
    struct NonMemberControlplaneJoinHandler;

    #[async_trait::async_trait]
    impl ControlplaneJoinHandler for NonMemberControlplaneJoinHandler {
        async fn join(
            &self,
            request: crate::replication::grpc::raft_rpc::ControlplaneJoinRequest,
        ) -> Result<ControlplaneJoinOutcome, RaftRpcRouterError> {
            Ok(ControlplaneJoinOutcome::Accepted {
                voter_count_after: if request.as_learner { 1 } else { 2 },
                admitted_as_learner: request.as_learner,
                ca_cert_pem: String::new(),
                encrypted_ca_key: Vec::new(),
                ca_key_nonce: [0u8; 12],
            })
        }

        async fn is_controlplane_member(&self, _node_name: &str) -> bool {
            false
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct RecordedControlplaneJoin {
        node_id: u64,
        addr: String,
        node_name: String,
        as_learner: bool,
        node_internal_ip: Option<String>,
        node_registration:
            Option<crate::replication::grpc::raft_rpc::RemoteNodeRegistrationSnapshot>,
        legacy_node_git_commit: Option<String>,
    }

    #[derive(Default)]
    struct RecordingControlplaneJoinHandler {
        calls: Mutex<Vec<RecordedControlplaneJoin>>,
    }

    impl RecordingControlplaneJoinHandler {
        fn calls(&self) -> Vec<RecordedControlplaneJoin> {
            self.calls
                .lock()
                .expect("recording join handler mutex poisoned")
                .clone()
        }
    }

    #[async_trait::async_trait]
    impl ControlplaneJoinHandler for RecordingControlplaneJoinHandler {
        async fn join(
            &self,
            request: crate::replication::grpc::raft_rpc::ControlplaneJoinRequest,
        ) -> Result<ControlplaneJoinOutcome, RaftRpcRouterError> {
            let crate::replication::grpc::raft_rpc::ControlplaneJoinRequest {
                node_id,
                addr,
                node_name,
                as_learner,
                supported_features: _,
                node_internal_ip,
                node_registration,
                legacy_node_git_commit,
            } = request;
            self.calls
                .lock()
                .expect("recording join handler mutex poisoned")
                .push(RecordedControlplaneJoin {
                    node_id,
                    addr,
                    node_name,
                    as_learner,
                    node_internal_ip,
                    node_registration,
                    legacy_node_git_commit,
                });
            Ok(ControlplaneJoinOutcome::Accepted {
                voter_count_after: if as_learner { 1 } else { 2 },
                admitted_as_learner: as_learner,
                ca_cert_pem: String::new(),
                encrypted_ca_key: Vec::new(),
                ca_key_nonce: [0u8; 12],
            })
        }

        async fn is_controlplane_member(&self, _node_name: &str) -> bool {
            true
        }
    }

    async fn open_connect(
        endpoint: &str,
        join: JoinRequest,
    ) -> (
        mpsc::Sender<generated::FollowerMessage>,
        tonic::codec::Streaming<generated::LeaderMessage>,
    ) {
        let channel = tonic::transport::Endpoint::from_shared(endpoint.to_string())
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = ReplicationClient::new(channel);
        let (tx, mut rx) = mpsc::channel(8);
        tx.send(generated::FollowerMessage {
            payload: Some(generated::follower_message::Payload::Join(join)),
        })
        .await
        .unwrap();
        let outbound = async_stream::stream! {
            while let Some(message) = rx.recv().await {
                yield message;
            }
        };
        let inbound = client
            .connect(tonic::Request::new(outbound))
            .await
            .unwrap()
            .into_inner();
        (tx, inbound)
    }

    fn request_with_join_token<T>(message: T, token: &str) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        request.metadata_mut().insert(
            crate::replication::grpc::JOIN_TOKEN_METADATA_KEY,
            token.parse().unwrap(),
        );
        request
    }

    fn request_with_node_client_cert<T>(message: T, node_name: &str) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        request
            .extensions_mut()
            .insert(crate::auth::TlsClientCertificate(node_client_cert_der(
                node_name,
                &["system:nodes"],
            )));
        request
    }

    /// A control-plane node client certificate: `system:nodes` plus the
    /// `system:controlplanes` group that the controlplane-token-gated bootstrap
    /// stamps. This is what authorizes raft consensus RPCs.
    fn request_with_controlplane_client_cert<T>(message: T, node_name: &str) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        request
            .extensions_mut()
            .insert(crate::auth::TlsClientCertificate(node_client_cert_der(
                node_name,
                &["system:nodes", "system:controlplanes"],
            )));
        request
    }

    fn node_client_cert_der(node_name: &str, orgs: &[&str]) -> Vec<u8> {
        use rcgen::{CertificateParams, DnType, KeyPair};

        let mut params = CertificateParams::default();
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, format!("system:node:{node_name}"));
        // Match production encoding: groups are a single comma-joined O attribute
        // (rcgen's DistinguishedName cannot hold two O RDNs). `user_from_cert`
        // splits them back apart.
        if !orgs.is_empty() {
            params
                .distinguished_name
                .push(DnType::OrganizationName, orgs.join(","));
        }
        let key_pair = KeyPair::generate().unwrap();
        params.self_signed(&key_pair).unwrap().der().to_vec()
    }

    fn request_with_admin_cert<T>(message: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        request
            .extensions_mut()
            .insert(crate::auth::TlsClientCertificate(node_client_cert_der(
                "admin",
                &["system:masters"],
            )));
        request
    }

    // ── CRIT-2: NodeRestriction on node-scoped RPCs ──

    #[test]
    fn caller_node_authority_token_only_is_unrestricted() {
        // No client certificate: not a system:nodes identity, so the raw
        // classifier is not node-restricted. Production node-scoped handlers
        // reject no-cert callers before this helper is used.
        let request = tonic::Request::new(());
        assert!(matches!(
            super::caller_node_authority(&request),
            super::CallerAuthority::Unrestricted
        ));
    }

    #[test]
    fn caller_node_authority_extracts_node_name() {
        let request = request_with_node_client_cert((), "worker-7");
        match super::caller_node_authority(&request) {
            super::CallerAuthority::Node(name) => assert_eq!(name, "worker-7"),
            super::CallerAuthority::Unrestricted => panic!("node cert must be node-bound"),
        }
    }

    #[test]
    fn caller_node_authority_admin_is_unrestricted() {
        let request = request_with_admin_cert(());
        assert!(matches!(
            super::caller_node_authority(&request),
            super::CallerAuthority::Unrestricted
        ));
    }

    #[test]
    fn enforce_node_authority_matrix() {
        assert!(
            super::enforce_node_authority(&super::CallerAuthority::Unrestricted, "any").is_ok()
        );
        assert!(
            super::enforce_node_authority(&super::CallerAuthority::Node("w1".to_string()), "w1")
                .is_ok()
        );
        let err =
            super::enforce_node_authority(&super::CallerAuthority::Node("w1".to_string()), "w2")
                .expect_err("node may not act for another node");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    // ── CRIT-1: raft RPC authentication ──

    async fn raft_test_server() -> super::GrpcReplicationServer {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor));
        super::GrpcReplicationServer::new(service, db)
    }

    #[tokio::test]
    async fn raft_append_entries_rejects_unauthenticated() {
        let grpc = raft_test_server().await;
        // No bootstrap token and no client certificate.
        let status = grpc
            .raft_append_entries(tonic::Request::new(generated::RaftAppendEntriesRequest {
                payload: vec![],
            }))
            .await
            .expect_err("unauthenticated raft RPC must be rejected");
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn submit_resource_command_rejects_worker_certificate_before_decode() {
        let grpc = raft_test_server().await;
        let status = grpc
            .submit_resource_command(request_with_node_client_cert(
                generated::SubmitResourceCommandRequest {
                    command_protobuf: Vec::new(),
                },
                "worker-a",
            ))
            .await
            .expect_err("worker identity must not submit generic resource commands");
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn submit_resource_command_rejects_follower_before_decode() {
        let (_tx, rx) = tokio::sync::watch::channel(false);
        let grpc = raft_test_server().await.with_leader_gate(rx);
        let status = grpc
            .submit_resource_command(request_with_controlplane_client_cert(
                generated::SubmitResourceCommandRequest {
                    command_protobuf: Vec::new(),
                },
                "cp2",
            ))
            .await
            .expect_err("follower must reject before decoding or mutating");
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn submit_resource_command_rejects_generic_pod_hard_delete() {
        let grpc = raft_test_server().await;
        let command = crate::datastore::command::StorageCommand::DeleteResource {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web".to_string(),
            preconditions: crate::datastore::ResourcePreconditions::uid("pod-uid"),
        };
        let status = grpc
            .submit_resource_command(request_with_controlplane_client_cert(
                generated::SubmitResourceCommandRequest {
                    command_protobuf: crate::datastore::command::encode_command_protobuf(&command)
                        .expect("encode command"),
                },
                "cp1",
            ))
            .await
            .expect_err("generic Pod hard delete must fail closed");
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn submit_resource_command_accepts_controlplane_create() {
        let grpc = raft_test_server().await;
        let command = crate::datastore::command::StorageCommand::CreateResource {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "settings".to_string(),
            data: serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"namespace": "default", "name": "settings"}
            }),
        };
        let response = grpc
            .submit_resource_command(request_with_controlplane_client_cert(
                generated::SubmitResourceCommandRequest {
                    command_protobuf: crate::datastore::command::encode_command_protobuf(&command)
                        .expect("encode command"),
                },
                "cp1",
            ))
            .await
            .expect("control-plane create")
            .into_inner();
        assert!(matches!(
            response.result,
            Some(generated::submit_resource_command_response::Result::Resource(resource))
                if resource.kind == "ConfigMap" && resource.name == "settings"
        ));
    }

    #[tokio::test]
    async fn raft_append_entries_rejects_bootstrap_token() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let token = crate::bootstrap::cluster_meta::read_join_token(db.as_ref())
            .await
            .unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor));
        let grpc = super::GrpcReplicationServer::new(service, db);

        let status = grpc
            .raft_append_entries(request_with_join_token(
                generated::RaftAppendEntriesRequest { payload: vec![] },
                &token,
            ))
            .await
            .expect_err("bootstrap token must not authenticate raft RPCs");
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn raft_vote_rejects_unauthenticated() {
        let grpc = raft_test_server().await;
        let status = grpc
            .raft_vote(tonic::Request::new(generated::RaftVoteRequest {
                payload: vec![],
            }))
            .await
            .expect_err("unauthenticated raft vote must be rejected");
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn raft_append_entries_accepts_controlplane_group_certificate() {
        // A node certificate carrying the `system:controlplanes` group (minted
        // only via the controlplane-token-gated bootstrap) authorizes the raft
        // peer; the RPC then proceeds (returning a router-disabled *result*, not
        // a Status error). No controlplane join handler / membership oracle is
        // wired — authorization is anchored on the certificate, so a control
        // plane authorizes without first having to learn raft membership.
        let grpc = raft_test_server().await;
        let resp = grpc
            .raft_append_entries(request_with_controlplane_client_cert(
                generated::RaftAppendEntriesRequest { payload: vec![] },
                "controlplane-2",
            ))
            .await
            .expect("system:controlplanes node cert must authorize the raft peer");
        assert!(resp.into_inner().result.is_some());
    }

    #[tokio::test]
    async fn raft_consensus_accepts_freshly_joining_controlplane_without_membership() {
        // Regression: a freshly-joining control plane has an empty raft
        // membership view and is not yet anyone's "current member", yet it must
        // accept the leader's append-entries / install-snapshot to catch up.
        // Because authorization is cert-anchored on `system:controlplanes` and
        // does NOT consult the (empty) local membership oracle, the bootstrap is
        // not deadlocked.
        let grpc = raft_test_server().await;
        let resp = grpc
            .raft_install_snapshot(request_with_controlplane_client_cert(
                generated::RaftInstallSnapshotRequest { payload: vec![] },
                "controlplane-3",
            ))
            .await
            .expect("a joining control plane must accept consensus RPCs to bootstrap");
        assert!(resp.into_inner().result.is_some());
    }

    #[tokio::test]
    async fn raft_vote_rejects_worker_node_certificate() {
        // A worker holds a valid `system:node:`/`system:nodes` client cert but
        // NOT the `system:controlplanes` group (its cert is signed via the
        // Kubernetes CSR API, which never grants that group). It must not be able
        // to drive consensus RPCs — otherwise it could send a `vote` with an
        // inflated term and force the leader to step down (control-plane DoS) or
        // otherwise manipulate consensus.
        let grpc = raft_test_server().await;
        let status = grpc
            .raft_vote(request_with_node_client_cert(
                generated::RaftVoteRequest { payload: vec![] },
                "worker-1",
            ))
            .await
            .expect_err("a worker node cert must not authorize a raft vote");
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn raft_append_entries_rejects_worker_node_certificate() {
        let grpc = raft_test_server().await;
        let status = grpc
            .raft_append_entries(request_with_node_client_cert(
                generated::RaftAppendEntriesRequest { payload: vec![] },
                "worker-1",
            ))
            .await
            .expect_err("a worker node cert must not authorize raft append-entries");
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn raft_install_snapshot_rejects_admin_certificate() {
        let grpc = raft_test_server().await;
        let status = grpc
            .raft_install_snapshot(request_with_admin_cert(
                generated::RaftInstallSnapshotRequest { payload: vec![] },
            ))
            .await
            .expect_err("admin cert must not authenticate the raft peer");
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn renew_node_lease_rejects_mismatched_node() {
        let db = crate::datastore::test_support::in_memory().await;
        let db: DatastoreHandle = Arc::new(db);
        let tracker = Arc::new(crate::node_lease_tracker::NodeLeaseTracker::new_for_test(
            chrono::DateTime::parse_from_rfc3339("2026-05-25T00:00:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
        ));
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor));
        let grpc = super::GrpcReplicationServer::new_with_node_lease_tracker(
            service,
            db.clone(),
            tracker.clone(),
        );

        // worker-1's cert tries to renew worker-2's lease.
        let status = grpc
            .renew_node_lease(request_with_node_client_cert(
                generated::RenewNodeLeaseRequest {
                    node_name: "worker-2".to_string(),
                    renew_time: "2026-05-25T00:00:10Z".to_string(),
                    lease_duration_seconds: 50,
                },
                "worker-1",
            ))
            .await
            .expect_err("node must not renew another node's lease");
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        // worker-2 must not have been touched.
        assert!(tracker.observed("worker-2").await.is_none());
    }

    #[tokio::test]
    async fn node_effect_rpc_rejects_nonpositive_lease_duration_before_tracker_mutation() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        let tracker = Arc::new(crate::node_lease_tracker::NodeLeaseTracker::new());
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor));
        let grpc =
            super::GrpcReplicationServer::new_with_node_lease_tracker(service, db, tracker.clone());

        for duration in [0, -1] {
            let status = grpc
                .renew_node_lease(request_with_node_client_cert(
                    generated::RenewNodeLeaseRequest {
                        node_name: "worker-1".to_string(),
                        renew_time: crate::utils::k8s_time_format(chrono::Utc::now()),
                        lease_duration_seconds: duration,
                    },
                    "worker-1",
                ))
                .await
                .expect_err("nonpositive lease duration must be rejected");
            assert_eq!(status.code(), tonic::Code::InvalidArgument);
        }
        assert!(tracker.observed("worker-1").await.is_none());
    }

    #[tokio::test]
    async fn outbox_terminal_decision_rpc_rejects_smuggling_and_malformed_rows_in_order() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        let created = db
            .create_resource(
                "v1",
                "Node",
                None,
                "worker-1",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Node",
                    "metadata": {"name": "worker-1", "uid": "node-uid-1"}
                }),
            )
            .await
            .expect("create worker Node");
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor));
        let grpc = super::GrpcReplicationServer::new(service, db.clone());
        let command = StorageCommand::PatchResource {
            api_version: "v1".to_string(),
            kind: "Node".to_string(),
            namespace: None,
            name: "worker-1".to_string(),
            patch_kind: crate::datastore::types::PatchKind::Merge,
            patch: serde_json::json!({"metadata": {"labels": {"smuggled": "true"}}}),
            preconditions: ResourcePreconditions::uid("node-uid-1"),
            strict_resource_version: false,
        };
        let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
            .encode_protobuf()
            .expect("encode payload");

        let rejected = grpc
            .apply_outbox(request_with_node_client_cert(
                generated::ApplyOutboxRequest {
                    idempotency_key: "smuggled-node-patch".to_string(),
                    operation: crate::kubelet::outbox::payload::OutboxOperation::NodeStatus
                        .as_str()
                        .to_string(),
                    payload_proto: payload,
                    authoring_node: "worker-1".to_string(),
                    client_id: "worker-1".to_string(),
                    stream_id: 1,
                    stream_seq: 1,
                },
                "worker-1",
            ))
            .await
            .expect("durably consumed authorization failures use the typed response");
        assert_eq!(
            rejected.into_inner().error_type.as_deref(),
            Some("ConflictTerminal")
        );
        assert_eq!(
            db.list_outbox_stream_watermarks().await.unwrap()[0].stream_seq,
            1,
            "RPC authorization rejection must durably consume sequence one"
        );

        let stored = db
            .get_resource("v1", "Node", None, "worker-1")
            .await
            .expect("read Node")
            .expect("Node exists");
        assert_eq!(stored.resource_version, created.resource_version);
        assert!(stored.data.pointer("/metadata/labels/smuggled").is_none());

        let valid_status_payload = || {
            crate::kubelet::outbox::payload::OutboxPayload::from_command(
                StorageCommand::UpdateStatus {
                    api_version: "v1".to_string(),
                    kind: "Node".to_string(),
                    namespace: None,
                    name: "worker-1".to_string(),
                    status: serde_json::json!({"conditions": []}),
                    expected_rv: None,
                    preconditions: ResourcePreconditions::uid("node-uid-1"),
                    observed_status_stamp: None,
                },
            )
            .encode_protobuf()
            .expect("encode valid RPC Node status")
        };
        grpc.apply_outbox(request_with_node_client_cert(
            generated::ApplyOutboxRequest {
                idempotency_key: "valid-after-smuggling".to_string(),
                operation: crate::kubelet::outbox::payload::OutboxOperation::NodeStatus
                    .as_str()
                    .to_string(),
                payload_proto: valid_status_payload(),
                authoring_node: "worker-1".to_string(),
                client_id: "worker-1".to_string(),
                stream_id: 1,
                stream_seq: 2,
            },
            "worker-1",
        ))
        .await
        .expect("sequence two applies after RPC terminal authorization decision");

        let malformed = grpc
            .apply_outbox(request_with_node_client_cert(
                generated::ApplyOutboxRequest {
                    idempotency_key: "malformed-rpc-row".to_string(),
                    operation: crate::kubelet::outbox::payload::OutboxOperation::NodeStatus
                        .as_str()
                        .to_string(),
                    payload_proto: vec![0xff, 0x00, 0x81],
                    authoring_node: "worker-1".to_string(),
                    client_id: "worker-1".to_string(),
                    stream_id: 1,
                    stream_seq: 3,
                },
                "worker-1",
            ))
            .await
            .expect("durably consumed malformed delivery uses the typed response");
        assert_eq!(
            malformed.into_inner().error_type.as_deref(),
            Some("InvalidRequest")
        );
        assert_eq!(
            db.list_outbox_stream_watermarks().await.unwrap()[0].stream_seq,
            3,
            "malformed RPC sequence must receive a durable terminal decision"
        );
        grpc.apply_outbox(request_with_node_client_cert(
            generated::ApplyOutboxRequest {
                idempotency_key: "valid-after-malformed".to_string(),
                operation: crate::kubelet::outbox::payload::OutboxOperation::NodeStatus
                    .as_str()
                    .to_string(),
                payload_proto: valid_status_payload(),
                authoring_node: "worker-1".to_string(),
                client_id: "worker-1".to_string(),
                stream_id: 1,
                stream_seq: 4,
            },
            "worker-1",
        ))
        .await
        .expect("sequence four applies after malformed RPC terminal decision");
    }

    #[tokio::test]
    async fn node_effect_rpc_rejects_wrong_uid_before_committed_apply() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        let created = db
            .create_resource(
                "v1",
                "Node",
                None,
                "worker-1",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Node",
                    "metadata": {"name": "worker-1", "uid": "node-uid-1"},
                    "status": {"conditions": []}
                }),
            )
            .await
            .expect("create worker Node");
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor));
        let grpc = super::GrpcReplicationServer::new(service, db.clone());
        let command = StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Node".to_string(),
            namespace: None,
            name: "worker-1".to_string(),
            status: serde_json::json!({"conditions": [{"type": "Ready", "status": "True"}]}),
            expected_rv: None,
            preconditions: ResourcePreconditions::uid("wrong-node-uid"),
            observed_status_stamp: None,
        };
        let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
            .encode_protobuf()
            .expect("encode payload");

        let response = grpc
            .apply_outbox(request_with_node_client_cert(
                generated::ApplyOutboxRequest {
                    idempotency_key: "wrong-node-uid".to_string(),
                    operation: crate::kubelet::outbox::payload::OutboxOperation::NodeStatus
                        .as_str()
                        .to_string(),
                    payload_proto: payload,
                    authoring_node: "worker-1".to_string(),
                    client_id: "worker-1".to_string(),
                    stream_id: 1,
                    stream_seq: 1,
                },
                "worker-1",
            ))
            .await
            .expect("durably consumed UID mismatch uses the typed response");
        assert_eq!(
            response.into_inner().error_type.as_deref(),
            Some("UidMismatch")
        );
        let stored = db
            .get_resource("v1", "Node", None, "worker-1")
            .await
            .expect("read Node")
            .expect("Node exists");
        assert_eq!(stored.resource_version, created.resource_version);
    }

    #[tokio::test]
    async fn outbox_transport_contract_rpc_rejects_unvalidated_stream_identity() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor));
        let grpc = super::GrpcReplicationServer::new(service, db.clone());
        let command = StorageCommand::UpdateNodeDataplane {
            node_name: "worker-1".to_string(),
            mode: "root".to_string(),
            encryption: "enabled".to_string(),
            public_key: Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
            endpoint: "192.0.2.10".to_string(),
            port: Some(7679),
        };
        let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
            .encode_protobuf()
            .expect("encode dataplane payload");

        let status = grpc
            .apply_outbox(request_with_node_client_cert(
                generated::ApplyOutboxRequest {
                    idempotency_key: String::new(),
                    operation: "NodeDataplane".to_string(),
                    payload_proto: payload,
                    authoring_node: "worker-1".to_string(),
                    client_id: String::new(),
                    stream_id: 0,
                    stream_seq: 0,
                },
                "worker-1",
            ))
            .await
            .expect_err("raw RPC must pass the focused request constructor before apply");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(
            db.get_node_dataplane("worker-1").await.unwrap().is_none(),
            "invalid delivery identity must be rejected before datastore or Raft work",
        );
    }

    #[tokio::test]
    async fn cleanup_intent_list_requires_current_leader_and_same_node_authority() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor));
        let (_leader_tx, follower_rx) = tokio::sync::watch::channel(false);
        let follower = super::GrpcReplicationServer::new(service.clone(), db.clone())
            .with_leader_gate(follower_rx);

        let status = follower
            .list_pod_cleanup_intents_for_node(request_with_node_client_cert(
                generated::ListPodCleanupIntentsForNodeRequest {
                    node_name: "worker-1".to_string(),
                },
                "worker-1",
            ))
            .await
            .expect_err("follower must not serve cleanup intents");
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), "not raft leader");

        let leader = super::GrpcReplicationServer::new(service, db);
        let status = leader
            .list_pod_cleanup_intents_for_node(request_with_node_client_cert(
                generated::ListPodCleanupIntentsForNodeRequest {
                    node_name: "worker-2".to_string(),
                },
                "worker-1",
            ))
            .await
            .expect_err("node must not list another node's cleanup intents");
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn cleanup_intent_ack_requires_current_leader_before_mutation() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor));
        let (_leader_tx, follower_rx) = tokio::sync::watch::channel(false);
        let follower = super::GrpcReplicationServer::new(service, db).with_leader_gate(follower_rx);

        let status = follower
            .delete_pod_cleanup_intent(request_with_node_client_cert(
                generated::DeletePodCleanupIntentRequest {
                    node_name: "worker-1".to_string(),
                    namespace: "default".to_string(),
                    pod_name: "web".to_string(),
                    pod_uid: "pod-uid".to_string(),
                    reason: "NodeLost".to_string(),
                },
                "worker-1",
            ))
            .await
            .expect_err("follower must not acknowledge cleanup intents");
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), "not raft leader");
    }

    #[tokio::test]
    async fn projected_token_rpc_requires_exact_bound_pod_uid_and_node() {
        let grpc = raft_test_server().await;
        let status = grpc
            .projected_service_account_token(request_with_node_client_cert(
                generated::ProjectedServiceAccountTokenRequest {
                    namespace: "default".to_string(),
                    service_account_name: "default".to_string(),
                    audiences: vec!["api".to_string()],
                    expiration_seconds: 3_600,
                    bound_pod_name: Some("web".to_string()),
                    bound_pod_uid: None,
                    bound_node_name: Some("worker-1".to_string()),
                    bound_node_uid: None,
                },
                "worker-1",
            ))
            .await
            .expect_err("node-originated issuance requires the exact bound Pod UID");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn renew_node_lease_rejects_renew_time_skew_over_100_seconds() {
        let db = crate::datastore::test_support::in_memory().await;
        let db: DatastoreHandle = Arc::new(db);
        let tracker = Arc::new(crate::node_lease_tracker::NodeLeaseTracker::new_for_test(
            chrono::Utc::now(),
        ));
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor));
        let grpc = super::GrpcReplicationServer::new_with_node_lease_tracker(
            service,
            db.clone(),
            tracker.clone(),
        );

        let skewed =
            crate::utils::k8s_time_format(chrono::Utc::now() - chrono::Duration::seconds(101));
        let status = grpc
            .renew_node_lease(request_with_node_client_cert(
                generated::RenewNodeLeaseRequest {
                    node_name: "worker-1".to_string(),
                    renew_time: skewed,
                    lease_duration_seconds: 50,
                },
                "worker-1",
            ))
            .await
            .expect_err("heartbeat renewTime skew over 100 seconds must be rejected");

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(
            tracker.observed("worker-1").await.is_none(),
            "rejected skewed heartbeat must not update in-memory lease state"
        );
    }

    #[tokio::test]
    async fn apply_outbox_rejects_node_dataplane_for_mismatched_author() {
        let db = crate::datastore::test_support::in_memory().await;
        let db: DatastoreHandle = Arc::new(db);
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor));
        let grpc = super::GrpcReplicationServer::new(service, db.clone());
        let command = StorageCommand::UpdateNodeDataplane {
            node_name: "worker-2".to_string(),
            mode: "root".to_string(),
            encryption: "enabled".to_string(),
            public_key: Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
            endpoint: "192.0.2.20".to_string(),
            port: Some(7679),
        };
        let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
            .encode_protobuf()
            .unwrap();

        let response = grpc
            .apply_outbox(request_with_node_client_cert(
                generated::ApplyOutboxRequest {
                    idempotency_key: "dataplane-worker-2-from-worker-1".to_string(),
                    operation: crate::kubelet::outbox::payload::OutboxOperation::NodeDataplane
                        .as_str()
                        .to_string(),
                    payload_proto: payload,
                    authoring_node: "worker-1".to_string(),
                    client_id: "client".to_string(),
                    stream_id: 1,
                    stream_seq: 1,
                },
                "worker-1",
            ))
            .await
            .expect("durably consumed author mismatch uses a typed response")
            .into_inner();

        assert_eq!(response.error_type.as_deref(), Some("ConflictTerminal"));
        assert!(
            db.get_node_dataplane("worker-2").await.unwrap().is_none(),
            "rejected dataplane update must not write peer metadata"
        );
    }

    #[test]
    fn validate_join_metadata_accepts_enabled_root_and_rootless() {
        let root = validate_join_metadata(&valid_join()).unwrap();
        assert_eq!(root.node_name, "worker-1");

        let mut rootless = valid_join();
        rootless.dataplane_mode = "rootless".to_string();
        assert!(validate_join_metadata(&rootless).is_ok());
    }

    #[test]
    fn validate_join_metadata_rejects_missing_enabled_wireguard_fields() {
        let mut missing_key = valid_join();
        missing_key.dataplane_public_key.clear();
        assert!(
            validate_join_metadata(&missing_key)
                .unwrap_err()
                .to_string()
                .contains("public key")
        );

        let mut missing_endpoint = valid_join();
        missing_endpoint.dataplane_endpoint.clear();
        assert!(
            validate_join_metadata(&missing_endpoint)
                .unwrap_err()
                .to_string()
                .contains("endpoint")
        );

        let mut missing_port = valid_join();
        missing_port.dataplane_port = 0;
        assert!(
            validate_join_metadata(&missing_port)
                .unwrap_err()
                .to_string()
                .contains("port")
        );
    }

    #[test]
    fn validate_join_metadata_defaults_empty_encryption_to_enabled() {
        let mut join = valid_join();
        join.dataplane_encryption.clear();
        let metadata = validate_join_metadata(&join).unwrap();
        assert_eq!(
            metadata.encryption,
            crate::networking::wireguard::DataplaneEncryption::Enabled
        );
    }

    #[test]
    fn validate_join_metadata_accepts_explicit_disabled_without_public_key() {
        let mut join = valid_join();
        join.dataplane_encryption = "disabled".to_string();
        join.dataplane_public_key.clear();
        join.dataplane_port = 0;
        let metadata = validate_join_metadata(&join).unwrap();
        assert_eq!(
            metadata.encryption,
            crate::networking::wireguard::DataplaneEncryption::Disabled
        );
        assert!(metadata.public_key.is_none());
    }

    #[tokio::test]
    async fn get_metadata_rpc_returns_cluster_metadata_for_node_cert() {
        let db = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        // T3: `append_log_apply_entry` removed. `current_log_index`
        // always returns 0; the raft `last_applied` is authoritative.
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor));
        let grpc = super::GrpcReplicationServer::new(service, db);

        let response = grpc
            .get_metadata(request_with_node_client_cert(
                MetadataRequest {},
                "worker-1",
            ))
            .await
            .unwrap()
            .into_inner();
        assert!(!response.cluster_id.is_empty());
        assert_eq!(response.leader_epoch, 0);
        assert_eq!(response.current_log_index, 0);
    }

    #[tokio::test]
    async fn observe_peer_endpoint_records_authenticated_node_remote_ip() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor));
        let grpc = super::GrpcReplicationServer::new(service.clone(), db);
        let mut request = request_with_node_client_cert(
            generated::ObservePeerEndpointRequest {
                node_name: "leader-a".to_string(),
            },
            "leader-a",
        );
        request
            .extensions_mut()
            .insert(tonic::transport::server::TcpConnectInfo {
                local_addr: None,
                remote_addr: Some("10.99.0.10:47000".parse().unwrap()),
            });

        let response = grpc
            .observe_peer_endpoint(request)
            .await
            .expect("observe endpoint should accept node cert")
            .into_inner();

        assert!(response.found);
        assert_eq!(response.endpoint, "10.99.0.10");
        assert_eq!(
            service.observed_peer_endpoint("leader-a").await.as_deref(),
            Some("10.99.0.10")
        );
    }

    #[tokio::test]
    async fn node_effect_observed_leader_endpoint_enqueues_external_ip_status() {
        let db = crate::datastore::test_support::in_memory().await;
        let addresses =
            crate::kubelet::node::NodeRegistrationAddresses::new("172.31.10.2".to_string(), None);
        crate::kubelet::node::register_node_at_addresses(
            &db,
            "leader-a",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            &addresses,
        )
        .await
        .unwrap();

        let query: Arc<dyn klights_leader_api::LeaderResourceQuery> =
            Arc::new(crate::control_plane::client::local::LocalApiClient::new(
                Arc::new(db.clone()),
                "leader-a".to_string(),
                crate::control_plane::client::local::always_leader_watch(),
            ));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            Arc::new(TaskSupervisor::new(TaskCategoryConfig::default())),
            None,
            "sqlite:observed-leader-endpoint-status",
        )
        .await
        .expect("open node-local outbox");
        let publisher = crate::kubelet::node::OutboxNodeSelfStatusPublisher::new(
            "leader-a",
            query.clone(),
            Arc::new(crate::kubelet::outbox::Outbox::new(node_local.clone())),
        );

        super::refresh_local_node_external_ip_from_observed_endpoint(
            query.as_ref(),
            &publisher,
            "leader-a",
            "10.99.0.10",
        )
        .await
        .expect("observed leader endpoint should enqueue local Node status");

        let row = node_local
            .claim_next_due_outbox(i64::MAX / 2, 1_000, "inspect")
            .await
            .expect("inspect outbox")
            .expect("external IP status row");
        assert_eq!(
            row.operation,
            crate::kubelet::outbox::payload::OutboxOperation::NodeStatus.as_str()
        );
        let payload =
            crate::kubelet::outbox::payload::OutboxPayload::decode_protobuf(&row.payload_proto)
                .expect("decode status payload");
        let StorageCommand::UpdateStatus { status, .. } = payload.command else {
            panic!("external IP publication must be status-only")
        };
        let addresses = status
            .pointer("/addresses")
            .and_then(|value| value.as_array())
            .unwrap();
        assert!(addresses.iter().any(|address| {
            address["type"] == "InternalIP" && address["address"] == "172.31.10.2"
        }));
        assert!(addresses.iter().any(|address| {
            address["type"] == "ExternalIP" && address["address"] == "10.99.0.10"
        }));
    }

    #[tokio::test]
    async fn node_effect_join_external_ip_uses_fresh_exact_status_after_metadata_cas() {
        let db = Arc::new(crate::datastore::test_support::in_memory().await);
        let created = db
            .create_resource(
                "v1",
                "Node",
                None,
                "worker-1",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Node",
                    "metadata": {"name": "worker-1", "uid": "worker-uid-1"},
                    "status": {
                        "conditions": [{"type": "Ready", "status": "True"}],
                        "addresses": [{"type": "InternalIP", "address": "10.0.0.8"}]
                    }
                }),
            )
            .await
            .expect("create joining Node");
        let dataplane = crate::networking::wireguard::DataplanePeerMetadata::try_new(
            "worker-1".to_string(),
            crate::networking::wireguard::DataplaneMode::Root,
            crate::networking::wireguard::DataplaneEncryption::Enabled,
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
            Some("192.0.2.80".to_string()),
            Some(51_820),
        )
        .expect("valid dataplane metadata");
        db.update_node_dataplane(dataplane.clone())
            .await
            .expect("store joining dataplane metadata");
        let query: Arc<dyn klights_leader_api::LeaderResourceQuery> =
            Arc::new(crate::control_plane::client::local::LocalApiClient::new(
                db.clone(),
                "leader-a".to_string(),
                crate::control_plane::client::local::always_leader_watch(),
            ));
        let status = Arc::new(RecordingNodeLifecycleStatus::default());
        let node_uid = created.uid.clone();

        super::refresh_joining_node_from_dataplane(
            db.as_ref(),
            query.as_ref(),
            status.as_ref(),
            &dataplane,
        )
        .await
        .expect("split joining Node projection");

        let stored = db
            .get_resource("v1", "Node", None, "worker-1")
            .await
            .expect("read joining Node")
            .expect("joining Node remains present");
        assert!(stored.resource_version > created.resource_version);
        assert_eq!(
            stored
                .data
                .pointer("/metadata/annotations/klights.io~1dataplane-endpoint")
                .and_then(serde_json::Value::as_str),
            Some("192.0.2.80")
        );
        assert!(
            stored
                .data
                .pointer("/status/addresses")
                .and_then(serde_json::Value::as_array)
                .expect("stored Node addresses")
                .iter()
                .all(|address| address["type"] != "ExternalIP"),
            "metadata CAS must not full-update Node status"
        );

        let request = status.take_request();
        assert_eq!(request.node_name(), "worker-1");
        assert_eq!(request.node_uid(), node_uid);
        assert_eq!(request.resource_version(), stored.resource_version);
        let StorageCommand::UpdateStatus {
            status,
            preconditions,
            expected_rv,
            ..
        } = request.into_command()
        else {
            panic!("join ExternalIP must use status-only authority")
        };
        assert_eq!(expected_rv, Some(stored.resource_version));
        assert_eq!(
            preconditions,
            ResourcePreconditions::uid_and_resource_version(created.uid, stored.resource_version,)
        );
        assert_eq!(
            status.pointer("/conditions/0/status"),
            Some(&serde_json::json!("True")),
            "fresh post-metadata status must preserve concurrent condition state"
        );
        assert!(
            status
                .pointer("/addresses")
                .and_then(serde_json::Value::as_array)
                .expect("published status addresses")
                .iter()
                .any(|address| {
                    address["type"] == "ExternalIP" && address["address"] == "192.0.2.80"
                })
        );
    }

    #[tokio::test]
    async fn get_metadata_rpc_rejects_missing_node_client_certificate() {
        let db = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor));
        let grpc = super::GrpcReplicationServer::new(service, db);

        let status = grpc
            .get_metadata(tonic::Request::new(MetadataRequest {}))
            .await
            .expect_err("metadata must reject requests without a node client certificate");
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn get_metadata_rpc_rejects_bootstrap_token_after_join_bootstrap() {
        let db = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let token = crate::bootstrap::cluster_meta::read_join_token(db.as_ref())
            .await
            .unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor));
        let grpc = super::GrpcReplicationServer::new(service, db);

        let status = grpc
            .get_metadata(request_with_join_token(MetadataRequest {}, &token))
            .await
            .expect_err("bootstrap token must not authenticate steady-state metadata RPC");
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn get_metadata_rpc_accepts_node_client_cert_without_bootstrap_token() {
        let db = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor));
        let grpc = super::GrpcReplicationServer::new(service, db);

        let response = grpc
            .get_metadata(request_with_node_client_cert(
                MetadataRequest {},
                "worker-1",
            ))
            .await
            .unwrap()
            .into_inner();

        assert!(!response.cluster_id.is_empty());
    }

    #[tokio::test]
    async fn renew_node_lease_rpc_rejects_bootstrap_token_on_leader() {
        let db = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let token = crate::bootstrap::cluster_meta::read_join_token(db.as_ref())
            .await
            .unwrap();
        let tracker = Arc::new(crate::node_lease_tracker::NodeLeaseTracker::new_for_test(
            chrono::DateTime::parse_from_rfc3339("2026-05-25T00:00:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
        ));
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor));
        let grpc =
            super::GrpcReplicationServer::new_with_node_lease_tracker(service, db, tracker.clone());

        let status = grpc
            .renew_node_lease(request_with_join_token(
                generated::RenewNodeLeaseRequest {
                    node_name: "worker-1".to_string(),
                    renew_time: "2026-05-25T00:00:10Z".to_string(),
                    lease_duration_seconds: 50,
                },
                &token,
            ))
            .await
            .expect_err("bootstrap token must not authenticate node lease renewal");
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
        assert!(tracker.observed("worker-1").await.is_none());
    }

    #[tokio::test]
    async fn renew_node_lease_rpc_updates_memory_without_cluster_db_write() {
        let db = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let before_rv = db.get_current_resource_version().await.unwrap();
        let tracker = Arc::new(crate::node_lease_tracker::NodeLeaseTracker::new_for_test(
            chrono::DateTime::parse_from_rfc3339("2026-05-25T00:00:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
        ));
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor));
        let grpc = super::GrpcReplicationServer::new_with_node_lease_tracker(
            service,
            db.clone(),
            tracker.clone(),
        );

        let renew_time = crate::utils::k8s_time_format(chrono::Utc::now());
        grpc.renew_node_lease(request_with_node_client_cert(
            generated::RenewNodeLeaseRequest {
                node_name: "worker-1".to_string(),
                renew_time: renew_time.clone(),
                lease_duration_seconds: 50,
            },
            "worker-1",
        ))
        .await
        .unwrap();

        let observed = tracker
            .observed("worker-1")
            .await
            .expect("renewal should be recorded in memory");
        assert_eq!(observed.node_name, "worker-1");
        assert_eq!(observed.renew_time_string(), renew_time);
        assert_eq!(db.get_current_resource_version().await.unwrap(), before_rv);
        assert!(
            db.get_resource(
                "coordination.k8s.io/v1",
                "Lease",
                Some("kube-node-lease"),
                "worker-1",
            )
            .await
            .unwrap()
            .is_none(),
            "dedicated heartbeat RPC must not create a Lease row"
        );
        assert!(db.list_applied_outbox().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn renew_node_lease_rpc_rejects_follower_local_heartbeat_write() {
        let db = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let tracker = Arc::new(crate::node_lease_tracker::NodeLeaseTracker::new_for_test(
            chrono::DateTime::parse_from_rfc3339("2026-05-25T00:00:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
        ));
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor));
        let (_is_leader_tx, is_leader_rx) = tokio::sync::watch::channel(false);
        let grpc =
            super::GrpcReplicationServer::new_with_node_lease_tracker(service, db, tracker.clone())
                .with_leader_gate(is_leader_rx);

        let status = grpc
            .renew_node_lease(request_with_node_client_cert(
                generated::RenewNodeLeaseRequest {
                    node_name: "worker-1".to_string(),
                    renew_time: "2026-05-25T00:00:10Z".to_string(),
                    lease_duration_seconds: 50,
                },
                "worker-1",
            ))
            .await
            .expect_err("follower must not accept worker lease renewals");

        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), "not raft leader");
        assert!(
            tracker.observed("worker-1").await.is_none(),
            "follower-local lease tracker must not be updated"
        );
    }

    #[tokio::test]
    async fn snapshot_rpc_rejects_invalid_bootstrap_token() {
        let db = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor));
        let grpc = super::GrpcReplicationServer::new(service, db);
        let mut request = tonic::Request::new(SnapshotRequest { last_applied_rv: 0 });
        request
            .metadata_mut()
            .insert("x-klights-join-token", "wrong-token".parse().unwrap());

        let status = match grpc.snapshot(request).await {
            Ok(_) => panic!("snapshot must reject requests with an invalid bootstrap token"),
            Err(status) => status,
        };
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn sign_controlplane_csr_sends_private_key_material_to_cp_and_replica() {
        for node_name in ["mn-controlplane2", "mn-replica"] {
            let db = Arc::new(crate::datastore::test_support::in_memory().await);
            create_scoped_token_for_test(
                db.as_ref(),
                "123456.fedcba9876543210",
                crate::bootstrap::bootstrap_token::BootstrapTokenScope::Controlplane,
            )
            .await;
            let namespace = format!("grpc-cp-token-{node_name}-{}", uuid::Uuid::new_v4());
            let grpc = grpc_test_server_with_signing_ca(db, &namespace).await;
            let (_, csr_pem) = crate::auth::generate_server_csr(
                "10.43.0.0/16",
                "10.50.4.0/24",
                Some("10.99.0.14"),
                node_name,
                None,
            )
            .unwrap();
            let mut request = tonic::Request::new(generated::SignControlplaneCsrRequest {
                node_name: node_name.to_string(),
                server_csr: csr_pem,
            });
            request.metadata_mut().insert(
                "x-klights-join-token",
                "123456.fedcba9876543210".parse().unwrap(),
            );

            let response = grpc
                .sign_controlplane_csr(request)
                .await
                .unwrap_or_else(|status| {
                    panic!("{node_name} controlplane bootstrap token should sign CSR: {status}")
                })
                .into_inner();
            assert!(
                !response.signed_server_cert.is_empty(),
                "{node_name} should receive a signed cert"
            );
            assert!(
                !response.encrypted_ca_key.is_empty(),
                "{node_name} should receive encrypted CA key material"
            );
            assert!(
                !response.encrypted_service_account_signing_key.is_empty(),
                "{node_name} should receive encrypted ServiceAccount signing key material"
            );
            assert_eq!(
                response.service_account_signing_key_nonce.len(),
                12,
                "{node_name} should receive a ServiceAccount signing key nonce"
            );
        }
    }

    #[tokio::test]
    async fn sign_controlplane_csr_rejects_worker_node_cert_without_controlplane_token() {
        // A worker authenticates this RPC with its own node client cert (every
        // worker holds one after kubelet bootstrap) and supplies an arbitrary,
        // non-empty join token in metadata. It must be rejected outright: it
        // holds no valid controlplane token AND is not a raft member, so it can
        // get neither the CA private key / SA signing key (→ system:masters
        // escalation) NOR a CA-trusted `klights-server` cert (→ API-server
        // impersonation). grpc_test_server_with_signing_ca wires no join
        // handler, so membership cannot be confirmed and the request fails
        // closed.
        let db = Arc::new(crate::datastore::test_support::in_memory().await);
        // Only a *worker*-scoped token exists; the supplied token can never be a
        // valid controlplane join token.
        create_scoped_token_for_test(
            db.as_ref(),
            "abcdef.0123456789abcdef",
            crate::bootstrap::bootstrap_token::BootstrapTokenScope::Worker,
        )
        .await;
        let namespace = format!("grpc-cp-worker-leak-{}", uuid::Uuid::new_v4());
        let grpc = grpc_test_server_with_signing_ca(db, &namespace).await;
        let (_, csr_pem) = crate::auth::generate_server_csr(
            "10.43.0.0/16",
            "10.50.4.0/24",
            Some("10.99.0.14"),
            "worker-1",
            None,
        )
        .unwrap();
        let mut request = request_with_node_client_cert(
            generated::SignControlplaneCsrRequest {
                node_name: "worker-1".to_string(),
                server_csr: csr_pem,
            },
            "worker-1",
        );
        request.metadata_mut().insert(
            "x-klights-join-token",
            "abcdef.0123456789abcdef".parse().unwrap(),
        );

        let status = grpc
            .sign_controlplane_csr(request)
            .await
            .expect_err("worker node cert with no controlplane token must be rejected");
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn join_as_controlplane_rejects_worker_node_cert_without_controlplane_token() {
        // A worker holds a node client cert but no controlplane token and is not
        // a raft member. It must NOT be admitted as a voter/learner — otherwise
        // it would receive the full replicated cluster.db (all Secrets) and
        // quorum influence.
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor));
        let (_is_leader_tx, is_leader_rx) = tokio::sync::watch::channel(true);
        let grpc = super::GrpcReplicationServer::new(service, db)
            .with_controlplane_join_handler(Arc::new(NonMemberControlplaneJoinHandler))
            .with_leader_gate(is_leader_rx);

        let request = request_with_node_client_cert(
            generated::JoinAsControlplaneRequest {
                node_id: raft_node_id_for_node_name_in_test("worker-1"),
                addr: "https://192.0.2.50:7679".to_string(),
                node_name: "worker-1".to_string(),
                as_learner: false,
                dataplane_public_key: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string(),
                dataplane_endpoint: "192.0.2.50".to_string(),
                dataplane_port: 7679,
                dataplane_mode: "root".to_string(),
                dataplane_encryption: "enabled".to_string(),
                node_internal_ip: "172.31.50.2".to_string(),
                node_git_commit: "testhash1".to_string(),
                node_registration: Some(test_node_registration_proto("testhash1")),
                supported_features: 0,
            },
            "worker-1",
        );

        let status = grpc
            .join_as_controlplane(request)
            .await
            .expect_err("worker node cert without controlplane token must be denied");
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn join_as_controlplane_accepts_valid_controlplane_token_for_first_join() {
        // First join: caller is not yet a member (NonMember handler) but presents
        // a valid controlplane bootstrap token → admitted.
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        create_scoped_token_for_test(
            db.as_ref(),
            "123456.fedcba9876543210",
            crate::bootstrap::bootstrap_token::BootstrapTokenScope::Controlplane,
        )
        .await;
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor));
        let (_is_leader_tx, is_leader_rx) = tokio::sync::watch::channel(true);
        let grpc = super::GrpcReplicationServer::new(service, db)
            .with_controlplane_join_handler(Arc::new(NonMemberControlplaneJoinHandler))
            .with_leader_gate(is_leader_rx);

        let join_request = generated::JoinAsControlplaneRequest {
            node_id: raft_node_id_for_node_name_in_test("mn-controlplane2"),
            addr: "https://192.0.2.20:7679".to_string(),
            node_name: "mn-controlplane2".to_string(),
            as_learner: false,
            dataplane_public_key: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string(),
            dataplane_endpoint: "192.0.2.20".to_string(),
            dataplane_port: 7679,
            dataplane_mode: "root".to_string(),
            dataplane_encryption: "enabled".to_string(),
            node_internal_ip: "172.31.20.2".to_string(),
            node_git_commit: "testhash2".to_string(),
            node_registration: Some(test_node_registration_proto("testhash2")),
            supported_features: 0,
        };

        let mut mismatched_id = join_request.clone();
        mismatched_id.node_id = mismatched_id.node_id.wrapping_add(1);
        let mut request = request_with_node_client_cert(mismatched_id, "mn-controlplane2");
        request.metadata_mut().insert(
            "x-klights-join-token",
            "123456.fedcba9876543210".parse().unwrap(),
        );
        let status = grpc
            .join_as_controlplane(request)
            .await
            .expect_err("raft node ID must be derived from the authenticated node name");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);

        let mut request = request_with_node_client_cert(join_request.clone(), "mn-controlplane2");
        request.metadata_mut().insert(
            "x-klights-join-token",
            "123456.fedcba9876543210".parse().unwrap(),
        );

        let response = grpc
            .join_as_controlplane(request)
            .await
            .expect("valid controlplane token must authorize first join")
            .into_inner();
        assert!(matches!(
            response.result,
            Some(generated::join_as_controlplane_response::Result::Accepted(
                _
            ))
        ));

        let mut legacy_first_join = join_request;
        legacy_first_join.node_registration = None;
        let mut request = request_with_node_client_cert(legacy_first_join, "mn-controlplane2");
        request.metadata_mut().insert(
            "x-klights-join-token",
            "123456.fedcba9876543210".parse().unwrap(),
        );
        let status = grpc
            .join_as_controlplane(request)
            .await
            .expect_err("first join without typed node registration must be rejected");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    fn raft_node_id_for_node_name_in_test(node_name: &str) -> u64 {
        crate::datastore::raft::types::raft_node_id_for_node_name(node_name)
    }

    #[tokio::test]
    async fn mount_service_accepts_replication_router_prefix() {
        let db = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor));
        let _router = super::mount_service(
            axum::Router::new(),
            service,
            db,
            crate::replication::grpc::transport_policy::GrpcTransportPolicy::shared_default(),
        );
    }

    #[tokio::test]
    async fn mounted_router_does_not_send_plain_rest_unknown_paths_to_grpc() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let db = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor));
        let app = super::mount_service(
            axum::Router::new(),
            service,
            db,
            crate::replication::grpc::transport_policy::GrpcTransportPolicy::shared_default(),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/metrics/slis")
                    .header("accept", "*/*")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_ne!(
            response.headers().get("content-type"),
            Some(&axum::http::HeaderValue::from_static("application/grpc"))
        );
    }

    #[tokio::test]
    async fn mounted_router_serves_grpc_get_metadata() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let (endpoint, _service, handle) = grpc_test_server_with_node_cert(db, "worker-1").await;
        let channel = tonic::transport::Endpoint::from_shared(endpoint)
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = ReplicationClient::new(channel);

        let response = client
            .get_metadata(tonic::Request::new(MetadataRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert!(!response.cluster_id.is_empty());
        handle.abort();
    }

    #[tokio::test]
    async fn mounted_router_serves_grpc_reflection_for_replication_service() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        let (endpoint, _service, handle) = grpc_test_server(db).await;
        let channel = tonic::transport::Endpoint::from_shared(endpoint)
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = ServerReflectionClient::new(channel);
        let outbound = async_stream::stream! {
            yield ServerReflectionRequest {
                host: String::new(),
                message_request: Some(
                    server_reflection_request::MessageRequest::ListServices(String::new())
                ),
            };
        };

        let mut inbound = client
            .server_reflection_info(tonic::Request::new(outbound))
            .await
            .unwrap()
            .into_inner();
        let response = inbound.message().await.unwrap().unwrap();
        let Some(server_reflection_response::MessageResponse::ListServicesResponse(services)) =
            response.message_response
        else {
            panic!("expected reflection ListServicesResponse, got {response:?}");
        };

        assert!(
            services
                .service
                .iter()
                .any(|service| service.name == "klights.replication.Replication")
        );
        handle.abort();
    }

    #[tokio::test]
    async fn connect_rejects_invalid_token_without_persisting_dataplane_metadata() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        let (endpoint, _service, handle) = grpc_test_server(db.clone()).await;
        let mut join = valid_join();
        join.token = "wrong-token".to_string();
        join.node_name = "bad-node".to_string();

        let (_tx, mut inbound) = open_connect(&endpoint, join).await;
        let first = inbound.message().await.unwrap().unwrap();
        match first.payload.unwrap() {
            generated::leader_message::Payload::JoinResponse(response) => {
                assert!(matches!(
                    response.result,
                    Some(generated::join_response::Result::Rejected(_))
                ));
            }
            other => panic!("expected JoinResponse, got {other:?}"),
        }
        assert!(db.get_node_dataplane("bad-node").await.unwrap().is_none());
        handle.abort();
    }

    #[tokio::test]
    async fn connect_persists_dataplane_endpoint_from_observed_peer_ip() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        let (endpoint, _service, handle) =
            grpc_test_server_with_node_cert(db.clone(), "worker-1").await;
        let mut join = valid_join();
        join.token.clear();
        join.dataplane_endpoint = "192.168.8.22".to_string();
        join.dataplane_port = 7679;

        let (_tx, mut inbound) = open_connect(&endpoint, join).await;
        let first = inbound.message().await.unwrap().unwrap();
        assert!(matches!(
            first.payload.unwrap(),
            generated::leader_message::Payload::JoinResponse(generated::JoinResponse {
                result: Some(generated::join_response::Result::Accepted(_)),
            })
        ));

        let metadata = db
            .get_node_dataplane("worker-1")
            .await
            .unwrap()
            .expect("accepted join must persist worker dataplane metadata");
        assert_eq!(metadata.endpoint.to_string(), "127.0.0.1");
        assert_eq!(metadata.port, Some(7679));
        handle.abort();
    }

    #[tokio::test]
    async fn connect_refreshes_existing_node_external_ip_from_observed_peer_ip() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        let (endpoint, _service, handle) =
            grpc_test_server_with_node_cert(db.clone(), "worker-1").await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "worker-1",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "worker-1"},
                "status": {
                    "addresses": [
                        {"type": "Hostname", "address": "worker-1"},
                        {"type": "InternalIP", "address": "192.168.8.22"},
                        {"type": "ExternalIP", "address": "192.168.8.22"}
                    ]
                }
            }),
        )
        .await
        .unwrap();
        let mut join = valid_join();
        join.token.clear();
        join.dataplane_endpoint = "192.168.8.22".to_string();
        join.dataplane_port = 7679;

        let (_tx, mut inbound) = open_connect(&endpoint, join).await;
        let _first = inbound.message().await.unwrap().unwrap();

        let node = db
            .get_resource("v1", "Node", None, "worker-1")
            .await
            .unwrap()
            .expect("worker Node should remain present");
        let external_ip = node.data["status"]["addresses"]
            .as_array()
            .unwrap()
            .iter()
            .find(|address| address["type"] == "ExternalIP")
            .and_then(|address| address["address"].as_str());
        assert_eq!(external_ip, Some("127.0.0.1"));
        handle.abort();
    }

    #[tokio::test]
    async fn connect_accepts_valid_join_and_streams_entries() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        db.allocate_node_subnet("leader", "10.42.0.0/16", "192.0.2.1")
            .await
            .unwrap();
        db.update_node_dataplane(
            crate::networking::wireguard::DataplanePeerMetadata::try_new(
                "leader".to_string(),
                crate::networking::wireguard::DataplaneMode::Root,
                crate::networking::wireguard::DataplaneEncryption::Enabled,
                Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
                Some("192.0.2.1".to_string()),
                Some(51_820),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        let (endpoint, service, handle) =
            grpc_test_server_with_node_cert(db.clone(), "worker-1").await;
        let mut join = valid_join();
        join.token.clear();

        let (_tx, mut inbound) = open_connect(&endpoint, join).await;
        let first = inbound.message().await.unwrap().unwrap();
        match first.payload.unwrap() {
            generated::leader_message::Payload::JoinResponse(generated::JoinResponse {
                result: Some(generated::join_response::Result::Accepted(accepted)),
            }) => {
                assert_eq!(accepted.peers.len(), 1);
                assert_eq!(accepted.peers[0].node_name, "leader");
                assert_eq!(accepted.peers[0].pod_cidr, "10.42.0.0/24");
                assert_eq!(accepted.peers[0].endpoint, "192.0.2.1");
            }
            other => panic!("expected accepted JoinResponse, got {other:?}"),
        }

        service.notify_entry(sample_entry(10));
        let streamed = inbound.message().await.unwrap().unwrap();
        match streamed.payload.unwrap() {
            generated::leader_message::Payload::StreamItem(item) => {
                assert!(matches!(
                    item.item,
                    Some(generated::stream_item::Item::Entry(_))
                ));
            }
            other => panic!("expected StreamItem, got {other:?}"),
        }
        handle.abort();
    }

    #[tokio::test]
    async fn accepted_legacy_controlplane_rejoin_without_snapshot_persists_dataplane_metadata() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let (endpoint, _service, handle) = grpc_test_server_full_with_node_cert(
            db.clone(),
            None,
            Some(Arc::new(AcceptingControlplaneJoinHandler)),
            Some("mn-controlplane2".to_string()),
        )
        .await;
        let channel = tonic::transport::Endpoint::from_shared(endpoint)
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = ReplicationClient::new(channel);
        let request = tonic::Request::new(generated::JoinAsControlplaneRequest {
            node_id: raft_node_id_for_node_name_in_test("mn-controlplane2"),
            addr: "https://192.0.2.20:7679".to_string(),
            node_name: "mn-controlplane2".to_string(),
            as_learner: false,
            dataplane_public_key: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string(),
            dataplane_endpoint: "192.0.2.20".to_string(),
            dataplane_port: 7679,
            dataplane_mode: "root".to_string(),
            dataplane_encryption: "enabled".to_string(),
            node_internal_ip: "172.31.20.2".to_string(),
            node_git_commit: "testhash3".to_string(),
            node_registration: None,
            supported_features: 0,
        });

        let response = client
            .join_as_controlplane(request)
            .await
            .unwrap()
            .into_inner();
        assert!(matches!(
            response.result,
            Some(generated::join_as_controlplane_response::Result::Accepted(
                _
            ))
        ));
        let metadata = db
            .get_node_dataplane("mn-controlplane2")
            .await
            .unwrap()
            .expect("accepted controlplane join must persist dataplane metadata");
        assert_eq!(metadata.endpoint.to_string(), "127.0.0.1");
        assert_eq!(metadata.port, Some(7679));
        handle.abort();
    }

    #[tokio::test]
    async fn accepted_controlplane_join_uses_observed_peer_ip_for_dataplane_and_raft_addr() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let join_handler = Arc::new(RecordingControlplaneJoinHandler::default());
        let (endpoint, _service, handle) = grpc_test_server_full_with_node_cert(
            db.clone(),
            None,
            Some(join_handler.clone()),
            Some("mn-controlplane2".to_string()),
        )
        .await;
        let channel = tonic::transport::Endpoint::from_shared(endpoint)
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = ReplicationClient::new(channel);
        let request = tonic::Request::new(generated::JoinAsControlplaneRequest {
            node_id: raft_node_id_for_node_name_in_test("mn-controlplane2"),
            addr: "https://172.31.14.2:7679".to_string(),
            node_name: "mn-controlplane2".to_string(),
            as_learner: false,
            dataplane_public_key: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string(),
            dataplane_endpoint: "172.31.14.2".to_string(),
            dataplane_port: 7679,
            dataplane_mode: "root".to_string(),
            dataplane_encryption: "enabled".to_string(),
            node_internal_ip: "172.31.14.2".to_string(),
            node_git_commit: "joinhash1".to_string(),
            node_registration: Some(test_node_registration_proto("joinhash1")),
            supported_features: 0,
        });

        let response = client
            .join_as_controlplane(request)
            .await
            .unwrap()
            .into_inner();
        assert!(matches!(
            response.result,
            Some(generated::join_as_controlplane_response::Result::Accepted(
                _
            ))
        ));

        let calls = join_handler.calls();
        assert_eq!(
            calls,
            vec![RecordedControlplaneJoin {
                node_id: raft_node_id_for_node_name_in_test("mn-controlplane2"),
                addr: "https://127.0.0.1:7679".to_string(),
                node_name: "mn-controlplane2".to_string(),
                as_learner: false,
                node_internal_ip: Some("172.31.14.2".to_string()),
                node_registration: Some(
                    super::validate_controlplane_node_registration(test_node_registration_proto(
                        "joinhash1",
                    ))
                    .unwrap(),
                ),
                legacy_node_git_commit: Some("joinhash1".to_string()),
            }],
            "raft membership must use the externally observed peer address"
        );
        let metadata = db
            .get_node_dataplane("mn-controlplane2")
            .await
            .unwrap()
            .expect("accepted controlplane join must persist dataplane metadata");
        assert_eq!(metadata.endpoint.to_string(), "127.0.0.1");
        assert_eq!(metadata.port, Some(7679));
        handle.abort();
    }

    #[tokio::test]
    async fn apply_outbox_pod_status_enqueues_matching_service() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        let _token = {
            crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
                .await
                .unwrap();
            crate::bootstrap::cluster_meta::read_join_token(db.as_ref())
                .await
                .unwrap()
        };
        db.create_resource(
            "v1",
            "Service",
            Some("default"),
            "web",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": "web", "namespace": "default"},
                "spec": {
                    "selector": {"app": "web"},
                    "ports": [{"name": "http", "port": 80, "targetPort": 8080}]
                }
            }),
        )
        .await
        .unwrap();
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "web-worker",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "web-worker",
                    "namespace": "default",
                    "uid": "pod-uid",
                    "labels": {"app": "web"}
                },
                "spec": {"nodeName": "worker-1", "containers": [{"name": "c", "image": "pause"}]},
                "status": {"phase": "Pending"}
            }),
        )
        .await
        .unwrap();
        let dispatcher = Arc::new(crate::controller_dispatcher::ControllerDispatcher::new(
            Arc::new(crate::controllers::service::ServiceIpam::new(
                "10.43.128.0/17",
            )),
        ));
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor));
        let grpc = super::GrpcReplicationServer::new_with_controller_dispatcher(
            service,
            db.clone(),
            dispatcher.clone(),
        );

        let command = StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web-worker".to_string(),
            status: serde_json::json!({
                "phase": "Running",
                "podIP": "10.43.1.2",
                "podIPs": [{"ip": "10.43.1.2"}],
                "conditions": [{"type": "Ready", "status": "True"}]
            }),
            expected_rv: None,
            preconditions: ResourcePreconditions {
                uid: Some("pod-uid".to_string()),
                resource_version: None,
            },
            observed_status_stamp: None,
        };
        let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
            .encode_protobuf()
            .unwrap();
        let response = grpc
            .apply_outbox(request_with_node_client_cert(
                generated::ApplyOutboxRequest {
                    idempotency_key: "pod-status-web-worker".to_string(),
                    operation: crate::kubelet::outbox::payload::OutboxOperation::PodStatus
                        .as_str()
                        .to_string(),
                    payload_proto: payload,
                    authoring_node: "worker-1".to_string(),
                    client_id: "client".to_string(),
                    stream_id: 1,
                    stream_seq: 1,
                },
                "worker-1",
            ))
            .await
            .unwrap()
            .into_inner();

        assert!(
            response.error.is_none(),
            "unexpected apply error: {response:?}"
        );
        assert!(!response.already_applied);
        let keys = dispatcher.queued_reconcile_keys_for_test().await;
        assert!(
            keys.iter().any(|key| {
                key.api_version == "v1"
                    && key.kind == "Service"
                    && key.namespace.as_deref() == Some("default")
                    && key.name == "web"
            }),
            "outbox-applied worker pod status must enqueue matching Services on the leader: {keys:?}"
        );
    }

    #[test]
    fn watch_heartbeat_proto_is_a_bookmark_carrying_the_cursor_rv() {
        // bug-grpc: the idle heartbeat must be a BOOKMARK that carries the
        // stream cursor RV so the worker treats it as liveness + a resume
        // point, and it must round-trip through the normal event proto shape
        // (the client decode requires a `resource`).
        let resume_position = WatchReplayPosition {
            resource_version: 4242,
            event_id: 77,
            resource_version_filter_through_event_id: 0,
        };
        let event = super::watch_heartbeat_proto("v1", "Pod", 4242, resume_position);
        assert_eq!(event.event_type, "BOOKMARK");
        assert_eq!(
            event
                .resume_position
                .as_ref()
                .map(|position| position.event_id),
            Some(77)
        );
        let resource = event.resource.expect("heartbeat must carry a resource");
        assert_eq!(resource.resource_version, 4242);
        let data: serde_json::Value =
            serde_json::from_slice(&resource.data_json).expect("heartbeat data_json must decode");
        assert_eq!(
            data.pointer("/metadata/resourceVersion")
                .and_then(|v| v.as_str()),
            Some("4242"),
            "bookmark metadata must carry the cursor RV as the resume point"
        );
        assert_eq!(data.get("kind").and_then(|v| v.as_str()), Some("Pod"));
    }
    // ─────────────────────────────────────────────────────────────────
    // memory-improvement.md §10 P1 — streaming snapshot serve path.
    // ─────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn channel_proto_sink_forwards_commits_as_protos_round_trip() {
        use crate::replication::snapshot::SnapshotCommitSink;
        use crate::replication::snapshot::stream_snapshot_commits;
        use crate::replication::snapshot_commit_channel_sink::SnapshotCommitChannelSink;

        // Build a fixture cluster with a couple of resources so the snapshot
        // emitter has real commits to stream.
        let db = crate::datastore::test_support::in_memory().await;
        crate::controllers::namespace::init_default_namespaces(&db)
            .await
            .unwrap();
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm-stream-1",
            serde_json::json!({"metadata": {"name": "cm-stream-1"}}),
        )
        .await
        .unwrap();
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm-stream-2",
            serde_json::json!({"metadata": {"name": "cm-stream-2"}}),
        )
        .await
        .unwrap();

        // Baseline: the legacy Vec path (equivalence oracle).
        let baseline = crate::replication::snapshot::generate_snapshot(&db, 0)
            .await
            .unwrap();

        // Streaming path: push straight into a bounded channel.
        let (tx, mut rx) = mpsc::channel::<Result<generated::ReplicationEntry, tonic::Status>>(64);
        let mut sink = SnapshotCommitChannelSink::new(tx);
        stream_snapshot_commits(&db, 0, &mut sink).await.unwrap();
        // `finish` drops the inner sender so the receiver stream terminates.
        sink.finish().unwrap();

        // Drain the channel, decode each proto back to a LogApplyCommit.
        let mut collected: Vec<crate::log_apply::LogApplyCommit> = Vec::new();
        while let Some(item) = rx.recv().await {
            let proto = item.expect("proto conversion must not fail");
            let commit = crate::replication::grpc::log_apply_commit_from_proto(proto).unwrap();
            collected.push(commit);
        }

        assert_eq!(
            collected.len(),
            baseline.len(),
            "streamed commit count must match the legacy Vec path"
        );
        for (streamed_commit, baseline_commit) in collected.iter().zip(baseline.iter()) {
            assert_eq!(
                streamed_commit.resource_version, baseline_commit.resource_version,
                "resource_version must match per commit"
            );
            assert_eq!(
                streamed_commit.mutations.len(),
                baseline_commit.mutations.len(),
                "mutation count must match per commit"
            );
        }
    }
}
