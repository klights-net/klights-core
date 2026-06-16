use anyhow::{Context, Result, anyhow};
use futures::stream::BoxStream;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tonic::{Request, Response, Status, metadata::MetadataMap};

use crate::controller_dispatcher::ControllerDispatcher;
use crate::datastore::backend::{DatastoreBackend, DatastoreHandle};
use crate::datastore::sqlite::DatastoreWatchReplaySource;
use crate::datastore::{Resource, ResourcePreconditions, WatchTarget};
use crate::networking::wireguard::{DataplaneEncryption, DataplaneMode, DataplanePeerMetadata};
use crate::replication::grpc::{
    JOIN_TOKEN_METADATA_KEY, entry_to_proto, generated, log_apply_commit_to_proto,
};
use crate::replication::protocol::{
    ExecStreamChannel, FollowerControlMessage, JoinResponse, JoinRole, NodeExecRequest,
    NodeExecStreamFrame, NodeExecSyncRequest, NodeExecSyncResponse, PodLogRequest, PodLogResponse,
};
use crate::replication::service::ReplicationService;
use crate::watch::WatchEventSelection;

use super::ca_files::ControlplaneCaFiles;
use super::snapshot_cache::SnapshotCache;

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

pub struct GrpcReplicationServer {
    service: Arc<ReplicationService>,
    db: DatastoreHandle,
    controller_dispatcher: Option<Arc<ControllerDispatcher>>,
    node_lease_tracker: Arc<crate::node_lease_tracker::NodeLeaseTracker>,
    snapshot_cache: Arc<SnapshotCache<(i64, i64), Vec<crate::log_apply::LogApplyCommit>>>,
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
        Self {
            service,
            db,
            controller_dispatcher,
            node_lease_tracker,
            snapshot_cache: Arc::new(SnapshotCache::new(Duration::from_secs(30))),
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

    /// Authenticate a raft consensus RPC (append-entries / vote /
    /// install-snapshot). Raft peers are all control-plane voters. They must
    /// present their node (`system:node:<name>` + `system:nodes`) client
    /// certificate. Bootstrap tokens are only for CSR bootstrap and admin certs
    /// are not raft peer credentials.
    async fn require_raft_peer_auth<T>(
        &self,
        request: &Request<T>,
    ) -> std::result::Result<(), Status> {
        let Some(cert) = request
            .extensions()
            .get::<crate::auth::TlsClientCertificate>()
        else {
            return Err(Status::unauthenticated(
                "raft RPC requires a node client certificate",
            ));
        };
        let user = crate::auth::user_from_cert(&cert.0).map_err(|err| {
            Status::unauthenticated(format!("invalid raft peer certificate: {err}"))
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
                Status::unauthenticated("raft peer certificate must be a node identity")
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
            return Err(Status::permission_denied(
                "raft consensus RPCs require a system:controlplanes node certificate",
            ));
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
    let is_node = identity.username.starts_with("system:node:")
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
            refresh_node_external_ip_from_dataplane(self.db.as_ref(), &dataplane)
                .await
                .map_err(|err| Status::internal(err.to_string()))?;
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
                                    if let Some(local_node_name) = local_node_name_for_observed_endpoint.as_deref()
                                        && let Err(err) = refresh_local_node_external_ip_from_observed_endpoint(
                                            db_for_observed_endpoint.as_ref(),
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
        let metadata = self.service.handle_metadata().await;
        let key = (metadata.current_rv, last_applied_rv);
        let db = self.db.clone();
        let entries = self
            .snapshot_cache
            .get_or_generate(key, move || async move {
                crate::replication::snapshot::generate_snapshot(db.as_ref(), last_applied_rv).await
            })
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        let stream = async_stream::stream! {
            for entry in entries {
                match log_apply_commit_to_proto(&entry) {
                    Ok(entry) => yield Ok(entry),
                    Err(err) => {
                        yield Err(Status::internal(err.to_string()));
                        break;
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
        }))
    }

    async fn get_cluster_membership(
        &self,
        request: Request<generated::ClusterMembershipRequest>,
    ) -> std::result::Result<Response<generated::ClusterMembershipResponse>, Status> {
        self.require_steady_state_auth(&request).await?;
        let membership = self.service.handle_cluster_membership().await;
        Ok(Response::new(generated::ClusterMembershipResponse {
            cluster_id: membership.cluster_id,
            voters: membership.voters,
            term: membership.term,
            leader_hint: membership.leader_hint.unwrap_or_default(),
        }))
    }

    async fn get_resource(
        &self,
        request: Request<generated::GetResourceRequest>,
    ) -> std::result::Result<Response<generated::GetResourceResponse>, Status> {
        self.require_steady_state_auth(&request).await?;
        let req = request.into_inner();
        let resource = self
            .db
            .get_resource(
                &req.api_version,
                &req.kind,
                req.namespace.as_deref(),
                &req.name,
            )
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
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
        let req = request.into_inner();
        let list = self
            .db
            .list_resources(
                &req.api_version,
                &req.kind,
                req.namespace.as_deref(),
                crate::datastore::ResourceListQuery::new(
                    req.label_selector.as_deref(),
                    req.field_selector.as_deref(),
                    req.limit,
                    req.continue_token.as_deref(),
                ),
            )
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        let items: Vec<generated::ResourceObject> =
            list.items.iter().map(resource_to_proto).collect();
        Ok(Response::new(generated::ListResourcesResponse {
            items,
            total: list.items.len() as i64,
            continue_token: list.continue_token,
            resource_version: list.resource_version,
            remaining_item_count: list.remaining_item_count,
        }))
    }

    async fn watch_resources(
        &self,
        request: Request<generated::WatchResourcesRequest>,
    ) -> std::result::Result<Response<Self::WatchResourcesStream>, Status> {
        self.require_steady_state_auth(&request).await?;
        // Issue #4: a worker watch must be served by the current raft leader.
        // Reject establishment on a stale follower so the worker reconnects to
        // the new leader instead of streaming from a deposed node.
        self.require_raft_leader()?;
        let req = request.into_inner();
        let topic = crate::watch::WatchTopic::new(&req.api_version, &req.kind);
        let signal_rx = self.db.subscribe_watch_signals(topic.clone());
        let replay_source =
            DatastoreWatchReplaySource::new(self.db.clone(), vec![watch_target_for_request(&req)]);
        let scope: crate::watch::WatchDeliveryScope = watch_delivery_scope_for_request(&req);
        let supervisor = self.service.task_supervisor();
        let heartbeat_interval = self.watch_heartbeat_interval;
        // Clone the leadership signal into the stream so the loop can race it
        // against the broadcast recv and terminate promptly on a leadership
        // change. Without this a deposed leader's broadcast goes silent and the
        // worker waits up to its ~60s idle watchdog before reconnecting, reading
        // stale informer-cached state in the window.
        let mut leader_rx = self.is_leader_rx.clone();
        let stream = async_stream::stream! {
            let mut last_rv = req.start_resource_version.max(0);
            let mut cursor = crate::watch::SignalWatchCursor::new(
                signal_rx,
                replay_source,
                topic,
                scope,
                last_rv,
                crate::watch::WindowPolicy::default_watch_delivery(),
            );
            if last_rv > 0
                && let Err(err) = cursor.prime_replay_or_expired().await
            {
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
                    yield Ok(watch_heartbeat_proto(&req.api_version, &req.kind, last_rv));
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
                        yield Ok(watch_heartbeat_proto(&req.api_version, &req.kind, last_rv));
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
                if !watch_event_matches(&event, &req) {
                    continue;
                }
                let resource = resource_from_event(&event);
                let rv = resource.resource_version;
                let event_type = watch_event_type(&event).to_string();
                yield Ok(generated::WatchEvent {
                    event_type,
                    resource: Some(resource_to_proto(&resource)),
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
        if let Some(node_name) = req.bound_node_name.as_deref() {
            enforce_node_authority(&caller, node_name)?;
        }
        let token_request = crate::control_plane::client::ProjectedServiceAccountTokenRequest {
            namespace: req.namespace,
            service_account_name: req.service_account_name,
            audiences: req.audiences,
            expiration_seconds: req.expiration_seconds,
            bound_pod_name: req.bound_pod_name,
            bound_pod_uid: req.bound_pod_uid,
            bound_node_name: req.bound_node_name,
            bound_node_uid: req.bound_node_uid,
        };
        let bound_pod =
            crate::control_plane::client::local::read_projected_service_account_token_bound_pod(
                &self.db,
                &token_request,
            )
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        let signing_key_pem = self.service_account_signing_key_pem().await?;
        let token =
            crate::control_plane::service_account_tokens::issue_projected_service_account_token(
                self.db.as_ref(),
                &signing_key_pem,
                &token_request,
                bound_pod.as_ref(),
            )
            .await
            .map_err(|err| Status::permission_denied(err.to_string()))?;
        Ok(Response::new(
            generated::ProjectedServiceAccountTokenResponse { token: token.token },
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
        // NodeRestriction: the outbox author must be the calling node.
        enforce_node_authority(&caller, &req.authoring_node)?;
        let operation =
            crate::kubelet::outbox::payload::OutboxOperation::try_from(req.operation.as_str())
                .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let payload = bytes::Bytes::from(req.payload_proto);
        let command_for_side_effects =
            crate::kubelet::outbox::payload::OutboxPayload::decode_protobuf(&payload)
                .ok()
                .map(|payload| payload.command);
        if let Some(command) = command_for_side_effects.as_ref() {
            crate::control_plane::client::apply::reject_node_author_mismatch(
                command,
                &req.authoring_node,
            )
            .map_err(|err| Status::permission_denied(err.to_string()))?;
        }
        let result =
            crate::control_plane::client::apply::apply_outbox_to_local_leader_with_resource(
                self.db.as_ref(),
                &req.idempotency_key,
                operation,
                payload,
                &req.authoring_node,
            )
            .await;
        match result {
            Ok(crate::control_plane::client::apply::LocalOutboxApply {
                result: crate::kubelet::outbox::OutboxApplyResult::Applied { applied_rv },
                resource,
                ..
            }) => {
                if let Some(command) = command_for_side_effects.as_ref() {
                    crate::control_plane::client::pod_status_side_effects::handle_applied_pod_side_effects(
                        self.controller_dispatcher.as_ref(),
                        command,
                        resource.as_ref(),
                        self.db.as_ref(),
                    )
                    .await;
                }
                Ok(Response::new(generated::ApplyOutboxResponse {
                    already_applied: false,
                    applied_rv,
                    error: None,
                    error_type: None,
                }))
            }
            Ok(crate::control_plane::client::apply::LocalOutboxApply {
                result: crate::kubelet::outbox::OutboxApplyResult::AlreadyApplied { applied_rv },
                ..
            }) => Ok(Response::new(generated::ApplyOutboxResponse {
                already_applied: true,
                applied_rv: applied_rv.unwrap_or(0),
                error: None,
                error_type: None,
            })),
            Err(err) => {
                let error_type = match &err {
                    crate::kubelet::outbox::OutboxApplyError::Retryable(_) => "Retryable",
                    crate::kubelet::outbox::OutboxApplyError::NotFound(_) => "NotFound",
                    crate::kubelet::outbox::OutboxApplyError::UidMismatch { .. } => "UidMismatch",
                    crate::kubelet::outbox::OutboxApplyError::ConflictTerminal(_) => {
                        "ConflictTerminal"
                    }
                };
                Ok(Response::new(generated::ApplyOutboxResponse {
                    already_applied: false,
                    applied_rv: 0,
                    error: Some(err.to_string()),
                    error_type: Some(error_type.to_string()),
                }))
            }
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
                        "leaseDurationSeconds": if req.lease_duration_seconds > 0 {
                            req.lease_duration_seconds
                        } else {
                            crate::node_lease_tracker::DEFAULT_NODE_LEASE_DURATION_SECONDS
                        },
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
        // NOTE: subnet allocation is intentionally NOT node-restricted — the
        // overlay controller legitimately allocates subnets for peer nodes
        // (see controllers/node_subnet.rs), so this is not a per-node-self RPC.
        let req = request.into_inner();
        let subnet = self
            .db
            .allocate_node_subnet(&req.node_name, &req.cluster_cidr, &req.node_ip)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(generated::NodeSubnetResponse {
            subnet: Some(node_subnet_to_proto(subnet)),
        }))
    }

    async fn get_node_subnet(
        &self,
        request: Request<generated::GetNodeSubnetRequest>,
    ) -> std::result::Result<Response<generated::GetNodeSubnetResponse>, Status> {
        self.require_steady_state_auth(&request).await?;
        let req = request.into_inner();
        let subnet = self
            .db
            .get_node_subnet(&req.node_name)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(match subnet {
            Some(subnet) => generated::GetNodeSubnetResponse {
                found: true,
                subnet: Some(node_subnet_to_proto(subnet)),
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
        let req = request.into_inner();
        let items = self
            .db
            .list_peer_subnets(&req.my_node_name)
            .await
            .map_err(|err| Status::internal(err.to_string()))?
            .into_iter()
            .map(node_subnet_to_proto)
            .collect();
        Ok(Response::new(generated::ListPeerSubnetsResponse { items }))
    }

    async fn get_node_dataplane(
        &self,
        request: Request<generated::GetNodeDataplaneRequest>,
    ) -> std::result::Result<Response<generated::GetNodeDataplaneResponse>, Status> {
        self.require_steady_state_auth(&request).await?;
        let req = request.into_inner();
        let metadata = self
            .db
            .get_node_dataplane(&req.node_name)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(match metadata {
            Some(metadata) => generated::GetNodeDataplaneResponse {
                found: true,
                metadata: Some(dataplane_metadata_to_proto(metadata)),
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
        let req = request.into_inner();
        let items = self
            .db
            .list_pod_cleanup_intents_for_node(&req.node_name)
            .await
            .map_err(|err| Status::internal(err.to_string()))?
            .into_iter()
            .map(pod_cleanup_intent_to_proto)
            .collect();
        Ok(Response::new(
            generated::ListPodCleanupIntentsForNodeResponse { items },
        ))
    }

    async fn delete_pod_cleanup_intent(
        &self,
        request: Request<generated::DeletePodCleanupIntentRequest>,
    ) -> std::result::Result<Response<generated::DeletePodCleanupIntentResponse>, Status> {
        self.require_steady_state_auth(&request).await?;
        let caller = caller_node_authority(&request);
        let req = request.into_inner();
        // NodeRestriction: a node may only clear its own pod cleanup intents.
        enforce_node_authority(&caller, &req.node_name)?;
        self.db
            .delete_pod_cleanup_intent(
                &req.node_name,
                &req.namespace,
                &req.pod_name,
                &req.pod_uid,
                &req.reason,
            )
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
        let req = request.into_inner();
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
        if !controlplane_token_authenticated
            && !handler.is_controlplane_member(&req.node_name).await
        {
            return Err(Status::permission_denied(
                "JoinAsControlplane requires a valid controlplane bootstrap token (first join) or an existing controlplane membership (rejoin)",
            ));
        }
        let observed_ip = remote_addr.map(|addr| addr.ip());
        let dataplane =
            validate_controlplane_join_dataplane_metadata_with_endpoint(&req, observed_ip)
                .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let raft_addr = raft_addr_with_observed_host(&req.addr, observed_ip)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let outcome = handler
            .join(
                req.node_id,
                raft_addr,
                req.node_name,
                req.as_learner,
                Some(req.node_internal_ip).filter(|value| !value.trim().is_empty()),
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
    generated::ResourceObject {
        api_version: resource.api_version.clone(),
        kind: resource.kind.clone(),
        namespace: resource.namespace.clone(),
        name: resource.name.clone(),
        uid: resource.uid.clone(),
        resource_version: resource.resource_version,
        data_json: serde_json::to_vec(&resource.data).unwrap_or_default(),
    }
}

fn node_subnet_to_proto(subnet: crate::datastore::NodeSubnet) -> generated::NodeSubnetObject {
    let forwarded: crate::replication::protocol::ForwardedNodeSubnet = subnet.into();
    generated::NodeSubnetObject {
        node_name: forwarded.node_name,
        subnet: forwarded.subnet,
        subnet_base_int: forwarded.subnet_base_int,
        vtep_ip: forwarded.vtep_ip,
        vtep_mac: forwarded.vtep_mac,
        node_ip: forwarded.node_ip,
        mode: forwarded.mode,
        hostport_range: forwarded.hostport_range,
    }
}

fn dataplane_metadata_to_proto(
    metadata: DataplanePeerMetadata,
) -> generated::DataplaneMetadataObject {
    generated::DataplaneMetadataObject {
        node_name: metadata.node_name,
        mode: metadata.mode.as_str().to_string(),
        encryption: metadata.encryption.as_str().to_string(),
        public_key: metadata.public_key.map(|key| key.to_string()),
        endpoint: metadata.endpoint.to_string(),
        port: metadata.port.map(u32::from),
    }
}

fn pod_cleanup_intent_to_proto(
    intent: crate::datastore::PodCleanupIntent,
) -> generated::PodCleanupIntentObject {
    generated::PodCleanupIntentObject {
        node_name: intent.node_name,
        namespace: intent.namespace,
        pod_name: intent.pod_name,
        pod_uid: intent.pod_uid,
        reason: intent.reason,
        resource_version: intent.resource_version,
        created_at_ms: intent.created_at_ms,
        pod_data_json: serde_json::to_vec(&intent.pod_data).unwrap_or_default(),
    }
}

fn resource_from_event(event: &crate::watch::WatchEvent) -> crate::datastore::Resource {
    Resource::from_watch_event_ref(event)
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
fn watch_heartbeat_proto(api_version: &str, kind: &str, last_rv: i64) -> generated::WatchEvent {
    let hb = crate::watch::WatchEvent::bookmark_typed(last_rv, api_version, kind);
    let resource = resource_from_event(&hb);
    generated::WatchEvent {
        event_type: watch_event_type(&hb).to_string(),
        resource: Some(resource_to_proto(&resource)),
    }
}

fn watch_event_matches(
    event: &crate::watch::WatchEvent,
    req: &generated::WatchResourcesRequest,
) -> bool {
    WatchEventSelection::new(&req.api_version, &req.kind)
        .namespace(req.namespace.as_deref())
        .label_selector(req.label_selector.as_deref())
        .field_selector(req.field_selector.as_deref())
        .matches(event)
}

fn watch_cursor_error_to_status(err: crate::watch::WatchCursorError, accepted_rv: i64) -> Status {
    match err {
        crate::watch::WatchCursorError::Expired => Status::out_of_range(format!(
            "WatchResources replay window expired: resume rv {accepted_rv} requires relist"
        )),
        crate::watch::WatchCursorError::Replay(err) => {
            Status::internal(format!("replay WatchResources failed: {err}"))
        }
        crate::watch::WatchCursorError::Closed => Status::unavailable("watch stream closed"),
    }
}

/// Complete when the raft leadership signal reports this node is no longer the
/// leader (or its sender is dropped). Used by the gRPC watch stream loop
/// (`watch_resources`) to terminate promptly on a leadership change. Checks the
/// current value first, then awaits the next change. `watch::Receiver::changed`
/// is cancel-safe, so polling this inside a `select!` each loop iteration (and
/// dropping the pending future when the broadcast recv wins) loses no
/// transition: the next iteration re-checks the current value.
async fn watch_leadership_lost(leader_rx: &mut tokio::sync::watch::Receiver<bool>) {
    if !*leader_rx.borrow() {
        return;
    }
    while leader_rx.changed().await.is_ok() {
        if !*leader_rx.borrow() {
            return;
        }
    }
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

async fn refresh_node_external_ip_from_dataplane(
    db: &dyn DatastoreBackend,
    dataplane: &DataplanePeerMetadata,
) -> Result<()> {
    let Some(resource) = db
        .get_resource("v1", "Node", None, &dataplane.node_name)
        .await?
    else {
        return Ok(());
    };
    let mut data = (*resource.data).clone();
    if !crate::kubelet::node::stamp_node_routing_metadata_and_external_ip_from_store(
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

async fn refresh_local_node_external_ip_from_observed_endpoint(
    db: &dyn DatastoreBackend,
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
    crate::kubelet::node::update_existing_node_external_ip_if_changed(
        db,
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::datastore::backend::{DatastoreBackend, DatastoreHandle};
    use crate::datastore::command::{
        COMMAND_CODEC_VERSION, CommandId, CommandMeta, StorageCommand,
    };
    use crate::datastore::types::ResourcePreconditions;
    use crate::replication::grpc::generated::replication_client::ReplicationClient;
    use crate::replication::grpc::generated::replication_server::Replication;
    use crate::replication::grpc::raft_rpc::{
        ControlplaneJoinHandler, ControlplaneJoinOutcome, RaftRpcRouterError,
    };
    use crate::replication::grpc::{
        generated::{
            self, ClusterMembershipRequest, JoinRequest, JoinRole, MetadataRequest, SnapshotRequest,
        },
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
                command_id: CommandId::new(),
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
            })
            .await
            .unwrap()
            .into_inner();

        db.broadcast_watch_event(crate::datastore::PendingWatchEvent {
            event: crate::watch::WatchEvent::modified(serde_json::json!({
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
        });

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
        let resource = event.resource.expect("watch event should carry resource");
        assert_eq!(resource.name, "resume-old");
        assert!(resource.resource_version > resume_rv);
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
            status.message().contains("requires relist"),
            "status should tell the worker to relist, got {status:?}"
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

    #[async_trait::async_trait]
    impl ControlplaneJoinHandler for AcceptingControlplaneJoinHandler {
        async fn join(
            &self,
            _node_id: u64,
            _addr: String,
            _node_name: String,
            as_learner: bool,
            _node_internal_ip: Option<String>,
        ) -> Result<ControlplaneJoinOutcome, RaftRpcRouterError> {
            Ok(ControlplaneJoinOutcome::Accepted {
                voter_count_after: if as_learner { 1 } else { 2 },
                admitted_as_learner: as_learner,
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
            _node_id: u64,
            _addr: String,
            _node_name: String,
            as_learner: bool,
            _node_internal_ip: Option<String>,
        ) -> Result<ControlplaneJoinOutcome, RaftRpcRouterError> {
            Ok(ControlplaneJoinOutcome::Accepted {
                voter_count_after: if as_learner { 1 } else { 2 },
                admitted_as_learner: as_learner,
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
            node_id: u64,
            addr: String,
            node_name: String,
            as_learner: bool,
            node_internal_ip: Option<String>,
        ) -> Result<ControlplaneJoinOutcome, RaftRpcRouterError> {
            self.calls
                .lock()
                .expect("recording join handler mutex poisoned")
                .push(RecordedControlplaneJoin {
                    node_id,
                    addr,
                    node_name,
                    as_learner,
                    node_internal_ip,
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

        let result = grpc
            .apply_outbox(request_with_node_client_cert(
                generated::ApplyOutboxRequest {
                    idempotency_key: "dataplane-worker-2-from-worker-1".to_string(),
                    operation: crate::kubelet::outbox::payload::OutboxOperation::NodeDataplane
                        .as_str()
                        .to_string(),
                    payload_proto: payload,
                    authoring_node: "worker-1".to_string(),
                },
                "worker-1",
            ))
            .await;
        let Err(status) = result else {
            panic!("node dataplane outbox must be bound to authoring node");
        };

        assert_eq!(status.code(), tonic::Code::PermissionDenied);
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
    async fn observed_leader_endpoint_stamps_local_node_external_ip() {
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

        super::refresh_local_node_external_ip_from_observed_endpoint(&db, "leader-a", "10.99.0.10")
            .await
            .expect("observed leader endpoint should update local Node");

        let node = db
            .get_resource("v1", "Node", None, "leader-a")
            .await
            .unwrap()
            .expect("leader Node should exist");
        let addresses = node
            .data
            .pointer("/status/addresses")
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
    async fn get_cluster_membership_rpc_returns_voters() {
        let db = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let cluster_id = db
            .get_klights_meta(crate::bootstrap::cluster_meta::KEY_CLUSTER_ID)
            .await
            .unwrap()
            .unwrap();
        crate::bootstrap::cluster_meta::write_cluster_membership(
            db.as_ref(),
            &crate::control_plane::client::membership::ClusterMembership {
                cluster_id: cluster_id.clone(),
                voters: vec!["mn-leader".to_string(), "mn-leader-2".to_string()],
                term: 3,
                leader_hint: Some("mn-leader-2".to_string()),
            },
        )
        .await
        .unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor));
        let grpc = super::GrpcReplicationServer::new(service, db);

        let response = grpc
            .get_cluster_membership(request_with_node_client_cert(
                ClusterMembershipRequest {},
                "worker-1",
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.cluster_id, cluster_id);
        assert_eq!(response.voters, vec!["mn-leader", "mn-leader-2"]);
        assert_eq!(response.term, 3);
        assert_eq!(response.leader_hint, "mn-leader-2");
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

        grpc.renew_node_lease(request_with_node_client_cert(
            generated::RenewNodeLeaseRequest {
                node_name: "worker-1".to_string(),
                renew_time: "2026-05-25T00:00:10Z".to_string(),
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
        assert_eq!(observed.renew_time_string(), "2026-05-25T00:00:10Z");
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

        let mut request = request_with_node_client_cert(
            generated::JoinAsControlplaneRequest {
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
            },
            "mn-controlplane2",
        );
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
    async fn accepted_controlplane_join_persists_dataplane_metadata() {
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
            node_id: 2,
            addr: "https://192.0.2.20:7679".to_string(),
            node_name: "mn-controlplane2".to_string(),
            as_learner: false,
            dataplane_public_key: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string(),
            dataplane_endpoint: "192.0.2.20".to_string(),
            dataplane_port: 7679,
            dataplane_mode: "root".to_string(),
            dataplane_encryption: "enabled".to_string(),
            node_internal_ip: "172.31.20.2".to_string(),
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
            node_id: 2,
            addr: "https://172.31.14.2:7679".to_string(),
            node_name: "mn-controlplane2".to_string(),
            as_learner: false,
            dataplane_public_key: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string(),
            dataplane_endpoint: "172.31.14.2".to_string(),
            dataplane_port: 7679,
            dataplane_mode: "root".to_string(),
            dataplane_encryption: "enabled".to_string(),
            node_internal_ip: "172.31.14.2".to_string(),
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
                node_id: 2,
                addr: "https://127.0.0.1:7679".to_string(),
                node_name: "mn-controlplane2".to_string(),
                as_learner: false,
                node_internal_ip: Some("172.31.14.2".to_string()),
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
        let event = super::watch_heartbeat_proto("v1", "Pod", 4242);
        assert_eq!(event.event_type, "BOOKMARK");
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
}
