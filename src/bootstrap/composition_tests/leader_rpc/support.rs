//! Base-owned support for the root-composed leader-RPC integration suite.

#[path = "harness.rs"]
mod harness;
pub(crate) use harness::*;

use std::path::PathBuf;
use std::sync::Arc;

use klights_replication::ReplicationService;

pub(crate) type SqliteTestStore = Arc<klights_cluster_datastore::sqlite::embedded::Datastore>;

pub(crate) type GrpcReplicationServer = klights_leader_rpc::server::GrpcReplicationServer;

/// Root-composed SQLite fixture for leader-RPC tests.
pub(crate) async fn canonical_sqlite_fixture()
-> anyhow::Result<klights_cluster_datastore::sqlite::embedded::Datastore> {
    crate::bootstrap::composition::cluster_store::selector::canonical_sqlite_fixture().await
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OutboxPayload {
    pub(crate) command: klights_cluster_core::StorageCommand,
}

impl OutboxPayload {
    pub(crate) fn from_command(command: klights_cluster_core::StorageCommand) -> Self {
        Self { command }
    }

    pub(crate) fn encode_protobuf(&self) -> anyhow::Result<Vec<u8>> {
        Ok(
            klights_leader_rpc::storage_wire_codec::encode_outbox_payload_protobuf(
                &klights_cluster_core::OutboxPayload::new(self.command.clone()),
            )?,
        )
    }

    pub(crate) fn decode_protobuf(bytes: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            command: klights_leader_rpc::storage_wire_codec::decode_outbox_payload_protobuf(bytes)?
                .into_command(),
        })
    }
}

#[derive(Clone, Copy)]
pub(crate) enum BootstrapTokenScope {
    Worker,
    Controlplane,
}

pub(crate) async fn ensure_cluster_metadata(
    db: &klights_cluster_datastore::sqlite::embedded::Datastore,
) -> anyhow::Result<()> {
    IntegrationLeaderRpcComposition::ensure_cluster_metadata_for(db).await
}

pub(crate) async fn ensure_worker_bootstrap_token(
    db: &klights_cluster_datastore::sqlite::embedded::Datastore,
) -> anyhow::Result<String> {
    IntegrationLeaderRpcComposition::ensure_worker_bootstrap_token_for(db).await
}

pub(crate) async fn create_scoped_bootstrap_token_secret_for_test(
    db: &klights_cluster_datastore::sqlite::embedded::Datastore,
    scope: BootstrapTokenScope,
    token: &str,
) -> anyhow::Result<()> {
    IntegrationLeaderRpcComposition::create_scoped_bootstrap_token_for(
        db,
        token,
        matches!(scope, BootstrapTokenScope::Controlplane),
    )
    .await
}

pub(crate) fn sqlite_passive_read_ports(
    db: &klights_cluster_datastore::sqlite::embedded::Datastore,
) -> IntegrationPassiveReadPorts {
    IntegrationLeaderRpcComposition::passive_reads_for(db)
}

pub(crate) fn controller_dispatcher_for_test(
    db: &klights_cluster_datastore::sqlite::embedded::Datastore,
) -> Arc<klights_controllers::ControllerDispatcher> {
    IntegrationLeaderRpcComposition::controller_dispatcher(db)
}

pub(crate) fn test_file_process_executor() -> klights_supervisor::FileProcessExecutor {
    IntegrationLeaderRpcComposition::file_process_executor()
}

pub(crate) fn grpc_runtime(service: Arc<ReplicationService>) -> IntegrationLeaderRpcRuntime {
    IntegrationLeaderRpcRuntime::new(service)
}

pub(crate) fn positioned_watch(
    passive_reads: &IntegrationPassiveReadPorts,
    db: SqliteTestStore,
) -> klights_watch::PositionedWatchService {
    IntegrationLeaderRpcComposition::positioned_watch(passive_reads, db)
}

pub(crate) fn pod_log_follow_watch(
    positioned_watch: klights_watch::PositionedWatchService,
) -> klights_kubelet::node_api::logs::PodLogFollowWatchSource {
    IntegrationLeaderRpcComposition::pod_log_follow_watch(positioned_watch)
}

pub(crate) async fn seed_namespace(
    db: &klights_cluster_datastore::sqlite::embedded::Datastore,
    name: &str,
) {
    IntegrationLeaderRpcComposition::seed_namespace(db, name).await;
}

pub(crate) fn broadcast_watch_event(
    db: &klights_cluster_datastore::sqlite::embedded::Datastore,
    event: klights_watch::WatchEvent,
) {
    IntegrationLeaderRpcComposition::broadcast_watch_event(db, event);
}

pub(crate) fn local_node_ports(
    db: SqliteTestStore,
    node_name: String,
    applied_outbox: Arc<dyn klights_cluster_store::AppliedOutboxLedger>,
    committed_apply: Arc<dyn klights_cluster_store::PrivilegedCommittedRaftApply>,
    resource_reads: Arc<dyn klights_cluster_store::ClusterResourceRead>,
) -> IntegrationLeaderRpcNodePorts {
    IntegrationLeaderRpcComposition::new(db, applied_outbox, committed_apply, resource_reads)
        .local_node_ports(node_name)
}

pub(crate) fn local_network_ports(
    db: SqliteTestStore,
    node_name: String,
    applied_outbox: Arc<dyn klights_cluster_store::AppliedOutboxLedger>,
    committed_apply: Arc<dyn klights_cluster_store::PrivilegedCommittedRaftApply>,
    resource_reads: Arc<dyn klights_cluster_store::ClusterResourceRead>,
) -> IntegrationLeaderRpcLocalNetworkPorts {
    IntegrationLeaderRpcComposition::new(db, applied_outbox, committed_apply, resource_reads)
        .local_network_ports(node_name)
}

pub(crate) fn focused_dataplane(
    dataplane: klights_cluster_store::DataplanePeerMetadata,
) -> Result<klights_leader_api::NetworkDataplane, klights_leader_api::NetworkTopologyError> {
    IntegrationLeaderRpcComposition::focused_dataplane(dataplane)
}

pub(crate) fn replication_service(
    db: SqliteTestStore,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
    applied_outbox: Arc<dyn klights_cluster_store::AppliedOutboxLedger>,
    committed_apply: Arc<dyn klights_cluster_store::PrivilegedCommittedRaftApply>,
    resource_reads: Arc<dyn klights_cluster_store::ClusterResourceRead>,
) -> ReplicationService {
    IntegrationLeaderRpcComposition::new(db, applied_outbox, committed_apply, resource_reads)
        .replication_service(supervisor)
}

pub(crate) fn replication_service_with_progress(
    db: SqliteTestStore,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
    follower_progress: Arc<klights_replication::FollowerProgressHub>,
    current_rv: i64,
    applied_outbox: Arc<dyn klights_cluster_store::AppliedOutboxLedger>,
    committed_apply: Arc<dyn klights_cluster_store::PrivilegedCommittedRaftApply>,
    resource_reads: Arc<dyn klights_cluster_store::ClusterResourceRead>,
) -> ReplicationService {
    IntegrationLeaderRpcComposition::new(db, applied_outbox, committed_apply, resource_reads)
        .replication_service_with_progress(supervisor, follower_progress, current_rv)
}

pub(crate) async fn serve_tls_test_app(app: axum::Router) -> (String, tokio::task::JoinHandle<()>) {
    IntegrationLeaderRpcComposition::serve_tls_test_app(app).await
}

pub(crate) trait GrpcReplicationServerTestExt: Sized {
    fn new(
        service: Arc<ReplicationService>,
        db: SqliteTestStore,
        applied_outbox: Arc<dyn klights_cluster_store::AppliedOutboxLedger>,
        committed_apply: Arc<dyn klights_cluster_store::PrivilegedCommittedRaftApply>,
        resource_reads: Arc<dyn klights_cluster_store::ClusterResourceRead>,
    ) -> Self;
    fn new_with_passive_reads(
        service: Arc<ReplicationService>,
        db: SqliteTestStore,
        passive_reads: IntegrationPassiveReadPorts,
        applied_outbox: Arc<dyn klights_cluster_store::AppliedOutboxLedger>,
        committed_apply: Arc<dyn klights_cluster_store::PrivilegedCommittedRaftApply>,
        resource_reads: Arc<dyn klights_cluster_store::ClusterResourceRead>,
    ) -> Self;
    fn new_with_controller_dispatcher(
        service: Arc<ReplicationService>,
        db: SqliteTestStore,
        controller_dispatcher: Arc<klights_controllers::ControllerDispatcher>,
        applied_outbox: Arc<dyn klights_cluster_store::AppliedOutboxLedger>,
        committed_apply: Arc<dyn klights_cluster_store::PrivilegedCommittedRaftApply>,
        resource_reads: Arc<dyn klights_cluster_store::ClusterResourceRead>,
    ) -> Self;
    fn new_with_node_lease_tracker(
        service: Arc<ReplicationService>,
        db: SqliteTestStore,
        node_lease_tracker: Arc<klights_controllers::node_lease::NodeLeaseTracker>,
        applied_outbox: Arc<dyn klights_cluster_store::AppliedOutboxLedger>,
        committed_apply: Arc<dyn klights_cluster_store::PrivilegedCommittedRaftApply>,
        resource_reads: Arc<dyn klights_cluster_store::ClusterResourceRead>,
    ) -> Self;
    fn with_namespace(self, data_root: &str) -> Self;
    fn with_leader_gate(self, is_leader_rx: tokio::sync::watch::Receiver<bool>) -> Self;
}

impl GrpcReplicationServerTestExt for GrpcReplicationServer {
    fn new(
        service: Arc<ReplicationService>,
        db: SqliteTestStore,
        applied_outbox: Arc<dyn klights_cluster_store::AppliedOutboxLedger>,
        committed_apply: Arc<dyn klights_cluster_store::PrivilegedCommittedRaftApply>,
        resource_reads: Arc<dyn klights_cluster_store::ClusterResourceRead>,
    ) -> Self {
        IntegrationLeaderRpcComposition::new(db, applied_outbox, committed_apply, resource_reads)
            .server(service, None, None, None)
    }

    fn new_with_passive_reads(
        service: Arc<ReplicationService>,
        db: SqliteTestStore,
        passive_reads: IntegrationPassiveReadPorts,
        applied_outbox: Arc<dyn klights_cluster_store::AppliedOutboxLedger>,
        committed_apply: Arc<dyn klights_cluster_store::PrivilegedCommittedRaftApply>,
        resource_reads: Arc<dyn klights_cluster_store::ClusterResourceRead>,
    ) -> Self {
        IntegrationLeaderRpcComposition::new(db, applied_outbox, committed_apply, resource_reads)
            .server(service, Some(passive_reads), None, None)
    }

    fn new_with_controller_dispatcher(
        service: Arc<ReplicationService>,
        db: SqliteTestStore,
        controller_dispatcher: Arc<klights_controllers::ControllerDispatcher>,
        applied_outbox: Arc<dyn klights_cluster_store::AppliedOutboxLedger>,
        committed_apply: Arc<dyn klights_cluster_store::PrivilegedCommittedRaftApply>,
        resource_reads: Arc<dyn klights_cluster_store::ClusterResourceRead>,
    ) -> Self {
        IntegrationLeaderRpcComposition::new(db, applied_outbox, committed_apply, resource_reads)
            .server(service, None, Some(controller_dispatcher), None)
    }

    fn new_with_node_lease_tracker(
        service: Arc<ReplicationService>,
        db: SqliteTestStore,
        node_lease_tracker: Arc<klights_controllers::node_lease::NodeLeaseTracker>,
        applied_outbox: Arc<dyn klights_cluster_store::AppliedOutboxLedger>,
        committed_apply: Arc<dyn klights_cluster_store::PrivilegedCommittedRaftApply>,
        resource_reads: Arc<dyn klights_cluster_store::ClusterResourceRead>,
    ) -> Self {
        IntegrationLeaderRpcComposition::new(db, applied_outbox, committed_apply, resource_reads)
            .server(service, None, None, Some(node_lease_tracker))
    }

    fn with_namespace(self, data_root: &str) -> Self {
        let etc = PathBuf::from(data_root).join("etc");
        self.with_runtime_files(klights_leader_rpc::ReplicationRuntimeFiles {
            ca_cert: etc.join("ca.crt"),
            ca_key: etc.join("ca.key"),
            service_account_signing_key: etc.join("service-account-signing.key"),
        })
    }

    fn with_leader_gate(self, is_leader_rx: tokio::sync::watch::Receiver<bool>) -> Self {
        IntegrationLeaderRpcComposition::with_leader_gate(self, is_leader_rx)
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn mount_service_full(
    app: axum::Router,
    service: Arc<ReplicationService>,
    db: SqliteTestStore,
    controller_dispatcher: Option<Arc<klights_controllers::ControllerDispatcher>>,
    node_lease_tracker: Option<Arc<klights_controllers::node_lease::NodeLeaseTracker>>,
    raft_rpc_router: Option<Arc<dyn klights_leader_rpc::raft_rpc::RaftRpcRouter>>,
    controlplane_join_handler: Option<Arc<dyn klights_leader_api::ControlplaneJoinHandler>>,
    data_root: &str,
    is_leader_rx: Option<tokio::sync::watch::Receiver<bool>>,
    local_node_name: Option<String>,
    node_self_query: Option<Arc<dyn klights_leader_api::LeaderResourceQuery>>,
    node_self_status: Option<Arc<dyn klights_leader_api::LeaderNodeSelfStatus>>,
    node_lifecycle_status: Option<Arc<dyn klights_leader_api::LeaderNodeLifecycleStatus>>,
    transport_policy: Arc<klights_leader_rpc::transport_policy::GrpcTransportPolicy>,
) -> axum::Router {
    IntegrationLeaderRpcComposition::from_sqlite(db).mount_service_full(
        app,
        service,
        None,
        controller_dispatcher,
        node_lease_tracker,
        raft_rpc_router,
        controlplane_join_handler,
        data_root,
        is_leader_rx,
        local_node_name,
        node_self_query,
        node_self_status,
        node_lifecycle_status,
        transport_policy,
    )
}

pub(crate) fn mount_service_with_passive_reads(
    app: axum::Router,
    service: Arc<ReplicationService>,
    db: SqliteTestStore,
    passive_reads: IntegrationPassiveReadPorts,
    transport_policy: Arc<klights_leader_rpc::transport_policy::GrpcTransportPolicy>,
) -> axum::Router {
    IntegrationLeaderRpcComposition::from_sqlite(db).mount_service_full(
        app,
        service,
        Some(passive_reads),
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
        transport_policy,
    )
}

pub(crate) fn mount_service_with_passive_reads_and_leader_gate(
    app: axum::Router,
    service: Arc<ReplicationService>,
    db: SqliteTestStore,
    passive_reads: IntegrationPassiveReadPorts,
    is_leader_rx: tokio::sync::watch::Receiver<bool>,
    transport_policy: Arc<klights_leader_rpc::transport_policy::GrpcTransportPolicy>,
) -> axum::Router {
    IntegrationLeaderRpcComposition::from_sqlite(db).mount_service_full(
        app,
        service,
        Some(passive_reads),
        None,
        None,
        None,
        None,
        "",
        Some(is_leader_rx),
        None,
        None,
        None,
        None,
        transport_policy,
    )
}

pub(crate) fn mount_service(
    app: axum::Router,
    service: Arc<ReplicationService>,
    db: SqliteTestStore,
    transport_policy: Arc<klights_leader_rpc::transport_policy::GrpcTransportPolicy>,
) -> axum::Router {
    mount_service_full(
        app,
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
        transport_policy,
    )
}

pub(crate) fn mount_service_with_controller_dispatcher(
    app: axum::Router,
    service: Arc<ReplicationService>,
    db: SqliteTestStore,
    controller_dispatcher: Option<Arc<klights_controllers::ControllerDispatcher>>,
    node_lease_tracker: Option<Arc<klights_controllers::node_lease::NodeLeaseTracker>>,
    transport_policy: Arc<klights_leader_rpc::transport_policy::GrpcTransportPolicy>,
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

pub(crate) fn mount_configured_test_service(
    app: axum::Router,
    grpc: GrpcReplicationServer,
    transport_policy: Arc<klights_leader_rpc::transport_policy::GrpcTransportPolicy>,
) -> axum::Router {
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
