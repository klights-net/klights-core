//! Top-level root-owned composition helpers for gRPC integration tests.
//!
//! The reusable transport crate deliberately cannot depend on the concrete
//! datastore, controller, or authentication implementations assembled here.

#![cfg(test)]

use std::path::PathBuf;
use std::sync::Arc;

use crate::datastore::backend::DatastoreHandle;
use klights_replication::ReplicationService;

pub(crate) type GrpcReplicationServer = klights_leader_rpc::server::GrpcReplicationServer;

struct TestClusterMetadataRead;

struct FixedRvTestClusterMetadataRead {
    current_rv: i64,
}

impl klights_cluster_store::ClusterMetadataRead for TestClusterMetadataRead {
    fn read_cluster_metadata(
        &self,
    ) -> klights_cluster_store::ClusterMetadataFuture<
        '_,
        klights_cluster_store::PersistedClusterMetadata,
    > {
        Box::pin(async { Ok(test_cluster_metadata(0)) })
    }
}

impl klights_cluster_store::ClusterMetadataRead for FixedRvTestClusterMetadataRead {
    fn read_cluster_metadata(
        &self,
    ) -> klights_cluster_store::ClusterMetadataFuture<
        '_,
        klights_cluster_store::PersistedClusterMetadata,
    > {
        let current_rv = self.current_rv;
        Box::pin(async move { Ok(test_cluster_metadata(current_rv)) })
    }
}

fn test_cluster_metadata(current_rv: i64) -> klights_cluster_store::PersistedClusterMetadata {
    klights_cluster_store::PersistedClusterMetadata::new(
        klights_cluster_core::ClusterMetadata {
            cluster_id: "klights-test-cluster".to_string(),
            leader_epoch: 0,
            current_rv,
        },
        klights_cluster_store::SnapshotMembership::LegacyOmitted,
    )
}

pub(crate) fn replication_service(
    db: DatastoreHandle,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
) -> ReplicationService {
    replication_service_with_metadata(db, Arc::new(TestClusterMetadataRead), supervisor)
}

pub(crate) fn replication_service_with_metadata(
    db: DatastoreHandle,
    metadata: Arc<dyn klights_cluster_store::ClusterMetadataRead>,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
) -> ReplicationService {
    ReplicationService::new_with_ports(
        metadata,
        Arc::new(crate::bootstrap::bootstrap_token::DatastoreBootstrapTokenValidation::new(db)),
        supervisor,
    )
}

pub(crate) fn replication_service_with_progress(
    db: DatastoreHandle,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
    follower_progress: Arc<klights_replication::FollowerProgressHub>,
    current_rv: i64,
) -> ReplicationService {
    ReplicationService::new_with_ports_and_progress(
        Arc::new(FixedRvTestClusterMetadataRead { current_rv }),
        Arc::new(crate::bootstrap::bootstrap_token::DatastoreBootstrapTokenValidation::new(db)),
        supervisor,
        follower_progress,
    )
}

/// Serve one root-composed integration-test router over the same TLS-only
/// scheme required by the production leader client.
pub(crate) async fn serve_tls_test_app(app: axum::Router) -> (String, tokio::task::JoinHandle<()>) {
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper_util::server::conn::auto::Builder;
    use tokio_rustls::TlsAcceptor;
    use tower::ServiceExt as _;

    let _ = rustls::crypto::ring::default_provider().install_default();
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let certificate_der = rustls::pki_types::CertificateDer::from(certificate.cert.der().to_vec());
    let private_key_der = rustls::pki_types::PrivateKeyDer::Pkcs8(
        rustls::pki_types::PrivatePkcs8KeyDer::from(certificate.key_pair.serialize_der()),
    );
    let mut server_config =
        rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_no_client_auth()
            .with_single_cert(vec![certificate_der], private_key_der)
            .unwrap();
    server_config.alpn_protocols = vec![b"h2".to_vec()];
    let acceptor = TlsAcceptor::from(Arc::new(server_config));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("https://{}", listener.local_addr().unwrap());
    let handle = tokio::spawn(async move {
        while let Ok((stream, remote_addr)) = listener.accept().await {
            let local_addr = stream.local_addr().ok();
            let acceptor = acceptor.clone();
            let app = app.clone();
            tokio::spawn(async move {
                let Ok(stream) = acceptor.accept(stream).await else {
                    return;
                };
                let io = TokioIo::new(stream);
                let service = hyper::service::service_fn(move |mut request| {
                    klights_leader_rpc::server::insert_tonic_tcp_connect_info(
                        &mut request,
                        local_addr,
                        Some(remote_addr),
                    );
                    app.clone().oneshot(request)
                });
                let _ = Builder::new(TokioExecutor::new())
                    .serve_connection_with_upgrades(io, service)
                    .await;
            });
        }
    });
    (endpoint, handle)
}

pub(crate) trait GrpcReplicationServerTestExt: Sized {
    fn new(service: Arc<ReplicationService>, db: DatastoreHandle) -> Self;

    fn new_with_passive_reads(
        service: Arc<ReplicationService>,
        db: DatastoreHandle,
        passive_reads: crate::datastore::selector::PassiveReadPorts,
    ) -> Self;

    fn new_with_controller_dispatcher(
        service: Arc<ReplicationService>,
        db: DatastoreHandle,
        controller_dispatcher: Arc<crate::controllers::ControllerDispatcher>,
    ) -> Self;

    fn new_with_node_lease_tracker(
        service: Arc<ReplicationService>,
        db: DatastoreHandle,
        node_lease_tracker: Arc<crate::node_lease_tracker::NodeLeaseTracker>,
    ) -> Self;

    fn with_namespace(self, data_root: &str) -> Self;

    fn with_leader_gate(self, is_leader_rx: tokio::sync::watch::Receiver<bool>) -> Self;
}

impl GrpcReplicationServerTestExt for GrpcReplicationServer {
    fn new(service: Arc<ReplicationService>, db: DatastoreHandle) -> Self {
        build_test_server(
            service,
            db,
            crate::datastore::test_support::unused_fail_closed_passive_read_ports(),
            None,
            None,
        )
    }

    fn new_with_passive_reads(
        service: Arc<ReplicationService>,
        db: DatastoreHandle,
        passive_reads: crate::datastore::selector::PassiveReadPorts,
    ) -> Self {
        build_test_server(service, db, passive_reads, None, None)
    }

    fn new_with_controller_dispatcher(
        service: Arc<ReplicationService>,
        db: DatastoreHandle,
        controller_dispatcher: Arc<crate::controllers::ControllerDispatcher>,
    ) -> Self {
        build_test_server(
            service,
            db,
            crate::datastore::test_support::unused_fail_closed_passive_read_ports(),
            Some(controller_dispatcher),
            None,
        )
    }

    fn new_with_node_lease_tracker(
        service: Arc<ReplicationService>,
        db: DatastoreHandle,
        node_lease_tracker: Arc<crate::node_lease_tracker::NodeLeaseTracker>,
    ) -> Self {
        build_test_server(
            service,
            db,
            crate::datastore::test_support::unused_fail_closed_passive_read_ports(),
            None,
            Some(node_lease_tracker),
        )
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
        self.with_authority(crate::authority_adapter::TestBooleanWatchAuthority::new(
            is_leader_rx,
        ))
    }
}

fn build_test_server(
    service: Arc<ReplicationService>,
    db: DatastoreHandle,
    passive_reads: crate::datastore::selector::PassiveReadPorts,
    controller_dispatcher: Option<Arc<crate::controllers::ControllerDispatcher>>,
    node_lease_tracker: Option<Arc<crate::node_lease_tracker::NodeLeaseTracker>>,
) -> GrpcReplicationServer {
    let node_lease_tracker = node_lease_tracker
        .unwrap_or_else(|| Arc::new(crate::node_lease_tracker::NodeLeaseTracker::new()));
    let local = Arc::new(
        crate::control_plane::client::local::LocalApiClient::new_with_node_lease_tracker_and_passive_reads(
            db.clone(),
            passive_reads,
            "grpc-test".to_string(),
            node_lease_tracker,
            crate::control_plane::client::local::always_leader_watch(),
        ),
    );
    if let Some(dispatcher) = controller_dispatcher {
        local.set_controller_dispatcher(dispatcher);
    }
    local.set_non_pod_finalization(Arc::new(
        crate::gc_delete_adapter::GcNonPodFinalizationAdapter::new(db),
    ));
    let projected_token = Arc::new(
        crate::control_plane::client::local::AuthenticatedProjectedTokenIssuer::new(local.clone()),
    );
    let ports =
        klights_leader_rpc::server::ReplicationServerPorts::from_shared(local, projected_token);
    let supervisor = service.task_supervisor();
    klights_leader_rpc::server::GrpcReplicationServer::new_with_ports(
        klights_leader_rpc::server::GrpcReplicationRuntimePorts::from_shared(
            crate::bootstrap::grpc_runtime_adapter::GrpcReplicationRuntimeAdapter::new(service),
        ),
        ports,
        Arc::new(
            crate::bootstrap::auth_adapters::AuthReplicationPeerAuthenticator::new(
                supervisor.clone(),
            ),
        ),
        Arc::new(
            crate::bootstrap::auth_adapters::AuthControlplaneCredentialIssuer::new(
                Arc::new(klights_auth::clock::SystemClock),
                supervisor,
            ),
        ),
        Arc::new(chrono::Utc::now),
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn mount_service_full(
    app: axum::Router,
    service: Arc<ReplicationService>,
    db: DatastoreHandle,
    controller_dispatcher: Option<Arc<crate::controllers::ControllerDispatcher>>,
    node_lease_tracker: Option<Arc<crate::node_lease_tracker::NodeLeaseTracker>>,
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
    let passive_reads = crate::datastore::test_support::unused_fail_closed_passive_read_ports();
    mount_service_full_with_passive_reads(
        app,
        service,
        db,
        passive_reads,
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
    db: DatastoreHandle,
    passive_reads: crate::datastore::selector::PassiveReadPorts,
    transport_policy: Arc<klights_leader_rpc::transport_policy::GrpcTransportPolicy>,
) -> axum::Router {
    mount_service_full_with_passive_reads(
        app,
        service,
        db,
        passive_reads,
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
    db: DatastoreHandle,
    passive_reads: crate::datastore::selector::PassiveReadPorts,
    is_leader_rx: tokio::sync::watch::Receiver<bool>,
    transport_policy: Arc<klights_leader_rpc::transport_policy::GrpcTransportPolicy>,
) -> axum::Router {
    mount_service_full_with_passive_reads(
        app,
        service,
        db,
        passive_reads,
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
    db: DatastoreHandle,
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
    db: DatastoreHandle,
    controller_dispatcher: Option<Arc<crate::controllers::ControllerDispatcher>>,
    node_lease_tracker: Option<Arc<crate::node_lease_tracker::NodeLeaseTracker>>,
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

#[allow(clippy::too_many_arguments)]
fn mount_service_full_with_passive_reads(
    app: axum::Router,
    service: Arc<ReplicationService>,
    db: DatastoreHandle,
    passive_reads: crate::datastore::selector::PassiveReadPorts,
    controller_dispatcher: Option<Arc<crate::controllers::ControllerDispatcher>>,
    node_lease_tracker: Option<Arc<crate::node_lease_tracker::NodeLeaseTracker>>,
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
    let node_lease_tracker = node_lease_tracker
        .unwrap_or_else(|| Arc::new(crate::node_lease_tracker::NodeLeaseTracker::new()));
    let local = Arc::new(
        crate::control_plane::client::local::LocalApiClient::new_with_node_lease_tracker_and_passive_reads(
            db.clone(),
            passive_reads,
            "grpc-test".to_string(),
            node_lease_tracker,
            crate::control_plane::client::local::always_leader_watch(),
        ),
    );
    if let Some(dispatcher) = controller_dispatcher {
        local.set_controller_dispatcher(dispatcher);
    }
    local.set_non_pod_finalization(Arc::new(
        crate::gc_delete_adapter::GcNonPodFinalizationAdapter::new(db),
    ));
    let projected_token = Arc::new(
        crate::control_plane::client::local::AuthenticatedProjectedTokenIssuer::new(local.clone()),
    );
    let ports =
        klights_leader_rpc::server::ReplicationServerPorts::from_shared(local, projected_token);
    let supervisor = service.task_supervisor();
    let etc = PathBuf::from(data_root).join("etc");
    let authority = is_leader_rx.map(crate::authority_adapter::TestBooleanWatchAuthority::new);

    klights_leader_rpc::server::mount_service_full_production(
        app,
        klights_leader_rpc::server::GrpcReplicationRuntimePorts::from_shared(
            crate::bootstrap::grpc_runtime_adapter::GrpcReplicationRuntimeAdapter::new(service),
        ),
        ports,
        Arc::new(
            crate::bootstrap::auth_adapters::AuthReplicationPeerAuthenticator::new(
                supervisor.clone(),
            ),
        ),
        Arc::new(
            crate::bootstrap::auth_adapters::AuthControlplaneCredentialIssuer::new(
                Arc::new(klights_auth::clock::SystemClock),
                supervisor,
            ),
        ),
        Arc::new(chrono::Utc::now),
        raft_rpc_router,
        controlplane_join_handler,
        klights_leader_rpc::ReplicationRuntimeFiles {
            ca_cert: etc.join("ca.crt"),
            ca_key: etc.join("ca.key"),
            service_account_signing_key: etc.join("service-account-signing.key"),
        },
        authority.map(|authority| authority as Arc<dyn klights_leader_api::LeaderAuthority>),
        local_node_name,
        node_self_query,
        node_self_status,
        node_lifecycle_status,
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
