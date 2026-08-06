//! Base-repository-only assembly for full-stack API integration tests.
//!
//! This module exists only behind `integration-test-harness`; normal builds
//! neither compile nor export it.

use std::sync::Arc;

use crate::datastore::DatastoreHandle;
use klights_reconcile_api::ControllerDispatcherPort as _;

/// Opaque root datastore capability for base-repository integration fixtures.
///
/// This alias is compiled only with `integration-test-harness`; production and
/// native-service APIs do not expose a datastore surface.
pub type IntegrationDatastoreHandle = DatastoreHandle;
pub type IntegrationWatchEvent = klights_watch::WatchEvent;
pub use klights_cluster_datastore::sqlite::embedded::{
    ResourceMutationPause as IntegrationResourceMutationPause,
    ResourceMutationPauseOperation as IntegrationResourceMutationPauseOperation,
};

pub struct IntegrationPassiveReadPorts {
    ports: crate::datastore::selector::PassiveReadPorts,
}

pub struct IntegrationLeaderRpcRuntime {
    runtime: Arc<crate::bootstrap::grpc_runtime_adapter::GrpcReplicationRuntimeAdapter>,
}

pub struct IntegrationLeaderRpcLocalNetworkPorts {
    local: Arc<crate::control_plane::client::local::LocalApiClient>,
}

pub struct IntegrationLeaderRpcNodePorts {
    local: Arc<crate::control_plane::client::local::LocalApiClient>,
}

impl IntegrationLeaderRpcNodePorts {
    pub fn resource_query(&self) -> Arc<dyn klights_leader_api::LeaderResourceQuery> {
        self.local.clone()
    }

    pub fn lifecycle_status(&self) -> Arc<dyn klights_leader_api::LeaderNodeLifecycleStatus> {
        self.local.clone()
    }
}

pub struct IntegrationLeaderRpcNodeLocal {
    stores: crate::datastore::node_local::NodeLocalStores,
}

pub struct IntegrationLeaderRpcClaimedOutbox {
    pub operation: String,
    pub payload_proto: Vec<u8>,
}

impl IntegrationLeaderRpcNodeLocal {
    pub fn outbox(&self) -> klights_kubelet::node_outbox::Outbox {
        use std::sync::Arc;
        let stores = klights_kubelet::node_outbox::OutboxStores::new(
            self.stores.outbox_producer(),
            self.stores.outbox_dispatcher(),
            self.stores.pod_status_checkpoints(),
            self.stores.runtime_observation_checkpoints(),
            self.stores.outbox_status_stamps(),
        );
        klights_kubelet::node_outbox::Outbox::compose(
            stores,
            crate::bootstrap::composition_adapters::outbox_payload_codec_adapter::new_codec(),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(klights_supervisor::SystemWallClock),
        )
    }

    pub async fn claim_next_due_outbox(
        &self,
        now_ms: i64,
        lease_ms: i64,
        lease_token: &str,
    ) -> anyhow::Result<Option<IntegrationLeaderRpcClaimedOutbox>> {
        let request =
            klights_node_store::OutboxClaimRequest::try_new(now_ms, lease_ms, lease_token)?;
        self.stores
            .outbox_dispatcher()
            .claim_next_due_outbox(request)
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))
            .map(|row| {
                row.map(|row| IntegrationLeaderRpcClaimedOutbox {
                    operation: row.operation().to_string(),
                    payload_proto: row.payload().to_vec(),
                })
            })
    }
}

impl IntegrationLeaderRpcLocalNetworkPorts {
    pub fn resource_query(&self) -> Arc<dyn klights_leader_api::LeaderResourceQuery> {
        self.local.clone()
    }

    pub async fn register_node_dataplane(
        &self,
        dataplane: klights_leader_api::NetworkDataplane,
    ) -> Result<(), klights_leader_api::NetworkTopologyError> {
        klights_leader_api::LeaderNetworkTopologyCommand::register_node_dataplane(
            self.local.as_ref(),
            dataplane,
        )
        .await
    }
}

impl IntegrationLeaderRpcRuntime {
    pub fn new(service: Arc<klights_replication::ReplicationService>) -> Self {
        Self {
            runtime: crate::bootstrap::grpc_runtime_adapter::GrpcReplicationRuntimeAdapter::new(
                service,
            ),
        }
    }

    pub async fn exec_sync(
        &self,
        request: klights_node_api::NodeExecSyncRequest,
    ) -> Result<klights_node_api::NodeExecSyncResult, klights_node_api::ExecSetupError> {
        klights_node_api::NodeExec::exec_sync(self.runtime.as_ref(), request).await
    }

    pub async fn collect_metrics(
        &self,
        request: klights_node_api::NodeMetricsRequest,
    ) -> Result<klights_node_api::NodeMetricsResult, klights_node_api::NodeMetricsError> {
        klights_node_api::NodeMetrics::collect_metrics(self.runtime.as_ref(), request).await
    }

    pub async fn open_exec(
        &self,
        request: klights_node_api::NodeExecRequest,
    ) -> Result<Box<dyn klights_node_api::NodeExecSession>, klights_node_api::ExecSetupError> {
        klights_node_api::NodeExec::open_exec(self.runtime.as_ref(), request).await
    }
}

/// Opaque feature-only assembly of the root-owned leader-RPC test graph.
///
/// The datastore stays owned by this capability. Public operations expose
/// only the focused RPC, controller, and transport ports exercised by the
/// base-owned composition suite.
pub struct IntegrationLeaderRpcComposition {
    db: IntegrationDatastoreHandle,
}

impl IntegrationLeaderRpcComposition {
    pub fn new(db: IntegrationDatastoreHandle) -> Self {
        Self { db }
    }

    pub fn passive_reads_for(
        db: &crate::datastore::sqlite::Datastore,
    ) -> IntegrationPassiveReadPorts {
        IntegrationPassiveReadPorts {
            ports: crate::datastore::selector::sqlite_passive_read_ports(db),
        }
    }

    pub async fn ensure_cluster_metadata_for(
        db: &dyn crate::datastore::DatastoreBackend,
    ) -> anyhow::Result<()> {
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db).await
    }

    pub async fn ensure_worker_bootstrap_token_for(
        db: &dyn crate::datastore::DatastoreBackend,
    ) -> anyhow::Result<String> {
        crate::bootstrap::bootstrap_token::ensure_worker_bootstrap_token(db).await
    }

    pub async fn create_scoped_bootstrap_token_for(
        db: &dyn crate::datastore::DatastoreBackend,
        token: &str,
        controlplane: bool,
    ) -> anyhow::Result<()> {
        let scope = if controlplane {
            crate::bootstrap::bootstrap_token::BootstrapTokenScope::Controlplane
        } else {
            crate::bootstrap::bootstrap_token::BootstrapTokenScope::Worker
        };
        crate::bootstrap::bootstrap_token::create_scoped_bootstrap_token_secret_for_test(
            db, scope, token,
        )
        .await
    }

    pub fn default_queue_only_dispatcher() -> klights_controllers::ControllerDispatcher {
        crate::bootstrap::controller_adapters::controller_runtime_adapter::default_queue_only_dispatcher_for_test()
    }

    pub fn file_process_executor() -> klights_supervisor::FileProcessExecutor {
        klights_supervisor::FileProcessExecutor::new(Arc::new(
            klights_supervisor::TaskSupervisor::new(
                klights_supervisor::TaskCategoryConfig::default(),
            ),
        ))
    }

    pub async fn serve_tls_test_app(app: axum::Router) -> (String, tokio::task::JoinHandle<()>) {
        use hyper_util::rt::{TokioExecutor, TokioIo};
        use hyper_util::server::conn::auto::Builder;
        use tokio_rustls::TlsAcceptor;
        use tower::ServiceExt as _;

        let _ = rustls::crypto::ring::default_provider().install_default();
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .expect("test TLS certificate generation must succeed");
        let certificate_der =
            rustls::pki_types::CertificateDer::from(certificate.cert.der().to_vec());
        let private_key_der = rustls::pki_types::PrivateKeyDer::Pkcs8(
            rustls::pki_types::PrivatePkcs8KeyDer::from(certificate.key_pair.serialize_der()),
        );
        let mut server_config =
            rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .with_no_client_auth()
                .with_single_cert(vec![certificate_der], private_key_der)
                .expect("test TLS configuration must accept generated identity");
        server_config.alpn_protocols = vec![b"h2".to_vec()];
        let acceptor = TlsAcceptor::from(Arc::new(server_config));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test TLS listener must bind");
        let endpoint = format!(
            "https://{}",
            listener
                .local_addr()
                .expect("test TLS listener must expose its address")
        );
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
                        klights_apiserver::insert_tonic_tcp_connect_info(
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

    pub fn positioned_watch(
        passive_reads: &IntegrationPassiveReadPorts,
        db: IntegrationDatastoreHandle,
    ) -> klights_watch::PositionedWatchService {
        crate::bootstrap::composition_adapters::positioned_watch_adapter::for_test(
            &passive_reads.ports,
            db,
        )
    }

    pub fn pod_log_follow_watch(
        positioned_watch: klights_watch::PositionedWatchService,
    ) -> klights_kubelet::node_api::logs::PodLogFollowWatchSource {
        klights_kubelet::node_api::logs::PodLogFollowWatchSource::new(Arc::new(
            crate::bootstrap::kubelet_ports::DatastorePodWatchSource::new(Arc::new(
                positioned_watch,
            )),
        ))
    }

    pub async fn seed_namespace(db: &dyn crate::datastore::DatastoreBackend, name: &str) {
        let _ = db
            .create_namespace(name, serde_json::json!({"metadata": {"name": name}}))
            .await;
    }

    pub fn broadcast_watch_event(
        db: &dyn crate::datastore::DatastoreBackend,
        event: klights_watch::WatchEvent,
    ) {
        let pending = crate::datastore::staged_post_commit_from_event(event);
        db.commit_observation_sink().observe(&[pending]);
    }

    pub fn local_node_ports(
        db: IntegrationDatastoreHandle,
        node_name: String,
    ) -> IntegrationLeaderRpcNodePorts {
        IntegrationLeaderRpcNodePorts {
            local: Arc::new(Self::local_client(db, node_name)),
        }
    }

    pub fn local_network_ports(
        db: IntegrationDatastoreHandle,
        node_name: String,
    ) -> IntegrationLeaderRpcLocalNetworkPorts {
        IntegrationLeaderRpcLocalNetworkPorts {
            local: Arc::new(Self::local_client(db, node_name)),
        }
    }

    pub fn focused_dataplane(
        dataplane: klights_cluster_store::DataplanePeerMetadata,
    ) -> Result<klights_leader_api::NetworkDataplane, klights_leader_api::NetworkTopologyError>
    {
        crate::control_plane::client::focused_dataplane(dataplane)
    }

    pub async fn open_node_local(
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
        connection_key: &'static str,
    ) -> anyhow::Result<IntegrationLeaderRpcNodeLocal> {
        Ok(IntegrationLeaderRpcNodeLocal {
            stores: crate::datastore::node_local::selector::open_node_local(
                crate::datastore::backend_kind::BackendKind::Sqlite,
                None,
                supervisor,
                None,
                connection_key,
            )
            .await?,
        })
    }

    fn local_client(
        db: IntegrationDatastoreHandle,
        node_name: String,
    ) -> crate::control_plane::client::local::LocalApiClient {
        crate::control_plane::client::local::LocalApiClient::new_with_node_lease_tracker_and_passive_reads(
            db,
            crate::datastore::selector::unused_fail_closed_passive_read_ports(),
            node_name,
            Arc::new(klights_controllers::node_lease::NodeLeaseTracker::new_at(
                chrono::Utc::now(),
            )),
            crate::control_plane::client::local::always_leader_watch(),
            Self::file_process_executor(),
        )
    }

    pub async fn initialize_default_namespaces(&self) -> anyhow::Result<()> {
        klights_controllers::namespace::init_default_namespaces_with_ca_path(
            &Self::file_process_executor(),
            self.db.as_ref(),
            &crate::paths::ca_cert_path(&crate::paths::runtime_namespace()),
            chrono::DateTime::UNIX_EPOCH,
            &DeterministicControllerIdentity::default(),
        )
        .await
    }

    pub async fn snapshot(
        &self,
        after_rv: i64,
    ) -> anyhow::Result<Vec<klights_cluster_core::SnapshotRestoreOperation>> {
        let mut sink = IntegrationLeaderRpcVecSnapshotSink::default();
        crate::datastore::snapshot_export::stream_snapshot_commits(
            self.db.as_ref(),
            after_rv,
            &mut sink,
        )
        .await?;
        Ok(sink.operations)
    }

    pub async fn stream_snapshot(
        &self,
        after_rv: i64,
        sender: tokio::sync::mpsc::Sender<
            Result<klights_cluster_core::SnapshotRestoreOperation, tonic::Status>,
        >,
    ) -> anyhow::Result<()> {
        let mut sink = IntegrationLeaderRpcChannelSnapshotSink {
            sender: Some(sender),
        };
        crate::datastore::snapshot_export::stream_snapshot_commits(
            self.db.as_ref(),
            after_rv,
            &mut sink,
        )
        .await?;
        sink.sender.take();
        Ok(())
    }

    pub async fn register_node_at_addresses(
        &self,
        node_name: &str,
        profile: &klights_kubelet::node_config::NodeRegistrationProfile,
        dataplane_health: Option<&klights_network_api::DataplaneHealthSnapshot>,
        addresses: &klights_kubelet::node::NodeRegistrationAddresses,
    ) -> anyhow::Result<()> {
        let snapshot = klights_kubelet::node::NodeRegistrationSnapshot::capture_local(
            &Self::file_process_executor(),
            node_name,
            profile,
            addresses.clone(),
            None,
            None,
        )
        .await;
        crate::bootstrap::node_registration_adapter::register_node_snapshot(
            self.db.as_ref(),
            None,
            dataplane_health,
            &snapshot,
        )
        .await
    }

    pub async fn reconcile_service_endpoints(
        &self,
        request: klights_controllers::endpoints::ServiceEndpointBatchReconcileRequest<'_>,
    ) -> anyhow::Result<()> {
        let pod_store = crate::pod_repository_composition::new_pod_store(self.db.clone());
        klights_controllers::endpoints::reconcile_service_endpoints_batch(
            self.db.as_ref(),
            &pod_store,
            request,
        )
        .await
    }

    pub async fn ensure_cluster_metadata(&self) -> anyhow::Result<()> {
        Self::ensure_cluster_metadata_for(self.db.as_ref()).await
    }

    pub async fn ensure_worker_bootstrap_token(&self) -> anyhow::Result<String> {
        Self::ensure_worker_bootstrap_token_for(self.db.as_ref()).await
    }

    pub async fn create_scoped_bootstrap_token(
        &self,
        token: &str,
        controlplane: bool,
    ) -> anyhow::Result<()> {
        Self::create_scoped_bootstrap_token_for(self.db.as_ref(), token, controlplane).await
    }

    pub fn replication_service(
        &self,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> klights_replication::ReplicationService {
        self.replication_service_with_metadata(
            Arc::new(IntegrationLeaderRpcMetadataRead { current_rv: 0 }),
            supervisor,
        )
    }

    pub fn replication_service_with_metadata(
        &self,
        metadata: Arc<dyn klights_cluster_store::ClusterMetadataRead>,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> klights_replication::ReplicationService {
        klights_replication::ReplicationService::new_with_ports(
            metadata,
            Arc::new(
                crate::bootstrap::bootstrap_token::DatastoreBootstrapTokenValidation::new(
                    self.db.clone(),
                ),
            ),
            supervisor,
        )
    }

    pub fn replication_service_with_progress(
        &self,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
        follower_progress: Arc<klights_replication::FollowerProgressHub>,
        current_rv: i64,
    ) -> klights_replication::ReplicationService {
        klights_replication::ReplicationService::new_with_ports_and_progress(
            Arc::new(IntegrationLeaderRpcMetadataRead { current_rv }),
            Arc::new(
                crate::bootstrap::bootstrap_token::DatastoreBootstrapTokenValidation::new(
                    self.db.clone(),
                ),
            ),
            supervisor,
            follower_progress,
        )
    }

    pub fn server(
        &self,
        service: Arc<klights_replication::ReplicationService>,
        passive_reads: Option<IntegrationPassiveReadPorts>,
        controller_dispatcher: Option<Arc<klights_controllers::ControllerDispatcher>>,
        node_lease_tracker: Option<Arc<klights_controllers::node_lease::NodeLeaseTracker>>,
    ) -> klights_leader_rpc::server::GrpcReplicationServer {
        let passive_reads = passive_reads.unwrap_or_else(|| IntegrationPassiveReadPorts {
            ports: crate::datastore::selector::unused_fail_closed_passive_read_ports(),
        });
        let (runtime, ports, peer_auth, credential_issuer, clock) = self.server_parts(
            service,
            passive_reads,
            controller_dispatcher,
            node_lease_tracker,
        );
        klights_leader_rpc::server::GrpcReplicationServer::new_with_ports(
            runtime,
            ports,
            peer_auth,
            credential_issuer,
            clock,
        )
    }

    pub fn with_leader_gate(
        server: klights_leader_rpc::server::GrpcReplicationServer,
        is_leader_rx: tokio::sync::watch::Receiver<bool>,
    ) -> klights_leader_rpc::server::GrpcReplicationServer {
        server.with_authority(
            crate::bootstrap::composition_adapters::authority_adapter::TestBooleanWatchAuthority::new(
                is_leader_rx,
            ),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn mount_service_full(
        &self,
        app: axum::Router,
        service: Arc<klights_replication::ReplicationService>,
        passive_reads: Option<IntegrationPassiveReadPorts>,
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
        let passive_reads = passive_reads.unwrap_or_else(|| IntegrationPassiveReadPorts {
            ports: crate::datastore::selector::unused_fail_closed_passive_read_ports(),
        });
        let (runtime, ports, peer_auth, credential_issuer, clock) = self.server_parts(
            service,
            passive_reads,
            controller_dispatcher,
            node_lease_tracker,
        );
        let etc = std::path::PathBuf::from(data_root).join("etc");
        let authority = is_leader_rx.map(
            crate::bootstrap::composition_adapters::authority_adapter::TestBooleanWatchAuthority::new,
        );
        klights_leader_rpc::server::mount_service_full_production(
            app,
            runtime,
            ports,
            peer_auth,
            credential_issuer,
            clock,
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

    fn server_parts(
        &self,
        service: Arc<klights_replication::ReplicationService>,
        passive_reads: IntegrationPassiveReadPorts,
        controller_dispatcher: Option<Arc<klights_controllers::ControllerDispatcher>>,
        node_lease_tracker: Option<Arc<klights_controllers::node_lease::NodeLeaseTracker>>,
    ) -> (
        klights_leader_rpc::server::GrpcReplicationRuntimePorts,
        klights_leader_rpc::server::ReplicationServerPorts,
        Arc<dyn klights_leader_rpc::server::ReplicationPeerAuthenticator>,
        Arc<dyn klights_leader_rpc::server::ControlplaneCredentialIssuer>,
        Arc<dyn klights_leader_rpc::server::GrpcWallClock>,
    ) {
        let node_lease_tracker = node_lease_tracker.unwrap_or_else(|| {
            Arc::new(klights_controllers::node_lease::NodeLeaseTracker::new_at(
                chrono::Utc::now(),
            ))
        });
        let local = Arc::new(
            crate::control_plane::client::local::LocalApiClient::new_with_node_lease_tracker_and_passive_reads(
                self.db.clone(),
                passive_reads.ports,
                "grpc-test".to_string(),
                node_lease_tracker,
                crate::control_plane::client::local::always_leader_watch(),
                Self::file_process_executor(),
            ),
        );
        if let Some(dispatcher) = controller_dispatcher {
            local.set_controller_dispatcher(dispatcher);
        }
        local.set_non_pod_finalization(Arc::new(
            crate::bootstrap::controller_adapters::gc_delete_adapter::GcNonPodFinalizationAdapter::new(
                self.db.clone(),
            ),
        ));
        let projected_token = Arc::new(
            crate::control_plane::client::local::AuthenticatedProjectedTokenIssuer::new(
                local.clone(),
            ),
        );
        let ports =
            klights_leader_rpc::server::ReplicationServerPorts::from_shared(local, projected_token);
        let supervisor = service.task_supervisor();
        (
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
}

struct IntegrationLeaderRpcMetadataRead {
    current_rv: i64,
}

#[derive(Default)]
struct IntegrationLeaderRpcVecSnapshotSink {
    operations: Vec<klights_cluster_core::SnapshotRestoreOperation>,
}

impl crate::datastore::snapshot_export::SnapshotCommitSink for IntegrationLeaderRpcVecSnapshotSink {
    async fn push(
        &mut self,
        operation: klights_cluster_core::SnapshotRestoreOperation,
    ) -> anyhow::Result<()> {
        self.operations.push(operation);
        Ok(())
    }
}

struct IntegrationLeaderRpcChannelSnapshotSink {
    sender: Option<
        tokio::sync::mpsc::Sender<
            Result<klights_cluster_core::SnapshotRestoreOperation, tonic::Status>,
        >,
    >,
}

impl crate::datastore::snapshot_export::SnapshotCommitSink
    for IntegrationLeaderRpcChannelSnapshotSink
{
    async fn push(
        &mut self,
        operation: klights_cluster_core::SnapshotRestoreOperation,
    ) -> anyhow::Result<()> {
        let Some(sender) = self.sender.as_ref() else {
            return Ok(());
        };
        sender
            .send(Ok(operation))
            .await
            .map_err(|error| anyhow::anyhow!("snapshot test receiver dropped: {error}"))
    }

    fn finish(&mut self) -> anyhow::Result<()> {
        self.sender.take();
        Ok(())
    }
}

impl klights_cluster_store::ClusterMetadataRead for IntegrationLeaderRpcMetadataRead {
    fn read_cluster_metadata(
        &self,
    ) -> klights_cluster_store::ClusterMetadataFuture<
        '_,
        klights_cluster_store::PersistedClusterMetadata,
    > {
        let current_rv = self.current_rv;
        Box::pin(async move {
            Ok(klights_cluster_store::PersistedClusterMetadata::new(
                klights_cluster_core::ClusterMetadata {
                    cluster_id: "klights-test-cluster".to_string(),
                    leader_epoch: 0,
                    current_rv,
                },
                klights_cluster_store::SnapshotMembership::LegacyOmitted,
            ))
        })
    }
}

/// Opaque feature-gated capability for the root's exact Raft composition.
///
/// The concrete datastore and adapter implementations remain private. Tests
/// receive only focused Raft ports and explicit fixture operations.
pub struct IntegrationRaftComposition {
    db: Arc<crate::datastore::sqlite::Datastore>,
}

impl IntegrationRaftComposition {
    pub const SNAPSHOT_EMIT_PAGE_SIZE: usize =
        crate::datastore::snapshot_export::SNAPSHOT_EMIT_PAGE_SIZE;

    pub fn new(db: Arc<crate::datastore::sqlite::Datastore>) -> Self {
        Self { db }
    }

    pub fn store_ports(&self) -> klights_replication::node::RaftStorePorts {
        crate::bootstrap::composition_adapters::cluster_store_replication_adapter::raft_store_ports_for_test(
            self.db.clone(),
        )
    }

    pub fn commit_materializer(
        &self,
    ) -> Arc<dyn klights_replication::materializer::RaftCommitMaterializer> {
        let db: IntegrationDatastoreHandle = self.db.clone();
        Arc::new(
            crate::bootstrap::composition_adapters::cluster_store_replication_adapter::DatastoreRaftCommitMaterializer::new(db),
        )
    }

    pub async fn state_machine(
        &self,
        applied_state: Arc<dyn klights_node_store::RaftAppliedStateDurability>,
        snapshot_applied_state: Arc<dyn klights_node_store::RaftAppliedStateDurability>,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> (
        klights_replication::state_machine::SqliteRaftStateMachine<
            klights_replication::snapshot::SqliteRaftSnapshotBuilder,
        >,
        Arc<klights_replication::activation::CommandCodecV3Activation>,
    ) {
        let materializer = self.commit_materializer();
        let activation = Arc::new(
            klights_replication::activation::CommandCodecV3Activation::load(materializer.as_ref())
                .await
                .expect("load command codec activation"),
        );
        let stores = crate::bootstrap::composition_adapters::cluster_store_replication_adapter::raft_state_machine_store_ports_for_test(self.db.clone());
        let snapshot_builder = klights_replication::snapshot::SqliteRaftSnapshotBuilder::new(
            self.db.focused_recovery_store(),
            self.db.focused_read_store(),
            Arc::new(crate::datastore::DatastoreBackendLifecyclePort::new(
                self.db.clone(),
            )),
            snapshot_applied_state,
            supervisor,
        );
        (
            klights_replication::state_machine::SqliteRaftStateMachine::new_with_command_codec_activation(
                stores,
                applied_state,
                snapshot_builder,
                activation.clone(),
            ),
            activation,
        )
    }

    pub fn controlplane_join_handler(
        &self,
        node: Arc<klights_replication::node::RaftNode>,
    ) -> Arc<dyn klights_leader_api::ControlplaneJoinHandler> {
        let db: IntegrationDatastoreHandle = self.db.clone();
        crate::bootstrap::controlplane_join_adapters::build_controlplane_join_handler(node, db)
    }

    pub async fn write_cluster_membership(
        &self,
        membership: &klights_cluster_core::ClusterMembership,
    ) -> anyhow::Result<()> {
        crate::bootstrap::cluster_meta::write_cluster_membership(self.db.as_ref(), membership).await
    }

    pub async fn read_cluster_membership(
        &self,
    ) -> anyhow::Result<klights_cluster_core::ClusterMembership> {
        crate::bootstrap::cluster_meta::read_cluster_membership(self.db.as_ref()).await
    }

    pub fn inject_resource_version(
        data: impl Into<Arc<serde_json::Value>>,
        resource_version: i64,
    ) -> serde_json::Value {
        crate::bootstrap::controller_adapters::controller_runtime_adapter::inject_resource_version(
            data,
            resource_version,
        )
    }
}

/// Resolves one admission webhook target through the root's concrete
/// datastore-to-native-service composition. This exists only for base-owned
/// full-adapter integration tests and does not expose the private adapter.
pub async fn resolve_admission_webhook_target_for_integration(
    db: IntegrationDatastoreHandle,
    client_config: &serde_json::Value,
) -> Result<
    k8s_native_service::admission::WebhookTarget,
    k8s_native_service::admission::AdmissionDependencyError,
> {
    use k8s_native_service::admission::WebhookTargetResolver as _;

    let query: Arc<dyn k8s_native_service::admission::AdmissionQuery> =
        crate::bootstrap::composition_adapters::resource_admission_adapter::DatastoreAdmissionQuery::new(db);
    k8s_native_service::admission::ServiceWebhookTargetResolver::new(query)
        .resolve(client_config)
        .await
}

/// Reads Namespace labels through the root admission query composition.
pub async fn admission_namespace_labels_for_integration(
    db: IntegrationDatastoreHandle,
    namespace: &str,
) -> std::collections::BTreeMap<String, String> {
    let query: Arc<dyn k8s_native_service::admission::AdmissionQuery> =
        crate::bootstrap::composition_adapters::resource_admission_adapter::DatastoreAdmissionQuery::new(db);
    k8s_native_service::admission::selectors::get_namespace_labels(query.as_ref(), namespace).await
}

/// Runs one mutating or validating pass through the exact concrete admission
/// dependencies assembled by root. Kept narrow and feature-only for the
/// base-owned cross-adapter integration suite.
pub async fn run_admission_for_integration(
    db: IntegrationDatastoreHandle,
    context: &k8s_native_service::admission::AdmissionRequestContext,
    is_mutating: bool,
) -> anyhow::Result<serde_json::Value> {
    let identity = DeterministicApiIdentity::default();
    let query: Arc<dyn k8s_native_service::admission::AdmissionQuery> =
        crate::bootstrap::composition_adapters::resource_admission_adapter::DatastoreAdmissionQuery::new(db);
    let target_resolver: Arc<dyn k8s_native_service::admission::WebhookTargetResolver> =
        k8s_native_service::admission::ServiceWebhookTargetResolver::new(Arc::clone(&query));
    let webhook_client: Arc<dyn k8s_native_service::admission::AdmissionWebhookClient> =
        k8s_native_service::admission::ReqwestAdmissionWebhookClient::new();
    k8s_native_service::admission::AdmissionEngine::new(
        &identity,
        query.as_ref(),
        target_resolver.as_ref(),
        webhook_client.as_ref(),
    )
    .run_with_context(context, is_mutating)
    .await
}

#[derive(Clone)]
pub struct IntegrationWatchHistoryFailureControl {
    fail: Arc<std::sync::atomic::AtomicBool>,
}

impl IntegrationWatchHistoryFailureControl {
    pub fn fail_subsequent_reads(&self) {
        self.fail.store(true, std::sync::atomic::Ordering::Release);
    }
}

struct ToggleFailingWatchHistory {
    delegate: Arc<dyn klights_cluster_store::DurableWatchHistoryRead>,
    fail: Arc<std::sync::atomic::AtomicBool>,
}

impl klights_cluster_store::DurableWatchHistoryRead for ToggleFailingWatchHistory {
    fn replay_watch_history(
        &self,
        request: klights_cluster_store::WatchHistoryRequest,
    ) -> klights_cluster_store::WatchHistoryFuture<'_, klights_cluster_store::WatchHistoryRead>
    {
        if self.fail.load(std::sync::atomic::Ordering::Acquire) {
            return Box::pin(async {
                Err(
                    klights_cluster_store::WatchHistoryError::PersistenceFailed {
                        message: "injected live replay read failure".to_string(),
                    },
                )
            });
        }
        self.delegate.replay_watch_history(request)
    }

    fn list_replay_floors(
        &self,
    ) -> klights_cluster_store::WatchHistoryFuture<'_, Vec<klights_cluster_store::DurableReplayFloor>>
    {
        self.delegate.list_replay_floors()
    }
}

struct IntegrationBoundTokenSubjects {
    db: IntegrationDatastoreHandle,
}

impl IntegrationBoundTokenSubjects {
    async fn uid(
        &self,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<String>, klights_leader_api::ClusterIdentityError> {
        self.db
            .get_resource("v1", kind, namespace, name)
            .await
            .map(|resource| resource.map(|resource| resource.uid))
            .map_err(|error| {
                klights_leader_api::ClusterIdentityError::dependency_failure(format!(
                    "credential subject lookup failed: {error}"
                ))
            })
    }
}

impl klights_leader_api::LeaderBoundTokenSubjectLookup for IntegrationBoundTokenSubjects {
    fn service_account_uid<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
    ) -> klights_leader_api::ClusterIdentityFuture<'a, Option<String>> {
        Box::pin(async move { self.uid("ServiceAccount", Some(namespace), name).await })
    }

    fn pod_uid<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
    ) -> klights_leader_api::ClusterIdentityFuture<'a, Option<String>> {
        Box::pin(async move { self.uid("Pod", Some(namespace), name).await })
    }

    fn node_uid<'a>(
        &'a self,
        name: &'a str,
    ) -> klights_leader_api::ClusterIdentityFuture<'a, Option<String>> {
        Box::pin(async move { self.uid("Node", None, name).await })
    }

    fn secret_uid<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
    ) -> klights_leader_api::ClusterIdentityFuture<'a, Option<String>> {
        Box::pin(async move { self.uid("Secret", Some(namespace), name).await })
    }
}

pub async fn validate_sa_token_bindings_for_integration(
    db: IntegrationDatastoreHandle,
    claims: &klights_auth::SaTokenClaims,
) -> Result<(), k8s_native_service::AppError> {
    klights_auth::authentication::validate_sa_token_bindings(
        &IntegrationBoundTokenSubjects { db },
        claims,
    )
    .await
    .map_err(k8s_native_service::AppError::from)
}

/// Seeds the production bootstrap-token Secret shape used by full-stack API
/// authentication cases and returns its bearer token.
pub async fn create_worker_bootstrap_token_for_integration(
    db: &IntegrationDatastoreHandle,
) -> anyhow::Result<String> {
    let token = crate::bootstrap::bootstrap_token::generate_bootstrap_token();
    crate::bootstrap::bootstrap_token::create_scoped_bootstrap_token_secret_for_test(
        db.as_ref(),
        crate::bootstrap::bootstrap_token::BootstrapTokenScope::Worker,
        &token,
    )
    .await?;
    Ok(token)
}

/// Seeds a fixed worker bootstrap Secret with a caller-selected lifetime so
/// full-stack GET tests can exercise the production rotation boundary.
pub async fn create_worker_bootstrap_token_with_ttl_for_integration(
    db: &IntegrationDatastoreHandle,
    token: &str,
    ttl: std::time::Duration,
) -> anyhow::Result<()> {
    crate::bootstrap::bootstrap_token::create_scoped_bootstrap_token_secret_with_ttl_for_test(
        db.as_ref(),
        crate::bootstrap::bootstrap_token::BootstrapTokenScope::Worker,
        token,
        ttl,
    )
    .await
}

/// Seeds a fixed control-plane bootstrap Secret with a caller-selected
/// lifetime for cross-Secret rotation isolation tests.
pub async fn create_controlplane_bootstrap_token_with_ttl_for_integration(
    db: &IntegrationDatastoreHandle,
    token: &str,
    ttl: std::time::Duration,
) -> anyhow::Result<()> {
    crate::bootstrap::bootstrap_token::create_scoped_bootstrap_token_secret_with_ttl_for_test(
        db.as_ref(),
        crate::bootstrap::bootstrap_token::BootstrapTokenScope::Controlplane,
        token,
        ttl,
    )
    .await
}

pub fn broadcast_watch_event_for_integration(
    db: &IntegrationDatastoreHandle,
    object: serde_json::Value,
) {
    let event = klights_watch::WatchEvent::added(object);
    let pending = crate::datastore::staged_post_commit_from_event(event);
    db.commit_observation_sink().observe(&[pending]);
}

pub async fn reconcile_namespace_termination_for_integration(
    db: IntegrationDatastoreHandle,
    namespace: &str,
) -> Result<(), k8s_native_service::AppError> {
    let store = crate::bootstrap::composition_adapters::api_state_adapter::RootNamespaceTerminationStore::new(db);
    let metrics = klights_controllers::side_effects::SideEffectMetrics::new();
    k8s_native_service::reconcile_namespace_termination_at(
        store.as_ref(),
        namespace,
        metrics.as_ref(),
        chrono::Utc::now(),
    )
    .await
}

pub async fn reconcile_namespace_termination_for_uid_for_integration(
    db: IntegrationDatastoreHandle,
    namespace: &str,
    expected_uid: &str,
) -> Result<k8s_native_service::NamespaceTerminationOutcome, k8s_native_service::AppError> {
    let store = crate::bootstrap::composition_adapters::api_state_adapter::RootNamespaceTerminationStore::new(db);
    let metrics = klights_controllers::side_effects::SideEffectMetrics::new();
    k8s_native_service::reconcile_namespace_termination_for_uid_with_outcome_at(
        store.as_ref(),
        namespace,
        expected_uid,
        metrics.as_ref(),
        chrono::Utc::now(),
    )
    .await
}

pub async fn mark_foreground_deletion_for_integration(
    db: IntegrationDatastoreHandle,
    target: k8s_native_service::generic_command::ResourceDeleteTarget<'_>,
    initial_resource: klights_cluster_core::Resource,
    delete_preconditions: klights_cluster_core::ResourcePreconditions,
) -> Result<klights_cluster_core::Resource, k8s_native_service::AppError> {
    let lifecycle =
        crate::bootstrap::finalizer_lifecycle_adapter::BorrowedFinalizerLifecycleStore::new(
            db.as_ref(),
        );
    k8s_native_service::generic_command::mark_foreground_deletion_with_retry(
        &lifecycle,
        target.api_version,
        target.kind,
        target.namespace,
        target.name,
        initial_resource,
        delete_preconditions,
        chrono::Utc::now(),
    )
    .await
}

pub async fn complete_non_foreground_delete_for_integration(
    db: IntegrationDatastoreHandle,
    request: k8s_native_service::generic_command::NonForegroundDeleteRequest<'_>,
) -> Result<k8s_native_service::generic_command::DeleteCompletion, k8s_native_service::AppError> {
    let lifecycle =
        crate::bootstrap::finalizer_lifecycle_adapter::BorrowedFinalizerLifecycleStore::new(
            db.as_ref(),
        );
    k8s_native_service::generic_command::complete_non_foreground_delete_with_live_recheck(
        &lifecycle, request,
    )
    .await
}

pub async fn delete_collection_listed_resource_for_integration(
    db: IntegrationDatastoreHandle,
    api_version: &'static str,
    kind: &'static str,
    namespace: Option<&str>,
    resource: klights_cluster_core::Resource,
) -> Result<bool, k8s_native_service::AppError> {
    let leader_rx = crate::control_plane::client::local::always_leader_watch();
    let resource_query = crate::bootstrap::outbox_apply_adapter::BackendResourceQueryFixture::new(
        db.clone(),
        leader_rx,
    );
    let lifecycle =
        crate::bootstrap::finalizer_lifecycle_adapter::BorrowedFinalizerLifecycleStore::new(
            db.as_ref(),
        );
    let strategy = k8s_native_service::generic_command::FinalizerAwareDeleteStrategy {
        resource_query: &resource_query,
        lifecycle: &lifecycle,
        operation_now: chrono::DateTime::from_timestamp(1_700_000_000, 0)
            .expect("fixed collection-delete integration timestamp"),
    };
    let target = klights_types::ResourceKey::new(
        api_version,
        kind,
        namespace.map(str::to_string),
        resource.name.clone(),
    );
    let intent = k8s_native_service::generic_command::DeleteIntent::collection_item(
        k8s_native_service::generic_command::DryRunMode::Live,
        klights_cluster_core::ResourcePreconditions::uid(resource.uid.clone()),
    );
    Ok(matches!(
        k8s_native_service::generic_command::delete_loaded_with_strategy(
            &strategy, target, resource, &intent,
        )
        .await?,
        k8s_native_service::generic_command::DeleteResult::HardDeleted(_)
    ))
}

fn deterministic_uuid_v4(value: u64) -> String {
    let first = ((value & 0x000f_ffff) << 12) | ((value >> 20) & 0x0fff);
    let second = (value >> 32) & 0xffff;
    let third = 0x4000 | ((value >> 48) & 0x0fff);
    let fourth = 0x8000 | ((value >> 60) & 0x000f);
    format!("{first:08x}-{second:04x}-{third:04x}-{fourth:04x}-000000000000")
}

#[derive(Default)]
struct DeterministicApiIdentity {
    next: std::sync::atomic::AtomicU64,
}

impl k8s_native_service::ApiIdentityGenerator for DeterministicApiIdentity {
    fn generate_name(&self, prefix: &str) -> String {
        let value = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("{prefix}{value:05}")
    }

    fn new_uid(&self) -> String {
        let value = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        deterministic_uuid_v4(value)
    }
}

struct AllowAllAuthorizer;

#[derive(Default)]
struct DeterministicControllerIdentity {
    next: std::sync::atomic::AtomicU64,
}

impl klights_controllers::ControllerIdentityGenerator for DeterministicControllerIdentity {
    fn generate_name(&self, prefix: &str) -> String {
        let value = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("{prefix}{value:05}")
    }

    fn new_uid(&self) -> String {
        let value = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        deterministic_uuid_v4(value)
    }
}

struct UnavailableNodeMetrics;

struct IntegrationNodeMetrics {
    inner: std::sync::RwLock<Arc<dyn klights_node_api::NodeMetrics>>,
}

impl IntegrationNodeMetrics {
    fn new() -> Self {
        Self {
            inner: std::sync::RwLock::new(Arc::new(UnavailableNodeMetrics)),
        }
    }

    fn set(&self, metrics: Arc<dyn klights_node_api::NodeMetrics>) {
        *self.inner.write().expect("integration node metrics lock") = metrics;
    }
}

impl klights_node_api::NodeMetrics for IntegrationNodeMetrics {
    fn collect_metrics(
        &self,
        request: klights_node_api::NodeMetricsRequest,
    ) -> klights_node_api::NodeMetricsFuture<'_, klights_node_api::NodeMetricsResult> {
        let metrics = self
            .inner
            .read()
            .expect("integration node metrics lock")
            .clone();
        Box::pin(async move { metrics.collect_metrics(request).await })
    }
}

#[derive(Clone, Default)]
pub struct IntegrationServiceRoutingObservation {
    sync_count: Arc<std::sync::atomic::AtomicUsize>,
    sync_now_count: Arc<std::sync::atomic::AtomicUsize>,
}

impl IntegrationServiceRoutingObservation {
    pub fn sync_count(&self) -> usize {
        self.sync_count.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn sync_now_count(&self) -> usize {
        self.sync_now_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl klights_reconcile_api::ServiceRoutingSync for IntegrationServiceRoutingObservation {
    fn request_service_routing_sync(
        &self,
    ) -> Result<(), klights_reconcile_api::ReconcileSinkError> {
        self.sync_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}

impl klights_node_api::NodeMetrics for UnavailableNodeMetrics {
    fn collect_metrics(
        &self,
        _request: klights_node_api::NodeMetricsRequest,
    ) -> klights_node_api::NodeMetricsFuture<'_, klights_node_api::NodeMetricsResult> {
        Box::pin(async {
            Err(klights_node_api::NodeMetricsError::unavailable(
                "node metrics are not configured for the integration harness",
            ))
        })
    }
}

#[async_trait::async_trait]
impl klights_auth::authorizer::Authorizer for AllowAllAuthorizer {
    async fn authorize(
        &self,
        _identity: &klights_auth::AuthenticatedIdentity,
        _request: &klights_auth::request_attributes::AuthorizationRequest,
    ) -> klights_auth::authorizer::AuthorizationDecision {
        klights_auth::authorizer::AuthorizationDecision::allow("integration harness allow-all")
    }
}

/// Narrow integration handle around one real registered replication follower.
pub struct IntegrationFollowerSession {
    replication: Arc<klights_replication::ReplicationService>,
    control_rx: tokio::sync::mpsc::Receiver<klights_node_api::FollowerControlMessage>,
    node_name: String,
    session_id: u64,
}

impl IntegrationFollowerSession {
    pub async fn recv(&mut self) -> Option<klights_node_api::FollowerControlMessage> {
        self.control_rx.recv().await
    }

    pub async fn complete_node_log_event(
        &self,
        event: klights_node_api::RoutedNodeLogEvent,
    ) -> anyhow::Result<()> {
        self.replication
            .complete_node_log_event(
                klights_node_api::FollowerCompletionContext::new(
                    &self.node_name,
                    self.session_id,
                    klights_node_api::NodeOperationKind::Log,
                ),
                event,
            )
            .await
    }

    pub async fn complete_node_exec_sync(
        &self,
        response: klights_node_api::RoutedNodeExecSyncResponse,
    ) -> anyhow::Result<()> {
        self.replication
            .complete_node_exec_sync(
                klights_node_api::FollowerCompletionContext::new(
                    &self.node_name,
                    self.session_id,
                    klights_node_api::NodeOperationKind::ExecSync,
                ),
                response,
            )
            .await
    }
}

/// Feature-only executor for the native exec-sync WebSocket adapter backed by
/// the same real replication service registered through the parent harness.
#[derive(Clone)]
pub struct IntegrationRemoteExecSync {
    node_exec: Arc<dyn klights_node_api::NodeExec>,
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
}

impl IntegrationRemoteExecSync {
    pub async fn run<S>(
        self,
        io: S,
        target: k8s_native_service::streaming::ExecTarget,
        subprotocol: String,
        node_name: String,
    ) where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let socket = tokio_tungstenite::WebSocketStream::from_raw_socket(
            io,
            tokio_tungstenite::tungstenite::protocol::Role::Server,
            None,
        )
        .await;
        k8s_native_service::streaming::handle_remote_exec_websocket_sync(
            socket,
            k8s_native_service::streaming::RemoteExecWebSocketSyncRequest {
                node_exec: self.node_exec,
                target,
                subprotocol,
                node_name,
                task_supervisor: self.task_supervisor,
            },
        )
        .await;
    }
}

#[derive(Clone)]
pub struct IntegrationCsrSignerObservation {
    request_count: Arc<std::sync::atomic::AtomicUsize>,
    changed: Arc<tokio::sync::Notify>,
}

impl IntegrationCsrSignerObservation {
    pub fn request_count(&self) -> usize {
        self.request_count
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub async fn wait_for_request(&self) {
        loop {
            let changed = self.changed.notified();
            if self.request_count() > 0 {
                return;
            }
            changed.await;
        }
    }
}

struct IntegrationRecordingCsrSigner {
    request_count: Arc<std::sync::atomic::AtomicUsize>,
    changed: Arc<tokio::sync::Notify>,
}

/// In-process leader delivery for the native API fixture. This keeps the real
/// node outbox codec, authorization, watermark, proposal, and committed-apply
/// path while deriving the authenticated worker identity from the lifecycle-
/// authored command under test.
struct IntegrationOutboxDelivery {
    embedded: Arc<klights_replication::leader_api::EmbeddedOutboxDelivery>,
    codec: Arc<dyn klights_leader_api::OutboxPayloadCodec>,
}

impl klights_leader_api::LeaderOutboxDelivery for IntegrationOutboxDelivery {
    fn deliver_outbox(
        &self,
        request: klights_leader_api::OutboxDeliveryRequest,
    ) -> klights_leader_api::OutboxDeliveryFuture<'_> {
        Box::pin(async move {
            let (
                codec_version,
                idempotency_key,
                operation,
                payload,
                client_id,
                stream_id,
                stream_sequence,
            ) = request.into_parts();
            if !klights_cluster_core::supports_command_codec_version(codec_version) {
                return Err(klights_leader_api::OutboxDeliveryError::codec_incompatible(
                    codec_version,
                    klights_cluster_core::COMMAND_CODEC_VERSION,
                ));
            }
            let decoded_command = self.codec.decode(payload.as_ref()).map_err(|error| {
                klights_leader_api::OutboxDeliveryError::invalid(
                    "delivery.payload",
                    error.to_string(),
                )
            });
            let authenticated_node = decoded_command
                .as_ref()
                .ok()
                .and_then(|command| match command {
                    klights_cluster_core::StorageCommand::FinalizeBoundPod {
                        node_name, ..
                    }
                    | klights_cluster_core::StorageCommand::UpdateNodeDataplane {
                        node_name, ..
                    } => Some(node_name.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| "test-node".to_string());
            let effect = self
                .embedded
                .deliver_authenticated_outbox_command_effect(
                    authenticated_node,
                    idempotency_key,
                    operation,
                    decoded_command,
                    Some(klights_cluster_core::OutboxStreamWatermark {
                        client_id,
                        stream_id,
                        stream_seq: stream_sequence,
                    }),
                )
                .await?;
            let (result, _, _, _) = effect.into_parts();
            Ok(result.into())
        })
    }
}

impl klights_auth::csr_signer::CsrSigner for IntegrationRecordingCsrSigner {
    fn sign(
        &self,
        _request: klights_auth::csr_signer::SignRequest,
    ) -> Result<klights_auth::csr_signer::SignResult, klights_auth::CredentialOperationError> {
        self.request_count
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        self.changed.notify_waiters();
        Ok(klights_auth::csr_signer::SignResult {
            certificate_pem: "-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----\n"
                .to_string(),
        })
    }
}

pub struct IntegrationHeldSupervisorTask {
    handle: klights_supervisor::SupervisedJoinHandle<()>,
}

impl IntegrationHeldSupervisorTask {
    pub fn abort(&self) {
        self.handle.abort();
    }
}

#[derive(Default)]
struct IntegrationHarnessOptions {
    csr_signer: Option<Arc<dyn klights_auth::csr_signer::CsrSigner>>,
    task_categories: klights_supervisor::TaskCategoryConfig,
    auth_clock: Option<Arc<dyn klights_auth::clock::Clock>>,
    authority: Option<Arc<dyn klights_leader_api::LeaderAuthority>>,
    bootstrap_token_authenticator:
        Option<Arc<dyn klights_leader_api::LeaderBootstrapTokenAuthentication>>,
}

/// Opaque full-stack API fixture owned by the base integration-test package.
#[derive(Clone)]
pub struct NativeApiTestHarness {
    router: axum::Router,
    datastore: IntegrationDatastoreHandle,
    sqlite: crate::datastore::sqlite::Datastore,
    nodeport_alloc: Arc<klights_controllers::service::NodePortAllocator>,
    pod_repository: Arc<crate::kubelet::pod_repository::PodRepository>,
    _node_local: Arc<crate::datastore::node_local::NodeLocalStores>,
    outbox_dispatcher: Arc<klights_kubelet::node_outbox::OutboxDispatcher>,
    controller_dispatcher: Arc<klights_controllers::ControllerDispatcher>,
    crd_registry: klights_controllers::crd::CrdRegistry,
    service_routing: Arc<dyn klights_reconcile_api::ServiceRoutingSync>,
    node_metrics: Arc<IntegrationNodeMetrics>,
    operational_replication: Option<Arc<klights_replication::ReplicationService>>,
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    node_lease_tracker: Arc<klights_controllers::node_lease::NodeLeaseTracker>,
    authority: Option<Arc<dyn klights_leader_api::LeaderAuthority>>,
    node_name: String,
}

impl NativeApiTestHarness {
    pub async fn new() -> anyhow::Result<Self> {
        Self::with_authorizer(Arc::new(AllowAllAuthorizer)).await
    }

    pub async fn with_authorizer(
        authorizer: Arc<dyn klights_auth::authorizer::Authorizer>,
    ) -> anyhow::Result<Self> {
        Self::assemble(
            authorizer,
            None,
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
        )
        .await
    }

    pub async fn with_authorizer_and_operational_endpoints(
        authorizer: Arc<dyn klights_auth::authorizer::Authorizer>,
    ) -> anyhow::Result<Self> {
        Self::assemble(
            authorizer,
            None,
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            true,
        )
        .await
    }

    pub async fn with_authorizer_and_audit_sink(
        authorizer: Arc<dyn klights_auth::authorizer::Authorizer>,
        audit_sink: Arc<dyn k8s_native_service::audit::AuditSink>,
    ) -> anyhow::Result<Self> {
        Self::assemble(
            authorizer,
            None,
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            None,
            None,
            None,
            None,
            None,
            Some(audit_sink),
            None,
            false,
        )
        .await
    }

    pub async fn with_pod_lifecycle_diagnostics(
        diagnostics: Arc<dyn klights_pod_api::PodLifecycleDiagnosticsQuery>,
    ) -> anyhow::Result<Self> {
        Self::assemble(
            Arc::new(AllowAllAuthorizer),
            Some(diagnostics),
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
        )
        .await
    }

    pub async fn with_authentication_dependencies(
        signing_keys: Arc<dyn klights_leader_api::LeaderServiceAccountSigningKeyState>,
        oidc: Option<Arc<dyn klights_auth::oidc::OidcValidator>>,
        webhook: Option<Arc<dyn klights_auth::webhook_auth::WebhookAuthenticator>>,
    ) -> anyhow::Result<Self> {
        Self::assemble(
            Arc::new(AllowAllAuthorizer),
            None,
            signing_keys,
            oidc,
            webhook,
            None,
            None,
            None,
            None,
            None,
            false,
        )
        .await
    }

    pub async fn with_authenticators(
        oidc: Option<Arc<dyn klights_auth::oidc::OidcValidator>>,
        webhook: Option<Arc<dyn klights_auth::webhook_auth::WebhookAuthenticator>>,
    ) -> anyhow::Result<Self> {
        Self::with_authentication_dependencies(
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            oidc,
            webhook,
        )
        .await
    }

    pub async fn with_bootstrap_token_authenticator(
        bootstrap_token_authenticator: Arc<
            dyn klights_leader_api::LeaderBootstrapTokenAuthentication,
        >,
    ) -> anyhow::Result<Self> {
        Self::assemble_with_options(
            Arc::new(AllowAllAuthorizer),
            None,
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            IntegrationHarnessOptions {
                bootstrap_token_authenticator: Some(bootstrap_token_authenticator),
                ..Default::default()
            },
        )
        .await
    }

    pub async fn with_signing_key_pem(signing_key_pem: String) -> anyhow::Result<Self> {
        Self::with_authentication_dependencies(
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::from_pem(
                signing_key_pem,
            ),
            None,
            None,
        )
        .await
    }

    pub async fn with_auth_clock(
        clock: Arc<dyn klights_auth::clock::Clock>,
    ) -> anyhow::Result<Self> {
        Self::assemble_with_options(
            Arc::new(AllowAllAuthorizer),
            None,
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            IntegrationHarnessOptions {
                auth_clock: Some(clock),
                ..Default::default()
            },
        )
        .await
    }

    pub async fn with_leader_authority() -> anyhow::Result<Self> {
        let (authority, _publisher) =
            klights_replication::authority::WatchLeaderAuthority::channel(true, None);
        Self::assemble_with_options(
            Arc::new(AllowAllAuthorizer),
            None,
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            IntegrationHarnessOptions {
                authority: Some(authority),
                ..Default::default()
            },
        )
        .await
    }

    pub async fn with_toggle_failing_watch_history()
    -> anyhow::Result<(Self, IntegrationWatchHistoryFailureControl)> {
        let fail = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let harness = Self::assemble(
            Arc::new(AllowAllAuthorizer),
            None,
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            None,
            None,
            Some(fail.clone()),
            None,
            None,
            None,
            None,
            false,
        )
        .await?;
        Ok((harness, IntegrationWatchHistoryFailureControl { fail }))
    }

    pub async fn with_mutation_side_effect_factory<F>(factory: F) -> anyhow::Result<Self>
    where
        F: FnOnce(
                IntegrationDatastoreHandle,
            ) -> Arc<klights_controllers::side_effects::SideEffectRegistry>
            + Send
            + 'static,
    {
        Self::assemble(
            Arc::new(AllowAllAuthorizer),
            None,
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            None,
            None,
            None,
            Some(Box::new(factory)),
            None,
            None,
            None,
            false,
        )
        .await
    }

    pub async fn with_service_routing_observation()
    -> anyhow::Result<(Self, IntegrationServiceRoutingObservation)> {
        let observation = IntegrationServiceRoutingObservation::default();
        let harness = Self::assemble(
            Arc::new(AllowAllAuthorizer),
            None,
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            None,
            None,
            None,
            None,
            Some(observation.clone()),
            None,
            None,
            false,
        )
        .await?;
        Ok((harness, observation))
    }

    pub async fn with_priority_fairness() -> anyhow::Result<(
        Self,
        Arc<k8s_native_service::priority_fairness::ApiPriorityFairness>,
    )> {
        let priority_fairness =
            Arc::new(k8s_native_service::priority_fairness::ApiPriorityFairness::new());
        let harness = Self::assemble(
            Arc::new(AllowAllAuthorizer),
            None,
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            None,
            None,
            None,
            None,
            None,
            None,
            Some(priority_fairness.clone()),
            false,
        )
        .await?;
        Ok((harness, priority_fairness))
    }

    pub async fn with_csr_signer_observation()
    -> anyhow::Result<(Self, IntegrationCsrSignerObservation)> {
        let request_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let changed = Arc::new(tokio::sync::Notify::new());
        let observation = IntegrationCsrSignerObservation {
            request_count: request_count.clone(),
            changed: changed.clone(),
        };
        let harness = Self::assemble_with_options(
            Arc::new(AllowAllAuthorizer),
            None,
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            IntegrationHarnessOptions {
                csr_signer: Some(Arc::new(IntegrationRecordingCsrSigner {
                    request_count,
                    changed,
                })),
                ..Default::default()
            },
        )
        .await?;
        Ok((harness, observation))
    }

    pub async fn with_held_pod_delete_workqueue()
    -> anyhow::Result<(Self, IntegrationHeldSupervisorTask)> {
        let harness = Self::assemble_with_options(
            Arc::new(AllowAllAuthorizer),
            None,
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            IntegrationHarnessOptions {
                task_categories: klights_supervisor::TaskCategoryConfig {
                    pod_delete_workqueue: 1,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await?;
        let handle = harness
            .task_supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::PodDeleteWorkqueue,
                "hold_foreground_delete_workqueue_for_integration",
                std::future::pending(),
            )
            .await?;
        Ok((harness, IntegrationHeldSupervisorTask { handle }))
    }

    async fn assemble(
        authorizer: Arc<dyn klights_auth::authorizer::Authorizer>,
        pod_lifecycle_diagnostics: Option<Arc<dyn klights_pod_api::PodLifecycleDiagnosticsQuery>>,
        signing_keys: Arc<dyn klights_leader_api::LeaderServiceAccountSigningKeyState>,
        oidc: Option<Arc<dyn klights_auth::oidc::OidcValidator>>,
        webhook: Option<Arc<dyn klights_auth::webhook_auth::WebhookAuthenticator>>,
        watch_history_failure: Option<Arc<std::sync::atomic::AtomicBool>>,
        mutation_side_effects_factory: Option<
            Box<
                dyn FnOnce(
                        IntegrationDatastoreHandle,
                    )
                        -> Arc<klights_controllers::side_effects::SideEffectRegistry>
                    + Send,
            >,
        >,
        service_routing_observation: Option<IntegrationServiceRoutingObservation>,
        audit_sink: Option<Arc<dyn k8s_native_service::audit::AuditSink>>,
        priority_fairness: Option<Arc<k8s_native_service::priority_fairness::ApiPriorityFairness>>,
        mount_operational_endpoints: bool,
    ) -> anyhow::Result<Self> {
        Self::assemble_with_options(
            authorizer,
            pod_lifecycle_diagnostics,
            signing_keys,
            oidc,
            webhook,
            watch_history_failure,
            mutation_side_effects_factory,
            service_routing_observation,
            audit_sink,
            priority_fairness,
            mount_operational_endpoints,
            IntegrationHarnessOptions::default(),
        )
        .await
    }

    async fn assemble_with_options(
        authorizer: Arc<dyn klights_auth::authorizer::Authorizer>,
        pod_lifecycle_diagnostics: Option<Arc<dyn klights_pod_api::PodLifecycleDiagnosticsQuery>>,
        signing_keys: Arc<dyn klights_leader_api::LeaderServiceAccountSigningKeyState>,
        oidc: Option<Arc<dyn klights_auth::oidc::OidcValidator>>,
        webhook: Option<Arc<dyn klights_auth::webhook_auth::WebhookAuthenticator>>,
        watch_history_failure: Option<Arc<std::sync::atomic::AtomicBool>>,
        mutation_side_effects_factory: Option<
            Box<
                dyn FnOnce(
                        IntegrationDatastoreHandle,
                    )
                        -> Arc<klights_controllers::side_effects::SideEffectRegistry>
                    + Send,
            >,
        >,
        service_routing_observation: Option<IntegrationServiceRoutingObservation>,
        audit_sink: Option<Arc<dyn k8s_native_service::audit::AuditSink>>,
        priority_fairness: Option<Arc<k8s_native_service::priority_fairness::ApiPriorityFairness>>,
        mount_operational_endpoints: bool,
        options: IntegrationHarnessOptions,
    ) -> anyhow::Result<Self> {
        let IntegrationHarnessOptions {
            csr_signer,
            task_categories,
            auth_clock,
            authority,
            bootstrap_token_authenticator,
        } = options;
        let auth_clock = auth_clock.unwrap_or_else(|| Arc::new(klights_auth::clock::SystemClock));
        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();
        let passive_reads = if let Some(fail) = watch_history_failure {
            let focused_reads = db.focused_read_store();
            crate::datastore::selector::PassiveReadPorts::new(
                focused_reads.clone(),
                Arc::new(ToggleFailingWatchHistory {
                    delegate: focused_reads.clone(),
                    fail,
                }),
                focused_reads,
            )
        } else {
            crate::datastore::selector::sqlite_passive_read_ports(&db)
        };
        let datastore: IntegrationDatastoreHandle = Arc::new(db.clone());
        let config = crate::KlightsConfig::test_default();
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(task_categories));
        let identity: Arc<dyn k8s_native_service::ApiIdentityGenerator> =
            Arc::new(DeterministicApiIdentity::default());
        let controller_identity: Arc<dyn klights_controllers::ControllerIdentityGenerator> =
            Arc::new(DeterministicControllerIdentity::default());
        let service_ipam = Arc::new(klights_controllers::service::ServiceIpam::new(
            &config.service_cidr,
        ));
        let nodeport_alloc = Arc::new(klights_controllers::service::NodePortAllocator::new());
        nodeport_alloc.set_ready();
        let metrics = klights_controllers::side_effects::SideEffectMetrics::new();
        let leader_rx = crate::control_plane::client::local::always_leader_watch();
        let resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery> = Arc::new(
            crate::bootstrap::outbox_apply_adapter::BackendResourceQueryFixture::new(
                datastore.clone(),
                leader_rx.clone(),
            ),
        );
        let proposal: Arc<dyn klights_replication::proposal::RaftProposal> = Arc::new(
            crate::bootstrap::outbox_apply_adapter::BackendProposalFixture::new(datastore.clone()),
        );
        let resource_command: Arc<dyn klights_leader_api::LeaderResourceCommand> = Arc::new(
            klights_replication::leader_api::EmbeddedLeaderResourceCommand::new(
                proposal.clone(),
                resource_query.clone(),
                leader_rx.clone(),
            ),
        );
        let node_local = Arc::new(
            crate::datastore::node_local::selector::open_node_local(
                crate::datastore::backend_kind::BackendKind::Sqlite,
                None,
                supervisor.clone(),
                None,
                "sqlite:native-api-integration-node-local",
            )
            .await?,
        );
        let outbox_notify = Arc::new(tokio::sync::Notify::new());
        let outbox_stores = klights_kubelet::node_outbox::OutboxStores::new(
            node_local.outbox_producer(),
            node_local.outbox_dispatcher(),
            node_local.pod_status_checkpoints(),
            node_local.runtime_observation_checkpoints(),
            node_local.outbox_status_stamps(),
        );
        let outbox_codec =
            crate::bootstrap::composition_adapters::outbox_payload_codec_adapter::new_codec();
        let outbox = Arc::new(klights_kubelet::node_outbox::Outbox::compose(
            outbox_stores.clone(),
            outbox_codec.clone(),
            outbox_notify.clone(),
            Arc::new(klights_supervisor::SystemWallClock),
        ));
        let outbox_delivery: Arc<dyn klights_leader_api::LeaderOutboxDelivery> =
            Arc::new(IntegrationOutboxDelivery {
                embedded: Arc::new(
                    klights_replication::leader_api::EmbeddedOutboxDelivery::new(
                        proposal,
                        resource_query.clone(),
                        leader_rx,
                    ),
                ),
                codec: outbox_codec.clone(),
            });
        let outbox_dispatcher = Arc::new(klights_kubelet::node_outbox::OutboxDispatcher::new(
            outbox_stores,
            outbox_codec,
            outbox_delivery,
            outbox_notify,
            Arc::new(klights_supervisor::SystemWallClock),
        ));
        let side_effects = Arc::new(crate::bootstrap::side_effects::default_registry(
            metrics.clone(),
            None,
            Some(supervisor.clone()),
            Some(datastore.clone()),
            controller_identity.clone(),
        ));
        let gc_coordination = Arc::new(klights_controllers::ControllerCoordination::new());
        let pod_repository_config = crate::pod_repository_composition::PodRepositoryBuildConfig {
            db: datastore.clone(),
            pod_workqueue_store: Some(node_local.pod_workqueue()),
            supervisor: supervisor.clone(),
            side_effects: side_effects.clone(),
            metrics: metrics.clone(),
            pod_network_cache: node_local.pod_network_cache(),
            assignment_waiter: Arc::new(klights_networking::PodNetworkAssignmentBus::new()),
            scheduling_mode: crate::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
            outbox: Some(outbox),
            cluster_api: Some(resource_query.clone()),
            controller_identity: controller_identity.clone(),
            #[cfg(not(test))]
            api_identity: identity.clone(),
            #[cfg(test)]
            scheduler_bind_gate: None,
            #[cfg(not(test))]
            gc_coordination: gc_coordination.clone(),
        };
        #[cfg(not(test))]
        let root_pod_parts = crate::pod_repository_composition::build_pod_repository_parts(
            pod_repository_config,
            None,
        );
        #[cfg(test)]
        let root_pod_parts =
            crate::pod_repository_composition::build_pod_repository_parts_with_test_support(
                pod_repository_config,
                None,
                identity.clone(),
                gc_coordination.clone(),
            );
        let pod_api = root_pod_parts.api;
        let pod_subresource = root_pod_parts.subresource;
        let pod_repository = Arc::new(root_pod_parts.repository_parts.repository);
        let api_pod_repository =
            crate::bootstrap::composition_adapters::api_state_adapter::RootApiPodRepository::new(
                pod_repository.clone(),
                pod_api.clone(),
                pod_subresource.clone(),
            );
        let controller_pod_port = Arc::new(
            crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerPodPort::new(
                pod_repository.clone(),
                pod_api,
                pod_subresource,
            ),
        );
        side_effects.set_pod_ports(pod_repository.clone(), pod_repository.clone());
        let finalizer_lifecycle = crate::bootstrap::finalizer_lifecycle_adapter::
            DatastoreFinalizerLifecycleAdapter::new_with_coordination(
                datastore.clone(),
                pod_repository.clone(),
                side_effects.clone(),
                metrics.clone(),
                gc_coordination.clone(),
            );
        let mutation_side_effects = mutation_side_effects_factory
            .map(|factory| factory(datastore.clone()))
            .unwrap_or_else(|| side_effects.clone());
        let mutation_effects = klights_controllers::side_effects::ResourceMutationEffects::new(
            mutation_side_effects,
            metrics.clone(),
        );
        let positioned_watch =
            crate::bootstrap::composition_adapters::positioned_watch_adapter::for_test(
                &passive_reads,
                datastore.clone(),
            );
        let watch_signals = crate::bootstrap::watch_commit_wiring::test_signal_source(&datastore);
        let generated = crate::bootstrap::composition_adapters::generated_handler_adapter::GeneratedHandlerAdapter::new(
            datastore.clone(),
            watch_signals.clone(),
            positioned_watch.clone(),
            klights_supervisor::FileProcessExecutor::new(supervisor.clone()),
            supervisor.clone(),
            config.data_root.join("etc/ca.crt"),
            controller_identity.clone(),
        );
        let network = klights_networking::test_support::mock_network();
        let controller_leader_ports = Arc::new(
            crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort::new(datastore.clone()),
        );
        let non_pod_finalization = Arc::new(
            crate::bootstrap::controller_adapters::gc_delete_adapter::GcNonPodFinalizationAdapter::new(datastore.clone()),
        );
        let controller_dependencies = klights_controllers::ControllerRuntimeDependencies {
            wall_time: chrono::Utc::now,
            resource_query: controller_leader_ports.clone(),
            deployment_store: controller_leader_ports.clone(),
            replicaset_store: controller_leader_ports.clone(),
            statefulset_store: controller_leader_ports.clone(),
            daemonset_store: controller_leader_ports.clone(),
            job_store: controller_leader_ports.clone(),
            service_store: controller_leader_ports.clone(),
            pvc_store: controller_leader_ports.clone(),
            pdb_store: controller_leader_ports.clone(),
            replicationcontroller_store: controller_leader_ports.clone(),
            apiservice_store: controller_leader_ports.clone(),
            csr_status_store: controller_leader_ports,
            pod_query: api_pod_repository.clone(),
            pdb_pod_reader: pod_repository.clone(),
            deployment_pod_reader: pod_repository.clone(),
            deployment_pod_mutation: controller_pod_port.clone(),
            replicaset_pod_mutation: controller_pod_port.clone(),
            statefulset_pod_mutation: controller_pod_port.clone(),
            daemonset_pod_mutation: controller_pod_port.clone(),
            job_pod_mutation: controller_pod_port.clone(),
            replicationcontroller_pod_mutation: controller_pod_port,
            pod_delete_sink: pod_repository.clone(),
            reconcile: Arc::new(
                crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerReconcilePort::new(
                    non_pod_finalization.clone(),
                ),
            ),
            network: Arc::new(
                crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerNetworkPort::new(
                    network.services().clone(),
                ),
            ),
            effects: Arc::new(
                crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerEffectPort::new(
                    klights_supervisor::FileProcessExecutor::new(supervisor.clone()),
                    config.data_root.join("local-path-provisioner"),
                ),
            ),
            coordination: gc_coordination.clone(),
            node_name: Arc::from(config.node_name.as_str()),
        };
        let node_metrics = Arc::new(IntegrationNodeMetrics::new());
        let csr_issuer = csr_signer.map(|signer| {
            Arc::new(crate::bootstrap::auth_adapters::AuthCsrIssuer::new(
                signer,
                auth_clock.clone(),
                supervisor.clone(),
            )) as Arc<dyn klights_controllers::csr_signer::CsrIssuer>
        });
        let hpa_controller =
            crate::bootstrap::controller_adapters::hpa_controller_adapter::controller(
                datastore.clone(),
                pod_repository.clone(),
                non_pod_finalization,
                gc_coordination.clone(),
                Arc::from(config.node_name.as_str()),
                node_metrics.clone(),
                controller_identity.clone(),
            );
        let controller_dispatcher =
            Arc::new(klights_controllers::ControllerDispatcher::new_complete(
                service_ipam.clone(),
                nodeport_alloc.clone(),
                supervisor.clone(),
                csr_issuer,
                hpa_controller,
                controller_dependencies,
                controller_identity.clone(),
            ));
        side_effects.set_controller_dispatcher(controller_dispatcher.clone());
        let service_routing: Arc<dyn klights_reconcile_api::ServiceRoutingSync> =
            match service_routing_observation {
                Some(services) => Arc::new(services),
                None => Arc::new(
                    crate::bootstrap::network_adapters::ApiServiceRoutingSyncAdapter::new(
                        network.services().clone(),
                    ),
                ),
            };
        let pod_logs_root = config.data_root.join("logs/pods");
        let wall_clock: Arc<dyn klights_supervisor::WallClock> =
            Arc::new(klights_supervisor::SystemWallClock);
        let pod_logs = crate::bootstrap::composition_adapters::node_log_runtime_adapter::pod_log_capabilities(
            Arc::new(
                klights_kubelet::node_api::logs::LocalNodeLogRuntime::new_with_pod_event_store(
                    pod_logs_root.clone(),
                    supervisor.clone(),
                    wall_clock.clone(),
                    klights_kubelet::node_api::logs::PodLogFollowWatchSource::new(Arc::new(
                        crate::bootstrap::kubelet_ports::DatastorePodWatchSource::new(Arc::new(
                            positioned_watch.clone(),
                        )),
                    )),
                ),
            ),
            Arc::new(
                klights_kubelet::node_api::logs::LocalNodeLogRuntime::new_without_pod_event_store(
                    pod_logs_root,
                    supervisor.clone(),
                    wall_clock,
                ),
            ),
            supervisor.clone(),
            config.node_name.clone(),
        );
        let rbac_policy_store: Arc<dyn klights_auth::rbac_policy_store::RbacPolicyStore> = Arc::new(
            klights_auth::rbac_policy_store::ReaderBackedRbacPolicyStore::new(Arc::new(
                crate::bootstrap::auth_adapters::DatastoreRbacResourceReader::new(
                    datastore.clone(),
                ),
            )),
        );
        let runtime_paths =
            k8s_native_service::ApiRuntimePaths::from_data_root(config.data_root.clone())?;
        let mut runtime_inputs = k8s_native_service::ApiRuntimeInputs::new(
            runtime_paths,
            config.api_slow_log_threshold,
        )?;
        if let Some(audit_sink) = audit_sink {
            runtime_inputs = runtime_inputs.with_audit_sink(audit_sink);
        }
        if let Some(priority_fairness) = priority_fairness {
            runtime_inputs = runtime_inputs.with_priority_fairness(priority_fairness);
        }
        let node_name = config.node_name.clone();
        let bootstrap_token_authenticator = bootstrap_token_authenticator.unwrap_or_else(|| {
            Arc::new(
                crate::bootstrap::auth_adapters::DatastoreBootstrapTokenAuthenticator::new(
                    datastore.clone(),
                ),
            )
        });
        let crd_registry = klights_controllers::crd::CrdRegistry::new();
        let operational_replication = mount_operational_endpoints.then(|| {
            Arc::new(klights_replication::ReplicationService::new_with_ports(
                db.focused_recovery_store(),
                Arc::new(
                    crate::bootstrap::bootstrap_token::DatastoreBootstrapTokenValidation::new(
                        datastore.clone(),
                    ),
                ),
                supervisor.clone(),
            ))
        });
        let remote_node_services = operational_replication.as_ref().map(|replication| {
            let adapter =
                crate::bootstrap::grpc_runtime_adapter::GrpcReplicationRuntimeAdapter::new(
                    replication.clone(),
                );
            (
                adapter.clone() as Arc<dyn klights_node_api::NodeExec>,
                adapter as Arc<dyn klights_node_api::NodeLog>,
            )
        });
        let node_lease_tracker = Arc::new(
            klights_controllers::node_lease::NodeLeaseTracker::new_at(chrono::Utc::now()),
        );
        let (router, outer_layers) = k8s_native_service::build_current_router(
            identity.clone(),
            authorizer,
            rbac_policy_store,
            bootstrap_token_authenticator.clone(),
            oidc,
            webhook,
            None,
            Arc::new(
                crate::bootstrap::composition_adapters::watch_stream_adapter::DatastoreWatchStreamAdapter::new(
                    datastore.clone(),
                    watch_signals,
                    positioned_watch.clone(),
                ),
            ),
            crate::bootstrap::composition_adapters::api_state_adapter::RootNamespaceTerminationStore::new(datastore.clone()),
            resource_query,
            resource_command,
            finalizer_lifecycle,
            mutation_effects,
            crate::bootstrap::composition_adapters::list_query_adapter::DatastoreListResourceVersionPort::new(datastore.clone()),
            crate::bootstrap::composition_adapters::list_query_adapter::DatastoreNamespaceListPort::new(datastore.clone()),
            crate::bootstrap::controller_adapters::resource_quota_admission_adapter::ResourceQuotaAdmissionAdapter::new(
                datastore.clone(),
            ),
            crate::bootstrap::composition_adapters::resource_admission_adapter::ResourceAdmissionAdapter::new(
                identity,
                datastore.clone(),
            ),
            crate::bootstrap::composition_adapters::custom_resource_read_adapter::CustomResourceReadAdapter::new(
                datastore.clone(),
                crate::bootstrap::watch_commit_wiring::test_signal_source(&datastore),
                positioned_watch,
                supervisor.clone(),
            ),
            generated.clone(),
            generated.clone(),
            generated.clone(),
            generated,
            Arc::new(
                crate::bootstrap::controller_adapters::gc_delete_adapter::GcOwnerLifecycleAdapter::new_with_coordination(
                    datastore.clone(),
                    pod_repository.clone(),
                    gc_coordination,
                ),
            ),
            api_pod_repository,
            crd_registry.clone(),
            crate::bootstrap::service_adapters::ApiServiceWriteAllocator::new(
                datastore.clone(),
                service_ipam,
                nodeport_alloc.clone(),
                controller_identity,
            ),
            controller_dispatcher.clone(),
            crate::bootstrap::composition_adapters::api_state_adapter::RootApiFailureMetrics::new(metrics),
            crate::bootstrap::composition_adapters::api_state_adapter::RootApiNodeLeaseObservations::new(node_lease_tracker.clone()),
            service_routing.clone(),
            pod_logs,
            None,
            node_metrics.clone(),
            klights_kubelet::node_api::port_forward::local_node_port_forward(supervisor.clone()),
            pod_lifecycle_diagnostics,
            None,
            remote_node_services,
            config.node_name,
            config.anonymous_auth,
            runtime_inputs,
            auth_clock,
            supervisor.clone(),
            signing_keys,
            authority.clone(),
        );
        let router = if mount_operational_endpoints {
            let operational_endpoints = klights_apiserver::OperationalEndpointHandlers::new(
                klights_apiserver::OperationalEndpointInputs::new(
                    klights_apiserver::OperationalNodeRole::Leader,
                    Arc::new(String::new),
                    crate::version::api_version_info(),
                    crate::bootstrap::operational_adapters::ApiClusterStatusMetadata::new(
                        datastore.clone(),
                    ),
                    operational_replication.as_ref().map(|replication| {
                        replication.clone()
                            as Arc<dyn klights_leader_api::LeaderFollowerDiagnostics>
                    }),
                    supervisor.clone(),
                ),
            );
            klights_apiserver::mount_operational_endpoints(
                router.into_router(),
                operational_endpoints,
            )
        } else {
            router.into_router()
        };
        Ok(Self {
            router: outer_layers.finish(router),
            datastore,
            sqlite: db,
            nodeport_alloc,
            pod_repository,
            _node_local: node_local,
            outbox_dispatcher,
            controller_dispatcher,
            crd_registry,
            service_routing,
            node_metrics,
            operational_replication,
            task_supervisor: supervisor,
            node_lease_tracker,
            authority,
            node_name,
        })
    }

    pub fn router(&self) -> axum::Router {
        self.router.clone()
    }

    pub fn router_with_authority(&self, is_leader: bool) -> axum::Router {
        let authority = if is_leader {
            self.authority
                .clone()
                .expect("leader authority harness must be selected")
        } else {
            let (authority, _publisher) =
                klights_replication::authority::WatchLeaderAuthority::channel(false, None);
            authority
        };
        klights_apiserver::wrap_authority_router(
            self.router.clone(),
            Some(Arc::new(
                klights_apiserver::HttpAuthorityRouter::from_authority(authority, None),
            )),
        )
    }

    pub async fn record_node_lease(
        &self,
        node_name: &str,
        lease: &serde_json::Value,
    ) -> anyhow::Result<()> {
        self.node_lease_tracker
            .record_from_lease_object(node_name, lease)
            .await
            .map(|_| ())
    }

    pub fn request_service_routing_sync(
        &self,
    ) -> Result<(), klights_reconcile_api::ReconcileSinkError> {
        self.service_routing.request_service_routing_sync()
    }

    pub async fn finalize_pod_deletion_after_actor_cleanup(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<bool> {
        let finalized_inline = self
            .pod_repository
            .finalize_pod_deletion_after_actor_cleanup(namespace, name, uid)
            .await?;
        if finalized_inline {
            return Ok(true);
        }
        self.drain_node_outbox().await?;
        let live = self
            .datastore
            .get_resource("v1", "Pod", Some(namespace), name)
            .await?;
        Ok(live.is_none_or(|pod| pod.uid != uid))
    }

    pub async fn drain_node_outbox(&self) -> anyhow::Result<()> {
        for _ in 0..1024 {
            if matches!(
                self.outbox_dispatcher
                    .dispatch_due_once(i64::MAX / 4)
                    .await?,
                klights_kubelet::node_outbox::DispatchOutcome::Idle { .. }
            ) {
                return Ok(());
            }
        }
        anyhow::bail!("node outbox drain exceeded 1024 deliveries")
    }

    pub async fn queued_reconcile_keys(&self) -> Vec<klights_reconcile_api::ReconcileKey> {
        self.controller_dispatcher.pending_reconcile_keys().await
    }

    pub async fn drain_controller_reconciles(
        &self,
    ) -> anyhow::Result<Vec<klights_reconcile_api::ReconcileKey>> {
        let mut drained = Vec::new();
        for _ in 0..1024 {
            if self
                .controller_dispatcher
                .pending_reconcile_keys()
                .await
                .is_empty()
            {
                return Ok(drained);
            }
            drained.push(
                self.controller_dispatcher
                    .dispatch_next_key_for_test()
                    .await,
            );
        }
        anyhow::bail!("controller reconcile drain exceeded 1024 operations")
    }

    pub async fn dispatch_next_controller_reconcile(&self) -> klights_reconcile_api::ReconcileKey {
        self.controller_dispatcher
            .dispatch_next_key_for_test()
            .await
    }

    pub async fn reconcile_endpointslice(
        &self,
        service_name: &str,
        service_uid: &str,
        namespace: &str,
        selector: Option<&serde_json::Value>,
        ports: Option<&serde_json::Value>,
    ) -> anyhow::Result<()> {
        klights_controllers::endpoints::reconcile_endpointslice(
            self.datastore.as_ref(),
            self.pod_repository.as_ref(),
            service_name,
            service_uid,
            namespace,
            selector,
            ports,
        )
        .await
    }

    pub fn spawn_controller_worker(
        &self,
        cancel: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let dispatcher = self.controller_dispatcher.clone();
        tokio::spawn(dispatcher.run_worker_pool(1, cancel))
    }

    pub async fn register_crd_value(&self, crd: &serde_json::Value) -> anyhow::Result<()> {
        klights_controllers::crd::register_crd_from_value(&self.crd_registry, crd).await
    }

    pub async fn register_crd_info(&self, info: klights_controllers::crd::CrdResourceInfo) {
        self.crd_registry.register(info).await;
    }

    pub async fn sync_crd_registry_from_datastore(&self) -> anyhow::Result<()> {
        klights_controllers::crd::sync_registry_from_datastore(
            self.datastore.as_ref(),
            &self.crd_registry,
        )
        .await
    }

    pub async fn crd_selectable_fields(
        &self,
        group: &str,
        version: &str,
        plural: &str,
    ) -> Option<Vec<String>> {
        self.crd_registry
            .get(group, version, plural)
            .await
            .map(|info| info.selectable_fields)
    }

    pub fn set_node_metrics(&self, metrics: Arc<dyn klights_node_api::NodeMetrics>) {
        self.node_metrics.set(metrics);
    }

    pub async fn ensure_operational_cluster_metadata(&self) -> anyhow::Result<()> {
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(self.datastore.as_ref()).await?;
        Ok(())
    }

    pub async fn seed_default_rbac(&self) -> anyhow::Result<()> {
        klights_controllers::rbac_reconcile::reconcile_default_rbac_objects(self.datastore.as_ref())
            .await
    }

    pub async fn register_operational_follower(
        &self,
        dataplane: klights_leader_api::NetworkDataplane,
    ) -> anyhow::Result<()> {
        let replication = self
            .operational_replication
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("operational replication is not installed"))?;
        let (_control_rx, _session) = replication.register_follower(dataplane).await;
        Ok(())
    }

    pub async fn register_integration_follower(
        &self,
        dataplane: klights_leader_api::NetworkDataplane,
    ) -> anyhow::Result<IntegrationFollowerSession> {
        let replication = self
            .operational_replication
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("operational replication is not installed"))?
            .clone();
        let node_name = dataplane.node_name().to_string();
        let (control_rx, session_id) = replication.register_follower(dataplane).await;
        Ok(IntegrationFollowerSession {
            replication,
            control_rx,
            node_name,
            session_id,
        })
    }

    pub fn integration_remote_exec_sync(&self) -> anyhow::Result<IntegrationRemoteExecSync> {
        let replication = self
            .operational_replication
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("operational replication is not installed"))?
            .clone();
        Ok(IntegrationRemoteExecSync {
            node_exec: crate::bootstrap::grpc_runtime_adapter::GrpcReplicationRuntimeAdapter::new(
                replication,
            ),
            task_supervisor: self.task_supervisor.clone(),
        })
    }

    /// Existing adapter cases require direct CRUD setup and observation.
    /// This capability is absent unless the integration-only feature is set.
    pub fn datastore(&self) -> IntegrationDatastoreHandle {
        self.datastore.clone()
    }

    pub fn subscribe_watch(
        &self,
        api_version: &str,
        kind: &str,
    ) -> tokio::sync::broadcast::Receiver<klights_watch::WatchEvent> {
        crate::bootstrap::watch_commit_wiring::subscribe_test_events(
            self.datastore.commit_observation_sink().as_ref(),
            klights_watch::WatchTopic::new(api_version, kind),
        )
    }

    pub fn install_resource_mutation_pause(
        &self,
        operation: IntegrationResourceMutationPauseOperation,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Arc<IntegrationResourceMutationPause> {
        self.sqlite
            .install_resource_mutation_pause(operation, api_version, kind, namespace, name)
    }

    pub fn exhaust_nodeports(&self) -> anyhow::Result<()> {
        for expected in 30000..=32767 {
            let allocated = self.nodeport_alloc.allocate().map_err(anyhow::Error::msg)?;
            anyhow::ensure!(
                allocated == expected,
                "unexpected NodePort allocation {allocated}"
            );
        }
        Ok(())
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }
}
