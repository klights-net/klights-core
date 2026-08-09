//! Focused root composition for leader-RPC integration tests.

use std::sync::Arc;

use crate::datastore::DatastoreHandle as IntegrationDatastoreHandle;

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
    stores: crate::bootstrap::node_store::NodeLocalStores,
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
            klights_auth::bootstrap_token::BootstrapTokenScope::Controlplane
        } else {
            klights_auth::bootstrap_token::BootstrapTokenScope::Worker
        };
        crate::bootstrap::bootstrap_token::create_scoped_bootstrap_token_secret_for_test(
            db, scope, token,
        )
        .await
    }

    pub fn controller_dispatcher(
        db: &crate::datastore::sqlite::Datastore,
    ) -> Arc<klights_controllers::ControllerDispatcher> {
        crate::bootstrap::controller_adapters::controller_runtime_adapter::dispatcher_for_test(
            db,
            Arc::new(klights_controllers::service::ServiceIpam::new(
                "10.43.128.0/17",
            )),
        )
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
        let identity = crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity();
        klights_controllers::namespace::init_default_namespaces_with_ca_path(
            &Self::file_process_executor(),
            self.db.as_ref(),
            &crate::paths::ca_cert_path(&crate::paths::runtime_namespace()),
            chrono::DateTime::UNIX_EPOCH,
            identity.as_ref(),
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
        let pod_store =
            crate::bootstrap::pod_repository_composition::new_pod_store(self.db.clone());
        klights_controllers::endpoints::reconcile_service_endpoints_batch(
            self.db.as_ref(),
            &pod_store,
            request,
        )
        .await
    }

    #[allow(dead_code)]
    pub async fn ensure_cluster_metadata(&self) -> anyhow::Result<()> {
        Self::ensure_cluster_metadata_for(self.db.as_ref()).await
    }

    #[allow(dead_code)]
    pub async fn ensure_worker_bootstrap_token(&self) -> anyhow::Result<String> {
        Self::ensure_worker_bootstrap_token_for(self.db.as_ref()).await
    }

    #[allow(dead_code)]
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

    #[allow(clippy::type_complexity)]
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
