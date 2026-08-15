//! Focused root composition for leader-RPC integration tests.

use std::sync::Arc;

type IntegrationDatastoreHandle = super::SqliteTestStore;

pub struct IntegrationPassiveReadPorts {
    ports: crate::bootstrap::cluster_store::selector::PassiveReadPorts,
}

struct UnavailableLeaderWatchForTest;

impl klights_leader_api::LeaderWatch for UnavailableLeaderWatchForTest {
    fn watch_resources(
        &self,
        _request: klights_leader_api::WatchRequest,
    ) -> klights_leader_api::LeaderWatchFuture<'_> {
        Box::pin(async {
            Err(klights_leader_api::LeaderWatchError::Unavailable {
                message: "positioned watch was not configured for this test server".to_string(),
            })
        })
    }
}

pub struct IntegrationLeaderRpcRuntime {
    runtime: Arc<crate::bootstrap::grpc_runtime_adapter::GrpcReplicationRuntimeAdapter>,
}

pub struct IntegrationLeaderRpcLocalNetworkPorts {
    network: Arc<crate::bootstrap::composition_adapters::leader_topology_cleanup_adapter::ClusterStoreLeaderNetwork>,
    resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
}

pub struct IntegrationLeaderRpcNodePorts {
    resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    lifecycle_status: Arc<dyn klights_leader_api::LeaderNodeLifecycleStatus>,
}

impl IntegrationLeaderRpcNodePorts {
    pub fn resource_query(&self) -> Arc<dyn klights_leader_api::LeaderResourceQuery> {
        self.resource_query.clone()
    }

    pub fn lifecycle_status(&self) -> Arc<dyn klights_leader_api::LeaderNodeLifecycleStatus> {
        self.lifecycle_status.clone()
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
        self.resource_query.clone()
    }

    pub async fn register_node_dataplane(
        &self,
        dataplane: klights_leader_api::NetworkDataplane,
    ) -> Result<(), klights_leader_api::NetworkTopologyError> {
        klights_leader_api::LeaderNetworkTopologyCommand::register_node_dataplane(
            self.network.as_ref(),
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
    applied_outbox: Arc<dyn klights_cluster_store::AppliedOutboxLedger>,
    resource_reads: Arc<dyn klights_cluster_store::ClusterResourceRead>,
}

impl IntegrationLeaderRpcComposition {
    pub fn new(
        db: IntegrationDatastoreHandle,
        applied_outbox: Arc<dyn klights_cluster_store::AppliedOutboxLedger>,
        _committed_apply: Arc<dyn klights_cluster_store::PrivilegedCommittedRaftApply>,
        resource_reads: Arc<dyn klights_cluster_store::ClusterResourceRead>,
    ) -> Self {
        Self {
            db,
            applied_outbox,
            resource_reads,
        }
    }

    pub fn from_sqlite(db: IntegrationDatastoreHandle) -> Self {
        let applied_outbox: Arc<dyn klights_cluster_store::AppliedOutboxLedger> = db.clone();
        let committed_apply = db.focused_committed_apply();
        let resource_reads = db.focused_read_store();
        Self::new(db, applied_outbox, committed_apply, resource_reads)
    }

    pub fn passive_reads_for(
        db: &klights_cluster_datastore::sqlite::embedded::Datastore,
    ) -> IntegrationPassiveReadPorts {
        IntegrationPassiveReadPorts {
            ports: crate::bootstrap::cluster_store::selector::sqlite_passive_read_ports(db),
        }
    }

    pub async fn ensure_cluster_metadata_for(
        db: &klights_cluster_datastore::sqlite::embedded::Datastore,
    ) -> anyhow::Result<()> {
        crate::bootstrap::cluster_meta::ensure_cluster_metadata_sqlite(db).await
    }

    pub async fn ensure_worker_bootstrap_token_for(
        db: &klights_cluster_datastore::sqlite::embedded::Datastore,
    ) -> anyhow::Result<String> {
        crate::bootstrap::bootstrap_token::ensure_worker_bootstrap_token(db).await
    }

    pub async fn create_scoped_bootstrap_token_for(
        db: &klights_cluster_datastore::sqlite::embedded::Datastore,
        token: &str,
        controlplane: bool,
    ) -> anyhow::Result<()> {
        let scope = if controlplane {
            klights_auth::bootstrap_token::BootstrapTokenScope::Controlplane
        } else {
            klights_auth::bootstrap_token::BootstrapTokenScope::Worker
        };
        crate::bootstrap::bootstrap_token::create_scoped_bootstrap_token_secret_with_ttl_for_test(
            db,
            scope,
            token,
            klights_auth::bootstrap_token::BOOTSTRAP_TOKEN_TTL,
        )
        .await
    }

    pub fn controller_dispatcher(
        db: &klights_cluster_datastore::sqlite::embedded::Datastore,
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
            db.as_ref(),
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

    pub async fn seed_namespace(
        db: &klights_cluster_datastore::sqlite::embedded::Datastore,
        name: &str,
    ) {
        let _ = db
            .create_namespace(name, serde_json::json!({"metadata": {"name": name}}))
            .await;
    }

    pub fn broadcast_watch_event(
        db: &klights_cluster_datastore::sqlite::embedded::Datastore,
        event: klights_watch::WatchEvent,
    ) {
        let pending = crate::bootstrap::watch_commit_wiring::staged_post_commit_from_event(event);
        if let Some(sink) = db.commit_observation_sink() {
            sink.observe(&[pending]);
        }
    }

    pub fn local_node_ports(&self, node_name: String) -> IntegrationLeaderRpcNodePorts {
        let (resource_query, lifecycle_status) = self.local_client_with_query(node_name);
        IntegrationLeaderRpcNodePorts {
            resource_query,
            lifecycle_status,
        }
    }

    pub fn local_network_ports(&self, node_name: String) -> IntegrationLeaderRpcLocalNetworkPorts {
        let (resource_query, _lifecycle_status) = self.local_client_with_query(node_name);
        let authority =
            crate::bootstrap::composition_adapters::authority_adapter::always_leader_watch();
        let proposal: Arc<dyn klights_replication::proposal::RaftProposal> = Arc::new(
            crate::bootstrap::outbox_apply_adapter::BackendProposalFixture::new(
                self.applied_outbox.clone(),
                self.db.clone(),
                self.resource_reads.clone(),
            ),
        );
        let network = Arc::new(
            crate::bootstrap::composition_adapters::leader_topology_cleanup_adapter::ClusterStoreLeaderNetwork::new_for_test(
                self.db.focused_read_store(),
                proposal,
                authority,
            ),
        );
        IntegrationLeaderRpcLocalNetworkPorts {
            network,
            resource_query,
        }
    }

    pub fn focused_dataplane(
        dataplane: klights_cluster_store::DataplanePeerMetadata,
    ) -> Result<klights_leader_api::NetworkDataplane, klights_leader_api::NetworkTopologyError>
    {
        crate::bootstrap::leader_conversions::topology::focused_dataplane(dataplane)
    }

    pub async fn open_node_local(
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
        connection_key: &'static str,
    ) -> anyhow::Result<IntegrationLeaderRpcNodeLocal> {
        Ok(IntegrationLeaderRpcNodeLocal {
            stores: crate::bootstrap::node_store::open_node_local(
                crate::bootstrap::cluster_store::backend_kind::BackendKind::Sqlite,
                None,
                supervisor,
                connection_key,
            )
            .await?,
        })
    }

    fn local_client_with_query(
        &self,
        _node_name: String,
    ) -> (
        Arc<dyn klights_leader_api::LeaderResourceQuery>,
        Arc<dyn klights_leader_api::LeaderNodeLifecycleStatus>,
    ) {
        let authority =
            crate::bootstrap::composition_adapters::authority_adapter::always_leader_watch();
        let resource_query = klights_watch::DatastoreResourceQueryAdapter::new_focused_for_test(
            self.resource_reads.clone(),
            crate::bootstrap::authority::AuthorityHandle::from(authority.clone()).authority_arc(),
        );
        let lifecycle_status =
            crate::bootstrap::local_leader_adapters::LocalNodeLifecycleStatusAdapter::new(
                resource_query.clone(),
                crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort::resource_commands_for_test(
                self.applied_outbox.clone(),
                self.db.clone(),
                self.resource_reads.clone(),
                ),
                authority,
            );
        (resource_query, Arc::new(lifecycle_status))
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
        let commands = crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort::resource_commands_for_test(
            self.applied_outbox.clone(),
            self.db.clone(),
            self.resource_reads.clone(),
        );
        let store = crate::bootstrap::composition_adapters::leader_bootstrap_store_adapter::LeaderBootstrapStore::new(
            self.resource_reads.clone(),
            self.db.focused_read_store(),
            commands,
        );
        crate::bootstrap::node_registration_adapter::register_leader_node_snapshot(
            &store,
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
        let pod_store = crate::bootstrap::pod_repository_composition::new_pod_store(
            Arc::new(self.db.as_ref().clone()),
            self.db.clone(),
            self.db.focused_read_store(),
            self.db.focused_read_store(),
        );
        let endpoint_store = crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort::new_for_test(
            Arc::new(self.db.as_ref().clone()),
            self.db.clone(),
            self.db.focused_read_store(),
            self.db.focused_read_store(),
        );
        klights_controllers::endpoints::reconcile_service_endpoints_batch(
            &endpoint_store,
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
                crate::bootstrap::bootstrap_token::DatastoreBootstrapTokenValidation::new_for_test(
                    self.db.as_ref(),
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
                crate::bootstrap::bootstrap_token::DatastoreBootstrapTokenValidation::new_for_test(
                    self.db.as_ref(),
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
        passive_reads: Option<IntegrationPassiveReadPorts>,
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
        let authority =
            crate::bootstrap::composition_adapters::authority_adapter::always_leader_watch();
        let proposal: Arc<dyn klights_replication::proposal::RaftProposal> = Arc::new(
            crate::bootstrap::outbox_apply_adapter::BackendProposalFixture::new(
                self.applied_outbox.clone(),
                self.db.clone(),
                self.resource_reads.clone(),
            ),
        );
        let network = Arc::new(
            crate::bootstrap::composition_adapters::leader_topology_cleanup_adapter::ClusterStoreLeaderNetwork::new_for_test(
                self.db.focused_read_store(),
                proposal.clone(),
                authority.clone(),
            ),
        );
        let pod_cleanup = Arc::new(
            crate::bootstrap::composition_adapters::leader_topology_cleanup_adapter::ClusterStoreLeaderPodCleanup::new_for_test(
                self.db.clone(),
                proposal,
                authority.clone(),
            ),
        );
        let resource_query = match passive_reads.as_ref() {
            Some(passive_reads) => {
                klights_watch::DatastoreResourceQueryAdapter::new_with_resource_reads_and_clock(
                    passive_reads.ports.resource_reads(),
                    crate::bootstrap::authority::AuthorityHandle::from(authority.clone())
                        .authority_arc(),
                    Arc::new(klights_supervisor::SystemWallClock),
                )
            }
            None => klights_watch::DatastoreResourceQueryAdapter::new_focused_for_test(
                self.resource_reads.clone(),
                crate::bootstrap::authority::AuthorityHandle::from(authority.clone())
                    .authority_arc(),
            ),
        };
        let authority_handle =
            crate::bootstrap::authority::AuthorityHandle::from(authority.clone());
        let side_effects =
            crate::bootstrap::local_leader_adapters::new_local_outbox_side_effect_state(
                self.resource_reads.clone(),
                self.db.clone(),
                self.db.focused_read_store(),
            );
        if let Some(dispatcher) = controller_dispatcher {
            side_effects.set_controller_dispatcher(dispatcher);
        }
        side_effects.set_non_pod_finalization(Arc::new(
            crate::bootstrap::controller_adapters::gc_delete_adapter::GcNonPodFinalizationAdapter::new_for_test(
                self.applied_outbox.clone(),
                self.db.clone(),
                self.resource_reads.clone(),
                self.db.focused_read_store(),
            ),
        ));
        let resource_command = crate::bootstrap::composition_adapters::
            committed_outbox_delivery_adapter::test_resource_command(
                &authority_handle,
                self.applied_outbox.clone(),
                self.db.clone(),
                self.resource_reads.clone(),
            );
        let authenticated_outbox = crate::bootstrap::composition_adapters::
            committed_outbox_delivery_adapter::test_outbox_delivery(
                &authority_handle,
                side_effects,
                "grpc-test".to_string(),
                self.applied_outbox.clone(),
                self.db.clone(),
                self.resource_reads.clone(),
            );
        let local_node_lease = Arc::new(
            crate::bootstrap::local_leader_adapters::LocalNodeLeaseRenewalAdapter::new(
                node_lease_tracker,
                authority.clone(),
            ),
        );
        let local_projected_token = Arc::new(
            crate::bootstrap::local_leader_adapters::LocalProjectedTokenAdapter::new(
                self.resource_reads.clone(),
                "grpc-test".to_string(),
                crate::paths::runtime_namespace(),
                crate::paths::service_account_signing_key_path(&crate::paths::runtime_namespace()),
                authority.clone(),
                Self::file_process_executor(),
            ),
        );
        let projected_token = Arc::new(
            crate::bootstrap::local_leader_adapters::AuthenticatedProjectedTokenIssuer::new(
                local_projected_token,
            ),
        );
        let positioned_watch: Arc<dyn klights_leader_api::LeaderWatch> = match passive_reads {
            Some(passive_reads) => {
                Arc::new(Self::positioned_watch(&passive_reads, self.db.clone()))
            }
            None => Arc::new(UnavailableLeaderWatchForTest),
        };
        let ports = klights_leader_rpc::server::ReplicationServerPorts::from_focused(
            resource_query,
            resource_command,
            positioned_watch,
            authenticated_outbox,
            projected_token,
            pod_cleanup,
            local_node_lease,
            network.clone(),
            network.clone(),
            network,
        );
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
