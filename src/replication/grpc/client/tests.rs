//! gRPC client tests.

mod cases {
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::datastore::backend::DatastoreHandle;
    use crate::datastore::command::{
        COMMAND_CODEC_VERSION, CommandId, CommandMeta, StorageCommand,
    };
    use crate::networking::wireguard::{DataplaneEncryption, DataplaneMode};
    use crate::replication::grpc::client::{
        ChannelLane, GrpcClientConfig, JoinDataplaneMetadata, LocalPodLogHandler,
        NodeExecStreamHandler, NodeExecSyncHandler, PodLogHandler, ReplicationGrpcClient,
    };
    use crate::replication::grpc::client::{ConnectDispatchContext, dispatch_leader_message};
    use crate::replication::grpc::generated::{self, follower_message, leader_message};
    use crate::replication::protocol::{
        ExecStreamChannel, JoinRole, NodeExecRequest, NodeExecStreamFrame, NodeExecSyncRequest,
        NodeExecSyncResponse, PodLogRequest, ReplicationEntry, StreamItem,
    };
    use crate::replication::service::ReplicationService;
    use crate::task_supervisor::{TaskCategoryConfig, TaskSupervisor};
    use futures::StreamExt as _;
    use tokio_util::sync::CancellationToken;

    use crate::leader_tls_policy::LeaderTlsVerification;

    static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn dataplane() -> JoinDataplaneMetadata {
        JoinDataplaneMetadata {
            public_key: None,
            endpoint: "127.0.0.1".to_string(),
            port: None,
            mode: DataplaneMode::Root,
            encryption: DataplaneEncryption::Disabled,
        }
    }

    fn default_transport_policy()
    -> crate::replication::grpc::transport_policy::SharedGrpcTransportPolicy {
        crate::replication::grpc::transport_policy::GrpcTransportPolicy::shared_default()
    }

    fn grpc_config_for_tls(ca_cert_path: Option<PathBuf>, skip_ca: bool) -> GrpcClientConfig {
        GrpcClientConfig {
            leader_endpoint: "https://leader:7679".to_string(),
            token: "abcdef.0123456789abcdef".to_string(),
            node_name: "worker-1".to_string(),
            role: JoinRole::Worker,
            dataplane: dataplane(),
            ca_cert_path,
            skip_ca,
            client_cert_pem: None,
            client_key_pem: None,
        }
    }

    #[test]
    fn tls_verification_policy_prefers_configured_ca_over_skip_ca() {
        let ca_path = PathBuf::from("/tmp/leader-ca.crt");
        let config = grpc_config_for_tls(Some(ca_path.clone()), true);

        assert_eq!(
            config.leader_tls_verification(),
            LeaderTlsVerification::CaFile(ca_path)
        );
    }

    #[test]
    fn tls_verification_policy_uses_configured_ca_without_skip_ca() {
        let ca_path = PathBuf::from("/tmp/leader-ca.crt");
        let config = grpc_config_for_tls(Some(ca_path.clone()), false);

        assert_eq!(
            config.leader_tls_verification(),
            LeaderTlsVerification::CaFile(ca_path)
        );
    }

    #[test]
    fn tls_verification_policy_uses_system_roots_without_ca_or_skip_ca() {
        let config = grpc_config_for_tls(None, false);

        assert_eq!(
            config.leader_tls_verification(),
            LeaderTlsVerification::SystemRoots
        );
    }

    #[test]
    fn worker_constructor_preserves_skip_ca_flag() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let client = ReplicationGrpcClient::worker(
            "https://leader:7679".to_string(),
            "worker-1".to_string(),
            "abcdef.0123456789abcdef".to_string(),
            dataplane(),
            None,
            true,
            supervisor,
            default_transport_policy(),
        );

        assert!(client.config.skip_ca);
    }

    #[test]
    fn steady_state_rpc_omits_bootstrap_token_from_metadata_and_join_payload() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let client = ReplicationGrpcClient::new(
            GrpcClientConfig {
                leader_endpoint: "https://leader:7679".to_string(),
                token: "abcdef.0123456789abcdef".to_string(),
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: false,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor,
            crate::replication::grpc::transport_policy::GrpcTransportPolicy::shared_default(),
        );

        let mut request =
            tonic::Request::new(crate::replication::grpc::generated::MetadataRequest {});
        client.add_join_token(&mut request).unwrap();

        assert!(
            !request
                .metadata()
                .contains_key(crate::replication::grpc::JOIN_TOKEN_METADATA_KEY)
        );
        assert_eq!(client.join_request().token, "");
    }

    #[tokio::test]
    async fn observe_leader_endpoint_request_sends_observed_endpoint_response() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let context = ConnectDispatchContext {
            supervisor,
            node_exec_sync_handler: Arc::new(tokio::sync::Mutex::new(None)),
            node_exec_stream_handler: Arc::new(tokio::sync::Mutex::new(None)),
            node_exec_inputs: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            pod_log_handler: Arc::new(tokio::sync::Mutex::new(None)),
            observed_leader_endpoint: Some("10.99.0.10".to_string()),
        };
        let (outbound, mut outbound_rx) = tokio::sync::mpsc::channel(1);
        let (stream_tx, _stream_rx) = tokio::sync::mpsc::channel(1);

        dispatch_leader_message(
            generated::LeaderMessage {
                payload: Some(leader_message::Payload::ObserveLeaderEndpointRequest(
                    generated::ObserveLeaderEndpointRequest {},
                )),
            },
            &outbound,
            &stream_tx,
            &context,
        )
        .await
        .expect("observe request should be handled");

        let response = outbound_rx
            .recv()
            .await
            .expect("client should send observed endpoint response");
        match response.payload {
            Some(follower_message::Payload::ObservedLeaderEndpoint(observed)) => {
                assert_eq!(observed.endpoint, "10.99.0.10");
            }
            other => panic!("unexpected follower response: {other:?}"),
        }
    }

    #[test]
    fn csr_rpc_allows_bootstrap_token_metadata_before_node_cert_exists() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let client = ReplicationGrpcClient::new(
            GrpcClientConfig {
                leader_endpoint: "https://leader:7679".to_string(),
                token: "abcdef.0123456789abcdef".to_string(),
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: false,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor,
            crate::replication::grpc::transport_policy::GrpcTransportPolicy::shared_default(),
        );

        // bug-grpc A2: the CSR token is now precomputed (so the `unary_call`
        // closure can attach it per attempt). With a bootstrap token and no
        // node-cert mTLS, the value must be present.
        let value = client
            .bootstrap_csr_token_value()
            .expect("token must parse as gRPC metadata");
        assert!(
            value.is_some(),
            "a configured bootstrap token (no node cert) must produce a CSR token value"
        );
    }

    struct TlsGrpcLeaderFixture {
        endpoint: String,
        token: String,
        namespace: String,
        ca_cert_path: PathBuf,
        wrong_ca_cert_path: PathBuf,
        node_cert_pem: String,
        node_key_pem: String,
        supervisor: Arc<TaskSupervisor>,
        /// bug-grpc A2: the client gets its OWN supervisor, distinct from the
        /// server's. `shutdown()` simulates a leader-process restart by tearing
        /// down the server + its supervisor; the worker's supervisor must stay
        /// alive (a real leader restart does not cancel the worker), otherwise
        /// the post-restart `renew_node_lease` (now routed through
        /// `supervisor.timeout`) would return "root shutdown" instead of the
        /// transport error that drives lane self-heal.
        client_supervisor: Arc<TaskSupervisor>,
        shutdown: CancellationToken,
        handle: tokio::task::JoinHandle<anyhow::Result<()>>,
    }

    impl TlsGrpcLeaderFixture {
        async fn start() -> Self {
            let namespace = format!("grpc-tls-leader-{}", unique_suffix());
            let (ca_cert_path, wrong_ca_cert_path, node_cert_pem, node_key_pem) =
                write_leader_tls_files(&namespace);
            let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
            crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
                .await
                .unwrap();
            let token = crate::bootstrap::cluster_meta::read_join_token(db.as_ref())
                .await
                .unwrap();
            let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
            let service = Arc::new(ReplicationService::new(db.clone(), supervisor.clone()));
            let app = crate::replication::grpc::server::mount_service(
                axum::Router::new(),
                service,
                db,
                default_transport_policy(),
            );
            let addr = reserve_loopback_addr();
            let endpoint = format!("https://localhost:{}", addr.port());
            let shutdown = CancellationToken::new();
            let server_shutdown = shutdown.clone();
            let server_supervisor = supervisor.clone();
            let server_namespace = namespace.clone();
            let handle = tokio::spawn(async move {
                crate::bootstrap::init::tls::serve_https(
                    app,
                    &addr.to_string(),
                    &server_namespace,
                    server_supervisor,
                    default_transport_policy(),
                    server_shutdown.cancelled_owned(),
                )
                .await
            });
            wait_for_tcp_listener(addr).await;

            let client_supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));

            Self {
                endpoint,
                token,
                namespace,
                ca_cert_path,
                wrong_ca_cert_path,
                node_cert_pem,
                node_key_pem,
                supervisor,
                client_supervisor,
                shutdown,
                handle,
            }
        }

        async fn connect(
            &self,
            ca_cert_path: Option<PathBuf>,
            skip_ca: bool,
        ) -> anyhow::Result<ReplicationGrpcClient> {
            ReplicationGrpcClient::connect(
                GrpcClientConfig {
                    leader_endpoint: self.endpoint.clone(),
                    token: self.token.clone(),
                    node_name: "worker-1".to_string(),
                    role: JoinRole::Worker,
                    dataplane: dataplane(),
                    ca_cert_path,
                    skip_ca,
                    client_cert_pem: Some(self.node_cert_pem.clone()),
                    client_key_pem: Some(self.node_key_pem.clone()),
                },
                self.client_supervisor.clone(),
                default_transport_policy(),
            )
            .await
        }

        async fn shutdown(self) {
            self.shutdown.cancel();
            let _ = tokio::time::timeout(Duration::from_secs(2), self.handle).await;
            let _ = self.supervisor.shutdown(Duration::from_secs(1)).await;
            let _ = std::fs::remove_dir_all(crate::paths::data_root_path(&self.namespace));
        }
    }

    fn unique_suffix() -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{}-{nanos}", std::process::id())
    }

    fn reserve_loopback_addr() -> SocketAddr {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap()
    }

    async fn wait_for_tcp_listener(addr: SocketAddr) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "TLS gRPC fixture did not start listening on {addr}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn write_leader_tls_files(namespace: &str) -> (PathBuf, PathBuf, String, String) {
        let etc_dir = crate::paths::etc_dir_path(namespace);
        std::fs::create_dir_all(&etc_dir).unwrap();

        let (ca_cert, ca_key, ca_cert_pem, _ca_key_pem) = crate::auth::generate_ca_full().unwrap();
        let (server_cert_pem, server_key_pem) =
            crate::auth::generate_server_cert(&ca_cert, &ca_key).unwrap();
        let (node_cert_pem, node_key_pem) =
            generate_node_client_cert(&ca_cert, &ca_key, "worker-1");
        let ca_cert_path = crate::paths::ca_cert_path(namespace);
        std::fs::write(&ca_cert_path, ca_cert_pem).unwrap();
        std::fs::write(crate::paths::server_cert_path(namespace), server_cert_pem).unwrap();
        std::fs::write(crate::paths::server_key_path(namespace), server_key_pem).unwrap();

        let (_, _, wrong_ca_cert_pem, _) = crate::auth::generate_ca_full().unwrap();
        let wrong_ca_cert_path = etc_dir.join("wrong-ca.crt");
        std::fs::write(&wrong_ca_cert_path, wrong_ca_cert_pem).unwrap();

        (
            ca_cert_path,
            wrong_ca_cert_path,
            node_cert_pem,
            node_key_pem,
        )
    }

    fn generate_node_client_cert(
        ca_cert: &rcgen::Certificate,
        ca_key: &rcgen::KeyPair,
        node_name: &str,
    ) -> (String, String) {
        use rcgen::{CertificateParams, DnType, KeyPair};

        let mut params = CertificateParams::default();
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, format!("system:node:{node_name}"));
        params
            .distinguished_name
            .push(DnType::OrganizationName, "system:nodes");
        params
            .extended_key_usages
            .push(rcgen::ExtendedKeyUsagePurpose::ClientAuth);
        let key_pair = KeyPair::generate().unwrap();
        let cert = params.signed_by(&key_pair, ca_cert, ca_key).unwrap();
        (cert.pem(), key_pair.serialize_pem())
    }

    fn test_node_client_cert_der(node_name: &str) -> Vec<u8> {
        use rcgen::{CertificateParams, DnType, KeyPair, KeyUsagePurpose};
        use time::{Duration, OffsetDateTime};

        let mut params = CertificateParams::default();
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, format!("system:node:{node_name}"));
        params
            .distinguished_name
            .push(DnType::OrganizationName, "system:nodes");
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        params.not_before = OffsetDateTime::now_utc() - Duration::seconds(60);
        params.not_after = OffsetDateTime::now_utc() + Duration::seconds(31_536_000);
        let key_pair = KeyPair::generate().unwrap();
        params.self_signed(&key_pair).unwrap().der().to_vec()
    }

    fn mount_test_service_with_node_cert(app: axum::Router, node_name: &str) -> axum::Router {
        app.layer(axum::Extension(crate::auth::TlsClientCertificate(
            test_node_client_cert_der(node_name),
        )))
    }

    #[tokio::test]
    async fn https_join_without_skip_ca_succeeds_with_trusted_ca() {
        let fixture = TlsGrpcLeaderFixture::start().await;
        let client = fixture
            .connect(Some(fixture.ca_cert_path.clone()), false)
            .await
            .unwrap();
        let metadata = client.metadata().await.unwrap();

        assert!(!metadata.cluster_id.is_empty());
        fixture.shutdown().await;
    }

    #[tokio::test]
    async fn https_join_without_skip_ca_rejects_wrong_ca() {
        let fixture = TlsGrpcLeaderFixture::start().await;
        let err = match fixture
            .connect(Some(fixture.wrong_ca_cert_path.clone()), false)
            .await
        {
            Ok(_) => panic!("wrong CA must not allow a verified TLS join"),
            Err(err) => err,
        };
        let message = format!("{err:#}");

        assert!(
            message.contains("UnknownIssuer")
                || message.contains("invalid peer certificate")
                || message.contains("certificate"),
            "expected TLS certificate validation failure, got: {message}"
        );
        assert!(
            !message.contains("invalid bootstrap token"),
            "wrong CA must fail during TLS validation before token auth: {message}"
        );
        fixture.shutdown().await;
    }

    #[tokio::test]
    async fn https_join_with_skip_ca_succeeds_without_ca_trust() {
        let fixture = TlsGrpcLeaderFixture::start().await;
        let client = fixture.connect(None, true).await.unwrap();
        let metadata = client.metadata().await.unwrap();

        assert!(!metadata.cluster_id.is_empty());
        fixture.shutdown().await;
    }

    #[tokio::test]
    async fn observed_leader_endpoint_uses_connected_peer_ip_for_hostname_endpoint() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let token = crate::bootstrap::cluster_meta::read_join_token(db.as_ref())
            .await
            .unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor.clone()));
        let app = crate::replication::grpc::server::mount_service(
            axum::Router::new(),
            service,
            db,
            default_transport_policy(),
        );
        let app = mount_test_service_with_node_cert(app, "worker-1");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://localhost:{}", listener.local_addr().unwrap().port());
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let client = ReplicationGrpcClient::connect(
            GrpcClientConfig {
                leader_endpoint: endpoint,
                token,
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: false,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor.clone(),
            default_transport_policy(),
        )
        .await
        .unwrap();

        assert_eq!(
            client.observed_leader_endpoint_for_report().as_deref(),
            Some("127.0.0.1"),
            "hostname leader endpoints must report the actual connected peer IP"
        );
        handle.abort();
        let _ = supervisor.shutdown(Duration::from_secs(1)).await;
    }

    #[test]
    fn observed_leader_endpoint_is_none_until_transport_observes_peer() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let client = ReplicationGrpcClient::new(
            GrpcClientConfig {
                leader_endpoint: "https://10.99.0.10:7679".to_string(),
                token: "abcdef.0123456789abcdef".to_string(),
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: false,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor,
            crate::replication::grpc::transport_policy::GrpcTransportPolicy::shared_default(),
        );

        assert_eq!(client.observed_leader_endpoint_for_report(), None);
    }

    #[tokio::test]
    async fn https_join_with_node_cert_succeeds_without_bootstrap_token() {
        let fixture = TlsGrpcLeaderFixture::start().await;
        let client = ReplicationGrpcClient::connect(
            GrpcClientConfig {
                leader_endpoint: fixture.endpoint.clone(),
                token: "wrong-token-must-not-be-sent".to_string(),
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: Some(fixture.ca_cert_path.clone()),
                skip_ca: false,
                client_cert_pem: Some(fixture.node_cert_pem.clone()),
                client_key_pem: Some(fixture.node_key_pem.clone()),
            },
            fixture.supervisor.clone(),
            default_transport_policy(),
        )
        .await
        .unwrap();
        let metadata = client.metadata().await.unwrap();

        assert!(!metadata.cluster_id.is_empty());
        fixture.shutdown().await;
    }

    async fn client_and_service() -> (
        ReplicationGrpcClient,
        Arc<ReplicationService>,
        DatastoreHandle,
        tokio::task::JoinHandle<()>,
    ) {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let token = crate::bootstrap::cluster_meta::read_join_token(db.as_ref())
            .await
            .unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor.clone()));
        let app = crate::replication::grpc::server::mount_service(
            axum::Router::new(),
            service.clone(),
            db.clone(),
            default_transport_policy(),
        );
        let app = mount_test_service_with_node_cert(app, "worker-1");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let client = ReplicationGrpcClient::connect(
            GrpcClientConfig {
                leader_endpoint: endpoint,
                token,
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: false,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor,
            default_transport_policy(),
        )
        .await
        .unwrap();
        (client, service, db, handle)
    }

    fn sample_entry(rv: i64) -> ReplicationEntry {
        ReplicationEntry {
            command: StorageCommand::CreateNamespace {
                name: format!("client-stream-{rv}"),
                data: serde_json::json!({"metadata": {"name": format!("client-stream-{rv}")}}),
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

    #[tokio::test]
    async fn client_connects_get_metadata_and_receives_stream_item() {
        let (client, service, _db, handle) = client_and_service().await;
        let metadata = client.metadata().await.unwrap();
        assert!(!metadata.cluster_id.is_empty());

        service.notify_entry(sample_entry(7));
        match client.stream_next().await.unwrap() {
            StreamItem::Entry(entry) => assert_eq!(entry.meta.resource_version, 7),
            other => panic!("expected entry, got {other:?}"),
        }
        client.ack(7).await.unwrap();
        handle.abort();
    }

    #[tokio::test]
    async fn client_reset_stream_drops_buffered_entries_before_reconnect() {
        let (client, service, _db, handle) = client_and_service().await;

        service.notify_entry(sample_entry(7));
        service.notify_entry(sample_entry(8));
        match client.stream_next().await.unwrap() {
            StreamItem::Entry(entry) => assert_eq!(entry.meta.resource_version, 7),
            other => panic!("expected entry, got {other:?}"),
        }

        client.reset_stream().await;
        client.ensure_joined().await.unwrap();
        service.notify_entry(sample_entry(9));

        match client.stream_next().await.unwrap() {
            StreamItem::Entry(entry) => assert_eq!(entry.meta.resource_version, 9),
            other => panic!("expected entry after reset, got {other:?}"),
        }
        handle.abort();
    }

    #[tokio::test]
    async fn unary_rpcs_reuse_a_single_channel() {
        // bug-grpc: every unary RPC used to rebuild the leader Channel
        // (a fresh TLS handshake per call). With per-lane channel pools,
        // repeated reads must reuse the warm Read-lane pool — the build
        // count must not grow across calls.
        let (client, _service, _db, handle) = client_and_service().await;
        client.metadata().await.unwrap(); // first Read-lane build (pool)
        let n = client.channel_build_count();
        for _ in 0..5 {
            client.metadata().await.unwrap();
        }
        assert_eq!(
            client.channel_build_count(),
            n,
            "unary reads must reuse the pooled Read-lane channels, not rebuild per call"
        );
        handle.abort();
    }

    #[tokio::test]
    async fn reset_stream_rebuilds_only_stream_lane() {
        // bug-grpc invariant §3.2.4: a stream reset must invalidate ONLY
        // the Stream lane. The hot Status/Read lanes must survive — the
        // old `clear_stream → invalidate everything` coupling needlessly
        // dropped the unary channel on every stream flap.
        let (client, _service, _db, handle) = client_and_service().await;
        client.metadata().await.unwrap();
        let before = client.channel_build_count();
        assert!(
            client.lane_endpoint(ChannelLane::Read).await.is_some(),
            "metadata must have populated the Read lane"
        );

        client.reset_stream().await;
        assert!(
            client.lane_endpoint(ChannelLane::Stream).await.is_none(),
            "reset_stream must drop the Stream lane"
        );

        // The Read lane survived: a read does not rebuild.
        client.metadata().await.unwrap();
        assert_eq!(
            client.channel_build_count(),
            before,
            "Read lane must survive a stream reset (no rebuild)"
        );
        handle.abort();
    }

    #[tokio::test]
    async fn endpoint_failover_rebuilds_only_target_lane() {
        // Failover: when the active leader endpoint changes, the lane's
        // pool (built for the old endpoint) must not be served; the next
        // RPC must attempt a fresh build against the new endpoint, while
        // other lanes are untouched.
        let (client, _service, _db, handle) = client_and_service().await;
        client.metadata().await.unwrap();
        let before = client.channel_build_count();
        let stream_endpoint = client.lane_endpoint(ChannelLane::Stream).await;

        client.set_current_leader_endpoint(Some("https://127.0.0.1:1".to_string()));
        // Bogus endpoint -> connect fails, but the build (handshake)
        // attempt must still happen, proving the stale pool was not served.
        let _ = client.metadata().await;
        assert!(
            client.channel_build_count() > before,
            "endpoint change must force a rebuild attempt on the Read lane"
        );
        assert_eq!(
            client.lane_endpoint(ChannelLane::Stream).await,
            stream_endpoint,
            "a Read-lane failover must not touch the Stream lane"
        );
        handle.abort();
    }

    #[tokio::test]
    async fn status_rpcs_reuse_pooled_channels() {
        // bug-grpc §5.1 (RED before the lane pool): the hot worker→leader
        // status path (`apply_outbox`) used to build a fresh TLS channel
        // per call. With the Status lane pool the build count must settle
        // at <= the pool size after the first call and never grow after.
        use crate::kubelet::outbox::OutboxApplyClient;
        let (client, _service, _db, handle) = client_and_service().await;

        // First status RPC builds the Status-lane pool.
        let _ = client
            .apply_outbox(
                "status-key-0",
                crate::kubelet::outbox::payload::OutboxOperation::PodStatus,
                bytes::Bytes::new(),
            )
            .await;
        let after_first = client.channel_build_count();
        let status_pool = client.lane_pool_len(ChannelLane::Status).await;
        assert!(
            status_pool >= 1 && after_first >= status_pool as u64,
            "first status RPC must build the Status-lane pool"
        );

        for i in 1..20 {
            let _ = client
                .apply_outbox(
                    &format!("status-key-{i}"),
                    crate::kubelet::outbox::payload::OutboxOperation::PodStatus,
                    bytes::Bytes::new(),
                )
                .await;
        }
        assert_eq!(
            client.channel_build_count(),
            after_first,
            "status RPCs must reuse the pooled Status-lane channels, not handshake per call"
        );
        assert!(
            client.lane_endpoint(ChannelLane::Stream).await
                != client.lane_endpoint(ChannelLane::Status).await
                || client.lane_pool_len(ChannelLane::Status).await > 1,
            "Status lane must not collapse onto the single Stream connection"
        );
        handle.abort();
    }

    #[tokio::test]
    async fn apply_outbox_retry_after_lost_response_is_deduped() {
        // Pillar A end-to-end lost-response dedupe: the leader commits an
        // outbox mutation and records the idempotency ledger entry, but the
        // response is dropped on the wire (lossy worker->leader link). The
        // dispatcher retries the SAME idempotency key. The leader must replay
        // the recorded result as AlreadyApplied — same applied RV, mutation
        // applied exactly once — never a second mutation.
        //
        // Server-side exactly-once is unit-tested at the datastore layer
        // (`raft_apply_same_idempotency_key_returns_same_rv_without_reapply`);
        // this locks the client->server gRPC path that carries the key.
        use crate::datastore::ResourcePreconditions;
        use crate::kubelet::outbox::OutboxApplyResult;
        use crate::kubelet::outbox::payload::{OutboxOperation, OutboxPayload};

        let (client, _service, db, handle) = client_and_service().await;

        // A Pod must exist for the PodStatus update to apply.
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "web",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"namespace": "default", "name": "web", "uid": "pod-uid-1"},
                "spec": {
                    "nodeName": "worker-1",
                    "containers": [{"name": "app", "image": "nginx"}]
                },
                "status": {"phase": "Pending"}
            }),
        )
        .await
        .expect("create pod");

        let payload = {
            let command = StorageCommand::UpdateStatus {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "web".to_string(),
                status: serde_json::json!({"phase": "Running", "message": "applied-once"}),
                expected_rv: None,
                preconditions: ResourcePreconditions {
                    uid: Some("pod-uid-1".to_string()),
                    resource_version: None,
                },
                observed_status_stamp: None,
            };
            bytes::Bytes::from(
                OutboxPayload::from_command(command)
                    .encode_protobuf()
                    .expect("encode pod status payload"),
            )
        };

        let key = "p3-lost-response-key";

        // First send: the leader commits and records the idempotency ledger.
        let first = client
            .apply_outbox_rpc(key, OutboxOperation::PodStatus, payload.clone())
            .await
            .expect("first apply must commit");
        let applied_rv = match first {
            OutboxApplyResult::Applied { applied_rv } => applied_rv,
            other => panic!("first apply must be Applied, got {other:?}"),
        };
        let rv_after_first = db.get_current_resource_version().await.unwrap();
        assert_eq!(applied_rv, rv_after_first);

        let pod_message = |db: DatastoreHandle| async move {
            db.get_resource("v1", "Pod", Some("default"), "web")
                .await
                .expect("read pod")
                .expect("pod exists")
                .data
                .pointer("/status/message")
                .and_then(|v| v.as_str())
                .map(str::to_string)
        };
        assert_eq!(
            pod_message(db.clone()).await.as_deref(),
            Some("applied-once"),
            "first apply must land the status mutation"
        );

        // The first response was "lost" on the wire; the dispatcher retries the
        // SAME key. The leader must replay the ledger as AlreadyApplied.
        let second = client
            .apply_outbox_rpc(key, OutboxOperation::PodStatus, payload)
            .await
            .expect("lost-response retry must succeed");
        match second {
            OutboxApplyResult::AlreadyApplied {
                applied_rv: replayed,
            } => {
                assert_eq!(
                    replayed,
                    Some(applied_rv),
                    "retry must replay the original applied RV"
                );
            }
            other => panic!("lost-response retry must be AlreadyApplied, got {other:?}"),
        }

        // Mutation applied exactly once: no new RV, single ledger row for the key.
        assert_eq!(
            db.get_current_resource_version().await.unwrap(),
            rv_after_first,
            "duplicate apply must not allocate another RV"
        );
        let matching = db
            .list_applied_outbox()
            .await
            .unwrap()
            .into_iter()
            .filter(|r| r.idempotency_key == key)
            .count();
        assert_eq!(
            matching, 1,
            "exactly one idempotency ledger row must exist for the retried key"
        );

        handle.abort();
    }

    #[tokio::test]
    async fn apply_outbox_aborts_on_per_call_deadline() {
        // bug-grpc: under partial packet loss the HTTP/2 keepalive PING still
        // gets through (connection deemed alive) while the RPC's response is
        // wedged. Without a per-call deadline `apply_outbox` blocks forever,
        // stalling every Status-lane slot — the 10-minute "stable cluster"
        // stall where a worker's pod deletions never reach the leader. The
        // deadline must abort the wedged call, evict the lane, and surface
        // Retryable so the dispatcher re-sends on a fresh connection.
        use crate::kubelet::outbox::OutboxApplyError;
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let token = crate::bootstrap::cluster_meta::read_join_token(db.as_ref())
            .await
            .unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor.clone()));
        let app = crate::replication::grpc::server::mount_service(
            axum::Router::new(),
            service,
            db.clone(),
            default_transport_policy(),
        );
        let app = mount_test_service_with_node_cert(app, "worker-1");
        // Wedge every ApplyOutbox call far longer than the client deadline,
        // simulating a response that never arrives over a lossy link.
        let app = app.layer(axum::middleware::from_fn(
            |request: axum::extract::Request, next: axum::middleware::Next| async move {
                if request.uri().path().ends_with("/ApplyOutbox") {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
                next.run(request).await
            },
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let mut client = ReplicationGrpcClient::connect(
            GrpcClientConfig {
                leader_endpoint: endpoint,
                token,
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: false,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor,
            default_transport_policy(),
        )
        .await
        .unwrap();
        client.override_unary_deadline(Duration::from_millis(800));

        let started = std::time::Instant::now();
        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            client.apply_outbox_rpc(
                "deadline-key",
                crate::kubelet::outbox::payload::OutboxOperation::PodStatus,
                bytes::Bytes::new(),
            ),
        )
        .await;

        let result = outcome.expect(
            "apply_outbox_rpc must return within the wall-clock bound (deadline must fire)",
        );
        assert!(
            matches!(result, Err(OutboxApplyError::Retryable(_))),
            "a wedged apply_outbox must surface Retryable, got {result:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "must abort near the 800ms deadline, not the 30s server wedge"
        );
        assert!(
            !client.lane_pool_present_for_test(ChannelLane::Status).await,
            "the per-call deadline must evict the wedged Status lane so the retry rebuilds"
        );
        handle.abort();
    }

    #[tokio::test]
    async fn renew_node_lease_aborts_on_per_call_deadline() {
        // bug-grpc A2: renew_node_lease is a Status-lane unary RPC with the
        // same lossy-link wedge as apply_outbox. Routed through `unary_call`,
        // a wedged call must abort at the per-call deadline, evict ONLY the
        // Status lane, and leave the Read lane's warm pool intact (lane
        // isolation).
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let token = crate::bootstrap::cluster_meta::read_join_token(db.as_ref())
            .await
            .unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor.clone()));
        let app = crate::replication::grpc::server::mount_service(
            axum::Router::new(),
            service,
            db.clone(),
            default_transport_policy(),
        );
        let app = mount_test_service_with_node_cert(app, "worker-1");
        let app = app.layer(axum::middleware::from_fn(
            |request: axum::extract::Request, next: axum::middleware::Next| async move {
                if request.uri().path().ends_with("/RenewNodeLease") {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
                next.run(request).await
            },
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let mut client = ReplicationGrpcClient::connect(
            GrpcClientConfig {
                leader_endpoint: endpoint,
                token,
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: false,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor,
            default_transport_policy(),
        )
        .await
        .unwrap();
        client.override_unary_deadline(Duration::from_millis(800));

        // Warm the Read lane with a non-wedged read so we can prove it is not
        // evicted by the Status-lane deadline.
        client.metadata().await.unwrap();
        assert!(
            client.lane_pool_present_for_test(ChannelLane::Read).await,
            "metadata must warm the Read lane"
        );

        let started = std::time::Instant::now();
        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            client.renew_node_lease_rpc("2026-06-11T00:00:00Z", 40),
        )
        .await;
        let result = outcome.expect("renew_node_lease_rpc must return within the wall-clock bound");
        assert!(
            result.is_err(),
            "a wedged renew_node_lease must surface an error, got {result:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "must abort near the 800ms deadline, not the 30s server wedge"
        );
        assert!(
            !client.lane_pool_present_for_test(ChannelLane::Status).await,
            "the per-call deadline must evict the wedged Status lane"
        );
        assert!(
            client.lane_pool_present_for_test(ChannelLane::Read).await,
            "the Status-lane deadline must NOT evict the Read lane (lane isolation)"
        );
        handle.abort();
    }

    #[tokio::test]
    async fn every_unary_rpc_is_bounded_by_per_call_deadline() {
        // bug-grpc A2 acceptance: NO unary worker→leader RPC may await a raw
        // tonic future. With every server path wedged far longer than the
        // per-call deadline, each unary RPC must still return within a
        // wall-clock bound — i.e. it routes through `unary_call`'s deadline.
        use crate::control_plane::client::{
            ListRequest, ProjectedServiceAccountTokenRequest, ResourceKey,
        };
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let token = crate::bootstrap::cluster_meta::read_join_token(db.as_ref())
            .await
            .unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor.clone()));
        let app = crate::replication::grpc::server::mount_service(
            axum::Router::new(),
            service,
            db.clone(),
            default_transport_policy(),
        );
        let app = mount_test_service_with_node_cert(app, "worker-1");
        // Wedge EVERY request path: any unary RPC that awaits a raw future
        // would hang here; only the deadline can rescue it.
        let app = app.layer(axum::middleware::from_fn(
            |request: axum::extract::Request, next: axum::middleware::Next| async move {
                tokio::time::sleep(Duration::from_secs(30)).await;
                next.run(request).await
            },
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        // Use `new` (not `connect`) so the wedged /Connect path is never hit;
        // unary RPCs build their lane channels lazily and independently.
        let mut client = ReplicationGrpcClient::new(
            GrpcClientConfig {
                leader_endpoint: endpoint,
                token,
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: false,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor,
            crate::replication::grpc::transport_policy::GrpcTransportPolicy::shared_default(),
        );
        client.override_unary_deadline(Duration::from_millis(300));

        // Each closure invokes one unary RPC; all must be bounded.
        macro_rules! assert_bounded {
            ($label:expr, $call:expr) => {{
                let outcome = tokio::time::timeout(Duration::from_secs(5), $call).await;
                assert!(
                    outcome.is_ok(),
                    "{} must be bounded by the per-call deadline, not the server wedge",
                    $label
                );
            }};
        }

        assert_bounded!("metadata", client.metadata());
        assert_bounded!("cluster_membership", client.cluster_membership());
        assert_bounded!(
            "get_resource_rpc",
            client.get_resource_rpc(ResourceKey {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "p".to_string(),
            })
        );
        assert_bounded!(
            "list_resources_rpc",
            client.list_resources_rpc(ListRequest {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: None,
                label_selector: None,
                field_selector: None,
                limit: None,
                continue_token: None,
            })
        );
        assert_bounded!(
            "projected_service_account_token_rpc",
            client.projected_service_account_token_rpc(ProjectedServiceAccountTokenRequest {
                namespace: "default".to_string(),
                service_account_name: "default".to_string(),
                audiences: vec!["api".to_string()],
                expiration_seconds: 3600,
                bound_pod_name: None,
                bound_pod_uid: None,
                bound_node_name: None,
                bound_node_uid: None,
            })
        );
        assert_bounded!(
            "join_as_controlplane_rpc",
            client.join_as_controlplane_rpc(2, "https://127.0.0.1:1", "cp2", false, "127.0.0.1")
        );
        assert_bounded!(
            "sign_controlplane_csr_rpc",
            client.sign_controlplane_csr_rpc("cp2", b"csr")
        );
        assert_bounded!(
            "renew_node_lease_rpc",
            client.renew_node_lease_rpc("2026-06-11T00:00:00Z", 40)
        );
        assert_bounded!(
            "allocate_node_subnet_rpc",
            client.allocate_node_subnet_rpc("worker-1", "10.42.0.0/16", "127.0.0.1")
        );
        assert_bounded!(
            "get_node_subnet_rpc",
            client.get_node_subnet_rpc("worker-1")
        );
        assert_bounded!(
            "list_peer_subnets_rpc",
            client.list_peer_subnets_rpc("worker-1")
        );
        assert_bounded!(
            "get_node_dataplane_rpc",
            client.get_node_dataplane_rpc("worker-1")
        );
        assert_bounded!(
            "observe_peer_endpoint_rpc",
            client.observe_peer_endpoint_rpc("worker-1")
        );
        assert_bounded!(
            "list_pod_cleanup_intents_for_node_rpc",
            client.list_pod_cleanup_intents_for_node_rpc("worker-1")
        );
        assert_bounded!(
            "delete_pod_cleanup_intent_rpc",
            client.delete_pod_cleanup_intent_rpc("worker-1", "default", "p", "uid", "gone")
        );
        assert_bounded!(
            "apply_outbox_rpc",
            client.apply_outbox_rpc(
                "k",
                crate::kubelet::outbox::payload::OutboxOperation::PodStatus,
                bytes::Bytes::new(),
            )
        );
        handle.abort();
    }

    #[tokio::test]
    async fn raft_append_entries_rpc_times_out_and_evicts_raft_lane() {
        // bug-grpc T6: the three Raft consensus RPCs (AppendEntries/Vote/
        // InstallSnapshot) used to bypass the supervised-deadline wrapper that
        // bounds every other unary worker→leader RPC. Under partial packet
        // loss the HTTP/2 keepalive PING still gets through (connection deemed
        // alive) while the RPC's response is wedged, so a follower's
        // AppendEntries could stall consensus indefinitely. Routed through
        // `raft_unary_call`, a wedged call must abort at the per-call
        // `raft_unary_deadline`, surface a deadline-exceeded error, and evict
        // ONLY the Raft lane so the next attempt rebuilds a fresh connection
        // while sibling lanes keep their warm pools.
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let token = crate::bootstrap::cluster_meta::read_join_token(db.as_ref())
            .await
            .unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor.clone()));
        let app = crate::replication::grpc::server::mount_service(
            axum::Router::new(),
            service,
            db.clone(),
            default_transport_policy(),
        );
        let app = mount_test_service_with_node_cert(app, "worker-1");
        // Wedge every RaftAppendEntries call far longer than the client
        // deadline, simulating a response that never arrives over a lossy link.
        let app = app.layer(axum::middleware::from_fn(
            |request: axum::extract::Request, next: axum::middleware::Next| async move {
                if request.uri().path().ends_with("/RaftAppendEntries") {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
                next.run(request).await
            },
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        // Use `new` (not `connect`) so the wedged /Connect path is never hit.
        let mut client = ReplicationGrpcClient::new(
            GrpcClientConfig {
                leader_endpoint: endpoint,
                token,
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: false,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor,
            crate::replication::grpc::transport_policy::GrpcTransportPolicy::shared_default(),
        );
        client.override_raft_unary_deadline(Duration::from_millis(50));

        let started = std::time::Instant::now();
        let outcome = tokio::time::timeout(
            Duration::from_secs(1),
            client.raft_append_entries_rpc(Vec::new()),
        )
        .await;

        let result = outcome.expect(
            "raft_append_entries_rpc must return within the wall-clock bound (deadline must fire)",
        );
        let message = format!("{}", result.unwrap_err());
        assert!(
            message.contains("deadline exceeded"),
            "a wedged raft_append_entries must surface a deadline-exceeded error, got: {message}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "must abort near the 50ms raft deadline, not the 30s server wedge"
        );
        assert!(
            !client.lane_pool_present_for_test(ChannelLane::Raft).await,
            "the per-call deadline must evict the wedged Raft lane so the retry rebuilds"
        );
        handle.abort();
    }

    #[tokio::test]
    async fn client_snapshot_decodes_entries() {
        let (client, _service, db, handle) = client_and_service().await;
        db.create_namespace(
            "snap-client",
            serde_json::json!({"metadata": {"name": "snap-client"}}),
        )
        .await
        .unwrap();

        let entries = client.snapshot(0).await.unwrap();
        assert!(entries.iter().any(|entry| matches!(
            entry.mutations.first(),
            Some(crate::log_apply::LogApplyMutation::PutNamespace(row)) if row.name == "snap-client"
        )));
        handle.abort();
    }

    #[tokio::test]
    async fn worker_join_does_not_persist_leader_service_account_signing_key() {
        let _env_guard = ENV_LOCK.lock().await;
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let leader_ns = format!("grpc-client-leader-{suffix}");
        let worker_ns = format!("grpc-client-worker-{suffix}");
        let leader_etc = crate::paths::etc_dir_path(&leader_ns);
        let worker_etc = crate::paths::etc_dir_path(&worker_ns);
        std::fs::create_dir_all(&leader_etc).unwrap();
        std::fs::create_dir_all(&worker_etc).unwrap();
        std::fs::write(
            leader_etc.join("service-account-signing.key"),
            "leader-sa-signing-key",
        )
        .unwrap();

        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let token = crate::bootstrap::cluster_meta::read_join_token(db.as_ref())
            .await
            .unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new_with_containerd_namespace(
            db.clone(),
            supervisor.clone(),
            leader_ns.clone(),
        ));
        let app = crate::replication::grpc::server::mount_service(
            axum::Router::new(),
            service,
            db,
            default_transport_policy(),
        );
        let app = mount_test_service_with_node_cert(app, "worker-1");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        unsafe { std::env::set_var("KLIGHTS_CONTAINERD_NAMESPACE", &worker_ns) };
        let _client = ReplicationGrpcClient::connect(
            GrpcClientConfig {
                leader_endpoint: endpoint,
                token,
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: false,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor.clone(),
            default_transport_policy(),
        )
        .await
        .unwrap();

        assert!(
            std::fs::read_to_string(worker_etc.join("service-account-signing.key")).is_err(),
            "worker join must not persist the leader ServiceAccount signing key"
        );

        handle.abort();
        let _ = supervisor.shutdown(std::time::Duration::from_secs(1)).await;
        let _ = std::fs::remove_dir_all(crate::paths::data_root_path(&leader_ns));
        let _ = std::fs::remove_dir_all(crate::paths::data_root_path(&worker_ns));
        unsafe { std::env::remove_var("KLIGHTS_CONTAINERD_NAMESPACE") };
    }

    #[tokio::test]
    async fn local_pod_log_follow_closes_on_matching_pod_deleted_event() {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let runtime_ns = format!("grpc-client-log-follow-{suffix}");
        let log_dir =
            crate::paths::pod_log_dir_path(&runtime_ns, "sonobuoy", "sonobuoy-e2e-job", "pod-uid")
                .join("e2e");
        tokio::fs::create_dir_all(&log_dir).await.unwrap();

        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let pod_event_db: crate::datastore::DatastoreHandle =
            Arc::new(crate::datastore::test_support::in_memory().await);
        pod_event_db.seed_namespace_for_test("sonobuoy").await;
        pod_event_db
            .create_resource(
                "v1",
                "Pod",
                Some("sonobuoy"),
                "sonobuoy-e2e-job",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "sonobuoy",
                        "name": "sonobuoy-e2e-job",
                        "uid": "pod-uid"
                    },
                    "status": {
                        "phase": "Running"
                    }
                }),
            )
            .await
            .unwrap();
        let handler = LocalPodLogHandler::new_with_pod_event_store(
            runtime_ns.clone(),
            supervisor.clone(),
            pod_event_db.clone(),
        );
        let mut stream = handler.follow_logs(PodLogRequest {
            request_id: "log-follow-delete".to_string(),
            node_name: "worker-1".to_string(),
            namespace: "sonobuoy".to_string(),
            pod_name: "sonobuoy-e2e-job".to_string(),
            pod_uid: "pod-uid".to_string(),
            container_name: "e2e".to_string(),
            follow: Some("true".to_string()),
            tail_lines: None,
            timestamps: None,
            since_time: None,
            since_seconds: None,
            limit_bytes: None,
            previous: None,
        });

        assert!(
            supervisor
                .timeout(
                    "test_pod_log_follow_waits",
                    Duration::from_millis(100),
                    stream.next(),
                )
                .await
                .unwrap()
                .is_err(),
            "follow stream should remain open until the pod delete event arrives"
        );

        pod_event_db
            .delete_resource("v1", "Pod", Some("sonobuoy"), "sonobuoy-e2e-job")
            .await
            .unwrap();

        let done = supervisor
            .timeout(
                "test_pod_log_follow_deleted",
                Duration::from_secs(2),
                stream.next(),
            )
            .await
            .unwrap()
            .unwrap();
        assert!(
            done.is_none(),
            "pod log follow must close after the matching pod delete event"
        );

        let _ = supervisor.shutdown(Duration::from_secs(1)).await;
        let _ = tokio::fs::remove_dir_all(crate::paths::data_root_path(&runtime_ns)).await;
    }

    struct StaticExecHandler;

    #[async_trait::async_trait]
    impl NodeExecSyncHandler for StaticExecHandler {
        async fn exec_sync(&self, request: NodeExecSyncRequest) -> NodeExecSyncResponse {
            NodeExecSyncResponse {
                request_id: request.request_id,
                stdout: b"worker-stdout\n".to_vec(),
                stderr: Vec::new(),
                exit_code: 0,
                error: None,
            }
        }
    }

    #[tokio::test]
    async fn client_replies_to_node_exec_sync_requests_on_connect_stream() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let token = crate::bootstrap::cluster_meta::read_join_token(db.as_ref())
            .await
            .unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor.clone()));
        let app = crate::replication::grpc::server::mount_service(
            axum::Router::new(),
            service.clone(),
            db,
            default_transport_policy(),
        );
        let app = mount_test_service_with_node_cert(app, "worker-1");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let client = ReplicationGrpcClient::new(
            GrpcClientConfig {
                leader_endpoint: endpoint,
                token,
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: false,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor,
            crate::replication::grpc::transport_policy::GrpcTransportPolicy::shared_default(),
        );
        client
            .set_node_exec_sync_handler(Arc::new(StaticExecHandler))
            .await;
        client.ensure_joined().await.unwrap();

        let response = service
            .request_node_exec_sync(NodeExecSyncRequest {
                request_id: String::new(),
                node_name: "worker-1".to_string(),
                namespace: "hostport-2155".to_string(),
                pod_name: "e2e-host-exec".to_string(),
                container_id: "worker-container".to_string(),
                command: vec![
                    "/bin/sh".to_string(),
                    "-c".to_string(),
                    "echo ok".to_string(),
                ],
                timeout_seconds: 300,
            })
            .await
            .unwrap();

        assert_eq!(response.stdout, b"worker-stdout\n");
        assert_eq!(response.exit_code, 0);
        handle.abort();
    }

    struct EchoExecStreamHandler;

    #[async_trait::async_trait]
    impl NodeExecStreamHandler for EchoExecStreamHandler {
        async fn exec_stream(
            &self,
            request: NodeExecRequest,
            mut input: tokio::sync::mpsc::Receiver<NodeExecStreamFrame>,
            output: tokio::sync::mpsc::Sender<NodeExecStreamFrame>,
        ) {
            while let Some(frame) = input.recv().await {
                if frame.channel == ExecStreamChannel::Stdin && !frame.data.is_empty() {
                    output
                        .send(NodeExecStreamFrame {
                            request_id: request.request_id.clone(),
                            channel: ExecStreamChannel::Stdout,
                            data: frame.data,
                            fin: false,
                        })
                        .await
                        .unwrap();
                }
                if frame.fin {
                    break;
                }
            }
            output
                .send(NodeExecStreamFrame {
                    request_id: request.request_id,
                    channel: ExecStreamChannel::Error,
                    data: serde_json::json!({"metadata": {}, "status": "Success"})
                        .to_string()
                        .into_bytes(),
                    fin: true,
                })
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn client_bridges_node_exec_stream_frames_on_connect_stream() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let token = crate::bootstrap::cluster_meta::read_join_token(db.as_ref())
            .await
            .unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor.clone()));
        let app = crate::replication::grpc::server::mount_service(
            axum::Router::new(),
            service.clone(),
            db,
            default_transport_policy(),
        );
        let app = mount_test_service_with_node_cert(app, "worker-1");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let client = ReplicationGrpcClient::new(
            GrpcClientConfig {
                leader_endpoint: endpoint,
                token,
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: false,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor.clone(),
            crate::replication::grpc::transport_policy::GrpcTransportPolicy::shared_default(),
        );
        client
            .set_node_exec_stream_handler(Arc::new(EchoExecStreamHandler))
            .await;
        client.ensure_joined().await.unwrap();

        let mut session = service
            .open_node_exec_stream(NodeExecRequest {
                request_id: String::new(),
                node_name: "worker-1".to_string(),
                namespace: "default".to_string(),
                pod_name: "remote-exec".to_string(),
                container_id: "remote-container".to_string(),
                command: vec!["/bin/sh".to_string()],
                tty: true,
                stdin: true,
                stdout: true,
                stderr: true,
            })
            .await
            .unwrap();
        session
            .send_frame(NodeExecStreamFrame {
                request_id: String::new(),
                channel: ExecStreamChannel::Stdin,
                data: b"echo hello\n".to_vec(),
                fin: false,
            })
            .await
            .unwrap();

        let echoed = supervisor
            .timeout(
                "test_node_exec_stream_echo",
                std::time::Duration::from_secs(2),
                session.recv_frame(),
            )
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .expect("echo frame should arrive");
        assert_eq!(echoed.channel, ExecStreamChannel::Stdout);
        assert_eq!(echoed.data, b"echo hello\n");

        session
            .send_frame(NodeExecStreamFrame {
                request_id: String::new(),
                channel: ExecStreamChannel::Stdin,
                data: Vec::new(),
                fin: true,
            })
            .await
            .unwrap();
        let status = supervisor
            .timeout(
                "test_node_exec_stream_status",
                std::time::Duration::from_secs(2),
                session.recv_frame(),
            )
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .expect("status frame should arrive");
        assert_eq!(status.channel, ExecStreamChannel::Error);
        assert!(status.fin);
        handle.abort();
    }

    // `client_times_out_hung_forward_response_and_clears_stream` removed
    // in T6 — it exercised the deleted ForwardCommand round-trip path.

    // --- Worker auto-rejoin: self-heal of wedged Read/Status lanes ---
    //
    // After a leader *process* restart the worker's warm channel pools
    // wedge. Only the Stream lane self-heals (`clear_stream`); the Read
    // (watch/informers) and Status (lease/outbox) lanes must also evict on
    // a transport-level error so the existing reconnect/heartbeat/dispatch
    // loops rebuild a fresh channel and the node rejoins without a restart.
    // Mirrors the raft-transport self-heal in datastore::raft::grpc_network.

    #[test]
    fn is_transport_status_classifies_connection_failures_only() {
        use tonic::Status;
        let cases: [(Status, bool); 7] = [
            (Status::unavailable("error trying to connect"), true),
            (Status::unknown("h2 protocol error: connection reset"), true),
            (Status::failed_precondition("not raft leader"), false),
            (Status::not_found("missing"), false),
            (Status::already_exists("dup"), false),
            (Status::aborted("conflict"), false),
            (Status::invalid_argument("bad request"), false),
        ];
        for (status, expected) in cases {
            assert_eq!(
                super::super::is_transport_status(&status),
                expected,
                "unexpected classification for code {:?}",
                status.code()
            );
        }
    }

    #[tokio::test]
    async fn status_lane_self_heals_after_leader_restart() {
        let _guard = ENV_LOCK.lock().await;
        let fixture = TlsGrpcLeaderFixture::start().await;
        let client = fixture
            .connect(Some(fixture.ca_cert_path.clone()), false)
            .await
            .unwrap();

        // Warm the Status lane with a successful lease renewal.
        client
            .renew_node_lease_rpc("2026-01-01T00:00:00Z", 40)
            .await
            .expect("initial lease renewal should succeed");
        assert!(
            client.lane_pool_present_for_test(ChannelLane::Status).await,
            "Status lane should be warm after a successful renewal"
        );

        // Leader restarts: tear down the server so the cached connection
        // wedges. The renewal now fails AND the wedged lane is evicted, so
        // the heartbeat loop rebuilds a fresh channel on the next attempt.
        fixture.shutdown().await;
        let result = client
            .renew_node_lease_rpc("2026-01-01T00:00:08Z", 40)
            .await;
        assert!(
            result.is_err(),
            "renewal must fail while the leader is down"
        );
        assert!(
            !client.lane_pool_present_for_test(ChannelLane::Status).await,
            "wedged Status lane must be evicted so the next renewal rebuilds"
        );
    }

    #[tokio::test]
    async fn read_lane_self_heals_after_leader_restart() {
        let _guard = ENV_LOCK.lock().await;
        let fixture = TlsGrpcLeaderFixture::start().await;
        let client = fixture
            .connect(Some(fixture.ca_cert_path.clone()), false)
            .await
            .unwrap();

        let watch_req = || crate::control_plane::client::WatchRequest {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: None,
            label_selector: None,
            field_selector: None,
            start_resource_version: None,
        };

        // Warm the Read lane by opening a watch stream. The stream itself
        // is not consumed — opening it is what builds the Read-lane pool.
        let _warm_stream = client
            .watch_resources_rpc(watch_req())
            .await
            .expect("initial watch open should succeed");
        assert!(
            client.lane_pool_present_for_test(ChannelLane::Read).await,
            "Read lane should be warm after opening a watch"
        );

        // Leader restarts: the next watch open fails and evicts the wedged
        // Read lane so the watch driver's reconnect rebuilds a fresh channel.
        fixture.shutdown().await;
        let result = client.watch_resources_rpc(watch_req()).await;
        assert!(
            result.is_err(),
            "watch open must fail while the leader is down"
        );
        assert!(
            !client.lane_pool_present_for_test(ChannelLane::Read).await,
            "wedged Read lane must be evicted so the next watch rebuilds"
        );
    }
}
