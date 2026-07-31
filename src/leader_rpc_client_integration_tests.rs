//! gRPC client tests.

mod cases {

    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::api::pod_subresources::local_node_log_runtime::LocalNodeLogRuntime;

    use crate::datastore::backend::DatastoreHandle;

    use klights_replication::ReplicationService;

    use futures::StreamExt as _;

    use klights_cluster_core::command::{
        COMMAND_CODEC_VERSION, CommandId, CommandMeta, StorageCommand,
    };

    use klights_cluster_core::{ReplicationEntry, StreamItem};
    use klights_leader_api::JoinRole;

    use klights_leader_api::{OutboxDeliveryOperation, OutboxDeliveryRequest};

    use klights_leader_rpc::client::GrpcChannelLane as ChannelLane;
    use klights_leader_rpc::client::{
        GrpcClientConfig, JoinDataplaneMetadata, NodeControlRuntimes, NodeExecCapability,
        NodeLogCapability, NodeMetricsCapability, ReplicationGrpcClient,
    };

    use klights_node_api::{
        ExecStreamChannel, ExecStreamOptions, NodeExec, NodeExecFrame, NodeExecRequest,
        NodeExecRuntimeFuture, NodeExecSession, NodeExecSyncRequest, NodeExecSyncResult,
        NodeExecTarget, NodeLogOptions, NodeLogRequest, NodeLogTarget, NodeMetrics,
        NodeMetricsFuture, NodeMetricsRequest, NodeMetricsRuntime, NodeMetricsTarget,
    };
    use klights_node_api::{
        NodeExecRuntime, NodeLogRuntime, NodeMetricsContainerSample, NodeMetricsNodeSample,
        NodeMetricsPodSample, NodeMetricsResult,
    };
    use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};
    use tokio_util::sync::CancellationToken;

    static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn unavailable_runtimes() -> NodeControlRuntimes {
        NodeControlRuntimes::new(
            NodeExecCapability::Unavailable,
            NodeLogCapability::Unavailable,
            NodeMetricsCapability::Unavailable,
        )
    }

    fn raft_receiver() -> klights_leader_rpc::raft_rpc::RaftReceiverAdmission {
        klights_leader_rpc::raft_rpc::RaftReceiverAdmission {
            addr: "test".to_string(),
            storage_incarnation: uuid::Uuid::nil().to_string(),
            admitted_log: None,
        }
    }

    fn dataplane() -> JoinDataplaneMetadata {
        JoinDataplaneMetadata {
            public_key: None,
            endpoint: "127.0.0.1".to_string(),
            port: None,
            mode: klights_leader_api::NetworkNodeMode::Root,
            encryption: klights_leader_api::DataplaneEncryption::Direct,
        }
    }

    fn outbox_delivery_request(
        key: impl Into<String>,
        payload: bytes::Bytes,
        stream_id: i64,
        stream_seq: i64,
    ) -> OutboxDeliveryRequest {
        let payload = if payload.is_empty() {
            Arc::<[u8]>::from(&b"test"[..])
        } else {
            Arc::<[u8]>::from(payload.to_vec())
        };
        OutboxDeliveryRequest::try_new(
            key,
            OutboxDeliveryOperation::PodStatus,
            payload,
            "client",
            stream_id,
            stream_seq,
        )
        .expect("valid delivery request")
    }

    fn default_transport_policy() -> klights_leader_rpc::transport_policy::SharedGrpcTransportPolicy
    {
        klights_leader_rpc::transport_policy::GrpcTransportPolicy::shared_default()
    }

    fn unary_deadline_policy(
        deadline: Duration,
    ) -> klights_leader_rpc::transport_policy::SharedGrpcTransportPolicy {
        let mut policy = klights_leader_rpc::transport_policy::GrpcTransportPolicy::default();
        policy.unary_deadline = deadline;
        policy.shared()
    }

    fn raft_deadline_policy(
        deadline: Duration,
    ) -> klights_leader_rpc::transport_policy::SharedGrpcTransportPolicy {
        let mut policy = klights_leader_rpc::transport_policy::GrpcTransportPolicy::default();
        policy.raft_unary_deadline = deadline;
        policy.shared()
    }

    fn current_renew_time_for_test() -> String {
        chrono::Utc::now().to_rfc3339()
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
            let concrete_db = crate::datastore::test_support::in_memory().await;
            let passive_reads =
                crate::datastore::test_support::sqlite_passive_read_ports(&concrete_db);
            let db: DatastoreHandle = Arc::new(concrete_db);
            crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
                .await
                .unwrap();
            let token = crate::bootstrap::cluster_meta::read_join_token(db.as_ref())
                .await
                .unwrap();
            let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
            let service = Arc::new(crate::grpc_test_support::replication_service(
                db.clone(),
                supervisor.clone(),
            ));
            let app = crate::grpc_test_support::mount_service_with_passive_reads(
                axum::Router::new(),
                service,
                db,
                passive_reads,
                default_transport_policy(),
            );
            let addr = reserve_loopback_addr();
            let endpoint = format!("https://localhost:{}", addr.port());
            let shutdown = CancellationToken::new();
            let server_shutdown = shutdown.clone();
            let server_supervisor = supervisor.clone();
            let server_namespace = namespace.clone();
            let server_data_root = crate::paths::data_root_path(&server_namespace);
            let handle = tokio::spawn(async move {
                crate::bootstrap::init::tls::serve_https(
                    app,
                    &addr.to_string(),
                    &server_data_root,
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
                unavailable_runtimes(),
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
        app.layer(axum::Extension(klights_types::TlsClientCertificate(
            test_node_client_cert_der(node_name),
        )))
    }

    async fn grpc_watch_gate_server(
        is_leader: bool,
    ) -> (
        String,
        DatastoreHandle,
        tokio::sync::watch::Sender<bool>,
        tokio::task::JoinHandle<()>,
    ) {
        grpc_watch_gate_server_with_policy(is_leader, default_transport_policy()).await
    }

    async fn grpc_watch_gate_server_with_policy(
        is_leader: bool,
        policy: Arc<klights_leader_rpc::transport_policy::GrpcTransportPolicy>,
    ) -> (
        String,
        DatastoreHandle,
        tokio::sync::watch::Sender<bool>,
        tokio::task::JoinHandle<()>,
    ) {
        let concrete_db = crate::datastore::test_support::in_memory().await;
        let passive_reads = crate::datastore::test_support::sqlite_passive_read_ports(&concrete_db);
        let db: DatastoreHandle = Arc::new(concrete_db);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(crate::grpc_test_support::replication_service(
            db.clone(),
            supervisor,
        ));
        let (leader_tx, leader_rx) = tokio::sync::watch::channel(is_leader);
        let app = crate::grpc_test_support::mount_service_with_passive_reads_and_leader_gate(
            axum::Router::new(),
            service,
            db.clone(),
            passive_reads,
            leader_rx,
            policy,
        );
        let app = mount_test_service_with_node_cert(app, "worker-1");
        let (endpoint, handle) = crate::grpc_test_support::serve_tls_test_app(app).await;
        (endpoint, db, leader_tx, handle)
    }

    async fn grpc_hanging_watch_server() -> (String, tokio::task::JoinHandle<()>) {
        async fn hang_watch_open(
            request: axum::extract::Request,
            next: axum::middleware::Next,
        ) -> axum::response::Response {
            if request.uri().path() == "/klights.replication.Replication/WatchResources" {
                return futures::future::pending::<axum::response::Response>().await;
            }
            next.run(request).await
        }

        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(crate::grpc_test_support::replication_service(
            db.clone(),
            supervisor,
        ));
        let (_leader_tx, leader_rx) = tokio::sync::watch::channel(true);
        let app = crate::grpc_test_support::mount_service_full(
            axum::Router::new(),
            service,
            db,
            None,
            None,
            None,
            None,
            "",
            Some(leader_rx),
            None,
            None,
            None,
            None,
            default_transport_policy(),
        )
        .layer(axum::middleware::from_fn(hang_watch_open));
        let app = mount_test_service_with_node_cert(app, "worker-1");
        let (endpoint, handle) = crate::grpc_test_support::serve_tls_test_app(app).await;
        (endpoint, handle)
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
        let service = Arc::new(crate::grpc_test_support::replication_service(
            db.clone(),
            supervisor.clone(),
        ));
        let app = crate::grpc_test_support::mount_service(
            axum::Router::new(),
            service,
            db,
            default_transport_policy(),
        );
        let app = mount_test_service_with_node_cert(app, "worker-1");
        let (endpoint, handle) = crate::grpc_test_support::serve_tls_test_app(app).await;
        let endpoint = endpoint.replace("127.0.0.1", "localhost");

        let client = ReplicationGrpcClient::connect(
            GrpcClientConfig {
                leader_endpoint: endpoint,
                token,
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: true,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor.clone(),
            default_transport_policy(),
            unavailable_runtimes(),
        )
        .await
        .unwrap();

        assert_eq!(
            client.observed_leader_endpoint().as_deref(),
            Some("127.0.0.1"),
            "hostname leader endpoints must report the actual connected peer IP"
        );
        handle.abort();
        let _ = supervisor.shutdown(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn watch_open_tries_next_endpoint_after_not_raft_leader() {
        let _guard = ENV_LOCK.lock().await;
        let (stale_endpoint, _stale_db, _stale_leader_tx, stale_handle) =
            grpc_watch_gate_server(false).await;
        let (leader_endpoint, leader_db, _leader_tx, leader_handle) =
            grpc_watch_gate_server(true).await;
        leader_db
            .create_namespace(
                "default",
                serde_json::json!({"metadata": {"name": "default"}}),
            )
            .await
            .expect("create namespace");
        let token = crate::bootstrap::cluster_meta::read_join_token(leader_db.as_ref())
            .await
            .expect("read token");
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let client = ReplicationGrpcClient::new(
            GrpcClientConfig {
                leader_endpoint: stale_endpoint.clone(),
                token,
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: true,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor,
            default_transport_policy(),
        );
        client.set_all_leader_endpoints(vec![stale_endpoint.clone(), leader_endpoint.clone()]);

        let mut stream = client
            .watch_resources_rpc(
                klights_leader_api::WatchRequest::try_new(
                    "v1",
                    "Pod",
                    None,
                    None,
                    Some("spec.nodeName=worker-1".to_string()),
                    None,
                    None,
                )
                .expect("valid Pod watch"),
            )
            .await
            .expect("watch open should fail over to the current leader");
        assert_eq!(
            client.current_leader_endpoint(),
            leader_endpoint,
            "successful watch open must pin the client to the endpoint that accepted it"
        );

        leader_db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "scheduled",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "scheduled",
                        "uid": "uid-scheduled"
                    },
                    "spec": {
                        "nodeName": "worker-1",
                        "containers": [{"name": "app", "image": "busybox"}]
                    },
                    "status": {"phase": "Pending"}
                }),
            )
            .await
            .expect("create pod");

        let event = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("watch should receive leader events")
            .expect("watch stream should stay open")
            .expect("watch event should decode");
        assert_eq!(
            event.event_type(),
            klights_leader_api::WatchEventType::Added
        );
        assert_eq!(
            event
                .resource()
                .data
                .pointer("/metadata/name")
                .and_then(|v| v.as_str()),
            Some("scheduled")
        );

        stale_handle.abort();
        leader_handle.abort();
    }

    #[tokio::test]
    async fn fresh_watch_without_resource_version_starts_after_compacted_history() {
        let _guard = ENV_LOCK.lock().await;
        let (endpoint, db, _leader_tx, handle) = grpc_watch_gate_server(true).await;
        db.create_namespace(
            "default",
            serde_json::json!({"metadata": {"name": "default"}}),
        )
        .await
        .expect("create namespace");
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "compacted",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"namespace": "default", "name": "compacted"},
                "data": {"key": "old"}
            }),
        )
        .await
        .expect("create compacted configmap");
        db.gc_watch_events(1, 1000)
            .await
            .expect("compact durable watch history");

        let token = crate::bootstrap::cluster_meta::read_join_token(db.as_ref())
            .await
            .expect("read token");
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let client = ReplicationGrpcClient::new(
            GrpcClientConfig {
                leader_endpoint: endpoint,
                token,
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: true,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor,
            default_transport_policy(),
        );
        let mut stream = client
            .watch_resources_rpc(
                klights_leader_api::WatchRequest::try_new(
                    "v1",
                    "ConfigMap",
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .expect("valid fresh watch"),
            )
            .await
            .expect("fresh watch must open after history compaction");
        assert_eq!(
            stream.accepted_cursor(),
            None,
            "remote transport must not claim the server-sampled cursor before an event or heartbeat delivers it"
        );

        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "future",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"namespace": "default", "name": "future"},
                "data": {"key": "new"}
            }),
        )
        .await
        .expect("create future configmap");

        let event = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("fresh watch must deliver a future event")
            .expect("fresh watch stream must remain open")
            .expect("fresh watch must not replay from compacted RV 0");
        assert_eq!(
            event
                .resource()
                .data
                .pointer("/metadata/name")
                .and_then(serde_json::Value::as_str),
            Some("future")
        );

        handle.abort();
    }

    #[tokio::test]
    async fn fresh_remote_watch_decodes_the_server_sampled_heartbeat_cursor() {
        let _guard = ENV_LOCK.lock().await;
        let policy = klights_leader_rpc::transport_policy::GrpcTransportPolicy {
            watch_heartbeat_interval: Duration::from_millis(100),
            ..Default::default()
        }
        .shared();
        let (endpoint, db, _leader_tx, handle) =
            grpc_watch_gate_server_with_policy(true, policy).await;
        let namespace = db
            .create_namespace(
                "anchor",
                serde_json::json!({"metadata": {"name": "anchor"}}),
            )
            .await
            .expect("create anchor namespace");
        let token = crate::bootstrap::cluster_meta::read_join_token(db.as_ref())
            .await
            .expect("read token");
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let client = ReplicationGrpcClient::new(
            GrpcClientConfig {
                leader_endpoint: endpoint,
                token,
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: true,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor,
            default_transport_policy(),
        );
        let mut stream = client
            .watch_resources_rpc(
                klights_leader_api::WatchRequest::try_new(
                    "v1",
                    "ConfigMap",
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .expect("valid fresh watch"),
            )
            .await
            .expect("fresh remote watch");
        assert_eq!(stream.accepted_cursor(), None);

        let heartbeat = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("fresh remote watch must receive a heartbeat")
            .expect("fresh remote watch remains open")
            .expect("heartbeat decodes");
        handle.abort();

        assert_eq!(
            heartbeat.event_type(),
            klights_leader_api::WatchEventType::Bookmark
        );
        assert_eq!(
            heartbeat.resource().resource_version,
            namespace.resource_version
        );
        let position = heartbeat
            .resume_position()
            .expect("decoded heartbeat carries exact event-ID position");
        assert_eq!(position.resource_version, namespace.resource_version);
        assert!(position.event_id > 0);
    }

    #[tokio::test]
    async fn watch_open_does_not_commit_failed_endpoint_candidate() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let original = "http://127.0.0.1:1".to_string();
        let failed_candidate = "http://127.0.0.1:2".to_string();
        let mut policy = *default_transport_policy();
        policy.connect_timeout = Duration::from_millis(50);
        let client = ReplicationGrpcClient::new(
            GrpcClientConfig {
                leader_endpoint: original.clone(),
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
            Arc::new(policy),
        );
        client.set_all_leader_endpoints(vec![original.clone(), failed_candidate]);

        let result = client
            .watch_resources_rpc(
                klights_leader_api::WatchRequest::try_new(
                    "v1",
                    "ConfigMap",
                    None,
                    None,
                    None,
                    Some(41),
                    None,
                )
                .expect("valid ConfigMap watch"),
            )
            .await;
        assert!(
            result.is_err(),
            "all unavailable candidates must fail watch open"
        );

        assert_eq!(
            client.current_leader_endpoint(),
            original,
            "candidate probing must not replace the accepted leader hint"
        );
    }

    #[tokio::test]
    async fn watch_open_has_a_supervised_response_header_deadline() {
        let (endpoint, handle) = grpc_hanging_watch_server().await;
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let mut policy = *default_transport_policy();
        policy.stream_open_deadline = Duration::from_millis(80);
        let client = ReplicationGrpcClient::new(
            GrpcClientConfig {
                leader_endpoint: endpoint,
                token: "abcdef.0123456789abcdef".to_string(),
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: true,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor,
            Arc::new(policy),
        );

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            client.watch_resources_rpc(
                klights_leader_api::WatchRequest::try_new(
                    "v1",
                    "ConfigMap",
                    None,
                    None,
                    None,
                    Some(41),
                    None,
                )
                .expect("valid ConfigMap watch"),
            ),
        )
        .await
        .expect("watch open must be bounded by the transport policy");
        let error = match result {
            Ok(_) => panic!("a server that never opens the stream must time out"),
            Err(error) => error,
        };

        assert!(
            format!("{error:#}").contains("deadline exceeded"),
            "deadline failure should remain diagnosable: {error:#}"
        );
        handle.abort();
    }

    #[tokio::test]
    async fn watch_open_failover_preserves_stream_out_of_range_for_relist() {
        let _guard = ENV_LOCK.lock().await;
        let (stale_endpoint, _stale_db, _stale_leader_tx, stale_handle) =
            grpc_watch_gate_server(false).await;
        let (leader_endpoint, leader_db, _leader_tx, leader_handle) =
            grpc_watch_gate_server(true).await;
        leader_db
            .create_namespace(
                "default",
                serde_json::json!({"metadata": {"name": "default"}}),
            )
            .await
            .expect("create namespace");
        let first = leader_db
            .create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "resume-old",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"namespace": "default", "name": "resume-old"},
                    "data": {"key": "old"}
                }),
            )
            .await
            .expect("create old configmap");
        leader_db
            .create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "resume-new",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"namespace": "default", "name": "resume-new"},
                    "data": {"key": "new"}
                }),
            )
            .await
            .expect("create new configmap");
        let resume_rv = (first.resource_version - 1).max(1);
        leader_db
            .gc_watch_events(1, 1000)
            .await
            .expect("trim durable watch window");

        let token = crate::bootstrap::cluster_meta::read_join_token(leader_db.as_ref())
            .await
            .expect("read token");
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let client = ReplicationGrpcClient::new(
            GrpcClientConfig {
                leader_endpoint: stale_endpoint.clone(),
                token,
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: true,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor,
            default_transport_policy(),
        );
        client.set_all_leader_endpoints(vec![stale_endpoint.clone(), leader_endpoint.clone()]);

        let mut stream = client
            .watch_resources_rpc(
                klights_leader_api::WatchRequest::try_new(
                    "v1",
                    "ConfigMap",
                    None,
                    None,
                    None,
                    Some(resume_rv),
                    None,
                )
                .expect("valid replay watch"),
            )
            .await
            .expect("watch open should fail over to the current leader");
        assert_eq!(
            client.current_leader_endpoint(),
            leader_endpoint,
            "successful watch open must pin the accepted leader endpoint"
        );

        let err = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("expired replay should produce a stream item")
            .expect("watch stream should yield")
            .expect_err("expired replay must surface as a stream error");
        assert!(matches!(
            err,
            klights_leader_api::LeaderWatchError::ReplayExpired { .. }
        ));

        stale_handle.abort();
        leader_handle.abort();
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
            klights_leader_rpc::transport_policy::GrpcTransportPolicy::shared_default(),
        );

        assert_eq!(client.observed_leader_endpoint(), None);
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
            unavailable_runtimes(),
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
        let service = Arc::new(crate::grpc_test_support::replication_service(
            db.clone(),
            supervisor.clone(),
        ));
        let controller_dispatcher = Arc::new(crate::controllers::ControllerDispatcher::default());
        let app = crate::grpc_test_support::mount_service_with_controller_dispatcher(
            axum::Router::new(),
            service.clone(),
            db.clone(),
            Some(controller_dispatcher),
            None,
            default_transport_policy(),
        );
        let app = mount_test_service_with_node_cert(app, "worker-1");
        let (endpoint, handle) = crate::grpc_test_support::serve_tls_test_app(app).await;
        let client = ReplicationGrpcClient::connect(
            GrpcClientConfig {
                leader_endpoint: endpoint,
                token,
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: true,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor,
            default_transport_policy(),
            unavailable_runtimes(),
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
                command_id: CommandId(format!("grpc-client-stream-{rv}")),
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
        client
            .ensure_joined_with_runtimes(unavailable_runtimes())
            .await
            .unwrap();
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
        use klights_leader_api::{
            LeaderOutboxDelivery, OutboxDeliveryOperation, OutboxDeliveryRequest,
        };
        let (client, _service, _db, handle) = client_and_service().await;

        // First status RPC builds the Status-lane pool.
        let _ = client
            .deliver_outbox(
                OutboxDeliveryRequest::try_new(
                    "status-key-0",
                    OutboxDeliveryOperation::PodStatus,
                    Arc::<[u8]>::from(&b"test"[..]),
                    "client",
                    1,
                    1,
                )
                .expect("valid delivery request"),
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
                .deliver_outbox(
                    OutboxDeliveryRequest::try_new(
                        format!("status-key-{i}"),
                        OutboxDeliveryOperation::PodStatus,
                        Arc::<[u8]>::from(&b"test"[..]),
                        "client",
                        1,
                        i,
                    )
                    .expect("valid delivery request"),
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
        // Watermarked end-to-end lost-response dedupe: the leader commits an
        // outbox mutation and advances the worker stream watermark, but the
        // response is dropped on the wire (lossy worker->leader link). The
        // dispatcher retries the SAME stream entry. The leader must replay it
        // as AlreadyApplied from the watermark — mutation applied exactly once,
        // never a second mutation.
        use crate::datastore::ResourcePreconditions;
        use crate::node_outbox::payload::OutboxPayload;
        use klights_leader_api::OutboxDeliveryResult as OutboxApplyResult;

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
            .apply_outbox_rpc(outbox_delivery_request(key, payload.clone(), 1, 1))
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
            .apply_outbox_rpc(outbox_delivery_request(key, payload, 1, 1))
            .await
            .expect("lost-response retry must succeed");
        assert!(
            matches!(second, OutboxApplyResult::AlreadyApplied { .. }),
            "lost-response retry must be AlreadyApplied, got {second:?}"
        );

        // Mutation applied exactly once: no new RV. Watermarked outbox entries
        // dedupe by stream watermark rather than the idempotency ledger.
        assert_eq!(
            db.get_current_resource_version().await.unwrap(),
            rv_after_first,
            "duplicate apply must not allocate another RV"
        );
        assert_eq!(
            db.list_outbox_stream_watermarks().await.unwrap(),
            vec![klights_cluster_core::OutboxStreamWatermark {
                client_id: "client".to_string(),
                stream_id: 1,
                stream_seq: 1,
            }]
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
        use klights_leader_api::OutboxDeliveryError as OutboxApplyError;
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let token = crate::bootstrap::cluster_meta::read_join_token(db.as_ref())
            .await
            .unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(crate::grpc_test_support::replication_service(
            db.clone(),
            supervisor.clone(),
        ));
        let app = crate::grpc_test_support::mount_service(
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
        let (endpoint, handle) = crate::grpc_test_support::serve_tls_test_app(app).await;

        let client = ReplicationGrpcClient::connect(
            GrpcClientConfig {
                leader_endpoint: endpoint,
                token,
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: true,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor,
            unary_deadline_policy(Duration::from_millis(800)),
            unavailable_runtimes(),
        )
        .await
        .unwrap();
        let started = std::time::Instant::now();
        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            client.apply_outbox_rpc(outbox_delivery_request(
                "deadline-key",
                bytes::Bytes::new(),
                1,
                1,
            )),
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
            !client.lane_pool_present(ChannelLane::Status).await,
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
        let service = Arc::new(crate::grpc_test_support::replication_service(
            db.clone(),
            supervisor.clone(),
        ));
        let app = crate::grpc_test_support::mount_service(
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
        let (endpoint, handle) = crate::grpc_test_support::serve_tls_test_app(app).await;

        let client = ReplicationGrpcClient::connect(
            GrpcClientConfig {
                leader_endpoint: endpoint,
                token,
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: true,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor,
            unary_deadline_policy(Duration::from_millis(800)),
            unavailable_runtimes(),
        )
        .await
        .unwrap();
        // Warm the Read lane with a non-wedged read so we can prove it is not
        // evicted by the Status-lane deadline.
        client.metadata().await.unwrap();
        assert!(
            client.lane_pool_present(ChannelLane::Read).await,
            "metadata must warm the Read lane"
        );

        let started = std::time::Instant::now();
        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            client.renew_node_lease_rpc(&current_renew_time_for_test(), 40),
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
            !client.lane_pool_present(ChannelLane::Status).await,
            "the per-call deadline must evict the wedged Status lane"
        );
        assert!(
            client.lane_pool_present(ChannelLane::Read).await,
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
        use klights_leader_api::ProjectedServiceAccountTokenRequest;
        use klights_types::ResourceKey;
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let token = crate::bootstrap::cluster_meta::read_join_token(db.as_ref())
            .await
            .unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(crate::grpc_test_support::replication_service(
            db.clone(),
            supervisor.clone(),
        ));
        let app = crate::grpc_test_support::mount_service(
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
        let (endpoint, handle) = crate::grpc_test_support::serve_tls_test_app(app).await;

        // Use `new` (not `connect`) so the wedged /Connect path is never hit;
        // unary RPCs build their lane channels lazily and independently.
        let client = ReplicationGrpcClient::new(
            GrpcClientConfig {
                leader_endpoint: endpoint,
                token,
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: true,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor,
            unary_deadline_policy(Duration::from_millis(300)),
        );

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
            client.list_resources_rpc(
                klights_leader_api::ResourceListRequest::try_new(
                    "v1",
                    "Pod",
                    None,
                    None,
                    None,
                    None,
                    None,
                    klights_leader_api::ResourceQueryConsistency::LeaderFresh,
                )
                .expect("valid list request"),
            )
        );
        assert_bounded!(
            "projected_service_account_token_rpc",
            client.projected_service_account_token_rpc(
                ProjectedServiceAccountTokenRequest::try_new(
                    "default",
                    "default",
                    vec!["api".to_string()],
                    3600,
                    "p",
                    "uid",
                    "worker-1",
                    None,
                )
                .unwrap()
            )
        );
        let profile = crate::kubelet::node_config::NodeRegistrationProfile::new(
            klights_network_api::NodePeerMode::Root,
            crate::kubelet::node_config::KubeletNodeRole::Controlplane { as_learner: false },
            true,
            klights_types::BuildIdentity::new("v1.34.6+klights-test", "test-commit"),
        );
        let controlplane_registration =
            crate::kubelet::node::NodeRegistrationSnapshot::capture_local(
                &crate::kubelet::file_blocking::test_file_process_executor(),
                "cp2",
                &profile,
                crate::kubelet::node::NodeRegistrationAddresses::new("127.0.0.1".to_string(), None),
                None,
                Some(7679),
            )
            .await;
        let controlplane_registration = klights_leader_api::ControlplaneJoinRegistrationSnapshot {
            node_name: controlplane_registration.node_name.clone(),
            node_internal_ip: controlplane_registration
                .addresses
                .internal_ip()
                .to_string(),
            as_learner: false,
            storage_incarnation: "00000000-0000-4000-8000-000000000002".to_string(),
            storage_log_attestation: klights_leader_api::RaftStorageAttestation {
                high_watermark: None,
                current_boundary: None,
            },
            snapshot: klights_leader_api::RemoteNodeRegistrationSnapshot {
                node_mode: match controlplane_registration.node_mode {
                    klights_network_api::NodePeerMode::Root => {
                        klights_leader_api::RemoteNodeMode::Root
                    }
                    klights_network_api::NodePeerMode::Rootless => {
                        klights_leader_api::RemoteNodeMode::Rootless
                    }
                },
                host: klights_leader_api::RemoteNodeHostFacts {
                    cpu_count: controlplane_registration.host.cpu_count,
                    memory_ki: controlplane_registration.host.memory_ki,
                    architecture: controlplane_registration.host.architecture,
                    operating_system: controlplane_registration.host.operating_system,
                    os_image: controlplane_registration.host.os_image,
                    kernel_version: controlplane_registration.host.kernel_version,
                    container_runtime_version: controlplane_registration
                        .host
                        .container_runtime_version,
                    kubelet_version: controlplane_registration.host.kubelet_version,
                    git_commit: controlplane_registration.host.git_commit,
                },
            },
        };
        assert_bounded!(
            "join_as_controlplane_rpc",
            client.join_as_controlplane_rpc(2, "https://127.0.0.1:1", &controlplane_registration,)
        );
        assert_bounded!(
            "sign_controlplane_csr_rpc",
            client.sign_controlplane_csr_rpc("cp2", b"csr")
        );
        assert_bounded!(
            "renew_node_lease_rpc",
            client.renew_node_lease_rpc(&current_renew_time_for_test(), 40)
        );
        assert_bounded!(
            "allocate_node_subnet_rpc",
            client.allocate_node_subnet_rpc(
                klights_leader_api::NodeSubnetAllocationRequest::try_new(
                    "worker-1",
                    "10.42.0.0/16",
                    "127.0.0.1",
                )
                .expect("valid request"),
            )
        );
        assert_bounded!(
            "get_node_subnet_rpc",
            client.get_node_subnet_rpc(
                klights_leader_api::NodeSubnetQuery::try_new("worker-1").expect("valid query"),
            )
        );
        assert_bounded!(
            "list_peer_subnets_rpc",
            client.list_peer_subnets_rpc(
                klights_leader_api::PeerSubnetsQuery::try_new("worker-1").expect("valid query"),
            )
        );
        assert_bounded!(
            "get_node_dataplane_rpc",
            client.get_node_dataplane_rpc(
                klights_leader_api::NodeDataplaneQuery::try_new("worker-1").expect("valid query"),
            )
        );
        assert_bounded!(
            "observe_peer_endpoint_rpc",
            client.observe_peer_endpoint_rpc("worker-1")
        );
        assert_bounded!(
            "list_pod_cleanup_intents_for_node_rpc",
            client.list_pod_cleanup_intents_for_node_rpc(
                klights_leader_api::PodCleanupIntentListRequest::try_new("worker-1").unwrap()
            )
        );
        assert_bounded!(
            "delete_pod_cleanup_intent_rpc",
            client.delete_pod_cleanup_intent_rpc(
                klights_leader_api::PodCleanupIntentAckRequest::try_new(
                    "worker-1", "default", "p", "uid", "gone"
                )
                .unwrap()
            )
        );
        assert_bounded!(
            "apply_outbox_rpc",
            client.apply_outbox_rpc(outbox_delivery_request("k", bytes::Bytes::new(), 1, 1,))
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
        let service = Arc::new(crate::grpc_test_support::replication_service(
            db.clone(),
            supervisor.clone(),
        ));
        let app = crate::grpc_test_support::mount_service(
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
        let (endpoint, handle) = crate::grpc_test_support::serve_tls_test_app(app).await;

        // Use `new` (not `connect`) so the wedged /Connect path is never hit.
        let client = ReplicationGrpcClient::new(
            GrpcClientConfig {
                leader_endpoint: endpoint,
                token,
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: true,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor,
            raft_deadline_policy(Duration::from_millis(50)),
        );

        let started = std::time::Instant::now();
        let outcome = tokio::time::timeout(
            Duration::from_secs(1),
            client.raft_append_entries_rpc(raft_receiver(), Vec::new()),
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
            !client.lane_pool_present(ChannelLane::Raft).await,
            "the per-call deadline must evict the wedged Raft lane so the retry rebuilds"
        );
        handle.abort();
    }

    /// T3: InstallSnapshot must use its own channel lane, NOT the Raft lane, so
    /// a stalled multi-chunk snapshot transfer cannot head-of-line-block
    /// heartbeats/AppendEntries multiplexed over the same connection under loss.
    /// Driving one install_snapshot RPC must materialize the InstallSnapshot
    /// lane pool and leave the Raft lane untouched.

    #[tokio::test]
    async fn install_snapshot_uses_a_lane_separate_from_append_entries() {
        use klights_leader_rpc::client::GrpcChannelLane as ChannelLane;
        // wedge nothing for IS (empty path never matches), just drive one call
        // through the client to materialize the lane pool.
        let (client, handle) = raft_timeout_client("/never-wedges").await;
        let _ = client
            .raft_install_snapshot_rpc(raft_receiver(), Vec::new())
            .await;
        assert!(
            client.lane_pool_present(ChannelLane::InstallSnapshot).await,
            "install_snapshot must use its own InstallSnapshot lane"
        );
        assert!(
            !client.lane_pool_present(ChannelLane::Raft).await,
            "install_snapshot must NOT touch the Raft (AppendEntries/Vote) lane"
        );
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
        let service = Arc::new(crate::grpc_test_support::replication_service(
            db.clone(),
            supervisor.clone(),
        ));
        let app = crate::grpc_test_support::mount_service(
            axum::Router::new(),
            service,
            db,
            default_transport_policy(),
        );
        let app = mount_test_service_with_node_cert(app, "worker-1");
        let (endpoint, handle) = crate::grpc_test_support::serve_tls_test_app(app).await;

        unsafe { std::env::set_var("KLIGHTS_CONTAINERD_NAMESPACE", &worker_ns) };
        let _client = ReplicationGrpcClient::connect(
            GrpcClientConfig {
                leader_endpoint: endpoint,
                token,
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: true,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor.clone(),
            default_transport_policy(),
            unavailable_runtimes(),
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
    async fn local_node_log_runtime_previous_is_empty_for_finite_and_follow() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let runtime = LocalNodeLogRuntime::new("previous-log-test".to_string(), supervisor.clone());
        let target =
            NodeLogTarget::try_new("worker-1", "default", "logger", "pod-uid", "main").unwrap();
        let request = NodeLogRequest::new(
            target,
            NodeLogOptions::new(
                Some("true".to_string()),
                None,
                None,
                None,
                None,
                None,
                Some("true".to_string()),
            ),
        );

        let finite = runtime.read_logs(request.clone()).await.unwrap();
        assert!(finite.content().is_empty());
        assert!(finite.terminal_error().is_none());

        let follow = runtime.open_logs(request).await.unwrap();
        let terminal = follow.recv_frame().await.unwrap().unwrap();
        assert!(terminal.content().is_empty());
        assert!(terminal.is_terminal());
        assert!(terminal.terminal_error().is_none());

        let _ = supervisor.shutdown(Duration::from_secs(1)).await;
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
        let concrete_pod_event_db = crate::datastore::test_support::in_memory().await;
        let passive_reads =
            crate::datastore::test_support::sqlite_passive_read_ports(&concrete_pod_event_db);
        let pod_event_db: crate::datastore::DatastoreHandle = Arc::new(concrete_pod_event_db);
        let positioned_watch =
            crate::positioned_watch_adapter::for_test(&passive_reads, pod_event_db.clone());
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
        let handler = LocalNodeLogRuntime::new_with_pod_event_store(
            crate::paths::pod_logs_root_path(&runtime_ns),
            supervisor.clone(),
            Arc::new(crate::auth::clock::SystemClock),
            crate::api::pod_subresources::logs::PodLogFollowWatchSource::new(Arc::new(
                crate::bootstrap::kubelet_ports::DatastorePodWatchSource::new(Arc::new(
                    positioned_watch,
                )),
            )),
        );
        let target =
            NodeLogTarget::try_new("worker-1", "sonobuoy", "sonobuoy-e2e-job", "pod-uid", "e2e")
                .unwrap();
        let stream = handler
            .open_logs(NodeLogRequest::new(
                target,
                NodeLogOptions::new(Some("true".to_string()), None, None, None, None, None, None),
            ))
            .await
            .unwrap();

        assert!(
            supervisor
                .timeout(
                    "test_pod_log_follow_waits",
                    Duration::from_millis(100),
                    stream.recv_frame(),
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
                stream.recv_frame(),
            )
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            done.is_some_and(|event| event.is_terminal()),
            "pod log follow must emit a terminal event after the matching pod delete event"
        );

        let _ = supervisor.shutdown(Duration::from_secs(1)).await;
        let _ = tokio::fs::remove_dir_all(crate::paths::data_root_path(&runtime_ns)).await;
    }

    struct StaticExecHandler;

    impl NodeExecRuntime for StaticExecHandler {
        fn exec_sync(
            &self,
            request: NodeExecSyncRequest,
        ) -> NodeExecRuntimeFuture<'_, NodeExecSyncResult> {
            Box::pin(async move {
                let _ = request;
                NodeExecSyncResult::success(b"worker-stdout\n".to_vec(), Vec::new(), 0)
            })
        }

        fn exec_stream(
            &self,
            request: NodeExecRequest,
            session: Box<dyn NodeExecSession>,
        ) -> NodeExecRuntimeFuture<'_, ()> {
            Box::pin(async move {
                let _ = (request, session);
            })
        }
    }

    struct StaticMetricsHandler;

    impl NodeMetricsRuntime for StaticMetricsHandler {
        fn collect_metrics(
            &self,
            request: NodeMetricsRequest,
        ) -> NodeMetricsFuture<'_, NodeMetricsResult> {
            Box::pin(async move {
                Ok(NodeMetricsResult::new(
                    request.target().clone(),
                    Some(NodeMetricsNodeSample::new(7_000_000, 11 * 1024 * 1024)),
                    vec![NodeMetricsPodSample::new(
                        "default",
                        "remote-pod",
                        "remote-uid",
                        vec![NodeMetricsContainerSample::new(
                            "app",
                            42_000_000,
                            6 * 1024 * 1024,
                        )],
                    )],
                ))
            })
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
        let service = Arc::new(crate::grpc_test_support::replication_service(
            db.clone(),
            supervisor.clone(),
        ));
        let app = crate::grpc_test_support::mount_service(
            axum::Router::new(),
            service.clone(),
            db,
            default_transport_policy(),
        );
        let app = mount_test_service_with_node_cert(app, "worker-1");
        let (endpoint, handle) = crate::grpc_test_support::serve_tls_test_app(app).await;

        let client = ReplicationGrpcClient::new(
            GrpcClientConfig {
                leader_endpoint: endpoint,
                token,
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: true,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor,
            klights_leader_rpc::transport_policy::GrpcTransportPolicy::shared_default(),
        );
        client
            .ensure_joined_with_runtimes(NodeControlRuntimes::new(
                NodeExecCapability::Available(Arc::new(StaticExecHandler)),
                NodeLogCapability::Unavailable,
                NodeMetricsCapability::Unavailable,
            ))
            .await
            .unwrap();

        let request = NodeExecSyncRequest::try_new(
            NodeExecTarget::try_new(
                "worker-1",
                "hostport-2155",
                "e2e-host-exec",
                "worker-container",
            )
            .unwrap(),
            vec![
                "/bin/sh".to_string(),
                "-c".to_string(),
                "echo ok".to_string(),
            ],
            300,
        )
        .unwrap();
        let runtime = crate::bootstrap::grpc_runtime_adapter::GrpcReplicationRuntimeAdapter::new(
            service.clone(),
        );
        let response = runtime.exec_sync(request).await.unwrap();

        assert_eq!(response.stdout(), b"worker-stdout\n");
        assert_eq!(response.exit_code(), 0);
        handle.abort();
    }

    #[tokio::test]
    async fn client_replies_to_node_metrics_requests_on_connect_stream() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let token = crate::bootstrap::cluster_meta::read_join_token(db.as_ref())
            .await
            .unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(crate::grpc_test_support::replication_service(
            db.clone(),
            supervisor.clone(),
        ));
        let app = crate::grpc_test_support::mount_service(
            axum::Router::new(),
            service.clone(),
            db,
            default_transport_policy(),
        );
        let app = mount_test_service_with_node_cert(app, "worker-1");
        let (endpoint, handle) = crate::grpc_test_support::serve_tls_test_app(app).await;

        let client = ReplicationGrpcClient::new(
            GrpcClientConfig {
                leader_endpoint: endpoint,
                token,
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: true,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor,
            klights_leader_rpc::transport_policy::GrpcTransportPolicy::shared_default(),
        );
        client
            .ensure_joined_with_runtimes(NodeControlRuntimes::new(
                NodeExecCapability::Unavailable,
                NodeLogCapability::Unavailable,
                NodeMetricsCapability::Available(Arc::new(StaticMetricsHandler)),
            ))
            .await
            .unwrap();

        let runtime = crate::bootstrap::grpc_runtime_adapter::GrpcReplicationRuntimeAdapter::new(
            service.clone(),
        );
        let response = runtime
            .collect_metrics(NodeMetricsRequest::new(
                NodeMetricsTarget::try_new("worker-1").unwrap(),
                Vec::new(),
            ))
            .await
            .unwrap();

        assert_eq!(response.target().node_name(), "worker-1");
        assert_eq!(response.pods()[0].uid(), "remote-uid");
        assert_eq!(response.pods()[0].containers()[0].cpu_nanos(), 42_000_000);
        handle.abort();
    }

    struct EchoExecStreamHandler;

    impl NodeExecRuntime for EchoExecStreamHandler {
        fn exec_sync(
            &self,
            request: NodeExecSyncRequest,
        ) -> NodeExecRuntimeFuture<'_, NodeExecSyncResult> {
            Box::pin(async move {
                let _ = request;
                NodeExecSyncResult::success(Vec::new(), Vec::new(), 0)
            })
        }

        fn exec_stream(
            &self,
            request: NodeExecRequest,
            session: Box<dyn NodeExecSession>,
        ) -> NodeExecRuntimeFuture<'_, ()> {
            Box::pin(async move {
                let _ = request;
                while let Some(frame) = session.recv_frame().await.unwrap() {
                    if frame.channel() == ExecStreamChannel::Stdin && !frame.data().is_empty() {
                        session
                            .send_frame(NodeExecFrame::new(
                                ExecStreamChannel::Stdout,
                                frame.data().to_vec(),
                                false,
                            ))
                            .await
                            .unwrap();
                    }
                    if frame.fin() {
                        break;
                    }
                }
                session
                    .send_frame(NodeExecFrame::new(
                        ExecStreamChannel::Error,
                        serde_json::json!({"metadata": {}, "status": "Success"})
                            .to_string()
                            .into_bytes(),
                        true,
                    ))
                    .await
                    .unwrap();
            })
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
        let service = Arc::new(crate::grpc_test_support::replication_service(
            db.clone(),
            supervisor.clone(),
        ));
        let app = crate::grpc_test_support::mount_service(
            axum::Router::new(),
            service.clone(),
            db,
            default_transport_policy(),
        );
        let app = mount_test_service_with_node_cert(app, "worker-1");
        let (endpoint, handle) = crate::grpc_test_support::serve_tls_test_app(app).await;

        let client = ReplicationGrpcClient::new(
            GrpcClientConfig {
                leader_endpoint: endpoint,
                token,
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: true,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor.clone(),
            klights_leader_rpc::transport_policy::GrpcTransportPolicy::shared_default(),
        );
        client
            .ensure_joined_with_runtimes(NodeControlRuntimes::new(
                NodeExecCapability::Available(Arc::new(EchoExecStreamHandler)),
                NodeLogCapability::Unavailable,
                NodeMetricsCapability::Unavailable,
            ))
            .await
            .unwrap();

        let request = NodeExecRequest::exec(
            NodeExecTarget::try_new("worker-1", "default", "remote-exec", "remote-container")
                .unwrap(),
            vec!["/bin/sh".to_string()],
            ExecStreamOptions::new(true, true, true, true),
        );
        let runtime = crate::bootstrap::grpc_runtime_adapter::GrpcReplicationRuntimeAdapter::new(
            service.clone(),
        );
        let session = runtime.open_exec(request).await.unwrap();
        session
            .send_frame(NodeExecFrame::new(
                ExecStreamChannel::Stdin,
                b"echo hello\n".to_vec(),
                false,
            ))
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
        assert_eq!(echoed.channel(), ExecStreamChannel::Stdout);
        assert_eq!(echoed.data(), b"echo hello\n");

        session
            .send_frame(NodeExecFrame::new(
                ExecStreamChannel::Stdin,
                Vec::new(),
                true,
            ))
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
        assert_eq!(status.channel(), ExecStreamChannel::Error);
        assert!(status.fin());
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
            .renew_node_lease_rpc(&current_renew_time_for_test(), 40)
            .await
            .expect("initial lease renewal should succeed");
        assert!(
            client.lane_pool_present(ChannelLane::Status).await,
            "Status lane should be warm after a successful renewal"
        );

        // Leader restarts: tear down the server so the cached connection
        // wedges. The renewal now fails AND the wedged lane is evicted, so
        // the heartbeat loop rebuilds a fresh channel on the next attempt.
        fixture.shutdown().await;
        let result = client
            .renew_node_lease_rpc(&current_renew_time_for_test(), 40)
            .await;
        assert!(
            result.is_err(),
            "renewal must fail while the leader is down"
        );
        assert!(
            !client.lane_pool_present(ChannelLane::Status).await,
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

        let watch_req = || {
            klights_leader_api::WatchRequest::try_new("v1", "Pod", None, None, None, None, None)
                .expect("valid Pod watch")
        };

        // Warm the Read lane by opening a watch stream. The stream itself
        // is not consumed — opening it is what builds the Read-lane pool.
        let _warm_stream = client
            .watch_resources_rpc(watch_req())
            .await
            .expect("initial watch open should succeed");
        assert!(
            client.lane_pool_present(ChannelLane::Read).await,
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
            !client.lane_pool_present(ChannelLane::Read).await,
            "wedged Read lane must be evicted so the next watch rebuilds"
        );
    }

    // ── Task 7: raft lane health and per-peer loss observability ─────────────

    /// Helper: build a test client against a server that wedges the given
    /// gRPC method path for 30 s, with a short raft_unary_deadline.

    async fn raft_timeout_client(
        wedge_path_suffix: &'static str,
    ) -> (ReplicationGrpcClient, tokio::task::JoinHandle<()>) {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let token = crate::bootstrap::cluster_meta::read_join_token(db.as_ref())
            .await
            .unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(crate::grpc_test_support::replication_service(
            db.clone(),
            supervisor.clone(),
        ));
        let app = crate::grpc_test_support::mount_service(
            axum::Router::new(),
            service,
            db.clone(),
            default_transport_policy(),
        );
        let app = mount_test_service_with_node_cert(app, "worker-1");
        let app = app.layer(axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| async move {
                if request.uri().path().ends_with(wedge_path_suffix) {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
                next.run(request).await
            },
        ));
        let (endpoint, handle) = crate::grpc_test_support::serve_tls_test_app(app).await;
        let client = ReplicationGrpcClient::new(
            GrpcClientConfig {
                leader_endpoint: endpoint,
                token,
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: true,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor,
            raft_deadline_policy(Duration::from_millis(50)),
        );
        (client, handle)
    }

    #[tokio::test]
    async fn append_entries_timeout_invalidates_lane() {
        let (client, handle) = raft_timeout_client("/RaftAppendEntries").await;
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            client.raft_append_entries_rpc(raft_receiver(), Vec::new()),
        )
        .await
        .expect("must complete within wall-clock bound");
        let msg = format!("{}", result.unwrap_err());
        assert!(
            msg.contains("deadline exceeded"),
            "AppendEntries timeout must report deadline exceeded, got: {msg}"
        );
        assert!(
            !client.lane_pool_present(ChannelLane::Raft).await,
            "Raft lane must be invalidated after AppendEntries deadline exceeded"
        );
        handle.abort();
    }

    #[tokio::test]
    async fn request_vote_timeout_invalidates_lane() {
        let (client, handle) = raft_timeout_client("/RaftVote").await;
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            client.raft_vote_rpc(raft_receiver(), Vec::new()),
        )
        .await
        .expect("must complete within wall-clock bound");
        let msg = format!("{}", result.unwrap_err());
        assert!(
            msg.contains("deadline exceeded"),
            "Vote timeout must report deadline exceeded, got: {msg}"
        );
        assert!(
            !client.lane_pool_present(ChannelLane::Raft).await,
            "Raft lane must be invalidated after Vote deadline exceeded"
        );
        handle.abort();
    }

    #[tokio::test]
    async fn install_snapshot_timeout_invalidates_lane() {
        let (client, handle) = raft_timeout_client("/RaftInstallSnapshot").await;
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            client.raft_install_snapshot_rpc(raft_receiver(), Vec::new()),
        )
        .await
        .expect("must complete within wall-clock bound");
        let msg = format!("{}", result.unwrap_err());
        assert!(
            msg.contains("deadline exceeded"),
            "InstallSnapshot timeout must report deadline exceeded, got: {msg}"
        );
        assert!(
            !client.lane_pool_present(ChannelLane::Raft).await,
            "Raft lane must be invalidated after InstallSnapshot deadline exceeded"
        );
        handle.abort();
    }

    #[tokio::test]
    async fn non_transport_raft_error_does_not_invalidate_lane() {
        // A raft application-layer error (e.g. the openraft state machine
        // returns an error inside the gRPC response body) must NOT evict the
        // Raft lane — only a transport-level timeout or connection failure
        // indicates a wedged connection that needs rebuilding.
        //
        // Strategy: call raft_append_entries_rpc against a real server.
        // The server decodes an empty/invalid payload and returns an error
        // inside the response body (not a transport-level tonic::Status).
        // The client must NOT call invalidate_lane — the Raft lane stays warm.
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let token = crate::bootstrap::cluster_meta::read_join_token(db.as_ref())
            .await
            .unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(crate::grpc_test_support::replication_service(
            db.clone(),
            supervisor.clone(),
        ));
        let app = crate::grpc_test_support::mount_service(
            axum::Router::new(),
            service,
            db.clone(),
            default_transport_policy(),
        );
        let app = mount_test_service_with_node_cert(app, "worker-1");
        let (endpoint, handle) = crate::grpc_test_support::serve_tls_test_app(app).await;
        let client = ReplicationGrpcClient::new(
            GrpcClientConfig {
                leader_endpoint: endpoint,
                token,
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker,
                dataplane: dataplane(),
                ca_cert_path: None,
                skip_ca: true,
                client_cert_pem: None,
                client_key_pem: None,
            },
            supervisor,
            klights_leader_rpc::transport_policy::GrpcTransportPolicy::shared_default(),
        );
        // Send an empty payload — the server returns an raft application error
        // in the response body, not a transport-level failure.
        let result = client
            .raft_append_entries_rpc(raft_receiver(), Vec::new())
            .await;
        // The call must complete (not time out) — it reached the server.
        // The result may be Ok(Err(msg)) (application error from the raft state
        // machine) or Err (transport) — we only care that the lane is NOT evicted.
        match &result {
            Ok(_) | Err(_) => {} // either is acceptable; lane state is the assertion
        }
        // Lane must still be present because no transport failure occurred.
        assert!(
            client.lane_pool_present(ChannelLane::Raft).await,
            "a non-transport raft error must not invalidate the Raft lane; result: {result:?}"
        );
        handle.abort();
    }

    #[tokio::test]
    async fn all_raft_rpcs_use_raft_unary_call() {
        // Structural invariant: all three raft RPC methods (AppendEntries, Vote,
        // InstallSnapshot) must route through raft_unary_call so they share the
        // same deadline + lane-invalidation policy.  We verify this by calling
        // all three against a wedge server and asserting:
        //   1. Each call reports "deadline exceeded" (not a different error).
        //   2. Each call evicts the Raft lane (which only raft_unary_call does).
        for (method, path) in [
            ("AppendEntries", "/RaftAppendEntries"),
            ("Vote", "/RaftVote"),
            ("InstallSnapshot", "/RaftInstallSnapshot"),
        ] {
            let (client, handle) = raft_timeout_client(path).await;

            let result = match method {
                "AppendEntries" => tokio::time::timeout(
                    Duration::from_secs(2),
                    client.raft_append_entries_rpc(raft_receiver(), Vec::new()),
                )
                .await
                .expect("must finish")
                .map(|_| ()),
                "Vote" => tokio::time::timeout(
                    Duration::from_secs(2),
                    client.raft_vote_rpc(raft_receiver(), Vec::new()),
                )
                .await
                .expect("must finish")
                .map(|_| ()),
                "InstallSnapshot" => tokio::time::timeout(
                    Duration::from_secs(2),
                    client.raft_install_snapshot_rpc(raft_receiver(), Vec::new()),
                )
                .await
                .expect("must finish")
                .map(|_| ()),
                _ => unreachable!("closed raft RPC test matrix"),
            };

            let msg = format!("{}", result.unwrap_err());
            assert!(
                msg.contains("deadline exceeded"),
                "{method} must report deadline exceeded (uses raft_unary_call), got: {msg}"
            );
            assert!(
                !client.lane_pool_present(ChannelLane::Raft).await,
                "{method} must evict Raft lane (proves raft_unary_call path)"
            );
            handle.abort();
        }
    }

    #[tokio::test]
    async fn raft_transport_records_per_peer_append_entries_and_timeout_counters() {
        // T7: after a deadline-exceeded AppendEntries, the client must have
        // incremented both raft_append_entries_call_count and raft_timeout_count.
        let (client, handle) = raft_timeout_client("/RaftAppendEntries").await;
        let payload = vec![0u8; 16]; // 16 bytes — verifiable in byte counter
        let _ = tokio::time::timeout(
            Duration::from_secs(2),
            client.raft_append_entries_rpc(raft_receiver(), payload.clone()),
        )
        .await;
        assert_eq!(
            client.raft_append_entries_call_count(),
            1,
            "raft_append_entries_call_count must be 1 after one call"
        );
        assert_eq!(
            client.raft_append_entries_byte_count(),
            payload.len() as u64,
            "raft_append_entries_byte_count must equal the payload length"
        );
        assert_eq!(
            client.raft_timeout_count(),
            1,
            "raft_timeout_count must be 1 after one deadline-exceeded AppendEntries"
        );
        handle.abort();
    }
}
