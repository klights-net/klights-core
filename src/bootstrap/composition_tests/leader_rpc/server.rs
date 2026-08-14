use super::support::GrpcReplicationServerTestExt as _;
use std::sync::Arc;

use std::sync::Mutex;

use klights::datastore::backend::{DatastoreBackend, DatastoreHandle};

use klights_cluster_core::ResourcePreconditions;

use klights_cluster_core::command::StorageCommand;

use klights_internal_protobuf::MetadataRequest;

use klights_internal_protobuf::replication_client::ReplicationClient;

use klights_internal_protobuf::replication_server::Replication;
use klights_internal_protobuf::{JoinRequest, JoinRole};

use klights_leader_api::{ControlplaneJoinHandler, ControlplaneJoinOutcome};

use klights_replication::ReplicationService;

use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};

use tokio::sync::mpsc;

use tonic_reflection::pb::v1::{
    ServerReflectionRequest, server_reflection_client::ServerReflectionClient,
    server_reflection_request, server_reflection_response,
};

#[derive(Default)]

struct RecordingNodeLifecycleStatus {
    requests: Mutex<Vec<klights_leader_api::NodeLifecycleStatusRequest>>,
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
        command_codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
    }
}

async fn create_scoped_token_for_test(
    db: &dyn DatastoreBackend,
    token: &str,
    scope: crate::bootstrap::composition_tests::leader_rpc::support::BootstrapTokenScope,
) {
    crate::bootstrap::composition_tests::leader_rpc::support::create_scoped_bootstrap_token_secret_for_test(
        db, scope, token,
    )
    .await
    .unwrap();
}

async fn grpc_test_server_with_signing_ca(
    db: DatastoreHandle,
    namespace: &str,
) -> super::support::GrpcReplicationServer {
    crate::bootstrap::composition_tests::leader_rpc::support::ensure_cluster_metadata(db.as_ref())
        .await
        .unwrap();
    let (ca_cert, ca_key, ca_cert_pem, ca_key_pem) =
        klights_auth::test_support::generate_ca_full_at(time::OffsetDateTime::now_utc()).unwrap();
    let etc = std::path::Path::new(namespace).join("etc");
    let ca_cert_path = etc.join("ca.crt");
    let ca_key_path = etc.join("ca.key");
    let service_account_key_path = etc.join("service-account-signing.key");
    std::fs::create_dir_all(&etc).unwrap();
    std::fs::write(&ca_cert_path, ca_cert_pem).unwrap();
    std::fs::write(&ca_key_path, ca_key_pem).unwrap();
    std::fs::write(&service_account_key_path, "service-account-signing-key").unwrap();
    drop((ca_cert, ca_key));

    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let service = Arc::new(
        crate::bootstrap::composition_tests::leader_rpc::support::replication_service(
            db.clone(),
            supervisor,
        ),
    );
    super::support::GrpcReplicationServer::new(service, db).with_namespace(namespace)
}

async fn grpc_test_server(
    db: DatastoreHandle,
) -> (String, Arc<ReplicationService>, tokio::task::JoinHandle<()>) {
    grpc_test_server_with_dispatcher(db, None).await
}

async fn grpc_test_server_with_dispatcher(
    db: DatastoreHandle,
    controller_dispatcher: Option<Arc<klights_controllers::ControllerDispatcher>>,
) -> (String, Arc<ReplicationService>, tokio::task::JoinHandle<()>) {
    grpc_test_server_full(db, controller_dispatcher, None).await
}

async fn grpc_test_server_full(
    db: DatastoreHandle,
    controller_dispatcher: Option<Arc<klights_controllers::ControllerDispatcher>>,
    controlplane_join_handler: Option<Arc<dyn ControlplaneJoinHandler>>,
) -> (String, Arc<ReplicationService>, tokio::task::JoinHandle<()>) {
    let (endpoint, service, _progress, handle) = grpc_test_server_full_with_node_cert(
        db,
        controller_dispatcher,
        controlplane_join_handler,
        None,
    )
    .await;
    (endpoint, service, handle)
}

async fn grpc_test_server_with_node_cert(
    db: DatastoreHandle,
    node_name: &str,
) -> (String, Arc<ReplicationService>, tokio::task::JoinHandle<()>) {
    let (endpoint, service, _progress, handle) =
        grpc_test_server_full_with_node_cert(db, None, None, Some(node_name.to_string())).await;
    (endpoint, service, handle)
}

async fn grpc_test_server_full_with_node_cert(
    db: DatastoreHandle,
    controller_dispatcher: Option<Arc<klights_controllers::ControllerDispatcher>>,
    controlplane_join_handler: Option<Arc<dyn ControlplaneJoinHandler>>,
    injected_node_cert: Option<String>,
) -> (
    String,
    Arc<ReplicationService>,
    Arc<klights_replication::FollowerProgressHub>,
    tokio::task::JoinHandle<()>,
) {
    grpc_test_server_full_with_node_cert_and_current_rv(
        db,
        controller_dispatcher,
        controlplane_join_handler,
        injected_node_cert,
        0,
    )
    .await
}

async fn grpc_test_server_full_with_node_cert_and_current_rv(
    db: DatastoreHandle,
    controller_dispatcher: Option<Arc<klights_controllers::ControllerDispatcher>>,
    controlplane_join_handler: Option<Arc<dyn ControlplaneJoinHandler>>,
    injected_node_cert: Option<String>,
    current_rv: i64,
) -> (
    String,
    Arc<ReplicationService>,
    Arc<klights_replication::FollowerProgressHub>,
    tokio::task::JoinHandle<()>,
) {
    crate::bootstrap::composition_tests::leader_rpc::support::ensure_cluster_metadata(db.as_ref())
        .await
        .unwrap();
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let follower_progress = Arc::new(klights_replication::FollowerProgressHub::new(0));
    let service = Arc::new(
        crate::bootstrap::composition_tests::leader_rpc::support::replication_service_with_progress(
            db.clone(),
            supervisor,
            follower_progress.clone(),
            current_rv,
        ),
    );
    let node_ports = crate::bootstrap::composition_tests::leader_rpc::support::local_node_ports(
        db.clone(),
        "test-leader".to_string(),
    );
    let app = super::support::mount_service_full(
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
        Some(node_ports.resource_query()),
        None,
        Some(node_ports.lifecycle_status()),
        klights_leader_rpc::transport_policy::GrpcTransportPolicy::shared_default(),
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
                            .insert(klights_types::TlsClientCertificate(node_client_cert_der(
                                node_name,
                                &["system:nodes"],
                            )));
                    }
                    klights_apiserver::insert_tonic_tcp_connect_info(
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
    (endpoint, service, follower_progress, handle)
}

/// bug-grpc A1/B2: serve the replication gRPC service built with an
/// explicit [`GrpcTransportPolicy`] so a test can shrink `max_message_bytes`
/// (decode-limit test) or `watch_heartbeat_interval` (per-stream heartbeat
/// test). `injected_node_cert` injects a node client cert so handlers that
/// require steady-state auth (e.g. `watch_resources`) accept the request;
/// `None` leaves auth to fail (the decode-size check fires first regardless).
async fn grpc_test_server_with_policy(
    db: klights::datastore::sqlite::Datastore,
    policy: Arc<klights_leader_rpc::transport_policy::GrpcTransportPolicy>,
    injected_node_cert: Option<&str>,
) -> (String, tokio::task::JoinHandle<()>) {
    let injected_node_cert = injected_node_cert.map(str::to_string);
    crate::bootstrap::composition_tests::leader_rpc::support::ensure_cluster_metadata(&db)
        .await
        .unwrap();
    let passive_reads =
        crate::bootstrap::composition_tests::leader_rpc::support::sqlite_passive_read_ports(&db);
    let db: DatastoreHandle = Arc::new(db);
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let service = Arc::new(
        crate::bootstrap::composition_tests::leader_rpc::support::replication_service(
            db.clone(),
            supervisor,
        ),
    );
    let grpc =
        super::support::GrpcReplicationServer::new_with_passive_reads(service, db, passive_reads)
            .with_watch_heartbeat_interval(policy.watch_heartbeat_interval);
    let app = super::support::mount_configured_test_service(axum::Router::new(), grpc, policy);
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
                            .insert(klights_types::TlsClientCertificate(node_client_cert_der(
                                node_name,
                                &["system:nodes"],
                            )));
                    }
                    klights_apiserver::insert_tonic_tcp_connect_info(
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
    let db = klights::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let policy = klights_leader_rpc::transport_policy::GrpcTransportPolicy {
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
    let oversized = tonic::Request::new(klights_internal_protobuf::ApplyOutboxRequest {
        idempotency_key: "k".to_string(),
        operation: "create".to_string(),
        payload_proto: vec![0u8; 8 * 1024],
        authoring_node: "worker-1".to_string(),
        client_id: "client".to_string(),
        stream_id: 1,
        stream_seq: 1,
        codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
    });
    let result = client.apply_outbox(oversized).await;
    assert!(
        result.is_err(),
        "server must reject a request exceeding the policy message limit, got {result:?}"
    );

    // A small request is not rejected on size grounds (it fails auth /
    // leadership later, but never with an OutOfRange size error).
    let small = tonic::Request::new(klights_internal_protobuf::ApplyOutboxRequest {
        idempotency_key: "k".to_string(),
        operation: "create".to_string(),
        payload_proto: vec![0u8; 16],
        authoring_node: "worker-1".to_string(),
        client_id: "client".to_string(),
        stream_id: 1,
        stream_seq: 1,
        codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
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
async fn fresh_idle_watch_heartbeat_carries_the_sampled_anchor() {
    let db = klights::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    db.create_namespace(
        "anchor",
        serde_json::json!({"metadata": {"name": "anchor"}}),
    )
    .await
    .expect("create anchor namespace");
    let policy = klights_leader_rpc::transport_policy::GrpcTransportPolicy {
        watch_heartbeat_interval: std::time::Duration::from_millis(100),
        ..Default::default()
    }
    .shared();
    let (endpoint, handle) =
        grpc_test_server_with_policy(db.clone(), policy, Some("worker-1")).await;
    let sampled_anchor = db
        .current_watch_replay_position()
        .await
        .expect("sample current replay anchor after server bootstrap");

    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = ReplicationClient::new(channel);
    let mut watch = client
        .watch_resources(klights_internal_protobuf::WatchResourcesRequest {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: None,
            field_selector: None,
            start_resource_version: None,
            label_selector: None,
            start_watch_replay_position: None,
        })
        .await
        .unwrap()
        .into_inner();

    let heartbeat = tokio::time::timeout(std::time::Duration::from_secs(1), watch.message())
        .await
        .expect("fresh idle watch must emit a heartbeat")
        .expect("heartbeat stream must remain healthy")
        .expect("heartbeat event");
    handle.abort();

    assert_eq!(heartbeat.event_type, "BOOKMARK");
    let resource = heartbeat.resource.expect("heartbeat resource");
    assert_eq!(resource.resource_version, sampled_anchor.resource_version);
    let position = heartbeat
        .resume_position
        .expect("heartbeat replay position");
    assert_eq!(position.resource_version, sampled_anchor.resource_version);
    assert_eq!(position.event_id, sampled_anchor.event_id);
}

#[tokio::test]
async fn watch_stream_emits_bookmark_during_stream_local_silence_under_nonmatching_traffic() {
    let db = klights::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    db.create_namespace("hb", serde_json::json!({"metadata": {"name": "hb"}}))
        .await
        .unwrap();
    let policy = klights_leader_rpc::transport_policy::GrpcTransportPolicy {
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
        .watch_resources(klights_internal_protobuf::WatchResourcesRequest {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: None,
            field_selector: None,
            start_resource_version: Some(0),
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
    let db = klights::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    crate::bootstrap::composition_tests::leader_rpc::support::ensure_cluster_metadata(&db)
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
    let policy = klights_leader_rpc::transport_policy::GrpcTransportPolicy {
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
        .watch_resources(klights_internal_protobuf::WatchResourcesRequest {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: None,
            field_selector: Some("spec.nodeName=worker-1".to_string()),
            start_resource_version: Some(0),
            label_selector: None,
            start_watch_replay_position: None,
        })
        .await
        .unwrap()
        .into_inner();

    crate::bootstrap::composition_tests::leader_rpc::support::broadcast_watch_event(
        &db,
        klights_watch::WatchEvent::modified(serde_json::json!({
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
    );

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
    super::support::GrpcReplicationServer,
    tokio::sync::watch::Sender<bool>,
) {
    let db = klights::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    grpc_leader_server_with_db(db, is_leader).await
}

async fn grpc_leader_server_with_db(
    db: klights::datastore::sqlite::Datastore,
    is_leader: bool,
) -> (
    super::support::GrpcReplicationServer,
    tokio::sync::watch::Sender<bool>,
) {
    crate::bootstrap::composition_tests::leader_rpc::support::ensure_cluster_metadata(&db)
        .await
        .unwrap();
    let passive_reads =
        crate::bootstrap::composition_tests::leader_rpc::support::sqlite_passive_read_ports(&db);
    let db: DatastoreHandle = Arc::new(db);
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let service = Arc::new(
        crate::bootstrap::composition_tests::leader_rpc::support::replication_service(
            db.clone(),
            supervisor,
        ),
    );
    let (leader_tx, is_leader_rx) = tokio::sync::watch::channel(is_leader);
    let grpc =
        super::support::GrpcReplicationServer::new_with_passive_reads(service, db, passive_reads)
            .with_leader_gate(is_leader_rx);
    (grpc, leader_tx)
}

fn watch_pods_request() -> klights_internal_protobuf::WatchResourcesRequest {
    klights_internal_protobuf::WatchResourcesRequest {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: None,
        field_selector: None,
        start_resource_version: Some(0),
        label_selector: None,
        start_watch_replay_position: None,
    }
}

fn watch_configmaps_from_rv(
    start_resource_version: i64,
) -> klights_internal_protobuf::WatchResourcesRequest {
    klights_internal_protobuf::WatchResourcesRequest {
        api_version: "v1".to_string(),
        kind: "ConfigMap".to_string(),
        namespace: None,
        field_selector: None,
        start_resource_version: Some(start_resource_version),
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

async fn configmap_replay_db() -> (klights::datastore::sqlite::Datastore, i64) {
    let db = klights::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    crate::bootstrap::composition_tests::leader_rpc::support::ensure_cluster_metadata(&db)
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

async fn register_grpc_watch_scope_crd(
    db: &dyn DatastoreBackend,
    group: &str,
    kind: &str,
    plural: &str,
    namespaced: bool,
) {
    db.create_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        &format!("{plural}.{group}"),
        serde_json::json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {"name": format!("{plural}.{group}")},
            "spec": {
                "group": group,
                "scope": if namespaced { "Namespaced" } else { "Cluster" },
                "names": {"kind": kind, "plural": plural, "singular": plural},
                "versions": [{"name": "v1", "served": true, "storage": true}]
            }
        }),
    )
    .await
    .expect("register CRD scope metadata");
}

fn custom_resource_watch_request(
    api_version: &str,
    kind: &str,
) -> klights_internal_protobuf::WatchResourcesRequest {
    klights_internal_protobuf::WatchResourcesRequest {
        api_version: api_version.to_string(),
        kind: kind.to_string(),
        namespace: None,
        field_selector: None,
        start_resource_version: Some(0),
        label_selector: None,
        start_watch_replay_position: None,
    }
}

#[tokio::test]
async fn grpc_watch_resolves_namespaced_crd_for_all_namespaces_delivery() {
    use futures::StreamExt;

    let db = klights::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    register_grpc_watch_scope_crd(&db, "example.com", "Widget", "widgets", true).await;
    let (grpc, _leader_tx) = grpc_leader_server_with_db(db.clone(), true).await;
    let mut stream = grpc
        .watch_resources(request_with_node_client_cert(
            custom_resource_watch_request("example.com/v1", "Widget"),
            "worker-1",
        ))
        .await
        .expect("namespaced CRD watch opens")
        .into_inner();

    db.create_resource(
        "example.com/v1",
        "Widget",
        Some("default"),
        "namespaced",
        serde_json::json!({
            "apiVersion": "example.com/v1",
            "kind": "Widget",
            "metadata": {"namespace": "default", "name": "namespaced"}
        }),
    )
    .await
    .expect("create namespaced custom resource");

    let event = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
        .await
        .expect("all-namespaces CRD watch must receive namespaced events")
        .expect("watch stream yields")
        .expect("watch stream stays healthy");
    assert_eq!(
        event.resource.expect("event resource").namespace.as_deref(),
        Some("default")
    );
}

#[tokio::test]
async fn grpc_watch_resolves_cluster_scoped_crd_delivery() {
    use futures::StreamExt;

    let db = klights::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    register_grpc_watch_scope_crd(
        &db,
        "cluster.example.com",
        "ClusterWidget",
        "clusterwidgets",
        false,
    )
    .await;
    let (grpc, _leader_tx) = grpc_leader_server_with_db(db.clone(), true).await;
    let mut stream = grpc
        .watch_resources(request_with_node_client_cert(
            custom_resource_watch_request("cluster.example.com/v1", "ClusterWidget"),
            "worker-1",
        ))
        .await
        .expect("cluster CRD watch opens")
        .into_inner();

    db.create_resource(
        "cluster.example.com/v1",
        "ClusterWidget",
        None,
        "clustered",
        serde_json::json!({
            "apiVersion": "cluster.example.com/v1",
            "kind": "ClusterWidget",
            "metadata": {"name": "clustered"}
        }),
    )
    .await
    .expect("create cluster custom resource");

    let event = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
        .await
        .expect("cluster CRD watch must receive cluster events")
        .expect("watch stream yields")
        .expect("watch stream stays healthy");
    assert_eq!(event.resource.expect("event resource").namespace, None);
}

#[tokio::test]
async fn watch_resources_replays_positive_resume_rv_through_positioned_history() {
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
        .expect("replayed event must carry a resume position");
    assert!(
        first_resume_position.event_id > 0,
        "scalar-RV watches must be upgraded to a composite resume position"
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
async fn watch_resources_replays_retained_event_for_zero_resume_without_new_signal() {
    use futures::StreamExt;

    let (db, _resume_rv) = configmap_replay_db().await;
    let (grpc, _leader_tx) = grpc_leader_server_with_db(db, true).await;
    let mut stream = grpc
        .watch_resources(request_with_node_client_cert(
            watch_configmaps_from_rv(0),
            "worker-1",
        ))
        .await
        .expect("leader should accept zero-rv watch")
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
            .expect("server must upgrade scalar watch to exact position")
            .event_id
            > 0
    );
}

#[tokio::test]
async fn watch_resources_maps_expired_positioned_replay_to_out_of_range() {
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
        klights_leader_rpc::is_watch_replay_expired_status(&status),
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
    let get = klights_internal_protobuf::GetResourceRequest {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "web".to_string(),
    };
    let list = klights_internal_protobuf::ListResourcesRequest {
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
    invalid_watch.start_resource_version = Some(-1);
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
            klights_internal_protobuf::GetNodeSubnetRequest {
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
            klights_internal_protobuf::AllocateNodeSubnetRequest {
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
            klights_internal_protobuf::AllocateNodeSubnetRequest {
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
                klights_internal_protobuf::AllocateNodeSubnetRequest {
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

fn test_node_registration_proto(
    git_commit: &str,
) -> klights_internal_protobuf::NodeRegistrationSnapshot {
    klights_internal_protobuf::NodeRegistrationSnapshot {
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

impl ControlplaneJoinHandler for AcceptingControlplaneJoinHandler {
    fn join(
        &self,
        request: klights_leader_api::ControlplaneJoinRequest,
    ) -> klights_leader_api::ControlplaneJoinFuture<'_> {
        Box::pin(async move {
            Ok(ControlplaneJoinOutcome::Accepted {
                voter_count_after: if request.as_learner { 1 } else { 2 },
                admitted_as_learner: request.as_learner,
                ca_cert_pem: String::new(),
                encrypted_ca_key: Vec::new(),
                ca_key_nonce: [0u8; 12],
            })
        })
    }

    // Permissive test double: treat callers as existing members so node-cert
    // (rejoin) JoinAsControlplane is accepted without a token. Token-gating
    // and non-member rejection are exercised by dedicated handlers/tests.
    fn is_controlplane_member<'a>(
        &'a self,
        _node_name: &'a str,
    ) -> klights_leader_api::ControlplaneMemberQueryFuture<'a> {
        Box::pin(async { true })
    }
}

/// Test double whose callers are never existing members — exercises the
/// "worker / first-time caller without a controlplane token is rejected"
/// path on JoinAsControlplane.
struct NonMemberControlplaneJoinHandler;

impl ControlplaneJoinHandler for NonMemberControlplaneJoinHandler {
    fn join(
        &self,
        request: klights_leader_api::ControlplaneJoinRequest,
    ) -> klights_leader_api::ControlplaneJoinFuture<'_> {
        Box::pin(async move {
            Ok(ControlplaneJoinOutcome::Accepted {
                voter_count_after: if request.as_learner { 1 } else { 2 },
                admitted_as_learner: request.as_learner,
                ca_cert_pem: String::new(),
                encrypted_ca_key: Vec::new(),
                ca_key_nonce: [0u8; 12],
            })
        })
    }

    fn is_controlplane_member<'a>(
        &'a self,
        _node_name: &'a str,
    ) -> klights_leader_api::ControlplaneMemberQueryFuture<'a> {
        Box::pin(async { false })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]

struct RecordedControlplaneJoin {
    node_id: u64,
    addr: String,
    node_name: String,
    as_learner: bool,
    node_internal_ip: Option<String>,
    node_registration: Option<klights_leader_api::RemoteNodeRegistrationSnapshot>,
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

impl ControlplaneJoinHandler for RecordingControlplaneJoinHandler {
    fn join(
        &self,
        request: klights_leader_api::ControlplaneJoinRequest,
    ) -> klights_leader_api::ControlplaneJoinFuture<'_> {
        Box::pin(async move {
            let klights_leader_api::ControlplaneJoinRequest {
                node_id,
                addr,
                node_name,
                as_learner,
                storage_incarnation: _,
                storage_log_attestation: _,
                command_codec_version: _,
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
        })
    }

    fn is_controlplane_member<'a>(
        &'a self,
        _node_name: &'a str,
    ) -> klights_leader_api::ControlplaneMemberQueryFuture<'a> {
        Box::pin(async { true })
    }
}

async fn open_connect(
    endpoint: &str,
    join: JoinRequest,
) -> (
    mpsc::Sender<klights_internal_protobuf::FollowerMessage>,
    tonic::codec::Streaming<klights_internal_protobuf::LeaderMessage>,
) {
    let channel = tonic::transport::Endpoint::from_shared(endpoint.to_string())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = ReplicationClient::new(channel);
    let (tx, mut rx) = mpsc::channel(8);
    tx.send(klights_internal_protobuf::FollowerMessage {
        payload: Some(klights_internal_protobuf::follower_message::Payload::Join(
            join,
        )),
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
        klights_leader_rpc::JOIN_TOKEN_METADATA_KEY,
        token.parse().unwrap(),
    );
    request
}

fn request_with_node_client_cert<T>(message: T, node_name: &str) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    request
        .extensions_mut()
        .insert(klights_types::TlsClientCertificate(node_client_cert_der(
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
        .insert(klights_types::TlsClientCertificate(node_client_cert_der(
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
        .insert(klights_types::TlsClientCertificate(node_client_cert_der(
            "admin",
            &["system:masters"],
        )));
    request
}

// ── CRIT-2: NodeRestriction on node-scoped RPCs ──

// ── CRIT-1: raft RPC authentication ──

async fn raft_test_server() -> super::support::GrpcReplicationServer {
    let db: DatastoreHandle = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let service = Arc::new(
        crate::bootstrap::composition_tests::leader_rpc::support::replication_service(
            db.clone(),
            supervisor,
        ),
    );
    super::support::GrpcReplicationServer::new(service, db)
}

fn test_raft_receiver_admission() -> Vec<u8> {
    serde_json::to_vec(&klights_leader_rpc::raft_rpc::RaftReceiverAdmission {
        addr: "test".to_string(),
        storage_incarnation: uuid::Uuid::nil().to_string(),
        admitted_log: None,
    })
    .unwrap()
}

#[tokio::test]
async fn raft_append_entries_rejects_unauthenticated() {
    let grpc = raft_test_server().await;
    // No bootstrap token and no client certificate.
    let status = grpc
        .raft_append_entries(tonic::Request::new(
            klights_internal_protobuf::RaftAppendEntriesRequest {
                payload: vec![],
                command_codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
                receiver_admission: test_raft_receiver_admission(),
            },
        ))
        .await
        .expect_err("unauthenticated raft RPC must be rejected");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
}

#[tokio::test]
async fn submit_resource_command_rejects_worker_certificate_before_decode() {
    let grpc = raft_test_server().await;
    let status = grpc
        .submit_resource_command(request_with_node_client_cert(
            klights_internal_protobuf::SubmitResourceCommandRequest {
                command_protobuf: Vec::new(),
                codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
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
            klights_internal_protobuf::SubmitResourceCommandRequest {
                command_protobuf: Vec::new(),
                codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
            },
            "cp2",
        ))
        .await
        .expect_err("follower must reject before decoding or mutating");
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn submit_resource_command_rejects_pre_v3_controlplane_before_decode() {
    let grpc = raft_test_server().await;
    let status = grpc
        .submit_resource_command(request_with_controlplane_client_cert(
            klights_internal_protobuf::SubmitResourceCommandRequest {
                command_protobuf: Vec::new(),
                codec_version: klights_cluster_core::COMMAND_CODEC_VERSION - 1,
            },
            "old-controlplane",
        ))
        .await
        .expect_err("a pre-v3 control plane must fail before command decoding");
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn submit_resource_command_rejects_generic_pod_hard_delete() {
    let grpc = raft_test_server().await;
    let command = klights_cluster_core::command::StorageCommand::DeleteResource {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "web".to_string(),
        preconditions: klights::datastore::ResourcePreconditions::uid("pod-uid"),
    };
    let status = grpc
        .submit_resource_command(request_with_controlplane_client_cert(
            klights_internal_protobuf::SubmitResourceCommandRequest {
                command_protobuf: klights_leader_rpc::storage_wire_codec::encode_command_protobuf(
                    &command,
                )
                .expect("encode command"),
                codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
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
    let command = klights_cluster_core::command::StorageCommand::CreateResource {
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
            klights_internal_protobuf::SubmitResourceCommandRequest {
                command_protobuf: klights_leader_rpc::storage_wire_codec::encode_command_protobuf(
                    &command,
                )
                .expect("encode command"),
                codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
            },
            "cp1",
        ))
        .await
        .expect("control-plane create")
        .into_inner();
    assert!(matches!(
        response.result,
        Some(klights_internal_protobuf::submit_resource_command_response::Result::Resource(resource))
            if resource.kind == "ConfigMap" && resource.name == "settings"
    ));
}

#[tokio::test]
async fn raft_append_entries_rejects_bootstrap_token() {
    let db: DatastoreHandle = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    crate::bootstrap::composition_tests::leader_rpc::support::ensure_cluster_metadata(db.as_ref())
        .await
        .unwrap();
    let token =
        crate::bootstrap::composition_tests::leader_rpc::support::ensure_worker_bootstrap_token(
            db.as_ref(),
        )
        .await
        .unwrap();
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let service = Arc::new(
        crate::bootstrap::composition_tests::leader_rpc::support::replication_service(
            db.clone(),
            supervisor,
        ),
    );
    let grpc = super::support::GrpcReplicationServer::new(service, db);

    let status = grpc
        .raft_append_entries(request_with_join_token(
            klights_internal_protobuf::RaftAppendEntriesRequest {
                payload: vec![],
                command_codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
                receiver_admission: test_raft_receiver_admission(),
            },
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
        .raft_vote(tonic::Request::new(
            klights_internal_protobuf::RaftVoteRequest {
                payload: vec![],
                command_codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
                receiver_admission: test_raft_receiver_admission(),
            },
        ))
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
            klights_internal_protobuf::RaftAppendEntriesRequest {
                payload: vec![],
                command_codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
                receiver_admission: test_raft_receiver_admission(),
            },
            "controlplane-2",
        ))
        .await
        .expect("system:controlplanes node cert must authorize the raft peer");
    assert!(resp.into_inner().result.is_some());
}

#[tokio::test]
async fn raft_append_entries_rejects_missing_receiver_admission_before_dispatch() {
    let grpc = raft_test_server().await;
    let status = grpc
        .raft_append_entries(request_with_controlplane_client_cert(
            klights_internal_protobuf::RaftAppendEntriesRequest {
                payload: vec![],
                command_codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
                receiver_admission: Vec::new(),
            },
            "controlplane-2",
        ))
        .await
        .expect_err("exact-v3 consensus must carry receiver admission proof");
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn raft_append_entries_rejects_authenticated_pre_v3_member_before_dispatch() {
    let grpc = raft_test_server().await;
    let status = grpc
        .raft_append_entries(request_with_controlplane_client_cert(
            klights_internal_protobuf::RaftAppendEntriesRequest {
                payload: vec![],
                command_codec_version: klights_cluster_core::COMMAND_CODEC_VERSION - 1,
                receiver_admission: test_raft_receiver_admission(),
            },
            "old-controlplane",
        ))
        .await
        .expect_err("an authenticated pre-v3 member must not dispatch consensus commands");
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
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
            klights_internal_protobuf::RaftInstallSnapshotRequest {
                payload: vec![],
                command_codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
                receiver_admission: test_raft_receiver_admission(),
            },
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
            klights_internal_protobuf::RaftVoteRequest {
                payload: vec![],
                command_codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
                receiver_admission: test_raft_receiver_admission(),
            },
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
            klights_internal_protobuf::RaftAppendEntriesRequest {
                payload: vec![],
                command_codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
                receiver_admission: test_raft_receiver_admission(),
            },
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
            klights_internal_protobuf::RaftInstallSnapshotRequest {
                payload: vec![],
                command_codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
                receiver_admission: test_raft_receiver_admission(),
            },
        ))
        .await
        .expect_err("admin cert must not authenticate the raft peer");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
}

#[tokio::test]
async fn renew_node_lease_rejects_mismatched_node() {
    let db = klights::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let db: DatastoreHandle = Arc::new(db);
    let tracker = Arc::new(klights_controllers::node_lease::NodeLeaseTracker::new_at(
        chrono::DateTime::parse_from_rfc3339("2026-05-25T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc),
    ));
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let service = Arc::new(
        crate::bootstrap::composition_tests::leader_rpc::support::replication_service(
            db.clone(),
            supervisor,
        ),
    );
    let grpc = super::support::GrpcReplicationServer::new_with_node_lease_tracker(
        service,
        db.clone(),
        tracker.clone(),
    );

    // worker-1's cert tries to renew worker-2's lease.
    let status = grpc
        .renew_node_lease(request_with_node_client_cert(
            klights_internal_protobuf::RenewNodeLeaseRequest {
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
    let db: DatastoreHandle = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    let tracker = Arc::new(klights_controllers::node_lease::NodeLeaseTracker::new_at(
        chrono::Utc::now(),
    ));
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let service = Arc::new(
        crate::bootstrap::composition_tests::leader_rpc::support::replication_service(
            db.clone(),
            supervisor,
        ),
    );
    let grpc = super::support::GrpcReplicationServer::new_with_node_lease_tracker(
        service,
        db,
        tracker.clone(),
    );

    for duration in [0, -1] {
        let status = grpc
            .renew_node_lease(request_with_node_client_cert(
                klights_internal_protobuf::RenewNodeLeaseRequest {
                    node_name: "worker-1".to_string(),
                    renew_time: klights_cluster_core::k8s_time::format_time(chrono::Utc::now()),
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
    let db: DatastoreHandle = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
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
    let service = Arc::new(
        crate::bootstrap::composition_tests::leader_rpc::support::replication_service(
            db.clone(),
            supervisor,
        ),
    );
    let grpc = super::support::GrpcReplicationServer::new(service, db.clone());
    let command = StorageCommand::PatchResource {
        api_version: "v1".to_string(),
        kind: "Node".to_string(),
        namespace: None,
        name: "worker-1".to_string(),
        patch_kind: klights_cluster_core::PatchKind::Merge,
        patch: serde_json::json!({"metadata": {"labels": {"smuggled": "true"}}}),
        preconditions: ResourcePreconditions::uid("node-uid-1"),
        strict_resource_version: false,
    };
    let payload =
        crate::bootstrap::composition_tests::leader_rpc::support::OutboxPayload::from_command(
            command,
        )
        .encode_protobuf()
        .expect("encode payload");

    let rejected = grpc
        .apply_outbox(request_with_node_client_cert(
            klights_internal_protobuf::ApplyOutboxRequest {
                idempotency_key: "smuggled-node-patch".to_string(),
                operation: klights_kubelet::node_outbox::payload::OutboxOperation::NodeStatus
                    .as_str()
                    .to_string(),
                payload_proto: payload,
                authoring_node: "worker-1".to_string(),
                client_id: "worker-1".to_string(),
                stream_id: 1,
                stream_seq: 1,
                codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
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
        crate::bootstrap::composition_tests::leader_rpc::support::OutboxPayload::from_command(
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
        klights_internal_protobuf::ApplyOutboxRequest {
            idempotency_key: "valid-after-smuggling".to_string(),
            operation: klights_kubelet::node_outbox::payload::OutboxOperation::NodeStatus
                .as_str()
                .to_string(),
            payload_proto: valid_status_payload(),
            authoring_node: "worker-1".to_string(),
            client_id: "worker-1".to_string(),
            stream_id: 1,
            stream_seq: 2,
            codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
        },
        "worker-1",
    ))
    .await
    .expect("sequence two applies after RPC terminal authorization decision");

    let malformed = grpc
        .apply_outbox(request_with_node_client_cert(
            klights_internal_protobuf::ApplyOutboxRequest {
                idempotency_key: "malformed-rpc-row".to_string(),
                operation: klights_kubelet::node_outbox::payload::OutboxOperation::NodeStatus
                    .as_str()
                    .to_string(),
                payload_proto: vec![0xff, 0x00, 0x81],
                authoring_node: "worker-1".to_string(),
                client_id: "worker-1".to_string(),
                stream_id: 1,
                stream_seq: 3,
                codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
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
        klights_internal_protobuf::ApplyOutboxRequest {
            idempotency_key: "valid-after-malformed".to_string(),
            operation: klights_kubelet::node_outbox::payload::OutboxOperation::NodeStatus
                .as_str()
                .to_string(),
            payload_proto: valid_status_payload(),
            authoring_node: "worker-1".to_string(),
            client_id: "worker-1".to_string(),
            stream_id: 1,
            stream_seq: 4,
            codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
        },
        "worker-1",
    ))
    .await
    .expect("sequence four applies after malformed RPC terminal decision");
}

#[tokio::test]
async fn node_effect_rpc_rejects_wrong_uid_before_committed_apply() {
    let db: DatastoreHandle = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
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
    let service = Arc::new(
        crate::bootstrap::composition_tests::leader_rpc::support::replication_service(
            db.clone(),
            supervisor,
        ),
    );
    let grpc = super::support::GrpcReplicationServer::new(service, db.clone());
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
    let payload =
        crate::bootstrap::composition_tests::leader_rpc::support::OutboxPayload::from_command(
            command,
        )
        .encode_protobuf()
        .expect("encode payload");

    let response = grpc
        .apply_outbox(request_with_node_client_cert(
            klights_internal_protobuf::ApplyOutboxRequest {
                idempotency_key: "wrong-node-uid".to_string(),
                operation: klights_kubelet::node_outbox::payload::OutboxOperation::NodeStatus
                    .as_str()
                    .to_string(),
                payload_proto: payload,
                authoring_node: "worker-1".to_string(),
                client_id: "worker-1".to_string(),
                stream_id: 1,
                stream_seq: 1,
                codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
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
async fn grpc_apply_outbox_accepts_joining_controlplane_node_status() {
    let db: DatastoreHandle = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    let created = db
        .create_resource(
            "v1",
            "Node",
            None,
            "mn-controlplane2",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {
                    "name": "mn-controlplane2",
                    "uid": "controlplane2-uid"
                },
                "status": {
                    "conditions": [{
                        "type": "Ready",
                        "status": "False",
                        "reason": "NetworkUnavailable",
                        "lastTransitionTime": "2026-07-19T02:12:10Z"
                    }, {
                        "type": "NetworkUnavailable",
                        "status": "True",
                        "reason": "DataplaneNotReady",
                        "lastTransitionTime": "2026-07-19T02:12:10Z"
                    }]
                }
            }),
        )
        .await
        .expect("create joining controlplane Node");
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let service = Arc::new(
        crate::bootstrap::composition_tests::leader_rpc::support::replication_service(
            db.clone(),
            supervisor,
        ),
    );
    let grpc = super::support::GrpcReplicationServer::new(service, db.clone());
    let payload =
        crate::bootstrap::composition_tests::leader_rpc::support::OutboxPayload::from_command(
            StorageCommand::UpdateStatus {
                api_version: "v1".to_string(),
                kind: "Node".to_string(),
                namespace: None,
                name: "mn-controlplane2".to_string(),
                status: serde_json::json!({
                    "conditions": [{
                        "type": "Ready",
                        "status": "True",
                        "reason": "Ready",
                        "lastTransitionTime": "2026-07-19T02:12:14Z"
                    }, {
                        "type": "NetworkUnavailable",
                        "status": "False",
                        "reason": "Ready",
                        "lastTransitionTime": "2026-07-19T02:12:14Z"
                    }]
                }),
                expected_rv: None,
                preconditions: ResourcePreconditions::uid(created.uid.clone()),
                observed_status_stamp: None,
            },
        )
        .encode_protobuf()
        .expect("encode controlplane Node status payload");

    let response = grpc
        .apply_outbox(request_with_controlplane_client_cert(
            klights_internal_protobuf::ApplyOutboxRequest {
                idempotency_key: "controlplane2-node-ready".to_string(),
                operation: klights_kubelet::node_outbox::payload::OutboxOperation::NodeStatus
                    .as_str()
                    .to_string(),
                payload_proto: payload,
                authoring_node: "mn-controlplane2".to_string(),
                client_id: "mn-controlplane2".to_string(),
                stream_id: 1,
                stream_seq: 1,
                codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
            },
            "mn-controlplane2",
        ))
        .await
        .expect("joining controlplane node cert must authorize durable NodeStatus delivery")
        .into_inner();

    assert!(
        response.error.is_none(),
        "unexpected apply error: {response:?}"
    );
    assert!(response.applied_rv > created.resource_version);
    let stored = db
        .get_resource("v1", "Node", None, "mn-controlplane2")
        .await
        .expect("read joining controlplane Node")
        .expect("joining controlplane Node exists");
    assert_eq!(
        stored.data.pointer("/status/conditions/0/status"),
        Some(&serde_json::json!("True"))
    );
    assert_eq!(
        stored.data.pointer("/status/conditions/1/status"),
        Some(&serde_json::json!("False"))
    );
}

#[tokio::test]
async fn outbox_transport_contract_rpc_rejects_unvalidated_stream_identity() {
    let db: DatastoreHandle = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let service = Arc::new(
        crate::bootstrap::composition_tests::leader_rpc::support::replication_service(
            db.clone(),
            supervisor,
        ),
    );
    let grpc = super::support::GrpcReplicationServer::new(service, db.clone());
    let command = StorageCommand::UpdateNodeDataplane {
        node_name: "worker-1".to_string(),
        mode: "root".to_string(),
        encryption: "enabled".to_string(),
        public_key: Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
        endpoint: "192.0.2.10".to_string(),
        port: Some(7679),
    };
    let payload =
        crate::bootstrap::composition_tests::leader_rpc::support::OutboxPayload::from_command(
            command,
        )
        .encode_protobuf()
        .expect("encode dataplane payload");

    let status = grpc
        .apply_outbox(request_with_node_client_cert(
            klights_internal_protobuf::ApplyOutboxRequest {
                idempotency_key: String::new(),
                operation: "NodeDataplane".to_string(),
                payload_proto: payload,
                authoring_node: "worker-1".to_string(),
                client_id: String::new(),
                stream_id: 0,
                stream_seq: 0,
                codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
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
    let db: DatastoreHandle = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let service = Arc::new(
        crate::bootstrap::composition_tests::leader_rpc::support::replication_service(
            db.clone(),
            supervisor,
        ),
    );
    let (_leader_tx, follower_rx) = tokio::sync::watch::channel(false);
    let follower = super::support::GrpcReplicationServer::new(service.clone(), db.clone())
        .with_leader_gate(follower_rx);

    let status = follower
        .list_pod_cleanup_intents_for_node(request_with_node_client_cert(
            klights_internal_protobuf::ListPodCleanupIntentsForNodeRequest {
                node_name: "worker-1".to_string(),
            },
            "worker-1",
        ))
        .await
        .expect_err("follower must not serve cleanup intents");
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert_eq!(status.message(), "not current leader authority");

    let leader = super::support::GrpcReplicationServer::new(service, db);
    let status = leader
        .list_pod_cleanup_intents_for_node(request_with_node_client_cert(
            klights_internal_protobuf::ListPodCleanupIntentsForNodeRequest {
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
    let db: DatastoreHandle = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let service = Arc::new(
        crate::bootstrap::composition_tests::leader_rpc::support::replication_service(
            db.clone(),
            supervisor,
        ),
    );
    let (_leader_tx, follower_rx) = tokio::sync::watch::channel(false);
    let follower =
        super::support::GrpcReplicationServer::new(service, db).with_leader_gate(follower_rx);

    let status = follower
        .delete_pod_cleanup_intent(request_with_node_client_cert(
            klights_internal_protobuf::DeletePodCleanupIntentRequest {
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
    assert_eq!(status.message(), "not current leader authority");
}

#[tokio::test]
async fn projected_token_rpc_requires_exact_bound_pod_uid_and_node() {
    let grpc = raft_test_server().await;
    let status = grpc
        .projected_service_account_token(request_with_node_client_cert(
            klights_internal_protobuf::ProjectedServiceAccountTokenRequest {
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
async fn projected_token_rpc_does_not_reauthorize_worker_as_leader_local_kubelet() {
    struct MissingAuthoritativeTokenState;

    impl klights_leader_api::LeaderAuthenticatedProjectedServiceAccountToken
        for MissingAuthoritativeTokenState
    {
        fn issue_authenticated_projected_service_account_token(
            &self,
            _request: klights_leader_api::ProjectedServiceAccountTokenRequest,
        ) -> klights_leader_api::ProjectedServiceAccountTokenFuture<'_> {
            Box::pin(async {
                Err(klights_leader_api::ProjectedServiceAccountTokenError::BoundPodNotFound)
            })
        }
    }

    let mut grpc = raft_test_server().await;
    grpc = grpc.with_projected_token(Arc::new(MissingAuthoritativeTokenState));
    let status = grpc
        .projected_service_account_token(request_with_node_client_cert(
            klights_internal_protobuf::ProjectedServiceAccountTokenRequest {
                namespace: "default".to_string(),
                service_account_name: "default".to_string(),
                audiences: vec!["api".to_string()],
                expiration_seconds: 3_600,
                bound_pod_name: Some("web".to_string()),
                bound_pod_uid: Some("pod-uid".to_string()),
                bound_node_name: Some("worker-1".to_string()),
                bound_node_uid: None,
            },
            "worker-1",
        ))
        .await
        .expect_err("unseeded test server cannot issue a token");
    assert_eq!(
        status.code(),
        tonic::Code::NotFound,
        "authenticated owning worker must reach the post-auth leader issuer: {status}"
    );
    assert_eq!(status.message(), "bound Pod was not found");
}

#[tokio::test]
async fn projected_token_rpc_rejects_authenticated_worker_claiming_another_node() {
    let grpc = raft_test_server().await;
    let status = grpc
        .projected_service_account_token(request_with_node_client_cert(
            klights_internal_protobuf::ProjectedServiceAccountTokenRequest {
                namespace: "default".to_string(),
                service_account_name: "default".to_string(),
                audiences: vec!["api".to_string()],
                expiration_seconds: 3_600,
                bound_pod_name: Some("web".to_string()),
                bound_pod_uid: Some("pod-uid".to_string()),
                bound_node_name: Some("worker-1".to_string()),
                bound_node_uid: None,
            },
            "worker-2",
        ))
        .await
        .expect_err("worker certificate must not claim another node");
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
    assert_eq!(
        status.message(),
        "node \"worker-2\" may not act for node \"worker-1\""
    );
}

#[tokio::test]
async fn projected_token_rpc_rejects_authenticated_controlplane_claiming_another_node() {
    let grpc = raft_test_server().await;
    let status = grpc
        .projected_service_account_token(request_with_controlplane_client_cert(
            klights_internal_protobuf::ProjectedServiceAccountTokenRequest {
                namespace: "default".to_string(),
                service_account_name: "default".to_string(),
                audiences: vec!["api".to_string()],
                expiration_seconds: 3_600,
                bound_pod_name: Some("web".to_string()),
                bound_pod_uid: Some("pod-uid".to_string()),
                bound_node_name: Some("worker-1".to_string()),
                bound_node_uid: None,
            },
            "controlplane-2",
        ))
        .await
        .expect_err("control-plane node certificate must not claim another node");
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
    assert_eq!(
        status.message(),
        "node \"controlplane-2\" may not act for node \"worker-1\""
    );
}

#[tokio::test]
async fn renew_node_lease_rejects_renew_time_skew_over_100_seconds() {
    let wall_time = chrono::DateTime::parse_from_rfc3339("2040-02-03T04:05:06Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let db = klights::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let db: DatastoreHandle = Arc::new(db);
    let tracker = Arc::new(klights_controllers::node_lease::NodeLeaseTracker::new_at(
        wall_time,
    ));
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let service = Arc::new(
        crate::bootstrap::composition_tests::leader_rpc::support::replication_service(
            db.clone(),
            supervisor,
        ),
    );
    let grpc = super::support::GrpcReplicationServer::new_with_node_lease_tracker(
        service,
        db.clone(),
        tracker.clone(),
    )
    .with_wall_clock(Arc::new(move || wall_time));

    let skewed =
        klights_cluster_core::k8s_time::format_time(wall_time - chrono::Duration::seconds(101));
    let status = grpc
        .renew_node_lease(request_with_node_client_cert(
            klights_internal_protobuf::RenewNodeLeaseRequest {
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
    let db = klights::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let db: DatastoreHandle = Arc::new(db);
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let service = Arc::new(
        crate::bootstrap::composition_tests::leader_rpc::support::replication_service(
            db.clone(),
            supervisor,
        ),
    );
    let grpc = super::support::GrpcReplicationServer::new(service, db.clone());
    let command = StorageCommand::UpdateNodeDataplane {
        node_name: "worker-2".to_string(),
        mode: "root".to_string(),
        encryption: "enabled".to_string(),
        public_key: Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
        endpoint: "192.0.2.20".to_string(),
        port: Some(7679),
    };
    let payload =
        crate::bootstrap::composition_tests::leader_rpc::support::OutboxPayload::from_command(
            command,
        )
        .encode_protobuf()
        .unwrap();

    let response = grpc
        .apply_outbox(request_with_node_client_cert(
            klights_internal_protobuf::ApplyOutboxRequest {
                idempotency_key: "dataplane-worker-2-from-worker-1".to_string(),
                operation: klights_kubelet::node_outbox::payload::OutboxOperation::NodeDataplane
                    .as_str()
                    .to_string(),
                payload_proto: payload,
                authoring_node: "worker-1".to_string(),
                client_id: "client".to_string(),
                stream_id: 1,
                stream_seq: 1,
                codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
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

#[tokio::test]
async fn get_metadata_rpc_returns_cluster_metadata_for_node_cert() {
    let db = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    crate::bootstrap::composition_tests::leader_rpc::support::ensure_cluster_metadata(db.as_ref())
        .await
        .unwrap();
    // T3: `append_log_apply_entry` removed. `current_log_index`
    // always returns 0; the raft `last_applied` is authoritative.
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let service = Arc::new(
        crate::bootstrap::composition_tests::leader_rpc::support::replication_service(
            db.clone(),
            supervisor,
        ),
    );
    let grpc = super::support::GrpcReplicationServer::new(service, db);

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
    let db: DatastoreHandle = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let service = Arc::new(
        crate::bootstrap::composition_tests::leader_rpc::support::replication_service(
            db.clone(),
            supervisor,
        ),
    );
    let grpc = super::support::GrpcReplicationServer::new(service.clone(), db);
    let mut request = request_with_node_client_cert(
        klights_internal_protobuf::ObservePeerEndpointRequest {
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
    struct TestWallClock;

    impl klights_kubelet::runtime_clock::RuntimeClock for TestWallClock {
        fn now_ms(&self) -> i64 {
            1_704_164_645_000
        }
    }

    let db = klights::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let addresses =
        klights_kubelet::node::NodeRegistrationAddresses::new("172.31.10.2".to_string(), None);
    let profile = klights_kubelet::node_config::NodeRegistrationProfile::new(
        klights_network_api::NodePeerMode::Root,
        klights_kubelet::node_config::KubeletNodeRole::Leader,
        false,
        klights_types::BuildIdentity::new("v0.0.0-test", "test-commit"),
    );
    let composition =
        crate::bootstrap::composition_tests::leader_rpc::support::IntegrationLeaderRpcComposition::new(
            Arc::new(db.clone()),
        );
    composition
        .register_node_at_addresses("leader-a", &profile, None, &addresses)
        .await
        .unwrap();

    let local_ports = crate::bootstrap::composition_tests::leader_rpc::support::local_network_ports(
        Arc::new(db.clone()),
        "leader-a".to_string(),
    );
    let query = local_ports.resource_query();
    let node_local =
        crate::bootstrap::composition_tests::leader_rpc::support::IntegrationLeaderRpcComposition::open_node_local(
            Arc::new(TaskSupervisor::new(TaskCategoryConfig::default())),
            "sqlite:observed-leader-endpoint-status",
        )
        .await
        .expect("open node-local outbox");
    let publisher = klights_kubelet::node::OutboxNodeSelfStatusPublisher::new(
        "leader-a",
        query.clone(),
        Arc::new(node_local.outbox()),
        Arc::new(TestWallClock),
    );

    klights_leader_rpc::server::refresh_local_node_external_ip_from_observed_endpoint(
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
        klights_kubelet::node_outbox::payload::OutboxOperation::NodeStatus.as_str()
    );
    let payload =
        crate::bootstrap::composition_tests::leader_rpc::support::OutboxPayload::decode_protobuf(
            &row.payload_proto,
        )
        .expect("decode status payload");
    let StorageCommand::UpdateStatus { status, .. } = payload.command else {
        panic!("external IP publication must be status-only")
    };
    let addresses = status
        .pointer("/addresses")
        .and_then(|value| value.as_array())
        .unwrap();
    assert!(
        addresses.iter().any(|address| {
            address["type"] == "InternalIP" && address["address"] == "172.31.10.2"
        })
    );
    assert!(
        addresses.iter().any(|address| {
            address["type"] == "ExternalIP" && address["address"] == "10.99.0.10"
        })
    );
}

#[tokio::test]
async fn node_effect_join_external_ip_is_atomic_with_metadata_cas_without_redundant_status() {
    let db = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
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
    let dataplane = klights_cluster_store::DataplanePeerMetadata::try_new(
        "worker-1".to_string(),
        klights_cluster_store::DataplaneMode::Root,
        klights_cluster_store::DataplaneEncryption::Enabled,
        Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
        Some("192.0.2.80".to_string()),
        Some(51_820),
    )
    .expect("valid dataplane metadata");
    db.update_node_dataplane(dataplane.clone())
        .await
        .expect("store joining dataplane metadata");
    let local = crate::bootstrap::composition_tests::leader_rpc::support::local_network_ports(
        db.clone(),
        "leader-a".to_string(),
    );
    let query = local.resource_query();
    let status = Arc::new(RecordingNodeLifecycleStatus::default());
    let node_uid = created.uid.clone();

    let focused =
        crate::bootstrap::composition_tests::leader_rpc::support::focused_dataplane(dataplane)
            .expect("focused dataplane");
    local
        .register_node_dataplane(focused.clone())
        .await
        .expect("register joining dataplane");
    klights_leader_rpc::server::publish_joining_node_external_ip(
        query.as_ref(),
        status.as_ref(),
        &focused,
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
            .any(|address| {
                address["type"] == "ExternalIP" && address["address"] == "192.0.2.80"
            }),
        "the Raft-routed metadata CAS must preserve the atomic ExternalIP projection"
    );
    assert_eq!(stored.uid, node_uid);
    assert!(
        status
            .requests
            .lock()
            .expect("recording Node lifecycle status mutex poisoned")
            .is_empty(),
        "the post-CAS publisher must not emit a redundant status command"
    );
}

#[tokio::test]
async fn get_metadata_rpc_rejects_missing_node_client_certificate() {
    let db = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    crate::bootstrap::composition_tests::leader_rpc::support::ensure_cluster_metadata(db.as_ref())
        .await
        .unwrap();
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let service = Arc::new(
        crate::bootstrap::composition_tests::leader_rpc::support::replication_service(
            db.clone(),
            supervisor,
        ),
    );
    let grpc = super::support::GrpcReplicationServer::new(service, db);

    let status = grpc
        .get_metadata(tonic::Request::new(MetadataRequest {}))
        .await
        .expect_err("metadata must reject requests without a node client certificate");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
}

#[tokio::test]
async fn get_metadata_rpc_rejects_bootstrap_token_after_join_bootstrap() {
    let db = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    crate::bootstrap::composition_tests::leader_rpc::support::ensure_cluster_metadata(db.as_ref())
        .await
        .unwrap();
    let token =
        crate::bootstrap::composition_tests::leader_rpc::support::ensure_worker_bootstrap_token(
            db.as_ref(),
        )
        .await
        .unwrap();
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let service = Arc::new(
        crate::bootstrap::composition_tests::leader_rpc::support::replication_service(
            db.clone(),
            supervisor,
        ),
    );
    let grpc = super::support::GrpcReplicationServer::new(service, db);

    let status = grpc
        .get_metadata(request_with_join_token(MetadataRequest {}, &token))
        .await
        .expect_err("bootstrap token must not authenticate steady-state metadata RPC");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
}

#[tokio::test]
async fn get_metadata_rpc_accepts_node_client_cert_without_bootstrap_token() {
    let db = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    crate::bootstrap::composition_tests::leader_rpc::support::ensure_cluster_metadata(db.as_ref())
        .await
        .unwrap();
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let service = Arc::new(
        crate::bootstrap::composition_tests::leader_rpc::support::replication_service(
            db.clone(),
            supervisor,
        ),
    );
    let grpc = super::support::GrpcReplicationServer::new(service, db);

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
    let db = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    crate::bootstrap::composition_tests::leader_rpc::support::ensure_cluster_metadata(db.as_ref())
        .await
        .unwrap();
    let token =
        crate::bootstrap::composition_tests::leader_rpc::support::ensure_worker_bootstrap_token(
            db.as_ref(),
        )
        .await
        .unwrap();
    let tracker = Arc::new(klights_controllers::node_lease::NodeLeaseTracker::new_at(
        chrono::DateTime::parse_from_rfc3339("2026-05-25T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc),
    ));
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let service = Arc::new(
        crate::bootstrap::composition_tests::leader_rpc::support::replication_service(
            db.clone(),
            supervisor,
        ),
    );
    let grpc = super::support::GrpcReplicationServer::new_with_node_lease_tracker(
        service,
        db,
        tracker.clone(),
    );

    let status = grpc
        .renew_node_lease(request_with_join_token(
            klights_internal_protobuf::RenewNodeLeaseRequest {
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
    let wall_time = chrono::DateTime::parse_from_rfc3339("2040-02-03T04:05:06Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let db = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    crate::bootstrap::composition_tests::leader_rpc::support::ensure_cluster_metadata(db.as_ref())
        .await
        .unwrap();
    let before_rv = db.get_current_resource_version().await.unwrap();
    let tracker = Arc::new(klights_controllers::node_lease::NodeLeaseTracker::new_at(
        wall_time,
    ));
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let service = Arc::new(
        crate::bootstrap::composition_tests::leader_rpc::support::replication_service(
            db.clone(),
            supervisor,
        ),
    );
    let grpc = super::support::GrpcReplicationServer::new_with_node_lease_tracker(
        service,
        db.clone(),
        tracker.clone(),
    )
    .with_wall_clock(Arc::new(move || wall_time));

    let renew_time = klights_cluster_core::k8s_time::format_time(wall_time);
    grpc.renew_node_lease(request_with_node_client_cert(
        klights_internal_protobuf::RenewNodeLeaseRequest {
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
    let db = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    crate::bootstrap::composition_tests::leader_rpc::support::ensure_cluster_metadata(db.as_ref())
        .await
        .unwrap();
    let tracker = Arc::new(klights_controllers::node_lease::NodeLeaseTracker::new_at(
        chrono::DateTime::parse_from_rfc3339("2026-05-25T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc),
    ));
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let service = Arc::new(
        crate::bootstrap::composition_tests::leader_rpc::support::replication_service(
            db.clone(),
            supervisor,
        ),
    );
    let (_is_leader_tx, is_leader_rx) = tokio::sync::watch::channel(false);
    let grpc = super::support::GrpcReplicationServer::new_with_node_lease_tracker(
        service,
        db,
        tracker.clone(),
    )
    .with_leader_gate(is_leader_rx);

    let status = grpc
        .renew_node_lease(request_with_node_client_cert(
            klights_internal_protobuf::RenewNodeLeaseRequest {
                node_name: "worker-1".to_string(),
                renew_time: "2026-05-25T00:00:10Z".to_string(),
                lease_duration_seconds: 50,
            },
            "worker-1",
        ))
        .await
        .expect_err("follower must not accept worker lease renewals");

    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert_eq!(status.message(), "not current leader authority");
    assert!(
        tracker.observed("worker-1").await.is_none(),
        "follower-local lease tracker must not be updated"
    );
}

#[tokio::test]
async fn sign_controlplane_csr_sends_private_key_material_to_cp_and_replica() {
    for node_name in ["mn-controlplane2", "mn-replica"] {
        let db = Arc::new(
            klights::datastore::sqlite::Datastore::new_in_memory()
                .await
                .unwrap(),
        );
        create_scoped_token_for_test(
            db.as_ref(),
            "123456.fedcba9876543210",
            crate::bootstrap::composition_tests::leader_rpc::support::BootstrapTokenScope::Controlplane,
        )
        .await;
        let data_root = tempfile::tempdir().unwrap();
        let namespace = data_root.path().to_string_lossy().to_string();
        let grpc = grpc_test_server_with_signing_ca(db, &namespace).await;
        let (_, csr_pem) = klights_auth::cert::generate_server_csr(
            "10.43.0.0/16",
            "10.50.4.0/24",
            Some("10.99.0.14"),
            node_name,
            None,
            time::OffsetDateTime::now_utc(),
        )
        .unwrap();
        let mut request =
            tonic::Request::new(klights_internal_protobuf::SignControlplaneCsrRequest {
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
    let db = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    // Only a *worker*-scoped token exists; the supplied token can never be a
    // valid controlplane join token.
    create_scoped_token_for_test(
        db.as_ref(),
        "abcdef.0123456789abcdef",
        crate::bootstrap::composition_tests::leader_rpc::support::BootstrapTokenScope::Worker,
    )
    .await;
    let data_root = tempfile::tempdir().unwrap();
    let namespace = data_root.path().to_string_lossy().to_string();
    let grpc = grpc_test_server_with_signing_ca(db, &namespace).await;
    let (_, csr_pem) = klights_auth::cert::generate_server_csr(
        "10.43.0.0/16",
        "10.50.4.0/24",
        Some("10.99.0.14"),
        "worker-1",
        None,
        time::OffsetDateTime::now_utc(),
    )
    .unwrap();
    let mut request = request_with_node_client_cert(
        klights_internal_protobuf::SignControlplaneCsrRequest {
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
    let db: DatastoreHandle = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    crate::bootstrap::composition_tests::leader_rpc::support::ensure_cluster_metadata(db.as_ref())
        .await
        .unwrap();
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let service = Arc::new(
        crate::bootstrap::composition_tests::leader_rpc::support::replication_service(
            db.clone(),
            supervisor,
        ),
    );
    let (_is_leader_tx, is_leader_rx) = tokio::sync::watch::channel(true);
    let grpc = super::support::GrpcReplicationServer::new(service, db)
        .with_controlplane_join_handler(Arc::new(NonMemberControlplaneJoinHandler))
        .with_leader_gate(is_leader_rx);

    let request = request_with_node_client_cert(
        klights_internal_protobuf::JoinAsControlplaneRequest {
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
            command_codec_version: 0,
            storage_incarnation: "00000000-0000-4000-8000-000000000001".to_string(),
            storage_log_attestation: Some(
                klights_internal_protobuf::RaftStorageAttestation::default(),
            ),
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
    let db: DatastoreHandle = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    crate::bootstrap::composition_tests::leader_rpc::support::ensure_cluster_metadata(db.as_ref())
        .await
        .unwrap();
    create_scoped_token_for_test(
        db.as_ref(),
        "123456.fedcba9876543210",
        crate::bootstrap::composition_tests::leader_rpc::support::BootstrapTokenScope::Controlplane,
    )
    .await;
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let service = Arc::new(
        crate::bootstrap::composition_tests::leader_rpc::support::replication_service(
            db.clone(),
            supervisor,
        ),
    );
    let (_is_leader_tx, is_leader_rx) = tokio::sync::watch::channel(true);
    let grpc = super::support::GrpcReplicationServer::new(service, db)
        .with_controlplane_join_handler(Arc::new(NonMemberControlplaneJoinHandler))
        .with_leader_gate(is_leader_rx);

    let join_request = klights_internal_protobuf::JoinAsControlplaneRequest {
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
        command_codec_version: 0,
        storage_incarnation: "00000000-0000-4000-8000-000000000002".to_string(),
        storage_log_attestation: Some(klights_internal_protobuf::RaftStorageAttestation::default()),
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
        Some(klights_internal_protobuf::join_as_controlplane_response::Result::Accepted(_))
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
    klights_cluster_core::raft_node_id_for_node_name(node_name)
}

#[tokio::test]
async fn mount_service_accepts_replication_router_prefix() {
    let db = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    crate::bootstrap::composition_tests::leader_rpc::support::ensure_cluster_metadata(db.as_ref())
        .await
        .unwrap();
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let service = Arc::new(
        crate::bootstrap::composition_tests::leader_rpc::support::replication_service(
            db.clone(),
            supervisor,
        ),
    );
    let _router = super::support::mount_service(
        axum::Router::new(),
        service,
        db,
        klights_leader_rpc::transport_policy::GrpcTransportPolicy::shared_default(),
    );
}

#[tokio::test]
async fn mounted_router_does_not_send_plain_rest_unknown_paths_to_grpc() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let db = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    crate::bootstrap::composition_tests::leader_rpc::support::ensure_cluster_metadata(db.as_ref())
        .await
        .unwrap();
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let service = Arc::new(
        crate::bootstrap::composition_tests::leader_rpc::support::replication_service(
            db.clone(),
            supervisor,
        ),
    );
    let app = super::support::mount_service(
        axum::Router::new(),
        service,
        db,
        klights_leader_rpc::transport_policy::GrpcTransportPolicy::shared_default(),
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
    let db: DatastoreHandle = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    crate::bootstrap::composition_tests::leader_rpc::support::ensure_cluster_metadata(db.as_ref())
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
    let db: DatastoreHandle = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
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
    let db: DatastoreHandle = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    let (endpoint, _service, handle) = grpc_test_server(db.clone()).await;
    let mut join = valid_join();
    join.token = "wrong-token".to_string();
    join.node_name = "bad-node".to_string();

    let (_tx, mut inbound) = open_connect(&endpoint, join).await;
    let first = inbound.message().await.unwrap().unwrap();
    match first.payload.unwrap() {
        klights_internal_protobuf::leader_message::Payload::JoinResponse(response) => {
            assert!(matches!(
                response.result,
                Some(klights_internal_protobuf::join_response::Result::Rejected(
                    _
                ))
            ));
        }
        other => panic!("expected JoinResponse, got {other:?}"),
    }
    assert!(db.get_node_dataplane("bad-node").await.unwrap().is_none());
    handle.abort();
}

#[tokio::test]
async fn connect_persists_dataplane_endpoint_from_observed_peer_ip() {
    let db: DatastoreHandle = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
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
        klights_internal_protobuf::leader_message::Payload::JoinResponse(
            klights_internal_protobuf::JoinResponse {
                result: Some(klights_internal_protobuf::join_response::Result::Accepted(
                    _
                )),
            }
        )
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
    let db: DatastoreHandle = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
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
async fn connect_accepts_valid_join_and_returns_dataplane_peers() {
    let db: DatastoreHandle = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    crate::bootstrap::composition_tests::leader_rpc::support::ensure_cluster_metadata(db.as_ref())
        .await
        .unwrap();
    db.allocate_node_subnet("leader", "10.42.0.0/16", "192.0.2.1")
        .await
        .unwrap();
    db.update_node_dataplane(
        klights_cluster_store::DataplanePeerMetadata::try_new(
            "leader".to_string(),
            klights_cluster_store::DataplaneMode::Root,
            klights_cluster_store::DataplaneEncryption::Enabled,
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
            Some("192.0.2.1".to_string()),
            Some(51_820),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let (endpoint, _service, handle) =
        grpc_test_server_with_node_cert(db.clone(), "worker-1").await;
    let mut join = valid_join();
    join.token.clear();

    let (_tx, mut inbound) = open_connect(&endpoint, join).await;
    let first = inbound.message().await.unwrap().unwrap();
    match first.payload.unwrap() {
        klights_internal_protobuf::leader_message::Payload::JoinResponse(
            klights_internal_protobuf::JoinResponse {
                result: Some(klights_internal_protobuf::join_response::Result::Accepted(accepted)),
            },
        ) => {
            assert_eq!(accepted.peers.len(), 1);
            assert_eq!(accepted.peers[0].node_name, "leader");
            assert_eq!(accepted.peers[0].pod_cidr, "10.42.0.0/24");
            assert_eq!(accepted.peers[0].endpoint, "192.0.2.1");
        }
        other => panic!("expected accepted JoinResponse, got {other:?}"),
    }
    handle.abort();
}

#[tokio::test]
async fn connect_follower_progress_heartbeats_never_regress_below_initial_rv() {
    let db: DatastoreHandle = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    crate::bootstrap::composition_tests::leader_rpc::support::ensure_cluster_metadata(db.as_ref())
        .await
        .unwrap();
    let (endpoint, _service, follower_progress, handle) =
        grpc_test_server_full_with_node_cert_and_current_rv(
            db.clone(),
            None,
            None,
            Some("worker-1".to_string()),
            100,
        )
        .await;
    let mut join = valid_join();
    join.token.clear();

    let (_tx, mut inbound) = open_connect(&endpoint, join).await;
    let first = inbound.message().await.unwrap().unwrap();
    let accepted_rv = match first.payload.unwrap() {
        klights_internal_protobuf::leader_message::Payload::JoinResponse(
            klights_internal_protobuf::JoinResponse {
                result: Some(klights_internal_protobuf::join_response::Result::Accepted(accepted)),
            },
        ) => accepted.current_rv,
        other => panic!("expected accepted JoinResponse, got {other:?}"),
    };

    let initial = inbound.message().await.unwrap().unwrap();
    let initial_rv = match initial.payload.unwrap() {
        klights_internal_protobuf::leader_message::Payload::StreamItem(
            klights_internal_protobuf::StreamItem {
                item: Some(klights_internal_protobuf::stream_item::Item::Heartbeat(heartbeat)),
            },
        ) => heartbeat.current_rv,
        other => panic!("expected initial follower progress heartbeat, got {other:?}"),
    };
    assert_eq!(initial_rv, accepted_rv);
    assert!(
        accepted_rv > 1,
        "seeded datastore must start above the progress hub"
    );

    follower_progress.advance(accepted_rv - 1);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), inbound.message())
            .await
            .is_err(),
        "a progress wake below the initial accepted RV must not emit a regressing heartbeat"
    );
    follower_progress.advance(accepted_rv + 1);
    let advanced = inbound.message().await.unwrap().unwrap();
    let advanced_rv = match advanced.payload.unwrap() {
        klights_internal_protobuf::leader_message::Payload::StreamItem(
            klights_internal_protobuf::StreamItem {
                item: Some(klights_internal_protobuf::stream_item::Item::Heartbeat(heartbeat)),
            },
        ) => heartbeat.current_rv,
        other => panic!("expected advanced follower progress heartbeat, got {other:?}"),
    };
    assert_eq!(advanced_rv, accepted_rv + 1);
    handle.abort();
}

#[tokio::test]
async fn accepted_legacy_controlplane_rejoin_without_snapshot_persists_dataplane_metadata() {
    let db: DatastoreHandle = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    crate::bootstrap::composition_tests::leader_rpc::support::ensure_cluster_metadata(db.as_ref())
        .await
        .unwrap();
    let (endpoint, _service, _progress, handle) = grpc_test_server_full_with_node_cert(
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
    let request = tonic::Request::new(klights_internal_protobuf::JoinAsControlplaneRequest {
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
        command_codec_version: 0,
        storage_incarnation: "00000000-0000-4000-8000-000000000002".to_string(),
        storage_log_attestation: Some(klights_internal_protobuf::RaftStorageAttestation::default()),
    });

    let response = client
        .join_as_controlplane(request)
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        response.result,
        Some(klights_internal_protobuf::join_as_controlplane_response::Result::Accepted(_))
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
    let db: DatastoreHandle = Arc::new(
        klights::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap(),
    );
    crate::bootstrap::composition_tests::leader_rpc::support::ensure_cluster_metadata(db.as_ref())
        .await
        .unwrap();
    let join_handler = Arc::new(RecordingControlplaneJoinHandler::default());
    let (endpoint, _service, _progress, handle) = grpc_test_server_full_with_node_cert(
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
    let request = tonic::Request::new(klights_internal_protobuf::JoinAsControlplaneRequest {
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
        command_codec_version: 0,
        storage_incarnation: "00000000-0000-4000-8000-000000000002".to_string(),
        storage_log_attestation: Some(klights_internal_protobuf::RaftStorageAttestation::default()),
    });

    let response = client
        .join_as_controlplane(request)
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        response.result,
        Some(klights_internal_protobuf::join_as_controlplane_response::Result::Accepted(_))
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
                klights_leader_rpc::server::validate_controlplane_node_registration(
                    test_node_registration_proto("joinhash1",)
                )
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
    let sqlite = klights::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let db: DatastoreHandle = Arc::new(sqlite.clone());
    let _token = {
        crate::bootstrap::composition_tests::leader_rpc::support::ensure_cluster_metadata(
            db.as_ref(),
        )
        .await
        .unwrap();
        crate::bootstrap::composition_tests::leader_rpc::support::ensure_worker_bootstrap_token(
            db.as_ref(),
        )
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
    let dispatcher =
        crate::bootstrap::composition_tests::leader_rpc::support::controller_dispatcher_for_test(
            &sqlite,
        );
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let service = Arc::new(
        crate::bootstrap::composition_tests::leader_rpc::support::replication_service(
            db.clone(),
            supervisor,
        ),
    );
    let grpc = super::support::GrpcReplicationServer::new_with_controller_dispatcher(
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
    let payload =
        crate::bootstrap::composition_tests::leader_rpc::support::OutboxPayload::from_command(
            command,
        )
        .encode_protobuf()
        .unwrap();
    let response = grpc
        .apply_outbox(request_with_node_client_cert(
            klights_internal_protobuf::ApplyOutboxRequest {
                idempotency_key: "pod-status-web-worker".to_string(),
                operation: klights_kubelet::node_outbox::payload::OutboxOperation::PodStatus
                    .as_str()
                    .to_string(),
                payload_proto: payload,
                authoring_node: "worker-1".to_string(),
                client_id: "client".to_string(),
                stream_id: 1,
                stream_seq: 1,
                codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
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
    let keys = klights_reconcile_api::ControllerDispatcherPort::pending_reconcile_keys(
        dispatcher.as_ref(),
    )
    .await;
    assert!(
        keys.iter().any(|key| {
            key.api_version() == "v1"
                && key.kind() == "Service"
                && key.namespace() == Some("default")
                && key.name() == "web"
        }),
        "outbox-applied worker pod status must enqueue matching Services on the leader: {keys:?}"
    );
    let service = db
        .get_resource("v1", "Service", Some("default"), "web")
        .await
        .unwrap()
        .expect("Service row");
    let composition =
        crate::bootstrap::composition_tests::leader_rpc::support::IntegrationLeaderRpcComposition::new(
            db.clone(),
        );
    composition
        .reconcile_service_endpoints(
            klights_controllers::endpoints::ServiceEndpointBatchReconcileRequest {
                service_name: "web",
                service_uid: &service.uid,
                namespace: "default",
                selector: service.data.pointer("/spec/selector"),
                service_ports: service.data.pointer("/spec/ports"),
                publish_not_ready: false,
            },
        )
        .await
        .unwrap();
    let endpoints = db
        .get_resource("v1", "Endpoints", Some("default"), "web")
        .await
        .unwrap()
        .expect("JSON Endpoints row after protobuf RPC status");
    let slice = db
        .get_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("default"),
            "web-klights",
        )
        .await
        .unwrap()
        .expect("JSON EndpointSlice row after protobuf RPC status");
    assert_eq!(
        endpoints.data.pointer("/subsets/0/addresses/0/ip"),
        Some(&serde_json::json!("10.43.1.2"))
    );
    assert_eq!(
        slice.data.pointer("/endpoints/0/conditions/ready"),
        Some(&serde_json::json!(true))
    );
}
