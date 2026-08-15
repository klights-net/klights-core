use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

use crate::bootstrap::composition_tests::native_api::support;

fn remote_dataplane(node_name: &str) -> klights_leader_api::NetworkDataplane {
    klights_leader_api::NetworkDataplane::try_new(
        node_name,
        klights_leader_api::NetworkNodeMode::Root,
        klights_leader_api::DataplaneEncryption::Direct,
        None,
        std::net::Ipv4Addr::LOCALHOST.into(),
        None,
    )
    .unwrap()
}

#[tokio::test]
async fn test_remote_pod_log_follow_keeps_http_body_open_until_terminal_frame() {
    let state = support::build_test_app_state_with_operational_endpoints().await;
    let remote_node = format!("{}-worker", state.node_name());
    let mut follower = state
        .register_integration_follower(remote_dataplane(&remote_node))
        .await
        .unwrap();
    state
        .resource_store()
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "remote-log-follow",
            json!({
                "apiVersion": "v1", "kind": "Pod",
                "metadata": {
                    "name": "remote-log-follow", "namespace": "default",
                    "uid": "remote-log-follow-uid"
                },
                "spec": {
                    "nodeName": remote_node,
                    "containers": [{"name": "main", "image": "busybox"}]
                },
                "status": {"phase": "Running"}
            }),
        )
        .await
        .unwrap();

    let response = state
        .router()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces/default/pods/remote-log-follow/log?container=main&follow=true&tailLines=200")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    if response.status() != StatusCode::OK {
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        panic!(
            "remote log follow returned {status}: {}",
            String::from_utf8_lossy(&body)
        );
    }
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .unwrap(),
        "text/plain; charset=utf-8"
    );
    let Some(klights_node_api::FollowerControlMessage::PodLog(request)) = follower.recv().await
    else {
        panic!("expected remote Pod log follow request");
    };
    assert!(request.follow);
    assert_eq!(request.request.options().tail_lines(), Some(200));

    let mut body_task =
        tokio::spawn(async move { to_bytes(response.into_body(), usize::MAX).await });
    follower
        .complete_node_log_event(klights_node_api::RoutedNodeLogEvent {
            request_id: request.request_id.clone(),
            event: klights_node_api::NodeLogEvent::data(b"tail ".to_vec()),
        })
        .await
        .unwrap();
    follower
        .complete_node_log_event(klights_node_api::RoutedNodeLogEvent {
            request_id: request.request_id.clone(),
            event: klights_node_api::NodeLogEvent::data(b"\xf6\n".to_vec()),
        })
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut body_task)
            .await
            .is_err(),
        "remote follow body must remain open after a non-terminal log frame"
    );
    follower
        .complete_node_log_event(klights_node_api::RoutedNodeLogEvent {
            request_id: request.request_id,
            event: klights_node_api::NodeLogEvent::terminal(),
        })
        .await
        .unwrap();
    let body = tokio::time::timeout(std::time::Duration::from_secs(2), &mut body_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(body.as_ref(), b"tail \xf6\n");
}

async fn remote_exec_fixture() -> (
    crate::bootstrap::composition_tests::native_api::support::IntegrationFollowerSession,
    k8s_native_service::test_support::streaming::RemoteExecSyncWebSocketFixture,
) {
    let state = support::build_test_app_state_with_operational_endpoints().await;
    let follower = state
        .register_integration_follower(remote_dataplane("worker-1"))
        .await
        .unwrap();
    let exec = state.integration_remote_exec_sync().unwrap();
    (follower, exec)
}

fn exec_target() -> k8s_native_service::streaming::ExecTarget {
    k8s_native_service::streaming::ExecTarget {
        namespace: "default".to_string(),
        pod_name: "worker-pod".to_string(),
        container_id: "container-id".to_string(),
        command: vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            "echo ok".to_string(),
        ],
    }
}

#[tokio::test]
async fn test_remote_exec_sync_websocket_closes_after_terminal_status_without_client_close() {
    use futures::StreamExt as _;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::protocol::Role;

    let (mut follower, exec) = remote_exec_fixture().await;
    let (server_io, client_io) = tokio::io::duplex(4096);
    let mut client_ws =
        tokio_tungstenite::WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
    let server = tokio::spawn(exec.run(
        server_io,
        exec_target(),
        "v4.channel.k8s.io".to_string(),
        "worker-1".to_string(),
    ));

    let Some(klights_node_api::FollowerControlMessage::NodeExecSync(request)) =
        follower.recv().await
    else {
        panic!("expected remote exec-sync request");
    };
    follower
        .complete_node_exec_sync(klights_node_api::RoutedNodeExecSyncResponse {
            request_id: request.request_id,
            result: klights_node_api::NodeExecSyncResult::success(
                b"worker-stdout\n".to_vec(),
                Vec::new(),
                0,
            ),
        })
        .await
        .unwrap();

    let stdout = tokio::time::timeout(std::time::Duration::from_secs(1), client_ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Message::Binary(stdout) = stdout else {
        panic!("expected stdout binary frame");
    };
    assert_eq!(stdout.first(), Some(&1));
    assert_eq!(&stdout[1..], b"worker-stdout\n");

    let status = tokio::time::timeout(std::time::Duration::from_secs(1), client_ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Message::Binary(status) = status else {
        panic!("expected terminal status binary frame");
    };
    assert_eq!(status.first(), Some(&3));
    let status: serde_json::Value = serde_json::from_slice(&status[1..]).unwrap();
    assert_eq!(status["status"], "Success");

    let close = tokio::time::timeout(std::time::Duration::from_millis(200), client_ws.next())
        .await
        .expect("server must close after terminal status");
    assert!(matches!(close, Some(Ok(Message::Close(_)))));
    server.await.unwrap();
}

#[tokio::test]
async fn test_remote_exec_sync_websocket_waits_for_peer_close_reply() {
    use futures::{SinkExt as _, StreamExt as _};
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::protocol::Role;

    let (mut follower, exec) = remote_exec_fixture().await;
    let (server_io, client_io) = tokio::io::duplex(4096);
    let mut client_ws =
        tokio_tungstenite::WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
    let mut server = tokio::spawn(exec.run(
        server_io,
        exec_target(),
        "v5.channel.k8s.io".to_string(),
        "worker-1".to_string(),
    ));

    let Some(klights_node_api::FollowerControlMessage::NodeExecSync(request)) =
        follower.recv().await
    else {
        panic!("expected remote exec-sync request");
    };
    follower
        .complete_node_exec_sync(klights_node_api::RoutedNodeExecSyncResponse {
            request_id: request.request_id,
            result: klights_node_api::NodeExecSyncResult::success(
                b"worker-stdout\n".to_vec(),
                Vec::new(),
                0,
            ),
        })
        .await
        .unwrap();

    for channel in [1u8, 3u8] {
        let message = tokio::time::timeout(std::time::Duration::from_secs(1), client_ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let Message::Binary(frame) = message else {
            panic!("expected binary frame on channel {channel}");
        };
        assert_eq!(frame.first(), Some(&channel));
    }
    let close = tokio::time::timeout(std::time::Duration::from_secs(1), client_ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(close, Message::Close(_)));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut server)
            .await
            .is_err(),
        "server returned before reading the peer close reply"
    );
    client_ws.flush().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), server)
        .await
        .expect("server did not finish after peer close")
        .unwrap();
}

#[tokio::test]
async fn service_delete_denied_leaves_service_endpoints_allocations_and_hooks() {
    let state = support::build_test_app_state_with_authorizer(std::sync::Arc::new(
        klights_auth::authorizer::DenyAuthorizer,
    ))
    .await;
    let db = state.resource_store();
    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "deny-svc",
        json!({
            "apiVersion": "v1", "kind": "Service",
            "metadata": {"name": "deny-svc", "namespace": "default", "uid": "svc-uid-123"},
            "spec": {"clusterIP": "10.0.0.100", "ports": [{"port": 80}]}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "deny-svc",
        json!({
            "apiVersion": "v1", "kind": "Endpoints",
            "metadata": {"name": "deny-svc", "namespace": "default"}
        }),
    )
    .await
    .unwrap();

    let response = state
        .router()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/namespaces/default/services/deny-svc")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let service = db
        .get_resource("v1", "Service", Some("default"), "deny-svc")
        .await
        .unwrap()
        .expect("denied Service delete must not remove the Service");
    assert_eq!(service.data["metadata"]["uid"], "svc-uid-123");
    assert!(
        db.get_resource("v1", "Endpoints", Some("default"), "deny-svc")
            .await
            .unwrap()
            .is_some(),
        "denied Service delete must not remove Endpoints"
    );
}

#[tokio::test]
async fn pod_create_denied_does_not_persist() {
    let state = support::build_test_app_state_with_authorizer(std::sync::Arc::new(
        klights_auth::authorizer::DenyAuthorizer,
    ))
    .await;
    let db = state.resource_store();
    let response = state
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces/default/pods")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "apiVersion": "v1", "kind": "Pod",
                        "metadata": {"name": "denied-pod", "namespace": "default"}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(
        db.get_resource("v1", "Pod", Some("default"), "denied-pod")
            .await
            .unwrap()
            .is_none(),
        "denied Pod create must not persist a Pod row"
    );
}

async fn seed_service_endpoints(
    db: &klights_cluster_datastore::test_support::ResourceTestStore,
    subsets: serde_json::Value,
) {
    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "svc",
        json!({
            "apiVersion": "v1", "kind": "Service",
            "metadata": {"name": "svc", "namespace": "default"},
            "spec": {"ports": [{"port": 80, "targetPort": 80}]}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "svc",
        json!({
            "apiVersion": "v1", "kind": "Endpoints",
            "metadata": {"name": "svc", "namespace": "default"},
            "subsets": subsets
        }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn service_proxy_fails_over_to_reachable_endpoint() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let live = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let live_port = live.local_addr().unwrap().port();
    let live_server = tokio::spawn(async move {
        let (mut stream, _) = live.accept().await.unwrap();
        let mut request = [0u8; 1024];
        let _ = stream.read(&mut request).await;
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 5\r\n\r\nalive",
            )
            .await
            .unwrap();
    });
    let dead = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_port = dead.local_addr().unwrap().port();
    drop(dead);

    let state = support::build_test_app_state().await;
    seed_service_endpoints(
        &state.resource_store(),
        json!([
            {"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": dead_port}]},
            {"addresses": [{"ip": "127.0.0.1"}], "ports": [{"port": live_port}]}
        ]),
    )
    .await;

    let response = state
        .router()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces/default/services/svc/proxy/")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), 1024).await.unwrap().as_ref(),
        b"alive"
    );
    live_server.await.unwrap();
}

#[tokio::test]
async fn service_proxy_allows_slow_valid_upstream_header_response() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let slow = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let slow_port = slow.local_addr().unwrap().port();
    let slow_server = tokio::spawn(async move {
        let (mut stream, _) = slow.accept().await.unwrap();
        let mut request = [0u8; 4096];
        let _ = stream.read(&mut request).await;
        tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 7\r\n\r\nupdated",
            )
            .await
            .unwrap();
    });

    let state = support::build_test_app_state().await;
    seed_service_endpoints(
        &state.resource_store(),
        json!([{
            "addresses": [{"ip": "127.0.0.1"}],
            "ports": [{"port": slow_port}]
        }]),
    )
    .await;
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(6),
        state.router().oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces/default/services/svc/proxy/guestbook?cmd=set")
                .body(Body::empty())
                .unwrap(),
        ),
    )
    .await
    .expect("service proxy should wait for a valid upstream response")
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), 1024).await.unwrap().as_ref(),
        b"updated"
    );
    slow_server.await.unwrap();
}

async fn occ_setup_pod() -> (axum::Router, String) {
    let app = support::build_test_router().await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces/default/pods")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "apiVersion": "v1", "kind": "Pod",
                        "metadata": {"name": "occ-stale", "namespace": "default"},
                        "spec": {"containers": [{"name": "c", "image": "nginx"}]}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let pod: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    let initial_rv = pod["metadata"]["resourceVersion"]
        .as_str()
        .expect("created Pod carries a resourceVersion")
        .to_string();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/api/v1/namespaces/default/pods/occ-stale")
                .header("content-type", "application/merge-patch+json")
                .body(Body::from(
                    json!({"metadata": {"labels": {"bump": "yes"}}}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    (app, initial_rv)
}

#[tokio::test]
async fn pod_status_patch_with_stale_client_rv_returns_409() {
    let (app, stale_rv) = occ_setup_pod().await;
    let response = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/api/v1/namespaces/default/pods/occ-stale/status")
                .header("content-type", "application/merge-patch+json")
                .body(Body::from(
                    json!({
                        "metadata": {"resourceVersion": stale_rv},
                        "status": {"phase": "Running"}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn pod_status_put_with_stale_client_rv_returns_409() {
    let (app, stale_rv) = occ_setup_pod().await;
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/v1/namespaces/default/pods/occ-stale/status")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "apiVersion": "v1", "kind": "Pod",
                        "metadata": {
                            "name": "occ-stale", "namespace": "default",
                            "resourceVersion": stale_rv
                        },
                        "status": {"phase": "Running"}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn pod_status_patch_without_client_rv_succeeds() {
    let (app, _) = occ_setup_pod().await;
    let response = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/api/v1/namespaces/default/pods/occ-stale/status")
                .header("content-type", "application/merge-patch+json")
                .body(Body::from(
                    json!({"status": {"phase": "Succeeded"}}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let pod: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(pod["status"]["phase"], "Succeeded");
}

#[tokio::test]
async fn pod_status_put_without_client_rv_succeeds() {
    let (app, _) = occ_setup_pod().await;
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/v1/namespaces/default/pods/occ-stale/status")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "apiVersion": "v1", "kind": "Pod",
                        "metadata": {"name": "occ-stale", "namespace": "default"},
                        "status": {"phase": "Failed"}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let pod: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(pod["status"]["phase"], "Failed");
}
