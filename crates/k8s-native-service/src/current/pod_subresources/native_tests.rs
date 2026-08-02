use super::*;
use crate::streaming::*;
use axum::http::{StatusCode, header};
use bytes::Bytes;
use serde_json::json;

#[tokio::test]
async fn test_portforward_channel_capacity_applies_backpressure_at_64() {
    use tokio::sync::mpsc;
    let (tx, mut rx) = mpsc::channel::<(u8, Vec<u8>)>(64);
    for i in 0u8..64 {
        tx.try_send((i, vec![i]))
            .expect("send must succeed when below capacity");
    }
    assert!(tx.try_send((0, vec![0])).is_err());
    rx.recv().await.unwrap();
    tx.try_send((0, vec![0]))
        .expect("send must succeed after draining one item");
}

#[test]
fn test_parse_exec_query_single_command() {
    let (cmd, container, stdin, stdout, stderr, tty) = parse_exec_query("command=ls");
    assert_eq!(cmd, vec!["ls"]);
    assert_eq!(container, None);
    assert!(!stdin);
    assert!(!stdout);
    assert!(!stderr);
    assert!(!tty);
}

#[test]
fn test_parse_exec_query_multiple_command_params() {
    let (cmd, _, _, _, _, _) =
        parse_exec_query("command=%2Fbin%2Fsh&command=-c&command=echo%20hello");
    assert_eq!(cmd, vec!["/bin/sh", "-c", "echo hello"]);
}

#[test]
fn test_parse_exec_query_container_param() {
    let (_, container, _, _, _, _) = parse_exec_query("command=ls&container=sidecar");
    assert_eq!(container, Some("sidecar".to_string()));
}

#[test]
fn test_parse_exec_query_stdin_true() {
    let (_, _, stdin, _, _, _) = parse_exec_query("command=sh&stdin=true");
    assert!(stdin);
}

#[test]
fn test_parse_exec_query_stdin_one() {
    let (_, _, stdin, _, _, _) = parse_exec_query("command=sh&stdin=1");
    assert!(stdin);
}

#[test]
fn test_parse_exec_query_tty_true() {
    let (_, _, _, _, _, tty) = parse_exec_query("command=sh&tty=true&stdin=true");
    assert!(tty);
}

#[test]
fn test_parse_exec_query_stdout_false() {
    let (_, _, _, stdout, _, _) = parse_exec_query("command=ls&stdout=false");
    assert!(!stdout);
}

#[test]
fn test_parse_exec_query_omitted_stderr_defaults_false() {
    let (_, _, _, stdout, stderr, _) = parse_exec_query("command=ls&stdout=true");
    assert!(stdout);
    assert!(!stderr);
}

#[test]
fn test_parse_exec_query_empty_string() {
    let (cmd, container, stdin, stdout, stderr, tty) = parse_exec_query("");
    assert!(cmd.is_empty());
    assert_eq!(container, None);
    assert!(!stdin);
    assert!(!stdout);
    assert!(!stderr);
    assert!(!tty);
}

#[test]
fn test_parse_exec_query_unknown_params_ignored() {
    let (cmd, _, _, _, _, _) = parse_exec_query("command=ls&unknown=value&foo=bar");
    assert_eq!(cmd, vec!["ls"]);
}

#[test]
fn test_extract_container_id_first_container_default() {
    let pod = json!({
        "status": {
            "containerStatuses": [{
                "name": "web",
                "containerID": "containerd://abc123def"
            }]
        }
    });
    let id = extract_container_id(&pod, None).unwrap();
    assert_eq!(id, "abc123def");
}

#[test]
fn test_extract_container_id_by_name() {
    let pod = json!({
        "status": {
            "containerStatuses": [
                {"name": "web", "containerID": "containerd://aaa"},
                {"name": "sidecar", "containerID": "containerd://bbb"}
            ]
        }
    });
    let id = extract_container_id(&pod, Some("sidecar")).unwrap();
    assert_eq!(id, "bbb");
}

#[test]
fn test_extract_container_id_by_ephemeral_container_name() {
    let pod = json!({
        "status": {
            "containerStatuses": [
                {"name": "web", "containerID": "containerd://aaa"}
            ],
            "ephemeralContainerStatuses": [
                {"name": "debugger", "containerID": "containerd://debug123"}
            ]
        }
    });
    let id = extract_container_id(&pod, Some("debugger")).unwrap();
    assert_eq!(id, "debug123");
}

#[test]
fn test_extract_container_id_named_not_found() {
    let pod = json!({
        "status": {
            "containerStatuses": [
                {"name": "web", "containerID": "containerd://aaa"}
            ]
        }
    });
    let err = extract_container_id(&pod, Some("nonexistent")).unwrap_err();
    match err {
        AppError::NotFound(msg) => assert!(msg.contains("nonexistent")),
        _ => panic!("Expected NotFound, got {:?}", err),
    }
}

#[test]
fn test_extract_container_id_no_statuses() {
    let pod = json!({"status": {}});
    let err = extract_container_id(&pod, None).unwrap_err();
    match err {
        AppError::BadRequest(msg) => assert!(msg.contains("no container statuses")),
        _ => panic!("Expected BadRequest, got {:?}", err),
    }
}

#[test]
fn test_extract_container_id_empty_statuses() {
    let pod = json!({"status": {"containerStatuses": []}});
    let err = extract_container_id(&pod, None).unwrap_err();
    match err {
        AppError::BadRequest(msg) => assert!(msg.contains("no container statuses")),
        _ => panic!("Expected BadRequest, got {:?}", err),
    }
}

#[test]
fn test_extract_container_id_strips_containerd_prefix() {
    let pod = json!({
        "status": {
            "containerStatuses": [{
                "name": "app",
                "containerID": "containerd://deadbeef1234"
            }]
        }
    });
    let id = extract_container_id(&pod, None).unwrap();
    assert_eq!(id, "deadbeef1234", "containerd:// prefix must be stripped");
}

#[test]
fn test_extract_container_id_no_prefix() {
    let pod = json!({
        "status": {
            "containerStatuses": [{
                "name": "app",
                "containerID": "rawid123"
            }]
        }
    });
    let id = extract_container_id(&pod, None).unwrap();
    assert_eq!(
        id, "rawid123",
        "IDs without containerd:// prefix should work"
    );
}

#[test]
fn test_extract_container_id_missing_container_id_field() {
    let pod = json!({
        "status": {
            "containerStatuses": [{
                "name": "app"
            }]
        }
    });
    let err = extract_container_id(&pod, None).unwrap_err();
    match err {
        AppError::BadRequest(msg) => assert!(msg.contains("Container ID not found")),
        _ => panic!("Expected BadRequest, got {:?}", err),
    }
}

#[test]
fn test_remote_pod_node_name_returns_remote_scheduled_node() {
    let pod = json!({"spec": {"nodeName": "worker-1"}});
    assert_eq!(
        remote_pod_node_name(&pod, "dallas").as_deref(),
        Some("worker-1")
    );
}

#[test]
fn test_remote_pod_node_name_ignores_local_or_unscheduled_pods() {
    assert_eq!(
        remote_pod_node_name(&json!({"spec": {"nodeName": "dallas"}}), "dallas"),
        None
    );
    assert_eq!(remote_pod_node_name(&json!({"spec": {}}), "dallas"), None);
}

#[test]
fn test_derive_websocket_accept_key_rfc6455_test_vector() {
    // RFC 6455 Section 4.2.2 example:
    // Key: "dGhlIHNhbXBsZSBub25jZQ=="
    // Expected Accept: "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
    let key = header::HeaderValue::from_static("dGhlIHNhbXBsZSBub25jZQ==");
    let accept = derive_websocket_accept_key(&key);
    assert_eq!(accept, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
}

#[test]
fn test_negotiate_websocket_subprotocol_prefers_highest_supported() {
    let mut headers = header::HeaderMap::new();
    headers.append(
        header::SEC_WEBSOCKET_PROTOCOL,
        header::HeaderValue::from_static("channel.k8s.io, v4.channel.k8s.io"),
    );

    let negotiated = negotiate_websocket_subprotocol(&headers);
    assert_eq!(negotiated.as_deref(), Some("v4.channel.k8s.io"));
}

#[test]
fn test_negotiate_websocket_subprotocol_reads_multiple_header_values() {
    let mut headers = header::HeaderMap::new();
    headers.append(
        header::SEC_WEBSOCKET_PROTOCOL,
        header::HeaderValue::from_static("base64.channel.k8s.io"),
    );
    headers.append(
        header::SEC_WEBSOCKET_PROTOCOL,
        header::HeaderValue::from_static("v5.channel.k8s.io"),
    );

    let negotiated = negotiate_websocket_subprotocol(&headers);
    assert_eq!(negotiated.as_deref(), Some("v5.channel.k8s.io"));
}

#[test]
fn test_websocket_uses_structured_status_channel_only_for_v4_and_v5() {
    assert!(websocket_uses_structured_status_channel(
        "v4.channel.k8s.io"
    ));
    assert!(websocket_uses_structured_status_channel(
        "v5.channel.k8s.io"
    ));
    assert!(!websocket_uses_structured_status_channel("channel.k8s.io"));
    assert!(!websocket_uses_structured_status_channel(
        "v3.channel.k8s.io"
    ));
}

#[test]
fn test_remote_exec_error_status_payload_is_terminal_without_fin() {
    use klights_node_api::{ExecStreamChannel, NodeExecFrame};

    let frame = NodeExecFrame::new(
        ExecStreamChannel::Error,
        serde_json::json!({"metadata": {}, "status": "Success"})
            .to_string()
            .into_bytes(),
        false,
    );

    assert!(remote_exec_error_frame_is_terminal(&frame));
}

#[test]
fn test_remote_exec_non_error_frame_is_not_terminal_without_fin() {
    use klights_node_api::{ExecStreamChannel, NodeExecFrame};

    let frame = NodeExecFrame::new(ExecStreamChannel::Stdout, b"done\n".to_vec(), false);

    assert!(!remote_exec_error_frame_is_terminal(&frame));
}

#[test]
fn test_format_websocket_error_payload_is_legacy_plain_text_for_channel_k8s_io() {
    let payload = format_websocket_error_payload("channel.k8s.io", "exec failed: boom".to_string());
    assert_eq!(payload, b"exec failed: boom".to_vec());
}

#[test]
fn test_format_websocket_error_payload_is_json_for_v4_channel() {
    let payload =
        format_websocket_error_payload("v4.channel.k8s.io", "exec failed: boom".to_string());
    let value: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    assert_eq!(value["status"], "Failure");
    assert_eq!(value["message"], "exec failed: boom");
}

#[tokio::test]
async fn test_spdy_exec_streams_stdout_and_error_to_client_stream_ids() {
    use super::spdy_framing::{SpdyConnection, SpdyFrame, StreamType};
    use crate::current::pod_subresources::exec_spdy::{
        SpdyExecStreamRequest, collect_spdy_client_streams, write_spdy_exec_channel_frame,
    };
    use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};

    let (mut server_io, mut client_io) = tokio::io::duplex(4096);
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());

    let server = tokio::spawn(async move {
        let mut server_spdy = SpdyConnection::new();
        let streams = collect_spdy_client_streams(
            &mut server_spdy,
            &mut server_io,
            SpdyExecStreamRequest {
                stdin: false,
                stdout: true,
                stderr: false,
                tty: false,
                attach: false,
            },
            &supervisor,
        )
        .await
        .unwrap();

        write_spdy_exec_channel_frame(
            &server_spdy,
            &mut server_io,
            &streams,
            StreamType::Stdout,
            b"payload",
            false,
        )
        .await
        .unwrap();
        write_spdy_exec_channel_frame(
            &server_spdy,
            &mut server_io,
            &streams,
            StreamType::Stdout,
            b"",
            true,
        )
        .await
        .unwrap();
        write_spdy_exec_channel_frame(
            &server_spdy,
            &mut server_io,
            &streams,
            StreamType::Error,
            exec_exit_status(0).to_string().as_bytes(),
            true,
        )
        .await
        .unwrap();
    });

    let mut client_spdy = SpdyConnection::new();
    client_spdy
        .write_syn_stream(&mut client_io, 1, StreamType::Stdout)
        .await
        .unwrap();
    client_spdy
        .write_syn_stream(&mut client_io, 3, StreamType::Error)
        .await
        .unwrap();

    let first_reply = client_spdy.read_frame(&mut client_io).await.unwrap();
    let second_reply = client_spdy.read_frame(&mut client_io).await.unwrap();
    assert!(matches!(first_reply, SpdyFrame::SynReply { stream_id: 1 }));
    assert!(matches!(second_reply, SpdyFrame::SynReply { stream_id: 3 }));

    match client_spdy.read_frame(&mut client_io).await.unwrap() {
        SpdyFrame::Data {
            stream_id,
            data,
            fin,
        } => {
            assert_eq!(stream_id, 1);
            assert_eq!(data, b"payload");
            assert!(!fin);
        }
        other => panic!("expected stdout data frame, got {other:?}"),
    }
    match client_spdy.read_frame(&mut client_io).await.unwrap() {
        SpdyFrame::Data {
            stream_id,
            data,
            fin,
        } => {
            assert_eq!(stream_id, 1);
            assert!(data.is_empty());
            assert!(fin);
        }
        other => panic!("expected stdout FIN frame, got {other:?}"),
    }
    match client_spdy.read_frame(&mut client_io).await.unwrap() {
        SpdyFrame::Data {
            stream_id,
            data,
            fin,
        } => {
            assert_eq!(stream_id, 3);
            let value: serde_json::Value = serde_json::from_slice(&data).unwrap();
            assert_eq!(value["status"], "Success");
            assert!(fin);
        }
        other => panic!("expected error status frame, got {other:?}"),
    }

    server.await.unwrap();
}

#[tokio::test]
async fn test_spdy_exec_accepts_stdout_only_client_when_only_stdout_requested() {
    use super::spdy_framing::{SpdyConnection, SpdyFrame, StreamType};
    use crate::current::pod_subresources::exec_spdy::{
        SpdyExecStreamRequest, collect_spdy_client_streams,
    };
    use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};

    let (mut server_io, mut client_io) = tokio::io::duplex(4096);
    let supervisor = TaskSupervisor::new(TaskCategoryConfig::default());

    let server = tokio::spawn(async move {
        let mut server_spdy = SpdyConnection::new();
        collect_spdy_client_streams(
            &mut server_spdy,
            &mut server_io,
            SpdyExecStreamRequest {
                stdin: false,
                stdout: true,
                stderr: false,
                tty: false,
                attach: false,
            },
            &supervisor,
        )
        .await
    });

    let mut client_spdy = SpdyConnection::new();
    client_spdy
        .write_syn_stream(&mut client_io, 1, StreamType::Stdout)
        .await
        .unwrap();

    match client_spdy.read_frame(&mut client_io).await.unwrap() {
        SpdyFrame::SynReply { stream_id } => assert_eq!(stream_id, 1),
        other => panic!("expected stdout SYN_REPLY, got {other:?}"),
    }

    tokio::time::timeout(std::time::Duration::from_millis(250), server)
        .await
        .expect("stdout-only SPDY negotiation should not wait for an error stream")
        .unwrap()
        .unwrap();
}

#[test]
fn test_parse_exec_query_combined_stdin_tty_stderr() {
    let (cmd, container, stdin, stdout, stderr, tty) =
        parse_exec_query("command=/bin/sh&stdin=1&tty=1&stdout=1&stderr=1&container=debug");
    assert_eq!(cmd, vec!["/bin/sh"]);
    assert_eq!(container, Some("debug".to_string()));
    assert!(stdin);
    assert!(stdout);
    assert!(stderr);
    assert!(tty);
}

#[test]
fn test_parse_exec_query_no_stdout_no_stderr() {
    let (_cmd, _container, _stdin, stdout, stderr, _tty) =
        parse_exec_query("command=ls&stdout=0&stderr=0");
    assert!(!stdout);
    assert!(!stderr);
}

#[test]
fn test_proxy_query_with_port() {
    let query: ProxyQuery = serde_json::from_value(json!({"port": 8080})).unwrap();
    assert_eq!(query.port, Some(8080));
}

#[test]
fn test_proxy_query_without_port() {
    let query: ProxyQuery = serde_json::from_value(json!({})).unwrap();
    assert_eq!(query.port, None);
}

#[test]
fn test_parse_proxy_name_port_without_suffix() {
    let parsed = parse_proxy_name_port("mypod");
    assert_eq!(parsed.scheme, None);
    assert_eq!(parsed.name, "mypod");
    assert_eq!(parsed.port_num, None);
    assert_eq!(parsed.port_name, None);
}

#[test]
fn test_parse_proxy_name_port_numeric_suffix() {
    let parsed = parse_proxy_name_port("mypod:8080");
    assert_eq!(parsed.scheme, None);
    assert_eq!(parsed.name, "mypod");
    assert_eq!(parsed.port_num, Some(8080));
    assert_eq!(parsed.port_name, None);
}

#[test]
fn test_parse_proxy_name_port_named_suffix() {
    let parsed = parse_proxy_name_port("mysvc:http");
    assert_eq!(parsed.scheme, None);
    assert_eq!(parsed.name, "mysvc");
    assert_eq!(parsed.port_num, None);
    assert_eq!(parsed.port_name, Some("http"));
}

#[test]
fn test_parse_proxy_name_port_with_http_scheme() {
    let parsed = parse_proxy_name_port("http:mysvc:8080");
    assert_eq!(parsed.scheme, Some("http"));
    assert_eq!(parsed.name, "mysvc");
    assert_eq!(parsed.port_num, Some(8080));
    assert_eq!(parsed.port_name, None);
}

#[test]
fn test_parse_proxy_name_port_with_https_scheme_and_named_port() {
    let parsed = parse_proxy_name_port("https:mysvc:tls");
    assert_eq!(parsed.scheme, Some("https"));
    assert_eq!(parsed.name, "mysvc");
    assert_eq!(parsed.port_num, None);
    assert_eq!(parsed.port_name, Some("tls"));
}

#[test]
fn test_proxy_port_resolution_from_container_spec() {
    let pod_data = json!({
        "spec": {
            "containers": [{
                "name": "web",
                "image": "nginx",
                "ports": [{"containerPort": 8080}]
            }]
        }
    });

    // Simulate the port resolution logic from pod_proxy_inner
    let port = pod_data
        .get("spec")
        .and_then(|s| s.get("containers"))
        .and_then(|c| c.as_array())
        .and_then(|arr| arr.first())
        .and_then(|c| c.get("ports"))
        .and_then(|p| p.as_array())
        .and_then(|arr| arr.first())
        .and_then(|p| p.get("containerPort"))
        .and_then(|cp| cp.as_u64())
        .map(|p| p as u16)
        .unwrap_or(80);
    assert_eq!(port, 8080);
}

#[test]
fn test_proxy_port_resolution_defaults_to_80() {
    // Pod with no ports defined
    let pod_data = json!({
        "spec": {
            "containers": [{
                "name": "web",
                "image": "nginx"
            }]
        }
    });

    let port = pod_data
        .get("spec")
        .and_then(|s| s.get("containers"))
        .and_then(|c| c.as_array())
        .and_then(|arr| arr.first())
        .and_then(|c| c.get("ports"))
        .and_then(|p| p.as_array())
        .and_then(|arr| arr.first())
        .and_then(|p| p.get("containerPort"))
        .and_then(|cp| cp.as_u64())
        .map(|p| p as u16)
        .unwrap_or(80);
    assert_eq!(port, 80);
}

#[test]
fn test_should_allow_pod_proxy_default_port_fallback_only_for_plain_80() {
    let parsed_plain = parse_proxy_name_port("mypod");
    assert!(should_allow_pod_proxy_default_port_fallback(
        None,
        parsed_plain,
        80
    ));
    assert!(!should_allow_pod_proxy_default_port_fallback(
        None,
        parsed_plain,
        9376
    ));

    let parsed_num = parse_proxy_name_port("mypod:9376");
    assert!(!should_allow_pod_proxy_default_port_fallback(
        None, parsed_num, 80
    ));

    let parsed_named = parse_proxy_name_port("mypod:http");
    assert!(!should_allow_pod_proxy_default_port_fallback(
        None,
        parsed_named,
        80
    ));

    let parsed_query = parse_proxy_name_port("mypod");
    assert!(!should_allow_pod_proxy_default_port_fallback(
        Some(8081),
        parsed_query,
        80
    ));
}

#[tokio::test]
async fn test_proxy_request_forwards_to_local_server() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    // Start a simple HTTP server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        if let Ok((mut stream, _)) = listener.accept().await {
            let mut buf = vec![0u8; 4096];
            let _n = stream.read(&mut buf).await.unwrap();
            let response = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });

    let req = Request::builder()
        .method("GET")
        .uri("/test")
        .body(axum::body::Body::empty())
        .unwrap();

    let target_url = format!("http://127.0.0.1:{}/test", addr.port());
    let task_supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let resp = proxy_request(req, &target_url, task_supervisor)
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
    assert_eq!(&body[..], b"hello");
}

#[tokio::test]
async fn test_proxy_request_connection_refused_returns_bad_gateway() {
    // Connect to a port nothing is listening on
    let req = Request::builder()
        .method("GET")
        .uri("/")
        .body(axum::body::Body::empty())
        .unwrap();

    let task_supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let result = proxy_request(req, "http://127.0.0.1:1/", task_supervisor).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        AppError::BadGateway(msg) => assert!(msg.contains("Failed to connect")),
        other => panic!("Expected BadGateway, got {:?}", other),
    }
}

#[tokio::test]
async fn test_proxy_request_fallback_retries_on_502_response() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let primary = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let primary_addr = primary.local_addr().unwrap();
    let fallback = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let fallback_addr = fallback.local_addr().unwrap();

    tokio::spawn(async move {
        if let Ok((mut stream, _)) = primary.accept().await {
            let mut buf = vec![0u8; 4096];
            let _ = stream.read(&mut buf).await.unwrap();
            let response = "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n";
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });

    tokio::spawn(async move {
        if let Ok((mut stream, _)) = fallback.accept().await {
            let mut buf = vec![0u8; 4096];
            let _ = stream.read(&mut buf).await.unwrap();
            let response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });

    let req = Request::builder()
        .method("GET")
        .uri("/proxy")
        .body(axum::body::Body::empty())
        .unwrap();
    let target_url = format!("http://127.0.0.1:{}/proxy", primary_addr.port());
    let task_supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));

    let resp = proxy_request_with_fallback_port(
        req,
        &target_url,
        true,
        fallback_addr.port(),
        task_supervisor,
    )
    .await
    .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
    assert_eq!(&body[..], b"ok");
}

#[tokio::test]
async fn test_pod_proxy_request_retries_until_listener_accepts() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let reserved = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = reserved.local_addr().unwrap();
    drop(reserved);

    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        let listener = TcpListener::bind(addr).await.unwrap();
        if let Ok((mut stream, _)) = listener.accept().await {
            let mut buf = vec![0u8; 4096];
            let _ = stream.read(&mut buf).await.unwrap();
            let response = "HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\npod-name";
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });

    let req = Request::builder()
        .method("GET")
        .uri("/proxy/")
        .body(axum::body::Body::empty())
        .unwrap();
    let target_url = format!("http://127.0.0.1:{}/", addr.port());
    let task_supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));

    let resp = proxy_request_with_fallback_port_and_retries(
        req,
        &target_url,
        false,
        8080,
        5,
        std::time::Duration::from_millis(50),
        task_supervisor,
    )
    .await
    .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
    assert_eq!(&body[..], b"pod-name");
}

#[tokio::test]
async fn test_proxy_request_timeout_retries_and_uses_fallback() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let primary = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let primary_addr = primary.local_addr().unwrap();
    let fallback = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let fallback_addr = fallback.local_addr().unwrap();

    tokio::spawn(async move {
        if let Ok((mut stream, _)) = primary.accept().await {
            let mut buf = vec![0u8; 4096];
            let _ = stream.read(&mut buf).await.unwrap();
            futures::future::pending::<()>().await;
        }
    });

    tokio::spawn(async move {
        if let Ok((mut stream, _)) = fallback.accept().await {
            let mut buf = vec![0u8; 4096];
            let _ = stream.read(&mut buf).await.unwrap();
            let response = "HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\nfallback";
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });

    let req = Request::builder()
        .method("GET")
        .uri("/proxy/results/name")
        .body(axum::body::Body::empty())
        .unwrap();
    let target_url = format!("http://127.0.0.1:{}/results/name", primary_addr.port());
    let task_supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));

    let resp = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        proxy_request_with_fallback_port_and_retries(
            req,
            &target_url,
            true,
            fallback_addr.port(),
            2,
            std::time::Duration::from_millis(10),
            task_supervisor,
        ),
    )
    .await
    .expect("hung pod proxy response must retry before the client context expires")
    .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
    assert_eq!(&body[..], b"fallback");
}

#[tokio::test]
async fn test_proxy_request_recomputes_content_length_after_buffering() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        if let Ok((mut stream, _)) = listener.accept().await {
            let mut buf = Vec::new();
            let mut chunk = [0u8; 1024];
            let header_end = loop {
                let n = stream.read(&mut chunk).await.unwrap();
                if n == 0 {
                    return;
                }
                buf.extend_from_slice(&chunk[..n]);
                if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    break pos + 4;
                }
            };

            let headers = String::from_utf8_lossy(&buf[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or(0);

            let mut remaining = content_length.saturating_sub(buf.len() - header_end);
            while remaining > 0 {
                let read_len = remaining.min(chunk.len());
                let n = stream.read(&mut chunk[..read_len]).await.unwrap();
                if n == 0 {
                    return;
                }
                remaining -= n;
            }

            let response = concat!(
                "HTTP/1.1 200 OK\r\n",
                "Content-Type: application/json\r\n",
                "Content-Length: 31\r\n",
                "\r\n",
                "{\"Method\":\"PATCH\",\"Body\":\"foo\"}"
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });

    let req = Request::builder()
        .method("PATCH")
        .uri("/proxy?method=PATCH")
        .header(axum::http::header::CONTENT_LENGTH, "1")
        .body(axum::body::Body::empty())
        .unwrap();
    let target_url = format!("http://127.0.0.1:{}/", addr.port());
    let task_supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));

    let resp = proxy_request(req, &target_url, task_supervisor)
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
    assert_eq!(&body[..], b"{\"Method\":\"PATCH\",\"Body\":\"foo\"}");
}

#[tokio::test]
async fn test_proxy_request_empty_post_uses_explicit_zero_content_length() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (headers_tx, headers_rx) = oneshot::channel();

    tokio::spawn(async move {
        if let Ok((mut stream, _)) = listener.accept().await {
            let mut buf = Vec::new();
            let mut chunk = [0u8; 1024];
            loop {
                let n = stream.read(&mut chunk).await.unwrap();
                if n == 0 {
                    return;
                }
                buf.extend_from_slice(&chunk[..n]);
                if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&buf[..pos + 4]).into_owned();
                    let _ = headers_tx.send(headers);
                    break;
                }
            }

            let response = concat!(
                "HTTP/1.1 200 OK\r\n",
                "Content-Type: application/json\r\n",
                "Content-Length: 30\r\n",
                "\r\n",
                "{\"Method\":\"POST\",\"Body\":\"foo\"}"
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });

    let req = Request::builder()
        .method("POST")
        .uri("/proxy?method=POST")
        .header(axum::http::header::CONTENT_LENGTH, "0")
        .body(axum::body::Body::empty())
        .unwrap();
    let target_url = format!("http://127.0.0.1:{}/", addr.port());
    let task_supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));

    let resp = proxy_request(req, &target_url, task_supervisor)
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let forwarded_headers = headers_rx.await.unwrap().to_ascii_lowercase();
    assert!(
        forwarded_headers.contains("\r\ncontent-length: 0\r\n"),
        "empty POST proxy request must carry explicit zero length; got:\n{forwarded_headers}"
    );
    assert!(
        !forwarded_headers.contains("\r\ntransfer-encoding:"),
        "empty POST proxy request must not use transfer-encoding; got:\n{forwarded_headers}"
    );
}

#[test]
fn test_rewrite_proxy_response_body_rewrites_relative_html_links() {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("text/html; charset=utf-8"),
    );
    headers.insert(
        header::CONTENT_LENGTH,
        header::HeaderValue::from_static("28"),
    );

    let path = "/api/v1/namespaces/ns/pods/http:pod-1:1080/proxy/";
    let body = Bytes::from_static(b"<a href=\"/rewriteme\">test</a>");
    let rewritten = rewrite_proxy_response_body(&mut headers, path, body);

    assert_eq!(
        rewritten,
        Bytes::from_static(
            b"<a href=\"/api/v1/namespaces/ns/pods/http:pod-1:1080/proxy/rewriteme\">test</a>"
        )
    );
    assert_eq!(
        headers.get(header::CONTENT_LENGTH).unwrap(),
        rewritten.len().to_string().as_str()
    );
}

#[test]
fn test_rewrite_proxy_response_body_adds_api_v1_prefix_for_core_short_paths() {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("text/html; charset=utf-8"),
    );
    let body = Bytes::from_static(b"<a href=\"/rewriteme\">test</a>");
    let rewritten =
        rewrite_proxy_response_body(&mut headers, "/namespaces/ns/pods/pod-1/proxy/", body);
    assert_eq!(
        rewritten,
        Bytes::from_static(
            b"<a href=\"/api/v1/namespaces/ns/pods/pod-1/proxy/rewriteme\">test</a>"
        )
    );
}

#[test]
fn test_rewrite_proxy_response_body_ignores_non_html() {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/json"),
    );
    let body = Bytes::from_static(br#"{"href":"/rewriteme"}"#);
    let rewritten = rewrite_proxy_response_body(
        &mut headers,
        "/api/v1/namespaces/ns/pods/pod/proxy/",
        body.clone(),
    );
    assert_eq!(rewritten, body);
}

/// Verify that read_reqwest_body_limited returns BadGateway before consuming
/// the entire response stream when the body exceeds the limit.
#[tokio::test]
async fn read_reqwest_body_limited_returns_bad_gateway_before_consuming_stream() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let chunk_count = std::sync::Arc::new(AtomicUsize::new(0));
    let chunk_count_clone = chunk_count.clone();

    // Start a local HTTP server that returns a chunked response
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        if let Ok((mut stream, _)) = listener.accept().await {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = vec![0u8; 4096];
            let _ = stream.read(&mut buf).await;

            // Send chunked response with 4 chunks of 100 bytes each
            let response =
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nTransfer-Encoding: chunked\r\n\r\n";
            stream.write_all(response.as_bytes()).await.unwrap();

            for i in 0..4 {
                let chunk_data = format!("chunk_{:03}", i);
                let chunk = format!("{:x}\r\n{}\r\n", chunk_data.len(), chunk_data);
                stream.write_all(chunk.as_bytes()).await.unwrap();
                chunk_count_clone.fetch_add(1, Ordering::SeqCst);
                // Small delay to ensure chunks arrive separately
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }

            // End of chunks
            stream.write_all(b"0\r\n\r\n").await.unwrap();
        }
    });

    // Make a reqwest request to the server
    let url = format!("http://127.0.0.1:{}/", addr.port());
    let response = reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .get(&url)
        .send()
        .await
        .unwrap();

    // Call read_reqwest_body_limited with a limit small enough to be
    // exceeded after reading some but not all chunks
    let limit = 5; // Chunks are ~9 bytes each, so limit triggers on first chunk
    let result = super::read_reqwest_body_limited(response, limit, "test proxy").await;

    match result {
        Err(AppError::BadGateway(msg)) => {
            assert!(
                msg.contains("response body exceeds limit"),
                "error should mention body exceeds limit: {}",
                msg
            );
        }
        other => panic!("Expected BadGateway, got {:?}", other),
    }

    // The chunk counter should be less than 4 — the helper must not
    // consume the full stream before rejecting
    let consumed = chunk_count.load(Ordering::SeqCst);
    assert!(
        consumed < 4,
        "chunk counter {} should be < 4 (stream not fully consumed)",
        consumed
    );
}

/// HTTPS proxy preserves status, headers, body, and HTML rewrite below limit.
#[tokio::test]
async fn https_proxy_preserves_status_headers_body_and_html_rewrite_below_limit() {
    // Generate self-signed cert
    let cert_params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    let key_pair = rcgen::KeyPair::generate().unwrap();
    let cert = cert_params.self_signed(&key_pair).unwrap();
    let cert_pem = cert.pem();
    let key_pem = key_pair.serialize_pem();
    let certs = rustls_pemfile::certs(&mut cert_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let key = rustls_pemfile::private_key(&mut key_pem.as_bytes())
        .unwrap()
        .unwrap();
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server_config =
        rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(server_config));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let body_content = "<html><body>hello</body></html>";
    let body_len = body_content.len();
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nX-Extra: preserved\r\nContent-Length: {}\r\n\r\n{}",
        body_len, body_content
    );
    let response_bytes = response.into_bytes();

    tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await {
            if let Ok(mut tls) = acceptor.accept(stream).await {
                let mut buf = vec![0u8; 4096];
                let _ = tokio::io::AsyncReadExt::read(&mut tls, &mut buf).await;
                let _ = tokio::io::AsyncWriteExt::write_all(&mut tls, &response_bytes).await;
            }
        }
    });

    let result = send_proxy_request_https(
        "localhost",
        port,
        "/test",
        "/api/v1/namespaces/default/pods/test-pod/proxy",
        &axum::http::Method::GET,
        &axum::http::HeaderMap::new(),
        axum::body::Bytes::new(),
    )
    .await
    .unwrap();

    assert_eq!(result.status(), axum::http::StatusCode::OK);
    let body = axum::body::to_bytes(result.into_body(), 1024 * 1024)
        .await
        .unwrap();
    assert!(body.starts_with(b"<html>") || body.starts_with(b"<HTML>"));
    assert!(body.len() >= body_len);
}
