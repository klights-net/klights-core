use super::*;
use crate::api::AdmissionContextRequest;

pub async fn pod_portforward(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    RawQuery(query): RawQuery,
    req: Request,
) -> Result<Response, AppError> {
    // Parse ports from query string
    let query_str = query.unwrap_or_default();
    let ports = crate::portforward::parse_ports_query(&query_str);

    if ports.is_empty() {
        return Err(AppError::BadRequest(
            "No ports specified in query string".to_string(),
        ));
    }

    // Get pod from PodRepository to find pod IP
    let pod = crate::kubelet::pod_repository::PodReader::get_pod(
        state.pod_repository.as_ref(),
        &namespace,
        &name,
    )
    .await?
    .ok_or_else(|| AppError::NotFound(format!("Pod {}/{} not found", namespace, name)))?;

    let _ = run_admission_for_request(
        state.db.as_ref(),
        build_admission_context(AdmissionContextRequest {
            api_version: "v1",
            kind: "Pod",
            operation: "CONNECT",
            namespace: Some(namespace.clone()),
            name: Some(name.clone()),
            object: Value::Null,
            old_object: Some((*pod.data).clone()),
            dry_run: false,
            subresource: Some("portforward"),
            options: None,
        }),
    )
    .await?;

    // Extract pod IP from status
    let pod_ip = pod
        .data
        .get("status")
        .and_then(|s| s.get("podIP"))
        .and_then(|ip| ip.as_str())
        .ok_or_else(|| AppError::BadRequest("Pod has no IP assigned yet".to_string()))?
        .to_string();

    // Check for WebSocket upgrade
    let upgrade_header = req
        .headers()
        .get(header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if upgrade_header.eq_ignore_ascii_case("websocket") {
        // Handle WebSocket portforward
        let ws_key = req
            .headers()
            .get(header::SEC_WEBSOCKET_KEY)
            .ok_or_else(|| AppError::BadRequest("Missing Sec-WebSocket-Key header".to_string()))?
            .clone();

        let subprotocol = negotiate_websocket_subprotocol(req.headers()).ok_or_else(|| {
            AppError::BadRequest("Missing or unsupported Sec-WebSocket-Protocol".to_string())
        })?;

        // Spawn WebSocket handler
        let on_upgrade = hyper::upgrade::on(req);

        let task_supervisor = state.task_supervisor.clone();
        let task_supervisor_for_session = task_supervisor.clone();
        if let Err(err) = task_supervisor
            .spawn_async(
                crate::task_supervisor::TaskCategory::Others,
                "pod_portforward_ws_upgrade",
                async move {
                    match on_upgrade.await {
                        Ok(upgraded) => {
                            use hyper_util::rt::TokioIo;
                            let io = TokioIo::new(upgraded);

                            use tokio_tungstenite::WebSocketStream;
                            let ws_stream = WebSocketStream::from_raw_socket(
                                io,
                                tokio_tungstenite::tungstenite::protocol::Role::Server,
                                None,
                            )
                            .await;

                            handle_portforward_websocket(
                                ws_stream,
                                pod_ip,
                                ports,
                                task_supervisor_for_session,
                            )
                            .await;
                        }
                        Err(e) => {
                            tracing::error!("WebSocket upgrade failed for portforward: {}", e);
                        }
                    }
                },
            )
            .await
        {
            tracing::warn!(
                "Failed to spawn portforward WebSocket upgrade task: {}",
                err
            );
        }

        // Return 101 Switching Protocols
        let response = Response::builder()
            .status(StatusCode::SWITCHING_PROTOCOLS)
            .header(header::UPGRADE, "websocket")
            .header(header::CONNECTION, "Upgrade")
            .header(
                header::SEC_WEBSOCKET_ACCEPT,
                derive_websocket_accept_key(&ws_key),
            )
            .header(header::SEC_WEBSOCKET_PROTOCOL, subprotocol)
            .body(axum::body::Body::empty())
            .map_err(|e| {
                AppError::Internal(format!("Failed to build WebSocket response: {}", e))
            })?;

        Ok(response)
    } else {
        Err(AppError::BadRequest(
            "Only WebSocket upgrade supported for portforward (SPDY not yet implemented)"
                .to_string(),
        ))
    }
}

async fn handle_portforward_websocket(
    ws_stream: tokio_tungstenite::WebSocketStream<
        hyper_util::rt::TokioIo<hyper::upgrade::Upgraded>,
    >,
    pod_ip: String,
    ports: Vec<u16>,
    task_supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
) {
    use futures::{SinkExt, StreamExt};
    use std::collections::HashMap;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::mpsc;
    use tokio_tungstenite::tungstenite::Message;

    let (mut ws_write, mut ws_read) = ws_stream.split();

    // Create channel for TCP→WebSocket communication; bounded at 64 frames (~1 MB cap)
    // to apply backpressure on slow WebSocket clients instead of buffering unboundedly.
    let (to_ws_tx, mut to_ws_rx) = mpsc::channel::<(u8, Vec<u8>)>(64);

    // HashMap to store TCP write handles (channel_id → write half)
    let mut tcp_writers: HashMap<u8, tokio::net::tcp::OwnedWriteHalf> = HashMap::new();

    // Create TCP connections and spawn reader tasks
    for (port_idx, port) in ports.iter().enumerate() {
        let addr = format!("{}:{}", pod_ip, port);
        let data_channel = crate::portforward::port_channel_id(port_idx, false);
        let error_channel = crate::portforward::port_channel_id(port_idx, true);

        match tokio::net::TcpStream::connect(&addr).await {
            Ok(tcp_stream) => {
                tracing::debug!(
                    "Connected to {}:{} (data channel {})",
                    pod_ip,
                    port,
                    data_channel
                );

                // Split TCP stream into read and write halves
                let (mut tcp_read, tcp_write) = tcp_stream.into_split();

                // Store write half for WebSocket→TCP writes
                tcp_writers.insert(data_channel, tcp_write);

                // Spawn task to read from TCP and send to WebSocket
                let to_ws_tx_clone = to_ws_tx.clone();
                if let Err(err) = task_supervisor
                    .spawn_async(
                        crate::task_supervisor::TaskCategory::Others,
                        format!("pod_portforward_tcp_reader_{}", data_channel),
                        async move {
                            let mut buf = vec![0u8; 4096];
                            loop {
                                match tcp_read.read(&mut buf).await {
                                    Ok(0) => {
                                        tracing::debug!("TCP EOF on channel {}", data_channel);
                                        break;
                                    }
                                    Ok(n) => {
                                        if to_ws_tx_clone
                                            .send((data_channel, buf[..n].to_vec()))
                                            .await
                                            .is_err()
                                        {
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            "TCP read error on channel {}: {}",
                                            data_channel,
                                            e
                                        );
                                        break;
                                    }
                                }
                            }
                        },
                    )
                    .await
                {
                    tracing::warn!(
                        "Failed to spawn TCP reader for portforward channel {}: {}",
                        data_channel,
                        err
                    );
                    let _ = to_ws_tx
                        .send((
                            error_channel,
                            format!("Failed to start stream reader: {}", err).into_bytes(),
                        ))
                        .await;
                }
            }
            Err(e) => {
                tracing::error!("Failed to connect to {}:{}: {}", pod_ip, port, e);
                let error_msg = format!("Failed to connect: {}", e);
                if to_ws_tx
                    .send((error_channel, error_msg.into_bytes()))
                    .await
                    .is_err()
                {
                    tracing::error!("Failed to send connection error");
                }
            }
        }
    }

    // Drop our copy of the sender so the channel closes when all TCP readers finish
    drop(to_ws_tx);

    // Main relay loop using tokio::select!
    loop {
        tokio::select! {
            // TCP → WebSocket: receive from channel and write to WebSocket
            Some((channel_id, data)) = to_ws_rx.recv() => {
                let mut payload = vec![channel_id];
                payload.extend_from_slice(&data);
                if let Err(e) = ws_write.send(Message::Binary(Bytes::from(payload))).await {
                    tracing::error!("Failed to send to WebSocket: {}", e);
                    break;
                }
            }

            // WebSocket → TCP: read from WebSocket and write to TCP stream
            Some(msg_result) = ws_read.next() => {
                match msg_result {
                    Ok(Message::Binary(data)) => {
                        if data.is_empty() {
                            continue;
                        }
                        let channel_id = data[0];

                        // Write to corresponding TCP stream
                        if let Some(tcp_writer) = tcp_writers.get_mut(&channel_id) {
                            if data.len() > 1
                                && let Err(e) = tcp_writer.write_all(&data[1..]).await {
                                    tracing::error!("Failed to write to TCP on channel {}: {}", channel_id, e);
                                    tcp_writers.remove(&channel_id);
                                }
                        } else {
                            tracing::warn!("Received data for unknown channel {}", channel_id);
                        }
                    }
                    Ok(Message::Close(_)) => {
                        tracing::debug!("WebSocket closed by client");
                        break;
                    }
                    Ok(_) => {
                        // Ignore other message types (Text, Ping, Pong)
                    }
                    Err(e) => {
                        tracing::error!("WebSocket read error: {}", e);
                        break;
                    }
                }
            }

            // Both channels closed
            else => {
                tracing::debug!("All channels closed, ending portforward");
                break;
            }
        }
    }

    tracing::debug!("Portforward session ended");
}

// GET /api/v1/namespaces/{ns}/pods/{name}/ephemeralcontainers
