use super::*;
use crate::api::AdmissionContextRequest;
use klights_node_api::{
    ByteFrame, NodePortForward, NodePortForwardChannel, NodePortForwardFrame,
    NodePortForwardRequest, NodePortForwardSession, NodePortForwardTarget,
};

pub(in crate::api) async fn pod_portforward(
    State(state): State<Arc<ApiState>>,
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
    let pod = crate::api::pod_repository_ports::get_pod(
        state.resource_mutation().pod_repository.as_ref(),
        &namespace,
        &name,
    )
    .await?
    .ok_or_else(|| AppError::NotFound(format!("Pod {}/{} not found", namespace, name)))?;

    let _ = run_admission_for_request(
        state.resource_mutation().db.as_ref(),
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
    let target = NodePortForwardTarget::try_new(namespace, name, pod_ip)
        .map_err(|error| AppError::BadRequest(error.to_string()))?;
    let node_request = NodePortForwardRequest::try_new(target, ports)
        .map_err(|error| AppError::BadRequest(error.to_string()))?;
    let node_port_forward = state.pod_node_subresources().node_port_forward.clone();

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

        let task_supervisor = state.operational().task_supervisor.clone();
        let relay_supervisor = task_supervisor.clone();
        if let Err(err) = task_supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Others,
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
                                node_port_forward,
                                node_request,
                                relay_supervisor,
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

async fn handle_portforward_websocket<IO>(
    ws_stream: tokio_tungstenite::WebSocketStream<IO>,
    node_port_forward: Arc<dyn NodePortForward>,
    request: NodePortForwardRequest,
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
) where
    IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let port_count = request.ports().len();
    let (mut ws_write, mut ws_read) = ws_stream.split();
    let websocket_closed = tokio_util::sync::CancellationToken::new();
    let reader_closed = websocket_closed.clone();
    let (mut ws_input_tx, mut ws_input_rx) = futures::channel::mpsc::channel(64);
    if let Err(error) = task_supervisor
        .spawn_async(
            klights_supervisor::TaskCategory::Others,
            "pod_portforward_ws_reader",
            async move {
                loop {
                    let message = tokio::select! {
                        biased;
                        _ = reader_closed.cancelled() => break,
                        message = ws_read.next() => message,
                    };
                    let Some(message) = message else {
                        break;
                    };
                    match message {
                        Ok(Message::Binary(data)) => {
                            tokio::select! {
                                biased;
                                _ = reader_closed.cancelled() => break,
                                result = ws_input_tx.send(data) => {
                                    if result.is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                        Ok(Message::Close(_)) => {
                            tracing::debug!("WebSocket closed by client");
                            reader_closed.cancel();
                            break;
                        }
                        Ok(_) => {}
                        Err(error) => {
                            tracing::error!(%error, "WebSocket read error");
                            reader_closed.cancel();
                            break;
                        }
                    }
                }
                reader_closed.cancel();
            },
        )
        .await
    {
        tracing::error!(%error, "failed to start port-forward WebSocket reader");
        return;
    }

    let open = node_port_forward.open_port_forward(request);
    tokio::pin!(open);
    let mut session: Box<dyn NodePortForwardSession> = tokio::select! {
        biased;
        _ = websocket_closed.cancelled() => return,
        result = &mut open => match result {
            Ok(session) => session,
            Err(error) => {
                tracing::error!(%error, "failed to open port-forward session");
                return;
            }
        },
    };
    let mut runtime_closed = false;

    // Main relay loop using tokio::select!
    loop {
        tokio::select! {
            biased;
            _ = websocket_closed.cancelled() => {
                let _ = session.cancel().await;
                break;
            }
            // TCP → WebSocket: receive a bounded capability frame and add
            // only the transport-private positional channel byte here.
            frame = session.recv_frame(), if !runtime_closed => {
                let frame = match frame {
                    Ok(Some(frame)) => frame,
                    Ok(None) => {
                        runtime_closed = true;
                        continue;
                    }
                    Err(error) => {
                        tracing::debug!(%error, "port-forward runtime receive ended");
                        break;
                    }
                };
                let Some(channel_id) = crate::portforward::port_channel_id(
                    frame.port_index(),
                    frame.channel() == NodePortForwardChannel::Error,
                ) else {
                    tracing::error!(port_index = frame.port_index(), "port-forward runtime returned an out-of-range port index");
                    break;
                };
                if frame.port_index() >= port_count {
                    tracing::error!(port_index = frame.port_index(), port_count, "port-forward runtime returned an unopened port index");
                    break;
                }
                let mut payload = bytes::BytesMut::with_capacity(1 + frame.payload().len());
                payload.extend_from_slice(&[channel_id]);
                payload.extend_from_slice(frame.payload());
                tokio::select! {
                    biased;
                    _ = websocket_closed.cancelled() => {
                        let _ = session.cancel().await;
                        break;
                    }
                    result = ws_write.send(Message::Binary(payload.freeze())) => {
                        if let Err(error) = result {
                            tracing::error!(%error, "failed to send to WebSocket");
                            break;
                        }
                    }
                }
            }

            // WebSocket → TCP: read from WebSocket and write to TCP stream
            Some(data) = ws_input_rx.next() => {
                if data.is_empty() {
                    continue;
                }
                let channel_id = data[0];
                let (port_index, channel) =
                    crate::portforward::port_channel_from_id(channel_id);
                if port_index >= port_count {
                    tracing::debug!(channel_id, port_index, port_count, "ignored port-forward frame for unopened channel");
                    continue;
                }
                let frame = match channel {
                    NodePortForwardChannel::Data => {
                        NodePortForwardFrame::data(port_index, data.slice(1..))
                    }
                    NodePortForwardChannel::Error => {
                        NodePortForwardFrame::error(port_index, data.slice(1..))
                    }
                };
                tokio::select! {
                    biased;
                    _ = websocket_closed.cancelled() => {
                        let _ = session.cancel().await;
                        break;
                    }
                    result = session.send_frame(frame) => {
                        if let Err(error) = result {
                            tracing::error!(
                                channel_id,
                                %error,
                                "failed to write port-forward runtime frame"
                            );
                        }
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

    websocket_closed.cancel();
    let _ = session.cancel().await;
    tracing::debug!("Portforward session ended");
}

#[cfg(test)]
mod cancellation_tests {
    use super::*;
    use klights_node_api::{
        BoundedByteStream, ByteStreamBounds, ByteStreamFuture, NodePortForwardFuture,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    struct BlockingSession {
        cancelled: AtomicBool,
        cancelled_notify: Arc<tokio::sync::Notify>,
    }

    impl BoundedByteStream for BlockingSession {
        type Frame = NodePortForwardFrame;

        fn bounds(&self) -> ByteStreamBounds {
            ByteStreamBounds::try_new(64, 64).unwrap()
        }

        fn is_cancelled(&self) -> bool {
            self.cancelled.load(Ordering::Acquire)
        }

        fn send_frame(&self, _frame: Self::Frame) -> ByteStreamFuture<'_, ()> {
            Box::pin(std::future::pending())
        }

        fn recv_frame(&self) -> ByteStreamFuture<'_, Option<Self::Frame>> {
            Box::pin(std::future::pending())
        }

        fn cancel(&mut self) -> ByteStreamFuture<'_, ()> {
            Box::pin(async move {
                if !self.cancelled.swap(true, Ordering::AcqRel) {
                    self.cancelled_notify.notify_one();
                }
                Ok(())
            })
        }
    }

    struct BlockingPortForward {
        cancelled_notify: Arc<tokio::sync::Notify>,
    }

    struct DropSignal(Arc<tokio::sync::Notify>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.notify_one();
        }
    }

    struct SlowOpenPortForward {
        started: Arc<tokio::sync::Notify>,
        dropped: Arc<tokio::sync::Notify>,
    }

    impl NodePortForward for SlowOpenPortForward {
        fn open_port_forward(
            &self,
            _request: NodePortForwardRequest,
        ) -> NodePortForwardFuture<'_, Box<dyn NodePortForwardSession>> {
            let dropped = self.dropped.clone();
            let started = self.started.clone();
            Box::pin(async move {
                let _drop_signal = DropSignal(dropped);
                started.notify_one();
                std::future::pending().await
            })
        }
    }

    impl NodePortForward for BlockingPortForward {
        fn open_port_forward(
            &self,
            _request: NodePortForwardRequest,
        ) -> NodePortForwardFuture<'_, Box<dyn NodePortForwardSession>> {
            Box::pin(async move {
                Ok(Box::new(BlockingSession {
                    cancelled: AtomicBool::new(false),
                    cancelled_notify: self.cancelled_notify.clone(),
                }) as Box<dyn NodePortForwardSession>)
            })
        }
    }

    #[tokio::test]
    async fn websocket_close_cancels_a_session_blocked_sending_to_tcp() {
        use futures::SinkExt as _;
        use tokio_tungstenite::tungstenite::{Message, protocol::Role};

        let (client_io, server_io) = tokio::io::duplex(1024);
        let client =
            tokio_tungstenite::WebSocketStream::from_raw_socket(client_io, Role::Client, None);
        let server =
            tokio_tungstenite::WebSocketStream::from_raw_socket(server_io, Role::Server, None);
        let (mut client, server) = tokio::join!(client, server);
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let cancelled = Arc::new(tokio::sync::Notify::new());
        let request = NodePortForwardRequest::try_new(
            NodePortForwardTarget::try_new("default", "pod", "127.0.0.1").unwrap(),
            vec![8080],
        )
        .unwrap();
        supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Others,
                "test_portforward_ws_relay",
                handle_portforward_websocket(
                    server,
                    Arc::new(BlockingPortForward {
                        cancelled_notify: cancelled.clone(),
                    }),
                    request,
                    supervisor.clone(),
                ),
            )
            .await
            .unwrap();

        client
            .send(Message::Binary(Bytes::from_static(&[0, 1])))
            .await
            .unwrap();
        client.send(Message::Close(None)).await.unwrap();
        supervisor
            .timeout(
                "test_portforward_ws_close_cancellation",
                Duration::from_secs(1),
                cancelled.notified(),
            )
            .await
            .unwrap()
            .expect("WebSocket close must interrupt a blocked runtime send");
        let _ = supervisor.shutdown(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn websocket_close_drops_a_backpressured_session_open() {
        use futures::SinkExt as _;
        use tokio_tungstenite::tungstenite::{Message, protocol::Role};

        let (client_io, server_io) = tokio::io::duplex(1024);
        let client =
            tokio_tungstenite::WebSocketStream::from_raw_socket(client_io, Role::Client, None);
        let server =
            tokio_tungstenite::WebSocketStream::from_raw_socket(server_io, Role::Server, None);
        let (mut client, server) = tokio::join!(client, server);
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let started = Arc::new(tokio::sync::Notify::new());
        let dropped = Arc::new(tokio::sync::Notify::new());
        let request = NodePortForwardRequest::try_new(
            NodePortForwardTarget::try_new("default", "pod", "127.0.0.1").unwrap(),
            vec![8080],
        )
        .unwrap();
        supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Others,
                "test_portforward_ws_slow_open",
                handle_portforward_websocket(
                    server,
                    Arc::new(SlowOpenPortForward {
                        started: started.clone(),
                        dropped: dropped.clone(),
                    }),
                    request,
                    supervisor.clone(),
                ),
            )
            .await
            .unwrap();

        supervisor
            .timeout(
                "test_portforward_ws_slow_open_started",
                Duration::from_secs(1),
                started.notified(),
            )
            .await
            .unwrap()
            .expect("session open must start before the close race");
        client.send(Message::Close(None)).await.unwrap();
        supervisor
            .timeout(
                "test_portforward_ws_close_slow_open",
                Duration::from_secs(1),
                dropped.notified(),
            )
            .await
            .unwrap()
            .expect("WebSocket close must drop an unfinished session open");
        let _ = supervisor.shutdown(Duration::from_secs(1)).await;
    }
}

// GET /api/v1/namespaces/{ns}/pods/{name}/ephemeralcontainers
