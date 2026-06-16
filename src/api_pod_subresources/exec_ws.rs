use super::*;

pub fn remote_exec_error_frame_is_terminal(
    frame: &crate::replication::protocol::NodeExecStreamFrame,
) -> bool {
    crate::replication::protocol::node_exec_error_frame_is_terminal(frame)
}

fn spdy_error_stream_frame_is_terminal(stream_id: u32, data: &[u8], fin: bool) -> bool {
    stream_id == 7
        && (fin || crate::replication::protocol::exec_error_status_payload_is_terminal(data))
}

async fn close_websocket_gracefully<S>(
    ws_sender: &mut futures::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<S>,
        tokio_tungstenite::tungstenite::Message,
    >,
    ws_receiver: &mut futures::stream::SplitStream<tokio_tungstenite::WebSocketStream<S>>,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
    peer_already_closed: bool,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use futures::sink::SinkExt as _;
    use futures::stream::StreamExt as _;
    use tokio_tungstenite::tungstenite::Message as TungsteniteMessage;
    use tokio_tungstenite::tungstenite::protocol::CloseFrame;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

    let _ = ws_sender
        .send(TungsteniteMessage::Close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "".into(),
        })))
        .await;

    if peer_already_closed {
        return;
    }

    let close_deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(250);
    loop {
        tokio::select! {
            _ = task_supervisor.sleep_until("exec_websocket_close_handshake", close_deadline) => {
                break;
            }
            msg = ws_receiver.next() => {
                match msg {
                    Some(Ok(TungsteniteMessage::Close(_))) | None => break,
                    Some(Ok(_)) => continue,
                    Some(Err(err)) => {
                        tracing::debug!("WebSocket close handshake read ended: {}", err);
                        break;
                    }
                }
            }
        }
    }
}

pub struct ExecWebSocketRequest {
    pub cri: Arc<tokio::sync::Mutex<crate::kubelet::cri::CriClient>>,
    pub task_supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
    pub target: ExecTarget,
    pub subprotocol: String,
    pub stream_options: ExecStreamOptions,
    pub attach: bool,
}

pub struct RemoteExecWebSocketRequest {
    pub session: crate::replication::service::NodeExecStreamSession,
    pub task_supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
    pub target: ExecTarget,
    pub subprotocol: String,
    pub stream_options: ExecStreamOptions,
    pub attach: bool,
}

pub struct RemoteExecWebSocketSyncRequest {
    pub replication: Arc<crate::replication::ReplicationService>,
    pub target: ExecTarget,
    pub subprotocol: String,
    pub node_name: String,
    pub task_supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
}

pub async fn handle_exec_websocket_tungstenite<S>(
    socket: tokio_tungstenite::WebSocketStream<S>,
    request: ExecWebSocketRequest,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use futures::sink::SinkExt as _;
    use futures::stream::StreamExt as _;
    use tokio_tungstenite::tungstenite::Message as TungsteniteMessage;

    let ExecWebSocketRequest {
        cri,
        task_supervisor,
        target,
        subprotocol,
        stream_options,
        attach,
    } = request;
    let ExecTarget {
        namespace,
        pod_name,
        container_id,
        command,
    } = target;
    let stdin = stream_options.stdin;
    let stdout = stream_options.stdout;
    let stderr = stream_options.stderr;
    let tty = stream_options.tty;

    tracing::info!(
        "kubectl {} (POST WebSocket): pod={}/{}, container={}, command={:?}, stdin={}, tty={}",
        if attach { "attach" } else { "exec" },
        namespace,
        pod_name,
        container_id,
        command,
        stdin,
        tty
    );

    let (mut ws_sender, mut ws_receiver) = socket.split();
    let mut peer_closed = false;

    // Attach is always a streaming operation. Exec only needs the streaming
    // path for stdin/TTY; non-interactive exec can use ExecSync below.
    if attach || stdin || tty {
        // Use CRI Exec API instead of nsenter
        // 1. Call CRI Exec to get streaming URL
        // 2. Connect to containerd streaming server with SPDY upgrade
        // 3. Create SPDY streams (stdin, stdout, stderr, error)
        // 4. Bridge kubectl WebSocket <-> containerd SPDY

        tracing::info!(
            "kubectl {} (CRI): pod={}/{}, container={}, command={:?}, stdin={}, tty={}",
            if attach { "attach" } else { "exec" },
            namespace,
            pod_name,
            container_id,
            command,
            stdin,
            tty
        );

        // Step 1: Call CRI Exec to get streaming URL
        let streaming_url = {
            let mut cri_client = cri.lock().await;
            let stream_result = if attach {
                attach_with_created_state_retry(
                    &mut cri_client,
                    task_supervisor.as_ref(),
                    AttachRequest {
                        container_id: &container_id,
                        stream_options: ExecStreamOptions {
                            tty,
                            stdin,
                            stdout,
                            stderr: stderr && !tty,
                        },
                    },
                )
                .await
                .map(|resp| resp.url)
            } else {
                exec_with_created_state_retry(
                    &mut cri_client,
                    task_supervisor.as_ref(),
                    ExecRequest {
                        container_id: &container_id,
                        command: &command,
                        stream_options: ExecStreamOptions {
                            tty,
                            stdin,
                            stdout,
                            stderr: stderr && !tty,
                        },
                    },
                )
                .await
                .map(|resp| resp.url)
            };
            match stream_result {
                Ok(url) => url,
                Err(e) => {
                    tracing::error!(
                        "CRI {} failed: {}",
                        if attach { "Attach" } else { "Exec" },
                        e
                    );
                    let mut frame = vec![3u8];
                    frame.extend_from_slice(&format_websocket_error_payload(
                        &subprotocol,
                        format!(
                            "CRI {} failed: {}",
                            if attach { "Attach" } else { "Exec" },
                            e
                        ),
                    ));
                    let _ = ws_sender
                        .send(TungsteniteMessage::Binary(frame.into()))
                        .await;
                    close_websocket_gracefully(
                        &mut ws_sender,
                        &mut ws_receiver,
                        task_supervisor.as_ref(),
                        peer_closed,
                    )
                    .await;
                    return;
                }
            }
        };

        tracing::debug!(
            "CRI {} streaming URL: {}",
            if attach { "Attach" } else { "Exec" },
            streaming_url
        );

        // Step 2: Connect to containerd streaming server with SPDY upgrade
        let mut containerd_stream =
            match crate::spdy::SpdyExec::connect_to_streaming_url(&streaming_url).await {
                Ok(stream) => stream,
                Err(e) => {
                    tracing::error!("Failed to connect to containerd streaming URL: {}", e);
                    let mut frame = vec![3u8];
                    frame.extend_from_slice(&format_websocket_error_payload(
                        &subprotocol,
                        format!("Failed to connect to containerd: {}", e),
                    ));
                    let _ = ws_sender
                        .send(TungsteniteMessage::Binary(frame.into()))
                        .await;
                    close_websocket_gracefully(
                        &mut ws_sender,
                        &mut ws_receiver,
                        task_supervisor.as_ref(),
                        peer_closed,
                    )
                    .await;
                    return;
                }
            };

        // Step 3: Create SPDY streams using SYN_STREAM frames
        let mut spdy = crate::spdy::SpdyExec::new();

        // Create stdin stream (stream ID 1) if needed
        if stdin
            && let Err(e) = spdy
                .write_syn_stream(&mut containerd_stream, 1, crate::spdy::StreamType::Stdin)
                .await
        {
            tracing::error!("Failed to create stdin SPDY stream: {}", e);
            close_websocket_gracefully(
                &mut ws_sender,
                &mut ws_receiver,
                task_supervisor.as_ref(),
                peer_closed,
            )
            .await;
            return;
        }

        // Create stdout stream (stream ID 3) if requested
        if stdout
            && let Err(e) = spdy
                .write_syn_stream(&mut containerd_stream, 3, crate::spdy::StreamType::Stdout)
                .await
        {
            tracing::error!("Failed to create stdout SPDY stream: {}", e);
            close_websocket_gracefully(
                &mut ws_sender,
                &mut ws_receiver,
                task_supervisor.as_ref(),
                peer_closed,
            )
            .await;
            return;
        }

        // Create stderr stream (stream ID 5) if requested and not tty mode
        if stderr
            && !tty
            && let Err(e) = spdy
                .write_syn_stream(&mut containerd_stream, 5, crate::spdy::StreamType::Stderr)
                .await
        {
            tracing::error!("Failed to create stderr SPDY stream: {}", e);
            close_websocket_gracefully(
                &mut ws_sender,
                &mut ws_receiver,
                task_supervisor.as_ref(),
                peer_closed,
            )
            .await;
            return;
        }

        // Create error stream (stream ID 7)
        if let Err(e) = spdy
            .write_syn_stream(&mut containerd_stream, 7, crate::spdy::StreamType::Error)
            .await
        {
            tracing::error!("Failed to create error SPDY stream: {}", e);
            close_websocket_gracefully(
                &mut ws_sender,
                &mut ws_receiver,
                task_supervisor.as_ref(),
                peer_closed,
            )
            .await;
            return;
        }

        // Create resize stream (stream ID 9) for TTY mode
        // The TTY shell won't output until it receives a terminal size event
        let resize_stream_id: u32 = 9;
        if tty {
            if let Err(e) = spdy
                .write_syn_stream(
                    &mut containerd_stream,
                    resize_stream_id,
                    crate::spdy::StreamType::Resize,
                )
                .await
            {
                tracing::error!("Failed to create resize SPDY stream: {}", e);
                close_websocket_gracefully(
                    &mut ws_sender,
                    &mut ws_receiver,
                    task_supervisor.as_ref(),
                    peer_closed,
                )
                .await;
                return;
            }

            // Send initial terminal size to unblock the shell prompt
            // Default 80x24 — kubectl will send the real size immediately after
            let initial_resize = serde_json::json!({"Width": 80, "Height": 24});
            if let Err(e) = spdy
                .write_data_frame(
                    &mut containerd_stream,
                    resize_stream_id,
                    initial_resize.to_string().as_bytes(),
                    false,
                )
                .await
            {
                tracing::error!("Failed to send initial resize: {}", e);
            }
        }

        // Step 4: Bridge kubectl WebSocket <-> containerd SPDY streams
        let mut exit_code: i32 = 0;
        let mut stdin_closed = false;
        let stdin_idle_timeout = std::time::Duration::from_secs(2);
        let mut stdin_deadline = tokio::time::Instant::now() + stdin_idle_timeout;

        loop {
            tokio::select! {
                // For non-TTY piped stdin, close stdin after idle timeout.
                // kubectl piping stdin (echo "cmd" | kubectl exec -i) often
                // doesn't send a Close frame, it just stops producing bytes.
                // Use deadline-based timing (not loop-iteration timeout) so
                // SPDY control traffic does not postpone EOF detection.
                _ = task_supervisor.sleep_until(
                    "exec_ws_stdin_idle_timeout",
                    stdin_deadline,
                ), if stdin && !tty && !stdin_closed => {
                    tracing::info!("Stdin idle timeout: {}s in non-TTY mode — closing stdin stream", stdin_idle_timeout.as_secs());
                    let _ = spdy.write_data_frame(&mut containerd_stream, 1, &[], true).await;
                    stdin_closed = true;
                    // Loop will continue to process output from SPDY
                }

                // Read from kubectl WebSocket, forward to containerd SPDY
                ws_msg = ws_receiver.next() => {
                    match ws_msg {
                        Some(Ok(TungsteniteMessage::Binary(data))) => {
                            if data.is_empty() {
                                continue;
                            }
                            match data[0] {
                                // Channel 0 = stdin
                                0 if stdin && !stdin_closed => {
                                    if data.len() == 1 {
                                        // Empty stdin payload = EOF
                                        // Send FIN on stdin stream to signal EOF to containerd
                                        let _ = spdy.write_data_frame(&mut containerd_stream, 1, &[], true).await;
                                        stdin_closed = true;
                                    } else { match spdy.write_data_frame(&mut containerd_stream, 1, &data[1..], false).await { Err(e) => {
                                        tracing::error!("Failed to write stdin to SPDY: {}", e);
                                        break;
                                    } _ => {
                                        stdin_deadline = tokio::time::Instant::now() + stdin_idle_timeout;
                                    }}}
                                }
                                // Channel 4 = resize (terminal size events)
                                4 if tty => {
                                    // kubectl sends JSON: {"Width": N, "Height": N}
                                    // Forward to containerd resize stream
                                    if let Err(e) = spdy.write_data_frame(&mut containerd_stream, resize_stream_id, &data[1..], false).await {
                                        tracing::error!("Failed to forward resize to containerd: {}", e);
                                    }
                                }
                                _ => {} // Ignore other channels
                            }
                        }
                        Some(Ok(TungsteniteMessage::Close(_))) | None => {
                            tracing::info!("WebSocket closed by client");
                            peer_closed = true;
                            // Send FIN on stdin stream to signal EOF
                            if stdin && !stdin_closed {
                                let _ = spdy.write_data_frame(&mut containerd_stream, 1, &[], true).await;
                            }
                            break;
                        }
                        Some(Err(e)) => {
                            tracing::error!("WebSocket receive error: {}", e);
                            peer_closed = true;
                            if stdin && !stdin_closed {
                                let _ = spdy.write_data_frame(&mut containerd_stream, 1, &[], true).await;
                            }
                            break;
                        }
                        _ => {}
                    }
                }

                // Read from containerd SPDY stream using proper frame parser
                frame_result = spdy.read_frame(&mut containerd_stream) => {
                    match frame_result {
                        Ok(crate::spdy::SpdyFrame::Data { stream_id, data, fin }) => {
                            let terminal_error_frame =
                                spdy_error_stream_frame_is_terminal(stream_id, &data, fin);
                            // Map SPDY stream IDs to kubectl channels
                            // Stream 3 = stdout → channel 1
                            // Stream 5 = stderr → channel 2
                            // Stream 7 = error → channel 3
                            let channel = match stream_id {
                                3 => 1, // stdout
                                5 => 2, // stderr
                                7 => {  // error (contains exit code JSON)
                                    // Parse exit code from error stream
                                    if let Ok(error_json) = serde_json::from_slice::<serde_json::Value>(&data) {
                                        if error_json.get("status").and_then(|s| s.as_str()) == Some("Success") {
                                            exit_code = 0;
                                        } else if let Some(causes) = error_json.pointer("/details/causes")
                                            && let Some(causes_arr) = causes.as_array() {
                                                for cause in causes_arr {
                                                    if cause.get("reason").and_then(|r| r.as_str()) == Some("ExitCode")
                                                        && let Some(code_str) = cause.get("message").and_then(|m| m.as_str()) {
                                                            exit_code = code_str.parse().unwrap_or(1);
                                                        }
                                                }
                                            }
                                    }
                                    3 // Send error on channel 3
                                }
                                _ => continue, // Unknown stream, skip
                            };

                            // Forward to kubectl WebSocket
                            if !data.is_empty() {
                                if stream_id == 7 && !websocket_uses_structured_status_channel(&subprotocol) {
                                    let is_success = serde_json::from_slice::<serde_json::Value>(&data)
                                        .ok()
                                        .and_then(|v| {
                                            v.get("status")
                                                .and_then(|s| s.as_str())
                                                .map(|status| status == "Success")
                                        })
                                        .unwrap_or(false);
                                    if is_success {
                                        if terminal_error_frame {
                                            break;
                                        }
                                        continue;
                                    }
                                }
                                let mut ws_frame = vec![channel];
                                ws_frame.extend_from_slice(&data);
                                if let Err(e) = ws_sender.send(TungsteniteMessage::Binary(ws_frame.into())).await {
                                    tracing::error!("Failed to send to WebSocket: {}", e);
                                    break;
                                }
                            }

                            // FIN on error stream means exec is done
                            if terminal_error_frame {
                                break;
                            }
                        }
                        Ok(crate::spdy::SpdyFrame::SynReply { .. }) => {
                            // SYN_REPLY from containerd — acknowledge stream creation
                            tracing::debug!("Received SYN_REPLY from containerd");
                        }
                        Ok(crate::spdy::SpdyFrame::Ping { id }) => {
                            // Echo ping back
                            if let Err(e) = spdy.write_ping(&mut containerd_stream, id).await {
                                tracing::error!("Failed to send PING: {}", e);
                                break;
                            }
                        }
                        Ok(crate::spdy::SpdyFrame::RstStream { .. }) => {
                            // Stream reset — containerd closed a stream
                            tracing::debug!("Received RST_STREAM from containerd");
                        }
                        Ok(crate::spdy::SpdyFrame::Settings) | Ok(crate::spdy::SpdyFrame::WindowUpdate { .. }) => {
                            // Ignore SETTINGS and WINDOW_UPDATE
                        }
                        Ok(crate::spdy::SpdyFrame::GoAway) => {
                            tracing::info!("Received GOAWAY from containerd");
                            break;
                        }
                        Ok(_) => {
                            // Other frames — ignore
                        }
                        Err(e) => {
                            tracing::error!("Failed to read SPDY frame from containerd: {}", e);
                            break;
                        }
                    }
                }
            }
        }

        // containerd already sent exit status JSON on error stream (stream 7 → channel 3)
        // Don't send duplicate exit status here — let containerd's be the only one
        close_websocket_gracefully(
            &mut ws_sender,
            &mut ws_receiver,
            task_supervisor.as_ref(),
            peer_closed,
        )
        .await;

        tracing::info!(
            "kubectl exec (CRI) completed: pod={}/{}, exit_code={}",
            namespace,
            pod_name,
            exit_code
        );
    } else {
        // Non-interactive mode: use exec_sync
        let result = {
            let mut cri_client = cri.lock().await;
            exec_sync_with_created_state_retry(
                &mut cri_client,
                task_supervisor.as_ref(),
                &container_id,
                &command,
                60,
            )
            .await
        };

        match result {
            Ok(exec_response) => {
                // Send stdout on channel 1
                if !exec_response.stdout.is_empty() {
                    let mut frame = vec![1u8];
                    frame.extend_from_slice(&exec_response.stdout);
                    if let Err(e) = ws_sender
                        .send(TungsteniteMessage::Binary(frame.into()))
                        .await
                    {
                        tracing::error!("Failed to send stdout: {}", e);
                    }
                }

                // Send stderr on channel 2
                if !exec_response.stderr.is_empty() {
                    let mut frame = vec![2u8];
                    frame.extend_from_slice(&exec_response.stderr);
                    if let Err(e) = ws_sender
                        .send(TungsteniteMessage::Binary(frame.into()))
                        .await
                    {
                        tracing::error!("Failed to send stderr: {}", e);
                    }
                }

                // v4/v5 subprotocols expect a structured status frame on channel 3.
                if websocket_uses_structured_status_channel(&subprotocol) {
                    let exit_msg = exec_exit_status(exec_response.exit_code);
                    let mut frame = vec![3u8];
                    frame.extend_from_slice(exit_msg.to_string().as_bytes());
                    let _ = ws_sender
                        .send(TungsteniteMessage::Binary(frame.into()))
                        .await;
                }
            }
            Err(e) => {
                tracing::error!("ExecSync failed: {}", e);
                let mut frame = vec![3u8];
                frame.extend_from_slice(&format_websocket_error_payload(
                    &subprotocol,
                    format!("exec failed: {}", e),
                ));
                let _ = ws_sender
                    .send(TungsteniteMessage::Binary(frame.into()))
                    .await;
            }
        }

        close_websocket_gracefully(
            &mut ws_sender,
            &mut ws_receiver,
            task_supervisor.as_ref(),
            peer_closed,
        )
        .await;
        tracing::info!(
            "kubectl exec (non-interactive) completed: pod={}/{}",
            namespace,
            pod_name
        );
    }
}

pub async fn handle_remote_exec_websocket_tungstenite<S>(
    socket: tokio_tungstenite::WebSocketStream<S>,
    request: RemoteExecWebSocketRequest,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use crate::replication::protocol::{ExecStreamChannel, NodeExecStreamFrame};
    use futures::sink::SinkExt as _;
    use futures::stream::StreamExt as _;
    use tokio_tungstenite::tungstenite::Message as TungsteniteMessage;

    let RemoteExecWebSocketRequest {
        mut session,
        task_supervisor,
        target,
        subprotocol,
        stream_options,
        attach,
    } = request;
    let ExecTarget {
        namespace,
        pod_name,
        container_id,
        command,
    } = target;
    let stdin = stream_options.stdin;
    let tty = stream_options.tty;

    tracing::info!(
        "kubectl remote {} (POST WebSocket): pod={}/{}, container={}, command={:?}, stdin={}, tty={}",
        if attach { "attach" } else { "exec" },
        namespace,
        pod_name,
        container_id,
        command,
        stdin,
        tty
    );

    let (mut ws_sender, mut ws_receiver) = socket.split();
    let mut peer_closed = false;
    let mut stdin_closed = !stdin;
    let stdin_idle_timeout = std::time::Duration::from_secs(2);
    let mut stdin_deadline = tokio::time::Instant::now() + stdin_idle_timeout;

    loop {
        tokio::select! {
            _ = task_supervisor.sleep_until(
                "remote_exec_ws_stdin_idle_timeout",
                stdin_deadline,
            ), if stdin && !tty && !stdin_closed => {
                tracing::info!(
                    "Remote exec stdin idle timeout: {}s in non-TTY mode - closing stdin stream",
                    stdin_idle_timeout.as_secs()
                );
                let _ = session
                    .send_frame(NodeExecStreamFrame {
                        request_id: String::new(),
                        channel: ExecStreamChannel::Stdin,
                        data: Vec::new(),
                        fin: true,
                    })
                    .await;
                stdin_closed = true;
            }

            ws_msg = ws_receiver.next() => {
                match ws_msg {
                    Some(Ok(TungsteniteMessage::Binary(data))) => {
                        if data.is_empty() {
                            continue;
                        }
                        match data[0] {
                            0 if stdin && !stdin_closed => {
                                if data.len() == 1 {
                                    let _ = session
                                        .send_frame(NodeExecStreamFrame {
                                            request_id: String::new(),
                                            channel: ExecStreamChannel::Stdin,
                                            data: Vec::new(),
                                            fin: true,
                                        })
                                        .await;
                                    stdin_closed = true;
                                } else { match session
                                    .send_frame(NodeExecStreamFrame {
                                        request_id: String::new(),
                                        channel: ExecStreamChannel::Stdin,
                                        data: data[1..].to_vec(),
                                        fin: false,
                                    })
                                    .await
                                { Err(e) => {
                                    tracing::error!("Remote exec WebSocket stdin forward failed: {}", e);
                                    break;
                                } _ => {
                                    stdin_deadline = tokio::time::Instant::now() + stdin_idle_timeout;
                                }}}
                            }
                            4 if tty => {
                                if let Err(e) = session
                                    .send_frame(NodeExecStreamFrame {
                                        request_id: String::new(),
                                        channel: ExecStreamChannel::Resize,
                                        data: data[1..].to_vec(),
                                        fin: false,
                                    })
                                    .await
                                {
                                    tracing::error!("Remote exec WebSocket resize forward failed: {}", e);
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                    Some(Ok(TungsteniteMessage::Close(_))) | None => {
                        tracing::info!("Remote exec WebSocket closed by client");
                        peer_closed = true;
                        if stdin && !stdin_closed {
                            let _ = session
                                .send_frame(NodeExecStreamFrame {
                                    request_id: String::new(),
                                    channel: ExecStreamChannel::Stdin,
                                    data: Vec::new(),
                                    fin: true,
                                })
                                .await;
                        }
                        break;
                    }
                    Some(Err(e)) => {
                        tracing::error!("Remote exec WebSocket receive error: {}", e);
                        peer_closed = true;
                        if stdin && !stdin_closed {
                            let _ = session
                                .send_frame(NodeExecStreamFrame {
                                    request_id: String::new(),
                                    channel: ExecStreamChannel::Stdin,
                                    data: Vec::new(),
                                    fin: true,
                                })
                                .await;
                        }
                        break;
                    }
                    _ => {}
                }
            }

            frame = session.recv_frame() => {
                let frame = match frame {
                    Ok(Some(frame)) => frame,
                    Ok(None) => break,
                    Err(e) => {
                        tracing::error!("Remote exec WebSocket receive from follower failed: {}", e);
                        break;
                    }
                };

                let terminal_error_frame = remote_exec_error_frame_is_terminal(&frame);
                let channel = match frame.channel {
                    ExecStreamChannel::Stdout => 1,
                    ExecStreamChannel::Stderr => 2,
                    ExecStreamChannel::Error => 3,
                    ExecStreamChannel::Stdin | ExecStreamChannel::Resize => continue,
                };

                if !frame.data.is_empty() {
                    if frame.channel == ExecStreamChannel::Error
                        && !websocket_uses_structured_status_channel(&subprotocol)
                    {
                        let is_success = serde_json::from_slice::<serde_json::Value>(&frame.data)
                            .ok()
                            .and_then(|v| {
                                v.get("status")
                                    .and_then(|s| s.as_str())
                                    .map(|status| status == "Success")
                            })
                            .unwrap_or(false);
                        if is_success {
                            if terminal_error_frame {
                                break;
                            }
                            continue;
                        }
                    }

                    let mut ws_frame = vec![channel];
                    ws_frame.extend_from_slice(&frame.data);
                    if let Err(e) = ws_sender.send(TungsteniteMessage::Binary(ws_frame.into())).await {
                        tracing::error!("Remote exec WebSocket send failed: {}", e);
                        break;
                    }
                }

                if terminal_error_frame {
                    break;
                }
            }
        }
    }

    session.close().await;
    close_websocket_gracefully(
        &mut ws_sender,
        &mut ws_receiver,
        task_supervisor.as_ref(),
        peer_closed,
    )
    .await;
    tracing::info!(
        "kubectl remote {} (WebSocket) completed: pod={}/{}",
        if attach { "attach" } else { "exec" },
        namespace,
        pod_name
    );
}

pub async fn handle_remote_exec_websocket_sync<S>(
    socket: tokio_tungstenite::WebSocketStream<S>,
    request: RemoteExecWebSocketSyncRequest,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use futures::sink::SinkExt as _;
    use futures::stream::StreamExt as _;
    use tokio_tungstenite::tungstenite::Message as TungsteniteMessage;

    let RemoteExecWebSocketSyncRequest {
        replication,
        target,
        subprotocol,
        node_name,
        task_supervisor,
    } = request;
    let ExecTarget {
        namespace,
        pod_name,
        container_id,
        command,
    } = target;

    tracing::info!(
        "kubectl remote exec-sync (WebSocket): pod={}/{}, container={}, command={:?}",
        namespace,
        pod_name,
        container_id,
        command
    );

    let (mut ws_sender, mut ws_receiver) = socket.split();

    let result = replication
        .request_node_exec_sync(crate::replication::protocol::NodeExecSyncRequest {
            request_id: String::new(),
            node_name,
            namespace: namespace.clone(),
            pod_name: pod_name.clone(),
            container_id: container_id.clone(),
            command: command.clone(),
            timeout_seconds: 300,
        })
        .await;

    match result {
        Ok(response) => {
            if !response.stdout.is_empty() {
                let mut frame = vec![1u8];
                frame.extend_from_slice(&response.stdout);
                if let Err(e) = ws_sender
                    .send(TungsteniteMessage::Binary(frame.into()))
                    .await
                {
                    tracing::error!("Failed to send stdout: {}", e);
                }
            }

            if !response.stderr.is_empty() {
                let mut frame = vec![2u8];
                frame.extend_from_slice(&response.stderr);
                if let Err(e) = ws_sender
                    .send(TungsteniteMessage::Binary(frame.into()))
                    .await
                {
                    tracing::error!("Failed to send stderr: {}", e);
                }
            }

            if let Some(error) = &response.error {
                tracing::error!("Remote exec-sync error: {}", error);
                let mut frame = vec![3u8];
                frame.extend_from_slice(&format_websocket_error_payload(
                    &subprotocol,
                    error.clone(),
                ));
                let _ = ws_sender
                    .send(TungsteniteMessage::Binary(frame.into()))
                    .await;
            } else if websocket_uses_structured_status_channel(&subprotocol) {
                let exit_msg = exec_exit_status(response.exit_code);
                let mut frame = vec![3u8];
                frame.extend_from_slice(exit_msg.to_string().as_bytes());
                let _ = ws_sender
                    .send(TungsteniteMessage::Binary(frame.into()))
                    .await;
            }
        }
        Err(e) => {
            tracing::error!("Remote exec-sync request failed: {}", e);
            let mut frame = vec![3u8];
            frame.extend_from_slice(&format_websocket_error_payload(
                &subprotocol,
                format!("remote exec failed: {}", e),
            ));
            let _ = ws_sender
                .send(TungsteniteMessage::Binary(frame.into()))
                .await;
        }
    }

    close_websocket_gracefully(
        &mut ws_sender,
        &mut ws_receiver,
        task_supervisor.as_ref(),
        false,
    )
    .await;
    tracing::info!(
        "kubectl remote exec-sync (WebSocket) completed: pod={}/{}",
        namespace,
        pod_name
    );
}

// GET /api/v1/namespaces/{ns}/pods/{name}/status
