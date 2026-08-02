//! Kubernetes WebSocket exec/attach channel adaptation.

use super::*;

use klights_node_api::{
    ExecStreamChannel, NodeExec, NodeExecFrame, NodeExecSession, NodeExecSyncRequest,
    NodeExecTarget,
};

pub struct RemoteExecWebSocketRequest {
    pub session: Box<dyn NodeExecSession>,
    pub task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    pub target: ExecTarget,
    pub subprotocol: String,
    pub stream_options: ExecStreamOptions,
    pub attach: bool,
}

pub struct RemoteExecWebSocketSyncRequest {
    pub node_exec: Arc<dyn NodeExec>,
    pub target: ExecTarget,
    pub subprotocol: String,
    pub node_name: String,
    pub task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
}

pub fn remote_exec_error_frame_is_terminal(frame: &NodeExecFrame) -> bool {
    frame.is_terminal()
}

async fn close_websocket_gracefully<S>(
    ws_sender: &mut futures::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<S>,
        tokio_tungstenite::tungstenite::Message,
    >,
    ws_receiver: &mut futures::stream::SplitStream<tokio_tungstenite::WebSocketStream<S>>,
    task_supervisor: &klights_supervisor::TaskSupervisor,
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

pub async fn handle_remote_exec_websocket_tungstenite<S>(
    socket: tokio_tungstenite::WebSocketStream<S>,
    request: RemoteExecWebSocketRequest,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
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
                    .send_frame(NodeExecFrame::new(
                        ExecStreamChannel::Stdin,
                        Vec::new(),
                        true,
                    ))
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
                                        .send_frame(NodeExecFrame::new(
                                            ExecStreamChannel::Stdin,
                                            Vec::new(),
                                            true,
                                        ))
                                        .await;
                                    stdin_closed = true;
                                } else { match session
                                    .send_frame(NodeExecFrame::new(
                                        ExecStreamChannel::Stdin,
                                        data[1..].to_vec(),
                                        false,
                                    ))
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
                                    .send_frame(NodeExecFrame::new(
                                        ExecStreamChannel::Resize,
                                        data[1..].to_vec(),
                                        false,
                                    ))
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
                        // A close must not queue behind a backpressured stdin
                        // FIN. Cancel the local session boundary immediately.
                        let _ = session.cancel().await;
                        break;
                    }
                    Some(Err(e)) => {
                        tracing::error!("Remote exec WebSocket receive error: {}", e);
                        peer_closed = true;
                        let _ = session.cancel().await;
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
                let channel = match frame.channel() {
                    ExecStreamChannel::Stdout => 1,
                    ExecStreamChannel::Stderr => 2,
                    ExecStreamChannel::Error => 3,
                    ExecStreamChannel::Stdin | ExecStreamChannel::Resize => continue,
                };

                if !frame.data().is_empty() {
                    if frame.channel() == ExecStreamChannel::Error
                        && !websocket_uses_structured_status_channel(&subprotocol)
                    {
                        let is_success = serde_json::from_slice::<serde_json::Value>(frame.data())
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
                    ws_frame.extend_from_slice(frame.data());
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

    let _ = session.cancel().await;
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
        node_exec,
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

    let result = NodeExecTarget::try_new(
        node_name,
        namespace.clone(),
        pod_name.clone(),
        container_id.clone(),
    )
    .and_then(|target| NodeExecSyncRequest::try_new(target, command.clone(), 300))
    .map(|request| node_exec.exec_sync(request));
    let result = match result {
        Ok(result) => result.await,
        Err(error) => Err(error),
    };

    match result {
        Ok(response) => {
            if !response.stdout().is_empty() {
                let mut frame = vec![1u8];
                frame.extend_from_slice(response.stdout());
                if let Err(e) = ws_sender
                    .send(TungsteniteMessage::Binary(frame.into()))
                    .await
                {
                    tracing::error!("Failed to send stdout: {}", e);
                }
            }

            if !response.stderr().is_empty() {
                let mut frame = vec![2u8];
                frame.extend_from_slice(response.stderr());
                if let Err(e) = ws_sender
                    .send(TungsteniteMessage::Binary(frame.into()))
                    .await
                {
                    tracing::error!("Failed to send stderr: {}", e);
                }
            }

            if let Some(error) = response.terminal_error() {
                tracing::error!("Remote exec-sync error: {}", error);
                let mut frame = vec![3u8];
                frame.extend_from_slice(&format_websocket_error_payload(
                    &subprotocol,
                    error.message().to_string(),
                ));
                let _ = ws_sender
                    .send(TungsteniteMessage::Binary(frame.into()))
                    .await;
            } else if websocket_uses_structured_status_channel(&subprotocol) {
                let exit_msg = exec_exit_status(response.exit_code());
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
