use super::*;
use crate::api::AdmissionContextRequest;

/// Build a K8s metav1.Status JSON for exec exit code (v4/v5 compatible).
/// v5 requires `metadata` and `details` fields; v4 tolerates them.
pub fn exec_exit_status(exit_code: i32) -> serde_json::Value {
    if exit_code == 0 {
        serde_json::json!({
            "metadata": {},
            "status": "Success",
            "details": {"causes": []}
        })
    } else {
        serde_json::json!({
            "metadata": {},
            "status": "Failure",
            "message": format!("command terminated with exit code {}", exit_code),
            "reason": "NonZeroExitCode",
            "details": {
                "causes": [{
                    "reason": "ExitCode",
                    "message": exit_code.to_string()
                }]
            }
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ExecStreamOptions {
    pub stdin: bool,
    pub stdout: bool,
    pub stderr: bool,
    pub tty: bool,
}

#[derive(Debug, Clone)]
pub struct ExecTarget {
    pub namespace: String,
    pub pod_name: String,
    pub container_id: String,
    pub command: Vec<String>,
}

pub struct ExecRequest<'a> {
    pub container_id: &'a str,
    pub command: &'a [String],
    pub stream_options: ExecStreamOptions,
}

struct LocalPodExecSpdyStreamRequest {
    req: Request,
    cri: Arc<tokio::sync::Mutex<crate::kubelet::cri::CriClient>>,
    target: ExecTarget,
    stream_options: ExecStreamOptions,
}

struct RemotePodExecStreamRequest {
    req: Request,
    node_name: String,
    target: ExecTarget,
    stream_options: ExecStreamOptions,
}

struct RemotePodExecSyncRequest {
    req: Request,
    node_name: String,
    target: ExecTarget,
}

pub async fn pod_exec(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    RawQuery(query): RawQuery,
    req: Request,
) -> Result<Response, AppError> {
    // Parse query parameters
    let query_str = query.unwrap_or_default();
    let (command, container, stdin, stdout, stderr, tty) = parse_exec_query(&query_str);
    let stream_options = ExecStreamOptions {
        stdin,
        stdout,
        stderr,
        tty,
    };

    // Get pod from PodRepository to find container ID
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
            subresource: Some("exec"),
            options: None,
        }),
    )
    .await?;

    // Extract container ID from pod status
    let container_id = extract_container_id(&pod.data, container.as_deref())?;
    let remote_node = remote_pod_node_name(&pod.data, &state.config.node_name);

    // Check for WebSocket upgrade (kubectl v1.29+ uses WebSocket with v5 subprotocol)
    let upgrade_header = req
        .headers()
        .get(header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if let Some(node_name) = remote_node {
        if crate::api_pod_subresources::exec_spdy::is_spdy_upgrade(req.headers()) {
            return pod_exec_remote_spdy_stream(
                state,
                RemotePodExecStreamRequest {
                    req,
                    node_name,
                    target: ExecTarget {
                        namespace,
                        pod_name: name,
                        container_id,
                        command,
                    },
                    stream_options,
                },
            )
            .await;
        }
        if !upgrade_header.eq_ignore_ascii_case("websocket") {
            return Err(AppError::BadRequest(format!(
                "Pod exec for pod on remote node '{}' requires WebSocket upgrade",
                node_name
            )));
        }
        if !stdin && !tty {
            return pod_exec_remote_websocket_sync(
                state,
                RemotePodExecSyncRequest {
                    req,
                    node_name,
                    target: ExecTarget {
                        namespace,
                        pod_name: name,
                        container_id,
                        command,
                    },
                },
            )
            .await;
        }
        return pod_exec_remote_websocket_stream(
            state,
            RemotePodExecStreamRequest {
                req,
                node_name,
                target: ExecTarget {
                    namespace,
                    pod_name: name,
                    container_id,
                    command,
                },
                stream_options,
            },
        )
        .await;
    }

    // Check if CRI is available for local pod exec.
    let cri_arc = state.cri.as_ref().cloned().ok_or_else(|| {
        AppError::Internal("CRI client not available (containerd not running)".to_string())
    })?;

    if crate::api_pod_subresources::exec_spdy::is_spdy_upgrade(req.headers()) {
        pod_exec_local_spdy_stream(
            state,
            LocalPodExecSpdyStreamRequest {
                req,
                cri: cri_arc.clone(),
                target: ExecTarget {
                    namespace,
                    pod_name: name,
                    container_id,
                    command,
                },
                stream_options,
            },
        )
        .await
    } else if upgrade_header.eq_ignore_ascii_case("websocket") {
        // Handle WebSocket upgrade (modern kubectl)
        // kubectl sends POST with Upgrade: websocket, which axum::extract::ws::WebSocketUpgrade
        // rejects because WebSocket spec requires GET. We manually upgrade here.

        // Extract WebSocket key for handshake
        let ws_key = req
            .headers()
            .get(header::SEC_WEBSOCKET_KEY)
            .ok_or_else(|| AppError::BadRequest("Missing Sec-WebSocket-Key header".to_string()))?
            .clone();

        let subprotocol = negotiate_websocket_subprotocol(req.headers()).ok_or_else(|| {
            AppError::BadRequest("Missing or unsupported Sec-WebSocket-Protocol".to_string())
        })?;
        let selected_subprotocol = subprotocol.clone();

        // Clone what we need for the WebSocket handler
        let cri_clone = cri_arc.clone();
        let task_supervisor = state.task_supervisor.clone();
        let target = ExecTarget {
            namespace,
            pod_name: name,
            container_id,
            command,
        };

        // Perform WebSocket handshake manually
        let on_upgrade = hyper::upgrade::on(req);

        if let Err(err) = state
            .task_supervisor
            .spawn_async(
                crate::task_supervisor::TaskCategory::Others,
                "pod_exec_ws_upgrade",
                async move {
                    match on_upgrade.await {
                        Ok(upgraded) => {
                            // Wrap hyper::Upgraded in TokioIo for AsyncRead/AsyncWrite compatibility
                            use hyper_util::rt::TokioIo;
                            let io = TokioIo::new(upgraded);

                            // Use tokio_tungstenite to wrap the upgraded connection
                            use tokio_tungstenite::WebSocketStream;
                            let ws_stream = WebSocketStream::from_raw_socket(
                                io,
                                tokio_tungstenite::tungstenite::protocol::Role::Server,
                                None, // Use default config
                            )
                            .await;

                            // Handle exec using tungstenite WebSocket
                            handle_exec_websocket_tungstenite(
                                ws_stream,
                                ExecWebSocketRequest {
                                    cri: cri_clone,
                                    task_supervisor,
                                    target,
                                    subprotocol: selected_subprotocol,
                                    stream_options,
                                },
                            )
                            .await;
                        }
                        Err(e) => {
                            tracing::error!("WebSocket upgrade failed: {}", e);
                        }
                    }
                },
            )
            .await
        {
            tracing::warn!("Failed to spawn pod exec WebSocket upgrade task: {}", err);
        }

        // Build 101 Switching Protocols response
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
        Err(AppError::BadRequest(format!(
            "Invalid Upgrade header: {}. WebSocket upgrade required",
            upgrade_header
        )))
    }
}

async fn pod_exec_local_spdy_stream(
    state: Arc<AppState>,
    request: LocalPodExecSpdyStreamRequest,
) -> Result<Response, AppError> {
    let LocalPodExecSpdyStreamRequest {
        req,
        cri,
        target,
        stream_options,
    } = request;

    if stream_options.stdin || stream_options.tty {
        return Err(AppError::BadRequest(
            "SPDY exec currently supports non-interactive commands; use WebSocket for stdin/tty"
                .to_string(),
        ));
    }

    let selected_subprotocol =
        crate::api_pod_subresources::exec_spdy::negotiate_spdy_subprotocol(req.headers());
    let on_upgrade = hyper::upgrade::on(req);
    let task_supervisor = state.task_supervisor.clone();
    let task_supervisor_for_handler = task_supervisor.clone();
    let request = crate::api_pod_subresources::exec_spdy::SpdyExecStreamRequest {
        stdin: stream_options.stdin,
        stdout: stream_options.stdout,
        stderr: stream_options.stderr,
        tty: stream_options.tty,
    };

    if let Err(err) = task_supervisor
        .spawn_async(
            crate::task_supervisor::TaskCategory::Others,
            "pod_exec_spdy_upgrade",
            async move {
                match on_upgrade.await {
                    Ok(upgraded) => {
                        let io = hyper_util::rt::TokioIo::new(upgraded);
                        crate::api_pod_subresources::exec_spdy::handle_local_exec_spdy(
                            io,
                            crate::api_pod_subresources::exec_spdy::LocalExecSpdyRequest {
                                cri,
                                task_supervisor: task_supervisor_for_handler,
                                target,
                                stream_request: request,
                            },
                        )
                        .await;
                    }
                    Err(err) => {
                        tracing::error!("SPDY exec upgrade failed: {}", err);
                    }
                }
            },
        )
        .await
    {
        tracing::warn!("Failed to spawn pod exec SPDY task: {}", err);
    }

    crate::api_pod_subresources::exec_spdy::spdy_switching_protocols_response(selected_subprotocol)
}

async fn pod_exec_remote_websocket_stream(
    state: Arc<AppState>,
    request: RemotePodExecStreamRequest,
) -> Result<Response, AppError> {
    let RemotePodExecStreamRequest {
        req,
        node_name,
        target,
        stream_options,
    } = request;
    let replication = state.replication.as_ref().cloned().ok_or_else(|| {
        AppError::Internal("replication service not available for remote pod exec".to_string())
    })?;

    let ws_key = req
        .headers()
        .get(header::SEC_WEBSOCKET_KEY)
        .ok_or_else(|| AppError::BadRequest("Missing Sec-WebSocket-Key header".to_string()))?
        .clone();

    let subprotocol = negotiate_websocket_subprotocol(req.headers()).ok_or_else(|| {
        AppError::BadRequest("Missing or unsupported Sec-WebSocket-Protocol".to_string())
    })?;
    let selected_subprotocol = subprotocol.clone();
    let task_supervisor = state.task_supervisor.clone();

    let on_upgrade = hyper::upgrade::on(req);
    let handler_target = target.clone();
    if let Err(err) = state
        .task_supervisor
        .spawn_async(
            crate::task_supervisor::TaskCategory::Others,
            "pod_exec_remote_websocket_stream_upgrade",
            async move {
                match on_upgrade.await {
                    Ok(upgraded) => {
                        use hyper_util::rt::TokioIo;
                        use tokio_tungstenite::WebSocketStream;

                        let io = TokioIo::new(upgraded);
                        let ws_stream = WebSocketStream::from_raw_socket(
                            io,
                            tokio_tungstenite::tungstenite::protocol::Role::Server,
                            None,
                        )
                        .await;

                        match replication
                            .open_node_exec_stream(crate::replication::protocol::NodeExecRequest {
                                request_id: String::new(),
                                node_name,
                                namespace: target.namespace,
                                pod_name: target.pod_name,
                                container_id: target.container_id,
                                command: target.command,
                                tty: stream_options.tty,
                                stdin: stream_options.stdin,
                                stdout: stream_options.stdout,
                                stderr: stream_options.stderr,
                            })
                            .await
                        {
                            Ok(session) => {
                                handle_remote_exec_websocket_tungstenite(
                                    ws_stream,
                                    RemoteExecWebSocketRequest {
                                        session,
                                        task_supervisor,
                                        target: handler_target,
                                        subprotocol: selected_subprotocol,
                                        stream_options,
                                    },
                                )
                                .await;
                            }
                            Err(err) => {
                                tracing::error!(
                                    "Remote WebSocket exec stream open failed: {}",
                                    err
                                );
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("Remote WebSocket exec upgrade failed: {}", e);
                    }
                }
            },
        )
        .await
    {
        tracing::warn!("Failed to spawn remote pod exec WebSocket task: {}", err);
    }

    Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header(header::UPGRADE, "websocket")
        .header(header::CONNECTION, "Upgrade")
        .header(
            header::SEC_WEBSOCKET_ACCEPT,
            derive_websocket_accept_key(&ws_key),
        )
        .header(header::SEC_WEBSOCKET_PROTOCOL, subprotocol)
        .body(axum::body::Body::empty())
        .map_err(|e| AppError::Internal(format!("Failed to build WebSocket response: {}", e)))
}

async fn pod_exec_remote_spdy_stream(
    state: Arc<AppState>,
    request: RemotePodExecStreamRequest,
) -> Result<Response, AppError> {
    let RemotePodExecStreamRequest {
        req,
        node_name,
        target,
        stream_options,
    } = request;

    if stream_options.stdin || stream_options.tty {
        return Err(AppError::BadRequest(format!(
            "SPDY exec for pod on remote node '{}' currently supports non-interactive commands; use WebSocket for stdin/tty",
            node_name
        )));
    }

    let replication = state.replication.as_ref().cloned().ok_or_else(|| {
        AppError::Internal("replication service not available for remote pod exec".to_string())
    })?;
    let selected_subprotocol =
        crate::api_pod_subresources::exec_spdy::negotiate_spdy_subprotocol(req.headers());
    let task_supervisor = state.task_supervisor.clone();
    let task_supervisor_for_handler = task_supervisor.clone();
    let request = crate::api_pod_subresources::exec_spdy::SpdyExecStreamRequest {
        stdin: stream_options.stdin,
        stdout: stream_options.stdout,
        stderr: stream_options.stderr,
        tty: stream_options.tty,
    };

    let on_upgrade = hyper::upgrade::on(req);
    if let Err(err) = task_supervisor
        .spawn_async(
            crate::task_supervisor::TaskCategory::Others,
            "pod_exec_remote_spdy_upgrade",
            async move {
                match on_upgrade.await {
                    Ok(upgraded) => {
                        let io = hyper_util::rt::TokioIo::new(upgraded);
                        crate::api_pod_subresources::exec_spdy::handle_remote_exec_spdy(
                            io,
                            crate::api_pod_subresources::exec_spdy::RemoteExecSpdyRequest {
                                replication,
                                task_supervisor: task_supervisor_for_handler,
                                node_name,
                                target,
                                stream_request: request,
                            },
                        )
                        .await;
                    }
                    Err(err) => {
                        tracing::error!("Remote SPDY exec upgrade failed: {}", err);
                    }
                }
            },
        )
        .await
    {
        tracing::warn!("Failed to spawn remote pod exec SPDY task: {}", err);
    }

    crate::api_pod_subresources::exec_spdy::spdy_switching_protocols_response(selected_subprotocol)
}

async fn pod_exec_remote_websocket_sync(
    state: Arc<AppState>,
    request: RemotePodExecSyncRequest,
) -> Result<Response, AppError> {
    let RemotePodExecSyncRequest {
        req,
        node_name,
        target,
    } = request;
    let replication = state.replication.as_ref().cloned().ok_or_else(|| {
        AppError::Internal("replication service not available for remote pod exec".to_string())
    })?;

    let ws_key = req
        .headers()
        .get(header::SEC_WEBSOCKET_KEY)
        .ok_or_else(|| AppError::BadRequest("Missing Sec-WebSocket-Key header".to_string()))?
        .clone();

    let subprotocol = negotiate_websocket_subprotocol(req.headers()).ok_or_else(|| {
        AppError::BadRequest("Missing or unsupported Sec-WebSocket-Protocol".to_string())
    })?;
    let selected_subprotocol = subprotocol.clone();
    let task_supervisor = state.task_supervisor.clone();

    let on_upgrade = hyper::upgrade::on(req);
    if let Err(err) = state
        .task_supervisor
        .spawn_async(
            crate::task_supervisor::TaskCategory::Others,
            "pod_exec_remote_ws_sync_upgrade",
            async move {
                match on_upgrade.await {
                    Ok(upgraded) => {
                        use hyper_util::rt::TokioIo;
                        use tokio_tungstenite::WebSocketStream;

                        let io = TokioIo::new(upgraded);
                        let ws_stream = WebSocketStream::from_raw_socket(
                            io,
                            tokio_tungstenite::tungstenite::protocol::Role::Server,
                            None,
                        )
                        .await;

                        handle_remote_exec_websocket_sync(
                            ws_stream,
                            RemoteExecWebSocketSyncRequest {
                                replication,
                                target,
                                subprotocol: selected_subprotocol,
                                node_name,
                                task_supervisor,
                            },
                        )
                        .await;
                    }
                    Err(e) => {
                        tracing::error!("Remote WebSocket exec-sync upgrade failed: {}", e);
                    }
                }
            },
        )
        .await
    {
        tracing::warn!("Failed to spawn remote pod exec WS sync task: {}", err);
    }

    Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header(header::UPGRADE, "websocket")
        .header(header::CONNECTION, "Upgrade")
        .header(
            header::SEC_WEBSOCKET_ACCEPT,
            derive_websocket_accept_key(&ws_key),
        )
        .header(header::SEC_WEBSOCKET_PROTOCOL, subprotocol)
        .body(axum::body::Body::empty())
        .map_err(|e| AppError::Internal(format!("Failed to build WebSocket response: {}", e)))
}

// POST /api/v1/namespaces/{ns}/pods/{name}/attach
// Admission-aware attach endpoint. Streaming attach wiring is intentionally deferred.
pub async fn pod_attach(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    RawQuery(query): RawQuery,
    _req: Request,
) -> Result<Response, AppError> {
    let query_str = query.unwrap_or_default();
    let (container, stdin, stdout, stderr, tty) = parse_attach_query(&query_str);

    let _pod = crate::kubelet::pod_repository::PodReader::get_pod(
        state.pod_repository.as_ref(),
        &namespace,
        &name,
    )
    .await?
    .ok_or_else(|| AppError::NotFound(format!("Pod {}/{} not found", namespace, name)))?;

    let mut attach_options = serde_json::json!({
        "apiVersion": "v1",
        "kind": "PodAttachOptions",
        "stdin": stdin,
        "stdout": stdout,
        "stderr": stderr,
        "tty": tty,
    });
    if let Some(container_name) = container.clone()
        && let Some(obj) = attach_options.as_object_mut()
    {
        obj.insert("container".to_string(), Value::String(container_name));
    }

    let _ = run_admission_for_request(
        state.db.as_ref(),
        build_admission_context(AdmissionContextRequest {
            api_version: "v1",
            kind: "Pod",
            operation: "CONNECT",
            namespace: Some(namespace),
            name: Some(name),
            object: attach_options,
            old_object: None,
            dry_run: false,
            subresource: Some("attach"),
            options: None,
        }),
    )
    .await?;

    let container_msg = container
        .as_deref()
        .map(|c| format!(" for container '{}'", c))
        .unwrap_or_default();
    Err(AppError::NotImplemented(format!(
        "Pod attach{} is not implemented yet",
        container_msg
    )))
}

// Derive Sec-WebSocket-Accept key from Sec-WebSocket-Key (RFC 6455)
pub fn derive_websocket_accept_key(key: &header::HeaderValue) -> String {
    use sha1::{Digest, Sha1};
    const WEBSOCKET_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    let mut hasher = Sha1::new();
    hasher.update(key.as_bytes());
    hasher.update(WEBSOCKET_GUID.as_bytes());
    let hash = hasher.finalize();

    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(hash)
}

pub fn negotiate_websocket_subprotocol(headers: &header::HeaderMap) -> Option<String> {
    const PREFERRED: &[&str] = &[
        "v5.channel.k8s.io",
        "v4.channel.k8s.io",
        "v3.channel.k8s.io",
        "v2.channel.k8s.io",
        "channel.k8s.io",
        "base64.channel.k8s.io",
    ];

    let mut offered = Vec::new();
    for value in headers.get_all(header::SEC_WEBSOCKET_PROTOCOL) {
        if let Ok(raw) = value.to_str() {
            offered.extend(
                raw.split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string()),
            );
        }
    }

    for preferred in PREFERRED {
        if offered.iter().any(|offered| offered == preferred) {
            return Some((*preferred).to_string());
        }
    }
    None
}

pub fn websocket_uses_structured_status_channel(subprotocol: &str) -> bool {
    matches!(subprotocol, "v4.channel.k8s.io" | "v5.channel.k8s.io")
}

pub fn format_websocket_error_payload(subprotocol: &str, message: String) -> Vec<u8> {
    if websocket_uses_structured_status_channel(subprotocol) {
        serde_json::json!({
            "metadata": {},
            "status": "Failure",
            "message": message,
            "details": {"causes": []}
        })
        .to_string()
        .into_bytes()
    } else {
        message.into_bytes()
    }
}

// Parse query string to extract command[] parameters and other flags
pub fn parse_exec_query(query: &str) -> (Vec<String>, Option<String>, bool, bool, bool, bool) {
    let mut command = Vec::new();
    let mut container = None;
    let mut stdin = false;
    let mut stdout = false;
    let mut stderr = false;
    let mut tty = false;

    for pair in query.split('&') {
        if let Some((key, value)) = pair.split_once('=') {
            match key {
                "command" => {
                    // URL decode the value (+ means space in query strings)
                    let form_decoded = value.replace('+', " ");
                    if let Ok(decoded) = urlencoding::decode(&form_decoded) {
                        command.push(decoded.to_string());
                    }
                }
                "container" => {
                    let form_decoded = value.replace('+', " ");
                    if let Ok(decoded) = urlencoding::decode(&form_decoded) {
                        container = Some(decoded.to_string());
                    }
                }
                "stdin" => stdin = value == "true" || value == "1",
                "stdout" => stdout = value == "true" || value == "1",
                "stderr" => stderr = value == "true" || value == "1",
                "tty" => tty = value == "true" || value == "1",
                _ => {}
            }
        }
    }

    (command, container, stdin, stdout, stderr, tty)
}

pub fn parse_attach_query(query: &str) -> (Option<String>, bool, bool, bool, bool) {
    let mut container = None;
    let mut stdin = false;
    let mut stdout = false;
    let mut stderr = false;
    let mut tty = false;

    for pair in query.split('&') {
        if let Some((key, value)) = pair.split_once('=') {
            match key {
                "container" => {
                    let form_decoded = value.replace('+', " ");
                    if let Ok(decoded) = urlencoding::decode(&form_decoded) {
                        container = Some(decoded.to_string());
                    }
                }
                "stdin" => stdin = value == "true" || value == "1",
                "stdout" => stdout = value == "true" || value == "1",
                "stderr" => stderr = value == "true" || value == "1",
                "tty" => tty = value == "true" || value == "1",
                _ => {}
            }
        }
    }

    (container, stdin, stdout, stderr, tty)
}

pub fn remote_pod_node_name(pod_data: &Value, local_node_name: &str) -> Option<String> {
    pod_data
        .pointer("/spec/nodeName")
        .and_then(Value::as_str)
        .filter(|node_name| !node_name.is_empty() && *node_name != local_node_name)
        .map(str::to_string)
}

// Extract container ID from pod status
pub fn extract_container_id(
    pod_data: &Value,
    container_name: Option<&str>,
) -> Result<String, AppError> {
    let container_statuses = pod_data
        .get("status")
        .and_then(|s| s.get("containerStatuses"))
        .and_then(|cs| cs.as_array())
        .cloned()
        .unwrap_or_default();
    let ephemeral_statuses = pod_data
        .get("status")
        .and_then(|s| s.get("ephemeralContainerStatuses"))
        .and_then(|cs| cs.as_array())
        .cloned()
        .unwrap_or_default();
    let statuses: Vec<Value> = container_statuses
        .into_iter()
        .chain(ephemeral_statuses)
        .collect();
    if statuses.is_empty() {
        return Err(AppError::BadRequest(
            "Pod has no container statuses".to_string(),
        ));
    }

    // If container name specified, find it; otherwise use first container
    let status = if let Some(name) = container_name {
        statuses
            .iter()
            .find(|s| s.get("name").and_then(|n| n.as_str()) == Some(name))
            .ok_or_else(|| AppError::NotFound(format!("Container '{}' not found in pod", name)))?
    } else {
        statuses
            .first()
            .ok_or_else(|| AppError::BadRequest("Pod has no containers".to_string()))?
    };

    // Extract container ID (format: "containerd://abc123")
    let container_id_full = status
        .get("containerID")
        .and_then(|id| id.as_str())
        .ok_or_else(|| AppError::BadRequest("Container ID not found in status".to_string()))?;

    // Strip "containerd://" prefix
    let container_id = container_id_full
        .strip_prefix("containerd://")
        .unwrap_or(container_id_full)
        .to_string();

    Ok(container_id)
}

pub async fn exec_sync_with_created_state_retry(
    cri_client: &mut crate::kubelet::cri::CriClient,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
    container_id: &str,
    command: &[String],
    timeout_seconds: i64,
) -> anyhow::Result<k8s_cri::v1::ExecSyncResponse> {
    use std::time::Duration;

    let first = cri_client
        .exec_sync(container_id, command, timeout_seconds)
        .await;
    if first
        .as_ref()
        .err()
        .map(|e| e.to_string().contains("CONTAINER_CREATED state"))
        != Some(true)
    {
        return first;
    }

    let _ = task_supervisor
        .sleep(
            "exec_sync_retry_created_state_250ms",
            Duration::from_millis(250),
        )
        .await;
    let second = cri_client
        .exec_sync(container_id, command, timeout_seconds)
        .await;
    if second
        .as_ref()
        .err()
        .map(|e| e.to_string().contains("CONTAINER_CREATED state"))
        != Some(true)
    {
        return second;
    }

    let _ = task_supervisor
        .sleep(
            "exec_sync_retry_created_state_500ms",
            Duration::from_millis(500),
        )
        .await;
    cri_client
        .exec_sync(container_id, command, timeout_seconds)
        .await
}

pub async fn exec_with_created_state_retry(
    cri_client: &mut crate::kubelet::cri::CriClient,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
    request: ExecRequest<'_>,
) -> anyhow::Result<k8s_cri::v1::ExecResponse> {
    use std::time::Duration;

    let ExecRequest {
        container_id,
        command,
        stream_options,
    } = request;
    let first = cri_client
        .exec(
            container_id,
            command,
            stream_options.tty,
            stream_options.stdin,
            stream_options.stdout,
            stream_options.stderr,
        )
        .await;
    if first
        .as_ref()
        .err()
        .map(|e| e.to_string().contains("CONTAINER_CREATED state"))
        != Some(true)
    {
        return first;
    }

    let _ = task_supervisor
        .sleep("exec_retry_created_state_250ms", Duration::from_millis(250))
        .await;
    let second = cri_client
        .exec(
            container_id,
            command,
            stream_options.tty,
            stream_options.stdin,
            stream_options.stdout,
            stream_options.stderr,
        )
        .await;
    if second
        .as_ref()
        .err()
        .map(|e| e.to_string().contains("CONTAINER_CREATED state"))
        != Some(true)
    {
        return second;
    }

    let _ = task_supervisor
        .sleep("exec_retry_created_state_500ms", Duration::from_millis(500))
        .await;
    cri_client
        .exec(
            container_id,
            command,
            stream_options.tty,
            stream_options.stdin,
            stream_options.stdout,
            stream_options.stderr,
        )
        .await
}

// Handle WebSocket connection for exec (GET upgrade path - currently unused, kept for future use)
pub async fn handle_exec_websocket(
    socket: WebSocket,
    cri: Arc<tokio::sync::Mutex<crate::kubelet::cri::CriClient>>,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
    container_id: String,
    command: Vec<String>,
    namespace: String,
    pod_name: String,
) {
    tracing::info!(
        "kubectl exec: pod={}/{}, container={}, command={:?}",
        namespace,
        pod_name,
        container_id,
        command
    );

    let (mut ws_sender, mut _ws_receiver) = socket.split();

    // Call CRI ExecSync (non-interactive)
    let result = {
        let mut cri_client = cri.lock().await;
        exec_sync_with_created_state_retry(
            &mut cri_client,
            task_supervisor,
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
                let mut frame = vec![1u8]; // Channel 1 = stdout
                frame.extend_from_slice(&exec_response.stdout);
                if let Err(e) = ws_sender.send(Message::Binary(Bytes::from(frame))).await {
                    tracing::error!("Failed to send stdout: {}", e);
                }
            }

            // Send stderr on channel 2
            if !exec_response.stderr.is_empty() {
                let mut frame = vec![2u8]; // Channel 2 = stderr
                frame.extend_from_slice(&exec_response.stderr);
                if let Err(e) = ws_sender.send(Message::Binary(Bytes::from(frame))).await {
                    tracing::error!("Failed to send stderr: {}", e);
                }
            }

            // Send exit code on channel 3 (error channel)
            let exit_msg = exec_exit_status(exec_response.exit_code);
            let mut frame = vec![3u8]; // Channel 3 = error/status
            frame.extend_from_slice(exit_msg.to_string().as_bytes());
            let _ = ws_sender.send(Message::Binary(Bytes::from(frame))).await;
        }
        Err(e) => {
            tracing::error!("ExecSync failed: {}", e);
            // Send error on channel 3
            let error_msg = serde_json::json!({
                "metadata": {},
                "status": "Failure",
                "message": format!("exec failed: {}", e),
                "details": {"causes": []}
            });
            let mut frame = vec![3u8];
            frame.extend_from_slice(error_msg.to_string().as_bytes());
            let _ = ws_sender.send(Message::Binary(Bytes::from(frame))).await;
        }
    }

    // Close WebSocket with proper 1000 Normal Closure
    let _ = ws_sender
        .send(Message::Close(Some(axum::extract::ws::CloseFrame {
            code: axum::extract::ws::close_code::NORMAL,
            reason: "".into(),
        })))
        .await;
    tracing::info!("kubectl exec completed: pod={}/{}", namespace, pod_name);
}

// Handle WebSocket connection for exec using tokio_tungstenite (for POST upgrade)
