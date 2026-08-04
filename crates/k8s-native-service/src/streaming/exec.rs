//! Kubernetes Pod exec and attach HTTP adaptation.

use super::*;
use klights_node_api::{
    ExecStreamOptions as NodeExecStreamOptions, NodeExec, NodeExecRequest, NodeExecTarget,
};

#[derive(Debug, Clone, Copy)]
pub struct ExecStreamOptions {
    pub stdin: bool,
    pub stdout: bool,
    pub stderr: bool,
    pub tty: bool,
}

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

#[derive(Debug, Clone)]
pub struct ExecTarget {
    pub namespace: String,
    pub pod_name: String,
    pub container_id: String,
    pub command: Vec<String>,
}

struct RemotePodExecStreamRequest {
    req: Request,
    node_exec: Arc<dyn NodeExec>,
    node_name: String,
    target: ExecTarget,
    stream_options: ExecStreamOptions,
    attach: bool,
}

struct RemotePodExecSyncRequest {
    req: Request,
    node_exec: Arc<dyn NodeExec>,
    node_name: String,
    target: ExecTarget,
}

pub async fn pod_exec<S>(
    State(state): State<Arc<S>>,
    Path((namespace, name)): Path<(String, String)>,
    RawQuery(query): RawQuery,
    req: Request,
) -> Result<Response, AppError>
where
    S: StreamingState + 'static,
{
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
    let pod = get_pod(
        state.streaming_dependencies().pod_query.as_ref(),
        &namespace,
        &name,
    )
    .await?
    .ok_or_else(|| AppError::NotFound(format!("Pod {}/{} not found", namespace, name)))?;

    let _ = run_admission_for_request(
        state.streaming_admission(),
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
    let remote_node = remote_pod_node_name(
        &pod.data,
        state.streaming_dependencies().local_node_name.as_ref(),
    );

    // Kubernetes v1.34 remotecommand uses WebSocket with the v5 subprotocol.
    let upgrade_header = req
        .headers()
        .get(header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if let Some(node_name) = remote_node {
        let node_exec = state
            .streaming_dependencies()
            .remote_node_exec
            .clone()
            .ok_or_else(|| {
                AppError::Internal(
                    "replication service not available for remote pod exec".to_string(),
                )
            })?;
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
                    node_exec,
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
                node_exec,
                node_name,
                target: ExecTarget {
                    namespace,
                    pod_name: name,
                    container_id,
                    command,
                },
                stream_options,
                attach: false,
            },
        )
        .await;
    }

    if !upgrade_header.eq_ignore_ascii_case("websocket") {
        return Err(AppError::BadRequest(
            "Pod exec requires WebSocket upgrade".to_string(),
        ));
    }

    let node_exec = state
        .streaming_dependencies()
        .local_node_exec
        .as_ref()
        .cloned()
        .ok_or_else(|| {
            AppError::Internal("local node exec runtime is not available".to_string())
        })?;
    let node_name = state.streaming_dependencies().local_node_name.to_string();
    let target = ExecTarget {
        namespace,
        pod_name: name,
        container_id,
        command,
    };
    if !stdin && !tty {
        pod_exec_remote_websocket_sync(
            state,
            RemotePodExecSyncRequest {
                req,
                node_exec,
                node_name,
                target,
            },
        )
        .await
    } else {
        pod_exec_remote_websocket_stream(
            state,
            RemotePodExecStreamRequest {
                req,
                node_exec,
                node_name,
                target,
                stream_options,
                attach: false,
            },
        )
        .await
    }
}

async fn pod_exec_remote_websocket_stream<S>(
    state: Arc<S>,
    request: RemotePodExecStreamRequest,
) -> Result<Response, AppError>
where
    S: StreamingState + 'static,
{
    let RemotePodExecStreamRequest {
        req,
        node_exec,
        node_name,
        target,
        stream_options,
        attach,
    } = request;
    let handler_target = target.clone();
    let ExecTarget {
        namespace,
        pod_name,
        container_id,
        command,
    } = target;
    let node_target = NodeExecTarget::try_new(node_name, namespace, pod_name, container_id)
        .map_err(|error| AppError::Internal(error.to_string()))?;
    let options = NodeExecStreamOptions::new(
        stream_options.stdin,
        stream_options.stdout,
        stream_options.stderr,
        stream_options.tty,
    );
    let node_request = if attach {
        NodeExecRequest::attach(node_target, options)
    } else {
        NodeExecRequest::exec(node_target, command, options)
    };

    let ws_key = req
        .headers()
        .get(header::SEC_WEBSOCKET_KEY)
        .ok_or_else(|| AppError::BadRequest("Missing Sec-WebSocket-Key header".to_string()))?
        .clone();

    let subprotocol = negotiate_websocket_subprotocol(req.headers()).ok_or_else(|| {
        AppError::BadRequest("Missing or unsupported Sec-WebSocket-Protocol".to_string())
    })?;
    let selected_subprotocol = subprotocol.clone();
    let task_supervisor = state.streaming_dependencies().task_supervisor.clone();

    let on_upgrade = hyper::upgrade::on(req);
    if let Err(err) = state
        .streaming_dependencies()
        .task_supervisor
        .spawn_async(
            klights_supervisor::TaskCategory::Others,
            format!(
                "pod_{}_remote_websocket_stream_upgrade",
                if attach { "attach" } else { "exec" }
            ),
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

                        match node_exec.open_exec(node_request).await {
                            Ok(session) => {
                                handle_remote_exec_websocket_tungstenite(
                                    ws_stream,
                                    RemoteExecWebSocketRequest {
                                        session,
                                        task_supervisor,
                                        target: handler_target,
                                        subprotocol: selected_subprotocol,
                                        stream_options,
                                        attach,
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

async fn pod_exec_remote_websocket_sync<S>(
    state: Arc<S>,
    request: RemotePodExecSyncRequest,
) -> Result<Response, AppError>
where
    S: StreamingState + 'static,
{
    let RemotePodExecSyncRequest {
        req,
        node_exec,
        node_name,
        target,
    } = request;

    let ws_key = req
        .headers()
        .get(header::SEC_WEBSOCKET_KEY)
        .ok_or_else(|| AppError::BadRequest("Missing Sec-WebSocket-Key header".to_string()))?
        .clone();

    let subprotocol = negotiate_websocket_subprotocol(req.headers()).ok_or_else(|| {
        AppError::BadRequest("Missing or unsupported Sec-WebSocket-Protocol".to_string())
    })?;
    let selected_subprotocol = subprotocol.clone();
    let task_supervisor = state.streaming_dependencies().task_supervisor.clone();

    let on_upgrade = hyper::upgrade::on(req);
    if let Err(err) = state
        .streaming_dependencies()
        .task_supervisor
        .spawn_async(
            klights_supervisor::TaskCategory::Others,
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
                                node_exec,
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
pub async fn pod_attach<S>(
    State(state): State<Arc<S>>,
    Path((namespace, name)): Path<(String, String)>,
    RawQuery(query): RawQuery,
    req: Request,
) -> Result<Response, AppError>
where
    S: StreamingState + 'static,
{
    let query_str = query.unwrap_or_default();
    let (container, stdin, stdout, stderr, tty) = parse_attach_query(&query_str);

    let pod = get_pod(
        state.streaming_dependencies().pod_query.as_ref(),
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
        state.streaming_admission(),
        build_admission_context(AdmissionContextRequest {
            api_version: "v1",
            kind: "Pod",
            operation: "CONNECT",
            namespace: Some(namespace.clone()),
            name: Some(name.clone()),
            object: attach_options,
            old_object: None,
            dry_run: false,
            subresource: Some("attach"),
            options: None,
        }),
    )
    .await?;

    let container_id = extract_container_id(&pod.data, container.as_deref())?;
    let stream_options = ExecStreamOptions {
        stdin,
        stdout,
        stderr,
        tty,
    };
    let remote_node = remote_pod_node_name(
        &pod.data,
        state.streaming_dependencies().local_node_name.as_ref(),
    );
    let target = ExecTarget {
        namespace,
        pod_name: name,
        container_id,
        command: Vec::new(),
    };

    let upgrade_header = req
        .headers()
        .get(header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if let Some(node_name) = remote_node {
        let node_exec = state
            .streaming_dependencies()
            .remote_node_exec
            .clone()
            .ok_or_else(|| {
                AppError::Internal(
                    "replication service not available for remote pod attach".to_string(),
                )
            })?;
        if !upgrade_header.eq_ignore_ascii_case("websocket") {
            return Err(AppError::BadRequest(format!(
                "Pod attach for pod on remote node '{}' requires WebSocket upgrade",
                node_name
            )));
        }
        return pod_exec_remote_websocket_stream(
            state,
            RemotePodExecStreamRequest {
                req,
                node_exec,
                node_name,
                target,
                stream_options,
                attach: true,
            },
        )
        .await;
    }

    if !upgrade_header.eq_ignore_ascii_case("websocket") {
        return Err(AppError::BadRequest(
            "Pod attach requires WebSocket upgrade".to_string(),
        ));
    }

    let node_exec = state
        .streaming_dependencies()
        .local_node_exec
        .as_ref()
        .cloned()
        .ok_or_else(|| {
            AppError::Internal("local node exec runtime is not available".to_string())
        })?;
    let node_name = state.streaming_dependencies().local_node_name.to_string();
    pod_exec_remote_websocket_stream(
        state,
        RemotePodExecStreamRequest {
            req,
            node_exec,
            node_name,
            target,
            stream_options,
            attach: true,
        },
    )
    .await
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
