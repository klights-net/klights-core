use super::*;
use klights_kubelet::node_api::logs::{
    LogQuery, PodLogFollowTermination, PodLogFollowTrace,
    build_log_output_bytes_at as read_log_output_bytes_at, build_pod_log_follow_event_cursor,
    follow_log_file_with_initial_query_at, follow_log_file_with_termination_watch_at,
    pod_identity_matches, pod_log_follow_terminal_reason,
};
#[cfg(test)]
use klights_kubelet::node_api::logs::{
    PodLogFollowWatchPort, PodLogFollowWatchSource, pod_log_follow_event_is_terminal,
};
use klights_node_api::{NodeLog, NodeLogOptions, NodeLogRequest, NodeLogTarget};
use std::sync::Arc;

struct RemotePodLogRequest<'a> {
    node_log: Arc<dyn NodeLog>,
    namespace: &'a str,
    name: &'a str,
    pod_uid: &'a str,
    container_name: &'a str,
    params: LogQuery,
    node_name: &'a str,
}

struct RemotePodLogWebSocketRequest {
    node_log: Arc<dyn NodeLog>,
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    namespace: String,
    name: String,
    pod_uid: String,
    container_name: String,
    params: LogQuery,
    node_name: String,
    req: Request,
}

/// All container names declared in a Pod spec (regular, init, ephemeral).
fn pod_container_names(pod_data: &Value) -> Vec<String> {
    let mut names = Vec::new();
    for field in ["containers", "initContainers", "ephemeralContainers"] {
        if let Some(arr) = pod_data
            .pointer(&format!("/spec/{field}"))
            .and_then(|v| v.as_array())
        {
            for c in arr {
                if let Some(n) = c.get("name").and_then(|n| n.as_str()) {
                    names.push(n.to_string());
                }
            }
        }
    }
    names
}

/// Validate a client-supplied `?container=` against the Pod's declared
/// containers. The name becomes a filesystem path segment when locating the log
/// file, so an unvalidated value (e.g. `../../etc`) would escape the pod's log
/// directory. Upstream Kubernetes returns 400 for a non-existent container.
fn validate_requested_container(
    pod_data: &Value,
    requested: &str,
    namespace: &str,
    name: &str,
) -> Result<(), AppError> {
    if pod_container_names(pod_data).iter().any(|n| n == requested) {
        Ok(())
    } else {
        Err(AppError::BadRequest(format!(
            "container {requested} is not valid for pod {namespace}/{name}"
        )))
    }
}

// GET /api/v1/namespaces/{ns}/pods/{name}/log
pub(in crate::api) async fn get_pod_log(
    State(state): State<Arc<ApiState>>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<LogQuery>,
    req: Request,
) -> Result<Response, AppError> {
    let operation_now = state.operational().clock.now();
    // Get pod from PodRepository
    let pod = crate::api::pod_repository_ports::get_pod(
        state.resource_mutation().pod_repository.as_ref(),
        &namespace,
        &name,
    )
    .await?;

    let Some(pod) = pod else {
        return Err(AppError::NotFound(format!(
            "Pod {}/{} not found",
            namespace, name
        )));
    };

    let pod_data = pod.data;

    // Determine container name
    let container_name = if let Some(ref c) = params.container {
        validate_requested_container(&pod_data, c, &namespace, &name)?;
        c.clone()
    } else {
        // Get first container from spec
        pod_data
            .get("spec")
            .and_then(|s| s.get("containers"))
            .and_then(|cs| cs.as_array())
            .and_then(|arr| arr.first())
            .and_then(|c| c.get("name"))
            .and_then(|n| n.as_str())
            .ok_or_else(|| AppError::BadRequest("No containers in pod spec".to_string()))?
            .to_string()
    };

    // Check if pod is on a remote node — proxy log request via gRPC
    let remote_node = crate::api::pod_subresources::exec::remote_pod_node_name(
        &pod_data,
        &state.operational().config.node_name,
    );
    if let Some(node_name) = remote_node {
        let pod_uid = pod_data
            .get("metadata")
            .and_then(|m| m.get("uid"))
            .and_then(|u| u.as_str())
            .ok_or_else(|| AppError::Internal("Pod has no UID".to_string()))?;
        let node_log: Arc<dyn NodeLog> = state
            .operational()
            .replication
            .as_ref()
            .map(|services| services.logs.clone())
            .ok_or_else(|| {
                AppError::Internal(
                    "replication service not available for remote pod log".to_string(),
                )
            })?;
        if is_pod_log_websocket_upgrade(req.headers()) {
            return get_remote_pod_log_websocket(RemotePodLogWebSocketRequest {
                node_log,
                task_supervisor: state.operational().task_supervisor.clone(),
                namespace,
                name,
                pod_uid: pod_uid.to_string(),
                container_name,
                params,
                node_name,
                req,
            })
            .await;
        }
        return get_remote_pod_log(RemotePodLogRequest {
            node_log,
            namespace: &namespace,
            name: &name,
            pod_uid,
            container_name: &container_name,
            params,
            node_name: &node_name,
        })
        .await;
    }

    // Build log file path
    let pod_uid = pod_data
        .get("metadata")
        .and_then(|m| m.get("uid"))
        .and_then(|u| u.as_str())
        .ok_or_else(|| AppError::Internal("Pod has no UID".to_string()))?;

    let log_path = state
        .operational()
        .config
        .runtime
        .paths
        .pod_log_dir(&namespace, &name, pod_uid)
        .join(&container_name)
        .join("0.log")
        .to_string_lossy()
        .into_owned();

    tracing::debug!("Reading container logs from: {}", log_path);

    // Check if follow is requested
    let follow = params.follow.as_deref() == Some("true");
    let previous = params.previous.as_deref() == Some("true");

    let upgrade_header = req
        .headers()
        .get(header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if upgrade_header.eq_ignore_ascii_case("websocket") {
        let ws_key = req
            .headers()
            .get(header::SEC_WEBSOCKET_KEY)
            .ok_or_else(|| AppError::BadRequest("Missing Sec-WebSocket-Key header".to_string()))?
            .clone();

        let subprotocol = negotiate_pod_log_websocket_subprotocol(req.headers());

        let on_upgrade = hyper::upgrade::on(req);
        let task_supervisor = state.operational().task_supervisor.clone();
        if let Err(err) = state
            .operational()
            .task_supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Others,
                "pod_log_ws_upgrade",
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
                            handle_pod_log_websocket_tungstenite(
                                ws_stream,
                                task_supervisor,
                                log_path,
                                params,
                                operation_now,
                            )
                            .await;
                        }
                        Err(e) => {
                            tracing::error!("WebSocket upgrade failed for pod log: {}", e);
                        }
                    }
                },
            )
            .await
        {
            tracing::warn!("Failed to spawn pod log WebSocket upgrade task: {}", err);
        }

        return build_pod_log_websocket_response(&ws_key, subprotocol);
    }

    // previous=true requests logs from previous container instance
    // Phase 1: return empty (we don't track previous container logs yet)
    if previous {
        return build_text_log_response(axum::body::Body::from(""));
    }

    if follow {
        // Streaming follow mode
        let termination = build_pod_log_follow_termination(
            state.as_ref(),
            &namespace,
            &name,
            pod_uid,
            &container_name,
        )
        .await?;
        let stream = follow_log_file_with_termination_watch_at(
            log_path,
            params,
            state.operational().task_supervisor.clone(),
            termination,
            operation_now,
        );
        build_text_log_response(axum::body::Body::from_stream(stream))
    } else {
        let output = build_log_output_bytes_at(
            &log_path,
            &params,
            state.operational().task_supervisor.as_ref(),
            operation_now,
        )
        .await?;

        build_text_log_response(axum::body::Body::from(output))
    }
}

async fn build_pod_log_follow_termination(
    state: &ApiState,
    namespace: &str,
    name: &str,
    pod_uid: &str,
    container_name: &str,
) -> Result<PodLogFollowTermination, AppError> {
    let pod_events =
        build_pod_log_follow_event_cursor(&state.pod_node_subresources().pod_log_follow_watch)
            .await
            .map_err(|error| {
                AppError::Internal(format!("failed to open Pod log follow watch: {error}"))
            })?;
    let current = crate::api::pod_repository_ports::get_pod(
        state.resource_mutation().pod_repository.as_ref(),
        namespace,
        name,
    )
    .await?;
    let (terminate_after_initial, current_found, identity_matches, current_phase, terminal_reason) =
        match current {
            Some(resource) => {
                let identity_matches =
                    pod_identity_matches(&resource.data, namespace, name, pod_uid);
                let current_phase = resource
                    .data
                    .pointer("/status/phase")
                    .and_then(|value| value.as_str())
                    .map(str::to_string);
                let terminal_reason = if identity_matches {
                    pod_log_follow_terminal_reason(&resource.data, container_name)
                } else {
                    Some("pod uid/name mismatch".to_string())
                };
                (
                    terminal_reason.is_some(),
                    true,
                    identity_matches,
                    current_phase,
                    terminal_reason,
                )
            }
            None => (
                true,
                false,
                false,
                None,
                Some("pod not found at follow start".to_string()),
            ),
        };

    tracing::info!(
        target: "klights::pod_logs",
        namespace,
        pod = name,
        uid = pod_uid,
        container = container_name,
        current_found,
        identity_matches,
        current_phase = ?current_phase,
        terminate_after_initial,
        terminal_reason = ?terminal_reason,
        "pod log follow termination watcher initialized"
    );

    Ok(PodLogFollowTermination::new(
        pod_events,
        namespace.to_string(),
        name.to_string(),
        pod_uid.to_string(),
        container_name.to_string(),
        terminate_after_initial,
    ))
}

async fn build_log_output_bytes_at(
    log_path: &str,
    params: &LogQuery,
    task_supervisor: &klights_supervisor::TaskSupervisor,
    operation_now: time::OffsetDateTime,
) -> Result<Bytes, AppError> {
    read_log_output_bytes_at(log_path, params, task_supervisor, operation_now)
        .await
        .map_err(|error| AppError::Internal(error.client_message().to_string()))
}

fn build_text_log_response(body: axum::body::Body) -> Result<Response, AppError> {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(body)
        .map(IntoResponse::into_response)
        .map_err(|e| AppError::Internal(format!("Failed to build pod log response: {}", e)))
}

fn is_pod_log_websocket_upgrade(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get(header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|upgrade| upgrade.eq_ignore_ascii_case("websocket"))
}

fn negotiate_pod_log_websocket_subprotocol(headers: &axum::http::HeaderMap) -> String {
    for value in headers.get_all(header::SEC_WEBSOCKET_PROTOCOL) {
        if let Ok(protocols) = value.to_str()
            && let Some(protocol) = protocols
                .split(',')
                .map(str::trim)
                .find(|p| *p == "binary.k8s.io" || *p == "base64.binary.k8s.io")
        {
            return protocol.to_string();
        }
    }
    "binary.k8s.io".to_string()
}

fn build_pod_log_websocket_response(
    ws_key: &axum::http::HeaderValue,
    subprotocol: String,
) -> Result<Response, AppError> {
    Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header(header::UPGRADE, "websocket")
        .header(header::CONNECTION, "Upgrade")
        .header(
            header::SEC_WEBSOCKET_ACCEPT,
            derive_websocket_accept_key(ws_key),
        )
        .header(header::SEC_WEBSOCKET_PROTOCOL, subprotocol)
        .body(axum::body::Body::empty())
        .map_err(|e| AppError::Internal(format!("Failed to build WebSocket response: {}", e)))
}

pub async fn handle_pod_log_websocket_tungstenite<S>(
    mut socket: tokio_tungstenite::WebSocketStream<S>,
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    log_path: String,
    params: LogQuery,
    operation_now: time::OffsetDateTime,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use tokio_tungstenite::tungstenite::Message as TungsteniteMessage;
    use tokio_tungstenite::tungstenite::protocol::CloseFrame;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

    if params.previous.as_deref() == Some("true") {
        let _ = socket
            .send(TungsteniteMessage::Close(Some(CloseFrame {
                code: CloseCode::Normal,
                reason: "".into(),
            })))
            .await;
        return;
    }

    if params.follow.as_deref() == Some("true") {
        let stream = follow_log_file_with_initial_query_at(
            log_path,
            params,
            task_supervisor.clone(),
            operation_now,
        );
        futures::pin_mut!(stream);
        while let Some(item) = stream.next().await {
            match item {
                Ok(chunk) => {
                    if socket
                        .send(TungsteniteMessage::Binary(chunk))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Err(e) => {
                    tracing::warn!("pod log websocket stream error: {}", e);
                    break;
                }
            }
        }
    } else {
        match build_log_output_bytes_at(&log_path, &params, task_supervisor.as_ref(), operation_now)
            .await
        {
            Ok(output) => {
                if !output.is_empty()
                    && socket
                        .send(TungsteniteMessage::Binary(output))
                        .await
                        .is_err()
                {
                    return;
                }
            }
            Err(e) => {
                tracing::warn!("Failed to read websocket pod log {}: {:?}", log_path, e);
            }
        }
    }

    let _ = socket
        .send(TungsteniteMessage::Close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "".into(),
        })))
        .await;
}

async fn get_remote_pod_log(request: RemotePodLogRequest<'_>) -> Result<Response, AppError> {
    let RemotePodLogRequest {
        node_log,
        namespace,
        name,
        pod_uid,
        container_name,
        params,
        node_name,
    } = request;
    let follow = params.follow.as_deref() == Some("true");
    let request =
        pod_log_request_from_parts(node_name, namespace, name, pod_uid, container_name, params)?;

    if follow {
        let mut session = node_log
            .open_logs(request)
            .await
            .map_err(|e| AppError::BadGateway(format!("remote pod log request failed: {e}")))?;
        let trace_namespace = namespace.to_string();
        let trace_name = name.to_string();
        let trace_container = container_name.to_string();
        let trace_node = node_name.to_string();
        let stream = async_stream::stream! {
            let _follow_trace = PodLogFollowTrace::new(
                "remote",
                format!("{trace_namespace}/{trace_name}:{trace_container}@{trace_node}"),
            );
            loop {
                match session.recv_frame().await {
                    Ok(Some(response)) => {
                        let terminal = response.is_terminal();
                        let (content, terminal_error, _) = response.into_parts();
                        if let Some(error) = terminal_error {
                            tracing::warn!(
                                target: "klights::pod_logs",
                                namespace = %trace_namespace,
                                pod = %trace_name,
                                container = %trace_container,
                                node = %trace_node,
                                error = %error,
                                "remote pod log follow returned error"
                            );
                            let _ = session.cancel().await;
                            yield Err(std::io::Error::other(format!("remote pod log error: {error}")));
                            break;
                        }
                        if !content.is_empty() {
                            yield Ok(content);
                        }
                        if terminal {
                            tracing::info!(
                                target: "klights::pod_logs",
                                namespace = %trace_namespace,
                                pod = %trace_name,
                                container = %trace_container,
                                node = %trace_node,
                                "remote pod log follow received terminal frame"
                            );
                            let _ = session.cancel().await;
                            break;
                        }
                    }
                    Ok(None) => {
                        tracing::info!(
                            target: "klights::pod_logs",
                            namespace = %trace_namespace,
                            pod = %trace_name,
                            container = %trace_container,
                            node = %trace_node,
                            "remote pod log follow session closed without terminal frame"
                        );
                        let _ = session.cancel().await;
                        break;
                    }
                    Err(err) => {
                        tracing::warn!(
                            target: "klights::pod_logs",
                            namespace = %trace_namespace,
                            pod = %trace_name,
                            container = %trace_container,
                            node = %trace_node,
                            error = %err,
                            "remote pod log follow session receive failed"
                        );
                        let _ = session.cancel().await;
                        yield Err(std::io::Error::other(format!("remote pod log stream failed: {err}")));
                        break;
                    }
                }
            }
        };

        return build_text_log_response(axum::body::Body::from_stream(stream));
    }

    let response = node_log
        .read_logs(request)
        .await
        .map_err(|e| AppError::BadGateway(format!("remote pod log request failed: {e}")))?;

    let (content, terminal_error) = response.into_parts();
    if let Some(error) = terminal_error {
        return Err(AppError::Internal(format!("remote pod log error: {error}")));
    }

    build_text_log_response(axum::body::Body::from(content))
}

fn pod_log_request_from_parts(
    node_name: &str,
    namespace: &str,
    pod_name: &str,
    pod_uid: &str,
    container_name: &str,
    params: LogQuery,
) -> Result<NodeLogRequest, AppError> {
    let target = NodeLogTarget::try_new(node_name, namespace, pod_name, pod_uid, container_name)
        .map_err(|err| AppError::BadRequest(format!("invalid pod log target: {err}")))?;

    Ok(NodeLogRequest::new(
        target,
        NodeLogOptions::new(
            params.follow,
            params.tail_lines,
            params.timestamps,
            params.since_time,
            params.since_seconds,
            params.limit_bytes,
            params.previous,
        ),
    ))
}

async fn get_remote_pod_log_websocket(
    request: RemotePodLogWebSocketRequest,
) -> Result<Response, AppError> {
    let RemotePodLogWebSocketRequest {
        node_log,
        task_supervisor,
        namespace,
        name,
        pod_uid,
        container_name,
        params,
        node_name,
        req,
    } = request;
    let ws_key = req
        .headers()
        .get(header::SEC_WEBSOCKET_KEY)
        .ok_or_else(|| AppError::BadRequest("Missing Sec-WebSocket-Key header".to_string()))?
        .clone();
    let subprotocol = negotiate_pod_log_websocket_subprotocol(req.headers());
    let on_upgrade = hyper::upgrade::on(req);
    let request = pod_log_request_from_parts(
        &node_name,
        &namespace,
        &name,
        &pod_uid,
        &container_name,
        params,
    )?;

    if let Err(err) = task_supervisor
        .spawn_async(
            klights_supervisor::TaskCategory::Others,
            "pod_log_remote_ws_upgrade",
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
                        handle_remote_pod_log_websocket_tungstenite(
                            ws_stream,
                            Some(node_log),
                            request,
                        )
                        .await;
                    }
                    Err(e) => {
                        tracing::error!("Remote WebSocket pod log upgrade failed: {}", e);
                    }
                }
            },
        )
        .await
    {
        tracing::warn!("Failed to spawn remote pod log WebSocket task: {}", err);
    }

    build_pod_log_websocket_response(&ws_key, subprotocol)
}

pub async fn handle_remote_pod_log_websocket_tungstenite<S>(
    mut socket: tokio_tungstenite::WebSocketStream<S>,
    node_log: Option<Arc<dyn NodeLog>>,
    request: NodeLogRequest,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use tokio_tungstenite::tungstenite::Message as TungsteniteMessage;
    use tokio_tungstenite::tungstenite::protocol::CloseFrame;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

    let output = match node_log {
        Some(node_log) => match node_log.read_logs(request).await {
            Ok(response) => {
                let (content, terminal_error) = response.into_parts();
                if let Some(error) = terminal_error {
                    Err(format!("remote pod log error: {error}"))
                } else {
                    Ok(content)
                }
            }
            Err(err) => Err(format!("remote pod log request failed: {err}")),
        },
        None => Err("replication service not available for remote pod log".to_string()),
    };

    match output {
        Ok(log_content) => {
            if !log_content.is_empty()
                && socket
                    .send(TungsteniteMessage::Binary(log_content.into()))
                    .await
                    .is_err()
            {
                return;
            }
        }
        Err(error) => {
            tracing::warn!("{}", error);
            let mut body = error.into_bytes();
            body.push(b'\n');
            let _ = socket.send(TungsteniteMessage::Binary(body.into())).await;
        }
    }

    let _ = socket
        .send(TungsteniteMessage::Close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "".into(),
        })))
        .await;
}

#[cfg(test)]
mod container_validation_tests {
    use super::{pod_container_names, validate_requested_container};
    use serde_json::json;

    fn pod() -> serde_json::Value {
        json!({
            "spec": {
                "containers": [{"name": "app"}, {"name": "sidecar"}],
                "initContainers": [{"name": "init"}],
                "ephemeralContainers": [{"name": "debug"}]
            }
        })
    }

    #[test]
    fn collects_all_container_kinds() {
        let names = pod_container_names(&pod());
        assert_eq!(names, vec!["app", "sidecar", "init", "debug"]);
    }

    #[test]
    fn valid_container_accepted() {
        assert!(validate_requested_container(&pod(), "sidecar", "ns", "p").is_ok());
        assert!(validate_requested_container(&pod(), "init", "ns", "p").is_ok());
        assert!(validate_requested_container(&pod(), "debug", "ns", "p").is_ok());
    }

    #[test]
    fn path_traversal_container_rejected() {
        let res = validate_requested_container(&pod(), "../../../../etc", "ns", "p");
        assert!(res.is_err(), "traversal container name must be rejected");
    }

    #[test]
    fn unknown_container_rejected() {
        assert!(validate_requested_container(&pod(), "nope", "ns", "p").is_err());
    }
}

#[cfg(test)]
mod watch_port_tests {
    use super::*;
    use std::sync::Mutex;

    struct RecordingWatchPort {
        requests: Arc<Mutex<Vec<klights_leader_api::WatchRequest>>>,
    }

    impl PodLogFollowWatchPort for RecordingWatchPort {
        fn open_pod_watch(&self) -> klights_leader_api::LeaderWatchFuture<'_> {
            Box::pin(async move {
                let request = klights_leader_api::WatchRequest::try_new(
                    "v1", "Pod", None, None, None, None, None,
                )?;
                self.requests.lock().unwrap().push(request);
                Ok(klights_leader_api::WatchStream::positioned(
                    Box::pin(futures::stream::empty()),
                    klights_leader_api::WatchResumeCursor::try_new(Some(41), None)?,
                ))
            })
        }
    }

    #[tokio::test]
    async fn follow_cursor_uses_positioned_leader_watch_handoff() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let source = PodLogFollowWatchSource::new(Arc::new(RecordingWatchPort {
            requests: requests.clone(),
        }));

        let cursor = build_pod_log_follow_event_cursor(&source).await.unwrap();
        assert_eq!(
            cursor
                .accepted_cursor()
                .and_then(|cursor| cursor.resource_version()),
            Some(41)
        );
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].api_version(), "v1");
        assert_eq!(requests[0].kind(), "Pod");
        assert_eq!(requests[0].start_resource_version(), None);
        assert_eq!(requests[0].start_watch_replay_position(), None);
    }

    #[test]
    fn replacement_pod_uid_does_not_terminate_old_log_follow() {
        let (_tx, receiver) = tokio::sync::broadcast::channel(1);
        let termination = PodLogFollowTermination::new_for_test(
            receiver,
            "default".to_string(),
            "same-name".to_string(),
            "old-uid".to_string(),
            "main".to_string(),
            false,
        );
        let replacement = klights_leader_api::ResourceEvent::try_new(
            klights_leader_api::WatchEventType::Deleted,
            klights_cluster_core::Resource::try_from_data(Arc::new(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "same-name",
                "uid": "replacement-uid"
            }
            })))
            .unwrap(),
            None,
        )
        .unwrap();

        assert!(!pod_log_follow_event_is_terminal(
            &termination,
            &replacement
        ));
    }
}

// POST /api/v1/namespaces/{ns}/pods/{name}/exec
// Handles both WebSocket (kubectl v1.29+) and SPDY (older kubectl) upgrades
