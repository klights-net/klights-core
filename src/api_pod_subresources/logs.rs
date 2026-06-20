use super::*;
use crate::replication::protocol::PodLogRequest;
use crate::watch::{
    EventType, SignalWatchCursor, WatchCursorError, WatchDeliveryScope, WatchEvent, WatchTopic,
    WindowPolicy,
};
use std::collections::VecDeque;
use std::io::{self, BufRead, Read, Seek, SeekFrom};
use std::{fs as blocking_fs, path::PathBuf};
#[cfg(test)]
use tokio::sync::broadcast;

const LOG_FORWARD_CHUNK_SIZE: usize = 64 * 1024;

#[derive(Clone, Debug, Deserialize)]
pub struct LogQuery {
    pub container: Option<String>,
    pub follow: Option<String>,
    #[serde(rename = "tailLines")]
    pub tail_lines: Option<usize>,
    pub timestamps: Option<String>,
    #[serde(rename = "sinceSeconds")]
    pub since_seconds: Option<i64>,
    /// RFC3339 timestamp — include only lines at or after this time.
    #[serde(rename = "sinceTime")]
    pub since_time: Option<String>,
    /// Maximum bytes of log output to return.
    #[serde(rename = "limitBytes")]
    pub limit_bytes: Option<usize>,
    pub previous: Option<String>,
    /// Ignored — klights serves all containers' logs via individual container files.
    #[serde(rename = "insecureSkipTLSVerifyBackend", default)]
    pub insecure_skip_tls_verify_backend: bool,
}

struct RemotePodLogRequest<'a> {
    state: Arc<AppState>,
    namespace: &'a str,
    name: &'a str,
    pod_uid: &'a str,
    container_name: &'a str,
    params: LogQuery,
    node_name: &'a str,
}

struct RemotePodLogWebSocketRequest {
    state: Arc<AppState>,
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
pub async fn get_pod_log(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<LogQuery>,
    req: Request,
) -> Result<Response, AppError> {
    // Get pod from PodRepository
    let pod = crate::kubelet::pod_repository::PodReader::get_pod(
        state.pod_repository.as_ref(),
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
    let remote_node =
        crate::api_pod_subresources::exec::remote_pod_node_name(&pod_data, &state.config.node_name);
    if let Some(node_name) = remote_node {
        let pod_uid = pod_data
            .get("metadata")
            .and_then(|m| m.get("uid"))
            .and_then(|u| u.as_str())
            .ok_or_else(|| AppError::Internal("Pod has no UID".to_string()))?;
        if is_pod_log_websocket_upgrade(req.headers()) {
            return get_remote_pod_log_websocket(RemotePodLogWebSocketRequest {
                state,
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
            state,
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

    let log_path = crate::paths::pod_log_dir_path(
        &state.config.containerd_namespace,
        &namespace,
        &name,
        pod_uid,
    )
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
        let task_supervisor = state.task_supervisor.clone();
        if let Err(err) = state
            .task_supervisor
            .spawn_async(
                crate::task_supervisor::TaskCategory::Others,
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
        let stream = follow_log_file_with_termination_watch(
            log_path,
            params,
            state.task_supervisor.clone(),
            termination,
        );
        build_text_log_response(axum::body::Body::from_stream(stream))
    } else {
        let output =
            build_log_output_bytes(&log_path, &params, state.task_supervisor.as_ref()).await?;

        build_text_log_response(axum::body::Body::from(output))
    }
}

async fn build_pod_log_follow_termination(
    state: &AppState,
    namespace: &str,
    name: &str,
    pod_uid: &str,
    container_name: &str,
) -> Result<PodLogFollowTermination, AppError> {
    let pod_events = build_pod_log_follow_event_cursor(state.db.clone()).await;
    let current = crate::kubelet::pod_repository::PodReader::get_pod(
        state.pod_repository.as_ref(),
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

pub async fn build_pod_log_follow_event_cursor(
    db: crate::datastore::DatastoreHandle,
) -> SignalWatchCursor<crate::datastore::sqlite::DatastoreWatchReplaySource> {
    let topic = WatchTopic::new("v1", "Pod");
    let signal_rx = db.subscribe_watch_signals(topic.clone());
    let start_rv = db.get_current_resource_version().await.unwrap_or(0);
    SignalWatchCursor::new(
        signal_rx,
        crate::datastore::sqlite::DatastoreWatchReplaySource::new(
            db,
            vec![crate::datastore::WatchTarget::namespaced("v1", "Pod")],
        ),
        topic,
        WatchDeliveryScope::NamespacedAll,
        start_rv,
        WindowPolicy::default_watch_delivery(),
    )
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
    task_supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
    log_path: String,
    params: LogQuery,
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
        let stream = follow_log_file_with_initial_query(log_path, params, task_supervisor.clone());
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
        match build_log_output_bytes(&log_path, &params, task_supervisor.as_ref()).await {
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

pub async fn build_log_output(
    log_path: &str,
    params: &LogQuery,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
) -> Result<String, AppError> {
    let bytes = build_log_output_bytes(log_path, params, task_supervisor).await?;
    Ok(String::from_utf8_lossy(bytes.as_ref()).into_owned())
}

pub async fn build_log_output_bytes(
    log_path: &str,
    params: &LogQuery,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
) -> Result<Bytes, AppError> {
    const ATTEMPTS: usize = 50;
    const RETRY_DELAY_MS: u64 = 100;

    for attempt in 0..ATTEMPTS {
        match build_log_output_once_bytes(log_path, params, task_supervisor).await {
            Ok(output) if !output.is_empty() || attempt + 1 == ATTEMPTS => return Ok(output),
            Ok(_) => {
                let _ = task_supervisor
                    .sleep(
                        "pod_logs_read_retry_delay",
                        std::time::Duration::from_millis(RETRY_DELAY_MS),
                    )
                    .await;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if attempt + 1 == ATTEMPTS {
                    return Ok(Bytes::new());
                }
                let _ = task_supervisor
                    .sleep(
                        "pod_logs_read_retry_delay",
                        std::time::Duration::from_millis(RETRY_DELAY_MS),
                    )
                    .await;
            }
            Err(e) => {
                // Log the on-disk path + raw IO error server-side only. The
                // client-facing 500 must not leak the internal log path
                // (containerd namespace, pod UID, data root) back to the caller.
                tracing::warn!("Failed to read log file {}: {}", log_path, e);
                return Err(AppError::Internal(
                    "failed to read container logs".to_string(),
                ));
            }
        }
    }

    Ok(Bytes::new())
}

async fn build_log_output_once_bytes(
    log_path: &str,
    params: &LogQuery,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
) -> io::Result<Bytes> {
    let path = PathBuf::from(log_path);
    let key = log_path.to_string();
    let params = params.clone();
    let since_cutoff = log_query_since_cutoff(&params);

    task_supervisor
        .run_blocking_file_keyed("pod_logs_build_output", key, move || {
            build_log_output_blocking(path, &params, since_cutoff)
        })
        .await
        .map_err(anyhow_to_io_error)?
}

fn build_log_output_blocking(
    path: PathBuf,
    params: &LogQuery,
    since_cutoff: Option<chrono::DateTime<chrono::Utc>>,
) -> io::Result<Bytes> {
    let file = blocking_fs::File::open(path)?;
    let mut reader = io::BufReader::new(file);
    let show_timestamps = params.timestamps.as_deref() == Some("true");
    let mut line = Vec::new();

    if let Some(tail_lines) = params.tail_lines {
        return build_tail_log_output_blocking_bytes(
            &mut reader,
            &mut line,
            show_timestamps,
            since_cutoff.as_ref(),
            tail_lines,
            params.limit_bytes,
        );
    }

    let mut output = Vec::new();
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        append_filtered_log_line_bytes(&mut output, &line, show_timestamps, since_cutoff.as_ref());
        if let Some(limit) = params.limit_bytes
            && output.len() >= limit
        {
            output.truncate(limit);
            break;
        }
    }

    Ok(Bytes::from(output))
}

fn build_tail_log_output_blocking_bytes(
    reader: &mut io::BufReader<blocking_fs::File>,
    line: &mut Vec<u8>,
    show_timestamps: bool,
    since_cutoff: Option<&chrono::DateTime<chrono::Utc>>,
    tail_lines: usize,
    limit_bytes: Option<usize>,
) -> io::Result<Bytes> {
    if tail_lines == 0 {
        return Ok(Bytes::new());
    }

    let mut tail = VecDeque::with_capacity(tail_lines);
    loop {
        line.clear();
        if reader.read_until(b'\n', line)? == 0 {
            break;
        }
        let raw_line = trim_raw_log_line_end(line);
        if !is_log_line_after_cutoff_bytes(raw_line, since_cutoff) {
            continue;
        }
        if tail.len() == tail_lines {
            tail.pop_front();
        }
        let mut parsed = Vec::new();
        append_raw_cri_log_line_for_client(&mut parsed, line, show_timestamps);
        tail.push_back(parsed);
    }

    let mut output = Vec::new();
    for line in tail {
        output.extend_from_slice(&line);
    }
    if let Some(limit) = limit_bytes {
        output.truncate(output.len().min(limit));
    }
    Ok(Bytes::from(output))
}

fn append_filtered_log_line_bytes(
    output: &mut Vec<u8>,
    raw_line_with_ending: &[u8],
    show_timestamps: bool,
    since_cutoff: Option<&chrono::DateTime<chrono::Utc>>,
) {
    let raw_line = trim_raw_log_line_end(raw_line_with_ending);
    if !is_log_line_after_cutoff_bytes(raw_line, since_cutoff) {
        return;
    }
    append_raw_cri_log_line_for_client(output, raw_line_with_ending, show_timestamps);
}

fn log_query_since_cutoff(params: &LogQuery) -> Option<chrono::DateTime<chrono::Utc>> {
    if let Some(ref ts) = params.since_time {
        chrono::DateTime::parse_from_rfc3339(ts)
            .ok()
            .map(|dt| dt.with_timezone(&chrono::Utc))
    } else {
        params
            .since_seconds
            .map(|secs| chrono::Utc::now() - chrono::Duration::seconds(secs))
    }
}

// Parse CRI log format: "2024-01-01T00:00:00.000000000Z stdout F message"
// Returns just the message unless show_timestamps is true
pub fn parse_cri_log_line(line: &str, show_timestamps: bool) -> String {
    let parts: Vec<&str> = line.splitn(4, ' ').collect();
    if parts.len() < 4 {
        // Malformed line, return as-is
        return line.to_string();
    }

    let timestamp = parts[0];
    let _stream = parts[1]; // stdout or stderr
    let _tag = parts[2]; // F (full) or P (partial)
    let message = parts[3];

    if show_timestamps {
        format!("{} {}", timestamp, message)
    } else {
        message.to_string()
    }
}

/// Check if a CRI log line's timestamp is after the cutoff time.
/// Returns true (include line) if no cutoff or if timestamp is after cutoff.
/// Malformed lines are always included.
pub fn is_log_line_after_cutoff(
    line: &str,
    cutoff: Option<&chrono::DateTime<chrono::Utc>>,
) -> bool {
    let Some(cutoff) = cutoff else {
        return true;
    };
    // Extract timestamp (first space-delimited field)
    let timestamp_str = match line.split_once(' ') {
        Some((ts, _)) => ts,
        None => return true, // malformed, include
    };
    // Parse RFC3339 timestamp (CRI format: "2024-01-01T00:00:00.000000000Z")
    match chrono::DateTime::parse_from_rfc3339(timestamp_str) {
        Ok(ts) => ts >= *cutoff,
        Err(_) => true, // unparseable timestamp, include
    }
}

fn is_log_line_after_cutoff_bytes(
    line: &[u8],
    cutoff: Option<&chrono::DateTime<chrono::Utc>>,
) -> bool {
    let Some(cutoff) = cutoff else {
        return true;
    };
    let timestamp = line.split(|byte| *byte == b' ').next().unwrap_or_default();
    std::str::from_utf8(timestamp)
        .ok()
        .and_then(|timestamp| chrono::DateTime::parse_from_rfc3339(timestamp).ok())
        .map(|ts| ts >= *cutoff)
        .unwrap_or(true)
}

pub fn follow_log_file_with_initial_query(
    log_path: String,
    params: LogQuery,
    task_supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
) -> impl futures::Stream<Item = Result<Bytes, std::io::Error>> {
    follow_log_file_inner(log_path, params, task_supervisor, None)
}

pub struct PodLogFollowTermination {
    pod_events: PodLogEventSource,
    namespace: String,
    name: String,
    uid: String,
    container_name: String,
    terminate_after_initial: bool,
}

impl PodLogFollowTermination {
    pub fn new(
        pod_events: SignalWatchCursor<crate::datastore::sqlite::DatastoreWatchReplaySource>,
        namespace: String,
        name: String,
        uid: String,
        container_name: String,
        terminate_after_initial: bool,
    ) -> Self {
        Self {
            pod_events: PodLogEventSource::Signal(pod_events),
            namespace,
            name,
            uid,
            container_name,
            terminate_after_initial,
        }
    }

    #[cfg(test)]
    pub fn new_for_test(
        pod_events: broadcast::Receiver<WatchEvent>,
        namespace: String,
        name: String,
        uid: String,
        container_name: String,
        terminate_after_initial: bool,
    ) -> Self {
        Self {
            pod_events: PodLogEventSource::Broadcast(pod_events),
            namespace,
            name,
            uid,
            container_name,
            terminate_after_initial,
        }
    }

    async fn next_event(&mut self) -> Result<WatchEvent, PodLogEventError> {
        self.pod_events.next_event().await
    }
}

enum PodLogEventSource {
    Signal(SignalWatchCursor<crate::datastore::sqlite::DatastoreWatchReplaySource>),
    #[cfg(test)]
    Broadcast(broadcast::Receiver<WatchEvent>),
}

enum PodLogEventError {
    #[cfg(test)]
    Lagged(u64),
    Closed,
    Expired,
    Replay(anyhow::Error),
}

impl PodLogEventSource {
    async fn next_event(&mut self) -> Result<WatchEvent, PodLogEventError> {
        match self {
            Self::Signal(cursor) => cursor.next_event().await.map_err(|err| match err {
                WatchCursorError::Closed => PodLogEventError::Closed,
                WatchCursorError::Expired => PodLogEventError::Expired,
                WatchCursorError::Replay(err) => PodLogEventError::Replay(err),
            }),
            #[cfg(test)]
            Self::Broadcast(rx) => rx.recv().await.map_err(|err| match err {
                broadcast::error::RecvError::Lagged(skipped) => PodLogEventError::Lagged(skipped),
                broadcast::error::RecvError::Closed => PodLogEventError::Closed,
            }),
        }
    }
}

pub fn follow_log_file_with_termination_watch(
    log_path: String,
    params: LogQuery,
    task_supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
    termination: PodLogFollowTermination,
) -> impl futures::Stream<Item = Result<Bytes, std::io::Error>> {
    follow_log_file_inner(log_path, params, task_supervisor, Some(termination))
}

fn follow_log_file_inner(
    log_path: String,
    initial_params: LogQuery,
    task_supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
    mut termination: Option<PodLogFollowTermination>,
) -> impl futures::Stream<Item = Result<Bytes, std::io::Error>> {
    async_stream::stream! {
        use inotify::{Inotify, WatchMask};

        let _follow_trace = PodLogFollowTrace::new("local", log_path.clone());
        let show_timestamps = initial_params.timestamps.as_deref() == Some("true");
        let mut pending_raw_log_line = Vec::new();
        let mut terminate_after_drain = termination
            .as_ref()
            .is_some_and(|watch| watch.terminate_after_initial);
        if let Some(termination) = termination.as_ref()
            && terminate_after_drain {
                tracing::info!(
                    target: "klights::pod_logs",
                    namespace = %termination.namespace,
                    pod = %termination.name,
                    uid = %termination.uid,
                    container = %termination.container_name,
                    "pod log follow will close after initial log drain because pod is already terminal"
                );
            }

        let file = match open_log_file_for_follow(
            &log_path,
            task_supervisor.as_ref(),
            termination.as_mut(),
        )
        .await
        {
            Ok(Some(f)) => f,
            Ok(None) => return,
            Err(e) => {
                tracing::warn!("Failed to open log file {}: {}", log_path, e);
                return;
            }
        };
        let mut file = file;

        // Set up inotify watch for file modifications — zero polling
        let inotify = match Inotify::init() {
            Ok(i) => i,
            Err(e) => {
                tracing::warn!("Failed to init inotify for log follow: {}", e);
                return;
            }
        };

        if let Err(e) = inotify.watches().add(
            &log_path,
            WatchMask::MODIFY | WatchMask::CLOSE_WRITE | WatchMask::DELETE_SELF | WatchMask::MOVE_SELF,
        ) {
            tracing::warn!("Failed to add inotify watch for {}: {}", log_path, e);
            return;
        }

        let async_inotify = match tokio::io::unix::AsyncFd::new(inotify) {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!("Failed to create AsyncFd for inotify: {}", e);
                return;
            }
        };

        if initial_follow_requires_snapshot(&initial_params) {
            match read_initial_follow_snapshot(&log_path, &initial_params, task_supervisor.as_ref()).await {
                Ok((offset, initial)) => {
                    match seek_log_file_supervised(&log_path, file, offset, task_supervisor.as_ref()).await {
                        Ok(seeked) => file = seeked,
                        Err(e) => {
                            tracing::error!("Error seeking log file after initial snapshot: {}", e);
                            yield Err(e);
                            return;
                        }
                    }
                    if !initial.is_empty() {
                        let initial =
                            format_raw_cri_log_bytes_for_client(initial.as_ref(), show_timestamps);
                        yield Ok(initial);
                    }
                }
                Err(e) => {
                    tracing::error!("Error reading initial log file content: {}", e);
                    yield Err(e);
                    return;
                }
            }
        } else {
            loop {
                match read_log_chunk_supervised(&log_path, file, task_supervisor.as_ref()).await {
                    Ok((next_file, chunk)) => {
                        file = next_file;
                        if chunk.is_empty() {
                            if !pending_raw_log_line.is_empty() {
                                let formatted = flush_pending_raw_log_line_for_client(
                                    &mut pending_raw_log_line,
                                    show_timestamps,
                                );
                                if !formatted.is_empty() {
                                    yield Ok(formatted);
                                }
                            }
                            break;
                        }
                        let formatted = format_complete_raw_cri_log_chunk_for_client(
                            chunk.as_ref(),
                            &mut pending_raw_log_line,
                            show_timestamps,
                        );
                        if !formatted.is_empty() {
                            yield Ok(formatted);
                        }
                    }
                    Err(e) => {
                        tracing::error!("Error reading initial log file content: {}", e);
                        yield Err(e);
                        return;
                    }
                }
            }
        }

        if terminate_after_drain {
            if let Some(termination) = termination.as_ref() {
                tracing::info!(
                    target: "klights::pod_logs",
                    namespace = %termination.namespace,
                    pod = %termination.name,
                    uid = %termination.uid,
                    container = %termination.container_name,
                    "pod log follow initial drain complete; closing stream"
                );
            }
            return;
        }

        loop {
            match read_log_chunk_supervised(&log_path, file, task_supervisor.as_ref()).await {
                Ok((next_file, chunk)) => {
                    file = next_file;
                    if !chunk.is_empty() {
                        let formatted = format_complete_raw_cri_log_chunk_for_client(
                            chunk.as_ref(),
                            &mut pending_raw_log_line,
                            show_timestamps,
                        );
                        if !formatted.is_empty() {
                            yield Ok(formatted);
                        }
                        continue;
                    }
                }
                Err(e) => {
                    tracing::error!("Error reading log file: {}", e);
                    yield Err(e);
                    return;
                }
            }

            if terminate_after_drain {
                if !pending_raw_log_line.is_empty() {
                    let formatted = flush_pending_raw_log_line_for_client(
                        &mut pending_raw_log_line,
                        show_timestamps,
                    );
                    if !formatted.is_empty() {
                        yield Ok(formatted);
                    }
                }
                if let Some(termination) = termination.as_ref() {
                    tracing::info!(
                        target: "klights::pod_logs",
                        namespace = %termination.namespace,
                        pod = %termination.name,
                        uid = %termination.uid,
                        container = %termination.container_name,
                        "pod log follow terminal drain complete; closing stream"
                    );
                }
                return;
            }

            if let Some(termination) = termination.as_mut() {
                tokio::select! {
                    readable = async_inotify.readable() => {
                        match readable {
                            Ok(mut guard) => {
                                drain_inotify_events(&mut guard);
                            }
                            Err(e) => {
                                tracing::error!("inotify wait error: {}", e);
                                break;
                            }
                        }
                    }
                    event = termination.next_event() => {
                        match event {
                            Ok(event) => {
                                if pod_log_follow_event_is_terminal(termination, &event) {
                                    tracing::info!(
                                        target: "klights::pod_logs",
                                        namespace = %termination.namespace,
                                        pod = %termination.name,
                                        uid = %termination.uid,
                                        container = %termination.container_name,
                                        "pod log follow received terminal watch event; draining remaining log bytes"
                                    );
                                    terminate_after_drain = true;
                                }
                            }
                            #[cfg(test)]
                            Err(PodLogEventError::Lagged(skipped)) => {
                                tracing::warn!(
                                    skipped,
                                    namespace = %termination.namespace,
                                    pod = %termination.name,
                                    uid = %termination.uid,
                                    "pod log follow missed pod watch events; continuing until a later terminal event"
                                );
                            }
                            Err(PodLogEventError::Expired) => {
                                tracing::warn!(
                                    namespace = %termination.namespace,
                                    pod = %termination.name,
                                    uid = %termination.uid,
                                    "pod log follow watch replay expired; continuing until a later terminal event"
                                );
                            }
                            Err(PodLogEventError::Replay(err)) => {
                                tracing::warn!(
                                    error = %err,
                                    namespace = %termination.namespace,
                                    pod = %termination.name,
                                    uid = %termination.uid,
                                    "pod log follow watch replay failed; continuing until a later terminal event"
                                );
                            }
                            Err(PodLogEventError::Closed) => {
                                tracing::warn!(
                                    namespace = %termination.namespace,
                                    pod = %termination.name,
                                    uid = %termination.uid,
                                    "pod log follow watch channel closed; ending stream"
                                );
                                terminate_after_drain = true;
                            }
                        }
                    }
                }
            } else {
                match async_inotify.readable().await {
                    Ok(mut guard) => {
                        if drain_inotify_events(&mut guard) {
                            tracing::info!(
                                target: "klights::pod_logs",
                                log_path = %log_path,
                                "pod log follow observed terminal log-file event; draining remaining bytes"
                            );
                            terminate_after_drain = true;
                        }
                    }
                    Err(e) => {
                        tracing::error!("inotify wait error: {}", e);
                        break;
                    }
                }
            }
        }
    }
}

fn pod_log_follow_event_is_terminal(
    termination: &PodLogFollowTermination,
    event: &WatchEvent,
) -> bool {
    if !pod_identity_matches(
        event.object.as_ref(),
        &termination.namespace,
        &termination.name,
        &termination.uid,
    ) {
        return false;
    }

    let terminal_reason = match event.event_type {
        EventType::Deleted => Some("pod deleted".to_string()),
        EventType::Added | EventType::Modified => {
            pod_log_follow_terminal_reason(event.object.as_ref(), &termination.container_name)
        }
        EventType::Bookmark | EventType::Error => None,
    };
    let terminal = terminal_reason.is_some();
    tracing::info!(
        target: "klights::pod_logs",
        namespace = %termination.namespace,
        pod = %termination.name,
        uid = %termination.uid,
        container = %termination.container_name,
        event_type = ?&event.event_type,
        phase = ?event.object.pointer("/status/phase").and_then(|value| value.as_str()),
        terminal,
        terminal_reason = ?terminal_reason,
        "pod log follow observed matching pod watch event"
    );
    terminal
}

fn pod_identity_matches(pod: &serde_json::Value, namespace: &str, name: &str, uid: &str) -> bool {
    pod.get("kind").and_then(|kind| kind.as_str()) == Some("Pod")
        && pod
            .pointer("/metadata/namespace")
            .and_then(|value| value.as_str())
            == Some(namespace)
        && pod
            .pointer("/metadata/name")
            .and_then(|value| value.as_str())
            == Some(name)
        && pod
            .pointer("/metadata/uid")
            .and_then(|value| value.as_str())
            == Some(uid)
}

fn pod_log_follow_terminal_reason(pod: &serde_json::Value, container_name: &str) -> Option<String> {
    if pod
        .pointer("/status/phase")
        .and_then(|value| value.as_str())
        .is_some_and(|phase| phase == "Succeeded" || phase == "Failed")
    {
        let phase = pod
            .pointer("/status/phase")
            .and_then(|value| value.as_str())
            .unwrap_or("unknown");
        return Some(format!("pod phase {phase}"));
    }

    [
        "containerStatuses",
        "initContainerStatuses",
        "ephemeralContainerStatuses",
    ]
    .iter()
    .find_map(|field| {
        let terminated = pod
            .get("status")
            .and_then(|status| status.get(*field))
            .and_then(|statuses| statuses.as_array())
            .is_some_and(|statuses| {
                statuses.iter().any(|status| {
                    status.get("name").and_then(|value| value.as_str()) == Some(container_name)
                        && status.pointer("/state/terminated").is_some()
                })
            });
        terminated.then(|| format!("{field}/{container_name} terminated"))
    })
}

fn format_raw_cri_log_bytes_for_client(bytes: &[u8], show_timestamps: bool) -> Bytes {
    let mut pending = Vec::new();
    let mut output =
        format_complete_raw_cri_log_chunk_for_client(bytes, &mut pending, show_timestamps).to_vec();
    if !pending.is_empty() {
        output.extend_from_slice(
            flush_pending_raw_log_line_for_client(&mut pending, show_timestamps).as_ref(),
        );
    }
    Bytes::from(output)
}

fn format_complete_raw_cri_log_chunk_for_client(
    chunk: &[u8],
    pending_raw_log_line: &mut Vec<u8>,
    show_timestamps: bool,
) -> Bytes {
    pending_raw_log_line.extend_from_slice(chunk);
    let mut output = Vec::with_capacity(chunk.len());
    let mut start = 0usize;

    while let Some(relative_end) = pending_raw_log_line[start..]
        .iter()
        .position(|byte| *byte == b'\n')
    {
        let end = start + relative_end + 1;
        append_raw_cri_log_line_for_client(
            &mut output,
            &pending_raw_log_line[start..end],
            show_timestamps,
        );
        start = end;
    }

    if start > 0 {
        pending_raw_log_line.drain(..start);
    }

    Bytes::from(output)
}

fn flush_pending_raw_log_line_for_client(
    pending_raw_log_line: &mut Vec<u8>,
    show_timestamps: bool,
) -> Bytes {
    let mut output = Vec::with_capacity(pending_raw_log_line.len() + 1);
    append_raw_cri_log_line_for_client(&mut output, pending_raw_log_line, show_timestamps);
    pending_raw_log_line.clear();
    Bytes::from(output)
}

fn append_raw_cri_log_line_for_client(
    output: &mut Vec<u8>,
    raw_line_with_ending: &[u8],
    show_timestamps: bool,
) {
    let raw_line = trim_raw_log_line_end(raw_line_with_ending);
    match split_cri_log_line_bytes(raw_line) {
        Some((timestamp, message)) => {
            if show_timestamps {
                output.extend_from_slice(timestamp);
                output.push(b' ');
            }
            output.extend_from_slice(message);
        }
        None => output.extend_from_slice(raw_line),
    }
    if raw_line_with_ending.ends_with(b"\n") {
        output.push(b'\n');
    }
}

fn split_cri_log_line_bytes(line: &[u8]) -> Option<(&[u8], &[u8])> {
    let first = line.iter().position(|byte| *byte == b' ')?;
    let rest = &line[first + 1..];
    let second = rest.iter().position(|byte| *byte == b' ')?;
    let rest = &rest[second + 1..];
    let third = rest.iter().position(|byte| *byte == b' ')?;
    Some((&line[..first], &rest[third + 1..]))
}

async fn open_log_file_supervised(
    log_path: &str,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
) -> io::Result<blocking_fs::File> {
    let path = PathBuf::from(log_path);
    let key = log_path.to_string();

    task_supervisor
        .run_blocking_file_keyed("pod_log_follow_open", key, move || {
            blocking_fs::File::open(path)
        })
        .await
        .map_err(anyhow_to_io_error)?
}

async fn open_log_file_for_follow(
    log_path: &str,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
    mut termination: Option<&mut PodLogFollowTermination>,
) -> io::Result<Option<blocking_fs::File>> {
    loop {
        match open_log_file_supervised(log_path, task_supervisor).await {
            Ok(file) => return Ok(Some(file)),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                if termination
                    .as_ref()
                    .is_some_and(|watch| watch.terminate_after_initial)
                {
                    return Ok(None);
                }
                let watcher = watch_nearest_log_path_parent(log_path, task_supervisor).await?;
                match open_log_file_supervised(log_path, task_supervisor).await {
                    Ok(file) => return Ok(Some(file)),
                    Err(err) if err.kind() == io::ErrorKind::NotFound => {
                        if wait_for_log_path_activity_or_terminal(
                            &watcher,
                            termination.as_deref_mut(),
                        )
                        .await?
                        {
                            return Ok(None);
                        }
                    }
                    Err(err) => return Err(err),
                }
            }
            Err(err) => return Err(err),
        }
    }
}

async fn watch_nearest_log_path_parent(
    log_path: &str,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
) -> io::Result<tokio::io::unix::AsyncFd<inotify::Inotify>> {
    use inotify::{Inotify, WatchMask};

    let watch_dir = nearest_existing_log_watch_dir(log_path, task_supervisor).await?;
    let inotify = Inotify::init()?;
    inotify.watches().add(
        &watch_dir,
        WatchMask::CREATE
            | WatchMask::MOVED_TO
            | WatchMask::CLOSE_WRITE
            | WatchMask::ATTRIB
            | WatchMask::DELETE_SELF
            | WatchMask::MOVE_SELF,
    )?;
    tokio::io::unix::AsyncFd::new(inotify)
}

async fn nearest_existing_log_watch_dir(
    log_path: &str,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
) -> io::Result<PathBuf> {
    let path = PathBuf::from(log_path);
    let key = log_path.to_string();
    task_supervisor
        .run_blocking_file_keyed("pod_log_follow_watch_dir", key, move || {
            nearest_existing_log_watch_dir_blocking(path)
        })
        .await
        .map_err(anyhow_to_io_error)?
}

fn nearest_existing_log_watch_dir_blocking(path: PathBuf) -> io::Result<PathBuf> {
    let mut candidate = path
        .parent()
        .map(PathBuf::from)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "log path has no parent"))?;
    loop {
        match blocking_fs::metadata(&candidate) {
            Ok(meta) if meta.is_dir() => return Ok(candidate),
            Ok(_) => {
                if candidate.pop() {
                    continue;
                }
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "no directory ancestor for log path",
                ));
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                if candidate.pop() {
                    continue;
                }
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "no directory ancestor for log path",
                ));
            }
            Err(err) => return Err(err),
        }
    }
}

async fn wait_for_log_path_activity_or_terminal(
    async_inotify: &tokio::io::unix::AsyncFd<inotify::Inotify>,
    termination: Option<&mut PodLogFollowTermination>,
) -> io::Result<bool> {
    if let Some(termination) = termination {
        tokio::select! {
            readable = async_inotify.readable() => {
                let mut guard = readable?;
                let _ = drain_inotify_events(&mut guard);
                Ok(false)
            }
            event = termination.next_event() => {
                match event {
                    Ok(event) => Ok(pod_log_follow_event_is_terminal(termination, &event)),
                    #[cfg(test)]
                    Err(PodLogEventError::Lagged(skipped)) => {
                        tracing::warn!(
                            skipped,
                            namespace = %termination.namespace,
                            pod = %termination.name,
                            uid = %termination.uid,
                            "pod log follow missed pod watch events before log file creation"
                        );
                        Ok(false)
                    }
                    Err(PodLogEventError::Expired) => {
                        tracing::warn!(
                            namespace = %termination.namespace,
                            pod = %termination.name,
                            uid = %termination.uid,
                            "pod log follow watch replay expired before log file creation"
                        );
                        Ok(false)
                    }
                    Err(PodLogEventError::Replay(err)) => {
                        tracing::warn!(
                            error = %err,
                            namespace = %termination.namespace,
                            pod = %termination.name,
                            uid = %termination.uid,
                            "pod log follow watch replay failed before log file creation"
                        );
                        Ok(false)
                    }
                    Err(PodLogEventError::Closed) => {
                        tracing::warn!(
                            namespace = %termination.namespace,
                            pod = %termination.name,
                            uid = %termination.uid,
                            "pod log follow watch channel closed before log file creation"
                        );
                        Ok(true)
                    }
                }
            }
        }
    } else {
        wait_for_log_path_activity(async_inotify).await?;
        Ok(false)
    }
}

async fn wait_for_log_path_activity(
    async_inotify: &tokio::io::unix::AsyncFd<inotify::Inotify>,
) -> io::Result<()> {
    let mut guard = async_inotify.readable().await?;
    let _ = drain_inotify_events(&mut guard);
    Ok(())
}

async fn read_log_chunk_supervised(
    log_path: &str,
    file: blocking_fs::File,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
) -> io::Result<(blocking_fs::File, Bytes)> {
    let key = log_path.to_string();
    let read = move || {
        let mut file = file;
        let mut buffer = vec![0u8; LOG_FORWARD_CHUNK_SIZE];
        let n = file.read(&mut buffer)?;
        buffer.truncate(n);
        Ok((file, Bytes::from(buffer)))
    };

    task_supervisor
        .run_blocking_file_keyed("pod_log_follow_read", key, read)
        .await
        .map_err(anyhow_to_io_error)?
}

async fn seek_log_file_supervised(
    log_path: &str,
    file: blocking_fs::File,
    offset: u64,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
) -> io::Result<blocking_fs::File> {
    let key = log_path.to_string();
    let seek = move || {
        let mut file = file;
        file.seek(SeekFrom::Start(offset))?;
        Ok(file)
    };

    task_supervisor
        .run_blocking_file_keyed("pod_log_follow_seek", key, seek)
        .await
        .map_err(anyhow_to_io_error)?
}

async fn read_initial_follow_snapshot(
    log_path: &str,
    params: &LogQuery,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
) -> io::Result<(u64, Bytes)> {
    let path = PathBuf::from(log_path);
    let key = log_path.to_string();
    let params = params.clone();
    let read = move || read_initial_follow_snapshot_blocking(path, &params);

    task_supervisor
        .run_blocking_file_keyed("pod_log_follow_initial_snapshot", key, read)
        .await
        .map_err(anyhow_to_io_error)?
}

fn initial_follow_requires_snapshot(params: &LogQuery) -> bool {
    params.tail_lines.is_some()
        || params.limit_bytes.is_some()
        || params.since_seconds.is_some()
        || params.since_time.is_some()
}

fn read_initial_follow_snapshot_blocking(
    path: PathBuf,
    params: &LogQuery,
) -> io::Result<(u64, Bytes)> {
    let mut file = blocking_fs::File::open(path)?;
    let len = file.metadata()?.len();
    let len_usize = len.min(usize::MAX as u64) as usize;
    let since_cutoff = log_query_since_cutoff(params);
    let mut output = if let Some(ref cutoff) = since_cutoff {
        read_since_filtered_prefix_bytes(&mut file, cutoff, params.limit_bytes)?
    } else if let Some(tail_lines) = params.tail_lines {
        read_tail_bytes(&mut file, len, tail_lines)?
    } else if let Some(limit) = params.limit_bytes {
        read_prefix_bytes(&mut file, len_usize.min(limit))?
    } else {
        read_prefix_bytes(&mut file, len_usize)?
    };

    if let Some(limit) = params.limit_bytes {
        output.truncate(output.len().min(limit));
    }

    Ok((len, Bytes::from(output)))
}

fn read_since_filtered_prefix_bytes(
    file: &mut blocking_fs::File,
    cutoff: &chrono::DateTime<chrono::Utc>,
    limit_bytes: Option<usize>,
) -> io::Result<Vec<u8>> {
    file.seek(SeekFrom::Start(0))?;
    let mut reader = io::BufReader::new(file);
    let mut output = Vec::new();
    let mut line = Vec::new();

    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        let trimmed = trim_raw_log_line_end(&line);
        let include = std::str::from_utf8(trimmed)
            .map(|line| is_log_line_after_cutoff(line, Some(cutoff)))
            .unwrap_or(true);
        if !include {
            continue;
        }

        if let Some(limit) = limit_bytes {
            let remaining = limit.saturating_sub(output.len());
            if remaining == 0 {
                break;
            }
            output.extend_from_slice(&line[..line.len().min(remaining)]);
            if output.len() >= limit {
                break;
            }
        } else {
            output.extend_from_slice(&line);
        }
    }

    Ok(output)
}

fn read_prefix_bytes(file: &mut blocking_fs::File, max_len: usize) -> io::Result<Vec<u8>> {
    file.seek(SeekFrom::Start(0))?;
    let mut output = Vec::with_capacity(max_len.min(LOG_FORWARD_CHUNK_SIZE));
    let mut limited = file.take(max_len as u64);
    limited.read_to_end(&mut output)?;
    Ok(output)
}

fn read_tail_bytes(
    file: &mut blocking_fs::File,
    len: u64,
    tail_lines: usize,
) -> io::Result<Vec<u8>> {
    if tail_lines == 0 || len == 0 {
        return Ok(Vec::new());
    }

    let mut pos = len;
    let mut suffix = Vec::new();
    while pos > 0 {
        let read_size = (pos as usize).min(LOG_FORWARD_CHUNK_SIZE);
        pos -= read_size as u64;
        file.seek(SeekFrom::Start(pos))?;
        let mut chunk = vec![0u8; read_size];
        file.read_exact(&mut chunk)?;
        let mut combined = Vec::with_capacity(chunk.len() + suffix.len());
        combined.extend_from_slice(&chunk);
        combined.extend_from_slice(&suffix);
        suffix = combined;
        if count_log_lines_for_tail(&suffix) > tail_lines {
            break;
        }
    }

    let start = tail_start_index(&suffix, tail_lines);
    Ok(suffix[start..].to_vec())
}

fn count_log_lines_for_tail(bytes: &[u8]) -> usize {
    let mut count = bytes.iter().filter(|byte| **byte == b'\n').count();
    if !bytes.ends_with(b"\n") && !bytes.is_empty() {
        count += 1;
    }
    count
}

fn tail_start_index(bytes: &[u8], tail_lines: usize) -> usize {
    if tail_lines == 0 {
        return bytes.len();
    }

    let mut idx = bytes.len();
    if bytes.ends_with(b"\n") {
        idx = idx.saturating_sub(1);
    }

    let mut seen = 0usize;
    while idx > 0 {
        idx -= 1;
        if bytes[idx] == b'\n' {
            seen += 1;
            if seen == tail_lines {
                return idx + 1;
            }
        }
    }
    0
}

fn trim_raw_log_line_end(line: &[u8]) -> &[u8] {
    let line = line.strip_suffix(b"\n").unwrap_or(line);
    line.strip_suffix(b"\r").unwrap_or(line)
}

fn drain_inotify_events(
    guard: &mut tokio::io::unix::AsyncFdReadyGuard<'_, inotify::Inotify>,
) -> bool {
    // Drain inotify events via raw fd read to avoid busy-loop (clear_ready alone leaves fd readable).
    let mut drain = [0u8; 4096];
    match guard.try_io(|inner| {
        use std::os::unix::io::AsRawFd;
        let fd = inner.get_ref().as_raw_fd();
        // SAFETY: `fd` is owned by the wrapping `inner` reader and stays valid
        // for this call. `drain` is a fresh stack buffer of size `drain.len()`;
        // read(2) writes at most that many bytes and never reads from it.
        let n = unsafe { libc::read(fd, drain.as_mut_ptr() as *mut libc::c_void, drain.len()) };
        if n < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(n)
        }
    }) {
        Ok(Ok(n)) => inotify_buffer_has_terminal_file_event(&drain[..n as usize]),
        Err(_) => false,
        Ok(Err(e)) if e.kind() == io::ErrorKind::WouldBlock => false,
        Ok(Err(e)) => {
            tracing::warn!("inotify drain error: {}", e);
            false
        }
    }
}

fn inotify_buffer_has_terminal_file_event(bytes: &[u8]) -> bool {
    let mut offset = 0usize;
    let header_len = std::mem::size_of::<libc::inotify_event>();
    while offset + header_len <= bytes.len() {
        // SAFETY: the offset bounds check above guarantees the fixed-size
        // inotify_event header is present in `bytes`.
        let event = unsafe {
            std::ptr::read_unaligned(bytes.as_ptr().add(offset) as *const libc::inotify_event)
        };
        if event.mask
            & (libc::IN_CLOSE_WRITE | libc::IN_DELETE_SELF | libc::IN_MOVE_SELF | libc::IN_IGNORED)
            != 0
        {
            return true;
        }
        offset += header_len + event.len as usize;
    }
    false
}

fn anyhow_to_io_error(error: anyhow::Error) -> io::Error {
    if let Some(io_err) = error.downcast_ref::<io::Error>() {
        io::Error::new(io_err.kind(), io_err.to_string())
    } else {
        io::Error::other(error.to_string())
    }
}

async fn get_remote_pod_log(request: RemotePodLogRequest<'_>) -> Result<Response, AppError> {
    let RemotePodLogRequest {
        state,
        namespace,
        name,
        pod_uid,
        container_name,
        params,
        node_name,
    } = request;
    let replication = state.replication.as_ref().cloned().ok_or_else(|| {
        AppError::Internal("replication service not available for remote pod log".to_string())
    })?;

    let request = PodLogRequest {
        request_id: String::new(),
        node_name: node_name.to_string(),
        namespace: namespace.to_string(),
        pod_name: name.to_string(),
        pod_uid: pod_uid.to_string(),
        container_name: container_name.to_string(),
        follow: params.follow.clone(),
        tail_lines: params.tail_lines.map(|t| t.to_string()),
        timestamps: params.timestamps.clone(),
        since_time: params.since_time.clone(),
        since_seconds: params.since_seconds,
        limit_bytes: params.limit_bytes.map(|l| l as i64),
        previous: params.previous.clone(),
    };

    if params.follow.as_deref() == Some("true") {
        let mut session = replication
            .request_pod_log_stream(request)
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
                match session.recv_response().await {
                    Ok(Some(response)) => {
                        if let Some(error) = response.error {
                            tracing::warn!(
                                target: "klights::pod_logs",
                                namespace = %trace_namespace,
                                pod = %trace_name,
                                container = %trace_container,
                                node = %trace_node,
                                error = %error,
                                "remote pod log follow returned error"
                            );
                            session.close().await;
                            yield Err(std::io::Error::other(format!("remote pod log error: {error}")));
                            break;
                        }
                        if !response.log_content.is_empty() {
                            yield Ok(response.log_content);
                        }
                        if response.fin {
                            tracing::info!(
                                target: "klights::pod_logs",
                                namespace = %trace_namespace,
                                pod = %trace_name,
                                container = %trace_container,
                                node = %trace_node,
                                "remote pod log follow received terminal frame"
                            );
                            session.close().await;
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
                        session.close().await;
                        yield Err(std::io::Error::other(format!("remote pod log stream failed: {err}")));
                        break;
                    }
                }
            }
        };

        return build_text_log_response(axum::body::Body::from_stream(stream));
    }

    let response = replication
        .request_pod_log(request)
        .await
        .map_err(|e| AppError::BadGateway(format!("remote pod log request failed: {e}")))?;

    if let Some(error) = response.error {
        return Err(AppError::Internal(format!("remote pod log error: {error}")));
    }

    build_text_log_response(axum::body::Body::from(response.log_content))
}

struct PodLogFollowTrace {
    mode: &'static str,
    target: String,
    started: std::time::Instant,
}

impl PodLogFollowTrace {
    fn new(mode: &'static str, target: String) -> Self {
        tracing::info!(
            target: "klights::pod_logs",
            mode,
            log_target = %target,
            "pod log follow stream started"
        );
        Self {
            mode,
            target,
            started: std::time::Instant::now(),
        }
    }
}

impl Drop for PodLogFollowTrace {
    fn drop(&mut self) {
        tracing::info!(
            target: "klights::pod_logs",
            mode = self.mode,
            log_target = %self.target,
            elapsed_ms = self.started.elapsed().as_millis() as u64,
            "pod log follow stream ended or client disconnected"
        );
    }
}

async fn get_remote_pod_log_websocket(
    request: RemotePodLogWebSocketRequest,
) -> Result<Response, AppError> {
    let RemotePodLogWebSocketRequest {
        state,
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
    let replication = state.replication.as_ref().cloned();
    let request = PodLogRequest {
        request_id: String::new(),
        node_name,
        namespace,
        pod_name: name,
        pod_uid,
        container_name,
        follow: params.follow.clone(),
        tail_lines: params.tail_lines.map(|t| t.to_string()),
        timestamps: params.timestamps.clone(),
        since_time: params.since_time.clone(),
        since_seconds: params.since_seconds,
        limit_bytes: params.limit_bytes.map(|l| l as i64),
        previous: params.previous.clone(),
    };

    if let Err(err) = state
        .task_supervisor
        .spawn_async(
            crate::task_supervisor::TaskCategory::Others,
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
                            replication,
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
    replication: Option<Arc<crate::replication::ReplicationService>>,
    request: PodLogRequest,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use tokio_tungstenite::tungstenite::Message as TungsteniteMessage;
    use tokio_tungstenite::tungstenite::protocol::CloseFrame;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

    let output = match replication {
        Some(replication) => match replication.request_pod_log(request).await {
            Ok(response) => {
                if let Some(error) = response.error {
                    Err(format!("remote pod log error: {error}"))
                } else {
                    Ok(response.log_content)
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

// POST /api/v1/namespaces/{ns}/pods/{name}/exec
// Handles both WebSocket (kubectl v1.29+) and SPDY (older kubectl) upgrades
