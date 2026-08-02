//! Kubernetes `pods/log` orchestration over the transport-neutral node API.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::Request;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use klights_node_api::{NodeLog, NodeLogOptions, NodeLogRequest, NodeLogTarget};
use klights_supervisor::{TaskCategory, TaskSupervisor};
use serde::Deserialize;
use serde_json::Value;
use sha1::{Digest as _, Sha1};

use super::log_transport::{
    NodeLogOrigin, PodLogWebSocketRequest, open_log_body, read_log_bytes, serve_log_websocket,
};
use crate::AppError;

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct PodLogQuery {
    pub container: Option<String>,
    pub follow: Option<String>,
    #[serde(rename = "tailLines")]
    pub tail_lines: Option<usize>,
    pub timestamps: Option<String>,
    #[serde(rename = "sinceSeconds")]
    pub since_seconds: Option<i64>,
    #[serde(rename = "sinceTime")]
    pub since_time: Option<String>,
    #[serde(rename = "limitBytes")]
    pub limit_bytes: Option<usize>,
    pub previous: Option<String>,
    #[serde(rename = "insecureSkipTLSVerifyBackend", default)]
    pub insecure_skip_tls_verify_backend: bool,
}

/// Root-composed capabilities used by the native Pod-log handler.
///
/// Local HTTP follow retains its Pod-event-aware runtime. Local WebSocket
/// follow intentionally receives the no-watch runtime because that is the
/// existing client-disconnect-owned termination mode. Remote requests use the
/// leader/worker node-log capability.
pub struct PodLogCapabilities {
    local_http: Arc<dyn NodeLog>,
    local_websocket: Arc<dyn NodeLog>,
    task_supervisor: Arc<TaskSupervisor>,
    local_node_name: String,
}

impl PodLogCapabilities {
    pub fn new(
        local_http: Arc<dyn NodeLog>,
        local_websocket: Arc<dyn NodeLog>,
        task_supervisor: Arc<TaskSupervisor>,
        local_node_name: impl Into<String>,
    ) -> Self {
        Self {
            local_http,
            local_websocket,
            task_supervisor,
            local_node_name: local_node_name.into(),
        }
    }
}

pub struct PodLogRouteRequest {
    pub namespace: String,
    pub name: String,
    pub pod: Option<Arc<Value>>,
    pub query: PodLogQuery,
    pub request: Request,
}

struct PodLogPlan {
    node_log: Arc<dyn NodeLog>,
    request: NodeLogRequest,
    origin: NodeLogOrigin,
    follow: bool,
    websocket_follow: bool,
    websocket_skip_previous_read: bool,
    websocket_send_terminal_error: bool,
    websocket_task_name: &'static str,
}

/// Serves one Kubernetes `pods/log` request after the root query adapter has
/// supplied the current Pod snapshot.
pub async fn get_pod_log(
    capabilities: &PodLogCapabilities,
    remote: Option<Arc<dyn NodeLog>>,
    route: PodLogRouteRequest,
) -> Result<Response, AppError> {
    let PodLogRouteRequest {
        namespace,
        name,
        pod,
        query,
        request,
    } = route;
    let pod = pod.ok_or_else(|| AppError::NotFound(format!("Pod {namespace}/{name} not found")))?;
    let websocket = is_pod_log_websocket_upgrade(request.headers());
    let plan = build_plan(
        capabilities,
        pod.as_ref(),
        &namespace,
        &name,
        query,
        websocket,
        remote.as_ref(),
    )?;

    if websocket {
        return start_websocket(capabilities.task_supervisor.clone(), request, plan).await;
    }

    let body = if plan.follow {
        open_log_body(plan.node_log, plan.request, plan.origin).await?
    } else {
        Body::from(read_log_bytes(plan.node_log.as_ref(), plan.request, plan.origin).await?)
    };
    build_text_log_response(body)
}

fn build_plan(
    capabilities: &PodLogCapabilities,
    pod: &Value,
    namespace: &str,
    name: &str,
    query: PodLogQuery,
    websocket: bool,
    remote: Option<&Arc<dyn NodeLog>>,
) -> Result<PodLogPlan, AppError> {
    let container_name = selected_container(pod, query.container.as_deref(), namespace, name)?;
    let pod_uid = pod
        .pointer("/metadata/uid")
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::Internal("Pod has no UID".to_string()))?;
    let remote_node = remote_pod_node_name(pod, &capabilities.local_node_name);
    let (node_log, node_name, origin) = match remote_node {
        Some(node_name) => (
            remote.cloned().ok_or_else(|| {
                AppError::Internal(
                    "replication service not available for remote pod log".to_string(),
                )
            })?,
            node_name,
            NodeLogOrigin::Remote,
        ),
        None => (
            if websocket {
                capabilities.local_websocket.clone()
            } else {
                capabilities.local_http.clone()
            },
            capabilities.local_node_name.clone(),
            NodeLogOrigin::Local,
        ),
    };
    let follow = query.follow.as_deref() == Some("true");
    let request =
        pod_log_request_from_parts(&node_name, namespace, name, pod_uid, &container_name, query)?;
    let remote = origin == NodeLogOrigin::Remote;

    Ok(PodLogPlan {
        node_log,
        request,
        origin,
        follow,
        // Remote WebSockets keep the existing finite read even for follow.
        websocket_follow: !remote && follow,
        websocket_skip_previous_read: !remote,
        websocket_send_terminal_error: remote,
        websocket_task_name: if remote {
            "pod_log_remote_ws_upgrade"
        } else {
            "pod_log_ws_upgrade"
        },
    })
}

fn selected_container(
    pod: &Value,
    requested: Option<&str>,
    namespace: &str,
    name: &str,
) -> Result<String, AppError> {
    if let Some(requested) = requested {
        if pod_container_names(pod).any(|candidate| candidate == requested) {
            return Ok(requested.to_string());
        }
        return Err(AppError::BadRequest(format!(
            "container {requested} is not valid for pod {namespace}/{name}"
        )));
    }

    pod.pointer("/spec/containers/0/name")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| AppError::BadRequest("No containers in pod spec".to_string()))
}

fn pod_container_names(pod: &Value) -> impl Iterator<Item = &str> {
    ["containers", "initContainers", "ephemeralContainers"]
        .into_iter()
        .flat_map(|field| {
            pod.pointer(&format!("/spec/{field}"))
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
        })
        .filter_map(|container| container.get("name").and_then(Value::as_str))
}

fn remote_pod_node_name(pod: &Value, local_node_name: &str) -> Option<String> {
    pod.pointer("/spec/nodeName")
        .and_then(Value::as_str)
        .filter(|node_name| !node_name.is_empty() && *node_name != local_node_name)
        .map(str::to_string)
}

fn pod_log_request_from_parts(
    node_name: &str,
    namespace: &str,
    pod_name: &str,
    pod_uid: &str,
    container_name: &str,
    query: PodLogQuery,
) -> Result<NodeLogRequest, AppError> {
    let target = NodeLogTarget::try_new(node_name, namespace, pod_name, pod_uid, container_name)
        .map_err(|error| AppError::BadRequest(format!("invalid pod log target: {error}")))?;
    Ok(NodeLogRequest::new(
        target,
        NodeLogOptions::new(
            query.follow,
            query.tail_lines,
            query.timestamps,
            query.since_time,
            query.since_seconds,
            query.limit_bytes,
            query.previous,
        ),
    ))
}

fn build_text_log_response(body: Body) -> Result<Response, AppError> {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(body)
        .map(IntoResponse::into_response)
        .map_err(|error| AppError::Internal(format!("Failed to build pod log response: {error}")))
}

fn is_pod_log_websocket_upgrade(headers: &HeaderMap) -> bool {
    headers
        .get(header::UPGRADE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|upgrade| upgrade.eq_ignore_ascii_case("websocket"))
}

fn negotiate_pod_log_websocket_subprotocol(headers: &HeaderMap) -> String {
    for value in headers.get_all(header::SEC_WEBSOCKET_PROTOCOL) {
        if let Ok(protocols) = value.to_str()
            && let Some(protocol) = protocols.split(',').map(str::trim).find(|protocol| {
                *protocol == "binary.k8s.io" || *protocol == "base64.binary.k8s.io"
            })
        {
            return protocol.to_string();
        }
    }
    "binary.k8s.io".to_string()
}

fn derive_websocket_accept_key(key: &HeaderValue) -> String {
    const WEBSOCKET_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    let mut hasher = Sha1::new();
    hasher.update(key.as_bytes());
    hasher.update(WEBSOCKET_GUID.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(hasher.finalize())
}

fn build_pod_log_websocket_response(
    key: &HeaderValue,
    subprotocol: String,
) -> Result<Response, AppError> {
    Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header(header::UPGRADE, "websocket")
        .header(header::CONNECTION, "Upgrade")
        .header(
            header::SEC_WEBSOCKET_ACCEPT,
            derive_websocket_accept_key(key),
        )
        .header(header::SEC_WEBSOCKET_PROTOCOL, subprotocol)
        .body(Body::empty())
        .map_err(|error| AppError::Internal(format!("Failed to build WebSocket response: {error}")))
}

async fn start_websocket(
    task_supervisor: Arc<TaskSupervisor>,
    request: Request,
    plan: PodLogPlan,
) -> Result<Response, AppError> {
    let key = request
        .headers()
        .get(header::SEC_WEBSOCKET_KEY)
        .ok_or_else(|| AppError::BadRequest("Missing Sec-WebSocket-Key header".to_string()))?
        .clone();
    let subprotocol = negotiate_pod_log_websocket_subprotocol(request.headers());
    let on_upgrade = hyper::upgrade::on(request);
    let task_name = plan.websocket_task_name;
    let websocket_request = PodLogWebSocketRequest {
        node_log: plan.node_log,
        request: plan.request,
        origin: plan.origin,
        follow: plan.websocket_follow,
        skip_previous_read: plan.websocket_skip_previous_read,
        send_terminal_error: plan.websocket_send_terminal_error,
    };

    if let Err(error) = task_supervisor
        .spawn_async(TaskCategory::Others, task_name, async move {
            match on_upgrade.await {
                Ok(upgraded) => {
                    let io = hyper_util::rt::TokioIo::new(upgraded);
                    let socket = tokio_tungstenite::WebSocketStream::from_raw_socket(
                        io,
                        tokio_tungstenite::tungstenite::protocol::Role::Server,
                        None,
                    )
                    .await;
                    serve_log_websocket(socket, websocket_request).await;
                }
                Err(error) => tracing::error!(%error, "Pod log WebSocket upgrade failed"),
            }
        })
        .await
    {
        tracing::warn!(%error, "Failed to spawn Pod log WebSocket upgrade task");
    }

    build_pod_log_websocket_response(&key, subprotocol)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    use klights_node_api::{
        BoundedByteStream, ByteStreamBounds, ByteStreamError, ByteStreamFuture, NodeLogEvent,
        NodeLogFuture, NodeLogResult,
    };
    use serde_json::json;
    use tokio::sync::Mutex;

    use super::*;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum CallKind {
        Read,
        Open,
    }

    struct RecordingNodeLog {
        calls: StdMutex<Vec<(CallKind, NodeLogRequest)>>,
        finite: Vec<u8>,
    }

    impl RecordingNodeLog {
        fn new(finite: &[u8]) -> Self {
            Self {
                calls: StdMutex::new(Vec::new()),
                finite: finite.to_vec(),
            }
        }
    }

    impl NodeLog for RecordingNodeLog {
        fn read_logs(&self, request: NodeLogRequest) -> NodeLogFuture<'_, NodeLogResult> {
            self.calls.lock().unwrap().push((CallKind::Read, request));
            let finite = self.finite.clone();
            Box::pin(async move { Ok(NodeLogResult::success(finite)) })
        }

        fn open_logs(
            &self,
            request: NodeLogRequest,
        ) -> NodeLogFuture<'_, Box<dyn BoundedByteStream<Frame = NodeLogEvent>>> {
            self.calls.lock().unwrap().push((CallKind::Open, request));
            Box::pin(async move {
                Ok(Box::new(RecordingStream {
                    events: Mutex::new(
                        [
                            NodeLogEvent::data(b"follow ".to_vec()),
                            NodeLogEvent::complete(b"done\n".to_vec()),
                        ]
                        .into(),
                    ),
                    cancelled: AtomicBool::new(false),
                })
                    as Box<dyn BoundedByteStream<Frame = NodeLogEvent>>)
            })
        }
    }

    struct RecordingStream {
        events: Mutex<VecDeque<NodeLogEvent>>,
        cancelled: AtomicBool,
    }

    impl BoundedByteStream for RecordingStream {
        type Frame = NodeLogEvent;

        fn bounds(&self) -> ByteStreamBounds {
            ByteStreamBounds::try_new(4, 4096).unwrap()
        }

        fn is_cancelled(&self) -> bool {
            self.cancelled.load(Ordering::Acquire)
        }

        fn send_frame(&self, _frame: NodeLogEvent) -> ByteStreamFuture<'_, ()> {
            Box::pin(async { Err(ByteStreamError::closed("receive-only fake")) })
        }

        fn recv_frame(&self) -> ByteStreamFuture<'_, Option<NodeLogEvent>> {
            Box::pin(async move { Ok(self.events.lock().await.pop_front()) })
        }

        fn cancel(&mut self) -> ByteStreamFuture<'_, ()> {
            Box::pin(async move {
                self.cancelled.store(true, Ordering::Release);
                Ok(())
            })
        }
    }

    fn pod(node_name: &str) -> Arc<Value> {
        Arc::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"namespace": "default", "name": "pod-a", "uid": "uid-a"},
            "spec": {
                "nodeName": node_name,
                "containers": [{"name": "app"}],
                "initContainers": [{"name": "init"}],
                "ephemeralContainers": [{"name": "debug"}]
            }
        }))
    }

    fn capabilities(
        local_http: Arc<dyn NodeLog>,
        local_websocket: Arc<dyn NodeLog>,
    ) -> PodLogCapabilities {
        PodLogCapabilities::new(
            local_http,
            local_websocket,
            Arc::new(TaskSupervisor::new(Default::default())),
            "node-a",
        )
    }

    #[test]
    fn phase17c_pod_log_query_preserves_every_kubernetes_field() {
        let query: PodLogQuery = serde_json::from_value(json!({
            "container": "debug", "follow": "true", "tailLines": 100,
            "timestamps": "true", "sinceSeconds": 60,
            "sinceTime": "2026-08-02T00:00:00Z", "limitBytes": 4096,
            "previous": "false", "insecureSkipTLSVerifyBackend": true
        }))
        .unwrap();
        assert_eq!(query.container.as_deref(), Some("debug"));
        assert_eq!(query.follow.as_deref(), Some("true"));
        assert_eq!(query.tail_lines, Some(100));
        assert_eq!(query.timestamps.as_deref(), Some("true"));
        assert_eq!(query.since_seconds, Some(60));
        assert_eq!(query.since_time.as_deref(), Some("2026-08-02T00:00:00Z"));
        assert_eq!(query.limit_bytes, Some(4096));
        assert_eq!(query.previous.as_deref(), Some("false"));
        assert!(query.insecure_skip_tls_verify_backend);
    }

    #[test]
    fn phase17c_pod_log_container_validation_covers_all_kinds_and_traversal() {
        let pod = pod("node-a");
        for container in ["app", "init", "debug"] {
            assert_eq!(
                selected_container(&pod, Some(container), "default", "pod-a").unwrap(),
                container
            );
        }
        assert!(matches!(
            selected_container(&pod, Some("../../etc"), "default", "pod-a"),
            Err(AppError::BadRequest(message))
                if message == "container ../../etc is not valid for pod default/pod-a"
        ));
    }

    #[tokio::test]
    async fn phase17c_pod_log_http_preserves_local_remote_finite_follow_and_identity() {
        for (node_name, follow, expected_call, expected_body) in [
            ("node-a", false, CallKind::Read, b"local\n".as_slice()),
            ("node-a", true, CallKind::Open, b"follow done\n".as_slice()),
            ("node-b", false, CallKind::Read, b"remote\n".as_slice()),
            ("node-b", true, CallKind::Open, b"follow done\n".as_slice()),
        ] {
            let local_http = Arc::new(RecordingNodeLog::new(b"local\n"));
            let local_websocket = Arc::new(RecordingNodeLog::new(b"wrong websocket\n"));
            let remote = Arc::new(RecordingNodeLog::new(b"remote\n"));
            let capabilities = capabilities(local_http.clone(), local_websocket);
            let response = get_pod_log(
                &capabilities,
                Some(remote.clone()),
                PodLogRouteRequest {
                    namespace: "default".to_string(),
                    name: "pod-a".to_string(),
                    pod: Some(pod(node_name)),
                    query: PodLogQuery {
                        container: Some("debug".to_string()),
                        follow: follow.then(|| "true".to_string()),
                        tail_lines: Some(20),
                        ..Default::default()
                    },
                    request: Request::new(Body::empty()),
                },
            )
            .await
            .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.headers().get(header::CONTENT_TYPE).unwrap(),
                "text/plain; charset=utf-8"
            );
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            assert_eq!(body.as_ref(), expected_body);

            let calls = if node_name == "node-a" {
                local_http.calls.lock().unwrap()
            } else {
                remote.calls.lock().unwrap()
            };
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0].0, expected_call);
            assert_eq!(calls[0].1.target().node_name(), node_name);
            assert_eq!(calls[0].1.target().pod_uid(), "uid-a");
            assert_eq!(calls[0].1.target().container_name(), "debug");
            assert_eq!(calls[0].1.options().tail_lines(), Some(20));
        }
    }

    #[test]
    fn phase17c_pod_log_websocket_plan_preserves_local_follow_and_remote_finite_modes() {
        let local_http = Arc::new(RecordingNodeLog::new(b"http"));
        let local_websocket = Arc::new(RecordingNodeLog::new(b"websocket"));
        let remote: Arc<dyn NodeLog> = Arc::new(RecordingNodeLog::new(b"remote"));
        let capabilities = capabilities(local_http, local_websocket);
        let query = PodLogQuery {
            follow: Some("true".to_string()),
            ..Default::default()
        };

        let local = build_plan(
            &capabilities,
            pod("node-a").as_ref(),
            "default",
            "pod-a",
            query.clone(),
            true,
            None,
        )
        .unwrap();
        assert!(local.websocket_follow);
        assert!(local.websocket_skip_previous_read);
        assert!(!local.websocket_send_terminal_error);
        assert_eq!(local.websocket_task_name, "pod_log_ws_upgrade");

        let remote = build_plan(
            &capabilities,
            pod("node-b").as_ref(),
            "default",
            "pod-a",
            query,
            true,
            Some(&remote),
        )
        .unwrap();
        assert!(!remote.websocket_follow);
        assert!(!remote.websocket_skip_previous_read);
        assert!(remote.websocket_send_terminal_error);
        assert_eq!(remote.websocket_task_name, "pod_log_remote_ws_upgrade");
        assert_eq!(remote.request.options().follow(), Some("true"));
    }

    #[test]
    fn phase17c_pod_log_websocket_handshake_preserves_protocol_and_rfc_accept_key() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_static("unknown, base64.binary.k8s.io, binary.k8s.io"),
        );
        assert_eq!(
            negotiate_pod_log_websocket_subprotocol(&headers),
            "base64.binary.k8s.io"
        );
        let response = build_pod_log_websocket_response(
            &HeaderValue::from_static("dGhlIHNhbXBsZSBub25jZQ=="),
            "binary.k8s.io".to_string(),
        )
        .unwrap();
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        assert_eq!(
            response
                .headers()
                .get(header::SEC_WEBSOCKET_ACCEPT)
                .unwrap(),
            "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
        );
        assert_eq!(
            response
                .headers()
                .get(header::SEC_WEBSOCKET_PROTOCOL)
                .unwrap(),
            "binary.k8s.io"
        );
    }

    #[test]
    fn phase17c_pod_log_errors_keep_exact_kubernetes_messages() {
        let local = Arc::new(RecordingNodeLog::new(b""));
        let capabilities = capabilities(local.clone(), local);
        assert!(matches!(
            build_plan(
                &capabilities,
                pod("node-b").as_ref(),
                "default",
                "pod-a",
                PodLogQuery::default(),
                false,
                None,
            ),
            Err(AppError::Internal(message))
                if message == "replication service not available for remote pod log"
        ));
        assert!(matches!(
            selected_container(&json!({"spec": {"containers": []}}), None, "ns", "empty"),
            Err(AppError::BadRequest(message)) if message == "No containers in pod spec"
        ));
    }
}
