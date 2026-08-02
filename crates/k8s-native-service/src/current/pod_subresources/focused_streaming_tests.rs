use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use axum::routing::get;
use klights_cluster_core::Resource;
use klights_leader_api::{
    LeaderResourceQuery, ResourceGetRequest, ResourceListRequest, ResourceListResult,
    ResourceQueryFuture,
};
use klights_node_api::{
    ExecSetupError, NodeExec, NodeExecFuture, NodeExecRequest, NodeExecSession,
    NodeExecSyncRequest, NodeExecSyncResult, NodeLog, NodeLogFuture, NodeLogRequest, NodeLogResult,
    NodePortForward, NodePortForwardFuture, NodePortForwardRequest, NodePortForwardSession,
    NodePortForwardSetupError,
};
use klights_pod_api::{
    PodGetRequest, PodListRequest, PodListResult, PodOwnerListRequest, PodQuery,
    PodRepositoryError, PodRepositoryFuture,
};
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::generic_command::{
    GenericCommandFuture, ResourceAdmissionPort, ResourceAdmissionRequest,
};
use crate::streaming::{StreamingDependencies, StreamingState, pod_attach, pod_exec};

struct FixedPodQuery {
    pod: Resource,
}

impl PodQuery for FixedPodQuery {
    fn get_pod(&self, _request: PodGetRequest) -> PodRepositoryFuture<'_, Option<Resource>> {
        let pod = self.pod.clone();
        Box::pin(async move { Ok(Some(pod)) })
    }

    fn list_pods(&self, _request: PodListRequest) -> PodRepositoryFuture<'_, PodListResult> {
        Box::pin(async { Err(PodRepositoryError::unavailable("unused Pod list")) })
    }

    fn list_pods_by_owner_uid(
        &self,
        _request: PodOwnerListRequest,
    ) -> PodRepositoryFuture<'_, Vec<Resource>> {
        Box::pin(async { Err(PodRepositoryError::unavailable("unused owner list")) })
    }
}

struct UnavailableNodeExec;

impl NodeExec for UnavailableNodeExec {
    fn exec_sync(&self, _request: NodeExecSyncRequest) -> NodeExecFuture<'_, NodeExecSyncResult> {
        Box::pin(async { Err(ExecSetupError::unavailable("unused exec sync")) })
    }

    fn open_exec(&self, _request: NodeExecRequest) -> NodeExecFuture<'_, Box<dyn NodeExecSession>> {
        Box::pin(async { Err(ExecSetupError::unavailable("unused exec stream")) })
    }
}

struct UnavailablePortForward;

impl NodePortForward for UnavailablePortForward {
    fn open_port_forward(
        &self,
        _request: NodePortForwardRequest,
    ) -> NodePortForwardFuture<'_, Box<dyn NodePortForwardSession>> {
        Box::pin(async {
            Err(NodePortForwardSetupError::unavailable(
                "unused port-forward",
            ))
        })
    }
}

struct UnusedResourceQuery;

impl LeaderResourceQuery for UnusedResourceQuery {
    fn get_resource(
        &self,
        _request: ResourceGetRequest,
    ) -> ResourceQueryFuture<'_, Option<Resource>> {
        Box::pin(async {
            Err(klights_leader_api::ResourceQueryError::query_failed(
                "unused",
            ))
        })
    }

    fn list_resources(
        &self,
        _request: ResourceListRequest,
    ) -> ResourceQueryFuture<'_, ResourceListResult> {
        Box::pin(async {
            Err(klights_leader_api::ResourceQueryError::query_failed(
                "unused",
            ))
        })
    }
}

struct AllowAdmission;

impl ResourceAdmissionPort for AllowAdmission {
    fn admit(&self, request: ResourceAdmissionRequest) -> GenericCommandFuture<'_, Value> {
        Box::pin(async move { Ok(request.object) })
    }
}

struct FocusedStreamingState {
    dependencies: StreamingDependencies,
    resource_query: UnusedResourceQuery,
    admission: AllowAdmission,
}

impl StreamingState for FocusedStreamingState {
    fn streaming_dependencies(&self) -> &StreamingDependencies {
        &self.dependencies
    }

    fn streaming_resource_query(&self) -> &dyn LeaderResourceQuery {
        &self.resource_query
    }

    fn streaming_admission(&self) -> &dyn ResourceAdmissionPort {
        &self.admission
    }
}

fn remote_pod(name: &str) -> Resource {
    Resource::try_from_data(Arc::new(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": "default", "uid": format!("{name}-uid")},
        "spec": {
            "nodeName": "remote-worker",
            "containers": [{"name": "shell", "image": "busybox"}]
        },
        "status": {
            "phase": "Running",
            "containerStatuses": [{
                "name": "shell",
                "containerID": "containerd://remote-container"
            }]
        }
    })))
    .unwrap()
}

fn streaming_router(name: &str) -> Router {
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(Default::default()));
    let remote_exec = Arc::new(UnavailableNodeExec);
    let state = Arc::new(FocusedStreamingState {
        dependencies: StreamingDependencies::new(
            Arc::new(FixedPodQuery {
                pod: remote_pod(name),
            }),
            None,
            Some(remote_exec),
            Arc::new(UnavailablePortForward),
            Arc::<str>::from("local-node"),
            supervisor,
        ),
        resource_query: UnusedResourceQuery,
        admission: AllowAdmission,
    });
    Router::new()
        .route(
            "/api/v1/namespaces/{namespace}/pods/{name}/exec",
            get(pod_exec::<FocusedStreamingState>).post(pod_exec::<FocusedStreamingState>),
        )
        .route(
            "/api/v1/namespaces/{namespace}/pods/{name}/attach",
            get(pod_attach::<FocusedStreamingState>),
        )
        .with_state(state)
}

fn websocket_request(path: &str) -> Request<Body> {
    Request::get(path)
        .header(header::CONNECTION, "Upgrade")
        .header(header::UPGRADE, "websocket")
        .header(header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
        .header(header::SEC_WEBSOCKET_VERSION, "13")
        .header(header::SEC_WEBSOCKET_PROTOCOL, "v5.channel.k8s.io")
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn test_remote_websocket_exec_rejects_spdy_upgrade() {
    let response = streaming_router("remote-exec-spdy-reject")
        .oneshot(
            Request::post("/api/v1/namespaces/default/pods/remote-exec-spdy-reject/exec?command=%2Fbin%2Fsh&stdin=1&stdout=1&stderr=1&tty=1")
                .header(header::CONNECTION, "Upgrade")
                .header(header::UPGRADE, "SPDY/3.1")
                .header("x-stream-protocol-version", "v4.channel.k8s.io")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_remote_websocket_exec_accepts_upgrade_instead_of_bad_request() {
    let response = streaming_router("remote-exec-ws")
        .oneshot(websocket_request(
            "/api/v1/namespaces/default/pods/remote-exec-ws/exec?command=%2Fbin%2Fsh&stdin=1&stdout=1&stderr=1&tty=1",
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(
        response.headers()[header::SEC_WEBSOCKET_PROTOCOL],
        "v5.channel.k8s.io"
    );
}

#[tokio::test]
async fn test_remote_websocket_attach_accepts_upgrade_instead_of_not_implemented() {
    let response = streaming_router("remote-attach-ws")
        .oneshot(websocket_request(
            "/api/v1/namespaces/default/pods/remote-attach-ws/attach?stdin=1&stdout=1&stderr=1&tty=1",
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(
        response.headers()[header::SEC_WEBSOCKET_PROTOCOL],
        "v5.channel.k8s.io"
    );
}

struct UnavailableNodeLog;

impl NodeLog for UnavailableNodeLog {
    fn read_logs(&self, _request: NodeLogRequest) -> NodeLogFuture<'_, NodeLogResult> {
        Box::pin(async { panic!("focused handshake must not read logs") })
    }

    fn open_logs(
        &self,
        _request: NodeLogRequest,
    ) -> NodeLogFuture<
        '_,
        Box<dyn klights_node_api::BoundedByteStream<Frame = klights_node_api::NodeLogEvent>>,
    > {
        Box::pin(async { panic!("focused handshake must not open logs") })
    }
}

fn log_capabilities() -> Arc<crate::subresources::pod::logs::PodLogCapabilities> {
    let node_log = Arc::new(UnavailableNodeLog);
    Arc::new(crate::subresources::pod::logs::PodLogCapabilities::new(
        node_log.clone(),
        node_log,
        Arc::new(klights_supervisor::TaskSupervisor::new(Default::default())),
        "local-node",
    ))
}

#[tokio::test]
async fn test_remote_pod_log_websocket_accepts_upgrade_before_proxying() {
    let capabilities = log_capabilities();
    let remote: Arc<dyn NodeLog> = Arc::new(UnavailableNodeLog);
    let pod = Arc::new(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "remote-log-ws", "namespace": "default", "uid": "remote-log-ws-uid"},
        "spec": {"nodeName": "remote-worker", "containers": [{"name": "main"}]}
    }));
    let app = Router::new().route(
        "/log",
        get(move |request: axum::extract::Request| {
            let capabilities = capabilities.clone();
            let remote = remote.clone();
            let pod = pod.clone();
            async move {
                crate::subresources::pod::logs::get_pod_log(
                    capabilities.as_ref(),
                    Some(remote),
                    crate::subresources::pod::logs::PodLogRouteRequest {
                        namespace: "default".to_string(),
                        name: "remote-log-ws".to_string(),
                        pod: Some(pod),
                        query: crate::subresources::pod::logs::PodLogQuery {
                            container: Some("main".to_string()),
                            ..Default::default()
                        },
                        request,
                    },
                )
                .await
            }
        }),
    );
    let response = app
        .oneshot(websocket_request("/log").map(Body::from))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(
        response.headers()[header::SEC_WEBSOCKET_PROTOCOL],
        "binary.k8s.io"
    );
}

#[tokio::test]
async fn test_pod_log_route_missing_pod_returns_not_found() {
    let result = crate::subresources::pod::logs::get_pod_log(
        log_capabilities().as_ref(),
        None,
        crate::subresources::pod::logs::PodLogRouteRequest {
            namespace: "default".to_string(),
            name: "missing".to_string(),
            pod: None,
            query: Default::default(),
            request: Request::get("/log").body(Body::empty()).unwrap(),
        },
    )
    .await;
    assert!(matches!(result, Err(crate::AppError::NotFound(_))));
}
