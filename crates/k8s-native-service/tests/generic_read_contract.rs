use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue};
use axum::response::Response;
use k8s_native_service::ApiState;
use k8s_native_service::generic_read::{
    ContinueResourceVersion, GeneratedListInnerRequest, GenericListResponse,
    GenericReadControllerInputs, GenericReadFuture, GenericReadOperationalInputs,
    GenericReadResourceInputs, GenericReadSnapshot, GenericReadSnapshotPort,
    GenericReadSnapshotRequest, GenericReadWatchRequest, ListQuery, ListResourceVersionFuture,
    ListResourceVersionPort, get_inner, list_inner, process_continue_token_at,
};
use klights_cluster_core::Resource;
use klights_leader_api::{
    LeaderResourceQuery, ResourceGetRequest, ResourceListRequest, ResourceListResult,
    ResourceQueryFuture,
};
use klights_node_api::{
    ExecSetupError, NodeExec, NodeExecFuture, NodeExecRequest, NodeExecSession,
    NodeExecSyncRequest, NodeExecSyncResult, NodePortForward, NodePortForwardFuture,
    NodePortForwardRequest, NodePortForwardSession, NodePortForwardSetupError,
};
use klights_pod_api::{
    PodGetRequest, PodListRequest, PodListResult, PodOwnerListRequest, PodQuery,
    PodRepositoryError, PodRepositoryFuture,
};
use serde_json::{Value, json};

#[derive(Default)]
struct Captures {
    list: Option<GenericListResponse>,
    get: Option<(Value, HeaderMap)>,
}

struct FakeResources {
    resource: Resource,
    captures: Mutex<Captures>,
}

struct UnavailableStreaming;

impl PodQuery for UnavailableStreaming {
    fn get_pod(&self, _request: PodGetRequest) -> PodRepositoryFuture<'_, Option<Resource>> {
        Box::pin(async { Err(PodRepositoryError::unavailable("unused test dependency")) })
    }

    fn list_pods(&self, _request: PodListRequest) -> PodRepositoryFuture<'_, PodListResult> {
        Box::pin(async { Err(PodRepositoryError::unavailable("unused test dependency")) })
    }

    fn list_pods_by_owner_uid(
        &self,
        _request: PodOwnerListRequest,
    ) -> PodRepositoryFuture<'_, Vec<Resource>> {
        Box::pin(async { Err(PodRepositoryError::unavailable("unused test dependency")) })
    }
}

impl NodeExec for UnavailableStreaming {
    fn exec_sync(&self, _request: NodeExecSyncRequest) -> NodeExecFuture<'_, NodeExecSyncResult> {
        Box::pin(async { Err(ExecSetupError::unavailable("unused test dependency")) })
    }

    fn open_exec(&self, _request: NodeExecRequest) -> NodeExecFuture<'_, Box<dyn NodeExecSession>> {
        Box::pin(async { Err(ExecSetupError::unavailable("unused test dependency")) })
    }
}

impl NodePortForward for UnavailableStreaming {
    fn open_port_forward(
        &self,
        _request: NodePortForwardRequest,
    ) -> NodePortForwardFuture<'_, Box<dyn NodePortForwardSession>> {
        Box::pin(async {
            Err(NodePortForwardSetupError::unavailable(
                "unused test dependency",
            ))
        })
    }
}

fn streaming_dependencies() -> k8s_native_service::StreamingDependencies {
    let unavailable = Arc::new(UnavailableStreaming);
    k8s_native_service::StreamingDependencies::new(
        unavailable.clone(),
        None,
        None,
        unavailable,
        Arc::<str>::from("test-node"),
        Arc::new(klights_supervisor::TaskSupervisor::new(Default::default())),
    )
}

impl FakeResources {
    fn new(resource: Resource) -> Self {
        Self {
            resource,
            captures: Mutex::new(Captures::default()),
        }
    }
}

impl LeaderResourceQuery for FakeResources {
    fn get_resource(
        &self,
        _request: ResourceGetRequest,
    ) -> ResourceQueryFuture<'_, Option<Resource>> {
        Box::pin(async move { Ok(Some(self.resource.clone())) })
    }

    fn list_resources(
        &self,
        _request: ResourceListRequest,
    ) -> ResourceQueryFuture<'_, ResourceListResult> {
        Box::pin(async { panic!("exact list must use the snapshot port") })
    }
}

impl GenericReadSnapshotPort for FakeResources {
    fn snapshot_resources_at_rv(
        &self,
        request: GenericReadSnapshotRequest,
    ) -> GenericReadFuture<'_, GenericReadSnapshot> {
        let resource = self.resource.clone();
        Box::pin(async move {
            assert_eq!(request.resource_version, 7);
            Ok(GenericReadSnapshot::List(
                ResourceListResult::try_new(
                    vec![resource],
                    7,
                    None,
                    Some("next-item".to_string()),
                    Some(2),
                )
                .unwrap(),
            ))
        })
    }
}

impl ListResourceVersionPort for FakeResources {
    fn advance_after(&self, _minimum_resource_version: i64) -> ListResourceVersionFuture<'_> {
        Box::pin(async { panic!("exact list must not allocate a fresh resourceVersion") })
    }
}

impl GenericReadResourceInputs for FakeResources {
    fn resource_query(&self) -> &dyn LeaderResourceQuery {
        self
    }

    fn snapshot_port(&self) -> &dyn GenericReadSnapshotPort {
        self
    }

    fn resource_versions(&self) -> &dyn ListResourceVersionPort {
        self
    }

    fn prepare_resource_for_read(
        &self,
        _api_version: &'static str,
        _kind: &'static str,
        resource: Resource,
        _is_get: bool,
    ) -> GenericReadFuture<'_, Value> {
        Box::pin(async move {
            let mut value = Arc::unwrap_or_clone(resource.data);
            value["metadata"]["resourceVersion"] =
                Value::String(resource.resource_version.to_string());
            Ok(value)
        })
    }

    fn build_watch(&self, _request: GenericReadWatchRequest) -> GenericReadFuture<'_, Response> {
        Box::pin(async { panic!("plain list must not enter the watch adapter") })
    }

    fn render_list(
        &self,
        response: GenericListResponse,
    ) -> Result<Response, k8s_native_service::AppError> {
        self.captures.lock().unwrap().list = Some(response);
        Ok(Response::new(Body::empty()))
    }

    fn render_get(&self, value: Value, headers: HeaderMap) -> Response {
        self.captures.lock().unwrap().get = Some((value, headers));
        Response::new(Body::empty())
    }
}

struct FakeControllers;

impl GenericReadControllerInputs for FakeControllers {
    fn observed_node_renew_time(&self, _node_name: &str) -> GenericReadFuture<'_, Option<String>> {
        Box::pin(async { Ok(None) })
    }
}

#[derive(Clone)]
struct FakeOperational;

impl GenericReadOperationalInputs for FakeOperational {
    fn operation_unix_timestamp_nanos(&self) -> i128 {
        1_700_000_000_000_000_000
    }

    fn wall_clock(&self) -> Arc<dyn klights_auth::clock::Clock> {
        Arc::new(klights_auth::clock::SystemClock)
    }

    fn has_local_authority(&self) -> bool {
        false
    }
}

fn resource() -> Resource {
    let mut resource = Resource::try_from_data(Arc::new(json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "settings", "namespace": "default"},
        "data": {"key": "value"}
    })))
    .unwrap();
    resource.resource_version = 7;
    resource
}

fn state() -> Arc<ApiState<(), FakeResources, (), FakeControllers, (), FakeOperational>> {
    Arc::new(ApiState::new(
        (),
        FakeResources::new(resource()),
        (),
        FakeControllers,
        (),
        FakeOperational,
        streaming_dependencies(),
    ))
}

#[tokio::test]
async fn exact_list_preserves_snapshot_metadata_and_accept_header() {
    let state = state();
    let mut headers = HeaderMap::new();
    headers.insert(
        "accept",
        HeaderValue::from_static("application/vnd.kubernetes.protobuf"),
    );
    list_inner(
        state.clone(),
        &klights_auth::AuthenticatedIdentity::anonymous(),
        GeneratedListInnerRequest {
            api_version: "v1",
            kind: "ConfigMap",
            list_kind: "ConfigMapList",
            namespace: Some("default".to_string()),
            namespaced: true,
            query: ListQuery {
                label_selector: None,
                field_selector: None,
                limit: Some(1),
                continue_token: None,
                watch: None,
                resource_version: Some("7".to_string()),
                resource_version_match: Some("Exact".to_string()),
                allow_watch_bookmarks: None,
                send_initial_events: None,
                timeout_seconds: None,
            },
            headers,
        },
    )
    .await
    .unwrap();

    let mut captures = state.resource_mutation().captures.lock().unwrap();
    let captured = captures.list.take().unwrap();
    assert_eq!(captured.response_rv, 7);
    assert_eq!(captured.remaining_item_count, Some(2));
    assert_eq!(captured.items[0]["metadata"]["resourceVersion"], "7");
    assert_eq!(
        captured.headers.get("accept").unwrap(),
        "application/vnd.kubernetes.protobuf"
    );
    let (_, continuation) =
        process_continue_token_at(captured.continue_token, 1_700_000_001).unwrap();
    assert_eq!(continuation, ContinueResourceVersion::Session(7));
}

#[tokio::test]
async fn get_preserves_resource_version_and_json_accept_header() {
    let state = state();
    let mut headers = HeaderMap::new();
    headers.insert("accept", HeaderValue::from_static("application/json"));
    get_inner(
        state.clone(),
        &klights_auth::AuthenticatedIdentity::anonymous(),
        "v1",
        "ConfigMap",
        Some("default"),
        "settings",
        headers,
    )
    .await
    .unwrap();

    let mut captures = state.resource_mutation().captures.lock().unwrap();
    let (value, headers) = captures.get.take().unwrap();
    assert_eq!(value["metadata"]["resourceVersion"], "7");
    assert_eq!(headers.get("accept").unwrap(), "application/json");
}
