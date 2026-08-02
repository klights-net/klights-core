//! Kubernetes streaming and backend-proxy HTTP adaptation.

use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query, RawQuery, Request, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use klights_node_api::{NodeExec, NodePortForward};
use klights_pod_api::{PodGetRequest, PodListRequest, PodQuery};
use klights_supervisor::TaskSupervisor;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    ApiState, AppError,
    admission::AdmissionRequestContext,
    generic_command::{
        GenericCommandAdmission, GenericCommandStore, ResourceAdmissionPort,
        ResourceAdmissionRequest,
    },
};

mod backend_proxy_headers;
mod exec;
pub(crate) mod exec_spdy;
mod exec_ws;
mod node_proxy;
mod portforward;
mod proxy;
pub(crate) mod spdy_framing;

/// Complete focused capabilities used by Kubernetes streaming routes.
#[derive(Clone)]
pub struct StreamingDependencies {
    pod_query: Arc<dyn PodQuery>,
    local_node_exec: Option<Arc<dyn NodeExec>>,
    remote_node_exec: Option<Arc<dyn NodeExec>>,
    node_port_forward: Arc<dyn NodePortForward>,
    local_node_name: Arc<str>,
    task_supervisor: Arc<TaskSupervisor>,
}

impl StreamingDependencies {
    pub fn new(
        pod_query: Arc<dyn PodQuery>,
        local_node_exec: Option<Arc<dyn NodeExec>>,
        remote_node_exec: Option<Arc<dyn NodeExec>>,
        node_port_forward: Arc<dyn NodePortForward>,
        local_node_name: Arc<str>,
        task_supervisor: Arc<TaskSupervisor>,
    ) -> Self {
        Self {
            pod_query,
            local_node_exec,
            remote_node_exec,
            node_port_forward,
            local_node_name,
            task_supervisor,
        }
    }
}

/// Focused Axum state access for the native streaming route family.
pub trait StreamingState: Send + Sync {
    fn streaming_dependencies(&self) -> &StreamingDependencies;
    fn streaming_resource_query(&self) -> &dyn klights_leader_api::LeaderResourceQuery;
    fn streaming_admission(&self) -> &dyn ResourceAdmissionPort;
}

impl<Auth, Resources, Discovery, Controllers, PodNode, Operational> StreamingState
    for ApiState<Auth, Resources, Discovery, Controllers, PodNode, Operational>
where
    Auth: Send + Sync,
    Resources: GenericCommandStore + GenericCommandAdmission,
    Discovery: Send + Sync,
    Controllers: Send + Sync,
    PodNode: Send + Sync,
    Operational: Send + Sync,
{
    fn streaming_dependencies(&self) -> &StreamingDependencies {
        self.streaming()
    }

    fn streaming_resource_query(&self) -> &dyn klights_leader_api::LeaderResourceQuery {
        self.resource_mutation().resource_query()
    }

    fn streaming_admission(&self) -> &dyn ResourceAdmissionPort {
        self.resource_mutation().admission()
    }
}

impl<S: StreamingState + ?Sized> StreamingState for Arc<S> {
    fn streaming_dependencies(&self) -> &StreamingDependencies {
        self.as_ref().streaming_dependencies()
    }

    fn streaming_resource_query(&self) -> &dyn klights_leader_api::LeaderResourceQuery {
        self.as_ref().streaming_resource_query()
    }

    fn streaming_admission(&self) -> &dyn ResourceAdmissionPort {
        self.as_ref().streaming_admission()
    }
}

struct AdmissionContextRequest<'a> {
    api_version: &'a str,
    kind: &'a str,
    operation: &'a str,
    namespace: Option<String>,
    name: Option<String>,
    object: Value,
    old_object: Option<Value>,
    dry_run: bool,
    subresource: Option<&'a str>,
    options: Option<Value>,
}

fn build_admission_context(request: AdmissionContextRequest<'_>) -> AdmissionRequestContext {
    let AdmissionContextRequest {
        api_version,
        kind,
        operation,
        namespace,
        name,
        object,
        old_object,
        dry_run,
        subresource,
        options,
    } = request;
    let mut context = AdmissionRequestContext::from_legacy(&object, api_version, kind, operation);
    if object.is_null() {
        let (group, version) = api_version.split_once('/').map_or_else(
            || (String::new(), api_version.to_string()),
            |(group, version)| (group.to_string(), version.to_string()),
        );
        context.api_version = api_version.to_string();
        context.api_group = group;
        context.version = version;
        context.kind = kind.to_string();
        context.resource = kind.to_ascii_lowercase() + "s";
        context.object = Value::Null;
    }
    context.operation = operation.to_string();
    context.namespace = namespace;
    context.name = name;
    context.dry_run = Some(dry_run);
    context.old_object = old_object;
    context.subresource = subresource.map(str::to_string);
    context.options = options;
    context
}

async fn run_admission_for_request(
    admission: &(impl ResourceAdmissionPort + ?Sized),
    context: AdmissionRequestContext,
) -> Result<Value, AppError> {
    admission
        .admit(ResourceAdmissionRequest {
            api_version: context.api_version,
            kind: context.kind,
            resource: Some(context.resource),
            operation: context.operation,
            namespace: context.namespace,
            name: context.name,
            object: context.object,
            old_object: context.old_object,
            dry_run: context.dry_run.unwrap_or(false),
            subresource: context.subresource,
            options: context.options,
        })
        .await
}

async fn get_pod(
    query: &dyn PodQuery,
    namespace: &str,
    name: &str,
) -> Result<Option<klights_cluster_core::Resource>, AppError> {
    Ok(query
        .get_pod(PodGetRequest::try_by_name(namespace, name)?)
        .await?)
}

pub use exec::{
    ExecStreamOptions, ExecTarget, derive_websocket_accept_key, exec_exit_status,
    extract_container_id, format_websocket_error_payload, negotiate_websocket_subprotocol,
    parse_attach_query, parse_exec_query, pod_attach, pod_exec, remote_pod_node_name,
    websocket_uses_structured_status_channel,
};
pub use exec_ws::{
    RemoteExecWebSocketRequest, RemoteExecWebSocketSyncRequest, handle_remote_exec_websocket_sync,
    handle_remote_exec_websocket_tungstenite, remote_exec_error_frame_is_terminal,
};
pub use node_proxy::{node_proxy, node_proxy_with_path};
pub use portforward::pod_portforward;
pub use proxy::{
    MAX_APISERVICE_RESPONSE_BODY_BYTES, MAX_PROXY_REQUEST_BODY_BYTES,
    MAX_PROXY_RESPONSE_BODY_BYTES, ProxyQuery, parse_proxy_name_port, pod_proxy, pod_proxy_inner,
    pod_proxy_with_path, proxy_request_with_fallback_port_and_retries, read_reqwest_body_limited,
    rewrite_proxy_response_body, send_proxy_request_https, service_proxy, service_proxy_inner,
    service_proxy_with_path, should_allow_pod_proxy_default_port_fallback,
};

#[cfg(any(test, feature = "test-support"))]
pub use proxy::{proxy_request, proxy_request_with_fallback, proxy_request_with_fallback_port};

#[cfg(any(test, feature = "test-support"))]
pub mod test_support {
    use std::sync::Arc;

    use klights_node_api::{
        ExecSetupError, NodeExec, NodeExecFuture, NodeExecRequest, NodeExecSession,
        NodeExecSyncRequest, NodeExecSyncResult, NodePortForward, NodePortForwardFuture,
        NodePortForwardRequest, NodePortForwardSession, NodePortForwardSetupError,
    };
    use klights_pod_api::{
        PodGetRequest, PodListRequest, PodListResult, PodOwnerListRequest, PodQuery,
        PodRepositoryError, PodRepositoryFuture,
    };

    pub use super::exec_spdy::*;
    pub use super::spdy_framing::*;

    pub(super) struct UnavailableStreaming;

    impl PodQuery for UnavailableStreaming {
        fn get_pod(
            &self,
            _request: PodGetRequest,
        ) -> PodRepositoryFuture<'_, Option<klights_cluster_core::Resource>> {
            Box::pin(async { Err(PodRepositoryError::unavailable("test streaming dependency")) })
        }

        fn list_pods(&self, _request: PodListRequest) -> PodRepositoryFuture<'_, PodListResult> {
            Box::pin(async { Err(PodRepositoryError::unavailable("test streaming dependency")) })
        }

        fn list_pods_by_owner_uid(
            &self,
            _request: PodOwnerListRequest,
        ) -> PodRepositoryFuture<'_, Vec<klights_cluster_core::Resource>> {
            Box::pin(async { Err(PodRepositoryError::unavailable("test streaming dependency")) })
        }
    }

    impl NodeExec for UnavailableStreaming {
        fn exec_sync(
            &self,
            _request: NodeExecSyncRequest,
        ) -> NodeExecFuture<'_, NodeExecSyncResult> {
            Box::pin(async { Err(ExecSetupError::unavailable("test streaming dependency")) })
        }

        fn open_exec(
            &self,
            _request: NodeExecRequest,
        ) -> NodeExecFuture<'_, Box<dyn NodeExecSession>> {
            Box::pin(async { Err(ExecSetupError::unavailable("test streaming dependency")) })
        }
    }

    impl NodePortForward for UnavailableStreaming {
        fn open_port_forward(
            &self,
            _request: NodePortForwardRequest,
        ) -> NodePortForwardFuture<'_, Box<dyn NodePortForwardSession>> {
            Box::pin(async {
                Err(NodePortForwardSetupError::unavailable(
                    "test streaming dependency",
                ))
            })
        }
    }

    pub fn unavailable_dependencies(
        task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> super::StreamingDependencies {
        let unavailable = Arc::new(UnavailableStreaming);
        super::StreamingDependencies::new(
            unavailable.clone(),
            None,
            None,
            unavailable,
            Arc::<str>::from("test-node"),
            task_supervisor,
        )
    }
}
