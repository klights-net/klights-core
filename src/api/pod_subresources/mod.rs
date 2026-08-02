use axum::{
    extract::{Path, Query, Request, State},
    response::Response,
};
use std::sync::Arc;

use crate::api::{ApiState, AppError};

// Authorization for all pod subresources is enforced by the global
// `authorize_request` middleware chokepoint (see src/api/auth_middleware.rs);
// handlers no longer authorize individually.

#[cfg(test)]
mod tests;

#[cfg(test)]
pub use k8s_native_service::streaming::{
    ExecTarget, ProxyQuery, RemoteExecWebSocketSyncRequest, derive_websocket_accept_key,
    exec_exit_status, extract_container_id, format_websocket_error_payload,
    handle_remote_exec_websocket_sync, handle_remote_exec_websocket_tungstenite,
    negotiate_websocket_subprotocol, parse_attach_query, parse_exec_query, parse_proxy_name_port,
    pod_proxy_inner, proxy_request, proxy_request_with_fallback, proxy_request_with_fallback_port,
    proxy_request_with_fallback_port_and_retries, remote_exec_error_frame_is_terminal,
    remote_pod_node_name, rewrite_proxy_response_body, send_proxy_request_https,
    service_proxy_inner, should_allow_pod_proxy_default_port_fallback,
    websocket_uses_structured_status_channel,
};
pub use k8s_native_service::streaming::{
    MAX_APISERVICE_RESPONSE_BODY_BYTES, MAX_PROXY_REQUEST_BODY_BYTES,
    MAX_PROXY_RESPONSE_BODY_BYTES, read_reqwest_body_limited,
};
pub(in crate::api) use k8s_native_service::streaming::{
    node_proxy, node_proxy_with_path, pod_attach, pod_exec, pod_portforward, pod_proxy,
    pod_proxy_with_path, service_proxy, service_proxy_with_path,
};
#[cfg(test)]
mod exec_spdy {
    pub(crate) use k8s_native_service::streaming::test_support::*;
}
#[cfg(test)]
mod spdy_framing {
    pub(crate) use k8s_native_service::streaming::test_support::*;
}
pub(in crate::api) use k8s_native_service::subresources::pod::{
    get_pod_ephemeral_containers, get_pod_status, patch_pod_ephemeral_containers,
    patch_pod_status_subresource, pod_binding, pod_eviction, update_pod_ephemeral_containers,
    update_pod_status_subresource,
};

pub(in crate::api) async fn get_pod_log(
    State(state): State<Arc<ApiState>>,
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<k8s_native_service::subresources::pod::logs::PodLogQuery>,
    request: Request,
) -> Result<Response, AppError> {
    let pod = k8s_native_service::generic_read::get_resource(
        state.resource_mutation().resource_query.as_ref(),
        "v1",
        "Pod",
        Some(&namespace),
        &name,
    )
    .await?
    .map(|resource| resource.data);

    k8s_native_service::subresources::pod::logs::get_pod_log(
        state.pod_node_subresources().pod_logs.as_ref(),
        state
            .operational()
            .replication
            .as_ref()
            .map(|services| services.logs.clone()),
        k8s_native_service::subresources::pod::logs::PodLogRouteRequest {
            namespace,
            name,
            pod,
            query,
            request,
        },
    )
    .await
}
