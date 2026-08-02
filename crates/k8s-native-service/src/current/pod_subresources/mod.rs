use axum::{
    extract::{Path, Query, Request, State},
    response::Response,
};
use std::sync::Arc;

use crate::current::{ApiState, AppError};

// Authorization for all pod subresources is enforced by the global
// `authorize_request` middleware chokepoint in `crate::auth_http`;
// handlers no longer authorize individually.

pub use crate::streaming::{MAX_APISERVICE_RESPONSE_BODY_BYTES, read_reqwest_body_limited};
pub(in crate::current) use crate::streaming::{
    node_proxy, node_proxy_with_path, pod_attach, pod_exec, pod_portforward, pod_proxy,
    pod_proxy_with_path, service_proxy, service_proxy_with_path,
};
pub(in crate::current) use crate::subresources::pod::{
    get_pod_ephemeral_containers, get_pod_status, patch_pod_ephemeral_containers,
    patch_pod_status_subresource, pod_binding, pod_eviction, update_pod_ephemeral_containers,
    update_pod_status_subresource,
};

pub(in crate::current) async fn get_pod_log(
    State(state): State<Arc<ApiState>>,
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<crate::subresources::pod::logs::PodLogQuery>,
    request: Request,
) -> Result<Response, AppError> {
    let pod = crate::generic_read::get_resource(
        state.resource_mutation().resource_query.as_ref(),
        "v1",
        "Pod",
        Some(&namespace),
        &name,
    )
    .await?
    .map(|resource| resource.data);

    crate::subresources::pod::logs::get_pod_log(
        state.pod_node_subresources().pod_logs.as_ref(),
        state
            .operational()
            .replication
            .as_ref()
            .map(|services| services.logs.clone()),
        crate::subresources::pod::logs::PodLogRouteRequest {
            namespace,
            name,
            pod,
            query,
            request,
        },
    )
    .await
}

#[cfg(test)]
pub(crate) use crate::streaming::{exec_spdy, spdy_framing};

#[cfg(test)]
mod native_tests;

#[cfg(test)]
mod focused_streaming_tests;

#[cfg(test)]
mod focused_policy_tests;
