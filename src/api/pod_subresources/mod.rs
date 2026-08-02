use axum::{
    Json,
    extract::{Path, Query, RawQuery, Request, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
#[cfg(test)]
use futures::stream::StreamExt;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

use crate::api::{ApiState, AppError, build_admission_context, run_admission_for_request};

// Authorization for all pod subresources is enforced by the global
// `authorize_request` middleware chokepoint (see src/api/auth_middleware.rs);
// handlers no longer authorize individually.

mod exec;
mod exec_spdy;
mod exec_ws;
mod node_proxy;
mod portforward;
mod proxy;
mod spdy_framing;
#[cfg(test)]
mod tests;

pub(in crate::api) use self::exec::*;
pub use self::exec_ws::*;
pub(in crate::api) use self::node_proxy::*;
pub(in crate::api) use self::portforward::*;
pub use self::proxy::MAX_APISERVICE_RESPONSE_BODY_BYTES;
pub use self::proxy::MAX_PROXY_REQUEST_BODY_BYTES;
pub use self::proxy::MAX_PROXY_RESPONSE_BODY_BYTES;
pub(in crate::api) use self::proxy::*;
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
