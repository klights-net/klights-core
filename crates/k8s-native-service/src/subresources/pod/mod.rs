use std::sync::Arc;

use axum::{
    Json,
    body::Bytes,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::Value;

use crate::generic_command::{GenericCommandState, ResourceAdmissionRequest};
use crate::{AppError, LenientJson, inject_resource_version};

async fn get_pod<S: GenericCommandState + ?Sized>(
    state: &S,
    namespace: &str,
    name: &str,
) -> Result<Option<klights_cluster_core::Resource>, AppError> {
    crate::generic_read::get_resource(
        state.command_store().resource_query(),
        "v1",
        "Pod",
        Some(namespace),
        name,
    )
    .await
}

pub(crate) mod binding;
pub(crate) mod ephemeral;
pub(crate) mod eviction;
pub(crate) mod log_transport;
pub mod logs;
pub(crate) mod status;

pub use binding::{BindingQuery, pod_binding};
pub use ephemeral::{
    get_pod_ephemeral_containers, patch_pod_ephemeral_containers, update_pod_ephemeral_containers,
};
pub use eviction::pod_eviction;
pub use status::{get_pod_status, patch_pod_status_subresource, update_pod_status_subresource};
