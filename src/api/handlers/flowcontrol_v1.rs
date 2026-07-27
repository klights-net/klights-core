use crate::api::*;
use axum::{
    Json,
    extract::{Query, State},
};
use serde_json::Value;
use std::sync::Arc;

pub(in crate::api) async fn delete_collection_flowschemas(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<DeleteCollectionQuery>,
) -> Result<Json<Value>, AppError> {
    crate::api::resource_command_ports::delete_non_pod_collection(
        state.resource_mutation().resource_query.as_ref(),
        state.resource_mutation().resource_command.as_ref(),
        "flowcontrol.apiserver.k8s.io/v1",
        "FlowSchema",
        None,
        query.label_selector.as_deref(),
    )
    .await?;
    Ok(Json(
        crate::api::mutation::response::delete_collection_success_status(),
    ))
}

pub(in crate::api) async fn delete_collection_prioritylevelconfigurations(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<DeleteCollectionQuery>,
) -> Result<Json<Value>, AppError> {
    crate::api::resource_command_ports::delete_non_pod_collection(
        state.resource_mutation().resource_query.as_ref(),
        state.resource_mutation().resource_command.as_ref(),
        "flowcontrol.apiserver.k8s.io/v1",
        "PriorityLevelConfiguration",
        None,
        query.label_selector.as_deref(),
    )
    .await?;
    Ok(Json(
        crate::api::mutation::response::delete_collection_success_status(),
    ))
}
