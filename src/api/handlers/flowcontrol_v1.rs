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
    k8s_native_service::generic_command::delete_non_pod_collection(
        state.resource_mutation().resource_query.as_ref(),
        state.resource_mutation().resource_command.as_ref(),
        "flowcontrol.apiserver.k8s.io/v1",
        "FlowSchema",
        None,
        query.label_selector.as_deref(),
    )
    .await?;
    Ok(Json(
        k8s_native_service::generic_command::delete_collection_success_status(),
    ))
}

pub(in crate::api) async fn delete_collection_prioritylevelconfigurations(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<DeleteCollectionQuery>,
) -> Result<Json<Value>, AppError> {
    k8s_native_service::generic_command::delete_non_pod_collection(
        state.resource_mutation().resource_query.as_ref(),
        state.resource_mutation().resource_command.as_ref(),
        "flowcontrol.apiserver.k8s.io/v1",
        "PriorityLevelConfiguration",
        None,
        query.label_selector.as_deref(),
    )
    .await?;
    Ok(Json(
        k8s_native_service::generic_command::delete_collection_success_status(),
    ))
}
