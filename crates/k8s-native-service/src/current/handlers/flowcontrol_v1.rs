use crate::current::*;
use axum::{
    Json,
    extract::{Query, State},
};
use serde_json::Value;
use std::sync::Arc;

pub(in crate::current) async fn delete_collection_flowschemas(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<DeleteCollectionQuery>,
) -> Result<Json<Value>, AppError> {
    crate::generic_command::delete_non_pod_collection(
        state.resource_mutation().resource_query.as_ref(),
        state.resource_mutation().resource_command.as_ref(),
        "flowcontrol.apiserver.k8s.io/v1",
        "FlowSchema",
        klights_leader_api::ResourceListScope::Cluster,
        query.label_selector.as_deref(),
    )
    .await?;
    Ok(Json(
        crate::generic_command::delete_collection_success_status(),
    ))
}

pub(in crate::current) async fn delete_collection_prioritylevelconfigurations(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<DeleteCollectionQuery>,
) -> Result<Json<Value>, AppError> {
    crate::generic_command::delete_non_pod_collection(
        state.resource_mutation().resource_query.as_ref(),
        state.resource_mutation().resource_command.as_ref(),
        "flowcontrol.apiserver.k8s.io/v1",
        "PriorityLevelConfiguration",
        klights_leader_api::ResourceListScope::Cluster,
        query.label_selector.as_deref(),
    )
    .await?;
    Ok(Json(
        crate::generic_command::delete_collection_success_status(),
    ))
}
