use crate::api::*;
use axum::{
    Json,
    extract::{Query, State},
};
use serde_json::Value;
use std::sync::Arc;

pub async fn delete_collection_flowschemas(
    State(state): State<Arc<AppState>>,
    Query(query): Query<DeleteCollectionQuery>,
) -> Result<Json<Value>, AppError> {
    let list = state
        .db
        .list_resources(
            "flowcontrol.apiserver.k8s.io/v1",
            "FlowSchema",
            None,
            crate::datastore::ResourceListQuery::new(
                query.label_selector.as_deref(),
                None,
                None,
                None,
            ),
        )
        .await?;
    for resource in list.items {
        let _ = state
            .db
            .delete_resource(
                "flowcontrol.apiserver.k8s.io/v1",
                "FlowSchema",
                None,
                &resource.name.clone(),
            )
            .await;
    }
    Ok(Json(
        crate::api::mutation::response::delete_collection_success_status(),
    ))
}

pub async fn delete_collection_prioritylevelconfigurations(
    State(state): State<Arc<AppState>>,
    Query(query): Query<DeleteCollectionQuery>,
) -> Result<Json<Value>, AppError> {
    let list = state
        .db
        .list_resources(
            "flowcontrol.apiserver.k8s.io/v1",
            "PriorityLevelConfiguration",
            None,
            crate::datastore::ResourceListQuery::new(
                query.label_selector.as_deref(),
                None,
                None,
                None,
            ),
        )
        .await?;
    for resource in list.items {
        let _ = state
            .db
            .delete_resource(
                "flowcontrol.apiserver.k8s.io/v1",
                "PriorityLevelConfiguration",
                None,
                &resource.name.clone(),
            )
            .await;
    }
    Ok(Json(
        crate::api::mutation::response::delete_collection_success_status(),
    ))
}
