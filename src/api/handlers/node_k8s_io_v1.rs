use crate::api::*;
use axum::{
    Json, Router,
    extract::{Query, State},
};
use serde_json::Value;
use std::sync::Arc;

pub fn node_k8s_io_v1_routes() -> Router<Arc<ApiState>> {
    Router::new()
        .route(
            "/runtimeclasses",
            get(list_runtimeclasses)
                .post(create_runtimeclass)
                .delete(delete_collection_runtimeclasses),
        )
        .route(
            "/runtimeclasses/{name}",
            get(get_runtimeclass)
                .put(update_runtimeclass)
                .patch(patch_runtimeclass)
                .delete(delete_runtimeclass),
        )
}

async fn delete_collection_runtimeclasses(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<DeleteCollectionQuery>,
) -> Result<Json<Value>, AppError> {
    crate::api::resource_command_ports::delete_non_pod_collection(
        state.resource_mutation().resource_query.as_ref(),
        state.resource_mutation().resource_command.as_ref(),
        "node.k8s.io/v1",
        "RuntimeClass",
        None,
        query.label_selector.as_deref(),
    )
    .await?;
    Ok(Json(
        crate::api::mutation::response::delete_collection_success_status(),
    ))
}
