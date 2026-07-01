use crate::api::*;
use axum::{
    Json, Router,
    extract::{Query, State},
};
use serde_json::Value;
use std::sync::Arc;

pub fn node_k8s_io_v1_routes() -> Router<Arc<AppState>> {
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
    State(state): State<Arc<AppState>>,
    Query(query): Query<DeleteCollectionQuery>,
) -> Result<Json<Value>, AppError> {
    let list = state
        .db
        .list_resources(
            "node.k8s.io/v1",
            "RuntimeClass",
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
                "node.k8s.io/v1",
                "RuntimeClass",
                None,
                &resource.name.clone(),
            )
            .await;
    }
    Ok(Json(
        crate::api::mutation::response::delete_collection_success_status(),
    ))
}
