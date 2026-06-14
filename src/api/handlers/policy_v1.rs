use crate::api::*;
use axum::{
    Json,
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
};
use serde_json::Value;
use std::sync::Arc;

pub fn policy_v1_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/poddisruptionbudgets", get(list_all_poddisruptionbudgets))
        .route(
            "/namespaces/{namespace}/poddisruptionbudgets",
            get(list_poddisruptionbudgets)
                .post(create_poddisruptionbudget)
                .delete(delete_collection_poddisruptionbudgets),
        )
        .route(
            "/namespaces/{namespace}/poddisruptionbudgets/{name}",
            get(get_poddisruptionbudget)
                .put(update_poddisruptionbudget)
                .patch(patch_poddisruptionbudget)
                .delete(delete_poddisruptionbudget),
        )
        .route(
            "/namespaces/{namespace}/poddisruptionbudgets/{name}/status",
            get(get_poddisruptionbudget)
                .put(update_poddisruptionbudget_status)
                .patch(patch_poddisruptionbudget_status),
        )
}

async fn create_poddisruptionbudget(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
    Query(query): Query<CreateUpdateQuery>,
    axum::Extension(identity): axum::Extension<crate::auth::AuthenticatedIdentity>,
    LenientJson(body): LenientJson<Value>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let result = create_poddisruptionbudget_base(
        State(state.clone()),
        Path(namespace.clone()),
        Query(query),
        axum::Extension(identity),
        LenientJson(body),
    )
    .await?;

    state.controller_dispatcher.enqueue(&result.1.0).await;

    Ok(result)
}

async fn update_poddisruptionbudget(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<CreateUpdateQuery>,
    axum::Extension(identity): axum::Extension<crate::auth::AuthenticatedIdentity>,
    LenientJson(body): LenientJson<Value>,
) -> Result<Json<Value>, AppError> {
    let result = update_poddisruptionbudget_base(
        State(state.clone()),
        Path((namespace.clone(), name.clone())),
        Query(query),
        axum::Extension(identity),
        LenientJson(body),
    )
    .await?;

    state.controller_dispatcher.enqueue(&result.0).await;

    Ok(result)
}

async fn patch_poddisruptionbudget(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<CreateUpdateQuery>,
    headers: HeaderMap,
    axum::Extension(identity): axum::Extension<crate::auth::AuthenticatedIdentity>,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    let result = patch_poddisruptionbudget_base(
        State(state.clone()),
        Path((namespace.clone(), name.clone())),
        Query(query),
        headers,
        axum::Extension(identity),
        body,
    )
    .await?;

    state.controller_dispatcher.enqueue(&result.0).await;

    Ok(result)
}
