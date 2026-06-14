use crate::api::*;
use axum::{
    Json,
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
};
use serde_json::Value;
use std::sync::Arc;

pub fn apps_v1_routes() -> Router<Arc<AppState>> {
    Router::new()
        // Cluster-wide list (all namespaces)
        .route("/deployments", get(list_all_deployments))
        .route("/replicasets", get(list_all_replicasets))
        .route("/statefulsets", get(list_all_statefulsets))
        .route("/daemonsets", get(list_all_daemonsets))
        .route("/controllerrevisions", get(list_all_controllerrevisions))
        // Namespaced resources
        .route(
            "/namespaces/{namespace}/deployments",
            get(list_deployments)
                .post(create_deployment)
                .delete(delete_collection_deployments),
        )
        .route(
            "/namespaces/{namespace}/deployments/{name}",
            get(get_deployment)
                .put(update_deployment)
                .patch(patch_deployment)
                .delete(delete_deployment),
        )
        .route(
            "/namespaces/{namespace}/deployments/{name}/status",
            get(get_deployment)
                .put(update_deployment_status)
                .patch(patch_deployment_status),
        )
        .route(
            "/namespaces/{namespace}/deployments/{name}/scale",
            get(get_deployment_scale)
                .put(update_deployment_scale)
                .patch(patch_deployment_scale),
        )
        .route(
            "/namespaces/{namespace}/replicasets",
            get(list_replicasets)
                .post(create_replicaset)
                .delete(delete_collection_replicasets),
        )
        .route(
            "/namespaces/{namespace}/replicasets/{name}",
            get(get_replicaset)
                .put(update_replicaset)
                .patch(patch_replicaset)
                .delete(delete_replicaset),
        )
        .route(
            "/namespaces/{namespace}/replicasets/{name}/status",
            get(get_replicaset)
                .put(update_replicaset_status)
                .patch(patch_replicaset_status),
        )
        .route(
            "/namespaces/{namespace}/replicasets/{name}/scale",
            get(get_replicaset_scale)
                .put(update_replicaset_scale)
                .patch(patch_replicaset_scale),
        )
        .route(
            "/namespaces/{namespace}/statefulsets",
            get(list_statefulsets)
                .post(create_statefulset)
                .delete(delete_collection_statefulsets),
        )
        .route(
            "/namespaces/{namespace}/statefulsets/{name}",
            get(get_statefulset)
                .put(update_statefulset)
                .patch(patch_statefulset)
                .delete(delete_statefulset),
        )
        .route(
            "/namespaces/{namespace}/statefulsets/{name}/status",
            get(get_statefulset)
                .put(update_statefulset_status)
                .patch(patch_statefulset_status),
        )
        .route(
            "/namespaces/{namespace}/statefulsets/{name}/scale",
            get(get_statefulset_scale)
                .put(update_statefulset_scale)
                .patch(patch_statefulset_scale),
        )
        .route(
            "/namespaces/{namespace}/daemonsets",
            get(list_daemonsets)
                .post(create_daemonset)
                .delete(delete_collection_daemonsets),
        )
        .route(
            "/namespaces/{namespace}/daemonsets/{name}",
            get(get_daemonset)
                .put(update_daemonset)
                .patch(patch_daemonset)
                .delete(delete_daemonset),
        )
        .route(
            "/namespaces/{namespace}/daemonsets/{name}/status",
            get(get_daemonset)
                .put(update_daemonset_status)
                .patch(patch_daemonset_status),
        )
        .route(
            "/namespaces/{namespace}/controllerrevisions",
            get(list_controllerrevisions)
                .post(create_controllerrevision)
                .delete(delete_collection_controllerrevisions),
        )
        .route(
            "/namespaces/{namespace}/controllerrevisions/{name}",
            get(get_controllerrevision)
                .put(update_controllerrevision)
                .patch(patch_controllerrevision)
                .delete(delete_controllerrevision),
        )
}

async fn create_deployment(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
    Query(query): Query<CreateUpdateQuery>,
    axum::Extension(identity): axum::Extension<crate::auth::AuthenticatedIdentity>,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let is_protobuf = body.len() >= 4 && &body[..4] == b"k8s\x00";
    if !is_protobuf {
        check_deployment_strict_decode_from_raw_json(&query, &body)?;
    }

    let parsed = parse_lenient_value_from_bytes(&body)?;
    let result = create_deployment_base(
        State(state.clone()),
        Path(namespace.clone()),
        Query(query),
        axum::Extension(identity),
        LenientJson(parsed),
    )
    .await?;

    let (status, json_response) = &result;
    if *status == StatusCode::CREATED {
        state.controller_dispatcher.enqueue(&json_response.0).await;
    }

    Ok(result)
}

async fn update_deployment(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<CreateUpdateQuery>,
    axum::Extension(identity): axum::Extension<crate::auth::AuthenticatedIdentity>,
    LenientJson(body): LenientJson<Value>,
) -> Result<Json<Value>, AppError> {
    let result = update_deployment_base(
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

async fn patch_deployment(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<CreateUpdateQuery>,
    headers: HeaderMap,
    axum::Extension(identity): axum::Extension<crate::auth::AuthenticatedIdentity>,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    let result = patch_deployment_base(
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
