use crate::api::*;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
};
use serde_json::{Value, json};
use std::sync::Arc;

const METRICS_API_VERSION: &str = "metrics.k8s.io/v1beta1";
const METRICS_WINDOW: &str = "30s";

pub fn metrics_v1beta1_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/nodes", get(list_node_metrics))
        .route("/nodes/{name}", get(get_node_metrics))
        .route("/pods", get(list_all_pod_metrics))
        .route("/namespaces/{namespace}/pods", get(list_pod_metrics))
        .route("/namespaces/{namespace}/pods/{name}", get(get_pod_metrics))
}

async fn list_node_metrics(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, AppError> {
    let list = state
        .db
        .list_resources(
            "v1",
            "Node",
            None,
            crate::datastore::ResourceListQuery::new(
                query.label_selector.as_deref(),
                query.field_selector.as_deref(),
                query.limit,
                query.continue_token.as_deref(),
            ),
        )
        .await?;
    let timestamp = crate::utils::k8s_timestamp();
    let items: Vec<Value> = list
        .items
        .iter()
        .map(|node| node_metrics_object(&node.name, &timestamp))
        .collect();

    Ok(Json(json!({
        "apiVersion": METRICS_API_VERSION,
        "kind": "NodeMetricsList",
        "metadata": list_metadata(list.resource_version, list.continue_token, list.remaining_item_count),
        "items": items,
    })))
}

async fn get_node_metrics(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<Value>, AppError> {
    let node = state
        .db
        .get_resource("v1", "Node", None, &name)
        .await?
        .ok_or_else(|| AppError::not_found(METRICS_API_VERSION, "NodeMetrics", &name))?;
    Ok(Json(node_metrics_object(
        &node.name,
        &crate::utils::k8s_timestamp(),
    )))
}

async fn list_all_pod_metrics(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, AppError> {
    list_pod_metrics_for_namespace(state, None, query).await
}

async fn list_pod_metrics(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, AppError> {
    list_pod_metrics_for_namespace(state, Some(namespace), query).await
}

async fn list_pod_metrics_for_namespace(
    state: Arc<AppState>,
    namespace: Option<String>,
    query: ListQuery,
) -> Result<Json<Value>, AppError> {
    let list = crate::kubelet::pod_repository::PodReader::list_pods(
        state.pod_repository.as_ref(),
        namespace.as_deref(),
        query.label_selector.as_deref(),
        query.field_selector.as_deref(),
        query.limit,
        query.continue_token.as_deref(),
    )
    .await
    .map_err(AppError::from)?;
    let timestamp = crate::utils::k8s_timestamp();
    let items: Vec<Value> = list
        .items
        .iter()
        .map(|pod| pod_metrics_object(&pod.name, pod.namespace.as_deref(), &pod.data, &timestamp))
        .collect();

    Ok(Json(json!({
        "apiVersion": METRICS_API_VERSION,
        "kind": "PodMetricsList",
        "metadata": list_metadata(list.resource_version, list.continue_token, list.remaining_item_count),
        "items": items,
    })))
}

async fn get_pod_metrics(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    let pod = crate::kubelet::pod_repository::PodReader::get_pod(
        state.pod_repository.as_ref(),
        &namespace,
        &name,
    )
    .await
    .map_err(AppError::from)?
    .ok_or_else(|| AppError::not_found(METRICS_API_VERSION, "PodMetrics", &name))?;
    Ok(Json(pod_metrics_object(
        &pod.name,
        pod.namespace.as_deref(),
        &pod.data,
        &crate::utils::k8s_timestamp(),
    )))
}

fn list_metadata(
    resource_version: i64,
    continue_token: Option<String>,
    remaining_item_count: Option<i64>,
) -> Value {
    let mut metadata = json!({
        "resourceVersion": resource_version.to_string(),
    });
    if let Some(token) = continue_token {
        metadata["continue"] = Value::String(token);
    }
    if let Some(count) = remaining_item_count {
        metadata["remainingItemCount"] = json!(count);
    }
    metadata
}

fn node_metrics_object(name: &str, timestamp: &str) -> Value {
    json!({
        "apiVersion": METRICS_API_VERSION,
        "kind": "NodeMetrics",
        "metadata": {"name": name},
        "timestamp": timestamp,
        "window": METRICS_WINDOW,
        "usage": zero_usage(),
    })
}

fn pod_metrics_object(name: &str, namespace: Option<&str>, pod: &Value, timestamp: &str) -> Value {
    let containers: Vec<Value> = pod
        .pointer("/spec/containers")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|container| container.get("name").and_then(Value::as_str))
        .map(|name| {
            json!({
                "name": name,
                "usage": zero_usage(),
            })
        })
        .collect();

    json!({
        "apiVersion": METRICS_API_VERSION,
        "kind": "PodMetrics",
        "metadata": {
            "name": name,
            "namespace": namespace.unwrap_or_default(),
        },
        "timestamp": timestamp,
        "window": METRICS_WINDOW,
        "containers": containers,
    })
}

fn zero_usage() -> Value {
    json!({
        "cpu": "0",
        "memory": "0",
    })
}
