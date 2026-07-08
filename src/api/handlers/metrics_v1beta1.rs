use crate::api::*;
use crate::datastore::ResourceList;
use crate::metrics::{
    METRICS_API_VERSION, MetricsObjectBuilder, MetricsSnapshot, PodMetric, RuntimeMetricsSnapshot,
};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
};
use serde_json::{Value, json};
use std::sync::Arc;

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
    let (pod_list, runtime) = all_pods_and_runtime_snapshot(&state).await?;
    let snapshot = MetricsSnapshot::from_pods(pod_list.items.iter(), &runtime);
    let builder = MetricsObjectBuilder::new(crate::utils::k8s_timestamp());
    let items: Vec<Value> = list
        .items
        .iter()
        .map(|node| builder.node_metrics_object(&node.name, snapshot.node_usage(&node.name)))
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
    let field_selector = format!("spec.nodeName={}", node.name);
    let pod_list = crate::kubelet::pod_repository::PodReader::list_pods(
        state.pod_repository.as_ref(),
        None,
        None,
        Some(&field_selector),
        None,
        None,
    )
    .await
    .map_err(AppError::from)?;
    let runtime = state
        .metrics_provider
        .runtime_snapshot_for_pods(&pod_list.items)
        .await;
    let snapshot = MetricsSnapshot::from_pods(pod_list.items.iter(), &runtime);
    let builder = MetricsObjectBuilder::new(crate::utils::k8s_timestamp());
    Ok(Json(builder.node_metrics_object(
        &node.name,
        snapshot.node_usage(&node.name),
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
    let runtime = state
        .metrics_provider
        .runtime_snapshot_for_pods(&list.items)
        .await;
    let builder = MetricsObjectBuilder::new(crate::utils::k8s_timestamp());
    let items: Vec<Value> = list
        .items
        .iter()
        .map(|pod| {
            let metric = PodMetric::from_resource(pod, &runtime);
            builder.pod_metrics_object(&metric)
        })
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
    let runtime = state
        .metrics_provider
        .runtime_snapshot_for_pods(std::slice::from_ref(&pod))
        .await;
    let builder = MetricsObjectBuilder::new(crate::utils::k8s_timestamp());
    let metric = PodMetric::from_resource(&pod, &runtime);
    Ok(Json(builder.pod_metrics_object(&metric)))
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

async fn all_pods_and_runtime_snapshot(
    state: &Arc<AppState>,
) -> Result<(ResourceList, RuntimeMetricsSnapshot), AppError> {
    let pod_list = crate::kubelet::pod_repository::PodReader::list_pods(
        state.pod_repository.as_ref(),
        None,
        None,
        None,
        None,
        None,
    )
    .await
    .map_err(AppError::from)?;
    let runtime = state
        .metrics_provider
        .runtime_snapshot_for_pods(&pod_list.items)
        .await;
    Ok((pod_list, runtime))
}
