use crate::api::*;
use crate::metrics::{METRICS_API_VERSION, MetricsObjectBuilder, MetricsSnapshot, PodMetric};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
};
use serde_json::{Value, json};
use std::sync::Arc;

pub fn metrics_v1beta1_routes() -> Router<Arc<ApiState>> {
    Router::new()
        .route("/nodes", get(list_node_metrics))
        .route("/nodes/{name}", get(get_node_metrics))
        .route("/pods", get(list_all_pod_metrics))
        .route("/namespaces/{namespace}/pods", get(list_pod_metrics))
        .route("/namespaces/{namespace}/pods/{name}", get(get_pod_metrics))
}

async fn list_node_metrics(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, AppError> {
    let list = crate::api::resource_query_ports::list_resources(
        state.resource_mutation().resource_query.as_ref(),
        "v1",
        "Node",
        None,
        query.label_selector.as_deref(),
        query.field_selector.as_deref(),
        query.limit,
        query.continue_token.as_deref(),
    )
    .await?;
    let runtime =
        runtime_snapshot_for_nodes(&state, list.items().iter().map(|node| node.name.as_str()))
            .await?;
    let snapshot = MetricsSnapshot::from_runtime_nodes(&runtime);
    let builder = MetricsObjectBuilder::new(crate::k8s_time::now_legacy_timestamp());
    let items: Vec<Value> = list
        .items()
        .iter()
        .filter_map(|node| {
            snapshot
                .available_node_usage(&node.name)
                .map(|usage| builder.node_metrics_object(&node.name, usage))
        })
        .collect();

    Ok(Json(json!({
        "apiVersion": METRICS_API_VERSION,
        "kind": "NodeMetricsList",
        "metadata": list_metadata(
            list.resource_version(),
            list.continue_token().map(str::to_string),
            list.remaining_item_count(),
        ),
        "items": items,
    })))
}

async fn get_node_metrics(
    State(state): State<Arc<ApiState>>,
    Path(name): Path<String>,
) -> Result<Json<Value>, AppError> {
    let node = crate::api::resource_query_ports::get_resource(
        state.resource_mutation().resource_query.as_ref(),
        "v1",
        "Node",
        None,
        &name,
    )
    .await?
    .ok_or_else(|| AppError::not_found(METRICS_API_VERSION, "NodeMetrics", &name))?;
    let node_probe = node_probe_pod(&node.name);
    let runtime = state
        .pod_node_subresources()
        .metrics_provider
        .runtime_snapshot_for_pods(std::slice::from_ref(&node_probe))
        .await;
    let snapshot = MetricsSnapshot::from_runtime_nodes(&runtime);
    let usage = snapshot.available_node_usage(&node.name).ok_or_else(|| {
        AppError::ServiceUnavailable(format!("NodeMetrics \"{}\" is unavailable", node.name))
    })?;
    let builder = MetricsObjectBuilder::new(crate::k8s_time::now_legacy_timestamp());
    Ok(Json(builder.node_metrics_object(&node.name, usage)))
}

async fn list_all_pod_metrics(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, AppError> {
    list_pod_metrics_for_namespace(state, None, query).await
}

async fn list_pod_metrics(
    State(state): State<Arc<ApiState>>,
    Path(namespace): Path<String>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, AppError> {
    list_pod_metrics_for_namespace(state, Some(namespace), query).await
}

async fn list_pod_metrics_for_namespace(
    state: Arc<ApiState>,
    namespace: Option<String>,
    query: ListQuery,
) -> Result<Json<Value>, AppError> {
    let list = crate::api::pod_repository_ports::list_pods(
        state.resource_mutation().pod_repository.as_ref(),
        namespace.as_deref(),
        query.label_selector.as_deref(),
        query.field_selector.as_deref(),
        query.limit,
        query.continue_token.as_deref(),
    )
    .await
    .map_err(AppError::from)?;
    let runtime = state
        .pod_node_subresources()
        .metrics_provider
        .runtime_snapshot_for_pods(&list.items)
        .await;
    let builder = MetricsObjectBuilder::new(crate::k8s_time::now_legacy_timestamp());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|pod| {
            PodMetric::from_resource(pod, &runtime)
                .map(|metric| builder.pod_metrics_object(&metric))
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
    State(state): State<Arc<ApiState>>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    let pod = crate::api::pod_repository_ports::get_pod(
        state.resource_mutation().pod_repository.as_ref(),
        &namespace,
        &name,
    )
    .await
    .map_err(AppError::from)?
    .ok_or_else(|| AppError::not_found(METRICS_API_VERSION, "PodMetrics", &name))?;
    let runtime = state
        .pod_node_subresources()
        .metrics_provider
        .runtime_snapshot_for_pods(std::slice::from_ref(&pod))
        .await;
    let builder = MetricsObjectBuilder::new(crate::k8s_time::now_legacy_timestamp());
    let metric = PodMetric::from_resource(&pod, &runtime).ok_or_else(|| {
        AppError::ServiceUnavailable(format!("PodMetrics \"{}\" is unavailable", pod.name))
    })?;
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

async fn runtime_snapshot_for_nodes<'a>(
    state: &Arc<ApiState>,
    node_names: impl IntoIterator<Item = &'a str>,
) -> Result<crate::metrics::RuntimeMetricsSnapshot, AppError> {
    let probes: Vec<klights_cluster_core::Resource> =
        node_names.into_iter().map(node_probe_pod).collect();
    let runtime = state
        .pod_node_subresources()
        .metrics_provider
        .runtime_snapshot_for_pods(&probes)
        .await;
    Ok(runtime)
}

fn node_probe_pod(node_name: &str) -> klights_cluster_core::Resource {
    klights_cluster_core::Resource {
        id: 0,
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("kube-system".to_string()),
        name: format!("node-metrics-probe-{node_name}"),
        uid: String::new(),
        resource_version: 0,
        data: Arc::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": format!("node-metrics-probe-{node_name}"),
                "namespace": "kube-system"
            },
            "spec": {
                "nodeName": node_name,
                "containers": [{"name": "probe"}]
            }
        })),
    }
}
