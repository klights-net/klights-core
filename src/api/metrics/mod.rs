mod model;
mod representation;

pub(in crate::api) use model::PodMetric;
pub(in crate::api) use representation::{METRICS_API_VERSION, MetricsObjectBuilder};

use klights_cluster_core::Resource;
use klights_node_api::{
    NodeMetrics, NodeMetricsRequest, NodeMetricsResult, NodeMetricsSnapshot, NodeMetricsTarget,
};
use serde_json::Value;
use std::collections::BTreeSet;

pub(in crate::api) async fn snapshot_for_resources(
    node_metrics: &dyn NodeMetrics,
    resources: &[Resource],
) -> NodeMetricsSnapshot {
    let results = futures::future::join_all(metric_nodes_for_resources(resources).into_iter().map(
        |node_name| {
            let request = NodeMetricsTarget::try_new(node_name)
                .map(|target| NodeMetricsRequest::new(target, Vec::new()));
            async move {
                match request {
                    Ok(request) => node_metrics.collect_metrics(request).await,
                    Err(error) => Err(error),
                }
            }
        },
    ))
    .await;

    NodeMetricsSnapshot::from_results(results.into_iter().filter_map(
        |result: Result<NodeMetricsResult, klights_node_api::NodeMetricsError>| match result {
            Ok(result) => Some(result),
            Err(error) => {
                tracing::debug!(%error, "node runtime metrics unavailable");
                None
            }
        },
    ))
}

fn metric_nodes_for_resources(resources: &[Resource]) -> Vec<String> {
    resources
        .iter()
        .filter(|resource| !resource_is_terminal(resource.data.as_ref()))
        .filter_map(|resource| {
            resource
                .data
                .pointer("/spec/nodeName")
                .and_then(Value::as_str)
                .filter(|node| !node.is_empty())
                .map(str::to_string)
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn resource_is_terminal(resource: &Value) -> bool {
    matches!(
        resource.pointer("/status/phase").and_then(Value::as_str),
        Some("Succeeded" | "Failed")
    )
}

#[cfg(test)]
mod tests;
