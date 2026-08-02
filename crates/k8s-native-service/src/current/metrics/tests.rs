use super::*;
use klights_node_api::{
    NodeMetricsContainerSample, NodeMetricsPodSample, NodeMetricsResult, NodeMetricsTarget,
};
use serde_json::{Value, json};
use std::sync::Arc;

fn pod_resource(body: Value) -> Resource {
    Resource {
        id: 1,
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "pod-a".to_string(),
        uid: "uid-a".to_string(),
        resource_version: 1,
        data: Arc::new(body),
    }
}

#[test]
fn pod_metric_is_unavailable_without_runtime_sample() {
    let pod = pod_resource(json!({
        "metadata": {"name": "pod-a", "namespace": "default", "uid": "uid-a"},
        "spec": {"containers": [{"name": "app"}]}
    }));

    assert!(PodMetric::from_resource(&pod, &NodeMetricsSnapshot::default()).is_none());
}

#[test]
fn pod_metric_renders_transport_neutral_runtime_sample() {
    let snapshot = NodeMetricsSnapshot::from_results([NodeMetricsResult::new(
        NodeMetricsTarget::try_new("node-a").unwrap(),
        None,
        vec![NodeMetricsPodSample::new(
            "default",
            "pod-a",
            "uid-a",
            vec![NodeMetricsContainerSample::new(
                "app",
                123_000_000,
                9 * 1024 * 1024,
            )],
        )],
    )]);
    let pod = pod_resource(json!({
        "metadata": {"name": "pod-a", "namespace": "default", "uid": "uid-a"},
        "spec": {"containers": [{"name": "app"}]}
    }));

    let metric = PodMetric::from_resource(&pod, &snapshot).unwrap();
    let object =
        MetricsObjectBuilder::new("2026-01-01T00:00:00Z".to_string()).pod_metrics_object(&metric);

    assert_eq!(object["containers"][0]["usage"]["cpu"], "123m");
    assert_eq!(object["containers"][0]["usage"]["memory"], "9216Ki");
}
