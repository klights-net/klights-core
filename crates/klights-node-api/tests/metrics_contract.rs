use klights_node_api::{
    NodeMetrics, NodeMetricsContainerSample, NodeMetricsError, NodeMetricsFuture,
    NodeMetricsNodeSample, NodeMetricsPodSample, NodeMetricsRequest, NodeMetricsResult,
    NodeMetricsRuntime, NodeMetricsTarget,
};

struct FakeMetrics;

impl NodeMetrics for FakeMetrics {
    fn collect_metrics(
        &self,
        request: NodeMetricsRequest,
    ) -> NodeMetricsFuture<'_, NodeMetricsResult> {
        Box::pin(async move {
            Ok(NodeMetricsResult::new(
                request.target().clone(),
                Some(NodeMetricsNodeSample::new(42, 4096)),
                vec![NodeMetricsPodSample::new(
                    "default",
                    "web",
                    "pod-uid",
                    vec![NodeMetricsContainerSample::new("main", 7, 1024)],
                )],
            ))
        })
    }
}

impl NodeMetricsRuntime for FakeMetrics {
    fn collect_metrics(
        &self,
        request: NodeMetricsRequest,
    ) -> NodeMetricsFuture<'_, NodeMetricsResult> {
        NodeMetrics::collect_metrics(self, request)
    }
}

fn assert_control_plane_object_safe(_: &dyn NodeMetrics) {}
fn assert_runtime_object_safe(_: &dyn NodeMetricsRuntime) {}

#[test]
fn metrics_ports_are_object_safe_and_contract_values_are_send_sync() {
    assert_control_plane_object_safe(&FakeMetrics);
    assert_runtime_object_safe(&FakeMetrics);

    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<NodeMetricsTarget>();
    assert_send_sync::<NodeMetricsRequest>();
    assert_send_sync::<NodeMetricsNodeSample>();
    assert_send_sync::<NodeMetricsPodSample>();
    assert_send_sync::<NodeMetricsContainerSample>();
    assert_send_sync::<NodeMetricsResult>();
    assert_send_sync::<NodeMetricsError>();
}

#[test]
fn request_and_result_preserve_target_filter_and_samples_without_correlation() {
    let target = NodeMetricsTarget::try_new("worker-a").expect("valid metrics target");
    let request = NodeMetricsRequest::new(
        target.clone(),
        vec!["uid-a".to_string(), String::new(), "uid-b".to_string()],
    );
    assert_eq!(request.target(), &target);
    assert_eq!(request.pod_uids(), ["uid-a", "", "uid-b"]);

    let node = NodeMetricsNodeSample::new(123, 456);
    let container = NodeMetricsContainerSample::new("main", 11, 22);
    let pod = NodeMetricsPodSample::new("default", "web", "pod-uid", vec![container.clone()]);
    let result = NodeMetricsResult::new(target, Some(node), vec![pod.clone()]);

    assert_eq!(result.target().node_name(), "worker-a");
    assert_eq!(result.node(), Some(&node));
    assert_eq!(result.pods(), [pod]);
    assert_eq!(node.cpu_nanos(), 123);
    assert_eq!(node.memory_bytes(), 456);
    assert_eq!(container.name(), "main");
    assert_eq!(container.cpu_nanos(), 11);
    assert_eq!(container.memory_bytes(), 22);
}

#[test]
fn target_validation_and_typed_errors_are_transport_neutral() {
    assert!(matches!(
        NodeMetricsTarget::try_new(""),
        Err(NodeMetricsError::InvalidRequest {
            field: "metrics.node_name",
            ..
        })
    ));

    for error in [
        NodeMetricsError::unavailable("node is disconnected"),
        NodeMetricsError::duplicate_request("duplicate private correlation"),
        NodeMetricsError::timeout("metrics timed out"),
        NodeMetricsError::closed("response channel closed"),
    ] {
        assert!(!error.to_string().is_empty());
    }
}
