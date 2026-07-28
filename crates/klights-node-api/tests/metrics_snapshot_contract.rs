use klights_node_api::{
    NodeMetricsContainerSample, NodeMetricsNodeSample, NodeMetricsPodSample, NodeMetricsResult,
    NodeMetricsSnapshot, NodeMetricsTarget, NodeMetricsUsage,
};

#[test]
fn snapshot_indexes_transport_neutral_node_and_pod_samples() {
    let snapshot = NodeMetricsSnapshot::from_results([NodeMetricsResult::new(
        NodeMetricsTarget::try_new("worker-a").unwrap(),
        Some(NodeMetricsNodeSample::new(42, 4096)),
        vec![NodeMetricsPodSample::new(
            "default",
            "web",
            "pod-uid",
            vec![NodeMetricsContainerSample::new("main", 7, 1024)],
        )],
    )]);

    assert_eq!(
        snapshot.node_usage("worker-a"),
        Some(NodeMetricsUsage::new(42, 4096))
    );
    assert_eq!(
        snapshot.container_usage("pod-uid", "default", "web", "main"),
        Some(NodeMetricsUsage::new(7, 1024))
    );
}

#[test]
fn snapshot_falls_back_to_namespace_and_name_when_uid_is_absent() {
    let snapshot = NodeMetricsSnapshot::from_results([NodeMetricsResult::new(
        NodeMetricsTarget::try_new("worker-a").unwrap(),
        None,
        vec![NodeMetricsPodSample::new(
            "default",
            "web",
            "",
            vec![NodeMetricsContainerSample::new("main", 9, 2048)],
        )],
    )]);

    assert_eq!(
        snapshot.container_usage("", "default", "web", "main"),
        Some(NodeMetricsUsage::new(9, 2048))
    );
}
