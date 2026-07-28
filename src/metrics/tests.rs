use super::model::RuntimeMetricsSnapshot;
use super::provider::NodeMetricsRequestCoalescer;
use super::sampling::parse_proc_node_usage;
use super::*;
use crate::datastore::Resource;
use klights_node_api::{
    NodeMetricsContainerSample, NodeMetricsError, NodeMetricsNodeSample, NodeMetricsPodSample,
    NodeMetricsResult, NodeMetricsTarget,
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
        "spec": {
            "nodeName": "node-a",
            "containers": [
                {"name": "app", "resources": {"requests": {"cpu": "250m", "memory": "64Mi"}}},
                {"name": "besteffort"}
            ]
        }
    }));

    assert!(PodMetric::from_resource(&pod, &RuntimeMetricsSnapshot::default()).is_none());
}

#[test]
fn runtime_sample_overrides_spec_requests_by_pod_uid_and_container_name() {
    let stats = k8s_cri::v1::PodSandboxStats {
        attributes: Some(k8s_cri::v1::PodSandboxAttributes {
            metadata: Some(k8s_cri::v1::PodSandboxMetadata {
                name: "pod-a".to_string(),
                namespace: "default".to_string(),
                uid: "uid-a".to_string(),
                attempt: 0,
            }),
            ..Default::default()
        }),
        linux: Some(k8s_cri::v1::LinuxPodSandboxStats {
            containers: vec![k8s_cri::v1::ContainerStats {
                attributes: Some(k8s_cri::v1::ContainerAttributes {
                    metadata: Some(k8s_cri::v1::ContainerMetadata {
                        name: "app".to_string(),
                        attempt: 0,
                    }),
                    ..Default::default()
                }),
                cpu: Some(k8s_cri::v1::CpuUsage {
                    usage_nano_cores: Some(k8s_cri::v1::UInt64Value { value: 123_000_000 }),
                    ..Default::default()
                }),
                memory: Some(k8s_cri::v1::MemoryUsage {
                    working_set_bytes: Some(k8s_cri::v1::UInt64Value {
                        value: 9 * 1024 * 1024,
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    };
    let runtime = RuntimeMetricsSnapshot::from_pod_sandbox_stats(vec![stats]);
    let pod = pod_resource(json!({
        "metadata": {"name": "pod-a", "namespace": "default", "uid": "uid-a"},
        "spec": {
            "containers": [
                {"name": "app", "resources": {"requests": {"cpu": "250m", "memory": "64Mi"}}}
            ]
        }
    }));

    let metric = PodMetric::from_resource(&pod, &runtime).unwrap();
    let object =
        MetricsObjectBuilder::new("2026-01-01T00:00:00Z".to_string()).pod_metrics_object(&metric);

    assert_eq!(object["containers"][0]["usage"]["cpu"], "123m");
    assert_eq!(object["containers"][0]["usage"]["memory"], "9216Ki");
}

#[test]
fn node_metrics_response_merges_into_runtime_snapshot() {
    let runtime = RuntimeMetricsSnapshot::from_node_metrics_results([Ok(NodeMetricsResult::new(
        NodeMetricsTarget::try_new("node-b").unwrap(),
        Some(NodeMetricsNodeSample::new(222_000_000, 99 * 1024 * 1024)),
        vec![NodeMetricsPodSample::new(
            "default",
            "pod-a",
            "uid-a",
            vec![NodeMetricsContainerSample::new(
                "app",
                88_000_000,
                7 * 1024 * 1024,
            )],
        )],
    ))]);
    let snapshot = MetricsSnapshot::from_runtime_nodes(&runtime);
    assert_eq!(
        snapshot.available_node_usage("node-b"),
        Some(ResourceUsage::new(222_000_000, 99 * 1024 * 1024))
    );
    let pod = pod_resource(json!({
        "metadata": {"name": "pod-a", "namespace": "default", "uid": "uid-a"},
        "spec": {
            "nodeName": "node-b",
            "containers": [
                {"name": "app", "resources": {"requests": {"cpu": "250m", "memory": "64Mi"}}}
            ]
        }
    }));

    let metric = PodMetric::from_resource(&pod, &runtime).unwrap();
    let object =
        MetricsObjectBuilder::new("2026-01-01T00:00:00Z".to_string()).pod_metrics_object(&metric);

    assert_eq!(object["containers"][0]["usage"]["cpu"], "88m");
    assert_eq!(object["containers"][0]["usage"]["memory"], "7168Ki");
}

#[test]
fn node_only_metrics_result_remains_a_partial_success() {
    let runtime = RuntimeMetricsSnapshot::from_node_metrics_results([Ok(NodeMetricsResult::new(
        NodeMetricsTarget::try_new("node-without-cri-stats").unwrap(),
        Some(NodeMetricsNodeSample::new(333_000_000, 128 * 1024 * 1024)),
        Vec::new(),
    ))]);

    assert_eq!(
        MetricsSnapshot::from_runtime_nodes(&runtime)
            .available_node_usage("node-without-cri-stats"),
        Some(ResourceUsage::new(333_000_000, 128 * 1024 * 1024))
    );
    assert!(runtime.by_uid.is_empty());
    assert!(runtime.by_namespace_name.is_empty());
}

#[test]
fn proc_node_usage_parser_reports_used_memory_and_cpu_counters() {
    let usage = parse_proc_node_usage(
        "cpu 10 20 30 100 5 2 3 0 0 0\ncpu0 1 2 3 4 5 6 7 0 0 0\n",
        "MemTotal:       1024 kB\nMemAvailable:    256 kB\n",
    )
    .unwrap();

    assert_eq!(usage.cpu_total_jiffies, 170);
    assert_eq!(usage.cpu_idle_jiffies, 105);
    assert_eq!(usage.memory_used_bytes, 768 * 1024);
}

#[tokio::test]
async fn coalesces_concurrent_node_metrics_fetches() {
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let coalescer = Arc::new(NodeMetricsRequestCoalescer::default());
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let started = Arc::new(tokio::sync::Barrier::new(2));
    let release = Arc::new(tokio::sync::Notify::new());

    let first = {
        let coalescer = coalescer.clone();
        let supervisor = supervisor.clone();
        let calls = calls.clone();
        let started = started.clone();
        let release = release.clone();
        tokio::spawn(async move {
            coalescer
                .get_or_spawn("node-a".to_string(), supervisor, async move {
                    calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    started.wait().await;
                    release.notified().await;
                    Ok(NodeMetricsResult::new(
                        NodeMetricsTarget::try_new("node-a").unwrap(),
                        None,
                        Vec::new(),
                    ))
                })
                .await
        })
    };

    started.wait().await;
    let second = {
        let coalescer = coalescer.clone();
        let supervisor = supervisor.clone();
        let calls = calls.clone();
        tokio::spawn(async move {
            coalescer
                .get_or_spawn("node-a".to_string(), supervisor, async move {
                    calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Err(NodeMetricsError::unavailable("must not run"))
                })
                .await
        })
    };

    release.notify_waiters();
    let (first, second) = tokio::join!(first, second);
    let first = first.unwrap();
    let second = second.unwrap();

    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(first.unwrap().target().node_name(), "node-a");
    assert_eq!(second.unwrap().target().node_name(), "node-a");

    let _ = supervisor.shutdown(std::time::Duration::from_secs(1)).await;
}
