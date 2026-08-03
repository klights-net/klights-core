use super::*;
use crate::datastore::DatastoreBackend;
use crate::hpa_controller_adapter::reconcile_hpa_with_metrics as reconcile_hpa_with_metrics_root;
use klights_node_api::{
    NodeMetrics, NodeMetricsContainerSample, NodeMetricsPodSample, NodeMetricsResult,
    NodeMetricsTarget,
};
use klights_reconcile_api::{ControllerStoreError, ControllerStoreResult};
use serde_json::json;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

async fn reconcile_hpa(
    db: &dyn crate::datastore::DatastoreBackend,
    pod_repository: &crate::kubelet::pod_repository::PodRepository,
    hpa: &serde_json::Value,
    node_name: &str,
) -> anyhow::Result<()> {
    reconcile_hpa_with_metrics_root(
        db,
        pod_repository,
        crate::controller_test_support::non_pod_finalization_port_for_test(),
        &klights_controllers::ControllerCoordination::new(),
        hpa,
        node_name,
        &crate::node_metrics_adapter::UnavailableNodeMetrics,
        crate::controller_test_support::deterministic_controller_identity().as_ref(),
        chrono::Utc::now(),
    )
    .await
}

async fn reconcile_hpa_with_metrics(
    db: &dyn crate::datastore::DatastoreBackend,
    pod_repository: &crate::kubelet::pod_repository::PodRepository,
    hpa: &serde_json::Value,
    node_name: &str,
    node_metrics: &dyn NodeMetrics,
) -> anyhow::Result<()> {
    reconcile_hpa_with_metrics_root(
        db,
        pod_repository,
        crate::controller_test_support::non_pod_finalization_port_for_test(),
        &klights_controllers::ControllerCoordination::new(),
        hpa,
        node_name,
        node_metrics,
        crate::controller_test_support::deterministic_controller_identity().as_ref(),
        chrono::Utc::now(),
    )
    .await
}

struct MissingTargetRuntime {
    current: Mutex<Resource>,
    conflict_updates_remaining: AtomicUsize,
    successful_updates: AtomicUsize,
}

#[async_trait]
impl HpaRuntime for MissingTargetRuntime {
    async fn get_hpa(
        &self,
        _api_version: &str,
        _namespace: &str,
        _name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        Ok(Some(self.current.lock().unwrap().clone()))
    }

    async fn get_scale_target(
        &self,
        _api_version: &str,
        _kind: &str,
        _namespace: &str,
        _name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        Ok(None)
    }

    async fn list_pods(&self, _namespace: &str) -> ControllerStoreResult<Vec<Resource>> {
        unreachable!("missing target never lists Pods")
    }

    async fn patch_scale_target(
        &self,
        _target: &ScaleTarget,
        _replicas: i64,
    ) -> ControllerStoreResult<Resource> {
        unreachable!("missing target never scales")
    }

    async fn reconcile_scaled_target(
        &self,
        _target: &ScaleTarget,
        _resource: &Value,
        _node_name: &str,
    ) -> ControllerStoreResult<()> {
        unreachable!("missing target never reconciles")
    }

    async fn update_hpa_status(
        &self,
        _current: &Resource,
        status: Value,
    ) -> ControllerStoreResult<()> {
        if self
            .conflict_updates_remaining
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(ControllerStoreError::conflict("synthetic HPA conflict"));
        }
        let mut current = self.current.lock().unwrap();
        std::sync::Arc::make_mut(&mut current.data)["status"] = status;
        self.successful_updates.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

fn missing_target_runtime(conflicts: usize) -> MissingTargetRuntime {
    let hpa = json!({
        "apiVersion": "autoscaling/v2",
        "kind": "HorizontalPodAutoscaler",
        "metadata": {
            "name": "missing",
            "namespace": "default",
            "uid": "hpa-missing",
            "generation": 1
        },
        "spec": {
            "scaleTargetRef": {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "name": "missing"
            },
            "minReplicas": 1,
            "maxReplicas": 3
        }
    });
    MissingTargetRuntime {
        current: Mutex::new(Resource::try_from_data(std::sync::Arc::new(hpa)).unwrap()),
        conflict_updates_remaining: AtomicUsize::new(conflicts),
        successful_updates: AtomicUsize::new(0),
    }
}

struct EmptyHpaMetrics;

#[async_trait]
impl HpaMetrics for EmptyHpaMetrics {
    async fn snapshot(&self, _pods: &[Resource]) -> HpaMetricsSnapshot {
        HpaMetricsSnapshot::default()
    }
}

#[tokio::test]
async fn missing_target_retries_status_conflict_and_then_stabilizes_as_noop() {
    let runtime = missing_target_runtime(1);
    let hpa = (*runtime.current.lock().unwrap().data).clone();
    let metrics = EmptyHpaMetrics;
    reconcile_hpa_with_runtime(&runtime, &hpa, "node-a", &metrics, chrono::Utc::now())
        .await
        .unwrap();
    assert_eq!(runtime.successful_updates.load(Ordering::Relaxed), 1);
    assert_eq!(
        runtime
            .current
            .lock()
            .unwrap()
            .data
            .pointer("/status/conditions/0/reason")
            .and_then(Value::as_str),
        Some("FailedGetScale")
    );

    let current = (*runtime.current.lock().unwrap().data).clone();
    reconcile_hpa_with_runtime(&runtime, &current, "node-a", &metrics, chrono::Utc::now())
        .await
        .unwrap();
    assert_eq!(
        runtime.successful_updates.load(Ordering::Relaxed),
        1,
        "stable missing-target status must not write again"
    );
}

async fn create_ready_pod(
    db: &dyn DatastoreBackend,
    namespace: &str,
    name: &str,
    labels: serde_json::Value,
) {
    db.create_resource(
        "v1",
        "Pod",
        Some(namespace),
        name,
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": labels
            },
            "spec": {
                "nodeName": "node-a",
                "containers": [{
                    "name": "app",
                    "image": "nginx",
                    "resources": {"requests": {"cpu": "100m"}}
                }]
            },
            "status": {
                "phase": "Running",
                "conditions": [{"type": "Ready", "status": "True"}],
                "containerStatuses": [{"name": "app", "ready": true}]
            }
        }),
    )
    .await
    .unwrap();
}

#[derive(Clone)]
struct StaticNodeMetrics {
    result: NodeMetricsResult,
}

impl NodeMetrics for StaticNodeMetrics {
    fn collect_metrics(
        &self,
        _request: klights_node_api::NodeMetricsRequest,
    ) -> klights_node_api::NodeMetricsFuture<'_, NodeMetricsResult> {
        Box::pin(async { Ok(self.result.clone()) })
    }
}

fn runtime_metrics_for_pods<'a>(
    namespace: &str,
    pod_names: impl IntoIterator<Item = &'a str>,
    cpu_nanos: u64,
    memory_bytes: u64,
) -> NodeMetricsResult {
    NodeMetricsResult::new(
        NodeMetricsTarget::try_new("node-a").unwrap(),
        None,
        pod_names
            .into_iter()
            .map(|name| {
                NodeMetricsPodSample::new(
                    namespace,
                    name,
                    "",
                    vec![NodeMetricsContainerSample::new(
                        "app",
                        cpu_nanos,
                        memory_bytes,
                    )],
                )
            })
            .collect(),
    )
}

#[tokio::test]
async fn hpa_v2_resource_metric_scales_deployment_from_resource_usage() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repository = crate::controller_test_support::pod_repository_for_test(&db);

    let _deployment = crate::controller_test_support::store_and_prepare(
        &db,
        "apps/v1",
        "Deployment",
        Some("default"),
        "web",
        json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "web", "namespace": "default", "uid": "deploy-web"},
            "spec": {
                "replicas": 4,
                "selector": {"matchLabels": {"app": "web"}},
                "template": {
                    "metadata": {"labels": {"app": "web"}},
                    "spec": {"containers": [{"name": "app", "image": "nginx"}]}
                }
            },
            "status": {"replicas": 4, "readyReplicas": 4}
        }),
    )
    .await;

    for index in 0..4 {
        create_ready_pod(
            &db,
            "default",
            &format!("web-{index}"),
            json!({"app": "web"}),
        )
        .await;
    }

    let hpa = crate::controller_test_support::store_and_prepare(
        &db,
        "autoscaling/v2",
        "HorizontalPodAutoscaler",
        Some("default"),
        "web",
        json!({
            "apiVersion": "autoscaling/v2",
            "kind": "HorizontalPodAutoscaler",
            "metadata": {"name": "web", "namespace": "default", "uid": "hpa-web", "generation": 1},
            "spec": {
                "scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": "web"},
                "minReplicas": 2,
                "maxReplicas": 8,
                "metrics": [{
                    "type": "Resource",
                    "resource": {
                        "name": "cpu",
                        "target": {"type": "Utilization", "averageUtilization": 50}
                    }
                }]
            }
        }),
    )
    .await;

    let pod_names: Vec<String> = (0..4).map(|index| format!("web-{index}")).collect();
    let pod_name_refs: Vec<&str> = pod_names.iter().map(String::as_str).collect();
    let node_metrics = StaticNodeMetrics {
        result: runtime_metrics_for_pods(
            "default",
            pod_name_refs.iter().copied(),
            100_000_000,
            64 * 1024 * 1024,
        ),
    };
    reconcile_hpa_with_metrics(&db, pod_repository.as_ref(), &hpa, "node-a", &node_metrics)
        .await
        .unwrap();

    let deployment = db
        .get_resource("apps/v1", "Deployment", Some("default"), "web")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(deployment.data.pointer("/spec/replicas"), Some(&json!(8)));

    let hpa = db
        .get_resource(
            "autoscaling/v2",
            "HorizontalPodAutoscaler",
            Some("default"),
            "web",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(hpa.data.pointer("/status/currentReplicas"), Some(&json!(4)));
    assert_eq!(hpa.data.pointer("/status/desiredReplicas"), Some(&json!(8)));
    assert_eq!(
        hpa.data
            .pointer("/status/currentMetrics/0/resource/current/averageUtilization"),
        Some(&json!(100))
    );
    assert_eq!(
        hpa.data.pointer("/status/conditions/0/type"),
        Some(&json!("AbleToScale"))
    );
    assert_eq!(
        hpa.data.pointer("/status/conditions/0/status"),
        Some(&json!("True"))
    );

    let _ = deployment;
}

#[tokio::test]
async fn hpa_v1_cpu_metric_scales_replicationcontroller_from_resource_usage() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repository = crate::controller_test_support::pod_repository_for_test(&db);

    let _rc = crate::controller_test_support::store_and_prepare(
        &db,
        "v1",
        "ReplicationController",
        Some("default"),
        "legacy",
        json!({
            "apiVersion": "v1",
            "kind": "ReplicationController",
            "metadata": {"name": "legacy", "namespace": "default", "uid": "rc-legacy"},
            "spec": {
                "replicas": 3,
                "selector": {"app": "legacy"},
                "template": {
                    "metadata": {"labels": {"app": "legacy"}},
                    "spec": {"containers": [{"name": "app", "image": "nginx"}]}
                }
            },
            "status": {"replicas": 3, "readyReplicas": 3}
        }),
    )
    .await;

    for index in 0..3 {
        create_ready_pod(
            &db,
            "default",
            &format!("legacy-{index}"),
            json!({"app": "legacy"}),
        )
        .await;
    }

    let hpa = crate::controller_test_support::store_and_prepare(
            &db,
            "autoscaling/v1",
            "HorizontalPodAutoscaler",
            Some("default"),
            "legacy",
            json!({
                "apiVersion": "autoscaling/v1",
                "kind": "HorizontalPodAutoscaler",
                "metadata": {"name": "legacy", "namespace": "default", "uid": "hpa-legacy", "generation": 1},
                "spec": {
                    "scaleTargetRef": {"apiVersion": "v1", "kind": "ReplicationController", "name": "legacy"},
                    "minReplicas": 1,
                    "maxReplicas": 5,
                    "targetCPUUtilizationPercentage": 60
                }
            }),
        )
        .await;

    let pod_names: Vec<String> = (0..3).map(|index| format!("legacy-{index}")).collect();
    let pod_name_refs: Vec<&str> = pod_names.iter().map(String::as_str).collect();
    let node_metrics = StaticNodeMetrics {
        result: runtime_metrics_for_pods(
            "default",
            pod_name_refs.iter().copied(),
            100_000_000,
            64 * 1024 * 1024,
        ),
    };
    reconcile_hpa_with_metrics(&db, pod_repository.as_ref(), &hpa, "node-a", &node_metrics)
        .await
        .unwrap();

    let rc = db
        .get_resource("v1", "ReplicationController", Some("default"), "legacy")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rc.data.pointer("/spec/replicas"), Some(&json!(5)));

    let hpa = db
        .get_resource(
            "autoscaling/v1",
            "HorizontalPodAutoscaler",
            Some("default"),
            "legacy",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(hpa.data.pointer("/status/currentReplicas"), Some(&json!(3)));
    assert_eq!(hpa.data.pointer("/status/desiredReplicas"), Some(&json!(5)));
    assert_eq!(
        hpa.data.pointer("/status/currentCPUUtilizationPercentage"),
        Some(&json!(100))
    );
}

#[tokio::test]
async fn hpa_does_not_scale_when_runtime_metrics_are_unavailable() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repository = crate::controller_test_support::pod_repository_for_test(&db);

    let _deployment = crate::controller_test_support::store_and_prepare(
        &db,
        "apps/v1",
        "Deployment",
        Some("default"),
        "web",
        json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "web", "namespace": "default", "uid": "deploy-web"},
            "spec": {
                "replicas": 4,
                "selector": {"matchLabels": {"app": "web"}},
                "template": {
                    "metadata": {"labels": {"app": "web"}},
                    "spec": {"containers": [{"name": "app", "image": "nginx"}]}
                }
            },
            "status": {"replicas": 4, "readyReplicas": 4}
        }),
    )
    .await;

    for index in 0..4 {
        create_ready_pod(
            &db,
            "default",
            &format!("web-{index}"),
            json!({"app": "web"}),
        )
        .await;
    }

    let hpa = crate::controller_test_support::store_and_prepare(
        &db,
        "autoscaling/v2",
        "HorizontalPodAutoscaler",
        Some("default"),
        "web",
        json!({
            "apiVersion": "autoscaling/v2",
            "kind": "HorizontalPodAutoscaler",
            "metadata": {"name": "web", "namespace": "default", "uid": "hpa-web", "generation": 1},
            "spec": {
                "scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": "web"},
                "minReplicas": 2,
                "maxReplicas": 8,
                "metrics": [{
                    "type": "Resource",
                    "resource": {
                        "name": "cpu",
                        "target": {"type": "Utilization", "averageUtilization": 50}
                    }
                }]
            }
        }),
    )
    .await;

    reconcile_hpa(&db, pod_repository.as_ref(), &hpa, "node-a")
        .await
        .unwrap();

    let deployment = db
        .get_resource("apps/v1", "Deployment", Some("default"), "web")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(deployment.data.pointer("/spec/replicas"), Some(&json!(4)));

    let hpa = db
        .get_resource(
            "autoscaling/v2",
            "HorizontalPodAutoscaler",
            Some("default"),
            "web",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(hpa.data.pointer("/status/currentReplicas"), Some(&json!(4)));
    assert_eq!(hpa.data.pointer("/status/desiredReplicas"), Some(&json!(4)));
    assert_eq!(
        hpa.data.pointer("/status/conditions/1/reason"),
        Some(&json!("FailedGetResourceMetric"))
    );
    assert!(hpa.data.pointer("/status/currentMetrics").is_none());
}
