use super::{ProbeManager, parse_probe_params};
use crate::lifecycle::LifecycleCommand;
use crate::runtime::test_support::MockCriRuntime;
use crate::runtime_clock::SystemRuntimeClock;
use klights_pod_api::{
    PodGetRequest, PodListRequest, PodListResult, PodOwnerListRequest, PodQuery,
    PodRepositoryError, PodRepositoryFuture,
};
use serde_json::{Value, json};
use std::sync::Arc;

struct StaticPodQuery(klights_cluster_core::Resource);

impl PodQuery for StaticPodQuery {
    fn get_pod(
        &self,
        _request: PodGetRequest,
    ) -> PodRepositoryFuture<'_, Option<klights_cluster_core::Resource>> {
        Box::pin(async { Ok(Some(self.0.clone())) })
    }

    fn list_pods(&self, _request: PodListRequest) -> PodRepositoryFuture<'_, PodListResult> {
        Box::pin(async { Err(PodRepositoryError::unavailable("unused list operation")) })
    }

    fn list_pods_by_owner_uid(
        &self,
        _request: PodOwnerListRequest,
    ) -> PodRepositoryFuture<'_, Vec<klights_cluster_core::Resource>> {
        Box::pin(async { Err(PodRepositoryError::unavailable("unused owner query")) })
    }
}

fn probe_manager() -> ProbeManager {
    let pod_reader: Arc<dyn PodQuery> = Arc::new(StaticPodQuery(
        klights_cluster_core::Resource::try_from_data(Arc::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "stored", "namespace": "default", "uid": "uid-stored"},
            "spec": {"containers": []},
            "status": {"phase": "Running", "podIP": "10.43.0.5"}
        })))
        .unwrap(),
    ));
    let (lifecycle_tx, _lifecycle_rx) = tokio::sync::mpsc::channel::<LifecycleCommand>(1);
    ProbeManager::new_with_lifecycle(
        Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        )),
        pod_reader,
        Some(Arc::new(MockCriRuntime::new())),
        lifecycle_tx,
        Arc::new(SystemRuntimeClock),
    )
}

fn pod_with_containers(name: &str, namespace: &str, containers: Value) -> Value {
    json!({
        "metadata": {"name": name, "namespace": namespace},
        "spec": {"containers": containers},
        "status": {"phase": "Running", "podIP": "10.43.0.5"}
    })
}

#[test]
fn probe_params_preserve_explicit_values() {
    let params = parse_probe_params(&json!({
        "initialDelaySeconds": 5,
        "periodSeconds": 15,
        "timeoutSeconds": 3,
        "failureThreshold": 5,
        "successThreshold": 2
    }));

    assert_eq!(params.initial_delay, 5);
    assert_eq!(params.interval_secs, 15);
    assert_eq!(params.timeout_secs, 3);
    assert_eq!(params.failure_threshold, 5);
    assert_eq!(params.success_threshold, 2);
}

#[test]
fn probe_params_apply_kubernetes_defaults_to_missing_and_zero_values() {
    for spec in [
        json!({}),
        json!({
            "periodSeconds": 0,
            "timeoutSeconds": 0,
            "failureThreshold": 0,
            "successThreshold": 0
        }),
    ] {
        let params = parse_probe_params(&spec);
        assert_eq!(params.initial_delay, 0);
        assert_eq!(params.interval_secs, 10);
        assert_eq!(params.timeout_secs, 1);
        assert_eq!(params.failure_threshold, 3);
        assert_eq!(params.success_threshold, 1);
    }
}

#[test]
fn probe_params_default_only_missing_values() {
    let params = parse_probe_params(&json!({
        "periodSeconds": 20,
        "failureThreshold": 10
    }));

    assert_eq!(params.initial_delay, 0);
    assert_eq!(params.interval_secs, 20);
    assert_eq!(params.timeout_secs, 1);
    assert_eq!(params.failure_threshold, 10);
    assert_eq!(params.success_threshold, 1);
}

#[tokio::test]
async fn test_start_probes_missing_metadata_returns_error() {
    let pod = json!({"spec": {"containers": []}});
    let result = probe_manager().start_probes(&pod).await;
    assert!(result.unwrap_err().to_string().contains("metadata"));
}

#[tokio::test]
async fn test_start_probes_missing_spec_returns_error() {
    let pod = json!({"metadata": {"name": "p", "namespace": "ns", "uid": "uid-p"}});
    let result = probe_manager().start_probes(&pod).await;
    assert!(result.unwrap_err().to_string().contains("spec"));
}

#[tokio::test]
async fn test_start_probes_missing_pod_ip_returns_error() {
    let pod = json!({
        "metadata": {"name": "p", "namespace": "ns", "uid": "uid-p"},
        "spec": {"containers": [{"name": "c", "image": "nginx"}]},
        "status": {"phase": "Running"}
    });
    let result = probe_manager().start_probes(&pod).await;
    assert!(result.unwrap_err().to_string().contains("podIP"));
}

#[tokio::test]
async fn test_start_probes_no_probes_defined_succeeds_with_no_tasks() {
    let pod = pod_with_containers(
        "simple",
        "default",
        json!([{"name": "app", "image": "nginx"}]),
    );
    let pm = probe_manager();
    pm.start_probes(&pod).await.unwrap();
    assert_eq!(pm.task_count_for_test("default/simple").await, Some(0));
    pm.stop_probes("default", "simple").await;
    assert!(pm.task_count_for_test("default/simple").await.is_none());
}

#[tokio::test]
async fn test_start_probes_spawns_tasks_for_readiness_and_liveness() {
    let pod = pod_with_containers(
        "probed",
        "default",
        json!([{
            "name": "app",
            "image": "nginx",
            "readinessProbe": {"httpGet": {"port": 80}, "initialDelaySeconds": 3600},
            "livenessProbe": {"tcpSocket": {"port": 80}, "initialDelaySeconds": 3600}
        }]),
    );
    let pm = probe_manager();
    pm.start_probes(&pod).await.unwrap();
    assert_eq!(pm.task_count_for_test("default/probed").await, Some(2));
    pm.stop_probes("default", "probed").await;
    assert!(pm.task_count_for_test("default/probed").await.is_none());
}

#[tokio::test]
async fn test_stop_probes_removes_tasks() {
    let pod = pod_with_containers(
        "stopping",
        "ns1",
        json!([{
            "name": "app",
            "image": "nginx",
            "readinessProbe": {"httpGet": {"port": 80}, "initialDelaySeconds": 3600}
        }]),
    );
    let pm = probe_manager();
    pm.start_probes(&pod).await.unwrap();
    assert_eq!(pm.task_count_for_test("ns1/stopping").await, Some(1));
    pm.stop_probes("ns1", "stopping").await;
    assert!(pm.task_count_for_test("ns1/stopping").await.is_none());
}

#[tokio::test]
async fn test_stop_probes_for_uid_leaves_recreated_same_name_pod_tasks() {
    let pod_for_uid = |uid: &str| {
        let mut pod = pod_with_containers(
            "ordinal-0",
            "statefulset-ns",
            json!([{
                "name": "app",
                "image": "registry.k8s.io/pause:3.10.1",
                "readinessProbe": {"tcpSocket": {"port": 80}, "initialDelaySeconds": 3600}
            }]),
        );
        pod["metadata"]["uid"] = json!(uid);
        pod
    };
    let old_pod = pod_for_uid("old-uid");
    let pm = probe_manager();
    pm.start_probes(&old_pod).await.unwrap();
    pm.start_probes(&pod_for_uid("new-uid")).await.unwrap();

    pm.stop_probes_for_uid("statefulset-ns", "ordinal-0", "old-uid")
        .await;
    assert!(
        pm.task_count_for_test("statefulset-ns/ordinal-0/old-uid")
            .await
            .is_none()
    );
    assert_eq!(
        pm.task_count_for_test("statefulset-ns/ordinal-0/new-uid")
            .await,
        Some(1)
    );
    pm.stop_probes_for_uid("statefulset-ns", "ordinal-0", "new-uid")
        .await;
}

#[tokio::test]
async fn test_stop_probes_nonexistent_pod_is_noop() {
    probe_manager().stop_probes("default", "nonexistent").await;
}

#[tokio::test]
async fn test_start_probes_multiple_containers_each_with_probes() {
    let pod = pod_with_containers(
        "multi",
        "default",
        json!([
            {
                "name": "web",
                "image": "nginx",
                "readinessProbe": {"httpGet": {"port": 80}, "initialDelaySeconds": 3600}
            },
            {
                "name": "sidecar",
                "image": "envoy",
                "livenessProbe": {"tcpSocket": {"port": 15000}, "initialDelaySeconds": 3600}
            }
        ]),
    );
    let pm = probe_manager();
    pm.start_probes(&pod).await.unwrap();
    assert_eq!(pm.task_count_for_test("default/multi").await, Some(2));
    pm.stop_probes("default", "multi").await;
    assert!(pm.task_count_for_test("default/multi").await.is_none());
}

#[tokio::test]
async fn test_start_probes_with_startup_probe_spawns_task() {
    let pod = pod_with_containers(
        "probe-pod",
        "default",
        json!([{
            "name": "app",
            "startupProbe": {
                "httpGet": {"path": "/healthz", "port": 8080},
                "initialDelaySeconds": 3600,
                "periodSeconds": 3,
                "failureThreshold": 10
            }
        }]),
    );
    let pm = probe_manager();
    pm.start_probes(&pod).await.unwrap();
    assert_eq!(pm.task_count_for_test("default/probe-pod").await, Some(1));
    pm.stop_probes("default", "probe-pod").await;
    assert!(pm.task_count_for_test("default/probe-pod").await.is_none());
}
