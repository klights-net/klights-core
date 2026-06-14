use super::*;
use crate::datastore::DatastoreBackend;
use serde_json::json;

/// Build a `PodRepository` Arc from an existing `DatastoreHandle` for tests
/// that exercise `update_pod_condition` after the kubelet migration to the
/// repository surface.
fn fixture_pod_repository(
    db_handle: &crate::datastore::DatastoreHandle,
) -> std::sync::Arc<crate::kubelet::pod_repository::PodRepository> {
    let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    ));
    let side_effects = std::sync::Arc::new(crate::side_effects::SideEffectRegistry::new());
    let metrics = crate::side_effects::SideEffectMetrics::new();
    std::sync::Arc::new(crate::kubelet::pod_repository::PodRepository::new(
        db_handle.clone(),
        supervisor,
        side_effects,
        metrics,
    ))
}

/// Helper: create a pod in DB with status.conditions array
async fn create_test_pod(
    db: &dyn DatastoreBackend,
    namespace: &str,
    name: &str,
    conditions: Vec<Value>,
) -> i64 {
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {"containers": [{"name": "app", "image": "nginx"}]},
        "status": {
            "phase": "Running",
            "conditions": conditions
        }
    });
    let created = db
        .create_resource("v1", "Pod", Some(namespace), name, pod)
        .await
        .unwrap();
    created.resource_version
}

/// Helper: read the Ready condition from a pod in DB
async fn get_ready_condition(
    db: &dyn DatastoreBackend,
    namespace: &str,
    name: &str,
) -> Option<Value> {
    let resource = db
        .get_resource("v1", "Pod", Some(namespace), name)
        .await
        .unwrap()?;
    resource
        .data
        .get("status")
        .and_then(|s| s.get("conditions"))
        .and_then(|c| c.as_array())
        .and_then(|arr| {
            arr.iter()
                .find(|c| c.get("type").and_then(|t| t.as_str()) == Some("Ready"))
        })
        .cloned()
}

#[tokio::test]
async fn test_update_pod_condition_readiness_success_sets_ready_true() {
    let db = crate::datastore::test_support::in_memory().await;
    let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());
    create_test_pod(
        &db,
        "default",
        "test-pod",
        vec![json!({"type": "Ready", "status": "False", "reason": "ReadinessProbeFailed"})],
    )
    .await;

    update_pod_condition(
        &db_handle,
        &fixture_pod_repository(&db_handle),
        "default/test-pod",
        "app",
        ProbeType::Readiness,
        true,
    )
    .await
    .unwrap();

    let cond = get_ready_condition(&db, "default", "test-pod")
        .await
        .unwrap();
    assert_eq!(cond["status"], "True");
    assert_eq!(cond["reason"], "ReadinessProbeSucceeded");
}

#[tokio::test]
async fn test_update_pod_condition_readiness_success_sets_container_status_ready() {
    let db = crate::datastore::test_support::in_memory().await;
    let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());
    // Create pod with containerStatuses where ready=false (readiness probe not yet passed)
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "probe-pod", "namespace": "default"},
        "spec": {"containers": [{"name": "web", "image": "nginx", "readinessProbe": {"httpGet": {"port": 80}}}]},
        "status": {
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "False", "reason": "ReadinessProbeFailed"}],
            "containerStatuses": [{"name": "web", "ready": false, "containerID": "containerd://abc"}]
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "probe-pod", pod)
        .await
        .unwrap();

    // Readiness probe succeeds
    update_pod_condition(
        &db_handle,
        &fixture_pod_repository(&db_handle),
        "default/probe-pod",
        "web",
        ProbeType::Readiness,
        true,
    )
    .await
    .unwrap();

    let resource = db
        .get_resource("v1", "Pod", Some("default"), "probe-pod")
        .await
        .unwrap()
        .unwrap();

    // containerStatuses[].ready must be updated to true
    let statuses = resource.data["status"]["containerStatuses"]
        .as_array()
        .unwrap();
    let web_status = statuses.iter().find(|s| s["name"] == "web").unwrap();
    assert_eq!(
        web_status["ready"], true,
        "containerStatuses[].ready must be set to true when readiness probe succeeds"
    );
}

#[tokio::test]
async fn test_update_pod_condition_readiness_failure_sets_container_status_not_ready() {
    let db = crate::datastore::test_support::in_memory().await;
    let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());
    // Create pod with containerStatuses where ready=true (readiness probe previously passed)
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "fail-pod", "namespace": "default"},
        "spec": {"containers": [{"name": "web", "image": "nginx", "readinessProbe": {"httpGet": {"port": 80}}}]},
        "status": {
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "True", "reason": "ReadinessProbeSucceeded"}],
            "containerStatuses": [{"name": "web", "ready": true, "containerID": "containerd://abc"}]
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "fail-pod", pod)
        .await
        .unwrap();

    // Readiness probe fails
    update_pod_condition(
        &db_handle,
        &fixture_pod_repository(&db_handle),
        "default/fail-pod",
        "web",
        ProbeType::Readiness,
        false,
    )
    .await
    .unwrap();

    let resource = db
        .get_resource("v1", "Pod", Some("default"), "fail-pod")
        .await
        .unwrap()
        .unwrap();

    // containerStatuses[].ready must be updated to false
    let statuses = resource.data["status"]["containerStatuses"]
        .as_array()
        .unwrap();
    let web_status = statuses.iter().find(|s| s["name"] == "web").unwrap();
    assert_eq!(
        web_status["ready"], false,
        "containerStatuses[].ready must be set to false when readiness probe fails"
    );
}

#[tokio::test]
async fn test_update_pod_condition_readiness_failure_sets_ready_false() {
    let db = crate::datastore::test_support::in_memory().await;
    let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());
    create_test_pod(
        &db,
        "default",
        "test-pod",
        vec![json!({"type": "Ready", "status": "True", "reason": "ReadinessProbeSucceeded"})],
    )
    .await;

    update_pod_condition(
        &db_handle,
        &fixture_pod_repository(&db_handle),
        "default/test-pod",
        "app",
        ProbeType::Readiness,
        false,
    )
    .await
    .unwrap();

    let cond = get_ready_condition(&db, "default", "test-pod")
        .await
        .unwrap();
    assert_eq!(cond["status"], "False");
    assert_eq!(cond["reason"], "ReadinessProbeFailed");
}

#[tokio::test]
async fn test_update_pod_condition_liveness_is_noop() {
    let db = crate::datastore::test_support::in_memory().await;
    let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());
    create_test_pod(
        &db,
        "default",
        "test-pod",
        vec![json!({"type": "Ready", "status": "True", "reason": "ReadinessProbeSucceeded"})],
    )
    .await;

    // Liveness probe should NOT modify conditions
    update_pod_condition(
        &db_handle,
        &fixture_pod_repository(&db_handle),
        "default/test-pod",
        "app",
        ProbeType::Liveness,
        false,
    )
    .await
    .unwrap();

    let cond = get_ready_condition(&db, "default", "test-pod")
        .await
        .unwrap();
    assert_eq!(
        cond["status"], "True",
        "Liveness probe must not change Ready condition"
    );
    assert_eq!(cond["reason"], "ReadinessProbeSucceeded");
}

#[tokio::test]
async fn test_update_pod_condition_updates_existing_ready_condition() {
    let db = crate::datastore::test_support::in_memory().await;
    let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());
    // Pod starts with Ready=True and an extra condition
    create_test_pod(
        &db,
        "default",
        "test-pod",
        vec![
            json!({"type": "Initialized", "status": "True"}),
            json!({"type": "Ready", "status": "True", "reason": "ReadinessProbeSucceeded"}),
        ],
    )
    .await;

    update_pod_condition(
        &db_handle,
        &fixture_pod_repository(&db_handle),
        "default/test-pod",
        "app",
        ProbeType::Readiness,
        false,
    )
    .await
    .unwrap();

    let resource = db
        .get_resource("v1", "Pod", Some("default"), "test-pod")
        .await
        .unwrap()
        .unwrap();

    let conditions = resource.data["status"]["conditions"].as_array().unwrap();
    // Should have 3 conditions: Initialized + Ready (updated) + ContainersReady (appended)
    assert_eq!(
        conditions.len(),
        3,
        "Should update Ready in-place and append ContainersReady"
    );

    let ready = conditions.iter().find(|c| c["type"] == "Ready").unwrap();
    assert_eq!(ready["status"], "False");
    assert_eq!(ready["reason"], "ReadinessProbeFailed");

    let containers_ready = conditions
        .iter()
        .find(|c| c["type"] == "ContainersReady")
        .unwrap();
    assert_eq!(containers_ready["status"], "False");
    assert_eq!(containers_ready["reason"], "ReadinessProbeFailed");

    // Initialized condition must be untouched
    let init = conditions
        .iter()
        .find(|c| c["type"] == "Initialized")
        .unwrap();
    assert_eq!(init["status"], "True");
}

#[tokio::test]
async fn test_update_pod_condition_creates_ready_condition_if_missing() {
    let db = crate::datastore::test_support::in_memory().await;
    let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());
    // Pod with conditions array but no Ready condition
    create_test_pod(
        &db,
        "default",
        "test-pod",
        vec![json!({"type": "PodScheduled", "status": "True"})],
    )
    .await;

    update_pod_condition(
        &db_handle,
        &fixture_pod_repository(&db_handle),
        "default/test-pod",
        "app",
        ProbeType::Readiness,
        true,
    )
    .await
    .unwrap();

    let resource = db
        .get_resource("v1", "Pod", Some("default"), "test-pod")
        .await
        .unwrap()
        .unwrap();

    let conditions = resource.data["status"]["conditions"].as_array().unwrap();
    assert_eq!(
        conditions.len(),
        3,
        "Should append Ready and ContainersReady conditions"
    );

    let ready = conditions.iter().find(|c| c["type"] == "Ready").unwrap();
    assert_eq!(ready["status"], "True");
    assert_eq!(ready["reason"], "ReadinessProbeSucceeded");
    assert!(ready.get("lastTransitionTime").is_some());

    let containers_ready = conditions
        .iter()
        .find(|c| c["type"] == "ContainersReady")
        .unwrap();
    assert_eq!(containers_ready["status"], "True");
    assert_eq!(containers_ready["reason"], "ReadinessProbeSucceeded");
}

#[tokio::test]
async fn test_update_pod_condition_invalid_pod_key_returns_error() {
    let db = crate::datastore::test_support::in_memory().await;
    let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());

    // Pod key without slash separator
    let result = update_pod_condition(
        &db_handle,
        &fixture_pod_repository(&db_handle),
        "invalid-key",
        "app",
        ProbeType::Readiness,
        true,
    )
    .await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Invalid pod key"));
}

#[tokio::test]
async fn test_update_pod_condition_deleted_pod_returns_ok() {
    let db = crate::datastore::test_support::in_memory().await;
    let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());

    // Pod doesn't exist in DB — function should return Ok (early return)
    let result = update_pod_condition(
        &db_handle,
        &fixture_pod_repository(&db_handle),
        "default/ghost-pod",
        "app",
        ProbeType::Readiness,
        true,
    )
    .await;
    assert!(
        result.is_ok(),
        "Should return Ok for non-existent pod, not error"
    );
}

#[tokio::test]
async fn test_update_pod_condition_for_uid_does_not_update_recreated_same_name_pod() {
    let db = crate::datastore::test_support::in_memory().await;
    let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "same-name", "namespace": "default", "uid": "uid-new"},
        "spec": {"containers": [{"name": "web", "image": "nginx", "readinessProbe": {"httpGet": {"port": 80}}}]},
        "status": {
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "False", "reason": "ReadinessProbeFailed"}],
            "containerStatuses": [{"name": "web", "ready": false, "containerID": "containerd://abc"}]
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "same-name", pod)
        .await
        .unwrap();

    update_pod_condition_for_uid(
        &db_handle,
        &fixture_pod_repository(&db_handle),
        PodConditionProbeUpdate {
            namespace: "default",
            name: "same-name",
            pod_uid: "uid-old",
            container_name: "web",
            probe_type: ProbeType::Readiness,
            success: true,
        },
    )
    .await
    .unwrap();

    let resource = db
        .get_resource("v1", "Pod", Some("default"), "same-name")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        resource.data["status"]["containerStatuses"][0]["ready"],
        json!(false),
        "stale readiness command must not update a same-name replacement pod"
    );
}

#[tokio::test]
async fn test_update_pod_condition_for_uid_updates_matching_pod_uid() {
    let db = crate::datastore::test_support::in_memory().await;
    let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "same-name", "namespace": "default", "uid": "uid-current"},
        "spec": {"containers": [{"name": "web", "image": "nginx", "readinessProbe": {"httpGet": {"port": 80}}}]},
        "status": {
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "False", "reason": "ReadinessProbeFailed"}],
            "containerStatuses": [{"name": "web", "ready": false, "containerID": "containerd://abc"}]
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "same-name", pod)
        .await
        .unwrap();

    update_pod_condition_for_uid(
        &db_handle,
        &fixture_pod_repository(&db_handle),
        PodConditionProbeUpdate {
            namespace: "default",
            name: "same-name",
            pod_uid: "uid-current",
            container_name: "web",
            probe_type: ProbeType::Readiness,
            success: true,
        },
    )
    .await
    .unwrap();

    let resource = db
        .get_resource("v1", "Pod", Some("default"), "same-name")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        resource.data["status"]["containerStatuses"][0]["ready"],
        json!(true),
        "current UID readiness command must update the owning pod"
    );
}

#[tokio::test]
async fn test_start_probes_missing_metadata_returns_error() {
    let db = crate::datastore::test_support::in_memory().await;
    let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());
    let pm = ProbeManager::new(db_handle, "klights".to_string());
    let pod = json!({"spec": {"containers": []}});
    let result = pm.start_probes(&pod).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("metadata"));
}

#[tokio::test]
async fn test_start_probes_missing_spec_returns_error() {
    let db = crate::datastore::test_support::in_memory().await;
    let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());
    let pm = ProbeManager::new(db_handle, "klights".to_string());
    let pod = json!({"metadata": {"name": "p", "namespace": "ns"}});
    let result = pm.start_probes(&pod).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("spec"));
}

#[tokio::test]
async fn test_start_probes_missing_pod_ip_returns_error() {
    let db = crate::datastore::test_support::in_memory().await;
    let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());
    let pm = ProbeManager::new(db_handle, "klights".to_string());
    let pod = json!({
        "metadata": {"name": "p", "namespace": "ns"},
        "spec": {"containers": [{"name": "c", "image": "nginx"}]},
        "status": {"phase": "Running"}
    });
    let result = pm.start_probes(&pod).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("podIP"));
}

#[tokio::test]
async fn test_start_probes_no_probes_defined_succeeds_with_no_tasks() {
    let db = crate::datastore::test_support::in_memory().await;
    let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());
    let pm = ProbeManager::new(db_handle, "klights".to_string());
    let pod = json!({
        "metadata": {"name": "simple", "namespace": "default"},
        "spec": {"containers": [{"name": "app", "image": "nginx"}]},
        "status": {"phase": "Running", "podIP": "10.43.0.5"}
    });
    pm.start_probes(&pod).await.unwrap();
    let tasks = pm.tasks.read().await;
    let handles = tasks.get("default/simple").unwrap();
    assert!(
        handles.is_empty(),
        "No probes defined means no probe tasks spawned"
    );
}

#[tokio::test]
async fn test_start_probes_spawns_tasks_for_readiness_and_liveness() {
    let db = crate::datastore::test_support::in_memory().await;
    let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());
    let pm = ProbeManager::new(db_handle, "klights".to_string());
    let pod = json!({
        "metadata": {"name": "probed", "namespace": "default"},
        "spec": {"containers": [{
            "name": "app",
            "image": "nginx",
            "readinessProbe": {"httpGet": {"port": 80, "path": "/ready"}, "periodSeconds": 5},
            "livenessProbe": {"tcpSocket": {"port": 80}, "periodSeconds": 10}
        }]},
        "status": {
            "phase": "Running",
            "podIP": "10.43.0.5",
            "containerStatuses": [{"name": "app", "containerID": "containerd://abc123"}]
        }
    });
    pm.start_probes(&pod).await.unwrap();
    let tasks = pm.tasks.read().await;
    let handles = tasks.get("default/probed").unwrap();
    assert_eq!(
        handles.len(),
        2,
        "Should have 1 readiness + 1 liveness task"
    );
    // Clean up spawned tasks
    drop(tasks);
    pm.stop_probes("default", "probed").await;
}

#[tokio::test]
async fn test_stop_probes_removes_tasks() {
    let db = crate::datastore::test_support::in_memory().await;
    let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());
    let pm = ProbeManager::new(db_handle, "klights".to_string());
    let pod = json!({
        "metadata": {"name": "stopping", "namespace": "ns1"},
        "spec": {"containers": [{
            "name": "app",
            "image": "nginx",
            "readinessProbe": {"httpGet": {"port": 80}, "periodSeconds": 60}
        }]},
        "status": {
            "phase": "Running",
            "podIP": "10.43.0.10",
            "containerStatuses": [{"name": "app", "containerID": "containerd://xyz"}]
        }
    });
    pm.start_probes(&pod).await.unwrap();

    // Verify task exists
    {
        let tasks = pm.tasks.read().await;
        assert!(tasks.contains_key("ns1/stopping"));
    }

    pm.stop_probes("ns1", "stopping").await;

    // Verify task removed
    let tasks = pm.tasks.read().await;
    assert!(
        !tasks.contains_key("ns1/stopping"),
        "stop_probes must remove entry from tasks map"
    );
}

#[tokio::test]
async fn test_stop_probes_for_uid_leaves_recreated_same_name_pod_tasks() {
    let db = crate::datastore::test_support::in_memory().await;
    let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());
    let pm = ProbeManager::new(db_handle, "klights".to_string());

    let pod_for_uid = |uid: &str, container_id: &str| {
        json!({
            "metadata": {"name": "ordinal-0", "namespace": "statefulset-ns", "uid": uid},
            "spec": {"containers": [{
                "name": "app",
                "image": "registry.k8s.io/pause:3.10.1",
                "readinessProbe": {"tcpSocket": {"port": 80}, "periodSeconds": 60}
            }]},
            "status": {
                "phase": "Running",
                "podIP": "10.43.0.10",
                "containerStatuses": [{"name": "app", "containerID": format!("containerd://{container_id}")}]
            }
        })
    };

    pm.start_probes(&pod_for_uid("old-uid", "old-container"))
        .await
        .unwrap();
    pm.start_probes(&pod_for_uid("new-uid", "new-container"))
        .await
        .unwrap();

    pm.stop_probes_for_uid("statefulset-ns", "ordinal-0", "old-uid")
        .await;

    let tasks = pm.tasks.read().await;
    assert!(
        !tasks.contains_key("statefulset-ns/ordinal-0/old-uid"),
        "old UID probe tasks must be removed"
    );
    assert!(
        tasks.contains_key("statefulset-ns/ordinal-0/new-uid"),
        "new UID probe tasks must not be stopped by old pod deletion"
    );
    drop(tasks);

    pm.stop_probes_for_uid("statefulset-ns", "ordinal-0", "new-uid")
        .await;
}

#[tokio::test]
async fn test_stop_probes_nonexistent_pod_is_noop() {
    let db = crate::datastore::test_support::in_memory().await;
    let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());
    let pm = ProbeManager::new(db_handle, "klights".to_string());
    // Should not panic or error
    pm.stop_probes("default", "nonexistent").await;
}

#[tokio::test]
async fn test_start_probes_multiple_containers_each_with_probes() {
    let db = crate::datastore::test_support::in_memory().await;
    let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());
    let pm = ProbeManager::new(db_handle, "klights".to_string());
    let pod = json!({
        "metadata": {"name": "multi", "namespace": "default"},
        "spec": {"containers": [
            {
                "name": "web",
                "image": "nginx",
                "readinessProbe": {"httpGet": {"port": 80}, "periodSeconds": 5}
            },
            {
                "name": "sidecar",
                "image": "envoy",
                "livenessProbe": {"tcpSocket": {"port": 15000}, "periodSeconds": 10}
            }
        ]},
        "status": {
            "phase": "Running",
            "podIP": "10.43.0.20",
            "containerStatuses": [
                {"name": "web", "containerID": "containerd://aaa"},
                {"name": "sidecar", "containerID": "containerd://bbb"}
            ]
        }
    });
    pm.start_probes(&pod).await.unwrap();
    let tasks = pm.tasks.read().await;
    let handles = tasks.get("default/multi").unwrap();
    assert_eq!(
        handles.len(),
        2,
        "Should have 1 readiness (web) + 1 liveness (sidecar)"
    );
    drop(tasks);
    pm.stop_probes("default", "multi").await;
}

#[tokio::test]
async fn test_update_pod_condition_readiness_toggle_preserves_transition_time() {
    let db = crate::datastore::test_support::in_memory().await;
    let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());
    create_test_pod(&db, "default", "toggle-pod", vec![
    json!({"type": "Ready", "status": "True", "reason": "ReadinessProbeSucceeded", "lastTransitionTime": "2026-01-01T00:00:00Z"})
]).await;

    // Toggle to false
    update_pod_condition(
        &db_handle,
        &fixture_pod_repository(&db_handle),
        "default/toggle-pod",
        "app",
        ProbeType::Readiness,
        false,
    )
    .await
    .unwrap();
    let cond = get_ready_condition(&db, "default", "toggle-pod")
        .await
        .unwrap();
    assert_eq!(cond["status"], "False");
    // lastTransitionTime should be updated (not the old value)
    assert_ne!(cond["lastTransitionTime"], "2026-01-01T00:00:00Z");

    // Toggle back to true
    update_pod_condition(
        &db_handle,
        &fixture_pod_repository(&db_handle),
        "default/toggle-pod",
        "app",
        ProbeType::Readiness,
        true,
    )
    .await
    .unwrap();
    let cond = get_ready_condition(&db, "default", "toggle-pod")
        .await
        .unwrap();
    assert_eq!(cond["status"], "True");
    assert_eq!(cond["reason"], "ReadinessProbeSucceeded");
}

#[tokio::test]
async fn test_update_pod_condition_startup_probe_is_noop() {
    // Startup probes don't update pod conditions directly (they gate liveness/readiness)
    let db = crate::datastore::test_support::in_memory().await;
    let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());
    create_test_pod(&db, "default", "startup-pod", vec![]).await;

    let result = update_pod_condition(
        &db_handle,
        &fixture_pod_repository(&db_handle),
        "default/startup-pod",
        "init",
        ProbeType::Startup,
        true,
    )
    .await;
    assert!(result.is_ok());

    // No Ready condition should be added by startup probe
    let cond = get_ready_condition(&db, "default", "startup-pod").await;
    assert!(
        cond.is_none(),
        "Startup probe should not create Ready condition"
    );
}

#[tokio::test]
async fn test_start_probes_with_startup_probe_spawns_task() {
    let db = crate::datastore::test_support::in_memory().await;
    let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());
    let pm = ProbeManager::new(db_handle.clone(), "klights-test".to_string());

    let pod = json!({
        "metadata": {"name": "probe-pod", "namespace": "default"},
        "spec": {
            "containers": [{
                "name": "app",
                "startupProbe": {
                    "httpGet": {"path": "/healthz", "port": 8080},
                    "initialDelaySeconds": 5,
                    "periodSeconds": 3,
                    "failureThreshold": 10
                }
            }]
        },
        "status": {
            "podIP": "10.43.0.5",
            "containerStatuses": [{"name": "app", "containerID": "containerd://abc123"}]
        }
    });

    // Create pod in DB first
    db.create_resource("v1", "Pod", Some("default"), "probe-pod", pod.clone())
        .await
        .unwrap();

    let result = pm.start_probes(&pod).await;
    assert!(
        result.is_ok(),
        "start_probes with startup probe should succeed"
    );

    // Verify tasks were spawned
    let tasks = pm.tasks.read().await;
    assert!(
        tasks.contains_key("default/probe-pod"),
        "Tasks should be registered for the pod"
    );
    let handles = tasks.get("default/probe-pod").unwrap();
    assert!(
        !handles.is_empty(),
        "At least one probe task should be spawned for startup probe"
    );

    // Clean up
    drop(tasks);
    pm.stop_probes("default", "probe-pod").await;
}

#[test]
fn test_parse_probe_params_with_all_values() {
    let probe_spec = json!({
        "initialDelaySeconds": 5,
        "periodSeconds": 15,
        "timeoutSeconds": 3,
        "failureThreshold": 5,
        "successThreshold": 2
    });

    let params = parse_probe_params(&probe_spec);

    assert_eq!(params.initial_delay, 5);
    assert_eq!(params.interval_secs, 15);
    assert_eq!(params.timeout_secs, 3);
    assert_eq!(params.failure_threshold, 5);
    assert_eq!(params.success_threshold, 2);
}

#[test]
fn test_parse_probe_params_with_defaults() {
    let probe_spec = json!({});

    let params = parse_probe_params(&probe_spec);

    assert_eq!(
        params.initial_delay, 0,
        "Default initialDelaySeconds should be 0"
    );
    assert_eq!(
        params.interval_secs, 10,
        "Default periodSeconds should be 10"
    );
    assert_eq!(params.timeout_secs, 1, "Default timeoutSeconds should be 1");
    assert_eq!(
        params.failure_threshold, 3,
        "Default failureThreshold should be 3"
    );
    assert_eq!(
        params.success_threshold, 1,
        "Default successThreshold should be 1"
    );
}

#[test]
fn test_parse_probe_params_with_partial_values() {
    let probe_spec = json!({
        "periodSeconds": 20,
        "failureThreshold": 10
    });

    let params = parse_probe_params(&probe_spec);

    assert_eq!(
        params.initial_delay, 0,
        "Missing initialDelaySeconds should default to 0"
    );
    assert_eq!(
        params.interval_secs, 20,
        "Specified periodSeconds should be 20"
    );
    assert_eq!(
        params.timeout_secs, 1,
        "Missing timeoutSeconds should default to 1"
    );
    assert_eq!(
        params.failure_threshold, 10,
        "Specified failureThreshold should be 10"
    );
    assert_eq!(
        params.success_threshold, 1,
        "Missing successThreshold should default to 1"
    );
}

#[test]
fn test_parse_probe_params_zero_values_use_k8s_defaults() {
    let probe_spec = json!({
        "periodSeconds": 0,
        "timeoutSeconds": 0,
        "failureThreshold": 0,
        "successThreshold": 0
    });

    let params = parse_probe_params(&probe_spec);

    assert_eq!(
        params.interval_secs, 10,
        "periodSeconds=0 should be treated as unset and default to 10"
    );
    assert_eq!(
        params.timeout_secs, 1,
        "timeoutSeconds=0 should be treated as unset and default to 1"
    );
    assert_eq!(
        params.failure_threshold, 3,
        "failureThreshold=0 should be treated as unset and default to 3"
    );
    assert_eq!(
        params.success_threshold, 1,
        "successThreshold=0 should be treated as unset and default to 1"
    );
}
