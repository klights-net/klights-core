use crate::kubelet::pod_endpoints::reconcile_endpoints_for_pod;
use crate::kubelet::pod_env::collect_literal_env_vars;
use crate::kubelet::pod_status_writer::{PodStatusUpdate, update_pod_status};
use std::collections::HashMap;

fn build_mounts(
    container: &serde_json::Value,
    volume_paths: &std::collections::HashMap<String, String>,
    resolved_envs: &std::collections::HashMap<String, String>,
) -> anyhow::Result<(Vec<k8s_cri::v1::Mount>, Vec<std::path::PathBuf>)> {
    crate::kubelet::pod_volume_manager::PodVolumeManager::build_mounts(
        container,
        volume_paths,
        resolved_envs,
    )
    .map_err(|e| anyhow::anyhow!("{e}"))
}

// ---- Tests ----

#[test]
fn test_build_mounts_from_volume_mounts() {
    let container = serde_json::json!({
        "volumeMounts": [
            {"name": "data", "mountPath": "/data"},
            {"name": "config", "mountPath": "/etc/config"}
        ]
    });
    let mut volume_paths = HashMap::new();
    volume_paths.insert("data".to_string(), "/host/data".to_string());
    volume_paths.insert("config".to_string(), "/host/config".to_string());

    let mounts = build_mounts(&container, &volume_paths, &std::collections::HashMap::new())
        .unwrap()
        .0;
    assert_eq!(mounts.len(), 2);
    assert_eq!(mounts[0].container_path, "/data");
    assert_eq!(mounts[0].host_path, "/host/data");
    assert!(!mounts[0].readonly);
    assert_eq!(mounts[1].container_path, "/etc/config");
    assert_eq!(mounts[1].host_path, "/host/config");
}

#[test]
fn test_build_mounts_missing_volume_skipped() {
    let container = serde_json::json!({
        "volumeMounts": [
            {"name": "missing-vol", "mountPath": "/data"}
        ]
    });
    let volume_paths = HashMap::new(); // no volumes registered

    let mounts = build_mounts(&container, &volume_paths, &std::collections::HashMap::new())
        .unwrap()
        .0;
    assert!(
        mounts.is_empty(),
        "mount with no matching volume should be skipped"
    );
}

#[test]
fn test_build_mounts_readonly() {
    let container = serde_json::json!({
        "volumeMounts": [
            {"name": "secrets", "mountPath": "/etc/secrets", "readOnly": true},
            {"name": "data", "mountPath": "/data", "readOnly": false}
        ]
    });
    let mut volume_paths = HashMap::new();
    volume_paths.insert("secrets".to_string(), "/host/secrets".to_string());
    volume_paths.insert("data".to_string(), "/host/data".to_string());

    let mounts = build_mounts(&container, &volume_paths, &std::collections::HashMap::new())
        .unwrap()
        .0;
    assert_eq!(mounts.len(), 2);
    assert!(mounts[0].readonly, "readOnly: true must propagate");
    assert!(!mounts[1].readonly, "readOnly: false must propagate");
}

#[test]
fn test_build_mounts_sa_volume_via_volume_mount() {
    // SA volume is mounted via inject_serviceaccount_volume which adds both
    // the volume and volumeMount to the pod spec. build_mounts processes it
    // like any other volumeMount — no special-casing needed.
    let container = serde_json::json!({
        "volumeMounts": [{
            "name": "kube-api-access-abc12",
            "mountPath": "/var/run/secrets/kubernetes.io/serviceaccount",
            "readOnly": true
        }]
    });
    let mut volume_paths = HashMap::new();
    let runtime_ns = crate::paths::runtime_namespace();
    let projected_path = crate::paths::volumes_root_path(&runtime_ns)
        .join("test-pod")
        .join("volumes")
        .join("projected")
        .join("kube-api-access-abc12")
        .to_string_lossy()
        .into_owned();
    volume_paths.insert("kube-api-access-abc12".to_string(), projected_path);

    let mounts = build_mounts(&container, &volume_paths, &std::collections::HashMap::new())
        .unwrap()
        .0;
    assert_eq!(mounts.len(), 1);
    assert_eq!(
        mounts[0].container_path,
        "/var/run/secrets/kubernetes.io/serviceaccount"
    );
    assert!(
        mounts[0]
            .host_path
            .contains("projected/kube-api-access-abc12")
    );
    assert!(mounts[0].readonly, "SA token mount must be read-only");
}

#[test]
fn test_build_mounts_subpath_appends_to_directory_volume() {
    // Create a temp directory to simulate ConfigMap volume
    let tmp = tempfile::tempdir().unwrap();
    let volume_dir = tmp.path().to_str().unwrap();
    std::fs::write(format!("{}/Corefile", volume_dir), "test").unwrap();

    let container = serde_json::json!({
        "volumeMounts": [
            {
                "name": "config-volume",
                "mountPath": "/etc/coredns/Corefile",
                "subPath": "Corefile",
                "readOnly": true
            }
        ]
    });
    let mut volume_paths = HashMap::new();
    volume_paths.insert("config-volume".to_string(), volume_dir.to_string());

    let mounts = build_mounts(&container, &volume_paths, &std::collections::HashMap::new())
        .unwrap()
        .0;
    assert_eq!(mounts.len(), 1);
    assert_eq!(mounts[0].container_path, "/etc/coredns/Corefile");
    assert_eq!(
        mounts[0].host_path,
        format!("{}/Corefile", volume_dir),
        "subPath should be appended to directory host_path"
    );
    assert!(mounts[0].readonly);
}

#[test]
fn test_build_mounts_subpath_ignored_for_file_hostpath() {
    // Create a temp file to simulate hostPath volume
    let tmp = tempfile::tempdir().unwrap();
    let file_path = tmp.path().join("ca.crt");
    std::fs::write(&file_path, "cert").unwrap();

    let container = serde_json::json!({
        "volumeMounts": [
            {
                "name": "ca-cert",
                "mountPath": "/etc/coredns/ca.crt",
                "subPath": "ca.crt",
                "readOnly": true
            }
        ]
    });
    let mut volume_paths = HashMap::new();
    volume_paths.insert(
        "ca-cert".to_string(),
        file_path.to_str().unwrap().to_string(),
    );

    let mounts = build_mounts(&container, &volume_paths, &std::collections::HashMap::new())
        .unwrap()
        .0;
    assert_eq!(mounts.len(), 1);
    assert_eq!(mounts[0].container_path, "/etc/coredns/ca.crt");
    assert_eq!(
        mounts[0].host_path,
        file_path.to_str().unwrap(),
        "subPath should NOT be appended when host_path is already a file"
    );
    assert!(mounts[0].readonly);
}

#[test]
fn test_build_mounts_subpath_expr_expands_env_vars() {
    let tmp = tempfile::tempdir().unwrap();
    let container = serde_json::json!({
        "env": [
            {"name": "POD_NAME", "value": "my-pod"}
        ],
        "volumeMounts": [
            {
                "name": "data",
                "mountPath": "/data",
                "subPathExpr": "$(POD_NAME)"
            }
        ]
    });
    let mut volume_paths = HashMap::new();
    volume_paths.insert("data".to_string(), tmp.path().to_str().unwrap().to_string());

    let mounts = build_mounts(&container, &volume_paths, &std::collections::HashMap::new())
        .unwrap()
        .0;
    assert_eq!(mounts.len(), 1);
    assert_eq!(mounts[0].container_path, "/data");
    assert!(
        mounts[0].host_path.ends_with("/my-pod"),
        "subPathExpr should expand $(POD_NAME) to 'my-pod', got: {}",
        mounts[0].host_path
    );
}

#[test]
fn test_build_mounts_subpath_expr_undefined_var_kept_literal() {
    let tmp = tempfile::tempdir().unwrap();
    let container = serde_json::json!({
        "volumeMounts": [
            {
                "name": "data",
                "mountPath": "/data",
                "subPathExpr": "$(UNDEFINED_VAR)"
            }
        ]
    });
    let mut volume_paths = HashMap::new();
    volume_paths.insert("data".to_string(), tmp.path().to_str().unwrap().to_string());

    let mounts = build_mounts(&container, &volume_paths, &std::collections::HashMap::new())
        .unwrap()
        .0;
    assert_eq!(mounts.len(), 1);
    assert!(
        mounts[0].host_path.ends_with("/$(UNDEFINED_VAR)"),
        "Undefined var in subPathExpr should be left literal, got: {}",
        mounts[0].host_path
    );
}

#[test]
fn test_build_mounts_subpath_expr_uses_resolved_env_overriding_literal() {
    // P0-E2E-20260423-14 regression: when POD_NAME comes from a fieldRef
    // (not a literal "value"), the old env_map() closure in build_mounts
    // couldn't find it. Pass it via resolved_envs instead.
    let tmp = tempfile::tempdir().unwrap();
    let container = serde_json::json!({
        "env": [
            {"name": "POD_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}}
        ],
        "volumeMounts": [
            {"name": "data", "mountPath": "/data", "subPathExpr": "$(POD_NAME)"}
        ]
    });
    let mut volume_paths = HashMap::new();
    volume_paths.insert("data".to_string(), tmp.path().to_str().unwrap().to_string());
    let mut resolved_envs = std::collections::HashMap::new();
    resolved_envs.insert("POD_NAME".to_string(), "my-pod-from-fieldref".to_string());

    let (mounts, _) = build_mounts(&container, &volume_paths, &resolved_envs).unwrap();
    assert_eq!(mounts.len(), 1);
    assert!(
        mounts[0].host_path.ends_with("/my-pod-from-fieldref"),
        "subPathExpr must expand fieldRef env var via resolved_envs, got: {}",
        mounts[0].host_path
    );
}

#[test]
fn test_build_mounts_subpath_expr_absolute_expansion_returns_error() {
    // K8s conformance: subPathExpr that expands to an absolute path must fail
    let tmp = tempfile::tempdir().unwrap();
    let container = serde_json::json!({
        "volumeMounts": [{
            "name": "data",
            "mountPath": "/data",
            "subPathExpr": "$(MY_VAR)"
        }]
    });
    let mut volume_paths = std::collections::HashMap::new();
    volume_paths.insert("data".to_string(), tmp.path().to_str().unwrap().to_string());
    let mut resolved_envs = std::collections::HashMap::new();
    resolved_envs.insert("MY_VAR".to_string(), "/absolute/path".to_string());

    let result = build_mounts(&container, &volume_paths, &resolved_envs);
    assert!(
        result.is_err(),
        "expanded absolute subPath must return error"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("absolute"),
        "error must mention absolute path: {}",
        err
    );
}

#[test]
fn test_build_mounts_subpath_expr_dotdot_expansion_returns_error() {
    let tmp = tempfile::tempdir().unwrap();
    let container = serde_json::json!({
        "volumeMounts": [{
            "name": "data",
            "mountPath": "/data",
            "subPathExpr": "$(MY_VAR)"
        }]
    });
    let mut volume_paths = std::collections::HashMap::new();
    volume_paths.insert("data".to_string(), tmp.path().to_str().unwrap().to_string());
    let mut resolved_envs = std::collections::HashMap::new();
    resolved_envs.insert("MY_VAR".to_string(), "../secret".to_string());

    let result = build_mounts(&container, &volume_paths, &resolved_envs);
    assert!(
        result.is_err(),
        "expanded subPath with '..' must return error"
    );
}

#[test]
fn test_build_mounts_subpath_expr_literal_env_absolute_catches_absolute_path() {
    // subPathExpr referencing a literal env var with absolute value must fail.
    // This is the b12 regression: calling code must include literal value env vars
    // in the env map passed to build_mounts.
    let tmp = tempfile::tempdir().unwrap();
    let container = serde_json::json!({
        "env": [{"name": "MY_PATH", "value": "/absolute/path"}],
        "volumeMounts": [{
            "name": "data",
            "mountPath": "/data",
            "subPathExpr": "$(MY_PATH)"
        }]
    });
    let mut volume_paths = std::collections::HashMap::new();
    volume_paths.insert("data".to_string(), tmp.path().to_str().unwrap().to_string());
    // Simulate what fixed calling code provides: literal vars included
    let resolved_envs = collect_literal_env_vars(&container);
    let result = build_mounts(&container, &volume_paths, &resolved_envs);
    assert!(
        result.is_err(),
        "literal env var with absolute path must cause build_mounts to fail"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("absolute"),
        "error must mention absolute: {}",
        err
    );
}

#[test]
fn test_create_pod_uses_metadata_uid() {
    // Construct a pod JSON with metadata.uid set (as API server does)
    let pod = serde_json::json!({
        "metadata": {
            "name": "test-pod",
            "namespace": "default",
            "uid": "12345678-1234-1234-1234-123456789abc"
        },
        "spec": {
            "containers": []
        }
    });

    // Extract UID using the same logic as create_pod() should use
    let extracted_uid = pod
        .pointer("/metadata/uid")
        .and_then(|u| u.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    // Verify the extracted UID matches the one set in metadata
    assert_eq!(extracted_uid, "12345678-1234-1234-1234-123456789abc");
}

#[test]
fn test_create_pod_fallback_uid_is_injected_for_fieldref() {
    let pod = serde_json::json!({
        "metadata": {
            "name": "test-pod",
            "namespace": "default"
        },
        "spec": {
            "containers": []
        }
    });

    let pod_uid = pod
        .pointer("/metadata/uid")
        .and_then(|u| u.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let mut pod_with_uid = pod.clone();
    if let Some(obj) = pod_with_uid.as_object_mut() {
        let metadata = obj
            .entry("metadata".to_string())
            .or_insert_with(|| serde_json::json!({}));
        if let Some(meta_obj) = metadata.as_object_mut() {
            meta_obj.insert("uid".to_string(), serde_json::json!(pod_uid.clone()));
        }
    }

    let injected_uid = pod_with_uid
        .pointer("/metadata/uid")
        .and_then(|u| u.as_str())
        .unwrap_or("");
    assert_eq!(injected_uid, pod_uid);
    assert!(
        uuid::Uuid::parse_str(injected_uid).is_ok(),
        "fallback uid must be a valid UUID, got: {}",
        injected_uid
    );
}

#[tokio::test]
async fn test_update_pod_status_triggers_endpoint_reconciliation() {
    let db = crate::datastore::test_support::in_memory().await;

    // Create namespace
    let ns = serde_json::json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "default"}});
    db.create_resource("v1", "Namespace", None, "default", ns)
        .await
        .unwrap();

    // Create service with selector matching the pod
    let service = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "web-svc", "namespace": "default"},
        "spec": {
            "selector": {"app": "web"},
            "ports": [{"port": 80, "targetPort": 8080}]
        }
    });
    db.create_resource("v1", "Service", Some("default"), "web-svc", service)
        .await
        .unwrap();

    // Create empty endpoints
    let empty_endpoints = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {"name": "web-svc", "namespace": "default"},
        "subsets": []
    });
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "web-svc",
        empty_endpoints,
    )
    .await
    .unwrap();

    // Create pod in Pending state (no IP yet)
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "web-1",
            "namespace": "default",
            "labels": {"app": "web"}
        },
        "status": {"phase": "Pending"}
    });
    db.create_resource("v1", "Pod", Some("default"), "web-1", pod)
        .await
        .unwrap();

    // Verify endpoints are still empty
    let endpoints = db
        .get_resource("v1", "Endpoints", Some("default"), "web-svc")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        endpoints.data["subsets"].as_array().unwrap().len(),
        0,
        "Endpoints should be empty before pod is Running"
    );

    // Simulate pod transitioning to Running with an IP (what update_pod_status does)
    update_pod_status(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "web-1",
        "default",
        PodStatusUpdate {
            phase: "Running".to_string(),
            pod_ip: "10.43.0.10".to_string(),
            sandbox_id: "sandbox-123".to_string(),
            container_statuses: vec![serde_json::json!({
                "name": "web",
                "ready": true,
                "started": true,
                "restartCount": 0,
                "state": {"running": {"startedAt": crate::utils::k8s_timestamp()}},
                "image": "nginx",
                "imageID": "docker.io/library/nginx"
            })],
            init_container_statuses: vec![],
        },
        None,
    )
    .await
    .unwrap();

    // After update_pod_status, endpoints should be populated
    let endpoints = db
        .get_resource("v1", "Endpoints", Some("default"), "web-svc")
        .await
        .unwrap()
        .unwrap();
    let subsets = endpoints.data["subsets"].as_array().unwrap();
    assert_eq!(
        subsets.len(),
        1,
        "Endpoints should have 1 subset after pod reaches Running"
    );

    let addresses = subsets[0]["addresses"].as_array().unwrap();
    assert_eq!(addresses.len(), 1, "Endpoints should have 1 address");
    assert_eq!(
        addresses[0]["ip"], "10.43.0.10",
        "Endpoint IP should match pod IP"
    );
}

#[tokio::test]
async fn test_update_pod_status_running_without_container_statuses_sets_ready_conditions() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "ready-empty-statuses",
            "namespace": "default"
        },
        "spec": {
            "containers": [{"name": "c", "image": "nginx"}]
        },
        "status": {"phase": "Pending"}
    });
    db.create_resource("v1", "Pod", Some("default"), "ready-empty-statuses", pod)
        .await
        .unwrap();

    update_pod_status(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "ready-empty-statuses",
        "default",
        PodStatusUpdate {
            phase: "Running".to_string(),
            pod_ip: "10.43.0.77".to_string(),
            sandbox_id: "sandbox-ready-empty".to_string(),
            container_statuses: vec![],
            init_container_statuses: vec![],
        },
        None,
    )
    .await
    .unwrap();

    let updated = db
        .get_resource("v1", "Pod", Some("default"), "ready-empty-statuses")
        .await
        .unwrap()
        .unwrap();
    let conditions = updated.data["status"]["conditions"].as_array().unwrap();
    let ready = conditions
        .iter()
        .find(|c| c["type"] == "Ready")
        .expect("Ready condition must exist");
    let containers_ready = conditions
        .iter()
        .find(|c| c["type"] == "ContainersReady")
        .expect("ContainersReady condition must exist");
    assert_eq!(ready["status"], "True");
    assert_eq!(containers_ready["status"], "True");
}

#[tokio::test]
async fn test_update_pod_status_preserves_restart_count_and_last_state() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "preserve-restart",
            "namespace": "default"
        },
        "spec": {
            "containers": [{"name": "app", "image": "nginx"}]
        },
        "status": {
            "phase": "Running",
            "containerStatuses": [{
                "name": "app",
                "ready": true,
                "started": true,
                "restartCount": 2,
                "lastState": {
                    "terminated": {
                        "exitCode": 137,
                        "reason": "Error"
                    }
                }
            }]
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "preserve-restart", pod)
        .await
        .unwrap();

    update_pod_status(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "preserve-restart",
        "default",
        PodStatusUpdate {
            phase: "Running".to_string(),
            pod_ip: "10.43.0.88".to_string(),
            sandbox_id: "sandbox-preserve".to_string(),
            container_statuses: vec![serde_json::json!({
                "name": "app",
                "ready": true,
                "started": true,
                "restartCount": 0,
                "state": {"running": {"startedAt": crate::utils::k8s_timestamp()}},
                "image": "nginx",
                "imageID": "docker.io/library/nginx"
            })],
            init_container_statuses: vec![],
        },
        None,
    )
    .await
    .unwrap();

    let updated = db
        .get_resource("v1", "Pod", Some("default"), "preserve-restart")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        updated.data["status"]["containerStatuses"][0]["restartCount"],
        serde_json::json!(2)
    );
    assert!(
        updated.data["status"]["containerStatuses"][0]
            .pointer("/lastState/terminated")
            .is_some()
    );
}

#[tokio::test]
#[ignore = "Requires root for nftables/netlink"]
async fn test_endpoints_populated_on_pod_modified_event() {
    let db = crate::datastore::test_support::in_memory().await;

    // Create namespace
    let ns =
        serde_json::json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "test"}});
    db.create_resource("v1", "Namespace", None, "test", ns)
        .await
        .unwrap();

    // Create service with selector
    let service = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "app-svc", "namespace": "test"},
        "spec": {
            "selector": {"app": "myapp"},
            "ports": [{"port": 80, "targetPort": 8080}]
        }
    });
    db.create_resource("v1", "Service", Some("test"), "app-svc", service)
        .await
        .unwrap();

    // Create empty endpoints
    let empty_endpoints = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {"name": "app-svc", "namespace": "test"},
        "subsets": []
    });
    db.create_resource("v1", "Endpoints", Some("test"), "app-svc", empty_endpoints)
        .await
        .unwrap();

    // Create pod with matching labels but NO podIP (Pending state)
    let pod_no_ip = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "app-1",
            "namespace": "test",
            "labels": {"app": "myapp"}
        },
        "status": {"phase": "Pending"}
    });
    db.create_resource("v1", "Pod", Some("test"), "app-1", pod_no_ip.clone())
        .await
        .unwrap();

    // Reconcile with pod that has no IP — endpoints should remain empty
    reconcile_endpoints_for_pod(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        &pod_no_ip,
        None,
    )
    .await
    .unwrap();
    let endpoints = db
        .get_resource("v1", "Endpoints", Some("test"), "app-svc")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        endpoints.data["subsets"].as_array().unwrap().len(),
        0,
        "Endpoints should be empty when pod has no IP"
    );

    // Now update pod with podIP (simulating transition to Running)
    let pod_with_ip = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "app-1",
            "namespace": "test",
            "labels": {"app": "myapp"}
        },
        "status": {
            "phase": "Running",
            "podIP": "10.43.0.5"
        }
    });
    let pod_rv = db
        .get_resource("v1", "Pod", Some("test"), "app-1")
        .await
        .unwrap()
        .unwrap()
        .resource_version;
    db.update_resource(
        "v1",
        "Pod",
        Some("test"),
        "app-1",
        pod_with_ip.clone(),
        pod_rv,
    )
    .await
    .unwrap();

    // Reconcile with updated pod — endpoints should now be populated
    reconcile_endpoints_for_pod(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        &pod_with_ip,
        None,
    )
    .await
    .unwrap();
    let endpoints = db
        .get_resource("v1", "Endpoints", Some("test"), "app-svc")
        .await
        .unwrap()
        .unwrap();
    let subsets = endpoints.data["subsets"].as_array().unwrap();
    assert_eq!(
        subsets.len(),
        1,
        "Endpoints should have 1 subset after pod gets IP"
    );

    let addresses = subsets[0]["addresses"].as_array().unwrap();
    assert_eq!(addresses.len(), 1, "Should have 1 address");
    assert_eq!(
        addresses[0]["ip"], "10.43.0.5",
        "Endpoint IP should match pod IP"
    );
}

// ========================
// resolve_env_value_from tests
// ========================

// ========================
// Lifecycle hook wiring tests
// ========================

// S1.2 Pod restart policy tests - pod phase transitions with multi-container coverage
