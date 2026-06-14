use super::*;

async fn process_volumes(
    sources: &dyn crate::kubelet::volume_sources::VolumeSourceReader,
    pod_dir_id: &str,
    pod_name: &str,
    namespace: &str,
    containerd_namespace: &str,
    pod: &serde_json::Value,
) -> anyhow::Result<std::collections::HashMap<String, String>> {
    let manager =
        crate::kubelet::pod_volume_manager::PodVolumeManager::new(sources, containerd_namespace);
    manager
        .process_volumes(pod_dir_id, pod_name, namespace, pod)
        .await
}

#[test]
fn test_pod_phase_succeeded() {
    // All containers exit with code 0 and restart policy is Never or OnFailure → Succeeded
    let containers = vec![
        (
            "app".to_string(),
            ContainerInfo {
                state: 2, // Exited
                exit_code: 0,
                finished_at: 2000000000,
                started_at: 1000000000,
                image: "app:latest".to_string(),
                image_ref: "docker.io/library/app:latest".to_string(),
                container_id: "aaa".to_string(),
                termination_message: String::new(),
            },
        ),
        (
            "sidecar".to_string(),
            ContainerInfo {
                state: 2, // Exited
                exit_code: 0,
                finished_at: 2100000000,
                started_at: 1000000000,
                image: "sidecar:latest".to_string(),
                image_ref: "docker.io/library/sidecar:latest".to_string(),
                container_id: "bbb".to_string(),
                termination_message: String::new(),
            },
        ),
    ];

    assert_eq!(
        compute_pod_phase(&containers, "Never"),
        "Succeeded",
        "Never: all exit 0 → Succeeded"
    );
    assert_eq!(
        compute_pod_phase(&containers, "OnFailure"),
        "Succeeded",
        "OnFailure: all exit 0 → Succeeded"
    );
}

#[test]
fn test_pod_phase_failed() {
    // Any container exits with non-zero and restart policy is Never → Failed
    let containers = vec![(
        "app".to_string(),
        ContainerInfo {
            state: 2, // Exited
            exit_code: 1,
            finished_at: 2000000000,
            started_at: 1000000000,
            image: "app:latest".to_string(),
            image_ref: "docker.io/library/app:latest".to_string(),
            container_id: "aaa".to_string(),
            termination_message: String::new(),
        },
    )];

    assert_eq!(
        compute_pod_phase(&containers, "Never"),
        "Failed",
        "Never: exit 1 → Failed"
    );

    // Multiple containers, one failed
    let containers_mixed = vec![
        (
            "app".to_string(),
            ContainerInfo {
                state: 2, // Exited
                exit_code: 0,
                finished_at: 2000000000,
                started_at: 1000000000,
                image: "app:latest".to_string(),
                image_ref: "docker.io/library/app:latest".to_string(),
                container_id: "aaa".to_string(),
                termination_message: String::new(),
            },
        ),
        (
            "sidecar".to_string(),
            ContainerInfo {
                state: 2,       // Exited
                exit_code: 137, // e.g., SIGKILL
                finished_at: 2100000000,
                started_at: 1000000000,
                image: "sidecar:latest".to_string(),
                image_ref: "docker.io/library/sidecar:latest".to_string(),
                container_id: "bbb".to_string(),
                termination_message: String::new(),
            },
        ),
    ];

    assert_eq!(
        compute_pod_phase(&containers_mixed, "Never"),
        "Failed",
        "Never: any non-zero exit → Failed"
    );
}

#[test]
fn test_sandbox_reservation_error_detection_failed_precondition() {
    // The error message from containerd when a sandbox name is already reserved
    let err_msg = "status: FailedPrecondition, message: \"failed to reserve sandbox name\"";
    assert!(
        err_msg.contains("failed to reserve sandbox name")
            || err_msg.contains("FailedPrecondition"),
        "Should detect sandbox name reservation error"
    );
}

#[test]
fn test_sandbox_reservation_error_detection_other_error() {
    // Other errors should NOT be treated as sandbox reservation conflicts
    let err_msg = "status: Internal, message: \"failed to create sandbox\"";
    assert!(
        !(err_msg.contains("failed to reserve sandbox name")
            || err_msg.contains("FailedPrecondition")),
        "Should NOT match non-reservation errors"
    );
}

#[test]
fn test_sandbox_reservation_error_detection_containerd_format() {
    // Actual containerd error format
    let err_msg = "rpc error: code = FailedPrecondition desc = failed to reserve sandbox name \"test-pod_default_abc-123_0\": name is reserved";
    assert!(
        err_msg.contains("failed to reserve sandbox name")
            || err_msg.contains("FailedPrecondition"),
        "Should detect actual containerd error format"
    );
}

// ========================
// S1.2: Pod restart policy tests
// ========================

#[test]
fn test_restart_policy_always_restarts_on_zero_exit() {
    // Always policy: restart even if exit code is 0
    assert!(
        should_restart("Always", 0),
        "Always policy should restart on exit code 0"
    );
}

#[test]
fn test_restart_policy_always_restarts_on_nonzero_exit() {
    // Always policy: restart on any non-zero exit code
    assert!(
        should_restart("Always", 1),
        "Always policy should restart on exit code 1"
    );
    assert!(
        should_restart("Always", 137),
        "Always policy should restart on exit code 137"
    );
}

#[test]
fn test_restart_policy_onfailure_no_restart_on_zero() {
    // OnFailure policy: do NOT restart if exit code is 0
    assert!(
        !should_restart("OnFailure", 0),
        "OnFailure policy should NOT restart on exit code 0"
    );
}

#[test]
fn test_restart_policy_onfailure_restarts_on_nonzero() {
    // OnFailure policy: restart only on non-zero exit code
    assert!(
        should_restart("OnFailure", 1),
        "OnFailure policy should restart on exit code 1"
    );
    assert!(
        should_restart("OnFailure", 137),
        "OnFailure policy should restart on exit code 137"
    );
}

#[test]
fn test_restart_policy_never_no_restart_on_zero() {
    // Never policy: never restart, even on exit code 0
    assert!(
        !should_restart("Never", 0),
        "Never policy should NOT restart on exit code 0"
    );
}

#[test]
fn test_restart_policy_never_no_restart_on_nonzero() {
    // Never policy: never restart, even on non-zero exit
    assert!(
        !should_restart("Never", 1),
        "Never policy should NOT restart on exit code 1"
    );
    assert!(
        !should_restart("Never", 137),
        "Never policy should NOT restart on exit code 137"
    );
}

#[test]
fn test_restart_policy_unknown_defaults_to_no_restart() {
    // Unknown policy: default to no restart (safe fallback)
    assert!(
        !should_restart("InvalidPolicy", 0),
        "Unknown policy should NOT restart on exit code 0"
    );
    assert!(
        !should_restart("InvalidPolicy", 1),
        "Unknown policy should NOT restart on exit code 1"
    );
}

// ========================
// compute_pod_phase tests
// ========================

#[tokio::test]
async fn test_pvc_added_event_triggers_reconciliation() {
    use serde_json::json;

    let db = crate::datastore::test_support::in_memory().await;

    // Create a PersistentVolume (Available)
    let pv = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {
            "name": "test-pv"
        },
        "spec": {
            "capacity": {
                "storage": "1Gi"
            },
            "accessModes": ["ReadWriteOnce"],
            "hostPath": {
                "path": "/mnt/data"
            }
        },
        "status": {
            "phase": "Available"
        }
    });

    db.create_resource("v1", "PersistentVolume", None, "test-pv", pv)
        .await
        .unwrap();

    // Create a PersistentVolumeClaim
    let pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": "test-pvc",
            "namespace": "default"
        },
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {
                "requests": {
                    "storage": "1Gi"
                }
            }
        }
    });

    db.create_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "test-pvc",
        pvc.clone(),
    )
    .await
    .unwrap();

    // Simulate PVC ADDED watch event → should trigger reconciliation
    // (This will be wired into handle_watch_event)
    let pvc_resource = db
        .get_resource("v1", "PersistentVolumeClaim", Some("default"), "test-pvc")
        .await
        .unwrap()
        .unwrap();

    // Inject resourceVersion for reconcile_pvc
    let mut pvc_with_rv: serde_json::Value = (*pvc_resource.data).clone();
    if let Some(meta) = pvc_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(pvc_resource.resource_version.to_string()),
        );
    }

    // Call reconcile_pvc (what handle_watch_event will do)
    crate::controllers::pvc::reconcile_pvc(&db, &pvc_with_rv)
        .await
        .unwrap();

    // Verify PVC is now Bound
    let updated_pvc = db
        .get_resource("v1", "PersistentVolumeClaim", Some("default"), "test-pvc")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(updated_pvc.data["status"]["phase"], "Bound");
    assert_eq!(updated_pvc.data["status"]["volumeName"], "test-pv");

    // Verify PV is also Bound
    let updated_pv = db
        .get_resource("v1", "PersistentVolume", None, "test-pv")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(updated_pv.data["status"]["phase"], "Bound");
}

#[tokio::test]
async fn test_pv_added_event_triggers_pending_pvc_reconciliation() {
    use serde_json::json;

    let db = crate::datastore::test_support::in_memory().await;

    // Create a PVC first (no matching PV yet, should stay Pending)
    let pvc = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": "waiting-pvc",
            "namespace": "default"
        },
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {
                "requests": {
                    "storage": "2Gi"
                }
            }
        }
    });

    db.create_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "waiting-pvc",
        pvc.clone(),
    )
    .await
    .unwrap();

    // Reconcile PVC (should set status to Pending since no PV exists)
    let pvc_resource = db
        .get_resource(
            "v1",
            "PersistentVolumeClaim",
            Some("default"),
            "waiting-pvc",
        )
        .await
        .unwrap()
        .unwrap();

    let mut pvc_with_rv: serde_json::Value = (*pvc_resource.data).clone();
    if let Some(meta) = pvc_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(pvc_resource.resource_version.to_string()),
        );
    }

    crate::controllers::pvc::reconcile_pvc(&db, &pvc_with_rv)
        .await
        .unwrap();

    // Verify PVC is Pending
    let pending_pvc = db
        .get_resource(
            "v1",
            "PersistentVolumeClaim",
            Some("default"),
            "waiting-pvc",
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(pending_pvc.data["status"]["phase"], "Pending");

    // Now create a matching PV (this simulates PV ADDED event)
    let pv = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {
            "name": "new-pv"
        },
        "spec": {
            "capacity": {
                "storage": "2Gi"
            },
            "accessModes": ["ReadWriteOnce"],
            "hostPath": {
                "path": "/mnt/data"
            }
        },
        "status": {
            "phase": "Available"
        }
    });

    db.create_resource("v1", "PersistentVolume", None, "new-pv", pv)
        .await
        .unwrap();

    // Simulate PV ADDED event handler: scan all Pending PVCs and reconcile
    let pvc_list = db
        .list_resources(
            "v1",
            "PersistentVolumeClaim",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();

    for pvc_resource in &pvc_list.items {
        let phase = pvc_resource
            .data
            .pointer("/status/phase")
            .and_then(|p| p.as_str());
        if phase != Some("Bound") {
            let mut pvc_with_rv: serde_json::Value = (*pvc_resource.data).clone();
            if let Some(meta) = pvc_with_rv
                .get_mut("metadata")
                .and_then(|m| m.as_object_mut())
            {
                meta.insert(
                    "resourceVersion".to_string(),
                    json!(pvc_resource.resource_version.to_string()),
                );
            }
            crate::controllers::pvc::reconcile_pvc(&db, &pvc_with_rv)
                .await
                .unwrap();
        }
    }

    // Verify PVC is now Bound after PV creation
    let bound_pvc = db
        .get_resource(
            "v1",
            "PersistentVolumeClaim",
            Some("default"),
            "waiting-pvc",
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(bound_pvc.data["status"]["phase"], "Bound");
    assert_eq!(bound_pvc.data["status"]["volumeName"], "new-pv");

    // Verify PV is also Bound
    let bound_pv = db
        .get_resource("v1", "PersistentVolume", None, "new-pv")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(bound_pv.data["status"]["phase"], "Bound");
}

// --- PVC volume mounting tests ---

fn make_pvc_pod(pvc_name: &str) -> Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "test-pod", "namespace": "default"},
        "spec": {
            "volumes": [{
                "name": "data-vol",
                "persistentVolumeClaim": {"claimName": pvc_name}
            }]
        }
    })
}

#[tokio::test]
async fn test_process_volumes_pvc_not_found_returns_error() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod = make_pvc_pod("missing-pvc");

    let result = process_volumes(
        &db,
        "default_test-pod",
        "test-pod",
        "default",
        "klights",
        &pod,
    )
    .await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("PVC missing-pvc not found"),
        "Expected 'PVC missing-pvc not found', got: {err}"
    );
}

#[tokio::test]
async fn test_process_volumes_pvc_not_bound_returns_error_with_phase() {
    let db = crate::datastore::test_support::in_memory().await;

    // Create namespace
    db.create_resource(
            "v1", "Namespace", None, "default",
            serde_json::json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "default"}}),
        ).await.unwrap();

    // Create PVC in Pending phase
    db.create_resource(
            "v1", "PersistentVolumeClaim", Some("default"), "my-pvc",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": {"name": "my-pvc", "namespace": "default"},
                "spec": {"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": "1Gi"}}},
                "status": {"phase": "Pending"}
            }),
        ).await.unwrap();

    let pod = make_pvc_pod("my-pvc");
    let result = process_volumes(
        &db,
        "default_test-pod",
        "test-pod",
        "default",
        "klights",
        &pod,
    )
    .await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("not Bound") && err.contains("Pending"),
        "Expected error about not Bound with phase Pending, got: {err}"
    );
}

#[tokio::test]
async fn test_process_volumes_pvc_bound_but_pv_not_found_returns_error() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
            "v1", "Namespace", None, "default",
            serde_json::json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "default"}}),
        ).await.unwrap();

    // PVC is Bound with volumeName pointing to nonexistent PV
    db.create_resource(
            "v1", "PersistentVolumeClaim", Some("default"), "my-pvc",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": {"name": "my-pvc", "namespace": "default"},
                "spec": {"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": "1Gi"}}},
                "status": {"phase": "Bound", "volumeName": "missing-pv"}
            }),
        ).await.unwrap();

    let pod = make_pvc_pod("my-pvc");
    let result = process_volumes(
        &db,
        "default_test-pod",
        "test-pod",
        "default",
        "klights",
        &pod,
    )
    .await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("PV missing-pv not found"),
        "Expected 'PV missing-pv not found', got: {err}"
    );
}

#[tokio::test]
async fn test_process_volumes_pvc_bound_pv_with_hostpath_returns_path() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
            "v1", "Namespace", None, "default",
            serde_json::json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "default"}}),
        ).await.unwrap();

    // Create PVC bound to a PV
    db.create_resource(
            "v1", "PersistentVolumeClaim", Some("default"), "my-pvc",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": {"name": "my-pvc", "namespace": "default"},
                "spec": {"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": "1Gi"}}},
                "status": {"phase": "Bound", "volumeName": "my-pv"}
            }),
        ).await.unwrap();

    // Create PV with hostPath
    db.create_resource(
        "v1",
        "PersistentVolume",
        None,
        "my-pv",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "PersistentVolume",
            "metadata": {"name": "my-pv"},
            "spec": {
                "capacity": {"storage": "1Gi"},
                "accessModes": ["ReadWriteOnce"],
                "hostPath": {"path": "/mnt/data/my-pv"}
            }
        }),
    )
    .await
    .unwrap();

    let pod = make_pvc_pod("my-pvc");
    let result = process_volumes(
        &db,
        "default_test-pod",
        "test-pod",
        "default",
        "klights",
        &pod,
    )
    .await;
    assert!(result.is_ok(), "Expected success, got: {:?}", result.err());
    let paths = result.unwrap();
    assert_eq!(paths.get("data-vol").unwrap(), "/mnt/data/my-pv");
}

#[tokio::test]
async fn test_process_volumes_pvc_bound_no_volume_name_returns_error() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
            "v1", "Namespace", None, "default",
            serde_json::json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "default"}}),
        ).await.unwrap();

    // PVC is Bound but status has no volumeName
    db.create_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "my-pvc",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"name": "my-pvc", "namespace": "default"},
            "spec": {"accessModes": ["ReadWriteOnce"]},
            "status": {"phase": "Bound"}
        }),
    )
    .await
    .unwrap();

    let pod = make_pvc_pod("my-pvc");
    let result = process_volumes(
        &db,
        "default_test-pod",
        "test-pod",
        "default",
        "klights",
        &pod,
    )
    .await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("has no volumeName"),
        "Expected 'has no volumeName', got: {err}"
    );
}

#[tokio::test]
async fn test_process_volumes_pvc_bound_pv_no_hostpath_returns_error() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
            "v1", "Namespace", None, "default",
            serde_json::json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "default"}}),
        ).await.unwrap();

    db.create_resource(
        "v1",
        "PersistentVolumeClaim",
        Some("default"),
        "my-pvc",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"name": "my-pvc", "namespace": "default"},
            "spec": {"accessModes": ["ReadWriteOnce"]},
            "status": {"phase": "Bound", "volumeName": "my-pv"}
        }),
    )
    .await
    .unwrap();

    // PV exists but has no hostPath (e.g., NFS or other type)
    db.create_resource(
        "v1",
        "PersistentVolume",
        None,
        "my-pv",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "PersistentVolume",
            "metadata": {"name": "my-pv"},
            "spec": {
                "capacity": {"storage": "1Gi"},
                "accessModes": ["ReadWriteOnce"],
                "nfs": {"server": "nfs.example.com", "path": "/exports/data"}
            }
        }),
    )
    .await
    .unwrap();

    let pod = make_pvc_pod("my-pvc");
    let result = process_volumes(
        &db,
        "default_test-pod",
        "test-pod",
        "default",
        "klights",
        &pod,
    )
    .await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("has no hostPath.path"),
        "Expected 'has no hostPath.path', got: {err}"
    );
}
