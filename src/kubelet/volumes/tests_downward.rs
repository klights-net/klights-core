use super::*;

#[tokio::test]
async fn test_downward_api_volume_creates_file_with_metadata_name() {
    use serde_json::json;
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    // Create test pod
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "test-pod",
            "namespace": "default",
            "uid": "abc-123"
        },
        "spec": {
            "containers": []
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "test-pod", pod)
        .await
        .unwrap();

    let items = json!([
        {"path": "pod-name", "fieldRef": {"fieldPath": "metadata.name"}}
    ]);

    let volume_path =
        create_downward_api_volume_at(root, &db, "default", "test-pod", "podinfo", None, &items)
            .await
            .unwrap();

    let content = crate::utils::read_utf8_file(format!("{}/pod-name", volume_path)).unwrap();
    assert_eq!(content, "test-pod");
}

#[tokio::test]
async fn test_downward_api_create_uses_keyed_blocking_boundary() {
    use serde_json::json;
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "test-pod",
            "namespace": "default",
            "uid": "abc-123"
        },
        "spec": {"containers": []}
    });
    db.create_resource("v1", "Pod", Some("default"), "test-pod", pod)
        .await
        .unwrap();

    let items = json!([
        {"path": "pod-name", "fieldRef": {"fieldPath": "metadata.name"}}
    ]);

    let before = blocking_fs_keyed_call_count();
    let _ =
        create_downward_api_volume_at(root, &db, "default", "test-pod", "podinfo", None, &items)
            .await
            .unwrap();
    assert!(
        blocking_fs_keyed_call_count() > before,
        "downwardAPI create must run through keyed blocking filesystem boundary"
    );
}

#[tokio::test]
async fn test_downward_api_volume_metadata_namespace() {
    use serde_json::json;
    let db = crate::datastore::test_support::in_memory().await;

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "my-pod",
            "namespace": "kube-system",
            "uid": "xyz"
        },
        "spec": {"containers": []}
    });
    db.create_resource("v1", "Pod", Some("kube-system"), "my-pod", pod)
        .await
        .unwrap();

    let pod_res = db
        .get_resource("v1", "Pod", Some("kube-system"), "my-pod")
        .await
        .unwrap()
        .unwrap();
    let ns = extract_field_ref(&pod_res.data, "metadata.namespace").unwrap();
    assert_eq!(ns, "kube-system");
}

#[tokio::test]
async fn test_downward_api_volume_metadata_labels() {
    use serde_json::json;
    let db = crate::datastore::test_support::in_memory().await;

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "labeled-pod",
            "namespace": "default",
            "labels": {
                "app": "nginx",
                "version": "v1.2.3"
            }
        },
        "spec": {"containers": []}
    });
    db.create_resource("v1", "Pod", Some("default"), "labeled-pod", pod)
        .await
        .unwrap();

    let pod_res = db
        .get_resource("v1", "Pod", Some("default"), "labeled-pod")
        .await
        .unwrap()
        .unwrap();
    let labels = extract_field_ref(&pod_res.data, "metadata.labels").unwrap();
    assert!(labels.contains("app=\"nginx\""));
    assert!(labels.contains("version=\"v1.2.3\""));
}

#[tokio::test]
async fn test_downward_api_volume_metadata_annotations() {
    use serde_json::json;
    let db = crate::datastore::test_support::in_memory().await;

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "annotated-pod",
            "namespace": "default",
            "annotations": {
                "prometheus.io/scrape": "true",
                "prometheus.io/port": "9090"
            }
        },
        "spec": {"containers": []}
    });
    db.create_resource("v1", "Pod", Some("default"), "annotated-pod", pod)
        .await
        .unwrap();

    let pod_res = db
        .get_resource("v1", "Pod", Some("default"), "annotated-pod")
        .await
        .unwrap()
        .unwrap();
    let annotations = extract_field_ref(&pod_res.data, "metadata.annotations").unwrap();
    assert!(annotations.contains("prometheus.io/scrape=\"true\""));
    assert!(annotations.contains("prometheus.io/port=\"9090\""));
}

#[tokio::test]
async fn test_downward_api_volume_status_pod_ip() {
    use serde_json::json;
    let db = crate::datastore::test_support::in_memory().await;

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "pod-with-ip",
            "namespace": "default"
        },
        "spec": {"containers": []},
        "status": {
            "podIP": "10.42.0.5"
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "pod-with-ip", pod)
        .await
        .unwrap();

    let pod_res = db
        .get_resource("v1", "Pod", Some("default"), "pod-with-ip")
        .await
        .unwrap()
        .unwrap();
    let ip = extract_field_ref(&pod_res.data, "status.podIP").unwrap();
    assert_eq!(ip, "10.42.0.5");
}

#[tokio::test]
async fn test_downward_api_volume_spec_node_name() {
    use serde_json::json;
    let db = crate::datastore::test_support::in_memory().await;

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "scheduled-pod",
            "namespace": "default"
        },
        "spec": {
            "nodeName": "node-01",
            "containers": []
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "scheduled-pod", pod)
        .await
        .unwrap();

    let pod_res = db
        .get_resource("v1", "Pod", Some("default"), "scheduled-pod")
        .await
        .unwrap()
        .unwrap();
    let node = extract_field_ref(&pod_res.data, "spec.nodeName").unwrap();
    assert_eq!(node, "node-01");
}

#[tokio::test]
async fn test_downward_api_volume_spec_service_account_name() {
    use serde_json::json;
    let db = crate::datastore::test_support::in_memory().await;

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "sa-pod",
            "namespace": "default"
        },
        "spec": {
            "serviceAccountName": "my-service-account",
            "containers": []
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "sa-pod", pod)
        .await
        .unwrap();

    let pod_res = db
        .get_resource("v1", "Pod", Some("default"), "sa-pod")
        .await
        .unwrap()
        .unwrap();
    let sa = extract_field_ref(&pod_res.data, "spec.serviceAccountName").unwrap();
    assert_eq!(sa, "my-service-account");
}

#[tokio::test]
async fn test_downward_api_volume_resource_field_ref_limits_cpu() {
    use serde_json::json;
    let db = crate::datastore::test_support::in_memory().await;

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "resource-pod",
            "namespace": "default"
        },
        "spec": {
            "containers": [{
                "name": "app",
                "image": "nginx",
                "resources": {
                    "limits": {
                        "cpu": "500m",
                        "memory": "256Mi"
                    },
                    "requests": {
                        "cpu": "100m",
                        "memory": "128Mi"
                    }
                }
            }]
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "resource-pod", pod)
        .await
        .unwrap();

    let pod_res = db
        .get_resource("v1", "Pod", Some("default"), "resource-pod")
        .await
        .unwrap()
        .unwrap();
    // K8s downward API converts quantities: CPU → whole cores (ceiling), memory → bytes
    let cpu = extract_resource_field_ref(&pod_res.data, Some("app"), "limits.cpu").unwrap();
    assert_eq!(cpu, "1"); // 500m → ceil(500/1000) = 1 core

    let mem = extract_resource_field_ref(&pod_res.data, Some("app"), "limits.memory").unwrap();
    assert_eq!(mem, "268435456"); // 256Mi → 268435456 bytes

    let req_cpu = extract_resource_field_ref(&pod_res.data, Some("app"), "requests.cpu").unwrap();
    assert_eq!(req_cpu, "1"); // 100m → ceil(100/1000) = 1 core

    let req_mem =
        extract_resource_field_ref(&pod_res.data, Some("app"), "requests.memory").unwrap();
    assert_eq!(req_mem, "134217728"); // 128Mi → 134217728 bytes
}

#[tokio::test]
async fn test_downward_api_volume_resource_field_ref_default_container() {
    use serde_json::json;
    let db = crate::datastore::test_support::in_memory().await;

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "multi-container-pod",
            "namespace": "default"
        },
        "spec": {
            "containers": [
                {
                    "name": "first",
                    "image": "nginx",
                    "resources": {
                        "limits": {"cpu": "200m"}
                    }
                },
                {
                    "name": "second",
                    "image": "redis",
                    "resources": {
                        "limits": {"cpu": "300m"}
                    }
                }
            ]
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "multi-container-pod", pod)
        .await
        .unwrap();

    let pod_res = db
        .get_resource("v1", "Pod", Some("default"), "multi-container-pod")
        .await
        .unwrap()
        .unwrap();

    // Without containerName, should use first container
    // K8s downward API: 200m → ceil(200/1000) = 1 core
    let cpu = extract_resource_field_ref(&pod_res.data, None, "limits.cpu").unwrap();
    assert_eq!(cpu, "1");

    // With containerName, should use specified container
    // 300m → ceil(300/1000) = 1 core
    let cpu2 = extract_resource_field_ref(&pod_res.data, Some("second"), "limits.cpu").unwrap();
    assert_eq!(cpu2, "1");
}

#[tokio::test]
async fn test_extract_resource_field_ref_missing_resources_returns_default() {
    use serde_json::json;
    let db = crate::datastore::test_support::in_memory().await;

    // Pod with NO resources block on its container
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "no-resources-pod",
            "namespace": "default"
        },
        "spec": {
            "containers": [
                {
                    "name": "app",
                    "image": "busybox"
                }
            ]
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "no-resources-pod", pod)
        .await
        .unwrap();

    let pod_res = db
        .get_resource("v1", "Pod", Some("default"), "no-resources-pod")
        .await
        .unwrap()
        .unwrap();

    // limits with no value set → node allocatable (non-zero, K8s spec)
    let limits_cpu = extract_resource_field_ref(&pod_res.data, Some("app"), "limits.cpu").unwrap();
    let limits_cpu_val: u64 = limits_cpu.parse().expect("limits.cpu must be numeric");
    assert!(
        limits_cpu_val > 0,
        "limits.cpu fallback must be node allocatable cores"
    );

    let limits_mem =
        extract_resource_field_ref(&pod_res.data, Some("app"), "limits.memory").unwrap();
    let limits_mem_val: u64 = limits_mem
        .parse()
        .expect("limits.memory must be numeric bytes");
    assert!(
        limits_mem_val > 0,
        "limits.memory fallback must be node allocatable bytes"
    );

    // requests with no value set → "0" (K8s spec)
    let requests_cpu =
        extract_resource_field_ref(&pod_res.data, Some("app"), "requests.cpu").unwrap();
    assert_eq!(requests_cpu, "0");

    let requests_mem =
        extract_resource_field_ref(&pod_res.data, Some("app"), "requests.memory").unwrap();
    assert_eq!(requests_mem, "0");
}

#[tokio::test]
async fn test_downward_api_volume_creates_labels_file() {
    use serde_json::json;
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "labeled-pod",
            "namespace": "default",
            "labels": {
                "app": "nginx",
                "version": "v1.2"
            }
        },
        "spec": {"containers": []}
    });
    db.create_resource("v1", "Pod", Some("default"), "labeled-pod", pod)
        .await
        .unwrap();

    let items = json!([
        {"path": "labels", "fieldRef": {"fieldPath": "metadata.labels"}}
    ]);

    let volume_path =
        create_downward_api_volume_at(root, &db, "default", "labeled-pod", "podinfo", None, &items)
            .await
            .unwrap();

    let content = crate::utils::read_utf8_file(format!("{}/labels", volume_path)).unwrap();
    assert!(content.contains("app=\"nginx\""));
    assert!(content.contains("version=\"v1.2\""));
}

#[tokio::test]
async fn test_downward_api_volume_creates_resource_limits_file() {
    use serde_json::json;
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "resource-pod",
            "namespace": "default"
        },
        "spec": {
            "containers": [{
                "name": "app",
                "image": "nginx",
                "resources": {
                    "limits": {"cpu": "500m", "memory": "256Mi"}
                }
            }]
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "resource-pod", pod)
        .await
        .unwrap();

    let items = json!([
        {"path": "cpu_limit", "resourceFieldRef": {"containerName": "app", "resource": "limits.cpu"}},
        {"path": "mem_limit", "resourceFieldRef": {"containerName": "app", "resource": "limits.memory"}}
    ]);

    let volume_path = create_downward_api_volume_at(
        root,
        &db,
        "default",
        "resource-pod",
        "resources",
        None,
        &items,
    )
    .await
    .unwrap();

    // K8s downward API converts: CPU → whole cores (ceiling), memory → bytes
    let cpu = crate::utils::read_utf8_file(format!("{}/cpu_limit", volume_path)).unwrap();
    assert_eq!(cpu, "1"); // 500m → ceil(500/1000) = 1 core

    let mem = crate::utils::read_utf8_file(format!("{}/mem_limit", volume_path)).unwrap();
    assert_eq!(mem, "268435456"); // 256Mi → 268435456 bytes
}

#[tokio::test]
async fn test_downward_api_volume_default_mode_permissions() {
    use serde_json::json;
    use std::os::unix::fs::PermissionsExt;

    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "pod1", "namespace": "default"},
        "spec": {"containers": []}
    });
    db.create_resource("v1", "Pod", Some("default"), "pod1", pod)
        .await
        .unwrap();

    let items = json!([
        {"path": "name", "fieldRef": {"fieldPath": "metadata.name"}}
    ]);

    let volume_path = create_downward_api_volume_at(
        root, &db, "default", "pod1", "info", None, // default mode (0o644)
        &items,
    )
    .await
    .unwrap();

    let metadata = std::fs::metadata(format!("{}/name", volume_path)).unwrap();
    let mode = metadata.permissions().mode() & 0o777;
    assert_eq!(mode, 0o644, "Default mode should be 0o644");
}

#[tokio::test]
async fn test_downward_api_volume_custom_mode_permissions() {
    use serde_json::json;
    use std::os::unix::fs::PermissionsExt;

    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "pod2", "namespace": "default"},
        "spec": {"containers": []}
    });
    db.create_resource("v1", "Pod", Some("default"), "pod2", pod)
        .await
        .unwrap();

    let items = json!([
        {"path": "name", "fieldRef": {"fieldPath": "metadata.name"}}
    ]);

    let volume_path = create_downward_api_volume_at(
        root,
        &db,
        "default",
        "pod2",
        "info",
        Some(256), // 0o400 = 256 decimal
        &items,
    )
    .await
    .unwrap();

    let metadata = std::fs::metadata(format!("{}/name", volume_path)).unwrap();
    let mode = metadata.permissions().mode() & 0o777;
    assert_eq!(mode, 0o400, "Custom mode should be 0o400");
}

#[tokio::test]
async fn test_downward_api_volume_per_file_mode_override() {
    use serde_json::json;
    use std::os::unix::fs::PermissionsExt;

    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "pod3", "namespace": "default", "uid": "uid123"},
        "spec": {"containers": []}
    });
    db.create_resource("v1", "Pod", Some("default"), "pod3", pod)
        .await
        .unwrap();

    let items = json!([
        {"path": "name", "fieldRef": {"fieldPath": "metadata.name"}},
        {"path": "uid", "mode": 384, "fieldRef": {"fieldPath": "metadata.uid"}}  // 0o600 = 384
    ]);

    let volume_path = create_downward_api_volume_at(
        root,
        &db,
        "default",
        "pod3",
        "info",
        Some(420), // default 0o644
        &items,
    )
    .await
    .unwrap();

    // name file should use default mode
    let name_meta = std::fs::metadata(format!("{}/name", volume_path)).unwrap();
    let name_mode = name_meta.permissions().mode() & 0o777;
    assert_eq!(name_mode, 0o644);

    // uid file should use per-file mode
    let uid_meta = std::fs::metadata(format!("{}/uid", volume_path)).unwrap();
    let uid_mode = uid_meta.permissions().mode() & 0o777;
    assert_eq!(uid_mode, 0o600);
}

#[tokio::test]
async fn test_downward_api_refresh_uses_keyed_blocking_boundary() {
    use serde_json::json;
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "pod-refresh", "namespace": "default"},
        "spec": {
            "containers": [{"name": "app", "image": "busybox"}],
            "volumes": [{
                "name": "podinfo",
                "downwardAPI": {
                    "items": [{"path": "name", "fieldRef": {"fieldPath": "metadata.name"}}]
                }
            }]
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "pod-refresh", pod.clone())
        .await
        .unwrap();

    let items = pod["spec"]["volumes"][0]["downwardAPI"]["items"].clone();
    create_downward_api_volume_at_with_db_name(DownwardApiVolumeWithDbNameRequest {
        volumes_root: root,
        sources: &db,
        namespace: "default",
        pod_dir_id: "default_pod-refresh",
        pod_db_name: "pod-refresh",
        volume_name: "podinfo",
        default_mode: None,
        items: &items,
    })
    .await
    .unwrap();

    let pod_for_refresh = db
        .get_resource("v1", "Pod", Some("default"), "pod-refresh")
        .await
        .unwrap()
        .unwrap();
    let before = blocking_fs_keyed_call_count();
    refresh_downward_api_volumes(&pod_for_refresh.data, root)
        .await
        .unwrap();
    assert!(
        blocking_fs_keyed_call_count() > before,
        "downwardAPI refresh must run through keyed blocking filesystem boundary"
    );
}

#[test]
fn test_downward_api_extract_field_ref_unsupported_field() {
    use serde_json::json;
    let pod = json!({
        "metadata": {"name": "test"}
    });

    let result = extract_field_ref(&pod, "metadata.creationTimestamp");
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Unsupported fieldPath")
    );
}

#[test]
fn test_extract_field_ref_labels_has_trailing_newline() {
    // K8s labels format: key="value"\n per entry, with trailing newline.
    // mounttest and other K8s tools expect this exact format.
    use serde_json::json;
    let pod = json!({
        "metadata": {
            "labels": {
                "app": "test",
                "env": "prod"
            }
        }
    });

    let result = extract_field_ref(&pod, "metadata.labels").unwrap();
    assert!(
        result.ends_with('\n'),
        "labels output must end with trailing newline, got: {:?}",
        result
    );
    // Each line should be key="value"
    for line in result.trim_end().split('\n') {
        assert!(
            line.contains('=') && line.contains('"'),
            "each line must be key=\"value\" format, got: {:?}",
            line
        );
    }
}

#[test]
fn test_extract_field_ref_annotations_has_trailing_newline() {
    use serde_json::json;
    let pod = json!({
        "metadata": {
            "annotations": {
                "note": "hello"
            }
        }
    });

    let result = extract_field_ref(&pod, "metadata.annotations").unwrap();
    assert!(
        result.ends_with('\n'),
        "annotations output must end with trailing newline, got: {:?}",
        result
    );
}

#[test]
fn test_downward_api_extract_resource_field_ref_invalid_path() {
    use serde_json::json;
    let pod = json!({
        "spec": {"containers": [{"name": "app", "resources": {}}]}
    });

    let result = extract_resource_field_ref(&pod, Some("app"), "invalidpath");
    assert!(result.is_err());
}
