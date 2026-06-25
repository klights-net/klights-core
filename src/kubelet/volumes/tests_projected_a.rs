use super::*;

#[tokio::test]
async fn test_projected_volume_with_service_account_token() {
    use serde_json::json;
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    // Create test pod
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "test-pod", "namespace": "default"},
        "spec": {"containers": []}
    });
    db.create_resource("v1", "Pod", Some("default"), "test-pod", pod)
        .await
        .unwrap();

    let sources = json!([
        {"serviceAccountToken": {"path": "token"}}
    ]);

    let path = create_projected_volume_at(ProjectedVolumeAtRequest {
        volumes_root: root,
        source_reader: &db,
        namespace: "default",
        pod_name: "test-pod",
        volume_name: "kube-api-access",
        default_mode: None,
        sources: &sources,
        token: Some("test-token-value"),
    })
    .await
    .unwrap();

    let token_content = crate::utils::read_utf8_file(format!("{}/token", path)).unwrap();
    assert_eq!(token_content, "test-token-value");
}

#[tokio::test]
async fn test_projected_volume_create_uses_keyed_blocking_boundary() {
    use serde_json::json;
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "test-pod", "namespace": "default"},
        "spec": {"containers": []}
    });
    db.create_resource("v1", "Pod", Some("default"), "test-pod", pod)
        .await
        .unwrap();

    let sources = json!([
        {"serviceAccountToken": {"path": "token"}}
    ]);

    let before = blocking_fs_keyed_call_count();
    let _ = create_projected_volume_at(ProjectedVolumeAtRequest {
        volumes_root: root,
        source_reader: &db,
        namespace: "default",
        pod_name: "test-pod",
        volume_name: "kube-api-access",
        default_mode: None,
        sources: &sources,
        token: Some("test-token-value"),
    })
    .await
    .unwrap();
    assert!(
        blocking_fs_keyed_call_count() > before,
        "projected volume creation must run through keyed blocking filesystem boundary"
    );
}

#[tokio::test]
async fn test_projected_volume_with_configmap_source() {
    use serde_json::json;
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    // Create ConfigMap
    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "kube-root-ca.crt", "namespace": "default"},
        "data": {"ca.crt": "-----BEGIN CERTIFICATE-----\ntest-ca\n-----END CERTIFICATE-----"}
    });
    db.create_resource("v1", "ConfigMap", Some("default"), "kube-root-ca.crt", cm)
        .await
        .unwrap();

    // Create pod
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "test-pod", "namespace": "default"},
        "spec": {"containers": []}
    });
    db.create_resource("v1", "Pod", Some("default"), "test-pod", pod)
        .await
        .unwrap();

    let sources = json!([
        {"configMap": {"name": "kube-root-ca.crt", "items": [{"key": "ca.crt", "path": "ca.crt"}]}}
    ]);

    let path = create_projected_volume_at(ProjectedVolumeAtRequest {
        volumes_root: root,
        source_reader: &db,
        namespace: "default",
        pod_name: "test-pod",
        volume_name: "kube-api-access",
        default_mode: None,
        sources: &sources,
        token: None,
    })
    .await
    .unwrap();

    let ca_content = crate::utils::read_utf8_file(format!("{}/ca.crt", path)).unwrap();
    assert_eq!(
        ca_content,
        "-----BEGIN CERTIFICATE-----\ntest-ca\n-----END CERTIFICATE-----"
    );
}

#[tokio::test]
async fn test_projected_volume_with_downward_api_source() {
    use serde_json::json;
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    // Create pod
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "test-pod", "namespace": "default"},
        "spec": {"containers": []}
    });
    db.create_resource("v1", "Pod", Some("default"), "test-pod", pod)
        .await
        .unwrap();

    let sources = json!([
        {"downwardAPI": {"items": [{"path": "namespace", "fieldRef": {"fieldPath": "metadata.namespace"}}]}}
    ]);

    let path = create_projected_volume_at(ProjectedVolumeAtRequest {
        volumes_root: root,
        source_reader: &db,
        namespace: "default",
        pod_name: "test-pod",
        volume_name: "kube-api-access",
        default_mode: None,
        sources: &sources,
        token: None,
    })
    .await
    .unwrap();

    let namespace_content = crate::utils::read_utf8_file(format!("{}/namespace", path)).unwrap();
    assert_eq!(namespace_content, "default");
}

#[tokio::test]
async fn test_projected_volume_combines_multiple_sources() {
    use serde_json::json;
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    // Create ConfigMap
    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "kube-root-ca.crt", "namespace": "default"},
        "data": {"ca.crt": "ca-cert-data"}
    });
    db.create_resource("v1", "ConfigMap", Some("default"), "kube-root-ca.crt", cm)
        .await
        .unwrap();

    // Create pod
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "test-pod", "namespace": "default"},
        "spec": {"containers": []}
    });
    db.create_resource("v1", "Pod", Some("default"), "test-pod", pod)
        .await
        .unwrap();

    // Combine serviceAccountToken + configMap + downwardAPI (mimics kube-api-access volume)
    let sources = json!([
        {"serviceAccountToken": {"path": "token"}},
        {"configMap": {"name": "kube-root-ca.crt", "items": [{"key": "ca.crt", "path": "ca.crt"}]}},
        {"downwardAPI": {"items": [{"path": "namespace", "fieldRef": {"fieldPath": "metadata.namespace"}}]}}
    ]);

    let path = create_projected_volume_at(ProjectedVolumeAtRequest {
        volumes_root: root,
        source_reader: &db,
        namespace: "default",
        pod_name: "test-pod",
        volume_name: "kube-api-access",
        default_mode: None,
        sources: &sources,
        token: Some("projected-token"),
    })
    .await
    .unwrap();

    // All three files should exist in the same directory
    assert!(std::path::Path::new(&format!("{}/token", path)).exists());
    assert!(std::path::Path::new(&format!("{}/ca.crt", path)).exists());
    assert!(std::path::Path::new(&format!("{}/namespace", path)).exists());

    let token = crate::utils::read_utf8_file(format!("{}/token", path)).unwrap();
    let ca = crate::utils::read_utf8_file(format!("{}/ca.crt", path)).unwrap();
    let ns = crate::utils::read_utf8_file(format!("{}/namespace", path)).unwrap();

    assert_eq!(token, "projected-token");
    assert_eq!(ca, "ca-cert-data");
    assert_eq!(ns, "default");
}

#[tokio::test]
async fn test_projected_volume_respects_default_mode() {
    use serde_json::json;
    use std::os::unix::fs::PermissionsExt;
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    // Create pod
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "test-pod", "namespace": "default"},
        "spec": {"containers": []}
    });
    db.create_resource("v1", "Pod", Some("default"), "test-pod", pod)
        .await
        .unwrap();

    let sources = json!([
        {"serviceAccountToken": {"path": "token"}}
    ]);

    let path = create_projected_volume_at(ProjectedVolumeAtRequest {
        volumes_root: root,
        source_reader: &db,
        namespace: "default",
        pod_name: "test-pod",
        volume_name: "kube-api-access",
        default_mode: Some(0o400),
        sources: &sources,
        token: Some("test-token"),
    })
    .await
    .unwrap();

    let metadata = std::fs::metadata(format!("{}/token", path)).unwrap();
    let mode = metadata.permissions().mode() & 0o777;
    assert_eq!(mode, 0o400);
}

#[test]
fn test_hostpath_volume_directory() {
    // Test mounting existing directory
    let tmp = tempfile::tempdir().unwrap();
    let dir_path = tmp.path().join("test-dir");
    std::fs::create_dir(&dir_path).unwrap();

    let result = resolve_host_path(dir_path.to_str().unwrap(), Some("Directory"));
    assert!(
        result.is_ok(),
        "Should succeed for existing directory with type=Directory"
    );
    assert_eq!(result.unwrap(), dir_path.to_str().unwrap());
}

#[test]
fn test_hostpath_volume_file() {
    // Test mounting existing file
    let tmp = tempfile::tempdir().unwrap();
    let file_path = tmp.path().join("test-file");
    std::fs::write(&file_path, "test content").unwrap();

    let result = resolve_host_path(file_path.to_str().unwrap(), Some("File"));
    assert!(
        result.is_ok(),
        "Should succeed for existing file with type=File"
    );
    assert_eq!(result.unwrap(), file_path.to_str().unwrap());
}

#[test]
fn test_hostpath_volume_directory_or_create() {
    // Test DirectoryOrCreate - creates directory if not exists
    let tmp = tempfile::tempdir().unwrap();
    let dir_path = tmp.path().join("new-dir");
    assert!(!dir_path.exists(), "Directory should not exist yet");

    let result = resolve_host_path(dir_path.to_str().unwrap(), Some("DirectoryOrCreate"));
    assert!(result.is_ok(), "Should succeed creating directory");
    assert!(dir_path.exists(), "Directory should be created");
    assert!(dir_path.is_dir(), "Created path should be a directory");
}

#[test]
fn test_hostpath_volume_file_or_create() {
    // Test FileOrCreate - creates file if not exists
    let tmp = tempfile::tempdir().unwrap();
    let file_path = tmp.path().join("subdir").join("new-file");
    assert!(!file_path.exists(), "File should not exist yet");

    let result = resolve_host_path(file_path.to_str().unwrap(), Some("FileOrCreate"));
    assert!(result.is_ok(), "Should succeed creating file");
    assert!(file_path.exists(), "File should be created");
    assert!(file_path.is_file(), "Created path should be a file");
}

#[test]
fn test_hostpath_volume_type_validation() {
    let tmp = tempfile::tempdir().unwrap();

    // Create a file, try to mount as Directory - should fail
    let file_path = tmp.path().join("a-file");
    std::fs::write(&file_path, "content").unwrap();
    let result = resolve_host_path(file_path.to_str().unwrap(), Some("Directory"));
    assert!(
        result.is_err(),
        "Should fail when file mounted as Directory"
    );
    assert!(result.unwrap_err().to_string().contains("not a directory"));

    // Create a directory, try to mount as File - should fail
    let dir_path = tmp.path().join("a-dir");
    std::fs::create_dir(&dir_path).unwrap();
    let result = resolve_host_path(dir_path.to_str().unwrap(), Some("File"));
    assert!(
        result.is_err(),
        "Should fail when directory mounted as File"
    );
    assert!(result.unwrap_err().to_string().contains("not a file"));

    // Non-existent path with type=Directory - should fail
    let missing = tmp.path().join("missing");
    let result = resolve_host_path(missing.to_str().unwrap(), Some("Directory"));
    assert!(result.is_err(), "Should fail when Directory does not exist");
    assert!(result.unwrap_err().to_string().contains("does not exist"));

    // Non-existent path with type=File - should fail
    let result = resolve_host_path(missing.to_str().unwrap(), Some("File"));
    assert!(result.is_err(), "Should fail when File does not exist");
    assert!(result.unwrap_err().to_string().contains("does not exist"));
}

#[test]
fn test_hostpath_volume_directory_or_create_existing_file_fails() {
    // DirectoryOrCreate with existing file should fail
    let tmp = tempfile::tempdir().unwrap();
    let file_path = tmp.path().join("existing-file");
    std::fs::write(&file_path, "content").unwrap();

    let result = resolve_host_path(file_path.to_str().unwrap(), Some("DirectoryOrCreate"));
    assert!(
        result.is_err(),
        "Should fail when DirectoryOrCreate finds existing file"
    );
    assert!(result.unwrap_err().to_string().contains("not a directory"));
}

#[test]
fn test_hostpath_volume_file_or_create_existing_directory_fails() {
    // FileOrCreate with existing directory should fail
    let tmp = tempfile::tempdir().unwrap();
    let dir_path = tmp.path().join("existing-dir");
    std::fs::create_dir(&dir_path).unwrap();

    let result = resolve_host_path(dir_path.to_str().unwrap(), Some("FileOrCreate"));
    assert!(
        result.is_err(),
        "Should fail when FileOrCreate finds existing directory"
    );
    assert!(result.unwrap_err().to_string().contains("not a file"));
}

#[test]
fn test_hostpath_volume_empty_type_no_validation() {
    // Empty type string - no validation, just return path
    let tmp = tempfile::tempdir().unwrap();
    let missing = tmp.path().join("missing");

    let result = resolve_host_path(missing.to_str().unwrap(), Some(""));
    assert!(
        result.is_ok(),
        "Should succeed with empty type (no validation)"
    );
}

#[test]
fn test_hostpath_volume_none_type_no_validation() {
    // None type - no validation, just return path
    let tmp = tempfile::tempdir().unwrap();
    let missing = tmp.path().join("missing");

    let result = resolve_host_path(missing.to_str().unwrap(), None);
    assert!(
        result.is_ok(),
        "Should succeed with None type (no validation)"
    );
}

#[tokio::test]
async fn test_refresh_downward_api_updates_annotation_file() {
    use tempfile::TempDir;

    let db = crate::datastore::test_support::in_memory().await;
    let tmp_dir = TempDir::new().unwrap();
    let volumes_root = tmp_dir.path().to_str().unwrap();

    // Create initial pod with annotation
    let pod_json = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "test-pod",
            "namespace": "default",
            "uid": "uid-test-pod",
            "annotations": {
                "key1": "initial-value"
            }
        },
        "spec": {
            "containers": [{
                "name": "nginx",
                "image": "nginx"
            }],
            "volumes": [{
                "name": "podinfo",
                "downwardAPI": {
                    "items": [{
                        "path": "annotations",
                        "fieldRef": {
                            "fieldPath": "metadata.annotations"
                        }
                    }]
                }
            }]
        }
    });

    db.create_resource("v1", "Pod", Some("default"), "test-pod", pod_json.clone())
        .await
        .unwrap();

    // Create downward API volume with initial annotations.
    let items = pod_json["spec"]["volumes"][0]["downwardAPI"]["items"].clone();
    let pod_dir_id = crate::kubelet::pod_runtime::service::pod_volume_dir_id(
        "default",
        "test-pod",
        "uid-test-pod",
    );
    create_downward_api_volume_at_with_db_name(DownwardApiVolumeWithDbNameRequest {
        volumes_root,
        sources: &db,
        namespace: "default",
        pod_dir_id: &pod_dir_id,
        pod_db_name: "test-pod",
        volume_name: "podinfo",
        default_mode: None,
        items: &items,
    })
    .await
    .unwrap();

    // Verify initial file content
    let volume_path = format!("{}/{pod_dir_id}/volumes/downward-api/podinfo", volumes_root);
    let annotations_path = format!("{}/annotations", volume_path);
    let initial_content = crate::utils::read_utf8_file(&annotations_path).unwrap();
    assert!(
        initial_content.contains("key1=\"initial-value\""),
        "Initial annotation should be present"
    );

    // Update pod annotations
    let updated_pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "test-pod",
            "namespace": "default",
            "uid": "uid-test-pod",
            "annotations": {
                "key1": "updated-value",
                "key2": "new-annotation"
            }
        },
        "spec": pod_json["spec"].clone()
    });

    let resource = db
        .get_resource("v1", "Pod", Some("default"), "test-pod")
        .await
        .unwrap()
        .unwrap();

    db.update_resource(
        "v1",
        "Pod",
        Some("default"),
        "test-pod",
        updated_pod,
        resource.resource_version,
    )
    .await
    .unwrap();

    // Refresh downward API volumes
    let pod_for_refresh = db
        .get_resource("v1", "Pod", Some("default"), "test-pod")
        .await
        .unwrap()
        .unwrap();

    refresh_downward_api_volumes(&pod_for_refresh.data, volumes_root)
        .await
        .unwrap();

    // Verify file content updated
    let updated_content = crate::utils::read_utf8_file(&annotations_path).unwrap();
    assert!(
        updated_content.contains("key1=\"updated-value\""),
        "Annotation should be updated"
    );
    assert!(
        updated_content.contains("key2=\"new-annotation\""),
        "New annotation should be added"
    );
    assert!(
        !updated_content.contains("initial-value"),
        "Old value should be gone"
    );
}

#[tokio::test]
async fn test_refresh_downward_api_skips_projected_volumes() {
    // refresh_downward_api_volumes must NOT create files for projected volumes.
    // The SA projected volume (kube-api-access-*) has a downwardAPI source for
    // metadata.namespace, but refreshing it wrote to volumes/downward-api/
    // instead of volumes/projected/, creating phantom directories that eventually
    // corrupted bind mounts.
    use tempfile::TempDir;

    let db = crate::datastore::test_support::in_memory().await;
    let tmp_dir = TempDir::new().unwrap();
    let volumes_root = tmp_dir.path().to_str().unwrap();

    // Create pod with SA projected volume (same structure as inject_serviceaccount_volume)
    let pod_json = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "sa-pod",
            "namespace": "default",
            "uid": "uid-sa-pod"
        },
        "spec": {
            "containers": [{"name": "app", "image": "busybox"}],
            "volumes": [{
                "name": "kube-api-access-abc12",
                "projected": {
                    "defaultMode": 420,
                    "sources": [
                        {"serviceAccountToken": {"expirationSeconds": 3607, "path": "token"}},
                        {"configMap": {"name": "kube-root-ca.crt", "items": [{"key": "ca.crt", "path": "ca.crt"}]}},
                        {"downwardAPI": {"items": [{"path": "namespace", "fieldRef": {"apiVersion": "v1", "fieldPath": "metadata.namespace"}}]}}
                    ]
                }
            }]
        }
    });

    db.create_resource("v1", "Pod", Some("default"), "sa-pod", pod_json.clone())
        .await
        .unwrap();

    let pod_resource = db
        .get_resource("v1", "Pod", Some("default"), "sa-pod")
        .await
        .unwrap()
        .unwrap();

    // Call refresh — should NOT create any files for the projected volume
    refresh_downward_api_volumes(&pod_resource.data, volumes_root)
        .await
        .unwrap();

    // The volumes/downward-api/ directory must NOT exist
    let pod_dir_id =
        crate::kubelet::pod_runtime::service::pod_volume_dir_id("default", "sa-pod", "uid-sa-pod");
    let phantom_dir = format!("{}/{pod_dir_id}/volumes/downward-api", volumes_root);
    assert!(
        !std::path::Path::new(&phantom_dir).exists(),
        "refresh must NOT create volumes/downward-api/ for projected volumes, but found: {}",
        phantom_dir
    );

    // The volumes/projected/ directory must also NOT exist (refresh doesn't create projected volumes)
    let projected_dir = format!("{}/{pod_dir_id}/volumes/projected", volumes_root);
    assert!(
        !std::path::Path::new(&projected_dir).exists(),
        "refresh must NOT create volumes/projected/ either"
    );
}

#[tokio::test]
async fn test_refresh_projected_downward_api_updates_labels_file() {
    // When a projected volume directory exists (created at pod startup) and contains
    // downwardAPI sources with mutable fields (labels), refresh should update them
    // at the correct volumes/projected/{name}/ path.
    use tempfile::TempDir;

    let db = crate::datastore::test_support::in_memory().await;
    let tmp_dir = TempDir::new().unwrap();
    let volumes_root = tmp_dir.path().to_str().unwrap();

    // Create pod with projected volume containing labels downwardAPI
    let pod_json = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "label-pod",
            "namespace": "default",
            "uid": "uid-label-pod",
            "labels": {"app": "initial"}
        },
        "spec": {
            "containers": [{"name": "app", "image": "busybox"}],
            "volumes": [{
                "name": "podinfo",
                "projected": {
                    "defaultMode": 420,
                    "sources": [
                        {"downwardAPI": {"items": [
                            {"path": "labels", "fieldRef": {"fieldPath": "metadata.labels"}}
                        ]}}
                    ]
                }
            }]
        }
    });

    db.create_resource("v1", "Pod", Some("default"), "label-pod", pod_json)
        .await
        .unwrap();

    // Simulate pod startup: create the projected volume directory with initial content.
    let pod_dir_id = crate::kubelet::pod_runtime::service::pod_volume_dir_id(
        "default",
        "label-pod",
        "uid-label-pod",
    );
    let vol_dir = format!("{}/{pod_dir_id}/volumes/projected/podinfo", volumes_root);
    std::fs::create_dir_all(&vol_dir).unwrap();
    std::fs::write(format!("{}/labels", vol_dir), "app=\"initial\"\n").unwrap();

    // Verify initial content
    let content = crate::utils::read_utf8_file(format!("{}/labels", vol_dir)).unwrap();
    assert!(content.contains("app=\"initial\""));

    // Update pod labels in the database
    let pod_resource = db
        .get_resource("v1", "Pod", Some("default"), "label-pod")
        .await
        .unwrap()
        .unwrap();
    let mut updated_pod: serde_json::Value = (*pod_resource.data).clone();
    if let Some(meta) = updated_pod
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "labels".to_string(),
            serde_json::json!({"app": "updated", "env": "test"}),
        );
    }
    db.update_resource(
        "v1",
        "Pod",
        Some("default"),
        "label-pod",
        updated_pod.clone(),
        pod_resource.resource_version,
    )
    .await
    .unwrap();

    // Refresh volumes
    refresh_downward_api_volumes(&updated_pod, volumes_root)
        .await
        .unwrap();

    // Verify labels file updated at the correct projected path
    let updated_content = crate::utils::read_utf8_file(format!("{}/labels", vol_dir)).unwrap();
    assert!(
        updated_content.contains("app=\"updated\""),
        "Label should be updated, got: {}",
        updated_content
    );
    assert!(
        updated_content.contains("env=\"test\""),
        "New label should appear, got: {}",
        updated_content
    );
    assert!(
        !updated_content.contains("initial"),
        "Old label value should be gone"
    );

    // Must NOT create phantom volumes/downward-api/ directory
    let phantom_dir = format!("{}/{pod_dir_id}/volumes/downward-api", volumes_root);
    assert!(
        !std::path::Path::new(&phantom_dir).exists(),
        "refresh must NOT create volumes/downward-api/ phantom directory"
    );
}

#[tokio::test]
async fn test_projected_volume_configmap_writes_files() {
    // Regression test: projected volume with configMap source was silently skipping file writes
    // This test reproduces the kube-root-ca.crt ConfigMap scenario from Sonobuoy failures
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    // Create kube-root-ca.crt ConfigMap (same as real K8s)
    let cm = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "kube-root-ca.crt",
            "namespace": "default"
        },
        "data": {
            "ca.crt": "-----BEGIN CERTIFICATE-----\nMIIBeDCCAR2gAwIBAgIBADAKBggqhkjOPQQDAjAjMSEwHwYDVQQDDBhrM3Mtc2Vy\n-----END CERTIFICATE-----\n"
        }
    });
    db.create_resource("v1", "ConfigMap", Some("default"), "kube-root-ca.crt", cm)
        .await
        .unwrap();

    // Create projected volume with configMap source
    let sources = serde_json::json!([
        {
            "configMap": {
                "name": "kube-root-ca.crt",
                "items": [
                    {"key": "ca.crt", "path": "ca.crt"}
                ]
            }
        }
    ]);

    let path = create_projected_volume_at(ProjectedVolumeAtRequest {
        volumes_root: root,
        source_reader: &db,
        namespace: "default",
        pod_name: "test-pod",
        volume_name: "kube-api-access",
        default_mode: Some(0o644),
        sources: &sources,
        token: None,
    })
    .await
    .unwrap();

    // The ca.crt file MUST exist
    let ca_cert_path = format!("{}/ca.crt", path);
    assert!(
        std::path::Path::new(&ca_cert_path).exists(),
        "ca.crt file must exist at {} but was not created",
        ca_cert_path
    );

    // Verify content
    let content = crate::utils::read_utf8_file(&ca_cert_path).unwrap();
    assert!(
        content.contains("BEGIN CERTIFICATE"),
        "ca.crt content should contain certificate data, got: {}",
        content
    );
}

#[tokio::test]
async fn test_projected_volume_configmap_with_items() {
    // Test projected volume with items key→path mapping
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let cm = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "app-config", "namespace": "default"},
        "data": {
            "config.yaml": "server: localhost\nport: 8080\n",
            "logging.conf": "level: debug\n"
        }
    });
    db.create_resource("v1", "ConfigMap", Some("default"), "app-config", cm)
        .await
        .unwrap();

    // Project only config.yaml, rename to app.yaml
    let sources = serde_json::json!([
        {
            "configMap": {
                "name": "app-config",
                "items": [
                    {"key": "config.yaml", "path": "app.yaml"}
                ]
            }
        }
    ]);

    let path = create_projected_volume_at(ProjectedVolumeAtRequest {
        volumes_root: root,
        source_reader: &db,
        namespace: "default",
        pod_name: "test-pod",
        volume_name: "config-volume",
        default_mode: None,
        sources: &sources,
        token: None,
    })
    .await
    .unwrap();

    // app.yaml should exist (renamed from config.yaml)
    assert!(std::path::Path::new(&format!("{}/app.yaml", path)).exists());
    let content = crate::utils::read_utf8_file(format!("{}/app.yaml", path)).unwrap();
    assert_eq!(content, "server: localhost\nport: 8080\n");

    // logging.conf should NOT exist (not in items list)
    assert!(!std::path::Path::new(&format!("{}/logging.conf", path)).exists());
}
