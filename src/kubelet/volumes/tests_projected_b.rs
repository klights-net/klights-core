use super::*;

#[tokio::test]
async fn test_projected_volume_missing_configmap_logs_error() {
    // Test that missing configMap produces error, not silent skip
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let sources = serde_json::json!([
        {
            "configMap": {
                "name": "nonexistent-cm",
                "items": [
                    {"key": "data", "path": "data.txt"}
                ]
            }
        }
    ]);

    let result = create_projected_volume_at(ProjectedVolumeAtRequest {
        volumes_root: root,
        source_reader: &db,
        namespace: "default",
        pod_name: "test-pod",
        volume_name: "missing-volume",
        default_mode: None,
        sources: &sources,
        token: None,
    })
    .await;

    // Should return error, not succeed with empty volume
    assert!(
        result.is_err(),
        "Missing ConfigMap should return error, not silently skip"
    );
    assert!(
        result.unwrap_err().to_string().contains("not found"),
        "Error should mention ConfigMap not found"
    );
}

#[tokio::test]
async fn test_projected_volume_optional_missing_configmap_creates_empty_volume() {
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let sources = serde_json::json!([
        {
            "configMap": {
                "name": "missing-optional-cm",
                "optional": true,
                "items": [
                    {"key": "data", "path": "data.txt"}
                ]
            }
        }
    ]);

    let path = create_projected_volume_at(ProjectedVolumeAtRequest {
        volumes_root: root,
        source_reader: &db,
        namespace: "default",
        pod_name: "test-pod",
        volume_name: "optional-volume",
        default_mode: None,
        sources: &sources,
        token: None,
    })
    .await
    .expect("optional missing ConfigMap should create an empty projected volume");

    assert!(std::path::Path::new(&path).is_dir());
    assert!(
        !std::path::Path::new(&format!("{}/data.txt", path)).exists(),
        "missing optional ConfigMap key must not create a file"
    );
}

#[tokio::test]
async fn test_projected_volume_configmap_missing_key_errors() {
    // Test that missing key in configMap data produces error with logging
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    // Create ConfigMap without the key we'll request
    let cm = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "partial-cm", "namespace": "default"},
        "data": {"existing-key": "value"}
    });
    db.create_resource("v1", "ConfigMap", Some("default"), "partial-cm", cm)
        .await
        .unwrap();

    // Try to project a non-existent key
    let sources = serde_json::json!([
        {
            "configMap": {
                "name": "partial-cm",
                "items": [
                    {"key": "nonexistent-key", "path": "data.txt"}
                ]
            }
        }
    ]);

    let result = create_projected_volume_at(ProjectedVolumeAtRequest {
        volumes_root: root,
        source_reader: &db,
        namespace: "default",
        pod_name: "test-pod",
        volume_name: "bad-volume",
        default_mode: None,
        sources: &sources,
        token: None,
    })
    .await;

    // Should return error mentioning the missing key
    assert!(result.is_err(), "Missing key should return error");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("nonexistent-key"),
        "Error should mention the missing key, got: {}",
        err_msg
    );
}

#[tokio::test]
async fn test_projected_volume_optional_configmap_missing_key_is_skipped() {
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let cm = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "partial-cm", "namespace": "default"},
        "data": {"existing-key": "value"}
    });
    db.create_resource("v1", "ConfigMap", Some("default"), "partial-cm", cm)
        .await
        .unwrap();

    let sources = serde_json::json!([
        {
            "configMap": {
                "name": "partial-cm",
                "optional": true,
                "items": [
                    {"key": "missing-key", "path": "missing.txt"}
                ]
            }
        }
    ]);

    let path = create_projected_volume_at(ProjectedVolumeAtRequest {
        volumes_root: root,
        source_reader: &db,
        namespace: "default",
        pod_name: "test-pod",
        volume_name: "optional-key-volume",
        default_mode: None,
        sources: &sources,
        token: None,
    })
    .await
    .expect("optional missing ConfigMap key should be skipped");

    assert!(std::path::Path::new(&path).is_dir());
    assert!(
        !std::path::Path::new(&format!("{}/missing.txt", path)).exists(),
        "missing optional ConfigMap key must not create a file"
    );
}

#[tokio::test]
async fn test_projected_volume_optional_missing_secret_creates_empty_volume() {
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let sources = serde_json::json!([
        {
            "secret": {
                "name": "missing-optional-secret",
                "optional": true,
                "items": [
                    {"key": "password", "path": "password"}
                ]
            }
        }
    ]);

    let path = create_projected_volume_at(ProjectedVolumeAtRequest {
        volumes_root: root,
        source_reader: &db,
        namespace: "default",
        pod_name: "test-pod",
        volume_name: "optional-secret-volume",
        default_mode: None,
        sources: &sources,
        token: None,
    })
    .await
    .expect("optional missing Secret should create an empty projected volume");

    assert!(std::path::Path::new(&path).is_dir());
    assert!(
        !std::path::Path::new(&format!("{}/password", path)).exists(),
        "missing optional Secret key must not create a file"
    );
}

#[tokio::test]
async fn test_projected_volume_optional_secret_missing_key_is_skipped() {
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    use base64::Engine;
    let secret = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "partial-secret", "namespace": "default"},
        "data": {
            "existing-key": base64::engine::general_purpose::STANDARD.encode("value")
        }
    });
    db.create_resource("v1", "Secret", Some("default"), "partial-secret", secret)
        .await
        .unwrap();

    let sources = serde_json::json!([
        {
            "secret": {
                "name": "partial-secret",
                "optional": true,
                "items": [
                    {"key": "missing-key", "path": "missing.txt"}
                ]
            }
        }
    ]);

    let path = create_projected_volume_at(ProjectedVolumeAtRequest {
        volumes_root: root,
        source_reader: &db,
        namespace: "default",
        pod_name: "test-pod",
        volume_name: "optional-secret-key-volume",
        default_mode: None,
        sources: &sources,
        token: None,
    })
    .await
    .expect("optional missing Secret key should be skipped");

    assert!(std::path::Path::new(&path).is_dir());
    assert!(
        !std::path::Path::new(&format!("{}/missing.txt", path)).exists(),
        "missing optional Secret key must not create a file"
    );
}

#[tokio::test]
async fn test_projected_volume_secret_writes_files() {
    // Test projected volume with secret source
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    // Create secret with base64-encoded data
    use base64::Engine;
    let secret = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "tls-secret", "namespace": "default"},
        "data": {
            "tls.crt": base64::engine::general_purpose::STANDARD.encode("cert-data"),
            "tls.key": base64::engine::general_purpose::STANDARD.encode("key-data")
        }
    });
    db.create_resource("v1", "Secret", Some("default"), "tls-secret", secret)
        .await
        .unwrap();

    // Create projected volume with secret source
    let sources = serde_json::json!([
        {
            "secret": {
                "name": "tls-secret",
                "items": [
                    {"key": "tls.crt", "path": "cert.pem"},
                    {"key": "tls.key", "path": "key.pem"}
                ]
            }
        }
    ]);

    let path = create_projected_volume_at(ProjectedVolumeAtRequest {
        volumes_root: root,
        source_reader: &db,
        namespace: "default",
        pod_name: "test-pod",
        volume_name: "tls-volume",
        default_mode: None,
        sources: &sources,
        token: None,
    })
    .await
    .unwrap();

    // Files should exist and be base64-decoded
    let cert_content = crate::utils::read_utf8_file(format!("{}/cert.pem", path)).unwrap();
    assert_eq!(cert_content, "cert-data");

    let key_content = crate::utils::read_utf8_file(format!("{}/key.pem", path)).unwrap();
    assert_eq!(key_content, "key-data");
}

// ── Projected volume: configMap source without items ──────────────

#[tokio::test]
async fn test_projected_volume_configmap_without_items_writes_all_keys() {
    use serde_json::json;
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    // ConfigMap with multiple keys
    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "multi-key-cm", "namespace": "default"},
        "data": {
            "app.conf": "server=localhost",
            "db.conf": "host=db",
            "log.conf": "level=info"
        }
    });
    db.create_resource("v1", "ConfigMap", Some("default"), "multi-key-cm", cm)
        .await
        .unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "test-pod", "namespace": "default"},
        "spec": {"containers": []}
    });
    db.create_resource("v1", "Pod", Some("default"), "test-pod", pod)
        .await
        .unwrap();

    // No items — all keys should be written as files
    let sources = json!([
        {"configMap": {"name": "multi-key-cm"}}
    ]);

    let path = create_projected_volume_at(ProjectedVolumeAtRequest {
        volumes_root: root,
        source_reader: &db,
        namespace: "default",
        pod_name: "test-pod",
        volume_name: "proj-vol",
        default_mode: None,
        sources: &sources,
        token: None,
    })
    .await
    .unwrap();

    assert_eq!(
        crate::utils::read_utf8_file(format!("{}/app.conf", path)).unwrap(),
        "server=localhost"
    );
    assert_eq!(
        crate::utils::read_utf8_file(format!("{}/db.conf", path)).unwrap(),
        "host=db"
    );
    assert_eq!(
        crate::utils::read_utf8_file(format!("{}/log.conf", path)).unwrap(),
        "level=info"
    );
}

// ── Projected volume: configMap source with per-file mode ─────────

#[tokio::test]
async fn test_projected_volume_configmap_items_per_file_mode() {
    use serde_json::json;
    use std::os::unix::fs::PermissionsExt;
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "mode-cm", "namespace": "default"},
        "data": {"run.sh": "#!/bin/sh\necho hi", "config.yaml": "key: val"}
    });
    db.create_resource("v1", "ConfigMap", Some("default"), "mode-cm", cm)
        .await
        .unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "test-pod", "namespace": "default"},
        "spec": {"containers": []}
    });
    db.create_resource("v1", "Pod", Some("default"), "test-pod", pod)
        .await
        .unwrap();

    // Item with explicit per-file mode (0o755 = 493)
    let sources = json!([
        {"configMap": {"name": "mode-cm", "items": [
            {"key": "run.sh", "path": "run.sh", "mode": 493},
            {"key": "config.yaml", "path": "config.yaml"}
        ]}}
    ]);

    let path = create_projected_volume_at(ProjectedVolumeAtRequest {
        volumes_root: root,
        source_reader: &db,
        namespace: "default",
        pod_name: "test-pod",
        volume_name: "proj-vol",
        default_mode: Some(0o644),
        sources: &sources,
        token: None,
    })
    .await
    .unwrap();

    // run.sh should have per-file mode 0o755
    let run_mode = std::fs::metadata(format!("{}/run.sh", path))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(
        run_mode, 0o755,
        "run.sh should have per-file mode 0755, got {:o}",
        run_mode
    );

    // config.yaml should fall back to defaultMode 0o644
    let cfg_mode = std::fs::metadata(format!("{}/config.yaml", path))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(
        cfg_mode, 0o644,
        "config.yaml should fall back to defaultMode 0644, got {:o}",
        cfg_mode
    );
}

// ── Projected volume: secret source without items ────────────────

#[tokio::test]
async fn test_projected_volume_secret_without_items_writes_all_keys() {
    use base64::Engine;
    use serde_json::json;
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let secret = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "all-keys-secret", "namespace": "default"},
        "data": {
            "cert.pem": base64::engine::general_purpose::STANDARD.encode("-----BEGIN CERT-----"),
            "key.pem": base64::engine::general_purpose::STANDARD.encode("-----BEGIN KEY-----")
        }
    });
    db.create_resource("v1", "Secret", Some("default"), "all-keys-secret", secret)
        .await
        .unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "test-pod", "namespace": "default"},
        "spec": {"containers": []}
    });
    db.create_resource("v1", "Pod", Some("default"), "test-pod", pod)
        .await
        .unwrap();

    // No items — all keys written
    let sources = json!([
        {"secret": {"name": "all-keys-secret"}}
    ]);

    let path = create_projected_volume_at(ProjectedVolumeAtRequest {
        volumes_root: root,
        source_reader: &db,
        namespace: "default",
        pod_name: "test-pod",
        volume_name: "proj-vol",
        default_mode: None,
        sources: &sources,
        token: None,
    })
    .await
    .unwrap();

    assert_eq!(
        crate::utils::read_utf8_file(format!("{}/cert.pem", path)).unwrap(),
        "-----BEGIN CERT-----"
    );
    assert_eq!(
        crate::utils::read_utf8_file(format!("{}/key.pem", path)).unwrap(),
        "-----BEGIN KEY-----"
    );
}

// ── Projected volume: secret with per-file mode ──────────────────

#[tokio::test]
async fn test_projected_volume_secret_items_per_file_mode() {
    use base64::Engine;
    use serde_json::json;
    use std::os::unix::fs::PermissionsExt;
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let secret = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "mode-secret", "namespace": "default"},
        "data": {
            "key.pem": base64::engine::general_purpose::STANDARD.encode("private-key")
        }
    });
    db.create_resource("v1", "Secret", Some("default"), "mode-secret", secret)
        .await
        .unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "test-pod", "namespace": "default"},
        "spec": {"containers": []}
    });
    db.create_resource("v1", "Pod", Some("default"), "test-pod", pod)
        .await
        .unwrap();

    // Per-file mode 0o400 (256 decimal) — read-only for owner
    let sources = json!([
        {"secret": {"name": "mode-secret", "items": [
            {"key": "key.pem", "path": "key.pem", "mode": 256}
        ]}}
    ]);

    let path = create_projected_volume_at(ProjectedVolumeAtRequest {
        volumes_root: root,
        source_reader: &db,
        namespace: "default",
        pod_name: "test-pod",
        volume_name: "proj-vol",
        default_mode: Some(0o644),
        sources: &sources,
        token: None,
    })
    .await
    .unwrap();

    let mode = std::fs::metadata(format!("{}/key.pem", path))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(
        mode, 0o400,
        "per-file mode 0400 should override defaultMode 0644, got {:o}",
        mode
    );
}

// ── Projected volume: downwardAPI with resourceFieldRef ──────────

#[tokio::test]
async fn test_projected_volume_downward_api_resource_field_ref() {
    use serde_json::json;
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "test-pod", "namespace": "default"},
        "spec": {
            "containers": [{
                "name": "app",
                "image": "busybox",
                "resources": {
                    "limits": {"cpu": "500m", "memory": "128Mi"},
                    "requests": {"cpu": "250m", "memory": "64Mi"}
                }
            }]
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "test-pod", pod)
        .await
        .unwrap();

    let sources = json!([
        {"downwardAPI": {"items": [
            {"path": "cpu-limit", "resourceFieldRef": {"containerName": "app", "resource": "limits.cpu"}},
            {"path": "mem-limit", "resourceFieldRef": {"containerName": "app", "resource": "limits.memory"}}
        ]}}
    ]);

    let path = create_projected_volume_at(ProjectedVolumeAtRequest {
        volumes_root: root,
        source_reader: &db,
        namespace: "default",
        pod_name: "test-pod",
        volume_name: "proj-vol",
        default_mode: None,
        sources: &sources,
        token: None,
    })
    .await
    .unwrap();

    // Files should exist with resource values
    assert!(
        std::path::Path::new(&format!("{}/cpu-limit", path)).exists(),
        "cpu-limit file should exist"
    );
    assert!(
        std::path::Path::new(&format!("{}/mem-limit", path)).exists(),
        "mem-limit file should exist"
    );
}

// ── Projected volume: multiple sources combined ──────────────────

#[tokio::test]
async fn test_projected_volume_combines_configmap_secret_downward_api() {
    use base64::Engine;
    use serde_json::json;
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "app-config", "namespace": "default"},
        "data": {"config.yaml": "app: test"}
    });
    db.create_resource("v1", "ConfigMap", Some("default"), "app-config", cm)
        .await
        .unwrap();

    let secret = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "app-secret", "namespace": "default"},
        "data": {
            "api-key": base64::engine::general_purpose::STANDARD.encode("secret-key-123")
        }
    });
    db.create_resource("v1", "Secret", Some("default"), "app-secret", secret)
        .await
        .unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "test-pod",
            "namespace": "default",
            "labels": {"app": "myapp"}
        },
        "spec": {"containers": []}
    });
    db.create_resource("v1", "Pod", Some("default"), "test-pod", pod)
        .await
        .unwrap();

    let sources = json!([
        {"configMap": {"name": "app-config", "items": [{"key": "config.yaml", "path": "config.yaml"}]}},
        {"secret": {"name": "app-secret", "items": [{"key": "api-key", "path": "api-key"}]}},
        {"downwardAPI": {"items": [{"path": "labels", "fieldRef": {"fieldPath": "metadata.labels"}}]}},
        {"serviceAccountToken": {"path": "token"}}
    ]);

    let path = create_projected_volume_at(ProjectedVolumeAtRequest {
        volumes_root: root,
        source_reader: &db,
        namespace: "default",
        pod_name: "test-pod",
        volume_name: "proj-vol",
        default_mode: None,
        sources: &sources,
        token: Some("my-sa-token"),
    })
    .await
    .unwrap();

    // All four source types produced files
    assert_eq!(
        crate::utils::read_utf8_file(format!("{}/config.yaml", path)).unwrap(),
        "app: test"
    );
    assert_eq!(
        crate::utils::read_utf8_file(format!("{}/api-key", path)).unwrap(),
        "secret-key-123"
    );
    assert_eq!(
        crate::utils::read_utf8_file(format!("{}/token", path)).unwrap(),
        "my-sa-token"
    );
    let labels = crate::utils::read_utf8_file(format!("{}/labels", path)).unwrap();
    assert!(
        labels.contains("app=\"myapp\""),
        "labels file should contain app label"
    );
}

// ── Projected volume: missing secret returns error ────────────────

#[tokio::test]
async fn test_projected_volume_missing_secret_returns_error() {
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
        {"secret": {"name": "nonexistent-secret"}}
    ]);

    let result = create_projected_volume_at(ProjectedVolumeAtRequest {
        volumes_root: root,
        source_reader: &db,
        namespace: "default",
        pod_name: "test-pod",
        volume_name: "proj-vol",
        default_mode: None,
        sources: &sources,
        token: None,
    })
    .await;

    assert!(result.is_err(), "should error when secret is not found");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("not found"),
        "error should mention 'not found', got: {}",
        err_msg
    );
}

// ── Projected volume: missing SA token returns error ──────────────

#[tokio::test]
async fn test_projected_volume_missing_sa_token_returns_error() {
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

    // SA token source but no token provided
    let sources = json!([
        {"serviceAccountToken": {"path": "token"}}
    ]);

    let result = create_projected_volume_at(ProjectedVolumeAtRequest {
        volumes_root: root,
        source_reader: &db,
        namespace: "default",
        pod_name: "test-pod",
        volume_name: "proj-vol",
        default_mode: None,
        sources: &sources,
        token: None,
    })
    .await;

    assert!(
        result.is_err(),
        "should error when token is None for SA source"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("token"),
        "error should mention 'token', got: {}",
        err_msg
    );
}

// ── Projected volume: sources not an array returns error ──────────

#[tokio::test]
async fn test_projected_volume_sources_not_array_returns_error() {
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

    // Invalid: sources is an object, not an array
    let sources = json!({"configMap": {"name": "test"}});

    let result = create_projected_volume_at(ProjectedVolumeAtRequest {
        volumes_root: root,
        source_reader: &db,
        namespace: "default",
        pod_name: "test-pod",
        volume_name: "proj-vol",
        default_mode: None,
        sources: &sources,
        token: None,
    })
    .await;

    assert!(result.is_err(), "should error when sources is not an array");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("array"),
        "error should mention 'array', got: {}",
        err_msg
    );
}

// ── Projected volume: configMap with no data field returns error ──

#[tokio::test]
async fn test_projected_volume_configmap_no_data_field_returns_error() {
    use serde_json::json;
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    // ConfigMap without a "data" field
    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "empty-cm", "namespace": "default"}
    });
    db.create_resource("v1", "ConfigMap", Some("default"), "empty-cm", cm)
        .await
        .unwrap();

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
        {"configMap": {"name": "empty-cm"}}
    ]);

    let result = create_projected_volume_at(ProjectedVolumeAtRequest {
        volumes_root: root,
        source_reader: &db,
        namespace: "default",
        pod_name: "test-pod",
        volume_name: "proj-vol",
        default_mode: None,
        sources: &sources,
        token: None,
    })
    .await;

    assert!(result.is_err(), "should error when configMap has no data");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("no data"),
        "error should mention 'no data', got: {}",
        err_msg
    );
}

// ── Projected volume: SA token default path ──────────────────────

#[tokio::test]
async fn test_projected_volume_sa_token_default_path_is_token() {
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

    // serviceAccountToken without explicit path — should default to "token"
    let sources = json!([
        {"serviceAccountToken": {}}
    ]);

    let path = create_projected_volume_at(ProjectedVolumeAtRequest {
        volumes_root: root,
        source_reader: &db,
        namespace: "default",
        pod_name: "test-pod",
        volume_name: "proj-vol",
        default_mode: None,
        sources: &sources,
        token: Some("default-path-token"),
    })
    .await
    .unwrap();

    assert_eq!(
        crate::utils::read_utf8_file(format!("{}/token", path)).unwrap(),
        "default-path-token",
        "SA token without explicit path should write to 'token'"
    );
}

// ── Projected volume: configMap default mode on files ─────────────
