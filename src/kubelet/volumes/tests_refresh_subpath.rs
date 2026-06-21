use super::*;

fn make_pod_reader(
    db: &crate::datastore::sqlite::Datastore,
) -> std::sync::Arc<dyn crate::kubelet::pod_repository::PodReader> {
    use crate::side_effects::{SideEffectMetrics, SideEffectRegistry};
    use crate::task_supervisor::{TaskCategoryConfig, TaskSupervisor};
    let supervisor = std::sync::Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let metrics = SideEffectMetrics::new();
    let side_effects = std::sync::Arc::new(SideEffectRegistry::new());
    std::sync::Arc::new(crate::kubelet::pod_repository::PodRepository::new(
        std::sync::Arc::new(db.clone()),
        supervisor,
        side_effects,
        metrics,
    ))
}

#[tokio::test]
async fn test_projected_volume_configmap_uses_default_mode() {
    use serde_json::json;
    use std::os::unix::fs::PermissionsExt;
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "mode-test-cm", "namespace": "default"},
        "data": {"file.txt": "content"}
    });
    db.create_resource("v1", "ConfigMap", Some("default"), "mode-test-cm", cm)
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
        {"configMap": {"name": "mode-test-cm"}}
    ]);

    // defaultMode 0o400
    let path = create_projected_volume_at(ProjectedVolumeAtRequest {
        volumes_root: root,
        source_reader: &db,
        namespace: "default",
        pod_name: "test-pod",
        volume_name: "proj-vol",
        default_mode: Some(0o400),
        sources: &sources,
        token: None,
    })
    .await
    .unwrap();

    let mode = std::fs::metadata(format!("{}/file.txt", path))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(
        mode, 0o400,
        "configMap file should use defaultMode 0400, got {:o}",
        mode
    );
}

// ── Projected volume: downwardAPI with custom per-file mode ──────

#[tokio::test]
async fn test_projected_volume_downward_api_per_file_mode() {
    use serde_json::json;
    use std::os::unix::fs::PermissionsExt;
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "test-pod", "namespace": "ns1"},
        "spec": {"containers": []}
    });
    db.create_resource("v1", "Pod", Some("ns1"), "test-pod", pod)
        .await
        .unwrap();

    // Per-file mode 0o444 (292 decimal) on downwardAPI item
    let sources = json!([
        {"downwardAPI": {"items": [
            {"path": "namespace", "fieldRef": {"fieldPath": "metadata.namespace"}, "mode": 292}
        ]}}
    ]);

    let path = create_projected_volume_at(ProjectedVolumeAtRequest {
        volumes_root: root,
        source_reader: &db,
        namespace: "ns1",
        pod_name: "test-pod",
        volume_name: "proj-vol",
        default_mode: Some(0o644),
        sources: &sources,
        token: None,
    })
    .await
    .unwrap();

    let content = crate::utils::read_utf8_file(format!("{}/namespace", path)).unwrap();
    assert_eq!(content, "ns1");

    let mode = std::fs::metadata(format!("{}/namespace", path))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(
        mode, 0o444,
        "per-file mode 0444 should override defaultMode, got {:o}",
        mode
    );
}

#[tokio::test]
async fn test_extract_resource_field_ref_limits_memory_absent_returns_node_allocatable() {
    use serde_json::json;
    let db = crate::datastore::test_support::in_memory().await;

    // Pod with NO resource limits set — like a burstable or best-effort pod
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "no-limits-pod", "namespace": "default"},
        "spec": {
            "containers": [{"name": "app", "image": "nginx"}]
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "no-limits-pod", pod)
        .await
        .unwrap();

    let pod_res = db
        .get_resource("v1", "Pod", Some("default"), "no-limits-pod")
        .await
        .unwrap()
        .unwrap();

    // When limits.memory is absent, must return node allocatable (non-zero)
    let mem = extract_resource_field_ref(&pod_res.data, Some("app"), "limits.memory").unwrap();
    assert_ne!(
        mem, "0",
        "limits.memory with no limit must return node allocatable, not 0"
    );

    // Verify it's a valid positive integer (bytes)
    let bytes: u64 = mem
        .parse()
        .expect("limits.memory fallback must be a numeric byte count");
    assert!(bytes > 0, "node allocatable memory must be positive");
}

#[tokio::test]
async fn test_secret_volume_refresh_on_update() {
    use std::fs;
    use tempfile::TempDir;

    let tmp = TempDir::new().unwrap();
    let volumes_root = tmp.path().to_str().unwrap();

    let db = crate::datastore::test_support::in_memory().await;

    // Create a Secret with initial data
    use base64::Engine;
    let initial_value = base64::engine::general_purpose::STANDARD.encode("initial-password");
    db.create_resource(
        "v1",
        "Secret",
        Some("default"),
        "my-secret",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "my-secret", "namespace": "default"},
            "data": {"password": initial_value}
        }),
    )
    .await
    .unwrap();

    // Create a Running pod that mounts the secret
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "test-pod",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "test-pod", "namespace": "default"},
            "spec": {
                "containers": [{"name": "app", "image": "nginx"}],
                "volumes": [{
                    "name": "secret-vol",
                    "secret": {"secretName": "my-secret"}
                }]
            },
            "status": {"phase": "Running"}
        }),
    )
    .await
    .unwrap();

    // Create initial secret volume on disk
    let vol_path = format!(
        "{}/default_test-pod/volumes/secret/secret-vol",
        volumes_root
    );
    fs::create_dir_all(&vol_path).unwrap();
    fs::write(format!("{}/password", vol_path), "initial-password").unwrap();

    // Verify initial content
    let content = crate::utils::read_utf8_file(format!("{}/password", vol_path)).unwrap();
    assert_eq!(content, "initial-password");

    // Update the Secret with new data
    let updated_value = base64::engine::general_purpose::STANDARD.encode("new-password");
    let current = db
        .get_resource("v1", "Secret", Some("default"), "my-secret")
        .await
        .unwrap()
        .unwrap();
    db.update_resource(
        "v1",
        "Secret",
        Some("default"),
        "my-secret",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "my-secret", "namespace": "default"},
            "data": {"password": updated_value}
        }),
        current.resource_version,
    )
    .await
    .unwrap();

    // Call refresh
    let updated = db
        .get_resource("v1", "Secret", Some("default"), "my-secret")
        .await
        .unwrap()
        .unwrap();
    let before = blocking_fs_keyed_call_count();
    refresh_secret_configmap_volumes_from_event(
        "Secret",
        "default",
        "my-secret",
        updated.data.as_ref(),
        volumes_root,
        make_pod_reader(&db).as_ref(),
    )
    .await
    .unwrap();
    assert!(
        blocking_fs_keyed_call_count() > before,
        "secret refresh must run through keyed blocking filesystem boundary"
    );

    // Verify file content updated
    let updated_content = crate::utils::read_utf8_file(format!("{}/password", vol_path)).unwrap();
    assert_eq!(updated_content, "new-password");
}

#[tokio::test]
async fn test_projected_secret_refresh_uses_event_payload_for_new_keys() {
    use base64::Engine;
    use std::fs;
    use tempfile::TempDir;

    let tmp = TempDir::new().unwrap();
    let volumes_root = tmp.path().to_str().unwrap();
    let db = crate::datastore::test_support::in_memory().await;
    let old_value = base64::engine::general_purpose::STANDARD.encode("value-1");
    let new_value = base64::engine::general_purpose::STANDARD.encode("value-3");

    db.create_resource(
        "v1",
        "Secret",
        Some("default"),
        "projected-secret",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "projected-secret", "namespace": "default"},
            "data": {"data-1": old_value}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "projected-pod",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "projected-pod", "namespace": "default"},
            "spec": {
                "containers": [{"name": "app", "image": "busybox"}],
                "volumes": [{
                    "name": "projected-vol",
                    "projected": {
                        "sources": [{"secret": {"name": "projected-secret", "optional": true}}]
                    }
                }]
            },
            "status": {"phase": "Running"}
        }),
    )
    .await
    .unwrap();

    let vol_path = format!(
        "{}/default_projected-pod/volumes/projected/projected-vol",
        volumes_root
    );
    fs::create_dir_all(&vol_path).unwrap();
    fs::write(format!("{}/data-1", vol_path), "value-1").unwrap();
    let event_secret = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "projected-secret", "namespace": "default"},
        "data": {
            "data-1": old_value,
            "data-3": new_value
        }
    });

    refresh_secret_configmap_volumes_from_event(
        "Secret",
        "default",
        "projected-secret",
        &event_secret,
        volumes_root,
        make_pod_reader(&db).as_ref(),
    )
    .await
    .unwrap();

    let data_3 = crate::utils::read_utf8_file(format!("{}/data-3", vol_path)).unwrap();
    assert_eq!(data_3, "value-3");
}

#[tokio::test]
async fn test_projected_secret_delete_event_clears_volume_with_stale_db_secret() {
    use base64::Engine;
    use std::fs;
    use tempfile::TempDir;

    let tmp = TempDir::new().unwrap();
    let volumes_root = tmp.path().to_str().unwrap();
    let db = crate::datastore::test_support::in_memory().await;
    let old_value = base64::engine::general_purpose::STANDARD.encode("value-1");

    db.create_resource(
        "v1",
        "Secret",
        Some("default"),
        "deleted-secret",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "deleted-secret", "namespace": "default"},
            "data": {"data-1": old_value}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "projected-pod",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "projected-pod", "namespace": "default"},
            "spec": {
                "containers": [{"name": "app", "image": "busybox"}],
                "volumes": [{
                    "name": "projected-vol",
                    "projected": {
                        "sources": [{"secret": {"name": "deleted-secret", "optional": true}}]
                    }
                }]
            },
            "status": {"phase": "Running"}
        }),
    )
    .await
    .unwrap();

    let vol_path = format!(
        "{}/default_projected-pod/volumes/projected/projected-vol",
        volumes_root
    );
    fs::create_dir_all(&vol_path).unwrap();
    fs::write(format!("{}/data-1", vol_path), "value-1").unwrap();

    refresh_secret_configmap_volumes_after_delete(
        "Secret",
        "default",
        "deleted-secret",
        volumes_root,
        make_pod_reader(&db).as_ref(),
    )
    .await
    .unwrap();

    assert!(
        !std::path::Path::new(&format!("{}/data-1", vol_path)).exists(),
        "deleted Secret watch events must clear projected volume files even when the local DB still has the old Secret"
    );
}

#[tokio::test]
async fn test_projected_configmap_delete_event_clears_existing_volume_with_missing_phase() {
    use std::fs;
    use tempfile::TempDir;

    let tmp = TempDir::new().unwrap();
    let volumes_root = tmp.path().to_str().unwrap();
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "deleted-config",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "deleted-config", "namespace": "default"},
            "data": {"data-1": "value-1"}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "projected-pod",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "projected-pod", "namespace": "default"},
            "spec": {
                "containers": [{"name": "app", "image": "busybox"}],
                "volumes": [{
                    "name": "deletecm-volume",
                    "projected": {
                        "sources": [{"configMap": {"name": "deleted-config", "optional": true}}]
                    }
                }]
            }
        }),
    )
    .await
    .unwrap();

    let vol_path = format!(
        "{}/default_projected-pod/volumes/projected/deletecm-volume",
        volumes_root
    );
    fs::create_dir_all(&vol_path).unwrap();
    fs::write(format!("{}/data-1", vol_path), "value-1").unwrap();

    refresh_secret_configmap_volumes_after_delete(
        "ConfigMap",
        "default",
        "deleted-config",
        volumes_root,
        make_pod_reader(&db).as_ref(),
    )
    .await
    .unwrap();

    assert!(
        !std::path::Path::new(&format!("{}/data-1", vol_path)).exists(),
        "deleted ConfigMap watch events must clear an existing mounted projected volume even when the fresh Pod list has not observed status.phase yet"
    );
}

#[tokio::test]
async fn test_secret_delete_event_clears_direct_volume_with_stale_db_secret() {
    use base64::Engine;
    use std::fs;
    use tempfile::TempDir;

    let tmp = TempDir::new().unwrap();
    let volumes_root = tmp.path().to_str().unwrap();
    let db = crate::datastore::test_support::in_memory().await;
    let old_value = base64::engine::general_purpose::STANDARD.encode("value-1");

    db.create_resource(
        "v1",
        "Secret",
        Some("default"),
        "deleted-secret",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "deleted-secret", "namespace": "default"},
            "data": {"data-1": old_value}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "secret-pod",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "secret-pod", "namespace": "default"},
            "spec": {
                "containers": [{"name": "app", "image": "busybox"}],
                "volumes": [{
                    "name": "secret-vol",
                    "secret": {"secretName": "deleted-secret", "optional": true}
                }]
            },
            "status": {"phase": "Running"}
        }),
    )
    .await
    .unwrap();

    let vol_path = format!(
        "{}/default_secret-pod/volumes/secret/secret-vol",
        volumes_root
    );
    fs::create_dir_all(&vol_path).unwrap();
    fs::write(format!("{}/data-1", vol_path), "value-1").unwrap();

    refresh_secret_configmap_volumes_after_delete(
        "Secret",
        "default",
        "deleted-secret",
        volumes_root,
        make_pod_reader(&db).as_ref(),
    )
    .await
    .unwrap();

    assert!(
        !std::path::Path::new(&format!("{}/data-1", vol_path)).exists(),
        "deleted Secret watch events must clear direct volume files even when the local DB still has the old Secret"
    );
}

#[tokio::test]
async fn test_configmap_volume_refresh_uses_event_payload_for_new_keys() {
    use std::fs;
    use tempfile::TempDir;

    let tmp = TempDir::new().unwrap();
    let volumes_root = tmp.path().to_str().unwrap();
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "direct-config",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "direct-config", "namespace": "default"},
            "data": {"data-1": "value-1"}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "cm-pod",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "cm-pod", "namespace": "default"},
            "spec": {
                "containers": [{"name": "app", "image": "busybox"}],
                "volumes": [{
                    "name": "config-vol",
                    "configMap": {"name": "direct-config", "optional": true}
                }]
            },
            "status": {"phase": "Running"}
        }),
    )
    .await
    .unwrap();

    let vol_path = format!(
        "{}/default_cm-pod/volumes/config-map/config-vol",
        volumes_root
    );
    fs::create_dir_all(&vol_path).unwrap();
    fs::write(format!("{}/data-1", vol_path), "value-1").unwrap();

    let event_configmap = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "direct-config", "namespace": "default"},
        "data": {
            "data-1": "value-1",
            "data-3": "value-3"
        }
    });

    refresh_secret_configmap_volumes_from_event(
        "ConfigMap",
        "default",
        "direct-config",
        &event_configmap,
        volumes_root,
        make_pod_reader(&db).as_ref(),
    )
    .await
    .unwrap();

    let data_3 = crate::utils::read_utf8_file(format!("{}/data-3", vol_path)).unwrap();
    assert_eq!(data_3, "value-3");
}

#[tokio::test]
async fn test_configmap_volume_refresh_on_update() {
    use std::fs;
    use tempfile::TempDir;

    let tmp = TempDir::new().unwrap();
    let volumes_root = tmp.path().to_str().unwrap();

    let db = crate::datastore::test_support::in_memory().await;

    // Create a ConfigMap with initial data
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "my-config",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "my-config", "namespace": "default"},
            "data": {"app.conf": "key=old-value"}
        }),
    )
    .await
    .unwrap();

    // Create a Running pod that mounts the configmap
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "cm-pod",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "cm-pod", "namespace": "default"},
            "spec": {
                "containers": [{"name": "app", "image": "nginx"}],
                "volumes": [{
                    "name": "config-vol",
                    "configMap": {"name": "my-config"}
                }]
            },
            "status": {"phase": "Running"}
        }),
    )
    .await
    .unwrap();

    // Create initial configmap volume on disk
    let vol_path = format!(
        "{}/default_cm-pod/volumes/config-map/config-vol",
        volumes_root
    );
    fs::create_dir_all(&vol_path).unwrap();
    fs::write(format!("{}/app.conf", vol_path), "key=old-value").unwrap();

    // Update the ConfigMap
    let current = db
        .get_resource("v1", "ConfigMap", Some("default"), "my-config")
        .await
        .unwrap()
        .unwrap();
    db.update_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "my-config",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "my-config", "namespace": "default"},
            "data": {"app.conf": "key=new-value"}
        }),
        current.resource_version,
    )
    .await
    .unwrap();

    // Call refresh
    let updated = db
        .get_resource("v1", "ConfigMap", Some("default"), "my-config")
        .await
        .unwrap()
        .unwrap();
    let before = blocking_fs_keyed_call_count();
    refresh_secret_configmap_volumes_from_event(
        "ConfigMap",
        "default",
        "my-config",
        updated.data.as_ref(),
        volumes_root,
        make_pod_reader(&db).as_ref(),
    )
    .await
    .unwrap();
    assert!(
        blocking_fs_keyed_call_count() > before,
        "configmap refresh must run through keyed blocking filesystem boundary"
    );

    // Verify file content updated
    let updated_content = crate::utils::read_utf8_file(format!("{}/app.conf", vol_path)).unwrap();
    assert_eq!(updated_content, "key=new-value");
}

#[tokio::test]
async fn test_configmap_volume_refresh_prunes_removed_keys() {
    use std::fs;
    use tempfile::TempDir;

    let tmp = TempDir::new().unwrap();
    let volumes_root = tmp.path().to_str().unwrap();

    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "my-config",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "my-config", "namespace": "default"},
            "data": {"data-1": "value-1", "data-2": "value-2"}
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "cm-pod",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "cm-pod", "namespace": "default"},
            "spec": {
                "containers": [{"name": "app", "image": "nginx"}],
                "volumes": [{"name": "config-vol", "configMap": {"name": "my-config"}}]
            },
            "status": {"phase": "Running"}
        }),
    )
    .await
    .unwrap();

    let vol_path = format!(
        "{}/default_cm-pod/volumes/config-map/config-vol",
        volumes_root
    );
    fs::create_dir_all(&vol_path).unwrap();
    fs::write(format!("{}/data-1", vol_path), "value-1").unwrap();
    fs::write(format!("{}/data-2", vol_path), "value-2").unwrap();

    let current = db
        .get_resource("v1", "ConfigMap", Some("default"), "my-config")
        .await
        .unwrap()
        .unwrap();
    db.update_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "my-config",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "my-config", "namespace": "default"},
            "data": {"data-2": "value-2-new"}
        }),
        current.resource_version,
    )
    .await
    .unwrap();

    let updated = db
        .get_resource("v1", "ConfigMap", Some("default"), "my-config")
        .await
        .unwrap()
        .unwrap();
    refresh_secret_configmap_volumes_from_event(
        "ConfigMap",
        "default",
        "my-config",
        updated.data.as_ref(),
        volumes_root,
        make_pod_reader(&db).as_ref(),
    )
    .await
    .unwrap();

    assert!(
        !std::path::Path::new(&format!("{}/data-1", vol_path)).exists(),
        "removed key file must be removed from mounted volume"
    );
    let updated = crate::utils::read_utf8_file(format!("{}/data-2", vol_path)).unwrap();
    assert_eq!(updated, "value-2-new");
}

#[tokio::test]
async fn test_configmap_volume_refresh_clears_files_on_source_delete() {
    use std::fs;
    use tempfile::TempDir;

    let tmp = TempDir::new().unwrap();
    let volumes_root = tmp.path().to_str().unwrap();

    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "my-config",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "my-config", "namespace": "default"},
            "data": {"data-1": "value-1"}
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "cm-pod",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "cm-pod", "namespace": "default"},
            "spec": {
                "containers": [{"name": "app", "image": "nginx"}],
                "volumes": [{
                    "name": "config-vol",
                    "configMap": {"name": "my-config", "optional": true}
                }]
            },
            "status": {"phase": "Running"}
        }),
    )
    .await
    .unwrap();

    let vol_path = format!(
        "{}/default_cm-pod/volumes/config-map/config-vol",
        volumes_root
    );
    fs::create_dir_all(&vol_path).unwrap();
    fs::write(format!("{}/data-1", vol_path), "value-1").unwrap();

    db.delete_resource("v1", "ConfigMap", Some("default"), "my-config")
        .await
        .unwrap();

    refresh_secret_configmap_volumes_after_delete(
        "ConfigMap",
        "default",
        "my-config",
        volumes_root,
        make_pod_reader(&db).as_ref(),
    )
    .await
    .unwrap();

    assert!(
        !std::path::Path::new(&format!("{}/data-1", vol_path)).exists(),
        "files must be removed when source ConfigMap is deleted"
    );
}

#[tokio::test]
async fn test_secret_volume_refreshes_existing_terminal_pod_mounts() {
    use std::fs;
    use tempfile::TempDir;

    let tmp = TempDir::new().unwrap();
    let volumes_root = tmp.path().to_str().unwrap();

    let db = crate::datastore::test_support::in_memory().await;

    // Create a Secret
    use base64::Engine;
    let val = base64::engine::general_purpose::STANDARD.encode("data");
    db.create_resource(
        "v1",
        "Secret",
        Some("default"),
        "skip-secret",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "skip-secret", "namespace": "default"},
            "data": {"key": val}
        }),
    )
    .await
    .unwrap();

    // Create a Succeeded pod. The API phase can race ahead of source watch
    // delivery under load while the node-local mounted volume still exists.
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "done-pod",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "done-pod", "namespace": "default"},
            "spec": {
                "containers": [{"name": "app", "image": "nginx"}],
                "volumes": [{"name": "s", "secret": {"secretName": "skip-secret"}}]
            },
            "status": {"phase": "Succeeded"}
        }),
    )
    .await
    .unwrap();

    // Create volume dir with stale content
    let vol_path = format!("{}/default_done-pod/volumes/secret/s", volumes_root);
    fs::create_dir_all(&vol_path).unwrap();
    fs::write(format!("{}/key", vol_path), "stale").unwrap();

    // Refresh should use mounted volume existence, not API phase, as the
    // node-local truth that the Secret projection can still need updates.
    let secret = db
        .get_resource("v1", "Secret", Some("default"), "skip-secret")
        .await
        .unwrap()
        .unwrap();
    refresh_secret_configmap_volumes_from_event(
        "Secret",
        "default",
        "skip-secret",
        secret.data.as_ref(),
        volumes_root,
        make_pod_reader(&db).as_ref(),
    )
    .await
    .unwrap();

    let content = crate::utils::read_utf8_file(format!("{}/key", vol_path)).unwrap();
    assert_eq!(
        content, "data",
        "terminal API phase must not suppress refresh for an existing mounted Secret volume"
    );
}

// ========================
// validate_volume_subpaths tests
// ========================

#[test]
fn test_validate_subpath_valid_relative_path_succeeds() {
    let pod = serde_json::json!({
        "spec": {
            "containers": [{
                "name": "app",
                "volumeMounts": [{
                    "name": "data",
                    "mountPath": "/data",
                    "subPath": "config/app.conf"
                }]
            }]
        }
    });
    assert!(validate_volume_subpaths(&pod).is_ok());
}

#[test]
fn test_validate_subpath_absolute_path_rejected() {
    let pod = serde_json::json!({
        "spec": {
            "containers": [{
                "name": "app",
                "volumeMounts": [{
                    "name": "data",
                    "mountPath": "/data",
                    "subPath": "/etc/passwd"
                }]
            }]
        }
    });
    let err = validate_volume_subpaths(&pod).unwrap_err();
    assert!(err.contains("must be a relative path"), "Error: {}", err);
}

#[test]
fn test_validate_subpath_dotdot_traversal_rejected() {
    let pod = serde_json::json!({
        "spec": {
            "containers": [{
                "name": "app",
                "volumeMounts": [{
                    "name": "data",
                    "mountPath": "/data",
                    "subPath": "../secrets"
                }]
            }]
        }
    });
    let err = validate_volume_subpaths(&pod).unwrap_err();
    assert!(err.contains("must not contain '..'"), "Error: {}", err);
}

#[test]
fn test_validate_subpath_dotdot_in_middle_rejected() {
    let pod = serde_json::json!({
        "spec": {
            "containers": [{
                "name": "app",
                "volumeMounts": [{
                    "name": "data",
                    "mountPath": "/data",
                    "subPath": "foo/../bar"
                }]
            }]
        }
    });
    let err = validate_volume_subpaths(&pod).unwrap_err();
    assert!(err.contains("must not contain '..'"), "Error: {}", err);
}

#[test]
fn test_validate_subpath_expr_absolute_rejected() {
    let pod = serde_json::json!({
        "spec": {
            "containers": [{
                "name": "app",
                "volumeMounts": [{
                    "name": "data",
                    "mountPath": "/data",
                    "subPathExpr": "/$(POD_NAME)"
                }]
            }]
        }
    });
    let err = validate_volume_subpaths(&pod).unwrap_err();
    assert!(err.contains("must be a relative path"), "Error: {}", err);
}

#[test]
fn test_validate_subpath_expr_expanded_absolute_deferred_to_kubelet() {
    let pod = serde_json::json!({
        "spec": {
            "containers": [{
                "name": "app",
                "env": [{"name": "BAD", "value": "/etc/passwd"}],
                "volumeMounts": [{
                    "name": "data",
                    "mountPath": "/data",
                    "subPathExpr": "$(BAD)"
                }]
            }]
        }
    });
    assert!(
        validate_volume_subpaths(&pod).is_ok(),
        "API admission validates raw subPathExpr only; kubelet validates expanded values"
    );
}

#[test]
fn test_validate_subpath_expr_expanded_backticks_deferred_to_kubelet() {
    let pod = serde_json::json!({
        "spec": {
            "containers": [{
                "name": "app",
                "env": [{"name": "BAD", "value": "`uname`"}],
                "volumeMounts": [{
                    "name": "data",
                    "mountPath": "/data",
                    "subPathExpr": "$(BAD)"
                }]
            }]
        }
    });
    assert!(
        validate_volume_subpaths(&pod).is_ok(),
        "API admission validates raw subPathExpr only; kubelet validates expanded values"
    );
}

#[test]
fn test_validate_subpath_no_volumemounts_succeeds() {
    let pod = serde_json::json!({
        "spec": {
            "containers": [{"name": "app", "image": "nginx"}]
        }
    });
    assert!(validate_volume_subpaths(&pod).is_ok());
}

#[test]
fn test_validate_subpath_no_spec_succeeds() {
    let pod = serde_json::json!({"metadata": {"name": "test"}});
    assert!(validate_volume_subpaths(&pod).is_ok());
}

#[test]
fn test_validate_subpath_init_containers_also_validated() {
    let pod = serde_json::json!({
        "spec": {
            "initContainers": [{
                "name": "init",
                "volumeMounts": [{
                    "name": "data",
                    "mountPath": "/data",
                    "subPath": "/absolute"
                }]
            }],
            "containers": [{"name": "app", "image": "nginx"}]
        }
    });
    let err = validate_volume_subpaths(&pod).unwrap_err();
    assert!(
        err.contains("initContainers"),
        "Error should mention initContainers: {}",
        err
    );
}
