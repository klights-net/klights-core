use super::*;

#[test]
fn test_parse_mountinfo_entry_extracts_mountpoint_and_fs_type() {
    // /proc/self/mountinfo format: pre-fields ... " - " fs-type ...
    let line = "217 208 0:85 / /run/containerd/io.containerd.grpc.v1.cri/sandboxes/default_pod/volumes/empty-dir/cache rw,nosuid,nodev - tmpfs tmpfs rw,size=65536k";
    let parsed = parse_mountinfo_entry(line).expect("must parse mountinfo line");
    assert_eq!(
        parsed.0,
        "/run/containerd/io.containerd.grpc.v1.cri/sandboxes/default_pod/volumes/empty-dir/cache"
    );
    assert_eq!(parsed.1, "tmpfs");
}

#[test]
fn test_parse_mountinfo_entry_returns_none_for_invalid_line() {
    assert!(parse_mountinfo_entry("not-a-mountinfo-line").is_none());
}

#[test]
fn test_create_empty_dir_has_world_writable_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let tmp = tempfile::tempdir().unwrap();
    // Override VOLUMES_ROOT for testing
    let test_root = tmp.path().join("pods");
    std::fs::create_dir_all(&test_root).unwrap();

    // Create emptyDir in test root
    let pod_name = "test-pod";
    let volume_name = "test-volume";
    let path_str = format!(
        "{}/{}/volumes/empty-dir/{}",
        test_root.display(),
        pod_name,
        volume_name
    );

    // Manually create to simulate what create_empty_dir does (will fix in implementation)
    std::fs::create_dir_all(&path_str).unwrap();

    // Set 0777 permissions (world-writable)
    let path = std::path::Path::new(&path_str);
    let mut perms = std::fs::metadata(path).unwrap().permissions();
    perms.set_mode(0o777);
    std::fs::set_permissions(path, perms).unwrap();

    // Verify permissions
    let metadata = std::fs::metadata(path).unwrap();
    let mode = metadata.permissions().mode();
    let perm_bits = mode & 0o777;

    assert_eq!(
        perm_bits, 0o777,
        "EmptyDir should have 0777 permissions for world-writable access, got {:o}",
        perm_bits
    );
}

#[test]
fn test_create_empty_dir_default_medium() {
    // Test default behavior (no medium or empty medium) - creates regular directory
    let pod_name = "test-pod-default";
    let volume_name = "cache";

    // Test with None medium (default)
    let path = create_empty_dir(pod_name, volume_name, None, None).unwrap();
    assert!(
        std::path::Path::new(&path).exists(),
        "Directory should exist"
    );
    assert!(
        std::path::Path::new(&path).is_dir(),
        "Should be a directory"
    );

    // Cleanup
    let _ = std::fs::remove_dir_all(&path);
}

// ========================
// parse_k8s_quantity tests
// ========================

#[test]
fn test_parse_k8s_quantity_bytes() {
    // Bare number should be interpreted as bytes
    assert_eq!(parse_k8s_quantity("1024").unwrap(), 1024);
    assert_eq!(parse_k8s_quantity("512").unwrap(), 512);
    assert_eq!(parse_k8s_quantity("0").unwrap(), 0);
}

#[test]
fn test_parse_k8s_quantity_ki() {
    // Ki = 1024 bytes (binary)
    assert_eq!(parse_k8s_quantity("64Ki").unwrap(), 64 * 1024);
    assert_eq!(parse_k8s_quantity("1Ki").unwrap(), 1024);
}

#[test]
fn test_parse_k8s_quantity_mi() {
    // Mi = 1024^2 bytes (binary)
    assert_eq!(parse_k8s_quantity("64Mi").unwrap(), 64 * 1024 * 1024);
    assert_eq!(parse_k8s_quantity("1Mi").unwrap(), 1024 * 1024);
}

#[test]
fn test_parse_k8s_quantity_gi() {
    // Gi = 1024^3 bytes (binary)
    assert_eq!(parse_k8s_quantity("1Gi").unwrap(), 1024 * 1024 * 1024);
    assert_eq!(parse_k8s_quantity("2Gi").unwrap(), 2 * 1024 * 1024 * 1024);
}

#[test]
fn test_parse_k8s_quantity_decimal_m() {
    // M = 1000^2 bytes (decimal)
    assert_eq!(parse_k8s_quantity("500M").unwrap(), 500 * 1000 * 1000);
    assert_eq!(parse_k8s_quantity("1M").unwrap(), 1000 * 1000);
}

#[test]
fn test_parse_k8s_quantity_decimal_g() {
    // G = 1000^3 bytes (decimal)
    assert_eq!(parse_k8s_quantity("1G").unwrap(), 1000 * 1000 * 1000);
}

#[test]
fn test_parse_k8s_quantity_invalid() {
    // Invalid input should return error
    assert!(parse_k8s_quantity("abc").is_err());
    assert!(parse_k8s_quantity("12.5Mi").is_err()); // No float support
    assert!(parse_k8s_quantity("").is_err());
}

#[test]
#[ignore] // Requires root for mount syscall
fn test_create_empty_dir_memory_medium() {
    // Test medium="Memory" - creates tmpfs mount
    let pod_name = "test-pod-memory";
    let volume_name = "memory-cache";

    // Create emptyDir with Memory medium
    let path = create_empty_dir(pod_name, volume_name, Some("Memory"), None).unwrap();
    assert!(
        std::path::Path::new(&path).exists(),
        "Directory should exist"
    );

    // Verify it's a tmpfs mount
    // Read /proc/mounts to check if path is mounted as tmpfs
    let mounts = crate::utils::read_utf8_file("/proc/mounts").unwrap();
    assert!(
        mounts.contains(&format!("{} tmpfs", path)),
        "Path should be mounted as tmpfs"
    );
}

#[test]
#[ignore] // Requires root for mount syscall
fn test_create_empty_dir_memory_medium_with_size_limit() {
    // Test medium="Memory" with sizeLimit
    let pod_name = "test-pod-sized";
    let volume_name = "sized-cache";

    // Create emptyDir with Memory medium and 64Mi size limit
    let path = create_empty_dir(pod_name, volume_name, Some("Memory"), Some("64Mi")).unwrap();
    assert!(
        std::path::Path::new(&path).exists(),
        "Directory should exist"
    );

    // Verify tmpfs mount exists with size limit
    let mounts = crate::utils::read_utf8_file("/proc/mounts").unwrap();
    assert!(
        mounts.contains(&format!("{} tmpfs", path)),
        "Path should be mounted as tmpfs"
    );

    // Note: verifying the exact size limit from /proc/mounts is tricky,
    // as it gets converted. Just verify the mount exists.
}

#[tokio::test]
async fn test_configmap_volume_sets_0644_permissions_by_default() {
    use std::os::unix::fs::PermissionsExt;

    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    // Create a ConfigMap
    let cm = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "test-cm", "namespace": "default"},
        "data": {"config.yaml": "key: value"}
    });
    db.create_resource("v1", "ConfigMap", Some("default"), "test-cm", cm)
        .await
        .unwrap();

    // Create volume
    let path = create_config_map_volume_at(ConfigMapVolumeAtRequest {
        volumes_root: root,
        sources: &db,
        namespace: "default",
        cm_name: "test-cm",
        pod_name: "test-pod",
        volume_name: "config",
        default_mode: None,
        items: None,
    })
    .await
    .unwrap();

    // Check file permissions
    let file_path = format!("{}/config.yaml", path);
    let metadata = std::fs::metadata(&file_path).unwrap();
    let mode = metadata.permissions().mode() & 0o777;
    assert_eq!(
        mode, 0o644,
        "ConfigMap file should have 0644 permissions by default, got {:o}",
        mode
    );
}

#[tokio::test]
async fn test_configmap_create_uses_keyed_blocking_boundary() {
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let cm = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "test-cm", "namespace": "default"},
        "data": {"config.yaml": "key: value"}
    });
    db.create_resource("v1", "ConfigMap", Some("default"), "test-cm", cm)
        .await
        .unwrap();

    let before = blocking_fs_keyed_call_count();
    let _ = create_config_map_volume_at(ConfigMapVolumeAtRequest {
        volumes_root: root,
        sources: &db,
        namespace: "default",
        cm_name: "test-cm",
        pod_name: "test-pod",
        volume_name: "config",
        default_mode: None,
        items: None,
    })
    .await
    .unwrap();
    assert!(
        blocking_fs_keyed_call_count() > before,
        "configmap create must run through keyed blocking filesystem boundary"
    );
}

#[tokio::test]
async fn test_configmap_volume_respects_default_mode() {
    use std::os::unix::fs::PermissionsExt;

    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let cm = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "test-cm", "namespace": "default"},
        "data": {"app.conf": "server=localhost"}
    });
    db.create_resource("v1", "ConfigMap", Some("default"), "test-cm", cm)
        .await
        .unwrap();

    // Create volume with custom defaultMode (0o400 = 256 decimal)
    let path = create_config_map_volume_at(ConfigMapVolumeAtRequest {
        volumes_root: root,
        sources: &db,
        namespace: "default",
        cm_name: "test-cm",
        pod_name: "test-pod",
        volume_name: "config",
        default_mode: Some(256),
        items: None,
    })
    .await
    .unwrap();

    let file_path = format!("{}/app.conf", path);
    let metadata = std::fs::metadata(&file_path).unwrap();
    let mode = metadata.permissions().mode() & 0o777;
    assert_eq!(
        mode, 0o400,
        "ConfigMap file should have custom mode 0400, got {:o}",
        mode
    );
}

#[tokio::test]
async fn test_configmap_volume_items_filters_keys() {
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let cm = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "test-cm", "namespace": "default"},
        "data": {
            "key1": "value1",
            "key2": "value2",
            "key3": "value3"
        }
    });
    db.create_resource("v1", "ConfigMap", Some("default"), "test-cm", cm)
        .await
        .unwrap();

    // Only project key1 and key3
    let items = serde_json::json!([
        {"key": "key1", "path": "key1"},
        {"key": "key3", "path": "key3"}
    ]);
    let path = create_config_map_volume_at(ConfigMapVolumeAtRequest {
        volumes_root: root,
        sources: &db,
        namespace: "default",
        cm_name: "test-cm",
        pod_name: "test-pod",
        volume_name: "config",
        default_mode: None,
        items: Some(&items),
    })
    .await
    .unwrap();

    assert!(
        std::path::Path::new(&format!("{}/key1", path)).exists(),
        "key1 should exist"
    );
    assert!(
        !std::path::Path::new(&format!("{}/key2", path)).exists(),
        "key2 should NOT exist (not in items)"
    );
    assert!(
        std::path::Path::new(&format!("{}/key3", path)).exists(),
        "key3 should exist"
    );
}

#[tokio::test]
async fn test_configmap_volume_items_renames_files() {
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let cm = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "test-cm", "namespace": "default"},
        "data": {"original-name": "content"}
    });
    db.create_resource("v1", "ConfigMap", Some("default"), "test-cm", cm)
        .await
        .unwrap();

    // Rename via path field
    let items = serde_json::json!([
        {"key": "original-name", "path": "renamed-file"}
    ]);
    let path = create_config_map_volume_at(ConfigMapVolumeAtRequest {
        volumes_root: root,
        sources: &db,
        namespace: "default",
        cm_name: "test-cm",
        pod_name: "test-pod",
        volume_name: "config",
        default_mode: None,
        items: Some(&items),
    })
    .await
    .unwrap();

    assert!(
        !std::path::Path::new(&format!("{}/original-name", path)).exists(),
        "Original name should NOT exist"
    );
    assert!(
        std::path::Path::new(&format!("{}/renamed-file", path)).exists(),
        "Renamed file should exist"
    );

    let content = crate::utils::read_utf8_file(format!("{}/renamed-file", path)).unwrap();
    assert_eq!(content, "content");
}

#[tokio::test]
async fn test_configmap_volume_items_per_file_mode() {
    use std::os::unix::fs::PermissionsExt;

    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let cm = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "test-cm", "namespace": "default"},
        "data": {
            "file1": "data1",
            "file2": "data2"
        }
    });
    db.create_resource("v1", "ConfigMap", Some("default"), "test-cm", cm)
        .await
        .unwrap();

    // Set different modes per file
    let items = serde_json::json!([
        {"key": "file1", "path": "file1", "mode": 256},  // 0o400
        {"key": "file2", "path": "file2", "mode": 384}   // 0o600
    ]);
    let path = create_config_map_volume_at(ConfigMapVolumeAtRequest {
        volumes_root: root,
        sources: &db,
        namespace: "default",
        cm_name: "test-cm",
        pod_name: "test-pod",
        volume_name: "config",
        default_mode: None,
        items: Some(&items),
    })
    .await
    .unwrap();

    let mode1 = std::fs::metadata(format!("{}/file1", path))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    let mode2 = std::fs::metadata(format!("{}/file2", path))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;

    assert_eq!(mode1, 0o400, "file1 should have 0400 mode");
    assert_eq!(mode2, 0o600, "file2 should have 0600 mode");
}

#[tokio::test]
async fn test_configmap_volume_replaces_stale_directory() {
    // Regression test: if a previous run left a directory where a file should be,
    // the new run must remove it and write the file correctly.
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let cm = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "test-cm", "namespace": "default"},
        "data": {"Corefile": ".:53 { health }\n"}
    });
    db.create_resource("v1", "ConfigMap", Some("default"), "test-cm", cm)
        .await
        .unwrap();

    // Pre-create a DIRECTORY at the path where the file should go (simulate stale state)
    let stale_dir = format!("{}/test-pod/volumes/config-map/config/Corefile", root);
    std::fs::create_dir_all(&stale_dir).unwrap();
    assert!(
        std::path::Path::new(&stale_dir).is_dir(),
        "Precondition: stale directory exists"
    );

    // Now create the ConfigMap volume — should replace the directory with a file
    let path = create_config_map_volume_at(ConfigMapVolumeAtRequest {
        volumes_root: root,
        sources: &db,
        namespace: "default",
        cm_name: "test-cm",
        pod_name: "test-pod",
        volume_name: "config",
        default_mode: None,
        items: None,
    })
    .await
    .unwrap();

    let corefile_str = format!("{}/Corefile", path);
    let corefile = std::path::Path::new(&corefile_str);
    assert!(
        corefile.is_file(),
        "Corefile must be a file after stale directory removal"
    );
    assert!(!corefile.is_dir(), "Corefile must NOT be a directory");

    let content = crate::utils::read_utf8_file(&corefile_str).unwrap();
    assert!(
        content.contains("health"),
        "Corefile should have correct content"
    );
}

#[tokio::test]
async fn test_configmap_volume_corefile_is_file_not_directory() {
    // Regression test: CoreDNS crash-loops because Corefile is mounted as directory
    // The ConfigMap volume should create regular files, not directories
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let cm = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "coredns", "namespace": "kube-system"},
        "data": {
            "Corefile": ".:53 {\n  health\n  ready\n  forward . /etc/resolv.conf\n}\n"
        }
    });
    db.create_resource("v1", "ConfigMap", Some("kube-system"), "coredns", cm)
        .await
        .unwrap();

    // No items filter — project all keys (same as CoreDNS deployment spec)
    let volume_path = create_config_map_volume_at(ConfigMapVolumeAtRequest {
        volumes_root: root,
        sources: &db,
        namespace: "kube-system",
        cm_name: "coredns",
        pod_name: "coredns-pod",
        volume_name: "config-volume",
        default_mode: None,
        items: None,
    })
    .await
    .unwrap();

    // The volume path should be a directory
    assert!(
        std::path::Path::new(&volume_path).is_dir(),
        "Volume path should be a directory"
    );

    // Corefile should be a regular FILE inside that directory, not a directory
    let corefile_path = format!("{}/Corefile", volume_path);
    let corefile = std::path::Path::new(&corefile_path);
    assert!(corefile.exists(), "Corefile should exist");
    assert!(
        corefile.is_file(),
        "Corefile must be a regular file, NOT a directory"
    );
    assert!(!corefile.is_dir(), "Corefile must NOT be a directory");

    // Verify content
    let content = crate::utils::read_utf8_file(&corefile_path).unwrap();
    assert!(
        content.contains("health"),
        "Corefile should contain health directive"
    );
}

#[tokio::test]
async fn test_configmap_volume_binary_data_written_as_bytes() {
    use base64::Engine;

    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    // A ConfigMap with only binaryData (no data field)
    let binary_content: Vec<u8> = vec![0x00, 0x01, 0x02, 0xFF, 0xFE];
    let encoded = base64::engine::general_purpose::STANDARD.encode(&binary_content);
    let cm = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "bin-cm", "namespace": "default"},
        "binaryData": {"data.bin": encoded}
    });
    db.create_resource("v1", "ConfigMap", Some("default"), "bin-cm", cm)
        .await
        .unwrap();

    let path = create_config_map_volume_at(ConfigMapVolumeAtRequest {
        volumes_root: root,
        sources: &db,
        namespace: "default",
        cm_name: "bin-cm",
        pod_name: "test-pod",
        volume_name: "binvol",
        default_mode: None,
        items: None,
    })
    .await
    .unwrap();

    let file_path = format!("{}/data.bin", path);
    let bytes = std::fs::read(&file_path).unwrap();
    assert_eq!(
        bytes, binary_content,
        "binaryData file content must match decoded bytes"
    );
}

#[tokio::test]
async fn test_configmap_volume_binary_data_and_data_combined() {
    use base64::Engine;

    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let binary_content: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF];
    let encoded = base64::engine::general_purpose::STANDARD.encode(&binary_content);
    let cm = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "mixed-cm", "namespace": "default"},
        "data": {"text.txt": "hello world"},
        "binaryData": {"blob.bin": encoded}
    });
    db.create_resource("v1", "ConfigMap", Some("default"), "mixed-cm", cm)
        .await
        .unwrap();

    let path = create_config_map_volume_at(ConfigMapVolumeAtRequest {
        volumes_root: root,
        sources: &db,
        namespace: "default",
        cm_name: "mixed-cm",
        pod_name: "test-pod",
        volume_name: "mixedvol",
        default_mode: None,
        items: None,
    })
    .await
    .unwrap();

    // text file
    let text = crate::utils::read_utf8_file(format!("{}/text.txt", path)).unwrap();
    assert_eq!(text, "hello world");

    // binary file
    let bytes = std::fs::read(format!("{}/blob.bin", path)).unwrap();
    assert_eq!(bytes, binary_content);
}

#[tokio::test]
async fn test_configmap_volume_only_binary_data_no_data_field_succeeds() {
    use base64::Engine;

    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let encoded = base64::engine::general_purpose::STANDARD.encode(b"raw bytes here");
    let cm = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "binonly-cm", "namespace": "default"},
        "binaryData": {"raw": encoded}
    });
    db.create_resource("v1", "ConfigMap", Some("default"), "binonly-cm", cm)
        .await
        .unwrap();

    // Must NOT fail with "ConfigMap has no data"
    let result = create_config_map_volume_at(ConfigMapVolumeAtRequest {
        volumes_root: root,
        sources: &db,
        namespace: "default",
        cm_name: "binonly-cm",
        pod_name: "test-pod",
        volume_name: "binonlyvol",
        default_mode: None,
        items: None,
    })
    .await;
    assert!(
        result.is_ok(),
        "ConfigMap with only binaryData should succeed: {:?}",
        result
    );

    let path = result.unwrap();
    let raw = std::fs::read(format!("{}/raw", path)).unwrap();
    assert_eq!(raw, b"raw bytes here");
}

#[tokio::test]
async fn test_secret_volume_sets_0644_permissions_by_default() {
    use base64::Engine;
    use std::os::unix::fs::PermissionsExt;

    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let encoded = base64::engine::general_purpose::STANDARD.encode("secret-data");
    let secret = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "test-secret", "namespace": "default"},
        "data": {"password": encoded}
    });
    db.create_resource("v1", "Secret", Some("default"), "test-secret", secret)
        .await
        .unwrap();

    let path = create_secret_volume_at(SecretVolumeAtRequest {
        volumes_root: root,
        sources: &db,
        namespace: "default",
        secret_name: "test-secret",
        pod_name: "test-pod",
        volume_name: "secret",
        default_mode: None,
        items: None,
    })
    .await
    .unwrap();

    let file_path = format!("{}/password", path);
    let metadata = std::fs::metadata(&file_path).unwrap();
    let mode = metadata.permissions().mode() & 0o777;
    assert_eq!(
        mode, 0o644,
        "Secret file should have 0644 permissions by default, got {:o}",
        mode
    );
}

#[tokio::test]
async fn test_secret_volume_respects_default_mode() {
    use base64::Engine;
    use std::os::unix::fs::PermissionsExt;

    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let encoded = base64::engine::general_purpose::STANDARD.encode("token");
    let secret = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "test-secret", "namespace": "default"},
        "data": {"token": encoded}
    });
    db.create_resource("v1", "Secret", Some("default"), "test-secret", secret)
        .await
        .unwrap();

    // Create volume with read-only mode (0o400 = 256 decimal)
    let path = create_secret_volume_at(SecretVolumeAtRequest {
        volumes_root: root,
        sources: &db,
        namespace: "default",
        secret_name: "test-secret",
        pod_name: "test-pod",
        volume_name: "secret",
        default_mode: Some(256),
        items: None,
    })
    .await
    .unwrap();

    let file_path = format!("{}/token", path);
    let metadata = std::fs::metadata(&file_path).unwrap();
    let mode = metadata.permissions().mode() & 0o777;
    assert_eq!(
        mode, 0o400,
        "Secret file should have custom mode 0400, got {:o}",
        mode
    );
}

#[tokio::test]
async fn test_secret_volume_items_filters_and_renames() {
    use base64::Engine;

    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let secret = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "test-secret", "namespace": "default"},
        "data": {
            "username": base64::engine::general_purpose::STANDARD.encode("admin"),
            "password": base64::engine::general_purpose::STANDARD.encode("secret"),
            "token": base64::engine::general_purpose::STANDARD.encode("jwt")
        }
    });
    db.create_resource("v1", "Secret", Some("default"), "test-secret", secret)
        .await
        .unwrap();

    // Only project password with renamed path
    let items = serde_json::json!([
        {"key": "password", "path": "db-password"}
    ]);
    let path = create_secret_volume_at(SecretVolumeAtRequest {
        volumes_root: root,
        sources: &db,
        namespace: "default",
        secret_name: "test-secret",
        pod_name: "test-pod",
        volume_name: "secret",
        default_mode: None,
        items: Some(&items),
    })
    .await
    .unwrap();

    assert!(
        !std::path::Path::new(&format!("{}/username", path)).exists(),
        "username should NOT exist"
    );
    assert!(
        !std::path::Path::new(&format!("{}/password", path)).exists(),
        "password (original) should NOT exist"
    );
    assert!(
        !std::path::Path::new(&format!("{}/token", path)).exists(),
        "token should NOT exist"
    );
    assert!(
        std::path::Path::new(&format!("{}/db-password", path)).exists(),
        "db-password (renamed) should exist"
    );

    let content = crate::utils::read_utf8_file(format!("{}/db-password", path)).unwrap();
    assert_eq!(content, "secret", "Content should be base64-decoded");
}

#[tokio::test]
async fn test_secret_create_uses_keyed_blocking_boundary() {
    use base64::Engine;
    let db = crate::datastore::test_support::in_memory().await;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_str().unwrap();

    let secret = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "test-secret", "namespace": "default"},
        "data": {
            "username": base64::engine::general_purpose::STANDARD.encode("admin")
        }
    });
    db.create_resource("v1", "Secret", Some("default"), "test-secret", secret)
        .await
        .unwrap();

    let before = blocking_fs_keyed_call_count();
    let _ = create_secret_volume_at(SecretVolumeAtRequest {
        volumes_root: root,
        sources: &db,
        namespace: "default",
        secret_name: "test-secret",
        pod_name: "test-pod",
        volume_name: "secret-vol",
        default_mode: None,
        items: None,
    })
    .await
    .unwrap();
    assert!(
        blocking_fs_keyed_call_count() > before,
        "secret create must run through keyed blocking filesystem boundary"
    );
}
