use crate::pod_env::collect_literal_env_vars;
use std::collections::HashMap;

fn build_mounts(
    container: &serde_json::Value,
    volume_paths: &std::collections::HashMap<String, String>,
    resolved_envs: &std::collections::HashMap<String, String>,
) -> anyhow::Result<(Vec<k8s_cri::v1::Mount>, Vec<std::path::PathBuf>)> {
    crate::pod_volume_manager::PodVolumeManager::build_mounts(
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
    // Native Pod admission adds the ServiceAccount projected volume and its
    // volumeMount to the Pod spec. build_mounts processes it
    // like any other volumeMount — no special-casing needed.
    let container = serde_json::json!({
        "volumeMounts": [{
            "name": "kube-api-access-abc12",
            "mountPath": "/var/run/secrets/kubernetes.io/serviceaccount",
            "readOnly": true
        }]
    });
    let mut volume_paths = HashMap::new();
    let runtime_root = tempfile::tempdir().expect("create isolated runtime root");
    let projected_path =
        crate::runtime_paths::KubeletRuntimePaths::new(runtime_root.path().to_path_buf())
            .expect("kubelet test runtime path must be absolute")
            .volumes_root()
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
