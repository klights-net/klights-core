use super::*;
use crate::kubelet::pod_env::{resolve_env_from, resolve_env_value_from};
use std::collections::HashMap;

fn test_pod_data() -> serde_json::Value {
    serde_json::json!({
        "metadata": {"name": "pod1", "namespace": "default", "uid": "uid1"}
    })
}

fn test_container_spec() -> serde_json::Value {
    serde_json::json!({"image": "nginx:latest"})
}

fn test_container_spec_with_image(image: &str) -> serde_json::Value {
    serde_json::json!({"image": image})
}

fn test_empty_env() -> HashMap<String, String> {
    HashMap::new()
}

#[test]
fn test_build_container_config_image() {
    let spec = test_container_spec_with_image("redis:7.0");
    let config = build_container_config(
        &spec,
        &test_pod_data(),
        "redis",
        "10.43.128.1",
        &[],
        &test_empty_env(),
    );
    let image = config.image.unwrap();
    assert_eq!(image.image, "redis:7.0");
}

#[test]
fn test_build_container_config_image_default() {
    let spec = test_container_spec();
    let config = build_container_config(
        &spec,
        &test_pod_data(),
        "c1",
        "10.43.128.1",
        &[],
        &test_empty_env(),
    );
    let image = config.image.unwrap();
    assert_eq!(image.image, "nginx:latest");
}

#[test]
fn test_build_container_config_command_and_args() {
    let spec = serde_json::json!({
        "image": "busybox",
        "command": ["/bin/sh", "-c"],
        "args": ["echo hello"]
    });
    let config = build_container_config(
        &spec,
        &test_pod_data(),
        "busybox",
        "10.43.128.1",
        &[],
        &test_empty_env(),
    );
    assert_eq!(config.command, vec!["/bin/sh", "-c"]);
    assert_eq!(config.args, vec!["echo hello"]);
}

#[test]
fn test_build_container_config_command_and_args_absent() {
    let spec = test_container_spec();
    let config = build_container_config(
        &spec,
        &test_pod_data(),
        "nginx",
        "10.43.128.1",
        &[],
        &test_empty_env(),
    );
    assert!(config.command.is_empty());
    assert!(config.args.is_empty());
}

#[test]
fn test_build_container_config_env_vars_preserved_and_k8s_appended() {
    let spec = serde_json::json!({
        "image": "app",
        "env": [
            {"name": "FOO", "value": "bar"},
            {"name": "DEBUG", "value": "true"}
        ]
    });
    let config = build_container_config(
        &spec,
        &test_pod_data(),
        "app",
        "10.43.128.1",
        &[],
        &test_empty_env(),
    );
    // User env vars come first
    assert_eq!(config.envs[0].key, "FOO");
    assert_eq!(config.envs[0].value, "bar");
    assert_eq!(config.envs[1].key, "DEBUG");
    assert_eq!(config.envs[1].value, "true");
    // KUBERNETES_SERVICE_* appended after user env vars
    assert_eq!(config.envs[2].key, "KUBERNETES_SERVICE_HOST");
    assert_eq!(config.envs[2].value, "10.43.128.1");
    assert_eq!(config.envs[3].key, "KUBERNETES_SERVICE_PORT");
    assert_eq!(config.envs[3].value, "443");
    assert_eq!(config.envs[4].key, "KUBERNETES_SERVICE_PORT_HTTPS");
    assert_eq!(config.envs[4].value, "443");
    assert_eq!(config.envs.len(), 5);
}

#[test]
fn test_build_container_config_env_vars_no_user_env() {
    let spec = test_container_spec_with_image("app");
    let config = build_container_config(
        &spec,
        &test_pod_data(),
        "app",
        "10.43.128.1",
        &[],
        &test_empty_env(),
    );
    // Only KUBERNETES_SERVICE_* vars
    assert_eq!(config.envs.len(), 3);
    assert_eq!(config.envs[0].key, "KUBERNETES_SERVICE_HOST");
}

#[test]
fn test_env_var_expansion_composing() {
    // K8s $(VAR_NAME) expansion: later env vars can reference earlier ones
    let spec = serde_json::json!({
        "image": "app",
        "env": [
            {"name": "BASE_URL", "value": "http://example.com"},
            {"name": "API_URL", "value": "$(BASE_URL)/api/v1"},
            {"name": "GREETING", "value": "hello"},
            {"name": "MESSAGE", "value": "$(GREETING) world"},
            // Undefined reference stays literal per K8s spec
            {"name": "BROKEN", "value": "$(UNDEFINED_VAR)/path"}
        ]
    });
    let pod_data =
        serde_json::json!({"metadata": {"name": "pod1", "namespace": "default", "uid": "uid1"}});
    let config = build_container_config(
        &spec,
        &pod_data,
        "app",
        "10.43.128.1",
        &[],
        &std::collections::HashMap::new(),
    );
    let env_map: std::collections::HashMap<_, _> = config
        .envs
        .iter()
        .map(|kv| (kv.key.as_str(), kv.value.as_str()))
        .collect();
    assert_eq!(env_map["BASE_URL"], "http://example.com");
    assert_eq!(
        env_map["API_URL"], "http://example.com/api/v1",
        "$(BASE_URL) should expand"
    );
    assert_eq!(
        env_map["MESSAGE"], "hello world",
        "$(GREETING) should expand"
    );
    // Undefined var: K8s leaves $(UNDEFINED_VAR) as-is (literal)
    assert_eq!(
        env_map["BROKEN"], "$(UNDEFINED_VAR)/path",
        "undefined var stays literal"
    );
}

#[test]
fn test_build_container_config_log_path() {
    let spec = serde_json::json!({"image": "nginx"});
    let pod_data =
        serde_json::json!({"metadata": {"name": "pod1", "namespace": "default", "uid": "uid1"}});
    let config = build_container_config(
        &spec,
        &pod_data,
        "my-container",
        "10.43.128.1",
        &[],
        &std::collections::HashMap::new(),
    );
    assert_eq!(config.log_path, "my-container/0.log");
}

/// Regression for P0-E2E-20260424-12b: full chain — secret → resolve → build_container_config → env var injected
#[tokio::test]
async fn test_secret_env_var_injected_in_container_config() {
    use base64::Engine;
    let db = crate::datastore::test_support::in_memory().await;

    let ns = serde_json::json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":"secrets-test"}});
    db.create_namespace("secrets-test", ns).await.unwrap();

    let secret = serde_json::json!({
        "apiVersion": "v1", "kind": "Secret",
        "metadata": {"name": "my-secret", "namespace": "secrets-test"},
        "data": {"data-1": base64::engine::general_purpose::STANDARD.encode("value-1")}
    });
    db.create_resource("v1", "Secret", Some("secrets-test"), "my-secret", secret)
        .await
        .unwrap();

    let container_spec = serde_json::json!({
        "image": "busybox",
        "env": [{"name": "data-1", "valueFrom": {"secretKeyRef": {"name": "my-secret", "key": "data-1"}}}]
    });
    let pod =
        serde_json::json!({"metadata": {"name": "p", "namespace": "secrets-test", "uid": "u1"}});

    let resolved_env_from = resolve_env_from(&container_spec, "secrets-test", &db).await;
    let resolved_env = resolve_env_value_from(&container_spec, "secrets-test", &db).await;

    let config = build_container_config(
        &container_spec,
        &pod,
        "test",
        "10.0.0.1",
        &resolved_env_from,
        &resolved_env,
    );
    let data1_env = config.envs.iter().find(|e| e.key == "data-1");
    assert!(
        data1_env.is_some(),
        "env var 'data-1' must be injected into container config"
    );
    assert_eq!(
        data1_env.unwrap().value,
        "value-1",
        "env var 'data-1' must have value 'value-1'"
    );
}

#[tokio::test]
async fn test_build_container_config_with_resolved_secret_env() {
    let container_spec = serde_json::json!({
        "image": "worker:latest",
        "env": [
            {"name": "PLAIN", "value": "direct"},
            {"name": "FROM_SECRET", "valueFrom": {"secretKeyRef": {"name": "s", "key": "k"}}}
        ]
    });

    let mut resolved = std::collections::HashMap::new();
    resolved.insert("FROM_SECRET".to_string(), "secret-value".to_string());

    let pod_data =
        serde_json::json!({"metadata": {"name": "pod1", "namespace": "default", "uid": "uid1"}});
    let config = build_container_config(
        &container_spec,
        &pod_data,
        "worker",
        "10.43.128.1",
        &[],
        &resolved,
    );

    // PLAIN should be from direct value
    assert_eq!(config.envs[0].key, "PLAIN");
    assert_eq!(config.envs[0].value, "direct");
    // FROM_SECRET should be resolved from the map
    assert_eq!(config.envs[1].key, "FROM_SECRET");
    assert_eq!(config.envs[1].value, "secret-value");
    // K8s service vars appended
    assert_eq!(config.envs[2].key, "KUBERNETES_SERVICE_HOST");
}

#[test]
fn test_build_container_config_unresolved_valuefrom_env_skipped() {
    // If an env var has valueFrom but wasn't resolved (not in map), it should be skipped
    let spec = serde_json::json!({
        "image": "app",
        "env": [
            {"name": "RESOLVED", "valueFrom": {"secretKeyRef": {"name": "s", "key": "k"}}},
            {"name": "UNRESOLVED", "valueFrom": {"secretKeyRef": {"name": "missing", "key": "k"}}}
        ]
    });

    let mut resolved = std::collections::HashMap::new();
    resolved.insert("RESOLVED".to_string(), "got-it".to_string());
    // UNRESOLVED is NOT in the map

    let pod_data =
        serde_json::json!({"metadata": {"name": "pod1", "namespace": "default", "uid": "uid1"}});
    let config = build_container_config(&spec, &pod_data, "app", "10.43.128.1", &[], &resolved);

    // Only RESOLVED + K8s service vars should be present
    assert_eq!(config.envs[0].key, "RESOLVED");
    assert_eq!(config.envs[0].value, "got-it");
    assert_eq!(config.envs[1].key, "KUBERNETES_SERVICE_HOST");
    // UNRESOLVED should not appear
    assert!(
        !config.envs.iter().any(|e| e.key == "UNRESOLVED"),
        "Unresolved valueFrom env should be skipped"
    );
}

#[test]
fn test_build_container_config_fieldref_metadata_name() {
    let pod_data = serde_json::json!({
        "metadata": {
            "name": "test-pod",
            "namespace": "default",
            "uid": "abc-123"
        }
    });
    let container_spec = serde_json::json!({
        "name": "app",
        "image": "nginx",
        "env": [
            {
                "name": "POD_NAME",
                "valueFrom": {
                    "fieldRef": {
                        "fieldPath": "metadata.name"
                    }
                }
            }
        ]
    });
    let config = build_container_config(
        &container_spec,
        &pod_data,
        "app",
        "10.43.128.1",
        &[],
        &std::collections::HashMap::new(),
    );

    // Find POD_NAME in envs
    let pod_name_env = config
        .envs
        .iter()
        .find(|e| e.key == "POD_NAME")
        .expect("POD_NAME env should exist");
    assert_eq!(pod_name_env.value, "test-pod");
}

#[test]
fn test_build_container_config_fieldref_metadata_uid() {
    let pod_data = serde_json::json!({
        "metadata": {
            "name": "test-pod",
            "namespace": "default",
            "uid": "abc-123-uid-456"
        }
    });
    let container_spec = serde_json::json!({
        "name": "app",
        "image": "nginx",
        "env": [
            {
                "name": "POD_UID",
                "valueFrom": {
                    "fieldRef": {
                        "fieldPath": "metadata.uid"
                    }
                }
            }
        ]
    });
    let config = build_container_config(
        &container_spec,
        &pod_data,
        "app",
        "10.43.128.1",
        &[],
        &std::collections::HashMap::new(),
    );

    let pod_uid_env = config
        .envs
        .iter()
        .find(|e| e.key == "POD_UID")
        .expect("POD_UID env should exist");
    assert_eq!(pod_uid_env.value, "abc-123-uid-456");
}

#[test]
fn test_build_container_config_fieldref_metadata_namespace() {
    let pod_data = serde_json::json!({
        "metadata": {
            "name": "test-pod",
            "namespace": "kube-system",
            "uid": "abc-123"
        }
    });
    let container_spec = serde_json::json!({
        "name": "app",
        "image": "nginx",
        "env": [
            {
                "name": "POD_NAMESPACE",
                "valueFrom": {
                    "fieldRef": {
                        "fieldPath": "metadata.namespace"
                    }
                }
            }
        ]
    });
    let config = build_container_config(
        &container_spec,
        &pod_data,
        "app",
        "10.43.128.1",
        &[],
        &std::collections::HashMap::new(),
    );

    let pod_ns_env = config
        .envs
        .iter()
        .find(|e| e.key == "POD_NAMESPACE")
        .expect("POD_NAMESPACE env should exist");
    assert_eq!(pod_ns_env.value, "kube-system");
}
