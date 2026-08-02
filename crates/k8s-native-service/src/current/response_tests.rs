use crate::current::validation::inject_resource_version;
use serde_json::json;

#[test]
fn test_inject_rv_sets_metadata_resource_version() {
    let input = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "test",
            "namespace": "default"
        }
    });

    let result = inject_resource_version(input, 42);

    assert_eq!(result["metadata"]["resourceVersion"], "42");
}

#[test]
fn test_inject_rv_creates_metadata_if_missing() {
    let input = json!({
        "apiVersion": "v1",
        "kind": "Pod"
    });

    let result = inject_resource_version(input, 100);

    assert!(result["metadata"].is_null());
}

#[test]
fn test_inject_rv_preserves_existing_fields() {
    let input = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "config",
            "namespace": "kube-system",
            "labels": {
                "app": "test"
            },
            "annotations": {
                "key": "value"
            }
        },
        "data": {
            "config.yaml": "content"
        }
    });

    let result = inject_resource_version(input.clone(), 123);

    assert_eq!(result["metadata"]["resourceVersion"], "123");
    assert_eq!(result["metadata"]["name"], "config");
    assert_eq!(result["metadata"]["namespace"], "kube-system");
    assert_eq!(result["metadata"]["labels"]["app"], "test");
    assert_eq!(result["metadata"]["annotations"]["key"], "value");
    assert!(!result["metadata"]["uid"].as_str().unwrap().is_empty());
    assert!(
        result["metadata"].get("creationTimestamp").is_none(),
        "response projection must not author storage identity"
    );
    assert_eq!(result["data"]["config.yaml"], "content");
}

#[test]
fn test_inject_rv_adds_uid_if_missing() {
    let data = json!({"metadata": {"name": "test"}});
    let result = inject_resource_version(data, 1);

    let uid = result["metadata"]["uid"].as_str().unwrap();
    assert!(!uid.is_empty());
    assert_eq!(uid.len(), 36, "uid should be UUID format");
}

#[test]
fn test_inject_rv_preserves_existing_uid() {
    let data = json!({
        "metadata": {"name": "test", "uid": "existing-uid-12345"}
    });
    let result = inject_resource_version(data, 1);
    assert_eq!(result["metadata"]["uid"], "existing-uid-12345");
}

#[test]
fn test_inject_rv_replaces_empty_uid() {
    let data = json!({
        "metadata": {"name": "test", "uid": ""}
    });
    let result = inject_resource_version(data, 1);

    let uid = result["metadata"]["uid"].as_str().unwrap();
    assert!(!uid.is_empty(), "uid must not remain empty");
    assert_eq!(uid.len(), 36, "uid should be UUID format");
}

#[test]
fn test_inject_rv_does_not_author_creation_timestamp() {
    let data = json!({"metadata": {"name": "test"}});
    let result = inject_resource_version(data, 1);

    assert!(result["metadata"].get("creationTimestamp").is_none());
}

#[test]
fn test_inject_rv_preserves_existing_creation_timestamp() {
    let data = json!({
        "metadata": {"name": "test", "creationTimestamp": "2026-01-01T00:00:00Z"}
    });
    let result = inject_resource_version(data, 1);
    assert_eq!(
        result["metadata"]["creationTimestamp"],
        "2026-01-01T00:00:00Z"
    );
}
