use crate::api::inject_resource_version;
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
        !result["metadata"]["creationTimestamp"]
            .as_str()
            .unwrap()
            .is_empty()
    );
    assert_eq!(result["data"]["config.yaml"], "content");
}
