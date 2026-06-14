use serde_json::json;

// Relocated from src/api_tests.rs (Slice 2)

#[test]
fn test_apply_patch_json_patch_rfc6902_add() {
    use crate::api::apply_patch;

    let current = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "test"},
        "data": {"key1": "value1"}
    });

    let patch = json!([
        {"op": "add", "path": "/data/key2", "value": "value2"}
    ]);

    let result = apply_patch(&current, &patch, Some("application/json-patch+json")).unwrap();

    assert_eq!(result["data"]["key1"], "value1");
    assert_eq!(result["data"]["key2"], "value2");
}

#[test]
fn test_apply_patch_json_patch_rfc6902_remove() {
    use crate::api::apply_patch;

    let current = json!({
        "data": {"key1": "value1", "key2": "value2"}
    });

    let patch = json!([
        {"op": "remove", "path": "/data/key1"}
    ]);

    let result = apply_patch(&current, &patch, Some("application/json-patch+json")).unwrap();

    assert!(
        result["data"].get("key1").is_none(),
        "key1 should be removed"
    );
    assert_eq!(result["data"]["key2"], "value2");
}

#[test]
fn test_apply_patch_json_patch_rfc6902_replace() {
    use crate::api::apply_patch;

    let current = json!({
        "data": {"key1": "value1"}
    });

    let patch = json!([
        {"op": "replace", "path": "/data/key1", "value": "updated"}
    ]);

    let result = apply_patch(&current, &patch, Some("application/json-patch+json")).unwrap();

    assert_eq!(result["data"]["key1"], "updated");
}

#[test]
fn test_apply_patch_merge_patch_rfc7386_null_removes_key() {
    use crate::api::apply_patch;

    let current = json!({
        "metadata": {"name": "test", "labels": {"app": "nginx"}},
        "data": {"key1": "value1", "key2": "value2"}
    });

    let patch = json!({
        "data": {"key1": null, "key3": "value3"}
    });

    let result = apply_patch(&current, &patch, Some("application/merge-patch+json")).unwrap();

    assert!(
        result["data"].get("key1").is_none(),
        "key1 should be removed by null"
    );
    assert_eq!(result["data"]["key2"], "value2");
    assert_eq!(result["data"]["key3"], "value3");
    assert_eq!(result["metadata"]["name"], "test");
    assert_eq!(result["metadata"]["labels"]["app"], "nginx");
}

#[test]
fn test_apply_patch_merge_patch_rfc7386_nested_merge() {
    use crate::api::apply_patch;

    let current = json!({
        "metadata": {
            "name": "test",
            "labels": {"app": "nginx", "version": "1.0"}
        }
    });

    let patch = json!({
        "metadata": {
            "labels": {"version": "2.0", "env": "prod"}
        }
    });

    let result = apply_patch(&current, &patch, Some("application/merge-patch+json")).unwrap();

    assert_eq!(result["metadata"]["name"], "test");
    assert_eq!(result["metadata"]["labels"]["app"], "nginx");
    assert_eq!(result["metadata"]["labels"]["version"], "2.0");
    assert_eq!(result["metadata"]["labels"]["env"], "prod");
}

#[test]
fn test_apply_patch_merge_patch_default_content_type() {
    use crate::api::apply_patch;

    let current = json!({"data": {"key1": "value1"}});
    let patch = json!({"data": {"key2": "value2"}});

    let result = apply_patch(&current, &patch, None).unwrap();
    assert_eq!(result["data"]["key1"], "value1");
    assert_eq!(result["data"]["key2"], "value2");

    let result = apply_patch(&current, &patch, Some("application/json")).unwrap();
    assert_eq!(result["data"]["key1"], "value1");
    assert_eq!(result["data"]["key2"], "value2");
}

#[test]
fn test_apply_patch_strategic_merge_patch_fallback() {
    use crate::api::apply_patch;

    let current = json!({
        "data": {"key1": "value1", "key2": "value2"}
    });

    let patch = json!({
        "data": {"key1": null, "key3": "value3"}
    });

    let result = apply_patch(
        &current,
        &patch,
        Some("application/strategic-merge-patch+json"),
    )
    .unwrap();

    assert!(
        result["data"].get("key1").is_none(),
        "key1 should be removed"
    );
    assert_eq!(result["data"]["key2"], "value2");
    assert_eq!(result["data"]["key3"], "value3");
}

#[test]
fn test_apply_patch_unsupported_content_type() {
    use crate::api::apply_patch;

    let current = json!({"data": {"key1": "value1"}});
    let patch = json!({"data": {"key2": "value2"}});

    let result = apply_patch(&current, &patch, Some("application/unsupported"));

    assert!(
        result.is_err(),
        "Should return error for unsupported content type"
    );
    let err_msg = format!("{:?}", result.unwrap_err());
    assert!(
        err_msg.contains("Unsupported"),
        "Error message should mention unsupported content type"
    );
}

#[test]
fn test_apply_patch_json_patch_invalid_format() {
    use crate::api::apply_patch;

    let current = json!({"data": {"key1": "value1"}});
    let patch = json!([
        {"path": "/data/key2", "value": "value2"}
    ]);

    let result = apply_patch(&current, &patch, Some("application/json-patch+json"));

    assert!(
        result.is_err(),
        "Should return error for invalid JSON Patch format"
    );
}

#[test]
fn test_apply_patch_server_side_apply_preserves_data() {
    use crate::api::apply_patch;

    let current = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "test", "namespace": "default"},
        "data": {"key1": "value1"}
    });

    let patch = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "test", "namespace": "default"},
        "data": {"key1": "updated", "key2": "new"}
    });

    let result = apply_patch(&current, &patch, Some("application/apply-patch+yaml")).unwrap();

    assert_eq!(result["data"]["key1"], "updated");
    assert_eq!(result["data"]["key2"], "new");
    assert_eq!(result["metadata"]["name"], "test");
    assert_eq!(result["metadata"]["namespace"], "default");
}

#[test]
fn test_apply_patch_ssa_yaml_body_fails_json_parse() {
    let yaml_body =
        b"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: test\ndata:\n  key1: updated\n";

    let result: Result<serde_json::Value, _> = serde_json::from_slice(yaml_body);

    assert!(
        result.is_err(),
        "Pure YAML body should fail serde_json::from_slice — this is the S5.1a bug"
    );

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("expected value") || err_msg.contains("key must be a string"),
        "Error should indicate JSON parse failure, got: {}",
        err_msg
    );
}

#[test]
fn test_apply_patch_ssa_json_body_works() {
    use crate::api::apply_patch;

    let current = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "test"},
        "data": {"key1": "value1"}
    });

    let json_body = b"{\"data\":{\"key1\":\"updated\",\"key2\":\"new\"}}";
    let patch: serde_json::Value = serde_json::from_slice(json_body).unwrap();

    let result = apply_patch(&current, &patch, Some("application/apply-patch+yaml")).unwrap();
    assert_eq!(result["data"]["key1"], "updated");
    assert_eq!(result["data"]["key2"], "new");
}

#[test]
fn test_apply_patch_strategic_merge_patch_null_removes_field() {
    use crate::api::apply_patch;

    let current = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "test", "annotations": {"old": "value"}},
        "data": {"key1": "value1", "key2": "value2"}
    });

    let patch = json!({
        "data": {"key2": null}
    });

    let result = apply_patch(
        &current,
        &patch,
        Some("application/strategic-merge-patch+json"),
    )
    .unwrap();
    assert_eq!(result["data"]["key1"], "value1");
    assert!(
        result["data"]["key2"].is_null() || result["data"].get("key2").is_none(),
        "null in merge patch should remove the field"
    );
    assert_eq!(result["metadata"]["annotations"]["old"], "value");
}

#[test]
fn test_apply_patch_empty_merge_patch_preserves_all() {
    use crate::api::apply_patch;

    let current = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "test"},
        "data": {"key1": "value1"}
    });

    let patch = json!({});

    let result = apply_patch(&current, &patch, Some("application/merge-patch+json")).unwrap();
    assert_eq!(
        result, current,
        "Empty merge patch should leave document unchanged"
    );
}

#[test]
fn test_apply_patch_json_patch_add_nested_path() {
    use crate::api::apply_patch;

    let current = json!({
        "metadata": {"name": "test", "labels": {}},
        "data": {}
    });

    let patch = json!([
        {"op": "add", "path": "/metadata/labels/app", "value": "nginx"},
        {"op": "add", "path": "/data/config", "value": "hello"}
    ]);

    let result = apply_patch(&current, &patch, Some("application/json-patch+json")).unwrap();
    assert_eq!(result["metadata"]["labels"]["app"], "nginx");
    assert_eq!(result["data"]["config"], "hello");
}

#[test]
fn test_apply_patch_json_patch_test_op_fails() {
    use crate::api::apply_patch;

    let current = json!({
        "metadata": {"name": "test"},
        "data": {"key": "wrong"}
    });

    let patch = json!([
        {"op": "test", "path": "/data/key", "value": "expected"},
        {"op": "replace", "path": "/data/key", "value": "new"}
    ]);

    let result = apply_patch(&current, &patch, Some("application/json-patch+json"));
    assert!(
        result.is_err(),
        "JSON Patch test op should fail when value doesn't match"
    );
}

// F3-02: apply_patch must NOT mutate the patch input. Callers reuse `patch`
// after the call for SSA last-applied annotation, admission diffs, and
// audit. A regression that switched to mutate-in-place on `patch` would
// silently corrupt those downstream consumers.
#[test]
fn strategic_merge_does_not_mutate_patch_input() {
    use crate::api::apply_patch;
    use serde_json::json;
    let current = json!({
        "spec": {
            "containers": [
                {"name": "c1", "image": "old"}
            ]
        }
    });
    let patch = json!({
        "spec": {
            "containers": [
                {"name": "c1", "image": "new"},
                {"name": "c2", "image": "fresh"}
            ]
        }
    });
    let patch_before = patch.clone();
    let _ = apply_patch(
        &current,
        &patch,
        Some("application/strategic-merge-patch+json"),
    );
    assert_eq!(
        patch, patch_before,
        "apply_patch must not mutate the patch input"
    );
}

// F3-02: strategic merge across containers must preserve the existing array
// order — patches that add a new container append; patches that touch an
// existing container update in place. Borrowed-key index lookup must keep
// this invariant.
#[test]
fn strategic_merge_container_merge_preserves_order() {
    use crate::api::apply_patch;
    use serde_json::json;
    let current = json!({
        "spec": {
            "containers": [
                {"name": "c1", "image": "i1"},
                {"name": "c2", "image": "i2"},
                {"name": "c3", "image": "i3"}
            ]
        }
    });
    let patch = json!({
        "spec": {
            "containers": [
                {"name": "c2", "image": "i2-new"},
                {"name": "c4", "image": "i4"}
            ]
        }
    });
    let merged = apply_patch(
        &current,
        &patch,
        Some("application/strategic-merge-patch+json"),
    )
    .expect("strategic merge must succeed");
    let containers = merged["spec"]["containers"].as_array().unwrap();
    assert_eq!(containers.len(), 4, "c4 must append");
    assert_eq!(containers[0]["name"], "c1");
    assert_eq!(containers[1]["name"], "c2");
    assert_eq!(containers[1]["image"], "i2-new", "in-place update");
    assert_eq!(containers[2]["name"], "c3");
    assert_eq!(containers[3]["name"], "c4");
}

// --- T1.3: strategic merge — nested container arrays + directives ---

const SMP: Option<&str> = Some("application/strategic-merge-patch+json");

#[test]
fn test_strategic_merge_nested_container_ports_merge_by_container_port() {
    use crate::api::apply_patch;
    let current = json!({
        "spec": {"containers": [
            {"name": "app", "ports": [
                {"containerPort": 80, "name": "http"},
                {"containerPort": 443, "name": "https"}
            ]}
        ]}
    });
    // Patch only the 443 port (change its name) and add 8080. 80 must survive.
    let patch = json!({
        "spec": {"containers": [
            {"name": "app", "ports": [
                {"containerPort": 443, "name": "tls"},
                {"containerPort": 8080, "name": "alt"}
            ]}
        ]}
    });
    let result = apply_patch(&current, &patch, SMP).unwrap();
    let ports = result["spec"]["containers"][0]["ports"].as_array().unwrap();
    assert_eq!(
        ports.len(),
        3,
        "ports merged by containerPort, not replaced: {ports:?}"
    );
    let by_port = |p: i64| ports.iter().find(|x| x["containerPort"] == p).unwrap();
    assert_eq!(by_port(80)["name"], "http");
    assert_eq!(by_port(443)["name"], "tls");
    assert_eq!(by_port(8080)["name"], "alt");
}

#[test]
fn test_strategic_merge_nested_container_env_merge_by_name() {
    use crate::api::apply_patch;
    let current = json!({
        "spec": {"containers": [
            {"name": "app", "env": [{"name": "A", "value": "1"}, {"name": "B", "value": "2"}]}
        ]}
    });
    let patch = json!({
        "spec": {"containers": [
            {"name": "app", "env": [{"name": "B", "value": "9"}, {"name": "C", "value": "3"}]}
        ]}
    });
    let result = apply_patch(&current, &patch, SMP).unwrap();
    let env = result["spec"]["containers"][0]["env"].as_array().unwrap();
    assert_eq!(env.len(), 3);
    let val = |n: &str| env.iter().find(|x| x["name"] == n).unwrap()["value"].clone();
    assert_eq!(val("A"), "1");
    assert_eq!(val("B"), "9");
    assert_eq!(val("C"), "3");
}

#[test]
fn test_strategic_merge_service_ports_merge_by_port() {
    use crate::api::apply_patch;
    let current = json!({"spec": {"ports": [
        {"port": 80, "name": "http"}, {"port": 443, "name": "https"}
    ]}});
    let patch = json!({"spec": {"ports": [{"port": 443, "name": "tls"}]}});
    let result = apply_patch(&current, &patch, SMP).unwrap();
    let ports = result["spec"]["ports"].as_array().unwrap();
    assert_eq!(ports.len(), 2, "service ports merge by port number");
    let by_port = |p: i64| ports.iter().find(|x| x["port"] == p).unwrap();
    assert_eq!(by_port(80)["name"], "http");
    assert_eq!(by_port(443)["name"], "tls");
}

#[test]
fn test_strategic_merge_patch_delete_directive_removes_list_element() {
    use crate::api::apply_patch;
    let current = json!({"spec": {"containers": [
        {"name": "app", "image": "a"}, {"name": "sidecar", "image": "b"}
    ]}});
    let patch = json!({"spec": {"containers": [{"name": "sidecar", "$patch": "delete"}]}});
    let result = apply_patch(&current, &patch, SMP).unwrap();
    let containers = result["spec"]["containers"].as_array().unwrap();
    assert_eq!(containers.len(), 1);
    assert_eq!(containers[0]["name"], "app");
}

#[test]
fn test_strategic_merge_patch_replace_directive_replaces_object() {
    use crate::api::apply_patch;
    let current = json!({"spec": {"selector": {"a": "1", "b": "2"}}});
    // $patch: replace on the selector map discards b.
    let patch = json!({"spec": {"selector": {"$patch": "replace", "a": "9"}}});
    let result = apply_patch(&current, &patch, SMP).unwrap();
    assert_eq!(result["spec"]["selector"], json!({"a": "9"}));
}

#[test]
fn test_strategic_merge_delete_from_primitive_list() {
    use crate::api::apply_patch;
    let current = json!({"metadata": {"finalizers": ["a", "b", "c"]}});
    let patch = json!({"metadata": {"$deleteFromPrimitiveList/finalizers": ["b"]}});
    let result = apply_patch(&current, &patch, SMP).unwrap();
    assert_eq!(result["metadata"]["finalizers"], json!(["a", "c"]));
}

#[test]
fn test_strategic_merge_set_element_order_reorders_keyed_list() {
    use crate::api::apply_patch;
    let current = json!({"spec": {"containers": [
        {"name": "a"}, {"name": "b"}, {"name": "c"}
    ]}});
    let patch = json!({"spec": {
        "$setElementOrder/containers": [{"name": "c"}, {"name": "a"}, {"name": "b"}]
    }});
    let result = apply_patch(&current, &patch, SMP).unwrap();
    let names: Vec<&str> = result["spec"]["containers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["c", "a", "b"]);
}
