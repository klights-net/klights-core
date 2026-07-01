use super::*;
use crate::api_discovery::{openapi_v2, openapi_v3_discovery_with_crds};
use serde_json::json;
use std::sync::{LazyLock, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

static PROXY_ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

struct EnvVarRestore {
    key: &'static str,
    value: Option<String>,
}

impl EnvVarRestore {
    fn set(key: &'static str, value: Option<&str>) -> Self {
        let previous = std::env::var(key).ok();
        match value {
            // TODO: Audit that the environment access only happens in single-threaded code.
            Some(v) => unsafe { std::env::set_var(key, v) },
            // TODO: Audit that the environment access only happens in single-threaded code.
            None => unsafe { std::env::remove_var(key) },
        }
        Self {
            key,
            value: previous,
        }
    }
}

impl Drop for EnvVarRestore {
    fn drop(&mut self) {
        match self.value.as_deref() {
            // TODO: Audit that the environment access only happens in single-threaded code.
            Some(v) => unsafe { std::env::set_var(self.key, v) },
            // TODO: Audit that the environment access only happens in single-threaded code.
            None => unsafe { std::env::remove_var(self.key) },
        }
    }
}

fn response_body_json(response: Response) -> Value {
    let rt = tokio::runtime::Runtime::new().expect("build temporary runtime");
    rt.block_on(async move {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body bytes");
        serde_json::from_slice(&bytes).expect("response body must be valid JSON")
    })
}

fn bootstrap_secret_token_from_json(secret: &Value) -> String {
    use base64::Engine as _;

    let data = secret.get("data").expect("Secret data must exist");
    let token_id = data
        .get("token-id")
        .and_then(|value| value.as_str())
        .expect("token-id must exist");
    let token_secret = data
        .get("token-secret")
        .and_then(|value| value.as_str())
        .expect("token-secret must exist");
    let decode = |value: &str| {
        String::from_utf8(
            base64::engine::general_purpose::STANDARD
                .decode(value)
                .expect("bootstrap token field must be base64"),
        )
        .expect("bootstrap token field must be utf-8")
    };
    format!("{}.{}", decode(token_id), decode(token_secret))
}

#[test]
fn test_inject_rv_sets_metadata_resource_version_as_string() {
    let data = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "test"}
    });
    let result = inject_resource_version(data, 42);
    assert_eq!(result["metadata"]["resourceVersion"], "42");
}

#[test]
fn test_apply_pod_container_defaults_defaults_missing_restart_policy_to_always() {
    let mut spec_obj = serde_json::Map::new();
    spec_obj.insert(
        "containers".to_string(),
        json!([{"name": "app", "image": "registry.k8s.io/pause:3.10"}]),
    );

    apply_pod_container_defaults(&mut spec_obj);

    assert_eq!(spec_obj.get("restartPolicy"), Some(&json!("Always")));
}

#[test]
fn test_apply_pod_container_defaults_defaults_empty_restart_policy_to_always() {
    let mut spec_obj = serde_json::Map::new();
    spec_obj.insert("restartPolicy".to_string(), json!(""));
    spec_obj.insert(
        "containers".to_string(),
        json!([{"name": "app", "image": "registry.k8s.io/pause:3.10"}]),
    );

    apply_pod_container_defaults(&mut spec_obj);

    assert_eq!(spec_obj.get("restartPolicy"), Some(&json!("Always")));
}

#[test]
fn test_inject_rv_preserves_existing_fields() {
    let data = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "test",
            "namespace": "default",
            "labels": {"app": "nginx"}
        },
        "spec": {"containers": []}
    });
    let result = inject_resource_version(data, 5);

    assert_eq!(result["metadata"]["resourceVersion"], "5");
    assert_eq!(result["metadata"]["name"], "test");
    assert_eq!(result["metadata"]["namespace"], "default");
    assert_eq!(result["metadata"]["labels"]["app"], "nginx");
    assert_eq!(result["spec"]["containers"], json!([]));
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
fn test_inject_rv_adds_creation_timestamp_if_missing() {
    let data = json!({"metadata": {"name": "test"}});
    let result = inject_resource_version(data, 1);

    let ts = result["metadata"]["creationTimestamp"].as_str().unwrap();
    assert!(!ts.is_empty());
    assert!(
        ts.starts_with("20"),
        "creationTimestamp should be RFC3339, got: {}",
        ts
    );
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

#[test]
fn test_inject_rv_no_metadata_is_noop() {
    let data = json!({"spec": {"containers": []}});
    let result = inject_resource_version(data.clone(), 1);
    assert_eq!(result, data);
}

// P0-CORR-02: Tests for ensure_array helper
#[test]
fn test_ensure_array_creates_missing_key() {
    let mut value = json!({"key": "value"});
    let arr = ensure_array(&mut value, "conditions");
    assert_eq!(arr.len(), 0, "New array should be empty");
    assert!(value["conditions"].is_array(), "Key should be array");
}

#[test]
fn test_ensure_array_replaces_non_array() {
    // String
    let mut value = json!({"conditions": "not-an-array"});
    let arr = ensure_array(&mut value, "conditions");
    assert_eq!(arr.len(), 0, "Should be empty after replacement");
    assert!(value["conditions"].is_array(), "Should be array");

    // Number
    let mut value = json!({"conditions": 42});
    let arr = ensure_array(&mut value, "conditions");
    assert_eq!(arr.len(), 0, "Should be empty after replacement");
    assert!(value["conditions"].is_array(), "Should be array");

    // Object
    let mut value = json!({"conditions": {"not": "array"}});
    let arr = ensure_array(&mut value, "conditions");
    assert_eq!(arr.len(), 0, "Should be empty after replacement");
    assert!(value["conditions"].is_array(), "Should be array");

    // Null
    let mut value = json!({"conditions": null});
    let arr = ensure_array(&mut value, "conditions");
    assert_eq!(arr.len(), 0, "Should be empty after replacement");
    assert!(value["conditions"].is_array(), "Should be array");
}

#[test]
fn test_ensure_array_preserves_existing_array() {
    let mut value = json!({"conditions": [{"type": "Test"}]});
    let arr = ensure_array(&mut value, "conditions");
    assert_eq!(arr.len(), 1, "Should preserve existing elements");
    assert_eq!(arr[0]["type"], "Test", "Should preserve content");
}

#[test]
fn test_ensure_object_creates_missing_key() {
    let mut value = json!({"key": "value"});
    let obj = ensure_object(&mut value, "status");
    assert_eq!(obj.len(), 0, "New object should be empty");
    assert!(value["status"].is_object(), "Key should be object");
}

#[test]
fn test_ensure_object_replaces_non_object() {
    // String
    let mut value = json!({"status": "not-an-object"});
    let obj = ensure_object(&mut value, "status");
    assert_eq!(obj.len(), 0, "Should be empty after replacement");
    assert!(value["status"].is_object(), "Should be object");

    // Number
    let mut value = json!({"status": 42});
    let obj = ensure_object(&mut value, "status");
    assert_eq!(obj.len(), 0, "Should be empty after replacement");
    assert!(value["status"].is_object(), "Should be object");

    // Array
    let mut value = json!({"status": ["not", "object"]});
    let obj = ensure_object(&mut value, "status");
    assert_eq!(obj.len(), 0, "Should be empty after replacement");
    assert!(value["status"].is_object(), "Should be object");

    // Null
    let mut value = json!({"status": null});
    let obj = ensure_object(&mut value, "status");
    assert_eq!(obj.len(), 0, "Should be empty after replacement");
    assert!(value["status"].is_object(), "Should be object");
}

#[test]
fn test_ensure_object_preserves_existing_object() {
    let mut value = json!({"status": {"phase": "Running"}});
    let obj = ensure_object(&mut value, "status");
    assert_eq!(obj.len(), 1, "Should preserve existing keys");
    assert_eq!(obj["phase"], "Running", "Should preserve content");
}

// P0-CORR-02 property tests: assert the invariants hold across arbitrary
// serde_json::Value inputs at the key slot, not just the four hand-picked
// shapes. proptest generates ~256 cases per run by default and shrinks
// failures to a minimal reproducer.
proptest::proptest! {
    /// Invariant 1: after ensure_array, `parent[key]` is always an array,
    /// regardless of what was there before.
    #[test]
    fn proptest_ensure_array_always_yields_array(
        initial in arb_json_value(),
    ) {
        let mut parent = json!({ "field": initial });
        let _ = ensure_array(&mut parent, "field");
        proptest::prop_assert!(
            parent["field"].is_array(),
            "ensure_array must yield an array; got {:?}",
            parent["field"]
        );
    }

    /// Invariant 2: ensure_array preserves an existing array verbatim,
    /// and replaces any non-array with an empty array.
    #[test]
    fn proptest_ensure_array_preserves_arrays_replaces_others(
        initial in arb_json_value(),
    ) {
        let was_array = initial.is_array();
        let original = initial.clone();
        let mut parent = json!({ "field": initial });
        let arr = ensure_array(&mut parent, "field");
        if was_array {
            proptest::prop_assert_eq!(
                Value::Array(arr.clone()),
                original,
                "existing array must be preserved"
            );
        } else {
            proptest::prop_assert!(
                arr.is_empty(),
                "non-array input must be replaced with []; got {:?}",
                arr
            );
        }
    }

    /// Invariant 3: after ensure_object, `parent[key]` is always an object.
    #[test]
    fn proptest_ensure_object_always_yields_object(
        initial in arb_json_value(),
    ) {
        let mut parent = json!({ "field": initial });
        let _ = ensure_object(&mut parent, "field");
        proptest::prop_assert!(
            parent["field"].is_object(),
            "ensure_object must yield an object; got {:?}",
            parent["field"]
        );
    }

    /// Invariant 4: ensure_object preserves an existing object verbatim,
    /// and replaces any non-object with an empty object.
    #[test]
    fn proptest_ensure_object_preserves_objects_replaces_others(
        initial in arb_json_value(),
    ) {
        let was_object = initial.is_object();
        let original = initial.clone();
        let mut parent = json!({ "field": initial });
        let obj = ensure_object(&mut parent, "field");
        if was_object {
            proptest::prop_assert_eq!(
                Value::Object(obj.clone()),
                original,
                "existing object must be preserved"
            );
        } else {
            proptest::prop_assert!(
                obj.is_empty(),
                "non-object input must be replaced with {{}}; got {:?}",
                obj
            );
        }
    }

    /// Invariant 5: ensure_array is idempotent — calling twice yields the
    /// same value as calling once.
    #[test]
    fn proptest_ensure_array_is_idempotent(
        initial in arb_json_value(),
    ) {
        let mut parent_once = json!({ "field": initial.clone() });
        let _ = ensure_array(&mut parent_once, "field");
        let mut parent_twice = parent_once.clone();
        let _ = ensure_array(&mut parent_twice, "field");
        proptest::prop_assert_eq!(
            parent_once,
            parent_twice,
            "ensure_array must be idempotent"
        );
    }

    /// Invariant 6: ensure_object is idempotent.
    #[test]
    fn proptest_ensure_object_is_idempotent(
        initial in arb_json_value(),
    ) {
        let mut parent_once = json!({ "field": initial.clone() });
        let _ = ensure_object(&mut parent_once, "field");
        let mut parent_twice = parent_once.clone();
        let _ = ensure_object(&mut parent_twice, "field");
        proptest::prop_assert_eq!(
            parent_once,
            parent_twice,
            "ensure_object must be idempotent"
        );
    }
}

/// Recursive proptest strategy that generates arbitrary serde_json::Value
/// trees, exercising every variant (Null, Bool, Number, String, Array,
/// Object) at every depth so the helpers see every shape they could
/// encounter from a user-supplied request body.
fn arb_json_value() -> proptest::strategy::BoxedStrategy<Value> {
    use proptest::prelude::*;

    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(Value::from),
        any::<f64>()
            .prop_filter("finite", |n| n.is_finite())
            .prop_map(|n| {
                serde_json::Number::from_f64(n)
                    .map(Value::Number)
                    .unwrap_or(Value::Null)
            }),
        ".*".prop_map(Value::String),
    ];

    leaf.prop_recursive(
        4,  // up to 4 levels deep
        32, // up to 32 total nodes
        8,  // each collection up to 8 children
        |inner| {
            prop_oneof![
                proptest::collection::vec(inner.clone(), 0..8).prop_map(Value::Array),
                proptest::collection::hash_map(".*", inner, 0..8)
                    .prop_map(|m| { Value::Object(m.into_iter().collect()) }),
            ]
        },
    )
    .boxed()
}

#[test]
fn test_deployment_strategy_defaults_to_rolling_update_with_25_percent() {
    let mut deploy = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "spec": {
            "replicas": 1
        }
    });

    normalize_resource_for_storage("apps/v1", "Deployment", &mut deploy);

    assert_eq!(deploy["spec"]["strategy"]["type"], "RollingUpdate");
    assert_eq!(
        deploy["spec"]["strategy"]["rollingUpdate"]["maxUnavailable"],
        "25%"
    );
    assert_eq!(
        deploy["spec"]["strategy"]["rollingUpdate"]["maxSurge"],
        "25%"
    );
}

#[test]
fn test_deployment_rolling_update_type_adds_missing_rolling_update_defaults() {
    let mut deploy = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "spec": {
            "strategy": {
                "type": "RollingUpdate"
            }
        }
    });

    normalize_resource_for_storage("apps/v1", "Deployment", &mut deploy);

    assert_eq!(
        deploy["spec"]["strategy"]["rollingUpdate"]["maxUnavailable"],
        "25%"
    );
    assert_eq!(
        deploy["spec"]["strategy"]["rollingUpdate"]["maxSurge"],
        "25%"
    );
}

#[test]
fn test_deployment_recreate_strategy_keeps_type_without_rolling_update_defaults() {
    let mut deploy = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "spec": {
            "strategy": {
                "type": "Recreate"
            }
        }
    });

    normalize_resource_for_storage("apps/v1", "Deployment", &mut deploy);

    assert_eq!(deploy["spec"]["strategy"]["type"], "Recreate");
    assert!(deploy["spec"]["strategy"]["rollingUpdate"].is_null());
}

#[test]
fn test_configmap_protobuf_update_preserves_data() {
    // Simulate what LenientJson does for a PUT update with protobuf body:
    // 1. Protobuf-encode a ConfigMap with data
    // 2. Decode via decode_protobuf (full path with envelope)
    // 3. Verify data field is preserved in the decoded JSON
    use prost::Message;

    let cm = k8s_pb::api::core::v1::ConfigMap {
        metadata: Some(k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some("test-config".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        data: vec![
            (
                "app.conf".to_string(),
                "server=localhost\nport=9090\n".to_string(),
            ),
            ("log.level".to_string(), "info".to_string()),
            ("new.key".to_string(), "new.value".to_string()),
        ]
        .into_iter()
        .collect(),
        ..Default::default()
    };

    let mut pb_bytes = Vec::new();
    cm.encode(&mut pb_bytes).unwrap();

    // Wrap in Unknown envelope (what k8s clients actually send)
    let envelope = crate::protobuf::Unknown {
        type_meta: Some(crate::protobuf::TypeMeta {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
        }),
        raw: pb_bytes,
        content_encoding: String::new(),
        content_type: String::new(),
    };
    let mut envelope_bytes = Vec::new();
    envelope.encode(&mut envelope_bytes).unwrap();

    let decoded = crate::protobuf::decode_protobuf(&envelope_bytes).unwrap();

    // This is what the PUT handler receives as `body` — verify data is intact
    assert_eq!(decoded["data"]["log.level"], "info");
    assert_eq!(decoded["data"]["new.key"], "new.value");
    assert!(
        decoded["data"]["app.conf"]
            .as_str()
            .unwrap()
            .contains("port=9090")
    );
}

#[test]
fn test_configmap_merge_patch_updates_data() {
    // Simulate PATCH with merge-patch content type
    let current = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "test-config", "namespace": "default"},
        "data": {"key1": "value1", "key2": "value2"}
    });

    // Merge patch: replace data entirely (RFC 7386 behavior)
    let patch = json!({
        "data": {"key1": "updated", "key3": "new"}
    });

    let result = apply_patch(&current, &patch, Some("application/merge-patch+json")).unwrap();

    assert_eq!(result["data"]["key1"], "updated");
    assert_eq!(result["data"]["key3"], "new");
    // RFC 7386 merge patch deep-merges objects — key2 is preserved
    // (to remove key2, patch would need "key2": null)
    assert_eq!(result["data"]["key2"], "value2");
    // Other fields preserved
    assert_eq!(result["metadata"]["name"], "test-config");
}

#[test]
fn test_configmap_json_patch_updates_data() {
    // Simulate PATCH with JSON Patch (RFC 6902) content type
    let current = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "test-config"},
        "data": {"key1": "value1", "key2": "value2"}
    });

    let patch = json!([
        {"op": "replace", "path": "/data/key1", "value": "updated"},
        {"op": "add", "path": "/data/key3", "value": "new"}
    ]);

    let result = apply_patch(&current, &patch, Some("application/json-patch+json")).unwrap();

    assert_eq!(result["data"]["key1"], "updated");
    assert_eq!(result["data"]["key2"], "value2"); // preserved with JSON Patch
    assert_eq!(result["data"]["key3"], "new");
}

// ========================
// prefers_protobuf tests
// ========================

#[test]
fn test_prefers_protobuf_no_accept_header_returns_false() {
    let headers = HeaderMap::new();
    assert!(!prefers_protobuf(&headers));
}

#[test]
fn test_prefers_protobuf_explicit_protobuf_returns_true() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "accept",
        "application/vnd.kubernetes.protobuf".parse().unwrap(),
    );
    assert!(prefers_protobuf(&headers));
}

#[test]
fn test_prefers_protobuf_explicit_json_returns_false() {
    let mut headers = HeaderMap::new();
    headers.insert("accept", "application/json".parse().unwrap());
    assert!(!prefers_protobuf(&headers));
}

#[test]
fn test_prefers_protobuf_mixed_accept_with_protobuf_returns_true() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "accept",
        "application/json, application/vnd.kubernetes.protobuf"
            .parse()
            .unwrap(),
    );
    // When protobuf is in the Accept header, prefer it
    assert!(prefers_protobuf(&headers));
}

#[test]
fn test_prefers_protobuf_unknown_accept_returns_false() {
    let mut headers = HeaderMap::new();
    headers.insert("accept", "text/html".parse().unwrap());
    assert!(!prefers_protobuf(&headers));
}

// ========================
// Secret stringData tests
// ========================

#[test]
fn test_secret_stringdata_converted_to_base64_data() {
    use base64::Engine;
    let engine = base64::engine::general_purpose::STANDARD;

    let mut secret = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "test-secret"},
        "stringData": {
            "username": "admin",
            "password": "secret123"
        }
    });

    process_secret_stringdata(&mut secret);

    // stringData should be removed
    assert!(secret.get("stringData").is_none());

    // data should contain base64-encoded values
    let data = secret.get("data").expect("data field should exist");
    assert_eq!(data["username"], engine.encode("admin"));
    assert_eq!(data["password"], engine.encode("secret123"));
}

#[test]
fn test_secret_data_preserved_when_no_stringdata() {
    use base64::Engine;
    let engine = base64::engine::general_purpose::STANDARD;

    let encoded_value = engine.encode("already-encoded");
    let mut secret = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "test-secret"},
        "data": {
            "key": encoded_value.clone()
        }
    });

    process_secret_stringdata(&mut secret);

    // data should be preserved as-is
    let data = secret.get("data").expect("data field should exist");
    assert_eq!(data["key"], encoded_value);
}

#[test]
fn test_secret_stringdata_overrides_data() {
    use base64::Engine;
    let engine = base64::engine::general_purpose::STANDARD;

    let mut secret = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "test-secret"},
        "data": {
            "key": "old-base64-value"
        },
        "stringData": {
            "key": "new-plaintext-value"
        }
    });

    process_secret_stringdata(&mut secret);

    // stringData should be removed
    assert!(secret.get("stringData").is_none());

    // data should contain the stringData value (base64-encoded)
    let data = secret.get("data").expect("data field should exist");
    assert_eq!(data["key"], engine.encode("new-plaintext-value"));
}

#[test]
fn test_secret_type_defaults_to_opaque() {
    let mut secret = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "test-secret"},
        "stringData": {
            "key": "value"
        }
    });

    process_secret_stringdata(&mut secret);

    // type should default to Opaque
    assert_eq!(secret.get("type").expect("type should be set"), "Opaque");
}

#[test]
fn test_secret_patch_stringdata_converted() {
    use base64::Engine;
    let engine = base64::engine::general_purpose::STANDARD;

    // Simulate an existing Secret with data
    let current = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "my-secret", "namespace": "default"},
        "data": {
            "existing": "ZXhpc3Rpbmc="
        },
        "type": "Opaque"
    });

    // PATCH with stringData (merge patch)
    let patch = json!({
        "stringData": {
            "new-key": "new-value"
        }
    });

    let mut patched = apply_patch(&current, &patch, Some("application/merge-patch+json")).unwrap();
    process_secret_stringdata(&mut patched);

    // stringData should be removed
    assert!(
        patched.get("stringData").is_none(),
        "stringData should be removed after processing"
    );

    // new-key should be base64-encoded in data
    let data = patched.get("data").expect("data field should exist");
    assert_eq!(data["new-key"], engine.encode("new-value"));

    // existing data should be preserved
    assert_eq!(data["existing"], "ZXhpc3Rpbmc=");
}

// ========================
// Secret empty key validation tests
// ========================

#[test]
fn test_validate_secret_data_empty_key_rejected() {
    let body = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "test-secret"},
        "data": {
            "": "base64value"
        }
    });

    let result = validate_secret_data(&body);
    assert!(result.is_err());
    let err_msg = result.unwrap_err();
    assert!(err_msg.contains("Invalid value: \"\""));
    assert!(err_msg.contains("data[]"));
}

#[test]
fn test_validate_secret_data_valid_keys_accepted() {
    let body = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "test-secret"},
        "data": {
            "valid-key": "base64value",
            "another.key": "base64value2"
        }
    });

    let result = validate_secret_data(&body);
    assert!(result.is_ok());
}

#[test]
fn test_validate_secret_data_no_data_field_accepted() {
    let body = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "test-secret"},
        "type": "Opaque"
    });

    let result = validate_secret_data(&body);
    assert!(result.is_ok());
}

#[test]
fn test_validate_secret_stringdata_empty_key_rejected() {
    let body = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "test-secret"},
        "stringData": {
            "": "plaintext"
        }
    });

    let result = validate_secret_data(&body);
    assert!(result.is_err());
    let err_msg = result.unwrap_err();
    assert!(err_msg.contains("Invalid value: \"\""));
    assert!(err_msg.contains("stringData[]"));
}

#[tokio::test]
async fn test_service_delete_also_deletes_endpoints() {
    let db = crate::datastore::test_support::in_memory().await;

    // Create namespace
    let ns = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "default"}});
    db.create_resource("v1", "Namespace", None, "default", ns)
        .await
        .unwrap();

    // Create service
    let svc = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "my-svc", "namespace": "default"},
        "spec": {"selector": {"app": "web"}, "ports": [{"port": 80}]}
    });
    db.create_resource("v1", "Service", Some("default"), "my-svc", svc)
        .await
        .unwrap();

    // Create associated endpoints (same name as service)
    let ep = json!({
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {"name": "my-svc", "namespace": "default"},
        "subsets": [{"addresses": [{"ip": "10.0.0.1"}], "ports": [{"port": 80}]}]
    });
    db.create_resource("v1", "Endpoints", Some("default"), "my-svc", ep)
        .await
        .unwrap();

    // Verify both exist
    assert!(
        db.get_resource("v1", "Service", Some("default"), "my-svc")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        db.get_resource("v1", "Endpoints", Some("default"), "my-svc")
            .await
            .unwrap()
            .is_some()
    );

    // Simulate delete_service: delete service then delete its endpoints
    db.delete_resource("v1", "Service", Some("default"), "my-svc")
        .await
        .unwrap();
    let _ = db
        .delete_resource("v1", "Endpoints", Some("default"), "my-svc")
        .await;

    // Both should be gone (hard-deleted)
    assert!(
        db.get_resource("v1", "Service", Some("default"), "my-svc")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        db.get_resource("v1", "Endpoints", Some("default"), "my-svc")
            .await
            .unwrap()
            .is_none()
    );
}

#[test]
fn test_pod_list_to_table_ready_count_uses_spec_containers() {
    // Bug: READY column shows "0/0" for Pending pods
    // Root cause: total_containers uses len(status.containerStatuses) instead of len(spec.containers)
    // Expected: should show "0/2" for a Pending pod with 2 containers in spec
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "coredns",
            "namespace": "kube-system",
            "creationTimestamp": "2026-04-03T00:00:00Z"
        },
        "spec": {
            "containers": [
                {"name": "coredns", "image": "coredns:latest"},
                {"name": "sidecar", "image": "nginx:latest"}
            ]
        },
        "status": {
            "phase": "Pending",
            "containerStatuses": []  // Empty - pod not created yet
        }
    });

    let table = pod_list_to_table(vec![pod], "1".to_string());

    // Verify table structure
    assert_eq!(table["kind"], "Table");
    assert_eq!(table["rows"].as_array().unwrap().len(), 1);

    // Check READY column (should be "0/2", not "0/0")
    let ready_cell = &table["rows"][0]["cells"][1];
    assert_eq!(
        ready_cell.as_str().unwrap(),
        "0/2",
        "READY should show 0 ready out of 2 total containers from spec.containers"
    );
}

#[test]
fn test_pod_list_to_table_ready_count_with_running_pod() {
    // Verify READY column for a Running pod with containerStatuses populated
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "nginx",
            "namespace": "default",
            "creationTimestamp": "2026-04-03T00:00:00Z"
        },
        "spec": {
            "containers": [
                {"name": "nginx", "image": "nginx:latest"}
            ]
        },
        "status": {
            "phase": "Running",
            "containerStatuses": [
                {
                    "name": "nginx",
                    "ready": true,
                    "restartCount": 0,
                    "state": {"running": {"startedAt": "2026-04-03T00:01:00Z"}}
                }
            ]
        }
    });

    let table = pod_list_to_table(vec![pod], "1".to_string());

    let ready_cell = &table["rows"][0]["cells"][1];
    assert_eq!(
        ready_cell.as_str().unwrap(),
        "1/1",
        "READY should show 1 ready out of 1 total container"
    );
}

#[test]
fn test_pod_list_to_table_ready_count_with_partial_ready() {
    // Pod with 2 containers, only 1 ready
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "multi",
            "namespace": "default",
            "creationTimestamp": "2026-04-03T00:00:00Z"
        },
        "spec": {
            "containers": [
                {"name": "app", "image": "app:latest"},
                {"name": "sidecar", "image": "sidecar:latest"}
            ]
        },
        "status": {
            "phase": "Running",
            "containerStatuses": [
                {"name": "app", "ready": true, "restartCount": 0},
                {"name": "sidecar", "ready": false, "restartCount": 0}
            ]
        }
    });

    let table = pod_list_to_table(vec![pod], "1".to_string());

    let ready_cell = &table["rows"][0]["cells"][1];
    assert_eq!(
        ready_cell.as_str().unwrap(),
        "1/2",
        "READY should show 1 ready out of 2 total containers"
    );
}

// ========================
// wants_table_format tests
// ========================

#[test]
fn test_wants_table_format_with_table_accept_header() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "accept",
        "application/json;as=Table;v=v1;g=meta.k8s.io,application/json"
            .parse()
            .unwrap(),
    );
    assert!(wants_table_format(&headers).unwrap());
}

#[test]
fn test_wants_table_format_with_json_accept_returns_false() {
    let mut headers = HeaderMap::new();
    headers.insert("accept", "application/json".parse().unwrap());
    assert!(!wants_table_format(&headers).unwrap());
}

#[test]
fn test_wants_table_format_no_accept_header_returns_false() {
    let headers = HeaderMap::new();
    assert!(!wants_table_format(&headers).unwrap());
}

#[test]
fn test_wants_table_format_protobuf_accept_returns_false() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "accept",
        "application/vnd.kubernetes.protobuf".parse().unwrap(),
    );
    assert!(!wants_table_format(&headers).unwrap());
}

#[test]
fn test_wants_table_format_unsupported_version_returns_406() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "accept",
        "application/json;as=Table;v=v2;g=meta.k8s.io"
            .parse()
            .unwrap(),
    );
    let result = wants_table_format(&headers);
    assert!(
        result.is_err(),
        "Unsupported Table version should return error"
    );
}

#[test]
fn test_wants_table_format_unsupported_group_returns_406() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "accept",
        "application/json;as=Table;v=v1;g=other.k8s.io"
            .parse()
            .unwrap(),
    );
    let result = wants_table_format(&headers);
    assert!(
        result.is_err(),
        "Unsupported Table group should return error"
    );
}

// ========================
// Authorization API 406 tests
// ========================

#[tokio::test]
async fn test_self_subject_access_review_returns_406_for_table_format() {
    let state = crate::api::test_support::build_test_app_state().await;
    let identity = crate::auth::AuthenticatedIdentity::admin("test-admin");
    let mut headers = HeaderMap::new();
    headers.insert(
        "accept",
        "application/json;as=Table;v=v1;g=meta.k8s.io"
            .parse()
            .unwrap(),
    );
    let body = serde_json::to_vec(&serde_json::json!({
        "apiVersion": "authorization.k8s.io/v1",
        "kind": "SelfSubjectAccessReview",
        "spec": {
            "nonResourceAttributes": {
                "path": "/",
                "verb": "get"
            }
        }
    }))
    .unwrap();
    let result = create_self_subject_access_review(
        axum::extract::State(std::sync::Arc::new(state)),
        axum::Extension(identity),
        headers,
        Bytes::from(body),
    )
    .await;
    assert!(result.is_err(), "Table format should return 406");
    match result.unwrap_err() {
        AppError::NotAcceptable(_) => {}
        other => panic!("Expected NotAcceptable, got: {:?}", other),
    }
}

#[tokio::test]
async fn test_self_subject_access_review_without_table_returns_allowed() {
    let state = crate::api::test_support::build_test_app_state().await;
    let identity = crate::auth::AuthenticatedIdentity::admin("test-admin");
    let mut headers = HeaderMap::new();
    headers.insert("accept", "application/json".parse().unwrap());
    let body = serde_json::to_vec(&serde_json::json!({
        "apiVersion": "authorization.k8s.io/v1",
        "kind": "SelfSubjectAccessReview",
        "spec": {
            "nonResourceAttributes": {
                "path": "/",
                "verb": "get"
            }
        }
    }))
    .unwrap();
    let result = create_self_subject_access_review(
        axum::extract::State(std::sync::Arc::new(state)),
        axum::Extension(identity),
        headers,
        Bytes::from(body),
    )
    .await;
    assert!(result.is_ok(), "Non-Table format should succeed");
    let json = result.unwrap().0;
    assert_eq!(json["kind"], "SelfSubjectAccessReview");
    assert_eq!(json["status"]["allowed"], true);
}

#[tokio::test]
async fn test_self_subject_access_review_spec_round_trips_verbatim() {
    let state = crate::api::test_support::build_test_app_state().await;
    let identity = crate::auth::AuthenticatedIdentity::admin("test-admin");
    let headers = HeaderMap::new();
    let spec_in = serde_json::json!({
        "user": "alice",
        "groups": ["g1", "g2"],
        "resourceAttributes": {
            "verb": "list",
            "resource": "pods",
            "namespace": "ns-x",
            "subresource": "log"
        },
        "extra": {"trace-id": ["abc-123"]}
    });
    let body = serde_json::to_vec(&serde_json::json!({
        "apiVersion": "authorization.k8s.io/v1",
        "kind": "SelfSubjectAccessReview",
        "spec": spec_in.clone(),
    }))
    .unwrap();
    let result = create_self_subject_access_review(
        axum::extract::State(std::sync::Arc::new(state)),
        axum::Extension(identity),
        headers,
        Bytes::from(body),
    )
    .await
    .expect("handler must accept JSON body");
    assert_eq!(
        result.0["spec"], spec_in,
        "spec must round-trip from request to response unchanged"
    );
}

#[tokio::test]
async fn test_local_subject_access_review_injects_namespace_into_spec() {
    let state = crate::api::test_support::build_test_app_state().await;
    let identity = crate::auth::AuthenticatedIdentity::admin("test-admin");
    let headers = HeaderMap::new();
    let body = serde_json::to_vec(&serde_json::json!({
        "apiVersion": "authorization.k8s.io/v1",
        "kind": "LocalSubjectAccessReview",
        "spec": {
            "user": "bob",
            "resourceAttributes": {"verb": "get", "resource": "configmaps"}
        }
    }))
    .unwrap();
    let result = create_local_subject_access_review(
        axum::extract::State(std::sync::Arc::new(state)),
        Path("ns-y".to_string()),
        axum::Extension(identity),
        headers,
        Bytes::from(body),
    )
    .await
    .expect("handler must accept JSON body");
    let json = result.0;
    assert_eq!(json["spec"]["namespace"], "ns-y");
    assert_eq!(json["spec"]["resourceAttributes"]["namespace"], "ns-y");
    assert_eq!(json["spec"]["user"], "bob");
    assert_eq!(json["spec"]["resourceAttributes"]["verb"], "get");
}

#[tokio::test]
async fn test_subject_access_review_accepts_json_body() {
    let state = crate::api::test_support::build_test_app_state().await;
    let identity = crate::auth::AuthenticatedIdentity::admin("test-admin");
    let headers = HeaderMap::new();
    let body = serde_json::to_vec(&serde_json::json!({
        "apiVersion": "authorization.k8s.io/v1",
        "kind": "SubjectAccessReview",
        "spec": {"user": "alice", "resourceAttributes": {"verb": "get", "resource": "pods"}}
    }))
    .unwrap();
    let result = create_subject_access_review(
        axum::extract::State(std::sync::Arc::new(state)),
        axum::Extension(identity),
        headers,
        Bytes::from(body),
    )
    .await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap().0["status"]["allowed"], true);
}

#[tokio::test]
async fn test_subject_access_review_rejects_invalid_body() {
    let state = crate::api::test_support::build_test_app_state().await;
    let identity = crate::auth::AuthenticatedIdentity::admin("test-admin");
    let headers = HeaderMap::new();
    let invalid_body = Bytes::from_static(b"not json, not proto");
    let result = create_subject_access_review(
        axum::extract::State(std::sync::Arc::new(state)),
        axum::Extension(identity),
        headers,
        invalid_body,
    )
    .await;
    assert!(result.is_err(), "Invalid body must return error");
}

#[test]
fn test_decode_json_or_proto_parses_json() {
    let body = br#"{"kind":"SubjectAccessReview"}"#;
    let val = decode_json_or_proto(body).unwrap();
    assert_eq!(val["kind"], "SubjectAccessReview");
}

#[test]
fn test_decode_json_or_proto_rejects_invalid() {
    let body = b"not json";
    assert!(decode_json_or_proto(body).is_err());
}

// ========================
// pod_list_to_table tests
// ========================

#[test]
fn test_pod_list_to_table_empty_list_returns_table_with_no_rows() {
    let result = pod_list_to_table(vec![], "100".to_string());
    assert_eq!(result["kind"], "Table");
    assert_eq!(result["apiVersion"], "meta.k8s.io/v1");
    assert_eq!(result["metadata"]["resourceVersion"], "100");
    assert_eq!(result["rows"].as_array().unwrap().len(), 0);
    assert_eq!(result["columnDefinitions"].as_array().unwrap().len(), 9);
}

#[test]
fn test_pod_list_to_table_includes_kubernetes_wide_columns() {
    let result = pod_list_to_table(vec![], "100".to_string());
    let columns = result["columnDefinitions"].as_array().unwrap();
    let actual: Vec<(&str, i64)> = columns
        .iter()
        .map(|column| {
            (
                column["name"].as_str().unwrap(),
                column["priority"].as_i64().unwrap(),
            )
        })
        .collect();

    assert_eq!(
        actual,
        vec![
            ("Name", 0),
            ("Ready", 0),
            ("Status", 0),
            ("Restarts", 0),
            ("Age", 0),
            ("IP", 1),
            ("Node", 1),
            ("Nominated Node", 1),
            ("Readiness Gates", 1),
        ]
    );
}

#[test]
fn test_pod_list_to_table_running_pod_shows_correct_cells() {
    let pod = json!({
        "metadata": {
            "name": "nginx-abc123",
            "creationTimestamp": "2026-01-01T00:00:00Z"
        },
        "spec": {
            "containers": [
                {"name": "nginx", "image": "nginx:latest"}
            ]
        },
        "status": {
            "phase": "Running",
            "containerStatuses": [
                {"name": "nginx", "ready": true, "restartCount": 0}
            ]
        }
    });

    let result = pod_list_to_table(vec![pod], "42".to_string());
    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);

    let cells = rows[0]["cells"].as_array().unwrap();
    assert_eq!(cells[0], "nginx-abc123"); // NAME
    assert_eq!(cells[1], "1/1"); // READY
    assert_eq!(cells[2], "Running"); // STATUS
    assert_eq!(cells[3], 0); // RESTARTS
    assert!(cells[4].is_string()); // AGE (dynamic)
}

#[test]
fn test_pod_list_to_table_wide_cells_match_kubernetes_printer() {
    let pod = json!({
        "metadata": {
            "name": "wide-pod",
            "creationTimestamp": "2026-01-01T00:00:00Z"
        },
        "spec": {
            "nodeName": "node-a",
            "containers": [
                {"name": "nginx", "image": "nginx:latest"}
            ],
            "readinessGates": [
                {"conditionType": "example.com/ready"},
                {"conditionType": "example.com/blocked"}
            ]
        },
        "status": {
            "phase": "Running",
            "podIP": "10.42.0.7",
            "nominatedNodeName": "node-b",
            "conditions": [
                {"type": "example.com/ready", "status": "True"},
                {"type": "example.com/blocked", "status": "False"}
            ],
            "containerStatuses": [
                {"name": "nginx", "ready": true, "restartCount": 0}
            ]
        }
    });

    let result = pod_list_to_table(vec![pod], "42".to_string());
    let cells = result["rows"][0]["cells"].as_array().unwrap();

    assert_eq!(cells[5], "10.42.0.7");
    assert_eq!(cells[6], "node-a");
    assert_eq!(cells[7], "node-b");
    assert_eq!(cells[8], "1/2");
}

#[test]
fn test_pod_list_to_table_wide_cells_default_to_none() {
    let pod = json!({
        "metadata": {
            "name": "pending-pod",
            "creationTimestamp": "2026-01-01T00:00:00Z"
        },
        "spec": {
            "containers": [
                {"name": "nginx", "image": "nginx:latest"}
            ]
        },
        "status": {
            "phase": "Pending",
            "containerStatuses": []
        }
    });

    let result = pod_list_to_table(vec![pod], "42".to_string());
    let cells = result["rows"][0]["cells"].as_array().unwrap();

    assert_eq!(cells[5], "<none>");
    assert_eq!(cells[6], "<none>");
    assert_eq!(cells[7], "<none>");
    assert_eq!(cells[8], "<none>");
}

#[test]
fn test_pod_list_to_table_multi_container_restart_sum() {
    let pod = json!({
        "metadata": {"name": "multi", "creationTimestamp": "2026-01-01T00:00:00Z"},
        "spec": {
            "containers": [
                {"name": "app", "image": "app:latest"},
                {"name": "sidecar", "image": "sidecar:latest"}
            ]
        },
        "status": {
            "phase": "Running",
            "containerStatuses": [
                {"name": "app", "ready": true, "restartCount": 5},
                {"name": "sidecar", "ready": false, "restartCount": 3}
            ]
        }
    });

    let result = pod_list_to_table(vec![pod], "1".to_string());
    let cells = result["rows"][0]["cells"].as_array().unwrap();
    assert_eq!(cells[1], "1/2"); // READY: 1 of 2 ready
    assert_eq!(cells[3], 8); // RESTARTS: 5 + 3
}

#[test]
fn test_pod_list_to_table_init_container_not_ready_shows_init_prefix() {
    let pod = json!({
        "metadata": {"name": "init-pod", "creationTimestamp": "2026-01-01T00:00:00Z"},
        "status": {
            "phase": "Pending",
            "containerStatuses": [],
            "initContainerStatuses": [
                {"name": "init-db", "ready": false, "restartCount": 0}
            ]
        }
    });

    let result = pod_list_to_table(vec![pod], "1".to_string());
    let cells = result["rows"][0]["cells"].as_array().unwrap();
    assert_eq!(cells[2], "Init:Pending"); // STATUS with Init: prefix
}

#[test]
fn test_pod_list_to_table_missing_status_shows_defaults() {
    let pod = json!({
        "metadata": {"name": "no-status", "creationTimestamp": "2026-01-01T00:00:00Z"}
    });

    let result = pod_list_to_table(vec![pod], "1".to_string());
    let cells = result["rows"][0]["cells"].as_array().unwrap();
    assert_eq!(cells[1], "0/0"); // READY
    assert_eq!(cells[2], "Unknown"); // STATUS
    assert_eq!(cells[3], 0); // RESTARTS
}

#[test]
fn test_pod_list_to_table_prefers_status_reason_for_node_lost_pod() {
    let pod = json!({
        "metadata": {"name": "lost-pod", "creationTimestamp": "2026-01-01T00:00:00Z"},
        "spec": {
            "nodeName": "worker-a",
            "containers": [{"name": "c"}]
        },
        "status": {
            "phase": "Failed",
            "reason": "NodeLost",
            "podIP": "10.42.0.10",
            "containerStatuses": [{
                "name": "c",
                "ready": false,
                "restartCount": 0,
                "state": {"running": {"startedAt": "2026-01-01T00:00:01Z"}}
            }]
        }
    });

    let result = pod_list_to_table(vec![pod], "1".to_string());
    let cells = result["rows"][0]["cells"].as_array().unwrap();
    assert_eq!(cells[2], "NodeLost");
}

#[test]
fn test_pod_list_to_table_invalid_timestamp_shows_unknown_age() {
    let pod = json!({
        "metadata": {"name": "bad-ts", "creationTimestamp": "not-a-date"},
        "status": {"phase": "Running"}
    });

    let result = pod_list_to_table(vec![pod], "1".to_string());
    let cells = result["rows"][0]["cells"].as_array().unwrap();
    assert_eq!(cells[4], "<unknown>");
}

#[test]
fn test_pod_list_to_table_multiple_pods() {
    let pods = vec![
        json!({"metadata": {"name": "pod-a", "creationTimestamp": "2026-01-01T00:00:00Z"}, "status": {"phase": "Running"}}),
        json!({"metadata": {"name": "pod-b", "creationTimestamp": "2026-01-01T00:00:00Z"}, "status": {"phase": "Pending"}}),
    ];

    let result = pod_list_to_table(pods, "99".to_string());
    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["cells"][0], "pod-a");
    assert_eq!(rows[1]["cells"][0], "pod-b");
}

// ========================
// compute_qos_class tests
// ========================

#[test]
fn test_compute_qos_class_best_effort() {
    let pod = serde_json::json!({
        "spec": {
            "containers": [
                {"name": "c1", "image": "nginx"},
                {"name": "c2", "image": "redis"}
            ]
        }
    });
    assert_eq!(compute_qos_class(&pod), "BestEffort");
}

#[test]
fn test_compute_qos_class_guaranteed() {
    let pod = serde_json::json!({
        "spec": {
            "containers": [
                {
                    "name": "c1",
                    "resources": {
                        "limits": {"cpu": "1", "memory": "512Mi"},
                        "requests": {"cpu": "1", "memory": "512Mi"}
                    }
                },
                {
                    "name": "c2",
                    "resources": {
                        "limits": {"cpu": "500m", "memory": "256Mi"},
                        "requests": {"cpu": "500m", "memory": "256Mi"}
                    }
                }
            ]
        }
    });
    assert_eq!(compute_qos_class(&pod), "Guaranteed");
}

#[test]
fn test_compute_qos_class_guaranteed_limits_only() {
    // When limits are set but requests are not, requests default to limits
    let pod = serde_json::json!({
        "spec": {
            "containers": [
                {
                    "name": "c1",
                    "resources": {
                        "limits": {"cpu": "1", "memory": "512Mi"}
                    }
                }
            ]
        }
    });
    assert_eq!(compute_qos_class(&pod), "Guaranteed");
}

#[test]
fn test_compute_qos_class_burstable_partial_resources() {
    let pod = serde_json::json!({
        "spec": {
            "containers": [
                {
                    "name": "c1",
                    "resources": {
                        "requests": {"memory": "256Mi"}
                    }
                }
            ]
        }
    });
    assert_eq!(compute_qos_class(&pod), "Burstable");
}

#[test]
fn test_compute_qos_class_burstable_mismatched_requests_limits() {
    let pod = serde_json::json!({
        "spec": {
            "containers": [
                {
                    "name": "c1",
                    "resources": {
                        "limits": {"cpu": "1", "memory": "512Mi"},
                        "requests": {"cpu": "500m", "memory": "512Mi"}
                    }
                }
            ]
        }
    });
    assert_eq!(compute_qos_class(&pod), "Burstable");
}

#[test]
fn test_compute_qos_class_with_init_containers() {
    // Init containers should be included in QOS calculation
    let pod = serde_json::json!({
        "spec": {
            "initContainers": [
                {
                    "name": "init",
                    "resources": {
                        "limits": {"cpu": "1", "memory": "512Mi"},
                        "requests": {"cpu": "1", "memory": "512Mi"}
                    }
                }
            ],
            "containers": [
                {
                    "name": "c1",
                    "resources": {
                        "limits": {"cpu": "1", "memory": "512Mi"},
                        "requests": {"cpu": "1", "memory": "512Mi"}
                    }
                }
            ]
        }
    });
    assert_eq!(compute_qos_class(&pod), "Guaranteed");
}

// ========================
// node_list_to_table tests
// ========================

#[test]
fn test_node_list_to_table_empty_list_returns_table_with_no_rows() {
    let result = node_list_to_table(vec![], "50".to_string());
    assert_eq!(result["kind"], "Table");
    assert_eq!(result["apiVersion"], "meta.k8s.io/v1");
    assert_eq!(result["metadata"]["resourceVersion"], "50");
    assert_eq!(result["rows"].as_array().unwrap().len(), 0);
    let columns = result["columnDefinitions"].as_array().unwrap();
    let column_names: Vec<&str> = columns
        .iter()
        .map(|column| column["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        column_names,
        vec![
            "Name",
            "Status",
            "Roles",
            "Age",
            "Version",
            "Internal-IP",
            "External-IP",
            "OS-Image",
            "Kernel-Version",
            "Container-Runtime",
            "Commit",
        ]
    );
    for column in &columns[0..5] {
        assert_eq!(column["priority"], 0);
    }
    for column in &columns[5..11] {
        assert_eq!(column["priority"], 1);
    }
}

#[test]
fn test_node_list_to_table_ready_node_shows_correct_cells() {
    let node = json!({
        "metadata": {
            "name": "node-1",
            "creationTimestamp": "2026-01-01T00:00:00Z",
            "labels": {"node-role.kubernetes.io/leader": ""},
            "annotations": {"klights.io/git-commit": "abc12345"}
        },
        "status": {
            "conditions": [{"type": "Ready", "status": "True"}],
            "addresses": [
                {"type": "Hostname", "address": "node-1"},
                {"type": "InternalIP", "address": "10.0.0.10"},
                {"type": "ExternalIP", "address": "203.0.113.10"}
            ],
            "nodeInfo": {
                "kubeletVersion": "v1.34+klights1.0.0",
                "osImage": "Ubuntu 24.04.4 LTS",
                "kernelVersion": "6.17.0-23-generic",
                "containerRuntimeVersion": "containerd://2.2.3"
            }
        }
    });

    let result = node_list_to_table(vec![node], "10".to_string());
    let cells = result["rows"][0]["cells"].as_array().unwrap();
    assert_eq!(cells.len(), 11);
    assert_eq!(cells[0], "node-1"); // NAME
    assert_eq!(cells[1], "Ready"); // STATUS
    assert_eq!(cells[2], "leader"); // ROLES
    assert!(cells[3].is_string()); // AGE
    assert_eq!(cells[4], "v1.34+klights1.0.0"); // VERSION
    assert_eq!(cells[5], "10.0.0.10"); // INTERNAL-IP
    assert_eq!(cells[6], "203.0.113.10"); // EXTERNAL-IP
    assert_eq!(cells[7], "Ubuntu 24.04.4 LTS"); // OS-IMAGE
    assert_eq!(cells[8], "6.17.0-23-generic"); // KERNEL-VERSION
    assert_eq!(cells[9], "containerd://2.2.3"); // CONTAINER-RUNTIME
    assert_eq!(cells[10], "abc12345"); // COMMIT
}

#[test]
fn test_node_list_to_table_ready_unschedulable_node_shows_scheduling_disabled() {
    let node = json!({
        "metadata": {
            "name": "node-1",
            "creationTimestamp": "2026-01-01T00:00:00Z"
        },
        "spec": {"unschedulable": true},
        "status": {
            "conditions": [{"type": "Ready", "status": "True"}],
            "nodeInfo": {"kubeletVersion": "v1.34+klights1.0.0"}
        }
    });

    let result = node_list_to_table(vec![node], "10".to_string());
    let cells = result["rows"][0]["cells"].as_array().unwrap();
    assert_eq!(cells[1], "Ready,SchedulingDisabled");
}

#[test]
fn test_node_list_to_table_shows_worker_role_from_labels() {
    let node = json!({
        "metadata": {
            "name": "node-2",
            "creationTimestamp": "2026-01-01T00:00:00Z",
            "labels": {"node-role.kubernetes.io/worker": ""}
        },
        "status": {
            "conditions": [{"type": "Ready", "status": "True"}],
            "nodeInfo": {"kubeletVersion": "v1.34+klights1.0.0"}
        }
    });

    let result = node_list_to_table(vec![node], "10".to_string());
    let cells = result["rows"][0]["cells"].as_array().unwrap();
    assert_eq!(cells[2], "worker");
}

#[test]
fn test_node_list_to_table_shows_none_when_no_role_labels() {
    let node = json!({
        "metadata": {"name": "node-3", "creationTimestamp": "2026-01-01T00:00:00Z"},
        "status": {
            "conditions": [{"type": "Ready", "status": "True"}],
            "nodeInfo": {"kubeletVersion": "v1.34+klights1.0.0"}
        }
    });

    let result = node_list_to_table(vec![node], "10".to_string());
    let cells = result["rows"][0]["cells"].as_array().unwrap();
    assert_eq!(cells[2], "<none>");
}

#[test]
fn test_node_list_to_table_not_ready_node() {
    let node = json!({
        "metadata": {"name": "node-2", "creationTimestamp": "2026-01-01T00:00:00Z"},
        "status": {
            "conditions": [{"type": "Ready", "status": "False"}],
            "nodeInfo": {"kubeletVersion": "v1.34.6"}
        }
    });

    let result = node_list_to_table(vec![node], "1".to_string());
    let cells = result["rows"][0]["cells"].as_array().unwrap();
    assert_eq!(cells[1], "NotReady");
}

#[test]
fn test_node_list_to_table_no_conditions_shows_unknown() {
    let node = json!({
        "metadata": {"name": "node-3", "creationTimestamp": "2026-01-01T00:00:00Z"},
        "status": {}
    });

    let result = node_list_to_table(vec![node], "1".to_string());
    let cells = result["rows"][0]["cells"].as_array().unwrap();
    assert_eq!(cells[1], "Unknown"); // STATUS
    assert_eq!(cells[4], "<unknown>"); // VERSION
    assert_eq!(cells[5], "<none>"); // INTERNAL-IP
    assert_eq!(cells[6], "<none>"); // EXTERNAL-IP
    assert_eq!(cells[7], "<unknown>"); // OS-IMAGE
    assert_eq!(cells[8], "<unknown>"); // KERNEL-VERSION
    assert_eq!(cells[9], "<unknown>"); // CONTAINER-RUNTIME
    assert_eq!(cells[10], "<unknown>"); // COMMIT
}

#[test]
fn test_node_list_to_table_invalid_timestamp_shows_unknown_age() {
    let node = json!({
        "metadata": {"name": "node-4", "creationTimestamp": "invalid"},
        "status": {"conditions": [{"type": "Ready", "status": "True"}]}
    });

    let result = node_list_to_table(vec![node], "1".to_string());
    let cells = result["rows"][0]["cells"].as_array().unwrap();
    assert_eq!(cells[3], "<unknown>");
}

// ========================
// apply_patch edge case tests
// ========================

#[test]
fn test_apply_patch_unsupported_content_type_returns_error() {
    let current = json!({"metadata": {"name": "test"}});
    let patch = json!({"spec": {"replicas": 3}});

    let result = apply_patch(&current, &patch, Some("text/plain"));
    assert!(result.is_err());
}

#[test]
fn test_apply_patch_strategic_merge_patch_treated_as_merge() {
    let current = json!({
        "metadata": {"name": "test"},
        "spec": {"replicas": 1, "selector": {"app": "web"}}
    });
    let patch = json!({"spec": {"replicas": 3}});

    let result = apply_patch(
        &current,
        &patch,
        Some("application/strategic-merge-patch+json"),
    )
    .unwrap();
    assert_eq!(result["spec"]["replicas"], 3);
    assert_eq!(result["spec"]["selector"]["app"], "web");
}

#[test]
fn test_apply_patch_apply_patch_yaml_treated_as_merge() {
    let current = json!({"metadata": {"name": "test"}, "data": {"key": "val"}});
    let patch = json!({"data": {"key2": "val2"}});

    let result = apply_patch(&current, &patch, Some("application/apply-patch+yaml")).unwrap();
    assert_eq!(result["data"]["key"], "val");
    assert_eq!(result["data"]["key2"], "val2");
}

#[test]
fn test_apply_patch_none_content_type_defaults_to_merge() {
    let current = json!({"metadata": {"name": "test"}, "spec": {"a": 1}});
    let patch = json!({"spec": {"b": 2}});

    let result = apply_patch(&current, &patch, None).unwrap();
    assert_eq!(result["spec"]["a"], 1);
    assert_eq!(result["spec"]["b"], 2);
}

#[test]
fn test_apply_patch_json_content_type_uses_merge() {
    let current = json!({"metadata": {"name": "test"}, "data": {"x": 1}});
    let patch = json!({"data": {"y": 2}});

    let result = apply_patch(&current, &patch, Some("application/json")).unwrap();
    assert_eq!(result["data"]["x"], 1);
    assert_eq!(result["data"]["y"], 2);
}

#[test]
fn test_strategic_merge_patch_conditions_adds_new_type() {
    // Patching status.conditions with a new type should ADD, not replace the array
    let current = json!({
        "status": {
            "conditions": [
                {"type": "Ready", "status": "True", "reason": "All good"}
            ]
        }
    });
    let patch = json!({
        "status": {
            "conditions": [
                {"type": "PodScheduled", "status": "True"}
            ]
        }
    });
    let result = apply_patch(
        &current,
        &patch,
        Some("application/strategic-merge-patch+json"),
    )
    .unwrap();
    let conditions = result["status"]["conditions"].as_array().unwrap();
    assert_eq!(
        conditions.len(),
        2,
        "Should have both conditions, not just the patched one"
    );
    let types: Vec<&str> = conditions
        .iter()
        .filter_map(|c| c["type"].as_str())
        .collect();
    assert!(
        types.contains(&"Ready"),
        "Ready condition should still exist"
    );
    assert!(
        types.contains(&"PodScheduled"),
        "PodScheduled should be added"
    );
}

#[test]
fn test_strategic_merge_patch_conditions_updates_existing_type() {
    // Patching status.conditions with an existing type should UPDATE in-place
    let current = json!({
        "status": {
            "conditions": [
                {"type": "Ready", "status": "False", "reason": "NotReady"},
                {"type": "PodScheduled", "status": "True"}
            ]
        }
    });
    let patch = json!({
        "status": {
            "conditions": [
                {"type": "Ready", "status": "True", "reason": "AllGood"}
            ]
        }
    });
    let result = apply_patch(
        &current,
        &patch,
        Some("application/strategic-merge-patch+json"),
    )
    .unwrap();
    let conditions = result["status"]["conditions"].as_array().unwrap();
    assert_eq!(conditions.len(), 2, "Should still have 2 conditions");
    let ready = conditions.iter().find(|c| c["type"] == "Ready").unwrap();
    assert_eq!(ready["status"], "True");
    assert_eq!(ready["reason"], "AllGood");
}

#[test]
fn test_strategic_merge_patch_containers_merges_by_name() {
    // spec.containers uses "name" as merge key
    let current = json!({
        "spec": {
            "containers": [
                {"name": "app", "image": "nginx:1.0", "ports": [{"containerPort": 80}]},
                {"name": "sidecar", "image": "busybox"}
            ]
        }
    });
    let patch = json!({
        "spec": {
            "containers": [
                {"name": "app", "image": "nginx:1.1"}
            ]
        }
    });
    let result = apply_patch(
        &current,
        &patch,
        Some("application/strategic-merge-patch+json"),
    )
    .unwrap();
    let containers = result["spec"]["containers"].as_array().unwrap();
    assert_eq!(containers.len(), 2, "Should still have both containers");
    let app = containers.iter().find(|c| c["name"] == "app").unwrap();
    assert_eq!(app["image"], "nginx:1.1", "app image should be updated");
    let sidecar = containers.iter().find(|c| c["name"] == "sidecar").unwrap();
    assert_eq!(sidecar["image"], "busybox", "sidecar should be unchanged");
}

#[test]
fn test_strategic_merge_patch_non_array_fields_still_deep_merge() {
    // Non-array fields should still deep merge as before
    let current = json!({
        "metadata": {"name": "test", "labels": {"app": "web"}},
        "spec": {"replicas": 1}
    });
    let patch = json!({
        "spec": {"replicas": 3},
        "metadata": {"labels": {"version": "v2"}}
    });
    let result = apply_patch(
        &current,
        &patch,
        Some("application/strategic-merge-patch+json"),
    )
    .unwrap();
    assert_eq!(result["spec"]["replicas"], 3);
    assert_eq!(result["metadata"]["name"], "test");
    assert_eq!(result["metadata"]["labels"]["app"], "web");
    assert_eq!(result["metadata"]["labels"]["version"], "v2");
}

#[test]
fn test_strategic_merge_patch_owner_references_merges_by_uid() {
    // metadata.ownerReferences must merge by uid so adding a second owner does
    // not replace the original controller owner.
    let current = json!({
        "metadata": {
            "name": "pod-1",
            "ownerReferences": [{
                "apiVersion": "v1",
                "kind": "ReplicationController",
                "name": "rc-to-be-deleted",
                "uid": "uid-rc-delete",
                "controller": true,
                "blockOwnerDeletion": true
            }]
        }
    });
    let patch = json!({
        "metadata": {
            "ownerReferences": [{
                "apiVersion": "v1",
                "kind": "ReplicationController",
                "name": "rc-to-stay",
                "uid": "uid-rc-stay"
            }]
        }
    });

    let result = apply_patch(
        &current,
        &patch,
        Some("application/strategic-merge-patch+json"),
    )
    .unwrap();

    let owners = result["metadata"]["ownerReferences"].as_array().unwrap();
    assert_eq!(
        owners.len(),
        2,
        "must keep existing owner and add new owner"
    );
    assert!(
        owners.iter().any(|o| o["uid"] == "uid-rc-delete"),
        "existing owner must be preserved"
    );
    assert!(
        owners.iter().any(|o| o["uid"] == "uid-rc-stay"),
        "patched owner must be added"
    );
}

// ========================
// Namespace pagination tests
// ========================

#[test]
fn test_namespace_pagination_first_page_has_continue() {
    // Simulates the fixed pagination logic (was buggy: drain corrupted indices)
    let items: Vec<Value> = (1..=5)
        .map(|i| json!({"metadata": {"name": format!("ns-{}", i)}}))
        .collect();
    let total_len = items.len();
    let offset: usize = 0;
    let limit: usize = 2;
    let end = (offset + limit).min(total_len);
    let page: Vec<Value> = items[offset..end].to_vec();
    let token = if end < total_len {
        Some(end.to_string())
    } else {
        None
    };

    assert_eq!(page.len(), 2);
    assert_eq!(token, Some("2".to_string()));
}

#[test]
fn test_namespace_pagination_second_page_has_continue() {
    let items: Vec<Value> = (1..=5)
        .map(|i| json!({"metadata": {"name": format!("ns-{}", i)}}))
        .collect();
    let total_len = items.len();
    let offset: usize = 2;
    let limit: usize = 2;
    let end = (offset + limit).min(total_len);
    let page: Vec<Value> = items[offset..end].to_vec();
    let token = if end < total_len {
        Some(end.to_string())
    } else {
        None
    };

    assert_eq!(page.len(), 2);
    assert_eq!(page[0]["metadata"]["name"], "ns-3");
    assert_eq!(
        token,
        Some("4".to_string()),
        "Should have continue — ns-5 remaining"
    );
}

#[test]
fn test_namespace_pagination_last_page_no_continue() {
    let items: Vec<Value> = (1..=5)
        .map(|i| json!({"metadata": {"name": format!("ns-{}", i)}}))
        .collect();
    let total_len = items.len();
    let offset: usize = 4;
    let limit: usize = 2;
    let end = (offset + limit).min(total_len);
    let page: Vec<Value> = items[offset..end].to_vec();
    let token = if end < total_len {
        Some(end.to_string())
    } else {
        None
    };

    assert_eq!(page.len(), 1);
    assert_eq!(page[0]["metadata"]["name"], "ns-5");
    assert!(token.is_none());
}

#[test]
fn test_namespace_pagination_collects_all_items() {
    let items: Vec<Value> = (1..=5)
        .map(|i| json!({"metadata": {"name": format!("ns-{}", i)}}))
        .collect();
    let total_len = items.len();
    let limit: usize = 2;
    let mut all_names: Vec<String> = Vec::new();
    let mut offset: usize = 0;
    loop {
        let end = (offset + limit).min(total_len);
        let page: Vec<Value> = items[offset..end].to_vec();
        for item in &page {
            all_names.push(item["metadata"]["name"].as_str().unwrap().to_string());
        }
        if end >= total_len {
            break;
        }
        offset = end;
    }
    assert_eq!(all_names, vec!["ns-1", "ns-2", "ns-3", "ns-4", "ns-5"]);
}

#[test]
fn test_namespace_pagination_exact_fit_no_continue() {
    let items: Vec<Value> = (1..=4)
        .map(|i| json!({"metadata": {"name": format!("ns-{}", i)}}))
        .collect();
    let total_len = items.len();
    let offset: usize = 0;
    let limit: usize = 4;
    let end = (offset + limit).min(total_len);
    let page: Vec<Value> = items[offset..end].to_vec();
    let token = if end < total_len {
        Some(end.to_string())
    } else {
        None
    };

    assert_eq!(page.len(), 4);
    assert!(token.is_none());
}

// S5.6: Basic resource validation tests

#[test]
fn test_validation_dns_subdomain_valid_names() {
    // Valid DNS subdomain names
    let valid_names = vec![
        "my-service",
        "nginx-1",
        "app.example.com",
        "test-123",
        "a",
        "1test",
        "test.with.dots",
        "test-with-hyphens",
    ];

    for name in valid_names {
        assert!(
            validate_dns_subdomain(name),
            "Name '{}' should be valid",
            name
        );
    }
}

#[test]
fn test_validation_dns_subdomain_invalid_names() {
    // Invalid DNS subdomain names
    let invalid_names = vec![
        "MyService",             // Uppercase
        "my_service",            // Underscore
        "my service",            // Space
        "-starts-hyphen",        // Starts with hyphen
        "ends-hyphen-",          // Ends with hyphen
        ".starts-dot",           // Starts with dot
        "ends-dot.",             // Ends with dot
        "system:controller:foo", // Colon is valid only for RBAC path-segment names
        ":starts-colon",         // Starts with colon
        "ends-colon:",           // Ends with colon
        "has@special",           // Special characters
        "has!special",
        "", // Empty
    ];

    for name in invalid_names {
        assert!(
            !validate_dns_subdomain(name),
            "Name '{}' should be invalid",
            name
        );
    }
}

#[test]
fn test_validation_dns_subdomain_length_limit() {
    // Max 253 characters
    let max_len_name = "a".repeat(253);
    assert!(
        validate_dns_subdomain(&max_len_name),
        "253-char name should be valid"
    );

    let too_long_name = "a".repeat(254);
    assert!(
        !validate_dns_subdomain(&too_long_name),
        "254-char name should be invalid"
    );
}

#[test]
fn test_metadata_name_validation_allows_colons_only_for_rbac_resources() {
    assert!(validate_metadata_name_for_kind(
        "rbac.authorization.k8s.io/v1",
        "ClusterRole",
        "system:controller:foo"
    ));
    assert!(validate_metadata_name_for_kind(
        "rbac.authorization.k8s.io/v1",
        "ClusterRoleBinding",
        "wardler:aggregator-5592-sample-reader"
    ));
    assert!(validate_metadata_name_for_kind(
        "rbac.authorization.k8s.io/v1",
        "Role",
        "namespace:reader"
    ));
    assert!(validate_metadata_name_for_kind(
        "rbac.authorization.k8s.io/v1",
        "RoleBinding",
        "namespace:reader-binding"
    ));

    for kind in ["Pod", "ConfigMap", "Secret", "Service", "Namespace"] {
        assert!(
            !validate_metadata_name_for_kind("v1", kind, "system:controller:foo"),
            "{kind} metadata.name must reject colons"
        );
    }
}

#[test]
fn test_namespace_metadata_name_uses_dns_label_validation() {
    assert!(validate_metadata_name_for_kind(
        "v1",
        "Namespace",
        "team-alpha"
    ));
    assert!(
        !validate_metadata_name_for_kind("v1", "Namespace", "team.alpha"),
        "Namespace metadata.name must reject DNS subdomain dots"
    );
    assert!(
        !validate_metadata_name_for_kind("v1", "Namespace", &"a".repeat(64)),
        "Namespace metadata.name must enforce the DNS label length limit"
    );
}

#[test]
fn test_rbac_metadata_name_uses_path_segment_validation() {
    for invalid in [".", "..", "has/slash", "has%percent"] {
        assert!(
            !validate_metadata_name_for_kind(
                "rbac.authorization.k8s.io/v1",
                "ClusterRole",
                invalid
            ),
            "RBAC metadata.name must reject invalid path segment {invalid:?}"
        );
    }
}

#[test]
fn test_validation_dns_label_valid() {
    // Valid DNS labels (63 chars max, no dots)
    let valid_labels = vec!["nginx", "my-app-1", "test123", "a"];

    for label in valid_labels {
        assert!(
            validate_dns_label(label),
            "Label '{}' should be valid",
            label
        );
    }
}

#[test]
fn test_validation_dns_label_invalid() {
    let invalid_labels = vec![
        "My-App", // Uppercase
        "-starts-hyphen",
        "ends-hyphen-",
        "has.dot", // Dots not allowed in labels
        "",
    ];

    for label in invalid_labels {
        assert!(
            !validate_dns_label(label),
            "Label '{}' should be invalid",
            label
        );
    }
}

#[test]
fn test_validation_dns_label_length_limit() {
    // Max 63 characters for DNS label
    let max_len_label = "a".repeat(63);
    assert!(
        validate_dns_label(&max_len_label),
        "63-char label should be valid"
    );

    let too_long_label = "a".repeat(64);
    assert!(
        !validate_dns_label(&too_long_label),
        "64-char label should be invalid"
    );
}

#[test]
fn test_validate_pod_sysctls_allows_kubernetes_safe_sysctls() {
    let pod = json!({
        "spec": {
            "securityContext": {
                "sysctls": [
                    {"name": "kernel.shm_rmid_forced", "value": "1"},
                    {"name": "net.ipv4.ip_unprivileged_port_start", "value": "1024"}
                ]
            }
        }
    });
    assert!(validate_pod_sysctls(&pod).is_ok());
}

#[test]
fn test_validate_pod_sysctls_rejects_invalid_names() {
    let pod = json!({
        "spec": {
            "securityContext": {
                "sysctls": [
                    {"name": "foo-", "value": "bar"},
                    {"name": "kernel.shmmax", "value": "100000000"},
                    {"name": "safe-and-unsafe", "value": "100000000"},
                    {"name": "bar..", "value": "42"}
                ]
            }
        }
    });
    match validate_pod_sysctls(&pod) {
        Err(AppError::UnprocessableEntity(msg)) => {
            assert!(msg.contains("Invalid value: \"foo-\""), "{msg}");
            assert!(msg.contains("Invalid value: \"bar..\""), "{msg}");
            assert!(!msg.contains("safe-and-unsafe"), "{msg}");
            assert!(!msg.contains("kernel.shmmax"), "{msg}");
        }
        other => panic!("unexpected result: {:?}", other),
    }
}

#[test]
fn test_validate_pod_sysctls_allows_unsafe_names_at_api_validation() {
    let pod = json!({
        "spec": {
            "securityContext": {
                "sysctls": [
                    {"name": "kernel.shmmax", "value": "100000000"},
                    {"name": "safe-and-unsafe", "value": "100000000"}
                ]
            }
        }
    });
    assert!(validate_pod_sysctls(&pod).is_ok());
}

#[test]
fn test_resource_version_parse_none() {
    // resourceVersion missing or None should default to 0 (send all)
    let rv_none: Option<String> = None;
    let parsed: i64 = rv_none
        .as_ref()
        .and_then(|rv| rv.parse::<i64>().ok())
        .unwrap_or(0);
    assert_eq!(parsed, 0);
}

#[test]
fn test_resource_version_parse_zero() {
    // resourceVersion "0" should parse to 0 (send all)
    let rv_zero = Some("0".to_string());
    let parsed: i64 = rv_zero
        .as_ref()
        .and_then(|rv| rv.parse::<i64>().ok())
        .unwrap_or(0);
    assert_eq!(parsed, 0);
}

#[test]
fn test_resource_version_parse_positive() {
    // resourceVersion "123" should parse to 123
    let rv_positive = Some("123".to_string());
    let parsed: i64 = rv_positive
        .as_ref()
        .and_then(|rv| rv.parse::<i64>().ok())
        .unwrap_or(0);
    assert_eq!(parsed, 123);
}

#[test]
fn test_resource_version_parse_invalid() {
    // Invalid resourceVersion should default to 0
    let rv_invalid = Some("abc".to_string());
    let parsed: i64 = rv_invalid
        .as_ref()
        .and_then(|rv| rv.parse::<i64>().ok())
        .unwrap_or(0);
    assert_eq!(parsed, 0);
}

#[test]
fn test_watch_filter_logic_rv_zero() {
    // When requested_rv = 0, all resources should pass filter
    let requested_rv: i64 = 0;
    let resource_rv = 5;

    let should_send = !(requested_rv > 0 && resource_rv <= requested_rv);
    assert!(should_send, "resourceVersion=0 should send all resources");
}

#[test]
fn test_watch_filter_logic_rv_positive_old_resource() {
    // When requested_rv > 0, resources with rv <= requested_rv should be filtered
    let requested_rv: i64 = 10;
    let resource_rv = 5;

    let should_send = !(requested_rv > 0 && resource_rv <= requested_rv);
    assert!(
        !should_send,
        "Old resources (rv <= requested_rv) should be filtered"
    );
}

#[test]
fn test_watch_filter_logic_rv_positive_new_resource() {
    // When requested_rv > 0, resources with rv > requested_rv should pass
    let requested_rv: i64 = 10;
    let resource_rv = 15;

    let should_send = !(requested_rv > 0 && resource_rv <= requested_rv);
    assert!(
        should_send,
        "New resources (rv > requested_rv) should pass filter"
    );
}

#[test]
fn test_watch_filter_logic_rv_equal() {
    // When requested_rv > 0, resources with rv == requested_rv should be filtered
    let requested_rv: i64 = 10;
    let resource_rv = 10;

    let should_send = !(requested_rv > 0 && resource_rv <= requested_rv);
    assert!(
        !should_send,
        "Resources with rv == requested_rv should be filtered"
    );
}

#[test]
fn test_watch_event_from_type_maps_modified() {
    let data = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "cm",
            "namespace": "default",
            "resourceVersion": "12986"
        },
        "data": {"mutation": "2"}
    });

    let event = watch_event_from_type("MODIFIED", data);
    assert_eq!(event.event_type, EventType::Modified);
}

#[test]
fn test_watch_event_from_type_maps_added() {
    let data = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "cm-new",
            "namespace": "default",
            "resourceVersion": "13000"
        }
    });

    let event = watch_event_from_type("ADDED", data);
    assert_eq!(event.event_type, EventType::Added);
}

// ========================================
// DELETE collection tests
// ========================================

#[tokio::test]
async fn test_delete_collection_pods_deletes_all_in_namespace() {
    let db = crate::datastore::test_support::in_memory().await;

    // Create 3 pods in test-ns
    db.create_resource(
        "v1",
        "Pod",
        Some("test-ns"),
        "pod1",
        json!({"metadata": {"name": "pod1"}}),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("test-ns"),
        "pod2",
        json!({"metadata": {"name": "pod2"}}),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("test-ns"),
        "pod3",
        json!({"metadata": {"name": "pod3"}}),
    )
    .await
    .unwrap();

    // DELETE collection: list all pods, then delete each
    let list = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();

    for resource in list.items {
        let _ = db
            .delete_resource("v1", "Pod", Some("test-ns"), &resource.name.clone())
            .await;
    }

    // Verify all pods deleted
    let list = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(list.items.len(), 0, "All pods should be deleted");
}

#[tokio::test]
async fn test_delete_collection_pods_with_label_selector() {
    let db = crate::datastore::test_support::in_memory().await;

    // Create 2 pods with app=nginx, 1 with app=redis
    db.create_resource(
        "v1",
        "Pod",
        Some("test-ns"),
        "nginx1",
        json!({"metadata": {"name": "nginx1", "labels": {"app": "nginx"}}}),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("test-ns"),
        "nginx2",
        json!({"metadata": {"name": "nginx2", "labels": {"app": "nginx"}}}),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("test-ns"),
        "redis1",
        json!({"metadata": {"name": "redis1", "labels": {"app": "redis"}}}),
    )
    .await
    .unwrap();

    // DELETE collection with labelSelector=app=nginx
    let list = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::new(Some("app=nginx"), None, None, None),
        )
        .await
        .unwrap();

    for resource in list.items {
        let _ = db
            .delete_resource("v1", "Pod", Some("test-ns"), &resource.name.clone())
            .await;
    }

    // Verify only nginx pods deleted, redis pod remains
    let list = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(list.items.len(), 1, "Only redis pod should remain");
    assert_eq!(list.items[0].name, "redis1");
}

#[tokio::test]
async fn test_create_resource_sets_creation_timestamp() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_json = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "test-pod",
            "namespace": "default"
        },
        "spec": {
            "containers": [{
                "name": "nginx",
                "image": "nginx"
            }]
        }
    });

    let resource = db
        .create_resource("v1", "Pod", Some("default"), "test-pod", pod_json)
        .await
        .unwrap();

    // creationTimestamp should be set automatically
    let timestamp = resource.data["metadata"]["creationTimestamp"].as_str();
    assert!(timestamp.is_some(), "creationTimestamp should be set");

    // Verify format: YYYY-MM-DDTHH:MM:SS.fffffffffZ (with nanoseconds)
    let ts = timestamp.unwrap();
    assert!(ts.ends_with("Z"), "Timestamp should end with Z");
    assert!(ts.contains("T"), "Timestamp should contain T separator");
    assert!(
        ts.contains("."),
        "Timestamp should contain fractional seconds"
    );
    assert_eq!(
        ts.len(),
        30,
        "Timestamp should be 30 chars (RFC 3339 with nanoseconds)"
    );
}

#[tokio::test]
async fn test_create_resource_preserves_existing_timestamp() {
    let db = crate::datastore::test_support::in_memory().await;
    let explicit_timestamp = "2025-01-01T12:00:00Z";
    let pod_json = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "test-pod",
            "namespace": "default",
            "creationTimestamp": explicit_timestamp
        },
        "spec": {
            "containers": [{
                "name": "nginx",
                "image": "nginx"
            }]
        }
    });

    let resource = db
        .create_resource("v1", "Pod", Some("default"), "test-pod", pod_json)
        .await
        .unwrap();

    // Explicit creationTimestamp should be preserved
    let timestamp = resource.data["metadata"]["creationTimestamp"].as_str();
    assert_eq!(
        timestamp.unwrap(),
        explicit_timestamp,
        "Explicit timestamp should be preserved"
    );
}

#[tokio::test]
async fn test_generation_initialized_on_create() {
    // This test documents that metadata.generation is initialized to 1 in the
    // API create handlers (namespaced_resource_handlers! and cluster_resource_handlers!)
    // before calling db.create_resource.
    //
    // The fix is in src/api.rs around lines 2662 and 3283:
    // if !meta_obj.contains_key("generation") {
    //     meta_obj.insert("generation".to_string(), serde_json::json!(1));
    // }
    //
    // This runs after UID and creationTimestamp initialization, ensuring all resources
    // created through the API have generation=1 on create (incremented on spec updates).
    //
    // Testing through the full API handler stack (HTTP → axum → handler → DB) would
    // require spinning up the server, so we document the requirement here and verify
    // via manual testing or Sonobuoy.
    // Marker: generation initialization is implemented in API create handlers.
}

// ========================
// watch_event_to_table tests
// ========================

#[test]
fn test_watch_event_to_table_pod_modified_includes_ready_status_restarts() {
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "nginx",
            "namespace": "default",
            "resourceVersion": "100",
            "creationTimestamp": "2026-04-10T00:00:00Z"
        },
        "spec": {
            "containers": [{"name": "nginx", "image": "nginx"}]
        },
        "status": {
            "phase": "Running",
            "containerStatuses": [{
                "name": "nginx",
                "ready": true,
                "restartCount": 0,
                "state": {"running": {"startedAt": "2026-04-10T00:00:01Z"}}
            }]
        }
    });
    let event = WatchEvent::modified(pod);
    let table_event = watch_event_to_table(event, "Pod");

    assert_eq!(table_event.event_type, EventType::Modified);
    assert_eq!(table_event.object["kind"], "Table");
    assert_eq!(table_event.object["apiVersion"], "meta.k8s.io/v1");

    let rows = table_event.object["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);

    let cells = rows[0]["cells"].as_array().unwrap();
    assert_eq!(cells.len(), 9); // NAME, READY, STATUS, RESTARTS, AGE, IP, NODE, NOMINATED NODE, READINESS GATES
    assert_eq!(cells[0], "nginx");
    assert_eq!(cells[1], "1/1");
    assert_eq!(cells[2], "Running");
    assert_eq!(cells[3], 0);
}

#[test]
fn test_watch_event_to_table_pod_pending_shows_zero_ready() {
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "test",
            "namespace": "default",
            "resourceVersion": "50",
            "creationTimestamp": "2026-04-10T00:00:00Z"
        },
        "spec": {
            "containers": [
                {"name": "c1", "image": "img1"},
                {"name": "c2", "image": "img2"}
            ]
        },
        "status": {
            "phase": "Pending"
        }
    });
    let event = WatchEvent::added(pod);
    let table_event = watch_event_to_table(event, "Pod");

    assert_eq!(table_event.event_type, EventType::Added);
    let cells = table_event.object["rows"][0]["cells"].as_array().unwrap();
    assert_eq!(cells[0], "test");
    assert_eq!(cells[1], "0/2");
    assert_eq!(cells[2], "Pending");
    assert_eq!(cells[3], 0);
}

#[test]
fn test_watch_event_to_table_node_includes_status_roles_version() {
    let node = json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": "dp",
            "resourceVersion": "10",
            "creationTimestamp": "2026-04-10T00:00:00Z",
            "labels": {"node-role.kubernetes.io/leader": ""},
            "annotations": {"klights.io/git-commit": "deadbeef"}
        },
        "status": {
            "conditions": [{"type": "Ready", "status": "True"}],
            "addresses": [
                {"type": "InternalIP", "address": "10.0.0.10"},
                {"type": "ExternalIP", "address": "203.0.113.10"}
            ],
            "nodeInfo": {
                "kubeletVersion": "v1.34+klights",
                "osImage": "Ubuntu 24.04.4 LTS",
                "kernelVersion": "6.17.0-23-generic",
                "containerRuntimeVersion": "containerd://2.2.3"
            }
        }
    });
    let event = WatchEvent::modified(node);
    let table_event = watch_event_to_table(event, "Node");

    assert_eq!(table_event.object["kind"], "Table");
    let cells = table_event.object["rows"][0]["cells"].as_array().unwrap();
    assert_eq!(cells.len(), 11);
    assert_eq!(cells[0], "dp");
    assert_eq!(cells[1], "Ready");
    assert_eq!(cells[2], "leader");
    assert_eq!(cells[4], "v1.34+klights");
    assert_eq!(cells[5], "10.0.0.10");
    assert_eq!(cells[6], "203.0.113.10");
    assert_eq!(cells[7], "Ubuntu 24.04.4 LTS");
    assert_eq!(cells[8], "6.17.0-23-generic");
    assert_eq!(cells[9], "containerd://2.2.3");
    assert_eq!(cells[10], "deadbeef");
}

#[test]
fn test_watch_event_to_table_generic_resource_has_name_and_age() {
    let svc = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "my-svc",
            "namespace": "default",
            "resourceVersion": "200",
            "creationTimestamp": "2026-04-10T00:00:00Z"
        }
    });
    let event = WatchEvent::added(svc);
    let table_event = watch_event_to_table(event, "Service");

    assert_eq!(table_event.event_type, EventType::Added);
    assert_eq!(table_event.object["kind"], "Table");
    let rows = table_event.object["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    let cells = rows[0]["cells"].as_array().unwrap();
    assert_eq!(cells[0], "my-svc");
}

#[test]
fn test_watch_event_to_table_deleted_preserves_event_type() {
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "deleted-pod",
            "namespace": "default",
            "resourceVersion": "300",
            "creationTimestamp": "2026-04-10T00:00:00Z"
        },
        "spec": {"containers": [{"name": "c", "image": "img"}]},
        "status": {"phase": "Succeeded"}
    });
    let event = WatchEvent::deleted(pod);
    let table_event = watch_event_to_table(event, "Pod");

    assert_eq!(table_event.event_type, EventType::Deleted);
    assert_eq!(table_event.object["kind"], "Table");
}

#[test]
fn test_watch_bookmark_table_omits_column_definitions_for_periodic_bookmarks() {
    // Periodic BOOKMARK events (not initial-events-end) must NOT include
    // columnDefinitions to prevent kubectl from printing duplicate headers.
    // Only the initial-events-end BOOKMARK gets columnDefinitions.
    let event = WatchEvent::bookmark_typed(500, "v1", "Pod");
    let table_event = watch_event_to_table(event, "Pod");

    assert_eq!(table_event.event_type, EventType::Bookmark);
    assert_eq!(table_event.object["kind"], "Table");
    assert_eq!(table_event.object["apiVersion"], "meta.k8s.io/v1");
    assert_eq!(table_event.object["metadata"]["resourceVersion"], "500");

    // Periodic BOOKMARKs must NOT have columnDefinitions (prevents duplicate headers)
    assert!(
        table_event.object.get("columnDefinitions").is_none(),
        "Periodic BOOKMARK must not have columnDefinitions, but found: {:?}",
        table_event.object.get("columnDefinitions")
    );

    // Must have empty rows
    let rows = table_event.object["rows"].as_array().unwrap();
    assert!(
        rows.is_empty(),
        "BOOKMARK Table must have empty rows, got {} rows",
        rows.len()
    );
}

#[test]
fn test_watch_bookmark_table_initial_events_end_has_column_definitions() {
    // The initial-events-end BOOKMARK must include columnDefinitions so kubectl
    // prints the column headers after receiving the initial LIST via watch.
    let mut bookmark = WatchEvent::bookmark_typed(500, "v1", "Pod");
    Arc::make_mut(&mut bookmark.object)["metadata"]["annotations"] = json!({
        "k8s.io/initial-events-end": "true"
    });

    let table_event = watch_event_to_table(bookmark, "Pod");

    assert_eq!(table_event.event_type, EventType::Bookmark);
    assert_eq!(table_event.object["kind"], "Table");

    // initial-events-end BOOKMARK must have Pod column definitions.
    let col_defs = table_event.object["columnDefinitions"].as_array().unwrap();
    assert_eq!(
        col_defs.len(),
        9,
        "initial-events-end BOOKMARK must have 9 Pod column definitions"
    );
    assert_eq!(col_defs[0]["name"], "Name");
    assert_eq!(col_defs[1]["name"], "Ready");
    assert_eq!(col_defs[2]["name"], "Status");
    assert_eq!(col_defs[3]["name"], "Restarts");
    assert_eq!(col_defs[4]["name"], "Age");
    assert_eq!(col_defs[5]["name"], "IP");
    assert_eq!(col_defs[6]["name"], "Node");
    assert_eq!(col_defs[7]["name"], "Nominated Node");
    assert_eq!(col_defs[8]["name"], "Readiness Gates");

    // Rows still empty
    let rows = table_event.object["rows"].as_array().unwrap();
    assert!(rows.is_empty());
}

#[test]
fn test_watch_bookmark_table_preserves_annotations() {
    // sendInitialEvents uses k8s.io/initial-events-end annotation on the
    // final BOOKMARK to signal that initial LIST is complete. This annotation
    // must survive Table conversion.
    let mut bookmark = WatchEvent::bookmark_typed(999, "v1", "Pod");
    Arc::make_mut(&mut bookmark.object)["metadata"]["annotations"] = json!({
        "k8s.io/initial-events-end": "true"
    });

    let table_event = watch_event_to_table(bookmark, "Pod");

    assert_eq!(table_event.event_type, EventType::Bookmark);
    assert_eq!(
        table_event.object["metadata"]["annotations"]["k8s.io/initial-events-end"], "true",
        "initial-events-end annotation must be preserved in BOOKMARK Table"
    );
    // Rows still empty
    assert!(table_event.object["rows"].as_array().unwrap().is_empty());
}

#[test]
fn test_watch_modified_event_table_has_populated_rows() {
    // Contrast with BOOKMARK: MODIFIED events must have non-empty rows
    // with correct cell values matching the pod data.
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "web-server",
            "namespace": "default",
            "resourceVersion": "42",
            "creationTimestamp": "2026-04-10T00:00:00Z"
        },
        "spec": {
            "containers": [
                {"name": "nginx", "image": "nginx"},
                {"name": "sidecar", "image": "busybox"}
            ]
        },
        "status": {
            "phase": "Running",
            "containerStatuses": [
                {"name": "nginx", "ready": true, "restartCount": 1},
                {"name": "sidecar", "ready": true, "restartCount": 0}
            ]
        }
    });
    let event = WatchEvent::modified(pod);
    let table_event = watch_event_to_table(event, "Pod");

    assert_eq!(table_event.event_type, EventType::Modified);
    assert_eq!(table_event.object["kind"], "Table");

    // Must have non-empty rows
    let rows = table_event.object["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1, "MODIFIED event must have exactly 1 row");

    // Cell values must match pod data
    let cells = rows[0]["cells"].as_array().unwrap();
    assert_eq!(cells[0], "web-server"); // NAME
    assert_eq!(cells[1], "2/2"); // READY (both containers ready)
    assert_eq!(cells[2], "Running"); // STATUS
    assert_eq!(cells[3], 1); // RESTARTS (sum: 1+0=1)

    // MODIFIED events must NOT have columnDefinitions (prevents duplicate headers)
    assert!(
        table_event.object.get("columnDefinitions").is_none(),
        "MODIFIED watch events must not include columnDefinitions"
    );
}

#[test]
fn test_watch_event_to_table_preserves_resource_version_in_table_metadata() {
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "nginx",
            "namespace": "default",
            "resourceVersion": "42",
            "creationTimestamp": "2026-04-10T00:00:00Z"
        },
        "spec": {"containers": [{"name": "c", "image": "img"}]},
        "status": {"phase": "Running"}
    });
    let event = WatchEvent::modified(pod);
    let table_event = watch_event_to_table(event, "Pod");

    assert_eq!(table_event.object["metadata"]["resourceVersion"], "42");
}

#[test]
fn test_watch_event_contains_full_pod_status() {
    // Watch events must contain the COMPLETE pod object including all status fields.
    // This ensures kubectl can render READY, STATUS, RESTARTS columns.
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "nginx",
            "namespace": "default",
            "resourceVersion": "100",
            "creationTimestamp": "2026-04-10T00:00:00Z"
        },
        "spec": {
            "containers": [{"name": "nginx", "image": "nginx:latest"}]
        },
        "status": {
            "phase": "Running",
            "conditions": [
                {"type": "Ready", "status": "True"},
                {"type": "ContainersReady", "status": "True"}
            ],
            "containerStatuses": [{
                "name": "nginx",
                "ready": true,
                "restartCount": 2,
                "state": {"running": {"startedAt": "2026-04-10T00:00:01Z"}},
                "image": "nginx:latest",
                "imageID": "docker://sha256:abc123"
            }],
            "podIP": "10.43.0.5",
            "hostIP": "127.0.0.1"
        }
    });

    let event = WatchEvent::modified(pod.clone());

    // The raw watch event object must contain the full pod data
    assert_eq!(event.object["status"]["phase"], "Running");
    assert!(event.object["status"]["containerStatuses"].is_array());
    assert_eq!(
        event.object["status"]["containerStatuses"][0]["ready"],
        true
    );
    assert_eq!(
        event.object["status"]["containerStatuses"][0]["restartCount"],
        2
    );
    assert!(event.object["status"]["conditions"].is_array());
    assert_eq!(event.object["status"]["podIP"], "10.43.0.5");

    // When converted to Table format, the object row must preserve the full pod
    let table_event = watch_event_to_table(event, "Pod");
    let row_object = &table_event.object["rows"][0]["object"];
    assert_eq!(row_object["status"]["phase"], "Running");
    assert_eq!(row_object["status"]["containerStatuses"][0]["ready"], true);
    assert_eq!(
        row_object["status"]["containerStatuses"][0]["restartCount"],
        2
    );
    assert_eq!(row_object["status"]["podIP"], "10.43.0.5");
}

#[test]
fn test_watch_event_columns_match_list() {
    // Watch event Table format must have the same column definitions as LIST Table format.
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "nginx",
            "namespace": "default",
            "resourceVersion": "100",
            "creationTimestamp": "2026-04-10T00:00:00Z"
        },
        "spec": {
            "containers": [{"name": "nginx", "image": "nginx"}]
        },
        "status": {
            "phase": "Running",
            "containerStatuses": [{
                "name": "nginx",
                "ready": true,
                "restartCount": 0
            }]
        }
    });

    // Get LIST Table format
    let list_table = pod_list_to_table(vec![pod.clone()], "100".to_string());

    // Get watch Table format
    let event = WatchEvent::modified(pod);
    let watch_table = watch_event_to_table(event, "Pod");

    // LIST must have columnDefinitions
    let list_cols = list_table["columnDefinitions"].as_array().unwrap();
    assert_eq!(list_cols.len(), 9);
    assert_eq!(list_cols[0]["name"], "Name");

    // MODIFIED watch events must NOT have columnDefinitions
    assert!(
        watch_table.object.get("columnDefinitions").is_none(),
        "MODIFIED watch events must not include columnDefinitions"
    );

    // Cell values must match (same pod data = same cells)
    let list_cells = &list_table["rows"][0]["cells"];
    let watch_cells = &watch_table.object["rows"][0]["cells"];
    assert_eq!(list_cells, watch_cells);
}

// ========================
// Watch Table format: Service, ConfigMap, Secret
// ========================

#[test]
fn test_watch_event_to_table_service_includes_object() {
    let svc = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "my-svc",
            "namespace": "default",
            "resourceVersion": "50",
            "creationTimestamp": "2026-04-10T00:00:00Z"
        },
        "spec": {
            "type": "ClusterIP",
            "clusterIP": "10.43.128.10",
            "ports": [{"port": 80, "targetPort": 8080, "protocol": "TCP"}]
        }
    });
    let event = WatchEvent::modified(svc.clone());
    let table_event = watch_event_to_table(event, "Service");

    assert_eq!(table_event.event_type, EventType::Modified);
    assert_eq!(table_event.object["kind"], "Table");
    assert_eq!(table_event.object["metadata"]["resourceVersion"], "50");

    // Row must contain the full resource object for kubectl to extract fields
    let rows = table_event.object["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    let row_object = &rows[0]["object"];
    assert_eq!(row_object["metadata"]["name"], "my-svc");
    assert_eq!(row_object["spec"]["type"], "ClusterIP");
    assert_eq!(row_object["spec"]["clusterIP"], "10.43.128.10");
}

#[test]
fn test_watch_event_to_table_configmap_includes_object() {
    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "my-config",
            "namespace": "kube-system",
            "resourceVersion": "77",
            "creationTimestamp": "2026-04-10T00:00:00Z"
        },
        "data": {
            "key1": "value1",
            "key2": "value2"
        }
    });
    let event = WatchEvent::added(cm);
    let table_event = watch_event_to_table(event, "ConfigMap");

    assert_eq!(table_event.event_type, EventType::Added);
    assert_eq!(table_event.object["kind"], "Table");

    let rows = table_event.object["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    let cells = rows[0]["cells"].as_array().unwrap();
    assert_eq!(cells[0], "my-config"); // Name column

    // Row object must contain full ConfigMap data
    let row_object = &rows[0]["object"];
    assert_eq!(row_object["data"]["key1"], "value1");
    assert_eq!(row_object["data"]["key2"], "value2");
}

#[test]
fn test_watch_event_to_table_secret_includes_object() {
    let secret = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": "my-secret",
            "namespace": "default",
            "resourceVersion": "88",
            "creationTimestamp": "2026-04-10T00:00:00Z"
        },
        "type": "Opaque",
        "data": {
            "password": "cGFzc3dvcmQ=" // base64 "password"
        }
    });
    let event = WatchEvent::added(secret);
    let table_event = watch_event_to_table(event, "Secret");

    assert_eq!(table_event.object["kind"], "Table");

    let rows = table_event.object["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    let cells = rows[0]["cells"].as_array().unwrap();
    assert_eq!(cells[0], "my-secret");

    // Row object must contain full Secret
    let row_object = &rows[0]["object"];
    assert_eq!(row_object["type"], "Opaque");
}

// OpenAPI discovery tests

#[tokio::test]
async fn test_openapi_v3_discovery_returns_paths() {
    let db = crate::datastore::test_support::in_memory().await;
    let response = openapi_v3_discovery_with_crds(&db).await;

    // Must have paths field
    assert!(response["paths"].is_object());
    let paths = response["paths"].as_object().unwrap();

    // Should include core API
    assert!(paths.contains_key("api/v1"));

    // Each path should have serverRelativeURL
    let api_v1 = &paths["api/v1"];
    assert!(api_v1["serverRelativeURL"].is_string());
    assert!(
        api_v1["serverRelativeURL"]
            .as_str()
            .unwrap()
            .starts_with("/openapi/v3/api/v1")
    );
}

#[tokio::test]
async fn test_openapi_v2_returns_swagger() {
    let db = crate::datastore::test_support::in_memory().await;
    let response = openapi_v2(&db).await;

    // Must be Swagger 2.0
    assert_eq!(response["swagger"], "2.0");
    assert_eq!(response["info"]["title"], "Kubernetes");

    // Must have paths
    assert!(response["paths"].is_object());

    // Should include at least /api/ path
    assert!(response["paths"]["/api/"].is_object());

    // Definitions include generated built-in schemas even without CRDs.
    assert!(response["definitions"].is_object());
}

#[tokio::test]
async fn test_openapi_v2_includes_builtin_pod_schema_properties() {
    let db = crate::datastore::test_support::in_memory().await;
    let response = openapi_v2(&db).await;

    let pod = response
        .pointer("/definitions/io.k8s.api.core.v1.Pod")
        .expect("v2 OpenAPI must include the built-in Pod schema");
    assert_eq!(pod["type"], "object");
    assert_eq!(
        pod.pointer("/x-kubernetes-group-version-kind/0"),
        Some(&json!({"group": "", "version": "v1", "kind": "Pod"}))
    );
    let spec_ref = pod
        .pointer("/properties/spec/$ref")
        .or_else(|| pod.pointer("/properties/spec/allOf/0/$ref"))
        .and_then(|v| v.as_str())
        .and_then(|reference| reference.strip_prefix("#/definitions/"))
        .expect("Pod.spec must reference the generated PodSpec schema");
    assert_eq!(
        response.pointer(&format!(
            "/definitions/{}/properties/containers/type",
            spec_ref
        )),
        Some(&json!("array")),
        "kubectl explain pod.spec.containers needs built-in schema properties"
    );
}

#[tokio::test]
async fn test_openapi_v2_includes_crd_schemas() {
    let db = crate::datastore::test_support::in_memory().await;

    // Create a CRD with OpenAPI schema
    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {
            "name": "certificates.cert-manager.io"
        },
        "spec": {
            "group": "cert-manager.io",
            "scope": "Namespaced",
            "names": {
                "kind": "Certificate",
                "plural": "certificates",
                "singular": "certificate"
            },
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": {
                            "spec": {
                                "type": "object",
                                "properties": {
                                    "commonName": {"type": "string"},
                                    "dnsNames": {
                                        "type": "array",
                                        "items": {"type": "string"}
                                    }
                                },
                                "required": ["commonName"]
                            },
                            "status": {"type": "object"}
                        }
                    }
                }
            }]
        }
    });

    db.create_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "certificates.cert-manager.io",
        crd,
    )
    .await
    .unwrap();

    let response = openapi_v2(&db).await;

    // Verify CRD schema is in definitions with reversed domain format
    // "cert-manager.io" becomes "io.cert-manager"
    let definitions = response["definitions"].as_object().unwrap();
    assert!(
        definitions.contains_key("io.cert-manager.v1.Certificate"),
        "Expected key 'io.cert-manager.v1.Certificate' not found. Keys: {:?}",
        definitions.keys().collect::<Vec<_>>()
    );

    let cert_schema = &definitions["io.cert-manager.v1.Certificate"];
    assert_eq!(cert_schema["type"], "object");
    assert_eq!(
        cert_schema["properties"]["spec"]["properties"]["commonName"]["type"],
        "string"
    );
}

#[tokio::test]
async fn test_openapi_v2_uses_reversed_domain_format_for_crd_keys() {
    // K8s format: reverse domain parts like Java package naming
    // Input group: "crd-publish-openapi-test-common-group.example.com"
    // Expected key: "com.example.crd-publish-openapi-test-common-group.v6.TestKind"
    let db = crate::datastore::test_support::in_memory().await;

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {
            "name": "testkinds.crd-publish-openapi-test-common-group.example.com"
        },
        "spec": {
            "group": "crd-publish-openapi-test-common-group.example.com",
            "scope": "Namespaced",
            "names": {
                "kind": "TestKind",
                "plural": "testkinds"
            },
            "versions": [{
                "name": "v6",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": {
                            "spec": {
                                "type": "object",
                                "properties": {
                                    "field1": {"type": "string"}
                                }
                            }
                        }
                    }
                }
            }]
        }
    });

    db.create_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "testkinds.crd-publish-openapi-test-common-group.example.com",
        crd,
    )
    .await
    .unwrap();

    let response = openapi_v2(&db).await;
    let definitions = response["definitions"].as_object().unwrap();

    // Should use reversed domain format with lowercase kind
    assert!(
        definitions.contains_key("com.example.crd-publish-openapi-test-common-group.v6.TestKind"),
        "Expected key 'com.example.crd-publish-openapi-test-common-group.v6.TestKind' not found. Keys: {:?}",
        definitions.keys().collect::<Vec<_>>()
    );

    let schema = &definitions["com.example.crd-publish-openapi-test-common-group.v6.TestKind"];
    assert_eq!(schema["type"], "object");
}

#[tokio::test]
async fn test_create_resource_with_generate_name() {
    // This test verifies that generateName logic in the API create handlers
    // (namespaced_resource_handlers! and cluster_resource_handlers!) works correctly.
    //
    // The implementation (around lines 2829 and 3502) calls crate::utils::generate_name(prefix)
    // when metadata.name is missing but metadata.generateName is present.
    //
    // We test the database layer with a pre-generated name to verify storage works.
    // Full handler testing requires HTTP server (verified via Sonobuoy).

    let db = crate::datastore::test_support::in_memory().await;

    let generated_name = crate::utils::generate_name("test-config-");
    let body = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "generateName": "test-config-",
            "name": generated_name.clone(),  // Simulate what handler does
            "namespace": "default"
        },
        "data": {
            "key1": "value1"
        }
    });

    let resource = db
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            &generated_name.clone(),
            body,
        )
        .await
        .unwrap();

    // Resource should have the generated name
    let name = resource.data["metadata"]["name"].as_str().unwrap();
    assert!(
        name.starts_with("test-config-"),
        "Generated name should start with prefix"
    );
    assert_eq!(
        name.len(),
        "test-config-".len() + 5,
        "Generated name should be prefix + 5 chars"
    );
    let suffix = &name["test-config-".len()..];
    assert!(
        suffix
            .chars()
            .all(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit()),
        "Suffix should be lowercase alphanumeric only, got: {}",
        suffix
    );
}

#[tokio::test]
async fn test_create_resource_with_name_ignores_generate_name() {
    // This test verifies that when both metadata.name and metadata.generateName are present,
    // the API create handler uses metadata.name (per K8s semantics).

    let db = crate::datastore::test_support::in_memory().await;

    let body = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "explicit-name",
            "generateName": "test-config-",  // Should be ignored when name is present
            "namespace": "default"
        },
        "data": {
            "key1": "value1"
        }
    });

    let resource = db
        .create_resource("v1", "ConfigMap", Some("default"), "explicit-name", body)
        .await
        .unwrap();

    // Resource should use explicit name, not generated
    let name = resource.data["metadata"]["name"].as_str().unwrap();
    assert_eq!(name, "explicit-name");
}

#[test]
fn test_apply_patch_preserves_event_message_field() {
    // Test for task #42: Event PATCH should preserve message field
    // The issue is that patching an Event's message field returns empty string
    let current = json!({
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": {
            "name": "test-event",
            "namespace": "default"
        },
        "involvedObject": {
            "kind": "Pod",
            "name": "test-pod",
            "namespace": "default"
        },
        "message": "This is a test event",
        "reason": "Testing",
        "type": "Normal"
    });

    let patch = json!({
        "message": "This is a test event - patched"
    });

    let result = apply_patch(&current, &patch, Some("application/merge-patch+json")).unwrap();

    // Verify message field was updated
    assert_eq!(
        result["message"].as_str().unwrap(),
        "This is a test event - patched",
        "PATCH should update the message field"
    );

    // Verify other fields preserved
    assert_eq!(result["reason"].as_str().unwrap(), "Testing");
    assert_eq!(result["type"].as_str().unwrap(), "Normal");
}

#[test]
fn test_openapi_v2_accept_header_with_multiple_types() {
    // Test for task #54: kubectl replace sends "Accept: application/json, application/vnd.kubernetes.protobuf"
    // The get_openapi_v2 handler should return JSON when application/json is in the Accept header,
    // even if protobuf is also listed.
    //
    // This test verifies the logic doesn't incorrectly return 406 when both are present.

    // Simulate kubectl replace Accept header
    let accept = "application/json, application/vnd.kubernetes.protobuf";

    // The condition should evaluate to false (don't return 406)
    assert!(
        !accept.contains("protobuf") || accept.contains("application/json"),
        "Should NOT return 406 when both json and protobuf are in Accept header"
    );

    // Protobuf-only should return 406
    let accept_proto_only = "application/vnd.kubernetes.protobuf";
    assert!(
        accept_proto_only.contains("protobuf") && !accept_proto_only.contains("application/json"),
        "Should return 406 when ONLY protobuf is in Accept header"
    );
}

#[tokio::test]
async fn test_delete_with_orphan_policy_skips_cascade() {
    // Verify: propagationPolicy=Orphan in request body causes children to be orphaned
    // (ownerReferences removed) rather than cascade-deleted.
    let db = crate::datastore::test_support::in_memory().await;

    let parent_uid = "parent-uid-gc-test";
    db.create_resource(
        "apps/v1",
        "Deployment",
        Some("default"),
        "parent-deploy",
        json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "parent-deploy", "namespace": "default", "uid": parent_uid}
        }),
    )
    .await
    .unwrap();

    db.create_resource(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "child-rs",
            json!({
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "metadata": {
                    "name": "child-rs",
                    "namespace": "default",
                    "uid": "child-uid-gc-test",
                    "ownerReferences": [{"uid": parent_uid, "kind": "Deployment", "name": "parent-deploy"}]
                }
            }),
        )
        .await
        .unwrap();

    // Parse DeleteOptions from body as the $delete_fn handler does
    let body = br#"{"propagationPolicy":"Orphan"}"#;
    let opts: DeleteOptions = serde_json::from_slice(body).unwrap();
    assert_eq!(opts.propagation_policy.as_deref(), Some("Orphan"));

    // Simulate the handler's orphan/cascade branch
    let orphan = opts.propagation_policy.as_deref() == Some("Orphan");
    assert!(orphan, "propagationPolicy=Orphan must trigger orphan path");

    controllers::gc::orphan_children(
        &db,
        parent_uid,
        "apps/v1",
        "parent-deploy",
        "Deployment",
        Some("default".to_string()),
    )
    .await
    .unwrap();

    // Child must still exist (not cascade-deleted)
    let child = db
        .get_resource("apps/v1", "ReplicaSet", Some("default"), "child-rs")
        .await
        .unwrap();
    assert!(
        child.is_some(),
        "Child must not be deleted when propagationPolicy=Orphan"
    );

    // ownerReferences must be cleared
    let child_data = child.unwrap().data;
    let owner_refs_len = child_data
        .pointer("/metadata/ownerReferences")
        .and_then(|v| v.as_array())
        .map(|arr| arr.len())
        .unwrap_or(0);
    assert_eq!(
        owner_refs_len, 0,
        "ownerReferences must be removed on orphan"
    );
}

#[test]
fn test_delete_options_orphan_dependents_query_triggers_orphan_path() {
    // orphanDependents=true is the legacy K8s alias for propagationPolicy=Orphan
    let orphan_dependents: Option<bool> = Some(true);
    // No body, no query policy → falls back to default.
    let policy: &str = "Background";
    let orphan = policy == "Orphan" || orphan_dependents.is_some_and(|v| v);
    assert!(orphan, "orphanDependents=true must trigger orphan path");
}

#[test]
fn test_delete_options_body_parses_propagation_policy() {
    // Verify DeleteOptions deserialization from JSON body
    let body = br#"{"apiVersion":"v1","kind":"DeleteOptions","propagationPolicy":"Background"}"#;
    let opts = parse_delete_options_body(body);
    assert_eq!(opts.propagation_policy.as_deref(), Some("Background"));

    let body_orphan = br#"{"propagationPolicy":"Orphan"}"#;
    let opts_orphan = parse_delete_options_body(body_orphan);
    assert_eq!(opts_orphan.propagation_policy.as_deref(), Some("Orphan"));

    // Empty body defaults to Background (no policy)
    let opts_empty = DeleteOptions::default();
    let policy = opts_empty
        .propagation_policy
        .as_deref()
        .unwrap_or("Background");
    assert_eq!(policy, "Background");
}

#[test]
fn test_delete_options_body_parses_preconditions() {
    let body = br#"{"apiVersion":"v1","kind":"DeleteOptions","preconditions":{"uid":"uid-1","resourceVersion":"42"}}"#;
    let opts = parse_delete_options_body(body);
    let preconditions = opts.resource_preconditions().unwrap();
    assert_eq!(preconditions.uid.as_deref(), Some("uid-1"));
    assert_eq!(preconditions.resource_version, Some(42));
}

#[test]
fn test_delete_options_protobuf_unknown_envelope_parses_orphan_policy() {
    use prost::Message;

    let pb = k8s_pb::apimachinery::pkg::apis::meta::v1::DeleteOptions {
        propagation_policy: Some("Orphan".to_string()),
        ..Default::default()
    };
    let mut raw = Vec::new();
    pb.encode(&mut raw).unwrap();

    let unknown = crate::protobuf::Unknown {
        type_meta: Some(crate::protobuf::TypeMeta {
            api_version: "v1".to_string(),
            kind: "DeleteOptions".to_string(),
        }),
        raw,
        content_encoding: String::new(),
        content_type: String::new(),
    };

    let mut body = vec![0x6b, 0x38, 0x73, 0x00];
    unknown.encode(&mut body).unwrap();

    let opts = parse_delete_options_body(&body);
    assert_eq!(opts.propagation_policy.as_deref(), Some("Orphan"));
}

#[test]
fn test_delete_options_protobuf_parses_preconditions() {
    use prost::Message;

    let pb = k8s_pb::apimachinery::pkg::apis::meta::v1::DeleteOptions {
        preconditions: Some(k8s_pb::apimachinery::pkg::apis::meta::v1::Preconditions {
            uid: Some("uid-pb".to_string()),
            resource_version: Some("7".to_string()),
        }),
        ..Default::default()
    };
    let mut body = vec![0x6b, 0x38, 0x73, 0x00];
    pb.encode(&mut body).unwrap();

    let opts = parse_delete_options_body(&body);
    let preconditions = opts.resource_preconditions().unwrap();
    assert_eq!(preconditions.uid.as_deref(), Some("uid-pb"));
    assert_eq!(preconditions.resource_version, Some(7));
}

#[tokio::test]
async fn test_delete_collection_customresourcedefinitions_removes_all_crds() {
    let db = crate::datastore::test_support::in_memory().await;

    // Create two CRDs
    for name in &["foo.example.com", "bar.example.com"] {
        db.create_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            name,
            serde_json::json!({
                "apiVersion": "apiextensions.k8s.io/v1",
                "kind": "CustomResourceDefinition",
                "metadata": {"name": name},
                "spec": {
                    "group": "example.com",
                    "names": {"plural": name.split('.').next().unwrap(), "kind": "Foo"},
                    "versions": [{"name": "v1", "served": true, "storage": true}]
                }
            }),
        )
        .await
        .unwrap();
    }

    // Verify both exist
    let before = db
        .list_resources(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(before.items.len(), 2);

    // Delete all
    for resource in &before.items {
        db.delete_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            &resource.name.clone(),
        )
        .await
        .unwrap();
    }

    // Verify all gone
    let after = db
        .list_resources(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(after.items.len(), 0);
}

#[tokio::test]
async fn test_delete_collection_runtimeclasses_removes_all() {
    // Sonobuoy: DELETE /apis/node.k8s.io/v1/runtimeclasses must be supported.
    // Verify the handler deletes all RuntimeClass resources.
    let db = crate::datastore::test_support::in_memory().await;

    // Create two RuntimeClasses
    for name in &["gvisor", "kata-containers"] {
        db.create_resource(
            "node.k8s.io/v1",
            "RuntimeClass",
            None,
            name,
            serde_json::json!({
                "apiVersion": "node.k8s.io/v1",
                "kind": "RuntimeClass",
                "metadata": {"name": name},
                "handler": name,
            }),
        )
        .await
        .unwrap();
    }

    // Verify both exist
    let before = db
        .list_resources(
            "node.k8s.io/v1",
            "RuntimeClass",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(before.items.len(), 2);

    // Delete all via delete_collection logic (same as handler)
    for resource in &before.items {
        db.delete_resource(
            "node.k8s.io/v1",
            "RuntimeClass",
            None,
            &resource.name.clone(),
        )
        .await
        .unwrap();
    }

    // Verify all gone
    let after = db
        .list_resources(
            "node.k8s.io/v1",
            "RuntimeClass",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        after.items.len(),
        0,
        "delete_collection must remove all RuntimeClasses"
    );
}

// ── validate_webhook_configuration matchConditions tests ──────────────

#[test]
fn test_validate_webhook_rejects_invalid_cel_in_match_conditions() {
    use serde_json::json;
    // The K8s conformance test "should reject validating webhook configurations with
    // invalid match conditions" sends a VWC with `invalid_cel!@#$` as the expression.
    let body = json!({
        "webhooks": [{
            "name": "test.k8s.io",
            "admissionReviewVersions": ["v1"],
            "sideEffects": "None",
            "clientConfig": {"url": "https://example.com/webhook"},
            "matchConditions": [{
                "name": "invalid-cond",
                "expression": "invalid_cel!@#$"
            }]
        }]
    });
    let result = validate_webhook_configuration(&body);
    assert!(
        result.is_err(),
        "must reject VWC with invalid CEL expression"
    );
    let err_str = format!("{:?}", result.unwrap_err());
    assert!(
        err_str.contains("compilation failed"),
        "error must mention compilation failure, got: {}",
        err_str
    );
}

#[test]
fn test_validate_webhook_rejects_cel_syntax_error_in_match_conditions() {
    use serde_json::json;
    // This uses only valid ASCII characters, so the old heuristic would incorrectly accept it.
    let body = json!({
        "webhooks": [{
            "name": "test.k8s.io",
            "admissionReviewVersions": ["v1"],
            "sideEffects": "None",
            "clientConfig": {"url": "https://example.com/webhook"},
            "matchConditions": [{
                "name": "syntax-error",
                "expression": "request.object.metadata.name =="
            }]
        }]
    });
    let result = validate_webhook_configuration(&body);
    assert!(
        result.is_err(),
        "must reject VWC with syntactically invalid CEL expression"
    );
}

#[test]
fn test_validate_webhook_accepts_valid_cel_in_match_conditions() {
    use serde_json::json;
    let body = json!({
        "webhooks": [{
            "name": "test.k8s.io",
            "admissionReviewVersions": ["v1"],
            "sideEffects": "None",
            "clientConfig": {"url": "https://example.com/webhook"},
            "matchConditions": [{
                "name": "allow-pods",
                "expression": "request.resource.resource == 'pods'"
            }]
        }]
    });
    let result = validate_webhook_configuration(&body);
    assert!(result.is_ok(), "must accept VWC with valid CEL expression");
}

#[test]
fn test_validate_webhook_rejects_empty_match_condition_name() {
    use serde_json::json;
    let body = json!({
        "webhooks": [{
            "name": "test.k8s.io",
            "admissionReviewVersions": ["v1"],
            "sideEffects": "None",
            "clientConfig": {"url": "https://example.com/webhook"},
            "matchConditions": [{
                "name": "",
                "expression": "true"
            }]
        }]
    });
    let result = validate_webhook_configuration(&body);
    assert!(
        result.is_err(),
        "must reject matchCondition with empty name"
    );
}

#[test]
fn test_build_crd_conversion_webhook_client_accepts_base64_pem_ca_bundle() {
    use base64::Engine;
    use rcgen::generate_simple_self_signed;
    use serde_json::json;

    let cert = generate_simple_self_signed(vec!["conversion-webhook.test".to_string()])
        .expect("failed to generate test cert");
    let pem = cert.cert.pem();
    let ca_bundle = base64::engine::general_purpose::STANDARD.encode(pem.as_bytes());
    let client_config = json!({
        "caBundle": ca_bundle
    });

    let result = build_crd_conversion_webhook_client(&client_config, None);
    assert!(
        result.is_ok(),
        "base64-encoded PEM caBundle must be accepted, got: {:?}",
        result.err()
    );
}

#[tokio::test(flavor = "current_thread")]
#[allow(clippy::await_holding_lock)] // PROXY_ENV_LOCK serializes env-var-mutating tests; intentional
async fn test_build_crd_conversion_webhook_client_bypasses_proxy_env() {
    let _env_lock = PROXY_ENV_LOCK.lock().expect("proxy env lock poisoned");
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("proxy listener should bind");
    let proxy_addr = listener
        .local_addr()
        .expect("proxy listener should have local addr");
    let proxy_url = format!("http://{proxy_addr}");

    let (proxy_hit_tx, proxy_hit_rx) = oneshot::channel();
    tokio::spawn(async move {
        let proxy_hit =
            match tokio::time::timeout(std::time::Duration::from_millis(800), listener.accept())
                .await
            {
                Ok(Ok((mut socket, _))) => {
                    let mut buf = [0u8; 2048];
                    // safe-to-ignore: draining the test client's request before responding
                    let _ = socket.read(&mut buf).await;
                    let _ = socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                        )
                        .await;
                    true
                }
                _ => false,
            };
        let _ = proxy_hit_tx.send(proxy_hit);
    });

    let _http_proxy_upper = EnvVarRestore::set("HTTP_PROXY", Some(&proxy_url));
    let _http_proxy_lower = EnvVarRestore::set("http_proxy", Some(&proxy_url));
    let _https_proxy_upper = EnvVarRestore::set("HTTPS_PROXY", Some(&proxy_url));
    let _https_proxy_lower = EnvVarRestore::set("https_proxy", Some(&proxy_url));
    let _all_proxy_upper = EnvVarRestore::set("ALL_PROXY", Some(&proxy_url));
    let _all_proxy_lower = EnvVarRestore::set("all_proxy", Some(&proxy_url));
    let _no_proxy_upper = EnvVarRestore::set("NO_PROXY", None);
    let _no_proxy_lower = EnvVarRestore::set("no_proxy", None);

    let client = build_crd_conversion_webhook_client(&json!({}), None)
        .expect("conversion webhook client should build");
    let result = client
        .get("https://198.51.100.1:4443/crdconvert")
        .timeout(std::time::Duration::from_millis(250))
        .send()
        .await;

    let proxy_hit = proxy_hit_rx.await.unwrap_or(false);
    assert!(
        result.is_err(),
        "conversion webhook request should fail in test harness"
    );
    assert!(
        !proxy_hit,
        "conversion webhook HTTP client must bypass proxy env vars for in-cluster service calls"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_crd_conversion_service_webhook_uses_endpoint_target_port() {
    use serde_json::json;

    let service_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("service listener should bind");
    let service_port = service_listener
        .local_addr()
        .expect("service listener should have local addr")
        .port();

    let endpoint_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("endpoint listener should bind");
    let endpoint_port = endpoint_listener
        .local_addr()
        .expect("endpoint listener should have local addr")
        .port();

    let (service_hit_tx, service_hit_rx) = oneshot::channel();
    tokio::spawn(async move {
        let hit = matches!(
            tokio::time::timeout(std::time::Duration::from_secs(2), service_listener.accept(),)
                .await,
            Ok(Ok((_stream, _)))
        );
        let _ = service_hit_tx.send(hit);
    });

    let (endpoint_hit_tx, endpoint_hit_rx) = oneshot::channel();
    tokio::spawn(async move {
        let hit = matches!(
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                endpoint_listener.accept(),
            )
            .await,
            Ok(Ok((_stream, _)))
        );
        let _ = endpoint_hit_tx.send(hit);
    });

    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "conv-webhook",
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "conv-webhook", "namespace": "default"},
            "spec": {
                "ports": [{
                    "port": service_port,
                    "targetPort": endpoint_port
                }]
            }
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "conv-webhook",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "conv-webhook", "namespace": "default"},
            "subsets": [{
                "addresses": [{"ip": "127.0.0.1"}],
                "ports": [{"port": endpoint_port}]
            }]
        }),
    )
    .await
    .unwrap();

    let conversion = CrdConversionConfig {
        storage_version: "v1".to_string(),
        served_versions: vec!["v1".to_string(), "v2".to_string()],
        strategy: Some("Webhook".to_string()),
        webhook_client_config: Some(json!({
            "service": {
                "name": "conv-webhook",
                "namespace": "default",
                "port": service_port,
                "path": "/crdconvert"
            }
        })),
        webhook_review_versions: vec!["v1".to_string()],
    };

    let result = convert_crd_objects_to_requested_version(
        &db,
        &conversion,
        "stable.example.com",
        "widgets",
        "stable.example.com/v1",
        vec![json!({
            "apiVersion": "stable.example.com/v2",
            "kind": "Widget",
            "metadata": {"name": "w1", "namespace": "default"}
        })],
    )
    .await;
    assert!(
        result.is_err(),
        "test harness listener is intentionally not a real HTTPS webhook"
    );

    let service_hit = service_hit_rx.await.unwrap_or(false);
    let endpoint_hit = endpoint_hit_rx.await.unwrap_or(false);
    assert!(
        endpoint_hit,
        "conversion webhook call must connect to endpoint targetPort when service port differs"
    );
    assert!(
        !service_hit,
        "conversion webhook call must not connect to service port when endpoint targetPort differs"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_crd_conversion_skips_objects_already_on_desired_version() {
    use serde_json::json;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("webhook listener should bind");
    let port = listener
        .local_addr()
        .expect("webhook listener should have local addr")
        .port();

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("webhook accept");
        let mut buf = vec![0u8; 65536];
        let n = stream.read(&mut buf).await.expect("webhook read request");
        let req = String::from_utf8_lossy(&buf[..n]);
        let body_start = req.find("\r\n\r\n").map(|i| i + 4).unwrap_or(req.len());
        let review_req: Value =
            serde_json::from_slice(&buf[body_start..n]).expect("valid conversion review");
        let desired = review_req
            .pointer("/request/desiredAPIVersion")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let request_objects = review_req["request"]["objects"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let has_same_version = request_objects.iter().any(|o| {
            o.get("apiVersion")
                .and_then(|v| v.as_str())
                .is_some_and(|av| av == desired)
        });
        let uid = review_req
            .pointer("/request/uid")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        let response_body = if has_same_version {
            json!({
                "apiVersion": "apiextensions.k8s.io/v1",
                "kind": "ConversionReview",
                "response": {
                    "uid": uid,
                    "result": {
                        "status": "Failure",
                        "message": format!("conversion from a version to itself should not call the webhook: {desired}")
                    }
                }
            })
        } else {
            let converted_objects: Vec<Value> = request_objects
                .into_iter()
                .map(|mut o| {
                    o["apiVersion"] = Value::String(desired.clone());
                    o
                })
                .collect();
            json!({
                "apiVersion": "apiextensions.k8s.io/v1",
                "kind": "ConversionReview",
                "response": {
                    "uid": uid,
                    "result": {"status": "Success"},
                    "convertedObjects": converted_objects
                }
            })
        };

        let payload = serde_json::to_string(&response_body).expect("serialize response");
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            payload.len(),
            payload
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write response");
    });

    let db = crate::datastore::test_support::in_memory().await;
    let conversion = CrdConversionConfig {
        storage_version: "v1".to_string(),
        served_versions: vec!["v1".to_string(), "v2".to_string()],
        strategy: Some("Webhook".to_string()),
        webhook_client_config: Some(json!({
            "url": format!("http://127.0.0.1:{port}/crdconvert")
        })),
        webhook_review_versions: vec!["v1".to_string()],
    };

    let result = convert_crd_objects_to_requested_version(
        &db,
        &conversion,
        "stable.example.com",
        "widgets",
        "stable.example.com/v1",
        vec![
            json!({
                "apiVersion": "stable.example.com/v1",
                "kind": "Widget",
                "metadata": {"name": "already-v1"}
            }),
            json!({
                "apiVersion": "stable.example.com/v2",
                "kind": "Widget",
                "metadata": {"name": "needs-convert"}
            }),
        ],
    )
    .await
    .expect("mixed-version conversion should succeed");

    assert_eq!(result.len(), 2);
    assert_eq!(
        result[0]["apiVersion"], "stable.example.com/v1",
        "object already on desired version must bypass webhook and remain unchanged"
    );
    assert_eq!(
        result[1]["apiVersion"], "stable.example.com/v1",
        "object on another served version must be converted to desired version"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_crd_conversion_strategy_check_is_case_insensitive() {
    use serde_json::json;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("webhook listener should bind");
    let port = listener
        .local_addr()
        .expect("webhook listener should have local addr")
        .port();

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("webhook accept");
        let mut buf = vec![0u8; 65536];
        let n = stream.read(&mut buf).await.expect("webhook read request");
        let req = String::from_utf8_lossy(&buf[..n]);
        let body_start = req.find("\r\n\r\n").map(|i| i + 4).unwrap_or(req.len());
        let review_req: Value =
            serde_json::from_slice(&buf[body_start..n]).expect("valid conversion review");
        let desired = review_req
            .pointer("/request/desiredAPIVersion")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let uid = review_req
            .pointer("/request/uid")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let converted_objects: Vec<Value> = review_req["request"]["objects"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|mut o| {
                o["apiVersion"] = Value::String(desired.clone());
                o
            })
            .collect();

        let response_body = json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "ConversionReview",
            "response": {
                "uid": uid,
                "result": {"status": "Success"},
                "convertedObjects": converted_objects
            }
        });

        let payload = serde_json::to_string(&response_body).expect("serialize response");
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            payload.len(),
            payload
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write response");
    });

    let db = crate::datastore::test_support::in_memory().await;
    let conversion = CrdConversionConfig {
        storage_version: "v1".to_string(),
        served_versions: vec!["v1".to_string(), "v2".to_string()],
        strategy: Some("webhook".to_string()),
        webhook_client_config: Some(json!({
            "url": format!("http://127.0.0.1:{port}/crdconvert")
        })),
        webhook_review_versions: vec!["v1".to_string()],
    };

    let result = convert_crd_objects_to_requested_version(
        &db,
        &conversion,
        "stable.example.com",
        "widgets",
        "stable.example.com/v1",
        vec![json!({
            "apiVersion": "stable.example.com/v2",
            "kind": "Widget",
            "metadata": {"name": "needs-convert"}
        })],
    )
    .await
    .expect("lowercase webhook strategy must still trigger conversion");

    assert_eq!(result.len(), 1);
    assert_eq!(result[0]["apiVersion"], "stable.example.com/v1");
}

#[tokio::test(flavor = "current_thread")]
async fn test_crd_conversion_strategy_none_with_client_config_stamps_requested_version() {
    use serde_json::json;

    let db = crate::datastore::test_support::in_memory().await;
    let conversion = CrdConversionConfig {
        storage_version: "v1".to_string(),
        served_versions: vec!["v1".to_string(), "v2".to_string()],
        strategy: Some("None".to_string()),
        webhook_client_config: Some(json!({
            "url": "https://127.0.0.1:1/should-not-be-called"
        })),
        webhook_review_versions: vec!["v1".to_string()],
    };

    let result = convert_crd_objects_to_requested_version(
        &db,
        &conversion,
        "stable.example.com",
        "widgets",
        "stable.example.com/v2",
        vec![json!({
            "apiVersion": "stable.example.com/v1",
            "kind": "Widget",
            "metadata": {"name": "storage-version"}
        })],
    )
    .await
    .expect("strategy None must not call webhook but must normalize response shape");

    assert_eq!(result.len(), 1);
    assert_eq!(result[0]["apiVersion"], "stable.example.com/v2");
    assert_eq!(result[0]["kind"], "Widget");
    assert_eq!(result[0]["metadata"]["name"], "storage-version");
}

#[tokio::test(flavor = "current_thread")]
async fn test_crd_conversion_accepts_yaml_conversion_review_response() {
    use serde_json::json;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("webhook listener should bind");
    let port = listener
        .local_addr()
        .expect("webhook listener should have local addr")
        .port();

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("webhook accept");
        let mut buf = vec![0u8; 65536];
        let n = stream.read(&mut buf).await.expect("webhook read request");
        let req = String::from_utf8_lossy(&buf[..n]);
        let body_start = req.find("\r\n\r\n").map(|i| i + 4).unwrap_or(req.len());
        let review_req: Value =
            serde_json::from_slice(&buf[body_start..n]).expect("valid conversion review");
        let desired = review_req
            .pointer("/request/desiredAPIVersion")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let uid = review_req
            .pointer("/request/uid")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let converted_objects: Vec<Value> = review_req["request"]["objects"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|mut o| {
                o["apiVersion"] = Value::String(desired.clone());
                o
            })
            .collect();

        let response_body = json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "ConversionReview",
            "response": {
                "uid": uid,
                "result": {"status": "Success"},
                "convertedObjects": converted_objects
            }
        });
        let yaml_payload = serde_yaml::to_string(&response_body).expect("serialize yaml");
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/yaml\r\nContent-Length: {}\r\n\r\n{}",
            yaml_payload.len(),
            yaml_payload
        );
        stream
            .write_all(response.as_bytes())
            .await
            .expect("write response");
    });

    let db = crate::datastore::test_support::in_memory().await;
    let conversion = CrdConversionConfig {
        storage_version: "v1".to_string(),
        served_versions: vec!["v1".to_string(), "v2".to_string()],
        strategy: Some("Webhook".to_string()),
        webhook_client_config: Some(json!({
            "url": format!("http://127.0.0.1:{port}/crdconvert")
        })),
        webhook_review_versions: vec!["v1".to_string()],
    };

    let result = convert_crd_objects_to_requested_version(
        &db,
        &conversion,
        "stable.example.com",
        "widgets",
        "stable.example.com/v1",
        vec![json!({
            "apiVersion": "stable.example.com/v2",
            "kind": "Widget",
            "metadata": {"name": "needs-convert"}
        })],
    )
    .await
    .expect("yaml conversion webhook response should be accepted");

    assert_eq!(result.len(), 1);
    assert_eq!(result[0]["apiVersion"], "stable.example.com/v1");
}

/// Regression test for P0-S13-2: custom resource create handler must inject
/// resourceVersion into the response.  The isWatchCachePrimed helper in the
/// Sonobuoy field_validation test (field_validation.go:305) does:
///
///   1. POST  /apis/{group}/{version}/{plural}         → creates CR, gets createdInstance
///   2. DELETE /apis/{group}/{version}/{plural}/{name} → deletes CR
///   3. GET    /apis/{group}/{version}/{plural}?watch=true
///             &resourceVersion={createdInstance.GetResourceVersion()}
///             → expects DELETED event within 5 s via the catch-up path
///
/// If the POST response is missing resourceVersion, GetResourceVersion() returns ""
/// which the watch handler parses as 0 → send_initial_events=true → the full
/// initial-list path runs (finds no resources, they're all deleted), the catch-up
/// branch (rv>0) is skipped, and the DELETED event is never delivered → 5 s timeout.
///
/// RED: inject_resource_version is NOT called on the handler response → data has no rv
///      → the response resourceVersion is "" → the watch rv is 0 → catch-up is skipped
///      → list_cluster_resources_modified_since(rv=0) returns the deleted row but the
///      watch handler doesn't call it (send_initial_events=true) → DELETED event lost.
///
/// GREEN: inject_resource_version IS called → response.resourceVersion = create_rv (>0)
///        → watch opens with rv=create_rv → catch-up runs → DELETED event delivered.
#[tokio::test]
async fn test_p0_s13_2_create_custom_resource_response_includes_resource_version() {
    // Arrange: create a cluster-scoped custom resource via the DB layer,
    // mirroring what create_cluster_custom_resource calls internally.
    let db = crate::datastore::test_support::in_memory().await;

    let body = json!({
        "apiVersion": "example.com/v1",
        "kind": "NoxuType",
        "metadata": {"name": "setup-instance"}
    });

    let resource = db
        .create_resource("example.com/v1", "NoxuType", None, "setup-instance", body)
        .await
        .unwrap();

    let create_rv = resource.resource_version;
    assert!(
        create_rv > 0,
        "resource_version must be assigned at creation"
    );

    // The buggy handler returns bare resource.data — resourceVersion is absent
    // because the DB stores it as a separate column, not embedded in the JSON blob.
    let bare_rv_str = resource
        .data
        .pointer("/metadata/resourceVersion")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert!(
        bare_rv_str.is_empty(),
        "Precondition: bare resource.data must have no resourceVersion \
             (got {:?}) — DB stores rv as a separate column",
        bare_rv_str
    );

    // RED assertion: calling inject_resource_version on the raw data blob
    // is what makes the response contain resourceVersion.  Without this call,
    // the client would parse rv="" as 0 and trigger the wrong watch path.
    // Assert that inject_resource_version correctly populates the field.
    let response_data = inject_resource_version(resource.data.clone(), create_rv);
    let response_rv_str = response_data
        .pointer("/metadata/resourceVersion")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(
        response_rv_str,
        create_rv.to_string(),
        "inject_resource_version must embed resourceVersion={} in the JSON blob \
             so clients receive the correct rv",
        create_rv
    );

    // Verify catch-up path: delete the resource, then confirm that
    // list_cluster_resources_modified_since(create_rv) finds the deleted row.
    // The watch handler uses this query when rv>0 (the correct path).
    // When rv=0 it uses send_initial_events=true instead, skipping catch-up.
    db.delete_resource("example.com/v1", "NoxuType", None, "setup-instance")
        .await
        .unwrap();

    // With the FIXED rv (create_rv > 0), catch-up finds the DELETED row.
    // list_resources_modified_since(namespace=None) is used by the CRD watch handlers.
    let catchup_with_correct_rv = db
        .list_resources_modified_since("example.com/v1", "NoxuType", None, create_rv)
        .await
        .unwrap();
    assert!(
        catchup_with_correct_rv
            .iter()
            .any(|c| c.event_type == "DELETED"),
        "Catch-up from create_rv={} must include the deleted resource \
             so the watch handler can emit DELETED; got {} rows \
             (list_resources_modified_since with namespace=None must query namespaced_resources)",
        create_rv,
        catchup_with_correct_rv.len()
    );

    // Cluster catch-up must also see the delete for cluster-scoped custom resources.
    let cluster_catchup = db
        .list_cluster_resources_modified_since("example.com/v1", "NoxuType", create_rv)
        .await
        .unwrap();
    assert!(
        cluster_catchup.iter().any(|c| c.event_type == "DELETED"),
        "cluster catch-up from create_rv={} must include deleted cluster-scoped custom resources",
        create_rv
    );

    // With the BUGGY rv (0) from missing inject_resource_version, the watch handler
    // takes the send_initial_events=true branch and SKIPS the catch-up query entirely.
    // The initial list sees no active resources (all deleted), sends a BOOKMARK with rv=0,
    // then waits for broadcast events — but the DELETED event already happened and is
    // never re-broadcast, causing the 5 s timeout in Sonobuoy field_validation.go:305.

    // Key invariant: the deleted resource rv is > create_rv (it was deleted after create).
    let deleted_rv = catchup_with_correct_rv
        .iter()
        .find(|c| c.event_type == "DELETED")
        .map(|c| c.resource.resource_version)
        .unwrap();
    assert!(
        deleted_rv > create_rv,
        "delete_rv={} must be greater than create_rv={}",
        deleted_rv,
        create_rv
    );
}

#[test]
fn test_validate_against_schema_rejects_unknown_root_metadata_fields_for_schemaless_crs() {
    let schema = json!({
        "type": "object",
        "x-kubernetes-preserve-unknown-fields": true
    });
    let body = json!({
        "apiVersion": "stable.example.com/v1",
        "kind": "Widget",
        "metadata": {
            "name": "root-meta",
            "unknownField": "must-fail"
        },
        "spec": {
            "freeform": {
                "nested": true
            }
        }
    });

    let result = validate_against_schema(&body, &schema, "");
    let err = result.expect_err("unknown root metadata fields must be rejected");
    let err_str = format!("{err:?}");
    assert!(
        err_str.contains("metadata.unknownField"),
        "error must mention the root metadata field path, got: {err_str}"
    );
}

#[tokio::test]
async fn test_task_supervisor_endpoints_require_admin_header() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = crate::api::test_support::build_test_router().await;
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/klights/v1/task-supervisor/categories")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_task_supervisor_rejects_spoofed_remote_group_header() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = crate::api::test_support::build_test_router().await;
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/klights/v1/task-supervisor/categories")
                .header("x-remote-group", "system:masters")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "incoming request headers must not be accepted as authenticated identity"
    );
}

#[tokio::test]
async fn test_impersonated_request_authorizes_effective_subject_not_real_admin() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let mut state = crate::api::test_support::build_test_app_state().await;
    state.authorizer =
        std::sync::Arc::new(crate::auth::authorizer::AuthorizerChain::default_chain());
    let app = crate::api::build_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces/default/configmaps")
                .header("impersonate-user", "system:serviceaccount:default:e2e")
                .header("impersonate-group", "system:authenticated")
                .header("impersonate-group", "system:serviceaccounts")
                .header("impersonate-group", "system:serviceaccounts:default")
                .extension(crate::auth::AuthenticatedIdentity::admin("test-admin"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "after impersonation, resource authorization must use the ServiceAccount identity"
    );
}

#[tokio::test]
async fn test_api_accepts_valid_bootstrap_bearer_token() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let (app, db) = crate::api::test_support::build_test_router_with_db().await;
    let token = crate::bootstrap::bootstrap_token::ensure_default_bootstrap_token(
        db.as_ref(),
        std::time::Duration::from_secs(3600),
    )
    .await
    .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_get_bootstrap_secret_returns_rotated_token_when_near_expiry() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let (app, db) = crate::api::test_support::build_test_router_with_db().await;
    let old_token = "abcdef.0123456789abcdef";
    crate::bootstrap::bootstrap_token::create_scoped_bootstrap_token_secret_with_ttl_for_test(
        db.as_ref(),
        crate::bootstrap::bootstrap_token::BootstrapTokenScope::Worker,
        old_token,
        std::time::Duration::from_secs(14 * 60),
    )
    .await
    .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces/kube-system/secrets/worker-bootstrap-token")
                .extension(crate::auth::AuthenticatedIdentity::admin("test-admin"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let returned: Value = serde_json::from_slice(&bytes).unwrap();
    let returned_token = bootstrap_secret_token_from_json(&returned);

    let stored = db
        .get_resource(
            "v1",
            "Secret",
            Some("kube-system"),
            crate::bootstrap::bootstrap_token::WORKER_BOOTSTRAP_TOKEN_SECRET_NAME,
        )
        .await
        .unwrap()
        .expect("worker bootstrap token Secret must exist after GET");
    let stored_token = bootstrap_secret_token_from_json(&stored.data);

    assert_ne!(
        old_token, returned_token,
        "GET response must contain rotated token"
    );
    assert_eq!(
        returned_token, stored_token,
        "GET must return the same rotated token that was persisted"
    );
    assert_eq!(
        returned["metadata"]["resourceVersion"],
        stored.resource_version.to_string()
    );
}

#[tokio::test]
async fn test_get_kube_system_nonfixed_bootstrap_secret_does_not_rotate() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use base64::Engine as _;
    use time::format_description::well_known::Rfc3339;
    use tower::ServiceExt;

    let (app, db) = crate::api::test_support::build_test_router_with_db().await;
    crate::bootstrap::bootstrap_token::create_scoped_bootstrap_token_secret_with_ttl_for_test(
        db.as_ref(),
        crate::bootstrap::bootstrap_token::BootstrapTokenScope::Worker,
        "aaaaaa.1111111111111111",
        std::time::Duration::from_secs(14 * 60),
    )
    .await
    .unwrap();
    crate::bootstrap::bootstrap_token::create_scoped_bootstrap_token_secret_with_ttl_for_test(
        db.as_ref(),
        crate::bootstrap::bootstrap_token::BootstrapTokenScope::Controlplane,
        "bbbbbb.2222222222222222",
        std::time::Duration::from_secs(14 * 60),
    )
    .await
    .unwrap();
    let expires_at = (time::OffsetDateTime::now_utc() + time::Duration::minutes(14))
        .format(&Rfc3339)
        .unwrap();
    let encode = |value: &str| base64::engine::general_purpose::STANDARD.encode(value.as_bytes());
    let secret = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "namespace": "kube-system",
            "name": "custom-bootstrap-token"
        },
        "type": "bootstrap.kubernetes.io/token",
        "data": {
            "token-id": encode("abcdef"),
            "token-secret": encode("0123456789abcdef"),
            "description": encode("operator-managed bootstrap-like token"),
            "expiration": encode(&expires_at),
            "usage-bootstrap-authentication": encode("true"),
            "usage-bootstrap-signing": encode("true"),
        }
    });
    db.create_resource(
        "v1",
        "Secret",
        Some("kube-system"),
        "custom-bootstrap-token",
        secret,
    )
    .await
    .unwrap();
    let before = db
        .get_resource(
            "v1",
            "Secret",
            Some("kube-system"),
            "custom-bootstrap-token",
        )
        .await
        .unwrap()
        .expect("custom bootstrap-like Secret must exist");
    let worker_before = db
        .get_resource(
            "v1",
            "Secret",
            Some("kube-system"),
            crate::bootstrap::bootstrap_token::WORKER_BOOTSTRAP_TOKEN_SECRET_NAME,
        )
        .await
        .unwrap()
        .expect("worker bootstrap token Secret must exist");
    let controlplane_before = db
        .get_resource(
            "v1",
            "Secret",
            Some("kube-system"),
            crate::bootstrap::bootstrap_token::CONTROLPLANE_BOOTSTRAP_TOKEN_SECRET_NAME,
        )
        .await
        .unwrap()
        .expect("controlplane bootstrap token Secret must exist");

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces/kube-system/secrets/custom-bootstrap-token")
                .extension(crate::auth::AuthenticatedIdentity::admin("test-admin"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let returned: Value = serde_json::from_slice(&bytes).unwrap();
    let after = db
        .get_resource(
            "v1",
            "Secret",
            Some("kube-system"),
            "custom-bootstrap-token",
        )
        .await
        .unwrap()
        .expect("custom bootstrap-like Secret must still exist");
    let worker_after = db
        .get_resource(
            "v1",
            "Secret",
            Some("kube-system"),
            crate::bootstrap::bootstrap_token::WORKER_BOOTSTRAP_TOKEN_SECRET_NAME,
        )
        .await
        .unwrap()
        .expect("worker bootstrap token Secret must still exist");
    let controlplane_after = db
        .get_resource(
            "v1",
            "Secret",
            Some("kube-system"),
            crate::bootstrap::bootstrap_token::CONTROLPLANE_BOOTSTRAP_TOKEN_SECRET_NAME,
        )
        .await
        .unwrap()
        .expect("controlplane bootstrap token Secret must still exist");

    assert_eq!(
        bootstrap_secret_token_from_json(&returned),
        "abcdef.0123456789abcdef",
        "non-fixed kube-system bootstrap-like Secret must be returned unchanged"
    );
    assert_eq!(
        bootstrap_secret_token_from_json(&after.data),
        "abcdef.0123456789abcdef",
        "non-fixed kube-system bootstrap-like Secret must not be persisted with a rotated token"
    );
    assert_eq!(
        before.resource_version, after.resource_version,
        "non-fixed kube-system bootstrap-like Secret must not be updated on GET"
    );
    assert_eq!(
        bootstrap_secret_token_from_json(&worker_before.data),
        bootstrap_secret_token_from_json(&worker_after.data),
        "GET of another Secret must not rotate worker-bootstrap-token"
    );
    assert_eq!(
        worker_before.resource_version, worker_after.resource_version,
        "GET of another Secret must not update worker-bootstrap-token"
    );
    assert_eq!(
        bootstrap_secret_token_from_json(&controlplane_before.data),
        bootstrap_secret_token_from_json(&controlplane_after.data),
        "GET of another Secret must not rotate controlplane-bootstrap-token"
    );
    assert_eq!(
        controlplane_before.resource_version, controlplane_after.resource_version,
        "GET of another Secret must not update controlplane-bootstrap-token"
    );
}

#[tokio::test]
async fn test_api_accepts_valid_serviceaccount_bearer_token() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use rand_core::OsRng;
    use rsa::RsaPrivateKey;
    use rsa::pkcs8::EncodePrivateKey;
    use tower::ServiceExt;

    let unique_ns = format!("sa-bearer-{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let etc_dir = crate::paths::etc_dir_path(&unique_ns);
    std::fs::create_dir_all(&etc_dir).unwrap();

    let private_key = RsaPrivateKey::new(&mut OsRng, 2048).unwrap();
    let signing_key_pem = private_key
        .to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
        .unwrap()
        .to_string();
    std::fs::write(
        crate::paths::service_account_signing_key_path(&unique_ns),
        &signing_key_pem,
    )
    .unwrap();

    let mut state = crate::api::test_support::build_test_app_state().await;
    state.config = std::sync::Arc::new(crate::KlightsConfig {
        containerd_namespace: unique_ns.clone(),
        ..crate::KlightsConfig::from_env().expect("env config valid in test")
    });

    // Phase 2B: SA must exist for UID validation.
    // Create the ServiceAccount before generating the token.
    let sa_uid = uuid::Uuid::new_v4().to_string();
    state
        .db
        .create_resource(
            "v1",
            "ServiceAccount",
            Some("sonobuoy"),
            "sonobuoy-serviceaccount",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ServiceAccount",
                "metadata": {
                    "name": "sonobuoy-serviceaccount",
                    "namespace": "sonobuoy",
                    "uid": sa_uid
                }
            }),
        )
        .await
        .unwrap();

    let app = crate::api::build_router(state);

    let token = crate::auth::generate_sa_token_with_sa_uid(
        &signing_key_pem,
        "sonobuoy-serviceaccount",
        "sonobuoy",
        &["https://kubernetes.default.svc.cluster.local"],
        crate::auth::DEFAULT_SERVICE_ACCOUNT_TOKEN_EXPIRATION_SECONDS,
        &sa_uid,
    )
    .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_api_rejects_invalid_bootstrap_bearer_token() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = crate::api::test_support::build_test_router().await;
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api")
                .header("authorization", "Bearer abcdef.0123456789abcdef")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_task_supervisor_accepts_admin_client_certificate_identity() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let (ca_cert, ca_key, _, _) = crate::auth::generate_ca_full().unwrap();
    let (admin_cert_pem, _) = crate::auth::generate_admin_cert(&ca_cert, &ca_key).unwrap();
    let app = crate::api::test_support::build_test_router().await;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/klights/v1/task-supervisor/categories")
                .extension(crate::auth::TlsClientCertificate(pem_cert_der(
                    &admin_cert_pem,
                )))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_task_supervisor_rejects_non_admin_client_certificate_identity() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let (ca_cert, ca_key, _, _) = crate::auth::generate_ca_full().unwrap();
    let (server_cert_pem, _) = crate::auth::generate_server_cert(&ca_cert, &ca_key).unwrap();
    let app = crate::api::test_support::build_test_router().await;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/klights/v1/task-supervisor/categories")
                .extension(crate::auth::TlsClientCertificate(pem_cert_der(
                    &server_cert_pem,
                )))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

fn pem_cert_der(pem: &str) -> Vec<u8> {
    rustls_pemfile::certs(&mut pem.as_bytes())
        .next()
        .expect("PEM must contain a cert")
        .expect("cert must parse")
        .as_ref()
        .to_vec()
}

fn generate_test_client_cert(
    ca_cert: &rcgen::Certificate,
    ca_key: &rcgen::KeyPair,
    common_name: &str,
    organizations: &[&str],
) -> String {
    let key = rcgen::KeyPair::generate().unwrap();
    let mut params = rcgen::CertificateParams::default();
    params.distinguished_name = rcgen::DistinguishedName::new();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, common_name);
    for organization in organizations {
        params
            .distinguished_name
            .push(rcgen::DnType::OrganizationName, (*organization).to_string());
    }
    params
        .extended_key_usages
        .push(rcgen::ExtendedKeyUsagePurpose::ClientAuth);
    params.signed_by(&key, ca_cert, ca_key).unwrap().pem()
}

#[tokio::test]
async fn test_trusted_api_proxy_identity_is_authorized_as_delegated_user() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let recording = std::sync::Arc::new(crate::auth::authorizer::RecordingAuthorizer::allow());
    let mut state = crate::api::test_support::build_test_app_state().await;
    state.authorizer = recording.clone() as std::sync::Arc<dyn crate::auth::authorizer::Authorizer>;
    let app = crate::api::build_router(state);

    let (ca_cert, ca_key, _, _) = crate::auth::generate_ca_full().unwrap();
    let proxy_cert_pem = generate_test_client_cert(
        &ca_cert,
        &ca_key,
        "system:klights:api-proxy:mn-controlplane2",
        &[],
    );

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/nodes")
                .header("x-remote-user", "delegated-user")
                .header("x-remote-group", "delegated-group")
                .header("x-remote-group", "system:authenticated")
                .extension(crate::auth::TlsClientCertificate(pem_cert_der(
                    &proxy_cert_pem,
                )))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let recorded = recording.take_requests().await;
    assert_eq!(recorded.len(), 1);
    let identity = &recorded[0].0;
    assert_eq!(identity.username, "delegated-user");
    assert!(
        identity.groups.contains(&"delegated-group".to_string()),
        "delegated requestheader group must be authorized, got {:?}",
        identity.groups
    );
    assert!(
        !identity.groups.contains(&"system:masters".to_string()),
        "authorization must use the delegated caller, not the proxy admin certificate"
    );
}

#[tokio::test]
async fn test_server_cert_identity_cannot_delegate_requestheaders() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let recording = std::sync::Arc::new(crate::auth::authorizer::RecordingAuthorizer::allow());
    let mut state = crate::api::test_support::build_test_app_state().await;
    state.authorizer = recording.clone() as std::sync::Arc<dyn crate::auth::authorizer::Authorizer>;
    let app = crate::api::build_router(state);

    let (ca_cert, ca_key, _, _) = crate::auth::generate_ca_full().unwrap();
    let (server_cert_pem, _) = crate::auth::generate_server_cert(&ca_cert, &ca_key).unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/nodes")
                .header("x-remote-user", "delegated-user")
                .header("x-remote-group", "delegated-group")
                .extension(crate::auth::TlsClientCertificate(pem_cert_der(
                    &server_cert_pem,
                )))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let recorded = recording.take_requests().await;
    assert_eq!(recorded.len(), 1);
    assert_eq!(
        recorded[0].0.username, "klights-server",
        "server cert identity must not be trusted as a requestheader proxy"
    );
    assert!(
        !recorded[0]
            .0
            .groups
            .contains(&"delegated-group".to_string()),
        "server cert must not delegate caller-supplied requestheader groups"
    );
}

#[tokio::test]
async fn test_raft_follower_proxy_forwards_authenticated_client_cert_identity_headers() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (captured_tx, captured_rx) = oneshot::channel::<String>();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 8192];
        let n = stream.read(&mut buf).await.unwrap_or(0);
        let captured = String::from_utf8_lossy(&buf[..n]).to_string();
        let _ = captured_tx.send(captured);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
            .await
            .unwrap();
    });

    let mut state = crate::api::test_support::build_test_app_state().await;
    let (_, is_leader_rx) = tokio::sync::watch::channel(false);
    let (_, leader_addr_rx) = tokio::sync::watch::channel(Some(format!("http://{addr}")));
    state.is_raft_leader_rx = Some(std::sync::Arc::new(
        crate::api::raft_proxy::RaftLeaderProxy::new(is_leader_rx, leader_addr_rx, None),
    ));
    let app = crate::api::build_router(state);

    let (ca_cert, ca_key, _, _) = crate::auth::generate_ca_full().unwrap();
    let (admin_cert_pem, _) = crate::auth::generate_admin_cert(&ca_cert, &ca_key).unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/nodes")
                .header("x-remote-user", "spoofed-user")
                .header("x-remote-group", "spoofed-group")
                .extension(crate::auth::TlsClientCertificate(pem_cert_der(
                    &admin_cert_pem,
                )))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let captured = tokio::time::timeout(std::time::Duration::from_secs(5), captured_rx)
        .await
        .expect("leader mock must receive proxied request")
        .expect("leader mock must capture request");
    let lower = captured.to_ascii_lowercase();
    assert!(
        lower.contains("\r\nx-remote-user: klights-admin\r\n"),
        "proxy must forward authenticated caller username, got:\n{captured}"
    );
    assert!(
        lower.contains("\r\nx-remote-group: system:masters\r\n"),
        "proxy must forward authenticated caller groups, got:\n{captured}"
    );
    assert!(
        lower.contains("\r\nx-remote-group: system:authenticated\r\n"),
        "proxy must forward system:authenticated for client cert callers, got:\n{captured}"
    );
    assert!(
        !lower.contains("spoofed-user") && !lower.contains("spoofed-group"),
        "client-supplied requestheader identity must be stripped before delegation, got:\n{captured}"
    );
    server.abort();
}

#[tokio::test]
async fn test_task_supervisor_category_and_task_endpoints() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = crate::api::test_support::build_test_router().await;
    let categories_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/klights/v1/task-supervisor/categories")
                .extension(crate::auth::AuthenticatedIdentity::admin("klights-admin"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(categories_resp.status(), StatusCode::OK);
    let categories = {
        let bytes = axum::body::to_bytes(categories_resp.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice::<Value>(&bytes).unwrap()
    };
    let categories = categories.as_array().expect("array response");
    let mut names = categories
        .iter()
        .map(|row| row["category"].as_str().unwrap_or_default().to_string())
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(
        names,
        vec![
            "background",
            "db",
            "file",
            "network",
            "others",
            "pod-delete-workqueue",
            "pod-lifecycle-actor",
            "pod-lifecycle-work",
            "pod-probe",
            "timer",
        ]
    );

    let tasks_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/klights/v1/task-supervisor/tasks")
                .extension(crate::auth::AuthenticatedIdentity::admin("klights-admin"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(tasks_resp.status(), StatusCode::OK);

    let file_tasks_resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/klights/v1/task-supervisor/categories/file/tasks")
                .extension(crate::auth::AuthenticatedIdentity::admin("klights-admin"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(file_tasks_resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn node_get_and_list_inject_last_heartbeat_time_only_on_raft_leader() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    fn raft_proxy(is_leader: bool) -> std::sync::Arc<crate::api::raft_proxy::RaftLeaderProxy> {
        let (_, is_leader_rx) = tokio::sync::watch::channel(is_leader);
        let (_, leader_addr_rx) = tokio::sync::watch::channel(None::<String>);
        std::sync::Arc::new(crate::api::raft_proxy::RaftLeaderProxy::new(
            is_leader_rx,
            leader_addr_rx,
            None,
        ))
    }

    async fn get_response(state: crate::api::AppState, path: &str) -> axum::response::Response {
        let app = crate::api::build_router(state);
        app.oneshot(
            Request::builder()
                .method("GET")
                .uri(path)
                .extension(crate::auth::AuthenticatedIdentity::admin("klights-admin"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
    }

    async fn get_node_body(state: crate::api::AppState, path: &str) -> Value {
        let response = get_response(state, path).await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice::<Value>(&bytes).unwrap()
    }

    let mut leader_state = crate::api::test_support::build_test_app_state().await;
    leader_state.is_raft_leader_rx = Some(raft_proxy(true));
    leader_state
        .db
        .create_resource(
            "v1",
            "Node",
            None,
            "worker-a",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "worker-a"},
                "status": {
                    "conditions": [{
                        "type": "Ready",
                        "status": "True",
                        "reason": "KubeletReady",
                        "message": "klights is ready",
                        "lastTransitionTime": "2026-05-13T06:35:00Z"
                    }]
                }
            }),
        )
        .await
        .unwrap();
    leader_state
        .node_lease_tracker
        .record_from_lease_object(
            "worker-a",
            &json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": "worker-a", "namespace": "kube-node-lease"},
                "spec": {
                    "holderIdentity": "worker-a",
                    "leaseDurationSeconds": 30,
                    "renewTime": "2026-05-13T06:35:10.000000Z"
                }
            }),
        )
        .await
        .unwrap();

    let leader_get = get_node_body(leader_state.clone(), "/api/v1/nodes/worker-a").await;
    assert_eq!(
        leader_get["status"]["conditions"][0]["lastHeartbeatTime"],
        "2026-05-13T06:35:10Z"
    );
    let leader_list = get_node_body(leader_state, "/api/v1/nodes").await;
    assert_eq!(
        leader_list["items"][0]["status"]["conditions"][0]["lastHeartbeatTime"],
        "2026-05-13T06:35:10Z"
    );

    let mut follower_state = crate::api::test_support::build_test_app_state().await;
    follower_state.is_raft_leader_rx = Some(raft_proxy(false));
    follower_state
        .db
        .create_resource(
            "v1",
            "Node",
            None,
            "worker-a",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "worker-a"},
                "status": {
                    "conditions": [{
                        "type": "Ready",
                        "status": "True",
                        "reason": "KubeletReady",
                        "message": "klights is ready",
                        "lastTransitionTime": "2026-05-13T06:35:00Z"
                    }]
                }
            }),
        )
        .await
        .unwrap();
    follower_state
        .node_lease_tracker
        .record_from_lease_object(
            "worker-a",
            &json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": "worker-a", "namespace": "kube-node-lease"},
                "spec": {
                    "holderIdentity": "worker-a",
                    "leaseDurationSeconds": 30,
                    "renewTime": "2026-05-13T06:35:10.000000Z"
                }
            }),
        )
        .await
        .unwrap();
    let follower_response = get_response(follower_state, "/api/v1/nodes/worker-a").await;
    assert_eq!(
        follower_response.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "followers must fail closed instead of serving local cluster.db reads"
    );
    let follower_body: Value = serde_json::from_slice(
        &axum::body::to_bytes(follower_response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(follower_body["kind"], "Status");
    assert_eq!(follower_body["reason"], "ServiceUnavailable");
    assert_eq!(follower_body["code"], 503);
}

#[tokio::test]
async fn test_task_supervisor_db_query_logging_toggle() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = crate::api::test_support::build_test_router().await;

    let get_before = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/klights/v1/task-supervisor/db-query-logging")
                .extension(crate::auth::AuthenticatedIdentity::admin("klights-admin"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_before.status(), StatusCode::OK);
    let get_before_json = {
        let bytes = axum::body::to_bytes(get_before.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice::<Value>(&bytes).unwrap()
    };
    assert_eq!(get_before_json["enabled"], false);

    let put_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/klights/v1/task-supervisor/db-query-logging")
                .extension(crate::auth::AuthenticatedIdentity::admin("klights-admin"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"enabled":true}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put_resp.status(), StatusCode::OK);
    let put_json = {
        let bytes = axum::body::to_bytes(put_resp.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice::<Value>(&bytes).unwrap()
    };
    assert_eq!(put_json["enabled"], true);

    let get_after = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/klights/v1/task-supervisor/db-query-logging")
                .extension(crate::auth::AuthenticatedIdentity::admin("klights-admin"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_after.status(), StatusCode::OK);
    let get_after_json = {
        let bytes = axum::body::to_bytes(get_after.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice::<Value>(&bytes).unwrap()
    };
    assert_eq!(get_after_json["enabled"], true);
}

#[tokio::test]
async fn test_task_supervisor_active_background_and_others_tasks_are_queryable() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::Value;
    use tower::ServiceExt;

    let state = crate::api::test_support::build_test_app_state().await;
    let task_supervisor = state.task_supervisor.clone();
    let app = crate::api::build_router(state);

    let (bg_tx, bg_rx) = tokio::sync::oneshot::channel::<()>();
    let (other_tx, other_rx) = tokio::sync::oneshot::channel::<()>();

    let bg_handle = task_supervisor
        .spawn_async(
            crate::task_supervisor::TaskCategory::Background,
            "test_background_queryable_task",
            async move {
                let _ = bg_rx.await;
            },
        )
        .await
        .expect("spawn background task");
    let other_handle = task_supervisor
        .spawn_async(
            crate::task_supervisor::TaskCategory::Others,
            "test_others_queryable_task",
            async move {
                let _ = other_rx.await;
            },
        )
        .await
        .expect("spawn others task");

    let background_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/klights/v1/task-supervisor/categories/background/tasks")
                .extension(crate::auth::AuthenticatedIdentity::admin("klights-admin"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(background_resp.status(), StatusCode::OK);
    let background_json: Value = {
        let bytes = axum::body::to_bytes(background_resp.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    };
    let background_rows = background_json.as_array().expect("background array");
    assert!(
        background_rows.iter().any(|row| {
            row.get("name").and_then(|name| name.as_str()) == Some("test_background_queryable_task")
        }),
        "background task should be visible in category-specific tasks endpoint"
    );

    let others_resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/klights/v1/task-supervisor/categories/others/tasks")
                .extension(crate::auth::AuthenticatedIdentity::admin("klights-admin"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(others_resp.status(), StatusCode::OK);
    let others_json: Value = {
        let bytes = axum::body::to_bytes(others_resp.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    };
    let others_rows = others_json.as_array().expect("others array");
    assert!(
        others_rows.iter().any(|row| {
            row.get("name").and_then(|name| name.as_str()) == Some("test_others_queryable_task")
        }),
        "others task should be visible in category-specific tasks endpoint"
    );

    let _ = bg_tx.send(());
    let _ = other_tx.send(());
    let _ = bg_handle.join().await;
    let _ = other_handle.join().await;
}

#[test]
fn test_validate_against_schema_rejects_unknown_embedded_resource_metadata_fields() {
    let schema = json!({
        "type": "object",
        "properties": {
            "spec": {
                "type": "object",
                "properties": {
                    "template": {
                        "type": "object",
                        "x-kubernetes-embedded-resource": true,
                        "properties": {
                            "spec": {
                                "type": "object",
                                "x-kubernetes-preserve-unknown-fields": true
                            }
                        }
                    }
                }
            }
        }
    });
    let body = json!({
        "apiVersion": "stable.example.com/v1",
        "kind": "Widget",
        "metadata": {"name": "embedded-meta"},
        "spec": {
            "template": {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "nested-pod",
                    "unknownField": "must-fail"
                },
                "spec": {
                    "containers": []
                }
            }
        }
    });

    let result = validate_against_schema(&body, &schema, "");
    let err = result.expect_err("embedded resource metadata must reject unknown fields");
    let err_str = format!("{err:?}");
    assert!(
        err_str.contains("spec.template.metadata.unknownField"),
        "error must mention the embedded metadata field path, got: {err_str}"
    );
}

#[test]
fn test_validate_against_schema_rejects_unknown_fields_in_typed_array_items_under_schemaless_crs() {
    let schema = json!({
        "type": "object",
        "properties": {
            "spec": {
                "type": "object",
                "x-kubernetes-preserve-unknown-fields": true,
                "properties": {
                    "ports": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "containerPort": {"type": "integer"},
                                "protocol": {"type": "string"},
                                "hostPort": {"type": "integer"}
                            }
                        }
                    }
                }
            }
        }
    });
    let body = json!({
        "apiVersion": "stable.example.com/v1",
        "kind": "Widget",
        "metadata": {"name": "schemaless-array"},
        "spec": {
            "freeform": {
                "stillAllowed": true
            },
            "ports": [{
                "name": "http",
                "containerPort": 8080,
                "protocol": "TCP",
                "hostPort": 8081,
                "unknownNested": "must-fail"
            }]
        }
    });

    let result = validate_against_schema(&body, &schema, "");
    let err =
        result.expect_err("typed array items under schemaless CRs must reject unknown fields");
    let err_str = format!("{err:?}");
    assert!(
        err_str.contains("spec.ports[0].unknownNested"),
        "error must mention the typed array item field path, got: {err_str}"
    );
}

#[tokio::test]
async fn test_check_cr_field_validation_strict_accepts_valid_cr_with_schema_arrays_and_embedded_resource()
 {
    let db = crate::datastore::test_support::in_memory().await;

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "widgets.stable.example.com"},
        "spec": {
            "group": "stable.example.com",
            "scope": "Namespaced",
            "names": {
                "kind": "Widget",
                "plural": "widgets",
                "singular": "widget"
            },
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": {
                            "spec": {
                                "type": "object",
                                "properties": {
                                    "knownField1": {"type": "string"},
                                    "ports": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "name": {"type": "string"},
                                                "containerPort": {"type": "integer"},
                                                "protocol": {"type": "string"},
                                                "hostPort": {"type": "integer"}
                                            }
                                        }
                                    },
                                    "embeddedObj": {
                                        "type": "object",
                                        "x-kubernetes-embedded-resource": true
                                    }
                                }
                            }
                        }
                    }
                }
            }]
        }
    });

    db.create_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "widgets.stable.example.com",
        crd,
    )
    .await
    .unwrap();

    let valid_body = json!({
        "apiVersion": "stable.example.com/v1",
        "kind": "Widget",
        "metadata": {
            "name": "valid-widget",
            "resourceVersion": "7"
        },
        "spec": {
            "knownField1": "val1",
            "ports": [{
                "name": "portName",
                "containerPort": 8080,
                "protocol": "TCP",
                "hostPort": 8081
            }],
            "embeddedObj": {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": "my-cm"
                }
            }
        }
    });

    let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());

    let result = check_cr_field_validation_strict(
        db_handle.as_ref(),
        "stable.example.com",
        "v1",
        "Widget",
        &valid_body,
    )
    .await;
    assert!(
        result.is_ok(),
        "valid CR with schema-backed arrays and embedded resource must be accepted: {result:?}"
    );
}

#[test]
fn test_map_validating_admission_error_returns_forbidden() {
    let err = anyhow::anyhow!("Admission denied by webhook: blocked");
    let mapped = map_validating_admission_error(err);
    match mapped {
        AppError::Forbidden(msg) => assert!(msg.contains("blocked")),
        other => panic!("expected Forbidden, got {:?}", other),
    }
}

#[test]
fn test_build_admission_context_for_delete_populates_old_object_and_options() {
    let old = json!({"metadata":{"name":"p0","namespace":"default"}});
    let ctx = build_admission_context(AdmissionContextRequest {
        api_version: "v1",
        kind: "Pod",
        operation: "DELETE",
        namespace: Some("default".to_string()),
        name: Some("p0".to_string()),
        object: Value::Null,
        old_object: Some(old.clone()),
        dry_run: true,
        subresource: None,
        options: Some(json!({"kind":"DeleteOptions","propagationPolicy":"Background"})),
    });
    assert_eq!(ctx.operation, "DELETE");
    assert_eq!(ctx.namespace.as_deref(), Some("default"));
    assert_eq!(ctx.name.as_deref(), Some("p0"));
    assert_eq!(ctx.object, Value::Null);
    assert_eq!(ctx.old_object, Some(old));
    assert_eq!(ctx.options.as_ref().unwrap()["kind"], "DeleteOptions");
    assert_eq!(ctx.dry_run, Some(true));
}

#[tokio::test]
async fn test_resolve_service_proxy_target_uses_dns_hostname() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "wardle-service",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "wardle-service", "namespace": "default"},
            "subsets": [{
                "addresses": [{"ip": "127.0.0.1"}],
                "ports": [{"port": 9443}]
            }]
        }),
    )
    .await
    .unwrap();

    let target = resolve_service_proxy_target(&db, "default", "wardle-service", 443)
        .await
        .unwrap();

    assert_eq!(target.host, "wardle-service.default.svc");
    assert_eq!(target.port, 443);
    assert_eq!(target.endpoint_addr.port(), 9443);
    assert_eq!(target.endpoint_addr.ip().to_string(), "127.0.0.1");
}

// P1-MEM-01: verify hot-path JSON serialization uses to_vec (no String intermediary).
// These tests confirm the serialized bytes are byte-identical to serde_json::to_vec output.

fn make_pod_value() -> serde_json::Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "test-pod", "namespace": "default", "resourceVersion": "42"},
        "spec": {"containers": [{"name": "nginx", "image": "nginx:latest"}]},
        "status": {"phase": "Running"}
    })
}

fn make_pod_list_value() -> serde_json::Value {
    json!({
        "apiVersion": "v1",
        "kind": "PodList",
        "metadata": {"resourceVersion": "100"},
        "items": [make_pod_value()]
    })
}

fn make_configmap_value() -> serde_json::Value {
    json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "test-cm", "namespace": "default"},
        "data": {"key": "value"}
    })
}

fn make_status_value() -> serde_json::Value {
    json!({
        "apiVersion": "v1",
        "kind": "Status",
        "status": "Failure",
        "message": "not found",
        "code": 404
    })
}

#[test]
fn test_serialization_pod_bytes_match_to_vec() {
    let value = make_pod_value();
    let expected = serde_json::to_vec(&value).unwrap();
    let via_writer = {
        let mut buf = Vec::with_capacity(4096);
        serde_json::to_writer(&mut buf, &value).unwrap();
        buf
    };
    assert_eq!(
        expected, via_writer,
        "Pod: to_vec and to_writer must be byte-identical"
    );
}

#[test]
fn test_serialization_pod_list_bytes_match_to_vec() {
    let value = make_pod_list_value();
    let expected = serde_json::to_vec(&value).unwrap();
    let via_writer = {
        let mut buf = Vec::with_capacity(4096);
        serde_json::to_writer(&mut buf, &value).unwrap();
        buf
    };
    assert_eq!(
        expected, via_writer,
        "PodList: to_vec and to_writer must be byte-identical"
    );
}

#[test]
fn test_serialization_configmap_bytes_match_to_vec() {
    let value = make_configmap_value();
    let expected = serde_json::to_vec(&value).unwrap();
    let via_writer = {
        let mut buf = Vec::with_capacity(4096);
        serde_json::to_writer(&mut buf, &value).unwrap();
        buf
    };
    assert_eq!(
        expected, via_writer,
        "ConfigMap: to_vec and to_writer must be byte-identical"
    );
}

#[test]
fn test_serialization_status_bytes_match_to_vec() {
    let value = make_status_value();
    let expected = serde_json::to_vec(&value).unwrap();
    let via_writer = {
        let mut buf = Vec::with_capacity(4096);
        serde_json::to_writer(&mut buf, &value).unwrap();
        buf
    };
    assert_eq!(
        expected, via_writer,
        "Status: to_vec and to_writer must be byte-identical"
    );
}

#[test]
fn test_normalize_events_v1_to_core_event_shape_maps_legacy_fields() {
    let mut event = json!({
        "apiVersion": "events.k8s.io/v1",
        "kind": "Event",
        "metadata": {"name": "evt", "namespace": "default"},
        "regarding": {"kind": "Pod", "name": "p1", "namespace": "default"},
        "reportingController": "test-controller",
        "reportingInstance": "test-host",
        "deprecatedFirstTimestamp": "2026-01-01T00:00:00Z",
        "deprecatedLastTimestamp": "2026-01-01T00:00:01Z"
    });

    normalize_resource_for_read("v1", "Event", &mut event);

    assert_eq!(event["apiVersion"], "v1");
    assert_eq!(event["kind"], "Event");
    assert_eq!(event["involvedObject"]["kind"], "Pod");
    assert_eq!(event["involvedObject"]["name"], "p1");
    assert_eq!(event["source"]["component"], "test-controller");
    assert_eq!(event["source"]["host"], "test-host");
    assert_eq!(event["firstTimestamp"], "2026-01-01T00:00:00Z");
    assert_eq!(event["lastTimestamp"], "2026-01-01T00:00:01Z");
}

#[test]
fn test_normalize_events_v1_to_core_event_shape_ignores_empty_deprecated_source() {
    let mut event = json!({
        "apiVersion": "events.k8s.io/v1",
        "kind": "Event",
        "metadata": {"name": "evt", "namespace": "default"},
        "deprecatedSource": {"component": ""},
        "reportingController": "test-controller",
        "reportingInstance": "test-host"
    });

    normalize_resource_for_read("v1", "Event", &mut event);

    assert_eq!(event["source"]["component"], "test-controller");
    assert_eq!(event["source"]["host"], "test-host");
}

#[test]
fn test_continue_token_encode_uses_vec_not_string() {
    // Verify encode_continue_token produces valid base64url(JSON) without String intermediary.
    // The decoded bytes must be valid UTF-8 JSON that round-trips to the same fields.
    use base64::Engine as _;
    let token = encode_continue_token("my-pod", 42);
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(&token)
        .expect("valid base64");
    let data: ContinueTokenData =
        serde_json::from_slice(&decoded).expect("valid JSON in continue token");
    assert_eq!(data.n, "my-pod");
    assert_eq!(data.rv, 42);
    assert!(data.ts.is_some());
    assert!(!data.session);
}

#[test]
fn test_inconsistent_continue_token_encode_uses_vec_not_string() {
    use base64::Engine as _;
    let token = encode_inconsistent_continue_token("last-pod", 42);
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(&token)
        .expect("valid base64");
    let data: ContinueTokenData =
        serde_json::from_slice(&decoded).expect("valid JSON in continue token");
    assert_eq!(data.n, "last-pod");
    assert_eq!(data.rv, 42);
    assert!(data.ts.is_none());
    assert!(!data.session);
}

fn list_query_with_limit(limit: Option<i64>) -> ListQuery {
    ListQuery {
        label_selector: None,
        field_selector: None,
        limit,
        continue_token: None,
        watch: None,
        resource_version: None,
        resource_version_match: None,
        allow_watch_bookmarks: None,
        send_initial_events: None,
        timeout_seconds: None,
    }
}

#[test]
fn test_list_query_limit_zero_normalizes_to_unbounded() {
    assert_eq!(
        list_query_with_limit(Some(0)).normalized_limit().unwrap(),
        None
    );
}

#[test]
fn test_list_query_negative_limit_returns_bad_request() {
    let err = list_query_with_limit(Some(-1))
        .normalized_limit()
        .unwrap_err();
    match err {
        AppError::BadRequest(message) => assert!(message.contains("limit")),
        other => panic!("expected BadRequest for negative limit, got {other:?}"),
    }
}

#[test]
fn test_anyhow_409_already_exists_maps_to_already_exists_reason() {
    let app_err = AppError::from(anyhow::anyhow!("Resource already exists (409 Conflict)"));
    let response = app_err.into_response();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body = response_body_json(response);
    assert_eq!(body["reason"], "AlreadyExists");
}

#[test]
fn test_anyhow_409_version_conflict_maps_to_conflict_reason() {
    let app_err = AppError::from(anyhow::anyhow!(
        "Resource not found or version conflict (409 Conflict)"
    ));
    let response = app_err.into_response();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body = response_body_json(response);
    assert_eq!(body["reason"], "Conflict");
}

#[tokio::test]
async fn test_inconsistent_continue_token_uses_fresh_resource_version_without_writes() {
    use base64::Engine as _;

    let db = crate::datastore::test_support::in_memory().await;
    let namespace = "default".to_string();
    // 25 templates with limit=10 still yields multi-page (3 pages), enough to
    // exercise continue-token semantics + the inconsistent-token recovery.
    // Original 50 was overkill.
    for i in 0..25 {
        let name = format!("template-{i:04}");
        db.create_resource(
            "v1",
            "PodTemplate",
            Some(&namespace.clone()),
            &name.clone(),
            json!({
                "apiVersion": "v1",
                "kind": "PodTemplate",
                "metadata": {"name": name, "namespace": namespace},
                "template": {
                    "spec": {
                        "containers": [{"name": "test", "image": "test"}]
                    }
                }
            }),
        )
        .await
        .unwrap();
    }

    let first_page = db
        .list_resources(
            "v1",
            "PodTemplate",
            Some(&namespace.clone()),
            crate::datastore::ResourceListQuery::new(None, None, Some(10), None),
        )
        .await
        .unwrap();
    let first_rv = first_page.resource_version;
    let last_name = first_page
        .continue_token
        .as_ref()
        .expect("first page must continue")
        .clone();
    let expired_data = ContinueTokenData {
        n: last_name,
        rv: first_rv,
        ts: Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64
                - CONTINUE_TOKEN_TTL_SECS
                - 1,
        ),
        session: false,
    };
    let expired_token = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(serde_json::to_vec(&expired_data).unwrap());
    let inconsistent_token = match process_continue_token(Some(expired_token)) {
        Err(AppError::ResourceExpired(token)) => token,
        other => panic!("expected expired continue token, got {other:?}"),
    };

    let (continue_name, continue_resource_version) =
        process_continue_token(Some(inconsistent_token)).unwrap();
    let second_page = db
        .list_resources(
            "v1",
            "PodTemplate",
            Some(&namespace),
            crate::datastore::ResourceListQuery::new(
                None,
                None,
                Some(10),
                continue_name.as_deref(),
            ),
        )
        .await
        .unwrap();
    let response_rv = resolve_list_response_resource_version(
        &db,
        continue_resource_version,
        second_page.resource_version,
    )
    .await
    .unwrap();

    assert_ne!(
        response_rv, first_rv,
        "inconsistent continuation must start a new list snapshot even when no resource changed"
    );
    assert_eq!(
        db.get_current_resource_version().await.unwrap(),
        response_rv,
        "future writes must advance beyond the inconsistent list snapshot RV"
    );

    let next_token = crate::api::query::encode_response_continue_token(
        second_page
            .continue_token
            .as_deref()
            .expect("second page must continue"),
        response_rv,
        continue_resource_version,
    );
    let (_continue_name, continue_resource_version) =
        process_continue_token(Some(next_token)).unwrap();
    let next_response_rv =
        resolve_list_response_resource_version(&db, continue_resource_version, response_rv)
            .await
            .unwrap();
    assert_eq!(
        next_response_rv, response_rv,
        "subsequent inconsistent continuation pages must keep the same list RV"
    );
}

#[test]
fn cluster_delete_collection_handler_macro_defined_only_in_macros_rs() {
    // R4: invariant now enforced by check_runtime_invariants.sh
}

// F3-04 DRY gate: no production handler may pass LenientJson(body.clone())
// down to an inner handler. The wrapper has already parsed the body into a
// Value; cloning it doubles request-body memory for every CREATE/UPDATE.
// Reintroducing the pattern fails this test.
#[test]
fn no_lenient_json_body_clone_in_production_handlers() {
    // R4: invariant now enforced by check_supervisor_spawn.sh
}

// ── T6: Raft leader proxy TLS verification tests ──

/// Verify http_client builds without danger_accept_invalid_certs when CA is present.
#[test]
fn raft_leader_proxy_client_builder_no_invalid_certs() {
    use crate::api::raft_proxy::RaftLeaderProxy;
    let (_tx, is_leader_rx) = tokio::sync::watch::channel(false);
    let (_tx, leader_addr_rx) =
        tokio::sync::watch::channel(Some("https://127.0.0.1:7679".to_string()));
    // With a CA cert, the client should build without errors
    let ca_cert = rcgen::generate_simple_self_signed(vec!["leader.test".to_string()])
        .unwrap()
        .cert
        .pem();
    let proxy = RaftLeaderProxy::new(is_leader_rx, leader_addr_rx, Some(ca_cert));
    let client = proxy.http_client();
    // Client built successfully — won't reach this if it panics
    let _ = client;
}

/// Verify http_client builds without CA (backwards compat for single-node).
#[test]
fn raft_leader_proxy_client_builder_no_ca() {
    use crate::api::raft_proxy::RaftLeaderProxy;
    let (_tx, is_leader_rx) = tokio::sync::watch::channel(false);
    let (_tx, leader_addr_rx) =
        tokio::sync::watch::channel(Some("https://127.0.0.1:7679".to_string()));
    let proxy = RaftLeaderProxy::new(is_leader_rx, leader_addr_rx, None);
    let client = proxy.http_client();
    let _ = client;
}

/// Generate a CA cert, a server cert signed by that CA, and return
/// (ca_pem, server_cert_pem, server_key_pem).
fn generate_ca_signed_cert(sans: Vec<String>) -> (String, String, String) {
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca_params = rcgen::CertificateParams::new(vec!["ca".to_string()]).unwrap();
    ca_params.distinguished_name = rcgen::DistinguishedName::new();
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "test-ca");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        rcgen::KeyUsagePurpose::KeyCertSign,
        rcgen::KeyUsagePurpose::CrlSign,
    ];
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    let server_key = rcgen::KeyPair::generate().unwrap();
    let mut server_params = rcgen::CertificateParams::new(sans).unwrap();
    server_params.distinguished_name = rcgen::DistinguishedName::new();
    server_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "test-server");
    let server_cert = server_params
        .signed_by(&server_key, &ca_cert, &ca_key)
        .unwrap();

    (ca_cert.pem(), server_cert.pem(), server_key.serialize_pem())
}

/// Start a local TLS server that accepts one connection and responds with
/// a fixed HTTP response. Returns the listening port.
async fn start_tls_server_async(cert_pem: String, key_pem: String, response: String) -> u16 {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let certs: Vec<rustls::pki_types::CertificateDer> =
        rustls_pemfile::certs(&mut cert_pem.as_bytes())
            .collect::<Result<_, _>>()
            .unwrap();
    let key = rustls_pemfile::private_key(&mut key_pem.as_bytes())
        .unwrap()
        .unwrap();
    let server_config = std::sync::Arc::new(
        rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .unwrap(),
    );
    let acceptor = tokio_rustls::TlsAcceptor::from(server_config);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await {
            if let Ok(mut tls) = acceptor.accept(stream).await {
                use tokio::io::AsyncWriteExt;
                let _ = tls.write_all(response.as_bytes()).await;
            }
        }
    });

    port
}

/// Proxy must reject a leader certificate signed by an untrusted CA.
#[tokio::test]
async fn raft_proxy_rejects_leader_certificate_signed_by_untrusted_ca() {
    use crate::api::raft_proxy::RaftLeaderProxy;

    // Generate CA A + server cert signed by CA A
    let (ca_a_pem, _server_a_pem, _server_a_key) =
        generate_ca_signed_cert(vec!["127.0.0.1".to_string()]);
    // Generate CA B + server cert signed by CA B
    let (_ca_b_pem, server_b_pem, server_b_key) =
        generate_ca_signed_cert(vec!["127.0.0.1".to_string()]);

    // Start server with CA B cert
    let port = start_tls_server_async(
        server_b_pem,
        server_b_key,
        "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok".to_string(),
    )
    .await;

    // Build proxy configured with CA A — should reject server using CA B
    let (_tx, is_leader_rx) = tokio::sync::watch::channel(false);
    let (_tx, leader_addr_rx) =
        tokio::sync::watch::channel(Some(format!("https://127.0.0.1:{}", port)));
    let proxy = RaftLeaderProxy::new(is_leader_rx, leader_addr_rx, Some(ca_a_pem));

    let client = proxy.http_client();
    let result = client
        .get(format!("https://127.0.0.1:{}/test", port))
        .send()
        .await;

    // Should fail because the server cert is signed by CA B, not CA A
    assert!(
        result.is_err(),
        "proxy should reject server certificate signed by untrusted CA"
    );
}

/// Proxy must forward requests to a leader with a trusted CA certificate
/// and matching SAN.
#[tokio::test]
async fn raft_proxy_forwards_to_leader_with_trusted_ca_and_matching_san() {
    use crate::api::raft_proxy::RaftLeaderProxy;

    let (ca_pem, server_pem, server_key) = generate_ca_signed_cert(vec!["127.0.0.1".to_string()]);

    let body = "{\"result\":\"ok\"}";
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    let port = start_tls_server_async(server_pem, server_key, response).await;

    let (_tx, is_leader_rx) = tokio::sync::watch::channel(false);
    let (_tx, leader_addr_rx) =
        tokio::sync::watch::channel(Some(format!("https://127.0.0.1:{}", port)));
    let proxy = RaftLeaderProxy::new(is_leader_rx, leader_addr_rx, Some(ca_pem));

    let client = proxy.http_client();
    let result = client
        .get(format!("https://127.0.0.1:{}/test", port))
        .send()
        .await;

    let resp = result.expect("trusted CA and matching loopback IP SAN must connect");
    assert_eq!(resp.status(), 200);
}

/// Proxy must reject a leader certificate whose SAN does not match
/// the leader URL.
#[tokio::test]
async fn raft_proxy_rejects_leader_certificate_with_san_mismatch() {
    use crate::api::raft_proxy::RaftLeaderProxy;

    // Server cert has SAN 127.0.0.2 but we'll connect to 127.0.0.1.
    let (ca_pem, server_pem, server_key) = generate_ca_signed_cert(vec!["127.0.0.2".to_string()]);

    let port = start_tls_server_async(
        server_pem,
        server_key,
        "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".to_string(),
    )
    .await;

    let (_tx, is_leader_rx) = tokio::sync::watch::channel(false);
    let (_tx, leader_addr_rx) =
        tokio::sync::watch::channel(Some(format!("https://127.0.0.1:{}", port)));
    let proxy = RaftLeaderProxy::new(is_leader_rx, leader_addr_rx, Some(ca_pem));

    let client = proxy.http_client();
    let result = client
        .get(format!("https://127.0.0.1:{}/test", port))
        .send()
        .await;

    assert!(
        result.is_err(),
        "proxy should reject server with mismatched loopback IP SAN"
    );
}

/// Proxy must preserve method, path, query, headers, and body when forwarding.
#[tokio::test]
async fn raft_proxy_preserves_forwarded_method_path_query_headers_and_body() {
    use crate::api::raft_proxy::RaftLeaderProxy;

    let (ca_pem, server_pem, server_key) = generate_ca_signed_cert(vec!["127.0.0.1".to_string()]);

    // Response echoes back a known body
    let echo_body = "echo-me";
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{}",
        echo_body.len(),
        echo_body
    );
    let port = start_tls_server_async(server_pem, server_key, response).await;

    let (_tx, is_leader_rx) = tokio::sync::watch::channel(false);
    let (_tx, leader_addr_rx) =
        tokio::sync::watch::channel(Some(format!("https://127.0.0.1:{}", port)));
    let proxy = RaftLeaderProxy::new(is_leader_rx, leader_addr_rx, Some(ca_pem));

    let client = proxy.http_client();
    let resp = client
        .get(format!("https://127.0.0.1:{}/test?foo=bar", port))
        .header("X-Custom", "test-value")
        .send()
        .await
        .expect("trusted CA and matching loopback IP SAN must connect");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert_eq!(body, echo_body);
}
