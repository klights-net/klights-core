use super::*;
use crate::watch::EventType;
use serde_json::{Value, json};

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
fn test_normalize_event_shapes_round_trip_core_and_events_v1_fields() {
    let original = json!({
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": {"name": "evt", "namespace": "default"},
        "involvedObject": {"kind": "Pod", "name": "p1", "namespace": "default"},
        "message": "pulled image",
        "source": {"component": "kubelet", "host": "node-1"},
        "firstTimestamp": "2026-01-01T00:00:00Z",
        "lastTimestamp": "2026-01-01T00:00:01Z",
        "count": 2,
        "eventTime": "2026-01-01T00:00:00.000001Z",
        "series": {"count": 2, "lastObservedTime": "2026-01-01T00:00:01.000001Z"},
        "action": "Pulling",
        "reason": "Pulled",
        "related": {"kind": "Node", "name": "node-1"},
        "reportingComponent": "kubelet",
        "reportingInstance": "node-1",
        "type": "Normal"
    });
    let mut event = original.clone();

    normalize_resource_for_read("events.k8s.io/v1", "Event", &mut event);

    assert_eq!(event["apiVersion"], "events.k8s.io/v1");
    assert_eq!(event["regarding"], original["involvedObject"]);
    assert_eq!(event["note"], original["message"]);
    assert_eq!(event["deprecatedSource"], original["source"]);
    assert_eq!(
        event["deprecatedFirstTimestamp"],
        original["firstTimestamp"]
    );
    assert_eq!(event["deprecatedLastTimestamp"], original["lastTimestamp"]);
    assert_eq!(event["deprecatedCount"], original["count"]);
    assert_eq!(event["reportingController"], original["reportingComponent"]);
    for legacy_key in [
        "involvedObject",
        "message",
        "source",
        "firstTimestamp",
        "lastTimestamp",
        "count",
        "reportingComponent",
    ] {
        assert!(event.get(legacy_key).is_none(), "legacy key {legacy_key}");
    }

    normalize_resource_for_read("v1", "Event", &mut event);
    assert_eq!(event, original);
}

#[test]
fn test_normalize_core_event_to_events_v1_does_not_fabricate_missing_fields() {
    let mut event = json!({
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": {"name": "evt", "namespace": "default"},
        "reason": "Pulled",
        "type": "Normal"
    });

    normalize_resource_for_read("events.k8s.io/v1", "Event", &mut event);

    assert_eq!(event["apiVersion"], "events.k8s.io/v1");
    assert_eq!(event["kind"], "Event");
    for absent in [
        "eventTime",
        "series",
        "action",
        "regarding",
        "note",
        "deprecatedSource",
        "deprecatedFirstTimestamp",
        "deprecatedLastTimestamp",
        "deprecatedCount",
        "reportingController",
        "reportingInstance",
    ] {
        assert!(event.get(absent).is_none(), "fabricated field {absent}");
    }
}
