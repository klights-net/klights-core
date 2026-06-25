use super::*;

pub(super) fn hydrate_watch_event_data(
    mut data: Value,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    resource_version: i64,
) -> Value {
    if let Some(obj) = data.as_object_mut() {
        obj.insert("apiVersion".to_string(), serde_json::json!(api_version));
        obj.insert("kind".to_string(), serde_json::json!(kind));
        let metadata = obj
            .entry("metadata")
            .or_insert_with(|| serde_json::json!({}));
        if let Some(meta_obj) = metadata.as_object_mut() {
            meta_obj.insert("name".to_string(), serde_json::json!(name));
            if let Some(ns) = namespace {
                meta_obj.insert("namespace".to_string(), serde_json::json!(ns));
            }
            meta_obj.insert(
                "resourceVersion".to_string(),
                serde_json::json!(resource_version.to_string()),
            );
        }
    }
    data
}

pub(super) fn ensure_resource_type_meta(data: &mut Value, api_version: &str, kind: &str) {
    let Some(obj) = data.as_object_mut() else {
        return;
    };

    let api_version_missing_or_empty = obj
        .get("apiVersion")
        .and_then(|v| v.as_str())
        .map(|s| s.is_empty())
        .unwrap_or(true);
    if api_version_missing_or_empty {
        obj.insert("apiVersion".to_string(), serde_json::json!(api_version));
    }

    let kind_missing_or_empty = obj
        .get("kind")
        .and_then(|v| v.as_str())
        .map(|s| s.is_empty())
        .unwrap_or(true);
    if kind_missing_or_empty {
        obj.insert("kind".to_string(), serde_json::json!(kind));
    }
}

pub(super) fn ensure_metadata_identity(data: &mut Value, namespace: Option<&str>, name: &str) {
    let Some(obj) = data.as_object_mut() else {
        return;
    };
    let metadata = obj
        .entry("metadata".to_string())
        .or_insert_with(|| serde_json::json!({}));
    let Some(meta_obj) = metadata.as_object_mut() else {
        return;
    };

    let name_missing_or_empty = meta_obj
        .get("name")
        .and_then(|v| v.as_str())
        .map(|s| s.is_empty())
        .unwrap_or(true);
    if name_missing_or_empty {
        meta_obj.insert("name".to_string(), serde_json::json!(name));
    }

    if let Some(ns) = namespace {
        let namespace_missing_or_empty = meta_obj
            .get("namespace")
            .and_then(|v| v.as_str())
            .map(|s| s.is_empty())
            .unwrap_or(true);
        if namespace_missing_or_empty {
            meta_obj.insert("namespace".to_string(), serde_json::json!(ns));
        }
    }
}

pub(super) fn ensure_metadata_uid(data: &mut Value) -> String {
    let generated = uuid::Uuid::new_v4().to_string();
    let Some(obj) = data.as_object_mut() else {
        return generated;
    };
    let metadata = obj
        .entry("metadata".to_string())
        .or_insert_with(|| serde_json::json!({}));
    let Some(meta_obj) = metadata.as_object_mut() else {
        return generated;
    };

    if let Some(uid) = meta_obj
        .get("uid")
        .and_then(|v| v.as_str())
        .filter(|s| !s.trim().is_empty())
    {
        return uid.to_string();
    }

    meta_obj.insert(
        "uid".to_string(),
        serde_json::Value::String(generated.clone()),
    );
    generated
}

pub(super) fn ensure_metadata_create_defaults(data: &mut Value) {
    let Some(obj) = data.as_object_mut() else {
        return;
    };
    let metadata = obj
        .entry("metadata".to_string())
        .or_insert_with(|| serde_json::json!({}));
    let Some(meta_obj) = metadata.as_object_mut() else {
        return;
    };

    if meta_obj
        .get("uid")
        .is_none_or(|v| v.is_null() || v.as_str().is_some_and(|s| s.trim().is_empty()))
    {
        meta_obj.insert(
            "uid".to_string(),
            serde_json::Value::String(uuid::Uuid::new_v4().to_string()),
        );
    }

    if meta_obj
        .get("creationTimestamp")
        .is_none_or(|v| v.is_null() || v.as_str().is_some_and(|s| s.trim().is_empty()))
    {
        meta_obj.insert(
            "creationTimestamp".to_string(),
            serde_json::Value::String(crate::utils::k8s_timestamp()),
        );
    }

    let generation = meta_obj
        .get("generation")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    if generation == 0 {
        meta_obj.insert("generation".to_string(), serde_json::json!(1));
    }
}

pub(super) fn metadata_uid(data: &Value) -> Option<&str> {
    data.pointer("/metadata/uid")
        .and_then(|v| v.as_str())
        .filter(|s| !s.trim().is_empty())
}

pub(super) fn validate_metadata_uid_immutable(incoming: &Value, existing: &Value) -> Result<()> {
    let Some(incoming_uid) = metadata_uid(incoming) else {
        return Ok(());
    };
    let Some(existing_uid) = metadata_uid(existing) else {
        return Ok(());
    };
    if incoming_uid == existing_uid {
        return Ok(());
    }
    Err(crate::datastore::errors::DatastoreError::conflict("metadata.uid is immutable").into())
}

pub(super) fn validate_resource_preconditions(
    preconditions: &ResourcePreconditions,
    current_uid: Option<&str>,
    current_rv: i64,
) -> Result<()> {
    if let Some(expected_uid) = preconditions.uid.as_deref()
        && current_uid != Some(expected_uid)
    {
        return Err(crate::datastore::errors::DatastoreError::conflict(format!(
            "UID precondition failed: expected {expected_uid}"
        ))
        .into());
    }
    if let Some(expected_rv) = preconditions.resource_version
        && current_rv != expected_rv
    {
        return Err(crate::datastore::errors::DatastoreError::conflict(format!(
            "resourceVersion precondition failed: expected {expected_rv} got {current_rv}"
        ))
        .into());
    }
    Ok(())
}

pub(super) fn warn_uid_precondition_mismatch(
    operation: &str,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    expected_uid: &str,
    live_uid: Option<&str>,
) {
    tracing::warn!(
        target: "klights::datastore::uid_precondition",
        operation = %operation,
        api_version = %api_version,
        kind = %kind,
        namespace = namespace.unwrap_or(""),
        name = %name,
        expected_uid = %expected_uid,
        live_uid = live_uid.unwrap_or(""),
        "UID mismatch on resource write"
    );
}

pub(super) fn preserve_server_metadata_fields_from_existing(data: &mut Value, existing: &Value) {
    let Some(meta_obj) = data.get_mut("metadata").and_then(|m| m.as_object_mut()) else {
        return;
    };
    let Some(existing_meta) = existing.get("metadata").and_then(|m| m.as_object()) else {
        return;
    };

    let uid_missing_or_empty = meta_obj
        .get("uid")
        .and_then(|v| v.as_str())
        .map(|s| s.is_empty())
        .unwrap_or(true);
    if uid_missing_or_empty && let Some(uid) = existing_meta.get("uid") {
        meta_obj.insert("uid".to_string(), uid.clone());
    }

    let ts_missing_or_empty = meta_obj
        .get("creationTimestamp")
        .and_then(|v| v.as_str())
        .map(|s| s.is_empty())
        .unwrap_or(true);
    if ts_missing_or_empty && let Some(ts) = existing_meta.get("creationTimestamp") {
        meta_obj.insert("creationTimestamp".to_string(), ts.clone());
    }

    if let Some(deletion_ts) = existing_meta
        .get("deletionTimestamp")
        .filter(|value| !value.is_null())
    {
        meta_obj.insert("deletionTimestamp".to_string(), deletion_ts.clone());
    }
    if let Some(grace) = existing_meta
        .get("deletionGracePeriodSeconds")
        .filter(|value| !value.is_null())
    {
        meta_obj.insert("deletionGracePeriodSeconds".to_string(), grace.clone());
    }
}

pub(super) fn resource_client_owned_state_equal(left: &Value, right: &Value) -> bool {
    let mut left = left.clone();
    let mut right = right.clone();
    strip_status_and_server_metadata(&mut left);
    strip_status_and_server_metadata(&mut right);
    left == right
}

fn strip_status_and_server_metadata(value: &mut Value) {
    let Some(obj) = value.as_object_mut() else {
        return;
    };
    obj.remove("status");

    let Some(metadata) = obj
        .get_mut("metadata")
        .and_then(|value| value.as_object_mut())
    else {
        return;
    };
    for key in [
        "resourceVersion",
        "uid",
        "creationTimestamp",
        "generation",
        "deletionTimestamp",
        "deletionGracePeriodSeconds",
        "managedFields",
    ] {
        metadata.remove(key);
    }
}

pub(super) fn ensure_pod_status_ip_arrays(data: &mut Value, api_version: &str, kind: &str) {
    if api_version != "v1" || kind != "Pod" {
        return;
    }

    let Some(status_obj) = data
        .get_mut("status")
        .and_then(|status| status.as_object_mut())
    else {
        return;
    };

    let pod_ip = status_obj
        .get("podIP")
        .and_then(|v| v.as_str())
        .filter(|ip| !ip.is_empty())
        .map(str::to_string);
    if let Some(pod_ip) = pod_ip {
        let first_pod_ip = status_obj
            .get("podIPs")
            .and_then(|v| v.as_array())
            .and_then(|ips| ips.first())
            .and_then(|entry| entry.get("ip"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if first_pod_ip != pod_ip {
            status_obj.insert("podIPs".to_string(), serde_json::json!([{ "ip": pod_ip }]));
        }
    }

    let host_ip = status_obj
        .get("hostIP")
        .and_then(|v| v.as_str())
        .filter(|ip| !ip.is_empty())
        .map(str::to_string);
    if let Some(host_ip) = host_ip {
        let first_host_ip = status_obj
            .get("hostIPs")
            .and_then(|v| v.as_array())
            .and_then(|ips| ips.first())
            .and_then(|entry| entry.get("ip"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if first_host_ip != host_ip {
            status_obj.insert(
                "hostIPs".to_string(),
                serde_json::json!([{ "ip": host_ip }]),
            );
        }
    }
}
