//! Static helper functions used across redb domain stores.
//!
//! All functions here are pure (no `&self`) and operate on redb
//! transactions, JSON values, or byte slices.

use std::collections::BTreeSet;

use ::redb::ReadableTable;
use anyhow::Result;
use serde_json::Value;

use klights_cluster_core::{Resource, ResourcePreconditions};
use klights_cluster_datastore::redb::tables;

/// Deserialize a redb value body into an Arc<Value>.
pub fn body_val(body: &[u8]) -> std::sync::Arc<Value> {
    std::sync::Arc::new(serde_json::from_slice(body).unwrap_or(Value::Null))
}

pub fn preserve_server_metadata_fields_from_existing(data: &mut Value, existing: &Value) {
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

/// Post-fetch field selector filtering (mirrors SQLite's `filter_by_field_selector`).
#[cfg(test)]
pub fn filter_by_field_selector(items: Vec<Resource>, selector: &str) -> Vec<Resource> {
    let selector = klights_types::FieldSelector::parse(selector)
        .expect("API validation must reject malformed field selectors before datastore filtering");
    items
        .into_iter()
        .filter(|item| {
            selector.matches_resource_with_identity(&item.api_version, &item.kind, &item.data)
        })
        .collect()
}

/// Resolve a dotted field path inside a JSON resource body.
#[cfg(test)]
pub fn resolve_field_path<'a>(data: &'a Value, path: &str) -> Option<std::borrow::Cow<'a, str>> {
    klights_types::resolve_field_value(data, path)
}

/// Ensure a resource JSON has a non-empty `metadata.uid`.
pub fn ensure_uid(data: &mut Value) {
    if let Some(obj) = data.as_object_mut()
        && let Some(metadata) = obj.get_mut("metadata")
        && let Some(meta_obj) = metadata.as_object_mut()
    {
        let missing = meta_obj
            .get("uid")
            .is_none_or(|v| v.is_null() || v.as_str().is_some_and(|s| s.trim().is_empty()));
        if missing {
            meta_obj.insert(
                "uid".to_string(),
                serde_json::Value::String(uuid::Uuid::new_v4().to_string()),
            );
        }
    }
}

/// Extract the UID from a resource JSON value.
pub fn resource_uid(data: &Value) -> Option<&str> {
    data.pointer("/metadata/uid")
        .and_then(|v| v.as_str())
        .filter(|s| !s.trim().is_empty())
}

/// Validate that the UID has not changed between incoming and current.
pub fn validate_uid_immutable(incoming: &Value, current: &Value) -> Result<()> {
    let Some(incoming_uid) = resource_uid(incoming) else {
        return Ok(());
    };
    let Some(current_uid) = resource_uid(current) else {
        return Ok(());
    };
    klights_cluster_core::validate_apply_preconditions(
        klights_cluster_core::ApplyPreconditions {
            uid: Some(incoming_uid),
            ..klights_cluster_core::ApplyPreconditions::default()
        },
        Some(klights_cluster_core::CurrentResourceState {
            uid: Some(current_uid),
            resource_version: 0,
        }),
    )
    .map_err(|_| {
        klights_cluster_datastore::errors::DatastoreError::conflict("metadata.uid is immutable")
            .into()
    })
}

/// Validate resource preconditions (UID and RV) against current state.
pub fn validate_resource_preconditions(
    preconditions: &ResourcePreconditions,
    current: &Value,
    current_rv: i64,
) -> Result<()> {
    klights_cluster_core::validate_apply_preconditions(
        klights_cluster_core::ApplyPreconditions {
            uid: preconditions.uid.as_deref(),
            resource_version: preconditions.resource_version,
            ..klights_cluster_core::ApplyPreconditions::default()
        },
        Some(klights_cluster_core::CurrentResourceState {
            uid: resource_uid(current),
            resource_version: current_rv,
        }),
    )
    .map_err(|violation| match violation {
        klights_cluster_core::ApplyPreconditionViolation::Uid { .. } => {
            klights_cluster_datastore::errors::DatastoreError::conflict("UID precondition failed")
                .into()
        }
        klights_cluster_core::ApplyPreconditionViolation::ResourceVersion { .. } => {
            klights_cluster_datastore::errors::DatastoreError::conflict(
                "resourceVersion precondition failed",
            )
            .into()
        }
        other => {
            klights_cluster_datastore::errors::DatastoreError::conflict(other.to_string()).into()
        }
    })
}

/// Increment the global resource version counter.
pub fn incr_rv(w: &::redb::WriteTransaction) -> Result<i64> {
    let mut meta = w.open_table(tables::META)?;
    let rv = meta
        .get("rv")?
        .map(|g| {
            std::str::from_utf8(g.value())
                .unwrap_or("0")
                .parse::<i64>()
                .unwrap_or(0)
        })
        .unwrap_or(0);
    let next = rv + 1;
    meta.insert("rv", next.to_string().as_bytes())?;
    Ok(next)
}

/// Advance the global resource-version counter up to at least `min_rv`.
pub fn advance_rv_to_at_least(w: &::redb::WriteTransaction, min_rv: i64) -> Result<()> {
    if min_rv <= 0 {
        return Ok(());
    }
    let mut meta = w.open_table(tables::META)?;
    let current = meta
        .get("rv")?
        .map(|g| {
            std::str::from_utf8(g.value())
                .unwrap_or("0")
                .parse::<i64>()
                .unwrap_or(0)
        })
        .unwrap_or(0);
    if current < min_rv {
        meta.insert("rv", min_rv.to_string().as_bytes())?;
    }
    Ok(())
}

/// Append a watch event to the watch-event table.
pub fn watch_insert(w: &::redb::WriteTransaction, rv: i64, event: &Value) -> Result<()> {
    advance_rv_to_at_least(w, rv)?;
    let event_id = {
        let mut meta = w.open_table(tables::META)?;
        let current = meta
            .get("watch_event_id")?
            .and_then(|value| std::str::from_utf8(value.value()).ok()?.parse::<u64>().ok())
            .unwrap_or(0);
        let next = current.saturating_add(1);
        meta.insert("watch_event_id", next.to_string().as_bytes())?;
        next
    };
    let mut stored = event.clone();
    if let Some(object) = stored.as_object_mut() {
        object.insert("resourceVersion".to_string(), Value::from(rv));
    }
    let mut we = w.open_table(tables::WATCH_EVENTS)?;
    we.insert(event_id, serde_json::to_vec(&stored)?.as_slice())?;
    Ok(())
}

/// Extract owner UIDs from a resource body.
pub fn extract_owner_uids(body: &[u8]) -> BTreeSet<String> {
    let data: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let mut uids = BTreeSet::new();
    if let Some(meta) = data.get("metadata")
        && let Some(refs) = meta.get("ownerReferences").and_then(|v| v.as_array())
    {
        for r in refs {
            if let Some(uid) = r.get("uid").and_then(|v| v.as_str()) {
                uids.insert(uid.to_string());
            }
        }
    }
    uids
}

/// Update the resources-by-owner index table.
pub fn update_owner_table(
    w: &::redb::WriteTransaction,
    av: &str,
    kind: &str,
    ns: Option<&str>,
    name: &str,
    old_body: Option<&[u8]>,
    new_body: Option<&[u8]>,
) -> Result<()> {
    let old_uids = old_body.map(extract_owner_uids).unwrap_or_default();
    let new_uids = new_body.map(extract_owner_uids).unwrap_or_default();
    let mut tbl = w.open_table(tables::RESOURCES_BY_OWNER)?;
    for uid in old_uids.iter() {
        let owner_key = owner_ref_key(uid, av, kind, ns, name);
        tbl.remove(owner_key.as_slice())?;
    }
    if let Some(body) = new_body {
        let rv = read_rv_meta(w)?;
        for uid in new_uids.iter() {
            let owner_key = owner_ref_key(uid, av, kind, ns, name);
            tbl.insert(owner_key.as_slice(), (rv as u64, body))?;
        }
    }
    Ok(())
}

/// Build a key for the resources-by-owner index.
pub fn owner_ref_key(uid: &str, av: &str, kind: &str, ns: Option<&str>, name: &str) -> Vec<u8> {
    let mut key = uid.as_bytes().to_vec();
    key.push(0);
    key.push(if ns.is_some() { 1 } else { 0 });
    key.extend_from_slice(av.as_bytes());
    key.push(0);
    key.extend_from_slice(kind.as_bytes());
    key.push(0);
    if let Some(ns) = ns {
        key.extend_from_slice(ns.as_bytes());
    }
    key.push(0);
    key.extend_from_slice(name.as_bytes());
    key
}

/// Read the current resource version from the meta table.
pub fn read_rv_meta(w: &::redb::WriteTransaction) -> Result<i64> {
    let guard = {
        let tbl = w.open_table(tables::META)?;
        let g = tbl.get("rv")?;
        g.map(|a| {
            std::str::from_utf8(a.value())
                .unwrap_or("0")
                .parse::<i64>()
                .unwrap_or(0)
        })
    };
    Ok(guard.unwrap_or(0))
}

/// Parse a `Resource` from a namespaced resource body.
pub fn resource_in_ns(_key_bytes: &[u8], rv: u64, body: &[u8]) -> Option<Resource> {
    let data: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let item_ns = data
        .get("metadata")
        .and_then(|m| m.get("namespace"))
        .and_then(|n| n.as_str())
        .map(|s| s.to_string());
    let item_name = data
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|n| n.as_str())
        .unwrap_or("")
        .to_string();
    let item_av = data
        .get("apiVersion")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let item_kind = data
        .get("kind")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if item_name.is_empty() && item_av.is_empty() {
        return None;
    }
    Some(Resource {
        id: 0,
        api_version: item_av,
        kind: item_kind,
        namespace: item_ns,
        name: item_name,
        uid: Resource::uid_from_data(&data),
        resource_version: rv as i64,
        data: std::sync::Arc::new(data),
    })
}

/// Current time in milliseconds since epoch.
#[cfg(test)]
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use std::sync::Arc;

    use super::*;

    #[test]
    fn resolve_field_path_simple() {
        let data = json!({"status":{"phase":"Running"}});
        let result = resolve_field_path(&data, "status.phase");
        assert_eq!(result.as_deref(), Some("Running"));
    }

    #[test]
    fn resolve_field_path_missing_key_returns_none() {
        let data = json!({"a":{"b":1}});
        assert!(resolve_field_path(&data, "a.c").is_none());
    }

    #[test]
    fn resolve_field_path_involved_object() {
        let data = json!({"involvedObject":{"kind":"Pod","name":"x"}});
        assert_eq!(
            resolve_field_path(&data, "involvedObject.kind").as_deref(),
            Some("Pod")
        );
    }

    #[test]
    fn validate_uid_immutable_allows_same() {
        let a = json!({"metadata":{"uid":"abc"}});
        let b = json!({"metadata":{"uid":"abc"}});
        assert!(validate_uid_immutable(&a, &b).is_ok());
    }

    #[test]
    fn validate_uid_immutable_rejects_change() {
        let a = json!({"metadata":{"uid":"abc"}});
        let b = json!({"metadata":{"uid":"xyz"}});
        assert!(validate_uid_immutable(&a, &b).is_err());
    }

    #[test]
    fn validate_uid_immutable_allows_missing() {
        let a = json!({"metadata":{}});
        let b = json!({"metadata":{}});
        assert!(validate_uid_immutable(&a, &b).is_ok());
    }

    #[test]
    fn body_val_parses() {
        let v = body_val(b"{\"x\":1}");
        assert_eq!(v.get("x").and_then(|v| v.as_i64()), Some(1));
    }

    #[test]
    fn body_val_invalid_json_returns_null() {
        let v = body_val(b"not json");
        assert!(v.is_null());
    }

    #[test]
    fn ensure_uid_adds_if_missing() {
        let mut data = json!({"metadata":{"name":"x"}});
        ensure_uid(&mut data);
        assert!(
            !data
                .pointer("/metadata/uid")
                .unwrap()
                .as_str()
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn ensure_uid_preserves_existing() {
        let mut data = json!({"metadata":{"uid":"existing"}});
        ensure_uid(&mut data);
        assert_eq!(
            data.pointer("/metadata/uid").unwrap().as_str().unwrap(),
            "existing"
        );
    }

    #[test]
    fn filter_by_field_selector_empty_returns_all() {
        let items = vec![Resource {
            id: 0,
            api_version: "v1".into(),
            kind: "Pod".into(),
            namespace: None,
            name: "a".into(),
            uid: String::new(),
            resource_version: 1,
            data: Arc::new(json!({})),
        }];
        assert_eq!(filter_by_field_selector(items, "").len(), 1);
    }

    #[test]
    fn filter_by_field_selector_applies_node_unschedulable_default() {
        let items = vec![Resource {
            id: 0,
            api_version: "v1".into(),
            kind: "Node".into(),
            namespace: None,
            name: "node-a".into(),
            uid: String::new(),
            resource_version: 1,
            data: Arc::new(json!({"spec": {}})),
        }];

        assert_eq!(
            filter_by_field_selector(items, "spec.unschedulable=false").len(),
            1
        );
    }

    #[test]
    fn extract_owner_uids_from_body() {
        let body = b"{\"metadata\":{\"ownerReferences\":[{\"uid\":\"u1\"},{\"uid\":\"u2\"}]}}";
        let uids = extract_owner_uids(body);
        assert!(uids.contains("u1"));
        assert!(uids.contains("u2"));
    }

    #[test]
    fn extract_owner_uids_no_refs_returns_empty() {
        let body = b"{\"metadata\":{}}";
        assert!(extract_owner_uids(body).is_empty());
    }

    #[test]
    fn owner_ref_key_encodes_correctly() {
        let k1 = owner_ref_key("uid", "v1", "Pod", Some("ns"), "name");
        let k2 = owner_ref_key("uid", "v1", "Pod", Some("ns"), "name");
        assert_eq!(k1, k2);
        let k3 = owner_ref_key("uid2", "v1", "Pod", Some("ns"), "name");
        assert_ne!(k1, k3);
    }

    #[test]
    fn now_ms_is_reasonable() {
        let t = now_ms();
        // Must be after year 2020 in ms
        assert!(t > 1_577_836_800_000);
    }
}
