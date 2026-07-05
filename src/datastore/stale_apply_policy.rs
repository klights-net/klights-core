//! Same-UID stale full-resource apply policy.
//!
//! When a raft-committed PUT arrives for a resource the local member already
//! stores under the same UID but at a different (stale) resource version, the
//! shared fields that the live actor owns must be preserved. This module owns
//! that preservation policy for full-resource applies so the SQLite apply code
//! stays free of scattered stale-PUT helpers.

use std::collections::HashSet;

use serde_json::Value;

/// Apply the centralized same-UID stale full-resource preservation policy.
///
/// `incoming` is the committed payload being applied; `existing` is the live
/// row currently stored under the same UID. The policy mutates `incoming` in
/// place to preserve only the shared actor-owned fields (Pod node name, Pod
/// owner references, and PV/PVC user labels/annotations).
pub fn apply_same_uid_stale_full_resource_policy(
    api_version: &str,
    kind: &str,
    incoming: &mut Value,
    existing: &Value,
) {
    preserve_pod_node_for_stale_put(api_version, kind, incoming, existing);
    preserve_pod_owner_refs_for_stale_put(api_version, kind, incoming, existing);
    preserve_storage_user_metadata_for_stale_put(api_version, kind, incoming, existing);
}

fn preserve_pod_node_for_stale_put(
    api_version: &str,
    kind: &str,
    incoming: &mut Value,
    existing: &Value,
) {
    if api_version != "v1" || kind != "Pod" {
        return;
    }
    let Some(existing_node) = existing
        .pointer("/spec/nodeName")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
    else {
        return;
    };
    let incoming_node = incoming
        .pointer("/spec/nodeName")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty());
    if incoming_node == Some(existing_node.as_str()) {
        return;
    }
    let Some(object) = incoming.as_object_mut() else {
        return;
    };
    let spec = object
        .entry("spec".to_string())
        .or_insert_with(|| Value::Object(Default::default()));
    let Some(spec) = spec.as_object_mut() else {
        return;
    };
    spec.insert("nodeName".to_string(), Value::String(existing_node));
}

fn preserve_pod_owner_refs_for_stale_put(
    api_version: &str,
    kind: &str,
    incoming: &mut Value,
    existing: &Value,
) {
    if api_version != "v1" || kind != "Pod" {
        return;
    }
    let Some(existing_owner_refs) = existing
        .pointer("/metadata/ownerReferences")
        .and_then(|value| value.as_array())
        .filter(|refs| !refs.is_empty())
    else {
        return;
    };
    let incoming_owner_refs = incoming
        .pointer("/metadata/ownerReferences")
        .and_then(|value| value.as_array())
        .map(|refs| refs.to_vec());
    let Some(metadata) = incoming
        .get_mut("metadata")
        .and_then(|value| value.as_object_mut())
    else {
        tracing::trace!(
            api_version = %api_version,
            kind = %kind,
            existing_count = existing_owner_refs.len(),
            "pod stale PUT owner reference preservation skipped: metadata block missing"
        );
        return;
    };

    let incoming_owner_refs = match incoming_owner_refs {
        None => {
            tracing::debug!(
                api_version = %api_version,
                kind = %kind,
                incoming_count = 0,
                existing_count = existing_owner_refs.len(),
                merged_count = existing_owner_refs.len(),
                incoming_uids = "missing",
                existing_uids = ?format_uids(existing_owner_refs),
                merged_uids = ?format_uids(existing_owner_refs),
                "stale Pod PUT retains missing ownerReferences from live row"
            );
            metadata.insert(
                "ownerReferences".to_string(),
                Value::Array(existing_owner_refs.to_vec()),
            );
            return;
        }
        Some(incoming_owner_refs) if incoming_owner_refs.is_empty() => {
            tracing::debug!(
                api_version = %api_version,
                kind = %kind,
                incoming_count = 0,
                existing_count = existing_owner_refs.len(),
                merged_count = 0,
                incoming_uids = "explicit-empty",
                existing_uids = ?format_uids(existing_owner_refs),
                merged_uids = "cleared",
                "stale Pod PUT explicit empty ownerReferences clears live owner references"
            );
            return;
        }
        Some(incoming_owner_refs) => incoming_owner_refs,
    };

    let incoming_count = incoming_owner_refs.len();
    let incoming_uids = format_uids(&incoming_owner_refs);
    let mut incoming_identities = HashSet::with_capacity(incoming_owner_refs.len());
    for owner_ref in incoming_owner_refs.iter() {
        if let Some(identity) = owner_reference_identity(owner_ref) {
            incoming_identities.insert(identity);
        }
    }

    let mut merged_owner_refs = incoming_owner_refs;
    for owner_ref in existing_owner_refs.iter() {
        if let Some(identity) = owner_reference_identity(owner_ref)
            && !incoming_identities.contains(&identity)
        {
            incoming_identities.insert(identity);
            merged_owner_refs.push(owner_ref.clone());
        }
    }

    tracing::debug!(
        api_version = %api_version,
        kind = %kind,
        incoming_count = incoming_count,
        existing_count = existing_owner_refs.len(),
        merged_count = merged_owner_refs.len(),
        incoming_uids = incoming_uids.as_str(),
        existing_uids = ?format_uids(existing_owner_refs),
        merged_uids = ?format_uids(&merged_owner_refs),
        "stale Pod PUT merges missing live ownerReferences into stale incoming ownerReferences"
    );
    metadata.insert(
        "ownerReferences".to_string(),
        Value::Array(merged_owner_refs),
    );
}

#[derive(Hash, Eq, PartialEq)]
enum OwnerReferenceIdentity {
    Uid(String),
    ApiKindName(String, String, String),
}

fn owner_reference_identity(owner_ref: &Value) -> Option<OwnerReferenceIdentity> {
    if let Some(uid) = owner_ref
        .get("uid")
        .and_then(|value| value.as_str())
        .filter(|uid| !uid.trim().is_empty())
    {
        return Some(OwnerReferenceIdentity::Uid(uid.to_string()));
    }

    let api_version = owner_ref
        .get("apiVersion")
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())?;
    let kind = owner_ref
        .get("kind")
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())?;
    let name = owner_ref
        .get("name")
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())?;
    Some(OwnerReferenceIdentity::ApiKindName(
        api_version.to_string(),
        kind.to_string(),
        name.to_string(),
    ))
}

fn format_uids(owner_references: &[Value]) -> String {
    let mut uids = Vec::with_capacity(owner_references.len());
    for owner_ref in owner_references {
        if let Some(uid) = owner_ref.get("uid").and_then(|value| value.as_str()) {
            uids.push(uid);
        }
    }
    if uids.is_empty() {
        "none".to_string()
    } else {
        uids.join(",")
    }
}

fn preserve_storage_user_metadata_for_stale_put(
    api_version: &str,
    kind: &str,
    incoming: &mut Value,
    existing: &Value,
) {
    if api_version != "v1" || !matches!(kind, "PersistentVolume" | "PersistentVolumeClaim") {
        return;
    }
    merge_missing_metadata_map_entries(incoming, existing, "labels");
    merge_missing_metadata_map_entries(incoming, existing, "annotations");
}

fn merge_missing_metadata_map_entries(data: &mut Value, existing: &Value, field: &str) {
    let Some(existing_entries) = existing
        .pointer(&format!("/metadata/{field}"))
        .and_then(|value| value.as_object())
        .filter(|entries| !entries.is_empty())
    else {
        return;
    };
    let Some(metadata) = data
        .get_mut("metadata")
        .and_then(|value| value.as_object_mut())
    else {
        return;
    };
    let entries = metadata
        .entry(field.to_string())
        .or_insert_with(|| Value::Object(Default::default()));
    let Some(entries) = entries.as_object_mut() else {
        return;
    };
    for (key, value) in existing_entries {
        entries.entry(key.clone()).or_insert_with(|| value.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct Case {
        name: &'static str,
        api_version: &'static str,
        kind: &'static str,
        incoming: Value,
        existing: Value,
        assert_path: &'static str,
        expected: Value,
    }

    #[test]
    fn same_uid_stale_full_resource_policy_preserves_only_shared_policy_fields() {
        let cases = vec![
            Case {
                name: "pod_node_name",
                api_version: "v1",
                kind: "Pod",
                incoming: json!({"metadata": {"name": "pod-a"}, "spec": {}}),
                existing: json!({"spec": {"nodeName": "worker-a"}}),
                assert_path: "/spec/nodeName",
                expected: json!("worker-a"),
            },
            Case {
                name: "pod_missing_owner_refs",
                api_version: "v1",
                kind: "Pod",
                incoming: json!({"metadata": {"name": "pod-a"}}),
                existing: json!({"metadata": {"ownerReferences": [{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "rs-a", "uid": "rs-uid"}]}}),
                assert_path: "/metadata/ownerReferences/0/uid",
                expected: json!("rs-uid"),
            },
            Case {
                name: "pod_explicit_empty_owner_refs",
                api_version: "v1",
                kind: "Pod",
                incoming: json!({"metadata": {"name": "pod-a", "ownerReferences": []}}),
                existing: json!({"metadata": {"ownerReferences": [{"uid": "rs-uid"}]}}),
                assert_path: "/metadata/ownerReferences",
                expected: json!([]),
            },
            Case {
                name: "pv_labels",
                api_version: "v1",
                kind: "PersistentVolume",
                incoming: json!({"metadata": {"name": "pv-a", "labels": {"incoming": "kept"}}}),
                existing: json!({"metadata": {"labels": {"user": "live"}, "annotations": {"note": "live"}}}),
                assert_path: "/metadata/labels/user",
                expected: json!("live"),
            },
            Case {
                name: "pvc_annotations",
                api_version: "v1",
                kind: "PersistentVolumeClaim",
                incoming: json!({"metadata": {"name": "pvc-a", "namespace": "default"}}),
                existing: json!({"metadata": {"annotations": {"note": "live"}}}),
                assert_path: "/metadata/annotations/note",
                expected: json!("live"),
            },
            Case {
                name: "deployment_no_metadata_preserve",
                api_version: "apps/v1",
                kind: "Deployment",
                incoming: json!({"metadata": {"name": "deploy-a"}}),
                existing: json!({"metadata": {"labels": {"user": "live"}, "annotations": {"note": "live"}}}),
                assert_path: "/metadata/labels/user",
                expected: Value::Null,
            },
        ];

        for case in cases {
            let mut incoming = case.incoming;
            apply_same_uid_stale_full_resource_policy(
                case.api_version,
                case.kind,
                &mut incoming,
                &case.existing,
            );
            assert_eq!(
                incoming
                    .pointer(case.assert_path)
                    .cloned()
                    .unwrap_or(Value::Null),
                case.expected,
                "{}",
                case.name
            );
        }
    }
}
