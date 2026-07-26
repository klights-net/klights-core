//! Pure Kubernetes Node JSON projection and merge policy.

const MANAGED_ROLE_LABELS: [&str; 6] = [
    "node-role.kubernetes.io/controlplane",
    "node-role.kubernetes.io/control-plane",
    "node-role.kubernetes.io/master",
    "node-role.kubernetes.io/leader",
    "node-role.kubernetes.io/replica",
    "node-role.kubernetes.io/worker",
];

pub use crate::status::set_node_external_ip;

pub fn set_node_external_ip_from_dataplane_annotation(node: &mut serde_json::Value) -> bool {
    let endpoint = node
        .pointer("/metadata/annotations")
        .and_then(serde_json::Value::as_object)
        .and_then(|annotations| {
            annotations
                .get(klights_types::DATAPLANE_ENDPOINT_ANNOTATION)
                .and_then(serde_json::Value::as_str)
        })
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    endpoint
        .as_deref()
        .is_some_and(|endpoint| set_node_external_ip(node, endpoint))
}

pub fn set_node_pod_cidr(node: &mut serde_json::Value, pod_cidr: &str) -> bool {
    let pod_cidr = pod_cidr.trim();
    if pod_cidr.is_empty() {
        return false;
    }
    let Some(node) = node.as_object_mut() else {
        return false;
    };
    let spec = node.entry("spec").or_insert_with(|| serde_json::json!({}));
    if !spec.is_object() {
        *spec = serde_json::json!({});
    }
    let Some(spec) = spec.as_object_mut() else {
        return false;
    };

    let mut changed = set_json_string_field(spec, "podCIDR", pod_cidr);
    let desired = serde_json::json!([pod_cidr]);
    if spec.get("podCIDRs") != Some(&desired) {
        spec.insert("podCIDRs".to_string(), desired);
        changed = true;
    }
    changed
}

pub fn prune_klights_managed_node_role_labels(node: &mut serde_json::Value) {
    let Some(labels) = node
        .pointer_mut("/metadata/labels")
        .and_then(serde_json::Value::as_object_mut)
    else {
        return;
    };
    for key in MANAGED_ROLE_LABELS {
        labels.remove(key);
    }
}

pub fn merge_existing_node_mutable_fields(
    desired: &mut serde_json::Value,
    existing: &serde_json::Value,
) {
    let desired_labels = desired.pointer("/metadata/labels").cloned();
    let desired_annotations = desired.pointer("/metadata/annotations").cloned();
    let desired_creation_timestamp = desired.pointer("/metadata/creationTimestamp").cloned();
    let desired_has_external_ip = node_status_external_ip(desired).is_some();
    let existing_external_ip = node_status_external_ip(existing).map(str::to_string);

    if let Some(existing_metadata) = existing.get("metadata").cloned() {
        desired["metadata"] = existing_metadata;
    }
    prune_klights_managed_node_role_labels(desired);
    merge_metadata_object_field(desired, "labels", desired_labels.as_ref());
    merge_metadata_object_field(desired, "annotations", desired_annotations.as_ref());
    if let Some(creation_timestamp) = desired_creation_timestamp
        && let Some(metadata) = desired
            .get_mut("metadata")
            .and_then(serde_json::Value::as_object_mut)
    {
        metadata.insert("creationTimestamp".to_string(), creation_timestamp);
    }
    if let Some(existing_spec) = existing.get("spec").cloned() {
        desired["spec"] = existing_spec;
    }
    if !desired_has_external_ip && let Some(existing_external_ip) = existing_external_ip {
        set_node_external_ip(desired, &existing_external_ip);
    }
    if let Some(incoming_status) = desired.get_mut("status") {
        crate::merge_node_status_for_update(incoming_status, existing);
    }
}

fn node_status_external_ip(node: &serde_json::Value) -> Option<&str> {
    node.pointer("/status/addresses")
        .and_then(serde_json::Value::as_array)
        .and_then(|addresses| {
            addresses.iter().find_map(|address| {
                (address.get("type").and_then(serde_json::Value::as_str) == Some("ExternalIP"))
                    .then(|| address.get("address").and_then(serde_json::Value::as_str))
                    .flatten()
            })
        })
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn merge_metadata_object_field(
    desired: &mut serde_json::Value,
    field: &str,
    overlay: Option<&serde_json::Value>,
) {
    let Some(overlay) = overlay.and_then(serde_json::Value::as_object) else {
        return;
    };
    let Some(metadata) = desired
        .get_mut("metadata")
        .and_then(serde_json::Value::as_object_mut)
    else {
        return;
    };
    let entry = metadata
        .entry(field.to_string())
        .or_insert_with(|| serde_json::json!({}));
    if !entry.is_object() {
        *entry = serde_json::json!({});
    }
    if let Some(entry) = entry.as_object_mut() {
        for (key, value) in overlay {
            entry.insert(key.clone(), value.clone());
        }
    }
}

fn set_json_string_field(
    object: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    value: &str,
) -> bool {
    if object.get(key).and_then(serde_json::Value::as_str) == Some(value) {
        return false;
    }
    object.insert(
        key.to_string(),
        serde_json::Value::String(value.to_string()),
    );
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routing_projection_is_idempotent() {
        let mut node = serde_json::json!({});
        assert!(set_node_pod_cidr(&mut node, "10.42.1.0/24"));
        assert!(set_node_external_ip(&mut node, "192.0.2.10"));
        assert!(!set_node_pod_cidr(&mut node, "10.42.1.0/24"));
        assert!(!set_node_external_ip(&mut node, "192.0.2.10"));
    }

    #[test]
    fn mutable_merge_preserves_existing_spec_and_prunes_stale_managed_roles() {
        let existing = serde_json::json!({
            "metadata": {
                "labels": {
                    "node-role.kubernetes.io/worker": "",
                    "user": "kept"
                }
            },
            "spec": {"podCIDR": "10.42.1.0/24"}
        });
        let mut desired = serde_json::json!({
            "metadata": {
                "labels": {
                    "node-role.kubernetes.io/controlplane": "",
                    "desired": "yes"
                }
            },
            "spec": {"podCIDR": "wrong"}
        });
        merge_existing_node_mutable_fields(&mut desired, &existing);
        assert_eq!(
            desired.pointer("/spec/podCIDR"),
            Some(&serde_json::json!("10.42.1.0/24"))
        );
        assert_eq!(
            desired.pointer("/metadata/labels/user"),
            Some(&serde_json::json!("kept"))
        );
        assert_eq!(
            desired.pointer("/metadata/labels/node-role.kubernetes.io~1controlplane"),
            Some(&serde_json::json!(""))
        );
        assert!(
            desired
                .pointer("/metadata/labels/node-role.kubernetes.io~1worker")
                .is_none()
        );
    }
}
