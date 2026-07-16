use crate::kubelet::node_role_labels::prune_klights_managed_node_role_labels;

pub use klights_cluster_core::merge_node_status_for_update;

pub fn set_node_external_ip(node: &mut serde_json::Value, external_ip: &str) -> bool {
    let external_ip = external_ip.trim();
    if external_ip.is_empty() {
        return false;
    }

    let Some(node_object) = node.as_object_mut() else {
        return false;
    };
    let status = node_object
        .entry("status")
        .or_insert_with(|| serde_json::json!({}));
    if !status.is_object() {
        *status = serde_json::json!({});
    }
    let Some(status_object) = status.as_object_mut() else {
        return false;
    };
    set_node_external_ip_in_status(status_object, external_ip)
}

fn node_address_json(address_type: &str, address: &str) -> serde_json::Value {
    serde_json::json!({"type": address_type, "address": address})
}

fn set_node_external_ip_in_status(
    status: &mut serde_json::Map<String, serde_json::Value>,
    external_ip: &str,
) -> bool {
    let addresses = status
        .entry("addresses")
        .or_insert_with(|| serde_json::json!([]));
    if !addresses.is_array() {
        *addresses = serde_json::json!([]);
    }

    let Some(addresses) = addresses.as_array_mut() else {
        return false;
    };
    let mut changed = false;
    let mut found_external = false;
    for address in addresses.iter_mut() {
        if address.get("type").and_then(|value| value.as_str()) == Some("ExternalIP") {
            found_external = true;
            if address.get("address").and_then(|value| value.as_str()) != Some(external_ip) {
                address["address"] = serde_json::json!(external_ip);
                changed = true;
            }
        }
    }
    if !found_external {
        addresses.push(node_address_json("ExternalIP", external_ip));
        changed = true;
    }
    changed
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
            .and_then(|metadata| metadata.as_object_mut())
    {
        metadata.insert("creationTimestamp".to_string(), creation_timestamp);
    }

    if let Some(existing_spec) = existing.get("spec").cloned() {
        desired["spec"] = existing_spec;
    }
    if !desired_has_external_ip && let Some(existing_external_ip) = existing_external_ip {
        set_node_external_ip(desired, &existing_external_ip);
    }
    // `status.conditions` is co-authored: the worker posts its
    // dataplane-derived `Ready`/`NetworkUnavailable` via this forwarded update,
    // while the leader's node_lifecycle controller writes `Ready=Unknown` on
    // lease expiry via CAS. This forwarded path drops the RV precondition
    // (apply_against_latest), so an unconditionally overwriting merge lets a
    // stale worker snapshot revert the leader's fresher Unknown. Merge
    // conditions per type by `lastTransitionTime` (newest wins, K8s condition
    // contract): a stale worker snapshot has an older transition time and
    // loses, while a genuine recovery transition stamps a newer time and wins.
    merge_node_status_conditions(desired, existing);
}

pub(crate) fn preserve_existing_network_conditions(
    desired: &mut serde_json::Value,
    existing: &serde_json::Value,
) {
    let Some(desired_conditions) = desired
        .pointer_mut("/status/conditions")
        .and_then(|value| value.as_array_mut())
    else {
        return;
    };
    let Some(existing_conditions) = existing
        .pointer("/status/conditions")
        .and_then(|value| value.as_array())
    else {
        return;
    };

    for cond_type in ["Ready", "NetworkUnavailable"] {
        let Some(existing_condition) = condition_by_type(existing_conditions, cond_type).cloned()
        else {
            continue;
        };
        match desired_conditions.iter().position(|condition| {
            condition.get("type").and_then(|value| value.as_str()) == Some(cond_type)
        }) {
            Some(index) => desired_conditions[index] = existing_condition,
            None => desired_conditions.push(existing_condition),
        }
    }
}

fn merge_node_status_conditions(desired: &mut serde_json::Value, existing: &serde_json::Value) {
    let Some(incoming_status) = desired.get_mut("status") else {
        return;
    };
    klights_cluster_core::merge_node_status_for_update(incoming_status, existing);
}

fn condition_by_type<'a>(
    conditions: &'a [serde_json::Value],
    cond_type: &str,
) -> Option<&'a serde_json::Value> {
    conditions
        .iter()
        .find(|condition| condition.get("type").and_then(|value| value.as_str()) == Some(cond_type))
}

fn node_status_external_ip(node: &serde_json::Value) -> Option<&str> {
    node.pointer("/status/addresses")
        .and_then(|value| value.as_array())
        .and_then(|addresses| {
            addresses.iter().find_map(|address| {
                if address.get("type").and_then(|value| value.as_str()) == Some("ExternalIP") {
                    address.get("address").and_then(|value| value.as_str())
                } else {
                    None
                }
            })
        })
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn merge_metadata_object_field(
    desired: &mut serde_json::Value,
    field: &str,
    desired_overlay: Option<&serde_json::Value>,
) {
    let Some(overlay) = desired_overlay.and_then(|v| v.as_object()) else {
        return;
    };
    let Some(metadata) = desired.get_mut("metadata").and_then(|v| v.as_object_mut()) else {
        return;
    };
    let entry = metadata
        .entry(field.to_string())
        .or_insert_with(|| serde_json::json!({}));
    if !entry.is_object() {
        *entry = serde_json::json!({});
    }
    if let Some(entry_obj) = entry.as_object_mut() {
        for (key, value) in overlay {
            entry_obj.insert(key.clone(), value.clone());
        }
    }
}
