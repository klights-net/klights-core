use crate::kubelet::node_role_labels::prune_klights_managed_node_role_labels;

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

/// Merge an incoming Node `/status` update with the live Node's co-authored
/// conditions, preserving fresher leader transitions while allowing newer
/// worker dataplane recovery transitions to apply.
pub fn merge_node_status_for_update(
    incoming_status: &mut serde_json::Value,
    existing: &serde_json::Value,
) {
    let mut desired = serde_json::json!({ "status": incoming_status.clone() });
    merge_node_status_conditions(&mut desired, existing);
    if let Some(status) = desired.get("status").cloned() {
        *incoming_status = status;
    }
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
    let Some(desired_conditions) = desired
        .pointer_mut("/status/conditions")
        .and_then(|value| value.as_array_mut())
    else {
        return;
    };
    let Some(existing_conditions) = existing
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
    else {
        return;
    };
    let desired_network_pair_should_replace =
        network_condition_pair_should_replace(desired_conditions, existing_conditions);
    let existing_network_pair_should_preserve =
        network_condition_pair_should_preserve_existing(desired_conditions, existing_conditions);
    for existing_cond in existing_conditions {
        let Some(cond_type) = existing_cond.get("type").and_then(|v| v.as_str()) else {
            continue;
        };
        match desired_conditions
            .iter()
            .position(|c| c.get("type").and_then(|v| v.as_str()) == Some(cond_type))
        {
            None => {
                desired_conditions.push(existing_cond.clone());
            }
            Some(idx) => {
                if desired_network_pair_should_replace && is_network_condition_type(cond_type) {
                    continue;
                }
                if existing_network_pair_should_preserve && is_network_condition_type(cond_type) {
                    desired_conditions[idx] = existing_cond.clone();
                    continue;
                }
                if !condition_is_strictly_newer(&desired_conditions[idx], existing_cond) {
                    desired_conditions[idx] = existing_cond.clone();
                }
            }
        }
    }
}

fn is_network_condition_type(cond_type: &str) -> bool {
    matches!(cond_type, "Ready" | "NetworkUnavailable")
}

fn network_condition_pair_has_newer_transition(
    desired_conditions: &[serde_json::Value],
    existing_conditions: &[serde_json::Value],
) -> bool {
    let desired_ready = condition_by_type(desired_conditions, "Ready");
    let desired_network = condition_by_type(desired_conditions, "NetworkUnavailable");
    let existing_ready = condition_by_type(existing_conditions, "Ready");
    let existing_network = condition_by_type(existing_conditions, "NetworkUnavailable");

    if !desired_network_pair_is_coherent(desired_ready, desired_network) {
        return false;
    }

    let desired_ready_is_newer = desired_ready.is_some_and(|desired| {
        existing_ready.is_some_and(|existing| condition_is_strictly_newer(desired, existing))
    });
    if desired_ready_is_newer {
        return true;
    }

    network_condition_pair_is_healthy(desired_ready, desired_network)
        && desired_network.is_some_and(|desired| {
            existing_network.is_some_and(|existing| condition_is_strictly_newer(desired, existing))
        })
}

fn network_condition_pair_should_replace(
    desired_conditions: &[serde_json::Value],
    existing_conditions: &[serde_json::Value],
) -> bool {
    if network_condition_pair_has_newer_transition(desired_conditions, existing_conditions) {
        return true;
    }

    let desired_ready = condition_by_type(desired_conditions, "Ready");
    let desired_network = condition_by_type(desired_conditions, "NetworkUnavailable");
    let existing_ready = condition_by_type(existing_conditions, "Ready");
    let existing_network = condition_by_type(existing_conditions, "NetworkUnavailable");

    network_condition_pair_is_healthy(desired_ready, desired_network)
        && network_condition_pair_is_unavailable(existing_ready, existing_network)
        && network_condition_pair_timestamps_tie(
            desired_ready,
            desired_network,
            existing_ready,
            existing_network,
        )
}

fn network_condition_pair_should_preserve_existing(
    desired_conditions: &[serde_json::Value],
    existing_conditions: &[serde_json::Value],
) -> bool {
    let desired_ready = condition_by_type(desired_conditions, "Ready");
    let desired_network = condition_by_type(desired_conditions, "NetworkUnavailable");
    let existing_ready = condition_by_type(existing_conditions, "Ready");
    let existing_network = condition_by_type(existing_conditions, "NetworkUnavailable");

    desired_network_pair_is_coherent(desired_ready, desired_network)
        && desired_network_pair_is_coherent(existing_ready, existing_network)
        && !network_condition_pair_should_replace(desired_conditions, existing_conditions)
}

fn desired_network_pair_is_coherent(
    ready: Option<&serde_json::Value>,
    network: Option<&serde_json::Value>,
) -> bool {
    matches!(
        (condition_status(ready), condition_status(network)),
        (Some("True"), Some("False")) | (Some("False"), Some("True"))
    )
}

fn network_condition_pair_is_healthy(
    ready: Option<&serde_json::Value>,
    network: Option<&serde_json::Value>,
) -> bool {
    matches!(
        (condition_status(ready), condition_status(network)),
        (Some("True"), Some("False"))
    )
}

fn network_condition_pair_is_unavailable(
    ready: Option<&serde_json::Value>,
    network: Option<&serde_json::Value>,
) -> bool {
    matches!(
        (condition_status(ready), condition_status(network)),
        (Some("False"), Some("True"))
    )
}

fn network_condition_pair_timestamps_tie(
    desired_ready: Option<&serde_json::Value>,
    desired_network: Option<&serde_json::Value>,
    existing_ready: Option<&serde_json::Value>,
    existing_network: Option<&serde_json::Value>,
) -> bool {
    condition_transition_time(desired_ready).is_some()
        && condition_transition_time(desired_ready) == condition_transition_time(existing_ready)
        && condition_transition_time(desired_network).is_some()
        && condition_transition_time(desired_network) == condition_transition_time(existing_network)
}

fn condition_by_type<'a>(
    conditions: &'a [serde_json::Value],
    cond_type: &str,
) -> Option<&'a serde_json::Value> {
    conditions
        .iter()
        .find(|condition| condition.get("type").and_then(|value| value.as_str()) == Some(cond_type))
}

fn condition_status(condition: Option<&serde_json::Value>) -> Option<&str> {
    condition
        .and_then(|condition| condition.get("status"))
        .and_then(|value| value.as_str())
}

fn condition_transition_time(condition: Option<&serde_json::Value>) -> Option<&str> {
    condition
        .and_then(|condition| condition.get("lastTransitionTime"))
        .and_then(|value| value.as_str())
}

fn condition_is_strictly_newer(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    let a_time = a.get("lastTransitionTime").and_then(|v| v.as_str());
    let b_time = b.get("lastTransitionTime").and_then(|v| v.as_str());
    match (a_time, b_time) {
        (Some(a_str), Some(b_str)) => match (parse_rfc3339_utc(a_str), parse_rfc3339_utc(b_str)) {
            (Some(a_dt), Some(b_dt)) => a_dt > b_dt,
            _ => a_str > b_str,
        },
        // Legacy or externally-created Node conditions may omit
        // lastTransitionTime. A timestamped transition has positive ordering
        // evidence and must supersede such a condition; otherwise controllers
        // can never move the Node to a new state.
        (Some(_), None) => true,
        (None, Some(_) | None) => false,
    }
}

fn parse_rfc3339_utc(s: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.with_timezone(&chrono::Utc))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamped_node_condition_replaces_legacy_condition_without_timestamp() {
        let existing = serde_json::json!({
            "status": {
                "conditions": [{
                    "type": "Ready",
                    "status": "True",
                    "reason": "E2E"
                }]
            }
        });
        let mut incoming = serde_json::json!({
            "conditions": [{
                "type": "Ready",
                "status": "Unknown",
                "reason": "NodeStatusUnknown",
                "lastTransitionTime": "2026-07-14T11:38:22Z"
            }]
        });

        merge_node_status_for_update(&mut incoming, &existing);

        assert_eq!(
            incoming
                .pointer("/conditions/0/status")
                .and_then(|v| v.as_str()),
            Some("Unknown"),
            "a timestamped controller transition must supersede a legacy condition with no timestamp"
        );
    }
}
