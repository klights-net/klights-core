pub use klights_cluster_core::{
    merge_existing_node_mutable_fields, merge_node_status_for_update, set_node_external_ip,
};

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

fn condition_by_type<'a>(
    conditions: &'a [serde_json::Value],
    cond_type: &str,
) -> Option<&'a serde_json::Value> {
    conditions
        .iter()
        .find(|condition| condition.get("type").and_then(|value| value.as_str()) == Some(cond_type))
}
