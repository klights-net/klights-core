use crate::protobuf::*;
pb_decode!(
    pb_priorityclass_to_json,
    k8s_pb::api::scheduling::v1::PriorityClass,
    pc,
    "scheduling.k8s.io/v1",
    "PriorityClass",
    obj,
    {
        if let Some(value) = pc.value {
            obj["value"] = json!(value);
        }
        if let Some(global_default) = pc.global_default {
            obj["globalDefault"] = json!(global_default);
        }
        if let Some(description) = &pc.description {
            obj["description"] = json!(description);
        }
        if let Some(preemption_policy) = &pc.preemption_policy {
            obj["preemptionPolicy"] = json!(preemption_policy);
        }
    }
);

/// PriorityClassList decoder
pub fn pb_priorityclasslist_to_json(
    list: &k8s_pb::api::scheduling::v1::PriorityClassList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "scheduling.k8s.io/v1", "kind": "PriorityClassList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_priorityclass_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}
