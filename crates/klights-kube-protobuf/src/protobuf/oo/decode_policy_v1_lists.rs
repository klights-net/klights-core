/// PodDisruptionBudgetList decoder
use crate::protobuf::*;
pub fn pb_pdblist_to_json(
    list: &k8s_pb::api::policy::v1::PodDisruptionBudgetList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "policy.k8s.io/v1", "kind": "PodDisruptionBudgetList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_poddisruptionbudget_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}
