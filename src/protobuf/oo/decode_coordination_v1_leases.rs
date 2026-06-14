/// LeaseList decoder
use crate::protobuf::*;
pub fn pb_leaselist_to_json(
    list: &k8s_pb::api::coordination::v1::LeaseList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "coordination.k8s.io/v1", "kind": "LeaseList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_lease_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}
