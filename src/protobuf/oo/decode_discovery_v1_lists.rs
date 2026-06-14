/// EndpointSliceList decoder
use crate::protobuf::*;
pub fn pb_endpointslicelist_to_json(
    list: &k8s_pb::api::discovery::v1::EndpointSliceList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "discovery.k8s.io/v1", "kind": "EndpointSliceList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_endpointslice_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}
