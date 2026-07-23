/// Encode RuntimeClassList from JSON value to protobuf
use crate::protobuf::*;
pub fn json_runtimeclasslist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::node::v1::RuntimeClassList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("RuntimeClassList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::node::v1::RuntimeClass::deserialize(item)?;
            Ok(json_runtimeclass_to_pb(&openapi))
        })
        .collect::<Result<Vec<_>, anyhow::Error>>()?;

    Ok(k8s_pb::api::node::v1::RuntimeClassList {
        metadata,
        items: pb_items,
    })
}
