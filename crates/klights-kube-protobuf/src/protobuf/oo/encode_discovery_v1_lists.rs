/// Encode EndpointSliceList from JSON value to protobuf
use crate::protobuf::*;
pub fn json_endpointslicelist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::discovery::v1::EndpointSliceList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("EndpointSliceList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::discovery::v1::EndpointSlice::deserialize(item)?;
            Ok(json_endpointslice_to_pb(&openapi))
        })
        .collect::<Result<Vec<_>, anyhow::Error>>()?;

    Ok(k8s_pb::api::discovery::v1::EndpointSliceList {
        metadata,
        items: pb_items,
    })
}
