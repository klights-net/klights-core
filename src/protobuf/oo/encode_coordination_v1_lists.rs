/// Encode LeaseList from JSON value to protobuf
use crate::protobuf::*;
pub fn json_leaselist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::coordination::v1::LeaseList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("LeaseList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::coordination::v1::Lease::deserialize(item)?;
            json_lease_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(k8s_pb::api::coordination::v1::LeaseList {
        metadata,
        items: pb_items,
    })
}
