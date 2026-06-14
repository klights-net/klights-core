/// Encode PodDisruptionBudgetList from JSON value to protobuf
use crate::protobuf::*;
pub fn json_pdblist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::policy::v1::PodDisruptionBudgetList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("PodDisruptionBudgetList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::policy::v1::PodDisruptionBudget::deserialize(item)?;
            Ok(json_pdb_to_pb(&openapi))
        })
        .collect::<Result<Vec<_>, anyhow::Error>>()?;

    Ok(k8s_pb::api::policy::v1::PodDisruptionBudgetList {
        metadata,
        items: pb_items,
    })
}
