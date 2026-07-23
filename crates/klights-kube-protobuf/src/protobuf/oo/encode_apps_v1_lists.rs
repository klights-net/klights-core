/// Encode ControllerRevisionList from JSON value to protobuf
use crate::protobuf::*;
pub fn json_controllerrevisionlist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::apps::v1::ControllerRevisionList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("ControllerRevisionList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::apps::v1::ControllerRevision::deserialize(item)?;
            json_controllerrevision_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(k8s_pb::api::apps::v1::ControllerRevisionList {
        metadata,
        items: pb_items,
    })
}

/// Encode ControllerRevision to protobuf
pub fn json_controllerrevision_to_pb(
    cr: &k8s_openapi::api::apps::v1::ControllerRevision,
) -> anyhow::Result<k8s_pb::api::apps::v1::ControllerRevision> {
    use k8s_pb::api::apps::v1 as appsv1;
    use k8s_pb::apimachinery::pkg::runtime::RawExtension;
    Ok(appsv1::ControllerRevision {
        metadata: Some(json_meta_to_pb(&cr.metadata)),
        revision: Some(cr.revision),
        data: cr.data.as_ref().map(|raw| RawExtension {
            raw: Some(raw.0.to_string().into_bytes()),
        }),
    })
}
