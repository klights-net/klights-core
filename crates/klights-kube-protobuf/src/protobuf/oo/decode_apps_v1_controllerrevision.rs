/// ControllerRevision decoder
use crate::protobuf::*;

pub fn pb_controllerrevision_to_json(
    item: &k8s_pb::api::apps::v1::ControllerRevision,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({
        "apiVersion": "apps/v1",
        "kind": "ControllerRevision",
        "metadata": item.metadata.as_ref().map(meta_to_json).unwrap_or_default(),
    });
    if let Some(data) = &item.data
        && let Some(raw) = &data.raw
    {
        obj["data"] = serde_json::from_slice(raw)?;
    }
    if let Some(revision) = item.revision {
        obj["revision"] = json!(revision);
    }
    Ok(obj)
}

pub fn pb_controllerrevisionlist_to_json(
    list: &k8s_pb::api::apps::v1::ControllerRevisionList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "apps/v1", "kind": "ControllerRevisionList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .map(pb_controllerrevision_to_json)
        .collect::<anyhow::Result<Vec<_>>>()?;
    obj["items"] = json!(items);
    Ok(obj)
}
