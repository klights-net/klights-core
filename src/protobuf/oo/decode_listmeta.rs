use crate::protobuf::*;
pub fn pb_listmeta_to_json(
    metadata: Option<&k8s_pb::apimachinery::pkg::apis::meta::v1::ListMeta>,
) -> Value {
    use serde_json::json;
    let mut meta_obj = json!({"resourceVersion": "0"});
    if let Some(metadata) = metadata {
        if let Some(rv) = &metadata.resource_version {
            meta_obj["resourceVersion"] = json!(rv);
        }
        if let Some(continue_token) = &metadata.r#continue {
            meta_obj["continue"] = json!(continue_token);
        }
        if let Some(remaining) = metadata.remaining_item_count {
            meta_obj["remainingItemCount"] = json!(remaining);
        }
    }
    meta_obj
}
