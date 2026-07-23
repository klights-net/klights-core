/// Encode CSIDriverList from JSON value to protobuf
use crate::protobuf::*;
pub fn json_csidriverlist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::storage::v1::CSIDriverList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });
    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("CSIDriverList missing items array"))?;
    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::storage::v1::CSIDriver::deserialize(item)?;
            Ok(json_csidriver_to_pb(&openapi))
        })
        .collect::<Result<Vec<_>, anyhow::Error>>()?;
    Ok(k8s_pb::api::storage::v1::CSIDriverList {
        metadata,
        items: pb_items,
    })
}

/// Encode CSIStorageCapacityList from JSON value to protobuf
pub fn json_csistoragecapacitylist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::storage::v1::CSIStorageCapacityList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });
    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("CSIStorageCapacityList missing items array"))?;
    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::storage::v1::CSIStorageCapacity::deserialize(item)?;
            Ok(json_csistoragecapacity_to_pb(&openapi))
        })
        .collect::<Result<Vec<_>, anyhow::Error>>()?;
    Ok(k8s_pb::api::storage::v1::CSIStorageCapacityList {
        metadata,
        items: pb_items,
    })
}

/// Encode VolumeAttachmentList from JSON value to protobuf
pub fn json_volumeattachmentlist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::storage::v1::VolumeAttachmentList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });
    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("VolumeAttachmentList missing items array"))?;
    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::storage::v1::VolumeAttachment::deserialize(item)?;
            Ok(json_volumeattachment_to_pb(&openapi))
        })
        .collect::<Result<Vec<_>, anyhow::Error>>()?;
    Ok(k8s_pb::api::storage::v1::VolumeAttachmentList {
        metadata,
        items: pb_items,
    })
}
