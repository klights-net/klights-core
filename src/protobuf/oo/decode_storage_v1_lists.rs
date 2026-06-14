use crate::protobuf::*;
pb_decode!(
    pb_storageclass_to_json,
    k8s_pb::api::storage::v1::StorageClass,
    sc,
    "storage.k8s.io/v1",
    "StorageClass",
    obj,
    {
        if let Some(provisioner) = &sc.provisioner {
            obj["provisioner"] = json!(provisioner);
        }
        if !sc.parameters.is_empty() {
            obj["parameters"] = json!(sc.parameters);
        }
        if let Some(reclaim_policy) = &sc.reclaim_policy {
            obj["reclaimPolicy"] = json!(reclaim_policy);
        }
        if !sc.mount_options.is_empty() {
            obj["mountOptions"] = json!(sc.mount_options);
        }
        if let Some(allow_volume_expansion) = sc.allow_volume_expansion {
            obj["allowVolumeExpansion"] = json!(allow_volume_expansion);
        }
        if let Some(volume_binding_mode) = &sc.volume_binding_mode {
            obj["volumeBindingMode"] = json!(volume_binding_mode);
        }
    }
);

pb_decode!(
    pb_csistoragecapacity_to_json,
    k8s_pb::api::storage::v1::CSIStorageCapacity,
    cap,
    "storage.k8s.io/v1",
    "CSIStorageCapacity",
    obj,
    {
        if let Some(sc_name) = &cap.storage_class_name {
            obj["storageClassName"] = json!(sc_name);
        }
        if let Some(capacity) = &cap.capacity
            && let Some(s) = &capacity.string
        {
            obj["capacity"] = json!(s);
        }
        if let Some(max_vol_size) = &cap.maximum_volume_size
            && let Some(s) = &max_vol_size.string
        {
            obj["maximumVolumeSize"] = json!(s);
        }
        if let Some(node_topology) = &cap.node_topology {
            let mut sel = json!({});
            if !node_topology.match_labels.is_empty() {
                sel["matchLabels"] = json!(node_topology.match_labels);
            }
            obj["nodeTopology"] = sel;
        }
    }
);

pb_decode!(
    pb_csinode_to_json,
    k8s_pb::api::storage::v1::CSINode,
    node,
    "storage.k8s.io/v1",
    "CSINode",
    obj,
    {
        if let Some(spec) = &node.spec {
            let drivers: Vec<Value> = spec
                .drivers
                .iter()
                .map(|d| {
                    let mut driver_obj = json!({});
                    if let Some(name) = &d.name {
                        driver_obj["name"] = json!(name);
                    }
                    if let Some(node_id) = &d.node_id {
                        driver_obj["nodeID"] = json!(node_id);
                    }
                    if !d.topology_keys.is_empty() {
                        driver_obj["topologyKeys"] = json!(d.topology_keys);
                    }
                    driver_obj
                })
                .collect();
            obj["spec"] = json!({"drivers": drivers});
        }
    }
);

pb_decode!(
    pb_csidriver_to_json,
    k8s_pb::api::storage::v1::CSIDriver,
    driver,
    "storage.k8s.io/v1",
    "CSIDriver",
    obj,
    {
        if let Some(spec) = &driver.spec {
            let mut spec_obj = json!({});
            if let Some(v) = spec.attach_required {
                spec_obj["attachRequired"] = json!(v);
            }
            if let Some(v) = spec.pod_info_on_mount {
                spec_obj["podInfoOnMount"] = json!(v);
            }
            if !spec.volume_lifecycle_modes.is_empty() {
                spec_obj["volumeLifecycleModes"] = json!(spec.volume_lifecycle_modes);
            }
            if let Some(v) = spec.storage_capacity {
                spec_obj["storageCapacity"] = json!(v);
            }
            if let Some(v) = &spec.fs_group_policy {
                spec_obj["fsGroupPolicy"] = json!(v);
            }
            if let Some(v) = spec.requires_republish {
                spec_obj["requiresRepublish"] = json!(v);
            }
            if let Some(v) = spec.se_linux_mount {
                spec_obj["seLinuxMount"] = json!(v);
            }
            obj["spec"] = spec_obj;
        }
    }
);

pb_decode!(
    pb_volumeattachment_to_json,
    k8s_pb::api::storage::v1::VolumeAttachment,
    va,
    "storage.k8s.io/v1",
    "VolumeAttachment",
    obj,
    {
        if let Some(spec) = &va.spec {
            let mut spec_obj = json!({});
            if let Some(attacher) = &spec.attacher {
                spec_obj["attacher"] = json!(attacher);
            }
            if let Some(node_name) = &spec.node_name {
                spec_obj["nodeName"] = json!(node_name);
            }
            if let Some(source) = &spec.source {
                let mut source_obj = json!({});
                if let Some(pv_name) = &source.persistent_volume_name {
                    source_obj["persistentVolumeName"] = json!(pv_name);
                }
                if let Some(_inline_vol) = &source.inline_volume_spec {
                    source_obj["inlineVolumeSpec"] = json!({"kind": "PersistentVolumeSpec"});
                }
                spec_obj["source"] = source_obj;
            }
            obj["spec"] = spec_obj;
        }
        if let Some(status) = &va.status {
            let mut status_obj = json!({});
            if let Some(attached) = status.attached {
                status_obj["attached"] = json!(attached);
            }
            if !status.attachment_metadata.is_empty() {
                status_obj["attachmentMetadata"] = json!(status.attachment_metadata);
            }
            obj["status"] = status_obj;
        }
    }
);

/// StorageClassList decoder
pub fn pb_storageclasslist_to_json(
    list: &k8s_pb::api::storage::v1::StorageClassList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "storage.k8s.io/v1", "kind": "StorageClassList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_storageclass_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}

/// CSINodeList decoder
pub fn pb_csinodelist_to_json(
    list: &k8s_pb::api::storage::v1::CSINodeList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "storage.k8s.io/v1", "kind": "CSINodeList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_csinode_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}

/// CSIDriverList decoder
pub fn pb_csidriverlist_to_json(
    list: &k8s_pb::api::storage::v1::CSIDriverList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "storage.k8s.io/v1", "kind": "CSIDriverList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_csidriver_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}

/// VolumeAttachmentList decoder
pub fn pb_volumeattachmentlist_to_json(
    list: &k8s_pb::api::storage::v1::VolumeAttachmentList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "storage.k8s.io/v1", "kind": "VolumeAttachmentList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_volumeattachment_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}
