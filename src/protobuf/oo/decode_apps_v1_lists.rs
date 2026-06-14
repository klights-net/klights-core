/// DeploymentList decoder
use crate::protobuf::*;
pub fn pb_deploymentlist_to_json(
    list: &k8s_pb::api::apps::v1::DeploymentList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "apps/v1", "kind": "DeploymentList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_deployment_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}

/// ReplicaSetList decoder
pub fn pb_replicasetlist_to_json(
    list: &k8s_pb::api::apps::v1::ReplicaSetList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "apps/v1", "kind": "ReplicaSetList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_replicaset_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}

/// StatefulSetList decoder
pub fn pb_statefulsetlist_to_json(
    list: &k8s_pb::api::apps::v1::StatefulSetList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "apps/v1", "kind": "StatefulSetList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_statefulset_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}

/// DaemonSetList decoder
pub fn pb_daemonsetlist_to_json(
    list: &k8s_pb::api::apps::v1::DaemonSetList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "apps/v1", "kind": "DaemonSetList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_daemonset_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}
