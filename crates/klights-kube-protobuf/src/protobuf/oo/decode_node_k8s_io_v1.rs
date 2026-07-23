use crate::protobuf::*;
pb_decode!(
    pb_runtimeclass_to_json,
    k8s_pb::api::node::v1::RuntimeClass,
    rc,
    "node.k8s.io/v1",
    "RuntimeClass",
    obj,
    {
        if let Some(handler) = &rc.handler {
            obj["handler"] = json!(handler);
        }
        if let Some(overhead) = &rc.overhead {
            let mut pod_fixed = serde_json::Map::new();
            for (k, v) in &overhead.pod_fixed {
                pod_fixed.insert(k.clone(), json!(v.string));
            }
            if !pod_fixed.is_empty() {
                obj["overhead"] = json!({"podFixed": pod_fixed});
            }
        }
        if let Some(scheduling) = &rc.scheduling {
            let mut sched = serde_json::Map::new();
            if !scheduling.node_selector.is_empty() {
                sched.insert("nodeSelector".to_string(), json!(scheduling.node_selector));
            }
            if !scheduling.tolerations.is_empty() {
                let tolerations: Vec<Value> = scheduling
                    .tolerations
                    .iter()
                    .map(|t| {
                        let mut tol = serde_json::Map::new();
                        if let Some(key) = &t.key {
                            tol.insert("key".to_string(), json!(key));
                        }
                        if let Some(operator) = &t.operator {
                            tol.insert("operator".to_string(), json!(operator));
                        }
                        if let Some(value) = &t.value {
                            tol.insert("value".to_string(), json!(value));
                        }
                        if let Some(effect) = &t.effect {
                            tol.insert("effect".to_string(), json!(effect));
                        }
                        if let Some(secs) = t.toleration_seconds {
                            tol.insert("tolerationSeconds".to_string(), json!(secs));
                        }
                        Value::Object(tol)
                    })
                    .collect();
                sched.insert("tolerations".to_string(), Value::Array(tolerations));
            }
            if !sched.is_empty() {
                obj["scheduling"] = Value::Object(sched);
            }
        }
    }
);

pub fn pb_runtimeclasslist_to_json(
    list: &k8s_pb::api::node::v1::RuntimeClassList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "node.k8s.io/v1", "kind": "RuntimeClassList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_runtimeclass_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}
