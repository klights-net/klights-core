use crate::protobuf::*;
pb_decode!(
    pb_replicationcontroller_to_json,
    k8s_pb::api::core::v1::ReplicationController,
    rc,
    "v1",
    "ReplicationController",
    obj,
    {
        if let Some(spec) = &rc.spec {
            let mut spec_obj = json!({});
            if let Some(replicas) = spec.replicas {
                spec_obj["replicas"] = json!(replicas);
            }
            spec_obj["selector"] = json!(spec.selector);
            if let Some(template) = &spec.template {
                spec_obj["template"] = pb_pod_template_spec_to_json(template);
            }
            if let Some(v) = spec.min_ready_seconds {
                spec_obj["minReadySeconds"] = json!(v);
            }
            obj["spec"] = spec_obj;
        }
        if let Some(status) = &rc.status {
            let mut status_obj = json!({});
            if let Some(v) = status.replicas {
                status_obj["replicas"] = json!(v);
            }
            if let Some(v) = status.fully_labeled_replicas {
                status_obj["fullyLabeledReplicas"] = json!(v);
            }
            if let Some(v) = status.ready_replicas {
                status_obj["readyReplicas"] = json!(v);
            }
            if let Some(v) = status.available_replicas {
                status_obj["availableReplicas"] = json!(v);
            }
            if let Some(v) = status.observed_generation {
                status_obj["observedGeneration"] = json!(v);
            }
            if !status.conditions.is_empty() {
                let conditions: Vec<Value> = status
                    .conditions
                    .iter()
                    .map(|c| {
                        let mut cond = json!({
                            "type": c.r#type.as_deref().unwrap_or(""),
                            "status": c.status.as_deref().unwrap_or("")
                        });
                        if let Some(reason) = &c.reason {
                            cond["reason"] = json!(reason);
                        }
                        if let Some(message) = &c.message {
                            cond["message"] = json!(message);
                        }
                        if let Some(t) = &c.last_transition_time {
                            cond["lastTransitionTime"] = pb_time_to_json(t);
                        }
                        cond
                    })
                    .collect();
                status_obj["conditions"] = json!(conditions);
            }
            obj["status"] = status_obj;
        }
    }
);

pb_decode!(
    pb_resourcequota_to_json,
    k8s_pb::api::core::v1::ResourceQuota,
    rq,
    "v1",
    "ResourceQuota",
    obj,
    {
        if let Some(spec) = &rq.spec {
            let mut spec_obj = json!({});
            if !spec.hard.is_empty() {
                let mut hard = json!({});
                for (key, qty) in &spec.hard {
                    hard[key] = json!(qty.string);
                }
                spec_obj["hard"] = hard;
            }
            if let Some(selector) = &spec.scope_selector {
                let mut sel_obj = json!({});
                if !selector.match_expressions.is_empty() {
                    let exprs: Vec<Value> = selector.match_expressions.iter().map(|expr| {
                    json!({"scopeName": expr.scope_name, "operator": expr.operator, "values": expr.values})
                }).collect();
                    sel_obj["matchExpressions"] = json!(exprs);
                }
                spec_obj["scopeSelector"] = sel_obj;
            }
            if !spec.scopes.is_empty() {
                spec_obj["scopes"] = json!(spec.scopes);
            }
            obj["spec"] = spec_obj;
        }
        if let Some(status) = &rq.status {
            let mut status_obj = json!({});
            if !status.hard.is_empty() {
                let mut hard = json!({});
                for (key, qty) in &status.hard {
                    hard[key] = json!(qty.string);
                }
                status_obj["hard"] = hard;
            }
            if !status.used.is_empty() {
                let mut used = json!({});
                for (key, qty) in &status.used {
                    used[key] = json!(qty.string);
                }
                status_obj["used"] = used;
            }
            obj["status"] = status_obj;
        }
    }
);

pb_decode!(
    pb_limitrange_to_json,
    k8s_pb::api::core::v1::LimitRange,
    lr,
    "v1",
    "LimitRange",
    obj,
    {
        if let Some(spec) = &lr.spec {
            let mut spec_obj = json!({});
            if !spec.limits.is_empty() {
                let limits: Vec<Value> = spec
                    .limits
                    .iter()
                    .map(|item| {
                        let mut limit_obj = json!({});
                        if let Some(typ) = &item.r#type {
                            limit_obj["type"] = json!(typ);
                        }
                        let convert_qty_map = |map: &std::collections::BTreeMap<
                            String,
                            k8s_pb::apimachinery::pkg::api::resource::Quantity,
                        >| {
                            if map.is_empty() {
                                return None;
                            }
                            let mut result = json!({});
                            for (k, v) in map {
                                result[k] = json!(v.string);
                            }
                            Some(result)
                        };
                        if let Some(v) = convert_qty_map(&item.default) {
                            limit_obj["default"] = v;
                        }
                        if let Some(v) = convert_qty_map(&item.default_request) {
                            limit_obj["defaultRequest"] = v;
                        }
                        if let Some(v) = convert_qty_map(&item.max) {
                            limit_obj["max"] = v;
                        }
                        if let Some(v) = convert_qty_map(&item.min) {
                            limit_obj["min"] = v;
                        }
                        if let Some(v) = convert_qty_map(&item.max_limit_request_ratio) {
                            limit_obj["maxLimitRequestRatio"] = v;
                        }
                        limit_obj
                    })
                    .collect();
                spec_obj["limits"] = json!(limits);
            }
            obj["spec"] = spec_obj;
        }
    }
);

/// NodeList decoder
pub fn pb_nodelist_to_json(list: &k8s_pb::api::core::v1::NodeList) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "v1", "kind": "NodeList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_node_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}

/// PodList decoder
pub fn pb_podlist_to_json(list: &k8s_pb::api::core::v1::PodList) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "v1", "kind": "PodList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_pod_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}

/// PodTemplateList decoder
pub fn pb_podtemplatelist_to_json(
    list: &k8s_pb::api::core::v1::PodTemplateList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "v1", "kind": "PodTemplateList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_podtemplate_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}

/// NamespaceList decoder
pub fn pb_namespacelist_to_json(
    list: &k8s_pb::api::core::v1::NamespaceList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "v1", "kind": "NamespaceList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_namespace_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}

/// ConfigMapList decoder
pub fn pb_configmaplist_to_json(
    list: &k8s_pb::api::core::v1::ConfigMapList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "v1", "kind": "ConfigMapList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_configmap_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}

/// SecretList decoder
pub fn pb_secretlist_to_json(list: &k8s_pb::api::core::v1::SecretList) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "v1", "kind": "SecretList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_secret_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}

/// ServiceList decoder
pub fn pb_servicelist_to_json(list: &k8s_pb::api::core::v1::ServiceList) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "v1", "kind": "ServiceList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_service_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}

/// ServiceAccountList decoder
pub fn pb_serviceaccountlist_to_json(
    list: &k8s_pb::api::core::v1::ServiceAccountList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "v1", "kind": "ServiceAccountList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_serviceaccount_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}

/// EndpointsList decoder
pub fn pb_endpointslist_to_json(
    list: &k8s_pb::api::core::v1::EndpointsList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "v1", "kind": "EndpointsList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_endpoints_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}

/// PersistentVolumeList decoder
pub fn pb_persistentvolumelist_to_json(
    list: &k8s_pb::api::core::v1::PersistentVolumeList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "v1", "kind": "PersistentVolumeList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_persistentvolume_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}

/// PersistentVolumeClaimList decoder
pub fn pb_persistentvolumeclaimlist_to_json(
    list: &k8s_pb::api::core::v1::PersistentVolumeClaimList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "v1", "kind": "PersistentVolumeClaimList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_persistentvolumeclaim_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}

/// EventList (core/v1) decoder
pub fn pb_eventlist_to_json(list: &k8s_pb::api::core::v1::EventList) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "v1", "kind": "EventList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_event_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}

/// EventList (events.k8s.io/v1) decoder
pub fn pb_events_v1_eventlist_to_json(
    list: &k8s_pb::api::events::v1::EventList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "events.k8s.io/v1", "kind": "EventList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_events_v1_event_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}

pub fn pb_resourcequotalist_to_json(
    list: &k8s_pb::api::core::v1::ResourceQuotaList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "v1", "kind": "ResourceQuotaList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_resourcequota_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}

/// LimitRangeList decoder
pub fn pb_limitrangelist_to_json(
    list: &k8s_pb::api::core::v1::LimitRangeList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "v1", "kind": "LimitRangeList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_limitrange_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}
