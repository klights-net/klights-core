use crate::protobuf::*;
pub fn pb_flowschema_to_json(
    fs: &k8s_pb::api::flowcontrol::v1::FlowSchema,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "flowcontrol.apiserver.k8s.io/v1", "kind": "FlowSchema"});
    if let Some(metadata) = &fs.metadata {
        obj["metadata"] = meta_to_json(metadata);
    }
    if let Some(spec) = &fs.spec {
        let mut spec_obj = json!({});
        if let Some(plc) = &spec.priority_level_configuration
            && let Some(name) = &plc.name
        {
            spec_obj["priorityLevelConfiguration"] = json!({"name": name});
        }
        if let Some(mp) = spec.matching_precedence {
            spec_obj["matchingPrecedence"] = json!(mp);
        }
        if let Some(dm) = &spec.distinguisher_method
            && let Some(t) = &dm.r#type
        {
            spec_obj["distinguisherMethod"] = json!({"type": t});
        }
        if !spec.rules.is_empty() {
            let rules: Vec<Value> = spec
                .rules
                .iter()
                .map(|rule| {
                    let subjects: Vec<Value> = rule
                        .subjects
                        .iter()
                        .map(|s| {
                            let mut subj = json!({});
                            if let Some(k) = &s.kind {
                                subj["kind"] = json!(k);
                            }
                            if let Some(u) = &s.user
                                && let Some(n) = &u.name
                            {
                                subj["user"] = json!({"name": n});
                            }
                            if let Some(g) = &s.group
                                && let Some(n) = &g.name
                            {
                                subj["group"] = json!({"name": n});
                            }
                            if let Some(sa) = &s.service_account {
                                subj["serviceAccount"] =
                                    json!({"namespace": sa.namespace, "name": sa.name});
                            }
                            subj
                        })
                        .collect();
                    let resource_rules: Vec<Value> = rule
                        .resource_rules
                        .iter()
                        .map(|r| {
                            json!({
                                "verbs": r.verbs,
                                "apiGroups": r.api_groups,
                                "resources": r.resources,
                                "clusterScope": r.cluster_scope,
                                "namespaces": r.namespaces,
                            })
                        })
                        .collect();
                    let non_resource_rules: Vec<Value> = rule
                        .non_resource_rules
                        .iter()
                        .map(|r| {
                            json!({
                                "verbs": r.verbs,
                                "nonResourceURLs": r.non_resource_ur_ls,
                            })
                        })
                        .collect();
                    json!({
                        "subjects": subjects,
                        "resourceRules": resource_rules,
                        "nonResourceRules": non_resource_rules,
                    })
                })
                .collect();
            spec_obj["rules"] = json!(rules);
        }
        obj["spec"] = spec_obj;
    }
    if let Some(status) = &fs.status
        && !status.conditions.is_empty()
    {
        let conds: Vec<Value> = status
            .conditions
            .iter()
            .map(|c| {
                json!({
                    "type": c.r#type,
                    "status": c.status,
                    "reason": c.reason,
                    "message": c.message,
                })
            })
            .collect();
        obj["status"] = json!({"conditions": conds});
    }
    Ok(obj)
}

pub fn pb_flowschemalist_to_json(
    list: &k8s_pb::api::flowcontrol::v1::FlowSchemaList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj =
        json!({"apiVersion": "flowcontrol.apiserver.k8s.io/v1", "kind": "FlowSchemaList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .filter_map(|item| pb_flowschema_to_json(item).ok())
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}
