use crate::protobuf::*;
pub fn pb_apiservice_condition_to_json(
    cond: &k8s_pb::kube_aggregator::pkg::apis::apiregistration::v1::APIServiceCondition,
) -> Value {
    use serde_json::json;

    let mut obj = json!({});
    if let Some(t) = &cond.r#type {
        obj["type"] = json!(t);
    }
    if let Some(status) = &cond.status {
        obj["status"] = json!(status);
    }
    if let Some(last_transition) = &cond.last_transition_time {
        let ts = pb_time_to_json(last_transition);
        if !ts.is_null() {
            obj["lastTransitionTime"] = ts;
        }
    }
    if let Some(reason) = &cond.reason {
        obj["reason"] = json!(reason);
    }
    if let Some(message) = &cond.message {
        obj["message"] = json!(message);
    }
    obj
}

pub fn pb_apiservice_to_json(
    apiservice: &k8s_pb::kube_aggregator::pkg::apis::apiregistration::v1::APIService,
) -> anyhow::Result<Value> {
    use serde_json::json;

    let mut obj = json!({"apiVersion": "apiregistration.k8s.io/v1", "kind": "APIService"});
    if let Some(metadata) = &apiservice.metadata {
        obj["metadata"] = meta_to_json(metadata);
    }

    if let Some(spec) = &apiservice.spec {
        let mut spec_obj = json!({});
        if let Some(service) = &spec.service {
            let mut service_obj = json!({});
            if let Some(namespace) = &service.namespace {
                service_obj["namespace"] = json!(namespace);
            }
            if let Some(name) = &service.name {
                service_obj["name"] = json!(name);
            }
            if let Some(port) = service.port {
                service_obj["port"] = json!(port);
            }
            if service_obj.as_object().is_some_and(|o| !o.is_empty()) {
                spec_obj["service"] = service_obj;
            }
        }
        if let Some(group) = &spec.group {
            spec_obj["group"] = json!(group);
        }
        if let Some(version) = &spec.version {
            spec_obj["version"] = json!(version);
        }
        if let Some(insecure_skip_tls_verify) = spec.insecure_skip_tls_verify {
            spec_obj["insecureSkipTLSVerify"] = json!(insecure_skip_tls_verify);
        }
        if let Some(ca_bundle) = &spec.ca_bundle
            && !ca_bundle.is_empty()
        {
            spec_obj["caBundle"] = json!(base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                ca_bundle
            ));
        }
        if let Some(group_priority_minimum) = spec.group_priority_minimum {
            spec_obj["groupPriorityMinimum"] = json!(group_priority_minimum);
        }
        if let Some(version_priority) = spec.version_priority {
            spec_obj["versionPriority"] = json!(version_priority);
        }
        if spec_obj.as_object().is_some_and(|o| !o.is_empty()) {
            obj["spec"] = spec_obj;
        }
    }

    if let Some(status) = &apiservice.status {
        let mut status_obj = json!({});
        if !status.conditions.is_empty() {
            status_obj["conditions"] = json!(
                status
                    .conditions
                    .iter()
                    .map(pb_apiservice_condition_to_json)
                    .collect::<Vec<_>>()
            );
        }
        if status_obj.as_object().is_some_and(|o| !o.is_empty()) {
            obj["status"] = status_obj;
        }
    }

    Ok(obj)
}

pub fn pb_apiservicelist_to_json(
    list: &k8s_pb::kube_aggregator::pkg::apis::apiregistration::v1::APIServiceList,
) -> anyhow::Result<Value> {
    use serde_json::json;

    let mut obj = json!({"apiVersion": "apiregistration.k8s.io/v1", "kind": "APIServiceList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());

    let items: Vec<Value> = list
        .items
        .iter()
        .map(pb_apiservice_to_json)
        .collect::<Result<Vec<_>, _>>()?;
    obj["items"] = json!(items);
    Ok(obj)
}
