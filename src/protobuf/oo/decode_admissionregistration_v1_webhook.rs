/// ValidatingAdmissionPolicyList decoder
use crate::protobuf::*;
pub fn pb_validatingadmissionpolicylist_to_json(
    list: &k8s_pb::api::admissionregistration::v1::ValidatingAdmissionPolicyList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicyList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .map(|item| {
            json!({
                "apiVersion": "admissionregistration.k8s.io/v1",
                "kind": "ValidatingAdmissionPolicy",
                "metadata": item.metadata.as_ref().map(meta_to_json).unwrap_or_default(),
            })
        })
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}

/// ValidatingAdmissionPolicyBindingList decoder
pub fn pb_validatingadmissionpolicybindinglist_to_json(
    list: &k8s_pb::api::admissionregistration::v1::ValidatingAdmissionPolicyBindingList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicyBindingList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .map(|item| {
            json!({
                "apiVersion": "admissionregistration.k8s.io/v1",
                "kind": "ValidatingAdmissionPolicyBinding",
                "metadata": item.metadata.as_ref().map(meta_to_json).unwrap_or_default(),
            })
        })
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}

/// Convert a protobuf LabelSelector to JSON
pub fn pb_label_selector_to_json(
    sel: &k8s_pb::apimachinery::pkg::apis::meta::v1::LabelSelector,
) -> Value {
    use serde_json::json;
    let mut obj = json!({});
    if !sel.match_labels.is_empty() {
        obj["matchLabels"] = json!(sel.match_labels);
    }
    if !sel.match_expressions.is_empty() {
        let exprs: Vec<Value> = sel
            .match_expressions
            .iter()
            .map(|e| json!({"key": e.key, "operator": e.operator, "values": e.values}))
            .collect();
        obj["matchExpressions"] = json!(exprs);
    }
    obj
}

/// Convert a protobuf RuleWithOperations to JSON
pub fn pb_rule_with_operations_to_json(
    r: &k8s_pb::api::admissionregistration::v1::RuleWithOperations,
) -> Value {
    use serde_json::json;
    let mut obj = json!({});
    if !r.operations.is_empty() {
        let operations: Vec<String> = r
            .operations
            .iter()
            .map(|op| normalize_admission_operation(op))
            .collect();
        obj["operations"] = json!(operations);
    }
    if let Some(rule) = &r.rule {
        if !rule.api_groups.is_empty() {
            obj["apiGroups"] = json!(rule.api_groups);
        }
        if !rule.api_versions.is_empty() {
            obj["apiVersions"] = json!(rule.api_versions);
        }
        if !rule.resources.is_empty() {
            obj["resources"] = json!(rule.resources);
        }
        if let Some(scope) = &rule.scope {
            obj["scope"] = json!(scope);
        }
    }
    obj
}

pub fn normalize_admission_operation(op: &str) -> String {
    if op.eq_ignore_ascii_case("create") {
        "CREATE".to_string()
    } else if op.eq_ignore_ascii_case("update") {
        "UPDATE".to_string()
    } else if op.eq_ignore_ascii_case("delete") {
        "DELETE".to_string()
    } else if op.eq_ignore_ascii_case("connect") {
        "CONNECT".to_string()
    } else {
        op.to_string()
    }
}

/// Convert a protobuf WebhookClientConfig to JSON
pub fn pb_webhook_client_config_to_json(
    cc: &k8s_pb::api::admissionregistration::v1::WebhookClientConfig,
) -> Value {
    use serde_json::json;
    let mut obj = json!({});
    if let Some(url) = &cc.url {
        obj["url"] = json!(url);
    }
    if let Some(svc) = &cc.service {
        let mut svc_obj = json!({});
        if let Some(ns) = &svc.namespace {
            svc_obj["namespace"] = json!(ns);
        }
        if let Some(name) = &svc.name {
            svc_obj["name"] = json!(name);
        }
        if let Some(path) = &svc.path {
            svc_obj["path"] = json!(path);
        }
        if let Some(port) = svc.port {
            svc_obj["port"] = json!(port);
        }
        obj["service"] = svc_obj;
    }
    if let Some(ca_bundle) = &cc.ca_bundle
        && !ca_bundle.is_empty()
    {
        obj["caBundle"] = json!(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            ca_bundle
        ));
    }
    obj
}

/// Decode MutatingWebhookConfiguration protobuf → JSON
pub fn pb_mutatingwebhookconfiguration_to_json(
    mwc: &k8s_pb::api::admissionregistration::v1::MutatingWebhookConfiguration,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "admissionregistration.k8s.io/v1", "kind": "MutatingWebhookConfiguration"});
    if let Some(metadata) = &mwc.metadata {
        obj["metadata"] = meta_to_json(metadata);
    }
    if !mwc.webhooks.is_empty() {
        let webhooks: Vec<Value> = mwc
            .webhooks
            .iter()
            .map(|wh| {
                let mut w = json!({});
                if let Some(name) = &wh.name {
                    w["name"] = json!(name);
                }
                if let Some(cc) = &wh.client_config {
                    w["clientConfig"] = pb_webhook_client_config_to_json(cc);
                }
                if !wh.rules.is_empty() {
                    w["rules"] = json!(
                        wh.rules
                            .iter()
                            .map(pb_rule_with_operations_to_json)
                            .collect::<Vec<_>>()
                    );
                }
                if let Some(fp) = &wh.failure_policy {
                    w["failurePolicy"] = json!(fp);
                }
                if let Some(mp) = &wh.match_policy {
                    w["matchPolicy"] = json!(mp);
                }
                if let Some(se) = &wh.side_effects {
                    w["sideEffects"] = json!(se);
                }
                if let Some(ts) = wh.timeout_seconds {
                    w["timeoutSeconds"] = json!(ts);
                }
                if !wh.admission_review_versions.is_empty() {
                    w["admissionReviewVersions"] = json!(wh.admission_review_versions);
                }
                if let Some(rp) = &wh.reinvocation_policy {
                    w["reinvocationPolicy"] = json!(rp);
                }
                if let Some(ns_sel) = &wh.namespace_selector {
                    let sel = pb_label_selector_to_json(ns_sel);
                    if sel.as_object().is_some_and(|o| !o.is_empty()) {
                        w["namespaceSelector"] = sel;
                    }
                }
                if let Some(obj_sel) = &wh.object_selector {
                    let sel = pb_label_selector_to_json(obj_sel);
                    if sel.as_object().is_some_and(|o| !o.is_empty()) {
                        w["objectSelector"] = sel;
                    }
                }
                if !wh.match_conditions.is_empty() {
                    let conds: Vec<Value> = wh
                        .match_conditions
                        .iter()
                        .map(|c| {
                            let mut co = json!({});
                            if let Some(n) = &c.name {
                                co["name"] = json!(n);
                            }
                            if let Some(e) = &c.expression {
                                co["expression"] = json!(e);
                            }
                            co
                        })
                        .collect();
                    w["matchConditions"] = json!(conds);
                }
                w
            })
            .collect();
        obj["webhooks"] = json!(webhooks);
    }
    Ok(obj)
}

/// Decode ValidatingWebhookConfiguration protobuf → JSON
pub fn pb_validatingwebhookconfiguration_to_json(
    vwc: &k8s_pb::api::admissionregistration::v1::ValidatingWebhookConfiguration,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingWebhookConfiguration"});
    if let Some(metadata) = &vwc.metadata {
        obj["metadata"] = meta_to_json(metadata);
    }
    if !vwc.webhooks.is_empty() {
        let webhooks: Vec<Value> = vwc
            .webhooks
            .iter()
            .map(|wh| {
                let mut w = json!({});
                if let Some(name) = &wh.name {
                    w["name"] = json!(name);
                }
                if let Some(cc) = &wh.client_config {
                    w["clientConfig"] = pb_webhook_client_config_to_json(cc);
                }
                if !wh.rules.is_empty() {
                    w["rules"] = json!(
                        wh.rules
                            .iter()
                            .map(pb_rule_with_operations_to_json)
                            .collect::<Vec<_>>()
                    );
                }
                if let Some(fp) = &wh.failure_policy {
                    w["failurePolicy"] = json!(fp);
                }
                if let Some(mp) = &wh.match_policy {
                    w["matchPolicy"] = json!(mp);
                }
                if let Some(se) = &wh.side_effects {
                    w["sideEffects"] = json!(se);
                }
                if let Some(ts) = wh.timeout_seconds {
                    w["timeoutSeconds"] = json!(ts);
                }
                if !wh.admission_review_versions.is_empty() {
                    w["admissionReviewVersions"] = json!(wh.admission_review_versions);
                }
                if let Some(ns_sel) = &wh.namespace_selector {
                    let sel = pb_label_selector_to_json(ns_sel);
                    if sel.as_object().is_some_and(|o| !o.is_empty()) {
                        w["namespaceSelector"] = sel;
                    }
                }
                if let Some(obj_sel) = &wh.object_selector {
                    let sel = pb_label_selector_to_json(obj_sel);
                    if sel.as_object().is_some_and(|o| !o.is_empty()) {
                        w["objectSelector"] = sel;
                    }
                }
                if !wh.match_conditions.is_empty() {
                    let conds: Vec<Value> = wh
                        .match_conditions
                        .iter()
                        .map(|c| {
                            let mut co = json!({});
                            if let Some(n) = &c.name {
                                co["name"] = json!(n);
                            }
                            if let Some(e) = &c.expression {
                                co["expression"] = json!(e);
                            }
                            co
                        })
                        .collect();
                    w["matchConditions"] = json!(conds);
                }
                w
            })
            .collect();
        obj["webhooks"] = json!(webhooks);
    }
    Ok(obj)
}

/// MutatingWebhookConfigurationList decoder
pub fn pb_mutatingwebhookconfigurationlist_to_json(
    list: &k8s_pb::api::admissionregistration::v1::MutatingWebhookConfigurationList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "admissionregistration.k8s.io/v1", "kind": "MutatingWebhookConfigurationList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .map(pb_mutatingwebhookconfiguration_to_json)
        .collect::<Result<Vec<_>, _>>()?;
    obj["items"] = json!(items);
    Ok(obj)
}

/// ValidatingWebhookConfigurationList decoder
pub fn pb_validatingwebhookconfigurationlist_to_json(
    list: &k8s_pb::api::admissionregistration::v1::ValidatingWebhookConfigurationList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingWebhookConfigurationList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .map(pb_validatingwebhookconfiguration_to_json)
        .collect::<Result<Vec<_>, _>>()?;
    obj["items"] = json!(items);
    Ok(obj)
}
