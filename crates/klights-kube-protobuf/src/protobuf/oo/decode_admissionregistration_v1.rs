// admissionregistration.k8s.io/v1 - ValidatingAdmissionPolicy
// Note: Minimal implementation for Sonobuoy compatibility.
// The spec contains complex nested types (ParamKind, MatchResources, Validation, etc.)
// that would require extensive helper functions. For now, we pass through spec as-is.
use crate::protobuf::*;
use serde_json::json;
pb_decode!(
    pb_validating_admission_policy_to_json,
    k8s_pb::api::admissionregistration::v1::ValidatingAdmissionPolicy,
    vap,
    "admissionregistration.k8s.io/v1",
    "ValidatingAdmissionPolicy",
    obj,
    {
        if let Some(spec) = &vap.spec {
            obj["spec"] = pb_vap_spec_to_json(spec);
        }
        if let Some(status) = &vap.status {
            obj["status"] = pb_vap_status_to_json(status);
        }
    }
);

fn pb_vap_spec_to_json(
    spec: &k8s_pb::api::admissionregistration::v1::ValidatingAdmissionPolicySpec,
) -> Value {
    let mut spec_obj = json!({});
    if let Some(param_kind) = &spec.param_kind {
        let mut param_kind_obj = json!({});
        if let Some(api_version) = &param_kind.api_version {
            param_kind_obj["apiVersion"] = json!(api_version);
        }
        if let Some(kind) = &param_kind.kind {
            param_kind_obj["kind"] = json!(kind);
        }
        if !param_kind_obj.as_object().is_some_and(|o| o.is_empty()) {
            spec_obj["paramKind"] = param_kind_obj;
        }
    }
    if let Some(match_constraints) = &spec.match_constraints {
        spec_obj["matchConstraints"] = pb_match_resources_to_json(match_constraints);
    }
    if !spec.validations.is_empty() {
        spec_obj["validations"] = json!(
            spec.validations
                .iter()
                .map(pb_validation_to_json)
                .collect::<Vec<_>>()
        );
    }
    if let Some(fp) = &spec.failure_policy {
        spec_obj["failurePolicy"] = json!(fp);
    }
    if !spec.audit_annotations.is_empty() {
        spec_obj["auditAnnotations"] = json!(
            spec.audit_annotations
                .iter()
                .map(pb_audit_annotation_to_json)
                .collect::<Vec<_>>()
        );
    }
    if !spec.match_conditions.is_empty() {
        spec_obj["matchConditions"] = json!(
            spec.match_conditions
                .iter()
                .map(pb_match_condition_to_json)
                .collect::<Vec<_>>()
        );
    }
    if !spec.variables.is_empty() {
        spec_obj["variables"] = json!(
            spec.variables
                .iter()
                .map(pb_variable_to_json)
                .collect::<Vec<_>>()
        );
    }
    spec_obj
}

fn pb_match_resources_to_json(
    match_resources: &k8s_pb::api::admissionregistration::v1::MatchResources,
) -> Value {
    let mut match_obj = json!({});
    if let Some(namespace_selector) = &match_resources.namespace_selector {
        match_obj["namespaceSelector"] = pb_label_selector_to_json(namespace_selector);
    }
    if let Some(object_selector) = &match_resources.object_selector {
        match_obj["objectSelector"] = pb_label_selector_to_json(object_selector);
    }
    if !match_resources.resource_rules.is_empty() {
        match_obj["resourceRules"] = json!(
            match_resources
                .resource_rules
                .iter()
                .map(pb_named_rule_with_operations_to_json)
                .collect::<Vec<_>>()
        );
    }
    if !match_resources.exclude_resource_rules.is_empty() {
        match_obj["excludeResourceRules"] = json!(
            match_resources
                .exclude_resource_rules
                .iter()
                .map(pb_named_rule_with_operations_to_json)
                .collect::<Vec<_>>()
        );
    }
    if let Some(policy) = &match_resources.match_policy {
        match_obj["matchPolicy"] = json!(policy);
    }
    match_obj
}

fn pb_named_rule_with_operations_to_json(
    rule: &k8s_pb::api::admissionregistration::v1::NamedRuleWithOperations,
) -> Value {
    let mut rule_obj = rule
        .rule_with_operations
        .as_ref()
        .map(pb_rule_with_operations_to_json)
        .unwrap_or_else(|| json!({}));
    if !rule.resource_names.is_empty() {
        rule_obj["resourceNames"] = json!(rule.resource_names);
    }
    rule_obj
}

fn insert_non_empty_string(obj: &mut Value, field: &str, value: &Option<String>) {
    if let Some(value) = value.as_deref().filter(|value| !value.is_empty()) {
        obj[field] = json!(value);
    }
}

fn pb_validation_to_json(validation: &k8s_pb::api::admissionregistration::v1::Validation) -> Value {
    let mut obj = json!({});
    insert_non_empty_string(&mut obj, "expression", &validation.expression);
    insert_non_empty_string(&mut obj, "message", &validation.message);
    insert_non_empty_string(&mut obj, "reason", &validation.reason);
    insert_non_empty_string(
        &mut obj,
        "messageExpression",
        &validation.message_expression,
    );
    obj
}

fn pb_audit_annotation_to_json(
    annotation: &k8s_pb::api::admissionregistration::v1::AuditAnnotation,
) -> Value {
    let mut obj = json!({});
    insert_non_empty_string(&mut obj, "key", &annotation.key);
    insert_non_empty_string(&mut obj, "valueExpression", &annotation.value_expression);
    obj
}

fn pb_match_condition_to_json(
    condition: &k8s_pb::api::admissionregistration::v1::MatchCondition,
) -> Value {
    let mut obj = json!({});
    insert_non_empty_string(&mut obj, "name", &condition.name);
    insert_non_empty_string(&mut obj, "expression", &condition.expression);
    obj
}

fn pb_variable_to_json(variable: &k8s_pb::api::admissionregistration::v1::Variable) -> Value {
    let mut obj = json!({});
    insert_non_empty_string(&mut obj, "name", &variable.name);
    insert_non_empty_string(&mut obj, "expression", &variable.expression);
    obj
}

fn pb_vap_status_to_json(
    status: &k8s_pb::api::admissionregistration::v1::ValidatingAdmissionPolicyStatus,
) -> Value {
    let mut obj = json!({});
    if let Some(observed_generation) = status.observed_generation {
        obj["observedGeneration"] = json!(observed_generation);
    }
    if let Some(type_checking) = &status.type_checking {
        let warnings: Vec<Value> = type_checking
            .expression_warnings
            .iter()
            .map(|warning| {
                let mut obj = json!({});
                if let Some(field_ref) = &warning.field_ref {
                    obj["fieldRef"] = json!(field_ref);
                }
                if let Some(text) = &warning.warning {
                    obj["warning"] = json!(text);
                }
                obj
            })
            .collect();
        obj["typeChecking"] = json!({"expressionWarnings": warnings});
    }
    if !status.conditions.is_empty() {
        obj["conditions"] = json!(
            status
                .conditions
                .iter()
                .map(pb_meta_condition_to_json)
                .collect::<Vec<_>>()
        );
    }
    obj
}

fn pb_meta_condition_to_json(
    condition: &k8s_pb::apimachinery::pkg::apis::meta::v1::Condition,
) -> Value {
    let mut obj = json!({});
    if let Some(condition_type) = &condition.r#type {
        obj["type"] = json!(condition_type);
    }
    if let Some(status) = &condition.status {
        obj["status"] = json!(status);
    }
    if let Some(observed_generation) = condition.observed_generation {
        obj["observedGeneration"] = json!(observed_generation);
    }
    if let Some(last_transition_time) = &condition.last_transition_time {
        obj["lastTransitionTime"] = pb_time_to_json(last_transition_time);
    }
    if let Some(reason) = &condition.reason {
        obj["reason"] = json!(reason);
    }
    if let Some(message) = &condition.message {
        obj["message"] = json!(message);
    }
    obj
}

// admissionregistration.k8s.io/v1 - ValidatingAdmissionPolicyBinding
pb_decode!(
    pb_validating_admission_policy_binding_to_json,
    k8s_pb::api::admissionregistration::v1::ValidatingAdmissionPolicyBinding,
    vapb,
    "admissionregistration.k8s.io/v1",
    "ValidatingAdmissionPolicyBinding",
    obj,
    {
        if let Some(spec) = &vapb.spec {
            let mut spec_obj = json!({});
            if let Some(policy_name) = &spec.policy_name {
                spec_obj["policyName"] = json!(policy_name);
            }
            if let Some(param_ref) = &spec.param_ref {
                let mut param_obj = json!({});
                if let Some(name) = &param_ref.name {
                    param_obj["name"] = json!(name);
                }
                if let Some(namespace) = &param_ref.namespace {
                    param_obj["namespace"] = json!(namespace);
                }
                if let Some(action) = &param_ref.parameter_not_found_action {
                    param_obj["parameterNotFoundAction"] = json!(action);
                }
                if let Some(selector) = &param_ref.selector {
                    let mut sel = json!({});
                    if !selector.match_labels.is_empty() {
                        sel["matchLabels"] = json!(selector.match_labels);
                    }
                    if !selector.match_expressions.is_empty() {
                        let exprs: Vec<Value> = selector.match_expressions.iter().map(|expr| {
                        json!({"key": expr.key, "operator": expr.operator, "values": expr.values})
                    }).collect();
                        sel["matchExpressions"] = json!(exprs);
                    }
                    if !sel.is_null() && sel.as_object().is_some_and(|o| !o.is_empty()) {
                        param_obj["selector"] = sel;
                    }
                }
                if !param_obj.is_null() && param_obj.as_object().is_some_and(|o| !o.is_empty()) {
                    spec_obj["paramRef"] = param_obj;
                }
            }
            if let Some(match_resources) = &spec.match_resources {
                let mut match_obj = json!({});
                if let Some(namespace_selector) = &match_resources.namespace_selector {
                    let mut sel = json!({});
                    if !namespace_selector.match_labels.is_empty() {
                        sel["matchLabels"] = json!(namespace_selector.match_labels);
                    }
                    if !namespace_selector.match_expressions.is_empty() {
                        let exprs: Vec<Value> = namespace_selector.match_expressions.iter().map(|expr| {
                        json!({"key": expr.key, "operator": expr.operator, "values": expr.values})
                    }).collect();
                        sel["matchExpressions"] = json!(exprs);
                    }
                    if !sel.is_null() && sel.as_object().is_some_and(|o| !o.is_empty()) {
                        match_obj["namespaceSelector"] = sel;
                    }
                }
                if let Some(object_selector) = &match_resources.object_selector {
                    let mut sel = json!({});
                    if !object_selector.match_labels.is_empty() {
                        sel["matchLabels"] = json!(object_selector.match_labels);
                    }
                    if !object_selector.match_expressions.is_empty() {
                        let exprs: Vec<Value> = object_selector.match_expressions.iter().map(|expr| {
                        json!({"key": expr.key, "operator": expr.operator, "values": expr.values})
                    }).collect();
                        sel["matchExpressions"] = json!(exprs);
                    }
                    if !sel.is_null() && sel.as_object().is_some_and(|o| !o.is_empty()) {
                        match_obj["objectSelector"] = sel;
                    }
                }
                if !match_resources.resource_rules.is_empty() {
                    let rules: Vec<Value> = match_resources
                        .resource_rules
                        .iter()
                        .map(|rule| {
                            let mut rule_obj = json!({});
                            if !rule.resource_names.is_empty() {
                                rule_obj["resourceNames"] = json!(rule.resource_names);
                            }
                            if let Some(rwo) = &rule.rule_with_operations {
                                if !rwo.operations.is_empty() {
                                    let operations: Vec<String> = rwo
                                        .operations
                                        .iter()
                                        .map(|op| normalize_admission_operation(op))
                                        .collect();
                                    rule_obj["operations"] = json!(operations);
                                }
                                if let Some(rule) = &rwo.rule {
                                    if !rule.api_groups.is_empty() {
                                        rule_obj["apiGroups"] = json!(rule.api_groups);
                                    }
                                    if !rule.api_versions.is_empty() {
                                        rule_obj["apiVersions"] = json!(rule.api_versions);
                                    }
                                    if !rule.resources.is_empty() {
                                        rule_obj["resources"] = json!(rule.resources);
                                    }
                                    if let Some(scope) = &rule.scope {
                                        rule_obj["scope"] = json!(scope);
                                    }
                                }
                            }
                            rule_obj
                        })
                        .collect();
                    match_obj["resourceRules"] = json!(rules);
                }
                if !match_resources.exclude_resource_rules.is_empty() {
                    let rules: Vec<Value> = match_resources
                        .exclude_resource_rules
                        .iter()
                        .map(|rule| {
                            let mut rule_obj = json!({});
                            if !rule.resource_names.is_empty() {
                                rule_obj["resourceNames"] = json!(rule.resource_names);
                            }
                            if let Some(rwo) = &rule.rule_with_operations {
                                if !rwo.operations.is_empty() {
                                    let operations: Vec<String> = rwo
                                        .operations
                                        .iter()
                                        .map(|op| normalize_admission_operation(op))
                                        .collect();
                                    rule_obj["operations"] = json!(operations);
                                }
                                if let Some(rule) = &rwo.rule {
                                    if !rule.api_groups.is_empty() {
                                        rule_obj["apiGroups"] = json!(rule.api_groups);
                                    }
                                    if !rule.api_versions.is_empty() {
                                        rule_obj["apiVersions"] = json!(rule.api_versions);
                                    }
                                    if !rule.resources.is_empty() {
                                        rule_obj["resources"] = json!(rule.resources);
                                    }
                                    if let Some(scope) = &rule.scope {
                                        rule_obj["scope"] = json!(scope);
                                    }
                                }
                            }
                            rule_obj
                        })
                        .collect();
                    match_obj["excludeResourceRules"] = json!(rules);
                }
                if let Some(policy) = &match_resources.match_policy {
                    match_obj["matchPolicy"] = json!(policy);
                }
                if !match_obj.is_null() && match_obj.as_object().is_some_and(|o| !o.is_empty()) {
                    spec_obj["matchResources"] = match_obj;
                }
            }
            if !spec.validation_actions.is_empty() {
                spec_obj["validationActions"] = json!(spec.validation_actions);
            }
            if !spec_obj.is_null() && spec_obj.as_object().is_some_and(|o| !o.is_empty()) {
                obj["spec"] = spec_obj;
            }
        }
    }
);
