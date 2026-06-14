/// Helper: Convert PolicyRule array to JSON
use crate::protobuf::*;
pub fn pb_policy_rules_to_json(rules: &[k8s_pb::api::rbac::v1::PolicyRule]) -> Value {
    use serde_json::json;

    rules
        .iter()
        .map(|rule| {
            let mut rule_obj = json!({});
            if !rule.verbs.is_empty() {
                rule_obj["verbs"] = json!(rule.verbs);
            }
            if !rule.api_groups.is_empty() {
                rule_obj["apiGroups"] = json!(rule.api_groups);
            }
            if !rule.resources.is_empty() {
                rule_obj["resources"] = json!(rule.resources);
            }
            if !rule.resource_names.is_empty() {
                rule_obj["resourceNames"] = json!(rule.resource_names);
            }
            if !rule.non_resource_ur_ls.is_empty() {
                rule_obj["nonResourceURLs"] = json!(rule.non_resource_ur_ls);
            }
            rule_obj
        })
        .collect()
}

pub fn pb_aggregation_rule_to_json(rule: &k8s_pb::api::rbac::v1::AggregationRule) -> Value {
    use serde_json::json;

    let mut obj = json!({});
    if !rule.cluster_role_selectors.is_empty() {
        obj["clusterRoleSelectors"] = rule
            .cluster_role_selectors
            .iter()
            .map(pb_label_selector_to_json)
            .collect::<Vec<_>>()
            .into();
    }
    obj
}

/// Helper: Convert RoleRef to JSON
pub fn pb_role_ref_to_json(role_ref: &k8s_pb::api::rbac::v1::RoleRef) -> Value {
    use serde_json::json;

    let mut obj = json!({});
    if let Some(api_group) = &role_ref.api_group {
        obj["apiGroup"] = json!(api_group);
    }
    if let Some(kind) = &role_ref.kind {
        obj["kind"] = json!(kind);
    }
    if let Some(name) = &role_ref.name {
        obj["name"] = json!(name);
    }
    obj
}

/// Helper: Convert Subject array to JSON
pub fn pb_subjects_to_json(subjects: &[k8s_pb::api::rbac::v1::Subject]) -> Value {
    use serde_json::json;

    subjects
        .iter()
        .map(|subject| {
            let mut sub_obj = json!({});
            if let Some(kind) = &subject.kind {
                sub_obj["kind"] = json!(kind);
            }
            if let Some(name) = &subject.name {
                sub_obj["name"] = json!(name);
            }
            if let Some(namespace) = &subject.namespace {
                sub_obj["namespace"] = json!(namespace);
            }
            if let Some(api_group) = &subject.api_group {
                sub_obj["apiGroup"] = json!(api_group);
            }
            sub_obj
        })
        .collect()
}

pb_decode!(
    pb_clusterrole_to_json,
    k8s_pb::api::rbac::v1::ClusterRole,
    cr,
    "rbac.authorization.k8s.io/v1",
    "ClusterRole",
    obj,
    {
        if let Some(aggregation_rule) = &cr.aggregation_rule {
            obj["aggregationRule"] = pb_aggregation_rule_to_json(aggregation_rule);
        }
        if !cr.rules.is_empty() {
            obj["rules"] = pb_policy_rules_to_json(&cr.rules);
        }
    }
);

pb_decode!(
    pb_role_to_json,
    k8s_pb::api::rbac::v1::Role,
    role,
    "rbac.authorization.k8s.io/v1",
    "Role",
    obj,
    {
        if !role.rules.is_empty() {
            obj["rules"] = pb_policy_rules_to_json(&role.rules);
        }
    }
);

pb_decode!(
    pb_clusterrolebinding_to_json,
    k8s_pb::api::rbac::v1::ClusterRoleBinding,
    crb,
    "rbac.authorization.k8s.io/v1",
    "ClusterRoleBinding",
    obj,
    {
        if let Some(role_ref) = &crb.role_ref {
            obj["roleRef"] = pb_role_ref_to_json(role_ref);
        }
        if !crb.subjects.is_empty() {
            obj["subjects"] = pb_subjects_to_json(&crb.subjects);
        }
    }
);

pb_decode!(
    pb_rolebinding_to_json,
    k8s_pb::api::rbac::v1::RoleBinding,
    rb,
    "rbac.authorization.k8s.io/v1",
    "RoleBinding",
    obj,
    {
        if let Some(role_ref) = &rb.role_ref {
            obj["roleRef"] = pb_role_ref_to_json(role_ref);
        }
        if !rb.subjects.is_empty() {
            obj["subjects"] = pb_subjects_to_json(&rb.subjects);
        }
    }
);
