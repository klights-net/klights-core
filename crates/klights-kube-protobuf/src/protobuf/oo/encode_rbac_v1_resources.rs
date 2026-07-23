/// Convert k8s-openapi ClusterRole to k8s-pb ClusterRole
use crate::protobuf::*;
pub fn json_clusterrole_to_pb(
    cr: &k8s_openapi::api::rbac::v1::ClusterRole,
) -> anyhow::Result<k8s_pb::api::rbac::v1::ClusterRole> {
    Ok(k8s_pb::api::rbac::v1::ClusterRole {
        metadata: Some(json_meta_to_pb(&cr.metadata)),
        aggregation_rule: cr
            .aggregation_rule
            .as_ref()
            .map(json_aggregation_rule_to_pb),
        rules: cr
            .rules
            .as_ref()
            .map(|rules| rules.iter().map(json_policy_rule_to_pb).collect())
            .unwrap_or_default(),
    })
}

fn json_aggregation_rule_to_pb(
    aggregation_rule: &k8s_openapi::api::rbac::v1::AggregationRule,
) -> k8s_pb::api::rbac::v1::AggregationRule {
    k8s_pb::api::rbac::v1::AggregationRule {
        cluster_role_selectors: aggregation_rule
            .cluster_role_selectors
            .as_ref()
            .map(|sels| sels.iter().map(json_label_selector_to_pb).collect())
            .unwrap_or_default(),
    }
}

/// Convert k8s-openapi ClusterRoleBinding to k8s-pb ClusterRoleBinding
pub fn json_clusterrolebinding_to_pb(
    crb: &k8s_openapi::api::rbac::v1::ClusterRoleBinding,
) -> anyhow::Result<k8s_pb::api::rbac::v1::ClusterRoleBinding> {
    Ok(k8s_pb::api::rbac::v1::ClusterRoleBinding {
        metadata: Some(json_meta_to_pb(&crb.metadata)),
        subjects: crb
            .subjects
            .as_ref()
            .map(|subjects| subjects.iter().map(json_subject_to_pb).collect())
            .unwrap_or_default(),
        role_ref: Some(json_role_ref_to_pb(&crb.role_ref)),
    })
}

/// Convert k8s-openapi Role to k8s-pb Role
pub fn json_role_to_pb(
    r: &k8s_openapi::api::rbac::v1::Role,
) -> anyhow::Result<k8s_pb::api::rbac::v1::Role> {
    Ok(k8s_pb::api::rbac::v1::Role {
        metadata: Some(json_meta_to_pb(&r.metadata)),
        rules: r
            .rules
            .as_ref()
            .map(|rules| rules.iter().map(json_policy_rule_to_pb).collect())
            .unwrap_or_default(),
    })
}

/// Convert k8s-openapi RoleBinding to k8s-pb RoleBinding
pub fn json_rolebinding_to_pb(
    rb: &k8s_openapi::api::rbac::v1::RoleBinding,
) -> anyhow::Result<k8s_pb::api::rbac::v1::RoleBinding> {
    Ok(k8s_pb::api::rbac::v1::RoleBinding {
        metadata: Some(json_meta_to_pb(&rb.metadata)),
        subjects: rb
            .subjects
            .as_ref()
            .map(|subjects| subjects.iter().map(json_subject_to_pb).collect())
            .unwrap_or_default(),
        role_ref: Some(json_role_ref_to_pb(&rb.role_ref)),
    })
}

pub fn json_policy_rule_to_pb(
    rule: &k8s_openapi::api::rbac::v1::PolicyRule,
) -> k8s_pb::api::rbac::v1::PolicyRule {
    k8s_pb::api::rbac::v1::PolicyRule {
        verbs: rule.verbs.clone(),
        api_groups: rule.api_groups.clone().unwrap_or_default(),
        resources: rule.resources.clone().unwrap_or_default(),
        resource_names: rule.resource_names.clone().unwrap_or_default(),
        non_resource_ur_ls: rule.non_resource_urls.clone().unwrap_or_default(),
    }
}

pub fn json_subject_to_pb(
    subject: &k8s_openapi::api::rbac::v1::Subject,
) -> k8s_pb::api::rbac::v1::Subject {
    k8s_pb::api::rbac::v1::Subject {
        kind: Some(subject.kind.clone()),
        api_group: subject.api_group.clone(),
        name: Some(subject.name.clone()),
        namespace: subject.namespace.clone(),
    }
}

pub fn json_role_ref_to_pb(
    role_ref: &k8s_openapi::api::rbac::v1::RoleRef,
) -> k8s_pb::api::rbac::v1::RoleRef {
    k8s_pb::api::rbac::v1::RoleRef {
        api_group: Some(role_ref.api_group.clone()),
        kind: Some(role_ref.kind.clone()),
        name: Some(role_ref.name.clone()),
    }
}

pub fn json_clusterrolelist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::rbac::v1::ClusterRoleList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });
    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("ClusterRoleList missing items array"))?;
    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::rbac::v1::ClusterRole::deserialize(item)?;
            json_clusterrole_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(k8s_pb::api::rbac::v1::ClusterRoleList {
        metadata,
        items: pb_items,
    })
}

pub fn json_clusterrolebindinglist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::rbac::v1::ClusterRoleBindingList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });
    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("ClusterRoleBindingList missing items array"))?;
    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::rbac::v1::ClusterRoleBinding::deserialize(item)?;
            json_clusterrolebinding_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(k8s_pb::api::rbac::v1::ClusterRoleBindingList {
        metadata,
        items: pb_items,
    })
}

pub fn json_rolelist_to_pb(value: &Value) -> anyhow::Result<k8s_pb::api::rbac::v1::RoleList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });
    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("RoleList missing items array"))?;
    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::rbac::v1::Role::deserialize(item)?;
            json_role_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(k8s_pb::api::rbac::v1::RoleList {
        metadata,
        items: pb_items,
    })
}

pub fn json_rolebindinglist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::rbac::v1::RoleBindingList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });
    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("RoleBindingList missing items array"))?;
    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::rbac::v1::RoleBinding::deserialize(item)?;
            json_rolebinding_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(k8s_pb::api::rbac::v1::RoleBindingList {
        metadata,
        items: pb_items,
    })
}
