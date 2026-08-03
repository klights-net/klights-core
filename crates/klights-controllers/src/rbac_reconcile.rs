use std::collections::BTreeSet;

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;
use klights_reconcile_api::ControllerStoreResult;
use serde_json::{Map, Value};

use crate::default_rbac_policy::{
    AUTOUPDATE_ANNOTATION, DefaultRbacObject, default_cluster_role_rules, default_rbac_fixtures,
};
use klights_types::LabelSelector;

#[async_trait]
pub trait RbacPolicyStore: Send + Sync {
    async fn get_rbac_object(
        &self,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>>;

    async fn create_rbac_object(
        &self,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: Value,
    ) -> ControllerStoreResult<Resource>;

    async fn update_rbac_object(
        &self,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: Value,
        expected_resource_version: i64,
    ) -> ControllerStoreResult<Resource>;

    async fn list_cluster_roles(&self) -> ControllerStoreResult<Vec<Resource>>;
}

pub async fn reconcile_default_rbac_objects<S: RbacPolicyStore + ?Sized>(store: &S) -> Result<()> {
    for fixture in default_rbac_fixtures() {
        reconcile_default_rbac_object(store, &fixture).await?;
    }

    reconcile_cluster_role_aggregation(store).await?;

    Ok(())
}

async fn reconcile_default_rbac_object<S: RbacPolicyStore + ?Sized>(
    store: &S,
    fixture: &DefaultRbacObject,
) -> Result<()> {
    let (kind, name, namespace) = fixture.key();
    let existing = store.get_rbac_object(kind, namespace, name).await?;

    match existing {
        Some(existing_obj) => {
            if !autoupdate_enabled(existing_obj.data.as_ref()) {
                return Ok(());
            }

            let expected = fixture.to_json_value();
            let mut patched = existing_obj
                .data
                .as_ref()
                .as_object()
                .cloned()
                .unwrap_or_default();
            let changed = reconcile_metadata(&mut patched, &expected)
                | reconcile_role_rules(&mut patched, &expected)
                | reconcile_aggregation_rule(&mut patched, &expected);

            if changed {
                store
                    .update_rbac_object(
                        kind,
                        namespace,
                        name,
                        Value::Object(patched),
                        existing_obj.resource_version,
                    )
                    .await?;
            }

            Ok(())
        }
        None => {
            store
                .create_rbac_object(kind, namespace, name, fixture.to_json_value())
                .await?;
            Ok(())
        }
    }
}

fn autoupdate_enabled(resource: &Value) -> bool {
    resource
        .pointer("/metadata/annotations")
        .and_then(|annotations| annotations.get(AUTOUPDATE_ANNOTATION))
        .and_then(|v| v.as_str())
        != Some("false")
}

fn reconcile_metadata(existing: &mut Map<String, Value>, desired: &Value) -> bool {
    let mut changed = false;

    let existing_meta = match existing.get_mut("metadata") {
        Some(Value::Object(meta)) => meta,
        _ => {
            if let Some(Value::Object(desired_meta)) = desired.get("metadata") {
                existing.insert("metadata".to_string(), Value::Object(desired_meta.clone()));
                return true;
            }
            return false;
        }
    };

    if let Some(Value::Object(desired_meta)) = desired.get("metadata") {
        changed |= ensure_map_entries(existing_meta, desired_meta, "labels");
        changed |= ensure_map_entries(existing_meta, desired_meta, "annotations");
    }

    changed
}

fn ensure_map_entries(
    existing_meta: &mut Map<String, Value>,
    desired_meta: &Map<String, Value>,
    field: &str,
) -> bool {
    let desired_map = match desired_meta.get(field) {
        Some(Value::Object(map)) => map,
        _ => return false,
    };

    let existing_map = match existing_meta.get_mut(field) {
        Some(Value::Object(existing)) => existing,
        _ => {
            existing_meta.insert(field.to_string(), Value::Object(desired_map.clone()));
            return true;
        }
    };

    let mut changed = false;
    for (key, desired_value) in desired_map {
        if existing_map.get(key) != Some(desired_value) {
            existing_map.insert(key.clone(), desired_value.clone());
            changed = true;
        }
    }

    changed
}

fn reconcile_role_rules(existing: &mut Map<String, Value>, desired: &Value) -> bool {
    let kind = existing.get("kind").and_then(Value::as_str);
    if !matches!(kind, Some("ClusterRole" | "Role")) {
        return false;
    }

    let existing_rules = match existing.get("rules") {
        Some(Value::Array(existing_rules)) => existing_rules.clone(),
        _ => Vec::new(),
    };
    let desired_rules = match desired.get("rules") {
        Some(Value::Array(rules)) => rules,
        _ => return false,
    };

    let mut merged_rules = existing_rules;
    let mut changed = false;

    for expected_rule in desired_rules {
        if !merged_rules
            .iter()
            .any(|rule| RuleShape::from_rule(rule) == RuleShape::from_rule(expected_rule))
        {
            merged_rules.push(expected_rule.clone());
            changed = true;
        }
    }

    if changed {
        existing.insert("rules".to_string(), Value::Array(merged_rules));
    }

    changed
}

/// Copy a fixture's `aggregationRule` onto an existing default object so that
/// upgraded clusters gain the field (and corrected selectors) on the
/// admin/edit/view ClusterRoles. No-op for fixtures that define no
/// `aggregationRule`, so it never strips a user-managed aggregationRule.
fn reconcile_aggregation_rule(existing: &mut Map<String, Value>, desired: &Value) -> bool {
    let Some(desired_rule) = desired.get("aggregationRule") else {
        return false;
    };
    if existing.get("aggregationRule") == Some(desired_rule) {
        return false;
    }
    existing.insert("aggregationRule".to_string(), desired_rule.clone());
    true
}

/// Recompute every aggregated ClusterRole (any role carrying an
/// `aggregationRule.clusterRoleSelectors`) from the current set of source
/// roles. Unlike a one-way add-only merge, this fully recomputes the managed
/// rule set on each pass, so privilege contributed by a source role is revoked
/// when that source loses the aggregation label or is deleted.
pub async fn reconcile_cluster_role_aggregation<S: RbacPolicyStore + ?Sized>(
    store: &S,
) -> Result<()> {
    let cluster_roles = store.list_cluster_roles().await?;

    // Snapshot every ClusterRole body once for selector matching.
    let role_values: Vec<Value> = cluster_roles
        .iter()
        .map(|resource| resource.data.as_ref().clone())
        .collect();

    for resource in &cluster_roles {
        let Some(selectors) = aggregation_selectors(resource.data.as_ref()) else {
            continue;
        };
        reconcile_aggregated_role(store, resource, &role_values, &selectors).await?;
    }

    Ok(())
}

/// Parse `aggregationRule.clusterRoleSelectors` into label selectors. Returns
/// `None` for ClusterRoles without an `aggregationRule` (their `rules` are not
/// controller-managed). A present-but-empty selector list yields `Some(vec![])`,
/// collapsing the role's aggregated rules down to its floor.
fn aggregation_selectors(role: &Value) -> Option<Vec<LabelSelector>> {
    let selectors = role
        .pointer("/aggregationRule/clusterRoleSelectors")
        .and_then(Value::as_array)?;
    Some(
        selectors
            .iter()
            .filter_map(|selector| LabelSelector::from_k8s_selector(selector).ok())
            .collect(),
    )
}

async fn reconcile_aggregated_role<S: RbacPolicyStore + ?Sized>(
    store: &S,
    target: &klights_cluster_core::Resource,
    cluster_roles: &[Value],
    selectors: &[LabelSelector],
) -> Result<()> {
    if !autoupdate_enabled(target.data.as_ref()) {
        return Ok(());
    }

    let target_name = target.name.as_str();

    // Floor: the role's own default rules (empty for user-defined aggregated
    // roles). The floor is never revoked; everything above it is recomputed
    // from the currently-qualifying source roles so stale grants drop out.
    let mut desired_rules: Vec<Value> = Vec::new();
    let mut seen: Vec<RuleShape> = Vec::new();
    for rule in default_cluster_role_rules(target_name) {
        push_unique_rule(&mut desired_rules, &mut seen, rule);
    }

    // Source roles matching any selector, ordered by name for determinism.
    let mut sources: Vec<&Value> = cluster_roles
        .iter()
        .filter(|source| role_name(source) != Some(target_name))
        .filter(|source| {
            selectors
                .iter()
                .any(|selector| selector.matches_labels(role_labels(source)))
        })
        .collect();
    sources.sort_by(|a, b| role_name(a).cmp(&role_name(b)));

    for source in sources {
        if let Some(rules) = source.get("rules").and_then(Value::as_array) {
            for rule in rules {
                push_unique_rule(&mut desired_rules, &mut seen, rule.clone());
            }
        }
    }

    let existing_rules = target
        .data
        .as_ref()
        .get("rules")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    if same_rule_set(&existing_rules, &desired_rules) {
        return Ok(());
    }

    let mut patched = target
        .data
        .as_ref()
        .as_object()
        .cloned()
        .unwrap_or_default();
    patched.insert("rules".to_string(), Value::Array(desired_rules));
    store
        .update_rbac_object(
            "ClusterRole",
            None,
            target_name,
            Value::Object(patched),
            target.resource_version,
        )
        .await?;

    Ok(())
}

fn role_name(role: &Value) -> Option<&str> {
    role.pointer("/metadata/name").and_then(Value::as_str)
}

fn role_labels(role: &Value) -> Option<&Map<String, Value>> {
    role.pointer("/metadata/labels").and_then(Value::as_object)
}

fn push_unique_rule(rules: &mut Vec<Value>, seen: &mut Vec<RuleShape>, rule: Value) {
    let shape = RuleShape::from_rule(&rule);
    if seen.contains(&shape) {
        return;
    }
    seen.push(shape);
    rules.push(rule);
}

fn same_rule_set(a: &[Value], b: &[Value]) -> bool {
    let a_shapes: BTreeSet<RuleShape> = a.iter().map(RuleShape::from_rule).collect();
    let b_shapes: BTreeSet<RuleShape> = b.iter().map(RuleShape::from_rule).collect();
    a_shapes == b_shapes
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RuleShape {
    verbs: BTreeSet<String>,
    api_groups: BTreeSet<String>,
    resources: BTreeSet<String>,
    resource_names: BTreeSet<String>,
    non_resource_urls: BTreeSet<String>,
}

impl RuleShape {
    fn from_rule(rule: &Value) -> Self {
        Self {
            verbs: strings_set(rule.get("verbs")),
            api_groups: strings_set(rule.get("apiGroups")),
            resources: strings_set(rule.get("resources")),
            resource_names: strings_set(rule.get("resourceNames")),
            non_resource_urls: strings_set(rule.get("nonResourceURLs")),
        }
    }
}

fn strings_set(value: Option<&Value>) -> BTreeSet<String> {
    value
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_str().map(ToString::to_string))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
#[path = "rbac_reconcile_tests.rs"]
mod tests;
