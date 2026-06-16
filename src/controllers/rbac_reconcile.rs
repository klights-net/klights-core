use std::collections::BTreeSet;

use anyhow::Result;
use serde_json::{Map, Value};

use crate::auth::default_rbac::{
    AUTOUPDATE_ANNOTATION, DefaultRbacObject, RBAC_API_VERSION, default_cluster_role_rules,
    default_rbac_fixtures,
};
use crate::datastore::DatastoreBackend;
use crate::label_selector::LabelSelector;

pub async fn reconcile_default_rbac_objects(db: &dyn DatastoreBackend) -> Result<()> {
    for fixture in default_rbac_fixtures() {
        reconcile_default_rbac_object(db, &fixture).await?;
    }

    reconcile_cluster_role_aggregation(db).await?;

    Ok(())
}

async fn reconcile_default_rbac_object(
    db: &dyn DatastoreBackend,
    fixture: &DefaultRbacObject,
) -> Result<()> {
    let (kind, name, namespace) = fixture.key();
    let existing = db
        .get_resource(RBAC_API_VERSION, kind, namespace, name)
        .await?;

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
                db.update_resource(
                    RBAC_API_VERSION,
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
            db.create_resource(
                RBAC_API_VERSION,
                kind,
                namespace,
                name,
                fixture.to_json_value(),
            )
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
async fn reconcile_cluster_role_aggregation(db: &dyn DatastoreBackend) -> Result<()> {
    let cluster_roles = db
        .list_resources_page(
            RBAC_API_VERSION,
            "ClusterRole",
            None,
            None,
            None,
            crate::datastore::types::ListPageRequest::unbounded(),
        )
        .await?;

    // Snapshot every ClusterRole body once for selector matching.
    let role_values: Vec<Value> = cluster_roles
        .items
        .iter()
        .map(|resource| resource.data.as_ref().clone())
        .collect();

    for resource in &cluster_roles.items {
        let Some(selectors) = aggregation_selectors(resource.data.as_ref()) else {
            continue;
        };
        reconcile_aggregated_role(db, resource, &role_values, &selectors).await?;
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

async fn reconcile_aggregated_role(
    db: &dyn DatastoreBackend,
    target: &crate::datastore::Resource,
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
    db.update_resource(
        RBAC_API_VERSION,
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
mod tests {
    use super::*;
    use crate::datastore::backend::DatastoreHandle;
    use crate::datastore::sqlite::Datastore;
    use std::sync::Arc;

    fn as_handle(db: &Datastore) -> DatastoreHandle {
        Arc::new(db.clone()) as DatastoreHandle
    }

    fn has_rule(rules: &[Value], expected: &Value) -> bool {
        let expected_shape = RuleShape::from_rule(expected);
        rules
            .iter()
            .any(|rule| RuleShape::from_rule(rule) == expected_shape)
    }

    #[tokio::test]
    async fn reconcile_default_rbac_objects_creates_missing_objects() {
        let db = crate::datastore::test_support::in_memory().await;
        let handle = as_handle(&db);

        reconcile_default_rbac_objects(handle.as_ref())
            .await
            .unwrap();

        for fixture in default_rbac_fixtures() {
            let found = handle
                .get_resource(
                    RBAC_API_VERSION,
                    fixture.kind,
                    fixture.namespace,
                    fixture.name,
                )
                .await
                .unwrap()
                .is_some();
            assert!(
                found,
                "expected default RBAC object {}/{}:{} to be present",
                fixture.kind,
                fixture.namespace.unwrap_or("<cluster>"),
                fixture.name
            );
        }
    }

    #[tokio::test]
    async fn reconcile_repairs_missing_cluster_role_rule_when_autoupdate_enabled() {
        let db = crate::datastore::test_support::in_memory().await;
        let handle = as_handle(&db);
        reconcile_default_rbac_objects(handle.as_ref())
            .await
            .unwrap();

        let fixture = super::default_rbac_fixtures()
            .into_iter()
            .find(|object| object.kind == "ClusterRole" && object.name == "system:discovery")
            .expect("fixture exists");
        let expected_rule = fixture
            .to_json_value()
            .get("rules")
            .and_then(Value::as_array)
            .and_then(|rules| rules.first())
            .cloned()
            .expect("fixture rule exists");

        let discovery = handle
            .get_resource(RBAC_API_VERSION, "ClusterRole", None, "system:discovery")
            .await
            .unwrap()
            .expect("system:discovery should exist");

        let mut patched = discovery
            .data
            .as_ref()
            .as_object()
            .cloned()
            .unwrap_or_default();
        patched.insert("rules".to_string(), Value::Array(vec![]));
        handle
            .update_resource(
                RBAC_API_VERSION,
                "ClusterRole",
                None,
                "system:discovery",
                Value::Object(patched),
                discovery.resource_version,
            )
            .await
            .unwrap();

        reconcile_default_rbac_objects(handle.as_ref())
            .await
            .unwrap();

        let updated = handle
            .get_resource(RBAC_API_VERSION, "ClusterRole", None, "system:discovery")
            .await
            .unwrap()
            .expect("system:discovery should exist");

        let rules = updated
            .data
            .get("rules")
            .and_then(Value::as_array)
            .expect("system:discovery should have rules");

        assert!(
            has_rule(rules, &expected_rule),
            "system:discovery should restore missing default rule"
        );
    }

    #[tokio::test]
    async fn reconcile_preserves_user_edits_when_autoupdate_false() {
        let db = crate::datastore::test_support::in_memory().await;
        let handle = as_handle(&db);
        reconcile_default_rbac_objects(handle.as_ref())
            .await
            .unwrap();

        let discovery = handle
            .get_resource(RBAC_API_VERSION, "ClusterRole", None, "system:discovery")
            .await
            .unwrap()
            .expect("system:discovery should exist");

        let mut patched = discovery
            .data
            .as_ref()
            .as_object()
            .cloned()
            .unwrap_or_default();
        if let Some(Value::Object(metadata)) = patched.get_mut("metadata") {
            if let Some(Value::Object(annotations)) = metadata.get_mut("annotations") {
                annotations.insert(
                    AUTOUPDATE_ANNOTATION.to_string(),
                    Value::String("false".to_string()),
                );
            } else {
                metadata.insert(
                    "annotations".to_string(),
                    serde_json::json!({AUTOUPDATE_ANNOTATION: "false"}),
                );
            }
        }
        patched.insert("rules".to_string(), Value::Array(vec![]));

        handle
            .update_resource(
                RBAC_API_VERSION,
                "ClusterRole",
                None,
                "system:discovery",
                Value::Object(patched),
                discovery.resource_version,
            )
            .await
            .unwrap();

        reconcile_default_rbac_objects(handle.as_ref())
            .await
            .unwrap();

        let updated = handle
            .get_resource(RBAC_API_VERSION, "ClusterRole", None, "system:discovery")
            .await
            .unwrap()
            .expect("system:discovery should exist");

        let annotations = updated
            .data
            .get("metadata")
            .and_then(|m| m.get("annotations"))
            .and_then(Value::as_object)
            .expect("metadata.annotations should exist");
        assert_eq!(
            annotations
                .get(AUTOUPDATE_ANNOTATION)
                .and_then(Value::as_str),
            Some("false"),
            "autoupdate=false should be preserved"
        );

        let rules = updated
            .data
            .get("rules")
            .and_then(Value::as_array)
            .expect("system:discovery should have rules");
        assert!(
            rules.is_empty(),
            "autoupdate=false should preserve user edits"
        );
    }

    #[tokio::test]
    async fn reconcile_repairs_missing_namespaced_role_rule_when_autoupdate_enabled() {
        let db = crate::datastore::test_support::in_memory().await;
        let handle = as_handle(&db);
        reconcile_default_rbac_objects(handle.as_ref())
            .await
            .unwrap();

        let fixture = super::default_rbac_fixtures()
            .into_iter()
            .find(|object| {
                object.kind == "Role"
                    && object.namespace == Some("kube-system")
                    && object.name == "extension-apiserver-authentication-reader"
            })
            .expect("fixture exists");
        let expected_rule = fixture
            .to_json_value()
            .get("rules")
            .and_then(Value::as_array)
            .and_then(|rules| rules.first())
            .cloned()
            .expect("fixture rule exists");

        let role = handle
            .get_resource(
                RBAC_API_VERSION,
                "Role",
                Some("kube-system"),
                "extension-apiserver-authentication-reader",
            )
            .await
            .unwrap()
            .expect("extension apiserver auth reader Role should exist");

        let mut patched = role.data.as_ref().as_object().cloned().unwrap_or_default();
        patched.insert("rules".to_string(), Value::Array(vec![]));
        handle
            .update_resource(
                RBAC_API_VERSION,
                "Role",
                Some("kube-system"),
                "extension-apiserver-authentication-reader",
                Value::Object(patched),
                role.resource_version,
            )
            .await
            .unwrap();

        reconcile_default_rbac_objects(handle.as_ref())
            .await
            .unwrap();

        let updated = handle
            .get_resource(
                RBAC_API_VERSION,
                "Role",
                Some("kube-system"),
                "extension-apiserver-authentication-reader",
            )
            .await
            .unwrap()
            .expect("extension apiserver auth reader Role should exist");

        let rules = updated
            .data
            .get("rules")
            .and_then(Value::as_array)
            .expect("Role should have rules");

        assert!(
            has_rule(rules, &expected_rule),
            "extension apiserver auth reader Role should restore missing default rule"
        );
    }

    #[tokio::test]
    async fn reconcile_aggregates_labeled_cluster_role_rules_into_user_facing_roles() {
        let db = crate::datastore::test_support::in_memory().await;
        let handle = as_handle(&db);
        reconcile_default_rbac_objects(handle.as_ref())
            .await
            .unwrap();

        let source_rule = serde_json::json!({
            "verbs": ["get"],
            "apiGroups": ["example.com"],
            "resources": ["widgets"],
            "resourceNames": [],
            "nonResourceURLs": []
        });
        handle
            .create_resource(
                RBAC_API_VERSION,
                "ClusterRole",
                None,
                "example-widget-viewer",
                serde_json::json!({
                    "apiVersion": RBAC_API_VERSION,
                    "kind": "ClusterRole",
                    "metadata": {
                        "name": "example-widget-viewer",
                        "labels": {"rbac.authorization.k8s.io/aggregate-to-view": "true"}
                    },
                    "rules": [source_rule.clone()]
                }),
            )
            .await
            .unwrap();

        reconcile_default_rbac_objects(handle.as_ref())
            .await
            .unwrap();

        let view = handle
            .get_resource(RBAC_API_VERSION, "ClusterRole", None, "view")
            .await
            .unwrap()
            .expect("view ClusterRole should exist");
        let view_rules = view
            .data
            .get("rules")
            .and_then(Value::as_array)
            .expect("view should have rules");
        assert!(
            has_rule(view_rules, &source_rule),
            "view should include rules from ClusterRoles labeled aggregate-to-view"
        );

        let admin = handle
            .get_resource(RBAC_API_VERSION, "ClusterRole", None, "admin")
            .await
            .unwrap()
            .expect("admin ClusterRole should exist");
        let admin_rules = admin
            .data
            .get("rules")
            .and_then(Value::as_array)
            .expect("admin should have rules");
        assert!(
            !has_rule(admin_rules, &source_rule),
            "aggregate-to-view must not leak into admin without the admin label"
        );
    }

    #[tokio::test]
    async fn default_admin_edit_view_carry_aggregation_rule() {
        let db = crate::datastore::test_support::in_memory().await;
        let handle = as_handle(&db);
        reconcile_default_rbac_objects(handle.as_ref())
            .await
            .unwrap();

        for (name, label) in [
            ("admin", "rbac.authorization.k8s.io/aggregate-to-admin"),
            ("edit", "rbac.authorization.k8s.io/aggregate-to-edit"),
            ("view", "rbac.authorization.k8s.io/aggregate-to-view"),
        ] {
            let role = handle
                .get_resource(RBAC_API_VERSION, "ClusterRole", None, name)
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("{name} ClusterRole should exist"));
            let selectors = role
                .data
                .pointer("/aggregationRule/clusterRoleSelectors")
                .and_then(Value::as_array)
                .unwrap_or_else(|| {
                    panic!("{name} must expose aggregationRule.clusterRoleSelectors")
                });
            assert!(
                selectors.iter().any(|selector| {
                    selector
                        .pointer("/matchLabels")
                        .and_then(Value::as_object)
                        .and_then(|labels| labels.get(label))
                        .and_then(Value::as_str)
                        == Some("true")
                }),
                "{name} aggregationRule must select {label}"
            );
        }
    }

    #[tokio::test]
    async fn reconcile_revokes_aggregated_rules_when_source_label_removed() {
        let db = crate::datastore::test_support::in_memory().await;
        let handle = as_handle(&db);
        reconcile_default_rbac_objects(handle.as_ref())
            .await
            .unwrap();

        let source_rule = serde_json::json!({
            "verbs": ["get"],
            "apiGroups": ["example.com"],
            "resources": ["widgets"],
            "resourceNames": [],
            "nonResourceURLs": []
        });
        handle
            .create_resource(
                RBAC_API_VERSION,
                "ClusterRole",
                None,
                "example-widget-viewer",
                serde_json::json!({
                    "apiVersion": RBAC_API_VERSION,
                    "kind": "ClusterRole",
                    "metadata": {
                        "name": "example-widget-viewer",
                        "labels": {"rbac.authorization.k8s.io/aggregate-to-view": "true"}
                    },
                    "rules": [source_rule.clone()]
                }),
            )
            .await
            .unwrap();

        reconcile_default_rbac_objects(handle.as_ref())
            .await
            .unwrap();

        let view = handle
            .get_resource(RBAC_API_VERSION, "ClusterRole", None, "view")
            .await
            .unwrap()
            .expect("view ClusterRole should exist");
        let view_rules = view
            .data
            .get("rules")
            .and_then(Value::as_array)
            .expect("view should have rules");
        assert!(
            has_rule(view_rules, &source_rule),
            "view should aggregate the labeled source rule"
        );
        let view_floor_len = super::default_cluster_role_rules("view").len();
        assert!(view_rules.len() > view_floor_len);

        // Drop the aggregate-to-view label from the source role.
        let source = handle
            .get_resource(
                RBAC_API_VERSION,
                "ClusterRole",
                None,
                "example-widget-viewer",
            )
            .await
            .unwrap()
            .expect("source role exists");
        let mut patched = source
            .data
            .as_ref()
            .as_object()
            .cloned()
            .unwrap_or_default();
        if let Some(Value::Object(metadata)) = patched.get_mut("metadata") {
            metadata.insert("labels".to_string(), serde_json::json!({}));
        }
        handle
            .update_resource(
                RBAC_API_VERSION,
                "ClusterRole",
                None,
                "example-widget-viewer",
                Value::Object(patched),
                source.resource_version,
            )
            .await
            .unwrap();

        reconcile_default_rbac_objects(handle.as_ref())
            .await
            .unwrap();

        let view = handle
            .get_resource(RBAC_API_VERSION, "ClusterRole", None, "view")
            .await
            .unwrap()
            .expect("view ClusterRole should exist");
        let view_rules = view
            .data
            .get("rules")
            .and_then(Value::as_array)
            .expect("view should have rules");
        assert!(
            !has_rule(view_rules, &source_rule),
            "revoked source rule must be removed from view after the label is dropped"
        );
        assert_eq!(
            view_rules.len(),
            view_floor_len,
            "view must retain its default floor rules after revocation"
        );
    }

    #[tokio::test]
    async fn reconcile_honors_user_defined_aggregation_rule_selectors() {
        let db = crate::datastore::test_support::in_memory().await;
        let handle = as_handle(&db);
        reconcile_default_rbac_objects(handle.as_ref())
            .await
            .unwrap();

        // A user-defined aggregated ClusterRole with its own selector.
        handle
            .create_resource(
                RBAC_API_VERSION,
                "ClusterRole",
                None,
                "monitoring",
                serde_json::json!({
                    "apiVersion": RBAC_API_VERSION,
                    "kind": "ClusterRole",
                    "metadata": {"name": "monitoring"},
                    "aggregationRule": {
                        "clusterRoleSelectors": [
                            {"matchLabels": {"example.com/aggregate-to-monitoring": "true"}}
                        ]
                    },
                    "rules": []
                }),
            )
            .await
            .unwrap();

        let monitoring_rule = serde_json::json!({
            "verbs": ["get", "list", "watch"],
            "apiGroups": ["monitoring.example.com"],
            "resources": ["dashboards"],
            "resourceNames": [],
            "nonResourceURLs": []
        });
        handle
            .create_resource(
                RBAC_API_VERSION,
                "ClusterRole",
                None,
                "dashboard-reader",
                serde_json::json!({
                    "apiVersion": RBAC_API_VERSION,
                    "kind": "ClusterRole",
                    "metadata": {
                        "name": "dashboard-reader",
                        "labels": {"example.com/aggregate-to-monitoring": "true"}
                    },
                    "rules": [monitoring_rule.clone()]
                }),
            )
            .await
            .unwrap();

        reconcile_default_rbac_objects(handle.as_ref())
            .await
            .unwrap();

        let monitoring = handle
            .get_resource(RBAC_API_VERSION, "ClusterRole", None, "monitoring")
            .await
            .unwrap()
            .expect("monitoring ClusterRole should exist");
        let monitoring_rules = monitoring
            .data
            .get("rules")
            .and_then(Value::as_array)
            .expect("monitoring should have rules");
        assert!(
            has_rule(monitoring_rules, &monitoring_rule),
            "user-defined aggregationRule selectors must aggregate matching source rules"
        );
    }
}
