use crate::default_rbac_policy::{
    AUTOUPDATE_ANNOTATION, RBAC_API_VERSION, default_cluster_role_rules, default_rbac_fixtures,
};
use crate::rbac_reconcile::*;
use async_trait::async_trait;
use klights_cluster_core::Resource;
use klights_reconcile_api::ControllerStoreResult;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

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
        fn strings(value: Option<&Value>) -> BTreeSet<String> {
            value
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        }
        Self {
            verbs: strings(rule.get("verbs")),
            api_groups: strings(rule.get("apiGroups")),
            resources: strings(rule.get("resources")),
            resource_names: strings(rule.get("resourceNames")),
            non_resource_urls: strings(rule.get("nonResourceURLs")),
        }
    }
}

type ObjectKey = (String, Option<String>, String);

#[derive(Default)]
struct MemoryRbacStore {
    objects: Mutex<BTreeMap<ObjectKey, Resource>>,
    next_resource_version: AtomicI64,
}

impl MemoryRbacStore {
    fn key(kind: &str, namespace: Option<&str>, name: &str) -> ObjectKey {
        (
            kind.to_string(),
            namespace.map(str::to_string),
            name.to_string(),
        )
    }

    fn store(&self, kind: &str, namespace: Option<&str>, name: &str, mut value: Value) -> Resource {
        let resource_version = self.next_resource_version.fetch_add(1, Ordering::Relaxed) + 1;
        value["metadata"]["resourceVersion"] = Value::String(resource_version.to_string());
        let resource = Resource::try_from_data(Arc::new(value)).expect("valid RBAC resource");
        self.objects
            .lock()
            .unwrap()
            .insert(Self::key(kind, namespace, name), resource.clone());
        resource
    }

    async fn get_resource(
        &self,
        _api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        self.get_rbac_object(kind, namespace, name).await
    }

    async fn create_resource(
        &self,
        _api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: Value,
    ) -> ControllerStoreResult<Resource> {
        self.create_rbac_object(kind, namespace, name, value).await
    }

    async fn update_resource(
        &self,
        _api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: Value,
        expected_resource_version: i64,
    ) -> ControllerStoreResult<Resource> {
        self.update_rbac_object(kind, namespace, name, value, expected_resource_version)
            .await
    }
}

#[async_trait]
impl RbacPolicyStore for MemoryRbacStore {
    async fn get_rbac_object(
        &self,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        Ok(self
            .objects
            .lock()
            .unwrap()
            .get(&Self::key(kind, namespace, name))
            .cloned())
    }

    async fn create_rbac_object(
        &self,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: Value,
    ) -> ControllerStoreResult<Resource> {
        Ok(self.store(kind, namespace, name, value))
    }

    async fn update_rbac_object(
        &self,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: Value,
        expected_resource_version: i64,
    ) -> ControllerStoreResult<Resource> {
        let current = self
            .objects
            .lock()
            .unwrap()
            .get(&Self::key(kind, namespace, name))
            .cloned()
            .expect("RBAC update target");
        assert_eq!(current.resource_version, expected_resource_version);
        Ok(self.store(kind, namespace, name, value))
    }

    async fn list_cluster_roles(&self) -> ControllerStoreResult<Vec<Resource>> {
        Ok(self
            .objects
            .lock()
            .unwrap()
            .iter()
            .filter(|((kind, namespace, _), _)| kind == "ClusterRole" && namespace.is_none())
            .map(|(_, resource)| resource.clone())
            .collect())
    }
}

fn as_handle(db: &Arc<MemoryRbacStore>) -> Arc<MemoryRbacStore> {
    db.clone()
}

fn has_rule(rules: &[Value], expected: &Value) -> bool {
    let expected_shape = RuleShape::from_rule(expected);
    rules
        .iter()
        .any(|rule| RuleShape::from_rule(rule) == expected_shape)
}

#[test]
fn cluster_admin_fixture_authorizes_resource_and_non_resource_requests() {
    let fixture = default_rbac_fixtures()
        .into_iter()
        .find(|object| object.kind == "ClusterRole" && object.name == "cluster-admin")
        .expect("cluster-admin fixture exists");
    let rules = fixture.rules.expect("cluster-admin rules");
    assert!(rules.iter().any(|rule| {
        rule.verbs == vec!["*"]
            && rule.api_groups == vec!["*"]
            && rule.resources == vec!["*"]
            && rule.non_resource_urls.is_empty()
    }));
    assert!(rules.iter().any(|rule| {
        rule.verbs == vec!["*"]
            && rule.api_groups.is_empty()
            && rule.resources.is_empty()
            && rule.non_resource_urls == vec!["*"]
    }));
}

#[tokio::test]
async fn reconcile_default_rbac_objects_creates_missing_objects() {
    let db = Arc::new(MemoryRbacStore::default());
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
    let db = Arc::new(MemoryRbacStore::default());
    let handle = as_handle(&db);
    reconcile_default_rbac_objects(handle.as_ref())
        .await
        .unwrap();

    let fixture = default_rbac_fixtures()
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
    let db = Arc::new(MemoryRbacStore::default());
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
    let db = Arc::new(MemoryRbacStore::default());
    let handle = as_handle(&db);
    reconcile_default_rbac_objects(handle.as_ref())
        .await
        .unwrap();

    let fixture = default_rbac_fixtures()
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
    let db = Arc::new(MemoryRbacStore::default());
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
    let db = Arc::new(MemoryRbacStore::default());
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
            .unwrap_or_else(|| panic!("{name} must expose aggregationRule.clusterRoleSelectors"));
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
    let db = Arc::new(MemoryRbacStore::default());
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
    let view_floor_len = default_cluster_role_rules("view").len();
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
    let db = Arc::new(MemoryRbacStore::default());
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
