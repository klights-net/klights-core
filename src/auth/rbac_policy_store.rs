//! RBAC policy store traits and transport-neutral implementations.
//!
//! Root composition supplies an `RbacResourceReader` adapter; auth owns only
//! policy resolution and in-memory fakes.

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

use crate::auth::rbac_rule_evaluator::{PolicyRule, Subject, SubjectKind};

/// A resolved role binding: the subjects and the rules they grant.
#[derive(Clone, Debug)]
pub struct ResolvedBinding {
    pub subjects: Vec<Subject>,
    pub rules: Vec<PolicyRule>,
    pub namespace: Option<String>,
}

/// An effective resource rule for SelfSubjectRulesReview.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectiveResourceRule {
    pub verbs: Vec<String>,
    pub api_group: String,
    pub resource: String,
    pub resource_names: Vec<String>,
}

/// An effective non-resource rule for SelfSubjectRulesReview.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectiveNonResourceRule {
    pub verbs: Vec<String>,
    pub non_resource_urls: Vec<String>,
}

/// Trait for reading RBAC policy objects. Production uses the datastore;
/// tests use `InMemoryRbacPolicyStore`.
#[async_trait]
pub trait RbacPolicyStore: Send + Sync {
    /// Return all role bindings that could apply to the given namespace
    /// (namespace-scoped RoleBindings in this namespace plus all
    /// ClusterRoleBindings).
    async fn list_bindings_for_namespace(&self, namespace: Option<&str>) -> Vec<ResolvedBinding>;

    /// Enumerate effective rules for an identity in a namespace.
    /// Returns (resource_rules, non_resource_rules, incomplete).
    /// resource_rules preserve the applicable RBAC rule dimensions without
    /// normalizing wildcards or collapsing distinct `resourceNames` scopes.
    async fn enumerate_effective_rules(
        &self,
        identity: &crate::auth::identity::AuthenticatedIdentity,
        namespace: Option<&str>,
    ) -> (
        Vec<EffectiveResourceRule>,
        Vec<EffectiveNonResourceRule>,
        bool,
    ) {
        let bindings = self.list_bindings_for_namespace(namespace).await;
        let mut resource_rules: Vec<EffectiveResourceRule> = Vec::new();
        let mut non_resource_rules: Vec<EffectiveNonResourceRule> = Vec::new();

        for binding in &bindings {
            let subject_match = binding.subjects.iter().any(|s| {
                crate::auth::rbac_rule_evaluator::subject_matches(
                    s,
                    &identity.username,
                    &identity.groups,
                )
            });
            if !subject_match {
                continue;
            }

            for rule in &binding.rules {
                for resource in &rule.resources {
                    for api_group in &rule.api_groups {
                        let effective = EffectiveResourceRule {
                            verbs: rule.verbs.clone(),
                            api_group: api_group.clone(),
                            resource: resource.clone(),
                            resource_names: rule.resource_names.clone(),
                        };
                        if !resource_rules.contains(&effective) {
                            resource_rules.push(effective);
                        }
                    }
                }

                if !rule.non_resource_urls.is_empty() {
                    let effective = EffectiveNonResourceRule {
                        verbs: rule.verbs.clone(),
                        non_resource_urls: rule.non_resource_urls.clone(),
                    };
                    if !non_resource_rules.contains(&effective) {
                        non_resource_rules.push(effective);
                    }
                }
            }
        }

        (resource_rules, non_resource_rules, false)
    }
}

/// Focused transport-neutral reader for raw Kubernetes RBAC resources.
///
/// Concrete datastore or remote leader adapters are composed outside auth.
#[async_trait]
pub trait RbacResourceReader: Send + Sync {
    async fn list_cluster_rbac_resources(&self, kind: &str) -> Result<Vec<Value>, String>;

    async fn list_namespaced_rbac_resources(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Value>, String>;
}

/// In-memory policy store for tests.
pub struct InMemoryRbacPolicyStore {
    bindings: Vec<ResolvedBinding>,
}

impl InMemoryRbacPolicyStore {
    pub fn new(bindings: Vec<ResolvedBinding>) -> Self {
        Self { bindings }
    }

    pub fn empty() -> Self {
        Self { bindings: vec![] }
    }
}

#[async_trait]
impl RbacPolicyStore for InMemoryRbacPolicyStore {
    async fn list_bindings_for_namespace(&self, namespace: Option<&str>) -> Vec<ResolvedBinding> {
        self.bindings
            .iter()
            .filter(|b| {
                b.namespace.is_none() || namespace.is_some() && b.namespace.as_deref() == namespace
            })
            .cloned()
            .map(|mut b| {
                // Phase 2B: default SA subjects to binding namespace
                if let Some(ns) = b.namespace.as_deref() {
                    default_service_account_subjects(&mut b.subjects, ns);
                }
                b
            })
            .collect()
    }
}

/// Resolves ClusterRole/ClusterRoleBinding and Role/RoleBinding objects from
/// an injected focused resource reader on each authorization check.
pub struct ReaderBackedRbacPolicyStore {
    reader: Arc<dyn RbacResourceReader>,
}

impl ReaderBackedRbacPolicyStore {
    pub fn new(reader: Arc<dyn RbacResourceReader>) -> Self {
        Self { reader }
    }
}

#[async_trait]
impl RbacPolicyStore for ReaderBackedRbacPolicyStore {
    async fn list_bindings_for_namespace(&self, namespace: Option<&str>) -> Vec<ResolvedBinding> {
        let mut bindings: Vec<ResolvedBinding> = Vec::new();

        // Load all ClusterRoles (needed for ClusterRoleBinding resolution)
        let cluster_roles = match load_cluster_roles(self.reader.as_ref()).await {
            Ok(roles) => roles,
            Err(err) => {
                tracing::warn!("failed to load ClusterRoles for RBAC: {err}");
                return bindings;
            }
        };

        // Load all ClusterRoleBindings → resolve to ResolvedBindings
        if let Ok(crb_list) = self
            .reader
            .list_cluster_rbac_resources("ClusterRoleBinding")
            .await
        {
            for crb in &crb_list {
                if let Some(binding) = resolve_cluster_role_binding(crb, &cluster_roles) {
                    bindings.push(binding);
                }
            }
        }

        // Load Roles for this namespace
        if let Some(ns) = namespace {
            let roles = match load_namespaced_roles(self.reader.as_ref(), ns).await {
                Ok(r) => r,
                Err(err) => {
                    tracing::warn!("failed to load Roles for namespace {ns}: {err}");
                    return bindings;
                }
            };

            // Load RoleBindings for this namespace → resolve
            if let Ok(rb_list) = self
                .reader
                .list_namespaced_rbac_resources(ns, "RoleBinding")
                .await
            {
                for rb in &rb_list {
                    if let Some(binding) = resolve_role_binding(rb, &roles, &cluster_roles, ns) {
                        bindings.push(binding);
                    }
                }
            }
        }

        bindings
    }
}

#[derive(Deserialize)]
struct RoleRef {
    #[serde(rename = "apiGroup")]
    api_group: String,
    kind: String,
    name: String,
}

#[derive(Deserialize)]
struct BindingObject {
    #[serde(rename = "roleRef")]
    role_ref: RoleRef,
    subjects: Vec<Value>,
}

#[derive(Deserialize)]
struct RuleObject {
    verbs: Vec<String>,
    #[serde(rename = "apiGroups", default)]
    api_groups: Vec<String>,
    #[serde(default)]
    resources: Vec<String>,
    #[serde(rename = "resourceNames", default)]
    resource_names: Vec<String>,
    #[serde(rename = "nonResourceURLs", default)]
    non_resource_urls: Vec<String>,
}

#[derive(Deserialize)]
struct RoleObject {
    rules: Vec<RuleObject>,
}

fn parse_subjects(subjects: &[Value]) -> Vec<Subject> {
    subjects
        .iter()
        .filter_map(|s| {
            let kind_str = s.get("kind")?.as_str()?;
            let name = s.get("name")?.as_str()?.to_string();
            let ns = s
                .get("namespace")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let kind = match kind_str {
                "User" => SubjectKind::User,
                "Group" => SubjectKind::Group,
                "ServiceAccount" => SubjectKind::ServiceAccount,
                _ => return None,
            };
            Some(Subject {
                kind,
                name,
                namespace: ns,
            })
        })
        .collect()
}

fn parse_rules(rules: &[RuleObject]) -> Vec<PolicyRule> {
    rules
        .iter()
        .map(|r| PolicyRule {
            verbs: r.verbs.clone(),
            api_groups: r.api_groups.clone(),
            resources: r.resources.clone(),
            resource_names: r.resource_names.clone(),
            non_resource_urls: r.non_resource_urls.clone(),
        })
        .collect()
}

fn resolve_cluster_role_binding(
    crb: &Value,
    cluster_roles: &std::collections::HashMap<String, Vec<PolicyRule>>,
) -> Option<ResolvedBinding> {
    let binding: BindingObject = serde_json::from_value(crb.clone()).ok()?;
    if !is_supported_role_ref_api_group(&binding.role_ref.api_group) {
        return None;
    }
    if binding.role_ref.kind != "ClusterRole" {
        return None;
    }
    let rules = cluster_roles.get(&binding.role_ref.name)?.clone();
    let subjects = parse_subjects(&binding.subjects);
    Some(ResolvedBinding {
        subjects,
        rules,
        namespace: None,
    })
}

fn resolve_role_binding(
    rb: &Value,
    roles: &std::collections::HashMap<String, Vec<PolicyRule>>,
    cluster_roles: &std::collections::HashMap<String, Vec<PolicyRule>>,
    namespace: &str,
) -> Option<ResolvedBinding> {
    let binding: BindingObject = serde_json::from_value(rb.clone()).ok()?;
    if !is_supported_role_ref_api_group(&binding.role_ref.api_group) {
        return None;
    }
    let rules = match binding.role_ref.kind.as_str() {
        "Role" => roles.get(&binding.role_ref.name)?.clone(),
        "ClusterRole" => cluster_roles.get(&binding.role_ref.name)?.clone(),
        _ => return None,
    };
    let mut subjects = parse_subjects(&binding.subjects);
    // Phase 2B: default ServiceAccount subjects to the binding's namespace
    default_service_account_subjects(&mut subjects, namespace);
    Some(ResolvedBinding {
        subjects,
        rules,
        namespace: Some(namespace.to_string()),
    })
}

fn is_supported_role_ref_api_group(api_group: &str) -> bool {
    api_group.is_empty() || api_group == "rbac.authorization.k8s.io"
}

/// Default ServiceAccount subjects in namespace-scoped bindings to the
/// binding's namespace. ClusterRoleBindings do NOT default SA namespaces.
fn default_service_account_subjects(subjects: &mut [Subject], namespace: &str) {
    for subject in subjects.iter_mut() {
        if subject.kind == SubjectKind::ServiceAccount && subject.namespace.is_none() {
            subject.namespace = Some(namespace.to_string());
        }
    }
}

async fn load_cluster_roles(
    reader: &dyn RbacResourceReader,
) -> Result<std::collections::HashMap<String, Vec<PolicyRule>>, String> {
    let list = reader.list_cluster_rbac_resources("ClusterRole").await?;
    let mut map = std::collections::HashMap::new();
    for item in &list {
        let name = item
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .map(|s| s.to_string());
        let Some(name) = name else { continue };
        let role: RoleObject = match serde_json::from_value(item.clone()) {
            Ok(r) => r,
            Err(_) => continue,
        };
        map.insert(name, parse_rules(&role.rules));
    }
    Ok(map)
}

async fn load_namespaced_roles(
    reader: &dyn RbacResourceReader,
    namespace: &str,
) -> Result<std::collections::HashMap<String, Vec<PolicyRule>>, String> {
    let list = reader
        .list_namespaced_rbac_resources(namespace, "Role")
        .await?;
    let mut map = std::collections::HashMap::new();
    for item in &list {
        let name = item
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .map(|s| s.to_string());
        let Some(name) = name else { continue };
        let role: RoleObject = match serde_json::from_value(item.clone()) {
            Ok(r) => r,
            Err(_) => continue,
        };
        map.insert(name, parse_rules(&role.rules));
    }
    Ok(map)
}

#[cfg(test)]
mod reader_tests {
    use super::*;
    use crate::auth::identity::AuthenticatedIdentity;
    use std::collections::HashMap;
    use std::sync::Arc;

    const RBAC_API_VERSION: &str = "rbac.authorization.k8s.io/v1";

    #[derive(Default)]
    struct FakeRbacResourceReader {
        resources: std::sync::Mutex<HashMap<(Option<String>, String), Vec<Value>>>,
    }

    impl FakeRbacResourceReader {
        async fn create_resource(
            &self,
            _api_version: &str,
            kind: &str,
            namespace: Option<&str>,
            _name: &str,
            value: Value,
        ) -> Result<(), String> {
            self.resources
                .lock()
                .expect("fake RBAC reader lock")
                .entry((namespace.map(str::to_string), kind.to_string()))
                .or_default()
                .push(value);
            Ok(())
        }
    }

    #[async_trait]
    impl RbacResourceReader for FakeRbacResourceReader {
        async fn list_cluster_rbac_resources(&self, kind: &str) -> Result<Vec<Value>, String> {
            Ok(self
                .resources
                .lock()
                .expect("fake RBAC reader lock")
                .get(&(None, kind.to_string()))
                .cloned()
                .unwrap_or_default())
        }

        async fn list_namespaced_rbac_resources(
            &self,
            namespace: &str,
            kind: &str,
        ) -> Result<Vec<Value>, String> {
            Ok(self
                .resources
                .lock()
                .expect("fake RBAC reader lock")
                .get(&(Some(namespace.to_string()), kind.to_string()))
                .cloned()
                .unwrap_or_default())
        }
    }

    fn reader_backed_store() -> (Arc<FakeRbacResourceReader>, ReaderBackedRbacPolicyStore) {
        let reader = Arc::new(FakeRbacResourceReader::default());
        let store = ReaderBackedRbacPolicyStore::new(reader.clone());
        (reader, store)
    }

    fn default_sa_identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity::service_account(
            "system:serviceaccount:default:default".to_string(),
            vec![
                "system:serviceaccounts".to_string(),
                "system:serviceaccounts:default".to_string(),
            ],
            None,
        )
    }

    #[tokio::test]
    async fn resolves_clusterrole_and_binding() {
        let (db, store) = reader_backed_store();

        db.create_resource(
            RBAC_API_VERSION,
            "ClusterRole",
            None,
            "test-role",
            serde_json::json!({
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "ClusterRole",
                "metadata": {"name": "test-role"},
                "rules": [{
                    "verbs": ["get", "list"],
                    "apiGroups": [""],
                    "resources": ["pods"]
                }]
            }),
        )
        .await
        .unwrap();

        db.create_resource(
            RBAC_API_VERSION,
            "ClusterRoleBinding",
            None,
            "test-binding",
            serde_json::json!({
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "ClusterRoleBinding",
                "metadata": {"name": "test-binding"},
                "roleRef": {
                    "apiGroup": "rbac.authorization.k8s.io",
                    "kind": "ClusterRole",
                    "name": "test-role"
                },
                "subjects": [{
                    "kind": "Group",
                    "apiGroup": "rbac.authorization.k8s.io",
                    "name": "system:authenticated"
                }]
            }),
        )
        .await
        .unwrap();

        let bindings = store.list_bindings_for_namespace(None).await;
        assert_eq!(bindings.len(), 1);
        let b = &bindings[0];
        assert_eq!(b.subjects.len(), 1);
        assert_eq!(b.subjects[0].kind, SubjectKind::Group);
        assert_eq!(b.subjects[0].name, "system:authenticated");
        assert_eq!(b.rules.len(), 1);
        assert_eq!(b.rules[0].verbs, vec!["get", "list"]);
        assert_eq!(b.rules[0].resources, vec!["pods"]);
        assert!(b.namespace.is_none());
    }

    #[tokio::test]
    async fn resolves_role_and_rolebinding_with_sa_defaulting() {
        let (db, store) = reader_backed_store();

        db.create_resource(
            RBAC_API_VERSION,
            "Role",
            Some("default"),
            "pod-reader",
            serde_json::json!({
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "Role",
                "metadata": {"name": "pod-reader", "namespace": "default"},
                "rules": [{
                    "verbs": ["get"],
                    "apiGroups": [""],
                    "resources": ["pods"]
                }]
            }),
        )
        .await
        .unwrap();

        db.create_resource(
            RBAC_API_VERSION,
            "RoleBinding",
            Some("default"),
            "read-pods",
            serde_json::json!({
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "RoleBinding",
                "metadata": {"name": "read-pods", "namespace": "default"},
                "roleRef": {
                    "apiGroup": "rbac.authorization.k8s.io",
                    "kind": "Role",
                    "name": "pod-reader"
                },
                "subjects": [{
                    "kind": "ServiceAccount",
                    "name": "default"
                }]
            }),
        )
        .await
        .unwrap();

        let bindings = store.list_bindings_for_namespace(Some("default")).await;
        assert_eq!(bindings.len(), 1);
        let b = &bindings[0];
        assert_eq!(b.subjects[0].kind, SubjectKind::ServiceAccount);
        assert_eq!(b.subjects[0].name, "default");
        assert_eq!(
            b.subjects[0].namespace.as_deref(),
            Some("default"),
            "SA subject namespace should be defaulted to binding namespace"
        );
        assert_eq!(b.namespace.as_deref(), Some("default"));
    }

    #[tokio::test]
    async fn rolebinding_with_empty_role_ref_api_group_resolves_role() {
        let (db, store) = reader_backed_store();

        db.create_resource(
            RBAC_API_VERSION,
            "Role",
            Some("kube-system"),
            "extension-apiserver-authentication-reader",
            serde_json::json!({
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "Role",
                "metadata": {
                    "name": "extension-apiserver-authentication-reader",
                    "namespace": "kube-system"
                },
                "rules": [{
                    "verbs": ["get", "list", "watch"],
                    "apiGroups": [""],
                    "resources": ["configmaps"],
                    "resourceNames": ["extension-apiserver-authentication"]
                }]
            }),
        )
        .await
        .unwrap();

        db.create_resource(
            RBAC_API_VERSION,
            "RoleBinding",
            Some("kube-system"),
            "wardler-auth-reader",
            serde_json::json!({
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "RoleBinding",
                "metadata": {
                    "name": "wardler-auth-reader",
                    "namespace": "kube-system"
                },
                "roleRef": {
                    "apiGroup": "",
                    "kind": "Role",
                    "name": "extension-apiserver-authentication-reader"
                },
                "subjects": [{
                    "kind": "ServiceAccount",
                    "name": "default",
                    "namespace": "aggregator-6743"
                }]
            }),
        )
        .await
        .unwrap();

        let bindings = store.list_bindings_for_namespace(Some("kube-system")).await;

        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].rules[0].resources, vec!["configmaps"]);
        assert_eq!(
            bindings[0].rules[0].resource_names,
            vec!["extension-apiserver-authentication"]
        );
    }

    #[tokio::test]
    async fn rolebinding_to_clusterrole_scoped_to_namespace() {
        let (db, store) = reader_backed_store();

        db.create_resource(
            RBAC_API_VERSION,
            "ClusterRole",
            None,
            "view-pods",
            serde_json::json!({
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "ClusterRole",
                "metadata": {"name": "view-pods"},
                "rules": [{
                    "verbs": ["get", "list", "watch"],
                    "apiGroups": [""],
                    "resources": ["pods"]
                }]
            }),
        )
        .await
        .unwrap();

        db.create_resource(
            RBAC_API_VERSION,
            "RoleBinding",
            Some("kube-system"),
            "view-pods-binding",
            serde_json::json!({
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "RoleBinding",
                "metadata": {"name": "view-pods-binding", "namespace": "kube-system"},
                "roleRef": {
                    "apiGroup": "rbac.authorization.k8s.io",
                    "kind": "ClusterRole",
                    "name": "view-pods"
                },
                "subjects": [{
                    "kind": "ServiceAccount",
                    "name": "default",
                    "namespace": "kube-system"
                }]
            }),
        )
        .await
        .unwrap();

        let bindings = store.list_bindings_for_namespace(Some("kube-system")).await;
        assert_eq!(bindings.len(), 1);
        let b = &bindings[0];
        assert_eq!(b.namespace.as_deref(), Some("kube-system"));
        assert_eq!(b.subjects[0].namespace.as_deref(), Some("kube-system"));
        assert_eq!(b.rules[0].resources, vec!["pods"]);
    }

    #[tokio::test]
    async fn enumerate_effective_rules_matches_identity() {
        let (db, store) = reader_backed_store();

        db.create_resource(
            RBAC_API_VERSION,
            "Role",
            Some("default"),
            "pod-reader",
            serde_json::json!({
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "Role",
                "metadata": {"name": "pod-reader", "namespace": "default"},
                "rules": [{
                    "verbs": ["get", "list"],
                    "apiGroups": [""],
                    "resources": ["pods"]
                }]
            }),
        )
        .await
        .unwrap();

        db.create_resource(
            RBAC_API_VERSION,
            "RoleBinding",
            Some("default"),
            "read-pods",
            serde_json::json!({
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "RoleBinding",
                "metadata": {"name": "read-pods", "namespace": "default"},
                "roleRef": {
                    "apiGroup": "rbac.authorization.k8s.io",
                    "kind": "Role",
                    "name": "pod-reader"
                },
                "subjects": [{
                    "kind": "ServiceAccount",
                    "name": "default"
                }]
            }),
        )
        .await
        .unwrap();

        let identity = default_sa_identity();
        let (resource_rules, _non_resource_rules, incomplete) = store
            .enumerate_effective_rules(&identity, Some("default"))
            .await;

        assert!(!incomplete);
        assert!(!resource_rules.is_empty());
        let pod_rule = resource_rules
            .iter()
            .find(|r| r.resource == "pods")
            .expect("should have pods rule");
        assert_eq!(pod_rule.api_group, "");
        assert!(pod_rule.verbs.contains(&"get".to_string()));
        assert!(pod_rule.verbs.contains(&"list".to_string()));
    }

    #[tokio::test]
    async fn enumerate_effective_rules_preserves_wildcards_and_subresource_wildcards() {
        let store = InMemoryRbacPolicyStore::new(vec![ResolvedBinding {
            namespace: Some("default".to_string()),
            subjects: vec![Subject {
                kind: SubjectKind::Group,
                name: "system:authenticated".to_string(),
                namespace: None,
            }],
            rules: vec![PolicyRule {
                verbs: vec!["*".to_string()],
                api_groups: vec!["*".to_string()],
                resources: vec!["pods/*".to_string()],
                resource_names: vec![],
                non_resource_urls: vec![],
            }],
        }]);
        let identity = AuthenticatedIdentity::client_cert("alice".to_string(), vec![]);

        let (resource_rules, _non_resource_rules, incomplete) = store
            .enumerate_effective_rules(&identity, Some("default"))
            .await;

        assert!(!incomplete);
        assert_eq!(resource_rules.len(), 1);
        assert_eq!(resource_rules[0].verbs, vec!["*"]);
        assert_eq!(resource_rules[0].api_group, "*");
        assert_eq!(resource_rules[0].resource, "pods/*");
    }

    #[tokio::test]
    async fn enumerate_effective_rules_keeps_different_resource_name_scopes_separate() {
        let store = InMemoryRbacPolicyStore::new(vec![ResolvedBinding {
            namespace: Some("default".to_string()),
            subjects: vec![Subject {
                kind: SubjectKind::Group,
                name: "system:authenticated".to_string(),
                namespace: None,
            }],
            rules: vec![
                PolicyRule {
                    verbs: vec!["get".to_string()],
                    api_groups: vec!["".to_string()],
                    resources: vec!["pods".to_string()],
                    resource_names: vec![],
                    non_resource_urls: vec![],
                },
                PolicyRule {
                    verbs: vec!["update".to_string()],
                    api_groups: vec!["".to_string()],
                    resources: vec!["pods".to_string()],
                    resource_names: vec!["named-pod".to_string()],
                    non_resource_urls: vec![],
                },
            ],
        }]);
        let identity = AuthenticatedIdentity::client_cert("alice".to_string(), vec![]);

        let (resource_rules, _non_resource_rules, incomplete) = store
            .enumerate_effective_rules(&identity, Some("default"))
            .await;

        assert!(!incomplete);
        assert_eq!(resource_rules.len(), 2);
        assert!(resource_rules.iter().any(|r| {
            r.verbs == vec!["get"] && r.resource == "pods" && r.resource_names.is_empty()
        }));
        assert!(resource_rules.iter().any(|r| {
            r.verbs == vec!["update"]
                && r.resource == "pods"
                && r.resource_names == vec!["named-pod"]
        }));
    }

    #[tokio::test]
    async fn empty_store_returns_empty() {
        let (_db, store) = reader_backed_store();

        let bindings = store.list_bindings_for_namespace(Some("default")).await;
        assert!(bindings.is_empty());
    }
}
