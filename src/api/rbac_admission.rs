//! Built-in admission for RBAC writes: privilege-escalation and bind checks.
//!
//! Kubernetes enforces two extra checks on the `rbac.authorization.k8s.io`
//! write path, on top of normal `create`/`update` authorization:
//!
//! * **escalate** — a user may not create/update a Role or ClusterRole that
//!   contains permissions the user does not already hold, unless the user has
//!   the `escalate` verb on roles/clusterroles.
//! * **bind** — a user may not create/update a RoleBinding or
//!   ClusterRoleBinding that references a role whose permissions exceed what the
//!   user holds, unless the user has the `bind` verb on the referenced role.
//!
//! Without these, anyone granted `create`/`update` on (cluster)rolebindings
//! could bind themselves to cluster-admin. This runs as a built-in admission
//! step on RBAC create/update before the object is persisted.

use crate::api::{AppError, AppState};
use crate::auth::identity::AuthenticatedIdentity;
use crate::auth::rbac_rule_evaluator::{PolicyRule, rules_cover_all};
use crate::auth::request_attributes::AuthorizationRequest;
use serde_json::Value;

const RBAC_GROUP: &str = "rbac.authorization.k8s.io";

/// Enforce the RBAC escalation/bind admission rules for a create or update of an
/// `rbac.authorization.k8s.io` resource. A no-op for every other resource.
pub async fn enforce_rbac_write_authorization(
    state: &AppState,
    identity: &AuthenticatedIdentity,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    object: &Value,
) -> Result<(), AppError> {
    // Only RBAC resources are subject to escalation/bind checks.
    if api_group_of(api_version) != RBAC_GROUP {
        return Ok(());
    }
    // system:masters holds `*` and is exempt (mirrors SystemMastersAuthorizer).
    if identity.is_admin() {
        return Ok(());
    }

    match kind {
        "Role" | "ClusterRole" => {
            enforce_role_escalation(state, identity, kind, namespace, object).await
        }
        "RoleBinding" | "ClusterRoleBinding" => {
            enforce_binding_escalation(state, identity, kind, namespace, object).await
        }
        _ => Ok(()),
    }
}

async fn enforce_role_escalation(
    state: &AppState,
    identity: &AuthenticatedIdentity,
    kind: &str,
    namespace: Option<&str>,
    object: &Value,
) -> Result<(), AppError> {
    let requested = parse_policy_rules(object.get("rules"));
    if requested.is_empty() {
        return Ok(());
    }

    // ClusterRole rules are cluster-scoped; Role rules are scoped to the Role's
    // namespace.
    let holder_ns = if kind == "ClusterRole" {
        None
    } else {
        namespace
    };
    let holder = holder_rules(state, identity, holder_ns).await;

    if rules_cover_all(&holder, &requested) {
        return Ok(());
    }

    // The `escalate` verb on the role resource overrides the check.
    let resource = if kind == "ClusterRole" {
        "clusterroles"
    } else {
        "roles"
    };
    if has_verb(state, identity, "escalate", resource, namespace, None).await {
        return Ok(());
    }

    Err(AppError::Forbidden(format!(
        "user \"{}\" cannot create/update {kind} with rules that exceed the \
         user's own permissions (requires the \"escalate\" verb on {resource}.{RBAC_GROUP})",
        identity.username
    )))
}

async fn enforce_binding_escalation(
    state: &AppState,
    identity: &AuthenticatedIdentity,
    kind: &str,
    namespace: Option<&str>,
    object: &Value,
) -> Result<(), AppError> {
    let Some(role_ref) = object.get("roleRef") else {
        return Ok(());
    };
    let ref_kind = role_ref
        .get("kind")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let ref_name = role_ref
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    if ref_name.is_empty() {
        return Ok(());
    }

    // Resolve the rules granted by the referenced role.
    let role_rules = referenced_role_rules(state, ref_kind, ref_name, namespace).await;
    if role_rules.is_empty() {
        // A role that grants nothing (or does not yet exist) cannot escalate.
        return Ok(());
    }

    let holder = holder_rules(state, identity, namespace).await;
    if rules_cover_all(&holder, &role_rules) {
        return Ok(());
    }

    // The `bind` verb on the referenced role (by name) overrides the check.
    let ref_resource = if ref_kind == "ClusterRole" {
        "clusterroles"
    } else {
        "roles"
    };
    if has_verb(
        state,
        identity,
        "bind",
        ref_resource,
        namespace,
        Some(ref_name),
    )
    .await
    {
        return Ok(());
    }

    Err(AppError::Forbidden(format!(
        "user \"{}\" cannot create/update {kind} that grants permissions the user \
         does not hold (requires the \"bind\" verb on {ref_resource}.{RBAC_GROUP} \
         \"{ref_name}\")",
        identity.username
    )))
}

/// The effective rules the identity holds in the given scope, as `PolicyRule`s.
async fn holder_rules(
    state: &AppState,
    identity: &AuthenticatedIdentity,
    namespace: Option<&str>,
) -> Vec<PolicyRule> {
    let (resource_rules, non_resource_rules, _incomplete) = state
        .rbac_policy_store
        .enumerate_effective_rules(identity, namespace)
        .await;
    let mut rules: Vec<PolicyRule> = Vec::new();
    for r in resource_rules {
        rules.push(PolicyRule {
            verbs: r.verbs,
            api_groups: vec![r.api_group],
            resources: vec![r.resource],
            resource_names: r.resource_names,
            non_resource_urls: vec![],
        });
    }
    for r in non_resource_rules {
        rules.push(PolicyRule {
            verbs: r.verbs,
            api_groups: vec![],
            resources: vec![],
            resource_names: vec![],
            non_resource_urls: r.non_resource_urls,
        });
    }
    rules
}

/// Load the rules of the role referenced by a binding. Returns an empty vec if
/// the role does not exist (binding to a nonexistent role grants nothing).
async fn referenced_role_rules(
    state: &AppState,
    ref_kind: &str,
    ref_name: &str,
    binding_namespace: Option<&str>,
) -> Vec<PolicyRule> {
    let api_version = format!("{RBAC_GROUP}/v1");
    let resource = match ref_kind {
        "ClusterRole" => state
            .db
            .get_resource(&api_version, "ClusterRole", None, ref_name)
            .await
            .ok()
            .flatten(),
        "Role" => {
            let Some(ns) = binding_namespace else {
                return Vec::new();
            };
            state
                .db
                .get_resource(&api_version, "Role", Some(ns), ref_name)
                .await
                .ok()
                .flatten()
        }
        _ => None,
    };
    match resource {
        Some(r) => parse_policy_rules(r.data.get("rules")),
        None => Vec::new(),
    }
}

/// Does the identity hold `verb` on `resource` (optionally name-scoped)?
async fn has_verb(
    state: &AppState,
    identity: &AuthenticatedIdentity,
    verb: &str,
    resource: &str,
    namespace: Option<&str>,
    name: Option<&str>,
) -> bool {
    let request =
        AuthorizationRequest::resource(verb, RBAC_GROUP, "v1", resource, None, namespace, name);
    state.authorizer.authorize(identity, &request).await.allowed
}

fn api_group_of(api_version: &str) -> &str {
    api_version.rsplit_once('/').map(|(g, _)| g).unwrap_or("")
}

fn string_array(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn parse_policy_rules(rules: Option<&Value>) -> Vec<PolicyRule> {
    let Some(arr) = rules.and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    arr.iter()
        .map(|rule| PolicyRule {
            verbs: string_array(rule.get("verbs")),
            api_groups: string_array(rule.get("apiGroups")),
            resources: string_array(rule.get("resources")),
            resource_names: string_array(rule.get("resourceNames")),
            non_resource_urls: string_array(rule.get("nonResourceURLs")),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use std::sync::Arc;
    use tower::ServiceExt;

    use crate::auth::authorizer::{AuthorizerChain, DenyAuthorizer, SystemMastersAuthorizer};
    use crate::auth::identity::AuthenticatedIdentity;
    use crate::auth::rbac_authorizer::RbacAuthorizer;
    use crate::auth::rbac_policy_store::DatastoreRbacPolicyStore;

    fn cluster_role(name: &str, rules: serde_json::Value) -> serde_json::Value {
        json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRole",
            "metadata": {"name": name},
            "rules": rules,
        })
    }

    fn cluster_role_binding(name: &str, role: &str, user: &str) -> serde_json::Value {
        json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRoleBinding",
            "metadata": {"name": name},
            "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": role},
            "subjects": [{"kind": "User", "apiGroup": "rbac.authorization.k8s.io", "name": user}],
        })
    }

    /// Build a router whose authorizer + policy store both read the same db, and
    /// seed a ClusterRole granting `alice` the given rules + a binding.
    async fn alice_router(alice_rules: serde_json::Value) -> axum::Router {
        let mut state = crate::api::test_support::build_test_app_state().await;
        let store = Arc::new(DatastoreRbacPolicyStore::new(state.db.clone()));
        state.authorizer = Arc::new(AuthorizerChain::new(vec![
            Box::new(SystemMastersAuthorizer),
            Box::new(RbacAuthorizer::new(store.clone())),
            Box::new(DenyAuthorizer),
        ]));
        state.rbac_policy_store = store;

        state
            .db
            .create_resource(
                "rbac.authorization.k8s.io/v1",
                "ClusterRole",
                None,
                "alice-role",
                cluster_role("alice-role", alice_rules),
            )
            .await
            .unwrap();
        state
            .db
            .create_resource(
                "rbac.authorization.k8s.io/v1",
                "ClusterRoleBinding",
                None,
                "alice-binding",
                cluster_role_binding("alice-binding", "alice-role", "alice"),
            )
            .await
            .unwrap();
        crate::api::build_router(state)
    }

    fn alice() -> AuthenticatedIdentity {
        AuthenticatedIdentity::client_cert("alice".to_string(), vec![])
    }

    async fn post_clusterrole(
        app: &axum::Router,
        identity: AuthenticatedIdentity,
        name: &str,
        rules: serde_json::Value,
    ) -> StatusCode {
        let body = cluster_role(name, rules);
        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/apis/rbac.authorization.k8s.io/v1/clusterroles")
                    .header("content-type", "application/json")
                    .extension(identity)
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }

    // alice may create/list/patch clusterroles (so the request itself is
    // authorized) but holds only `get pods`.
    fn alice_base_rules() -> serde_json::Value {
        json!([
            {"verbs": ["get","list"], "apiGroups": [""], "resources": ["pods"],
             "resourceNames": [], "nonResourceURLs": []},
            {"verbs": ["create","update","patch","get","list"],
             "apiGroups": ["rbac.authorization.k8s.io"],
             "resources": ["clusterroles","clusterrolebindings"],
             "resourceNames": [], "nonResourceURLs": []}
        ])
    }

    /// Server-side-apply (PATCH application/apply-patch+yaml) of a ClusterRole.
    async fn apply_clusterrole(
        app: &axum::Router,
        identity: AuthenticatedIdentity,
        name: &str,
        rules: serde_json::Value,
    ) -> StatusCode {
        let body = cluster_role(name, rules);
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!(
                        "/apis/rbac.authorization.k8s.io/v1/clusterroles/{name}"
                    ))
                    .header("content-type", "application/apply-patch+yaml")
                    .extension(identity)
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }

    /// merge-patch of an existing ClusterRole.
    async fn merge_patch_clusterrole(
        app: &axum::Router,
        identity: AuthenticatedIdentity,
        name: &str,
        patch: serde_json::Value,
    ) -> StatusCode {
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!(
                        "/apis/rbac.authorization.k8s.io/v1/clusterroles/{name}"
                    ))
                    .header("content-type", "application/merge-patch+json")
                    .extension(identity)
                    .body(Body::from(serde_json::to_vec(&patch).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }

    #[tokio::test]
    async fn escalation_blocked_creating_clusterrole_beyond_holder() {
        let app = alice_router(alice_base_rules()).await;
        // alice tries to grant secrets/* which she does not hold.
        let status = post_clusterrole(
            &app,
            alice(),
            "evil",
            json!([{"verbs": ["*"], "apiGroups": [""], "resources": ["secrets"],
                    "resourceNames": [], "nonResourceURLs": []}]),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "escalation must be denied");
    }

    #[tokio::test]
    async fn escalation_allowed_when_rules_are_covered() {
        let app = alice_router(alice_base_rules()).await;
        // alice grants only `get pods`, which she holds.
        let status = post_clusterrole(
            &app,
            alice(),
            "fine",
            json!([{"verbs": ["get"], "apiGroups": [""], "resources": ["pods"],
                    "resourceNames": [], "nonResourceURLs": []}]),
        )
        .await;
        assert!(
            status.is_success(),
            "covered rules must be allowed, got {status}"
        );
    }

    #[tokio::test]
    async fn escalation_allowed_with_escalate_verb() {
        // Grant alice the escalate verb on clusterroles in addition to base.
        let mut rules = alice_base_rules();
        rules.as_array_mut().unwrap().push(json!({
            "verbs": ["escalate"], "apiGroups": ["rbac.authorization.k8s.io"],
            "resources": ["clusterroles"], "resourceNames": [], "nonResourceURLs": []
        }));
        let app = alice_router(rules).await;
        let status = post_clusterrole(
            &app,
            alice(),
            "escalated",
            json!([{"verbs": ["*"], "apiGroups": [""], "resources": ["secrets"],
                    "resourceNames": [], "nonResourceURLs": []}]),
        )
        .await;
        assert!(
            status.is_success(),
            "escalate verb must permit escalation, got {status}"
        );
    }

    #[tokio::test]
    async fn bind_to_privileged_role_blocked() {
        let app = alice_router(alice_base_rules()).await;
        // Seed a privileged ClusterRole alice does not hold.
        // (Reuse the router's state db via a fresh create through the API would
        // require admin; instead bind to a role created out-of-band.)
        // Create the privileged role using a system:masters request first.
        let admin = AuthenticatedIdentity::client_cert(
            "cluster-admin".to_string(),
            vec!["system:masters".to_string()],
        );
        let priv_status = post_clusterrole(
            &app,
            admin,
            "privileged",
            json!([{"verbs": ["*"], "apiGroups": [""], "resources": ["secrets"],
                    "resourceNames": [], "nonResourceURLs": []}]),
        )
        .await;
        assert!(priv_status.is_success(), "admin create should succeed");

        // alice tries to bind to it.
        let body = cluster_role_binding("evil-binding", "privileged", "alice");
        let status = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/apis/rbac.authorization.k8s.io/v1/clusterrolebindings")
                    .header("content-type", "application/json")
                    .extension(alice())
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap()
            .status();
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "binding to a role beyond holder must be denied"
        );
    }

    #[tokio::test]
    async fn escalation_blocked_apply_create_clusterrole_beyond_holder() {
        // Server-side-apply create (PATCH apply-patch) of a brand-new ClusterRole
        // must enforce escalation just like POST create.
        let app = alice_router(alice_base_rules()).await;
        let status = apply_clusterrole(
            &app,
            alice(),
            "evil-apply",
            json!([{"verbs": ["*"], "apiGroups": [""], "resources": ["secrets"],
                    "resourceNames": [], "nonResourceURLs": []}]),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "apply-create escalation must be denied"
        );
    }

    #[tokio::test]
    async fn escalation_blocked_merge_patch_clusterrole_beyond_holder() {
        // Patching an existing ClusterRole to add rules beyond the holder must be
        // denied (the patch path previously skipped the escalation check).
        let app = alice_router(alice_base_rules()).await;
        let admin = AuthenticatedIdentity::client_cert(
            "cluster-admin".to_string(),
            vec!["system:masters".to_string()],
        );
        let seeded = post_clusterrole(
            &app,
            admin,
            "target",
            json!([{"verbs": ["get"], "apiGroups": [""], "resources": ["pods"],
                    "resourceNames": [], "nonResourceURLs": []}]),
        )
        .await;
        assert!(seeded.is_success(), "admin seed should succeed");

        let status = merge_patch_clusterrole(
            &app,
            alice(),
            "target",
            json!({"rules": [{"verbs": ["*"], "apiGroups": [""], "resources": ["secrets"],
                    "resourceNames": [], "nonResourceURLs": []}]}),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "patch escalation must be denied"
        );
    }

    #[tokio::test]
    async fn bind_to_privileged_role_blocked_via_apply_create() {
        // The headline exploit: a delegated user apply-creates a binding to
        // cluster-admin-equivalent. Must be denied.
        let app = alice_router(alice_base_rules()).await;
        let admin = AuthenticatedIdentity::client_cert(
            "cluster-admin".to_string(),
            vec!["system:masters".to_string()],
        );
        let priv_status = post_clusterrole(
            &app,
            admin,
            "privileged",
            json!([{"verbs": ["*"], "apiGroups": [""], "resources": ["secrets"],
                    "resourceNames": [], "nonResourceURLs": []}]),
        )
        .await;
        assert!(priv_status.is_success(), "admin create should succeed");

        let body = cluster_role_binding("evil-binding-apply", "privileged", "alice");
        let status = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(
                        "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/evil-binding-apply",
                    )
                    .header("content-type", "application/apply-patch+yaml")
                    .extension(alice())
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap()
            .status();
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "apply-create binding to a privileged role must be denied"
        );
    }

    #[tokio::test]
    async fn escalation_allowed_apply_create_when_covered() {
        // Covered rules via apply-create must still succeed.
        let app = alice_router(alice_base_rules()).await;
        let status = apply_clusterrole(
            &app,
            alice(),
            "fine-apply",
            json!([{"verbs": ["get"], "apiGroups": [""], "resources": ["pods"],
                    "resourceNames": [], "nonResourceURLs": []}]),
        )
        .await;
        assert!(
            status.is_success(),
            "covered apply-create must be allowed, got {status}"
        );
    }

    #[tokio::test]
    async fn system_masters_bypasses_escalation_check() {
        let app = alice_router(alice_base_rules()).await;
        let admin = AuthenticatedIdentity::client_cert(
            "cluster-admin".to_string(),
            vec!["system:masters".to_string()],
        );
        let status = post_clusterrole(
            &app,
            admin,
            "admin-made",
            json!([{"verbs": ["*"], "apiGroups": ["*"], "resources": ["*"],
                    "resourceNames": [], "nonResourceURLs": []}]),
        )
        .await;
        assert!(
            status.is_success(),
            "system:masters must bypass, got {status}"
        );
    }
}
