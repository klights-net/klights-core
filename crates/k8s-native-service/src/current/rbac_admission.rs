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

use crate::current::AppError;
use klights_auth::AuthenticatedIdentity;
use klights_auth::rbac_rule_evaluator::{PolicyRule, rules_cover_all};
use klights_auth::request_attributes::AuthorizationRequest;
use serde_json::Value;

const RBAC_GROUP: &str = "rbac.authorization.k8s.io";

pub(crate) struct RbacWriteAuthorizationInputs<'a> {
    pub(crate) authorizer: &'a dyn klights_auth::authorizer::Authorizer,
    pub(crate) policy_store: &'a dyn klights_auth::rbac_policy_store::RbacPolicyStore,
    pub(crate) resource_query: &'a dyn klights_leader_api::LeaderResourceQuery,
}

pub(crate) async fn enforce_rbac_write_authorization_with_inputs(
    inputs: &RbacWriteAuthorizationInputs<'_>,
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
            enforce_role_escalation(inputs, identity, kind, namespace, object).await
        }
        "RoleBinding" | "ClusterRoleBinding" => {
            enforce_binding_escalation(inputs, identity, kind, namespace, object).await
        }
        _ => Ok(()),
    }
}

async fn enforce_role_escalation(
    inputs: &RbacWriteAuthorizationInputs<'_>,
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
    let holder = holder_rules(inputs, identity, holder_ns).await;

    if rules_cover_all(&holder, &requested) {
        return Ok(());
    }

    // The `escalate` verb on the role resource overrides the check.
    let resource = if kind == "ClusterRole" {
        "clusterroles"
    } else {
        "roles"
    };
    if has_verb(inputs, identity, "escalate", resource, namespace, None).await {
        return Ok(());
    }

    Err(AppError::Forbidden(format!(
        "user \"{}\" cannot create/update {kind} with rules that exceed the \
         user's own permissions (requires the \"escalate\" verb on {resource}.{RBAC_GROUP})",
        identity.username
    )))
}

async fn enforce_binding_escalation(
    inputs: &RbacWriteAuthorizationInputs<'_>,
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
    let role_rules = referenced_role_rules(inputs, ref_kind, ref_name, namespace).await;
    if role_rules.is_empty() {
        // A role that grants nothing (or does not yet exist) cannot escalate.
        return Ok(());
    }

    let holder = holder_rules(inputs, identity, namespace).await;
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
        inputs,
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
    inputs: &RbacWriteAuthorizationInputs<'_>,
    identity: &AuthenticatedIdentity,
    namespace: Option<&str>,
) -> Vec<PolicyRule> {
    let (resource_rules, non_resource_rules, _incomplete) = inputs
        .policy_store
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
    inputs: &RbacWriteAuthorizationInputs<'_>,
    ref_kind: &str,
    ref_name: &str,
    binding_namespace: Option<&str>,
) -> Vec<PolicyRule> {
    let api_version = format!("{RBAC_GROUP}/v1");
    let resource = match ref_kind {
        "ClusterRole" => crate::current::resource_query_ports::get_resource(
            inputs.resource_query,
            &api_version,
            "ClusterRole",
            None,
            ref_name,
        )
        .await
        .ok()
        .flatten(),
        "Role" => {
            let Some(ns) = binding_namespace else {
                return Vec::new();
            };
            crate::current::resource_query_ports::get_resource(
                inputs.resource_query,
                &api_version,
                "Role",
                Some(ns),
                ref_name,
            )
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
    inputs: &RbacWriteAuthorizationInputs<'_>,
    identity: &AuthenticatedIdentity,
    verb: &str,
    resource: &str,
    namespace: Option<&str>,
    name: Option<&str>,
) -> bool {
    let request =
        AuthorizationRequest::resource(verb, RBAC_GROUP, "v1", resource, None, namespace, name);
    inputs
        .authorizer
        .authorize(identity, &request)
        .await
        .allowed
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
#[path = "rbac_admission_tests.rs"]
mod tests;
