use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use klights_auth::AuthenticatedIdentity;
use klights_auth::authorizer::{AuthorizationDecision, Authorizer};
use klights_auth::rbac_policy_store::{InMemoryRbacPolicyStore, ResolvedBinding};
use klights_auth::rbac_rule_evaluator::{PolicyRule, Subject, SubjectKind};
use klights_auth::request_attributes::AuthorizationRequest;
use klights_leader_api::{
    LeaderResourceQuery, ResourceGetRequest, ResourceListRequest, ResourceListResult,
    ResourceQueryFuture,
};
use serde_json::{Value, json};

use super::*;

#[derive(Default)]
struct VerbAuthorizer {
    escalate: bool,
    bind: bool,
}

#[async_trait]
impl Authorizer for VerbAuthorizer {
    async fn authorize(
        &self,
        _identity: &AuthenticatedIdentity,
        request: &AuthorizationRequest,
    ) -> AuthorizationDecision {
        if (request.verb == "escalate" && self.escalate) || (request.verb == "bind" && self.bind) {
            AuthorizationDecision::allow("focused RBAC override")
        } else {
            AuthorizationDecision::deny("focused RBAC deny")
        }
    }
}

#[derive(Default)]
struct RoleQuery {
    roles: BTreeMap<String, Arc<Value>>,
}

impl RoleQuery {
    fn with_cluster_role(name: &str, rules: Value) -> Self {
        Self {
            roles: BTreeMap::from([(
                name.to_string(),
                Arc::new(json!({
                    "apiVersion": "rbac.authorization.k8s.io/v1",
                    "kind": "ClusterRole",
                    "metadata": {"name": name, "uid": format!("{name}-uid")},
                    "rules": rules,
                })),
            )]),
        }
    }
}

impl LeaderResourceQuery for RoleQuery {
    fn get_resource(
        &self,
        request: ResourceGetRequest,
    ) -> ResourceQueryFuture<'_, Option<klights_cluster_core::Resource>> {
        let value = self.roles.get(&request.key().name).cloned();
        Box::pin(async move {
            value
                .map(klights_cluster_core::Resource::try_from_data)
                .transpose()
                .map_err(|error| {
                    klights_leader_api::ResourceQueryError::corrupt_response(error.to_string())
                })
        })
    }

    fn list_resources(
        &self,
        _request: ResourceListRequest,
    ) -> ResourceQueryFuture<'_, ResourceListResult> {
        Box::pin(async { ResourceListResult::try_new(Vec::new(), 1, None, None, None) })
    }
}

fn rule(resource: &str) -> PolicyRule {
    PolicyRule {
        verbs: vec!["get".to_string()],
        api_groups: vec!["".to_string()],
        resources: vec![resource.to_string()],
        resource_names: Vec::new(),
        non_resource_urls: Vec::new(),
    }
}

fn rules_json(resource: &str) -> Value {
    json!([{"verbs":["get"],"apiGroups":[""],"resources":[resource]}])
}

fn alice() -> AuthenticatedIdentity {
    AuthenticatedIdentity::client_cert("alice".to_string(), Vec::new())
}

fn policy_store(held: Vec<PolicyRule>) -> InMemoryRbacPolicyStore {
    InMemoryRbacPolicyStore::new(vec![ResolvedBinding {
        namespace: None,
        subjects: vec![Subject {
            kind: SubjectKind::User,
            name: "alice".to_string(),
            namespace: None,
        }],
        rules: held,
    }])
}

async fn enforce_role(
    held: Vec<PolicyRule>,
    authorizer: &VerbAuthorizer,
    requested_resource: &str,
    identity: &AuthenticatedIdentity,
) -> Result<(), AppError> {
    let store = policy_store(held);
    let query = RoleQuery::default();
    enforce_rbac_write_authorization_with_inputs(
        &RbacWriteAuthorizationInputs {
            authorizer,
            policy_store: &store,
            resource_query: &query,
        },
        identity,
        "rbac.authorization.k8s.io/v1",
        "ClusterRole",
        None,
        &json!({"rules": rules_json(requested_resource)}),
    )
    .await
}

async fn enforce_binding(
    held: Vec<PolicyRule>,
    authorizer: &VerbAuthorizer,
) -> Result<(), AppError> {
    let store = policy_store(held);
    let query = RoleQuery::with_cluster_role("privileged", rules_json("secrets"));
    enforce_rbac_write_authorization_with_inputs(
        &RbacWriteAuthorizationInputs {
            authorizer,
            policy_store: &store,
            resource_query: &query,
        },
        &alice(),
        "rbac.authorization.k8s.io/v1",
        "ClusterRoleBinding",
        None,
        &json!({"roleRef":{"kind":"ClusterRole","name":"privileged"}}),
    )
    .await
}

async fn assert_role_blocked() {
    assert!(matches!(
        enforce_role(
            vec![rule("pods")],
            &VerbAuthorizer::default(),
            "secrets",
            &alice()
        )
        .await,
        Err(AppError::Forbidden(_))
    ));
}

async fn assert_binding_blocked() {
    assert!(matches!(
        enforce_binding(vec![rule("pods")], &VerbAuthorizer::default()).await,
        Err(AppError::Forbidden(_))
    ));
}

#[tokio::test]
async fn escalation_blocked_creating_clusterrole_beyond_holder() {
    assert_role_blocked().await;
}

#[tokio::test]
async fn escalation_allowed_when_rules_are_covered() {
    enforce_role(
        vec![rule("pods")],
        &VerbAuthorizer::default(),
        "pods",
        &alice(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn escalation_allowed_with_escalate_verb() {
    enforce_role(
        vec![rule("pods")],
        &VerbAuthorizer {
            escalate: true,
            bind: false,
        },
        "secrets",
        &alice(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn bind_to_privileged_role_blocked() {
    assert_binding_blocked().await;
}

#[tokio::test]
async fn escalation_blocked_apply_create_clusterrole_beyond_holder() {
    assert_role_blocked().await;
}

#[tokio::test]
async fn escalation_blocked_merge_patch_clusterrole_beyond_holder() {
    assert_role_blocked().await;
}

#[tokio::test]
async fn bind_to_privileged_role_blocked_via_apply_create() {
    assert_binding_blocked().await;
}

#[tokio::test]
async fn escalation_allowed_apply_create_when_covered() {
    enforce_role(
        vec![rule("pods")],
        &VerbAuthorizer::default(),
        "pods",
        &alice(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn system_masters_bypasses_escalation_check() {
    let admin =
        AuthenticatedIdentity::client_cert("admin".to_string(), vec!["system:masters".to_string()]);
    enforce_role(Vec::new(), &VerbAuthorizer::default(), "secrets", &admin)
        .await
        .unwrap();
}
