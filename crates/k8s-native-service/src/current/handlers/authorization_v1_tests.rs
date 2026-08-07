use std::collections::VecDeque;
use std::sync::Mutex;

use async_trait::async_trait;
use axum::http::HeaderMap;
use klights_auth::AuthenticatedIdentity;
use klights_auth::authorizer::{AuthorizationDecision, Authorizer};
use klights_auth::rbac_policy_store::{InMemoryRbacPolicyStore, ResolvedBinding};
use klights_auth::rbac_rule_evaluator::{PolicyRule, Subject, SubjectKind};
use klights_auth::request_attributes::AuthorizationRequest;
use serde_json::{Value, json};

use super::*;

struct SequenceAuthorizer {
    decisions: Mutex<VecDeque<AuthorizationDecision>>,
    identities: Mutex<Vec<AuthenticatedIdentity>>,
    requests: Mutex<Vec<AuthorizationRequest>>,
}

impl SequenceAuthorizer {
    fn new(decisions: Vec<AuthorizationDecision>) -> Self {
        Self {
            decisions: Mutex::new(decisions.into()),
            identities: Mutex::new(Vec::new()),
            requests: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl Authorizer for SequenceAuthorizer {
    async fn authorize(
        &self,
        identity: &AuthenticatedIdentity,
        request: &AuthorizationRequest,
    ) -> AuthorizationDecision {
        self.identities.lock().unwrap().push(identity.clone());
        self.requests.lock().unwrap().push(request.clone());
        self.decisions
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| AuthorizationDecision::deny("mock exhausted"))
    }
}

fn caller() -> AuthenticatedIdentity {
    AuthenticatedIdentity::client_cert(
        "caller-admin".to_string(),
        vec!["system:masters".to_string()],
    )
}

fn sar_spec() -> Value {
    json!({
        "user": "alice",
        "groups": ["devs", "auditors"],
        "uid": "uid-alice",
        "extra": {"scopes": ["read", "write"], "trace-id": ["abc-123"]},
        "resourceAttributes": {"verb": "get", "resource": "pods", "namespace": "default"}
    })
}

#[tokio::test]
async fn subject_access_review_evaluates_exact_spec_subject() {
    let authorizer = SequenceAuthorizer::new(vec![
        AuthorizationDecision::allow("can review subject"),
        AuthorizationDecision::allow("matched requested subject"),
    ]);
    let decision = evaluate_subject_access_review(&authorizer, &caller(), &sar_spec())
        .await
        .unwrap();
    assert!(decision.allowed);
    assert_eq!(decision.reason, "matched requested subject");
    let identities = authorizer.identities.lock().unwrap();
    assert_eq!(identities[1].username, "alice");
    assert_eq!(identities[1].groups, vec!["devs", "auditors"]);
    assert_eq!(identities[1].uid.as_deref(), Some("uid-alice"));
    assert_eq!(identities[1].extra.len(), 3);
    let requests = authorizer.requests.lock().unwrap();
    assert_eq!(requests[0].verb, "create");
    assert_eq!(requests[1].verb, "get");
    assert_eq!(requests[1].namespace.as_deref(), Some("default"));
}

#[tokio::test]
async fn local_subject_access_review_evaluates_exact_spec_subject() {
    let authorizer = SequenceAuthorizer::new(vec![AuthorizationDecision::allow(
        "matched requested subject",
    )]);
    let decision = evaluate_requested_subject(&authorizer, &caller(), &sar_spec()).await;
    assert!(decision.allowed);
    assert_eq!(authorizer.identities.lock().unwrap()[0].username, "alice");
    assert_eq!(authorizer.requests.lock().unwrap()[0].verb, "get");
}

#[tokio::test]
async fn subject_access_review_requires_review_permission() {
    let authorizer = SequenceAuthorizer::new(vec![
        AuthorizationDecision::deny("cannot review"),
        AuthorizationDecision::allow("must not be consumed"),
    ]);
    let error = evaluate_subject_access_review(&authorizer, &caller(), &sar_spec())
        .await
        .unwrap_err();
    assert!(matches!(error, AppError::Forbidden(reason) if reason == "cannot review"));
    assert_eq!(authorizer.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn subject_access_review_allowed_denied_and_evaluation_error() {
    for (decision, allowed, denied, evaluation_error) in [
        (AuthorizationDecision::allow("allowed"), true, false, None),
        (AuthorizationDecision::deny("denied"), false, true, None),
        (
            AuthorizationDecision::evaluation_error("policy backend failed"),
            false,
            false,
            Some("policy backend failed"),
        ),
    ] {
        let authorizer =
            SequenceAuthorizer::new(vec![AuthorizationDecision::allow("can review"), decision]);
        let result = evaluate_subject_access_review(&authorizer, &caller(), &sar_spec())
            .await
            .unwrap();
        assert_eq!(result.allowed, allowed);
        assert_eq!(result.denied, denied);
        assert_eq!(result.evaluation_error.as_deref(), evaluation_error);
    }
}

#[tokio::test]
async fn self_subject_rules_review_returns_policy_store_rules_without_probe_resources() {
    let store = InMemoryRbacPolicyStore::new(vec![ResolvedBinding {
        namespace: Some("default".to_string()),
        subjects: vec![Subject {
            kind: SubjectKind::Group,
            name: "devs".to_string(),
            namespace: None,
        }],
        rules: vec![PolicyRule {
            verbs: vec!["get".to_string()],
            api_groups: vec!["".to_string()],
            resources: vec!["pods".to_string()],
            resource_names: Vec::new(),
            non_resource_urls: Vec::new(),
        }],
    }]);
    let identity =
        AuthenticatedIdentity::client_cert("alice".to_string(), vec!["devs".to_string()]);
    let status = self_subject_rules_status(&store, &identity, Some("default")).await;
    assert_eq!(status["incomplete"], false);
    assert_eq!(status["resourceRules"].as_array().unwrap().len(), 1);
    assert_eq!(status["resourceRules"][0]["resources"][0], "pods");
    assert!(status["nonResourceRules"].as_array().unwrap().is_empty());
}

#[test]
fn test_self_subject_access_review_returns_406_for_table_format() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "accept",
        "application/json;as=Table;v=v1;g=meta.k8s.io"
            .parse()
            .unwrap(),
    );
    assert!(matches!(
        reject_table_format(&headers, "SelfSubjectAccessReview"),
        Err(AppError::NotAcceptable(message))
            if message.contains("SelfSubjectAccessReview")
    ));
}

#[tokio::test]
async fn test_self_subject_access_review_without_table_returns_allowed() {
    let headers = HeaderMap::new();
    reject_table_format(&headers, "SelfSubjectAccessReview").unwrap();
    let authorizer = SequenceAuthorizer::new(vec![AuthorizationDecision::allow("allowed")]);
    let spec = json!({"nonResourceAttributes": {"path": "/", "verb": "get"}});
    let decision = evaluate_requested_subject(&authorizer, &caller(), &spec).await;
    assert!(decision.allowed);
}

#[test]
fn test_self_subject_access_review_spec_round_trips_verbatim() {
    let spec = json!({
        "user": "alice",
        "groups": ["g1", "g2"],
        "resourceAttributes": {
            "verb": "list",
            "resource": "pods",
            "namespace": "ns-x",
            "subresource": "log"
        },
        "extra": {"trace-id": ["abc-123"]}
    });
    let mut decoded = json!({
        "apiVersion": "authorization.k8s.io/v1",
        "kind": "SelfSubjectAccessReview",
        "spec": spec.clone(),
    });
    assert_eq!(take_spec(&mut decoded), spec);
}

#[test]
fn test_local_subject_access_review_injects_namespace_into_spec() {
    let mut spec = json!({
        "user": "bob",
        "resourceAttributes": {"verb": "get", "resource": "configmaps"}
    });
    stamp_local_review_namespace(&mut spec, "ns-y");
    assert_eq!(spec["namespace"], "ns-y");
    assert_eq!(spec["resourceAttributes"]["namespace"], "ns-y");
    assert_eq!(spec["user"], "bob");
}

#[tokio::test]
async fn test_subject_access_review_accepts_json_body() {
    let body = br#"{
        "apiVersion":"authorization.k8s.io/v1",
        "kind":"SubjectAccessReview",
        "spec":{"user":"alice","resourceAttributes":{"verb":"get","resource":"pods"}}
    }"#;
    let mut decoded = crate::current::extractors::decode_json_or_proto(body).unwrap();
    let spec = take_spec(&mut decoded);
    let authorizer = SequenceAuthorizer::new(vec![
        AuthorizationDecision::allow("can review"),
        AuthorizationDecision::allow("allowed"),
    ]);
    let decision = evaluate_subject_access_review(&authorizer, &caller(), &spec)
        .await
        .unwrap();
    assert!(decision.allowed);
}

#[test]
fn test_subject_access_review_rejects_invalid_body() {
    assert!(crate::current::extractors::decode_json_or_proto(b"not json, not proto").is_err());
}
