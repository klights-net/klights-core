use crate::api::*;
use crate::auth::authorizer::AuthorizationDecision;
use crate::auth::identity::AuthenticatedIdentity;
use crate::auth::request_attributes::AuthorizationRequest;

/// Take `spec` out of a decoded request body without deep-cloning. The
/// json! response macro consumes its arguments, so moving spec straight
/// from the request avoids the per-call `serde_json::Value::clone()`
/// that used to fire on every authz hot-path call. Returns an empty
/// object when the field is absent or not an object.
fn take_spec(decoded: &mut Value) -> Value {
    decoded
        .as_object_mut()
        .and_then(|obj| obj.remove("spec"))
        .unwrap_or_else(|| serde_json::json!({}))
}

/// Build an AuthorizationRequest from a SelfSubjectAccessReview spec.
fn build_request_from_sar_spec(spec: &Value) -> Option<AuthorizationRequest> {
    let resource_attrs = spec.get("resourceAttributes");
    let non_resource_attrs = spec.get("nonResourceAttributes");
    let fallback_namespace = spec.get("namespace").and_then(|v| v.as_str());

    if let Some(attrs) = resource_attrs {
        let verb = attrs.get("verb").and_then(|v| v.as_str()).unwrap_or("");
        let group = attrs.get("group").and_then(|v| v.as_str()).unwrap_or("");
        let resource = attrs.get("resource").and_then(|v| v.as_str()).unwrap_or("");
        let subresource = attrs.get("subresource").and_then(|v| v.as_str());
        let namespace = attrs
            .get("namespace")
            .and_then(|v| v.as_str())
            .or(fallback_namespace);
        let name = attrs.get("name").and_then(|v| v.as_str());
        Some(AuthorizationRequest::resource(
            verb,
            group,
            "",
            resource,
            subresource,
            namespace,
            name,
        ))
    } else if let Some(attrs) = non_resource_attrs {
        let verb = attrs.get("verb").and_then(|v| v.as_str()).unwrap_or("");
        let path = attrs.get("path").and_then(|v| v.as_str()).unwrap_or("");
        Some(AuthorizationRequest::non_resource(verb, path))
    } else {
        None
    }
}

fn decision_status(decision: &AuthorizationDecision) -> Value {
    let mut status = serde_json::Map::new();
    status.insert("allowed".to_string(), serde_json::json!(decision.allowed));
    status.insert("denied".to_string(), serde_json::json!(decision.denied));
    status.insert(
        "reason".to_string(),
        serde_json::json!(decision.reason.clone()),
    );
    if let Some(error) = decision.evaluation_error.as_deref() {
        status.insert("evaluationError".to_string(), serde_json::json!(error));
    }
    Value::Object(status)
}

fn build_subject_identity_from_sar_spec(spec: &Value) -> AuthenticatedIdentity {
    let username = spec
        .get("user")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let groups = spec
        .get("groups")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let uid = spec
        .get("uid")
        .and_then(|v| v.as_str())
        .map(ToString::to_string);
    let extra = spec
        .get("extra")
        .and_then(|v| v.as_object())
        .map(|obj| {
            let mut pairs = Vec::new();
            for (key, value) in obj {
                if let Some(values) = value.as_array() {
                    for item in values {
                        if let Some(s) = item.as_str() {
                            pairs.push((key.clone(), s.to_string()));
                        }
                    }
                }
            }
            pairs
        })
        .unwrap_or_default();

    AuthenticatedIdentity {
        username,
        groups,
        uid,
        extra,
    }
}

pub(crate) async fn create_self_subject_access_review(
    State(state): State<Arc<AppState>>,
    axum::Extension(identity): axum::Extension<AuthenticatedIdentity>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    if wants_table_format(&headers)? {
        return Err(AppError::NotAcceptable(
            "Table format is not supported for SelfSubjectAccessReview".to_string(),
        ));
    }

    let mut decoded: Value = decode_json_or_proto(&body)?;
    let spec = take_spec(&mut decoded);

    let decision = if let Some(request) = build_request_from_sar_spec(&spec) {
        state.authorizer.authorize(&identity, &request).await
    } else {
        // No attributes specified: check if identity is authenticated
        AuthorizationDecision {
            allowed: !identity.username.starts_with("system:anonymous"),
            denied: false,
            reason: String::new(),
            evaluation_error: None,
        }
    };

    Ok(Json(serde_json::json!({
        "apiVersion": "authorization.k8s.io/v1",
        "kind": "SelfSubjectAccessReview",
        "metadata": {
            "creationTimestamp": crate::utils::k8s_timestamp()
        },
        "spec": spec,
        "status": decision_status(&decision),
    })))
}

pub(crate) async fn create_subject_access_review(
    State(state): State<Arc<AppState>>,
    axum::Extension(identity): axum::Extension<AuthenticatedIdentity>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    if wants_table_format(&headers)? {
        return Err(AppError::NotAcceptable(
            "Table format is not supported for SubjectAccessReview".to_string(),
        ));
    }

    let mut decoded: Value = decode_json_or_proto(&body)?;
    let spec = take_spec(&mut decoded);

    let review_request = AuthorizationRequest::resource(
        "create",
        "authorization.k8s.io",
        "v1",
        "subjectaccessreviews",
        None,
        None,
        None,
    );

    let review_decision = state.authorizer.authorize(&identity, &review_request).await;
    if !review_decision.allowed {
        let reason = if let Some(err) = review_decision.evaluation_error.as_ref() {
            format!("cannot review other subjects: {err}")
        } else if review_decision.reason.is_empty() {
            "caller not authorized to review other subjects".to_string()
        } else {
            review_decision.reason.clone()
        };
        return Err(AppError::Forbidden(reason));
    }

    let decision = if let Some(request) = build_request_from_sar_spec(&spec) {
        // SubjectAccessReview evaluates the subject described by spec, not the caller.
        let eval_identity = build_subject_identity_from_sar_spec(&spec);
        state.authorizer.authorize(&eval_identity, &request).await
    } else {
        AuthorizationDecision {
            allowed: !identity.username.starts_with("system:anonymous"),
            denied: false,
            reason: String::new(),
            evaluation_error: None,
        }
    };

    Ok(Json(serde_json::json!({
        "apiVersion": "authorization.k8s.io/v1",
        "kind": "SubjectAccessReview",
        "metadata": {
            "creationTimestamp": crate::utils::k8s_timestamp()
        },
        "spec": spec,
        "status": decision_status(&decision),
    })))
}

pub(crate) async fn create_self_subject_rules_review(
    State(state): State<Arc<AppState>>,
    axum::Extension(identity): axum::Extension<AuthenticatedIdentity>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    if wants_table_format(&headers)? {
        return Err(AppError::NotAcceptable(
            "Table format is not supported for SelfSubjectRulesReview".to_string(),
        ));
    }

    let mut decoded: Value = decode_json_or_proto(&body)?;
    let spec = take_spec(&mut decoded);

    // Phase 2B: enumerate effective rules from the RBAC policy store.
    let namespace = spec.get("namespace").and_then(|v| v.as_str());

    let (effective_resource, effective_non_resource, incomplete) = state
        .rbac_policy_store
        .enumerate_effective_rules(&identity, namespace)
        .await;

    let resource_rules: Vec<Value> = effective_resource
        .iter()
        .map(|r| {
            serde_json::json!({
                "verbs": r.verbs,
                "apiGroups": [&r.api_group],
                "resources": [&r.resource],
                "resourceNames": r.resource_names,
            })
        })
        .collect();

    let non_resource_rules: Vec<Value> = effective_non_resource
        .iter()
        .map(|r| {
            serde_json::json!({
                "verbs": r.verbs,
                "nonResourceURLs": r.non_resource_urls,
            })
        })
        .collect();

    Ok(Json(serde_json::json!({
        "apiVersion": "authorization.k8s.io/v1",
        "kind": "SelfSubjectRulesReview",
        "metadata": {
            "creationTimestamp": crate::utils::k8s_timestamp()
        },
        "spec": spec,
        "status": {
            "resourceRules": resource_rules,
            "nonResourceRules": non_resource_rules,
            "incomplete": incomplete
        }
    })))
}

pub(crate) async fn create_local_subject_access_review(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
    axum::Extension(identity): axum::Extension<AuthenticatedIdentity>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    if wants_table_format(&headers)? {
        return Err(AppError::NotAcceptable(
            "Table format is not supported for LocalSubjectAccessReview".to_string(),
        ));
    }

    let mut decoded: Value = decode_json_or_proto(&body)?;
    let mut spec = take_spec(&mut decoded);
    if let Some(obj) = spec.as_object_mut() {
        obj.entry("namespace".to_string())
            .and_modify(|existing| {
                if existing.is_null() {
                    *existing = serde_json::json!(&namespace);
                }
            })
            .or_insert_with(|| serde_json::json!(&namespace));

        if let Some(resource_attrs) = obj
            .get_mut("resourceAttributes")
            .and_then(Value::as_object_mut)
        {
            resource_attrs
                .entry("namespace".to_string())
                .and_modify(|existing| {
                    if existing.is_null() {
                        *existing = serde_json::json!(&namespace);
                    }
                })
                .or_insert_with(|| serde_json::json!(&namespace));
        } else {
            obj.insert("namespace".to_string(), serde_json::json!(&namespace));
        }
    }

    let decision = if let Some(request) = build_request_from_sar_spec(&spec) {
        // LocalSubjectAccessReview evaluates the subject described by spec, not the caller.
        let eval_identity = build_subject_identity_from_sar_spec(&spec);
        state.authorizer.authorize(&eval_identity, &request).await
    } else {
        AuthorizationDecision {
            allowed: !identity.username.starts_with("system:anonymous"),
            denied: false,
            reason: String::new(),
            evaluation_error: None,
        }
    };

    Ok(Json(serde_json::json!({
        "apiVersion": "authorization.k8s.io/v1",
        "kind": "LocalSubjectAccessReview",
        "metadata": {
            "creationTimestamp": crate::utils::k8s_timestamp(),
            "namespace": namespace
        },
        "spec": spec,
        "status": decision_status(&decision),
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::authorizer::{AuthorizationDecision, Authorizer};
    use crate::auth::rbac_policy_store::{InMemoryRbacPolicyStore, ResolvedBinding};
    use crate::auth::rbac_rule_evaluator::{PolicyRule, Subject, SubjectKind};
    use async_trait::async_trait;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    struct SequenceAuthorizer {
        decisions: Arc<Mutex<VecDeque<AuthorizationDecision>>>,
        seen: Arc<Mutex<Vec<AuthenticatedIdentity>>>,
        seen_requests: Arc<Mutex<Vec<AuthorizationRequest>>>,
    }

    #[async_trait]
    impl Authorizer for SequenceAuthorizer {
        async fn authorize(
            &self,
            identity: &AuthenticatedIdentity,
            request: &AuthorizationRequest,
        ) -> AuthorizationDecision {
            self.seen.lock().unwrap().push(identity.clone());
            self.seen_requests.lock().unwrap().push(request.clone());
            let mut decisions = self.decisions.lock().unwrap();
            decisions
                .pop_front()
                .unwrap_or(AuthorizationDecision::deny("mock exhausted"))
        }
    }

    fn sar_spec() -> Value {
        serde_json::json!({
            "user": "alice",
            "groups": ["devs", "auditors"],
            "uid": "uid-alice",
            "extra": {
                "scopes": ["read", "write"],
                "trace-id": ["abc-123"]
            },
            "resourceAttributes": {
                "verb": "get",
                "resource": "pods",
                "namespace": "default"
            }
        })
    }

    async fn state_with_sequence_authorizer(
        decisions: Vec<AuthorizationDecision>,
    ) -> (
        AppState,
        Arc<Mutex<Vec<AuthenticatedIdentity>>>,
        Arc<Mutex<Vec<AuthorizationRequest>>>,
    ) {
        let mut state = crate::api::test_support::build_test_app_state().await;
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_requests = Arc::new(Mutex::new(Vec::new()));
        state.authorizer = Arc::new(SequenceAuthorizer {
            decisions: Arc::new(Mutex::new(VecDeque::from(decisions))),
            seen: seen.clone(),
            seen_requests: seen_requests.clone(),
        });
        (state, seen, seen_requests)
    }

    #[tokio::test]
    async fn subject_access_review_evaluates_exact_spec_subject() {
        let (state, seen, seen_requests) = state_with_sequence_authorizer(vec![
            AuthorizationDecision::allow("can review subject"),
            AuthorizationDecision::allow("matched requested subject"),
        ])
        .await;
        let body = serde_json::to_vec(&serde_json::json!({
            "apiVersion": "authorization.k8s.io/v1",
            "kind": "SubjectAccessReview",
            "spec": sar_spec()
        }))
        .unwrap();

        let result = create_subject_access_review(
            State(Arc::new(state)),
            axum::Extension(AuthenticatedIdentity::admin("caller-admin")),
            HeaderMap::new(),
            Bytes::from(body),
        )
        .await
        .expect("SAR should be accepted");

        assert_eq!(result.0["status"]["allowed"], true);
        assert_eq!(result.0["status"]["reason"], "matched requested subject");
        {
            let requests = seen_requests.lock().unwrap();
            assert_eq!(requests.len(), 2);
            let eval_request =
                AuthorizationRequest::resource("get", "", "", "pods", None, Some("default"), None);
            let review_request = AuthorizationRequest::resource(
                "create",
                "authorization.k8s.io",
                "v1",
                "subjectaccessreviews",
                None,
                None,
                None,
            );
            assert_eq!(requests[1], eval_request);
            assert_eq!(requests[0], review_request);
        }
        assert_eq!(seen.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn local_subject_access_review_evaluates_exact_spec_subject() {
        let (state, seen, seen_requests) =
            state_with_sequence_authorizer(vec![AuthorizationDecision::allow(
                "matched requested subject",
            )])
            .await;
        let body = serde_json::to_vec(&serde_json::json!({
            "apiVersion": "authorization.k8s.io/v1",
            "kind": "LocalSubjectAccessReview",
            "spec": sar_spec()
        }))
        .unwrap();

        let result = create_local_subject_access_review(
            State(Arc::new(state)),
            Path("default".to_string()),
            axum::Extension(AuthenticatedIdentity::admin("caller-admin")),
            HeaderMap::new(),
            Bytes::from(body),
        )
        .await
        .expect("LocalSAR should be accepted");

        assert_eq!(result.0["status"]["allowed"], true);
        assert_eq!(result.0["status"]["reason"], "matched requested subject");
        assert_eq!(seen.lock().unwrap().len(), 1);
        assert_eq!(seen_requests.lock().unwrap().len(), 1);
        let request = seen_requests.lock().unwrap()[0].clone();
        let expected =
            AuthorizationRequest::resource("get", "", "", "pods", None, Some("default"), None);
        assert_eq!(request, expected);
    }

    #[tokio::test]
    async fn subject_access_review_requires_review_permission() {
        let (state, seen, seen_requests) = state_with_sequence_authorizer(vec![
            AuthorizationDecision::deny("cannot review"),
            AuthorizationDecision::allow("matched requested subject"),
        ])
        .await;
        let body = serde_json::to_vec(&serde_json::json!({
            "apiVersion": "authorization.k8s.io/v1",
            "kind": "SubjectAccessReview",
            "spec": sar_spec()
        }))
        .unwrap();

        let result = create_subject_access_review(
            State(Arc::new(state)),
            axum::Extension(AuthenticatedIdentity::admin("caller-admin")),
            HeaderMap::new(),
            Bytes::from(body),
        )
        .await;

        assert!(result.is_err());
        if let Err(AppError::Forbidden(reason)) = result {
            assert_eq!(reason, "cannot review");
        } else {
            panic!("expected forbidden error");
        }
        assert_eq!(seen.lock().unwrap().len(), 1);
        let seen_requests = seen_requests.lock().unwrap();
        assert_eq!(seen_requests.len(), 1);
        assert_eq!(
            seen_requests[0],
            AuthorizationRequest::resource(
                "create",
                "authorization.k8s.io",
                "v1",
                "subjectaccessreviews",
                None,
                None,
                None,
            )
        );
    }

    #[tokio::test]
    async fn subject_access_review_allowed_denied_and_evaluation_error() {
        let (state, _seen, _seen_requests) = state_with_sequence_authorizer(vec![
            AuthorizationDecision::allow("can review subject"),
            AuthorizationDecision::allow("allowed for subject"),
        ])
        .await;
        let body = serde_json::to_vec(&serde_json::json!({
            "apiVersion": "authorization.k8s.io/v1",
            "kind": "SubjectAccessReview",
            "spec": sar_spec()
        }))
        .unwrap();

        let allowed_result = create_subject_access_review(
            State(Arc::new(state)),
            axum::Extension(AuthenticatedIdentity::admin("caller-admin")),
            HeaderMap::new(),
            Bytes::from(body),
        )
        .await
        .unwrap();
        assert_eq!(allowed_result.0["status"]["allowed"], true);
        assert_eq!(allowed_result.0["status"]["reason"], "allowed for subject");
        assert!(allowed_result.0["status"].get("evaluationError").is_none());

        let (state, _seen, _seen_requests) = state_with_sequence_authorizer(vec![
            AuthorizationDecision::allow("can review subject"),
            AuthorizationDecision::deny("not allowed for subject"),
        ])
        .await;
        let denied_result = create_subject_access_review(
            State(Arc::new(state)),
            axum::Extension(AuthenticatedIdentity::admin("caller-admin")),
            HeaderMap::new(),
            Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "apiVersion": "authorization.k8s.io/v1",
                    "kind": "SubjectAccessReview",
                    "spec": sar_spec()
                }))
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        assert_eq!(denied_result.0["status"]["allowed"], false);
        assert_eq!(denied_result.0["status"]["denied"], true);
        assert_eq!(
            denied_result.0["status"]["reason"],
            "not allowed for subject"
        );

        let (state, _seen, seen_requests) = state_with_sequence_authorizer(vec![
            AuthorizationDecision::allow("can review subject"),
            AuthorizationDecision::evaluation_error("policy backend failed"),
        ])
        .await;
        let error_result = create_subject_access_review(
            State(Arc::new(state)),
            axum::Extension(AuthenticatedIdentity::admin("caller-admin")),
            HeaderMap::new(),
            Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "apiVersion": "authorization.k8s.io/v1",
                    "kind": "SubjectAccessReview",
                    "spec": sar_spec()
                }))
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            error_result.0["status"]["evaluationError"],
            "policy backend failed"
        );
        let seen_requests = seen_requests.lock().unwrap();
        assert_eq!(seen_requests.len(), 2);
    }

    #[tokio::test]
    async fn self_subject_rules_review_returns_policy_store_rules_without_probe_resources() {
        let mut state = crate::api::test_support::build_test_app_state().await;
        state.rbac_policy_store = Arc::new(InMemoryRbacPolicyStore::new(vec![
            ResolvedBinding {
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
                    resource_names: vec![],
                    non_resource_urls: vec![],
                }],
            },
            ResolvedBinding {
                namespace: None,
                subjects: vec![Subject {
                    kind: SubjectKind::Group,
                    name: "devs".to_string(),
                    namespace: None,
                }],
                rules: vec![
                    PolicyRule {
                        verbs: vec!["watch".to_string()],
                        api_groups: vec!["apps".to_string()],
                        resources: vec!["deployments".to_string()],
                        resource_names: vec![],
                        non_resource_urls: vec![],
                    },
                    PolicyRule {
                        verbs: vec!["get".to_string()],
                        api_groups: vec![],
                        resources: vec![],
                        resource_names: vec![],
                        non_resource_urls: vec!["/healthz".to_string()],
                    },
                ],
            },
        ]));
        let body = serde_json::to_vec(&serde_json::json!({
            "apiVersion": "authorization.k8s.io/v1",
            "kind": "SelfSubjectRulesReview",
            "spec": {
                "namespace": "default"
            }
        }))
        .unwrap();

        let result = create_self_subject_rules_review(
            State(Arc::new(state)),
            axum::Extension(AuthenticatedIdentity::client_cert(
                "alice".to_string(),
                vec!["devs".to_string()],
            )),
            HeaderMap::new(),
            Bytes::from(body),
        )
        .await
        .expect("SSRR should be accepted");

        assert_eq!(result.0["status"]["incomplete"], false);
        let resource_rules = result.0["status"]["resourceRules"].as_array().unwrap();
        assert_eq!(resource_rules.len(), 2);
        assert!(resource_rules.iter().any(|r| {
            r["verbs"] == serde_json::json!(["get"])
                && r["apiGroups"] == serde_json::json!([""])
                && r["resources"] == serde_json::json!(["pods"])
                && r["resourceNames"] == serde_json::json!([])
        }));
        assert!(resource_rules.iter().any(|r| {
            r["verbs"] == serde_json::json!(["watch"])
                && r["apiGroups"] == serde_json::json!(["apps"])
                && r["resources"] == serde_json::json!(["deployments"])
                && r["resourceNames"] == serde_json::json!([])
        }));
        assert!(!resource_rules.iter().any(|r| {
            r["resources"]
                .as_array()
                .unwrap()
                .iter()
                .any(|resource| resource == "secrets" || resource == "configmaps")
        }));

        let non_resource_rules = result.0["status"]["nonResourceRules"].as_array().unwrap();
        assert_eq!(non_resource_rules.len(), 1);
        assert_eq!(non_resource_rules[0]["verbs"], serde_json::json!(["get"]));
        assert_eq!(
            non_resource_rules[0]["nonResourceURLs"],
            serde_json::json!(["/healthz"])
        );
    }
}
