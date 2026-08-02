use crate::current::*;
use klights_auth::AuthenticatedIdentity;
use klights_auth::authorizer::AuthorizationDecision;
use klights_auth::request_attributes::AuthorizationRequest;

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

fn reject_table_format(headers: &HeaderMap, kind: &str) -> Result<(), AppError> {
    if wants_table_format(headers)? {
        return Err(AppError::NotAcceptable(format!(
            "Table format is not supported for {kind}"
        )));
    }
    Ok(())
}

fn stamp_local_review_namespace(spec: &mut Value, namespace: &str) {
    let Some(obj) = spec.as_object_mut() else {
        return;
    };
    obj.entry("namespace".to_string())
        .and_modify(|existing| {
            if existing.is_null() {
                *existing = serde_json::json!(namespace);
            }
        })
        .or_insert_with(|| serde_json::json!(namespace));

    if let Some(resource_attrs) = obj
        .get_mut("resourceAttributes")
        .and_then(Value::as_object_mut)
    {
        resource_attrs
            .entry("namespace".to_string())
            .and_modify(|existing| {
                if existing.is_null() {
                    *existing = serde_json::json!(namespace);
                }
            })
            .or_insert_with(|| serde_json::json!(namespace));
    }
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

async fn evaluate_requested_subject(
    authorizer: &dyn klights_auth::authorizer::Authorizer,
    fallback_identity: &AuthenticatedIdentity,
    spec: &Value,
) -> AuthorizationDecision {
    if let Some(request) = build_request_from_sar_spec(spec) {
        let subject = build_subject_identity_from_sar_spec(spec);
        authorizer.authorize(&subject, &request).await
    } else {
        AuthorizationDecision {
            allowed: !fallback_identity.username.starts_with("system:anonymous"),
            denied: false,
            reason: String::new(),
            evaluation_error: None,
        }
    }
}

async fn evaluate_subject_access_review(
    authorizer: &dyn klights_auth::authorizer::Authorizer,
    caller: &AuthenticatedIdentity,
    spec: &Value,
) -> Result<AuthorizationDecision, AppError> {
    let review_request = AuthorizationRequest::resource(
        "create",
        "authorization.k8s.io",
        "v1",
        "subjectaccessreviews",
        None,
        None,
        None,
    );
    let review_decision = authorizer.authorize(caller, &review_request).await;
    if !review_decision.allowed {
        let reason = if let Some(error) = review_decision.evaluation_error.as_ref() {
            format!("cannot review other subjects: {error}")
        } else if review_decision.reason.is_empty() {
            "caller not authorized to review other subjects".to_string()
        } else {
            review_decision.reason
        };
        return Err(AppError::Forbidden(reason));
    }
    Ok(evaluate_requested_subject(authorizer, caller, spec).await)
}

async fn self_subject_rules_status(
    policy_store: &dyn klights_auth::rbac_policy_store::RbacPolicyStore,
    identity: &AuthenticatedIdentity,
    namespace: Option<&str>,
) -> Value {
    let (effective_resource, effective_non_resource, incomplete) = policy_store
        .enumerate_effective_rules(identity, namespace)
        .await;
    let resource_rules: Vec<Value> = effective_resource
        .iter()
        .map(|rule| {
            serde_json::json!({
                "verbs": rule.verbs,
                "apiGroups": [&rule.api_group],
                "resources": [&rule.resource],
                "resourceNames": rule.resource_names,
            })
        })
        .collect();
    let non_resource_rules: Vec<Value> = effective_non_resource
        .iter()
        .map(|rule| {
            serde_json::json!({
                "verbs": rule.verbs,
                "nonResourceURLs": rule.non_resource_urls,
            })
        })
        .collect();
    serde_json::json!({
        "resourceRules": resource_rules,
        "nonResourceRules": non_resource_rules,
        "incomplete": incomplete,
    })
}

pub(crate) async fn create_self_subject_access_review(
    State(state): State<Arc<ApiState>>,
    axum::Extension(identity): axum::Extension<AuthenticatedIdentity>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    reject_table_format(&headers, "SelfSubjectAccessReview")?;

    let mut decoded: Value = decode_json_or_proto(&body)?;
    let spec = take_spec(&mut decoded);

    let decision =
        evaluate_requested_subject(state.auth_policy().authorizer.as_ref(), &identity, &spec).await;

    Ok(Json(serde_json::json!({
        "apiVersion": "authorization.k8s.io/v1",
        "kind": "SelfSubjectAccessReview",
        "metadata": {
            "creationTimestamp": klights_cluster_core::k8s_time::format_time(
                klights_auth::clock::chrono_utc(state.operational().clock.now())
            )
        },
        "spec": spec,
        "status": decision_status(&decision),
    })))
}

#[cfg(test)]
#[path = "authorization_v1_tests.rs"]
mod tests;

pub(crate) async fn create_subject_access_review(
    State(state): State<Arc<ApiState>>,
    axum::Extension(identity): axum::Extension<AuthenticatedIdentity>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    reject_table_format(&headers, "SubjectAccessReview")?;

    let mut decoded: Value = decode_json_or_proto(&body)?;
    let spec = take_spec(&mut decoded);

    let decision =
        evaluate_subject_access_review(state.auth_policy().authorizer.as_ref(), &identity, &spec)
            .await?;

    Ok(Json(serde_json::json!({
        "apiVersion": "authorization.k8s.io/v1",
        "kind": "SubjectAccessReview",
        "metadata": {
            "creationTimestamp": klights_cluster_core::k8s_time::format_time(
                klights_auth::clock::chrono_utc(state.operational().clock.now())
            )
        },
        "spec": spec,
        "status": decision_status(&decision),
    })))
}

pub(crate) async fn create_self_subject_rules_review(
    State(state): State<Arc<ApiState>>,
    axum::Extension(identity): axum::Extension<AuthenticatedIdentity>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    reject_table_format(&headers, "SelfSubjectRulesReview")?;

    let mut decoded: Value = decode_json_or_proto(&body)?;
    let spec = take_spec(&mut decoded);

    // Phase 2B: enumerate effective rules from the RBAC policy store.
    let namespace = spec.get("namespace").and_then(|v| v.as_str());

    let status = self_subject_rules_status(
        state.auth_policy().rbac_policy_store.as_ref(),
        &identity,
        namespace,
    )
    .await;

    Ok(Json(serde_json::json!({
        "apiVersion": "authorization.k8s.io/v1",
        "kind": "SelfSubjectRulesReview",
        "metadata": {
            "creationTimestamp": klights_cluster_core::k8s_time::format_time(
                klights_auth::clock::chrono_utc(state.operational().clock.now())
            )
        },
        "spec": spec,
        "status": status
    })))
}

pub(crate) async fn create_local_subject_access_review(
    State(state): State<Arc<ApiState>>,
    Path(namespace): Path<String>,
    axum::Extension(identity): axum::Extension<AuthenticatedIdentity>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    reject_table_format(&headers, "LocalSubjectAccessReview")?;

    let mut decoded: Value = decode_json_or_proto(&body)?;
    let mut spec = take_spec(&mut decoded);
    stamp_local_review_namespace(&mut spec, &namespace);

    let decision =
        evaluate_requested_subject(state.auth_policy().authorizer.as_ref(), &identity, &spec).await;

    Ok(Json(serde_json::json!({
        "apiVersion": "authorization.k8s.io/v1",
        "kind": "LocalSubjectAccessReview",
        "metadata": {
            "creationTimestamp": klights_cluster_core::k8s_time::format_time(
                klights_auth::clock::chrono_utc(state.operational().clock.now())
            ),
            "namespace": namespace
        },
        "spec": spec,
        "status": decision_status(&decision),
    })))
}
