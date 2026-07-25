use crate::api::*;

#[derive(Deserialize)]
pub struct TokenReviewRequest {
    #[serde(default)]
    spec: TokenReviewSpec,
}

#[derive(Deserialize, Default)]
pub struct TokenReviewSpec {
    token: Option<String>,
    audiences: Option<Vec<String>>,
}

pub fn tokenreview_user_from_claims(claims: &crate::auth::SaTokenClaims) -> Value {
    let groups = crate::auth::serviceaccount_groups_from_claims(claims);
    let mut extra = serde_json::Map::new();

    if let Some((_, rest)) = claims.sub.split_once("system:serviceaccount:")
        && let Some((namespace, name)) = rest.split_once(':')
    {
        extra.insert(
            "authentication.kubernetes.io/serviceaccount.name".to_string(),
            serde_json::json!([name]),
        );
        extra.insert(
            "authentication.kubernetes.io/serviceaccount.namespace".to_string(),
            serde_json::json!([namespace]),
        );
    }

    if let Some(uid) = claims
        .kubernetes_io
        .as_ref()
        .and_then(|k| k.serviceaccount.as_ref())
        .and_then(|sa| sa.uid.as_deref())
    {
        extra.insert(
            "authentication.kubernetes.io/serviceaccount.uid".to_string(),
            serde_json::json!([uid]),
        );
    }
    if let Some(jti) = claims.jti.as_deref().filter(|v| !v.is_empty()) {
        extra.insert(
            "authentication.kubernetes.io/credential-id".to_string(),
            serde_json::json!([format!("JTI={jti}")]),
        );
    }
    if let Some(pod_name) = claims
        .kubernetes_io
        .as_ref()
        .and_then(|k| k.pod.as_ref())
        .and_then(|p| p.name.as_deref())
        .filter(|v| !v.is_empty())
    {
        extra.insert(
            "authentication.kubernetes.io/pod-name".to_string(),
            serde_json::json!([pod_name]),
        );
    }
    if let Some(pod_uid) = claims
        .kubernetes_io
        .as_ref()
        .and_then(|k| k.pod.as_ref())
        .and_then(|p| p.uid.as_deref())
        .filter(|v| !v.is_empty())
    {
        extra.insert(
            "authentication.kubernetes.io/pod-uid".to_string(),
            serde_json::json!([pod_uid]),
        );
    }
    if let Some(node_name) = claims
        .kubernetes_io
        .as_ref()
        .and_then(|k| k.node.as_ref())
        .and_then(|n| n.name.as_deref())
        .filter(|v| !v.is_empty())
    {
        extra.insert(
            "authentication.kubernetes.io/node-name".to_string(),
            serde_json::json!([node_name]),
        );
    }
    if let Some(node_uid) = claims
        .kubernetes_io
        .as_ref()
        .and_then(|k| k.node.as_ref())
        .and_then(|n| n.uid.as_deref())
        .filter(|v| !v.is_empty())
    {
        extra.insert(
            "authentication.kubernetes.io/node-uid".to_string(),
            serde_json::json!([node_uid]),
        );
    }

    let mut user = serde_json::json!({
        "username": claims.sub,
        "groups": groups
    });
    if !extra.is_empty() {
        user["extra"] = Value::Object(extra);
    }
    user
}

pub fn tokenreview_user_from_identity(
    identity: &crate::auth::identity::AuthenticatedIdentity,
) -> Value {
    let mut user = serde_json::json!({
        "username": identity.username,
        "groups": identity.groups,
    });
    if let Some(uid) = identity.uid.as_deref() {
        user["uid"] = serde_json::json!(uid);
    }
    if !identity.extra.is_empty() {
        let mut extra = serde_json::Map::new();
        for (key, value) in &identity.extra {
            extra
                .entry(key.clone())
                .or_insert_with(|| serde_json::json!([]))
                .as_array_mut()
                .expect("inserted extra value must be an array")
                .push(serde_json::json!(value));
        }
        user["extra"] = Value::Object(extra);
    }
    user
}

fn token_review_response(mut request: Value, status: Value) -> Value {
    if request.get("metadata").is_none() {
        request["metadata"] = serde_json::json!({});
    }
    request["status"] = status;
    request
}

fn authenticated_token_review(request: Value, user: Value, audiences: Vec<String>) -> Value {
    let mut status = serde_json::json!({
        "authenticated": true,
        "user": user,
    });
    if !audiences.is_empty() {
        status["audiences"] = serde_json::json!(audiences);
    }
    token_review_response(request, status)
}

/// TokenReview — create-only resource, no Table support.
pub async fn create_token_review(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<crate::api::response::K8sResponse, AppError> {
    if wants_table_format(&headers)? {
        return Err(AppError::NotAcceptable(
            "Table format is not supported for TokenReview".to_string(),
        ));
    }

    let req_body = decode_json_or_proto(&body)?;
    let req: TokenReviewRequest = TokenReviewRequest::deserialize(&req_body)
        .map_err(|e| AppError::BadRequest(format!("Invalid TokenReview payload: {}", e)))?;
    let requested_audiences = req.spec.audiences.unwrap_or_default();
    let token = req.spec.token.unwrap_or_default();

    if token.is_empty() {
        return Err(AppError::BadRequest(
            "TokenReview spec.token must not be empty".to_string(),
        ));
    }

    match crate::api::auth_middleware::authenticate_token_for_review(
        &state,
        &token,
        &requested_audiences,
    )
    .await
    {
        Ok(crate::auth::middleware::ReviewedTokenIdentity::ServiceAccount {
            claims,
            audiences,
        }) => Ok(crate::api::response::K8sResponse::new(
            authenticated_token_review(req_body, tokenreview_user_from_claims(&claims), audiences),
            &headers,
        )),
        Ok(crate::auth::middleware::ReviewedTokenIdentity::Other {
            identity,
            audiences,
        }) => Ok(crate::api::response::K8sResponse::new(
            authenticated_token_review(
                req_body,
                tokenreview_user_from_identity(&identity),
                audiences,
            ),
            &headers,
        )),
        Err(klights_auth::AuthenticationError::Unauthenticated { .. }) => {
            Ok(crate::api::response::K8sResponse::new(
                token_review_response(req_body, serde_json::json!({"authenticated": false})),
                &headers,
            ))
        }
        Err(error) => Ok(crate::api::response::K8sResponse::new(
            token_review_response(
                req_body,
                serde_json::json!({
                    "authenticated": false,
                    "error": error.to_string(),
                }),
            ),
            &headers,
        )),
    }
}
