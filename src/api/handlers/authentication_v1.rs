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

/// TokenReview — create-only resource, no Table support.
pub async fn create_token_review(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    if wants_table_format(&headers)? {
        return Err(AppError::NotAcceptable(
            "Table format is not supported for TokenReview".to_string(),
        ));
    }

    let req_body = decode_json_or_proto(&body)?;
    let req: TokenReviewRequest = serde_json::from_value(req_body)
        .map_err(|e| AppError::BadRequest(format!("Invalid TokenReview payload: {}", e)))?;
    let requested_audiences = req.spec.audiences.unwrap_or_default();
    let token = req.spec.token.unwrap_or_default();

    if token.is_empty() {
        return Ok(Json(serde_json::json!({
            "apiVersion": "authentication.k8s.io/v1",
            "kind": "TokenReview",
            "status": {
                "authenticated": false
            }
        })));
    }

    let signing_key_pem =
        crate::auth::read_service_account_signing_key_async(&state.config.containerd_namespace)
            .await
            .map_err(|e| AppError::InternalError(format!("Failed to read signing key: {e}")))?;
    let claims = match crate::auth::decode_serviceaccount_token(
        &token,
        &signing_key_pem,
        Some(&requested_audiences),
    ) {
        Ok(claims) => claims,
        Err(_) => {
            return Ok(Json(serde_json::json!({
                "apiVersion": "authentication.k8s.io/v1",
                "kind": "TokenReview",
                "status": {
                    "authenticated": false
                }
            })));
        }
    };

    // Honor SA-UID revocation and bound pod/node invalidation, exactly like the
    // request auth path — otherwise TokenReview would report a token from a
    // deleted SA (or bound to a deleted pod) as authenticated.
    if crate::auth::validate_sa_token_bindings(&state, &claims)
        .await
        .is_err()
    {
        return Ok(Json(serde_json::json!({
            "apiVersion": "authentication.k8s.io/v1",
            "kind": "TokenReview",
            "status": {
                "authenticated": false
            }
        })));
    }

    let audiences = if requested_audiences.is_empty() {
        claims.aud.clone()
    } else {
        requested_audiences
            .into_iter()
            .filter(|aud| claims.aud.iter().any(|claim_aud| claim_aud == aud))
            .collect()
    };

    Ok(Json(serde_json::json!({
        "apiVersion": "authentication.k8s.io/v1",
        "kind": "TokenReview",
        "status": {
            "authenticated": true,
            "user": tokenreview_user_from_claims(&claims),
            "audiences": audiences
        }
    })))
}
