//! Request authentication for the klights API.

use crate::auth::identity::AuthenticatedIdentity;
use axum::extract::Request;
use axum::http::header::AUTHORIZATION;
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TlsClientCertificate(pub Vec<u8>);

/// Header carrying the *end user's* actual client certificate (base64 DER),
/// forwarded by a trusted follower API proxy to the leader so the leader can
/// cryptographically re-authenticate it against the cluster CA. Unlike the
/// `x-remote-*` assertion headers, this is a verifiable credential, not a
/// claim: it is how a kubectl admin's `system:masters` access survives the
/// proxy hop without letting a control plane mint that access by assertion.
pub const FORWARDED_CLIENT_CERT_HEADER: &str = "x-remote-client-certificate";

pub async fn authenticate_request(
    state: Arc<crate::api::AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    let requestheader_identity = requestheader_identity_from_headers(request.headers());
    let forwarded_client_cert = forwarded_client_cert_from_headers(request.headers());
    strip_remote_identity_headers(&mut request);

    let extension_user = request.extensions().get::<AuthenticatedIdentity>().cloned();
    let client_cert = request.extensions().get::<TlsClientCertificate>().cloned();
    // Proxy delegation trust is anchored to the *presented mTLS client
    // certificate* — the node/proxy cert, which the TLS layer already validated
    // against the cluster CA — and never to a username string (which a bearer
    // token authenticator such as a webhook/OIDC, or an injected extension
    // identity, could also produce). Only a request that arrives over a
    // connection authenticated with a trusted API-proxy client certificate may
    // cause the leader to honor a forwarded/delegated caller identity.
    let is_trusted_proxy = client_cert_is_trusted_proxy(client_cert.as_ref());
    let authorization = match request.headers().get(AUTHORIZATION) {
        Some(value) => match value.to_str() {
            Ok(raw) => Some(raw.to_string()),
            Err(_) => {
                return crate::api::AppError::Unauthorized(
                    "invalid Authorization header".to_string(),
                )
                .into_response();
            }
        },
        None => None,
    };

    let identity =
        match authenticate_parts(&state, extension_user, client_cert, authorization).await {
            Ok(id) => id,
            Err(err) => return err.into_response(),
        };
    let real_identity = identity.unwrap_or_else(AuthenticatedIdentity::anonymous);
    // A follower API proxy delegates the original caller's identity to the
    // leader. We only honor that delegation when the connection itself is
    // authenticated as a trusted internal API proxy (its own mTLS cert), and
    // the proxy's own credential must never be presented as the end user (its
    // cert is the transport, not the identity).
    let authenticated_identity = if is_trusted_proxy {
        if let Some(cert_der) = forwarded_client_cert {
            // Strongest, unforgeable delegation: the proxy forwarded the user's
            // actual client certificate. Re-authenticate it against the cluster
            // CA so the real identity — including `system:masters` — is proven,
            // not merely asserted. A forwarded cert that fails verification is
            // rejected rather than silently downgraded.
            match authenticate_forwarded_client_cert(&state, &cert_der) {
                Ok(id) => id,
                Err(err) => {
                    return crate::api::AppError::Unauthorized(format!(
                        "invalid forwarded client certificate: {err}"
                    ))
                    .into_response();
                }
            }
        } else if let Some(requestheader_identity) = requestheader_identity {
            // Non-cert credential (e.g. a bearer token the follower already
            // authenticated): fall back to the header-asserted identity, which
            // has `system:masters` stripped — a control plane cannot vouch for
            // cluster-admin on its own say-so.
            requestheader_identity
        } else {
            real_identity
        }
    } else {
        real_identity
    };
    let effective_identity = match crate::auth::impersonation::effective_identity_from_headers(
        state.authorizer.as_ref(),
        &authenticated_identity,
        request.headers(),
    )
    .await
    {
        Ok(id) => id,
        Err(err) => return err.into_response(),
    };
    inject_remote_identity_headers(&mut request, &effective_identity);
    request.extensions_mut().insert(effective_identity);

    next.run(request).await
}

/// Global authorization chokepoint.
///
/// Runs immediately after [`authenticate_request`] (so `AuthenticatedIdentity`
/// is present in extensions) and before any handler. It derives Kubernetes
/// authorization attributes from the request via
/// [`crate::auth::request_info::resolve_request_info`] and evaluates them
/// against the authorizer chain exactly once. This is what makes authorization
/// "secure by construction": no routed handler can run without passing through
/// here, so a handler can never accidentally skip RBAC. Kubernetes public
/// informational endpoints are authorized through the bootstrapped RBAC roles
/// such as `system:public-info-viewer`, `system:discovery`, and
/// `system:monitoring`.
pub async fn authorize_request(
    state: Arc<crate::api::AppState>,
    request: Request,
    next: Next,
) -> Response {
    use crate::auth::request_info::{ResolvedAuthz, resolve_request_info};

    let ResolvedAuthz::Authorize(authz) = resolve_request_info(
        request.method(),
        request.uri().path(),
        request.uri().query(),
    );

    let identity = request
        .extensions()
        .get::<AuthenticatedIdentity>()
        .cloned()
        .unwrap_or_else(AuthenticatedIdentity::anonymous);

    let decision = state.authorizer.authorize(&identity, &authz).await;
    if decision.allowed {
        return next.run(request).await;
    }

    let reason = if decision.reason.is_empty() {
        let target = authz
            .resource
            .as_deref()
            .or(authz.non_resource_url.as_deref())
            .unwrap_or("resource");
        format!(
            "forbidden: User \"{}\" cannot {} {target}",
            identity.username, authz.verb
        )
    } else {
        decision.reason
    };
    crate::api::AppError::Forbidden(reason).into_response()
}

async fn authenticate_parts(
    state: &crate::api::AppState,
    extension_user: Option<AuthenticatedIdentity>,
    client_cert: Option<TlsClientCertificate>,
    authorization: Option<String>,
) -> Result<Option<AuthenticatedIdentity>, crate::api::AppError> {
    if let Some(user) = extension_user {
        return Ok(Some(user));
    }

    if let Some(cert) = client_cert {
        let user = crate::auth::user_from_cert(&cert.0).map_err(|err| {
            crate::api::AppError::Unauthorized(format!("invalid client certificate: {err}"))
        })?;
        let identity = AuthenticatedIdentity::client_cert(user.username, user.groups);
        return Ok(Some(identity));
    }

    let Some(raw) = authorization else {
        return Ok(None);
    };
    let Some(token) = raw.strip_prefix("Bearer ") else {
        return Err(crate::api::AppError::Unauthorized(
            "unsupported Authorization scheme".to_string(),
        ));
    };

    Ok(Some(authenticate_bearer_token(state, token).await?))
}

async fn authenticate_bearer_token(
    state: &crate::api::AppState,
    token: &str,
) -> Result<AuthenticatedIdentity, crate::api::AppError> {
    match token.split('.').count() {
        2 => {
            match crate::bootstrap::bootstrap_token::validate_bootstrap_token(
                state.db.as_ref(),
                token,
            )
            .await
            {
                Ok(identity) => {
                    let id = AuthenticatedIdentity::bootstrap(
                        &identity.token_id,
                        &identity.extra_groups,
                    );
                    Ok(id)
                }
                Err(bootstrap_err) => {
                    if let Some(result) = crate::auth::webhook_auth::try_webhook_auth(
                        &state.webhook_authenticator,
                        token,
                    )
                    .await
                    {
                        return result;
                    }
                    Err(crate::api::AppError::Unauthorized(format!(
                        "invalid bootstrap token: {bootstrap_err}"
                    )))
                }
            }
        }
        3 => {
            // Try SA token first
            match validate_sa_token(state, token).await {
                Ok(identity) => Ok(identity),
                Err(_sa_err) => {
                    // SA failed — try OIDC if configured
                    if let Some(result) =
                        crate::auth::oidc::try_oidc_auth(&state.oidc_authenticator, token).await
                    {
                        return result;
                    }
                    // OIDC not configured — try webhook if configured
                    if let Some(result) = crate::auth::webhook_auth::try_webhook_auth(
                        &state.webhook_authenticator,
                        token,
                    )
                    .await
                    {
                        return result;
                    }
                    // Neither OIDC nor webhook configured — return SA error
                    Err(_sa_err)
                }
            }
        }
        _ => {
            if let Some(result) =
                crate::auth::webhook_auth::try_webhook_auth(&state.webhook_authenticator, token)
                    .await
            {
                return result;
            }
            Err(crate::api::AppError::Unauthorized(
                "invalid bearer token".to_string(),
            ))
        }
    }
}

async fn validate_sa_token(
    state: &crate::api::AppState,
    token: &str,
) -> Result<AuthenticatedIdentity, crate::api::AppError> {
    let signing_key_pem = crate::auth::read_service_account_signing_key_supervised(
        &state.config.containerd_namespace,
        state.task_supervisor.as_ref(),
    )
    .await
    .map_err(|err| {
        crate::api::AppError::Unauthorized(format!(
            "invalid serviceaccount token: failed to read signing key: {err}"
        ))
    })?;
    let audiences = vec!["https://kubernetes.default.svc.cluster.local".to_string()];
    let claims =
        crate::auth::decode_serviceaccount_token(token, &signing_key_pem, Some(&audiences))
            .map_err(|err| {
                crate::api::AppError::Unauthorized(format!("invalid serviceaccount token: {err}"))
            })?;

    validate_sa_token_bindings(state, &claims).await?;

    let groups = crate::auth::serviceaccount_groups_from_claims(&claims);
    let uid = crate::auth::serviceaccount_uid_from_claims(&claims);
    let identity = AuthenticatedIdentity::service_account(claims.sub.clone(), groups, uid);
    Ok(identity)
}

/// Validate that a decoded ServiceAccount token's bound subjects still exist
/// with matching UIDs. Shared by the request auth path and the TokenReview
/// handler so the two authentication surfaces cannot diverge.
///
/// Checks:
/// * the ServiceAccount UID — rejects tokens minted for a since-deleted or
///   recreated SA (revocation), and
/// * any bound pod / node reference — rejects bound (projected) tokens once the
///   pod or node they were minted for is gone or recreated with a new UID,
///   mirroring upstream Kubernetes bound-token invalidation.
pub async fn validate_sa_token_bindings(
    state: &crate::api::AppState,
    claims: &crate::auth::SaTokenClaims,
) -> Result<(), crate::api::AppError> {
    let Some((ns, sa_name)) = claims
        .sub
        .strip_prefix("system:serviceaccount:")
        .and_then(|rest| rest.split_once(':'))
    else {
        return Ok(());
    };

    // ServiceAccount UID (token revocation on SA delete/recreate).
    if let Some(token_uid) = crate::auth::serviceaccount_uid_from_claims(claims) {
        let stored_sa_uid = state
            .db
            .get_resource("v1", "ServiceAccount", Some(ns), sa_name)
            .await
            .ok()
            .flatten()
            .and_then(|sa| {
                sa.data
                    .pointer("/metadata/uid")
                    .and_then(|u| u.as_str())
                    .map(str::to_string)
            });
        crate::auth::validate_service_account_uid(Some(&token_uid), stored_sa_uid.as_deref())
            .map_err(|err| {
                crate::api::AppError::Unauthorized(format!(
                    "invalid serviceaccount token UID: {err}"
                ))
            })?;
    }

    // Bound pod / node existence + UID match.
    if let Some(k8s) = claims.kubernetes_io.as_ref() {
        if let Some(pod) = k8s.pod.as_ref()
            && let Some(pod_name) = pod.name.as_deref().filter(|v| !v.is_empty())
        {
            // Pod reads must go through the pod repository (PodStore), never a
            // direct ("v1","Pod") datastore call (actor-owned Pod invariant).
            let stored = crate::kubelet::pod_repository::PodReader::get_pod(
                state.pod_repository.as_ref(),
                ns,
                pod_name,
            )
            .await
            .ok()
            .flatten()
            .map(|p| p.uid);
            validate_bound_object_uid("pod", pod_name, pod.uid.as_deref(), stored.as_deref())?;
        }
        if let Some(node) = k8s.node.as_ref()
            && let Some(node_name) = node.name.as_deref().filter(|v| !v.is_empty())
        {
            let stored = state
                .db
                .get_resource("v1", "Node", None, node_name)
                .await
                .ok()
                .flatten()
                .and_then(|n| {
                    n.data
                        .pointer("/metadata/uid")
                        .and_then(|u| u.as_str())
                        .map(str::to_string)
                });
            validate_bound_object_uid("node", node_name, node.uid.as_deref(), stored.as_deref())?;
        }
        if let Some(secret) = k8s.secret.as_ref()
            && let Some(secret_name) = secret.name.as_deref().filter(|v| !v.is_empty())
        {
            let stored = state
                .db
                .get_resource("v1", "Secret", Some(ns), secret_name)
                .await
                .ok()
                .flatten()
                .and_then(|s| {
                    s.data
                        .pointer("/metadata/uid")
                        .and_then(|u| u.as_str())
                        .map(str::to_string)
                });
            validate_bound_object_uid(
                "secret",
                secret_name,
                secret.uid.as_deref(),
                stored.as_deref(),
            )?;
        }
    }

    Ok(())
}

fn validate_bound_object_uid(
    kind: &str,
    name: &str,
    token_uid: Option<&str>,
    stored_uid: Option<&str>,
) -> Result<(), crate::api::AppError> {
    match stored_uid {
        None => Err(crate::api::AppError::Unauthorized(format!(
            "serviceaccount token bound {kind} \"{name}\" no longer exists"
        ))),
        Some(stored) => match token_uid {
            Some(tok) if tok != stored => Err(crate::api::AppError::Unauthorized(format!(
                "serviceaccount token bound {kind} \"{name}\" UID mismatch"
            ))),
            _ => Ok(()),
        },
    }
}

fn strip_remote_identity_headers(request: &mut Request) {
    request.headers_mut().remove("x-remote-user");
    request.headers_mut().remove("x-remote-group");
    request.headers_mut().remove("x-remote-uid");
    request.headers_mut().remove(FORWARDED_CLIENT_CERT_HEADER);
    let extra_headers = request
        .headers()
        .keys()
        .filter(|name| name.as_str().starts_with("x-remote-extra-"))
        .cloned()
        .collect::<Vec<HeaderName>>();
    for name in extra_headers {
        request.headers_mut().remove(name);
    }
}

fn requestheader_identity_from_headers(headers: &HeaderMap) -> Option<AuthenticatedIdentity> {
    let username = headers
        .get("x-remote-user")
        .and_then(|value| value.to_str().ok())?
        .to_string();
    if username.is_empty() {
        return None;
    }

    // A front-proxy asserts the *end user's* identity via these headers. It must
    // never be able to elevate that user into the cluster-admin group
    // `system:masters` (which the default cluster-admin ClusterRoleBinding
    // targets) purely by injecting a header. Drop it from asserted groups so a
    // compromised/over-broad proxy cert cannot mint admin access. Enforced
    // policy must not exceed the advertised requestheader trust.
    let groups = headers
        .get_all("x-remote-group")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .filter(|group| *group != "system:masters")
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let uid = headers
        .get("x-remote-uid")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);
    let extra = headers
        .iter()
        .filter_map(|(name, value)| {
            let key = name.as_str().strip_prefix("x-remote-extra-")?;
            let value = value.to_str().ok()?;
            Some((key.to_string(), value.to_string()))
        })
        .collect::<Vec<_>>();

    Some(AuthenticatedIdentity {
        username,
        groups,
        uid,
        extra,
    })
}

/// Decode the forwarded end-user client certificate (base64 DER) from the
/// [`FORWARDED_CLIENT_CERT_HEADER`], if present and well-formed.
fn forwarded_client_cert_from_headers(headers: &HeaderMap) -> Option<Vec<u8>> {
    use base64::Engine;
    let raw = headers
        .get(FORWARDED_CLIENT_CERT_HEADER)
        .and_then(|value| value.to_str().ok())?;
    base64::engine::general_purpose::STANDARD.decode(raw).ok()
}

/// Re-authenticate a forwarded client certificate against the cluster CA.
///
/// Returns the proven end-user identity (CN → username, O → groups). Fails if
/// no cluster CA is configured or the certificate is not validly CA-signed /
/// is outside its validity window — callers must reject, never downgrade.
fn authenticate_forwarded_client_cert(
    state: &crate::api::AppState,
    cert_der: &[u8],
) -> anyhow::Result<AuthenticatedIdentity> {
    let ca_pem = state.cluster_ca_pem.as_deref().ok_or_else(|| {
        anyhow::anyhow!("no cluster CA configured to verify forwarded client certificate")
    })?;
    let user = crate::auth::verify_client_cert_signed_by_ca(cert_der, ca_pem)?;
    Ok(AuthenticatedIdentity::client_cert(
        user.username,
        user.groups,
    ))
}

/// Decide whether the peer on this connection is a trusted internal API proxy,
/// based solely on the certificate it presented at the TLS layer.
///
/// This is the *only* gate that lets the leader honor a delegated/forwarded
/// caller identity. It is deliberately keyed on the presented mTLS client
/// certificate rather than on the resolved `AuthenticatedIdentity`: a bearer
/// token authenticator (webhook/OIDC/bootstrap) or an extension-injected
/// identity could otherwise mint a username under the API-proxy prefix and be
/// wrongly trusted to delegate. A present client certificate, by contrast, has
/// already been verified against the cluster CA by the TLS stack, so its
/// subject is authoritative.
fn client_cert_is_trusted_proxy(client_cert: Option<&TlsClientCertificate>) -> bool {
    let Some(cert) = client_cert else {
        return false;
    };
    let Ok(user) = crate::auth::user_from_cert(&cert.0) else {
        return false;
    };
    is_trusted_api_proxy_identity(&AuthenticatedIdentity::client_cert(
        user.username,
        user.groups,
    ))
}

fn is_trusted_api_proxy_identity(identity: &AuthenticatedIdentity) -> bool {
    let Some(node_name) = identity
        .username
        .strip_prefix(crate::auth::API_PROXY_COMMON_NAME_PREFIX)
    else {
        return false;
    };
    !node_name.is_empty()
        && !identity
            .groups
            .iter()
            .any(|group| group == "system:masters")
}

fn inject_remote_identity_headers(request: &mut Request, user: &AuthenticatedIdentity) {
    if let Ok(value) = HeaderValue::from_str(&user.username) {
        request.headers_mut().insert("x-remote-user", value);
    }
    for group in &user.groups {
        if let Ok(value) = HeaderValue::from_str(group) {
            request.headers_mut().append("x-remote-group", value);
        }
    }
    if let Some(uid) = user.uid.as_deref()
        && let Ok(value) = HeaderValue::from_str(uid)
    {
        request.headers_mut().insert("x-remote-uid", value);
    }
    for (key, value) in &user.extra {
        let Ok(header_name) = HeaderName::from_bytes(format!("x-remote-extra-{key}").as_bytes())
        else {
            continue;
        };
        if let Ok(header_value) = HeaderValue::from_str(value) {
            request.headers_mut().append(header_name, header_value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::webhook_auth::{
        TokenReviewStatus, TokenReviewUser, WebhookAuth, WebhookTokenReviewer,
    };
    use std::time::Duration;

    struct StaticWebhookReviewer {
        status: TokenReviewStatus,
    }

    #[async_trait::async_trait]
    impl WebhookTokenReviewer for StaticWebhookReviewer {
        async fn review_token(
            &self,
            _token: &str,
            _audiences: &[String],
        ) -> Result<Option<TokenReviewStatus>, String> {
            Ok(Some(self.status.clone()))
        }
    }

    fn sa_claims(value: serde_json::Value) -> crate::auth::SaTokenClaims {
        serde_json::from_value(value).expect("valid SaTokenClaims")
    }

    #[test]
    fn requestheader_identity_strips_system_masters_group() {
        // The x-remote-* headers are an *assertion* by the proxy ("trust me,
        // the user is X in groups Y"). A proxy must never be able to elevate a
        // user into `system:masters` purely by asserting that group in a header
        // — that is the control plane vouching for cluster-admin on its own say
        // so. Real cluster-admin access must instead be *proven* by forwarding
        // the user's actual CA-signed client certificate (see
        // `forwarded_client_cert_from_headers` /
        // `verify_client_cert_signed_by_ca`), which an attacker cannot forge
        // without the CA key. So the header-asserted path drops system:masters.
        let mut headers = HeaderMap::new();
        headers.insert("x-remote-user", HeaderValue::from_static("alice"));
        headers.append("x-remote-group", HeaderValue::from_static("dev"));
        headers.append("x-remote-group", HeaderValue::from_static("system:masters"));
        headers.append(
            "x-remote-group",
            HeaderValue::from_static("system:authenticated"),
        );

        let identity = requestheader_identity_from_headers(&headers).expect("identity present");
        assert_eq!(identity.username, "alice");
        assert!(
            !identity.groups.iter().any(|g| g == "system:masters"),
            "a front proxy must not be able to assert system:masters via header injection"
        );
        assert!(identity.groups.contains(&"dev".to_string()));
        assert!(
            identity
                .groups
                .contains(&"system:authenticated".to_string())
        );
    }

    #[test]
    fn only_api_proxy_client_cert_is_trusted_to_delegate() {
        let (ca_cert, ca_key, _, _) = crate::auth::generate_ca_full().unwrap();

        // A real API-proxy client cert (CA-signed, CN under the proxy prefix,
        // no system:masters) is the only thing trusted to delegate.
        let (proxy_pem, _) = crate::auth::cert::generate_api_proxy_cert(&ca_cert, &ca_key, "cp1")
            .expect("api-proxy cert");
        let proxy_der = TlsClientCertificate(pem_to_der(&proxy_pem));
        assert!(
            client_cert_is_trusted_proxy(Some(&proxy_der)),
            "a well-formed api-proxy client cert must be trusted to delegate"
        );

        // The admin client cert (system:masters) is a normal end-user cert, not
        // a proxy — it must never be treated as a delegating proxy.
        let (admin_pem, _) = crate::auth::generate_admin_cert(&ca_cert, &ca_key).unwrap();
        let admin_der = TlsClientCertificate(pem_to_der(&admin_pem));
        assert!(
            !client_cert_is_trusted_proxy(Some(&admin_der)),
            "an admin/end-user cert must not be trusted as a delegating proxy"
        );

        // No client cert at all (e.g. a bearer-token-only connection) is never a
        // trusted proxy — so a token whose username happens to fall under the
        // api-proxy prefix cannot unlock delegation.
        assert!(!client_cert_is_trusted_proxy(None));
    }

    #[test]
    fn proxy_cannot_be_trusted_using_its_own_master_credential() {
        use crate::auth::cert::api_proxy_common_name;

        // Defense in depth on the subject check itself: even a cert under the
        // api-proxy prefix that *also* carries system:masters in its own O must
        // not be trusted to delegate (that would be a control plane elevating
        // itself rather than faithfully delegating an end user's access).
        let elevated_proxy = AuthenticatedIdentity::client_cert(
            api_proxy_common_name("cp1"),
            vec!["system:masters".to_string()],
        );
        assert!(
            !is_trusted_api_proxy_identity(&elevated_proxy),
            "a proxy presenting system:masters in its own cert must not be trusted to delegate"
        );

        // A normal proxy credential (api-proxy CN, no system:masters) is trusted.
        let proxy = AuthenticatedIdentity::client_cert(api_proxy_common_name("cp1"), vec![]);
        assert!(is_trusted_api_proxy_identity(&proxy));

        // A non-proxy identity (e.g. a normal user) is never a trusted proxy.
        let user = AuthenticatedIdentity::client_cert("klights-admin".to_string(), vec![]);
        assert!(!is_trusted_api_proxy_identity(&user));
    }

    #[test]
    fn forwarded_client_cert_header_roundtrips_base64_der() {
        use base64::Engine;
        let der = vec![0x30u8, 0x82, 0x01, 0x02, 0x03];
        let encoded = base64::engine::general_purpose::STANDARD.encode(&der);
        let mut headers = HeaderMap::new();
        headers.insert(
            FORWARDED_CLIENT_CERT_HEADER,
            HeaderValue::from_str(&encoded).unwrap(),
        );
        assert_eq!(forwarded_client_cert_from_headers(&headers), Some(der));

        // Absent header → None; malformed base64 → None (never panics).
        assert_eq!(forwarded_client_cert_from_headers(&HeaderMap::new()), None);
        let mut bad = HeaderMap::new();
        bad.insert(
            FORWARDED_CLIENT_CERT_HEADER,
            HeaderValue::from_static("not base64!!!"),
        );
        assert_eq!(forwarded_client_cert_from_headers(&bad), None);
    }

    #[tokio::test]
    async fn forwarded_admin_cert_authenticates_as_system_masters() {
        let (ca_cert, ca_key, ca_pem, _) = crate::auth::generate_ca_full().unwrap();
        let (admin_pem, _) = crate::auth::generate_admin_cert(&ca_cert, &ca_key).unwrap();
        let der = pem_to_der(&admin_pem);

        let mut state = crate::api::test_support::build_test_app_state().await;
        state.cluster_ca_pem = Some(std::sync::Arc::new(ca_pem));

        let identity = authenticate_forwarded_client_cert(&state, &der)
            .expect("CA-signed admin cert must authenticate");
        assert_eq!(identity.username, "klights-admin");
        assert!(
            identity.groups.iter().any(|g| g == "system:masters"),
            "forwarded admin cert must keep system:masters via native re-auth"
        );
    }

    #[tokio::test]
    async fn forwarded_cert_rejected_when_no_cluster_ca_configured() {
        let (ca_cert, ca_key, _, _) = crate::auth::generate_ca_full().unwrap();
        let (admin_pem, _) = crate::auth::generate_admin_cert(&ca_cert, &ca_key).unwrap();
        let der = pem_to_der(&admin_pem);

        // No CA configured (cluster_ca_pem is None): cannot verify → reject.
        let state = crate::api::test_support::build_test_app_state().await;
        assert!(authenticate_forwarded_client_cert(&state, &der).is_err());
    }

    fn pem_to_der(pem_str: &str) -> Vec<u8> {
        use x509_parser::pem::Pem;
        let (pem, _) = Pem::read(std::io::Cursor::new(pem_str.as_bytes())).unwrap();
        pem.contents
    }

    async fn seed_sa(state: &crate::api::AppState, ns: &str, name: &str, uid: &str) {
        state
            .db
            .create_resource(
                "v1",
                "ServiceAccount",
                Some(ns),
                name,
                serde_json::json!({
                    "apiVersion": "v1", "kind": "ServiceAccount",
                    "metadata": {"name": name, "namespace": ns, "uid": uid}
                }),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn bound_token_rejected_when_pod_deleted() {
        let state = crate::api::test_support::build_test_app_state().await;
        seed_sa(&state, "default", "myapp", "sa-uid-1").await;
        // Token bound to pod p1/pod-uid-1, but the pod does not exist.
        let claims = sa_claims(serde_json::json!({
            "sub": "system:serviceaccount:default:myapp",
            "kubernetes.io": {
                "serviceaccount": {"uid": "sa-uid-1"},
                "pod": {"name": "p1", "uid": "pod-uid-1"}
            }
        }));
        let res = validate_sa_token_bindings(&state, &claims).await;
        assert!(
            res.is_err(),
            "token bound to a deleted pod must be rejected"
        );
    }

    #[tokio::test]
    async fn bound_token_rejected_when_pod_uid_mismatch() {
        let state = crate::api::test_support::build_test_app_state().await;
        seed_sa(&state, "default", "myapp", "sa-uid-1").await;
        state
            .db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "p1",
                serde_json::json!({
                    "apiVersion": "v1", "kind": "Pod",
                    "metadata": {"name": "p1", "namespace": "default", "uid": "different-uid"}
                }),
            )
            .await
            .unwrap();
        let claims = sa_claims(serde_json::json!({
            "sub": "system:serviceaccount:default:myapp",
            "kubernetes.io": {
                "serviceaccount": {"uid": "sa-uid-1"},
                "pod": {"name": "p1", "uid": "pod-uid-1"}
            }
        }));
        let res = validate_sa_token_bindings(&state, &claims).await;
        assert!(
            res.is_err(),
            "token bound to a recreated pod must be rejected"
        );
    }

    #[tokio::test]
    async fn bound_token_accepted_when_pod_present_and_uid_matches() {
        let state = crate::api::test_support::build_test_app_state().await;
        seed_sa(&state, "default", "myapp", "sa-uid-1").await;
        state
            .db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "p1",
                serde_json::json!({
                    "apiVersion": "v1", "kind": "Pod",
                    "metadata": {"name": "p1", "namespace": "default", "uid": "pod-uid-1"}
                }),
            )
            .await
            .unwrap();
        let claims = sa_claims(serde_json::json!({
            "sub": "system:serviceaccount:default:myapp",
            "kubernetes.io": {
                "serviceaccount": {"uid": "sa-uid-1"},
                "pod": {"name": "p1", "uid": "pod-uid-1"}
            }
        }));
        let res = validate_sa_token_bindings(&state, &claims).await;
        assert!(res.is_ok(), "matching bound pod must pass: {res:?}");
    }

    #[tokio::test]
    async fn secret_bound_token_rejected_when_secret_deleted() {
        let state = crate::api::test_support::build_test_app_state().await;
        seed_sa(&state, "default", "myapp", "sa-uid-1").await;
        // Token bound to a Secret that does not exist → revoked.
        let claims = sa_claims(serde_json::json!({
            "sub": "system:serviceaccount:default:myapp",
            "kubernetes.io": {
                "serviceaccount": {"uid": "sa-uid-1"},
                "secret": {"name": "s1", "uid": "secret-uid-1"}
            }
        }));
        let res = validate_sa_token_bindings(&state, &claims).await;
        assert!(
            res.is_err(),
            "token bound to a deleted secret must be rejected"
        );
    }

    #[tokio::test]
    async fn secret_bound_token_accepted_when_secret_present_and_uid_matches() {
        let state = crate::api::test_support::build_test_app_state().await;
        seed_sa(&state, "default", "myapp", "sa-uid-1").await;
        state
            .db
            .create_resource(
                "v1",
                "Secret",
                Some("default"),
                "s1",
                serde_json::json!({
                    "apiVersion": "v1", "kind": "Secret",
                    "metadata": {"name": "s1", "namespace": "default", "uid": "secret-uid-1"}
                }),
            )
            .await
            .unwrap();
        let claims = sa_claims(serde_json::json!({
            "sub": "system:serviceaccount:default:myapp",
            "kubernetes.io": {
                "serviceaccount": {"uid": "sa-uid-1"},
                "secret": {"name": "s1", "uid": "secret-uid-1"}
            }
        }));
        let res = validate_sa_token_bindings(&state, &claims).await;
        assert!(res.is_ok(), "matching bound secret must pass: {res:?}");
    }

    #[tokio::test]
    async fn unbound_token_unaffected_by_pod_checks() {
        // A plain (non-projected) SA token with no pod/node binding must still
        // pass as long as the SA exists with the right UID.
        let state = crate::api::test_support::build_test_app_state().await;
        seed_sa(&state, "default", "myapp", "sa-uid-1").await;
        let claims = sa_claims(serde_json::json!({
            "sub": "system:serviceaccount:default:myapp",
            "kubernetes.io": {"serviceaccount": {"uid": "sa-uid-1"}}
        }));
        let res = validate_sa_token_bindings(&state, &claims).await;
        assert!(res.is_ok(), "unbound token must pass: {res:?}");
    }

    #[tokio::test]
    async fn opaque_bearer_token_falls_back_to_webhook_authenticator() {
        let mut state = crate::api::test_support::build_test_app_state().await;
        state.webhook_authenticator = Some(Arc::new(WebhookAuth::new(
            Arc::new(StaticWebhookReviewer {
                status: TokenReviewStatus {
                    authenticated: true,
                    user: Some(TokenReviewUser {
                        username: "opaque-user".to_string(),
                        uid: Some("opaque-uid".to_string()),
                        groups: vec!["opaque-group".to_string()],
                        extra: Vec::new(),
                    }),
                    error: None,
                    audiences: Vec::new(),
                },
            }),
            Duration::from_secs(60),
            Duration::from_secs(10),
            vec!["https://kubernetes.default.svc.cluster.local".to_string()],
        )));

        let identity = authenticate_bearer_token(&state, "opaque-token")
            .await
            .expect("opaque token should authenticate through webhook fallback");

        assert_eq!(identity.username, "opaque-user");
        assert_eq!(identity.uid, Some("opaque-uid".to_string()));
        assert!(identity.groups.contains(&"opaque-group".to_string()));
    }
}
