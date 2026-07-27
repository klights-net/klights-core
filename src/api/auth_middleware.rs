//! HTTP adaptation for request authentication and authorization.

use std::sync::Arc;

use axum::extract::Request;
use axum::http::header::AUTHORIZATION;
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use klights_auth::{AuthenticationError, ImpersonationError};
use klights_types::TlsClientCertificate;

use super::{ApiState, AppError};
use crate::auth::clock::SystemClock;
use crate::auth::identity::AuthenticatedIdentity;
use crate::auth::impersonation::ImpersonationRequest;
use crate::auth::middleware::{
    AuthnRuntime, BoundTokenSubjectLookup, ServiceAccountSigningKeyProvider,
    authenticate_forwarded_client_cert, authenticate_parts, client_cert_is_trusted_proxy,
    resolve_request_identity,
};

pub const FORWARDED_CLIENT_CERT_HEADER: &str = "x-remote-client-certificate";
const IMPERSONATE_USER: &str = "impersonate-user";
const IMPERSONATE_GROUP: &str = "impersonate-group";
const IMPERSONATE_UID: &str = "impersonate-uid";
const IMPERSONATE_EXTRA_PREFIX: &str = "impersonate-extra-";

struct ApiAuthResources<'a> {
    state: &'a ApiState,
}

impl ApiAuthResources<'_> {
    async fn resource_uid(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<String>, AuthenticationError> {
        self.state
            .resource_mutation()
            .db
            .get_resource(api_version, kind, namespace, name)
            .await
            .map_err(|error| {
                AuthenticationError::dependency_failure(format!(
                    "credential subject lookup failed: {error}"
                ))
            })
            .map(|resource| {
                resource.and_then(|resource| {
                    resource
                        .data
                        .pointer("/metadata/uid")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string)
                })
            })
    }
}

#[async_trait::async_trait]
impl ServiceAccountSigningKeyProvider for ApiAuthResources<'_> {
    async fn service_account_signing_key_pem(&self) -> Result<String, AuthenticationError> {
        let signing_key_path = crate::paths::service_account_signing_key_path(
            &self.state.operational().config.containerd_namespace,
        );
        crate::auth::read_service_account_signing_key_supervised(
            &signing_key_path,
            self.state.operational().task_supervisor.as_ref(),
        )
        .await
        .map_err(|error| {
            AuthenticationError::dependency_failure(format!(
                "serviceaccount signing key unavailable: {error}"
            ))
        })
    }
}

#[async_trait::async_trait]
impl BoundTokenSubjectLookup for ApiAuthResources<'_> {
    async fn service_account_uid(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<String>, AuthenticationError> {
        self.resource_uid("v1", "ServiceAccount", Some(namespace), name)
            .await
    }

    async fn pod_uid(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<String>, AuthenticationError> {
        crate::api::pod_repository_ports::get_pod(
            self.state.resource_mutation().pod_repository.as_ref(),
            namespace,
            name,
        )
        .await
        .map(|pod| pod.map(|pod| pod.uid))
        .map_err(|error| {
            AuthenticationError::dependency_failure(format!("bound Pod lookup failed: {error}"))
        })
    }

    async fn node_uid(&self, name: &str) -> Result<Option<String>, AuthenticationError> {
        self.resource_uid("v1", "Node", None, name).await
    }

    async fn secret_uid(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<String>, AuthenticationError> {
        self.resource_uid("v1", "Secret", Some(namespace), name)
            .await
    }
}

#[cfg(test)]
async fn validate_sa_token_bindings(
    state: &ApiState,
    claims: &crate::auth::SaTokenClaims,
) -> Result<(), AppError> {
    crate::auth::validate_sa_token_bindings(&ApiAuthResources { state }, claims)
        .await
        .map_err(AppError::from)
}

pub(in crate::api) async fn authenticate_token_for_review(
    state: &ApiState,
    token: &str,
    audiences: &[String],
) -> Result<crate::auth::middleware::ReviewedTokenIdentity, AuthenticationError> {
    let resources = ApiAuthResources { state };
    let clock = SystemClock;
    let auth_policy = state.auth_policy();
    let runtime = AuthnRuntime::new(
        auth_policy.bootstrap_token_authenticator.as_ref(),
        &resources,
        &resources,
        auth_policy.oidc_authenticator.as_deref(),
        auth_policy.webhook_authenticator.as_deref(),
        &clock,
        &state.operational().task_supervisor,
        false,
    );
    crate::auth::middleware::authenticate_token_for_review(&runtime, token, audiences).await
}

pub(in crate::api) async fn authenticate_request(
    state: Arc<ApiState>,
    mut request: Request,
    next: Next,
) -> Response {
    let requestheader_identity = requestheader_identity_from_headers(request.headers());
    let forwarded_client_cert = forwarded_client_cert_from_headers(request.headers());
    strip_remote_identity_headers(&mut request);

    let extension_user = request.extensions().get::<AuthenticatedIdentity>().cloned();
    let client_cert = request.extensions().get::<TlsClientCertificate>().cloned();
    let is_trusted_proxy = match client_cert_is_trusted_proxy(
        client_cert.as_ref(),
        &state.operational().task_supervisor,
    )
    .await
    {
        Ok(is_trusted) => is_trusted,
        Err(error) => return AppError::from(error).into_response(),
    };
    let authorization = match request.headers().get(AUTHORIZATION) {
        Some(value) => match value.to_str() {
            Ok(raw) => Some(raw.to_string()),
            Err(_) => {
                return AppError::Unauthorized("invalid Authorization header".to_string())
                    .into_response();
            }
        },
        None => None,
    };

    let resources = ApiAuthResources { state: &state };
    let clock = SystemClock;
    let auth_policy = state.auth_policy();
    let runtime = AuthnRuntime::new(
        auth_policy.bootstrap_token_authenticator.as_ref(),
        &resources,
        &resources,
        auth_policy.oidc_authenticator.as_deref(),
        auth_policy.webhook_authenticator.as_deref(),
        &clock,
        &state.operational().task_supervisor,
        state.operational().config.anonymous_auth,
    );
    let identity = match authenticate_parts(&runtime, extension_user, client_cert, authorization)
        .await
        .and_then(|identity| resolve_request_identity(&runtime, identity))
    {
        Ok(identity) => identity,
        Err(error) => return AppError::from(error).into_response(),
    };

    let authenticated_identity = if is_trusted_proxy {
        if let Some(cert_der) = forwarded_client_cert {
            match authenticate_forwarded_client_cert(
                auth_policy.cluster_ca_pem.as_deref().map(String::as_str),
                &cert_der,
                &state.operational().task_supervisor,
            )
            .await
            {
                Ok(identity) => identity,
                Err(error) => {
                    return AppError::from(error).into_response();
                }
            }
        } else if let Some(requestheader_identity) = requestheader_identity {
            requestheader_identity
        } else {
            identity
        }
    } else {
        identity
    };

    let impersonation = match parse_impersonation_headers(request.headers()) {
        Ok(impersonation) => impersonation,
        Err(error) => return AppError::from(error).into_response(),
    };
    let effective_identity = match crate::auth::impersonation::effective_identity(
        auth_policy.authorizer.as_ref(),
        &authenticated_identity,
        impersonation,
    )
    .await
    {
        Ok(identity) => identity,
        Err(error) => return AppError::from(error).into_response(),
    };
    inject_remote_identity_headers(&mut request, &effective_identity);
    request.extensions_mut().insert(effective_identity);

    next.run(request).await
}

/// Global authorization chokepoint for every routed and fallback request.
pub(in crate::api) async fn authorize_request(
    state: Arc<ApiState>,
    request: Request,
    next: Next,
) -> Response {
    use crate::api::request_info::{ResolvedAuthz, resolve_request_info};

    let ResolvedAuthz::Authorize(authorization) = resolve_request_info(
        request.method(),
        request.uri().path(),
        request.uri().query(),
    );
    let identity = request
        .extensions()
        .get::<AuthenticatedIdentity>()
        .cloned()
        .unwrap_or_else(AuthenticatedIdentity::anonymous);

    let auth_policy = state.auth_policy();
    let decision = auth_policy
        .authorizer
        .authorize(&identity, &authorization)
        .await;
    auth_policy
        .audit_sink
        .record(crate::audit::AuditEvent::authorization(
            &identity,
            &authorization,
            &decision,
        ));
    if decision.allowed {
        return next.run(request).await;
    }

    let reason = if decision.reason.is_empty() {
        let target = authorization
            .resource
            .as_deref()
            .or(authorization.non_resource_url.as_deref())
            .unwrap_or("resource");
        format!(
            "forbidden: User \"{}\" cannot {} {target}",
            identity.username, authorization.verb
        )
    } else {
        decision.reason
    };
    AppError::Forbidden(reason).into_response()
}

fn parse_impersonation_headers(
    headers: &HeaderMap,
) -> Result<Option<ImpersonationRequest>, ImpersonationError> {
    let users = header_values(headers, IMPERSONATE_USER)?;
    let groups = header_values(headers, IMPERSONATE_GROUP)?;
    let uids = header_values(headers, IMPERSONATE_UID)?;
    let extra = impersonation_extra_values(headers)?;

    if users.is_empty() {
        if !groups.is_empty() || !uids.is_empty() || !extra.is_empty() {
            return Err(ImpersonationError::invalid_request(
                "Impersonate-User is required when using impersonation headers",
            ));
        }
        return Ok(None);
    }
    if users.len() > 1 {
        return Err(ImpersonationError::invalid_request(
            "Impersonate-User may only be specified once",
        ));
    }
    let username = users.into_iter().next().expect("one impersonated user");
    if username.is_empty() {
        return Err(ImpersonationError::invalid_request(
            "Impersonate-User must not be empty",
        ));
    }
    if groups.iter().any(String::is_empty) {
        return Err(ImpersonationError::invalid_request(
            "Impersonate-Group must not be empty",
        ));
    }
    if uids.len() > 1 {
        return Err(ImpersonationError::invalid_request(
            "Impersonate-Uid may only be specified once",
        ));
    }
    if uids.iter().any(String::is_empty) {
        return Err(ImpersonationError::invalid_request(
            "Impersonate-Uid must not be empty",
        ));
    }

    Ok(Some(ImpersonationRequest {
        username,
        groups,
        uid: uids.into_iter().next(),
        extra,
    }))
}

fn header_values(
    headers: &HeaderMap,
    name: &'static str,
) -> Result<Vec<String>, ImpersonationError> {
    headers
        .get_all(name)
        .iter()
        .map(|value| {
            value.to_str().map(str::to_string).map_err(|_| {
                ImpersonationError::invalid_request(format!("{name} contains invalid header value"))
            })
        })
        .collect()
}

fn impersonation_extra_values(
    headers: &HeaderMap,
) -> Result<Vec<(String, String)>, ImpersonationError> {
    let mut extra_headers = headers
        .keys()
        .filter_map(|name| {
            header_suffix_ignore_ascii_case(name.as_str(), IMPERSONATE_EXTRA_PREFIX)
                .map(|suffix| (name.clone(), suffix.to_string()))
        })
        .collect::<Vec<(HeaderName, String)>>();
    extra_headers.sort_by(|left, right| left.1.cmp(&right.1));

    let mut values = Vec::new();
    for (name, suffix) in extra_headers {
        if suffix.is_empty() {
            return Err(ImpersonationError::invalid_request(
                "Impersonate-Extra header name must not be empty",
            ));
        }
        let decoded = urlencoding::decode(&suffix)
            .map_err(|_| {
                ImpersonationError::invalid_request(format!(
                    "invalid Impersonate-Extra header name: {suffix}"
                ))
            })?
            .into_owned();
        for value in headers.get_all(&name).iter() {
            let value = value.to_str().map_err(|_| {
                ImpersonationError::invalid_request(format!(
                    "{} contains invalid header value",
                    name.as_str()
                ))
            })?;
            if value.is_empty() {
                return Err(ImpersonationError::invalid_request(
                    "Impersonate-Extra value must not be empty",
                ));
            }
            values.push((decoded.clone(), value.to_string()));
        }
    }
    Ok(values)
}

fn header_suffix_ignore_ascii_case<'a>(name: &'a str, prefix: &str) -> Option<&'a str> {
    name.get(..prefix.len())
        .is_some_and(|actual| actual.eq_ignore_ascii_case(prefix))
        .then(|| &name[prefix.len()..])
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
        .collect::<Vec<_>>();
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
    let groups = headers
        .get_all("x-remote-group")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .filter(|group| *group != "system:masters")
        .map(str::to_string)
        .collect();
    let uid = headers
        .get("x-remote-uid")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let extra = headers
        .iter()
        .filter_map(|(name, value)| {
            let key = name.as_str().strip_prefix("x-remote-extra-")?;
            Some((key.to_string(), value.to_str().ok()?.to_string()))
        })
        .collect();
    Some(AuthenticatedIdentity {
        username,
        groups,
        uid,
        extra,
    })
}

fn forwarded_client_cert_from_headers(headers: &HeaderMap) -> Option<Vec<u8>> {
    use base64::Engine;
    let raw = headers
        .get(FORWARDED_CLIENT_CERT_HEADER)
        .and_then(|value| value.to_str().ok())?;
    base64::engine::general_purpose::STANDARD.decode(raw).ok()
}

fn inject_remote_identity_headers(request: &mut Request, identity: &AuthenticatedIdentity) {
    if let Ok(value) = HeaderValue::from_str(&identity.username) {
        request.headers_mut().insert("x-remote-user", value);
    }
    for group in &identity.groups {
        if let Ok(value) = HeaderValue::from_str(group) {
            request.headers_mut().append("x-remote-group", value);
        }
    }
    if let Some(uid) = identity.uid.as_deref()
        && let Ok(value) = HeaderValue::from_str(uid)
    {
        request.headers_mut().insert("x-remote-uid", value);
    }
    for (key, value) in &identity.extra {
        let Ok(name) = HeaderName::from_bytes(format!("x-remote-extra-{key}").as_bytes()) else {
            continue;
        };
        if let Ok(value) = HeaderValue::from_str(value) {
            request.headers_mut().append(name, value);
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use super::*;

    #[test]
    fn impersonation_header_extraction_is_api_owned_and_strict() {
        let mut headers = HeaderMap::new();
        headers.append(IMPERSONATE_GROUP, HeaderValue::from_static("developers"));
        assert!(matches!(
            parse_impersonation_headers(&headers),
            Err(ImpersonationError::InvalidRequest { .. })
        ));

        headers.insert(IMPERSONATE_USER, HeaderValue::from_static("alice"));
        headers.insert("impersonate-extra-scopes", HeaderValue::from_static("view"));
        let request = parse_impersonation_headers(&headers)
            .expect("valid headers")
            .expect("impersonation present");
        assert_eq!(request.username, "alice");
        assert_eq!(request.groups, vec!["developers"]);
        assert_eq!(
            request.extra,
            vec![("scopes".to_string(), "view".to_string())]
        );
    }

    #[test]
    fn requestheader_identity_cannot_assert_system_masters() {
        let mut headers = HeaderMap::new();
        headers.insert("x-remote-user", HeaderValue::from_static("alice"));
        headers.append("x-remote-group", HeaderValue::from_static("developers"));
        headers.append("x-remote-group", HeaderValue::from_static("system:masters"));
        let identity = requestheader_identity_from_headers(&headers).unwrap();
        assert_eq!(identity.groups, vec!["developers"]);
    }

    #[test]
    fn forwarded_client_cert_header_roundtrips_base64_der() {
        use base64::Engine;

        let der = vec![0x30, 0x82, 0x01, 0x02, 0x03];
        let encoded = base64::engine::general_purpose::STANDARD.encode(&der);
        let mut headers = HeaderMap::new();
        headers.insert(
            FORWARDED_CLIENT_CERT_HEADER,
            HeaderValue::from_str(&encoded).unwrap(),
        );
        assert_eq!(forwarded_client_cert_from_headers(&headers), Some(der));

        assert_eq!(forwarded_client_cert_from_headers(&HeaderMap::new()), None);
        headers.insert(
            FORWARDED_CLIENT_CERT_HEADER,
            HeaderValue::from_static("not base64!!!"),
        );
        assert_eq!(forwarded_client_cert_from_headers(&headers), None);
    }

    fn sa_claims(value: serde_json::Value) -> crate::auth::SaTokenClaims {
        serde_json::from_value(value).expect("valid ServiceAccount token claims")
    }

    async fn seed_service_account(
        state: &crate::api::ApiState,
        namespace: &str,
        name: &str,
        uid: &str,
    ) {
        state
            .resource_mutation()
            .db
            .create_resource(
                "v1",
                "ServiceAccount",
                Some(namespace),
                name,
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ServiceAccount",
                    "metadata": {
                        "name": name,
                        "namespace": namespace,
                        "uid": uid
                    }
                }),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn bound_pod_adapter_is_uid_qualified_and_fail_closed() {
        let state = crate::api::test_support::build_test_app_state().await;
        seed_service_account(&state, "default", "myapp", "sa-uid-1").await;
        let claims = |pod_uid: &str| {
            sa_claims(serde_json::json!({
                "sub": "system:serviceaccount:default:myapp",
                "kubernetes.io": {
                    "serviceaccount": {"uid": "sa-uid-1"},
                    "pod": {"name": "p1", "uid": pod_uid}
                }
            }))
        };

        assert!(
            validate_sa_token_bindings(&state, &claims("pod-uid-1"))
                .await
                .is_err(),
            "a token bound to a deleted Pod must be rejected"
        );

        state
            .resource_mutation()
            .db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "p1",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "name": "p1",
                        "namespace": "default",
                        "uid": "pod-uid-1"
                    }
                }),
            )
            .await
            .unwrap();
        assert!(
            validate_sa_token_bindings(&state, &claims("replacement-uid"))
                .await
                .is_err(),
            "a token bound to a same-name replacement Pod must be rejected"
        );
        assert!(
            validate_sa_token_bindings(&state, &claims("pod-uid-1"))
                .await
                .is_ok(),
            "a token bound to the current Pod UID must pass"
        );
    }

    #[tokio::test]
    async fn bound_secret_adapter_is_uid_qualified_and_fail_closed() {
        let state = crate::api::test_support::build_test_app_state().await;
        seed_service_account(&state, "default", "myapp", "sa-uid-1").await;
        let claims = sa_claims(serde_json::json!({
            "sub": "system:serviceaccount:default:myapp",
            "kubernetes.io": {
                "serviceaccount": {"uid": "sa-uid-1"},
                "secret": {"name": "s1", "uid": "secret-uid-1"}
            }
        }));

        assert!(
            validate_sa_token_bindings(&state, &claims).await.is_err(),
            "a token bound to a deleted Secret must be rejected"
        );
        state
            .resource_mutation()
            .db
            .create_resource(
                "v1",
                "Secret",
                Some("default"),
                "s1",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Secret",
                    "metadata": {
                        "name": "s1",
                        "namespace": "default",
                        "uid": "secret-uid-1"
                    }
                }),
            )
            .await
            .unwrap();
        assert!(
            validate_sa_token_bindings(&state, &claims).await.is_ok(),
            "a token bound to the current Secret UID must pass"
        );
    }

    #[tokio::test]
    async fn unbound_service_account_token_uses_only_service_account_identity() {
        let state = crate::api::test_support::build_test_app_state().await;
        seed_service_account(&state, "default", "myapp", "sa-uid-1").await;
        let claims = sa_claims(serde_json::json!({
            "sub": "system:serviceaccount:default:myapp",
            "kubernetes.io": {"serviceaccount": {"uid": "sa-uid-1"}}
        }));

        assert!(validate_sa_token_bindings(&state, &claims).await.is_ok());
    }

    #[tokio::test]
    async fn authorization_denial_writes_structured_audit_event() {
        let audit_sink = Arc::new(crate::audit::MemoryAuditSink::default());
        let authorizer: Arc<dyn crate::auth::authorizer::Authorizer> = Arc::new(
            crate::auth::authorizer::RecordingAuthorizer::deny("policy denied secret read"),
        );
        let mut state =
            crate::api::test_support::build_test_app_state_with_authorizer(authorizer).await;
        state.audit_sink = audit_sink.clone();
        let app = crate::api::build_router(state);

        let mut request = Request::builder()
            .method("GET")
            .uri("/api/v1/namespaces/default/secrets/db-password")
            .body(Body::empty())
            .unwrap();
        request
            .extensions_mut()
            .insert(AuthenticatedIdentity::client_cert(
                "alice".to_string(),
                Vec::new(),
            ));

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let events = audit_sink.events();
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.stage, crate::audit::AuditStage::Authorization);
        assert_eq!(event.user.username, "alice");
        assert_eq!(event.verb, "get");
        assert_eq!(event.api_group.as_deref(), None);
        assert_eq!(event.api_version.as_deref(), Some("v1"));
        assert_eq!(event.resource.as_deref(), Some("secrets"));
        assert_eq!(event.subresource.as_deref(), None);
        assert_eq!(event.namespace.as_deref(), Some("default"));
        assert_eq!(event.name.as_deref(), Some("db-password"));
        assert_eq!(event.non_resource_url.as_deref(), None);
        assert!(!event.allowed);
        assert_eq!(event.reason, "policy denied secret read");
    }

    #[tokio::test]
    async fn pod_exec_authorization_writes_high_value_audit_event() {
        let audit_sink = Arc::new(crate::audit::MemoryAuditSink::default());
        let authorizer: Arc<dyn crate::auth::authorizer::Authorizer> =
            Arc::new(crate::auth::authorizer::RecordingAuthorizer::allow());
        let mut state =
            crate::api::test_support::build_test_app_state_with_authorizer(authorizer).await;
        state.audit_sink = audit_sink.clone();
        let app = crate::api::build_router(state);

        let mut request = Request::builder()
            .method("POST")
            .uri("/api/v1/namespaces/default/pods/web/exec?container=app&command=id")
            .body(Body::empty())
            .unwrap();
        request
            .extensions_mut()
            .insert(AuthenticatedIdentity::client_cert(
                "operator".to_string(),
                Vec::new(),
            ));

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let events = audit_sink.events();
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.stage, crate::audit::AuditStage::Authorization);
        assert_eq!(event.user.username, "operator");
        assert_eq!(event.verb, "create");
        assert_eq!(event.resource.as_deref(), Some("pods"));
        assert_eq!(event.subresource.as_deref(), Some("exec"));
        assert_eq!(event.namespace.as_deref(), Some("default"));
        assert_eq!(event.name.as_deref(), Some("web"));
        assert!(event.allowed);
        assert!(event.high_value);
    }

    #[tokio::test]
    async fn auth_policy_failures_have_json_and_protobuf_parity() {
        use prost::Message;

        #[derive(Clone, Copy)]
        enum Failure {
            InvalidAuthorization,
            InvalidImpersonation,
            ForbiddenImpersonation,
            AuthorizationDenied,
        }

        struct FailureCase {
            failure: Failure,
            expected_status: StatusCode,
            expected_reason: &'static str,
            expected_message: &'static str,
        }

        let failures = [
            FailureCase {
                failure: Failure::InvalidAuthorization,
                expected_status: StatusCode::UNAUTHORIZED,
                expected_reason: "Unauthorized",
                expected_message: "invalid Authorization header",
            },
            FailureCase {
                failure: Failure::InvalidImpersonation,
                expected_status: StatusCode::BAD_REQUEST,
                expected_reason: "BadRequest",
                expected_message: "Impersonate-User is required when using impersonation headers",
            },
            FailureCase {
                failure: Failure::ForbiddenImpersonation,
                expected_status: StatusCode::FORBIDDEN,
                expected_reason: "Forbidden",
                expected_message: "impersonation denied",
            },
            FailureCase {
                failure: Failure::AuthorizationDenied,
                expected_status: StatusCode::FORBIDDEN,
                expected_reason: "Forbidden",
                expected_message: "authorization denied",
            },
        ];
        let encodings = [
            (None, "application/json"),
            (
                Some("application/vnd.kubernetes.protobuf"),
                "application/vnd.kubernetes.protobuf",
            ),
        ];

        for failure in failures {
            for (accept, expected_content_type) in encodings {
                let state = match failure.failure {
                    Failure::ForbiddenImpersonation => {
                        let authorizer: Arc<dyn crate::auth::authorizer::Authorizer> =
                            Arc::new(crate::auth::authorizer::RecordingAuthorizer::deny(
                                "impersonation denied",
                            ));
                        crate::api::test_support::build_test_app_state_with_authorizer(authorizer)
                            .await
                    }
                    Failure::AuthorizationDenied => {
                        let authorizer: Arc<dyn crate::auth::authorizer::Authorizer> =
                            Arc::new(crate::auth::authorizer::RecordingAuthorizer::deny(
                                "authorization denied",
                            ));
                        crate::api::test_support::build_test_app_state_with_authorizer(authorizer)
                            .await
                    }
                    Failure::InvalidAuthorization | Failure::InvalidImpersonation => {
                        crate::api::test_support::build_test_app_state().await
                    }
                };
                let app = crate::api::build_router(state);
                let mut request = Request::builder().uri("/api");
                if let Some(accept) = accept {
                    request = request.header("accept", accept);
                }
                let mut request = request.body(Body::empty()).unwrap();
                match failure.failure {
                    Failure::InvalidAuthorization => {
                        request.headers_mut().insert(
                            AUTHORIZATION,
                            HeaderValue::from_bytes(&[0xff]).expect("opaque invalid header"),
                        );
                    }
                    Failure::InvalidImpersonation => {
                        request
                            .extensions_mut()
                            .insert(AuthenticatedIdentity::client_cert(
                                "alice".to_string(),
                                Vec::new(),
                            ));
                        request
                            .headers_mut()
                            .insert(IMPERSONATE_GROUP, HeaderValue::from_static("developers"));
                    }
                    Failure::ForbiddenImpersonation => {
                        request
                            .extensions_mut()
                            .insert(AuthenticatedIdentity::client_cert(
                                "alice".to_string(),
                                Vec::new(),
                            ));
                        request
                            .headers_mut()
                            .insert(IMPERSONATE_USER, HeaderValue::from_static("bob"));
                    }
                    Failure::AuthorizationDenied => {
                        request
                            .extensions_mut()
                            .insert(AuthenticatedIdentity::client_cert(
                                "alice".to_string(),
                                Vec::new(),
                            ));
                    }
                }

                let response = app.oneshot(request).await.unwrap();
                assert_eq!(response.status(), failure.expected_status);
                assert_eq!(
                    response
                        .headers()
                        .get("content-type")
                        .and_then(|value| value.to_str().ok()),
                    Some(expected_content_type)
                );
                let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
                let (kind, reason, code, message) = if expected_content_type == "application/json" {
                    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
                    (
                        value["kind"].as_str().unwrap().to_string(),
                        value["reason"].as_str().unwrap().to_string(),
                        value["code"].as_u64().unwrap() as u16,
                        value["message"].as_str().unwrap().to_string(),
                    )
                } else {
                    assert_eq!(&body[..4], b"k8s\0");
                    let envelope = klights_kube_protobuf::Unknown::decode(&body[4..]).unwrap();
                    let type_meta = envelope.type_meta.expect("Status type metadata");
                    let status =
                        klights_kube_protobuf::apimachinery::pkg::apis::meta::v1::Status::decode(
                            envelope.raw.as_slice(),
                        )
                        .unwrap();
                    (
                        type_meta.kind,
                        status.reason.unwrap_or_default(),
                        u16::try_from(status.code.unwrap_or_default()).unwrap(),
                        status.message.unwrap_or_default(),
                    )
                };
                assert_eq!(kind, "Status");
                assert_eq!(reason, failure.expected_reason);
                assert_eq!(
                    code,
                    failure.expected_status.as_u16(),
                    "decoded Status code"
                );
                assert_eq!(message, failure.expected_message);
            }
        }
    }
}
