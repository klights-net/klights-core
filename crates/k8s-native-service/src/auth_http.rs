//! HTTP adaptation for request authentication and authorization.

use std::sync::Arc;

use crate::policy_inputs::{AuthenticationHttpInputs, AuthorizationHttpInputs};
use axum::extract::Request;
use axum::http::header::AUTHORIZATION;
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use klights_auth::{AuthenticationError, ImpersonationError};
use klights_types::TlsClientCertificate;

use super::AppError;
use klights_auth::AuthenticatedIdentity;
use klights_auth::authentication::{
    AuthnRuntime, authenticate_forwarded_client_cert, authenticate_parts,
    client_cert_is_trusted_proxy, resolve_request_identity,
};
use klights_auth::clock::SnapshotClock;
use klights_auth::impersonation::ImpersonationRequest;

pub const FORWARDED_CLIENT_CERT_HEADER: &str = "x-remote-client-certificate";
const IMPERSONATE_USER: &str = "impersonate-user";
const IMPERSONATE_GROUP: &str = "impersonate-group";
const IMPERSONATE_UID: &str = "impersonate-uid";
const IMPERSONATE_EXTRA_PREFIX: &str = "impersonate-extra-";

pub async fn authenticate_token_for_review(
    inputs: &AuthenticationHttpInputs,
    token: &str,
    audiences: &[String],
) -> Result<klights_auth::authentication::ReviewedTokenIdentity, AuthenticationError> {
    let policy = inputs.policy();
    let runtime_inputs = inputs.runtime();
    let clock = SnapshotClock::new(runtime_inputs.clock().now());
    let runtime = AuthnRuntime::new(
        policy.bootstrap_token_authenticator().as_ref(),
        runtime_inputs.signing_keys().as_ref(),
        runtime_inputs.bound_token_subjects().as_ref(),
        policy.oidc_authenticator().map(Arc::as_ref),
        policy.webhook_authenticator().map(Arc::as_ref),
        &clock,
        runtime_inputs.task_supervisor().as_ref(),
        false,
    );
    klights_auth::authentication::authenticate_token_for_review(&runtime, token, audiences).await
}

pub async fn authenticate_request(
    inputs: Arc<AuthenticationHttpInputs>,
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
        inputs.runtime().task_supervisor().as_ref(),
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

    let policy = inputs.policy();
    let runtime_inputs = inputs.runtime();
    let clock = SnapshotClock::new(runtime_inputs.clock().now());
    let runtime = AuthnRuntime::new(
        policy.bootstrap_token_authenticator().as_ref(),
        runtime_inputs.signing_keys().as_ref(),
        runtime_inputs.bound_token_subjects().as_ref(),
        policy.oidc_authenticator().map(Arc::as_ref),
        policy.webhook_authenticator().map(Arc::as_ref),
        &clock,
        runtime_inputs.task_supervisor().as_ref(),
        policy.anonymous_auth(),
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
                policy.cluster_ca_pem().map(|pem| pem.as_str()),
                &cert_der,
                runtime_inputs.task_supervisor().as_ref(),
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
    let effective_identity = match klights_auth::impersonation::effective_identity(
        policy.authorizer().as_ref(),
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
pub async fn authorize_request(
    inputs: Arc<AuthorizationHttpInputs<dyn crate::audit::AuditSink>>,
    request: Request,
    next: Next,
) -> Response {
    use crate::request_info::{ResolvedAuthz, resolve_request_info};

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

    let decision = inputs
        .authorizer()
        .authorize(&identity, &authorization)
        .await;
    let operation_now = klights_auth::clock::chrono_utc(inputs.clock().now());
    inputs
        .audit()
        .record(crate::audit::AuditEvent::authorization(
            &identity,
            &authorization,
            &decision,
            operation_now,
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
mod tests;
