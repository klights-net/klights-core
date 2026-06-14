//! Kubernetes impersonation header parsing and authorization.

use crate::api::AppError;
use crate::auth::authorizer::{AuthorizationDecision, Authorizer};
use crate::auth::identity::AuthenticatedIdentity;
use crate::auth::request_attributes::AuthorizationRequest;
use axum::http::{HeaderMap, HeaderName};

const IMPERSONATE_USER: &str = "impersonate-user";
const IMPERSONATE_GROUP: &str = "impersonate-group";
const IMPERSONATE_UID: &str = "impersonate-uid";
const IMPERSONATE_EXTRA_PREFIX: &str = "impersonate-extra-";
const AUTHENTICATION_API_GROUP: &str = "authentication.k8s.io";
const SERVICEACCOUNT_PREFIX: &str = "system:serviceaccount:";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImpersonationRequest {
    pub username: String,
    pub groups: Vec<String>,
    pub uid: Option<String>,
    pub extra: Vec<(String, String)>,
}

pub async fn effective_identity_from_headers(
    authorizer: &dyn Authorizer,
    real_identity: &AuthenticatedIdentity,
    headers: &HeaderMap,
) -> Result<AuthenticatedIdentity, AppError> {
    let Some(request) = parse_impersonation_headers(headers)? else {
        return Ok(real_identity.clone());
    };

    authorize_impersonation(authorizer, real_identity, &request).await?;

    Ok(AuthenticatedIdentity {
        username: request.username,
        groups: request.groups,
        uid: request.uid,
        extra: request.extra,
    })
}

pub fn parse_impersonation_headers(
    headers: &HeaderMap,
) -> Result<Option<ImpersonationRequest>, AppError> {
    let users = header_values(headers, IMPERSONATE_USER)?;
    let groups = header_values(headers, IMPERSONATE_GROUP)?;
    let uids = header_values(headers, IMPERSONATE_UID)?;
    let extra = impersonation_extra_values(headers)?;

    if users.is_empty() {
        if !groups.is_empty() || !uids.is_empty() || !extra.is_empty() {
            return Err(AppError::BadRequest(
                "Impersonate-User is required when using impersonation headers".to_string(),
            ));
        }
        return Ok(None);
    }

    if users.len() > 1 {
        return Err(AppError::BadRequest(
            "Impersonate-User may only be specified once".to_string(),
        ));
    }
    let username = users.into_iter().next().unwrap();
    if username.is_empty() {
        return Err(AppError::BadRequest(
            "Impersonate-User must not be empty".to_string(),
        ));
    }

    if groups.iter().any(String::is_empty) {
        return Err(AppError::BadRequest(
            "Impersonate-Group must not be empty".to_string(),
        ));
    }
    if uids.len() > 1 {
        return Err(AppError::BadRequest(
            "Impersonate-Uid may only be specified once".to_string(),
        ));
    }
    if uids.iter().any(String::is_empty) {
        return Err(AppError::BadRequest(
            "Impersonate-Uid must not be empty".to_string(),
        ));
    }

    Ok(Some(ImpersonationRequest {
        username,
        groups,
        uid: uids.into_iter().next(),
        extra,
    }))
}

async fn authorize_impersonation(
    authorizer: &dyn Authorizer,
    real_identity: &AuthenticatedIdentity,
    request: &ImpersonationRequest,
) -> Result<(), AppError> {
    let (api_group, resource, namespace, name) =
        if let Some((namespace, name)) = service_account_username_parts(&request.username) {
            ("", "serviceaccounts", Some(namespace), name)
        } else {
            ("", "users", None, request.username.as_str())
        };
    authorize_impersonate_value(
        authorizer,
        real_identity,
        api_group,
        resource,
        namespace,
        name,
    )
    .await?;

    for group in &request.groups {
        authorize_impersonate_value(authorizer, real_identity, "", "groups", None, group).await?;
    }

    if let Some(uid) = request.uid.as_deref() {
        authorize_impersonate_value(
            authorizer,
            real_identity,
            AUTHENTICATION_API_GROUP,
            "uids",
            None,
            uid,
        )
        .await?;
    }

    for (key, value) in &request.extra {
        let resource = format!("userextras/{key}");
        authorize_impersonate_value(
            authorizer,
            real_identity,
            AUTHENTICATION_API_GROUP,
            &resource,
            None,
            value,
        )
        .await?;
    }

    Ok(())
}

async fn authorize_impersonate_value(
    authorizer: &dyn Authorizer,
    real_identity: &AuthenticatedIdentity,
    api_group: &str,
    resource: &str,
    namespace: Option<&str>,
    name: &str,
) -> Result<(), AppError> {
    let request = AuthorizationRequest::resource(
        "impersonate",
        api_group,
        "",
        resource,
        None,
        namespace,
        Some(name),
    );
    let decision = authorizer.authorize(real_identity, &request).await;
    if decision.allowed {
        return Ok(());
    }
    Err(AppError::Forbidden(impersonation_forbidden_message(
        &decision, resource, name,
    )))
}

fn impersonation_forbidden_message(
    decision: &AuthorizationDecision,
    resource: &str,
    name: &str,
) -> String {
    if let Some(error) = decision.evaluation_error.as_deref() {
        return format!("cannot impersonate {resource} {name}: {error}");
    }
    if !decision.reason.is_empty() {
        return decision.reason.clone();
    }
    format!("cannot impersonate {resource} {name}")
}

fn header_values(headers: &HeaderMap, name: &'static str) -> Result<Vec<String>, AppError> {
    headers
        .get_all(name)
        .iter()
        .map(|value| {
            value
                .to_str()
                .map(|s| s.to_string())
                .map_err(|_| AppError::BadRequest(format!("{name} contains invalid header value")))
        })
        .collect()
}

fn impersonation_extra_values(headers: &HeaderMap) -> Result<Vec<(String, String)>, AppError> {
    let mut extra_headers = headers
        .keys()
        .filter_map(|name| {
            header_suffix_ignore_ascii_case(name.as_str(), IMPERSONATE_EXTRA_PREFIX)
                .map(|suffix| (name.clone(), suffix.to_string()))
        })
        .collect::<Vec<(HeaderName, String)>>();
    extra_headers.sort_by(|a, b| a.1.cmp(&b.1));

    let mut values = Vec::new();
    for (name, suffix) in extra_headers {
        if suffix.is_empty() {
            return Err(AppError::BadRequest(
                "Impersonate-Extra header name must not be empty".to_string(),
            ));
        }
        let decoded = urlencoding::decode(&suffix)
            .map_err(|_| {
                AppError::BadRequest(format!("invalid Impersonate-Extra header name: {suffix}"))
            })?
            .into_owned();
        for value in headers.get_all(&name).iter() {
            let value = value.to_str().map_err(|_| {
                AppError::BadRequest(format!("{} contains invalid header value", name.as_str()))
            })?;
            if value.is_empty() {
                return Err(AppError::BadRequest(
                    "Impersonate-Extra value must not be empty".to_string(),
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

fn service_account_username_parts(username: &str) -> Option<(&str, &str)> {
    let rest = username.strip_prefix(SERVICEACCOUNT_PREFIX)?;
    let (namespace, name) = rest.split_once(':')?;
    if namespace.is_empty() || name.is_empty() || name.contains(':') {
        return None;
    }
    Some((namespace, name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::authorizer::AuthorizationDecision;
    use async_trait::async_trait;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    struct SequenceAuthorizer {
        decisions: Mutex<VecDeque<AuthorizationDecision>>,
        seen_requests: Arc<Mutex<Vec<AuthorizationRequest>>>,
    }

    #[async_trait]
    impl Authorizer for SequenceAuthorizer {
        async fn authorize(
            &self,
            _identity: &AuthenticatedIdentity,
            request: &AuthorizationRequest,
        ) -> AuthorizationDecision {
            self.seen_requests.lock().unwrap().push(request.clone());
            self.decisions
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| AuthorizationDecision::deny("mock exhausted"))
        }
    }

    fn authorizer(
        decisions: Vec<AuthorizationDecision>,
    ) -> (SequenceAuthorizer, Arc<Mutex<Vec<AuthorizationRequest>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        (
            SequenceAuthorizer {
                decisions: Mutex::new(VecDeque::from(decisions)),
                seen_requests: seen.clone(),
            },
            seen,
        )
    }

    #[test]
    fn parse_requires_user_for_other_impersonation_headers() {
        let mut headers = HeaderMap::new();
        headers.append(IMPERSONATE_GROUP, "developers".parse().unwrap());

        let err = parse_impersonation_headers(&headers).expect_err("missing user must fail");

        assert!(matches!(err, AppError::BadRequest(_)));
    }

    #[tokio::test]
    async fn service_account_impersonation_authorizes_each_impersonated_attribute() {
        let (authorizer, seen) = authorizer(vec![
            AuthorizationDecision::allow("sa"),
            AuthorizationDecision::allow("group-authenticated"),
            AuthorizationDecision::allow("group-sa"),
            AuthorizationDecision::allow("group-sa-ns"),
            AuthorizationDecision::allow("uid"),
            AuthorizationDecision::allow("extra"),
        ]);
        let mut headers = HeaderMap::new();
        headers.insert(
            IMPERSONATE_USER,
            "system:serviceaccount:default:e2e".parse().unwrap(),
        );
        headers.append(IMPERSONATE_GROUP, "system:authenticated".parse().unwrap());
        headers.append(IMPERSONATE_GROUP, "system:serviceaccounts".parse().unwrap());
        headers.append(
            IMPERSONATE_GROUP,
            "system:serviceaccounts:default".parse().unwrap(),
        );
        headers.insert(IMPERSONATE_UID, "sa-uid-a".parse().unwrap());
        headers.append("impersonate-extra-scopes", "view".parse().unwrap());

        let identity = effective_identity_from_headers(
            &authorizer,
            &AuthenticatedIdentity::admin("real-admin"),
            &headers,
        )
        .await
        .expect("authorized impersonation should succeed");

        assert_eq!(identity.username, "system:serviceaccount:default:e2e");
        assert_eq!(
            identity.groups,
            vec![
                "system:authenticated",
                "system:serviceaccounts",
                "system:serviceaccounts:default"
            ]
        );
        assert_eq!(identity.uid.as_deref(), Some("sa-uid-a"));
        assert_eq!(
            identity.extra,
            vec![("scopes".to_string(), "view".to_string())]
        );

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 6);
        assert_eq!(
            seen[0],
            AuthorizationRequest::resource(
                "impersonate",
                "",
                "",
                "serviceaccounts",
                None,
                Some("default"),
                Some("e2e")
            )
        );
        assert_eq!(
            seen[1],
            AuthorizationRequest::resource(
                "impersonate",
                "",
                "",
                "groups",
                None,
                None,
                Some("system:authenticated")
            )
        );
        assert_eq!(
            seen[4],
            AuthorizationRequest::resource(
                "impersonate",
                AUTHENTICATION_API_GROUP,
                "",
                "uids",
                None,
                None,
                Some("sa-uid-a")
            )
        );
        assert_eq!(
            seen[5],
            AuthorizationRequest::resource(
                "impersonate",
                AUTHENTICATION_API_GROUP,
                "",
                "userextras/scopes",
                None,
                None,
                Some("view")
            )
        );
    }

    #[tokio::test]
    async fn impersonation_denied_without_permission() {
        let (authorizer, _seen) = authorizer(vec![AuthorizationDecision::deny("no sudo")]);
        let mut headers = HeaderMap::new();
        headers.insert(IMPERSONATE_USER, "alice".parse().unwrap());

        let err = effective_identity_from_headers(
            &authorizer,
            &AuthenticatedIdentity::client_cert("bob".to_string(), vec![]),
            &headers,
        )
        .await
        .expect_err("denied impersonation must fail");

        match err {
            AppError::Forbidden(reason) => assert_eq!(reason, "no sudo"),
            other => panic!("expected forbidden, got {other:?}"),
        }
    }
}
