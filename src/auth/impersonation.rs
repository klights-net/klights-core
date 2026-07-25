//! Framework-neutral Kubernetes impersonation authorization.

use crate::auth::AuthError;
use crate::auth::authorizer::{AuthorizationDecision, Authorizer};
use crate::auth::identity::AuthenticatedIdentity;
use crate::auth::request_attributes::AuthorizationRequest;

const AUTHENTICATION_API_GROUP: &str = "authentication.k8s.io";
const SERVICEACCOUNT_PREFIX: &str = "system:serviceaccount:";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImpersonationRequest {
    pub username: String,
    pub groups: Vec<String>,
    pub uid: Option<String>,
    pub extra: Vec<(String, String)>,
}

pub async fn effective_identity(
    authorizer: &dyn Authorizer,
    real_identity: &AuthenticatedIdentity,
    request: Option<ImpersonationRequest>,
) -> Result<AuthenticatedIdentity, AuthError> {
    let Some(request) = request else {
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

async fn authorize_impersonation(
    authorizer: &dyn Authorizer,
    real_identity: &AuthenticatedIdentity,
    request: &ImpersonationRequest,
) -> Result<(), AuthError> {
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
) -> Result<(), AuthError> {
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
    Err(AuthError::Forbidden(impersonation_forbidden_message(
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
        let request = ImpersonationRequest {
            username: "system:serviceaccount:default:e2e".to_string(),
            groups: vec![
                "system:authenticated".to_string(),
                "system:serviceaccounts".to_string(),
                "system:serviceaccounts:default".to_string(),
            ],
            uid: Some("sa-uid-a".to_string()),
            extra: vec![("scopes".to_string(), "view".to_string())],
        };

        let identity = effective_identity(
            &authorizer,
            &AuthenticatedIdentity::admin("real-admin"),
            Some(request),
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

        let err = effective_identity(
            &authorizer,
            &AuthenticatedIdentity::client_cert("bob".to_string(), vec![]),
            Some(ImpersonationRequest {
                username: "alice".to_string(),
                groups: Vec::new(),
                uid: None,
                extra: Vec::new(),
            }),
        )
        .await
        .expect_err("denied impersonation must fail");

        match err {
            AuthError::Forbidden(reason) => assert_eq!(reason, "no sudo"),
            other => panic!("expected forbidden, got {other:?}"),
        }
    }
}
