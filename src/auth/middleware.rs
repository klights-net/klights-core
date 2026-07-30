//! Transport-neutral request authentication policy.
//!
//! The API layer extracts HTTP credentials and adapts failures. This module
//! consumes only focused authentication capabilities and scalar policy input.

use klights_auth::AuthenticatedIdentity;
use klights_auth::clock::Clock;
use klights_auth::cluster_identity::{
    BootstrapTokenAuthenticator, BoundTokenSubjectLookup, ServiceAccountSigningKeyProvider,
};
use klights_auth::{AuthenticationError, validate_bound_object_uid};
use klights_supervisor::{TaskCategory, TaskSupervisor};
use klights_types::TlsClientCertificate;

pub(crate) struct AuthnRuntime<'a> {
    bootstrap_tokens: &'a dyn BootstrapTokenAuthenticator,
    service_account_signing_keys: &'a dyn ServiceAccountSigningKeyProvider,
    bound_token_subjects: &'a dyn BoundTokenSubjectLookup,
    oidc_authenticator: Option<&'a dyn klights_auth::oidc::OidcValidator>,
    webhook_authenticator: Option<&'a dyn klights_auth::webhook_auth::WebhookAuthenticator>,
    clock: &'a dyn Clock,
    task_supervisor: &'a TaskSupervisor,
    anonymous_auth: bool,
}

impl<'a> AuthnRuntime<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        bootstrap_tokens: &'a dyn BootstrapTokenAuthenticator,
        service_account_signing_keys: &'a dyn ServiceAccountSigningKeyProvider,
        bound_token_subjects: &'a dyn BoundTokenSubjectLookup,
        oidc_authenticator: Option<&'a dyn klights_auth::oidc::OidcValidator>,
        webhook_authenticator: Option<&'a dyn klights_auth::webhook_auth::WebhookAuthenticator>,
        clock: &'a dyn Clock,
        task_supervisor: &'a TaskSupervisor,
        anonymous_auth: bool,
    ) -> Self {
        Self {
            bootstrap_tokens,
            service_account_signing_keys,
            bound_token_subjects,
            oidc_authenticator,
            webhook_authenticator,
            clock,
            task_supervisor,
            anonymous_auth,
        }
    }
}

pub(crate) async fn authenticate_parts(
    runtime: &AuthnRuntime<'_>,
    extension_user: Option<AuthenticatedIdentity>,
    client_cert: Option<TlsClientCertificate>,
    authorization: Option<String>,
) -> Result<Option<AuthenticatedIdentity>, AuthenticationError> {
    if let Some(user) = extension_user {
        return Ok(Some(user));
    }

    if let Some(cert) = client_cert {
        let user = runtime
            .task_supervisor
            .run_blocking(
                TaskCategory::Others,
                "authenticate-client-certificate",
                move || klights_auth::user::user_from_cert(&cert.0),
            )
            .await
            .map_err(|error| {
                AuthenticationError::internal_failure(format!(
                    "client certificate authentication failed: {error}"
                ))
            })?
            .map_err(|error| {
                AuthenticationError::unauthenticated(format!("invalid client certificate: {error}"))
            })?;
        return Ok(Some(AuthenticatedIdentity::client_cert(
            user.username,
            user.groups,
        )));
    }

    let Some(raw) = authorization else {
        return Ok(None);
    };
    let Some(token) = raw.strip_prefix("Bearer ") else {
        return Err(AuthenticationError::unauthenticated(
            "unsupported Authorization scheme",
        ));
    };

    authenticate_bearer_token(runtime, token).await.map(Some)
}

pub(crate) fn resolve_request_identity(
    runtime: &AuthnRuntime<'_>,
    identity: Option<AuthenticatedIdentity>,
) -> Result<AuthenticatedIdentity, AuthenticationError> {
    match identity {
        Some(identity) => Ok(identity),
        None if runtime.anonymous_auth => Ok(AuthenticatedIdentity::anonymous()),
        None => Err(AuthenticationError::unauthenticated(
            "anonymous authentication is disabled",
        )),
    }
}

async fn authenticate_bearer_token(
    runtime: &AuthnRuntime<'_>,
    token: &str,
) -> Result<AuthenticatedIdentity, AuthenticationError> {
    match token.split('.').count() {
        2 => match runtime
            .bootstrap_tokens
            .authenticate_bootstrap_token(token)
            .await
        {
            Ok(identity) => Ok(identity),
            Err(bootstrap_error) => {
                if let Some(result) = klights_auth::webhook_auth::try_webhook_auth(
                    runtime.webhook_authenticator,
                    token,
                )
                .await
                {
                    return result
                        .map_err(|error| preferred_authentication_error(bootstrap_error, error));
                }
                Err(bootstrap_error)
            }
        },
        3 => match validate_sa_token(runtime, token).await {
            Ok(identity) => Ok(identity),
            Err(service_account_error) => {
                if let Some(result) = klights_auth::oidc::try_oidc_auth(
                    runtime.oidc_authenticator,
                    token,
                    runtime.clock,
                )
                .await
                {
                    match result {
                        Ok(identity) => return Ok(identity),
                        Err(error) => {
                            let selected =
                                preferred_authentication_error(service_account_error, error);
                            if let Some(result) = klights_auth::webhook_auth::try_webhook_auth(
                                runtime.webhook_authenticator,
                                token,
                            )
                            .await
                            {
                                return result.map_err(|error| {
                                    preferred_authentication_error(selected, error)
                                });
                            }
                            return Err(selected);
                        }
                    }
                }
                if let Some(result) = klights_auth::webhook_auth::try_webhook_auth(
                    runtime.webhook_authenticator,
                    token,
                )
                .await
                {
                    return result.map_err(|error| {
                        preferred_authentication_error(service_account_error, error)
                    });
                }
                Err(service_account_error)
            }
        },
        _ => {
            if let Some(result) =
                klights_auth::webhook_auth::try_webhook_auth(runtime.webhook_authenticator, token)
                    .await
            {
                return result;
            }
            Err(AuthenticationError::unauthenticated("invalid bearer token"))
        }
    }
}

fn preferred_authentication_error(
    current: AuthenticationError,
    candidate: AuthenticationError,
) -> AuthenticationError {
    fn priority(error: &AuthenticationError) -> u8 {
        match error {
            AuthenticationError::Unauthenticated { .. } => 0,
            AuthenticationError::DependencyFailure { .. } => 1,
            AuthenticationError::InternalFailure { .. } => 2,
        }
    }
    if priority(&candidate) > priority(&current) {
        candidate
    } else {
        current
    }
}

async fn validate_sa_token(
    runtime: &AuthnRuntime<'_>,
    token: &str,
) -> Result<AuthenticatedIdentity, AuthenticationError> {
    let audiences = vec!["https://kubernetes.default.svc.cluster.local".to_string()];
    let claims = validate_sa_token_claims(runtime, token, &audiences).await?;
    Ok(service_account_identity(&claims))
}

async fn validate_sa_token_claims(
    runtime: &AuthnRuntime<'_>,
    token: &str,
    audiences: &[String],
) -> Result<klights_auth::SaTokenClaims, AuthenticationError> {
    let signing_key_pem = runtime
        .service_account_signing_keys
        .service_account_signing_key_pem()
        .await?;
    let audiences = audiences.to_vec();
    let token = token.to_string();
    let now = runtime.clock.now();
    let task_supervisor: &klights_supervisor::TaskSupervisor = runtime.task_supervisor;
    let claims = task_supervisor
        .run_blocking(
            TaskCategory::Others,
            "decode-service-account-token",
            move || {
                struct SnapshotClock(time::OffsetDateTime);
                impl Clock for SnapshotClock {
                    fn now(&self) -> time::OffsetDateTime {
                        self.0
                    }
                }
                klights_auth::decode_serviceaccount_token_with_clock(
                    &token,
                    &signing_key_pem,
                    Some(&audiences),
                    &SnapshotClock(now),
                )
            },
        )
        .await
        .map_err(|error| {
            AuthenticationError::internal_failure(format!(
                "serviceaccount token verification failed: {error}"
            ))
        })?
        .map_err(|error| {
            AuthenticationError::unauthenticated(format!("invalid serviceaccount token: {error}"))
        })?;

    validate_sa_token_bindings(runtime.bound_token_subjects, &claims).await?;

    Ok(claims)
}

fn service_account_identity(claims: &klights_auth::SaTokenClaims) -> AuthenticatedIdentity {
    AuthenticatedIdentity::service_account(
        claims.sub.clone(),
        klights_auth::serviceaccount_groups_from_claims(claims),
        klights_auth::serviceaccount_uid_from_claims(claims),
    )
}

pub(crate) enum ReviewedTokenIdentity {
    ServiceAccount {
        claims: klights_auth::SaTokenClaims,
        audiences: Vec<String>,
    },
    Other {
        identity: AuthenticatedIdentity,
        audiences: Vec<String>,
    },
}

pub(crate) async fn authenticate_token_for_review(
    runtime: &AuthnRuntime<'_>,
    token: &str,
    audiences: &[String],
) -> Result<ReviewedTokenIdentity, AuthenticationError> {
    const DEFAULT_API_AUDIENCE: &str = "https://kubernetes.default.svc.cluster.local";
    let implicit_audiences;
    let validation_audiences = if audiences.is_empty() {
        implicit_audiences = vec![DEFAULT_API_AUDIENCE.to_string()];
        implicit_audiences.as_slice()
    } else {
        audiences
    };

    fn intersect_requested_audiences(
        requested: &[String],
        validated: &[String],
        authenticator: &str,
    ) -> Result<Vec<String>, AuthenticationError> {
        if requested.is_empty() {
            return Ok(Vec::new());
        }
        let intersection = requested
            .iter()
            .filter(|audience| validated.contains(audience))
            .cloned()
            .collect::<Vec<_>>();
        if intersection.is_empty() {
            Err(AuthenticationError::unauthenticated(format!(
                "{authenticator} audiences do not intersect TokenReview audiences"
            )))
        } else {
            Ok(intersection)
        }
    }

    match token.split('.').count() {
        2 => {
            let bootstrap_result = runtime
                .bootstrap_tokens
                .authenticate_bootstrap_token(token)
                .await
                .and_then(|identity| {
                    intersect_requested_audiences(
                        audiences,
                        &[DEFAULT_API_AUDIENCE.to_string()],
                        "bootstrap token",
                    )
                    .map(|audiences| ReviewedTokenIdentity::Other {
                        identity,
                        audiences,
                    })
                });
            match bootstrap_result {
                Ok(reviewed) => Ok(reviewed),
                Err(bootstrap_error) => {
                    if let Some(result) = klights_auth::webhook_auth::try_webhook_auth_for_review(
                        runtime.webhook_authenticator,
                        token,
                        audiences,
                    )
                    .await
                    {
                        return result
                            .map(
                                |(identity, validated_audiences)| ReviewedTokenIdentity::Other {
                                    identity,
                                    audiences: if audiences.is_empty() {
                                        Vec::new()
                                    } else {
                                        validated_audiences
                                    },
                                },
                            )
                            .map_err(|error| {
                                preferred_authentication_error(bootstrap_error, error)
                            });
                    }
                    Err(bootstrap_error)
                }
            }
        }
        3 => match validate_sa_token_claims(runtime, token, validation_audiences).await {
            Ok(claims) => {
                let validated_audiences =
                    intersect_requested_audiences(audiences, &claims.aud, "service-account token")?;
                Ok(ReviewedTokenIdentity::ServiceAccount {
                    claims,
                    audiences: validated_audiences,
                })
            }
            Err(service_account_error) => {
                let mut selected = service_account_error;
                if let Some(result) = klights_auth::oidc::try_oidc_auth_for_review(
                    runtime.oidc_authenticator,
                    token,
                    runtime.clock,
                    audiences,
                )
                .await
                {
                    match result {
                        Ok((identity, validated_audiences)) => {
                            return Ok(ReviewedTokenIdentity::Other {
                                identity,
                                audiences: if audiences.is_empty() {
                                    Vec::new()
                                } else {
                                    validated_audiences
                                },
                            });
                        }
                        Err(error) => {
                            selected = preferred_authentication_error(selected, error);
                        }
                    }
                }
                if let Some(result) = klights_auth::webhook_auth::try_webhook_auth_for_review(
                    runtime.webhook_authenticator,
                    token,
                    audiences,
                )
                .await
                {
                    return result
                        .map(
                            |(identity, validated_audiences)| ReviewedTokenIdentity::Other {
                                identity,
                                audiences: if audiences.is_empty() {
                                    Vec::new()
                                } else {
                                    validated_audiences
                                },
                            },
                        )
                        .map_err(|error| preferred_authentication_error(selected, error));
                }
                Err(selected)
            }
        },
        _ => {
            if let Some(result) = klights_auth::webhook_auth::try_webhook_auth_for_review(
                runtime.webhook_authenticator,
                token,
                audiences,
            )
            .await
            {
                return result.map(|(identity, validated_audiences)| {
                    ReviewedTokenIdentity::Other {
                        identity,
                        audiences: if audiences.is_empty() {
                            Vec::new()
                        } else {
                            validated_audiences
                        },
                    }
                });
            }
            Err(AuthenticationError::unauthenticated("invalid bearer token"))
        }
    }
}

/// Validate that a decoded ServiceAccount token's bound subjects still exist
/// with matching UIDs.
pub async fn validate_sa_token_bindings(
    subjects: &dyn BoundTokenSubjectLookup,
    claims: &klights_auth::SaTokenClaims,
) -> Result<(), AuthenticationError> {
    let Some((namespace, service_account_name)) = claims
        .sub
        .strip_prefix("system:serviceaccount:")
        .and_then(|rest| rest.split_once(':'))
    else {
        return Ok(());
    };

    if let Some(token_uid) = klights_auth::serviceaccount_uid_from_claims(claims) {
        let stored_uid = subjects
            .service_account_uid(namespace, service_account_name)
            .await?;
        klights_auth::validate_service_account_uid(Some(&token_uid), stored_uid.as_deref())
            .map_err(|error| {
                AuthenticationError::unauthenticated(format!(
                    "invalid serviceaccount token UID: {error}"
                ))
            })?;
    }

    if let Some(kubernetes) = claims.kubernetes_io.as_ref() {
        if let Some(pod) = kubernetes.pod.as_ref()
            && let Some(name) = pod.name.as_deref().filter(|name| !name.is_empty())
        {
            let stored_uid = subjects.pod_uid(namespace, name).await?;
            validate_bound_object_uid("pod", name, pod.uid.as_deref(), stored_uid.as_deref())?;
        }
        if let Some(node) = kubernetes.node.as_ref()
            && let Some(name) = node.name.as_deref().filter(|name| !name.is_empty())
        {
            let stored_uid = subjects.node_uid(name).await?;
            validate_bound_object_uid("node", name, node.uid.as_deref(), stored_uid.as_deref())?;
        }
        if let Some(secret) = kubernetes.secret.as_ref()
            && let Some(name) = secret.name.as_deref().filter(|name| !name.is_empty())
        {
            let stored_uid = subjects.secret_uid(namespace, name).await?;
            validate_bound_object_uid(
                "secret",
                name,
                secret.uid.as_deref(),
                stored_uid.as_deref(),
            )?;
        }
    }

    Ok(())
}

/// Re-authenticate a forwarded client certificate against explicitly supplied
/// cluster trust material.
pub(crate) async fn authenticate_forwarded_client_cert(
    cluster_ca_pem: Option<&str>,
    cert_der: &[u8],
    task_supervisor: &TaskSupervisor,
) -> Result<AuthenticatedIdentity, AuthenticationError> {
    let ca_pem = cluster_ca_pem
        .ok_or_else(|| {
            AuthenticationError::dependency_failure(
                "no cluster CA configured to verify forwarded client certificate",
            )
        })?
        .to_string();
    let cert_der = cert_der.to_vec();
    let user = task_supervisor
        .run_blocking(
            TaskCategory::Others,
            "authenticate-forwarded-client-certificate",
            move || klights_auth::user::verify_client_cert_signed_by_ca(&cert_der, &ca_pem),
        )
        .await
        .map_err(|error| {
            AuthenticationError::internal_failure(format!(
                "forwarded certificate authentication failed: {error}"
            ))
        })?
        .map_err(|error| {
            AuthenticationError::unauthenticated(format!(
                "invalid forwarded client certificate: {error}"
            ))
        })?;
    Ok(AuthenticatedIdentity::client_cert(
        user.username,
        user.groups,
    ))
}

/// A connection may delegate identity only when its presented client
/// certificate is an internal API proxy certificate.
pub(crate) async fn client_cert_is_trusted_proxy(
    client_cert: Option<&TlsClientCertificate>,
    task_supervisor: &TaskSupervisor,
) -> Result<bool, AuthenticationError> {
    let Some(cert) = client_cert else {
        return Ok(false);
    };
    let cert = cert.clone();
    let user = task_supervisor
        .run_blocking(
            TaskCategory::Others,
            "classify-api-proxy-certificate",
            move || klights_auth::user::user_from_cert(&cert.0),
        )
        .await
        .map_err(|error| {
            AuthenticationError::internal_failure(format!(
                "proxy certificate classification failed: {error}"
            ))
        })?;
    let Ok(user) = user else {
        return Ok(false);
    };
    Ok(is_trusted_api_proxy_identity(
        &AuthenticatedIdentity::client_cert(user.username, user.groups),
    ))
}

fn is_trusted_api_proxy_identity(identity: &AuthenticatedIdentity) -> bool {
    let Some(node_name) = identity
        .username
        .strip_prefix(klights_auth::cert::API_PROXY_COMMON_NAME_PREFIX)
    else {
        return false;
    };
    !node_name.is_empty()
        && !identity
            .groups
            .iter()
            .any(|group| group == "system:masters")
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use klights_auth::clock::{FixedClock, SystemMonotonicClock};
    use klights_auth::webhook_auth::{
        TokenReviewStatus, TokenReviewUser, WebhookAuth, WebhookTokenReviewer,
    };

    struct RejectBootstrap;
    struct AcceptBootstrap;
    struct RejectSigningKey;
    struct StaticSigningKey(String);
    struct StaticWebhookReviewer {
        status: TokenReviewStatus,
    }

    #[derive(Default)]
    struct Subjects {
        service_accounts: HashMap<(String, String), String>,
        pods: HashMap<(String, String), String>,
        nodes: HashMap<String, String>,
        secrets: HashMap<(String, String), String>,
        failure: Option<AuthenticationError>,
    }

    #[async_trait::async_trait]
    impl BootstrapTokenAuthenticator for RejectBootstrap {
        async fn authenticate_bootstrap_token(
            &self,
            _token: &str,
        ) -> Result<AuthenticatedIdentity, AuthenticationError> {
            Err(AuthenticationError::unauthenticated(
                "not a bootstrap token",
            ))
        }
    }

    #[async_trait::async_trait]
    impl BootstrapTokenAuthenticator for AcceptBootstrap {
        async fn authenticate_bootstrap_token(
            &self,
            _token: &str,
        ) -> Result<AuthenticatedIdentity, AuthenticationError> {
            Ok(AuthenticatedIdentity::bootstrap("abcdef", &[]))
        }
    }

    #[async_trait::async_trait]
    impl ServiceAccountSigningKeyProvider for RejectSigningKey {
        async fn service_account_signing_key_pem(&self) -> Result<String, AuthenticationError> {
            Err(AuthenticationError::dependency_failure(
                "signing key unavailable",
            ))
        }
    }

    #[async_trait::async_trait]
    impl ServiceAccountSigningKeyProvider for StaticSigningKey {
        async fn service_account_signing_key_pem(&self) -> Result<String, AuthenticationError> {
            Ok(self.0.clone())
        }
    }

    #[async_trait::async_trait]
    impl WebhookTokenReviewer for StaticWebhookReviewer {
        async fn review_token(
            &self,
            _token: &str,
            _audiences: &[String],
        ) -> Result<Option<TokenReviewStatus>, AuthenticationError> {
            Ok(Some(self.status.clone()))
        }
    }

    #[async_trait::async_trait]
    impl BoundTokenSubjectLookup for Subjects {
        async fn service_account_uid(
            &self,
            namespace: &str,
            name: &str,
        ) -> Result<Option<String>, AuthenticationError> {
            if let Some(error) = &self.failure {
                return Err(error.clone());
            }
            Ok(self
                .service_accounts
                .get(&(namespace.to_string(), name.to_string()))
                .cloned())
        }

        async fn pod_uid(
            &self,
            namespace: &str,
            name: &str,
        ) -> Result<Option<String>, AuthenticationError> {
            if let Some(error) = &self.failure {
                return Err(error.clone());
            }
            Ok(self
                .pods
                .get(&(namespace.to_string(), name.to_string()))
                .cloned())
        }

        async fn node_uid(&self, name: &str) -> Result<Option<String>, AuthenticationError> {
            if let Some(error) = &self.failure {
                return Err(error.clone());
            }
            Ok(self.nodes.get(name).cloned())
        }

        async fn secret_uid(
            &self,
            namespace: &str,
            name: &str,
        ) -> Result<Option<String>, AuthenticationError> {
            if let Some(error) = &self.failure {
                return Err(error.clone());
            }
            Ok(self
                .secrets
                .get(&(namespace.to_string(), name.to_string()))
                .cloned())
        }
    }

    #[tokio::test]
    async fn service_account_authentication_uses_injected_signer_without_host_state() {
        let signing_key = klights_auth::cert::generate_ca_full_at(time::OffsetDateTime::now_utc())
            .unwrap()
            .3;
        let token = crate::auth::generate_sa_token_with_sa_uid(
            &signing_key,
            "default",
            "default",
            &["https://kubernetes.default.svc.cluster.local"],
            3600,
            "sa-uid",
        )
        .unwrap();
        let mut subjects = Subjects::default();
        subjects.service_accounts.insert(
            ("default".to_string(), "default".to_string()),
            "sa-uid".to_string(),
        );
        let supervisor = TaskSupervisor::new(Default::default());
        let signing_keys = StaticSigningKey(signing_key);
        let runtime = AuthnRuntime::new(
            &RejectBootstrap,
            &signing_keys,
            &subjects,
            None,
            None,
            &klights_auth::clock::SystemClock,
            &supervisor,
            false,
        );

        let identity = authenticate_bearer_token(&runtime, &token)
            .await
            .expect("injected signer must authenticate a ServiceAccount token");

        assert_eq!(identity.username, "system:serviceaccount:default:default");
        supervisor.shutdown(Duration::from_secs(1)).await;
    }

    fn claims(value: serde_json::Value) -> klights_auth::SaTokenClaims {
        serde_json::from_value(value).expect("valid service account claims")
    }

    #[tokio::test]
    async fn bound_subject_validation_is_store_port_driven() {
        let mut subjects = Subjects::default();
        subjects.service_accounts.insert(
            ("default".to_string(), "app".to_string()),
            "sa-uid".to_string(),
        );
        subjects.pods.insert(
            ("default".to_string(), "app-pod".to_string()),
            "pod-uid".to_string(),
        );
        let matching = claims(serde_json::json!({
            "sub": "system:serviceaccount:default:app",
            "kubernetes.io": {
                "serviceaccount": {"uid": "sa-uid"},
                "pod": {"name": "app-pod", "uid": "pod-uid"}
            }
        }));
        assert!(
            validate_sa_token_bindings(&subjects, &matching)
                .await
                .is_ok()
        );

        let mismatched = claims(serde_json::json!({
            "sub": "system:serviceaccount:default:app",
            "kubernetes.io": {
                "serviceaccount": {"uid": "sa-uid"},
                "pod": {"name": "app-pod", "uid": "replacement-uid"}
            }
        }));
        let error = validate_sa_token_bindings(&subjects, &mismatched)
            .await
            .expect_err("same-name replacement must invalidate a bound token");
        assert!(error.to_string().contains("UID mismatch"));

        subjects.pods.clear();
        let error = validate_sa_token_bindings(&subjects, &matching)
            .await
            .expect_err("deleted bound pod must invalidate a token");
        assert!(error.to_string().contains("no longer exists"));
    }

    #[tokio::test]
    async fn bound_subject_dependency_failure_is_not_credential_rejection() {
        let subjects = Subjects {
            failure: Some(AuthenticationError::dependency_failure(
                "subject store unavailable",
            )),
            ..Default::default()
        };
        let token_claims = claims(serde_json::json!({
            "sub": "system:serviceaccount:default:app",
            "kubernetes.io": {
                "serviceaccount": {"uid": "sa-uid"}
            }
        }));
        let error = validate_sa_token_bindings(&subjects, &token_claims)
            .await
            .expect_err("dependency failure must propagate");
        assert!(matches!(
            error,
            AuthenticationError::DependencyFailure { .. }
        ));
    }

    #[tokio::test]
    async fn opaque_bearer_token_falls_back_to_injected_webhook() {
        let subjects = Subjects::default();
        let webhook = WebhookAuth::new(
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
            Arc::new(SystemMonotonicClock),
        );
        let clock = FixedClock {
            now: time::OffsetDateTime::UNIX_EPOCH,
        };
        let supervisor = TaskSupervisor::new(Default::default());
        let runtime = AuthnRuntime::new(
            &RejectBootstrap,
            &RejectSigningKey,
            &subjects,
            None,
            Some(&webhook),
            &clock,
            &supervisor,
            false,
        );

        let identity = authenticate_bearer_token(&runtime, "opaque-token")
            .await
            .expect("opaque token should authenticate through webhook fallback");

        assert_eq!(identity.username, "opaque-user");
        assert_eq!(identity.uid, Some("opaque-uid".to_string()));
        assert!(identity.groups.iter().any(|group| group == "opaque-group"));
    }

    #[tokio::test]
    async fn tokenreview_bootstrap_audiences_are_intersected_with_default_api_audience() {
        let subjects = Subjects::default();
        let clock = FixedClock {
            now: time::OffsetDateTime::UNIX_EPOCH,
        };
        let supervisor = TaskSupervisor::new(Default::default());
        let runtime = AuthnRuntime::new(
            &AcceptBootstrap,
            &RejectSigningKey,
            &subjects,
            None,
            None,
            &clock,
            &supervisor,
            false,
        );
        let requested = vec![
            "https://kubernetes.default.svc.cluster.local".to_string(),
            "https://untrusted.example.test".to_string(),
        ];

        let reviewed =
            authenticate_token_for_review(&runtime, "abcdef.0123456789abcdef", &requested)
                .await
                .expect("bootstrap token should authenticate for the default API audience");
        let ReviewedTokenIdentity::Other {
            identity,
            audiences,
        } = reviewed
        else {
            panic!("bootstrap token must produce a non-service-account identity");
        };
        assert_eq!(identity.username, "system:bootstrap:abcdef");
        assert_eq!(
            audiences,
            vec!["https://kubernetes.default.svc.cluster.local".to_string()]
        );
    }

    #[tokio::test]
    async fn dependency_failure_is_not_overwritten_by_later_credential_rejection() {
        let subjects = Subjects::default();
        let webhook = WebhookAuth::new(
            Arc::new(StaticWebhookReviewer {
                status: TokenReviewStatus {
                    authenticated: false,
                    user: None,
                    error: None,
                    audiences: Vec::new(),
                },
            }),
            Duration::from_secs(60),
            Duration::from_secs(10),
            vec!["https://kubernetes.default.svc.cluster.local".to_string()],
            Arc::new(SystemMonotonicClock),
        );
        let clock = FixedClock {
            now: time::OffsetDateTime::UNIX_EPOCH,
        };
        let supervisor = TaskSupervisor::new(Default::default());
        let runtime = AuthnRuntime::new(
            &RejectBootstrap,
            &RejectSigningKey,
            &subjects,
            None,
            Some(&webhook),
            &clock,
            &supervisor,
            false,
        );

        let error = authenticate_bearer_token(&runtime, "a.b.c")
            .await
            .expect_err("all credential methods reject or fail");
        assert!(matches!(
            error,
            AuthenticationError::DependencyFailure { .. }
        ));
    }

    #[test]
    fn anonymous_policy_is_an_injected_scalar() {
        let subjects = Subjects::default();
        let clock = FixedClock {
            now: time::OffsetDateTime::UNIX_EPOCH,
        };
        let supervisor = TaskSupervisor::new(Default::default());
        let enabled = AuthnRuntime::new(
            &RejectBootstrap,
            &RejectSigningKey,
            &subjects,
            None,
            None,
            &clock,
            &supervisor,
            true,
        );
        assert_eq!(
            resolve_request_identity(&enabled, None)
                .expect("anonymous enabled")
                .username,
            "system:anonymous"
        );

        let disabled = AuthnRuntime::new(
            &RejectBootstrap,
            &RejectSigningKey,
            &subjects,
            None,
            None,
            &clock,
            &supervisor,
            false,
        );
        assert_eq!(
            resolve_request_identity(&disabled, None)
                .expect_err("anonymous disabled")
                .to_string(),
            "anonymous authentication is disabled"
        );
    }

    #[tokio::test]
    async fn forwarded_cert_requires_explicit_cluster_ca() {
        let (ca_cert, ca_key, _, _) =
            klights_auth::cert::generate_ca_full_at(time::OffsetDateTime::now_utc()).unwrap();
        let (admin_pem, _) = klights_auth::cert::generate_admin_cert_at(
            &ca_cert,
            &ca_key,
            time::OffsetDateTime::now_utc(),
        )
        .unwrap();
        let der = pem_to_der(&admin_pem);
        let supervisor = TaskSupervisor::new(Default::default());
        assert!(
            authenticate_forwarded_client_cert(None, &der, &supervisor)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn forwarded_admin_cert_preserves_system_masters() {
        let (ca_cert, ca_key, ca_pem, _) =
            klights_auth::cert::generate_ca_full_at(time::OffsetDateTime::now_utc()).unwrap();
        let (admin_pem, _) = klights_auth::cert::generate_admin_cert_at(
            &ca_cert,
            &ca_key,
            time::OffsetDateTime::now_utc(),
        )
        .unwrap();
        let supervisor = TaskSupervisor::new(Default::default());
        let identity =
            authenticate_forwarded_client_cert(Some(&ca_pem), &pem_to_der(&admin_pem), &supervisor)
                .await
                .expect("CA-signed admin certificate");
        assert_eq!(identity.username, "klights-admin");
        assert!(
            identity
                .groups
                .iter()
                .any(|group| group == "system:masters")
        );
    }

    #[tokio::test]
    async fn only_internal_proxy_certificate_can_delegate() {
        let (ca_cert, ca_key, _, _) =
            klights_auth::cert::generate_ca_full_at(time::OffsetDateTime::now_utc()).unwrap();
        let (proxy_pem, _) = klights_auth::cert::generate_api_proxy_cert(
            &ca_cert,
            &ca_key,
            "cp1",
            time::OffsetDateTime::now_utc(),
        )
        .unwrap();
        let supervisor = TaskSupervisor::new(Default::default());
        let proxy = TlsClientCertificate(pem_to_der(&proxy_pem));
        assert!(
            client_cert_is_trusted_proxy(Some(&proxy), &supervisor)
                .await
                .unwrap()
        );

        let (admin_pem, _) = klights_auth::cert::generate_admin_cert_at(
            &ca_cert,
            &ca_key,
            time::OffsetDateTime::now_utc(),
        )
        .unwrap();
        let admin = TlsClientCertificate(pem_to_der(&admin_pem));
        assert!(
            !client_cert_is_trusted_proxy(Some(&admin), &supervisor)
                .await
                .unwrap()
        );
        assert!(
            !client_cert_is_trusted_proxy(None, &supervisor)
                .await
                .unwrap()
        );
    }

    #[test]
    fn proxy_with_system_masters_cannot_delegate() {
        let elevated_proxy = AuthenticatedIdentity::client_cert(
            klights_auth::cert::api_proxy_common_name("cp1"),
            vec!["system:masters".to_string()],
        );
        assert!(!is_trusted_api_proxy_identity(&elevated_proxy));
    }

    fn pem_to_der(pem: &str) -> Vec<u8> {
        use x509_parser::pem::Pem;
        let (pem, _) = Pem::read(std::io::Cursor::new(pem.as_bytes())).unwrap();
        pem.contents
    }
}
