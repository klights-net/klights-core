//! K8s auth subsystem: certificate generation, kubeconfig, SA tokens, user identity.
//!
//! This module provides authentication and authorization utilities for klights:
//! - CA and server/client certificate initialization
//! - Kubeconfig generation
//! - ServiceAccount JWT token generation
//! - User identity extraction from client certificates

#[cfg(test)]
pub(crate) fn test_admin(username: impl Into<String>) -> klights_auth::AuthenticatedIdentity {
    klights_auth::AuthenticatedIdentity::client_cert(
        username.into(),
        vec!["system:masters".to_string()],
    )
}
pub(crate) use self::kubeconfig::{KubeconfigParams, generate_kubeconfig};
pub use self::middleware::validate_sa_token_bindings;
#[cfg(test)]
use klights_auth::{
    BoundServiceAccountToken, SaTokenClaims, ServiceAccountTokenRequest,
    decode_serviceaccount_token_with_clock, generate_sa_token_with_bound_pod_and_clock,
};
#[cfg(test)]
pub fn decode_serviceaccount_token(
    token: &str,
    ca_key_pem: &str,
    requested_audiences: Option<&[String]>,
) -> Result<SaTokenClaims, String> {
    decode_serviceaccount_token_with_clock(
        token,
        ca_key_pem,
        requested_audiences,
        &klights_auth::clock::SystemClock,
    )
}
#[cfg(test)]
pub fn generate_sa_token(
    ca_key_pem: &str,
    service_account: &str,
    namespace: &str,
    audiences: &[&str],
) -> anyhow::Result<String> {
    generate_sa_token_with_bound_pod(ServiceAccountTokenRequest {
        ca_key_pem,
        service_account,
        namespace,
        audiences,
        expiration_seconds: None,
        bound: BoundServiceAccountToken::default(),
    })
}
#[cfg(test)]
pub fn generate_sa_token_with_sa_uid(
    ca_key_pem: &str,
    service_account: &str,
    namespace: &str,
    audiences: &[&str],
    expiration_seconds: i64,
    sa_uid: &str,
) -> anyhow::Result<String> {
    generate_sa_token_with_bound_pod(ServiceAccountTokenRequest {
        ca_key_pem,
        service_account,
        namespace,
        audiences,
        expiration_seconds: Some(expiration_seconds),
        bound: BoundServiceAccountToken {
            sa_uid: Some(sa_uid),
            ..BoundServiceAccountToken::default()
        },
    })
}
#[cfg(test)]
pub fn generate_sa_token_with_bound_pod(
    request: ServiceAccountTokenRequest<'_>,
) -> anyhow::Result<String> {
    generate_sa_token_with_bound_pod_and_clock(request, &klights_auth::clock::SystemClock)
}

mod kubeconfig;
pub(crate) mod middleware;

#[cfg(test)]
mod object_safety_tests {
    fn assert_object_safe<T: ?Sized>() {}

    #[test]
    fn every_substitutable_auth_port_is_object_safe() {
        assert_object_safe::<dyn klights_auth::authorizer::Authorizer>();
        assert_object_safe::<dyn klights_auth::clock::Clock>();
        assert_object_safe::<dyn klights_auth::clock::MonotonicClock>();
        assert_object_safe::<dyn klights_auth::csr_signer::CsrSigner>();
        assert_object_safe::<dyn klights_auth::cluster_identity::BootstrapTokenAuthenticator>();
        assert_object_safe::<dyn klights_auth::cluster_identity::BoundTokenSubjectLookup>();
        assert_object_safe::<dyn klights_auth::cluster_identity::ServiceAccountSigningKeyProvider>(
        );
        assert_object_safe::<dyn klights_auth::node_policy_store::NodePolicyStore>();
        assert_object_safe::<dyn klights_auth::oidc::OidcDiscovery>();
        assert_object_safe::<dyn klights_auth::oidc::OidcValidator>();
        assert_object_safe::<dyn klights_auth::rbac_policy_store::RbacPolicyStore>();
        assert_object_safe::<dyn klights_auth::rbac_policy_store::RbacResourceReader>();
        assert_object_safe::<dyn klights_auth::webhook_auth::WebhookAuthenticator>();
        assert_object_safe::<dyn klights_auth::webhook_auth::WebhookTokenReviewer>();
    }
}
