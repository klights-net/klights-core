//! K8s auth subsystem: certificate generation, kubeconfig, SA tokens, user identity.
//!
//! This module provides authentication and authorization utilities for klights:
//! - CA and server/client certificate initialization
//! - Kubeconfig generation
//! - ServiceAccount JWT token generation
//! - User identity extraction from client certificates

pub use self::cert::{
    API_PROXY_COMMON_NAME_PREFIX, APISERVICE_PROXY_GROUP, CONTROLPLANE_NODES_GROUP, NODES_GROUP,
    api_proxy_common_name, generate_api_proxy_cert, generate_apiservice_proxy_cert,
    generate_server_csr,
};
#[cfg(test)]
pub use self::cert::{generate_admin_cert, generate_ca_full, generate_server_cert};
#[cfg(test)]
pub(crate) fn test_admin(username: impl Into<String>) -> klights_auth::AuthenticatedIdentity {
    klights_auth::AuthenticatedIdentity::client_cert(
        username.into(),
        vec!["system:masters".to_string()],
    )
}
pub(crate) use self::kubeconfig::{KubeconfigParams, generate_kubeconfig};
pub use self::middleware::{BoundTokenSubjectLookup, validate_sa_token_bindings};
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
        &clock::SystemClock,
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
    generate_sa_token_with_bound_pod_and_clock(request, &clock::SystemClock)
}
pub use self::user::user_from_cert;
pub use self::user::verify_client_cert_signed_by_ca;

pub mod ca_transport;
pub(crate) mod cert;
pub mod clock;
pub mod csr_policy;
pub mod csr_signer;
mod kubeconfig;
pub mod kubelet_client_cert;
pub(crate) mod middleware;
pub mod oidc;
#[cfg(test)]
mod oidc_tests;
mod user;
pub mod webhook_auth;
#[cfg(test)]
mod webhook_auth_tests;

#[cfg(test)]
mod object_safety_tests {
    fn assert_object_safe<T: ?Sized>() {}

    #[test]
    fn every_substitutable_auth_port_is_object_safe() {
        assert_object_safe::<dyn klights_auth::authorizer::Authorizer>();
        assert_object_safe::<dyn super::clock::Clock>();
        assert_object_safe::<dyn super::clock::MonotonicClock>();
        assert_object_safe::<dyn super::csr_signer::CsrSigner>();
        assert_object_safe::<dyn super::middleware::BootstrapTokenAuthenticator>();
        assert_object_safe::<dyn super::middleware::BoundTokenSubjectLookup>();
        assert_object_safe::<dyn super::middleware::ServiceAccountSigningKeyProvider>();
        assert_object_safe::<dyn klights_auth::node_policy_store::NodePolicyStore>();
        assert_object_safe::<dyn super::oidc::OidcDiscovery>();
        assert_object_safe::<dyn super::oidc::OidcValidator>();
        assert_object_safe::<dyn klights_auth::rbac_policy_store::RbacPolicyStore>();
        assert_object_safe::<dyn klights_auth::rbac_policy_store::RbacResourceReader>();
        assert_object_safe::<dyn super::webhook_auth::WebhookAuthenticator>();
        assert_object_safe::<dyn super::webhook_auth::WebhookTokenReviewer>();
    }
}
