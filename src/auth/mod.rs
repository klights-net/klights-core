//! K8s auth subsystem: certificate generation, kubeconfig, SA tokens, user identity.
//!
//! This module provides authentication and authorization utilities for klights:
//! - CA and server/client certificate initialization
//! - Kubeconfig generation
//! - ServiceAccount JWT token generation
//! - User identity extraction from client certificates

pub use self::cert::{
    API_PROXY_COMMON_NAME_PREFIX, APISERVICE_PROXY_GROUP, CONTROLPLANE_NODES_GROUP, CertInitResult,
    CertPaths, InitCertificateRequest, NODES_GROUP, PendingCsr, api_proxy_common_name,
    generate_api_proxy_cert, generate_apiservice_proxy_cert, generate_server_csr,
    init_certificates,
};
#[cfg(test)]
pub use self::cert::{generate_admin_cert, generate_ca_full, generate_server_cert};
pub use self::identity::AuthenticatedIdentity;
pub use self::middleware::{BoundTokenSubjectLookup, validate_sa_token_bindings};
pub use self::token::persist_service_account_signing_key;
pub use self::token::read_service_account_signing_key_async;
pub use self::token::read_service_account_signing_key_supervised;
pub use self::token::{
    BoundServiceAccountToken, ServiceAccountTokenRequest,
    generate_sa_token_with_bound_pod_and_clock, generate_sa_token_with_bound_pod_at,
};
pub use self::token::{
    SaTokenClaims, decode_serviceaccount_token_with_clock, serviceaccount_groups_from_claims,
    serviceaccount_uid_from_claims, validate_service_account_uid,
};
#[cfg(test)]
pub use self::token::{
    decode_serviceaccount_token, generate_sa_token, generate_sa_token_with_bound_pod,
    generate_sa_token_with_sa_uid,
};
pub use self::user::user_from_cert;
pub use self::user::verify_client_cert_signed_by_ca;

pub mod authorizer;
pub mod bootstrap_authorizer;
pub mod ca_transport;
mod cert;
pub mod clock;
pub mod csr_policy;
pub mod csr_signer;
pub mod identity;
pub mod impersonation;
mod kubeconfig;
pub mod kubelet_client_cert;
pub(crate) mod middleware;
pub mod node_authorizer;
pub mod node_policy_store;
pub mod oidc;
#[cfg(test)]
mod oidc_tests;
pub mod rbac_authorizer;
pub mod rbac_policy_store;
pub mod rbac_rule_evaluator;
pub mod request_attributes;
mod token;
mod user;
pub mod webhook_auth;
#[cfg(test)]
mod webhook_auth_tests;

#[cfg(test)]
mod object_safety_tests {
    fn assert_object_safe<T: ?Sized>() {}

    #[test]
    fn every_substitutable_auth_port_is_object_safe() {
        assert_object_safe::<dyn super::authorizer::Authorizer>();
        assert_object_safe::<dyn super::clock::Clock>();
        assert_object_safe::<dyn super::clock::MonotonicClock>();
        assert_object_safe::<dyn super::csr_signer::CsrSigner>();
        assert_object_safe::<dyn super::middleware::BootstrapTokenAuthenticator>();
        assert_object_safe::<dyn super::middleware::BoundTokenSubjectLookup>();
        assert_object_safe::<dyn super::middleware::ServiceAccountSigningKeyProvider>();
        assert_object_safe::<dyn super::node_policy_store::NodePolicyStore>();
        assert_object_safe::<dyn super::oidc::OidcDiscovery>();
        assert_object_safe::<dyn super::oidc::OidcValidator>();
        assert_object_safe::<dyn super::rbac_policy_store::RbacPolicyStore>();
        assert_object_safe::<dyn super::rbac_policy_store::RbacResourceReader>();
        assert_object_safe::<dyn super::webhook_auth::WebhookAuthenticator>();
        assert_object_safe::<dyn super::webhook_auth::WebhookTokenReviewer>();
    }
}
