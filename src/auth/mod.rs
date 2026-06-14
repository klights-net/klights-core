//! K8s auth subsystem: certificate generation, kubeconfig, SA tokens, user identity.
//!
//! This module provides authentication and authorization utilities for klights:
//! - CA and server/client certificate initialization
//! - Kubeconfig generation
//! - ServiceAccount JWT token generation
//! - User identity extraction from client certificates

pub use self::cert::{
    API_PROXY_COMMON_NAME_PREFIX, APISERVICE_PROXY_COMMON_NAME, APISERVICE_PROXY_GROUP,
    CONTROLPLANE_NODES_GROUP, CertInitResult, CertPaths, InitCertificateRequest, NODES_GROUP,
    PendingCsr, api_proxy_common_name, generate_api_proxy_cert, generate_apiservice_proxy_cert,
    generate_server_csr, init_certificates,
};
#[cfg(test)]
pub use self::cert::{generate_admin_cert, generate_ca_full, generate_server_cert};
pub use self::identity::AuthenticatedIdentity;
pub use self::middleware::{
    FORWARDED_CLIENT_CERT_HEADER, TlsClientCertificate, authenticate_request, authorize_request,
    validate_sa_token_bindings,
};
pub use self::token::generate_sa_token;
pub use self::token::generate_sa_token_with_sa_uid;
pub use self::token::persist_service_account_signing_key;
pub use self::token::read_service_account_signing_key;
pub use self::token::read_service_account_signing_key_async;
pub use self::token::read_service_account_signing_key_supervised;
pub use self::token::{
    BoundServiceAccountToken, DEFAULT_SERVICE_ACCOUNT_TOKEN_EXPIRATION_SECONDS,
    MIN_SERVICE_ACCOUNT_TOKEN_EXPIRATION_SECONDS, ServiceAccountTokenRequest,
    generate_sa_token_with_bound_pod, normalize_service_account_token_expiration_seconds,
};
pub use self::token::{
    SaTokenClaims, decode_serviceaccount_token, serviceaccount_groups_from_claims,
    serviceaccount_uid_from_claims, validate_service_account_uid,
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
pub mod node_authorizer;
pub mod node_policy_store;
//mod token;
pub mod default_rbac;
mod default_roles;
mod middleware;
pub mod oidc;
#[cfg(test)]
mod oidc_tests;
pub mod rbac_authorizer;
pub mod rbac_policy_store;
pub mod rbac_rule_evaluator;
pub mod request_attributes;
pub mod request_info;
mod token;
mod user;
pub mod webhook_auth;
#[cfg(test)]
mod webhook_auth_tests;
