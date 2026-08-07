//! Framework-neutral authentication and authorization contracts for klights.

use std::{error::Error, fmt};
pub mod authentication;
pub mod authorizer;
pub mod bootstrap_authorizer;
pub mod bootstrap_token;
pub mod ca_transport;
pub mod cert;
pub mod clock;
pub mod csr_policy;
pub mod csr_signer;
pub mod identity;
pub mod impersonation;
pub mod kubeconfig;
pub mod kubelet_client_cert;
pub mod node_authorizer;
pub mod node_policy_store;
pub mod oidc;
pub mod projected_service_account_token;
pub mod rbac_authorizer;
pub mod rbac_policy_store;
pub mod rbac_rule_evaluator;
pub mod request_attributes;
pub mod service_account;
pub mod user;
pub mod webhook_auth;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

#[cfg(test)]
mod oidc_tests;
#[cfg(test)]
mod webhook_auth_tests;

pub use authorizer::Authorizer;
pub use clock::{Clock, MonotonicClock};
pub use csr_signer::CsrSigner;
pub use identity::AuthenticatedIdentity;
pub use node_policy_store::NodePolicyStore;
pub use oidc::{OidcDiscovery, OidcValidator};
pub use rbac_policy_store::{RbacPolicyStore, RbacResourceReader};
pub use service_account::{
    BoundServiceAccountToken, SaKubernetesIoClaims, SaObjectClaims, SaServiceAccountClaims,
    SaTokenClaims, ServiceAccountTokenRequest, decode_serviceaccount_token_with_clock,
    generate_sa_token_with_bound_pod_and_clock, generate_sa_token_with_bound_pod_at,
    serviceaccount_groups_from_claims, serviceaccount_uid_from_claims,
    validate_service_account_uid,
};
pub use webhook_auth::{WebhookAuthenticator, WebhookTokenReviewer};

/// Authentication failed without selecting an HTTP status or response shape.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthenticationError {
    Unauthenticated { message: String },
    DependencyFailure { message: String },
    InternalFailure { message: String },
}

impl AuthenticationError {
    pub fn unauthenticated(message: impl Into<String>) -> Self {
        Self::Unauthenticated {
            message: message.into(),
        }
    }

    pub fn dependency_failure(message: impl Into<String>) -> Self {
        Self::DependencyFailure {
            message: message.into(),
        }
    }

    pub fn internal_failure(message: impl Into<String>) -> Self {
        Self::InternalFailure {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        match self {
            Self::Unauthenticated { message }
            | Self::DependencyFailure { message }
            | Self::InternalFailure { message } => message,
        }
    }
}

impl fmt::Display for AuthenticationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message())
    }
}

impl Error for AuthenticationError {}

/// Credential issuance or key handling failed without selecting a transport
/// status or consumer response shape.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CredentialOperationError {
    Rejected { message: String },
    DependencyFailure { message: String },
    InternalFailure { message: String },
}

impl CredentialOperationError {
    pub fn rejected(message: impl Into<String>) -> Self {
        Self::Rejected {
            message: message.into(),
        }
    }

    pub fn dependency_failure(message: impl Into<String>) -> Self {
        Self::DependencyFailure {
            message: message.into(),
        }
    }

    pub fn internal_failure(message: impl Into<String>) -> Self {
        Self::InternalFailure {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        match self {
            Self::Rejected { message }
            | Self::DependencyFailure { message }
            | Self::InternalFailure { message } => message,
        }
    }
}

impl fmt::Display for CredentialOperationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message())
    }
}

impl Error for CredentialOperationError {}

/// Framework-neutral kubelet certificate request consumed by auth policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KubeletCertificateRequest {
    pub signer_name: String,
    pub csr_pem: Vec<u8>,
    pub usages: Vec<String>,
    pub username: String,
    pub groups: Vec<String>,
    pub expiration_seconds: Option<u32>,
}

/// Framework-neutral result of kubelet certificate policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KubeletCertificateOutcome {
    Issued {
        node_name: String,
        certificate_pem: String,
        issued_at_unix_seconds: i64,
    },
    Rejected {
        reason: String,
    },
}

/// Framework-neutral identity extracted from an authenticated peer certificate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerCertificateIdentity {
    pub username: String,
    pub groups: Vec<String>,
}

/// Impersonation policy failed without selecting an HTTP response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ImpersonationError {
    InvalidRequest { message: String },
    Forbidden { message: String },
}

impl ImpersonationError {
    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            message: message.into(),
        }
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::Forbidden {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        match self {
            Self::InvalidRequest { message } | Self::Forbidden { message } => message,
        }
    }
}

impl fmt::Display for ImpersonationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message())
    }
}

impl Error for ImpersonationError {}

/// Validate the existence and identity of an object bound into a projected
/// ServiceAccount token.
pub fn validate_bound_object_uid(
    kind: &str,
    name: &str,
    token_uid: Option<&str>,
    stored_uid: Option<&str>,
) -> Result<(), AuthenticationError> {
    let Some(stored_uid) = stored_uid else {
        return Err(AuthenticationError::unauthenticated(format!(
            "serviceaccount token bound {kind} \"{name}\" no longer exists"
        )));
    };
    if token_uid.is_some_and(|token_uid| token_uid != stored_uid) {
        return Err(AuthenticationError::unauthenticated(format!(
            "serviceaccount token bound {kind} \"{name}\" UID mismatch"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn neutral_errors_preserve_policy_classification_and_message() {
        let cases = [
            (
                AuthenticationError::unauthenticated("invalid token").to_string(),
                "invalid token",
            ),
            (
                AuthenticationError::dependency_failure("credential store unavailable").to_string(),
                "credential store unavailable",
            ),
            (
                AuthenticationError::internal_failure("verification worker failed").to_string(),
                "verification worker failed",
            ),
            (
                ImpersonationError::invalid_request("invalid header").to_string(),
                "invalid header",
            ),
            (
                ImpersonationError::forbidden("policy denied").to_string(),
                "policy denied",
            ),
        ];

        for (actual, expected) in cases {
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn bound_object_validation_is_uid_qualified_and_fail_closed() {
        let cases = [
            (Some("uid-a"), Some("uid-a"), true),
            (None, Some("uid-a"), true),
            (Some("uid-a"), Some("uid-b"), false),
            (Some("uid-a"), None, false),
        ];

        for (token_uid, stored_uid, accepted) in cases {
            assert_eq!(
                validate_bound_object_uid("pod", "example", token_uid, stored_uid).is_ok(),
                accepted,
                "token_uid={token_uid:?}, stored_uid={stored_uid:?}"
            );
        }
    }
}
