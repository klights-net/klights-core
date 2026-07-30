//! Focused cluster identity and signing-state capabilities.
//!
//! Authentication policy consumes these effects through injected ports. Durable
//! storage and concrete cluster-resource adapters remain composition-owned.

use crate::{AuthenticatedIdentity, AuthenticationError};

#[async_trait::async_trait]
pub trait BootstrapTokenAuthenticator: Send + Sync {
    async fn authenticate_bootstrap_token(
        &self,
        token: &str,
    ) -> Result<AuthenticatedIdentity, AuthenticationError>;
}

#[async_trait::async_trait]
pub trait ServiceAccountSigningKeyProvider: Send + Sync {
    async fn service_account_signing_key_pem(&self) -> Result<String, AuthenticationError>;
}

#[async_trait::async_trait]
pub trait BoundTokenSubjectLookup: Send + Sync {
    async fn service_account_uid(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<String>, AuthenticationError>;
    async fn pod_uid(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<String>, AuthenticationError>;
    async fn node_uid(&self, name: &str) -> Result<Option<String>, AuthenticationError>;
    async fn secret_uid(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<String>, AuthenticationError>;
}
