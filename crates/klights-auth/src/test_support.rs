//! Explicit opt-in test support for cross-crate integration tests.
//!
//! This module is absent from the normal production public API.

use anyhow::Result;
use rcgen::{Certificate, KeyPair};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use time::OffsetDateTime;
use tokio::sync::Notify;

/// Deterministic authorization fake for full-stack integration fixtures.
pub struct AllowAllAuthorizer;

#[async_trait::async_trait]
impl crate::authorizer::Authorizer for AllowAllAuthorizer {
    async fn authorize(
        &self,
        _identity: &crate::AuthenticatedIdentity,
        _request: &crate::request_attributes::AuthorizationRequest,
    ) -> crate::authorizer::AuthorizationDecision {
        crate::authorizer::AuthorizationDecision::allow("integration harness allow-all")
    }
}

/// Authorizer fake that returns one fixed decision and records exact calls.
pub struct RecordingAuthorizer {
    calls: tokio::sync::Mutex<
        Vec<(
            crate::AuthenticatedIdentity,
            crate::request_attributes::AuthorizationRequest,
        )>,
    >,
    decision: crate::authorizer::AuthorizationDecision,
}

impl RecordingAuthorizer {
    pub fn allow() -> Self {
        Self::new(crate::authorizer::AuthorizationDecision::allow(
            "integration recording allow",
        ))
    }

    pub fn deny(reason: &str) -> Self {
        Self::new(crate::authorizer::AuthorizationDecision::deny(reason))
    }

    pub fn new(decision: crate::authorizer::AuthorizationDecision) -> Self {
        Self {
            calls: tokio::sync::Mutex::new(Vec::new()),
            decision,
        }
    }

    pub async fn take_calls(
        &self,
    ) -> Vec<(
        crate::AuthenticatedIdentity,
        crate::request_attributes::AuthorizationRequest,
    )> {
        std::mem::take(&mut *self.calls.lock().await)
    }

    pub async fn take_requests(&self) -> Vec<crate::request_attributes::AuthorizationRequest> {
        self.take_calls()
            .await
            .into_iter()
            .map(|(_, request)| request)
            .collect()
    }
}

#[async_trait::async_trait]
impl crate::authorizer::Authorizer for RecordingAuthorizer {
    async fn authorize(
        &self,
        identity: &crate::AuthenticatedIdentity,
        request: &crate::request_attributes::AuthorizationRequest,
    ) -> crate::authorizer::AuthorizationDecision {
        self.calls
            .lock()
            .await
            .push((identity.clone(), request.clone()));
        self.decision.clone()
    }
}

/// Observation handle paired with [`recording_csr_signer`].
#[derive(Clone)]
pub struct IntegrationCsrSignerObservation {
    request_count: Arc<AtomicUsize>,
    changed: Arc<Notify>,
}

impl IntegrationCsrSignerObservation {
    pub fn request_count(&self) -> usize {
        self.request_count.load(Ordering::Acquire)
    }

    pub async fn wait_for_request(&self) {
        loop {
            let changed = self.changed.notified();
            if self.request_count() > 0 {
                return;
            }
            changed.await;
        }
    }
}

struct IntegrationRecordingCsrSigner {
    request_count: Arc<AtomicUsize>,
    changed: Arc<Notify>,
}

impl crate::csr_signer::CsrSigner for IntegrationRecordingCsrSigner {
    fn sign(
        &self,
        _request: crate::csr_signer::SignRequest,
    ) -> Result<crate::csr_signer::SignResult, crate::CredentialOperationError> {
        self.request_count.fetch_add(1, Ordering::AcqRel);
        self.changed.notify_waiters();
        Ok(crate::csr_signer::SignResult {
            certificate_pem: "-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----\n"
                .to_string(),
        })
    }
}

/// Builds the canonical CSR signer fake and its event-driven observation handle.
pub fn recording_csr_signer() -> (
    Arc<dyn crate::csr_signer::CsrSigner>,
    IntegrationCsrSignerObservation,
) {
    let request_count = Arc::new(AtomicUsize::new(0));
    let changed = Arc::new(Notify::new());
    let observation = IntegrationCsrSignerObservation {
        request_count: Arc::clone(&request_count),
        changed: Arc::clone(&changed),
    };
    let signer = Arc::new(IntegrationRecordingCsrSigner {
        request_count,
        changed,
    });
    (signer, observation)
}

pub fn generate_ca_full_at(
    valid_at: OffsetDateTime,
) -> Result<(Certificate, KeyPair, String, String)> {
    crate::cert::generate_ca_full_at(valid_at)
}

#[allow(clippy::too_many_arguments)]
pub fn generate_server_cert_with_config_at(
    ca_cert: &Certificate,
    ca_key: &KeyPair,
    service_cidr: &str,
    pod_subnet: &str,
    host_ip: Option<String>,
    node_name: &str,
    api_fqdn: Option<&str>,
    valid_at: OffsetDateTime,
) -> Result<(String, String)> {
    crate::cert::generate_server_cert_with_config_at(
        ca_cert,
        ca_key,
        service_cidr,
        pod_subnet,
        host_ip,
        node_name,
        api_fqdn,
        valid_at,
    )
}

pub fn generate_server_cert_at(
    ca_cert: &Certificate,
    ca_key: &KeyPair,
    valid_at: OffsetDateTime,
) -> Result<(String, String)> {
    crate::cert::generate_server_cert_at(ca_cert, ca_key, valid_at)
}

pub fn generate_admin_cert_at(
    ca_cert: &Certificate,
    ca_key: &KeyPair,
    valid_at: OffsetDateTime,
) -> Result<(String, String)> {
    crate::cert::generate_admin_cert_at(ca_cert, ca_key, valid_at)
}

#[cfg(test)]
mod tests {
    use crate::{
        AuthenticatedIdentity,
        authorizer::{AuthorizationDecision, Authorizer},
        csr_signer::SignRequest,
        request_attributes::AuthorizationRequest,
    };

    use super::{AllowAllAuthorizer, RecordingAuthorizer, recording_csr_signer};

    #[tokio::test]
    async fn allow_all_authorizer_preserves_the_harness_allow_decision() {
        let decision = AllowAllAuthorizer
            .authorize(
                &AuthenticatedIdentity::anonymous(),
                &AuthorizationRequest::resource("get", "", "v1", "pods", None, None, None),
            )
            .await;

        assert_eq!(
            decision,
            AuthorizationDecision::allow("integration harness allow-all")
        );
    }

    #[tokio::test]
    async fn recording_authorizer_preserves_identity_request_and_decision() {
        let authorizer = RecordingAuthorizer::deny("denied by fixture");
        let identity = AuthenticatedIdentity::anonymous();
        let request = AuthorizationRequest::resource(
            "delete",
            "default",
            "v1",
            "pods",
            Some("pod-a"),
            None,
            None,
        );

        let decision = authorizer.authorize(&identity, &request).await;
        assert_eq!(decision, AuthorizationDecision::deny("denied by fixture"));
        assert_eq!(authorizer.take_calls().await, vec![(identity, request)]);
    }

    #[tokio::test]
    async fn recording_csr_signer_observes_requests_without_polling() {
        let (signer, observation) = recording_csr_signer();
        signer
            .sign(SignRequest {
                csr_pem: vec![],
                common_name: "system:node:fixture".to_string(),
                organizations: vec!["system:nodes".to_string()],
                usages: vec!["client auth".to_string()],
                ttl_seconds: 300,
            })
            .expect("fixture signer must accept the request");

        observation.wait_for_request().await;
        assert_eq!(observation.request_count(), 1);
    }
}
