//! Worker node credential policy and CSR bootstrap workflow.
//!
//! This module owns framework-neutral credential DTOs, focused ports,
//! deterministic certificate validation, and the supervised bootstrap flow.
//! Filesystem and HTTP/TLS implementations remain private composition adapters.

use async_trait::async_trait;
use time::OffsetDateTime;

use crate::CredentialOperationError;

const WORKER_CREDENTIAL_RENEW_BEFORE_SECONDS: i64 = 3600;

/// Persisted node client identity used for steady-state leader traffic.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerCredential {
    certificate_pem: String,
    private_key_pem: String,
    node_name: String,
    kubeconfig_yaml: String,
}

impl WorkerCredential {
    pub fn try_new(
        certificate_pem: String,
        private_key_pem: String,
        node_name: String,
        kubeconfig_yaml: String,
    ) -> Result<Self, CredentialOperationError> {
        if certificate_pem.trim().is_empty() {
            return Err(CredentialOperationError::rejected(
                "worker credential certificate PEM must not be empty",
            ));
        }
        if private_key_pem.trim().is_empty() {
            return Err(CredentialOperationError::rejected(
                "worker credential private key PEM must not be empty",
            ));
        }
        if node_name.trim().is_empty() {
            return Err(CredentialOperationError::rejected(
                "worker credential node name must not be empty",
            ));
        }
        Ok(Self {
            certificate_pem,
            private_key_pem,
            node_name,
            kubeconfig_yaml,
        })
    }

    pub fn certificate_pem(&self) -> &str {
        &self.certificate_pem
    }

    pub fn private_key_pem(&self) -> &str {
        &self.private_key_pem
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub fn kubeconfig_yaml(&self) -> &str {
        &self.kubeconfig_yaml
    }

    pub fn into_tls_parts(self) -> (String, String) {
        (self.certificate_pem, self.private_key_pem)
    }
}

/// Focused asynchronous persistence port for worker credentials.
#[async_trait]
pub trait WorkerCredentialStore: Send + Sync {
    async fn load(&self) -> Result<Option<WorkerCredential>, CredentialOperationError>;
    async fn save(&self, credential: &WorkerCredential) -> Result<(), CredentialOperationError>;
    async fn delete(&self) -> Result<(), CredentialOperationError>;
}

/// Focused transport port for Kubernetes CSR submission and observation.
#[async_trait]
pub trait WorkerCsrBootstrapClient: Send + Sync {
    async fn submit_kubelet_client_csr(
        &self,
        csr: &crate::kubelet_client_cert::KubeletClientCsr,
    ) -> Result<String, CredentialOperationError>;

    async fn wait_for_certificate(
        &self,
        csr_name: &str,
    ) -> Result<String, CredentialOperationError>;
}

/// Result of resolving the local persisted credential.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerCredentialSource {
    Existing(WorkerCredential),
    BootstrapRequired,
}

/// Validate a persisted credential against one operation-scoped instant.
pub fn validate_worker_credential_at(
    credential: &WorkerCredential,
    now: OffsetDateTime,
) -> Result<(), CredentialOperationError> {
    use x509_parser::pem::Pem;
    use x509_parser::prelude::*;

    let pem = Pem::read(std::io::Cursor::new(credential.certificate_pem.as_bytes()))
        .map_err(|error| {
            CredentialOperationError::rejected(format!(
                "failed to parse stored certificate PEM: {error}"
            ))
        })?
        .0;
    let (_, certificate) = X509Certificate::from_der(&pem.contents).map_err(|error| {
        CredentialOperationError::rejected(format!(
            "failed to parse stored certificate DER: {error}"
        ))
    })?;

    let common_name = certificate
        .subject()
        .iter_common_name()
        .next()
        .and_then(|attribute| attribute.as_str().ok())
        .unwrap_or("");
    let expected_common_name = format!("system:node:{}", credential.node_name);
    if common_name != expected_common_name {
        return Err(CredentialOperationError::rejected(format!(
            "stored certificate CN mismatch: got {common_name:?}, expected {expected_common_name:?}"
        )));
    }

    let has_system_nodes = certificate.subject().iter_organization().any(|attribute| {
        attribute
            .as_str()
            .map(|organization| {
                organization
                    .split(',')
                    .any(|group| group.trim() == crate::cert::NODES_GROUP)
            })
            .unwrap_or(false)
    });
    if !has_system_nodes {
        return Err(CredentialOperationError::rejected(
            "stored certificate missing O=system:nodes",
        ));
    }

    let not_after = certificate.validity().not_after.timestamp();
    if not_after < now.unix_timestamp() {
        return Err(CredentialOperationError::rejected(
            "stored certificate is expired",
        ));
    }
    if not_after
        <= now
            .unix_timestamp()
            .saturating_add(WORKER_CREDENTIAL_RENEW_BEFORE_SECONDS)
    {
        return Err(CredentialOperationError::rejected(
            "stored certificate expires too soon for startup renewal window",
        ));
    }
    Ok(())
}

/// Whether the certificate contains an organization/group value.
pub fn worker_credential_has_group(credential: &WorkerCredential, group: &str) -> bool {
    use x509_parser::pem::Pem;

    let Ok((pem, _)) = Pem::read(std::io::Cursor::new(credential.certificate_pem.as_bytes()))
    else {
        return false;
    };
    match crate::user::user_from_cert(&pem.contents) {
        Ok(user) => user.groups.iter().any(|candidate| candidate == group),
        Err(_) => false,
    }
}

/// Resolve and validate a persisted credential without blocking the runtime.
pub async fn resolve_worker_credential(
    store: &dyn WorkerCredentialStore,
    crypto: &klights_supervisor::CryptoExecutor,
    now: OffsetDateTime,
) -> Result<WorkerCredentialSource, CredentialOperationError> {
    match store.load().await? {
        Some(credential) => {
            let credential_for_validation = credential.clone();
            let validation = crypto
                .run_blocking("validate-persisted-worker-credential", move || {
                    validate_worker_credential_at(&credential_for_validation, now)
                })
                .await
                .map_err(|error| {
                    CredentialOperationError::dependency_failure(format!(
                        "persisted credential validation worker failed: {error}"
                    ))
                })?;
            match validation {
                Ok(()) => Ok(WorkerCredentialSource::Existing(credential)),
                Err(error) => {
                    store.delete().await.map_err(|delete_error| {
                        contextual_error(
                            &format!("persisted credential invalid ({error}); failed to clear it"),
                            delete_error,
                        )
                    })?;
                    Err(CredentialOperationError::rejected(format!(
                        "persisted credential invalid, cleared for bootstrap: {error}"
                    )))
                }
            }
        }
        None => Ok(WorkerCredentialSource::BootstrapRequired),
    }
}

/// Generate, submit, observe, and persist a kubelet client credential.
pub async fn bootstrap_worker_credential(
    node_name: &str,
    client: &dyn WorkerCsrBootstrapClient,
    store: &dyn WorkerCredentialStore,
    crypto: &klights_supervisor::CryptoExecutor,
) -> Result<WorkerCredential, CredentialOperationError> {
    let node_name_for_csr = node_name.to_string();
    let csr = crypto
        .run_blocking("generate-worker-kubelet-client-csr", move || {
            crate::kubelet_client_cert::generate_kubelet_client_csr(&node_name_for_csr)
        })
        .await
        .map_err(|error| {
            CredentialOperationError::dependency_failure(format!(
                "kubelet client CSR worker failed: {error}"
            ))
        })?
        .map_err(|error| {
            CredentialOperationError::internal_failure(format!(
                "failed to generate kubelet client CSR: {error}"
            ))
        })?;

    let csr_name = client
        .submit_kubelet_client_csr(&csr)
        .await
        .map_err(|error| contextual_error("failed to submit CSR", error))?;
    let certificate_pem = client
        .wait_for_certificate(&csr_name)
        .await
        .map_err(|error| contextual_error("failed to obtain certificate", error))?;

    let credential = WorkerCredential::try_new(
        certificate_pem,
        csr.private_key_pem,
        node_name.to_string(),
        String::new(),
    )?;
    store
        .save(&credential)
        .await
        .map_err(|error| contextual_error("failed to persist credential", error))?;
    Ok(credential)
}

fn contextual_error(context: &str, error: CredentialOperationError) -> CredentialOperationError {
    let message = format!("{context}: {error}");
    match error {
        CredentialOperationError::Rejected { .. } => CredentialOperationError::rejected(message),
        CredentialOperationError::DependencyFailure { .. } => {
            CredentialOperationError::dependency_failure(message)
        }
        CredentialOperationError::InternalFailure { .. } => {
            CredentialOperationError::internal_failure(message)
        }
    }
}

/// Reject an unauthenticated CA-unverified initial join.
pub fn validate_insecure_bootstrap_authentication(
    skip_ca: bool,
    token: &str,
) -> Result<(), CredentialOperationError> {
    if skip_ca && token.trim().is_empty() {
        return Err(CredentialOperationError::rejected(
            "refusing insecure leader bootstrap: skip-ca disables TLS CA verification and no bootstrap token was provided",
        ));
    }
    Ok(())
}
