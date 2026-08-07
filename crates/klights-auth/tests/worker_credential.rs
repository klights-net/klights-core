use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use klights_auth::{
    CredentialOperationError,
    worker_credential::{
        WorkerCredential, WorkerCredentialSource, WorkerCredentialStore, WorkerCsrBootstrapClient,
        bootstrap_worker_credential, resolve_worker_credential,
        validate_insecure_bootstrap_authentication, validate_worker_credential_at,
    },
};
use klights_supervisor::{CryptoExecutor, TaskCategoryConfig, TaskSupervisor};
use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair};
use time::{Duration, OffsetDateTime};

#[derive(Default)]
struct MemoryStore {
    credential: Mutex<Option<WorkerCredential>>,
    deletes: Mutex<usize>,
    fail_load: bool,
    fail_save: bool,
    fail_delete: bool,
}

impl MemoryStore {
    fn with(credential: WorkerCredential) -> Self {
        Self {
            credential: Mutex::new(Some(credential)),
            deletes: Mutex::new(0),
            ..Self::default()
        }
    }

    fn failing_load() -> Self {
        Self {
            fail_load: true,
            ..Self::default()
        }
    }

    fn failing_save() -> Self {
        Self {
            fail_save: true,
            ..Self::default()
        }
    }

    fn with_delete_failure(credential: WorkerCredential) -> Self {
        Self {
            credential: Mutex::new(Some(credential)),
            fail_delete: true,
            ..Self::default()
        }
    }

    fn loaded(&self) -> Option<WorkerCredential> {
        self.credential.lock().expect("credential lock").clone()
    }

    fn delete_count(&self) -> usize {
        *self.deletes.lock().expect("delete count lock")
    }
}

#[async_trait]
impl WorkerCredentialStore for MemoryStore {
    async fn load(&self) -> Result<Option<WorkerCredential>, CredentialOperationError> {
        if self.fail_load {
            return Err(CredentialOperationError::dependency_failure("load failed"));
        }
        Ok(self.loaded())
    }

    async fn save(&self, credential: &WorkerCredential) -> Result<(), CredentialOperationError> {
        if self.fail_save {
            return Err(CredentialOperationError::dependency_failure("save failed"));
        }
        *self.credential.lock().expect("credential lock") = Some(credential.clone());
        Ok(())
    }

    async fn delete(&self) -> Result<(), CredentialOperationError> {
        if self.fail_delete {
            return Err(CredentialOperationError::dependency_failure(
                "delete failed",
            ));
        }
        *self.credential.lock().expect("credential lock") = None;
        *self.deletes.lock().expect("delete count lock") += 1;
        Ok(())
    }
}

#[derive(Default)]
struct RecordingCsrClient {
    submissions: Mutex<Vec<CsrSubmission>>,
    issued_certificate: Mutex<String>,
    reject_submission: bool,
}

struct CsrSubmission {
    pem: Vec<u8>,
    node_name: String,
}

impl RecordingCsrClient {
    fn issuing(certificate_pem: String) -> Self {
        Self {
            submissions: Mutex::new(Vec::new()),
            issued_certificate: Mutex::new(certificate_pem),
            reject_submission: false,
        }
    }

    fn rejecting() -> Self {
        Self {
            reject_submission: true,
            ..Self::default()
        }
    }
}

#[async_trait]
impl WorkerCsrBootstrapClient for RecordingCsrClient {
    async fn submit_kubelet_client_csr(
        &self,
        csr: &klights_auth::kubelet_client_cert::KubeletClientCsr,
    ) -> Result<String, CredentialOperationError> {
        if self.reject_submission {
            return Err(CredentialOperationError::rejected("CSR rejected"));
        }
        self.submissions
            .lock()
            .expect("submission lock")
            .push(CsrSubmission {
                pem: csr.csr_pem.clone(),
                node_name: csr.node_name.clone(),
            });
        Ok("worker-csr-00001".to_string())
    }

    async fn wait_for_certificate(
        &self,
        csr_name: &str,
    ) -> Result<String, CredentialOperationError> {
        assert_eq!(csr_name, "worker-csr-00001");
        Ok(self
            .issued_certificate
            .lock()
            .expect("issued certificate lock")
            .clone())
    }
}

fn credential_at(
    node_name: &str,
    groups: &[&str],
    now: OffsetDateTime,
    not_after: OffsetDateTime,
) -> WorkerCredential {
    let key = KeyPair::generate().expect("test key");
    let mut params = CertificateParams::new(Vec::<String>::new()).expect("test parameters");
    params.not_before = now - Duration::minutes(5);
    params.not_after = not_after;
    let mut subject = DistinguishedName::new();
    subject.push(DnType::CommonName, format!("system:node:{node_name}"));
    for group in groups {
        subject.push(DnType::OrganizationName, *group);
    }
    params.distinguished_name = subject;
    let certificate = params.self_signed(&key).expect("test certificate");
    WorkerCredential::try_new(
        certificate.pem(),
        key.serialize_pem(),
        node_name.to_string(),
        String::new(),
    )
    .expect("valid generated credential")
}

fn crypto() -> (Arc<TaskSupervisor>, CryptoExecutor) {
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let crypto = CryptoExecutor::new(supervisor.clone());
    (supervisor, crypto)
}

#[test]
fn credential_dto_rejects_blank_identity_material() {
    for (certificate, key, node) in [
        ("", "key", "worker-a"),
        ("certificate", "", "worker-a"),
        ("certificate", "key", "  "),
    ] {
        assert!(
            WorkerCredential::try_new(
                certificate.to_string(),
                key.to_string(),
                node.to_string(),
                String::new(),
            )
            .is_err()
        );
    }
}

#[test]
fn validation_uses_the_explicit_operation_time_and_renewal_window() {
    let now = OffsetDateTime::from_unix_timestamp(1_800_000_000).expect("fixed time");
    let valid = credential_at("worker-a", &["system:nodes"], now, now + Duration::hours(2));
    validate_worker_credential_at(&valid, now).expect("credential outside renewal window");

    let expiring = credential_at(
        "worker-a",
        &["system:nodes"],
        now,
        now + Duration::seconds(3600),
    );
    let error = validate_worker_credential_at(&expiring, now)
        .expect_err("credential inside one-hour renewal window must be rejected");
    assert!(error.message().contains("expires too soon"));

    let outside_window = credential_at(
        "worker-a",
        &["system:nodes"],
        now,
        now + Duration::seconds(3601),
    );
    validate_worker_credential_at(&outside_window, now)
        .expect("credential one second beyond renewal window is accepted");

    // Proves policy does not consult ambient wall time.
    validate_worker_credential_at(&valid, now - Duration::days(30))
        .expect("same certificate is valid at the supplied earlier instant");
}

#[test]
fn validation_rejects_identity_group_and_expiry_mismatches() {
    let now = OffsetDateTime::from_unix_timestamp(1_800_000_000).expect("fixed time");
    let original = credential_at("worker-a", &["system:nodes"], now, now + Duration::hours(2));
    let wrong_node = WorkerCredential::try_new(
        original.certificate_pem().to_string(),
        original.private_key_pem().to_string(),
        "worker-b".to_string(),
        String::new(),
    )
    .expect("structurally valid mismatched credential");
    assert!(
        validate_worker_credential_at(&wrong_node, now)
            .expect_err("CN mismatch")
            .message()
            .contains("CN mismatch")
    );

    let wrong_group = credential_at(
        "worker-a",
        &["system:masters"],
        now,
        now + Duration::hours(2),
    );
    assert!(
        validate_worker_credential_at(&wrong_group, now)
            .expect_err("missing system:nodes")
            .message()
            .contains("O=system:nodes")
    );

    let expired = credential_at(
        "worker-a",
        &["system:nodes"],
        now - Duration::hours(2),
        now - Duration::minutes(1),
    );
    assert!(
        validate_worker_credential_at(&expired, now)
            .expect_err("expired certificate")
            .message()
            .contains("expired")
    );
}

#[test]
fn validation_rejects_malformed_pem_and_der_and_accepts_comma_separated_group() {
    let now = OffsetDateTime::from_unix_timestamp(1_800_000_000).expect("fixed time");
    for certificate in [
        "not a certificate",
        "-----BEGIN CERTIFICATE-----\nAQID\n-----END CERTIFICATE-----\n",
    ] {
        let credential = WorkerCredential::try_new(
            certificate.to_string(),
            "private key".to_string(),
            "worker-a".to_string(),
            String::new(),
        )
        .expect("structurally valid malformed credential");
        assert!(validate_worker_credential_at(&credential, now).is_err());
    }

    let grouped = credential_at(
        "worker-a",
        &["other, system:nodes"],
        now,
        now + Duration::hours(2),
    );
    validate_worker_credential_at(&grouped, now)
        .expect("comma-separated organization includes system:nodes");
}

#[test]
fn insecure_bootstrap_authentication_fails_closed() {
    assert!(validate_insecure_bootstrap_authentication(true, "").is_err());
    assert!(validate_insecure_bootstrap_authentication(true, "   ").is_err());
    validate_insecure_bootstrap_authentication(true, "abcdef.0123456789abcdef")
        .expect("authenticated skip-ca bootstrap");
    validate_insecure_bootstrap_authentication(false, "")
        .expect("verified TLS does not need this guard");
}

#[tokio::test]
async fn resolve_reuses_a_valid_credential_without_mutating_the_store() {
    let now = OffsetDateTime::from_unix_timestamp(1_800_000_000).expect("fixed time");
    let credential = credential_at("worker-a", &["system:nodes"], now, now + Duration::days(2));
    let store = MemoryStore::with(credential.clone());
    let (supervisor, crypto) = crypto();

    let source = resolve_worker_credential(&store, &crypto, now)
        .await
        .expect("resolve valid credential");
    assert!(matches!(
        source,
        WorkerCredentialSource::Existing(found) if found == credential
    ));
    assert_eq!(store.delete_count(), 0);
    let _ = supervisor.shutdown(std::time::Duration::from_secs(1)).await;
}

#[tokio::test]
async fn resolve_missing_credential_requires_bootstrap_without_delete() {
    let now = OffsetDateTime::from_unix_timestamp(1_800_000_000).expect("fixed time");
    let store = MemoryStore::default();
    let (supervisor, crypto) = crypto();

    let source = resolve_worker_credential(&store, &crypto, now)
        .await
        .expect("resolve missing credential");
    assert_eq!(source, WorkerCredentialSource::BootstrapRequired);
    assert_eq!(store.delete_count(), 0);

    let _ = supervisor.shutdown(std::time::Duration::from_secs(1)).await;
}

#[tokio::test]
async fn resolve_clears_an_invalid_credential_and_preserves_the_error_branch() {
    let now = OffsetDateTime::from_unix_timestamp(1_800_000_000).expect("fixed time");
    let store = MemoryStore::with(
        WorkerCredential::try_new(
            "not a certificate".to_string(),
            "private key".to_string(),
            "worker-a".to_string(),
            String::new(),
        )
        .expect("structurally valid corrupt credential"),
    );
    let (supervisor, crypto) = crypto();

    let error = resolve_worker_credential(&store, &crypto, now)
        .await
        .expect_err("corrupt persisted credential must remain a distinct error branch");
    assert!(error.message().contains("persisted credential invalid"));
    assert_eq!(store.delete_count(), 1);
    assert!(store.loaded().is_none());
    let _ = supervisor.shutdown(std::time::Duration::from_secs(1)).await;
}

#[tokio::test]
async fn resolve_surfaces_store_load_and_delete_failures() {
    let now = OffsetDateTime::from_unix_timestamp(1_800_000_000).expect("fixed time");
    let (supervisor, crypto) = crypto();

    let load_error = resolve_worker_credential(&MemoryStore::failing_load(), &crypto, now)
        .await
        .expect_err("load failure must be surfaced");
    assert!(load_error.message().contains("load failed"));

    let corrupt = WorkerCredential::try_new(
        "not a certificate".to_string(),
        "private key".to_string(),
        "worker-a".to_string(),
        String::new(),
    )
    .expect("structurally valid corrupt credential");
    let store = MemoryStore::with_delete_failure(corrupt);
    let delete_error = resolve_worker_credential(&store, &crypto, now)
        .await
        .expect_err("delete failure must be surfaced");
    assert!(delete_error.message().contains("failed to clear it"));
    assert!(delete_error.message().contains("delete failed"));
    assert!(store.loaded().is_some());

    let _ = supervisor.shutdown(std::time::Duration::from_secs(1)).await;
}

#[tokio::test]
async fn csr_bootstrap_uses_canonical_kubelet_contract_and_persists_once() {
    let store = MemoryStore::default();
    let issued = "-----BEGIN CERTIFICATE-----\nissued\n-----END CERTIFICATE-----\n";
    let client = RecordingCsrClient::issuing(issued.to_string());
    let (supervisor, crypto) = crypto();

    let credential = bootstrap_worker_credential("worker-a", &client, &store, &crypto)
        .await
        .expect("bootstrap credential");

    assert_eq!(credential.node_name(), "worker-a");
    assert_eq!(credential.certificate_pem(), issued);
    assert_eq!(store.loaded(), Some(credential));
    {
        let submissions = client.submissions.lock().expect("submission lock");
        assert_eq!(submissions.len(), 1);
        assert_eq!(submissions[0].node_name, "worker-a");
        assert!(
            String::from_utf8_lossy(&submissions[0].pem)
                .contains("-----BEGIN CERTIFICATE REQUEST-----")
        );
    }
    let _ = supervisor.shutdown(std::time::Duration::from_secs(1)).await;
}

#[tokio::test]
async fn csr_bootstrap_surfaces_store_save_failure() {
    let store = MemoryStore::failing_save();
    let client = RecordingCsrClient::issuing(
        "-----BEGIN CERTIFICATE-----\nissued\n-----END CERTIFICATE-----\n".to_string(),
    );
    let (supervisor, crypto) = crypto();

    let error = bootstrap_worker_credential("worker-a", &client, &store, &crypto)
        .await
        .expect_err("save failure must be surfaced");
    assert!(error.message().contains("failed to persist credential"));
    assert!(error.message().contains("save failed"));
    assert!(store.loaded().is_none());

    let _ = supervisor.shutdown(std::time::Duration::from_secs(1)).await;
}

#[tokio::test]
async fn csr_client_rejection_does_not_persist_a_credential() {
    let store = MemoryStore::default();
    let client = RecordingCsrClient::rejecting();
    let (supervisor, crypto) = crypto();

    let error = bootstrap_worker_credential("worker-a", &client, &store, &crypto)
        .await
        .expect_err("CSR rejection must be surfaced");
    assert!(error.message().contains("CSR rejected"));
    assert!(store.loaded().is_none());

    let _ = supervisor.shutdown(std::time::Duration::from_secs(1)).await;
}
