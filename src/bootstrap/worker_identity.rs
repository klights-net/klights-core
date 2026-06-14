//! Worker credential store: persist and load node client certificates.
//!
//! Workers bootstrap by submitting a CSR with a bootstrap token, then switch
//! to node client certificate auth for all steady-state traffic. This module
//! owns the credential lifecycle: generate, persist, load, and validate.

use anyhow::{Context as _, Result};
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose};
use serde_json::json;
use std::{path::PathBuf, sync::Arc};

use crate::leader_tls_policy::{LeaderTlsVerification, LeaderTlsVerificationPolicy};

const WORKER_CREDENTIAL_RENEW_BEFORE_SECONDS: i64 = 3600;

/// Stored node client certificate and private key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerCredential {
    pub certificate_pem: String,
    pub private_key_pem: String,
    pub node_name: String,
    pub kubeconfig_yaml: String,
}

/// Object-safe worker credential store.
///
/// Production writes to the worker data root with atomic rename; tests use
/// in-memory mocks.
pub trait WorkerCredentialStore: Send + Sync {
    /// Load a previously persisted credential, or `None` if none exists.
    fn load(&self) -> Result<Option<WorkerCredential>>;

    /// Persist a credential atomically. Must survive process restart.
    fn save(&self, cred: &WorkerCredential) -> Result<()>;

    /// Remove any persisted credential (e.g. on corruption or wrong node name).
    fn delete(&self) -> Result<()>;
}

/// Async worker credential store for production use from async runtime paths.
#[async_trait]
pub trait AsyncWorkerCredentialStore: Send + Sync {
    /// Load a previously persisted credential, or `None` if none exists.
    async fn load(&self) -> Result<Option<WorkerCredential>>;

    /// Persist a credential atomically. Must survive process restart.
    async fn save(&self, cred: &WorkerCredential) -> Result<()>;

    /// Remove any persisted credential.
    async fn delete(&self) -> Result<()>;
}

/// In-memory credential store for tests.
#[cfg(test)]
#[derive(Default)]
pub struct InMemoryWorkerCredentialStore {
    stored: std::sync::Mutex<Option<WorkerCredential>>,
}

#[cfg(test)]
impl InMemoryWorkerCredentialStore {
    pub fn new() -> Self {
        Self {
            stored: std::sync::Mutex::new(None),
        }
    }
}

#[cfg(test)]
impl WorkerCredentialStore for InMemoryWorkerCredentialStore {
    fn load(&self) -> Result<Option<WorkerCredential>> {
        Ok(self.stored.lock().unwrap().clone())
    }

    fn save(&self, cred: &WorkerCredential) -> Result<()> {
        *self.stored.lock().unwrap() = Some(cred.clone());
        Ok(())
    }

    fn delete(&self) -> Result<()> {
        *self.stored.lock().unwrap() = None;
        Ok(())
    }
}

/// Write `content` to a `.tmp` file adjacent to `final_path`, then rename
/// atomically so a crash mid-write never leaves a truncated target file.
fn atomic_write(final_path: &std::path::Path, content: &[u8]) -> Result<()> {
    use std::io::Write;

    let tmp_path = std::path::PathBuf::from(format!("{}.tmp", final_path.display()));
    {
        let mut f = std::fs::File::create(&tmp_path)
            .with_context(|| format!("failed to create {}", tmp_path.display()))?;
        f.write_all(content)
            .with_context(|| format!("failed to write {}", tmp_path.display()))?;
        f.sync_all().context("failed to sync temp file")?;
    } // drop the file handle before rename
    std::fs::rename(&tmp_path, final_path).with_context(|| {
        format!(
            "failed to rename {} -> {}",
            tmp_path.display(),
            final_path.display()
        )
    })
}

#[cfg(unix)]
fn set_owner_only(path: &std::path::Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("failed to set 0600 permissions on {}", path.display()))
}

#[cfg(not(unix))]
fn set_owner_only(_path: &std::path::Path) -> Result<()> {
    Ok(())
}

/// Filesystem-backed worker credential store.
///
/// Stores credentials under an explicit directory:
/// - `node.crt` — client certificate PEM
/// - `node.key` — client private key PEM
/// - `node_kubeconfig.yaml` — kubeconfig (optional)
///
/// Writes use atomic rename through a temporary file so a crash mid-write
/// does not leave a truncated credential on disk.
pub struct FilesystemWorkerCredentialStore {
    dir: PathBuf,
    node_name: String,
}

impl FilesystemWorkerCredentialStore {
    /// Create a store rooted at `dir` (typically the etc directory).
    pub fn new(dir: PathBuf, node_name: &str) -> Self {
        Self {
            dir,
            node_name: node_name.to_string(),
        }
    }

    fn cert_path(&self) -> PathBuf {
        self.dir.join("node.crt")
    }

    fn key_path(&self) -> PathBuf {
        self.dir.join("node.key")
    }

    fn kubeconfig_path(&self) -> PathBuf {
        self.dir.join("node_kubeconfig.yaml")
    }
}

impl WorkerCredentialStore for FilesystemWorkerCredentialStore {
    fn load(&self) -> Result<Option<WorkerCredential>> {
        let cert_path = self.cert_path();
        let key_path = self.key_path();

        if !cert_path.exists() || !key_path.exists() {
            return Ok(None);
        }

        let certificate_pem = std::fs::read_to_string(&cert_path)
            .with_context(|| format!("failed to read {}", cert_path.display()))?;
        let private_key_pem = std::fs::read_to_string(&key_path)
            .with_context(|| format!("failed to read {}", key_path.display()))?;
        let kubeconfig_path = self.kubeconfig_path();
        let kubeconfig_yaml = if kubeconfig_path.exists() {
            std::fs::read_to_string(&kubeconfig_path).unwrap_or_default()
        } else {
            String::new()
        };

        Ok(Some(WorkerCredential {
            certificate_pem,
            private_key_pem,
            node_name: self.node_name.clone(),
            kubeconfig_yaml,
        }))
    }

    fn save(&self, cred: &WorkerCredential) -> Result<()> {
        std::fs::create_dir_all(&self.dir)
            .with_context(|| format!("failed to create {}", self.dir.display()))?;

        let cert_path = self.cert_path();
        let key_path = self.key_path();
        atomic_write(&cert_path, cred.certificate_pem.as_bytes())?;
        atomic_write(&key_path, cred.private_key_pem.as_bytes())?;
        set_owner_only(&key_path)?;

        if !cred.kubeconfig_yaml.is_empty() {
            atomic_write(&self.kubeconfig_path(), cred.kubeconfig_yaml.as_bytes())?;
        }

        Ok(())
    }

    fn delete(&self) -> Result<()> {
        for path in [self.cert_path(), self.key_path(), self.kubeconfig_path()] {
            if path.exists() {
                std::fs::remove_file(&path)
                    .with_context(|| format!("failed to remove {}", path.display()))?;
            }
        }
        Ok(())
    }
}

/// Filesystem credential store that performs all blocking file I/O through
/// the application supervisor.
pub struct SupervisedFilesystemWorkerCredentialStore {
    dir: PathBuf,
    node_name: String,
    supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
}

impl SupervisedFilesystemWorkerCredentialStore {
    pub fn new(
        dir: PathBuf,
        node_name: &str,
        supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
    ) -> Self {
        Self {
            dir,
            node_name: node_name.to_string(),
            supervisor,
        }
    }

    pub fn for_namespace(
        namespace: &str,
        node_name: &str,
        supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
    ) -> Self {
        Self::new(crate::paths::etc_dir_path(namespace), node_name, supervisor)
    }

    fn key(&self) -> String {
        self.dir.to_string_lossy().to_string()
    }
}

#[async_trait]
impl AsyncWorkerCredentialStore for SupervisedFilesystemWorkerCredentialStore {
    async fn load(&self) -> Result<Option<WorkerCredential>> {
        let dir = self.dir.clone();
        let node_name = self.node_name.clone();
        self.supervisor
            .run_blocking_file_keyed("worker_credential_load", self.key(), move || {
                let store = FilesystemWorkerCredentialStore::new(dir, &node_name);
                WorkerCredentialStore::load(&store)
            })
            .await
            .context("worker credential load task failed")?
    }

    async fn save(&self, cred: &WorkerCredential) -> Result<()> {
        let dir = self.dir.clone();
        let node_name = self.node_name.clone();
        let cred = cred.clone();
        self.supervisor
            .run_blocking_file_keyed("worker_credential_save", self.key(), move || {
                let store = FilesystemWorkerCredentialStore::new(dir, &node_name);
                WorkerCredentialStore::save(&store, &cred)
            })
            .await
            .context("worker credential save task failed")?
    }

    async fn delete(&self) -> Result<()> {
        let dir = self.dir.clone();
        let node_name = self.node_name.clone();
        self.supervisor
            .run_blocking_file_keyed("worker_credential_delete", self.key(), move || {
                let store = FilesystemWorkerCredentialStore::new(dir, &node_name);
                WorkerCredentialStore::delete(&store)
            })
            .await
            .context("worker credential delete task failed")?
    }
}

/// Validate that a persisted worker credential is still usable.
///
/// Checks:
/// - Certificate contains the expected CN and O
/// - Certificate is not expired
pub fn validate_credential(cred: &WorkerCredential) -> Result<()> {
    use x509_parser::pem::Pem;
    use x509_parser::prelude::*;

    let pem = Pem::read(std::io::Cursor::new(cred.certificate_pem.as_bytes()))
        .map_err(|e| anyhow::anyhow!("failed to parse stored certificate PEM: {e}"))?
        .0;
    let (_, cert) = X509Certificate::from_der(&pem.contents)
        .map_err(|e| anyhow::anyhow!("failed to parse stored certificate DER: {e}"))?;

    // Check CN matches expected node name
    let subject = cert.subject();
    let cn = subject
        .iter_common_name()
        .next()
        .and_then(|a| a.as_str().ok())
        .unwrap_or("");
    let expected_cn = format!("system:node:{}", cred.node_name);
    if cn != expected_cn {
        return Err(anyhow::anyhow!(
            "stored certificate CN mismatch: got {cn:?}, expected {expected_cn:?}"
        ));
    }

    // Check O contains system:nodes. A single O attribute may carry several
    // comma-joined groups (control-plane node certs carry
    // `system:nodes,system:controlplanes`), so split before comparing — matching
    // how `user_from_cert` derives groups.
    let has_system_nodes = subject.iter_organization().any(|a| {
        a.as_str()
            .map(|o| o.split(',').any(|g| g.trim() == "system:nodes"))
            .unwrap_or(false)
    });
    if !has_system_nodes {
        return Err(anyhow::anyhow!("stored certificate missing O=system:nodes"));
    }

    // Check not expired
    let now = x509_parser::time::ASN1Time::now();
    let not_after = cert.validity().not_after;
    if not_after < now {
        return Err(anyhow::anyhow!("stored certificate is expired"));
    }
    let renewal_deadline = now
        .timestamp()
        .saturating_add(WORKER_CREDENTIAL_RENEW_BEFORE_SECONDS);
    if not_after.timestamp() <= renewal_deadline {
        return Err(anyhow::anyhow!(
            "stored certificate expires too soon for startup renewal window"
        ));
    }

    Ok(())
}

/// Whether a persisted credential's certificate carries the given group (an O /
/// organizationName value). Used to detect a control-plane node certificate
/// minted before the `system:controlplanes` group existed (an in-place upgrade,
/// or a seed-leader cert preserved across runs) so it can be re-minted. Returns
/// `false` if the certificate cannot be parsed.
pub fn credential_has_group(cred: &WorkerCredential, group: &str) -> bool {
    use x509_parser::pem::Pem;
    let Ok((pem, _)) = Pem::read(std::io::Cursor::new(cred.certificate_pem.as_bytes())) else {
        return false;
    };
    match crate::auth::user_from_cert(&pem.contents) {
        Ok(user) => user.groups.iter().any(|g| g == group),
        Err(_) => false,
    }
}

/// Resolved worker credential source.
#[derive(Debug)]
pub enum CredentialSource {
    /// Use a persisted node client certificate.
    ExistingCert(WorkerCredential),
    /// No valid persisted cert — bootstrap via CSR with token.
    BootstrapRequired,
}

/// Determine the worker credential source on startup.
///
/// Loads the persisted credential store. If a valid, unexpired cert exists
/// with the correct node name, returns `ExistingCert`. Otherwise returns
/// `BootstrapRequired`.
#[cfg(test)]
pub fn resolve_credential(store: &dyn WorkerCredentialStore) -> Result<CredentialSource> {
    match store.load()? {
        Some(cred) => match validate_credential(&cred) {
            Ok(()) => Ok(CredentialSource::ExistingCert(cred)),
            Err(e) => {
                // Corrupted/invalid cert — clear it so bootstrap can proceed
                let _ = store.delete();
                Err(anyhow::anyhow!(
                    "persisted credential invalid, cleared for bootstrap: {e}"
                ))
            }
        },
        None => Ok(CredentialSource::BootstrapRequired),
    }
}

/// Async variant of [`resolve_credential`] for production runtime paths.
pub async fn resolve_credential_async(
    store: &dyn AsyncWorkerCredentialStore,
) -> Result<CredentialSource> {
    match store.load().await? {
        Some(cred) => match validate_credential(&cred) {
            Ok(()) => Ok(CredentialSource::ExistingCert(cred)),
            Err(e) => {
                let _ = store.delete().await;
                Err(anyhow::anyhow!(
                    "persisted credential invalid, cleared for bootstrap: {e}"
                ))
            }
        },
        None => Ok(CredentialSource::BootstrapRequired),
    }
}

/// Resolve a valid persisted credential or bootstrap one through CSR.
#[cfg(test)]
pub async fn resolve_or_bootstrap_credential(
    node_name: &str,
    store: &dyn WorkerCredentialStore,
    client: &dyn CsrBootstrapClient,
) -> Result<WorkerCredential> {
    match resolve_credential(store) {
        Ok(CredentialSource::ExistingCert(cred)) => Ok(cred),
        Ok(CredentialSource::BootstrapRequired) => {
            bootstrap_with_csr(node_name, client, store).await
        }
        Err(err) => bootstrap_with_csr(node_name, client, store)
            .await
            .with_context(|| {
                format!("failed to bootstrap after invalid persisted credential: {err}")
            }),
    }
}

/// Client for submitting CSRs and waiting for certificate issuance.
///
/// Abstracts the Kubernetes API for CSR create + watch so the bootstrap
/// orchestrator can be tested without a live API server.
#[async_trait]
pub trait CsrBootstrapClient: Send + Sync {
    /// Submit a CSR and return the CSR object name.
    async fn submit_csr(
        &self,
        csr_pem: &[u8],
        signer_name: &str,
        usages: &[String],
    ) -> Result<String>;

    /// Wait for the CSR to be approved and return the issued certificate PEM.
    async fn wait_for_certificate(&self, csr_name: &str) -> Result<String>;
}

/// Refuse a CA-unverified ("skip-ca") leader bootstrap unless it is
/// authenticated by a bootstrap token. With CA verification disabled, the
/// bootstrap token is the only thing binding the connection to the real
/// cluster, so an empty token would mean a fully unauthenticated, MITM-able
/// join — which we reject.
fn guard_insecure_bootstrap(skip_ca: bool, token: &str) -> Result<()> {
    if skip_ca && token.trim().is_empty() {
        return Err(anyhow::anyhow!(
            "refusing insecure leader bootstrap: skip-ca disables TLS CA \
             verification and no bootstrap token was provided"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod skip_ca_guard_tests {
    use super::guard_insecure_bootstrap;

    #[test]
    fn skip_ca_without_token_is_rejected() {
        assert!(guard_insecure_bootstrap(true, "").is_err());
        assert!(guard_insecure_bootstrap(true, "   ").is_err());
    }

    #[test]
    fn skip_ca_with_token_is_allowed() {
        assert!(guard_insecure_bootstrap(true, "abc.def").is_ok());
    }

    #[test]
    fn secure_bootstrap_without_token_is_allowed() {
        // When CA verification is on, an empty token here is fine (token is
        // still required elsewhere, but this guard only governs skip-ca).
        assert!(guard_insecure_bootstrap(false, "").is_ok());
    }
}

/// Kubernetes API-backed CSR bootstrap client.
pub struct HttpCsrBootstrapClient {
    client: reqwest::Client,
    base_url: String,
    token: String,
}

const CSR_WATCH_TIMEOUT_SECONDS: u64 = 5;
const CSR_EMPTY_WATCH_RETRIES: usize = 12;

impl HttpCsrBootstrapClient {
    pub async fn new(
        leader_endpoint: String,
        token: String,
        ca_cert_path: Option<PathBuf>,
        skip_ca: bool,
        supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
    ) -> Result<Self> {
        let mut builder = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none());
        match LeaderTlsVerificationPolicy::new(ca_cert_path, skip_ca).verification() {
            LeaderTlsVerification::CaFile(path) => {
                let path_for_task = path.clone();
                let ca_pem = supervisor
                    .run_blocking_file_keyed(
                        "worker_csr_bootstrap_ca_cert",
                        path.to_string_lossy().to_string(),
                        move || std::fs::read(path_for_task),
                    )
                    .await
                    .context("failed to read leader CA certificate")?
                    .context("failed to read leader CA certificate")?;
                builder = builder.add_root_certificate(
                    reqwest::Certificate::from_pem(&ca_pem)
                        .context("failed to parse leader CA certificate")?,
                );
            }
            LeaderTlsVerification::SkipCa => {
                // Fail closed: insecure (CA-unverified) bootstrap is only ever
                // permitted when authenticated by a bootstrap token. Without one
                // there is nothing tying this connection to the real cluster.
                guard_insecure_bootstrap(true, &token)?;
                tracing::warn!(
                    leader_endpoint = %leader_endpoint,
                    security = "insecure-bootstrap",
                    "SECURITY: leader TLS CA verification is DISABLED for worker CSR \
                     bootstrap (skip-ca). The initial join is exposed to \
                     man-in-the-middle. Provide the leader CA certificate (or a CA \
                     hash pin) for a secure join; skip-ca should be used only on a \
                     trusted network."
                );
                builder = builder.danger_accept_invalid_certs(true);
            }
            LeaderTlsVerification::SystemRoots => {}
        }

        Ok(Self {
            client: builder
                .build()
                .context("failed to build CSR bootstrap HTTP client")?,
            base_url: normalize_api_endpoint(&leader_endpoint),
            token,
        })
    }

    fn csr_collection_url(&self) -> String {
        format!(
            "{}/apis/certificates.k8s.io/v1/certificatesigningrequests",
            self.base_url
        )
    }

    fn csr_named_url(&self, csr_name: &str) -> String {
        format!("{}/{}", self.csr_collection_url(), csr_name)
    }

    fn csr_watch_url(&self, csr_name: &str, resource_version: Option<&str>) -> String {
        let mut url = format!(
            "{}?watch=true&timeoutSeconds={}&fieldSelector=metadata.name%3D{}",
            self.csr_collection_url(),
            CSR_WATCH_TIMEOUT_SECONDS,
            csr_name
        );
        if let Some(rv) = resource_version.filter(|rv| !rv.is_empty()) {
            url.push_str("&resourceVersion=");
            url.push_str(rv);
        }
        url
    }

    async fn get_csr(&self, csr_name: &str) -> Result<serde_json::Value> {
        self.client
            .get(self.csr_named_url(csr_name))
            .bearer_auth(&self.token)
            .send()
            .await
            .with_context(|| format!("failed to get CSR {csr_name}"))?
            .error_for_status()
            .with_context(|| format!("CSR {csr_name} get request was rejected"))?
            .json::<serde_json::Value>()
            .await
            .with_context(|| format!("failed to decode CSR {csr_name}"))
    }
}

#[async_trait]
impl CsrBootstrapClient for HttpCsrBootstrapClient {
    async fn submit_csr(
        &self,
        csr_pem: &[u8],
        signer_name: &str,
        usages: &[String],
    ) -> Result<String> {
        let body = json!({
            "apiVersion": "certificates.k8s.io/v1",
            "kind": "CertificateSigningRequest",
            "metadata": {
                "generateName": "klights-node-client-"
            },
            "spec": {
                "request": general_purpose::STANDARD.encode(csr_pem),
                "signerName": signer_name,
                "usages": usages,
            }
        });
        let response = self
            .client
            .post(self.csr_collection_url())
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .context("failed to create CSR")?
            .error_for_status()
            .context("CSR create request was rejected")?
            .json::<serde_json::Value>()
            .await
            .context("failed to decode CSR create response")?;

        response
            .pointer("/metadata/name")
            .and_then(|name| name.as_str())
            .filter(|name| !name.is_empty())
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow::anyhow!("CSR create response did not include metadata.name"))
    }

    async fn wait_for_certificate(&self, csr_name: &str) -> Result<String> {
        let mut empty_watch_closes = 0usize;
        loop {
            let current = self.get_csr(csr_name).await?;

            if let Some(cert) = issued_certificate_pem(&current)? {
                return Ok(cert);
            }
            let resource_version = current
                .pointer("/metadata/resourceVersion")
                .and_then(|rv| rv.as_str());

            let mut response = self
                .client
                .get(self.csr_watch_url(csr_name, resource_version))
                .bearer_auth(&self.token)
                .send()
                .await
                .with_context(|| format!("failed to watch CSR {csr_name}"))?
                .error_for_status()
                .with_context(|| format!("CSR {csr_name} watch request was rejected"))?;
            let mut pending = Vec::new();
            let mut saw_relevant_event = false;

            while let Some(chunk) = response
                .chunk()
                .await
                .with_context(|| format!("failed reading CSR {csr_name} watch stream"))?
            {
                pending.extend_from_slice(&chunk);
                while let Some(newline) = pending.iter().position(|b| *b == b'\n') {
                    let line: Vec<u8> = pending.drain(..=newline).collect();
                    let line = line.strip_suffix(b"\n").unwrap_or(&line);
                    let line = line.strip_suffix(b"\r").unwrap_or(line);
                    if line.is_empty() {
                        continue;
                    }
                    let event: serde_json::Value =
                        serde_json::from_slice(line).context("failed to decode CSR watch event")?;
                    let object = event.get("object").unwrap_or(&event);
                    if !csr_object_matches_name(object, csr_name) {
                        continue;
                    }
                    saw_relevant_event = true;
                    if let Some(cert) = issued_certificate_pem(object)? {
                        return Ok(cert);
                    }
                }
            }

            if saw_relevant_event {
                empty_watch_closes = 0;
            } else {
                empty_watch_closes += 1;
                if empty_watch_closes >= CSR_EMPTY_WATCH_RETRIES {
                    return Err(anyhow::anyhow!(
                        "CSR {csr_name} watch ended before certificate was issued"
                    ));
                }
            }
        }
    }
}

fn normalize_api_endpoint(endpoint: &str) -> String {
    let trimmed = endpoint.trim().trim_end_matches('/');
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_string()
    } else {
        format!("https://{trimmed}")
    }
}

fn issued_certificate_pem(csr: &serde_json::Value) -> Result<Option<String>> {
    if let Some(encoded) = csr
        .pointer("/status/certificate")
        .and_then(|cert| cert.as_str())
        .filter(|cert| !cert.is_empty())
    {
        let pem = general_purpose::STANDARD
            .decode(encoded)
            .context("failed to decode issued CSR certificate")?;
        return String::from_utf8(pem)
            .context("issued CSR certificate was not valid UTF-8")
            .map(Some);
    }

    if let Some(reason) = terminal_csr_condition(csr) {
        return Err(anyhow::anyhow!("CSR was not issued: {reason}"));
    }

    Ok(None)
}

fn csr_object_matches_name(csr: &serde_json::Value, csr_name: &str) -> bool {
    csr.pointer("/metadata/name").and_then(|name| name.as_str()) == Some(csr_name)
}

fn terminal_csr_condition(csr: &serde_json::Value) -> Option<String> {
    let conditions = csr.pointer("/status/conditions")?.as_array()?;
    conditions.iter().find_map(|condition| {
        let status = condition.get("status").and_then(|status| status.as_str());
        let kind = condition.get("type").and_then(|kind| kind.as_str());
        match (kind, status) {
            (Some("Denied" | "Failed"), Some("True")) => {
                let reason = condition
                    .get("reason")
                    .and_then(|reason| reason.as_str())
                    .unwrap_or("unknown");
                let message = condition
                    .get("message")
                    .and_then(|message| message.as_str())
                    .unwrap_or("");
                Some(if message.is_empty() {
                    reason.to_string()
                } else {
                    format!("{reason}: {message}")
                })
            }
            _ => None,
        }
    })
}

/// Complete the CSR bootstrap flow: generate CSR, submit, wait for cert,
/// persist credential.
///
/// Returns the persisted `WorkerCredential` on success.
#[cfg(test)]
pub async fn bootstrap_with_csr(
    node_name: &str,
    client: &dyn CsrBootstrapClient,
    store: &dyn WorkerCredentialStore,
) -> Result<WorkerCredential> {
    let csr = crate::auth::kubelet_client_cert::generate_kubelet_client_csr(node_name)
        .context("failed to generate kubelet client CSR")?;

    let csr_name = client
        .submit_csr(
            &csr.csr_pem,
            "kubernetes.io/kube-apiserver-client-kubelet",
            &["client auth".to_string()],
        )
        .await
        .context("failed to submit CSR")?;

    let certificate_pem = client
        .wait_for_certificate(&csr_name)
        .await
        .context("failed to obtain certificate")?;

    let cred = WorkerCredential {
        certificate_pem,
        private_key_pem: csr.private_key_pem,
        node_name: node_name.to_string(),
        kubeconfig_yaml: String::new(), // built by caller with endpoint info
    };

    store.save(&cred).context("failed to persist credential")?;
    Ok(cred)
}

/// Complete the CSR bootstrap flow and persist through an async credential store.
pub async fn bootstrap_with_csr_async_store(
    node_name: &str,
    client: &dyn CsrBootstrapClient,
    store: &dyn AsyncWorkerCredentialStore,
) -> Result<WorkerCredential> {
    let csr = crate::auth::kubelet_client_cert::generate_kubelet_client_csr(node_name)
        .context("failed to generate kubelet client CSR")?;

    let csr_name = client
        .submit_csr(
            &csr.csr_pem,
            "kubernetes.io/kube-apiserver-client-kubelet",
            &["client auth".to_string()],
        )
        .await
        .context("failed to submit CSR")?;

    let certificate_pem = client
        .wait_for_certificate(&csr_name)
        .await
        .context("failed to obtain certificate")?;

    let cred = WorkerCredential {
        certificate_pem,
        private_key_pem: csr.private_key_pem,
        node_name: node_name.to_string(),
        kubeconfig_yaml: String::new(),
    };

    store
        .save(&cred)
        .await
        .context("failed to persist credential")?;
    Ok(cred)
}

/// Recorded CSR submission request.
#[cfg(test)]
#[derive(Clone, Debug)]
pub struct CsrSubmission {
    pub csr_pem: Vec<u8>,
    pub signer_name: String,
    pub usages: Vec<String>,
}

/// In-memory mock CSR bootstrap client for tests.
#[cfg(test)]
pub struct InMemoryCsrBootstrapClient {
    submitted_csrs: std::sync::Mutex<Vec<CsrSubmission>>,
    /// Certificate to return for `wait_for_certificate`. Use empty string
    /// to simulate timeout/denial.
    certificate_response: String,
}

#[cfg(test)]
impl InMemoryCsrBootstrapClient {
    pub fn new(certificate_response: String) -> Self {
        Self {
            submitted_csrs: std::sync::Mutex::new(Vec::new()),
            certificate_response,
        }
    }

    pub fn take_requests(&self) -> Vec<CsrSubmission> {
        std::mem::take(&mut self.submitted_csrs.lock().unwrap())
    }
}

#[cfg(test)]
#[async_trait]
impl CsrBootstrapClient for InMemoryCsrBootstrapClient {
    async fn submit_csr(
        &self,
        csr_pem: &[u8],
        signer_name: &str,
        usages: &[String],
    ) -> Result<String> {
        self.submitted_csrs.lock().unwrap().push(CsrSubmission {
            csr_pem: csr_pem.to_vec(),
            signer_name: signer_name.to_string(),
            usages: usages.to_vec(),
        });
        Ok("test-csr-name".to_string())
    }

    async fn wait_for_certificate(&self, _csr_name: &str) -> Result<String> {
        if self.certificate_response.is_empty() {
            Err(anyhow::anyhow!("CSR approval timed out or denied"))
        } else {
            Ok(self.certificate_response.clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_credential(node_name: &str) -> WorkerCredential {
        WorkerCredential {
            certificate_pem: "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----\n"
                .to_string(),
            private_key_pem: "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
                .to_string(),
            node_name: node_name.to_string(),
            kubeconfig_yaml: format!(
                "apiVersion: v1\nkind: Config\ncurrent-context: node-{node_name}\n"
            ),
        }
    }

    #[test]
    fn test_in_memory_store_save_and_load_returns_credential() {
        let store = InMemoryWorkerCredentialStore::new();
        let cred = sample_credential("tokyo");
        store.save(&cred).unwrap();
        let loaded = store.load().unwrap();
        assert_eq!(loaded, Some(cred));
    }

    #[test]
    fn test_in_memory_store_load_empty_returns_none() {
        let store = InMemoryWorkerCredentialStore::new();
        let loaded = store.load().unwrap();
        assert_eq!(loaded, None);
    }

    #[test]
    fn test_in_memory_store_delete_clears_credential() {
        let store = InMemoryWorkerCredentialStore::new();
        store.save(&sample_credential("tokyo")).unwrap();
        store.delete().unwrap();
        assert_eq!(store.load().unwrap(), None);
    }

    // --- Credential validation tests ---

    /// Generate a self-signed node client certificate for testing validation.
    fn generate_test_cert_with_validity(
        node_name: &str,
        orgs: &[&str],
        not_before_offset_seconds: i64,
        not_after_offset_seconds: i64,
    ) -> (String, String) {
        use rcgen::{CertificateParams, DnType, KeyPair, KeyUsagePurpose};
        use time::{Duration, OffsetDateTime};

        let mut params = CertificateParams::default();
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, format!("system:node:{node_name}"));
        for org in orgs {
            params
                .distinguished_name
                .push(DnType::OrganizationName, (*org).to_string());
        }
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        let now = OffsetDateTime::now_utc();
        params.not_before = now + Duration::seconds(not_before_offset_seconds);
        params.not_after = now + Duration::seconds(not_after_offset_seconds);

        let node_key = KeyPair::generate().unwrap();
        let node_cert = params.self_signed(&node_key).unwrap();

        (node_cert.pem(), node_key.serialize_pem())
    }

    fn generate_test_cert(node_name: &str, orgs: &[&str]) -> (String, String) {
        generate_test_cert_with_validity(node_name, orgs, -60, 31_536_000)
    }

    #[test]
    fn credential_has_group_detects_controlplane_group() {
        // A worker cert (system:nodes only) does not carry the control-plane
        // group; a control-plane cert does. This drives the upgrade re-mint of a
        // pre-`system:controlplanes` control-plane node cert.
        let (worker_cert, worker_key) = generate_test_cert("w", &["system:nodes"]);
        let worker = WorkerCredential {
            certificate_pem: worker_cert,
            private_key_pem: worker_key,
            node_name: "w".to_string(),
            kubeconfig_yaml: String::new(),
        };
        assert!(!credential_has_group(&worker, "system:controlplanes"));
        assert!(credential_has_group(&worker, "system:nodes"));

        let (cp_cert, cp_key) = generate_test_cert("cp", &["system:controlplanes"]);
        let cp = WorkerCredential {
            certificate_pem: cp_cert,
            private_key_pem: cp_key,
            node_name: "cp".to_string(),
            kubeconfig_yaml: String::new(),
        };
        assert!(credential_has_group(&cp, "system:controlplanes"));
    }

    #[test]
    fn test_validate_credential_with_valid_cert_passes() {
        let (cert_pem, key_pem) = generate_test_cert("tokyo", &["system:nodes"]);
        let cred = WorkerCredential {
            certificate_pem: cert_pem,
            private_key_pem: key_pem,
            node_name: "tokyo".to_string(),
            kubeconfig_yaml: "...".to_string(),
        };
        assert!(validate_credential(&cred).is_ok());
    }

    #[test]
    fn test_validate_credential_wrong_node_name_fails() {
        let (cert_pem, key_pem) = generate_test_cert("tokyo", &["system:nodes"]);
        let cred = WorkerCredential {
            certificate_pem: cert_pem,
            private_key_pem: key_pem,
            node_name: "osaka".to_string(), // wrong node name
            kubeconfig_yaml: "...".to_string(),
        };
        let err = validate_credential(&cred).unwrap_err();
        assert!(err.to_string().contains("CN mismatch"));
    }

    #[test]
    fn test_validate_credential_missing_system_nodes_fails() {
        let (cert_pem, key_pem) = generate_test_cert("tokyo", &["other-org"]);
        let cred = WorkerCredential {
            certificate_pem: cert_pem,
            private_key_pem: key_pem,
            node_name: "tokyo".to_string(),
            kubeconfig_yaml: "...".to_string(),
        };
        let err = validate_credential(&cred).unwrap_err();
        assert!(err.to_string().contains("O=system:nodes"));
    }

    #[test]
    fn test_validate_credential_expiring_within_renewal_window_fails() {
        let (cert_pem, key_pem) =
            generate_test_cert_with_validity("tokyo", &["system:nodes"], -60, 600);
        let cred = WorkerCredential {
            certificate_pem: cert_pem,
            private_key_pem: key_pem,
            node_name: "tokyo".to_string(),
            kubeconfig_yaml: "...".to_string(),
        };

        let err = validate_credential(&cred).unwrap_err();
        assert!(
            err.to_string().contains("expires too soon"),
            "unexpected error: {err}"
        );
    }

    // --- Credential resolution tests ---

    #[test]
    fn test_resolve_credential_with_empty_store_returns_bootstrap_required() {
        let store = InMemoryWorkerCredentialStore::new();
        match resolve_credential(&store).unwrap() {
            CredentialSource::BootstrapRequired => {}
            _ => panic!("expected BootstrapRequired"),
        }
    }

    #[test]
    fn test_resolve_credential_with_valid_cert_returns_existing() {
        let store = InMemoryWorkerCredentialStore::new();
        let (cert_pem, key_pem) = generate_test_cert("tokyo", &["system:nodes"]);
        let cred = WorkerCredential {
            certificate_pem: cert_pem,
            private_key_pem: key_pem,
            node_name: "tokyo".to_string(),
            kubeconfig_yaml: "...".to_string(),
        };
        store.save(&cred).unwrap();
        match resolve_credential(&store).unwrap() {
            CredentialSource::ExistingCert(c) => {
                assert_eq!(c.node_name, "tokyo");
            }
            _ => panic!("expected ExistingCert"),
        }
    }

    #[test]
    fn test_resolve_credential_with_corrupted_cert_clears_and_errors() {
        let store = InMemoryWorkerCredentialStore::new();
        let cred = sample_credential("tokyo");
        store.save(&cred).unwrap();

        // The sample credential has a fake cert, so validate will fail
        let err = resolve_credential(&store).unwrap_err();
        assert!(err.to_string().contains("invalid"));
        // Store should be cleared
        assert!(store.load().unwrap().is_none());
    }

    #[tokio::test]
    async fn test_resolve_or_bootstrap_with_empty_store_submits_csr() {
        let store = InMemoryWorkerCredentialStore::new();
        let client = InMemoryCsrBootstrapClient::new(fake_cert_pem());

        let cred = resolve_or_bootstrap_credential("tokyo", &store, &client)
            .await
            .unwrap();

        assert_eq!(cred.node_name, "tokyo");
        assert_eq!(
            store.load().unwrap().unwrap().certificate_pem,
            fake_cert_pem()
        );

        let requests = client.take_requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].signer_name,
            "kubernetes.io/kube-apiserver-client-kubelet"
        );
    }

    #[tokio::test]
    async fn test_resolve_or_bootstrap_with_valid_cert_skips_csr() {
        let store = InMemoryWorkerCredentialStore::new();
        let (cert_pem, key_pem) = generate_test_cert("tokyo", &["system:nodes"]);
        let cred = WorkerCredential {
            certificate_pem: cert_pem.clone(),
            private_key_pem: key_pem,
            node_name: "tokyo".to_string(),
            kubeconfig_yaml: "...".to_string(),
        };
        store.save(&cred).unwrap();
        let client = InMemoryCsrBootstrapClient::new(fake_cert_pem());

        let resolved = resolve_or_bootstrap_credential("tokyo", &store, &client)
            .await
            .unwrap();

        assert_eq!(resolved.certificate_pem, cert_pem);
        assert!(client.take_requests().is_empty());
    }

    // --- CSR bootstrap orchestrator tests ---

    fn fake_cert_pem() -> String {
        "-----BEGIN CERTIFICATE-----\nFAKECERT\n-----END CERTIFICATE-----\n".to_string()
    }

    #[tokio::test]
    async fn test_bootstrap_with_csr_persists_credential() {
        let store = InMemoryWorkerCredentialStore::new();
        let client = InMemoryCsrBootstrapClient::new(fake_cert_pem());

        let cred = bootstrap_with_csr("tokyo", &client, &store).await.unwrap();
        assert_eq!(cred.node_name, "tokyo");
        assert_eq!(cred.certificate_pem, fake_cert_pem());
        assert!(!cred.private_key_pem.is_empty());

        // Verify persisted
        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded.node_name, "tokyo");
        assert_eq!(loaded.certificate_pem, fake_cert_pem());
    }

    #[tokio::test]
    async fn test_bootstrap_with_csr_submits_correct_csr() {
        let store = InMemoryWorkerCredentialStore::new();
        let client = InMemoryCsrBootstrapClient::new(fake_cert_pem());

        bootstrap_with_csr("tokyo", &client, &store).await.unwrap();

        let requests = client.take_requests();
        assert_eq!(requests.len(), 1);
        let req = &requests[0];
        assert_eq!(
            req.signer_name,
            "kubernetes.io/kube-apiserver-client-kubelet"
        );
        assert!(req.usages.contains(&"client auth".to_string()));

        // Verify CSR PEM has the right subject
        let pem_str = String::from_utf8_lossy(&req.csr_pem);
        assert!(pem_str.contains("-----BEGIN CERTIFICATE REQUEST-----"));
    }

    #[tokio::test]
    async fn test_bootstrap_with_csr_denial_returns_error() {
        let store = InMemoryWorkerCredentialStore::new();
        let client = InMemoryCsrBootstrapClient::new(String::new()); // empty = denial

        let err = bootstrap_with_csr("tokyo", &client, &store)
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("timed out")
                || msg.contains("denied")
                || msg.contains("obtain certificate"),
            "unexpected error: {msg}"
        );
        // Store should be empty
        assert!(store.load().unwrap().is_none());
    }

    #[tokio::test]
    async fn http_csr_client_prefers_known_ca_path_over_skip_ca() {
        let dir = tempfile::tempdir().unwrap();
        let missing_ca = dir.path().join("missing-leader-ca.crt");
        let supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));

        let result = HttpCsrBootstrapClient::new(
            "https://leader:7679".to_string(),
            "abcdef.0123456789abcdef".to_string(),
            Some(missing_ca),
            true,
            supervisor.clone(),
        )
        .await;

        assert!(
            result.is_err(),
            "known CA path must not be downgraded by --skip-ca"
        );
        let err = result.err().unwrap();
        assert!(
            err.to_string()
                .contains("failed to read leader CA certificate"),
            "unexpected error: {err}"
        );
        let _ = supervisor.shutdown(std::time::Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn http_csr_wait_ignores_unrelated_watch_events_and_rechecks_after_close() {
        use axum::body::Body;
        use axum::extract::{Path, State};
        use axum::http::Response;
        use axum::routing::get;
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Clone)]
        struct CsrWaitState {
            named_gets: Arc<AtomicUsize>,
            target_cert_b64: String,
            other_cert_b64: String,
        }

        fn csr_obj(name: &str, rv: &str, cert_b64: Option<&str>) -> serde_json::Value {
            let mut obj = json!({
                "apiVersion": "certificates.k8s.io/v1",
                "kind": "CertificateSigningRequest",
                "metadata": {"name": name, "resourceVersion": rv},
                "spec": {"signerName": "kubernetes.io/kube-apiserver-client-kubelet"}
            });
            if let Some(cert) = cert_b64 {
                obj["status"] = json!({"certificate": cert});
            }
            obj
        }

        async fn get_named_csr(
            Path(name): Path<String>,
            State(state): State<CsrWaitState>,
        ) -> axum::Json<serde_json::Value> {
            let call = state.named_gets.fetch_add(1, Ordering::SeqCst);
            let cert = (call > 0).then_some(state.target_cert_b64.as_str());
            axum::Json(csr_obj(&name, if call == 0 { "10" } else { "12" }, cert))
        }

        async fn watch_csrs(State(state): State<CsrWaitState>) -> Response<Body> {
            let unrelated = json!({
                "type": "MODIFIED",
                "object": csr_obj("other-csr", "11", Some(&state.other_cert_b64))
            });
            let mut body = serde_json::to_vec(&unrelated).unwrap();
            body.push(b'\n');
            Response::builder().body(Body::from(body)).unwrap()
        }

        let target_cert = "-----BEGIN CERTIFICATE-----\nTARGET\n-----END CERTIFICATE-----\n";
        let other_cert = "-----BEGIN CERTIFICATE-----\nOTHER\n-----END CERTIFICATE-----\n";
        let state = CsrWaitState {
            named_gets: Arc::new(AtomicUsize::new(0)),
            target_cert_b64: general_purpose::STANDARD.encode(target_cert.as_bytes()),
            other_cert_b64: general_purpose::STANDARD.encode(other_cert.as_bytes()),
        };
        let named_gets = state.named_gets.clone();
        let app = axum::Router::new()
            .route(
                "/apis/certificates.k8s.io/v1/certificatesigningrequests/{name}",
                get(get_named_csr),
            )
            .route(
                "/apis/certificates.k8s.io/v1/certificatesigningrequests",
                get(watch_csrs),
            )
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let client = HttpCsrBootstrapClient {
            client: reqwest::Client::new(),
            base_url,
            token: "bootstrap-token".to_string(),
        };

        let cert = client.wait_for_certificate("target-csr").await.unwrap();

        assert_eq!(cert, target_cert);
        assert!(
            named_gets.load(Ordering::SeqCst) >= 2,
            "wait_for_certificate must re-read the named CSR after a watch closes"
        );
        server.abort();
    }

    // --- Filesystem credential store tests ---

    struct FsTestContext {
        _dir: tempfile::TempDir,
        store: FilesystemWorkerCredentialStore,
    }

    fn fs_store_for_test(node_name: &str) -> FsTestContext {
        let dir = tempfile::tempdir().unwrap();
        let etc = dir.path().join("etc");
        let store = FilesystemWorkerCredentialStore::new(etc, node_name);
        FsTestContext { _dir: dir, store }
    }

    #[test]
    fn test_filesystem_store_save_and_load_returns_credential() {
        let ctx = fs_store_for_test("tokyo");
        let cred = sample_credential("tokyo");
        ctx.store.save(&cred).unwrap();
        let loaded = ctx.store.load().unwrap().unwrap();
        assert_eq!(loaded.certificate_pem, cred.certificate_pem);
        assert_eq!(loaded.private_key_pem, cred.private_key_pem);
        assert_eq!(loaded.node_name, "tokyo");
    }

    #[cfg(unix)]
    #[test]
    fn test_filesystem_store_saves_node_private_key_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let ctx = fs_store_for_test("tokyo");
        ctx.store.save(&sample_credential("tokyo")).unwrap();

        let mode = std::fs::metadata(ctx.store.key_path())
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn test_filesystem_store_load_empty_returns_none() {
        let ctx = fs_store_for_test("tokyo");
        assert!(ctx.store.load().unwrap().is_none());
    }

    #[test]
    fn test_filesystem_store_delete_removes_files() {
        let ctx = fs_store_for_test("tokyo");
        ctx.store.save(&sample_credential("tokyo")).unwrap();
        assert!(ctx.store.cert_path().exists());
        assert!(ctx.store.key_path().exists());
        ctx.store.delete().unwrap();
        assert!(!ctx.store.cert_path().exists());
        assert!(!ctx.store.key_path().exists());
        assert!(ctx.store.load().unwrap().is_none());
    }

    #[test]
    fn test_filesystem_store_kubeconfig_persisted_when_present() {
        let ctx = fs_store_for_test("tokyo");
        let cred = sample_credential("tokyo");
        ctx.store.save(&cred).unwrap();
        let loaded = ctx.store.load().unwrap().unwrap();
        assert!(!loaded.kubeconfig_yaml.is_empty());
        assert!(loaded.kubeconfig_yaml.contains("current-context"));
    }

    #[test]
    fn test_filesystem_store_empty_kubeconfig_not_written() {
        let ctx = fs_store_for_test("tokyo");
        let mut cred = sample_credential("tokyo");
        cred.kubeconfig_yaml = String::new();
        ctx.store.save(&cred).unwrap();
        assert!(!ctx.store.kubeconfig_path().exists());
        let loaded = ctx.store.load().unwrap().unwrap();
        assert!(loaded.kubeconfig_yaml.is_empty());
    }

    #[tokio::test]
    async fn test_supervised_filesystem_store_save_load_delete_returns_credential() {
        let dir = tempfile::tempdir().unwrap();
        let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let store = SupervisedFilesystemWorkerCredentialStore::new(
            dir.path().join("etc"),
            "tokyo",
            supervisor.clone(),
        );
        let cred = sample_credential("tokyo");

        store.save(&cred).await.unwrap();
        let loaded = store.load().await.unwrap().unwrap();
        assert_eq!(loaded.certificate_pem, cred.certificate_pem);
        assert_eq!(loaded.private_key_pem, cred.private_key_pem);

        store.delete().await.unwrap();
        assert!(store.load().await.unwrap().is_none());
        let _ = supervisor.shutdown(std::time::Duration::from_secs(1)).await;
    }

    #[test]
    fn test_filesystem_store_resolve_credential_with_valid_cert() {
        let ctx = fs_store_for_test("tokyo");
        let (cert_pem, key_pem) = generate_test_cert("tokyo", &["system:nodes"]);
        let cred = WorkerCredential {
            certificate_pem: cert_pem,
            private_key_pem: key_pem,
            node_name: "tokyo".to_string(),
            kubeconfig_yaml: "...".to_string(),
        };
        ctx.store.save(&cred).unwrap();
        match resolve_credential(&ctx.store).unwrap() {
            CredentialSource::ExistingCert(c) => assert_eq!(c.node_name, "tokyo"),
            _ => panic!("expected ExistingCert"),
        }
    }
}
