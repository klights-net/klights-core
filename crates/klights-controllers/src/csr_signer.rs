//! Event-driven CSR signer controller for kubelet TLS bootstrap.
//!
//! Watches CSR create/update events and auto-approves + signs valid
//! kubelet client CSRs. Certificate policy and signing are supplied through a
//! focused root adapter — no auth implementation or signing logic is inline.
//!
//! Pure OO design: the issuer is injected via trait, making the
//! controller fully unit-testable with a mock signer and in-memory
//! datastore.

use async_trait::async_trait;
use klights_cluster_core::Resource;
use klights_reconcile_api::ControllerStoreResult;
use serde_json::Value;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct CsrIssuanceRequest {
    pub signer_name: String,
    pub csr_pem: Vec<u8>,
    pub usages: Vec<String>,
    pub username: String,
    pub groups: Vec<String>,
    pub expiration_seconds: Option<u32>,
}

#[derive(Clone, Debug)]
pub struct IssuedCsr {
    pub node_name: String,
    pub certificate_pem: String,
    pub issued_at: time::OffsetDateTime,
}

#[derive(Clone, Debug)]
pub enum CsrIssuanceOutcome {
    Issued(IssuedCsr),
    Rejected { reason: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CsrIssuanceError {
    DependencyFailure { message: String },
    InternalFailure { message: String },
}

impl std::fmt::Display for CsrIssuanceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::DependencyFailure { message } | Self::InternalFailure { message } => message,
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for CsrIssuanceError {}

/// Consumer-owned capability joining certificate policy, signing, and time.
#[async_trait]
pub trait CsrIssuer: Send + Sync {
    async fn issue(
        &self,
        request: CsrIssuanceRequest,
    ) -> Result<CsrIssuanceOutcome, CsrIssuanceError>;
}

#[async_trait]
pub trait CsrStatusStore: Send + Sync {
    async fn get_csr(&self, name: &str) -> ControllerStoreResult<Option<Resource>>;

    async fn update_csr_status(
        &self,
        name: &str,
        uid: &str,
        resource_version: i64,
        status: Value,
    ) -> ControllerStoreResult<()>;
}

/// CSR signer controller that validates and signs kubelet client CSRs.
///
/// The injected [`CsrIssuer`] keeps auth policy and key material in the root
/// adapter while retaining a deterministic controller seam.
pub struct CsrSignerController {
    issuer: Arc<dyn CsrIssuer>,
}

impl CsrSignerController {
    pub fn new(issuer: Arc<dyn CsrIssuer>) -> Self {
        Self { issuer }
    }
}

impl CsrSignerController {
    pub async fn reconcile(
        &self,
        store: &dyn CsrStatusStore,
        resource: Value,
    ) -> anyhow::Result<()> {
        let csr_name = extract_name(&resource);
        let live_resource = match store.get_csr(&csr_name).await? {
            Some(resource) => resource,
            None => return Ok(()),
        };
        let resource_version = live_resource.resource_version;
        let uid = live_resource.uid.clone();
        let resource = Arc::unwrap_or_clone(live_resource.data);

        if has_deletion_timestamp(&resource) {
            return Ok(());
        }

        // Only process pending CSRs
        if !is_csr_pending(&resource) {
            return Ok(());
        }

        let signer_name = extract_signer_name(&resource);
        let csr_pem = match extract_csr_request(&resource) {
            Some(p) => p,
            None => return Ok(()),
        };
        let usages = extract_usages(&resource);
        let username = extract_username(&resource);
        let groups = extract_groups(&resource);
        let expiration_seconds = extract_expiration_seconds(&resource);

        let issuance = self
            .issuer
            .issue(CsrIssuanceRequest {
                signer_name,
                csr_pem,
                usages,
                username,
                groups,
                expiration_seconds,
            })
            .await;
        let issued = match issuance {
            Ok(CsrIssuanceOutcome::Issued(issued)) => issued,
            Ok(CsrIssuanceOutcome::Rejected { reason }) => {
                tracing::info!("CSR {csr_name} rejected by policy: {reason}");
                return Ok(());
            }
            Err(err) => {
                tracing::error!("failed to sign CSR {csr_name}: {err}");
                return Err(anyhow::anyhow!("signing failed: {err}"));
            }
        };

        // Update CSR status with certificate and approval
        update_csr_with_certificate(
            store,
            &csr_name,
            &uid,
            resource_version,
            &issued.certificate_pem,
            issued.issued_at,
        )
        .await?;

        tracing::info!("CSR {csr_name} signed for node {}", issued.node_name);
        Ok(())
    }
}

// --- Helper functions (private to this module) ---

fn is_csr_pending(csr: &Value) -> bool {
    // CSR is pending if no certificate has been issued
    let status = csr.get("status");
    let certificate = status
        .and_then(|s| s.get("certificate"))
        .and_then(|c| c.as_str());
    certificate.is_none() || certificate == Some("")
}

fn has_deletion_timestamp(csr: &Value) -> bool {
    csr.pointer("/metadata/deletionTimestamp")
        .and_then(|v| v.as_str())
        .is_some_and(|s| !s.is_empty())
}

fn extract_signer_name(csr: &Value) -> String {
    csr.get("spec")
        .and_then(|s| s.get("signerName"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

fn extract_csr_request(csr: &Value) -> Option<Vec<u8>> {
    let b64 = csr
        .get("spec")
        .and_then(|s| s.get("request"))
        .and_then(|v| v.as_str())?;

    use base64::Engine;
    base64::engine::general_purpose::STANDARD.decode(b64).ok()
}

fn extract_usages(csr: &Value) -> Vec<String> {
    csr.get("spec")
        .and_then(|s| s.get("usages"))
        .and_then(|u| u.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

fn extract_username(csr: &Value) -> String {
    csr.get("spec")
        .and_then(|s| s.get("username"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

fn extract_groups(csr: &Value) -> Vec<String> {
    csr.get("spec")
        .and_then(|s| s.get("groups"))
        .and_then(|u| u.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

fn extract_expiration_seconds(csr: &Value) -> Option<u32> {
    let value = csr.get("spec").and_then(|s| s.get("expirationSeconds"))?;
    match value.as_u64().and_then(|n| u32::try_from(n).ok()) {
        Some(n) => Some(n),
        None => Some(0),
    }
}

fn extract_name(csr: &Value) -> String {
    csr.get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string()
}

async fn update_csr_with_certificate<S: CsrStatusStore + ?Sized>(
    store: &S,
    csr_name: &str,
    uid: &str,
    resource_version: i64,
    certificate_pem: &str,
    now: time::OffsetDateTime,
) -> anyhow::Result<()> {
    let existing = store.get_csr(csr_name).await?;

    let Some(existing) = existing else {
        return Ok(());
    };
    if existing.uid != uid || existing.resource_version != resource_version {
        return Ok(());
    }

    let csr = Arc::unwrap_or_clone(existing.data);
    if has_deletion_timestamp(&csr) || !is_csr_pending(&csr) {
        return Ok(());
    }

    let now = now
        .replace_nanosecond(0)
        .map_err(|err| anyhow::anyhow!("failed to normalize CSR timestamp: {err}"))?;
    let now_str = now
        .format(&time::format_description::well_known::Rfc3339)
        .map_err(|err| anyhow::anyhow!("failed to format CSR timestamp: {err}"))?;

    // Build approval + issued conditions
    let conditions = serde_json::json!([
        {
            "type": "Approved",
            "status": "True",
            "reason": "AutoApproved",
            "message": "Auto-approved by klights CSR signer",
            "lastUpdateTime": now_str,
        },
        {
            "type": "Failed",
            "status": "False",
            "reason": "NotFailed",
            "message": "",
            "lastUpdateTime": now_str,
        },
    ]);

    // K8s expects status.certificate to be base64-encoded bytes
    use base64::Engine;
    let cert_b64 = base64::engine::general_purpose::STANDARD.encode(certificate_pem.as_bytes());

    let status = serde_json::json!({
        "certificate": cert_b64,
        "conditions": conditions,
    });

    store
        .update_csr_status(csr_name, uid, resource_version, status)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use klights_cluster_core::Resource;
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    struct RecordingIssuer {
        requests: Mutex<Vec<CsrIssuanceRequest>>,
        outcome: CsrIssuanceOutcome,
    }

    impl RecordingIssuer {
        fn issuing(at: time::OffsetDateTime) -> Self {
            Self {
                requests: Mutex::new(Vec::new()),
                outcome: CsrIssuanceOutcome::Issued(IssuedCsr {
                    node_name: "tokyo".into(),
                    certificate_pem:
                        "-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----\n".into(),
                    issued_at: at,
                }),
            }
        }

        fn rejecting(reason: &str) -> Self {
            Self {
                requests: Mutex::new(Vec::new()),
                outcome: CsrIssuanceOutcome::Rejected {
                    reason: reason.into(),
                },
            }
        }

        fn request_count(&self) -> usize {
            self.requests.lock().unwrap().len()
        }

        fn take_requests(&self) -> Vec<CsrIssuanceRequest> {
            std::mem::take(&mut self.requests.lock().unwrap())
        }
    }

    #[async_trait]
    impl CsrIssuer for RecordingIssuer {
        async fn issue(
            &self,
            request: CsrIssuanceRequest,
        ) -> Result<CsrIssuanceOutcome, CsrIssuanceError> {
            self.requests.lock().unwrap().push(request);
            Ok(self.outcome.clone())
        }
    }

    #[derive(Clone)]
    struct MemoryStore {
        resource: Arc<Mutex<Option<Resource>>>,
    }

    impl MemoryStore {
        fn new(mut value: Value) -> Self {
            value["metadata"]["uid"] = json!("csr-uid");
            value["metadata"]["resourceVersion"] = json!("7");
            Self {
                resource: Arc::new(Mutex::new(Some(
                    Resource::try_from_data(Arc::new(value)).unwrap(),
                ))),
            }
        }

        fn value(&self) -> Value {
            Arc::unwrap_or_clone(
                self.resource
                    .lock()
                    .unwrap()
                    .as_ref()
                    .expect("CSR exists")
                    .data
                    .clone(),
            )
        }

        fn replace(&self, mut value: Value) {
            value["metadata"]["uid"] = json!("csr-uid");
            value["metadata"]["resourceVersion"] = json!("8");
            *self.resource.lock().unwrap() =
                Some(Resource::try_from_data(Arc::new(value)).unwrap());
        }
    }

    #[async_trait]
    impl CsrStatusStore for MemoryStore {
        async fn get_csr(&self, name: &str) -> ControllerStoreResult<Option<Resource>> {
            Ok(self
                .resource
                .lock()
                .unwrap()
                .as_ref()
                .filter(|resource| resource.name == name)
                .cloned())
        }

        async fn update_csr_status(
            &self,
            name: &str,
            uid: &str,
            resource_version: i64,
            status: Value,
        ) -> ControllerStoreResult<()> {
            let mut guard = self.resource.lock().unwrap();
            let current = guard.as_ref().expect("CSR exists");
            assert_eq!(current.name, name);
            assert_eq!(current.uid, uid);
            assert_eq!(current.resource_version, resource_version);
            let mut value = Arc::unwrap_or_clone(current.data.clone());
            value["status"] = status;
            *guard = Some(Resource::try_from_data(Arc::new(value)).unwrap());
            Ok(())
        }
    }

    fn issuing() -> Arc<RecordingIssuer> {
        Arc::new(RecordingIssuer::issuing(time::OffsetDateTime::UNIX_EPOCH))
    }

    fn valid_csr_json() -> Value {
        let request_b64 = base64::engine::general_purpose::STANDARD.encode(b"fake-csr-request");
        json!({
            "apiVersion": "certificates.k8s.io/v1",
            "kind": "CertificateSigningRequest",
            "metadata": {"name": "csr-tokyo"},
            "spec": {
                "request": request_b64,
                "signerName": "kubernetes.io/kube-apiserver-client-kubelet",
                "usages": ["client auth"],
                "username": "system:bootstrap:abcdef",
                "groups": ["system:bootstrappers", "system:bootstrappers:klights:worker"]
            },
            "status": {}
        })
    }

    fn already_signed_csr() -> Value {
        let mut csr = valid_csr_json();
        csr["status"] = json!({"certificate": "existing-cert"});
        csr
    }

    #[tokio::test]
    async fn valid_csr_is_signed_and_status_updated() {
        let store = MemoryStore::new(valid_csr_json());
        let signer = issuing();
        CsrSignerController::new(signer.clone())
            .reconcile(&store, valid_csr_json())
            .await
            .unwrap();

        assert_eq!(signer.request_count(), 1);
        let requests = signer.take_requests();
        assert_eq!(
            requests[0].signer_name,
            "kubernetes.io/kube-apiserver-client-kubelet"
        );
        assert_eq!(requests[0].username, "system:bootstrap:abcdef");
        let updated = store.value();
        let cert = updated
            .pointer("/status/certificate")
            .unwrap()
            .as_str()
            .unwrap();
        assert!(
            String::from_utf8(
                base64::engine::general_purpose::STANDARD
                    .decode(cert)
                    .unwrap()
            )
            .unwrap()
            .contains("CERTIFICATE")
        );
        assert_eq!(updated["status"]["conditions"][0]["type"], "Approved");
    }

    #[tokio::test]
    async fn csr_status_conditions_use_injected_clock() {
        let store = MemoryStore::new(valid_csr_json());
        let fixed_now = time::OffsetDateTime::from_unix_timestamp(1_704_067_200).unwrap();
        CsrSignerController::new(Arc::new(RecordingIssuer::issuing(fixed_now)))
            .reconcile(&store, valid_csr_json())
            .await
            .unwrap();
        let updated = store.value();
        assert_eq!(
            updated["status"]["conditions"][0]["lastUpdateTime"],
            "2024-01-01T00:00:00Z"
        );
        assert_eq!(
            updated["status"]["conditions"][1]["lastUpdateTime"],
            "2024-01-01T00:00:00Z"
        );
    }

    #[tokio::test]
    async fn already_signed_csr_is_skipped() {
        let signed = already_signed_csr();
        let store = MemoryStore::new(signed.clone());
        let signer = issuing();
        CsrSignerController::new(signer.clone())
            .reconcile(&store, signed)
            .await
            .unwrap();
        assert_eq!(signer.request_count(), 0);
    }

    #[tokio::test]
    async fn stale_pending_csr_snapshot_is_skipped_when_live_csr_is_already_signed() {
        let stale = valid_csr_json();
        let store = MemoryStore::new(stale.clone());
        let mut live = stale.clone();
        live["status"] = json!({"certificate": "existing-cert"});
        store.replace(live);
        let signer = issuing();
        CsrSignerController::new(signer.clone())
            .reconcile(&store, stale)
            .await
            .unwrap();
        assert_eq!(signer.request_count(), 0);
        assert_eq!(store.value()["status"]["certificate"], "existing-cert");
    }

    #[tokio::test]
    async fn rejected_csr_is_not_persisted() {
        let store = MemoryStore::new(valid_csr_json());
        let signer = Arc::new(RecordingIssuer::rejecting("policy rejected"));
        CsrSignerController::new(signer.clone())
            .reconcile(&store, valid_csr_json())
            .await
            .unwrap();
        assert_eq!(signer.request_count(), 1);
        assert!(store.value()["status"]["certificate"].is_null());
    }

    #[tokio::test]
    async fn wrong_signer_name_is_forwarded_to_policy() {
        let mut csr = valid_csr_json();
        csr["spec"]["signerName"] = json!("kubernetes.io/other-signer");
        let store = MemoryStore::new(csr.clone());
        let signer = Arc::new(RecordingIssuer::rejecting("wrong signer"));
        CsrSignerController::new(signer.clone())
            .reconcile(&store, csr)
            .await
            .unwrap();
        assert_eq!(signer.request_count(), 1);
        assert_eq!(
            signer.take_requests()[0].signer_name,
            "kubernetes.io/other-signer"
        );
    }

    #[tokio::test]
    async fn deleted_csr_is_skipped_before_issuance() {
        let mut csr = valid_csr_json();
        csr["metadata"]["deletionTimestamp"] = json!("2026-01-01T00:00:00Z");
        let store = MemoryStore::new(csr.clone());
        let signer = issuing();
        CsrSignerController::new(signer.clone())
            .reconcile(&store, csr)
            .await
            .unwrap();
        assert_eq!(signer.request_count(), 0);
    }
}
