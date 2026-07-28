//! Event-driven CSR signer controller for kubelet TLS bootstrap.
//!
//! Watches CSR create/update events and auto-approves + signs valid
//! kubelet client CSRs. Certificate policy and signing are supplied through a
//! focused root adapter — no auth implementation or signing logic is inline.
//!
//! Pure OO design: the issuer is injected via trait, making the
//! controller fully unit-testable with a mock signer and in-memory
//! datastore.

use crate::controllers::{Context, Controller};
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
pub(crate) trait CsrStatusStore: Send + Sync {
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

#[async_trait]
impl Controller for CsrSignerController {
    fn name(&self) -> &'static str {
        "certificatesigningrequest"
    }

    async fn reconcile(&self, resource: Value, ctx: Context) -> anyhow::Result<()> {
        let csr_name = extract_name(&resource);
        let live_resource = match ctx.csr_status_store().get_csr(&csr_name).await? {
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
            ctx.csr_status_store(),
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

#[cfg(test)]
const API_VERSION: &str = "certificates.k8s.io/v1";
#[cfg(test)]
const KIND: &str = "CertificateSigningRequest";

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
    use async_trait::async_trait;
    use base64::Engine;
    use serde_json::json;

    struct RecordingIssuer {
        requests: std::sync::Mutex<Vec<CsrIssuanceRequest>>,
        outcome: CsrIssuanceOutcome,
    }

    impl RecordingIssuer {
        fn issuing(at: time::OffsetDateTime) -> Self {
            Self {
                requests: Default::default(),
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
                requests: Default::default(),
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

    fn issuing() -> Arc<RecordingIssuer> {
        Arc::new(RecordingIssuer::issuing(time::OffsetDateTime::UNIX_EPOCH))
    }

    fn as_handle(
        db: &crate::datastore::sqlite::Datastore,
    ) -> crate::datastore::backend::DatastoreHandle {
        Arc::new(db.clone()) as crate::datastore::backend::DatastoreHandle
    }

    async fn raft_handle() -> crate::datastore::backend::DatastoreHandle {
        use crate::bootstrap::sequenced_datastore::SequencedDatastore;
        use crate::datastore::backend::DatastoreHandle;
        use crate::datastore::raft::proposal::RaftProposal;
        use klights_cluster_core::StorageCommand;

        struct InlineProposer {
            inner: DatastoreHandle,
        }

        #[async_trait]
        impl RaftProposal for InlineProposer {
            async fn propose_command(
                &self,
                command: StorageCommand,
            ) -> anyhow::Result<crate::datastore::raft::types::StorageCommandResult> {
                let payload = crate::node_outbox::payload::OutboxPayload::from_command(command)
                    .encode_protobuf()?;
                let key = format!("csr-signer-test-{}", uuid::Uuid::new_v4());
                let outcome = crate::bootstrap::outbox_apply_adapter::propose_outbox_on_backend(
                    self.inner.as_ref(),
                    &key,
                    crate::node_outbox::payload::OutboxOperation::PodStatus,
                    bytes::Bytes::from(payload),
                    "csr-signer-test",
                )
                .await
                .map_err(|err| anyhow::anyhow!("inline raft propose failed: {err}"))?;
                Ok(crate::datastore::raft::types::StorageCommandResult {
                    applied_rv: outcome.applied_resource_version(),
                    error_message: None,
                    rejection_code: None,
                    public_resource_changed: false,
                    applied_mutation: None,
                    pod_endpoint_effect: Default::default(),
                })
            }

            async fn propose_outbox_command(
                &self,
                idempotency_key: &str,
                operation: &str,
                command: StorageCommand,
                authoring_node: &str,
                _watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
            ) -> std::result::Result<
                crate::node_outbox::OutboxApplyResult,
                crate::node_outbox::OutboxApplyError,
            > {
                let payload = crate::node_outbox::payload::OutboxPayload::from_command(command)
                    .encode_protobuf()
                    .map_err(|err| {
                        crate::node_outbox::OutboxApplyError::Retryable(err.to_string())
                    })?;
                let operation = crate::node_outbox::payload::OutboxOperation::try_from(operation)
                    .map_err(|err| {
                    crate::node_outbox::OutboxApplyError::Retryable(err.to_string())
                })?;
                crate::bootstrap::outbox_apply_adapter::propose_outbox_on_backend(
                    self.inner.as_ref(),
                    idempotency_key,
                    operation,
                    bytes::Bytes::from(payload),
                    authoring_node,
                )
                .await
                .map(|outcome| outcome.result)
            }
        }

        let inner: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        let ds = SequencedDatastore::new(inner.clone(), Arc::new(InlineProposer { inner }));
        Arc::new(ds)
    }

    fn valid_csr_json() -> serde_json::Value {
        // Generate a valid CSR PEM
        use rcgen::{CertificateParams, DnType, KeyPair};
        let mut params = CertificateParams::default();
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, "system:node:tokyo".to_string());
        params
            .distinguished_name
            .push(DnType::OrganizationName, "system:nodes".to_string());
        let key_pair = KeyPair::generate().unwrap();
        let csr = params.serialize_request(&key_pair).unwrap();
        let csr_pem = csr.pem().unwrap();

        use base64::Engine;
        let request_b64 = base64::engine::general_purpose::STANDARD.encode(csr_pem.as_bytes());

        json!({
            "apiVersion": "certificates.k8s.io/v1",
            "kind": "CertificateSigningRequest",
            "metadata": {
                "name": "csr-tokyo",
            },
            "spec": {
                "request": request_b64,
                "signerName": "kubernetes.io/kube-apiserver-client-kubelet",
                "usages": ["client auth"],
                "username": "system:bootstrap:abcdef",
                "groups": [
                    "system:bootstrappers",
                    "system:bootstrappers:klights:worker"
                ],
            },
            "status": {}
        })
    }

    fn csr_with_system_masters() -> serde_json::Value {
        use rcgen::{CertificateParams, DnType, KeyPair};
        let mut params = CertificateParams::default();
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, "system:node:bad".to_string());
        params
            .distinguished_name
            .push(DnType::OrganizationName, "system:masters".to_string());
        let key_pair = KeyPair::generate().unwrap();
        let csr = params.serialize_request(&key_pair).unwrap();
        let csr_pem = csr.pem().unwrap();

        use base64::Engine;
        let request_b64 = base64::engine::general_purpose::STANDARD.encode(csr_pem.as_bytes());

        json!({
            "apiVersion": "certificates.k8s.io/v1",
            "kind": "CertificateSigningRequest",
            "metadata": { "name": "csr-bad" },
            "spec": {
                "request": request_b64,
                "signerName": "kubernetes.io/kube-apiserver-client-kubelet",
                "usages": ["client auth"],
            },
            "status": {}
        })
    }

    fn already_signed_csr() -> serde_json::Value {
        let mut csr = valid_csr_json();
        csr["status"] = json!({
            "certificate": "-----BEGIN CERTIFICATE-----\nMIID...\n-----END CERTIFICATE-----",
        });
        csr
    }

    // --- Tests ---

    #[tokio::test]
    async fn valid_csr_is_signed_and_status_updated() {
        let db = crate::datastore::test_support::in_memory().await;
        let handle = as_handle(&db);

        // Create the CSR in the datastore
        let csr = valid_csr_json();
        db.create_resource(API_VERSION, KIND, None, "csr-tokyo", csr.clone())
            .await
            .unwrap();

        let signer = issuing();
        let controller = CsrSignerController::new(signer.clone());
        let ctx = Context::new(handle.clone(), "test-node".to_string());

        controller.reconcile(csr, ctx).await.unwrap();

        // Verify the signer was called
        assert_eq!(signer.request_count(), 1);
        let requests = signer.take_requests();
        assert_eq!(
            requests[0].signer_name,
            "kubernetes.io/kube-apiserver-client-kubelet"
        );
        assert_eq!(requests[0].username, "system:bootstrap:abcdef");

        // Verify status was updated
        let updated = handle
            .get_resource(API_VERSION, KIND, None, "csr-tokyo")
            .await
            .unwrap()
            .expect("CSR should exist");
        let cert_b64 = updated.data["status"]["certificate"].as_str().unwrap_or("");
        let cert_bytes = base64::engine::general_purpose::STANDARD
            .decode(cert_b64)
            .expect("certificate should be base64-encoded");
        let cert_str = std::str::from_utf8(&cert_bytes).unwrap();
        assert!(cert_str.contains("CERTIFICATE"));

        // Verify approval condition exists
        let conditions = updated.data["status"]["conditions"].as_array().unwrap();
        let approved = conditions.iter().find(|c| c["type"] == "Approved");
        assert!(approved.is_some());
        assert_eq!(approved.unwrap()["status"], "True");
    }

    #[tokio::test]
    async fn valid_csr_is_signed_and_status_updated_through_raft_backend() {
        let handle = raft_handle().await;

        let csr = valid_csr_json();
        handle
            .create_resource(API_VERSION, KIND, None, "csr-tokyo", csr.clone())
            .await
            .unwrap();

        let signer = issuing();
        let controller = CsrSignerController::new(signer.clone());
        let ctx = Context::new(handle.clone(), "test-node".to_string());

        controller.reconcile(csr, ctx).await.unwrap();

        assert_eq!(signer.request_count(), 1);
        let updated = handle
            .get_resource(API_VERSION, KIND, None, "csr-tokyo")
            .await
            .unwrap()
            .expect("CSR should exist");
        let cert_b64 = updated
            .data
            .pointer("/status/certificate")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        let cert_bytes = base64::engine::general_purpose::STANDARD
            .decode(cert_b64)
            .expect("certificate should be base64-encoded");
        let cert_str = std::str::from_utf8(&cert_bytes).unwrap();
        assert!(
            cert_str.contains("CERTIFICATE"),
            "raft-routed CSR signing must persist status.certificate"
        );
    }

    #[tokio::test]
    async fn csr_status_conditions_use_injected_clock() {
        let db = crate::datastore::test_support::in_memory().await;
        let handle = as_handle(&db);

        let csr = valid_csr_json();
        db.create_resource(API_VERSION, KIND, None, "csr-tokyo", csr.clone())
            .await
            .unwrap();

        let fixed_now =
            time::OffsetDateTime::from_unix_timestamp(1_704_067_200).expect("valid timestamp");
        let signer = Arc::new(RecordingIssuer::issuing(fixed_now));
        let controller = CsrSignerController::new(signer);
        let ctx = Context::new(handle.clone(), "test-node".to_string());

        controller.reconcile(csr, ctx).await.unwrap();

        let updated = handle
            .get_resource(API_VERSION, KIND, None, "csr-tokyo")
            .await
            .unwrap()
            .expect("CSR should exist");
        let conditions = updated.data["status"]["conditions"].as_array().unwrap();
        assert_eq!(conditions[0]["lastUpdateTime"], "2024-01-01T00:00:00Z");
        assert_eq!(conditions[1]["lastUpdateTime"], "2024-01-01T00:00:00Z");
    }

    #[tokio::test]
    async fn already_signed_csr_is_skipped() {
        let db = crate::datastore::test_support::in_memory().await;
        let handle = as_handle(&db);

        let csr = already_signed_csr();
        db.create_resource(API_VERSION, KIND, None, "csr-done", csr.clone())
            .await
            .unwrap();

        let signer = issuing();
        let controller = CsrSignerController::new(signer.clone());
        let ctx = Context::new(handle.clone(), "test-node".to_string());

        controller.reconcile(csr, ctx).await.unwrap();

        // Signer should NOT be called for already-signed CSR
        assert_eq!(signer.request_count(), 0);
    }

    #[tokio::test]
    async fn stale_pending_csr_snapshot_is_skipped_when_live_csr_is_already_signed() {
        let db = crate::datastore::test_support::in_memory().await;
        let handle = as_handle(&db);

        let stale_pending = valid_csr_json();
        db.create_resource(API_VERSION, KIND, None, "csr-tokyo", stale_pending.clone())
            .await
            .unwrap();

        let mut live_signed = stale_pending.clone();
        live_signed["status"] = json!({
            "certificate": "existing-cert",
            "conditions": [{
                "type": "Approved",
                "status": "True",
                "reason": "Existing",
                "message": "already signed",
                "lastUpdateTime": "2024-01-01T00:00:00Z"
            }]
        });
        let current = db
            .get_resource(API_VERSION, KIND, None, "csr-tokyo")
            .await
            .unwrap()
            .unwrap();
        db.update_resource(
            API_VERSION,
            KIND,
            None,
            "csr-tokyo",
            live_signed,
            current.resource_version,
        )
        .await
        .unwrap();

        let signer = issuing();
        let controller = CsrSignerController::new(signer.clone());
        let ctx = Context::new(handle.clone(), "test-node".to_string());

        controller.reconcile(stale_pending, ctx).await.unwrap();

        assert_eq!(
            signer.request_count(),
            0,
            "stale pending CSR events must not trigger signing after live CSR is signed"
        );
        let updated = handle
            .get_resource(API_VERSION, KIND, None, "csr-tokyo")
            .await
            .unwrap()
            .expect("CSR should still exist");
        assert_eq!(
            updated
                .data
                .pointer("/status/certificate")
                .and_then(|v| v.as_str()),
            Some("existing-cert"),
            "stale reconcile must not overwrite the live certificate"
        );
    }

    #[tokio::test]
    async fn csr_with_system_masters_is_not_signed() {
        let db = crate::datastore::test_support::in_memory().await;
        let handle = as_handle(&db);

        let csr = csr_with_system_masters();
        db.create_resource(API_VERSION, KIND, None, "csr-bad", csr.clone())
            .await
            .unwrap();

        let signer = Arc::new(RecordingIssuer::rejecting("policy rejected"));
        let controller = CsrSignerController::new(signer.clone());
        let ctx = Context::new(handle.clone(), "test-node".to_string());

        controller.reconcile(csr, ctx).await.unwrap();

        assert_eq!(signer.request_count(), 1);
    }

    #[tokio::test]
    async fn csr_with_wrong_signer_name_is_skipped() {
        let db = crate::datastore::test_support::in_memory().await;
        let handle = as_handle(&db);

        let mut csr = valid_csr_json();
        csr["spec"]["signerName"] = json!("kubernetes.io/other-signer");
        csr["metadata"]["name"] = json!("csr-other");
        db.create_resource(API_VERSION, KIND, None, "csr-other", csr.clone())
            .await
            .unwrap();

        let signer = Arc::new(RecordingIssuer::rejecting("wrong signer"));
        let controller = CsrSignerController::new(signer.clone());
        let ctx = Context::new(handle.clone(), "test-node".to_string());

        controller.reconcile(csr, ctx).await.unwrap();

        assert_eq!(signer.request_count(), 1);
    }

    #[tokio::test]
    async fn controller_name_is_correct() {
        let signer = issuing();
        let controller = CsrSignerController::new(signer);
        assert_eq!(controller.name(), "certificatesigningrequest");
    }
}
