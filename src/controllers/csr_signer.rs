//! Event-driven CSR signer controller for kubelet TLS bootstrap.
//!
//! Watches CSR create/update events and auto-approves + signs valid
//! kubelet client CSRs. Thin orchestrator over `BootstrapCsrPolicy` and
//! `CsrSigner` — no policy logic or signing logic inline.
//!
//! Pure OO design: the signer is injected via trait, making the
//! controller fully unit-testable with a mock signer and in-memory
//! datastore.

use crate::auth::clock::{Clock, SystemClock};
use crate::auth::csr_policy::{
    KubeletClientCsrValidationInput, validate_kubelet_client_csr_request,
};
use crate::auth::csr_signer::{CsrSigner, SignRequest};
use crate::controller::{Context, Controller};
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;

/// CSR signer controller that validates and signs kubelet client CSRs.
///
/// The injected `CsrSigner` trait makes signing mockable in tests.
pub struct CsrSignerController {
    signer: Arc<dyn CsrSigner>,
    clock: Arc<dyn Clock>,
}

impl CsrSignerController {
    pub fn new(signer: Arc<dyn CsrSigner>) -> Self {
        Self::new_with_clock(signer, Arc::new(SystemClock))
    }

    pub fn new_with_clock(signer: Arc<dyn CsrSigner>, clock: Arc<dyn Clock>) -> Self {
        Self { signer, clock }
    }
}

#[async_trait]
impl Controller for CsrSignerController {
    fn name(&self) -> &'static str {
        "certificatesigningrequest"
    }

    async fn reconcile(&self, resource: Value, ctx: Context) -> anyhow::Result<()> {
        let csr_name = extract_name(&resource);
        let live_resource = match ctx
            .db_handle()
            .get_resource(API_VERSION, KIND, None, &csr_name)
            .await?
        {
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

        // Validate using the pure policy object
        let validation = validate_kubelet_client_csr_request(KubeletClientCsrValidationInput {
            signer_name: &signer_name,
            csr_pem: &csr_pem,
            usages: &usages,
            username: &username,
            groups: &groups,
            expiration_seconds,
        });
        if !validation.valid {
            tracing::info!("CSR {csr_name} rejected by policy: {}", validation.reason);
            return Ok(());
        }

        let node_name = match validation.node_name {
            Some(n) => n,
            None => return Ok(()),
        };

        // Sign via the injected signer (mockable!)
        let sign_request = SignRequest {
            csr_pem,
            common_name: format!("system:node:{node_name}"),
            organizations: vec!["system:nodes".to_string()],
            usages,
            ttl_seconds: validation.ttl_seconds,
        };

        let result = match self.signer.sign(sign_request) {
            Ok(r) => r,
            Err(err) => {
                tracing::error!("failed to sign CSR {csr_name}: {err}");
                return Err(anyhow::anyhow!("signing failed: {err}"));
            }
        };

        // Update CSR status with certificate and approval
        update_csr_with_certificate(
            ctx.db_handle().as_ref(),
            &csr_name,
            &uid,
            resource_version,
            &result.certificate_pem,
            self.clock.now(),
        )
        .await?;

        tracing::info!("CSR {csr_name} signed for node {node_name}");
        Ok(())
    }
}

// --- Helper functions (private to this module) ---

const API_VERSION: &str = "certificates.k8s.io/v1";
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

async fn update_csr_with_certificate(
    db: &dyn crate::datastore::backend::DatastoreBackend,
    csr_name: &str,
    uid: &str,
    resource_version: i64,
    certificate_pem: &str,
    now: time::OffsetDateTime,
) -> anyhow::Result<()> {
    let existing = db.get_resource(API_VERSION, KIND, None, csr_name).await?;

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

    db.update_status_only_with_preconditions(
        API_VERSION,
        KIND,
        None,
        csr_name,
        status,
        crate::datastore::ResourcePreconditions {
            resource_version: Some(resource_version),
            uid: Some(uid.to_string()),
        },
    )
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::csr_signer::RecordingCsrSigner;
    use async_trait::async_trait;
    use base64::Engine;
    use serde_json::json;

    fn as_handle(
        db: &crate::datastore::sqlite::Datastore,
    ) -> crate::datastore::backend::DatastoreHandle {
        Arc::new(db.clone()) as crate::datastore::backend::DatastoreHandle
    }

    async fn raft_handle() -> crate::datastore::backend::DatastoreHandle {
        use crate::datastore::backend::DatastoreHandle;
        use crate::datastore::command::StorageCommand;
        use crate::datastore::replicated::{RaftProposer, ReplicatedDatastore, ReplicationMode};

        struct InlineProposer {
            inner: DatastoreHandle,
        }

        #[async_trait]
        impl RaftProposer for InlineProposer {
            async fn propose_command(&self, command: StorageCommand) -> anyhow::Result<()> {
                let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
                    .encode_protobuf()?;
                let key = format!("csr-signer-test-{}", uuid::Uuid::new_v4());
                crate::datastore::raft::state_machine::propose_outbox_on_backend(
                    self.inner.as_ref(),
                    &key,
                    crate::kubelet::outbox::payload::OutboxOperation::PodStatus,
                    bytes::Bytes::from(payload),
                    "csr-signer-test",
                )
                .await
                .map_err(|err| anyhow::anyhow!("inline raft propose failed: {err}"))?;
                Ok(())
            }

            async fn propose_outbox_command(
                &self,
                idempotency_key: &str,
                operation: &str,
                command: StorageCommand,
                authoring_node: &str,
            ) -> std::result::Result<
                crate::kubelet::outbox::OutboxApplyResult,
                crate::kubelet::outbox::OutboxApplyError,
            > {
                let payload = crate::kubelet::outbox::payload::OutboxPayload::from_command(command)
                    .encode_protobuf()
                    .map_err(|err| {
                        crate::kubelet::outbox::OutboxApplyError::Retryable(err.to_string())
                    })?;
                let operation =
                    crate::kubelet::outbox::payload::OutboxOperation::try_from(operation).map_err(
                        |err| crate::kubelet::outbox::OutboxApplyError::Retryable(err.to_string()),
                    )?;
                crate::datastore::raft::state_machine::propose_outbox_on_backend(
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
        let ds = ReplicatedDatastore::new(
            inner.clone(),
            ReplicationMode::Raft {
                node_name: "test-node".to_string(),
            },
        );
        ds.set_raft_proposer(Arc::new(InlineProposer { inner }));
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

        let signer = Arc::new(RecordingCsrSigner::new());
        let controller = CsrSignerController::new(signer.clone());
        let ctx = Context::new(handle.clone(), "test-node".to_string());

        controller.reconcile(csr, ctx).await.unwrap();

        // Verify the signer was called
        assert_eq!(signer.request_count(), 1);
        let requests = signer.take_requests();
        assert_eq!(requests[0].common_name, "system:node:tokyo");
        assert!(
            requests[0]
                .organizations
                .contains(&"system:nodes".to_string())
        );

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

        let signer = Arc::new(RecordingCsrSigner::new());
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

        let signer = Arc::new(RecordingCsrSigner::new());
        let fixed_now =
            time::OffsetDateTime::from_unix_timestamp(1_704_067_200).expect("valid timestamp");
        let controller = CsrSignerController::new_with_clock(
            signer,
            Arc::new(crate::auth::clock::FixedClock { now: fixed_now }),
        );
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

        let signer = Arc::new(RecordingCsrSigner::new());
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

        let signer = Arc::new(RecordingCsrSigner::new());
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

        let signer = Arc::new(RecordingCsrSigner::new());
        let controller = CsrSignerController::new(signer.clone());
        let ctx = Context::new(handle.clone(), "test-node".to_string());

        controller.reconcile(csr, ctx).await.unwrap();

        // Signer should NOT be called for invalid CSR
        assert_eq!(signer.request_count(), 0);
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

        let signer = Arc::new(RecordingCsrSigner::new());
        let controller = CsrSignerController::new(signer.clone());
        let ctx = Context::new(handle.clone(), "test-node".to_string());

        controller.reconcile(csr, ctx).await.unwrap();

        assert_eq!(signer.request_count(), 0);
    }

    #[tokio::test]
    async fn controller_name_is_correct() {
        let signer = Arc::new(RecordingCsrSigner::new());
        let controller = CsrSignerController::new(signer);
        assert_eq!(controller.name(), "certificatesigningrequest");
    }
}
