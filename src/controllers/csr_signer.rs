//! Root composition adapter for the engine-neutral CSR signer policy.

use async_trait::async_trait;
use klights_controllers::csr_signer::CsrIssuer;
use serde_json::Value;
use std::sync::Arc;

use super::{Context, Controller};

pub struct CsrSignerController {
    policy: klights_controllers::csr_signer::CsrSignerController,
}

impl CsrSignerController {
    pub fn new(issuer: Arc<dyn CsrIssuer>) -> Self {
        Self {
            policy: klights_controllers::csr_signer::CsrSignerController::new(issuer),
        }
    }
}

#[async_trait]
impl Controller for CsrSignerController {
    fn name(&self) -> &'static str {
        "certificatesigningrequest"
    }

    async fn reconcile(&self, resource: Value, ctx: Context) -> anyhow::Result<()> {
        self.policy
            .reconcile(ctx.csr_status_store(), resource)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use base64::Engine as _;
    use klights_controllers::csr_signer::{
        CsrIssuanceError, CsrIssuanceOutcome, CsrIssuanceRequest, CsrIssuer, IssuedCsr,
    };
    use std::sync::Mutex;

    struct RecordingIssuer {
        requests: Mutex<Vec<CsrIssuanceRequest>>,
    }

    #[async_trait]
    impl CsrIssuer for RecordingIssuer {
        async fn issue(
            &self,
            request: CsrIssuanceRequest,
        ) -> Result<CsrIssuanceOutcome, CsrIssuanceError> {
            self.requests.lock().unwrap().push(request);
            Ok(CsrIssuanceOutcome::Issued(IssuedCsr {
                node_name: "worker-a".to_string(),
                certificate_pem: "-----BEGIN CERTIFICATE-----\nTEST\n-----END CERTIFICATE-----"
                    .to_string(),
                issued_at: time::OffsetDateTime::UNIX_EPOCH,
            }))
        }
    }

    fn issuer() -> Arc<RecordingIssuer> {
        Arc::new(RecordingIssuer {
            requests: Mutex::new(Vec::new()),
        })
    }

    fn valid_csr_json() -> Value {
        serde_json::json!({
            "apiVersion": "certificates.k8s.io/v1",
            "kind": "CertificateSigningRequest",
            "metadata": {"name": "csr-raft"},
            "spec": {
                "request": base64::engine::general_purpose::STANDARD.encode(b"fake-csr"),
                "signerName": "kubernetes.io/kube-apiserver-client-kubelet",
                "usages": ["client auth"],
                "username": "system:bootstrap:abcdef",
                "groups": ["system:bootstrappers"]
            },
            "status": {}
        })
    }

    async fn raft_handle() -> crate::datastore::backend::DatastoreHandle {
        use crate::bootstrap::sequenced_datastore::SequencedDatastore;
        use klights_cluster_core::StorageCommand;
        use klights_replication::proposal::RaftProposal;

        struct InlineProposer {
            inner: crate::datastore::backend::DatastoreHandle,
        }

        #[async_trait]
        impl RaftProposal for InlineProposer {
            async fn propose_command(
                &self,
                command: StorageCommand,
            ) -> anyhow::Result<klights_cluster_store::StorageCommandResult> {
                crate::bootstrap::outbox_apply_adapter::propose_command_on_backend(
                    self.inner.as_ref(),
                    command,
                )
                .await
                .map_err(|error| anyhow::anyhow!("inline raft proposal failed: {error}"))
            }

            async fn propose_outbox_command(
                &self,
                idempotency_key: &str,
                operation: &str,
                command: StorageCommand,
                authoring_node: &str,
                _watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
            ) -> Result<
                klights_cluster_core::OutboxApplyOutcome,
                klights_cluster_core::OutboxApplyError,
            > {
                let operation =
                    klights_kubelet::node_outbox::payload::OutboxOperation::try_from(operation)
                        .map_err(|error| {
                            klights_cluster_core::OutboxApplyError::Retryable(error.to_string())
                        })?;
                crate::bootstrap::outbox_apply_adapter::propose_outbox_command_on_backend(
                    self.inner.as_ref(),
                    idempotency_key,
                    operation,
                    command,
                    authoring_node,
                    None,
                )
                .await
                .map(|outcome| outcome.into_parts().0)
            }
        }

        let inner: crate::datastore::backend::DatastoreHandle =
            Arc::new(crate::datastore::test_support::in_memory().await);
        Arc::new(SequencedDatastore::new(
            inner.clone(),
            Arc::new(InlineProposer { inner }),
        ))
    }

    #[tokio::test]
    async fn adapter_persists_signed_csr_through_raft_backend() {
        let handle = raft_handle().await;
        let csr = valid_csr_json();
        handle
            .create_resource(
                "certificates.k8s.io/v1",
                "CertificateSigningRequest",
                None,
                "csr-raft",
                csr.clone(),
            )
            .await
            .unwrap();

        CsrSignerController::new(issuer())
            .reconcile(csr, Context::new(handle.clone(), "test-node".to_string()))
            .await
            .unwrap();

        let signed = handle
            .get_resource(
                "certificates.k8s.io/v1",
                "CertificateSigningRequest",
                None,
                "csr-raft",
            )
            .await
            .unwrap()
            .unwrap();
        assert!(signed.data.pointer("/status/certificate").is_some());
    }

    #[test]
    fn adapter_controller_name_is_stable() {
        let controller = CsrSignerController::new(issuer());
        assert_eq!(controller.name(), "certificatesigningrequest");
    }
}
