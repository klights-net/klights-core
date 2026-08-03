//! `Controller` wrapper for the focused CSR signer policy.

use std::sync::Arc;

pub struct CsrSignerController {
    policy: crate::csr_signer::CsrSignerController,
}

impl CsrSignerController {
    pub fn new(issuer: Arc<dyn crate::csr_signer::CsrIssuer>) -> Self {
        Self {
            policy: crate::csr_signer::CsrSignerController::new(issuer),
        }
    }
}

#[async_trait::async_trait]
impl crate::Controller for CsrSignerController {
    fn name(&self) -> &'static str {
        "certificatesigningrequest"
    }

    async fn reconcile(
        &self,
        resource: serde_json::Value,
        context: crate::Context,
    ) -> anyhow::Result<()> {
        self.policy
            .reconcile(context.csr_status_store(), resource)
            .await
    }
}
