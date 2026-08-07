//! `Controller` wrapper for `PodDisruptionBudget`.

pub struct PDBController;

#[async_trait::async_trait]
impl crate::Controller for PDBController {
    fn name(&self) -> &'static str {
        "poddisruptionbudget"
    }

    async fn reconcile(
        &self,
        resource: serde_json::Value,
        context: crate::Context,
    ) -> anyhow::Result<()> {
        crate::pdb::reconcile_pdb_at(
            context.pdb_store(),
            context.pod_query(),
            &resource,
            context.reconcile_time(),
        )
        .await
    }
}
