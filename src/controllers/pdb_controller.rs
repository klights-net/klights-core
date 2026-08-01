//! `Controller` impl for `PodDisruptionBudget`. Registered in `ControllerDispatcher`.

use klights_controllers::pdb as pdb_core;

pub struct PDBController;

#[async_trait::async_trait]
impl crate::controllers::Controller for PDBController {
    fn name(&self) -> &'static str {
        "poddisruptionbudget"
    }

    async fn reconcile(
        &self,
        resource: serde_json::Value,
        ctx: crate::controllers::Context,
    ) -> anyhow::Result<()> {
        pdb_core::reconcile_pdb_at(
            ctx.pdb_store(),
            ctx.pdb_reader(),
            &resource,
            ctx.reconcile_time(),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controllers::Controller;

    #[test]
    fn test_pdb_controller_name() {
        assert_eq!(PDBController.name(), "poddisruptionbudget");
    }
}
