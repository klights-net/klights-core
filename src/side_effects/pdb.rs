//! Side effect to update PDB status after Pod mutations.

use super::{PodRepositorySlot, SideEffect};
use crate::controllers::pdb;
use crate::datastore::DatastoreBackend;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;

/// Updates PodDisruptionBudget status after Pod create/update/delete.
///
/// Registered only for `(v1, Pod)` — the registry handles the kind dispatch.
/// Holds a [`PodRepositorySlot`] so the late-bound `PodRepository` is
/// resolved at `apply` time (the registry is constructed before the
/// repository in bootstrap).
pub struct PDBReconcileEffect {
    pod_repository: PodRepositorySlot,
}

#[async_trait]
impl SideEffect for PDBReconcileEffect {
    fn name(&self) -> &'static str {
        "pdb_reconcile"
    }

    async fn apply(&self, resource: &Value, db: &dyn DatastoreBackend) -> Result<()> {
        let namespace = resource
            .pointer("/metadata/namespace")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if namespace.is_empty() {
            return Ok(());
        }

        let Some(pod_repository) = self.pod_repository.get() else {
            // PodRepository is late-bound from bootstrap; before that point
            // PDB status reconcile is a no-op (matches the previous registry
            // construction order where this effect was inert until late wiring).
            tracing::debug!(
                "PDBReconcileEffect skipped for {}: PodRepository not yet bound",
                namespace
            );
            return Ok(());
        };

        pdb::reconcile_pdbs_for_namespace(db, pod_repository.as_ref(), namespace).await;
        Ok(())
    }
}

/// Create a PDBReconcileEffect instance backed by the supplied late-bound
/// `PodRepository` slot.
pub fn pdb_reconcile(pod_repository: PodRepositorySlot) -> Arc<dyn SideEffect> {
    Arc::new(PDBReconcileEffect { pod_repository })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_pdb_reconcile_name() {
        let effect = pdb_reconcile(PodRepositorySlot::new());
        assert_eq!(effect.name(), "pdb_reconcile");
    }
}
