use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use klights_pod_api::{PodListRequest, PodQuery};
use serde_json::Value;

use crate::controllers::pdb;
use crate::datastore::{DatastoreBackend, DatastoreHandle};
use crate::side_effects::pdb::{PdbSideEffectPort, apply_pdb_event, pdb_event_namespace};
use klights_controllers::side_effects::{PodSideEffectPortsSlot, SideEffect};

/// Updates PodDisruptionBudget status after Pod create/update/delete.
///
/// Registered only for `(v1, Pod)` — the registry handles the kind dispatch.
/// The repository remains late-bound because the registry is constructed
/// before the repository in bootstrap.
struct PdbReconcileEffect {
    db: DatastoreHandle,
    pod_repository: PodSideEffectPortsSlot,
}

struct BoundPdbPort<'a> {
    db: &'a dyn DatastoreBackend,
    pod_query: &'a dyn PodQuery,
}

#[async_trait]
impl crate::controllers::pdb::PdbPodReader for BoundPdbPort<'_> {
    async fn list_namespace_pods(
        &self,
        namespace: &str,
    ) -> klights_reconcile_api::ControllerStoreResult<Vec<klights_cluster_core::Resource>> {
        let request = PodListRequest::try_new(Some(namespace.to_string()), None, None, None, None)
            .map_err(|error| {
                klights_reconcile_api::ControllerStoreError::internal(format!(
                    "invalid PDB Pod list request: {error}"
                ))
            })?;
        self.pod_query
            .list_pods(request)
            .await
            .map(|listing| listing.into_parts().0)
            .map_err(|error| {
                klights_reconcile_api::ControllerStoreError::unavailable(format!(
                    "PDB Pod list failed: {error}"
                ))
            })
    }
}

#[async_trait]
impl PdbSideEffectPort for BoundPdbPort<'_> {
    async fn reconcile_namespace(&self, namespace: &str) -> Result<()> {
        pdb::reconcile_pdbs_for_namespace(self.db, self, namespace, chrono::Utc::now()).await;
        Ok(())
    }
}

#[async_trait]
impl SideEffect for PdbReconcileEffect {
    fn name(&self) -> &'static str {
        "pdb_reconcile"
    }

    async fn apply(&self, resource: &Value) -> Result<()> {
        let Some(namespace) = pdb_event_namespace(resource) else {
            return Ok(());
        };
        let Some(pod_query) = self.pod_repository.query() else {
            tracing::debug!(
                "PDBReconcileEffect skipped for {}: PodRepository not yet bound",
                namespace
            );
            return Ok(());
        };
        apply_pdb_event(
            resource,
            &BoundPdbPort {
                db: self.db.as_ref(),
                pod_query: pod_query.as_ref(),
            },
        )
        .await
    }
}

pub(crate) fn effect(
    db: DatastoreHandle,
    pod_repository: PodSideEffectPortsSlot,
) -> Arc<dyn SideEffect> {
    Arc::new(PdbReconcileEffect { db, pod_repository })
}
