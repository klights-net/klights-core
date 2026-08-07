use std::sync::Arc;

use crate::datastore::{DatastoreBackend, DatastoreHandle};
use anyhow::Result;
use async_trait::async_trait;
use klights_controllers::pdb;
use klights_controllers::side_effects::PodSideEffectPortsSlot;
use klights_controllers::side_effects::pdb::PdbSideEffectPort;
use klights_pod_api::PodQuery;

struct RootPdbSideEffectPort {
    db: DatastoreHandle,
    pod_repository: PodSideEffectPortsSlot,
}

struct BoundPdbPort<'a> {
    db: &'a dyn DatastoreBackend,
    pod_query: &'a dyn PodQuery,
}

#[async_trait]
impl PdbSideEffectPort for BoundPdbPort<'_> {
    async fn reconcile_namespace(&self, namespace: &str) -> Result<()> {
        pdb::reconcile_pdbs_for_namespace(self.db, self.pod_query, namespace, chrono::Utc::now())
            .await;
        Ok(())
    }
}

#[async_trait]
impl PdbSideEffectPort for RootPdbSideEffectPort {
    async fn reconcile_namespace(&self, namespace: &str) -> Result<()> {
        let Some(pod_query) = self.pod_repository.query() else {
            tracing::debug!(
                "PDBReconcileEffect skipped for {}: PodRepository not yet bound",
                namespace
            );
            return Ok(());
        };
        BoundPdbPort {
            db: self.db.as_ref(),
            pod_query: pod_query.as_ref(),
        }
        .reconcile_namespace(namespace)
        .await
    }
}

pub(crate) fn port(
    db: DatastoreHandle,
    pod_repository: PodSideEffectPortsSlot,
) -> Arc<dyn PdbSideEffectPort> {
    Arc::new(RootPdbSideEffectPort { db, pod_repository })
}

#[cfg(test)]
mod adapter_tests {
    #[tokio::test]
    async fn test_pdb_reconcile_name() {
        let (_db, db_handle) =
            crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
        let effect = klights_controllers::side_effects::pdb::effect(super::port(
            db_handle,
            klights_controllers::side_effects::PodSideEffectPortsSlot::new(),
        ));
        assert_eq!(effect.name(), "pdb_reconcile");
    }
}
