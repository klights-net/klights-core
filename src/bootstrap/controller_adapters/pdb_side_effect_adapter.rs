use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use klights_controllers::pdb;
use klights_controllers::side_effects::PodSideEffectPortsSlot;
use klights_controllers::side_effects::pdb::PdbSideEffectPort;
use klights_pod_api::PodQuery;

struct RootPdbSideEffectPort {
    store: Arc<super::controller_runtime_adapter::RootControllerLeaderPort>,
    pod_repository: PodSideEffectPortsSlot,
}

struct BoundPdbPort<'a> {
    store: &'a super::controller_runtime_adapter::RootControllerLeaderPort,
    pod_query: &'a dyn PodQuery,
}

#[async_trait]
impl PdbSideEffectPort for BoundPdbPort<'_> {
    async fn reconcile_namespace(&self, namespace: &str) -> Result<()> {
        pdb::reconcile_pdbs_for_namespace(
            self.store,
            self.pod_query,
            namespace,
            chrono::Utc::now(),
        )
        .await;
        Ok(())
    }
}

#[async_trait]
impl PdbSideEffectPort for RootPdbSideEffectPort {
    async fn reconcile_namespace(&self, namespace: &str) -> Result<()> {
        let Some(pod_query) = self.pod_repository.query() else {
            tracing::debug!(
                "PDBReconcileEffect skipped for {}: Pod query not yet bound",
                namespace
            );
            return Ok(());
        };
        BoundPdbPort {
            store: self.store.as_ref(),
            pod_query: pod_query.as_ref(),
        }
        .reconcile_namespace(namespace)
        .await
    }
}

pub(crate) fn port(
    store: Arc<super::controller_runtime_adapter::RootControllerLeaderPort>,
    pod_repository: PodSideEffectPortsSlot,
) -> Arc<dyn PdbSideEffectPort> {
    Arc::new(RootPdbSideEffectPort {
        store,
        pod_repository,
    })
}

#[cfg(test)]
mod adapter_tests {
    #[tokio::test]
    async fn test_pdb_reconcile_name() {
        let db = crate::bootstrap::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
        let ports = crate::bootstrap::cluster_store::selector::sqlite_opened_passive_store(&db);
        let effect = klights_controllers::side_effects::pdb::effect(super::port(
            std::sync::Arc::new(crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort::new_for_test(
                ports.applied_outbox,
                std::sync::Arc::new(db.clone()),
                ports.read_ports.resource_reads(),
                ports.ownership_reads,
            )),
            klights_controllers::side_effects::PodSideEffectPortsSlot::new(),
        ));
        assert_eq!(effect.name(), "pdb_reconcile");
    }
}
