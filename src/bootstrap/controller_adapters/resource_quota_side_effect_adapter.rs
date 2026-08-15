use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use klights_cluster_store::ClusterResourceRead;
use klights_controllers::side_effects::PodSideEffectPortsSlot;
use klights_controllers::side_effects::resource_quota::ResourceQuotaSideEffectPort;
use klights_pod_api::PodQuery;

/// Recounts ResourceQuota status.used after any namespaced resource mutation.
///
/// The late-bound repository is resolved for every event so construction
/// order remains independent and pod-scoped counts always use `PodQuery`.
struct RootResourceQuotaSideEffectPort {
    resource_reads: Arc<dyn ClusterResourceRead>,
    status_store: Arc<super::controller_runtime_adapter::RootControllerLeaderPort>,
    pod_repository: PodSideEffectPortsSlot,
}

struct BoundResourceQuotaPort<'a> {
    resource_reads: &'a dyn ClusterResourceRead,
    status_store: &'a dyn klights_controllers::common::ControllerStatusStore,
    pod_query: &'a dyn PodQuery,
}

#[async_trait]
impl ResourceQuotaSideEffectPort for BoundResourceQuotaPort<'_> {
    async fn recount_namespace(&self, namespace: &str) -> Result<()> {
        crate::bootstrap::controller_adapters::resource_quota_controller_adapter::reconcile_resource_quotas_for_namespace(
            self.resource_reads,
            self.status_store,
            self.pod_query,
            namespace,
        )
        .await
    }
}

#[async_trait]
impl ResourceQuotaSideEffectPort for RootResourceQuotaSideEffectPort {
    async fn recount_namespace(&self, namespace: &str) -> Result<()> {
        let Some(pod_query) = self.pod_repository.query() else {
            tracing::debug!(
                "ResourceQuotaEffect skipped for {}: Pod query not yet bound",
                namespace
            );
            return Ok(());
        };
        BoundResourceQuotaPort {
            resource_reads: self.resource_reads.as_ref(),
            status_store: self.status_store.as_ref(),
            pod_query: pod_query.as_ref(),
        }
        .recount_namespace(namespace)
        .await
    }
}

pub(crate) fn port(
    resource_reads: Arc<dyn ClusterResourceRead>,
    status_store: Arc<super::controller_runtime_adapter::RootControllerLeaderPort>,
    pod_repository: PodSideEffectPortsSlot,
) -> Arc<dyn ResourceQuotaSideEffectPort> {
    Arc::new(RootResourceQuotaSideEffectPort {
        resource_reads,
        status_store,
        pod_repository,
    })
}

#[cfg(test)]
mod adapter_tests {
    #[tokio::test]
    async fn test_resource_quota_recount_name() {
        let db = crate::bootstrap::composition::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
        let ports =
            crate::bootstrap::composition::cluster_store::selector::sqlite_opened_passive_store(
                &db,
            );
        let effect = klights_controllers::side_effects::resource_quota::effect(super::port(
            ports.read_ports.resource_reads(),
            std::sync::Arc::new(
                crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort::new_for_test(
                    ports.applied_outbox,
                    std::sync::Arc::new(db.clone()),
                    ports.read_ports.resource_reads(),
                    ports.ownership_reads,
                ),
            ),
            klights_controllers::side_effects::PodSideEffectPortsSlot::new(),
        ));
        assert_eq!(effect.name(), "resource_quota_recount");
    }
}
