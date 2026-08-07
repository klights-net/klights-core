use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::datastore::{DatastoreBackend, DatastoreHandle};
use klights_controllers::side_effects::PodSideEffectPortsSlot;
use klights_controllers::side_effects::resource_quota::ResourceQuotaSideEffectPort;
use klights_pod_api::PodQuery;

/// Recounts ResourceQuota status.used after any namespaced resource mutation.
///
/// The late-bound repository is resolved for every event so construction
/// order remains independent and pod-scoped counts always use `PodQuery`.
struct RootResourceQuotaSideEffectPort {
    db: DatastoreHandle,
    pod_repository: PodSideEffectPortsSlot,
}

struct BoundResourceQuotaPort<'a> {
    db: &'a dyn DatastoreBackend,
    pod_query: &'a dyn PodQuery,
}

#[async_trait]
impl ResourceQuotaSideEffectPort for BoundResourceQuotaPort<'_> {
    async fn recount_namespace(&self, namespace: &str) -> Result<()> {
        crate::bootstrap::controller_adapters::resource_quota_controller_adapter::reconcile_resource_quotas_for_namespace(
            self.db,
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
                "ResourceQuotaEffect skipped for {}: PodRepository not yet bound",
                namespace
            );
            return Ok(());
        };
        BoundResourceQuotaPort {
            db: self.db.as_ref(),
            pod_query: pod_query.as_ref(),
        }
        .recount_namespace(namespace)
        .await
    }
}

pub(crate) fn port(
    db: DatastoreHandle,
    pod_repository: PodSideEffectPortsSlot,
) -> Arc<dyn ResourceQuotaSideEffectPort> {
    Arc::new(RootResourceQuotaSideEffectPort { db, pod_repository })
}

#[cfg(test)]
mod adapter_tests {
    #[tokio::test]
    async fn test_resource_quota_recount_name() {
        let (_db, db_handle) =
            crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
        let effect = klights_controllers::side_effects::resource_quota::effect(super::port(
            db_handle,
            klights_controllers::side_effects::PodSideEffectPortsSlot::new(),
        ));
        assert_eq!(effect.name(), "resource_quota_recount");
    }
}
