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
struct DirectResourceQuotaSideEffectPort {
    db: crate::datastore::DatastoreHandle,
    status_store: Arc<super::controller_runtime_adapter::RootControllerLeaderPort>,
    pod_repository: PodSideEffectPortsSlot,
}

#[cfg(test)]
struct DirectResourceQuotaRuntime<'a> {
    db: &'a dyn crate::datastore::DatastoreBackend,
    status_store: &'a dyn klights_controllers::common::ControllerStatusStore,
    pod_query: &'a dyn PodQuery,
}

#[cfg(test)]
#[async_trait]
impl klights_controllers::resource_quota::ResourceQuotaRuntime for DirectResourceQuotaRuntime<'_> {
    async fn list_quota_resources(
        &self,
        api_version: &str,
        kind: &str,
        namespace: &str,
    ) -> klights_reconcile_api::ControllerStoreResult<Vec<klights_cluster_core::Resource>> {
        self.db
            .list_resources(
                api_version,
                kind,
                Some(namespace),
                klights_cluster_store::ResourceListOptions::all(),
            )
            .await
            .map(|page| page.items)
            .map_err(|error| {
                klights_reconcile_api::ControllerStoreError::unavailable(error.to_string())
            })
    }
    async fn list_namespace_pods(
        &self,
        namespace: &str,
    ) -> klights_reconcile_api::ControllerStoreResult<Vec<klights_cluster_core::Resource>> {
        let request = klights_pod_api::PodListRequest::try_new(
            Some(namespace.to_string()),
            None,
            None,
            None,
            None,
        )
        .map_err(|error| {
            klights_reconcile_api::ControllerStoreError::internal(error.to_string())
        })?;
        self.pod_query
            .list_pods(request)
            .await
            .map(|listing| listing.into_parts().0)
            .map_err(|error| {
                klights_reconcile_api::ControllerStoreError::unavailable(error.to_string())
            })
    }
    async fn write_resource_quota_status(
        &self,
        resource: &klights_cluster_core::Resource,
        status: &serde_json::Value,
    ) -> klights_reconcile_api::ControllerStoreResult<()> {
        klights_controllers::common::write_status_for_resource(self.status_store, resource, status).await.map(|_| ()).map_err(crate::bootstrap::controller_adapters::controller_store_error_adapter::map_controller_store_error)
    }
}

#[cfg(test)]
#[async_trait]
impl ResourceQuotaSideEffectPort for DirectResourceQuotaSideEffectPort {
    async fn recount_namespace(&self, namespace: &str) -> Result<()> {
        let Some(pod_query) = self.pod_repository.query() else {
            return Ok(());
        };
        klights_controllers::resource_quota::reconcile_resource_quotas_with_runtime(
            &DirectResourceQuotaRuntime {
                db: self.db.as_ref(),
                status_store: self.status_store.as_ref(),
                pod_query: pod_query.as_ref(),
            },
            namespace,
        )
        .await
    }
}

#[cfg(test)]
pub(crate) fn port_for_test(
    db: crate::datastore::DatastoreHandle,
    status_store: Arc<super::controller_runtime_adapter::RootControllerLeaderPort>,
    pod_repository: PodSideEffectPortsSlot,
) -> Arc<dyn ResourceQuotaSideEffectPort> {
    Arc::new(DirectResourceQuotaSideEffectPort {
        db,
        status_store,
        pod_repository,
    })
}

#[cfg(test)]
mod adapter_tests {
    #[tokio::test]
    async fn test_resource_quota_recount_name() {
        let (db, db_handle) =
            crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
        let effect = klights_controllers::side_effects::resource_quota::effect(super::port(
            db.focused_read_store(),
            std::sync::Arc::new(
                crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort::new(db_handle),
            ),
            klights_controllers::side_effects::PodSideEffectPortsSlot::new(),
        ));
        assert_eq!(effect.name(), "resource_quota_recount");
    }
}
