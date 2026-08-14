use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;
use klights_cluster_store::{
    ClusterResourceRead, ResourceCollectionScope, ResourceListQuery, ResourceListRead,
    ResourceListRequest,
};
use klights_pod_api::{PodListRequest, PodQuery};
use serde_json::Value;

use crate::bootstrap::controller_adapters::controller_store_error_adapter::map_controller_store_error;
use klights_controllers::resource_quota::{
    ResourceQuotaRuntime, reconcile_resource_quotas_with_runtime,
};

struct ResourceQuotaControllerAdapter<'a> {
    resource_reads: &'a dyn ClusterResourceRead,
    status_store: &'a dyn klights_controllers::common::ControllerStatusStore,
    pod_query: &'a dyn PodQuery,
}

#[async_trait]
impl ResourceQuotaRuntime for ResourceQuotaControllerAdapter<'_> {
    async fn list_quota_resources(
        &self,
        api_version: &str,
        kind: &str,
        namespace: &str,
    ) -> klights_reconcile_api::ControllerStoreResult<Vec<Resource>> {
        match self
            .resource_reads
            .list_resources(ResourceListRequest::new(
                api_version,
                kind,
                ResourceCollectionScope::Namespace(namespace.to_string()),
                ResourceListQuery::all(),
            ))
            .await
            .map_err(|error| map_controller_store_error(error.into()))?
        {
            ResourceListRead::Current(page) | ResourceListRead::Historical(page) => {
                Ok(page.into_items())
            }
            ResourceListRead::Expired {
                requested,
                oldest_available,
                ..
            } => Err(klights_reconcile_api::ControllerStoreError::unavailable(
                format!(
                    "ResourceQuota LIST at resourceVersion {requested} expired before {oldest_available}"
                ),
            )),
        }
    }

    async fn list_namespace_pods(
        &self,
        namespace: &str,
    ) -> klights_reconcile_api::ControllerStoreResult<Vec<Resource>> {
        let request = PodListRequest::try_new(Some(namespace.to_string()), None, None, None, None)
            .map_err(|error| {
                klights_reconcile_api::ControllerStoreError::internal(format!(
                    "invalid ResourceQuota Pod list request: {error}"
                ))
            })?;
        self.pod_query
            .list_pods(request)
            .await
            .map(|listing| listing.into_parts().0)
            .map_err(|error| {
                klights_reconcile_api::ControllerStoreError::unavailable(format!(
                    "ResourceQuota Pod list failed: {error}"
                ))
            })
    }

    async fn write_resource_quota_status(
        &self,
        resource: &Resource,
        status: &Value,
    ) -> klights_reconcile_api::ControllerStoreResult<()> {
        klights_controllers::common::write_status_for_resource(self.status_store, resource, status)
            .await
            .map(|_| ())
            .map_err(map_controller_store_error)
    }
}

pub async fn reconcile_resource_quotas_for_namespace(
    resource_reads: &dyn ClusterResourceRead,
    status_store: &dyn klights_controllers::common::ControllerStatusStore,
    pod_query: &dyn PodQuery,
    namespace: &str,
) -> Result<()> {
    reconcile_resource_quotas_with_runtime(
        &ResourceQuotaControllerAdapter {
            resource_reads,
            status_store,
            pod_query,
        },
        namespace,
    )
    .await
}
