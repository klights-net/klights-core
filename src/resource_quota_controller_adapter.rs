use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;
use klights_pod_api::{PodListRequest, PodQuery};
use serde_json::Value;

use crate::controller_store_error_adapter::map_controller_store_error;
use crate::datastore::{DatastoreBackend, ResourceListQuery};
use klights_controllers::resource_quota::{
    ResourceQuotaRuntime, reconcile_resource_quotas_with_runtime,
};

struct ResourceQuotaControllerAdapter<'a> {
    db: &'a dyn DatastoreBackend,
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
        self.db
            .list_resources(api_version, kind, Some(namespace), ResourceListQuery::all())
            .await
            .map(|listing| listing.items)
            .map_err(map_controller_store_error)
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
        klights_controllers::common::write_status_for_resource(self.db, resource, status)
            .await
            .map(|_| ())
            .map_err(map_controller_store_error)
    }
}

pub async fn reconcile_resource_quotas_for_namespace(
    db: &dyn DatastoreBackend,
    pod_query: &dyn PodQuery,
    namespace: &str,
) -> Result<()> {
    reconcile_resource_quotas_with_runtime(
        &ResourceQuotaControllerAdapter { db, pod_query },
        namespace,
    )
    .await
}

#[cfg(test)]
use klights_controllers::resource_quota::*;
#[cfg(test)]
#[path = "controller_test_debt/resource_quota/tests.rs"]
mod policy_tests;
