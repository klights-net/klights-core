use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;
use klights_pod_api::{PodListRequest, PodQuery};
use serde_json::Value;

use crate::controllers::resource_quota::{
    ResourceQuotaRuntime, reconcile_resource_quotas_with_runtime,
};
use crate::datastore::{DatastoreBackend, ResourceListQuery};

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
    ) -> Result<Vec<Resource>> {
        self.db
            .list_resources(api_version, kind, Some(namespace), ResourceListQuery::all())
            .await
            .map(|listing| listing.items)
    }

    async fn list_namespace_pods(&self, namespace: &str) -> Result<Vec<Resource>> {
        self.pod_query
            .list_pods(PodListRequest::try_new(
                Some(namespace.to_string()),
                None,
                None,
                None,
                None,
            )?)
            .await
            .map(|listing| listing.into_parts().0)
            .map_err(Into::into)
    }

    async fn write_resource_quota_status(&self, resource: &Resource, status: &Value) -> Result<()> {
        crate::controllers::common::write_status_for_resource(self.db, resource, status)
            .await
            .map(|_| ())
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
