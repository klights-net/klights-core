use klights_reconcile_api::{
    QuotaResourceListFuture, ReconcileSinkError, ResourceQuotaAdmissionRuntime,
};

pub(crate) struct ResourceQuotaAdmissionAdapter {
    db: crate::datastore::DatastoreHandle,
}

impl ResourceQuotaAdmissionAdapter {
    pub(crate) fn new(db: crate::datastore::DatastoreHandle) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self { db })
    }
}

impl ResourceQuotaAdmissionRuntime for ResourceQuotaAdmissionAdapter {
    fn list_resources<'a>(
        &'a self,
        api_version: &'a str,
        kind: &'a str,
        namespace: &'a str,
    ) -> QuotaResourceListFuture<'a> {
        Box::pin(async move {
            self.db
                .list_resources(
                    api_version,
                    kind,
                    Some(namespace),
                    crate::datastore::ResourceListQuery::all(),
                )
                .await
                .map(|list| list.items)
                .map_err(|error| ReconcileSinkError::unavailable(error.to_string()))
        })
    }

    fn pod_has_deletion_timestamp(&self, pod: &serde_json::Value) -> bool {
        crate::controllers::resource_quota::pod_has_deletion_timestamp(pod)
    }

    fn pod_matches_resource_quota_scopes(
        &self,
        pod: &serde_json::Value,
        quota: &serde_json::Value,
    ) -> bool {
        crate::controllers::resource_quota::pod_matches_resource_quota_scopes(pod, quota)
    }

    fn resource_quota_has_pod_scope_constraints(&self, quota: &serde_json::Value) -> bool {
        crate::controllers::resource_quota::resource_quota_has_pod_scope_constraints(quota)
    }

    fn parse_resource_quantity(&self, resource_key: &str, quantity: &str) -> Option<i64> {
        crate::controllers::resource_quota::parse_resource_quantity(resource_key, quantity)
    }

    fn format_resource_quantity(&self, resource_key: &str, value: i64) -> String {
        crate::controllers::resource_quota::format_resource_quantity(resource_key, value)
    }

    fn calculate_pod_effective_resource_for_key(
        &self,
        pod: &serde_json::Value,
        bucket: &str,
        resource_key: &str,
    ) -> i64 {
        crate::controllers::resource_quota::calculate_pod_effective_resource_for_key(
            pod,
            bucket,
            resource_key,
        )
    }
}
