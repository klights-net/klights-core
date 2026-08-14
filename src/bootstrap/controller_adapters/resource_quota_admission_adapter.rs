use klights_reconcile_api::{
    QuotaResourceListFuture, ReconcileSinkError, ResourceQuotaAdmissionRuntime,
};

pub(crate) struct ResourceQuotaAdmissionAdapter {
    resource_reads: std::sync::Arc<dyn klights_cluster_store::ClusterResourceRead>,
}

impl ResourceQuotaAdmissionAdapter {
    pub(crate) fn new(
        resource_reads: std::sync::Arc<dyn klights_cluster_store::ClusterResourceRead>,
    ) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self { resource_reads })
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
            match self
                .resource_reads
                .list_resources(klights_cluster_store::ResourceListRequest::new(
                    api_version,
                    kind,
                    klights_cluster_store::ResourceCollectionScope::Namespace(
                        namespace.to_string(),
                    ),
                    klights_cluster_store::ResourceListQuery::all(),
                ))
                .await
                .map_err(|error| ReconcileSinkError::unavailable(error.to_string()))?
            {
                klights_cluster_store::ResourceListRead::Current(page)
                | klights_cluster_store::ResourceListRead::Historical(page) => {
                    Ok(page.into_items())
                }
                klights_cluster_store::ResourceListRead::Expired {
                    requested,
                    oldest_available,
                    ..
                } => Err(ReconcileSinkError::unavailable(format!(
                    "{api_version}/{kind} LIST at resourceVersion {requested} expired before {oldest_available}"
                ))),
            }
        })
    }

    fn pod_has_deletion_timestamp(&self, pod: &serde_json::Value) -> bool {
        klights_controllers::resource_quota::pod_has_deletion_timestamp(pod)
    }

    fn pod_matches_resource_quota_scopes(
        &self,
        pod: &serde_json::Value,
        quota: &serde_json::Value,
    ) -> bool {
        klights_controllers::resource_quota::pod_matches_resource_quota_scopes(pod, quota)
    }

    fn resource_quota_has_pod_scope_constraints(&self, quota: &serde_json::Value) -> bool {
        klights_controllers::resource_quota::resource_quota_has_pod_scope_constraints(quota)
    }

    fn parse_resource_quantity(&self, resource_key: &str, quantity: &str) -> Option<i64> {
        klights_controllers::resource_quota::parse_resource_quantity(resource_key, quantity)
    }

    fn format_resource_quantity(&self, resource_key: &str, value: i64) -> String {
        klights_controllers::resource_quota::format_resource_quantity(resource_key, value)
    }

    fn calculate_pod_effective_resource_for_key(
        &self,
        pod: &serde_json::Value,
        bucket: &str,
        resource_key: &str,
    ) -> i64 {
        klights_controllers::resource_quota::calculate_pod_effective_resource_for_key(
            pod,
            bucket,
            resource_key,
        )
    }
}
