#![cfg_attr(test, allow(dead_code))]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use klights_cluster_core::{Resource, ResourcePatchRequest, ResourcePreconditions};
use serde_json::Value;

/// Resource persistence capabilities used directly by HTTP handlers.
///
/// The API owner defines this object-safe surface; the composition root adapts
/// the concrete cluster datastore. Lower-owner implementation types never
/// enter API state.
#[async_trait::async_trait]
pub(crate) trait ApiResourceStore:
    crate::api::AdmissionExecution
    + crate::api::NamespaceTerminationStore
    + crate::api::watch_stream::WatchStreamSource
    + Send
    + Sync
{
    async fn create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
    ) -> anyhow::Result<Resource>;

    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> anyhow::Result<Option<Resource>>;

    async fn update_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        expected_resource_version: i64,
    ) -> anyhow::Result<Resource>;

    async fn update_status_only_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: Value,
        preconditions: ResourcePreconditions,
    ) -> anyhow::Result<Resource>;

    async fn patch_resource_latest_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        request: ResourcePatchRequest,
    ) -> anyhow::Result<Option<Resource>>;

    async fn delete_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> anyhow::Result<()>;

    async fn list_resource_keys_for_scope(
        &self,
        api_version: String,
        kind: String,
        namespaced: bool,
    ) -> anyhow::Result<Vec<(Option<String>, String)>>;

    async fn create_namespace(&self, name: &str, data: Value) -> anyhow::Result<Resource>;
    async fn get_namespace(&self, name: &str) -> anyhow::Result<Option<Resource>>;
    async fn update_namespace(
        &self,
        name: &str,
        data: Value,
        expected_resource_version: i64,
    ) -> anyhow::Result<Resource>;
    async fn delete_namespace(&self, name: &str) -> anyhow::Result<()>;
}

/// Complete Pod HTTP capability bundle. Every constituent contract is a
/// neutral leaf-owned port; the root adapter is the only object that knows the
/// concrete kubelet repository.
pub(crate) trait ApiPodRepository:
    klights_pod_api::PodQuery
    + klights_pod_api::PodSnapshotQuery
    + klights_pod_api::PodApiMutation
    + klights_pod_api::PodSubresourceMutation
    + klights_pod_api::PodEvictionDelete
    + klights_reconcile_api::NamespaceTerminationQueueSink
    + Send
    + Sync
{
    fn eviction_admission_port(&self) -> Arc<dyn klights_reconcile_api::PodEvictionAdmissionSink>;

    fn namespace_bootstrap_port(&self) -> Arc<dyn klights_reconcile_api::NamespaceBootstrapSink>;

    fn bind_pod_from_api(
        &self,
        namespace: &str,
        name: &str,
        binding: Value,
        dry_run: bool,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>>;
}

/// Failure counters and exposition used by API request/background paths.
#[derive(Clone, Debug)]
pub(crate) struct ApiFailureEntry {
    pub(crate) api_version: String,
    pub(crate) kind: String,
    pub(crate) namespace: Option<String>,
    pub(crate) name: String,
    pub(crate) hook: String,
    pub(crate) context: String,
    pub(crate) error: String,
}

pub(crate) trait ApiFailureMetrics:
    crate::api::NamespaceTerminationMetrics
    + klights_reconcile_api::ReconcileFailureMetrics
    + Send
    + Sync
{
    fn render_prometheus(&self) -> String;
    fn recent_failures(&self) -> Vec<ApiFailureEntry>;
}

pub(crate) type ApiNodeLeaseObservedFuture<'a> =
    Pin<Box<dyn Future<Output = Option<String>> + Send + 'a>>;

/// Read-only API projection of leader lease observations.
pub(crate) trait ApiNodeLeaseObservations: Send + Sync {
    fn observed_renew_time<'a>(&'a self, node_name: &'a str) -> ApiNodeLeaseObservedFuture<'a>;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeApiPortSet {
        resource_store: Arc<dyn ApiResourceStore>,
        pod_repository: Arc<dyn ApiPodRepository>,
        failure_metrics: Arc<dyn ApiFailureMetrics>,
        node_lease_observations: Arc<dyn ApiNodeLeaseObservations>,
    }

    fn compose_fake_api(
        resource_store: Arc<dyn ApiResourceStore>,
        pod_repository: Arc<dyn ApiPodRepository>,
        failure_metrics: Arc<dyn ApiFailureMetrics>,
        node_lease_observations: Arc<dyn ApiNodeLeaseObservations>,
    ) -> FakeApiPortSet {
        FakeApiPortSet {
            resource_store,
            pod_repository,
            failure_metrics,
            node_lease_observations,
        }
    }

    #[test]
    fn api_ports_are_object_safe_and_fake_composable() {
        fn assert_object_safe<T: ?Sized>() {}
        assert_object_safe::<dyn ApiResourceStore>();
        assert_object_safe::<dyn ApiPodRepository>();
        assert_object_safe::<dyn ApiFailureMetrics>();
        assert_object_safe::<dyn ApiNodeLeaseObservations>();
        let _ = std::mem::size_of::<FakeApiPortSet>();
        let _ = compose_fake_api;
    }
}
