#![cfg_attr(test, allow(dead_code))]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Complete Pod HTTP capability bundle. Every constituent contract is a
/// neutral leaf-owned port; the root adapter is the only object that knows the
/// concrete kubelet repository.
pub trait ApiPodRepository:
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
}

/// Failure counters and exposition used by API request/background paths.
#[derive(Clone, Debug)]
pub struct ApiFailureEntry {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
    pub hook: String,
    pub context: String,
    pub error: String,
}

pub trait ApiFailureMetrics: klights_reconcile_api::ReconcileFailureMetrics + Send + Sync {
    fn render_prometheus(&self) -> String;
    fn recent_failures(&self) -> Vec<ApiFailureEntry>;
}

pub type ApiNodeLeaseObservedFuture<'a> = Pin<Box<dyn Future<Output = Option<String>> + Send + 'a>>;

/// Read-only API projection of leader lease observations.
pub trait ApiNodeLeaseObservations: Send + Sync {
    fn observed_renew_time<'a>(&'a self, node_name: &'a str) -> ApiNodeLeaseObservedFuture<'a>;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeApiPortSet {
        pod_repository: Arc<dyn ApiPodRepository>,
        failure_metrics: Arc<dyn ApiFailureMetrics>,
        node_lease_observations: Arc<dyn ApiNodeLeaseObservations>,
    }

    fn compose_fake_api(
        pod_repository: Arc<dyn ApiPodRepository>,
        failure_metrics: Arc<dyn ApiFailureMetrics>,
        node_lease_observations: Arc<dyn ApiNodeLeaseObservations>,
    ) -> FakeApiPortSet {
        FakeApiPortSet {
            pod_repository,
            failure_metrics,
            node_lease_observations,
        }
    }

    #[test]
    fn api_ports_are_object_safe_and_fake_composable() {
        fn assert_object_safe<T: ?Sized>() {}
        assert_object_safe::<dyn ApiPodRepository>();
        assert_object_safe::<dyn ApiFailureMetrics>();
        assert_object_safe::<dyn ApiNodeLeaseObservations>();
        let _ = std::mem::size_of::<FakeApiPortSet>();
        let _ = compose_fake_api;
    }
}
