use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use anyhow::Result;
use async_trait::async_trait;
use klights_leader_api::{
    CacheReadinessFuture, CacheReadinessRequest, LeaderCacheReadiness, LeaderNetworkTopologyQuery,
    LeaderNodeLeaseRenewal, LeaderNodeSubnetAllocation, LeaderOutboxDelivery,
    LeaderPodCleanupIntents, LeaderProjectedServiceAccountToken, LeaderResourceCommand,
    LeaderResourceQuery, LeaderWatch, LeaderWatchFuture, NetworkTopologyError,
    NetworkTopologyFuture, NodeDataplaneQuery, NodeDataplaneResult, NodeLeaseRenewalError,
    NodeLeaseRenewalFuture, NodeLeaseRenewalRequest, NodeLeaseRenewalResult,
    NodeSubnetAllocationError, NodeSubnetAllocationFuture, NodeSubnetAllocationRequest,
    NodeSubnetAllocationResult, NodeSubnetQuery, NodeSubnetResult, OutboxDeliveryError,
    OutboxDeliveryFuture, OutboxDeliveryRequest, PeerSubnetsQuery, PeerSubnetsResult,
    PodCleanupIntent, PodCleanupIntentAckRequest, PodCleanupIntentError, PodCleanupIntentFuture,
    PodCleanupIntentListRequest, ProjectedServiceAccountTokenError,
    ProjectedServiceAccountTokenFuture, ProjectedServiceAccountTokenRequest, ResourceCommandFuture,
    ResourceCommandRequest, ResourceCommandResult, ResourceGetRequest, ResourceListRequest,
    ResourceListResult, ResourceQueryConsistency, ResourceQueryFuture, WatchRequest,
};
use tokio_util::sync::CancellationToken;

use klights_cluster_core::Resource;
use klights_supervisor::{SupervisedJoinHandle, TaskSupervisor};
use klights_watch::RemoteInformerCache;

use super::ReplicationGrpcClient;

#[derive(Clone)]
pub struct RemoteApiClient {
    node_name: String,
    grpc: Option<Arc<ReplicationGrpcClient>>,
    supervisor: Option<Arc<TaskSupervisor>>,
    cache: Arc<dyn RemoteInformerCache>,
    worker_informers_started: Arc<AtomicBool>,
    /// bug-grpc: per-stream idle timeout; overridable in tests.
    watch_idle_timeout: std::time::Duration,
}

impl RemoteApiClient {
    /// Constructs an explicitly disconnected client for fail-closed startup or
    /// transport-unavailable operation.
    pub fn without_transport(
        node_name: impl Into<String>,
        cache: Arc<dyn RemoteInformerCache>,
    ) -> Self {
        Self {
            node_name: node_name.into(),
            grpc: None,
            supervisor: None,
            cache,
            worker_informers_started: Arc::new(AtomicBool::new(false)),
            watch_idle_timeout: super::watch::WATCH_IDLE_TIMEOUT,
        }
    }

    pub fn from_grpc(
        grpc: Arc<ReplicationGrpcClient>,
        supervisor: Arc<TaskSupervisor>,
        node_name: String,
        cache: Arc<dyn RemoteInformerCache>,
    ) -> Self {
        Self {
            node_name,
            grpc: Some(grpc),
            supervisor: Some(supervisor),
            cache,
            worker_informers_started: Arc::new(AtomicBool::new(false)),
            watch_idle_timeout: super::watch::WATCH_IDLE_TIMEOUT,
        }
    }

    pub async fn start_required_worker_informers(
        self: &Arc<Self>,
        cancel: CancellationToken,
    ) -> Result<Vec<SupervisedJoinHandle<()>>> {
        super::watch::start_required_worker_informers(
            self.grpc.clone(),
            self.supervisor.clone(),
            self.cache.clone(),
            self.worker_informers_started.clone(),
            Self::required_worker_list_requests(&self.node_name),
            cancel,
            self.watch_idle_timeout,
        )
        .await
    }

    fn list_pods_on_node_request(node_name: &str) -> ResourceListRequest {
        ResourceListRequest::try_new(
            "v1",
            "Pod",
            klights_leader_api::ResourceListScope::AllNamespaces,
            None,
            Some(format!("spec.nodeName={node_name}")),
            None,
            None,
            ResourceQueryConsistency::Cached,
        )
        .expect("required worker Pod scope is valid")
    }

    fn required_worker_list_requests(node_name: &str) -> Vec<ResourceListRequest> {
        let mut reqs = vec![Self::list_pods_on_node_request(node_name)];
        for (api_version, kind, scope) in [
            (
                "v1",
                "ConfigMap",
                klights_leader_api::ResourceListScope::AllNamespaces,
            ),
            (
                "v1",
                "Secret",
                klights_leader_api::ResourceListScope::AllNamespaces,
            ),
            (
                "v1",
                "PersistentVolumeClaim",
                klights_leader_api::ResourceListScope::AllNamespaces,
            ),
            (
                "v1",
                "PersistentVolume",
                klights_leader_api::ResourceListScope::Cluster,
            ),
            (
                "node.k8s.io/v1",
                "RuntimeClass",
                klights_leader_api::ResourceListScope::Cluster,
            ),
            (
                "scheduling.k8s.io/v1",
                "PriorityClass",
                klights_leader_api::ResourceListScope::Cluster,
            ),
            (
                "v1",
                "ServiceAccount",
                klights_leader_api::ResourceListScope::AllNamespaces,
            ),
            (
                "v1",
                "Service",
                klights_leader_api::ResourceListScope::AllNamespaces,
            ),
            (
                "v1",
                "Endpoints",
                klights_leader_api::ResourceListScope::AllNamespaces,
            ),
            (
                "discovery.k8s.io/v1",
                "EndpointSlice",
                klights_leader_api::ResourceListScope::AllNamespaces,
            ),
            ("v1", "Node", klights_leader_api::ResourceListScope::Cluster),
            (
                "coordination.k8s.io/v1",
                "Lease",
                klights_leader_api::ResourceListScope::Namespace("kube-node-lease".into()),
            ),
            (
                "v1",
                "Namespace",
                klights_leader_api::ResourceListScope::Cluster,
            ),
        ] {
            reqs.push(
                ResourceListRequest::try_new(
                    api_version,
                    kind,
                    scope,
                    None,
                    None,
                    None,
                    None,
                    ResourceQueryConsistency::Cached,
                )
                .expect("required worker informer scope is valid"),
            );
        }
        reqs
    }
}

impl LeaderResourceQuery for RemoteApiClient {
    fn get_resource(
        &self,
        request: ResourceGetRequest,
    ) -> ResourceQueryFuture<'_, Option<Resource>> {
        Box::pin(super::resource::get_resource(
            self.grpc.as_ref(),
            self.cache.as_ref(),
            request,
        ))
    }

    fn list_resources(
        &self,
        request: ResourceListRequest,
    ) -> ResourceQueryFuture<'_, ResourceListResult> {
        Box::pin(super::resource::list_resources(
            self.grpc.as_ref(),
            self.cache.as_ref(),
            request,
        ))
    }
}

impl LeaderResourceCommand for RemoteApiClient {
    fn submit_resource_command(
        &self,
        request: ResourceCommandRequest,
    ) -> ResourceCommandFuture<'_, ResourceCommandResult> {
        Box::pin(super::resource::submit_resource_command(
            self.grpc.as_ref(),
            request,
        ))
    }
}

impl LeaderNodeLeaseRenewal for RemoteApiClient {
    fn renew_node_lease(
        &self,
        request: NodeLeaseRenewalRequest,
    ) -> NodeLeaseRenewalFuture<'_, NodeLeaseRenewalResult> {
        Box::pin(async move {
            if request.node_name() != self.node_name {
                return Err(NodeLeaseRenewalError::unauthorized(format!(
                    "node {} cannot renew the lease for {}",
                    self.node_name,
                    request.node_name()
                )));
            }
            let grpc = self.grpc.as_ref().ok_or_else(|| {
                NodeLeaseRenewalError::unavailable("RemoteApiClient missing gRPC transport")
            })?;
            grpc.renew_node_lease_focused_rpc(
                request.renew_time(),
                request.lease_duration_seconds(),
            )
            .await?;
            Ok(NodeLeaseRenewalResult::Renewed)
        })
    }
}

impl LeaderWatch for RemoteApiClient {
    fn watch_resources(&self, req: WatchRequest) -> LeaderWatchFuture<'_> {
        Box::pin(super::watch::watch_resources(self.grpc.as_ref(), req))
    }
}

impl LeaderCacheReadiness for RemoteApiClient {
    fn wait_cache_ready(&self, scope: CacheReadinessRequest) -> CacheReadinessFuture<'_> {
        Box::pin(super::watch::wait_cache_ready(
            self.grpc.is_some(),
            self.cache.as_ref(),
            scope,
        ))
    }
}

impl LeaderProjectedServiceAccountToken for RemoteApiClient {
    fn issue_projected_service_account_token(
        &self,
        request: ProjectedServiceAccountTokenRequest,
    ) -> ProjectedServiceAccountTokenFuture<'_> {
        Box::pin(async move {
            let grpc = self.grpc.as_ref().ok_or_else(|| {
                ProjectedServiceAccountTokenError::unavailable(
                    "RemoteApiClient missing gRPC transport",
                )
            })?;
            grpc.projected_service_account_token_rpc(request).await
        })
    }
}

impl LeaderPodCleanupIntents for RemoteApiClient {
    fn list_pod_cleanup_intents(
        &self,
        request: PodCleanupIntentListRequest,
    ) -> PodCleanupIntentFuture<'_, Vec<PodCleanupIntent>> {
        Box::pin(async move {
            let grpc = self.grpc.as_ref().ok_or_else(|| {
                PodCleanupIntentError::unavailable("RemoteApiClient missing gRPC transport")
            })?;
            grpc.list_pod_cleanup_intents_for_node_rpc(request).await
        })
    }

    fn acknowledge_pod_cleanup_intent(
        &self,
        request: PodCleanupIntentAckRequest,
    ) -> PodCleanupIntentFuture<'_, ()> {
        Box::pin(async move {
            let grpc = self.grpc.as_ref().ok_or_else(|| {
                PodCleanupIntentError::unavailable("RemoteApiClient missing gRPC transport")
            })?;
            grpc.delete_pod_cleanup_intent_rpc(request).await
        })
    }
}

impl LeaderNodeSubnetAllocation for RemoteApiClient {
    fn allocate_node_subnet(
        &self,
        request: NodeSubnetAllocationRequest,
    ) -> NodeSubnetAllocationFuture<'_, NodeSubnetAllocationResult> {
        Box::pin(async move {
            let grpc = self.grpc.as_ref().ok_or_else(|| {
                NodeSubnetAllocationError::retryable("RemoteApiClient missing gRPC transport")
            })?;
            grpc.allocate_node_subnet_rpc(request).await
        })
    }
}

impl LeaderNetworkTopologyQuery for RemoteApiClient {
    fn get_node_subnet(
        &self,
        request: NodeSubnetQuery,
    ) -> NetworkTopologyFuture<'_, NodeSubnetResult> {
        Box::pin(async move {
            let grpc = self.grpc.as_ref().ok_or_else(|| {
                NetworkTopologyError::retryable("RemoteApiClient missing gRPC transport")
            })?;
            grpc.get_node_subnet_rpc(request).await
        })
    }

    fn list_peer_subnets(
        &self,
        request: PeerSubnetsQuery,
    ) -> NetworkTopologyFuture<'_, PeerSubnetsResult> {
        Box::pin(async move {
            let grpc = self.grpc.as_ref().ok_or_else(|| {
                NetworkTopologyError::retryable("RemoteApiClient missing gRPC transport")
            })?;
            grpc.list_peer_subnets_rpc(request).await
        })
    }

    fn get_node_dataplane(
        &self,
        request: NodeDataplaneQuery,
    ) -> NetworkTopologyFuture<'_, NodeDataplaneResult> {
        Box::pin(async move {
            let grpc = self.grpc.as_ref().ok_or_else(|| {
                NetworkTopologyError::retryable("RemoteApiClient missing gRPC transport")
            })?;
            grpc.get_node_dataplane_rpc(request).await
        })
    }
}

#[async_trait]
impl LeaderOutboxDelivery for RemoteApiClient {
    fn deliver_outbox(&self, request: OutboxDeliveryRequest) -> OutboxDeliveryFuture<'_> {
        Box::pin(async move {
            let grpc = self.grpc.as_ref().ok_or_else(|| {
                OutboxDeliveryError::unavailable("RemoteApiClient missing gRPC transport")
            })?;
            if grpc.node_name() != self.node_name {
                return Err(OutboxDeliveryError::conflict(format!(
                    "RemoteApiClient node identity {} does not match gRPC identity {}",
                    self.node_name,
                    grpc.node_name(),
                )));
            }
            grpc.deliver_outbox(request).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::RemoteApiClient;

    #[test]
    fn all_required_worker_cache_scopes_prime() {
        let requests = RemoteApiClient::required_worker_list_requests("worker-1");
        assert!(requests.iter().any(|request| request.api_version() == "v1"
            && request.kind() == "Pod"
            && request.field_selector() == Some("spec.nodeName=worker-1")));
        for (api_version, kind, namespace) in [
            ("v1", "ConfigMap", None),
            ("v1", "Secret", None),
            ("v1", "PersistentVolumeClaim", None),
            ("v1", "PersistentVolume", None),
            ("node.k8s.io/v1", "RuntimeClass", None),
            ("scheduling.k8s.io/v1", "PriorityClass", None),
            ("v1", "ServiceAccount", None),
            ("v1", "Service", None),
            ("v1", "Endpoints", None),
            ("discovery.k8s.io/v1", "EndpointSlice", None),
            ("v1", "Node", None),
            ("coordination.k8s.io/v1", "Lease", Some("kube-node-lease")),
            ("v1", "Namespace", None),
        ] {
            assert!(
                requests
                    .iter()
                    .any(|request| request.api_version() == api_version
                        && request.kind() == kind
                        && request.namespace() == namespace),
                "missing worker cache scope {api_version}/{kind}/{namespace:?}"
            );
        }
    }
}
