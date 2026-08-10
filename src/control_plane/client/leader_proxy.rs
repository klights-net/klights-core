//! T6 step 3: switching `LeaderApiClient` for non-leader leader-class boots.
//!
//! Every leader-class member (cp + replica) holds the same `cluster_api`
//! binding — a `LeaderProxyApiClient` that wraps a local
//! `LocalApiClient` and a remote forwarder. Per-call dispatch:
//!
//! - **Kubernetes API reads and watches** go to the elected leader. When this
//!   member is leader they use the local client; otherwise they use the remote
//!   forwarder. Followers do not serve application reads from their local
//!   raft-applied `cluster.db`.
//! - **Pod cleanup intent reads/deletes** prefer the remote leader path.
//!   Startup recovery may run before a rejoining old leader has observed
//!   its demotion, so the local leadership watch can briefly be stale.
//! - **Writes** consult `is_leader_rx` on entry: when `true` they
//!   dispatch to the local client (which routes through the local
//!   datastore → raft proposer → raft → state-machine apply); when
//!   `false` they dispatch to the remote forwarder, which carries the
//!   call to the current elected leader's API server over gRPC.
//!
//! Leadership change is a state flip on the same instance — no
//! re-construction, no rewiring. The proxy reads `is_leader_rx` per
//! call, so the next read or write after promotion / demotion picks the new
//! dispatch target without any setup.
//!
//! Every capability is stored as its own focused trait object. Tests inject
//! recording fakes through `LeaderClientPorts` and assert the dispatch table
//! without spinning up a real cluster.

use std::sync::Arc;

#[cfg(test)]
use bytes::Bytes;
use futures::StreamExt as _;
#[cfg(test)]
use klights_kubelet::node_outbox::payload::OutboxOperationExt as _;
use klights_leader_api::{
    CacheReadinessError, CacheReadinessFuture, CacheReadinessRequest, LeaderCacheReadiness,
    LeaderNetworkTopologyQuery, LeaderNodeLeaseRenewal, LeaderNodeSubnetAllocation,
    LeaderOutboxDelivery, LeaderPodCleanupIntents, LeaderProjectedServiceAccountToken,
    LeaderResourceCommand, LeaderResourceQuery, LeaderWatch, LeaderWatchError, LeaderWatchFuture,
    NetworkTopologyError, NetworkTopologyFuture, NodeDataplaneQuery, NodeDataplaneResult,
    NodeLeaseRenewalError, NodeLeaseRenewalFuture, NodeLeaseRenewalRequest, NodeLeaseRenewalResult,
    NodeSubnetAllocationError, NodeSubnetAllocationFuture, NodeSubnetAllocationRequest,
    NodeSubnetAllocationResult, NodeSubnetQuery, NodeSubnetResult, OutboxDeliveryError,
    OutboxDeliveryFuture, OutboxDeliveryRequest, PeerSubnetsQuery, PeerSubnetsResult,
    PodCleanupIntent, PodCleanupIntentAckRequest, PodCleanupIntentError, PodCleanupIntentFuture,
    PodCleanupIntentListRequest, ProjectedServiceAccountTokenFuture,
    ProjectedServiceAccountTokenRequest, ResourceCommandError, ResourceCommandFuture,
    ResourceCommandRequest, ResourceCommandResult, ResourceGetRequest, ResourceListRequest,
    ResourceListResult, ResourceQueryConsistency, ResourceQueryFuture, WatchRequest, WatchStream,
};
use tokio::sync::watch;

use super::LeaderClientPorts;
use klights_cluster_core::Resource;

#[cfg(test)]
use klights_leader_api::pod_get_request;

struct ArcPair<T: ?Sized> {
    local: Option<Arc<T>>,
    remote: Option<Arc<T>>,
}

impl<T: ?Sized> Clone for ArcPair<T> {
    fn clone(&self) -> Self {
        Self {
            local: self.local.clone(),
            remote: self.remote.clone(),
        }
    }
}

impl<T: ?Sized> ArcPair<T> {
    fn empty() -> Self {
        Self {
            local: None,
            remote: None,
        }
    }

    fn set(&mut self, local: Arc<T>, remote: Arc<T>) {
        self.local = Some(local);
        self.remote = Some(remote);
    }

    fn target(&self, is_leader: bool) -> Option<&Arc<T>> {
        if is_leader {
            self.local.as_ref()
        } else {
            self.remote.as_ref()
        }
    }
}

#[derive(Clone)]
struct FocusedLeaderTargets {
    resource_commands: ArcPair<dyn LeaderResourceCommand>,
    node_lease_renewals: ArcPair<dyn LeaderNodeLeaseRenewal>,
    outbox_deliveries: ArcPair<dyn LeaderOutboxDelivery>,
}

impl FocusedLeaderTargets {
    fn empty() -> Self {
        Self {
            resource_commands: ArcPair::empty(),
            node_lease_renewals: ArcPair::empty(),
            outbox_deliveries: ArcPair::empty(),
        }
    }
}

/// Leader-aware `LeaderApiClient` that dispatches each call to a local
/// `LocalApiClient` (reads, plus writes when self is the elected
/// leader) or a remote forwarder (writes when self is a follower /
/// learner). The decision is per-call; promotion / demotion flips the
/// watch and the next write picks the new target without rewiring.
pub struct LeaderProxyApiClient {
    resource_queries: ArcPair<dyn LeaderResourceQuery>,
    watches: ArcPair<dyn LeaderWatch>,
    cache_readiness: ArcPair<dyn LeaderCacheReadiness>,
    projected_tokens: ArcPair<dyn LeaderProjectedServiceAccountToken>,
    pod_cleanup_intents: ArcPair<dyn LeaderPodCleanupIntents>,
    node_subnet_allocations: ArcPair<dyn LeaderNodeSubnetAllocation>,
    network_topology: ArcPair<dyn LeaderNetworkTopologyQuery>,
    focused_targets: Option<Arc<FocusedLeaderTargets>>,
    is_leader_rx: watch::Receiver<bool>,
}

impl LeaderProxyApiClient {
    /// Construct a switching proxy.
    ///
    /// `local` handles all reads and writes-while-leader. `remote`
    /// handles writes-while-follower (forwarding to the current
    /// elected leader). `is_leader_rx` is the bootstrap's leadership
    /// watch — the SAME receiver fed to `LocalApiClient`'s gate, so
    /// the two layers can never disagree about who the leader is.
    pub fn new(
        local: LeaderClientPorts,
        remote: LeaderClientPorts,
        is_leader_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            resource_queries: ArcPair {
                local: Some(local.resource_query),
                remote: Some(remote.resource_query),
            },
            watches: ArcPair {
                local: Some(local.watch),
                remote: Some(remote.watch),
            },
            cache_readiness: ArcPair {
                local: Some(local.cache_readiness),
                remote: Some(remote.cache_readiness),
            },
            projected_tokens: ArcPair {
                local: Some(local.projected_tokens),
                remote: Some(remote.projected_tokens),
            },
            pod_cleanup_intents: ArcPair {
                local: Some(local.pod_cleanup_intents),
                remote: Some(remote.pod_cleanup_intents),
            },
            node_subnet_allocations: ArcPair {
                local: Some(local.node_subnet_allocation),
                remote: Some(remote.node_subnet_allocation),
            },
            network_topology: ArcPair {
                local: Some(local.network_topology),
                remote: Some(remote.network_topology),
            },
            focused_targets: None,
            is_leader_rx,
        }
    }

    #[cfg(test)]
    fn from_clients<L, R>(
        local: Arc<L>,
        remote: Arc<R>,
        is_leader_rx: watch::Receiver<bool>,
    ) -> Self
    where
        L: LeaderResourceQuery
            + LeaderWatch
            + LeaderCacheReadiness
            + LeaderProjectedServiceAccountToken
            + LeaderPodCleanupIntents
            + LeaderNodeSubnetAllocation
            + LeaderNetworkTopologyQuery
            + Send
            + Sync
            + 'static,
        R: LeaderResourceQuery
            + LeaderWatch
            + LeaderCacheReadiness
            + LeaderProjectedServiceAccountToken
            + LeaderPodCleanupIntents
            + LeaderNodeSubnetAllocation
            + LeaderNetworkTopologyQuery
            + Send
            + Sync
            + 'static,
    {
        Self::new(
            LeaderClientPorts::from_client(local),
            LeaderClientPorts::from_client(remote),
            is_leader_rx,
        )
    }

    pub fn with_resource_command_targets(
        mut self,
        local: Arc<dyn LeaderResourceCommand>,
        remote: Arc<dyn LeaderResourceCommand>,
    ) -> Self {
        self.focused_targets_mut()
            .resource_commands
            .set(local, remote);
        self
    }

    pub fn with_outbox_delivery_targets(
        mut self,
        local: Arc<dyn LeaderOutboxDelivery>,
        remote: Arc<dyn LeaderOutboxDelivery>,
    ) -> Self {
        self.focused_targets_mut()
            .outbox_deliveries
            .set(local, remote);
        self
    }

    pub fn with_node_lease_renewal_targets(
        mut self,
        local: Arc<dyn LeaderNodeLeaseRenewal>,
        remote: Arc<dyn LeaderNodeLeaseRenewal>,
    ) -> Self {
        self.focused_targets_mut()
            .node_lease_renewals
            .set(local, remote);
        self
    }

    fn focused_targets_mut(&mut self) -> &mut FocusedLeaderTargets {
        let targets = self
            .focused_targets
            .get_or_insert_with(|| Arc::new(FocusedLeaderTargets::empty()));
        Arc::make_mut(targets)
    }

    fn is_leader(&self) -> bool {
        *self.is_leader_rx.borrow()
    }

    fn target<'a, T: ?Sized>(&self, pair: &'a ArcPair<T>) -> &'a Arc<T> {
        pair.target(self.is_leader())
            .expect("leader proxy focused target is wired at construction")
    }

    #[cfg(test)]
    async fn deliver_test_outbox(
        &self,
        idempotency_key: &str,
        operation: klights_kubelet::node_outbox::payload::OutboxOperation,
        payload: Bytes,
        client_id: &str,
        stream_id: i64,
        stream_seq: i64,
    ) -> std::result::Result<
        klights_leader_api::OutboxDeliveryResult,
        klights_leader_api::OutboxDeliveryError,
    > {
        let request = OutboxDeliveryRequest::try_new(
            idempotency_key,
            operation.try_delivery_operation()?,
            Arc::<[u8]>::from(payload.to_vec()),
            client_id,
            stream_id,
            stream_seq,
        )?;
        self.deliver_outbox(request).await
    }
}

impl LeaderResourceCommand for LeaderProxyApiClient {
    fn submit_resource_command(
        &self,
        request: ResourceCommandRequest,
    ) -> ResourceCommandFuture<'_, ResourceCommandResult> {
        let mut leadership_rx = self.is_leader_rx.clone();
        let generation_is_leader = *leadership_rx.borrow_and_update();
        let target = self.focused_targets.as_ref().and_then(|targets| {
            targets
                .resource_commands
                .target(generation_is_leader)
                .cloned()
        });
        Box::pin(async move {
            if leadership_rx.has_changed().unwrap_or(true) {
                return Err(ResourceCommandError::retryable(
                    "leadership changed before resource-command dispatch",
                ));
            }
            match target {
                Some(target) => target.submit_resource_command(request).await,
                None => Err(ResourceCommandError::retryable(
                    "leader proxy resource-command target is not wired",
                )),
            }
        })
    }
}

impl LeaderNodeLeaseRenewal for LeaderProxyApiClient {
    fn renew_node_lease(
        &self,
        request: NodeLeaseRenewalRequest,
    ) -> NodeLeaseRenewalFuture<'_, NodeLeaseRenewalResult> {
        let target = self
            .focused_targets
            .as_ref()
            .and_then(|targets| targets.node_lease_renewals.target(self.is_leader()));
        match target {
            Some(target) => target.renew_node_lease(request),
            None => Box::pin(async {
                Err(NodeLeaseRenewalError::unavailable(
                    "leader proxy Node lease-renewal target is not wired",
                ))
            }),
        }
    }
}

fn terminate_watch_on_leadership_change(
    stream: WatchStream,
    leadership_rx: watch::Receiver<bool>,
) -> WatchStream {
    stream.map_inner(|stream| {
        Box::pin(futures::stream::unfold(
            (stream, leadership_rx),
            move |(mut stream, mut leadership_rx)| async move {
                tokio::select! {
                    biased;
                    changed = leadership_rx.changed() => {
                        let _ = changed;
                        None
                    }
                    item = stream.next() => {
                        item.map(|item| (item, (stream, leadership_rx)))
                    }
                }
            },
        ))
    })
}

impl LeaderResourceQuery for LeaderProxyApiClient {
    fn get_resource(
        &self,
        request: ResourceGetRequest,
    ) -> ResourceQueryFuture<'_, Option<Resource>> {
        let mut leadership_rx = self.is_leader_rx.clone();
        let initial_is_leader = *leadership_rx.borrow_and_update();
        let target = self
            .resource_queries
            .target(initial_is_leader)
            .expect("resource-query targets are wired")
            .clone();
        Box::pin(async move {
            let consistency = request.consistency();
            let result = target.get_resource(request).await?;
            if consistency == ResourceQueryConsistency::LeaderFresh
                && leadership_rx.has_changed().unwrap_or(true)
            {
                return Err(klights_leader_api::ResourceQueryError::retryable(
                    "leadership changed during leader-fresh resource query",
                ));
            }
            Ok(result)
        })
    }

    fn list_resources(
        &self,
        request: ResourceListRequest,
    ) -> ResourceQueryFuture<'_, ResourceListResult> {
        let mut leadership_rx = self.is_leader_rx.clone();
        let initial_is_leader = *leadership_rx.borrow_and_update();
        let target = self
            .resource_queries
            .target(initial_is_leader)
            .expect("resource-query targets are wired")
            .clone();
        Box::pin(async move {
            let consistency = request.consistency();
            let result = target.list_resources(request).await?;
            if consistency == ResourceQueryConsistency::LeaderFresh
                && leadership_rx.has_changed().unwrap_or(true)
            {
                return Err(klights_leader_api::ResourceQueryError::retryable(
                    "leadership changed during leader-fresh resource query",
                ));
            }
            Ok(result)
        })
    }
}

impl LeaderWatch for LeaderProxyApiClient {
    fn watch_resources(&self, req: WatchRequest) -> LeaderWatchFuture<'_> {
        let mut leadership_rx = self.is_leader_rx.clone();
        let initial_is_leader = *leadership_rx.borrow_and_update();
        let target = self
            .watches
            .target(initial_is_leader)
            .expect("watch targets are wired")
            .clone();
        Box::pin(async move {
            let stream = LeaderWatch::watch_resources(target.as_ref(), req).await?;
            if leadership_rx.has_changed().unwrap_or(true) {
                return Err(LeaderWatchError::unavailable(
                    "leadership changed while establishing watch",
                ));
            }
            Ok(terminate_watch_on_leadership_change(stream, leadership_rx))
        })
    }
}

impl LeaderCacheReadiness for LeaderProxyApiClient {
    fn wait_cache_ready(&self, scope: CacheReadinessRequest) -> CacheReadinessFuture<'_> {
        self.target(&self.cache_readiness).wait_cache_ready(scope)
    }
}

impl LeaderProjectedServiceAccountToken for LeaderProxyApiClient {
    fn issue_projected_service_account_token(
        &self,
        request: ProjectedServiceAccountTokenRequest,
    ) -> ProjectedServiceAccountTokenFuture<'_> {
        self.target(&self.projected_tokens)
            .issue_projected_service_account_token(request)
    }
}

impl LeaderPodCleanupIntents for LeaderProxyApiClient {
    fn list_pod_cleanup_intents(
        &self,
        request: PodCleanupIntentListRequest,
    ) -> PodCleanupIntentFuture<'_, Vec<PodCleanupIntent>> {
        Box::pin(async move {
            let local_is_leader = self.is_leader();
            let remote = self
                .pod_cleanup_intents
                .remote
                .as_ref()
                .expect("remote cleanup-intent target is wired");
            match remote.list_pod_cleanup_intents(request.clone()).await {
                Ok(intents) => Ok(intents),
                Err(_) if local_is_leader => {
                    self.pod_cleanup_intents
                        .local
                        .as_ref()
                        .expect("local cleanup-intent target is wired")
                        .list_pod_cleanup_intents(request)
                        .await
                }
                Err(error) => Err(error),
            }
        })
    }

    fn acknowledge_pod_cleanup_intent(
        &self,
        request: PodCleanupIntentAckRequest,
    ) -> PodCleanupIntentFuture<'_, ()> {
        Box::pin(async move {
            let remote = self
                .pod_cleanup_intents
                .remote
                .as_ref()
                .expect("remote cleanup-intent target is wired");
            match remote.acknowledge_pod_cleanup_intent(request.clone()).await {
                Ok(()) => Ok(()),
                Err(_) if self.is_leader() => {
                    self.pod_cleanup_intents
                        .local
                        .as_ref()
                        .expect("local cleanup-intent target is wired")
                        .acknowledge_pod_cleanup_intent(request)
                        .await
                }
                Err(error) => Err(error),
            }
        })
    }
}

impl LeaderNodeSubnetAllocation for LeaderProxyApiClient {
    fn allocate_node_subnet(
        &self,
        request: NodeSubnetAllocationRequest,
    ) -> NodeSubnetAllocationFuture<'_, NodeSubnetAllocationResult> {
        self.target(&self.node_subnet_allocations)
            .allocate_node_subnet(request)
    }
}

impl LeaderNetworkTopologyQuery for LeaderProxyApiClient {
    fn get_node_subnet(
        &self,
        request: NodeSubnetQuery,
    ) -> NetworkTopologyFuture<'_, NodeSubnetResult> {
        self.target(&self.network_topology).get_node_subnet(request)
    }

    fn list_peer_subnets(
        &self,
        request: PeerSubnetsQuery,
    ) -> NetworkTopologyFuture<'_, PeerSubnetsResult> {
        self.target(&self.network_topology)
            .list_peer_subnets(request)
    }

    fn get_node_dataplane(
        &self,
        request: NodeDataplaneQuery,
    ) -> NetworkTopologyFuture<'_, NodeDataplaneResult> {
        self.target(&self.network_topology)
            .get_node_dataplane(request)
    }
}

impl LeaderOutboxDelivery for LeaderProxyApiClient {
    fn deliver_outbox(&self, request: OutboxDeliveryRequest) -> OutboxDeliveryFuture<'_> {
        let target = self
            .focused_targets
            .as_ref()
            .and_then(|targets| targets.outbox_deliveries.target(self.is_leader()));
        match target {
            Some(target) => target.deliver_outbox(request),
            None => Box::pin(async {
                Err(OutboxDeliveryError::unavailable(
                    "leader proxy durable-delivery target is not wired",
                ))
            }),
        }
    }
}

/// T6 step 4 placeholder remote: surfaces every write attempt as a
/// clean Retryable / anyhow error pointing at "remote forwarder not
/// yet wired". This is the boot-time stub used until step 4b builds
/// the real gRPC forwarder pointing at the current elected leader.
///
/// The proxy never falls back from remote to local — when a non-leader
/// member's write hits this stub it returns immediately and the outbox
/// dispatcher re-queues. Combined with step 1's inner gate (which
/// refuses the same write on the local arm) the cluster.db on
/// non-leader members stays unchanged: writes pile up in the outbox
/// until either promotion (local arm opens) or step 4b ships (remote
/// arm forwards). No silent local-only writes happen.
pub struct StubRemoteForwarder {
    node_name: String,
}

impl StubRemoteForwarder {
    pub fn new(node_name: String) -> Self {
        Self { node_name }
    }

    fn unavailable(&self) -> String {
        format!(
            "remote leader forwarder not yet wired on {} (T6 step 4b); \
             non-leader writes pile up in the outbox until promotion or until \
             the forwarder lands",
            self.node_name
        )
    }
}

impl LeaderResourceQuery for StubRemoteForwarder {
    fn get_resource(
        &self,
        request: ResourceGetRequest,
    ) -> ResourceQueryFuture<'_, Option<Resource>> {
        let message = self.unavailable();
        Box::pin(async move {
            if request.consistency() == ResourceQueryConsistency::LeaderFresh {
                Err(klights_leader_api::ResourceQueryError::retryable(message))
            } else {
                Ok(None)
            }
        })
    }

    fn list_resources(
        &self,
        request: ResourceListRequest,
    ) -> ResourceQueryFuture<'_, ResourceListResult> {
        let message = self.unavailable();
        Box::pin(async move {
            if request.consistency() == ResourceQueryConsistency::LeaderFresh {
                return Err(klights_leader_api::ResourceQueryError::retryable(message));
            }
            ResourceListResult::try_new(
                Vec::new(),
                0,
                None,
                request.continue_token().map(str::to_owned),
                None,
            )
        })
    }
}

impl LeaderResourceCommand for StubRemoteForwarder {
    fn submit_resource_command(
        &self,
        _request: ResourceCommandRequest,
    ) -> ResourceCommandFuture<'_, ResourceCommandResult> {
        let message = self.unavailable();
        Box::pin(async move { Err(ResourceCommandError::retryable(message)) })
    }
}

impl LeaderNodeLeaseRenewal for StubRemoteForwarder {
    fn renew_node_lease(
        &self,
        _request: NodeLeaseRenewalRequest,
    ) -> NodeLeaseRenewalFuture<'_, NodeLeaseRenewalResult> {
        let message = self.unavailable();
        Box::pin(async move { Err(NodeLeaseRenewalError::unavailable(message)) })
    }
}

impl LeaderWatch for StubRemoteForwarder {
    fn watch_resources(&self, _req: WatchRequest) -> LeaderWatchFuture<'_> {
        let message = self.unavailable();
        Box::pin(async move { Err(LeaderWatchError::unavailable(message)) })
    }
}

impl LeaderCacheReadiness for StubRemoteForwarder {
    fn wait_cache_ready(&self, _scope: CacheReadinessRequest) -> CacheReadinessFuture<'_> {
        let message = self.unavailable();
        Box::pin(async move { Err(CacheReadinessError::unavailable(message)) })
    }
}

impl LeaderProjectedServiceAccountToken for StubRemoteForwarder {
    fn issue_projected_service_account_token(
        &self,
        _request: ProjectedServiceAccountTokenRequest,
    ) -> ProjectedServiceAccountTokenFuture<'_> {
        let message = self.unavailable();
        Box::pin(async move {
            Err(klights_leader_api::ProjectedServiceAccountTokenError::unavailable(message))
        })
    }
}

impl LeaderPodCleanupIntents for StubRemoteForwarder {
    fn list_pod_cleanup_intents(
        &self,
        _request: PodCleanupIntentListRequest,
    ) -> PodCleanupIntentFuture<'_, Vec<PodCleanupIntent>> {
        let message = self.unavailable();
        Box::pin(async move { Err(PodCleanupIntentError::unavailable(message)) })
    }

    fn acknowledge_pod_cleanup_intent(
        &self,
        _request: PodCleanupIntentAckRequest,
    ) -> PodCleanupIntentFuture<'_, ()> {
        let message = self.unavailable();
        Box::pin(async move { Err(PodCleanupIntentError::unavailable(message)) })
    }
}

impl LeaderNodeSubnetAllocation for StubRemoteForwarder {
    fn allocate_node_subnet(
        &self,
        _request: NodeSubnetAllocationRequest,
    ) -> NodeSubnetAllocationFuture<'_, NodeSubnetAllocationResult> {
        let message = self.unavailable();
        Box::pin(async move { Err(NodeSubnetAllocationError::retryable(message)) })
    }
}

impl LeaderNetworkTopologyQuery for StubRemoteForwarder {
    fn get_node_subnet(
        &self,
        _request: NodeSubnetQuery,
    ) -> NetworkTopologyFuture<'_, NodeSubnetResult> {
        let message = self.unavailable();
        Box::pin(async move { Err(NetworkTopologyError::retryable(message)) })
    }

    fn list_peer_subnets(
        &self,
        _request: PeerSubnetsQuery,
    ) -> NetworkTopologyFuture<'_, PeerSubnetsResult> {
        let message = self.unavailable();
        Box::pin(async move { Err(NetworkTopologyError::retryable(message)) })
    }

    fn get_node_dataplane(
        &self,
        _request: NodeDataplaneQuery,
    ) -> NetworkTopologyFuture<'_, NodeDataplaneResult> {
        let message = self.unavailable();
        Box::pin(async move { Err(NetworkTopologyError::retryable(message)) })
    }
}

impl LeaderOutboxDelivery for StubRemoteForwarder {
    fn deliver_outbox(&self, _request: OutboxDeliveryRequest) -> OutboxDeliveryFuture<'_> {
        let message = self.unavailable();
        Box::pin(async move { Err(OutboxDeliveryError::unavailable(message)) })
    }
}

#[cfg(test)]
#[path = "tests/authority.rs"]
mod tests;
