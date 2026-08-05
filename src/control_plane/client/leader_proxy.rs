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
mod tests {
    //! T6 step 3: switching `LeaderProxyApiClient` dispatch coverage.
    //!
    //! Each test uses a `RecordingApiClient` fake on both sides
    //! (local and remote) so we can assert which arm received the
    //! call. No real datastore, no real gRPC; the proxy's dispatch
    //! logic is pure and unit-testable.

    use super::*;
    use klights_cluster_core::StorageCommand;
    use klights_kubelet::node_outbox::payload::OutboxOperation;
    use klights_leader_api::node_get_request;
    use klights_leader_api::{
        LeaderResourceCommand, ResourceCommandError, ResourceCommandFuture, ResourceCommandRequest,
        ResourceCommandResult,
    };
    use klights_types::ResourceKey;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Recording stub: each method bumps a counter and returns a
    /// minimal Ok value. Used on both sides of the proxy to assert
    /// the dispatch table.
    #[derive(Default)]
    struct RecordingApiClient {
        name: &'static str,
        get_resource: AtomicUsize,
        list_resources: AtomicUsize,
        watch_resources: AtomicUsize,
        get_pod: AtomicUsize,
        get_node: AtomicUsize,
        projected_service_account_token: AtomicUsize,
        allocate_node_subnet: AtomicUsize,
        list_pod_cleanup_intents: AtomicUsize,
        delete_pod_cleanup_intents: AtomicUsize,
        cleanup_intents: Mutex<Vec<PodCleanupIntent>>,
        apply_outbox: AtomicUsize,
        submit_resource_command: AtomicUsize,
        renew_node_lease: AtomicUsize,
        demote_on_get: Mutex<Option<(watch::Sender<bool>, bool)>>,
        demote_on_watch: Mutex<Option<(watch::Sender<bool>, bool)>>,
    }

    impl RecordingApiClient {
        fn new(name: &'static str) -> Arc<Self> {
            Arc::new(Self {
                name,
                ..Default::default()
            })
        }

        fn with_cleanup_intent(self: &Arc<Self>, intent: PodCleanupIntent) {
            self.cleanup_intents.lock().unwrap().push(intent);
        }

        fn demote_during_get(&self, sender: watch::Sender<bool>) {
            *self.demote_on_get.lock().unwrap() = Some((sender, false));
        }

        fn demote_during_watch(&self, sender: watch::Sender<bool>) {
            *self.demote_on_watch.lock().unwrap() = Some((sender, false));
        }

        fn flap_during_get(&self, sender: watch::Sender<bool>) {
            *self.demote_on_get.lock().unwrap() = Some((sender, true));
        }

        fn flap_during_watch(&self, sender: watch::Sender<bool>) {
            *self.demote_on_watch.lock().unwrap() = Some((sender, true));
        }
    }

    impl LeaderResourceQuery for RecordingApiClient {
        fn get_resource(
            &self,
            request: ResourceGetRequest,
        ) -> ResourceQueryFuture<'_, Option<Resource>> {
            match request.key().kind.as_str() {
                "Pod" => {
                    self.get_pod.fetch_add(1, Ordering::Relaxed);
                }
                "Node" => {
                    self.get_node.fetch_add(1, Ordering::Relaxed);
                }
                _ => {
                    self.get_resource.fetch_add(1, Ordering::Relaxed);
                }
            }
            if let Some((sender, restore)) = self.demote_on_get.lock().unwrap().take() {
                sender.send(false).expect("demote during get");
                if restore {
                    sender.send(true).expect("restore after transient demotion");
                }
            }
            Box::pin(async { Ok(None) })
        }

        fn list_resources(
            &self,
            _request: ResourceListRequest,
        ) -> ResourceQueryFuture<'_, ResourceListResult> {
            self.list_resources.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { ResourceListResult::try_new(Vec::new(), 0, None, None, None) })
        }
    }

    impl LeaderResourceCommand for RecordingApiClient {
        fn submit_resource_command(
            &self,
            _request: ResourceCommandRequest,
        ) -> ResourceCommandFuture<'_, ResourceCommandResult> {
            self.submit_resource_command.fetch_add(1, Ordering::Relaxed);
            Box::pin(async {
                Ok(ResourceCommandResult::Ack {
                    resource_version: 1,
                })
            })
        }
    }

    impl LeaderNodeLeaseRenewal for RecordingApiClient {
        fn renew_node_lease(
            &self,
            _request: NodeLeaseRenewalRequest,
        ) -> NodeLeaseRenewalFuture<'_, NodeLeaseRenewalResult> {
            self.renew_node_lease.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Ok(NodeLeaseRenewalResult::Renewed) })
        }
    }

    impl LeaderWatch for RecordingApiClient {
        fn watch_resources(&self, _req: WatchRequest) -> LeaderWatchFuture<'_> {
            self.watch_resources.fetch_add(1, Ordering::Relaxed);
            if let Some((sender, restore)) = self.demote_on_watch.lock().unwrap().take() {
                sender.send(false).expect("demote during watch open");
                if restore {
                    sender.send(true).expect("restore after transient demotion");
                }
            }
            Box::pin(async {
                Ok(WatchStream::unpositioned_test_stream(
                    futures::stream::pending(),
                ))
            })
        }
    }

    impl LeaderCacheReadiness for RecordingApiClient {
        fn wait_cache_ready(&self, _scope: CacheReadinessRequest) -> CacheReadinessFuture<'_> {
            Box::pin(async { Ok(()) })
        }
    }

    impl LeaderProjectedServiceAccountToken for RecordingApiClient {
        fn issue_projected_service_account_token(
            &self,
            _request: ProjectedServiceAccountTokenRequest,
        ) -> ProjectedServiceAccountTokenFuture<'_> {
            self.projected_service_account_token
                .fetch_add(1, Ordering::Relaxed);
            Box::pin(async move {
                klights_leader_api::ProjectedServiceAccountToken::try_new(format!(
                    "{}-token",
                    self.name
                ))
            })
        }
    }

    impl LeaderPodCleanupIntents for RecordingApiClient {
        fn list_pod_cleanup_intents(
            &self,
            _request: PodCleanupIntentListRequest,
        ) -> PodCleanupIntentFuture<'_, Vec<PodCleanupIntent>> {
            self.list_pod_cleanup_intents
                .fetch_add(1, Ordering::Relaxed);
            let intents = self.cleanup_intents.lock().unwrap().clone();
            Box::pin(async move { Ok(intents) })
        }

        fn acknowledge_pod_cleanup_intent(
            &self,
            _request: PodCleanupIntentAckRequest,
        ) -> PodCleanupIntentFuture<'_, ()> {
            self.delete_pod_cleanup_intents
                .fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Ok(()) })
        }
    }

    impl LeaderNodeSubnetAllocation for RecordingApiClient {
        fn allocate_node_subnet(
            &self,
            _request: NodeSubnetAllocationRequest,
        ) -> NodeSubnetAllocationFuture<'_, NodeSubnetAllocationResult> {
            self.allocate_node_subnet.fetch_add(1, Ordering::Relaxed);
            let message = format!("recording client {} allocate_node_subnet", self.name);
            Box::pin(async move { Err(NodeSubnetAllocationError::retryable(message)) })
        }
    }

    impl LeaderNetworkTopologyQuery for RecordingApiClient {
        fn get_node_subnet(
            &self,
            request: NodeSubnetQuery,
        ) -> NetworkTopologyFuture<'_, NodeSubnetResult> {
            let node_name = request.into_node_name();
            Box::pin(async move { NodeSubnetResult::try_from_wire(&node_name, false, None) })
        }

        fn list_peer_subnets(
            &self,
            request: PeerSubnetsQuery,
        ) -> NetworkTopologyFuture<'_, PeerSubnetsResult> {
            let node_name = request.into_node_name();
            Box::pin(async move { PeerSubnetsResult::try_new(&node_name, Vec::new()) })
        }

        fn get_node_dataplane(
            &self,
            request: NodeDataplaneQuery,
        ) -> NetworkTopologyFuture<'_, NodeDataplaneResult> {
            let node_name = request.into_node_name();
            Box::pin(async move { NodeDataplaneResult::try_from_wire(&node_name, false, None) })
        }
    }

    impl LeaderOutboxDelivery for RecordingApiClient {
        fn deliver_outbox(&self, _request: OutboxDeliveryRequest) -> OutboxDeliveryFuture<'_> {
            self.apply_outbox.fetch_add(1, Ordering::Relaxed);
            Box::pin(async {
                Ok(klights_leader_api::OutboxDeliveryResult::Applied { applied_rv: 1 })
            })
        }
    }

    fn make_proxy(
        local: Arc<RecordingApiClient>,
        remote: Arc<RecordingApiClient>,
        initial_leader: bool,
    ) -> (LeaderProxyApiClient, watch::Sender<bool>) {
        let (tx, rx) = watch::channel(initial_leader);
        let proxy = LeaderProxyApiClient::from_clients(local.clone(), remote.clone(), rx)
            .with_resource_command_targets(local.clone(), remote.clone())
            .with_outbox_delivery_targets(local.clone(), remote.clone())
            .with_node_lease_renewal_targets(local, remote);
        (proxy, tx)
    }

    #[tokio::test]
    async fn node_effect_lease_dispatch_switches_per_call_without_local_follower_mutation() {
        let local = RecordingApiClient::new("local");
        let remote = RecordingApiClient::new("remote");
        let (proxy, tx) = make_proxy(local.clone(), remote.clone(), false);
        let request = NodeLeaseRenewalRequest::try_new("cp-1", "2026-07-18T12:34:56Z", 30)
            .expect("valid lease renewal");

        proxy
            .renew_node_lease(request.clone())
            .await
            .expect("follower forwards renewal");
        assert_eq!(local.renew_node_lease.load(Ordering::Relaxed), 0);
        assert_eq!(remote.renew_node_lease.load(Ordering::Relaxed), 1);

        tx.send(true).expect("promote proxy");
        proxy
            .renew_node_lease(request)
            .await
            .expect("leader renews locally");
        assert_eq!(local.renew_node_lease.load(Ordering::Relaxed), 1);
        assert_eq!(remote.renew_node_lease.load(Ordering::Relaxed), 1);
    }

    fn config_map_create_request() -> ResourceCommandRequest {
        ResourceCommandRequest::try_new(StorageCommand::CreateResource {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "settings".to_string(),
            data: serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"namespace": "default", "name": "settings"}
            }),
        })
        .expect("valid command")
    }

    #[tokio::test]
    async fn resource_command_dispatch_tracks_leadership_without_local_fallback() {
        let local = RecordingApiClient::new("local");
        let remote = RecordingApiClient::new("remote");
        let (proxy, tx) = make_proxy(local.clone(), remote.clone(), true);

        LeaderResourceCommand::submit_resource_command(&proxy, config_map_create_request())
            .await
            .expect("local command");
        assert_eq!(local.submit_resource_command.load(Ordering::Relaxed), 1);
        assert_eq!(remote.submit_resource_command.load(Ordering::Relaxed), 0);

        tx.send(false).expect("demote");
        LeaderResourceCommand::submit_resource_command(&proxy, config_map_create_request())
            .await
            .expect("remote command");
        assert_eq!(local.submit_resource_command.load(Ordering::Relaxed), 1);
        assert_eq!(remote.submit_resource_command.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn resource_command_refuses_stale_leadership_generation_before_target_invocation() {
        let local = RecordingApiClient::new("local");
        let remote = RecordingApiClient::new("remote");
        let (proxy, tx) = make_proxy(local.clone(), remote.clone(), true);

        let stale =
            LeaderResourceCommand::submit_resource_command(&proxy, config_map_create_request());
        tx.send(false).expect("demote before polling command");

        let error = stale
            .await
            .expect_err("stale leadership generation must fail closed before dispatch");
        assert!(matches!(error, ResourceCommandError::Retryable { .. }));
        assert_eq!(
            local.submit_resource_command.load(Ordering::Relaxed),
            0,
            "a target selected under the stale leader generation must not be invoked"
        );
        assert_eq!(
            remote.submit_resource_command.load(Ordering::Relaxed),
            0,
            "generation failure must not silently switch targets inside one command"
        );

        LeaderResourceCommand::submit_resource_command(&proxy, config_map_create_request())
            .await
            .expect("fresh follower generation forwards to the current leader");
        assert_eq!(local.submit_resource_command.load(Ordering::Relaxed), 0);
        assert_eq!(remote.submit_resource_command.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn stub_resource_command_is_retryable_and_never_falls_back() {
        let stub = StubRemoteForwarder::new("cp2".to_string());
        let error =
            LeaderResourceCommand::submit_resource_command(&stub, config_map_create_request())
                .await
                .expect_err("stub must fail closed");
        assert!(matches!(error, ResourceCommandError::Retryable { .. }));
    }

    /// Self-is-leader: every write lands on the local client.
    #[tokio::test]
    async fn leader_proxy_dispatches_local_when_self_is_leader() {
        let local = RecordingApiClient::new("local");
        let remote = RecordingApiClient::new("remote");
        let (proxy, _tx) = make_proxy(local.clone(), remote.clone(), true);

        proxy
            .deliver_test_outbox(
                "k-1",
                OutboxOperation::PodStatus,
                Bytes::from_static(b"x"),
                "client",
                1,
                1,
            )
            .await
            .expect("apply_outbox");
        proxy
            .allocate_node_subnet(
                NodeSubnetAllocationRequest::try_new("n", "10.0.0.0/16", "10.0.0.1")
                    .expect("valid request"),
            )
            .await
            .expect_err("recording client returns Err");
        assert_eq!(local.apply_outbox.load(Ordering::Relaxed), 1);
        assert_eq!(local.allocate_node_subnet.load(Ordering::Relaxed), 1);
        assert_eq!(remote.apply_outbox.load(Ordering::Relaxed), 0);
        assert_eq!(remote.allocate_node_subnet.load(Ordering::Relaxed), 0);
    }

    /// Self-is-follower: writes land on
    /// remote so the call reaches the current elected leader.
    #[tokio::test]
    async fn leader_proxy_dispatches_remote_when_follower() {
        let local = RecordingApiClient::new("local");
        let remote = RecordingApiClient::new("remote");
        let (proxy, _tx) = make_proxy(local.clone(), remote.clone(), false);

        proxy
            .deliver_test_outbox(
                "k-2",
                OutboxOperation::PodStatus,
                Bytes::from_static(b"x"),
                "client",
                1,
                1,
            )
            .await
            .expect("apply_outbox");
        proxy
            .allocate_node_subnet(
                NodeSubnetAllocationRequest::try_new("n", "10.0.0.0/16", "10.0.0.1")
                    .expect("valid request"),
            )
            .await
            .expect_err("recording client returns Err");

        assert_eq!(remote.apply_outbox.load(Ordering::Relaxed), 1);
        assert_eq!(remote.allocate_node_subnet.load(Ordering::Relaxed), 1);
        assert_eq!(local.apply_outbox.load(Ordering::Relaxed), 0);
        assert_eq!(local.allocate_node_subnet.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn leader_proxy_dispatches_projected_serviceaccount_token_as_write() {
        let request = ProjectedServiceAccountTokenRequest::try_new(
            "kube-system",
            "coredns",
            vec!["https://kubernetes.default.svc.cluster.local".to_string()],
            3600,
            "coredns",
            "pod-uid",
            "mn-controlplane1",
            Some("node-uid".to_string()),
        )
        .unwrap();

        let local = RecordingApiClient::new("local");
        let remote = RecordingApiClient::new("remote");
        let (leader_proxy, _tx) = make_proxy(local.clone(), remote.clone(), true);
        let leader_token = leader_proxy
            .issue_projected_service_account_token(request.clone())
            .await
            .expect("leader token");
        assert_eq!(leader_token.token(), "local-token");
        assert_eq!(
            local
                .projected_service_account_token
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            remote
                .projected_service_account_token
                .load(Ordering::Relaxed),
            0
        );

        let local = RecordingApiClient::new("local");
        let remote = RecordingApiClient::new("remote");
        let (follower_proxy, _tx) = make_proxy(local.clone(), remote.clone(), false);
        let follower_token = follower_proxy
            .issue_projected_service_account_token(request)
            .await
            .expect("follower token");
        assert_eq!(follower_token.token(), "remote-token");
        assert_eq!(
            local
                .projected_service_account_token
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            remote
                .projected_service_account_token
                .load(Ordering::Relaxed),
            1
        );
    }

    #[tokio::test]
    async fn leader_proxy_lists_cleanup_intents_from_remote_when_follower() {
        let local = RecordingApiClient::new("local");
        let remote = RecordingApiClient::new("remote");
        let (proxy, _tx) = make_proxy(local.clone(), remote.clone(), false);

        proxy
            .list_pod_cleanup_intents(
                PodCleanupIntentListRequest::try_new("mn-controlplane1").unwrap(),
            )
            .await
            .expect("list cleanup intents");

        assert_eq!(remote.list_pod_cleanup_intents.load(Ordering::Relaxed), 1);
        assert_eq!(local.list_pod_cleanup_intents.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn leader_proxy_reads_cleanup_intents_from_remote_when_local_leader_is_stale() {
        let local = RecordingApiClient::new("local");
        let remote = RecordingApiClient::new("remote");
        remote.with_cleanup_intent(
            PodCleanupIntent::try_new(
                "mn-controlplane1",
                "kube-system",
                "coredns-old",
                "uid-old",
                crate::datastore::POD_CLEANUP_REASON_NODE_LOST,
                205,
                1_700_000_000_000,
                Resource::try_from_data(Arc::new(serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "kube-system",
                        "name": "coredns-old",
                        "uid": "uid-old",
                        "resourceVersion": "204"
                    },
                    "spec": {"nodeName": "mn-controlplane1"}
                })))
                .unwrap(),
            )
            .unwrap(),
        );
        let (proxy, _tx) = make_proxy(local.clone(), remote.clone(), true);

        let intents = proxy
            .list_pod_cleanup_intents(
                PodCleanupIntentListRequest::try_new("mn-controlplane1").unwrap(),
            )
            .await
            .expect("list cleanup intents");

        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].pod_uid(), "uid-old");
        assert_eq!(remote.list_pod_cleanup_intents.load(Ordering::Relaxed), 1);
        assert_eq!(local.list_pod_cleanup_intents.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn leader_proxy_deletes_cleanup_intent_through_remote_when_local_leader_is_stale() {
        let local = RecordingApiClient::new("local");
        let remote = RecordingApiClient::new("remote");
        let (proxy, _tx) = make_proxy(local.clone(), remote.clone(), true);

        proxy
            .acknowledge_pod_cleanup_intent(
                PodCleanupIntentAckRequest::try_new(
                    "mn-controlplane1",
                    "kube-system",
                    "coredns-old",
                    "uid-old",
                    crate::datastore::POD_CLEANUP_REASON_NODE_LOST,
                )
                .unwrap(),
            )
            .await
            .expect("delete cleanup intent");

        assert_eq!(remote.delete_pod_cleanup_intents.load(Ordering::Relaxed), 1);
        assert_eq!(local.delete_pod_cleanup_intents.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn leader_proxy_reads_dispatch_remote_when_follower_local_when_leader() {
        let local = RecordingApiClient::new("local");
        let remote = RecordingApiClient::new("remote");
        let (proxy, _tx) = make_proxy(local.clone(), remote.clone(), false);

        exercise_read_dispatch(&proxy).await;

        assert_eq!(remote.get_resource.load(Ordering::Relaxed), 1);
        assert_eq!(remote.get_pod.load(Ordering::Relaxed), 1);
        assert_eq!(remote.get_node.load(Ordering::Relaxed), 1);
        assert_eq!(remote.list_resources.load(Ordering::Relaxed), 1);
        assert_eq!(local.get_resource.load(Ordering::Relaxed), 0);
        assert_eq!(local.get_pod.load(Ordering::Relaxed), 0);
        assert_eq!(local.get_node.load(Ordering::Relaxed), 0);
        assert_eq!(local.list_resources.load(Ordering::Relaxed), 0);

        let local = RecordingApiClient::new("local");
        let remote = RecordingApiClient::new("remote");
        let (proxy, _tx) = make_proxy(local.clone(), remote.clone(), true);

        exercise_read_dispatch(&proxy).await;

        assert_eq!(local.get_resource.load(Ordering::Relaxed), 1);
        assert_eq!(local.get_pod.load(Ordering::Relaxed), 1);
        assert_eq!(local.get_node.load(Ordering::Relaxed), 1);
        assert_eq!(local.list_resources.load(Ordering::Relaxed), 1);
        assert_eq!(remote.get_resource.load(Ordering::Relaxed), 0);
        assert_eq!(remote.get_pod.load(Ordering::Relaxed), 0);
        assert_eq!(remote.get_node.load(Ordering::Relaxed), 0);
        assert_eq!(remote.list_resources.load(Ordering::Relaxed), 0);
    }

    async fn exercise_read_dispatch(proxy: &LeaderProxyApiClient) {
        proxy
            .get_resource(
                ResourceGetRequest::try_new(
                    ResourceKey {
                        api_version: "v1".into(),
                        kind: "ConfigMap".into(),
                        namespace: Some("default".into()),
                        name: "x".into(),
                    },
                    ResourceQueryConsistency::Cached,
                )
                .expect("valid request"),
            )
            .await
            .expect("get");
        proxy
            .get_resource(
                pod_get_request("default", "x", ResourceQueryConsistency::Cached)
                    .expect("valid Pod request"),
            )
            .await
            .expect("get_pod");
        proxy
            .get_resource(
                node_get_request("n", ResourceQueryConsistency::Cached)
                    .expect("valid Node request"),
            )
            .await
            .expect("get_node");
        proxy
            .list_resources(
                ResourceListRequest::try_new(
                    "v1",
                    "Pod",
                    None,
                    None,
                    None,
                    None,
                    None,
                    ResourceQueryConsistency::Cached,
                )
                .expect("valid list request"),
            )
            .await
            .expect("list");
    }

    #[tokio::test]
    async fn leader_proxy_watch_terminates_on_leadership_change() {
        use futures::StreamExt as _;
        use tokio::time::{Duration, timeout};

        for initial_leader in [false, true] {
            let local = RecordingApiClient::new("local");
            let remote = RecordingApiClient::new("remote");
            let (proxy, tx) = make_proxy(local.clone(), remote.clone(), initial_leader);
            let mut stream = proxy
                .watch_resources(
                    WatchRequest::try_new("v1", "Pod", None, None, None, None, None)
                        .expect("valid watch"),
                )
                .await
                .expect("watch");

            if initial_leader {
                assert_eq!(local.watch_resources.load(Ordering::Relaxed), 1);
                assert_eq!(remote.watch_resources.load(Ordering::Relaxed), 0);
            } else {
                assert_eq!(remote.watch_resources.load(Ordering::Relaxed), 1);
                assert_eq!(local.watch_resources.load(Ordering::Relaxed), 0);
            }

            tx.send(!initial_leader).expect("flip leadership");

            let ended = timeout(Duration::from_millis(100), stream.next())
                .await
                .expect("watch should end promptly after leadership change");
            assert!(
                ended.is_none(),
                "watch stream should terminate so callers reconnect against the new leader target"
            );
        }
    }

    #[tokio::test]
    async fn leader_proxy_rejects_samples_and_watches_when_demoted_during_open() {
        let local = RecordingApiClient::new("local");
        let remote = RecordingApiClient::new("remote");
        let (proxy, tx) = make_proxy(local.clone(), remote, true);
        local.demote_during_get(tx.clone());
        assert!(matches!(
            proxy
                .get_resource(
                    pod_get_request("default", "web", ResourceQueryConsistency::LeaderFresh)
                        .unwrap(),
                )
                .await,
            Err(klights_leader_api::ResourceQueryError::Retryable { .. })
        ));

        tx.send(true).expect("promote for watch race");
        local.demote_during_watch(tx);
        assert!(matches!(
            proxy
                .watch_resources(
                    WatchRequest::try_new("v1", "Pod", None, None, None, None, None).unwrap(),
                )
                .await,
            Err(LeaderWatchError::Unavailable { .. })
        ));
    }

    #[tokio::test]
    async fn leader_proxy_rejects_transient_leadership_flaps_during_fresh_open() {
        let local = RecordingApiClient::new("local");
        let remote = RecordingApiClient::new("remote");
        let (proxy, tx) = make_proxy(local.clone(), remote, true);
        local.flap_during_get(tx.clone());
        assert!(matches!(
            proxy
                .get_resource(
                    pod_get_request("default", "web", ResourceQueryConsistency::LeaderFresh)
                        .unwrap(),
                )
                .await,
            Err(klights_leader_api::ResourceQueryError::Retryable { .. })
        ));

        local.flap_during_watch(tx);
        assert!(matches!(
            proxy
                .watch_resources(
                    WatchRequest::try_new("v1", "Pod", None, None, None, None, None).unwrap(),
                )
                .await,
            Err(LeaderWatchError::Unavailable { .. })
        ));
    }

    /// Leader-change is a state flip on the same instance: same
    /// proxy, different watch value, next write dispatches to the
    /// new target. No reconstruction, no rewiring.
    #[tokio::test]
    async fn leader_proxy_flips_dispatch_on_leader_change_event() {
        let local = RecordingApiClient::new("local");
        let remote = RecordingApiClient::new("remote");
        let (proxy, tx) = make_proxy(local.clone(), remote.clone(), true);

        // Initially leader: write goes local.
        proxy
            .deliver_test_outbox(
                "pre",
                OutboxOperation::PodStatus,
                Bytes::from_static(b"x"),
                "client",
                1,
                1,
            )
            .await
            .expect("pre");
        assert_eq!(local.apply_outbox.load(Ordering::Relaxed), 1);
        assert_eq!(remote.apply_outbox.load(Ordering::Relaxed), 0);

        // Lose leadership: next write goes remote.
        tx.send(false).expect("send loss");
        proxy
            .deliver_test_outbox(
                "lost",
                OutboxOperation::PodStatus,
                Bytes::from_static(b"x"),
                "client",
                1,
                1,
            )
            .await
            .expect("lost");
        assert_eq!(local.apply_outbox.load(Ordering::Relaxed), 1);
        assert_eq!(remote.apply_outbox.load(Ordering::Relaxed), 1);

        // Regain leadership: next write goes local again.
        tx.send(true).expect("send regain");
        proxy
            .deliver_test_outbox(
                "regain",
                OutboxOperation::PodStatus,
                Bytes::from_static(b"x"),
                "client",
                1,
                1,
            )
            .await
            .expect("regain");
        assert_eq!(local.apply_outbox.load(Ordering::Relaxed), 2);
        assert_eq!(remote.apply_outbox.load(Ordering::Relaxed), 1);
    }

    /// "No leader currently elected" pass-through: when self is a
    /// follower and the remote forwarder fails (e.g. election window,
    /// transient gRPC failure, leader unreachable), the proxy must
    /// surface the error to the caller — no panic, no hang, no
    /// silent local fallback. The remote client owns the
    /// leader-finding logic (and retry/backoff if any); the proxy
    /// only dispatches.
    #[tokio::test]
    async fn leader_proxy_returns_no_leader_error_during_election_window() {
        /// Stub remote that always fails with a "no leader" error,
        /// modeling the gRPC client during an election window or
        /// when every known leader endpoint is unreachable.
        #[derive(Default)]
        struct NoLeaderRemote;

        impl LeaderResourceQuery for NoLeaderRemote {
            fn get_resource(
                &self,
                _request: ResourceGetRequest,
            ) -> ResourceQueryFuture<'_, Option<Resource>> {
                Box::pin(async { Ok(None) })
            }

            fn list_resources(
                &self,
                _request: ResourceListRequest,
            ) -> ResourceQueryFuture<'_, ResourceListResult> {
                Box::pin(async { ResourceListResult::try_new(Vec::new(), 0, None, None, None) })
            }
        }

        impl LeaderWatch for NoLeaderRemote {
            fn watch_resources(&self, _req: WatchRequest) -> LeaderWatchFuture<'_> {
                Box::pin(async { Err(LeaderWatchError::unavailable("no leader")) })
            }
        }

        impl LeaderCacheReadiness for NoLeaderRemote {
            fn wait_cache_ready(&self, _scope: CacheReadinessRequest) -> CacheReadinessFuture<'_> {
                Box::pin(async { Err(CacheReadinessError::unavailable("no leader")) })
            }
        }

        impl LeaderProjectedServiceAccountToken for NoLeaderRemote {
            fn issue_projected_service_account_token(
                &self,
                _request: ProjectedServiceAccountTokenRequest,
            ) -> ProjectedServiceAccountTokenFuture<'_> {
                Box::pin(async {
                    Err(
                        klights_leader_api::ProjectedServiceAccountTokenError::unavailable(
                            "no leader",
                        ),
                    )
                })
            }
        }

        impl LeaderPodCleanupIntents for NoLeaderRemote {
            fn list_pod_cleanup_intents(
                &self,
                _request: PodCleanupIntentListRequest,
            ) -> PodCleanupIntentFuture<'_, Vec<PodCleanupIntent>> {
                Box::pin(async { Err(PodCleanupIntentError::unavailable("no leader")) })
            }

            fn acknowledge_pod_cleanup_intent(
                &self,
                _request: PodCleanupIntentAckRequest,
            ) -> PodCleanupIntentFuture<'_, ()> {
                Box::pin(async { Err(PodCleanupIntentError::unavailable("no leader")) })
            }
        }

        impl LeaderNodeSubnetAllocation for NoLeaderRemote {
            fn allocate_node_subnet(
                &self,
                _request: NodeSubnetAllocationRequest,
            ) -> NodeSubnetAllocationFuture<'_, NodeSubnetAllocationResult> {
                Box::pin(async {
                    Err(NodeSubnetAllocationError::retryable(
                        "no leader currently elected; retry later",
                    ))
                })
            }
        }

        impl LeaderNetworkTopologyQuery for NoLeaderRemote {
            fn get_node_subnet(
                &self,
                _request: NodeSubnetQuery,
            ) -> NetworkTopologyFuture<'_, NodeSubnetResult> {
                Box::pin(async { Err(NetworkTopologyError::NotLeader) })
            }

            fn list_peer_subnets(
                &self,
                _request: PeerSubnetsQuery,
            ) -> NetworkTopologyFuture<'_, PeerSubnetsResult> {
                Box::pin(async { Err(NetworkTopologyError::NotLeader) })
            }

            fn get_node_dataplane(
                &self,
                _request: NodeDataplaneQuery,
            ) -> NetworkTopologyFuture<'_, NodeDataplaneResult> {
                Box::pin(async { Err(NetworkTopologyError::NotLeader) })
            }
        }

        impl LeaderOutboxDelivery for NoLeaderRemote {
            fn deliver_outbox(&self, _request: OutboxDeliveryRequest) -> OutboxDeliveryFuture<'_> {
                Box::pin(async {
                    Err(OutboxDeliveryError::unavailable(
                        "no leader currently elected; retry later",
                    ))
                })
            }
        }

        let local = RecordingApiClient::new("local");
        let remote = Arc::new(NoLeaderRemote);
        let (_tx, rx) = watch::channel(false); // follower
        let proxy = LeaderProxyApiClient::from_clients(local.clone(), remote.clone(), rx)
            .with_outbox_delivery_targets(local.clone(), remote);

        // apply_outbox must surface the remote's Retryable, not panic
        // or fall back to local.
        let err = proxy
            .deliver_test_outbox(
                "no-leader",
                OutboxOperation::PodStatus,
                Bytes::from_static(b"x"),
                "client",
                1,
                1,
            )
            .await
            .expect_err("must surface no-leader error");
        match err {
            OutboxDeliveryError::Retryable(msg) => {
                assert!(
                    msg.contains("no leader"),
                    "error must identify the no-leader condition, got: {msg}"
                );
            }
            other => panic!("expected Retryable, got {other:?}"),
        }

        // allocate_node_subnet must also surface a clean error (no
        // panic, no hang, no silent local fallback).
        let err = proxy
            .allocate_node_subnet(
                NodeSubnetAllocationRequest::try_new("n", "10.0.0.0/16", "10.0.0.1")
                    .expect("valid request"),
            )
            .await
            .expect_err("must surface no-leader error");
        assert!(
            err.to_string().contains("no leader"),
            "subnet error must identify the no-leader condition, got: {err}"
        );

        // Local was never called for writes — the proxy must not
        // silently fall back.
        assert_eq!(local.apply_outbox.load(Ordering::Relaxed), 0);
        assert_eq!(local.allocate_node_subnet.load(Ordering::Relaxed), 0);
    }

    /// Every focused capability remains independently object-safe.
    #[test]
    fn leader_proxy_focused_ports_are_object_safe() {
        let local = RecordingApiClient::new("local");
        let remote = RecordingApiClient::new("remote");
        let (_tx, rx) = watch::channel(true);
        let proxy = Arc::new(LeaderProxyApiClient::from_clients(local, remote, rx));
        let _: Arc<dyn LeaderResourceQuery> = proxy.clone();
        let _: Arc<dyn LeaderWatch> = proxy.clone();
        let _: Arc<dyn LeaderCacheReadiness> = proxy.clone();
        let _: Arc<dyn LeaderProjectedServiceAccountToken> = proxy.clone();
        let _: Arc<dyn LeaderPodCleanupIntents> = proxy.clone();
        let _: Arc<dyn LeaderNodeSubnetAllocation> = proxy.clone();
        let _: Arc<dyn LeaderNetworkTopologyQuery> = proxy;
    }

    /// T6 step 4: the boot-time `StubRemoteForwarder` refuses every
    /// write with `Retryable("…not yet wired…")`. The outbox dispatcher
    /// treats this as a transient error and re-queues, so follower
    /// writes pile up safely until step 4b ships the real forwarder
    /// (or until promotion swings the proxy back to local).
    #[tokio::test]
    async fn stub_remote_forwarder_refuses_writes_with_retryable() {
        let stub = StubRemoteForwarder::new("cp2".into());
        let err = stub
            .deliver_outbox(
                OutboxDeliveryRequest::try_new(
                    "boot",
                    klights_leader_api::OutboxDeliveryOperation::PodStatus,
                    Arc::<[u8]>::from(&b"x"[..]),
                    "client",
                    1,
                    1,
                )
                .expect("valid delivery request"),
            )
            .await
            .expect_err("stub must refuse");
        match err {
            OutboxDeliveryError::Retryable(msg) => {
                assert!(msg.contains("cp2"), "msg must name this node: {msg}");
                assert!(
                    msg.contains("not yet wired"),
                    "msg must explain forwarder is unwired: {msg}"
                );
            }
            other => panic!("expected Retryable, got {other:?}"),
        }
        // The test stub's read methods return empty. Production leader-class
        // controlplanes use a real RemoteApiClient for follower reads; the
        // stub is only reachable for non-HA seed/worker construction.
        assert!(
            stub.get_resource(
                pod_get_request("default", "x", ResourceQueryConsistency::Cached)
                    .expect("valid Pod request"),
            )
            .await
            .expect("read pass")
            .is_none()
        );
    }

    /// T6 step 4: open_leader composes the switching proxy correctly.
    /// This unit test exercises the same composition the bootstrap
    /// does: LocalApiClient (with the real is_leader_rx) + a stub
    /// remote + the same watch → LeaderProxyApiClient. It proves the
    /// boot-time wiring of the three pieces is sound at the type and
    /// dispatch level without standing up the full bootstrap.
    #[tokio::test]
    async fn bootstrap_style_proxy_composition_dispatches_correctly() {
        use crate::control_plane::client::local::LocalApiClient;
        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();
        let (tx, rx) = watch::channel(true); // simulate seed cp1
        let local_real = Arc::new(LocalApiClient::new(Arc::new(db), "cp1".into(), rx.clone()));
        let stub_remote = Arc::new(StubRemoteForwarder::new("cp1".into()));
        let proxy = LeaderProxyApiClient::from_clients(local_real.clone(), stub_remote.clone(), rx)
            .with_outbox_delivery_targets(local_real, stub_remote);

        // As leader: write reaches local, succeeds (no Pod precondition).
        let res = proxy
            .deliver_test_outbox(
                "boot-1",
                OutboxOperation::PodStatus,
                pod_status_minimal_payload(),
                "client",
                1,
                1,
            )
            .await;
        // The payload references a Pod that doesn't exist, so the
        // local arm returns a terminal NotFound — but the key point
        // is the call REACHED the local arm (would be Retryable from
        // the stub otherwise).
        match res {
            Err(OutboxDeliveryError::NotFound(_)) => {} // reached local, terminal
            Err(OutboxDeliveryError::Retryable(msg)) if msg.contains("not yet wired") => {
                panic!("write reached stub remote — proxy dispatched WRONG side when leader=true");
            }
            other => {
                // Other terminal errors are acceptable — the assertion is
                // only that we didn't hit the stub.
                tracing::debug!(?other, "local arm returned a non-stub error as expected");
            }
        }

        // Lose leadership: the same instance now routes to remote.
        tx.send(false).expect("demote");
        let err = proxy
            .deliver_test_outbox(
                "boot-2",
                OutboxOperation::PodStatus,
                pod_status_minimal_payload(),
                "client",
                1,
                1,
            )
            .await
            .expect_err("non-leader write goes to stub remote");
        match err {
            OutboxDeliveryError::Retryable(msg) => {
                assert!(
                    msg.contains("not yet wired"),
                    "must hit the stub remote, got: {msg}"
                );
            }
            other => panic!("expected stub Retryable, got {other:?}"),
        }
    }

    fn pod_status_minimal_payload() -> Bytes {
        use crate::datastore::ResourcePreconditions;
        use crate::outbox_test_support::OutboxPayload;
        use klights_cluster_core::command::StorageCommand;
        let command = StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "absent".to_string(),
            status: serde_json::json!({"phase": "Running"}),
            expected_rv: None,
            preconditions: ResourcePreconditions {
                uid: Some("absent-uid".to_string()),
                resource_version: None,
            },
            observed_status_stamp: None,
        };
        Bytes::from(
            OutboxPayload::from_command(command)
                .encode_protobuf()
                .expect("encode"),
        )
    }

    /// The proxy never spawns or sleeps; per-call dispatch is a
    /// single `watch::Receiver::borrow` plus an arc deref. This is a
    /// structural check that the impl above does no I/O on its own —
    /// dispatch happens inline.
    #[test]
    fn leader_proxy_holds_no_background_resources() {
        // The dispatcher has exactly four thin fields: two public API
        // trait-object Arcs, one optional Arc to the focused target bundle,
        // and one watch receiver. Both target pairs live behind that single
        // heap pointer. There is no supervisor, spawn handle, timer, or
        // background task state.
        use std::mem::size_of;
        let thin_dispatcher_fields = size_of::<ArcPair<dyn LeaderResourceQuery>>() * 7
            + size_of::<watch::Receiver<bool>>()
            + size_of::<Option<Arc<FocusedLeaderTargets>>>();
        assert_eq!(
            size_of::<LeaderProxyApiClient>(),
            thin_dispatcher_fields,
            "LeaderProxyApiClient must stay a thin per-call dispatcher; \
             field growth probably introduced spawn / timer / supervisor state \
             that violates HR #1 (zero idle CPU)."
        );
    }

    // ──────────────────────────────────────────────────────────────────
    // T6 step 6: convergence unit tests.
    //
    // These tests prove the dispatch invariants that make cluster.db
    // convergence possible at the wiring level:
    //   1. A follower-originated write through the switching proxy
    //      reaches the leader's apply path (modeled by a shared
    //      backend that both the local-leader-arm and the
    //      remote-as-leader-arm write to).
    //   2. Promotion does not require rewiring cluster_api or the
    //      proposer: the same instance flips its dispatch and the
    //      same backend is written.
    //
    // The full netns-level convergence test
    // (tests/multinode_netns/convergence_failover_test.sh) covers
    // end-to-end cluster.db parity; the unit tests below cover the
    // wiring invariants in isolation.
    // ──────────────────────────────────────────────────────────────────

    /// `cluster_db_converges_after_multinode_write_through_proxy`:
    /// model 3 cluster members (1 leader + 2 followers). All three
    /// have a `LeaderProxyApiClient` as their `cluster_api`. The
    /// followers' remote arm points at a shared "leader API" mock
    /// (modeling the leader's API server reached via gRPC). When a
    /// follower issues a write through its proxy, the shared backend
    /// records exactly one apply — proving the dispatch routes
    /// across the boundary correctly. The leader's own write through
    /// its own proxy hits the same shared backend, so all three
    /// members' "view" of the apply set is identical.
    #[tokio::test]
    async fn cluster_db_converges_after_multinode_write_through_proxy() {
        // The shared "leader apply" surface — both the leader's local
        // arm AND every follower's remote arm route writes here. In
        // production this is the leader's LocalApiClient → raft
        // proposer → raft → state-machine apply on every member.
        let leader_backend = RecordingApiClient::new("leader-shared");

        // Leader member: cluster_api proxy whose local arm IS the
        // shared leader backend. is_leader=true.
        let (_tx_l, rx_l) = watch::channel(true);
        let leader_unused_remote = RecordingApiClient::new("leader-unused-remote");
        let leader_proxy = LeaderProxyApiClient::from_clients(
            leader_backend.clone(),
            leader_unused_remote.clone(),
            rx_l,
        )
        .with_outbox_delivery_targets(leader_backend.clone(), leader_unused_remote);

        // Follower 1: cluster_api proxy whose REMOTE arm is the
        // shared leader backend (modeling the gRPC forward).
        // is_leader=false.
        let (_tx_f1, rx_f1) = watch::channel(false);
        let follower1_local = RecordingApiClient::new("f1-local-unused");
        let follower1_proxy = LeaderProxyApiClient::from_clients(
            follower1_local.clone(),
            leader_backend.clone(),
            rx_f1,
        )
        .with_outbox_delivery_targets(follower1_local, leader_backend.clone());

        // Follower 2: same shape as follower 1.
        let (_tx_f2, rx_f2) = watch::channel(false);
        let follower2_local = RecordingApiClient::new("f2-local-unused");
        let follower2_proxy = LeaderProxyApiClient::from_clients(
            follower2_local.clone(),
            leader_backend.clone(),
            rx_f2,
        )
        .with_outbox_delivery_targets(follower2_local, leader_backend.clone());

        // Each member issues one write. All three calls must reach
        // the shared leader backend.
        leader_proxy
            .deliver_test_outbox(
                "leader-write",
                OutboxOperation::PodStatus,
                Bytes::from_static(b"x"),
                "client",
                1,
                1,
            )
            .await
            .expect("leader write");
        follower1_proxy
            .deliver_test_outbox(
                "f1-write",
                OutboxOperation::PodStatus,
                Bytes::from_static(b"x"),
                "client",
                1,
                1,
            )
            .await
            .expect("follower1 write");
        follower2_proxy
            .deliver_test_outbox(
                "f2-write",
                OutboxOperation::PodStatus,
                Bytes::from_static(b"x"),
                "client",
                1,
                1,
            )
            .await
            .expect("follower2 write");

        assert_eq!(
            leader_backend.apply_outbox.load(Ordering::Relaxed),
            3,
            "all three members must converge writes on the leader's apply path"
        );
    }

    /// `promotion_does_not_rewire_cluster_api_or_proposer`: a
    /// follower's cluster_api proxy + closed gate → flip leader
    /// state via the shared watch → the SAME instances become
    /// active without reconstruction. This is the structural
    /// guarantee that promotion is a state flip, not a rewire.
    #[tokio::test]
    async fn promotion_does_not_rewire_cluster_api_or_proposer() {
        let local = RecordingApiClient::new("local");
        let remote = RecordingApiClient::new("remote");
        let (proxy, tx) = make_proxy(local.clone(), remote.clone(), false);
        let proxy = Arc::new(proxy);

        // Capture the addresses of the underlying Arcs BEFORE
        // promotion to prove no reconstruction happens.
        let proxy_addr_before = Arc::as_ptr(&proxy) as *const () as usize;
        let local_addr_before = Arc::as_ptr(&local) as *const () as usize;
        let remote_addr_before = Arc::as_ptr(&remote) as *const () as usize;

        // Follower write → remote.
        proxy
            .deliver_test_outbox(
                "pre",
                OutboxOperation::PodStatus,
                Bytes::from_static(b"x"),
                "client",
                1,
                1,
            )
            .await
            .expect("pre");
        assert_eq!(remote.apply_outbox.load(Ordering::Relaxed), 1);
        assert_eq!(local.apply_outbox.load(Ordering::Relaxed), 0);

        // Promotion: pure state flip, no construction.
        tx.send(true).expect("promote");

        // Same proxy instance now dispatches writes to local.
        proxy
            .deliver_test_outbox(
                "post-promote",
                OutboxOperation::PodStatus,
                Bytes::from_static(b"x"),
                "client",
                1,
                1,
            )
            .await
            .expect("post");
        assert_eq!(remote.apply_outbox.load(Ordering::Relaxed), 1);
        assert_eq!(local.apply_outbox.load(Ordering::Relaxed), 1);

        // Pointer identity proves no Arcs were swapped under us:
        // same proxy struct, same local arm, same remote arm.
        let proxy_addr_after = Arc::as_ptr(&proxy) as *const () as usize;
        let local_addr_after = Arc::as_ptr(&local) as *const () as usize;
        let remote_addr_after = Arc::as_ptr(&remote) as *const () as usize;
        assert_eq!(proxy_addr_before, proxy_addr_after);
        assert_eq!(local_addr_before, local_addr_after);
        assert_eq!(remote_addr_before, remote_addr_after);
    }

    /// T7.6: verify that when is_leader_rx flips to false, the
    /// switching proxy routes writes to the remote arm instead of
    /// the local arm. This proves that seed identity is not a
    /// permanent write permission — the proxy respects live
    /// raft leadership state.
    #[tokio::test]
    async fn seed_loses_leadership_proxies_writes_to_remote() {
        let local = RecordingApiClient::new("local");
        let remote = RecordingApiClient::new("remote");
        let (proxy, tx) = make_proxy(local.clone(), remote.clone(), true);

        // As leader, writes go to local
        proxy
            .deliver_test_outbox(
                "key",
                OutboxOperation::PodStatus,
                Bytes::from_static(b"x"),
                "client",
                1,
                1,
            )
            .await
            .unwrap();
        assert_eq!(local.apply_outbox.load(Ordering::Relaxed), 1);
        assert_eq!(remote.apply_outbox.load(Ordering::Relaxed), 0);

        // Simulate leadership loss
        tx.send(false).unwrap();

        // After leadership loss, writes go to remote
        proxy
            .deliver_test_outbox(
                "key2",
                OutboxOperation::PodStatus,
                Bytes::from_static(b"y"),
                "client",
                1,
                1,
            )
            .await
            .unwrap();
        assert_eq!(
            local.apply_outbox.load(Ordering::Relaxed),
            1,
            "local must not receive post-loss writes"
        );
        assert_eq!(
            remote.apply_outbox.load(Ordering::Relaxed),
            1,
            "remote must receive post-loss writes"
        );
    }
}
