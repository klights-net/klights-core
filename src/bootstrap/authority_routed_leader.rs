//! Private bootstrap authority-routed leader capability dispatcher.
//!
//! Every leader-class member (cp + replica) holds the same `cluster_api`
//! binding — an `AuthorityRoutedLeader` that wraps a local
//! a local capability set and a remote forwarder. Per-call dispatch:
//!
//! - **Kubernetes API reads and watches** go to the elected leader. When this
//!   member is leader they use the local client; otherwise they use the remote
//!   forwarder. Followers do not serve application reads from their local
//!   raft-applied `cluster.db`.
//! - **Pod cleanup intent reads/deletes** prefer the remote leader path.
//!   Startup recovery may run before a rejoining old leader has observed
//!   its demotion, so the local leadership watch can briefly be stale.
//! - **Writes** consult the backend-neutral authority route on entry: local
//!   authority dispatches to the local client (which routes through the local
//!   datastore → raft proposer → raft → state-machine apply); forwarded or
//!   temporarily unavailable authority dispatches to the remote arm, which
//!   carries the call to the current elected leader's API server over gRPC.
//!
//! Leadership change is a state flip on the same instance — no
//! re-construction, no rewiring. The adapter samples authority per
//! call, so the next read or write after promotion / demotion picks the new
//! dispatch target without any setup.
//!
//! Every capability is stored as its own focused trait object. Tests inject
//! recording fakes through the individual focused capabilities and assert the dispatch table
//! without spinning up a real cluster.

use std::sync::Arc;

#[cfg(test)]
use bytes::Bytes;
use futures::StreamExt as _;
#[cfg(test)]
use klights_kubelet::node_outbox::payload::OutboxOperationExt as _;
use klights_leader_api::{
    AuthorityRoute, CacheReadinessError, CacheReadinessFuture, CacheReadinessRequest,
    LeaderCacheReadiness, LeaderNetworkTopologyQuery, LeaderNodeLeaseRenewal,
    LeaderNodeSubnetAllocation, LeaderOutboxDelivery, LeaderPodCleanupIntents,
    LeaderProjectedServiceAccountToken, LeaderResourceCommand, LeaderResourceQuery, LeaderWatch,
    LeaderWatchError, LeaderWatchFuture, NetworkTopologyError, NetworkTopologyFuture,
    NodeDataplaneQuery, NodeDataplaneResult, NodeLeaseRenewalError, NodeLeaseRenewalFuture,
    NodeLeaseRenewalRequest, NodeLeaseRenewalResult, NodeSubnetAllocationError,
    NodeSubnetAllocationFuture, NodeSubnetAllocationRequest, NodeSubnetAllocationResult,
    NodeSubnetQuery, NodeSubnetResult, OutboxDeliveryError, OutboxDeliveryFuture,
    OutboxDeliveryRequest, PeerSubnetsQuery, PeerSubnetsResult, PodCleanupIntent,
    PodCleanupIntentAckRequest, PodCleanupIntentError, PodCleanupIntentFuture,
    PodCleanupIntentListRequest, ProjectedServiceAccountTokenFuture,
    ProjectedServiceAccountTokenRequest, ResourceCommandError, ResourceCommandFuture,
    ResourceCommandRequest, ResourceCommandResult, ResourceGetRequest, ResourceListRequest,
    ResourceListResult, ResourceQueryConsistency, ResourceQueryFuture, WatchRequest, WatchStream,
};

use crate::bootstrap::authority::AuthorityHandle;
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

    fn target(&self, route: &AuthorityRoute) -> Option<&Arc<T>> {
        match route {
            AuthorityRoute::Local(_) => self.local.as_ref(),
            AuthorityRoute::Forward { .. } | AuthorityRoute::Unavailable => self.remote.as_ref(),
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
/// the local capability set (reads, plus writes when self is the elected
/// leader) or a remote forwarder (writes when self is a follower /
/// learner). The decision is per-call; promotion / demotion changes the
/// authority route and the next write picks the new target without rewiring.
pub(crate) struct AuthorityRoutedLeader {
    resource_queries: ArcPair<dyn LeaderResourceQuery>,
    watches: ArcPair<dyn LeaderWatch>,
    cache_readiness: ArcPair<dyn LeaderCacheReadiness>,
    projected_tokens: ArcPair<dyn LeaderProjectedServiceAccountToken>,
    pod_cleanup_intents: ArcPair<dyn LeaderPodCleanupIntents>,
    node_subnet_allocations: ArcPair<dyn LeaderNodeSubnetAllocation>,
    network_topology: ArcPair<dyn LeaderNetworkTopologyQuery>,
    focused_targets: Option<Arc<FocusedLeaderTargets>>,
    authority: AuthorityHandle,
}

impl AuthorityRoutedLeader {
    /// Construct a switching proxy.
    ///
    /// `local` handles all reads and writes-while-leader. `remote`
    /// handles writes-while-follower (forwarding to the current
    /// elected leader). The authority handle is the same backend-neutral
    /// capability fed by bootstrap, so the two layers cannot disagree.
    // Keep every capability explicit: bundling these arguments would recreate
    // the deleted god client and weaken per-capability route ownership.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        local_resource_query: Arc<dyn LeaderResourceQuery>,
        local_watch: Arc<dyn LeaderWatch>,
        local_cache_readiness: Arc<dyn LeaderCacheReadiness>,
        local_projected_tokens: Arc<dyn LeaderProjectedServiceAccountToken>,
        local_pod_cleanup_intents: Arc<dyn LeaderPodCleanupIntents>,
        local_node_subnet_allocation: Arc<dyn LeaderNodeSubnetAllocation>,
        local_network_topology: Arc<dyn LeaderNetworkTopologyQuery>,
        remote_resource_query: Arc<dyn LeaderResourceQuery>,
        remote_watch: Arc<dyn LeaderWatch>,
        remote_cache_readiness: Arc<dyn LeaderCacheReadiness>,
        remote_projected_tokens: Arc<dyn LeaderProjectedServiceAccountToken>,
        remote_pod_cleanup_intents: Arc<dyn LeaderPodCleanupIntents>,
        remote_node_subnet_allocation: Arc<dyn LeaderNodeSubnetAllocation>,
        remote_network_topology: Arc<dyn LeaderNetworkTopologyQuery>,
        authority: impl Into<AuthorityHandle>,
    ) -> Self {
        Self {
            resource_queries: ArcPair {
                local: Some(local_resource_query),
                remote: Some(remote_resource_query),
            },
            watches: ArcPair {
                local: Some(local_watch),
                remote: Some(remote_watch),
            },
            cache_readiness: ArcPair {
                local: Some(local_cache_readiness),
                remote: Some(remote_cache_readiness),
            },
            projected_tokens: ArcPair {
                local: Some(local_projected_tokens),
                remote: Some(remote_projected_tokens),
            },
            pod_cleanup_intents: ArcPair {
                local: Some(local_pod_cleanup_intents),
                remote: Some(remote_pod_cleanup_intents),
            },
            node_subnet_allocations: ArcPair {
                local: Some(local_node_subnet_allocation),
                remote: Some(remote_node_subnet_allocation),
            },
            network_topology: ArcPair {
                local: Some(local_network_topology),
                remote: Some(remote_network_topology),
            },
            focused_targets: None,
            authority: authority.into(),
        }
    }

    pub(crate) fn with_resource_command_targets(
        mut self,
        local: Arc<dyn LeaderResourceCommand>,
        remote: Arc<dyn LeaderResourceCommand>,
    ) -> Self {
        self.focused_targets_mut()
            .resource_commands
            .set(local, remote);
        self
    }

    pub(crate) fn with_outbox_delivery_targets(
        mut self,
        local: Arc<dyn LeaderOutboxDelivery>,
        remote: Arc<dyn LeaderOutboxDelivery>,
    ) -> Self {
        self.focused_targets_mut()
            .outbox_deliveries
            .set(local, remote);
        self
    }

    pub(crate) fn with_node_lease_renewal_targets(
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

    fn target<'a, T: ?Sized>(&self, pair: &'a ArcPair<T>, route: &AuthorityRoute) -> &'a Arc<T> {
        pair.target(route)
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

impl LeaderResourceCommand for AuthorityRoutedLeader {
    fn submit_resource_command(
        &self,
        request: ResourceCommandRequest,
    ) -> ResourceCommandFuture<'_, ResourceCommandResult> {
        let authority = self.authority.clone();
        let route = authority.route();
        let permit = match &route {
            AuthorityRoute::Local(permit) => Some(permit.clone()),
            AuthorityRoute::Forward { .. } | AuthorityRoute::Unavailable => None,
        };
        let target = self
            .focused_targets
            .as_ref()
            .and_then(|targets| targets.resource_commands.target(&route).cloned());
        Box::pin(async move {
            if let Some(permit) = permit.as_ref()
                && authority.validate(permit).is_err()
            {
                return Err(ResourceCommandError::retryable(
                    "leader authority changed before resource-command dispatch",
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

impl LeaderNodeLeaseRenewal for AuthorityRoutedLeader {
    fn renew_node_lease(
        &self,
        request: NodeLeaseRenewalRequest,
    ) -> NodeLeaseRenewalFuture<'_, NodeLeaseRenewalResult> {
        let route = self.authority.route();
        let target = self
            .focused_targets
            .as_ref()
            .and_then(|targets| targets.node_lease_renewals.target(&route));
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

fn terminate_watch_on_authority_change(
    stream: WatchStream,
    authority: AuthorityHandle,
    permit: Option<klights_leader_api::AuthorityPermit>,
) -> WatchStream {
    stream.map_inner(|stream| {
        Box::pin(futures::stream::unfold(
            (stream, authority, permit),
            move |(mut stream, authority, permit)| async move {
                let authority_change = wait_for_authority_change(authority.clone(), permit.clone());
                tokio::select! {
                    biased;
                    _ = authority_change => None,
                    item = stream.next() => {
                        item.map(|item| (item, (stream, authority, permit)))
                    }
                }
            },
        ))
    })
}

async fn wait_for_authority_change(
    authority: AuthorityHandle,
    permit: Option<klights_leader_api::AuthorityPermit>,
) {
    match permit {
        Some(permit) => authority.wait_for_revocation(&permit).await,
        None => {
            let _ = authority.acquire().await;
        }
    }
}

impl LeaderResourceQuery for AuthorityRoutedLeader {
    fn get_resource(
        &self,
        request: ResourceGetRequest,
    ) -> ResourceQueryFuture<'_, Option<Resource>> {
        let authority = self.authority.clone();
        let route = authority.route();
        let permit = match &route {
            AuthorityRoute::Local(permit) => Some(permit.clone()),
            AuthorityRoute::Forward { .. } | AuthorityRoute::Unavailable => None,
        };
        let target = self
            .resource_queries
            .target(&route)
            .expect("resource-query targets are wired")
            .clone();
        Box::pin(async move {
            let consistency = request.consistency();
            let result = target.get_resource(request).await?;
            if consistency == ResourceQueryConsistency::LeaderFresh
                && let Some(permit) = permit.as_ref()
                && authority.validate(permit).is_err()
            {
                return Err(klights_leader_api::ResourceQueryError::retryable(
                    "leader authority changed during leader-fresh resource query",
                ));
            }
            Ok(result)
        })
    }

    fn list_resources(
        &self,
        request: ResourceListRequest,
    ) -> ResourceQueryFuture<'_, ResourceListResult> {
        let authority = self.authority.clone();
        let route = authority.route();
        let permit = match &route {
            AuthorityRoute::Local(permit) => Some(permit.clone()),
            AuthorityRoute::Forward { .. } | AuthorityRoute::Unavailable => None,
        };
        let target = self
            .resource_queries
            .target(&route)
            .expect("resource-query targets are wired")
            .clone();
        Box::pin(async move {
            let consistency = request.consistency();
            let result = target.list_resources(request).await?;
            if consistency == ResourceQueryConsistency::LeaderFresh
                && let Some(permit) = permit.as_ref()
                && authority.validate(permit).is_err()
            {
                return Err(klights_leader_api::ResourceQueryError::retryable(
                    "leader authority changed during leader-fresh resource query",
                ));
            }
            Ok(result)
        })
    }
}

impl LeaderWatch for AuthorityRoutedLeader {
    fn watch_resources(&self, req: WatchRequest) -> LeaderWatchFuture<'_> {
        let authority = self.authority.clone();
        let route = authority.route();
        let permit = match &route {
            AuthorityRoute::Local(permit) => Some(permit.clone()),
            AuthorityRoute::Forward { .. } | AuthorityRoute::Unavailable => None,
        };
        let target = self
            .watches
            .target(&route)
            .expect("watch targets are wired")
            .clone();
        Box::pin(async move {
            let stream = LeaderWatch::watch_resources(target.as_ref(), req).await?;
            if let Some(permit) = permit.as_ref()
                && authority.validate(permit).is_err()
            {
                return Err(LeaderWatchError::unavailable(
                    "leader authority changed while establishing watch",
                ));
            }
            Ok(terminate_watch_on_authority_change(
                stream, authority, permit,
            ))
        })
    }
}

impl LeaderCacheReadiness for AuthorityRoutedLeader {
    fn wait_cache_ready(&self, scope: CacheReadinessRequest) -> CacheReadinessFuture<'_> {
        let route = self.authority.route();
        self.target(&self.cache_readiness, &route)
            .wait_cache_ready(scope)
    }
}

impl LeaderProjectedServiceAccountToken for AuthorityRoutedLeader {
    fn issue_projected_service_account_token(
        &self,
        request: ProjectedServiceAccountTokenRequest,
    ) -> ProjectedServiceAccountTokenFuture<'_> {
        let route = self.authority.route();
        self.target(&self.projected_tokens, &route)
            .issue_projected_service_account_token(request)
    }
}

impl LeaderPodCleanupIntents for AuthorityRoutedLeader {
    fn list_pod_cleanup_intents(
        &self,
        request: PodCleanupIntentListRequest,
    ) -> PodCleanupIntentFuture<'_, Vec<PodCleanupIntent>> {
        Box::pin(async move {
            let route = self.authority.route();
            let remote = self
                .pod_cleanup_intents
                .remote
                .as_ref()
                .expect("remote cleanup-intent target is wired");
            match remote.list_pod_cleanup_intents(request.clone()).await {
                Ok(intents) => Ok(intents),
                Err(error) => match route {
                    AuthorityRoute::Local(_) => {
                        self.pod_cleanup_intents
                            .local
                            .as_ref()
                            .expect("local cleanup-intent target is wired")
                            .list_pod_cleanup_intents(request)
                            .await
                    }
                    AuthorityRoute::Forward { .. } | AuthorityRoute::Unavailable => Err(error),
                },
            }
        })
    }

    fn acknowledge_pod_cleanup_intent(
        &self,
        request: PodCleanupIntentAckRequest,
    ) -> PodCleanupIntentFuture<'_, ()> {
        Box::pin(async move {
            let route = self.authority.route();
            let remote = self
                .pod_cleanup_intents
                .remote
                .as_ref()
                .expect("remote cleanup-intent target is wired");
            match remote.acknowledge_pod_cleanup_intent(request.clone()).await {
                Ok(()) => Ok(()),
                Err(error) => match route {
                    AuthorityRoute::Local(_) => {
                        self.pod_cleanup_intents
                            .local
                            .as_ref()
                            .expect("local cleanup-intent target is wired")
                            .acknowledge_pod_cleanup_intent(request)
                            .await
                    }
                    AuthorityRoute::Forward { .. } | AuthorityRoute::Unavailable => Err(error),
                },
            }
        })
    }
}

impl LeaderNodeSubnetAllocation for AuthorityRoutedLeader {
    fn allocate_node_subnet(
        &self,
        request: NodeSubnetAllocationRequest,
    ) -> NodeSubnetAllocationFuture<'_, NodeSubnetAllocationResult> {
        let route = self.authority.route();
        self.target(&self.node_subnet_allocations, &route)
            .allocate_node_subnet(request)
    }
}

impl LeaderNetworkTopologyQuery for AuthorityRoutedLeader {
    fn get_node_subnet(
        &self,
        request: NodeSubnetQuery,
    ) -> NetworkTopologyFuture<'_, NodeSubnetResult> {
        let route = self.authority.route();
        self.target(&self.network_topology, &route)
            .get_node_subnet(request)
    }

    fn list_peer_subnets(
        &self,
        request: PeerSubnetsQuery,
    ) -> NetworkTopologyFuture<'_, PeerSubnetsResult> {
        let route = self.authority.route();
        self.target(&self.network_topology, &route)
            .list_peer_subnets(request)
    }

    fn get_node_dataplane(
        &self,
        request: NodeDataplaneQuery,
    ) -> NetworkTopologyFuture<'_, NodeDataplaneResult> {
        let route = self.authority.route();
        self.target(&self.network_topology, &route)
            .get_node_dataplane(request)
    }
}

impl LeaderOutboxDelivery for AuthorityRoutedLeader {
    fn deliver_outbox(&self, request: OutboxDeliveryRequest) -> OutboxDeliveryFuture<'_> {
        let route = self.authority.route();
        let target = self
            .focused_targets
            .as_ref()
            .and_then(|targets| targets.outbox_deliveries.target(&route));
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
pub(crate) struct StubRemoteForwarder {
    node_name: String,
}

impl StubRemoteForwarder {
    pub(crate) fn new(node_name: String) -> Self {
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
#[path = "tests/authority_routed_leader.rs"]
mod tests;
