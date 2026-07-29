use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use futures::StreamExt as _;
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
    PodCleanupIntentListRequest, ProjectedServiceAccountTokenError,
    ProjectedServiceAccountTokenFuture, ProjectedServiceAccountTokenRequest, ResourceCommandError,
    ResourceCommandFuture, ResourceCommandRequest, ResourceCommandResult, ResourceEvent,
    ResourceGetRequest, ResourceListRequest, ResourceListResult, ResourceQueryConsistency,
    ResourceQueryFuture, WatchRequest, WatchResumeCursor, WatchStream,
};
use tokio_util::sync::CancellationToken;

#[cfg(test)]
use super::Pod;
use super::informer::{RemoteInformerCache, list as list_cached, replace_scope, scope_for_request};
use super::{
    ListRequest, ResourceList, focused_watch_event, legacy_list_request, legacy_list_response,
    legacy_watch_event, query_error, query_list_result,
};
use crate::replication::grpc::client::ReplicationGrpcClient;
use klights_cluster_core::Resource;
#[cfg(test)]
use klights_cluster_core::WatchReplayPosition;
use klights_supervisor::{SupervisedJoinHandle, TaskCategory, TaskSupervisor};

/// bug-grpc: a worker watch stream that delivers neither an event nor a
/// heartbeat BOOKMARK within this window is treated as wedged and dropped, so
/// the driver reconnects from `next_resource_version` (catch-up replay
/// re-delivers anything missed). Sized at ~3× the leader heartbeat
/// (`server::WATCH_HEARTBEAT_INTERVAL`, 20 s) so a healthy-but-quiet stream
/// never trips, while a partial-loss wedge (keepalive PING slips through but
/// watch DATA does not) is caught within a minute instead of stalling
/// indefinitely (the 10-minute "stable cluster" pod-deletion stall).
const WATCH_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Outcome of a single idle-bounded poll of a watch stream.
enum IdleNext {
    /// The stream produced an item (event or decode error).
    Item(std::result::Result<ResourceEvent, LeaderWatchError>),
    /// The stream ended (None) or the supervisor declined the timer.
    Closed,
    /// No item arrived within the idle window — the stream is wedged.
    Idle,
}

/// Poll `stream` for the next item, bounded by `idle`. Returns [`IdleNext::Idle`]
/// when the window elapses with no item. Without a supervisor (unit tests for
/// cache paths) it falls back to an unbounded poll.
async fn next_event_within_idle(
    supervisor: Option<&Arc<TaskSupervisor>>,
    idle: std::time::Duration,
    stream: &mut WatchStream,
) -> IdleNext {
    let Some(supervisor) = supervisor else {
        return match stream.next().await {
            Some(item) => IdleNext::Item(item),
            None => IdleNext::Closed,
        };
    };
    match supervisor
        .timeout("remote_watch_idle", idle, stream.next())
        .await
    {
        Ok(Ok(Some(item))) => IdleNext::Item(item),
        Ok(Ok(None)) => IdleNext::Closed,
        Ok(Err(_elapsed)) => IdleNext::Idle,
        Err(_shutdown) => IdleNext::Closed,
    }
}

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
    #[cfg(test)]
    pub fn new(node_name: String) -> Self {
        Self {
            node_name,
            grpc: None,
            supervisor: None,
            cache: Arc::new(crate::remote_informer_cache_adapter::WatchCacheAdapter::new()),
            worker_informers_started: Arc::new(AtomicBool::new(false)),
            watch_idle_timeout: WATCH_IDLE_TIMEOUT,
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
            watch_idle_timeout: WATCH_IDLE_TIMEOUT,
        }
    }

    #[cfg(test)]
    pub fn new_for_tests(node_name: &str) -> Self {
        Self::new(node_name.to_string())
    }

    /// In tests, directly insert a pod into the informer cache without going
    /// through gRPC. This lets us test cache-hit read paths independently.
    #[cfg(test)]
    pub async fn cache_insert_pod(&self, pod: Pod) {
        self.cache.insert(pod).await;
    }

    /// Mark a cache scope as primed.
    #[cfg(test)]
    pub async fn cache_prime_scope(&self, scope: CacheReadinessRequest) {
        let request = ListRequest {
            api_version: scope.api_version().to_string(),
            kind: scope.kind().to_string(),
            namespace: scope.namespace().map(str::to_owned),
            label_selector: scope.label_selector().map(str::to_owned),
            field_selector: scope.field_selector().map(str::to_owned),
            limit: None,
            continue_token: None,
        };
        replace_scope(
            self.cache.as_ref(),
            &request,
            ResourceList {
                items: Vec::new(),
                resource_version: 0,
                watch_replay_position: Some(WatchReplayPosition::from_resource_version(0)),
                continue_token: None,
                remaining_item_count: None,
            },
        )
        .await
        .expect("test cache scope baseline must be valid");
        self.cache
            .mark_ready(scope)
            .await
            .expect("test cache scope must have a baseline before readiness");
    }

    /// Clear a cache scope (simulates watch 410 Gone).
    #[cfg(test)]
    pub async fn cache_clear_scope_for_test(&self, scope: &CacheReadinessRequest) {
        self.cache.clear_ready(scope).await;
    }

    pub async fn start_required_worker_informers(
        self: &Arc<Self>,
        cancel: CancellationToken,
    ) -> Result<Vec<SupervisedJoinHandle<()>>> {
        let supervisor = self
            .supervisor
            .as_ref()
            .ok_or_else(|| anyhow!("RemoteApiClient missing TaskSupervisor"))?
            .clone();
        if self
            .worker_informers_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(Vec::new());
        }
        let mut handles = Vec::new();
        for req in self.required_worker_list_requests() {
            let client = self.clone();
            let cancel = cancel.clone();
            match supervisor
                .spawn_async(
                    TaskCategory::Network,
                    "remote_api_informer_watch",
                    async move {
                        client.run_watch_driver(req, cancel).await;
                    },
                )
                .await
            {
                Ok(handle) => handles.push(handle),
                Err(err) => {
                    self.worker_informers_started
                        .store(false, Ordering::Release);
                    return Err(err.into());
                }
            }
        }
        Ok(handles)
    }

    async fn run_watch_driver(self: Arc<Self>, req: ListRequest, cancel: CancellationToken) {
        let mut next_resource_version = None;
        let mut next_watch_replay_position = None;
        // Consecutive failed reconnects; reset to 0 once the stream delivers an
        // event. Drives the shared exponential reconnect backoff so a sustained
        // leader/WAN outage cannot become a fixed-interval reconnect storm.
        let mut reconnect_attempt: u32 = 0;
        loop {
            if cancel.is_cancelled() {
                return;
            }
            if next_resource_version.is_none() {
                match self.prime_list_scope(req.clone()).await {
                    Ok(list) => {
                        next_resource_version = Some(list.resource_version);
                        next_watch_replay_position = list.watch_replay_position;
                    }
                    Err(err) => {
                        tracing::warn!(
                            api_version = %req.api_version,
                            kind = %req.kind,
                            error = %err,
                            "failed to prime remote informer scope"
                        );
                        self.sleep_before_reconnect(reconnect_attempt).await;
                        reconnect_attempt = reconnect_attempt.saturating_add(1);
                        continue;
                    }
                }
            }
            let watch_req = WatchRequest::try_new(
                req.api_version.clone(),
                req.kind.clone(),
                req.namespace.clone(),
                req.label_selector.clone(),
                req.field_selector.clone(),
                next_resource_version,
                next_watch_replay_position,
            )
            .expect("worker informer LIST identity and cursor are validated");
            match self.watch_resources(watch_req).await {
                Ok(mut stream) => loop {
                    let next = tokio::select! {
                        _ = cancel.cancelled() => return,
                        next = next_event_within_idle(
                            self.supervisor.as_ref(),
                            self.watch_idle_timeout,
                            &mut stream,
                        ) => next,
                    };
                    match next {
                        IdleNext::Item(Ok(event)) => {
                            // bug-grpc B2/B3: cursor-advance-only-after-safe-apply.
                            // The resume RV must advance ONLY once the event is
                            // decoded and applied (a BOOKMARK applies as a no-op
                            // success, so its RV is a valid resume point). If
                            // apply fails, leave next_resource_version pointing
                            // before this event and reconnect, so catch-up replay
                            // re-delivers it — never advance past an event that
                            // was not applied (silent loss on reconnect).
                            let mut applied_cursor = WatchResumeCursor::try_new(
                                next_resource_version,
                                next_watch_replay_position,
                            )
                            .expect("informer cursor remains valid");
                            if let Err(err) = applied_cursor.advance_after_apply(&event) {
                                tracing::warn!(error = %err, "remote informer cursor rejected event before apply");
                                break;
                            }
                            let legacy_event = legacy_watch_event(&event);
                            let event = match focused_watch_event(
                                legacy_event,
                                event.resume_position(),
                            ) {
                                Ok(event) => event,
                                Err(err) => {
                                    tracing::warn!(error = %err, "remote informer rejected selector transition");
                                    break;
                                }
                            };
                            self.cache.apply_event(&event).await;
                            next_resource_version = applied_cursor.resource_version();
                            next_watch_replay_position = applied_cursor.replay_position();
                            reconnect_attempt = 0;
                        }
                        IdleNext::Item(Err(err)) => {
                            if watch_error_requires_relist(&err) {
                                next_resource_version = None;
                                next_watch_replay_position = None;
                            }
                            tracing::warn!(
                                api_version = %req.api_version,
                                kind = %req.kind,
                                error = %err,
                                "remote informer watch stream failed"
                            );
                            break;
                        }
                        IdleNext::Idle => {
                            // No event or heartbeat within the idle window: the
                            // stream is wedged (loss let keepalive through but
                            // not watch data). Drop it and reconnect from the
                            // last resourceVersion; catch-up replay re-delivers
                            // anything missed, so no event is lost.
                            tracing::warn!(
                                api_version = %req.api_version,
                                kind = %req.kind,
                                "remote informer watch idle past heartbeat window; reconnecting from last resourceVersion"
                            );
                            break;
                        }
                        IdleNext::Closed => break,
                    }
                },
                Err(err) => {
                    if watch_error_requires_relist(&err) {
                        next_resource_version = None;
                        next_watch_replay_position = None;
                    }
                    tracing::warn!(
                        api_version = %req.api_version,
                        kind = %req.kind,
                        error = %err,
                        "failed to open remote informer watch stream"
                    );
                }
            }
            self.sleep_before_reconnect(reconnect_attempt).await;
            reconnect_attempt = reconnect_attempt.saturating_add(1);
        }
    }

    async fn sleep_before_reconnect(&self, attempt: u32) {
        if let Some(supervisor) = &self.supervisor {
            let _ = supervisor
                .sleep(
                    "remote_api_informer_reconnect",
                    klights_supervisor::reconnect_backoff::delay(attempt),
                )
                .await;
        }
    }

    async fn prime_list_scope(
        &self,
        req: ListRequest,
    ) -> std::result::Result<ResourceList, klights_leader_api::ResourceQueryError> {
        let grpc = self.grpc.as_ref().ok_or_else(|| {
            klights_leader_api::ResourceQueryError::retryable(
                "RemoteApiClient missing gRPC transport",
            )
        })?;
        let request = klights_leader_api::ResourceListRequest::try_new(
            req.api_version.clone(),
            req.kind.clone(),
            req.namespace.clone(),
            req.label_selector.clone(),
            req.field_selector.clone(),
            req.limit,
            req.continue_token.clone(),
            klights_leader_api::ResourceQueryConsistency::LeaderFresh,
        )?;
        let list = legacy_list_response(grpc.list_resources_rpc(request).await?);
        replace_scope(self.cache.as_ref(), &req, list.clone())
            .await
            .map_err(query_error)?;
        self.cache
            .mark_ready(scope_for_request(&req))
            .await
            .map_err(query_error)?;
        Ok(list)
    }

    fn grpc(&self) -> Result<&Arc<ReplicationGrpcClient>> {
        self.grpc
            .as_ref()
            .ok_or_else(|| anyhow!("RemoteApiClient missing gRPC transport"))
    }

    fn list_pods_on_node_request(&self, node_name: &str) -> ListRequest {
        ListRequest {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: None,
            label_selector: None,
            field_selector: Some(format!("spec.nodeName={node_name}")),
            limit: None,
            continue_token: None,
        }
    }

    fn required_worker_list_requests(&self) -> Vec<ListRequest> {
        let mut reqs = vec![self.list_pods_on_node_request(&self.node_name)];
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
            reqs.push(ListRequest {
                api_version: api_version.to_string(),
                kind: kind.to_string(),
                namespace: namespace.map(str::to_string),
                label_selector: None,
                field_selector: None,
                limit: None,
                continue_token: None,
            });
        }
        reqs
    }
}

fn watch_error_requires_relist(err: &LeaderWatchError) -> bool {
    matches!(err, LeaderWatchError::ReplayExpired { .. })
}

impl LeaderResourceQuery for RemoteApiClient {
    fn get_resource(
        &self,
        request: ResourceGetRequest,
    ) -> ResourceQueryFuture<'_, Option<Resource>> {
        Box::pin(async move {
            let consistency = request.consistency();
            let key = request.into_key();
            if consistency == ResourceQueryConsistency::LeaderFresh {
                let grpc = self.grpc.as_ref().ok_or_else(|| {
                    klights_leader_api::ResourceQueryError::retryable(
                        "leader-fresh resource query has no gRPC transport",
                    )
                })?;
                let resource = grpc.get_resource_rpc(key.clone()).await?;
                if let Some(resource) = &resource {
                    self.cache.insert(resource.clone()).await;
                }
                return Ok(resource);
            }

            if let Some(resource) = self.cache.get(&key).await {
                return Ok(Some(resource));
            }
            let legacy_request = ListRequest {
                api_version: key.api_version.clone(),
                kind: key.kind.clone(),
                namespace: key.namespace.clone(),
                label_selector: None,
                field_selector: None,
                limit: None,
                continue_token: None,
            };
            let scope = scope_for_request(&legacy_request);
            if self.cache.is_ready(&scope).await {
                return Ok(None);
            }
            if self.grpc.is_some() {
                self.prime_list_scope(legacy_request)
                    .await
                    .map_err(query_error)?;
                return Ok(self.cache.get(&key).await);
            }
            Ok(None)
        })
    }

    fn list_resources(
        &self,
        request: ResourceListRequest,
    ) -> ResourceQueryFuture<'_, ResourceListResult> {
        Box::pin(async move {
            let consistency = request.consistency();
            let legacy_request = legacy_list_request(&request);
            let list = if consistency == ResourceQueryConsistency::LeaderFresh {
                self.prime_list_scope(legacy_request).await?
            } else {
                let scope = scope_for_request(&legacy_request);
                if self.cache.is_ready(&scope).await {
                    list_cached(self.cache.as_ref(), &legacy_request)
                        .await
                        .map_err(query_error)?
                } else if self.grpc.is_some() {
                    self.prime_list_scope(legacy_request)
                        .await
                        .map_err(query_error)?
                } else {
                    list_cached(self.cache.as_ref(), &legacy_request)
                        .await
                        .map_err(query_error)?
                }
            };
            query_list_result(list)
        })
    }
}

impl LeaderResourceCommand for RemoteApiClient {
    fn submit_resource_command(
        &self,
        request: ResourceCommandRequest,
    ) -> ResourceCommandFuture<'_, ResourceCommandResult> {
        Box::pin(async move {
            let grpc = self.grpc.as_ref().ok_or_else(|| {
                ResourceCommandError::retryable("RemoteApiClient missing gRPC transport")
            })?;
            grpc.submit_resource_command_rpc(request).await
        })
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
        Box::pin(async move {
            self.grpc()
                .map_err(|error| LeaderWatchError::unavailable(error.to_string()))?
                .watch_resources_rpc(req)
                .await
        })
    }
}

impl LeaderCacheReadiness for RemoteApiClient {
    fn wait_cache_ready(&self, scope: CacheReadinessRequest) -> CacheReadinessFuture<'_> {
        Box::pin(async move {
            if self.grpc.is_none() && !self.cache.is_ready(&scope).await {
                return Err(CacheReadinessError::unavailable(format!(
                    "cache scope {scope:?} not yet primed"
                )));
            }
            self.cache.wait_ready(scope).await;
            Ok(())
        })
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
    use std::sync::Arc;

    use bytes::Bytes;
    use futures::StreamExt as _;
    use serde_json::json;

    use crate::control_plane::client::remote::RemoteApiClient;
    use crate::datastore::ResourcePreconditions;
    use crate::datastore::backend::DatastoreHandle;
    use crate::node_outbox::payload::OutboxPayload;
    use crate::replication::grpc::client::{
        GrpcClientConfig, JoinDataplaneMetadata, ReplicationGrpcClient,
    };
    use crate::replication::protocol::JoinRole;
    use crate::replication::service::ReplicationService;
    use klights_cluster_core::command::StorageCommand;
    use klights_leader_api::OutboxDeliveryError as OutboxApplyError;
    use klights_leader_api::{
        CacheReadinessError, CacheReadinessRequest, LeaderCacheReadiness,
        LeaderNetworkTopologyQuery, LeaderNodeSubnetAllocation, LeaderResourceCommand,
        LeaderResourceQuery, LeaderWatch, LeaderWatchError, NodeDataplaneQuery,
        NodeSubnetAllocationError, NodeSubnetAllocationRequest, NodeSubnetQuery, PeerSubnetsQuery,
        ResourceEvent, ResourceGetRequest, ResourceListRequest, ResourceQueryConsistency,
        WatchEventType, WatchRequest, pod_get_request,
    };
    use klights_leader_api::{LeaderOutboxDelivery, OutboxDeliveryRequest};
    use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};
    use klights_types::ResourceKey;

    fn dataplane() -> JoinDataplaneMetadata {
        JoinDataplaneMetadata {
            public_key: None,
            endpoint: "127.0.0.1".to_string(),
            port: None,
            mode: klights_leader_api::NetworkNodeMode::Root,
            encryption: klights_leader_api::DataplaneEncryption::Direct,
        }
    }

    #[test]
    fn remote_api_client_exposes_resource_command_capability() {
        fn assert_capability<T: LeaderResourceCommand>() {}
        assert_capability::<RemoteApiClient>();
    }

    /// Self-signed `system:node:<name>` certificate (DER) for simulating the
    /// mTLS node identity in the in-process test harness.
    fn test_node_cert_der(node_name: &str) -> Vec<u8> {
        use rcgen::{CertificateParams, DnType, KeyPair};
        let mut params = CertificateParams::default();
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, format!("system:node:{node_name}"));
        params
            .distinguished_name
            .push(DnType::OrganizationName, "system:nodes".to_string());
        let key_pair = KeyPair::generate().unwrap();
        params.self_signed(&key_pair).unwrap().der().to_vec()
    }

    async fn remote_client_and_leader_db() -> (
        RemoteApiClient,
        DatastoreHandle,
        tokio::task::JoinHandle<()>,
    ) {
        remote_client_and_leader_db_with_node_names("worker-1".to_string(), "worker-1".to_string())
            .await
    }

    async fn remote_client_and_leader_db_with_node_names(
        remote_node_name: String,
        grpc_node_name: String,
    ) -> (
        RemoteApiClient,
        DatastoreHandle,
        tokio::task::JoinHandle<()>,
    ) {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let token = crate::bootstrap::cluster_meta::read_join_token(db.as_ref())
            .await
            .unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let service = Arc::new(ReplicationService::new(db.clone(), supervisor.clone()));
        let app = crate::replication::grpc::server::mount_service(
            axum::Router::new(),
            service,
            db.clone(),
            crate::replication::grpc::transport_policy::GrpcTransportPolicy::shared_default(),
        );
        // Simulate the mTLS edge: in production the TLS layer injects the
        // caller's client certificate; over the in-process plaintext channel we
        // inject the gRPC transport's node cert so node-scoped RPCs
        // (NodeRestriction) see the same authenticated identity.
        let grpc_node_cert = test_node_cert_der(&grpc_node_name);
        let app = app.layer(axum::middleware::from_fn(
            move |mut request: axum::extract::Request, next: axum::middleware::Next| {
                let grpc_node_cert = grpc_node_cert.clone();
                async move {
                    request
                        .extensions_mut()
                        .insert(klights_types::TlsClientCertificate(grpc_node_cert));
                    next.run(request).await
                }
            },
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let grpc = Arc::new(
            ReplicationGrpcClient::connect(
                GrpcClientConfig {
                    leader_endpoint: endpoint,
                    token,
                    node_name: grpc_node_name,
                    role: JoinRole::Worker,
                    dataplane: dataplane(),
                    ca_cert_path: None,
                    skip_ca: false,
                    client_cert_pem: None,
                    client_key_pem: None,
                },
                supervisor.clone(),
                crate::replication::grpc::transport_policy::GrpcTransportPolicy::shared_default(),
                crate::replication::grpc::client::NodeControlRuntimes::new(
                    crate::replication::grpc::client::NodeExecCapability::Unavailable,
                    crate::replication::grpc::client::NodeLogCapability::Unavailable,
                    crate::replication::grpc::client::NodeMetricsCapability::Unavailable,
                ),
            )
            .await
            .unwrap(),
        );
        (
            RemoteApiClient::from_grpc(
                grpc,
                supervisor,
                remote_node_name,
                Arc::new(crate::remote_informer_cache_adapter::WatchCacheAdapter::new()),
            ),
            db,
            handle,
        )
    }

    fn make_pod(ns: &str, name: &str, uid: &str, node_name: &str, phase: &str) -> super::Pod {
        let data = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": ns,
                "name": name,
                "uid": uid
            },
            "spec": {
                "nodeName": node_name,
                "containers": [{"name": "app", "image": "nginx"}]
            },
            "status": {
                "phase": phase
            }
        });
        crate::datastore::Resource {
            id: 0,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some(ns.to_string()),
            name: name.to_string(),
            uid: uid.to_string(),
            resource_version: 1,
            data: std::sync::Arc::new(data),
        }
    }

    fn pod_status_payload(uid: &str) -> Bytes {
        let command = StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web".to_string(),
            status: json!({"phase": "Running"}),
            expected_rv: None,
            preconditions: ResourcePreconditions {
                uid: Some(uid.to_string()),
                resource_version: None,
            },
            observed_status_stamp: None,
        };
        Bytes::from(
            OutboxPayload::from_command(command)
                .encode_protobuf()
                .expect("encode outbox payload"),
        )
    }

    #[test]
    fn watch_error_requires_relist_requires_typed_replay_expiry() {
        let expired = LeaderWatchError::ReplayExpired {
            accepted_resource_version: 51,
        };
        assert!(
            super::watch_error_requires_relist(&expired),
            "typed replay expiry must trigger relist"
        );

        for (error, name) in [
            (
                LeaderWatchError::transport("expired but unmarked"),
                "transport",
            ),
            (LeaderWatchError::Timeout, "timeout"),
            (LeaderWatchError::Cancelled, "cancelled"),
        ] {
            assert!(
                !super::watch_error_requires_relist(&error),
                "{name} must not trigger relist"
            );
        }
    }

    #[tokio::test]
    async fn grpc_cache_read_primes_unready_scope_before_reporting_miss() {
        let (client, db, handle) = remote_client_and_leader_db().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "web",
            (*make_pod("default", "web", "uid-1", "worker-1", "Pending").data).clone(),
        )
        .await
        .unwrap();

        let pod = client
            .get_resource(
                pod_get_request("default", "web", ResourceQueryConsistency::Cached)
                    .expect("valid Pod request"),
            )
            .await
            .expect("remote cache-prime get pod")
            .expect("unready cache scope should be synchronously primed before reporting absence");
        assert_eq!(pod.uid, "uid-1");

        db.update_status_only_with_preconditions(
            "v1",
            "Pod",
            Some("default"),
            "web",
            json!({"phase": "Running"}),
            ResourcePreconditions {
                uid: Some("uid-1".to_string()),
                resource_version: None,
            },
        )
        .await
        .unwrap();
        let cached = client
            .get_resource(
                pod_get_request("default", "web", ResourceQueryConsistency::Cached)
                    .expect("valid Pod request"),
            )
            .await
            .expect("remote cached pod")
            .expect("pod should remain cached");
        assert_eq!(
            cached
                .data
                .pointer("/status/phase")
                .and_then(|value| value.as_str()),
            Some("Pending"),
            "cache hit should not perform an unnecessary strong read"
        );
        handle.abort();
    }

    #[tokio::test]
    async fn missing_grpc_apply_outbox_is_retryable_not_acknowledged() {
        let client = RemoteApiClient::new_for_tests("worker-1");

        let err = client
            .deliver_outbox(
                OutboxDeliveryRequest::try_new(
                    "missing-grpc-watermarked-status",
                    klights_leader_api::OutboxDeliveryOperation::PodStatus,
                    Arc::<[u8]>::from(pod_status_payload("uid-1").to_vec()),
                    "worker-client",
                    7,
                    1,
                )
                .expect("valid delivery request"),
            )
            .await
            .expect_err("missing gRPC must not acknowledge a sequenced outbox row");

        assert!(
            matches!(&err, OutboxApplyError::Retryable(message) if message.contains("missing gRPC transport")),
            "missing gRPC should be a retryable dispatcher error, got {err:?}"
        );
    }

    #[tokio::test]
    async fn grpc_apply_outbox_node_identity_mismatch_is_terminal() {
        let (client, _db, handle) = remote_client_and_leader_db_with_node_names(
            "worker-1".to_string(),
            "worker-2".to_string(),
        )
        .await;

        let err = client
            .deliver_outbox(
                OutboxDeliveryRequest::try_new(
                    "identity-mismatch",
                    klights_leader_api::OutboxDeliveryOperation::PodStatus,
                    Arc::<[u8]>::from(pod_status_payload("uid-1").to_vec()),
                    "worker-client",
                    7,
                    1,
                )
                .expect("valid delivery request"),
            )
            .await
            .expect_err("identity mismatch must not remain in durable retry");

        assert!(
            matches!(&err, OutboxApplyError::ConflictTerminal(message) if message.contains("RemoteApiClient node identity")),
            "identity mismatch must be terminal, got {err:?}"
        );
        handle.abort();
    }

    #[tokio::test]
    async fn grpc_apply_outbox_uid_mismatch_propagates() {
        let (client, db, handle) = remote_client_and_leader_db().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "web",
            (*make_pod("default", "web", "uid-1", "worker-1", "Pending").data).clone(),
        )
        .await
        .unwrap();

        let err = client
            .deliver_outbox(
                OutboxDeliveryRequest::try_new(
                    "uid-mismatch",
                    klights_leader_api::OutboxDeliveryOperation::PodStatus,
                    Arc::<[u8]>::from(pod_status_payload("uid-2").to_vec()),
                    "client",
                    1,
                    1,
                )
                .expect("valid delivery request"),
            )
            .await
            .expect_err("unwatermarked leader uid mismatch must propagate");
        assert!(matches!(err, OutboxApplyError::UidMismatch { .. }));
        handle.abort();
    }

    #[tokio::test]
    async fn grpc_focused_pod_watch_streams_leader_events() {
        let (client, db, handle) = remote_client_and_leader_db().await;
        let mut stream = client
            .watch_resources(
                WatchRequest::try_new(
                    "v1",
                    "Pod",
                    None,
                    None,
                    Some("spec.nodeName=worker-1".to_string()),
                    None,
                    None,
                )
                .expect("valid Pod watch"),
            )
            .await
            .expect("open remote pod watch");
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "watched",
            (*make_pod("default", "watched", "uid-watch", "worker-1", "Pending").data).clone(),
        )
        .await
        .unwrap();

        let event = stream
            .next()
            .await
            .expect("watch should yield")
            .expect("watch event should decode");
        assert_eq!(event.resource().name, "watched");
        assert_eq!(event.resource().uid, "uid-watch");
        handle.abort();
    }

    #[tokio::test]
    async fn grpc_network_metadata_uses_typed_unary_rpcs() {
        let (client, db, handle) = remote_client_and_leader_db().await;

        let subnet = client
            .allocate_node_subnet(
                NodeSubnetAllocationRequest::try_new("worker-1", "10.42.0.0/16", "192.0.2.20")
                    .expect("valid request"),
            )
            .await
            .expect("allocate worker subnet through typed gRPC")
            .into_subnet();
        assert_eq!(subnet.node_name(), "worker-1");
        assert_eq!(subnet.subnet(), "10.42.0.0/24");

        let fetched = client
            .get_node_subnet(NodeSubnetQuery::try_new("worker-1").expect("valid query"))
            .await
            .expect("get worker subnet through typed gRPC")
            .into_option()
            .expect("worker subnet should exist");
        assert_eq!(fetched, subnet);

        let peer_error = client
            .allocate_node_subnet(
                NodeSubnetAllocationRequest::try_new("worker-2", "10.42.0.0/16", "192.0.2.21")
                    .expect("valid request"),
            )
            .await
            .expect_err("worker certificate must not allocate a peer subnet");
        assert!(matches!(
            peer_error,
            NodeSubnetAllocationError::Unauthorized { .. }
        ));
        let peers = client
            .list_peer_subnets(PeerSubnetsQuery::try_new("worker-1").expect("valid query"))
            .await
            .expect("list peer subnets through typed gRPC")
            .into_vec();
        assert!(peers.is_empty());

        let stored_metadata = db
            .get_node_dataplane("worker-1")
            .await
            .expect("dataplane metadata lookup")
            .expect("join should have stored worker dataplane metadata");
        let fetched_metadata = client
            .get_node_dataplane(NodeDataplaneQuery::try_new("worker-1").expect("valid query"))
            .await
            .expect("get worker dataplane metadata through typed gRPC")
            .into_option();
        assert_eq!(
            fetched_metadata,
            Some(
                crate::control_plane::client::focused_dataplane(stored_metadata)
                    .expect("valid focused metadata"),
            )
        );

        handle.abort();
    }

    #[tokio::test]
    async fn grpc_watch_replays_events_after_start_resource_version() {
        let (client, db, handle) = remote_client_and_leader_db().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "old",
            (*make_pod("default", "old", "uid-old", "worker-1", "Pending").data).clone(),
        )
        .await
        .unwrap();
        let start_rv = db.get_current_resource_version().await.unwrap();
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "missed",
            (*make_pod("default", "missed", "uid-missed", "worker-1", "Pending").data).clone(),
        )
        .await
        .unwrap();

        let mut stream = client
            .watch_resources(
                WatchRequest::try_new(
                    "v1",
                    "Pod",
                    None,
                    None,
                    Some("spec.nodeName=worker-1".to_string()),
                    Some(start_rv),
                    None,
                )
                .expect("valid continuation watch"),
            )
            .await
            .expect("open continuation watch");
        let event = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
            .await
            .expect("continuation watch should replay missed event")
            .expect("stream should yield")
            .expect("watch event should decode");
        assert!(
            event
                .resume_position()
                .is_some_and(|position| position.event_id > 0),
            "gRPC watch events must carry an apply-order resume position"
        );
        let pod_name = event
            .resource()
            .data
            .pointer("/metadata/name")
            .and_then(|value| value.as_str());
        assert_eq!(pod_name, Some("missed"));
        handle.abort();
    }

    #[tokio::test]
    async fn grpc_list_position_round_trips_into_lossless_watch_resume() {
        let (client, db, handle) = remote_client_and_leader_db().await;
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "before-list",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"namespace": "default", "name": "before-list"}
            }),
        )
        .await
        .unwrap();
        let list_req = ResourceListRequest::try_new(
            "v1",
            "ConfigMap",
            Some("default".to_string()),
            None,
            None,
            None,
            None,
            ResourceQueryConsistency::LeaderFresh,
        )
        .expect("valid ConfigMap list request");
        let list = client
            .list_resources(list_req)
            .await
            .expect("list through gRPC");
        let list_position = list
            .watch_replay_position()
            .expect("gRPC LIST must preserve its atomic replay position");
        assert!(list_position.event_id > 0);

        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "after-list",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"namespace": "default", "name": "after-list"}
            }),
        )
        .await
        .unwrap();
        let mut stream = client
            .watch_resources(
                WatchRequest::try_new(
                    "v1",
                    "ConfigMap",
                    Some("default".to_string()),
                    None,
                    None,
                    Some(list.resource_version()),
                    Some(list_position),
                )
                .expect("valid positioned ConfigMap watch"),
            )
            .await
            .expect("resume watch from atomic list position");
        let event = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
            .await
            .expect("post-list event must replay")
            .expect("stream should yield")
            .expect("event should decode");
        assert_eq!(event.resource().data["metadata"]["name"], "after-list");
        assert!(
            event
                .resume_position()
                .is_some_and(|position| position.event_id > list_position.event_id)
        );
        handle.abort();
    }

    #[tokio::test]
    async fn watch_continuation_after_disconnect() {
        // Tests that the informer cache can be rebuilt after a watch disconnect.
        // Simulates: cache primed, disconnect clears scope, re-list repopulates.
        let client = RemoteApiClient::new_for_tests("worker-1");

        let pod_scope = CacheReadinessRequest::try_new("v1", "Pod", None, None, None)
            .expect("valid cache scope");

        // Prime the scope and insert data
        client.cache_prime_scope(pod_scope.clone()).await;
        client
            .cache_insert_pod(make_pod("default", "web", "uid-1", "worker-1", "Running"))
            .await;

        // Verify cache is ready
        assert!(client.wait_cache_ready(pod_scope.clone()).await.is_ok());

        // Simulate 410 Gone: clear scope and re-prime
        // In production, RemoteApiClient would re-list and re-prime;
        // here we test that the rebuilt cache works correctly.
        client.cache_clear_scope_for_test(&pod_scope).await;
        assert!(client.wait_cache_ready(pod_scope.clone()).await.is_err());

        // Re-prime and re-insert
        client.cache_prime_scope(pod_scope.clone()).await;
        client
            .cache_insert_pod(make_pod("default", "web", "uid-2", "worker-1", "Running"))
            .await;
        assert!(client.wait_cache_ready(pod_scope).await.is_ok());
        let pod = client
            .get_resource(
                pod_get_request("default", "web", ResourceQueryConsistency::Cached)
                    .expect("valid Pod request"),
            )
            .await
            .unwrap();
        assert!(pod.is_some());
        assert_eq!(pod.unwrap().uid, "uid-2");
    }

    #[tokio::test]
    async fn unary_fallback_on_cache_miss() {
        // Tests that when the cache misses, the client signals the result
        // correctly (None when not found). In production this would trigger
        // a unary gRPC GetResource; here the cache simply returns None.
        let client = RemoteApiClient::new_for_tests("worker-1");

        // No pod in cache → cache miss → returns None
        let result = client
            .get_resource(
                pod_get_request("default", "nonexistent", ResourceQueryConsistency::Cached)
                    .expect("valid Pod request"),
            )
            .await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none(), "cache miss should return None");

        // Insert pod → cache hit
        client
            .cache_insert_pod(make_pod("default", "web", "uid-1", "worker-1", "Running"))
            .await;
        let result = client
            .get_resource(
                pod_get_request("default", "web", ResourceQueryConsistency::Cached)
                    .expect("valid Pod request"),
            )
            .await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_some(), "cache hit should return pod");
    }

    #[tokio::test]
    async fn cache_based_get_resource_returns_primed_value() {
        let client = RemoteApiClient::new_for_tests("worker-1");
        let scope =
            CacheReadinessRequest::try_new("v1", "Pod", Some("default".to_string()), None, None)
                .expect("valid cache scope");
        let pod = make_pod("default", "web", "uid-1", "worker-1", "Running");
        client.cache_prime_scope(scope).await;
        client.cache_insert_pod(pod.clone()).await;

        let fetched = client
            .get_resource(
                ResourceGetRequest::try_new(
                    ResourceKey {
                        api_version: "v1".to_string(),
                        kind: "Pod".to_string(),
                        namespace: Some("default".to_string()),
                        name: "web".to_string(),
                    },
                    ResourceQueryConsistency::Cached,
                )
                .expect("valid Pod request"),
            )
            .await
            .expect("get_resource");

        assert_eq!(
            fetched.as_ref().map(|resource| resource.uid.as_str()),
            Some("uid-1")
        );
        assert_eq!(
            fetched.as_ref().map(|resource| resource.resource_version),
            Some(pod.resource_version)
        );
    }

    #[tokio::test]
    async fn leader_fresh_get_never_falls_back_to_a_primed_cache_without_transport() {
        let client = RemoteApiClient::new_for_tests("worker-1");
        let scope =
            CacheReadinessRequest::try_new("v1", "Pod", Some("default".to_string()), None, None)
                .expect("valid cache scope");
        client.cache_prime_scope(scope).await;
        client
            .cache_insert_pod(make_pod(
                "default",
                "web",
                "stale-uid",
                "worker-1",
                "Running",
            ))
            .await;

        let error = client
            .get_resource(
                pod_get_request("default", "web", ResourceQueryConsistency::LeaderFresh)
                    .expect("valid leader-fresh request"),
            )
            .await
            .expect_err("leader-fresh must fail closed without a leader transport");
        assert!(matches!(
            error,
            klights_leader_api::ResourceQueryError::Retryable { .. }
        ));
        let error = client
            .list_resources(
                ResourceListRequest::try_new(
                    "v1",
                    "Pod",
                    Some("default".to_string()),
                    None,
                    None,
                    None,
                    None,
                    ResourceQueryConsistency::LeaderFresh,
                )
                .unwrap(),
            )
            .await
            .expect_err("leader-fresh LIST must fail closed without a leader transport");
        assert!(matches!(
            error,
            klights_leader_api::ResourceQueryError::Retryable { .. }
        ));
    }

    #[tokio::test]
    async fn cache_readiness_keeps_selector_scopes_distinct() {
        let client = RemoteApiClient::new_for_tests("worker-1");
        let selected = CacheReadinessRequest::try_new(
            "v1",
            "Pod",
            None,
            None,
            Some("spec.nodeName=worker-1".to_string()),
        )
        .expect("valid selected Pod scope");
        let unfiltered = CacheReadinessRequest::try_new("v1", "Pod", None, None, None)
            .expect("valid unfiltered Pod scope");

        client.cache_prime_scope(selected.clone()).await;
        assert!(client.wait_cache_ready(selected).await.is_ok());
        assert!(matches!(
            client.wait_cache_ready(unfiltered).await,
            Err(CacheReadinessError::Unavailable { .. })
        ));
    }

    #[tokio::test]
    async fn apply_outbox_without_grpc_is_retryable() {
        let client = RemoteApiClient::new_for_tests("worker-1");

        let err = client
            .deliver_outbox(
                OutboxDeliveryRequest::try_new(
                    "key-1",
                    klights_leader_api::OutboxDeliveryOperation::PodStatus,
                    Arc::<[u8]>::from(&b"test"[..]),
                    "client",
                    1,
                    1,
                )
                .expect("valid delivery request"),
            )
            .await
            .expect_err("missing gRPC must not acknowledge an outbox row");
        assert!(
            matches!(&err, OutboxApplyError::Retryable(message) if message.contains("missing gRPC transport")),
            "missing gRPC should be retryable, got {err:?}"
        );
    }

    #[tokio::test]
    async fn all_required_worker_cache_scopes_prime() {
        let client = RemoteApiClient::new_for_tests("worker-1");
        let requests = client.required_worker_list_requests();
        let scopes: Vec<_> = requests
            .iter()
            .map(crate::control_plane::client::informer::scope_for_request)
            .collect();

        assert!(
            requests.iter().any(|req| req.api_version == "v1"
                && req.kind == "Pod"
                && req.field_selector.as_deref() == Some("spec.nodeName=worker-1")),
            "worker Pod informer must be scoped to this node"
        );
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
                requests.iter().any(|req| req.api_version == api_version
                    && req.kind == kind
                    && req.namespace.as_deref() == namespace),
                "missing worker cache scope {api_version}/{kind}/{namespace:?}"
            );
        }

        for scope in &scopes {
            client.cache_prime_scope(scope.clone()).await;
        }

        for scope in scopes {
            let result = client.wait_cache_ready(scope.clone()).await;
            assert!(
                result.is_ok(),
                "wait_cache_ready for {scope:?} should succeed"
            );
        }
    }

    #[tokio::test]
    async fn watch_idle_timeout_fires_when_stream_is_wedged() {
        // bug-grpc: a worker watch that delivers neither an event nor a
        // heartbeat within the idle window is wedged (partial loss let the
        // keepalive PING through but not the watch DATA). The driver must
        // surface Idle so it can reconnect from the last resourceVersion,
        // instead of blocking forever (the 10-minute pod-deletion stall).
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));

        let mut wedged = super::WatchStream::unpositioned_test_stream(futures::stream::pending());
        let started = std::time::Instant::now();
        let outcome = super::next_event_within_idle(
            Some(&supervisor),
            std::time::Duration::from_millis(150),
            &mut wedged,
        )
        .await;
        assert!(
            matches!(outcome, super::IdleNext::Idle),
            "a wedged stream must surface Idle within the idle window"
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(5));

        // A live stream passes its item straight through — no false idle.
        let pod = make_pod("default", "web", "uid-1", "worker-1", "Running");
        let event =
            ResourceEvent::try_new(WatchEventType::Added, pod, None).expect("valid live event");
        let mut live =
            super::WatchStream::unpositioned_test_stream(futures::stream::once(async move {
                Ok(event)
            }));
        let outcome = super::next_event_within_idle(
            Some(&supervisor),
            std::time::Duration::from_secs(5),
            &mut live,
        )
        .await;
        assert!(
            matches!(outcome, super::IdleNext::Item(Ok(_))),
            "a live event must pass through, not be reported as idle"
        );
    }

    /// bug-grpc B2/B3: cursor-advance-only-after-safe-apply. `run_watch_driver`
    /// advances its resume `next_resource_version` only after applying each
    /// canonical event. This locks the direct watch-cache behavior: BOOKMARK is
    /// a no-op while a resource event updates the cache before cursor advance.
    #[tokio::test]
    async fn informer_apply_event_gates_cursor_advance() {
        let cache = klights_watch::WatchCache::new();

        // BOOKMARK: apply is a no-op success, so its RV is a safe resume point
        // the driver may advance to.
        let bookmark = ResourceEvent::try_new(
            WatchEventType::Bookmark,
            crate::datastore::Resource::from_data_lossy(Arc::new(serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"resourceVersion": "42"}
            }))),
            None,
        )
        .expect("valid bookmark");
        assert!(
            cache.apply_event(&bookmark).await.is_none(),
            "a BOOKMARK must apply as a no-op so its RV is a valid resume point"
        );

        // A well-formed event applies successfully (cursor may advance).
        let pod = make_pod("default", "web", "uid-1", "worker-1", "Running");
        let good =
            ResourceEvent::try_new(WatchEventType::Added, pod, None).expect("valid Pod event");
        assert!(
            cache.apply_event(&good).await.is_some(),
            "a well-formed event must apply so its RV becomes the resume point"
        );
    }
}
