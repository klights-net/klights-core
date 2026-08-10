use klights_leader_api::{
    AuthenticatedOutboxDeliveryRequest, CacheReadinessFuture, CacheReadinessRequest,
    LeaderAuthenticatedOutboxDelivery, LeaderAuthenticatedProjectedServiceAccountToken,
    LeaderCacheReadiness, LeaderNetworkTopologyCommand, LeaderNetworkTopologyQuery,
    LeaderNodeLeaseRenewal, LeaderNodeLifecycleStatus, LeaderNodeSubnetAllocation,
    LeaderOutboxDelivery, LeaderPodCleanupIntents, LeaderProjectedServiceAccountToken,
    LeaderResourceQuery, LeaderWatch, LeaderWatchFuture, NetworkDataplane, NetworkTopologyError,
    NetworkTopologyFuture, NodeDataplaneQuery, NodeDataplaneResult, NodeLeaseRenewalError,
    NodeLeaseRenewalFuture, NodeLeaseRenewalRequest, NodeLeaseRenewalResult,
    NodeLifecycleStatusError, NodeLifecycleStatusFuture, NodeLifecycleStatusRequest,
    NodeLifecycleStatusResult, NodeSubnetAllocationError, NodeSubnetAllocationFuture,
    NodeSubnetAllocationRequest, NodeSubnetAllocationResult, NodeSubnetQuery, NodeSubnetResult,
    OutboxDeliveryFuture, OutboxDeliveryRequest, OutboxPayloadCodec, PeerSubnetsQuery,
    PeerSubnetsResult, PodCleanupIntent, PodCleanupIntentAckRequest, PodCleanupIntentError,
    PodCleanupIntentFuture, PodCleanupIntentListRequest, ProjectedServiceAccountTokenError,
    ProjectedServiceAccountTokenFuture, ProjectedServiceAccountTokenRequest, ResourceGetRequest,
    ResourceListRequest, ResourceListResult, ResourceQueryConsistency, ResourceQueryFuture,
    WatchRequest,
};
#[cfg(any(test, feature = "pod-repository-test-support"))]
use klights_leader_api::{
    LeaderResourceCommand, ResourceCommandFuture, ResourceCommandRequest, ResourceCommandResult,
};
use std::sync::Arc;
use tokio::sync::OnceCell;
use tokio::sync::watch;

use crate::control_plane::client::{
    focused_dataplane, focused_node_subnet, query_error, query_list_result,
};
use crate::datastore::{DatastoreHandle, Resource};
use async_trait::async_trait;
use klights_auth::projected_service_account_token::{
    authorize_projected_service_account_token, sign_authorized_projected_service_account_token,
};
use klights_cluster_core::LogApplyPodCleanupIntentRow as StoredPodCleanupIntent;
use klights_cluster_core::command::StorageCommand;
use klights_controllers::ControllerDispatcher;
#[cfg(test)]
use klights_kubelet::node_outbox::payload::OutboxOperationExt as _;
use klights_kubelet::pod_repository::store::PodStore;
use klights_replication::proposal::RaftProposal;

pub(crate) fn ensure_mark_delete_timestamps(
    data: &mut serde_json::Value,
    grace_seconds: i64,
    operation_now: chrono::DateTime<chrono::Utc>,
) {
    let Some(metadata) = data
        .get_mut("metadata")
        .and_then(serde_json::Value::as_object_mut)
    else {
        return;
    };
    if metadata
        .get("deletionTimestamp")
        .and_then(serde_json::Value::as_str)
        .is_none_or(str::is_empty)
    {
        metadata.insert(
            "deletionTimestamp".to_string(),
            serde_json::Value::String(klights_cluster_core::k8s_time::format_legacy_timestamp(
                operation_now,
            )),
        );
    }
    metadata
        .entry("deletionGracePeriodSeconds".to_string())
        .or_insert_with(|| serde_json::Value::from(grace_seconds));
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
type ProjectedTokenAsyncBoundary = Arc<
    dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>>
        + Send
        + Sync,
>;

#[cfg(any(test, feature = "pod-repository-test-support"))]
#[derive(Clone)]
struct ProjectedTokenIssueTestProbe {
    async_boundary: ProjectedTokenAsyncBoundary,
    sign_attempts: Arc<std::sync::atomic::AtomicUsize>,
}

#[cfg(test)]
struct ProjectedTokenIssueTestRegistration {
    namespace: String,
    sign_attempts: Arc<std::sync::atomic::AtomicUsize>,
}

#[cfg(test)]
impl ProjectedTokenIssueTestRegistration {
    fn sign_attempts(&self) -> usize {
        self.sign_attempts
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(test)]
impl Drop for ProjectedTokenIssueTestRegistration {
    fn drop(&mut self) {
        projected_token_issue_test_probes()
            .lock()
            .expect("projected-token test probe lock")
            .remove(&self.namespace);
    }
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
fn projected_token_issue_test_probes()
-> &'static std::sync::Mutex<std::collections::HashMap<String, ProjectedTokenIssueTestProbe>> {
    static PROBES: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<String, ProjectedTokenIssueTestProbe>>,
    > = std::sync::OnceLock::new();
    PROBES.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

#[cfg(test)]
fn install_projected_token_issue_test_probe(
    namespace: String,
    async_boundary: ProjectedTokenAsyncBoundary,
) -> ProjectedTokenIssueTestRegistration {
    let sign_attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let replaced = projected_token_issue_test_probes()
        .lock()
        .expect("projected-token test probe lock")
        .insert(
            namespace.clone(),
            ProjectedTokenIssueTestProbe {
                async_boundary,
                sign_attempts: sign_attempts.clone(),
            },
        );
    assert!(replaced.is_none(), "projected-token test namespace reused");
    ProjectedTokenIssueTestRegistration {
        namespace,
        sign_attempts,
    }
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
fn projected_token_issue_test_probe(namespace: &str) -> Option<ProjectedTokenIssueTestProbe> {
    projected_token_issue_test_probes()
        .lock()
        .expect("projected-token test probe lock")
        .get(namespace)
        .cloned()
}

#[cfg(test)]
use klights_leader_api::pod_get_request;

#[cfg(any(test, feature = "pod-repository-test-support"))]
fn test_watch_signals(db: &DatastoreHandle) -> Arc<dyn klights_watch::WatchSignalSubscribe> {
    let sink = db.commit_observation_sink();
    sink.as_any()
        .downcast_ref::<crate::bootstrap::watch_commit_wiring::WatchCommitObservationSink>()
        .expect("test datastore watch sink")
        .signal_source()
}

pub(crate) struct LocalApiPersistencePorts {
    db: DatastoreHandle,
    positioned_watch: klights_watch::PositionedWatchService,
}

/// Focused leader-query adapter over the selected passive cluster store.
///
/// This adapter is constructed before the legacy P9 compatibility shell so
/// canonical resource commands can resolve idempotent committed results
/// without a dependency cycle through `LocalApiClient`.
pub(crate) struct ClusterStoreLeaderResourceQuery {
    db: DatastoreHandle,
    is_leader_rx: watch::Receiver<bool>,
}

impl ClusterStoreLeaderResourceQuery {
    pub(crate) fn new(db: DatastoreHandle, is_leader_rx: watch::Receiver<bool>) -> Self {
        Self { db, is_leader_rx }
    }

    fn sample_leader_fresh(
        &self,
        consistency: ResourceQueryConsistency,
    ) -> Result<Option<watch::Receiver<bool>>, klights_leader_api::ResourceQueryError> {
        if consistency != ResourceQueryConsistency::LeaderFresh {
            return Ok(None);
        }
        let mut receiver = self.is_leader_rx.clone();
        if !*receiver.borrow_and_update() {
            return Err(klights_leader_api::ResourceQueryError::retryable(
                "leader-fresh resource query reached a non-leader local store",
            ));
        }
        Ok(Some(receiver))
    }
}

impl LeaderResourceQuery for ClusterStoreLeaderResourceQuery {
    fn get_resource(
        &self,
        request: ResourceGetRequest,
    ) -> ResourceQueryFuture<'_, Option<Resource>> {
        Box::pin(async move {
            let leadership = self.sample_leader_fresh(request.consistency())?;
            let key = request.key();
            let resource = self
                .db
                .get_resource(
                    &key.api_version,
                    &key.kind,
                    key.namespace.as_deref(),
                    &key.name,
                )
                .await
                .map_err(query_error)?;
            if leadership
                .as_ref()
                .is_some_and(|receiver| receiver.has_changed().unwrap_or(true))
            {
                return Err(klights_leader_api::ResourceQueryError::retryable(
                    "leadership changed during local leader-fresh resource query",
                ));
            }
            Ok(resource)
        })
    }

    fn list_resources(
        &self,
        request: ResourceListRequest,
    ) -> ResourceQueryFuture<'_, ResourceListResult> {
        Box::pin(async move {
            let leadership = self.sample_leader_fresh(request.consistency())?;
            let list = self
                .db
                .list_resources(
                    request.api_version(),
                    request.kind(),
                    request.namespace(),
                    crate::datastore::ResourceListQuery::new(
                        request.label_selector(),
                        request.field_selector(),
                        request.limit(),
                        request.continue_token(),
                    ),
                )
                .await
                .map_err(query_error)?;
            if leadership
                .as_ref()
                .is_some_and(|receiver| receiver.has_changed().unwrap_or(true))
            {
                return Err(klights_leader_api::ResourceQueryError::retryable(
                    "leadership changed during local leader-fresh resource query",
                ));
            }
            query_list_result(list)
        })
    }
}

pub(crate) struct ClusterStoreLeaderNetwork {
    db: DatastoreHandle,
    proposal: Arc<dyn RaftProposal>,
    is_leader_rx: watch::Receiver<bool>,
}

impl ClusterStoreLeaderNetwork {
    pub(crate) fn new(
        db: DatastoreHandle,
        proposal: Arc<dyn RaftProposal>,
        is_leader_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            db,
            proposal,
            is_leader_rx,
        }
    }

    fn require_leader(&self) -> Result<(), NetworkTopologyError> {
        if *self.is_leader_rx.borrow() {
            Ok(())
        } else {
            Err(NetworkTopologyError::NotLeader)
        }
    }

    pub(crate) async fn update_node_peer_attributes(
        &self,
        node_name: &str,
        mode: klights_types::NodePeerMode,
        hostport_range: Option<klights_types::HostPortRange>,
    ) -> Result<(), NetworkTopologyError> {
        self.require_leader()?;
        self.proposal
            .propose_command(StorageCommand::UpdateNodePeerAttributes {
                node_name: node_name.to_string(),
                mode: match mode {
                    klights_types::NodePeerMode::Root => "root",
                    klights_types::NodePeerMode::Rootless => "rootless",
                }
                .to_string(),
                hostport_range: hostport_range.map(|range| range.to_string()),
            })
            .await
            .map_err(|error| NetworkTopologyError::query_failed(error.to_string()))?;
        Ok(())
    }

    pub(crate) async fn delete_node_subnet(
        &self,
        node_name: &str,
    ) -> Result<(), NetworkTopologyError> {
        self.require_leader()?;
        self.proposal
            .propose_command(StorageCommand::DeleteNodeSubnet {
                node_name: node_name.to_string(),
            })
            .await
            .map_err(|error| NetworkTopologyError::query_failed(error.to_string()))?;
        Ok(())
    }
}

impl LeaderNodeSubnetAllocation for ClusterStoreLeaderNetwork {
    fn allocate_node_subnet(
        &self,
        request: NodeSubnetAllocationRequest,
    ) -> NodeSubnetAllocationFuture<'_, NodeSubnetAllocationResult> {
        Box::pin(async move {
            self.require_leader()
                .map_err(|_| NodeSubnetAllocationError::NotLeader)?;
            let (node_name, cluster_cidr, node_ip) = request.into_parts();
            self.proposal
                .propose_command(StorageCommand::AllocateNodeSubnet {
                    node_name: node_name.clone(),
                    subnet: cluster_cidr.clone(),
                    node_ip: node_ip.to_string(),
                })
                .await
                .map_err(|error| {
                    let message = error.to_string();
                    if super::node_subnet_allocation_is_exhausted(&message) {
                        NodeSubnetAllocationError::exhausted(cluster_cidr.clone())
                    } else if message.to_ascii_lowercase().contains("conflict") {
                        NodeSubnetAllocationError::conflict(message)
                    } else {
                        NodeSubnetAllocationError::allocation_failed(message)
                    }
                })?;
            let subnet = self
                .db
                .get_node_subnet(&node_name)
                .await
                .map_err(|error| NodeSubnetAllocationError::allocation_failed(error.to_string()))?
                .map(focused_node_subnet)
                .transpose()
                .map_err(|error| NodeSubnetAllocationError::corrupt_response(error.to_string()))?;
            NodeSubnetAllocationResult::try_from_wire(&node_name, subnet)
        })
    }
}

impl LeaderNetworkTopologyQuery for ClusterStoreLeaderNetwork {
    fn get_node_subnet(
        &self,
        request: NodeSubnetQuery,
    ) -> NetworkTopologyFuture<'_, NodeSubnetResult> {
        Box::pin(async move {
            self.require_leader()?;
            let node_name = request.into_node_name();
            let subnet = self
                .db
                .get_node_subnet(&node_name)
                .await
                .map_err(|error| NetworkTopologyError::query_failed(error.to_string()))?
                .map(focused_node_subnet)
                .transpose()?;
            NodeSubnetResult::try_from_wire(&node_name, subnet.is_some(), subnet)
        })
    }

    fn list_peer_subnets(
        &self,
        request: PeerSubnetsQuery,
    ) -> NetworkTopologyFuture<'_, PeerSubnetsResult> {
        Box::pin(async move {
            self.require_leader()?;
            let node_name = request.into_node_name();
            let rows = self
                .db
                .list_peer_subnets(
                    klights_cluster_store::PeerTopologyRequest::excluding(&node_name)
                        .map_err(|error| NetworkTopologyError::query_failed(error.to_string()))?,
                )
                .await
                .map_err(|error| NetworkTopologyError::query_failed(error.to_string()))?;
            let subnets = rows
                .into_iter()
                .map(focused_node_subnet)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            PeerSubnetsResult::try_new(&node_name, subnets)
        })
    }

    fn get_node_dataplane(
        &self,
        request: NodeDataplaneQuery,
    ) -> NetworkTopologyFuture<'_, NodeDataplaneResult> {
        Box::pin(async move {
            self.require_leader()?;
            let node_name = request.into_node_name();
            let metadata = self
                .db
                .get_node_dataplane(&node_name)
                .await
                .map_err(|error| NetworkTopologyError::query_failed(error.to_string()))?
                .map(focused_dataplane)
                .transpose()?;
            NodeDataplaneResult::try_from_wire(&node_name, metadata.is_some(), metadata)
        })
    }
}

impl LeaderNetworkTopologyCommand for ClusterStoreLeaderNetwork {
    fn register_node_dataplane(&self, metadata: NetworkDataplane) -> NetworkTopologyFuture<'_, ()> {
        Box::pin(async move {
            self.require_leader()?;
            let metadata = crate::control_plane::client::legacy_dataplane(metadata)?;
            self.proposal
                .propose_command(StorageCommand::UpdateNodeDataplane {
                    node_name: metadata.node_name,
                    mode: metadata.mode.as_str().to_string(),
                    encryption: metadata.encryption.as_str().to_string(),
                    public_key: metadata.public_key.as_ref().map(ToString::to_string),
                    endpoint: metadata.endpoint.to_string(),
                    port: metadata.port,
                })
                .await
                .map_err(|error| NetworkTopologyError::query_failed(error.to_string()))?;
            Ok(())
        })
    }
}

impl LocalApiPersistencePorts {
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    pub(crate) fn new(
        db: DatastoreHandle,
        passive_reads: crate::datastore::selector::PassiveReadPorts,
        _watch_signals: Arc<dyn klights_watch::WatchSignalSubscribe>,
    ) -> Self {
        let positioned_watch =
            crate::bootstrap::composition_adapters::positioned_watch_adapter::for_test(
                &passive_reads,
                db.clone(),
            );
        Self {
            db,
            positioned_watch,
        }
    }

    pub(crate) fn new_with_positioned_watch(
        db: DatastoreHandle,
        positioned_watch: klights_watch::PositionedWatchService,
    ) -> Self {
        Self {
            db,
            positioned_watch,
        }
    }
}

/// T6 step 1: builds a `watch::Receiver<bool>` that is permanently true.
///
/// Use cases:
/// - Tests that exercise leader-only write paths (the only role they
///   model) and don't care about the gate.
/// - Boot paths that have already established "this is the leader" before
///   any write originates (e.g. a single-voter seed after
///   `bootstrap_single_voter` succeeds).
///
/// Production code that runs on cp/replica must NOT use this helper —
/// it must subscribe to the bootstrap's real `is_leader_tx` watch so the
/// gate tracks live raft state. A source guard added in T6 step 5 will
/// enforce that.
pub fn always_leader_watch() -> watch::Receiver<bool> {
    let (tx, rx) = watch::channel(true);
    // Keep the sender alive forever so the receiver never observes a
    // sender-dropped closure. `Box::leak` is the simplest way to express
    // "this channel lives for the program's lifetime" without an Arc
    // dance, and it's only invoked from boot/test wiring (never hot).
    Box::leak(Box::new(tx));
    rx
}

pub(crate) fn focused_pod_cleanup_intent(
    intent: StoredPodCleanupIntent,
) -> std::result::Result<PodCleanupIntent, PodCleanupIntentError> {
    let snapshot = Resource::try_from_data(Arc::new(intent.pod_data)).map_err(|error| {
        PodCleanupIntentError::corrupt_intent(format!(
            "cleanup intent Pod snapshot has invalid identity: {error}"
        ))
    })?;
    PodCleanupIntent::try_new(
        intent.node_name,
        intent.namespace,
        intent.pod_name,
        intent.pod_uid,
        intent.reason,
        intent.resource_version,
        intent.created_at_ms,
        snapshot,
    )
}

pub(crate) fn legacy_pod_cleanup_intent(intent: PodCleanupIntent) -> StoredPodCleanupIntent {
    let (node_name, namespace, pod_name, pod_uid, reason, resource_version, created_at_ms, pod) =
        intent.into_parts();
    StoredPodCleanupIntent {
        node_name,
        namespace,
        pod_name,
        pod_uid,
        reason,
        resource_version,
        created_at_ms,
        pod_data: (*pod.data).clone(),
    }
}

pub(crate) struct ClusterStoreLeaderPodCleanup {
    db: DatastoreHandle,
    proposal: Arc<dyn RaftProposal>,
    is_leader_rx: watch::Receiver<bool>,
}

impl ClusterStoreLeaderPodCleanup {
    pub(crate) fn new(
        db: DatastoreHandle,
        proposal: Arc<dyn RaftProposal>,
        is_leader_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            db,
            proposal,
            is_leader_rx,
        }
    }

    fn require_leader(&self) -> Result<(), PodCleanupIntentError> {
        if *self.is_leader_rx.borrow() {
            Ok(())
        } else {
            Err(PodCleanupIntentError::NotLeader)
        }
    }

    pub(crate) async fn move_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<(), PodCleanupIntentError> {
        self.require_leader()?;
        self.proposal
            .propose_command(StorageCommand::MovePodToCleanupIntent {
                node_name: node_name.to_string(),
                namespace: namespace.to_string(),
                pod_name: pod_name.to_string(),
                pod_uid: pod_uid.to_string(),
                reason: reason.to_string(),
            })
            .await
            .map_err(|error| PodCleanupIntentError::unavailable(error.to_string()))?;
        Ok(())
    }

    pub(crate) async fn delete_all_for_node(
        &self,
        node_name: &str,
    ) -> Result<(), PodCleanupIntentError> {
        self.require_leader()?;
        self.proposal
            .propose_command(StorageCommand::DeletePodCleanupIntentsForNode {
                node_name: node_name.to_string(),
            })
            .await
            .map_err(|error| PodCleanupIntentError::unavailable(error.to_string()))?;
        Ok(())
    }
}

impl LeaderPodCleanupIntents for ClusterStoreLeaderPodCleanup {
    fn list_pod_cleanup_intents(
        &self,
        request: PodCleanupIntentListRequest,
    ) -> PodCleanupIntentFuture<'_, Vec<PodCleanupIntent>> {
        Box::pin(async move {
            self.require_leader()?;
            self.db
                .list_pod_cleanup_intents_for_node(request.node_name())
                .await
                .map_err(|error| PodCleanupIntentError::unavailable(error.to_string()))?
                .into_iter()
                .map(focused_pod_cleanup_intent)
                .collect()
        })
    }

    fn acknowledge_pod_cleanup_intent(
        &self,
        request: PodCleanupIntentAckRequest,
    ) -> PodCleanupIntentFuture<'_, ()> {
        Box::pin(async move {
            self.require_leader()?;
            let (node_name, namespace, pod_name, pod_uid, reason) = request.into_parts();
            self.proposal
                .propose_command(StorageCommand::DeletePodCleanupIntent {
                    node_name,
                    namespace,
                    pod_name,
                    pod_uid,
                    reason,
                })
                .await
                .map_err(|error| PodCleanupIntentError::unavailable(error.to_string()))?;
            Ok(())
        })
    }
}

pub(crate) struct ClusterStoreLeaderMaintenance {
    db: DatastoreHandle,
    proposal: Arc<dyn RaftProposal>,
    is_leader_rx: watch::Receiver<bool>,
}

impl ClusterStoreLeaderMaintenance {
    pub(crate) fn new(
        db: DatastoreHandle,
        proposal: Arc<dyn RaftProposal>,
        is_leader_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            db,
            proposal,
            is_leader_rx,
        }
    }

    fn require_leader(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            *self.is_leader_rx.borrow(),
            "operation requires current raft leader"
        );
        Ok(())
    }

    pub(crate) async fn gc_applied_outbox(
        &self,
        now_ms: i64,
        ttl_ms: i64,
    ) -> anyhow::Result<usize> {
        self.require_leader()?;
        let cutoff_ms = now_ms.saturating_sub(ttl_ms);
        let prunable = self.db.applied_outbox_gc_prunable_count(cutoff_ms).await?;
        if prunable == 0 {
            return Ok(0);
        }
        self.proposal
            .propose_command(StorageCommand::GcAppliedOutbox { cutoff_ms })
            .await?;
        Ok(prunable)
    }
}

#[async_trait]
impl klights_cluster_store::ClusterWatchMaintenance for ClusterStoreLeaderMaintenance {
    async fn advance_resource_version_after(&self, min_rv: i64) -> anyhow::Result<i64> {
        self.require_leader()?;
        let before = self.db.get_current_resource_version().await?;
        let new_rv = before.saturating_add(1).max(min_rv.saturating_add(1));
        self.proposal
            .propose_command(StorageCommand::AdvanceResourceVersion { min_rv, new_rv })
            .await?;
        self.db.get_current_resource_version().await.or(Ok(new_rv))
    }

    async fn watch_events_gc_prunable_count(
        &self,
        max_rows: i64,
        batch_cap: i64,
    ) -> anyhow::Result<usize> {
        self.db
            .watch_events_gc_prunable_count(max_rows, batch_cap)
            .await
    }

    async fn gc_watch_events(&self, max_rows: i64, batch_cap: i64) -> anyhow::Result<usize> {
        self.require_leader()?;
        let prunable = self
            .db
            .watch_events_gc_prunable_count(max_rows, batch_cap)
            .await?;
        if prunable == 0 {
            return Ok(0);
        }
        self.proposal
            .propose_command(StorageCommand::GcWatchEvents {
                max_rows,
                batch_cap,
            })
            .await?;
        Ok(prunable)
    }
}

#[async_trait]
impl klights_cluster_store::ClusterMetadataMutation for ClusterStoreLeaderMaintenance {
    async fn get_klights_meta(&self, key: &str) -> anyhow::Result<Option<String>> {
        self.db.get_klights_meta(key).await
    }

    async fn set_klights_meta(&self, key: &str, value: &str) -> anyhow::Result<()> {
        self.require_leader()?;
        self.proposal
            .propose_command(StorageCommand::SetKlightsMeta {
                key: key.to_string(),
                value: value.to_string(),
            })
            .await?;
        Ok(())
    }
}

pub(crate) struct RootOutboxSideEffectState {
    db: DatastoreHandle,
    controller_dispatcher: OnceCell<Arc<ControllerDispatcher>>,
    non_pod_finalization: OnceCell<Arc<dyn klights_reconcile_api::GcNonPodFinalizationPort>>,
    namespace_termination: OnceCell<Arc<dyn klights_reconcile_api::NamespaceTerminationSink>>,
}

impl RootOutboxSideEffectState {
    fn new(db: DatastoreHandle) -> Self {
        Self {
            db,
            controller_dispatcher: OnceCell::new(),
            non_pod_finalization: OnceCell::new(),
            namespace_termination: OnceCell::new(),
        }
    }

    fn set_controller_dispatcher(&self, dispatcher: Arc<ControllerDispatcher>) {
        let _ = self.controller_dispatcher.set(dispatcher);
    }

    fn set_non_pod_finalization(
        &self,
        port: Arc<dyn klights_reconcile_api::GcNonPodFinalizationPort>,
    ) {
        let _ = self.non_pod_finalization.set(port);
    }

    fn set_namespace_termination(
        &self,
        port: Arc<dyn klights_reconcile_api::NamespaceTerminationSink>,
    ) {
        let _ = self.namespace_termination.set(port);
    }
}

pub(crate) struct RootCommittedOutboxDelivery {
    embedded: Arc<klights_replication::leader_api::EmbeddedOutboxDelivery>,
    side_effects: Arc<RootOutboxSideEffectState>,
    codec: Arc<dyn OutboxPayloadCodec>,
    local_node: String,
}

impl RootCommittedOutboxDelivery {
    pub(crate) fn new(
        embedded: Arc<klights_replication::leader_api::EmbeddedOutboxDelivery>,
        side_effects: Arc<RootOutboxSideEffectState>,
        codec: Arc<dyn OutboxPayloadCodec>,
        local_node: String,
    ) -> Self {
        Self {
            embedded,
            side_effects,
            codec,
            local_node,
        }
    }

    async fn deliver_authenticated(
        &self,
        request: AuthenticatedOutboxDeliveryRequest,
    ) -> Result<klights_leader_api::OutboxDeliveryResult, klights_leader_api::OutboxDeliveryError>
    {
        let (authenticated_node, request) = request.into_parts();
        let (
            codec_version,
            idempotency_key,
            operation,
            payload,
            client_id,
            stream_id,
            stream_sequence,
        ) = request.into_parts();
        if !klights_cluster_core::supports_command_codec_version(codec_version) {
            return Err(klights_leader_api::OutboxDeliveryError::codec_incompatible(
                codec_version,
                klights_cluster_core::COMMAND_CODEC_VERSION,
            ));
        }
        let decoded_command = self.codec.decode(payload.as_ref()).map_err(|error| {
            klights_leader_api::OutboxDeliveryError::invalid("delivery.payload", error.to_string())
        });
        let side_effect_command = decoded_command
            .as_ref()
            .ok()
            .filter(|command| {
                klights_controllers::side_effects::applied_pod::needs_committed_pod_side_effects(
                    command,
                )
            })
            .cloned();
        let watermark = Some(klights_cluster_core::OutboxStreamWatermark {
            client_id,
            stream_id,
            stream_seq: stream_sequence,
        });
        let effect = self
            .embedded
            .deliver_authenticated_outbox_command_effect(
                authenticated_node,
                idempotency_key,
                operation,
                decoded_command,
                watermark,
            )
            .await?;
        let (result, resource_effect, pod_endpoint_effect, resource) = effect.into_parts();
        if let Some(command) = side_effect_command.as_ref() {
            self.dispatch_committed_side_effects(
                command,
                resource.as_ref(),
                resource_effect,
                pod_endpoint_effect,
            )
            .await?;
        }
        Ok(result.into())
    }

    async fn dispatch_committed_side_effects(
        &self,
        command: &StorageCommand,
        resource: Option<&Resource>,
        resource_effect: klights_cluster_core::ResourceMutationEffect,
        pod_endpoint_effect: klights_cluster_core::PodEndpointEffect,
    ) -> Result<(), klights_leader_api::OutboxDeliveryError> {
        if resource_effect == klights_cluster_core::ResourceMutationEffect::Unchanged
            && resource.is_none()
        {
            return Ok(());
        }
        let controller_dispatcher =
            self.side_effects
                .controller_dispatcher
                .get()
                .ok_or_else(|| {
                    klights_leader_api::OutboxDeliveryError::unavailable(
                        "controller dispatcher is not ready for committed Pod side effects",
                    )
                })?;
        let gc_pod_delete_sink = if matches!(
            command,
            StorageCommand::DeleteResource { api_version, kind, .. }
                if api_version == "v1" && kind == "Pod"
        ) || matches!(command, StorageCommand::FinalizeBoundPod { .. })
        {
            Some(controller_dispatcher.pod_delete_sink())
        } else {
            None
        };
        crate::bootstrap::controller_adapters::applied_pod_side_effect_adapter::handle_applied_pod_side_effects(
            klights_controllers::side_effects::applied_pod::AppliedPodSideEffectSinks::new(
                Some(controller_dispatcher.as_ref()
                    as &dyn klights_reconcile_api::ControllerReconcileSink),
                Some(controller_dispatcher.as_ref()
                    as &dyn klights_reconcile_api::ServiceReconcileSink),
                #[cfg(any(test, feature = "pod-repository-test-support"))]
                gc_pod_delete_sink,
                #[cfg(not(any(test, feature = "pod-repository-test-support")))]
                gc_pod_delete_sink,
                self.side_effects
                    .non_pod_finalization
                    .get()
                    .map(Arc::as_ref),
                self.side_effects
                    .namespace_termination
                    .get()
                    .map(Arc::as_ref),
                controller_dispatcher.gc_coordination(),
            ),
            command,
            resource,
            pod_endpoint_effect,
            self.side_effects.db.as_ref(),
        )
        .await
        .map_err(|error| klights_leader_api::OutboxDeliveryError::unavailable(error.to_string()))
    }
}

impl LeaderAuthenticatedOutboxDelivery for RootCommittedOutboxDelivery {
    fn deliver_authenticated_outbox(
        &self,
        request: AuthenticatedOutboxDeliveryRequest,
    ) -> OutboxDeliveryFuture<'_> {
        Box::pin(self.deliver_authenticated(request))
    }
}

impl LeaderOutboxDelivery for RootCommittedOutboxDelivery {
    fn deliver_outbox(&self, request: OutboxDeliveryRequest) -> OutboxDeliveryFuture<'_> {
        Box::pin(async move {
            let request =
                AuthenticatedOutboxDeliveryRequest::try_new(self.local_node.clone(), request)?;
            self.deliver_authenticated(request).await
        })
    }
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
#[derive(Clone)]
struct LocalApiTestServices {
    resource_command: Arc<dyn LeaderResourceCommand>,
    outbox_delivery: Arc<RootCommittedOutboxDelivery>,
}

#[derive(Clone)]
pub struct LocalApiClient {
    db: DatastoreHandle,
    resource_query: Arc<dyn LeaderResourceQuery>,
    positioned_watch: klights_watch::PositionedWatchService,
    pod_store: Arc<PodStore>,
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    test_services: LocalApiTestServices,
    authoring_node: String,
    containerd_namespace: String,
    service_account_signing_key_path: std::path::PathBuf,
    file_process: klights_supervisor::FileProcessExecutor,
    crypto: klights_supervisor::CryptoExecutor,
    node_lease_tracker: Arc<klights_controllers::node_lease::NodeLeaseTracker>,
    /// Set once the leader's `ControllerDispatcher` is constructed (later in
    /// bootstrap than `LocalApiClient`). When present, every successful
    /// outbox apply on a Pod status fires the same Service / workload
    /// reconcile keys that the gRPC `Replication::apply_outbox` handler
    /// fires for remote-worker forwarded writes.
    outbox_side_effects: Arc<RootOutboxSideEffectState>,
    /// T6 step 1 inner gate: every mutation method on this client first
    /// reads `*is_leader_rx.borrow()`. When false (this node is not the
    /// elected raft leader) the call is refused with
    /// `WriteRejection::FollowerWrite`; reads stay allowed. Promotion is
    /// a watch flip — no rewiring needed. The receiver is mandatory in
    /// the constructor so the gate cannot be skipped by accident.
    is_leader_rx: watch::Receiver<bool>,
}

/// A one-operation leadership generation fence. `watch` versions advance on
/// every send, so `has_changed` detects both ordinary demotion and a
/// demote/promote ABA even when the latest boolean is `true` again.
struct LeadershipGenerationFence {
    receiver: watch::Receiver<bool>,
}

impl LeadershipGenerationFence {
    fn sample(
        mut receiver: watch::Receiver<bool>,
    ) -> Result<Self, ProjectedServiceAccountTokenError> {
        if !*receiver.borrow_and_update() {
            return Err(ProjectedServiceAccountTokenError::NotLeader);
        }
        Ok(Self { receiver })
    }

    fn ensure_unchanged(&self) -> Result<(), ProjectedServiceAccountTokenError> {
        let current = self.receiver.borrow();
        if !*current || self.receiver.has_changed().unwrap_or(true) {
            Err(ProjectedServiceAccountTokenError::NotLeader)
        } else {
            Ok(())
        }
    }

    #[cfg(test)]
    fn sign_if_unchanged<T>(
        &self,
        sign: impl FnOnce() -> T,
    ) -> Result<T, ProjectedServiceAccountTokenError> {
        let current = self.receiver.borrow();
        if !*current || self.receiver.has_changed().unwrap_or(true) {
            return Err(ProjectedServiceAccountTokenError::NotLeader);
        }
        Ok(sign())
    }
}

impl LocalApiClient {
    fn issue_projected_token_after_transport_auth(
        &self,
        request: ProjectedServiceAccountTokenRequest,
    ) -> ProjectedServiceAccountTokenFuture<'_> {
        Box::pin(async move {
            let leadership = LeadershipGenerationFence::sample(self.is_leader_rx.clone())?;
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            if let Some(probe) = projected_token_issue_test_probe(&self.containerd_namespace) {
                (probe.async_boundary)().await;
            }
            let signing_key_pem = klights_cluster_datastore::signing_key_state::read_with_executor(
                &self.service_account_signing_key_path,
                &self.file_process,
            )
            .await;
            let signing_key_pem = signing_key_pem.map_err(|error| {
                ProjectedServiceAccountTokenError::signing_failed(format!(
                    "ServiceAccount signing key for {} is unavailable: {error}",
                    self.containerd_namespace
                ))
            });
            leadership.ensure_unchanged()?;
            let signing_key_pem = signing_key_pem?;
            let resources = crate::bootstrap::composition_adapters::projected_token_resource_adapter::ProjectedTokenResourceAdapter::new(
                self.db.as_ref(),
                self.pod_store.as_ref(),
            );
            let claims = authorize_projected_service_account_token(&resources, &request).await;
            leadership.ensure_unchanged()?;
            let claims = claims?;
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            if let Some(probe) = projected_token_issue_test_probe(&self.containerd_namespace) {
                probe
                    .sign_attempts
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            let crypto: &klights_supervisor::CryptoExecutor = &self.crypto;
            let token = crypto
                .run_blocking("sign-projected-service-account-token", move || {
                    sign_authorized_projected_service_account_token(
                        &signing_key_pem,
                        claims,
                        &klights_auth::clock::SystemClock,
                    )
                })
                .await
                .map_err(|error| {
                    ProjectedServiceAccountTokenError::signing_failed(format!(
                        "projected ServiceAccount token signing worker failed: {error}"
                    ))
                })?;
            leadership.ensure_unchanged()?;
            token
        })
    }

    #[cfg(any(test, feature = "pod-repository-test-support"))]
    pub(crate) fn new(
        db: DatastoreHandle,
        authoring_node: String,
        is_leader_rx: watch::Receiver<bool>,
    ) -> Self {
        Self::new_with_file_process(
            db,
            authoring_node,
            is_leader_rx,
            crate::bootstrap::file_blocking::test_file_process_executor(),
        )
    }

    #[cfg(test)]
    pub(crate) fn new_with_passive_reads(
        db: DatastoreHandle,
        passive_reads: crate::datastore::selector::PassiveReadPorts,
        authoring_node: String,
        is_leader_rx: watch::Receiver<bool>,
    ) -> Self {
        Self::new_with_node_lease_tracker_and_containerd_namespace_and_file_process_with_reads(
            db,
            passive_reads,
            authoring_node,
            std::env::var("KLIGHTS_CONTAINERD_NAMESPACE").unwrap_or_else(|_| "klights".to_string()),
            Arc::new(klights_controllers::node_lease::NodeLeaseTracker::new_at(
                chrono::Utc::now(),
            )),
            is_leader_rx,
            crate::bootstrap::file_blocking::test_file_process_executor(),
        )
    }

    #[cfg(any(test, feature = "pod-repository-test-support"))]
    pub(crate) fn new_with_file_process(
        db: DatastoreHandle,
        authoring_node: String,
        is_leader_rx: watch::Receiver<bool>,
        file_process: klights_supervisor::FileProcessExecutor,
    ) -> Self {
        Self::new_with_node_lease_tracker_and_containerd_namespace_and_file_process(
            db,
            authoring_node,
            std::env::var("KLIGHTS_CONTAINERD_NAMESPACE").unwrap_or_else(|_| "klights".to_string()),
            Arc::new(klights_controllers::node_lease::NodeLeaseTracker::new_at(
                chrono::Utc::now(),
            )),
            is_leader_rx,
            file_process,
        )
    }

    #[cfg(test)]
    pub(crate) fn new_with_node_lease_tracker(
        db: DatastoreHandle,
        authoring_node: String,
        node_lease_tracker: Arc<klights_controllers::node_lease::NodeLeaseTracker>,
        is_leader_rx: watch::Receiver<bool>,
    ) -> Self {
        Self::new_with_node_lease_tracker_and_file_process(
            db,
            authoring_node,
            node_lease_tracker,
            is_leader_rx,
            crate::bootstrap::file_blocking::test_file_process_executor(),
        )
    }

    #[cfg(any(test, feature = "pod-repository-test-support"))]
    pub(crate) fn new_with_node_lease_tracker_and_passive_reads(
        db: DatastoreHandle,
        passive_reads: crate::datastore::selector::PassiveReadPorts,
        authoring_node: String,
        node_lease_tracker: Arc<klights_controllers::node_lease::NodeLeaseTracker>,
        is_leader_rx: watch::Receiver<bool>,
        file_process: klights_supervisor::FileProcessExecutor,
    ) -> Self {
        Self::new_with_node_lease_tracker_and_containerd_namespace_and_file_process_with_reads(
            db,
            passive_reads,
            authoring_node,
            std::env::var("KLIGHTS_CONTAINERD_NAMESPACE").unwrap_or_else(|_| "klights".to_string()),
            node_lease_tracker,
            is_leader_rx,
            file_process,
        )
    }

    #[cfg(test)]
    pub(crate) fn new_with_node_lease_tracker_and_file_process(
        db: DatastoreHandle,
        authoring_node: String,
        node_lease_tracker: Arc<klights_controllers::node_lease::NodeLeaseTracker>,
        is_leader_rx: watch::Receiver<bool>,
        file_process: klights_supervisor::FileProcessExecutor,
    ) -> Self {
        Self::new_with_node_lease_tracker_and_containerd_namespace_and_file_process(
            db,
            authoring_node,
            std::env::var("KLIGHTS_CONTAINERD_NAMESPACE").unwrap_or_else(|_| "klights".to_string()),
            node_lease_tracker,
            is_leader_rx,
            file_process,
        )
    }

    #[cfg(any(test, feature = "pod-repository-test-support"))]
    pub(crate) fn new_with_node_lease_tracker_and_containerd_namespace_and_file_process(
        db: DatastoreHandle,
        authoring_node: String,
        containerd_namespace: String,
        node_lease_tracker: Arc<klights_controllers::node_lease::NodeLeaseTracker>,
        is_leader_rx: watch::Receiver<bool>,
        file_process: klights_supervisor::FileProcessExecutor,
    ) -> Self {
        Self::new_with_node_lease_tracker_and_containerd_namespace_and_file_process_with_reads(
            db,
            crate::datastore::selector::unused_fail_closed_passive_read_ports(),
            authoring_node,
            containerd_namespace,
            node_lease_tracker,
            is_leader_rx,
            file_process,
        )
    }

    #[cfg(any(test, feature = "pod-repository-test-support"))]
    fn new_with_node_lease_tracker_and_containerd_namespace_and_file_process_with_reads(
        db: DatastoreHandle,
        passive_reads: crate::datastore::selector::PassiveReadPorts,
        authoring_node: String,
        containerd_namespace: String,
        node_lease_tracker: Arc<klights_controllers::node_lease::NodeLeaseTracker>,
        is_leader_rx: watch::Receiver<bool>,
        file_process: klights_supervisor::FileProcessExecutor,
    ) -> Self {
        let signing_key_path =
            crate::paths::service_account_signing_key_path(&containerd_namespace);
        Self::new_with_node_lease_tracker_namespace_signing_key_and_file_process(
            LocalApiPersistencePorts::new(db.clone(), passive_reads, test_watch_signals(&db)),
            authoring_node,
            containerd_namespace,
            signing_key_path,
            node_lease_tracker,
            is_leader_rx,
            file_process,
        )
    }

    pub(crate) fn new_with_node_lease_tracker_namespace_signing_key_and_file_process(
        persistence: LocalApiPersistencePorts,
        authoring_node: String,
        containerd_namespace: String,
        service_account_signing_key_path: std::path::PathBuf,
        node_lease_tracker: Arc<klights_controllers::node_lease::NodeLeaseTracker>,
        is_leader_rx: watch::Receiver<bool>,
        file_process: klights_supervisor::FileProcessExecutor,
    ) -> Self {
        let LocalApiPersistencePorts {
            db,
            positioned_watch,
        } = persistence;
        let pod_store = Arc::new(crate::bootstrap::pod_repository_composition::new_pod_store(
            db.clone(),
        ));
        let resource_query: Arc<dyn LeaderResourceQuery> = Arc::new(
            ClusterStoreLeaderResourceQuery::new(db.clone(), is_leader_rx.clone()),
        );
        let crypto = file_process.crypto_executor();
        let outbox_side_effects = Arc::new(RootOutboxSideEffectState::new(db.clone()));
        #[cfg(any(test, feature = "pod-repository-test-support"))]
        let test_services = {
            let proposal: Arc<dyn klights_replication::proposal::RaftProposal> = Arc::new(
                crate::bootstrap::outbox_apply_adapter::BackendProposalFixture::new(db.clone()),
            );
            let resource_query: Arc<dyn LeaderResourceQuery> = Arc::new(
                crate::bootstrap::outbox_apply_adapter::BackendResourceQueryFixture::new(
                    db.clone(),
                    is_leader_rx.clone(),
                ),
            );
            let resource_command = Arc::new(
                klights_replication::leader_api::EmbeddedLeaderResourceCommand::new(
                    proposal.clone(),
                    resource_query.clone(),
                    is_leader_rx.clone(),
                ),
            );
            let embedded_outbox = Arc::new(
                klights_replication::leader_api::EmbeddedOutboxDelivery::new(
                    proposal,
                    resource_query,
                    is_leader_rx.clone(),
                ),
            );
            let committed_outbox = Arc::new(RootCommittedOutboxDelivery::new(
                embedded_outbox,
                outbox_side_effects.clone(),
                crate::bootstrap::composition_adapters::outbox_payload_codec_adapter::new_codec(),
                authoring_node.clone(),
            ));
            LocalApiTestServices {
                resource_command,
                outbox_delivery: committed_outbox,
            }
        };
        Self {
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            test_services,
            db: db.clone(),
            resource_query,
            positioned_watch,
            pod_store,
            authoring_node,
            containerd_namespace,
            service_account_signing_key_path,
            file_process,
            crypto,
            node_lease_tracker,
            outbox_side_effects,
            is_leader_rx,
        }
    }

    #[cfg(test)]
    pub(crate) async fn deliver_test_outbox(
        &self,
        idempotency_key: &str,
        operation: klights_kubelet::node_outbox::payload::OutboxOperation,
        payload: bytes::Bytes,
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
        self.test_services
            .outbox_delivery
            .deliver_outbox(request)
            .await
    }

    /// Wire in the leader's `ControllerDispatcher`. Called from the bootstrap
    /// runtime once the dispatcher has been built. Idempotent: a second call
    /// is silently ignored (OnceCell::set returns Err on repeat).
    pub fn set_controller_dispatcher(&self, dispatcher: Arc<ControllerDispatcher>) {
        self.outbox_side_effects
            .set_controller_dispatcher(dispatcher);
    }

    pub fn set_non_pod_finalization(
        &self,
        port: Arc<dyn klights_reconcile_api::GcNonPodFinalizationPort>,
    ) {
        self.outbox_side_effects.set_non_pod_finalization(port);
    }

    pub fn set_namespace_termination(
        &self,
        port: Arc<dyn klights_reconcile_api::NamespaceTerminationSink>,
    ) {
        self.outbox_side_effects.set_namespace_termination(port);
    }

    pub(crate) fn outbox_side_effect_state(&self) -> Arc<RootOutboxSideEffectState> {
        self.outbox_side_effects.clone()
    }
}

impl LeaderResourceQuery for LocalApiClient {
    fn get_resource(
        &self,
        request: ResourceGetRequest,
    ) -> ResourceQueryFuture<'_, Option<Resource>> {
        self.resource_query.get_resource(request)
    }

    fn list_resources(
        &self,
        request: ResourceListRequest,
    ) -> ResourceQueryFuture<'_, ResourceListResult> {
        self.resource_query.list_resources(request)
    }
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
impl LeaderResourceCommand for LocalApiClient {
    fn submit_resource_command(
        &self,
        request: ResourceCommandRequest,
    ) -> ResourceCommandFuture<'_, ResourceCommandResult> {
        self.test_services
            .resource_command
            .submit_resource_command(request)
    }
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
impl LeaderOutboxDelivery for LocalApiClient {
    fn deliver_outbox(&self, request: OutboxDeliveryRequest) -> OutboxDeliveryFuture<'_> {
        self.test_services.outbox_delivery.deliver_outbox(request)
    }
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
impl LeaderAuthenticatedOutboxDelivery for LocalApiClient {
    fn deliver_authenticated_outbox(
        &self,
        request: klights_leader_api::AuthenticatedOutboxDeliveryRequest,
    ) -> OutboxDeliveryFuture<'_> {
        self.test_services
            .outbox_delivery
            .deliver_authenticated_outbox(request)
    }
}

impl LeaderNodeLeaseRenewal for LocalApiClient {
    fn renew_node_lease(
        &self,
        request: NodeLeaseRenewalRequest,
    ) -> NodeLeaseRenewalFuture<'_, NodeLeaseRenewalResult> {
        Box::pin(async move {
            if !*self.is_leader_rx.borrow() {
                return Err(NodeLeaseRenewalError::NotLeader);
            }
            let (node_name, renew_time, lease_duration_seconds) = request.into_parts();
            self.node_lease_tracker
                .record_from_lease_object(
                    &node_name,
                    &serde_json::json!({
                        "metadata": {
                            "name": node_name,
                            "namespace": "kube-node-lease"
                        },
                        "spec": {
                            "holderIdentity": node_name,
                            "leaseDurationSeconds": lease_duration_seconds,
                            "renewTime": renew_time
                        }
                    }),
                )
                .await
                .map_err(|error| NodeLeaseRenewalError::InvalidRequest {
                    field: "lease.renew_time",
                    message: error.to_string(),
                })?;
            Ok(NodeLeaseRenewalResult::Renewed)
        })
    }
}

impl LeaderNodeLifecycleStatus for LocalApiClient {
    fn submit_node_lifecycle_status(
        &self,
        request: NodeLifecycleStatusRequest,
    ) -> NodeLifecycleStatusFuture<'_, NodeLifecycleStatusResult> {
        Box::pin(async move {
            if !*self.is_leader_rx.borrow() {
                return Err(NodeLifecycleStatusError::NotLeader);
            }
            let get = klights_leader_api::node_get_request(
                request.node_name(),
                klights_leader_api::ResourceQueryConsistency::LeaderFresh,
            )
            .map_err(|error| NodeLifecycleStatusError::apply_failed(error.to_string()))?;
            let current = LeaderResourceQuery::get_resource(self, get)
                .await
                .map_err(|error| NodeLifecycleStatusError::apply_failed(error.to_string()))?
                .ok_or(NodeLifecycleStatusError::NotFound)?;
            if current.uid != request.node_uid() {
                return Err(NodeLifecycleStatusError::UidMismatch);
            }
            if current.resource_version != request.resource_version() {
                return Err(NodeLifecycleStatusError::conflict(format!(
                    "Node resourceVersion changed from {} to {}",
                    request.resource_version(),
                    current.resource_version
                )));
            }
            let command = request.into_command();
            let StorageCommand::UpdateStatus {
                api_version,
                kind,
                namespace,
                name,
                status,
                preconditions,
                ..
            } = command
            else {
                unreachable!("NodeLifecycleStatusRequest admits only UpdateStatus")
            };
            let resource = self
                .db
                .update_status_only_with_preconditions(
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                    status,
                    preconditions,
                )
                .await
                .map_err(node_lifecycle_status_store_error)?;
            Ok(NodeLifecycleStatusResult::Updated {
                resource_version: resource.resource_version,
            })
        })
    }
}

fn node_lifecycle_status_store_error(error: anyhow::Error) -> NodeLifecycleStatusError {
    let message = format!("{error:#}");
    let lower = message.to_ascii_lowercase();
    if lower.contains("uid mismatch") {
        NodeLifecycleStatusError::UidMismatch
    } else if lower.contains("not found") || lower.contains("query returned no rows") {
        NodeLifecycleStatusError::NotFound
    } else if lower.contains("conflict") || lower.contains("precondition") {
        NodeLifecycleStatusError::conflict(message)
    } else if lower.contains("not raft leader") || lower.contains("follower") {
        NodeLifecycleStatusError::NotLeader
    } else {
        NodeLifecycleStatusError::apply_failed(message)
    }
}

impl LeaderWatch for LocalApiClient {
    fn watch_resources(&self, request: WatchRequest) -> LeaderWatchFuture<'_> {
        self.positioned_watch.watch_resources(request)
    }
}

impl LeaderCacheReadiness for LocalApiClient {
    fn wait_cache_ready(&self, _scope: CacheReadinessRequest) -> CacheReadinessFuture<'_> {
        Box::pin(async { Ok(()) })
    }
}

impl LeaderProjectedServiceAccountToken for LocalApiClient {
    fn issue_projected_service_account_token(
        &self,
        request: ProjectedServiceAccountTokenRequest,
    ) -> ProjectedServiceAccountTokenFuture<'_> {
        Box::pin(async move {
            if request.bound_node_name() != self.authoring_node {
                return Err(ProjectedServiceAccountTokenError::Unauthorized);
            }
            self.issue_projected_token_after_transport_auth(request)
                .await
        })
    }
}

/// Narrow adapter mounted only behind the gRPC handler's authenticated-node
/// check. Keeping this separate prevents local kubelet callers from bypassing
/// `LocalApiClient`'s `authoring_node` restriction.
pub(crate) struct AuthenticatedProjectedTokenIssuer {
    local: Arc<LocalApiClient>,
}

impl AuthenticatedProjectedTokenIssuer {
    pub(crate) fn new(local: Arc<LocalApiClient>) -> Self {
        Self { local }
    }
}

impl LeaderAuthenticatedProjectedServiceAccountToken for AuthenticatedProjectedTokenIssuer {
    fn issue_authenticated_projected_service_account_token(
        &self,
        request: ProjectedServiceAccountTokenRequest,
    ) -> ProjectedServiceAccountTokenFuture<'_> {
        self.local
            .issue_projected_token_after_transport_auth(request)
    }
}

impl LeaderPodCleanupIntents for LocalApiClient {
    fn list_pod_cleanup_intents(
        &self,
        request: PodCleanupIntentListRequest,
    ) -> PodCleanupIntentFuture<'_, Vec<PodCleanupIntent>> {
        Box::pin(async move {
            if !*self.is_leader_rx.borrow() {
                return Err(PodCleanupIntentError::NotLeader);
            }
            if request.node_name() != self.authoring_node {
                return Err(PodCleanupIntentError::Unauthorized);
            }
            self.db
                .list_pod_cleanup_intents_for_node(request.node_name())
                .await
                .map_err(|error| PodCleanupIntentError::unavailable(error.to_string()))?
                .into_iter()
                .map(focused_pod_cleanup_intent)
                .collect()
        })
    }

    fn acknowledge_pod_cleanup_intent(
        &self,
        request: PodCleanupIntentAckRequest,
    ) -> PodCleanupIntentFuture<'_, ()> {
        Box::pin(async move {
            if !*self.is_leader_rx.borrow() {
                return Err(PodCleanupIntentError::NotLeader);
            }
            if request.node_name() != self.authoring_node {
                return Err(PodCleanupIntentError::Unauthorized);
            }
            let (node_name, namespace, pod_name, pod_uid, reason) = request.into_parts();
            self.db
                .delete_pod_cleanup_intent(&node_name, &namespace, &pod_name, &pod_uid, &reason)
                .await
                .map_err(|error| PodCleanupIntentError::unavailable(error.to_string()))
        })
    }
}

impl LeaderNodeSubnetAllocation for LocalApiClient {
    fn allocate_node_subnet(
        &self,
        request: NodeSubnetAllocationRequest,
    ) -> NodeSubnetAllocationFuture<'_, NodeSubnetAllocationResult> {
        Box::pin(async move {
            if !*self.is_leader_rx.borrow() {
                return Err(NodeSubnetAllocationError::NotLeader);
            }
            let (node_name, cluster_cidr, node_ip) = request.into_parts();
            let subnet = self
                .db
                .allocate_node_subnet(&node_name, &cluster_cidr, &node_ip.to_string())
                .await
                .map_err(|error| {
                    let message = error.to_string();
                    if super::node_subnet_allocation_is_exhausted(&message) {
                        NodeSubnetAllocationError::exhausted(cluster_cidr.clone())
                    } else if message.to_ascii_lowercase().contains("conflict") {
                        NodeSubnetAllocationError::conflict(message)
                    } else {
                        NodeSubnetAllocationError::allocation_failed(message)
                    }
                })?;
            let subnet = focused_node_subnet(subnet)
                .map_err(|error| NodeSubnetAllocationError::corrupt_response(error.to_string()))?;
            NodeSubnetAllocationResult::try_from_wire(&node_name, Some(subnet))
        })
    }
}

impl LeaderNetworkTopologyQuery for LocalApiClient {
    fn get_node_subnet(
        &self,
        request: NodeSubnetQuery,
    ) -> NetworkTopologyFuture<'_, NodeSubnetResult> {
        Box::pin(async move {
            if !*self.is_leader_rx.borrow() {
                return Err(NetworkTopologyError::NotLeader);
            }
            let node_name = request.into_node_name();
            let subnet = self
                .db
                .get_node_subnet(&node_name)
                .await
                .map_err(|error| NetworkTopologyError::query_failed(error.to_string()))?
                .map(focused_node_subnet)
                .transpose()?;
            NodeSubnetResult::try_from_wire(&node_name, subnet.is_some(), subnet)
        })
    }

    fn list_peer_subnets(
        &self,
        request: PeerSubnetsQuery,
    ) -> NetworkTopologyFuture<'_, PeerSubnetsResult> {
        Box::pin(async move {
            if !*self.is_leader_rx.borrow() {
                return Err(NetworkTopologyError::NotLeader);
            }
            let node_name = request.into_node_name();
            let subnets = self
                .db
                .list_peer_subnets(
                    klights_cluster_store::PeerTopologyRequest::excluding(&node_name)
                        .map_err(|error| NetworkTopologyError::query_failed(error.to_string()))?,
                )
                .await
                .map_err(|error| NetworkTopologyError::query_failed(error.to_string()))?
                .into_iter()
                .map(focused_node_subnet)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            PeerSubnetsResult::try_new(&node_name, subnets)
        })
    }

    fn get_node_dataplane(
        &self,
        request: NodeDataplaneQuery,
    ) -> NetworkTopologyFuture<'_, NodeDataplaneResult> {
        Box::pin(async move {
            if !*self.is_leader_rx.borrow() {
                return Err(NetworkTopologyError::NotLeader);
            }
            let node_name = request.into_node_name();
            let metadata = self
                .db
                .get_node_dataplane(&node_name)
                .await
                .map_err(|error| NetworkTopologyError::query_failed(error.to_string()))?
                .map(focused_dataplane)
                .transpose()?;
            NodeDataplaneResult::try_from_wire(&node_name, metadata.is_some(), metadata)
        })
    }
}

impl LeaderNetworkTopologyCommand for LocalApiClient {
    fn register_node_dataplane(&self, metadata: NetworkDataplane) -> NetworkTopologyFuture<'_, ()> {
        Box::pin(async move {
            if !*self.is_leader_rx.borrow() {
                return Err(NetworkTopologyError::NotLeader);
            }
            let metadata = crate::control_plane::client::legacy_dataplane(metadata)?;
            self.db
                .update_node_dataplane(metadata.clone())
                .await
                .map_err(|error| NetworkTopologyError::query_failed(error.to_string()))?;
            let Some(resource) = self
                .db
                .get_resource("v1", "Node", None, &metadata.node_name)
                .await
                .map_err(|error| NetworkTopologyError::query_failed(error.to_string()))?
            else {
                return Ok(());
            };
            let mut data = (*resource.data).clone();
            if !crate::bootstrap::composition_adapters::node_routing_metadata::stamp_from_store(
                self.db.as_ref(),
                &metadata.node_name,
                &mut data,
            )
            .await
            .map_err(|error| NetworkTopologyError::query_failed(error.to_string()))?
            {
                return Ok(());
            }
            self.db
                .update_resource_with_preconditions(
                    "v1",
                    "Node",
                    None,
                    &metadata.node_name,
                    data,
                    klights_cluster_core::ResourcePreconditions::from_resource(&resource),
                )
                .await
                .map_err(|error| NetworkTopologyError::query_failed(error.to_string()))?;
            Ok(())
        })
    }
}

#[cfg(test)]
#[path = "tests/local.rs"]
mod inner_gate_tests;
