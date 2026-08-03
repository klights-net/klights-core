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
    ResourceListRequest, ResourceListResult, ResourceQueryFuture, WatchRequest,
};
#[cfg(test)]
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
use crate::kubelet::pod_repository::store::PodStore;
use klights_cluster_core::LogApplyPodCleanupIntentRow as StoredPodCleanupIntent;
use klights_cluster_core::command::StorageCommand;
use klights_controllers::ControllerDispatcher;
#[cfg(test)]
use klights_kubelet::node_outbox::payload::OutboxOperationExt as _;

#[cfg(test)]
type ProjectedTokenAsyncBoundary = Arc<
    dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>>
        + Send
        + Sync,
>;

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
fn projected_token_issue_test_probe(namespace: &str) -> Option<ProjectedTokenIssueTestProbe> {
    projected_token_issue_test_probes()
        .lock()
        .expect("projected-token test probe lock")
        .get(namespace)
        .cloned()
}

#[cfg(test)]
use klights_leader_api::{ResourceQueryConsistency, pod_get_request};

#[cfg(test)]
fn test_watch_signals(db: &DatastoreHandle) -> Arc<dyn klights_watch::WatchSignalSubscribe> {
    let sink = db.commit_observation_sink();
    sink.as_any()
        .downcast_ref::<crate::watch_commit_observation_adapter::WatchCommitObservationSink>()
        .expect("test datastore watch sink")
        .signal_source()
}

pub(crate) struct LocalApiPersistencePorts {
    db: DatastoreHandle,
    positioned_watch: klights_watch::PositionedWatchService,
}

impl LocalApiPersistencePorts {
    #[cfg(test)]
    pub(crate) fn new(
        db: DatastoreHandle,
        passive_reads: crate::datastore::selector::PassiveReadPorts,
        _watch_signals: Arc<dyn klights_watch::WatchSignalSubscribe>,
    ) -> Self {
        let positioned_watch =
            crate::positioned_watch_adapter::for_test(&passive_reads, db.clone());
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
        crate::applied_pod_side_effect_adapter::handle_applied_pod_side_effects(
            klights_controllers::side_effects::applied_pod::AppliedPodSideEffectSinks::new(
                Some(controller_dispatcher.as_ref()
                    as &dyn klights_reconcile_api::ControllerReconcileSink),
                Some(controller_dispatcher.as_ref()
                    as &dyn klights_reconcile_api::ServiceReconcileSink),
                #[cfg(test)]
                gc_pod_delete_sink,
                #[cfg(not(test))]
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

#[cfg(test)]
#[derive(Clone)]
struct LocalApiTestServices {
    resource_command: Arc<dyn LeaderResourceCommand>,
    outbox_delivery: Arc<RootCommittedOutboxDelivery>,
}

#[derive(Clone)]
pub struct LocalApiClient {
    db: DatastoreHandle,
    positioned_watch: klights_watch::PositionedWatchService,
    pod_store: Arc<PodStore>,
    #[cfg(test)]
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
            #[cfg(test)]
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
            let claims =
                crate::control_plane::service_account_tokens::authorize_projected_service_account_token(
                self.db.as_ref(),
                self.pod_store.as_ref(),
                &request,
            )
            .await;
            leadership.ensure_unchanged()?;
            let claims = claims?;
            #[cfg(test)]
            if let Some(probe) = projected_token_issue_test_probe(&self.containerd_namespace) {
                probe
                    .sign_attempts
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            let crypto: &klights_supervisor::CryptoExecutor = &self.crypto;
            let token = crypto
                .run_blocking("sign-projected-service-account-token", move || {
                    crate::control_plane::service_account_tokens::sign_authorized_projected_service_account_token(
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

    #[cfg(test)]
    pub(crate) fn new(
        db: DatastoreHandle,
        authoring_node: String,
        is_leader_rx: watch::Receiver<bool>,
    ) -> Self {
        Self::new_with_file_process(
            db,
            authoring_node,
            is_leader_rx,
            crate::kubelet::file_blocking::test_file_process_executor(),
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
            crate::kubelet::file_blocking::test_file_process_executor(),
        )
    }

    #[cfg(test)]
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
            crate::kubelet::file_blocking::test_file_process_executor(),
        )
    }

    #[cfg(test)]
    pub(crate) fn new_with_node_lease_tracker_and_passive_reads(
        db: DatastoreHandle,
        passive_reads: crate::datastore::selector::PassiveReadPorts,
        authoring_node: String,
        node_lease_tracker: Arc<klights_controllers::node_lease::NodeLeaseTracker>,
        is_leader_rx: watch::Receiver<bool>,
    ) -> Self {
        Self::new_with_node_lease_tracker_and_containerd_namespace_and_file_process_with_reads(
            db,
            passive_reads,
            authoring_node,
            std::env::var("KLIGHTS_CONTAINERD_NAMESPACE").unwrap_or_else(|_| "klights".to_string()),
            node_lease_tracker,
            is_leader_rx,
            crate::kubelet::file_blocking::test_file_process_executor(),
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

    #[cfg(test)]
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
            crate::datastore::test_support::unused_fail_closed_passive_read_ports(),
            authoring_node,
            containerd_namespace,
            node_lease_tracker,
            is_leader_rx,
            file_process,
        )
    }

    #[cfg(test)]
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
        let pod_store = Arc::new(crate::pod_repository_composition::new_pod_store(db.clone()));
        let crypto = file_process.crypto_executor();
        let outbox_side_effects = Arc::new(RootOutboxSideEffectState::new(db.clone()));
        #[cfg(test)]
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
                crate::outbox_payload_codec_adapter::new_codec(),
                authoring_node.clone(),
            ));
            LocalApiTestServices {
                resource_command,
                outbox_delivery: committed_outbox,
            }
        };
        Self {
            #[cfg(test)]
            test_services,
            db: db.clone(),
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
        Box::pin(async move {
            let leader_fresh =
                request.consistency() == klights_leader_api::ResourceQueryConsistency::LeaderFresh;
            let mut leadership_rx = self.is_leader_rx.clone();
            let sampled_is_leader = *leadership_rx.borrow_and_update();
            if leader_fresh && !sampled_is_leader {
                return Err(klights_leader_api::ResourceQueryError::retryable(
                    "leader-fresh resource query reached a non-leader local client",
                ));
            }
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
            if leader_fresh && leadership_rx.has_changed().unwrap_or(true) {
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
            let leader_fresh =
                request.consistency() == klights_leader_api::ResourceQueryConsistency::LeaderFresh;
            let mut leadership_rx = self.is_leader_rx.clone();
            let sampled_is_leader = *leadership_rx.borrow_and_update();
            if leader_fresh && !sampled_is_leader {
                return Err(klights_leader_api::ResourceQueryError::retryable(
                    "leader-fresh resource query reached a non-leader local client",
                ));
            }
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
            if leader_fresh && leadership_rx.has_changed().unwrap_or(true) {
                return Err(klights_leader_api::ResourceQueryError::retryable(
                    "leadership changed during local leader-fresh resource query",
                ));
            }
            query_list_result(list)
        })
    }
}

#[cfg(test)]
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

#[cfg(test)]
impl LeaderOutboxDelivery for LocalApiClient {
    fn deliver_outbox(&self, request: OutboxDeliveryRequest) -> OutboxDeliveryFuture<'_> {
        self.test_services.outbox_delivery.deliver_outbox(request)
    }
}

#[cfg(test)]
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
            if !crate::node_routing_metadata::stamp_from_store(
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
mod inner_gate_tests {
    //! T6 step 1: `LocalApiClient` inner write gate.
    //!
    //! Every mutation method must consult `is_leader_rx` and refuse with
    //! `WriteRejection::FollowerWrite` (or the OutboxApplyError equivalent)
    //! when this node is not the elected raft leader. Reads stay allowed.
    //! Promotion is a watch flip — the same instance starts accepting
    //! writes the moment the receiver observes `true`.

    use super::*;
    use crate::datastore::ReplicatedCreateOptions;
    use crate::datastore::ResourcePreconditions;
    use crate::datastore::{DatastoreBackend, ResourceListQuery};
    use crate::outbox_test_support::OutboxPayload;
    use futures::StreamExt as _;
    use klights_cluster_core::command::StorageCommand;
    use klights_kubelet::node_outbox::payload::OutboxOperation;
    use klights_leader_api::OutboxDeliveryError as OutboxApplyError;
    use klights_leader_api::{
        LeaderResourceCommand, ResourceCommandError, ResourceCommandRequest, ResourceCommandResult,
        ResourceQueryError, WatchEventType,
    };
    use klights_types::ResourceKey;

    fn pod_status_payload() -> bytes::Bytes {
        let command = StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web".to_string(),
            status: serde_json::json!({"phase": "Running"}),
            expected_rv: None,
            preconditions: ResourcePreconditions {
                uid: Some("uid-1".to_string()),
                resource_version: None,
            },
            observed_status_stamp: None,
        };
        bytes::Bytes::from(
            OutboxPayload::from_command(command)
                .encode_protobuf()
                .expect("encode pod status payload"),
        )
    }

    async fn make_pod(db: &crate::datastore::sqlite::Datastore) {
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "web",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"namespace": "default", "name": "web", "uid": "uid-1"},
                "spec": {"containers": [{"name": "app", "image": "nginx"}]}
            }),
        )
        .await
        .expect("create pod");
    }

    #[tokio::test]
    async fn local_protobuf_pod_status_reconciles_json_endpoint_tables() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        let service = db
            .create_resource(
                "v1",
                "Service",
                Some("default"),
                "web",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Service",
                    "metadata": {"namespace": "default", "name": "web", "uid": "service-uid"},
                    "spec": {"selector": {"app": "web"}, "ports": [{"port": 80, "targetPort": 8080}]}
                }),
            )
            .await
            .unwrap();
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "web",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"namespace": "default", "name": "web", "uid": "uid-1", "labels": {"app": "web"}},
                "spec": {"nodeName": "worker-1", "containers": [{"name": "app", "image": "nginx", "ports": [{"containerPort": 8080}]}]},
                "status": {"phase": "Pending"}
            }),
        )
        .await
        .unwrap();
        let client = LocalApiClient::new(
            db.clone(),
            "worker-1".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        );
        let dispatcher =
            Arc::new(crate::controller_test_support::default_queue_only_dispatcher_for_test());
        client.set_controller_dispatcher(dispatcher.clone());
        let command = StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web".to_string(),
            status: serde_json::json!({
                "phase": "Running",
                "podIP": "10.42.0.8",
                "podIPs": [{"ip": "10.42.0.8"}],
                "conditions": [{"type": "Ready", "status": "True"}]
            }),
            expected_rv: None,
            preconditions: ResourcePreconditions::uid("uid-1"),
            observed_status_stamp: None,
        };
        let payload = bytes::Bytes::from(
            OutboxPayload::from_command(command)
                .encode_protobuf()
                .expect("encode local Pod status protobuf"),
        );
        client
            .deliver_test_outbox(
                "local-pod-ready",
                OutboxOperation::PodStatus,
                payload,
                "worker-1",
                1,
                1,
            )
            .await
            .expect("apply local Pod status");

        let keys = dispatcher.queued_reconcile_keys_for_test().await;
        assert_eq!(
            keys.iter()
                .filter(|key| key.kind() == "Service" && key.name() == "web")
                .count(),
            1
        );
        let pod_store = crate::kubelet::pod_repository::store::PodStore::new(db.clone());
        klights_controllers::endpoints::reconcile_service_endpoints_batch(
            db.as_ref(),
            &pod_store,
            klights_controllers::endpoints::ServiceEndpointBatchReconcileRequest {
                service_name: "web",
                service_uid: &service.uid,
                namespace: "default",
                selector: service.data.pointer("/spec/selector"),
                service_ports: service.data.pointer("/spec/ports"),
                publish_not_ready: false,
            },
        )
        .await
        .unwrap();
        let endpoints = db
            .get_resource("v1", "Endpoints", Some("default"), "web")
            .await
            .unwrap()
            .expect("JSON Endpoints row");
        let slice = db
            .get_resource(
                "discovery.k8s.io/v1",
                "EndpointSlice",
                Some("default"),
                "web-klights",
            )
            .await
            .unwrap()
            .expect("JSON EndpointSlice row");
        assert_eq!(
            endpoints.data.pointer("/subsets/0/addresses/0/ip"),
            Some(&serde_json::json!("10.42.0.8"))
        );
        assert_eq!(
            slice.data.pointer("/endpoints/0/conditions/ready"),
            Some(&serde_json::json!(true))
        );
    }

    /// Mutation gate: every `LeaderApiClient` mutation refuses when
    /// `is_leader_rx=false`. Asserts the gate fires before any datastore
    /// work happens.
    #[tokio::test]
    async fn local_api_client_refuses_apply_outbox_when_not_leader() {
        let db = crate::datastore::test_support::in_memory().await;
        make_pod(&db).await;
        let (_tx, rx) = watch::channel(false);
        let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);

        let err = client
            .deliver_test_outbox(
                "idem-1",
                OutboxOperation::PodStatus,
                pod_status_payload(),
                "client",
                1,
                1,
            )
            .await
            .expect_err("non-leader apply_outbox must be rejected");
        assert_eq!(err, OutboxApplyError::NotLeader);
        assert!(err.is_retryable());
    }

    #[tokio::test]
    async fn outbox_terminal_decision_local_invalid_and_malformed_rows_consume_in_order() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        db.create_resource(
            "v1",
            "Node",
            None,
            "node-a",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "node-a", "uid": "node-uid-a"},
                "status": {"conditions": []}
            }),
        )
        .await
        .expect("create local Node");
        let client = LocalApiClient::new(
            db.clone(),
            "node-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        );
        let command = StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Node".to_string(),
            namespace: None,
            name: "node-a".to_string(),
            status: serde_json::json!({"conditions": []}),
            expected_rv: Some(7),
            preconditions: ResourcePreconditions {
                uid: Some("node-uid-a".to_string()),
                resource_version: Some(7),
            },
            observed_status_stamp: None,
        };
        let payload = bytes::Bytes::from(
            OutboxPayload::from_command(command)
                .encode_protobuf()
                .expect("encode invalid worker Node status"),
        );

        let error = client
            .deliver_test_outbox(
                "invalid-node-status-rv",
                OutboxOperation::NodeStatus,
                payload,
                "client",
                1,
                1,
            )
            .await
            .expect_err("local focused delivery must enforce NodeSelfStatusRequest validation");
        assert!(matches!(
            error,
            klights_leader_api::OutboxDeliveryError::InvalidRequest {
                field: "status.resource_version",
                ..
            }
        ));
        assert_eq!(
            db.list_outbox_stream_watermarks().await.unwrap()[0].stream_seq,
            1,
            "local authorization rejection must durably consume sequence one"
        );

        let valid_status = || {
            bytes::Bytes::from(
                OutboxPayload::from_command(StorageCommand::UpdateStatus {
                    api_version: "v1".to_string(),
                    kind: "Node".to_string(),
                    namespace: None,
                    name: "node-a".to_string(),
                    status: serde_json::json!({"conditions": []}),
                    expected_rv: None,
                    preconditions: ResourcePreconditions::uid("node-uid-a"),
                    observed_status_stamp: None,
                })
                .encode_protobuf()
                .expect("encode valid local Node status"),
            )
        };
        client
            .deliver_test_outbox(
                "valid-node-status-after-invalid",
                OutboxOperation::NodeStatus,
                valid_status(),
                "client",
                1,
                2,
            )
            .await
            .expect("sequence two applies after terminal authorization decision");

        let malformed = client
            .deliver_test_outbox(
                "malformed-node-status",
                OutboxOperation::NodeStatus,
                bytes::Bytes::from_static(&[0xff, 0x00, 0x81]),
                "client",
                1,
                3,
            )
            .await
            .expect_err("malformed delivery stays fail-closed");
        assert!(malformed.is_terminal());
        assert_eq!(
            db.list_outbox_stream_watermarks().await.unwrap()[0].stream_seq,
            3,
            "malformed sequence must receive a durable terminal decision"
        );
        client
            .deliver_test_outbox(
                "valid-node-status-after-malformed",
                OutboxOperation::NodeStatus,
                valid_status(),
                "client",
                1,
                4,
            )
            .await
            .expect("sequence four applies after malformed terminal decision");
    }

    #[tokio::test]
    async fn exact_codec_rejection_precedes_decode_ledger_and_watermark() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        let client = LocalApiClient::new(
            db.clone(),
            "node-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        );
        let initial_rv = db
            .get_current_resource_version()
            .await
            .expect("read initial RV");

        for advertised in [
            klights_cluster_core::COMMAND_CODEC_VERSION - 1,
            klights_cluster_core::COMMAND_CODEC_VERSION + 1,
        ] {
            let idempotency_key = format!("incompatible-codec-{advertised}");
            let error = klights_leader_api::LeaderOutboxDelivery::deliver_outbox(
                &client,
                klights_leader_api::OutboxDeliveryRequest::try_new_versioned(
                    advertised,
                    idempotency_key.clone(),
                    klights_leader_api::OutboxDeliveryOperation::PodMetadata,
                    Arc::<[u8]>::from([0xff, 0x00, 0x81]),
                    "peer-a",
                    71,
                    1,
                )
                .expect("transport preserves the advertised codec"),
            )
            .await
            .expect_err("only exact codec v3 is accepted");
            assert_eq!(
                error,
                klights_leader_api::OutboxDeliveryError::codec_incompatible(
                    advertised,
                    klights_cluster_core::COMMAND_CODEC_VERSION,
                )
            );
            assert!(error.is_retryable());
            assert!(
                db.get_applied_outbox(&idempotency_key)
                    .await
                    .expect("read incompatible ledger")
                    .is_none(),
                "exact-version rejection must precede ledger insertion"
            );
            assert!(
                db.list_outbox_stream_watermarks()
                    .await
                    .expect("read incompatible watermarks")
                    .is_empty(),
                "exact-version rejection must precede watermark advancement"
            );
            assert_eq!(
                db.get_current_resource_version()
                    .await
                    .expect("read RV after rejection"),
                initial_rv,
                "rejected opaque bytes must not mutate cluster state"
            );
        }
    }

    #[tokio::test]
    async fn local_resource_command_is_leader_gated_before_datastore_mutation() {
        let db = crate::datastore::test_support::in_memory().await;
        let (_tx, rx) = watch::channel(false);
        let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);
        let request = ResourceCommandRequest::try_new(StorageCommand::CreateResource {
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
        .expect("valid command");

        let error = LeaderResourceCommand::submit_resource_command(&client, request)
            .await
            .expect_err("a follower must reject resource commands");
        assert_eq!(error, ResourceCommandError::NotLeader);
        assert!(
            client
                .db
                .get_resource("v1", "ConfigMap", Some("default"), "settings")
                .await
                .expect("read after rejection")
                .is_none()
        );
    }

    #[tokio::test]
    async fn local_resource_command_returns_the_created_resource() {
        let db = crate::datastore::test_support::in_memory().await;
        let (_tx, rx) = watch::channel(true);
        let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);
        let request = ResourceCommandRequest::try_new(StorageCommand::CreateResource {
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
        .expect("valid command");

        let result = LeaderResourceCommand::submit_resource_command(&client, request)
            .await
            .expect("leader command");
        assert!(
            matches!(result, ResourceCommandResult::Resource(resource) if resource.name == "settings")
        );
    }

    #[tokio::test]
    async fn local_resource_command_preserves_duplicate_create_as_already_exists() {
        let db = crate::datastore::test_support::in_memory().await;
        let (_tx, rx) = watch::channel(true);
        let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);
        let command = StorageCommand::CreateResource {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "settings".to_string(),
            data: serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"namespace": "default", "name": "settings"}
            }),
        };
        LeaderResourceCommand::submit_resource_command(
            &client,
            ResourceCommandRequest::try_new(command.clone()).expect("valid command"),
        )
        .await
        .expect("first create");
        let error = LeaderResourceCommand::submit_resource_command(
            &client,
            ResourceCommandRequest::try_new(command).expect("valid command"),
        )
        .await
        .expect_err("duplicate create must be rejected");
        assert!(matches!(error, ResourceCommandError::AlreadyExists { .. }));
    }

    /// `allocate_node_subnet` writes cluster state and must be gated.
    #[tokio::test]
    async fn local_api_client_refuses_allocate_node_subnet_when_not_leader() {
        let db = crate::datastore::test_support::in_memory().await;
        let (_tx, rx) = watch::channel(false);
        let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);

        let request = klights_leader_api::NodeSubnetAllocationRequest::try_new(
            "node-a",
            "10.50.0.0/16",
            "10.99.0.10",
        )
        .expect("valid allocation request");
        let err =
            klights_leader_api::LeaderNodeSubnetAllocation::allocate_node_subnet(&client, request)
                .await
                .expect_err("non-leader subnet allocation must be rejected");
        assert!(
            matches!(
                err,
                klights_leader_api::NodeSubnetAllocationError::NotLeader
            ),
            "expected typed NotLeader, got: {err}"
        );
    }

    #[tokio::test]
    async fn local_api_client_maps_subnet_exhaustion_to_typed_error() {
        let db = crate::datastore::test_support::in_memory().await;
        let (_tx, rx) = watch::channel(true);
        let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);

        for node_name in ["node-a", "node-b"] {
            let request = klights_leader_api::NodeSubnetAllocationRequest::try_new(
                node_name,
                "10.50.0.0/24",
                "10.99.0.10",
            )
            .expect("valid allocation request");
            let result = klights_leader_api::LeaderNodeSubnetAllocation::allocate_node_subnet(
                &client, request,
            )
            .await;
            if node_name == "node-a" {
                result.expect("the only /24 must be allocated");
            } else {
                assert!(
                    matches!(
                        result,
                        Err(klights_leader_api::NodeSubnetAllocationError::Exhausted { .. })
                    ),
                    "the second allocation must report typed exhaustion, got {result:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn local_api_client_refuses_network_topology_query_when_not_leader() {
        let db = crate::datastore::test_support::in_memory().await;
        let (_tx, rx) = watch::channel(false);
        let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);
        let request =
            klights_leader_api::NodeSubnetQuery::try_new("node-a").expect("valid topology query");

        let err = klights_leader_api::LeaderNetworkTopologyQuery::get_node_subnet(&client, request)
            .await
            .expect_err("non-leader topology query must fail closed");
        assert!(matches!(
            err,
            klights_leader_api::NetworkTopologyError::NotLeader
        ));
    }

    /// Cached reads may use follower-applied state, but LeaderFresh must not.
    #[tokio::test]
    async fn local_api_client_allows_reads_when_not_leader() {
        let db = crate::datastore::test_support::in_memory().await;
        make_pod(&db).await;
        let (_tx, rx) = watch::channel(false);
        let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);

        let key = ResourceKey {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web".to_string(),
        };
        assert!(
            client
                .get_resource(
                    ResourceGetRequest::try_new(key.clone(), ResourceQueryConsistency::Cached)
                        .expect("valid Pod request"),
                )
                .await
                .expect("read allowed")
                .is_some(),
            "non-leader get_resource must succeed"
        );
        assert!(
            client
                .get_resource(
                    pod_get_request("default", "web", ResourceQueryConsistency::Cached)
                        .expect("valid Pod request"),
                )
                .await
                .expect("read allowed")
                .is_some(),
            "non-leader get_pod must succeed"
        );
        let listed = client
            .list_resources(
                ResourceListRequest::try_new(
                    "v1",
                    "Pod",
                    Some("default".to_string()),
                    None,
                    None,
                    None,
                    None,
                    ResourceQueryConsistency::Cached,
                )
                .expect("valid Pod list request"),
            )
            .await
            .expect("list allowed");
        assert_eq!(
            listed.items().len(),
            1,
            "non-leader list_resources must succeed"
        );
        assert!(matches!(
            client
                .get_resource(
                    ResourceGetRequest::try_new(key, ResourceQueryConsistency::LeaderFresh)
                        .expect("valid fresh Pod request"),
                )
                .await,
            Err(ResourceQueryError::Retryable { .. })
        ));
        assert!(matches!(
            client
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
                    .expect("valid fresh Pod list request"),
                )
                .await,
            Err(ResourceQueryError::Retryable { .. })
        ));
    }

    #[tokio::test]
    async fn local_selector_watch_synthesizes_deleted_when_pod_leaves_node() {
        let concrete_db = crate::datastore::test_support::in_memory().await;
        let passive_reads = crate::datastore::test_support::sqlite_passive_read_ports(&concrete_db);
        let db: DatastoreHandle = Arc::new(concrete_db);
        let pod = db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "moving",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"namespace": "default", "name": "moving", "uid": "uid-moving"},
                    "spec": {"nodeName": "node-a", "containers": [{"name": "app", "image": "pause"}]}
                }),
            )
            .await
            .unwrap();
        let (_tx, rx) = watch::channel(true);
        let client =
            LocalApiClient::new_with_passive_reads(db.clone(), passive_reads, "node-a".into(), rx);
        let mut stream = client
            .watch_resources(
                WatchRequest::try_new(
                    "v1",
                    "Pod",
                    None,
                    None,
                    Some("spec.nodeName=node-a".to_string()),
                    None,
                    None,
                )
                .expect("valid Pod watch"),
            )
            .await
            .unwrap();

        let mut moved = (*pod.data).clone();
        moved["spec"]["nodeName"] = serde_json::Value::String("node-b".to_string());
        db.update_resource(
            "v1",
            "Pod",
            Some("default"),
            "moving",
            moved,
            pod.resource_version,
        )
        .await
        .unwrap();

        let event = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
            .await
            .expect("leave transition should arrive")
            .expect("stream should remain open")
            .expect("event should decode");
        assert_eq!(event.event_type(), WatchEventType::Deleted);
        assert_eq!(event.resource().data["metadata"]["name"], "moving");
    }

    async fn register_watch_scope_crd(
        db: &DatastoreHandle,
        group: &str,
        kind: &str,
        plural: &str,
        namespaced: bool,
    ) {
        db.create_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            &format!("{plural}.{group}"),
            serde_json::json!({
                "apiVersion": "apiextensions.k8s.io/v1",
                "kind": "CustomResourceDefinition",
                "metadata": {"name": format!("{plural}.{group}")},
                "spec": {
                    "group": group,
                    "scope": if namespaced { "Namespaced" } else { "Cluster" },
                    "names": {"kind": kind, "plural": plural, "singular": plural},
                    "versions": [{"name": "v1", "served": true, "storage": true}]
                }
            }),
        )
        .await
        .expect("register CRD scope metadata");
    }

    #[tokio::test]
    async fn local_positioned_watch_resolves_namespaced_crd_for_all_namespaces_delivery() {
        let concrete_db = crate::datastore::test_support::in_memory().await;
        let passive_reads = crate::datastore::test_support::sqlite_passive_read_ports(&concrete_db);
        let db: DatastoreHandle = Arc::new(concrete_db);
        register_watch_scope_crd(&db, "example.com", "Widget", "widgets", true).await;
        let (_tx, rx) = watch::channel(true);
        let client =
            LocalApiClient::new_with_passive_reads(db.clone(), passive_reads, "node-a".into(), rx);
        let mut stream = client
            .watch_resources(
                WatchRequest::try_new("example.com/v1", "Widget", None, None, None, None, None)
                    .expect("valid namespaced CRD watch"),
            )
            .await
            .expect("namespaced CRD watch opens");

        db.create_resource(
            "example.com/v1",
            "Widget",
            Some("default"),
            "namespaced",
            serde_json::json!({
                "apiVersion": "example.com/v1",
                "kind": "Widget",
                "metadata": {"namespace": "default", "name": "namespaced"}
            }),
        )
        .await
        .expect("create namespaced CR");

        let event = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
            .await
            .expect("all-namespaces CRD watch must receive namespaced events")
            .expect("watch remains open")
            .expect("event is valid");
        assert_eq!(event.resource().namespace.as_deref(), Some("default"));
    }

    #[tokio::test]
    async fn local_positioned_watch_resolves_cluster_scoped_crd_delivery() {
        let concrete_db = crate::datastore::test_support::in_memory().await;
        let passive_reads = crate::datastore::test_support::sqlite_passive_read_ports(&concrete_db);
        let db: DatastoreHandle = Arc::new(concrete_db);
        register_watch_scope_crd(
            &db,
            "cluster.example.com",
            "ClusterWidget",
            "clusterwidgets",
            false,
        )
        .await;
        let (_tx, rx) = watch::channel(true);
        let client =
            LocalApiClient::new_with_passive_reads(db.clone(), passive_reads, "node-a".into(), rx);
        let mut stream = client
            .watch_resources(
                WatchRequest::try_new(
                    "cluster.example.com/v1",
                    "ClusterWidget",
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .expect("valid cluster CRD watch"),
            )
            .await
            .expect("cluster CRD watch opens");

        db.create_resource(
            "cluster.example.com/v1",
            "ClusterWidget",
            None,
            "clustered",
            serde_json::json!({
                "apiVersion": "cluster.example.com/v1",
                "kind": "ClusterWidget",
                "metadata": {"name": "clustered"}
            }),
        )
        .await
        .expect("create cluster CR");

        let event = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
            .await
            .expect("cluster CRD watch must receive cluster events")
            .expect("watch remains open")
            .expect("event is valid");
        assert_eq!(event.resource().namespace, None);
    }

    #[tokio::test]
    async fn exact_position_selector_watch_replays_late_lower_rv_leave_as_deleted() {
        let db = crate::datastore::test_support::in_memory().await;
        let selected = serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "namespace": "default",
                "name": "selected",
                "uid": "uid-selected",
                "labels": {"track": "yes"}
            }
        });
        db.apply_replicated_create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "selected",
            selected.clone(),
            ReplicatedCreateOptions {
                resource_version: 40,
                meta_uid: Some("uid-selected".into()),
            },
        )
        .await
        .unwrap();
        db.apply_replicated_create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "rv-high-water",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "namespace": "default",
                    "name": "rv-high-water",
                    "uid": "uid-high-water"
                }
            }),
            ReplicatedCreateOptions {
                resource_version: 50,
                meta_uid: Some("uid-high-water".into()),
            },
        )
        .await
        .unwrap();

        let list = db
            .list_resources(
                "v1",
                "ConfigMap",
                Some("default"),
                ResourceListQuery::new(Some("track=yes"), None, None, None),
            )
            .await
            .unwrap();
        assert_eq!(list.items.len(), 1);
        assert_eq!(list.resource_version, 50);
        let list_position = list
            .watch_replay_position
            .expect("LIST must carry its exact durable position");

        let mut nonmatching = selected;
        nonmatching["metadata"]["labels"]["track"] = serde_json::json!("no");
        db.apply_replicated_create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "selected",
            nonmatching,
            ReplicatedCreateOptions {
                resource_version: 45,
                meta_uid: Some("uid-selected".into()),
            },
        )
        .await
        .unwrap();

        let passive_reads = crate::datastore::test_support::sqlite_passive_read_ports(&db);
        let db: DatastoreHandle = Arc::new(db);
        let (_tx, rx) = watch::channel(true);
        let client = LocalApiClient::new_with_passive_reads(db, passive_reads, "node-a".into(), rx);
        let mut stream = client
            .watch_resources(
                WatchRequest::try_new(
                    "v1",
                    "ConfigMap",
                    Some("default".into()),
                    Some("track=yes".into()),
                    None,
                    Some(50),
                    Some(list_position),
                )
                .expect("valid positioned selector watch"),
            )
            .await
            .unwrap();

        let event = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
            .await
            .expect("retained lower-RV leave must replay")
            .expect("watch remains open")
            .expect("event decodes");
        assert_eq!(event.event_type(), WatchEventType::Deleted);
        assert_eq!(event.resource().data["metadata"]["labels"]["track"], "yes");
        assert!(
            event
                .resume_position()
                .is_some_and(|position| position.event_id > list_position.event_id),
            "resume cursor must advance through the lower-RV mutation"
        );
    }

    #[tokio::test]
    async fn local_omitted_rv_watch_starts_after_existing_objects() {
        let concrete_db = crate::datastore::test_support::in_memory().await;
        let passive_reads = crate::datastore::test_support::sqlite_passive_read_ports(&concrete_db);
        let db: DatastoreHandle = Arc::new(concrete_db);
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "existing",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"namespace": "default", "name": "existing"}
            }),
        )
        .await
        .unwrap();
        let (_tx, rx) = watch::channel(true);
        let client =
            LocalApiClient::new_with_passive_reads(db.clone(), passive_reads, "node-a".into(), rx);
        let mut stream = client
            .watch_resources(
                WatchRequest::try_new(
                    "v1",
                    "ConfigMap",
                    Some("default".to_string()),
                    None,
                    None,
                    None,
                    None,
                )
                .expect("valid ConfigMap watch"),
            )
            .await
            .unwrap();
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "fresh",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"namespace": "default", "name": "fresh"}
            }),
        )
        .await
        .unwrap();

        let event = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
            .await
            .expect("post-establishment event should arrive")
            .expect("stream should remain open")
            .expect("event should decode");
        assert_eq!(event.resource().data["metadata"]["name"], "fresh");
    }

    /// Promotion is a watch flip. The same client instance must start
    /// accepting writes the moment is_leader_rx observes `true`. No
    /// re-construction or rewiring.
    #[tokio::test]
    async fn local_api_client_flips_to_accepting_writes_on_promotion() {
        let db = crate::datastore::test_support::in_memory().await;
        make_pod(&db).await;
        let (tx, rx) = watch::channel(false);
        let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);
        client.set_controller_dispatcher(Arc::new(
            crate::controller_test_support::default_queue_only_dispatcher_for_test(),
        ));

        // Pre-promotion: write refused.
        let pre = client
            .deliver_test_outbox(
                "idem-2",
                OutboxOperation::PodStatus,
                pod_status_payload(),
                "client",
                1,
                1,
            )
            .await;
        assert!(pre.is_err(), "pre-promotion write must be refused");

        // Promotion: flip the watch.
        tx.send(true).expect("send promotion signal");

        // Post-promotion: same client instance, write succeeds.
        let post = client
            .deliver_test_outbox(
                "idem-3",
                OutboxOperation::PodStatus,
                pod_status_payload(),
                "client",
                1,
                1,
            )
            .await;
        assert!(
            post.is_ok(),
            "post-promotion write must succeed on the same instance, got: {post:?}"
        );
    }

    /// Demotion is the symmetric flip. A live leader that loses
    /// leadership (term lost, voluntary step-down) must stop accepting
    /// writes on the next call.
    #[tokio::test]
    async fn local_api_client_revokes_writes_on_demotion() {
        let db = crate::datastore::test_support::in_memory().await;
        make_pod(&db).await;
        let (tx, rx) = watch::channel(true);
        let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);
        client.set_controller_dispatcher(Arc::new(
            crate::controller_test_support::default_queue_only_dispatcher_for_test(),
        ));

        // Pre-demotion: write succeeds.
        let pre = client
            .deliver_test_outbox(
                "idem-4",
                OutboxOperation::PodStatus,
                pod_status_payload(),
                "client",
                1,
                1,
            )
            .await;
        assert!(pre.is_ok(), "pre-demotion write must succeed");

        // Demotion: flip the watch to false.
        tx.send(false).expect("send demotion signal");

        // Post-demotion: same client instance, write refused.
        let post = client
            .deliver_test_outbox(
                "idem-5",
                OutboxOperation::PodStatus,
                pod_status_payload(),
                "client",
                1,
                1,
            )
            .await
            .expect_err("post-demotion write must be refused");
        assert_eq!(post, OutboxApplyError::NotLeader);
        assert!(post.is_retryable());
    }

    /// The focused delivery port uses the same leader gate as every local
    /// mutation and must surface a retryable result after demotion.
    #[tokio::test]
    async fn outbox_apply_client_respects_leader_gate() {
        let db = crate::datastore::test_support::in_memory().await;
        make_pod(&db).await;
        let (_tx, rx) = watch::channel(false);
        let client = LocalApiClient::new(Arc::new(db), "node-a".to_string(), rx);
        let trait_obj: &dyn klights_leader_api::LeaderOutboxDelivery = &client;

        let err = trait_obj
            .deliver_outbox(
                klights_leader_api::OutboxDeliveryRequest::try_new(
                    "idem-6",
                    klights_leader_api::OutboxDeliveryOperation::PodStatus,
                    Arc::<[u8]>::from(pod_status_payload().to_vec()),
                    "client",
                    1,
                    1,
                )
                .expect("valid delivery request"),
            )
            .await
            .expect_err("non-leader outbox apply must be refused");
        assert_eq!(err, OutboxApplyError::NotLeader);
        assert!(
            err.is_retryable(),
            "outbox dispatcher must re-queue typed NotLeader"
        );
    }

    /// Compile-time pin: the `is_leader_rx` field is a required
    /// `watch::Receiver<bool>` and the constructor signature demands it.
    /// If a future refactor moves the field behind an `Option<>` or
    /// adds a default-true fallback, this test breaks at compile time
    /// (it asserts the exact constructor arity and parameter type).
    #[test]
    fn local_api_client_constructor_requires_is_leader_rx() {
        // Force the compiler to verify the constructor signature. This
        // closure can only be constructed if `LocalApiClient::new` has
        // exactly the (DatastoreHandle, String, watch::Receiver<bool>)
        // shape — any change to the watch arg breaks the binding.
        let _check: fn(DatastoreHandle, String, watch::Receiver<bool>) -> LocalApiClient =
            LocalApiClient::new;
        let _check_with_tracker: fn(
            DatastoreHandle,
            String,
            Arc<klights_controllers::node_lease::NodeLeaseTracker>,
            watch::Receiver<bool>,
        ) -> LocalApiClient = LocalApiClient::new_with_node_lease_tracker;
    }

    /// `always_leader_watch()` returns a receiver permanently held at
    /// `true`. Required for tests and for boot paths where leadership
    /// has already been established (e.g. cp1 after bootstrap_single_voter
    /// runs synchronously, before any real watch wiring exists).
    #[test]
    fn always_leader_watch_observes_true_forever() {
        let rx = always_leader_watch();
        assert!(*rx.borrow(), "always_leader_watch must start true");
        // The internal sender is leaked — drop the rx clone we have and
        // recreate; both copies must still observe true.
        drop(rx);
        let rx2 = always_leader_watch();
        assert!(*rx2.borrow(), "always_leader_watch must stay true");
    }

    #[tokio::test]
    async fn local_projected_token_capability_remains_self_node_scoped() {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        let client = LocalApiClient::new(db, "leader-cp1".to_string(), always_leader_watch());
        let request = ProjectedServiceAccountTokenRequest::try_new(
            "default",
            "default",
            vec!["api".to_string()],
            3_600,
            "client",
            "pod-uid",
            "mn-worker",
            None,
        )
        .unwrap();

        let error = LeaderProjectedServiceAccountToken::issue_projected_service_account_token(
            &client, request,
        )
        .await
        .expect_err("local kubelet capability must not mint for another node");
        assert_eq!(error, ProjectedServiceAccountTokenError::Unauthorized);
    }

    #[test]
    fn projected_token_leadership_fence_rejects_demotion_and_aba() {
        for transitions in [&[false][..], &[false, true][..]] {
            let (tx, rx) = watch::channel(true);
            let fence = LeadershipGenerationFence::sample(rx).expect("initial leader");
            for state in transitions {
                tx.send(*state).unwrap();
            }
            assert_eq!(
                fence.ensure_unchanged(),
                Err(ProjectedServiceAccountTokenError::NotLeader),
                "every leadership generation change must invalidate the operation: {transitions:?}"
            );
        }
    }

    #[test]
    fn projected_token_signing_fence_blocks_demotion_until_signing_finishes() {
        let (leadership_tx, leadership_rx) = watch::channel(true);
        let _leadership_keepalive = leadership_rx.clone();
        let fence = LeadershipGenerationFence::sample(leadership_rx).expect("initial leader");
        let (signing_entered_tx, signing_entered_rx) = std::sync::mpsc::channel();
        let (release_signing_tx, release_signing_rx) = std::sync::mpsc::channel();
        let signing = std::thread::spawn(move || {
            fence.sign_if_unchanged(|| {
                signing_entered_tx.send(()).unwrap();
                release_signing_rx.recv().unwrap();
                "signed"
            })
        });
        signing_entered_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("signing operation did not enter its fenced section");

        let (transition_started_tx, transition_started_rx) = std::sync::mpsc::channel();
        let (transition_done_tx, transition_done_rx) = std::sync::mpsc::channel();
        let transition = std::thread::spawn(move || {
            transition_started_tx.send(()).unwrap();
            leadership_tx.send(false).unwrap();
            leadership_tx.send(true).unwrap();
            transition_done_tx.send(()).unwrap();
        });
        transition_started_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("leadership transition did not start");
        assert!(
            transition_done_rx
                .recv_timeout(std::time::Duration::from_millis(100))
                .is_err(),
            "demotion must not linearize while synchronous signing holds the fence"
        );

        release_signing_tx.send(()).unwrap();
        assert_eq!(signing.join().unwrap(), Ok("signed"));
        transition_done_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("leadership transition did not finish after signing released the fence");
        transition.join().unwrap();
    }

    #[test]
    fn projected_token_leadership_fence_accepts_only_stable_generation() {
        let (_tx, rx) = watch::channel(true);
        let fence = LeadershipGenerationFence::sample(rx).expect("initial leader");
        assert_eq!(fence.ensure_unchanged(), Ok(()));

        let (_tx, rx) = watch::channel(false);
        assert!(matches!(
            LeadershipGenerationFence::sample(rx),
            Err(ProjectedServiceAccountTokenError::NotLeader)
        ));
    }

    #[tokio::test]
    async fn projected_token_full_issuance_rejects_demotion_and_aba_before_signing() {
        for transitions in [&[false][..], &[false, true][..]] {
            let entered = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            let reader = {
                let entered = entered.clone();
                let release = release.clone();
                Arc::new(move || {
                    let entered = entered.clone();
                    let release = release.clone();
                    let future: std::pin::Pin<
                        Box<dyn std::future::Future<Output = ()> + Send + 'static>,
                    > = Box::pin(async move {
                        entered.notify_one();
                        release.notified().await;
                    });
                    future
                })
            };
            let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
            let (leadership_tx, leadership_rx) = watch::channel(true);
            let data_root = tempfile::tempdir().unwrap();
            let namespace = data_root.path().to_str().unwrap().to_string();
            let signing_key_path = data_root.path().join("etc/service-account-signing.key");
            klights_supervisor::runtime_fs::create_dir_all(signing_key_path.parent().unwrap())
                .unwrap();
            std::fs::write(&signing_key_path, "unused-test-signing-key").unwrap();
            let sign_probe = install_projected_token_issue_test_probe(namespace.clone(), reader);
            let client = Arc::new(
                LocalApiClient::new_with_node_lease_tracker_namespace_signing_key_and_file_process(
                    LocalApiPersistencePorts::new(
                        db.clone(),
                        crate::datastore::test_support::unused_fail_closed_passive_read_ports(),
                        test_watch_signals(&db),
                    ),
                    "node-a".to_string(),
                    namespace,
                    signing_key_path,
                    Arc::new(klights_controllers::node_lease::NodeLeaseTracker::new_at(
                        chrono::Utc::now(),
                    )),
                    leadership_rx,
                    crate::kubelet::file_blocking::test_file_process_executor(),
                ),
            );
            let request = ProjectedServiceAccountTokenRequest::try_new(
                "default",
                "default",
                vec!["api".to_string()],
                3_600,
                "pod-a",
                "pod-uid-a",
                "node-a",
                Some("node-uid-a".to_string()),
            )
            .unwrap();
            let issue = {
                let client = client.clone();
                tokio::spawn(async move {
                    client
                        .issue_projected_token_after_transport_auth(request)
                        .await
                })
            };
            tokio::time::timeout(std::time::Duration::from_secs(2), entered.notified())
                .await
                .expect("signing-key reader did not enter its async boundary");
            for state in transitions {
                leadership_tx.send(*state).unwrap();
            }
            release.notify_one();

            assert_eq!(
                tokio::time::timeout(std::time::Duration::from_secs(2), issue)
                    .await
                    .expect("issuance did not finish after releasing signing-key reader")
                    .unwrap(),
                Err(ProjectedServiceAccountTokenError::NotLeader),
                "full issuance must reject leadership transition {transitions:?}"
            );
            assert_eq!(
                sign_probe.sign_attempts(),
                0,
                "synchronous signing must not be invoked after {transitions:?}"
            );
        }
    }

    async fn seed_projected_token_adapter_resources(db: &dyn DatastoreBackend) {
        for name in ["default", "other"] {
            db.create_resource(
                "v1",
                "ServiceAccount",
                Some("default"),
                name,
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ServiceAccount",
                    "metadata": {"namespace": "default", "name": name, "uid": format!("sa-{name}")}
                }),
            )
            .await
            .unwrap();
        }
        db.create_resource(
            "v1",
            "Node",
            None,
            "node-a",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "node-a", "uid": "node-uid-a"}
            }),
        )
        .await
        .unwrap();
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "pod-a",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"namespace": "default", "name": "pod-a", "uid": "pod-uid-a"},
                "spec": {"serviceAccountName": "default", "nodeName": "node-a"}
            }),
        )
        .await
        .unwrap();
    }

    async fn seeded_authenticated_projected_token_adapter() -> (
        tempfile::TempDir,
        AuthenticatedProjectedTokenIssuer,
        ProjectedServiceAccountTokenRequest,
    ) {
        let db: DatastoreHandle = Arc::new(crate::datastore::test_support::in_memory().await);
        seed_projected_token_adapter_resources(db.as_ref()).await;
        let data_root = tempfile::tempdir().unwrap();
        let namespace = data_root.path().to_str().unwrap().to_string();
        let signing_key_path = data_root.path().join("etc/service-account-signing.key");
        klights_supervisor::runtime_fs::create_dir_all(signing_key_path.parent().unwrap()).unwrap();
        let signing_key =
            klights_auth::test_support::generate_ca_full_at(time::OffsetDateTime::now_utc())
                .unwrap()
                .3;
        std::fs::write(&signing_key_path, &signing_key).unwrap();
        let local = Arc::new(
            LocalApiClient::new_with_node_lease_tracker_namespace_signing_key_and_file_process(
                LocalApiPersistencePorts::new(
                    db.clone(),
                    crate::datastore::test_support::unused_fail_closed_passive_read_ports(),
                    test_watch_signals(&db),
                ),
                "leader-cp1".to_string(),
                namespace,
                signing_key_path,
                Arc::new(klights_controllers::node_lease::NodeLeaseTracker::new_at(
                    chrono::Utc::now(),
                )),
                always_leader_watch(),
                crate::kubelet::file_blocking::test_file_process_executor(),
            ),
        );
        let request = ProjectedServiceAccountTokenRequest::try_new(
            "default",
            "default",
            vec!["api".to_string()],
            3_600,
            "pod-a",
            "pod-uid-a",
            "node-a",
            Some("node-uid-a".to_string()),
        )
        .unwrap();
        (
            data_root,
            AuthenticatedProjectedTokenIssuer::new(local),
            request,
        )
    }

    #[tokio::test]
    async fn authenticated_projected_token_adapter_signs_from_seeded_leader_state() {
        let (_data_root, adapter, request) = seeded_authenticated_projected_token_adapter().await;
        let token = adapter
            .issue_authenticated_projected_service_account_token(request)
            .await
            .expect("privileged post-auth adapter must sign authoritative bound claims");
        assert_eq!(token.token().split('.').count(), 3);
    }

    #[tokio::test]
    async fn authenticated_projected_token_adapter_preserves_binding_mismatches() {
        let cases = [
            (
                "service account",
                "other",
                "pod-uid-a",
                "node-a",
                "node-uid-a",
            ),
            ("Pod UID", "default", "wrong-pod", "node-a", "node-uid-a"),
            ("node name", "default", "pod-uid-a", "node-b", "node-uid-a"),
            ("node UID", "default", "pod-uid-a", "node-a", "wrong-node"),
        ];
        for (label, service_account, pod_uid, node_name, node_uid) in cases {
            let (_data_root, adapter, _) = seeded_authenticated_projected_token_adapter().await;
            let request = ProjectedServiceAccountTokenRequest::try_new(
                "default",
                service_account,
                vec!["api".to_string()],
                3_600,
                "pod-a",
                pod_uid,
                node_name,
                Some(node_uid.to_string()),
            )
            .unwrap();
            assert!(
                matches!(
                    adapter
                        .issue_authenticated_projected_service_account_token(request)
                        .await,
                    Err(ProjectedServiceAccountTokenError::BindingMismatch { .. })
                ),
                "{label} mismatch must remain a binding mismatch"
            );
        }
    }

    /// The test-only focused services preserve the production leader gate:
    /// invoke delivery with watch=false, assert typed refusal, and confirm
    /// cluster.db has no trace of a proposal.
    #[tokio::test]
    async fn delegated_outbox_service_refuses_before_proposal_on_non_leader() {
        let db = crate::datastore::test_support::in_memory().await;
        make_pod(&db).await;
        let pre_rv = db
            .get_resource("v1", "Pod", Some("default"), "web")
            .await
            .expect("read pod")
            .expect("pod exists")
            .resource_version;
        let (_tx, rx) = watch::channel(false);
        let client = LocalApiClient::new(Arc::new(db.clone()), "node-a".to_string(), rx);

        let err = client
            .deliver_test_outbox(
                "n1raft-audit",
                OutboxOperation::PodStatus,
                pod_status_payload(),
                "client",
                1,
                1,
            )
            .await
            .expect_err("non-leader delivery must refuse before proposal");
        assert_eq!(err, OutboxApplyError::NotLeader);
        assert!(err.is_retryable());

        // Confirm proposal never executed: resourceVersion and status remain
        // unchanged from the pre-call state.
        let post = db
            .get_resource("v1", "Pod", Some("default"), "web")
            .await
            .expect("re-read pod")
            .expect("pod still exists");
        assert_eq!(
            post.resource_version, pre_rv,
            "non-leader proposal must not advance cluster.db resourceVersion"
        );
        assert!(
            post.data.pointer("/status/phase").is_none(),
            "non-leader proposal must not write Pod status"
        );
    }
}
