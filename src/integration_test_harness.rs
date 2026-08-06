//! Base-repository-only assembly for full-stack API integration tests.
//!
//! This module exists only behind `integration-test-harness`; normal builds
//! neither compile nor export it.

use std::sync::Arc;

use crate::datastore::DatastoreHandle;
use klights_pod_api::PodSubresourceMutation as _;
use klights_reconcile_api::ControllerDispatcherPort as _;

/// Opaque root datastore capability for base-repository integration fixtures.
///
/// This alias is compiled only with `integration-test-harness`; production and
/// native-service APIs do not expose a datastore surface.
pub type IntegrationDatastoreHandle = DatastoreHandle;
pub type IntegrationWatchEvent = klights_watch::WatchEvent;
pub use klights_cluster_datastore::sqlite::embedded::{
    ResourceMutationPause as IntegrationResourceMutationPause,
    ResourceMutationPauseOperation as IntegrationResourceMutationPauseOperation,
};

pub mod leader_rpc;
pub mod raft;

/// Resolves one admission webhook target through the root's concrete
/// datastore-to-native-service composition. This exists only for base-owned
/// full-adapter integration tests and does not expose the private adapter.
pub async fn resolve_admission_webhook_target_for_integration(
    db: IntegrationDatastoreHandle,
    client_config: &serde_json::Value,
) -> Result<
    k8s_native_service::admission::WebhookTarget,
    k8s_native_service::admission::AdmissionDependencyError,
> {
    use k8s_native_service::admission::WebhookTargetResolver as _;

    let query: Arc<dyn k8s_native_service::admission::AdmissionQuery> =
        crate::bootstrap::composition_adapters::resource_admission_adapter::DatastoreAdmissionQuery::new(db);
    k8s_native_service::admission::ServiceWebhookTargetResolver::new(query)
        .resolve(client_config)
        .await
}

/// Reads Namespace labels through the root admission query composition.
pub async fn admission_namespace_labels_for_integration(
    db: IntegrationDatastoreHandle,
    namespace: &str,
) -> std::collections::BTreeMap<String, String> {
    let query: Arc<dyn k8s_native_service::admission::AdmissionQuery> =
        crate::bootstrap::composition_adapters::resource_admission_adapter::DatastoreAdmissionQuery::new(db);
    k8s_native_service::admission::selectors::get_namespace_labels(query.as_ref(), namespace).await
}

/// Runs one mutating or validating pass through the exact concrete admission
/// dependencies assembled by root. Kept narrow and feature-only for the
/// base-owned cross-adapter integration suite.
pub async fn run_admission_for_integration(
    db: IntegrationDatastoreHandle,
    context: &k8s_native_service::admission::AdmissionRequestContext,
    is_mutating: bool,
) -> anyhow::Result<serde_json::Value> {
    let identity = DeterministicApiIdentity::default();
    let query: Arc<dyn k8s_native_service::admission::AdmissionQuery> =
        crate::bootstrap::composition_adapters::resource_admission_adapter::DatastoreAdmissionQuery::new(db);
    let target_resolver: Arc<dyn k8s_native_service::admission::WebhookTargetResolver> =
        k8s_native_service::admission::ServiceWebhookTargetResolver::new(Arc::clone(&query));
    let webhook_client: Arc<dyn k8s_native_service::admission::AdmissionWebhookClient> =
        k8s_native_service::admission::ReqwestAdmissionWebhookClient::new();
    k8s_native_service::admission::AdmissionEngine::new(
        &identity,
        query.as_ref(),
        target_resolver.as_ref(),
        webhook_client.as_ref(),
    )
    .run_with_context(context, is_mutating)
    .await
}

#[derive(Clone)]
pub struct IntegrationWatchHistoryFailureControl {
    fail: Arc<std::sync::atomic::AtomicBool>,
}

impl IntegrationWatchHistoryFailureControl {
    pub fn fail_subsequent_reads(&self) {
        self.fail.store(true, std::sync::atomic::Ordering::Release);
    }
}

struct ToggleFailingWatchHistory {
    delegate: Arc<dyn klights_cluster_store::DurableWatchHistoryRead>,
    fail: Arc<std::sync::atomic::AtomicBool>,
}

impl klights_cluster_store::DurableWatchHistoryRead for ToggleFailingWatchHistory {
    fn replay_watch_history(
        &self,
        request: klights_cluster_store::WatchHistoryRequest,
    ) -> klights_cluster_store::WatchHistoryFuture<'_, klights_cluster_store::WatchHistoryRead>
    {
        if self.fail.load(std::sync::atomic::Ordering::Acquire) {
            return Box::pin(async {
                Err(
                    klights_cluster_store::WatchHistoryError::PersistenceFailed {
                        message: "injected live replay read failure".to_string(),
                    },
                )
            });
        }
        self.delegate.replay_watch_history(request)
    }

    fn list_replay_floors(
        &self,
    ) -> klights_cluster_store::WatchHistoryFuture<'_, Vec<klights_cluster_store::DurableReplayFloor>>
    {
        self.delegate.list_replay_floors()
    }
}

struct IntegrationBoundTokenSubjects {
    db: IntegrationDatastoreHandle,
}

impl IntegrationBoundTokenSubjects {
    async fn uid(
        &self,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<String>, klights_leader_api::ClusterIdentityError> {
        self.db
            .get_resource("v1", kind, namespace, name)
            .await
            .map(|resource| resource.map(|resource| resource.uid))
            .map_err(|error| {
                klights_leader_api::ClusterIdentityError::dependency_failure(format!(
                    "credential subject lookup failed: {error}"
                ))
            })
    }
}

impl klights_leader_api::LeaderBoundTokenSubjectLookup for IntegrationBoundTokenSubjects {
    fn service_account_uid<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
    ) -> klights_leader_api::ClusterIdentityFuture<'a, Option<String>> {
        Box::pin(async move { self.uid("ServiceAccount", Some(namespace), name).await })
    }

    fn pod_uid<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
    ) -> klights_leader_api::ClusterIdentityFuture<'a, Option<String>> {
        Box::pin(async move { self.uid("Pod", Some(namespace), name).await })
    }

    fn node_uid<'a>(
        &'a self,
        name: &'a str,
    ) -> klights_leader_api::ClusterIdentityFuture<'a, Option<String>> {
        Box::pin(async move { self.uid("Node", None, name).await })
    }

    fn secret_uid<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
    ) -> klights_leader_api::ClusterIdentityFuture<'a, Option<String>> {
        Box::pin(async move { self.uid("Secret", Some(namespace), name).await })
    }
}

pub async fn validate_sa_token_bindings_for_integration(
    db: IntegrationDatastoreHandle,
    claims: &klights_auth::SaTokenClaims,
) -> Result<(), k8s_native_service::AppError> {
    klights_auth::authentication::validate_sa_token_bindings(
        &IntegrationBoundTokenSubjects { db },
        claims,
    )
    .await
    .map_err(k8s_native_service::AppError::from)
}

/// Seeds the production bootstrap-token Secret shape used by full-stack API
/// authentication cases and returns its bearer token.
pub async fn create_worker_bootstrap_token_for_integration(
    db: &IntegrationDatastoreHandle,
) -> anyhow::Result<String> {
    let token = crate::bootstrap::bootstrap_token::generate_bootstrap_token();
    crate::bootstrap::bootstrap_token::create_scoped_bootstrap_token_secret_for_test(
        db.as_ref(),
        crate::bootstrap::bootstrap_token::BootstrapTokenScope::Worker,
        &token,
    )
    .await?;
    Ok(token)
}

/// Seeds a fixed worker bootstrap Secret with a caller-selected lifetime so
/// full-stack GET tests can exercise the production rotation boundary.
pub async fn create_worker_bootstrap_token_with_ttl_for_integration(
    db: &IntegrationDatastoreHandle,
    token: &str,
    ttl: std::time::Duration,
) -> anyhow::Result<()> {
    crate::bootstrap::bootstrap_token::create_scoped_bootstrap_token_secret_with_ttl_for_test(
        db.as_ref(),
        crate::bootstrap::bootstrap_token::BootstrapTokenScope::Worker,
        token,
        ttl,
    )
    .await
}

/// Seeds a fixed control-plane bootstrap Secret with a caller-selected
/// lifetime for cross-Secret rotation isolation tests.
pub async fn create_controlplane_bootstrap_token_with_ttl_for_integration(
    db: &IntegrationDatastoreHandle,
    token: &str,
    ttl: std::time::Duration,
) -> anyhow::Result<()> {
    crate::bootstrap::bootstrap_token::create_scoped_bootstrap_token_secret_with_ttl_for_test(
        db.as_ref(),
        crate::bootstrap::bootstrap_token::BootstrapTokenScope::Controlplane,
        token,
        ttl,
    )
    .await
}

pub fn broadcast_watch_event_for_integration(
    db: &IntegrationDatastoreHandle,
    object: serde_json::Value,
) {
    let event = klights_watch::WatchEvent::added(object);
    let pending = crate::datastore::staged_post_commit_from_event(event);
    db.commit_observation_sink().observe(&[pending]);
}

pub async fn reconcile_namespace_termination_for_integration(
    db: IntegrationDatastoreHandle,
    namespace: &str,
) -> Result<(), k8s_native_service::AppError> {
    let store = crate::bootstrap::composition_adapters::api_state_adapter::RootNamespaceTerminationStore::new(db);
    let metrics = klights_controllers::side_effects::SideEffectMetrics::new();
    k8s_native_service::reconcile_namespace_termination_at(
        store.as_ref(),
        namespace,
        metrics.as_ref(),
        chrono::Utc::now(),
    )
    .await
}

pub async fn reconcile_namespace_termination_for_uid_for_integration(
    db: IntegrationDatastoreHandle,
    namespace: &str,
    expected_uid: &str,
) -> Result<k8s_native_service::NamespaceTerminationOutcome, k8s_native_service::AppError> {
    let store = crate::bootstrap::composition_adapters::api_state_adapter::RootNamespaceTerminationStore::new(db);
    let metrics = klights_controllers::side_effects::SideEffectMetrics::new();
    k8s_native_service::reconcile_namespace_termination_for_uid_with_outcome_at(
        store.as_ref(),
        namespace,
        expected_uid,
        metrics.as_ref(),
        chrono::Utc::now(),
    )
    .await
}

pub async fn mark_foreground_deletion_for_integration(
    db: IntegrationDatastoreHandle,
    target: k8s_native_service::generic_command::ResourceDeleteTarget<'_>,
    initial_resource: klights_cluster_core::Resource,
    delete_preconditions: klights_cluster_core::ResourcePreconditions,
) -> Result<klights_cluster_core::Resource, k8s_native_service::AppError> {
    let lifecycle =
        crate::bootstrap::finalizer_lifecycle_adapter::BorrowedFinalizerLifecycleStore::new(
            db.as_ref(),
        );
    k8s_native_service::generic_command::mark_foreground_deletion_with_retry(
        &lifecycle,
        target.api_version,
        target.kind,
        target.namespace,
        target.name,
        initial_resource,
        delete_preconditions,
        chrono::Utc::now(),
    )
    .await
}

pub async fn complete_non_foreground_delete_for_integration(
    db: IntegrationDatastoreHandle,
    request: k8s_native_service::generic_command::NonForegroundDeleteRequest<'_>,
) -> Result<k8s_native_service::generic_command::DeleteCompletion, k8s_native_service::AppError> {
    let lifecycle =
        crate::bootstrap::finalizer_lifecycle_adapter::BorrowedFinalizerLifecycleStore::new(
            db.as_ref(),
        );
    k8s_native_service::generic_command::complete_non_foreground_delete_with_live_recheck(
        &lifecycle, request,
    )
    .await
}

pub async fn delete_collection_listed_resource_for_integration(
    db: IntegrationDatastoreHandle,
    api_version: &'static str,
    kind: &'static str,
    namespace: Option<&str>,
    resource: klights_cluster_core::Resource,
) -> Result<bool, k8s_native_service::AppError> {
    let leader_rx = crate::control_plane::client::local::always_leader_watch();
    let resource_query = crate::bootstrap::outbox_apply_adapter::BackendResourceQueryFixture::new(
        db.clone(),
        leader_rx,
    );
    let lifecycle =
        crate::bootstrap::finalizer_lifecycle_adapter::BorrowedFinalizerLifecycleStore::new(
            db.as_ref(),
        );
    let strategy = k8s_native_service::generic_command::FinalizerAwareDeleteStrategy {
        resource_query: &resource_query,
        lifecycle: &lifecycle,
        operation_now: chrono::DateTime::from_timestamp(1_700_000_000, 0)
            .expect("fixed collection-delete integration timestamp"),
    };
    let target = klights_types::ResourceKey::new(
        api_version,
        kind,
        namespace.map(str::to_string),
        resource.name.clone(),
    );
    let intent = k8s_native_service::generic_command::DeleteIntent::collection_item(
        k8s_native_service::generic_command::DryRunMode::Live,
        klights_cluster_core::ResourcePreconditions::uid(resource.uid.clone()),
    );
    Ok(matches!(
        k8s_native_service::generic_command::delete_loaded_with_strategy(
            &strategy, target, resource, &intent,
        )
        .await?,
        k8s_native_service::generic_command::DeleteResult::HardDeleted(_)
    ))
}

fn deterministic_uuid_v4(value: u64) -> String {
    let first = ((value & 0x000f_ffff) << 12) | ((value >> 20) & 0x0fff);
    let second = (value >> 32) & 0xffff;
    let third = 0x4000 | ((value >> 48) & 0x0fff);
    let fourth = 0x8000 | ((value >> 60) & 0x000f);
    format!("{first:08x}-{second:04x}-{third:04x}-{fourth:04x}-000000000000")
}

#[derive(Default)]
struct DeterministicApiIdentity {
    next: std::sync::atomic::AtomicU64,
}

impl k8s_native_service::ApiIdentityGenerator for DeterministicApiIdentity {
    fn generate_name(&self, prefix: &str) -> String {
        let value = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("{prefix}{value:05}")
    }

    fn new_uid(&self) -> String {
        let value = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        deterministic_uuid_v4(value)
    }
}

struct AllowAllAuthorizer;

#[derive(Default)]
struct DeterministicControllerIdentity {
    next: std::sync::atomic::AtomicU64,
}

impl klights_controllers::ControllerIdentityGenerator for DeterministicControllerIdentity {
    fn generate_name(&self, prefix: &str) -> String {
        let value = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("{prefix}{value:05}")
    }

    fn new_uid(&self) -> String {
        let value = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        deterministic_uuid_v4(value)
    }
}

struct UnavailableNodeMetrics;

struct IntegrationNodeMetrics {
    inner: std::sync::RwLock<Arc<dyn klights_node_api::NodeMetrics>>,
}

impl IntegrationNodeMetrics {
    fn new() -> Self {
        Self {
            inner: std::sync::RwLock::new(Arc::new(UnavailableNodeMetrics)),
        }
    }

    fn set(&self, metrics: Arc<dyn klights_node_api::NodeMetrics>) {
        *self.inner.write().expect("integration node metrics lock") = metrics;
    }
}

impl klights_node_api::NodeMetrics for IntegrationNodeMetrics {
    fn collect_metrics(
        &self,
        request: klights_node_api::NodeMetricsRequest,
    ) -> klights_node_api::NodeMetricsFuture<'_, klights_node_api::NodeMetricsResult> {
        let metrics = self
            .inner
            .read()
            .expect("integration node metrics lock")
            .clone();
        Box::pin(async move { metrics.collect_metrics(request).await })
    }
}

#[derive(Clone, Default)]
pub struct IntegrationServiceRoutingObservation {
    sync_count: Arc<std::sync::atomic::AtomicUsize>,
    sync_now_count: Arc<std::sync::atomic::AtomicUsize>,
}

impl IntegrationServiceRoutingObservation {
    pub fn sync_count(&self) -> usize {
        self.sync_count.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn sync_now_count(&self) -> usize {
        self.sync_now_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl klights_reconcile_api::ServiceRoutingSync for IntegrationServiceRoutingObservation {
    fn request_service_routing_sync(
        &self,
    ) -> Result<(), klights_reconcile_api::ReconcileSinkError> {
        self.sync_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}

impl klights_node_api::NodeMetrics for UnavailableNodeMetrics {
    fn collect_metrics(
        &self,
        _request: klights_node_api::NodeMetricsRequest,
    ) -> klights_node_api::NodeMetricsFuture<'_, klights_node_api::NodeMetricsResult> {
        Box::pin(async {
            Err(klights_node_api::NodeMetricsError::unavailable(
                "node metrics are not configured for the integration harness",
            ))
        })
    }
}

#[async_trait::async_trait]
impl klights_auth::authorizer::Authorizer for AllowAllAuthorizer {
    async fn authorize(
        &self,
        _identity: &klights_auth::AuthenticatedIdentity,
        _request: &klights_auth::request_attributes::AuthorizationRequest,
    ) -> klights_auth::authorizer::AuthorizationDecision {
        klights_auth::authorizer::AuthorizationDecision::allow("integration harness allow-all")
    }
}

/// Narrow integration handle around one real registered replication follower.
pub struct IntegrationFollowerSession {
    replication: Arc<klights_replication::ReplicationService>,
    control_rx: tokio::sync::mpsc::Receiver<klights_node_api::FollowerControlMessage>,
    node_name: String,
    session_id: u64,
}

impl IntegrationFollowerSession {
    pub async fn recv(&mut self) -> Option<klights_node_api::FollowerControlMessage> {
        self.control_rx.recv().await
    }

    pub async fn complete_node_log_event(
        &self,
        event: klights_node_api::RoutedNodeLogEvent,
    ) -> anyhow::Result<()> {
        self.replication
            .complete_node_log_event(
                klights_node_api::FollowerCompletionContext::new(
                    &self.node_name,
                    self.session_id,
                    klights_node_api::NodeOperationKind::Log,
                ),
                event,
            )
            .await
    }

    pub async fn complete_node_exec_sync(
        &self,
        response: klights_node_api::RoutedNodeExecSyncResponse,
    ) -> anyhow::Result<()> {
        self.replication
            .complete_node_exec_sync(
                klights_node_api::FollowerCompletionContext::new(
                    &self.node_name,
                    self.session_id,
                    klights_node_api::NodeOperationKind::ExecSync,
                ),
                response,
            )
            .await
    }
}

/// Feature-only executor for the native exec-sync WebSocket adapter backed by
/// the same real replication service registered through the parent harness.
#[derive(Clone)]
pub struct IntegrationRemoteExecSync {
    node_exec: Arc<dyn klights_node_api::NodeExec>,
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
}

impl IntegrationRemoteExecSync {
    pub async fn run<S>(
        self,
        io: S,
        target: k8s_native_service::streaming::ExecTarget,
        subprotocol: String,
        node_name: String,
    ) where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let socket = tokio_tungstenite::WebSocketStream::from_raw_socket(
            io,
            tokio_tungstenite::tungstenite::protocol::Role::Server,
            None,
        )
        .await;
        k8s_native_service::streaming::handle_remote_exec_websocket_sync(
            socket,
            k8s_native_service::streaming::RemoteExecWebSocketSyncRequest {
                node_exec: self.node_exec,
                target,
                subprotocol,
                node_name,
                task_supervisor: self.task_supervisor,
            },
        )
        .await;
    }
}

#[derive(Clone)]
pub struct IntegrationCsrSignerObservation {
    request_count: Arc<std::sync::atomic::AtomicUsize>,
    changed: Arc<tokio::sync::Notify>,
}

impl IntegrationCsrSignerObservation {
    pub fn request_count(&self) -> usize {
        self.request_count
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub async fn wait_for_request(&self) {
        loop {
            let changed = self.changed.notified();
            if self.request_count() > 0 {
                return;
            }
            changed.await;
        }
    }
}

struct IntegrationRecordingCsrSigner {
    request_count: Arc<std::sync::atomic::AtomicUsize>,
    changed: Arc<tokio::sync::Notify>,
}

/// In-process leader delivery for the native API fixture. This keeps the real
/// node outbox codec, authorization, watermark, proposal, and committed-apply
/// path while keeping the authenticated worker identity fixed by fixture
/// composition, independently of the command payload under test.
struct IntegrationOutboxDelivery {
    embedded: Arc<klights_replication::leader_api::EmbeddedOutboxDelivery>,
    codec: Arc<dyn klights_leader_api::OutboxPayloadCodec>,
    authenticated_node: String,
}

impl klights_leader_api::LeaderOutboxDelivery for IntegrationOutboxDelivery {
    fn deliver_outbox(
        &self,
        request: klights_leader_api::OutboxDeliveryRequest,
    ) -> klights_leader_api::OutboxDeliveryFuture<'_> {
        Box::pin(async move {
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
                klights_leader_api::OutboxDeliveryError::invalid(
                    "delivery.payload",
                    error.to_string(),
                )
            });
            let effect = self
                .embedded
                .deliver_authenticated_outbox_command_effect(
                    self.authenticated_node.clone(),
                    idempotency_key,
                    operation,
                    decoded_command,
                    Some(klights_cluster_core::OutboxStreamWatermark {
                        client_id,
                        stream_id,
                        stream_seq: stream_sequence,
                    }),
                )
                .await?;
            let (result, _, _, _) = effect.into_parts();
            Ok(result.into())
        })
    }
}

impl klights_auth::csr_signer::CsrSigner for IntegrationRecordingCsrSigner {
    fn sign(
        &self,
        _request: klights_auth::csr_signer::SignRequest,
    ) -> Result<klights_auth::csr_signer::SignResult, klights_auth::CredentialOperationError> {
        self.request_count
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        self.changed.notify_waiters();
        Ok(klights_auth::csr_signer::SignResult {
            certificate_pem: "-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----\n"
                .to_string(),
        })
    }
}

pub struct IntegrationHeldSupervisorTask {
    handle: klights_supervisor::SupervisedJoinHandle<()>,
}

pub struct IntegrationSchedulerBindGate {
    gate: Arc<crate::bootstrap::composition_adapters::pod_native_adapter::SchedulerBindGateForTest>,
}

impl IntegrationSchedulerBindGate {
    pub async fn wait_for_entered_at_least(&self, target: usize) {
        self.gate.wait_for_entered_at_least(target).await;
    }

    pub fn release_all(&self) {
        self.gate.release_all();
    }
}

/// Opaque worker-owned repository composition for node-local/outbox tests.
pub struct IntegrationPodWorkerComposition {
    repository: Arc<crate::kubelet::pod_repository::PodRepository>,
    node_local: Arc<crate::datastore::node_local::NodeLocalStores>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntegrationPodFinalizationOutcome {
    DeletedOrAlreadyGone,
    Queued,
    FinalizersPending,
}

async fn integration_finalize_pod_after_actor_cleanup(
    repository: &crate::kubelet::pod_repository::PodRepository,
    namespace: &str,
    name: &str,
    uid: &str,
) -> anyhow::Result<IntegrationPodFinalizationOutcome> {
    let key = crate::kubelet::pod_runtime::service::PodRuntimeKey::new(namespace, name, uid);
    Ok(match repository.deletion_finalizer().finalize_after_actor_cleanup(&key).await? {
        crate::kubelet::pod_runtime::service::PodDeletionFinalizeResult::DeletedOrAlreadyGone => {
            IntegrationPodFinalizationOutcome::DeletedOrAlreadyGone
        }
        crate::kubelet::pod_runtime::service::PodDeletionFinalizeResult::Queued => {
            IntegrationPodFinalizationOutcome::Queued
        }
        crate::kubelet::pod_runtime::service::PodDeletionFinalizeResult::FinalizersPending => {
            IntegrationPodFinalizationOutcome::FinalizersPending
        }
    })
}

impl IntegrationPodWorkerComposition {
    pub async fn new(resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>) -> Self {
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let node_local = Arc::new(
            crate::datastore::node_local::selector::open_node_local(
                crate::datastore::backend_kind::BackendKind::Sqlite,
                None,
                supervisor.clone(),
                None,
                "sqlite:pod-worker-composition-integration",
            )
            .await
            .expect("worker repository node-local store"),
        );
        let ports = klights_kubelet::node_outbox::OutboxStores::new(
            node_local.outbox_producer(),
            node_local.outbox_dispatcher(),
            node_local.pod_status_checkpoints(),
            node_local.runtime_observation_checkpoints(),
            node_local.outbox_status_stamps(),
        );
        let outbox = Arc::new(klights_kubelet::node_outbox::Outbox::compose(
            ports,
            crate::bootstrap::composition_adapters::outbox_payload_codec_adapter::new_codec(),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(klights_supervisor::SystemWallClock),
        ));
        let parts = crate::pod_repository_composition::build_worker_pod_repository_parts(
            crate::pod_repository_composition::WorkerPodRepositoryBuildConfig {
                resource_query,
                pod_workqueue_store: node_local.pod_workqueue(),
                supervisor,
                metrics: klights_controllers::side_effects::SideEffectMetrics::new(),
                pod_network_cache: Arc::new(IntegrationEmptyPodNetworkCache),
                assignment_waiter: Arc::new(klights_networking::PodNetworkAssignmentBus::new()),
                outbox,
            },
        );
        Self {
            repository: Arc::new(parts.repository),
            node_local,
        }
    }

    pub async fn claim_next_due_outbox(
        &self,
        now_ms: i64,
        lease_ms: i64,
        lease_token: &str,
    ) -> anyhow::Result<Option<IntegrationClaimedPodOutbox>> {
        claim_pod_outbox(&self.node_local, now_ms, lease_ms, lease_token).await
    }

    pub async fn finalize_pod_deletion_after_actor_cleanup(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<IntegrationPodFinalizationOutcome> {
        integration_finalize_pod_after_actor_cleanup(self.repository.as_ref(), namespace, name, uid)
            .await
    }

    pub async fn get_pod(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        crate::kubelet::pod_repository::PodReader::get_pod(
            self.repository.as_ref(),
            namespace,
            name,
        )
        .await
    }

    pub async fn get_pod_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        crate::kubelet::pod_repository::PodReader::get_pod_for_uid(
            self.repository.as_ref(),
            namespace,
            name,
            uid,
        )
        .await
    }

    pub async fn set_pod_status_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        update: crate::kubelet::pod_repository::PodStatusUpdate,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::set_pod_status_for_uid(
            self.repository.as_ref(),
            namespace,
            name,
            uid,
            update,
            expected_rv,
        )
        .await
    }

    pub async fn apply_runtime_reconcile_status_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        update: crate::kubelet::pod_repository::RuntimeReconcileStatus,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::apply_runtime_reconcile_status_for_uid(
            self.repository.as_ref(),
            namespace,
            name,
            uid,
            update,
            expected_rv,
        )
        .await
    }

    pub async fn record_sandbox_id_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        sandbox_id: &str,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodMetadataWriter::record_sandbox_id_for_uid(
            self.repository.as_ref(),
            namespace,
            name,
            uid,
            sandbox_id,
        )
        .await
    }

    pub async fn update_pod_owner_references_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        owner_references: Vec<serde_json::Value>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodObjectWriter::update_pod_owner_references_for_uid(
            self.repository.as_ref(),
            namespace,
            name,
            uid,
            owner_references,
        )
        .await
    }

    pub async fn merge_pod_labels_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        labels: Vec<(String, String)>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodObjectWriter::merge_pod_labels_for_uid(
            self.repository.as_ref(),
            namespace,
            name,
            uid,
            labels,
        )
        .await
    }

    pub async fn seed_status_checkpoint(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        base_position: i64,
        status: serde_json::Value,
        updated_ms: i64,
    ) -> anyhow::Result<()> {
        let checkpoint = klights_node_store::PodStatusCheckpointUpsert::try_new(
            klights_types::PodIdentity::new(namespace, name, uid),
            base_position,
            serde_json::to_vec(&status)?,
            updated_ms,
        )?;
        self.node_local
            .pod_status_checkpoints()
            .upsert_pod_status_checkpoint(checkpoint)
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))
    }

    pub async fn has_status_checkpoint(&self, uid: &str) -> anyhow::Result<bool> {
        let key = klights_node_store::PodCheckpointKey::try_new(uid)?;
        self.node_local
            .pod_status_checkpoints()
            .get_pod_status_checkpoint(key)
            .await
            .map(|value| value.is_some())
            .map_err(|error| anyhow::anyhow!(error.to_string()))
    }

    async fn dispatch_due_once(
        &self,
        delivery: Arc<dyn klights_leader_api::LeaderOutboxDelivery>,
    ) -> anyhow::Result<klights_kubelet::node_outbox::DispatchOutcome> {
        let stores = klights_kubelet::node_outbox::OutboxStores::new(
            self.node_local.outbox_producer(),
            self.node_local.outbox_dispatcher(),
            self.node_local.pod_status_checkpoints(),
            self.node_local.runtime_observation_checkpoints(),
            self.node_local.outbox_status_stamps(),
        );
        klights_kubelet::node_outbox::OutboxDispatcher::new(
            stores,
            crate::bootstrap::composition_adapters::outbox_payload_codec_adapter::new_codec(),
            delivery,
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(klights_supervisor::SystemWallClock),
        )
        .dispatch_due_once(i64::MAX / 4)
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))
    }
}

pub struct IntegrationWorkerFinalizationRaceOutcome {
    pub initially_pending: bool,
    pub resource_version_advanced: bool,
    pub dispatched: bool,
    pub removed_after_dispatch: bool,
    pub completed_after_committed_absence: bool,
    pub node_mismatch_rejected: bool,
}

pub struct IntegrationWorkerFinalizationDeliveryOutcome {
    pub queued: bool,
    pub exact_uid_bound_command: bool,
    pub committed_resource_receipt: bool,
    pub authoritative_pod_removed: bool,
}

pub async fn run_worker_actor_finalization_delivery_scenario()
-> anyhow::Result<IntegrationWorkerFinalizationDeliveryOutcome> {
    let sqlite = crate::datastore::sqlite::Datastore::new_in_memory().await?;
    let db: crate::datastore::DatastoreHandle = Arc::new(sqlite.clone());
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "leader-finalize",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "leader-finalize",
                "uid": "uid-leader-finalize",
                "deletionTimestamp": "2026-05-13T00:00:00Z",
                "deletionGracePeriodSeconds": 0
            },
            "spec": {
                "nodeName": "worker-1",
                "containers": [{"name": "app", "image": "nginx"}]
            },
            "status": {"phase": "Running"}
        }),
    )
    .await?;
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let cluster_api = Arc::new(
        crate::control_plane::client::local::LocalApiClient::new_with_node_lease_tracker_and_passive_reads(
            db.clone(),
            crate::datastore::selector::sqlite_passive_read_ports(&sqlite),
            "worker-1".to_string(),
            Arc::new(klights_controllers::node_lease::NodeLeaseTracker::new_at(chrono::Utc::now())),
            crate::control_plane::client::local::always_leader_watch(),
            klights_supervisor::FileProcessExecutor::new(supervisor),
        ),
    );
    let repository = IntegrationPodWorkerComposition::new(cluster_api).await;
    let queued = repository
        .finalize_pod_deletion_after_actor_cleanup(
            "default",
            "leader-finalize",
            "uid-leader-finalize",
        )
        .await?
        == IntegrationPodFinalizationOutcome::Queued;
    let request = klights_node_store::OutboxClaimRequest::try_new(
        i64::MAX / 4,
        1_000,
        "finalization-delivery",
    )?;
    let row = repository
        .node_local
        .outbox_dispatcher()
        .claim_next_due_outbox(request)
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?
        .expect("worker finalization must enqueue an outbox row");
    let command = crate::bootstrap::composition_adapters::outbox_payload_codec_adapter::new_codec()
        .decode(row.payload())
        .expect("worker finalization command must decode");
    let exact_uid_bound_command = matches!(
        &command,
        klights_cluster_core::StorageCommand::FinalizeBoundPod {
            namespace,
            name,
            pod_uid,
            node_name,
            observed_resource_version,
        } if namespace == "default"
            && name == "leader-finalize"
            && pod_uid == "uid-leader-finalize"
            && node_name == "worker-1"
            && *observed_resource_version > 0
    );
    let applied = crate::bootstrap::outbox_apply_adapter::propose_outbox_command_on_backend(
        db.as_ref(),
        row.idempotency_key(),
        klights_kubelet::outbox::OutboxOperation::PodMetadata,
        command,
        "worker-1",
        None,
    )
    .await
    .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let (_, _, _, committed_resource) = applied.into_parts();
    let authoritative_pod_removed = db
        .get_resource("v1", "Pod", Some("default"), "leader-finalize")
        .await?
        .is_none();
    Ok(IntegrationWorkerFinalizationDeliveryOutcome {
        queued,
        exact_uid_bound_command,
        committed_resource_receipt: committed_resource.is_some(),
        authoritative_pod_removed,
    })
}

pub async fn run_worker_actor_finalization_race()
-> anyhow::Result<IntegrationWorkerFinalizationRaceOutcome> {
    let sqlite = crate::datastore::sqlite::Datastore::new_in_memory().await?;
    let db: crate::datastore::DatastoreHandle = Arc::new(sqlite.clone());
    let created = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "rv-retry-finalize",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "rv-retry-finalize",
                    "uid": "uid-rv-retry-finalize",
                    "deletionTimestamp": "2026-07-24T00:00:00Z",
                    "deletionGracePeriodSeconds": 0
                },
                "spec": {
                    "nodeName": "worker-1",
                    "containers": [{"name": "app", "image": "nginx"}]
                },
                "status": {"phase": "Running"}
            }),
        )
        .await?;
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let cluster_api = Arc::new(
        crate::control_plane::client::local::LocalApiClient::new_with_node_lease_tracker_and_passive_reads(
            db.clone(),
            crate::datastore::selector::sqlite_passive_read_ports(&sqlite),
            "worker-1".to_string(),
            Arc::new(klights_controllers::node_lease::NodeLeaseTracker::new_at(chrono::Utc::now())),
            crate::control_plane::client::local::always_leader_watch(),
            klights_supervisor::FileProcessExecutor::new(supervisor),
        ),
    );
    let repository = IntegrationPodWorkerComposition::new(cluster_api.clone()).await;
    let initially_pending = repository
        .finalize_pod_deletion_after_actor_cleanup(
            "default",
            "rv-retry-finalize",
            "uid-rv-retry-finalize",
        )
        .await?
        == IntegrationPodFinalizationOutcome::Queued;
    let raced = db
        .update_status_only(
            "v1",
            "Pod",
            Some("default"),
            "rv-retry-finalize",
            serde_json::json!({"phase": "Running", "reason": "ConcurrentStatus"}),
            Some(created.resource_version),
        )
        .await?;
    let dispatched = repository.dispatch_due_once(cluster_api.clone()).await?
        == klights_kubelet::node_outbox::DispatchOutcome::Dispatched;
    let removed_after_dispatch = db
        .get_resource("v1", "Pod", Some("default"), "rv-retry-finalize")
        .await?
        .is_none();
    let completed_after_committed_absence = repository
        .finalize_pod_deletion_after_actor_cleanup(
            "default",
            "rv-retry-finalize",
            "uid-rv-retry-finalize",
        )
        .await?
        == IntegrationPodFinalizationOutcome::DeletedOrAlreadyGone;
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "wrong-node-finalize",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "wrong-node-finalize",
                "uid": "uid-wrong-node-finalize",
                "deletionTimestamp": "2026-07-24T00:00:00Z",
                "deletionGracePeriodSeconds": 0
            },
            "spec": {
                "nodeName": "worker-2",
                "containers": [{"name": "app", "image": "nginx"}]
            }
        }),
    )
    .await?;
    repository
        .finalize_pod_deletion_after_actor_cleanup(
            "default",
            "wrong-node-finalize",
            "uid-wrong-node-finalize",
        )
        .await?;
    let _ = repository.dispatch_due_once(cluster_api).await?;
    let node_mismatch_rejected = db
        .get_resource("v1", "Pod", Some("default"), "wrong-node-finalize")
        .await?
        .is_some();
    Ok(IntegrationWorkerFinalizationRaceOutcome {
        initially_pending,
        resource_version_advanced: raced.resource_version > created.resource_version,
        dispatched,
        removed_after_dispatch,
        completed_after_committed_absence,
        node_mismatch_rejected,
    })
}

impl IntegrationHeldSupervisorTask {
    pub fn abort(&self) {
        self.handle.abort();
    }
}

/// Opaque root-owned repository fixture for base composition tests.
pub struct IntegrationPodRepositoryComposition {
    _sqlite: crate::datastore::sqlite::Datastore,
    db: crate::datastore::DatastoreHandle,
    repository: Arc<crate::kubelet::pod_repository::PodRepository>,
    pod_api: Arc<k8s_native_service::PodApiService>,
    pod_subresource: Arc<k8s_native_service::PodSubresourceService>,
    pod_scheduling: Arc<dyn klights_pod_api::PodScheduling>,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
    background: crate::kubelet::pod_repository::background::PodRepositoryBackground,
    controller_dispatcher: Option<Arc<klights_controllers::ControllerDispatcher>>,
    node_local: Option<Arc<crate::datastore::node_local::NodeLocalStores>>,
    outbox_delivery: Option<Arc<dyn klights_leader_api::LeaderOutboxDelivery>>,
    delete_observation: Option<Arc<tokio::sync::Mutex<Option<(bool, bool)>>>>,
}

struct IntegrationEmptyPodNetworkCache;

impl klights_node_store::PodNetworkCache for IntegrationEmptyPodNetworkCache {
    fn get_network_for_uid(
        &self,
        _pod_uid: klights_node_store::PodUidKey,
    ) -> klights_node_store::CacheNetworkFuture<'_, Option<klights_node_store::PodNetworkEndpoint>>
    {
        Box::pin(async { Ok(None) })
    }

    fn get_network_for_pod(
        &self,
        _pod: klights_types::PodIdentity,
    ) -> klights_node_store::CacheNetworkFuture<'_, Option<klights_node_store::PodNetworkEndpoint>>
    {
        Box::pin(async { Ok(None) })
    }

    fn get_network_for_sandbox(
        &self,
        _sandbox_id: klights_node_store::SandboxKey,
    ) -> klights_node_store::CacheNetworkFuture<'_, Option<klights_node_store::PodNetworkEndpoint>>
    {
        Box::pin(async { Ok(None) })
    }

    fn get_network_for_assignment(
        &self,
        _sandbox_id: klights_node_store::SandboxKey,
        _pod: klights_types::PodIdentity,
    ) -> klights_node_store::CacheNetworkFuture<'_, Option<klights_node_store::PodNetworkEndpoint>>
    {
        Box::pin(async { Ok(None) })
    }

    fn delete_network_for_sandbox(
        &self,
        _sandbox_id: klights_node_store::SandboxKey,
    ) -> klights_node_store::CacheNetworkFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn delete_network_if_matches(
        &self,
        _request: klights_node_store::PodNetworkAllocationRequest,
    ) -> klights_node_store::CacheNetworkFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }

    fn list_network_assignments(
        &self,
    ) -> klights_node_store::CacheNetworkFuture<
        '_,
        Vec<klights_node_store::PodNetworkAssignmentSnapshot>,
    > {
        Box::pin(async { Ok(Vec::new()) })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntegrationBoundPodDeleteOutcome {
    Removed,
    IdentityChanged,
    FinalizersPending,
    Retry,
}

pub struct IntegrationStatusRaceOutcome {
    pub attempts: usize,
    pub resource: Option<crate::datastore::Resource>,
    pub conflict: bool,
}

pub struct IntegrationApiDeleteStatusRaceOutcome {
    pub created: crate::datastore::Resource,
    pub deleted: crate::datastore::Resource,
    pub persisted: crate::datastore::Resource,
    pub status_bumps: usize,
}

struct IntegrationDeleteStatusRacingRaftProposal {
    inner: crate::datastore::DatastoreHandle,
    pod_name: String,
    bumps: Arc<std::sync::atomic::AtomicUsize>,
}

impl IntegrationDeleteStatusRacingRaftProposal {
    async fn apply(
        &self,
        command: klights_cluster_core::StorageCommand,
        idempotency_key: &str,
        operation: klights_kubelet::outbox::OutboxOperation,
        authoring_node: &str,
    ) -> Result<
        klights_replication::proposal::RaftProposalEffect,
        klights_cluster_core::OutboxApplyError,
    > {
        let targets_delete_mark = match &command {
            klights_cluster_core::StorageCommand::UpdateResource {
                api_version,
                kind,
                namespace,
                name,
                data,
                ..
            } => {
                api_version == "v1"
                    && kind == "Pod"
                    && namespace.as_deref() == Some("default")
                    && name == &self.pod_name
                    && data
                        .pointer("/metadata/deletionTimestamp")
                        .and_then(serde_json::Value::as_str)
                        .is_some()
            }
            klights_cluster_core::StorageCommand::PatchResource {
                api_version,
                kind,
                namespace,
                name,
                patch,
                ..
            } => {
                api_version == "v1"
                    && kind == "Pod"
                    && namespace.as_deref() == Some("default")
                    && name == &self.pod_name
                    && patch
                        .pointer("/metadata/deletionTimestamp")
                        .and_then(serde_json::Value::as_str)
                        .is_some()
            }
            _ => false,
        };
        if targets_delete_mark {
            let bump = self.bumps.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            if let Some(current) = self
                .inner
                .get_resource("v1", "Pod", Some("default"), &self.pod_name)
                .await
                .map_err(|error| {
                    klights_cluster_core::OutboxApplyError::Retryable(error.to_string())
                })?
            {
                self.inner.update_status_only_with_preconditions(
                    "v1", "Pod", Some("default"), &self.pod_name,
                    serde_json::json!({"phase": "Running", "podIP": "10.42.0.55", "raceBump": bump}),
                    crate::datastore::ResourcePreconditions::uid(current.uid),
                ).await.map_err(|error| klights_cluster_core::OutboxApplyError::Retryable(error.to_string()))?;
            }
        }
        crate::bootstrap::outbox_apply_adapter::propose_outbox_command_on_backend(
            self.inner.as_ref(),
            idempotency_key,
            operation,
            command,
            authoring_node,
            None,
        )
        .await
    }
}

#[async_trait::async_trait]
impl klights_replication::proposal::RaftProposal for IntegrationDeleteStatusRacingRaftProposal {
    async fn propose_command(
        &self,
        command: klights_cluster_core::StorageCommand,
    ) -> anyhow::Result<klights_cluster_store::StorageCommandResult> {
        let effect = self
            .apply(
                command,
                &format!("delete-race-{}", uuid::Uuid::new_v4()),
                klights_kubelet::outbox::OutboxOperation::PodStatus,
                "delete-race-leader",
            )
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let (result, resource_effect, pod_endpoint_effect, committed_resource) =
            effect.into_parts();
        let applied_rv = match result {
            klights_cluster_core::OutboxApplyOutcome::Applied { applied_rv } => Some(applied_rv),
            klights_cluster_core::OutboxApplyOutcome::AlreadyApplied { applied_rv } => applied_rv,
        };
        Ok(klights_cluster_store::StorageCommandResult::new(
            applied_rv,
            None,
            None,
            resource_effect == klights_cluster_core::ResourceMutationEffect::Changed,
            committed_resource.map(klights_cluster_store::AppliedMutation::Resource),
            pod_endpoint_effect,
        ))
    }

    async fn propose_outbox_command(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: klights_cluster_core::StorageCommand,
        authoring_node: &str,
        _watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> Result<klights_cluster_core::OutboxApplyOutcome, klights_cluster_core::OutboxApplyError>
    {
        let operation =
            klights_kubelet::outbox::OutboxOperation::try_from(operation).map_err(|error| {
                klights_cluster_core::OutboxApplyError::Retryable(error.to_string())
            })?;
        Ok(self
            .apply(command, idempotency_key, operation, authoring_node)
            .await?
            .into_parts()
            .0)
    }
}

pub async fn run_raft_delete_mark_status_race(
    pod_name: &str,
    grace_period_seconds: Option<i64>,
) -> anyhow::Result<IntegrationApiDeleteStatusRaceOutcome> {
    let sqlite = crate::datastore::sqlite::Datastore::new_in_memory().await?;
    let inner: crate::datastore::DatastoreHandle = Arc::new(sqlite.clone());
    let bumps = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let proposal = Arc::new(IntegrationDeleteStatusRacingRaftProposal {
        inner: inner.clone(),
        pod_name: pod_name.to_string(),
        bumps: bumps.clone(),
    });
    let db: crate::datastore::DatastoreHandle = Arc::new(
        crate::bootstrap::sequenced_datastore::SequencedDatastore::new_with_clock(
            inner,
            proposal,
            Arc::new(klights_supervisor::SystemWallClock),
        ),
    );
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let local_query: Arc<dyn klights_leader_api::LeaderResourceQuery> = Arc::new(crate::control_plane::client::local::LocalApiClient::new_with_node_lease_tracker_and_passive_reads(
        db.clone(), crate::datastore::selector::sqlite_passive_read_ports(&sqlite), "delete-race-leader".to_string(),
        Arc::new(klights_controllers::node_lease::NodeLeaseTracker::new_at(chrono::Utc::now())), crate::control_plane::client::local::always_leader_watch(), klights_supervisor::FileProcessExecutor::new(supervisor.clone()),
    ));
    let parts = crate::pod_repository_composition::build_integration_pod_repository_parts(
        crate::pod_repository_composition::PodRepositoryBuildConfig {
            db: db.clone(), pod_workqueue_store: None, supervisor, side_effects: Arc::new(klights_controllers::side_effects::SideEffectRegistry::new()),
            metrics: klights_controllers::side_effects::SideEffectMetrics::new(), pod_network_cache: Arc::new(IntegrationEmptyPodNetworkCache), assignment_waiter: Arc::new(klights_networking::PodNetworkAssignmentBus::new()),
            scheduling_mode: crate::pod_repository_composition::PodSchedulingMode::InlineSingleNode, outbox: None, cluster_api: None,
            controller_identity: crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity(),
            #[cfg(not(test))]
            api_identity: Arc::new(crate::bootstrap::controller_adapters::system_identity_adapter::SystemIdentityGenerator),
            #[cfg(not(test))]
            gc_coordination: Arc::new(klights_controllers::ControllerCoordination::new()),
            scheduler_bind_gate: None,
        }, local_query,
    );
    use klights_pod_api::PodApiMutation as _;
    let created = parts.api.create_pod(klights_pod_api::PodApiCreateRequest { namespace: "default".to_string(), body: serde_json::json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":pod_name},"spec":{"containers":[{"name":"c","image":"busybox"}]}}), dry_run: false }).await.map_err(anyhow::Error::new)?.resource.expect("race create persists");
    let deleted = match parts
        .api
        .delete_pod(klights_pod_api::PodApiDeleteRequest {
            namespace: "default".to_string(),
            name: pod_name.to_string(),
            options: k8s_native_service::DeleteOptions {
                _grace_period_seconds: grace_period_seconds,
                preconditions: None,
                ..Default::default()
            }
            .into(),
            dry_run: false,
        })
        .await
        .map_err(anyhow::Error::new)?
    {
        klights_pod_api::PodApiDeleteOutcome::GracefulSet(resource) => resource,
        klights_pod_api::PodApiDeleteOutcome::DryRun(_) => {
            anyhow::bail!("raft delete race unexpectedly dry-ran")
        }
    };
    let persisted = db
        .get_resource("v1", "Pod", Some("default"), pod_name)
        .await?
        .expect("actor-owned row remains");
    Ok(IntegrationApiDeleteStatusRaceOutcome {
        created,
        deleted,
        persisted,
        status_bumps: bumps.load(std::sync::atomic::Ordering::SeqCst),
    })
}

pub async fn run_api_delete_status_race(
    pod_name: &str,
    grace_period_seconds: Option<i64>,
) -> anyhow::Result<IntegrationApiDeleteStatusRaceOutcome> {
    let repo = IntegrationPodRepositoryComposition::new_inline().await;
    let created = repo
        .api_create_pod(crate::kubelet::pod_repository::PodApiCreateRequest {
            namespace: "default".to_string(),
            name: String::new(),
            body: serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": pod_name},
                "spec": {"containers": [{"name": "c", "image": "busybox"}]}
            }),
            dry_run: false,
            run_admission: false,
        })
        .await
        .map_err(anyhow::Error::new)?
        .resource
        .expect("delete race Pod create persists");
    let pause = repo._sqlite.install_resource_mutation_pause(
        IntegrationResourceMutationPauseOperation::BuildPatchCommand,
        "v1",
        "Pod",
        Some("default"),
        pod_name,
    );
    let delete = repo.api_delete_pod(
        "default",
        pod_name,
        k8s_native_service::DeleteOptions {
            _grace_period_seconds: grace_period_seconds,
            preconditions: None,
            ..Default::default()
        },
        false,
    );
    let race = async {
        pause.wait_until_reached().await;
        let current = repo
            .read_pod("default", pod_name)
            .await?
            .expect("delete race Pod exists before mark");
        let updated = repo
            .update_pod_status(
                "default",
                pod_name,
                serde_json::json!({"phase": "Running", "raceBump": 1}),
                Some(current.resource_version),
            )
            .await;
        pause.resume();
        updated
    };
    let (deleted, raced) = tokio::join!(delete, race);
    raced?;
    let deleted = match deleted.map_err(anyhow::Error::new)? {
        crate::kubelet::pod_repository::PodApiDeleteOutcome::GracefulSet(resource) => resource,
        crate::kubelet::pod_repository::PodApiDeleteOutcome::DryRun(_) => {
            anyhow::bail!("delete race unexpectedly dry-ran")
        }
    };
    let persisted = repo
        .read_pod("default", pod_name)
        .await?
        .expect("actor-owned row remains after delete mark");
    Ok(IntegrationApiDeleteStatusRaceOutcome {
        created,
        deleted,
        persisted,
        status_bumps: 1,
    })
}

pub struct IntegrationClaimedPodOutbox {
    pub operation: String,
    pub pod_uid: String,
    pub command: IntegrationPodOutboxCommand,
}

pub struct IntegrationPodWatchEvent {
    pub event_type: String,
    pub resource: crate::datastore::Resource,
}

pub struct IntegrationPodWorkqueueEntry {
    pub namespace: String,
    pub name: String,
    pub uid: String,
    pub target_node: Option<String>,
}

struct IntegrationRecordingPodDeleteHook {
    db: crate::datastore::DatastoreHandle,
    observed: Arc<tokio::sync::Mutex<Option<(bool, bool)>>>,
}

#[async_trait::async_trait]
impl klights_controllers::side_effects::SideEffect for IntegrationRecordingPodDeleteHook {
    fn name(&self) -> &'static str {
        "integration_recording_pod_delete_hook"
    }

    async fn apply(&self, resource: &serde_json::Value) -> anyhow::Result<()> {
        let namespace = resource
            .pointer("/metadata/namespace")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("default");
        let name = resource
            .pointer("/metadata/name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let exists = self
            .db
            .get_resource("v1", "Pod", Some(namespace), name)
            .await?
            .is_some();
        let original_owner = resource
            .pointer("/metadata/ownerReferences/0/name")
            .and_then(serde_json::Value::as_str)
            == Some("rs-x");
        *self.observed.lock().await = Some((exists, original_owner));
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
pub enum IntegrationPodOutboxCommand {
    SandboxAnnotationPatch {
        api_version: String,
        kind: String,
        namespace: Option<String>,
        name: String,
        patch_kind: klights_cluster_core::PatchKind,
        pod_uid: String,
        resource_version: i64,
        strict_resource_version: bool,
        sandbox_id: String,
    },
    DeleteMarkPatch {
        api_version: String,
        kind: String,
        namespace: Option<String>,
        name: String,
        patch_kind: klights_cluster_core::PatchKind,
        pod_uid: String,
        resource_version: Option<i64>,
        strict_resource_version: bool,
        grace_period_seconds: i64,
        has_deletion_timestamp: bool,
    },
    FinalizeBoundPod {
        namespace: String,
        name: String,
        pod_uid: String,
        node_name: String,
        observed_resource_version: i64,
    },
    Other,
}

async fn claim_pod_outbox(
    stores: &crate::datastore::node_local::NodeLocalStores,
    now_ms: i64,
    lease_ms: i64,
    lease_token: &str,
) -> anyhow::Result<Option<IntegrationClaimedPodOutbox>> {
    let request = klights_node_store::OutboxClaimRequest::try_new(now_ms, lease_ms, lease_token)?;
    stores
        .outbox_dispatcher()
        .claim_next_due_outbox(request)
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))
        .map(|row| {
            row.map(|row| {
                let command = crate::bootstrap::composition_adapters::outbox_payload_codec_adapter::new_codec()
                    .decode(row.payload().as_ref())
                    .expect("integration outbox command must decode");
                let command = match command {
                    klights_cluster_core::StorageCommand::PatchResource {
                        api_version,
                        kind,
                        namespace,
                        name,
                        patch_kind,
                        patch,
                        preconditions,
                        strict_resource_version,
                    } if patch.pointer("/metadata/annotations/klights.dev~1sandbox-id").is_some() => {
                        IntegrationPodOutboxCommand::SandboxAnnotationPatch {
                            api_version,
                            kind,
                            namespace,
                            name,
                            patch_kind,
                            pod_uid: preconditions.uid.unwrap_or_default(),
                            resource_version: preconditions.resource_version.unwrap_or_default(),
                            strict_resource_version,
                            sandbox_id: patch
                                .pointer("/metadata/annotations/klights.dev~1sandbox-id")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or_default()
                                .to_string(),
                        }
                    }
                    klights_cluster_core::StorageCommand::PatchResource {
                        api_version,
                        kind,
                        namespace,
                        name,
                        patch_kind,
                        patch,
                        preconditions,
                        strict_resource_version,
                        ..
                    } if patch.pointer("/metadata/deletionTimestamp").is_some() => {
                        IntegrationPodOutboxCommand::DeleteMarkPatch {
                            api_version,
                            kind,
                            namespace,
                            name,
                            patch_kind,
                            pod_uid: preconditions.uid.unwrap_or_default(),
                            resource_version: preconditions.resource_version,
                            strict_resource_version,
                            grace_period_seconds: patch
                                .pointer("/metadata/deletionGracePeriodSeconds")
                                .and_then(serde_json::Value::as_i64)
                                .unwrap_or_default(),
                            has_deletion_timestamp: patch
                                .pointer("/metadata/deletionTimestamp")
                                .and_then(serde_json::Value::as_str)
                                .is_some_and(|value| !value.is_empty()),
                        }
                    }
                    klights_cluster_core::StorageCommand::FinalizeBoundPod {
                        namespace,
                        name,
                        pod_uid,
                        node_name,
                        observed_resource_version,
                    } => IntegrationPodOutboxCommand::FinalizeBoundPod {
                        namespace,
                        name,
                        pod_uid,
                        node_name,
                        observed_resource_version,
                    },
                    _ => IntegrationPodOutboxCommand::Other,
                };
                IntegrationClaimedPodOutbox {
                operation: row.operation().to_string(),
                pod_uid: row.subject().pod_uid().to_string(),
                command,
            }})
        })
}

#[derive(Clone, Copy)]
pub enum IntegrationDeferredRuntimeFinalizerOutcome {
    Deleted,
    Pending,
    Error,
}

struct IntegrationFixedDeletionFinalizer {
    outcome: IntegrationDeferredRuntimeFinalizerOutcome,
}

#[async_trait::async_trait]
impl crate::kubelet::pod_runtime::deletion_finalizer::PodDeletionFinalizer
    for IntegrationFixedDeletionFinalizer
{
    async fn finalize_after_actor_cleanup(
        &self,
        _key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
    ) -> anyhow::Result<klights_kubelet::runtime_types::PodDeletionFinalizeResult> {
        use klights_kubelet::runtime_types::PodDeletionFinalizeResult;
        match self.outcome {
            IntegrationDeferredRuntimeFinalizerOutcome::Deleted => {
                Ok(PodDeletionFinalizeResult::DeletedOrAlreadyGone)
            }
            IntegrationDeferredRuntimeFinalizerOutcome::Pending => {
                Ok(PodDeletionFinalizeResult::FinalizersPending)
            }
            IntegrationDeferredRuntimeFinalizerOutcome::Error => {
                anyhow::bail!("injected finalizer error")
            }
        }
    }
}

pub async fn run_deferred_runtime_cleanup_case(
    uid: &str,
    outcome: IntegrationDeferredRuntimeFinalizerOutcome,
) -> (bool, bool) {
    use crate::kubelet::pod_runtime::deletion_finalizer::PodDeletionFinalizer as _;
    let deferred = crate::kubelet::pod_repository::status::DeferredRuntimeReducerHandle::default();
    deferred.insert_marker(uid);
    let finalizer = crate::kubelet::pod_repository::DeferredRuntimeCleanupFinalizer::new(
        Arc::new(IntegrationFixedDeletionFinalizer { outcome }),
        deferred.clone(),
    );
    let result = finalizer
        .finalize_after_actor_cleanup(&crate::kubelet::pod_runtime::service::PodRuntimeKey::new(
            "default",
            "deferred-runtime",
            uid,
        ))
        .await;
    (result.is_ok(), !deferred.contains(uid))
}

enum IntegrationStatusRaceMode {
    Scheduler,
    Probe {
        conflicts_remaining: std::sync::atomic::AtomicUsize,
    },
}

struct IntegrationStatusRaceWriter {
    store: Arc<crate::kubelet::pod_repository::store::PodStore>,
    attempts: std::sync::atomic::AtomicUsize,
    mode: IntegrationStatusRaceMode,
}

#[async_trait::async_trait]
impl crate::kubelet::pod_repository::state_only_writer::StateOnlyWriter
    for IntegrationStatusRaceWriter
{
    async fn write_status(
        &self,
        namespace: &str,
        name: &str,
        status: serde_json::Value,
        expected_resource_version: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        let attempt = self
            .attempts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        let inject = match &self.mode {
            IntegrationStatusRaceMode::Scheduler => attempt == 1,
            IntegrationStatusRaceMode::Probe {
                conflicts_remaining,
            } => conflicts_remaining
                .fetch_update(
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                    |remaining| remaining.checked_sub(1),
                )
                .is_ok(),
        };
        if inject {
            let current = self.store.get(namespace, name).await?.expect("race pod");
            let mut raced = current.data.as_ref().clone();
            match self.mode {
                IntegrationStatusRaceMode::Scheduler => {
                    raced["spec"]["nodeName"] = serde_json::json!("dp")
                }
                IntegrationStatusRaceMode::Probe { .. } => {
                    if raced
                        .pointer("/metadata/annotations")
                        .and_then(serde_json::Value::as_object)
                        .is_none()
                    {
                        raced["metadata"]["annotations"] = serde_json::json!({});
                    }
                    raced["metadata"]["annotations"]["klights.dev/probe-readiness-race-attempt"] =
                        serde_json::json!(attempt.to_string());
                }
            }
            self.store
                .update(namespace, name, raced, current.resource_version)
                .await?;
            return Err(anyhow::Error::new(
                klights_pod_api::PodRepositoryError::conflict("injected status race"),
            ));
        }
        self.store
            .integration_update_status(namespace, name, status, expected_resource_version)
            .await
    }
}

struct IntegrationNoopPodMutationReconcile;

impl klights_reconcile_api::PodMutationReconcileSink for IntegrationNoopPodMutationReconcile {
    fn reconcile_pod_mutation(
        &self,
        _request: klights_reconcile_api::PodMutationReconcileRequest,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'_> {
        Box::pin(async { Ok(()) })
    }
}

async fn integration_status_race_service(
    pod_name: &str,
    pod: serde_json::Value,
    mode: IntegrationStatusRaceMode,
) -> (
    crate::kubelet::pod_repository::status::PodStatusService,
    Arc<IntegrationStatusRaceWriter>,
    crate::datastore::Resource,
) {
    let sqlite = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .unwrap();
    let db: crate::datastore::DatastoreHandle = Arc::new(sqlite);
    let store = Arc::new(crate::pod_repository_composition::new_pod_store(db));
    let created = store.create("default", pod_name, pod).await.unwrap();
    let writer = Arc::new(IntegrationStatusRaceWriter {
        store: store.clone(),
        attempts: std::sync::atomic::AtomicUsize::new(0),
        mode,
    });
    let service = crate::kubelet::pod_repository::status::PodStatusService::new(
        store,
        writer.clone(),
        Arc::new(IntegrationNoopPodMutationReconcile),
        None,
        None,
        crate::kubelet::context::HostIpState::default(),
        Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
    );
    (service, writer, created)
}

pub async fn run_scheduler_status_race(
    pod: serde_json::Value,
    update: crate::kubelet::pod_repository::PodStatusUpdate,
) -> IntegrationStatusRaceOutcome {
    let (service, writer, _) = integration_status_race_service(
        "scheduled-race",
        pod,
        IntegrationStatusRaceMode::Scheduler,
    )
    .await;
    let result = service
        .integration_set_pod_status("default", "scheduled-race", &update, None)
        .await;
    let conflict = result
        .as_ref()
        .err()
        .is_some_and(klights_cluster_datastore::errors::is_conflict_error);
    IntegrationStatusRaceOutcome {
        attempts: writer.attempts.load(std::sync::atomic::Ordering::SeqCst),
        conflict,
        resource: result.ok(),
    }
}

pub async fn run_probe_readiness_status_race(
    pod_name: &str,
    pod: serde_json::Value,
    conflicts: usize,
    pin_resource_version: bool,
) -> IntegrationStatusRaceOutcome {
    let (service, writer, created) = integration_status_race_service(
        pod_name,
        pod,
        IntegrationStatusRaceMode::Probe {
            conflicts_remaining: std::sync::atomic::AtomicUsize::new(conflicts),
        },
    )
    .await;
    let result = service
        .integration_set_probe_readiness(
            "default",
            pod_name,
            "c",
            true,
            pin_resource_version.then_some(created.resource_version),
        )
        .await;
    let conflict = result
        .as_ref()
        .err()
        .is_some_and(klights_cluster_datastore::errors::is_conflict_error);
    IntegrationStatusRaceOutcome {
        attempts: writer.attempts.load(std::sync::atomic::Ordering::SeqCst),
        conflict,
        resource: result.ok(),
    }
}

pub struct IntegrationPodWatchRunnerFixture {
    runner: crate::kubelet::pod_repository::background::PodWatchRunner,
}

pub struct IntegrationPodNetworkFixture {
    stores: Option<Arc<crate::datastore::node_local::NodeLocalStores>>,
    service: crate::kubelet::pod_repository::network::PodNetworkService,
}

impl IntegrationPodNetworkFixture {
    pub fn with_cache_and_waiter(
        cache: Arc<dyn klights_node_store::PodNetworkCache>,
        waiter: Arc<dyn klights_network_api::PodNetworkAssignmentWaiter>,
    ) -> Self {
        Self {
            stores: None,
            service: crate::kubelet::pod_repository::network::PodNetworkService::new(
                cache,
                Arc::new(klights_supervisor::TaskSupervisor::new(
                    klights_supervisor::TaskCategoryConfig::default(),
                )),
                waiter,
                crate::kubelet::context::HostIpState::default(),
            ),
        }
    }

    pub async fn node_local_with_waiter(
        waiter: Arc<dyn klights_network_api::PodNetworkAssignmentWaiter>,
    ) -> Self {
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let stores = Arc::new(
            crate::datastore::node_local::selector::open_node_local(
                crate::datastore::backend_kind::BackendKind::Sqlite,
                None,
                supervisor.clone(),
                None,
                "sqlite:pod-network-integration",
            )
            .await
            .expect("Pod network integration store"),
        );
        let service = crate::kubelet::pod_repository::network::PodNetworkService::new(
            stores.pod_network_cache(),
            supervisor,
            waiter,
            crate::kubelet::context::HostIpState::default(),
        );
        Self {
            stores: Some(stores),
            service,
        }
    }

    pub async fn reserve_assignment(
        &self,
        sandbox_id: &str,
        pod_name: &str,
        pod_uid: &str,
        veth_host: &str,
        netns_path: &str,
    ) -> anyhow::Result<()> {
        let stores = self.stores.as_ref().expect("node-local network fixture");
        stores
            .pod_ipam()
            .reserve_ip_and_insert_network(
                klights_node_store::PodNetworkAllocationRequest::try_new(
                    sandbox_id,
                    klights_types::PodIdentity::new("default", pod_name, pod_uid),
                    0x0a2a_0000,
                    256,
                    veth_host,
                    netns_path,
                )?,
            )
            .await
            .map(|_| ())
            .map_err(anyhow::Error::from)
    }

    pub async fn read_assignment(
        &self,
        sandbox_id: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        host_network: bool,
    ) -> anyhow::Result<crate::kubelet::pod_repository::PodNetworkAssignment> {
        self.service
            .read_pod_network_assignment(sandbox_id, namespace, pod_name, pod_uid, host_network)
            .await
    }
}

impl IntegrationPodWatchRunnerFixture {
    pub fn new() -> Self {
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        Self {
            runner: crate::kubelet::pod_repository::background::PodWatchRunner::new(supervisor),
        }
    }

    pub fn started(&self) -> bool {
        self.runner
            .started
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn start(&self) {
        self.runner.start();
    }
}

pub struct IntegrationDeadlineTimerRunnerFixture {
    runner: crate::kubelet::pod_repository::background::DeadlineTimerRunner,
}

impl IntegrationDeadlineTimerRunnerFixture {
    pub fn new() -> Self {
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        Self {
            runner: crate::kubelet::pod_repository::background::DeadlineTimerRunner::new(
                supervisor,
            ),
        }
    }

    pub fn schedule_uid_bound_wakeup(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        delay_ms: u64,
        reason: &'static str,
    ) {
        self.runner
            .schedule_uid_bound_wakeup(namespace, name, uid, delay_ms, reason);
    }
}

pub struct IntegrationPodStoreFixture {
    _sqlite: crate::datastore::sqlite::Datastore,
    store: Arc<crate::kubelet::pod_repository::store::PodStore>,
}

impl IntegrationPodStoreFixture {
    pub async fn new() -> Self {
        let sqlite = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .expect("Pod store integration fixture");
        let datastore: crate::datastore::DatastoreHandle = Arc::new(sqlite.clone());
        let store = Arc::new(crate::pod_repository_composition::new_pod_store(datastore));
        Self {
            _sqlite: sqlite,
            store,
        }
    }

    pub async fn seed_pod(
        &self,
        namespace: &str,
        name: &str,
        pod: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.store.create(namespace, name, pod).await
    }

    pub async fn read_pod(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        self.store.get(namespace, name).await
    }

    pub async fn list_pods(
        &self,
        namespace: Option<&str>,
        label_selector: Option<&str>,
    ) -> anyhow::Result<crate::kubelet::pod_repository::PodResourceList> {
        self.store
            .list(namespace, label_selector, None, None, None)
            .await
    }

    pub async fn list_pods_by_owner_uid(
        &self,
        namespace: &str,
        owner_uid: &str,
    ) -> anyhow::Result<Vec<crate::datastore::Resource>> {
        self.store
            .integration_list_by_owner(namespace, owner_uid)
            .await
    }

    pub async fn mark_pod_deleting_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        deletion_body: &serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.store
            .integration_mark_deleting_latest(namespace, name, uid, deletion_body)
            .await
    }

    pub async fn update_pod(
        &self,
        namespace: &str,
        name: &str,
        pod: serde_json::Value,
        expected_resource_version: i64,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.store
            .update(namespace, name, pod, expected_resource_version)
            .await
    }

    pub async fn update_pod_status(
        &self,
        namespace: &str,
        name: &str,
        status: serde_json::Value,
        expected_resource_version: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.store
            .integration_update_status(namespace, name, status, expected_resource_version)
            .await
    }

    pub async fn finalize_bound_pod_after_actor_cleanup(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<IntegrationBoundPodDeleteOutcome> {
        let outcome = self
            .store
            .finalize_bound_with_uid(namespace, name, uid)
            .await?;
        Ok(map_bound_delete_outcome(outcome))
    }

    pub async fn delete_unscheduled_pod_with_uid_and_observed_resource_version(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        observed_resource_version: i64,
    ) -> anyhow::Result<klights_pod_api::UnscheduledPodDeletionOutcome> {
        let deletion = crate::kubelet::pod_repository::workqueue::test_leader_unscheduled_deletion(
            self.store.clone(),
        );
        deletion
            .delete_unscheduled_pod(klights_pod_api::UnscheduledPodDeletionRequest::try_new(
                klights_types::PodIdentity::new(namespace, name, uid),
                observed_resource_version,
            )?)
            .await
            .map_err(anyhow::Error::new)
    }
}

fn map_bound_delete_outcome(
    outcome: crate::kubelet::pod_repository::store::BoundPodDeleteOutcome,
) -> IntegrationBoundPodDeleteOutcome {
    match outcome {
        crate::kubelet::pod_repository::store::BoundPodDeleteOutcome::Removed => {
            IntegrationBoundPodDeleteOutcome::Removed
        }
        crate::kubelet::pod_repository::store::BoundPodDeleteOutcome::IdentityChanged => {
            IntegrationBoundPodDeleteOutcome::IdentityChanged
        }
        crate::kubelet::pod_repository::store::BoundPodDeleteOutcome::FinalizersPending => {
            IntegrationBoundPodDeleteOutcome::FinalizersPending
        }
        crate::kubelet::pod_repository::store::BoundPodDeleteOutcome::Retry => {
            IntegrationBoundPodDeleteOutcome::Retry
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntegrationPodDeleteCasRaceKind {
    SchedulerBind,
    StatusUpdate,
}

pub struct IntegrationUnscheduledPodDeleteCasRaceOutcome {
    pub disposition: klights_pod_api::UnscheduledPodDeletionOutcome,
    pub raced: bool,
    pub created_resource_version: i64,
    pub live: crate::datastore::Resource,
}

pub struct IntegrationBoundPodDeleteCasRaceOutcome {
    pub disposition: IntegrationBoundPodDeleteOutcome,
    pub raced: bool,
    pub created_resource_version: i64,
    pub live: crate::datastore::Resource,
}

struct IntegrationPodDeleteCasRacingProposal {
    inner: crate::datastore::DatastoreHandle,
    pod_name: String,
    race: IntegrationPodDeleteCasRaceKind,
    raced: Arc<std::sync::atomic::AtomicBool>,
}

impl IntegrationPodDeleteCasRacingProposal {
    fn targets_pod_delete(&self, command: &klights_cluster_core::StorageCommand) -> bool {
        matches!(
            command,
            klights_cluster_core::StorageCommand::DeleteResource {
                api_version,
                kind,
                namespace,
                name,
                ..
            } if api_version == "v1"
                && kind == "Pod"
                && namespace.as_deref() == Some("default")
                && name == &self.pod_name
        )
    }

    async fn inject_race(&self) -> anyhow::Result<()> {
        let current = self
            .inner
            .get_resource("v1", "Pod", Some("default"), &self.pod_name)
            .await?
            .expect("CAS race target Pod exists");
        match self.race {
            IntegrationPodDeleteCasRaceKind::SchedulerBind => {
                let mut body = (*current.data).clone();
                body["spec"]["nodeName"] = serde_json::json!("node-bound-by-scheduler");
                self.inner
                    .update_main_resource_with_preconditions(
                        "v1",
                        "Pod",
                        Some("default"),
                        &self.pod_name,
                        body,
                        crate::datastore::ResourcePreconditions {
                            uid: Some(current.uid),
                            resource_version: Some(current.resource_version),
                        },
                    )
                    .await?;
            }
            IntegrationPodDeleteCasRaceKind::StatusUpdate => {
                self.inner
                    .update_status_only_with_preconditions(
                        "v1",
                        "Pod",
                        Some("default"),
                        &self.pod_name,
                        serde_json::json!({
                            "phase": "Running",
                            "podIP": "10.42.0.77",
                            "raceBump": true
                        }),
                        crate::datastore::ResourcePreconditions::uid(current.uid),
                    )
                    .await?;
            }
        }
        self.raced.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    async fn apply(
        &self,
        command: klights_cluster_core::StorageCommand,
        idempotency_key: &str,
        operation: klights_kubelet::outbox::OutboxOperation,
        authoring_node: &str,
    ) -> Result<
        klights_replication::proposal::RaftProposalEffect,
        klights_cluster_core::OutboxApplyError,
    > {
        if self.targets_pod_delete(&command) {
            self.inject_race().await.map_err(|error| {
                klights_cluster_core::OutboxApplyError::Retryable(error.to_string())
            })?;
        }
        crate::bootstrap::outbox_apply_adapter::propose_outbox_command_on_backend(
            self.inner.as_ref(),
            idempotency_key,
            operation,
            command,
            authoring_node,
            None,
        )
        .await
    }
}

#[async_trait::async_trait]
impl klights_replication::proposal::RaftProposal for IntegrationPodDeleteCasRacingProposal {
    async fn propose_command(
        &self,
        command: klights_cluster_core::StorageCommand,
    ) -> anyhow::Result<klights_cluster_store::StorageCommandResult> {
        let effect = self
            .apply(
                command,
                &format!("delete-cas-race-{}", uuid::Uuid::new_v4()),
                klights_kubelet::outbox::OutboxOperation::PodStatus,
                "delete-cas-race-leader",
            )
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let (result, resource_effect, pod_endpoint_effect, committed_resource) =
            effect.into_parts();
        let applied_resource_version = match result {
            klights_cluster_core::OutboxApplyOutcome::Applied { applied_rv } => Some(applied_rv),
            klights_cluster_core::OutboxApplyOutcome::AlreadyApplied { applied_rv } => applied_rv,
        };
        Ok(klights_cluster_store::StorageCommandResult::new(
            applied_resource_version,
            None,
            None,
            resource_effect == klights_cluster_core::ResourceMutationEffect::Changed,
            committed_resource.map(klights_cluster_store::AppliedMutation::Resource),
            pod_endpoint_effect,
        ))
    }

    async fn propose_outbox_command(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: klights_cluster_core::StorageCommand,
        authoring_node: &str,
        _watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> Result<klights_cluster_core::OutboxApplyOutcome, klights_cluster_core::OutboxApplyError>
    {
        let operation =
            klights_kubelet::outbox::OutboxOperation::try_from(operation).map_err(|error| {
                klights_cluster_core::OutboxApplyError::Retryable(error.to_string())
            })?;
        Ok(self
            .apply(command, idempotency_key, operation, authoring_node)
            .await?
            .into_parts()
            .0)
    }
}

async fn integration_pod_delete_cas_race_store(
    pod_name: &str,
    race: IntegrationPodDeleteCasRaceKind,
) -> (
    Arc<crate::kubelet::pod_repository::store::PodStore>,
    crate::datastore::DatastoreHandle,
    Arc<std::sync::atomic::AtomicBool>,
) {
    let sqlite = crate::datastore::sqlite::Datastore::new_in_memory()
        .await
        .expect("delete CAS race datastore");
    let inner: crate::datastore::DatastoreHandle = Arc::new(sqlite);
    let raced = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let proposal = Arc::new(IntegrationPodDeleteCasRacingProposal {
        inner: inner.clone(),
        pod_name: pod_name.to_string(),
        race,
        raced: raced.clone(),
    });
    let datastore: crate::datastore::DatastoreHandle = Arc::new(
        crate::bootstrap::sequenced_datastore::SequencedDatastore::new_with_clock(
            inner,
            proposal,
            Arc::new(klights_supervisor::SystemWallClock),
        ),
    );
    (
        Arc::new(crate::pod_repository_composition::new_pod_store(
            datastore.clone(),
        )),
        datastore,
        raced,
    )
}

pub async fn run_unscheduled_pod_delete_cas_race(
    pod_name: &str,
    pod_uid: &str,
    race: IntegrationPodDeleteCasRaceKind,
) -> anyhow::Result<IntegrationUnscheduledPodDeleteCasRaceOutcome> {
    let (store, datastore, raced) = integration_pod_delete_cas_race_store(pod_name, race).await;
    let created = store
        .create(
            "default",
            pod_name,
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": pod_name,
                    "namespace": "default",
                    "uid": pod_uid,
                    "deletionTimestamp": "2026-01-01T00:00:00Z",
                    "deletionGracePeriodSeconds": 0
                },
                "spec": {"nodeName": "", "containers": [{"name": "app", "image": "nginx:latest"}]},
                "status": {"phase": "Running"}
            }),
        )
        .await?;
    let deletion =
        crate::kubelet::pod_repository::workqueue::test_leader_unscheduled_deletion(store);
    let disposition = deletion
        .delete_unscheduled_pod(klights_pod_api::UnscheduledPodDeletionRequest::try_new(
            klights_types::PodIdentity::new("default", pod_name, pod_uid),
            created.resource_version,
        )?)
        .await
        .map_err(anyhow::Error::new)?;
    let live = datastore
        .get_resource("v1", "Pod", Some("default"), pod_name)
        .await?
        .expect("Pod survives lost unscheduled delete CAS");
    Ok(IntegrationUnscheduledPodDeleteCasRaceOutcome {
        disposition,
        raced: raced.load(std::sync::atomic::Ordering::SeqCst),
        created_resource_version: created.resource_version,
        live,
    })
}

pub async fn run_bound_pod_delete_cas_race(
    pod_name: &str,
    pod_uid: &str,
) -> anyhow::Result<IntegrationBoundPodDeleteCasRaceOutcome> {
    let (store, datastore, raced) = integration_pod_delete_cas_race_store(
        pod_name,
        IntegrationPodDeleteCasRaceKind::StatusUpdate,
    )
    .await;
    let created = store
        .create(
            "default",
            pod_name,
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": pod_name,
                    "namespace": "default",
                    "uid": pod_uid,
                    "deletionTimestamp": "2026-01-01T00:00:00Z",
                    "deletionGracePeriodSeconds": 0
                },
                "spec": {"nodeName": "worker-a", "containers": [{"name": "app", "image": "nginx:latest"}]},
                "status": {"phase": "Running"}
            }),
        )
        .await?;
    let disposition = map_bound_delete_outcome(
        store
            .finalize_bound_with_uid("default", pod_name, pod_uid)
            .await?,
    );
    let live = datastore
        .get_resource("v1", "Pod", Some("default"), pod_name)
        .await?
        .expect("Pod survives lost actor finalization CAS");
    Ok(IntegrationBoundPodDeleteCasRaceOutcome {
        disposition,
        raced: raced.load(std::sync::atomic::Ordering::SeqCst),
        created_resource_version: created.resource_version,
        live,
    })
}

impl IntegrationPodRepositoryComposition {
    pub async fn new_inline() -> Self {
        Self::new_exact(
            None,
            false,
            false,
            false,
            crate::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
            None,
            None,
        )
        .await
    }

    pub async fn new_deferred_leader() -> Self {
        Self::new_exact(
            None,
            false,
            false,
            false,
            crate::pod_repository_composition::PodSchedulingMode::DeferredMultiNodeLeader,
            None,
            None,
        )
        .await
    }

    pub async fn new_deferred_leader_with_node_outbox() -> Self {
        Self::new_exact(
            None,
            false,
            true,
            false,
            crate::pod_repository_composition::PodSchedulingMode::DeferredMultiNodeLeader,
            None,
            None,
        )
        .await
    }

    pub async fn new_deferred_leader_with_bind_gate() -> (Self, IntegrationSchedulerBindGate) {
        let gate = Arc::new(
            crate::bootstrap::composition_adapters::pod_native_adapter::SchedulerBindGateForTest::new(),
        );
        let fixture = Self::new_exact(
            None,
            false,
            false,
            false,
            crate::pod_repository_composition::PodSchedulingMode::DeferredMultiNodeLeader,
            Some(gate.clone()),
            None,
        )
        .await;
        (fixture, IntegrationSchedulerBindGate { gate })
    }

    pub async fn new_cluster_backed(
        resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    ) -> Self {
        Self::new_exact(
            Some(resource_query),
            false,
            false,
            false,
            crate::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
            None,
            None,
        )
        .await
    }

    pub async fn new_with_node_outbox() -> Self {
        Self::new_exact(
            None,
            false,
            true,
            false,
            crate::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
            None,
            None,
        )
        .await
    }

    pub async fn new_cluster_backed_with_node_outbox(
        resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    ) -> Self {
        Self::new_exact(
            Some(resource_query),
            false,
            true,
            false,
            crate::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
            None,
            None,
        )
        .await
    }

    pub async fn new_with_status_dispatcher() -> Self {
        Self::new_exact(
            None,
            true,
            false,
            false,
            crate::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
            None,
            None,
        )
        .await
    }

    pub async fn new_with_gc_workqueue() -> Self {
        Self::new_exact(
            None,
            false,
            false,
            true,
            crate::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
            None,
            None,
        )
        .await
    }

    pub async fn new_with_delete_side_effect_observation() -> Self {
        let observation = Arc::new(tokio::sync::Mutex::new(None));
        Self::new_exact(
            None,
            false,
            false,
            false,
            crate::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
            None,
            Some(observation),
        )
        .await
    }

    async fn new_exact(
        repository_cluster_api: Option<Arc<dyn klights_leader_api::LeaderResourceQuery>>,
        with_dispatcher: bool,
        with_outbox: bool,
        with_workqueue: bool,
        scheduling_mode: crate::pod_repository_composition::PodSchedulingMode,
        scheduler_bind_gate: Option<Arc<crate::bootstrap::composition_adapters::pod_native_adapter::SchedulerBindGateForTest>>,
        delete_observation: Option<Arc<tokio::sync::Mutex<Option<(bool, bool)>>>>,
    ) -> Self {
        Self::new_exact_on(
            None,
            repository_cluster_api,
            with_dispatcher,
            with_outbox,
            with_workqueue,
            scheduling_mode,
            scheduler_bind_gate,
            delete_observation,
        )
        .await
    }

    async fn new_exact_on(
        sqlite: Option<crate::datastore::sqlite::Datastore>,
        repository_cluster_api: Option<Arc<dyn klights_leader_api::LeaderResourceQuery>>,
        with_dispatcher: bool,
        with_outbox: bool,
        with_workqueue: bool,
        scheduling_mode: crate::pod_repository_composition::PodSchedulingMode,
        scheduler_bind_gate: Option<Arc<crate::bootstrap::composition_adapters::pod_native_adapter::SchedulerBindGateForTest>>,
        delete_observation: Option<Arc<tokio::sync::Mutex<Option<(bool, bool)>>>>,
    ) -> Self {
        let sqlite = match sqlite {
            Some(sqlite) => sqlite,
            None => crate::datastore::sqlite::Datastore::new_in_memory()
                .await
                .expect("Pod repository integration composition"),
        };
        let db: crate::datastore::DatastoreHandle = Arc::new(sqlite.clone());
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let local_client = Arc::new(
            crate::control_plane::client::local::LocalApiClient::new_with_node_lease_tracker_and_passive_reads(
                db.clone(),
                crate::datastore::selector::sqlite_passive_read_ports(&sqlite),
                "pod-repository-composition".to_string(),
                Arc::new(klights_controllers::node_lease::NodeLeaseTracker::new_at(
                    chrono::Utc::now(),
                )),
                crate::control_plane::client::local::always_leader_watch(),
                klights_supervisor::FileProcessExecutor::new(supervisor.clone()),
            ),
        );
        let local_query: Arc<dyn klights_leader_api::LeaderResourceQuery> = local_client.clone();
        let native_resource_query = repository_cluster_api.clone().unwrap_or(local_query);
        let metrics = klights_controllers::side_effects::SideEffectMetrics::new();
        let controller_dispatcher = with_dispatcher.then(|| {
            Arc::new(
                klights_controllers::ControllerDispatcher::with_task_supervisor(
                    Arc::new(klights_controllers::service::ServiceIpam::new(
                        "10.43.128.0/17",
                    )),
                    supervisor.clone(),
                ),
            )
        });
        let mut side_effect_registry = if with_dispatcher {
            crate::bootstrap::side_effects::default_registry(
                metrics.clone(),
                None,
                Some(supervisor.clone()),
                Some(db.clone()),
                crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity(),
            )
        } else {
            klights_controllers::side_effects::SideEffectRegistry::new()
        };
        if let Some(observed) = &delete_observation {
            side_effect_registry.register(
                "v1",
                "Pod",
                Arc::new(IntegrationRecordingPodDeleteHook {
                    db: db.clone(),
                    observed: observed.clone(),
                }),
                klights_controllers::side_effects::ErrorPolicy::Fail,
            );
        }
        let side_effects = Arc::new(side_effect_registry);
        if let Some(dispatcher) = &controller_dispatcher {
            side_effects.set_controller_dispatcher(dispatcher.clone());
        }
        let node_local = if with_outbox || with_workqueue {
            Some(Arc::new(
                crate::datastore::node_local::selector::open_node_local(
                    crate::datastore::backend_kind::BackendKind::Sqlite,
                    None,
                    supervisor.clone(),
                    None,
                    "sqlite:pod-repository-outbox-integration",
                )
                .await
                .expect("Pod repository outbox node-local store"),
            ))
        } else {
            None
        };
        let outbox = with_outbox.then(|| {
            let stores = node_local.as_ref().expect("node outbox fixture");
            let ports = klights_kubelet::node_outbox::OutboxStores::new(
                stores.outbox_producer(),
                stores.outbox_dispatcher(),
                stores.pod_status_checkpoints(),
                stores.runtime_observation_checkpoints(),
                stores.outbox_status_stamps(),
            );
            Arc::new(klights_kubelet::node_outbox::Outbox::compose(
                ports,
                crate::bootstrap::composition_adapters::outbox_payload_codec_adapter::new_codec(),
                Arc::new(tokio::sync::Notify::new()),
                Arc::new(klights_supervisor::SystemWallClock),
            ))
        });
        let parts = crate::pod_repository_composition::build_integration_pod_repository_parts(
            crate::pod_repository_composition::PodRepositoryBuildConfig {
                db: db.clone(),
                pod_workqueue_store: with_workqueue.then(|| node_local.as_ref().expect("GC workqueue fixture").pod_workqueue()),
                supervisor: supervisor.clone(),
                side_effects: side_effects.clone(),
                metrics,
                pod_network_cache: Arc::new(IntegrationEmptyPodNetworkCache),
                assignment_waiter: Arc::new(
                    klights_networking::PodNetworkAssignmentBus::new(),
                ),
                scheduling_mode,
                outbox,
                cluster_api: repository_cluster_api,
                controller_identity: crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity(),
                #[cfg(not(test))]
                api_identity: Arc::new(
                    crate::bootstrap::controller_adapters::system_identity_adapter::SystemIdentityGenerator,
                ),
                #[cfg(not(test))]
                gc_coordination: Arc::new(klights_controllers::ControllerCoordination::new()),
                scheduler_bind_gate,
            },
            native_resource_query,
        );
        let repository_parts = parts.repository_parts;
        let repository = Arc::new(repository_parts.repository);
        if with_dispatcher {
            side_effects.set_pod_ports(repository.clone(), repository.clone());
        }
        Self {
            _sqlite: sqlite,
            db,
            repository,
            pod_api: parts.api,
            pod_subresource: parts.subresource,
            pod_scheduling: parts.scheduling,
            supervisor,
            background: repository_parts.background,
            controller_dispatcher,
            node_local,
            outbox_delivery: with_outbox.then_some(local_client),
            delete_observation,
        }
    }

    pub fn background_is_available(&self) -> bool {
        true
    }

    pub fn workqueue_start_called(&self) -> bool {
        self.background.workqueue_start_called()
    }

    pub async fn start_background(&self) -> anyhow::Result<()> {
        self.background.start().await
    }

    pub async fn pending_reconcile_keys(&self) -> Vec<klights_reconcile_api::ReconcileKey> {
        self.controller_dispatcher
            .as_ref()
            .expect("status dispatcher fixture")
            .pending_reconcile_keys()
            .await
    }

    pub async fn enqueue_reconcile_key(&self, key: klights_reconcile_api::ReconcileKey) {
        klights_reconcile_api::ControllerDispatcherPort::enqueue_reconcile(
            self.controller_dispatcher
                .as_ref()
                .expect("status dispatcher fixture")
                .as_ref(),
            key,
        )
        .await;
    }

    pub async fn claim_next_due_outbox(
        &self,
        now_ms: i64,
        lease_ms: i64,
        lease_token: &str,
    ) -> anyhow::Result<Option<IntegrationClaimedPodOutbox>> {
        claim_pod_outbox(
            self.node_local.as_ref().expect("node outbox fixture"),
            now_ms,
            lease_ms,
            lease_token,
        )
        .await
    }

    pub async fn drain_node_outbox_to_local_leader(&self) -> anyhow::Result<()> {
        let stores = self.node_local.as_ref().expect("node outbox fixture");
        let ports = klights_kubelet::node_outbox::OutboxStores::new(
            stores.outbox_producer(),
            stores.outbox_dispatcher(),
            stores.pod_status_checkpoints(),
            stores.runtime_observation_checkpoints(),
            stores.outbox_status_stamps(),
        );
        let dispatcher = klights_kubelet::node_outbox::OutboxDispatcher::new(
            ports,
            crate::bootstrap::composition_adapters::outbox_payload_codec_adapter::new_codec(),
            self.outbox_delivery
                .as_ref()
                .expect("outbox delivery fixture")
                .clone(),
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(klights_supervisor::SystemWallClock),
        );
        loop {
            if matches!(
                dispatcher.dispatch_due_once(i64::MAX / 4).await?,
                klights_kubelet::node_outbox::DispatchOutcome::Idle { .. }
            ) {
                return Ok(());
            }
        }
    }

    pub fn active_supervised_task_count(&self) -> usize {
        self.supervisor.active_tasks(None).len()
    }

    pub async fn request_gc_pod_delete(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<()> {
        klights_reconcile_api::GcPodDeleteSink::request_gc_pod_delete(
            self.repository.as_ref(),
            klights_reconcile_api::GcPodDeleteRequest::new(klights_types::PodIdentity::new(
                namespace, name, uid,
            )),
        )
        .await
        .map_err(anyhow::Error::new)
    }

    pub async fn run_delete_side_effect_order_case(&self) -> anyhow::Result<Option<(bool, bool)>> {
        let observed = self
            .delete_observation
            .as_ref()
            .expect("delete side-effect observation fixture");
        self.seed_pod(
            "default",
            "side-effect-pod",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "side-effect-pod",
                    "namespace": "default",
                    "uid": "uid-side-effect-pod",
                    "labels": {"app": "web"},
                    "ownerReferences": [{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "rs-x", "uid": "rs-x-uid", "controller": true}]
                },
                "spec": {"containers": [{"name": "c", "image": "nginx"}]},
                "status": {"phase": "Pending"}
            }),
        )
        .await?;
        crate::kubelet::pod_repository::PodObjectWriter::delete_pod(
            self.repository.as_ref(),
            "default",
            "side-effect-pod",
        )
        .await?;
        let value = *observed.lock().await;
        Ok(value)
    }

    pub async fn claim_uid_bound_pod_work(
        &self,
    ) -> anyhow::Result<Option<IntegrationPodWorkqueueEntry>> {
        let stores = self.node_local.as_ref().expect("GC workqueue fixture");
        let row = stores
            .pod_workqueue()
            .claim_due_work(klights_node_store::DueTimeMs::try_new(i64::MAX)?)
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        Ok(row.and_then(|row| {
            let klights_node_store::PodWorkIdentity::Pod(identity) = row.identity() else {
                return None;
            };
            let payload: serde_json::Value = serde_json::from_slice(row.payload()).ok()?;
            Some(IntegrationPodWorkqueueEntry {
                namespace: identity.namespace.clone(),
                name: identity.name.clone(),
                uid: identity.uid.clone(),
                target_node: payload
                    .get("target_node")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string),
            })
        }))
    }

    pub async fn run_gc_cascade(
        &self,
        owner_uid: &str,
        owner_api_version: &str,
        owner_name: &str,
        owner_kind: &str,
        namespace: &str,
    ) -> anyhow::Result<()> {
        let coordination = klights_controllers::ControllerCoordination::new();
        klights_controllers::gc::cascade_delete_with_uid(
            self.db.as_ref(),
            owner_uid,
            owner_api_version,
            owner_name,
            owner_kind,
            Some(namespace.to_string()),
            self.repository.as_ref(),
            &crate::bootstrap::controller_adapters::gc_delete_adapter::GcNonPodFinalizationAdapter::new(self.db.clone()),
            &coordination,
        )
        .await
    }

    /// Exercises the committed outbox reducer with a fixed authenticated-node
    /// input. This is a reducer scenario, not a delivery-authentication fixture.
    pub async fn apply_uid_bound_worker_status_reducer_scenario(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        authenticated_node: &str,
        status: serde_json::Value,
    ) -> anyhow::Result<()> {
        let command = klights_cluster_core::StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some(namespace.to_string()),
            name: name.to_string(),
            status,
            expected_rv: None,
            preconditions: crate::datastore::ResourcePreconditions::uid(uid),
            observed_status_stamp: None,
        };
        let codec =
            crate::bootstrap::composition_adapters::outbox_payload_codec_adapter::new_codec();
        let payload = codec.encode(&command)?;
        let command = codec.decode(payload.as_ref())?;
        let built = self
            .db
            .build_log_apply_commit_for_outbox(
                "integration-uid-bound-worker-status",
                "PodStatus",
                command,
                authenticated_node,
            )
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let klights_cluster_core::BuildOutboxOutcome::NeedsPropose { commit, .. } = built else {
            anyhow::bail!("expected fresh UID-bound worker status commit");
        };
        self.db.apply_log_apply_commit(commit).await?;
        Ok(())
    }

    pub async fn seed_pod(
        &self,
        namespace: &str,
        name: &str,
        pod: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.repository
            .integration_seed_pod(namespace, name, pod)
            .await
    }

    pub async fn seed_mutating_webhook_configuration(
        &self,
        name: &str,
        configuration: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.db
            .create_resource(
                "admissionregistration.k8s.io/v1",
                "MutatingWebhookConfiguration",
                None,
                name,
                configuration,
            )
            .await
    }

    pub async fn read_pod(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        self.repository.integration_read_pod(namespace, name).await
    }

    pub async fn update_pod(
        &self,
        namespace: &str,
        name: &str,
        pod: serde_json::Value,
        expected_resource_version: i64,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.repository
            .integration_update_pod(namespace, name, pod, expected_resource_version)
            .await
    }

    pub async fn update_pod_status(
        &self,
        namespace: &str,
        name: &str,
        status: serde_json::Value,
        expected_resource_version: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.repository
            .integration_update_pod_status(namespace, name, status, expected_resource_version)
            .await
    }

    pub async fn finalize_bound_pod_after_actor_cleanup(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<IntegrationBoundPodDeleteOutcome> {
        let outcome = self
            .repository
            .integration_finalize_bound_pod(namespace, name, uid)
            .await?;
        Ok(map_bound_delete_outcome(outcome))
    }

    pub async fn finalize_pod_deletion_after_actor_cleanup(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<IntegrationPodFinalizationOutcome> {
        integration_finalize_pod_after_actor_cleanup(self.repository.as_ref(), namespace, name, uid)
            .await
    }

    pub fn has_deferred_runtime_for_uid(&self, pod_uid: &str) -> bool {
        self.repository
            .integration_has_deferred_runtime_for_uid(pod_uid)
    }

    pub async fn api_create_pod(
        &self,
        request: crate::kubelet::pod_repository::PodApiCreateRequest,
    ) -> Result<
        crate::kubelet::pod_repository::PodApiCreateResult,
        klights_pod_api::PodRepositoryError,
    > {
        use klights_pod_api::PodApiMutation as _;
        let result = self
            .pod_api
            .create_pod(klights_pod_api::PodApiCreateRequest {
                namespace: request.namespace,
                body: request.body,
                dry_run: request.dry_run,
            })
            .await?;
        Ok(crate::kubelet::pod_repository::PodApiCreateResult {
            resource: result.resource,
            body: result.body,
        })
    }

    pub async fn api_update_pod(
        &self,
        namespace: &str,
        name: &str,
        body: serde_json::Value,
        current: crate::datastore::Resource,
        dry_run: bool,
    ) -> Result<
        crate::kubelet::pod_repository::PodApiUpdateOutcome,
        klights_pod_api::PodRepositoryError,
    > {
        use klights_pod_api::PodApiMutation as _;
        match self
            .pod_api
            .update_pod(klights_pod_api::PodApiUpdateRequest {
                namespace: namespace.to_string(),
                name: name.to_string(),
                body,
                current,
                dry_run,
            })
            .await?
        {
            klights_pod_api::PodApiWriteOutcome::Persisted(resource) => {
                Ok(crate::kubelet::pod_repository::PodApiUpdateOutcome::Persisted(resource))
            }
            klights_pod_api::PodApiWriteOutcome::DryRun(value) => Ok(
                crate::kubelet::pod_repository::PodApiUpdateOutcome::DryRun(value),
            ),
        }
    }

    pub async fn api_patch_pod(
        &self,
        namespace: &str,
        name: &str,
        patch: serde_json::Value,
        patch_type: crate::kubelet::pod_repository::PodStatusPatchType,
        dry_run: bool,
    ) -> Result<
        crate::kubelet::pod_repository::PodApiUpdateOutcome,
        klights_pod_api::PodRepositoryError,
    > {
        use klights_pod_api::PodApiMutation as _;
        match self
            .pod_api
            .patch_pod(klights_pod_api::PodApiPatchRequest {
                namespace: namespace.to_string(),
                name: name.to_string(),
                patch,
                patch_kind: integration_pod_patch_kind(patch_type),
                dry_run,
            })
            .await?
        {
            klights_pod_api::PodApiWriteOutcome::Persisted(resource) => {
                Ok(crate::kubelet::pod_repository::PodApiUpdateOutcome::Persisted(resource))
            }
            klights_pod_api::PodApiWriteOutcome::DryRun(value) => Ok(
                crate::kubelet::pod_repository::PodApiUpdateOutcome::DryRun(value),
            ),
        }
    }

    pub async fn api_delete_pod<O>(
        &self,
        namespace: &str,
        name: &str,
        options: O,
        dry_run: bool,
    ) -> Result<
        crate::kubelet::pod_repository::PodApiDeleteOutcome,
        klights_pod_api::PodRepositoryError,
    >
    where
        O: Into<klights_pod_api::PodDeleteOptions> + Send,
    {
        use klights_pod_api::PodApiMutation as _;
        match self
            .pod_api
            .delete_pod(klights_pod_api::PodApiDeleteRequest {
                namespace: namespace.to_string(),
                name: name.to_string(),
                options: options.into(),
                dry_run,
            })
            .await?
        {
            klights_pod_api::PodApiDeleteOutcome::GracefulSet(resource) => {
                Ok(crate::kubelet::pod_repository::PodApiDeleteOutcome::GracefulSet(resource))
            }
            klights_pod_api::PodApiDeleteOutcome::DryRun(value) => Ok(
                crate::kubelet::pod_repository::PodApiDeleteOutcome::DryRun(value),
            ),
        }
    }

    pub async fn api_delete_collection_pods(
        &self,
        namespace: &str,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        dry_run: bool,
    ) -> Result<(), klights_pod_api::PodRepositoryError> {
        use klights_pod_api::PodApiMutation as _;
        self.pod_api
            .delete_collection_pods(klights_pod_api::PodApiDeleteCollectionRequest {
                namespace: namespace.to_string(),
                label_selector: label_selector.map(str::to_string),
                field_selector: field_selector.map(str::to_string),
                dry_run,
            })
            .await
    }

    pub async fn ordinary_mark_pod_terminating(
        &self,
        request: klights_pod_api::PodMarkTerminatingRequest,
    ) -> Result<crate::datastore::Resource, klights_pod_api::PodRepositoryError> {
        let target = request.into_target();
        let options = target
            .uid()
            .map(k8s_native_service::DeleteOptions::with_uid_precondition)
            .unwrap_or_default();
        match self
            .api_delete_pod(target.namespace(), target.name(), options, false)
            .await?
        {
            crate::kubelet::pod_repository::PodApiDeleteOutcome::GracefulSet(resource) => {
                Ok(resource)
            }
            crate::kubelet::pod_repository::PodApiDeleteOutcome::DryRun(_) => {
                unreachable!("ordinary mark is never dry-run")
            }
        }
    }

    pub async fn schedule_all_unbound_pods(
        &self,
    ) -> Result<(), klights_pod_api::PodRepositoryError> {
        self.pod_scheduling.schedule_all_unbound_pods().await
    }

    pub async fn replace_status_from_api(
        &self,
        namespace: &str,
        name: &str,
        status: serde_json::Value,
        expected_resource_version: i64,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.pod_subresource
            .replace_status(klights_pod_api::PodStatusReplaceRequest {
                namespace: namespace.to_string(),
                name: name.to_string(),
                expected_uid: None,
                status,
                expected_resource_version,
            })
            .await
            .map_err(anyhow::Error::new)
    }

    pub async fn replace_status_from_api_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        status: serde_json::Value,
        expected_resource_version: i64,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.pod_subresource
            .replace_status(klights_pod_api::PodStatusReplaceRequest {
                namespace: namespace.to_string(),
                name: name.to_string(),
                expected_uid: Some(uid.to_string()),
                status,
                expected_resource_version,
            })
            .await
            .map_err(anyhow::Error::new)
    }

    pub async fn patch_status_from_api(
        &self,
        namespace: &str,
        name: &str,
        patch: serde_json::Value,
        patch_type: crate::kubelet::pod_repository::PodStatusPatchType,
        expected_resource_version: i64,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.pod_subresource
            .patch_status(klights_pod_api::PodStatusPatchRequest {
                namespace: namespace.to_string(),
                name: name.to_string(),
                patch,
                patch_kind: integration_pod_patch_kind(patch_type),
                expected_resource_version: Some(expected_resource_version),
            })
            .await
            .map_err(anyhow::Error::new)
    }

    pub async fn update_ephemeral_containers(
        &self,
        namespace: &str,
        name: &str,
        containers: Vec<serde_json::Value>,
        expected_resource_version: i64,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.pod_subresource
            .update_ephemeral_containers(klights_pod_api::PodEphemeralContainersRequest {
                namespace: namespace.to_string(),
                name: name.to_string(),
                containers,
                expected_resource_version,
            })
            .await
            .map_err(anyhow::Error::new)
    }

    pub async fn seed_non_pod_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        anyhow::ensure!(kind != "Pod", "Pod fixtures must use seed_pod");
        self.db
            .create_resource(api_version, kind, Some(namespace), name, value)
            .await
    }

    pub async fn seed_scheduling_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        anyhow::ensure!(kind != "Pod", "Pod fixtures must use seed_pod");
        self.db
            .create_resource(api_version, kind, namespace, name, value)
            .await
    }

    pub async fn list_scheduling_resources(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
    ) -> anyhow::Result<crate::datastore::ResourceList> {
        anyhow::ensure!(kind != "Pod", "Pod fixtures must use list_pods");
        self.db
            .list_resources(
                api_version,
                kind,
                namespace,
                crate::datastore::ResourceListQuery::all(),
            )
            .await
    }

    pub async fn pod_watch_events_since(
        &self,
        resource_version: i64,
    ) -> anyhow::Result<Vec<IntegrationPodWatchEvent>> {
        self.db
            .list_watch_events_since(
                &[crate::datastore::WatchTarget::namespaced("v1", "Pod")],
                resource_version,
            )
            .await
            .map(|events| {
                events
                    .into_iter()
                    .map(|event| IntegrationPodWatchEvent {
                        event_type: event.event_type.into_owned(),
                        resource: event.resource,
                    })
                    .collect()
            })
    }

    pub async fn read_non_pod_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        anyhow::ensure!(kind != "Pod", "Pod fixtures must use read_pod");
        self.db
            .get_resource(api_version, kind, Some(namespace), name)
            .await
    }

    pub async fn seed_namespace(
        &self,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.db.create_namespace(name, value).await
    }

    pub async fn read_namespace(
        &self,
        name: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        self.db.get_namespace(name).await
    }

    pub async fn update_namespace(
        &self,
        name: &str,
        value: serde_json::Value,
        expected_resource_version: i64,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.db
            .update_namespace(name, value, expected_resource_version)
            .await
    }

    pub async fn reconcile_namespace_termination(
        &self,
        name: &str,
        now: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<()> {
        let store = crate::bootstrap::composition_adapters::api_state_adapter::RootNamespaceTerminationStore::new(
            self.db.clone(),
        );
        k8s_native_service::reconcile_namespace_termination_at(
            store.as_ref(),
            name,
            klights_controllers::side_effects::SideEffectMetrics::new().as_ref(),
            now,
        )
        .await
        .map_err(|error| anyhow::anyhow!("{error:?}"))
    }

    pub async fn reconcile_pod_disruption_budget(
        &self,
        pdb: &serde_json::Value,
        now: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<()> {
        klights_controllers::pdb::reconcile_pdb_at(self.db.as_ref(), self, pdb, now).await
    }
}

fn integration_pod_patch_kind(
    patch_type: crate::kubelet::pod_repository::PodStatusPatchType,
) -> klights_pod_api::PodStatusPatchKind {
    match patch_type {
        crate::kubelet::pod_repository::PodStatusPatchType::JsonPatch => {
            klights_pod_api::PodStatusPatchKind::JsonPatch
        }
        crate::kubelet::pod_repository::PodStatusPatchType::MergePatch => {
            klights_pod_api::PodStatusPatchKind::MergePatch
        }
        crate::kubelet::pod_repository::PodStatusPatchType::StrategicMerge => {
            klights_pod_api::PodStatusPatchKind::StrategicMerge
        }
        crate::kubelet::pod_repository::PodStatusPatchType::ApplyPatch => {
            klights_pod_api::PodStatusPatchKind::ApplyPatch
        }
    }
}

#[async_trait::async_trait]
impl crate::kubelet::pod_repository::PodReader for IntegrationPodRepositoryComposition {
    async fn get_pod(
        &self,
        ns: &str,
        name: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        crate::kubelet::pod_repository::PodReader::get_pod(self.repository.as_ref(), ns, name).await
    }

    async fn get_pod_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        crate::kubelet::pod_repository::PodReader::get_pod_for_uid(
            self.repository.as_ref(),
            ns,
            name,
            uid,
        )
        .await
    }

    async fn list_pods(
        &self,
        ns: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        limit: Option<i64>,
        continue_token: Option<&str>,
    ) -> anyhow::Result<crate::kubelet::pod_repository::PodResourceList> {
        crate::kubelet::pod_repository::PodReader::list_pods(
            self.repository.as_ref(),
            ns,
            label_selector,
            field_selector,
            limit,
            continue_token,
        )
        .await
    }

    async fn list_pods_by_owner_uid(
        &self,
        ns: &str,
        owner_uid: &str,
    ) -> anyhow::Result<Vec<crate::datastore::Resource>> {
        crate::kubelet::pod_repository::PodReader::list_pods_by_owner_uid(
            self.repository.as_ref(),
            ns,
            owner_uid,
        )
        .await
    }
}

impl klights_pod_api::PodQuery for IntegrationPodRepositoryComposition {
    fn get_pod(
        &self,
        request: klights_pod_api::PodGetRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Option<crate::datastore::Resource>> {
        klights_pod_api::PodQuery::get_pod(self.repository.as_ref(), request)
    }

    fn list_pods(
        &self,
        request: klights_pod_api::PodListRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodListResult> {
        klights_pod_api::PodQuery::list_pods(self.repository.as_ref(), request)
    }

    fn list_pods_by_owner_uid(
        &self,
        request: klights_pod_api::PodOwnerListRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Vec<crate::datastore::Resource>> {
        klights_pod_api::PodQuery::list_pods_by_owner_uid(self.repository.as_ref(), request)
    }
}

impl klights_pod_api::PodUpdate for IntegrationPodRepositoryComposition {
    fn update_pod(
        &self,
        request: klights_pod_api::PodUpdateRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, crate::datastore::Resource> {
        klights_pod_api::PodUpdate::update_pod(self.repository.as_ref(), request)
    }
}

#[async_trait::async_trait]
impl crate::kubelet::pod_repository::PodStatusWriter for IntegrationPodRepositoryComposition {
    async fn set_pod_status(
        &self,
        ns: &str,
        name: &str,
        update: crate::kubelet::pod_repository::PodStatusUpdate,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::set_pod_status(
            self.repository.as_ref(),
            ns,
            name,
            update,
            expected_rv,
        )
        .await
    }
    async fn set_pod_status_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        update: crate::kubelet::pod_repository::PodStatusUpdate,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::set_pod_status_for_uid(
            self.repository.as_ref(),
            ns,
            name,
            uid,
            update,
            expected_rv,
        )
        .await
    }
    async fn apply_runtime_reconcile_status(
        &self,
        ns: &str,
        name: &str,
        update: crate::kubelet::pod_repository::RuntimeReconcileStatus,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::apply_runtime_reconcile_status(
            self.repository.as_ref(),
            ns,
            name,
            update,
            expected_rv,
        )
        .await
    }
    async fn apply_runtime_reconcile_status_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        update: crate::kubelet::pod_repository::RuntimeReconcileStatus,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::apply_runtime_reconcile_status_for_uid(
            self.repository.as_ref(),
            ns,
            name,
            uid,
            update,
            expected_rv,
        )
        .await
    }
    async fn mark_start_pending_for_retry_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        error_message: &str,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::mark_start_pending_for_retry_for_uid(
            self.repository.as_ref(),
            ns,
            name,
            uid,
            error_message,
        )
        .await
    }
    async fn set_probe_readiness(
        &self,
        ns: &str,
        name: &str,
        container_name: &str,
        ready: bool,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::set_probe_readiness(
            self.repository.as_ref(),
            ns,
            name,
            container_name,
            ready,
            expected_rv,
        )
        .await
    }
    async fn set_probe_readiness_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        container_name: &str,
        ready: bool,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::set_probe_readiness_for_uid(
            self.repository.as_ref(),
            ns,
            name,
            uid,
            container_name,
            ready,
            expected_rv,
        )
        .await
    }
    async fn set_deadline_exceeded(
        &self,
        ns: &str,
        name: &str,
        message: String,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::set_deadline_exceeded(
            self.repository.as_ref(),
            ns,
            name,
            message,
            expected_rv,
        )
        .await
    }
    async fn set_deadline_exceeded_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        message: String,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::set_deadline_exceeded_for_uid(
            self.repository.as_ref(),
            ns,
            name,
            uid,
            message,
            expected_rv,
        )
        .await
    }
    async fn apply_ephemeral_container_statuses(
        &self,
        ns: &str,
        name: &str,
        statuses: Vec<serde_json::Value>,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::apply_ephemeral_container_statuses(
            self.repository.as_ref(),
            ns,
            name,
            statuses,
            expected_rv,
        )
        .await
    }
    async fn apply_ephemeral_container_statuses_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        statuses: Vec<serde_json::Value>,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::apply_ephemeral_container_statuses_for_uid(
            self.repository.as_ref(),
            ns,
            name,
            uid,
            statuses,
            expected_rv,
        )
        .await
    }
    async fn note_container_restart(
        &self,
        ns: &str,
        name: &str,
        container_name: &str,
        terminated: serde_json::Value,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        crate::kubelet::pod_repository::PodStatusWriter::note_container_restart(
            self.repository.as_ref(),
            ns,
            name,
            container_name,
            terminated,
            expected_rv,
        )
        .await
    }
    async fn note_container_restart_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        container_name: &str,
        terminated: serde_json::Value,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        crate::kubelet::pod_repository::PodStatusWriter::note_container_restart_for_uid(
            self.repository.as_ref(),
            ns,
            name,
            uid,
            container_name,
            terminated,
            expected_rv,
        )
        .await
    }
}

#[async_trait::async_trait]
impl crate::kubelet::pod_repository::PodMetadataWriter for IntegrationPodRepositoryComposition {
    async fn record_sandbox_id(
        &self,
        ns: &str,
        name: &str,
        sandbox_id: &str,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodMetadataWriter::record_sandbox_id(
            self.repository.as_ref(),
            ns,
            name,
            sandbox_id,
        )
        .await
    }
    async fn record_sandbox_id_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        sandbox_id: &str,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodMetadataWriter::record_sandbox_id_for_uid(
            self.repository.as_ref(),
            ns,
            name,
            uid,
            sandbox_id,
        )
        .await
    }
}

#[async_trait::async_trait]
impl crate::kubelet::pod_repository::PodObjectWriter for IntegrationPodRepositoryComposition {
    async fn create_controller_pod(
        &self,
        ns: &str,
        name: &str,
        _node_name: &str,
        pod: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.api_create_pod(crate::kubelet::pod_repository::PodApiCreateRequest {
            namespace: ns.to_string(),
            name: name.to_string(),
            body: pod,
            dry_run: false,
            run_admission: true,
        })
        .await
        .map_err(anyhow::Error::new)?
        .resource
        .ok_or_else(|| anyhow::anyhow!("controller pod {ns}/{name} create returned dry-run"))
    }
    async fn delete_pod(&self, ns: &str, name: &str) -> anyhow::Result<()> {
        crate::kubelet::pod_repository::PodObjectWriter::delete_pod(
            self.repository.as_ref(),
            ns,
            name,
        )
        .await
    }
    async fn update_pod_owner_references(
        &self,
        ns: &str,
        name: &str,
        owner_refs: Vec<serde_json::Value>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodObjectWriter::update_pod_owner_references(
            self.repository.as_ref(),
            ns,
            name,
            owner_refs,
        )
        .await
    }
    async fn update_pod_owner_references_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        owner_refs: Vec<serde_json::Value>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodObjectWriter::update_pod_owner_references_for_uid(
            self.repository.as_ref(),
            ns,
            name,
            uid,
            owner_refs,
        )
        .await
    }
    async fn merge_pod_labels(
        &self,
        ns: &str,
        name: &str,
        labels: Vec<(String, String)>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodObjectWriter::merge_pod_labels(
            self.repository.as_ref(),
            ns,
            name,
            labels,
        )
        .await
    }
    async fn merge_pod_labels_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        labels: Vec<(String, String)>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodObjectWriter::merge_pod_labels_for_uid(
            self.repository.as_ref(),
            ns,
            name,
            uid,
            labels,
        )
        .await
    }
}

#[async_trait::async_trait]
impl crate::kubelet::pod_repository::PodNetworkReader for IntegrationPodRepositoryComposition {
    async fn read_pod_network_assignment(
        &self,
        sandbox_id: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        host_network: bool,
    ) -> anyhow::Result<crate::kubelet::pod_repository::PodNetworkAssignment> {
        crate::kubelet::pod_repository::PodNetworkReader::read_pod_network_assignment(
            self.repository.as_ref(),
            sandbox_id,
            namespace,
            pod_name,
            pod_uid,
            host_network,
        )
        .await
    }
}

impl crate::kubelet::pod_repository::PodWatchSource for IntegrationPodRepositoryComposition {
    fn subscribe_pod_watch(&self) -> tokio::sync::broadcast::Receiver<klights_watch::WatchEvent> {
        crate::kubelet::pod_repository::PodWatchSource::subscribe_pod_watch(
            self.repository.as_ref(),
        )
    }
}

#[async_trait::async_trait]
impl crate::kubelet::pod_repository::PodSubresourceWriter for IntegrationPodRepositoryComposition {
    async fn replace_status_from_api(
        &self,
        ns: &str,
        name: &str,
        status: serde_json::Value,
        expected_rv: i64,
    ) -> anyhow::Result<crate::datastore::Resource> {
        IntegrationPodRepositoryComposition::replace_status_from_api(
            self,
            ns,
            name,
            status,
            expected_rv,
        )
        .await
    }

    async fn replace_status_from_api_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        status: serde_json::Value,
        expected_rv: i64,
    ) -> anyhow::Result<crate::datastore::Resource> {
        IntegrationPodRepositoryComposition::replace_status_from_api_for_uid(
            self,
            ns,
            name,
            uid,
            status,
            expected_rv,
        )
        .await
    }

    async fn patch_status_from_api(
        &self,
        ns: &str,
        name: &str,
        patch: serde_json::Value,
        patch_type: crate::kubelet::pod_repository::PodStatusPatchType,
        expected_rv: i64,
    ) -> anyhow::Result<crate::datastore::Resource> {
        IntegrationPodRepositoryComposition::patch_status_from_api(
            self,
            ns,
            name,
            patch,
            patch_type,
            expected_rv,
        )
        .await
    }

    async fn update_ephemeral_containers(
        &self,
        ns: &str,
        name: &str,
        containers: Vec<serde_json::Value>,
        expected_rv: i64,
    ) -> anyhow::Result<crate::datastore::Resource> {
        IntegrationPodRepositoryComposition::update_ephemeral_containers(
            self,
            ns,
            name,
            containers,
            expected_rv,
        )
        .await
    }

    async fn update_ephemeral_containers_for_uid(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
        containers: Vec<serde_json::Value>,
        expected_rv: i64,
    ) -> anyhow::Result<crate::datastore::Resource> {
        let current = self
            .read_pod(ns, name)
            .await?
            .ok_or_else(|| klights_pod_api::PodRepositoryError::not_found(ns, name))?;
        crate::kubelet::pod_repository::ensure_pod_uid_matches(&current.data, uid, ns, name)?;
        IntegrationPodRepositoryComposition::update_ephemeral_containers(
            self,
            ns,
            name,
            containers,
            expected_rv,
        )
        .await
    }
}

impl klights_pod_api::PodSubresourceMutation for IntegrationPodRepositoryComposition {
    fn replace_status(
        &self,
        request: klights_pod_api::PodStatusReplaceRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, crate::datastore::Resource> {
        self.pod_subresource.replace_status(request)
    }

    fn patch_status(
        &self,
        request: klights_pod_api::PodStatusPatchRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, crate::datastore::Resource> {
        self.pod_subresource.patch_status(request)
    }

    fn update_ephemeral_containers(
        &self,
        request: klights_pod_api::PodEphemeralContainersRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, crate::datastore::Resource> {
        self.pod_subresource.update_ephemeral_containers(request)
    }
}

#[async_trait::async_trait]
impl klights_controllers::pdb::PdbPodReader for IntegrationPodRepositoryComposition {
    async fn list_namespace_pods(
        &self,
        namespace: &str,
    ) -> klights_reconcile_api::ControllerStoreResult<Vec<crate::datastore::Resource>> {
        crate::kubelet::pod_repository::PodReader::list_pods(
            self,
            Some(namespace),
            None,
            None,
            None,
            None,
        )
        .await
        .map(|list| list.items)
        .map_err(|error| klights_reconcile_api::ControllerStoreError::internal(error.to_string()))
    }
}

#[derive(Default)]
struct IntegrationHarnessOptions {
    csr_signer: Option<Arc<dyn klights_auth::csr_signer::CsrSigner>>,
    task_categories: klights_supervisor::TaskCategoryConfig,
    auth_clock: Option<Arc<dyn klights_auth::clock::Clock>>,
    authority: Option<Arc<dyn klights_leader_api::LeaderAuthority>>,
    bootstrap_token_authenticator:
        Option<Arc<dyn klights_leader_api::LeaderBootstrapTokenAuthentication>>,
}

#[derive(Clone, Copy)]
enum EndpointFixtureKind {
    Pod,
    Service,
    Endpoints,
    EndpointSlice,
}

impl EndpointFixtureKind {
    const fn api_version(self) -> &'static str {
        match self {
            Self::Pod | Self::Service | Self::Endpoints => "v1",
            Self::EndpointSlice => "discovery.k8s.io/v1",
        }
    }

    const fn kind(self) -> &'static str {
        match self {
            Self::Pod => "Pod",
            Self::Service => "Service",
            Self::Endpoints => "Endpoints",
            Self::EndpointSlice => "EndpointSlice",
        }
    }
}

/// Opaque full-stack API fixture owned by the base integration-test package.
#[derive(Clone)]
pub struct NativeApiTestHarness {
    router: axum::Router,
    datastore: IntegrationDatastoreHandle,
    sqlite: crate::datastore::sqlite::Datastore,
    nodeport_alloc: Arc<klights_controllers::service::NodePortAllocator>,
    pod_repository: Arc<crate::kubelet::pod_repository::PodRepository>,
    _node_local: Arc<crate::datastore::node_local::NodeLocalStores>,
    outbox_dispatcher: Arc<klights_kubelet::node_outbox::OutboxDispatcher>,
    controller_dispatcher: Arc<klights_controllers::ControllerDispatcher>,
    crd_registry: klights_controllers::crd::CrdRegistry,
    service_routing: Arc<dyn klights_reconcile_api::ServiceRoutingSync>,
    node_metrics: Arc<IntegrationNodeMetrics>,
    operational_replication: Option<Arc<klights_replication::ReplicationService>>,
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    node_lease_tracker: Arc<klights_controllers::node_lease::NodeLeaseTracker>,
    authority: Option<Arc<dyn klights_leader_api::LeaderAuthority>>,
    node_name: String,
}

impl NativeApiTestHarness {
    pub async fn new() -> anyhow::Result<Self> {
        Self::with_authorizer(Arc::new(AllowAllAuthorizer)).await
    }

    pub async fn with_authorizer(
        authorizer: Arc<dyn klights_auth::authorizer::Authorizer>,
    ) -> anyhow::Result<Self> {
        Self::assemble(
            authorizer,
            None,
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
        )
        .await
    }

    pub async fn with_authorizer_and_operational_endpoints(
        authorizer: Arc<dyn klights_auth::authorizer::Authorizer>,
    ) -> anyhow::Result<Self> {
        Self::assemble(
            authorizer,
            None,
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            true,
        )
        .await
    }

    pub async fn with_authorizer_and_audit_sink(
        authorizer: Arc<dyn klights_auth::authorizer::Authorizer>,
        audit_sink: Arc<dyn k8s_native_service::audit::AuditSink>,
    ) -> anyhow::Result<Self> {
        Self::assemble(
            authorizer,
            None,
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            None,
            None,
            None,
            None,
            None,
            Some(audit_sink),
            None,
            false,
        )
        .await
    }

    pub async fn with_pod_lifecycle_diagnostics(
        diagnostics: Arc<dyn klights_pod_api::PodLifecycleDiagnosticsQuery>,
    ) -> anyhow::Result<Self> {
        Self::assemble(
            Arc::new(AllowAllAuthorizer),
            Some(diagnostics),
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
        )
        .await
    }

    pub async fn with_authentication_dependencies(
        signing_keys: Arc<dyn klights_leader_api::LeaderServiceAccountSigningKeyState>,
        oidc: Option<Arc<dyn klights_auth::oidc::OidcValidator>>,
        webhook: Option<Arc<dyn klights_auth::webhook_auth::WebhookAuthenticator>>,
    ) -> anyhow::Result<Self> {
        Self::assemble(
            Arc::new(AllowAllAuthorizer),
            None,
            signing_keys,
            oidc,
            webhook,
            None,
            None,
            None,
            None,
            None,
            false,
        )
        .await
    }

    pub async fn with_authenticators(
        oidc: Option<Arc<dyn klights_auth::oidc::OidcValidator>>,
        webhook: Option<Arc<dyn klights_auth::webhook_auth::WebhookAuthenticator>>,
    ) -> anyhow::Result<Self> {
        Self::with_authentication_dependencies(
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            oidc,
            webhook,
        )
        .await
    }

    pub async fn with_bootstrap_token_authenticator(
        bootstrap_token_authenticator: Arc<
            dyn klights_leader_api::LeaderBootstrapTokenAuthentication,
        >,
    ) -> anyhow::Result<Self> {
        Self::assemble_with_options(
            Arc::new(AllowAllAuthorizer),
            None,
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            IntegrationHarnessOptions {
                bootstrap_token_authenticator: Some(bootstrap_token_authenticator),
                ..Default::default()
            },
        )
        .await
    }

    pub async fn with_signing_key_pem(signing_key_pem: String) -> anyhow::Result<Self> {
        Self::with_authentication_dependencies(
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::from_pem(
                signing_key_pem,
            ),
            None,
            None,
        )
        .await
    }

    pub async fn with_auth_clock(
        clock: Arc<dyn klights_auth::clock::Clock>,
    ) -> anyhow::Result<Self> {
        Self::assemble_with_options(
            Arc::new(AllowAllAuthorizer),
            None,
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            IntegrationHarnessOptions {
                auth_clock: Some(clock),
                ..Default::default()
            },
        )
        .await
    }

    pub async fn with_leader_authority() -> anyhow::Result<Self> {
        let (authority, _publisher) =
            klights_replication::authority::WatchLeaderAuthority::channel(true, None);
        Self::assemble_with_options(
            Arc::new(AllowAllAuthorizer),
            None,
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            IntegrationHarnessOptions {
                authority: Some(authority),
                ..Default::default()
            },
        )
        .await
    }

    pub async fn with_toggle_failing_watch_history()
    -> anyhow::Result<(Self, IntegrationWatchHistoryFailureControl)> {
        let fail = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let harness = Self::assemble(
            Arc::new(AllowAllAuthorizer),
            None,
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            None,
            None,
            Some(fail.clone()),
            None,
            None,
            None,
            None,
            false,
        )
        .await?;
        Ok((harness, IntegrationWatchHistoryFailureControl { fail }))
    }

    pub async fn with_mutation_side_effect_factory<F>(factory: F) -> anyhow::Result<Self>
    where
        F: FnOnce(
                IntegrationDatastoreHandle,
            ) -> Arc<klights_controllers::side_effects::SideEffectRegistry>
            + Send
            + 'static,
    {
        Self::assemble(
            Arc::new(AllowAllAuthorizer),
            None,
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            None,
            None,
            None,
            Some(Box::new(factory)),
            None,
            None,
            None,
            false,
        )
        .await
    }

    pub async fn with_service_routing_observation()
    -> anyhow::Result<(Self, IntegrationServiceRoutingObservation)> {
        let observation = IntegrationServiceRoutingObservation::default();
        let harness = Self::assemble(
            Arc::new(AllowAllAuthorizer),
            None,
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            None,
            None,
            None,
            None,
            Some(observation.clone()),
            None,
            None,
            false,
        )
        .await?;
        Ok((harness, observation))
    }

    pub async fn with_priority_fairness() -> anyhow::Result<(
        Self,
        Arc<k8s_native_service::priority_fairness::ApiPriorityFairness>,
    )> {
        let priority_fairness =
            Arc::new(k8s_native_service::priority_fairness::ApiPriorityFairness::new());
        let harness = Self::assemble(
            Arc::new(AllowAllAuthorizer),
            None,
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            None,
            None,
            None,
            None,
            None,
            None,
            Some(priority_fairness.clone()),
            false,
        )
        .await?;
        Ok((harness, priority_fairness))
    }

    pub async fn with_csr_signer_observation()
    -> anyhow::Result<(Self, IntegrationCsrSignerObservation)> {
        let request_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let changed = Arc::new(tokio::sync::Notify::new());
        let observation = IntegrationCsrSignerObservation {
            request_count: request_count.clone(),
            changed: changed.clone(),
        };
        let harness = Self::assemble_with_options(
            Arc::new(AllowAllAuthorizer),
            None,
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            IntegrationHarnessOptions {
                csr_signer: Some(Arc::new(IntegrationRecordingCsrSigner {
                    request_count,
                    changed,
                })),
                ..Default::default()
            },
        )
        .await?;
        Ok((harness, observation))
    }

    pub async fn with_held_pod_delete_workqueue()
    -> anyhow::Result<(Self, IntegrationHeldSupervisorTask)> {
        let harness = Self::assemble_with_options(
            Arc::new(AllowAllAuthorizer),
            None,
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            IntegrationHarnessOptions {
                task_categories: klights_supervisor::TaskCategoryConfig {
                    pod_delete_workqueue: 1,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await?;
        let handle = harness
            .task_supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::PodDeleteWorkqueue,
                "hold_foreground_delete_workqueue_for_integration",
                std::future::pending(),
            )
            .await?;
        Ok((harness, IntegrationHeldSupervisorTask { handle }))
    }

    async fn assemble(
        authorizer: Arc<dyn klights_auth::authorizer::Authorizer>,
        pod_lifecycle_diagnostics: Option<Arc<dyn klights_pod_api::PodLifecycleDiagnosticsQuery>>,
        signing_keys: Arc<dyn klights_leader_api::LeaderServiceAccountSigningKeyState>,
        oidc: Option<Arc<dyn klights_auth::oidc::OidcValidator>>,
        webhook: Option<Arc<dyn klights_auth::webhook_auth::WebhookAuthenticator>>,
        watch_history_failure: Option<Arc<std::sync::atomic::AtomicBool>>,
        mutation_side_effects_factory: Option<
            Box<
                dyn FnOnce(
                        IntegrationDatastoreHandle,
                    )
                        -> Arc<klights_controllers::side_effects::SideEffectRegistry>
                    + Send,
            >,
        >,
        service_routing_observation: Option<IntegrationServiceRoutingObservation>,
        audit_sink: Option<Arc<dyn k8s_native_service::audit::AuditSink>>,
        priority_fairness: Option<Arc<k8s_native_service::priority_fairness::ApiPriorityFairness>>,
        mount_operational_endpoints: bool,
    ) -> anyhow::Result<Self> {
        Self::assemble_with_options(
            authorizer,
            pod_lifecycle_diagnostics,
            signing_keys,
            oidc,
            webhook,
            watch_history_failure,
            mutation_side_effects_factory,
            service_routing_observation,
            audit_sink,
            priority_fairness,
            mount_operational_endpoints,
            IntegrationHarnessOptions::default(),
        )
        .await
    }

    async fn assemble_with_options(
        authorizer: Arc<dyn klights_auth::authorizer::Authorizer>,
        pod_lifecycle_diagnostics: Option<Arc<dyn klights_pod_api::PodLifecycleDiagnosticsQuery>>,
        signing_keys: Arc<dyn klights_leader_api::LeaderServiceAccountSigningKeyState>,
        oidc: Option<Arc<dyn klights_auth::oidc::OidcValidator>>,
        webhook: Option<Arc<dyn klights_auth::webhook_auth::WebhookAuthenticator>>,
        watch_history_failure: Option<Arc<std::sync::atomic::AtomicBool>>,
        mutation_side_effects_factory: Option<
            Box<
                dyn FnOnce(
                        IntegrationDatastoreHandle,
                    )
                        -> Arc<klights_controllers::side_effects::SideEffectRegistry>
                    + Send,
            >,
        >,
        service_routing_observation: Option<IntegrationServiceRoutingObservation>,
        audit_sink: Option<Arc<dyn k8s_native_service::audit::AuditSink>>,
        priority_fairness: Option<Arc<k8s_native_service::priority_fairness::ApiPriorityFairness>>,
        mount_operational_endpoints: bool,
        options: IntegrationHarnessOptions,
    ) -> anyhow::Result<Self> {
        let IntegrationHarnessOptions {
            csr_signer,
            task_categories,
            auth_clock,
            authority,
            bootstrap_token_authenticator,
        } = options;
        let auth_clock = auth_clock.unwrap_or_else(|| Arc::new(klights_auth::clock::SystemClock));
        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();
        let passive_reads = if let Some(fail) = watch_history_failure {
            let focused_reads = db.focused_read_store();
            crate::datastore::selector::PassiveReadPorts::new(
                focused_reads.clone(),
                Arc::new(ToggleFailingWatchHistory {
                    delegate: focused_reads.clone(),
                    fail,
                }),
                focused_reads,
            )
        } else {
            crate::datastore::selector::sqlite_passive_read_ports(&db)
        };
        let datastore: IntegrationDatastoreHandle = Arc::new(db.clone());
        let config = crate::KlightsConfig::test_default();
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(task_categories));
        let identity: Arc<dyn k8s_native_service::ApiIdentityGenerator> =
            Arc::new(DeterministicApiIdentity::default());
        let controller_identity: Arc<dyn klights_controllers::ControllerIdentityGenerator> =
            Arc::new(DeterministicControllerIdentity::default());
        let service_ipam = Arc::new(klights_controllers::service::ServiceIpam::new(
            &config.service_cidr,
        ));
        let nodeport_alloc = Arc::new(klights_controllers::service::NodePortAllocator::new());
        nodeport_alloc.set_ready();
        let metrics = klights_controllers::side_effects::SideEffectMetrics::new();
        let leader_rx = crate::control_plane::client::local::always_leader_watch();
        let resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery> = Arc::new(
            crate::bootstrap::outbox_apply_adapter::BackendResourceQueryFixture::new(
                datastore.clone(),
                leader_rx.clone(),
            ),
        );
        let proposal: Arc<dyn klights_replication::proposal::RaftProposal> = Arc::new(
            crate::bootstrap::outbox_apply_adapter::BackendProposalFixture::new(datastore.clone()),
        );
        let resource_command: Arc<dyn klights_leader_api::LeaderResourceCommand> = Arc::new(
            klights_replication::leader_api::EmbeddedLeaderResourceCommand::new(
                proposal.clone(),
                resource_query.clone(),
                leader_rx.clone(),
            ),
        );
        let node_local = Arc::new(
            crate::datastore::node_local::selector::open_node_local(
                crate::datastore::backend_kind::BackendKind::Sqlite,
                None,
                supervisor.clone(),
                None,
                "sqlite:native-api-integration-node-local",
            )
            .await?,
        );
        let outbox_notify = Arc::new(tokio::sync::Notify::new());
        let outbox_stores = klights_kubelet::node_outbox::OutboxStores::new(
            node_local.outbox_producer(),
            node_local.outbox_dispatcher(),
            node_local.pod_status_checkpoints(),
            node_local.runtime_observation_checkpoints(),
            node_local.outbox_status_stamps(),
        );
        let outbox_codec =
            crate::bootstrap::composition_adapters::outbox_payload_codec_adapter::new_codec();
        let outbox = Arc::new(klights_kubelet::node_outbox::Outbox::compose(
            outbox_stores.clone(),
            outbox_codec.clone(),
            outbox_notify.clone(),
            Arc::new(klights_supervisor::SystemWallClock),
        ));
        let outbox_delivery: Arc<dyn klights_leader_api::LeaderOutboxDelivery> =
            Arc::new(IntegrationOutboxDelivery {
                embedded: Arc::new(
                    klights_replication::leader_api::EmbeddedOutboxDelivery::new(
                        proposal,
                        resource_query.clone(),
                        leader_rx,
                    ),
                ),
                codec: outbox_codec.clone(),
                authenticated_node: "test-node".to_string(),
            });
        let outbox_dispatcher = Arc::new(klights_kubelet::node_outbox::OutboxDispatcher::new(
            outbox_stores,
            outbox_codec,
            outbox_delivery,
            outbox_notify,
            Arc::new(klights_supervisor::SystemWallClock),
        ));
        let side_effects = Arc::new(crate::bootstrap::side_effects::default_registry(
            metrics.clone(),
            None,
            Some(supervisor.clone()),
            Some(datastore.clone()),
            controller_identity.clone(),
        ));
        let gc_coordination = Arc::new(klights_controllers::ControllerCoordination::new());
        let pod_repository_config = crate::pod_repository_composition::PodRepositoryBuildConfig {
            db: datastore.clone(),
            pod_workqueue_store: Some(node_local.pod_workqueue()),
            supervisor: supervisor.clone(),
            side_effects: side_effects.clone(),
            metrics: metrics.clone(),
            pod_network_cache: node_local.pod_network_cache(),
            assignment_waiter: Arc::new(klights_networking::PodNetworkAssignmentBus::new()),
            scheduling_mode: crate::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
            outbox: Some(outbox),
            cluster_api: Some(resource_query.clone()),
            controller_identity: controller_identity.clone(),
            #[cfg(not(test))]
            api_identity: identity.clone(),
            #[cfg(not(test))]
            gc_coordination: gc_coordination.clone(),
            scheduler_bind_gate: None,
        };
        #[cfg(not(test))]
        let root_pod_parts = crate::pod_repository_composition::build_pod_repository_parts(
            pod_repository_config,
            None,
        );
        #[cfg(test)]
        let root_pod_parts =
            crate::pod_repository_composition::build_pod_repository_parts_with_test_support(
                pod_repository_config,
                None,
                identity.clone(),
                gc_coordination.clone(),
            );
        let pod_api = root_pod_parts.api;
        let pod_subresource = root_pod_parts.subresource;
        let pod_repository = Arc::new(root_pod_parts.repository_parts.repository);
        let api_pod_repository =
            crate::bootstrap::composition_adapters::api_state_adapter::RootApiPodRepository::new(
                pod_repository.clone(),
                pod_api.clone(),
                pod_subresource.clone(),
            );
        let controller_pod_port = Arc::new(
            crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerPodPort::new(
                pod_repository.clone(),
                pod_api,
                pod_subresource,
            ),
        );
        side_effects.set_pod_ports(pod_repository.clone(), pod_repository.clone());
        let finalizer_lifecycle = crate::bootstrap::finalizer_lifecycle_adapter::
            DatastoreFinalizerLifecycleAdapter::new_with_coordination(
                datastore.clone(),
                pod_repository.clone(),
                side_effects.clone(),
                metrics.clone(),
                gc_coordination.clone(),
            );
        let mutation_side_effects = mutation_side_effects_factory
            .map(|factory| factory(datastore.clone()))
            .unwrap_or_else(|| side_effects.clone());
        let mutation_effects = klights_controllers::side_effects::ResourceMutationEffects::new(
            mutation_side_effects,
            metrics.clone(),
        );
        let positioned_watch =
            crate::bootstrap::composition_adapters::positioned_watch_adapter::for_test(
                &passive_reads,
                datastore.clone(),
            );
        let watch_signals = crate::bootstrap::watch_commit_wiring::test_signal_source(&datastore);
        let generated = crate::bootstrap::composition_adapters::generated_handler_adapter::GeneratedHandlerAdapter::new(
            datastore.clone(),
            watch_signals.clone(),
            positioned_watch.clone(),
            klights_supervisor::FileProcessExecutor::new(supervisor.clone()),
            supervisor.clone(),
            config.data_root.join("etc/ca.crt"),
            controller_identity.clone(),
        );
        let network = klights_networking::test_support::mock_network();
        let controller_leader_ports = Arc::new(
            crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort::new(datastore.clone()),
        );
        let non_pod_finalization = Arc::new(
            crate::bootstrap::controller_adapters::gc_delete_adapter::GcNonPodFinalizationAdapter::new(datastore.clone()),
        );
        let controller_dependencies = klights_controllers::ControllerRuntimeDependencies {
            wall_time: chrono::Utc::now,
            resource_query: controller_leader_ports.clone(),
            deployment_store: controller_leader_ports.clone(),
            replicaset_store: controller_leader_ports.clone(),
            statefulset_store: controller_leader_ports.clone(),
            daemonset_store: controller_leader_ports.clone(),
            job_store: controller_leader_ports.clone(),
            service_store: controller_leader_ports.clone(),
            pvc_store: controller_leader_ports.clone(),
            pdb_store: controller_leader_ports.clone(),
            replicationcontroller_store: controller_leader_ports.clone(),
            apiservice_store: controller_leader_ports.clone(),
            csr_status_store: controller_leader_ports,
            pod_query: api_pod_repository.clone(),
            pdb_pod_reader: pod_repository.clone(),
            deployment_pod_reader: pod_repository.clone(),
            deployment_pod_mutation: controller_pod_port.clone(),
            replicaset_pod_mutation: controller_pod_port.clone(),
            statefulset_pod_mutation: controller_pod_port.clone(),
            daemonset_pod_mutation: controller_pod_port.clone(),
            job_pod_mutation: controller_pod_port.clone(),
            replicationcontroller_pod_mutation: controller_pod_port,
            pod_delete_sink: pod_repository.clone(),
            reconcile: Arc::new(
                crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerReconcilePort::new(
                    non_pod_finalization.clone(),
                ),
            ),
            network: Arc::new(
                crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerNetworkPort::new(
                    network.services().clone(),
                ),
            ),
            effects: Arc::new(
                crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerEffectPort::new(
                    klights_supervisor::FileProcessExecutor::new(supervisor.clone()),
                    config.data_root.join("local-path-provisioner"),
                ),
            ),
            coordination: gc_coordination.clone(),
            node_name: Arc::from(config.node_name.as_str()),
        };
        let node_metrics = Arc::new(IntegrationNodeMetrics::new());
        let csr_issuer = csr_signer.map(|signer| {
            Arc::new(crate::bootstrap::auth_adapters::AuthCsrIssuer::new(
                signer,
                auth_clock.clone(),
                supervisor.clone(),
            )) as Arc<dyn klights_controllers::csr_signer::CsrIssuer>
        });
        let hpa_controller =
            crate::bootstrap::controller_adapters::hpa_controller_adapter::controller(
                datastore.clone(),
                pod_repository.clone(),
                non_pod_finalization,
                gc_coordination.clone(),
                Arc::from(config.node_name.as_str()),
                node_metrics.clone(),
                controller_identity.clone(),
            );
        let controller_dispatcher =
            Arc::new(klights_controllers::ControllerDispatcher::new_complete(
                service_ipam.clone(),
                nodeport_alloc.clone(),
                supervisor.clone(),
                csr_issuer,
                hpa_controller,
                controller_dependencies,
                controller_identity.clone(),
            ));
        side_effects.set_controller_dispatcher(controller_dispatcher.clone());
        let service_routing: Arc<dyn klights_reconcile_api::ServiceRoutingSync> =
            match service_routing_observation {
                Some(services) => Arc::new(services),
                None => Arc::new(
                    crate::bootstrap::network_adapters::ApiServiceRoutingSyncAdapter::new(
                        network.services().clone(),
                    ),
                ),
            };
        let pod_logs_root = config.data_root.join("logs/pods");
        let wall_clock: Arc<dyn klights_supervisor::WallClock> =
            Arc::new(klights_supervisor::SystemWallClock);
        let pod_logs = crate::bootstrap::composition_adapters::node_log_runtime_adapter::pod_log_capabilities(
            Arc::new(
                klights_kubelet::node_api::logs::LocalNodeLogRuntime::new_with_pod_event_store(
                    pod_logs_root.clone(),
                    supervisor.clone(),
                    wall_clock.clone(),
                    klights_kubelet::node_api::logs::PodLogFollowWatchSource::new(Arc::new(
                        crate::bootstrap::kubelet_ports::DatastorePodWatchSource::new(Arc::new(
                            positioned_watch.clone(),
                        )),
                    )),
                ),
            ),
            Arc::new(
                klights_kubelet::node_api::logs::LocalNodeLogRuntime::new_without_pod_event_store(
                    pod_logs_root,
                    supervisor.clone(),
                    wall_clock,
                ),
            ),
            supervisor.clone(),
            config.node_name.clone(),
        );
        let rbac_policy_store: Arc<dyn klights_auth::rbac_policy_store::RbacPolicyStore> = Arc::new(
            klights_auth::rbac_policy_store::ReaderBackedRbacPolicyStore::new(Arc::new(
                crate::bootstrap::auth_adapters::DatastoreRbacResourceReader::new(
                    datastore.clone(),
                ),
            )),
        );
        let runtime_paths =
            k8s_native_service::ApiRuntimePaths::from_data_root(config.data_root.clone())?;
        let mut runtime_inputs = k8s_native_service::ApiRuntimeInputs::new(
            runtime_paths,
            config.api_slow_log_threshold,
        )?;
        if let Some(audit_sink) = audit_sink {
            runtime_inputs = runtime_inputs.with_audit_sink(audit_sink);
        }
        if let Some(priority_fairness) = priority_fairness {
            runtime_inputs = runtime_inputs.with_priority_fairness(priority_fairness);
        }
        let node_name = config.node_name.clone();
        let bootstrap_token_authenticator = bootstrap_token_authenticator.unwrap_or_else(|| {
            Arc::new(
                crate::bootstrap::auth_adapters::DatastoreBootstrapTokenAuthenticator::new(
                    datastore.clone(),
                ),
            )
        });
        let crd_registry = klights_controllers::crd::CrdRegistry::new();
        let operational_replication = mount_operational_endpoints.then(|| {
            Arc::new(klights_replication::ReplicationService::new_with_ports(
                db.focused_recovery_store(),
                Arc::new(
                    crate::bootstrap::bootstrap_token::DatastoreBootstrapTokenValidation::new(
                        datastore.clone(),
                    ),
                ),
                supervisor.clone(),
            ))
        });
        let remote_node_services = operational_replication.as_ref().map(|replication| {
            let adapter =
                crate::bootstrap::grpc_runtime_adapter::GrpcReplicationRuntimeAdapter::new(
                    replication.clone(),
                );
            (
                adapter.clone() as Arc<dyn klights_node_api::NodeExec>,
                adapter as Arc<dyn klights_node_api::NodeLog>,
            )
        });
        let node_lease_tracker = Arc::new(
            klights_controllers::node_lease::NodeLeaseTracker::new_at(chrono::Utc::now()),
        );
        let (router, outer_layers) = k8s_native_service::build_current_router(
            identity.clone(),
            authorizer,
            rbac_policy_store,
            bootstrap_token_authenticator.clone(),
            oidc,
            webhook,
            None,
            Arc::new(
                crate::bootstrap::composition_adapters::watch_stream_adapter::DatastoreWatchStreamAdapter::new(
                    datastore.clone(),
                    watch_signals,
                    positioned_watch.clone(),
                ),
            ),
            crate::bootstrap::composition_adapters::api_state_adapter::RootNamespaceTerminationStore::new(datastore.clone()),
            resource_query,
            resource_command,
            finalizer_lifecycle,
            mutation_effects,
            crate::bootstrap::composition_adapters::list_query_adapter::DatastoreListResourceVersionPort::new(datastore.clone()),
            crate::bootstrap::composition_adapters::list_query_adapter::DatastoreNamespaceListPort::new(datastore.clone()),
            crate::bootstrap::controller_adapters::resource_quota_admission_adapter::ResourceQuotaAdmissionAdapter::new(
                datastore.clone(),
            ),
            crate::bootstrap::composition_adapters::resource_admission_adapter::ResourceAdmissionAdapter::new(
                identity,
                datastore.clone(),
            ),
            crate::bootstrap::composition_adapters::custom_resource_read_adapter::CustomResourceReadAdapter::new(
                datastore.clone(),
                crate::bootstrap::watch_commit_wiring::test_signal_source(&datastore),
                positioned_watch,
                supervisor.clone(),
            ),
            generated.clone(),
            generated.clone(),
            generated.clone(),
            generated,
            Arc::new(
                crate::bootstrap::controller_adapters::gc_delete_adapter::GcOwnerLifecycleAdapter::new_with_coordination(
                    datastore.clone(),
                    pod_repository.clone(),
                    gc_coordination,
                ),
            ),
            api_pod_repository,
            crd_registry.clone(),
            crate::bootstrap::service_adapters::ApiServiceWriteAllocator::new(
                datastore.clone(),
                service_ipam,
                nodeport_alloc.clone(),
                controller_identity,
            ),
            controller_dispatcher.clone(),
            crate::bootstrap::composition_adapters::api_state_adapter::RootApiFailureMetrics::new(metrics),
            crate::bootstrap::composition_adapters::api_state_adapter::RootApiNodeLeaseObservations::new(node_lease_tracker.clone()),
            service_routing.clone(),
            pod_logs,
            None,
            node_metrics.clone(),
            klights_kubelet::node_api::port_forward::local_node_port_forward(supervisor.clone()),
            pod_lifecycle_diagnostics,
            None,
            remote_node_services,
            config.node_name,
            config.anonymous_auth,
            runtime_inputs,
            auth_clock,
            supervisor.clone(),
            signing_keys,
            authority.clone(),
        );
        let router = if mount_operational_endpoints {
            let operational_endpoints = klights_apiserver::OperationalEndpointHandlers::new(
                klights_apiserver::OperationalEndpointInputs::new(
                    klights_apiserver::OperationalNodeRole::Leader,
                    Arc::new(String::new),
                    crate::version::api_version_info(),
                    crate::bootstrap::operational_adapters::ApiClusterStatusMetadata::new(
                        datastore.clone(),
                    ),
                    operational_replication.as_ref().map(|replication| {
                        replication.clone()
                            as Arc<dyn klights_leader_api::LeaderFollowerDiagnostics>
                    }),
                    supervisor.clone(),
                ),
            );
            klights_apiserver::mount_operational_endpoints(
                router.into_router(),
                operational_endpoints,
            )
        } else {
            router.into_router()
        };
        Ok(Self {
            router: outer_layers.finish(router),
            datastore,
            sqlite: db,
            nodeport_alloc,
            pod_repository,
            _node_local: node_local,
            outbox_dispatcher,
            controller_dispatcher,
            crd_registry,
            service_routing,
            node_metrics,
            operational_replication,
            task_supervisor: supervisor,
            node_lease_tracker,
            authority,
            node_name,
        })
    }

    pub fn router(&self) -> axum::Router {
        self.router.clone()
    }

    pub fn router_with_authority(&self, is_leader: bool) -> axum::Router {
        let authority = if is_leader {
            self.authority
                .clone()
                .expect("leader authority harness must be selected")
        } else {
            let (authority, _publisher) =
                klights_replication::authority::WatchLeaderAuthority::channel(false, None);
            authority
        };
        klights_apiserver::wrap_authority_router(
            self.router.clone(),
            Some(Arc::new(
                klights_apiserver::HttpAuthorityRouter::from_authority(authority, None),
            )),
        )
    }

    pub async fn record_node_lease(
        &self,
        node_name: &str,
        lease: &serde_json::Value,
    ) -> anyhow::Result<()> {
        self.node_lease_tracker
            .record_from_lease_object(node_name, lease)
            .await
            .map(|_| ())
    }

    pub fn request_service_routing_sync(
        &self,
    ) -> Result<(), klights_reconcile_api::ReconcileSinkError> {
        self.service_routing.request_service_routing_sync()
    }

    pub async fn finalize_pod_deletion_after_actor_cleanup(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<bool> {
        let outcome = self
            .pod_repository
            .integration_finalize_bound_pod(namespace, name, uid)
            .await?;
        Ok(matches!(
            outcome,
            crate::kubelet::pod_repository::store::BoundPodDeleteOutcome::Removed
                | crate::kubelet::pod_repository::store::BoundPodDeleteOutcome::IdentityChanged
        ))
    }

    pub async fn drain_node_outbox(&self) -> anyhow::Result<()> {
        for _ in 0..1024 {
            if matches!(
                self.outbox_dispatcher
                    .dispatch_due_once(i64::MAX / 4)
                    .await?,
                klights_kubelet::node_outbox::DispatchOutcome::Idle { .. }
            ) {
                return Ok(());
            }
        }
        anyhow::bail!("node outbox drain exceeded 1024 deliveries")
    }

    pub async fn queued_reconcile_keys(&self) -> Vec<klights_reconcile_api::ReconcileKey> {
        self.controller_dispatcher.pending_reconcile_keys().await
    }

    pub async fn drain_controller_reconciles(
        &self,
    ) -> anyhow::Result<Vec<klights_reconcile_api::ReconcileKey>> {
        let mut drained = Vec::new();
        for _ in 0..1024 {
            if self
                .controller_dispatcher
                .pending_reconcile_keys()
                .await
                .is_empty()
            {
                return Ok(drained);
            }
            drained.push(
                self.controller_dispatcher
                    .dispatch_next_key_for_test()
                    .await,
            );
        }
        anyhow::bail!("controller reconcile drain exceeded 1024 operations")
    }

    pub async fn dispatch_next_controller_reconcile(&self) -> klights_reconcile_api::ReconcileKey {
        self.controller_dispatcher
            .dispatch_next_key_for_test()
            .await
    }

    pub async fn reconcile_endpointslice(
        &self,
        service_name: &str,
        service_uid: &str,
        namespace: &str,
        selector: Option<&serde_json::Value>,
        ports: Option<&serde_json::Value>,
    ) -> anyhow::Result<()> {
        klights_controllers::endpoints::reconcile_endpointslice(
            self.datastore.as_ref(),
            self.pod_repository.as_ref(),
            service_name,
            service_uid,
            namespace,
            selector,
            ports,
        )
        .await
    }

    async fn seed_endpoint_fixture(
        &self,
        kind: EndpointFixtureKind,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        self.datastore
            .create_resource(kind.api_version(), kind.kind(), namespace, name, value)
            .await
    }

    pub async fn seed_endpoint_namespace(
        &self,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        self.sqlite.create_namespace(name, value).await
    }

    pub async fn seed_endpoint_pod(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        self.seed_endpoint_fixture(EndpointFixtureKind::Pod, Some(namespace), name, value)
            .await
    }

    pub async fn seed_endpoint_service(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        self.seed_endpoint_fixture(EndpointFixtureKind::Service, Some(namespace), name, value)
            .await
    }

    pub async fn seed_endpoints(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        self.seed_endpoint_fixture(EndpointFixtureKind::Endpoints, Some(namespace), name, value)
            .await
    }

    pub async fn seed_endpoint_slice(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        self.seed_endpoint_fixture(
            EndpointFixtureKind::EndpointSlice,
            Some(namespace),
            name,
            value,
        )
        .await
    }

    async fn observe_endpoint_fixture(
        &self,
        kind: EndpointFixtureKind,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        self.datastore
            .get_resource(kind.api_version(), kind.kind(), Some(namespace), name)
            .await
    }

    pub async fn observe_endpoints(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        self.observe_endpoint_fixture(EndpointFixtureKind::Endpoints, namespace, name)
            .await
    }

    pub async fn observe_endpoint_slice(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        self.observe_endpoint_fixture(EndpointFixtureKind::EndpointSlice, namespace, name)
            .await
    }

    pub async fn observe_endpoint_slices(
        &self,
        namespace: &str,
        label_selector: Option<&str>,
    ) -> anyhow::Result<Vec<klights_cluster_core::Resource>> {
        Ok(self
            .datastore
            .list_resources(
                EndpointFixtureKind::EndpointSlice.api_version(),
                EndpointFixtureKind::EndpointSlice.kind(),
                Some(namespace),
                crate::datastore::ResourceListQuery::new(label_selector, None, None, None),
            )
            .await?
            .items)
    }

    pub async fn replace_endpoints(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
        expected_rv: i64,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        self.datastore
            .update_resource("v1", "Endpoints", Some(namespace), name, value, expected_rv)
            .await
    }

    pub async fn remove_endpoints(&self, namespace: &str, name: &str) -> anyhow::Result<()> {
        self.datastore
            .delete_resource("v1", "Endpoints", Some(namespace), name)
            .await
    }

    pub async fn remove_endpoint_slice(&self, namespace: &str, name: &str) -> anyhow::Result<()> {
        self.datastore
            .delete_resource(
                "discovery.k8s.io/v1",
                "EndpointSlice",
                Some(namespace),
                name,
            )
            .await
    }

    pub async fn endpoint_fixture_resource_version(&self) -> anyhow::Result<i64> {
        self.datastore.get_current_resource_version().await
    }

    pub fn endpoint_fixture_value_with_resource_version(
        value: impl Into<Arc<serde_json::Value>>,
        resource_version: i64,
    ) -> serde_json::Value {
        crate::bootstrap::controller_adapters::controller_runtime_adapter::inject_resource_version(
            value,
            resource_version,
        )
    }

    pub async fn reconcile_endpoints(
        &self,
        service_name: &str,
        namespace: &str,
        selector: Option<&serde_json::Value>,
        ports: Option<&serde_json::Value>,
        publish_not_ready: bool,
    ) -> anyhow::Result<()> {
        klights_controllers::endpoints::reconcile_endpoints(
            self.datastore.as_ref(),
            self.pod_repository.as_ref(),
            service_name,
            namespace,
            selector,
            ports,
            publish_not_ready,
        )
        .await
    }

    pub async fn reconcile_service_endpoint_batch(
        &self,
        service_name: &str,
        service_uid: &str,
        namespace: &str,
        selector: Option<&serde_json::Value>,
        ports: Option<&serde_json::Value>,
        publish_not_ready: bool,
    ) -> anyhow::Result<()> {
        klights_controllers::endpoints::reconcile_service_endpoints_batch(
            self.datastore.as_ref(),
            self.pod_repository.as_ref(),
            klights_controllers::endpoints::ServiceEndpointBatchReconcileRequest {
                service_name,
                service_uid,
                namespace,
                selector,
                service_ports: ports,
                publish_not_ready,
            },
        )
        .await
    }

    pub async fn mirror_endpoint_fixture(
        &self,
        endpoints: &serde_json::Value,
    ) -> anyhow::Result<()> {
        klights_controllers::endpoints::mirror_endpoints_to_endpointslice_at(
            self.datastore.as_ref(),
            endpoints,
            chrono::Utc::now(),
            crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity().as_ref(),
        )
        .await
    }

    pub async fn cascade_delete_endpoint_service(
        &self,
        owner_uid: &str,
        owner_name: &str,
        owner_namespace: &str,
    ) -> anyhow::Result<()> {
        struct FailClosedGcPodDeleteSink;

        impl klights_reconcile_api::GcPodDeleteSink for FailClosedGcPodDeleteSink {
            fn request_gc_pod_delete(
                &self,
                _request: klights_reconcile_api::GcPodDeleteRequest,
            ) -> klights_reconcile_api::GcPodDeleteFuture<'_> {
                Box::pin(async {
                    Err(klights_reconcile_api::GcPodDeleteError::unavailable(
                        "endpoint owner-cascade fixture must not request Pod deletion",
                    ))
                })
            }
        }

        klights_controllers::gc::cascade_delete_with_uid(
            self.datastore.as_ref(),
            owner_uid,
            "v1",
            owner_name,
            "Service",
            Some(owner_namespace.to_owned()),
            &FailClosedGcPodDeleteSink,
            &crate::bootstrap::controller_adapters::gc_delete_adapter::GcNonPodFinalizationAdapter::new(
                self.datastore.clone(),
            ),
            &klights_controllers::ControllerCoordination::new(),
        )
        .await
    }

    pub fn spawn_controller_worker(
        &self,
        cancel: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let dispatcher = self.controller_dispatcher.clone();
        tokio::spawn(dispatcher.run_worker_pool(1, cancel))
    }

    pub async fn register_crd_value(&self, crd: &serde_json::Value) -> anyhow::Result<()> {
        klights_controllers::crd::register_crd_from_value(&self.crd_registry, crd).await
    }

    pub async fn register_crd_info(&self, info: klights_controllers::crd::CrdResourceInfo) {
        self.crd_registry.register(info).await;
    }

    pub async fn sync_crd_registry_from_datastore(&self) -> anyhow::Result<()> {
        klights_controllers::crd::sync_registry_from_datastore(
            self.datastore.as_ref(),
            &self.crd_registry,
        )
        .await
    }

    pub async fn crd_selectable_fields(
        &self,
        group: &str,
        version: &str,
        plural: &str,
    ) -> Option<Vec<String>> {
        self.crd_registry
            .get(group, version, plural)
            .await
            .map(|info| info.selectable_fields)
    }

    pub fn set_node_metrics(&self, metrics: Arc<dyn klights_node_api::NodeMetrics>) {
        self.node_metrics.set(metrics);
    }

    pub async fn ensure_operational_cluster_metadata(&self) -> anyhow::Result<()> {
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(self.datastore.as_ref()).await?;
        Ok(())
    }

    pub async fn seed_default_rbac(&self) -> anyhow::Result<()> {
        klights_controllers::rbac_reconcile::reconcile_default_rbac_objects(self.datastore.as_ref())
            .await
    }

    pub async fn register_operational_follower(
        &self,
        dataplane: klights_leader_api::NetworkDataplane,
    ) -> anyhow::Result<()> {
        let replication = self
            .operational_replication
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("operational replication is not installed"))?;
        let (_control_rx, _session) = replication.register_follower(dataplane).await;
        Ok(())
    }

    pub async fn register_integration_follower(
        &self,
        dataplane: klights_leader_api::NetworkDataplane,
    ) -> anyhow::Result<IntegrationFollowerSession> {
        let replication = self
            .operational_replication
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("operational replication is not installed"))?
            .clone();
        let node_name = dataplane.node_name().to_string();
        let (control_rx, session_id) = replication.register_follower(dataplane).await;
        Ok(IntegrationFollowerSession {
            replication,
            control_rx,
            node_name,
            session_id,
        })
    }

    pub fn integration_remote_exec_sync(&self) -> anyhow::Result<IntegrationRemoteExecSync> {
        let replication = self
            .operational_replication
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("operational replication is not installed"))?
            .clone();
        Ok(IntegrationRemoteExecSync {
            node_exec: crate::bootstrap::grpc_runtime_adapter::GrpcReplicationRuntimeAdapter::new(
                replication,
            ),
            task_supervisor: self.task_supervisor.clone(),
        })
    }

    /// Existing adapter cases require direct CRUD setup and observation.
    /// This capability is absent unless the integration-only feature is set.
    pub fn datastore(&self) -> IntegrationDatastoreHandle {
        self.datastore.clone()
    }

    pub fn subscribe_watch(
        &self,
        api_version: &str,
        kind: &str,
    ) -> tokio::sync::broadcast::Receiver<klights_watch::WatchEvent> {
        crate::bootstrap::watch_commit_wiring::subscribe_test_events(
            self.datastore.commit_observation_sink().as_ref(),
            klights_watch::WatchTopic::new(api_version, kind),
        )
    }

    pub fn install_resource_mutation_pause(
        &self,
        operation: IntegrationResourceMutationPauseOperation,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Arc<IntegrationResourceMutationPause> {
        self.sqlite
            .install_resource_mutation_pause(operation, api_version, kind, namespace, name)
    }

    pub fn exhaust_nodeports(&self) -> anyhow::Result<()> {
        for expected in 30000..=32767 {
            let allocated = self.nodeport_alloc.allocate().map_err(anyhow::Error::msg)?;
            anyhow::ensure!(
                allocated == expected,
                "unexpected NodePort allocation {allocated}"
            );
        }
        Ok(())
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }
}

pub(crate) mod node_delivery_support {
    //! Feature-gated real-adapter helpers for node-delivery integration tests.

    use std::sync::Arc;

    use anyhow::Result;
    use klights_kubelet::node_outbox::{Outbox, OutboxDispatcher, OutboxStores};
    use klights_leader_api::LeaderOutboxDelivery;
    use tokio::sync::Notify;

    use crate::datastore::DatastoreBackend as _;
    use crate::datastore::node_local::{LegacyDeliveryTestStore as _, NodeLocalStores};

    #[derive(Clone)]
    pub struct IntegrationNodeDeliveryCluster {
        db: Arc<crate::datastore::sqlite::Datastore>,
    }

    impl IntegrationNodeDeliveryCluster {
        pub async fn open() -> Result<Self> {
            Ok(Self {
                db: Arc::new(crate::datastore::sqlite::Datastore::new_in_memory().await?),
            })
        }

        pub async fn seed_node(
            &self,
            name: &str,
            value: serde_json::Value,
        ) -> Result<klights_cluster_core::Resource> {
            self.db
                .create_resource("v1", "Node", None, name, value)
                .await
        }

        pub async fn observe_node(
            &self,
            name: &str,
        ) -> Result<Option<klights_cluster_core::Resource>> {
            self.db.get_resource("v1", "Node", None, name).await
        }

        pub async fn replace_node_if_current(
            &self,
            name: &str,
            value: serde_json::Value,
            current: &klights_cluster_core::Resource,
        ) -> Result<klights_cluster_core::Resource> {
            self.db
                .update_resource_with_preconditions(
                    "v1",
                    "Node",
                    None,
                    name,
                    value,
                    klights_cluster_core::ResourcePreconditions::from_resource(current),
                )
                .await
        }

        pub async fn allocate_node_subnet(
            &self,
            node_name: &str,
            cluster_cidr: &str,
            node_ip: &str,
        ) -> Result<()> {
            self.db
                .allocate_node_subnet(node_name, cluster_cidr, node_ip)
                .await
                .map(|_| ())
        }

        pub async fn seed_pod(
            &self,
            namespace: &str,
            name: &str,
            value: serde_json::Value,
        ) -> Result<klights_cluster_core::Resource> {
            self.db
                .create_resource("v1", "Pod", Some(namespace), name, value)
                .await
        }

        pub async fn mark_pod_terminating(
            &self,
            namespace: &str,
            name: &str,
            value: serde_json::Value,
            expected_rv: i64,
        ) -> Result<klights_cluster_core::Resource> {
            self.db
                .update_resource("v1", "Pod", Some(namespace), name, value, expected_rv)
                .await
        }

        pub async fn observe_pod(
            &self,
            namespace: &str,
            name: &str,
        ) -> Result<Option<klights_cluster_core::Resource>> {
            self.db
                .get_resource("v1", "Pod", Some(namespace), name)
                .await
        }

        pub async fn public_resource_version(&self) -> Result<i64> {
            self.db.get_current_resource_version().await
        }

        pub async fn watch_replay_position(
            &self,
        ) -> Result<klights_cluster_core::WatchReplayPosition> {
            self.db.current_watch_replay_position().await
        }

        pub async fn outbox_stream_watermarks(
            &self,
        ) -> Result<Vec<klights_cluster_core::OutboxStreamWatermark>> {
            self.db.list_outbox_stream_watermarks().await
        }

        pub async fn seed_namespace(&self, name: &str, value: serde_json::Value) -> Result<()> {
            self.db.create_namespace(name, value).await.map(|_| ())
        }

        pub async fn observe_events(
            &self,
            namespace: &str,
        ) -> Result<Vec<klights_cluster_core::Resource>> {
            Ok(self
                .db
                .list_resources(
                    "v1",
                    "Event",
                    Some(namespace),
                    crate::datastore::ResourceListQuery::all(),
                )
                .await?
                .items)
        }

        pub async fn observe_events_all_namespaces(
            &self,
        ) -> Result<Vec<klights_cluster_core::Resource>> {
            Ok(self
                .db
                .list_resources(
                    "v1",
                    "Event",
                    None,
                    crate::datastore::ResourceListQuery::all(),
                )
                .await?
                .items)
        }

        pub async fn apply_outbox_event_create(&self, payload: &[u8]) -> Result<()> {
            let command =
                klights_leader_rpc::storage_wire_codec::decode_outbox_payload_protobuf(payload)?
                    .into_command();
            match command {
                klights_cluster_core::StorageCommand::CreateResource {
                    api_version,
                    kind,
                    namespace,
                    name,
                    data,
                    ..
                } => self
                    .db
                    .create_resource(&api_version, &kind, namespace.as_deref(), &name, data)
                    .await
                    .map(|_| ()),
                other => {
                    anyhow::bail!("unsupported outbox command in pod-event fixture: {other:?}")
                }
            }
        }

        pub fn node_ports(
            &self,
            authenticated_node: &str,
        ) -> super::leader_rpc::IntegrationLeaderRpcNodePorts {
            super::leader_rpc::IntegrationLeaderRpcComposition::local_node_ports(
                self.db.clone(),
                authenticated_node.to_string(),
            )
        }

        pub fn heartbeat_event_source(
            &self,
        ) -> Arc<dyn klights_kubelet::node_heartbeat::NodeHeartbeatEventSource> {
            let passive = super::leader_rpc::IntegrationLeaderRpcComposition::passive_reads_for(
                self.db.as_ref(),
            );
            super::leader_rpc::IntegrationLeaderRpcComposition::node_heartbeat_event_source(
                &passive,
                self.db.clone(),
            )
        }

        pub async fn observe_lease_resource_version(&self, node_name: &str) -> Result<Option<i64>> {
            Ok(self
                .db
                .get_resource(
                    "coordination.k8s.io/v1",
                    "Lease",
                    Some("kube-node-lease"),
                    node_name,
                )
                .await?
                .map(|resource| resource.resource_version))
        }

        pub async fn register_node_snapshot(
            &self,
            outbox: Option<&IntegrationNodeOutbox>,
            dataplane_health: Option<&klights_network_api::DataplaneHealthSnapshot>,
            snapshot: &klights_kubelet::node::NodeRegistrationSnapshot,
        ) -> Result<()> {
            crate::bootstrap::node_registration_adapter::register_node_snapshot(
                self.db.as_ref(),
                outbox.map(IntegrationNodeOutbox::inner),
                dataplane_health,
                snapshot,
            )
            .await
        }

        fn pod_event_query(&self) -> IntegrationPodEventAdapter<'_> {
            IntegrationPodEventAdapter::new(self.db.as_ref())
        }

        pub async fn emit_pod_event_to_outbox(
            &self,
            outbox: &IntegrationNodeOutbox,
            record: klights_kubelet::pod_events::PodEventRecord<'_>,
        ) -> Result<serde_json::Value> {
            let adapter = self.pod_event_query();
            outbox.emit_pod_event(&adapter, record).await
        }

        pub async fn emit_control_plane_pod_event(
            &self,
            record: klights_kubelet::pod_events::PodEventRecord<'_>,
        ) -> Result<serde_json::Value> {
            let adapter = self.pod_event_query();
            klights_kubelet::pod_events::emit_control_plane_pod_event(&adapter, &adapter, record)
                .await
        }

        pub async fn reject_pod_event_without_outbox(
            &self,
            record: klights_kubelet::pod_events::PodEventRecord<'_>,
        ) -> Result<serde_json::Value> {
            let adapter = self.pod_event_query();
            klights_kubelet::pod_events::emit_pod_event_with_outbox(&adapter, None, record).await
        }
    }

    struct IntegrationPodEventAdapter<'a> {
        inner:
            crate::bootstrap::composition_adapters::pod_event_adapter::DatastorePodEventAdapter<'a>,
    }

    impl<'a> IntegrationPodEventAdapter<'a> {
        fn new(db: &'a dyn crate::datastore::DatastoreBackend) -> Self {
            Self {
            inner: crate::bootstrap::composition_adapters::pod_event_adapter::DatastorePodEventAdapter::new(db),
        }
        }
    }

    #[async_trait::async_trait]
    impl klights_kubelet::pod_events::PodEventQuery for IntegrationPodEventAdapter<'_> {
        async fn namespace_eligibility(
            &self,
            namespace: &str,
        ) -> Result<klights_kubelet::pod_events::PodEventNamespaceEligibility> {
            klights_kubelet::pod_events::PodEventQuery::namespace_eligibility(
                &self.inner,
                namespace,
            )
            .await
        }
        async fn list_events(
            &self,
            namespace: &str,
        ) -> Result<Vec<klights_cluster_core::Resource>> {
            klights_kubelet::pod_events::PodEventQuery::list_events(&self.inner, namespace).await
        }
    }

    #[async_trait::async_trait]
    impl klights_kubelet::pod_events::PodEventEffect for IntegrationPodEventAdapter<'_> {
        async fn create_event(
            &self,
            namespace: &str,
            name: &str,
            event: serde_json::Value,
        ) -> Result<()> {
            klights_kubelet::pod_events::PodEventEffect::create_event(
                &self.inner,
                namespace,
                name,
                event,
            )
            .await
        }
    }

    pub fn author_bound_pod_finalization(
        namespace: String,
        name: String,
        pod_uid: String,
        node_name: String,
        observed_resource_version: i64,
    ) -> klights_cluster_core::StorageCommand {
        crate::bootstrap::composition_adapters::bound_pod_finalization_adapter::author(
            namespace,
            name,
            pod_uid,
            node_name,
            observed_resource_version,
        )
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct IntegrationNodeDeliveryOutboxInsert {
        pub idempotency_key: String,
        pub enqueued_ms: i64,
        pub subject_key: String,
        pub subject_api_version: String,
        pub subject_kind: String,
        pub subject_namespace: Option<String>,
        pub subject_name: String,
        pub subject_uid: Option<String>,
        pub pod_uid: String,
        pub operation: String,
        pub classification: klights_node_store::OutboxClassification,
        pub payload_proto: Vec<u8>,
        pub next_due_ms: i64,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct IntegrationNodeDeliveryOutboxRow {
        pub id: i64,
        pub client_id: String,
        pub idempotency_key: String,
        pub enqueued_ms: i64,
        pub subject_key: String,
        pub subject_api_version: String,
        pub subject_kind: String,
        pub subject_namespace: Option<String>,
        pub subject_name: String,
        pub subject_uid: Option<String>,
        pub pod_uid: String,
        pub operation: String,
        pub priority_class: i64,
        pub supersedable_pod_status: bool,
        pub is_terminal_pod_delete: bool,
        pub stream_id: i64,
        pub stream_seq: i64,
        pub payload_proto: Vec<u8>,
        pub attempt: i64,
        pub next_due_ms: i64,
        pub leased_until_ms: i64,
        pub lease_token: Option<String>,
        pub last_error: Option<String>,
    }

    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
    pub struct IntegrationNodeDeliveryDeadLetterRow {
        pub id: i64,
        pub original_id: i64,
        pub client_id: String,
        pub idempotency_key: String,
        pub enqueued_ms: i64,
        pub subject_key: String,
        pub subject_api_version: String,
        pub subject_kind: String,
        pub subject_namespace: Option<String>,
        pub subject_name: String,
        pub subject_uid: Option<String>,
        pub pod_uid: String,
        pub operation: String,
        pub stream_id: i64,
        pub stream_seq: i64,
        pub payload_proto: Vec<u8>,
        pub attempts: i64,
        pub last_error: String,
        pub moved_at_ms: i64,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct IntegrationNodeDeliveryDeadLetterInsert<'a> {
        pub idempotency_key: &'a str,
        pub operation: &'a str,
        pub subject_key: &'a str,
        pub subject_api_version: &'a str,
        pub subject_kind: &'a str,
        pub subject_namespace: Option<&'a str>,
        pub subject_name: &'a str,
        pub subject_uid: Option<&'a str>,
        pub pod_uid: &'a str,
        pub payload_proto: &'a [u8],
        pub attempts: i64,
        pub last_error: &'a str,
        pub moved_at_ms: i64,
    }

    #[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
    pub struct IntegrationNodeDeliveryOutboxStats {
        pub pending: i64,
        pub oldest_age_seconds: f64,
        pub dead_letter_count: i64,
        pub dispatch_total: i64,
        pub dispatch_errors_total: i64,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct IntegrationNodeDeliveryPodStatusCheckpoint {
        pub pod_uid: String,
        pub namespace: String,
        pub pod_name: String,
        pub base_rv: i64,
        pub applied_rv: Option<i64>,
        pub status: serde_json::Value,
        pub updated_ms: i64,
    }

    impl From<crate::datastore::node_local::OutboxRow> for IntegrationNodeDeliveryOutboxRow {
        fn from(row: crate::datastore::node_local::OutboxRow) -> Self {
            Self {
                id: row.id,
                client_id: row.client_id,
                idempotency_key: row.idempotency_key,
                enqueued_ms: row.enqueued_ms,
                subject_key: row.subject_key,
                subject_api_version: row.subject_api_version,
                subject_kind: row.subject_kind,
                subject_namespace: row.subject_namespace,
                subject_name: row.subject_name,
                subject_uid: row.subject_uid,
                pod_uid: row.pod_uid,
                operation: row.operation,
                priority_class: row.priority_class,
                supersedable_pod_status: row.supersedable_pod_status,
                is_terminal_pod_delete: row.is_terminal_pod_delete,
                stream_id: row.stream_id,
                stream_seq: row.stream_seq,
                payload_proto: row.payload_proto,
                attempt: row.attempt,
                next_due_ms: row.next_due_ms,
                leased_until_ms: row.leased_until_ms,
                lease_token: row.lease_token,
                last_error: row.last_error,
            }
        }
    }

    impl From<crate::datastore::node_local::DeadLetterRow> for IntegrationNodeDeliveryDeadLetterRow {
        fn from(row: crate::datastore::node_local::DeadLetterRow) -> Self {
            Self {
                id: row.id,
                original_id: row.original_id,
                client_id: row.client_id,
                idempotency_key: row.idempotency_key,
                enqueued_ms: row.enqueued_ms,
                subject_key: row.subject_key,
                subject_api_version: row.subject_api_version,
                subject_kind: row.subject_kind,
                subject_namespace: row.subject_namespace,
                subject_name: row.subject_name,
                subject_uid: row.subject_uid,
                pod_uid: row.pod_uid,
                operation: row.operation,
                stream_id: row.stream_id,
                stream_seq: row.stream_seq,
                payload_proto: row.payload_proto,
                attempts: row.attempts,
                last_error: row.last_error,
                moved_at_ms: row.moved_at_ms,
            }
        }
    }

    #[derive(Clone)]
    pub struct IntegrationNodeOutbox {
        inner: Outbox,
    }

    impl IntegrationNodeOutbox {
        pub async fn record_pod_status_checkpoint(
            &self,
            pod: &klights_cluster_core::Resource,
            status: serde_json::Value,
            updated_ms: i64,
        ) -> Result<()> {
            self.inner
                .record_pod_status_checkpoint(pod, status, updated_ms)
                .await
        }

        pub async fn enqueue_command(
            &self,
            command: klights_kubelet::node_outbox::OutboxCommand,
        ) -> Result<()> {
            self.inner.enqueue_command(command).await
        }

        pub async fn merge_pod_status_checkpoint(
            &self,
            pod: klights_cluster_core::Resource,
        ) -> Result<klights_cluster_core::Resource> {
            self.inner.merge_pod_status_checkpoint(pod).await
        }

        pub async fn record_runtime_observation_checkpoint(
            &self,
            pod_uid: &str,
            container_ids: Vec<String>,
            generation: u64,
            updated_ms: i64,
        ) -> Result<()> {
            self.inner
                .record_runtime_observation_checkpoint(
                    pod_uid,
                    container_ids,
                    generation,
                    updated_ms,
                )
                .await
        }

        pub async fn get_runtime_observation_checkpoint(
            &self,
            pod_uid: &str,
        ) -> Result<Option<klights_kubelet::node_outbox::RuntimeObservationCheckpointState>>
        {
            self.inner.get_runtime_observation_checkpoint(pod_uid).await
        }

        pub async fn delete_runtime_observation_checkpoint(&self, pod_uid: &str) -> Result<()> {
            self.inner
                .delete_runtime_observation_checkpoint(pod_uid)
                .await
        }

        pub async fn next_status_stamp_at(&self, now_us: i64) -> Result<i64> {
            klights_kubelet::node_outbox::next_status_stamp_with_clock_for_integration_test(
                &self.inner,
                now_us,
            )
            .await
        }

        async fn emit_pod_event(
            &self,
            query: &IntegrationPodEventAdapter<'_>,
            record: klights_kubelet::pod_events::PodEventRecord<'_>,
        ) -> Result<serde_json::Value> {
            klights_kubelet::pod_events::emit_pod_event_with_outbox(
                query,
                Some(&self.inner),
                record,
            )
            .await
        }

        pub(crate) fn inner(&self) -> &Outbox {
            &self.inner
        }
    }

    impl klights_leader_api::NodeOutbox for IntegrationNodeOutbox {
        fn enqueue(
            &self,
            command: klights_leader_api::NodeOutboxCommand,
        ) -> klights_leader_api::NodeOutboxFuture<'_, klights_leader_api::NodeOutboxRoute> {
            klights_leader_api::NodeOutbox::enqueue(&self.inner, command)
        }
        fn next_status_stamp(&self) -> klights_leader_api::NodeOutboxFuture<'_, i64> {
            klights_leader_api::NodeOutbox::next_status_stamp(&self.inner)
        }
        fn record_pod_status_checkpoint<'a>(
            &'a self,
            checkpoint: &'a klights_cluster_core::Resource,
            updated_ms: i64,
        ) -> klights_leader_api::NodeOutboxFuture<'a, ()> {
            klights_leader_api::NodeOutbox::record_pod_status_checkpoint(
                &self.inner,
                checkpoint,
                updated_ms,
            )
        }
        fn merge_pod_status_checkpoint(
            &self,
            pod: klights_cluster_core::Resource,
        ) -> klights_leader_api::NodeOutboxFuture<'_, klights_cluster_core::Resource> {
            klights_leader_api::NodeOutbox::merge_pod_status_checkpoint(&self.inner, pod)
        }
        fn delete_pod_status_checkpoint<'a>(
            &'a self,
            pod_uid: &'a str,
        ) -> klights_leader_api::NodeOutboxFuture<'a, ()> {
            klights_leader_api::NodeOutbox::delete_pod_status_checkpoint(&self.inner, pod_uid)
        }
        fn record_runtime_observation_checkpoint<'a>(
            &'a self,
            pod_uid: &'a str,
            container_ids: Vec<String>,
            generation: u64,
            updated_ms: i64,
        ) -> klights_leader_api::NodeOutboxFuture<'a, ()> {
            klights_leader_api::NodeOutbox::record_runtime_observation_checkpoint(
                &self.inner,
                pod_uid,
                container_ids,
                generation,
                updated_ms,
            )
        }
        fn get_runtime_observation_checkpoint<'a>(
            &'a self,
            pod_uid: &'a str,
        ) -> klights_leader_api::NodeOutboxFuture<
            'a,
            Option<klights_leader_api::NodeRuntimeObservationCheckpoint>,
        > {
            klights_leader_api::NodeOutbox::get_runtime_observation_checkpoint(&self.inner, pod_uid)
        }
        fn delete_runtime_observation_checkpoint<'a>(
            &'a self,
            pod_uid: &'a str,
        ) -> klights_leader_api::NodeOutboxFuture<'a, ()> {
            klights_leader_api::NodeOutbox::delete_runtime_observation_checkpoint(
                &self.inner,
                pod_uid,
            )
        }
    }

    pub struct IntegrationNodeDispatcher {
        inner: OutboxDispatcher,
    }

    impl IntegrationNodeDispatcher {
        pub async fn dispatch_due_once(
            &self,
            now_ms: i64,
        ) -> Result<klights_kubelet::node_outbox::DispatchOutcome> {
            self.inner.dispatch_due_once(now_ms).await
        }

        pub fn rtt_estimate_ms(&self) -> i64 {
            self.inner.rtt_estimate_ms()
        }
    }

    #[derive(Clone)]
    pub struct IntegrationNodeDeliveryStore {
        stores: NodeLocalStores,
    }

    impl IntegrationNodeDeliveryStore {
        pub async fn open(connection_key: &'static str) -> Result<Self> {
            Ok(Self {
                stores: crate::datastore::node_local::selector::open_node_local(
                    crate::datastore::backend_kind::BackendKind::Sqlite,
                    None,
                    Arc::new(klights_supervisor::TaskSupervisor::new(
                        klights_supervisor::TaskCategoryConfig::default(),
                    )),
                    None,
                    connection_key,
                )
                .await?,
            })
        }

        pub async fn open_with_sqlite(
            connection_key: &'static str,
        ) -> Result<(Self, Option<Self>)> {
            let (stores, sqlite) =
                crate::datastore::node_local::selector::open_node_local_with_sqlite(
                    crate::datastore::backend_kind::BackendKind::Sqlite,
                    None,
                    Arc::new(klights_supervisor::TaskSupervisor::new(
                        klights_supervisor::TaskCategoryConfig::default(),
                    )),
                    None,
                    connection_key,
                )
                .await?;
            Ok((
                Self { stores },
                sqlite.map(|stores| Self {
                    stores: (*stores).clone(),
                }),
            ))
        }

        pub fn outbox(&self) -> IntegrationNodeOutbox {
            IntegrationNodeOutbox {
                inner: outbox_from_node_db(self.stores.clone()),
            }
        }

        pub fn outbox_with_notify(&self, notify: Arc<Notify>) -> IntegrationNodeOutbox {
            IntegrationNodeOutbox {
                inner: outbox_with_notify(self.stores.clone(), notify),
            }
        }

        pub fn dispatcher(
            &self,
            client: Arc<dyn LeaderOutboxDelivery>,
        ) -> IntegrationNodeDispatcher {
            IntegrationNodeDispatcher {
                inner: dispatcher_for_tests(self.stores.clone(), client),
            }
        }

        pub fn dispatcher_with_notify(
            &self,
            client: Arc<dyn LeaderOutboxDelivery>,
            notify: Arc<Notify>,
        ) -> IntegrationNodeDispatcher {
            IntegrationNodeDispatcher {
                inner: dispatcher_with_notify(self.stores.clone(), client, notify),
            }
        }

        pub fn dispatcher_with_rtt(
            &self,
            client: Arc<dyn LeaderOutboxDelivery>,
            rtt: Arc<klights_types::RttEstimator>,
        ) -> IntegrationNodeDispatcher {
            IntegrationNodeDispatcher {
                inner: dispatcher_with_rtt_estimator(self.stores.clone(), client, rtt),
            }
        }

        pub fn dispatcher_with_lease_renewal(
            &self,
            client: Arc<dyn LeaderOutboxDelivery>,
            supervisor: Arc<klights_supervisor::TaskSupervisor>,
            lease_ms: i64,
        ) -> IntegrationNodeDispatcher {
            IntegrationNodeDispatcher {
                inner: dispatcher_for_tests(self.stores.clone(), client)
                    .with_lease_renewal_for_test(supervisor, lease_ms),
            }
        }

        pub fn batch_dispatcher(
            &self,
            client: Arc<dyn LeaderOutboxDelivery>,
            batch_size: usize,
        ) -> IntegrationNodeDispatcher {
            IntegrationNodeDispatcher {
                inner: dispatcher_for_tests(self.stores.clone(), client)
                    .with_batch_mode(batch_size),
            }
        }

        pub fn production_dispatcher(
            &self,
            client: Arc<dyn LeaderOutboxDelivery>,
            notify: Arc<Notify>,
        ) -> IntegrationNodeDispatcher {
            IntegrationNodeDispatcher {
                inner: dispatcher_with_notify(self.stores.clone(), client, notify)
                    .with_batch_mode(klights_kubelet::node_outbox::PRODUCTION_DISPATCH_BATCH_SIZE),
            }
        }

        pub async fn enqueue_fixture_row(
            &self,
            row: IntegrationNodeDeliveryOutboxInsert,
        ) -> Result<()> {
            self.stores
                .legacy_enqueue_outbox(crate::datastore::node_local::OutboxInsert {
                    idempotency_key: row.idempotency_key,
                    enqueued_ms: row.enqueued_ms,
                    subject_key: row.subject_key,
                    subject_api_version: row.subject_api_version,
                    subject_kind: row.subject_kind,
                    subject_namespace: row.subject_namespace,
                    subject_name: row.subject_name,
                    subject_uid: row.subject_uid,
                    pod_uid: row.pod_uid,
                    operation: row.operation,
                    classification: row.classification,
                    payload_proto: row.payload_proto,
                    next_due_ms: row.next_due_ms,
                })
                .await
        }
        pub async fn claim_next_due(
            &self,
            now_ms: i64,
            lease_ms: i64,
            lease_token: &str,
        ) -> Result<Option<IntegrationNodeDeliveryOutboxRow>> {
            self.stores
                .legacy_claim_next_due_outbox(now_ms, lease_ms, lease_token)
                .await
                .map(|row| row.map(Into::into))
        }
        pub async fn fail_claim_attempt(
            &self,
            id: i64,
            lease_token: &str,
            backoff_until_ms: i64,
            error: &str,
        ) -> Result<bool> {
            self.stores
                .legacy_mark_outbox_attempt_failed(id, lease_token, backoff_until_ms, error)
                .await
        }
        pub async fn complete_claim(&self, id: i64, lease_token: &str) -> Result<bool> {
            self.stores.legacy_complete_outbox(id, lease_token).await
        }
        pub async fn claim_due_batch(
            &self,
            now_ms: i64,
            limit: usize,
            lease_ms: i64,
            lease_token: &str,
        ) -> Result<Vec<IntegrationNodeDeliveryOutboxRow>> {
            self.stores
                .legacy_claim_due_outbox_batch(now_ms, limit, lease_ms, lease_token)
                .await
                .map(|rows| rows.into_iter().map(Into::into).collect())
        }
        pub async fn requeue_expired_leases(&self, now_ms: i64) -> Result<usize> {
            self.stores
                .legacy_requeue_expired_outbox_leases(now_ms)
                .await
        }
        pub async fn next_wake_ms(&self, now_ms: i64) -> Result<Option<i64>> {
            self.stores.legacy_next_outbox_wake_ms(now_ms).await
        }
        pub async fn dead_letter_at_attempt_limit(&self, key: &str, max: i64) -> Result<bool> {
            self.stores
                .legacy_move_outbox_to_dead_letter_if_max_attempts(key, max)
                .await
        }
        pub async fn list_dead_letters(&self) -> Result<Vec<IntegrationNodeDeliveryDeadLetterRow>> {
            self.stores
                .legacy_list_dead_letter()
                .await
                .map(|rows| rows.into_iter().map(Into::into).collect())
        }
        pub async fn delete_dead_letter(&self, id: i64) -> Result<bool> {
            self.stores.legacy_delete_dead_letter(id).await
        }
        pub async fn replay_dead_letter(
            &self,
            id: i64,
            classification: klights_node_store::OutboxClassification,
        ) -> Result<bool> {
            self.stores
                .legacy_replay_dead_letter(id, classification)
                .await
        }
        pub async fn delivery_stats(&self) -> Result<IntegrationNodeDeliveryOutboxStats> {
            self.stores.legacy_outbox_stats().await.map(|stats| {
                IntegrationNodeDeliveryOutboxStats {
                    pending: stats.pending,
                    oldest_age_seconds: stats.oldest_age_seconds,
                    dead_letter_count: stats.dead_letter_count,
                    dispatch_total: stats.dispatch_total,
                    dispatch_errors_total: stats.dispatch_errors_total,
                }
            })
        }
        pub async fn upsert_pod_status_checkpoint(
            &self,
            uid: &str,
            namespace: &str,
            name: &str,
            rv: i64,
            status: serde_json::Value,
            updated_ms: i64,
        ) -> Result<()> {
            self.stores
                .legacy_upsert_pod_status_checkpoint(uid, namespace, name, rv, status, updated_ms)
                .await
        }
        pub async fn get_pod_status_checkpoint(
            &self,
            uid: &str,
        ) -> Result<Option<IntegrationNodeDeliveryPodStatusCheckpoint>> {
            self.stores
                .legacy_get_pod_status_checkpoint(uid)
                .await
                .map(|row| {
                    row.map(|checkpoint| IntegrationNodeDeliveryPodStatusCheckpoint {
                        pod_uid: checkpoint.pod_uid,
                        namespace: checkpoint.namespace,
                        pod_name: checkpoint.pod_name,
                        base_rv: checkpoint.base_rv,
                        applied_rv: checkpoint.applied_rv,
                        status: checkpoint.status,
                        updated_ms: checkpoint.updated_ms,
                    })
                })
        }
        pub async fn mark_pod_status_checkpoint_applied(
            &self,
            uid: &str,
            rv: i64,
            applied_ms: i64,
        ) -> Result<()> {
            self.stores
                .legacy_mark_pod_status_checkpoint_applied(uid, rv, applied_ms)
                .await
        }
        pub async fn insert_dead_letter(
            &self,
            row: IntegrationNodeDeliveryDeadLetterInsert<'_>,
        ) -> Result<()> {
            self.stores
                .insert_dead_letter_test_only(crate::datastore::node_local::DeadLetterTestInsert {
                    idempotency_key: row.idempotency_key,
                    operation: row.operation,
                    subject_key: row.subject_key,
                    subject_api_version: row.subject_api_version,
                    subject_kind: row.subject_kind,
                    subject_namespace: row.subject_namespace,
                    subject_name: row.subject_name,
                    subject_uid: row.subject_uid,
                    pod_uid: row.pod_uid,
                    payload_proto: row.payload_proto,
                    attempts: row.attempts,
                    last_error: row.last_error,
                    moved_at_ms: row.moved_at_ms,
                })
                .await
        }
        pub async fn outbox_stream_position(&self, key: &str) -> Result<Option<(i64, i64)>> {
            self.stores.outbox_stream_position_for_test(key).await
        }
        pub async fn set_outbox_operation(&self, key: &str, operation: &str) -> Result<()> {
            self.stores
                .set_outbox_operation_for_test(key, operation)
                .await
        }
        pub async fn outbox_operation(&self, key: &str) -> Result<Option<String>> {
            self.stores.outbox_operation_for_test(key).await
        }

        pub async fn client_id(&self) -> Result<Option<String>> {
            self.stores
                .identity()
                .get_node_meta("outbox_client_id")
                .await
                .map_err(|error| anyhow::anyhow!(error.to_string()))
        }
    }

    #[cfg(test)]
    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct OutboxPayload {
        pub command: klights_cluster_core::StorageCommand,
    }

    #[cfg(test)]
    impl OutboxPayload {
        pub(crate) fn from_command(command: klights_cluster_core::StorageCommand) -> Self {
            Self { command }
        }

        pub(crate) fn encode_protobuf(&self) -> Result<Vec<u8>> {
            Ok(
                klights_leader_rpc::storage_wire_codec::encode_outbox_payload_protobuf(
                    &klights_cluster_core::OutboxPayload::new(self.command.clone()),
                )?,
            )
        }

        pub(crate) fn decode_protobuf(bytes: &[u8]) -> Result<Self> {
            Ok(Self {
                command: klights_leader_rpc::storage_wire_codec::decode_outbox_payload_protobuf(
                    bytes,
                )?
                .into_command(),
            })
        }
    }

    pub(crate) trait NodeLocalStoresRef {
        fn node_local_stores(&self) -> &NodeLocalStores;
    }

    impl NodeLocalStoresRef for NodeLocalStores {
        fn node_local_stores(&self) -> &NodeLocalStores {
            self
        }
    }

    impl NodeLocalStoresRef for Arc<NodeLocalStores> {
        fn node_local_stores(&self) -> &NodeLocalStores {
            self.as_ref()
        }
    }

    pub(crate) fn outbox_stores(node_db: &NodeLocalStores) -> OutboxStores {
        OutboxStores::new(
            node_db.outbox_producer(),
            node_db.outbox_dispatcher(),
            node_db.pod_status_checkpoints(),
            node_db.runtime_observation_checkpoints(),
            node_db.outbox_status_stamps(),
        )
    }

    pub(crate) fn outbox_from_node_db(node_db: impl NodeLocalStoresRef) -> Outbox {
        outbox_with_notify(node_db, Arc::new(Notify::new()))
    }

    pub(crate) fn outbox_with_notify(
        node_db: impl NodeLocalStoresRef,
        notify: Arc<Notify>,
    ) -> Outbox {
        let node_db = node_db.node_local_stores();
        Outbox::compose(
            outbox_stores(node_db),
            crate::bootstrap::composition_adapters::outbox_payload_codec_adapter::new_codec(),
            notify,
            Arc::new(klights_supervisor::SystemWallClock),
        )
    }

    pub(crate) fn dispatcher_for_tests(
        node_db: impl NodeLocalStoresRef,
        client: Arc<dyn LeaderOutboxDelivery>,
    ) -> OutboxDispatcher {
        dispatcher_with_notify(node_db, client, Arc::new(Notify::new()))
    }

    pub(crate) fn dispatcher_with_notify(
        node_db: impl NodeLocalStoresRef,
        client: Arc<dyn LeaderOutboxDelivery>,
        notify: Arc<Notify>,
    ) -> OutboxDispatcher {
        let node_db = node_db.node_local_stores();
        OutboxDispatcher::new(
            outbox_stores(node_db),
            crate::bootstrap::composition_adapters::outbox_payload_codec_adapter::new_codec(),
            client,
            notify,
            Arc::new(klights_supervisor::SystemWallClock),
        )
    }

    pub(crate) fn dispatcher_with_rtt_estimator(
        node_db: impl NodeLocalStoresRef,
        client: Arc<dyn LeaderOutboxDelivery>,
        rtt: Arc<klights_types::RttEstimator>,
    ) -> OutboxDispatcher {
        let node_db = node_db.node_local_stores();
        OutboxDispatcher::compose_with_rtt_estimator_for_test(
            outbox_stores(node_db),
            crate::bootstrap::composition_adapters::outbox_payload_codec_adapter::new_codec(),
            client,
            Arc::new(Notify::new()),
            rtt,
            Arc::new(klights_supervisor::SystemWallClock),
        )
    }
}

pub use node_delivery_support::{
    IntegrationNodeDeliveryCluster, IntegrationNodeDeliveryDeadLetterInsert,
    IntegrationNodeDeliveryOutboxInsert, IntegrationNodeDeliveryStore, IntegrationNodeDispatcher,
    IntegrationNodeOutbox, author_bound_pod_finalization,
};
