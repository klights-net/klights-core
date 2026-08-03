//! Base-repository-only assembly for full-stack API integration tests.
//!
//! This module exists only behind `integration-test-harness`; normal builds
//! neither compile nor export it.

use std::sync::Arc;

use crate::datastore::DatastoreHandle;
use klights_reconcile_api::ControllerDispatcherPort as _;

/// Opaque root datastore capability for base-repository integration fixtures.
///
/// This alias is compiled only with `integration-test-harness`; production and
/// native-service APIs do not expose a datastore surface.
pub type IntegrationDatastoreHandle = DatastoreHandle;
pub type IntegrationWatchEvent = crate::watch::WatchEvent;
pub use klights_cluster_datastore::sqlite::embedded::{
    ResourceMutationPause as IntegrationResourceMutationPause,
    ResourceMutationPauseOperation as IntegrationResourceMutationPauseOperation,
};

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
    let event = crate::watch::WatchEvent::added(object);
    let pending = crate::datastore::staged_post_commit_from_event(event);
    db.commit_observation_sink().observe(&[pending]);
}

pub async fn reconcile_namespace_termination_for_integration(
    db: IntegrationDatastoreHandle,
    namespace: &str,
) -> Result<(), k8s_native_service::AppError> {
    let store = crate::api_state_adapter::RootNamespaceTerminationStore::new(db);
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
    let store = crate::api_state_adapter::RootNamespaceTerminationStore::new(db);
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
/// path while deriving the authenticated worker identity from the lifecycle-
/// authored command under test.
struct IntegrationOutboxDelivery {
    embedded: Arc<klights_replication::leader_api::EmbeddedOutboxDelivery>,
    codec: Arc<dyn klights_leader_api::OutboxPayloadCodec>,
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
            let authenticated_node = decoded_command
                .as_ref()
                .ok()
                .and_then(|command| match command {
                    klights_cluster_core::StorageCommand::FinalizeBoundPod {
                        node_name, ..
                    }
                    | klights_cluster_core::StorageCommand::UpdateNodeDataplane {
                        node_name, ..
                    } => Some(node_name.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| "test-node".to_string());
            let effect = self
                .embedded
                .deliver_authenticated_outbox_command_effect(
                    authenticated_node,
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

impl IntegrationHeldSupervisorTask {
    pub fn abort(&self) {
        self.handle.abort();
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
            crate::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
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
            crate::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
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
            crate::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
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
            crate::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
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
            crate::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
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
            crate::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
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
            crate::signing_key_state_adapter::RootServiceAccountSigningKeyState::from_pem(
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
            crate::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
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
            crate::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
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
            crate::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
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
            crate::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
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
            crate::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
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
            crate::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
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
            crate::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
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
            crate::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
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
        let db = crate::datastore::test_support::in_memory().await;
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
            crate::datastore::test_support::sqlite_passive_read_ports(&db)
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
        let outbox_codec = crate::outbox_payload_codec_adapter::new_codec();
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
        let root_pod_parts = crate::pod_repository_composition::build_pod_repository_parts(
            crate::pod_repository_composition::PodRepositoryBuildConfig {
                db: datastore.clone(),
                pod_workqueue_store: Some(node_local.pod_workqueue()),
                supervisor: supervisor.clone(),
                side_effects: side_effects.clone(),
                metrics: metrics.clone(),
                pod_network_cache: node_local.pod_network_cache(),
                assignment_waiter: Arc::new(klights_networking::PodNetworkAssignmentBus::new()),
                scheduling_mode:
                    crate::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
                outbox: Some(outbox),
                cluster_api: Some(resource_query.clone()),
                controller_identity: controller_identity.clone(),
                api_identity: identity.clone(),
                gc_coordination: gc_coordination.clone(),
            },
            None,
        );
        let pod_api = root_pod_parts.api;
        let pod_subresource = root_pod_parts.subresource;
        let pod_repository = Arc::new(root_pod_parts.repository_parts.repository);
        let api_pod_repository = crate::api_state_adapter::RootApiPodRepository::new(
            pod_repository.clone(),
            pod_api.clone(),
            pod_subresource.clone(),
        );
        let controller_pod_port = Arc::new(
            crate::controller_runtime_adapter::RootControllerPodPort::new(
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
            crate::positioned_watch_adapter::for_test(&passive_reads, datastore.clone());
        let watch_signals = crate::watch_commit_observation_adapter::test_signal_source(&datastore);
        let generated = crate::generated_handler_adapter::GeneratedHandlerAdapter::new(
            datastore.clone(),
            watch_signals.clone(),
            positioned_watch.clone(),
            klights_supervisor::FileProcessExecutor::new(supervisor.clone()),
            supervisor.clone(),
            config.data_root.join("etc/ca.crt"),
            controller_identity.clone(),
        );
        let network = crate::networking::test_support::mock_network(datastore.clone());
        let controller_leader_ports = Arc::new(
            crate::controller_runtime_adapter::RootControllerLeaderPort::new(datastore.clone()),
        );
        let non_pod_finalization = Arc::new(
            crate::gc_delete_adapter::GcNonPodFinalizationAdapter::new(datastore.clone()),
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
                crate::controller_runtime_adapter::RootControllerReconcilePort::new(
                    non_pod_finalization.clone(),
                ),
            ),
            network: Arc::new(
                crate::controller_runtime_adapter::RootControllerNetworkPort::new(
                    network.services().clone(),
                ),
            ),
            effects: Arc::new(
                crate::controller_runtime_adapter::RootControllerEffectPort::new(
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
        let hpa_controller = crate::hpa_controller_adapter::controller(
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
        let pod_logs = crate::node_log_runtime_adapter::pod_log_capabilities(
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
                crate::watch_stream_adapter::DatastoreWatchStreamAdapter::new(
                    datastore.clone(),
                    watch_signals,
                    positioned_watch.clone(),
                ),
            ),
            crate::api_state_adapter::RootNamespaceTerminationStore::new(datastore.clone()),
            resource_query,
            resource_command,
            finalizer_lifecycle,
            mutation_effects,
            crate::list_query_adapter::DatastoreListResourceVersionPort::new(datastore.clone()),
            crate::list_query_adapter::DatastoreNamespaceListPort::new(datastore.clone()),
            crate::resource_quota_admission_adapter::ResourceQuotaAdmissionAdapter::new(
                datastore.clone(),
            ),
            crate::resource_admission_adapter::ResourceAdmissionAdapter::new(
                identity,
                datastore.clone(),
            ),
            crate::custom_resource_read_adapter::CustomResourceReadAdapter::new(
                datastore.clone(),
                crate::watch_commit_observation_adapter::test_signal_source(&datastore),
                positioned_watch,
                supervisor.clone(),
            ),
            generated.clone(),
            generated.clone(),
            generated.clone(),
            generated,
            Arc::new(
                crate::gc_delete_adapter::GcOwnerLifecycleAdapter::new_with_coordination(
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
            crate::api_state_adapter::RootApiFailureMetrics::new(metrics),
            crate::api_state_adapter::RootApiNodeLeaseObservations::new(node_lease_tracker.clone()),
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
        let finalized_inline = self
            .pod_repository
            .finalize_pod_deletion_after_actor_cleanup(namespace, name, uid)
            .await?;
        if finalized_inline {
            return Ok(true);
        }
        self.drain_node_outbox().await?;
        let live = self
            .datastore
            .get_resource("v1", "Pod", Some(namespace), name)
            .await?;
        Ok(live.is_none_or(|pod| pod.uid != uid))
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
    ) -> tokio::sync::broadcast::Receiver<crate::watch::WatchEvent> {
        crate::watch_commit_observation_adapter::subscribe_test_events(
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
