//! Base-repository-only assembly for full-stack API integration tests.
//!
//! This module exists only as a root-private base-owned test module; normal builds
//! neither compile nor export it.

use std::sync::Arc;

use crate::datastore::DatastoreHandle;
use k8s_native_service::test_support::admission::{
    DeterministicApiIdentity, deterministic_uuid_v4,
};
use klights_auth::test_support::{
    AllowAllAuthorizer, IntegrationCsrSignerObservation, recording_csr_signer,
};
use klights_reconcile_api::ControllerDispatcherPort as _;

pub struct IntegrationHeldSupervisorTask {
    handle: klights_supervisor::SupervisedJoinHandle<()>,
}

impl IntegrationHeldSupervisorTask {
    pub fn abort(&self) {
        self.handle.abort();
    }
}

/// P12.2c-owned finalizer fixture bridge. Its persistence input is the
/// canonical narrow resource fixture; it exposes no datastore capability.
pub async fn mark_foreground_deletion_for_integration(
    store: klights_cluster_datastore::test_support::ResourceTestStore,
    target: k8s_native_service::generic_command::ResourceDeleteTarget<'_>,
    initial_resource: klights_cluster_core::Resource,
    delete_preconditions: klights_cluster_core::ResourcePreconditions,
) -> Result<klights_cluster_core::Resource, k8s_native_service::AppError> {
    k8s_native_service::generic_command::mark_foreground_deletion_with_retry(
        &store,
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

/// P12.2c-owned finalizer fixture bridge. See
/// [`mark_foreground_deletion_for_integration`] for its narrow boundary.
pub async fn complete_non_foreground_delete_for_integration(
    store: klights_cluster_datastore::test_support::ResourceTestStore,
    request: k8s_native_service::generic_command::NonForegroundDeleteRequest<'_>,
) -> Result<k8s_native_service::generic_command::DeleteCompletion, k8s_native_service::AppError> {
    k8s_native_service::generic_command::complete_non_foreground_delete_with_live_recheck(
        &store, request,
    )
    .await
}

#[derive(Default)]
pub(super) struct DeterministicControllerIdentity {
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
    datastore: DatastoreHandle,
    resource_store: klights_cluster_datastore::test_support::ResourceTestStore,
    commit_watch_fixture: Arc<klights_watch::test_support::CommitWatchFixture>,
    nodeport_alloc: Arc<klights_controllers::service::NodePortAllocator>,
    pod_query: Arc<dyn klights_pod_api::PodQuery>,
    pod_finalization: Arc<dyn klights_pod_api::BoundPodFinalization>,
    _node_local: Arc<crate::bootstrap::node_store::NodeLocalStores>,
    #[allow(dead_code)]
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

    pub async fn with_toggle_failing_watch_history() -> anyhow::Result<(
        Self,
        klights_cluster_datastore::test_support::WatchHistoryFailureControl,
    )> {
        let control = klights_cluster_datastore::test_support::WatchHistoryFailureControl::new();
        let harness = Self::assemble(
            Arc::new(AllowAllAuthorizer),
            None,
            crate::bootstrap::composition_adapters::signing_key_state_adapter::RootServiceAccountSigningKeyState::for_test(),
            None,
            None,
            Some(control.clone()),
            None,
            None,
            None,
            None,
            false,
        )
        .await?;
        Ok((harness, control))
    }

    pub async fn with_mutation_side_effect_factory<F>(factory: F) -> anyhow::Result<Self>
    where
        F: FnOnce(
                klights_cluster_datastore::test_support::ResourceTestStore,
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
        let (csr_signer, observation) = recording_csr_signer();
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
                csr_signer: Some(csr_signer),
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
        watch_history_failure: Option<
            klights_cluster_datastore::test_support::WatchHistoryFailureControl,
        >,
        mutation_side_effects_factory: Option<
            Box<
                dyn FnOnce(
                        klights_cluster_datastore::test_support::ResourceTestStore,
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
        watch_history_failure: Option<
            klights_cluster_datastore::test_support::WatchHistoryFailureControl,
        >,
        mutation_side_effects_factory: Option<
            Box<
                dyn FnOnce(
                        klights_cluster_datastore::test_support::ResourceTestStore,
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
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(task_categories));
        let commit_watch_fixture =
            Arc::new(klights_watch::test_support::CommitWatchFixture::new(64));
        let executor = klights_cluster_datastore::sqlite::open_in_memory(
            supervisor.clone(),
            "sqlite:native-api-integration",
        )
        .await?;
        let db =
            crate::datastore::sqlite::Datastore::new_in_memory_with_watch_and_executor_with_sink(
                executor,
                commit_watch_fixture.clone(),
                crate::bootstrap::composition_adapters::outbox_response_codec_adapter::new_codec(),
                Arc::new(klights_supervisor::SystemWallClock),
            )
            .await?;
        let passive_reads = if let Some(control) = watch_history_failure {
            let focused_reads = db.focused_read_store();
            crate::datastore::selector::PassiveReadPorts::new(
                focused_reads.clone(),
                klights_cluster_datastore::test_support::toggle_failing_watch_history_for_test_support(
                    focused_reads.clone(),
                    control,
                ),
                focused_reads,
            )
        } else {
            crate::datastore::selector::sqlite_passive_read_ports(&db)
        };
        let resource_store =
            klights_cluster_datastore::test_support::ResourceTestStore::from_embedded_for_test_support(
                db.canonical_embedded_for_test_support(),
            );
        let datastore: DatastoreHandle = Arc::new(db.clone());
        let config = crate::KlightsConfig::test_default();
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
        let leader_rx =
            crate::bootstrap::composition_adapters::authority_adapter::always_leader_watch();
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
                crate::bootstrap::composition_adapters::authority_adapter::always_leader_authority(
                ),
            ),
        );
        let node_local = Arc::new(
            crate::bootstrap::node_store::open_node_local(
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
                        crate::bootstrap::composition_adapters::authority_adapter::always_leader_authority(),
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
        let pod_repository_config = crate::bootstrap::pod_repository_composition::PodRepositoryBuildConfig {
            db: datastore.clone(),
            pod_workqueue_store: Some(node_local.pod_workqueue()),
            supervisor: supervisor.clone(),
            side_effects: side_effects.clone(),
            metrics: metrics.clone(),
            pod_network_cache: node_local.pod_network_cache(),
            assignment_waiter: Arc::new(klights_networking::PodNetworkAssignmentBus::new()),
            scheduling_mode: crate::bootstrap::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
            outbox: Some(outbox),
            cluster_api: Some(resource_query.clone()),
            resource_commands: None,
            remote_delivery_required: false,
            controller_identity: controller_identity.clone(),
            #[cfg(not(test))]
            api_identity: identity.clone(),
            #[cfg(not(test))]
            gc_coordination: gc_coordination.clone(),
            scheduler_bind_gate: None,
            post_write_maintenance_notify: None,
        };
        let (
            pod_query,
            pod_snapshot,
            pod_update,
            _pod_status_writer,
            _pod_workqueue,
            _pod_network_assignment,
            _pod_host_ip,
            _background,
            _deletion_finalizer,
            _dirty_counter,
            mutation_reconcile,
            gc_delete,
            eviction_admission,
            namespace_bootstrap,
            namespace_termination_queue,
            pod_api,
            pod_subresource,
            _pod_scheduling,
            _watch_source,
            bound_pod_finalization,
            _deferred_runtime,
            _test_api,
            _test_subresource,
        ) = crate::bootstrap::pod_repository_composition::build_pod_repository_parts(
            pod_repository_config,
            None,
        );
        let pod_api = pod_api.expect("native API root Pod API service");
        let pod_subresource = pod_subresource.expect("native API root Pod subresource service");
        let pod_query = pod_query;
        let api_pod_repository =
            crate::bootstrap::composition_adapters::api_state_adapter::RootApiPodRepository::new(
                pod_query.clone(),
                pod_snapshot.clone(),
                mutation_reconcile.clone(),
                namespace_termination_queue.clone(),
                eviction_admission.clone(),
                namespace_bootstrap.clone(),
                pod_api.clone(),
                pod_subresource.clone(),
            );
        let controller_pod_port = Arc::new(
            crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerPodPort::new(
                pod_query.clone(),
                pod_update.clone(),
                pod_api.clone(),
                pod_subresource.clone(),
            ),
        );
        let controller_pod_mutations =
            Arc::new(klights_controllers::ControllerPodMutationAdapter::new(
                controller_pod_port.clone(),
                controller_pod_port.clone(),
            ));
        side_effects.set_pod_ports(pod_query.clone(), gc_delete.clone());
        let finalizer_lifecycle = crate::bootstrap::finalizer_lifecycle_adapter::
            DatastoreFinalizerLifecycleAdapter::new_with_coordination(
                datastore.clone(),
                resource_command.clone(),
                gc_delete.clone(),
                side_effects.clone(),
                metrics.clone(),
                gc_coordination.clone(),
            );
        let mutation_side_effects = mutation_side_effects_factory
            .map(|factory| factory(resource_store.clone()))
            .unwrap_or_else(|| side_effects.clone());
        let mutation_effects = klights_controllers::side_effects::ResourceMutationEffects::new(
            mutation_side_effects,
            metrics.clone(),
        );
        let positioned_watch = crate::bootstrap::composition_adapters::positioned_watch_adapter::datastore_positioned_watch_service(
            &passive_reads,
            datastore.clone(),
            commit_watch_fixture.signal_source(),
        );
        let watch_signals = commit_watch_fixture.signal_source();
        let generated = crate::bootstrap::composition_adapters::generated_handler_adapter::GeneratedHandlerAdapter::new(
            crate::bootstrap::composition_adapters::generated_handler_adapter::GeneratedHandlerStorage::new(
                datastore.clone(),
                crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort::resource_commands_for_test(datastore.clone()),
            ),
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
            deployment_pod_mutation: controller_pod_mutations.clone(),
            replicaset_pod_mutation: controller_pod_mutations.clone(),
            statefulset_pod_mutation: controller_pod_mutations.clone(),
            daemonset_pod_mutation: controller_pod_mutations.clone(),
            job_pod_mutation: controller_pod_mutations.clone(),
            replicationcontroller_pod_mutation: controller_pod_mutations.clone(),
            pod_delete_sink: gc_delete.clone(),
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
                api_pod_repository.clone(),
                gc_delete.clone(),
                controller_pod_mutations,
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
            resource_command.clone(),
            finalizer_lifecycle,
            mutation_effects,
            crate::bootstrap::composition_adapters::list_query_adapter::DatastoreListResourceVersionPort::new(Arc::new(
                crate::bootstrap::composition_adapters::leader_maintenance_adapter::ClusterStoreLeaderMaintenance::new(
                    datastore.clone(),
                    Arc::new(crate::bootstrap::outbox_apply_adapter::BackendProposalFixture::new(datastore.clone())),
                    crate::bootstrap::composition_adapters::authority_adapter::always_leader_watch(),
                ),
            )),
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
                commit_watch_fixture.signal_source(),
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
                    resource_command,
                    gc_delete.clone(),
                    gc_coordination,
                ),
            ),
            api_pod_repository.clone(),
            crd_registry.clone(),
            crate::bootstrap::service_adapters::ApiServiceWriteAllocator::new(
                Arc::new(
                    crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort::new(
                        datastore.clone(),
                    ),
                ),
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
            resource_store,
            commit_watch_fixture,
            nodeport_alloc,
            pod_query: api_pod_repository,
            pod_finalization: bound_pod_finalization,
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
        let request = klights_pod_api::BoundPodFinalizationRequest::try_new(
            klights_types::PodIdentity::new(namespace, name, uid),
        )?;
        let outcome = self
            .pod_finalization
            .finalize_bound_pod(request)
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        Ok(matches!(
            outcome,
            klights_pod_api::BoundPodFinalizationOutcome::Removed
                | klights_pod_api::BoundPodFinalizationOutcome::Accepted
                | klights_pod_api::BoundPodFinalizationOutcome::IdentityChanged
        ))
    }

    #[allow(dead_code)]
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
            let queued = self.controller_dispatcher.pending_reconcile_keys().await;
            if queued.is_empty() {
                return Ok(drained);
            }
            for key in queued {
                if !drained.contains(&key) {
                    drained.push(key);
                }
            }

            let cancel = tokio_util::sync::CancellationToken::new();
            let worker = self.spawn_controller_worker(cancel.clone()).await?;
            let ready_queue_drained =
                tokio::time::timeout(std::time::Duration::from_secs(30), async {
                    loop {
                        if self
                            .controller_dispatcher
                            .pending_reconcile_keys()
                            .await
                            .is_empty()
                        {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                })
                .await;
            cancel.cancel();
            let worker_result = worker.join().await;
            if ready_queue_drained.is_err() {
                worker_result?;
                anyhow::bail!("controller reconcile ready-queue drain timed out");
            }
            worker_result?;
        }
        anyhow::bail!("controller reconcile drain exceeded 1024 worker rounds")
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
            self.pod_query.as_ref(),
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
        self.resource_store
            .create_resource(kind.api_version(), kind.kind(), namespace, name, value)
            .await
    }

    pub async fn seed_endpoint_namespace(
        &self,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        self.resource_store.create_namespace(name, value).await
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
        self.resource_store
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
            .resource_store
            .list_resources(
                EndpointFixtureKind::EndpointSlice.api_version(),
                EndpointFixtureKind::EndpointSlice.kind(),
                Some(namespace),
                klights_cluster_store::ResourceListOptions::new(label_selector, None, None, None),
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
        self.resource_store
            .update_resource("v1", "Endpoints", Some(namespace), name, value, expected_rv)
            .await
    }

    pub async fn remove_endpoints(&self, namespace: &str, name: &str) -> anyhow::Result<()> {
        self.resource_store
            .delete_resource("v1", "Endpoints", Some(namespace), name)
            .await
    }

    pub async fn remove_endpoint_slice(&self, namespace: &str, name: &str) -> anyhow::Result<()> {
        self.resource_store
            .delete_resource(
                "discovery.k8s.io/v1",
                "EndpointSlice",
                Some(namespace),
                name,
            )
            .await
    }

    pub async fn endpoint_fixture_resource_version(&self) -> anyhow::Result<i64> {
        self.resource_store.get_current_resource_version().await
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
            self.pod_query.as_ref(),
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
            self.pod_query.as_ref(),
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

    pub async fn spawn_controller_worker(
        &self,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<klights_supervisor::SupervisedJoinHandle<()>, klights_supervisor::TaskAdmissionError>
    {
        let dispatcher = self.controller_dispatcher.clone();
        self.task_supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Background,
                "native_api_controller_worker",
                async move { dispatcher.run_worker_pool(1, cancel).await },
            )
            .await
    }

    /// Returns the neutral native-service registry already wired into this
    /// router. CRD fixture behavior remains owned by native-service support.
    pub fn crd_registry(&self) -> klights_leader_api::CrdRegistry {
        self.crd_registry.clone()
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

    /// Canonical narrow persistence fixture bound to this router's exact store.
    pub fn resource_store(&self) -> klights_cluster_datastore::test_support::ResourceTestStore {
        self.resource_store.clone()
    }

    pub fn commit_watch_fixture(&self) -> Arc<klights_watch::test_support::CommitWatchFixture> {
        self.commit_watch_fixture.clone()
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
