#![cfg(test)]

//! Private root composition fixtures for native Kubernetes API integration tests.
//!
//! This module deliberately exposes only the assembled router and the
//! integration-only datastore capability to sibling root-composition tests.
//! Native API implementation modules remain private to `k8s-native-service`.

pub(crate) use super::assembly_support::support::*;
pub use k8s_native_service::test_support::streaming::RemoteExecSyncWebSocketFixture;
pub use klights_auth::test_support::IntegrationCsrSignerObservation;
pub use klights_cluster_datastore;
pub use klights_cluster_datastore::test_support::{
    ResourceMutationPause as IntegrationResourceMutationPause,
    ResourceMutationPauseOperation as IntegrationResourceMutationPauseOperation,
    WatchHistoryFailureControl as IntegrationWatchHistoryFailureControl,
};

pub type IntegrationWatchEvent = klights_watch::WatchEvent;

use klights_auth::bootstrap_token::BootstrapTokenScopePolicy as _;
use std::sync::Arc;

#[derive(Clone)]
struct ResourceAdmissionQuery {
    store: klights_cluster_datastore::test_support::ResourceTestStore,
}

#[async_trait::async_trait]
impl k8s_native_service::admission::AdmissionQuery for ResourceAdmissionQuery {
    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<
        Option<k8s_native_service::admission::AdmissionResource>,
        k8s_native_service::admission::AdmissionDependencyError,
    > {
        self.store
            .get_resource(api_version, kind, namespace, name)
            .await
            .map(|resource| {
                resource.map(
                    |resource| k8s_native_service::admission::AdmissionResource {
                        name: resource.name,
                        data: resource.data,
                    },
                )
            })
            .map_err(|error| {
                k8s_native_service::admission::AdmissionDependencyError::new(error.to_string())
            })
    }

    async fn list_resources(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        label_selector: Option<&str>,
    ) -> Result<
        Vec<k8s_native_service::admission::AdmissionResource>,
        k8s_native_service::admission::AdmissionDependencyError,
    > {
        self.store
            .list_resources(
                api_version,
                kind,
                namespace,
                klights_cluster_store::ResourceListOptions::new(label_selector, None, None, None),
            )
            .await
            .map(|list| {
                list.items
                    .into_iter()
                    .map(
                        |resource| k8s_native_service::admission::AdmissionResource {
                            name: resource.name,
                            data: resource.data,
                        },
                    )
                    .collect()
            })
            .map_err(|error| {
                k8s_native_service::admission::AdmissionDependencyError::new(error.to_string())
            })
    }
}

#[derive(Clone)]
struct ResourceBoundTokenSubjects {
    store: klights_cluster_datastore::test_support::ResourceTestStore,
}

impl ResourceBoundTokenSubjects {
    async fn uid(
        &self,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<String>, klights_leader_api::ClusterIdentityError> {
        self.store
            .get_resource("v1", kind, namespace, name)
            .await
            .map(|resource| resource.map(|resource| resource.uid))
            .map_err(|error| {
                klights_leader_api::ClusterIdentityError::dependency_failure(error.to_string())
            })
    }
}

impl klights_leader_api::LeaderBoundTokenSubjectLookup for ResourceBoundTokenSubjects {
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

pub struct AllowAllAuthorizer;

#[async_trait::async_trait]
impl klights_auth::authorizer::Authorizer for AllowAllAuthorizer {
    async fn authorize(
        &self,
        _identity: &klights_auth::AuthenticatedIdentity,
        _request: &klights_auth::request_attributes::AuthorizationRequest,
    ) -> klights_auth::authorizer::AuthorizationDecision {
        klights_auth::authorizer::AuthorizationDecision::allow("parent integration allow-all")
    }
}

pub struct RecordingAuthorizer {
    requests: tokio::sync::Mutex<
        Vec<(
            klights_auth::AuthenticatedIdentity,
            klights_auth::request_attributes::AuthorizationRequest,
        )>,
    >,
    decision: klights_auth::authorizer::AuthorizationDecision,
}

impl RecordingAuthorizer {
    pub fn allow() -> Self {
        Self::new(klights_auth::authorizer::AuthorizationDecision::allow(
            "parent integration recording allow",
        ))
    }

    pub fn deny(reason: &str) -> Self {
        Self::new(klights_auth::authorizer::AuthorizationDecision::deny(
            reason,
        ))
    }

    fn new(decision: klights_auth::authorizer::AuthorizationDecision) -> Self {
        Self {
            requests: tokio::sync::Mutex::new(Vec::new()),
            decision,
        }
    }

    pub async fn take_requests(
        &self,
    ) -> Vec<(
        klights_auth::AuthenticatedIdentity,
        klights_auth::request_attributes::AuthorizationRequest,
    )> {
        std::mem::take(&mut *self.requests.lock().await)
    }
}

#[async_trait::async_trait]
impl klights_auth::authorizer::Authorizer for RecordingAuthorizer {
    async fn authorize(
        &self,
        identity: &klights_auth::AuthenticatedIdentity,
        request: &klights_auth::request_attributes::AuthorizationRequest,
    ) -> klights_auth::authorizer::AuthorizationDecision {
        self.requests
            .lock()
            .await
            .push((identity.clone(), request.clone()));
        self.decision.clone()
    }
}

pub struct TestAppState {
    harness: NativeApiTestHarness,
}

impl TestAppState {
    pub fn router(&self) -> axum::Router {
        self.harness.router()
    }

    pub fn router_with_authority(&self, is_leader: bool) -> axum::Router {
        self.harness.router_with_authority(is_leader)
    }

    pub async fn record_node_lease(
        &self,
        node_name: &str,
        lease: &serde_json::Value,
    ) -> anyhow::Result<()> {
        self.harness.record_node_lease(node_name, lease).await
    }

    pub fn resource_store(&self) -> klights_cluster_datastore::test_support::ResourceTestStore {
        self.harness.resource_store()
    }

    pub fn commit_watch_fixture(&self) -> Arc<klights_watch::test_support::CommitWatchFixture> {
        self.harness.commit_watch_fixture()
    }

    pub fn resource_mutation(&self) -> TestResourceMutation {
        TestResourceMutation {
            store: self.resource_store(),
        }
    }

    pub fn install_resource_mutation_pause(
        &self,
        operation: IntegrationResourceMutationPauseOperation,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Arc<crate::bootstrap::native_api_composition::support::IntegrationResourceMutationPause>
    {
        self.resource_store().install_resource_mutation_pause(
            operation,
            api_version,
            kind,
            namespace,
            name,
        )
    }

    pub fn nodeport_exhaustion_fixture(
        &self,
    ) -> klights_controllers::test_support::NodePortExhaustionFixture {
        self.harness.nodeport_exhaustion_fixture()
    }

    pub async fn finalize_pod_deletion_after_actor_cleanup(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<bool> {
        let outcome = self
            .bound_pod_finalization_fixture()
            .finalize(namespace, name, uid)
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        Ok(matches!(
            outcome,
            klights_pod_api::BoundPodFinalizationOutcome::Removed
                | klights_pod_api::BoundPodFinalizationOutcome::Accepted
                | klights_pod_api::BoundPodFinalizationOutcome::IdentityChanged
        ))
    }

    pub fn bound_pod_finalization_fixture(
        &self,
    ) -> klights_pod_api::test_support::BoundPodFinalizationFixture {
        self.harness.bound_pod_finalization_fixture()
    }

    pub fn controller_runtime_fixture(
        &self,
    ) -> klights_controllers::test_support::ControllerRuntimeFixture {
        self.harness.controller_runtime_fixture()
    }

    pub fn endpoint_reconcile_fixture(
        &self,
    ) -> klights_controllers::test_support::EndpointReconcileFixture {
        self.harness.endpoint_reconcile_fixture()
    }

    pub fn endpoint_resource_fixture(
        &self,
    ) -> klights_cluster_datastore::test_support::EndpointResourceFixture {
        self.harness.endpoint_resource_fixture()
    }

    pub async fn reconcile_endpointslice(
        &self,
        service_name: &str,
        service_uid: &str,
        namespace: &str,
        selector: Option<&serde_json::Value>,
        ports: Option<&serde_json::Value>,
    ) -> anyhow::Result<()> {
        self.endpoint_reconcile_fixture()
            .reconcile_endpointslice(service_name, service_uid, namespace, selector, ports)
            .await
    }

    pub async fn seed_endpoint_namespace(
        &self,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        self.endpoint_resource_fixture()
            .seed_namespace(name, value)
            .await
    }

    pub async fn seed_endpoint_pod(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        self.endpoint_resource_fixture()
            .seed_pod(namespace, name, value)
            .await
    }

    pub async fn seed_endpoint_service(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        self.endpoint_resource_fixture()
            .seed_service(namespace, name, value)
            .await
    }

    pub async fn seed_endpoints(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        self.endpoint_resource_fixture()
            .seed_endpoints(namespace, name, value)
            .await
    }

    pub async fn seed_endpoint_slice(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        self.endpoint_resource_fixture()
            .seed_endpoint_slice(namespace, name, value)
            .await
    }

    pub async fn observe_endpoints(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        self.endpoint_resource_fixture()
            .endpoints(namespace, name)
            .await
    }

    pub async fn observe_endpoint_slice(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        self.endpoint_resource_fixture()
            .endpoint_slice(namespace, name)
            .await
    }

    pub async fn observe_endpoint_slices(
        &self,
        namespace: &str,
        label_selector: Option<&str>,
    ) -> anyhow::Result<Vec<klights_cluster_core::Resource>> {
        self.endpoint_resource_fixture()
            .endpoint_slices(namespace, label_selector)
            .await
    }

    pub async fn replace_endpoints(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
        expected_rv: i64,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        self.endpoint_resource_fixture()
            .replace_endpoints(namespace, name, value, expected_rv)
            .await
    }

    pub async fn remove_endpoints(&self, namespace: &str, name: &str) -> anyhow::Result<()> {
        self.endpoint_resource_fixture()
            .remove_endpoints(namespace, name)
            .await
    }

    pub async fn remove_endpoint_slice(&self, namespace: &str, name: &str) -> anyhow::Result<()> {
        self.endpoint_resource_fixture()
            .remove_endpoint_slice(namespace, name)
            .await
    }

    pub async fn endpoint_fixture_resource_version(&self) -> anyhow::Result<i64> {
        self.endpoint_resource_fixture()
            .current_resource_version()
            .await
    }

    pub fn endpoint_fixture_value_with_resource_version(
        value: impl Into<Arc<serde_json::Value>>,
        resource_version: i64,
    ) -> serde_json::Value {
        klights_cluster_datastore::test_support::EndpointResourceFixture::value_with_resource_version(value, resource_version)
    }

    pub async fn reconcile_endpoints(
        &self,
        service_name: &str,
        namespace: &str,
        selector: Option<&serde_json::Value>,
        ports: Option<&serde_json::Value>,
        publish_not_ready: bool,
    ) -> anyhow::Result<()> {
        self.endpoint_reconcile_fixture()
            .reconcile_endpoints(service_name, namespace, selector, ports, publish_not_ready)
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
        self.endpoint_reconcile_fixture()
            .reconcile_service_endpoint_batch(
                service_name,
                service_uid,
                namespace,
                selector,
                ports,
                publish_not_ready,
            )
            .await
    }

    pub async fn mirror_endpoint_fixture_at(
        &self,
        endpoints: &serde_json::Value,
        mirrored_at: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<()> {
        self.endpoint_reconcile_fixture()
            .mirror_endpoints_at(endpoints, mirrored_at)
            .await
    }

    pub async fn cascade_delete_endpoint_service(
        &self,
        owner_uid: &str,
        owner_name: &str,
        owner_namespace: &str,
    ) -> anyhow::Result<()> {
        self.endpoint_reconcile_fixture()
            .cascade_delete_service(owner_uid, owner_name, owner_namespace)
            .await
    }

    pub async fn register_crd_value(&self, crd: &serde_json::Value) -> anyhow::Result<()> {
        k8s_native_service::test_support::resource::register_crd(&self.harness.crd_registry(), crd)
            .await
            .map_err(anyhow::Error::msg)
    }

    pub async fn register_crd_info(&self, info: klights_controllers::crd::CrdResourceInfo) {
        self.harness.crd_registry().register(info).await;
    }

    pub async fn sync_crd_registry_from_datastore(&self) -> anyhow::Result<()> {
        let crds = self
            .resource_store()
            .list_resources(
                "apiextensions.k8s.io/v1",
                "CustomResourceDefinition",
                None,
                klights_cluster_store::ResourceListOptions::all(),
            )
            .await?;
        for crd in crds.items {
            k8s_native_service::test_support::resource::register_crd(
                &self.harness.crd_registry(),
                crd.data.as_ref(),
            )
            .await
            .map_err(anyhow::Error::msg)?;
        }
        Ok(())
    }

    pub async fn crd_selectable_fields(
        &self,
        group: &str,
        version: &str,
        plural: &str,
    ) -> Option<Vec<String>> {
        self.harness
            .crd_registry()
            .get(group, version, plural)
            .await
            .map(|info| info.selectable_fields)
    }

    pub fn set_node_metrics(&self, metrics: Arc<dyn klights_node_api::NodeMetrics>) {
        self.harness.node_metrics_fixture().replace(metrics);
    }

    pub async fn ensure_operational_cluster_metadata(&self) -> anyhow::Result<()> {
        self.harness.ensure_operational_cluster_metadata().await
    }

    pub async fn seed_default_rbac(&self) -> anyhow::Result<()> {
        self.harness.seed_default_rbac().await
    }

    pub async fn register_operational_follower(
        &self,
        dataplane: klights_leader_api::NetworkDataplane,
    ) -> anyhow::Result<()> {
        self.harness.register_operational_follower(dataplane).await
    }

    pub async fn register_integration_follower(
        &self,
        dataplane: klights_leader_api::NetworkDataplane,
    ) -> anyhow::Result<crate::bootstrap::native_api_composition::support::IntegrationFollowerSession>
    {
        self.harness.register_integration_follower(dataplane).await
    }

    pub fn integration_remote_exec_sync(&self) -> anyhow::Result<RemoteExecSyncWebSocketFixture> {
        self.harness.integration_remote_exec_sync()
    }

    pub fn subscribe_watch(
        &self,
        api_version: &str,
        kind: &str,
    ) -> tokio::sync::broadcast::Receiver<
        crate::bootstrap::native_api_composition::support::IntegrationWatchEvent,
    > {
        self.commit_watch_fixture()
            .subscribe(klights_watch::WatchTopic::new(api_version, kind))
    }

    pub fn node_name(&self) -> &str {
        self.harness.node_name()
    }
}

pub struct TestResourceMutation {
    pub store: klights_cluster_datastore::test_support::ResourceTestStore,
}

#[derive(Default)]
pub struct DeterministicControllerIdentity {
    next: std::sync::atomic::AtomicU64,
}

impl klights_controllers::ControllerIdentityGenerator for DeterministicControllerIdentity {
    fn generate_name(&self, prefix: &str) -> String {
        let value = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("{prefix}{value:05}")
    }

    fn new_uid(&self) -> String {
        let value = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("00000000-0000-4000-8000-{value:012}")
    }
}

pub fn deterministic_controller_identity()
-> Arc<dyn klights_controllers::ControllerIdentityGenerator> {
    Arc::new(DeterministicControllerIdentity::default())
}

pub fn file_process_executor() -> klights_supervisor::FileProcessExecutor {
    klights_supervisor::FileProcessExecutor::new(Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    )))
}

pub async fn get_pod(
    db: &klights_cluster_datastore::test_support::ResourceTestStore,
    namespace: &str,
    name: &str,
) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
    db.get_resource("v1", "Pod", Some(namespace), name).await
}

pub async fn validate_sa_token_bindings(
    state: &TestAppState,
    claims: &klights_auth::SaTokenClaims,
) -> Result<(), k8s_native_service::AppError> {
    klights_auth::authentication::validate_sa_token_bindings(
        &ResourceBoundTokenSubjects {
            store: state.resource_store(),
        },
        claims,
    )
    .await
    .map_err(k8s_native_service::AppError::from)
}

pub async fn resolve_admission_webhook_target(
    db: klights_cluster_datastore::test_support::ResourceTestStore,
    client_config: &serde_json::Value,
) -> Result<
    k8s_native_service::admission::WebhookTarget,
    k8s_native_service::admission::AdmissionDependencyError,
> {
    use k8s_native_service::admission::WebhookTargetResolver as _;
    k8s_native_service::admission::ServiceWebhookTargetResolver::new(Arc::new(
        ResourceAdmissionQuery { store: db },
    ))
    .resolve(client_config)
    .await
}

pub async fn admission_namespace_labels(
    db: klights_cluster_datastore::test_support::ResourceTestStore,
    namespace: &str,
) -> std::collections::BTreeMap<String, String> {
    k8s_native_service::admission::selectors::get_namespace_labels(
        &ResourceAdmissionQuery { store: db },
        namespace,
    )
    .await
}

pub async fn run_admission(
    db: klights_cluster_datastore::test_support::ResourceTestStore,
    context: &k8s_native_service::admission::AdmissionRequestContext,
    is_mutating: bool,
) -> anyhow::Result<serde_json::Value> {
    let identity = k8s_native_service::test_support::admission::DeterministicApiIdentity::default();
    let query = ResourceAdmissionQuery { store: db };
    let target_resolver =
        k8s_native_service::admission::ServiceWebhookTargetResolver::new(Arc::new(query.clone()));
    let webhook_client = k8s_native_service::admission::ReqwestAdmissionWebhookClient::new();
    k8s_native_service::admission::AdmissionEngine::new(
        &identity,
        &query,
        target_resolver.as_ref(),
        webhook_client.as_ref(),
    )
    .run_with_context(context, is_mutating)
    .await
}

pub async fn create_worker_bootstrap_token(
    db: &klights_cluster_datastore::test_support::ResourceTestStore,
) -> anyhow::Result<String> {
    let token = klights_auth::bootstrap_token::generate_bootstrap_token([1, 2, 3], [4; 8]);
    create_scoped_bootstrap_token(
        db,
        klights_auth::bootstrap_token::BootstrapTokenScope::Worker,
        &token,
        std::time::Duration::from_secs(30 * 60),
    )
    .await?;
    Ok(token)
}

pub async fn create_worker_bootstrap_token_with_ttl(
    db: &klights_cluster_datastore::test_support::ResourceTestStore,
    token: &str,
    ttl: std::time::Duration,
) -> anyhow::Result<()> {
    create_scoped_bootstrap_token(
        db,
        klights_auth::bootstrap_token::BootstrapTokenScope::Worker,
        token,
        ttl,
    )
    .await
}

pub async fn create_controlplane_bootstrap_token_with_ttl(
    db: &klights_cluster_datastore::test_support::ResourceTestStore,
    token: &str,
    ttl: std::time::Duration,
) -> anyhow::Result<()> {
    create_scoped_bootstrap_token(
        db,
        klights_auth::bootstrap_token::BootstrapTokenScope::Controlplane,
        token,
        ttl,
    )
    .await
}

async fn create_scoped_bootstrap_token(
    store: &klights_cluster_datastore::test_support::ResourceTestStore,
    scope: klights_auth::bootstrap_token::BootstrapTokenScope,
    token: &str,
    ttl: std::time::Duration,
) -> anyhow::Result<()> {
    let data = klights_auth::bootstrap_token::build_scoped_bootstrap_token_secret_at(
        scope,
        token,
        ttl,
        time::OffsetDateTime::now_utc(),
    )?;
    let namespace = klights_auth::bootstrap_token::BOOTSTRAP_TOKEN_NAMESPACE;
    let name = scope.secret_name();
    match store
        .get_resource("v1", "Secret", Some(namespace), name)
        .await?
    {
        Some(existing) => {
            store
                .update_resource(
                    "v1",
                    "Secret",
                    Some(namespace),
                    name,
                    data,
                    existing.resource_version,
                )
                .await?;
        }
        None => {
            store
                .create_resource("v1", "Secret", Some(namespace), name, data)
                .await?;
        }
    }
    Ok(())
}

pub fn broadcast_watch_event(
    fixture: &klights_watch::test_support::CommitWatchFixture,
    object: serde_json::Value,
) {
    fixture.publish(klights_watch::WatchEvent::added(object));
}

pub async fn reconcile_namespace_termination(
    db: &klights_cluster_datastore::test_support::ResourceTestStore,
    namespace: &str,
    _metrics: &(impl klights_reconcile_api::ReconcileFailureMetrics + ?Sized),
) -> Result<(), k8s_native_service::AppError> {
    let lifecycle = namespace_lifecycle_for_test_datastore(db.clone());
    k8s_native_service::reconcile_namespace_termination_at(
        lifecycle.as_ref(),
        namespace,
        _metrics,
        chrono::Utc::now(),
    )
    .await
}

pub async fn reconcile_namespace_termination_for_uid_with_outcome(
    db: &klights_cluster_datastore::test_support::ResourceTestStore,
    namespace: &str,
    expected_uid: &str,
    _metrics: &(impl klights_reconcile_api::ReconcileFailureMetrics + ?Sized),
) -> Result<k8s_native_service::NamespaceTerminationOutcome, k8s_native_service::AppError> {
    let lifecycle = namespace_lifecycle_for_test_datastore(db.clone());
    k8s_native_service::reconcile_namespace_termination_for_uid_with_outcome_at(
        lifecycle.as_ref(),
        namespace,
        expected_uid,
        _metrics,
        chrono::Utc::now(),
    )
    .await
}

pub struct GeneratedDeleteCompletionRequest<'a> {
    pub target: k8s_native_service::generic_command::ResourceDeleteTarget<'a>,
    pub initial_resource: klights_cluster_core::Resource,
    pub delete_preconditions: klights_cluster_core::ResourcePreconditions,
    pub orphan_children_before_completion: bool,
    pub uid_mismatch_is_conflict: bool,
}

pub use k8s_native_service::generic_command::DeleteCompletion;

pub async fn mark_foreground_deletion_with_retry(
    db: &klights_cluster_datastore::test_support::ResourceTestStore,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    initial_resource: klights_cluster_core::Resource,
    delete_preconditions: klights_cluster_core::ResourcePreconditions,
) -> Result<klights_cluster_core::Resource, k8s_native_service::AppError> {
    let lifecycle = IntegrationFinalizerLifecycleStore(db.clone());
    k8s_native_service::generic_command::mark_foreground_deletion_with_retry(
        &lifecycle,
        api_version,
        kind,
        namespace,
        name,
        initial_resource,
        delete_preconditions,
        chrono::Utc::now(),
    )
    .await
}

pub async fn complete_non_foreground_delete_with_live_recheck(
    db: &klights_cluster_datastore::test_support::ResourceTestStore,
    request: GeneratedDeleteCompletionRequest<'_>,
) -> Result<DeleteCompletion, k8s_native_service::AppError> {
    let lifecycle = IntegrationFinalizerLifecycleStore(db.clone());
    k8s_native_service::generic_command::complete_non_foreground_delete_with_live_recheck(
        &lifecycle,
        k8s_native_service::generic_command::NonForegroundDeleteRequest {
            target: request.target,
            initial_resource: request.initial_resource,
            delete_preconditions: request.delete_preconditions,
            orphan_children_before_completion: request.orphan_children_before_completion,
            uid_mismatch_is_conflict: request.uid_mismatch_is_conflict,
            grace_seconds: 0,
            operation_now: chrono::Utc::now(),
        },
    )
    .await
}

pub async fn delete_collection_listed_resource(
    state: &TestAppState,
    api_version: &'static str,
    kind: &'static str,
    namespace: Option<&str>,
    resource: klights_cluster_core::Resource,
) -> Result<bool, k8s_native_service::AppError> {
    let store = state.resource_store();
    let resource_query = IntegrationResourceQuery { db: store.clone() };
    let lifecycle = IntegrationFinalizerLifecycleStore(store);
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

pub async fn build_test_app_state() -> TestAppState {
    TestAppState {
        harness: NativeApiTestHarness::new()
            .await
            .expect("assemble native API integration harness"),
    }
}

pub async fn build_test_app_state_with_authorizer(
    authorizer: Arc<dyn klights_auth::authorizer::Authorizer>,
) -> TestAppState {
    TestAppState {
        harness: NativeApiTestHarness::with_authorizer(authorizer)
            .await
            .expect("assemble authorized native API integration harness"),
    }
}

pub async fn build_test_app_state_with_operational_endpoints() -> TestAppState {
    TestAppState {
        harness: NativeApiTestHarness::with_authorizer_and_operational_endpoints(Arc::new(
            AllowAllAuthorizer,
        ))
        .await
        .expect("assemble native API integration harness with operational endpoints"),
    }
}

pub async fn build_test_app_state_with_authorizer_and_audit_sink(
    authorizer: Arc<dyn klights_auth::authorizer::Authorizer>,
    audit_sink: Arc<dyn k8s_native_service::audit::AuditSink>,
) -> TestAppState {
    TestAppState {
        harness: NativeApiTestHarness::with_authorizer_and_audit_sink(authorizer, audit_sink)
            .await
            .expect("assemble authorized native API integration harness with audit sink"),
    }
}

pub async fn build_test_app_state_with_pod_lifecycle_diagnostics(
    diagnostics: Arc<dyn klights_pod_api::PodLifecycleDiagnosticsQuery>,
) -> TestAppState {
    TestAppState {
        harness: NativeApiTestHarness::with_pod_lifecycle_diagnostics(diagnostics)
            .await
            .expect("assemble native API integration harness with pod lifecycle diagnostics"),
    }
}

pub async fn build_test_app_state_with_signing_key_pem(signing_key_pem: String) -> TestAppState {
    TestAppState {
        harness: NativeApiTestHarness::with_signing_key_pem(signing_key_pem)
            .await
            .expect("assemble native API integration harness with signing key"),
    }
}

pub async fn build_test_app_state_with_auth_clock(
    clock: Arc<dyn klights_auth::clock::Clock>,
) -> TestAppState {
    TestAppState {
        harness: NativeApiTestHarness::with_auth_clock(clock)
            .await
            .expect("assemble native API integration harness with auth clock"),
    }
}

pub async fn build_test_app_state_with_leader_authority() -> TestAppState {
    TestAppState {
        harness: NativeApiTestHarness::with_leader_authority()
            .await
            .expect("assemble native API integration harness with leader authority"),
    }
}

pub async fn build_test_app_state_with_authentication_dependencies(
    signing_keys: Arc<dyn klights_leader_api::LeaderServiceAccountSigningKeyState>,
    oidc: Option<Arc<dyn klights_auth::oidc::OidcValidator>>,
    webhook: Option<Arc<dyn klights_auth::webhook_auth::WebhookAuthenticator>>,
) -> TestAppState {
    TestAppState {
        harness: NativeApiTestHarness::with_authentication_dependencies(
            signing_keys,
            oidc,
            webhook,
        )
        .await
        .expect("assemble native API integration harness with authentication dependencies"),
    }
}

pub async fn build_test_app_state_with_authenticators(
    oidc: Option<Arc<dyn klights_auth::oidc::OidcValidator>>,
    webhook: Option<Arc<dyn klights_auth::webhook_auth::WebhookAuthenticator>>,
) -> TestAppState {
    TestAppState {
        harness: NativeApiTestHarness::with_authenticators(oidc, webhook)
            .await
            .expect("assemble native API integration harness with authenticators"),
    }
}

pub async fn build_test_app_state_with_bootstrap_token_authenticator(
    bootstrap_token_authenticator: Arc<dyn klights_leader_api::LeaderBootstrapTokenAuthentication>,
) -> TestAppState {
    TestAppState {
        harness: NativeApiTestHarness::with_bootstrap_token_authenticator(
            bootstrap_token_authenticator,
        )
        .await
        .expect("assemble native API integration harness with bootstrap authenticator"),
    }
}

pub async fn build_test_app_state_with_toggle_failing_watch_history() -> (
    TestAppState,
    crate::bootstrap::native_api_composition::support::IntegrationWatchHistoryFailureControl,
) {
    let (harness, control) = NativeApiTestHarness::with_toggle_failing_watch_history()
        .await
        .expect("assemble native API integration harness with failing watch history");
    (TestAppState { harness }, control)
}

pub async fn build_test_app_state_with_mutation_side_effect_factory<F>(factory: F) -> TestAppState
where
    F: FnOnce(
            klights_cluster_datastore::test_support::ResourceTestStore,
        ) -> Arc<klights_controllers::side_effects::SideEffectRegistry>
        + Send
        + 'static,
{
    TestAppState {
        harness: NativeApiTestHarness::with_mutation_side_effect_factory(factory)
            .await
            .expect("assemble native API integration harness with mutation side effects"),
    }
}

pub async fn build_test_app_state_with_service_routing_observation() -> (
    TestAppState,
    klights_networking::test_support::ServiceRoutingObservation,
) {
    let (harness, observation) = NativeApiTestHarness::with_service_routing_observation()
        .await
        .expect("assemble native API integration harness with service routing observation");
    (TestAppState { harness }, observation)
}

pub async fn build_test_app_state_with_priority_fairness() -> (
    TestAppState,
    Arc<k8s_native_service::priority_fairness::ApiPriorityFairness>,
) {
    let (harness, priority_fairness) = NativeApiTestHarness::with_priority_fairness()
        .await
        .expect("assemble native API integration harness with priority fairness");
    (TestAppState { harness }, priority_fairness)
}

pub async fn build_test_app_state_with_csr_signer_observation() -> (
    TestAppState,
    crate::bootstrap::native_api_composition::support::IntegrationCsrSignerObservation,
) {
    let (harness, observation) = NativeApiTestHarness::with_csr_signer_observation()
        .await
        .expect("assemble native API integration harness with CSR signer observation");
    (TestAppState { harness }, observation)
}

pub async fn build_test_app_state_with_held_pod_delete_workqueue() -> (
    TestAppState,
    crate::bootstrap::native_api_composition::support::IntegrationHeldSupervisorTask,
) {
    let (harness, held) = NativeApiTestHarness::with_held_pod_delete_workqueue()
        .await
        .expect("assemble native API integration harness with held Pod delete workqueue");
    (TestAppState { harness }, held)
}

pub async fn build_test_router() -> axum::Router {
    NativeApiTestHarness::new()
        .await
        .expect("assemble native API integration harness")
        .router()
}

pub async fn build_test_router_with_authorizer_and_operational_endpoints(
    authorizer: Arc<dyn klights_auth::authorizer::Authorizer>,
) -> axum::Router {
    NativeApiTestHarness::with_authorizer_and_operational_endpoints(authorizer)
        .await
        .expect("assemble authorized native API integration harness with operational endpoints")
        .router()
}

pub async fn build_test_router_with_db() -> (
    axum::Router,
    klights_cluster_datastore::test_support::ResourceTestStore,
) {
    let harness = NativeApiTestHarness::new()
        .await
        .expect("assemble native API integration harness");
    (harness.router(), harness.resource_store())
}

pub async fn build_test_router_with_db_and_list_cursor_clock(
    clock: Arc<dyn klights_supervisor::WallClock>,
) -> (
    axum::Router,
    klights_cluster_datastore::test_support::ResourceTestStore,
) {
    let harness = NativeApiTestHarness::with_list_cursor_clock(clock)
        .await
        .expect("assemble native API integration harness with fixed LIST cursor clock");
    (harness.router(), harness.resource_store())
}

pub async fn in_memory() -> klights_cluster_datastore::test_support::ResourceTestStore {
    NativeApiTestHarness::new()
        .await
        .expect("assemble native API integration datastore")
        .resource_store()
}

struct IntegrationResourceQuery {
    db: klights_cluster_datastore::test_support::ResourceTestStore,
}

#[derive(Clone)]
struct IntegrationNamespaceLifecycleStore(
    klights_cluster_datastore::test_support::ResourceTestStore,
);

fn namespace_lifecycle_error(
    error: anyhow::Error,
) -> klights_reconcile_api::NamespaceLifecycleError {
    if klights_cluster_datastore::errors::is_conflict_error(&error) {
        klights_reconcile_api::NamespaceLifecycleError::Conflict {
            message: error.to_string(),
        }
    } else {
        klights_reconcile_api::NamespaceLifecycleError::Internal {
            message: error.to_string(),
        }
    }
}

impl klights_reconcile_api::NamespaceLifecycleStore for IntegrationNamespaceLifecycleStore {
    fn get_terminating_namespace(
        &self,
        namespace: String,
    ) -> klights_reconcile_api::NamespaceLifecycleFuture<'_, Option<klights_cluster_core::Resource>>
    {
        Box::pin(async move {
            self.0
                .get_namespace(&namespace)
                .await
                .map_err(namespace_lifecycle_error)
        })
    }

    fn list_namespace_pods(
        &self,
        namespace: String,
    ) -> klights_reconcile_api::NamespaceLifecycleFuture<'_, Vec<klights_cluster_core::Resource>>
    {
        Box::pin(async move {
            self.0
                .list_namespace_pods(&namespace)
                .await
                .map_err(namespace_lifecycle_error)
        })
    }

    fn mark_namespace_pod_terminating(
        &self,
        pod: klights_cluster_core::Resource,
        namespace: String,
        body: serde_json::Value,
    ) -> klights_reconcile_api::NamespaceLifecycleFuture<'_, ()> {
        Box::pin(async move {
            self.0
                .update_main_strict(
                    &pod.api_version,
                    &pod.kind,
                    Some(&namespace),
                    &pod.name,
                    body,
                    klights_cluster_core::ResourcePreconditions::from_resource(&pod),
                )
                .await
                .map(|_| ())
                .map_err(namespace_lifecycle_error)
        })
    }

    fn update_terminating_namespace(
        &self,
        namespace: String,
        body: serde_json::Value,
        expected_resource_version: i64,
    ) -> klights_reconcile_api::NamespaceLifecycleFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async move {
            self.0
                .update_namespace(&namespace, body, expected_resource_version)
                .await
                .map_err(namespace_lifecycle_error)
        })
    }

    fn list_namespace_non_pod_resources(
        &self,
        namespace: String,
    ) -> klights_reconcile_api::NamespaceLifecycleFuture<'_, Vec<klights_cluster_core::Resource>>
    {
        Box::pin(async move {
            self.0
                .list_namespace_non_pod_resources(&namespace)
                .await
                .map_err(namespace_lifecycle_error)
        })
    }

    fn delete_namespace_non_pod_resource(
        &self,
        resource: klights_cluster_core::Resource,
        namespace: String,
    ) -> klights_reconcile_api::NamespaceLifecycleFuture<'_, ()> {
        Box::pin(async move {
            self.0
                .delete_non_pod_strict(
                    &resource.api_version,
                    &resource.kind,
                    Some(&namespace),
                    &resource.name,
                    klights_cluster_core::ResourcePreconditions::from_resource(&resource),
                )
                .await
                .map_err(namespace_lifecycle_error)
        })
    }

    fn count_namespace_resources(
        &self,
        namespace: String,
    ) -> klights_reconcile_api::NamespaceLifecycleFuture<'_, i64> {
        Box::pin(async move {
            self.0
                .count_namespace_resources(&namespace)
                .await
                .map_err(namespace_lifecycle_error)
        })
    }

    fn delete_terminating_namespace(
        &self,
        namespace: String,
    ) -> klights_reconcile_api::NamespaceLifecycleFuture<'_, ()> {
        Box::pin(async move {
            self.0
                .delete_namespace(&namespace)
                .await
                .map_err(namespace_lifecycle_error)
        })
    }
}

pub(crate) fn namespace_lifecycle_for_test_datastore(
    db: klights_cluster_datastore::test_support::ResourceTestStore,
) -> Arc<dyn klights_reconcile_api::NamespaceLifecycleStore> {
    Arc::new(IntegrationNamespaceLifecycleStore(db))
}

#[derive(Clone)]
struct IntegrationFinalizerLifecycleStore(
    klights_cluster_datastore::test_support::ResourceTestStore,
);

fn finalizer_lifecycle_error(
    error: anyhow::Error,
) -> klights_reconcile_api::FinalizerLifecycleError {
    if klights_cluster_datastore::errors::is_conflict_error(&error) {
        klights_reconcile_api::FinalizerLifecycleError::Conflict(error.to_string())
    } else {
        klights_reconcile_api::FinalizerLifecycleError::Internal(error.to_string())
    }
}

impl klights_reconcile_api::FinalizerLifecyclePort for IntegrationFinalizerLifecycleStore {
    fn get_resource(
        &self,
        target: klights_reconcile_api::FinalizerResourceTarget,
    ) -> klights_reconcile_api::FinalizerLifecycleFuture<'_, Option<klights_cluster_core::Resource>>
    {
        Box::pin(async move {
            self.0
                .get_resource(
                    target.api_version(),
                    target.kind(),
                    target.namespace(),
                    target.name(),
                )
                .await
                .map_err(finalizer_lifecycle_error)
        })
    }

    fn update_resource(
        &self,
        request: klights_reconcile_api::FinalizerUpdateRequest,
    ) -> klights_reconcile_api::FinalizerLifecycleFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async move {
            self.0
                .update_main_strict(
                    request.target.api_version(),
                    request.target.kind(),
                    request.target.namespace(),
                    request.target.name(),
                    request.data,
                    request.preconditions,
                )
                .await
                .map_err(finalizer_lifecycle_error)
        })
    }

    fn delete_with_tombstone(
        &self,
        request: klights_reconcile_api::FinalizerTombstoneDeleteRequest,
    ) -> klights_reconcile_api::FinalizerLifecycleFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async move {
            let target = request.target;
            let resource = self
                .0
                .get_resource(
                    target.api_version(),
                    target.kind(),
                    target.namespace(),
                    target.name(),
                )
                .await
                .map_err(finalizer_lifecycle_error)?
                .ok_or_else(|| {
                    klights_reconcile_api::FinalizerLifecycleError::NotFound(format!(
                        "{}/{} not found",
                        target.kind(),
                        target.name()
                    ))
                })?;
            self.0
                .delete_non_pod_strict(
                    target.api_version(),
                    target.kind(),
                    target.namespace(),
                    target.name(),
                    request.preconditions,
                )
                .await
                .map_err(finalizer_lifecycle_error)?;
            Ok(resource)
        })
    }

    fn orphan_children(
        &self,
        request: klights_reconcile_api::FinalizerOrphanRequest,
    ) -> klights_reconcile_api::FinalizerLifecycleFuture<'_, ()> {
        Box::pin(async move {
            for child in self
                .0
                .owned_resources(&request.owner_uid, request.target.namespace())
                .await
                .map_err(finalizer_lifecycle_error)?
            {
                let mut data = (*child.data).clone();
                if let Some(references) = data
                    .pointer_mut("/metadata/ownerReferences")
                    .and_then(serde_json::Value::as_array_mut)
                {
                    references.retain(|reference| {
                        reference.get("uid").and_then(serde_json::Value::as_str)
                            != Some(request.owner_uid.as_str())
                    });
                }
                self.0
                    .update_main_strict(
                        &child.api_version,
                        &child.kind,
                        child.namespace.as_deref(),
                        &child.name,
                        data,
                        klights_cluster_core::ResourcePreconditions::from_resource(&child),
                    )
                    .await
                    .map_err(finalizer_lifecycle_error)?;
            }
            Ok(())
        })
    }

    fn run_finalized_effects(
        &self,
        _request: klights_reconcile_api::FinalizerEffectsRequest,
    ) -> klights_reconcile_api::FinalizerLifecycleFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

impl klights_leader_api::LeaderResourceQuery for IntegrationResourceQuery {
    fn get_resource(
        &self,
        request: klights_leader_api::ResourceGetRequest,
    ) -> klights_leader_api::ResourceQueryFuture<'_, Option<klights_cluster_core::Resource>> {
        Box::pin(async move {
            let key = request.into_key();
            self.db
                .get_resource(
                    &key.api_version,
                    &key.kind,
                    key.namespace.as_deref(),
                    &key.name,
                )
                .await
                .map_err(|error| {
                    klights_leader_api::ResourceQueryError::query_failed(error.to_string())
                })
        })
    }

    fn list_resources(
        &self,
        request: klights_leader_api::ResourceListRequest,
    ) -> klights_leader_api::ResourceQueryFuture<'_, klights_leader_api::ResourceListResult> {
        Box::pin(async move {
            let list = self
                .db
                .list_resources(
                    request.api_version(),
                    request.kind(),
                    request.namespace(),
                    klights_cluster_store::ResourceListOptions::new(
                        request.label_selector(),
                        request.field_selector(),
                        request.limit(),
                        request.continue_token(),
                    ),
                )
                .await
                .map_err(|error| {
                    klights_leader_api::ResourceQueryError::query_failed(error.to_string())
                })?;
            klights_leader_api::ResourceListResult::try_new(
                list.items,
                list.resource_version,
                list.watch_replay_position,
                list.continue_token,
                list.remaining_item_count,
            )
        })
    }
}

pub fn resource_query_for_test_datastore(
    db: klights_cluster_datastore::test_support::ResourceTestStore,
) -> Arc<dyn klights_leader_api::LeaderResourceQuery> {
    crate::bootstrap::composition_adapters::resource_query_adapter::DatastoreResourceQueryAdapter::new_focused_for_test(
        db.focused_resource_reads_for_test_support(),
        crate::bootstrap::composition_adapters::authority_adapter::always_leader_authority(),
    )
}
