//! Private leader-local capability adapters.
//!
//! These implementations are deliberately composed at the bootstrap boundary.
//! Node leases, node lifecycle status, projected-token identity/signing policy,
//! cache readiness, and committed-outbox effects each have focused owners.

use std::sync::Arc;

use klights_leader_api::{
    CacheReadinessFuture, CacheReadinessRequest, LeaderAuthenticatedProjectedServiceAccountToken,
    LeaderCacheReadiness, LeaderNodeLeaseRenewal, LeaderNodeLifecycleStatus,
    LeaderProjectedServiceAccountToken, LeaderResourceCommand, LeaderResourceQuery,
    NodeLeaseRenewalError, NodeLeaseRenewalFuture, NodeLeaseRenewalRequest, NodeLeaseRenewalResult,
    NodeLifecycleStatusError, NodeLifecycleStatusFuture, NodeLifecycleStatusRequest,
    NodeLifecycleStatusResult, ProjectedServiceAccountTokenError,
    ProjectedServiceAccountTokenFuture, ProjectedServiceAccountTokenRequest, ResourceCommandError,
    ResourceCommandRequest, ResourceCommandResult,
};

use crate::bootstrap::authority::AuthorityHandle;
use crate::datastore::DatastoreHandle;

/// Root-local cache readiness is a concrete bootstrap capability.  The
/// worker/cache implementation owns real readiness; a root-local leader has
/// no separate relist gate once its focused store is composed.
pub(crate) struct LocalCacheReadinessAdapter;

impl LeaderCacheReadiness for LocalCacheReadinessAdapter {
    fn wait_cache_ready(&self, _scope: CacheReadinessRequest) -> CacheReadinessFuture<'_> {
        Box::pin(async { Ok(()) })
    }
}

pub(crate) fn new_local_outbox_side_effect_state(
    db: DatastoreHandle,
) -> Arc<crate::bootstrap::composition_adapters::committed_outbox_delivery_adapter::RootOutboxSideEffectState>
{
    Arc::new(
        crate::bootstrap::composition_adapters::committed_outbox_delivery_adapter::
            RootOutboxSideEffectState::new(db),
    )
}

/// Bootstrap-owned in-memory Node lease publisher.
pub(crate) struct LocalNodeLeaseRenewal {
    tracker: Arc<klights_controllers::node_lease::NodeLeaseTracker>,
    authority: AuthorityHandle,
}

impl LocalNodeLeaseRenewal {
    pub(crate) fn new<A: Into<AuthorityHandle>>(
        tracker: Arc<klights_controllers::node_lease::NodeLeaseTracker>,
        authority: A,
    ) -> Self {
        Self {
            tracker,
            authority: authority.into(),
        }
    }
}

impl LeaderNodeLeaseRenewal for LocalNodeLeaseRenewal {
    fn renew_node_lease(
        &self,
        request: NodeLeaseRenewalRequest,
    ) -> NodeLeaseRenewalFuture<'_, NodeLeaseRenewalResult> {
        Box::pin(async move {
            if self.authority.local_permit().is_err() {
                return Err(NodeLeaseRenewalError::NotLeader);
            }
            let (node_name, renew_time, lease_duration_seconds) = request.into_parts();
            self.tracker
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

pub(crate) type LocalNodeLeaseRenewalAdapter = LocalNodeLeaseRenewal;

/// Bootstrap-owned Node status CAS publisher.
pub(crate) struct LocalNodeLifecycleStatus {
    resource_query: Arc<dyn LeaderResourceQuery>,
    resource_commands: Arc<dyn LeaderResourceCommand>,
    authority: AuthorityHandle,
}

impl LocalNodeLifecycleStatus {
    pub(crate) fn new<A: Into<AuthorityHandle>>(
        resource_query: Arc<dyn LeaderResourceQuery>,
        resource_commands: Arc<dyn LeaderResourceCommand>,
        authority: A,
    ) -> Self {
        Self {
            resource_query,
            resource_commands,
            authority: authority.into(),
        }
    }
}

impl LeaderNodeLifecycleStatus for LocalNodeLifecycleStatus {
    fn submit_node_lifecycle_status(
        &self,
        request: NodeLifecycleStatusRequest,
    ) -> NodeLifecycleStatusFuture<'_, NodeLifecycleStatusResult> {
        Box::pin(async move {
            if self.authority.local_permit().is_err() {
                return Err(NodeLifecycleStatusError::NotLeader);
            }
            let get = klights_leader_api::node_get_request(
                request.node_name(),
                klights_leader_api::ResourceQueryConsistency::LeaderFresh,
            )
            .map_err(|error| NodeLifecycleStatusError::apply_failed(error.to_string()))?;
            let current = self
                .resource_query
                .get_resource(get)
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
            let command = ResourceCommandRequest::try_new(request.into_command())
                .map_err(node_lifecycle_status_command_error)?;
            let result = self
                .resource_commands
                .submit_resource_command(command)
                .await
                .map_err(node_lifecycle_status_command_error)?;
            let resource_version = match result {
                ResourceCommandResult::Resource(resource) => resource.resource_version,
                ResourceCommandResult::Ack { resource_version } => resource_version,
            };
            Ok(NodeLifecycleStatusResult::Updated { resource_version })
        })
    }
}

pub(crate) type LocalNodeLifecycleStatusAdapter = LocalNodeLifecycleStatus;

fn node_lifecycle_status_command_error(error: ResourceCommandError) -> NodeLifecycleStatusError {
    let message = error.to_string();
    match error {
        ResourceCommandError::NotLeader => NodeLifecycleStatusError::NotLeader,
        ResourceCommandError::NotFound { .. } => NodeLifecycleStatusError::NotFound,
        ResourceCommandError::Conflict { .. } => NodeLifecycleStatusError::conflict(message),
        ResourceCommandError::Retryable { .. } => NodeLifecycleStatusError::retryable(message),
        ResourceCommandError::Timeout => NodeLifecycleStatusError::Timeout,
        ResourceCommandError::Cancelled => NodeLifecycleStatusError::Cancelled,
        _ => NodeLifecycleStatusError::apply_failed(message),
    }
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
pub(crate) type ProjectedTokenAsyncBoundary = Arc<
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
#[allow(dead_code)]
pub(crate) struct ProjectedTokenIssueTestRegistration {
    namespace: String,
    sign_attempts: Arc<std::sync::atomic::AtomicUsize>,
}

#[cfg(test)]
impl ProjectedTokenIssueTestRegistration {
    #[allow(dead_code)]
    pub(crate) fn sign_attempts(&self) -> usize {
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
#[allow(dead_code)]
pub(crate) fn install_projected_token_issue_test_probe(
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

/// Leadership and signing-fence checks shared by both token entry points.
pub(crate) struct LeadershipGenerationFence {
    authority: AuthorityHandle,
    permit: klights_leader_api::AuthorityPermit,
    signing_fence: Option<klights_replication::authority::AuthoritySigningFence>,
}

impl LeadershipGenerationFence {
    pub(crate) fn sample<A: Into<AuthorityHandle>>(
        authority: A,
    ) -> Result<Self, ProjectedServiceAccountTokenError> {
        let authority = authority.into();
        let permit = authority
            .local_permit()
            .map_err(|_| ProjectedServiceAccountTokenError::NotLeader)?;
        Ok(Self {
            authority,
            permit,
            signing_fence: None,
        })
    }

    pub(crate) fn with_signing_fence(
        mut self,
        signing_fence: Option<klights_replication::authority::AuthoritySigningFence>,
    ) -> Self {
        self.signing_fence = signing_fence;
        self
    }

    pub(crate) fn ensure_unchanged(&self) -> Result<(), ProjectedServiceAccountTokenError> {
        self.authority
            .validate(&self.permit)
            .map_err(|_| ProjectedServiceAccountTokenError::NotLeader)
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn sign_if_unchanged<T>(
        &self,
        sign: impl FnOnce() -> T,
    ) -> Result<T, ProjectedServiceAccountTokenError> {
        #[cfg(any(test, feature = "pod-repository-test-support"))]
        let legacy_watch = self.authority.legacy_watch_for_test();
        #[cfg(any(test, feature = "pod-repository-test-support"))]
        let _legacy_current = legacy_watch.as_ref().map(|receiver| receiver.borrow());
        #[cfg(test)]
        let _authority_read = self
            .signing_fence
            .as_ref()
            .map(klights_replication::authority::AuthoritySigningFence::blocking_read);
        self.ensure_unchanged()?;
        Ok(sign())
    }
}

/// Bootstrap-owned projected ServiceAccount token issuer.
pub(crate) struct LocalProjectedToken {
    db: DatastoreHandle,
    pod_store: Arc<klights_kubelet::pod_repository::store::PodStore>,
    authoring_node: String,
    containerd_namespace: String,
    signing_key_path: std::path::PathBuf,
    file_process: klights_supervisor::FileProcessExecutor,
    crypto: klights_supervisor::CryptoExecutor,
    authority: AuthorityHandle,
    signing_fence: Option<klights_replication::authority::AuthoritySigningFence>,
}

impl LocalProjectedToken {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new<A: Into<AuthorityHandle>>(
        db: DatastoreHandle,
        authoring_node: String,
        containerd_namespace: String,
        signing_key_path: std::path::PathBuf,
        authority: A,
        file_process: klights_supervisor::FileProcessExecutor,
    ) -> Self {
        let pod_store = Arc::new(crate::bootstrap::pod_repository_composition::new_pod_store(
            db.clone(),
        ));
        let crypto = file_process.crypto_executor();
        Self {
            db,
            pod_store,
            authoring_node,
            containerd_namespace,
            signing_key_path,
            file_process,
            crypto,
            authority: authority.into(),
            signing_fence: None,
        }
    }

    pub(crate) fn with_authority_signing_fence(
        mut self,
        signing_fence: klights_replication::authority::AuthoritySigningFence,
    ) -> Self {
        self.signing_fence = Some(signing_fence);
        self
    }

    pub(crate) fn issue_projected_token_after_transport_auth(
        &self,
        request: ProjectedServiceAccountTokenRequest,
    ) -> ProjectedServiceAccountTokenFuture<'_> {
        Box::pin(async move {
            let leadership = LeadershipGenerationFence::sample(self.authority.clone())?
                .with_signing_fence(self.signing_fence.clone());
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            if let Some(probe) = projected_token_issue_test_probe(&self.containerd_namespace) {
                (probe.async_boundary)().await;
            }
            let signing_key_pem = klights_cluster_datastore::signing_key_state::read_with_executor(
                &self.signing_key_path,
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
            let resources =
                crate::bootstrap::composition_adapters::projected_token_resource_adapter::
                    ProjectedTokenResourceAdapter::new(self.db.as_ref(), self.pod_store.as_ref());
            let claims =
                klights_auth::projected_service_account_token::authorize_projected_service_account_token(
                    &resources,
                    &request,
                )
                .await;
            leadership.ensure_unchanged()?;
            let claims = claims?;
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            if let Some(probe) = projected_token_issue_test_probe(&self.containerd_namespace) {
                probe
                    .sign_attempts
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            let crypto: &klights_supervisor::CryptoExecutor = &self.crypto;
            leadership.ensure_unchanged()?;
            let signing_fence = leadership.signing_fence.clone();
            let authority = leadership.authority.clone();
            let permit = leadership.permit.clone();
            let token = crypto
                .run_blocking("sign-projected-service-account-token", move || {
                    let _authority_read = signing_fence
                        .as_ref()
                        .map(klights_replication::authority::AuthoritySigningFence::blocking_read);
                    if authority.validate(&permit).is_err() {
                        return Err(ProjectedServiceAccountTokenError::NotLeader);
                    }
                    klights_auth::projected_service_account_token::
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
}

impl LeaderProjectedServiceAccountToken for LocalProjectedToken {
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
/// check. It intentionally bypasses the self-node check above because the
/// transport has already authenticated and constrained the request.
pub(crate) struct AuthenticatedProjectedTokenService {
    projected: Arc<LocalProjectedToken>,
}

impl AuthenticatedProjectedTokenService {
    pub(crate) fn new(projected: Arc<LocalProjectedToken>) -> Self {
        Self { projected }
    }
}

impl LeaderAuthenticatedProjectedServiceAccountToken for AuthenticatedProjectedTokenService {
    fn issue_authenticated_projected_service_account_token(
        &self,
        request: ProjectedServiceAccountTokenRequest,
    ) -> ProjectedServiceAccountTokenFuture<'_> {
        self.projected
            .issue_projected_token_after_transport_auth(request)
    }
}

pub(crate) type LocalProjectedTokenAdapter = LocalProjectedToken;
pub(crate) type AuthenticatedProjectedTokenIssuer = AuthenticatedProjectedTokenService;
