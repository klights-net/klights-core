use std::sync::Arc;

use klights_cluster_core::Resource;
use serde_json::Value;

use crate::datastore::DatastoreHandle;
use k8s_native_service::ports::{
    ApiFailureEntry, ApiFailureMetrics, ApiNodeLeaseObservations, ApiNodeLeaseObservedFuture,
    ApiPodRepository,
};

fn validate_effect_authority() -> anyhow::Result<()> {
    klights_leader_api::validate_authority_if_scoped()
        .map_err(|error| anyhow::anyhow!("leader authority rejected effect: {error}"))?;
    klights_leader_api::validate_controller_lease_if_scoped()
        .map_err(|error| anyhow::anyhow!("controller authority rejected effect: {error}"))
}

fn validate_pod_effect_authority() -> Result<(), klights_pod_api::PodRepositoryError> {
    klights_leader_api::validate_authority_if_scoped().map_err(|error| {
        klights_pod_api::PodRepositoryError::unavailable(format!(
            "leader authority rejected Pod effect: {error}"
        ))
    })?;
    klights_leader_api::validate_controller_lease_if_scoped().map_err(|error| {
        klights_pod_api::PodRepositoryError::unavailable(format!(
            "controller authority rejected Pod effect: {error}"
        ))
    })
}

pub(crate) struct RootNamespaceTerminationStore {
    inner: DatastoreHandle,
    commands: Arc<dyn klights_leader_api::LeaderResourceCommand>,
}

impl RootNamespaceTerminationStore {
    #[cfg(test)]
    pub(crate) fn new(inner: DatastoreHandle) -> Arc<Self> {
        let authority = super::authority_adapter::always_leader_authority();
        let query = super::resource_query_adapter::DatastoreResourceQueryAdapter::new(
            inner.clone(),
            authority.clone(),
        );
        let commands = Arc::new(
            klights_replication::leader_api::EmbeddedLeaderResourceCommand::new(
                Arc::new(
                    crate::bootstrap::outbox_apply_adapter::BackendProposalFixture::new(
                        inner.clone(),
                    ),
                ),
                query,
                authority,
            ),
        );
        Self::new_with_commands(inner, commands)
    }

    pub(crate) fn new_with_commands(
        inner: DatastoreHandle,
        commands: Arc<dyn klights_leader_api::LeaderResourceCommand>,
    ) -> Arc<Self> {
        Arc::new(Self { inner, commands })
    }

    async fn submit_resource(
        &self,
        command: klights_cluster_core::StorageCommand,
    ) -> Result<Resource, klights_reconcile_api::NamespaceLifecycleError> {
        let request = klights_leader_api::ResourceCommandRequest::try_new(command)
            .map_err(map_namespace_command_error)?;
        match self
            .commands
            .submit_resource_command(request)
            .await
            .map_err(map_namespace_command_error)?
        {
            klights_leader_api::ResourceCommandResult::Resource(resource) => Ok(resource),
            klights_leader_api::ResourceCommandResult::Ack { .. } => {
                Err(klights_reconcile_api::NamespaceLifecycleError::Internal {
                    message: "namespace mutation returned no resource".to_string(),
                })
            }
        }
    }

    async fn submit_ack(
        &self,
        command: klights_cluster_core::StorageCommand,
    ) -> Result<(), klights_reconcile_api::NamespaceLifecycleError> {
        let request = klights_leader_api::ResourceCommandRequest::try_new(command)
            .map_err(map_namespace_command_error)?;
        self.commands
            .submit_resource_command(request)
            .await
            .map(|_| ())
            .map_err(map_namespace_command_error)
    }
}

fn map_namespace_command_error(
    error: klights_leader_api::ResourceCommandError,
) -> klights_reconcile_api::NamespaceLifecycleError {
    let message = error.to_string();
    match error {
        klights_leader_api::ResourceCommandError::NotFound { .. } => {
            klights_reconcile_api::NamespaceLifecycleError::NotFound { message }
        }
        klights_leader_api::ResourceCommandError::AlreadyExists { .. }
        | klights_leader_api::ResourceCommandError::Conflict { .. } => {
            klights_reconcile_api::NamespaceLifecycleError::Conflict { message }
        }
        klights_leader_api::ResourceCommandError::NotLeader
        | klights_leader_api::ResourceCommandError::Retryable { .. }
        | klights_leader_api::ResourceCommandError::Timeout
        | klights_leader_api::ResourceCommandError::Cancelled => {
            klights_reconcile_api::NamespaceLifecycleError::Unavailable { message }
        }
        _ => klights_reconcile_api::NamespaceLifecycleError::Internal { message },
    }
}

fn map_namespace_lifecycle_error(
    error: anyhow::Error,
) -> klights_reconcile_api::NamespaceLifecycleError {
    let message = error.to_string();
    if message.to_ascii_lowercase().contains("not found") {
        klights_reconcile_api::NamespaceLifecycleError::NotFound { message }
    } else if klights_cluster_datastore::errors::is_conflict_error(&error) {
        klights_reconcile_api::NamespaceLifecycleError::Conflict { message }
    } else {
        klights_reconcile_api::NamespaceLifecycleError::Internal { message }
    }
}

impl klights_reconcile_api::NamespaceLifecycleStore for RootNamespaceTerminationStore {
    fn get_terminating_namespace(
        &self,
        namespace: String,
    ) -> klights_reconcile_api::NamespaceLifecycleFuture<'_, Option<Resource>> {
        Box::pin(async move {
            self.inner
                .get_namespace(&namespace)
                .await
                .map_err(map_namespace_lifecycle_error)
        })
    }

    fn list_namespace_pods(
        &self,
        namespace: String,
    ) -> klights_reconcile_api::NamespaceLifecycleFuture<'_, Vec<Resource>> {
        Box::pin(async move {
            self.inner
                .list_namespace_resources_of_kind(&namespace, "Pod")
                .await
                .map_err(map_namespace_lifecycle_error)
        })
    }

    fn mark_namespace_pod_terminating(
        &self,
        pod: Resource,
        namespace: String,
        body: Value,
    ) -> klights_reconcile_api::NamespaceLifecycleFuture<'_, ()> {
        Box::pin(async move {
            validate_effect_authority().map_err(|error| {
                klights_reconcile_api::NamespaceLifecycleError::Unavailable {
                    message: error.to_string(),
                }
            })?;
            self.submit_resource(klights_cluster_core::StorageCommand::UpdateResource {
                api_version: pod.api_version.clone(),
                kind: pod.kind.clone(),
                namespace: Some(namespace),
                name: pod.name.clone(),
                data: body,
                expected_rv: pod.resource_version,
                preconditions: klights_cluster_core::ResourcePreconditions::from_resource(&pod),
                preserve_status: false,
            })
            .await?;
            Ok(())
        })
    }

    fn update_terminating_namespace(
        &self,
        namespace: String,
        body: Value,
        expected_resource_version: i64,
    ) -> klights_reconcile_api::NamespaceLifecycleFuture<'_, Resource> {
        Box::pin(async move {
            validate_effect_authority().map_err(|error| {
                klights_reconcile_api::NamespaceLifecycleError::Unavailable {
                    message: error.to_string(),
                }
            })?;
            self.submit_resource(klights_cluster_core::StorageCommand::UpdateNamespace {
                name: namespace,
                data: body,
                expected_rv: expected_resource_version,
            })
            .await
        })
    }

    fn list_namespace_non_pod_resources(
        &self,
        namespace: String,
    ) -> klights_reconcile_api::NamespaceLifecycleFuture<'_, Vec<Resource>> {
        Box::pin(async move {
            self.inner
                .list_namespace_resources_excluding_kind(&namespace, "Pod")
                .await
                .map_err(map_namespace_lifecycle_error)
        })
    }

    fn delete_namespace_non_pod_resource(
        &self,
        resource: Resource,
        namespace: String,
    ) -> klights_reconcile_api::NamespaceLifecycleFuture<'_, ()> {
        Box::pin(async move {
            validate_effect_authority().map_err(|error| {
                klights_reconcile_api::NamespaceLifecycleError::Unavailable {
                    message: error.to_string(),
                }
            })?;
            self.submit_ack(klights_cluster_core::StorageCommand::DeleteResource {
                api_version: resource.api_version.clone(),
                kind: resource.kind.clone(),
                namespace: Some(namespace),
                name: resource.name.clone(),
                preconditions: klights_cluster_core::ResourcePreconditions::from_resource(
                    &resource,
                ),
            })
            .await
        })
    }

    fn count_namespace_resources(
        &self,
        namespace: String,
    ) -> klights_reconcile_api::NamespaceLifecycleFuture<'_, i64> {
        Box::pin(async move {
            self.inner
                .count_namespace_resources(&namespace)
                .await
                .map_err(map_namespace_lifecycle_error)
        })
    }

    fn delete_terminating_namespace(
        &self,
        namespace: String,
    ) -> klights_reconcile_api::NamespaceLifecycleFuture<'_, ()> {
        Box::pin(async move {
            validate_effect_authority().map_err(|error| {
                klights_reconcile_api::NamespaceLifecycleError::Unavailable {
                    message: error.to_string(),
                }
            })?;
            self.submit_ack(klights_cluster_core::StorageCommand::DeleteNamespace {
                name: namespace,
            })
            .await
        })
    }
}

pub(crate) struct RootNamespaceTerminationReconciler {
    store: Arc<dyn klights_reconcile_api::NamespaceLifecycleStore>,
    metrics: Arc<klights_controllers::side_effects::SideEffectMetrics>,
}

impl RootNamespaceTerminationReconciler {
    pub(crate) fn new(
        store: Arc<dyn klights_reconcile_api::NamespaceLifecycleStore>,
        metrics: Arc<klights_controllers::side_effects::SideEffectMetrics>,
    ) -> Arc<Self> {
        Arc::new(Self { store, metrics })
    }
}

impl klights_reconcile_api::NamespaceTerminationSink for RootNamespaceTerminationReconciler {
    fn reconcile_namespace_termination(
        &self,
        request: klights_reconcile_api::NamespaceTerminationRequest,
    ) -> klights_reconcile_api::NamespaceTerminationFuture<'_> {
        Box::pin(async move {
            let outcome = match request.expected_uid {
                Some(uid) => {
                    k8s_native_service::reconcile_namespace_termination_for_uid_with_outcome_at(
                        self.store.as_ref(),
                        &request.namespace,
                        &uid,
                        self.metrics.as_ref(),
                        klights_supervisor::SystemWallClock::now_utc(),
                    )
                    .await
                }
                None => k8s_native_service::reconcile_namespace_termination_at(
                    self.store.as_ref(),
                    &request.namespace,
                    self.metrics.as_ref(),
                    klights_supervisor::SystemWallClock::now_utc(),
                )
                .await
                .map(|()| k8s_native_service::NamespaceTerminationOutcome::Finalized),
            }
            .map_err(|error| {
                klights_reconcile_api::ReconcileSinkError::unavailable(format!("{error:?}"))
            })?;
            Ok(match outcome {
                k8s_native_service::NamespaceTerminationOutcome::Finalized => {
                    klights_reconcile_api::NamespaceTerminationOutcome::Finalized
                }
                k8s_native_service::NamespaceTerminationOutcome::StillPending => {
                    klights_reconcile_api::NamespaceTerminationOutcome::StillPending
                }
            })
        })
    }
}

pub(crate) struct RootApiPodRepository {
    query: Arc<dyn klights_pod_api::PodQuery>,
    snapshot: Arc<dyn klights_pod_api::PodSnapshotQuery>,
    mutation_reconcile: Arc<dyn klights_reconcile_api::PodMutationReconcileSink>,
    namespace_termination_queue: Arc<dyn klights_reconcile_api::NamespaceTerminationQueueSink>,
    eviction_admission: Arc<dyn klights_reconcile_api::PodEvictionAdmissionSink>,
    namespace_bootstrap: Arc<dyn klights_reconcile_api::NamespaceBootstrapSink>,
    api: Arc<dyn klights_pod_api::PodApiMutation>,
    subresource: Arc<dyn klights_pod_api::PodSubresourceMutation>,
}

impl RootApiPodRepository {
    /// Focused-port constructor: the concrete root repository aggregate no
    /// longer exists; callers hand the individual capability ports from
    /// the focused Pod composition ports.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        query: Arc<dyn klights_pod_api::PodQuery>,
        snapshot: Arc<dyn klights_pod_api::PodSnapshotQuery>,
        mutation_reconcile: Arc<dyn klights_reconcile_api::PodMutationReconcileSink>,
        namespace_termination_queue: Arc<dyn klights_reconcile_api::NamespaceTerminationQueueSink>,
        eviction_admission: Arc<dyn klights_reconcile_api::PodEvictionAdmissionSink>,
        namespace_bootstrap: Arc<dyn klights_reconcile_api::NamespaceBootstrapSink>,
        api: Arc<dyn klights_pod_api::PodApiMutation>,
        subresource: Arc<dyn klights_pod_api::PodSubresourceMutation>,
    ) -> Arc<Self> {
        Arc::new(Self {
            query,
            snapshot,
            mutation_reconcile,
            namespace_termination_queue,
            eviction_admission,
            namespace_bootstrap,
            api,
            subresource,
        })
    }
}

impl klights_pod_api::PodQuery for RootApiPodRepository {
    fn get_pod(
        &self,
        request: klights_pod_api::PodGetRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Option<Resource>> {
        self.query.get_pod(request)
    }

    fn list_pods(
        &self,
        request: klights_pod_api::PodListRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodListResult> {
        self.query.list_pods(request)
    }

    fn list_pods_by_owner_uid(
        &self,
        request: klights_pod_api::PodOwnerListRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Vec<Resource>> {
        self.query.list_pods_by_owner_uid(request)
    }
}

impl klights_pod_api::PodSnapshotQuery for RootApiPodRepository {
    fn snapshot_pods(
        &self,
        request: klights_pod_api::PodSnapshotListRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodSnapshotListOutcome> {
        self.snapshot.snapshot_pods(request)
    }
}

impl klights_pod_api::PodApiMutation for RootApiPodRepository {
    fn create_pod(
        &self,
        request: klights_pod_api::PodApiCreateRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodApiCreateResult> {
        Box::pin(async move {
            validate_pod_effect_authority()?;
            klights_pod_api::PodApiMutation::create_pod(self.api.as_ref(), request).await
        })
    }

    fn update_pod(
        &self,
        request: klights_pod_api::PodApiUpdateRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodApiWriteOutcome> {
        Box::pin(async move {
            validate_pod_effect_authority()?;
            klights_pod_api::PodApiMutation::update_pod(self.api.as_ref(), request).await
        })
    }

    fn patch_pod(
        &self,
        request: klights_pod_api::PodApiPatchRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodApiWriteOutcome> {
        Box::pin(async move {
            validate_pod_effect_authority()?;
            klights_pod_api::PodApiMutation::patch_pod(self.api.as_ref(), request).await
        })
    }

    fn delete_pod(
        &self,
        request: klights_pod_api::PodApiDeleteRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodApiDeleteOutcome> {
        Box::pin(async move {
            validate_pod_effect_authority()?;
            klights_pod_api::PodApiMutation::delete_pod(self.api.as_ref(), request).await
        })
    }

    fn delete_collection_pods(
        &self,
        request: klights_pod_api::PodApiDeleteCollectionRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, ()> {
        Box::pin(async move {
            validate_pod_effect_authority()?;
            klights_pod_api::PodApiMutation::delete_collection_pods(self.api.as_ref(), request)
                .await
        })
    }

    fn bind_pod(
        &self,
        request: klights_pod_api::PodBindingRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, ()> {
        Box::pin(async move {
            validate_pod_effect_authority()?;
            klights_pod_api::PodApiMutation::bind_pod(self.api.as_ref(), request).await
        })
    }
}

impl klights_pod_api::PodSubresourceMutation for RootApiPodRepository {
    fn replace_status(
        &self,
        request: klights_pod_api::PodStatusReplaceRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Resource> {
        Box::pin(async move {
            validate_pod_effect_authority()?;
            klights_pod_api::PodSubresourceMutation::replace_status(
                self.subresource.as_ref(),
                request,
            )
            .await
        })
    }

    fn patch_status(
        &self,
        request: klights_pod_api::PodStatusPatchRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Resource> {
        Box::pin(async move {
            validate_pod_effect_authority()?;
            klights_pod_api::PodSubresourceMutation::patch_status(
                self.subresource.as_ref(),
                request,
            )
            .await
        })
    }

    fn update_ephemeral_containers(
        &self,
        request: klights_pod_api::PodEphemeralContainersRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Resource> {
        Box::pin(async move {
            validate_pod_effect_authority()?;
            klights_pod_api::PodSubresourceMutation::update_ephemeral_containers(
                self.subresource.as_ref(),
                request,
            )
            .await
        })
    }
}

impl klights_pod_api::PodEvictionDelete for RootApiPodRepository {
    fn delete_for_eviction(
        &self,
        request: klights_pod_api::PodEvictionDeleteRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodEvictionDeleteOutcome> {
        Box::pin(async move {
            validate_pod_effect_authority()?;
            let (namespace, name, options, dry_run) = request.into_parts();
            let outcome = klights_pod_api::PodApiMutation::delete_pod(
                self.api.as_ref(),
                klights_pod_api::PodApiDeleteRequest {
                    namespace,
                    name,
                    options,
                    dry_run,
                },
            )
            .await?;
            match outcome {
                klights_pod_api::PodApiDeleteOutcome::DryRun(_) => {
                    Ok(klights_pod_api::PodEvictionDeleteOutcome::DryRun)
                }
                klights_pod_api::PodApiDeleteOutcome::GracefulSet(resource) => {
                    let _ = self
                        .mutation_reconcile
                        .reconcile_pod_mutation(
                            klights_reconcile_api::PodMutationReconcileRequest::RunHooks {
                                pod: resource.clone(),
                                named_hook: None,
                                context: "pod_eviction_mark_terminating",
                            },
                        )
                        .await;
                    Ok(klights_pod_api::PodEvictionDeleteOutcome::Persisted(
                        resource,
                    ))
                }
            }
        })
    }
}

impl klights_reconcile_api::NamespaceTerminationQueueSink for RootApiPodRepository {
    fn enqueue_namespace_termination(
        &self,
        namespace: String,
        uid: String,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'_> {
        self.namespace_termination_queue
            .enqueue_namespace_termination(namespace, uid)
    }
}

impl ApiPodRepository for RootApiPodRepository {
    fn eviction_admission_port(&self) -> Arc<dyn klights_reconcile_api::PodEvictionAdmissionSink> {
        self.eviction_admission.clone()
    }

    fn namespace_bootstrap_port(&self) -> Arc<dyn klights_reconcile_api::NamespaceBootstrapSink> {
        self.namespace_bootstrap.clone()
    }
}

pub(crate) struct RootApiFailureMetrics {
    inner: Arc<klights_controllers::side_effects::SideEffectMetrics>,
}

impl RootApiFailureMetrics {
    pub(crate) fn new(
        inner: Arc<klights_controllers::side_effects::SideEffectMetrics>,
    ) -> Arc<Self> {
        Arc::new(Self { inner })
    }
}

impl klights_reconcile_api::ReconcileFailureMetrics for RootApiFailureMetrics {
    fn record_cascade_delete_failure(&self) {
        klights_reconcile_api::ReconcileFailureMetrics::record_cascade_delete_failure(
            self.inner.as_ref(),
        );
    }

    fn record_namespace_delete_failure(&self) {
        klights_reconcile_api::ReconcileFailureMetrics::record_namespace_delete_failure(
            self.inner.as_ref(),
        );
    }
}

impl ApiFailureMetrics for RootApiFailureMetrics {
    fn render_prometheus(&self) -> String {
        self.inner.render_prometheus()
    }

    fn recent_failures(&self) -> Vec<ApiFailureEntry> {
        self.inner
            .recent_failures()
            .into_iter()
            .map(|entry| ApiFailureEntry {
                api_version: entry.api_version,
                kind: entry.kind,
                namespace: entry.namespace,
                name: entry.name,
                hook: entry.hook,
                context: entry.context,
                error: entry.error,
            })
            .collect()
    }
}

pub(crate) struct RootApiNodeLeaseObservations {
    inner: Arc<klights_controllers::node_lease::NodeLeaseTracker>,
}

impl RootApiNodeLeaseObservations {
    pub(crate) fn new(inner: Arc<klights_controllers::node_lease::NodeLeaseTracker>) -> Arc<Self> {
        Arc::new(Self { inner })
    }
}

impl ApiNodeLeaseObservations for RootApiNodeLeaseObservations {
    fn observed_renew_time<'a>(&'a self, node_name: &'a str) -> ApiNodeLeaseObservedFuture<'a> {
        Box::pin(async move {
            self.inner
                .observed(node_name)
                .await
                .map(|observation| observation.renew_time_string())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use klights_leader_api::{AuthorityRoute, LeaderAuthority};
    use serde_json::json;
    use std::sync::atomic::Ordering;

    #[tokio::test]
    async fn root_namespace_termination_reconciler_is_the_controller_effect_port() {
        let (_db, db_handle) =
            crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
        let metrics = klights_controllers::side_effects::SideEffectMetrics::new();
        let store = RootNamespaceTerminationStore::new(db_handle);
        let reconciler = RootNamespaceTerminationReconciler::new(store, metrics);
        let effect = klights_controllers::side_effects::namespace_termination::effect(reconciler);
        assert_eq!(effect.name(), "namespace_termination");
    }

    #[tokio::test]
    async fn root_namespace_termination_reconciler_shares_metrics_arc() {
        let (_db, db_handle) =
            crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
        let metrics = klights_controllers::side_effects::SideEffectMetrics::new();
        let store = RootNamespaceTerminationStore::new(db_handle);
        let _reconciler = RootNamespaceTerminationReconciler::new(store, metrics.clone());

        metrics
            .namespace_delete_failures_total
            .fetch_add(7, Ordering::Relaxed);
        assert_eq!(
            metrics
                .namespace_delete_failures_total
                .load(Ordering::Relaxed),
            7
        );
    }

    #[tokio::test]
    async fn reconcile_namespace_termination_already_deleted_is_ok() {
        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();
        let store = RootNamespaceTerminationStore::new(Arc::new(db.clone()));
        let metrics = klights_controllers::side_effects::SideEffectMetrics::new();

        k8s_native_service::reconcile_namespace_termination_at(
            store.as_ref(),
            "ghost-ns",
            metrics.as_ref(),
            chrono::DateTime::UNIX_EPOCH,
        )
        .await
        .expect("reconcile against missing namespace must be ok");

        assert_eq!(
            metrics
                .namespace_delete_failures_total
                .load(Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn reconcile_namespace_termination_success_does_not_increment_counter() {
        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();
        let store = RootNamespaceTerminationStore::new(Arc::new(db.clone()));
        let metrics = klights_controllers::side_effects::SideEffectMetrics::new();
        let ns_name = "term-test-ns";
        db.create_namespace(
            ns_name,
            json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": ns_name,
                    "deletionTimestamp": "2026-01-01T00:00:00.000000000Z"
                },
                "spec": {"finalizers": []},
                "status": {"phase": "Terminating"}
            }),
        )
        .await
        .expect("create ns");

        k8s_native_service::reconcile_namespace_termination_at(
            store.as_ref(),
            ns_name,
            metrics.as_ref(),
            chrono::DateTime::UNIX_EPOCH,
        )
        .await
        .expect("reconcile ok");

        assert_eq!(
            metrics
                .namespace_delete_failures_total
                .load(Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn stale_http_authority_scope_rejects_write_after_demote_promote_aba() {
        let (_datastore, db) =
            crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
        let store = RootNamespaceTerminationStore::new(db.clone());
        let (authority, publisher) =
            klights_replication::authority::WatchLeaderAuthority::channel(true, None);
        let AuthorityRoute::Local(permit) = authority.route() else {
            panic!("test authority must begin local");
        };
        let reached_effect_boundary = Arc::new(tokio::sync::Notify::new());
        let resume_effect = Arc::new(tokio::sync::Notify::new());
        let operation = klights_leader_api::scope_authority(authority, permit, {
            let reached_effect_boundary = reached_effect_boundary.clone();
            let resume_effect = resume_effect.clone();
            async move {
                reached_effect_boundary.notify_one();
                resume_effect.notified().await;
                klights_reconcile_api::NamespaceLifecycleStore::update_terminating_namespace(
                    store.as_ref(),
                    "stale-http-write".to_string(),
                    json!({
                        "apiVersion": "v1",
                        "kind": "Namespace",
                        "metadata": {
                            "name": "stale-http-write"
                        }
                    }),
                    1,
                )
                .await
            }
        });
        let transition = async {
            reached_effect_boundary.notified().await;
            publisher.publish(false, None).await;
            publisher.publish(true, None).await;
            resume_effect.notify_one();
        };
        let (result, ()) = tokio::join!(operation, transition);

        let error = result.expect_err("stale HTTP permit must reject the write");
        assert!(format!("{error:?}").contains("leader authority rejected effect"));
        assert!(
            db.get_namespace("stale-http-write")
                .await
                .unwrap()
                .is_none()
        );
    }
}
