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
}

impl RootNamespaceTerminationStore {
    pub(crate) fn new(inner: DatastoreHandle) -> Arc<Self> {
        Arc::new(Self { inner })
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
            self.inner
                .update_resource_with_preconditions(
                    &pod.api_version,
                    &pod.kind,
                    Some(&namespace),
                    &pod.name,
                    body,
                    klights_cluster_core::ResourcePreconditions::from_resource(&pod),
                )
                .await
                .map_err(map_namespace_lifecycle_error)?;
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
            self.inner
                .update_namespace(&namespace, body, expected_resource_version)
                .await
                .map_err(map_namespace_lifecycle_error)
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
            self.inner
                .delete_resource(
                    &resource.api_version,
                    &resource.kind,
                    Some(&namespace),
                    &resource.name,
                )
                .await
                .map_err(map_namespace_lifecycle_error)
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
            self.inner
                .delete_namespace(&namespace)
                .await
                .map_err(map_namespace_lifecycle_error)
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
    inner: Arc<crate::kubelet::pod_repository::PodRepository>,
    api: Arc<dyn klights_pod_api::PodApiMutation>,
    subresource: Arc<dyn klights_pod_api::PodSubresourceMutation>,
}

impl RootApiPodRepository {
    pub(crate) fn new(
        inner: Arc<crate::kubelet::pod_repository::PodRepository>,
        api: Arc<dyn klights_pod_api::PodApiMutation>,
        subresource: Arc<dyn klights_pod_api::PodSubresourceMutation>,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner,
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
        klights_pod_api::PodQuery::get_pod(self.inner.as_ref(), request)
    }

    fn list_pods(
        &self,
        request: klights_pod_api::PodListRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodListResult> {
        klights_pod_api::PodQuery::list_pods(self.inner.as_ref(), request)
    }

    fn list_pods_by_owner_uid(
        &self,
        request: klights_pod_api::PodOwnerListRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Vec<Resource>> {
        klights_pod_api::PodQuery::list_pods_by_owner_uid(self.inner.as_ref(), request)
    }
}

impl klights_pod_api::PodSnapshotQuery for RootApiPodRepository {
    fn snapshot_pods(
        &self,
        request: klights_pod_api::PodSnapshotListRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodSnapshotListOutcome> {
        klights_pod_api::PodSnapshotQuery::snapshot_pods(self.inner.as_ref(), request)
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
                        .inner
                        .mutation_reconcile_port()
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
        klights_reconcile_api::NamespaceTerminationQueueSink::enqueue_namespace_termination(
            self.inner.as_ref(),
            namespace,
            uid,
        )
    }
}

impl ApiPodRepository for RootApiPodRepository {
    fn eviction_admission_port(&self) -> Arc<dyn klights_reconcile_api::PodEvictionAdmissionSink> {
        self.inner.eviction_admission_port()
    }

    fn namespace_bootstrap_port(&self) -> Arc<dyn klights_reconcile_api::NamespaceBootstrapSink> {
        self.inner.namespace_bootstrap_port()
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
            publisher.publish(false, None);
            publisher.publish(true, None);
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
