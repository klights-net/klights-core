use std::sync::Arc;

use klights_cluster_core::{Resource, ResourcePatchRequest, ResourcePreconditions};
use serde_json::Value;

use crate::api::state_ports::{
    ApiFailureEntry, ApiFailureMetrics, ApiNodeLeaseObservations, ApiNodeLeaseObservedFuture,
    ApiPodRepository, ApiResourceStore,
};
use crate::api::{AppError, NamespaceTerminationStore};
use crate::datastore::DatastoreHandle;

pub(crate) struct RootApiResourceStore {
    inner: DatastoreHandle,
    watch: crate::watch_stream_adapter::DatastoreWatchStreamAdapter,
}

impl RootApiResourceStore {
    pub(crate) fn new(
        inner: DatastoreHandle,
        watch_signals: Arc<dyn klights_watch::WatchSignalSubscribe>,
    ) -> Arc<Self> {
        Arc::new(Self {
            watch: crate::watch_stream_adapter::DatastoreWatchStreamAdapter::new(
                inner.clone(),
                watch_signals,
            ),
            inner,
        })
    }
}

impl crate::api::watch_stream::WatchStreamSource for RootApiResourceStore {
    fn wait_until_fresh<'a>(
        &'a self,
        target_rv: i64,
        api_version: &'a str,
        kind: &'a str,
        task_supervisor: &'a klights_supervisor::TaskSupervisor,
    ) -> crate::api::watch_stream::WatchSourceWaitFuture<'a> {
        crate::api::watch_stream::WatchStreamSource::wait_until_fresh(
            &self.watch,
            target_rv,
            api_version,
            kind,
            task_supervisor,
        )
    }

    fn list_watch_resources<'a>(
        &'a self,
        api_version: &'a str,
        kind: &'a str,
        namespace: Option<&'a str>,
        label_selector: Option<&'a str>,
        field_selector: Option<&'a str>,
        limit: Option<i64>,
    ) -> crate::api::watch_stream::WatchSourceListFuture<'a> {
        crate::api::watch_stream::WatchStreamSource::list_watch_resources(
            &self.watch,
            api_version,
            kind,
            namespace,
            label_selector,
            field_selector,
            limit,
        )
    }

    fn watch_resources(
        &self,
        request: klights_leader_api::WatchRequest,
    ) -> klights_leader_api::LeaderWatchFuture<'_> {
        crate::api::watch_stream::WatchStreamSource::watch_resources(&self.watch, request)
    }
}

#[async_trait::async_trait]
impl NamespaceTerminationStore for RootApiResourceStore {
    async fn get_terminating_namespace(
        &self,
        namespace: &str,
    ) -> Result<Option<Resource>, AppError> {
        NamespaceTerminationStore::get_terminating_namespace(self.inner.as_ref(), namespace).await
    }

    async fn list_namespace_pods(&self, namespace: &str) -> Result<Vec<Resource>, AppError> {
        NamespaceTerminationStore::list_namespace_pods(self.inner.as_ref(), namespace).await
    }

    async fn mark_namespace_pod_terminating(
        &self,
        pod: &Resource,
        namespace: &str,
        body: Value,
    ) -> Result<(), AppError> {
        NamespaceTerminationStore::mark_namespace_pod_terminating(
            self.inner.as_ref(),
            pod,
            namespace,
            body,
        )
        .await
    }

    async fn update_terminating_namespace(
        &self,
        namespace: &str,
        body: Value,
        expected_resource_version: i64,
    ) -> Result<Resource, AppError> {
        NamespaceTerminationStore::update_terminating_namespace(
            self.inner.as_ref(),
            namespace,
            body,
            expected_resource_version,
        )
        .await
    }

    async fn list_namespace_non_pod_resources(
        &self,
        namespace: &str,
    ) -> Result<Vec<Resource>, AppError> {
        NamespaceTerminationStore::list_namespace_non_pod_resources(self.inner.as_ref(), namespace)
            .await
    }

    async fn delete_namespace_non_pod_resource(
        &self,
        resource: &Resource,
        namespace: &str,
    ) -> Result<(), AppError> {
        NamespaceTerminationStore::delete_namespace_non_pod_resource(
            self.inner.as_ref(),
            resource,
            namespace,
        )
        .await
    }

    async fn count_namespace_resources(&self, namespace: &str) -> Result<i64, AppError> {
        NamespaceTerminationStore::count_namespace_resources(self.inner.as_ref(), namespace).await
    }

    async fn delete_terminating_namespace(&self, namespace: &str) -> anyhow::Result<()> {
        NamespaceTerminationStore::delete_terminating_namespace(self.inner.as_ref(), namespace)
            .await
    }
}

#[async_trait::async_trait]
impl ApiResourceStore for RootApiResourceStore {
    async fn create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
    ) -> anyhow::Result<Resource> {
        self.inner
            .create_resource(api_version, kind, namespace, name, data)
            .await
    }

    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> anyhow::Result<Option<Resource>> {
        self.inner
            .get_resource(api_version, kind, namespace, name)
            .await
    }

    async fn update_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        expected_resource_version: i64,
    ) -> anyhow::Result<Resource> {
        self.inner
            .update_resource(
                api_version,
                kind,
                namespace,
                name,
                data,
                expected_resource_version,
            )
            .await
    }

    async fn update_status_only_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: Value,
        preconditions: ResourcePreconditions,
    ) -> anyhow::Result<Resource> {
        self.inner
            .update_status_only_with_preconditions(
                api_version,
                kind,
                namespace,
                name,
                status,
                preconditions,
            )
            .await
    }

    async fn patch_resource_latest_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        request: ResourcePatchRequest,
    ) -> anyhow::Result<Option<Resource>> {
        self.inner
            .patch_resource_latest_with_preconditions(api_version, kind, namespace, name, request)
            .await
    }

    async fn delete_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> anyhow::Result<()> {
        self.inner
            .delete_resource(api_version, kind, namespace, name)
            .await
    }

    async fn list_resource_keys_for_scope(
        &self,
        api_version: String,
        kind: String,
        namespaced: bool,
    ) -> anyhow::Result<Vec<(Option<String>, String)>> {
        self.inner
            .list_resource_keys_for_scope(api_version, kind, namespaced)
            .await
    }

    async fn create_namespace(&self, name: &str, data: Value) -> anyhow::Result<Resource> {
        self.inner.create_namespace(name, data).await
    }

    async fn get_namespace(&self, name: &str) -> anyhow::Result<Option<Resource>> {
        self.inner.get_namespace(name).await
    }

    async fn update_namespace(
        &self,
        name: &str,
        data: Value,
        expected_resource_version: i64,
    ) -> anyhow::Result<Resource> {
        self.inner
            .update_namespace(name, data, expected_resource_version)
            .await
    }

    async fn delete_namespace(&self, name: &str) -> anyhow::Result<()> {
        self.inner.delete_namespace(name).await
    }
}

pub(crate) struct RootApiPodRepository {
    inner: Arc<crate::kubelet::pod_repository::PodRepository>,
    api: Arc<crate::pod_api_service::PodApiService>,
    subresource: Arc<crate::pod_subresource_service::PodSubresourceService>,
}

impl RootApiPodRepository {
    pub(crate) fn new(
        inner: Arc<crate::kubelet::pod_repository::PodRepository>,
        api: Arc<crate::pod_api_service::PodApiService>,
        subresource: Arc<crate::pod_subresource_service::PodSubresourceService>,
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
        klights_pod_api::PodApiMutation::create_pod(self.api.as_ref(), request)
    }

    fn update_pod(
        &self,
        request: klights_pod_api::PodApiUpdateRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodApiWriteOutcome> {
        klights_pod_api::PodApiMutation::update_pod(self.api.as_ref(), request)
    }

    fn patch_pod(
        &self,
        request: klights_pod_api::PodApiPatchRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodApiWriteOutcome> {
        klights_pod_api::PodApiMutation::patch_pod(self.api.as_ref(), request)
    }

    fn delete_pod(
        &self,
        request: klights_pod_api::PodApiDeleteRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodApiDeleteOutcome> {
        klights_pod_api::PodApiMutation::delete_pod(self.api.as_ref(), request)
    }

    fn delete_collection_pods(
        &self,
        request: klights_pod_api::PodApiDeleteCollectionRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, ()> {
        klights_pod_api::PodApiMutation::delete_collection_pods(self.api.as_ref(), request)
    }
}

impl klights_pod_api::PodSubresourceMutation for RootApiPodRepository {
    fn replace_status(
        &self,
        request: klights_pod_api::PodStatusReplaceRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Resource> {
        klights_pod_api::PodSubresourceMutation::replace_status(self.subresource.as_ref(), request)
    }

    fn patch_status(
        &self,
        request: klights_pod_api::PodStatusPatchRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Resource> {
        klights_pod_api::PodSubresourceMutation::patch_status(self.subresource.as_ref(), request)
    }

    fn update_ephemeral_containers(
        &self,
        request: klights_pod_api::PodEphemeralContainersRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Resource> {
        klights_pod_api::PodSubresourceMutation::update_ephemeral_containers(
            self.subresource.as_ref(),
            request,
        )
    }
}

impl klights_pod_api::PodEvictionDelete for RootApiPodRepository {
    fn delete_for_eviction(
        &self,
        request: klights_pod_api::PodEvictionDeleteRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodEvictionDeleteOutcome> {
        Box::pin(async move {
            let (namespace, name, options, dry_run) = request.into_parts();
            let outcome = self
                .api
                .repository_delete_pod(&namespace, &name, options, dry_run)
                .await?;
            match outcome {
                crate::kubelet::pod_repository::PodApiDeleteOutcome::DryRun(_) => {
                    Ok(klights_pod_api::PodEvictionDeleteOutcome::DryRun)
                }
                crate::kubelet::pod_repository::PodApiDeleteOutcome::GracefulSet(resource) => {
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

    fn bind_pod_from_api(
        &self,
        namespace: &str,
        name: &str,
        binding: Value,
        dry_run: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + '_>> {
        let namespace = namespace.to_owned();
        let name = name.to_owned();
        Box::pin(async move {
            self.api
                .bind_pod_from_api(&namespace, &name, binding, dry_run)
                .await
                .map_err(|error| anyhow::anyhow!("{error:?}"))
        })
    }
}

pub(crate) struct RootApiFailureMetrics {
    inner: Arc<crate::side_effects::SideEffectMetrics>,
}

impl RootApiFailureMetrics {
    pub(crate) fn new(inner: Arc<crate::side_effects::SideEffectMetrics>) -> Arc<Self> {
        Arc::new(Self { inner })
    }
}

impl crate::api::NamespaceTerminationMetrics for RootApiFailureMetrics {
    fn record_namespace_delete_failure(&self) {
        klights_reconcile_api::ReconcileFailureMetrics::record_namespace_delete_failure(
            self.inner.as_ref(),
        );
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
    inner: Arc<crate::node_lease_tracker::NodeLeaseTracker>,
}

impl RootApiNodeLeaseObservations {
    pub(crate) fn new(inner: Arc<crate::node_lease_tracker::NodeLeaseTracker>) -> Arc<Self> {
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
