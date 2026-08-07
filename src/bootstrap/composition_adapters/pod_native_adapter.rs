//! Root adapters from Kubernetes-native Pod ports to kubelet/datastore owners.

use std::sync::Arc;

use async_trait::async_trait;
use klights_cluster_core::Resource;
use klights_pod_api::{
    PodActorFinalizeRequest, PodControlPlaneEventRequest, PodControlPlaneEventSink,
    PodDeleteMarkOutcome, PodDeleteMarkRequest, PodDeleteOrchestration, PodGetRequest,
    PodListRequest, PodListResult, PodMarkedRetryRequest, PodMetadataPatchRequest, PodPersistence,
    PodPersistenceCreateRequest, PodPersistenceReplaceRequest, PodQuery, PodRepositoryError,
    PodRepositoryFuture, PodSpecValidation, PodStatusPersistence, PodStatusWriteRequest,
};

use crate::datastore::{DatastoreHandle, ResourceListQuery};
use crate::kubelet::pod_repository::delete_coordinator::PodDeleteCoordinator;
use crate::kubelet::pod_repository::store::PodStore;
use k8s_native_service::AdmissionResourceStore;

#[cfg(any(test, feature = "pod-repository-test-support"))]
pub(crate) struct SchedulerBindGateForTest {
    entered: std::sync::atomic::AtomicUsize,
    entered_notify: tokio::sync::Notify,
    release_notify: tokio::sync::Notify,
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
#[allow(dead_code)]
impl SchedulerBindGateForTest {
    pub fn new() -> Self {
        Self {
            entered: std::sync::atomic::AtomicUsize::new(0),
            entered_notify: tokio::sync::Notify::new(),
            release_notify: tokio::sync::Notify::new(),
        }
    }

    pub async fn wait_for_entered_at_least(&self, target: usize) {
        loop {
            if self.entered.load(std::sync::atomic::Ordering::SeqCst) >= target {
                return;
            }
            self.entered_notify.notified().await;
        }
    }

    pub fn release_all(&self) {
        self.release_notify.notify_waiters();
    }

    async fn enter_and_wait(&self) {
        self.entered
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.entered_notify.notify_waiters();
        self.release_notify.notified().await;
    }
}

pub(crate) struct RootPodNativeAdapter {
    store: Arc<PodStore>,
    delete_coordinator: Arc<PodDeleteCoordinator>,
    db: DatastoreHandle,
    wall_clock: Arc<dyn klights_supervisor::WallClock>,
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    scheduler_bind_gate: Option<Arc<SchedulerBindGateForTest>>,
}

impl RootPodNativeAdapter {
    pub(crate) fn new(
        store: Arc<PodStore>,
        delete_coordinator: Arc<PodDeleteCoordinator>,
        db: DatastoreHandle,
        wall_clock: Arc<dyn klights_supervisor::WallClock>,
        #[cfg(any(test, feature = "pod-repository-test-support"))] scheduler_bind_gate: Option<
            Arc<SchedulerBindGateForTest>,
        >,
    ) -> Arc<Self> {
        Arc::new(Self {
            store,
            delete_coordinator,
            db,
            wall_clock,
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            scheduler_bind_gate,
        })
    }
}

fn map_store_error(error: anyhow::Error, namespace: &str, name: &str) -> PodRepositoryError {
    if let Some(error) = error.downcast_ref::<PodRepositoryError>() {
        return error.clone();
    }
    let message = error.to_string();
    if message.contains("already exists") && message.contains("409 Conflict") {
        PodRepositoryError::already_exists(message)
    } else if message.contains("409 Conflict") {
        PodRepositoryError::conflict(message)
    } else if message.to_ascii_lowercase().contains("not found") {
        PodRepositoryError::not_found(namespace, name)
    } else {
        PodRepositoryError::internal(message)
    }
}

impl PodQuery for RootPodNativeAdapter {
    fn get_pod(&self, request: PodGetRequest) -> PodRepositoryFuture<'_, Option<Resource>> {
        PodQuery::get_pod(self.store.as_ref(), request)
    }

    fn list_pods(&self, request: PodListRequest) -> PodRepositoryFuture<'_, PodListResult> {
        PodQuery::list_pods(self.store.as_ref(), request)
    }

    fn list_pods_by_owner_uid(
        &self,
        request: klights_pod_api::PodOwnerListRequest,
    ) -> PodRepositoryFuture<'_, Vec<Resource>> {
        PodQuery::list_pods_by_owner_uid(self.store.as_ref(), request)
    }
}

impl PodPersistence for RootPodNativeAdapter {
    fn create_pod(
        &self,
        request: PodPersistenceCreateRequest,
    ) -> PodRepositoryFuture<'_, Resource> {
        Box::pin(async move {
            self.store
                .create(&request.namespace, &request.name, request.body)
                .await
                .map_err(|error| map_store_error(error, &request.namespace, &request.name))
        })
    }

    fn replace_pod(
        &self,
        request: PodPersistenceReplaceRequest,
    ) -> PodRepositoryFuture<'_, Resource> {
        Box::pin(async move {
            self.store
                .update(
                    &request.namespace,
                    &request.name,
                    request.body,
                    request.expected_resource_version,
                )
                .await
                .map_err(|error| map_store_error(error, &request.namespace, &request.name))
        })
    }

    fn replace_pod_including_status(
        &self,
        request: PodPersistenceReplaceRequest,
    ) -> PodRepositoryFuture<'_, Resource> {
        #[cfg(any(test, feature = "pod-repository-test-support"))]
        let scheduler_bind_gate = self.scheduler_bind_gate.clone();
        Box::pin(async move {
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            if let Some(gate) = scheduler_bind_gate {
                gate.enter_and_wait().await;
            }
            self.store
                .update_including_status_for_scheduler(
                    &request.namespace,
                    &request.name,
                    request.body,
                    request.expected_resource_version,
                )
                .await
                .map_err(|error| map_store_error(error, &request.namespace, &request.name))
        })
    }

    fn patch_pod_metadata(
        &self,
        request: PodMetadataPatchRequest,
    ) -> PodRepositoryFuture<'_, Resource> {
        Box::pin(async move {
            self.store
                .patch_metadata(
                    &request.namespace,
                    &request.name,
                    &request.expected_uid,
                    request.expected_resource_version,
                    request.patch,
                )
                .await
                .map_err(|error| map_store_error(error, &request.namespace, &request.name))
        })
    }
}

impl PodStatusPersistence for RootPodNativeAdapter {
    fn write_pod_status(
        &self,
        request: PodStatusWriteRequest,
    ) -> PodRepositoryFuture<'_, Resource> {
        Box::pin(async move {
            self.store
                .update_status_typed(
                    &request.namespace,
                    &request.name,
                    request.status,
                    request.expected_resource_version,
                )
                .await
        })
    }
}

impl PodDeleteOrchestration for RootPodNativeAdapter {
    fn preview_delete(
        &self,
        resource: &Resource,
        requested_grace_period_seconds: Option<i64>,
    ) -> serde_json::Value {
        self.delete_coordinator
            .dry_run_delete_body(resource, requested_grace_period_seconds)
    }

    fn mark_and_queue_delete(
        &self,
        request: PodDeleteMarkRequest,
    ) -> PodRepositoryFuture<'_, PodDeleteMarkOutcome> {
        Box::pin(async move {
            let outcome = self
                .delete_coordinator
                .mark_and_queue_api_delete(
                    &request.namespace,
                    &request.name,
                    request.requested_grace_period_seconds,
                    &request.preconditions,
                    request.initial_resource,
                )
                .await?;
            Ok(PodDeleteMarkOutcome {
                updated: outcome.updated,
                previous: outcome.previous,
                uid: outcome.uid,
                changed: outcome.changed,
            })
        })
    }

    fn enqueue_actor_finalize_if_ready(
        &self,
        request: PodActorFinalizeRequest,
    ) -> PodRepositoryFuture<'_, ()> {
        Box::pin(async move {
            self.delete_coordinator
                .enqueue_actor_finalize_if_ready(
                    &request.namespace,
                    &request.name,
                    &request.resource,
                )
                .await;
            Ok(())
        })
    }

    fn enqueue_marked_retry(&self, request: PodMarkedRetryRequest) -> PodRepositoryFuture<'_, ()> {
        Box::pin(async move {
            self.delete_coordinator
                .enqueue_marked_pod_retry(
                    request.namespace,
                    request.name,
                    request.uid,
                    request.run_after,
                    &request.pod_data,
                )
                .await
                .map_err(|error| PodRepositoryError::internal(error.to_string()))
        })
    }
}

impl PodSpecValidation for RootPodNativeAdapter {
    fn validate_volume_paths(&self, pod: &serde_json::Value) -> Result<(), PodRepositoryError> {
        klights_kubelet::volumes::validate_volume_subpaths(pod)
            .and_then(|()| klights_kubelet::volumes::validate_volume_projection_paths(pod))
            .map_err(PodRepositoryError::unprocessable)
    }
}

impl PodControlPlaneEventSink for RootPodNativeAdapter {
    fn emit_pod_event(&self, request: PodControlPlaneEventRequest) -> PodRepositoryFuture<'_, ()> {
        Box::pin(async move {
            let adapter = crate::bootstrap::composition_adapters::pod_event_adapter::DatastorePodEventAdapter::new(
                self.db.as_ref(),
            );
            klights_kubelet::pod_events::emit_control_plane_pod_event(
                &adapter,
                &adapter,
                klights_kubelet::pod_events::PodEventRecord {
                    pod: request.pod.as_ref(),
                    reason: &request.reason,
                    message: &request.message,
                    event_type: &request.event_type,
                    reporting_component: &request.reporting_component,
                    reporting_instance: &request.reporting_instance,
                    operation_now: self.wall_clock.now_utc(),
                },
            )
            .await
            .map(|_| ())
            .map_err(|error| PodRepositoryError::internal(error.to_string()))
        })
    }
}

#[async_trait]
impl AdmissionResourceStore for RootPodNativeAdapter {
    async fn get_admission_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>, klights_leader_api::ResourceQueryError> {
        self.db
            .get_resource(api_version, kind, namespace, name)
            .await
            .map_err(|error| {
                klights_leader_api::ResourceQueryError::retryable(format!(
                    "Pod admission resource read failed: {error}"
                ))
            })
    }

    async fn list_admission_resources(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>, klights_leader_api::ResourceQueryError> {
        self.db
            .list_resources(api_version, kind, namespace, ResourceListQuery::all())
            .await
            .map(|list| list.items)
            .map_err(|error| {
                klights_leader_api::ResourceQueryError::retryable(format!(
                    "Pod admission resource list failed: {error}"
                ))
            })
    }
}
