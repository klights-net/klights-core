//! Private concrete persistence adapters for the Pod repository.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use klights_cluster_datastore::diagnostics::{NoopResourceWrite, log_noop_resource_write};
use klights_kubelet::unscheduled_deletion::{
    EligibleUnscheduledPodDeletion, UnscheduledPodDeleteCasOutcome,
    UnscheduledPodDeletionObservation, UnscheduledPodDeletionPort,
    UnscheduledPodDeletionPortFuture, UnscheduledPodDeletionService,
};
#[cfg(feature = "pod-repository-test-support")]
use klights_pod_api::BoundPodFinalization;
use klights_pod_api::{
    BoundPodFinalizationError, BoundPodFinalizationFuture, BoundPodFinalizationOutcome,
    BoundPodFinalizationRequest, PodListResult, PodRepositoryCreateRequest, PodRepositoryError,
    PodRepositoryFuture, PodRepositoryGetRequest, PodRepositoryListRequest,
    PodRepositoryOwnerListRequest, PodRepositoryPatchRequest, PodRepositoryReadPersistence,
    PodRepositoryReplaceRequest, PodRepositoryStatusNoop, PodRepositoryStatusRequest,
    PodRepositoryWritePersistence, PodSnapshotListOutcome, PodSnapshotListRequest,
    UnscheduledPodDeletion,
};
use klights_types::PodIdentity;

use crate::datastore::DatastoreHandle;
#[cfg(any(test, feature = "pod-repository-test-support"))]
use klights_kubelet::pod_repository::store::PodRepositoryWatchPersistence;
use klights_kubelet::pod_repository::store::{ActorPodDeleteObservation, PodStore};

struct RootPodRepositoryPersistenceAdapter {
    db: DatastoreHandle,
    sandbox_gc_dirty: Arc<AtomicUsize>,
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    delete_cas_hook: Option<Arc<dyn PodDeleteCasTestHook>>,
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
pub(crate) trait PodDeleteCasTestHook: Send + Sync {
    fn before_delete_cas<'a>(
        &'a self,
        identity: &'a PodIdentity,
        observed_resource_version: i64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>>;
}

pub(crate) trait LocalBoundPodFinalizationPersistence: Send + Sync {
    fn finalize_bound_pod(
        &self,
        request: BoundPodFinalizationRequest,
    ) -> BoundPodFinalizationFuture<'_>;
}

pub(crate) struct RootPodRepositoryPersistenceParts {
    pub(crate) store: Arc<PodStore>,
    pub(crate) bound_finalization: Arc<dyn LocalBoundPodFinalizationPersistence>,
    pub(crate) unscheduled_deletion: Arc<dyn UnscheduledPodDeletion>,
}

fn pod_persistence_error(error: anyhow::Error, namespace: &str, name: &str) -> PodRepositoryError {
    if let Some(error) = error.downcast_ref::<klights_cluster_core::StorageMutationError>() {
        use klights_cluster_core::StorageCommandRejectionCode;
        let message = error.message().to_string();
        return match error.rejection_code() {
            Some(StorageCommandRejectionCode::AlreadyExists) => {
                PodRepositoryError::already_exists(message)
            }
            Some(StorageCommandRejectionCode::Conflict) => PodRepositoryError::conflict(message),
            Some(StorageCommandRejectionCode::NotFound) => {
                PodRepositoryError::not_found(namespace, name)
            }
            Some(StorageCommandRejectionCode::InvalidCommit) => {
                PodRepositoryError::internal(message)
            }
            None => PodRepositoryError::unavailable(message),
        };
    }
    if let Some(error) = error.downcast_ref::<klights_cluster_datastore::errors::DatastoreError>() {
        return match error {
            klights_cluster_datastore::errors::DatastoreError::AlreadyExists { message } => {
                PodRepositoryError::already_exists(message)
            }
            klights_cluster_datastore::errors::DatastoreError::Conflict { message } => {
                PodRepositoryError::conflict(message)
            }
            klights_cluster_datastore::errors::DatastoreError::NotFound { .. } => {
                PodRepositoryError::not_found(namespace, name)
            }
        };
    }
    PodRepositoryError::unavailable(error.to_string())
}

impl PodRepositoryReadPersistence for RootPodRepositoryPersistenceAdapter {
    fn get_persisted_pod(
        &self,
        request: PodRepositoryGetRequest,
    ) -> PodRepositoryFuture<'_, Option<klights_cluster_core::Resource>> {
        Box::pin(async move {
            self.db
                .get_resource("v1", "Pod", Some(&request.namespace), &request.name)
                .await
                .map_err(|error| pod_persistence_error(error, &request.namespace, &request.name))
        })
    }

    fn list_persisted_pods(
        &self,
        request: PodRepositoryListRequest,
    ) -> PodRepositoryFuture<'_, PodListResult> {
        Box::pin(async move {
            let list = self
                .db
                .list_resources(
                    "v1",
                    "Pod",
                    request.namespace.as_deref(),
                    crate::datastore::ResourceListQuery::new(
                        request.label_selector.as_deref(),
                        request.field_selector.as_deref(),
                        request.limit,
                        request.continue_token.as_deref(),
                    ),
                )
                .await
                .map_err(|error| {
                    pod_persistence_error(
                        error,
                        request.namespace.as_deref().unwrap_or_default(),
                        "Pod list",
                    )
                })?;
            PodListResult::try_new(
                list.items,
                list.resource_version,
                list.continue_token,
                list.remaining_item_count,
            )
        })
    }

    fn snapshot_persisted_pods(
        &self,
        request: PodSnapshotListRequest,
    ) -> PodRepositoryFuture<'_, PodSnapshotListOutcome> {
        Box::pin(async move {
            let list = request.list;
            let snapshot = self
                .db
                .snapshot_resources_at_rv(
                    "v1",
                    "Pod",
                    list.namespace(),
                    crate::datastore::ResourceListQuery::new(
                        list.label_selector(),
                        list.field_selector(),
                        list.limit(),
                        list.continue_token(),
                    ),
                    request.snapshot_resource_version,
                )
                .await
                .map_err(|error| PodRepositoryError::unavailable(error.to_string()))?;
            Ok(match snapshot {
                crate::datastore::SnapshotAtRv::List(list) => {
                    PodSnapshotListOutcome::List(PodListResult::try_new(
                        list.items,
                        list.resource_version,
                        list.continue_token,
                        list.remaining_item_count,
                    )?)
                }
                crate::datastore::SnapshotAtRv::Current => PodSnapshotListOutcome::Current,
                crate::datastore::SnapshotAtRv::Expired => PodSnapshotListOutcome::Expired,
            })
        })
    }

    fn list_persisted_pods_by_owner(
        &self,
        request: PodRepositoryOwnerListRequest,
    ) -> PodRepositoryFuture<'_, Vec<klights_cluster_core::Resource>> {
        Box::pin(async move {
            self.db
                .list_resources_by_owner_uid(
                    "v1",
                    "Pod",
                    Some(&request.namespace),
                    &request.owner_uid,
                )
                .await
                .map_err(|error| pod_persistence_error(error, &request.namespace, "Pod owner list"))
        })
    }
}

impl PodRepositoryWritePersistence for RootPodRepositoryPersistenceAdapter {
    fn create_persisted_pod(
        &self,
        request: PodRepositoryCreateRequest,
    ) -> PodRepositoryFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async move {
            self.db
                .create_resource(
                    "v1",
                    "Pod",
                    Some(&request.namespace),
                    &request.name,
                    request.body,
                )
                .await
                .map_err(|error| pod_persistence_error(error, &request.namespace, &request.name))
        })
    }

    fn replace_persisted_pod(
        &self,
        request: PodRepositoryReplaceRequest,
    ) -> PodRepositoryFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async move {
            self.db
                .update_resource_with_preconditions(
                    "v1",
                    "Pod",
                    Some(&request.namespace),
                    &request.name,
                    request.body,
                    request.preconditions,
                )
                .await
                .map_err(|error| pod_persistence_error(error, &request.namespace, &request.name))
        })
    }

    fn patch_persisted_pod(
        &self,
        request: PodRepositoryPatchRequest,
    ) -> PodRepositoryFuture<'_, Option<klights_cluster_core::Resource>> {
        Box::pin(async move {
            self.db
                .patch_resource_latest_with_preconditions(
                    "v1",
                    "Pod",
                    Some(&request.namespace),
                    &request.name,
                    crate::datastore::ResourcePatchRequest::new(
                        request.patch_kind,
                        request.patch,
                        request.preconditions,
                    ),
                )
                .await
                .map_err(|error| pod_persistence_error(error, &request.namespace, &request.name))
        })
    }

    fn write_persisted_pod_status(
        &self,
        request: PodRepositoryStatusRequest,
    ) -> PodRepositoryFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async move {
            self.db
                .update_status_only_with_preconditions(
                    "v1",
                    "Pod",
                    Some(&request.namespace),
                    &request.name,
                    request.status,
                    request.preconditions,
                )
                .await
                .map_err(|error| pod_persistence_error(error, &request.namespace, &request.name))
        })
    }

    fn log_persisted_pod_status_noop(&self, request: PodRepositoryStatusNoop<'_>) {
        log_noop_resource_write(NoopResourceWrite {
            operation: "pod_store_update_status",
            api_version: "v1",
            kind: "Pod",
            namespace: Some(request.namespace),
            name: request.name,
            uid: &request.resource.uid,
            resource_version: request.resource.resource_version,
            reason: "pod status unchanged",
        });
    }
}

#[cfg(any(test, feature = "pod-repository-test-support"))]
impl PodRepositoryWatchPersistence for RootPodRepositoryPersistenceAdapter {
    fn pod_watch_receiver(&self) -> tokio::sync::broadcast::Receiver<klights_watch::WatchEvent> {
        self.db
            .subscribe_watch(klights_watch::WatchTopic::new("v1", "Pod"))
    }
}

impl LocalBoundPodFinalizationPersistence for RootPodRepositoryPersistenceAdapter {
    fn finalize_bound_pod(
        &self,
        request: BoundPodFinalizationRequest,
    ) -> BoundPodFinalizationFuture<'_> {
        Box::pin(async move {
            let identity = request.into_identity();
            let current = self
                .db
                .get_resource("v1", "Pod", Some(&identity.namespace), &identity.name)
                .await
                .map_err(|error| BoundPodFinalizationError::unavailable(error.to_string()))?;
            let observed_resource_version =
                match klights_kubelet::pod_repository::store::classify_bound_finalization(
                    current.as_ref(),
                    &identity.uid,
                ) {
                    ActorPodDeleteObservation::Ready {
                        resource_version, ..
                    } => resource_version,
                    ActorPodDeleteObservation::IdentityChanged => {
                        return Ok(BoundPodFinalizationOutcome::IdentityChanged);
                    }
                    ActorPodDeleteObservation::FinalizersPending => {
                        return Ok(BoundPodFinalizationOutcome::FinalizersPending);
                    }
                    ActorPodDeleteObservation::Retry => {
                        return Ok(BoundPodFinalizationOutcome::Retry);
                    }
                };
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            if let Some(hook) = &self.delete_cas_hook {
                hook.before_delete_cas(&identity, observed_resource_version)
                    .await
                    .map_err(|error| BoundPodFinalizationError::unavailable(error.to_string()))?;
            }
            let preconditions = klights_cluster_core::ResourcePreconditions {
                uid: Some(identity.uid),
                resource_version: Some(observed_resource_version),
            };
            match self
                .db
                .delete_resource_with_preconditions(
                    "v1",
                    "Pod",
                    Some(&identity.namespace),
                    &identity.name,
                    preconditions,
                )
                .await
            {
                Ok(()) => {
                    self.sandbox_gc_dirty.fetch_add(1, Ordering::Release);
                    Ok(BoundPodFinalizationOutcome::Removed)
                }
                Err(error) if klights_cluster_datastore::errors::is_conflict_error(&error) => {
                    Ok(BoundPodFinalizationOutcome::Retry)
                }
                Err(error)
                    if error
                        .downcast_ref::<klights_cluster_datastore::errors::DatastoreError>()
                        .is_some_and(|error| {
                            matches!(
                                error,
                                klights_cluster_datastore::errors::DatastoreError::NotFound { .. }
                            )
                        }) =>
                {
                    Ok(BoundPodFinalizationOutcome::IdentityChanged)
                }
                Err(error) => Err(BoundPodFinalizationError::unavailable(error.to_string())),
            }
        })
    }
}

#[cfg(feature = "pod-repository-test-support")]
impl BoundPodFinalization for RootPodRepositoryPersistenceAdapter {
    fn finalize_bound_pod(
        &self,
        request: BoundPodFinalizationRequest,
    ) -> BoundPodFinalizationFuture<'_> {
        LocalBoundPodFinalizationPersistence::finalize_bound_pod(self, request)
    }
}

impl RootPodRepositoryPersistenceAdapter {
    fn validate_delete_lease() -> Result<(), klights_pod_api::UnscheduledPodDeletionError> {
        klights_leader_api::validate_controller_lease_if_scoped().map_err(|error| {
            klights_pod_api::UnscheduledPodDeletionError::unavailable(format!(
                "controller authority rejected unscheduled Pod deletion: {error}"
            ))
        })
    }
}

impl UnscheduledPodDeletionPort for RootPodRepositoryPersistenceAdapter {
    fn observe_pod<'a>(
        &'a self,
        identity: &'a PodIdentity,
    ) -> UnscheduledPodDeletionPortFuture<'a, Option<UnscheduledPodDeletionObservation>> {
        Box::pin(async move {
            Self::validate_delete_lease()?;
            let Some(resource) = self
                .db
                .get_resource("v1", "Pod", Some(&identity.namespace), &identity.name)
                .await
                .map_err(|error| {
                    klights_pod_api::UnscheduledPodDeletionError::unavailable(error.to_string())
                })?
            else {
                return Ok(None);
            };
            let node_name = resource
                .data
                .pointer("/spec/nodeName")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string);
            let terminating = resource
                .data
                .pointer("/metadata/deletionTimestamp")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|timestamp| !timestamp.is_empty());
            let has_finalizers = resource
                .data
                .pointer("/metadata/finalizers")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|finalizers| !finalizers.is_empty());
            UnscheduledPodDeletionObservation::try_new(
                PodIdentity::new(
                    resource.namespace.as_deref().unwrap_or_default(),
                    &resource.name,
                    &resource.uid,
                ),
                resource.resource_version,
                node_name,
                terminating,
                has_finalizers,
            )
            .map(Some)
        })
    }

    fn compare_and_swap_delete(
        &self,
        eligible: EligibleUnscheduledPodDeletion,
    ) -> UnscheduledPodDeletionPortFuture<'_, UnscheduledPodDeleteCasOutcome> {
        Box::pin(async move {
            Self::validate_delete_lease()?;
            let identity = eligible.identity();
            let preconditions = klights_cluster_core::ResourcePreconditions {
                uid: Some(identity.uid.clone()),
                resource_version: Some(eligible.observed_resource_version()),
            };
            #[cfg(any(test, feature = "pod-repository-test-support"))]
            if let Some(hook) = &self.delete_cas_hook {
                hook.before_delete_cas(identity, eligible.observed_resource_version())
                    .await
                    .map_err(|error| {
                        klights_pod_api::UnscheduledPodDeletionError::unavailable(error.to_string())
                    })?;
            }
            match self
                .db
                .delete_resource_with_preconditions(
                    "v1",
                    "Pod",
                    Some(&identity.namespace),
                    &identity.name,
                    preconditions,
                )
                .await
            {
                Ok(()) => {
                    self.sandbox_gc_dirty.fetch_add(1, Ordering::Release);
                    Ok(UnscheduledPodDeleteCasOutcome::Removed)
                }
                Err(error) if klights_cluster_datastore::errors::is_conflict_error(&error) => {
                    Ok(UnscheduledPodDeleteCasOutcome::Conflict)
                }
                Err(error)
                    if error
                        .downcast_ref::<klights_cluster_datastore::errors::DatastoreError>()
                        .is_some_and(|error| {
                            matches!(
                                error,
                                klights_cluster_datastore::errors::DatastoreError::NotFound { .. }
                            )
                        }) =>
                {
                    Ok(UnscheduledPodDeleteCasOutcome::Gone)
                }
                Err(error) => Err(klights_pod_api::UnscheduledPodDeletionError::unavailable(
                    error.to_string(),
                )),
            }
        })
    }
}

fn concrete_adapter(
    db: DatastoreHandle,
    sandbox_gc_dirty: Arc<AtomicUsize>,
    #[cfg(any(test, feature = "pod-repository-test-support"))] delete_cas_hook: Option<
        Arc<dyn PodDeleteCasTestHook>,
    >,
) -> Arc<RootPodRepositoryPersistenceAdapter> {
    Arc::new(RootPodRepositoryPersistenceAdapter {
        db,
        sandbox_gc_dirty,
        #[cfg(any(test, feature = "pod-repository-test-support"))]
        delete_cas_hook,
    })
}

pub(crate) fn new_root_parts(
    db: DatastoreHandle,
    _wall_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
) -> RootPodRepositoryPersistenceParts {
    let sandbox_gc_dirty = Arc::new(AtomicUsize::new(1));
    let concrete = concrete_adapter(
        db,
        sandbox_gc_dirty.clone(),
        #[cfg(any(test, feature = "pod-repository-test-support"))]
        None,
    );
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    let store = Arc::new(PodStore::from_persistence_with_watch(
        concrete.clone(),
        concrete.clone(),
        sandbox_gc_dirty,
        Some(concrete.clone()),
    ));
    #[cfg(not(any(test, feature = "pod-repository-test-support")))]
    let store = Arc::new(PodStore::from_persistence(
        concrete.clone(),
        concrete.clone(),
        sandbox_gc_dirty,
    ));
    RootPodRepositoryPersistenceParts {
        store,
        bound_finalization: concrete.clone(),
        unscheduled_deletion: Arc::new(UnscheduledPodDeletionService::new(concrete.clone())),
    }
}

#[cfg(feature = "pod-repository-test-support")]
pub(crate) fn new_root_parts_with_delete_cas_hook(
    db: DatastoreHandle,
    delete_cas_hook: Arc<dyn PodDeleteCasTestHook>,
) -> RootPodRepositoryPersistenceParts {
    let sandbox_gc_dirty = Arc::new(AtomicUsize::new(1));
    let concrete = concrete_adapter(db, sandbox_gc_dirty.clone(), Some(delete_cas_hook));
    let store = Arc::new(PodStore::from_persistence_with_watch(
        concrete.clone(),
        concrete.clone(),
        sandbox_gc_dirty,
        Some(concrete.clone()),
    ));
    RootPodRepositoryPersistenceParts {
        store,
        bound_finalization: concrete.clone(),
        unscheduled_deletion: Arc::new(UnscheduledPodDeletionService::new(concrete)),
    }
}

pub(crate) fn new_store(db: DatastoreHandle) -> PodStore {
    let sandbox_gc_dirty = Arc::new(AtomicUsize::new(1));
    let concrete = concrete_adapter(
        db,
        sandbox_gc_dirty.clone(),
        #[cfg(any(test, feature = "pod-repository-test-support"))]
        None,
    );
    #[cfg(any(test, feature = "pod-repository-test-support"))]
    {
        PodStore::from_persistence_with_watch(
            concrete.clone(),
            concrete.clone(),
            sandbox_gc_dirty,
            Some(concrete),
        )
    }
    #[cfg(not(any(test, feature = "pod-repository-test-support")))]
    PodStore::from_persistence(concrete.clone(), concrete, sandbox_gc_dirty)
}

#[cfg(test)]
mod tests {
    use klights_cluster_core::{StorageCommandRejectionCode, StorageMutationError};

    use super::pod_persistence_error;

    #[test]
    fn neutral_rejections_preserve_pod_repository_categories() {
        let already_exists = pod_persistence_error(
            StorageMutationError::rejected(
                StorageCommandRejectionCode::AlreadyExists,
                "Resource already exists (409 Conflict)",
            )
            .into(),
            "default",
            "duplicate",
        );
        assert!(matches!(
            already_exists,
            klights_pod_api::PodRepositoryError::AlreadyExists { .. }
        ));

        let conflict = pod_persistence_error(
            StorageMutationError::rejected(
                StorageCommandRejectionCode::Conflict,
                "resourceVersion precondition failed (409 Conflict)",
            )
            .into(),
            "default",
            "pod",
        );
        assert!(matches!(
            conflict,
            klights_pod_api::PodRepositoryError::Conflict { .. }
        ));
    }
}
