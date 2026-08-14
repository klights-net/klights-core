//! Private concrete persistence adapters for the Pod repository.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use klights_cluster_datastore::diagnostics::{NoopResourceWrite, log_noop_resource_write};
use klights_kubelet::unscheduled_deletion::{
    EligibleUnscheduledPodDeletion, UnscheduledPodDeleteCasOutcome,
    UnscheduledPodDeletionObservation, UnscheduledPodDeletionPort,
    UnscheduledPodDeletionPortFuture, UnscheduledPodDeletionService,
};
use klights_leader_api::{
    LeaderResourceCommand, ResourceCommandError, ResourceCommandRequest, ResourceCommandResult,
};
#[cfg(test)]
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
#[cfg(test)]
use klights_kubelet::pod_repository::store::PodRepositoryWatchPersistence;
use klights_kubelet::pod_repository::store::{ActorPodDeleteObservation, PodStore};

struct RootPodRepositoryPersistenceAdapter {
    db: DatastoreHandle,
    commands: Option<Arc<dyn LeaderResourceCommand>>,
    sandbox_gc_dirty: Arc<AtomicUsize>,
    #[cfg(test)]
    delete_cas_hook: Option<Arc<dyn PodDeleteCasTestHook>>,
}

impl RootPodRepositoryPersistenceAdapter {
    async fn submit_resource(
        &self,
        command: klights_cluster_core::StorageCommand,
        namespace: &str,
        name: &str,
    ) -> Result<klights_cluster_core::Resource, PodRepositoryError> {
        let commands = self.commands.as_ref().ok_or_else(|| {
            PodRepositoryError::unavailable(
                "Raft resource command capability is unavailable for root Pod persistence",
            )
        })?;
        let request = ResourceCommandRequest::try_new(command)
            .map_err(|error| pod_command_error(error, namespace, name))?;
        match commands
            .submit_resource_command(request)
            .await
            .map_err(|error| pod_command_error(error, namespace, name))?
        {
            ResourceCommandResult::Resource(resource) => Ok(resource),
            ResourceCommandResult::Ack { .. } => Err(PodRepositoryError::internal(
                "root Pod persistence command returned no resource",
            )),
        }
    }
}

fn pod_command_error(
    error: ResourceCommandError,
    namespace: &str,
    name: &str,
) -> PodRepositoryError {
    let message = error.to_string();
    match error {
        ResourceCommandError::AlreadyExists { .. } => PodRepositoryError::already_exists(message),
        ResourceCommandError::Conflict { .. } => PodRepositoryError::conflict(message),
        ResourceCommandError::NotFound { .. } => PodRepositoryError::not_found(namespace, name),
        ResourceCommandError::NotLeader
        | ResourceCommandError::Retryable { .. }
        | ResourceCommandError::Timeout
        | ResourceCommandError::Cancelled => PodRepositoryError::unavailable(message),
        _ => PodRepositoryError::internal(message),
    }
}

#[cfg(test)]
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
                    klights_cluster_store::ResourceListOptions::new(
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
                    klights_cluster_store::ResourceListOptions::new(
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
                klights_cluster_store::SnapshotAtRv::List(list) => {
                    PodSnapshotListOutcome::List(PodListResult::try_new(
                        list.items,
                        list.resource_version,
                        list.continue_token,
                        list.remaining_item_count,
                    )?)
                }
                klights_cluster_store::SnapshotAtRv::Current => PodSnapshotListOutcome::Current,
                klights_cluster_store::SnapshotAtRv::Expired => PodSnapshotListOutcome::Expired,
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
            if self.commands.is_some() {
                return self
                    .submit_resource(
                        klights_cluster_core::StorageCommand::CreateResource {
                            api_version: "v1".into(),
                            kind: "Pod".into(),
                            namespace: Some(request.namespace.clone()),
                            name: request.name.clone(),
                            data: request.body,
                        },
                        &request.namespace,
                        &request.name,
                    )
                    .await;
            }
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
            if self.commands.is_some() {
                return self
                    .submit_resource(
                        klights_cluster_core::StorageCommand::UpdateResource {
                            api_version: "v1".into(),
                            kind: "Pod".into(),
                            namespace: Some(request.namespace.clone()),
                            name: request.name.clone(),
                            data: request.body,
                            expected_rv: request.preconditions.resource_version.unwrap_or_default(),
                            preconditions: request.preconditions,
                            preserve_status: false,
                        },
                        &request.namespace,
                        &request.name,
                    )
                    .await;
            }
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
            if self.commands.is_some() {
                return self
                    .submit_resource(
                        klights_cluster_core::StorageCommand::PatchResource {
                            api_version: "v1".into(),
                            kind: "Pod".into(),
                            namespace: Some(request.namespace.clone()),
                            name: request.name.clone(),
                            patch_kind: request.patch_kind,
                            patch: request.patch,
                            strict_resource_version: request
                                .preconditions
                                .resource_version
                                .is_some(),
                            preconditions: request.preconditions,
                        },
                        &request.namespace,
                        &request.name,
                    )
                    .await
                    .map(Some);
            }
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
            if self.commands.is_some() {
                return self
                    .submit_resource(
                        klights_cluster_core::StorageCommand::UpdateStatus {
                            api_version: "v1".into(),
                            kind: "Pod".into(),
                            namespace: Some(request.namespace.clone()),
                            name: request.name.clone(),
                            status: request.status,
                            expected_rv: request.preconditions.resource_version,
                            preconditions: request.preconditions,
                            observed_status_stamp: None,
                        },
                        &request.namespace,
                        &request.name,
                    )
                    .await;
            }
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

#[cfg(test)]
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
            #[cfg(test)]
            if let Some(hook) = &self.delete_cas_hook {
                hook.before_delete_cas(&identity, observed_resource_version)
                    .await
                    .map_err(|error| BoundPodFinalizationError::unavailable(error.to_string()))?;
            }
            let preconditions = klights_cluster_core::ResourcePreconditions {
                uid: Some(identity.uid.clone()),
                resource_version: Some(observed_resource_version),
            };
            if let Some(commands) = &self.commands {
                let command = klights_cluster_core::StorageCommand::DeleteResource {
                    api_version: "v1".to_string(),
                    kind: "Pod".to_string(),
                    namespace: Some(identity.namespace.clone()),
                    name: identity.name.clone(),
                    preconditions,
                };
                let request = ResourceCommandRequest::try_new(command)
                    .map_err(|error| BoundPodFinalizationError::unavailable(error.to_string()))?;
                return match commands.submit_resource_command(request).await {
                    Ok(ResourceCommandResult::Ack { .. }) => {
                        self.sandbox_gc_dirty.fetch_add(1, Ordering::Release);
                        Ok(BoundPodFinalizationOutcome::Removed)
                    }
                    Ok(ResourceCommandResult::Resource(_)) => {
                        Err(BoundPodFinalizationError::unavailable(
                            "Raft actor Pod finalization unexpectedly returned a resource",
                        ))
                    }
                    Err(ResourceCommandError::Conflict { .. }) => {
                        Ok(BoundPodFinalizationOutcome::Retry)
                    }
                    Err(ResourceCommandError::NotFound { .. }) => {
                        Ok(BoundPodFinalizationOutcome::IdentityChanged)
                    }
                    Err(error) => Err(BoundPodFinalizationError::unavailable(error.to_string())),
                };
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

#[cfg(test)]
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
            #[cfg(test)]
            if let Some(hook) = &self.delete_cas_hook {
                hook.before_delete_cas(identity, eligible.observed_resource_version())
                    .await
                    .map_err(|error| {
                        klights_pod_api::UnscheduledPodDeletionError::unavailable(error.to_string())
                    })?;
            }
            if let Some(commands) = &self.commands {
                let command = klights_cluster_core::StorageCommand::DeleteResource {
                    api_version: "v1".to_string(),
                    kind: "Pod".to_string(),
                    namespace: Some(identity.namespace.clone()),
                    name: identity.name.clone(),
                    preconditions,
                };
                let request = ResourceCommandRequest::try_new(command).map_err(|error| {
                    klights_pod_api::UnscheduledPodDeletionError::unavailable(error.to_string())
                })?;
                return match commands.submit_resource_command(request).await {
                    Ok(ResourceCommandResult::Ack { .. }) => {
                        self.sandbox_gc_dirty.fetch_add(1, Ordering::Release);
                        Ok(UnscheduledPodDeleteCasOutcome::Removed)
                    }
                    Ok(ResourceCommandResult::Resource(_)) => {
                        Err(klights_pod_api::UnscheduledPodDeletionError::unavailable(
                            "Raft unscheduled Pod deletion unexpectedly returned a resource",
                        ))
                    }
                    Err(ResourceCommandError::Conflict { .. }) => {
                        Ok(UnscheduledPodDeleteCasOutcome::Conflict)
                    }
                    Err(ResourceCommandError::NotFound { .. }) => {
                        Ok(UnscheduledPodDeleteCasOutcome::Gone)
                    }
                    Err(error) => Err(klights_pod_api::UnscheduledPodDeletionError::unavailable(
                        error.to_string(),
                    )),
                };
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
    commands: Option<Arc<dyn LeaderResourceCommand>>,
    sandbox_gc_dirty: Arc<AtomicUsize>,
    #[cfg(test)] delete_cas_hook: Option<Arc<dyn PodDeleteCasTestHook>>,
) -> Arc<RootPodRepositoryPersistenceAdapter> {
    Arc::new(RootPodRepositoryPersistenceAdapter {
        db,
        commands,
        sandbox_gc_dirty,
        #[cfg(test)]
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
        None,
        sandbox_gc_dirty.clone(),
        #[cfg(test)]
        None,
    );
    #[cfg(test)]
    let store = Arc::new(PodStore::from_persistence_with_watch(
        concrete.clone(),
        concrete.clone(),
        sandbox_gc_dirty,
        Some(concrete.clone()),
    ));
    #[cfg(not(test))]
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

pub(crate) fn new_raft_root_parts(
    db: DatastoreHandle,
    commands: Arc<dyn LeaderResourceCommand>,
    _wall_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
) -> RootPodRepositoryPersistenceParts {
    let sandbox_gc_dirty = Arc::new(AtomicUsize::new(1));
    let concrete = concrete_adapter(
        db,
        Some(commands),
        sandbox_gc_dirty.clone(),
        #[cfg(test)]
        None,
    );
    #[cfg(test)]
    let store = Arc::new(PodStore::from_persistence_with_watch(
        concrete.clone(),
        concrete.clone(),
        sandbox_gc_dirty,
        Some(concrete.clone()),
    ));
    #[cfg(not(test))]
    let store = Arc::new(PodStore::from_persistence(
        concrete.clone(),
        concrete.clone(),
        sandbox_gc_dirty,
    ));
    RootPodRepositoryPersistenceParts {
        store,
        bound_finalization: concrete.clone(),
        unscheduled_deletion: Arc::new(UnscheduledPodDeletionService::new(concrete)),
    }
}

#[cfg(test)]
pub(crate) fn new_root_parts_with_delete_cas_hook(
    db: DatastoreHandle,
    delete_cas_hook: Arc<dyn PodDeleteCasTestHook>,
) -> RootPodRepositoryPersistenceParts {
    let sandbox_gc_dirty = Arc::new(AtomicUsize::new(1));
    let concrete = concrete_adapter(db, None, sandbox_gc_dirty.clone(), Some(delete_cas_hook));
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
        None,
        sandbox_gc_dirty.clone(),
        #[cfg(test)]
        None,
    );
    #[cfg(test)]
    {
        PodStore::from_persistence_with_watch(
            concrete.clone(),
            concrete.clone(),
            sandbox_gc_dirty,
            Some(concrete),
        )
    }
    #[cfg(not(test))]
    PodStore::from_persistence(concrete.clone(), concrete, sandbox_gc_dirty)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use klights_cluster_core::{
        ResourcePreconditions, StorageCommand, StorageCommandRejectionCode, StorageMutationError,
    };
    use klights_leader_api::{
        LeaderResourceCommand, ResourceCommandError, ResourceCommandFuture, ResourceCommandRequest,
        ResourceCommandResult,
    };
    use klights_pod_api::{
        BoundPodFinalizationOutcome, BoundPodFinalizationRequest, UnscheduledPodDeletionOutcome,
        UnscheduledPodDeletionRequest,
    };
    use klights_types::PodIdentity;

    use super::{new_raft_root_parts, pod_persistence_error};

    struct RecordingResourceCommands {
        commands: Mutex<Vec<StorageCommand>>,
        disposition: CommandDisposition,
    }

    #[derive(Clone, Copy)]
    enum CommandDisposition {
        Ack,
        Conflict,
        NotLeader,
    }

    impl LeaderResourceCommand for RecordingResourceCommands {
        fn submit_resource_command(
            &self,
            request: ResourceCommandRequest,
        ) -> ResourceCommandFuture<'_, ResourceCommandResult> {
            Box::pin(async move {
                self.commands.lock().unwrap().push(request.into_command());
                match self.disposition {
                    CommandDisposition::Ack => Ok(ResourceCommandResult::Ack {
                        resource_version: 2,
                    }),
                    CommandDisposition::Conflict => Err(ResourceCommandError::Conflict {
                        message: "resourceVersion precondition failed".to_string(),
                    }),
                    CommandDisposition::NotLeader => Err(ResourceCommandError::NotLeader),
                }
            })
        }
    }

    #[tokio::test]
    async fn raft_root_actor_finalization_submits_uid_bound_delete_without_local_mutation() {
        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();
        let created = db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "ordered-2",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "ordered-2",
                        "uid": "ordered-2-uid",
                        "deletionTimestamp": "2026-08-12T00:00:00Z"
                    },
                    "spec": {
                        "nodeName": "worker-a",
                        "containers": [{"name": "app", "image": "registry.k8s.io/pause:3.10"}]
                    }
                }),
            )
            .await
            .unwrap();
        let commands = Arc::new(RecordingResourceCommands {
            commands: Mutex::new(Vec::new()),
            disposition: CommandDisposition::Ack,
        });
        let parts = new_raft_root_parts(
            Arc::new(db.clone()),
            commands.clone(),
            Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
        );

        let outcome = parts
            .bound_finalization
            .finalize_bound_pod(
                BoundPodFinalizationRequest::try_new(PodIdentity::new(
                    "default",
                    "ordered-2",
                    "ordered-2-uid",
                ))
                .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(outcome, BoundPodFinalizationOutcome::Removed);
        assert_eq!(
            commands.commands.lock().unwrap().as_slice(),
            &[StorageCommand::DeleteResource {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "ordered-2".to_string(),
                preconditions: ResourcePreconditions {
                    uid: Some("ordered-2-uid".to_string()),
                    resource_version: Some(created.resource_version),
                },
            }]
        );
        assert!(
            db.get_resource("v1", "Pod", Some("default"), "ordered-2")
                .await
                .unwrap()
                .is_some(),
            "Raft-root actor finalization must wait for committed apply to remove the Pod"
        );
    }

    #[tokio::test]
    async fn raft_root_unscheduled_delete_submits_exact_cas_without_local_mutation() {
        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();
        let created = db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "unscheduled",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "unscheduled",
                        "uid": "unscheduled-uid",
                        "deletionTimestamp": "2026-08-12T00:00:00Z"
                    },
                    "spec": {
                        "containers": [{"name": "app", "image": "registry.k8s.io/pause:3.10"}]
                    }
                }),
            )
            .await
            .unwrap();
        let commands = Arc::new(RecordingResourceCommands {
            commands: Mutex::new(Vec::new()),
            disposition: CommandDisposition::Ack,
        });
        let parts = new_raft_root_parts(
            Arc::new(db.clone()),
            commands.clone(),
            Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
        );

        let outcome = parts
            .unscheduled_deletion
            .delete_unscheduled_pod(
                UnscheduledPodDeletionRequest::try_new(
                    PodIdentity::new("default", "unscheduled", "unscheduled-uid"),
                    created.resource_version,
                )
                .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(outcome, UnscheduledPodDeletionOutcome::Removed);
        assert_eq!(
            commands.commands.lock().unwrap().as_slice(),
            &[StorageCommand::DeleteResource {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "unscheduled".to_string(),
                preconditions: ResourcePreconditions {
                    uid: Some("unscheduled-uid".to_string()),
                    resource_version: Some(created.resource_version),
                },
            }]
        );
        assert!(
            db.get_resource("v1", "Pod", Some("default"), "unscheduled")
                .await
                .unwrap()
                .is_some(),
            "Raft-root unscheduled deletion must wait for committed apply to remove the Pod"
        );
    }

    #[tokio::test]
    async fn raft_root_unscheduled_delete_conflict_and_rejection_preserve_passive_row() {
        for (disposition, expects_retry) in [
            (CommandDisposition::Conflict, true),
            (CommandDisposition::NotLeader, false),
        ] {
            let db = crate::datastore::sqlite::Datastore::new_in_memory()
                .await
                .unwrap();
            let created = db
                .create_resource(
                    "v1",
                    "Pod",
                    Some("default"),
                    "unscheduled",
                    serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {
                            "namespace": "default",
                            "name": "unscheduled",
                            "uid": "unscheduled-uid",
                            "deletionTimestamp": "2026-08-12T00:00:00Z"
                        },
                        "spec": {
                            "containers": [{"name": "app", "image": "registry.k8s.io/pause:3.10"}]
                        }
                    }),
                )
                .await
                .unwrap();
            let commands = Arc::new(RecordingResourceCommands {
                commands: Mutex::new(Vec::new()),
                disposition,
            });
            let parts = new_raft_root_parts(
                Arc::new(db.clone()),
                commands.clone(),
                Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
            );

            let result = parts
                .unscheduled_deletion
                .delete_unscheduled_pod(
                    UnscheduledPodDeletionRequest::try_new(
                        PodIdentity::new("default", "unscheduled", "unscheduled-uid"),
                        created.resource_version,
                    )
                    .unwrap(),
                )
                .await;

            if expects_retry {
                assert_eq!(result.unwrap(), UnscheduledPodDeletionOutcome::Retry);
            } else {
                assert!(
                    result.is_err(),
                    "authority rejection must remain retryable upstream"
                );
            }
            assert_eq!(commands.commands.lock().unwrap().len(), 1);
            assert!(
                db.get_resource("v1", "Pod", Some("default"), "unscheduled")
                    .await
                    .unwrap()
                    .is_some(),
                "rejected Raft command must preserve the passive Pod row"
            );
        }
    }

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
