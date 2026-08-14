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

use klights_kubelet::pod_repository::store::{ActorPodDeleteObservation, PodStore};

struct RootPodRepositoryPersistenceAdapter {
    resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    ownership_reads: Arc<dyn klights_cluster_store::ClusterOwnershipRead>,
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
            self.resource_query
                .get_resource(
                    klights_leader_api::ResourceGetRequest::try_new(
                        klights_types::ResourceKey::new(
                            "v1",
                            "Pod",
                            Some(request.namespace.clone()),
                            request.name.clone(),
                        ),
                        klights_leader_api::ResourceQueryConsistency::LeaderFresh,
                    )
                    .map_err(|error| {
                        pod_persistence_error(
                            anyhow::Error::msg(error.to_string()),
                            &request.namespace,
                            &request.name,
                        )
                    })?,
                )
                .await
                .map_err(|error| {
                    pod_persistence_error(
                        anyhow::Error::msg(error.to_string()),
                        &request.namespace,
                        &request.name,
                    )
                })
        })
    }

    fn list_persisted_pods(
        &self,
        request: PodRepositoryListRequest,
    ) -> PodRepositoryFuture<'_, PodListResult> {
        Box::pin(async move {
            let list = self
                .resource_query
                .list_resources(
                    klights_leader_api::ResourceListRequest::try_new(
                        "v1",
                        "Pod",
                        request
                            .namespace
                            .clone()
                            .map(klights_leader_api::ResourceListScope::Namespace)
                            .unwrap_or(klights_leader_api::ResourceListScope::AllNamespaces),
                        request.label_selector.clone(),
                        request.field_selector.clone(),
                        request.limit,
                        request.continue_token.clone(),
                        klights_leader_api::ResourceQueryConsistency::LeaderFresh,
                    )
                    .map_err(|error| {
                        pod_persistence_error(
                            anyhow::Error::msg(error.to_string()),
                            request.namespace.as_deref().unwrap_or_default(),
                            "Pod list",
                        )
                    })?,
                )
                .await
                .map_err(|error| {
                    pod_persistence_error(
                        anyhow::Error::msg(error.to_string()),
                        request.namespace.as_deref().unwrap_or_default(),
                        "Pod list",
                    )
                })?;
            let resource_version = list.resource_version();
            let continue_token = list.continue_token().map(str::to_owned);
            let remaining_item_count = list.remaining_item_count();
            PodListResult::try_new(
                list.into_items(),
                resource_version,
                continue_token,
                remaining_item_count,
            )
        })
    }

    fn snapshot_persisted_pods(
        &self,
        request: PodSnapshotListRequest,
    ) -> PodRepositoryFuture<'_, PodSnapshotListOutcome> {
        Box::pin(async move {
            let list = &request.list;
            let query = klights_leader_api::ResourceListRequest::try_new(
                "v1",
                "Pod",
                list.namespace()
                    .map(|value| klights_leader_api::ResourceListScope::Namespace(value.to_owned()))
                    .unwrap_or(klights_leader_api::ResourceListScope::AllNamespaces),
                list.label_selector().map(str::to_owned),
                list.field_selector().map(str::to_owned),
                list.limit(),
                list.continue_token().map(str::to_owned),
                klights_leader_api::ResourceQueryConsistency::LeaderFresh,
            )
            .map_err(|error| PodRepositoryError::unavailable(error.to_string()))?
            .with_resource_version_match(
                klights_leader_api::ResourceListResourceVersionMatch::Exact(
                    request.snapshot_resource_version,
                ),
            )
            .map_err(|error| PodRepositoryError::unavailable(error.to_string()))?;
            match self.resource_query.list_resources(query).await {
                Ok(list) => {
                    let resource_version = list.resource_version();
                    let continue_token = list.continue_token().map(str::to_owned);
                    let remaining_item_count = list.remaining_item_count();
                    Ok(PodSnapshotListOutcome::List(PodListResult::try_new(
                        list.into_items(),
                        resource_version,
                        continue_token,
                        remaining_item_count,
                    )?))
                }
                Err(klights_leader_api::ResourceQueryError::NotFound { .. }) => {
                    Ok(PodSnapshotListOutcome::Current)
                }
                Err(error) => Err(PodRepositoryError::unavailable(error.to_string())),
            }
        })
    }

    fn list_persisted_pods_by_owner(
        &self,
        request: PodRepositoryOwnerListRequest,
    ) -> PodRepositoryFuture<'_, Vec<klights_cluster_core::Resource>> {
        Box::pin(async move {
            let owned = self
                .ownership_reads
                .list_resources_by_owner_uid(
                    klights_cluster_store::OwnedKindRequest::try_new(
                        "v1",
                        "Pod",
                        Some(request.namespace.clone()),
                        request.owner_uid.clone(),
                    )
                    .map_err(|error| {
                        pod_persistence_error(
                            anyhow::Error::msg(error.to_string()),
                            &request.namespace,
                            "Pod owner list",
                        )
                    })?,
                )
                .await
                .map_err(|error| {
                    pod_persistence_error(
                        anyhow::Error::msg(error.to_string()),
                        &request.namespace,
                        "Pod owner list",
                    )
                })?;
            Ok(owned)
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
            Err(PodRepositoryError::unavailable(
                "root Pod create requires leader resource commands",
            ))
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
            Err(PodRepositoryError::unavailable(
                "root Pod replace requires leader resource commands",
            ))
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
            Err(PodRepositoryError::unavailable(
                "root Pod patch requires leader resource commands",
            ))
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
            Err(PodRepositoryError::unavailable(
                "root Pod status requires leader resource commands",
            ))
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

impl LocalBoundPodFinalizationPersistence for RootPodRepositoryPersistenceAdapter {
    fn finalize_bound_pod(
        &self,
        request: BoundPodFinalizationRequest,
    ) -> BoundPodFinalizationFuture<'_> {
        Box::pin(async move {
            let identity = request.into_identity();
            let current = self
                .resource_query
                .get_resource(
                    klights_leader_api::ResourceGetRequest::try_new(
                        klights_types::ResourceKey::new(
                            "v1",
                            "Pod",
                            Some(identity.namespace.clone()),
                            identity.name.clone(),
                        ),
                        klights_leader_api::ResourceQueryConsistency::LeaderFresh,
                    )
                    .map_err(|error| BoundPodFinalizationError::unavailable(error.to_string()))?,
                )
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
            Err(BoundPodFinalizationError::unavailable(
                "actor Pod finalization requires leader resource commands",
            ))
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
            let resource = self
                .resource_query
                .get_resource(
                    klights_leader_api::ResourceGetRequest::try_new(
                        klights_types::ResourceKey::new(
                            "v1",
                            "Pod",
                            Some(identity.namespace.clone()),
                            identity.name.clone(),
                        ),
                        klights_leader_api::ResourceQueryConsistency::LeaderFresh,
                    )
                    .map_err(|error| {
                        klights_pod_api::UnscheduledPodDeletionError::unavailable(error.to_string())
                    })?,
                )
                .await
                .map_err(|error| {
                    klights_pod_api::UnscheduledPodDeletionError::unavailable(error.to_string())
                })?;
            let Some(resource) = resource else {
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
            Err(klights_pod_api::UnscheduledPodDeletionError::unavailable(
                "unscheduled Pod deletion requires leader resource commands",
            ))
        })
    }
}

fn concrete_adapter(
    resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    ownership_reads: Arc<dyn klights_cluster_store::ClusterOwnershipRead>,
    commands: Option<Arc<dyn LeaderResourceCommand>>,
    sandbox_gc_dirty: Arc<AtomicUsize>,
    #[cfg(test)] delete_cas_hook: Option<Arc<dyn PodDeleteCasTestHook>>,
) -> Arc<RootPodRepositoryPersistenceAdapter> {
    Arc::new(RootPodRepositoryPersistenceAdapter {
        resource_query,
        ownership_reads,
        commands,
        sandbox_gc_dirty,
        #[cfg(test)]
        delete_cas_hook,
    })
}

pub(crate) fn new_raft_root_parts(
    resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    ownership_reads: Arc<dyn klights_cluster_store::ClusterOwnershipRead>,
    commands: Arc<dyn LeaderResourceCommand>,
    _wall_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
) -> RootPodRepositoryPersistenceParts {
    let sandbox_gc_dirty = Arc::new(AtomicUsize::new(1));
    let concrete = concrete_adapter(
        resource_query,
        ownership_reads,
        Some(commands),
        sandbox_gc_dirty.clone(),
        #[cfg(test)]
        None,
    );
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
pub(crate) fn new_root_parts(
    resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    ownership_reads: Arc<dyn klights_cluster_store::ClusterOwnershipRead>,
    commands: Arc<dyn LeaderResourceCommand>,
    wall_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
) -> RootPodRepositoryPersistenceParts {
    new_raft_root_parts(resource_query, ownership_reads, commands, wall_clock)
}

#[cfg(test)]
pub(crate) fn new_root_parts_with_delete_cas_hook(
    resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    ownership_reads: Arc<dyn klights_cluster_store::ClusterOwnershipRead>,
    commands: Arc<dyn LeaderResourceCommand>,
    delete_cas_hook: Arc<dyn PodDeleteCasTestHook>,
) -> RootPodRepositoryPersistenceParts {
    let sandbox_gc_dirty = Arc::new(AtomicUsize::new(1));
    let concrete = concrete_adapter(
        resource_query,
        ownership_reads,
        Some(commands),
        sandbox_gc_dirty.clone(),
        Some(delete_cas_hook),
    );
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

/// Narrow test-only bridge for fixtures that deliberately exercise the
/// root's historical in-memory handle. Production construction never takes a
/// broad backend; this bridge is kept beside the one consumer type it serves.
#[cfg(test)]
pub(crate) fn new_root_parts_from_test_handle(
    db: crate::datastore::DatastoreHandle,
    wall_clock: Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
) -> RootPodRepositoryPersistenceParts {
    let authority = crate::bootstrap::authority::AuthorityHandle::from(
        crate::bootstrap::composition_adapters::authority_adapter::always_leader_watch(),
    );
    let query = crate::bootstrap::composition_adapters::resource_query_adapter::DatastoreResourceQueryAdapter::new(
        db.clone(), authority.clone(),
    );
    let commands: Arc<dyn LeaderResourceCommand> = Arc::new(
        klights_replication::leader_api::EmbeddedLeaderResourceCommand::new(
            Arc::new(
                crate::bootstrap::outbox_apply_adapter::BackendProposalFixture::new(db.clone()),
            ),
            query.clone(),
            authority.authority_arc(),
        ),
    );
    new_root_parts(
        query,
        Arc::new(TestOwnershipReads { db }),
        commands,
        wall_clock,
    )
}

#[cfg(test)]
pub(crate) fn new_store_from_test_handle(db: crate::datastore::DatastoreHandle) -> PodStore {
    let authority = crate::bootstrap::authority::AuthorityHandle::from(
        crate::bootstrap::composition_adapters::authority_adapter::always_leader_watch(),
    );
    let query = crate::bootstrap::composition_adapters::resource_query_adapter::DatastoreResourceQueryAdapter::new(
        db.clone(), authority.clone(),
    );
    let commands: Arc<dyn LeaderResourceCommand> = Arc::new(
        klights_replication::leader_api::EmbeddedLeaderResourceCommand::new(
            Arc::new(
                crate::bootstrap::outbox_apply_adapter::BackendProposalFixture::new(db.clone()),
            ),
            query.clone(),
            authority.authority_arc(),
        ),
    );
    new_store(query, Arc::new(TestOwnershipReads { db }), commands)
}

#[cfg(test)]
struct TestOwnershipReads {
    db: crate::datastore::DatastoreHandle,
}

#[cfg(test)]
impl klights_cluster_store::ClusterOwnershipRead for TestOwnershipReads {
    fn find_owned_resources(
        &self,
        request: klights_cluster_store::OwnerUidRequest,
    ) -> klights_cluster_store::OwnershipReadFuture<'_, Vec<klights_cluster_core::Resource>> {
        Box::pin(async move {
            self.db
                .find_owned_resources(request.owner_uid(), request.namespace())
                .await
                .map_err(|error| {
                    klights_cluster_store::ResourceReadError::retryable(error.to_string())
                })
        })
    }

    fn list_resources_by_owner_uid(
        &self,
        request: klights_cluster_store::OwnedKindRequest,
    ) -> klights_cluster_store::OwnershipReadFuture<'_, Vec<klights_cluster_core::Resource>> {
        Box::pin(async move {
            self.db
                .list_resources_by_owner_uid(
                    request.api_version(),
                    request.kind(),
                    request.namespace(),
                    request.owner_uid(),
                )
                .await
                .map_err(|error| {
                    klights_cluster_store::ResourceReadError::retryable(error.to_string())
                })
        })
    }

    fn find_owned_by_name_kind_empty_uid(
        &self,
        request: klights_cluster_store::OwnerNameKindRequest,
    ) -> klights_cluster_store::OwnershipReadFuture<'_, Vec<klights_cluster_core::Resource>> {
        Box::pin(async move {
            self.db
                .find_owned_by_name_kind_empty_uid(
                    request.owner_api_version(),
                    request.owner_name(),
                    request.owner_kind(),
                    request.namespace(),
                )
                .await
                .map_err(|error| {
                    klights_cluster_store::ResourceReadError::retryable(error.to_string())
                })
        })
    }
}

#[cfg(test)]
pub(crate) fn new_store(
    resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    ownership_reads: Arc<dyn klights_cluster_store::ClusterOwnershipRead>,
    commands: Arc<dyn LeaderResourceCommand>,
) -> PodStore {
    let sandbox_gc_dirty = Arc::new(AtomicUsize::new(1));
    let concrete = concrete_adapter(
        resource_query,
        ownership_reads,
        Some(commands),
        sandbox_gc_dirty.clone(),
        #[cfg(test)]
        None,
    );
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

    use super::{RootPodRepositoryPersistenceParts, new_raft_root_parts, pod_persistence_error};

    struct RecordingResourceCommands {
        commands: Mutex<Vec<StorageCommand>>,
        disposition: CommandDisposition,
    }

    fn persistence_parts(
        db: &crate::datastore::sqlite::Datastore,
        commands: Arc<dyn LeaderResourceCommand>,
    ) -> RootPodRepositoryPersistenceParts {
        let passive = db.focused_read_store();
        let authority =
            crate::bootstrap::composition_adapters::authority_adapter::always_leader_authority();
        let query = crate::bootstrap::composition_adapters::resource_query_adapter::DatastoreResourceQueryAdapter::new_focused_for_test(
            passive.clone(), authority,
        );
        new_raft_root_parts(
            query,
            passive,
            commands,
            Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
        )
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
        let parts = persistence_parts(&db, commands.clone());

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
        let parts = persistence_parts(&db, commands.clone());

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
            let parts = persistence_parts(&db, commands.clone());

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
