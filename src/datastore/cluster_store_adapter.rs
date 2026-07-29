//! Temporary root adapters from the legacy datastore to cluster-store ports.

use klights_cluster_core::{CommittedApplyOutcome, LogApplyCommit, LogApplyMutation};
#[cfg(test)]
use klights_cluster_store::{
    AllocatorStateError, AllocatorStateFuture, AppliedOutboxLookup, ClusterResourceRead,
    CommittedApplyError, CommittedApplyFuture, DurableAllocatorRead, DurableAllocatorState,
    DurableApplyLedgerRead, DurableReplayFloor, DurableReplayTarget, DurableWatchEvent,
    DurableWatchHistoryRead, DurableWatchScope, ResourceCollectionKey, ResourceCollectionScope,
    ResourceContinuation, ResourceGetRequest, ResourceListPage, ResourceListRead,
    ResourceListRequest, ResourceListSnapshot, ResourceReadError, ResourceReadFuture,
    ResourceVersionMatch, WatchHistoryError, WatchHistoryFuture, WatchHistoryPage,
    WatchHistoryRead, WatchHistoryRequest,
};
#[cfg(test)]
use klights_cluster_store::{
    AuthoritativeSnapshot, AuthoritativeSnapshotCapture, AuthoritativeSnapshotPersistence,
    ClusterMetadataFuture, ClusterMetadataRead, ClusterMetadataStoreError,
    PersistedClusterMetadata, SnapshotCaptureHeader, SnapshotCapturePage, SnapshotCapturePageKind,
    SnapshotCaptureSession, SnapshotMembership, SnapshotPersistenceFuture,
};
use klights_cluster_store::{
    CommittedRaftApplyReceipt, CommittedRaftApplyRequest, PrivilegedCommittedRaftApply,
    SnapshotPersistenceError,
};

use super::DatastoreHandle;
#[cfg(test)]
use super::ResourceListQuery;

pub(crate) struct DatastoreRaftCommitMaterializer {
    db: DatastoreHandle,
}

impl DatastoreRaftCommitMaterializer {
    pub(crate) fn new(db: DatastoreHandle) -> Self {
        Self { db }
    }
}

fn map_storage_mutation_error(error: anyhow::Error) -> klights_cluster_core::StorageMutationError {
    use klights_cluster_core::{StorageCommandRejectionCode, StorageMutationError};
    use klights_cluster_datastore::errors::DatastoreError;

    let diagnostic = format!("{error:#}");
    let concrete_code = error.chain().find_map(|source| {
        source
            .downcast_ref::<DatastoreError>()
            .map(|error| match error {
                DatastoreError::AlreadyExists { .. } => StorageCommandRejectionCode::AlreadyExists,
                DatastoreError::NotFound { .. } => StorageCommandRejectionCode::NotFound,
                DatastoreError::Conflict { .. } => StorageCommandRejectionCode::Conflict,
            })
    });
    let lower = diagnostic.to_ascii_lowercase();
    let rejection_code = concrete_code.or_else(|| {
        if lower.contains("already exists") && lower.contains("409 conflict") {
            Some(StorageCommandRejectionCode::AlreadyExists)
        } else if lower.contains("409 conflict")
            || lower.contains("version conflict")
            || lower.contains("rv conflict")
        {
            Some(StorageCommandRejectionCode::Conflict)
        } else if lower.contains("not found") {
            Some(StorageCommandRejectionCode::NotFound)
        } else {
            None
        }
    });

    match rejection_code {
        Some(code) => StorageMutationError::rejected(code, diagnostic),
        None => StorageMutationError::persistence(diagnostic),
    }
}

#[async_trait::async_trait]
impl crate::datastore::raft::node::RaftCommitMaterializer for DatastoreRaftCommitMaterializer {
    async fn read_raft_metadata(
        &self,
        key: &str,
    ) -> Result<Option<String>, klights_cluster_core::StorageMutationError> {
        self.db.get_klights_meta(key).await.map_err(|error| {
            klights_cluster_core::StorageMutationError::persistence(error.to_string())
        })
    }

    async fn build_command(
        &self,
        command: klights_cluster_core::StorageCommand,
        operation: &str,
        authoring_node: &str,
    ) -> Result<klights_cluster_core::LogApplyCommit, klights_cluster_core::StorageMutationError>
    {
        self.db
            .build_log_apply_commit_for_command(command, operation, authoring_node)
            .await
            .map_err(map_storage_mutation_error)
    }

    async fn build_outbox(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: klights_cluster_core::StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> Result<klights_cluster_core::BuildOutboxOutcome, klights_cluster_core::OutboxApplyError>
    {
        self.db
            .build_log_apply_commit_for_outbox_with_watermark(
                idempotency_key,
                operation,
                command,
                authoring_node,
                watermark,
            )
            .await
    }
}

#[cfg(test)]
pub(crate) fn raft_state_machine_store_ports_for_test(
    db: DatastoreHandle,
) -> crate::datastore::raft::state_machine_impl::RaftStateMachineStorePorts {
    let materializer = std::sync::Arc::new(DatastoreRaftCommitMaterializer::new(db.clone()));
    let persistence: std::sync::Arc<dyn PrivilegedCommittedRaftApply> =
        std::sync::Arc::new(LegacyTestCommittedRaftApply::new_for_test(db.clone()));
    crate::datastore::raft::state_machine_impl::RaftStateMachineStorePorts::new(
        std::sync::Arc::new(ObservedCommittedRaftApply::new_for_test(persistence)),
        std::sync::Arc::new(LegacyTestSnapshotPersistence::new_for_test(db.clone())),
        std::sync::Arc::new(crate::datastore::DatastoreDurableRecoveryPort::new(
            db.clone(),
        )),
        std::sync::Arc::new(crate::datastore::DatastoreBackendLifecyclePort::new(db)),
        materializer,
    )
}

#[cfg(test)]
pub(crate) fn raft_store_ports_for_test(
    db: DatastoreHandle,
) -> crate::datastore::raft::node::RaftStorePorts {
    let materializer = std::sync::Arc::new(DatastoreRaftCommitMaterializer::new(db.clone()));
    crate::datastore::raft::node::RaftStorePorts::new(
        materializer,
        raft_state_machine_store_ports_for_test(db),
    )
}

/// Read-only capability wrapper over the legacy umbrella datastore handle.
///
/// REMOVE(Phase 10): concrete cluster datastore adapters implement the
/// cluster-store ports directly after physical extraction.
#[cfg(test)]
pub(crate) struct LegacyTestClusterResourceRead {
    db: DatastoreHandle,
}

#[cfg(test)]
impl LegacyTestClusterResourceRead {
    pub(crate) fn new(db: DatastoreHandle) -> Self {
        Self { db }
    }
}

// Packet 6A defines the adapter before a later consumer migration. Keep its
// complete construction boundary checked without runtime wiring or lint
// suppression.
#[cfg(test)]
const _: fn(DatastoreHandle) -> LegacyTestClusterResourceRead = LegacyTestClusterResourceRead::new;

#[cfg(test)]
impl ClusterResourceRead for LegacyTestClusterResourceRead {
    fn get_resource(
        &self,
        request: ResourceGetRequest,
    ) -> ResourceReadFuture<'_, Option<klights_cluster_core::Resource>> {
        Box::pin(async move {
            let key = request.key();
            self.db
                .get_resource(
                    &key.api_version,
                    &key.kind,
                    key.namespace.as_deref(),
                    &key.name,
                )
                .await
                .map_err(map_query_error)
        })
    }

    fn list_resources(
        &self,
        request: ResourceListRequest,
    ) -> ResourceReadFuture<'_, ResourceListRead> {
        Box::pin(async move {
            let query = request.query();
            let namespace = match request.scope() {
                ResourceCollectionScope::Cluster | ResourceCollectionScope::AllNamespaces => None,
                ResourceCollectionScope::Namespace(namespace) => Some(namespace.as_str()),
            };
            let all_namespaces = matches!(request.scope(), ResourceCollectionScope::AllNamespaces);
            let root_query = ResourceListQuery::new(
                query.label_selector(),
                query.field_selector(),
                (!all_namespaces).then_some(query.limit()).flatten(),
                (!all_namespaces)
                    .then(|| query.continuation().map(|cursor| cursor.after().name()))
                    .flatten(),
            );
            let continuation_position = query
                .continuation()
                .map(|cursor| cursor.snapshot().position());
            let requested_position = match query.resource_version_match() {
                ResourceVersionMatch::AtPosition(position) => Some(position),
                _ => continuation_position,
            };
            let read = if let Some(position) = requested_position {
                let target = match request.scope() {
                    ResourceCollectionScope::Cluster => crate::datastore::WatchTarget::cluster(
                        request.api_version(),
                        request.kind(),
                    ),
                    ResourceCollectionScope::AllNamespaces => {
                        crate::datastore::WatchTarget::namespaced(
                            request.api_version(),
                            request.kind(),
                        )
                    }
                    ResourceCollectionScope::Namespace(namespace) => {
                        crate::datastore::WatchTarget::namespaced_in_namespace(
                            request.api_version(),
                            request.kind(),
                            namespace,
                        )
                    }
                };
                self.db
                    .snapshot_resources_at_position(
                        &[target],
                        query.label_selector(),
                        query.field_selector(),
                        position,
                    )
                    .await
                    .map_err(map_query_error)?
            } else if let ResourceVersionMatch::Exact(rv) = query.resource_version_match() {
                self.db
                    .snapshot_resources_at_rv(
                        request.api_version(),
                        request.kind(),
                        namespace,
                        root_query,
                        rv,
                    )
                    .await
                    .map_err(map_query_error)?
            } else {
                crate::datastore::SnapshotAtRv::Current
            };

            match read {
                crate::datastore::SnapshotAtRv::Expired => {
                    let requested = requested_position.map_or_else(
                        || match query.resource_version_match() {
                            ResourceVersionMatch::Exact(rv)
                            | ResourceVersionMatch::NotOlderThan(rv) => rv,
                            ResourceVersionMatch::AtPosition(position) => position.resource_version,
                            ResourceVersionMatch::Any => 0,
                        },
                        |position| position.resource_version,
                    );
                    let oldest_available = self
                        .db
                        .earliest_watch_event_rv()
                        .await
                        .map_err(map_query_error)?
                        .unwrap_or(requested.saturating_add(1));
                    Ok(ResourceListRead::Expired {
                        requested,
                        oldest_available,
                    })
                }
                crate::datastore::SnapshotAtRv::List(mut page) => {
                    if all_namespaces || query.continuation().is_some() {
                        normalize_collection_page(&mut page, query.continuation(), all_namespaces);
                    }
                    let page = port_page(
                        page,
                        query.continuation().map(|cursor| cursor.snapshot()),
                        query.limit(),
                    )?;
                    Ok(ResourceListRead::Historical(page))
                }
                crate::datastore::SnapshotAtRv::Current => {
                    if requested_position.is_some() {
                        return Err(ResourceReadError::CorruptData {
                            message:
                                "positioned datastore LIST returned an unpinned Current sentinel"
                                    .to_string(),
                        });
                    }
                    let mut page = self
                        .db
                        .list_resources(
                            request.api_version(),
                            request.kind(),
                            namespace,
                            root_query,
                        )
                        .await
                        .map_err(map_query_error)?;
                    if let ResourceVersionMatch::NotOlderThan(requested) =
                        query.resource_version_match()
                        && page.resource_version < requested
                    {
                        return Err(ResourceReadError::Conflict {
                            message: format!(
                                "current resourceVersion {} is older than requested {requested}",
                                page.resource_version
                            ),
                        });
                    }
                    if all_namespaces {
                        normalize_collection_page(&mut page, query.continuation(), true);
                    }
                    Ok(ResourceListRead::Current(port_page(
                        page,
                        None,
                        query.limit(),
                    )?))
                }
            }
        })
    }
}

#[cfg(test)]
fn normalize_collection_page(
    page: &mut crate::datastore::ResourceList,
    continuation: Option<&ResourceContinuation>,
    all_namespaces: bool,
) {
    if all_namespaces {
        page.items.sort_by(|left, right| {
            left.namespace
                .as_deref()
                .cmp(&right.namespace.as_deref())
                .then_with(|| left.name.cmp(&right.name))
        });
    } else {
        page.items.sort_by(|left, right| left.name.cmp(&right.name));
    }
    if let Some(cursor) = continuation {
        if all_namespaces {
            page.items.retain(|item| {
                (item.namespace.as_deref(), item.name.as_str())
                    > (cursor.after().namespace(), cursor.after().name())
            });
        } else {
            page.items
                .retain(|item| item.name.as_str() > cursor.after().name());
        }
    }
    page.continue_token = None;
    page.remaining_item_count = None;
}

#[cfg(test)]
fn map_query_error(error: anyhow::Error) -> ResourceReadError {
    let message = format!("{error:#}");
    let lower = message.to_ascii_lowercase();
    if klights_cluster_datastore::errors::is_conflict_error(&error) {
        ResourceReadError::Conflict { message }
    } else if lower.contains("selector") {
        ResourceReadError::InvalidSelector { message }
    } else if lower.contains("continue") || lower.contains("cursor") {
        ResourceReadError::InvalidContinuation { message }
    } else if lower.contains("unsupported") || lower.contains("does not implement") {
        ResourceReadError::UnsupportedMode { message }
    } else if lower.contains("corrupt")
        || lower.contains("decode")
        || lower.contains("invalid data")
    {
        ResourceReadError::CorruptData { message }
    } else if lower.contains("cancel") {
        ResourceReadError::Cancelled
    } else if lower.contains("timeout") || lower.contains("timed out") {
        ResourceReadError::Timeout
    } else {
        ResourceReadError::retryable(message)
    }
}

#[cfg(test)]
fn port_page(
    mut page: crate::datastore::ResourceList,
    pinned: Option<ResourceListSnapshot>,
    limit: Option<i64>,
) -> Result<ResourceListPage, ResourceReadError> {
    let position = match (pinned, page.watch_replay_position) {
        (Some(snapshot), _) => snapshot,
        (None, Some(position)) => ResourceListSnapshot::try_new(position)?,
        (None, None) => ResourceListSnapshot::try_new(klights_cluster_core::WatchReplayPosition {
            resource_version: page.resource_version,
            event_id: 0,
            resource_version_filter_through_event_id: 0,
        })?,
    };
    let has_more =
        limit.is_some_and(|limit| i64::try_from(page.items.len()).unwrap_or(i64::MAX) > limit);
    if let Some(limit) = limit.and_then(|limit| usize::try_from(limit).ok())
        && page.items.len() > limit
    {
        page.items.truncate(limit);
    }
    let continuation = if has_more || page.continue_token.is_some() {
        page.items.last().map(|item| {
            ResourceContinuation::new(
                ResourceCollectionKey::new(item.namespace.clone(), item.name.clone()),
                position,
            )
        })
    } else {
        None
    };
    ResourceListPage::try_new(
        page.items,
        position,
        continuation,
        page.remaining_item_count,
    )
}

/// Privileged committed-apply wrapper over the legacy umbrella datastore.
///
/// Construction is crate-private and no normal API/controller command path
/// receives this type. The separate ledger-read implementation does not grant
/// apply rights when injected on its own as a trait object.
///
/// REMOVE(Phase 10): concrete cluster datastore adapters implement the
/// cluster-store ports directly after physical extraction.
#[cfg(test)]
pub(crate) struct LegacyTestCommittedRaftApply {
    db: DatastoreHandle,
}

#[cfg(test)]
impl LegacyTestCommittedRaftApply {
    fn new_for_test(db: DatastoreHandle) -> Self {
        Self { db }
    }
}

#[cfg(test)]
impl PrivilegedCommittedRaftApply for LegacyTestCommittedRaftApply {
    fn apply_committed_raft(
        &self,
        request: CommittedRaftApplyRequest,
    ) -> CommittedApplyFuture<'_, CommittedRaftApplyReceipt> {
        Box::pin(async move {
            self.db
                .apply_raft_log_apply_commit_receipt(request.into_commit())
                .await
                .map_err(map_committed_apply_error)
        })
    }
}

/// Root decorator that projects the canonical persistence receipt into the
/// OpenRaft response and publishes active post-commit wakeups.
pub(crate) struct ObservedCommittedRaftApply {
    persistence: std::sync::Arc<dyn PrivilegedCommittedRaftApply>,
    wakeups: Option<std::sync::Arc<dyn klights_leader_api::PostCommitWakeup>>,
}

impl ObservedCommittedRaftApply {
    pub(crate) fn new(
        persistence: std::sync::Arc<dyn PrivilegedCommittedRaftApply>,
        wakeups: std::sync::Arc<dyn klights_leader_api::PostCommitWakeup>,
    ) -> Self {
        Self {
            persistence,
            wakeups: Some(wakeups),
        }
    }

    #[cfg(test)]
    fn new_for_test(persistence: std::sync::Arc<dyn PrivilegedCommittedRaftApply>) -> Self {
        Self {
            persistence,
            wakeups: None,
        }
    }

    fn publish_visible_commit(&self, commit: &LogApplyCommit, resource_version: i64) {
        let Some(wakeups) = &self.wakeups else {
            return;
        };
        let advances = post_commit_advances(commit, resource_version);
        wakeups.wake(&advances);
        for mutation in commit.mutations() {
            if let LogApplyMutation::DeleteNamespaceContents { name } = mutation {
                wakeups.wake_namespace_contents(name, resource_version);
            }
        }
    }
}

#[async_trait::async_trait]
impl crate::datastore::raft::state_machine_impl::RaftCommittedApply for ObservedCommittedRaftApply {
    async fn apply_committed(
        &self,
        request: CommittedRaftApplyRequest,
    ) -> Result<
        crate::datastore::raft::types::StorageCommandResult,
        klights_cluster_store::CommittedApplyError,
    > {
        let commit = request.commit().clone();
        let receipt = self.persistence.apply_committed_raft(request).await?;
        if let CommittedApplyOutcome::Visible {
            resource_version, ..
        } = receipt.outcome()
        {
            self.publish_visible_commit(&commit, *resource_version);
        }
        Ok(storage_command_result_from_committed_outcome(&receipt))
    }
}

pub(crate) fn storage_command_result_from_committed_outcome(
    receipt: &CommittedRaftApplyReceipt,
) -> crate::datastore::raft::types::StorageCommandResult {
    let applied_mutation = receipt
        .applied_resource()
        .cloned()
        .map(crate::datastore::raft::types::AppliedMutation::Resource);
    match receipt.outcome() {
        CommittedApplyOutcome::Visible {
            resource_version, ..
        } => crate::datastore::raft::types::StorageCommandResult {
            applied_rv: Some(*resource_version),
            error_message: None,
            rejection_code: None,
            public_resource_changed: true,
            applied_mutation,
            pod_endpoint_effect: receipt.pod_endpoint_effect(),
        },
        CommittedApplyOutcome::NoPublicChange {
            resource_version, ..
        } => crate::datastore::raft::types::StorageCommandResult {
            applied_rv: Some(*resource_version),
            error_message: None,
            rejection_code: None,
            public_resource_changed: false,
            applied_mutation,
            pod_endpoint_effect: receipt.pod_endpoint_effect(),
        },
        CommittedApplyOutcome::Rejected(rejection) => {
            use klights_cluster_core::{CommittedApplyRejection, StorageCommandRejectionCode};
            let rejection_code = match rejection {
                CommittedApplyRejection::AlreadyExists { .. } => {
                    StorageCommandRejectionCode::AlreadyExists
                }
                CommittedApplyRejection::NotFound { .. } => StorageCommandRejectionCode::NotFound,
                CommittedApplyRejection::UidConflict { .. }
                | CommittedApplyRejection::ResourceVersionConflict { .. } => {
                    StorageCommandRejectionCode::Conflict
                }
                CommittedApplyRejection::InvalidCommit { .. } => {
                    StorageCommandRejectionCode::InvalidCommit
                }
                _ => StorageCommandRejectionCode::InvalidCommit,
            };
            crate::datastore::raft::types::StorageCommandResult {
                applied_rv: None,
                error_message: Some(rejection.message().to_string()),
                rejection_code: Some(rejection_code),
                public_resource_changed: false,
                applied_mutation: None,
                pod_endpoint_effect: receipt.pod_endpoint_effect(),
            }
        }
        _ => crate::datastore::raft::types::StorageCommandResult {
            applied_rv: None,
            error_message: Some("unsupported canonical committed-apply outcome".to_string()),
            rejection_code: Some(klights_cluster_core::StorageCommandRejectionCode::InvalidCommit),
            public_resource_changed: false,
            applied_mutation: None,
            pod_endpoint_effect: receipt.pod_endpoint_effect(),
        },
    }
}

impl super::sqlite::Datastore {
    pub async fn apply_raft_log_apply_commit(
        &self,
        commit: LogApplyCommit,
    ) -> anyhow::Result<crate::datastore::raft::types::StorageCommandResult> {
        let receipt = self.apply_raft_log_apply_commit_receipt(commit).await?;
        Ok(storage_command_result_from_committed_outcome(&receipt))
    }
}

fn post_commit_advances(
    commit: &LogApplyCommit,
    resource_version: i64,
) -> Vec<klights_leader_api::PostCommitAdvance> {
    commit
        .mutations()
        .iter()
        .filter_map(|mutation| {
            let (api_version, kind, namespace) = match mutation {
                LogApplyMutation::PutResource(row) => {
                    (&row.api_version, &row.kind, row.namespace.clone())
                }
                LogApplyMutation::PatchResourceLatest(row) => {
                    (&row.api_version, &row.kind, row.namespace.clone())
                }
                LogApplyMutation::DeleteResource(row) => {
                    (&row.api_version, &row.kind, row.namespace.clone())
                }
                LogApplyMutation::FinalizeBoundPod(row) => {
                    return Some(klights_leader_api::PostCommitAdvance::new(
                        "v1",
                        "Pod",
                        Some(row.namespace.clone()),
                        resource_version,
                    ));
                }
                LogApplyMutation::PutNamespace(_) | LogApplyMutation::DeleteNamespace { .. } => {
                    return Some(klights_leader_api::PostCommitAdvance::new(
                        "v1",
                        "Namespace",
                        None,
                        resource_version,
                    ));
                }
                LogApplyMutation::PutWatchEvent(row) => {
                    (&row.api_version, &row.kind, row.namespace.clone())
                }
                _ => return None,
            };
            Some(klights_leader_api::PostCommitAdvance::new(
                api_version,
                kind,
                namespace,
                resource_version,
            ))
        })
        .collect()
}

#[cfg(test)]
impl DurableApplyLedgerRead for LegacyTestCommittedRaftApply {
    fn current_apply_position(
        &self,
    ) -> CommittedApplyFuture<'_, klights_cluster_core::WatchReplayPosition> {
        Box::pin(async move {
            self.db
                .current_watch_replay_position()
                .await
                .map_err(map_committed_apply_error)
        })
    }

    fn get_applied_outbox(
        &self,
        lookup: AppliedOutboxLookup,
    ) -> CommittedApplyFuture<'_, Option<klights_cluster_core::LogApplyAppliedOutboxRow>> {
        Box::pin(async move {
            self.db
                .get_applied_outbox(lookup.idempotency_key())
                .await
                .map_err(map_committed_apply_error)
        })
    }

    fn list_outbox_watermarks(
        &self,
    ) -> CommittedApplyFuture<'_, Vec<klights_cluster_core::OutboxStreamWatermark>> {
        Box::pin(async move {
            self.db
                .list_outbox_stream_watermarks()
                .await
                .map_err(map_committed_apply_error)
        })
    }
}

#[cfg(test)]
fn map_committed_apply_error(error: anyhow::Error) -> CommittedApplyError {
    let message = format!("{error:#}");
    let lower = message.to_ascii_lowercase();
    if lower.contains("unsupported")
        || lower.contains("does not support")
        || lower.contains("does not implement")
    {
        CommittedApplyError::UnsupportedMode { message }
    } else if lower.contains("corrupt")
        || lower.contains("decode")
        || lower.contains("invalid data")
    {
        CommittedApplyError::CorruptData { message }
    } else if lower.contains("cancel") {
        CommittedApplyError::Cancelled
    } else if lower.contains("timeout") || lower.contains("timed out") {
        CommittedApplyError::Timeout
    } else if lower.contains("busy") || lower.contains("locked") {
        CommittedApplyError::Retryable { message }
    } else {
        CommittedApplyError::persistence_failed(message)
    }
}

/// Read-only durable-history wrapper over the legacy umbrella datastore.
///
/// This type owns no live watch receiver or broadcast capability.
/// REMOVE(Phase 10): concrete cluster datastore adapters implement the
/// cluster-store ports directly after physical extraction.
#[cfg(test)]
pub(crate) struct LegacyTestDurableWatchHistory {
    db: DatastoreHandle,
}

#[cfg(test)]
impl LegacyTestDurableWatchHistory {
    pub(crate) fn new(db: DatastoreHandle) -> Self {
        Self { db }
    }
}

#[cfg(test)]
const _: fn(DatastoreHandle) -> LegacyTestDurableWatchHistory = LegacyTestDurableWatchHistory::new;

#[cfg(test)]
impl DurableWatchHistoryRead for LegacyTestDurableWatchHistory {
    fn replay_watch_history(
        &self,
        request: WatchHistoryRequest,
    ) -> WatchHistoryFuture<'_, WatchHistoryRead> {
        Box::pin(async move {
            let targets = request
                .targets()
                .iter()
                .map(|target| match target.scope() {
                    DurableWatchScope::Cluster => {
                        crate::datastore::WatchTarget::cluster(target.api_version(), target.kind())
                    }
                    DurableWatchScope::Namespaced(None) => {
                        crate::datastore::WatchTarget::namespaced(
                            target.api_version(),
                            target.kind(),
                        )
                    }
                    DurableWatchScope::Namespaced(Some(namespace)) => {
                        crate::datastore::WatchTarget::namespaced_in_namespace(
                            target.api_version(),
                            target.kind(),
                            namespace,
                        )
                    }
                })
                .collect::<Vec<_>>();
            match self
                .db
                .list_watch_events_after_position_checked_bounded(
                    &targets,
                    request.position(),
                    request.limit(),
                )
                .await
                .map_err(map_watch_history_error)?
            {
                crate::datastore::PositionedWatchReplayRead::Events(page) => {
                    let events = page
                        .events
                        .into_iter()
                        .map(|event| klights_cluster_core::PositionedWatchEvent {
                            position: event.position,
                            event: DurableWatchEvent::new(
                                event.event.event_type,
                                event.event.resource,
                            ),
                        })
                        .collect();
                    Ok(WatchHistoryRead::Events(WatchHistoryPage::try_new(
                        events,
                        page.next_position,
                    )?))
                }
                crate::datastore::PositionedWatchReplayRead::Expired => {
                    Ok(WatchHistoryRead::Expired)
                }
            }
        })
    }

    fn list_replay_floors(&self) -> WatchHistoryFuture<'_, Vec<DurableReplayFloor>> {
        Box::pin(async move {
            self.db
                .list_watch_replay_floors()
                .await
                .map_err(map_watch_history_error)?
                .into_iter()
                .map(|floor| {
                    let target = match (floor.api_version, floor.kind, floor.namespace_key) {
                        (api_version, kind, namespace)
                            if api_version == "*" && kind == "*" && namespace == "*" =>
                        {
                            DurableReplayTarget::All
                        }
                        (api_version, kind, namespace) if namespace == "#cluster" => {
                            DurableReplayTarget::Cluster { api_version, kind }
                        }
                        (api_version, kind, namespace) => DurableReplayTarget::Namespaced {
                            api_version,
                            kind,
                            namespace,
                        },
                    };
                    DurableReplayFloor::new(
                        target,
                        floor.floor_resource_version,
                        floor.floor_event_id,
                        floor.position_is_exact,
                    )
                })
                .collect()
        })
    }
}

#[cfg(test)]
fn map_watch_history_error(error: anyhow::Error) -> WatchHistoryError {
    let message = format!("{error:#}");
    let lower = message.to_ascii_lowercase();
    if lower.contains("corrupt") || lower.contains("decode") || lower.contains("invalid") {
        WatchHistoryError::CorruptData { message }
    } else if lower.contains("unsupported")
        || lower.contains("does not support")
        || lower.contains("does not implement")
    {
        WatchHistoryError::UnsupportedMode { message }
    } else if lower.contains("cancel") {
        WatchHistoryError::Cancelled
    } else if lower.contains("timeout") || lower.contains("timed out") {
        WatchHistoryError::Timeout
    } else if lower.contains("busy") || lower.contains("locked") {
        WatchHistoryError::Retryable { message }
    } else {
        WatchHistoryError::persistence_failed(message)
    }
}

/// Read-only durable allocator wrapper over the legacy umbrella datastore.
/// REMOVE(Phase 10): concrete cluster datastore adapters implement the
/// cluster-store ports directly after physical extraction.
#[cfg(test)]
pub(crate) struct LegacyTestDurableAllocatorRead {
    db: DatastoreHandle,
}

#[cfg(test)]
impl LegacyTestDurableAllocatorRead {
    pub(crate) fn new(db: DatastoreHandle) -> Self {
        Self { db }
    }
}

#[cfg(test)]
const _: fn(DatastoreHandle) -> LegacyTestDurableAllocatorRead =
    LegacyTestDurableAllocatorRead::new;

#[cfg(test)]
impl DurableAllocatorRead for LegacyTestDurableAllocatorRead {
    fn read_allocator_state(&self) -> AllocatorStateFuture<'_, DurableAllocatorState> {
        Box::pin(async move {
            let observation = self
                .db
                .read_durable_allocator_observation()
                .await
                .map_err(map_allocator_error)?;
            DurableAllocatorState::try_new(observation.position)
        })
    }
}

#[cfg(test)]
fn map_allocator_error(error: anyhow::Error) -> AllocatorStateError {
    let message = format!("{error:#}");
    let lower = message.to_ascii_lowercase();
    if lower.contains("invalid") || lower.contains("malformed") || lower.contains("unknown") {
        AllocatorStateError::CorruptData { message }
    } else if lower.contains("unsupported")
        || lower.contains("does not support")
        || lower.contains("does not implement")
    {
        AllocatorStateError::UnsupportedMode { message }
    } else if lower.contains("cancel") {
        AllocatorStateError::Cancelled
    } else if lower.contains("timeout") || lower.contains("timed out") {
        AllocatorStateError::Timeout
    } else if lower.contains("busy") || lower.contains("locked") {
        AllocatorStateError::Retryable { message }
    } else {
        AllocatorStateError::persistence_failed(message)
    }
}

/// Root-only OpenRaft envelope adapter over the SQLite recovery port.
pub(crate) struct SqliteRaftSnapshotRestore {
    recovery: std::sync::Arc<klights_cluster_datastore::sqlite::recovery::SqliteRecoveryStore>,
}

impl SqliteRaftSnapshotRestore {
    pub(crate) fn new(
        recovery: std::sync::Arc<klights_cluster_datastore::sqlite::recovery::SqliteRecoveryStore>,
        _authority: crate::datastore::raft::SnapshotInstallAuthority,
    ) -> Self {
        Self { recovery }
    }
}

#[async_trait::async_trait]
impl crate::datastore::raft::state_machine_impl::RaftSnapshotRestore for SqliteRaftSnapshotRestore {
    async fn restore_snapshot(
        &self,
        data: crate::datastore::raft::snapshot::RaftSnapshotData,
    ) -> Result<(), SnapshotPersistenceError> {
        let metadata = data.cluster_metadata;
        let membership = match data.cluster_membership {
            None => klights_cluster_datastore::sqlite::recovery::SnapshotMembership::LegacyOmitted,
            Some(crate::datastore::raft::snapshot::RaftSnapshotMembership::AuthoritativeAbsent) => {
                klights_cluster_datastore::sqlite::recovery::SnapshotMembership::AuthoritativeAbsent
            }
            Some(crate::datastore::raft::snapshot::RaftSnapshotMembership::Present(value)) => {
                klights_cluster_datastore::sqlite::recovery::SnapshotMembership::Present(value)
            }
        };
        self.recovery
            .restore_snapshot_parts(
                data.operations,
                data.current_rv,
                data.watch_event_high_water,
                data.watch_replay_floors.map(|floors| {
                    floors
                        .into_iter()
                        .map(|floor| {
                            klights_cluster_datastore::sqlite::recovery::SnapshotReplayFloor {
                                api_version: floor.api_version,
                                kind: floor.kind,
                                namespace_key: floor.namespace_key,
                                floor_resource_version: floor.floor_resource_version,
                                floor_event_id: floor.floor_event_id,
                                position_is_exact: floor.position_is_exact,
                            }
                        })
                        .collect()
                }),
                Some(
                    klights_cluster_datastore::sqlite::recovery::SnapshotMetadata {
                        cluster_id: metadata
                            .as_ref()
                            .map(|metadata| metadata.cluster_id.clone())
                            .unwrap_or_default(),
                        leader_epoch: metadata
                            .as_ref()
                            .map_or(0, |metadata| metadata.leader_epoch),
                        membership,
                        command_codec_activation_version: data.command_codec_activation_version,
                    },
                ),
            )
            .await
    }
}

/// Generic legacy persistence adapter retained only for root mock tests.
#[cfg(test)]
pub(crate) struct LegacyTestSnapshotPersistence {
    db: DatastoreHandle,
    recovery: std::sync::Arc<dyn crate::datastore::DurableRecoveryStore>,
    lifecycle: std::sync::Arc<dyn crate::datastore::BackendLifecycleStore>,
}

#[cfg(test)]
impl LegacyTestSnapshotPersistence {
    fn new_for_test(db: DatastoreHandle) -> Self {
        Self::from_handle(db)
    }

    fn from_handle(db: DatastoreHandle) -> Self {
        let recovery = std::sync::Arc::new(crate::datastore::DatastoreDurableRecoveryPort::new(
            db.clone(),
        ));
        let lifecycle = std::sync::Arc::new(crate::datastore::DatastoreBackendLifecyclePort::new(
            db.clone(),
        ));
        Self {
            db,
            recovery,
            lifecycle,
        }
    }

    pub(crate) async fn restore_authoritative_raft_snapshot(
        &self,
        data: crate::datastore::raft::snapshot::RaftSnapshotData,
    ) -> anyhow::Result<()> {
        let metadata = data.cluster_metadata;
        let membership = match data.cluster_membership {
            None => crate::datastore::ReplicatedMembershipState::LegacyOmitted,
            Some(crate::datastore::raft::snapshot::RaftSnapshotMembership::AuthoritativeAbsent) => {
                crate::datastore::ReplicatedMembershipState::AuthoritativeAbsent
            }
            Some(crate::datastore::raft::snapshot::RaftSnapshotMembership::Present(value)) => {
                crate::datastore::ReplicatedMembershipState::Present(value)
            }
        };
        self.db
            .replace_replicated_resource_state(
                data.operations,
                data.current_rv,
                data.watch_event_high_water,
                data.watch_replay_floors,
                Some(crate::datastore::ReplicatedSnapshotMetadata {
                    cluster_id: metadata
                        .as_ref()
                        .map(|metadata| metadata.cluster_id.clone())
                        .unwrap_or_default(),
                    leader_epoch: metadata
                        .as_ref()
                        .map_or(0, |metadata| metadata.leader_epoch),
                    membership,
                    command_codec_activation_version: data.command_codec_activation_version,
                }),
            )
            .await
    }
}

#[async_trait::async_trait]
#[cfg(test)]
impl crate::datastore::raft::state_machine_impl::RaftSnapshotRestore
    for LegacyTestSnapshotPersistence
{
    async fn restore_snapshot(
        &self,
        data: crate::datastore::raft::snapshot::RaftSnapshotData,
    ) -> Result<(), SnapshotPersistenceError> {
        self.restore_authoritative_raft_snapshot(data)
            .await
            .map_err(|error| SnapshotPersistenceError::persistence_failed(error.to_string()))
    }
}

#[cfg(test)]
impl AuthoritativeSnapshotPersistence for LegacyTestSnapshotPersistence {
    fn restore_authoritative_snapshot(
        &self,
        snapshot: AuthoritativeSnapshot,
    ) -> SnapshotPersistenceFuture<'_> {
        Box::pin(async move {
            let mut parts = snapshot.into_parts();
            let position = parts.position();
            let operations = parts.take_operations();
            let replay_floors = parts.take_replay_floors();
            let (metadata, membership) = parts.into_metadata_and_membership();
            self.db
                .replace_replicated_resource_state(
                    operations,
                    metadata.current_rv,
                    position.map(|position| position.event_id),
                    replay_floors.map(|floors| {
                        floors
                            .into_iter()
                            .map(|floor| {
                                let (
                                    target,
                                    floor_resource_version,
                                    floor_event_id,
                                    position_is_exact,
                                ) = floor.into_parts();
                                let (api_version, kind, namespace_key) = match target {
                                    DurableReplayTarget::All => {
                                        ("*".to_string(), "*".to_string(), "*".to_string())
                                    }
                                    DurableReplayTarget::Cluster { api_version, kind } => {
                                        (api_version, kind, "#cluster".to_string())
                                    }
                                    DurableReplayTarget::Namespaced {
                                        api_version,
                                        kind,
                                        namespace,
                                    } => (api_version, kind, namespace),
                                };
                                crate::datastore::WatchReplayFloor {
                                    api_version,
                                    kind,
                                    namespace_key,
                                    floor_resource_version,
                                    floor_event_id,
                                    position_is_exact,
                                }
                            })
                            .collect()
                    }),
                    Some(crate::datastore::ReplicatedSnapshotMetadata {
                        cluster_id: metadata.cluster_id,
                        leader_epoch: metadata.leader_epoch,
                        membership: match membership {
                            SnapshotMembership::LegacyOmitted => {
                                crate::datastore::ReplicatedMembershipState::LegacyOmitted
                            }
                            SnapshotMembership::AuthoritativeAbsent => {
                                crate::datastore::ReplicatedMembershipState::AuthoritativeAbsent
                            }
                            SnapshotMembership::Present(value) => {
                                crate::datastore::ReplicatedMembershipState::Present(value)
                            }
                        },
                        command_codec_activation_version: None,
                    }),
                )
                .await
                .map_err(map_snapshot_persistence_error)
        })
    }
}

/// Normalizes backend table-family fragments into bounded public commit pages.
///
/// A backend may expose several adjacent commit pages while it walks distinct
/// physical tables. The cluster-store port deliberately exposes one logical
/// commit family, so combine adjacent fragments up to the caller's page bound.
/// At most one bounded remainder page is retained between calls; the adapter
/// never reconstructs or buffers a complete snapshot.
#[cfg(test)]
struct NormalizingSnapshotCaptureSession {
    inner: Box<dyn SnapshotCaptureSession>,
    buffered: Option<SnapshotCapturePage>,
    page_limit: usize,
}

#[cfg(test)]
impl NormalizingSnapshotCaptureSession {
    fn new(inner: Box<dyn SnapshotCaptureSession>, page_limit: usize) -> Self {
        Self {
            inner,
            buffered: None,
            page_limit,
        }
    }

    async fn next_normalized_page(
        &mut self,
    ) -> Result<Option<SnapshotCapturePage>, SnapshotPersistenceError> {
        let Some(first) = (match self.buffered.take() {
            Some(page) => Some(page),
            None => self.inner.next_page().await?,
        }) else {
            return Ok(None);
        };
        if first.kind() != SnapshotCapturePageKind::Commits {
            return Ok(Some(first));
        }

        let mut operations = first
            .into_operations()
            .expect("commit page kind must contain snapshot restore operations");
        while operations.len() < self.page_limit {
            let Some(next) = self.inner.next_page().await? else {
                break;
            };
            if next.kind() != SnapshotCapturePageKind::Commits {
                self.buffered = Some(next);
                break;
            }

            let remaining = self.page_limit - operations.len();
            let mut next_operations = next
                .into_operations()
                .expect("commit page kind must contain snapshot restore operations");
            if next_operations.len() <= remaining {
                operations.append(&mut next_operations);
                continue;
            }

            let remainder = next_operations.split_off(remaining);
            operations.append(&mut next_operations);
            self.buffered = Some(SnapshotCapturePage::try_operations(remainder)?);
            break;
        }
        Ok(Some(SnapshotCapturePage::try_operations(operations)?))
    }
}

#[cfg(test)]
impl SnapshotCaptureSession for NormalizingSnapshotCaptureSession {
    fn header(&self) -> &SnapshotCaptureHeader {
        self.inner.header()
    }

    fn next_page(&mut self) -> SnapshotPersistenceFuture<'_, Option<SnapshotCapturePage>> {
        Box::pin(self.next_normalized_page())
    }

    fn cancel(&mut self) -> SnapshotPersistenceFuture<'_> {
        self.buffered = None;
        self.inner.cancel()
    }
}

#[cfg(test)]
impl AuthoritativeSnapshotCapture for LegacyTestSnapshotPersistence {
    fn begin_capture(
        &self,
        request: klights_cluster_store::SnapshotCaptureRequest,
    ) -> SnapshotPersistenceFuture<'_, Box<dyn klights_cluster_store::SnapshotCaptureSession>> {
        Box::pin(async move {
            let fence = self
                .lifecycle
                .acquire_snapshot_exclusive_fence()
                .await
                .map_err(map_snapshot_persistence_error)?
                .ok_or_else(|| SnapshotPersistenceError::PersistenceFailed {
                    message: "backend does not provide a snapshot capture fence".to_string(),
                })?;
            let session = self
                .recovery
                .begin_pinned_snapshot_capture(request, fence)
                .await
                .map_err(map_snapshot_persistence_error)?;
            Ok(Box::new(NormalizingSnapshotCaptureSession::new(
                session,
                request.page_limit().get(),
            )) as Box<dyn SnapshotCaptureSession>)
        })
    }
}

#[cfg(test)]
fn map_snapshot_persistence_error(error: anyhow::Error) -> SnapshotPersistenceError {
    if let Some(error) = error.downcast_ref::<SnapshotPersistenceError>() {
        return error.clone();
    }
    if let Some(error) = error.downcast_ref::<klights_node_store::RaftDurabilityError>() {
        return match error {
            klights_node_store::RaftDurabilityError::InvalidInput { message, .. }
            | klights_node_store::RaftDurabilityError::CorruptData { message, .. } => {
                SnapshotPersistenceError::CorruptData {
                    message: message.clone(),
                }
            }
            klights_node_store::RaftDurabilityError::Retryable { message, .. } => {
                SnapshotPersistenceError::Retryable {
                    message: message.clone(),
                }
            }
            klights_node_store::RaftDurabilityError::Timeout => SnapshotPersistenceError::Timeout,
            klights_node_store::RaftDurabilityError::Cancelled => {
                SnapshotPersistenceError::Cancelled
            }
            klights_node_store::RaftDurabilityError::PersistenceFailed { operation, message } => {
                SnapshotPersistenceError::persistence_failed(format!("{operation}: {message}"))
            }
            _ => SnapshotPersistenceError::persistence_failed(error.to_string()),
        };
    }
    let message = format!("{error:#}");
    let lower = message.to_ascii_lowercase();
    if lower.contains("corrupt") || lower.contains("decode") || lower.contains("invalid") {
        SnapshotPersistenceError::CorruptData { message }
    } else if lower.contains("unsupported")
        || lower.contains("does not support")
        || lower.contains("does not implement")
    {
        SnapshotPersistenceError::UnsupportedMode { message }
    } else if lower.contains("cancel") {
        SnapshotPersistenceError::Cancelled
    } else if lower.contains("timeout") || lower.contains("timed out") {
        SnapshotPersistenceError::Timeout
    } else if lower.contains("busy") || lower.contains("locked") {
        SnapshotPersistenceError::Retryable { message }
    } else {
        SnapshotPersistenceError::persistence_failed(message)
    }
}

/// Read-only canonical metadata wrapper over the legacy umbrella datastore.
/// REMOVE(Phase 10): concrete cluster datastore adapters implement the
/// cluster-store ports directly after physical extraction.
#[cfg(test)]
pub(crate) struct LegacyTestClusterMetadataRead {
    db: DatastoreHandle,
}

#[cfg(test)]
impl LegacyTestClusterMetadataRead {
    pub(crate) fn new(db: DatastoreHandle) -> Self {
        Self { db }
    }
}

#[cfg(test)]
const _: fn(DatastoreHandle) -> LegacyTestClusterMetadataRead = LegacyTestClusterMetadataRead::new;

#[cfg(test)]
impl ClusterMetadataRead for LegacyTestClusterMetadataRead {
    fn read_cluster_metadata(&self) -> ClusterMetadataFuture<'_, PersistedClusterMetadata> {
        Box::pin(async move {
            let observation = self
                .db
                .read_cluster_metadata_observation()
                .await
                .map_err(map_cluster_metadata_error)?;
            let membership = match observation.membership {
                crate::datastore::ReplicatedMembershipState::LegacyOmitted => {
                    SnapshotMembership::LegacyOmitted
                }
                crate::datastore::ReplicatedMembershipState::AuthoritativeAbsent => {
                    SnapshotMembership::AuthoritativeAbsent
                }
                crate::datastore::ReplicatedMembershipState::Present(value) => {
                    SnapshotMembership::Present(value)
                }
            };
            Ok(PersistedClusterMetadata::new(
                observation.metadata,
                membership,
            ))
        })
    }
}

#[cfg(test)]
fn map_cluster_metadata_error(error: anyhow::Error) -> ClusterMetadataStoreError {
    let message = format!("{error:#}");
    let lower = message.to_ascii_lowercase();
    if lower.contains("incomplete") || lower.contains("missing") || lower.contains("empty") {
        ClusterMetadataStoreError::Incomplete { message }
    } else if lower.contains("invalid")
        || lower.contains("malformed")
        || lower.contains("duplicate")
    {
        ClusterMetadataStoreError::CorruptData { message }
    } else if lower.contains("cancel") {
        ClusterMetadataStoreError::Cancelled
    } else if lower.contains("timeout") || lower.contains("timed out") {
        ClusterMetadataStoreError::Timeout
    } else if lower.contains("busy") || lower.contains("locked") {
        ClusterMetadataStoreError::Retryable { message }
    } else {
        ClusterMetadataStoreError::persistence_failed(message)
    }
}
