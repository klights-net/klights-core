//! Temporary root adapters from the legacy datastore to cluster-store ports.

use klights_cluster_core::{CommittedApplyOutcome, LogApplyCommit, LogApplyMutation};
use klights_cluster_store::{
    AllocatorStateError, AllocatorStateFuture, AppliedOutboxLookup, AuthoritativeSnapshot,
    AuthoritativeSnapshotCapture, AuthoritativeSnapshotPersistence, ClusterMetadataFuture,
    ClusterMetadataRead, ClusterMetadataStoreError, ClusterResourceRead, CommittedApplyError,
    CommittedApplyFuture, CommittedRaftApplyReceipt, CommittedRaftApplyRequest,
    DurableAllocatorRead, DurableAllocatorState, DurableApplyLedgerRead, DurableReplayFloor,
    DurableReplayTarget, DurableWatchEvent, DurableWatchHistoryRead, DurableWatchScope,
    PersistedClusterMetadata, PrivilegedCommittedRaftApply, ResourceCollectionKey,
    ResourceCollectionScope, ResourceContinuation, ResourceGetRequest, ResourceListPage,
    ResourceListRead, ResourceListRequest, ResourceListSnapshot, ResourceReadError,
    ResourceReadFuture, ResourceVersionMatch, SnapshotCaptureHeader, SnapshotCapturePage,
    SnapshotCapturePageKind, SnapshotCaptureSession, SnapshotMembership, SnapshotPersistenceError,
    SnapshotPersistenceFuture, WatchHistoryError, WatchHistoryFuture, WatchHistoryPage,
    WatchHistoryRead, WatchHistoryRequest,
};

use super::{DatastoreHandle, ResourceListQuery};

pub(crate) struct DatastoreRaftCommitMaterializer {
    db: DatastoreHandle,
}

impl DatastoreRaftCommitMaterializer {
    pub(crate) fn new(db: DatastoreHandle) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl crate::datastore::raft::node::RaftCommitMaterializer for DatastoreRaftCommitMaterializer {
    async fn read_raft_metadata(
        &self,
        key: &str,
    ) -> Result<Option<String>, crate::datastore::raft::node::RaftMaterializationError> {
        self.db.get_klights_meta(key).await.map_err(|error| {
            crate::datastore::raft::node::RaftMaterializationError::persistence(error.to_string())
        })
    }

    async fn build_command(
        &self,
        command: klights_cluster_core::StorageCommand,
        operation: &str,
        authoring_node: &str,
    ) -> Result<
        klights_cluster_core::LogApplyCommit,
        crate::datastore::raft::node::RaftMaterializationError,
    > {
        self.db
            .build_log_apply_commit_for_command(command, operation, authoring_node)
            .await
            .map_err(|error| {
                crate::datastore::raft::node::RaftMaterializationError::persistence(
                    error.to_string(),
                )
            })
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
    crate::datastore::raft::state_machine_impl::RaftStateMachineStorePorts::new(
        std::sync::Arc::new(DatastoreCommittedRaftApply::new_for_test(db.clone())),
        std::sync::Arc::new(DatastoreAuthoritativeSnapshotPersistence::new_for_test(
            db.clone(),
        )),
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
pub(crate) struct DatastoreClusterResourceRead {
    db: DatastoreHandle,
}

impl DatastoreClusterResourceRead {
    pub(crate) fn new(db: DatastoreHandle) -> Self {
        Self { db }
    }
}

// Packet 6A defines the adapter before a later consumer migration. Keep its
// complete construction boundary checked without runtime wiring or lint
// suppression.
const _: fn(DatastoreHandle) -> DatastoreClusterResourceRead = DatastoreClusterResourceRead::new;

impl ClusterResourceRead for DatastoreClusterResourceRead {
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
pub(crate) struct DatastoreCommittedRaftApply {
    db: DatastoreHandle,
    wakeups: Option<std::sync::Arc<dyn klights_leader_api::PostCommitWakeup>>,
}

impl DatastoreCommittedRaftApply {
    pub(crate) fn new(
        db: DatastoreHandle,
        _authority: crate::datastore::raft::CommittedApplyAuthority,
        wakeups: std::sync::Arc<dyn klights_leader_api::PostCommitWakeup>,
    ) -> Self {
        Self {
            db,
            wakeups: Some(wakeups),
        }
    }

    #[cfg(test)]
    fn new_for_test(db: DatastoreHandle) -> Self {
        Self { db, wakeups: None }
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

    pub(crate) async fn apply_committed_raft_result(
        &self,
        request: CommittedRaftApplyRequest,
    ) -> Result<crate::datastore::raft::types::StorageCommandResult, CommittedApplyError> {
        let commit = request.into_commit();
        let result = self
            .db
            .apply_raft_log_apply_commit(commit.clone())
            .await
            .map_err(map_committed_apply_error)?;
        if result.public_resource_changed
            && let Some(resource_version) = result.applied_rv
        {
            self.publish_visible_commit(&commit, resource_version);
        }
        Ok(result)
    }
}

#[async_trait::async_trait]
impl crate::datastore::raft::state_machine_impl::RaftCommittedApply
    for DatastoreCommittedRaftApply
{
    async fn apply_committed(
        &self,
        request: CommittedRaftApplyRequest,
    ) -> Result<
        crate::datastore::raft::types::StorageCommandResult,
        klights_cluster_store::CommittedApplyError,
    > {
        self.apply_committed_raft_result(request).await
    }
}

impl PrivilegedCommittedRaftApply for DatastoreCommittedRaftApply {
    fn apply_committed_raft(
        &self,
        request: CommittedRaftApplyRequest,
    ) -> CommittedApplyFuture<'_, CommittedRaftApplyReceipt> {
        Box::pin(async move {
            let commit = request.into_commit();
            let outcome = self
                .db
                .apply_raft_log_apply_commit_outcome(commit.clone())
                .await
                .map_err(map_committed_apply_error)?;
            if let CommittedApplyOutcome::Visible {
                resource_version, ..
            } = &outcome
            {
                self.publish_visible_commit(&commit, *resource_version);
            }
            Ok(CommittedRaftApplyReceipt::new(outcome))
        })
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

impl DurableApplyLedgerRead for DatastoreCommittedRaftApply {
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
                .map(|record| record.map(Into::into))
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
pub(crate) struct DatastoreDurableWatchHistory {
    db: DatastoreHandle,
}

impl DatastoreDurableWatchHistory {
    pub(crate) fn new(db: DatastoreHandle) -> Self {
        Self { db }
    }
}

const _: fn(DatastoreHandle) -> DatastoreDurableWatchHistory = DatastoreDurableWatchHistory::new;

impl DurableWatchHistoryRead for DatastoreDurableWatchHistory {
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
pub(crate) struct DatastoreDurableAllocatorRead {
    db: DatastoreHandle,
}

impl DatastoreDurableAllocatorRead {
    pub(crate) fn new(db: DatastoreHandle) -> Self {
        Self { db }
    }
}

const _: fn(DatastoreHandle) -> DatastoreDurableAllocatorRead = DatastoreDurableAllocatorRead::new;

impl DurableAllocatorRead for DatastoreDurableAllocatorRead {
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

/// Privileged authoritative-restore wrapper over the legacy umbrella datastore.
/// REMOVE(Phase 10): concrete cluster datastore adapters implement the
/// cluster-store ports directly after physical extraction.
pub(crate) struct DatastoreAuthoritativeSnapshotPersistence {
    db: DatastoreHandle,
    recovery: std::sync::Arc<dyn crate::datastore::DurableRecoveryStore>,
}

impl DatastoreAuthoritativeSnapshotPersistence {
    pub(crate) fn new(
        db: DatastoreHandle,
        recovery: std::sync::Arc<dyn crate::datastore::DurableRecoveryStore>,
        _authority: crate::datastore::raft::SnapshotInstallAuthority,
    ) -> Self {
        Self { db, recovery }
    }

    #[cfg(test)]
    fn new_for_test(db: DatastoreHandle) -> Self {
        Self::from_handle(db)
    }

    #[cfg(test)]
    fn from_handle(db: DatastoreHandle) -> Self {
        let recovery = std::sync::Arc::new(crate::datastore::DatastoreDurableRecoveryPort::new(
            db.clone(),
        ));
        Self { db, recovery }
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
impl crate::datastore::raft::state_machine_impl::RaftSnapshotRestore
    for DatastoreAuthoritativeSnapshotPersistence
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

impl AuthoritativeSnapshotPersistence for DatastoreAuthoritativeSnapshotPersistence {
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
struct NormalizingSnapshotCaptureSession {
    inner: Box<dyn SnapshotCaptureSession>,
    buffered: Option<SnapshotCapturePage>,
    page_limit: usize,
}

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

impl AuthoritativeSnapshotCapture for DatastoreAuthoritativeSnapshotPersistence {
    fn begin_capture(
        &self,
        request: klights_cluster_store::SnapshotCaptureRequest,
    ) -> SnapshotPersistenceFuture<'_, Box<dyn klights_cluster_store::SnapshotCaptureSession>> {
        Box::pin(async move {
            let session = self
                .recovery
                .begin_pinned_snapshot_capture(request)
                .await
                .map_err(map_snapshot_persistence_error)?;
            Ok(Box::new(NormalizingSnapshotCaptureSession::new(
                session,
                request.page_limit().get(),
            )) as Box<dyn SnapshotCaptureSession>)
        })
    }
}

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
pub(crate) struct DatastoreClusterMetadataRead {
    db: DatastoreHandle,
}

impl DatastoreClusterMetadataRead {
    pub(crate) fn new(db: DatastoreHandle) -> Self {
        Self { db }
    }
}

const _: fn(DatastoreHandle) -> DatastoreClusterMetadataRead = DatastoreClusterMetadataRead::new;

impl ClusterMetadataRead for DatastoreClusterMetadataRead {
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

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Arc;

    use klights_cluster_core::{
        ClusterMembership, ClusterMetadata, LogApplyAppliedOutboxRow, LogApplyCommit,
        LogApplyMutation, LogApplyNodeDataplaneRow, LogApplyNodeSubnetRow,
        LogApplyPodCleanupIntentRow, LogApplyResourceRow, LogApplyWatchEventRow,
        NoPublicChangeReason, OutboxStreamWatermark, SnapshotRestoreOperation, WatchReplayPosition,
    };
    use klights_cluster_store::{
        AppliedOutboxLookup, AuthoritativeSnapshot, AuthoritativeSnapshotCapture,
        AuthoritativeSnapshotPersistence, ClusterMetadataRead, ClusterResourceRead,
        CommittedApplyError, CommittedRaftApplyRequest, DurableAllocatorRead,
        DurableApplyLedgerRead, DurableReplayFloor, DurableReplayTarget, DurableWatchHistoryRead,
        DurableWatchTarget, PrivilegedCommittedRaftApply, ResourceCollectionScope,
        ResourceGetRequest, ResourceListQuery, ResourceListRead, ResourceListRequest,
        ResourceReadError, ResourceVersionMatch, SnapshotCaptureHeader, SnapshotCapturePage,
        SnapshotCaptureSession, SnapshotMembership, SnapshotPersistenceError,
        SnapshotPersistenceFuture, WatchHistoryRead, WatchHistoryRequest,
    };
    use serde_json::json;

    type TestSinkFuture<'a> = SnapshotPersistenceFuture<'a>;

    trait TestPageSink: Send {
        fn begin_capture(&mut self, header: &SnapshotCaptureHeader) -> TestSinkFuture<'_>;
        fn push_page(&mut self, page: SnapshotCapturePage) -> TestSinkFuture<'_>;
    }

    trait PullCaptureTestExt {
        fn collect_snapshot_pages<'a>(
            &'a self,
            sink: &'a mut dyn TestPageSink,
        ) -> SnapshotPersistenceFuture<'a, SnapshotCaptureHeader>;
    }

    impl<T: AuthoritativeSnapshotCapture + ?Sized> PullCaptureTestExt for T {
        fn collect_snapshot_pages<'a>(
            &'a self,
            sink: &'a mut dyn TestPageSink,
        ) -> SnapshotPersistenceFuture<'a, SnapshotCaptureHeader> {
            Box::pin(async move {
                let request = klights_cluster_store::SnapshotCaptureRequest::try_new(
                    klights_cluster_store::SnapshotPageLimit::try_new(
                        klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE,
                    )?,
                    std::time::Duration::from_secs(30),
                )?;
                let mut session = self.begin_capture(request).await?;
                let header = session.header().clone();
                sink.begin_capture(&header).await?;
                while let Some(page) = session.next_page().await? {
                    sink.push_page(page).await?;
                }
                Ok(header)
            })
        }
    }

    use super::{
        DatastoreAuthoritativeSnapshotPersistence, DatastoreClusterMetadataRead,
        DatastoreClusterResourceRead, DatastoreCommittedRaftApply, DatastoreDurableAllocatorRead,
        DatastoreDurableWatchHistory, NormalizingSnapshotCaptureSession, map_committed_apply_error,
        map_snapshot_persistence_error,
    };
    use crate::datastore::sqlite::Datastore;
    use crate::datastore::{DatastoreBackend, DatastoreHandle};

    async fn persistent_snapshot_store() -> (tempfile::TempDir, Datastore) {
        let root = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let db = Datastore::new_persistent_paths(&root.path().join("cluster.db"), supervisor, None)
            .await
            .unwrap();
        (root, db)
    }

    struct FragmentedCaptureSession {
        header: SnapshotCaptureHeader,
        pages: VecDeque<SnapshotCapturePage>,
    }

    impl SnapshotCaptureSession for FragmentedCaptureSession {
        fn header(&self) -> &SnapshotCaptureHeader {
            &self.header
        }

        fn next_page(&mut self) -> SnapshotPersistenceFuture<'_, Option<SnapshotCapturePage>> {
            let page = self.pages.pop_front();
            Box::pin(async move { Ok(page) })
        }

        fn cancel(&mut self) -> SnapshotPersistenceFuture<'_> {
            self.pages.clear();
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn normalizing_session_coalesces_commits_with_one_bounded_remainder() {
        let page_limit = 256;
        let commit_page = |start: i64| {
            SnapshotCapturePage::try_operations(
                (start..start + 200)
                    .map(|resource_version| {
                        SnapshotRestoreOperation::new(resource_version, None, Vec::new())
                    })
                    .collect(),
            )
            .unwrap()
        };
        let header = SnapshotCaptureHeader::try_new(
            None,
            WatchReplayPosition {
                resource_version: 600,
                event_id: 0,
                resource_version_filter_through_event_id: 0,
            },
            ClusterMetadata {
                cluster_id: "normalizing-session".into(),
                leader_epoch: 1,
                current_rv: 600,
            },
            SnapshotMembership::AuthoritativeAbsent,
        )
        .unwrap();
        let inner = FragmentedCaptureSession {
            header,
            pages: VecDeque::from([commit_page(1), commit_page(201), commit_page(401)]),
        };
        let mut session = NormalizingSnapshotCaptureSession::new(Box::new(inner), page_limit);

        let mut lengths = Vec::new();
        while let Some(page) = session.next_page().await.unwrap() {
            lengths.push(page.len());
            assert!(page.len() <= page_limit);
            assert!(
                session
                    .buffered
                    .as_ref()
                    .is_none_or(|buffered| buffered.len() <= page_limit),
                "normalization may retain only one bounded remainder page"
            );
        }
        assert_eq!(lengths, vec![256, 256, 88]);
    }

    #[tokio::test]
    async fn raft_restore_replaces_divergent_metadata_and_authoritative_absence() {
        let destination = Datastore::new_in_memory().await.unwrap();
        destination
            .replace_replicated_resource_state(
                Vec::new(),
                9,
                Some(0),
                Some(Vec::new()),
                Some(crate::datastore::ReplicatedSnapshotMetadata {
                    cluster_id: "divergent-cluster".into(),
                    leader_epoch: 99,
                    membership: crate::datastore::ReplicatedMembershipState::Present(
                        ClusterMembership {
                            cluster_id: "divergent-cluster".into(),
                            voters: vec!["stale-cp".into()],
                            term: 99,
                            leader_hint: Some("stale-cp".into()),
                        },
                    ),
                    command_codec_activation_version: None,
                }),
            )
            .await
            .unwrap();
        let restore =
            DatastoreAuthoritativeSnapshotPersistence::new_for_test(Arc::new(destination.clone()));
        restore
            .restore_authoritative_raft_snapshot(crate::datastore::raft::snapshot::RaftSnapshotData {
                last_applied: None,
                membership: openraft::StoredMembership::default(),
                current_rv: 5,
                command_codec_activation_version: None,
                watch_event_high_water: Some(0),
                watch_replay_floors: Some(Vec::new()),
                cluster_metadata: Some(ClusterMetadata {
                    cluster_id: "leader-cluster".into(),
                    leader_epoch: 7,
                    current_rv: 5,
                }),
                cluster_membership: Some(
                    crate::datastore::raft::snapshot::RaftSnapshotMembership::AuthoritativeAbsent,
                ),
                operations: Vec::new(),
            })
            .await
            .unwrap();

        let observed = destination
            .read_cluster_metadata_observation()
            .await
            .unwrap();
        assert_eq!(
            observed.metadata,
            ClusterMetadata {
                cluster_id: "leader-cluster".into(),
                leader_epoch: 7,
                current_rv: 5,
            }
        );
        assert_eq!(
            observed.membership,
            crate::datastore::ReplicatedMembershipState::AuthoritativeAbsent
        );
    }

    #[test]
    fn adapter_maps_unsupported_backend_and_mode_errors_semantically() {
        assert!(matches!(
            map_committed_apply_error(anyhow::anyhow!(
                "redb backend does not support raft log-apply commit replay"
            )),
            CommittedApplyError::UnsupportedMode { .. }
        ));
        assert!(matches!(
            map_snapshot_persistence_error(anyhow::anyhow!(
                "redb backend does not support authoritative snapshot restore"
            )),
            SnapshotPersistenceError::UnsupportedMode { .. }
        ));
    }

    async fn seeded_reader() -> (Datastore, DatastoreClusterResourceRead) {
        let db = Datastore::new_in_memory()
            .await
            .expect("in-memory datastore");
        for (name, tier, ordinal) in [
            ("alpha", "frontend", 1),
            ("beta", "backend", 2),
            ("gamma", "frontend", 3),
        ] {
            db.create_resource(
                "v1",
                "ConfigMap",
                Some("tenant-a"),
                name,
                json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {
                        "namespace": "tenant-a",
                        "name": name,
                        "labels": {"tier": tier}
                    },
                    "data": {"ordinal": ordinal.to_string()}
                }),
            )
            .await
            .unwrap_or_else(|error| panic!("seed {name}: {error:#}"));
        }
        db.create_resource(
            "v1",
            "Node",
            None,
            "worker-a",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "worker-a"}
            }),
        )
        .await
        .expect("seed cluster-scoped Node");
        let handle: DatastoreHandle = Arc::new(db.clone()) as Arc<dyn DatastoreBackend>;
        (db, DatastoreClusterResourceRead::new(handle))
    }

    async fn committed_store() -> (
        Datastore,
        DatastoreCommittedRaftApply,
        DatastoreClusterResourceRead,
    ) {
        let db = Datastore::new_in_memory()
            .await
            .expect("in-memory datastore");
        let handle: DatastoreHandle = Arc::new(db.clone()) as Arc<dyn DatastoreBackend>;
        (
            db,
            DatastoreCommittedRaftApply::new_for_test(handle.clone()),
            DatastoreClusterResourceRead::new(handle),
        )
    }

    fn committed_v1(mutations: Vec<LogApplyMutation>) -> LogApplyCommit {
        LogApplyCommit::try_new(mutations).expect("test commit must be an RV-zero live template")
    }

    fn status_commit(
        idempotency_key: &str,
        status_message: &str,
        status_stamp: i64,
        stream_seq: i64,
    ) -> LogApplyCommit {
        LogApplyCommit::try_new_with_watermark(
            vec![
                LogApplyMutation::PutResource(LogApplyResourceRow {
                    api_version: "v1".to_string(),
                    kind: "Pod".to_string(),
                    namespace: Some("default".to_string()),
                    name: "adapter-status".to_string(),
                    uid: "adapter-status-uid".to_string(),
                    resource_version: 0,
                    data: json!({
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {
                            "namespace": "default",
                            "name": "adapter-status",
                            "uid": "adapter-status-uid"
                        },
                        "status": {"phase": "Running", "message": status_message}
                    }),
                    require_absent: false,
                    require_existing: true,
                    precondition_uid: Some("adapter-status-uid".to_string()),
                    precondition_resource_version: None,
                    status_only: true,
                }),
                LogApplyMutation::PutAppliedOutbox(LogApplyAppliedOutboxRow {
                    idempotency_key: idempotency_key.to_string(),
                    subject_key: "v1/Pod/default/adapter-status/adapter-status-uid".to_string(),
                    operation: "PodStatus".to_string(),
                    first_seen_ms: status_stamp,
                    applied_rv: None,
                    result_proto: crate::replication::outbox_response_wire::encode_outbox_response(
                        &klights_cluster_core::command::StorageResponse::Ack {
                            resource_version: 0,
                        },
                    )
                    .expect("encode applied-outbox acknowledgement"),
                    status_stamp: Some(status_stamp),
                }),
            ],
            Some(OutboxStreamWatermark {
                client_id: "adapter-worker".to_string(),
                stream_id: 11,
                stream_seq,
            }),
        )
        .expect("status commit must be an RV-zero live template")
    }

    async fn adapter_pod(reader: &DatastoreClusterResourceRead) -> klights_cluster_core::Resource {
        reader
            .get_resource(ResourceGetRequest::new(
                "v1",
                "Pod",
                Some("default".to_string()),
                "adapter-status",
            ))
            .await
            .expect("read adapter Pod")
            .expect("adapter Pod exists")
    }

    #[tokio::test]
    async fn adapter_preserves_identity_selectors_pages_and_position() {
        let (db, reader) = seeded_reader().await;

        struct Case {
            name: &'static str,
            label_selector: Option<&'static str>,
            field_selector: Option<&'static str>,
            expected_names: &'static [&'static str],
        }
        let cases = [
            Case {
                name: "label selector",
                label_selector: Some("tier=frontend"),
                field_selector: None,
                expected_names: &["alpha", "gamma"],
            },
            Case {
                name: "field selector",
                label_selector: None,
                field_selector: Some("metadata.name=beta"),
                expected_names: &["beta"],
            },
            Case {
                name: "combined selectors",
                label_selector: Some("tier=frontend"),
                field_selector: Some("metadata.name!=alpha"),
                expected_names: &["gamma"],
            },
        ];

        for case in cases {
            let page = reader
                .list_resources(ResourceListRequest::new(
                    "v1",
                    "ConfigMap",
                    ResourceCollectionScope::Namespace("tenant-a".to_string()),
                    ResourceListQuery::try_new(
                        case.label_selector.map(str::to_owned),
                        case.field_selector.map(str::to_owned),
                        None,
                        None,
                        ResourceVersionMatch::Any,
                    )
                    .expect("valid query"),
                ))
                .await
                .unwrap_or_else(|error| panic!("{}: {error}", case.name));
            let actual = page
                .items()
                .iter()
                .map(|resource| resource.name.as_str())
                .collect::<Vec<_>>();
            assert_eq!(actual, case.expected_names, "{}", case.name);
            let position = page
                .snapshot()
                .unwrap_or_else(|| panic!("{}: missing positioned snapshot", case.name))
                .position();
            assert_eq!(
                position.resource_version,
                page.snapshot().unwrap().resource_version(),
                "{}",
                case.name
            );
            assert!(position.event_id > 0, "{}", case.name);
        }

        let unfiltered = reader
            .list_resources(ResourceListRequest::new(
                "v1",
                "ConfigMap",
                ResourceCollectionScope::Namespace("tenant-a".to_string()),
                ResourceListQuery::try_new(None, None, Some(1), None, ResourceVersionMatch::Any)
                    .expect("valid unfiltered page"),
            ))
            .await
            .expect("unfiltered page");
        assert_eq!(unfiltered.items()[0].name, "alpha");
        assert_eq!(
            unfiltered
                .continuation()
                .map(|cursor| cursor.after().name()),
            Some("alpha")
        );
        assert_eq!(unfiltered.remaining_item_count(), Some(2));

        let first = reader
            .list_resources(ResourceListRequest::new(
                "v1",
                "ConfigMap",
                ResourceCollectionScope::Namespace("tenant-a".to_string()),
                ResourceListQuery::try_new(
                    Some("tier=frontend".to_string()),
                    None,
                    Some(1),
                    None,
                    ResourceVersionMatch::Any,
                )
                .expect("valid first page"),
            ))
            .await
            .expect("first page");
        assert_eq!(first.items()[0].name, "alpha");
        assert_eq!(
            first.continuation().map(|cursor| cursor.after().name()),
            Some("alpha")
        );
        let first_position = first.snapshot().expect("first position").position();

        let second = reader
            .list_resources(ResourceListRequest::new(
                "v1",
                "ConfigMap",
                ResourceCollectionScope::Namespace("tenant-a".to_string()),
                ResourceListQuery::try_new(
                    Some("tier=frontend".to_string()),
                    None,
                    Some(1),
                    first.continuation().cloned(),
                    ResourceVersionMatch::Any,
                )
                .expect("valid continuation"),
            ))
            .await
            .expect("second page");
        assert_eq!(second.items()[0].name, "gamma");
        assert_eq!(second.continuation(), None);
        assert_eq!(
            second.snapshot().map(|snapshot| snapshot.position()),
            Some(first_position)
        );
        assert_eq!(
            second.snapshot().map(|snapshot| snapshot.position()),
            Some(
                db.current_watch_replay_position()
                    .await
                    .expect("current position")
            )
        );

        let found = reader
            .get_resource(ResourceGetRequest::new(
                "v1",
                "ConfigMap",
                Some("tenant-a".to_string()),
                "alpha",
            ))
            .await
            .expect("get alpha")
            .expect("alpha exists");
        assert_eq!(found.api_version, "v1");
        assert_eq!(found.kind, "ConfigMap");
        assert_eq!(found.namespace.as_deref(), Some("tenant-a"));
        assert_eq!(found.name, "alpha");
        assert!(!found.uid.is_empty());
        assert!(found.resource_version > 0);

        assert!(
            reader
                .get_resource(ResourceGetRequest::new(
                    "v1",
                    "ConfigMap",
                    Some("tenant-b".to_string()),
                    "alpha",
                ))
                .await
                .expect("get absent namespace")
                .is_none()
        );

        let cluster_page = reader
            .list_resources(ResourceListRequest::new(
                "v1",
                "Node",
                ResourceCollectionScope::Cluster,
                ResourceListQuery::all(),
            ))
            .await
            .expect("cluster-scoped list");
        assert_eq!(
            cluster_page
                .items()
                .iter()
                .map(|resource| (resource.namespace.as_deref(), resource.name.as_str()))
                .collect::<Vec<_>>(),
            vec![(None, "worker-a")]
        );
    }

    #[tokio::test]
    async fn exact_not_older_than_and_expired_list_results_are_typed() {
        let (db, reader) = seeded_reader().await;
        let historical = reader
            .list_resources(ResourceListRequest::new(
                "v1",
                "ConfigMap",
                ResourceCollectionScope::Namespace("tenant-a".to_string()),
                ResourceListQuery::try_new(None, None, None, None, ResourceVersionMatch::Exact(2))
                    .unwrap(),
            ))
            .await
            .expect("historical exact list");
        assert!(matches!(historical, ResourceListRead::Historical(_)));
        assert_eq!(
            historical
                .items()
                .iter()
                .map(|resource| resource.name.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "beta"]
        );

        let current = db.get_current_resource_version().await.unwrap();
        assert!(matches!(
            reader
                .list_resources(ResourceListRequest::new(
                    "v1",
                    "ConfigMap",
                    ResourceCollectionScope::Namespace("tenant-a".to_string()),
                    ResourceListQuery::try_new(
                        None,
                        None,
                        None,
                        None,
                        ResourceVersionMatch::NotOlderThan(current + 1),
                    )
                    .unwrap(),
                ))
                .await,
            Err(ResourceReadError::Conflict { .. })
        ));

        assert!(db.gc_watch_events(1, 1_000).await.unwrap() > 0);
        let expired = reader
            .list_resources(ResourceListRequest::new(
                "v1",
                "ConfigMap",
                ResourceCollectionScope::Namespace("tenant-a".to_string()),
                ResourceListQuery::try_new(None, None, None, None, ResourceVersionMatch::Exact(0))
                    .unwrap(),
            ))
            .await
            .expect("expired exact list is a typed result");
        match expired {
            ResourceListRead::Expired {
                requested,
                oldest_available,
            } => {
                assert_eq!(requested, 0);
                assert!(oldest_available > requested);
            }
            other => panic!("expected typed expired result, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn privileged_adapter_preserves_normal_committed_apply_and_durable_position() {
        let (_db, apply, reader) = committed_store().await;
        let before = apply
            .current_apply_position()
            .await
            .expect("position before apply");
        let receipt = apply
            .apply_committed_raft(CommittedRaftApplyRequest::new(committed_v1(vec![
                LogApplyMutation::PutResource(LogApplyResourceRow {
                    api_version: "v1".to_string(),
                    kind: "ConfigMap".to_string(),
                    namespace: Some("default".to_string()),
                    name: "adapter-normal".to_string(),
                    uid: "adapter-normal-uid".to_string(),
                    resource_version: 0,
                    data: json!({
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {
                            "namespace": "default",
                            "name": "adapter-normal",
                            "uid": "adapter-normal-uid"
                        }
                    }),
                    require_absent: true,
                    require_existing: false,
                    precondition_uid: None,
                    precondition_resource_version: None,
                    status_only: false,
                }),
            ])))
            .await
            .expect("normal committed apply");
        assert!(receipt.terminal_rejection().is_none());
        assert!(receipt.applied_resource().is_none());

        let rv = receipt
            .applied_resource_version()
            .expect("normal apply allocates an RV");
        let after = apply
            .current_apply_position()
            .await
            .expect("position after apply");
        assert!(rv > before.resource_version);
        assert_eq!(after.resource_version, rv);
        assert!(after.event_id > before.event_id);

        let resource = reader
            .get_resource(ResourceGetRequest::new(
                "v1",
                "ConfigMap",
                Some("default".to_string()),
                "adapter-normal",
            ))
            .await
            .expect("read normal result")
            .expect("normal result exists");
        assert_eq!(resource.uid, "adapter-normal-uid");
        assert_eq!(resource.resource_version, rv);

        let before_conflict = apply
            .current_apply_position()
            .await
            .expect("position before terminal conflict");
        let conflict = apply
            .apply_committed_raft(CommittedRaftApplyRequest::new(committed_v1(vec![
                LogApplyMutation::PutResource(LogApplyResourceRow {
                    api_version: "v1".to_string(),
                    kind: "ConfigMap".to_string(),
                    namespace: Some("default".to_string()),
                    name: "adapter-normal".to_string(),
                    uid: "different-uid".to_string(),
                    resource_version: 0,
                    data: json!({
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {
                            "namespace": "default",
                            "name": "adapter-normal",
                            "uid": "different-uid"
                        }
                    }),
                    require_absent: true,
                    require_existing: false,
                    precondition_uid: None,
                    precondition_resource_version: None,
                    status_only: false,
                }),
            ])))
            .await
            .expect("terminal conflicts are committed apply receipts");
        assert!(conflict.applied_resource_version().is_none());
        assert!(conflict.terminal_rejection().is_some());
        assert_eq!(
            apply
                .current_apply_position()
                .await
                .expect("position after terminal conflict"),
            before_conflict,
            "terminal conflict must not allocate public RV or watch history"
        );
    }

    #[tokio::test]
    async fn committed_status_ledger_invariants_are_table_driven_and_atomic() {
        let (db, apply, reader) = committed_store().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "adapter-status",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "adapter-status",
                    "uid": "adapter-status-uid"
                },
                "spec": {"nodeName": "node-a"},
                "status": {"phase": "Pending", "message": "origin"}
            }),
        )
        .await
        .expect("seed Pod");
        let seed_position = apply
            .current_apply_position()
            .await
            .expect("position after Pod seed");

        let fresh_receipt = apply
            .apply_committed_raft(CommittedRaftApplyRequest::new(status_commit(
                "fresh", "fresh", 200, 1,
            )))
            .await
            .expect("fresh stamped status");
        let fresh_rv = fresh_receipt
            .applied_resource_version()
            .expect("fresh status allocates an RV");
        let fresh_position = apply
            .current_apply_position()
            .await
            .expect("fresh durable position");
        let fresh_row = apply
            .get_applied_outbox(AppliedOutboxLookup::new("fresh"))
            .await
            .expect("fresh ledger lookup")
            .expect("fresh ledger row");
        assert_eq!(fresh_position.resource_version, fresh_rv);
        assert!(fresh_position.event_id > seed_position.event_id);
        assert_eq!(adapter_pod(&reader).await.resource_version, fresh_rv);
        assert_eq!(fresh_row.applied_rv, Some(fresh_rv));
        assert_eq!(fresh_row.status_stamp, Some(200));
        assert_eq!(
            apply
                .list_outbox_watermarks()
                .await
                .expect("fresh watermark"),
            vec![OutboxStreamWatermark {
                client_id: "adapter-worker".to_string(),
                stream_id: 11,
                stream_seq: 1,
            }]
        );

        struct Case {
            name: &'static str,
            idempotency_key: &'static str,
            status_message: &'static str,
            status_stamp: i64,
            stream_seq: i64,
            reason: NoPublicChangeReason,
        }
        let cases = [
            Case {
                name: "stale stamp",
                idempotency_key: "stale",
                status_message: "stale",
                status_stamp: 100,
                stream_seq: 2,
                reason: NoPublicChangeReason::StaleStatusStamp,
            },
            Case {
                name: "equal stamp",
                idempotency_key: "equal",
                status_message: "equal",
                status_stamp: 200,
                stream_seq: 3,
                reason: NoPublicChangeReason::EqualStatusStamp,
            },
        ];

        for case in cases {
            let before_position = apply
                .current_apply_position()
                .await
                .unwrap_or_else(|error| panic!("{} before position: {error}", case.name));
            let commit = status_commit(
                case.idempotency_key,
                case.status_message,
                case.status_stamp,
                case.stream_seq,
            );
            let receipt = apply
                .apply_committed_raft(CommittedRaftApplyRequest::new(commit.clone()))
                .await
                .unwrap_or_else(|error| panic!("{} apply: {error}", case.name));
            assert_eq!(
                receipt.applied_resource_version(),
                Some(fresh_rv),
                "{}",
                case.name
            );
            assert!(receipt.terminal_rejection().is_none(), "{}", case.name);
            assert!(
                matches!(
                    receipt.outcome(),
                    klights_cluster_core::CommittedApplyOutcome::NoPublicChange { reason, .. }
                        if *reason == case.reason
                ),
                "{}",
                case.name
            );
            assert_eq!(
                apply
                    .current_apply_position()
                    .await
                    .unwrap_or_else(|error| panic!("{} after position: {error}", case.name)),
                before_position,
                "{} must not change public RV or durable watch history",
                case.name
            );
            let pod = adapter_pod(&reader).await;
            assert_eq!(pod.resource_version, fresh_rv, "{}", case.name);
            assert_eq!(
                pod.data
                    .pointer("/status/message")
                    .and_then(|value| value.as_str()),
                Some("fresh"),
                "{}",
                case.name
            );
            let row = apply
                .get_applied_outbox(AppliedOutboxLookup::new(case.idempotency_key))
                .await
                .unwrap_or_else(|error| panic!("{} ledger lookup: {error}", case.name))
                .unwrap_or_else(|| panic!("{} ledger row", case.name));
            assert_eq!(row.applied_rv, Some(fresh_rv), "{}", case.name);
            assert_eq!(row.status_stamp, Some(case.status_stamp), "{}", case.name);
            assert_eq!(
                apply
                    .list_outbox_watermarks()
                    .await
                    .unwrap_or_else(|error| panic!("{} watermark: {error}", case.name))[0]
                    .stream_seq,
                case.stream_seq,
                "{}",
                case.name
            );

            let duplicate_position = apply
                .current_apply_position()
                .await
                .expect("position before duplicate");
            let duplicate = apply
                .apply_committed_raft(CommittedRaftApplyRequest::new(commit))
                .await
                .unwrap_or_else(|error| panic!("{} duplicate: {error}", case.name));
            assert_eq!(duplicate.applied_resource_version(), Some(fresh_rv));
            assert!(matches!(
                duplicate.outcome(),
                klights_cluster_core::CommittedApplyOutcome::NoPublicChange {
                    reason: NoPublicChangeReason::DuplicateIdempotencyKey,
                    ..
                }
            ));
            assert_eq!(
                apply
                    .current_apply_position()
                    .await
                    .expect("position after duplicate"),
                duplicate_position,
                "{} duplicate must be idempotent",
                case.name
            );
        }

        let before_gap = apply
            .current_apply_position()
            .await
            .expect("position before gap");
        let gap = apply
            .apply_committed_raft(CommittedRaftApplyRequest::new(status_commit(
                "gap",
                "must-not-apply",
                300,
                5,
            )))
            .await
            .expect_err("watermark gap must reject the whole transaction");
        assert!(gap.to_string().contains("outbox stream gap"));
        assert_eq!(
            apply
                .current_apply_position()
                .await
                .expect("position after gap"),
            before_gap
        );
        assert!(
            apply
                .get_applied_outbox(AppliedOutboxLookup::new("gap"))
                .await
                .expect("gap ledger lookup")
                .is_none(),
            "failed watermarked apply must not persist its outbox ledger row"
        );
        assert_eq!(
            apply
                .list_outbox_watermarks()
                .await
                .expect("watermark after gap")[0]
                .stream_seq,
            3
        );
        assert_eq!(
            adapter_pod(&reader)
                .await
                .data
                .pointer("/status/message")
                .and_then(|value| value.as_str()),
            Some("fresh")
        );
    }

    #[tokio::test]
    async fn committed_apply_outcome_does_not_depend_on_fallible_post_commit_observation() {
        let (db, apply, reader) = committed_store().await;
        db.fail_next_watch_position_observation();

        let receipt = apply
            .apply_committed_raft(CommittedRaftApplyRequest::new(committed_v1(vec![
                LogApplyMutation::PutResource(LogApplyResourceRow {
                    api_version: "v1".to_string(),
                    kind: "ConfigMap".to_string(),
                    namespace: Some("default".to_string()),
                    name: "atomic-outcome".to_string(),
                    uid: "atomic-outcome-uid".to_string(),
                    resource_version: 0,
                    data: json!({
                        "metadata": {
                            "name": "atomic-outcome",
                            "namespace": "default",
                            "uid": "atomic-outcome-uid"
                        }
                    }),
                    require_absent: true,
                    require_existing: false,
                    precondition_uid: None,
                    precondition_resource_version: None,
                    status_only: false,
                }),
            ])))
            .await
            .expect("transaction-derived outcome must not perform a separate observation");

        assert!(matches!(
            receipt.outcome(),
            klights_cluster_core::CommittedApplyOutcome::Visible { .. }
        ));
        assert!(
            reader
                .get_resource(ResourceGetRequest::new(
                    "v1",
                    "ConfigMap",
                    Some("default".to_string()),
                    "atomic-outcome",
                ))
                .await
                .unwrap()
                .is_some()
        );
    }

    fn snapshot_watch_event(event_id: i64, resource_version: i64, name: &str) -> LogApplyMutation {
        LogApplyMutation::PutWatchEvent(LogApplyWatchEventRow {
            event_id: Some(event_id),
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: name.to_string(),
            resource_version,
            event_type: "ADDED".to_string(),
            data: json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "namespace": "default",
                    "name": name,
                    "uid": format!("{name}-uid"),
                    "resourceVersion": resource_version.to_string()
                }
            }),
        })
    }

    #[tokio::test]
    async fn durable_history_and_allocator_preserve_event_order_and_empty_high_water() {
        let db = Datastore::new_in_memory()
            .await
            .expect("in-memory datastore");
        db.replace_replicated_resource_state(
            vec![
                SnapshotRestoreOperation::new(
                    100,
                    None,
                    vec![snapshot_watch_event(1, 100, "higher-rv")],
                ),
                SnapshotRestoreOperation::new(
                    50,
                    None,
                    vec![snapshot_watch_event(2, 50, "later-lower-rv")],
                ),
            ],
            100,
            Some(5),
            Some(Vec::new()),
            Some(crate::datastore::ReplicatedSnapshotMetadata {
                cluster_id: "history-cluster".to_string(),
                leader_epoch: 1,
                membership: crate::datastore::ReplicatedMembershipState::LegacyOmitted,
                command_codec_activation_version: None,
            }),
        )
        .await
        .expect("restore positioned history");
        let handle: DatastoreHandle = Arc::new(db.clone()) as Arc<dyn DatastoreBackend>;
        let history = DatastoreDurableWatchHistory::new(handle.clone());
        let allocator = DatastoreDurableAllocatorRead::new(handle.clone());
        let metadata = DatastoreClusterMetadataRead::new(handle);
        let request = WatchHistoryRequest::new(
            vec![DurableWatchTarget::namespaced_in_namespace(
                "v1",
                "ConfigMap",
                "default",
            )],
            WatchReplayPosition::default(),
            8,
        )
        .unwrap();
        let page = match history
            .replay_watch_history(request)
            .await
            .expect("positioned history")
        {
            WatchHistoryRead::Events(page) => page,
            WatchHistoryRead::Expired => panic!("fresh cursor must remain replayable"),
        };
        assert_eq!(
            page.events()
                .iter()
                .map(|event| (
                    event.position.event_id,
                    event.event.resource().resource_version
                ))
                .collect::<Vec<_>>(),
            vec![(1, 100), (2, 50)],
            "durable event ID, not public RV, orders positioned replay"
        );
        assert_eq!(page.next_position().event_id, 2);

        let empty = match history
            .replay_watch_history(
                WatchHistoryRequest::new(
                    vec![DurableWatchTarget::namespaced_in_namespace(
                        "v1",
                        "ConfigMap",
                        "default",
                    )],
                    page.next_position(),
                    8,
                )
                .unwrap(),
            )
            .await
            .expect("empty suffix")
        {
            WatchHistoryRead::Events(page) => page,
            WatchHistoryRead::Expired => panic!("exact suffix must remain replayable"),
        };
        assert!(empty.events().is_empty());
        assert_eq!(empty.next_position().event_id, 5);

        let state = allocator
            .read_allocator_state()
            .await
            .expect("allocator state");
        assert_eq!(state.position().resource_version, 100);
        assert_eq!(state.position().event_id, 5);
        assert_eq!(state.next_resource_version(), 101);
        assert_eq!(state.next_event_id(), 6);
        assert_eq!(
            metadata
                .read_cluster_metadata()
                .await
                .expect("cluster metadata")
                .membership(),
            &SnapshotMembership::AuthoritativeAbsent,
            "absent membership metadata must not become an explicit empty membership"
        );
    }

    #[tokio::test]
    async fn authoritative_restore_preserves_complete_recovery_state_exactly() {
        let db = Datastore::new_in_memory()
            .await
            .expect("in-memory datastore");
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "stale-destination",
            json!({"metadata": {"name": "stale-destination", "namespace": "default"}}),
        )
        .await
        .expect("seed divergent cluster row");
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:authoritative-restore-node-local-test",
        )
        .await
        .expect("open node-local store");
        node_local
            .record_owned_sandbox(
                "local-pod-uid",
                "default",
                "local-pod",
                "node-a",
                "local-sandbox",
                0,
            )
            .await
            .expect("seed node-local sandbox state");

        let handle: DatastoreHandle = Arc::new(db.clone()) as Arc<dyn DatastoreBackend>;
        let restore = DatastoreAuthoritativeSnapshotPersistence::new_for_test(handle.clone());
        let history = DatastoreDurableWatchHistory::new(handle.clone());
        let allocator = DatastoreDurableAllocatorRead::new(handle.clone());
        let metadata_reader = DatastoreClusterMetadataRead::new(handle.clone());
        let ledger = DatastoreCommittedRaftApply::new_for_test(handle);

        let resource_data = json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "namespace": "default",
                "name": "restored",
                "uid": "restored-uid",
                "resourceVersion": "7"
            },
            "data": {"state": "leader"}
        });
        let operations = vec![
            SnapshotRestoreOperation::new(
                7,
                None,
                vec![
                    LogApplyMutation::PutResource(LogApplyResourceRow {
                        api_version: "v1".to_string(),
                        kind: "ConfigMap".to_string(),
                        namespace: Some("default".to_string()),
                        name: "restored".to_string(),
                        uid: "restored-uid".to_string(),
                        resource_version: 7,
                        data: resource_data,
                        require_absent: false,
                        require_existing: false,
                        precondition_uid: None,
                        precondition_resource_version: None,
                        status_only: false,
                    }),
                    snapshot_watch_event(4, 7, "restored"),
                    LogApplyMutation::PutAppliedOutbox(LogApplyAppliedOutboxRow {
                        idempotency_key: "snapshot-ledger".to_string(),
                        subject_key: "v1/ConfigMap/default/restored/restored-uid".to_string(),
                        operation: "Update".to_string(),
                        first_seen_ms: 123,
                        applied_rv: Some(7),
                        result_proto: vec![9, 8, 7],
                        status_stamp: Some(44),
                    }),
                ],
            ),
            SnapshotRestoreOperation::new(
                6,
                None,
                vec![snapshot_watch_event(5, 6, "later-lower-rv")],
            ),
            SnapshotRestoreOperation::new(
                7,
                Some(OutboxStreamWatermark {
                    client_id: "worker-a".to_string(),
                    stream_id: 3,
                    stream_seq: 4,
                }),
                Vec::new(),
            ),
        ];
        let floors = vec![
            DurableReplayFloor::all(1, 1, true).unwrap(),
            DurableReplayFloor::cluster("v1", "ConfigMap", 2, 2, true).unwrap(),
            DurableReplayFloor::namespaced("v1", "ConfigMap", "default", 4, 4, true).unwrap(),
        ];
        let membership = ClusterMembership {
            cluster_id: "cluster-a".to_string(),
            voters: vec!["cp-1".to_string(), "cp-2".to_string()],
            term: 9,
            leader_hint: Some("https://cp-1:7446".to_string()),
        };
        let snapshot = AuthoritativeSnapshot::try_new(
            operations,
            Some(WatchReplayPosition {
                resource_version: 7,
                event_id: 9,
                resource_version_filter_through_event_id: 0,
            }),
            Some(floors.clone()),
            ClusterMetadata {
                cluster_id: "cluster-a".to_string(),
                leader_epoch: 3,
                current_rv: 7,
            },
            SnapshotMembership::Present(membership.clone()),
        )
        .expect("valid authoritative snapshot");
        restore
            .restore_authoritative_snapshot(snapshot)
            .await
            .expect("atomic authoritative restore");

        assert!(
            db.get_resource("v1", "ConfigMap", Some("default"), "stale-destination")
                .await
                .unwrap()
                .is_none(),
            "authoritative restore must remove divergent cluster rows"
        );
        assert_eq!(
            db.get_resource("v1", "ConfigMap", Some("default"), "restored")
                .await
                .unwrap()
                .unwrap()
                .resource_version,
            7
        );
        assert_eq!(
            node_local
                .get_pod_runtime("local-pod-uid")
                .await
                .unwrap()
                .and_then(|row| row.sandbox_id),
            Some("local-sandbox".to_string()),
            "cluster restore must preserve node-local runtime state"
        );

        let state = allocator.read_allocator_state().await.unwrap();
        assert_eq!(state.position().resource_version, 7);
        assert_eq!(state.position().event_id, 9);
        assert_eq!(state.next_resource_version(), 8);
        assert_eq!(state.next_event_id(), 10);
        assert_eq!(history.list_replay_floors().await.unwrap(), floors);
        let restored_metadata = metadata_reader.read_cluster_metadata().await.unwrap();
        assert_eq!(
            restored_metadata.metadata(),
            &ClusterMetadata {
                cluster_id: "cluster-a".to_string(),
                leader_epoch: 3,
                current_rv: 7,
            }
        );
        assert_eq!(
            restored_metadata.membership(),
            &SnapshotMembership::Present(membership)
        );

        let applied = ledger
            .get_applied_outbox(AppliedOutboxLookup::new("snapshot-ledger"))
            .await
            .unwrap()
            .expect("snapshot outbox ledger row");
        assert_eq!(applied.applied_rv, Some(7));
        assert_eq!(applied.result_proto, vec![9, 8, 7]);
        assert_eq!(applied.status_stamp, Some(44));
        assert_eq!(
            ledger.list_outbox_watermarks().await.unwrap(),
            vec![OutboxStreamWatermark {
                client_id: "worker-a".to_string(),
                stream_id: 3,
                stream_seq: 4,
            }]
        );

        let replay = match history
            .replay_watch_history(
                WatchHistoryRequest::new(
                    vec![DurableWatchTarget::namespaced_in_namespace(
                        "v1",
                        "ConfigMap",
                        "default",
                    )],
                    WatchReplayPosition {
                        resource_version: 7,
                        event_id: 4,
                        resource_version_filter_through_event_id: 0,
                    },
                    8,
                )
                .unwrap(),
            )
            .await
            .unwrap()
        {
            WatchHistoryRead::Events(page) => page,
            WatchHistoryRead::Expired => panic!("restored exact cursor must remain valid"),
        };
        assert_eq!(replay.events().len(), 1);
        assert_eq!(replay.events()[0].position.event_id, 5);
        assert_eq!(replay.events()[0].event.resource().resource_version, 6);

        let created = db
            .create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "after-restore",
                json!({"metadata": {"name": "after-restore", "namespace": "default"}}),
            )
            .await
            .expect("first post-restore allocation");
        assert_eq!(created.resource_version, 8);
        assert_eq!(
            db.current_watch_replay_position().await.unwrap().event_id,
            10
        );
    }

    #[tokio::test]
    async fn all_namespace_pages_use_composite_cursor_and_pinned_history() {
        let db = Datastore::new_in_memory().await.unwrap();
        for (namespace, marker) in [("tenant-a", "a"), ("tenant-b", "b")] {
            db.create_resource(
                "v1", "ConfigMap", Some(namespace), "same-name",
                json!({"metadata": {"name": "same-name", "namespace": namespace}, "data": {"marker": marker}}),
            ).await.unwrap();
        }
        let handle: DatastoreHandle = Arc::new(db.clone()) as Arc<dyn DatastoreBackend>;
        let reader = DatastoreClusterResourceRead::new(handle);
        let first = reader
            .list_resources(ResourceListRequest::new(
                "v1",
                "ConfigMap",
                ResourceCollectionScope::AllNamespaces,
                ResourceListQuery::try_new(None, None, Some(1), None, ResourceVersionMatch::Any)
                    .unwrap(),
            ))
            .await
            .unwrap();
        assert!(matches!(first, ResourceListRead::Current(_)));
        assert_eq!(first.items()[0].namespace.as_deref(), Some("tenant-a"));
        let cursor = first.continuation().cloned().expect("composite cursor");
        assert_eq!(cursor.after().namespace(), Some("tenant-a"));

        db.create_resource(
            "v1",
            "ConfigMap",
            Some("tenant-aa"),
            "same-name",
            json!({"metadata": {"name": "same-name", "namespace": "tenant-aa"}}),
        )
        .await
        .expect("concurrent mutation after first page");

        let second = reader
            .list_resources(ResourceListRequest::new(
                "v1",
                "ConfigMap",
                ResourceCollectionScope::AllNamespaces,
                ResourceListQuery::try_new(
                    None,
                    None,
                    Some(1),
                    Some(cursor.clone()),
                    ResourceVersionMatch::Exact(cursor.snapshot().resource_version()),
                )
                .unwrap(),
            ))
            .await
            .unwrap();
        assert!(matches!(second, ResourceListRead::Historical(_)));
        assert_eq!(second.items()[0].namespace.as_deref(), Some("tenant-b"));
        assert_eq!(second.snapshot(), Some(cursor.snapshot()));
    }

    #[tokio::test]
    async fn hostile_list_limit_is_semantically_unbounded_without_process_sized_reserve() {
        let (_db, reader) = seeded_reader().await;
        let page = reader
            .list_resources(ResourceListRequest::new(
                "v1",
                "ConfigMap",
                ResourceCollectionScope::Namespace("tenant-a".into()),
                ResourceListQuery::try_new(
                    None,
                    None,
                    Some(i64::MAX),
                    None,
                    ResourceVersionMatch::Any,
                )
                .unwrap(),
            ))
            .await
            .unwrap();
        assert_eq!(page.items().len(), 3);
        assert!(page.continuation().is_none());
    }

    #[tokio::test]
    async fn atomic_metadata_reads_reject_malformed_or_incomplete_observations() {
        let db = Datastore::new_in_memory().await.unwrap();
        db.set_klights_meta(crate::bootstrap::cluster_meta::KEY_CLUSTER_ID, "cluster-a")
            .await
            .unwrap();
        db.set_klights_meta(
            crate::bootstrap::cluster_meta::KEY_LEADER_EPOCH,
            "not-a-number",
        )
        .await
        .unwrap();
        let handle: DatastoreHandle = Arc::new(db.clone()) as Arc<dyn DatastoreBackend>;
        let reader = DatastoreClusterMetadataRead::new(handle.clone());
        assert!(
            reader
                .read_cluster_metadata()
                .await
                .unwrap_err()
                .to_string()
                .contains("leader_epoch")
        );

        db.set_klights_meta(crate::bootstrap::cluster_meta::KEY_LEADER_EPOCH, "0")
            .await
            .unwrap();
        db.set_klights_meta(crate::bootstrap::cluster_meta::KEY_RAFT_VOTERS, "[]")
            .await
            .unwrap();
        assert!(
            reader
                .read_cluster_metadata()
                .await
                .unwrap_err()
                .to_string()
                .contains("incomplete")
        );
        db.set_klights_meta(crate::bootstrap::cluster_meta::KEY_RAFT_TERM, "0")
            .await
            .unwrap();
        db.set_klights_meta(crate::bootstrap::cluster_meta::KEY_RAFT_LEADER_HINT, "")
            .await
            .unwrap();
        assert!(
            reader
                .read_cluster_metadata()
                .await
                .unwrap_err()
                .to_string()
                .contains("voter set")
        );

        let allocator = DatastoreDurableAllocatorRead::new(handle);
        assert_eq!(
            allocator
                .read_allocator_state()
                .await
                .unwrap()
                .next_resource_version(),
            1
        );
    }

    #[tokio::test]
    async fn explicit_absent_membership_clears_stale_destination_keys() {
        let db = Datastore::new_in_memory().await.unwrap();
        for (key, value) in [
            (crate::bootstrap::cluster_meta::KEY_CLUSTER_ID, "cluster-a"),
            (crate::bootstrap::cluster_meta::KEY_LEADER_EPOCH, "0"),
            (
                crate::bootstrap::cluster_meta::KEY_RAFT_VOTERS,
                "[\"cp-1\"]",
            ),
            (crate::bootstrap::cluster_meta::KEY_RAFT_TERM, "4"),
            (crate::bootstrap::cluster_meta::KEY_RAFT_LEADER_HINT, "cp-1"),
        ] {
            db.set_klights_meta(key, value).await.unwrap();
        }
        let handle: DatastoreHandle = Arc::new(db.clone()) as Arc<dyn DatastoreBackend>;
        let restore = DatastoreAuthoritativeSnapshotPersistence::new_for_test(handle);
        restore
            .restore_authoritative_snapshot(
                AuthoritativeSnapshot::try_new(
                    Vec::new(),
                    Some(WatchReplayPosition::default()),
                    Some(Vec::new()),
                    ClusterMetadata {
                        cluster_id: "cluster-a".into(),
                        leader_epoch: 0,
                        current_rv: 0,
                    },
                    SnapshotMembership::AuthoritativeAbsent,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        for key in [
            crate::bootstrap::cluster_meta::KEY_RAFT_VOTERS,
            crate::bootstrap::cluster_meta::KEY_RAFT_TERM,
            crate::bootstrap::cluster_meta::KEY_RAFT_LEADER_HINT,
        ] {
            assert_eq!(db.get_klights_meta(key).await.unwrap(), None);
        }
    }

    #[derive(Default)]
    struct CapturedPages {
        pages: Vec<SnapshotCapturePage>,
        headers: Vec<SnapshotCaptureHeader>,
    }

    impl TestPageSink for CapturedPages {
        fn begin_capture(&mut self, header: &SnapshotCaptureHeader) -> TestSinkFuture<'_> {
            assert!(
                self.pages.is_empty(),
                "begin_capture must precede every page"
            );
            self.headers.push(header.clone());
            Box::pin(async { Ok(()) })
        }

        fn push_page(&mut self, page: SnapshotCapturePage) -> TestSinkFuture<'_> {
            assert_eq!(self.headers.len(), 1, "capture must begin exactly once");
            self.pages.push(page);
            Box::pin(async { Ok(()) })
        }
    }

    struct BlockingCaptureSink {
        reached: Arc<tokio::sync::Notify>,
        resume: Arc<tokio::sync::Notify>,
        blocked: bool,
    }

    impl TestPageSink for BlockingCaptureSink {
        fn begin_capture(&mut self, _header: &SnapshotCaptureHeader) -> TestSinkFuture<'_> {
            Box::pin(async { Ok(()) })
        }

        fn push_page(&mut self, _page: SnapshotCapturePage) -> TestSinkFuture<'_> {
            if self.blocked {
                return Box::pin(async { Ok(()) });
            }
            self.blocked = true;
            let reached = self.reached.clone();
            let resume = self.resume.clone();
            Box::pin(async move {
                reached.notify_one();
                resume.notified().await;
                Ok(())
            })
        }
    }

    #[derive(Default)]
    struct RejectBeginCapture {
        pages: usize,
    }

    impl TestPageSink for RejectBeginCapture {
        fn begin_capture(&mut self, _header: &SnapshotCaptureHeader) -> TestSinkFuture<'_> {
            Box::pin(async {
                Err(SnapshotPersistenceError::UnsupportedMode {
                    message: "test rejected capture header".to_string(),
                })
            })
        }

        fn push_page(&mut self, _page: SnapshotCapturePage) -> TestSinkFuture<'_> {
            self.pages += 1;
            Box::pin(async { Ok(()) })
        }
    }

    fn snapshot_from_capture(
        header: &SnapshotCaptureHeader,
        pages: &[SnapshotCapturePage],
    ) -> AuthoritativeSnapshot {
        let current_rv = header.metadata().current_rv;
        let mut operations = Vec::new();
        let mut floors = Vec::new();
        for page in pages {
            if let Some(rows) = page.operations() {
                operations.extend_from_slice(rows);
            } else if let Some(rows) = page.applied_outbox() {
                operations.extend(rows.iter().cloned().map(|row| {
                    SnapshotRestoreOperation::new(
                        current_rv,
                        None,
                        vec![LogApplyMutation::PutAppliedOutbox(row)],
                    )
                }));
            } else if let Some(rows) = page.outbox_watermarks() {
                operations.extend(rows.iter().cloned().map(|outbox_watermark| {
                    SnapshotRestoreOperation::new(current_rv, Some(outbox_watermark), Vec::new())
                }));
            } else if let Some(rows) = page.replay_floors() {
                floors.extend_from_slice(rows);
            }
        }
        AuthoritativeSnapshot::try_new(
            operations,
            Some(header.position()),
            Some(floors),
            header.metadata().clone(),
            header.membership().clone(),
        )
        .expect("captured pages form one valid authoritative snapshot")
    }

    #[tokio::test]
    async fn capture_releases_mutation_fence_before_consumer_accepts_first_page() {
        let (_root, db) = persistent_snapshot_store().await;
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "fenced",
            json!({"metadata": {"name": "fenced", "namespace": "default"}}),
        )
        .await
        .unwrap();
        db.set_klights_meta(
            crate::bootstrap::cluster_meta::KEY_CLUSTER_ID,
            "capture-fence-cluster",
        )
        .await
        .unwrap();
        db.set_klights_meta(crate::bootstrap::cluster_meta::KEY_LEADER_EPOCH, "1")
            .await
            .unwrap();
        let handle: DatastoreHandle = Arc::new(db);
        let reached = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        let reached_wait = reached.notified();
        tokio::pin!(reached_wait);

        let capture_handle = handle.clone();
        let capture_reached = reached.clone();
        let capture_resume = resume.clone();
        let capture_task = tokio::spawn(async move {
            let capture = DatastoreAuthoritativeSnapshotPersistence::new_for_test(capture_handle);
            let mut sink = BlockingCaptureSink {
                reached: capture_reached,
                resume: capture_resume,
                blocked: false,
            };
            capture.collect_snapshot_pages(&mut sink).await
        });
        reached_wait.await;

        let mutation_handle = handle.clone();
        let mutation_fence_task = tokio::spawn(async move {
            crate::datastore::DatastoreBackend::acquire_snapshot_mutation_fence(
                mutation_handle.as_ref(),
            )
            .await
            .unwrap()
            .expect("sqlite supplies a mutation fence")
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), mutation_fence_task)
            .await
            .expect("capture must release the mutation fence before consumer backpressure")
            .unwrap();

        resume.notify_one();
        capture_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn capture_keyset_pages_every_unbounded_family_without_gaps_or_duplicates() {
        let db = Datastore::new_in_memory().await.unwrap();
        let result_proto = crate::replication::outbox_response_wire::encode_outbox_response(
            &klights_cluster_core::command::StorageResponse::Ack {
                resource_version: 1,
            },
        )
        .unwrap();
        let mutations = (0..=klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE)
            .map(|index| {
                LogApplyMutation::PutAppliedOutbox(LogApplyAppliedOutboxRow {
                    idempotency_key: format!("capture-page-{index:04}"),
                    subject_key: format!("subject-{index:04}"),
                    operation: "Update".into(),
                    first_seen_ms: index as i64,
                    applied_rv: Some(1),
                    result_proto: result_proto.clone(),
                    status_stamp: None,
                })
            })
            .collect();
        let mut operations = vec![SnapshotRestoreOperation::new(1, None, mutations)];
        operations.extend(
            (0..=klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE).map(|index| {
                SnapshotRestoreOperation::new(
                    1,
                    Some(OutboxStreamWatermark {
                        client_id: format!("worker-{index:04}"),
                        stream_id: 1,
                        stream_seq: 1,
                    }),
                    Vec::new(),
                )
            }),
        );
        let floors = (0..=klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE)
            .map(|index| crate::datastore::WatchReplayFloor {
                api_version: "v1".into(),
                kind: "ConfigMap".into(),
                namespace_key: format!("ns-{index:04}"),
                floor_resource_version: 1,
                floor_event_id: 0,
                position_is_exact: true,
            })
            .collect();
        db.replace_replicated_resource_state(
            operations,
            1,
            Some(0),
            Some(floors),
            Some(crate::datastore::ReplicatedSnapshotMetadata {
                cluster_id: "capture-page-cluster".into(),
                leader_epoch: 1,
                membership: crate::datastore::ReplicatedMembershipState::AuthoritativeAbsent,
                command_codec_activation_version: None,
            }),
        )
        .await
        .unwrap();
        let oversized =
            std::num::NonZeroUsize::new(klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE + 1)
                .unwrap();
        assert!(
            db.list_outbox_stream_watermarks_paged(None, oversized)
                .await
                .is_err()
        );
        assert!(
            db.list_watch_replay_floors_paged(None, oversized)
                .await
                .is_err()
        );
        let handle: DatastoreHandle = Arc::new(db);
        let capture = DatastoreAuthoritativeSnapshotPersistence::new_for_test(handle);
        let mut pages = CapturedPages::default();
        let header = capture.collect_snapshot_pages(&mut pages).await.unwrap();

        let expected_page_lengths = vec![klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE, 1];
        assert_eq!(
            pages
                .pages
                .iter()
                .filter_map(|page| page.applied_outbox().map(<[_]>::len))
                .collect::<Vec<_>>(),
            expected_page_lengths
        );
        assert_eq!(
            pages
                .pages
                .iter()
                .filter_map(|page| page.outbox_watermarks().map(<[_]>::len))
                .collect::<Vec<_>>(),
            expected_page_lengths
        );
        assert_eq!(
            pages
                .pages
                .iter()
                .filter_map(|page| page.replay_floors().map(<[_]>::len))
                .collect::<Vec<_>>(),
            expected_page_lengths
        );
        let watermark_ids = pages
            .pages
            .iter()
            .filter_map(SnapshotCapturePage::outbox_watermarks)
            .flatten()
            .map(|row| row.client_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(watermark_ids.len(), 513);
        assert!(watermark_ids.windows(2).all(|pair| pair[0] < pair[1]));
        let floor_namespaces = pages
            .pages
            .iter()
            .filter_map(SnapshotCapturePage::replay_floors)
            .flatten()
            .map(|row| match row.target() {
                DurableReplayTarget::Namespaced { namespace, .. } => namespace.as_str(),
                _ => panic!("fixture contains namespaced floors only"),
            })
            .collect::<Vec<_>>();
        assert_eq!(floor_namespaces.len(), 513);
        assert!(floor_namespaces.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(pages.headers.len(), 1);
        let begun = &pages.headers[0];
        assert_eq!(
            begun.command_codec_activation_version(),
            header.command_codec_activation_version()
        );
        assert_eq!(begun.position(), header.position());
        assert_eq!(begun.metadata(), header.metadata());
        assert_eq!(begun.membership(), header.membership());

        let mut rejected = RejectBeginCapture::default();
        let error = capture
            .collect_snapshot_pages(&mut rejected)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("test rejected capture header"));
        assert_eq!(rejected.pages, 0, "begin failure must prevent every page");
    }

    #[tokio::test]
    async fn capture_restores_every_durable_family_and_preserves_outbox_dedupe() {
        let source = Datastore::new_in_memory().await.unwrap();
        let result_proto = crate::replication::outbox_response_wire::encode_outbox_response(
            &klights_cluster_core::command::StorageResponse::Ack {
                resource_version: 7,
            },
        )
        .unwrap();
        let outbox = LogApplyAppliedOutboxRow {
            idempotency_key: "captured-dedupe".into(),
            subject_key: "v1/ConfigMap/default/captured/captured-uid".into(),
            operation: "Update".into(),
            first_seen_ms: 123,
            applied_rv: Some(7),
            result_proto,
            status_stamp: None,
        };
        let watermark = OutboxStreamWatermark {
            client_id: "worker-a".into(),
            stream_id: 2,
            stream_seq: 1,
        };
        let resource_data = json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "namespace": "default",
                "name": "captured",
                "uid": "captured-uid",
                "resourceVersion": "7"
            }
        });
        let operation = SnapshotRestoreOperation::new(
            7,
            Some(watermark.clone()),
            vec![
                LogApplyMutation::PutResource(LogApplyResourceRow {
                    api_version: "v1".into(),
                    kind: "ConfigMap".into(),
                    namespace: Some("default".into()),
                    name: "captured".into(),
                    uid: "captured-uid".into(),
                    resource_version: 7,
                    data: resource_data.clone(),
                    require_absent: false,
                    require_existing: false,
                    precondition_uid: None,
                    precondition_resource_version: None,
                    status_only: false,
                }),
                LogApplyMutation::PutResource(LogApplyResourceRow {
                    api_version: "v1".into(),
                    kind: "Node".into(),
                    namespace: None,
                    name: "cp-1".into(),
                    uid: "cp-1-uid".into(),
                    resource_version: 7,
                    data: json!({
                        "apiVersion": "v1",
                        "kind": "Node",
                        "metadata": {
                            "name": "cp-1",
                            "uid": "cp-1-uid",
                            "resourceVersion": "7"
                        }
                    }),
                    require_absent: false,
                    require_existing: false,
                    precondition_uid: None,
                    precondition_resource_version: None,
                    status_only: false,
                }),
                LogApplyMutation::PutWatchEvent(LogApplyWatchEventRow {
                    event_id: Some(4),
                    api_version: "v1".into(),
                    kind: "ConfigMap".into(),
                    namespace: Some("default".into()),
                    name: "captured".into(),
                    resource_version: 7,
                    event_type: "ADDED".into(),
                    data: resource_data,
                }),
                LogApplyMutation::PutNodeSubnet(LogApplyNodeSubnetRow {
                    node_name: "cp-1".into(),
                    subnet: "10.42.1.0/24".into(),
                    subnet_base_int: u32::from(std::net::Ipv4Addr::new(10, 42, 1, 0)),
                    gateway_ip: "10.42.1.1".into(),
                    node_ip: "10.0.0.1".into(),
                    mode: "root".into(),
                    hostport_range: None,
                }),
                LogApplyMutation::PutNodeDataplane(LogApplyNodeDataplaneRow {
                    node_name: "cp-1".into(),
                    mode: "root".into(),
                    encryption: "enabled".into(),
                    public_key: Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".into()),
                    endpoint: "10.0.0.1".into(),
                    port: Some(51820),
                }),
                LogApplyMutation::PutPodCleanupIntent(LogApplyPodCleanupIntentRow {
                    node_name: "cp-1".into(),
                    namespace: "default".into(),
                    pod_name: "cleanup-pod".into(),
                    pod_uid: "cleanup-uid".into(),
                    reason: "NodeLost".into(),
                    resource_version: 7,
                    created_at_ms: 456,
                    pod_data: json!({"metadata": {"name": "cleanup-pod", "uid": "cleanup-uid"}}),
                }),
                LogApplyMutation::PutAppliedOutbox(outbox.clone()),
            ],
        );
        let floors = vec![
            DurableReplayFloor::all(1, 1, true).unwrap(),
            DurableReplayFloor::namespaced("v1", "ConfigMap", "default", 4, 4, true).unwrap(),
        ];
        let membership = ClusterMembership {
            cluster_id: "capture-cluster".into(),
            voters: vec!["cp-1".into()],
            term: 3,
            leader_hint: Some("cp-1".into()),
        };
        source
            .replace_replicated_resource_state(
                vec![operation],
                7,
                Some(9),
                Some(
                    floors
                        .iter()
                        .cloned()
                        .map(|floor| {
                            let (target, floor_resource_version, floor_event_id, position_is_exact) =
                                floor.into_parts();
                            let (api_version, kind, namespace_key) = match target {
                                DurableReplayTarget::All => ("*".into(), "*".into(), "*".into()),
                                DurableReplayTarget::Cluster { api_version, kind } => {
                                    (api_version, kind, "#cluster".into())
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
                        .collect(),
                ),
                Some(crate::datastore::ReplicatedSnapshotMetadata {
                    cluster_id: "capture-cluster".into(),
                    leader_epoch: 2,
                    membership: crate::datastore::ReplicatedMembershipState::Present(
                        membership.clone(),
                    ),
                    command_codec_activation_version: None,
                }),
            )
            .await
            .unwrap();

        let source_handle: DatastoreHandle = Arc::new(source.clone());
        let capture = DatastoreAuthoritativeSnapshotPersistence::new_for_test(source_handle);
        let mut pages = CapturedPages::default();
        let header = capture.collect_snapshot_pages(&mut pages).await.unwrap();
        assert_eq!(
            pages
                .pages
                .iter()
                .map(SnapshotCapturePage::kind)
                .collect::<Vec<_>>(),
            vec![
                klights_cluster_store::SnapshotCapturePageKind::Commits,
                klights_cluster_store::SnapshotCapturePageKind::OutboxWatermarks,
                klights_cluster_store::SnapshotCapturePageKind::AppliedOutbox,
                klights_cluster_store::SnapshotCapturePageKind::ReplayFloors,
            ],
            "capture families must stay in streaming JSON order"
        );

        let destination = Datastore::new_in_memory().await.unwrap();
        destination
            .create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "divergent",
                json!({"metadata": {"name": "divergent", "namespace": "default"}}),
            )
            .await
            .unwrap();
        let destination_handle: DatastoreHandle = Arc::new(destination.clone());
        let restore =
            DatastoreAuthoritativeSnapshotPersistence::new_for_test(destination_handle.clone());
        restore
            .restore_authoritative_snapshot(snapshot_from_capture(&header, &pages.pages))
            .await
            .unwrap();

        assert!(
            destination
                .get_resource("v1", "ConfigMap", Some("default"), "divergent")
                .await
                .unwrap()
                .is_none()
        );
        let restored_resource = destination
            .get_resource("v1", "ConfigMap", Some("default"), "captured")
            .await
            .unwrap()
            .unwrap();
        let source_resource = source
            .get_resource("v1", "ConfigMap", Some("default"), "captured")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            restored_resource.resource_version,
            source_resource.resource_version
        );
        assert_eq!(restored_resource.data, source_resource.data);
        assert_eq!(
            destination.get_node_subnet("cp-1").await.unwrap(),
            source.get_node_subnet("cp-1").await.unwrap()
        );
        assert_eq!(
            destination.get_node_dataplane("cp-1").await.unwrap(),
            source.get_node_dataplane("cp-1").await.unwrap()
        );
        assert_eq!(
            destination
                .list_pod_cleanup_intents_for_node("cp-1")
                .await
                .unwrap(),
            source
                .list_pod_cleanup_intents_for_node("cp-1")
                .await
                .unwrap()
        );
        let destination_history = DatastoreDurableWatchHistory::new(destination_handle.clone());
        assert_eq!(
            destination_history.list_replay_floors().await.unwrap(),
            floors
        );
        let destination_allocator = DatastoreDurableAllocatorRead::new(destination_handle.clone());
        assert_eq!(
            destination_allocator
                .read_allocator_state()
                .await
                .unwrap()
                .position(),
            header.position()
        );
        let destination_metadata = DatastoreClusterMetadataRead::new(destination_handle.clone());
        assert_eq!(
            destination_metadata
                .read_cluster_metadata()
                .await
                .unwrap()
                .membership(),
            &SnapshotMembership::Present(membership)
        );
        let destination_ledger = DatastoreCommittedRaftApply::new_for_test(destination_handle);
        assert_eq!(
            destination_ledger
                .get_applied_outbox(AppliedOutboxLookup::new("captured-dedupe"))
                .await
                .unwrap(),
            Some(outbox.clone())
        );
        assert_eq!(
            destination_ledger.list_outbox_watermarks().await.unwrap(),
            vec![watermark]
        );

        let before = destination_ledger.current_apply_position().await.unwrap();
        let duplicate = destination_ledger
            .apply_committed_raft(CommittedRaftApplyRequest::new(
                crate::replication::log_apply_wire::test_live_commit(
                    0,
                    vec![LogApplyMutation::PutAppliedOutbox(outbox)],
                ),
            ))
            .await
            .unwrap();
        assert!(matches!(
            duplicate.outcome(),
            klights_cluster_core::CommittedApplyOutcome::NoPublicChange {
                reason: NoPublicChangeReason::DuplicateIdempotencyKey,
                ..
            }
        ));
        assert_eq!(
            destination_ledger.current_apply_position().await.unwrap(),
            before
        );
    }

    #[tokio::test]
    async fn fenced_capture_streams_bounded_commits_ledgers_and_watermarks() {
        let (db, apply, _reader) = committed_store().await;
        db.set_klights_meta(crate::bootstrap::cluster_meta::KEY_CLUSTER_ID, "cluster-a")
            .await
            .unwrap();
        db.set_klights_meta(crate::bootstrap::cluster_meta::KEY_LEADER_EPOCH, "0")
            .await
            .unwrap();
        db.create_resource("v1", "Pod", Some("default"), "adapter-status", json!({
            "metadata": {"name": "adapter-status", "namespace": "default", "uid": "adapter-status-uid"},
            "status": {"phase": "Pending"}
        })).await.unwrap();
        apply
            .apply_committed_raft(CommittedRaftApplyRequest::new(status_commit(
                "capture-ledger",
                "fresh",
                10,
                1,
            )))
            .await
            .unwrap();
        let handle: DatastoreHandle = Arc::new(db) as Arc<dyn DatastoreBackend>;
        let capture = DatastoreAuthoritativeSnapshotPersistence::new_for_test(handle);
        let mut pages = CapturedPages::default();
        let header = capture.collect_snapshot_pages(&mut pages).await.unwrap();
        assert_eq!(header.metadata().cluster_id, "cluster-a");
        assert!(
            pages
                .pages
                .iter()
                .all(|page| page.len() <= klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE)
        );
        assert!(
            pages
                .pages
                .iter()
                .any(|page| page.applied_outbox().is_some_and(|rows| rows
                    .iter()
                    .any(|row| row.idempotency_key == "capture-ledger")))
        );
        assert!(pages.pages.iter().any(|page| {
            page.outbox_watermarks()
                .is_some_and(|rows| rows.iter().any(|row| row.stream_seq == 1))
        }));
        assert!(pages.pages.iter().all(|page| {
            page.operations().is_none_or(|operations| {
                operations.iter().all(|operation| {
                    operation.outbox_watermark().is_none()
                        && operation.mutations().iter().all(|mutation| {
                            !matches!(
                                mutation,
                                klights_cluster_core::LogApplyMutation::PutAppliedOutbox(_)
                            )
                        })
                })
            })
        }));
    }
}
