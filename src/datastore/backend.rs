//! `DatastoreBackend` is the runtime contract every higher-level klights
//! component depends on for state. The trait is SQL-free; backend
//! implementations live in sibling modules (`sqlite/` today, additional
//! backends slot in alongside).

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::broadcast;

use crate::datastore::command::StorageCommand;
#[cfg(test)]
use crate::watch::{WatchEvent, WatchReceiver};
use crate::watch::{WatchSignal, WatchTopic};

#[cfg(test)]
use super::command::CommandMeta;
#[cfg(test)]
use super::types::PendingWatchEvent;
#[cfg(test)]
use super::types::ReplicatedCreateOptions;
use super::types::{
    AppliedOutboxRecord, CatchUpResource, ListPageRequest, NodeSubnet, PatchKind, PodCleanupIntent,
    PodEndpointEvent, PodEndpointRow, PodNetworkEndpoint, PodSlotAdmissionEvent,
    PodSlotAdmissionResult, PodWorkqueueEntry, PodWorkqueueKind, PositionedWatchReplayRead,
    RawWatchEvent, ReplicatedSnapshotMetadata, Resource, ResourceBatchOperation, ResourceList,
    ResourceListQuery, ResourcePatchRequest, ResourcePreconditions, SandboxRef, SnapshotAtRv,
    WatchReplayFloor, WatchReplayPosition, WatchReplayRead, WatchTarget,
};

/// Exclusive guard held while a logical snapshot walks multiple bounded read
/// pages. Backends without this coordination return `None`.
pub struct SnapshotExclusiveFence {
    _guard: tokio::sync::OwnedRwLockWriteGuard<()>,
}

impl SnapshotExclusiveFence {
    pub(crate) fn new(guard: tokio::sync::OwnedRwLockWriteGuard<()>) -> Self {
        Self { _guard: guard }
    }
}

/// Shared guard held by authoritative Raft state-machine mutations so apply
/// and install cannot overlap an exclusive snapshot capture.
pub struct SnapshotMutationFence {
    _guard: tokio::sync::OwnedRwLockReadGuard<()>,
}

impl SnapshotMutationFence {
    pub(crate) fn new(guard: tokio::sync::OwnedRwLockReadGuard<()>) -> Self {
        Self { _guard: guard }
    }
}

/// `DatastoreBackend` is the runtime contract. Every state operation goes
/// through this trait.
///
/// **Phase 3 will add:** `async fn snapshot(&self) -> Result<SnapshotHandle>`
/// for Raft FSM log compaction. SQLite impls via `online_backup`; KV impls
/// (redb, etc.) via MVCC reader. Not on the trait today because no caller
/// exists.
#[async_trait]
pub trait DatastoreBackend: Send + Sync {
    async fn acquire_snapshot_exclusive_fence(&self) -> Result<Option<SnapshotExclusiveFence>> {
        Ok(None)
    }

    async fn acquire_snapshot_mutation_fence(&self) -> Result<Option<SnapshotMutationFence>> {
        Ok(None)
    }

    /// Release backend-specific resources (file locks, connections, etc.)
    /// after graceful shutdown work is complete.  No-op by default.
    fn close(&self) {}

    /// Late-bind a `RaftProposer` so mutating methods can route writes
    /// through openraft consensus when this backend is a
    /// `ReplicatedDatastore` in `ReplicationMode::Raft`. Default impl is a
    /// no-op so non-replicated backends (sqlite, redb) ignore it; only
    /// `ReplicatedDatastore` actually stores the handle. The RaftNode is
    /// constructed after the datastore handle, so this attach happens
    /// once at boot in `bootstrap::phases::datastore::open_leader`.
    fn attach_raft_proposer(
        &self,
        _proposer: std::sync::Arc<dyn crate::datastore::replicated::RaftProposer>,
    ) {
    }

    fn subscribe_watch_signals(&self, topic: WatchTopic) -> broadcast::Receiver<WatchSignal>;

    #[cfg(test)]
    fn subscribe_watch(&self, topic: WatchTopic) -> broadcast::Receiver<WatchEvent>;

    #[cfg(test)]
    fn subscribe_watch_many(&self, topics: Vec<WatchTopic>) -> WatchReceiver;

    #[cfg(test)]
    fn broadcast_watch_event(&self, pending: PendingWatchEvent);

    /// TO-BE-CLEANUP: legacy replicated StorageCommand apply test support.
    ///
    /// Apply a replicated command locally without going through role-based
    /// public write admission.  Leaders use this for forwarded writes after
    /// bootstrap-token validation; replicas use it for snapshot and stream apply.
    #[cfg(test)]
    async fn apply_replicated_command(
        &self,
        command: StorageCommand,
        meta: CommandMeta,
    ) -> Result<()> {
        crate::datastore::replicated::apply_command_to_backend(self, command, meta).await
    }

    /// Atomically replace Kubernetes resource tables from a full leader snapshot.
    ///
    /// This is used during replica bootstrap before local API/kubelet work starts.
    /// It must not go through public write admission or forwarding, and it must
    /// preserve node-local tables such as pod sandboxes, pod networks, pod
    /// endpoints, and pod workqueue rows. When `metadata` is present, the
    /// backend must persist the leader cluster identity in the same transaction
    /// so a promoted replica restarts with the original cluster id.
    async fn replace_replicated_resource_state(
        &self,
        entries: Vec<crate::log_apply::LogApplyCommit>,
        current_rv: i64,
        watch_event_high_water: Option<i64>,
        watch_replay_floors: Option<Vec<WatchReplayFloor>>,
        metadata: Option<ReplicatedSnapshotMetadata>,
    ) -> Result<()> {
        let _ = (
            entries,
            current_rv,
            watch_event_high_water,
            watch_replay_floors,
            metadata,
        );
        Err(anyhow::anyhow!(
            "backend does not support atomic replicated resource-state replacement"
        ))
    }

    /// Apply one committed logical datastore delta from the leader commit log.
    ///
    /// This is a private replication/consensus surface. It must replay exact
    /// leader-committed rows and metadata without invoking public Kubernetes
    /// create/update/delete semantics, UID generation, local preconditions, or
    /// follower read/write admission.
    async fn apply_log_apply_commit(&self, commit: crate::log_apply::LogApplyCommit) -> Result<()> {
        let _ = commit;
        Err(anyhow::anyhow!(
            "backend does not support log-apply commit replay"
        ))
    }

    /// Apply one committed raft log-apply entry and return the state-machine
    /// result that `client_write` observes. This has no default fallback to
    /// `apply_log_apply_commit`: raft apply must preserve terminal rejection
    /// results without aborting learner catch-up.
    async fn apply_raft_log_apply_commit(
        &self,
        commit: crate::log_apply::LogApplyCommit,
    ) -> Result<crate::datastore::raft::types::StorageCommandResult>;

    /// Append one committed log-apply entry to the backend-local durable log.
    /// T3: `append_log_apply_entry`, `list_log_apply_entries_after`,
    /// `save_log_apply_checkpoint`, `load_log_apply_checkpoint` removed.
    /// These were consumed only by the BackupApplier (deleted in T1.6).
    /// Raft `AppendEntries` through `apply_log_apply_commit` is the sole
    /// replication path. `current_log_apply_index` default-returns 0;
    /// the raft log's `last_applied` is the authoritative index.
    async fn current_log_apply_index(&self) -> Result<i64> {
        Ok(0)
    }

    async fn create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
    ) -> Result<Resource>;

    /// TO-BE-CLEANUP: legacy replicated StorageCommand apply test support.
    ///
    /// Apply an authoritative leader `CreateResource` entry on a local replica.
    ///
    /// This is not the public Kubernetes create path. Public creates must keep
    /// rejecting existing names. Replicated creates converge a follower cache to
    /// the leader's object identity, including delete/recreate slots where the
    /// same name now has a different UID.
    #[cfg(test)]
    async fn apply_replicated_create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        options: ReplicatedCreateOptions,
    ) -> Result<Resource> {
        let incoming_uid = super::types::Resource::uid_from_data(&data);
        if let Some(expected_uid) = options.meta_uid.as_deref()
            && !incoming_uid.is_empty()
            && expected_uid != incoming_uid
        {
            return Err(super::errors::DatastoreError::conflict(format!(
                    "replicated create UID precondition failed: expected {expected_uid} got {incoming_uid}"
                ))
                .into());
        }
        if let Some(existing) = self
            .get_resource(api_version, kind, namespace, name)
            .await?
        {
            if incoming_uid.is_empty() || existing.uid == incoming_uid {
                self.update_resource(
                    api_version,
                    kind,
                    namespace,
                    name,
                    data,
                    existing.resource_version,
                )
                .await
            } else {
                tracing::warn!(
                    api_version = %api_version,
                    kind = %kind,
                    namespace = namespace.unwrap_or(""),
                    name = %name,
                    old_uid = %existing.uid,
                    new_uid = %incoming_uid,
                    resource_version = options.resource_version,
                    "replicated create replaced stale same-name resource with different UID"
                );
                self.delete_resource(api_version, kind, namespace, name)
                    .await?;
                self.create_resource(api_version, kind, namespace, name, data)
                    .await
            }
        } else {
            self.create_resource(api_version, kind, namespace, name, data)
                .await
        }
    }

    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>>;

    async fn list_resources(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        query: ResourceListQuery<'_>,
    ) -> Result<ResourceList> {
        self.list_resources_page(
            api_version,
            kind,
            namespace,
            query.label_selector,
            query.field_selector,
            query.page_request()?,
        )
        .await
    }

    async fn list_resources_page(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList>;

    /// Atomically list every collection used to establish one multi-topic
    /// watch baseline. Implementations must scan all targets and capture the
    /// returned durable replay position in the same read transaction/snapshot.
    /// Items retain target order and are namespace/name ordered within each
    /// target so CRD storage-version precedence remains explicit to callers.
    /// Pagination and field selectors are intentionally excluded: this is the
    /// focused CRD conversion-watch establishment primitive.
    async fn list_resources_for_watch_targets(
        &self,
        _targets: &[WatchTarget],
        _label_selector: Option<&str>,
    ) -> Result<ResourceList> {
        Err(anyhow::anyhow!(
            "datastore backend does not implement atomic multi-target watch baseline LIST"
        ))
    }

    async fn list_resource_keys_for_scope(
        &self,
        api_version: String,
        kind: String,
        namespaced: bool,
    ) -> Result<Vec<(Option<String>, String)>>;

    async fn update_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        expected_rv: i64,
    ) -> Result<Resource>;

    async fn update_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource>;

    /// Main-resource update path for resources that may expose a status
    /// subresource. Implementations should preserve the latest stored status
    /// while applying the caller's spec/metadata update.
    async fn update_main_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        self.update_resource_with_preconditions(
            api_version,
            kind,
            namespace,
            name,
            data,
            preconditions,
        )
        .await
    }

    async fn apply_resource_batch(&self, operations: Vec<ResourceBatchOperation>) -> Result<()> {
        let _ = operations;
        Err(anyhow::anyhow!(
            "backend does not support raft-backed resource batch writes"
        ))
    }

    /// Update only the `.status` subtree of a resource atomically.
    ///
    /// `.spec`, `.metadata`, and other top-level fields are preserved verbatim
    /// — there is no read-modify-write race where a concurrent `.spec` edit
    /// could be lost. `expected_rv = Some(rv)` enables compare-and-swap (409
    /// Conflict on mismatch); `None` skips the check.
    async fn update_status_only(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: Value,
        expected_rv: Option<i64>,
    ) -> Result<Resource>;

    async fn update_status_only_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource>;

    async fn delete_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<()>;
    async fn delete_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> Result<()>;
    async fn delete_resource_with_preconditions_observed_rv(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> Result<i64> {
        self.delete_resource_with_preconditions(api_version, kind, namespace, name, preconditions)
            .await?;
        self.get_current_resource_version().await
    }

    /// Mark a non-finalizer delete target as terminating without emitting a
    /// watch event.
    ///
    /// Backends may return `Ok(None)` when they do not support this internal
    /// optimization; callers should fall back to their hard-delete path.
    async fn mark_for_delete_without_watch(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
        grace_seconds: i64,
    ) -> Result<Option<Resource>> {
        let _ = (
            api_version,
            kind,
            namespace,
            name,
            preconditions,
            grace_seconds,
        );
        Ok(None)
    }

    /// Mark and remove a resource in one terminal-delete commit without
    /// emitting an extra watch event.
    ///
    /// Backends that do not have a dedicated terminal-delete command
    /// may emulate this by delegating to
    /// `mark_for_delete_without_watch` followed by a normal delete.
    async fn delete_resource_without_watch_with_tombstone(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
        grace_seconds: i64,
    ) -> Result<Resource> {
        if let Some(candidate) = self
            .mark_for_delete_without_watch(
                api_version,
                kind,
                namespace,
                name,
                preconditions.clone(),
                grace_seconds,
            )
            .await?
        {
            let delete_preconditions = ResourcePreconditions::uid_and_resource_version(
                candidate.uid.clone(),
                candidate.resource_version,
            );
            self.delete_resource_with_preconditions(
                api_version,
                kind,
                namespace,
                name,
                delete_preconditions,
            )
            .await?;
            return Ok(candidate);
        }

        let candidate = self
            .get_resource(api_version, kind, namespace, name)
            .await?
            .ok_or_else(|| {
                super::errors::DatastoreError::not_found(format!(
                    "delete_resource_without_watch_with_tombstone: {api_version}/{kind}/{name} not found"
                ))
            })?;
        let mut data = (*candidate.data).clone();
        let Some(meta) = data.get_mut("metadata").and_then(Value::as_object_mut) else {
            return Err(anyhow::anyhow!(
                "delete_resource_without_watch_with_tombstone: {api_version}/{kind}/{name} is missing metadata"
            ));
        };
        if meta
            .get("deletionTimestamp")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
        {
            meta.insert(
                "deletionTimestamp".to_string(),
                Value::String(crate::utils::k8s_timestamp()),
            );
        }
        meta.entry("deletionGracePeriodSeconds".to_string())
            .or_insert_with(|| Value::from(grace_seconds));

        let candidate = self
            .update_resource_with_preconditions(
                api_version,
                kind,
                namespace,
                name,
                data,
                preconditions,
            )
            .await?;
        let delete_preconditions = ResourcePreconditions::uid_and_resource_version(
            candidate.uid.clone(),
            candidate.resource_version,
        );
        self.delete_resource_with_preconditions(
            api_version,
            kind,
            namespace,
            name,
            delete_preconditions,
        )
        .await?;
        Ok(candidate)
    }

    async fn get_current_resource_version(&self) -> Result<i64>;
    async fn create_namespace(&self, name: &str, data: Value) -> Result<Resource>;
    async fn get_namespace(&self, name: &str) -> Result<Option<Resource>>;

    /// Test-only: idempotently ensure a namespace row exists so tests that
    /// drive the API create path (which enforces the upstream NamespaceLifecycle
    /// "namespace must exist" rule) behave like a live cluster. The default impl
    /// best-effort creates via `create_namespace`; backends may override with a
    /// cheaper path that does not advance the observed resourceVersion counter
    /// (so RV-asserting tests stay deterministic).
    #[cfg(test)]
    async fn seed_namespace_for_test(&self, name: &str) {
        let _ = self
            .create_namespace(name, serde_json::json!({"metadata": {"name": name}}))
            .await;
    }
    async fn list_namespaces(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
    ) -> Result<ResourceList> {
        self.list_namespaces_page(label_selector, field_selector, ListPageRequest::unbounded())
            .await
    }
    async fn list_namespaces_page(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList>;
    async fn update_namespace(&self, name: &str, data: Value, expected_rv: i64)
    -> Result<Resource>;
    async fn delete_namespace_contents(&self, name: &str) -> Result<()>;
    async fn delete_namespace(&self, name: &str) -> Result<()>;
    async fn delete_namespace_observed_rv(&self, name: &str) -> Result<i64> {
        self.delete_namespace(name).await?;
        self.get_current_resource_version().await
    }
    async fn pod_workqueue_enqueue(
        &self,
        kind: PodWorkqueueKind,
        pod: &crate::pod_identity::PodIdentity,
        payload: Value,
        attempt_count: i64,
        min_delay_ms: i64,
        last_error: Option<&str>,
    ) -> Result<()>;
    async fn pod_workqueue_peek_next_due(&self) -> Result<Option<i64>>;
    async fn pod_workqueue_claim_due(&self, now_ms: i64) -> Result<Option<PodWorkqueueEntry>>;
    async fn pod_workqueue_complete(&self, id: i64) -> Result<()>;
    async fn pod_workqueue_record_failure(
        &self,
        row: PodWorkqueueEntry,
        min_delay_ms: i64,
        error: &str,
    ) -> Result<()>;
    async fn pod_workqueue_dead_letter(&self, id: i64, error: &str) -> Result<()>;

    async fn record_sandbox(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        sandbox_id: &str,
    ) -> Result<()>;
    async fn get_sandbox(&self, namespace: &str, pod_name: &str) -> Result<Option<String>>;
    async fn get_sandbox_for_uid(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
    ) -> Result<Option<String>>;
    async fn delete_sandbox(&self, namespace: &str, pod_name: &str) -> Result<()>;
    async fn delete_sandbox_for_uid(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        sandbox_id: &str,
    ) -> Result<()>;

    async fn delete_pod_network(&self, sandbox_id: &str) -> Result<()>;

    /// **Performance contract:** O(log n) lookup expected. Backends without
    /// expression indexes (e.g., redb) must maintain a secondary index
    /// manually inside their mutation methods — O(n) full-table scans are
    /// not acceptable since this method is on the controller-reconcile hot
    /// path.
    async fn find_owned_resources(
        &self,
        owner_uid: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>>;

    /// Return resources of `(api_version, kind)` owned by `owner_uid`.
    ///
    /// `namespace = Some(ns)` queries the namespaced_resources table;
    /// `namespace = None` queries the cluster_resources table.
    ///
    /// Matches ownerReferences at any array position; callers must not assume
    /// Kubernetes puts the controller owner in index 0.
    ///
    /// **Performance contract:** O(log n) lookup expected. Backends without
    /// expression indexes (e.g., redb) must maintain a secondary index
    /// manually inside their mutation methods — O(n) full-table scans are
    /// not acceptable since this method is on the controller-reconcile hot
    /// path.
    async fn list_resources_by_owner_uid(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        owner_uid: &str,
    ) -> Result<Vec<Resource>>;

    /// **Performance contract:** O(log n) lookup expected. Backends without
    /// expression indexes (e.g., redb) must maintain a secondary index
    /// manually inside their mutation methods — O(n) full-table scans are
    /// not acceptable since this method is on the controller-reconcile hot
    /// path.
    async fn find_owned_by_name_kind_empty_uid(
        &self,
        owner_api_version: &str,
        owner_name: &str,
        owner_kind: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>>;

    async fn list_cluster_resources_modified_since(
        &self,
        api_version: &str,
        kind: &str,
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>>;

    async fn list_cluster_resources(&self) -> Result<Vec<Resource>>;

    async fn list_resources_modified_since(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>>;

    async fn advance_resource_version_after(&self, min_rv: i64) -> Result<i64>;

    async fn list_namespace_resources(&self, namespace: &str) -> Result<Vec<Resource>>;

    async fn list_namespace_resources_of_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>>;

    async fn list_namespace_resources_excluding_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>>;

    async fn count_namespace_resources(&self, namespace: &str) -> Result<i64>;

    async fn list_watch_events_since(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>>;

    /// Read a replay suffix only if the retained watch history still covers
    /// `since_rv`. Backends with a durable watch-event table should override
    /// this so the floor check and event read happen in the same read snapshot.
    async fn list_watch_events_since_checked(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
    ) -> Result<WatchReplayRead> {
        if since_rv > 0
            && let Some(earliest) = self.earliest_watch_event_rv().await?
            && since_rv + 1 < earliest
        {
            return Ok(WatchReplayRead::Expired);
        }
        self.list_watch_events_since(targets, since_rv)
            .await
            .map(WatchReplayRead::Events)
    }

    async fn list_watch_events_since_checked_bounded(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<WatchReplayRead> {
        match self
            .list_watch_events_since_checked(targets, since_rv)
            .await?
        {
            WatchReplayRead::Events(mut events) => {
                events.truncate(limit.get());
                Ok(WatchReplayRead::Events(events))
            }
            WatchReplayRead::Expired => Ok(WatchReplayRead::Expired),
        }
    }

    async fn list_watch_events_after_position_checked_bounded(
        &self,
        _targets: &[WatchTarget],
        _position: WatchReplayPosition,
        _limit: std::num::NonZeroUsize,
    ) -> Result<PositionedWatchReplayRead<CatchUpResource>> {
        Err(anyhow::anyhow!(
            "datastore backend does not implement durable positioned watch replay"
        ))
    }

    /// Capture the current durable watch-log insertion boundary. A watch that
    /// subscribes and then establishes a baseline can replay strictly after
    /// this position without translating the boundary through resourceVersion.
    async fn current_watch_replay_position(&self) -> Result<WatchReplayPosition> {
        Err(anyhow::anyhow!(
            "datastore backend does not implement a durable watch replay anchor"
        ))
    }

    /// Reconstruct the atomically consistent resource state represented by a
    /// durable watch cursor across one or more targets. The represented state
    /// is the exact inverse of positioned replay, including the composite
    /// positive-RV handoff filter. Selectors are applied after reconstruction.
    async fn snapshot_resources_at_position(
        &self,
        _targets: &[WatchTarget],
        _label_selector: Option<&str>,
        _field_selector: Option<&str>,
        position: WatchReplayPosition,
    ) -> Result<SnapshotAtRv> {
        let current = self.current_watch_replay_position().await?;
        let covers_current = position.event_id >= current.event_id
            || (position.resource_version_filter_through_event_id >= current.event_id
                && position.resource_version >= current.resource_version)
            || (position.event_id == 0
                && position.resource_version_filter_through_event_id == 0
                && position.resource_version >= current.resource_version);
        if covers_current {
            Ok(SnapshotAtRv::Current)
        } else {
            Ok(SnapshotAtRv::Expired)
        }
    }

    /// Replay durable watch rows with routing/cursor metadata carried in typed
    /// fields and the original object JSON left as bytes.
    async fn list_raw_watch_events_since_checked_bounded(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<WatchReplayRead<RawWatchEvent>>;

    async fn list_raw_watch_events_after_position_checked_bounded(
        &self,
        _targets: &[WatchTarget],
        _position: WatchReplayPosition,
        _limit: std::num::NonZeroUsize,
    ) -> Result<PositionedWatchReplayRead<RawWatchEvent>> {
        Err(anyhow::anyhow!(
            "datastore backend does not implement durable raw positioned watch replay"
        ))
    }

    /// Lowest `resourceVersion` still retained in the durable `watch_events`
    /// window, or `None` when the window is empty. A watch whose requested /
    /// resume `resourceVersion` is older than this can no longer be replayed
    /// in full and must be answered with `410 Gone` (Expired). Defaults to
    /// `None` (never report a gap) for backends/adapters that do not own the
    /// cluster watch-event window.
    async fn earliest_watch_event_rv(&self) -> Result<Option<i64>> {
        Ok(None)
    }

    /// Reconstruct the resources of `(api_version, kind, namespace)` exactly as
    /// they existed at `snapshot_rv`, for a plain LIST with
    /// `resourceVersionMatch=Exact` and for consistent paginated continuations
    /// (the continue token's session rv). The result is selector-filtered and
    /// paginated per `query`.
    ///
    /// Returns [`SnapshotAtRv::Current`] when `snapshot_rv` is at or beyond the
    /// current state (serve the live list), [`SnapshotAtRv::Expired`] when the
    /// rv predates the reconstructable history window (caller answers 410), or
    /// [`SnapshotAtRv::List`] with the reconstructed page.
    ///
    /// The default impl supports only the trivial current/expired split;
    /// backends with a durable watch-event history override it with a real
    /// reconstruction.
    async fn snapshot_resources_at_rv(
        &self,
        _api_version: &str,
        _kind: &str,
        _namespace: Option<&str>,
        _query: ResourceListQuery<'_>,
        snapshot_rv: i64,
    ) -> Result<SnapshotAtRv> {
        let current = self.get_current_resource_version().await?;
        if snapshot_rv >= current {
            Ok(SnapshotAtRv::Current)
        } else {
            Ok(SnapshotAtRv::Expired)
        }
    }

    /// List resource watch events after `since_rv` across all scopes.
    ///
    /// Replication reconnect uses this durable history to replay ADDED,
    /// MODIFIED, and DELETED events in resourceVersion order.
    async fn list_all_watch_events_since(&self, since_rv: i64) -> Result<Vec<CatchUpResource>>;

    /// memory-improvement.md §10 P1: keyset-paginated form of
    /// `list_all_watch_events_since`. Streams the watch-events table batch
    /// by batch for the snapshot serve path so a multi-million-row table
    /// never has to be materialized into one `Vec`. Each item carries its
    /// `watch_events.id` so the caller can advance the cursor; ordering and
    /// content match the full-list form exactly.
    async fn list_all_watch_events_since_paged(
        &self,
        since_rv: i64,
        after_resource_version: i64,
        after_id: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<(i64, CatchUpResource)>>;

    /// Snapshot-only apply-order page bounded by a pre-captured allocator
    /// anchor. Rows are ordered solely by durable event ID so a concurrently
    /// applied lower resourceVersion cannot fall behind the continuation key.
    async fn list_all_watch_events_after_id_bounded(
        &self,
        after_id: i64,
        through_id: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<(i64, CatchUpResource)>> {
        let _ = (after_id, through_id, limit);
        Err(anyhow::anyhow!(
            "datastore backend does not implement bounded event-ID snapshot paging"
        ))
    }

    async fn list_watch_replay_floors(&self) -> Result<Vec<WatchReplayFloor>> {
        Ok(Vec::new())
    }

    /// List deleted resource watch events after `since_rv` across all scopes.
    ///
    /// Replication reconnect uses this to catch up deletes that cannot be
    /// reconstructed from a current-state snapshot because the object is no
    /// longer present.
    async fn list_deleted_watch_events_since(&self, since_rv: i64) -> Result<Vec<CatchUpResource>>;

    /// Allocate or return existing /24 subnet for node and node IP mapping.
    async fn allocate_node_subnet(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<NodeSubnet>;

    /// F2-04: persist peer-mode + hostport-range projected from
    /// `klights.io/mode` / `klights.io/hostport-range` Node annotations.
    async fn update_node_peer_attributes(
        &self,
        node_name: &str,
        mode: crate::controllers::annotations::NodePeerMode,
        hostport_range: Option<crate::networking::types::HostPortRange>,
    ) -> Result<()>;

    /// Persist cluster-visible dataplane metadata for a node. The metadata
    /// must already be validated and must not contain any private key material.
    async fn update_node_dataplane(
        &self,
        metadata: crate::networking::wireguard::DataplanePeerMetadata,
    ) -> Result<()> {
        let _ = metadata;
        Err(anyhow::anyhow!(
            "backend does not support node dataplane metadata"
        ))
    }

    async fn get_node_dataplane(
        &self,
        node_name: &str,
    ) -> Result<Option<crate::networking::wireguard::DataplanePeerMetadata>> {
        let _ = node_name;
        Err(anyhow::anyhow!(
            "backend does not support node dataplane metadata"
        ))
    }

    /// Get node subnet record.
    async fn get_node_subnet(&self, node_name: &str) -> Result<Option<NodeSubnet>>;

    /// List peer node subnets. Includes root and rootless peers.
    async fn list_peer_subnets(&self, my_node_name: &str) -> Result<Vec<NodeSubnet>>;

    /// Delete a node subnet row.
    async fn delete_node_subnet(&self, node_name: &str) -> Result<()>;

    async fn move_pod_to_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        let _ = (node_name, namespace, pod_name, pod_uid, reason);
        Err(anyhow::anyhow!(
            "backend does not support pod cleanup intents"
        ))
    }

    async fn list_pod_cleanup_intents_for_node(
        &self,
        node_name: &str,
    ) -> Result<Vec<PodCleanupIntent>> {
        let _ = node_name;
        Ok(Vec::new())
    }

    async fn delete_pod_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        let _ = (node_name, namespace, pod_name, pod_uid, reason);
        Ok(())
    }

    async fn delete_pod_cleanup_intents_for_node(&self, node_name: &str) -> Result<()> {
        let _ = node_name;
        Ok(())
    }

    async fn pod_slot_try_admit(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        node_name: &str,
    ) -> Result<PodSlotAdmissionResult>;

    async fn pod_slot_mark_terminating(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        node_name: &str,
    ) -> Result<()>;

    async fn pod_slot_clear_if_uid(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        node_name: &str,
    ) -> Result<()>;

    fn subscribe_pod_slot_admissions(&self) -> broadcast::Receiver<PodSlotAdmissionEvent>;

    /// Patch an object by applying the chosen merge patch strategy.
    async fn patch_resource_latest(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        patch_kind: PatchKind,
        patch: Value,
    ) -> Result<Option<Resource>>;
    async fn patch_resource_latest_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        request: ResourcePatchRequest,
    ) -> Result<Option<Resource>>;

    /// Get pod network allocation record for a sandbox.
    async fn get_pod_network(&self, sandbox_id: &str) -> Result<Option<PodNetworkEndpoint>>;

    /// Get pod network allocation record for an exact pod identity.
    async fn get_pod_network_for_pod(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
    ) -> Result<Option<PodNetworkEndpoint>>;

    /// Atomically allocate pod network state.
    async fn ipam_allocate_and_record_pod_network(
        &self,
        sandbox_id: &str,
        pod: &crate::pod_identity::PodIdentity,
        subnet_base_int: u32,
        subnet_size: u32,
        veth_host: &str,
        netns_path: &str,
    ) -> Result<(String, u32)>;

    /// List sandbox records for orphan cleanup.
    async fn list_sandboxes(&self) -> Result<Vec<SandboxRef>>;
    /// List all sandbox IDs that still have pod_network rows.
    async fn list_pod_network_sandbox_ids(&self) -> Result<Vec<String>>;

    /// Delete old watch events to keep the retention table bounded.
    async fn gc_watch_events(&self, max_rows: i64, batch_cap: i64) -> Result<usize>;

    /// Count how many watch events would be removed by `gc_watch_events`
    /// without mutating storage. Used by raft-mode maintenance to avoid
    /// proposing no-op GC entries on idle clusters.
    async fn watch_events_gc_prunable_count(&self, max_rows: i64, batch_cap: i64) -> Result<usize>;

    /// Count how many applied_outbox rows would be removed by
    /// `gc_applied_outbox` when using `cutoff_ms` as the retention cutoff.
    /// Defaults to unsupported for backends that cannot classify applied_outbox
    /// retention in O(1) or at least an indexed O(n) way.
    async fn applied_outbox_gc_prunable_count(&self, cutoff_ms: i64) -> Result<usize> {
        let _ = cutoff_ms;
        Err(anyhow::anyhow!(
            "backend does not support applied_outbox prunable-count query"
        ))
    }

    /// Look up the pod_endpoints row for `pod_ip`. Returns `None` when no
    /// pod currently advertises that address. Phase 1 has no production
    /// consumer beyond the SqlitePodEndpointResolver; Phase 2 hybrid
    /// reconcilers will be the active callers.
    async fn pod_endpoint_get_by_pod_ip(
        &self,
        pod_ip: std::net::Ipv4Addr,
    ) -> Result<Option<PodEndpointRow>>;

    /// List every pod_endpoints row for startup/recovery reconciliation.
    async fn pod_endpoint_list_all(&self) -> Result<Vec<PodEndpointRow>>;

    /// Subscribe to the pod_endpoints broadcast channel.
    fn subscribe_pod_endpoints(&self) -> broadcast::Receiver<PodEndpointEvent>;

    /// Read a key from the `_klights_meta` table.
    /// Returns `None` if the key does not exist.
    async fn get_klights_meta(&self, key: &str) -> Result<Option<String>>;

    /// Write a key/value pair to the `_klights_meta` table.
    async fn set_klights_meta(&self, key: &str, value: &str) -> Result<()>;

    /// List raft-replicated worker outbox stream watermarks. Snapshot emitters
    /// use this to preserve retry/dedup progress without the legacy
    /// applied_outbox ledger.
    async fn list_outbox_stream_watermarks(
        &self,
    ) -> Result<Vec<crate::log_apply::OutboxStreamWatermark>> {
        Err(anyhow::anyhow!(
            "backend does not support outbox stream watermark listing"
        ))
    }

    async fn get_applied_outbox(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<AppliedOutboxRecord>>;

    async fn insert_applied_outbox(&self, record: AppliedOutboxRecord) -> Result<bool>;

    async fn list_applied_outbox(&self) -> Result<Vec<AppliedOutboxRecord>> {
        Err(anyhow::anyhow!(
            "backend does not support applied_outbox listing"
        ))
    }

    /// memory-improvement.md §10 P1: keyset-paginated form of
    /// `list_applied_outbox`. Streams the dedup ledger batch by batch for
    /// the snapshot serve path. Ordering and content match the full-list
    /// form exactly.
    async fn list_applied_outbox_paged(
        &self,
        after_key: Option<&str>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<AppliedOutboxRecord>> {
        // memory-improvement.md §10 P1: default fallback — load the full
        // ledger and filter by `idempotency_key` in memory. Production sqlite
        // overrides with a real keyset query; this preserves pre-P1 behavior
        // for backends that don't.
        let limit = limit.get();
        let rows = self.list_applied_outbox().await?;
        let mut out: Vec<AppliedOutboxRecord> = Vec::with_capacity(rows.len().min(limit));
        for record in rows {
            let past_cursor = after_key.is_none_or(|k| record.idempotency_key.as_str() > k);
            if past_cursor {
                out.push(record);
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    async fn delete_uncommitted_applied_outbox_placeholder(
        &self,
        idempotency_key: &str,
        reserved_rv: i64,
    ) -> Result<bool> {
        let _ = idempotency_key;
        let _ = reserved_rv;
        Err(anyhow::anyhow!(
            "backend does not support applied_outbox placeholder cleanup"
        ))
    }

    /// Apply an outbox payload transactionally: check idempotency, apply
    /// mutation, and insert ledger row all in one cluster.db transaction.
    async fn apply_outbox_transactionally(
        &self,
        idempotency_key: &str,
        operation: &str,
        payload: &[u8],
        authoring_node: &str,
    ) -> std::result::Result<
        crate::kubelet::outbox::OutboxApplyResult,
        crate::kubelet::outbox::OutboxApplyError,
    >;

    async fn apply_outbox_transactionally_with_watermark(
        &self,
        idempotency_key: &str,
        operation: &str,
        payload: &[u8],
        authoring_node: &str,
        watermark: Option<crate::log_apply::OutboxStreamWatermark>,
    ) -> std::result::Result<
        crate::kubelet::outbox::OutboxApplyResult,
        crate::kubelet::outbox::OutboxApplyError,
    > {
        let _ = watermark;
        self.apply_outbox_transactionally(idempotency_key, operation, payload, authoring_node)
            .await
    }

    /// T1.4: build a materialized `LogApplyCommit` for a regular (non-outbox)
    /// raft write without touching the applied_outbox ledger. The leader's
    /// proposer encodes the returned commit and submits it through
    /// `client_write`; every raft member then applies the decoded commit via
    /// `apply_log_apply_commit`.
    async fn build_log_apply_commit_for_command(
        &self,
        command: StorageCommand,
        operation: &str,
        authoring_node: &str,
    ) -> Result<crate::log_apply::LogApplyCommit> {
        let _ = (command, operation, authoring_node);
        Err(anyhow::anyhow!(
            "backend does not support generic raft commit materialization"
        ))
    }

    /// T1.3/T1.4: build a `LogApplyCommit` from an outbox payload WITHOUT
    /// applying it. The leader's raft proposer encodes the returned commit
    /// as the raft entry payload (via a placeholder/ledger-aware path) and
    /// submits through `client_write`; the state machine apply path on every
    /// node is the only caller that actually mutates `cluster.db` (via
    /// `apply_log_apply_commit`).
    async fn build_log_apply_commit_for_outbox(
        &self,
        idempotency_key: &str,
        operation: &str,
        payload: &[u8],
        authoring_node: &str,
    ) -> std::result::Result<
        crate::datastore::sqlite::BuildOutboxOutcome,
        crate::kubelet::outbox::OutboxApplyError,
    >;

    async fn build_log_apply_commit_for_outbox_with_watermark(
        &self,
        idempotency_key: &str,
        operation: &str,
        payload: &[u8],
        authoring_node: &str,
        watermark: Option<crate::log_apply::OutboxStreamWatermark>,
    ) -> std::result::Result<
        crate::datastore::sqlite::BuildOutboxOutcome,
        crate::kubelet::outbox::OutboxApplyError,
    > {
        let _ = watermark;
        self.build_log_apply_commit_for_outbox(idempotency_key, operation, payload, authoring_node)
            .await
    }

    /// Prune all applied_outbox entries older than `ttl_ms`. Returns the
    /// number of pruned rows.
    async fn gc_applied_outbox(&self, now_ms: i64, ttl_ms: i64) -> Result<usize>;
}

// ---------------------------------------------------------------------------
// Focused storage interfaces
//
// `DatastoreBackend` is the umbrella trait that the production implementation
// satisfies. The focused traits below give narrow-typed views over the same
// backend so call-site signatures can declare exactly which capabilities they
// need (e.g. a watch helper takes `&dyn WatchStore`, not the entire backend).
//
// Method signatures duplicate those on `DatastoreBackend` and the blanket
// impls delegate, so there is exactly one source of truth for each method
// body — the existing `impl DatastoreBackend for Datastore` block.
// `DatastoreHandle` continues to type-erase the umbrella for call sites that
// need every capability.
// ---------------------------------------------------------------------------

/// Resource CRUD on the namespaced/cluster tables.
#[async_trait]
pub trait ResourceStore: Send + Sync {
    async fn create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
    ) -> Result<Resource>;
    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>>;
    async fn delete_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<()>;
    async fn delete_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> Result<()>;
    async fn update_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        expected_rv: i64,
    ) -> Result<Resource>;
    async fn update_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource>;
    async fn get_current_resource_version(&self) -> Result<i64>;
}

#[async_trait]
impl<T: DatastoreBackend + ?Sized> ResourceStore for T {
    async fn create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
    ) -> Result<Resource> {
        DatastoreBackend::create_resource(self, api_version, kind, namespace, name, data).await
    }
    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        DatastoreBackend::get_resource(self, api_version, kind, namespace, name).await
    }
    async fn delete_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<()> {
        DatastoreBackend::delete_resource(self, api_version, kind, namespace, name).await
    }
    async fn delete_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> Result<()> {
        DatastoreBackend::delete_resource_with_preconditions(
            self,
            api_version,
            kind,
            namespace,
            name,
            preconditions,
        )
        .await
    }
    async fn update_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        DatastoreBackend::update_resource(
            self,
            api_version,
            kind,
            namespace,
            name,
            data,
            expected_rv,
        )
        .await
    }
    async fn update_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        DatastoreBackend::update_resource_with_preconditions(
            self,
            api_version,
            kind,
            namespace,
            name,
            data,
            preconditions,
        )
        .await
    }
    async fn get_current_resource_version(&self) -> Result<i64> {
        DatastoreBackend::get_current_resource_version(self).await
    }
}

/// Resource list and selector queries.
#[async_trait]
pub trait ResourceListStore: Send + Sync {
    async fn list_resources_page(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList>;
    async fn list_resource_keys_for_scope(
        &self,
        api_version: String,
        kind: String,
        namespaced: bool,
    ) -> Result<Vec<(Option<String>, String)>>;
}

/// Status-subresource writes.
#[async_trait]
pub trait StatusStore: Send + Sync {
    async fn update_status_only(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: Value,
        expected_rv: Option<i64>,
    ) -> Result<Resource>;
    async fn update_status_only_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource>;
}

/// Owner-reference indexes and ownership lookups.
#[async_trait]
pub trait OwnershipStore: Send + Sync {
    async fn find_owned_resources(
        &self,
        owner_uid: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>>;
    async fn list_resources_by_owner_uid(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        owner_uid: &str,
    ) -> Result<Vec<Resource>>;
    async fn find_owned_by_name_kind_empty_uid(
        &self,
        owner_api_version: &str,
        owner_name: &str,
        owner_kind: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>>;
}

/// ResourceVersion read required to anchor watch bootstrap.
#[async_trait]
pub trait CurrentResourceVersionStore: Send + Sync {
    async fn get_current_resource_version(&self) -> Result<i64>;
}

/// Durable watch establishment anchors and positioned membership snapshots.
#[async_trait]
pub trait WatchReplayAnchorStore: Send + Sync {
    async fn current_watch_replay_position(&self) -> Result<WatchReplayPosition>;

    async fn snapshot_resources_at_position(
        &self,
        targets: &[WatchTarget],
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        position: WatchReplayPosition,
    ) -> Result<SnapshotAtRv>;
}

/// Raw watch replay rows for optimized selectorless streams.
#[async_trait]
pub trait RawWatchReplayStore: Send + Sync {
    async fn list_raw_watch_events_after_position_checked_bounded(
        &self,
        targets: &[WatchTarget],
        position: WatchReplayPosition,
        limit: std::num::NonZeroUsize,
    ) -> Result<PositionedWatchReplayRead<RawWatchEvent>>;
}

/// Watch-event subscription, broadcast access, and replay queries.
#[async_trait]
pub trait WatchStore: Send + Sync {
    fn subscribe_watch_signals(&self, topic: WatchTopic) -> broadcast::Receiver<WatchSignal>;
    #[cfg(test)]
    fn subscribe_watch(&self, topic: WatchTopic) -> broadcast::Receiver<WatchEvent>;
    async fn list_watch_events_since(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>>;

    async fn list_watch_events_since_checked(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
    ) -> Result<WatchReplayRead> {
        if since_rv > 0
            && let Some(earliest) = self.earliest_watch_event_rv().await?
            && since_rv + 1 < earliest
        {
            return Ok(WatchReplayRead::Expired);
        }
        self.list_watch_events_since(targets, since_rv)
            .await
            .map(WatchReplayRead::Events)
    }

    async fn list_watch_events_since_checked_bounded(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<WatchReplayRead> {
        match self
            .list_watch_events_since_checked(targets, since_rv)
            .await?
        {
            WatchReplayRead::Events(mut events) => {
                events.truncate(limit.get());
                Ok(WatchReplayRead::Events(events))
            }
            WatchReplayRead::Expired => Ok(WatchReplayRead::Expired),
        }
    }

    async fn list_watch_events_after_position_checked_bounded(
        &self,
        _targets: &[WatchTarget],
        _position: WatchReplayPosition,
        _limit: std::num::NonZeroUsize,
    ) -> Result<PositionedWatchReplayRead<CatchUpResource>> {
        Err(anyhow::anyhow!(
            "watch store does not implement durable positioned watch replay"
        ))
    }

    async fn earliest_watch_event_rv(&self) -> Result<Option<i64>> {
        Ok(None)
    }
}

/// Transitional composition-root adapter from the legacy backend handle into
/// the focused watch port. New consumers should store `Arc<dyn WatchStore>`,
/// not `DatastoreHandle`.
pub struct DatastoreBackendWatchStore {
    db: DatastoreHandle,
}

impl DatastoreBackendWatchStore {
    pub fn new(db: DatastoreHandle) -> Self {
        Self { db }
    }
}

#[async_trait]
impl CurrentResourceVersionStore for DatastoreBackendWatchStore {
    async fn get_current_resource_version(&self) -> Result<i64> {
        self.db.get_current_resource_version().await
    }
}

#[async_trait]
impl WatchReplayAnchorStore for DatastoreBackendWatchStore {
    async fn current_watch_replay_position(&self) -> Result<WatchReplayPosition> {
        self.db.current_watch_replay_position().await
    }

    async fn snapshot_resources_at_position(
        &self,
        targets: &[WatchTarget],
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        position: WatchReplayPosition,
    ) -> Result<SnapshotAtRv> {
        self.db
            .snapshot_resources_at_position(targets, label_selector, field_selector, position)
            .await
    }
}

#[async_trait]
impl RawWatchReplayStore for DatastoreBackendWatchStore {
    async fn list_raw_watch_events_after_position_checked_bounded(
        &self,
        targets: &[WatchTarget],
        position: WatchReplayPosition,
        limit: std::num::NonZeroUsize,
    ) -> Result<PositionedWatchReplayRead<RawWatchEvent>> {
        self.db
            .list_raw_watch_events_after_position_checked_bounded(targets, position, limit)
            .await
    }
}

#[async_trait]
impl WatchStore for DatastoreBackendWatchStore {
    fn subscribe_watch_signals(&self, topic: WatchTopic) -> broadcast::Receiver<WatchSignal> {
        self.db.subscribe_watch_signals(topic)
    }

    #[cfg(test)]
    fn subscribe_watch(&self, topic: WatchTopic) -> broadcast::Receiver<WatchEvent> {
        self.db.subscribe_watch(topic)
    }

    async fn list_watch_events_since(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        self.db.list_watch_events_since(targets, since_rv).await
    }

    async fn list_watch_events_since_checked(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
    ) -> Result<WatchReplayRead> {
        self.db
            .list_watch_events_since_checked(targets, since_rv)
            .await
    }

    async fn list_watch_events_since_checked_bounded(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<WatchReplayRead> {
        self.db
            .list_watch_events_since_checked_bounded(targets, since_rv, limit)
            .await
    }

    async fn list_watch_events_after_position_checked_bounded(
        &self,
        targets: &[WatchTarget],
        position: WatchReplayPosition,
        limit: std::num::NonZeroUsize,
    ) -> Result<PositionedWatchReplayRead<CatchUpResource>> {
        self.db
            .list_watch_events_after_position_checked_bounded(targets, position, limit)
            .await
    }

    async fn earliest_watch_event_rv(&self) -> Result<Option<i64>> {
        self.db.earliest_watch_event_rv().await
    }
}

/// Durable watch history and resourceVersion recovery.
#[async_trait]
pub trait WatchHistoryStore: Send + Sync {
    async fn list_cluster_resources_modified_since(
        &self,
        api_version: &str,
        kind: &str,
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>>;
    async fn list_resources_modified_since(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>>;
    async fn list_all_watch_events_since(&self, since_rv: i64) -> Result<Vec<CatchUpResource>>;
    async fn list_deleted_watch_events_since(&self, since_rv: i64) -> Result<Vec<CatchUpResource>>;
    async fn advance_resource_version_after(&self, min_rv: i64) -> Result<i64>;
    async fn watch_events_gc_prunable_count(&self, max_rows: i64, batch_cap: i64) -> Result<usize>;
    async fn gc_watch_events(&self, max_rows: i64, batch_cap: i64) -> Result<usize>;
}

/// Sandbox / pod-network / IPAM state used by the kubelet networking layer.
#[async_trait]
pub trait NetworkStore: Send + Sync {
    async fn record_sandbox(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        sandbox_id: &str,
    ) -> Result<()>;
    async fn get_sandbox(&self, namespace: &str, pod_name: &str) -> Result<Option<String>>;
    async fn delete_sandbox(&self, namespace: &str, pod_name: &str) -> Result<()>;
    async fn delete_sandbox_for_uid(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        sandbox_id: &str,
    ) -> Result<()>;
    async fn delete_pod_network(&self, sandbox_id: &str) -> Result<()>;
    async fn get_pod_network(&self, sandbox_id: &str) -> Result<Option<PodNetworkEndpoint>>;
}

/// Node, sandbox, IPAM, and pod-endpoint metadata outside Pod objects.
#[async_trait]
pub trait NetworkMetadataStore: Send + Sync {
    async fn get_sandbox_for_uid(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
    ) -> Result<Option<String>>;
    async fn get_pod_network_for_pod(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
    ) -> Result<Option<PodNetworkEndpoint>>;
    async fn ipam_allocate_and_record_pod_network(
        &self,
        sandbox_id: &str,
        pod: &crate::pod_identity::PodIdentity,
        subnet_base_int: u32,
        subnet_size: u32,
        veth_host: &str,
        netns_path: &str,
    ) -> Result<(String, u32)>;
    async fn list_sandboxes(&self) -> Result<Vec<SandboxRef>>;
    async fn list_pod_network_sandbox_ids(&self) -> Result<Vec<String>>;
    async fn allocate_node_subnet(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<NodeSubnet>;
    async fn update_node_peer_attributes(
        &self,
        node_name: &str,
        mode: crate::controllers::annotations::NodePeerMode,
        hostport_range: Option<crate::networking::types::HostPortRange>,
    ) -> Result<()>;
    async fn update_node_dataplane(
        &self,
        metadata: crate::networking::wireguard::DataplanePeerMetadata,
    ) -> Result<()>;
    async fn get_node_dataplane(
        &self,
        node_name: &str,
    ) -> Result<Option<crate::networking::wireguard::DataplanePeerMetadata>>;
    async fn get_node_subnet(&self, node_name: &str) -> Result<Option<NodeSubnet>>;
    async fn list_peer_subnets(&self, my_node_name: &str) -> Result<Vec<NodeSubnet>>;
    async fn delete_node_subnet(&self, node_name: &str) -> Result<()>;
    async fn pod_endpoint_get_by_pod_ip(
        &self,
        pod_ip: std::net::Ipv4Addr,
    ) -> Result<Option<PodEndpointRow>>;
    async fn pod_endpoint_list_all(&self) -> Result<Vec<PodEndpointRow>>;
    fn subscribe_pod_endpoints(&self) -> broadcast::Receiver<PodEndpointEvent>;
}

/// Durable pod workqueue CRUD.
#[async_trait]
pub trait PodWorkqueueStore: Send + Sync {
    async fn pod_workqueue_enqueue(
        &self,
        kind: PodWorkqueueKind,
        pod: &crate::pod_identity::PodIdentity,
        payload: Value,
        attempt_count: i64,
        min_delay_ms: i64,
        last_error: Option<&str>,
    ) -> Result<()>;
    async fn pod_workqueue_peek_next_due(&self) -> Result<Option<i64>>;
    async fn pod_workqueue_claim_due(&self, now_ms: i64) -> Result<Option<PodWorkqueueEntry>>;
    async fn pod_workqueue_complete(&self, id: i64) -> Result<()>;
    async fn pod_workqueue_record_failure(
        &self,
        row: PodWorkqueueEntry,
        min_delay_ms: i64,
        error: &str,
    ) -> Result<()>;
    async fn pod_workqueue_dead_letter(&self, id: i64, error: &str) -> Result<()>;
}

#[async_trait]
impl<T: DatastoreBackend + ?Sized> PodWorkqueueStore for T {
    async fn pod_workqueue_enqueue(
        &self,
        kind: PodWorkqueueKind,
        pod: &crate::pod_identity::PodIdentity,
        payload: Value,
        attempt_count: i64,
        min_delay_ms: i64,
        last_error: Option<&str>,
    ) -> Result<()> {
        DatastoreBackend::pod_workqueue_enqueue(
            self,
            kind,
            pod,
            payload,
            attempt_count,
            min_delay_ms,
            last_error,
        )
        .await
    }

    async fn pod_workqueue_peek_next_due(&self) -> Result<Option<i64>> {
        DatastoreBackend::pod_workqueue_peek_next_due(self).await
    }

    async fn pod_workqueue_claim_due(&self, now_ms: i64) -> Result<Option<PodWorkqueueEntry>> {
        DatastoreBackend::pod_workqueue_claim_due(self, now_ms).await
    }

    async fn pod_workqueue_complete(&self, id: i64) -> Result<()> {
        DatastoreBackend::pod_workqueue_complete(self, id).await
    }

    async fn pod_workqueue_record_failure(
        &self,
        row: PodWorkqueueEntry,
        min_delay_ms: i64,
        error: &str,
    ) -> Result<()> {
        DatastoreBackend::pod_workqueue_record_failure(self, row, min_delay_ms, error).await
    }

    async fn pod_workqueue_dead_letter(&self, id: i64, error: &str) -> Result<()> {
        DatastoreBackend::pod_workqueue_dead_letter(self, id, error).await
    }
}

/// Namespace lifecycle (create, get, delete, list contents).
#[async_trait]
pub trait NamespaceStore: Send + Sync {
    async fn create_namespace(&self, name: &str, data: Value) -> Result<Resource>;
    async fn get_namespace(&self, name: &str) -> Result<Option<Resource>>;
    async fn list_namespaces_page(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList>;
    async fn update_namespace(&self, name: &str, data: Value, expected_rv: i64)
    -> Result<Resource>;
    async fn delete_namespace(&self, name: &str) -> Result<()>;
    async fn delete_namespace_contents(&self, name: &str) -> Result<()>;
}

/// Namespace content enumeration and accounting.
#[async_trait]
pub trait NamespaceContentStore: Send + Sync {
    async fn list_namespace_resources(&self, namespace: &str) -> Result<Vec<Resource>>;
    async fn list_namespace_resources_of_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>>;
    async fn list_namespace_resources_excluding_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>>;
    async fn count_namespace_resources(&self, namespace: &str) -> Result<i64>;
}

/// Replication and snapshot-apply entry points.
#[async_trait]
pub trait ReplicationStore: Send + Sync {
    /// TO-BE-CLEANUP: legacy replicated StorageCommand apply test support.
    #[cfg(test)]
    async fn apply_replicated_command(
        &self,
        command: StorageCommand,
        meta: CommandMeta,
    ) -> Result<()>;
    async fn replace_replicated_resource_state(
        &self,
        entries: Vec<crate::log_apply::LogApplyCommit>,
        current_rv: i64,
        watch_event_high_water: Option<i64>,
        watch_replay_floors: Option<Vec<WatchReplayFloor>>,
        metadata: Option<ReplicatedSnapshotMetadata>,
    ) -> Result<()>;
    async fn apply_log_apply_commit(&self, commit: crate::log_apply::LogApplyCommit) -> Result<()>;
    async fn apply_raft_log_apply_commit(
        &self,
        commit: crate::log_apply::LogApplyCommit,
    ) -> Result<crate::datastore::raft::types::StorageCommandResult>;
}

#[async_trait]
impl<T: DatastoreBackend + ?Sized> ReplicationStore for T {
    #[cfg(test)]
    async fn apply_replicated_command(
        &self,
        command: StorageCommand,
        meta: CommandMeta,
    ) -> Result<()> {
        DatastoreBackend::apply_replicated_command(self, command, meta).await
    }

    async fn replace_replicated_resource_state(
        &self,
        entries: Vec<crate::log_apply::LogApplyCommit>,
        current_rv: i64,
        watch_event_high_water: Option<i64>,
        watch_replay_floors: Option<Vec<WatchReplayFloor>>,
        metadata: Option<ReplicatedSnapshotMetadata>,
    ) -> Result<()> {
        DatastoreBackend::replace_replicated_resource_state(
            self,
            entries,
            current_rv,
            watch_event_high_water,
            watch_replay_floors,
            metadata,
        )
        .await
    }

    async fn apply_log_apply_commit(&self, commit: crate::log_apply::LogApplyCommit) -> Result<()> {
        DatastoreBackend::apply_log_apply_commit(self, commit).await
    }

    async fn apply_raft_log_apply_commit(
        &self,
        commit: crate::log_apply::LogApplyCommit,
    ) -> Result<crate::datastore::raft::types::StorageCommandResult> {
        DatastoreBackend::apply_raft_log_apply_commit(self, commit).await
    }
}

#[async_trait]
impl<T: ReplicationStore + ?Sized> ReplicationStore for std::sync::Arc<T> {
    #[cfg(test)]
    async fn apply_replicated_command(
        &self,
        command: StorageCommand,
        meta: CommandMeta,
    ) -> Result<()> {
        self.as_ref().apply_replicated_command(command, meta).await
    }

    async fn replace_replicated_resource_state(
        &self,
        entries: Vec<crate::log_apply::LogApplyCommit>,
        current_rv: i64,
        watch_event_high_water: Option<i64>,
        watch_replay_floors: Option<Vec<WatchReplayFloor>>,
        metadata: Option<ReplicatedSnapshotMetadata>,
    ) -> Result<()> {
        self.as_ref()
            .replace_replicated_resource_state(
                entries,
                current_rv,
                watch_event_high_water,
                watch_replay_floors,
                metadata,
            )
            .await
    }

    async fn apply_log_apply_commit(&self, commit: crate::log_apply::LogApplyCommit) -> Result<()> {
        self.as_ref().apply_log_apply_commit(commit).await
    }

    async fn apply_raft_log_apply_commit(
        &self,
        commit: crate::log_apply::LogApplyCommit,
    ) -> Result<crate::datastore::raft::types::StorageCommandResult> {
        self.as_ref().apply_raft_log_apply_commit(commit).await
    }
}

/// Backend-local metadata keys.
#[async_trait]
pub trait MetaStore: Send + Sync {
    async fn get_klights_meta(&self, key: &str) -> Result<Option<String>>;
    async fn set_klights_meta(&self, key: &str, value: &str) -> Result<()>;
}

/// Selector for the watch-event publisher path used by a backend.
///
/// Defined at the trait layer so every backend reads from one type and
/// future variants don't fork match arms across modules. Today only
/// the runtime selector lives in each backend (`sqlite/watch_mode.rs`
/// in DSB-04); DSB-00 ships the type itself.
///
/// `#[non_exhaustive]` lets backends introduce variants in the future
/// without breaking external match exhaustiveness.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchBroadcastMode {
    /// Canonical post-F6-01b mode. Any backend whose mutation methods
    /// commit before returning uses this — SQLite (post-F6-01b), redb,
    /// and any future backend.
    PostCommitOnly,

    /// Phase 3 Raft FSM apply hook is the publisher on every node
    /// (leader and follower alike). Documented future variant; no
    /// DSB implementation. The runtime probe never returns this in
    /// DSB-00..DSB-07.
    RaftApply,

    /// SQLite-only transitional mode. No equivalent on other backends.
    #[deprecated(note = "SQLite update_hook coexistence; remove after F6-01b lands")]
    HookOnly,

    /// SQLite-only transitional mode (F6-01a partial: in-memory
    /// duplicate-suppression set coexists with hook).
    #[deprecated(note = "SQLite update_hook coexistence; remove after F6-01b lands")]
    HookWithDedup,
}

/// Handle to a datastore backend, suitable for sharing across runtime components.
///
/// API server, controllers, kubelet and networking hooks should depend on this
/// handle (or `&dyn DatastoreBackend`) rather than the concrete `Datastore`
/// type so that alternate backends (in-memory for tests, dual-DB SQLite for
/// production, replicated SQLite for HA) can be swapped without touching
/// runtime call sites.
///
/// New helper code can take `&dyn ResourceStore`, `&dyn WatchStore`, etc.
/// directly — the focused traits expose only the methods they need.
pub type DatastoreHandle = std::sync::Arc<dyn DatastoreBackend>;
