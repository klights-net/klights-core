//! `DatastoreBackend` is the runtime contract every higher-level klights
//! component depends on for state. The trait is SQL-free; backend
//! implementations live in sibling modules (`sqlite/` today, additional
//! backends slot in alongside).

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
#[cfg(test)]
use std::sync::Arc;

fn snapshot_replay_floor_cursor_key(
    cursor: &klights_cluster_store::SnapshotReplayFloorCursor,
) -> (String, String, String) {
    match cursor.target() {
        klights_cluster_store::DurableReplayTarget::All => {
            ("*".to_string(), "*".to_string(), "*".to_string())
        }
        klights_cluster_store::DurableReplayTarget::Cluster { api_version, kind } => {
            (api_version.clone(), kind.clone(), "#cluster".to_string())
        }
        klights_cluster_store::DurableReplayTarget::Namespaced {
            api_version,
            kind,
            namespace,
        } => (api_version.clone(), kind.clone(), namespace.clone()),
    }
}

use klights_cluster_core::{PodEndpointEffect, ResourceMutationEffect};
pub use klights_cluster_store::CommittedOutboxApply;

async fn apply_with_default_endpoint_effect<F, Fut>(
    is_pod_status: bool,
    apply: F,
) -> std::result::Result<CommittedOutboxApply, klights_cluster_core::OutboxApplyError>
where
    F: FnOnce() -> Fut + Send,
    Fut: std::future::Future<
            Output = std::result::Result<
                klights_cluster_core::OutboxApplyOutcome,
                klights_cluster_core::OutboxApplyError,
            >,
        > + Send,
{
    if is_pod_status {
        return Err(klights_cluster_core::OutboxApplyError::Retryable(
            "backend must implement an atomic Pod endpoint effect for v1/Pod status apply"
                .to_string(),
        ));
    }

    let result = apply().await?;
    let resource_effect = if matches!(
        result,
        klights_cluster_core::OutboxApplyOutcome::Applied { .. }
    ) {
        ResourceMutationEffect::Changed
    } else {
        ResourceMutationEffect::Unchanged
    };
    Ok(CommittedOutboxApply::new(
        result,
        resource_effect,
        PodEndpointEffect::NotApplicable,
    ))
}
#[cfg(test)]
use tokio::sync::broadcast;

use klights_cluster_core::command::StorageCommand;
#[cfg(test)]
use klights_watch::WatchTopic;
#[cfg(test)]
use klights_watch::{WatchEvent, WatchReceiver};

#[cfg(test)]
use crate::datastore::ReplicatedCreateOptions;
use klights_cluster_core::{
    LogApplyAppliedOutboxRow, LogApplyPodCleanupIntentRow, PatchKind, Resource,
    ResourceBatchOperation, ResourcePatchRequest, ResourcePreconditions, WatchReplayPosition,
};
#[cfg(test)]
use klights_cluster_store::StagedPostCommit;
use klights_cluster_store::{
    CatchUpResource, ClusterMetadataObservation, DurableAllocatorObservation, ListPageRequest,
    PositionedWatchReplayRead, ReplicatedSnapshotMetadata, ResourceList, ResourceListOptions,
    SnapshotAtRv, WatchReplayFloor, WatchReplayRead, WatchTarget,
};

pub use klights_cluster_store::{SnapshotExclusiveFence, SnapshotMutationFence};

#[cfg(test)]
pub use klights_cluster_store::CommitObservationSink;

/// `DatastoreBackend` is the runtime contract. Every state operation goes
/// through this trait.
///
/// **Phase 3 will add:** `async fn snapshot(&self) -> Result<SnapshotHandle>`
/// for Raft FSM log compaction. SQLite impls via `online_backup`; KV impls
/// (redb, etc.) via MVCC reader. Not on the trait today because no caller
/// exists.
#[async_trait]
pub trait DatastoreBackend: Send + Sync {
    #[cfg(test)]
    fn commit_observation_sink(&self) -> Arc<dyn CommitObservationSink>;
    /// Atomically observe both durable allocators and their persisted mode.
    async fn read_durable_allocator_observation(&self) -> Result<DurableAllocatorObservation> {
        Err(anyhow::anyhow!(
            "datastore backend does not implement atomic allocator observation"
        ))
    }

    /// Atomically and strictly parse cluster identity plus membership.
    async fn read_cluster_metadata_observation(&self) -> Result<ClusterMetadataObservation> {
        Err(anyhow::anyhow!(
            "datastore backend does not implement atomic cluster metadata observation"
        ))
    }

    async fn acquire_snapshot_exclusive_fence(&self) -> Result<Option<SnapshotExclusiveFence>> {
        Ok(None)
    }

    async fn acquire_snapshot_mutation_fence(&self) -> Result<Option<SnapshotMutationFence>> {
        Ok(None)
    }

    async fn begin_pinned_snapshot_capture(
        &self,
        _request: klights_cluster_store::SnapshotCaptureRequest,
        _fence: SnapshotExclusiveFence,
    ) -> Result<Box<dyn klights_cluster_store::SnapshotCaptureSession>> {
        Err(anyhow::anyhow!(
            "datastore backend does not implement pinned snapshot capture"
        ))
    }

    /// Release backend-specific resources (file locks, connections, etc.)
    /// after graceful shutdown work is complete.  No-op by default.
    fn close(&self) {}

    #[cfg(test)]
    fn subscribe_watch(&self, _topic: WatchTopic) -> broadcast::Receiver<WatchEvent> {
        panic!("watch event subscription is unavailable for this datastore adapter")
    }

    #[cfg(test)]
    fn subscribe_watch_many(&self, topics: Vec<WatchTopic>) -> WatchReceiver;

    #[cfg(test)]
    fn broadcast_watch_event(&self, pending: StagedPostCommit);

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
        entries: Vec<klights_cluster_core::SnapshotRestoreOperation>,
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
    async fn apply_log_apply_commit(
        &self,
        commit: klights_cluster_core::LogApplyCommit,
    ) -> Result<()> {
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
        _commit: klights_cluster_core::LogApplyCommit,
    ) -> Result<klights_cluster_store::StorageCommandResult> {
        Err(anyhow::anyhow!(
            "datastore backend does not expose application-side committed raft apply"
        ))
    }

    /// Apply one committed Raft entry and return the canonical outcome derived
    /// inside the same datastore transaction. Backends must fail before
    /// mutation when they cannot provide this atomic classification.
    async fn apply_raft_log_apply_commit_receipt(
        &self,
        _commit: klights_cluster_core::LogApplyCommit,
    ) -> Result<klights_cluster_store::CommittedRaftApplyReceipt> {
        Err(anyhow::anyhow!(
            "datastore backend does not support atomic committed-apply receipts"
        ))
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
        let incoming_uid = Resource::uid_from_data(&data);
        if let Some(expected_uid) = options.meta_uid.as_deref()
            && !incoming_uid.is_empty()
            && expected_uid != incoming_uid
        {
            return Err(klights_cluster_datastore::errors::DatastoreError::conflict(format!(
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
        query: ResourceListOptions<'_>,
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
    ) -> Result<Resource>;

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
    ) -> Result<WatchReplayRead<klights_cluster_store::DurableRawWatchEvent>>;

    async fn list_raw_watch_events_after_position_checked_bounded(
        &self,
        _targets: &[WatchTarget],
        _position: WatchReplayPosition,
        _limit: std::num::NonZeroUsize,
    ) -> Result<PositionedWatchReplayRead<klights_cluster_store::DurableRawWatchEvent>> {
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
        _query: ResourceListOptions<'_>,
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

    async fn list_watch_replay_floors_paged(
        &self,
        after: Option<&klights_cluster_store::SnapshotReplayFloorCursor>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<WatchReplayFloor>> {
        if limit.get() > klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE {
            return Err(anyhow::anyhow!(
                "watch replay-floor page limit {} exceeds {}",
                limit,
                klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE
            ));
        }
        let after = after.map(snapshot_replay_floor_cursor_key);
        let mut rows = self.list_watch_replay_floors().await?;
        rows.retain(|row| {
            after.as_ref().is_none_or(|cursor| {
                (
                    row.api_version.as_str(),
                    row.kind.as_str(),
                    row.namespace_key.as_str(),
                ) > (cursor.0.as_str(), cursor.1.as_str(), cursor.2.as_str())
            })
        });
        rows.truncate(limit.get());
        Ok(rows)
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
    ) -> Result<klights_cluster_store::StoredNodeSubnet>;

    /// F2-04: persist peer-mode + hostport-range projected from
    /// `klights.io/mode` / `klights.io/hostport-range` Node annotations.
    async fn update_node_peer_attributes(
        &self,
        node_name: &str,
        mode: klights_types::NodePeerMode,
        hostport_range: Option<klights_types::HostPortRange>,
    ) -> Result<()>;

    /// Persist cluster-visible dataplane metadata for a node. The metadata
    /// must already be validated and must not contain any private key material.
    async fn update_node_dataplane(
        &self,
        metadata: klights_cluster_store::DataplanePeerMetadata,
    ) -> Result<()> {
        let _ = metadata;
        Err(anyhow::anyhow!(
            "backend does not support node dataplane metadata"
        ))
    }

    async fn get_node_dataplane(
        &self,
        node_name: &str,
    ) -> Result<Option<klights_cluster_store::DataplanePeerMetadata>> {
        let _ = node_name;
        Err(anyhow::anyhow!(
            "backend does not support node dataplane metadata"
        ))
    }

    /// Get node subnet record.
    async fn get_node_subnet(
        &self,
        node_name: &str,
    ) -> Result<Option<klights_cluster_store::StoredNodeSubnet>>;

    /// List peer node subnets. Includes root and rootless peers.
    async fn list_peer_subnets(
        &self,
        request: klights_cluster_store::PeerTopologyRequest,
    ) -> Result<Vec<klights_cluster_store::StoredNodeSubnet>>;

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
    ) -> Result<Vec<LogApplyPodCleanupIntentRow>> {
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
    ) -> Result<Vec<klights_cluster_core::OutboxStreamWatermark>> {
        Err(anyhow::anyhow!(
            "backend does not support outbox stream watermark listing"
        ))
    }

    async fn list_outbox_stream_watermarks_paged(
        &self,
        after: Option<&klights_cluster_store::SnapshotOutboxWatermarkCursor>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<klights_cluster_core::OutboxStreamWatermark>> {
        if limit.get() > klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE {
            return Err(anyhow::anyhow!(
                "outbox-watermark page limit {} exceeds {}",
                limit,
                klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE
            ));
        }
        let mut rows = self.list_outbox_stream_watermarks().await?;
        rows.retain(|row| {
            after.is_none_or(|cursor| {
                (row.client_id.as_str(), row.stream_id) > (cursor.client_id(), cursor.stream_id())
            })
        });
        rows.truncate(limit.get());
        Ok(rows)
    }

    async fn get_applied_outbox(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<LogApplyAppliedOutboxRow>>;

    async fn insert_applied_outbox(&self, record: LogApplyAppliedOutboxRow) -> Result<bool>;

    async fn list_applied_outbox(&self) -> Result<Vec<LogApplyAppliedOutboxRow>> {
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
    ) -> Result<Vec<LogApplyAppliedOutboxRow>> {
        // memory-improvement.md §10 P1: default fallback — load the full
        // ledger and filter by `idempotency_key` in memory. Production sqlite
        // overrides with a real keyset query; this preserves pre-P1 behavior
        // for backends that don't.
        let limit = limit.get();
        let rows = self.list_applied_outbox().await?;
        let mut out: Vec<LogApplyAppliedOutboxRow> = Vec::with_capacity(rows.len().min(limit));
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

    /// Apply an outbox payload transactionally: check idempotency, apply
    /// mutation, and insert ledger row all in one cluster.db transaction.
    async fn apply_outbox_transactionally(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
    ) -> std::result::Result<
        klights_cluster_core::OutboxApplyOutcome,
        klights_cluster_core::OutboxApplyError,
    >;

    async fn apply_outbox_transactionally_with_watermark(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<
        klights_cluster_core::OutboxApplyOutcome,
        klights_cluster_core::OutboxApplyError,
    > {
        let _ = watermark;
        self.apply_outbox_transactionally(idempotency_key, operation, command, authoring_node)
            .await
    }

    /// Internal committed-apply signal used to decide whether a newly applied
    /// outbox entry produced a resource effect. Backends with a canonical
    /// commit outcome override this so callers do not infer the answer with an
    /// additional resource lookup.
    async fn apply_outbox_transactionally_with_watermark_effect(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<CommittedOutboxApply, klights_cluster_core::OutboxApplyError> {
        let is_pod_status = matches!(
            &command,
            StorageCommand::UpdateStatus {
                api_version,
                kind,
                ..
            } if api_version == "v1" && kind == "Pod"
        );
        apply_with_default_endpoint_effect(is_pod_status, || {
            self.apply_outbox_transactionally_with_watermark(
                idempotency_key,
                operation,
                command,
                authoring_node,
                watermark,
            )
        })
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
    ) -> Result<klights_cluster_core::LogApplyCommit> {
        let _ = (command, operation, authoring_node);
        Err(anyhow::anyhow!(
            "backend does not support generic raft commit materialization"
        ))
    }

    /// T1.3/T1.4: build a `LogApplyCommit` from an outbox payload WITHOUT
    /// applying it. The leader's raft proposer encodes the returned commit
    /// as the fixed committed-apply-v1 Raft entry payload and
    /// submits through `client_write`; the state machine apply path on every
    /// node is the only caller that actually mutates `cluster.db` (via
    /// `apply_log_apply_commit`).
    async fn build_log_apply_commit_for_outbox(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
    ) -> std::result::Result<
        klights_cluster_core::BuildOutboxOutcome,
        klights_cluster_core::OutboxApplyError,
    >;
    async fn build_log_apply_commit_for_outbox_with_watermark(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<
        klights_cluster_core::BuildOutboxOutcome,
        klights_cluster_core::OutboxApplyError,
    > {
        let _ = watermark;
        self.build_log_apply_commit_for_outbox(idempotency_key, operation, command, authoring_node)
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
    ) -> Result<PositionedWatchReplayRead<klights_cluster_store::DurableRawWatchEvent>>;
}

/// Watch-event subscription, broadcast access, and replay queries.
#[async_trait]
pub trait WatchStore: Send + Sync {
    #[cfg(test)]
    fn subscribe_watch(&self, _topic: WatchTopic) -> broadcast::Receiver<WatchEvent> {
        panic!("watch event subscription is unavailable for this datastore adapter")
    }
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
    async fn list_all_watch_events_since_paged(
        &self,
        since_rv: i64,
        after_resource_version: i64,
        after_id: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<(i64, CatchUpResource)>>;
    async fn list_watch_replay_floors(&self) -> Result<Vec<WatchReplayFloor>>;
    async fn list_watch_replay_floors_paged(
        &self,
        after: Option<&klights_cluster_store::SnapshotReplayFloorCursor>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<WatchReplayFloor>>;
    async fn list_deleted_watch_events_since(&self, since_rv: i64) -> Result<Vec<CatchUpResource>>;
    async fn advance_resource_version_after(&self, min_rv: i64) -> Result<i64>;
    async fn watch_events_gc_prunable_count(&self, max_rows: i64, batch_cap: i64) -> Result<usize>;
    async fn gc_watch_events(&self, max_rows: i64, batch_cap: i64) -> Result<usize>;
}

/// Cluster-owned node subnet and dataplane metadata.
#[async_trait]
pub trait NetworkMetadataStore: Send + Sync {
    async fn allocate_node_subnet(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<klights_cluster_store::StoredNodeSubnet>;
    async fn update_node_peer_attributes(
        &self,
        node_name: &str,
        mode: klights_types::NodePeerMode,
        hostport_range: Option<klights_types::HostPortRange>,
    ) -> Result<()>;
    async fn update_node_dataplane(
        &self,
        metadata: klights_cluster_store::DataplanePeerMetadata,
    ) -> Result<()>;
    async fn get_node_dataplane(
        &self,
        node_name: &str,
    ) -> Result<Option<klights_cluster_store::DataplanePeerMetadata>>;
    async fn get_node_subnet(
        &self,
        node_name: &str,
    ) -> Result<Option<klights_cluster_store::StoredNodeSubnet>>;
    async fn list_peer_subnets(
        &self,
        request: klights_cluster_store::PeerTopologyRequest,
    ) -> Result<Vec<klights_cluster_store::StoredNodeSubnet>>;
    async fn delete_node_subnet(&self, node_name: &str) -> Result<()>;
}

/// Namespace lifecycle (create, get, delete, list contents).
#[async_trait]
pub trait NamespaceStore: Send + Sync {
    async fn create_namespace(&self, name: &str, data: Value) -> Result<Resource>;
    async fn get_namespace(&self, name: &str) -> Result<Option<Resource>>;
    #[cfg(test)]
    async fn seed_namespace_for_test(&self, name: &str);
    async fn list_namespaces(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
    ) -> Result<ResourceList>;
    async fn list_namespaces_page(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList>;
    async fn update_namespace(&self, name: &str, data: Value, expected_rv: i64)
    -> Result<Resource>;
    async fn delete_namespace(&self, name: &str) -> Result<()>;
    async fn delete_namespace_observed_rv(&self, name: &str) -> Result<i64>;
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
    async fn replace_replicated_resource_state(
        &self,
        entries: Vec<klights_cluster_core::SnapshotRestoreOperation>,
        current_rv: i64,
        watch_event_high_water: Option<i64>,
        watch_replay_floors: Option<Vec<WatchReplayFloor>>,
        metadata: Option<ReplicatedSnapshotMetadata>,
    ) -> Result<()>;
    async fn apply_log_apply_commit(
        &self,
        commit: klights_cluster_core::LogApplyCommit,
    ) -> Result<()>;
    async fn apply_raft_log_apply_commit(
        &self,
        commit: klights_cluster_core::LogApplyCommit,
    ) -> Result<klights_cluster_store::StorageCommandResult>;
    async fn apply_raft_log_apply_commit_receipt(
        &self,
        commit: klights_cluster_core::LogApplyCommit,
    ) -> Result<klights_cluster_store::CommittedRaftApplyReceipt>;
    #[cfg(test)]
    async fn apply_replicated_create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        options: ReplicatedCreateOptions,
    ) -> Result<Resource>;
}

#[async_trait]
impl<T: ReplicationStore + ?Sized> ReplicationStore for std::sync::Arc<T> {
    async fn replace_replicated_resource_state(
        &self,
        entries: Vec<klights_cluster_core::SnapshotRestoreOperation>,
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

    async fn apply_log_apply_commit(
        &self,
        commit: klights_cluster_core::LogApplyCommit,
    ) -> Result<()> {
        self.as_ref().apply_log_apply_commit(commit).await
    }

    async fn apply_raft_log_apply_commit(
        &self,
        commit: klights_cluster_core::LogApplyCommit,
    ) -> Result<klights_cluster_store::StorageCommandResult> {
        self.as_ref().apply_raft_log_apply_commit(commit).await
    }

    async fn apply_raft_log_apply_commit_receipt(
        &self,
        commit: klights_cluster_core::LogApplyCommit,
    ) -> Result<klights_cluster_store::CommittedRaftApplyReceipt> {
        self.as_ref()
            .apply_raft_log_apply_commit_receipt(commit)
            .await
    }

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
        self.as_ref()
            .apply_replicated_create_resource(api_version, kind, namespace, name, data, options)
            .await
    }
}

/// Atomic cluster-state observations used by authoritative recovery capture.
#[async_trait]
pub trait DurableRecoveryStore: Send + Sync {
    async fn read_durable_allocator_observation(&self) -> Result<DurableAllocatorObservation>;
    async fn read_cluster_metadata_observation(&self) -> Result<ClusterMetadataObservation>;
    async fn begin_pinned_snapshot_capture(
        &self,
        request: klights_cluster_store::SnapshotCaptureRequest,
        fence: SnapshotExclusiveFence,
    ) -> Result<Box<dyn klights_cluster_store::SnapshotCaptureSession>>;
}

#[async_trait]
impl<T: DurableRecoveryStore + ?Sized> DurableRecoveryStore for std::sync::Arc<T> {
    async fn read_durable_allocator_observation(&self) -> Result<DurableAllocatorObservation> {
        self.as_ref().read_durable_allocator_observation().await
    }

    async fn read_cluster_metadata_observation(&self) -> Result<ClusterMetadataObservation> {
        self.as_ref().read_cluster_metadata_observation().await
    }

    async fn begin_pinned_snapshot_capture(
        &self,
        request: klights_cluster_store::SnapshotCaptureRequest,
        fence: SnapshotExclusiveFence,
    ) -> Result<Box<dyn klights_cluster_store::SnapshotCaptureSession>> {
        self.as_ref()
            .begin_pinned_snapshot_capture(request, fence)
            .await
    }
}

#[cfg(test)]
pub(crate) fn root_cluster_store_error(
    error: anyhow::Error,
) -> klights_cluster_store::ClusterStoreError {
    klights_cluster_datastore::map_cluster_store_adapter_error(
        error,
        klights_cluster_store::PersistenceBackend::Root,
        "root datastore compatibility adapter",
    )
}

/// Test-only watch bus controls.
#[cfg(test)]
pub trait TestWatchStore: Send + Sync {
    fn subscribe_watch_many(&self, topics: Vec<WatchTopic>) -> WatchReceiver;
    fn broadcast_watch_event(&self, pending: StagedPostCommit);
}

/// Broad read queries used by leader-side controllers and API helpers.
#[async_trait]
pub trait ClusterResourceQueryStore: Send + Sync {
    async fn list_resources(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        query: ResourceListOptions<'_>,
    ) -> Result<ResourceList>;
    async fn list_resources_for_watch_targets(
        &self,
        _targets: &[WatchTarget],
        _label_selector: Option<&str>,
    ) -> Result<ResourceList>;
    async fn list_cluster_resources(&self) -> Result<Vec<Resource>>;
}

/// Leader-owned mutations that must not leak into worker/kubelet code.
#[allow(clippy::too_many_arguments)]
#[async_trait]
pub trait LeaderResourceMutationStore: Send + Sync {
    async fn update_main_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource>;
    async fn apply_resource_batch(&self, operations: Vec<ResourceBatchOperation>) -> Result<()>;
    async fn delete_resource_with_preconditions_observed_rv(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> Result<i64>;
    async fn mark_for_delete_without_watch(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
        grace_seconds: i64,
    ) -> Result<Option<Resource>>;
    async fn delete_resource_without_watch_with_tombstone(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
        grace_seconds: i64,
    ) -> Result<Resource>;
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
}

/// Durable watch history queries that are not part of live watch bootstrap.
#[async_trait]
pub trait WatchMaintenanceStore: Send + Sync {
    async fn list_raw_watch_events_since_checked_bounded(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<WatchReplayRead<klights_cluster_store::DurableRawWatchEvent>>;
    async fn snapshot_resources_at_rv(
        &self,
        _api_version: &str,
        _kind: &str,
        _namespace: Option<&str>,
        _query: ResourceListOptions<'_>,
        snapshot_rv: i64,
    ) -> Result<SnapshotAtRv>;
    async fn list_all_watch_events_after_id_bounded(
        &self,
        after_id: i64,
        through_id: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<(i64, CatchUpResource)>>;
}

/// Actor-owned Pod cleanup and same-name slot admission state.
#[allow(clippy::too_many_arguments)]
#[async_trait]
pub trait PodCleanupStore: Send + Sync {
    async fn move_pod_to_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()>;
    async fn list_pod_cleanup_intents_for_node(
        &self,
        node_name: &str,
    ) -> Result<Vec<LogApplyPodCleanupIntentRow>>;
    async fn delete_pod_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()>;
    async fn delete_pod_cleanup_intents_for_node(&self, node_name: &str) -> Result<()>;
}

/// Applied-outbox ledger and commit-building state.
#[async_trait]
pub trait AppliedOutboxStore: Send + Sync {
    async fn applied_outbox_gc_prunable_count(&self, cutoff_ms: i64) -> Result<usize>;
    async fn list_outbox_stream_watermarks(
        &self,
    ) -> Result<Vec<klights_cluster_core::OutboxStreamWatermark>>;
    async fn list_outbox_stream_watermarks_paged(
        &self,
        after: Option<&klights_cluster_store::SnapshotOutboxWatermarkCursor>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<klights_cluster_core::OutboxStreamWatermark>>;
    async fn get_applied_outbox(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<LogApplyAppliedOutboxRow>>;
    async fn insert_applied_outbox(&self, record: LogApplyAppliedOutboxRow) -> Result<bool>;
    async fn list_applied_outbox(&self) -> Result<Vec<LogApplyAppliedOutboxRow>>;
    async fn list_applied_outbox_paged(
        &self,
        after_key: Option<&str>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<LogApplyAppliedOutboxRow>>;
    async fn apply_outbox_transactionally(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
    ) -> std::result::Result<
        klights_cluster_core::OutboxApplyOutcome,
        klights_cluster_core::OutboxApplyError,
    >;
    async fn apply_outbox_transactionally_with_watermark(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<
        klights_cluster_core::OutboxApplyOutcome,
        klights_cluster_core::OutboxApplyError,
    >;
    async fn apply_outbox_transactionally_with_watermark_effect(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<CommittedOutboxApply, klights_cluster_core::OutboxApplyError>;
    async fn build_log_apply_commit_for_command(
        &self,
        command: StorageCommand,
        operation: &str,
        authoring_node: &str,
    ) -> Result<klights_cluster_core::LogApplyCommit>;
    async fn build_log_apply_commit_for_outbox(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
    ) -> std::result::Result<
        klights_cluster_core::BuildOutboxOutcome,
        klights_cluster_core::OutboxApplyError,
    >;
    async fn build_log_apply_commit_for_outbox_with_watermark(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> std::result::Result<
        klights_cluster_core::BuildOutboxOutcome,
        klights_cluster_core::OutboxApplyError,
    >;
    async fn gc_applied_outbox(&self, now_ms: i64, ttl_ms: i64) -> Result<usize>;
}

/// Backend-local metadata keys.
#[async_trait]
pub trait MetaStore: Send + Sync {
    async fn get_klights_meta(&self, key: &str) -> Result<Option<String>>;
    async fn set_klights_meta(&self, key: &str, value: &str) -> Result<()>;
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

#[cfg(test)]
mod endpoint_effect_default_tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::apply_with_default_endpoint_effect;

    struct FakeBackend {
        mutation_called: AtomicBool,
    }

    impl FakeBackend {
        async fn apply(
            &self,
        ) -> Result<klights_cluster_core::OutboxApplyOutcome, klights_cluster_core::OutboxApplyError>
        {
            self.mutation_called.store(true, Ordering::SeqCst);
            Ok(klights_cluster_core::OutboxApplyOutcome::Applied { applied_rv: 1 })
        }
    }

    #[tokio::test]
    async fn fake_backend_cannot_apply_pod_status_without_atomic_endpoint_effect() {
        let backend = FakeBackend {
            mutation_called: AtomicBool::new(false),
        };

        let error = match apply_with_default_endpoint_effect(true, || backend.apply()).await {
            Err(error) => error,
            Ok(_) => panic!("default endpoint-effect path must fail closed"),
        };

        assert!(matches!(
            error,
            klights_cluster_core::OutboxApplyError::Retryable(message)
                if message.contains("atomic Pod endpoint effect")
        ));
        assert!(
            !backend.mutation_called.load(Ordering::SeqCst),
            "fallback must reject before a backend mutation can commit"
        );
    }
}

#[cfg(test)]
mod cluster_store_error_tests {
    use std::error::Error as _;

    use super::root_cluster_store_error;

    #[test]
    fn root_cluster_store_mapper_keeps_boundary_and_source() {
        let error = root_cluster_store_error(anyhow::Error::msg("storage paused"));

        assert_eq!(
            error.kind(),
            klights_cluster_store::ClusterStoreErrorKind::Persistence
        );
        assert_eq!(
            error.backend(),
            Some(klights_cluster_store::PersistenceBackend::Root)
        );
        assert_eq!(error.operation(), "root datastore compatibility adapter");
        assert!(error.source().is_some());
    }
}
