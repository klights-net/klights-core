//! `DatastoreBackend` is the runtime contract every higher-level klights
//! component depends on for state. The trait is SQL-free; backend
//! implementations live in sibling modules (`sqlite/` today, additional
//! backends slot in alongside).

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::broadcast;

use crate::networking::VtepMac;
use crate::watch::{WatchEvent, WatchReceiver, WatchTopic};

use super::command::{CommandMeta, StorageCommand};
use super::types::{
    AppliedOutboxRecord, CatchUpResource, ListPageRequest, NodeSubnet, PatchKind,
    PendingWatchEvent, PodCleanupIntent, PodEndpointEvent, PodEndpointRow, PodNetworkEndpoint,
    PodSlotAdmissionEvent, PodSlotAdmissionResult, PodWorkqueueEntry, PodWorkqueueKind,
    ReplicatedCreateOptions, ReplicatedSnapshotMetadata, Resource, ResourceList, ResourceListQuery,
    ResourcePatchRequest, ResourcePreconditions, SandboxRef, SnapshotAtRv, WatchReplayRead,
    WatchTarget,
};

/// `DatastoreBackend` is the runtime contract. Every state operation goes
/// through this trait.
///
/// **Phase 3 will add:** `async fn snapshot(&self) -> Result<SnapshotHandle>`
/// for Raft FSM log compaction. SQLite impls via `online_backup`; KV impls
/// (redb, etc.) via MVCC reader. Not on the trait today because no caller
/// exists.
#[async_trait]
pub trait DatastoreBackend: Send + Sync {
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

    fn subscribe_watch(&self, topic: WatchTopic) -> broadcast::Receiver<WatchEvent>;
    fn subscribe_watch_many(&self, topics: Vec<WatchTopic>) -> WatchReceiver;

    /// Broadcast a watch event after DB transaction commits.
    fn broadcast_watch_event(&self, pending: PendingWatchEvent);

    /// Apply a replicated command locally without going through role-based
    /// public write admission.  Leaders use this for forwarded writes after
    /// bootstrap-token validation; replicas use it for snapshot and stream apply.
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
        metadata: Option<ReplicatedSnapshotMetadata>,
    ) -> Result<()> {
        let _ = (entries, current_rv, metadata);
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

    /// Apply an authoritative leader `CreateResource` entry on a local replica.
    ///
    /// This is not the public Kubernetes create path. Public creates must keep
    /// rejecting existing names. Replicated creates converge a follower cache to
    /// the leader's object identity, including delete/recreate slots where the
    /// same name now has a different UID.
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

    /// Update `vtep_mac` for a node after creating the VXLAN interface.
    async fn update_node_vtep_mac(&self, node_name: &str, vtep_mac: &VtepMac) -> Result<()>;

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

    /// List peer node subnets. F2-04: includes rootless peers (no `vtep_mac`).
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

    async fn delete_uncommitted_applied_outbox_placeholder(
        &self,
        idempotency_key: &str,
    ) -> Result<bool> {
        let _ = idempotency_key;
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

    /// T1.3/T1.4: build a `LogApplyCommit` from an outbox payload WITHOUT
    /// applying it. The leader's raft proposer encodes the returned commit
    /// as the raft entry payload and submits through `client_write`; the
    /// state machine apply path on every node is the only caller that
    /// actually mutates `cluster.db` (via `apply_log_apply_commit`).
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

#[async_trait]
impl<T: DatastoreBackend + ?Sized> ResourceListStore for T {
    async fn list_resources_page(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList> {
        DatastoreBackend::list_resources_page(
            self,
            api_version,
            kind,
            namespace,
            label_selector,
            field_selector,
            page,
        )
        .await
    }

    async fn list_resource_keys_for_scope(
        &self,
        api_version: String,
        kind: String,
        namespaced: bool,
    ) -> Result<Vec<(Option<String>, String)>> {
        DatastoreBackend::list_resource_keys_for_scope(self, api_version, kind, namespaced).await
    }
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

#[async_trait]
impl<T: DatastoreBackend + ?Sized> StatusStore for T {
    async fn update_status_only(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: Value,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        DatastoreBackend::update_status_only(
            self,
            api_version,
            kind,
            namespace,
            name,
            status,
            expected_rv,
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
    ) -> Result<Resource> {
        DatastoreBackend::update_status_only_with_preconditions(
            self,
            api_version,
            kind,
            namespace,
            name,
            status,
            preconditions,
        )
        .await
    }
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

#[async_trait]
impl<T: DatastoreBackend + ?Sized> OwnershipStore for T {
    async fn find_owned_resources(
        &self,
        owner_uid: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        DatastoreBackend::find_owned_resources(self, owner_uid, namespace).await
    }

    async fn list_resources_by_owner_uid(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        owner_uid: &str,
    ) -> Result<Vec<Resource>> {
        DatastoreBackend::list_resources_by_owner_uid(self, api_version, kind, namespace, owner_uid)
            .await
    }

    async fn find_owned_by_name_kind_empty_uid(
        &self,
        owner_api_version: &str,
        owner_name: &str,
        owner_kind: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        DatastoreBackend::find_owned_by_name_kind_empty_uid(
            self,
            owner_api_version,
            owner_name,
            owner_kind,
            namespace,
        )
        .await
    }
}

/// Watch-event subscription, broadcast access, and replay queries.
#[async_trait]
pub trait WatchStore: Send + Sync {
    fn subscribe_watch(&self, topic: WatchTopic) -> broadcast::Receiver<WatchEvent>;
    fn subscribe_watch_many(&self, topics: Vec<WatchTopic>) -> WatchReceiver;
    async fn list_watch_events_since(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>>;
}

#[async_trait]
impl<T: DatastoreBackend + ?Sized> WatchStore for T {
    fn subscribe_watch(&self, topic: WatchTopic) -> broadcast::Receiver<WatchEvent> {
        DatastoreBackend::subscribe_watch(self, topic)
    }
    fn subscribe_watch_many(&self, topics: Vec<WatchTopic>) -> WatchReceiver {
        DatastoreBackend::subscribe_watch_many(self, topics)
    }
    async fn list_watch_events_since(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        DatastoreBackend::list_watch_events_since(self, targets, since_rv).await
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

#[async_trait]
impl<T: DatastoreBackend + ?Sized> WatchHistoryStore for T {
    async fn list_cluster_resources_modified_since(
        &self,
        api_version: &str,
        kind: &str,
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        DatastoreBackend::list_cluster_resources_modified_since(self, api_version, kind, since_rv)
            .await
    }

    async fn list_resources_modified_since(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        DatastoreBackend::list_resources_modified_since(
            self,
            api_version,
            kind,
            namespace,
            since_rv,
        )
        .await
    }

    async fn list_all_watch_events_since(&self, since_rv: i64) -> Result<Vec<CatchUpResource>> {
        DatastoreBackend::list_all_watch_events_since(self, since_rv).await
    }

    async fn list_deleted_watch_events_since(&self, since_rv: i64) -> Result<Vec<CatchUpResource>> {
        DatastoreBackend::list_deleted_watch_events_since(self, since_rv).await
    }

    async fn advance_resource_version_after(&self, min_rv: i64) -> Result<i64> {
        DatastoreBackend::advance_resource_version_after(self, min_rv).await
    }

    async fn watch_events_gc_prunable_count(&self, max_rows: i64, batch_cap: i64) -> Result<usize> {
        DatastoreBackend::watch_events_gc_prunable_count(self, max_rows, batch_cap).await
    }

    async fn gc_watch_events(&self, max_rows: i64, batch_cap: i64) -> Result<usize> {
        DatastoreBackend::gc_watch_events(self, max_rows, batch_cap).await
    }
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

#[async_trait]
impl<T: DatastoreBackend + ?Sized> NetworkStore for T {
    async fn record_sandbox(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        sandbox_id: &str,
    ) -> Result<()> {
        DatastoreBackend::record_sandbox(self, namespace, pod_name, pod_uid, sandbox_id).await
    }
    async fn get_sandbox(&self, namespace: &str, pod_name: &str) -> Result<Option<String>> {
        DatastoreBackend::get_sandbox(self, namespace, pod_name).await
    }
    async fn delete_sandbox(&self, namespace: &str, pod_name: &str) -> Result<()> {
        DatastoreBackend::delete_sandbox(self, namespace, pod_name).await
    }
    async fn delete_sandbox_for_uid(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        sandbox_id: &str,
    ) -> Result<()> {
        DatastoreBackend::delete_sandbox_for_uid(self, namespace, pod_name, pod_uid, sandbox_id)
            .await
    }
    async fn delete_pod_network(&self, sandbox_id: &str) -> Result<()> {
        DatastoreBackend::delete_pod_network(self, sandbox_id).await
    }
    async fn get_pod_network(&self, sandbox_id: &str) -> Result<Option<PodNetworkEndpoint>> {
        DatastoreBackend::get_pod_network(self, sandbox_id).await
    }
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
    async fn update_node_vtep_mac(&self, node_name: &str, vtep_mac: &VtepMac) -> Result<()>;
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

#[async_trait]
impl<T: DatastoreBackend + ?Sized> NetworkMetadataStore for T {
    async fn get_sandbox_for_uid(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
    ) -> Result<Option<String>> {
        DatastoreBackend::get_sandbox_for_uid(self, namespace, pod_name, pod_uid).await
    }

    async fn get_pod_network_for_pod(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
    ) -> Result<Option<PodNetworkEndpoint>> {
        DatastoreBackend::get_pod_network_for_pod(self, namespace, pod_name, pod_uid).await
    }

    async fn ipam_allocate_and_record_pod_network(
        &self,
        sandbox_id: &str,
        pod: &crate::pod_identity::PodIdentity,
        subnet_base_int: u32,
        subnet_size: u32,
        veth_host: &str,
        netns_path: &str,
    ) -> Result<(String, u32)> {
        DatastoreBackend::ipam_allocate_and_record_pod_network(
            self,
            sandbox_id,
            pod,
            subnet_base_int,
            subnet_size,
            veth_host,
            netns_path,
        )
        .await
    }

    async fn list_sandboxes(&self) -> Result<Vec<SandboxRef>> {
        DatastoreBackend::list_sandboxes(self).await
    }

    async fn list_pod_network_sandbox_ids(&self) -> Result<Vec<String>> {
        DatastoreBackend::list_pod_network_sandbox_ids(self).await
    }

    async fn allocate_node_subnet(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<NodeSubnet> {
        DatastoreBackend::allocate_node_subnet(self, node_name, cluster_cidr, node_ip).await
    }

    async fn update_node_vtep_mac(&self, node_name: &str, vtep_mac: &VtepMac) -> Result<()> {
        DatastoreBackend::update_node_vtep_mac(self, node_name, vtep_mac).await
    }

    async fn update_node_peer_attributes(
        &self,
        node_name: &str,
        mode: crate::controllers::annotations::NodePeerMode,
        hostport_range: Option<crate::networking::types::HostPortRange>,
    ) -> Result<()> {
        DatastoreBackend::update_node_peer_attributes(self, node_name, mode, hostport_range).await
    }

    async fn update_node_dataplane(
        &self,
        metadata: crate::networking::wireguard::DataplanePeerMetadata,
    ) -> Result<()> {
        DatastoreBackend::update_node_dataplane(self, metadata).await
    }

    async fn get_node_dataplane(
        &self,
        node_name: &str,
    ) -> Result<Option<crate::networking::wireguard::DataplanePeerMetadata>> {
        DatastoreBackend::get_node_dataplane(self, node_name).await
    }

    async fn get_node_subnet(&self, node_name: &str) -> Result<Option<NodeSubnet>> {
        DatastoreBackend::get_node_subnet(self, node_name).await
    }

    async fn list_peer_subnets(&self, my_node_name: &str) -> Result<Vec<NodeSubnet>> {
        DatastoreBackend::list_peer_subnets(self, my_node_name).await
    }

    async fn delete_node_subnet(&self, node_name: &str) -> Result<()> {
        DatastoreBackend::delete_node_subnet(self, node_name).await
    }

    async fn pod_endpoint_get_by_pod_ip(
        &self,
        pod_ip: std::net::Ipv4Addr,
    ) -> Result<Option<PodEndpointRow>> {
        DatastoreBackend::pod_endpoint_get_by_pod_ip(self, pod_ip).await
    }

    async fn pod_endpoint_list_all(&self) -> Result<Vec<PodEndpointRow>> {
        DatastoreBackend::pod_endpoint_list_all(self).await
    }

    fn subscribe_pod_endpoints(&self) -> broadcast::Receiver<PodEndpointEvent> {
        DatastoreBackend::subscribe_pod_endpoints(self)
    }
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

#[async_trait]
impl<T: DatastoreBackend + ?Sized> NamespaceStore for T {
    async fn create_namespace(&self, name: &str, data: Value) -> Result<Resource> {
        DatastoreBackend::create_namespace(self, name, data).await
    }
    async fn get_namespace(&self, name: &str) -> Result<Option<Resource>> {
        DatastoreBackend::get_namespace(self, name).await
    }
    async fn list_namespaces_page(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList> {
        DatastoreBackend::list_namespaces_page(self, label_selector, field_selector, page).await
    }
    async fn update_namespace(
        &self,
        name: &str,
        data: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        DatastoreBackend::update_namespace(self, name, data, expected_rv).await
    }
    async fn delete_namespace(&self, name: &str) -> Result<()> {
        DatastoreBackend::delete_namespace(self, name).await
    }
    async fn delete_namespace_contents(&self, name: &str) -> Result<()> {
        DatastoreBackend::delete_namespace_contents(self, name).await
    }
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

#[async_trait]
impl<T: DatastoreBackend + ?Sized> NamespaceContentStore for T {
    async fn list_namespace_resources(&self, namespace: &str) -> Result<Vec<Resource>> {
        DatastoreBackend::list_namespace_resources(self, namespace).await
    }

    async fn list_namespace_resources_of_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>> {
        DatastoreBackend::list_namespace_resources_of_kind(self, namespace, kind).await
    }

    async fn list_namespace_resources_excluding_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>> {
        DatastoreBackend::list_namespace_resources_excluding_kind(self, namespace, kind).await
    }

    async fn count_namespace_resources(&self, namespace: &str) -> Result<i64> {
        DatastoreBackend::count_namespace_resources(self, namespace).await
    }
}

/// Replication and snapshot-apply entry points.
#[async_trait]
pub trait ReplicationStore: Send + Sync {
    async fn apply_replicated_command(
        &self,
        command: StorageCommand,
        meta: CommandMeta,
    ) -> Result<()>;
    async fn replace_replicated_resource_state(
        &self,
        entries: Vec<crate::log_apply::LogApplyCommit>,
        current_rv: i64,
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
        metadata: Option<ReplicatedSnapshotMetadata>,
    ) -> Result<()> {
        DatastoreBackend::replace_replicated_resource_state(self, entries, current_rv, metadata)
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
        metadata: Option<ReplicatedSnapshotMetadata>,
    ) -> Result<()> {
        self.as_ref()
            .replace_replicated_resource_state(entries, current_rv, metadata)
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

#[async_trait]
impl<T: DatastoreBackend + ?Sized> MetaStore for T {
    async fn get_klights_meta(&self, key: &str) -> Result<Option<String>> {
        DatastoreBackend::get_klights_meta(self, key).await
    }

    async fn set_klights_meta(&self, key: &str, value: &str) -> Result<()> {
        DatastoreBackend::set_klights_meta(self, key, value).await
    }
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
