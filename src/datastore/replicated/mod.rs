//! Role-aware ReplicatedDatastore wrapper (DSB-HA-02).
//!
//! In production, every `DatastoreHandle` is a `ReplicatedDatastore`.
//! The concrete SQLite `Datastore` only implements `DatastoreApplier`;
//! `DatastoreBackend` is the public runtime contract, satisfied solely
//! by `ReplicatedDatastore`.
//!
//! ## Architecture
//!
//! ```text
//! API/controller write
//!         │
//!         ▼
//! ReplicatedDatastore (implements DatastoreBackend)
//!         │
//!         ├── SingleNode/Raft: require_raft_proposer → propose through raft
//!         │
//!         ▼
//! Datastore (inherent methods — actual SQL execution)
//! ```

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use std::sync::{Arc, OnceLock};

use crate::datastore::backend::DatastoreBackend;
use crate::datastore::command::{COMMAND_CODEC_VERSION, CommandId, CommandMeta, StorageCommand};
use crate::datastore::types::{ReplicatedCreateOptions, ResourcePatchRequest};
use crate::networking::VtepMac;

mod backend_impl;

// ---------------------------------------------------------------------------
// RaftProposer — late-bound binding from ReplicatedDatastore to RaftNode.
//
// P3-11c4: in Raft mode, the wrapper's mutation methods route every
// StorageCommand through `RaftProposer::propose_command`. The proposer
// encodes the command as an `OutboxPayload` and submits it to openraft's
// `client_write`. openraft replicates it to peers and then drives the
// state machine's apply, which calls back into `apply_outbox_transactionally_direct`
// on the same wrapper (bypassing the propose route) to actually mutate the
// inner backend and emit the `log_apply` stream the BackupApplier consumes.
//
// The trait is intentionally narrow: a single method that takes a
// fully-formed StorageCommand and returns once openraft has committed +
// applied it. Concrete impl lives in `crate::datastore::raft::node`.
// ---------------------------------------------------------------------------

/// Late-bound handle to the cluster's Raft consensus engine. The wrapper's
/// mutation methods use this in `ReplicationMode::Raft` to route writes
/// through `Raft::client_write` instead of touching the inner backend
/// directly.
#[async_trait]
pub trait RaftProposer: Send + Sync {
    /// Submit a `StorageCommand` for replication and apply. Returns once
    /// openraft has committed the entry and the state machine has applied
    /// it to the local backend. Non-leader voters refuse before local
    /// commit materialization; callers must route writes to the leader.
    async fn propose_command(&self, command: StorageCommand) -> Result<()>;

    /// T6 step 4c: propose an outbox-flavored write through raft.
    /// Same end result as `propose_command` (build LogApplyCommit →
    /// raft commit → state machine apply on every member) but
    /// preserves the caller's `idempotency_key` and `operation` so
    /// the applied_outbox dedup row is keyed correctly. Returns
    /// `OutboxApplyResult` for the outbox dispatcher.
    async fn propose_outbox_command(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
    ) -> std::result::Result<
        crate::kubelet::outbox::OutboxApplyResult,
        crate::kubelet::outbox::OutboxApplyError,
    >;
}

// ---------------------------------------------------------------------------
// ReplicationMode
// ---------------------------------------------------------------------------

/// Cluster replication mode for the `ReplicatedDatastore` wrapper.
///
/// T7.2/T7.7: `LeaderFollower` removed. Every cluster.db write goes
/// through raft — `Raft` for leader-class boots, `SingleNode` (N=1
/// raft) for workers that can promote when CPs join.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplicationMode {
    SingleNode { node_name: String },
    Raft { node_name: String },
}

impl ReplicationMode {
    pub fn node_name(&self) -> &str {
        match self {
            ReplicationMode::SingleNode { node_name } => node_name,
            ReplicationMode::Raft { node_name } => node_name,
        }
    }
}

// ---------------------------------------------------------------------------
// WriteRejection
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, thiserror::Error)]
pub enum WriteRejection {
    #[error("this node is a follower and does not accept writes; redirect to leader")]
    FollowerWrite,
    #[error("write rejected: {0}")]
    Other(String),
}

impl WriteRejection {
    pub fn status_code(&self) -> u16 {
        503
    }
}

// ---------------------------------------------------------------------------
// DatastoreApplier — deterministic local apply trait
// ---------------------------------------------------------------------------

/// Trait for deterministic local apply of storage commands.
///
/// Local storage engines implement this trait.  The replication layer
/// calls `apply_command` after role-based logic determines the write
/// should proceed.
#[async_trait]
pub trait DatastoreApplier: Send + Sync {
    async fn apply_command(&self, cmd: StorageCommand, meta: CommandMeta) -> Result<()>;
}

// `ForwardedWrite` and `CommandForwarder` removed in T6 — the legacy
// generic storage-forward shim. Workers now route writes through outbox
// -> ApplyOutbox via the LeaderApiClient surface.

pub type ReplicationObserverFn = Arc<dyn Fn(StorageCommand, CommandMeta) + Send + Sync>;

#[derive(Clone, Default)]
pub struct ReplicationObserver {
    inner: Arc<tokio::sync::RwLock<Option<ReplicationObserverFn>>>,
}

impl ReplicationObserver {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn set(&self, observer: ReplicationObserverFn) {
        *self.inner.write().await = Some(observer);
    }

    async fn notify(&self, command: StorageCommand, meta: CommandMeta) {
        let observer = self.inner.read().await.clone();
        if let Some(observer) = observer {
            observer(command, meta);
        }
    }
}

// ---------------------------------------------------------------------------
// ReplicatedDatastore
// ---------------------------------------------------------------------------

pub struct ReplicatedDatastore {
    inner: Arc<dyn DatastoreBackend>,
    mode: ReplicationMode,
    observer: Option<ReplicationObserver>,
    raft_proposer: OnceLock<Arc<dyn RaftProposer>>,
}

/// T7.2: Typed error when a ReplicatedDatastore write is attempted
/// without an attached RaftProposer. Every cluster.db write must go
/// through raft — even single-node deployments run N=1 raft and can
/// promote when additional control planes join.
#[derive(Debug, Clone, thiserror::Error)]
pub enum RaftWriteError {
    #[error(
        "raft proposer not attached; cluster.db write rejected — leader must bind proposer before accepting writes"
    )]
    MissingProposer,
}

impl ReplicatedDatastore {
    pub fn new(inner: Arc<dyn DatastoreBackend>, mode: ReplicationMode) -> Self {
        Self::with_observer(inner, mode, None)
    }

    pub fn with_observer(
        inner: Arc<dyn DatastoreBackend>,
        mode: ReplicationMode,
        observer: Option<ReplicationObserver>,
    ) -> Self {
        Self {
            inner,
            mode,
            observer,
            raft_proposer: OnceLock::new(),
        }
    }

    /// Late-bind the RaftProposer that the Raft-mode mutation arms route
    /// writes through. Idempotent: a second call with a different proposer
    /// is a silent no-op (the first wins). Must be invoked once after the
    /// RaftNode is constructed in the bootstrap path.
    pub fn set_raft_proposer(&self, proposer: Arc<dyn RaftProposer>) {
        let _ = self.raft_proposer.set(proposer);
    }

    /// T7.2: Require a raft proposer for every cluster.db write. Every
    /// replication mode (including SingleNode, which is N=1 raft) routes
    /// writes through the proposer. A single node can promote to a
    /// multi-voter cluster when additional control planes join.
    pub(crate) fn require_raft_proposer(&self) -> Result<Arc<dyn RaftProposer>> {
        self.raft_proposer
            .get()
            .cloned()
            .ok_or_else(|| anyhow!(RaftWriteError::MissingProposer))
    }

    /// Encode `command` as an `OutboxPayload` protobuf and submit through
    /// the installed RaftProposer. Returns when openraft has committed the
    /// entry and the state machine has applied it.
    pub(crate) async fn propose_command_via_raft(
        &self,
        proposer: &Arc<dyn RaftProposer>,
        command: StorageCommand,
    ) -> Result<()> {
        proposer.propose_command(command).await
    }

    fn authoring_node(&self) -> String {
        self.mode.node_name().to_string()
    }

    fn meta_for_rv(&self, resource_version: i64, uid: Option<String>) -> CommandMeta {
        CommandMeta {
            command_id: CommandId::new(),
            codec_version: COMMAND_CODEC_VERSION,
            resource_version,
            uid,
            timestamp_ms: current_epoch_millis(),
            authoring_node: self.authoring_node(),
        }
    }

    // T3: `watch_event_mutations_for_rv`, `resource_commit_from_watch_history`,
    // and `log_apply_commit_for_applied_command` removed. These built
    // `LogApplyCommit` payloads for the dead `log_apply_entries` table.
    // Raft `AppendEntries` through `apply_log_apply_commit` (T1.3) is
    // the sole replication path.
}
fn current_epoch_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

pub async fn apply_command_to_backend<B>(
    backend: &B,
    command: StorageCommand,
    meta: CommandMeta,
) -> Result<()>
where
    B: DatastoreBackend + ?Sized,
{
    align_resource_version_before_replicated_apply(backend, meta.resource_version).await?;
    match command {
        StorageCommand::CreateResource {
            api_version,
            kind,
            namespace,
            name,
            data,
        } => {
            backend
                .apply_replicated_create_resource(
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                    data,
                    ReplicatedCreateOptions::new(meta.resource_version, meta.uid.clone()),
                )
                .await?;
        }
        StorageCommand::UpdateResource {
            api_version,
            kind,
            namespace,
            name,
            mut data,
            expected_rv,
            preconditions,
        } => {
            let current = backend
                .get_resource(&api_version, &kind, namespace.as_deref(), &name)
                .await?;
            if let Some(current) = current.as_ref()
                && current.resource_version >= meta.resource_version
                && meta
                    .uid
                    .as_ref()
                    .is_none_or(|expected_uid| current.uid == *expected_uid)
            {
                crate::resource_semantics::preserve_status_subresource_on_main_update(
                    &api_version,
                    &kind,
                    &current.data,
                    &mut data,
                );
            }
            let mut preconditions = preconditions;
            if preconditions.resource_version.is_some() {
                preconditions.resource_version = current
                    .as_ref()
                    .map(|resource| resource.resource_version)
                    .or(Some(expected_rv));
            }
            backend
                .update_resource_with_preconditions(
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                    data,
                    preconditions,
                )
                .await?;
        }
        StorageCommand::DeleteResource {
            api_version,
            kind,
            namespace,
            name,
            preconditions,
        } => {
            backend
                .delete_resource_with_preconditions(
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                    preconditions,
                )
                .await?;
        }
        StorageCommand::PatchResource {
            api_version,
            kind,
            namespace,
            name,
            patch_kind,
            patch,
            preconditions,
        } => {
            let current = backend
                .get_resource(&api_version, &kind, namespace.as_deref(), &name)
                .await?;
            let mut preconditions = preconditions;
            if preconditions.resource_version.is_some() {
                preconditions.resource_version =
                    current.as_ref().map(|resource| resource.resource_version);
            }
            backend
                .patch_resource_latest_with_preconditions(
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                    ResourcePatchRequest::new(patch_kind, patch, preconditions),
                )
                .await?;
        }
        StorageCommand::UpdateStatus {
            api_version,
            kind,
            namespace,
            name,
            status,
            expected_rv,
            preconditions,
            observed_status_stamp,
        } => {
            let current = backend
                .get_resource(&api_version, &kind, namespace.as_deref(), &name)
                .await?;
            let mut status = status;
            // Scheduler-owned Pod conditions (e.g. DisruptionTarget set by
            // preemption) are never rebuilt by any status writer, so they must
            // be preserved on every v1/Pod UpdateStatus apply — including the
            // leader-direct path that carries no outbox stamp. Without this, a
            // kubelet runtime-reconcile snapshot computed before a preemption
            // write (and proposed as UpdateStatus with stamp=None) verbatim-
            // replaces the live status and permanently drops DisruptionTarget,
            // which is the live SchedulerPreemption conformance failure.
            if api_version == "v1"
                && kind == "Pod"
                && let Some(current) = current.as_ref()
            {
                let owner = if observed_status_stamp.is_some() {
                    crate::pod_status_merge::PodStatusOwner::KubeletRuntime
                } else {
                    crate::pod_status_merge::PodStatusOwner::ReplicatedApply
                };
                crate::pod_status_merge::merge_pod_status_for_update(
                    &api_version,
                    &kind,
                    current.data.as_ref(),
                    &mut status,
                    owner,
                );
            }
            let mut preconditions = preconditions;
            if preconditions.resource_version.is_some() {
                preconditions.resource_version = current
                    .as_ref()
                    .map(|resource| resource.resource_version)
                    .or(expected_rv);
            }
            backend
                .update_status_only_with_preconditions(
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                    status,
                    preconditions,
                )
                .await?;
        }
        StorageCommand::ApplyResourceBatch { operations } => {
            backend.apply_resource_batch(operations).await?;
        }
        StorageCommand::CreateNamespace { name, data } => {
            if let Some(existing) = backend.get_namespace(&name).await? {
                backend
                    .update_namespace(&name, data, existing.resource_version)
                    .await?;
            } else {
                backend.create_namespace(&name, data).await?;
            }
        }
        StorageCommand::UpdateNamespace {
            name,
            data,
            expected_rv,
        } => {
            let expected_rv = backend
                .get_namespace(&name)
                .await?
                .map(|resource| resource.resource_version)
                .unwrap_or(expected_rv);
            backend.update_namespace(&name, data, expected_rv).await?;
        }
        StorageCommand::DeleteNamespace { name } => {
            backend.delete_namespace(&name).await?;
        }
        StorageCommand::DeleteNamespaceContents { name } => {
            backend.delete_namespace_contents(&name).await?;
        }
        StorageCommand::AllocateNodeSubnet {
            node_name,
            subnet,
            node_ip,
        } => {
            backend
                .allocate_node_subnet(&node_name, &subnet, &node_ip)
                .await?;
        }
        StorageCommand::UpdateNodeVtepMac {
            node_name,
            vtep_mac,
        } => {
            let mac = VtepMac::parse(&vtep_mac)
                .map_err(|err| anyhow!("invalid VTEP MAC '{}': {}", vtep_mac, err))?;
            backend.update_node_vtep_mac(&node_name, &mac).await?;
        }
        StorageCommand::UpdateNodePeerAttributes {
            node_name,
            mode,
            hostport_range,
        } => {
            let peer_mode = crate::controllers::annotations::parse_node_peer_mode(Some(&mode))
                .unwrap_or(crate::controllers::annotations::NodePeerMode::Root);
            let hpr = hostport_range
                .as_deref()
                .and_then(|value| crate::networking::types::HostPortRange::parse(value).ok());
            backend
                .update_node_peer_attributes(&node_name, peer_mode, hpr)
                .await?;
        }
        StorageCommand::UpdateNodeDataplane {
            node_name,
            mode,
            encryption,
            public_key,
            endpoint,
            port,
        } => {
            let metadata = crate::networking::wireguard::DataplanePeerMetadata::try_new(
                node_name,
                crate::networking::wireguard::DataplaneMode::parse(&mode)?,
                crate::networking::wireguard::DataplaneEncryption::parse(Some(&encryption))?,
                public_key,
                Some(endpoint),
                port,
            )?;
            backend.update_node_dataplane(metadata).await?;
        }
        StorageCommand::DeleteNodeSubnet { node_name } => {
            backend.delete_node_subnet(&node_name).await?;
        }
        StorageCommand::PodSlotTryAdmit {
            namespace,
            pod_name,
            pod_uid,
            node_name,
        } => {
            backend
                .pod_slot_try_admit(&namespace, &pod_name, &pod_uid, &node_name)
                .await?;
        }
        StorageCommand::PodSlotMarkTerminating {
            namespace,
            pod_name,
            pod_uid,
            node_name,
        } => {
            backend
                .pod_slot_mark_terminating(&namespace, &pod_name, &pod_uid, &node_name)
                .await?;
        }
        StorageCommand::PodSlotClearIfUid {
            namespace,
            pod_name,
            pod_uid,
            node_name,
        } => {
            backend
                .pod_slot_clear_if_uid(&namespace, &pod_name, &pod_uid, &node_name)
                .await?;
        }
        StorageCommand::MovePodToCleanupIntent {
            node_name,
            namespace,
            pod_name,
            pod_uid,
            reason,
        } => {
            backend
                .move_pod_to_cleanup_intent(&node_name, &namespace, &pod_name, &pod_uid, &reason)
                .await?;
        }
        StorageCommand::DeletePodCleanupIntent {
            node_name,
            namespace,
            pod_name,
            pod_uid,
            reason,
        } => {
            backend
                .delete_pod_cleanup_intent(&node_name, &namespace, &pod_name, &pod_uid, &reason)
                .await?;
        }
        StorageCommand::DeletePodCleanupIntentsForNode { node_name } => {
            backend
                .delete_pod_cleanup_intents_for_node(&node_name)
                .await?;
        }
        StorageCommand::WatchEventAppend { event_bytes, rv } => {
            let _ = (event_bytes, rv);
        }
        StorageCommand::GcWatchEvents {
            max_rows,
            batch_cap,
        } => {
            backend.gc_watch_events(max_rows, batch_cap).await?;
        }
        StorageCommand::AdvanceResourceVersion { min_rv, .. } => {
            backend.advance_resource_version_after(min_rv).await?;
        }
        StorageCommand::EnsureClusterMetadata { cluster_id } => {
            let existing = backend
                .get_klights_meta(crate::bootstrap::cluster_meta::KEY_CLUSTER_ID)
                .await?;
            if existing.is_none() {
                backend
                    .set_klights_meta(crate::bootstrap::cluster_meta::KEY_CLUSTER_ID, &cluster_id)
                    .await?;
                backend
                    .set_klights_meta(crate::bootstrap::cluster_meta::KEY_LEADER_EPOCH, "0")
                    .await?;
            }
            // If cluster_id already exists, this is idempotent: do not
            // overwrite. Followers replay this command but the seed
            // already wrote it, so the insert is a no-op.
        }
        StorageCommand::SetKlightsMeta { key, value } => {
            backend.set_klights_meta(&key, &value).await?;
        }
    }

    let current_rv = backend.get_current_resource_version().await.unwrap_or(0);
    if current_rv < meta.resource_version {
        backend
            .advance_resource_version_after(meta.resource_version.saturating_sub(1))
            .await?;
    }
    Ok(())
}

async fn align_resource_version_before_replicated_apply<B>(
    backend: &B,
    target_rv: i64,
) -> Result<()>
where
    B: DatastoreBackend + ?Sized,
{
    if target_rv <= 0 {
        return Ok(());
    }
    let current_rv = backend.get_current_resource_version().await.unwrap_or(0);
    let desired_before = target_rv.saturating_sub(1);
    if current_rv < desired_before {
        backend
            .advance_resource_version_after(desired_before.saturating_sub(1))
            .await?;
    }
    Ok(())
}

#[async_trait]
impl DatastoreApplier for ReplicatedDatastore {
    async fn apply_command(&self, cmd: StorageCommand, meta: CommandMeta) -> Result<()> {
        apply_command_to_backend(self.inner.as_ref(), cmd, meta).await
    }
}

// Delegate every DatastoreBackend method to self.inner.
// Public reads and writes check replication mode first. Replication apply
// bypasses public admission and writes leader data to the local backend.
#[cfg(test)]
#[path = "tests.rs"]
mod tests;
