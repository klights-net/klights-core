//! OpenRaft state machine over focused cluster-store and node-store ports.
//!
//! Implements openraft 0.9 storage-v2 `RaftStateMachine`. T1.3 unified
//! the apply path so every committed `EntryPayload::Normal` carries a
//! `LogApplyCommit` protobuf (built by the leader's proposer via
//! `backend.build_log_apply_commit_for_outbox`). The state machine decodes
//! the commit and calls `backend.apply_log_apply_commit` — the same code
//! every voter follower and learner runs — so cluster.db is byte-identical
//! across the cluster.
//!
//! Snapshot APIs are wired via `snapshot::SqliteRaftSnapshotBuilder`,
//! which consumes the authoritative cluster-store snapshot session.
//! `install_snapshot` replays the bundled `LogApplyCommit` entries through
//! `apply_log_apply_commit` and `get_current_snapshot` rebuilds the snapshot
//! when openraft asks for an outbound transfer.

use std::io::Cursor;
use std::sync::Arc;

use async_trait::async_trait;
use klights_cluster_core::NodeId;
use klights_cluster_store::{
    BackendLifecycleStore, COMMAND_CODEC_ACTIVATION_VERSION_META_KEY,
    COMMAND_CODEC_V3_ACTIVATION_VALUE, StorageCommandResult,
};
use klights_node_store::{OpaqueRaftBytes, RaftAppliedStateDurability, RaftAppliedStateWrite};
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{
    AnyError, EntryPayload, LogId, Snapshot, SnapshotMeta, StorageError, StorageIOError,
    StoredMembership,
};

use crate::activation::CommandCodecV3Activation;
use crate::materializer::RaftCommitMaterializer;
use crate::types::{RaftMemberNode, TypeConfig};

#[async_trait]
pub trait RaftCommittedApply: Send + Sync {
    async fn apply_committed(
        &self,
        request: klights_cluster_store::CommittedRaftApplyRequest,
    ) -> Result<StorageCommandResult, klights_cluster_store::CommittedApplyError>;
}

#[async_trait]
pub trait RaftSnapshotRestore: Send + Sync {
    async fn restore_snapshot(
        &self,
        snapshot_bytes: Vec<u8>,
    ) -> Result<(), klights_cluster_store::SnapshotPersistenceError>;
}

pub struct RaftStateMachineStorePorts {
    committed_apply: Arc<dyn RaftCommittedApply>,
    snapshot_restore: Arc<dyn RaftSnapshotRestore>,
    lifecycle: Arc<dyn BackendLifecycleStore>,
    metadata: Arc<dyn RaftCommitMaterializer>,
}

impl RaftStateMachineStorePorts {
    pub fn new(
        committed_apply: Arc<dyn RaftCommittedApply>,
        snapshot_restore: Arc<dyn RaftSnapshotRestore>,
        lifecycle: Arc<dyn BackendLifecycleStore>,
        metadata: Arc<dyn RaftCommitMaterializer>,
    ) -> Self {
        Self {
            committed_apply,
            snapshot_restore,
            lifecycle,
            metadata,
        }
    }
}

#[derive(Clone)]
pub struct SqliteRaftStateMachine<B> {
    committed_apply: Arc<dyn RaftCommittedApply>,
    snapshot_restore: Arc<dyn RaftSnapshotRestore>,
    lifecycle: Arc<dyn BackendLifecycleStore>,
    metadata: Arc<dyn RaftCommitMaterializer>,
    applied_state: Arc<dyn RaftAppliedStateDurability>,
    snapshot_builder: B,
    command_codec_v3_activation: Arc<CommandCodecV3Activation>,
}

impl<B> SqliteRaftStateMachine<B> {
    pub fn new_with_command_codec_activation(
        stores: RaftStateMachineStorePorts,
        applied_state: Arc<dyn RaftAppliedStateDurability>,
        snapshot_builder: B,
        command_codec_v3_activation: Arc<CommandCodecV3Activation>,
    ) -> Self {
        Self {
            committed_apply: stores.committed_apply,
            snapshot_restore: stores.snapshot_restore,
            lifecycle: stores.lifecycle,
            metadata: stores.metadata,
            applied_state,
            snapshot_builder,
            command_codec_v3_activation,
        }
    }
}

fn commit_activates_command_codec_v3(commit: &klights_cluster_core::LogApplyCommit) -> bool {
    commit.mutations().iter().any(|mutation| {
        matches!(
            mutation,
            klights_cluster_core::LogApplyMutation::PutKlightsMeta { key, value }
                if key == COMMAND_CODEC_ACTIVATION_VERSION_META_KEY
                    && value == COMMAND_CODEC_V3_ACTIVATION_VALUE
        )
    })
}

fn ioerr_read(e: impl std::fmt::Display) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::read_state_machine(AnyError::error(e.to_string())),
    }
}

fn ioerr_write(e: impl std::fmt::Display) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::write_state_machine(AnyError::error(e.to_string())),
    }
}

fn apply_err(log_id: LogId<NodeId>, e: impl std::fmt::Display) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::apply(log_id, AnyError::error(e.to_string())),
    }
}

impl<B> SqliteRaftStateMachine<B>
where
    B: RaftSnapshotBuilder<TypeConfig> + Clone,
{
    async fn load_applied_state(
        &self,
    ) -> Result<
        (
            Option<LogId<NodeId>>,
            StoredMembership<NodeId, RaftMemberNode>,
        ),
        StorageError<NodeId>,
    > {
        let (last, membership) = self
            .applied_state
            .load_applied_state()
            .await
            .map_err(ioerr_read)?
            .into_parts();
        let last = last
            .map(|bytes| serde_json::from_slice(bytes.as_slice()))
            .transpose()
            .map_err(ioerr_read)?
            .flatten();
        let membership = membership
            .map(|bytes| serde_json::from_slice(bytes.as_slice()))
            .transpose()
            .map_err(ioerr_read)?
            .unwrap_or_default();
        Ok((last, membership))
    }

    async fn read_last_applied(&self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(self.load_applied_state().await?.0)
    }

    async fn write_applied_state(
        &self,
        id: Option<LogId<NodeId>>,
        membership: Option<&StoredMembership<NodeId, RaftMemberNode>>,
    ) -> Result<(), StorageError<NodeId>> {
        let last = id
            .map(|value| serde_json::to_vec(&Some(value)))
            .transpose()
            .map_err(ioerr_write)?
            .map(OpaqueRaftBytes::new);
        let membership = membership
            .map(serde_json::to_vec)
            .transpose()
            .map_err(ioerr_write)?
            .map(OpaqueRaftBytes::new);
        self.applied_state
            .store_applied_state(RaftAppliedStateWrite::new(last, membership))
            .await
            .map_err(ioerr_write)
    }

    async fn build_current_snapshot(
        &mut self,
    ) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let mut builder = self.get_snapshot_builder().await;
        builder.build_snapshot().await
    }
}

impl<B> RaftStateMachine<TypeConfig> for SqliteRaftStateMachine<B>
where
    B: RaftSnapshotBuilder<TypeConfig> + Clone,
{
    type SnapshotBuilder = B;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<NodeId>>,
            StoredMembership<NodeId, RaftMemberNode>,
        ),
        StorageError<NodeId>,
    > {
        self.load_applied_state().await
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<StorageCommandResult>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let _snapshot_mutation = self
            .lifecycle
            .acquire_snapshot_mutation_fence()
            .await
            .map_err(ioerr_write)?;
        let mut out = Vec::new();
        // P3-8: defensive fence against stale-leader writes. openraft
        // already refuses to dispatch an entry whose term is below the
        // current term at the consensus layer, but the state machine
        // double-checks against `last_applied` so a buggy lower layer
        // (or a manual `Raft::initialize` from an operator) cannot
        // silently apply commits at an older term and rewrite history.
        let mut last_applied_term = self.read_last_applied().await?.map(|id| id.leader_id.term);
        for entry in entries {
            let log_id = entry.log_id;
            if let Some(prev_term) = last_applied_term
                && log_id.leader_id.term < prev_term
            {
                return Err(apply_err(
                    log_id,
                    format!(
                        "stale-term apply rejected: entry term {} < last_applied term {}",
                        log_id.leader_id.term, prev_term
                    ),
                ));
            }
            last_applied_term = Some(log_id.leader_id.term);
            let membership_entry = matches!(&entry.payload, EntryPayload::Membership(_));
            match entry.payload {
                EntryPayload::Blank => {
                    out.push(StorageCommandResult::default());
                }
                EntryPayload::Membership(m) => {
                    let stored = StoredMembership::new(Some(log_id), m);
                    self.write_applied_state(Some(log_id), Some(&stored))
                        .await?;
                    out.push(StorageCommandResult::default());
                }
                EntryPayload::Normal(payload) => {
                    // T1.3: raft entry payloads carry a `LogApplyCommit`
                    // protobuf (built by the leader's proposer via
                    // `backend.build_log_apply_commit_for_outbox`). Every
                    // node — leader, voter follower, learner — applies
                    // through the same `apply_log_apply_commit` →
                    // `apply_commit_in_tx` path so cluster.db state is
                    // byte-identical across the cluster.
                    let commit = crate::log_apply_wire::decode_commit_protobuf(payload.as_slice())
                        .map_err(|e| apply_err(log_id, e))?;
                    let activates_command_codec_v3 = commit_activates_command_codec_v3(&commit);
                    let result = self
                        .committed_apply
                        .apply_committed(klights_cluster_store::CommittedRaftApplyRequest::new(
                            commit,
                        ))
                        .await
                        .map_err(|e| apply_err(log_id, e))?;
                    if activates_command_codec_v3 {
                        let persisted = self
                            .metadata
                            .read_raft_metadata(COMMAND_CODEC_ACTIVATION_VERSION_META_KEY)
                            .await
                            .map_err(|error| apply_err(log_id, error))?;
                        if persisted.as_deref() != Some(COMMAND_CODEC_V3_ACTIVATION_VALUE) {
                            return Err(apply_err(
                                log_id,
                                "exact-v3 activation mutation did not persist its marker",
                            ));
                        }
                        self.command_codec_v3_activation
                            .mark_command_codec_v3_activated();
                    }
                    out.push(result);
                }
            }
            if !membership_entry {
                self.write_applied_state(Some(log_id), None).await?;
            }
        }
        Ok(out)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.snapshot_builder.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, RaftMemberNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let bytes = snapshot.into_inner();
        let _snapshot_mutation = self
            .lifecycle
            .acquire_snapshot_mutation_fence()
            .await
            .map_err(ioerr_write)?;
        // Raft snapshot install semantics: the destination state machine
        // must become byte/key-identical to the leader snapshot at the
        // snapshot index. Applying snapshot commits over the existing local
        // store (merge) is a correctness bug — rows the leader has deleted
        // but a lagging follower/learner still holds are never removed, so
        // the member silently diverges (observed after lossy Sonobuoy:
        // followers/learner carry more rows than the leader). Use the
        // authoritative replace primitive, which deletes all replicated
        // tables first, then replays the snapshot commits and restores the
        // leader RV. (finding.md H1 / P0 cluster.db divergence.)
        self.snapshot_restore
            .restore_snapshot(bytes)
            .await
            .map_err(|e| StorageError::IO {
                source: StorageIOError::write_state_machine(AnyError::error(e.to_string())),
            })?;
        match self
            .metadata
            .read_raft_metadata(COMMAND_CODEC_ACTIVATION_VERSION_META_KEY)
            .await
            .map_err(ioerr_read)?
            .as_deref()
        {
            Some(COMMAND_CODEC_V3_ACTIVATION_VALUE) => self
                .command_codec_v3_activation
                .mark_command_codec_v3_activated(),
            None => self
                .command_codec_v3_activation
                .clear_command_codec_v3_activation(),
            Some(other) => {
                return Err(ioerr_read(format!(
                    "snapshot restored unsupported command codec activation version {other:?}"
                )));
            }
        }
        let stored =
            StoredMembership::new(meta.last_log_id, meta.last_membership.membership().clone());
        self.write_applied_state(meta.last_log_id, Some(&stored))
            .await?;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        Ok(Some(self.build_current_snapshot().await?))
    }
}
