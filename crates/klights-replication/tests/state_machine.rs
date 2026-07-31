use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use klights_cluster_core::{NodeId, StorageCommand};
use klights_cluster_store::{
    BackendLifecycleStore, CommittedApplyError, CommittedRaftApplyRequest, SnapshotExclusiveFence,
    SnapshotMutationFence, SnapshotPersistenceError, StorageCommandResult,
};
use klights_node_store::{
    EncodedRaftAppliedState, RaftAppliedStateDurability, RaftAppliedStateWrite,
    RaftDurabilityFuture,
};
use klights_replication::activation::CommandCodecV3Activation;
use klights_replication::materializer::RaftCommitMaterializer;
use klights_replication::state_machine::{
    RaftCommittedApply, RaftSnapshotRestore, RaftStateMachineStorePorts, SqliteRaftStateMachine,
};
use klights_replication::types::{RaftMemberNode, TypeConfig};
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{Entry, EntryPayload, LeaderId, LogId, Membership, Snapshot, StorageError};

struct MemoryAppliedState {
    state: Mutex<EncodedRaftAppliedState>,
}

impl RaftAppliedStateDurability for MemoryAppliedState {
    fn load_applied_state(&self) -> RaftDurabilityFuture<'_, EncodedRaftAppliedState> {
        Box::pin(async move { Ok(self.state.lock().unwrap().clone()) })
    }

    fn store_applied_state(&self, update: RaftAppliedStateWrite) -> RaftDurabilityFuture<'_, ()> {
        Box::pin(async move {
            let (last, membership) = update.into_parts();
            let mut state = self.state.lock().unwrap();
            let (old_last, old_membership) = state.clone().into_parts();
            *state = EncodedRaftAppliedState::new(last.or(old_last), membership.or(old_membership));
            Ok(())
        })
    }
}

struct NoopLifecycle;

#[async_trait]
impl BackendLifecycleStore for NoopLifecycle {
    async fn acquire_snapshot_exclusive_fence(
        &self,
    ) -> anyhow::Result<Option<SnapshotExclusiveFence>> {
        Ok(None)
    }

    async fn acquire_snapshot_mutation_fence(
        &self,
    ) -> anyhow::Result<Option<SnapshotMutationFence>> {
        Ok(None)
    }

    fn close(&self) {}
}

struct NoopPorts;

#[async_trait]
impl RaftCommittedApply for NoopPorts {
    async fn apply_committed(
        &self,
        _request: CommittedRaftApplyRequest,
    ) -> Result<StorageCommandResult, CommittedApplyError> {
        unreachable!("blank and membership owner tests do not apply normal commands")
    }
}

#[async_trait]
impl RaftSnapshotRestore for NoopPorts {
    async fn restore_snapshot(
        &self,
        _snapshot_bytes: Vec<u8>,
    ) -> Result<(), SnapshotPersistenceError> {
        unreachable!("owner-local applied-state tests do not restore snapshots")
    }
}

#[async_trait]
impl RaftCommitMaterializer for NoopPorts {
    async fn read_raft_metadata(
        &self,
        _key: &str,
    ) -> Result<Option<String>, klights_cluster_core::StorageMutationError> {
        Ok(None)
    }

    async fn build_command(
        &self,
        _command: StorageCommand,
        _operation: &str,
        _authoring_node: &str,
    ) -> Result<klights_cluster_core::LogApplyCommit, klights_cluster_core::StorageMutationError>
    {
        unreachable!("owner-local state-machine tests do not materialize commands")
    }

    async fn build_outbox(
        &self,
        _idempotency_key: &str,
        _operation: &str,
        _command: StorageCommand,
        _authoring_node: &str,
        _watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> Result<klights_cluster_core::BuildOutboxOutcome, klights_cluster_core::OutboxApplyError>
    {
        unreachable!("owner-local state-machine tests do not materialize outbox rows")
    }
}

#[derive(Clone)]
struct UnusedSnapshotBuilder;

impl RaftSnapshotBuilder<TypeConfig> for UnusedSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        unreachable!("owner-local applied-state tests do not build snapshots")
    }
}

type TestStateMachine = SqliteRaftStateMachine<UnusedSnapshotBuilder>;

async fn fresh_sm() -> TestStateMachine {
    let ports = Arc::new(NoopPorts);
    let activation = Arc::new(
        CommandCodecV3Activation::load(ports.as_ref())
            .await
            .unwrap(),
    );
    SqliteRaftStateMachine::new_with_command_codec_activation(
        RaftStateMachineStorePorts::new(
            ports.clone(),
            ports.clone(),
            Arc::new(NoopLifecycle),
            ports,
        ),
        Arc::new(MemoryAppliedState {
            state: Mutex::new(EncodedRaftAppliedState::new(None, None)),
        }),
        UnusedSnapshotBuilder,
        activation,
    )
}

#[tokio::test]
async fn applied_state_starts_empty() {
    let mut state_machine = fresh_sm().await;
    let (last, membership) = state_machine.applied_state().await.unwrap();
    assert!(last.is_none());
    assert!(membership.log_id().is_none());
}

#[tokio::test]
async fn apply_blank_entry_advances_last_applied() {
    let mut state_machine = fresh_sm().await;
    let entry = Entry::<TypeConfig> {
        log_id: LogId::new(LeaderId::new(1, 10), 1),
        payload: EntryPayload::Blank,
    };
    let out = state_machine.apply(vec![entry]).await.unwrap();
    assert_eq!(out.len(), 1);
    assert!(!out[0].public_resource_changed);
    let (last, _) = state_machine.applied_state().await.unwrap();
    assert_eq!(last.unwrap().index, 1);
}

#[tokio::test]
async fn apply_rejects_entry_with_lower_term_than_last_applied() {
    let mut state_machine = fresh_sm().await;
    state_machine
        .apply(vec![Entry::<TypeConfig> {
            log_id: LogId::new(LeaderId::new(5, 10), 1),
            payload: EntryPayload::Blank,
        }])
        .await
        .unwrap();
    let error = state_machine
        .apply(vec![Entry::<TypeConfig> {
            log_id: LogId::new(LeaderId::new(3, 10), 2),
            payload: EntryPayload::Blank,
        }])
        .await
        .expect_err("stale-term entry must be rejected");
    assert!(error.to_string().contains("stale-term apply rejected"));
    let (last, _) = state_machine.applied_state().await.unwrap();
    assert_eq!(last.unwrap().leader_id.term, 5);
}

#[tokio::test]
async fn apply_accepts_same_or_higher_term_entries() {
    let mut state_machine = fresh_sm().await;
    for (term, node, index) in [(2, 10, 1), (2, 10, 2), (7, 20, 3)] {
        state_machine
            .apply(vec![Entry::<TypeConfig> {
                log_id: LogId::new(LeaderId::new(term, node), index),
                payload: EntryPayload::Blank,
            }])
            .await
            .unwrap();
    }
    let (last, _) = state_machine.applied_state().await.unwrap();
    assert_eq!(last.unwrap().leader_id.term, 7);
}

#[tokio::test]
async fn apply_membership_entry_stores_membership() {
    let mut state_machine = fresh_sm().await;
    let voters: BTreeSet<NodeId> = [10, 20, 30].into_iter().collect();
    let membership: Membership<NodeId, RaftMemberNode> = Membership::new(vec![voters], None);
    let out = state_machine
        .apply(vec![Entry::<TypeConfig> {
            log_id: LogId::new(LeaderId::new(2, 10), 7),
            payload: EntryPayload::Membership(membership),
        }])
        .await
        .unwrap();
    assert!(!out[0].public_resource_changed);
    let (last, stored) = state_machine.applied_state().await.unwrap();
    assert_eq!(last.unwrap().index, 7);
    assert_eq!(stored.membership().voter_ids().count(), 3);
    assert_eq!(stored.log_id().unwrap().index, 7);
}
