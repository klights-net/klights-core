use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use klights_cluster_core::NodeId;
use klights_node_store::{
    EncodedRaftLogEntry, EncodedRaftLogState, EncodedRaftStorageBoundary, OpaqueRaftBytes,
    RaftDurabilityFuture, RaftLogBatch, RaftLogCoordinate, RaftLogDurability, RaftLogPersistence,
    RaftLogRange, RaftPurgeRequest,
};
use klights_replication::log_storage::SqliteRaftLogStorage;
use klights_replication::types::{StorageCommandPayload, TypeConfig};
use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};
use openraft::storage::RaftLogStorage;
use openraft::{Entry, EntryPayload, LeaderId, LogId, RaftLogReader, Vote};

#[derive(Default)]
struct MemoryRaftLog {
    state: Mutex<MemoryRaftLogState>,
}

#[derive(Default)]
struct MemoryRaftLogState {
    entries: BTreeMap<u64, EncodedRaftLogEntry>,
    last_purged: Option<OpaqueRaftBytes>,
    vote: Option<OpaqueRaftBytes>,
    committed: Option<OpaqueRaftBytes>,
}

impl RaftLogPersistence for MemoryRaftLog {
    fn read_log_range(
        &self,
        range: RaftLogRange,
    ) -> RaftDurabilityFuture<'_, Vec<EncodedRaftLogEntry>> {
        Box::pin(async move {
            let state = self.state.lock().unwrap();
            Ok(state
                .entries
                .range(range.start_inclusive()..range.end_exclusive().unwrap_or(u64::MAX))
                .map(|(_, entry)| entry.clone())
                .collect())
        })
    }

    fn load_log_state(&self) -> RaftDurabilityFuture<'_, EncodedRaftLogState> {
        Box::pin(async move {
            let state = self.state.lock().unwrap();
            Ok(EncodedRaftLogState::new(
                state
                    .entries
                    .last_key_value()
                    .map(|(_, entry)| entry.coordinate()),
                state.last_purged.clone(),
            ))
        })
    }

    fn append_log_entries(&self, entries: RaftLogBatch) -> RaftDurabilityFuture<'_, ()> {
        Box::pin(async move {
            let mut state = self.state.lock().unwrap();
            for entry in entries.into_vec() {
                state.entries.insert(entry.coordinate().index(), entry);
            }
            Ok(())
        })
    }

    fn truncate_log_from(&self, from_inclusive: u64) -> RaftDurabilityFuture<'_, ()> {
        Box::pin(async move {
            self.state
                .lock()
                .unwrap()
                .entries
                .retain(|index, _| *index < from_inclusive);
            Ok(())
        })
    }

    fn purge_log_through(&self, request: RaftPurgeRequest) -> RaftDurabilityFuture<'_, ()> {
        Box::pin(async move {
            let (through, encoded) = request.into_parts();
            let mut state = self.state.lock().unwrap();
            state.entries.retain(|index, _| *index > through.index());
            state.last_purged = Some(encoded);
            Ok(())
        })
    }

    fn load_vote(&self) -> RaftDurabilityFuture<'_, Option<OpaqueRaftBytes>> {
        Box::pin(async move { Ok(self.state.lock().unwrap().vote.clone()) })
    }

    fn store_vote(&self, encoded_vote: OpaqueRaftBytes) -> RaftDurabilityFuture<'_, ()> {
        Box::pin(async move {
            self.state.lock().unwrap().vote = Some(encoded_vote);
            Ok(())
        })
    }

    fn load_committed(&self) -> RaftDurabilityFuture<'_, Option<OpaqueRaftBytes>> {
        Box::pin(async move { Ok(self.state.lock().unwrap().committed.clone()) })
    }

    fn store_committed(&self, encoded_committed: OpaqueRaftBytes) -> RaftDurabilityFuture<'_, ()> {
        Box::pin(async move {
            self.state.lock().unwrap().committed = Some(encoded_committed);
            Ok(())
        })
    }

    fn load_or_create_storage_incarnation(&self) -> RaftDurabilityFuture<'_, String> {
        Box::pin(async { Ok("owner-local-memory-log".to_string()) })
    }

    fn load_storage_log_attestation(&self) -> RaftDurabilityFuture<'_, Option<RaftLogCoordinate>> {
        Box::pin(async move {
            Ok(self
                .state
                .lock()
                .unwrap()
                .entries
                .last_key_value()
                .map(|(_, entry)| entry.coordinate()))
        })
    }

    fn load_storage_boundary_state(&self) -> RaftDurabilityFuture<'_, EncodedRaftStorageBoundary> {
        Box::pin(async move {
            let state = self.state.lock().unwrap();
            Ok(EncodedRaftStorageBoundary::new(
                state
                    .entries
                    .last_key_value()
                    .map(|(_, entry)| entry.coordinate()),
                state.last_purged.clone(),
                None,
            ))
        })
    }

    fn reset_orphaned_learner_log(&self) -> RaftDurabilityFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }
}

impl RaftLogDurability for MemoryRaftLog {
    fn load_storage_current_boundary(&self) -> RaftDurabilityFuture<'_, Option<RaftLogCoordinate>> {
        self.load_storage_log_attestation()
    }
}

fn entry_for(index: u64, term: u64, leader_node: NodeId, payload: &[u8]) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::new(LeaderId::new(term, leader_node), index),
        payload: EntryPayload::Normal(StorageCommandPayload::from_bytes(payload.to_vec())),
    }
}

fn fresh_storage() -> (SqliteRaftLogStorage, Arc<dyn RaftLogDurability>) {
    let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
    let durability: Arc<dyn RaftLogDurability> = Arc::new(MemoryRaftLog::default());
    (
        SqliteRaftLogStorage::new(durability.clone(), supervisor),
        durability,
    )
}

async fn append_one(durability: &dyn RaftLogDurability, entry: &Entry<TypeConfig>) {
    let encoded = EncodedRaftLogEntry::new(
        RaftLogCoordinate::new(
            entry.log_id.index,
            entry.log_id.leader_id.term,
            entry.log_id.leader_id.voted_for().unwrap_or_default(),
        ),
        OpaqueRaftBytes::new(serde_json::to_vec(entry).unwrap()),
    );
    durability
        .append_log_entries(RaftLogBatch::new(vec![encoded]).unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn append_then_read_back_roundtrip() {
    let (mut storage, durability) = fresh_storage();
    let entries = vec![
        entry_for(1, 1, 10, b"a"),
        entry_for(2, 1, 10, b"b"),
        entry_for(3, 1, 10, b"c"),
    ];
    for entry in &entries {
        append_one(durability.as_ref(), entry).await;
    }
    let state = storage.get_log_state().await.unwrap();
    assert_eq!(state.last_log_id.unwrap().index, 3);
    assert!(state.last_purged_log_id.is_none());
    let got = storage.try_get_log_entries(1..4).await.unwrap();
    assert_eq!(got.len(), 3);
    assert_eq!(got[0].log_id.index, 1);
    assert_eq!(got[2].log_id.index, 3);
}

#[tokio::test]
async fn truncate_removes_divergent_tail() {
    let (mut storage, durability) = fresh_storage();
    for index in 1..=5 {
        append_one(durability.as_ref(), &entry_for(index, 1, 10, b"x")).await;
    }
    storage
        .truncate(LogId::new(LeaderId::new(1, 10), 3))
        .await
        .unwrap();
    let got = storage.try_get_log_entries(0..10).await.unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].log_id.index, 1);
    assert_eq!(got[1].log_id.index, 2);
}

#[tokio::test]
async fn purge_removes_prefix_and_updates_last_purged() {
    let (mut storage, durability) = fresh_storage();
    for index in 1..=5 {
        append_one(durability.as_ref(), &entry_for(index, 1, 10, b"x")).await;
    }
    storage
        .purge(LogId::new(LeaderId::new(1, 10), 3))
        .await
        .unwrap();
    let state = storage.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id.unwrap().index, 3);
    assert_eq!(state.last_log_id.unwrap().index, 5);
    let got = storage.try_get_log_entries(0..10).await.unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].log_id.index, 4);
}

#[tokio::test]
async fn vote_round_trips() {
    let (mut storage, _) = fresh_storage();
    assert!(storage.read_vote().await.unwrap().is_none());
    let vote = Vote::new(7, 10);
    storage.save_vote(&vote).await.unwrap();
    assert_eq!(storage.read_vote().await.unwrap().unwrap(), vote);
}

#[tokio::test]
async fn committed_round_trips() {
    let (mut storage, _) = fresh_storage();
    assert!(storage.read_committed().await.unwrap().is_none());
    let id = LogId::new(LeaderId::new(2, 10), 42);
    storage.save_committed(Some(id)).await.unwrap();
    assert_eq!(storage.read_committed().await.unwrap().unwrap(), id);
}
