#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::datastore::node_local::NodeLocalStores;
    use klights_node_store::{
        EncodedRaftLogEntry, OpaqueRaftBytes, RaftLogBatch, RaftLogCoordinate, RaftLogDurability,
    };
    use klights_replication::log_storage::SqliteRaftLogStorage;
    use klights_replication::types::{NodeId, StorageCommandPayload, TypeConfig};
    use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};
    use openraft::storage::RaftLogStorage;
    use openraft::{Entry, EntryPayload, LeaderId, LogId, RaftLogReader, Vote};

    fn entry_for(index: u64, term: u64, leader_node: NodeId, payload: &[u8]) -> Entry<TypeConfig> {
        Entry {
            log_id: LogId::new(LeaderId::new(term, leader_node), index),
            payload: EntryPayload::Normal(StorageCommandPayload::from_bytes(payload.to_vec())),
        }
    }

    async fn fresh_storage() -> (SqliteRaftLogStorage, Arc<dyn RaftLogDurability>) {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let executor = klights_node_datastore::open::open_with_opts(
            klights_node_datastore::open::in_memory_opts(),
            supervisor.clone(),
            "sqlite:raft-log-test",
        )
        .await
        .expect("open node-local executor");
        let nl: Arc<dyn RaftLogDurability> =
            Arc::new(NodeLocalStores::from_executor(executor).expect("create node-local db"));
        (SqliteRaftLogStorage::new(nl.clone(), supervisor), nl)
    }

    async fn append_one(durability: &dyn RaftLogDurability, e: &Entry<TypeConfig>) {
        let encoded = EncodedRaftLogEntry::new(
            RaftLogCoordinate::new(
                e.log_id.index,
                e.log_id.leader_id.term,
                e.log_id.leader_id.voted_for().unwrap_or_default(),
            ),
            OpaqueRaftBytes::new(serde_json::to_vec(e).unwrap()),
        );
        durability
            .append_log_entries(RaftLogBatch::new(vec![encoded]).unwrap())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn append_then_read_back_roundtrip() {
        let (mut s, durability) = fresh_storage().await;
        let entries = vec![
            entry_for(1, 1, 10, b"a"),
            entry_for(2, 1, 10, b"b"),
            entry_for(3, 1, 10, b"c"),
        ];
        for e in &entries {
            append_one(durability.as_ref(), e).await;
        }
        let state = s.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id.unwrap().index, 3);
        assert!(state.last_purged_log_id.is_none());
        let got = s.try_get_log_entries(1..4).await.unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].log_id.index, 1);
        assert_eq!(got[2].log_id.index, 3);
    }

    #[tokio::test]
    async fn truncate_removes_divergent_tail() {
        let (mut s, durability) = fresh_storage().await;
        for i in 1..=5 {
            append_one(durability.as_ref(), &entry_for(i, 1, 10, b"x")).await;
        }
        s.truncate(LogId::new(LeaderId::new(1, 10), 3))
            .await
            .unwrap();
        let got = s.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].log_id.index, 1);
        assert_eq!(got[1].log_id.index, 2);
    }

    #[tokio::test]
    async fn purge_removes_prefix_and_updates_last_purged() {
        let (mut s, durability) = fresh_storage().await;
        for i in 1..=5 {
            append_one(durability.as_ref(), &entry_for(i, 1, 10, b"x")).await;
        }
        s.purge(LogId::new(LeaderId::new(1, 10), 3)).await.unwrap();
        let state = s.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.unwrap().index, 3);
        assert_eq!(state.last_log_id.unwrap().index, 5);
        let got = s.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].log_id.index, 4);
    }

    #[tokio::test]
    async fn vote_round_trips() {
        let (mut s, _) = fresh_storage().await;
        assert!(s.read_vote().await.unwrap().is_none());
        let v = Vote::new(7, 10);
        s.save_vote(&v).await.unwrap();
        assert_eq!(s.read_vote().await.unwrap().unwrap(), v);
    }

    #[tokio::test]
    async fn committed_round_trips() {
        let (mut s, _) = fresh_storage().await;
        assert!(s.read_committed().await.unwrap().is_none());
        let id = LogId::new(LeaderId::new(2, 10), 42);
        s.save_committed(Some(id)).await.unwrap();
        assert_eq!(s.read_committed().await.unwrap().unwrap(), id);
    }
}
