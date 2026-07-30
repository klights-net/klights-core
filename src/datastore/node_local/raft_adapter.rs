//! Node-local OpenRaft conversion over opaque durability ports.
//!
//! The passive persistence implementation never decodes OpenRaft values. This
//! adapter reconstructs current-boundary coordinates and supplies neutral
//! coordinates alongside byte-exact applied-state payloads.

use std::sync::Arc;

use klights_node_store::{
    EncodedRaftAppliedState, EncodedRaftAppliedValue, EncodedRaftLogEntry, EncodedRaftLogState,
    OpaqueRaftBytes, RaftAppliedStateDurability, RaftAppliedStatePersistence,
    RaftAppliedStatePersistenceWrite, RaftAppliedStateWrite, RaftDurabilityError,
    RaftDurabilityFuture, RaftLogBatch, RaftLogCoordinate, RaftLogDurability, RaftLogPersistence,
    RaftLogRange, RaftPurgeRequest,
};
use openraft::LogId;

use crate::datastore::raft::types::NodeId;

pub(crate) struct OpenRaftNodeDurabilityAdapter {
    log: Arc<dyn RaftLogPersistence>,
    applied: Arc<dyn RaftAppliedStatePersistence>,
}

impl OpenRaftNodeDurabilityAdapter {
    pub(crate) fn new(
        log: Arc<dyn RaftLogPersistence>,
        applied: Arc<dyn RaftAppliedStatePersistence>,
    ) -> Self {
        Self { log, applied }
    }
}

fn coordinate(log_id: LogId<NodeId>) -> RaftLogCoordinate {
    RaftLogCoordinate::new(
        log_id.index,
        log_id.leader_id.term,
        log_id.leader_id.node_id,
    )
}

fn persistence(operation: &'static str, error: impl std::fmt::Display) -> RaftDurabilityError {
    RaftDurabilityError::persistence_failed(operation, error.to_string())
}

fn load_current_boundary(
    log: &dyn RaftLogPersistence,
) -> RaftDurabilityFuture<'_, Option<RaftLogCoordinate>> {
    Box::pin(async move {
        let (last, purged, applied) = log.load_storage_boundary_state().await?.into_parts();
        let purged = purged
            .map(|bytes| {
                serde_json::from_slice::<LogId<NodeId>>(bytes.as_slice())
                    .map(coordinate)
                    .map_err(|error| persistence("load_storage_current_boundary", error))
            })
            .transpose()?;
        let applied = applied
            .map(|bytes| {
                serde_json::from_slice::<Option<LogId<NodeId>>>(bytes.as_slice())
                    .map(|value| value.map(coordinate))
                    .map_err(|error| persistence("load_storage_current_boundary", error))
            })
            .transpose()?
            .flatten();
        Ok([last, purged, applied]
            .into_iter()
            .flatten()
            .max_by_key(|value| value.index()))
    })
}

fn store_applied(
    applied: &dyn RaftAppliedStatePersistence,
    state: RaftAppliedStateWrite,
) -> RaftDurabilityFuture<'_, ()> {
    Box::pin(async move {
        let (last, membership) = state.into_parts();
        let last = last
            .map(|bytes| {
                let decoded = serde_json::from_slice::<Option<LogId<NodeId>>>(bytes.as_slice())
                    .map_err(|error| persistence("decode applied LogId", error))?;
                Ok(EncodedRaftAppliedValue::new(decoded.map(coordinate), bytes))
            })
            .transpose()?;
        applied
            .store_applied_state_persistence(RaftAppliedStatePersistenceWrite::new(
                last, membership,
            ))
            .await
    })
}

impl RaftLogPersistence for OpenRaftNodeDurabilityAdapter {
    fn read_log_range(
        &self,
        range: RaftLogRange,
    ) -> RaftDurabilityFuture<'_, Vec<EncodedRaftLogEntry>> {
        self.log.read_log_range(range)
    }

    fn load_log_state(&self) -> RaftDurabilityFuture<'_, EncodedRaftLogState> {
        self.log.load_log_state()
    }

    fn append_log_entries(&self, entries: RaftLogBatch) -> RaftDurabilityFuture<'_, ()> {
        self.log.append_log_entries(entries)
    }

    fn truncate_log_from(&self, from_inclusive: u64) -> RaftDurabilityFuture<'_, ()> {
        self.log.truncate_log_from(from_inclusive)
    }

    fn purge_log_through(&self, request: RaftPurgeRequest) -> RaftDurabilityFuture<'_, ()> {
        self.log.purge_log_through(request)
    }

    fn load_vote(&self) -> RaftDurabilityFuture<'_, Option<OpaqueRaftBytes>> {
        self.log.load_vote()
    }

    fn store_vote(&self, encoded_vote: OpaqueRaftBytes) -> RaftDurabilityFuture<'_, ()> {
        self.log.store_vote(encoded_vote)
    }

    fn load_committed(&self) -> RaftDurabilityFuture<'_, Option<OpaqueRaftBytes>> {
        self.log.load_committed()
    }

    fn store_committed(&self, encoded_committed: OpaqueRaftBytes) -> RaftDurabilityFuture<'_, ()> {
        self.log.store_committed(encoded_committed)
    }

    fn load_or_create_storage_incarnation(&self) -> RaftDurabilityFuture<'_, String> {
        self.log.load_or_create_storage_incarnation()
    }

    fn load_storage_log_attestation(&self) -> RaftDurabilityFuture<'_, Option<RaftLogCoordinate>> {
        self.log.load_storage_log_attestation()
    }

    fn load_storage_boundary_state(
        &self,
    ) -> RaftDurabilityFuture<'_, klights_node_store::EncodedRaftStorageBoundary> {
        self.log.load_storage_boundary_state()
    }

    fn reset_orphaned_learner_log(&self) -> RaftDurabilityFuture<'_, bool> {
        self.log.reset_orphaned_learner_log()
    }
}

impl RaftLogDurability for OpenRaftNodeDurabilityAdapter {
    fn load_storage_current_boundary(&self) -> RaftDurabilityFuture<'_, Option<RaftLogCoordinate>> {
        load_current_boundary(self.log.as_ref())
    }
}

impl RaftAppliedStateDurability for OpenRaftNodeDurabilityAdapter {
    fn load_applied_state(&self) -> RaftDurabilityFuture<'_, EncodedRaftAppliedState> {
        self.applied.load_applied_state()
    }

    fn store_applied_state(&self, state: RaftAppliedStateWrite) -> RaftDurabilityFuture<'_, ()> {
        store_applied(self.applied.as_ref(), state)
    }
}

#[cfg(test)]
impl RaftLogPersistence for crate::datastore::node_local::NodeLocalStores {
    fn read_log_range(
        &self,
        range: RaftLogRange,
    ) -> RaftDurabilityFuture<'_, Vec<EncodedRaftLogEntry>> {
        self.raft_persistence_ref().read_log_range(range)
    }

    fn load_log_state(&self) -> RaftDurabilityFuture<'_, EncodedRaftLogState> {
        self.raft_persistence_ref().load_log_state()
    }

    fn append_log_entries(&self, entries: RaftLogBatch) -> RaftDurabilityFuture<'_, ()> {
        self.raft_persistence_ref().append_log_entries(entries)
    }

    fn truncate_log_from(&self, from_inclusive: u64) -> RaftDurabilityFuture<'_, ()> {
        self.raft_persistence_ref()
            .truncate_log_from(from_inclusive)
    }

    fn purge_log_through(&self, request: RaftPurgeRequest) -> RaftDurabilityFuture<'_, ()> {
        self.raft_persistence_ref().purge_log_through(request)
    }

    fn load_vote(&self) -> RaftDurabilityFuture<'_, Option<OpaqueRaftBytes>> {
        self.raft_persistence_ref().load_vote()
    }

    fn store_vote(&self, encoded_vote: OpaqueRaftBytes) -> RaftDurabilityFuture<'_, ()> {
        self.raft_persistence_ref().store_vote(encoded_vote)
    }

    fn load_committed(&self) -> RaftDurabilityFuture<'_, Option<OpaqueRaftBytes>> {
        self.raft_persistence_ref().load_committed()
    }

    fn store_committed(&self, encoded_committed: OpaqueRaftBytes) -> RaftDurabilityFuture<'_, ()> {
        self.raft_persistence_ref()
            .store_committed(encoded_committed)
    }

    fn load_or_create_storage_incarnation(&self) -> RaftDurabilityFuture<'_, String> {
        self.raft_persistence_ref()
            .load_or_create_storage_incarnation()
    }

    fn load_storage_log_attestation(&self) -> RaftDurabilityFuture<'_, Option<RaftLogCoordinate>> {
        self.raft_persistence_ref().load_storage_log_attestation()
    }

    fn load_storage_boundary_state(
        &self,
    ) -> RaftDurabilityFuture<'_, klights_node_store::EncodedRaftStorageBoundary> {
        self.raft_persistence_ref().load_storage_boundary_state()
    }

    fn reset_orphaned_learner_log(&self) -> RaftDurabilityFuture<'_, bool> {
        self.raft_persistence_ref().reset_orphaned_learner_log()
    }
}

#[cfg(test)]
impl RaftLogDurability for crate::datastore::node_local::NodeLocalStores {
    fn load_storage_current_boundary(&self) -> RaftDurabilityFuture<'_, Option<RaftLogCoordinate>> {
        load_current_boundary(self.raft_persistence_ref())
    }
}

#[cfg(test)]
impl RaftAppliedStateDurability for crate::datastore::node_local::NodeLocalStores {
    fn load_applied_state(&self) -> RaftDurabilityFuture<'_, EncodedRaftAppliedState> {
        self.raft_persistence_ref().load_applied_state()
    }

    fn store_applied_state(&self, state: RaftAppliedStateWrite) -> RaftDurabilityFuture<'_, ()> {
        store_applied(self.raft_persistence_ref(), state)
    }
}

#[cfg(test)]
mod tests {
    use klights_node_store::{EncodedRaftStorageBoundary, RaftAppliedStatePersistence};
    use openraft::LeaderId;

    use super::*;

    struct MemoryPersistence {
        boundary: EncodedRaftStorageBoundary,
        applied: std::sync::Mutex<Option<RaftAppliedStatePersistenceWrite>>,
    }

    impl RaftLogPersistence for MemoryPersistence {
        fn read_log_range(
            &self,
            _range: RaftLogRange,
        ) -> RaftDurabilityFuture<'_, Vec<EncodedRaftLogEntry>> {
            Box::pin(async { Ok(Vec::new()) })
        }

        fn load_log_state(&self) -> RaftDurabilityFuture<'_, EncodedRaftLogState> {
            Box::pin(async { Ok(EncodedRaftLogState::new(None, None)) })
        }

        fn append_log_entries(&self, _entries: RaftLogBatch) -> RaftDurabilityFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }

        fn truncate_log_from(&self, _from_inclusive: u64) -> RaftDurabilityFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }

        fn purge_log_through(&self, _request: RaftPurgeRequest) -> RaftDurabilityFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }

        fn load_vote(&self) -> RaftDurabilityFuture<'_, Option<OpaqueRaftBytes>> {
            Box::pin(async { Ok(None) })
        }

        fn store_vote(&self, _encoded_vote: OpaqueRaftBytes) -> RaftDurabilityFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }

        fn load_committed(&self) -> RaftDurabilityFuture<'_, Option<OpaqueRaftBytes>> {
            Box::pin(async { Ok(None) })
        }

        fn store_committed(
            &self,
            _encoded_committed: OpaqueRaftBytes,
        ) -> RaftDurabilityFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }

        fn load_or_create_storage_incarnation(&self) -> RaftDurabilityFuture<'_, String> {
            Box::pin(async { Ok("fixture".to_string()) })
        }

        fn load_storage_log_attestation(
            &self,
        ) -> RaftDurabilityFuture<'_, Option<RaftLogCoordinate>> {
            Box::pin(async { Ok(None) })
        }

        fn load_storage_boundary_state(
            &self,
        ) -> RaftDurabilityFuture<'_, EncodedRaftStorageBoundary> {
            Box::pin(async { Ok(self.boundary.clone()) })
        }

        fn reset_orphaned_learner_log(&self) -> RaftDurabilityFuture<'_, bool> {
            Box::pin(async { Ok(false) })
        }
    }

    impl RaftAppliedStatePersistence for MemoryPersistence {
        fn load_applied_state(&self) -> RaftDurabilityFuture<'_, EncodedRaftAppliedState> {
            Box::pin(async { Ok(EncodedRaftAppliedState::new(None, None)) })
        }

        fn store_applied_state_persistence(
            &self,
            state: RaftAppliedStatePersistenceWrite,
        ) -> RaftDurabilityFuture<'_, ()> {
            Box::pin(async move {
                *self.applied.lock().unwrap() = Some(state);
                Ok(())
            })
        }
    }

    fn fixture(
        boundary: EncodedRaftStorageBoundary,
    ) -> (Arc<MemoryPersistence>, OpenRaftNodeDurabilityAdapter) {
        let persistence = Arc::new(MemoryPersistence {
            boundary,
            applied: std::sync::Mutex::new(None),
        });
        let adapter = OpenRaftNodeDurabilityAdapter::new(persistence.clone(), persistence.clone());
        (persistence, adapter)
    }

    #[tokio::test]
    async fn current_boundary_decodes_only_in_openraft_adapter() {
        let purged = LogId::new(LeaderId::new(2, 3), 7);
        let applied = LogId::new(LeaderId::new(4, 5), 11);
        let (_, adapter) = fixture(EncodedRaftStorageBoundary::new(
            Some(RaftLogCoordinate::new(9, 3, 4)),
            Some(OpaqueRaftBytes::new(serde_json::to_vec(&purged).unwrap())),
            Some(OpaqueRaftBytes::new(
                serde_json::to_vec(&Some(applied)).unwrap(),
            )),
        ));

        assert_eq!(
            adapter
                .load_storage_current_boundary()
                .await
                .unwrap()
                .unwrap(),
            RaftLogCoordinate::new(11, 4, 5)
        );
    }

    #[tokio::test]
    async fn applied_conversion_preserves_exact_bytes_and_supplies_coordinate() {
        let applied = LogId::new(LeaderId::new(7, 11), 93);
        let bytes = serde_json::to_vec(&Some(applied)).unwrap();
        let (persistence, adapter) = fixture(EncodedRaftStorageBoundary::new(None, None, None));

        adapter
            .store_applied_state(RaftAppliedStateWrite::new(
                Some(OpaqueRaftBytes::new(bytes.clone())),
                Some(OpaqueRaftBytes::new(vec![4, 0, 4])),
            ))
            .await
            .unwrap();

        let stored = persistence.applied.lock().unwrap().take().unwrap();
        let (last, membership) = stored.into_parts();
        let (coordinate, payload) = last.unwrap().into_parts();
        assert_eq!(coordinate, Some(RaftLogCoordinate::new(93, 7, 11)));
        assert_eq!(payload.as_slice(), bytes);
        assert_eq!(membership.unwrap().as_slice(), [4, 0, 4]);
    }

    #[tokio::test]
    async fn encoded_null_remains_a_present_byte_exact_applied_value() {
        let bytes = serde_json::to_vec(&Option::<LogId<NodeId>>::None).unwrap();
        let (persistence, adapter) = fixture(EncodedRaftStorageBoundary::new(None, None, None));

        adapter
            .store_applied_state(RaftAppliedStateWrite::new(
                Some(OpaqueRaftBytes::new(bytes.clone())),
                None,
            ))
            .await
            .unwrap();

        let stored = persistence.applied.lock().unwrap().take().unwrap();
        let (last, _) = stored.into_parts();
        let (coordinate, payload) = last.unwrap().into_parts();
        assert_eq!(coordinate, None);
        assert_eq!(payload.as_slice(), bytes);
    }

    #[tokio::test]
    async fn boundary_decode_failure_keeps_the_existing_persistence_operation() {
        let (_, adapter) = fixture(EncodedRaftStorageBoundary::new(
            None,
            Some(OpaqueRaftBytes::new(vec![0xff])),
            None,
        ));

        assert!(matches!(
            adapter.load_storage_current_boundary().await.unwrap_err(),
            RaftDurabilityError::PersistenceFailed {
                operation: "load_storage_current_boundary",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn applied_decode_failure_keeps_the_existing_persistence_operation() {
        let (_, adapter) = fixture(EncodedRaftStorageBoundary::new(None, None, None));

        assert!(matches!(
            adapter
                .store_applied_state(RaftAppliedStateWrite::new(
                    Some(OpaqueRaftBytes::new(vec![0xff])),
                    None,
                ))
                .await
                .unwrap_err(),
            RaftDurabilityError::PersistenceFailed {
                operation: "decode applied LogId",
                ..
            }
        ));
    }
}
