//! Node-local storage ports for klights.

mod cache_network;
mod delivery;
mod identity;
mod open;
mod raft_durability;
mod runtime_work;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

pub use cache_network::{
    CacheNetworkError, CacheNetworkFuture, EndpointDeleteOutcome, EndpointUpsertOutcome, NodeKey,
    PodEndpointMode, PodEndpointRecord, PodEndpointStore, PodEndpointStoreEvent,
    PodEndpointStoreEventSource, PodEndpointStoreEventStream, PodEndpointStoreEventSubscription,
    PodIpamStore, PodNetworkAllocation, PodNetworkAllocationRequest, PodNetworkAssignmentSnapshot,
    PodNetworkCache, PodNetworkEndpoint, PodUidKey, SandboxKey,
};

pub use delivery::{
    DeadLetterEntry, DeadLetterKey, DeadLetterMoveRequest, DeadLetterReplayRequest,
    DeadLetterStore, DeliveryError, DeliveryFuture, MAX_OUTBOX_BATCH, OUTBOX_DIAGNOSTIC_AGING_MS,
    OutboxAttemptFailure, OutboxAttemptFailureRecord, OutboxBatchClaimRequest, OutboxClaimRequest,
    OutboxClassification, OutboxCompletion, OutboxDispatchCounters, OutboxDispatcherStore,
    OutboxEnqueue, OutboxFailureDisposition, OutboxLease, OutboxNow, OutboxPriority,
    OutboxProducerStore, OutboxRecord, OutboxSequence, OutboxSequencePolicy, OutboxStats,
    OutboxStatusStampStore, OutboxSubject, OutboxSupersedability, OutboxSupersedeRequest,
    PodCheckpointKey, PodStatusCheckpoint, PodStatusCheckpointApplied, PodStatusCheckpointStore,
    PodStatusCheckpointUpsert, RuntimeObservationCheckpoint, RuntimeObservationCheckpointStore,
    RuntimeObservationGeneration, TerminalDeleteClassification,
};
pub use identity::{NodeIdentity, NodeIdentityError, NodeIdentityFuture};
pub use open::NodeStoreOpenError;
pub use raft_durability::{
    EncodedRaftAppliedState, EncodedRaftAppliedValue, EncodedRaftLogEntry, EncodedRaftLogState,
    EncodedRaftStorageBoundary, OpaqueRaftBytes, RaftAppliedStateDurability,
    RaftAppliedStatePersistence, RaftAppliedStatePersistenceWrite, RaftAppliedStateWrite,
    RaftDurabilityError, RaftDurabilityFuture, RaftLogBatch, RaftLogCoordinate, RaftLogDurability,
    RaftLogPersistence, RaftLogRange, RaftPurgeRequest,
};
pub use runtime_work::{
    DueTimeMs, ObservedPodVersion, OwnedPodSandbox, PodRuntimeAdmission, PodRuntimeCgroup,
    PodRuntimeRecord, PodRuntimeStore, PodSlotAdmissionEvent, PodSlotAdmissionEventSource,
    PodSlotAdmissionRequest, PodSlotAdmissionResult, PodSlotAdmissionState, PodSlotAdmissionStore,
    PodSlotClearResult, PodSlotEventSubscription, PodSlotMutationResult, PodWorkIdentity,
    PodWorkqueueClaimRequest, PodWorkqueueEnqueue, PodWorkqueueEntry, PodWorkqueueKind,
    PodWorkqueueLease, PodWorkqueueLeaseToken, PodWorkqueueMutationOutcome, PodWorkqueueRequeue,
    PodWorkqueueStore, ProbeKey, ProbeResult, ProbeState, ProbeStateStore, RuntimeNamespace,
    RuntimePodUid, RuntimeWorkError, RuntimeWorkFuture, WorkItemId,
};

#[cfg(test)]
mod open_error_contract {
    #[test]
    fn node_open_error_keeps_schema_mismatch_actionable() {
        let error = super::NodeStoreOpenError::SchemaMismatch {
            path: "node.db".to_string(),
            expected: "new".to_string(),
            actual: "old".to_string(),
            hint: "wipe node state".to_string(),
        };
        let message = error.to_string();
        assert!(message.contains("node.db"));
        assert!(message.contains("new"));
        assert!(message.contains("old"));
        assert!(message.contains("wipe node state"));
    }
}
