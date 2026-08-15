//! Cluster storage ports for klights.

/// Durable metadata key for the cluster's stable identity.
pub const CLUSTER_ID_META_KEY: &str = "cluster_id";
/// Durable metadata key for the current leader epoch.
pub const LEADER_EPOCH_META_KEY: &str = "leader_epoch";
/// Durable metadata key recording exact activation of command codec v3.
pub const COMMAND_CODEC_ACTIVATION_VERSION_META_KEY: &str = "command_codec_activation_version";
/// The only accepted persisted command codec activation value.
pub const COMMAND_CODEC_V3_ACTIVATION_VALUE: &str = "3";

mod apply_result;
mod backend_snapshot;
mod committed_apply;
mod durable_recovery;
mod namespace_content;
mod ownership;
mod persistence_contract;
mod persistence_error;
mod persistence_ports;
mod post_commit;
mod raw_watch_history;
mod read_validation;
mod replay_retention;
mod resource_read;
mod resource_scope;
mod response_codec;
mod topology;
mod watch_range;

pub use apply_result::{AppliedMutation, StorageCommandResult};
pub use backend_snapshot::{
    BackendLifecycleStore, DatastoreSnapshotter, SnapshotEntry, SnapshotEnvelope,
    SnapshotExclusiveFence, SnapshotMutationFence, SnapshotRestoreError, SnapshotTable,
    compute_schema_fingerprint,
};
pub use committed_apply::{
    AppliedOutboxLookup, CommittedApplyError, CommittedApplyFuture, CommittedRaftApplyReceipt,
    CommittedRaftApplyRequest, DurableApplyLedgerRead, PrivilegedCommittedRaftApply,
};
pub use durable_recovery::{
    AllocatorStateError, AllocatorStateFuture, AuthoritativeSnapshot, AuthoritativeSnapshotCapture,
    AuthoritativeSnapshotParts, AuthoritativeSnapshotPersistence, ClusterMetadataFuture,
    ClusterMetadataRead, ClusterMetadataStoreError, DurableAllocatorRead, DurableAllocatorState,
    DurableReplayBoundary, DurableReplayFloor, DurableReplayTarget, DurableWatchEvent,
    DurableWatchHistoryRead, DurableWatchScope, DurableWatchTarget, MAX_SNAPSHOT_CAPTURE_PAGE,
    MAX_WATCH_HISTORY_PAGE, PersistedClusterMetadata, RAFT_LEADER_HINT_META_KEY,
    RAFT_TERM_META_KEY, RAFT_VOTERS_META_KEY, SnapshotCaptureCursor, SnapshotCaptureHeader,
    SnapshotCapturePage, SnapshotCapturePageKind, SnapshotCaptureRequest, SnapshotCaptureSession,
    SnapshotCommitCursor, SnapshotMembership, SnapshotOutboxWatermarkCursor, SnapshotPageLimit,
    SnapshotPersistenceError, SnapshotPersistenceFuture, SnapshotReplayFloorCursor,
    WatchHistoryError, WatchHistoryFuture, WatchHistoryPage, WatchHistoryRead, WatchHistoryRequest,
};
pub use namespace_content::{
    NamespaceContentFuture, NamespaceContentRead, NamespaceKindRequest, NamespaceRequest,
};
pub use ownership::{
    ClusterOwnershipRead, OwnedKindRequest, OwnerNameKindRequest, OwnerUidRequest,
    OwnershipReadFuture,
};
pub use persistence_contract::{
    CatchUpResource, ClusterMetadataObservation, DurableAllocatorObservation, ListPageRequest,
    POD_CLEANUP_REASON_NODE_LOST, PositionedWatchReplay, PositionedWatchReplayRead,
    ReplicatedMembershipState, ReplicatedSnapshotMetadata, ResourceList, ResourceListOptions,
    SnapshotAtRv, WatchReplayFloor, WatchReplayRead, WatchTarget, WatchTargetScope,
};
pub use persistence_error::{
    ClusterStoreError, ClusterStoreErrorKind, ClusterStoreResult, PersistenceBackend,
};
pub use persistence_ports::{
    AppliedOutboxLedger, ClusterMetadataMutation, ClusterNamespaceMutation, ClusterPodCleanupStore,
    ClusterResourceMutation, ClusterTopologyMutation, ClusterWatchMaintenance,
    CommittedOutboxApply,
};
#[cfg(feature = "test-support")]
pub use post_commit::StagedResourceEvent;
pub use post_commit::{CommitObservationSink, StagedPostCommit};
pub use raw_watch_history::{
    DurableRawWatchEvent, DurableRawWatchHistoryRead, PositionedRawWatchHistoryPage,
    PositionedRawWatchHistoryRead, RawWatchEventsAfterPositionRequest, RawWatchEventsSinceRequest,
    RawWatchHistoryFuture, RawWatchHistoryPage, RawWatchHistoryRead,
};
pub use replay_retention::{ReplayAvailability, ReplayRetentionBoundary};
pub use resource_read::{
    ClusterResourceRead, ResourceCollectionKey, ResourceCollectionScope, ResourceContinuation,
    ResourceGetRequest, ResourceListPage, ResourceListQuery, ResourceListRead,
    ResourceListRecoveryContinuation, ResourceListRequest, ResourceListSnapshot, ResourceReadError,
    ResourceReadFuture, ResourceReadStatus, ResourceVersionMatch,
};
pub use resource_scope::{
    ClusterResourceScopeRead, ResourceKeyScopeRequest, ResourceScopeSnapshot,
    ResourceSnapshotAtPositionRequest, ResourceSnapshotRead, ResourceWatchTargetsRequest,
};
pub use response_codec::OutboxResponseCodec;
pub use topology::{
    ClusterTopologyFuture, ClusterTopologyRead, ClusterTopologyReadError, DataplaneEncryption,
    DataplaneMetadataError, DataplaneMode, DataplanePeerMetadata, NodeTopologyRequest,
    PeerTopologyRequest, StoredNodeSubnet, WireGuardPublicKey,
};
pub use watch_range::{
    DurableWatchRangeRead, ModifiedClusterResourcesRequest, ModifiedResourcesRequest,
    WatchEventsSinceRequest, WatchRangeFuture, WatchRangeStart,
};
