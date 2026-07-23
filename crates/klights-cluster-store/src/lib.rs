//! Cluster storage ports for klights.

/// Durable metadata key for the cluster's stable identity.
pub const CLUSTER_ID_META_KEY: &str = "cluster_id";
/// Durable metadata key for the current leader epoch.
pub const LEADER_EPOCH_META_KEY: &str = "leader_epoch";

mod committed_apply;
mod durable_recovery;
mod resource_read;

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
pub use resource_read::{
    ClusterResourceRead, ResourceCollectionKey, ResourceCollectionScope, ResourceContinuation,
    ResourceGetRequest, ResourceListPage, ResourceListQuery, ResourceListRead, ResourceListRequest,
    ResourceListSnapshot, ResourceReadError, ResourceReadFuture, ResourceReadStatus,
    ResourceVersionMatch,
};
