//! Datastore — runtime contract (`DatastoreBackend`) plus backend
//! implementations. The trait surface is in `backend.rs`, shared types in
//! `types.rs`, and each backend lives in its own sibling folder. Today
//! there is one backend: `sqlite/`. Future backends slot in alongside
//! with the same internal shape.

pub mod backend;
pub mod backend_kind;
pub mod domain;
pub mod node_local;
pub mod pod_serviceaccount;
pub mod redb;
pub(crate) mod selector;
pub mod snapshot;
pub(crate) mod snapshot_export;
pub mod sqlite;
pub mod types;

#[cfg(any(test, feature = "integration-test-harness"))]
pub use backend::CommitObservationSink;
pub(crate) use backend::DatastoreBackendLifecyclePort;
#[cfg(test)]
pub use backend::TestWatchStore;
pub use backend::{
    AppliedOutboxStore, ClusterResourceQueryStore, CommittedOutboxApply,
    CurrentResourceVersionStore, DatastoreBackend, DatastoreHandle, DurableRecoveryStore,
    LeaderResourceMutationStore, MetaStore, NamespaceContentStore, NamespaceStore,
    NetworkMetadataStore, OwnershipStore, PodCleanupStore, RawWatchReplayStore, ReplicationStore,
    ResourceListStore, ResourceStore, SnapshotExclusiveFence, SnapshotMutationFence, StatusStore,
    WatchHistoryStore, WatchMaintenanceStore, WatchReplayAnchorStore, WatchStore,
};
pub use klights_cluster_core::{
    PatchKind, PositionedWatchEvent, Resource, ResourceBatchOperation, ResourceBatchPutMode,
    ResourcePatchRequest, ResourcePreconditions, WatchReplayPosition,
};
#[cfg(test)]
pub use klights_cluster_datastore::sqlite::embedded::ReplicatedCreateOptions;
pub use klights_cluster_store::StagedPostCommit;
#[cfg(any(test, feature = "integration-test-harness"))]
pub(crate) use klights_watch::WatchTopic;
pub use types::{
    CatchUpResource, ClusterMetadataObservation, DurableAllocatorObservation, ListPageRequest,
    POD_CLEANUP_REASON_NODE_LOST, PositionedWatchReplay, PositionedWatchReplayRead,
    ReplicatedMembershipState, ReplicatedSnapshotMetadata, ResourceList, ResourceListQuery,
    SnapshotAtRv, WatchReplayFloor, WatchReplayRead, WatchTarget, WatchTargetScope,
};

#[cfg(test)]
pub use sqlite::create_staged_post_commit;
#[cfg(any(test, feature = "integration-test-harness"))]
pub use sqlite::{staged_post_commit_from_event, staged_test_event};
