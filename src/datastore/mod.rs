//! Datastore — runtime contract (`DatastoreBackend`) plus backend
//! implementations. The trait surface is in `backend.rs`, shared types in
//! `types.rs`, and each backend lives in its own sibling folder. Today
//! there is one backend: `sqlite/`. Future backends slot in alongside
//! with the same internal shape.

pub mod backend;
pub mod backend_kind;
pub(crate) mod cluster_store_adapter;
pub mod diagnostics;
pub mod domain;
pub mod node_local;
pub mod pod_serviceaccount;
pub mod redb;
pub(crate) mod selector;
pub mod snapshot;
pub(crate) mod snapshot_export;
pub mod sqlite;
pub mod types;

pub(crate) use backend::DatastoreBackendLifecyclePort;
pub use backend::{
    AppliedOutboxStore, ClusterResourceQueryStore, CommittedOutboxApply,
    CurrentResourceVersionStore, DatastoreBackend, DatastoreBackendMetaStore,
    DatastoreBackendWatchStore, DatastoreHandle, DurableRecoveryStore, LeaderResourceMutationStore,
    MetaStore, NamespaceContentStore, NamespaceStore, NetworkMetadataStore, OwnershipStore,
    PodCleanupStore, RawWatchReplayStore, ReplicationStore, ResourceListStore, ResourceStore,
    SnapshotExclusiveFence, SnapshotMutationFence, StatusStore, WatchBroadcastMode,
    WatchHistoryStore, WatchMaintenanceStore, WatchReplayAnchorStore, WatchStore,
};
#[cfg(test)]
pub use backend::{CommitObservationSink, TestWatchStore};
pub use klights_cluster_core::{
    PatchKind, PositionedWatchEvent, Resource, ResourceBatchOperation, ResourceBatchPutMode,
    ResourcePatchRequest, ResourcePreconditions, WatchReplayPosition,
};
pub use klights_cluster_store::StagedPostCommit;
#[cfg(test)]
pub(crate) use klights_watch::WatchTopic;
#[cfg(test)]
pub use types::ReplicatedCreateOptions;
pub use types::{
    CatchUpResource, ClusterMetadataObservation, DurableAllocatorObservation, ListPageRequest,
    POD_CLEANUP_REASON_NODE_LOST, PositionedWatchReplay, PositionedWatchReplayRead,
    ReplicatedMembershipState, ReplicatedSnapshotMetadata, ResourceList, ResourceListQuery,
    SnapshotAtRv, WatchReplayFloor, WatchReplayRead, WatchTarget, WatchTargetScope,
};

#[cfg(test)]
pub use sqlite::test_support;

#[cfg(test)]
pub use sqlite::{create_staged_post_commit, staged_post_commit_from_event, staged_test_event};
