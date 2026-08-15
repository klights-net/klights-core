//! Datastore — runtime contract (`DatastoreBackend`) plus backend
//! implementations. The trait surface is in `backend.rs`, while canonical
//! persistence and snapshot values live in `klights-cluster-store`; each backend lives in
//! its own sibling folder. Today
//! there is one backend: `sqlite/`. Future backends slot in alongside
//! with the same internal shape.

pub mod backend;

#[cfg(test)]
pub use backend::CommitObservationSink;
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
