//! Root-private datastore vocabulary re-exported from canonical lower crates.

pub use klights_cluster_core::{
    PatchKind, PositionedWatchEvent, Resource, ResourceBatchOperation, ResourceBatchPutMode,
    ResourcePatchRequest, ResourcePreconditions, WatchReplayPosition,
};
#[cfg(test)]
pub use klights_cluster_datastore::sqlite::embedded::ReplicatedCreateOptions;
#[cfg(test)]
pub use klights_cluster_store::CommitObservationSink;
pub use klights_cluster_store::{
    CommittedOutboxApply, SnapshotExclusiveFence, SnapshotMutationFence, StagedPostCommit,
};
