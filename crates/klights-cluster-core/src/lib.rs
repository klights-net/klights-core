//! Pure cluster-state domain logic for klights.

pub mod apply;
pub mod command;
pub mod membership;
pub mod recovery;
pub mod replay;
pub mod resource;
pub mod stale_resource;
pub mod status;

pub use apply::{
    ApplyPreconditionPolicy, ApplyPreconditionViolation, ApplyPreconditions, CurrentResourceState,
    ResourceDeleteDecision, ResourceEventType, ResourceWriteDecision, StatusStampDecision,
    decide_resource_delete, decide_resource_put, decide_status_stamp,
    resource_bodies_equal_ignoring_metadata_field, resource_client_owned_state_equal,
    validate_apply_preconditions,
};
pub use command::{
    COMMAND_CODEC_VERSION, CommandError, CommandId, CommandMeta, StorageCommand, StorageResponse,
};
pub use membership::{
    ClusterMembership, NodeId, RaftShape, merge_controlplane_join_membership_metadata,
    raft_node_id_for_node_name,
};
pub use recovery::{ClusterMetadata, MetadataComparison, compare_metadata, needs_confirmation};
pub use replay::{PositionedWatchEvent, WatchReplayPosition};
pub use resource::{
    PatchKind, Resource, ResourceBatchOperation, ResourceBatchPutMode, ResourceEventObject,
    ResourcePatchRequest, ResourcePreconditions,
};
pub use stale_resource::apply_same_uid_stale_full_resource_policy;
pub use status::{
    ConditionMergeMode, FieldMergeMode, FreshStatusMode, GenericStaleStatusMode,
    GenericStatusMergePolicy, StatusApplyFreshness, StatusApplyOrigin, StatusMergeProfile,
    StatusMergeProfileKind, StatusMergeRegistry, apply_status_merge, merge_node_status_for_update,
    merge_status_for_apply,
};
