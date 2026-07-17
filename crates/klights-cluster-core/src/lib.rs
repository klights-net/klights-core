//! Pure cluster-state domain logic for klights.

pub mod apply;
pub mod command;
pub mod log_apply;
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
pub use log_apply::{
    ClusterMetaMutation, ClusterMutation, CommittedApplyOutcome, CommittedApplyRejection,
    InvalidOutboxStreamSequence, LogApplyAppliedOutboxRow, LogApplyCommit, LogApplyMutation,
    LogApplyNamespaceRow, LogApplyNodeDataplaneRow, LogApplyNodeSubnetAllocation,
    LogApplyNodeSubnetRow, LogApplyPodCleanupIntentKey, LogApplyPodCleanupIntentRow,
    LogApplyResourceKey, LogApplyResourcePatch, LogApplyResourceRow, LogApplyWatchEventRow,
    NamespaceMutation, NetworkMutation, NoPublicChangeReason, OutboxLedgerMutation,
    OutboxStreamWatermark, OutboxWatermarkDecision, PodCleanupMutation, ResourceMutation,
    ResourceVersionAssignment, ResourceVersionAssignmentError, UnsupportedClusterMutationVersion,
    VersionedClusterMutation, WatchHistoryMutation, commit_with_outbox_rows_only,
    decide_outbox_watermark, is_stamped_pod_status_outbox_operation,
    stamped_pod_status_subject_and_stamp,
};
pub use membership::{
    ClusterMembership, NodeId, RaftShape, merge_controlplane_join_membership_metadata,
    raft_node_id_for_node_name,
};
pub use recovery::{ClusterMetadata, MetadataComparison, compare_metadata, needs_confirmation};
pub use replay::{PositionedWatchEvent, WatchReplayPosition};
pub use resource::{
    PatchKind, Resource, ResourceBatchOperation, ResourceBatchPutMode, ResourceEventObject,
    ResourceIdentityError, ResourcePatchRequest, ResourcePreconditions,
};
pub use stale_resource::apply_same_uid_stale_full_resource_policy;
pub use status::{
    ConditionMergeMode, FieldMergeMode, FreshStatusMode, GenericStaleStatusMode,
    GenericStatusMergePolicy, StatusApplyFreshness, StatusApplyOrigin, StatusMergeProfile,
    StatusMergeProfileKind, StatusMergeRegistry, apply_status_merge, merge_node_status_for_update,
    merge_status_for_apply,
};
