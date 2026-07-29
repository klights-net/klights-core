//! Pure cluster-state domain logic for klights.

pub mod apply;
pub mod command;
pub mod k8s_time;
pub mod log_apply;
pub mod membership;
pub mod node_projection;
pub mod outbox;
pub mod recovery;
pub mod replay;
pub mod resource;
pub mod stale_resource;
pub mod status;

pub use apply::{
    ApplyPreconditionViolation, ApplyPreconditions, CurrentResourceState, ResourceDeleteDecision,
    ResourceEventType, ResourceWriteDecision, StatusStampDecision, decide_resource_delete,
    decide_resource_put, decide_status_stamp, resource_bodies_equal_ignoring_metadata_field,
    resource_client_owned_state_equal, validate_apply_preconditions,
};
pub use command::{
    COMMAND_CODEC_VERSION, CommandError, CommandId, CommandMeta,
    DEFAULT_NODE_HEARTBEAT_INTERVAL_SECONDS, DEFAULT_NODE_LEASE_DURATION_SECONDS,
    LeaseRenewCommandError, StorageCommand, StorageCommandRejectionCode, StorageMutationError,
    StorageResponse, supports_command_codec_version, validate_lease_renew_command,
};
pub use log_apply::{
    ClusterMetaMutation, ClusterMutation, CommittedApplyOutcome, CommittedApplyRejection,
    InvalidOutboxStreamSequence, LiveCommitResourceVersionError, LogApplyAppliedOutboxRow,
    LogApplyCommit, LogApplyMutation, LogApplyNamespaceRow, LogApplyNodeDataplaneRow,
    LogApplyNodeSubnetAllocation, LogApplyNodeSubnetRow, LogApplyPodActorFinalization,
    LogApplyPodCleanupIntentKey, LogApplyPodCleanupIntentRow, LogApplyResourceKey,
    LogApplyResourcePatch, LogApplyResourceRow, LogApplyWatchEventRow, NamespaceMutation,
    NetworkMutation, NoPublicChangeReason, OutboxLedgerMutation, OutboxStreamWatermark,
    OutboxWatermarkDecision, PodCleanupMutation, ResourceMutation, SnapshotRestoreOperation,
    UnsupportedClusterMutationVersion, VersionedClusterMutation, WatchHistoryMutation,
    commit_with_outbox_rows_only, decide_outbox_watermark, is_stamped_pod_status_outbox_operation,
    resource_snapshot_restore_operation, stamped_pod_status_subject_and_stamp,
};
pub use membership::{
    ClusterMembership, NodeId, RaftShape, merge_controlplane_join_membership_metadata,
    raft_node_id_for_node_name,
};
pub use node_projection::{
    merge_existing_node_mutable_fields, prune_klights_managed_node_role_labels,
    set_node_external_ip_from_dataplane_annotation, set_node_pod_cidr,
};
pub use outbox::{
    BuildOutboxOutcome, OutboxApplyError, OutboxApplyOutcome, OutboxOperation, OutboxPayload,
    OutboxPriority, UnknownOutboxOperation, classify_apply_error, classify_apply_error_for_command,
    pod_target, subject_key_for_command,
};
pub use recovery::{ClusterMetadata, MetadataComparison, compare_metadata, needs_confirmation};
pub use replay::{PositionedWatchEvent, WatchReplayPosition};
pub use resource::{
    PatchKind, PodEndpointState, Resource, ResourceBatchOperation, ResourceBatchPutMode,
    ResourceEventObject, ResourceIdentityError, ResourcePatchRequest, ResourcePreconditions,
    pod_endpoint_state, pod_endpoint_state_changed,
};
pub use stale_resource::apply_same_uid_stale_full_resource_policy;
pub use status::set_node_external_ip;
pub use status::{
    ConditionMergeMode, FieldMergeMode, FreshStatusMode, GenericStaleStatusMode,
    GenericStatusMergePolicy, StatusApplyFreshness, StatusApplyOrigin, StatusMergeProfile,
    StatusMergeProfileKind, StatusMergeRegistry, apply_status_merge, merge_node_status_for_update,
    merge_status_for_apply,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceMutationEffect {
    Unchanged,
    Changed,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PodEndpointEffect {
    #[default]
    NotApplicable,
    Unchanged,
    Changed,
}
