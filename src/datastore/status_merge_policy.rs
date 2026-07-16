//! Compatibility path for the canonical cluster apply-status policy.

pub use klights_cluster_core::{
    ConditionMergeMode, FieldMergeMode, FreshStatusMode, GenericStaleStatusMode,
    GenericStatusMergePolicy, StatusApplyFreshness, StatusApplyOrigin, StatusMergeProfile,
    StatusMergeProfileKind, StatusMergeRegistry, apply_status_merge, merge_status_for_apply,
};
