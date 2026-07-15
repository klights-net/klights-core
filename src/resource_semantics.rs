//! Transitional compatibility path for canonical pure resource policy.

#[deprecated(note = "use klights_types resource helpers directly; removed in Phase 3.4")]
pub use klights_types::resource_semantics::{
    has_builtin_status_subresource, is_pod_delete_mark_patch, is_zero_grace_pod_delete_mark_patch,
    mark_terminating_pod_unready_at, pod_delete_mark_patch_without_status,
    preserve_status_subresource_on_main_update,
};

mod clock_boundary {
    use serde_json::Value;

    pub fn mark_terminating_pod_unready(data: &mut Value) {
        let now = crate::utils::k8s_timestamp();
        klights_types::mark_terminating_pod_unready_at(data, &now);
    }
}

#[deprecated(note = "pass an explicit timestamp to klights_types; removed in Phase 3.4")]
pub use clock_boundary::mark_terminating_pod_unready;
