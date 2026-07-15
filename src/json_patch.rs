//! Transitional compatibility path for canonical RFC 7396 merge-patch policy.

mod compatibility {
    use serde_json::Value;

    pub fn apply_merge_patch(target: &mut Value, patch: &Value) -> anyhow::Result<()> {
        klights_types::apply_merge_patch(target, patch);
        Ok(())
    }
}

#[deprecated(note = "use klights_types::apply_merge_patch; removed in Phase 3.4")]
pub use compatibility::apply_merge_patch;
