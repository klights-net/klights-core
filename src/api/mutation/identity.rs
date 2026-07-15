//! Transitional compatibility paths for the canonical resource identity value.

#[deprecated(note = "use klights_types::ResourceKey directly; removed in Phase 3.4")]
pub type ResourceIdentity = klights_types::ResourceKey;
