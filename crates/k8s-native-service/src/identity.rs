//! API-owned identity generation capability.

/// Supplies Kubernetes object names and UIDs at API semantic authoring points.
///
/// Entropy policy and implementation stay in the root composition crate. This
/// narrow object-safe port lets native API orchestration remain deterministic
/// in focused tests without owning an entropy crate or process-global state.
pub trait ApiIdentityGenerator: Send + Sync {
    fn generate_name(&self, prefix: &str) -> String;
    fn new_uid(&self) -> String;
}
