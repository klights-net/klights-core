//! Compatibility facade and internal-wire codec for cluster storage commands.
//!
//! The canonical domain values are owned by `klights-cluster-core`. This root
//! facade is the one temporary compatibility adapter allowed by packet 5.1 so
//! existing composition-crate consumers retain their source paths while later
//! Phase 5 packets extract adjacent semantics.
//!
//! REMOVE(Phase 5.5): migrate remaining root consumers to
//! `klights_cluster_core::command` and move the private generated-wire codec to
//! the replication adapter package.

pub use klights_cluster_core::command::*;

/// Generate the UUID-v4 idempotency key for a newly authored command.
///
/// Entropy belongs to this root adapter rather than the canonical value-only
/// domain. Decoding continues to accept the persisted/wire string unchanged.
pub(crate) fn new_command_id() -> CommandId {
    CommandId(uuid::Uuid::new_v4().to_string())
}

pub mod codec;

pub use codec::*;

#[cfg(test)]
mod tests {
    use super::new_command_id;

    #[test]
    fn root_adapter_generates_distinct_uuid_v4_command_ids() {
        let first = new_command_id();
        let second = new_command_id();
        assert_ne!(first, second);
        for command_id in [first, second] {
            let parsed = uuid::Uuid::parse_str(&command_id.0).expect("command ID must be a UUID");
            assert_eq!(parsed.get_version(), Some(uuid::Version::Random));
        }
    }
}
