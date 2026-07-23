//! Compatibility facade and internal-wire codec for cluster storage commands.
//!
//! The canonical domain values are owned by `klights-cluster-core`. This root
//! facade is the one temporary compatibility adapter allowed by packet 5.1 so
//! existing composition-crate consumers retain their source paths while later
//! Phase 5 packets extract adjacent semantics. Private generated-wire encoding
//! is owned separately by the root `storage_wire_codec` module.
//!
//! REMOVE(Phase 5.5): migrate remaining root consumers directly to
//! `klights_cluster_core::command`.

pub use klights_cluster_core::command::*;
