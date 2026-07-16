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

pub mod codec;

pub use codec::*;
