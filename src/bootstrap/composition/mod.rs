//! Root-private construction bundles.
//!
//! This namespace contains concrete cross-crate assembly only. Feature crates
//! receive the focused capabilities produced here and cannot import or
//! downcast the construction bundles themselves.

pub(crate) mod authority;
pub(crate) mod authority_routed_leader;
pub(crate) mod cluster_store;
pub(crate) mod node_store;
pub(crate) mod pod_repository;
