//! Transitional compatibility re-export for raft transport composition.
//!
//! The neutral estimator is owned by `klights-types`; this path remains only
//! until the Phase 12 raft move rewires the root adapter mechanically.

pub use klights_types::{RTT_DEFAULT_MS, RttEstimator};
