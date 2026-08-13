//! P12.1f closed every capability formerly owned by this module: pure Pod
//! value/query/persistence/status/network/delete/actor fakes moved to their
//! canonical crates in P12.1a-e, and every remaining "true root Pod
//! assembly" case (`PodRepositoryScenarioOwner` and its derived
//! `Integration*Fixture` views, the worker/store/network scenario fixtures,
//! and every `run_*` orchestration function) moved to the private
//! `#[cfg(test)]` composition tests under
//! `src/bootstrap/pod_repository_composition/{assembly_support,
//! construction_tests, deletion_tests, network_tests, scheduling_tests,
//! status_tests, store_watch_tests, worker_tests}.rs`, following the
//! established `src/bootstrap/pod_repository_composition/workqueue_tests.rs`
//! precedent. Root Pod-repository composition testing behavior now runs via
//! `cargo test -p klights` rather than through this feature.
//!
//! This file intentionally stays as an empty stub: P12.1g deletes it along
//! with the `pod-repository-test-support` feature and its `lib.rs` export.
