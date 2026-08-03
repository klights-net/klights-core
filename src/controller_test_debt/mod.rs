//! Transitional registration for root-owned real-adapter controller tests.
//!
//! Phase 18 P2d removes the production `controllers` module. These suites
//! still exercise root datastore and runtime adapters, so P2g owns their
//! migration to the base integration surface. Keeping the registrations
//! explicit and test-only preserves coverage without resurrecting a root
//! production controller owner.

mod deployment;
mod replicaset;
mod replicationcontroller;
