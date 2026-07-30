//! Embedded controller-coordination adapter.
//!
//! Controller owners consume only the backend-neutral
//! [`klights_leader_api::ControllerCoordination`] contract. Root constructs
//! this adapter from the selected cluster engine's authority capability.

pub mod lease_loop;
pub use lease_loop::run_under_lease;
