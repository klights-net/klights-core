//! Base-repository-only assembly for full-stack integration tests.
//!
//! Focused fixture families remain available only behind
//! `integration-test-harness`; normal builds neither compile nor export them.

pub mod leader_rpc;
pub mod native_api;
pub mod node_delivery;
pub mod pod_repository;
pub mod raft;
