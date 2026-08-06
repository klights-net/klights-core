//! Shared root-composition fixtures for the Raft integration target.

#[path = "harness.rs"]
mod harness;
pub(crate) use harness::IntegrationRaftComposition;
