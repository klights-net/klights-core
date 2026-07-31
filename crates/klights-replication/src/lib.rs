//! Embedded OpenRaft replication engine.

pub mod activation;
pub mod authority;
pub mod committed_apply;
mod compressed;
mod fanout;
pub mod flow_control;
pub mod grpc_network;
pub mod join;
pub mod leader_api;
pub mod log_apply_wire;
pub mod log_storage;
pub mod materializer;
pub mod membership;
pub mod membership_client;
mod network;
pub mod node;
pub mod node_durability;
pub mod proposal;
pub mod rpc_router;
mod service;
pub mod snapshot;
pub mod state_machine;
pub mod types;

pub use network::LeaderForwarder;
pub use service::{FollowerMetrics, FollowerStatus, ReplicationService};

#[cfg(test)]
pub(crate) fn test_unproven_member(addr: impl Into<String>) -> types::RaftMemberNode {
    types::RaftMemberNode::new(addr.into(), uuid::Uuid::nil().to_string(), None)
}
