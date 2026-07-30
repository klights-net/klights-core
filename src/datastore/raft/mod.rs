pub mod grpc_network;

pub mod network;
pub mod node;

#[cfg(test)]
pub(crate) fn test_unproven_member(
    addr: impl Into<String>,
) -> klights_replication::types::RaftMemberNode {
    klights_replication::types::RaftMemberNode::new(
        addr.into(),
        uuid::Uuid::nil().to_string(),
        None,
    )
}

#[cfg(test)]
mod log_storage_tests;
#[cfg(test)]
mod state_machine_tests;
