//! Phase 3 Raft type configuration.
//!
//! Pins the openraft generic types used by klights:
//! - `NodeId = u64` — stable per-cluster identifier derived from node name
//!   on first registration; persisted in node-local meta alongside the
//!   human-readable node name. u64 (rather than `String`) keeps openraft's
//!   `Vote` and `LeaderId` types compact and hashable without serde
//!   round-trips inside the consensus core hot path.
//! - `Node = BasicNode` — carries the API endpoint URL so peers can drive
//!   `RaftNetwork` without consulting an external membership directory.
//! - `D = StorageCommandPayload` — opaque bytes carrying a serialized
//!   `crate::datastore::command::StorageCommand` (protobuf), the unit of
//!   replication. The Raft state machine deserializes inside `apply`.
//! - `R = StorageCommandResult` — result of applying a command on the
//!   leader, returned to the proposer.
//!
//! The single-apply-path invariant from the Phase 3 plan dictates that
//! both manual promotion (`klights leader`) and openraft auto-election
//! route writes through `Raft::client_write`, which serializes them into
//! `StorageCommandPayload` and runs them through `RaftStateMachine::apply`.

use std::io::Cursor;

use openraft::BasicNode;
use openraft::TokioRuntime;
use openraft::declare_raft_types;
use openraft::impls::OneshotResponder;
use serde::{Deserialize, Serialize};

use crate::datastore::Resource;

pub use klights_cluster_core::{NodeId, RaftShape, raft_node_id_for_node_name};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageCommandPayload(pub Vec<u8>);

impl StorageCommandPayload {
    pub fn from_bytes(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AppliedMutation {
    Resource(Resource),
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StorageCommandResult {
    pub applied_rv: Option<i64>,
    pub error_message: Option<String>,
    pub applied_mutation: Option<AppliedMutation>,
}

declare_raft_types!(
    pub TypeConfig:
        D            = StorageCommandPayload,
        R            = StorageCommandResult,
        NodeId       = NodeId,
        Node         = BasicNode,
        Entry        = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = TokioRuntime,
        Responder    = OneshotResponder<TypeConfig>,
);

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::Membership;
    use std::collections::BTreeMap;

    #[test]
    fn membership_round_trips_three_voters() {
        let mut nodes: BTreeMap<NodeId, BasicNode> = BTreeMap::new();
        nodes.insert(
            1,
            BasicNode {
                addr: "https://10.99.0.10:7679".to_string(),
            },
        );
        nodes.insert(
            2,
            BasicNode {
                addr: "https://10.99.0.13:7679".to_string(),
            },
        );
        nodes.insert(
            3,
            BasicNode {
                addr: "https://10.99.0.11:7679".to_string(),
            },
        );
        let voters: std::collections::BTreeSet<NodeId> = nodes.keys().copied().collect();
        let m: Membership<NodeId, BasicNode> = Membership::new(vec![voters], nodes);
        assert_eq!(m.voter_ids().count(), 3);
    }

    #[test]
    fn storage_command_payload_round_trips() {
        let payload = StorageCommandPayload::from_bytes(vec![1, 2, 3, 4]);
        let encoded = serde_json::to_vec(&payload).unwrap();
        let decoded: StorageCommandPayload = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(payload, decoded);
        assert_eq!(decoded.as_slice(), &[1, 2, 3, 4]);
    }
}
