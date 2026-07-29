//! Phase 3 Raft type configuration.
//!
//! Pins the openraft generic types used by klights:
//! - `NodeId = u64` — stable per-cluster identifier derived from node name
//!   on first registration; persisted in node-local meta alongside the
//!   human-readable node name. u64 (rather than `String`) keeps openraft's
//!   `Vote` and `LeaderId` types compact and hashable without serde
//!   round-trips inside the consensus core hot path.
//! - `Node = RaftMemberNode` — carries the API endpoint URL so peers can drive
//!   `RaftNetwork` without consulting an external membership directory.
//! - `D = StorageCommandPayload` — opaque bytes carrying a serialized
//!   `klights_cluster_core::StorageCommand` (protobuf), the unit of
//!   replication. The Raft state machine deserializes inside `apply`.
//! - `R = StorageCommandResult` — result of applying a command on the
//!   leader, returned to the proposer.
//!
//! The single-apply-path invariant from the Phase 3 plan dictates that
//! both manual promotion (`klights leader`) and openraft auto-election
//! route writes through `Raft::client_write`, which serializes them into
//! `StorageCommandPayload` and runs them through `RaftStateMachine::apply`.

use std::io::Cursor;

use openraft::TokioRuntime;
use openraft::declare_raft_types;
use openraft::impls::OneshotResponder;
use serde::{Deserialize, Serialize};

use super::super::Resource;

pub use klights_cluster_core::{NodeId, RaftShape, raft_node_id_for_node_name};

/// Receiver admission proof captured by each OpenRaft replication worker.
///
/// OpenRaft may retain a worker after a deterministic node ID is recreated.
/// Carrying the receiver's durable incarnation in membership makes the old
/// worker distinguishable from the replacement worker created by
/// remove/re-add. `admitted_log` additionally prevents a restored node.db
/// with the same UUID from silently rolling back below a proven boundary.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftMemberNode {
    pub addr: String,
    pub storage_incarnation: String,
    pub admitted_log: Option<RaftMemberLogId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftMemberLogId {
    pub term: u64,
    pub leader_node_id: NodeId,
    pub index: u64,
}

impl RaftMemberNode {
    pub fn new(
        addr: String,
        storage_incarnation: String,
        admitted_log: Option<RaftMemberLogId>,
    ) -> Self {
        Self {
            addr,
            storage_incarnation,
            admitted_log,
        }
    }

    #[cfg(test)]
    pub(crate) fn without_admission(addr: impl Into<String>) -> Self {
        Self::new(addr.into(), uuid::Uuid::nil().to_string(), None)
    }

    #[cfg(test)]
    pub fn unproven(addr: impl Into<String>) -> Self {
        Self::without_admission(addr)
    }
}

impl From<RaftMemberNode> for crate::replication::grpc::raft_rpc::RaftReceiverAdmission {
    fn from(value: RaftMemberNode) -> Self {
        Self {
            addr: value.addr,
            storage_incarnation: value.storage_incarnation,
            admitted_log: value.admitted_log.map(|log| {
                crate::replication::grpc::raft_rpc::RaftReceiverLogId {
                    term: log.term,
                    leader_node_id: log.leader_node_id,
                    index: log.index,
                }
            }),
        }
    }
}

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rejection_code: Option<klights_cluster_core::StorageCommandRejectionCode>,
    /// True only when this state-machine invocation newly committed a
    /// Kubernetes-visible resource change. This is deliberately independent
    /// of `applied_mutation`, which carries delete tombstones rather than a
    /// general-purpose change signal.
    #[serde(default, skip_serializing_if = "is_false")]
    pub public_resource_changed: bool,
    pub applied_mutation: Option<AppliedMutation>,
    /// Ephemeral local handoff from committed SQLite apply to the leader-side
    /// side-effect dispatcher. It is never serialized into a Raft response.
    #[serde(skip)]
    pub(crate) pod_endpoint_effect: klights_cluster_core::PodEndpointEffect,
}

const fn is_false(value: &bool) -> bool {
    !*value
}

declare_raft_types!(
    pub TypeConfig:
        D            = StorageCommandPayload,
        R            = StorageCommandResult,
        NodeId       = NodeId,
        Node         = RaftMemberNode,
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
        let mut nodes: BTreeMap<NodeId, RaftMemberNode> = BTreeMap::new();
        nodes.insert(
            1,
            RaftMemberNode::unproven("https://10.99.0.10:7679".to_string()),
        );
        nodes.insert(
            2,
            RaftMemberNode::unproven("https://10.99.0.13:7679".to_string()),
        );
        nodes.insert(
            3,
            RaftMemberNode::unproven("https://10.99.0.11:7679".to_string()),
        );
        let voters: std::collections::BTreeSet<NodeId> = nodes.keys().copied().collect();
        let m: Membership<NodeId, RaftMemberNode> = Membership::new(vec![voters], nodes);
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

    #[test]
    fn storage_command_result_decodes_legacy_payload_without_change_signal() {
        let decoded: StorageCommandResult = serde_json::from_value(serde_json::json!({
            "applied_rv": 7,
            "error_message": null,
            "applied_mutation": null
        }))
        .unwrap();

        assert!(!decoded.public_resource_changed);
        assert!(
            serde_json::to_value(StorageCommandResult::default())
                .unwrap()
                .get("public_resource_changed")
                .is_none(),
            "false change signals must keep the legacy serialized shape"
        );
    }
}
