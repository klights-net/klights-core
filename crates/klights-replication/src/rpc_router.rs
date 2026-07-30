//! Opaque leader-RPC dispatch into the embedded OpenRaft engine.

use async_trait::async_trait;
use openraft::Raft;

use crate::types::{NodeId, TypeConfig};

/// Dispatches opaque authenticated Raft envelopes into OpenRaft while
/// enforcing the receiver admission proof bound into current membership.
#[derive(Clone)]
pub struct RaftNodeRpcRouter {
    raft: Raft<TypeConfig>,
    storage_incarnation: String,
}

impl RaftNodeRpcRouter {
    pub fn new(raft: Raft<TypeConfig>, storage_incarnation: String) -> Self {
        Self {
            raft,
            storage_incarnation,
        }
    }

    fn validate_receiver_admission(
        &self,
        receiver: &klights_leader_rpc::raft_rpc::RaftReceiverAdmission,
    ) -> std::result::Result<(), klights_leader_rpc::raft_rpc::RaftRpcRouterError> {
        use klights_leader_rpc::raft_rpc::RaftRpcRouterError;
        if receiver.storage_incarnation != self.storage_incarnation {
            return Err(RaftRpcRouterError::Retryable(format!(
                "stale Raft receiver incarnation: membership admits {}, local node.db is {}",
                receiver.storage_incarnation, self.storage_incarnation
            )));
        }
        let Some(required) = receiver.admitted_log.as_ref() else {
            return Ok(());
        };
        let metrics = self.raft.metrics().borrow().clone();
        let local_index = [
            metrics.last_log_index,
            metrics.last_applied.as_ref().map(|log| log.index),
            metrics.snapshot.as_ref().map(|log| log.index),
            metrics.purged.as_ref().map(|log| log.index),
        ]
        .into_iter()
        .flatten()
        .max();
        if local_index.is_none_or(|index| index < required.index) {
            return Err(RaftRpcRouterError::Retryable(format!(
                "Raft receiver durable boundary is behind admitted index {}",
                required.index
            )));
        }
        let equal_anchor_mismatch = [
            metrics.last_applied.as_ref(),
            metrics.snapshot.as_ref(),
            metrics.purged.as_ref(),
        ]
        .into_iter()
        .flatten()
        .filter(|log| log.index == required.index)
        .any(|log| {
            log.leader_id.term != required.term || log.leader_id.node_id != required.leader_node_id
        });
        if equal_anchor_mismatch {
            return Err(RaftRpcRouterError::Retryable(
                "Raft receiver durable boundary identity differs from admitted LogId".to_string(),
            ));
        }
        Ok(())
    }
}

fn append_entries_starts_unanchored_nonzero_suffix(
    request: &openraft::raft::AppendEntriesRequest<TypeConfig>,
) -> bool {
    request.prev_log_id.is_none()
        && request
            .entries
            .first()
            .is_some_and(|entry| entry.log_id.index > 0)
}

#[async_trait]
impl klights_leader_rpc::raft_rpc::RaftRpcRouter for RaftNodeRpcRouter {
    async fn append_entries(
        &self,
        receiver: klights_leader_rpc::raft_rpc::RaftReceiverAdmission,
        payload: Vec<u8>,
    ) -> std::result::Result<Vec<u8>, klights_leader_rpc::raft_rpc::RaftRpcRouterError> {
        use klights_leader_rpc::raft_rpc::RaftRpcRouterError;
        self.validate_receiver_admission(&receiver)?;
        let request: openraft::raft::AppendEntriesRequest<TypeConfig> =
            serde_json::from_slice(&payload)
                .map_err(|error| RaftRpcRouterError::Dispatch(format!("decode AE: {error}")))?;
        if append_entries_starts_unanchored_nonzero_suffix(&request) {
            return Err(RaftRpcRouterError::Retryable(
                "AppendEntries starts an unanchored nonzero suffix; Raft member session reset required"
                    .to_string(),
            ));
        }
        let response = self.raft.append_entries(request).await.map_err(|error| {
            RaftRpcRouterError::RemoteFatal(format!("raft.append_entries: {error}"))
        })?;
        serde_json::to_vec(&response)
            .map_err(|error| RaftRpcRouterError::Dispatch(format!("encode AE resp: {error}")))
    }

    async fn vote(
        &self,
        receiver: klights_leader_rpc::raft_rpc::RaftReceiverAdmission,
        payload: Vec<u8>,
    ) -> std::result::Result<Vec<u8>, klights_leader_rpc::raft_rpc::RaftRpcRouterError> {
        use klights_leader_rpc::raft_rpc::RaftRpcRouterError;
        self.validate_receiver_admission(&receiver)?;
        let request: openraft::raft::VoteRequest<NodeId> = serde_json::from_slice(&payload)
            .map_err(|error| RaftRpcRouterError::Dispatch(format!("decode Vote: {error}")))?;
        let response = self
            .raft
            .vote(request)
            .await
            .map_err(|error| RaftRpcRouterError::RemoteFatal(format!("raft.vote: {error}")))?;
        serde_json::to_vec(&response)
            .map_err(|error| RaftRpcRouterError::Dispatch(format!("encode Vote resp: {error}")))
    }

    async fn install_snapshot(
        &self,
        receiver: klights_leader_rpc::raft_rpc::RaftReceiverAdmission,
        payload: Vec<u8>,
    ) -> std::result::Result<Vec<u8>, klights_leader_rpc::raft_rpc::RaftRpcRouterError> {
        use klights_leader_rpc::raft_rpc::RaftRpcRouterError;
        self.validate_receiver_admission(&receiver)?;
        let request: openraft::raft::InstallSnapshotRequest<TypeConfig> =
            serde_json::from_slice(&payload)
                .map_err(|error| RaftRpcRouterError::Dispatch(format!("decode IS: {error}")))?;
        let response = match self.raft.install_snapshot(request).await {
            Ok(response) => response,
            Err(openraft::error::RaftError::APIError(
                openraft::error::InstallSnapshotError::SnapshotMismatch(mismatch),
            )) => {
                let encoded = serde_json::to_string(&mismatch)
                    .unwrap_or_else(|error| format!("invalid:{error}"));
                return Err(RaftRpcRouterError::snapshot_mismatch(encoded));
            }
            Err(openraft::error::RaftError::Fatal(error)) => {
                return Err(RaftRpcRouterError::RemoteFatal(format!(
                    "raft.install_snapshot: {error}"
                )));
            }
        };
        serde_json::to_vec(&response)
            .map_err(|error| RaftRpcRouterError::Dispatch(format!("encode IS resp: {error}")))
    }
}
