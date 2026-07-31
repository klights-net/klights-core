//! Transport-neutral forwarding seam used by the embedded Raft node.
//!
//! Concrete gRPC transport lives in `grpc_network`; in-process test
//! transports belong to their consuming test module and are not exported by
//! the replication engine.

use async_trait::async_trait;

use crate::types::{NodeId, StorageCommandPayload};

/// Forward a write to the current Raft leader when the local node is a
/// follower.
#[async_trait]
pub trait LeaderForwarder: Send + Sync {
    async fn forward_propose(
        &self,
        leader_id: NodeId,
        payload: StorageCommandPayload,
    ) -> anyhow::Result<()>;
}
