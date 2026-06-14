//! Phase 3 Raft network adapter.
//!
//! Step 4 ships a minimal stub network that satisfies the openraft
//! `RaftNetwork` + `RaftNetworkFactory` trait bounds so a single-voter
//! cluster (Step 5) can compile and run end-to-end. Single-voter Raft
//! never issues RPCs to peers, so every method returning `Unreachable`
//! is harmless there.
//!
//! Step 6 will replace this with two real implementations:
//! - `LoopbackRaftNetwork` — in-process for multi-voter unit tests
//! - `GrpcRaftNetwork` — wraps the existing `ReplicationGrpcClient`
//!   transport for production peer communication

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use openraft::BasicNode;
use openraft::Raft;
use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use crate::datastore::raft::types::{NodeId, StorageCommandPayload, TypeConfig};

/// Trait used by `RaftNode::propose` to forward a write to the current
/// Raft leader when this node is a follower. Implementations are
/// transport-specific: in-process unit tests use `LoopbackRegistry`;
/// production will use a gRPC client carrying the proposal over the
/// existing replication transport.
#[async_trait]
pub trait LeaderForwarder: Send + Sync {
    async fn forward_propose(
        &self,
        leader_id: NodeId,
        payload: StorageCommandPayload,
    ) -> anyhow::Result<()>;
}

/// Stub network — returns `Unreachable` for every RPC. Suitable for
/// single-voter clusters and as a placeholder during Phase 3 bring-up.
#[derive(Clone, Debug, Default)]
pub struct StubRaftNetwork;

impl RaftNetworkFactory<TypeConfig> for StubRaftNetwork {
    type Network = StubRaftNetwork;

    async fn new_client(&mut self, _target: NodeId, _node: &BasicNode) -> Self::Network {
        StubRaftNetwork
    }
}

fn unreachable(rpc: &str) -> Unreachable {
    Unreachable::new(&std::io::Error::new(
        std::io::ErrorKind::NotConnected,
        format!("StubRaftNetwork: {rpc} not implemented (Step 6 will wire real transport)"),
    ))
}

impl RaftNetwork<TypeConfig> for StubRaftNetwork {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        Err(RPCError::Unreachable(unreachable("append_entries")))
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        Err(RPCError::Unreachable(unreachable("install_snapshot")))
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        Err(RPCError::Unreachable(unreachable("vote")))
    }
}

// =========================================================================
// Loopback network — routes RPCs between in-process Raft instances.
// Suitable for multi-voter unit tests (Step 6) and as a Phase 3 fixture
// while the gRPC network is being built out.
// =========================================================================

#[derive(Clone, Default)]
pub struct LoopbackRegistry {
    inner: Arc<RwLock<HashMap<NodeId, Raft<TypeConfig>>>>,
}

impl LoopbackRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, node_id: NodeId, raft: Raft<TypeConfig>) {
        self.inner.write().unwrap().insert(node_id, raft);
    }

    fn lookup(&self, node_id: NodeId) -> Option<Raft<TypeConfig>> {
        self.inner.read().unwrap().get(&node_id).cloned()
    }
}

#[async_trait]
impl LeaderForwarder for LoopbackRegistry {
    async fn forward_propose(
        &self,
        leader_id: NodeId,
        payload: StorageCommandPayload,
    ) -> anyhow::Result<()> {
        let raft = self.lookup(leader_id).ok_or_else(|| {
            anyhow::anyhow!("loopback forward: leader {leader_id} not registered")
        })?;
        raft.client_write(payload)
            .await
            .map(|_| ())
            .map_err(|e| anyhow::anyhow!("loopback forward to {leader_id}: {e}"))
    }
}

// GrpcLeaderForwarder removed — T1. Non-leader voters now use the
// same log_apply sync path as replicas; there is no need for a
// separate no-op forwarder.

#[derive(Clone)]
pub struct LoopbackRaftNetworkFactory {
    registry: LoopbackRegistry,
}

impl LoopbackRaftNetworkFactory {
    pub fn new(registry: LoopbackRegistry) -> Self {
        Self { registry }
    }
}

impl RaftNetworkFactory<TypeConfig> for LoopbackRaftNetworkFactory {
    type Network = LoopbackRaftNetwork;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        LoopbackRaftNetwork {
            target,
            registry: self.registry.clone(),
        }
    }
}

pub struct LoopbackRaftNetwork {
    target: NodeId,
    registry: LoopbackRegistry,
}

impl RaftNetwork<TypeConfig> for LoopbackRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let raft = self.registry.lookup(self.target).ok_or_else(|| {
            RPCError::Unreachable(unreachable("append_entries: peer not registered"))
        })?;
        raft.append_entries(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let raft = self.registry.lookup(self.target).ok_or_else(|| {
            RPCError::Unreachable(unreachable("install_snapshot: peer not registered"))
        })?;
        raft.install_snapshot(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let raft = self
            .registry
            .lookup(self.target)
            .ok_or_else(|| RPCError::Unreachable(unreachable("vote: peer not registered")))?;
        raft.vote(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn factory_returns_stub() {
        let mut f = StubRaftNetwork;
        let _net = f.new_client(42u64, &BasicNode { addr: "x".into() }).await;
    }

    #[tokio::test]
    async fn vote_returns_unreachable() {
        let mut net = StubRaftNetwork;
        let rpc = VoteRequest::new(
            openraft::Vote::new(1, 10),
            Some(openraft::LogId::new(openraft::LeaderId::new(1, 10), 0)),
        );
        let err = net.vote(rpc, RPCOption::new(Duration::from_secs(1))).await;
        match err {
            Err(RPCError::Unreachable(_)) => {}
            other => panic!("expected Unreachable, got {other:?}"),
        }
    }
}
