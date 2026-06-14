//! Phase 3 production Raft network: carries openraft AppendEntries /
//! Vote / InstallSnapshot RPCs over the existing replication gRPC
//! transport (`src/replication/grpc/client/`).
//!
//! Server side: `src/replication/grpc/server.rs::raft_append_entries` /
//! `raft_vote` / `raft_install_snapshot` dispatch through the optional
//! `RaftRpcRouter` (`src/replication/grpc/raft_rpc.rs`).
//!
//! The wire envelope is intentionally opaque: each request carries
//! `bytes` (serde-encoded openraft RPC payload). Encoder/decoder live in
//! this module so the gRPC layer stays openraft-type-free.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use openraft::BasicNode;
use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use crate::datastore::raft::types::{NodeId, TypeConfig};

/// Per-peer reusable client surface. Production wires this to the
/// existing `ReplicationGrpcClient`; tests can swap in an in-process
/// impl that points directly at a `Replication` server bound to a
/// loopback port.
#[async_trait]
pub trait GrpcRaftRpcClient: Send + Sync {
    async fn append_entries(&self, payload: Vec<u8>) -> Result<Vec<u8>, GrpcRaftRpcError>;
    async fn vote(&self, payload: Vec<u8>) -> Result<Vec<u8>, GrpcRaftRpcError>;
    async fn install_snapshot(&self, payload: Vec<u8>) -> Result<Vec<u8>, GrpcRaftRpcError>;
}

/// Transport-level outcome of a peer RPC. `Unreachable` triggers
/// openraft retry/backoff; `Remote` propagates a consensus-layer error
/// to the caller; `Server` wraps server-side router errors (e.g.
/// "router not installed") that should be surfaced as unreachable so
/// the cluster heals once the peer finishes bootstrapping its router.
#[derive(Debug, thiserror::Error)]
pub enum GrpcRaftRpcError {
    #[error("peer unreachable: {0}")]
    Unreachable(String),
    #[error("peer returned consensus error: {0}")]
    Remote(String),
    #[error("peer raft router error: {0}")]
    Server(String),
}

/// Factory that materializes per-peer `GrpcRaftRpcClient` instances on
/// demand. Production passes a closure that opens a `ReplicationGrpcClient`
/// keyed by peer address; tests pass a closure that builds an
/// in-process loopback client.
pub trait GrpcRaftClientFactory: Send + Sync {
    fn client_for(&self, addr: &str) -> Arc<dyn GrpcRaftRpcClient>;
}

/// `GrpcRaftNetwork` implements both `RaftNetworkFactory` (openraft
/// asks the factory for a per-peer `Network` instance) and `RaftNetwork`
/// (the per-peer instance dispatches actual RPCs). Sharing the same
/// type keeps the openraft trait bounds happy and lets us cheaply clone
/// the factory across new_client calls.
#[derive(Clone)]
pub struct GrpcRaftNetwork {
    factory: Arc<dyn GrpcRaftClientFactory>,
    /// `target -> peer client`. Populated lazily by `new_client` and by
    /// `client_for_bound` rebuilds after an eviction.
    clients: Arc<RwLock<HashMap<NodeId, Arc<dyn GrpcRaftRpcClient>>>>,
    /// `target -> peer address`. Retained across client evictions so a
    /// dropped connection can be rebuilt from the same address without
    /// waiting for openraft to call `new_client` again.
    addrs: Arc<RwLock<HashMap<NodeId, String>>>,
    /// Bound target id when this struct is used as a `RaftNetwork`. The
    /// `RaftNetworkFactory::new_client` path clones the network and
    /// stamps the target id so subsequent RPC calls know which peer
    /// they're talking to.
    bound_target: Option<NodeId>,
}

impl GrpcRaftNetwork {
    pub fn new(factory: Arc<dyn GrpcRaftClientFactory>) -> Self {
        Self {
            factory,
            clients: Arc::new(RwLock::new(HashMap::new())),
            addrs: Arc::new(RwLock::new(HashMap::new())),
            bound_target: None,
        }
    }

    /// Return the cached client for the bound peer, rebuilding it from the
    /// retained peer address on a cache miss (e.g. after a transport-error
    /// eviction). This makes each RPC self-healing: a wedged connection is
    /// replaced by a fresh one on the very next call rather than staying
    /// dead until openraft happens to re-invoke `new_client`.
    fn client_for_bound(&self) -> Result<Arc<dyn GrpcRaftRpcClient>, Unreachable> {
        let target = self.bound_target.ok_or_else(|| {
            Unreachable::new(&std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "GrpcRaftNetwork RPC called on factory clone without bound target",
            ))
        })?;
        if let Some(client) = self.clients.read().unwrap().get(&target).cloned() {
            return Ok(client);
        }
        let addr = self
            .addrs
            .read()
            .unwrap()
            .get(&target)
            .cloned()
            .ok_or_else(|| {
                Unreachable::new(&std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    format!("GrpcRaftNetwork: no address registered for peer {target}"),
                ))
            })?;
        let client = self.factory.client_for(&addr);
        self.clients.write().unwrap().insert(target, client.clone());
        Ok(client)
    }

    /// Evict the cached client for a peer after a transport-level failure
    /// so the next RPC rebuilds a fresh connection. The peer address is
    /// retained for the rebuild.
    fn invalidate(&self, target: NodeId) {
        if self.clients.write().unwrap().remove(&target).is_some() {
            tracing::warn!(
                target,
                "GrpcRaftNetwork: evicting wedged peer client; will rebuild on next RPC"
            );
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for GrpcRaftNetwork {
    type Network = GrpcRaftNetwork;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        // Record the peer address so an evicted client can be rebuilt
        // without another new_client call, then materialize (or reuse)
        // the per-peer client and stamp the returned network with the
        // target id.
        self.addrs
            .write()
            .unwrap()
            .insert(target, node.addr.clone());
        let exists = self.clients.read().unwrap().contains_key(&target);
        if !exists {
            tracing::info!(
                target,
                addr = %node.addr,
                "GrpcRaftNetwork::new_client: creating peer client"
            );
            let client = self.factory.client_for(&node.addr);
            self.clients.write().unwrap().insert(target, client);
        }
        GrpcRaftNetwork {
            factory: self.factory.clone(),
            clients: self.clients.clone(),
            addrs: self.addrs.clone(),
            bound_target: Some(target),
        }
    }
}

fn unreachable_io(msg: impl Into<String>) -> Unreachable {
    let msg = msg.into();
    Unreachable::new(&std::io::Error::new(std::io::ErrorKind::NotConnected, msg))
}

impl RaftNetwork<TypeConfig> for GrpcRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let target = self.bound_target.unwrap_or(0);
        let client = self.client_for_bound().map_err(RPCError::Unreachable)?;
        let entries_len = rpc.entries.len();
        tracing::debug!(
            target,
            entries = entries_len,
            "GrpcRaftNetwork::append_entries: sending"
        );
        let payload = serde_json::to_vec(&rpc)
            .map_err(|e| RPCError::Unreachable(unreachable_io(format!("encode AE: {e}"))))?;
        let bytes = match client.append_entries(payload).await {
            Ok(b) => {
                tracing::debug!(target, "GrpcRaftNetwork::append_entries: success");
                b
            }
            Err(GrpcRaftRpcError::Unreachable(msg)) | Err(GrpcRaftRpcError::Server(msg)) => {
                tracing::warn!(target, %msg, "GrpcRaftNetwork::append_entries: unreachable/server error");
                self.invalidate(target);
                return Err(RPCError::Unreachable(unreachable_io(msg)));
            }
            Err(GrpcRaftRpcError::Remote(msg)) => {
                // Consensus-layer error encoded as a plain string at
                // the gRPC envelope level. Translate into a generic
                // RemoteError carrying the string in a RaftError shell.
                let raft_err: RaftError<NodeId> =
                    RaftError::Fatal(openraft::error::Fatal::Panicked);
                tracing::warn!(target, %msg, "raft RPC remote error (synthetic)");
                return Err(RPCError::RemoteError(RemoteError::new(target, raft_err)));
            }
        };
        serde_json::from_slice(&bytes)
            .map_err(|e| RPCError::Unreachable(unreachable_io(format!("decode AE: {e}"))))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let target = self.bound_target.unwrap_or(0);
        let client = self.client_for_bound().map_err(RPCError::Unreachable)?;
        let payload = serde_json::to_vec(&rpc)
            .map_err(|e| RPCError::Unreachable(unreachable_io(format!("encode Vote: {e}"))))?;
        let bytes = match client.vote(payload).await {
            Ok(b) => b,
            Err(GrpcRaftRpcError::Unreachable(msg)) | Err(GrpcRaftRpcError::Server(msg)) => {
                self.invalidate(target);
                return Err(RPCError::Unreachable(unreachable_io(msg)));
            }
            Err(GrpcRaftRpcError::Remote(msg)) => {
                let raft_err: RaftError<NodeId> =
                    RaftError::Fatal(openraft::error::Fatal::Panicked);
                tracing::warn!(target, %msg, "raft Vote RPC remote error (synthetic)");
                return Err(RPCError::RemoteError(RemoteError::new(target, raft_err)));
            }
        };
        serde_json::from_slice(&bytes)
            .map_err(|e| RPCError::Unreachable(unreachable_io(format!("decode Vote: {e}"))))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let target = self.bound_target.unwrap_or(0);
        let client = self.client_for_bound().map_err(RPCError::Unreachable)?;
        let payload = serde_json::to_vec(&rpc)
            .map_err(|e| RPCError::Unreachable(unreachable_io(format!("encode IS: {e}"))))?;
        let bytes = match client.install_snapshot(payload).await {
            Ok(b) => b,
            Err(GrpcRaftRpcError::Unreachable(msg)) | Err(GrpcRaftRpcError::Server(msg)) => {
                self.invalidate(target);
                return Err(RPCError::Unreachable(unreachable_io(msg)));
            }
            Err(GrpcRaftRpcError::Remote(msg)) => {
                let raft_err: RaftError<NodeId, InstallSnapshotError> =
                    RaftError::Fatal(openraft::error::Fatal::Panicked);
                tracing::warn!(target, %msg, "raft InstallSnapshot RPC remote error (synthetic)");
                return Err(RPCError::RemoteError(RemoteError::new(target, raft_err)));
            }
        };
        serde_json::from_slice(&bytes)
            .map_err(|e| RPCError::Unreachable(unreachable_io(format!("decode IS: {e}"))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// In-process loopback client that records each call and returns a
    /// canned reply. Lets the network round-trip be tested without
    /// spinning up a real tonic server.
    struct LoopbackClient {
        ae_log: Mutex<Vec<Vec<u8>>>,
        vote_log: Mutex<Vec<Vec<u8>>>,
        snap_log: Mutex<Vec<Vec<u8>>>,
        ae_reply: Vec<u8>,
        vote_reply: Vec<u8>,
        snap_reply: Vec<u8>,
    }

    #[async_trait]
    impl GrpcRaftRpcClient for LoopbackClient {
        async fn append_entries(&self, payload: Vec<u8>) -> Result<Vec<u8>, GrpcRaftRpcError> {
            self.ae_log.lock().unwrap().push(payload);
            Ok(self.ae_reply.clone())
        }
        async fn vote(&self, payload: Vec<u8>) -> Result<Vec<u8>, GrpcRaftRpcError> {
            self.vote_log.lock().unwrap().push(payload);
            Ok(self.vote_reply.clone())
        }
        async fn install_snapshot(&self, payload: Vec<u8>) -> Result<Vec<u8>, GrpcRaftRpcError> {
            self.snap_log.lock().unwrap().push(payload);
            Ok(self.snap_reply.clone())
        }
    }

    struct UnreachableClient;

    #[async_trait]
    impl GrpcRaftRpcClient for UnreachableClient {
        async fn append_entries(&self, _payload: Vec<u8>) -> Result<Vec<u8>, GrpcRaftRpcError> {
            Err(GrpcRaftRpcError::Unreachable("peer down".into()))
        }
        async fn vote(&self, _payload: Vec<u8>) -> Result<Vec<u8>, GrpcRaftRpcError> {
            Err(GrpcRaftRpcError::Unreachable("peer down".into()))
        }
        async fn install_snapshot(&self, _payload: Vec<u8>) -> Result<Vec<u8>, GrpcRaftRpcError> {
            Err(GrpcRaftRpcError::Unreachable("peer down".into()))
        }
    }

    struct FixedFactory {
        client: Arc<dyn GrpcRaftRpcClient>,
    }

    impl GrpcRaftClientFactory for FixedFactory {
        fn client_for(&self, _addr: &str) -> Arc<dyn GrpcRaftRpcClient> {
            self.client.clone()
        }
    }

    fn sample_vote_request() -> VoteRequest<NodeId> {
        VoteRequest::new(
            openraft::Vote::new(2, 10),
            Some(openraft::LogId::new(openraft::LeaderId::new(2, 10), 5)),
        )
    }

    fn sample_vote_response() -> Vec<u8> {
        let resp = openraft::raft::VoteResponse {
            vote: openraft::Vote::new(2, 20),
            vote_granted: true,
            last_log_id: Some(openraft::LogId::new(openraft::LeaderId::new(2, 10), 5)),
        };
        serde_json::to_vec(&resp).unwrap()
    }

    fn sample_ae_request() -> AppendEntriesRequest<TypeConfig> {
        AppendEntriesRequest {
            vote: openraft::Vote::new(2, 10),
            prev_log_id: None,
            entries: Vec::new(),
            leader_commit: None,
        }
    }

    fn sample_ae_response() -> Vec<u8> {
        serde_json::to_vec(&AppendEntriesResponse::<NodeId>::Success).unwrap()
    }

    /// Factory whose `client_for` hands out `fail_first` on the very first
    /// build and `then` on every subsequent build, counting calls. Lets a
    /// test prove that a transport error evicts the cached client and the
    /// next RPC rebuilds a fresh one from the stored peer address.
    struct SeqFactory {
        calls: Arc<std::sync::atomic::AtomicUsize>,
        fail_first: Arc<dyn GrpcRaftRpcClient>,
        then: Arc<dyn GrpcRaftRpcClient>,
    }

    impl GrpcRaftClientFactory for SeqFactory {
        fn client_for(&self, _addr: &str) -> Arc<dyn GrpcRaftRpcClient> {
            let n = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n == 0 {
                self.fail_first.clone()
            } else {
                self.then.clone()
            }
        }
    }

    /// A wedged peer connection (transport error) must evict the cached
    /// client so the next RPC rebuilds a fresh connection instead of
    /// reusing the dead one forever. Without eviction a follower whose
    /// raft link drops under packet loss would stop applying entries
    /// permanently — the root cause behind the follower-served watch hang.
    #[tokio::test]
    async fn transport_error_invalidates_cached_client_and_next_rpc_rebuilds() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = Arc::new(AtomicUsize::new(0));
        let working = Arc::new(LoopbackClient {
            ae_log: Mutex::new(Vec::new()),
            vote_log: Mutex::new(Vec::new()),
            snap_log: Mutex::new(Vec::new()),
            ae_reply: sample_ae_response(),
            vote_reply: Vec::new(),
            snap_reply: Vec::new(),
        });
        let factory = Arc::new(SeqFactory {
            calls: calls.clone(),
            fail_first: Arc::new(UnreachableClient),
            then: working.clone(),
        });
        let mut network = GrpcRaftNetwork::new(factory);
        let mut peer = network
            .new_client(
                20u64,
                &BasicNode {
                    addr: "down".into(),
                },
            )
            .await;

        // First RPC uses the initially-built (wedged) client → Unreachable.
        let err = peer
            .append_entries(
                sample_ae_request(),
                RPCOption::new(std::time::Duration::from_secs(1)),
            )
            .await
            .expect_err("wedged peer must error");
        assert!(matches!(err, RPCError::Unreachable(_)));

        // The dead client must have been evicted from the per-peer cache.
        assert!(
            peer.clients.read().unwrap().get(&20u64).is_none(),
            "transport error must invalidate the cached peer client"
        );

        // Next RPC rebuilds a fresh client from the stored address and
        // succeeds — the peer self-heals without openraft re-calling
        // new_client.
        peer.append_entries(
            sample_ae_request(),
            RPCOption::new(std::time::Duration::from_secs(1)),
        )
        .await
        .expect("peer recovers after rebuilding the client");

        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "factory must rebuild the client after invalidation"
        );
        assert_eq!(working.ae_log.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn vote_rpc_round_trips_through_grpc_envelope() {
        let loopback = Arc::new(LoopbackClient {
            ae_log: Mutex::new(Vec::new()),
            vote_log: Mutex::new(Vec::new()),
            snap_log: Mutex::new(Vec::new()),
            ae_reply: Vec::new(),
            vote_reply: sample_vote_response(),
            snap_reply: Vec::new(),
        });
        let factory = Arc::new(FixedFactory {
            client: loopback.clone(),
        });
        let mut network = GrpcRaftNetwork::new(factory);
        let mut peer = network
            .new_client(20u64, &BasicNode { addr: "x".into() })
            .await;
        let response = peer
            .vote(
                sample_vote_request(),
                RPCOption::new(std::time::Duration::from_secs(1)),
            )
            .await
            .expect("vote round-trip");
        assert!(response.vote_granted);
        assert_eq!(response.vote.leader_id().get_term(), 2);
        let log = loopback.vote_log.lock().unwrap();
        assert_eq!(
            log.len(),
            1,
            "client wrote one request through the envelope"
        );
    }

    #[tokio::test]
    async fn unreachable_peer_surfaces_as_rpc_error_unreachable() {
        let factory = Arc::new(FixedFactory {
            client: Arc::new(UnreachableClient),
        });
        let mut network = GrpcRaftNetwork::new(factory);
        let mut peer = network
            .new_client(
                20u64,
                &BasicNode {
                    addr: "down".into(),
                },
            )
            .await;
        let err = peer
            .vote(
                sample_vote_request(),
                RPCOption::new(std::time::Duration::from_secs(1)),
            )
            .await
            .expect_err("unreachable peer must error");
        match err {
            RPCError::Unreachable(_) => {}
            other => panic!("expected Unreachable, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn new_client_caches_peer_clients_across_calls() {
        let loopback = Arc::new(LoopbackClient {
            ae_log: Mutex::new(Vec::new()),
            vote_log: Mutex::new(Vec::new()),
            snap_log: Mutex::new(Vec::new()),
            ae_reply: Vec::new(),
            vote_reply: sample_vote_response(),
            snap_reply: Vec::new(),
        });
        let factory = Arc::new(FixedFactory {
            client: loopback.clone(),
        });
        let mut network = GrpcRaftNetwork::new(factory);
        let _peer1 = network
            .new_client(20u64, &BasicNode { addr: "x".into() })
            .await;
        let _peer1_again = network
            .new_client(20u64, &BasicNode { addr: "x".into() })
            .await;
        // Both new_client calls for the same target reuse one entry.
        assert_eq!(network.clients.read().unwrap().len(), 1);
        let _peer2 = network
            .new_client(30u64, &BasicNode { addr: "y".into() })
            .await;
        assert_eq!(network.clients.read().unwrap().len(), 2);
    }
}
