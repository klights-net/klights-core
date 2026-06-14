//! Server-side router for the Phase 3 Raft consensus RPCs.
//!
//! The gRPC layer receives `RaftAppendEntries` / `RaftVote` /
//! `RaftInstallSnapshot` envelopes carrying opaque serde-encoded
//! openraft RPC payloads. It hands them to a `RaftRpcRouter` that
//! deserializes, dispatches to the local `Raft<TypeConfig>` instance,
//! and serializes the response.
//!
//! The router is provided by the leader bootstrap (P3-11c) so that the
//! existing `Replication` gRPC service can stay agnostic of openraft
//! types: it only ever sees `Vec<u8>` envelopes.

use async_trait::async_trait;

/// Errors returned by the router. The gRPC layer wraps these in
/// `Status::internal` (transport-level) or `RaftRpcRouterError::Disabled`
/// (router not installed → respond with the proto `error` arm so the
/// client side can translate to `RPCError::Unreachable`).
#[derive(Debug, thiserror::Error)]
pub enum RaftRpcRouterError {
    #[error("raft RPC router not installed on this server")]
    Disabled,
    #[error("raft RPC router dispatch: {0}")]
    Dispatch(String),
}

/// Server-side dispatcher for Raft consensus RPCs. Implementations
/// deserialize the incoming bytes (serde JSON of the openraft RPC
/// payload), call the local `Raft<TypeConfig>` instance, and serialize
/// the response back into the wire envelope.
#[async_trait]
pub trait RaftRpcRouter: Send + Sync {
    async fn append_entries(&self, payload: Vec<u8>) -> Result<Vec<u8>, RaftRpcRouterError>;
    async fn vote(&self, payload: Vec<u8>) -> Result<Vec<u8>, RaftRpcRouterError>;
    async fn install_snapshot(&self, payload: Vec<u8>) -> Result<Vec<u8>, RaftRpcRouterError>;
}

/// Outcome of a `JoinAsControlplane` RPC. The server-side handler in
/// `server.rs` translates this into the protobuf `oneof result`.
#[derive(Debug, Clone)]
pub enum ControlplaneJoinOutcome {
    /// This node is the current Raft leader and successfully ran
    /// `add_voter` (or `add_learner_only` when `admitted_as_learner=true`).
    /// `voter_count_after` is the voter membership size after the
    /// change — unchanged for learner joins (learners don't affect
    /// quorum).
    Accepted {
        voter_count_after: u32,
        /// T1.5.x: true when the leader admitted this node as a raft
        /// learner. False for voter admissions.
        admitted_as_learner: bool,
        /// Cluster CA cert PEM (plaintext). Written to etc/ca.crt by the joiner.
        ca_cert_pem: String,
        /// Cluster CA key PEM, AES-256-GCM encrypted with the join token.
        encrypted_ca_key: Vec<u8>,
        /// AES-GCM nonce (12 bytes).
        ca_key_nonce: [u8; 12],
    },
    /// This node is a follower; the joiner should retry against the
    /// supplied leader id/addr.
    RedirectToLeader { leader_id: u64, leader_addr: String },
    /// No leader is currently elected (membership change in flight,
    /// freshly-bootstrapped cluster waiting for first election, etc.).
    /// The joiner retries with exponential backoff.
    Denied { reason: String },
}

#[async_trait]
pub trait ControlplaneJoinHandler: Send + Sync {
    /// T1.5.x: `as_learner=true` dispatches to `RaftNode::add_learner_only`
    /// (no follow-up `change_membership` — the node stays a learner).
    /// `as_learner=false` keeps the existing voter join path
    /// (`RaftNode::add_voter`).
    async fn join(
        &self,
        node_id: u64,
        addr: String,
        node_name: String,
        as_learner: bool,
        node_internal_ip: Option<String>,
    ) -> Result<ControlplaneJoinOutcome, RaftRpcRouterError>;

    /// Whether `node_name` is a current raft member (voter or learner) of this
    /// cluster, as seen by the local raft node's committed membership.
    ///
    /// This is the authoritative "is this an existing control-plane node?"
    /// signal. Because voter/learner admission is gated on a valid controlplane
    /// bootstrap token (see `JoinAsControlplane`), membership is a trustworthy
    /// proxy for "was authorized as a control plane" — a worker, which can never
    /// obtain a controlplane token, is never a member. The node-cert (cert
    /// renewal) path of `SignControlplaneCsr` uses this so a worker presenting
    /// only its `system:node:` client cert cannot have a CA-trusted server
    /// certificate minted for it.
    async fn is_controlplane_member(&self, node_name: &str) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::Mutex;

    #[derive(Default)]
    struct CountingRouter {
        ae_calls: Mutex<usize>,
        vote_calls: Mutex<usize>,
        snap_calls: Mutex<usize>,
    }

    #[async_trait]
    impl RaftRpcRouter for CountingRouter {
        async fn append_entries(&self, payload: Vec<u8>) -> Result<Vec<u8>, RaftRpcRouterError> {
            *self.ae_calls.lock().unwrap() += 1;
            Ok(payload)
        }
        async fn vote(&self, payload: Vec<u8>) -> Result<Vec<u8>, RaftRpcRouterError> {
            *self.vote_calls.lock().unwrap() += 1;
            Ok(payload)
        }
        async fn install_snapshot(&self, payload: Vec<u8>) -> Result<Vec<u8>, RaftRpcRouterError> {
            *self.snap_calls.lock().unwrap() += 1;
            Ok(payload)
        }
    }

    #[tokio::test]
    async fn router_dispatches_each_rpc_independently() {
        let router: Arc<dyn RaftRpcRouter> = Arc::new(CountingRouter::default());
        let out = router.append_entries(vec![1, 2, 3]).await.unwrap();
        assert_eq!(out, vec![1, 2, 3]);
        let out = router.vote(vec![4]).await.unwrap();
        assert_eq!(out, vec![4]);
        let out = router.install_snapshot(vec![5, 6]).await.unwrap();
        assert_eq!(out, vec![5, 6]);
    }
}
