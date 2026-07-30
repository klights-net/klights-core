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

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct RaftReceiverAdmission {
    pub addr: String,
    pub storage_incarnation: String,
    pub admitted_log: Option<RaftReceiverLogId>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct RaftReceiverLogId {
    pub term: u64,
    pub leader_node_id: u64,
    pub index: u64,
}

/// Errors returned by the router. The gRPC layer wraps these in
/// `Status::internal` (transport-level) or `RaftRpcRouterError::Disabled`
/// (router not installed → respond with the proto `error` arm so the
/// client side can translate to `RPCError::Unreachable`).
#[derive(Debug, thiserror::Error)]
pub enum RaftRpcRouterError {
    #[error("raft RPC router not installed on this server")]
    Disabled,
    #[error("raft RPC retryable: {0}")]
    Retryable(String),
    #[error("raft RPC remote fatal: {0}")]
    RemoteFatal(String),
    #[error("raft RPC snapshot mismatch: {0}")]
    SnapshotMismatch(String),
    #[error("raft RPC router dispatch: {0}")]
    Dispatch(String),
}

impl RaftRpcRouterError {
    pub fn snapshot_mismatch(encoded_error: String) -> Self {
        Self::SnapshotMismatch(encoded_error)
    }
}

/// Server-side dispatcher for Raft consensus RPCs. Implementations
/// deserialize the incoming bytes (serde JSON of the openraft RPC
/// payload), call the local `Raft<TypeConfig>` instance, and serialize
/// the response back into the wire envelope.
#[async_trait]
pub trait RaftRpcRouter: Send + Sync {
    async fn append_entries(
        &self,
        receiver: RaftReceiverAdmission,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, RaftRpcRouterError>;
    async fn vote(
        &self,
        receiver: RaftReceiverAdmission,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, RaftRpcRouterError>;
    async fn install_snapshot(
        &self,
        receiver: RaftReceiverAdmission,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, RaftRpcRouterError>;
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
        async fn append_entries(
            &self,
            _receiver: RaftReceiverAdmission,
            payload: Vec<u8>,
        ) -> Result<Vec<u8>, RaftRpcRouterError> {
            *self.ae_calls.lock().unwrap() += 1;
            Ok(payload)
        }
        async fn vote(
            &self,
            _receiver: RaftReceiverAdmission,
            payload: Vec<u8>,
        ) -> Result<Vec<u8>, RaftRpcRouterError> {
            *self.vote_calls.lock().unwrap() += 1;
            Ok(payload)
        }
        async fn install_snapshot(
            &self,
            _receiver: RaftReceiverAdmission,
            payload: Vec<u8>,
        ) -> Result<Vec<u8>, RaftRpcRouterError> {
            *self.snap_calls.lock().unwrap() += 1;
            Ok(payload)
        }
    }

    #[tokio::test]
    async fn router_dispatches_each_rpc_independently() {
        let router: Arc<dyn RaftRpcRouter> = Arc::new(CountingRouter::default());
        let receiver = RaftReceiverAdmission {
            addr: "loopback".to_string(),
            storage_incarnation: uuid::Uuid::nil().to_string(),
            admitted_log: None,
        };
        let out = router
            .append_entries(receiver.clone(), vec![1, 2, 3])
            .await
            .unwrap();
        assert_eq!(out, vec![1, 2, 3]);
        let out = router.vote(receiver.clone(), vec![4]).await.unwrap();
        assert_eq!(out, vec![4]);
        let out = router.install_snapshot(receiver, vec![5, 6]).await.unwrap();
        assert_eq!(out, vec![5, 6]);
    }
}
