//! P3-11c production wiring for the Raft peer transport.
//!
//! Lives outside `datastore/` so it can reference `TaskSupervisor`
//! directly (forbidden inside the datastore module by
//! `scripts/check_supervisor_spawn.sh`). Implements
//! `datastore::raft::grpc_network::GrpcRaftClientFactory` against the
//! existing `ReplicationGrpcClient` so each Raft peer reuses the same
//! mTLS / token / connection-pool path as worker→leader joins.

use std::sync::Arc;

use async_trait::async_trait;

use crate::datastore::raft::grpc_network::{
    GrpcRaftClientFactory, GrpcRaftRpcClient, GrpcRaftRpcError, RaftPeerTransportError,
};
use crate::task_supervisor::TaskSupervisor;

/// Per-peer client wrapping a `ReplicationGrpcClient`. Translates the
/// envelope-bytes return of the three Raft RPCs into the typed
/// `GrpcRaftRpcError` the network layer expects.
pub struct ReplicationGrpcRaftRpcClient {
    inner: Arc<crate::replication::grpc::client::ReplicationGrpcClient>,
    /// P3/P0#3-fix1: the peer address this client targets (the address openraft
    /// membership passed to `client_for`), carried into the typed transport
    /// error so raft RPC failures log the exact peer (not a flattened generic
    /// "gRPC RaftAppendEntries failed").
    peer_addr: String,
}

impl ReplicationGrpcRaftRpcClient {
    pub fn new(
        inner: Arc<crate::replication::grpc::client::ReplicationGrpcClient>,
        peer_addr: String,
    ) -> Self {
        Self { inner, peer_addr }
    }
}

fn map_rpc_outcome(
    peer_addr: &str,
    outcome: anyhow::Result<std::result::Result<Vec<u8>, String>>,
) -> Result<Vec<u8>, GrpcRaftRpcError> {
    match outcome {
        Ok(Ok(bytes)) => Ok(bytes),
        Ok(Err(server_msg)) => {
            // Server-side router error (e.g. router not installed yet)
            // — surface as unreachable so openraft retries with backoff.
            Err(GrpcRaftRpcError::Server(server_msg))
        }
        Err(transport_err) => {
            // P0#3 fix #1: preserve the exact tonic status. Downcast to the
            // production client's `UnaryRpcError::Status` before flattening;
            // a plain `transport_err.to_string()` loses the code and message,
            // hiding auth/deadline/unavailable distinctions.
            use crate::replication::grpc::client::UnaryRpcError;
            let (tonic_code, tonic_message, detail) =
                if let Some(unary) = transport_err.downcast_ref::<UnaryRpcError>() {
                    match unary {
                        UnaryRpcError::Status(status) => (
                            Some(status.code()),
                            Some(status.message().to_string()),
                            status.to_string(),
                        ),
                        other => (None, None, other.to_string()),
                    }
                } else {
                    (None, None, transport_err.to_string())
                };
            Err(GrpcRaftRpcError::Unreachable(RaftPeerTransportError {
                peer_addr: peer_addr.to_string(),
                tonic_code,
                tonic_message,
                detail,
            }))
        }
    }
}

#[async_trait]
impl GrpcRaftRpcClient for ReplicationGrpcRaftRpcClient {
    async fn append_entries(&self, payload: Vec<u8>) -> Result<Vec<u8>, GrpcRaftRpcError> {
        map_rpc_outcome(
            self.peer_addr.as_str(),
            self.inner.raft_append_entries_rpc(payload).await,
        )
    }
    async fn vote(&self, payload: Vec<u8>) -> Result<Vec<u8>, GrpcRaftRpcError> {
        map_rpc_outcome(
            self.peer_addr.as_str(),
            self.inner.raft_vote_rpc(payload).await,
        )
    }
    async fn install_snapshot(&self, payload: Vec<u8>) -> Result<Vec<u8>, GrpcRaftRpcError> {
        map_rpc_outcome(
            self.peer_addr.as_str(),
            self.inner.raft_install_snapshot_rpc(payload).await,
        )
    }
}

/// Materials shared across every per-peer client this factory builds.
#[derive(Clone)]
pub struct ReplicationGrpcRaftClientTemplate {
    pub node_name: String,
    pub token: String,
    pub ca_cert_path: Option<std::path::PathBuf>,
    pub skip_ca: bool,
    pub client_cert_pem: Option<String>,
    pub client_key_pem: Option<String>,
    pub dataplane: crate::replication::grpc::client::JoinDataplaneMetadata,
    pub transport_policy: crate::replication::grpc::transport_policy::SharedGrpcTransportPolicy,
}

/// Mints a per-peer `ReplicationGrpcClient` on demand, keyed on the
/// peer address openraft passes in via `BasicNode.addr` when it calls
/// `RaftNetworkFactory::new_client`.
pub struct ReplicationGrpcRaftClientFactory {
    supervisor: Arc<TaskSupervisor>,
    template: ReplicationGrpcRaftClientTemplate,
}

impl ReplicationGrpcRaftClientFactory {
    pub fn new(
        supervisor: Arc<TaskSupervisor>,
        template: ReplicationGrpcRaftClientTemplate,
    ) -> Self {
        Self {
            supervisor,
            template,
        }
    }
}

impl GrpcRaftClientFactory for ReplicationGrpcRaftClientFactory {
    fn client_for(&self, addr: &str) -> Arc<dyn GrpcRaftRpcClient> {
        let config = crate::replication::grpc::client::GrpcClientConfig {
            leader_endpoint: addr.to_string(),
            token: self.template.token.clone(),
            node_name: self.template.node_name.clone(),
            role: crate::replication::protocol::JoinRole::Worker,
            dataplane: self.template.dataplane.clone(),
            ca_cert_path: self.template.ca_cert_path.clone(),
            skip_ca: self.template.skip_ca,
            client_cert_pem: self.template.client_cert_pem.clone(),
            client_key_pem: self.template.client_key_pem.clone(),
        };
        let client = Arc::new(
            crate::replication::grpc::client::ReplicationGrpcClient::new(
                config,
                self.supervisor.clone(),
                self.template.transport_policy.clone(),
            ),
        );
        Arc::new(ReplicationGrpcRaftRpcClient::new(client, addr.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datastore::raft::grpc_network::GrpcRaftRpcError;
    use crate::replication::grpc::client::UnaryRpcError;

    /// P0#3 fix #1: a tonic transport failure must reach `map_rpc_outcome` as a
    /// STRUCTURED `RaftPeerTransportError` carrying the peer address and the
    /// exact tonic code + message — not a flattened generic string that hides
    /// whether the peer rejected auth, timed out, or was unreachable.
    /// Previously the conversion did `transport_err.to_string()`, losing both.
    #[test]
    fn map_rpc_outcome_preserves_tonic_status_and_peer_addr() {
        let status = tonic::Status::unavailable("connection refused");
        let anyhow_err: anyhow::Error = anyhow::Error::new(UnaryRpcError::Status(status));
        let outcome: anyhow::Result<std::result::Result<Vec<u8>, String>> = Err(anyhow_err);

        let err = map_rpc_outcome("https://10.99.0.14:7679", outcome)
            .err()
            .expect("transport failure must map to a GrpcRaftRpcError");
        match err {
            GrpcRaftRpcError::Unreachable(te) => {
                assert_eq!(te.peer_addr, "https://10.99.0.14:7679");
                assert_eq!(te.tonic_code, Some(tonic::Code::Unavailable));
                assert!(
                    te.tonic_message
                        .as_deref()
                        .is_some_and(|m| m.contains("connection refused")),
                    "tonic message must be preserved, got: {:?}",
                    te.tonic_message
                );
            }
            other => panic!("expected Unreachable(RaftPeerTransportError), got {other:?}"),
        }
    }

    /// A non-tonic transport error (e.g. a connect failure that never produced
    /// a tonic::Status) still maps to Unreachable with the peer addr and no
    /// tonic code, rather than panicking.
    #[test]
    fn map_rpc_outcome_handles_non_tonic_transport_error() {
        let anyhow_err: anyhow::Error = anyhow::anyhow!("connect tcp: connection refused");
        let outcome: anyhow::Result<std::result::Result<Vec<u8>, String>> = Err(anyhow_err);
        let err = map_rpc_outcome("https://10.99.0.10:7679", outcome)
            .err()
            .expect("transport failure must map");
        match err {
            GrpcRaftRpcError::Unreachable(te) => {
                assert_eq!(te.peer_addr, "https://10.99.0.10:7679");
                assert_eq!(te.tonic_code, None);
                assert!(te.detail.contains("connect tcp"));
            }
            other => panic!("expected Unreachable, got {other:?}"),
        }
    }
}
