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
    GrpcRaftClientFactory, GrpcRaftRpcClient, GrpcRaftRpcError,
};
use crate::task_supervisor::TaskSupervisor;

/// Per-peer client wrapping a `ReplicationGrpcClient`. Translates the
/// envelope-bytes return of the three Raft RPCs into the typed
/// `GrpcRaftRpcError` the network layer expects.
pub struct ReplicationGrpcRaftRpcClient {
    inner: Arc<crate::replication::grpc::client::ReplicationGrpcClient>,
}

impl ReplicationGrpcRaftRpcClient {
    pub fn new(inner: Arc<crate::replication::grpc::client::ReplicationGrpcClient>) -> Self {
        Self { inner }
    }
}

fn map_rpc_outcome(
    outcome: anyhow::Result<std::result::Result<Vec<u8>, String>>,
) -> Result<Vec<u8>, GrpcRaftRpcError> {
    match outcome {
        Ok(Ok(bytes)) => Ok(bytes),
        Ok(Err(server_msg)) => {
            // Server-side router error (e.g. router not installed yet)
            // — surface as unreachable so openraft retries with backoff.
            Err(GrpcRaftRpcError::Server(server_msg))
        }
        Err(transport_err) => Err(GrpcRaftRpcError::Unreachable(transport_err.to_string())),
    }
}

#[async_trait]
impl GrpcRaftRpcClient for ReplicationGrpcRaftRpcClient {
    async fn append_entries(&self, payload: Vec<u8>) -> Result<Vec<u8>, GrpcRaftRpcError> {
        map_rpc_outcome(self.inner.raft_append_entries_rpc(payload).await)
    }
    async fn vote(&self, payload: Vec<u8>) -> Result<Vec<u8>, GrpcRaftRpcError> {
        map_rpc_outcome(self.inner.raft_vote_rpc(payload).await)
    }
    async fn install_snapshot(&self, payload: Vec<u8>) -> Result<Vec<u8>, GrpcRaftRpcError> {
        map_rpc_outcome(self.inner.raft_install_snapshot_rpc(payload).await)
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
        Arc::new(ReplicationGrpcRaftRpcClient::new(client))
    }
}
