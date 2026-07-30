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
use klights_supervisor::TaskSupervisor;

/// Per-peer client wrapping a `ReplicationGrpcClient`. Translates the
/// envelope-bytes return of the three Raft RPCs into the typed
/// `GrpcRaftRpcError` the network layer expects.
pub struct ReplicationGrpcRaftRpcClient {
    inner: Arc<klights_leader_rpc::client::ReplicationGrpcClient>,
    /// P3/P0#3-fix1: the peer address this client targets (the address openraft
    /// membership passed to `client_for`), carried into the typed transport
    /// error so raft RPC failures log the exact peer (not a flattened generic
    /// "gRPC RaftAppendEntries failed").
    peer_addr: String,
}

impl ReplicationGrpcRaftRpcClient {
    pub fn new(
        inner: Arc<klights_leader_rpc::client::ReplicationGrpcClient>,
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
            if server_msg.starts_with("raft RPC retryable:") {
                Err(GrpcRaftRpcError::Retryable(server_msg))
            } else if server_msg.starts_with("raft RPC remote fatal:") {
                Err(GrpcRaftRpcError::Remote(server_msg))
            } else if let Some(encoded) = server_msg.strip_prefix("raft RPC snapshot mismatch: ") {
                match serde_json::from_str(encoded) {
                    Ok(mismatch) => Err(GrpcRaftRpcError::SnapshotMismatch(mismatch)),
                    Err(error) => Err(GrpcRaftRpcError::Server(format!(
                        "decode remote snapshot mismatch: {error}"
                    ))),
                }
            } else {
                // Disabled/uninstalled routers and legacy Dispatch strings are
                // retryable server-admission failures. Only the explicit
                // RemoteFatal variant above receives terminal classification.
                Err(GrpcRaftRpcError::Server(server_msg))
            }
        }
        Err(transport_err) => {
            // P0#3 fix #1: preserve the exact tonic status. Downcast to the
            // production client's `UnaryRpcError::Status` before flattening;
            // a plain `transport_err.to_string()` loses the code and message,
            // hiding auth/deadline/unavailable distinctions.
            use klights_leader_rpc::client::UnaryRpcError;
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
    async fn append_entries(
        &self,
        receiver: crate::datastore::raft::types::RaftMemberNode,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, GrpcRaftRpcError> {
        let receiver = klights_leader_rpc::raft_rpc::RaftReceiverAdmission {
            addr: receiver.addr,
            storage_incarnation: receiver.storage_incarnation,
            admitted_log: receiver.admitted_log.map(|log| {
                klights_leader_rpc::raft_rpc::RaftReceiverLogId {
                    term: log.term,
                    leader_node_id: log.leader_node_id,
                    index: log.index,
                }
            }),
        };
        map_rpc_outcome(
            self.peer_addr.as_str(),
            self.inner.raft_append_entries_rpc(receiver, payload).await,
        )
    }
    async fn vote(
        &self,
        receiver: crate::datastore::raft::types::RaftMemberNode,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, GrpcRaftRpcError> {
        let receiver = klights_leader_rpc::raft_rpc::RaftReceiverAdmission {
            addr: receiver.addr,
            storage_incarnation: receiver.storage_incarnation,
            admitted_log: receiver.admitted_log.map(|log| {
                klights_leader_rpc::raft_rpc::RaftReceiverLogId {
                    term: log.term,
                    leader_node_id: log.leader_node_id,
                    index: log.index,
                }
            }),
        };
        map_rpc_outcome(
            self.peer_addr.as_str(),
            self.inner.raft_vote_rpc(receiver, payload).await,
        )
    }
    async fn install_snapshot(
        &self,
        receiver: crate::datastore::raft::types::RaftMemberNode,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, GrpcRaftRpcError> {
        let receiver = klights_leader_rpc::raft_rpc::RaftReceiverAdmission {
            addr: receiver.addr,
            storage_incarnation: receiver.storage_incarnation,
            admitted_log: receiver.admitted_log.map(|log| {
                klights_leader_rpc::raft_rpc::RaftReceiverLogId {
                    term: log.term,
                    leader_node_id: log.leader_node_id,
                    index: log.index,
                }
            }),
        };
        map_rpc_outcome(
            self.peer_addr.as_str(),
            self.inner
                .raft_install_snapshot_rpc(receiver, payload)
                .await,
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
    pub dataplane: klights_leader_rpc::client::JoinDataplaneMetadata,
    pub transport_policy: klights_leader_rpc::transport_policy::SharedGrpcTransportPolicy,
}

/// Mints a per-peer `ReplicationGrpcClient` on demand, keyed on the
/// peer address openraft passes in via `RaftMemberNode.addr` when it calls
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
        let config = klights_leader_rpc::client::GrpcClientConfig {
            leader_endpoint: addr.to_string(),
            token: self.template.token.clone(),
            node_name: self.template.node_name.clone(),
            role: klights_leader_api::JoinRole::Worker,
            dataplane: self.template.dataplane.clone(),
            ca_cert_path: self.template.ca_cert_path.clone(),
            skip_ca: self.template.skip_ca,
            client_cert_pem: self.template.client_cert_pem.clone(),
            client_key_pem: self.template.client_key_pem.clone(),
        };
        let client = Arc::new(klights_leader_rpc::client::ReplicationGrpcClient::new(
            config,
            self.supervisor.clone(),
            self.template.transport_policy.clone(),
        ));
        Arc::new(ReplicationGrpcRaftRpcClient::new(client, addr.to_string()))
    }
}

/// Metadata probe for Raft-member capability checks. It deliberately reuses
/// the exact mTLS/CA/dataplane transport template used by Raft RPC clients.
pub struct ReplicationGrpcMemberFeatureProbe {
    supervisor: Arc<TaskSupervisor>,
    template: ReplicationGrpcRaftClientTemplate,
    local_node_id: crate::datastore::raft::types::NodeId,
}

impl ReplicationGrpcMemberFeatureProbe {
    pub fn new(
        supervisor: Arc<TaskSupervisor>,
        template: ReplicationGrpcRaftClientTemplate,
    ) -> Self {
        let local_node_id =
            crate::datastore::raft::types::raft_node_id_for_node_name(&template.node_name);
        Self {
            supervisor,
            template,
            local_node_id,
        }
    }

    fn local_metadata_for_member(
        local_node_id: crate::datastore::raft::types::NodeId,
        node_id: crate::datastore::raft::types::NodeId,
    ) -> Option<klights_leader_api::MetadataResponse> {
        (node_id == local_node_id).then(|| klights_leader_api::MetadataResponse {
            cluster_id: String::new(),
            leader_epoch: 0,
            current_rv: 0,
            current_log_index: 0,
            command_codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
        })
    }
}

#[async_trait::async_trait]
impl crate::datastore::raft::node::MemberFeatureProbe for ReplicationGrpcMemberFeatureProbe {
    async fn metadata_for_member(
        &self,
        node_id: crate::datastore::raft::types::NodeId,
        addr: &str,
    ) -> anyhow::Result<klights_leader_api::MetadataResponse> {
        if let Some(metadata) = Self::local_metadata_for_member(self.local_node_id, node_id) {
            return Ok(metadata);
        }
        let client = klights_leader_rpc::client::ReplicationGrpcClient::new(
            klights_leader_rpc::client::GrpcClientConfig {
                leader_endpoint: addr.to_string(),
                token: self.template.token.clone(),
                node_name: self.template.node_name.clone(),
                role: klights_leader_api::JoinRole::Worker,
                dataplane: self.template.dataplane.clone(),
                ca_cert_path: self.template.ca_cert_path.clone(),
                skip_ca: self.template.skip_ca,
                client_cert_pem: self.template.client_cert_pem.clone(),
                client_key_pem: self.template.client_key_pem.clone(),
            },
            self.supervisor.clone(),
            self.template.transport_policy.clone(),
        );
        client.metadata().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datastore::raft::grpc_network::GrpcRaftRpcError;
    use klights_leader_rpc::client::UnaryRpcError;

    #[test]
    fn local_member_feature_probe_shortcuts_to_local_capabilities() {
        let local = crate::datastore::raft::types::raft_node_id_for_node_name("cp-1");
        let metadata = ReplicationGrpcMemberFeatureProbe::local_metadata_for_member(local, local)
            .expect("the local member must not require a self gRPC connection");
        assert_eq!(
            metadata.command_codec_version,
            klights_cluster_core::COMMAND_CODEC_VERSION
        );
        assert!(
            ReplicationGrpcMemberFeatureProbe::local_metadata_for_member(local, local + 1)
                .is_none()
        );
    }

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

    #[test]
    fn map_rpc_outcome_treats_remote_raft_fatal_as_terminal() {
        let outcome = Ok(Err(
            "raft RPC remote fatal: raft.install_snapshot: storage fatal".to_string(),
        ));
        assert!(matches!(
            map_rpc_outcome("https://10.99.0.14:7679", outcome),
            Err(GrpcRaftRpcError::Remote(message)) if message.contains("storage fatal")
        ));
    }

    #[test]
    fn map_rpc_outcome_preserves_snapshot_mismatch_structure() {
        let mismatch = openraft::error::SnapshotMismatch {
            expect: ("snapshot-a", 0).into(),
            got: ("snapshot-a", 512).into(),
        };
        let outcome = Ok(Err(
            klights_leader_rpc::raft_rpc::RaftRpcRouterError::snapshot_mismatch(
                serde_json::to_string(&mismatch).unwrap(),
            )
            .to_string(),
        ));
        assert!(matches!(
            map_rpc_outcome("https://10.99.0.14:7679", outcome),
            Err(GrpcRaftRpcError::SnapshotMismatch(actual)) if actual == mismatch
        ));
    }
}
