use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;

use crate::datastore::raft::node::{RaftMemberAdmissionResult, RaftNode};
use crate::datastore::raft::types::{RaftShape, raft_node_id_for_node_name};
use crate::replication::grpc::raft_rpc::{
    ControlplaneJoinHandler, ControlplaneJoinOutcome, ControlplaneJoinRequest, RaftRpcRouterError,
    RemoteNodeMode, RemoteNodeRegistrationSnapshot,
};

/// Bootstrap-owned adapter that coordinates authenticated control-plane joins
/// across Raft admission, Kubernetes Node registration, and cluster metadata.
pub struct RaftNodeJoinHandler {
    node: Arc<RaftNode>,
    pub(crate) db: crate::datastore::DatastoreHandle,
    membership_metadata_mutex: tokio::sync::Mutex<()>,
}

impl RaftNodeJoinHandler {
    pub fn new(node: Arc<RaftNode>, db: crate::datastore::DatastoreHandle) -> Self {
        Self {
            node,
            db,
            membership_metadata_mutex: tokio::sync::Mutex::new(()),
        }
    }

    /// Register a joining control-plane Node through the leader-owned cluster
    /// datastore so the Raft proposer replicates the row to every voter.
    async fn register_voter_node(
        &self,
        node_name: &str,
        addr: &str,
        as_learner: bool,
        node_internal_ip: Option<String>,
        node_registration: Option<RemoteNodeRegistrationSnapshot>,
        legacy_node_git_commit: Option<String>,
    ) -> anyhow::Result<()> {
        use crate::kubelet::node::{
            NodeRegistrationAddresses, NodeRegistrationHostFacts, NodeRegistrationSnapshot,
        };

        let joiner_ip = addr
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .split(':')
            .next()
            .unwrap_or("")
            .to_string();
        let joiner_grpc_port = addr
            .rsplit(':')
            .next()
            .and_then(|port| port.parse::<u16>().ok());
        let node_role = crate::kubelet::node_config::KubeletNodeRole::Controlplane { as_learner };
        let leader_shape = self.node.current_shape();
        let joiner_shape = RaftShape {
            voter_count: leader_shape.voter_count,
            is_leader: false,
            is_learner: as_learner,
        };
        let registration_addresses = NodeRegistrationAddresses::new(
            node_internal_ip.unwrap_or_else(|| joiner_ip.clone()),
            Some(joiner_ip),
        );
        let (node_mode, host) = match node_registration {
            Some(registration) => {
                let node_mode = match registration.node_mode {
                    RemoteNodeMode::Root => klights_types::NodePeerMode::Root,
                    RemoteNodeMode::Rootless => klights_types::NodePeerMode::Rootless,
                };
                let host = NodeRegistrationHostFacts {
                    cpu_count: registration.host.cpu_count,
                    memory_ki: registration.host.memory_ki,
                    architecture: registration.host.architecture,
                    operating_system: registration.host.operating_system,
                    os_image: registration.host.os_image,
                    kernel_version: registration.host.kernel_version,
                    container_runtime_version: registration.host.container_runtime_version,
                    kubelet_version: registration.host.kubelet_version,
                    git_commit: registration.host.git_commit,
                };
                (node_mode, host)
            }
            None => {
                let existing = self
                    .db
                    .get_resource("v1", "Node", None, node_name)
                    .await?
                    .with_context(|| {
                        format!(
                            "legacy JoinAsControlplane rejoin for {node_name} has no persisted Node registration snapshot"
                        )
                    })?;
                let node_mode = klights_types::parse_node_peer_mode(
                    existing
                        .data
                        .pointer("/metadata/annotations/klights.io~1mode")
                        .and_then(serde_json::Value::as_str),
                )?;
                let host = NodeRegistrationHostFacts::from_existing_node(
                    &existing.data,
                    legacy_node_git_commit.as_deref(),
                )?;
                (node_mode, host)
            }
        };
        let snapshot = NodeRegistrationSnapshot {
            node_name: node_name.to_string(),
            node_mode,
            node_role,
            publish_external_ip: true,
            addresses: registration_addresses,
            raft_shape: Some(joiner_shape),
            grpc_port: joiner_grpc_port,
            host,
        };

        // A remote node owns its kubelet and dataplane status. Keep it
        // unavailable until it has reconciled peers and publishes status.
        let pending_dataplane = crate::networking::dataplane_health::DataplaneHealth::new_healthy();
        pending_dataplane.set_peers_pending();
        let pending_dataplane = pending_dataplane.snapshot();
        crate::bootstrap::node_registration_adapter::register_node_snapshot(
            self.db.as_ref(),
            None,
            Some(&pending_dataplane),
            &snapshot,
        )
        .await
    }

    async fn refresh_cluster_membership_metadata(
        &self,
        admitted_node_name: &str,
        as_learner: bool,
    ) -> anyhow::Result<()> {
        let _guard = self.membership_metadata_mutex.lock().await;
        let membership = match crate::bootstrap::cluster_meta::read_cluster_membership(
            self.db.as_ref(),
        )
        .await
        {
            Ok(membership) => membership,
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "JoinAsControlplane: cluster membership metadata unavailable; skipping voter metadata refresh"
                );
                return Ok(());
            }
        };
        let latest = match crate::bootstrap::cluster_meta::read_cluster_membership(self.db.as_ref())
            .await
        {
            Ok(latest) => Some(latest),
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "JoinAsControlplane: latest cluster membership metadata unavailable; refreshing from initial snapshot"
                );
                None
            }
        };
        let membership = klights_cluster_core::merge_controlplane_join_membership_metadata(
            membership,
            latest.as_ref(),
            admitted_node_name,
            as_learner,
            self.node.authoring_node(),
        );
        crate::bootstrap::cluster_meta::write_cluster_membership(self.db.as_ref(), &membership)
            .await
            .with_context(|| {
                format!(
                    "failed to refresh cluster membership metadata after admitting {admitted_node_name}"
                )
            })
    }
}

pub(crate) fn validate_command_codec_v3_join(
    command_codec_version: u32,
) -> std::result::Result<(), String> {
    if command_codec_version != crate::log_apply::COMMAND_CODEC_VERSION {
        return Err(
            "joining voters and learners must advertise exact command codec version 3".to_string(),
        );
    }
    Ok(())
}

#[async_trait]
impl ControlplaneJoinHandler for RaftNodeJoinHandler {
    async fn join(
        &self,
        request: ControlplaneJoinRequest,
    ) -> std::result::Result<ControlplaneJoinOutcome, RaftRpcRouterError> {
        let ControlplaneJoinRequest {
            node_id,
            addr,
            node_name,
            as_learner,
            storage_incarnation,
            storage_log_attestation,
            command_codec_version,
            node_internal_ip,
            node_registration,
            legacy_node_git_commit,
        } = request;
        if !self.node.is_leader() {
            return Ok(match self.node.current_leader_info() {
                Some((leader_id, leader_addr)) => ControlplaneJoinOutcome::RedirectToLeader {
                    leader_id,
                    leader_addr,
                },
                None => ControlplaneJoinOutcome::Denied {
                    reason: "no leader currently elected; retry later".into(),
                },
            });
        }
        if let Err(reason) = validate_command_codec_v3_join(command_codec_version) {
            return Ok(ControlplaneJoinOutcome::Denied { reason });
        }
        tracing::info!(
            joining_node_id = node_id,
            joining_node_name = %node_name,
            joining_addr = %addr,
            storage_incarnation = %storage_incarnation,
            as_learner,
            "JoinAsControlplane: leader admitting durable Raft storage incarnation"
        );
        let admission = self
            .node
            .admit_controlplane_member_with_limit(
                node_id,
                addr.clone(),
                as_learner,
                storage_incarnation,
                storage_log_attestation,
                crate::bootstrap::node_role::controlplane_limit(),
            )
            .await
            .map_err(|error| {
                RaftRpcRouterError::Dispatch(format!(
                    "admit control-plane member {node_id}: {error}"
                ))
            })?;
        if admission == RaftMemberAdmissionResult::Unchanged {
            return Ok(ControlplaneJoinOutcome::Accepted {
                voter_count_after: self.node.current_shape().voter_count,
                admitted_as_learner: as_learner,
                ca_cert_pem: String::new(),
                encrypted_ca_key: Vec::new(),
                ca_key_nonce: [0u8; 12],
            });
        }
        if let Err(error) = self
            .register_voter_node(
                &node_name,
                &addr,
                as_learner,
                node_internal_ip,
                node_registration,
                legacy_node_git_commit,
            )
            .await
        {
            return Err(RaftRpcRouterError::Dispatch(format!(
                "register joining Node row for {node_name}: {error}"
            )));
        }
        let voter_count_after = self.node.current_shape().voter_count;
        self.refresh_cluster_membership_metadata(&node_name, as_learner)
            .await
            .map_err(|error| {
                RaftRpcRouterError::Dispatch(format!(
                    "refresh cluster membership metadata: {error}"
                ))
            })?;
        Ok(ControlplaneJoinOutcome::Accepted {
            voter_count_after,
            admitted_as_learner: as_learner,
            ca_cert_pem: String::new(),
            encrypted_ca_key: Vec::new(),
            ca_key_nonce: [0u8; 12],
        })
    }

    async fn is_controlplane_member(&self, node_name: &str) -> bool {
        let target = raft_node_id_for_node_name(node_name);
        self.node
            .raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .nodes()
            .any(|(id, _)| *id == target)
    }
}
