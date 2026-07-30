use std::sync::Arc;

use anyhow::Context;
use klights_leader_api::{
    ControlplaneJoinError, ControlplaneJoinHandler, ControlplaneJoinMetadata,
    ControlplaneJoinMetadataFuture, ControlplaneJoinRegistration,
    ControlplaneJoinRegistrationFuture, ControlplaneJoinRequest, RemoteNodeMode,
};

use crate::datastore::DatastoreHandle;
use klights_replication::node::RaftNode;
use klights_replication::types::RaftShape;

struct ClusterControlplaneJoinRegistration {
    db: DatastoreHandle,
}

impl ControlplaneJoinRegistration for ClusterControlplaneJoinRegistration {
    fn register<'a>(
        &'a self,
        request: &'a ControlplaneJoinRequest,
        voter_count_after: u32,
    ) -> ControlplaneJoinRegistrationFuture<'a> {
        Box::pin(async move {
            self.register_inner(request, voter_count_after)
                .await
                .map_err(|error| ControlplaneJoinError::new(error.to_string()))
        })
    }
}

impl ClusterControlplaneJoinRegistration {
    async fn register_inner(
        &self,
        request: &ControlplaneJoinRequest,
        voter_count_after: u32,
    ) -> anyhow::Result<()> {
        use crate::kubelet::node::{
            NodeRegistrationAddresses, NodeRegistrationHostFacts, NodeRegistrationSnapshot,
        };

        let joiner_ip = request
            .addr
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .split(':')
            .next()
            .unwrap_or("")
            .to_string();
        let joiner_grpc_port = request
            .addr
            .rsplit(':')
            .next()
            .and_then(|port| port.parse::<u16>().ok());
        let node_role = crate::kubelet::node_config::KubeletNodeRole::Controlplane {
            as_learner: request.as_learner,
        };
        let joiner_shape = RaftShape {
            voter_count: voter_count_after,
            is_leader: false,
            is_learner: request.as_learner,
        };
        let role_projection =
            crate::authority_adapter::project_raft_shape(&node_role, &joiner_shape);
        let registration_addresses = NodeRegistrationAddresses::new(
            request
                .node_internal_ip
                .clone()
                .unwrap_or_else(|| joiner_ip.clone()),
            Some(joiner_ip),
        );
        let (node_mode, host) = match request.node_registration.as_ref() {
            Some(registration) => {
                let node_mode = match registration.node_mode {
                    RemoteNodeMode::Root => klights_types::NodePeerMode::Root,
                    RemoteNodeMode::Rootless => klights_types::NodePeerMode::Rootless,
                };
                let host = NodeRegistrationHostFacts {
                    cpu_count: registration.host.cpu_count,
                    memory_ki: registration.host.memory_ki,
                    architecture: registration.host.architecture.clone(),
                    operating_system: registration.host.operating_system.clone(),
                    os_image: registration.host.os_image.clone(),
                    kernel_version: registration.host.kernel_version.clone(),
                    container_runtime_version: registration.host.container_runtime_version.clone(),
                    kubelet_version: registration.host.kubelet_version.clone(),
                    git_commit: registration.host.git_commit.clone(),
                };
                (node_mode, host)
            }
            None => {
                let existing = self
                    .db
                    .get_resource("v1", "Node", None, &request.node_name)
                    .await?
                    .with_context(|| {
                        format!(
                            "legacy JoinAsControlplane rejoin for {} has no persisted Node registration snapshot",
                            request.node_name
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
                    request.legacy_node_git_commit.as_deref(),
                )?;
                (node_mode, host)
            }
        };
        let snapshot = NodeRegistrationSnapshot {
            node_name: request.node_name.clone(),
            node_mode,
            node_role,
            publish_external_ip: true,
            addresses: registration_addresses,
            role_projection: Some(role_projection),
            grpc_port: joiner_grpc_port,
            host,
        };

        let pending_dataplane =
            klights_networking::dataplane_health::DataplaneHealth::new_healthy();
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
}

struct ClusterControlplaneJoinMetadata {
    node: Arc<RaftNode>,
    db: DatastoreHandle,
    mutex: tokio::sync::Mutex<()>,
}

impl ControlplaneJoinMetadata for ClusterControlplaneJoinMetadata {
    fn refresh<'a>(
        &'a self,
        node_name: &'a str,
        as_learner: bool,
    ) -> ControlplaneJoinMetadataFuture<'a> {
        Box::pin(async move {
            self.refresh_inner(node_name, as_learner)
                .await
                .map_err(|error| ControlplaneJoinError::new(error.to_string()))
        })
    }
}

impl ClusterControlplaneJoinMetadata {
    async fn refresh_inner(&self, node_name: &str, as_learner: bool) -> anyhow::Result<()> {
        let _guard = self.mutex.lock().await;
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
            node_name,
            as_learner,
            self.node.authoring_node(),
        );
        crate::bootstrap::cluster_meta::write_cluster_membership(self.db.as_ref(), &membership)
            .await
            .with_context(|| {
                format!("failed to refresh cluster membership metadata after admitting {node_name}")
            })
    }
}

pub(crate) fn build_controlplane_join_handler(
    node: Arc<RaftNode>,
    db: DatastoreHandle,
) -> Arc<dyn ControlplaneJoinHandler> {
    let membership = node.membership();
    Arc::new(klights_replication::join::ControlplaneJoinCoordinator::new(
        Arc::new(klights_replication::join::RaftControlplaneJoinAuthority::new(membership.clone())),
        Arc::new(
            klights_replication::join::RaftControlplaneJoinAdmission::new(
                membership.clone(),
                crate::bootstrap::node_role::controlplane_limit(),
            ),
        ),
        Arc::new(klights_replication::join::RaftControlplaneMemberQuery::new(
            membership,
        )),
        Arc::new(ClusterControlplaneJoinRegistration { db: db.clone() }),
        Arc::new(ClusterControlplaneJoinMetadata {
            node,
            db,
            mutex: tokio::sync::Mutex::new(()),
        }),
    ))
}
