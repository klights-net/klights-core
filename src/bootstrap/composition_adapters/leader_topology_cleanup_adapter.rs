//! Composition-owned leader capabilities for node topology and Pod cleanup.
//!
//! These ports are the canonical local implementations used by bootstrap and
//! the sequenced compatibility shell.  Bootstrap deliberately does not
//! implement these capability traits.

use std::sync::Arc;

use klights_cluster_core::LogApplyPodCleanupIntentRow as StoredPodCleanupIntent;
use klights_cluster_core::command::StorageCommand;
use klights_leader_api::{
    LeaderNetworkTopologyCommand, LeaderNetworkTopologyQuery, LeaderNodeSubnetAllocation,
    LeaderPodCleanupIntents, NetworkDataplane, NetworkTopologyError, NetworkTopologyFuture,
    NodeDataplaneQuery, NodeDataplaneResult, NodeSubnetAllocationError, NodeSubnetAllocationFuture,
    NodeSubnetAllocationRequest, NodeSubnetAllocationResult, NodeSubnetQuery, NodeSubnetResult,
    PeerSubnetsQuery, PeerSubnetsResult, PodCleanupIntent, PodCleanupIntentAckRequest,
    PodCleanupIntentError, PodCleanupIntentFuture, PodCleanupIntentListRequest,
};

use crate::bootstrap::authority::AuthorityHandle;
use crate::bootstrap::leader_conversions::topology::{
    focused_dataplane, focused_node_subnet, node_subnet_allocation_is_exhausted,
};
use crate::datastore::Resource;
use klights_replication::proposal::RaftProposal;

pub(crate) struct ClusterStoreLeaderNetwork {
    topology_reads: Arc<dyn klights_cluster_store::ClusterTopologyRead>,
    proposal: Arc<dyn RaftProposal>,
    authority: AuthorityHandle,
}

impl ClusterStoreLeaderNetwork {
    pub(crate) fn new<A: Into<AuthorityHandle>>(
        topology_reads: Arc<dyn klights_cluster_store::ClusterTopologyRead>,
        proposal: Arc<dyn RaftProposal>,
        authority: A,
    ) -> Self {
        Self {
            topology_reads,
            proposal,
            authority: authority.into(),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test<A: Into<AuthorityHandle>>(
        topology_reads: Arc<dyn klights_cluster_store::ClusterTopologyRead>,
        proposal: Arc<dyn RaftProposal>,
        authority: A,
    ) -> Self {
        Self::new(topology_reads, proposal, authority)
    }

    fn require_leader(&self) -> Result<(), NetworkTopologyError> {
        self.authority
            .local_permit()
            .map(|_| ())
            .map_err(|_| NetworkTopologyError::NotLeader)
    }
}

impl LeaderNodeSubnetAllocation for ClusterStoreLeaderNetwork {
    fn allocate_node_subnet(
        &self,
        request: NodeSubnetAllocationRequest,
    ) -> NodeSubnetAllocationFuture<'_, NodeSubnetAllocationResult> {
        Box::pin(async move {
            self.require_leader()
                .map_err(|_| NodeSubnetAllocationError::NotLeader)?;
            let (node_name, cluster_cidr, node_ip) = request.into_parts();
            self.proposal
                .propose_command(StorageCommand::AllocateNodeSubnet {
                    node_name: node_name.clone(),
                    subnet: cluster_cidr.clone(),
                    node_ip: node_ip.to_string(),
                })
                .await
                .map_err(|error| {
                    let message = error.to_string();
                    if node_subnet_allocation_is_exhausted(&message) {
                        NodeSubnetAllocationError::exhausted(cluster_cidr.clone())
                    } else if message.to_ascii_lowercase().contains("conflict") {
                        NodeSubnetAllocationError::conflict(message)
                    } else {
                        NodeSubnetAllocationError::allocation_failed(message)
                    }
                })?;
            let subnet = self
                .topology_reads
                .get_node_subnet(
                    klights_cluster_store::NodeTopologyRequest::try_new(&node_name).map_err(
                        |error| NodeSubnetAllocationError::allocation_failed(error.to_string()),
                    )?,
                )
                .await
                .map_err(|error| NodeSubnetAllocationError::allocation_failed(error.to_string()))?
                .map(focused_node_subnet)
                .transpose()
                .map_err(|error| NodeSubnetAllocationError::corrupt_response(error.to_string()))?;
            NodeSubnetAllocationResult::try_from_wire(&node_name, subnet)
        })
    }
}

impl LeaderNetworkTopologyQuery for ClusterStoreLeaderNetwork {
    fn get_node_subnet(
        &self,
        request: NodeSubnetQuery,
    ) -> NetworkTopologyFuture<'_, NodeSubnetResult> {
        Box::pin(async move {
            self.require_leader()?;
            let node_name = request.into_node_name();
            let subnet = self
                .topology_reads
                .get_node_subnet(
                    klights_cluster_store::NodeTopologyRequest::try_new(&node_name)
                        .map_err(|error| NetworkTopologyError::query_failed(error.to_string()))?,
                )
                .await
                .map_err(|error| NetworkTopologyError::query_failed(error.to_string()))?
                .map(focused_node_subnet)
                .transpose()?;
            NodeSubnetResult::try_from_wire(&node_name, subnet.is_some(), subnet)
        })
    }

    fn list_peer_subnets(
        &self,
        request: PeerSubnetsQuery,
    ) -> NetworkTopologyFuture<'_, PeerSubnetsResult> {
        Box::pin(async move {
            self.require_leader()?;
            let node_name = request.into_node_name();
            let subnets = self
                .topology_reads
                .list_peer_subnets(
                    klights_cluster_store::PeerTopologyRequest::excluding(&node_name)
                        .map_err(|error| NetworkTopologyError::query_failed(error.to_string()))?,
                )
                .await
                .map_err(|error| NetworkTopologyError::query_failed(error.to_string()))?
                .into_iter()
                .map(focused_node_subnet)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            PeerSubnetsResult::try_new(&node_name, subnets)
        })
    }

    fn get_node_dataplane(
        &self,
        request: NodeDataplaneQuery,
    ) -> NetworkTopologyFuture<'_, NodeDataplaneResult> {
        Box::pin(async move {
            self.require_leader()?;
            let node_name = request.into_node_name();
            let metadata = self
                .topology_reads
                .get_node_dataplane(
                    klights_cluster_store::NodeTopologyRequest::try_new(&node_name)
                        .map_err(|error| NetworkTopologyError::query_failed(error.to_string()))?,
                )
                .await
                .map_err(|error| NetworkTopologyError::query_failed(error.to_string()))?
                .map(focused_dataplane)
                .transpose()?;
            NodeDataplaneResult::try_from_wire(&node_name, metadata.is_some(), metadata)
        })
    }
}

impl LeaderNetworkTopologyCommand for ClusterStoreLeaderNetwork {
    fn register_node_dataplane(&self, metadata: NetworkDataplane) -> NetworkTopologyFuture<'_, ()> {
        Box::pin(async move {
            self.require_leader()?;
            let metadata =
                crate::bootstrap::leader_conversions::topology::legacy_dataplane(metadata)?;
            self.proposal
                .propose_command(StorageCommand::UpdateNodeDataplane {
                    node_name: metadata.node_name,
                    mode: metadata.mode.as_str().to_string(),
                    encryption: metadata.encryption.as_str().to_string(),
                    public_key: metadata.public_key.as_ref().map(ToString::to_string),
                    endpoint: metadata.endpoint.to_string(),
                    port: metadata.port,
                })
                .await
                .map_err(|error| NetworkTopologyError::query_failed(error.to_string()))?;
            Ok(())
        })
    }
}

pub(crate) fn focused_pod_cleanup_intent(
    intent: StoredPodCleanupIntent,
) -> std::result::Result<PodCleanupIntent, PodCleanupIntentError> {
    let snapshot = Resource::try_from_data(Arc::new(intent.pod_data)).map_err(|error| {
        PodCleanupIntentError::corrupt_intent(format!(
            "cleanup intent Pod snapshot has invalid identity: {error}"
        ))
    })?;
    PodCleanupIntent::try_new(
        intent.node_name,
        intent.namespace,
        intent.pod_name,
        intent.pod_uid,
        intent.reason,
        intent.resource_version,
        intent.created_at_ms,
        snapshot,
    )
}

pub(crate) struct ClusterStoreLeaderPodCleanup {
    pod_cleanup: Arc<dyn klights_cluster_store::ClusterPodCleanupStore>,
    proposal: Arc<dyn RaftProposal>,
    authority: AuthorityHandle,
}

impl ClusterStoreLeaderPodCleanup {
    pub(crate) fn new<A: Into<AuthorityHandle>>(
        pod_cleanup: Arc<dyn klights_cluster_store::ClusterPodCleanupStore>,
        proposal: Arc<dyn RaftProposal>,
        authority: A,
    ) -> Self {
        Self {
            pod_cleanup,
            proposal,
            authority: authority.into(),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test<A: Into<AuthorityHandle>>(
        pod_cleanup: Arc<dyn klights_cluster_store::ClusterPodCleanupStore>,
        proposal: Arc<dyn RaftProposal>,
        authority: A,
    ) -> Self {
        Self::new(pod_cleanup, proposal, authority)
    }

    fn require_leader(&self) -> Result<(), PodCleanupIntentError> {
        self.authority
            .local_permit()
            .map(|_| ())
            .map_err(|_| PodCleanupIntentError::NotLeader)
    }
}

impl LeaderPodCleanupIntents for ClusterStoreLeaderPodCleanup {
    fn list_pod_cleanup_intents(
        &self,
        request: PodCleanupIntentListRequest,
    ) -> PodCleanupIntentFuture<'_, Vec<PodCleanupIntent>> {
        Box::pin(async move {
            self.require_leader()?;
            self.pod_cleanup
                .list_pod_cleanup_intents_for_node(request.node_name())
                .await
                .map_err(|error| PodCleanupIntentError::unavailable(error.to_string()))?
                .into_iter()
                .map(focused_pod_cleanup_intent)
                .collect()
        })
    }

    fn acknowledge_pod_cleanup_intent(
        &self,
        request: PodCleanupIntentAckRequest,
    ) -> PodCleanupIntentFuture<'_, ()> {
        Box::pin(async move {
            self.require_leader()?;
            let (node_name, namespace, pod_name, pod_uid, reason) = request.into_parts();
            self.proposal
                .propose_command(StorageCommand::DeletePodCleanupIntent {
                    node_name,
                    namespace,
                    pod_name,
                    pod_uid,
                    reason,
                })
                .await
                .map_err(|error| PodCleanupIntentError::unavailable(error.to_string()))?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use klights_cluster_core::{OutboxApplyError, OutboxApplyOutcome, OutboxStreamWatermark};
    use klights_cluster_store::StorageCommandResult;
    use klights_leader_api::{DataplaneEncryption, NetworkNodeMode};
    use std::sync::Mutex;

    struct RecordingApplyingProposal {
        inner: crate::bootstrap::outbox_apply_adapter::BackendProposalFixture,
        commands: Mutex<Vec<StorageCommand>>,
    }

    #[async_trait]
    impl RaftProposal for RecordingApplyingProposal {
        async fn propose_command(
            &self,
            command: StorageCommand,
        ) -> anyhow::Result<StorageCommandResult> {
            self.commands.lock().unwrap().push(command.clone());
            self.inner.propose_command(command).await
        }

        async fn propose_outbox_command(
            &self,
            _idempotency_key: &str,
            _operation: &str,
            _command: StorageCommand,
            _authoring_node: &str,
            _watermark: Option<OutboxStreamWatermark>,
        ) -> Result<OutboxApplyOutcome, OutboxApplyError> {
            unreachable!("network topology does not submit outbox commands")
        }
    }

    fn authority(is_leader: bool) -> Arc<dyn klights_leader_api::LeaderAuthority> {
        klights_replication::authority::WatchLeaderAuthority::channel(is_leader, None).0
    }

    async fn fixture(
        is_leader: bool,
    ) -> (
        klights_cluster_datastore::sqlite::embedded::Datastore,
        Arc<RecordingApplyingProposal>,
        ClusterStoreLeaderNetwork,
    ) {
        let db = crate::bootstrap::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
        let canonical = db.clone();
        let proposal = Arc::new(RecordingApplyingProposal {
            inner: crate::bootstrap::outbox_apply_adapter::BackendProposalFixture::new(
                Arc::new(canonical.clone()),
                Arc::new(canonical.clone()),
                canonical.focused_read_store(),
            ),
            commands: Default::default(),
        });
        let adapter = ClusterStoreLeaderNetwork::new(
            db.focused_read_store(),
            proposal.clone(),
            authority(is_leader),
        );
        (db, proposal, adapter)
    }

    #[tokio::test]
    async fn network_cluster_writes_with_proposer_route_through_raft() {
        let (db, proposal, adapter) = fixture(true).await;
        adapter
            .allocate_node_subnet(
                NodeSubnetAllocationRequest::try_new("worker-1", "10.50.0.0/16", "192.0.2.10")
                    .unwrap(),
            )
            .await
            .expect("subnet allocation must apply through Raft");
        adapter
            .register_node_dataplane(
                NetworkDataplane::try_new(
                    "worker-1",
                    NetworkNodeMode::Root,
                    DataplaneEncryption::Direct,
                    None,
                    "192.0.2.10".parse().unwrap(),
                    None,
                )
                .unwrap(),
            )
            .await
            .expect("dataplane update must apply through Raft");

        assert!(matches!(
            proposal.commands.lock().unwrap().as_slice(),
            [StorageCommand::AllocateNodeSubnet { node_name, .. }, StorageCommand::UpdateNodeDataplane { node_name: dataplane_node, .. }]
                if node_name == "worker-1" && dataplane_node == "worker-1"
        ));
        assert!(db.get_node_subnet("worker-1").await.unwrap().is_some());
        assert!(db.get_node_dataplane("worker-1").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn raft_mode_follower_proposer_rejects_network_cluster_writes_no_local_mutation() {
        let (db, proposal, adapter) = fixture(false).await;
        let error = adapter
            .register_node_dataplane(
                NetworkDataplane::try_new(
                    "worker-1",
                    NetworkNodeMode::Root,
                    DataplaneEncryption::Direct,
                    None,
                    "192.0.2.10".parse().unwrap(),
                    None,
                )
                .unwrap(),
            )
            .await
            .expect_err("follower topology write must be rejected");

        assert_eq!(error, NetworkTopologyError::NotLeader);
        assert!(proposal.commands.lock().unwrap().is_empty());
        assert!(db.get_node_subnet("worker-1").await.unwrap().is_none());
        assert!(db.get_node_dataplane("worker-1").await.unwrap().is_none());
    }
}
