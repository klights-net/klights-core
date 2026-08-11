//! Focused worker-side topology and cleanup ports.
//!
//! These methods deliberately return the validated leader-api projections. A
//! worker can request leader-owned topology/cleanup effects, but it cannot
//! obtain a cluster datastore handle or fall back to a local cluster write.

use anyhow::Result;
use klights_leader_api::{
    NetworkDataplane, NodeDataplaneQuery, NodeSubnet, NodeSubnetAllocationRequest, NodeSubnetQuery,
    PeerSubnetsQuery, PodCleanupIntent, PodCleanupIntentAckRequest, PodCleanupIntentListRequest,
};

use super::WorkerStoreAdapter;

impl WorkerStoreAdapter {
    /// Ask the leader to allocate (or return) this worker's pod subnet.
    pub async fn allocate_node_subnet(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<NodeSubnet> {
        let request = NodeSubnetAllocationRequest::try_new(node_name, cluster_cidr, node_ip)
            .map_err(anyhow::Error::new)?;
        let result = self
            .subnet_allocation
            .allocate_node_subnet(request)
            .await
            .map_err(anyhow::Error::new)?;
        Ok(result.into_subnet())
    }

    /// Read one node's leader-owned dataplane projection.
    pub async fn get_node_dataplane(&self, node_name: &str) -> Result<Option<NetworkDataplane>> {
        let request = NodeDataplaneQuery::try_new(node_name).map_err(anyhow::Error::new)?;
        self.network_topology
            .get_node_dataplane(request)
            .await
            .map_err(anyhow::Error::new)
            .map(|result| result.into_option())
    }

    /// Read one node's leader-owned subnet projection.
    pub async fn get_node_subnet(&self, node_name: &str) -> Result<Option<NodeSubnet>> {
        let request = NodeSubnetQuery::try_new(node_name).map_err(anyhow::Error::new)?;
        self.network_topology
            .get_node_subnet(request)
            .await
            .map_err(anyhow::Error::new)
            .map(|result| result.into_option())
    }

    /// Read peer subnets while excluding the requesting worker.
    pub async fn list_peer_subnets(&self, excluded_node_name: &str) -> Result<Vec<NodeSubnet>> {
        let request = PeerSubnetsQuery::try_new(excluded_node_name).map_err(anyhow::Error::new)?;
        self.network_topology
            .list_peer_subnets(request)
            .await
            .map_err(anyhow::Error::new)
            .map(|result| result.into_vec())
    }

    /// Read cleanup intents durably owned by this worker's leader.
    pub async fn list_pod_cleanup_intents(&self, node_name: &str) -> Result<Vec<PodCleanupIntent>> {
        let request =
            PodCleanupIntentListRequest::try_new(node_name).map_err(anyhow::Error::new)?;
        self.cleanup_intents
            .list_pod_cleanup_intents(request)
            .await
            .map_err(anyhow::Error::new)
    }

    /// Acknowledge exactly one UID-qualified cleanup intent at the leader.
    pub async fn acknowledge_pod_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        let request =
            PodCleanupIntentAckRequest::try_new(node_name, namespace, pod_name, pod_uid, reason)
                .map_err(anyhow::Error::new)?;
        self.cleanup_intents
            .acknowledge_pod_cleanup_intent(request)
            .await
            .map_err(anyhow::Error::new)
    }
}
