//! Root composition adapters for the focused networking contract.
//!
//! This is the only application layer allowed to see the umbrella leader
//! client while preparing networking. Each consumer receives a separately
//! erased focused capability and cannot recover unrelated leader operations.

use std::sync::Arc;

use crate::control_plane::client::LeaderApiClient;
use klights_leader_api::{
    LeaderNetworkTopologyQuery, LeaderNodeSubnetAllocation, LeaderResourceQuery, LeaderWatch,
    LeaderWatchFuture, NetworkTopologyFuture, NodeDataplaneQuery, NodeDataplaneResult,
    NodeSubnetAllocationFuture, NodeSubnetAllocationRequest, NodeSubnetAllocationResult,
    NodeSubnetQuery, NodeSubnetResult, PeerSubnetsQuery, PeerSubnetsResult, ResourceGetRequest,
    ResourceListRequest, ResourceListResult, ResourceQueryFuture, WatchRequest,
};

#[derive(Clone)]
pub(crate) struct FocusedNetworkLeaderPorts {
    inner: Arc<dyn LeaderApiClient>,
}

impl FocusedNetworkLeaderPorts {
    pub(crate) fn new(inner: Arc<dyn LeaderApiClient>) -> Arc<Self> {
        Arc::new(Self { inner })
    }

    pub(crate) fn subnet_allocation(self: &Arc<Self>) -> Arc<dyn LeaderNodeSubnetAllocation> {
        self.clone()
    }

    pub(crate) fn topology(self: &Arc<Self>) -> Arc<dyn LeaderNetworkTopologyQuery> {
        self.clone()
    }

    pub(crate) fn resource_query(self: &Arc<Self>) -> Arc<dyn LeaderResourceQuery> {
        self.clone()
    }

    pub(crate) fn watch(self: &Arc<Self>) -> Arc<dyn LeaderWatch> {
        self.clone()
    }
}

impl LeaderNodeSubnetAllocation for FocusedNetworkLeaderPorts {
    fn allocate_node_subnet(
        &self,
        request: NodeSubnetAllocationRequest,
    ) -> NodeSubnetAllocationFuture<'_, NodeSubnetAllocationResult> {
        self.inner.allocate_node_subnet(request)
    }
}

impl LeaderNetworkTopologyQuery for FocusedNetworkLeaderPorts {
    fn get_node_subnet(
        &self,
        request: NodeSubnetQuery,
    ) -> NetworkTopologyFuture<'_, NodeSubnetResult> {
        self.inner.get_node_subnet(request)
    }

    fn list_peer_subnets(
        &self,
        request: PeerSubnetsQuery,
    ) -> NetworkTopologyFuture<'_, PeerSubnetsResult> {
        self.inner.list_peer_subnets(request)
    }

    fn get_node_dataplane(
        &self,
        request: NodeDataplaneQuery,
    ) -> NetworkTopologyFuture<'_, NodeDataplaneResult> {
        self.inner.get_node_dataplane(request)
    }
}

impl LeaderResourceQuery for FocusedNetworkLeaderPorts {
    fn get_resource(
        &self,
        request: ResourceGetRequest,
    ) -> ResourceQueryFuture<'_, Option<crate::datastore::Resource>> {
        self.inner.get_resource(request)
    }

    fn list_resources(
        &self,
        request: ResourceListRequest,
    ) -> ResourceQueryFuture<'_, ResourceListResult> {
        self.inner.list_resources(request)
    }
}

impl LeaderWatch for FocusedNetworkLeaderPorts {
    fn watch_resources(&self, request: WatchRequest) -> LeaderWatchFuture<'_> {
        self.inner.watch_resources(request)
    }
}

pub(crate) fn network_mode(mode: &crate::bootstrap::NodeMode) -> crate::networking::NetworkMode {
    match mode {
        crate::bootstrap::NodeMode::Root => crate::networking::NetworkMode::Root,
        crate::bootstrap::NodeMode::Rootless { .. } => crate::networking::NetworkMode::Rootless,
    }
}

pub(crate) fn cleanup_config(
    mode: &crate::bootstrap::NodeMode,
    config: &crate::KlightsConfig,
) -> anyhow::Result<crate::networking::NetworkCleanupConfig> {
    crate::networking::NetworkCleanupConfig::try_new(
        network_mode(mode),
        config.bridge_name.clone(),
        config.wireguard_device.clone(),
        config.containerd_namespace.clone(),
        matches!(
            mode,
            crate::bootstrap::NodeMode::Rootless {
                rootlesskit_pid,
                ..
            } if *rootlesskit_pid != 0
        ),
    )
    .map_err(anyhow::Error::msg)
}
