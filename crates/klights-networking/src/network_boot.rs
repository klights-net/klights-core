//! Mode-aware network boot dispatcher owned by `klights-networking`.

use anyhow::Result;
use std::sync::Arc;

use crate::NetworkPlane;
use crate::dataplane_health::DataplaneHealth;
use crate::rootless::{RootlessNetworkBoot, RootlessNetworkPlane, RootlessNetworkStores};
use klights_types::PodSubnet;

pub struct NetworkBootStores {
    pub(crate) subnet_allocation: Arc<dyn klights_leader_api::LeaderNodeSubnetAllocation>,
    pub(crate) topology: Arc<dyn klights_leader_api::LeaderNetworkTopologyQuery>,
    pub(crate) pod_network_cache: Arc<dyn klights_node_store::PodNetworkCache>,
    pub(crate) pod_ipam: Arc<dyn klights_node_store::PodIpamStore>,
    pub(crate) pod_runtime: Arc<dyn klights_node_store::PodRuntimeStore>,
    pub(crate) assignment_publisher: Arc<dyn klights_network_api::PodNetworkAssignmentPublisher>,
}

impl NetworkBootStores {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        subnet_allocation: Arc<dyn klights_leader_api::LeaderNodeSubnetAllocation>,
        topology: Arc<dyn klights_leader_api::LeaderNetworkTopologyQuery>,
        pod_network_cache: Arc<dyn klights_node_store::PodNetworkCache>,
        pod_ipam: Arc<dyn klights_node_store::PodIpamStore>,
        pod_runtime: Arc<dyn klights_node_store::PodRuntimeStore>,
        assignment_publisher: Arc<dyn klights_network_api::PodNetworkAssignmentPublisher>,
    ) -> Self {
        Self {
            subnet_allocation,
            topology,
            pod_network_cache,
            pod_ipam,
            pod_runtime,
            assignment_publisher,
        }
    }
}

pub enum NetworkBoot {
    Root(Arc<NetworkPlane>),
    Rootless(Arc<RootlessNetworkPlane>),
}

impl NetworkBoot {
    pub async fn boot(
        cfg: &crate::NetworkBootConfig,
        stores: NetworkBootStores,
        cancel: tokio_util::sync::CancellationToken,
        task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Result<Self> {
        match cfg.mode() {
            crate::NetworkMode::Root => Ok(Self::Root(
                NetworkPlane::boot(cfg, stores, cancel, task_supervisor).await?,
            )),
            crate::NetworkMode::Rootless => Ok(Self::Rootless(
                boot_rootless(cfg, stores, cancel, task_supervisor).await?,
            )),
        }
    }

    pub fn local_pod_subnet(&self) -> PodSubnet {
        match self {
            Self::Root(plane) => plane.local_pod_subnet(),
            Self::Rootless(plane) => *plane.local_subnet(),
        }
    }

    pub fn peer_router(&self) -> Arc<dyn klights_network_api::PeerRouter> {
        match self {
            Self::Root(plane) => plane.peer_router(),
            Self::Rootless(plane) => plane.clone(),
        }
    }

    pub fn health(&self) -> &DataplaneHealth {
        match self {
            Self::Root(plane) => plane.health(),
            Self::Rootless(plane) => plane.health(),
        }
    }
}

async fn boot_rootless(
    cfg: &crate::NetworkBootConfig,
    stores: NetworkBootStores,
    cancel: tokio_util::sync::CancellationToken,
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
) -> Result<Arc<RootlessNetworkPlane>> {
    let NetworkBootStores {
        subnet_allocation,
        topology,
        pod_network_cache,
        pod_ipam,
        pod_runtime,
        assignment_publisher,
    } = stores;
    RootlessNetworkPlane::boot(RootlessNetworkBoot::new(
        cfg.bridge().clone(),
        cfg.node().clone(),
        *cfg.cluster_cidr(),
        cfg.host_ip(),
        cfg.encryption(),
        crate::wireguard::WireGuardBootConfig::try_new(
            cfg.wireguard_device(),
            cfg.wireguard_key_path(),
            cfg.wireguard_port(),
        )
        .map_err(anyhow::Error::new)?,
        crate::PodLinkMtu::try_new(crate::pod_link_mtu_for_encryption(cfg.encryption()))
            .map_err(anyhow::Error::msg)?,
        RootlessNetworkStores::new(
            subnet_allocation,
            topology,
            pod_network_cache,
            pod_ipam,
            pod_runtime,
            assignment_publisher,
        ),
        cancel,
        task_supervisor,
    ))
    .await
}
