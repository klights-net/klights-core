use anyhow::{Context, Result};
use std::net::Ipv4Addr;
use std::sync::Arc;

use crate::{RootDatapath, RootPeerDataplane, RootPeerDataplaneBoot};
use klights_types::{NodeName, PodSubnet};

/// Root-mode CNI datapath and peer-router implementation.
pub struct NetworkPlane {
    root: RootDatapath,
    pod_network_cache: Arc<dyn klights_node_store::PodNetworkCache>,
    pod_ipam: Arc<dyn klights_node_store::PodIpamStore>,
    pod_runtime: Arc<dyn klights_node_store::PodRuntimeStore>,
    assignment_publisher: Arc<dyn klights_network_api::PodNetworkAssignmentPublisher>,
    sandbox_operations: crate::SandboxOperationLocks,
    my_node: NodeName,
    host_ip: Ipv4Addr,
    peer: Arc<RootPeerDataplane>,
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
}

impl NetworkPlane {
    pub(crate) async fn boot(
        cfg: &crate::NetworkBootConfig,
        stores: crate::NetworkBootStores,
        cancel: tokio_util::sync::CancellationToken,
        task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Result<Arc<Self>> {
        let crate::NetworkBootStores {
            subnet_allocation,
            topology,
            pod_network_cache,
            pod_ipam,
            pod_runtime,
            assignment_publisher,
        } = stores;
        let my_node = cfg.node().clone();
        let host_ip = cfg.host_ip();
        let local_subnet =
            crate::NodeSubnetAllocator::new(subnet_allocation, topology, task_supervisor.clone())
                .allocate_or_reuse_existing(
                    cfg.node().as_ref(),
                    &cfg.cluster_cidr().to_string(),
                    &cfg.host_ip().to_string(),
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to allocate local node subnet for {} at {}",
                        cfg.node(),
                        cfg.host_ip()
                    )
                })?;
        let root = RootDatapath::boot(
            cfg.bridge().clone(),
            local_subnet,
            crate::PodLinkMtu::try_new(crate::pod_link_mtu_for_encryption(cfg.encryption()))
                .map_err(anyhow::Error::msg)?,
            cancel.clone(),
            task_supervisor.clone(),
        )
        .await?;
        let peer = Arc::new(
            RootPeerDataplane::boot(
                &root,
                RootPeerDataplaneBoot::new(
                    cfg.encryption(),
                    crate::wireguard::WireGuardBootConfig::try_new(
                        cfg.wireguard_device(),
                        cfg.wireguard_key_path(),
                        cfg.wireguard_port(),
                    )
                    .map_err(anyhow::Error::new)?,
                    cancel,
                    task_supervisor.clone(),
                ),
            )
            .await,
        );
        Ok(Arc::new(Self {
            root,
            pod_network_cache,
            pod_ipam,
            pod_runtime,
            assignment_publisher,
            sandbox_operations: crate::SandboxOperationLocks::default(),
            my_node,
            host_ip,
            peer,
            task_supervisor,
        }))
    }

    pub fn local_pod_subnet(&self) -> PodSubnet {
        self.root.pod_subnet()
    }

    pub fn health(&self) -> &crate::dataplane_health::DataplaneHealth {
        self.peer.health()
    }

    pub fn peer_router(&self) -> Arc<dyn klights_network_api::PeerRouter> {
        self.peer.clone()
    }

    async fn cni_add(
        &self,
        request: klights_network_api::CniAddRequest,
    ) -> Result<klights_network_api::PodNetwork> {
        let (sandbox_id, pod, netns_setns_path, netns_record_path, host_network) =
            request.into_parts();
        let _sandbox_guard = self.sandbox_operations.acquire(sandbox_id.as_str()).await;
        let bridge_idx = self
            .root
            .bridge_index()
            .await
            .with_context(|| format!("bridge {} not found", self.root.bridge()))?;
        let pod_subnet = self.root.pod_subnet();
        crate::add(crate::CniAddArgs {
            cache: self.pod_network_cache.as_ref(),
            ipam: self.pod_ipam.as_ref(),
            runtime: self.pod_runtime.as_ref(),
            assignment_publisher: self.assignment_publisher.as_ref(),
            handle: self.root.handle(),
            sandbox_id: sandbox_id.as_str(),
            pod,
            bridge_name: self.root.bridge(),
            bridge_idx,
            netns_setns_path: netns_setns_path.as_str(),
            netns_record_path: netns_record_path.as_str(),
            pod_subnet: &pod_subnet,
            pod_link_mtu: self.root.pod_link_mtu(),
            host_network,
            host_ip: &self.host_ip.to_string(),
            _node_name: &self.my_node,
            task_supervisor: self.task_supervisor.clone(),
        })
        .await
    }

    async fn cni_del(&self, sandbox_id: &str) -> Result<()> {
        let _sandbox_guard = self.sandbox_operations.acquire(sandbox_id).await;
        if self
            .pod_network_cache
            .get_network_for_sandbox(klights_node_store::SandboxKey::try_new(sandbox_id)?)
            .await
            .context("failed to look up root pod network allocation")?
            .is_none()
        {
            tracing::debug!(
                "root cni::del {}: no pod_networks record (host-network or already deleted)",
                sandbox_id
            );
            return Ok(());
        }
        let bridge_idx = self
            .root
            .bridge_index()
            .await
            .with_context(|| format!("bridge {} not found", self.root.bridge()))?;
        crate::del(
            self.pod_network_cache.as_ref(),
            self.root.handle(),
            sandbox_id,
            bridge_idx,
        )
        .await
    }

    async fn shutdown_impl(&self) -> Result<()> {
        self.peer.shutdown().await?;
        self.root.shutdown();
        Ok(())
    }
}

impl klights_network_api::Datapath for NetworkPlane {
    fn cni_add(
        &self,
        request: klights_network_api::CniAddRequest,
    ) -> klights_network_api::DatapathFuture<'_, klights_network_api::PodNetwork> {
        Box::pin(async move {
            Self::cni_add(self, request)
                .await
                .map_err(|error| klights_network_api::DatapathError::setup(error.to_string()))
        })
    }

    fn cni_del<'a>(
        &'a self,
        sandbox_id: &'a klights_network_api::SandboxId,
    ) -> klights_network_api::DatapathFuture<'a, ()> {
        Box::pin(async move {
            Self::cni_del(self, sandbox_id.as_str())
                .await
                .map_err(|error| klights_network_api::DatapathError::teardown(error.to_string()))
        })
    }

    fn host_ip(&self) -> klights_network_api::DatapathFuture<'_, std::net::IpAddr> {
        Box::pin(async move { Ok(std::net::IpAddr::V4(self.host_ip)) })
    }

    fn pod_gateway_ip(&self) -> klights_network_api::DatapathFuture<'_, std::net::IpAddr> {
        Box::pin(async move { Ok(std::net::IpAddr::V4(self.root.pod_subnet().bridge_ip())) })
    }

    fn shutdown(&self) -> klights_network_api::DatapathFuture<'_, ()> {
        Box::pin(async move {
            self.shutdown_impl()
                .await
                .map_err(|error| klights_network_api::DatapathError::shutdown(error.to_string()))
        })
    }
}
