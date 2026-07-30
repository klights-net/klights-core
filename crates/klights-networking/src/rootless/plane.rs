//! Rootless network boot path (F2-01).
//!
//! `NetworkPlane` (root mode) is too heavy for rootless: it owns host-network
//! bridge and route-device state via rtnetlink. Those operations are not valid
//! in a user namespace where klights does not own the host interfaces.
//!
//! `RootlessNetworkPlane` keeps the slice of boot-time state every mode needs
//! (the local pod subnet and bridge/veth CNI in the rootless network namespace)
//! and drops root-only host-network setup. Remaining rootless lifecycle work
//! (pasta process management, bypass4netns socket grafting, hostport
//! publication) attaches to this struct rather than growing rootless-only
//! branches inside the root-mode plane.

use anyhow::{Context, Result};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, OnceLock};

use crate::BridgeName;
use crate::dataplane_health::DataplaneHealth;
use klights_types::{ClusterCidr, NodeName, PodSubnet};

pub struct RootlessNetworkStores {
    subnet_allocation: Arc<dyn klights_leader_api::LeaderNodeSubnetAllocation>,
    topology: Arc<dyn klights_leader_api::LeaderNetworkTopologyQuery>,
    pod_network_cache: Arc<dyn klights_node_store::PodNetworkCache>,
    pod_ipam: Arc<dyn klights_node_store::PodIpamStore>,
    pod_runtime: Arc<dyn klights_node_store::PodRuntimeStore>,
    assignment_publisher: Arc<dyn klights_network_api::PodNetworkAssignmentPublisher>,
}

impl RootlessNetworkStores {
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

pub struct RootlessNetworkBoot {
    bridge: BridgeName,
    node: NodeName,
    cluster_cidr: ClusterCidr,
    host_ip: Ipv4Addr,
    encryption: crate::wireguard::DataplaneEncryption,
    wireguard: crate::wireguard::WireGuardBootConfig,
    pod_link_mtu: u32,
    stores: RootlessNetworkStores,
    cancel: tokio_util::sync::CancellationToken,
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
}

impl RootlessNetworkBoot {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        bridge: BridgeName,
        node: NodeName,
        cluster_cidr: ClusterCidr,
        host_ip: Ipv4Addr,
        encryption: crate::wireguard::DataplaneEncryption,
        wireguard: crate::wireguard::WireGuardBootConfig,
        pod_link_mtu: crate::PodLinkMtu,
        stores: RootlessNetworkStores,
        cancel: tokio_util::sync::CancellationToken,
        task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Self {
        Self {
            bridge,
            node,
            cluster_cidr,
            host_ip,
            encryption,
            wireguard,
            pod_link_mtu: pod_link_mtu.get(),
            stores,
            cancel,
            task_supervisor,
        }
    }
}

pub struct RootlessNetworkPlane {
    pod_network_cache: Arc<dyn klights_node_store::PodNetworkCache>,
    pod_ipam: Arc<dyn klights_node_store::PodIpamStore>,
    pod_runtime: Arc<dyn klights_node_store::PodRuntimeStore>,
    assignment_publisher: Arc<dyn klights_network_api::PodNetworkAssignmentPublisher>,
    sandbox_operations: crate::cni::SandboxOperationLocks,
    rt: rtnetlink::Handle,
    _rt_conn: klights_supervisor::SupervisedJoinHandle<()>,
    /// Resolved local pod subnet allocated through the same IPAM path that
    /// root mode uses, so cluster-wide /24 layout matches across modes.
    local_subnet: PodSubnet,
    bridge: BridgeName,
    pod_link_mtu: u32,
    bridge_idx: OnceLock<u32>,
    my_node: NodeName,
    host_ip: Ipv4Addr,
    wireguard_device: String,
    wireguard_idx: OnceLock<u32>,
    wireguard: OnceLock<Arc<crate::wireguard::WireGuardController>>,
    health: DataplaneHealth,
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
}

impl RootlessNetworkPlane {
    /// Boot the rootless network plane. Allocates the node-local pod subnet
    /// through the shared IPAM and returns; intentionally skips boot-time
    /// host-network mutation. The bridge is created
    /// lazily on the first non-hostNetwork CNI ADD so unit tests and idle
    /// rootless starts do not require netlink mutations until pods need them.
    pub async fn boot(request: RootlessNetworkBoot) -> Result<Arc<Self>> {
        let RootlessNetworkBoot {
            bridge,
            node,
            cluster_cidr,
            host_ip,
            encryption,
            wireguard,
            pod_link_mtu,
            stores,
            cancel,
            task_supervisor,
        } = request;
        let RootlessNetworkStores {
            subnet_allocation,
            topology,
            pod_network_cache,
            pod_ipam,
            pod_runtime,
            assignment_publisher,
        } = stores;
        let my_node = node;
        let (conn, handle, _) = rtnetlink::new_connection()
            .context("failed to open rtnetlink for rootless network plane")?;
        let rt_cancel = cancel.clone();
        let rt_conn = task_supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Network,
                "rootless_network_plane_rtnetlink_connection",
                async move {
                    tokio::select! {
                        _ = conn => {}
                        _ = rt_cancel.cancelled() => {}
                    }
                },
            )
            .await
            .context("failed to spawn rootless network plane rtnetlink connection task")?;
        let local_subnet = crate::subnet_allocator::NodeSubnetAllocator::new(
            subnet_allocation,
            topology,
            task_supervisor.clone(),
        )
        .allocate_or_reuse_existing(
            my_node.as_ref(),
            &cluster_cidr.to_string(),
            &host_ip.to_string(),
        )
        .await
        .with_context(|| {
            format!(
                "failed to allocate local rootless node subnet for {} at {}",
                my_node, host_ip
            )
        })?;
        let plane = Arc::new(Self {
            pod_network_cache,
            pod_ipam,
            pod_runtime,
            assignment_publisher,
            sandbox_operations: crate::cni::SandboxOperationLocks::default(),
            rt: handle,
            _rt_conn: rt_conn,
            local_subnet,
            bridge,
            pod_link_mtu,
            bridge_idx: OnceLock::new(),
            my_node,
            host_ip,
            wireguard_device: wireguard.device().to_string(),
            wireguard_idx: OnceLock::new(),
            wireguard: OnceLock::new(),
            health: DataplaneHealth::new_healthy(),
            task_supervisor: task_supervisor.clone(),
        });
        if encryption == crate::wireguard::DataplaneEncryption::Enabled
            && let Err(err) = plane
                .ensure_wireguard_enabled(wireguard.key_path(), wireguard.port(), cancel)
                .await
        {
            plane
                .health
                .set_unavailable(format!("rootless WireGuard dataplane: {err:#}"));
            tracing::error!(
                error = %err,
                "rootless WireGuard dataplane setup failed; node will report NotReady"
            );
        }
        Ok(plane)
    }

    /// Local pod subnet record allocated at boot.
    pub fn local_subnet(&self) -> &PodSubnet {
        &self.local_subnet
    }

    /// Dataplane health snapshot. Callers wire this into node conditions
    /// so that WireGuard/pasta failures surface as `NetworkUnavailable=True`
    /// instead of the node silently accepting plaintext.
    pub fn health(&self) -> &DataplaneHealth {
        &self.health
    }

    fn ignore_eexist<T>(res: std::result::Result<T, rtnetlink::Error>) -> Result<()> {
        match res {
            Ok(_) => Ok(()),
            Err(err) if crate::root_datapath::is_nl_eexist_error(&err) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    async fn ensure_link_up_and_mtu(&self, idx: u32, expected_mtu: u32) -> Result<()> {
        self.rt
            .link()
            .set(idx)
            .mtu(expected_mtu)
            .execute()
            .await
            .context("failed to set rootless interface MTU")?;
        self.rt
            .link()
            .set(idx)
            .up()
            .execute()
            .await
            .context("failed to bring rootless interface up")?;
        Ok(())
    }

    async fn ensure_bridge_once(&self) -> Result<u32> {
        if let Some(idx) = self.bridge_idx.get() {
            return Ok(*idx);
        }

        if self.link_index(self.bridge.as_ref()).await.is_err() {
            self.rt
                .link()
                .add()
                .bridge(self.bridge.as_ref().to_string())
                .execute()
                .await
                .with_context(|| format!("failed to create rootless bridge {}", self.bridge))?;
            tracing::info!(bridge = %self.bridge, "created rootless bridge");
        }

        let idx = self
            .link_index(self.bridge.as_ref())
            .await
            .with_context(|| format!("rootless bridge {} not found after creation", self.bridge))?;

        Self::ignore_eexist(
            self.rt
                .address()
                .add(
                    idx,
                    IpAddr::V4(self.local_subnet.bridge_ip()),
                    self.local_subnet.prefix(),
                )
                .execute()
                .await,
        )?;

        self.ensure_link_up_and_mtu(idx, self.pod_link_mtu).await?;
        let _ = self.bridge_idx.set(idx);
        Ok(idx)
    }

    pub async fn prepare_service_routing_bridge(&self) -> Result<()> {
        self.ensure_bridge_once().await.with_context(|| {
            format!(
                "rootless bridge {} not ready for service routing",
                self.bridge
            )
        })?;
        Ok(())
    }

    async fn link_index(&self, name: &str) -> Result<u32> {
        use futures::stream::TryStreamExt;

        let mut links = self.rt.link().get().match_name(name.to_owned()).execute();
        if let Some(link) = links
            .try_next()
            .await
            .context("rtnl list-link failed while resolving rootless interface index")?
        {
            Ok(link.header.index)
        } else {
            anyhow::bail!("interface {} not found", name)
        }
    }

    async fn link_index_cached(&self, name: &str, cache: &OnceLock<u32>) -> Result<u32> {
        if let Some(idx) = cache.get() {
            return Ok(*idx);
        }
        let idx = self.link_index(name).await?;
        let _ = cache.set(idx);
        Ok(idx)
    }

    async fn ensure_wireguard_once(&self) -> Result<u32> {
        match self.link_index(&self.wireguard_device).await {
            Ok(idx) => {
                let _ = self.wireguard_idx.set(idx);
            }
            Err(_) => {
                match self
                    .rt
                    .link()
                    .add()
                    .wireguard(self.wireguard_device.clone())
                    .execute()
                    .await
                {
                    Ok(_) => {}
                    Err(err) if crate::root_datapath::is_nl_eexist_error(&err) => {}
                    Err(err) => {
                        return Err(err).context("failed to create rootless WireGuard link");
                    }
                }
            }
        }
        let idx = self.link_index(&self.wireguard_device).await?;
        self.rt
            .link()
            .set(idx)
            .mtu(crate::wireguard::WIREGUARD_MTU)
            .execute()
            .await
            .context("failed to set rootless WireGuard MTU")?;
        self.rt
            .link()
            .set(idx)
            .up()
            .execute()
            .await
            .context("failed to bring rootless WireGuard link up")?;
        let _ = self.wireguard_idx.set(idx);
        Ok(idx)
    }

    async fn ensure_wireguard_enabled(
        &self,
        key_path: &std::path::Path,
        port: u16,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<()> {
        self.ensure_wireguard_once().await?;
        let identity = crate::wireguard::WireGuardIdentity::load_or_create(
            key_path,
            self.task_supervisor.as_ref(),
        )
        .await?;
        let config = crate::wireguard::WireGuardDeviceConfig::try_new(
            self.wireguard_device.clone(),
            identity.private_key().clone(),
            port,
        )?;
        let controller = Arc::new(
            crate::wireguard::WireGuardController::open(
                config,
                self.task_supervisor.as_ref(),
                cancel,
            )
            .await?,
        );
        let _ = self.wireguard.set(controller);

        // Validate that pasta is exposing the WireGuard UDP port at the
        // host edge. If /proc/net/udp doesn't show the port as bound,
        // other nodes cannot reach this rootless node's encrypted dataplane.
        crate::rootless::pasta::verify_wireguard_udp_port(port, self.task_supervisor.as_ref())
            .await?;

        Ok(())
    }
}

impl klights_network_api::Datapath for RootlessNetworkPlane {
    fn cni_add(
        &self,
        request: klights_network_api::CniAddRequest,
    ) -> klights_network_api::DatapathFuture<'_, klights_network_api::PodNetwork> {
        Box::pin(async move {
            let (sandbox_id, pod, netns_setns_path, netns_record_path, host_network) =
                request.into_parts();
            if host_network {
                return Ok(klights_network_api::PodNetwork::new(IpAddr::V4(
                    self.host_ip,
                )));
            }
            let _sandbox_guard = self.sandbox_operations.acquire(sandbox_id.as_str()).await;

            let bridge_idx = self
                .ensure_bridge_once()
                .await
                .with_context(|| format!("rootless bridge {} not ready", self.bridge))
                .map_err(|error| klights_network_api::DatapathError::setup(error.to_string()))?;
            crate::cni::add(crate::cni::CniAddArgs {
                cache: self.pod_network_cache.as_ref(),
                ipam: self.pod_ipam.as_ref(),
                runtime: self.pod_runtime.as_ref(),
                assignment_publisher: self.assignment_publisher.as_ref(),
                handle: &self.rt,
                sandbox_id: sandbox_id.as_str(),
                pod,
                bridge_name: &self.bridge,
                bridge_idx,
                netns_setns_path: netns_setns_path.as_str(),
                netns_record_path: netns_record_path.as_str(),
                pod_subnet: &self.local_subnet,
                pod_link_mtu: self.pod_link_mtu,
                host_network,
                host_ip: &self.host_ip.to_string(),
                _node_name: &self.my_node,
                task_supervisor: self.task_supervisor.clone(),
            })
            .await
            .map_err(|error| klights_network_api::DatapathError::setup(error.to_string()))
        })
    }

    fn cni_del<'a>(
        &'a self,
        sandbox_id: &'a klights_network_api::SandboxId,
    ) -> klights_network_api::DatapathFuture<'a, ()> {
        Box::pin(async move {
            let _sandbox_guard = self.sandbox_operations.acquire(sandbox_id.as_str()).await;
            if self
                .pod_network_cache
                .get_network_for_sandbox(
                    klights_node_store::SandboxKey::try_new(sandbox_id.as_str()).map_err(
                        |error| klights_network_api::DatapathError::teardown(error.to_string()),
                    )?,
                )
                .await
                .context("failed to look up rootless pod network allocation")
                .map_err(|error| klights_network_api::DatapathError::teardown(error.to_string()))?
                .is_none()
            {
                tracing::debug!(
                    "rootless cni::del {}: no pod_networks record (host-network or already deleted)",
                    sandbox_id
                );
                return Ok(());
            }

            let bridge_idx = self
                .ensure_bridge_once()
                .await
                .with_context(|| format!("rootless bridge {} not ready", self.bridge))
                .map_err(|error| klights_network_api::DatapathError::teardown(error.to_string()))?;
            crate::cni::del(
                self.pod_network_cache.as_ref(),
                &self.rt,
                sandbox_id.as_str(),
                bridge_idx,
            )
            .await
            .map_err(|error| klights_network_api::DatapathError::teardown(error.to_string()))
        })
    }

    fn host_ip(&self) -> klights_network_api::DatapathFuture<'_, std::net::IpAddr> {
        Box::pin(async move { Ok(IpAddr::V4(self.host_ip)) })
    }

    fn pod_gateway_ip(&self) -> klights_network_api::DatapathFuture<'_, std::net::IpAddr> {
        Box::pin(async move { Ok(IpAddr::V4(self.local_subnet.bridge_ip())) })
    }

    fn shutdown(&self) -> klights_network_api::DatapathFuture<'_, ()> {
        Box::pin(async move {
            if let Some(controller) = self.wireguard.get() {
                controller.shutdown().await.map_err(|error| {
                    klights_network_api::DatapathError::shutdown(error.to_string())
                })?;
            }
            self._rt_conn.abort();
            Ok(())
        })
    }
}

impl klights_network_api::PeerRouter for RootlessNetworkPlane {
    fn apply_peer_route<'a>(
        &'a self,
        route: &'a klights_network_api::PeerRoute,
    ) -> klights_network_api::PeerRouterFuture<'a> {
        Box::pin(async move {
            let result: anyhow::Result<()> = async {
                match route {
                    klights_network_api::PeerRoute::WireGuard(route) => {
                        let controller = self
                            .wireguard
                            .get()
                            .context("rootless WireGuard dataplane is not initialized")?;
                        let idx = self
                            .link_index_cached(&self.wireguard_device, &self.wireguard_idx)
                            .await?;
                        self.ensure_bridge_once().await?;
                        crate::peer_dataplane::apply_wireguard_peer_route_with_rollback(
                            || Box::pin(controller.apply_peer(route)),
                            || {
                                Box::pin(crate::wireguard::apply_wireguard_pod_route(
                                    &self.rt,
                                    idx,
                                    route,
                                    self.local_subnet.bridge_ip(),
                                ))
                            },
                            || {
                                Box::pin(crate::wireguard::remove_wireguard_pod_route(
                                    &self.rt,
                                    idx,
                                    route,
                                    self.local_subnet.bridge_ip(),
                                ))
                            },
                            || Box::pin(controller.remove_peer(route)),
                        )
                        .await
                    }
                    klights_network_api::PeerRoute::Direct(route) => {
                        crate::wireguard::apply_unencrypted_direct_route(&self.rt, route).await
                    }
                }
            }
            .await;
            result.map_err(|error| klights_network_api::PeerRouterError::apply(error.to_string()))
        })
    }

    fn remove_peer_route<'a>(
        &'a self,
        route: &'a klights_network_api::PeerRoute,
    ) -> klights_network_api::PeerRouterFuture<'a> {
        Box::pin(async move {
            let result: anyhow::Result<()> = async {
                match route {
                    klights_network_api::PeerRoute::WireGuard(route) => {
                        let idx = self
                            .link_index_cached(&self.wireguard_device, &self.wireguard_idx)
                            .await?;
                        crate::wireguard::remove_wireguard_pod_route(
                            &self.rt,
                            idx,
                            route,
                            self.local_subnet.bridge_ip(),
                        )
                        .await?;
                        if let Some(controller) = self.wireguard.get() {
                            controller.remove_peer(route).await?;
                        }
                        Ok(())
                    }
                    klights_network_api::PeerRoute::Direct(route) => {
                        crate::wireguard::remove_unencrypted_direct_route(&self.rt, route).await
                    }
                }
            }
            .await;
            result.map_err(|error| klights_network_api::PeerRouterError::remove(error.to_string()))
        })
    }
}
