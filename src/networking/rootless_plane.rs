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

use crate::networking::BridgeName;
use crate::networking::dataplane_health::DataplaneHealth;
use klights_types::{NodeName, PodSubnet};

pub struct RootlessNetworkPlane {
    pod_network_cache: Arc<dyn klights_node_store::PodNetworkCache>,
    pod_ipam: Arc<dyn klights_node_store::PodIpamStore>,
    pod_runtime: Arc<dyn klights_node_store::PodRuntimeStore>,
    assignment_publisher: Arc<dyn klights_network_api::PodNetworkAssignmentPublisher>,
    sandbox_operations: crate::networking::cni::SandboxOperationLocks,
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
    wireguard: OnceLock<Arc<crate::networking::wireguard::WireGuardController>>,
    health: DataplaneHealth,
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
}

impl RootlessNetworkPlane {
    /// Boot the rootless network plane. Allocates the node-local pod subnet
    /// through the shared IPAM and returns; intentionally skips boot-time
    /// host-network mutation. The bridge is created
    /// lazily on the first non-hostNetwork CNI ADD so unit tests and idle
    /// rootless starts do not require netlink mutations until pods need them.
    pub(crate) async fn boot(
        cfg: &crate::networking::NetworkBootConfig,
        stores: crate::networking::boot::NetworkBootStores,
        cancel: tokio_util::sync::CancellationToken,
        task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Result<Arc<Self>> {
        let crate::networking::boot::NetworkBootStores {
            subnet_allocation,
            topology,
            pod_network_cache,
            pod_ipam,
            pod_runtime,
            assignment_publisher,
        } = stores;
        let bridge = cfg.bridge().clone();
        let my_node = cfg.node().clone();
        let host_ip = cfg.host_ip();
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
        let local_subnet = crate::networking::subnet_allocator::NodeSubnetAllocator::new(
            subnet_allocation,
            topology,
            task_supervisor.clone(),
        )
        .allocate_or_reuse_existing(
            cfg.node().as_ref(),
            &cfg.cluster_cidr().to_string(),
            &cfg.host_ip().to_string(),
        )
        .await
        .with_context(|| {
            format!(
                "failed to allocate local rootless node subnet for {} at {}",
                cfg.node(),
                cfg.host_ip()
            )
        })?;
        let plane = Arc::new(Self {
            pod_network_cache,
            pod_ipam,
            pod_runtime,
            assignment_publisher,
            sandbox_operations: crate::networking::cni::SandboxOperationLocks::default(),
            rt: handle,
            _rt_conn: rt_conn,
            local_subnet,
            bridge,
            pod_link_mtu: crate::networking::pod_link_mtu_for_encryption(cfg.encryption()),
            bridge_idx: OnceLock::new(),
            my_node,
            host_ip,
            wireguard_device: cfg.wireguard_device().to_string(),
            wireguard_idx: OnceLock::new(),
            wireguard: OnceLock::new(),
            health: DataplaneHealth::new_healthy(),
            task_supervisor: task_supervisor.clone(),
        });
        if cfg.encryption() == crate::networking::wireguard::DataplaneEncryption::Enabled
            && let Err(err) = plane.ensure_wireguard_enabled(cfg, cancel).await
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
            Err(err) if crate::networking::is_nl_eexist_error(&err) => Ok(()),
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

    pub(crate) async fn prepare_service_routing_bridge(&self) -> Result<()> {
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
                    Err(err) if crate::networking::is_nl_eexist_error(&err) => {}
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
            .mtu(crate::networking::wireguard::WIREGUARD_MTU)
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
        cfg: &crate::networking::NetworkBootConfig,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<()> {
        self.ensure_wireguard_once().await?;
        let identity = crate::networking::wireguard::WireGuardIdentity::load_or_create(
            cfg.wireguard_key_path(),
            self.task_supervisor.as_ref(),
        )
        .await?;
        let config = crate::networking::wireguard::WireGuardDeviceConfig::try_new(
            self.wireguard_device.clone(),
            identity.private_key().clone(),
            cfg.wireguard_port(),
        )?;
        let controller = Arc::new(
            crate::networking::wireguard::WireGuardController::open(
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
        crate::networking::rootless::pasta::verify_wireguard_udp_port(
            cfg.wireguard_port(),
            self.task_supervisor.as_ref(),
        )
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
            crate::networking::cni::add(crate::networking::cni::CniAddArgs {
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
            crate::networking::cni::del(
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
                        super::plane::apply_wireguard_peer_route_with_rollback(
                            || Box::pin(controller.apply_peer(route)),
                            || {
                                Box::pin(crate::networking::wireguard::apply_wireguard_pod_route(
                                    &self.rt,
                                    idx,
                                    route,
                                    self.local_subnet.bridge_ip(),
                                ))
                            },
                            || {
                                Box::pin(crate::networking::wireguard::remove_wireguard_pod_route(
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
                        crate::networking::wireguard::apply_unencrypted_direct_route(
                            &self.rt, route,
                        )
                        .await
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
                        crate::networking::wireguard::remove_wireguard_pod_route(
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
                        crate::networking::wireguard::remove_unencrypted_direct_route(
                            &self.rt, route,
                        )
                        .await
                    }
                }
            }
            .await;
            result.map_err(|error| klights_network_api::PeerRouterError::remove(error.to_string()))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::config::KlightsConfig;
    use crate::networking::NetworkBootConfig;

    fn rootless_test_config(node_name: &str) -> KlightsConfig {
        let ns = "klights";
        KlightsConfig {
            bridge_name: ns.to_string(),
            pod_subnet: "10.42.0.0/16".to_string(),
            cluster_cidr: "10.42.0.0/16".to_string(),
            service_cidr: "10.43.128.0/17".to_string(),
            tls_port: 7443,
            api_fqdn: None,
            log_file: None,
            containerd_namespace: ns.to_string(),
            containerd_socket: None,
            node_name: node_name.to_string(),
            node_ip: None,
            anonymous_auth: true,
            dataplane_encryption: crate::networking::wireguard::DataplaneEncryption::Disabled,
            external_endpoint: None,
            worker_dataplane_no_ingress: false,
            wireguard_device: crate::networking::wireguard::DEFAULT_WIREGUARD_DEVICE.to_string(),
            wireguard_port: crate::networking::wireguard::DEFAULT_WIREGUARD_PORT,
            cluster_db_path: crate::paths::test_data_root_path(ns)
                .join("db")
                .join("sqlite")
                .join("cluster.db"),
            node_db_path: crate::paths::test_data_root_path(ns)
                .join("db")
                .join("sqlite")
                .join("node.db"),
            data_root: crate::paths::test_data_root_path(ns),
            api_slow_log_threshold: std::time::Duration::from_millis(
                crate::bootstrap::config::DEFAULT_API_SLOW_LOG_MS,
            ),
            node_not_ready_pod_eviction_grace: std::time::Duration::ZERO,
            max_watch_events: crate::bootstrap::config::DEFAULT_MAX_WATCH_EVENTS,
            gc_interval: std::time::Duration::from_secs(
                crate::bootstrap::config::DEFAULT_GC_INTERVAL_SECONDS,
            ),
            in_memory: true,
            db_encryption: crate::DbEncryption::Disabled,
            db_key_file: None,
            datastore_backend: crate::datastore::backend_kind::BackendKind::Sqlite,
            node_local_backend: crate::datastore::backend_kind::BackendKind::Sqlite,
            oidc_issuer_url: None,
            oidc_client_id: None,
            oidc_username_claim: "sub".to_string(),
            oidc_groups_claim: "groups".to_string(),
            oidc_groups_prefix: String::new(),
            oidc_ca_bundle: None,
            webhook_auth_url: None,
            webhook_auth_client_cert: None,
            webhook_auth_client_key: None,
            webhook_auth_audiences: String::new(),
            webhook_auth_cache_authorized_ttl_secs: 300,
            webhook_auth_cache_unauthorized_ttl_secs: 30,
            webhook_auth_ca_bundle: None,
        }
    }

    async fn node_local_for_test(
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> crate::datastore::node_local::NodeLocalHandle {
        crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:rootless-plane-test",
        )
        .await
        .expect("open node-local test db")
    }

    fn cluster_api_for_test(
        db: crate::datastore::sqlite::Datastore,
        node_name: &str,
    ) -> Arc<crate::control_plane::client::local::LocalApiClient> {
        Arc::new(crate::control_plane::client::local::LocalApiClient::new(
            Arc::new(db),
            node_name.to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ))
    }

    fn focused_test_config(cfg: &KlightsConfig, node_ip: &str) -> NetworkBootConfig {
        NetworkBootConfig::try_new(
            crate::networking::NetworkMode::Rootless,
            &cfg.bridge_name,
            &cfg.node_name,
            &cfg.cluster_cidr,
            node_ip,
            cfg.dataplane_encryption,
            cfg.wireguard_device.clone(),
            "/tmp/klights-rootless-plane-test.key",
            cfg.wireguard_port,
        )
        .expect("focused rootless test config")
    }

    #[tokio::test]
    async fn boot_rootless_does_not_create_vxlan_or_write_vtep() {
        let db = crate::datastore::test_support::in_memory().await;
        let cfg = rootless_test_config("rootless-node-a");

        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let node_local = node_local_for_test(supervisor.clone()).await;
        let node_network =
            crate::datastore::node_local::network_adapter::NodeLocalNetworkAdapter::new(node_local);
        let assignment_bus =
            Arc::new(crate::networking::pod_network_events::PodNetworkAssignmentBus::new());
        let cancel = tokio_util::sync::CancellationToken::new();
        let cluster_api = cluster_api_for_test(db.clone(), &cfg.node_name);
        let subnet_allocation: Arc<dyn klights_leader_api::LeaderNodeSubnetAllocation> =
            cluster_api.clone();
        let topology: Arc<dyn klights_leader_api::LeaderNetworkTopologyQuery> = cluster_api;
        let focused = focused_test_config(&cfg, "192.168.1.5");
        let plane = RootlessNetworkPlane::boot(
            &focused,
            crate::networking::boot::NetworkBootStores::new(
                subnet_allocation,
                topology,
                node_network.clone(),
                node_network.clone(),
                node_network,
                assignment_bus,
            ),
            cancel,
            supervisor,
        )
        .await
        .expect("rootless boot must succeed");

        // Local subnet allocated through the shared IPAM path.
        let row = db
            .get_node_subnet(&cfg.node_name)
            .await
            .expect("get_node_subnet must succeed")
            .expect("rootless boot must record a node_subnets row");
        assert_eq!(*plane.local_subnet(), row.subnet);

        assert_eq!(row.node_name.as_str(), cfg.node_name);
    }

    #[tokio::test]
    async fn rootless_datapath_host_network_returns_detected_host_ip() {
        use klights_network_api::Datapath;

        let db = crate::datastore::test_support::in_memory().await;
        let cfg = rootless_test_config("rootless-hostnet-node");
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let node_local = node_local_for_test(supervisor.clone()).await;
        let node_network =
            crate::datastore::node_local::network_adapter::NodeLocalNetworkAdapter::new(node_local);
        let assignment_bus =
            Arc::new(crate::networking::pod_network_events::PodNetworkAssignmentBus::new());
        let cancel = tokio_util::sync::CancellationToken::new();
        let cluster_api = cluster_api_for_test(db.clone(), &cfg.node_name);
        let subnet_allocation: Arc<dyn klights_leader_api::LeaderNodeSubnetAllocation> =
            cluster_api.clone();
        let topology: Arc<dyn klights_leader_api::LeaderNetworkTopologyQuery> = cluster_api;
        let focused = focused_test_config(&cfg, "192.168.77.9");
        let plane = RootlessNetworkPlane::boot(
            &focused,
            crate::networking::boot::NetworkBootStores::new(
                subnet_allocation,
                topology,
                node_network.clone(),
                node_network.clone(),
                node_network,
                assignment_bus,
            ),
            cancel,
            supervisor,
        )
        .await
        .expect("rootless boot must succeed");

        let network = plane
            .cni_add(
                klights_network_api::CniAddRequest::try_new(
                    "hostnet-sandbox",
                    klights_types::PodIdentity::new("default", "hostnet-pod", "hostnet-uid"),
                    "/proc/self/ns/net",
                    "/proc/self/ns/net",
                    true,
                )
                .expect("valid host-network CNI request"),
            )
            .await
            .expect("host-network CNI add should not use the Phase-2 stub");

        assert_eq!(
            network.ip_addr(),
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(192, 168, 77, 9))
        );
        assert_eq!(
            plane.host_ip().await.expect("host_ip must be available"),
            network.ip_addr()
        );
    }

    // Rootless datapath invariants (Datapath impl, cni::add/del,
    // ensure_bridge_once) are enforced by the base-repo source guard run by
    // `./build.sh`.

    #[test]
    fn rootless_plane_has_explicit_service_router_bridge_preparation_step() {
        let _ordered_step = RootlessNetworkPlane::prepare_service_routing_bridge;
    }

    #[tokio::test]
    async fn rootless_plane_exposes_dataplane_health_after_boot() {
        let db = crate::datastore::test_support::in_memory().await;
        let cfg = rootless_test_config("rootless-health-node");
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let node_local = node_local_for_test(supervisor.clone()).await;
        let node_network =
            crate::datastore::node_local::network_adapter::NodeLocalNetworkAdapter::new(node_local);
        let assignment_bus =
            Arc::new(crate::networking::pod_network_events::PodNetworkAssignmentBus::new());
        let cancel = tokio_util::sync::CancellationToken::new();
        let cluster_api = cluster_api_for_test(db.clone(), &cfg.node_name);
        let subnet_allocation: Arc<dyn klights_leader_api::LeaderNodeSubnetAllocation> =
            cluster_api.clone();
        let topology: Arc<dyn klights_leader_api::LeaderNetworkTopologyQuery> = cluster_api;
        let focused = focused_test_config(&cfg, "192.168.1.5");
        let plane = RootlessNetworkPlane::boot(
            &focused,
            crate::networking::boot::NetworkBootStores::new(
                subnet_allocation,
                topology,
                node_network.clone(),
                node_network.clone(),
                node_network,
                assignment_bus,
            ),
            cancel,
            supervisor,
        )
        .await
        .expect("rootless boot must succeed");

        // With encryption disabled, health must be healthy (disabled is a
        // valid explicit choice, not a failure).
        let status = plane.health().status();
        assert!(
            status.is_healthy(),
            "disabled encryption must leave health healthy, got {status:?}"
        );
    }
}
