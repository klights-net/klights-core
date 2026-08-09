use anyhow::{Context, Result};
use klights_networking::{RootDatapath, RootPeerDataplane, RootPeerDataplaneBoot};
use klights_types::{NodeName, PodSubnet};

use std::net::Ipv4Addr;
use std::sync::Arc;

use super::boot::NetworkBootStores;

/// Concrete root-mode networking implementation used by klights runtime.
pub struct NetworkPlane {
    root: RootDatapath,
    pod_network_cache: Arc<dyn klights_node_store::PodNetworkCache>,
    pod_ipam: Arc<dyn klights_node_store::PodIpamStore>,
    pod_runtime: Arc<dyn klights_node_store::PodRuntimeStore>,
    assignment_publisher: Arc<dyn klights_network_api::PodNetworkAssignmentPublisher>,
    sandbox_operations: klights_networking::SandboxOperationLocks,
    my_node: NodeName,
    host_ip: Ipv4Addr,
    peer: Arc<RootPeerDataplane>,
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
}

impl NetworkPlane {
    /// Boot the shared networking plane. Opens one rtnetlink connection,
    /// prepares the local bridge/CNI datapath, and initializes the selected
    /// cross-node dataplane. WireGuard is the default encrypted dataplane;
    /// explicit direct-route mode installs only kernel routes.
    pub(crate) async fn boot(
        cfg: &super::NetworkBootConfig,
        stores: super::boot::NetworkBootStores,
        cancel: tokio_util::sync::CancellationToken,
        task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Result<Arc<Self>> {
        let NetworkBootStores {
            subnet_allocation,
            topology,
            pod_network_cache,
            pod_ipam,
            pod_runtime,
            assignment_publisher,
        } = stores;
        let my_node = cfg.node().clone();
        let host_ip = cfg.host_ip();

        let local_subnet = klights_networking::NodeSubnetAllocator::new(
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
                "failed to allocate local node subnet for {} at {}",
                cfg.node(),
                cfg.host_ip()
            )
        })?;

        let root = RootDatapath::boot(
            cfg.bridge().clone(),
            local_subnet,
            klights_networking::PodLinkMtu::try_new(super::pod_link_mtu_for_encryption(
                cfg.encryption(),
            ))
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
                    klights_networking::wireguard::WireGuardBootConfig::try_new(
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

        let plane = Arc::new(Self {
            root,
            pod_network_cache,
            pod_ipam,
            pod_runtime,
            assignment_publisher,
            sandbox_operations: klights_networking::SandboxOperationLocks::default(),
            my_node,
            host_ip,
            peer,
            task_supervisor: task_supervisor.clone(),
        });

        Ok(plane)
    }

    pub fn local_pod_subnet(&self) -> PodSubnet {
        self.root.pod_subnet()
    }

    /// Dataplane health snapshot. WireGuard failures are recorded here
    /// so callers can set `NetworkUnavailable=True` on the Node.
    pub fn health(&self) -> &klights_networking::dataplane_health::DataplaneHealth {
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
        klights_networking::add(klights_networking::CniAddArgs {
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
        klights_networking::del(
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

// Hybrid peer boot invariants (boot ordering, overlay avoidance,
// peer-endpoint arms) are enforced by
// `tests/source_guard_networking_invariants.py`,
// run as part of `./build.sh`.

#[cfg(test)]
mod stale_route_tests {
    use super::*;

    #[tokio::test]
    async fn root_cni_del_without_allocation_does_not_resolve_missing_bridge() {
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let node_local = crate::bootstrap::node_store::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor.clone(),
            None,
            "sqlite:root-cni-del-test",
        )
        .await
        .expect("open node-local test store");
        let node_network = Arc::new(node_local);
        let assignment_bus = Arc::new(klights_networking::PodNetworkAssignmentBus::new());
        let (connection, handle, _) = rtnetlink::new_connection().expect("open rtnetlink");
        let cancel = tokio_util::sync::CancellationToken::new();
        let connection_cancel = cancel.clone();
        let connection = supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Network,
                "test_root_cni_del_rtnetlink_connection",
                async move {
                    tokio::select! {
                        _ = connection => {}
                        _ = connection_cancel.cancelled() => {}
                    }
                },
            )
            .await
            .expect("spawn rtnetlink test connection");
        let root = RootDatapath::from_open_connection(
            handle,
            connection,
            klights_networking::BridgeName::parse("missing-cni0").unwrap(),
            PodSubnet::parse("10.42.1.0/24").unwrap(),
            klights_networking::PodLinkMtu::try_new(1500).unwrap(),
        );
        let peer = Arc::new(RootPeerDataplane::direct_for_test(
            root.handle().clone(),
            root.pod_subnet(),
            "missing-wg0",
            supervisor.clone(),
        ));
        let plane = NetworkPlane {
            root,
            pod_network_cache: node_network.pod_network_cache(),
            pod_ipam: node_network.pod_ipam(),
            pod_runtime: node_network.pod_runtime(),
            assignment_publisher: assignment_bus,
            sandbox_operations: klights_networking::SandboxOperationLocks::default(),
            my_node: NodeName::parse("node-a").unwrap(),
            host_ip: Ipv4Addr::new(192, 0, 2, 1),
            peer,
            task_supervisor: supervisor,
        };

        let result = NetworkPlane::cni_del(&plane, "already-gone").await;
        cancel.cancel();
        plane.root.shutdown();
        assert!(
            result.is_ok(),
            "idempotent DEL without an allocation must not require the bridge: {result:?}"
        );
    }
}
