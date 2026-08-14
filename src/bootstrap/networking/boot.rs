//! Mode-aware network boot dispatcher (F2-01).
//!
//! `NetworkPlane` (root mode) and `RootlessNetworkPlane` differ on what they
//! touch at boot: root takes the host bridge and selected peer-route dataplane
//! state, while rootless allocates the local pod subnet and prepares the same
//! bridge/veth/nftables model inside the user network namespace. Putting the
//! choice behind one enum keeps the mode decision at a single boundary instead
//! of scattering `if rootless` checks across bootstrap, controllers, and nft
//! code.

use anyhow::Result;
use std::sync::Arc;

use klights_networking::dataplane_health::DataplaneHealth;
use klights_networking::rootless::{
    RootlessNetworkBoot, RootlessNetworkPlane, RootlessNetworkStores,
};
use klights_types::PodSubnet;

use super::NetworkPlane;

pub(crate) struct NetworkBootStores {
    pub(crate) subnet_allocation: Arc<dyn klights_leader_api::LeaderNodeSubnetAllocation>,
    pub(crate) topology: Arc<dyn klights_leader_api::LeaderNetworkTopologyQuery>,
    pub(crate) pod_network_cache: Arc<dyn klights_node_store::PodNetworkCache>,
    pub(crate) pod_ipam: Arc<dyn klights_node_store::PodIpamStore>,
    pub(crate) pod_runtime: Arc<dyn klights_node_store::PodRuntimeStore>,
    pub(crate) assignment_publisher: Arc<dyn klights_network_api::PodNetworkAssignmentPublisher>,
}

impl NetworkBootStores {
    pub(crate) fn new(
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
    /// Dispatch on the validated focused network mode and run its boot path.
    pub(crate) async fn boot(
        cfg: &super::NetworkBootConfig,
        stores: NetworkBootStores,
        cancel: tokio_util::sync::CancellationToken,
        task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Result<Self> {
        match cfg.mode() {
            super::NetworkMode::Root => {
                let plane = NetworkPlane::boot(cfg, stores, cancel, task_supervisor).await?;
                Ok(Self::Root(plane))
            }
            super::NetworkMode::Rootless => {
                let plane = boot_rootless(cfg, stores, cancel, task_supervisor).await?;
                Ok(Self::Rootless(plane))
            }
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

    /// Dataplane health snapshot. Callers wire this into node conditions
    /// so that WireGuard/pasta failures surface as `NetworkUnavailable=True`.
    pub fn health(&self) -> &DataplaneHealth {
        match self {
            Self::Root(plane) => plane.health(),
            Self::Rootless(plane) => plane.health(),
        }
    }
}

pub(crate) async fn boot_rootless(
    cfg: &super::NetworkBootConfig,
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
        klights_networking::wireguard::WireGuardBootConfig::try_new(
            cfg.wireguard_device(),
            cfg.wireguard_key_path(),
            cfg.wireguard_port(),
        )
        .map_err(anyhow::Error::new)?,
        klights_networking::PodLinkMtu::try_new(super::pod_link_mtu_for_encryption(
            cfg.encryption(),
        ))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn rootless_test_config(node_name: &str) -> crate::KlightsConfig {
        let ns = "klights";
        let data_root = std::env::temp_dir().join(format!("klights-network-boot-{node_name}"));
        crate::KlightsConfig {
            bridge_name: ns.to_string(),
            pod_subnet: "10.42.0.0/16".to_string(),
            cluster_cidr: "10.42.0.0/16".to_string(),
            service_cidr: "10.43.128.0/17".to_string(),
            tls_port: 7443,
            api_fqdn: None,
            log_file: None,
            containerd_namespace: ns.to_string(),
            containerd_socket: None,
            registry_proxy: klights_kubelet::registry_proxy::RegistryProxyConfig::from_inputs(
                false, None, false,
            )
            .unwrap(),
            node_name: node_name.to_string(),
            node_ip: None,
            anonymous_auth: true,
            dataplane_encryption: klights_networking::wireguard::DataplaneEncryption::Disabled,
            external_endpoint: None,
            worker_dataplane_no_ingress: false,
            wireguard_device: klights_networking::wireguard::DEFAULT_WIREGUARD_DEVICE.to_string(),
            wireguard_port: klights_networking::wireguard::DEFAULT_WIREGUARD_PORT,
            cluster_db_path: data_root
                .clone()
                .join("db")
                .join("sqlite")
                .join("cluster.db"),
            node_db_path: data_root.clone().join("db").join("sqlite").join("node.db"),
            data_root,
            api_slow_log_threshold: std::time::Duration::from_millis(
                crate::bootstrap::config::DEFAULT_API_SLOW_LOG_MS,
            ),
            node_not_ready_pod_eviction_grace: std::time::Duration::ZERO,
            max_watch_events: crate::bootstrap::config::DEFAULT_MAX_WATCH_EVENTS,
            gc_interval: std::time::Duration::from_secs(
                crate::bootstrap::config::DEFAULT_GC_INTERVAL_SECONDS,
            ),
            in_memory: true,
            datastore_backend: crate::bootstrap::cluster_store::backend_kind::BackendKind::Sqlite,
            node_local_backend: crate::bootstrap::cluster_store::backend_kind::BackendKind::Sqlite,
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
    ) -> crate::bootstrap::node_store::NodeLocalStores {
        crate::bootstrap::node_store::open_node_local(
            crate::bootstrap::cluster_store::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            "sqlite:network-boot-test",
        )
        .await
        .expect("open node-local test db")
    }

    fn network_port_for_test(
        db: crate::datastore::sqlite::Datastore,
    ) -> Arc<
        crate::bootstrap::composition_adapters::leader_topology_cleanup_adapter::ClusterStoreLeaderNetwork,
    >{
        let db: crate::datastore::DatastoreHandle = Arc::new(db);
        Arc::new(
            crate::bootstrap::composition_adapters::leader_topology_cleanup_adapter::ClusterStoreLeaderNetwork::new(
                db.clone(),
                Arc::new(crate::bootstrap::outbox_apply_adapter::BackendProposalFixture::new(db)),
                crate::bootstrap::composition_adapters::authority_adapter::always_leader_watch(),
            ),
        )
    }

    #[tokio::test]
    async fn network_boot_dispatches_rootless_mode_to_rootless_plane() {
        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();
        let cfg = rootless_test_config("rootless-dispatch-node");
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let node_local = node_local_for_test(supervisor.clone()).await;
        let node_network = Arc::new(node_local);
        let assignment_bus = Arc::new(klights_networking::PodNetworkAssignmentBus::new());
        let cancel = tokio_util::sync::CancellationToken::new();
        let network = network_port_for_test(db.clone());
        let subnet_allocation: Arc<dyn klights_leader_api::LeaderNodeSubnetAllocation> =
            network.clone();
        let topology: Arc<dyn klights_leader_api::LeaderNetworkTopologyQuery> = network;
        let focused = crate::bootstrap::networking::NetworkBootConfig::try_new(
            crate::bootstrap::networking::NetworkMode::Rootless,
            &cfg.bridge_name,
            &cfg.node_name,
            &cfg.cluster_cidr,
            "192.168.1.6",
            cfg.dataplane_encryption,
            cfg.wireguard_device.clone(),
            "/tmp/klights-test-wireguard.key",
            cfg.wireguard_port,
        )
        .expect("focused test config");

        let boot = NetworkBoot::boot(
            &focused,
            NetworkBootStores::new(
                subnet_allocation,
                topology,
                node_network.pod_network_cache(),
                node_network.pod_ipam(),
                node_network.pod_runtime(),
                assignment_bus,
            ),
            cancel,
            supervisor,
        )
        .await
        .expect("rootless dispatch must succeed");

        let _peer_router = boot.peer_router();
        let row = db
            .get_node_subnet(&cfg.node_name)
            .await
            .expect("get_node_subnet must succeed")
            .expect("rootless boot must allocate the local subnet via shared IPAM");
        assert_eq!(row.node_name.as_str(), cfg.node_name);
    }
}
