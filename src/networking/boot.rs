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

use crate::networking::dataplane_health::DataplaneHealth;
use crate::networking::{NetworkPlane, RootlessNetworkPlane};
use klights_types::PodSubnet;

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
        cfg: &crate::networking::NetworkBootConfig,
        stores: NetworkBootStores,
        cancel: tokio_util::sync::CancellationToken,
        task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Result<Self> {
        match cfg.mode() {
            crate::networking::NetworkMode::Root => {
                let plane = NetworkPlane::boot(cfg, stores, cancel, task_supervisor).await?;
                Ok(Self::Root(plane))
            }
            crate::networking::NetworkMode::Rootless => {
                let plane =
                    RootlessNetworkPlane::boot(cfg, stores, cancel, task_supervisor).await?;
                Ok(Self::Rootless(plane))
            }
        }
    }

    /// Borrow the root-mode `NetworkPlane` if present. Returns `None` in
    /// rootless mode.
    pub fn root_plane(&self) -> Option<&Arc<NetworkPlane>> {
        match self {
            Self::Root(p) => Some(p),
            Self::Rootless(_) => None,
        }
    }

    /// Borrow the rootless-mode plane if present. Returns `None` in root mode.
    /// Phase 2 reconcilers (peer route install, hostport publication) attach
    /// here.
    pub fn rootless_plane(&self) -> Option<&Arc<RootlessNetworkPlane>> {
        match self {
            Self::Rootless(p) => Some(p),
            Self::Root(_) => None,
        }
    }

    pub fn local_pod_subnet(&self) -> PodSubnet {
        match self {
            Self::Root(plane) => plane.local_pod_subnet(),
            Self::Rootless(plane) => *plane.local_subnet(),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn rootless_test_config(node_name: &str) -> crate::KlightsConfig {
        let ns = "klights";
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
            "sqlite:network-boot-test",
        )
        .await
        .expect("open node-local test db")
    }

    fn cluster_api_for_test(
        db: crate::datastore::sqlite::Datastore,
        node_name: &str,
    ) -> Arc<dyn crate::control_plane::client::LeaderApiClient> {
        Arc::new(crate::control_plane::client::local::LocalApiClient::new(
            Arc::new(db),
            node_name.to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ))
    }

    #[tokio::test]
    async fn network_boot_dispatches_rootless_mode_to_rootless_plane() {
        let db = crate::datastore::test_support::in_memory().await;
        let cfg = rootless_test_config("rootless-dispatch-node");
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
        let focused = crate::networking::NetworkBootConfig::try_new(
            crate::networking::NetworkMode::Rootless,
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
                node_network.clone(),
                node_network.clone(),
                node_network,
                assignment_bus,
            ),
            cancel,
            supervisor,
        )
        .await
        .expect("rootless dispatch must succeed");

        assert!(
            boot.root_plane().is_none(),
            "rootless dispatch must not return a root NetworkPlane"
        );
        let row = db
            .get_node_subnet(&cfg.node_name)
            .await
            .expect("get_node_subnet must succeed")
            .expect("rootless boot must allocate the local subnet via shared IPAM");
        assert_eq!(row.node_name.as_str(), cfg.node_name);
    }
}
