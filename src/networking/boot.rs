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

use crate::bootstrap::NodeMode;
use crate::control_plane::client::LeaderApiClient;
use crate::datastore::node_local::NodeLocalHandle;
use crate::networking::dataplane_health::DataplaneHealth;
use crate::networking::{NetworkPlane, PodSubnet, RootlessNetworkPlane};

pub enum NetworkBoot {
    Root(Arc<NetworkPlane>),
    Rootless(Arc<RootlessNetworkPlane>),
}

impl NetworkBoot {
    /// Dispatch on `NodeMode` and run the matching boot path.
    pub async fn boot(
        node_mode: &NodeMode,
        cfg: &crate::KlightsConfig,
        cluster_api: Arc<dyn LeaderApiClient>,
        node_local: NodeLocalHandle,
        node_ip: &str,
        cancel: tokio_util::sync::CancellationToken,
        task_supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
    ) -> Result<Self> {
        match node_mode {
            NodeMode::Root => {
                let plane = NetworkPlane::boot(
                    cfg,
                    cluster_api,
                    node_local,
                    node_ip,
                    cancel,
                    task_supervisor,
                )
                .await?;
                Ok(Self::Root(plane))
            }
            NodeMode::Rootless { .. } => {
                let plane = RootlessNetworkPlane::boot(
                    cfg,
                    cluster_api,
                    node_local,
                    node_ip,
                    cancel,
                    task_supervisor,
                )
                .await?;
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
            Self::Rootless(plane) => plane.local_subnet().subnet,
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
    use crate::bootstrap::NodeMode;

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
            vxlan_vni: 1,
            vxlan_port: 4789,
            vxlan_device: "klights.vxlan".to_string(),
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
        supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
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
        let supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let node_local = node_local_for_test(supervisor.clone()).await;
        let cancel = tokio_util::sync::CancellationToken::new();
        let user_netns = std::path::PathBuf::from("/proc/self/ns/user");
        let mode = NodeMode::Rootless {
            user_netns,
            rootlesskit_pid: 0,
        };

        let boot = NetworkBoot::boot(
            &mode,
            &cfg,
            cluster_api_for_test(db.clone(), &cfg.node_name),
            node_local,
            "192.168.1.6",
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
        assert!(
            row.vtep_mac.is_none(),
            "rootless dispatch must not write vtep_mac"
        );
    }
}
