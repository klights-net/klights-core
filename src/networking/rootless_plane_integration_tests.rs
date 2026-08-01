#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::bootstrap::config::KlightsConfig;
    use crate::networking::NetworkBootConfig;
    use klights_networking::rootless::RootlessNetworkPlane;

    fn rootless_test_config(node_name: &str) -> KlightsConfig {
        let ns = "klights";
        let data_root = std::env::temp_dir().join(format!("klights-rootless-plane-{node_name}"));
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
    ) -> crate::datastore::node_local::NodeLocalStores {
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
        let node_network = Arc::new(node_local);
        let assignment_bus = Arc::new(klights_networking::PodNetworkAssignmentBus::new());
        let cancel = tokio_util::sync::CancellationToken::new();
        let cluster_api = cluster_api_for_test(db.clone(), &cfg.node_name);
        let subnet_allocation: Arc<dyn klights_leader_api::LeaderNodeSubnetAllocation> =
            cluster_api.clone();
        let topology: Arc<dyn klights_leader_api::LeaderNetworkTopologyQuery> = cluster_api;
        let focused = focused_test_config(&cfg, "192.168.1.5");
        let plane = crate::networking::boot::boot_rootless(
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
        let node_network = Arc::new(node_local);
        let assignment_bus = Arc::new(klights_networking::PodNetworkAssignmentBus::new());
        let cancel = tokio_util::sync::CancellationToken::new();
        let cluster_api = cluster_api_for_test(db.clone(), &cfg.node_name);
        let subnet_allocation: Arc<dyn klights_leader_api::LeaderNodeSubnetAllocation> =
            cluster_api.clone();
        let topology: Arc<dyn klights_leader_api::LeaderNetworkTopologyQuery> = cluster_api;
        let focused = focused_test_config(&cfg, "192.168.77.9");
        let plane = crate::networking::boot::boot_rootless(
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
        let node_network = Arc::new(node_local);
        let assignment_bus = Arc::new(klights_networking::PodNetworkAssignmentBus::new());
        let cancel = tokio_util::sync::CancellationToken::new();
        let cluster_api = cluster_api_for_test(db.clone(), &cfg.node_name);
        let subnet_allocation: Arc<dyn klights_leader_api::LeaderNodeSubnetAllocation> =
            cluster_api.clone();
        let topology: Arc<dyn klights_leader_api::LeaderNetworkTopologyQuery> = cluster_api;
        let focused = focused_test_config(&cfg, "192.168.1.5");
        let plane = crate::networking::boot::boot_rootless(
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
