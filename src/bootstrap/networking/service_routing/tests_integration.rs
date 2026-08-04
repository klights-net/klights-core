// Integration tests — root + the `nft` binary required for verification.
// Run via `sudo -E cargo test -- --ignored networking::service_routing`.
#[cfg(test)]
mod integration_tests {
    use klights_networking::service_routing::*;
    use klights_types::{ClusterCidr, PodSubnet};
    use tokio_util::sync::CancellationToken;
    #[tokio::test]
    #[ignore = "requires root/netfilter access"]
    async fn test_worker_exits_within_100ms_on_cancel() {
        use klights_network_api::ServiceRouter;
        let cancel = CancellationToken::new();
        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .expect("in-mem datastore");
        let task_supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let cluster_api =
            std::sync::Arc::new(crate::control_plane::client::local::LocalApiClient::new(
                std::sync::Arc::new(db),
                "node-a".to_string(),
                crate::control_plane::client::local::always_leader_watch(),
            ));
        let node_local = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            task_supervisor.clone(),
            None,
            "sqlite:service-router-shutdown-test",
        )
        .await
        .expect("open node-local test db");
        let node_network = std::sync::Arc::new(node_local);
        let topology: std::sync::Arc<dyn klights_leader_api::LeaderNetworkTopologyQuery> =
            cluster_api.clone();
        let endpoint_source: std::sync::Arc<dyn klights_network_api::PodEndpointEventSource> =
            std::sync::Arc::new(klights_networking::StorePodEndpointResolver::new(
                node_network.clone(),
                node_network,
                topology,
            ));
        let resource_query: std::sync::Arc<dyn klights_leader_api::LeaderResourceQuery> =
            cluster_api.clone();
        let watch: std::sync::Arc<dyn klights_leader_api::LeaderWatch> = cluster_api;
        let rt = NftServiceRouter::boot(NftServiceRouterBoot::new(
            NftServiceRouterStores::new(
                std::sync::Arc::new(
                    crate::bootstrap::composition_adapters::networking_state_adapter::LeaderRoutingStateAdapter::new(resource_query),
                ),
                watch,
                endpoint_source,
            ),
            NftServiceRouterTableConfig::new("node-a", "klights-test-shutdown", "klights-test"),
            NftServiceRouterNetworkConfig::new(
                PodSubnet::parse("10.42.0.0/24").unwrap(),
                ClusterCidr::parse("10.42.0.0/16").unwrap(),
                ClusterCidr::parse("10.43.128.0/17").unwrap(),
                ServiceRoutingMode::new(),
            ),
            NftServiceRouterRuntime::new(
                std::time::Duration::from_millis(50),
                cancel,
                task_supervisor.clone(),
            ),
        ))
        .await
        .expect("boot router");

        let started = std::time::Instant::now();
        let _ = rt.cleanup().await;
        assert!(
            started.elapsed() < std::time::Duration::from_millis(100),
            "cleanup took {:?}",
            started.elapsed()
        );
    }
}
