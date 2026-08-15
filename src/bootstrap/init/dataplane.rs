//! Dataplane metadata helpers extracted from runtime.rs (R3 refactor).

use crate::KlightsConfig;
use crate::bootstrap::NodeMode;

pub async fn local_join_dataplane_metadata(
    config: &KlightsConfig,
    node_mode: &NodeMode,
    node_ip: &str,
    supervisor: &klights_supervisor::TaskSupervisor,
) -> anyhow::Result<klights_leader_rpc::client::JoinDataplaneMetadata> {
    let _ = node_ip;
    let identity = local_dataplane_identity(config, node_mode, supervisor).await?;
    Ok(klights_leader_rpc::client::JoinDataplaneMetadata {
        public_key: identity.public_key().map(str::to_string),
        endpoint: config.external_endpoint.clone().unwrap_or_default(),
        port: identity.port(),
        mode: match identity.mode() {
            klights_networking::wireguard::DataplaneMode::Root => {
                klights_leader_api::NetworkNodeMode::Root
            }
            klights_networking::wireguard::DataplaneMode::Rootless => {
                klights_leader_api::NetworkNodeMode::Rootless
            }
        },
        encryption: match identity.encryption() {
            klights_networking::wireguard::DataplaneEncryption::Enabled => {
                klights_leader_api::DataplaneEncryption::WireGuard
            }
            klights_networking::wireguard::DataplaneEncryption::Disabled => {
                klights_leader_api::DataplaneEncryption::Direct
            }
        },
    })
}

async fn local_dataplane_identity(
    config: &KlightsConfig,
    node_mode: &NodeMode,
    supervisor: &klights_supervisor::TaskSupervisor,
) -> anyhow::Result<klights_networking::wireguard::LocalDataplaneIdentity> {
    let mode = match node_mode {
        NodeMode::Root => klights_networking::wireguard::DataplaneMode::Root,
        NodeMode::Rootless { .. } => klights_networking::wireguard::DataplaneMode::Rootless,
    };
    klights_networking::wireguard::LocalDataplaneIdentity::load(
        &config.data_root.join("etc/wireguard-private.key"),
        mode,
        config.dataplane_encryption,
        config.wireguard_port,
        supervisor,
    )
    .await
}

pub async fn publish_local_dataplane_metadata_self_heal_with_resource_reads(
    resource_reads: &dyn klights_cluster_store::ClusterResourceRead,
    command: &dyn klights_leader_api::LeaderNetworkTopologyCommand,
    config: &KlightsConfig,
    node_mode: &NodeMode,
    supervisor: &klights_supervisor::TaskSupervisor,
) -> anyhow::Result<bool> {
    let identity = local_dataplane_identity(config, node_mode, supervisor).await?;
    klights_networking::wireguard::publish_local_dataplane_metadata_self_heal(
        resource_reads,
        command,
        &config.node_name,
        config.external_endpoint.as_deref(),
        &identity,
    )
    .await
}

pub async fn enqueue_worker_dataplane_metadata_outbox(
    outbox: Option<&klights_kubelet::node_outbox::Outbox>,
    node_name: &str,
    dataplane: &klights_leader_rpc::client::JoinDataplaneMetadata,
) -> anyhow::Result<()> {
    klights_kubelet::node_outbox::enqueue_node_dataplane_metadata(
        outbox,
        node_name,
        dataplane.mode,
        dataplane.encryption,
        dataplane.public_key.clone(),
        dataplane.endpoint.clone(),
        dataplane.port,
        klights_supervisor::system_time_epoch_millis(klights_supervisor::SystemWallClock::now()),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_dataplane_command(
        db: &klights_cluster_datastore::sqlite::embedded::Datastore,
    ) -> crate::bootstrap::composition_adapters::leader_topology_cleanup_adapter::ClusterStoreLeaderNetwork{
        let canonical = db.clone();
        crate::bootstrap::composition_adapters::leader_topology_cleanup_adapter::ClusterStoreLeaderNetwork::new(
            db.focused_read_store(),
            {
                std::sync::Arc::new(crate::bootstrap::outbox_apply_adapter::BackendProposalFixture::new(std::sync::Arc::new(canonical.clone()), std::sync::Arc::new(canonical.clone()), canonical.focused_read_store()))
            },
            crate::bootstrap::composition_adapters::authority_adapter::always_leader_watch(),
        )
    }

    #[tokio::test]
    async fn local_join_dataplane_metadata_without_external_endpoint_does_not_advertise_internal_ip()
     {
        let mut config = crate::KlightsConfig::test_default();
        config.node_name = "mn-worker".to_string();
        config.external_endpoint = None;
        config.dataplane_encryption = klights_networking::wireguard::DataplaneEncryption::Disabled;
        let supervisor = klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        );

        let metadata = local_join_dataplane_metadata(
            &config,
            &crate::bootstrap::NodeMode::Root,
            "172.31.11.2",
            &supervisor,
        )
        .await
        .expect("join metadata can rely on leader-observed endpoint");

        assert_eq!(
            metadata.endpoint, "",
            "join metadata must not advertise KLIGHTS_NODE_IP as an external endpoint"
        );
    }

    #[tokio::test]
    async fn self_heal_publishes_node_dataplane_from_registered_external_ip() {
        let db = crate::bootstrap::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
        let mut config = crate::KlightsConfig::test_default();
        config.node_name = "leader-a".to_string();
        config.external_endpoint = None;
        config.dataplane_encryption = klights_networking::wireguard::DataplaneEncryption::Disabled;
        let supervisor = klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        );

        db.create_resource(
            "v1",
            "Node",
            None,
            "leader-a",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "leader-a"},
                "status": {
                    "addresses": [
                        {"type": "ExternalIP", "address": "198.51.100.47"}
                    ]
                }
            }),
        )
        .await
        .unwrap();

        let published = publish_local_dataplane_metadata_self_heal_with_resource_reads(
            db.focused_read_store().as_ref(),
            &test_dataplane_command(&db),
            &config,
            &NodeMode::Root,
            &supervisor,
        )
        .await
        .expect("self-heal publish must succeed");
        assert!(
            published,
            "self-heal must publish when an ExternalIP exists"
        );

        let stored = db
            .get_node_dataplane("leader-a")
            .await
            .unwrap()
            .expect("node_dataplane row must exist after self-heal");
        assert_eq!(stored.node_name, "leader-a");
        assert_eq!(stored.endpoint.to_string(), "198.51.100.47");
    }

    #[tokio::test]
    async fn self_heal_is_noop_without_resolvable_endpoint() {
        let db = crate::bootstrap::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
        let mut config = crate::KlightsConfig::test_default();
        config.node_name = "leader-a".to_string();
        config.external_endpoint = None;
        let supervisor = klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        );

        let published = publish_local_dataplane_metadata_self_heal_with_resource_reads(
            db.focused_read_store().as_ref(),
            &test_dataplane_command(&db),
            &config,
            &NodeMode::Root,
            &supervisor,
        )
        .await
        .expect("self-heal must not error when no endpoint is resolvable");
        assert!(!published, "self-heal must be a no-op without an endpoint");
        assert!(
            db.get_node_dataplane("leader-a").await.unwrap().is_none(),
            "no node_dataplane row should be written without an endpoint"
        );
    }
}
