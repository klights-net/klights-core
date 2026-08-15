//! Dataplane metadata helpers extracted from runtime.rs (R3 refactor).

use crate::KlightsConfig;
use crate::bootstrap::NodeMode;

use super::leader_control_stream::runtime_epoch_ms;

pub async fn local_join_dataplane_metadata(
    config: &KlightsConfig,
    node_mode: &NodeMode,
    node_ip: &str,
    supervisor: &klights_supervisor::TaskSupervisor,
) -> anyhow::Result<klights_leader_rpc::client::JoinDataplaneMetadata> {
    let _ = node_ip;
    let identity = local_dataplane_identity(config, node_mode, supervisor).await?;
    Ok(klights_leader_rpc::client::JoinDataplaneMetadata {
        public_key: identity.public_key,
        endpoint: config.external_endpoint.clone().unwrap_or_default(),
        port: identity.port,
        mode: match identity.mode {
            klights_networking::wireguard::DataplaneMode::Root => {
                klights_leader_api::NetworkNodeMode::Root
            }
            klights_networking::wireguard::DataplaneMode::Rootless => {
                klights_leader_api::NetworkNodeMode::Rootless
            }
        },
        encryption: match identity.encryption {
            klights_networking::wireguard::DataplaneEncryption::Enabled => {
                klights_leader_api::DataplaneEncryption::WireGuard
            }
            klights_networking::wireguard::DataplaneEncryption::Disabled => {
                klights_leader_api::DataplaneEncryption::Direct
            }
        },
    })
}

/// Build cluster-visible dataplane metadata using an explicit external
/// endpoint (e.g. one observed from a peer or read back from the local Node's
/// `ExternalIP`) instead of requiring `KLIGHTS_EXTERNAL_ENDPOINT`. The
/// WireGuard public key / port are still derived from the local config and
/// on-disk identity.
pub async fn local_dataplane_peer_metadata_with_endpoint(
    config: &KlightsConfig,
    node_mode: &NodeMode,
    endpoint: &str,
    supervisor: &klights_supervisor::TaskSupervisor,
) -> anyhow::Result<klights_cluster_store::DataplanePeerMetadata> {
    let endpoint = endpoint.trim();
    if endpoint.is_empty() {
        anyhow::bail!(
            "KLIGHTS_EXTERNAL_ENDPOINT is required before publishing cluster-visible dataplane metadata"
        );
    }
    let identity = local_dataplane_identity(config, node_mode, supervisor).await?;
    klights_cluster_store::DataplanePeerMetadata::try_new(
        config.node_name.clone(),
        match identity.mode {
            klights_networking::wireguard::DataplaneMode::Root => {
                klights_cluster_store::DataplaneMode::Root
            }
            klights_networking::wireguard::DataplaneMode::Rootless => {
                klights_cluster_store::DataplaneMode::Rootless
            }
        },
        match identity.encryption {
            klights_networking::wireguard::DataplaneEncryption::Enabled => {
                klights_cluster_store::DataplaneEncryption::Enabled
            }
            klights_networking::wireguard::DataplaneEncryption::Disabled => {
                klights_cluster_store::DataplaneEncryption::Disabled
            }
        },
        identity.public_key,
        Some(endpoint.to_string()),
        identity.port,
    )
    .map_err(anyhow::Error::new)
}

struct LocalDataplaneIdentity {
    mode: klights_networking::wireguard::DataplaneMode,
    encryption: klights_networking::wireguard::DataplaneEncryption,
    public_key: Option<String>,
    port: Option<u16>,
}

async fn local_dataplane_identity(
    config: &KlightsConfig,
    node_mode: &NodeMode,
    supervisor: &klights_supervisor::TaskSupervisor,
) -> anyhow::Result<LocalDataplaneIdentity> {
    let mode = match node_mode {
        NodeMode::Root => klights_networking::wireguard::DataplaneMode::Root,
        NodeMode::Rootless { .. } => klights_networking::wireguard::DataplaneMode::Rootless,
    };
    let encryption = config.dataplane_encryption;
    let port = if encryption == klights_networking::wireguard::DataplaneEncryption::Enabled {
        Some(config.wireguard_port)
    } else {
        None
    };
    let public_key = if encryption == klights_networking::wireguard::DataplaneEncryption::Enabled {
        let key_path = config.data_root.join("etc/wireguard-private.key");
        let identity =
            klights_networking::wireguard::WireGuardIdentity::load_or_create(&key_path, supervisor)
                .await?;
        Some(identity.public_key().to_string())
    } else {
        None
    };
    Ok(LocalDataplaneIdentity {
        mode,
        encryption,
        public_key,
        port,
    })
}

/// Read the local Node's `ExternalIP` from `status.addresses`, if present.
fn node_status_external_ip(node: &serde_json::Value) -> Option<&str> {
    node.pointer("/status/addresses")
        .and_then(|value| value.as_array())
        .and_then(|addresses| {
            addresses.iter().find_map(|address| {
                if address.get("type").and_then(|value| value.as_str()) == Some("ExternalIP") {
                    address
                        .get("address")
                        .and_then(|value| value.as_str())
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                } else {
                    None
                }
            })
        })
}

/// Resolve the external endpoint to advertise for the local node. Prefers the
/// explicitly configured `KLIGHTS_EXTERNAL_ENDPOINT`; otherwise falls back to
/// the `ExternalIP` already recorded on the local Node object (e.g. on a leader
/// restart, where registration persisted the address in a previous run).
///
/// Never falls back to the internal node IP — that would advertise an
/// unreachable address as the cross-node WireGuard endpoint.
#[cfg(test)]
pub async fn resolve_local_external_endpoint(
    resource_reads: &dyn klights_cluster_store::ClusterResourceRead,
    config: &KlightsConfig,
) -> anyhow::Result<Option<String>> {
    if let Some(endpoint) = config
        .external_endpoint
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Ok(Some(endpoint.to_string()));
    }
    let Some(node) = resource_reads
        .get_resource(klights_cluster_store::ResourceGetRequest::new(
            "v1",
            "Node",
            None,
            &config.node_name,
        ))
        .await?
    else {
        return Ok(None);
    };
    Ok(node_status_external_ip(&node.data).map(str::to_string))
}

/// Publish the local `node_dataplane` row from an explicit external endpoint
/// only when no row exists yet. Used by the observed-endpoint watcher to
/// back-fill dataplane metadata once an endpoint becomes known (either from the
/// local Node's `ExternalIP` or discovered from a peer), without clobbering a
/// row a later, authoritative publish may have written.
///
/// Returns `Ok(true)` when a new row was written, `Ok(false)` when one already
/// existed.
#[cfg(test)]
pub async fn ensure_node_dataplane_published(
    topology_reads: &dyn klights_cluster_store::ClusterTopologyRead,
    command: &dyn klights_leader_api::LeaderNetworkTopologyCommand,
    config: &KlightsConfig,
    node_mode: &NodeMode,
    endpoint: &str,
    supervisor: &klights_supervisor::TaskSupervisor,
) -> anyhow::Result<bool> {
    if topology_reads
        .get_node_dataplane(klights_cluster_store::NodeTopologyRequest::try_new(
            &config.node_name,
        )?)
        .await?
        .is_some()
    {
        return Ok(false);
    }
    let metadata =
        local_dataplane_peer_metadata_with_endpoint(config, node_mode, endpoint, supervisor)
            .await?;
    command
        .register_node_dataplane(
            crate::bootstrap::leader_conversions::topology::focused_dataplane(metadata)?,
        )
        .await
        .map_err(anyhow::Error::new)?;
    tracing::info!(
        node = %config.node_name,
        endpoint = %endpoint.trim(),
        "published missing dataplane metadata from observed/registered external endpoint"
    );
    Ok(true)
}

/// Publish the local `node_dataplane` row from a resolved external endpoint,
/// self-healing the case where the leader booted without
/// `KLIGHTS_EXTERNAL_ENDPOINT` but already has an `ExternalIP` on its Node.
///
/// Returns `Ok(true)` when metadata was published, `Ok(false)` when no external
/// endpoint could be resolved yet (the watcher / observed-endpoint path will
/// publish it later once an endpoint becomes known).
#[cfg(test)]
pub async fn publish_local_dataplane_metadata_self_heal(
    resource_reads: &dyn klights_cluster_store::ClusterResourceRead,
    command: &dyn klights_leader_api::LeaderNetworkTopologyCommand,
    config: &KlightsConfig,
    node_mode: &NodeMode,
    supervisor: &klights_supervisor::TaskSupervisor,
) -> anyhow::Result<bool> {
    let Some(endpoint) = resolve_local_external_endpoint(resource_reads, config).await? else {
        return Ok(false);
    };
    let metadata =
        local_dataplane_peer_metadata_with_endpoint(config, node_mode, &endpoint, supervisor)
            .await?;
    command
        .register_node_dataplane(
            crate::bootstrap::leader_conversions::topology::focused_dataplane(metadata)?,
        )
        .await
        .map_err(anyhow::Error::new)?;
    tracing::info!(
        node = %config.node_name,
        endpoint = %endpoint,
        "published local dataplane metadata from resolved external endpoint"
    );
    Ok(true)
}

pub async fn publish_local_dataplane_metadata_self_heal_with_resource_reads(
    resource_reads: &dyn klights_cluster_store::ClusterResourceRead,
    command: &dyn klights_leader_api::LeaderNetworkTopologyCommand,
    config: &KlightsConfig,
    node_mode: &NodeMode,
    supervisor: &klights_supervisor::TaskSupervisor,
) -> anyhow::Result<bool> {
    let endpoint = if let Some(endpoint) = config
        .external_endpoint
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(endpoint.to_string())
    } else {
        resource_reads
            .get_resource(klights_cluster_store::ResourceGetRequest::new(
                "v1",
                "Node",
                None,
                &config.node_name,
            ))
            .await
            .map_err(anyhow::Error::new)?
            .and_then(|node| node_status_external_ip(&node.data).map(str::to_string))
    };
    let Some(endpoint) = endpoint else {
        return Ok(false);
    };
    let metadata =
        local_dataplane_peer_metadata_with_endpoint(config, node_mode, &endpoint, supervisor)
            .await?;
    command
        .register_node_dataplane(
            crate::bootstrap::leader_conversions::topology::focused_dataplane(metadata)?,
        )
        .await
        .map_err(anyhow::Error::new)?;
    tracing::info!(node = %config.node_name, endpoint = %endpoint, "published local dataplane metadata from resolved external endpoint");
    Ok(true)
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
        runtime_epoch_ms(),
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
    async fn local_dataplane_peer_metadata_requires_external_endpoint_without_internal_ip_fallback()
    {
        let mut config = crate::KlightsConfig::test_default();
        config.node_name = "mn-controlplane1".to_string();
        config.external_endpoint = None;
        config.dataplane_encryption = klights_networking::wireguard::DataplaneEncryption::Disabled;
        let supervisor = klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        );

        // An empty endpoint must be rejected — there is no internal-IP fallback
        // for cluster-visible dataplane metadata.
        let err = local_dataplane_peer_metadata_with_endpoint(
            &config,
            &crate::bootstrap::NodeMode::Root,
            "   ",
            &supervisor,
        )
        .await
        .expect_err("persisted peer metadata needs an explicit external endpoint");

        assert!(
            err.to_string().contains("KLIGHTS_EXTERNAL_ENDPOINT"),
            "error should name the missing external endpoint env var: {err:#}"
        );
    }

    #[tokio::test]
    async fn resolve_local_external_endpoint_prefers_config_external_endpoint() {
        let db = crate::bootstrap::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
        let mut config = crate::KlightsConfig::test_default();
        config.node_name = "leader-a".to_string();
        config.external_endpoint = Some("203.0.113.10".to_string());

        let endpoint = resolve_local_external_endpoint(db.focused_read_store().as_ref(), &config)
            .await
            .expect("resolve must succeed");
        assert_eq!(endpoint.as_deref(), Some("203.0.113.10"));
    }

    #[tokio::test]
    async fn resolve_local_external_endpoint_falls_back_to_node_external_ip() {
        let db = crate::bootstrap::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
        let mut config = crate::KlightsConfig::test_default();
        config.node_name = "leader-a".to_string();
        config.external_endpoint = None;

        // Leader already registered with an ExternalIP (e.g. a restart) but no
        // KLIGHTS_EXTERNAL_ENDPOINT in the environment.
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
                        {"type": "InternalIP", "address": "10.174.0.3"},
                        {"type": "ExternalIP", "address": "198.51.100.47"}
                    ]
                }
            }),
        )
        .await
        .unwrap();

        let endpoint = resolve_local_external_endpoint(db.focused_read_store().as_ref(), &config)
            .await
            .expect("resolve must succeed");
        assert_eq!(endpoint.as_deref(), Some("198.51.100.47"));
    }

    #[tokio::test]
    async fn resolve_local_external_endpoint_none_without_endpoint_or_external_ip() {
        let db = crate::bootstrap::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
        let mut config = crate::KlightsConfig::test_default();
        config.node_name = "leader-a".to_string();
        config.external_endpoint = None;

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
                        {"type": "InternalIP", "address": "10.174.0.3"}
                    ]
                }
            }),
        )
        .await
        .unwrap();

        let endpoint = resolve_local_external_endpoint(db.focused_read_store().as_ref(), &config)
            .await
            .expect("resolve must succeed");
        assert_eq!(
            endpoint, None,
            "must not fall back to the internal node IP as an external endpoint"
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

        let published = publish_local_dataplane_metadata_self_heal(
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
    async fn ensure_node_dataplane_published_writes_when_missing() {
        let db = crate::bootstrap::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
        let mut config = crate::KlightsConfig::test_default();
        config.node_name = "leader-a".to_string();
        config.dataplane_encryption = klights_networking::wireguard::DataplaneEncryption::Disabled;
        let supervisor = klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        );

        let wrote = ensure_node_dataplane_published(
            db.focused_read_store().as_ref(),
            &test_dataplane_command(&db),
            &config,
            &NodeMode::Root,
            "198.51.100.47",
            &supervisor,
        )
        .await
        .expect("publish must succeed");
        assert!(wrote, "first publish must write the row");

        let stored = db
            .get_node_dataplane("leader-a")
            .await
            .unwrap()
            .expect("row must exist");
        assert_eq!(stored.endpoint.to_string(), "198.51.100.47");
    }

    #[tokio::test]
    async fn ensure_node_dataplane_published_is_noop_when_row_exists() {
        let db = crate::bootstrap::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
        let mut config = crate::KlightsConfig::test_default();
        config.node_name = "leader-a".to_string();
        config.dataplane_encryption = klights_networking::wireguard::DataplaneEncryption::Disabled;
        let supervisor = klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        );

        // Pre-existing row written from an authoritative endpoint.
        db.update_node_dataplane(
            klights_cluster_store::DataplanePeerMetadata::try_new(
                "leader-a".to_string(),
                klights_cluster_store::DataplaneMode::Root,
                klights_cluster_store::DataplaneEncryption::Disabled,
                None,
                Some("203.0.113.99".to_string()),
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();

        let wrote = ensure_node_dataplane_published(
            db.focused_read_store().as_ref(),
            &test_dataplane_command(&db),
            &config,
            &NodeMode::Root,
            "198.51.100.47",
            &supervisor,
        )
        .await
        .expect("publish must succeed");
        assert!(!wrote, "existing row must not be overwritten");

        let stored = db
            .get_node_dataplane("leader-a")
            .await
            .unwrap()
            .expect("row must exist");
        assert_eq!(
            stored.endpoint.to_string(),
            "203.0.113.99",
            "existing endpoint must be preserved"
        );
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

        let published = publish_local_dataplane_metadata_self_heal(
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
