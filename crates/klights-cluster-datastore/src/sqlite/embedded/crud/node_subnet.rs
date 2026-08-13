use super::super::queries;
use super::*;
use anyhow::Context;
use klights_cluster_store::DataplanePeerMetadata;
use klights_types::ClusterCidr;
use rusqlite::OptionalExtension;

fn row_to_node_subnet(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<klights_cluster_store::StoredNodeSubnet> {
    use klights_types::HostPortRange;
    use klights_types::{NodePeerMode, parse_node_peer_mode};

    let node_name_str: String = row.get(0)?;
    let subnet_str: String = row.get(1)?;
    let gateway_ip_str: String = row.get(3)?;
    let node_ip_str: String = row.get(4)?;
    let mode_str: String = row.get(5).unwrap_or_else(|_| "root".to_string());
    let hostport_range_opt: Option<String> = row.get(6).unwrap_or(None);

    let node_name = NodeName::parse(&node_name_str).map_err(parse_node_subnet_error(0))?;
    let subnet = PodSubnet::parse(&subnet_str).map_err(parse_node_subnet_error(1))?;
    let gateway_ip: Ipv4Addr =
        gateway_ip_str
            .parse()
            .map_err(|error: std::net::AddrParseError| {
                rusqlite::Error::FromSqlConversionFailure(
                    3,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?;
    let node_ip: Ipv4Addr = node_ip_str
        .parse()
        .map_err(|error: std::net::AddrParseError| {
            rusqlite::Error::FromSqlConversionFailure(
                4,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?;
    let mode = parse_node_peer_mode(Some(mode_str.as_str())).unwrap_or(NodePeerMode::Root);
    let hostport_range = hostport_range_opt
        .as_deref()
        .filter(|value| !value.is_empty())
        .and_then(|value| HostPortRange::parse(value).ok());

    Ok(klights_cluster_store::StoredNodeSubnet {
        node_name,
        subnet,
        subnet_base_int: row.get::<_, i64>(2)? as u32,
        gateway_ip,
        node_ip,
        mode,
        hostport_range,
    })
}

fn parse_node_subnet_error(index: usize) -> impl Fn(String) -> rusqlite::Error {
    move |message| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Text,
            Box::new(NodeSubnetParseError(message)),
        )
    }
}

#[derive(Debug)]
struct NodeSubnetParseError(String);

impl std::fmt::Display for NodeSubnetParseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for NodeSubnetParseError {}

impl Datastore {
    // ---- node_subnets CRUD ----------------------------------------

    /// Allocate the next free /24 from `cluster_cidr` for this node.
    /// Idempotent: if the node already has a subnet, returns it unchanged.
    /// Fails if the cluster CIDR is exhausted (all /24s taken).
    pub async fn allocate_node_subnet(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<klights_cluster_store::StoredNodeSubnet> {
        let node_name_typed = NodeName::parse(node_name)
            .map_err(|e| anyhow!("Invalid node name {}: {}", node_name, e))?;
        let node_ip_typed: Ipv4Addr = node_ip
            .parse()
            .with_context(|| format!("Invalid node IP {}", node_ip))?;
        let cluster = ClusterCidr::parse(cluster_cidr)
            .map_err(|e| anyhow!("Invalid cluster CIDR {}: {}", cluster_cidr, e))?;
        anyhow::ensure!(
            cluster.prefix() <= 24,
            "cluster CIDR prefix must be ≤ /24 (got /{})",
            cluster.prefix()
        );

        let cluster_base = cluster.network();
        let prefix_len = cluster.prefix();
        let node_name_str = node_name_typed.as_str().to_string();
        let node_ip_str = node_ip.to_string();

        self.db_call("db_query", move |conn| {
            // Return existing allocation if present.
            let existing: Option<klights_cluster_store::StoredNodeSubnet> = conn
                .query_row(
                    queries::NODE_SUBNET_SELECT_BY_NAME,
                    rusqlite::params![node_name_str],
                    row_to_node_subnet,
                )
                .optional()?;
            if let Some(s) = existing {
                return Ok(s);
            }

            // Enumerate /24 subnets within the cluster CIDR and pick the first free one.
            let host_bits = 32u32.saturating_sub(prefix_len as u32);
            let subnet_count = 1u32.checked_shl(host_bits - 8).unwrap_or(1).max(1);

            for i in 0..subnet_count {
                let base = cluster_base + (i << 8);
                let subnet_typed = PodSubnet::parse(&format!("{}/24", Ipv4Addr::from(base)))
                    .expect("constructed /24 must parse");
                let subnet_cidr = subnet_typed.to_string();
                let gateway_ip_typed = Ipv4Addr::from(base);
                let gateway_ip_str = gateway_ip_typed.to_string();

                // mode + hostport_range default to root / unknown and are
                // reconciled from Node annotations by run_peer_watch.
                let result = conn.execute(
                    queries::NODE_SUBNET_INSERT_OR_IGNORE,
                    rusqlite::params![
                        node_name_str,
                        subnet_cidr,
                        base as i64,
                        gateway_ip_str,
                        node_ip_str,
                        0i64
                    ],
                )?;

                if result > 0 {
                    return Ok(klights_cluster_store::StoredNodeSubnet {
                        node_name: NodeName::parse(&node_name_str).expect("validated"),
                        subnet: subnet_typed,
                        subnet_base_int: base,
                        gateway_ip: gateway_ip_typed,
                        node_ip: node_ip_typed,
                        mode: klights_types::NodePeerMode::Root,
                        hostport_range: None,
                    });
                }
            }

            Err(klights_supervisor::DbError::Sqlite(
                rusqlite::Error::QueryReturnedNoRows,
            ))
        })
        .await
        .map_err(|e| anyhow!("node_subnet allocation failed: {}", e))
    }

    /// Get the subnet record for a specific node.
    pub async fn get_node_subnet(
        &self,
        node_name: &str,
    ) -> Result<Option<klights_cluster_store::StoredNodeSubnet>> {
        self.focused_reads.get_node_subnet(node_name).await
    }

    /// List all node subnets or every peer except one explicitly named node.
    ///
    /// Includes root and rootless peers. The controller decides per-peer
    /// routing from the projected `mode`.
    pub async fn list_peer_subnets(
        &self,
        request: klights_cluster_store::PeerTopologyRequest,
    ) -> Result<Vec<klights_cluster_store::StoredNodeSubnet>> {
        self.focused_reads.list_peer_subnets(request).await
    }

    /// F2-04: persist the peer-mode + hostport-range projection from
    /// `klights.io/mode` / `klights.io/hostport-range` annotations.
    /// `hostport_range` is stored as `NULL` when `None`.
    pub async fn update_node_peer_attributes(
        &self,
        node_name: &str,
        mode: klights_types::NodePeerMode,
        hostport_range: Option<klights_types::HostPortRange>,
    ) -> Result<()> {
        let node_name = node_name.to_string();
        let mode_str = match mode {
            klights_types::NodePeerMode::Root => "root".to_string(),
            klights_types::NodePeerMode::Rootless => "rootless".to_string(),
        };
        let hostport_range_str = hostport_range.map(|r| r.to_string());
        self.db_call("db_query", move |conn| {
            conn.execute(
                queries::NODE_SUBNET_UPDATE_PEER_ATTRIBUTES,
                rusqlite::params![mode_str, hostport_range_str, node_name],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("Failed to update node peer attributes: {}", e))
    }

    /// Remove a node's subnet record (called when a Node is deleted).
    pub async fn delete_node_subnet(&self, node_name: &str) -> Result<()> {
        let node_name = node_name.to_string();
        self.db_call("db_query", move |conn| {
            conn.execute(queries::NODE_SUBNET_DELETE, rusqlite::params![node_name])?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("Failed to delete node subnet: {}", e))
    }

    pub async fn update_node_dataplane(&self, metadata: DataplanePeerMetadata) -> Result<()> {
        let node_name = metadata.node_name;
        let mode = metadata.mode.as_str().to_string();
        let encryption = metadata.encryption.as_str().to_string();
        let public_key = metadata.public_key.map(|key| key.to_string());
        let endpoint = metadata.endpoint.to_string();
        let port = metadata.port.map(i64::from);
        self.db_call("db_query", move |conn| {
            conn.execute(
                queries::NODE_DATAPLANE_UPSERT,
                rusqlite::params![
                    node_name, mode, encryption, public_key, endpoint, port, 0i64
                ],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("Failed to update node dataplane metadata: {}", e))
    }

    pub async fn get_node_dataplane(
        &self,
        node_name: &str,
    ) -> Result<Option<DataplanePeerMetadata>> {
        self.focused_reads.get_node_dataplane(node_name).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_allocate_node_subnet_first_node_gets_first_24() {
        let db = Datastore::new_in_memory().await.unwrap();
        let subnet = db
            .allocate_node_subnet("node-a", "10.42.0.0/16", "192.168.1.1")
            .await
            .unwrap();
        assert_eq!(subnet.subnet.to_string(), "10.42.0.0/24");
        assert_eq!(subnet.gateway_ip.to_string(), "10.42.0.0");
        assert_eq!(subnet.node_ip.to_string(), "192.168.1.1");
        assert_eq!(subnet.node_name.as_str(), "node-a");
    }

    #[tokio::test]
    async fn test_allocate_node_subnet_second_node_gets_next_24() {
        let db = Datastore::new_in_memory().await.unwrap();
        db.allocate_node_subnet("node-a", "10.42.0.0/16", "192.168.1.1")
            .await
            .unwrap();
        let subnet = db
            .allocate_node_subnet("node-b", "10.42.0.0/16", "192.168.1.2")
            .await
            .unwrap();
        assert_eq!(subnet.subnet.to_string(), "10.42.1.0/24");
        assert_eq!(subnet.gateway_ip.to_string(), "10.42.1.0");
    }

    #[tokio::test]
    async fn test_allocate_node_subnet_idempotent_for_existing_node() {
        let db = Datastore::new_in_memory().await.unwrap();
        let first = db
            .allocate_node_subnet("node-a", "10.42.0.0/16", "192.168.1.1")
            .await
            .unwrap();
        let second = db
            .allocate_node_subnet("node-a", "10.42.0.0/16", "192.168.1.1")
            .await
            .unwrap();
        assert_eq!(first.subnet, second.subnet);
    }

    #[tokio::test]
    async fn test_get_node_subnet_returns_none_when_absent() {
        let db = Datastore::new_in_memory().await.unwrap();
        assert!(db.get_node_subnet("nonexistent").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_get_node_subnet_returns_record_after_allocation() {
        let db = Datastore::new_in_memory().await.unwrap();
        db.allocate_node_subnet("node-a", "10.42.0.0/16", "10.0.0.1")
            .await
            .unwrap();
        let record = db.get_node_subnet("node-a").await.unwrap().unwrap();
        assert_eq!(record.node_ip.to_string(), "10.0.0.1");
    }

    #[tokio::test]
    async fn test_list_peer_subnets_excludes_self_and_includes_peer_rows() {
        let db = Datastore::new_in_memory().await.unwrap();
        db.allocate_node_subnet("node-a", "10.42.0.0/16", "10.0.0.1")
            .await
            .unwrap();
        db.allocate_node_subnet("node-b", "10.42.0.0/16", "10.0.0.2")
            .await
            .unwrap();
        let peers = db
            .list_peer_subnets(
                klights_cluster_store::PeerTopologyRequest::excluding("node-a").unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].node_name.as_str(), "node-b");
        assert_eq!(
            db.list_peer_subnets(klights_cluster_store::PeerTopologyRequest::all())
                .await
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn test_delete_node_subnet_removes_record() {
        let db = Datastore::new_in_memory().await.unwrap();
        db.allocate_node_subnet("node-a", "10.42.0.0/16", "10.0.0.1")
            .await
            .unwrap();
        db.delete_node_subnet("node-a").await.unwrap();
        assert!(db.get_node_subnet("node-a").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn list_peer_subnets_includes_rootless_peers() {
        let db = Datastore::new_in_memory().await.unwrap();
        db.allocate_node_subnet("node-a", "10.42.0.0/16", "10.0.0.1")
            .await
            .unwrap();
        db.allocate_node_subnet("rootless-d", "10.42.0.0/16", "10.0.0.4")
            .await
            .unwrap();
        db.update_node_peer_attributes("rootless-d", klights_types::NodePeerMode::Rootless, None)
            .await
            .unwrap();
        let peers = db
            .list_peer_subnets(
                klights_cluster_store::PeerTopologyRequest::excluding("node-a").unwrap(),
            )
            .await
            .unwrap();
        let peer = peers
            .iter()
            .find(|peer| peer.node_name.as_str() == "rootless-d")
            .expect("rootless peer remains visible to route projection");
        assert_eq!(peer.mode, klights_types::NodePeerMode::Rootless);
    }

    #[tokio::test]
    async fn all_peer_subnets_ignores_the_empty_snapshot_sentinel_row() {
        let db = Datastore::new_in_memory().await.unwrap();
        db.allocate_node_subnet("healthy-a", "10.42.0.0/16", "192.0.2.10")
            .await
            .unwrap();
        db.allocate_node_subnet("healthy-b", "10.42.0.0/16", "192.0.2.11")
            .await
            .unwrap();
        db.db_call("insert_empty_node_subnet_test", |conn| {
            conn.execute(
                queries::NODE_SUBNET_UPSERT_EXACT,
                rusqlite::params![
                    "",
                    "10.42.99.0/24",
                    i64::from(u32::from(Ipv4Addr::new(10, 42, 99, 0))),
                    "10.42.99.0",
                    "192.0.2.99",
                    "root",
                    Option::<String>::None,
                ],
            )?;
            Ok(())
        })
        .await
        .unwrap();

        let rows = db
            .list_peer_subnets(klights_cluster_store::PeerTopologyRequest::all())
            .await
            .expect("the empty snapshot sentinel must be filtered before row decoding");
        let mut names = rows
            .iter()
            .map(|row| row.node_name.as_str())
            .collect::<Vec<_>>();
        names.sort_unstable();
        assert_eq!(names, ["healthy-a", "healthy-b"]);
    }
}
