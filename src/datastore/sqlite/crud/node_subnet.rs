use super::super::queries;
use super::*;
use crate::networking::ClusterCidr;
use crate::networking::wireguard::{DataplaneEncryption, DataplaneMode, DataplanePeerMetadata};
use anyhow::Context;
use rusqlite::OptionalExtension;
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
    ) -> Result<NodeSubnet> {
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
            let existing: Option<NodeSubnet> = conn
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
                let vtep_ip_typed = Ipv4Addr::from(base);
                let vtep_ip_str = vtep_ip_typed.to_string();

                // mode + hostport_range default to root / unknown and are
                // reconciled from Node annotations by run_peer_watch.
                let result = conn.execute(
                    queries::NODE_SUBNET_INSERT_OR_IGNORE,
                    rusqlite::params![
                        node_name_str,
                        subnet_cidr,
                        base as i64,
                        vtep_ip_str,
                        node_ip_str,
                        0i64
                    ],
                )?;

                if result > 0 {
                    return Ok(NodeSubnet {
                        node_name: NodeName::parse(&node_name_str).expect("validated"),
                        subnet: subnet_typed,
                        subnet_base_int: base,
                        vtep_ip: vtep_ip_typed,
                        node_ip: node_ip_typed,
                        mode: crate::controllers::annotations::NodePeerMode::Root,
                        hostport_range: None,
                    });
                }
            }

            Err(tokio_rusqlite::Error::Rusqlite(
                rusqlite::Error::QueryReturnedNoRows,
            ))
        })
        .await
        .map_err(|e| anyhow!("node_subnet allocation failed: {}", e))
    }

    /// Get the subnet record for a specific node.
    pub async fn get_node_subnet(&self, node_name: &str) -> Result<Option<NodeSubnet>> {
        let node_name = node_name.to_string();
        self.db_call("db_query", move |conn| {
            conn.query_row(
                queries::NODE_SUBNET_SELECT_BY_NAME,
                rusqlite::params![node_name],
                row_to_node_subnet,
            )
            .optional()
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|e| anyhow!("Failed to get node subnet: {}", e))
    }

    /// List all peer node subnets (everyone except `my_node_name`).
    ///
    /// Includes root and rootless peers. The controller decides per-peer
    /// routing from the projected `mode`.
    pub async fn list_peer_subnets(&self, my_node_name: &str) -> Result<Vec<NodeSubnet>> {
        let my_node_name = my_node_name.to_string();
        self.db_call("db_query", move |conn| {
            let mut stmt = conn.prepare(queries::NODE_SUBNET_LIST_PEERS)?;
            let rows = stmt
                .query_map(rusqlite::params![my_node_name], row_to_node_subnet)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
        .map_err(|e| anyhow!("Failed to list peer subnets: {}", e))
    }

    /// F2-04: persist the peer-mode + hostport-range projection from
    /// `klights.io/mode` / `klights.io/hostport-range` annotations.
    /// `hostport_range` is stored as `NULL` when `None`.
    pub async fn update_node_peer_attributes(
        &self,
        node_name: &str,
        mode: crate::controllers::annotations::NodePeerMode,
        hostport_range: Option<crate::networking::types::HostPortRange>,
    ) -> Result<()> {
        let node_name = node_name.to_string();
        let mode_str = match mode {
            crate::controllers::annotations::NodePeerMode::Root => "root".to_string(),
            crate::controllers::annotations::NodePeerMode::Rootless => "rootless".to_string(),
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
        let node_name = node_name.to_string();
        self.db_call("db_query", move |conn| {
            conn.query_row(
                queries::NODE_DATAPLANE_SELECT_BY_NAME,
                rusqlite::params![node_name],
                row_to_node_dataplane,
            )
            .optional()
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|e| anyhow!("Failed to get node dataplane metadata: {}", e))
    }
}

fn row_to_node_dataplane(row: &rusqlite::Row<'_>) -> rusqlite::Result<DataplanePeerMetadata> {
    let node_name: String = row.get(0)?;
    let mode: String = row.get(1)?;
    let encryption: String = row.get(2)?;
    let public_key: Option<String> = row.get(3)?;
    let endpoint: String = row.get(4)?;
    let port: Option<i64> = row.get(5)?;
    let port = port
        .map(u16::try_from)
        .transpose()
        .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?;

    DataplanePeerMetadata::try_new(
        node_name,
        DataplaneMode::parse(&mode).map_err(to_sql_error)?,
        DataplaneEncryption::parse(Some(&encryption)).map_err(to_sql_error)?,
        public_key,
        Some(endpoint),
        port,
    )
    .map_err(to_sql_error)
}

fn to_sql_error(err: anyhow::Error) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(err.to_string())))
}
