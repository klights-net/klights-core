use super::super::queries;
use crate::log_apply::{
    LogApplyNodeDataplaneRow, LogApplyNodeSubnetAllocation, LogApplyNodeSubnetRow,
};
use crate::networking::{ClusterCidr, NodeName, PodSubnet};
use rusqlite::OptionalExtension;
use std::collections::BTreeSet;
use std::net::Ipv4Addr;

pub(in crate::datastore::sqlite) struct NetworkStateApplier<'tx, 'conn> {
    tx: &'tx rusqlite::Transaction<'conn>,
}

impl<'tx, 'conn> NetworkStateApplier<'tx, 'conn> {
    pub(super) fn new(tx: &'tx rusqlite::Transaction<'conn>) -> Self {
        Self { tx }
    }

    pub(in crate::datastore::sqlite) fn put_node_subnet(
        &self,
        row: LogApplyNodeSubnetRow,
    ) -> tokio_rusqlite::Result<()> {
        self.tx.execute(
            queries::NODE_SUBNET_UPSERT_EXACT,
            rusqlite::params![
                row.node_name,
                row.subnet,
                i64::from(row.subnet_base_int),
                row.vtep_ip,
                row.node_ip,
                row.mode,
                row.hostport_range
            ],
        )?;
        Ok(())
    }

    pub(in crate::datastore::sqlite) fn allocate_node_subnet(
        &self,
        allocation: LogApplyNodeSubnetAllocation,
    ) -> tokio_rusqlite::Result<()> {
        let row = self.allocate_node_subnet_row(allocation)?;
        self.put_node_subnet(row)
    }

    pub(in crate::datastore::sqlite) fn delete_node_subnet(
        &self,
        node_name: String,
    ) -> tokio_rusqlite::Result<()> {
        self.tx
            .execute(queries::NODE_SUBNET_DELETE, rusqlite::params![node_name])?;
        Ok(())
    }

    pub(in crate::datastore::sqlite) fn put_node_dataplane(
        &self,
        row: LogApplyNodeDataplaneRow,
    ) -> tokio_rusqlite::Result<()> {
        self.tx.execute(
            queries::NODE_DATAPLANE_UPSERT,
            rusqlite::params![
                row.node_name,
                row.mode,
                row.encryption,
                row.public_key,
                row.endpoint,
                row.port.map(i64::from),
                0i64
            ],
        )?;
        Ok(())
    }

    pub(in crate::datastore::sqlite) fn delete_node_dataplane(
        &self,
        node_name: String,
    ) -> tokio_rusqlite::Result<()> {
        self.tx
            .execute(queries::NODE_DATAPLANE_DELETE, rusqlite::params![node_name])?;
        Ok(())
    }

    fn allocate_node_subnet_row(
        &self,
        allocation: LogApplyNodeSubnetAllocation,
    ) -> tokio_rusqlite::Result<LogApplyNodeSubnetRow> {
        let node_name_typed = NodeName::parse(&allocation.node_name).map_err(|err| {
            super::super::cluster_replace::other_error(format!(
                "Invalid node name {}: {err}",
                allocation.node_name
            ))
        })?;
        let node_ip_typed: Ipv4Addr = allocation.node_ip.parse().map_err(|err| {
            super::super::cluster_replace::other_error(format!(
                "Invalid node IP {}: {err}",
                allocation.node_ip
            ))
        })?;
        let cluster = ClusterCidr::parse(&allocation.cluster_cidr).map_err(|err| {
            super::super::cluster_replace::other_error(format!(
                "Invalid cluster CIDR {}: {err}",
                allocation.cluster_cidr
            ))
        })?;
        if cluster.prefix() > 24 {
            return Err(super::super::cluster_replace::other_error(format!(
                "cluster CIDR prefix must be <= /24 (got /{})",
                cluster.prefix()
            )));
        }

        let existing = self
            .tx
            .query_row(
                queries::NODE_SUBNET_SELECT_BY_NAME,
                rusqlite::params![node_name_typed.as_str()],
                |row| {
                    Ok(LogApplyNodeSubnetRow {
                        node_name: row.get(0)?,
                        subnet: row.get(1)?,
                        subnet_base_int: row.get::<_, i64>(2)? as u32,
                        vtep_ip: row.get(3)?,
                        node_ip: row.get(4)?,
                        mode: row.get(5)?,
                        hostport_range: row.get(6)?,
                    })
                },
            )
            .optional()?;
        if let Some(existing) = existing {
            return Ok(existing);
        }

        let mut allocated = BTreeSet::new();
        {
            let mut stmt = self
                .tx
                .prepare("SELECT subnet_base_int FROM node_subnets")?;
            let rows = stmt.query_map([], |row| row.get::<_, i64>(0))?;
            for row in rows {
                allocated.insert(row? as u32);
            }
        }

        let cluster_base = cluster.network();
        let host_bits = 32u32.saturating_sub(cluster.prefix() as u32);
        let subnet_count = 1u32.checked_shl(host_bits - 8).unwrap_or(1).max(1);
        for i in 0..subnet_count {
            let base = cluster_base + (i << 8);
            if allocated.contains(&base) {
                continue;
            }
            let subnet_typed = PodSubnet::parse(&format!("{}/24", Ipv4Addr::from(base)))
                .expect("constructed /24 must parse");
            let vtep_ip = Ipv4Addr::from(base);
            return Ok(LogApplyNodeSubnetRow {
                node_name: node_name_typed.as_str().to_string(),
                subnet: subnet_typed.to_string(),
                subnet_base_int: base,
                vtep_ip: vtep_ip.to_string(),
                node_ip: node_ip_typed.to_string(),
                mode: "root".to_string(),
                hostport_range: None,
            });
        }

        Err(tokio_rusqlite::Error::Rusqlite(
            rusqlite::Error::QueryReturnedNoRows,
        ))
    }
}
