use super::super::queries;
use super::*;
use rusqlite::OptionalExtension;
use std::net::Ipv4Addr;

/// Helper: parse one row of the `pod_endpoints` table into a typed
/// `PodEndpointRow`. Errors propagate as `tokio_rusqlite::Error::Other`
/// so the closure surface stays uniform with the rest of the CRUD layer.
fn row_to_pod_endpoint(row: &rusqlite::Row<'_>) -> rusqlite::Result<PodEndpointRow> {
    let pod_uid: String = row.get(0)?;
    let namespace: String = row.get(1)?;
    let pod_name: String = row.get(2)?;
    let node_name: String = row.get(3)?;
    let mode_str: String = row.get(4)?;
    let pod_ip_str: String = row.get(5)?;
    let node_ip_str: Option<String> = row.get(6)?;
    let host_port_tcp: Option<i64> = row.get(7)?;
    let host_port_udp: Option<i64> = row.get(8)?;
    let generation: i64 = row.get(9)?;
    let updated_at: i64 = row.get(10)?;

    let mode = PodEndpointMode::parse(&mode_str).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(
            4,
            rusqlite::types::Type::Text,
            Box::new(PodEndpointParseError(e.to_string())),
        )
    })?;
    let pod_ip: Ipv4Addr = pod_ip_str.parse().map_err(|e: std::net::AddrParseError| {
        rusqlite::Error::FromSqlConversionFailure(5, rusqlite::types::Type::Text, Box::new(e))
    })?;
    let node_ip: Ipv4Addr = node_ip_str
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or(&pod_ip_str)
        .parse()
        .map_err(|e: std::net::AddrParseError| {
            rusqlite::Error::FromSqlConversionFailure(6, rusqlite::types::Type::Text, Box::new(e))
        })?;

    Ok(PodEndpointRow {
        pod_uid,
        namespace,
        pod_name,
        node_name,
        mode,
        pod_ip,
        node_ip,
        host_port_tcp: host_port_tcp.map(|v| v as u16),
        host_port_udp: host_port_udp.map(|v| v as u16),
        generation,
        updated_at,
    })
}

#[derive(Debug)]
struct PodEndpointParseError(String);

impl std::fmt::Display for PodEndpointParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for PodEndpointParseError {}

impl Datastore {
    /// Insert-or-replace a `pod_endpoints` row keyed by `pod_uid`. Idempotent —
    /// a second call for the same uid replaces the previous row in full.
    /// Emits `PodEndpointEvent::Upsert` after the SQL commit succeeds.
    pub async fn pod_endpoint_upsert(&self, row: PodEndpointRow) -> Result<()> {
        let row_for_db = row.clone();
        let prior_ip: Option<String> = self
            .node_db_call("db_pod_endpoint_upsert", move |conn| {
                let prior_ip = conn
                    .query_row(
                        queries::POD_ENDPOINT_GET_IP_FOR_DELETE,
                        rusqlite::params![row_for_db.pod_uid.clone()],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?;
                conn.execute(
                    queries::POD_ENDPOINT_UPSERT,
                    rusqlite::params![
                        row_for_db.pod_uid,
                        row_for_db.namespace,
                        row_for_db.pod_name,
                        row_for_db.node_name,
                        row_for_db.mode.as_str(),
                        row_for_db.pod_ip.to_string(),
                        row_for_db.node_ip.to_string(),
                        row_for_db.host_port_tcp.map(|v| v as i64),
                        row_for_db.host_port_udp.map(|v| v as i64),
                        row_for_db.generation,
                        row_for_db.updated_at,
                    ],
                )?;
                Ok(prior_ip)
            })
            .await
            .map_err(|e| anyhow!("pod_endpoint_upsert failed: {}", e))?;

        if let Some(prior_ip) = prior_ip
            && prior_ip != row.pod_ip.to_string()
            && let Ok(pod_ip) = prior_ip.parse::<Ipv4Addr>()
        {
            let _ = self.pod_endpoint_sender().send(PodEndpointEvent::Delete {
                pod_uid: row.pod_uid.clone(),
                pod_ip,
            });
        }
        let _ = self
            .pod_endpoint_sender()
            .send(PodEndpointEvent::Upsert(row));
        Ok(())
    }

    /// Delete the row for `pod_uid`. No-op if no row exists. Emits a
    /// `PodEndpointEvent::Delete` carrying the prior pod_ip when a row
    /// was actually removed; subscribers can use the pod_ip to flush
    /// downstream caches without an extra read.
    pub async fn pod_endpoint_delete_by_uid(&self, pod_uid: &str) -> Result<()> {
        let pod_uid_owned = pod_uid.to_string();
        let prior_ip: Option<String> = self
            .node_db_call("db_pod_endpoint_delete_lookup_ip", {
                let pod_uid_owned = pod_uid_owned.clone();
                move |conn| {
                    conn.query_row(
                        queries::POD_ENDPOINT_GET_IP_FOR_DELETE,
                        rusqlite::params![pod_uid_owned],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
                    .map_err(tokio_rusqlite::Error::from)
                }
            })
            .await
            .map_err(|e| anyhow!("pod_endpoint_delete_by_uid (lookup) failed: {}", e))?;

        let Some(prior_ip) = prior_ip else {
            return Ok(());
        };

        let pod_uid_for_delete = pod_uid_owned.clone();
        self.node_db_call("db_pod_endpoint_delete", move |conn| {
            conn.execute(
                queries::POD_ENDPOINT_DELETE,
                rusqlite::params![pod_uid_for_delete],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("pod_endpoint_delete_by_uid failed: {}", e))?;

        if let Ok(pod_ip) = prior_ip.parse::<Ipv4Addr>() {
            let _ = self.pod_endpoint_sender().send(PodEndpointEvent::Delete {
                pod_uid: pod_uid_owned,
                pod_ip,
            });
        }
        Ok(())
    }

    /// List every endpoint row whose `node_name` matches.
    pub async fn pod_endpoint_list_by_node(&self, node_name: &str) -> Result<Vec<PodEndpointRow>> {
        let node_name = node_name.to_string();
        self.node_db_call("db_pod_endpoint_list_by_node", move |conn| {
            let mut stmt = conn.prepare(queries::POD_ENDPOINT_LIST_BY_NODE)?;
            let rows = stmt
                .query_map(rusqlite::params![node_name], row_to_pod_endpoint)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
        .map_err(|e| anyhow!("pod_endpoint_list_by_node failed: {}", e))
    }

    /// List every endpoint row, ordered by uid for deterministic reconciliation.
    pub async fn pod_endpoint_list_all(&self) -> Result<Vec<PodEndpointRow>> {
        self.node_db_call("db_pod_endpoint_list_all", move |conn| {
            let mut stmt = conn.prepare(queries::POD_ENDPOINT_LIST_ALL)?;
            let rows = stmt
                .query_map([], row_to_pod_endpoint)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
        .map_err(|e| anyhow!("pod_endpoint_list_all failed: {}", e))
    }

    /// Look up the endpoint row by `pod_ip`. Returns `None` if no pod
    /// currently advertises that address.
    pub async fn pod_endpoint_get_by_pod_ip(
        &self,
        pod_ip: Ipv4Addr,
    ) -> Result<Option<PodEndpointRow>> {
        let pod_ip = pod_ip.to_string();
        self.node_db_call("db_pod_endpoint_get_by_pod_ip", move |conn| {
            conn.query_row(
                queries::POD_ENDPOINT_GET_BY_POD_IP,
                rusqlite::params![pod_ip],
                row_to_pod_endpoint,
            )
            .optional()
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|e| anyhow!("pod_endpoint_get_by_pod_ip failed: {}", e))
    }
}
