use super::super::queries;
use super::*;
use crate::pod_identity::PodIdentity;
use rusqlite::OptionalExtension;
impl Datastore {
    // ========== Pod Sandbox Methods ==========
    // These methods track the containerd sandbox_id for each pod so that
    // delete_pod() can always call RemovePodSandbox (and trigger CNI DEL)
    // even when the pod annotation is missing (e.g. pod creation failed
    // before update_pod_status() ran). Prevents CNI netns/veth leaks.

    /// Record the sandbox_id for a pod immediately after RunPodSandbox succeeds.
    /// Uses INSERT OR REPLACE so it is idempotent for retries / pod recreation.
    pub async fn record_sandbox(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        sandbox_id: &str,
    ) -> Result<()> {
        let ns = namespace.to_string();
        let pn = pod_name.to_string();
        let pu = pod_uid.to_string();
        let sid = sandbox_id.to_string();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        self.node_db_call("db_query", move |conn| {
            conn.execute(
                queries::POD_SANDBOX_INSERT_OR_REPLACE,
                rusqlite::params![ns, pn, pu, sid, now],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("Failed to record sandbox: {}", e))
    }

    /// Look up the sandbox_id for a pod by namespace + pod_name.
    /// Returns None if no record exists (pod was created before this fix).
    pub async fn get_sandbox(&self, namespace: &str, pod_name: &str) -> Result<Option<String>> {
        let ns = namespace.to_string();
        let pn = pod_name.to_string();
        let result = self
            .node_db_call("db_query", move |conn| {
                let mut stmt = conn.prepare(queries::POD_SANDBOX_GET)?;
                let row = stmt.query_row(rusqlite::params![ns, pn], |row| row.get::<_, String>(0));
                Ok(row)
            })
            .await;
        match result {
            Ok(Ok(sid)) => Ok(Some(sid)),
            Ok(Err(rusqlite::Error::QueryReturnedNoRows)) => Ok(None),
            Ok(Err(e)) => Err(anyhow!("Failed to get sandbox: {}", e)),
            Err(e) => Err(anyhow!("Failed to get sandbox: {}", e)),
        }
    }

    /// Look up the sandbox_id for a pod by namespace + pod_name + pod_uid.
    /// This avoids treating stale rows from same-name pod recreation as active.
    pub async fn get_sandbox_for_uid(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
    ) -> Result<Option<String>> {
        let ns = namespace.to_string();
        let pn = pod_name.to_string();
        let pu = pod_uid.to_string();
        let result = self
            .node_db_call("db_query", move |conn| {
                let mut stmt = conn.prepare(queries::POD_SANDBOX_GET_FOR_UID)?;
                let row =
                    stmt.query_row(rusqlite::params![ns, pn, pu], |row| row.get::<_, String>(0));
                Ok(row)
            })
            .await;
        match result {
            Ok(Ok(sid)) => Ok(Some(sid)),
            Ok(Err(rusqlite::Error::QueryReturnedNoRows)) => Ok(None),
            Ok(Err(e)) => Err(anyhow!("Failed to get sandbox by uid: {}", e)),
            Err(e) => Err(anyhow!("Failed to get sandbox by uid: {}", e)),
        }
    }

    /// List every sandbox currently tracked in `pod_sandboxes`. Used by
    /// the sandbox GC to detect rows that no longer have a matching CRI
    /// sandbox.
    pub async fn list_sandboxes(&self) -> Result<Vec<crate::datastore::SandboxRef>> {
        let rows = self
            .node_db_call("db_query", |conn| {
                let mut stmt = conn.prepare(queries::POD_SANDBOX_LIST)?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok(crate::datastore::SandboxRef {
                            namespace: row.get::<_, String>(0)?,
                            pod_name: row.get::<_, String>(1)?,
                            pod_uid: row.get::<_, String>(2)?,
                            sandbox_id: row.get::<_, String>(3)?,
                        })
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await
            .map_err(|e| anyhow!("Failed to list sandboxes: {}", e))?;
        Ok(rows)
    }

    /// Remove the sandbox record after RemovePodSandbox succeeds.
    pub async fn delete_sandbox(&self, namespace: &str, pod_name: &str) -> Result<()> {
        let ns = namespace.to_string();
        let pn = pod_name.to_string();
        self.node_db_call("db_query", move |conn| {
            conn.execute(queries::POD_SANDBOX_DELETE, rusqlite::params![ns, pn])?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("Failed to delete sandbox: {}", e))
    }

    /// Remove the sandbox record only if the row still belongs to the same
    /// Pod UID and sandbox ID. This is the race-free delete path for old
    /// WatchDeleted/GC work that may overlap same-name Pod recreation.
    pub async fn delete_sandbox_for_uid(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        sandbox_id: &str,
    ) -> Result<()> {
        let ns = namespace.to_string();
        let pn = pod_name.to_string();
        let pu = pod_uid.to_string();
        let sid = sandbox_id.to_string();
        self.node_db_call("db_query", move |conn| {
            conn.execute(
                queries::POD_SANDBOX_DELETE_FOR_UID,
                rusqlite::params![ns, pn, pu, sid],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("Failed to delete sandbox by uid: {}", e))
    }

    /// Record a pod network allocation in pod_networks.
    /// Called by cni::add() after the veth pair and IP are set up.
    pub async fn record_pod_network(
        &self,
        sandbox_id: &str,
        pod: &PodIdentity,
        ip_addr: &str,
        ip_int: u32,
        veth_host: &str,
        netns_path: &str,
    ) -> Result<()> {
        let sid = sandbox_id.to_string();
        let ns = pod.namespace.clone();
        let pn = pod.name.clone();
        let pu = pod.uid.clone();
        let ip = ip_addr.to_string();
        let ii = ip_int as i64;
        let vh = veth_host.to_string();
        let np = netns_path.to_string();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        self.node_db_call("db_query", move |conn| {
            conn.execute(
                queries::POD_NETWORK_INSERT,
                rusqlite::params![sid, ns, pn, pu, ip, ii, vh, np, now],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("Failed to record pod network: {}", e))
    }

    /// Atomically allocate and reserve a pod IP by inserting into pod_networks.
    ///
    /// This method performs allocation and row insertion in a single sqlite call,
    /// eliminating races between `ipam_allocate` and `record_pod_network`.
    ///
    /// Idempotent for retries: if the sandbox already has a record, returns that
    /// existing `(ip_addr, ip_int)` allocation.
    pub async fn ipam_allocate_and_record_pod_network(
        &self,
        sandbox_id: &str,
        pod: &PodIdentity,
        subnet_base_int: u32,
        subnet_size: u32,
        veth_host: &str,
        netns_path: &str,
    ) -> Result<(String, u32)> {
        let sid = sandbox_id.to_string();
        let ns = pod.namespace.clone();
        let pn = pod.name.clone();
        let pu = pod.uid.clone();
        let vh = veth_host.to_string();
        let np = netns_path.to_string();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        self.node_db_call("db_query", move |conn| {
            if subnet_size < 4 {
                return Err(tokio_rusqlite::Error::Other(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "subnet too small for pod IPAM",
                ))));
            }

            let start = subnet_base_int + 2; // base+1 is the bridge gateway
            let end = subnet_base_int + subnet_size - 2; // reserve broadcast
            if start > end {
                return Err(tokio_rusqlite::Error::Other(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "subnet has no usable pod IPs",
                ))));
            }

            // Idempotency for retries of the same sandbox.
            if let Some((existing_ip, existing_ip_int)) = conn
                .query_row(
                    queries::POD_NETWORK_GET_BY_SANDBOX,
                    rusqlite::params![sid.clone()],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u32)),
                )
                .optional()?
            {
                return Ok((existing_ip, existing_ip_int));
            }

            let max_allocated: Option<i64> = conn.query_row(
                queries::POD_NETWORK_MAX_IP_IN_RANGE,
                rusqlite::params![start as i64, end as i64],
                |row| row.get(0),
            )?;
            let next_after_max = max_allocated
                .map(|v| v as u32 + 1)
                .filter(|candidate| *candidate <= end)
                .unwrap_or(start);
            let usable_count = end - start + 1;

            for offset in 0..usable_count {
                let candidate = start + ((next_after_max - start + offset) % usable_count);
                let ip_addr = crate::utils::ip_u32_to_string(candidate);
                let inserted = conn.execute(
                    queries::POD_NETWORK_INSERT_ON_CONFLICT_NOTHING,
                    rusqlite::params![
                        sid.clone(),
                        ns.clone(),
                        pn.clone(),
                        pu.clone(),
                        ip_addr,
                        candidate as i64,
                        vh.clone(),
                        np.clone(),
                        now
                    ],
                )?;
                if inserted > 0 {
                    return Ok((crate::utils::ip_u32_to_string(candidate), candidate));
                }
            }

            Err(tokio_rusqlite::Error::Other(Box::new(std::io::Error::new(
                std::io::ErrorKind::AddrNotAvailable,
                "no free IPs in pod subnet",
            ))))
        })
        .await
        .map_err(|e| anyhow!("Atomic IPAM allocation failed: {}", e))
    }

    /// Retrieve pod network info by sandbox_id.
    /// Returns None if no record (host-network pod or pre-P2 pod).
    pub async fn get_pod_network(
        &self,
        sandbox_id: &str,
    ) -> Result<Option<crate::datastore::PodNetworkEndpoint>> {
        let sid = sandbox_id.to_string();
        let result = self
            .node_db_call("db_query", move |conn| {
                let mut stmt = conn.prepare(queries::POD_NETWORK_GET_ENDPOINT)?;
                let row = stmt.query_row(rusqlite::params![sid], |row| {
                    Ok(crate::datastore::PodNetworkEndpoint {
                        ip_addr: row.get::<_, String>(0)?,
                        veth_host: row.get::<_, String>(1)?,
                        netns_path: row.get::<_, String>(2)?,
                    })
                });
                Ok(row)
            })
            .await;
        match result {
            Ok(Ok(rec)) => Ok(Some(rec)),
            Ok(Err(rusqlite::Error::QueryReturnedNoRows)) => Ok(None),
            Ok(Err(e)) => Err(anyhow!("Failed to get pod network: {}", e)),
            Err(e) => Err(anyhow!("Failed to get pod network: {}", e)),
        }
    }

    /// Retrieve pod network info by Kubernetes pod identity.
    /// Returns None if no record exists for the exact pod UID.
    pub async fn get_pod_network_for_pod(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
    ) -> Result<Option<crate::datastore::PodNetworkEndpoint>> {
        let ns = namespace.to_string();
        let pn = pod_name.to_string();
        let pu = pod_uid.to_string();
        let result = self
            .node_db_call("db_query", move |conn| {
                let mut stmt = conn.prepare(queries::POD_NETWORK_GET_ENDPOINT_FOR_POD)?;
                let row = stmt.query_row(rusqlite::params![ns, pn, pu], |row| {
                    Ok(crate::datastore::PodNetworkEndpoint {
                        ip_addr: row.get::<_, String>(0)?,
                        veth_host: row.get::<_, String>(1)?,
                        netns_path: row.get::<_, String>(2)?,
                    })
                });
                Ok(row)
            })
            .await;
        match result {
            Ok(Ok(rec)) => Ok(Some(rec)),
            Ok(Err(rusqlite::Error::QueryReturnedNoRows)) => Ok(None),
            Ok(Err(e)) => Err(anyhow!("Failed to get pod network by pod identity: {}", e)),
            Err(e) => Err(anyhow!("Failed to get pod network by pod identity: {}", e)),
        }
    }

    /// Remove pod network record after veth teardown and IP release.
    pub async fn delete_pod_network(&self, sandbox_id: &str) -> Result<()> {
        let sid = sandbox_id.to_string();
        self.node_db_call("db_query", move |conn| {
            conn.execute(queries::POD_NETWORK_DELETE, rusqlite::params![sid])?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("Failed to delete pod network: {}", e))
    }

    /// List all sandbox IDs that still have pod_networks allocations.
    /// Used by sandbox GC to release leaked IPAM rows when CRI sandboxes are gone.
    pub async fn list_pod_network_sandbox_ids(&self) -> Result<Vec<String>> {
        self.node_db_call("db_query", |conn| {
            let mut stmt = conn.prepare(queries::POD_NETWORK_LIST_SANDBOX_IDS)?;
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
        .map_err(|e| anyhow!("Failed to list pod network sandbox IDs: {}", e))
    }

    /// Allocate the next free IP from the pod subnet.
    /// Returns (ip_addr_string, ip_int) for the allocated IP.
    /// Starts after the highest allocated IP and wraps through the usable range
    /// so freed gaps are reused before reporting exhaustion.
    pub async fn ipam_allocate(
        &self,
        subnet_base_int: u32,
        subnet_size: u32,
    ) -> Result<(String, u32)> {
        self.node_db_call("db_query", move |conn| {
            if subnet_size < 4 {
                return Err(tokio_rusqlite::Error::Other(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "subnet too small for pod IPAM",
                ))));
            }

            let start = subnet_base_int + 2; // base+1 is the bridge gateway
            let end = subnet_base_int + subnet_size - 2; // reserve broadcast
            if start > end {
                return Err(tokio_rusqlite::Error::Other(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "subnet has no usable pod IPs",
                ))));
            }

            let max_allocated: Option<i64> = conn.query_row(
                queries::POD_NETWORK_MAX_IP_IN_RANGE,
                rusqlite::params![start as i64, end as i64],
                |row| row.get(0),
            )?;
            let next_after_max = max_allocated
                .map(|v| v as u32 + 1)
                .filter(|candidate| *candidate <= end)
                .unwrap_or(start);
            let usable_count = end - start + 1;

            for offset in 0..usable_count {
                let candidate = start + ((next_after_max - start + offset) % usable_count);

                let in_use: i64 = conn
                    .query_row(
                        queries::POD_NETWORK_COUNT_BY_IP,
                        rusqlite::params![candidate as i64],
                        |row| row.get(0),
                    )
                    .unwrap_or(0);
                if in_use == 0 {
                    let ip_addr = crate::utils::ip_u32_to_string(candidate);
                    return Ok((ip_addr, candidate));
                }
            }
            Err(tokio_rusqlite::Error::Other(Box::new(std::io::Error::new(
                std::io::ErrorKind::AddrNotAvailable,
                "no free IPs in pod subnet",
            ))))
        })
        .await
        .map_err(|e| anyhow!("IPAM exhausted or failed: {}", e))
    }
}
