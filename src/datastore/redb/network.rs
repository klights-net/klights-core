//! `RedbNetworkStore` — IPAM allocation, pod network endpoint management,
//! node subnet management, and pod endpoint tracking.

use std::collections::BTreeSet;
use std::net::Ipv4Addr;
use std::sync::Arc;

use ::redb::{ReadableDatabase, ReadableTable};
use anyhow::{Result, anyhow};
use serde_json::Value;
use tokio::sync::broadcast;

use crate::controllers::annotations::NodePeerMode;
use crate::datastore::redb::accessor::RedbAccessor;
use crate::datastore::redb::helpers;
use crate::datastore::redb::tables;
use crate::datastore::types::*;
use crate::networking::types::HostPortRange;
use crate::networking::{ClusterCidr, NodeName, PodSubnet};

pub struct RedbNetworkStore {
    pub accessor: Arc<RedbAccessor>,
    endpoint_tx: broadcast::Sender<PodEndpointEvent>,
}

impl RedbNetworkStore {
    pub fn new(
        accessor: Arc<RedbAccessor>,
        endpoint_tx: broadcast::Sender<PodEndpointEvent>,
    ) -> Self {
        Self {
            accessor,
            endpoint_tx,
        }
    }

    async fn db_call<T, F>(&self, label: &str, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&::redb::Database) -> Result<T> + Send + 'static,
    {
        self.accessor.call(label, f).await
    }

    // -----------------------------------------------------------------------
    // IPAM / pod network
    // -----------------------------------------------------------------------

    pub async fn ipam_alloc(
        &self,
        request: PodNetworkAllocationRequest<'_>,
    ) -> Result<(String, u32)> {
        let request = request.into_owned();
        self.db_call("ipam_alloc_impl", move |db| {
            let request = request.as_borrowed();
            if request.subnet.size < 4 {
                return Err(anyhow!("subnet too small for pod IPAM"));
            }
            let start = request.subnet.base_int + 2;
            let end = request.subnet.base_int + request.subnet.size - 2;
            if start > end {
                return Err(anyhow!("subnet has no usable pod IPs"));
            }

            let w = db.begin_write()?;

            let existing: Option<(String, u32)> = {
                let t = w.open_table(tables::POD_NETWORKS)?;
                let opt = t.get(request.sandbox_id)?;
                opt.map(|g| {
                    let bytes = g.value().to_vec();
                    let v: Value = serde_json::from_slice(&bytes).unwrap_or_default();
                    (
                        v.get("ip")
                            .and_then(|s| s.as_str())
                            .unwrap_or("")
                            .to_string(),
                        v.get("ip_int").and_then(|x| x.as_u64()).unwrap_or(0) as u32,
                    )
                })
            };
            let existing = existing.and_then(|(ip, ip_int)| {
                if ip.is_empty() {
                    None
                } else {
                    Some((ip, ip_int))
                }
            });
            if let Some((ip_str, ip_int)) = existing {
                w.commit()?;
                return Ok((ip_str, ip_int));
            }

            let mut used = BTreeSet::new();
            {
                let t = w.open_table(tables::POD_NETWORKS)?;
                for e in t.iter()? {
                    let v: Value = serde_json::from_slice(e?.1.value()).unwrap_or_default();
                    if let Some(i) = v.get("ip_int").and_then(|x| x.as_u64()) {
                        used.insert(i as u32);
                    }
                }
            }
            let ip_int = (start..=end)
                .find(|i| !used.contains(i))
                .ok_or_else(|| anyhow!("no free IP"))?;
            let ip = Ipv4Addr::from(ip_int);
            {
                let mut t = w.open_table(tables::POD_NETWORKS)?;
                let v = serde_json::json!({
                    "ip": ip.to_string(),
                    "ip_int": ip_int,
                    "veth": request.link.veth_host,
                    "netns": request.link.netns_path,
                    "ns": request.pod.namespace,
                    "pod": request.pod.name,
                    "uid": request.pod.uid,
                });
                t.insert(request.sandbox_id, serde_json::to_vec(&v)?.as_slice())?;
            }
            w.commit()?;
            Ok((ip.to_string(), ip_int))
        })
        .await
    }

    pub async fn get_pnet(&self, sid: &str) -> Result<Option<PodNetworkEndpoint>> {
        let sid_owned = sid.to_string();
        self.db_call("get_pnet_impl", move |db| {
            let sid: &str = &sid_owned;
            let r = db.begin_read()?;
            let t = r.open_table(tables::POD_NETWORKS)?;
            Ok(t.get(sid)?.map(|g| {
                let v: Value = serde_json::from_slice(g.value()).unwrap_or_default();
                PodNetworkEndpoint {
                    ip_addr: v
                        .get("ip")
                        .and_then(|s| s.as_str())
                        .unwrap_or("")
                        .to_string(),
                    veth_host: v
                        .get("veth")
                        .and_then(|s| s.as_str())
                        .unwrap_or("")
                        .to_string(),
                    netns_path: v
                        .get("netns")
                        .and_then(|s| s.as_str())
                        .unwrap_or("")
                        .to_string(),
                }
            }))
        })
        .await
    }

    pub async fn get_pnet_for_pod(
        &self,
        ns: &str,
        pod_name: &str,
        pod_uid: &str,
    ) -> Result<Option<PodNetworkEndpoint>> {
        let ns_owned = ns.to_string();
        let pod_name_owned = pod_name.to_string();
        let pod_uid_owned = pod_uid.to_string();
        self.db_call("get_pnet_for_pod_impl", move |db| {
            let ns: &str = &ns_owned;
            let pod_name: &str = &pod_name_owned;
            let pod_uid: &str = &pod_uid_owned;
            let r = db.begin_read()?;
            let t = r.open_table(tables::POD_NETWORKS)?;
            for e in t.iter()? {
                let (_, val) = e?;
                let v: Value = serde_json::from_slice(val.value()).unwrap_or_default();
                let matches = v.get("ns").and_then(|s| s.as_str()) == Some(ns)
                    && v.get("pod").and_then(|s| s.as_str()) == Some(pod_name)
                    && v.get("uid").and_then(|s| s.as_str()) == Some(pod_uid);
                if matches {
                    return Ok(Some(PodNetworkEndpoint {
                        ip_addr: v
                            .get("ip")
                            .and_then(|s| s.as_str())
                            .unwrap_or("")
                            .to_string(),
                        veth_host: v
                            .get("veth")
                            .and_then(|s| s.as_str())
                            .unwrap_or("")
                            .to_string(),
                        netns_path: v
                            .get("netns")
                            .and_then(|s| s.as_str())
                            .unwrap_or("")
                            .to_string(),
                    }));
                }
            }
            Ok(None)
        })
        .await
    }

    pub async fn delete_pnet(&self, sid: &str) -> Result<()> {
        let sid_owned = sid.to_string();
        self.db_call("delete_pnet_impl", move |db| {
            let sid: &str = &sid_owned;
            let w = db.begin_write()?;
            {
                let mut t = w.open_table(tables::POD_NETWORKS)?;
                t.remove(sid)?;
            }
            Ok(w.commit()?)
        })
        .await
    }

    pub async fn list_pnet_sandbox_ids(&self) -> Result<Vec<String>> {
        self.db_call("list_pnet_sandbox_ids_impl", move |db| {
            let r = db.begin_read()?;
            let t = r.open_table(tables::POD_NETWORKS)?;
            let mut ids = Vec::new();
            for e in t.iter()? {
                let (k, _) = e?;
                ids.push(k.value().to_string());
            }
            Ok(ids)
        })
        .await
    }

    // -----------------------------------------------------------------------
    // Node subnet
    // -----------------------------------------------------------------------

    pub async fn allocate_node_subnet(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<NodeSubnet> {
        let cluster_cidr_owned = cluster_cidr.to_string();
        let node_ip_owned = node_ip.to_string();
        let node_name_owned = node_name.to_string();
        self.db_call("allocate_node_subnet_impl", move |db| {
            let cluster_cidr: &str = &cluster_cidr_owned;
            let node_ip: &str = &node_ip_owned;
            let node_name: &str = &node_name_owned;
            let node_name_typed =
                NodeName::parse(node_name).map_err(|e| anyhow!("invalid node name: {e}"))?;
            let node_ip_typed: Ipv4Addr = node_ip
                .parse()
                .map_err(|e| anyhow!("invalid node IP: {e}"))?;
            let cluster = ClusterCidr::parse(cluster_cidr)
                .map_err(|e| anyhow!("invalid cluster CIDR: {e}"))?;
            if cluster.prefix() > 24 {
                return Err(anyhow!(
                    "cluster CIDR prefix must be ≤ /24 (got /{})",
                    cluster.prefix()
                ));
            }
            let cluster_base = cluster.network();
            let prefix_len = cluster.prefix();

            let w = db.begin_write()?;

            let existing_bytes: Option<Vec<u8>> = {
                let t = w.open_table(tables::NODE_SUBNETS)?;
                let opt = t.get(node_name)?;
                opt.map(|g| g.value().to_vec())
            };
            if let Some(bytes) = existing_bytes {
                let v: Value = serde_json::from_slice(&bytes).unwrap_or_default();
                let subnet_str = v.get("subnet").and_then(|s| s.as_str()).unwrap_or("");
                let subnet_base = v
                    .get("subnet_base_int")
                    .and_then(|x| x.as_u64())
                    .unwrap_or(0) as u32;
                let vtep_ip_str = v.get("vtep_ip").and_then(|s| s.as_str()).unwrap_or("");
                let mode_str = v.get("mode").and_then(|s| s.as_str()).unwrap_or("root");
                let hpr_str = v.get("hostport_range").and_then(|s| s.as_str());

                let subnet =
                    PodSubnet::parse(subnet_str).map_err(|e| anyhow!("bad subnet: {e}"))?;
                let vtep_ip: Ipv4Addr = vtep_ip_str
                    .parse()
                    .map_err(|e| anyhow!("bad vtep_ip: {e}"))?;
                let mode = parse_peer_mode(mode_str);
                let hostport_range = hpr_str.and_then(|s| HostPortRange::parse(s).ok());

                w.commit()?;
                return Ok(NodeSubnet {
                    node_name: node_name_typed,
                    subnet,
                    subnet_base_int: subnet_base,
                    vtep_ip,
                    node_ip: node_ip_typed,
                    mode,
                    hostport_range,
                });
            }

            let host_bits = 32u32.saturating_sub(prefix_len as u32);
            let subnet_count = 1u32
                .checked_shl(host_bits.saturating_sub(8))
                .unwrap_or(1)
                .max(1);

            let mut allocated = BTreeSet::new();
            {
                let t = w.open_table(tables::NODE_SUBNETS)?;
                for e in t.iter()? {
                    let (_, val) = e?;
                    let v: Value = serde_json::from_slice(val.value()).unwrap_or_default();
                    if let Some(b) = v.get("subnet_base_int").and_then(|x| x.as_u64()) {
                        allocated.insert(b as u32);
                    }
                }
            }

            for i in 0..subnet_count {
                let base = cluster_base + (i << 8);
                if allocated.contains(&base) {
                    continue;
                }
                let subnet_typed =
                    PodSubnet::parse(&format!("{}/24", Ipv4Addr::from(base))).expect("valid /24");
                let subnet_cidr = subnet_typed.to_string();
                let vtep_ip = Ipv4Addr::from(base);

                let v = serde_json::json!({
                    "subnet": subnet_cidr,
                    "subnet_base_int": base,
                    "vtep_ip": vtep_ip.to_string(),
                    "node_ip": node_ip,
                    "mode": "root",
                    "hostport_range": null,
                });
                {
                    let mut t = w.open_table(tables::NODE_SUBNETS)?;
                    t.insert(node_name, serde_json::to_vec(&v)?.as_slice())?;
                }
                w.commit()?;
                return Ok(NodeSubnet {
                    node_name: node_name_typed,
                    subnet: subnet_typed,
                    subnet_base_int: base,
                    vtep_ip,
                    node_ip: node_ip_typed,
                    mode: NodePeerMode::Root,
                    hostport_range: None,
                });
            }

            Err(anyhow!("no free /24 subnets in cluster CIDR"))
        })
        .await
    }

    pub async fn update_peer_attrs(
        &self,
        node_name: &str,
        mode: NodePeerMode,
        hostport_range: Option<HostPortRange>,
    ) -> Result<()> {
        let node_name_owned = node_name.to_string();
        self.db_call("update_peer_attrs_impl", move |db| {
            let node_name: &str = &node_name_owned;
            let w = db.begin_write()?;
            {
                let bytes: Vec<u8> = {
                    let t = w.open_table(tables::NODE_SUBNETS)?;
                    let g = t
                        .get(node_name)?
                        .ok_or_else(|| anyhow!("node subnet not found"))?;
                    g.value().to_vec()
                };
                let mut v: Value = serde_json::from_slice(&bytes).unwrap_or_default();
                if let Some(obj) = v.as_object_mut() {
                    obj.insert(
                        "mode".into(),
                        Value::String(match mode {
                            NodePeerMode::Root => "root".into(),
                            NodePeerMode::Rootless => "rootless".into(),
                        }),
                    );
                    obj.insert(
                        "hostport_range".into(),
                        hostport_range
                            .as_ref()
                            .map(|r| Value::String(r.to_string()))
                            .unwrap_or(Value::Null),
                    );
                }
                let mut t = w.open_table(tables::NODE_SUBNETS)?;
                t.insert(node_name, serde_json::to_vec(&v)?.as_slice())?;
            }
            Ok(w.commit()?)
        })
        .await
    }

    pub async fn get_node_subnet(&self, node_name: &str) -> Result<Option<NodeSubnet>> {
        let node_name_owned = node_name.to_string();
        self.db_call("get_node_subnet_impl", move |db| {
            let node_name: &str = &node_name_owned;
            let r = db.begin_read()?;
            let t = r.open_table(tables::NODE_SUBNETS)?;
            match t.get(node_name)? {
                Some(g) => Ok(Some(parse_node_subnet_value(node_name, g.value())?)),
                None => Ok(None),
            }
        })
        .await
    }

    pub async fn list_peer_subnets(&self, my_node: &str) -> Result<Vec<NodeSubnet>> {
        let my_node_owned = my_node.to_string();
        self.db_call("list_peer_subnets_impl", move |db| {
            let my_node: &str = &my_node_owned;
            let r = db.begin_read()?;
            let t = r.open_table(tables::NODE_SUBNETS)?;
            let mut items = Vec::new();
            for e in t.iter()? {
                let (k, val) = e?;
                let name = k.value();
                if name == my_node {
                    continue;
                }
                items.push(parse_node_subnet_value(name, val.value())?);
            }
            Ok(items)
        })
        .await
    }

    pub async fn delete_node_subnet(&self, node_name: &str) -> Result<()> {
        let node_name_owned = node_name.to_string();
        self.db_call("delete_node_subnet_impl", move |db| {
            let node_name: &str = &node_name_owned;
            let w = db.begin_write()?;
            {
                let mut t = w.open_table(tables::NODE_SUBNETS)?;
                t.remove(node_name)?;
            }
            Ok(w.commit()?)
        })
        .await
    }

    pub async fn update_node_dataplane(
        &self,
        metadata: crate::networking::wireguard::DataplanePeerMetadata,
    ) -> Result<()> {
        self.db_call("update_node_dataplane_impl", move |db| {
            let value = serde_json::json!({
                "mode": metadata.mode.as_str(),
                "encryption": metadata.encryption.as_str(),
                "public_key": metadata.public_key.as_ref().map(|key| key.to_string()),
                "endpoint": metadata.endpoint.to_string(),
                "port": metadata.port,
            });
            let w = db.begin_write()?;
            {
                let mut t = w.open_table(tables::NODE_DATAPLANE)?;
                t.insert(
                    metadata.node_name.as_str(),
                    serde_json::to_vec(&value)?.as_slice(),
                )?;
            }
            Ok(w.commit()?)
        })
        .await
    }

    pub async fn get_node_dataplane(
        &self,
        node_name: &str,
    ) -> Result<Option<crate::networking::wireguard::DataplanePeerMetadata>> {
        let node_name_owned = node_name.to_string();
        self.db_call("get_node_dataplane_impl", move |db| {
            let r = db.begin_read()?;
            let t = match r.open_table(tables::NODE_DATAPLANE) {
                Ok(t) => t,
                Err(::redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(err) => return Err(err.into()),
            };
            match t.get(node_name_owned.as_str())? {
                Some(value) => {
                    let body: Value = serde_json::from_slice(value.value()).unwrap_or_default();
                    let mode = body.get("mode").and_then(|v| v.as_str()).unwrap_or("");
                    let encryption = body
                        .get("encryption")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let public_key = body
                        .get("public_key")
                        .and_then(|v| v.as_str())
                        .map(str::to_string);
                    let endpoint = body
                        .get("endpoint")
                        .and_then(|v| v.as_str())
                        .map(str::to_string);
                    let port = body
                        .get("port")
                        .and_then(|v| v.as_u64())
                        .map(u16::try_from)
                        .transpose()
                        .map_err(|err| anyhow!("bad dataplane port: {err}"))?;
                    Ok(Some(
                        crate::networking::wireguard::DataplanePeerMetadata::try_new(
                            node_name_owned,
                            crate::networking::wireguard::DataplaneMode::parse(mode)?,
                            crate::networking::wireguard::DataplaneEncryption::parse(Some(
                                encryption,
                            ))?,
                            public_key,
                            endpoint,
                            port,
                        )?,
                    ))
                }
                None => Ok(None),
            }
        })
        .await
    }

    // -----------------------------------------------------------------------
    // Pod endpoints
    // -----------------------------------------------------------------------

    pub async fn pod_endpoint_get_by_pod_ip(
        &self,
        pod_ip: Ipv4Addr,
    ) -> Result<Option<PodEndpointRow>> {
        self.db_call("pod_endpoint_get_by_pod_ip_impl", move |db| {
            let r = db.begin_read()?;
            let t = r.open_table(tables::POD_ENDPOINTS)?;
            for e in t.iter()? {
                let (_, val) = e?;
                let v: Value = serde_json::from_slice(val.value()).unwrap_or_default();
                let stored_ip = v.get("pod_ip").and_then(|s| s.as_str()).unwrap_or("");
                if stored_ip == pod_ip.to_string() {
                    return Ok(Some(helpers::parse_pod_endpoint(&v)?));
                }
            }
            Ok(None)
        })
        .await
    }

    pub async fn pod_endpoint_list_all(&self) -> Result<Vec<PodEndpointRow>> {
        self.db_call("pod_endpoint_list_all_impl", move |db| {
            let r = db.begin_read()?;
            let t = r.open_table(tables::POD_ENDPOINTS)?;
            let mut rows = Vec::new();
            for e in t.iter()? {
                let (_, val) = e?;
                let v: Value = serde_json::from_slice(val.value()).unwrap_or_default();
                rows.push(helpers::parse_pod_endpoint(&v)?);
            }
            rows.sort_by(|a, b| a.pod_uid.cmp(&b.pod_uid));
            Ok(rows)
        })
        .await
    }

    pub fn subscribe_endpoints(&self) -> broadcast::Receiver<PodEndpointEvent> {
        self.endpoint_tx.subscribe()
    }
}

// Standalone helpers

fn parse_node_subnet_value(name: &str, body: &[u8]) -> Result<NodeSubnet> {
    let v: Value = serde_json::from_slice(body).unwrap_or_default();
    let node_name = NodeName::parse(name).map_err(|e| anyhow!("bad node name: {e}"))?;
    let subnet_str = v.get("subnet").and_then(|s| s.as_str()).unwrap_or("");
    let subnet = PodSubnet::parse(subnet_str).map_err(|e| anyhow!("bad subnet: {e}"))?;
    let subnet_base_int = v
        .get("subnet_base_int")
        .and_then(|x| x.as_u64())
        .unwrap_or(0) as u32;
    let vtep_ip_str = v.get("vtep_ip").and_then(|s| s.as_str()).unwrap_or("");
    let vtep_ip: Ipv4Addr = vtep_ip_str
        .parse()
        .map_err(|e| anyhow!("bad vtep_ip: {e}"))?;
    let node_ip_str = v.get("node_ip").and_then(|s| s.as_str()).unwrap_or("");
    let node_ip: Ipv4Addr = node_ip_str
        .parse()
        .map_err(|e| anyhow!("bad node_ip: {e}"))?;
    let mode_str = v.get("mode").and_then(|s| s.as_str()).unwrap_or("root");
    let hpr_str = v.get("hostport_range").and_then(|s| s.as_str());
    Ok(NodeSubnet {
        node_name,
        subnet,
        subnet_base_int,
        vtep_ip,
        node_ip,
        mode: parse_peer_mode(mode_str),
        hostport_range: hpr_str.and_then(|s| HostPortRange::parse(s).ok()),
    })
}

fn parse_peer_mode(s: &str) -> NodePeerMode {
    match s {
        "rootless" => NodePeerMode::Rootless,
        _ => NodePeerMode::Root,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::broadcast;

    use crate::datastore::redb::accessor::RedbAccessor;
    use crate::datastore::redb::helpers;
    use crate::datastore::redb::open_boundary;
    use crate::datastore::redb::sandbox::RedbSandboxStore;
    use crate::task_supervisor::TaskSupervisor;

    use super::*;

    fn store() -> RedbNetworkStore {
        let db = open_boundary::open_in_memory_blocking().unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(Default::default()));
        let accessor = Arc::new(RedbAccessor::new(Arc::new(db), supervisor));
        let (tx, _) = broadcast::channel(256);
        RedbNetworkStore::new(accessor, tx)
    }

    fn ipam_request<'a>(
        sandbox_id: &'a str,
        pod_name: &'a str,
        pod_uid: &'a str,
        subnet_size: u32,
    ) -> PodNetworkAllocationRequest<'a> {
        PodNetworkAllocationRequest::new(
            sandbox_id,
            PodNetworkAllocationPod::new("ns", pod_name, pod_uid),
            PodNetworkAllocationSubnet::new(0x0a000000, subnet_size),
            PodNetworkAllocationLink::new("veth0", "/ns"),
        )
    }

    #[test]
    fn parse_peer_mode_defaults_to_root() {
        assert_eq!(parse_peer_mode("root"), NodePeerMode::Root);
        assert_eq!(parse_peer_mode("garbage"), NodePeerMode::Root);
        assert_eq!(parse_peer_mode(""), NodePeerMode::Root);
    }

    #[test]
    fn parse_peer_mode_rootless() {
        assert_eq!(parse_peer_mode("rootless"), NodePeerMode::Rootless);
    }

    #[test]
    fn parse_pod_endpoint_infers_node_ip_from_pod_ip() {
        let v = serde_json::json!({"pod_uid":"u","namespace":"ns","pod_name":"p","node_name":"n","mode":"vxlan","pod_ip":"10.0.0.1"});
        let row = helpers::parse_pod_endpoint(&v).unwrap();
        assert_eq!(row.node_ip, std::net::Ipv4Addr::new(10, 0, 0, 1));
    }

    #[test]
    fn parse_pod_endpoint_explicit_node_ip() {
        let v = serde_json::json!({"pod_uid":"u","namespace":"ns","pod_name":"p","node_name":"n","mode":"vxlan","pod_ip":"10.0.0.1","node_ip":"192.168.0.1"});
        let row = helpers::parse_pod_endpoint(&v).unwrap();
        assert_eq!(row.node_ip, std::net::Ipv4Addr::new(192, 168, 0, 1));
    }

    #[test]
    fn parse_pod_endpoint_host_ports() {
        let v = serde_json::json!({"pod_uid":"u","namespace":"ns","pod_name":"p","node_name":"n","mode":"vxlan","pod_ip":"10.0.0.1","host_port_tcp":8080,"host_port_udp":9090});
        let row = helpers::parse_pod_endpoint(&v).unwrap();
        assert_eq!(row.host_port_tcp, Some(8080));
        assert_eq!(row.host_port_udp, Some(9090));
    }

    #[tokio::test]
    async fn ipam_alloc_idempotent() {
        let s = store();
        let (ip1, int1) = s
            .ipam_alloc(ipam_request("s1", "p", "u", 256))
            .await
            .unwrap();
        let (ip2, int2) = s
            .ipam_alloc(ipam_request("s1", "p", "u", 256))
            .await
            .unwrap();
        assert_eq!(ip1, ip2);
        assert_eq!(int1, int2);
    }

    #[tokio::test]
    async fn ipam_alloc_uses_first_free_ip() {
        let s = store();
        let (ip1, _) = s
            .ipam_alloc(ipam_request("s1", "p1", "u1", 256))
            .await
            .unwrap();
        let (ip2, _) = s
            .ipam_alloc(ipam_request("s2", "p2", "u2", 256))
            .await
            .unwrap();
        assert_ne!(ip1, ip2);
    }

    #[tokio::test]
    async fn ipam_alloc_exhaustion_errors() {
        let s = store();
        // Tiny subnet: only 2 usable IPs (subnet_base+2 .. subnet_base+sz-2)
        // sz=4 means range is 2..2 = empty
        let err = s
            .ipam_alloc(ipam_request("s1", "p", "u", 3))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("no free")
                || err.to_string().contains("usable")
                || err.to_string().contains("too small")
        );
    }

    #[tokio::test]
    async fn sandbox_record_and_get_by_uid() {
        let s = RedbSandboxStore::new(store().accessor.clone());
        s.record("ns", "pod", "uid-1", "sid-abc").await.unwrap();
        let got = s.get_for_uid("ns", "pod", "uid-1").await.unwrap();
        assert_eq!(got.as_deref(), Some("sid-abc"));
    }

    #[tokio::test]
    async fn sandbox_get_for_pod_returns_newest() {
        let s = RedbSandboxStore::new(store().accessor.clone());
        s.record("ns", "pod", "uid-a", "sid-old").await.unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        s.record("ns", "pod", "uid-b", "sid-new").await.unwrap();
        let newest = s.get_for_pod("ns", "pod").await.unwrap();
        assert_eq!(newest.as_deref(), Some("sid-new"));
    }

    #[tokio::test]
    async fn sandbox_delete_for_uid_only_removes_matching_sid() {
        let s = RedbSandboxStore::new(store().accessor.clone());
        s.record("ns", "pod", "uid-1", "sid-a").await.unwrap();
        s.delete_for_uid("ns", "pod", "uid-1", "sid-wrong")
            .await
            .unwrap();
        assert!(s.get_for_uid("ns", "pod", "uid-1").await.unwrap().is_some());
        s.delete_for_uid("ns", "pod", "uid-1", "sid-a")
            .await
            .unwrap();
        assert!(s.get_for_uid("ns", "pod", "uid-1").await.unwrap().is_none());
    }
}
