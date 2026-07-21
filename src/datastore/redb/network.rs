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

            let existing_bytes: Option<Vec<u8>> = {
                let t = w.open_table(tables::POD_NETWORKS)?;
                let opt = t.get(request.sandbox_id)?;
                opt.map(|g| g.value().to_vec())
            };
            if let Some(bytes) = existing_bytes {
                let existing = parse_pod_network_value(&bytes)?;
                w.commit()?;
                return Ok((existing.ip.to_string(), existing.ip_int));
            }

            let mut used = BTreeSet::new();
            {
                let t = w.open_table(tables::POD_NETWORKS)?;
                for e in t.iter()? {
                    let (_, value) = e?;
                    used.insert(parse_pod_network_value(value.value())?.ip_int);
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
            match t.get(sid)? {
                Some(value) => Ok(Some(parse_pod_network_value(value.value())?.endpoint())),
                None => Ok(None),
            }
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
                let network = parse_pod_network_value(val.value())?;
                if network.namespace == ns
                    && network.pod_name == pod_name
                    && network.pod_uid == pod_uid
                {
                    return Ok(Some(network.endpoint()));
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
                let existing = parse_persisted_node_subnet(&bytes)?;

                w.commit()?;
                return Ok(NodeSubnet {
                    node_name: node_name_typed,
                    subnet: existing.subnet,
                    subnet_base_int: existing.subnet_base_int,
                    gateway_ip: existing.gateway_ip,
                    node_ip: node_ip_typed,
                    mode: existing.mode,
                    hostport_range: existing.hostport_range,
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
                    allocated.insert(parse_persisted_node_subnet(val.value())?.subnet_base_int);
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
                    gateway_ip: vtep_ip,
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
                let mut v: Value = serde_json::from_slice(&bytes)
                    .map_err(|error| anyhow!("malformed persisted node subnet JSON: {error}"))?;
                let obj = v
                    .as_object_mut()
                    .ok_or_else(|| anyhow!("persisted node subnet must be a JSON object"))?;
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
                let encoded = serde_json::to_vec(&v)?;
                parse_persisted_node_subnet(&encoded)?;
                let mut t = w.open_table(tables::NODE_SUBNETS)?;
                t.insert(node_name, encoded.as_slice())?;
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
                    let body: Value = serde_json::from_slice(value.value()).map_err(|error| {
                        anyhow!("malformed persisted node dataplane JSON: {error}")
                    })?;
                    let mode = required_persisted_string(&body, "node dataplane", "mode")?;
                    let encryption =
                        required_persisted_string(&body, "node dataplane", "encryption")?;
                    let public_key =
                        optional_persisted_string(&body, "node dataplane", "public_key")?;
                    let endpoint = optional_persisted_string(&body, "node dataplane", "endpoint")?;
                    let port = optional_persisted_port(&body, "port")?;
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
                let v: Value = serde_json::from_slice(val.value())
                    .map_err(|error| anyhow!("malformed persisted pod endpoint JSON: {error}"))?;
                let endpoint = helpers::parse_pod_endpoint(&v)?;
                if endpoint.pod_ip == pod_ip {
                    return Ok(Some(endpoint));
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
                let v: Value = serde_json::from_slice(val.value())
                    .map_err(|error| anyhow!("malformed persisted pod endpoint JSON: {error}"))?;
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

struct PersistedPodNetwork {
    ip: Ipv4Addr,
    ip_int: u32,
    veth_host: String,
    netns_path: String,
    namespace: String,
    pod_name: String,
    pod_uid: String,
}

impl PersistedPodNetwork {
    fn endpoint(self) -> PodNetworkEndpoint {
        PodNetworkEndpoint {
            ip_addr: self.ip.to_string(),
            veth_host: self.veth_host,
            netns_path: self.netns_path,
        }
    }
}

fn parse_pod_network_value(body: &[u8]) -> Result<PersistedPodNetwork> {
    let value: Value = serde_json::from_slice(body)
        .map_err(|error| anyhow!("malformed persisted pod network JSON: {error}"))?;
    let ip: Ipv4Addr = required_persisted_string(&value, "pod network", "ip")?
        .parse()
        .map_err(|error| anyhow!("invalid persisted pod network IP: {error}"))?;
    let ip_int = required_persisted_u32(&value, "pod network", "ip_int")?;
    if ip_int != u32::from(ip) {
        return Err(anyhow!(
            "persisted pod network ip_int does not match its IP address"
        ));
    }
    Ok(PersistedPodNetwork {
        ip,
        ip_int,
        veth_host: required_persisted_string(&value, "pod network", "veth")?.to_string(),
        netns_path: required_persisted_string(&value, "pod network", "netns")?.to_string(),
        namespace: required_persisted_string(&value, "pod network", "ns")?.to_string(),
        pod_name: required_persisted_string(&value, "pod network", "pod")?.to_string(),
        pod_uid: required_persisted_string(&value, "pod network", "uid")?.to_string(),
    })
}

struct PersistedNodeSubnet {
    subnet: PodSubnet,
    subnet_base_int: u32,
    gateway_ip: Ipv4Addr,
    node_ip: Ipv4Addr,
    mode: NodePeerMode,
    hostport_range: Option<HostPortRange>,
}

fn parse_persisted_node_subnet(body: &[u8]) -> Result<PersistedNodeSubnet> {
    let value: Value = serde_json::from_slice(body)
        .map_err(|error| anyhow!("malformed persisted node subnet JSON: {error}"))?;
    let subnet_str = required_persisted_string(&value, "node subnet", "subnet")?;
    let subnet = PodSubnet::parse(subnet_str).map_err(|error| anyhow!("bad subnet: {error}"))?;
    if subnet.prefix() != 24 {
        return Err(anyhow!("persisted node subnet must use a /24 prefix"));
    }
    if subnet.to_string() != subnet_str {
        return Err(anyhow!("persisted node subnet CIDR must be canonical"));
    }
    let subnet_base_int = required_persisted_u32(&value, "node subnet", "subnet_base_int")?;
    if subnet_base_int != subnet.base() {
        return Err(anyhow!(
            "persisted node subnet base integer does not match its CIDR"
        ));
    }
    let gateway_ip: Ipv4Addr = required_persisted_string(&value, "node subnet", "vtep_ip")?
        .parse()
        .map_err(|error| anyhow!("bad vtep_ip: {error}"))?;
    if gateway_ip != subnet.base_ip() {
        return Err(anyhow!(
            "persisted node subnet gateway compatibility field does not match its CIDR"
        ));
    }
    let node_ip = required_persisted_string(&value, "node subnet", "node_ip")?
        .parse()
        .map_err(|error| anyhow!("bad node_ip: {error}"))?;
    let mode = parse_persisted_peer_mode(&value)?;
    let hostport_range = parse_persisted_hostport_range(&value)?;
    match (&mode, &hostport_range) {
        (NodePeerMode::Root, Some(_)) => {
            return Err(anyhow!(
                "persisted root node subnet must not carry a host-port range"
            ));
        }
        (NodePeerMode::Rootless, None) => {
            return Err(anyhow!(
                "persisted rootless node subnet requires a host-port range"
            ));
        }
        _ => {}
    }
    Ok(PersistedNodeSubnet {
        subnet,
        subnet_base_int,
        gateway_ip,
        node_ip,
        mode,
        hostport_range,
    })
}

fn parse_node_subnet_value(name: &str, body: &[u8]) -> Result<NodeSubnet> {
    let node_name = NodeName::parse(name).map_err(|e| anyhow!("bad node name: {e}"))?;
    let subnet = parse_persisted_node_subnet(body)?;
    Ok(NodeSubnet {
        node_name,
        subnet: subnet.subnet,
        subnet_base_int: subnet.subnet_base_int,
        gateway_ip: subnet.gateway_ip,
        node_ip: subnet.node_ip,
        mode: subnet.mode,
        hostport_range: subnet.hostport_range,
    })
}

fn required_persisted_string<'a>(value: &'a Value, record: &str, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("{record} field {field} must be a string"))
}

fn required_persisted_u32(value: &Value, record: &str, field: &str) -> Result<u32> {
    let raw = value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("{record} field {field} must be a non-negative integer"))?;
    u32::try_from(raw).map_err(|error| anyhow!("{record} field {field} outside u32: {error}"))
}

fn optional_persisted_string(value: &Value, record: &str, field: &str) -> Result<Option<String>> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(anyhow!("{record} field {field} must be a string or null")),
    }
}

fn optional_persisted_port(value: &Value, field: &str) -> Result<Option<u16>> {
    let Some(raw) = value.get(field) else {
        return Ok(None);
    };
    if raw.is_null() {
        return Ok(None);
    }
    let raw = raw
        .as_i64()
        .ok_or_else(|| anyhow!("node dataplane field {field} must be an integer"))?;
    u16::try_from(raw)
        .ok()
        .filter(|port| *port != 0)
        .map(Some)
        .ok_or_else(|| anyhow!("node dataplane field {field} outside 1..=65535"))
}

fn parse_persisted_hostport_range(value: &Value) -> Result<Option<HostPortRange>> {
    match value.get("hostport_range") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(range)) => HostPortRange::parse(range)
            .map(Some)
            .map_err(|error| anyhow!("invalid persisted hostport_range {range:?}: {error}")),
        Some(_) => Err(anyhow!(
            "node subnet field hostport_range must be a string or null"
        )),
    }
}

fn parse_persisted_peer_mode(value: &Value) -> Result<NodePeerMode> {
    match value.get("mode") {
        None => Ok(NodePeerMode::Root),
        Some(Value::String(mode)) => parse_peer_mode(mode),
        Some(_) => Err(anyhow!("node subnet field mode must be a string")),
    }
}

fn parse_peer_mode(s: &str) -> Result<NodePeerMode> {
    match s {
        "root" => Ok(NodePeerMode::Root),
        "rootless" => Ok(NodePeerMode::Rootless),
        other => Err(anyhow!(
            "unknown persisted node peer mode {other:?}, expected root or rootless"
        )),
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
    fn parse_peer_mode_rejects_unknown_values() {
        assert_eq!(parse_peer_mode("root").unwrap(), NodePeerMode::Root);
        assert!(parse_peer_mode("garbage").is_err());
        assert!(parse_peer_mode("").is_err());
    }

    #[test]
    fn parse_peer_mode_rootless() {
        assert_eq!(parse_peer_mode("rootless").unwrap(), NodePeerMode::Rootless);
    }

    #[test]
    fn parse_pod_endpoint_infers_node_ip_from_pod_ip() {
        let v = serde_json::json!({"pod_uid":"u","namespace":"ns","pod_name":"p","node_name":"n","mode":"encrypted_direct","pod_ip":"10.0.0.1","generation":1,"updated_at":2});
        let row = helpers::parse_pod_endpoint(&v).unwrap();
        assert_eq!(row.node_ip, std::net::Ipv4Addr::new(10, 0, 0, 1));
    }

    #[test]
    fn parse_pod_endpoint_explicit_node_ip() {
        let v = serde_json::json!({"pod_uid":"u","namespace":"ns","pod_name":"p","node_name":"n","mode":"encrypted_direct","pod_ip":"10.0.0.1","node_ip":"192.168.0.1","generation":1,"updated_at":2});
        let row = helpers::parse_pod_endpoint(&v).unwrap();
        assert_eq!(row.node_ip, std::net::Ipv4Addr::new(192, 168, 0, 1));
    }

    #[test]
    fn parse_pod_endpoint_host_ports() {
        let v = serde_json::json!({"pod_uid":"u","namespace":"ns","pod_name":"p","node_name":"n","mode":"hostport","pod_ip":"10.0.0.1","node_ip":"192.0.2.1","host_port_tcp":8080,"host_port_udp":9090,"generation":1,"updated_at":2});
        let row = helpers::parse_pod_endpoint(&v).unwrap();
        assert_eq!(row.host_port_tcp, Some(8080));
        assert_eq!(row.host_port_udp, Some(9090));
    }

    #[test]
    fn parse_pod_endpoint_rejects_malformed_mode_ip_and_ports() {
        let cases = [
            (
                "unknown mode",
                serde_json::json!({"pod_uid":"u","namespace":"ns","pod_name":"p","node_name":"n","mode":"vxlan","pod_ip":"10.0.0.1","generation":1,"updated_at":2}),
            ),
            (
                "invalid pod IP",
                serde_json::json!({"pod_uid":"u","namespace":"ns","pod_name":"p","node_name":"n","mode":"encrypted_direct","pod_ip":"not-an-ip","generation":1,"updated_at":2}),
            ),
            (
                "invalid node IP",
                serde_json::json!({"pod_uid":"u","namespace":"ns","pod_name":"p","node_name":"n","mode":"hostport","pod_ip":"10.0.0.1","node_ip":"not-an-ip","host_port_tcp":8080,"generation":1,"updated_at":2}),
            ),
            (
                "zero port",
                serde_json::json!({"pod_uid":"u","namespace":"ns","pod_name":"p","node_name":"n","mode":"hostport","pod_ip":"10.0.0.1","node_ip":"192.0.2.1","host_port_tcp":0,"generation":1,"updated_at":2}),
            ),
            (
                "negative port",
                serde_json::json!({"pod_uid":"u","namespace":"ns","pod_name":"p","node_name":"n","mode":"hostport","pod_ip":"10.0.0.1","node_ip":"192.0.2.1","host_port_udp":-1,"generation":1,"updated_at":2}),
            ),
            (
                "oversized port",
                serde_json::json!({"pod_uid":"u","namespace":"ns","pod_name":"p","node_name":"n","mode":"hostport","pod_ip":"10.0.0.1","node_ip":"192.0.2.1","host_port_tcp":65536,"generation":1,"updated_at":2}),
            ),
        ];
        for (name, value) in cases {
            assert!(
                helpers::parse_pod_endpoint(&value).is_err(),
                "{name} must be rejected instead of normalized: {value}"
            );
        }
    }

    async fn insert_raw_pod_endpoint(store: &RedbNetworkStore, key: &str, body: &[u8]) {
        let key = key.to_string();
        let body = body.to_vec();
        store
            .accessor
            .call("insert_raw_pod_endpoint_test", move |db| {
                let write = db.begin_write()?;
                {
                    let mut table = write.open_table(tables::POD_ENDPOINTS)?;
                    table.insert(key.as_str(), body.as_slice())?;
                }
                Ok(write.commit()?)
            })
            .await
            .unwrap();
    }

    async fn insert_raw_pod_network(store: &RedbNetworkStore, key: &str, body: &[u8]) {
        let key = key.to_string();
        let body = body.to_vec();
        store
            .accessor
            .call("insert_raw_pod_network_test", move |db| {
                let write = db.begin_write()?;
                {
                    let mut table = write.open_table(tables::POD_NETWORKS)?;
                    table.insert(key.as_str(), body.as_slice())?;
                }
                Ok(write.commit()?)
            })
            .await
            .unwrap();
    }

    fn valid_pod_network_value() -> Value {
        serde_json::json!({
            "ip": "10.0.0.2",
            "ip_int": 0x0a000002_u32,
            "veth": "veth0",
            "netns": "/ns",
            "ns": "ns",
            "pod": "pod",
            "uid": "uid",
        })
    }

    fn valid_node_subnet_value() -> Value {
        serde_json::json!({
            "subnet": "10.42.7.0/24",
            "subnet_base_int": 0x0a2a0700_u32,
            "vtep_ip": "10.42.7.0",
            "node_ip": "192.0.2.7",
            "mode": "root",
            "hostport_range": null,
        })
    }

    async fn insert_raw_node_subnet_value(store: &RedbNetworkStore, key: &str, value: Value) {
        let key = key.to_string();
        let body = serde_json::to_vec(&value).unwrap();
        store
            .accessor
            .call("insert_raw_node_subnet_test", move |db| {
                let write = db.begin_write()?;
                {
                    let mut table = write.open_table(tables::NODE_SUBNETS)?;
                    table.insert(key.as_str(), body.as_slice())?;
                }
                Ok(write.commit()?)
            })
            .await
            .unwrap();
    }

    async fn insert_raw_node_subnet(store: &RedbNetworkStore, key: &str, mode: &str) {
        let mut value = valid_node_subnet_value();
        value["mode"] = Value::String(mode.to_string());
        insert_raw_node_subnet_value(store, key, value).await;
    }

    async fn insert_raw_node_dataplane(store: &RedbNetworkStore, key: &str, body: Value) {
        let key = key.to_string();
        let body = serde_json::to_vec(&body).unwrap();
        store
            .accessor
            .call("insert_raw_node_dataplane_test", move |db| {
                let write = db.begin_write()?;
                {
                    let mut table = write.open_table(tables::NODE_DATAPLANE)?;
                    table.insert(key.as_str(), body.as_slice())?;
                }
                Ok(write.commit()?)
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn pod_endpoint_read_and_list_reject_malformed_json() {
        let s = store();
        insert_raw_pod_endpoint(&s, "broken", b"{not-json").await;

        assert!(s.pod_endpoint_list_all().await.is_err());
        assert!(
            s.pod_endpoint_get_by_pod_ip(Ipv4Addr::new(10, 0, 0, 1))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn node_subnet_read_paths_reject_unknown_persisted_mode() {
        let s = store();
        insert_raw_node_subnet(&s, "peer-a", "mystery").await;

        assert!(s.get_node_subnet("peer-a").await.is_err());
        assert!(s.list_peer_subnets("local-node").await.is_err());
        assert!(
            s.allocate_node_subnet("peer-a", "10.42.0.0/16", "192.0.2.7")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn node_subnet_paths_reject_invalid_base_and_redundancy() {
        let mut cases = Vec::new();

        let mut missing = valid_node_subnet_value();
        missing.as_object_mut().unwrap().remove("subnet_base_int");
        cases.push(("missing base", missing));

        for (name, base) in [
            ("negative base", serde_json::json!(-1)),
            ("non-integer base", serde_json::json!("170526464")),
            ("fractional base", serde_json::json!(170526464.5)),
            (
                "base outside u32",
                serde_json::json!(u64::from(u32::MAX) + 1),
            ),
            ("CIDR/base mismatch", serde_json::json!(0x0a2a0800_u32)),
        ] {
            let mut value = valid_node_subnet_value();
            value["subnet_base_int"] = base;
            cases.push((name, value));
        }

        for (name, subnet) in [
            ("non-/24 CIDR", "10.42.7.0/25"),
            ("CIDR with host bits", "10.42.7.7/24"),
        ] {
            let mut value = valid_node_subnet_value();
            value["subnet"] = Value::String(subnet.to_string());
            cases.push((name, value));
        }

        let mut gateway_mismatch = valid_node_subnet_value();
        gateway_mismatch["vtep_ip"] = Value::String("10.42.7.1".to_string());
        cases.push(("CIDR/gateway mismatch", gateway_mismatch));

        let mut root_with_range = valid_node_subnet_value();
        root_with_range["hostport_range"] = Value::String("20000-20999".to_string());
        cases.push(("root peer with host-port range", root_with_range));

        let mut rootless_without_range = valid_node_subnet_value();
        rootless_without_range["mode"] = Value::String("rootless".to_string());
        cases.push((
            "rootless peer without host-port range",
            rootless_without_range,
        ));

        for (name, value) in cases {
            let s = store();
            insert_raw_node_subnet_value(&s, "peer-a", value.clone()).await;
            assert!(
                s.get_node_subnet("peer-a").await.is_err(),
                "{name} must fail get"
            );
            assert!(
                s.list_peer_subnets("local-node").await.is_err(),
                "{name} must fail list"
            );
            assert!(
                s.allocate_node_subnet("peer-a", "10.42.0.0/16", "192.0.2.7")
                    .await
                    .is_err(),
                "{name} must fail idempotent allocation"
            );

            let s = store();
            insert_raw_node_subnet_value(&s, "peer-a", value).await;
            assert!(
                s.allocate_node_subnet("fresh-peer", "10.42.0.0/16", "192.0.2.8")
                    .await
                    .is_err(),
                "{name} must fail allocation scan"
            );
        }
    }

    #[tokio::test]
    async fn node_subnet_legacy_missing_mode_defaults_to_root() {
        for (name, hostport) in [("absent", None), ("null", Some(Value::Null))] {
            let s = store();
            let mut value = valid_node_subnet_value();
            let object = value.as_object_mut().unwrap();
            object.remove("mode");
            match hostport {
                Some(value) => {
                    object.insert("hostport_range".into(), value);
                }
                None => {
                    object.remove("hostport_range");
                }
            }
            insert_raw_node_subnet_value(&s, "peer-a", value).await;

            let fetched = s.get_node_subnet("peer-a").await.unwrap().unwrap();
            assert_eq!(fetched.mode, NodePeerMode::Root, "{name} hostport field");
            assert_eq!(fetched.hostport_range, None, "{name} hostport field");

            let listed = s.list_peer_subnets("local-node").await.unwrap();
            assert_eq!(listed.len(), 1);
            assert_eq!(listed[0].mode, NodePeerMode::Root);
            assert_eq!(listed[0].hostport_range, None);

            let allocated = s
                .allocate_node_subnet("peer-a", "10.42.0.0/16", "192.0.2.8")
                .await
                .unwrap();
            assert_eq!(allocated.mode, NodePeerMode::Root);
            assert_eq!(allocated.hostport_range, None);
        }
    }

    #[tokio::test]
    async fn node_subnet_paths_reject_present_non_string_mode() {
        for (name, mode) in [("null", Value::Null), ("number", serde_json::json!(1))] {
            let s = store();
            let mut value = valid_node_subnet_value();
            value["mode"] = mode;
            insert_raw_node_subnet_value(&s, "peer-a", value).await;
            assert!(s.get_node_subnet("peer-a").await.is_err(), "{name}");
            assert!(s.list_peer_subnets("local-node").await.is_err(), "{name}");
            assert!(
                s.allocate_node_subnet("peer-a", "10.42.0.0/16", "192.0.2.7")
                    .await
                    .is_err(),
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn update_peer_attrs_rejects_self_poisoning_candidates_without_mutating_prior_row() {
        for (name, mode, range) in [
            (
                "root with range",
                NodePeerMode::Root,
                Some(HostPortRange::parse("20000-20999").unwrap()),
            ),
            ("rootless without range", NodePeerMode::Rootless, None),
        ] {
            let s = store();
            insert_raw_node_subnet_value(&s, "peer-a", valid_node_subnet_value()).await;
            let before = s.get_node_subnet("peer-a").await.unwrap().unwrap();

            assert!(
                s.update_peer_attrs("peer-a", mode, range).await.is_err(),
                "{name} must be rejected before persistence"
            );
            assert_eq!(
                s.get_node_subnet("peer-a").await.unwrap().unwrap(),
                before,
                "{name} must preserve the prior readable row"
            );
        }
    }

    #[tokio::test]
    async fn update_peer_attrs_accepts_both_valid_mode_range_combinations() {
        let s = store();
        insert_raw_node_subnet_value(&s, "peer-a", valid_node_subnet_value()).await;
        let rootless_range = HostPortRange::parse("20000-20999").unwrap();

        s.update_peer_attrs("peer-a", NodePeerMode::Rootless, Some(rootless_range))
            .await
            .unwrap();
        let rootless = s.get_node_subnet("peer-a").await.unwrap().unwrap();
        assert_eq!(rootless.mode, NodePeerMode::Rootless);
        assert_eq!(rootless.hostport_range, Some(rootless_range));

        s.update_peer_attrs("peer-a", NodePeerMode::Root, None)
            .await
            .unwrap();
        let root = s.get_node_subnet("peer-a").await.unwrap().unwrap();
        assert_eq!(root.mode, NodePeerMode::Root);
        assert_eq!(root.hostport_range, None);
    }

    #[test]
    fn node_subnet_decoder_rejects_malformed_json_and_hostport_range() {
        assert!(parse_node_subnet_value("peer-a", b"{not-json").is_err());
        let invalid_range = serde_json::to_vec(&serde_json::json!({
            "subnet": "10.42.7.0/24",
            "subnet_base_int": 0x0a2a0700_u32,
            "vtep_ip": "10.42.7.0",
            "node_ip": "192.0.2.7",
            "mode": "rootless",
            "hostport_range": "not-a-range",
        }))
        .unwrap();
        assert!(parse_node_subnet_value("peer-a", &invalid_range).is_err());
    }

    #[tokio::test]
    async fn node_dataplane_read_rejects_malformed_json_and_ports() {
        for (name, port) in [
            ("negative", serde_json::json!(-1)),
            ("zero", serde_json::json!(0)),
            ("oversized", serde_json::json!(65536)),
            ("non-integer", serde_json::json!("51820")),
        ] {
            let s = store();
            insert_raw_node_dataplane(
                &s,
                "peer-a",
                serde_json::json!({
                    "mode": "root",
                    "encryption": "disabled",
                    "public_key": null,
                    "endpoint": "192.0.2.7",
                    "port": port,
                }),
            )
            .await;
            assert!(
                s.get_node_dataplane("peer-a").await.is_err(),
                "{name} persisted dataplane port must be rejected"
            );
        }

        let s = store();
        let key = "broken".to_string();
        s.accessor
            .call("insert_malformed_node_dataplane_test", move |db| {
                let write = db.begin_write()?;
                {
                    let mut table = write.open_table(tables::NODE_DATAPLANE)?;
                    table.insert(key.as_str(), b"{not-json".as_slice())?;
                }
                Ok(write.commit()?)
            })
            .await
            .unwrap();
        assert!(s.get_node_dataplane("broken").await.is_err());
    }

    #[tokio::test]
    async fn node_dataplane_read_rejects_invalid_field_shapes_and_invariants() {
        let valid = || {
            serde_json::json!({
                "mode": "root",
                "encryption": "disabled",
                "public_key": null,
                "endpoint": "192.0.2.7",
                "port": null,
            })
        };
        let mut cases = Vec::new();

        for field in ["mode", "encryption"] {
            for (shape, replacement) in [
                ("missing", None),
                ("null", Some(Value::Null)),
                ("non-string", Some(Value::Bool(true))),
            ] {
                let mut value = valid();
                match replacement {
                    Some(replacement) => value[field] = replacement,
                    None => {
                        value.as_object_mut().unwrap().remove(field);
                    }
                }
                cases.push((format!("{shape} {field}"), value));
            }
        }

        for (field, invalid) in [("mode", "mystery"), ("encryption", "mystery")] {
            let mut value = valid();
            value[field] = Value::String(invalid.to_string());
            cases.push((format!("unknown {field}"), value));
        }

        for field in ["public_key", "endpoint"] {
            let mut value = valid();
            value[field] = Value::Bool(true);
            cases.push((format!("non-string {field}"), value));
        }

        let mut missing_key = valid();
        missing_key["encryption"] = Value::String("enabled".to_string());
        missing_key["port"] = serde_json::json!(51820);
        cases.push(("encrypted row missing public key".to_string(), missing_key));

        let mut missing_port = valid();
        missing_port["encryption"] = Value::String("enabled".to_string());
        missing_port["public_key"] =
            Value::String("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string());
        cases.push(("encrypted row missing port".to_string(), missing_port));

        for (name, value) in cases {
            let s = store();
            insert_raw_node_dataplane(&s, "peer-a", value).await;
            assert!(
                s.get_node_dataplane("peer-a").await.is_err(),
                "{name} must be rejected"
            );
        }
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
    async fn pod_network_paths_reject_corrupt_persisted_rows() {
        let mut cases: Vec<(String, Vec<u8>)> =
            vec![("malformed JSON".to_string(), b"{not-json".to_vec())];

        for field in ["ip", "ip_int", "veth", "netns", "ns", "pod", "uid"] {
            let mut missing = valid_pod_network_value();
            missing.as_object_mut().unwrap().remove(field);
            cases.push((
                format!("missing {field}"),
                serde_json::to_vec(&missing).unwrap(),
            ));

            let mut wrong_type = valid_pod_network_value();
            wrong_type[field] = Value::Bool(true);
            cases.push((
                format!("wrong-type {field}"),
                serde_json::to_vec(&wrong_type).unwrap(),
            ));
        }

        for (name, ip_int) in [
            ("negative ip_int", serde_json::json!(-1)),
            (
                "ip_int outside u32",
                serde_json::json!(u64::from(u32::MAX) + 1),
            ),
            ("IP/ip_int mismatch", serde_json::json!(0x0a000003_u32)),
        ] {
            let mut value = valid_pod_network_value();
            value["ip_int"] = ip_int;
            cases.push((name.to_string(), serde_json::to_vec(&value).unwrap()));
        }

        for (name, body) in cases {
            let s = store();
            insert_raw_pod_network(&s, "corrupt", &body).await;
            assert!(
                s.ipam_alloc(ipam_request("corrupt", "pod", "uid", 256))
                    .await
                    .is_err(),
                "{name} must fail idempotent allocation"
            );
            assert!(
                s.ipam_alloc(ipam_request("fresh", "fresh", "fresh-uid", 256))
                    .await
                    .is_err(),
                "{name} must fail the used-address scan"
            );
            assert!(
                s.get_pnet("corrupt").await.is_err(),
                "{name} must fail get-by-sandbox"
            );
            assert!(
                s.get_pnet_for_pod("ns", "pod", "uid").await.is_err(),
                "{name} must fail get-by-pod"
            );
        }
    }

    #[tokio::test]
    async fn valid_persisted_pod_network_row_remains_compatible() {
        let s = store();
        let body = serde_json::to_vec(&valid_pod_network_value()).unwrap();
        insert_raw_pod_network(&s, "legacy", &body).await;

        let existing = s
            .ipam_alloc(ipam_request("legacy", "pod", "uid", 256))
            .await
            .unwrap();
        assert_eq!(existing, ("10.0.0.2".to_string(), 0x0a000002));

        let by_sandbox = s.get_pnet("legacy").await.unwrap().unwrap();
        let by_pod = s
            .get_pnet_for_pod("ns", "pod", "uid")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(by_sandbox, by_pod);
        assert_eq!(by_sandbox.ip_addr, "10.0.0.2");

        let fresh = s
            .ipam_alloc(ipam_request("fresh", "fresh", "fresh-uid", 256))
            .await
            .unwrap();
        assert_ne!(fresh.1, 0x0a000002);
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
