use super::prelude::*;
use super::*;

// ============ ServiceSpec =================================================
// Pure-data representation of one Service for nft rule generation. Decouples
// the JSON-walking logic from the rule-building logic so the rule builder
// is unit-testable without needing real K8s objects.

/// L4 protocol klights routes. K8s spec also allows SCTP but it's vanishingly
/// rare and not exercised by chainsaw — defer until a real ask appears.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Protocol {
    Tcp,
    Udp,
}

impl Protocol {
    /// IPv4 header `protocol` field value (tcp = 6, udp = 17).
    pub(super) fn ip_proto(self) -> u8 {
        match self {
            Protocol::Tcp => libc::IPPROTO_TCP as u8,
            Protocol::Udp => libc::IPPROTO_UDP as u8,
        }
    }

    /// Transport-header dport field for the matching protocol.
    pub(super) fn dport_field(self) -> TransportHeaderField {
        match self {
            Protocol::Tcp => TransportHeaderField::Tcp(TcpHeaderField::Dport),
            Protocol::Udp => TransportHeaderField::Udp(UdpHeaderField::Dport),
        }
    }

    /// Parse from a K8s spec `protocol` string. Defaults to TCP if absent
    /// (matches the K8s default).
    pub fn parse(s: Option<&str>) -> Option<Self> {
        let protocol = s.filter(|value| !value.is_empty()).unwrap_or("TCP");
        match protocol.to_ascii_uppercase().as_str() {
            "TCP" => Some(Protocol::Tcp),
            "UDP" => Some(Protocol::Udp),
            _ => None,
        }
    }
}

/// One remote rootless pod endpoint that the root-side nft table can DNAT.
///
/// The match key is the pod IP plus L4 protocol; the target is the rootless
/// node's host-reachable IP plus the per-pod hostport published through
/// `pod_endpoints`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RemotePodEndpointSpec {
    pub pod_ip: Ipv4Addr,
    pub node_ip: Ipv4Addr,
    pub host_port: u16,
    pub protocol: Protocol,
}

/// Build the remote-pod DNAT inventory for this node from `pod_endpoints`.
/// Local rows and direct VXLAN rows are ignored; only remote hostport rows
/// belong in the root-side hybrid chain.
pub fn remote_pod_endpoint_specs_from_rows(
    local_node_name: &str,
    rows: Vec<crate::datastore::PodEndpointRow>,
) -> Vec<RemotePodEndpointSpec> {
    let mut specs = Vec::new();
    for row in rows {
        if row.node_name == local_node_name
            || row.mode != crate::datastore::PodEndpointMode::Hostport
        {
            continue;
        }
        if let Some(host_port) = row.host_port_tcp {
            specs.push(RemotePodEndpointSpec {
                pod_ip: row.pod_ip,
                node_ip: row.node_ip,
                host_port,
                protocol: Protocol::Tcp,
            });
        }
        if let Some(host_port) = row.host_port_udp {
            specs.push(RemotePodEndpointSpec {
                pod_ip: row.pod_ip,
                node_ip: row.node_ip,
                host_port,
                protocol: Protocol::Udp,
            });
        }
    }
    specs.sort_by_key(|spec| {
        (
            u32::from(spec.pod_ip),
            match spec.protocol {
                Protocol::Tcp => 0u8,
                Protocol::Udp => 1u8,
            },
            spec.host_port,
        )
    });
    specs
}

/// One service-port + its endpoints.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortSpec {
    /// ClusterIP-side port (`spec.ports[].port`).
    pub service_port: u16,
    /// Pod-side port (`spec.ports[].targetPort` if set, else `port`).
    pub target_port: u16,
    /// `spec.ports[].nodePort` for NodePort/LoadBalancer services. None
    /// for plain ClusterIP.
    pub node_port: Option<u16>,
    pub protocol: Protocol,
    /// Ready endpoint IPs. Empty = no DNAT for this port.
    pub endpoints: Vec<Ipv4Addr>,
}

/// All the data needed to write nft rules for one Service.
/// Headless services and ExternalName services are filtered out before
/// they reach this struct; if you have a `ServiceSpec`, it is routable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceSpec {
    pub cluster_ip: Ipv4Addr,
    pub ports: Vec<PortSpec>,
    /// `ClientIP` enables source-IP–based session affinity via `jhash ip saddr`.
    /// None/`None` means no affinity (random load balancing, default).
    pub session_affinity: SessionAffinity,
}

/// Conntrack original-direction tuple for one active ClusterIP service port.
///
/// nft NAT decisions are cached in conntrack. When a Service drops a UDP port,
/// old UDP mappings can otherwise keep DNATing until the kernel timeout. The
/// forward-chain guard uses these tuples to allow only still-live ClusterIP
/// service mappings and to drop stale mappings inside the Service CIDR.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ServiceCtTuple {
    pub cluster_ip: Ipv4Addr,
    pub protocol: Protocol,
    pub service_port: u16,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ServiceRuleKey {
    cluster_ip: Ipv4Addr,
    session_affinity: u8,
    service_port: u16,
    target_port: u16,
    node_port: Option<u16>,
    protocol: Protocol,
    endpoint: Ipv4Addr,
}

/// Canonical, order-insensitive view of the nft service DNAT rule inventory.
///
/// Datastore watch ordering can change without changing the effective Service
/// rules. The router uses this snapshot to skip kernel rewrites when the same
/// set of ClusterIP/NodePort endpoint mappings is already installed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceRuleSnapshot {
    entries: Vec<ServiceRuleKey>,
}

impl ServiceRuleSnapshot {
    pub fn from_services(services: &[ServiceSpec]) -> Self {
        let mut entries = Vec::new();
        for service in services {
            let session_affinity = match service.session_affinity {
                SessionAffinity::None => 0,
                SessionAffinity::ClientIp => 1,
            };
            for port in &service.ports {
                for endpoint in &port.endpoints {
                    entries.push(ServiceRuleKey {
                        cluster_ip: service.cluster_ip,
                        session_affinity,
                        service_port: port.service_port,
                        target_port: port.target_port,
                        node_port: port.node_port,
                        protocol: port.protocol,
                        endpoint: *endpoint,
                    });
                }
            }
        }
        entries.sort();
        Self { entries }
    }
}

pub fn service_ct_guard_tuples(services: &[ServiceSpec]) -> Vec<ServiceCtTuple> {
    let mut tuples = Vec::new();
    for service in services {
        for port in &service.ports {
            if port.endpoints.is_empty() {
                continue;
            }
            tuples.push(ServiceCtTuple {
                cluster_ip: service.cluster_ip,
                protocol: port.protocol,
                service_port: port.service_port,
            });
        }
    }
    tuples.sort();
    tuples.dedup();
    tuples
}

#[cfg(test)]
pub fn service_ct_guard_applies_to_forward_packet(
    bridge_ifname: &str,
    iifname: &str,
    oifname: &str,
) -> bool {
    iifname == bridge_ifname || oifname == bridge_ifname
}

pub fn legacy_unscoped_service_tables_to_cleanup(current_table: &str) -> Vec<&'static str> {
    if current_table == "klights" {
        Vec::new()
    } else {
        vec!["klights"]
    }
}

impl ServiceSpec {
    /// Parse from the JSON shape `Service`+`Endpoints` arrives in from
    /// the klights datastore. Returns `None` for services that should
    /// not produce nft rules: missing/None/empty cluster IP, ExternalName
    /// type, no parseable ports.
    ///
    /// `endpoints` is the matching `v1.Endpoints` object (selector-based
    /// services). Selectorless services with manually-created
    /// EndpointSlices use [`from_service_and_endpointslices`] instead.
    pub fn from_service_and_endpoints(
        service: &serde_json::Value,
        endpoints: Option<&serde_json::Value>,
    ) -> Option<Self> {
        let spec = service.get("spec")?;
        let cluster_ip = parse_routable_cluster_ip(spec)?;

        // Service ports — ClusterIP-side declarations.
        let svc_ports = spec.get("ports")?.as_array()?;

        // Walk Endpoints subsets to discover (target_port, ready_ips).
        let subsets = endpoints
            .and_then(|e| e.get("subsets"))
            .and_then(|s| s.as_array());
        let mut ports: Vec<PortSpec> = Vec::new();

        if let Some(subsets) = subsets {
            for subset in subsets {
                let addrs = subset
                    .get("addresses")
                    .and_then(|a| a.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|a| a.get("ip").and_then(|i| i.as_str()))
                            .filter_map(|s| s.parse::<Ipv4Addr>().ok())
                            .filter(|ip| !ip.is_unspecified())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                if addrs.is_empty() {
                    continue;
                }
                let subset_ports = match subset.get("ports").and_then(|p| p.as_array()) {
                    Some(p) => p,
                    None => continue,
                };
                for ep_port in subset_ports {
                    if let Some(p) = port_spec_from_subset(ep_port, svc_ports, &addrs) {
                        ports.push(p);
                    }
                }
            }
        }

        if ports.is_empty() {
            return None;
        }
        let session_affinity = parse_session_affinity(spec);
        Some(ServiceSpec {
            cluster_ip,
            ports,
            session_affinity,
        })
    }

    /// Same as [`from_service_and_endpoints`] but for selectorless
    /// services where the endpoints come from one or more
    /// `discovery.k8s.io/v1.EndpointSlice` objects.
    pub fn from_service_and_endpointslices(
        service: &serde_json::Value,
        slices: &[&serde_json::Value],
    ) -> Option<Self> {
        let spec = service.get("spec")?;
        let cluster_ip = parse_routable_cluster_ip(spec)?;
        let svc_ports = spec.get("ports")?.as_array()?;

        let mut ports: Vec<PortSpec> = Vec::new();
        for slice in slices {
            let slice_ports = match slice.get("ports").and_then(|p| p.as_array()) {
                Some(p) => p,
                None => continue,
            };
            let endpoints = match slice.get("endpoints").and_then(|e| e.as_array()) {
                Some(e) => e,
                None => continue,
            };

            let ready_ips: Vec<Ipv4Addr> = endpoints
                .iter()
                .filter(|ep| {
                    ep.pointer("/conditions/ready")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(true)
                })
                .filter_map(|ep| {
                    ep.get("addresses")
                        .and_then(|a| a.as_array())
                        .and_then(|a| a.first())
                        .and_then(|v| v.as_str())
                })
                .filter_map(|s| s.parse::<Ipv4Addr>().ok())
                .filter(|ip| !ip.is_unspecified())
                .collect();
            if ready_ips.is_empty() {
                continue;
            }

            for slice_port in slice_ports {
                if let Some(p) = port_spec_from_endpointslice(slice_port, svc_ports, &ready_ips) {
                    ports.push(p);
                }
            }
        }

        if ports.is_empty() {
            return None;
        }
        let session_affinity = parse_session_affinity(spec);
        Some(ServiceSpec {
            cluster_ip,
            ports,
            session_affinity,
        })
    }
}

/// Parse `spec.sessionAffinity` from a Service JSON spec.
pub(super) fn parse_session_affinity(spec: &serde_json::Value) -> SessionAffinity {
    match spec.get("sessionAffinity").and_then(|v| v.as_str()) {
        Some("ClientIP") => SessionAffinity::ClientIp,
        _ => SessionAffinity::None,
    }
}

/// Strict-parse a JSON number as an L4 port. Returns `None` for missing,
/// non-numeric, zero (reserved), or out-of-range values (>65535).
///
/// Replaces the `value.as_u64()? as u16` truncation pattern, which would
/// silently produce wrong rules for K8s objects with malformed port
/// fields. K8s spec says ports are 1-65535; anything else is invalid
/// data and should not become a DNAT rule.
pub(super) fn parse_port(v: Option<&serde_json::Value>) -> Option<u16> {
    let n = v?.as_u64()?;
    if n == 0 {
        return None;
    }
    u16::try_from(n).ok()
}

/// Returns the ClusterIP only if it's something we should DNAT for.
/// None for headless services, ExternalName, missing/empty IPs.
fn parse_routable_cluster_ip(spec: &serde_json::Value) -> Option<Ipv4Addr> {
    let svc_type = spec
        .get("type")
        .and_then(|t| t.as_str())
        .unwrap_or("ClusterIP");
    if svc_type == "ExternalName" {
        return None;
    }
    let cluster_ip = spec.get("clusterIP")?.as_str()?;
    if cluster_ip.is_empty() || cluster_ip == "None" {
        return None;
    }
    cluster_ip.parse::<Ipv4Addr>().ok()
}

/// Build one `PortSpec` from a v1.Endpoints subset port + the parent
/// service's port list. Returns None if the protocol can't be parsed,
/// or if there's no matching service port (defensive — orphan
/// endpoint ports shouldn't produce DNAT rules).
fn port_spec_from_subset(
    ep_port: &serde_json::Value,
    svc_ports: &[serde_json::Value],
    addrs: &[Ipv4Addr],
) -> Option<PortSpec> {
    let target_port = parse_port(ep_port.get("port"))?;
    let protocol = Protocol::parse(ep_port.get("protocol").and_then(|p| p.as_str()))?;
    let ep_name = ep_port.get("name").and_then(|n| n.as_str());

    // Match by endpoint port name first (for named targetPort/service
    // port mappings), then by numeric targetPort fallback.
    let svc_port_obj = svc_ports.iter().find(|sp| {
        if Protocol::parse(sp.get("protocol").and_then(|p| p.as_str())) != Some(protocol) {
            return false;
        }

        if let Some(ep_name) = ep_name {
            if sp.get("name").and_then(|n| n.as_str()) == Some(ep_name) {
                return true;
            }
            if sp.get("targetPort").and_then(|tp| tp.as_str()) == Some(ep_name) {
                return true;
            }
        }

        service_target_port_number(sp) == Some(target_port)
    })?;
    let service_port = parse_port(svc_port_obj.get("port"))?;
    let node_port = parse_port(svc_port_obj.get("nodePort"));

    Some(PortSpec {
        service_port,
        target_port,
        node_port,
        protocol,
        endpoints: addrs.to_vec(),
    })
}

/// Build one `PortSpec` from an EndpointSlice port + the parent service's
/// port list. Matches by name first, then by target-port number.
fn port_spec_from_endpointslice(
    slice_port: &serde_json::Value,
    svc_ports: &[serde_json::Value],
    ready_ips: &[Ipv4Addr],
) -> Option<PortSpec> {
    let target_port = parse_port(slice_port.get("port"))?;
    let protocol = Protocol::parse(slice_port.get("protocol").and_then(|p| p.as_str()))?;
    let slice_name = slice_port.get("name").and_then(|n| n.as_str());

    let svc_port_obj = svc_ports.iter().find(|sp| {
        if Protocol::parse(sp.get("protocol").and_then(|p| p.as_str())) != Some(protocol) {
            return false;
        }

        if let (Some(slice_n), Some(svc_n)) = (slice_name, sp.get("name").and_then(|n| n.as_str()))
            && slice_n == svc_n
        {
            return true;
        }
        if let Some(slice_n) = slice_name
            && sp.get("targetPort").and_then(|tp| tp.as_str()) == Some(slice_n)
        {
            return true;
        }
        service_target_port_number(sp) == Some(target_port)
    })?;
    let service_port = parse_port(svc_port_obj.get("port"))?;
    let node_port = parse_port(svc_port_obj.get("nodePort"));

    Some(PortSpec {
        service_port,
        target_port,
        node_port,
        protocol,
        endpoints: ready_ips.to_vec(),
    })
}

/// Resolve the effective numeric target port for one Service port item.
/// Handles both IntOrString forms:
/// - integer `targetPort` (or fallback to `port` when omitted)
/// - string `targetPort` when it is numeric text ("8443")
fn service_target_port_number(service_port: &serde_json::Value) -> Option<u16> {
    if let Some(target) = parse_port(service_port.get("targetPort")) {
        return Some(target);
    }
    if let Some(target) = service_port
        .get("targetPort")
        .and_then(|tp| tp.as_str())
        .and_then(|s| s.parse::<u16>().ok())
    {
        return Some(target);
    }
    parse_port(service_port.get("port"))
}

/// Threshold for the i-th step in an N-endpoint probability ladder.
/// Step `k` (where `k = endpoints_remaining`) gets probability `1/k`,
/// so the threshold against a uniform u32 is `UINT32_MAX / k`.
///
/// Each endpoint sees roughly 1/N of new connections (same statistical
/// distribution kube-proxy achieves with iptables `-m statistic --mode
/// random --probability X`), expressed in nft's `meta random < threshold`
/// form.
pub(super) fn probability_for_ladder_step(endpoints_remaining: usize) -> u32 {
    debug_assert!(
        endpoints_remaining >= 2,
        "ladder is only used for >= 2 remaining"
    );
    (u32::MAX / endpoints_remaining as u32).to_be()
}

// ---- CIDR helpers -------------------------------------------------------

pub(super) fn prefix_len_from_mask(mask: Ipv4Addr) -> u8 {
    u32::from(mask).count_ones() as u8
}
