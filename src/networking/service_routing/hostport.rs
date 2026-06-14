use super::prelude::*;
use super::service_rules::Protocol;

// ============ HostPortSpec ===============================================
// Per-pod hostport mapping. Typed data extracted from a Pod's container
// ports so the nft rule builder can consume it directly.

/// One hostPort declared on a container port.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostPortSpec {
    /// `containerPort.hostIP` — `None` means "any host destination IP"
    /// (i.e. omit the `ip daddr` match in the rule).
    pub host_ip: Option<Ipv4Addr>,
    /// `containerPort.hostPort` — the port the host listens on.
    pub host_port: u16,
    /// `containerPort.containerPort` — the pod-side port the rule
    /// DNATs to.
    pub container_port: u16,
    pub protocol: Protocol,
}

impl HostPortSpec {
    /// Walk a Pod's `spec.containers[].ports[]` and return every port
    /// with a non-zero `hostPort` declaration. hostPort=0 (or missing)
    /// is skipped; protocol defaults to TCP; `0.0.0.0`/empty hostIP is
    /// treated as "any IP".
    pub fn from_pod(pod: &serde_json::Value) -> Vec<HostPortSpec> {
        let mut specs = Vec::new();
        let containers = match pod.pointer("/spec/containers").and_then(|c| c.as_array()) {
            Some(c) => c,
            None => return specs,
        };
        for container in containers {
            let ports = match container.get("ports").and_then(|p| p.as_array()) {
                Some(p) => p,
                None => continue,
            };
            for port_obj in ports {
                // Strict parse: parse_port rejects 0, negatives, and
                // out-of-range values rather than silently truncating
                // (the previous `as u16` cast would have produced
                // incorrect rules for malformed input).
                let host_port = match super::service_rules::parse_port(port_obj.get("hostPort")) {
                    Some(p) => p,
                    None => continue,
                };
                let container_port =
                    match super::service_rules::parse_port(port_obj.get("containerPort")) {
                        Some(p) => p,
                        None => continue,
                    };
                let protocol =
                    match Protocol::parse(port_obj.get("protocol").and_then(|p| p.as_str())) {
                        Some(p) => p,
                        None => continue,
                    };
                let host_ip = port_obj
                    .get("hostIP")
                    .and_then(|ip| ip.as_str())
                    .filter(|s| !s.is_empty() && *s != "0.0.0.0")
                    .and_then(|s| s.parse::<Ipv4Addr>().ok());
                specs.push(HostPortSpec {
                    host_ip,
                    host_port,
                    container_port,
                    protocol,
                });
            }
        }
        specs
    }
}
