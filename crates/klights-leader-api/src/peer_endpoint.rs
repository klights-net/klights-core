//! Transport-neutral peer endpoint projection from Kubernetes Node objects.

/// A peer Node and its TLS endpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerEndpoint {
    node_name: String,
    endpoint: String,
}

impl PeerEndpoint {
    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

/// Project a remote Node's ExternalIP into a TLS endpoint.
pub fn peer_endpoint_from_node(
    node: &serde_json::Value,
    local_node_name: &str,
    grpc_port_annotation: &str,
    default_port: u16,
) -> Option<PeerEndpoint> {
    if node.get("kind").and_then(|value| value.as_str()) != Some("Node") {
        return None;
    }
    let node_name = node
        .pointer("/metadata/name")
        .and_then(|value| value.as_str())?;
    if node_name == local_node_name {
        return None;
    }
    let external_ip = node_external_ip(node)?;
    let external_ip = external_ip.parse::<std::net::IpAddr>().ok()?;
    let port = node
        .pointer("/metadata/annotations")
        .and_then(|value| value.get(grpc_port_annotation))
        .and_then(|value| value.as_str())
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(default_port);
    Some(PeerEndpoint {
        node_name: node_name.to_string(),
        endpoint: format!("https://{}:{port}", uri_host_for_ip(external_ip)),
    })
}

/// Return a Node's first ExternalIP status address.
pub fn node_external_ip(node: &serde_json::Value) -> Option<&str> {
    node.pointer("/status/addresses")
        .and_then(|value| value.as_array())
        .and_then(|addresses| {
            addresses.iter().find_map(|address| {
                if address.get("type").and_then(|value| value.as_str()) == Some("ExternalIP") {
                    address.get("address").and_then(|value| value.as_str())
                } else {
                    None
                }
            })
        })
}

fn uri_host_for_ip(ip: std::net::IpAddr) -> String {
    match ip {
        std::net::IpAddr::V4(ip) => ip.to_string(),
        std::net::IpAddr::V6(ip) => format!("[{ip}]"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const GRPC_PORT_ANNOTATION: &str = "example.test/grpc-port";

    #[test]
    fn peer_endpoint_from_node_uses_external_ip_only() {
        let node = json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a"},
            "status": {
                "addresses": [
                    {"type": "InternalIP", "address": "172.31.11.2"},
                    {"type": "ExternalIP", "address": "10.99.0.11"}
                ]
            }
        });

        assert_eq!(
            peer_endpoint_from_node(&node, "leader-a", GRPC_PORT_ANNOTATION, 7679),
            Some(PeerEndpoint {
                node_name: "worker-a".to_string(),
                endpoint: "https://10.99.0.11:7679".to_string(),
            })
        );
    }

    #[test]
    fn peer_endpoint_from_node_ignores_internal_ip_only_peer() {
        let node = json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a"},
            "status": {
                "addresses": [
                    {"type": "InternalIP", "address": "172.31.11.2"}
                ]
            }
        });

        assert_eq!(
            peer_endpoint_from_node(&node, "leader-a", GRPC_PORT_ANNOTATION, 7679),
            None
        );
    }

    #[test]
    fn peer_endpoint_from_node_ignores_local_node() {
        let node = json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "leader-a"},
            "status": {
                "addresses": [
                    {"type": "ExternalIP", "address": "10.99.0.10"}
                ]
            }
        });

        assert_eq!(
            peer_endpoint_from_node(&node, "leader-a", GRPC_PORT_ANNOTATION, 7679),
            None
        );
    }
}
