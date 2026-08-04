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

/// Result of projecting one Node watch transition for control-plane discovery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlplaneDiscoveryEvent {
    /// A control-plane Node appeared or changed.
    Upsert {
        node_name: String,
        endpoint: String,
        is_leader: bool,
    },
    /// A previously discovered control-plane Node was deleted.
    Remove { node_name: String },
    /// The transition does not describe a discoverable control-plane endpoint.
    Ignore,
}

/// Project a Kubernetes Node watch transition into a control-plane endpoint.
pub fn extract_controlplane_endpoint(
    event_type: crate::WatchEventType,
    node: &serde_json::Value,
    grpc_port_annotation: &str,
    default_port: u16,
) -> ControlplaneDiscoveryEvent {
    match event_type {
        crate::WatchEventType::Deleted => {
            let Some(node_name) = node_name(node) else {
                return ControlplaneDiscoveryEvent::Ignore;
            };
            if is_controlplane_node(node) {
                ControlplaneDiscoveryEvent::Remove {
                    node_name: node_name.to_string(),
                }
            } else {
                ControlplaneDiscoveryEvent::Ignore
            }
        }
        crate::WatchEventType::Bookmark | crate::WatchEventType::Error => {
            ControlplaneDiscoveryEvent::Ignore
        }
        crate::WatchEventType::Added | crate::WatchEventType::Modified => {
            if !is_controlplane_node(node) {
                return ControlplaneDiscoveryEvent::Ignore;
            }
            let Some(node_name) = node_name(node) else {
                return ControlplaneDiscoveryEvent::Ignore;
            };
            let Some(ip) = node_external_ip(node) else {
                return ControlplaneDiscoveryEvent::Ignore;
            };
            let port = node
                .pointer("/metadata/annotations")
                .and_then(|value| value.get(grpc_port_annotation))
                .and_then(|value| value.as_str())
                .and_then(|value| value.parse::<u16>().ok())
                .unwrap_or(default_port);
            ControlplaneDiscoveryEvent::Upsert {
                node_name: node_name.to_string(),
                endpoint: format!("https://{ip}:{port}"),
                is_leader: is_leader_node(node),
            }
        }
    }
}

fn is_controlplane_node(node: &serde_json::Value) -> bool {
    let Some(labels) = node
        .pointer("/metadata/labels")
        .and_then(|value| value.as_object())
    else {
        return false;
    };
    labels.contains_key("node-role.kubernetes.io/controlplane")
        || labels.contains_key("node-role.kubernetes.io/leader")
}

fn is_leader_node(node: &serde_json::Value) -> bool {
    node.pointer("/metadata/labels")
        .and_then(|value| value.as_object())
        .is_some_and(|labels| labels.contains_key("node-role.kubernetes.io/leader"))
}

fn node_name(node: &serde_json::Value) -> Option<&str> {
    node.pointer("/metadata/name")
        .and_then(|value| value.as_str())
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
    const DEFAULT_PORT: u16 = 7679;

    fn controlplane_node(name: &str, ip: &str, grpc_port: Option<u16>) -> serde_json::Value {
        let mut annotations = serde_json::Map::new();
        if let Some(port) = grpc_port {
            annotations.insert(GRPC_PORT_ANNOTATION.to_string(), json!(port.to_string()));
        }
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {
                "name": name,
                "labels": {"node-role.kubernetes.io/controlplane": ""},
                "annotations": annotations,
            },
            "status": {"addresses": [
                {"type": "Hostname", "address": name},
                {"type": "ExternalIP", "address": ip},
            ]},
        })
    }

    fn assert_discovery(
        event_type: crate::WatchEventType,
        node: &serde_json::Value,
        expected: ControlplaneDiscoveryEvent,
    ) {
        assert_eq!(
            extract_controlplane_endpoint(event_type, node, GRPC_PORT_ANNOTATION, DEFAULT_PORT,),
            expected
        );
    }

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

    #[test]
    fn extract_controlplane_endpoint_added_with_explicit_port() {
        let node = controlplane_node("cp1", "10.0.0.1", Some(7679));
        assert_discovery(
            crate::WatchEventType::Added,
            &node,
            ControlplaneDiscoveryEvent::Upsert {
                node_name: "cp1".to_string(),
                endpoint: "https://10.0.0.1:7679".to_string(),
                is_leader: false,
            },
        );
    }

    #[test]
    fn extract_controlplane_endpoint_added_default_port() {
        let node = controlplane_node("cp2", "10.0.0.2", None);
        assert_discovery(
            crate::WatchEventType::Added,
            &node,
            ControlplaneDiscoveryEvent::Upsert {
                node_name: "cp2".to_string(),
                endpoint: "https://10.0.0.2:7679".to_string(),
                is_leader: false,
            },
        );
    }

    #[test]
    fn extract_controlplane_endpoint_uses_external_ip() {
        let node = json!({
            "metadata": {
                "name": "cp3",
                "labels": {"node-role.kubernetes.io/controlplane": ""},
            },
            "status": {"addresses": [
                {"type": "ExternalIP", "address": "192.0.2.4"},
            ]},
        });
        assert_discovery(
            crate::WatchEventType::Added,
            &node,
            ControlplaneDiscoveryEvent::Upsert {
                node_name: "cp3".to_string(),
                endpoint: "https://192.0.2.4:7679".to_string(),
                is_leader: false,
            },
        );
    }

    #[test]
    fn extract_controlplane_endpoint_prefers_external_ip_for_api_reconnect() {
        let node = json!({
            "metadata": {
                "name": "cp4",
                "labels": {"node-role.kubernetes.io/controlplane": ""},
            },
            "status": {"addresses": [
                {"type": "ExternalIP", "address": "192.0.2.4"},
                {"type": "InternalIP", "address": "10.0.0.4"},
            ]},
        });
        assert_discovery(
            crate::WatchEventType::Added,
            &node,
            ControlplaneDiscoveryEvent::Upsert {
                node_name: "cp4".to_string(),
                endpoint: "https://192.0.2.4:7679".to_string(),
                is_leader: false,
            },
        );
    }

    #[test]
    fn extract_controlplane_endpoint_ignores_internal_ip_without_external_ip() {
        let node = json!({
            "metadata": {
                "name": "cp-internal-only",
                "labels": {"node-role.kubernetes.io/controlplane": ""},
            },
            "status": {"addresses": [
                {"type": "InternalIP", "address": "10.0.0.4"},
            ]},
        });
        assert_discovery(
            crate::WatchEventType::Added,
            &node,
            ControlplaneDiscoveryEvent::Ignore,
        );
    }

    #[test]
    fn extract_controlplane_endpoint_leader_label() {
        let node = json!({
            "metadata": {
                "name": "seed",
                "labels": {"node-role.kubernetes.io/leader": ""},
            },
            "status": {"addresses": [
                {"type": "ExternalIP", "address": "10.0.0.10"},
            ]},
        });
        assert_discovery(
            crate::WatchEventType::Added,
            &node,
            ControlplaneDiscoveryEvent::Upsert {
                node_name: "seed".to_string(),
                endpoint: "https://10.0.0.10:7679".to_string(),
                is_leader: true,
            },
        );
    }

    #[test]
    fn extract_controlplane_endpoint_modified() {
        let node = controlplane_node("cp1", "10.0.0.1", Some(8888));
        assert_discovery(
            crate::WatchEventType::Modified,
            &node,
            ControlplaneDiscoveryEvent::Upsert {
                node_name: "cp1".to_string(),
                endpoint: "https://10.0.0.1:8888".to_string(),
                is_leader: false,
            },
        );
    }

    #[test]
    fn extract_controlplane_endpoint_deleted() {
        let node = json!({
            "metadata": {
                "name": "cp1",
                "labels": {"node-role.kubernetes.io/controlplane": ""},
            },
        });
        assert_discovery(
            crate::WatchEventType::Deleted,
            &node,
            ControlplaneDiscoveryEvent::Remove {
                node_name: "cp1".to_string(),
            },
        );
    }

    #[test]
    fn extract_controlplane_endpoint_worker_ignored() {
        let node = json!({
            "metadata": {
                "name": "w1",
                "labels": {"node-role.kubernetes.io/worker": ""},
            },
            "status": {"addresses": [
                {"type": "ExternalIP", "address": "10.0.1.1"},
            ]},
        });
        assert_discovery(
            crate::WatchEventType::Added,
            &node,
            ControlplaneDiscoveryEvent::Ignore,
        );
    }

    #[test]
    fn extract_controlplane_endpoint_bookmark_ignored() {
        assert_discovery(
            crate::WatchEventType::Bookmark,
            &json!({}),
            ControlplaneDiscoveryEvent::Ignore,
        );
    }

    #[test]
    fn extract_controlplane_endpoint_no_ip_ignored() {
        let node = json!({
            "metadata": {
                "name": "cp-no-ip",
                "labels": {"node-role.kubernetes.io/controlplane": ""},
            },
            "status": {"addresses": [
                {"type": "Hostname", "address": "cp-no-ip"},
            ]},
        });
        assert_discovery(
            crate::WatchEventType::Added,
            &node,
            ControlplaneDiscoveryEvent::Ignore,
        );
    }

    #[test]
    fn extract_controlplane_endpoint_no_labels_ignored() {
        let node = json!({
            "metadata": {"name": "bare"},
            "status": {"addresses": [
                {"type": "InternalIP", "address": "10.0.0.99"},
            ]},
        });
        assert_discovery(
            crate::WatchEventType::Added,
            &node,
            ControlplaneDiscoveryEvent::Ignore,
        );
    }

    #[test]
    fn extract_controlplane_endpoint_custom_port() {
        let node = json!({
            "metadata": {
                "name": "cp-custom",
                "labels": {"node-role.kubernetes.io/controlplane": ""},
                "annotations": {GRPC_PORT_ANNOTATION: "9999"},
            },
            "status": {"addresses": [
                {"type": "ExternalIP", "address": "10.0.0.50"},
            ]},
        });
        assert_discovery(
            crate::WatchEventType::Added,
            &node,
            ControlplaneDiscoveryEvent::Upsert {
                node_name: "cp-custom".to_string(),
                endpoint: "https://10.0.0.50:9999".to_string(),
                is_leader: false,
            },
        );
    }
}
