//! Controlplane endpoint discovery from Node watch events.
//!
//! Workers subscribe to the same Node watch broadcast channel used by
//! `WorkerStoreAdapter` and extract controlplane endpoints (IP + gRPC port)
//! from Node objects. Discovered endpoints are fed into the gRPC client's
//! `set_all_leader_endpoints` so the reconnect loop can reach any
//! controlplane when the initially specified leader is down.

use crate::controllers::annotations::GRPC_PORT_ANNOTATION;
use crate::watch::events::{EventType, WatchEvent};

/// Result of inspecting a single watch event for controlplane discovery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlplaneDiscoveryEvent {
    /// Controlplane node appeared or changed: carries the node name and
    /// the fully-formed gRPC endpoint URL (`https://<ip>:<port>`).
    Upsert {
        node_name: String,
        endpoint: String,
        is_leader: bool,
    },
    /// A previously discovered controlplane node was deleted.
    Remove { node_name: String },
    /// Not a controlplane Node event — skip.
    Ignore,
}

/// Inspect a watch event and extract a controlplane endpoint if applicable.
///
/// A Node is identified as controlplane if it carries either the
/// `node-role.kubernetes.io/controlplane` or
/// `node-role.kubernetes.io/leader` label.
///
/// The gRPC port is read from the `klights.io/grpc-port` annotation
/// (defaulting to 7679 when absent). The node IP is extracted from
/// `status.addresses`, using `ExternalIP` only. `InternalIP` is the
/// Kubernetes node-local identity address and is not necessarily reachable
/// as an API/Raft transport endpoint from other nodes.
pub fn extract_controlplane_endpoint(event: &WatchEvent) -> ControlplaneDiscoveryEvent {
    // Only process Node events with object data.
    let obj = match event.event_type {
        EventType::Added | EventType::Modified => event.object.as_ref(),
        EventType::Deleted => {
            // For deletions the object may be minimal — just extract the
            // node name and signal removal if it had a controlplane label.
            let obj = event.object.as_ref();
            let name = match node_name(obj) {
                Some(n) => n,
                None => return ControlplaneDiscoveryEvent::Ignore,
            };
            if is_controlplane_node(obj) {
                return ControlplaneDiscoveryEvent::Remove {
                    node_name: name.to_string(),
                };
            }
            return ControlplaneDiscoveryEvent::Ignore;
        }
        EventType::Bookmark | EventType::Error => return ControlplaneDiscoveryEvent::Ignore,
    };

    if !is_controlplane_node(obj) {
        return ControlplaneDiscoveryEvent::Ignore;
    }

    let name = match node_name(obj) {
        Some(n) => n.to_string(),
        None => return ControlplaneDiscoveryEvent::Ignore,
    };

    let ip = match node_external_ip(obj) {
        Some(ip) => ip.to_string(),
        None => return ControlplaneDiscoveryEvent::Ignore,
    };

    let port = grpc_port_from_annotations(obj).unwrap_or(DEFAULT_GRPC_PORT);

    ControlplaneDiscoveryEvent::Upsert {
        node_name: name,
        endpoint: format!("https://{ip}:{port}"),
        is_leader: is_leader_node(obj),
    }
}

/// Default gRPC port when the annotation is absent.
const DEFAULT_GRPC_PORT: u16 = 7679;

fn is_controlplane_node(obj: &serde_json::Value) -> bool {
    let Some(labels) = obj.pointer("/metadata/labels").and_then(|v| v.as_object()) else {
        return false;
    };
    labels.contains_key("node-role.kubernetes.io/controlplane")
        || labels.contains_key("node-role.kubernetes.io/leader")
}

fn is_leader_node(obj: &serde_json::Value) -> bool {
    obj.pointer("/metadata/labels")
        .and_then(|v| v.as_object())
        .map(|labels| labels.contains_key("node-role.kubernetes.io/leader"))
        .unwrap_or(false)
}

fn node_name(obj: &serde_json::Value) -> Option<&str> {
    obj.pointer("/metadata/name").and_then(|v| v.as_str())
}

fn node_external_ip(obj: &serde_json::Value) -> Option<&str> {
    node_address(obj, "ExternalIP")
}

fn node_address<'a>(obj: &'a serde_json::Value, addr_type: &str) -> Option<&'a str> {
    obj.pointer("/status/addresses")
        .and_then(|v| v.as_array())
        .and_then(|addrs| {
            addrs.iter().find_map(|addr| {
                if addr.get("type").and_then(|v| v.as_str()) == Some(addr_type) {
                    addr.get("address").and_then(|a| a.as_str())
                } else {
                    None
                }
            })
        })
}

fn grpc_port_from_annotations(obj: &serde_json::Value) -> Option<u16> {
    obj.pointer("/metadata/annotations")
        .and_then(|v| v.get(GRPC_PORT_ANNOTATION))
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<u16>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Arc;

    fn node_event(event_type: EventType, node: serde_json::Value) -> WatchEvent {
        WatchEvent {
            event_type,
            object: Arc::new(node),
            encoded_payload: None,
        }
    }

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
                "labels": {
                    "node-role.kubernetes.io/controlplane": "",
                },
                "annotations": annotations,
            },
            "status": {
                "addresses": [
                    {"type": "Hostname", "address": name},
                    {"type": "ExternalIP", "address": ip},
                ],
            },
        })
    }

    fn worker_node(name: &str, ip: &str) -> serde_json::Value {
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {
                "name": name,
                "labels": {
                    "node-role.kubernetes.io/worker": "",
                },
            },
            "status": {
                "addresses": [
                    {"type": "ExternalIP", "address": ip},
                ],
            },
        })
    }

    fn leader_node(name: &str, ip: &str) -> serde_json::Value {
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {
                "name": name,
                "labels": {
                    "node-role.kubernetes.io/leader": "",
                },
            },
            "status": {
                "addresses": [
                    {"type": "ExternalIP", "address": ip},
                ],
            },
        })
    }

    #[test]
    fn extract_controlplane_endpoint_added_with_explicit_port() {
        let node = controlplane_node("cp1", "10.0.0.1", Some(7679));
        let event = node_event(EventType::Added, node);
        assert_eq!(
            extract_controlplane_endpoint(&event),
            ControlplaneDiscoveryEvent::Upsert {
                node_name: "cp1".to_string(),
                endpoint: "https://10.0.0.1:7679".to_string(),
                is_leader: false,
            }
        );
    }

    #[test]
    fn extract_controlplane_endpoint_added_default_port() {
        let node = controlplane_node("cp2", "10.0.0.2", None);
        let event = node_event(EventType::Added, node);
        assert_eq!(
            extract_controlplane_endpoint(&event),
            ControlplaneDiscoveryEvent::Upsert {
                node_name: "cp2".to_string(),
                endpoint: "https://10.0.0.2:7679".to_string(),
                is_leader: false,
            }
        );
    }

    #[test]
    fn extract_controlplane_endpoint_uses_external_ip() {
        let node = json!({
            "metadata": {
                "name": "cp3",
                "labels": {"node-role.kubernetes.io/controlplane": ""},
            },
            "status": {
                "addresses": [
                    {"type": "ExternalIP", "address": "192.0.2.4"},
                ],
            },
        });
        let event = node_event(EventType::Added, node);
        assert_eq!(
            extract_controlplane_endpoint(&event),
            ControlplaneDiscoveryEvent::Upsert {
                node_name: "cp3".to_string(),
                endpoint: "https://192.0.2.4:7679".to_string(),
                is_leader: false,
            }
        );
    }

    #[test]
    fn extract_controlplane_endpoint_prefers_external_ip_for_api_reconnect() {
        let node = json!({
            "metadata": {
                "name": "cp4",
                "labels": {"node-role.kubernetes.io/controlplane": ""},
            },
            "status": {
                "addresses": [
                    {"type": "ExternalIP", "address": "192.0.2.4"},
                    {"type": "InternalIP", "address": "10.0.0.4"},
                ],
            },
        });
        let event = node_event(EventType::Added, node);
        assert_eq!(
            extract_controlplane_endpoint(&event),
            ControlplaneDiscoveryEvent::Upsert {
                node_name: "cp4".to_string(),
                endpoint: "https://192.0.2.4:7679".to_string(),
                is_leader: false,
            }
        );
    }

    #[test]
    fn extract_controlplane_endpoint_ignores_internal_ip_without_external_ip() {
        let node = json!({
            "metadata": {
                "name": "cp-internal-only",
                "labels": {"node-role.kubernetes.io/controlplane": ""},
            },
            "status": {
                "addresses": [
                    {"type": "InternalIP", "address": "10.0.0.4"},
                ],
            },
        });
        let event = node_event(EventType::Added, node);
        assert_eq!(
            extract_controlplane_endpoint(&event),
            ControlplaneDiscoveryEvent::Ignore
        );
    }

    #[test]
    fn extract_controlplane_endpoint_leader_label() {
        let node = leader_node("seed", "10.0.0.10");
        let event = node_event(EventType::Added, node);
        assert_eq!(
            extract_controlplane_endpoint(&event),
            ControlplaneDiscoveryEvent::Upsert {
                node_name: "seed".to_string(),
                endpoint: "https://10.0.0.10:7679".to_string(),
                is_leader: true,
            }
        );
    }

    #[test]
    fn extract_controlplane_endpoint_modified() {
        let node = controlplane_node("cp1", "10.0.0.1", Some(8888));
        let event = node_event(EventType::Modified, node);
        assert_eq!(
            extract_controlplane_endpoint(&event),
            ControlplaneDiscoveryEvent::Upsert {
                node_name: "cp1".to_string(),
                endpoint: "https://10.0.0.1:8888".to_string(),
                is_leader: false,
            }
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
        let event = node_event(EventType::Deleted, node);
        assert_eq!(
            extract_controlplane_endpoint(&event),
            ControlplaneDiscoveryEvent::Remove {
                node_name: "cp1".to_string(),
            }
        );
    }

    #[test]
    fn extract_controlplane_endpoint_worker_ignored() {
        let node = worker_node("w1", "10.0.1.1");
        let event = node_event(EventType::Added, node);
        assert_eq!(
            extract_controlplane_endpoint(&event),
            ControlplaneDiscoveryEvent::Ignore
        );
    }

    #[test]
    fn extract_controlplane_endpoint_bookmark_ignored() {
        let event = WatchEvent {
            event_type: EventType::Bookmark,
            object: Arc::new(json!({})),
            encoded_payload: None,
        };
        assert_eq!(
            extract_controlplane_endpoint(&event),
            ControlplaneDiscoveryEvent::Ignore
        );
    }

    #[test]
    fn extract_controlplane_endpoint_no_ip_ignored() {
        let node = json!({
            "metadata": {
                "name": "cp-no-ip",
                "labels": {"node-role.kubernetes.io/controlplane": ""},
            },
            "status": {
                "addresses": [
                    {"type": "Hostname", "address": "cp-no-ip"},
                ],
            },
        });
        let event = node_event(EventType::Added, node);
        assert_eq!(
            extract_controlplane_endpoint(&event),
            ControlplaneDiscoveryEvent::Ignore
        );
    }

    #[test]
    fn extract_controlplane_endpoint_no_labels_ignored() {
        let node = json!({
            "metadata": {"name": "bare"},
            "status": {
                "addresses": [{"type": "InternalIP", "address": "10.0.0.99"}],
            },
        });
        let event = node_event(EventType::Added, node);
        assert_eq!(
            extract_controlplane_endpoint(&event),
            ControlplaneDiscoveryEvent::Ignore
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
            "status": {
                "addresses": [{"type": "ExternalIP", "address": "10.0.0.50"}],
            },
        });
        let event = node_event(EventType::Added, node);
        assert_eq!(
            extract_controlplane_endpoint(&event),
            ControlplaneDiscoveryEvent::Upsert {
                node_name: "cp-custom".to_string(),
                endpoint: "https://10.0.0.50:9999".to_string(),
                is_leader: false,
            }
        );
    }
}
