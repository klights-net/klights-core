use crate::kubelet::node_status_merge::set_node_external_ip;
use crate::utils::k8s_time_now;

/// The two network-related Node conditions (`Ready` + `NetworkUnavailable`)
/// derived from the local dataplane health. Shared by initial registration and
/// the event-driven readiness reconciler so both encode health identically.
pub(super) struct NodeNetworkConditions {
    pub(super) ready_status: &'static str,
    pub(super) ready_reason: &'static str,
    pub(super) ready_message: String,
    pub(super) net_unavail_status: &'static str,
    pub(super) net_unavail_reason: &'static str,
    pub(super) net_unavail_message: String,
}

impl NodeNetworkConditions {
    pub(super) fn from_health(
        dataplane_health: Option<&crate::networking::dataplane_health::DataplaneHealth>,
    ) -> Self {
        use crate::networking::dataplane_health::DataplaneHealthStatus;
        match dataplane_health.map(|health| health.status()) {
            None | Some(DataplaneHealthStatus::Healthy) => Self {
                ready_status: "True",
                ready_reason: "KubeletReady",
                ready_message: "klights is ready".to_string(),
                net_unavail_status: "False",
                net_unavail_reason: "RouteCreated",
                net_unavail_message: "RouteController created a route".to_string(),
            },
            Some(DataplaneHealthStatus::Unavailable { reason }) => Self {
                ready_status: "False",
                ready_reason: "NetworkUnavailable",
                ready_message: reason.clone(),
                net_unavail_status: "True",
                net_unavail_reason: "DataplaneNotReady",
                net_unavail_message: reason,
            },
        }
    }
}

/// Update one Node condition in place to match the desired status/reason/message.
/// Returns true if anything changed (so callers can skip a no-op write and keep
/// the node idle-silent). `lastTransitionTime` is refreshed only when the
/// `status` value itself flips, per the K8s condition contract.
fn set_node_condition(
    node: &mut serde_json::Value,
    cond_type: &str,
    status: &str,
    reason: &str,
    message: &str,
) -> bool {
    let Some(conditions) = node
        .pointer_mut("/status/conditions")
        .and_then(|value| value.as_array_mut())
    else {
        return false;
    };
    if let Some(existing) = conditions
        .iter_mut()
        .find(|cond| cond.get("type").and_then(|t| t.as_str()) == Some(cond_type))
    {
        let status_changed = existing.get("status").and_then(|v| v.as_str()) != Some(status);
        let reason_changed = existing.get("reason").and_then(|v| v.as_str()) != Some(reason);
        let message_changed = existing.get("message").and_then(|v| v.as_str()) != Some(message);
        if !status_changed && !reason_changed && !message_changed {
            return false;
        }
        existing["status"] = serde_json::json!(status);
        existing["reason"] = serde_json::json!(reason);
        existing["message"] = serde_json::json!(message);
        if status_changed {
            existing["lastTransitionTime"] = serde_json::json!(k8s_time_now());
        }
        true
    } else {
        conditions.push(serde_json::json!({
            "type": cond_type,
            "status": status,
            "reason": reason,
            "message": message,
            "lastTransitionTime": k8s_time_now(),
        }));
        true
    }
}

/// Apply the `Ready` + `NetworkUnavailable` conditions to a Node object in
/// place. Returns true if either condition actually changed.
pub(super) fn apply_network_conditions(
    node: &mut serde_json::Value,
    conditions: &NodeNetworkConditions,
) -> bool {
    let ready_changed = set_node_condition(
        node,
        "Ready",
        conditions.ready_status,
        conditions.ready_reason,
        &conditions.ready_message,
    );
    let net_changed = set_node_condition(
        node,
        "NetworkUnavailable",
        conditions.net_unavail_status,
        conditions.net_unavail_reason,
        &conditions.net_unavail_message,
    );
    ready_changed || net_changed
}

pub fn set_node_external_ip_from_dataplane_annotation(node: &mut serde_json::Value) -> bool {
    let endpoint = node
        .pointer("/metadata/annotations")
        .and_then(|value| value.as_object())
        .and_then(|annotations| {
            annotations
                .get(klights_network_api::DATAPLANE_ENDPOINT_ANNOTATION)
                .and_then(|value| value.as_str())
        })
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let Some(endpoint) = endpoint else {
        return false;
    };
    set_node_external_ip(node, &endpoint)
}

pub fn set_node_pod_cidr(node: &mut serde_json::Value, pod_cidr: &str) -> bool {
    let pod_cidr = pod_cidr.trim();
    if pod_cidr.is_empty() {
        return false;
    }
    let Some(node_object) = node.as_object_mut() else {
        return false;
    };
    let spec = node_object
        .entry("spec")
        .or_insert_with(|| serde_json::json!({}));
    if !spec.is_object() {
        *spec = serde_json::json!({});
    }
    let Some(spec_object) = spec.as_object_mut() else {
        return false;
    };

    let mut changed = set_json_string_field(spec_object, "podCIDR", pod_cidr);
    let desired = serde_json::json!([pod_cidr]);
    if spec_object.get("podCIDRs") != Some(&desired) {
        spec_object.insert("podCIDRs".to_string(), desired);
        changed = true;
    }
    changed
}

pub fn set_node_dataplane_annotations(
    node: &mut serde_json::Value,
    metadata: &crate::networking::wireguard::DataplanePeerMetadata,
) -> bool {
    use klights_network_api::{
        DATAPLANE_ENCRYPTION_ANNOTATION, DATAPLANE_ENDPOINT_ANNOTATION, DATAPLANE_MODE_ANNOTATION,
        DATAPLANE_PORT_ANNOTATION, DATAPLANE_PUBLIC_KEY_ANNOTATION,
    };

    let Some(node_object) = node.as_object_mut() else {
        return false;
    };
    let metadata_object = node_object
        .entry("metadata")
        .or_insert_with(|| serde_json::json!({}));
    if !metadata_object.is_object() {
        *metadata_object = serde_json::json!({});
    }
    let Some(metadata_object) = metadata_object.as_object_mut() else {
        return false;
    };
    let annotations = metadata_object
        .entry("annotations")
        .or_insert_with(|| serde_json::json!({}));
    if !annotations.is_object() {
        *annotations = serde_json::json!({});
    }
    let Some(annotations) = annotations.as_object_mut() else {
        return false;
    };

    let mut changed = false;
    changed |= set_json_string_field(
        annotations,
        DATAPLANE_ENDPOINT_ANNOTATION,
        &metadata.endpoint.to_string(),
    );
    changed |= set_json_string_field(
        annotations,
        DATAPLANE_MODE_ANNOTATION,
        metadata.mode.as_str(),
    );
    changed |= set_json_string_field(
        annotations,
        DATAPLANE_ENCRYPTION_ANNOTATION,
        metadata.encryption.as_str(),
    );
    if let Some(port) = metadata.port {
        changed |= set_json_string_field(annotations, DATAPLANE_PORT_ANNOTATION, &port.to_string());
    } else {
        changed |= annotations.remove(DATAPLANE_PORT_ANNOTATION).is_some();
    }
    if let Some(public_key) = metadata.public_key.as_ref() {
        changed |= set_json_string_field(
            annotations,
            DATAPLANE_PUBLIC_KEY_ANNOTATION,
            &public_key.to_string(),
        );
    } else {
        changed |= annotations
            .remove(DATAPLANE_PUBLIC_KEY_ANNOTATION)
            .is_some();
    }
    changed
}

fn set_json_string_field(
    object: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    value: &str,
) -> bool {
    if object.get(key).and_then(|existing| existing.as_str()) == Some(value) {
        return false;
    }
    object.insert(key.to_string(), serde_json::json!(value));
    true
}

#[cfg(test)]
pub(super) fn stamp_current_git_commit_annotation(node: &mut serde_json::Value) -> bool {
    use crate::controllers::annotations::GIT_COMMIT_ANNOTATION;

    let Some(node_object) = node.as_object_mut() else {
        return false;
    };
    let metadata = node_object
        .entry("metadata")
        .or_insert_with(|| serde_json::json!({}));
    if !metadata.is_object() {
        *metadata = serde_json::json!({});
    }
    let Some(metadata_object) = metadata.as_object_mut() else {
        return false;
    };
    let annotations = metadata_object
        .entry("annotations")
        .or_insert_with(|| serde_json::json!({}));
    if !annotations.is_object() {
        *annotations = serde_json::json!({});
    }
    let Some(annotations_object) = annotations.as_object_mut() else {
        return false;
    };
    let current = annotations_object
        .get(GIT_COMMIT_ANNOTATION)
        .and_then(|value| value.as_str());
    if current == Some(crate::version::GIT_COMMIT_SHORT) {
        return false;
    }
    annotations_object.insert(
        GIT_COMMIT_ANNOTATION.to_string(),
        serde_json::json!(crate::version::GIT_COMMIT_SHORT),
    );
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controllers::annotations::{
        DATAPLANE_ENCRYPTION_ANNOTATION, DATAPLANE_ENDPOINT_ANNOTATION, DATAPLANE_MODE_ANNOTATION,
        DATAPLANE_PORT_ANNOTATION, DATAPLANE_PUBLIC_KEY_ANNOTATION, GIT_COMMIT_ANNOTATION,
    };
    use crate::networking::wireguard::{
        DataplaneEncryption, DataplaneMode, DataplanePeerMetadata, WireGuardPublicKey,
    };
    use std::net::IpAddr;

    #[test]
    fn node_status_projection_sets_pod_cidr_pair_once() {
        let mut node = serde_json::json!({"apiVersion": "v1", "kind": "Node"});

        assert!(set_node_pod_cidr(&mut node, " 10.50.0.0/24 "));
        assert_eq!(
            node.pointer("/spec/podCIDR")
                .and_then(|value| value.as_str()),
            Some("10.50.0.0/24")
        );
        assert_eq!(
            node.pointer("/spec/podCIDRs/0")
                .and_then(|value| value.as_str()),
            Some("10.50.0.0/24")
        );
        assert!(!set_node_pod_cidr(&mut node, "10.50.0.0/24"));
    }

    #[test]
    fn node_status_projection_sets_dataplane_annotations_and_removes_absent_optional_fields() {
        let mut node = serde_json::json!({
            "metadata": {
                "annotations": {
                    DATAPLANE_PORT_ANNOTATION: "51820",
                    DATAPLANE_PUBLIC_KEY_ANNOTATION: "stale"
                }
            }
        });
        let metadata = DataplanePeerMetadata {
            node_name: "node-a".to_string(),
            endpoint: "192.0.2.15".parse::<IpAddr>().expect("endpoint"),
            port: None,
            public_key: None,
            mode: DataplaneMode::Root,
            encryption: DataplaneEncryption::Enabled,
        };

        assert!(set_node_dataplane_annotations(&mut node, &metadata));
        let annotations = node
            .pointer("/metadata/annotations")
            .and_then(|value| value.as_object())
            .expect("annotations");
        assert_eq!(
            annotations
                .get(DATAPLANE_ENDPOINT_ANNOTATION)
                .and_then(|value| value.as_str()),
            Some("192.0.2.15")
        );
        assert_eq!(
            annotations
                .get(DATAPLANE_MODE_ANNOTATION)
                .and_then(|value| value.as_str()),
            Some("root")
        );
        assert_eq!(
            annotations
                .get(DATAPLANE_ENCRYPTION_ANNOTATION)
                .and_then(|value| value.as_str()),
            Some("enabled")
        );
        assert!(!annotations.contains_key(DATAPLANE_PORT_ANNOTATION));
        assert!(!annotations.contains_key(DATAPLANE_PUBLIC_KEY_ANNOTATION));
        assert!(!set_node_dataplane_annotations(&mut node, &metadata));
    }

    #[test]
    fn node_status_projection_sets_dataplane_optional_annotation_fields() {
        let mut node = serde_json::json!({});
        let metadata = DataplanePeerMetadata {
            node_name: "node-a".to_string(),
            endpoint: "192.0.2.20".parse::<IpAddr>().expect("endpoint"),
            port: Some(51821),
            public_key: Some(
                WireGuardPublicKey::parse("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
                    .expect("public key"),
            ),
            mode: DataplaneMode::Rootless,
            encryption: DataplaneEncryption::Disabled,
        };

        assert!(set_node_dataplane_annotations(&mut node, &metadata));
        let annotations = node
            .pointer("/metadata/annotations")
            .and_then(|value| value.as_object())
            .expect("annotations");
        assert_eq!(
            annotations
                .get(DATAPLANE_PORT_ANNOTATION)
                .and_then(|value| value.as_str()),
            Some("51821")
        );
        assert_eq!(
            annotations
                .get(DATAPLANE_PUBLIC_KEY_ANNOTATION)
                .and_then(|value| value.as_str()),
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
        );
    }

    #[test]
    fn node_status_projection_sets_external_ip_from_dataplane_annotation() {
        let mut node = serde_json::json!({
            "metadata": {
                "annotations": {
                    DATAPLANE_ENDPOINT_ANNOTATION: " 203.0.113.10 "
                }
            },
            "status": {
                "addresses": [
                    {"type": "InternalIP", "address": "10.0.0.5"}
                ]
            }
        });

        assert!(set_node_external_ip_from_dataplane_annotation(&mut node));
        let addresses = node
            .pointer("/status/addresses")
            .and_then(|value| value.as_array())
            .expect("addresses");
        assert!(addresses.iter().any(|address| {
            address.get("type").and_then(|value| value.as_str()) == Some("ExternalIP")
                && address.get("address").and_then(|value| value.as_str()) == Some("203.0.113.10")
        }));
        assert!(!set_node_external_ip_from_dataplane_annotation(&mut node));
    }

    #[test]
    fn node_status_projection_updates_git_commit_annotation_once() {
        let mut node = serde_json::json!({
            "metadata": {
                "annotations": {
                    GIT_COMMIT_ANNOTATION: "old"
                }
            }
        });

        assert!(stamp_current_git_commit_annotation(&mut node));
        assert_eq!(
            node.pointer("/metadata/annotations/klights.io~1git-commit")
                .and_then(|value| value.as_str()),
            Some(crate::version::GIT_COMMIT_SHORT)
        );
        assert!(!stamp_current_git_commit_annotation(&mut node));
    }
}
