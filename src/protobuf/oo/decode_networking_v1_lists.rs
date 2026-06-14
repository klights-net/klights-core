/// Single Ingress decoder (P0-E2E-20260423-01).
use crate::protobuf::*;
pub fn pb_single_ingress_to_json(
    ingress: &k8s_pb::api::networking::v1::Ingress,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "networking.k8s.io/v1", "kind": "Ingress"});
    if let Some(meta) = &ingress.metadata {
        obj["metadata"] = meta_to_json(meta);
    }
    if let Some(spec) = &ingress.spec {
        let mut spec_obj = json!({});
        if let Some(cn) = &spec.ingress_class_name {
            spec_obj["ingressClassName"] = json!(cn);
        }
        obj["spec"] = spec_obj;
    }
    if let Some(status) = &ingress.status {
        let mut status_obj = json!({});
        if let Some(lb) = &status.load_balancer {
            let ingress_points: Vec<Value> = lb
                .ingress
                .iter()
                .map(|point| {
                    let mut point_obj = json!({});
                    if let Some(ip) = &point.ip {
                        point_obj["ip"] = json!(ip);
                    }
                    if let Some(hostname) = &point.hostname {
                        point_obj["hostname"] = json!(hostname);
                    }
                    if !point.ports.is_empty() {
                        point_obj["ports"] = json!(
                            point
                                .ports
                                .iter()
                                .map(|port| {
                                    let mut port_obj = json!({});
                                    if let Some(port_number) = port.port {
                                        port_obj["port"] = json!(port_number);
                                    }
                                    if let Some(protocol) = &port.protocol {
                                        port_obj["protocol"] = json!(protocol);
                                    }
                                    if let Some(error) = &port.error {
                                        port_obj["error"] = json!(error);
                                    }
                                    port_obj
                                })
                                .collect::<Vec<_>>()
                        );
                    }
                    point_obj
                })
                .collect();
            status_obj["loadBalancer"] = json!({"ingress": ingress_points});
        }
        obj["status"] = status_obj;
    }
    Ok(obj)
}

/// Single IngressClass decoder (P0-E2E-20260423-01).
pub fn pb_single_ingressclass_to_json(
    ic: &k8s_pb::api::networking::v1::IngressClass,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "networking.k8s.io/v1", "kind": "IngressClass"});
    if let Some(meta) = &ic.metadata {
        obj["metadata"] = meta_to_json(meta);
    }
    if let Some(spec) = &ic.spec {
        let mut spec_obj = json!({});
        if let Some(ctrl) = &spec.controller {
            spec_obj["controller"] = json!(ctrl);
        }
        obj["spec"] = spec_obj;
    }
    Ok(obj)
}

/// IngressList decoder
pub fn pb_ingresslist_to_json(
    list: &k8s_pb::api::networking::v1::IngressList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "networking.k8s.io/v1", "kind": "IngressList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .map(|item| {
            json!({
                "apiVersion": "networking.k8s.io/v1",
                "kind": "Ingress",
                "metadata": item.metadata.as_ref().map(meta_to_json).unwrap_or_default(),
            })
        })
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}

/// IngressClassList decoder
pub fn pb_ingressclasslist_to_json(
    list: &k8s_pb::api::networking::v1::IngressClassList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "networking.k8s.io/v1", "kind": "IngressClassList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items: Vec<Value> = list
        .items
        .iter()
        .map(|item| {
            json!({
                "apiVersion": "networking.k8s.io/v1",
                "kind": "IngressClass",
                "metadata": item.metadata.as_ref().map(meta_to_json).unwrap_or_default(),
            })
        })
        .collect();
    obj["items"] = json!(items);
    Ok(obj)
}

/// Convert a protobuf NetworkPolicyPort to JSON.
fn pb_np_port_to_json(p: &k8s_pb::api::networking::v1::NetworkPolicyPort) -> Value {
    use serde_json::json;
    let mut obj = json!({});
    if let Some(proto) = &p.protocol {
        obj["protocol"] = json!(proto);
    }
    if let Some(port) = &p.port {
        obj["port"] = intorstring_to_json(port);
    }
    if let Some(end_port) = p.end_port {
        obj["endPort"] = json!(end_port);
    }
    obj
}

/// Convert a protobuf NetworkPolicyPeer to JSON.
fn pb_np_peer_to_json(p: &k8s_pb::api::networking::v1::NetworkPolicyPeer) -> Value {
    use serde_json::json;
    let mut obj = json!({});
    if let Some(sel) = &p.pod_selector {
        obj["podSelector"] = pb_label_selector_to_json(sel);
    }
    if let Some(sel) = &p.namespace_selector {
        obj["namespaceSelector"] = pb_label_selector_to_json(sel);
    }
    if let Some(block) = &p.ip_block {
        let mut ip_obj = json!({});
        if let Some(cidr) = &block.cidr {
            ip_obj["cidr"] = json!(cidr);
        }
        if !block.except.is_empty() {
            ip_obj["except"] = json!(block.except);
        }
        obj["ipBlock"] = ip_obj;
    }
    obj
}

/// Convert a single protobuf NetworkPolicy to its full JSON representation.
/// Shared between single-resource decoding and the list decoder so spec is
/// preserved in both paths (F1-01).
pub fn pb_single_networkpolicy_to_json(
    np: &k8s_pb::api::networking::v1::NetworkPolicy,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy"});
    if let Some(meta) = &np.metadata {
        obj["metadata"] = meta_to_json(meta);
    }
    if let Some(spec) = &np.spec {
        let mut spec_obj = json!({});
        if let Some(sel) = &spec.pod_selector {
            spec_obj["podSelector"] = pb_label_selector_to_json(sel);
        } else {
            spec_obj["podSelector"] = json!({});
        }
        if !spec.policy_types.is_empty() {
            spec_obj["policyTypes"] = json!(spec.policy_types);
        }
        if !spec.ingress.is_empty() {
            let rules: Vec<Value> = spec
                .ingress
                .iter()
                .map(|r| {
                    let mut rule_obj = json!({});
                    if !r.from.is_empty() {
                        rule_obj["from"] =
                            json!(r.from.iter().map(pb_np_peer_to_json).collect::<Vec<_>>());
                    }
                    if !r.ports.is_empty() {
                        rule_obj["ports"] =
                            json!(r.ports.iter().map(pb_np_port_to_json).collect::<Vec<_>>());
                    }
                    rule_obj
                })
                .collect();
            spec_obj["ingress"] = json!(rules);
        }
        if !spec.egress.is_empty() {
            let rules: Vec<Value> = spec
                .egress
                .iter()
                .map(|r| {
                    let mut rule_obj = json!({});
                    if !r.to.is_empty() {
                        rule_obj["to"] =
                            json!(r.to.iter().map(pb_np_peer_to_json).collect::<Vec<_>>());
                    }
                    if !r.ports.is_empty() {
                        rule_obj["ports"] =
                            json!(r.ports.iter().map(pb_np_port_to_json).collect::<Vec<_>>());
                    }
                    rule_obj
                })
                .collect();
            spec_obj["egress"] = json!(rules);
        }
        obj["spec"] = spec_obj;
    }
    Ok(obj)
}

/// NetworkPolicyList decoder. Each item delegates to the single-resource decoder
/// so spec is preserved in list responses too.
pub fn pb_networkpolicylist_to_json(
    list: &k8s_pb::api::networking::v1::NetworkPolicyList,
) -> anyhow::Result<Value> {
    use serde_json::json;
    let mut obj = json!({"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicyList"});
    obj["metadata"] = pb_listmeta_to_json(list.metadata.as_ref());
    let items = list
        .items
        .iter()
        .map(pb_single_networkpolicy_to_json)
        .collect::<anyhow::Result<Vec<_>>>()?;
    obj["items"] = json!(items);
    Ok(obj)
}
