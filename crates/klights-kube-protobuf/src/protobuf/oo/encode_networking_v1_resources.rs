/// Encode IngressList from JSON value to protobuf
use crate::protobuf::*;
pub fn json_ingresslist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::networking::v1::IngressList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("IngressList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::networking::v1::Ingress::deserialize(item)?;
            json_ingress_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(k8s_pb::api::networking::v1::IngressList {
        metadata,
        items: pb_items,
    })
}

/// Encode IngressClassList from JSON value to protobuf
pub fn json_ingressclasslist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::networking::v1::IngressClassList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("IngressClassList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::networking::v1::IngressClass::deserialize(item)?;
            json_ingressclass_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(k8s_pb::api::networking::v1::IngressClassList {
        metadata,
        items: pb_items,
    })
}

/// Encode NetworkPolicyList from JSON value to protobuf
pub fn json_networkpolicylist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::networking::v1::NetworkPolicyList> {
    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("NetworkPolicyList missing items array"))?;

    let pb_items = items
        .iter()
        .map(|item| {
            let openapi = k8s_openapi::api::networking::v1::NetworkPolicy::deserialize(item)?;
            json_networkpolicy_to_pb(&openapi)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(k8s_pb::api::networking::v1::NetworkPolicyList {
        metadata,
        items: pb_items,
    })
}

/// Encode Ingress to protobuf (minimal implementation)
pub fn json_ingress_to_pb(
    ingress: &k8s_openapi::api::networking::v1::Ingress,
) -> anyhow::Result<k8s_pb::api::networking::v1::Ingress> {
    use k8s_pb::api::networking::v1 as networkingv1;
    Ok(networkingv1::Ingress {
        metadata: Some(json_meta_to_pb(&ingress.metadata)),
        spec: ingress.spec.as_ref().map(|spec| networkingv1::IngressSpec {
            ingress_class_name: spec.ingress_class_name.clone(),
            ..Default::default()
        }),
        status: ingress
            .status
            .as_ref()
            .map(|status| networkingv1::IngressStatus {
                load_balancer: status.load_balancer.as_ref().map(|lb| {
                    networkingv1::IngressLoadBalancerStatus {
                        ingress: lb
                            .ingress
                            .as_ref()
                            .map(|points| {
                                points
                                    .iter()
                                    .map(|point| networkingv1::IngressLoadBalancerIngress {
                                        ip: point.ip.clone(),
                                        hostname: point.hostname.clone(),
                                        ports: point
                                            .ports
                                            .as_ref()
                                            .map(|ports| {
                                                ports
                                                    .iter()
                                                    .map(|port| networkingv1::IngressPortStatus {
                                                        port: Some(port.port),
                                                        protocol: Some(port.protocol.clone()),
                                                        error: port.error.clone(),
                                                    })
                                                    .collect()
                                            })
                                            .unwrap_or_default(),
                                    })
                                    .collect()
                            })
                            .unwrap_or_default(),
                    }
                }),
            }),
    })
}

/// Encode IngressClass to protobuf (minimal implementation)
pub fn json_ingressclass_to_pb(
    ic: &k8s_openapi::api::networking::v1::IngressClass,
) -> anyhow::Result<k8s_pb::api::networking::v1::IngressClass> {
    use k8s_pb::api::networking::v1 as networkingv1;
    Ok(networkingv1::IngressClass {
        metadata: Some(json_meta_to_pb(&ic.metadata)),
        spec: ic.spec.as_ref().map(|spec| networkingv1::IngressClassSpec {
            controller: spec.controller.clone(),
            ..Default::default()
        }),
    })
}

/// Encode NetworkPolicy spec ingress/egress port to protobuf.
fn json_np_port_to_pb(
    p: &k8s_openapi::api::networking::v1::NetworkPolicyPort,
) -> k8s_pb::api::networking::v1::NetworkPolicyPort {
    k8s_pb::api::networking::v1::NetworkPolicyPort {
        protocol: p.protocol.clone(),
        port: p.port.as_ref().map(openapi_intorstring_to_pb),
        end_port: p.end_port,
    }
}

/// Encode NetworkPolicy peer to protobuf.
fn json_np_peer_to_pb(
    p: &k8s_openapi::api::networking::v1::NetworkPolicyPeer,
) -> k8s_pb::api::networking::v1::NetworkPolicyPeer {
    k8s_pb::api::networking::v1::NetworkPolicyPeer {
        pod_selector: p.pod_selector.as_ref().map(json_label_selector_to_pb),
        namespace_selector: p.namespace_selector.as_ref().map(json_label_selector_to_pb),
        ip_block: p
            .ip_block
            .as_ref()
            .map(|b| k8s_pb::api::networking::v1::IpBlock {
                cidr: Some(b.cidr.clone()),
                except: b.except.clone().unwrap_or_default(),
            }),
    }
}

/// Encode NetworkPolicy to protobuf with full ingress/egress/policyTypes
/// coverage (F1-01).
pub fn json_networkpolicy_to_pb(
    np: &k8s_openapi::api::networking::v1::NetworkPolicy,
) -> anyhow::Result<k8s_pb::api::networking::v1::NetworkPolicy> {
    use k8s_pb::api::networking::v1 as networkingv1;
    Ok(networkingv1::NetworkPolicy {
        metadata: Some(json_meta_to_pb(&np.metadata)),
        spec: np
            .spec
            .as_ref()
            .map(|spec| networkingv1::NetworkPolicySpec {
                pod_selector: Some(json_label_selector_to_pb(&spec.pod_selector)),
                ingress: spec
                    .ingress
                    .as_ref()
                    .map(|rules| {
                        rules
                            .iter()
                            .map(|r| networkingv1::NetworkPolicyIngressRule {
                                ports: r
                                    .ports
                                    .as_ref()
                                    .map(|ps| ps.iter().map(json_np_port_to_pb).collect())
                                    .unwrap_or_default(),
                                from: r
                                    .from
                                    .as_ref()
                                    .map(|fr| fr.iter().map(json_np_peer_to_pb).collect())
                                    .unwrap_or_default(),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                egress: spec
                    .egress
                    .as_ref()
                    .map(|rules| {
                        rules
                            .iter()
                            .map(|r| networkingv1::NetworkPolicyEgressRule {
                                ports: r
                                    .ports
                                    .as_ref()
                                    .map(|ps| ps.iter().map(json_np_port_to_pb).collect())
                                    .unwrap_or_default(),
                                to: r
                                    .to
                                    .as_ref()
                                    .map(|to| to.iter().map(json_np_peer_to_pb).collect())
                                    .unwrap_or_default(),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                policy_types: spec.policy_types.clone().unwrap_or_default(),
            }),
    })
}

/// Encode ServiceCIDR to protobuf.
///
/// `k8s_pb` currently ships ServiceCIDR under networking/v1beta1. The wire
/// shape is compatible with v1 for fields we serve (`metadata`, `spec.cidrs`,
/// `status.conditions`), so we use that message type for protobuf transport.
pub fn json_servicecidr_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::networking::v1beta1::ServiceCIDR> {
    use k8s_pb::api::networking::v1beta1 as netv1beta1;

    let metadata = value
        .get("metadata")
        .cloned()
        .map(serde_json::from_value::<k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta>)
        .transpose()?
        .map(|m| json_meta_to_pb(&m));

    let spec =
        value
            .get("spec")
            .and_then(|s| s.as_object())
            .map(|spec| netv1beta1::ServiceCIDRSpec {
                cidrs: spec
                    .get("cidrs")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|item| item.as_str().map(ToString::to_string))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default(),
            });

    let status = value
        .get("status")
        .and_then(|s| s.as_object())
        .map(|status| netv1beta1::ServiceCIDRStatus {
            conditions: status
                .get("conditions")
                .and_then(|v| v.as_array())
                .map(|conds| {
                    conds
                        .iter()
                        .map(|c| k8s_pb::apimachinery::pkg::apis::meta::v1::Condition {
                            r#type: c
                                .get("type")
                                .and_then(|v| v.as_str())
                                .map(ToString::to_string),
                            status: c
                                .get("status")
                                .and_then(|v| v.as_str())
                                .map(ToString::to_string),
                            observed_generation: c
                                .get("observedGeneration")
                                .and_then(|v| v.as_i64()),
                            last_transition_time: c
                                .get("lastTransitionTime")
                                .and_then(|v| v.as_str())
                                .and_then(raw_time_str_to_pb),
                            reason: c
                                .get("reason")
                                .and_then(|v| v.as_str())
                                .map(ToString::to_string),
                            message: c
                                .get("message")
                                .and_then(|v| v.as_str())
                                .map(ToString::to_string),
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default(),
        });

    Ok(netv1beta1::ServiceCIDR {
        metadata,
        spec,
        status,
    })
}

/// Encode ServiceCIDRList to protobuf.
pub fn json_servicecidrlist_to_pb(
    value: &Value,
) -> anyhow::Result<k8s_pb::api::networking::v1beta1::ServiceCIDRList> {
    use k8s_pb::api::networking::v1beta1 as netv1beta1;

    let metadata = value.get("metadata").and_then(|m| {
        let openapi =
            k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta::deserialize(m).ok()?;
        Some(json_listmeta_to_pb(&openapi))
    });

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("ServiceCIDRList missing items array"))?;

    let pb_items = items
        .iter()
        .map(json_servicecidr_to_pb)
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(netv1beta1::ServiceCIDRList {
        metadata,
        items: pb_items,
    })
}
