use crate::protobuf::*;
pub fn json_endpoint_address_to_pb(
    addr: &k8s_openapi::api::core::v1::EndpointAddress,
) -> k8s_pb::api::core::v1::EndpointAddress {
    k8s_pb::api::core::v1::EndpointAddress {
        ip: Some(addr.ip.clone()),
        hostname: addr.hostname.clone(),
        node_name: addr.node_name.clone(),
        target_ref: addr.target_ref.as_ref().map(json_obj_ref_to_pb),
    }
}

pub fn json_endpoint_port_to_pb(
    port: &k8s_openapi::api::core::v1::EndpointPort,
) -> k8s_pb::api::core::v1::EndpointPort {
    k8s_pb::api::core::v1::EndpointPort {
        name: port.name.clone(),
        port: Some(port.port),
        protocol: port.protocol.clone(),
        app_protocol: port.app_protocol.clone(),
    }
}

pub fn json_endpoint_subset_to_pb(
    subset: &k8s_openapi::api::core::v1::EndpointSubset,
) -> k8s_pb::api::core::v1::EndpointSubset {
    k8s_pb::api::core::v1::EndpointSubset {
        addresses: subset
            .addresses
            .as_ref()
            .map(|addrs| addrs.iter().map(json_endpoint_address_to_pb).collect())
            .unwrap_or_default(),
        not_ready_addresses: subset
            .not_ready_addresses
            .as_ref()
            .map(|addrs| addrs.iter().map(json_endpoint_address_to_pb).collect())
            .unwrap_or_default(),
        ports: subset
            .ports
            .as_ref()
            .map(|ports| ports.iter().map(json_endpoint_port_to_pb).collect())
            .unwrap_or_default(),
    }
}

/// Convert k8s-openapi Namespace to k8s-pb Namespace
pub fn json_namespace_to_pb(
    ns: &k8s_openapi::api::core::v1::Namespace,
) -> anyhow::Result<k8s_pb::api::core::v1::Namespace> {
    Ok(k8s_pb::api::core::v1::Namespace {
        metadata: Some(json_meta_to_pb(&ns.metadata)),
        spec: ns
            .spec
            .as_ref()
            .map(|spec| k8s_pb::api::core::v1::NamespaceSpec {
                finalizers: spec.finalizers.clone().unwrap_or_default(),
            }),
        status: ns
            .status
            .as_ref()
            .map(|status| k8s_pb::api::core::v1::NamespaceStatus {
                phase: status.phase.clone(),
                conditions: status
                    .conditions
                    .as_ref()
                    .map(|conds| {
                        conds
                            .iter()
                            .map(|c| k8s_pb::api::core::v1::NamespaceCondition {
                                r#type: Some(c.type_.clone()),
                                status: Some(c.status.clone()),
                                last_transition_time: c
                                    .last_transition_time
                                    .as_ref()
                                    .map(json_time_to_pb),
                                reason: c.reason.clone(),
                                message: c.message.clone(),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
            }),
    })
}

/// Convert k8s-openapi ObjectMeta to k8s-pb ObjectMeta
pub fn json_meta_to_pb(
    meta: &k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
) -> k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta {
    k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta {
        name: meta.name.clone(),
        namespace: meta.namespace.clone(),
        uid: meta.uid.clone(),
        resource_version: meta.resource_version.clone(),
        generation: meta.generation,
        labels: meta
            .labels
            .clone()
            .map(|btree| btree.into_iter().collect())
            .unwrap_or_default(),
        annotations: meta
            .annotations
            .clone()
            .map(|btree| btree.into_iter().collect())
            .unwrap_or_default(),
        finalizers: meta.finalizers.clone().unwrap_or_default(),
        owner_references: meta
            .owner_references
            .as_ref()
            .map(|refs| {
                refs.iter()
                    .map(
                        |r| k8s_pb::apimachinery::pkg::apis::meta::v1::OwnerReference {
                            api_version: r.api_version.clone().into(),
                            kind: r.kind.clone().into(),
                            name: r.name.clone().into(),
                            uid: r.uid.clone().into(),
                            controller: r.controller,
                            block_owner_deletion: r.block_owner_deletion,
                        },
                    )
                    .collect()
            })
            .unwrap_or_default(),
        creation_timestamp: meta.creation_timestamp.as_ref().map(json_time_to_pb),
        deletion_timestamp: meta.deletion_timestamp.as_ref().map(json_time_to_pb),
        deletion_grace_period_seconds: meta.deletion_grace_period_seconds,
        self_link: meta.self_link.clone(),
        managed_fields: vec![], // Skip managedFields for now
        ..Default::default()
    }
}

/// Convert k8s-openapi Time to k8s-pb Timestamp
pub fn json_time_to_pb(
    time: &k8s_openapi::apimachinery::pkg::apis::meta::v1::Time,
) -> k8s_pb::apimachinery::pkg::apis::meta::v1::Time {
    // k8s-openapi Time wraps chrono::DateTime
    // Convert to Unix timestamp
    k8s_pb::apimachinery::pkg::apis::meta::v1::Time {
        seconds: Some(time.0.timestamp()),
        nanos: Some(time.0.timestamp_subsec_nanos() as i32),
    }
}

/// Convert k8s-openapi ResourceQuota to k8s-pb ResourceQuota
pub fn json_resourcequota_to_pb(
    rq: &k8s_openapi::api::core::v1::ResourceQuota,
) -> anyhow::Result<k8s_pb::api::core::v1::ResourceQuota> {
    use k8s_pb::apimachinery::pkg::api::resource::Quantity;

    let convert_quantities = |map: &Option<
        std::collections::BTreeMap<String, k8s_openapi::apimachinery::pkg::api::resource::Quantity>,
    >|
     -> std::collections::BTreeMap<String, Quantity> {
        map.as_ref()
            .map(|m| {
                m.iter()
                    .map(|(k, v)| {
                        (
                            k.clone(),
                            Quantity {
                                string: Some(v.0.clone()),
                            },
                        )
                    })
                    .collect()
            })
            .unwrap_or_default()
    };

    Ok(k8s_pb::api::core::v1::ResourceQuota {
        metadata: Some(json_meta_to_pb(&rq.metadata)),
        spec: rq
            .spec
            .as_ref()
            .map(|spec| k8s_pb::api::core::v1::ResourceQuotaSpec {
                hard: convert_quantities(&spec.hard),
                scopes: spec.scopes.clone().unwrap_or_default(),
                scope_selector: spec.scope_selector.as_ref().map(|sel| {
                    k8s_pb::api::core::v1::ScopeSelector {
                        match_expressions: sel
                            .match_expressions
                            .as_ref()
                            .map(|exprs| {
                                exprs
                                    .iter()
                                    .map(|e| {
                                        k8s_pb::api::core::v1::ScopedResourceSelectorRequirement {
                                            scope_name: Some(e.scope_name.clone()),
                                            operator: Some(e.operator.clone()),
                                            values: e.values.clone().unwrap_or_default(),
                                        }
                                    })
                                    .collect()
                            })
                            .unwrap_or_default(),
                    }
                }),
            }),
        status: rq
            .status
            .as_ref()
            .map(|status| k8s_pb::api::core::v1::ResourceQuotaStatus {
                hard: convert_quantities(&status.hard),
                used: convert_quantities(&status.used),
            }),
    })
}

/// Convert k8s-openapi LimitRange to k8s-pb LimitRange
pub fn json_limitrange_to_pb(
    lr: &k8s_openapi::api::core::v1::LimitRange,
) -> anyhow::Result<k8s_pb::api::core::v1::LimitRange> {
    use k8s_pb::apimachinery::pkg::api::resource::Quantity;

    let convert_qty_map = |map: &Option<
        std::collections::BTreeMap<String, k8s_openapi::apimachinery::pkg::api::resource::Quantity>,
    >|
     -> std::collections::BTreeMap<String, Quantity> {
        map.as_ref()
            .map(|m| {
                m.iter()
                    .map(|(k, v)| {
                        (
                            k.clone(),
                            Quantity {
                                string: Some(v.0.clone()),
                            },
                        )
                    })
                    .collect()
            })
            .unwrap_or_default()
    };

    Ok(k8s_pb::api::core::v1::LimitRange {
        metadata: Some(json_meta_to_pb(&lr.metadata)),
        spec: lr
            .spec
            .as_ref()
            .map(|spec| k8s_pb::api::core::v1::LimitRangeSpec {
                limits: spec
                    .limits
                    .iter()
                    .map(|item| k8s_pb::api::core::v1::LimitRangeItem {
                        r#type: Some(item.type_.clone()),
                        default: convert_qty_map(&item.default),
                        default_request: convert_qty_map(&item.default_request),
                        max: convert_qty_map(&item.max),
                        min: convert_qty_map(&item.min),
                        max_limit_request_ratio: convert_qty_map(&item.max_limit_request_ratio),
                    })
                    .collect(),
            }),
    })
}
