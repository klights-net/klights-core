use crate::protobuf::*;
pub fn json_endpointslice_to_pb(
    slice: &k8s_openapi::api::discovery::v1::EndpointSlice,
) -> k8s_pb::api::discovery::v1::EndpointSlice {
    use k8s_pb::api::discovery::v1 as discoveryv1;

    let endpoints = slice
        .endpoints
        .iter()
        .map(|ep| discoveryv1::Endpoint {
            addresses: ep.addresses.clone(),
            conditions: ep
                .conditions
                .as_ref()
                .map(|cond| discoveryv1::EndpointConditions {
                    ready: cond.ready,
                    serving: cond.serving,
                    terminating: cond.terminating,
                }),
            hostname: ep.hostname.clone(),
            node_name: ep.node_name.clone(),
            target_ref: ep
                .target_ref
                .as_ref()
                .map(|r| k8s_pb::api::core::v1::ObjectReference {
                    kind: r.kind.clone(),
                    name: r.name.clone(),
                    namespace: r.namespace.clone(),
                    uid: r.uid.clone(),
                    ..Default::default()
                }),
            zone: ep.zone.clone(),
            ..Default::default()
        })
        .collect();

    let ports = slice
        .ports
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|p| discoveryv1::EndpointPort {
            name: p.name.clone(),
            port: p.port,
            protocol: p.protocol.clone(),
            app_protocol: p.app_protocol.clone(),
        })
        .collect();

    discoveryv1::EndpointSlice {
        metadata: Some(json_meta_to_pb(&slice.metadata)),
        address_type: Some(slice.address_type.clone()),
        endpoints,
        ports,
    }
}
