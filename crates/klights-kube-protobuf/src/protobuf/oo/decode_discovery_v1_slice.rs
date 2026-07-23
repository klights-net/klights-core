use crate::protobuf::*;
pb_decode!(
    pb_endpointslice_to_json,
    k8s_pb::api::discovery::v1::EndpointSlice,
    slice,
    "discovery.k8s.io/v1",
    "EndpointSlice",
    obj,
    {
        if let Some(addr_type) = &slice.address_type {
            obj["addressType"] = json!(addr_type);
        }
        if !slice.endpoints.is_empty() {
            let endpoints: Vec<Value> = slice
                .endpoints
                .iter()
                .map(|ep| {
                    let mut ep_obj = json!({});
                    if !ep.addresses.is_empty() {
                        ep_obj["addresses"] = json!(ep.addresses);
                    }
                    if let Some(cond) = &ep.conditions {
                        let mut cond_obj = json!({});
                        if let Some(ready) = cond.ready {
                            cond_obj["ready"] = json!(ready);
                        }
                        if let Some(serving) = cond.serving {
                            cond_obj["serving"] = json!(serving);
                        }
                        if let Some(terminating) = cond.terminating {
                            cond_obj["terminating"] = json!(terminating);
                        }
                        ep_obj["conditions"] = cond_obj;
                    }
                    if let Some(hostname) = &ep.hostname {
                        ep_obj["hostname"] = json!(hostname);
                    }
                    if let Some(node_name) = &ep.node_name {
                        ep_obj["nodeName"] = json!(node_name);
                    }
                    if let Some(target_ref) = &ep.target_ref {
                        let mut ref_obj = json!({});
                        if let Some(kind) = &target_ref.kind {
                            ref_obj["kind"] = json!(kind);
                        }
                        if let Some(name) = &target_ref.name {
                            ref_obj["name"] = json!(name);
                        }
                        if let Some(namespace) = &target_ref.namespace {
                            ref_obj["namespace"] = json!(namespace);
                        }
                        if let Some(uid) = &target_ref.uid {
                            ref_obj["uid"] = json!(uid);
                        }
                        ep_obj["targetRef"] = ref_obj;
                    }
                    if let Some(zone) = &ep.zone {
                        ep_obj["zone"] = json!(zone);
                    }
                    ep_obj
                })
                .collect();
            obj["endpoints"] = json!(endpoints);
        }
        if !slice.ports.is_empty() {
            let ports: Vec<Value> = slice
                .ports
                .iter()
                .map(|port| {
                    let mut port_obj = json!({});
                    if let Some(name) = &port.name {
                        port_obj["name"] = json!(name);
                    }
                    if let Some(p) = port.port {
                        port_obj["port"] = json!(p);
                    }
                    if let Some(protocol) = &port.protocol {
                        port_obj["protocol"] = json!(protocol);
                    }
                    if let Some(app_protocol) = &port.app_protocol {
                        port_obj["appProtocol"] = json!(app_protocol);
                    }
                    port_obj
                })
                .collect();
            obj["ports"] = json!(ports);
        }
    }
);
