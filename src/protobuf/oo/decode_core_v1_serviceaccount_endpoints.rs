use crate::protobuf::*;
pb_decode!(
    pb_serviceaccount_to_json,
    k8s_pb::api::core::v1::ServiceAccount,
    sa,
    "v1",
    "ServiceAccount",
    obj,
    {
        if !sa.secrets.is_empty() {
            let secrets: Vec<Value> = sa
                .secrets
                .iter()
                .map(|s| {
                    let mut secret_ref = json!({});
                    if let Some(name) = &s.name {
                        secret_ref["name"] = json!(name);
                    }
                    secret_ref
                })
                .collect();
            obj["secrets"] = json!(secrets);
        }
        if let Some(automount) = sa.automount_service_account_token {
            obj["automountServiceAccountToken"] = json!(automount);
        }
        if !sa.image_pull_secrets.is_empty() {
            let ips: Vec<Value> = sa
                .image_pull_secrets
                .iter()
                .map(|s| {
                    let mut ref_obj = json!({});
                    if let Some(name) = &s.name {
                        ref_obj["name"] = json!(name);
                    }
                    ref_obj
                })
                .collect();
            obj["imagePullSecrets"] = json!(ips);
        }
    }
);

pb_decode!(
    pb_endpoints_to_json,
    k8s_pb::api::core::v1::Endpoints,
    ep,
    "v1",
    "Endpoints",
    obj,
    {
        if !ep.subsets.is_empty() {
            let subsets: Vec<Value> = ep
                .subsets
                .iter()
                .map(|subset| {
                    let mut subset_obj = json!({});
                    if !subset.addresses.is_empty() {
                        let addresses: Vec<Value> = subset
                            .addresses
                            .iter()
                            .map(|addr| {
                                let mut addr_obj = json!({});
                                if let Some(ip) = &addr.ip {
                                    addr_obj["ip"] = json!(ip);
                                }
                                if let Some(hostname) = &addr.hostname {
                                    addr_obj["hostname"] = json!(hostname);
                                }
                                if let Some(node_name) = &addr.node_name {
                                    addr_obj["nodeName"] = json!(node_name);
                                }
                                if let Some(target_ref) = &addr.target_ref {
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
                                    addr_obj["targetRef"] = ref_obj;
                                }
                                addr_obj
                            })
                            .collect();
                        subset_obj["addresses"] = json!(addresses);
                    }
                    if !subset.ports.is_empty() {
                        let ports: Vec<Value> = subset
                            .ports
                            .iter()
                            .map(|port| {
                                let mut port_obj = json!({});
                                if let Some(port_num) = port.port {
                                    port_obj["port"] = json!(port_num);
                                }
                                if let Some(protocol) = &port.protocol {
                                    port_obj["protocol"] = json!(protocol);
                                }
                                if let Some(name) = &port.name {
                                    port_obj["name"] = json!(name);
                                }
                                port_obj
                            })
                            .collect();
                        subset_obj["ports"] = json!(ports);
                    }
                    subset_obj
                })
                .collect();
            obj["subsets"] = json!(subsets);
        }
    }
);
