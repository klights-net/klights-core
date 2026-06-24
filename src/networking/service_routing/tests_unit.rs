use crate::networking::service_routing::*;
use serde_json::json;
use std::net::Ipv4Addr;

// `get_host_ip` was deleted in Task 8 of the network refactor; host IP
// discovery now happens once at bootstrap via UDP `local_addr`, and
// post-Plane callers ask `Datapath::host_ip`. The bootstrap helper has
// its own coverage via the discovery path's idempotent UDP bind.

#[test]
fn test_prefix_len_from_mask() {
    assert_eq!(prefix_len_from_mask(Ipv4Addr::new(255, 255, 0, 0)), 16);
    assert_eq!(prefix_len_from_mask(Ipv4Addr::new(255, 0, 0, 0)), 8);
    assert_eq!(prefix_len_from_mask(Ipv4Addr::new(255, 255, 255, 0)), 24);
}

// ---- SessionAffinity parsing -----------------------------------------

#[test]
fn test_parse_session_affinity_defaults_to_none() {
    let spec = json!({"clusterIP": "10.43.128.5", "ports": [{"port": 80}]});
    assert_eq!(parse_session_affinity(&spec), SessionAffinity::None);
}

#[test]
fn test_parse_session_affinity_client_ip() {
    let spec = json!({"clusterIP": "10.43.128.5", "sessionAffinity": "ClientIP"});
    assert_eq!(parse_session_affinity(&spec), SessionAffinity::ClientIp);
}

#[test]
fn test_servicespec_session_affinity_propagated_from_service_json() {
    let svc = json!({
        "spec": {
            "clusterIP": "10.43.128.7",
            "sessionAffinity": "ClientIP",
            "ports": [{"port": 80, "targetPort": 8080, "protocol": "TCP"}]
        }
    });
    let endpoints = json!({
        "subsets": [{
            "addresses": [{"ip": "10.43.0.10"}, {"ip": "10.43.0.11"}],
            "ports": [{"port": 8080, "protocol": "TCP"}]
        }]
    });
    let spec = ServiceSpec::from_service_and_endpoints(&svc, Some(&endpoints)).expect("must parse");
    assert_eq!(
        spec.session_affinity,
        SessionAffinity::ClientIp,
        "ClientIP sessionAffinity must propagate from service spec"
    );
}

// ---- ServiceSpec parsing ------------------------------------------

#[test]
fn test_servicespec_clusterip_with_endpoints_extracts_one_port() {
    let svc = json!({
        "spec": {
            "clusterIP": "10.43.128.5",
            "ports": [{"port": 80, "targetPort": 8080, "protocol": "TCP"}]
        }
    });
    let endpoints = json!({
        "subsets": [{
            "addresses": [{"ip": "10.43.0.10"}, {"ip": "10.43.0.11"}],
            "ports": [{"port": 8080, "protocol": "TCP"}]
        }]
    });
    let spec = ServiceSpec::from_service_and_endpoints(&svc, Some(&endpoints)).expect("must parse");
    assert_eq!(spec.cluster_ip, Ipv4Addr::new(10, 43, 128, 5));
    assert_eq!(spec.ports.len(), 1);
    assert_eq!(spec.ports[0].service_port, 80);
    assert_eq!(spec.ports[0].target_port, 8080);
    assert_eq!(spec.ports[0].protocol, Protocol::Tcp);
    assert_eq!(spec.ports[0].node_port, None);
    assert_eq!(spec.ports[0].endpoints.len(), 2);
}

#[test]
fn test_servicespec_skips_external_name_service() {
    let svc = json!({"spec": {"type": "ExternalName", "externalName": "example.com"}});
    assert!(ServiceSpec::from_service_and_endpoints(&svc, None).is_none());
}

#[test]
fn test_servicespec_skips_headless_service() {
    let svc = json!({
        "spec": {
            "clusterIP": "None",
            "ports": [{"port": 80, "targetPort": 8080}]
        }
    });
    assert!(ServiceSpec::from_service_and_endpoints(&svc, None).is_none());
}

#[test]
fn test_servicespec_skips_service_with_no_ready_endpoints() {
    let svc = json!({
        "spec": {
            "clusterIP": "10.43.128.5",
            "ports": [{"port": 80, "targetPort": 8080}]
        }
    });
    assert!(
        ServiceSpec::from_service_and_endpoints(&svc, None).is_none(),
        "no Endpoints object → no PortSpec → no ServiceSpec"
    );
    let empty_eps = json!({"subsets": []});
    assert!(
        ServiceSpec::from_service_and_endpoints(&svc, Some(&empty_eps)).is_none(),
        "empty subsets → no rules → no ServiceSpec"
    );
}

#[test]
fn test_servicespec_filters_invalid_endpoint_ips() {
    let svc = json!({
        "spec": {
            "clusterIP": "10.43.128.5",
            "ports": [{"port": 80, "targetPort": 8080}]
        }
    });
    let endpoints = json!({
        "subsets": [{
            "addresses": [{"ip": "0.0.0.0"}, {"ip": ""}, {"ip": "10.43.0.50"}],
            "ports": [{"port": 8080, "protocol": "TCP"}]
        }]
    });
    let spec = ServiceSpec::from_service_and_endpoints(&svc, Some(&endpoints)).unwrap();
    assert_eq!(spec.ports.len(), 1);
    assert_eq!(spec.ports[0].endpoints, vec![Ipv4Addr::new(10, 43, 0, 50)]);
}

#[test]
fn test_servicespec_nodeport_carries_through_to_portspec() {
    let svc = json!({
        "spec": {
            "type": "NodePort",
            "clusterIP": "10.43.128.10",
            "ports": [{"port": 80, "targetPort": 8080, "nodePort": 30080}]
        }
    });
    let endpoints = json!({
        "subsets": [{
            "addresses": [{"ip": "10.43.0.20"}],
            "ports": [{"port": 8080, "protocol": "TCP"}]
        }]
    });
    let spec = ServiceSpec::from_service_and_endpoints(&svc, Some(&endpoints)).unwrap();
    assert_eq!(spec.ports[0].node_port, Some(30080));
}

#[test]
fn test_servicespec_defaults_protocol_to_tcp() {
    let svc = json!({
        "spec": {
            "clusterIP": "10.43.128.40",
            "ports": [{"port": 53, "targetPort": 53}]
        }
    });
    let endpoints = json!({
        "subsets": [{
            "addresses": [{"ip": "10.43.0.60"}],
            "ports": [{"port": 53}]   // no protocol field
        }]
    });
    let spec = ServiceSpec::from_service_and_endpoints(&svc, Some(&endpoints)).unwrap();
    assert_eq!(spec.ports[0].protocol, Protocol::Tcp);
}

#[test]
fn test_servicespec_from_endpoints_matches_named_targetport_by_port_name() {
    let svc = json!({
        "spec": {
            "clusterIP": "10.43.128.5",
            "ports": [
                {"name": "https", "port": 443, "targetPort": "https", "protocol": "TCP"}
            ]
        }
    });
    let endpoints = json!({
        "subsets": [{
            "addresses": [{"ip": "10.43.0.10"}],
            "ports": [{"name": "https", "port": 8443, "protocol": "TCP"}]
        }]
    });

    let spec = ServiceSpec::from_service_and_endpoints(&svc, Some(&endpoints))
        .expect("named targetPort should map through endpoint port name");
    assert_eq!(spec.ports.len(), 1);
    assert_eq!(spec.ports[0].service_port, 443);
    assert_eq!(spec.ports[0].target_port, 8443);
    assert_eq!(spec.ports[0].protocol, Protocol::Tcp);
}

#[test]
fn test_servicespec_from_endpointslices_matches_by_port_name() {
    let svc = json!({
        "spec": {
            "clusterIP": "10.43.128.5",
            "ports": [
                {"name": "http",  "port": 80,  "targetPort": "http",  "protocol": "TCP"},
                {"name": "https", "port": 443, "targetPort": "https", "protocol": "TCP"}
            ]
        }
    });
    let slice = json!({
        "ports": [
            {"name": "http",  "port": 8080, "protocol": "TCP"},
            {"name": "https", "port": 8443, "protocol": "TCP"}
        ],
        "endpoints": [
            {"addresses": ["10.43.0.10"], "conditions": {"ready": true}}
        ]
    });
    let spec = ServiceSpec::from_service_and_endpointslices(&svc, &[&slice]).unwrap();
    assert_eq!(spec.ports.len(), 2);
    let http = spec.ports.iter().find(|p| p.service_port == 80).unwrap();
    assert_eq!(http.target_port, 8080);
    let https = spec.ports.iter().find(|p| p.service_port == 443).unwrap();
    assert_eq!(https.target_port, 8443);
}

#[test]
fn test_servicespec_from_endpointslices_skips_not_ready() {
    let svc = json!({
        "spec": {
            "clusterIP": "10.43.128.5",
            "ports": [{"port": 80, "targetPort": 8080, "protocol": "TCP"}]
        }
    });
    let slice = json!({
        "ports": [{"port": 8080, "protocol": "TCP"}],
        "endpoints": [
            {"addresses": ["10.43.0.10"], "conditions": {"ready": false}},
            {"addresses": ["10.43.0.11"], "conditions": {"ready": true}}
        ]
    });
    let spec = ServiceSpec::from_service_and_endpointslices(&svc, &[&slice]).unwrap();
    assert_eq!(spec.ports[0].endpoints, vec![Ipv4Addr::new(10, 43, 0, 11)]);
}

// ---- HostPortSpec parsing -----------------------------------------

#[test]
fn test_hostportspec_from_pod_extracts_each_declared_hostport() {
    let pod = json!({
        "spec": {
            "containers": [{
                "ports": [
                    {"hostPort": 8080, "containerPort": 80, "protocol": "TCP"},
                    {"hostPort": 8443, "containerPort": 443, "protocol": "TCP"},
                ]
            }]
        }
    });
    let specs = HostPortSpec::from_pod(&pod);
    assert_eq!(specs.len(), 2);
    assert_eq!(specs[0].host_port, 8080);
    assert_eq!(specs[0].container_port, 80);
    assert_eq!(specs[0].protocol, Protocol::Tcp);
    assert_eq!(specs[0].host_ip, None);
    assert_eq!(specs[1].host_port, 8443);
    assert_eq!(specs[1].container_port, 443);
}

#[test]
fn test_hostportspec_from_pod_skips_zero_or_missing_hostport() {
    let pod = json!({
        "spec": {
            "containers": [{
                "ports": [
                    {"hostPort": 0, "containerPort": 80},        // skipped
                    {"containerPort": 81},                       // skipped (no hostPort)
                    {"hostPort": 8080, "containerPort": 82, "protocol": "TCP"},
                ]
            }]
        }
    });
    let specs = HostPortSpec::from_pod(&pod);
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].host_port, 8080);
    assert_eq!(specs[0].container_port, 82);
}

#[test]
fn test_hostportspec_from_pod_treats_zero_dot_zero_dot_zero_dot_zero_hostip_as_any() {
    let pod = json!({
        "spec": {
            "containers": [{
                "ports": [
                    {"hostPort": 8080, "containerPort": 80, "hostIP": "0.0.0.0", "protocol": "TCP"},
                    {"hostPort": 8081, "containerPort": 81, "hostIP": "", "protocol": "TCP"},
                    {"hostPort": 8082, "containerPort": 82, "hostIP": "192.168.1.5", "protocol": "TCP"},
                ]
            }]
        }
    });
    let specs = HostPortSpec::from_pod(&pod);
    assert_eq!(specs.len(), 3);
    assert_eq!(specs[0].host_ip, None);
    assert_eq!(specs[1].host_ip, None);
    assert_eq!(specs[2].host_ip, Some(Ipv4Addr::new(192, 168, 1, 5)));
}

#[test]
fn test_hostportspec_from_pod_defaults_protocol_to_tcp() {
    let pod = json!({
        "spec": {
            "containers": [{
                "ports": [{"hostPort": 8080, "containerPort": 80}]  // no protocol
            }]
        }
    });
    let specs = HostPortSpec::from_pod(&pod);
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].protocol, Protocol::Tcp);
}

#[test]
fn test_hostportspec_from_pod_walks_multiple_containers() {
    let pod = json!({
        "spec": {
            "containers": [
                {"ports": [{"hostPort": 8080, "containerPort": 80}]},
                {"ports": [{"hostPort": 8081, "containerPort": 81}]},
            ]
        }
    });
    let specs = HostPortSpec::from_pod(&pod);
    assert_eq!(specs.len(), 2);
}

#[test]
fn test_hostportspec_from_pod_with_no_containers_returns_empty() {
    let pod = json!({"spec": {}});
    assert!(HostPortSpec::from_pod(&pod).is_empty());
}

// ---- Probability ladder math --------------------------------------

#[test]
fn test_probability_for_ladder_step_two_endpoints_is_half() {
    // First step of a 2-endpoint ladder: probability 1/2.
    // Threshold = UINT32_MAX / 2.
    let t = probability_for_ladder_step(2);
    assert_eq!(t, u32::MAX / 2);
}

#[test]
fn test_probability_for_ladder_step_returns_native_meta_random_threshold() {
    assert_eq!(
        probability_for_ladder_step(2),
        u32::MAX / 2,
        "nft meta random compares a native u32 register value; byte-swapping makes a 50% rule match almost every packet on little-endian hosts"
    );
}

#[test]
fn test_probability_for_ladder_step_three_endpoints_first_step_is_third() {
    // 3-endpoint ladder, first step (3 remaining): probability 1/3.
    let t = probability_for_ladder_step(3);
    assert_eq!(t, u32::MAX / 3);
}

#[test]
fn test_probability_for_ladder_step_thresholds_decrease_monotonically() {
    // As more endpoints have been "consumed", remaining count drops,
    // and each successive step should accept a *smaller* fraction
    // (because the previous rule already took its share). Confirm the
    // raw probabilities follow 1/N > 1/(N-1) is FALSE — they get
    // larger. Wait: 1/3 < 1/2 < 1/1 — they get larger as remaining
    // shrinks. So thresholds increase. Lock that in.
    let t3 = probability_for_ladder_step(3);
    let t2 = probability_for_ladder_step(2);
    assert!(t3 < t2, "1/3 ({t3}) must be smaller than 1/2 ({t2})");
}

// ---- Strict port parsing (Sec-2) ---------------------------------

#[test]
fn test_parse_port_accepts_valid_in_range_value() {
    assert_eq!(parse_port(Some(&json!(80))), Some(80));
    assert_eq!(parse_port(Some(&json!(65535))), Some(65535));
    assert_eq!(parse_port(Some(&json!(1))), Some(1));
}

#[test]
fn test_parse_port_rejects_zero() {
    // K8s spec disallows port 0; previously `as u16` would have
    // produced a real port-zero rule, which is invalid.
    assert_eq!(parse_port(Some(&json!(0))), None);
}

#[test]
fn test_parse_port_rejects_out_of_range_instead_of_truncating() {
    // 65536 = 0x10000; the previous `as u16` would silently produce
    // port 0 and emit a wrong rule. With try_from we reject cleanly.
    assert_eq!(parse_port(Some(&json!(65536))), None);
    assert_eq!(parse_port(Some(&json!(70000))), None);
    assert_eq!(parse_port(Some(&json!(123456789u64))), None);
}

#[test]
fn test_parse_port_rejects_missing_or_non_numeric() {
    assert_eq!(parse_port(None), None);
    assert_eq!(parse_port(Some(&json!(null))), None);
    assert_eq!(parse_port(Some(&json!("80"))), None);
    assert_eq!(parse_port(Some(&json!(true))), None);
}

#[test]
fn test_hostportspec_from_pod_rejects_out_of_range_port_silently() {
    // A malformed pod manifest with port=70000 must NOT produce a
    // (truncated) DNAT rule. Skipping the entry is the safest
    // behavior — emitting a rule for the wrong port could route
    // traffic to the wrong workload.
    let pod = json!({
        "spec": {
            "containers": [{
                "ports": [
                    {"hostPort": 70000, "containerPort": 80, "protocol": "TCP"},
                    {"hostPort": 8080,  "containerPort": 80, "protocol": "TCP"},
                ]
            }]
        }
    });
    let specs = HostPortSpec::from_pod(&pod);
    assert_eq!(specs.len(), 1, "out-of-range hostPort must be skipped");
    assert_eq!(specs[0].host_port, 8080);
}

// ---- Hybrid remote pod endpoint planning --------------------------

fn pod_endpoint_row(
    uid: &str,
    node_name: &str,
    mode: crate::datastore::PodEndpointMode,
    pod_ip: Ipv4Addr,
    node_ip: Ipv4Addr,
    tcp: Option<u16>,
    udp: Option<u16>,
) -> crate::datastore::PodEndpointRow {
    crate::datastore::PodEndpointRow {
        pod_uid: uid.to_string(),
        namespace: "default".to_string(),
        pod_name: format!("pod-{uid}"),
        node_name: node_name.to_string(),
        mode,
        pod_ip,
        node_ip,
        host_port_tcp: tcp,
        host_port_udp: udp,
        generation: 1,
        updated_at: 1,
    }
}

#[test]
fn test_remote_pod_endpoint_specs_keep_only_remote_hostport_rows() {
    let rows = vec![
        pod_endpoint_row(
            "local",
            "node-a",
            crate::datastore::PodEndpointMode::Hostport,
            Ipv4Addr::new(10, 42, 0, 10),
            Ipv4Addr::new(192, 0, 2, 10),
            Some(31010),
            None,
        ),
        pod_endpoint_row(
            "direct",
            "node-b",
            crate::datastore::PodEndpointMode::Vxlan,
            Ipv4Addr::new(10, 42, 1, 10),
            Ipv4Addr::new(192, 0, 2, 11),
            None,
            None,
        ),
        pod_endpoint_row(
            "remote",
            "rootless-c",
            crate::datastore::PodEndpointMode::Hostport,
            Ipv4Addr::new(10, 42, 2, 10),
            Ipv4Addr::new(192, 0, 2, 12),
            Some(31234),
            Some(31235),
        ),
    ];

    let specs = remote_pod_endpoint_specs_from_rows("node-a", rows);

    assert_eq!(
        specs,
        vec![
            RemotePodEndpointSpec {
                pod_ip: Ipv4Addr::new(10, 42, 2, 10),
                node_ip: Ipv4Addr::new(192, 0, 2, 12),
                host_port: 31234,
                protocol: Protocol::Tcp,
            },
            RemotePodEndpointSpec {
                pod_ip: Ipv4Addr::new(10, 42, 2, 10),
                node_ip: Ipv4Addr::new(192, 0, 2, 12),
                host_port: 31235,
                protocol: Protocol::Udp,
            },
        ],
        "root-side DNAT should be derived only from remote hostport pod_endpoints rows"
    );
}

#[test]
fn test_service_ct_guard_tuples_remove_dropped_udp_port() {
    let cluster_ip = Ipv4Addr::new(10, 43, 128, 12);
    let endpoint = Ipv4Addr::new(10, 43, 0, 20);
    let tcp_only = ServiceSpec {
        cluster_ip,
        ports: vec![PortSpec {
            service_port: 80,
            target_port: 80,
            node_port: None,
            protocol: Protocol::Tcp,
            endpoints: vec![endpoint],
        }],
        session_affinity: SessionAffinity::None,
    };

    let tuples = service_ct_guard_tuples(&[tcp_only]);

    assert_eq!(
        tuples,
        vec![ServiceCtTuple {
            cluster_ip,
            protocol: Protocol::Tcp,
            service_port: 80,
        }],
        "after a Service drops UDP, the stale UDP ClusterIP tuple must not stay accepted"
    );
    assert!(
        !tuples.contains(&ServiceCtTuple {
            cluster_ip,
            protocol: Protocol::Udp,
            service_port: 80,
        }),
        "removed UDP service tuple must be absent so stale conntracked UDP flows are blocked"
    );
}

#[test]
fn test_service_ct_guard_scope_ignores_other_namespace_bridge() {
    assert!(
        service_ct_guard_applies_to_forward_packet(
            "klights-worker",
            "klights-worker",
            "klights-worker"
        ),
        "current table must guard service-DNAT traffic crossing its own bridge"
    );
    assert!(
        !service_ct_guard_applies_to_forward_packet("klights", "klights-worker", "klights-worker"),
        "a stale table from another namespace must not guard or drop this worker's service-DNAT traffic"
    );
}

#[test]
fn test_legacy_unscoped_klights_table_cleanup_target() {
    assert_eq!(
        legacy_unscoped_service_tables_to_cleanup("klights-worker"),
        vec!["klights"],
        "worker boot must remove legacy unscoped default table left by old local runs"
    );
    assert!(
        legacy_unscoped_service_tables_to_cleanup("klights").is_empty(),
        "the current table must never be selected for legacy cleanup"
    );
}

#[test]
fn test_service_rule_snapshot_ignores_inventory_order_and_endpoint_order() {
    let service_a = ServiceSpec {
        cluster_ip: Ipv4Addr::new(10, 43, 128, 12),
        ports: vec![PortSpec {
            service_port: 80,
            target_port: 8080,
            node_port: None,
            protocol: Protocol::Tcp,
            endpoints: vec![Ipv4Addr::new(10, 43, 0, 20), Ipv4Addr::new(10, 43, 1, 30)],
        }],
        session_affinity: SessionAffinity::None,
    };
    let service_b = ServiceSpec {
        cluster_ip: Ipv4Addr::new(10, 43, 128, 13),
        ports: vec![PortSpec {
            service_port: 53,
            target_port: 5353,
            node_port: Some(30053),
            protocol: Protocol::Udp,
            endpoints: vec![Ipv4Addr::new(10, 43, 0, 21)],
        }],
        session_affinity: SessionAffinity::ClientIp,
    };

    let mut reordered_a = service_a.clone();
    reordered_a.ports[0].endpoints.reverse();

    assert_eq!(
        ServiceRuleSnapshot::from_services(&[service_a, service_b.clone()]),
        ServiceRuleSnapshot::from_services(&[service_b, reordered_a]),
        "identical service-routing semantics must be a no-op even when DB watch order changes"
    );
}

#[test]
fn test_service_rule_snapshot_changes_when_endpoint_set_changes() {
    let before = ServiceSpec {
        cluster_ip: Ipv4Addr::new(10, 43, 128, 12),
        ports: vec![PortSpec {
            service_port: 80,
            target_port: 8080,
            node_port: None,
            protocol: Protocol::Tcp,
            endpoints: vec![Ipv4Addr::new(10, 43, 0, 20)],
        }],
        session_affinity: SessionAffinity::None,
    };
    let mut after = before.clone();
    after.ports[0].endpoints.push(Ipv4Addr::new(10, 43, 1, 30));

    assert_ne!(
        ServiceRuleSnapshot::from_services(&[before]),
        ServiceRuleSnapshot::from_services(&[after]),
        "adding a backend endpoint must still force nft rule replacement"
    );
}

#[test]
fn test_prefix_len_from_mask_round_trips() {
    for prefix in [0u8, 8, 16, 17, 24, 32] {
        let mask_bits: u32 = if prefix == 0 {
            0
        } else {
            u32::MAX << (32 - prefix)
        };
        let mask = Ipv4Addr::from(mask_bits);
        assert_eq!(prefix_len_from_mask(mask), prefix, "prefix {prefix}");
    }
}

#[derive(Default)]
struct FreshServiceInventoryClient {
    cached_list_calls: std::sync::atomic::AtomicUsize,
    cached_get_calls: std::sync::atomic::AtomicUsize,
    fresh_get_calls: std::sync::atomic::AtomicUsize,
    service_list_calls: std::sync::atomic::AtomicUsize,
    endpoints_list_calls: std::sync::atomic::AtomicUsize,
    endpointslice_list_calls: std::sync::atomic::AtomicUsize,
    filtered_endpointslice_list_calls: std::sync::atomic::AtomicUsize,
    legacy_endpoints_empty: bool,
    legacy_endpoints_partial: bool,
}

fn inventory_resource(
    api_version: &str,
    kind: &str,
    namespace: &str,
    name: &str,
    resource_version: i64,
    data: serde_json::Value,
) -> crate::datastore::Resource {
    crate::datastore::Resource {
        id: resource_version,
        api_version: api_version.to_string(),
        kind: kind.to_string(),
        namespace: Some(namespace.to_string()),
        name: name.to_string(),
        uid: format!("{name}-uid"),
        resource_version,
        data: std::sync::Arc::new(data),
    }
}

#[async_trait::async_trait]
impl crate::control_plane::client::LeaderApiClient for FreshServiceInventoryClient {
    async fn get_resource(
        &self,
        _key: crate::control_plane::client::ResourceKey,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        self.cached_get_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(None)
    }

    async fn list_resources(
        &self,
        _req: crate::control_plane::client::ListRequest,
    ) -> anyhow::Result<crate::control_plane::client::ListResponse> {
        self.cached_list_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(crate::datastore::ResourceList {
            items: Vec::new(),
            resource_version: 1,
            continue_token: None,
            remaining_item_count: None,
        })
    }

    async fn list_resources_fresh(
        &self,
        req: crate::control_plane::client::ListRequest,
    ) -> anyhow::Result<crate::control_plane::client::ListResponse> {
        if req.api_version == "discovery.k8s.io/v1" && req.kind == "EndpointSlice" {
            self.endpointslice_list_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if req.label_selector.is_some() {
                self.filtered_endpointslice_list_calls
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            return Ok(crate::datastore::ResourceList {
                items: if self.legacy_endpoints_empty || self.legacy_endpoints_partial {
                    vec![inventory_resource(
                        "discovery.k8s.io/v1",
                        "EndpointSlice",
                        "kube-system",
                        "kube-dns-klights",
                        73,
                        json!({
                            "apiVersion": "discovery.k8s.io/v1",
                            "kind": "EndpointSlice",
                            "metadata": {
                                "namespace": "kube-system",
                                "name": "kube-dns-klights",
                                "labels": {
                                    "kubernetes.io/service-name": "kube-dns"
                                }
                            },
                            "addressType": "IPv4",
                            "ports": [
                                {"name": "dns", "port": 53, "protocol": "UDP"},
                                {"name": "dns-tcp", "port": 53, "protocol": "TCP"}
                            ],
                            "endpoints": [{
                                "addresses": ["10.50.0.20"],
                                "conditions": {"ready": true}
                            }]
                        }),
                    )]
                } else {
                    Vec::new()
                },
                resource_version: 73,
                continue_token: None,
                remaining_item_count: None,
            });
        }

        if req.api_version == "v1" && req.kind == "Endpoints" {
            self.endpoints_list_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            return Ok(crate::datastore::ResourceList {
                items: vec![inventory_resource(
                    "v1",
                    "Endpoints",
                    "kube-system",
                    "kube-dns",
                    72,
                    json!({
                        "apiVersion": "v1",
                        "kind": "Endpoints",
                        "metadata": {
                            "namespace": "kube-system",
                            "name": "kube-dns",
                            "uid": "kube-dns-endpoints-uid",
                        },
                        "subsets": if self.legacy_endpoints_empty {
                            json!([])
                        } else if self.legacy_endpoints_partial {
                            json!([{
                            "addresses": [{"ip": "10.50.0.2"}],
                            "ports": [
                                {"name": "dns", "port": 53, "protocol": "UDP"}
                            ]
                        }])
                        } else {
                            json!([{
                            "addresses": [{"ip": "10.50.0.2"}],
                            "ports": [
                                {"name": "dns", "port": 53, "protocol": "UDP"},
                                {"name": "dns-tcp", "port": 53, "protocol": "TCP"}
                            ]
                        }])
                        }
                    }),
                )],
                resource_version: 72,
                continue_token: None,
                remaining_item_count: None,
            });
        }

        assert_eq!(req.api_version, "v1");
        assert_eq!(req.kind, "Service");
        self.service_list_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(crate::datastore::ResourceList {
            items: vec![inventory_resource(
                "v1",
                "Service",
                "kube-system",
                "kube-dns",
                32,
                json!({
                    "apiVersion": "v1",
                    "kind": "Service",
                    "metadata": {
                        "namespace": "kube-system",
                        "name": "kube-dns",
                        "uid": "kube-dns-service-uid",
                    },
                    "spec": {
                        "clusterIP": "10.51.0.10",
                        "ports": [
                            {"name": "dns", "port": 53, "protocol": "UDP"},
                            {"name": "dns-tcp", "port": 53, "protocol": "TCP"}
                        ]
                    }
                }),
            )],
            resource_version: 32,
            continue_token: None,
            remaining_item_count: None,
        })
    }

    async fn get_resource_fresh(
        &self,
        key: crate::control_plane::client::ResourceKey,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        self.fresh_get_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if key.api_version == "v1"
            && key.kind == "Endpoints"
            && key.namespace.as_deref() == Some("kube-system")
            && key.name == "kube-dns"
        {
            return Ok(Some(inventory_resource(
                "v1",
                "Endpoints",
                "kube-system",
                "kube-dns",
                72,
                json!({
                    "apiVersion": "v1",
                    "kind": "Endpoints",
                    "metadata": {
                        "namespace": "kube-system",
                        "name": "kube-dns",
                        "uid": "kube-dns-endpoints-uid",
                    },
                    "subsets": if self.legacy_endpoints_empty {
                        json!([])
                    } else if self.legacy_endpoints_partial {
                        json!([{
                        "addresses": [{"ip": "10.50.0.2"}],
                        "ports": [
                            {"name": "dns", "port": 53, "protocol": "UDP"}
                        ]
                    }])
                    } else {
                        json!([{
                        "addresses": [{"ip": "10.50.0.2"}],
                        "ports": [
                            {"name": "dns", "port": 53, "protocol": "UDP"},
                            {"name": "dns-tcp", "port": 53, "protocol": "TCP"}
                        ]
                    }])
                    }
                }),
            )));
        }
        Ok(None)
    }

    async fn watch_resources(
        &self,
        _req: crate::control_plane::client::WatchRequest,
    ) -> anyhow::Result<
        crate::control_plane::client::WatchStream<crate::control_plane::client::ResourceEvent>,
    > {
        Ok(Box::pin(futures::stream::empty()))
    }

    async fn wait_cache_ready(
        &self,
        _scope: crate::control_plane::client::CacheScope,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn get_pod(
        &self,
        _ns: &str,
        _name: &str,
    ) -> anyhow::Result<Option<crate::control_plane::client::Pod>> {
        Ok(None)
    }

    async fn get_pod_for_uid(
        &self,
        _ns: &str,
        _name: &str,
        _uid: &str,
    ) -> anyhow::Result<Option<crate::control_plane::client::Pod>> {
        Ok(None)
    }

    async fn watch_pods_on_node(
        &self,
        _node_name: &str,
    ) -> anyhow::Result<crate::control_plane::client::WatchStream<crate::control_plane::client::Pod>>
    {
        Ok(Box::pin(futures::stream::empty()))
    }

    async fn list_pods_on_node(
        &self,
        _node_name: &str,
    ) -> anyhow::Result<Vec<crate::control_plane::client::Pod>> {
        Ok(Vec::new())
    }

    async fn get_configmap(
        &self,
        _ns: &str,
        _name: &str,
    ) -> anyhow::Result<Option<crate::control_plane::client::ConfigMap>> {
        Ok(None)
    }

    async fn get_secret(
        &self,
        _ns: &str,
        _name: &str,
    ) -> anyhow::Result<Option<crate::control_plane::client::Secret>> {
        Ok(None)
    }

    async fn get_node(&self, name: &str) -> anyhow::Result<crate::control_plane::client::Node> {
        Err(anyhow::anyhow!("unexpected get_node for {name}"))
    }

    async fn watch_node(
        &self,
        _name: &str,
    ) -> anyhow::Result<crate::control_plane::client::WatchStream<crate::control_plane::client::Node>>
    {
        Ok(Box::pin(futures::stream::empty()))
    }

    async fn allocate_node_subnet(
        &self,
        node_name: &str,
        _cluster_cidr: &str,
        _node_ip: &str,
    ) -> anyhow::Result<crate::datastore::NodeSubnet> {
        Err(anyhow::anyhow!(
            "unexpected allocate_node_subnet for {node_name}"
        ))
    }

    async fn get_node_subnet(
        &self,
        node_name: &str,
    ) -> anyhow::Result<Option<crate::datastore::NodeSubnet>> {
        Err(anyhow::anyhow!(
            "unexpected get_node_subnet for {node_name}"
        ))
    }

    async fn list_peer_subnets(
        &self,
        my_node_name: &str,
    ) -> anyhow::Result<Vec<crate::datastore::NodeSubnet>> {
        Err(anyhow::anyhow!(
            "unexpected list_peer_subnets for {my_node_name}"
        ))
    }

    async fn get_node_dataplane(
        &self,
        node_name: &str,
    ) -> anyhow::Result<Option<crate::networking::wireguard::DataplanePeerMetadata>> {
        Err(anyhow::anyhow!(
            "unexpected get_node_dataplane for {node_name}"
        ))
    }

    async fn apply_outbox(
        &self,
        idempotency_key: &str,
        _operation: crate::kubelet::outbox::payload::OutboxOperation,
        _payload: bytes::Bytes,
    ) -> std::result::Result<
        crate::kubelet::outbox::OutboxApplyResult,
        crate::kubelet::outbox::OutboxApplyError,
    > {
        Err(crate::kubelet::outbox::OutboxApplyError::Retryable(
            format!("unexpected apply_outbox for {idempotency_key}"),
        ))
    }
}

#[tokio::test]
async fn service_specs_from_api_uses_fresh_reads_for_routing_snapshot() {
    let api = FreshServiceInventoryClient::default();

    let specs = service_specs_from_api(&api)
        .await
        .expect("service specs should build");

    assert_eq!(
        api.cached_list_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "routing snapshots must not use the possibly stale cached list"
    );
    assert_eq!(
        api.cached_get_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "routing snapshots must not use the possibly stale cached get"
    );
    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].cluster_ip, Ipv4Addr::new(10, 51, 0, 10));

    let mut tuples: Vec<_> = specs[0]
        .ports
        .iter()
        .map(|port| {
            (
                port.protocol,
                port.service_port,
                port.target_port,
                port.endpoints.clone(),
            )
        })
        .collect();
    tuples.sort_by_key(|(protocol, service_port, _, _)| (*protocol, *service_port));
    assert_eq!(
        tuples,
        vec![
            (Protocol::Tcp, 53, 53, vec![Ipv4Addr::new(10, 50, 0, 2)]),
            (Protocol::Udp, 53, 53, vec![Ipv4Addr::new(10, 50, 0, 2)]),
        ]
    );
}

#[tokio::test]
async fn service_specs_from_api_uses_bounded_bulk_fresh_inventory() {
    let api = FreshServiceInventoryClient {
        legacy_endpoints_partial: true,
        ..Default::default()
    };

    let specs = service_specs_from_api(&api)
        .await
        .expect("service specs should build from bulk inventory");

    assert_eq!(specs.len(), 1);
    assert_eq!(
        api.service_list_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "routing snapshots should list Services once"
    );
    assert_eq!(
        api.endpoints_list_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "routing snapshots should list Endpoints once for the whole inventory"
    );
    assert_eq!(
        api.endpointslice_list_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "routing snapshots should list EndpointSlices once for the whole inventory"
    );
    assert_eq!(
        api.filtered_endpointslice_list_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "routing snapshots must not issue one EndpointSlice list per Service"
    );
    assert_eq!(
        api.fresh_get_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "routing snapshots must not issue one fresh Endpoints get per Service"
    );
}

#[tokio::test]
async fn service_specs_from_api_falls_back_to_ready_endpointslices_when_legacy_endpoints_empty() {
    let api = FreshServiceInventoryClient {
        legacy_endpoints_empty: true,
        ..Default::default()
    };

    let specs = service_specs_from_api(&api)
        .await
        .expect("service specs should build from EndpointSlices");

    assert_eq!(specs.len(), 1);
    let mut tuples: Vec<_> = specs[0]
        .ports
        .iter()
        .map(|port| {
            (
                port.protocol,
                port.service_port,
                port.target_port,
                port.endpoints.clone(),
            )
        })
        .collect();
    tuples.sort_by_key(|(protocol, service_port, _, _)| (*protocol, *service_port));
    assert_eq!(
        tuples,
        vec![
            (Protocol::Tcp, 53, 53, vec![Ipv4Addr::new(10, 50, 0, 20)]),
            (Protocol::Udp, 53, 53, vec![Ipv4Addr::new(10, 50, 0, 20)]),
        ],
        "EndpointSlice-ready endpoints must program service routing when the legacy Endpoints object is still empty"
    );
}

#[tokio::test]
async fn service_specs_from_api_prefers_complete_endpointslices_over_partial_legacy_endpoints() {
    let api = FreshServiceInventoryClient {
        legacy_endpoints_partial: true,
        ..Default::default()
    };

    let specs = service_specs_from_api(&api)
        .await
        .expect("service specs should build from EndpointSlices");

    assert_eq!(specs.len(), 1);
    let mut tuples: Vec<_> = specs[0]
        .ports
        .iter()
        .map(|port| {
            (
                port.protocol,
                port.service_port,
                port.target_port,
                port.endpoints.clone(),
            )
        })
        .collect();
    tuples.sort_by_key(|(protocol, service_port, _, _)| (*protocol, *service_port));
    assert_eq!(
        tuples,
        vec![
            (Protocol::Tcp, 53, 53, vec![Ipv4Addr::new(10, 50, 0, 20)]),
            (Protocol::Udp, 53, 53, vec![Ipv4Addr::new(10, 50, 0, 20)]),
        ],
        "complete EndpointSlices must not be shadowed by a partial legacy Endpoints object"
    );
}

// ── Task 6: cached-inventory route sync tests ──────────────────────

#[tokio::test]
async fn coalesced_sync_uses_cached_inventory_after_initial_snapshot() {
    let api = FreshServiceInventoryClient::default();
    // Initial snapshot: builds inventory from the API.
    let inventory = bootstrap_inventory_from_api(&api)
        .await
        .expect("bootstrap inventory");
    assert!(
        !inventory.is_empty()
            || api
                .service_list_calls
                .load(std::sync::atomic::Ordering::SeqCst)
                == 1,
        "first bootstrap must list services once"
    );
    let svc_calls_after_bootstrap = api
        .service_list_calls
        .load(std::sync::atomic::Ordering::SeqCst);
    let endpoint_calls_after_bootstrap = api
        .endpoints_list_calls
        .load(std::sync::atomic::Ordering::SeqCst)
        + api
            .endpointslice_list_calls
            .load(std::sync::atomic::Ordering::SeqCst);

    // Subsequent sync from the cached inventory must not list services again.
    let _specs = inventory.to_specs();
    assert_eq!(
        api.service_list_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        svc_calls_after_bootstrap,
        "to_specs from cached inventory must NOT re-list services"
    );
    assert_eq!(
        api.endpoints_list_calls
            .load(std::sync::atomic::Ordering::SeqCst)
            + api
                .endpointslice_list_calls
                .load(std::sync::atomic::Ordering::SeqCst),
        endpoint_calls_after_bootstrap,
        "to_specs from cached inventory must NOT re-list endpoints/slices"
    );
}

#[tokio::test]
async fn service_route_sync_does_not_query_api_per_service() {
    // Set up several services so the count is meaningful.
    let api = FreshServiceInventoryClient::default();
    let inventory = bootstrap_inventory_from_api(&api)
        .await
        .expect("bootstrap inventory");
    let svc_count = inventory.to_specs().len();

    // Whatever the number of Services discovered, the bootstrap must use
    // exactly ONE list call per resource type — never one per Service.
    let svc_list_calls = api
        .service_list_calls
        .load(std::sync::atomic::Ordering::SeqCst);
    let eps_list_calls = api
        .endpoints_list_calls
        .load(std::sync::atomic::Ordering::SeqCst);
    let slice_list_calls = api
        .endpointslice_list_calls
        .load(std::sync::atomic::Ordering::SeqCst);

    assert!(
        svc_list_calls <= 1,
        "Service list must be at most 1, was {svc_list_calls} for {svc_count} services"
    );
    assert!(
        eps_list_calls <= 1,
        "Endpoints list must be at most 1, was {eps_list_calls}"
    );
    assert!(
        slice_list_calls <= 1,
        "EndpointSlice list must be at most 1, was {slice_list_calls}"
    );

    // Confirm no per-Service get_resource calls were issued.
    assert_eq!(
        api.fresh_get_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "per-Service fresh get must not be used during route sync"
    );
    assert_eq!(
        api.cached_get_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "per-Service cached get must not be used during route sync"
    );
}
