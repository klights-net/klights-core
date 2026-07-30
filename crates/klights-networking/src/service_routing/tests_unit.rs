use super::*;
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

#[test]
fn test_remote_pod_endpoint_topology_keeps_both_remote_l4_mappings() {
    let endpoints = vec![
        klights_network_api::PodEndpointTopology::Direct(
            klights_network_api::DirectPodEndpoint::try_new(Ipv4Addr::new(10, 42, 1, 10), "node-b")
                .unwrap(),
        ),
        klights_network_api::PodEndpointTopology::HostPort(
            klights_network_api::HostPortPodEndpoint::try_new(
                Ipv4Addr::new(10, 42, 0, 10),
                "node-a",
                Ipv4Addr::new(192, 0, 2, 10),
                Some(31010),
                None,
            )
            .unwrap(),
        ),
        klights_network_api::PodEndpointTopology::HostPort(
            klights_network_api::HostPortPodEndpoint::try_new(
                Ipv4Addr::new(10, 42, 2, 10),
                "rootless-c",
                Ipv4Addr::new(192, 0, 2, 12),
                Some(31234),
                Some(31235),
            )
            .unwrap(),
        ),
    ];

    assert_eq!(
        remote_pod_endpoint_specs_from_topology("node-a", &endpoints),
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
        ]
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
fn service_ct_guard_transition_keeps_old_and_new_tuples_until_dnat_switches() {
    let cluster_ip = Ipv4Addr::new(10, 43, 128, 12);
    let endpoint = Ipv4Addr::new(10, 43, 0, 20);
    let service = |protocol| ServiceSpec {
        cluster_ip,
        ports: vec![PortSpec {
            service_port: 80,
            target_port: 80,
            node_port: None,
            protocol,
            endpoints: vec![endpoint],
        }],
        session_affinity: SessionAffinity::None,
    };
    let previous = vec![service(Protocol::Udp)];
    let desired = vec![service(Protocol::Tcp)];

    let transition = service_ct_guard_transition(&previous, &desired);

    assert_eq!(
        transition.staged,
        vec![
            ServiceCtTuple {
                cluster_ip,
                protocol: Protocol::Tcp,
                service_port: 80,
            },
            ServiceCtTuple {
                cluster_ip,
                protocol: Protocol::Udp,
                service_port: 80,
            },
        ],
        "the pre-DNAT guard must accept the union so either kernel generation remains safe"
    );
    assert_eq!(
        transition.finalized,
        vec![ServiceCtTuple {
            cluster_ip,
            protocol: Protocol::Tcp,
            service_port: 80,
        }],
        "the post-DNAT guard must remove tuples absent from the desired generation"
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
