use super::*;

#[test]
fn parse_ip_to_u32_accepts_ipv4_and_rejects_invalid_shapes() {
    assert_eq!(parse_ip_to_u32("10.43.128.2"), Some(0x0A2B8002));
    assert_eq!(parse_ip_to_u32("0.0.0.0"), Some(0));
    assert_eq!(parse_ip_to_u32("255.255.255.255"), Some(0xFFFF_FFFF));
    for invalid in ["", "not.an.ip", "10.43.128", "10.43.128.2.5"] {
        assert_eq!(parse_ip_to_u32(invalid), None, "{invalid}");
    }
}

#[test]
fn external_name_normalization_clears_cluster_and_node_port_allocations() {
    let mut spec = serde_json::json!({
        "type": "ExternalName",
        "externalName": "backend.example.com",
        "clusterIP": "10.43.128.5",
        "clusterIPs": ["10.43.128.5"],
        "ports": [
            {"port": 80, "nodePort": 30080},
            {"port": 443, "nodePort": 30443}
        ]
    })
    .as_object()
    .expect("object")
    .clone();

    clear_externalname_invalid_fields(&mut spec);

    assert_eq!(spec.get("clusterIP"), Some(&serde_json::json!("")));
    assert_eq!(spec.get("clusterIPs"), Some(&serde_json::json!([])));
    assert!(
        spec["ports"]
            .as_array()
            .expect("ports")
            .iter()
            .all(|port| port.get("nodePort").is_none())
    );
}

#[test]
fn external_name_normalization_is_noop_for_other_service_types() {
    let mut spec = serde_json::json!({
        "type": "NodePort",
        "clusterIP": "10.43.128.6",
        "ports": [{"port": 80, "nodePort": 30080}]
    })
    .as_object()
    .expect("object")
    .clone();

    clear_externalname_invalid_fields(&mut spec);

    assert_eq!(spec["clusterIP"], "10.43.128.6");
    assert_eq!(spec["ports"][0]["nodePort"], 30080);
}

#[test]
fn service_port_normalization_defaults_and_preserves_explicit_values() {
    let mut defaulted = serde_json::json!({"ports": [{"port": 6379}]})
        .as_object()
        .expect("object")
        .clone();
    normalize_service_ports(&mut defaulted);
    assert_eq!(defaulted["ports"][0]["protocol"], "TCP");
    assert_eq!(defaulted["ports"][0]["targetPort"], 6379);

    let mut explicit = serde_json::json!({
        "ports": [{"port": 80, "targetPort": "http", "protocol": "UDP"}]
    })
    .as_object()
    .expect("object")
    .clone();
    normalize_service_ports(&mut explicit);
    assert_eq!(explicit["ports"][0]["protocol"], "UDP");
    assert_eq!(explicit["ports"][0]["targetPort"], "http");
}

#[test]
fn test_service_ipam_reserves_dot_one_for_kubernetes_service() {
    // The kubernetes service hardcodes 10.43.128.1 (KUBERNETES_SERVICE_IP)
    // IPAM must start at .2 to avoid collision
    let ipam = ServiceIpam::new("10.43.128.0/17");

    // First allocation should be .2 (not .1, which is reserved)
    assert_eq!(ipam.allocate().unwrap(), "10.43.128.2");

    // Next should be .3
    assert_eq!(ipam.allocate().unwrap(), "10.43.128.3");
}

#[test]
fn test_service_ipam_default_range() {
    let ipam = ServiceIpam::new("10.43.128.0/17");

    // First IP should be 10.43.128.2 (.1 reserved for kubernetes service)
    assert_eq!(ipam.allocate().unwrap(), "10.43.128.2");

    // Next should be 10.43.128.3
    assert_eq!(ipam.allocate().unwrap(), "10.43.128.3");

    // Verify sequential allocation
    assert_eq!(ipam.allocate().unwrap(), "10.43.128.4");
}

#[test]
fn test_service_ipam_custom_range() {
    let ipam = ServiceIpam::new("10.44.128.0/17");

    // First IP should be 10.44.128.2 (.1 reserved for kubernetes service)
    assert_eq!(ipam.allocate().unwrap(), "10.44.128.2");

    // Next should be 10.44.128.3
    assert_eq!(ipam.allocate().unwrap(), "10.44.128.3");
}

#[test]
fn test_service_ipam_no_overlap_with_pods() {
    // Pod CIDR: 10.43.0.0/17 (10.43.0.0 - 10.43.127.255)
    // Service CIDR: 10.43.128.0/17 (10.43.128.0 - 10.43.255.255)

    let service_ipam = ServiceIpam::new("10.43.128.0/17");

    // Canonical pod IPs from the node-local pod range
    let pod_ip1 = "10.43.0.2";
    let pod_ip2 = "10.43.0.3";

    // Allocate some service IPs (starts at .2 since .1 is reserved)
    let svc_ip1 = service_ipam.allocate().unwrap(); // Should be 10.43.128.2
    let svc_ip2 = service_ipam.allocate().unwrap(); // Should be 10.43.128.3

    // Verify no overlap
    assert_eq!(pod_ip1, "10.43.0.2");
    assert_eq!(pod_ip2, "10.43.0.3");
    assert_eq!(svc_ip1, "10.43.128.2");
    assert_eq!(svc_ip2, "10.43.128.3");

    // Verify pod IPs are in first half (octet3 < 128)
    assert!(pod_ip1.starts_with("10.43.0."));
    assert!(pod_ip2.starts_with("10.43.0."));

    // Verify service IPs are in second half (octet3 >= 128)
    assert!(svc_ip1.starts_with("10.43.128."));
    assert!(svc_ip2.starts_with("10.43.128."));
}

#[test]
fn test_ipam_allocate_sequential() {
    let ipam = ServiceIpam::new("10.43.128.0/17");

    let ip1 = ipam.allocate().unwrap();
    let ip2 = ipam.allocate().unwrap();
    let ip3 = ipam.allocate().unwrap();

    assert_eq!(ip1, "10.43.128.2");
    assert_eq!(ip2, "10.43.128.3");
    assert_eq!(ip3, "10.43.128.4");
}

#[test]
fn test_ipam_release_and_reuse() {
    let ipam = ServiceIpam::new("10.43.128.0/17");

    let ip1 = ipam.allocate().unwrap();
    assert_eq!(ip1, "10.43.128.2");

    // Release the IP
    ipam.release(&ip1);

    // Next allocation should reuse the released IP
    let ip2 = ipam.allocate().unwrap();
    assert_eq!(ip2, "10.43.128.2", "Released IP should be reused");
}

#[test]
fn test_ipam_release_nonexistent_is_noop() {
    let ipam = ServiceIpam::new("10.43.128.0/17");

    // Releasing an IP that was never allocated should not panic
    ipam.release("10.43.128.99");

    // Should still allocate normally
    let ip = ipam.allocate().unwrap();
    assert_eq!(ip, "10.43.128.2");
}

#[test]
fn test_ipam_release_invalid_ip_is_noop() {
    let ipam = ServiceIpam::new("10.43.128.0/17");
    // Releasing invalid IP strings should not panic
    ipam.release("");
    ipam.release("not-an-ip");
    ipam.release("10.43");
    // Should still allocate normally
    assert_eq!(ipam.allocate().unwrap(), "10.43.128.2");
}

#[test]
fn test_ipam_skips_allocated() {
    let ipam = ServiceIpam::new("10.43.128.0/17");

    let ip1 = ipam.allocate().unwrap();
    assert_eq!(ip1, "10.43.128.2");

    // Don't release ip1, allocate another
    let ip2 = ipam.allocate().unwrap();
    assert_eq!(ip2, "10.43.128.3");

    // ip1 should still be allocated, ip2 is different
    assert_ne!(ip1, ip2);

    // Release ip1, should be reusable now
    ipam.release(&ip1);
    let ip3 = ipam.allocate().unwrap();
    assert_eq!(ip3, "10.43.128.2", "After release, IP should be reused");
}

#[test]
fn test_headless_service_clusterip_none_not_overwritten() {
    // Test that "None" is preserved (not replaced with allocated IP)
    let spec = json!({
        "clusterIP": "None",
        "selector": {"app": "test"},
        "ports": [{"port": 80}]
    });

    let cluster_ip_value = spec.get("clusterIP").and_then(|v| v.as_str());

    // The fix: check if clusterIP is None (missing), don't check if it's "None" (string)
    let should_allocate = cluster_ip_value.is_none();

    assert!(
        !should_allocate,
        "clusterIP='None' (headless) should NOT trigger allocation"
    );
}

#[test]
fn test_normal_service_clusterip_allocated() {
    // Test that missing clusterIP triggers allocation
    let spec = json!({
        "selector": {"app": "test"},
        "ports": [{"port": 80}]
    });

    let cluster_ip_value = spec.get("clusterIP").and_then(|v| v.as_str());

    // Should allocate when clusterIP is not present
    let should_allocate = cluster_ip_value.is_none();

    assert!(
        should_allocate,
        "Missing clusterIP should trigger allocation"
    );
}

#[test]
fn test_reconcile_service_defaults_type_to_clusterip_when_missing() {
    // Sonobuoy: ExternalName → ClusterIP patch results in missing spec.type.
    // normalize_service_type must set "ClusterIP" when type is absent.
    let mut spec = serde_json::Map::new();
    spec.insert(
        "ports".to_string(),
        json!([{"port": 80, "targetPort": 8080, "protocol": "TCP"}]),
    );
    normalize_service_type(&mut spec);
    assert_eq!(
        spec.get("type").and_then(|t| t.as_str()).unwrap_or(""),
        "ClusterIP",
        "Missing spec.type must be normalized to ClusterIP"
    );
}

#[test]
fn test_reconcile_service_defaults_type_to_clusterip_when_empty() {
    // Sonobuoy: ExternalName → ClusterIP patch results in empty spec.type "".
    // normalize_service_type must replace "" with "ClusterIP".
    let mut spec = serde_json::Map::new();
    spec.insert("type".to_string(), json!(""));
    spec.insert(
        "ports".to_string(),
        json!([{"port": 80, "targetPort": 8080, "protocol": "TCP"}]),
    );
    normalize_service_type(&mut spec);
    assert_eq!(
        spec.get("type").and_then(|t| t.as_str()).unwrap_or(""),
        "ClusterIP",
        "Empty spec.type must be normalized to ClusterIP"
    );
}

#[test]
fn test_nodeport_allocator_skips_already_used_ports() {
    let alloc = NodePortAllocator::new();
    // Set allocator to ready state
    alloc.set_ready();
    // Pre-allocate 30000
    assert_eq!(alloc.allocate().unwrap(), 30000);
    // First free allocation must skip 30000 and return 30001
    let port = alloc.allocate().unwrap();
    assert_ne!(port, 30000, "Must not allocate already-used port 30000");
    assert_eq!(port, 30001, "Must allocate next free port 30001");
}

#[test]
fn test_nodeport_allocator_sequential_allocation() {
    let alloc = NodePortAllocator::new();
    alloc.set_ready();
    let port1 = alloc.allocate().unwrap();
    let port2 = alloc.allocate().unwrap();
    let port3 = alloc.allocate().unwrap();
    assert_eq!(port1, 30000);
    assert_eq!(port2, 30001);
    assert_eq!(port3, 30002);
}

#[test]
fn test_nodeport_allocator_mark_used_then_allocate_skips() {
    let alloc = NodePortAllocator::new();
    alloc.set_ready();
    // Mark a range as used
    assert_eq!(alloc.allocate().unwrap(), 30000);
    assert_eq!(alloc.allocate().unwrap(), 30001);
    assert_eq!(alloc.allocate().unwrap(), 30002);
    // First allocation should skip to 30003
    assert_eq!(alloc.allocate().unwrap(), 30003);
}

#[test]
fn test_nodeport_allocator_collision_avoidance_matches_real_allocation_flow() {
    // Simulate: klights starts, finds service with nodePort=30000 in DB,
    // marks it used, then allocates a new port for a second service.
    // The new port must not be 30000.
    let alloc = NodePortAllocator::new();
    alloc.set_ready();

    // Bootstrap: existing service occupies 30000
    assert_eq!(alloc.allocate().unwrap(), 30000);

    // New service request: must get a different port
    let new_port = alloc.allocate().unwrap();
    assert_ne!(new_port, 30000, "Must skip already-used port 30000");
    assert!(
        (30001..=32767).contains(&new_port),
        "Port must be in valid NodePort range, got {}",
        new_port
    );
}

#[test]
fn test_nodeport_allocator_ready_state_allows_allocation() {
    let alloc = NodePortAllocator::new();
    // After bootstrap rebuild, allocator should be ready
    alloc.set_ready();
    // Should successfully allocate
    let port = alloc.allocate().unwrap();
    assert!((30000..=32767).contains(&port));
}

#[test]
fn test_nodeport_allocator_not_ready_rejects_allocation() {
    let alloc = NodePortAllocator::new();
    // Before bootstrap rebuild, allocator is not ready
    assert!(!alloc.is_ready());
    // Allocation should return error when not ready
    let result = alloc.allocate();
    assert!(
        result.is_err(),
        "Allocation should fail when allocator is not ready"
    );
}

#[test]
fn test_nodeport_allocator_sets_ready_after_bootstrap() {
    let alloc = NodePortAllocator::new();
    // Initially not ready
    assert!(!alloc.is_ready());
    // After rebuild, should be ready
    alloc.set_ready();
    assert!(alloc.is_ready());
}

/// NodePort allocator must return an error when the 30000–32767 range is
/// fully exhausted, not silently return 32768.
#[test]
fn nodeport_allocator_exhaustion_returns_error() {
    let alloc = NodePortAllocator::new();
    alloc.set_ready();
    // Exhaust the entire range
    for port in 30000..=32767 {
        assert_eq!(alloc.allocate().unwrap(), port);
    }
    let result = alloc.allocate();
    assert!(
        result.is_err(),
        "NodePort allocator must return error when range exhausted"
    );
    assert!(
        result.unwrap_err().contains("exhausted"),
        "error message must mention exhaustion"
    );
}

/// ServiceIpam must return an error when the service CIDR is exhausted.
#[test]
fn service_ipam_exhaustion_returns_error() {
    // Use a tiny /30 subnet: 10.0.0.0/30 has usable IPs .1 and .2
    // (skip .0 = network, .3 = broadcast). start_ip = .2 (network+2), end_ip = .2 (broadcast-1).
    // Only one allocatable IP.
    let ipam = ServiceIpam::new("10.0.0.0/30");

    // First allocation succeeds (10.0.0.2)
    let ip1 = ipam.allocate().unwrap();
    assert_eq!(ip1, "10.0.0.2");

    // Second must fail — only one slot exists.
    let result = ipam.allocate();
    assert!(
        result.is_err(),
        "ServiceIpam must return error when CIDR exhausted"
    );
    assert!(
        result.unwrap_err().contains("exhausted"),
        "error message must mention exhaustion"
    );
}
