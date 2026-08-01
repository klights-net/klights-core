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
