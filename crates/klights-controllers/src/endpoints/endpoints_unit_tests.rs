use super::*;

#[test]
fn target_port_resolution_preserves_kubernetes_int_or_string_rules() {
    let cases = [
        (serde_json::json!({"port": 80, "targetPort": 0}), Some(80)),
        (serde_json::json!({"port": 81}), Some(81)),
        (
            serde_json::json!({"port": 80, "targetPort": 8080}),
            Some(8080),
        ),
        (
            serde_json::json!({
                "port": 80,
                "targetPort": {"type": 1, "strVal": "8081"}
            }),
            Some(8081),
        ),
    ];
    for (service_port, expected) in cases {
        assert_eq!(resolve_target_port(&service_port, &[]), expected);
    }

    let service_port = serde_json::json!({
        "port": 80,
        "targetPort": {"type": 1, "strVal": "http"}
    });
    let pod = serde_json::json!({
        "spec": {"containers": [{"ports": [{"name": "http", "containerPort": 8082}]}]}
    });
    assert_eq!(resolve_target_port(&service_port, &[&pod]), Some(8082));
}

#[test]
fn endpoint_desired_state_comparison_ignores_metadata() {
    let current = serde_json::json!({
        "metadata": {"resourceVersion": "11"},
        "subsets": [{"addresses": [{"ip": "10.42.0.2"}]}]
    });
    let desired = serde_json::json!({
        "metadata": {"resourceVersion": "12"},
        "subsets": [{"addresses": [{"ip": "10.42.0.2"}]}]
    });
    assert!(endpoints_desired_state_matches(&current, &desired));
}

#[test]
fn endpoint_slice_desired_state_detects_endpoint_changes() {
    let current = serde_json::json!({
        "addressType": "IPv4",
        "ports": [{"port": 80}],
        "endpoints": [{"addresses": ["10.42.0.2"]}]
    });
    let desired = serde_json::json!({
        "addressType": "IPv4",
        "ports": [{"port": 80}],
        "endpoints": [{"addresses": ["10.42.0.3"]}]
    });
    assert!(!endpointslice_desired_state_matches(&current, &desired));
}
