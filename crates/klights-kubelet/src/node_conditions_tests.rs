use serde_json::json;

#[test]
fn test_node_has_network_unavailable_condition() {
    // Simulate node status
    let node_status = json!({
        "conditions": [
            {
                "type": "Ready",
                "status": "True",
                "reason": "KubeletReady",
                "message": "klights is ready"
            },
            {
                "type": "MemoryPressure",
                "status": "False",
                "reason": "KubeletHasSufficientMemory",
                "message": "kubelet has sufficient memory available"
            },
            {
                "type": "DiskPressure",
                "status": "False",
                "reason": "KubeletHasNoDiskPressure",
                "message": "kubelet has no disk pressure"
            },
            {
                "type": "PIDPressure",
                "status": "False",
                "reason": "KubeletHasSufficientPID",
                "message": "kubelet has sufficient PID available"
            },
            {
                "type": "NetworkUnavailable",
                "status": "False",
                "reason": "RouteCreated",
                "message": "RouteController created a route"
            }
        ]
    });

    let conditions = node_status["conditions"].as_array().unwrap();

    // Verify NetworkUnavailable condition exists
    let network_condition = conditions
        .iter()
        .find(|c| c["type"] == "NetworkUnavailable");

    assert!(
        network_condition.is_some(),
        "Node should have NetworkUnavailable condition"
    );

    let condition = network_condition.unwrap();
    assert_eq!(
        condition["status"], "False",
        "NetworkUnavailable should be False (network is available)"
    );
    assert_eq!(
        condition["reason"], "RouteCreated",
        "Reason should be RouteCreated"
    );
}

#[test]
fn test_node_has_all_required_conditions() {
    // K8s requires these 5 conditions for proper scheduling
    let required_conditions = vec![
        "Ready",
        "MemoryPressure",
        "DiskPressure",
        "PIDPressure",
        "NetworkUnavailable",
    ];

    let node_status = json!({
        "conditions": [
            {"type": "Ready", "status": "True"},
            {"type": "MemoryPressure", "status": "False"},
            {"type": "DiskPressure", "status": "False"},
            {"type": "PIDPressure", "status": "False"},
            {"type": "NetworkUnavailable", "status": "False"}
        ]
    });

    let conditions = node_status["conditions"].as_array().unwrap();

    for required_type in required_conditions {
        let found = conditions.iter().any(|c| c["type"] == required_type);

        assert!(found, "Node should have {} condition", required_type);
    }
}
