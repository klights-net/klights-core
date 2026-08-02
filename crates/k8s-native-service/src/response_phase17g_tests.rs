use super::*;
use crate::watch::{EventType, WatchEvent};
use axum::http::HeaderMap;
use serde_json::{Value, json};

fn operation_time() -> time::OffsetDateTime {
    time::OffsetDateTime::parse(
        "2026-08-02T00:00:00Z",
        &time::format_description::well_known::Rfc3339,
    )
    .expect("fixed response operation time")
}

fn pod_list_to_table(items: Vec<Value>, resource_version: String) -> Value {
    pod_list_to_table_at(items, resource_version, operation_time())
}

fn node_list_to_table(items: Vec<Value>, resource_version: String) -> Value {
    node_list_to_table_at(items, resource_version, operation_time())
}

fn watch_event_to_table(event: WatchEvent, kind: &str) -> WatchEvent {
    watch_event_to_table_at(event, kind, operation_time())
}

#[test]
fn test_pod_list_to_table_ready_count_uses_spec_containers() {
    // Bug: READY column shows "0/0" for Pending pods
    // Root cause: total_containers uses len(status.containerStatuses) instead of len(spec.containers)
    // Expected: should show "0/2" for a Pending pod with 2 containers in spec
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "coredns",
            "namespace": "kube-system",
            "creationTimestamp": "2026-04-03T00:00:00Z"
        },
        "spec": {
            "containers": [
                {"name": "coredns", "image": "coredns:latest"},
                {"name": "sidecar", "image": "nginx:latest"}
            ]
        },
        "status": {
            "phase": "Pending",
            "containerStatuses": []  // Empty - pod not created yet
        }
    });

    let table = pod_list_to_table(vec![pod], "1".to_string());

    // Verify table structure
    assert_eq!(table["kind"], "Table");
    assert_eq!(table["rows"].as_array().unwrap().len(), 1);

    // Check READY column (should be "0/2", not "0/0")
    let ready_cell = &table["rows"][0]["cells"][1];
    assert_eq!(
        ready_cell.as_str().unwrap(),
        "0/2",
        "READY should show 0 ready out of 2 total containers from spec.containers"
    );
}

#[test]
fn test_pod_list_to_table_ready_count_with_running_pod() {
    // Verify READY column for a Running pod with containerStatuses populated
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "nginx",
            "namespace": "default",
            "creationTimestamp": "2026-04-03T00:00:00Z"
        },
        "spec": {
            "containers": [
                {"name": "nginx", "image": "nginx:latest"}
            ]
        },
        "status": {
            "phase": "Running",
            "containerStatuses": [
                {
                    "name": "nginx",
                    "ready": true,
                    "restartCount": 0,
                    "state": {"running": {"startedAt": "2026-04-03T00:01:00Z"}}
                }
            ]
        }
    });

    let table = pod_list_to_table(vec![pod], "1".to_string());

    let ready_cell = &table["rows"][0]["cells"][1];
    assert_eq!(
        ready_cell.as_str().unwrap(),
        "1/1",
        "READY should show 1 ready out of 1 total container"
    );
}

#[test]
fn test_pod_list_to_table_ready_count_with_partial_ready() {
    // Pod with 2 containers, only 1 ready
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "multi",
            "namespace": "default",
            "creationTimestamp": "2026-04-03T00:00:00Z"
        },
        "spec": {
            "containers": [
                {"name": "app", "image": "app:latest"},
                {"name": "sidecar", "image": "sidecar:latest"}
            ]
        },
        "status": {
            "phase": "Running",
            "containerStatuses": [
                {"name": "app", "ready": true, "restartCount": 0},
                {"name": "sidecar", "ready": false, "restartCount": 0}
            ]
        }
    });

    let table = pod_list_to_table(vec![pod], "1".to_string());

    let ready_cell = &table["rows"][0]["cells"][1];
    assert_eq!(
        ready_cell.as_str().unwrap(),
        "1/2",
        "READY should show 1 ready out of 2 total containers"
    );
}

#[test]
fn test_wants_table_format_with_table_accept_header() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "accept",
        "application/json;as=Table;v=v1;g=meta.k8s.io,application/json"
            .parse()
            .unwrap(),
    );
    assert!(wants_table_format(&headers).unwrap());
}

#[test]
fn test_wants_table_format_with_json_accept_returns_false() {
    let mut headers = HeaderMap::new();
    headers.insert("accept", "application/json".parse().unwrap());
    assert!(!wants_table_format(&headers).unwrap());
}

#[test]
fn test_wants_table_format_no_accept_header_returns_false() {
    let headers = HeaderMap::new();
    assert!(!wants_table_format(&headers).unwrap());
}

#[test]
fn test_wants_table_format_protobuf_accept_returns_false() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "accept",
        "application/vnd.kubernetes.protobuf".parse().unwrap(),
    );
    assert!(!wants_table_format(&headers).unwrap());
}

#[test]
fn test_wants_table_format_unsupported_version_returns_406() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "accept",
        "application/json;as=Table;v=v2;g=meta.k8s.io"
            .parse()
            .unwrap(),
    );
    let result = wants_table_format(&headers);
    assert!(
        result.is_err(),
        "Unsupported Table version should return error"
    );
}

#[test]
fn test_wants_table_format_unsupported_group_returns_406() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "accept",
        "application/json;as=Table;v=v1;g=other.k8s.io"
            .parse()
            .unwrap(),
    );
    let result = wants_table_format(&headers);
    assert!(
        result.is_err(),
        "Unsupported Table group should return error"
    );
}

#[test]
fn test_pod_list_to_table_empty_list_returns_table_with_no_rows() {
    let result = pod_list_to_table(vec![], "100".to_string());
    assert_eq!(result["kind"], "Table");
    assert_eq!(result["apiVersion"], "meta.k8s.io/v1");
    assert_eq!(result["metadata"]["resourceVersion"], "100");
    assert_eq!(result["rows"].as_array().unwrap().len(), 0);
    assert_eq!(result["columnDefinitions"].as_array().unwrap().len(), 9);
}

#[test]
fn test_pod_list_to_table_includes_kubernetes_wide_columns() {
    let result = pod_list_to_table(vec![], "100".to_string());
    let columns = result["columnDefinitions"].as_array().unwrap();
    let actual: Vec<(&str, i64)> = columns
        .iter()
        .map(|column| {
            (
                column["name"].as_str().unwrap(),
                column["priority"].as_i64().unwrap(),
            )
        })
        .collect();

    assert_eq!(
        actual,
        vec![
            ("Name", 0),
            ("Ready", 0),
            ("Status", 0),
            ("Restarts", 0),
            ("Age", 0),
            ("IP", 1),
            ("Node", 1),
            ("Nominated Node", 1),
            ("Readiness Gates", 1),
        ]
    );
}

#[test]
fn test_pod_list_to_table_running_pod_shows_correct_cells() {
    let pod = json!({
        "metadata": {
            "name": "nginx-abc123",
            "creationTimestamp": "2026-01-01T00:00:00Z"
        },
        "spec": {
            "containers": [
                {"name": "nginx", "image": "nginx:latest"}
            ]
        },
        "status": {
            "phase": "Running",
            "containerStatuses": [
                {"name": "nginx", "ready": true, "restartCount": 0}
            ]
        }
    });

    let result = pod_list_to_table(vec![pod], "42".to_string());
    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);

    let cells = rows[0]["cells"].as_array().unwrap();
    assert_eq!(cells[0], "nginx-abc123"); // NAME
    assert_eq!(cells[1], "1/1"); // READY
    assert_eq!(cells[2], "Running"); // STATUS
    assert_eq!(cells[3], 0); // RESTARTS
    assert!(cells[4].is_string()); // AGE (dynamic)
}

#[test]
fn test_pod_list_to_table_wide_cells_match_kubernetes_printer() {
    let pod = json!({
        "metadata": {
            "name": "wide-pod",
            "creationTimestamp": "2026-01-01T00:00:00Z"
        },
        "spec": {
            "nodeName": "node-a",
            "containers": [
                {"name": "nginx", "image": "nginx:latest"}
            ],
            "readinessGates": [
                {"conditionType": "example.com/ready"},
                {"conditionType": "example.com/blocked"}
            ]
        },
        "status": {
            "phase": "Running",
            "podIP": "10.42.0.7",
            "nominatedNodeName": "node-b",
            "conditions": [
                {"type": "example.com/ready", "status": "True"},
                {"type": "example.com/blocked", "status": "False"}
            ],
            "containerStatuses": [
                {"name": "nginx", "ready": true, "restartCount": 0}
            ]
        }
    });

    let result = pod_list_to_table(vec![pod], "42".to_string());
    let cells = result["rows"][0]["cells"].as_array().unwrap();

    assert_eq!(cells[5], "10.42.0.7");
    assert_eq!(cells[6], "node-a");
    assert_eq!(cells[7], "node-b");
    assert_eq!(cells[8], "1/2");
}

#[test]
fn test_pod_list_to_table_wide_cells_default_to_none() {
    let pod = json!({
        "metadata": {
            "name": "pending-pod",
            "creationTimestamp": "2026-01-01T00:00:00Z"
        },
        "spec": {
            "containers": [
                {"name": "nginx", "image": "nginx:latest"}
            ]
        },
        "status": {
            "phase": "Pending",
            "containerStatuses": []
        }
    });

    let result = pod_list_to_table(vec![pod], "42".to_string());
    let cells = result["rows"][0]["cells"].as_array().unwrap();

    assert_eq!(cells[5], "<none>");
    assert_eq!(cells[6], "<none>");
    assert_eq!(cells[7], "<none>");
    assert_eq!(cells[8], "<none>");
}

#[test]
fn test_pod_list_to_table_multi_container_restart_sum() {
    let pod = json!({
        "metadata": {"name": "multi", "creationTimestamp": "2026-01-01T00:00:00Z"},
        "spec": {
            "containers": [
                {"name": "app", "image": "app:latest"},
                {"name": "sidecar", "image": "sidecar:latest"}
            ]
        },
        "status": {
            "phase": "Running",
            "containerStatuses": [
                {"name": "app", "ready": true, "restartCount": 5},
                {"name": "sidecar", "ready": false, "restartCount": 3}
            ]
        }
    });

    let result = pod_list_to_table(vec![pod], "1".to_string());
    let cells = result["rows"][0]["cells"].as_array().unwrap();
    assert_eq!(cells[1], "1/2"); // READY: 1 of 2 ready
    assert_eq!(cells[3], 8); // RESTARTS: 5 + 3
}

#[test]
fn test_pod_list_to_table_init_container_not_ready_shows_init_prefix() {
    let pod = json!({
        "metadata": {"name": "init-pod", "creationTimestamp": "2026-01-01T00:00:00Z"},
        "status": {
            "phase": "Pending",
            "containerStatuses": [],
            "initContainerStatuses": [
                {"name": "init-db", "ready": false, "restartCount": 0}
            ]
        }
    });

    let result = pod_list_to_table(vec![pod], "1".to_string());
    let cells = result["rows"][0]["cells"].as_array().unwrap();
    assert_eq!(cells[2], "Init:Pending"); // STATUS with Init: prefix
}

#[test]
fn test_pod_list_to_table_missing_status_shows_defaults() {
    let pod = json!({
        "metadata": {"name": "no-status", "creationTimestamp": "2026-01-01T00:00:00Z"}
    });

    let result = pod_list_to_table(vec![pod], "1".to_string());
    let cells = result["rows"][0]["cells"].as_array().unwrap();
    assert_eq!(cells[1], "0/0"); // READY
    assert_eq!(cells[2], "Unknown"); // STATUS
    assert_eq!(cells[3], 0); // RESTARTS
}

#[test]
fn test_pod_list_to_table_prefers_status_reason_for_node_lost_pod() {
    let pod = json!({
        "metadata": {"name": "lost-pod", "creationTimestamp": "2026-01-01T00:00:00Z"},
        "spec": {
            "nodeName": "worker-a",
            "containers": [{"name": "c"}]
        },
        "status": {
            "phase": "Failed",
            "reason": "NodeLost",
            "podIP": "10.42.0.10",
            "containerStatuses": [{
                "name": "c",
                "ready": false,
                "restartCount": 0,
                "state": {"running": {"startedAt": "2026-01-01T00:00:01Z"}}
            }]
        }
    });

    let result = pod_list_to_table(vec![pod], "1".to_string());
    let cells = result["rows"][0]["cells"].as_array().unwrap();
    assert_eq!(cells[2], "NodeLost");
}

#[test]
fn test_pod_list_to_table_invalid_timestamp_shows_unknown_age() {
    let pod = json!({
        "metadata": {"name": "bad-ts", "creationTimestamp": "not-a-date"},
        "status": {"phase": "Running"}
    });

    let result = pod_list_to_table(vec![pod], "1".to_string());
    let cells = result["rows"][0]["cells"].as_array().unwrap();
    assert_eq!(cells[4], "<unknown>");
}

#[test]
fn test_pod_list_to_table_multiple_pods() {
    let pods = vec![
        json!({"metadata": {"name": "pod-a", "creationTimestamp": "2026-01-01T00:00:00Z"}, "status": {"phase": "Running"}}),
        json!({"metadata": {"name": "pod-b", "creationTimestamp": "2026-01-01T00:00:00Z"}, "status": {"phase": "Pending"}}),
    ];

    let result = pod_list_to_table(pods, "99".to_string());
    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["cells"][0], "pod-a");
    assert_eq!(rows[1]["cells"][0], "pod-b");
}

#[test]
fn test_node_list_to_table_empty_list_returns_table_with_no_rows() {
    let result = node_list_to_table(vec![], "50".to_string());
    assert_eq!(result["kind"], "Table");
    assert_eq!(result["apiVersion"], "meta.k8s.io/v1");
    assert_eq!(result["metadata"]["resourceVersion"], "50");
    assert_eq!(result["rows"].as_array().unwrap().len(), 0);
    let columns = result["columnDefinitions"].as_array().unwrap();
    let column_names: Vec<&str> = columns
        .iter()
        .map(|column| column["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        column_names,
        vec![
            "Name",
            "Status",
            "Roles",
            "Age",
            "Version",
            "Internal-IP",
            "External-IP",
            "OS-Image",
            "Kernel-Version",
            "Container-Runtime",
            "Commit",
        ]
    );
    for column in &columns[0..5] {
        assert_eq!(column["priority"], 0);
    }
    for column in &columns[5..11] {
        assert_eq!(column["priority"], 1);
    }
}

#[test]
fn test_node_list_to_table_ready_node_shows_correct_cells() {
    let node = json!({
        "metadata": {
            "name": "node-1",
            "creationTimestamp": "2026-01-01T00:00:00Z",
            "labels": {"node-role.kubernetes.io/leader": ""},
            "annotations": {"klights.io/git-commit": "abc12345"}
        },
        "status": {
            "conditions": [{"type": "Ready", "status": "True"}],
            "addresses": [
                {"type": "Hostname", "address": "node-1"},
                {"type": "InternalIP", "address": "10.0.0.10"},
                {"type": "ExternalIP", "address": "203.0.113.10"}
            ],
            "nodeInfo": {
                "kubeletVersion": "v1.34+klights1.0.0",
                "osImage": "Ubuntu 24.04.4 LTS",
                "kernelVersion": "6.17.0-23-generic",
                "containerRuntimeVersion": "containerd://2.2.3"
            }
        }
    });

    let result = node_list_to_table(vec![node], "10".to_string());
    let cells = result["rows"][0]["cells"].as_array().unwrap();
    assert_eq!(cells.len(), 11);
    assert_eq!(cells[0], "node-1"); // NAME
    assert_eq!(cells[1], "Ready"); // STATUS
    assert_eq!(cells[2], "leader"); // ROLES
    assert!(cells[3].is_string()); // AGE
    assert_eq!(cells[4], "v1.34+klights1.0.0"); // VERSION
    assert_eq!(cells[5], "10.0.0.10"); // INTERNAL-IP
    assert_eq!(cells[6], "203.0.113.10"); // EXTERNAL-IP
    assert_eq!(cells[7], "Ubuntu 24.04.4 LTS"); // OS-IMAGE
    assert_eq!(cells[8], "6.17.0-23-generic"); // KERNEL-VERSION
    assert_eq!(cells[9], "containerd://2.2.3"); // CONTAINER-RUNTIME
    assert_eq!(cells[10], "abc12345"); // COMMIT
}

#[test]
fn test_node_list_to_table_ready_unschedulable_node_shows_scheduling_disabled() {
    let node = json!({
        "metadata": {
            "name": "node-1",
            "creationTimestamp": "2026-01-01T00:00:00Z"
        },
        "spec": {"unschedulable": true},
        "status": {
            "conditions": [{"type": "Ready", "status": "True"}],
            "nodeInfo": {"kubeletVersion": "v1.34+klights1.0.0"}
        }
    });

    let result = node_list_to_table(vec![node], "10".to_string());
    let cells = result["rows"][0]["cells"].as_array().unwrap();
    assert_eq!(cells[1], "Ready,SchedulingDisabled");
}

#[test]
fn test_node_list_to_table_shows_worker_role_from_labels() {
    let node = json!({
        "metadata": {
            "name": "node-2",
            "creationTimestamp": "2026-01-01T00:00:00Z",
            "labels": {"node-role.kubernetes.io/worker": ""}
        },
        "status": {
            "conditions": [{"type": "Ready", "status": "True"}],
            "nodeInfo": {"kubeletVersion": "v1.34+klights1.0.0"}
        }
    });

    let result = node_list_to_table(vec![node], "10".to_string());
    let cells = result["rows"][0]["cells"].as_array().unwrap();
    assert_eq!(cells[2], "worker");
}

#[test]
fn test_node_list_to_table_shows_none_when_no_role_labels() {
    let node = json!({
        "metadata": {"name": "node-3", "creationTimestamp": "2026-01-01T00:00:00Z"},
        "status": {
            "conditions": [{"type": "Ready", "status": "True"}],
            "nodeInfo": {"kubeletVersion": "v1.34+klights1.0.0"}
        }
    });

    let result = node_list_to_table(vec![node], "10".to_string());
    let cells = result["rows"][0]["cells"].as_array().unwrap();
    assert_eq!(cells[2], "<none>");
}

#[test]
fn test_node_list_to_table_not_ready_node() {
    let node = json!({
        "metadata": {"name": "node-2", "creationTimestamp": "2026-01-01T00:00:00Z"},
        "status": {
            "conditions": [{"type": "Ready", "status": "False"}],
            "nodeInfo": {"kubeletVersion": "v1.34.6"}
        }
    });

    let result = node_list_to_table(vec![node], "1".to_string());
    let cells = result["rows"][0]["cells"].as_array().unwrap();
    assert_eq!(cells[1], "NotReady");
}

#[test]
fn test_node_list_to_table_no_conditions_shows_unknown() {
    let node = json!({
        "metadata": {"name": "node-3", "creationTimestamp": "2026-01-01T00:00:00Z"},
        "status": {}
    });

    let result = node_list_to_table(vec![node], "1".to_string());
    let cells = result["rows"][0]["cells"].as_array().unwrap();
    assert_eq!(cells[1], "Unknown"); // STATUS
    assert_eq!(cells[4], "<unknown>"); // VERSION
    assert_eq!(cells[5], "<none>"); // INTERNAL-IP
    assert_eq!(cells[6], "<none>"); // EXTERNAL-IP
    assert_eq!(cells[7], "<unknown>"); // OS-IMAGE
    assert_eq!(cells[8], "<unknown>"); // KERNEL-VERSION
    assert_eq!(cells[9], "<unknown>"); // CONTAINER-RUNTIME
    assert_eq!(cells[10], "<unknown>"); // COMMIT
}

#[test]
fn test_node_list_to_table_invalid_timestamp_shows_unknown_age() {
    let node = json!({
        "metadata": {"name": "node-4", "creationTimestamp": "invalid"},
        "status": {"conditions": [{"type": "Ready", "status": "True"}]}
    });

    let result = node_list_to_table(vec![node], "1".to_string());
    let cells = result["rows"][0]["cells"].as_array().unwrap();
    assert_eq!(cells[3], "<unknown>");
}

#[test]
fn test_watch_event_to_table_pod_modified_includes_ready_status_restarts() {
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "nginx",
            "namespace": "default",
            "resourceVersion": "100",
            "creationTimestamp": "2026-04-10T00:00:00Z"
        },
        "spec": {
            "containers": [{"name": "nginx", "image": "nginx"}]
        },
        "status": {
            "phase": "Running",
            "containerStatuses": [{
                "name": "nginx",
                "ready": true,
                "restartCount": 0,
                "state": {"running": {"startedAt": "2026-04-10T00:00:01Z"}}
            }]
        }
    });
    let event = WatchEvent::modified(pod);
    let table_event = watch_event_to_table(event, "Pod");

    assert_eq!(table_event.event_type, EventType::Modified);
    assert_eq!(table_event.object["kind"], "Table");
    assert_eq!(table_event.object["apiVersion"], "meta.k8s.io/v1");

    let rows = table_event.object["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);

    let cells = rows[0]["cells"].as_array().unwrap();
    assert_eq!(cells.len(), 9); // NAME, READY, STATUS, RESTARTS, AGE, IP, NODE, NOMINATED NODE, READINESS GATES
    assert_eq!(cells[0], "nginx");
    assert_eq!(cells[1], "1/1");
    assert_eq!(cells[2], "Running");
    assert_eq!(cells[3], 0);
}

#[test]
fn test_watch_event_to_table_pod_pending_shows_zero_ready() {
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "test",
            "namespace": "default",
            "resourceVersion": "50",
            "creationTimestamp": "2026-04-10T00:00:00Z"
        },
        "spec": {
            "containers": [
                {"name": "c1", "image": "img1"},
                {"name": "c2", "image": "img2"}
            ]
        },
        "status": {
            "phase": "Pending"
        }
    });
    let event = WatchEvent::added(pod);
    let table_event = watch_event_to_table(event, "Pod");

    assert_eq!(table_event.event_type, EventType::Added);
    let cells = table_event.object["rows"][0]["cells"].as_array().unwrap();
    assert_eq!(cells[0], "test");
    assert_eq!(cells[1], "0/2");
    assert_eq!(cells[2], "Pending");
    assert_eq!(cells[3], 0);
}

#[test]
fn test_watch_event_to_table_node_includes_status_roles_version() {
    let node = json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": "dp",
            "resourceVersion": "10",
            "creationTimestamp": "2026-04-10T00:00:00Z",
            "labels": {"node-role.kubernetes.io/leader": ""},
            "annotations": {"klights.io/git-commit": "deadbeef"}
        },
        "status": {
            "conditions": [{"type": "Ready", "status": "True"}],
            "addresses": [
                {"type": "InternalIP", "address": "10.0.0.10"},
                {"type": "ExternalIP", "address": "203.0.113.10"}
            ],
            "nodeInfo": {
                "kubeletVersion": "v1.34+klights",
                "osImage": "Ubuntu 24.04.4 LTS",
                "kernelVersion": "6.17.0-23-generic",
                "containerRuntimeVersion": "containerd://2.2.3"
            }
        }
    });
    let event = WatchEvent::modified(node);
    let table_event = watch_event_to_table(event, "Node");

    assert_eq!(table_event.object["kind"], "Table");
    let cells = table_event.object["rows"][0]["cells"].as_array().unwrap();
    assert_eq!(cells.len(), 11);
    assert_eq!(cells[0], "dp");
    assert_eq!(cells[1], "Ready");
    assert_eq!(cells[2], "leader");
    assert_eq!(cells[4], "v1.34+klights");
    assert_eq!(cells[5], "10.0.0.10");
    assert_eq!(cells[6], "203.0.113.10");
    assert_eq!(cells[7], "Ubuntu 24.04.4 LTS");
    assert_eq!(cells[8], "6.17.0-23-generic");
    assert_eq!(cells[9], "containerd://2.2.3");
    assert_eq!(cells[10], "deadbeef");
}

#[test]
fn test_watch_event_to_table_generic_resource_has_name_and_age() {
    let svc = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "my-svc",
            "namespace": "default",
            "resourceVersion": "200",
            "creationTimestamp": "2026-04-10T00:00:00Z"
        }
    });
    let event = WatchEvent::added(svc);
    let table_event = watch_event_to_table(event, "Service");

    assert_eq!(table_event.event_type, EventType::Added);
    assert_eq!(table_event.object["kind"], "Table");
    let rows = table_event.object["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    let cells = rows[0]["cells"].as_array().unwrap();
    assert_eq!(cells[0], "my-svc");
}

#[test]
fn test_watch_event_to_table_deleted_preserves_event_type() {
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "deleted-pod",
            "namespace": "default",
            "resourceVersion": "300",
            "creationTimestamp": "2026-04-10T00:00:00Z"
        },
        "spec": {"containers": [{"name": "c", "image": "img"}]},
        "status": {"phase": "Succeeded"}
    });
    let event = WatchEvent::deleted(pod);
    let table_event = watch_event_to_table(event, "Pod");

    assert_eq!(table_event.event_type, EventType::Deleted);
    assert_eq!(table_event.object["kind"], "Table");
}

#[test]
fn test_watch_bookmark_table_omits_column_definitions_for_periodic_bookmarks() {
    // Periodic BOOKMARK events (not initial-events-end) must NOT include
    // columnDefinitions to prevent kubectl from printing duplicate headers.
    // Only the initial-events-end BOOKMARK gets columnDefinitions.
    let event = WatchEvent::bookmark_typed(500, "v1", "Pod");
    let table_event = watch_event_to_table(event, "Pod");

    assert_eq!(table_event.event_type, EventType::Bookmark);
    assert_eq!(table_event.object["kind"], "Table");
    assert_eq!(table_event.object["apiVersion"], "meta.k8s.io/v1");
    assert_eq!(table_event.object["metadata"]["resourceVersion"], "500");

    // Periodic BOOKMARKs must NOT have columnDefinitions (prevents duplicate headers)
    assert!(
        table_event.object.get("columnDefinitions").is_none(),
        "Periodic BOOKMARK must not have columnDefinitions, but found: {:?}",
        table_event.object.get("columnDefinitions")
    );

    // Must have empty rows
    let rows = table_event.object["rows"].as_array().unwrap();
    assert!(
        rows.is_empty(),
        "BOOKMARK Table must have empty rows, got {} rows",
        rows.len()
    );
}

#[test]
fn test_watch_bookmark_table_initial_events_end_has_column_definitions() {
    // The initial-events-end BOOKMARK must include columnDefinitions so kubectl
    // prints the column headers after receiving the initial LIST via watch.
    let mut bookmark = WatchEvent::bookmark_typed(500, "v1", "Pod");
    Arc::make_mut(&mut bookmark.object)["metadata"]["annotations"] = json!({
        "k8s.io/initial-events-end": "true"
    });

    let table_event = watch_event_to_table(bookmark, "Pod");

    assert_eq!(table_event.event_type, EventType::Bookmark);
    assert_eq!(table_event.object["kind"], "Table");

    // initial-events-end BOOKMARK must have Pod column definitions.
    let col_defs = table_event.object["columnDefinitions"].as_array().unwrap();
    assert_eq!(
        col_defs.len(),
        9,
        "initial-events-end BOOKMARK must have 9 Pod column definitions"
    );
    assert_eq!(col_defs[0]["name"], "Name");
    assert_eq!(col_defs[1]["name"], "Ready");
    assert_eq!(col_defs[2]["name"], "Status");
    assert_eq!(col_defs[3]["name"], "Restarts");
    assert_eq!(col_defs[4]["name"], "Age");
    assert_eq!(col_defs[5]["name"], "IP");
    assert_eq!(col_defs[6]["name"], "Node");
    assert_eq!(col_defs[7]["name"], "Nominated Node");
    assert_eq!(col_defs[8]["name"], "Readiness Gates");

    // Rows still empty
    let rows = table_event.object["rows"].as_array().unwrap();
    assert!(rows.is_empty());
}

#[test]
fn test_watch_bookmark_table_preserves_annotations() {
    // sendInitialEvents uses k8s.io/initial-events-end annotation on the
    // final BOOKMARK to signal that initial LIST is complete. This annotation
    // must survive Table conversion.
    let mut bookmark = WatchEvent::bookmark_typed(999, "v1", "Pod");
    Arc::make_mut(&mut bookmark.object)["metadata"]["annotations"] = json!({
        "k8s.io/initial-events-end": "true"
    });

    let table_event = watch_event_to_table(bookmark, "Pod");

    assert_eq!(table_event.event_type, EventType::Bookmark);
    assert_eq!(
        table_event.object["metadata"]["annotations"]["k8s.io/initial-events-end"], "true",
        "initial-events-end annotation must be preserved in BOOKMARK Table"
    );
    // Rows still empty
    assert!(table_event.object["rows"].as_array().unwrap().is_empty());
}

#[test]
fn test_watch_modified_event_table_has_populated_rows() {
    // Contrast with BOOKMARK: MODIFIED events must have non-empty rows
    // with correct cell values matching the pod data.
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "web-server",
            "namespace": "default",
            "resourceVersion": "42",
            "creationTimestamp": "2026-04-10T00:00:00Z"
        },
        "spec": {
            "containers": [
                {"name": "nginx", "image": "nginx"},
                {"name": "sidecar", "image": "busybox"}
            ]
        },
        "status": {
            "phase": "Running",
            "containerStatuses": [
                {"name": "nginx", "ready": true, "restartCount": 1},
                {"name": "sidecar", "ready": true, "restartCount": 0}
            ]
        }
    });
    let event = WatchEvent::modified(pod);
    let table_event = watch_event_to_table(event, "Pod");

    assert_eq!(table_event.event_type, EventType::Modified);
    assert_eq!(table_event.object["kind"], "Table");

    // Must have non-empty rows
    let rows = table_event.object["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1, "MODIFIED event must have exactly 1 row");

    // Cell values must match pod data
    let cells = rows[0]["cells"].as_array().unwrap();
    assert_eq!(cells[0], "web-server"); // NAME
    assert_eq!(cells[1], "2/2"); // READY (both containers ready)
    assert_eq!(cells[2], "Running"); // STATUS
    assert_eq!(cells[3], 1); // RESTARTS (sum: 1+0=1)

    // MODIFIED events must NOT have columnDefinitions (prevents duplicate headers)
    assert!(
        table_event.object.get("columnDefinitions").is_none(),
        "MODIFIED watch events must not include columnDefinitions"
    );
}

#[test]
fn test_watch_event_to_table_preserves_resource_version_in_table_metadata() {
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "nginx",
            "namespace": "default",
            "resourceVersion": "42",
            "creationTimestamp": "2026-04-10T00:00:00Z"
        },
        "spec": {"containers": [{"name": "c", "image": "img"}]},
        "status": {"phase": "Running"}
    });
    let event = WatchEvent::modified(pod);
    let table_event = watch_event_to_table(event, "Pod");

    assert_eq!(table_event.object["metadata"]["resourceVersion"], "42");
}

#[test]
fn test_watch_event_contains_full_pod_status() {
    // Watch events must contain the COMPLETE pod object including all status fields.
    // This ensures kubectl can render READY, STATUS, RESTARTS columns.
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "nginx",
            "namespace": "default",
            "resourceVersion": "100",
            "creationTimestamp": "2026-04-10T00:00:00Z"
        },
        "spec": {
            "containers": [{"name": "nginx", "image": "nginx:latest"}]
        },
        "status": {
            "phase": "Running",
            "conditions": [
                {"type": "Ready", "status": "True"},
                {"type": "ContainersReady", "status": "True"}
            ],
            "containerStatuses": [{
                "name": "nginx",
                "ready": true,
                "restartCount": 2,
                "state": {"running": {"startedAt": "2026-04-10T00:00:01Z"}},
                "image": "nginx:latest",
                "imageID": "docker://sha256:abc123"
            }],
            "podIP": "10.43.0.5",
            "hostIP": "127.0.0.1"
        }
    });

    let event = WatchEvent::modified(pod.clone());

    // The raw watch event object must contain the full pod data
    assert_eq!(event.object["status"]["phase"], "Running");
    assert!(event.object["status"]["containerStatuses"].is_array());
    assert_eq!(
        event.object["status"]["containerStatuses"][0]["ready"],
        true
    );
    assert_eq!(
        event.object["status"]["containerStatuses"][0]["restartCount"],
        2
    );
    assert!(event.object["status"]["conditions"].is_array());
    assert_eq!(event.object["status"]["podIP"], "10.43.0.5");

    // When converted to Table format, the object row must preserve the full pod
    let table_event = watch_event_to_table(event, "Pod");
    let row_object = &table_event.object["rows"][0]["object"];
    assert_eq!(row_object["status"]["phase"], "Running");
    assert_eq!(row_object["status"]["containerStatuses"][0]["ready"], true);
    assert_eq!(
        row_object["status"]["containerStatuses"][0]["restartCount"],
        2
    );
    assert_eq!(row_object["status"]["podIP"], "10.43.0.5");
}

#[test]
fn test_watch_event_columns_match_list() {
    // Watch event Table format must have the same column definitions as LIST Table format.
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "nginx",
            "namespace": "default",
            "resourceVersion": "100",
            "creationTimestamp": "2026-04-10T00:00:00Z"
        },
        "spec": {
            "containers": [{"name": "nginx", "image": "nginx"}]
        },
        "status": {
            "phase": "Running",
            "containerStatuses": [{
                "name": "nginx",
                "ready": true,
                "restartCount": 0
            }]
        }
    });

    // Get LIST Table format
    let list_table = pod_list_to_table(vec![pod.clone()], "100".to_string());

    // Get watch Table format
    let event = WatchEvent::modified(pod);
    let watch_table = watch_event_to_table(event, "Pod");

    // LIST must have columnDefinitions
    let list_cols = list_table["columnDefinitions"].as_array().unwrap();
    assert_eq!(list_cols.len(), 9);
    assert_eq!(list_cols[0]["name"], "Name");

    // MODIFIED watch events must NOT have columnDefinitions
    assert!(
        watch_table.object.get("columnDefinitions").is_none(),
        "MODIFIED watch events must not include columnDefinitions"
    );

    // Cell values must match (same pod data = same cells)
    let list_cells = &list_table["rows"][0]["cells"];
    let watch_cells = &watch_table.object["rows"][0]["cells"];
    assert_eq!(list_cells, watch_cells);
}

#[test]
fn test_watch_event_to_table_service_includes_object() {
    let svc = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "my-svc",
            "namespace": "default",
            "resourceVersion": "50",
            "creationTimestamp": "2026-04-10T00:00:00Z"
        },
        "spec": {
            "type": "ClusterIP",
            "clusterIP": "10.43.128.10",
            "ports": [{"port": 80, "targetPort": 8080, "protocol": "TCP"}]
        }
    });
    let event = WatchEvent::modified(svc.clone());
    let table_event = watch_event_to_table(event, "Service");

    assert_eq!(table_event.event_type, EventType::Modified);
    assert_eq!(table_event.object["kind"], "Table");
    assert_eq!(table_event.object["metadata"]["resourceVersion"], "50");

    // Row must contain the full resource object for kubectl to extract fields
    let rows = table_event.object["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    let row_object = &rows[0]["object"];
    assert_eq!(row_object["metadata"]["name"], "my-svc");
    assert_eq!(row_object["spec"]["type"], "ClusterIP");
    assert_eq!(row_object["spec"]["clusterIP"], "10.43.128.10");
}

#[test]
fn test_watch_event_to_table_configmap_includes_object() {
    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "my-config",
            "namespace": "kube-system",
            "resourceVersion": "77",
            "creationTimestamp": "2026-04-10T00:00:00Z"
        },
        "data": {
            "key1": "value1",
            "key2": "value2"
        }
    });
    let event = WatchEvent::added(cm);
    let table_event = watch_event_to_table(event, "ConfigMap");

    assert_eq!(table_event.event_type, EventType::Added);
    assert_eq!(table_event.object["kind"], "Table");

    let rows = table_event.object["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    let cells = rows[0]["cells"].as_array().unwrap();
    assert_eq!(cells[0], "my-config"); // Name column

    // Row object must contain full ConfigMap data
    let row_object = &rows[0]["object"];
    assert_eq!(row_object["data"]["key1"], "value1");
    assert_eq!(row_object["data"]["key2"], "value2");
}

#[test]
fn test_watch_event_to_table_secret_includes_object() {
    let secret = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": "my-secret",
            "namespace": "default",
            "resourceVersion": "88",
            "creationTimestamp": "2026-04-10T00:00:00Z"
        },
        "type": "Opaque",
        "data": {
            "password": "cGFzc3dvcmQ=" // base64 "password"
        }
    });
    let event = WatchEvent::added(secret);
    let table_event = watch_event_to_table(event, "Secret");

    assert_eq!(table_event.object["kind"], "Table");

    let rows = table_event.object["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    let cells = rows[0]["cells"].as_array().unwrap();
    assert_eq!(cells[0], "my-secret");

    // Row object must contain full Secret
    let row_object = &rows[0]["object"];
    assert_eq!(row_object["type"], "Opaque");
}
