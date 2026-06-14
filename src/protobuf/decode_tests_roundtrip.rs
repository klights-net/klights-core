use crate::protobuf::*;

#[test]
pub fn test_lease_protobuf_roundtrip() {
    // Build a Lease protobuf message
    use k8s_pb::api::coordination::v1::{Lease, LeaseSpec};
    use k8s_pb::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
    use prost::Message;

    let pb = Lease {
        metadata: Some(ObjectMeta {
            name: Some("test-lease".to_string()),
            namespace: Some("kube-system".to_string()),
            ..Default::default()
        }),
        spec: Some(LeaseSpec {
            holder_identity: Some("controller-1".to_string()),
            lease_duration_seconds: Some(15),
            acquire_time: Some(MicroTime {
                seconds: Some(1700000000),
                nanos: Some(0),
            }),
            renew_time: Some(MicroTime {
                seconds: Some(1700000015),
                nanos: Some(0),
            }),
            lease_transitions: Some(3),
            preferred_holder: None,
            strategy: None,
        }),
    };

    let mut buf = Vec::new();
    pb.encode(&mut buf).unwrap();

    // Decode the protobuf
    let decoded = decode_protobuf_resource("", "Lease", &buf).unwrap();

    assert_eq!(decoded["apiVersion"], "coordination.k8s.io/v1");
    assert_eq!(decoded["kind"], "Lease");
    assert_eq!(decoded["metadata"]["name"], "test-lease");
    assert_eq!(decoded["metadata"]["namespace"], "kube-system");
    assert_eq!(decoded["spec"]["holderIdentity"], "controller-1");
    assert_eq!(decoded["spec"]["leaseDurationSeconds"], 15);
    assert_eq!(decoded["spec"]["leaseTransitions"], 3);
    assert!(decoded["spec"]["acquireTime"].is_string());
    assert!(decoded["spec"]["renewTime"].is_string());
}

// ========================
// encode_protobuf roundtrip tests
// ========================

#[test]
pub fn test_encode_protobuf_pod_roundtrip() {
    use serde_json::json;

    // Create a Pod JSON
    let pod_json = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "test-pod",
            "namespace": "default",
            "uid": "test-uid-123",
            "resourceVersion": "42",
            "labels": {
                "app": "test"
            }
        },
        "spec": {
            "containers": [{
                "name": "nginx",
                "image": "nginx:latest"
            }]
        }
    });

    // Encode to protobuf
    let protobuf_bytes = encode_protobuf(&pod_json).unwrap();

    // Verify magic prefix
    assert_eq!(&protobuf_bytes[0..4], &[0x6b, 0x38, 0x73, 0x00]);

    // Decode back (decode_protobuf expects data without magic prefix)
    let decoded = decode_protobuf(&protobuf_bytes[4..]).unwrap();

    // Verify round-trip preserves data
    assert_eq!(decoded["apiVersion"], "v1");
    assert_eq!(decoded["kind"], "Pod");
    assert_eq!(decoded["metadata"]["name"], "test-pod");
    assert_eq!(decoded["metadata"]["namespace"], "default");
    assert_eq!(decoded["metadata"]["uid"], "test-uid-123");
    assert_eq!(decoded["metadata"]["resourceVersion"], "42");
    assert_eq!(decoded["metadata"]["labels"]["app"], "test");
    assert_eq!(decoded["spec"]["containers"][0]["name"], "nginx");
    assert_eq!(decoded["spec"]["containers"][0]["image"], "nginx:latest");
}

#[test]
pub fn test_pod_protobuf_roundtrip_preserves_node_selector() {
    use serde_json::json;

    let pod_json = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "restricted-pod",
            "namespace": "default"
        },
        "spec": {
            "nodeSelector": {
                "label": "nonempty"
            },
            "containers": [{
                "name": "restricted-pod",
                "image": "registry.k8s.io/pause:3.10.1"
            }]
        }
    });

    let protobuf_bytes = encode_protobuf(&pod_json).unwrap();
    let decoded = decode_protobuf(&protobuf_bytes[4..]).unwrap();

    assert_eq!(
        decoded.pointer("/spec/nodeSelector/label"),
        Some(&json!("nonempty")),
        "protobuf Pod create must preserve nodeSelector for scheduler predicates"
    );
}

#[test]
pub fn test_pod_protobuf_roundtrip_preserves_env_value_from_sources() {
    use serde_json::json;

    let pod_json = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "env-value-from-pod",
            "namespace": "default",
            "annotations": {
                "mysubpath": "mypath"
            }
        },
        "spec": {
            "containers": [{
                "name": "worker",
                "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1",
                "env": [
                    {
                        "name": "ANNOTATION",
                        "valueFrom": {
                            "fieldRef": {
                                "apiVersion": "v1",
                                "fieldPath": "metadata.annotations['mysubpath']"
                            }
                        }
                    },
                    {
                        "name": "CPU_LIMIT",
                        "valueFrom": {
                            "resourceFieldRef": {
                                "containerName": "worker",
                                "resource": "limits.cpu"
                            }
                        }
                    },
                    {
                        "name": "FROM_CONFIG",
                        "valueFrom": {
                            "configMapKeyRef": {
                                "name": "app-config",
                                "key": "setting",
                                "optional": true
                            }
                        }
                    },
                    {
                        "name": "FROM_SECRET",
                        "valueFrom": {
                            "secretKeyRef": {
                                "name": "app-secret",
                                "key": "password",
                                "optional": false
                            }
                        }
                    }
                ]
            }]
        }
    });

    let protobuf_bytes = encode_protobuf(&pod_json).unwrap();
    let decoded = decode_protobuf(&protobuf_bytes[4..]).unwrap();

    assert_eq!(
        decoded.pointer("/spec/containers/0/env"),
        pod_json.pointer("/spec/containers/0/env"),
        "Pod protobuf response encoding must preserve env.valueFrom so a GET-modify-UPDATE round trip cannot erase fieldRef/resource/configMap/secret env sources"
    );
}

#[test]
pub fn test_pod_protobuf_roundtrip_preserves_required_node_affinity_match_fields() {
    use serde_json::json;

    let pod_json = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "affinity-pod",
            "namespace": "default"
        },
        "spec": {
            "affinity": {
                "nodeAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": {
                        "nodeSelectorTerms": [{
                            "matchFields": [{
                                "key": "metadata.name",
                                "operator": "In",
                                "values": ["mn-leader"]
                            }]
                        }]
                    }
                }
            },
            "containers": [{
                "name": "affinity-pod",
                "image": "registry.k8s.io/pause:3.10.1"
            }]
        }
    });

    let protobuf_bytes = encode_protobuf(&pod_json).unwrap();
    let decoded = decode_protobuf(&protobuf_bytes[4..]).unwrap();

    assert_eq!(
        decoded.pointer("/spec/affinity/nodeAffinity/requiredDuringSchedulingIgnoredDuringExecution/nodeSelectorTerms/0/matchFields/0/key"),
        Some(&json!("metadata.name")),
        "protobuf Pod create must preserve required nodeAffinity matchFields for scheduler predicates"
    );
    assert_eq!(
        decoded.pointer("/spec/affinity/nodeAffinity/requiredDuringSchedulingIgnoredDuringExecution/nodeSelectorTerms/0/matchFields/0/operator"),
        Some(&json!("In"))
    );
    assert_eq!(
        decoded.pointer("/spec/affinity/nodeAffinity/requiredDuringSchedulingIgnoredDuringExecution/nodeSelectorTerms/0/matchFields/0/values/0"),
        Some(&json!("mn-leader"))
    );
}

#[test]
pub fn test_pod_protobuf_roundtrip_preserves_priority_fields() {
    use serde_json::json;

    let pod_json = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "priority-pod",
            "namespace": "default"
        },
        "spec": {
            "priorityClassName": "p4",
            "priority": 4,
            "preemptionPolicy": "Never",
            "containers": [{
                "name": "priority-pod",
                "image": "registry.k8s.io/pause:3.10.1",
                "resources": {
                    "requests": {
                        "example.com/fakecpu": "500"
                    }
                }
            }]
        }
    });

    let protobuf_bytes = encode_protobuf(&pod_json).unwrap();
    let decoded = decode_protobuf(&protobuf_bytes[4..]).unwrap();

    assert_eq!(
        decoded.pointer("/spec/priorityClassName"),
        Some(&json!("p4")),
        "protobuf Pod create must preserve priorityClassName for scheduler preemption"
    );
    assert_eq!(
        decoded.pointer("/spec/priority"),
        Some(&json!(4)),
        "protobuf Pod create must preserve priority for scheduler preemption"
    );
    assert_eq!(
        decoded.pointer("/spec/preemptionPolicy"),
        Some(&json!("Never")),
        "protobuf Pod create must preserve preemptionPolicy"
    );
    assert_eq!(
        decoded.pointer("/spec/containers/0/resources/requests/example.com~1fakecpu"),
        Some(&json!("500")),
        "protobuf Pod create must preserve extended-resource requests"
    );
}

#[test]
pub fn test_replicaset_protobuf_roundtrip_preserves_template_priority_and_extended_request() {
    use serde_json::json;

    let rs_json = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {"name": "rs-priority", "namespace": "default"},
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "rs-priority"}},
            "template": {
                "metadata": {"labels": {"app": "rs-priority"}},
                "spec": {
                    "priorityClassName": "p1",
                    "containers": [{
                        "name": "pod1",
                        "image": "registry.k8s.io/pause:3.10.1",
                        "resources": {
                            "requests": {
                                "example.com/fakecpu": "200"
                            }
                        }
                    }]
                }
            }
        }
    });

    let protobuf_bytes = encode_protobuf(&rs_json).unwrap();
    let decoded = decode_protobuf(&protobuf_bytes[4..]).unwrap();

    assert_eq!(
        decoded.pointer("/spec/template/spec/priorityClassName"),
        Some(&json!("p1")),
        "protobuf ReplicaSet create must preserve template priorityClassName"
    );
    assert_eq!(
        decoded.pointer("/spec/template/spec/containers/0/resources/requests/example.com~1fakecpu"),
        Some(&json!("200")),
        "protobuf ReplicaSet create must preserve template extended-resource requests"
    );
}

#[test]
pub fn test_encode_protobuf_pod_conditions_roundtrip() {
    use serde_json::json;

    let pod_json = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "ready-pod",
            "namespace": "default",
            "uid": "uid-cond"
        },
        "spec": {
            "containers": [{
                "name": "app",
                "image": "nginx"
            }]
        },
        "status": {
            "phase": "Running",
            "podIP": "10.43.0.5",
            "podIPs": [{"ip": "10.43.0.5"}],
            "hostIP": "192.168.1.1",
            "hostIPs": [{"ip": "192.168.1.1"}],
            "qosClass": "BestEffort",
            "conditions": [
                {
                    "type": "PodScheduled",
                    "status": "True",
                    "lastTransitionTime": "2026-04-12T00:00:00Z"
                },
                {
                    "type": "Initialized",
                    "status": "True",
                    "lastTransitionTime": "2026-04-12T00:00:01Z"
                },
                {
                    "type": "ContainersReady",
                    "status": "True",
                    "lastTransitionTime": "2026-04-12T00:00:02Z"
                },
                {
                    "type": "Ready",
                    "status": "True",
                    "lastTransitionTime": "2026-04-12T00:00:02Z"
                }
            ],
            "containerStatuses": [{
                "name": "app",
                "ready": true,
                "restartCount": 0,
                "image": "nginx:latest",
                "imageID": "sha256:abc",
                "containerID": "containerd://xyz",
                "state": {"running": {"startedAt": "2026-04-12T00:00:02Z"}}
            }]
        }
    });

    let protobuf_bytes = encode_protobuf(&pod_json).unwrap();
    let decoded = decode_protobuf(&protobuf_bytes[4..]).unwrap();

    // Verify conditions survive the roundtrip
    let conditions = decoded["status"]["conditions"].as_array().unwrap();
    assert_eq!(conditions.len(), 4, "All 4 conditions must be preserved");

    // Verify Ready condition specifically (this is what Sonobuoy checks)
    let ready = conditions
        .iter()
        .find(|c| c["type"] == "Ready")
        .expect("Ready condition must exist");
    assert_eq!(ready["status"], "True");

    // Verify other conditions
    let initialized = conditions
        .iter()
        .find(|c| c["type"] == "Initialized")
        .expect("Initialized condition must exist");
    assert_eq!(initialized["status"], "True");

    let containers_ready = conditions
        .iter()
        .find(|c| c["type"] == "ContainersReady")
        .expect("ContainersReady condition must exist");
    assert_eq!(containers_ready["status"], "True");

    // Verify other status fields
    assert_eq!(decoded["status"]["phase"], "Running");
    assert_eq!(decoded["status"]["podIP"], "10.43.0.5");
    assert_eq!(decoded["status"]["podIPs"][0]["ip"], "10.43.0.5");
    assert_eq!(decoded["status"]["hostIP"], "192.168.1.1");
    assert_eq!(decoded["status"]["hostIPs"][0]["ip"], "192.168.1.1");
    assert_eq!(decoded["status"]["qosClass"], "BestEffort");
}

#[test]
pub fn test_encode_protobuf_pod_init_container_statuses_roundtrip() {
    use serde_json::json;

    let pod_json = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "init-pod",
            "namespace": "default",
            "uid": "uid-init"
        },
        "spec": {
            "initContainers": [{
                "name": "init",
                "image": "busybox"
            }],
            "containers": [{
                "name": "app",
                "image": "nginx"
            }]
        },
        "status": {
            "phase": "Running",
            "initContainerStatuses": [{
                "name": "init",
                "ready": true,
                "restartCount": 0,
                "image": "busybox:latest",
                "imageID": "sha256:init-abc",
                "containerID": "containerd://init-xyz",
                "state": {"terminated": {"exitCode": 0, "startedAt": "2026-04-12T00:00:00Z", "finishedAt": "2026-04-12T00:00:01Z"}}
            }],
            "containerStatuses": [{
                "name": "app",
                "ready": true,
                "restartCount": 0,
                "image": "nginx:latest",
                "imageID": "sha256:abc",
                "containerID": "containerd://xyz",
                "state": {"running": {"startedAt": "2026-04-12T00:00:02Z"}}
            }]
        }
    });

    let protobuf_bytes = encode_protobuf(&pod_json).unwrap();
    let decoded = decode_protobuf(&protobuf_bytes[4..]).unwrap();

    let init_statuses = decoded["status"]["initContainerStatuses"]
        .as_array()
        .unwrap();
    assert_eq!(init_statuses.len(), 1);
    assert_eq!(init_statuses[0]["name"], "init");
    assert_eq!(init_statuses[0]["ready"], true);
}

#[test]
pub fn test_container_status_running_state_roundtrip() {
    // Verifies that state.running survives JSON→protobuf→JSON roundtrip.
    // Before fix: json_container_status_to_pb omitted state → decoded state was {}.
    use serde_json::json;

    let pod_json = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "running-pod", "namespace": "default", "uid": "uid-running"},
        "spec": {"containers": [{"name": "app", "image": "nginx"}]},
        "status": {
            "phase": "Running",
            "containerStatuses": [{
                "name": "app",
                "ready": true,
                "started": true,
                "restartCount": 0,
                "image": "nginx:latest",
                "imageID": "sha256:abc",
                "containerID": "containerd://abc123",
                "state": {
                    "running": {"startedAt": "2026-04-12T10:00:00Z"}
                }
            }]
        }
    });

    let pb_bytes = encode_protobuf(&pod_json).unwrap();
    let decoded = decode_protobuf(&pb_bytes[4..]).unwrap();

    let cs = &decoded["status"]["containerStatuses"][0];
    let state = &cs["state"];

    assert!(
        state.is_object() && !state.as_object().unwrap().is_empty(),
        "state must not be empty {{}}, got: {:?}",
        state
    );
    assert!(
        state.get("running").is_some(),
        "state.running must be present after roundtrip, got: {:?}",
        state
    );
    assert_eq!(
        state["running"]["startedAt"], "2026-04-12T10:00:00Z",
        "startedAt must survive roundtrip"
    );
    assert!(
        state.get("terminated").is_none(),
        "must not have terminated"
    );
    assert!(state.get("waiting").is_none(), "must not have waiting");
}

#[test]
pub fn test_container_status_terminated_state_roundtrip() {
    use serde_json::json;

    let pod_json = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "exited-pod", "namespace": "default", "uid": "uid-exited"},
        "spec": {"containers": [{"name": "app", "image": "busybox"}]},
        "status": {
            "phase": "Failed",
            "containerStatuses": [{
                "name": "app",
                "ready": false,
                "started": false,
                "restartCount": 2,
                "image": "busybox:latest",
                "imageID": "sha256:def",
                "containerID": "containerd://def456",
                "state": {
                    "terminated": {
                        "exitCode": 1,
                        "reason": "Error",
                        "message": "container exited with error",
                        "startedAt": "2026-04-12T10:00:00Z",
                        "finishedAt": "2026-04-12T10:01:00Z"
                    }
                }
            }]
        }
    });

    let pb_bytes = encode_protobuf(&pod_json).unwrap();
    let decoded = decode_protobuf(&pb_bytes[4..]).unwrap();

    let state = &decoded["status"]["containerStatuses"][0]["state"];

    assert!(
        state.is_object() && !state.as_object().unwrap().is_empty(),
        "state must not be empty {{}}"
    );
    let terminated = state
        .get("terminated")
        .expect("state.terminated must be present");
    assert_eq!(terminated["exitCode"], 1, "exitCode must survive roundtrip");
    assert_eq!(
        terminated["reason"], "Error",
        "reason must survive roundtrip"
    );
    assert_eq!(
        terminated["message"], "container exited with error",
        "message must survive roundtrip"
    );
    assert_eq!(terminated["startedAt"], "2026-04-12T10:00:00Z");
    assert_eq!(terminated["finishedAt"], "2026-04-12T10:01:00Z");
    assert!(state.get("running").is_none());
    assert!(state.get("waiting").is_none());
}

#[test]
pub fn test_container_status_waiting_state_roundtrip() {
    use serde_json::json;

    let pod_json = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "waiting-pod", "namespace": "default", "uid": "uid-waiting"},
        "spec": {"containers": [{"name": "app", "image": "nginx"}]},
        "status": {
            "phase": "Pending",
            "containerStatuses": [{
                "name": "app",
                "ready": false,
                "started": false,
                "restartCount": 0,
                "image": "nginx:latest",
                "imageID": "",
                "state": {
                    "waiting": {
                        "reason": "ContainerCreating",
                        "message": "waiting for volume"
                    }
                }
            }]
        }
    });

    let pb_bytes = encode_protobuf(&pod_json).unwrap();
    let decoded = decode_protobuf(&pb_bytes[4..]).unwrap();

    let state = &decoded["status"]["containerStatuses"][0]["state"];

    assert!(
        state.is_object() && !state.as_object().unwrap().is_empty(),
        "state must not be empty {{}}"
    );
    let waiting = state.get("waiting").expect("state.waiting must be present");
    assert_eq!(waiting["reason"], "ContainerCreating");
    assert_eq!(waiting["message"], "waiting for volume");
    assert!(state.get("running").is_none());
    assert!(state.get("terminated").is_none());
}

#[test]
pub fn test_container_status_crashloopbackoff_state_roundtrip() {
    // CrashLoopBackOff uses waiting state — critical for Sonobuoy pod failure reporting
    use serde_json::json;

    let pod_json = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "crash-pod", "namespace": "default", "uid": "uid-crash"},
        "spec": {"containers": [{"name": "app", "image": "bad-image"}]},
        "status": {
            "phase": "Running",
            "containerStatuses": [{
                "name": "app",
                "ready": false,
                "started": false,
                "restartCount": 5,
                "image": "bad-image:latest",
                "imageID": "",
                "state": {
                    "waiting": {
                        "reason": "CrashLoopBackOff",
                        "message": "back-off 5m0s restarting failed container"
                    }
                }
            }]
        }
    });

    let pb_bytes = encode_protobuf(&pod_json).unwrap();
    let decoded = decode_protobuf(&pb_bytes[4..]).unwrap();

    let state = &decoded["status"]["containerStatuses"][0]["state"];
    let waiting = state
        .get("waiting")
        .expect("CrashLoopBackOff must encode as waiting state");
    assert_eq!(waiting["reason"], "CrashLoopBackOff");
    assert!(!waiting["message"].as_str().unwrap_or("").is_empty());
}

#[test]
pub fn test_container_status_running_state_roundtrip_real_sonobuoy_pod() {
    // Reproduces exact Sonobuoy pod format to catch state:{} deserialization issues.
    // Uses microsecond timestamps and sha256: imageID format from real pods.
    use serde_json::json;

    let pod_json = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "pod-handle-http-request",
            "namespace": "container-lifecycle-hook-3595",
            "uid": "uid-sono",
            "annotations": {
                "klights.dev/sandbox-id": "a0471ff379fe8766c8d123c722d1a599"
            }
        },
        "spec": {
            "containers": [
                {
                    "name": "container-handle-http-request",
                    "image": "registry.k8s.io/e2e-test-images/agnhost:2.56",
                    "args": ["netexec"],
                    "ports": [{"containerPort": 8080, "protocol": "TCP"}]
                },
                {
                    "name": "container-handle-https-request",
                    "image": "registry.k8s.io/e2e-test-images/agnhost:2.56",
                    "args": ["netexec", "--http-port", "9090"],
                    "ports": [{"containerPort": 9090, "protocol": "TCP"}]
                }
            ],
            "nodeName": "dp"
        },
        "status": {
            "phase": "Running",
            "podIP": "10.43.0.27",
            "hostIP": "127.0.0.1",
            "conditions": [
                {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-04-12T01:46:36.658533Z"},
                {"type": "Ready", "status": "True", "lastTransitionTime": "2026-04-12T01:46:40.844246Z"},
                {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-04-12T01:46:40.844246Z"},
                {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-04-12T01:46:36.658537Z"}
            ],
            "containerStatuses": [
                {
                    "name": "container-handle-http-request",
                    "containerID": "containerd://c52d442bb7ab7eb592402ba0c07c1de0d7b21bd87e79307cae98b92623b70f1e",
                    "image": "registry.k8s.io/e2e-test-images/agnhost:2.56",
                    "imageID": "docker.io/library/registry.k8s.io/e2e-test-images/agnhost:2.56",
                    "ready": true,
                    "started": true,
                    "restartCount": 0,
                    "state": {
                        "running": {"startedAt": "2026-04-12T01:46:40.543664Z"}
                    }
                },
                {
                    "name": "container-handle-https-request",
                    "containerID": "containerd://28d1abcec6ce87440d83d25ce54c84f81b70828de79b434b3657f8fcc124092f",
                    "image": "registry.k8s.io/e2e-test-images/agnhost:2.56",
                    "imageID": "sha256:676de2b863ea3fd8526c98d7ee908e537431841396b58d450c43f6a6166ae447",
                    "ready": true,
                    "started": true,
                    "restartCount": 0,
                    "state": {
                        "running": {"startedAt": "2026-04-12T01:46:40.739530Z"}
                    }
                }
            ]
        }
    });

    let pb_bytes = encode_protobuf(&pod_json).unwrap();
    let decoded = decode_protobuf(&pb_bytes[4..]).unwrap();

    // Check BOTH containers have proper state
    for i in 0..2 {
        let cs = &decoded["status"]["containerStatuses"][i];
        let state = &cs["state"];
        assert!(
            state.is_object() && !state.as_object().unwrap().is_empty(),
            "container {} state must not be empty {{}}, got: {:?}",
            i,
            state
        );
        assert!(
            state.get("running").is_some(),
            "container {} state.running must be present, got: {:?}",
            i,
            state
        );
    }
}

#[test]
pub fn test_pod_container_port_hostip_roundtrip_preserved() {
    use serde_json::json;

    let pod_json = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "hostport-pod",
            "namespace": "default"
        },
        "spec": {
            "containers": [{
                "name": "agnhost",
                "image": "registry.k8s.io/e2e-test-images/agnhost:2.56",
                "ports": [{
                    "containerPort": 8080,
                    "hostPort": 54323,
                    "hostIP": "127.0.0.1",
                    "protocol": "TCP"
                }]
            }]
        }
    });

    let pb_bytes = encode_protobuf(&pod_json).unwrap();
    let decoded = decode_protobuf(&pb_bytes[4..]).unwrap();
    assert_eq!(
        decoded["spec"]["containers"][0]["ports"][0]["hostIP"], "127.0.0.1",
        "container port hostIP must survive protobuf roundtrip"
    );
    assert_eq!(
        decoded["spec"]["containers"][0]["ports"][0]["hostPort"],
        54323
    );
}

#[test]
pub fn test_container_status_state_with_full_sonobuoy_pod_spec() {
    // Reproduces the EXACT pod structure from Sonobuoy lifecycle_hook.go failure.
    // Includes resources: {}, volumeMounts, projected volumes, ports — all fields
    // that the actual Sonobuoy test pod has. If state.running is lost during
    // protobuf encode, this test will catch it.
    use serde_json::json;

    let pod_json = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "pod-handle-http-request",
            "namespace": "container-lifecycle-hook-3595",
            "uid": "test-uid-1234",
            "creationTimestamp": "2026-04-12T01:46:36.658533Z",
            "generation": 1,
            "annotations": {
                "klights.dev/sandbox-id": "a0471ff379fe8766c8d123c722d1a599"
            }
        },
        "spec": {
            "containers": [
                {
                    "name": "container-handle-http-request",
                    "image": "registry.k8s.io/e2e-test-images/agnhost:2.56",
                    "args": ["netexec"],
                    "ports": [{"containerPort": 8080, "protocol": "TCP"}],
                    "resources": {},
                    "volumeMounts": [{
                        "mountPath": "/var/run/secrets/kubernetes.io/serviceaccount",
                        "name": "kube-api-access-5w0ts",
                        "readOnly": true
                    }]
                },
                {
                    "name": "container-handle-https-request",
                    "image": "registry.k8s.io/e2e-test-images/agnhost:2.56",
                    "args": ["netexec", "--http-port", "9090", "--udp-port", "9091",
                             "--tls-cert-file", "/localhost.crt", "--tls-private-key-file", "/localhost.key"],
                    "ports": [{"containerPort": 9090, "protocol": "TCP"}],
                    "resources": {},
                    "volumeMounts": [{
                        "mountPath": "/var/run/secrets/kubernetes.io/serviceaccount",
                        "name": "kube-api-access-5w0ts",
                        "readOnly": true
                    }]
                }
            ],
            "nodeName": "dp",
            "terminationGracePeriodSeconds": 30,
            "volumes": [{
                "name": "kube-api-access-5w0ts",
                "projected": {
                    "defaultMode": 420,
                    "sources": [
                        {"serviceAccountToken": {"expirationSeconds": 3607, "path": "token"}},
                        {"configMap": {"name": "kube-root-ca.crt", "items": [{"key": "ca.crt", "path": "ca.crt"}]}},
                        {"downwardAPI": {"items": [{"fieldRef": {"apiVersion": "v1", "fieldPath": "metadata.namespace"}, "path": "namespace"}]}}
                    ]
                }
            }]
        },
        "status": {
            "phase": "Running",
            "podIP": "10.43.0.27",
            "hostIP": "127.0.0.1",
            "conditions": [
                {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-04-12T01:46:36.658533Z"},
                {"type": "Ready", "status": "True", "lastTransitionTime": "2026-04-12T01:46:40.844246Z"},
                {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-04-12T01:46:40.844246Z"},
                {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-04-12T01:46:36.658537Z"}
            ],
            "containerStatuses": [
                {
                    "name": "container-handle-http-request",
                    "containerID": "containerd://c52d442bb7ab7eb592402ba0c07c1de0d7b21bd87e79307cae98b92623b70f1e",
                    "image": "registry.k8s.io/e2e-test-images/agnhost:2.56",
                    "imageID": "sha256:676de2b863ea3fd8526c98d7ee908e537431841396b58d450c43f6a6166ae447",
                    "ready": true,
                    "started": true,
                    "restartCount": 0,
                    "state": {"running": {"startedAt": "2026-04-12T01:46:40.543664Z"}}
                },
                {
                    "name": "container-handle-https-request",
                    "containerID": "containerd://28d1abcec6ce87440d83d25ce54c84f81b70828de79b434b3657f8fcc124092f",
                    "image": "registry.k8s.io/e2e-test-images/agnhost:2.56",
                    "imageID": "sha256:676de2b863ea3fd8526c98d7ee908e537431841396b58d450c43f6a6166ae447",
                    "ready": true,
                    "started": true,
                    "restartCount": 0,
                    "state": {"running": {"startedAt": "2026-04-12T01:46:40.739530Z"}}
                }
            ],
            "qosClass": "BestEffort"
        }
    });

    // Step 1: Verify serde_json can parse into k8s_openapi::Pod
    let openapi_pod: k8s_openapi::api::core::v1::Pod =
        serde_json::from_value(pod_json.clone()).expect("Must parse into k8s_openapi::Pod");

    // Step 2: Verify state.running is present in k8s_openapi
    let status = openapi_pod.status.as_ref().expect("status must be present");
    let container_statuses = status
        .container_statuses
        .as_ref()
        .expect("containerStatuses must be present");
    for (i, cs) in container_statuses.iter().enumerate() {
        let state = cs
            .state
            .as_ref()
            .unwrap_or_else(|| panic!("container {} state must be Some, not None", i));
        assert!(
            state.running.is_some(),
            "container {} state.running must be Some in k8s_openapi, got running={:?} terminated={:?} waiting={:?}",
            i,
            state.running,
            state.terminated,
            state.waiting
        );
    }

    // Step 3: Verify protobuf roundtrip preserves state.running
    let pb_bytes = encode_protobuf(&pod_json).unwrap();
    let decoded = decode_protobuf(&pb_bytes[4..]).unwrap();

    for i in 0..2 {
        let cs = &decoded["status"]["containerStatuses"][i];
        let state = &cs["state"];
        assert!(
            state.is_object() && !state.as_object().unwrap().is_empty(),
            "container {} decoded state must not be empty {{}}, got: {:?}",
            i,
            state
        );
        assert!(
            state.get("running").is_some(),
            "container {} decoded state.running must be present, got: {:?}",
            i,
            state
        );
    }
}

#[test]
pub fn test_container_status_state_with_klights_specific_fields() {
    // Tests that k8s_openapi deserialization handles klights-specific fields:
    // - restartPolicy: "" (empty string, klights default)
    // - serviceAccountName: "" (empty string)
    // - Custom annotations (klights.dev/sandbox-id)
    // - terminationGracePeriodSeconds as integer
    // These could cause serde_json::from_value::<Pod> to fail,
    // triggering the JSON-in-Unknown fallback that corrupts state.
    use serde_json::json;

    let pod_json = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "test-pod",
            "namespace": "test-ns",
            "uid": "uid-123",
            "creationTimestamp": "2026-04-12T01:46:36.658533Z",
            "generation": 1,
            "annotations": {
                "klights.dev/sandbox-id": "abc123"
            }
        },
        "spec": {
            "containers": [{
                "name": "app",
                "image": "nginx",
                "resources": {},
                "volumeMounts": [{
                    "mountPath": "/var/run/secrets/kubernetes.io/serviceaccount",
                    "name": "kube-api-access-xyz",
                    "readOnly": true
                }]
            }],
            "restartPolicy": "",
            "serviceAccountName": "",
            "nodeName": "dp",
            "terminationGracePeriodSeconds": 30,
            "volumes": [{
                "name": "kube-api-access-xyz",
                "projected": {
                    "defaultMode": 420,
                    "sources": [
                        {"serviceAccountToken": {"expirationSeconds": 3607, "path": "token"}},
                        {"configMap": {"name": "kube-root-ca.crt", "items": [{"key": "ca.crt", "path": "ca.crt"}]}},
                        {"downwardAPI": {"items": [{"fieldRef": {"apiVersion": "v1", "fieldPath": "metadata.namespace"}, "path": "namespace"}]}}
                    ]
                }
            }]
        },
        "status": {
            "phase": "Running",
            "podIP": "10.43.0.5",
            "hostIP": "127.0.0.1",
            "qosClass": "BestEffort",
            "conditions": [
                {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-04-12T01:46:36Z"},
                {"type": "Ready", "status": "True", "lastTransitionTime": "2026-04-12T01:46:40Z"},
                {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-04-12T01:46:40Z"},
                {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-04-12T01:46:36Z"}
            ],
            "containerStatuses": [{
                "name": "app",
                "containerID": "containerd://abc123",
                "image": "nginx",
                "imageID": "sha256:abc",
                "ready": true,
                "started": true,
                "restartCount": 0,
                "state": {"running": {"startedAt": "2026-04-12T01:46:40Z"}}
            }]
        }
    });

    // This is the critical test: can serde_json::from_value succeed?
    // If it fails, encode_protobuf falls back to JSON-in-Unknown, causing state: {} in Go client.
    let result = serde_json::from_value::<k8s_openapi::api::core::v1::Pod>(pod_json.clone());
    assert!(
        result.is_ok(),
        "serde_json::from_value must succeed for klights pod JSON: {:?}",
        result.err()
    );

    let openapi_pod = result.unwrap();
    let cs = &openapi_pod
        .status
        .as_ref()
        .unwrap()
        .container_statuses
        .as_ref()
        .unwrap()[0];
    assert!(
        cs.state.as_ref().unwrap().running.is_some(),
        "state.running must survive k8s_openapi deserialization"
    );

    // Full protobuf roundtrip
    let pb_bytes = encode_protobuf(&pod_json).unwrap();
    let decoded = decode_protobuf(&pb_bytes[4..]).unwrap();
    let state = &decoded["status"]["containerStatuses"][0]["state"];
    assert!(
        state.get("running").is_some(),
        "state.running must survive full protobuf roundtrip with klights fields, got: {:?}",
        state
    );
}

// F1-01: single NetworkPolicy must roundtrip through the public protobuf
// encode/decode path. Before this task, only NetworkPolicyList was registered;
// a single namespaced NetworkPolicy GET over protobuf would EOF.
#[test]
pub fn roundtrip_networking_v1_networkpolicy() {
    use serde_json::json;

    let np_json = json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {
            "name": "allow-frontend",
            "namespace": "default"
        },
        "spec": {
            "podSelector": {
                "matchLabels": {"app": "backend"}
            },
            "policyTypes": ["Ingress", "Egress"],
            "ingress": [{
                "from": [{
                    "podSelector": {"matchLabels": {"role": "frontend"}}
                }],
                "ports": [{"protocol": "TCP", "port": 8080}]
            }],
            "egress": [{
                "to": [{
                    "ipBlock": {"cidr": "10.0.0.0/24", "except": ["10.0.0.5/32"]}
                }],
                "ports": [{"protocol": "TCP", "port": 5432}]
            }]
        }
    });

    let pb_bytes = encode_protobuf(&np_json).unwrap();
    assert_eq!(
        &pb_bytes[0..4],
        &[0x6b, 0x38, 0x73, 0x00],
        "encoded NetworkPolicy must wear the k8s magic prefix"
    );
    let decoded = decode_protobuf(&pb_bytes[4..]).unwrap();

    assert_eq!(decoded["apiVersion"], "networking.k8s.io/v1");
    assert_eq!(decoded["kind"], "NetworkPolicy");
    assert_eq!(decoded["metadata"]["name"], "allow-frontend");
    assert_eq!(decoded["metadata"]["namespace"], "default");
    assert_eq!(
        decoded["spec"]["podSelector"]["matchLabels"]["app"],
        "backend"
    );
    let policy_types = decoded["spec"]["policyTypes"]
        .as_array()
        .expect("policyTypes must roundtrip as array");
    assert!(policy_types.iter().any(|v| v == "Ingress"));
    assert!(policy_types.iter().any(|v| v == "Egress"));

    let ingress = &decoded["spec"]["ingress"][0];
    assert_eq!(
        ingress["from"][0]["podSelector"]["matchLabels"]["role"],
        "frontend"
    );
    assert_eq!(ingress["ports"][0]["protocol"], "TCP");
    assert_eq!(ingress["ports"][0]["port"], 8080);

    let egress = &decoded["spec"]["egress"][0];
    assert_eq!(egress["to"][0]["ipBlock"]["cidr"], "10.0.0.0/24");
    assert_eq!(egress["to"][0]["ipBlock"]["except"][0], "10.0.0.5/32");
    assert_eq!(egress["ports"][0]["protocol"], "TCP");
    assert_eq!(egress["ports"][0]["port"], 5432);
}

// F3-03: encode_message_to_vec must pre-size capacity to encoded_len so prost
// writes in place without reallocations. The exact-fit guarantee is the
// load-bearing property — a default Vec::new() doubles 4-6 times for typical
// pod payloads.
#[test]
pub fn encode_message_to_vec_presizes_exact_capacity() {
    use crate::protobuf::encode_message_to_vec;
    use k8s_pb::api::core::v1 as corev1;
    use k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use prost::Message;

    let pod = corev1::Pod {
        metadata: Some(ObjectMeta {
            name: Some("presize-pod".to_string()),
            namespace: Some("default".to_string()),
            uid: Some("uid-presize".to_string()),
            ..Default::default()
        }),
        spec: Some(corev1::PodSpec::default()),
        status: Some(corev1::PodStatus::default()),
    };

    let buf = encode_message_to_vec(&pod).expect("encode must succeed");
    assert_eq!(
        buf.capacity(),
        pod.encoded_len(),
        "F3-03 contract: capacity must equal encoded_len so encode writes in place"
    );
    assert_eq!(buf.len(), pod.encoded_len());
}
