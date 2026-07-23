use crate::protobuf::*;
use prost::Message;

#[test]
pub fn test_encode_protobuf_fallback_content_type() {
    // When encode_protobuf_resource fails (e.g., unknown kind), the fallback
    // wraps JSON bytes in Unknown envelope with content_type: "".
    // The Go client sees empty content_type and tries to decode raw as protobuf.
    // JSON bytes are NOT valid protobuf — Go gets garbage, possibly state: {}.
    //
    // This test verifies that for "Pod" kind, encode_protobuf_resource ALWAYS succeeds
    // (never falls through to the JSON-in-Unknown fallback).
    use serde_json::json;

    // Even with unusual but valid fields, Pod encode must succeed
    let pod_json = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "test-pod",
            "namespace": "default",
            "uid": "uid-1",
            "creationTimestamp": "2026-04-12T00:00:00.000000Z",
            "generation": 1,
            "labels": {"app": "test", "version": "v1"},
            "annotations": {
                "klights.dev/sandbox-id": "sandbox123",
                "some.other/annotation": "value"
            },
            "ownerReferences": [{
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "name": "test-rs",
                "uid": "rs-uid",
                "controller": true,
                "blockOwnerDeletion": true
            }],
            "finalizers": ["some-finalizer"]
        },
        "spec": {
            "containers": [{
                "name": "app",
                "image": "nginx:1.21",
                "command": ["/bin/sh"],
                "args": ["-c", "sleep 3600"],
                "env": [{"name": "FOO", "value": "bar"}],
                "ports": [{"containerPort": 8080, "protocol": "TCP", "name": "http"}],
                "resources": {
                    "requests": {"cpu": "100m", "memory": "128Mi"},
                    "limits": {"cpu": "200m", "memory": "256Mi"}
                },
                "volumeMounts": [
                    {"name": "data", "mountPath": "/data"},
                    {"name": "sa", "mountPath": "/var/run/secrets/kubernetes.io/serviceaccount", "readOnly": true}
                ],
                "livenessProbe": {
                    "httpGet": {"path": "/healthz", "port": 8080},
                    "initialDelaySeconds": 5,
                    "periodSeconds": 10
                },
                "readinessProbe": {
                    "httpGet": {"path": "/ready", "port": 8080},
                    "initialDelaySeconds": 3,
                    "periodSeconds": 5
                },
                "lifecycle": {
                    "preStop": {"exec": {"command": ["/bin/sh", "-c", "sleep 5"]}}
                },
                "securityContext": {
                    "runAsNonRoot": true,
                    "runAsUser": 1000
                }
            }],
            "restartPolicy": "Always",
            "serviceAccountName": "default",
            "nodeName": "dp",
            "terminationGracePeriodSeconds": 30,
            "dnsPolicy": "ClusterFirst",
            "volumes": [
                {"name": "data", "emptyDir": {}},
                {"name": "sa", "projected": {
                    "defaultMode": 420,
                    "sources": [
                        {"serviceAccountToken": {"expirationSeconds": 3607, "path": "token"}},
                        {"configMap": {"name": "kube-root-ca.crt", "items": [{"key": "ca.crt", "path": "ca.crt"}]}}
                    ]
                }}
            ]
        },
        "status": {
            "phase": "Running",
            "podIP": "10.43.0.5",
            "hostIP": "127.0.0.1",
            "qosClass": "Burstable",
            "startTime": "2026-04-12T00:00:01Z",
            "conditions": [
                {"type": "Initialized", "status": "True", "lastTransitionTime": "2026-04-12T00:00:00Z"},
                {"type": "Ready", "status": "True", "lastTransitionTime": "2026-04-12T00:00:02Z"},
                {"type": "ContainersReady", "status": "True", "lastTransitionTime": "2026-04-12T00:00:02Z"},
                {"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-04-12T00:00:00Z"}
            ],
            "containerStatuses": [{
                "name": "app",
                "containerID": "containerd://container123",
                "image": "nginx:1.21",
                "imageID": "sha256:abc123",
                "ready": true,
                "started": true,
                "restartCount": 2,
                "state": {"running": {"startedAt": "2026-04-12T00:00:02Z"}}
            }]
        }
    });

    // The critical assertion: encode_protobuf_resource must succeed for Pod
    // If it fails, we fall back to JSON-in-Unknown which corrupts Go decoding
    let result = encode_protobuf_resource("Pod", &pod_json);
    assert!(
        result.is_ok(),
        "encode_protobuf_resource(\"Pod\") must succeed for complex pod, but got: {:?}",
        result.err()
    );

    // Also verify full roundtrip
    let pb_bytes = encode_protobuf(&pod_json).unwrap();
    let decoded = decode_protobuf(&pb_bytes[4..]).unwrap();
    let state = &decoded["status"]["containerStatuses"][0]["state"];
    assert!(
        state.get("running").is_some(),
        "state.running must survive roundtrip for complex pod, got: {:?}",
        state
    );
}

#[test]
pub fn test_raw_container_state_to_pb_running() {
    let state = serde_json::json!({"running": {"startedAt": "2026-04-12T10:00:00Z"}});
    let pb = raw_container_state_to_pb(&state);
    assert!(pb.running.is_some(), "running must be Some");
    assert!(pb.terminated.is_none());
    assert!(pb.waiting.is_none());
    assert!(pb.running.unwrap().started_at.is_some());
}

#[test]
pub fn test_raw_container_state_to_pb_terminated() {
    let state = serde_json::json!({"terminated": {
        "exitCode": 1, "signal": 15, "reason": "Error",
        "startedAt": "2026-04-12T10:00:00Z",
        "finishedAt": "2026-04-12T10:01:00Z",
        "containerID": "containerd://abc"
    }});
    let pb = raw_container_state_to_pb(&state);
    assert!(pb.running.is_none());
    assert!(pb.waiting.is_none());
    let t = pb.terminated.unwrap();
    assert_eq!(t.exit_code, Some(1));
    assert_eq!(t.signal, Some(15));
    assert_eq!(t.reason.as_deref(), Some("Error"));
    assert_eq!(t.container_id.as_deref(), Some("containerd://abc"));
}

#[test]
pub fn test_raw_container_state_to_pb_waiting() {
    let state =
        serde_json::json!({"waiting": {"reason": "ImagePullBackOff", "message": "pulling..."}});
    let pb = raw_container_state_to_pb(&state);
    assert!(pb.running.is_none());
    assert!(pb.terminated.is_none());
    let w = pb.waiting.unwrap();
    assert_eq!(w.reason.as_deref(), Some("ImagePullBackOff"));
    assert_eq!(w.message.as_deref(), Some("pulling..."));
}

#[test]
pub fn test_raw_container_status_to_pb_preserves_all_fields() {
    let cs = serde_json::json!({
        "name": "web",
        "containerID": "containerd://xyz",
        "image": "nginx:1.21",
        "imageID": "sha256:abc123",
        "ready": true,
        "started": true,
        "restartCount": 2,
        "state": {"running": {"startedAt": "2026-04-12T10:00:00Z"}},
        "lastState": {"terminated": {"exitCode": 0, "reason": "Completed"}}
    });
    let pb = raw_container_status_to_pb(&cs);
    assert_eq!(pb.name.as_deref(), Some("web"));
    assert_eq!(pb.container_id.as_deref(), Some("containerd://xyz"));
    assert_eq!(pb.image.as_deref(), Some("nginx:1.21"));
    assert_eq!(pb.image_id.as_deref(), Some("sha256:abc123"));
    assert_eq!(pb.ready, Some(true));
    assert_eq!(pb.started, Some(true));
    assert_eq!(pb.restart_count, Some(2));
    assert!(pb.state.as_ref().unwrap().running.is_some());
    assert!(pb.last_state.as_ref().unwrap().terminated.is_some());
}

#[test]
pub fn test_protobuf_wire_format_contains_state_running() {
    // Verify the actual protobuf wire bytes contain the state.running field.
    // This catches issues where our Rust decoder compensates for encoding bugs
    // that the Go decoder doesn't.
    use prost::Message;
    use serde_json::json;

    let pod_json = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "test", "namespace": "default", "uid": "uid-1"},
        "spec": {"containers": [{"name": "app", "image": "nginx"}]},
        "status": {
            "phase": "Running",
            "containerStatuses": [{
                "name": "app",
                "containerID": "containerd://abc123",
                "image": "nginx",
                "imageID": "sha256:abc",
                "ready": true,
                "started": true,
                "restartCount": 0,
                "state": {"running": {"startedAt": "2026-04-12T10:00:00Z"}}
            }]
        }
    });

    let pb_bytes = encode_protobuf(&pod_json).unwrap();

    // Skip 4-byte magic prefix, decode Unknown envelope
    let unknown = Unknown::decode(&pb_bytes[4..]).unwrap();

    // Decode the raw Pod protobuf
    let pod_pb = k8s_pb::api::core::v1::Pod::decode(unknown.raw.as_slice()).unwrap();

    // Navigate to container status
    let status = pod_pb.status.as_ref().expect("Pod.status must be Some");
    assert!(
        !status.container_statuses.is_empty(),
        "containerStatuses must be non-empty"
    );

    let cs = &status.container_statuses[0];
    assert_eq!(cs.name.as_deref(), Some("app"), "container name");
    assert_eq!(cs.ready, Some(true), "container ready");

    // THE CRITICAL CHECK: state must be Some with running: Some
    let state = cs
        .state
        .as_ref()
        .expect("ContainerStatus.state must be Some, not None");
    assert!(
        state.running.is_some(),
        "ContainerState.running must be Some in protobuf wire format. Got: running={:?}, terminated={:?}, waiting={:?}",
        state.running,
        state.terminated,
        state.waiting
    );

    let running = state.running.as_ref().unwrap();
    assert!(
        running.started_at.is_some(),
        "startedAt must be present in running state"
    );
}

#[test]
pub fn test_protobuf_raw_json_preserves_terminated_state() {
    use prost::Message;
    use serde_json::json;

    let pod_json = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "exited", "namespace": "default", "uid": "uid-2"},
        "spec": {"containers": [{"name": "job", "image": "busybox"}]},
        "status": {
            "phase": "Succeeded",
            "containerStatuses": [{
                "name": "job",
                "containerID": "containerd://def456",
                "image": "busybox",
                "imageID": "sha256:def",
                "ready": false,
                "started": false,
                "restartCount": 0,
                "state": {"terminated": {
                    "exitCode": 0,
                    "reason": "Completed",
                    "startedAt": "2026-04-12T10:00:00Z",
                    "finishedAt": "2026-04-12T10:05:00Z"
                }}
            }]
        }
    });

    let pb_bytes = encode_protobuf(&pod_json).unwrap();
    let unknown = Unknown::decode(&pb_bytes[4..]).unwrap();
    let pod_pb = k8s_pb::api::core::v1::Pod::decode(unknown.raw.as_slice()).unwrap();

    let cs = &pod_pb.status.as_ref().unwrap().container_statuses[0];
    let state = cs.state.as_ref().expect("state must be Some");
    let terminated = state.terminated.as_ref().expect("terminated must be Some");
    assert_eq!(terminated.exit_code, Some(0));
    assert_eq!(terminated.reason.as_deref(), Some("Completed"));
    assert!(terminated.started_at.is_some());
    assert!(terminated.finished_at.is_some());
}

#[test]
pub fn test_protobuf_raw_json_preserves_waiting_state() {
    use prost::Message;
    use serde_json::json;

    let pod_json = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "pending", "namespace": "default", "uid": "uid-3"},
        "spec": {"containers": [{"name": "app", "image": "nginx"}]},
        "status": {
            "phase": "Pending",
            "containerStatuses": [{
                "name": "app",
                "containerID": "",
                "image": "nginx",
                "imageID": "",
                "ready": false,
                "started": false,
                "restartCount": 0,
                "state": {"waiting": {"reason": "ContainerCreating"}}
            }]
        }
    });

    let pb_bytes = encode_protobuf(&pod_json).unwrap();
    let unknown = Unknown::decode(&pb_bytes[4..]).unwrap();
    let pod_pb = k8s_pb::api::core::v1::Pod::decode(unknown.raw.as_slice()).unwrap();

    let cs = &pod_pb.status.as_ref().unwrap().container_statuses[0];
    let state = cs.state.as_ref().expect("state must be Some");
    let waiting = state.waiting.as_ref().expect("waiting must be Some");
    assert_eq!(waiting.reason.as_deref(), Some("ContainerCreating"));
}

#[test]
pub fn test_protobuf_raw_json_with_extra_fields_preserves_state() {
    // Pod JSON with extra fields that k8s_openapi might not know about.
    // The raw JSON path should preserve state regardless.
    use prost::Message;
    use serde_json::json;

    let pod_json = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "rich",
            "namespace": "default",
            "uid": "uid-4",
            "annotations": {
                "klights.dev/sandbox-id": "sandbox-xyz",
                "some.custom/annotation": "value"
            }
        },
        "spec": {"containers": [{"name": "app", "image": "nginx"}]},
        "status": {
            "phase": "Running",
            "podIP": "10.43.0.50",
            "podIPs": [{"ip": "10.43.0.50"}],
            "hostIP": "192.168.1.100",
            "hostIPs": [{"ip": "192.168.1.100"}],
            "qosClass": "BestEffort",
            "conditions": [
                {"type": "Ready", "status": "True", "lastTransitionTime": "2026-04-12T10:00:00Z"}
            ],
            "containerStatuses": [{
                "name": "app",
                "containerID": "containerd://abc123",
                "image": "docker.io/library/nginx:latest",
                "imageID": "docker.io/library/nginx@sha256:abc",
                "ready": true,
                "started": true,
                "restartCount": 3,
                "state": {"running": {"startedAt": "2026-04-12T10:00:00Z"}},
                "lastState": {"terminated": {
                    "exitCode": 1,
                    "reason": "Error",
                    "startedAt": "2026-04-12T09:50:00Z",
                    "finishedAt": "2026-04-12T09:55:00Z"
                }}
            }]
        }
    });

    let pb_bytes = encode_protobuf(&pod_json).unwrap();
    let unknown = Unknown::decode(&pb_bytes[4..]).unwrap();
    let pod_pb = k8s_pb::api::core::v1::Pod::decode(unknown.raw.as_slice()).unwrap();

    let status = pod_pb.status.as_ref().unwrap();
    assert_eq!(status.pod_ip.as_deref(), Some("10.43.0.50"));
    assert_eq!(status.pod_ips.len(), 1);
    assert_eq!(status.pod_ips[0].ip.as_deref(), Some("10.43.0.50"));
    assert_eq!(status.host_ip.as_deref(), Some("192.168.1.100"));
    assert_eq!(status.host_ips.len(), 1);
    assert_eq!(status.host_ips[0].ip.as_deref(), Some("192.168.1.100"));

    let cs = &status.container_statuses[0];
    assert_eq!(cs.restart_count, Some(3));

    // Current state: running
    let state = cs.state.as_ref().expect("state must be Some");
    assert!(state.running.is_some(), "current state must be running");

    // Last state: terminated
    let last = cs.last_state.as_ref().expect("lastState must be Some");
    let terminated = last
        .terminated
        .as_ref()
        .expect("last terminated must be Some");
    assert_eq!(terminated.exit_code, Some(1));
    assert_eq!(terminated.reason.as_deref(), Some("Error"));
}

#[test]
pub fn test_protobuf_raw_json_crashloopbackoff_state() {
    // CrashLoopBackOff produces a waiting state with message
    use prost::Message;
    use serde_json::json;

    let pod_json = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "crash", "namespace": "default", "uid": "uid-5"},
        "spec": {"containers": [{"name": "app", "image": "bad:latest"}]},
        "status": {
            "phase": "Running",
            "containerStatuses": [{
                "name": "app",
                "containerID": "containerd://crash123",
                "image": "bad:latest",
                "imageID": "sha256:bad",
                "ready": false,
                "started": false,
                "restartCount": 5,
                "state": {"waiting": {
                    "reason": "CrashLoopBackOff",
                    "message": "back-off 120s restarting failed container"
                }},
                "lastState": {"terminated": {
                    "exitCode": 137,
                    "signal": 9,
                    "reason": "OOMKilled",
                    "startedAt": "2026-04-12T09:00:00Z",
                    "finishedAt": "2026-04-12T09:01:00Z",
                    "containerID": "containerd://old123"
                }}
            }]
        }
    });

    let pb_bytes = encode_protobuf(&pod_json).unwrap();
    let unknown = Unknown::decode(&pb_bytes[4..]).unwrap();
    let pod_pb = k8s_pb::api::core::v1::Pod::decode(unknown.raw.as_slice()).unwrap();

    let cs = &pod_pb.status.as_ref().unwrap().container_statuses[0];

    // Current state: waiting/CrashLoopBackOff
    let state = cs.state.as_ref().expect("state must be Some");
    let waiting = state.waiting.as_ref().expect("waiting must be Some");
    assert_eq!(waiting.reason.as_deref(), Some("CrashLoopBackOff"));
    assert!(waiting.message.as_ref().unwrap().contains("back-off"));

    // Last state: terminated/OOMKilled
    let last = cs.last_state.as_ref().expect("lastState must be Some");
    let terminated = last.terminated.as_ref().expect("terminated must be Some");
    assert_eq!(terminated.exit_code, Some(137));
    assert_eq!(terminated.signal, Some(9));
    assert_eq!(terminated.reason.as_deref(), Some("OOMKilled"));
    assert_eq!(
        terminated.container_id.as_deref(),
        Some("containerd://old123")
    );
}

#[test]
pub fn test_encode_protobuf_namespace_roundtrip() {
    use serde_json::json;

    let ns_json = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": "test-ns",
            "uid": "ns-uid-123"
        }
    });

    let protobuf_bytes = encode_protobuf(&ns_json).unwrap();

    // Verify magic prefix
    assert_eq!(&protobuf_bytes[0..4], &[0x6b, 0x38, 0x73, 0x00]);

    // decode_protobuf expects data without magic prefix
    let decoded = decode_protobuf(&protobuf_bytes[4..]).unwrap();

    assert_eq!(decoded["apiVersion"], "v1");
    assert_eq!(decoded["kind"], "Namespace");
    assert_eq!(decoded["metadata"]["name"], "test-ns");
    assert_eq!(decoded["metadata"]["uid"], "ns-uid-123");
}

#[test]
pub fn test_encode_protobuf_unknown_kind_returns_error() {
    use serde_json::json;

    // Unsupported kinds must error so the HTTP layer can fall back to JSON
    // response negotiation instead of emitting invalid protobuf payloads.
    let custom_json = json!({
        "apiVersion": "custom.io/v1",
        "kind": "CustomThing",
        "metadata": {
            "name": "my-custom"
        },
        "spec": {
            "field": "value"
        }
    });

    let err = encode_protobuf(&custom_json).unwrap_err();
    assert!(
        err.to_string()
            .contains("Unknown kind for protobuf encoding"),
        "unexpected error: {err}"
    );
}

#[test]
pub fn test_protobuf_decode_priority_class() {
    use k8s_pb::api::scheduling::v1::PriorityClass;

    let pb = PriorityClass {
        metadata: Some(k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some("high-priority".to_string()),
            ..Default::default()
        }),
        value: Some(1000),
        global_default: Some(false),
        description: Some("High priority class".to_string()),
        ..Default::default()
    };

    let mut buf = Vec::new();
    pb.encode(&mut buf).unwrap();

    let result = decode_protobuf_resource("", "PriorityClass", &buf);
    assert!(
        result.is_ok(),
        "Failed to decode PriorityClass: {:?}",
        result.err()
    );

    let json = result.unwrap();
    assert_eq!(json["apiVersion"], "scheduling.k8s.io/v1");
    assert_eq!(json["kind"], "PriorityClass");
    assert_eq!(json["metadata"]["name"], "high-priority");
    assert_eq!(json["value"], 1000);
    assert_eq!(json["globalDefault"], false);
    assert_eq!(json["description"], "High priority class");
}

#[test]
pub fn test_protobuf_decode_runtimeclass() {
    use k8s_pb::api::node::v1::RuntimeClass;
    use prost::Message;

    let pb = RuntimeClass {
        metadata: Some(k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some("gvisor".to_string()),
            ..Default::default()
        }),
        handler: Some("runsc".to_string()),
        ..Default::default()
    };

    let mut buf = Vec::new();
    pb.encode(&mut buf).unwrap();

    let result = decode_protobuf_resource("", "RuntimeClass", &buf);
    assert!(
        result.is_ok(),
        "Failed to decode RuntimeClass: {:?}",
        result.err()
    );

    let json = result.unwrap();
    assert_eq!(json["apiVersion"], "node.k8s.io/v1");
    assert_eq!(json["kind"], "RuntimeClass");
    assert_eq!(json["metadata"]["name"], "gvisor");
    assert_eq!(json["handler"], "runsc");
}

#[test]
pub fn test_protobuf_encode_runtimeclass() {
    use prost::Message;

    let json = serde_json::json!({
        "apiVersion": "node.k8s.io/v1",
        "kind": "RuntimeClass",
        "metadata": {"name": "gvisor"},
        "handler": "runsc"
    });

    let buf = encode_protobuf_resource("RuntimeClass", &json).unwrap();
    let decoded = k8s_pb::api::node::v1::RuntimeClass::decode(&buf[..]).unwrap();
    assert_eq!(decoded.handler, Some("runsc".to_string()));
    assert_eq!(decoded.metadata.unwrap().name, Some("gvisor".to_string()));
}

#[test]
pub fn test_protobuf_decode_volume_attachment() {
    use k8s_pb::api::storage::v1::VolumeAttachment;

    let pb = VolumeAttachment {
        metadata: Some(k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some("test-attachment".to_string()),
            ..Default::default()
        }),
        spec: Some(k8s_pb::api::storage::v1::VolumeAttachmentSpec {
            attacher: Some("test-attacher".to_string()),
            node_name: Some("node-1".to_string()),
            source: Some(k8s_pb::api::storage::v1::VolumeAttachmentSource {
                persistent_volume_name: Some("pv-test".to_string()),
                ..Default::default()
            }),
        }),
        ..Default::default()
    };

    let mut buf = Vec::new();
    pb.encode(&mut buf).unwrap();

    let result = decode_protobuf_resource("", "VolumeAttachment", &buf);
    assert!(
        result.is_ok(),
        "Failed to decode VolumeAttachment: {:?}",
        result.err()
    );

    let json = result.unwrap();
    assert_eq!(json["apiVersion"], "storage.k8s.io/v1");
    assert_eq!(json["kind"], "VolumeAttachment");
    assert_eq!(json["metadata"]["name"], "test-attachment");
    assert_eq!(json["spec"]["attacher"], "test-attacher");
    assert_eq!(json["spec"]["nodeName"], "node-1");
    assert_eq!(json["spec"]["source"]["persistentVolumeName"], "pv-test");
}

#[test]
pub fn test_protobuf_decode_replication_controller() {
    use k8s_pb::api::core::v1::ReplicationController;

    let pb = ReplicationController {
        metadata: Some(k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some("test-rc".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        spec: Some(k8s_pb::api::core::v1::ReplicationControllerSpec {
            replicas: Some(3),
            selector: vec![("app".to_string(), "test".to_string())]
                .into_iter()
                .collect(),
            template: Some(k8s_pb::api::core::v1::PodTemplateSpec {
                metadata: Some(k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta {
                    labels: vec![("app".to_string(), "test".to_string())]
                        .into_iter()
                        .collect(),
                    ..Default::default()
                }),
                spec: Some(k8s_pb::api::core::v1::PodSpec {
                    containers: vec![k8s_pb::api::core::v1::Container {
                        name: Some("nginx".to_string()),
                        image: Some("nginx:latest".to_string()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            }),
            ..Default::default()
        }),
        ..Default::default()
    };

    let mut buf = Vec::new();
    pb.encode(&mut buf).unwrap();

    let result = decode_protobuf_resource("", "ReplicationController", &buf);
    assert!(
        result.is_ok(),
        "Failed to decode ReplicationController: {:?}",
        result.err()
    );

    let json = result.unwrap();
    assert_eq!(json["apiVersion"], "v1");
    assert_eq!(json["kind"], "ReplicationController");
    assert_eq!(json["metadata"]["name"], "test-rc");
    assert_eq!(json["metadata"]["namespace"], "default");
    assert_eq!(json["spec"]["replicas"], 3);
    assert_eq!(json["spec"]["selector"]["app"], "test");
}

#[test]
pub fn test_protobuf_decode_pod_disruption_budget() {
    use k8s_pb::api::policy::v1::PodDisruptionBudget;

    let pb = PodDisruptionBudget {
        metadata: Some(k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some("test-pdb".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        spec: Some(k8s_pb::api::policy::v1::PodDisruptionBudgetSpec {
            min_available: Some(k8s_pb::apimachinery::pkg::util::intstr::IntOrString {
                r#type: Some(0),
                int_val: Some(2),
                str_val: None,
            }),
            selector: Some(k8s_pb::apimachinery::pkg::apis::meta::v1::LabelSelector {
                match_labels: vec![("app".to_string(), "web".to_string())]
                    .into_iter()
                    .collect(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        status: Some(k8s_pb::api::policy::v1::PodDisruptionBudgetStatus {
            observed_generation: Some(1),
            disruptions_allowed: Some(1),
            current_healthy: Some(3),
            desired_healthy: Some(2),
            expected_pods: Some(3),
            ..Default::default()
        }),
    };

    let mut buf = Vec::new();
    pb.encode(&mut buf).unwrap();

    let result = decode_protobuf_resource("", "PodDisruptionBudget", &buf);
    assert!(
        result.is_ok(),
        "Failed to decode PodDisruptionBudget: {:?}",
        result.err()
    );

    let json = result.unwrap();
    assert_eq!(json["apiVersion"], "policy/v1");
    assert_eq!(json["kind"], "PodDisruptionBudget");
    assert_eq!(json["metadata"]["name"], "test-pdb");
    assert_eq!(json["metadata"]["namespace"], "default");
    assert_eq!(json["spec"]["minAvailable"], 2);
    assert_eq!(json["spec"]["selector"]["matchLabels"]["app"], "web");
    assert_eq!(json["status"]["observedGeneration"], 1);
    assert_eq!(json["status"]["disruptionsAllowed"], 1);
    assert_eq!(json["status"]["currentHealthy"], 3);
    assert_eq!(json["status"]["desiredHealthy"], 2);
    assert_eq!(json["status"]["expectedPods"], 3);
}

// Regression test: Go client (gogoproto) sends intVal=0 alongside type=1 and strVal="2%"
// for string-type IntOrString. intorstring_to_json must use the type discriminant, not
// check int_val first, or it would return 0 instead of "2%".
#[test]
pub fn test_protobuf_decode_pdb_string_min_available_with_explicit_zero_int_val() {
    use k8s_pb::api::policy::v1::PodDisruptionBudget;
    use prost::Message;

    let pb = PodDisruptionBudget {
        metadata: Some(k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some("test-pdb".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        spec: Some(k8s_pb::api::policy::v1::PodDisruptionBudgetSpec {
            // type=1 (string), int_val=0 explicitly set (as gogoproto may do), str_val="2%"
            min_available: Some(k8s_pb::apimachinery::pkg::util::intstr::IntOrString {
                r#type: Some(1),
                int_val: Some(0),
                str_val: Some("2%".to_string()),
            }),
            ..Default::default()
        }),
        ..Default::default()
    };

    let mut buf = Vec::new();
    pb.encode(&mut buf).unwrap();

    let result = decode_protobuf_resource("policy/v1", "PodDisruptionBudget", &buf);
    assert!(result.is_ok(), "Failed to decode: {:?}", result.err());
    let json = result.unwrap();
    assert_eq!(
        json["spec"]["minAvailable"],
        serde_json::Value::String("2%".to_string()),
        "String-type IntOrString with intVal=0 must decode as \"2%\" not 0"
    );
}
