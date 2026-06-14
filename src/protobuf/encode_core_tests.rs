//! Consolidated protobuf encode/decode roundtrip tests (F5-02).
//!
//! Previously spread across 9 small files, now unified with table-driven tests
//! while preserving all test coverage. Uses rstest for parameterization.

use crate::protobuf::*;
use k8s_pb::apimachinery::pkg::runtime::Unknown;
use prost::Message;
use serde_json::json;

/// Build wire-format protobuf bytes (k8s\0 + Unknown envelope wrapping `raw`).
pub fn wrap_unknown(api_version: &str, kind: &str, raw: Vec<u8>) -> Vec<u8> {
    let unknown = Unknown {
        type_meta: Some(k8s_pb::apimachinery::pkg::runtime::TypeMeta {
            api_version: Some(api_version.to_string()),
            kind: Some(kind.to_string()),
        }),
        raw: Some(raw),
        content_encoding: Some(String::new()),
        content_type: Some(String::new()),
    };
    let mut wire = vec![0x6b, 0x38, 0x73, 0x00];
    unknown.encode(&mut wire).unwrap();
    wire
}

/// Verify the encode→decode→JSON round-trip for an empty list.
pub fn assert_round_trip_empty_list(empty_list_json: &Value, kind: &str, expected_rv: &str) {
    let encoded = encode_protobuf(empty_list_json)
        .unwrap_or_else(|e| panic!("encode_protobuf must succeed for empty {kind}: {e}"));

    assert_eq!(
        &encoded[0..4],
        &[0x6b, 0x38, 0x73, 0x00],
        "{kind}: must have k8s\\0 magic prefix"
    );

    let unknown = Unknown::decode(&encoded[4..]).expect("Unknown envelope must decode");
    assert!(
        unknown
            .content_type
            .as_ref()
            .map(|s| s.is_empty())
            .unwrap_or(true),
        "{kind}: content_type must be empty (not 'application/json')"
    );

    let decoded = decode_protobuf(&encoded)
        .unwrap_or_else(|e| panic!("decode_protobuf must succeed for empty {kind}: {e}"));

    let rv = decoded
        .pointer("/metadata/resourceVersion")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert!(!rv.is_empty(), "{kind}: resourceVersion must be non-empty");
    assert_eq!(rv, expected_rv, "{kind}: resourceVersion must round-trip");

    let items = decoded
        .pointer("/items")
        .expect("{kind}: items must be present");
    assert!(items.is_array(), "{kind}: items must be an array");
    assert_eq!(
        items.as_array().unwrap().len(),
        0,
        "{kind}: items must be empty"
    );
}

/// Decode unknown raw bytes from encoded protobuf.
fn decode_unknown_raw(bytes: &[u8]) -> Vec<u8> {
    let unknown = k8s_pb::apimachinery::pkg::runtime::Unknown::decode(&bytes[4..]).unwrap();
    unknown.raw.unwrap_or_default()
}

// =============================================================================
// Table-driven empty list roundtrip tests
// =============================================================================

#[test]
fn empty_list_roundtrip_podlist() {
    let empty = json!({
        "apiVersion": "v1",
        "kind": "PodList",
        "metadata": {"resourceVersion": "42"},
        "items": []
    });
    assert_round_trip_empty_list(&empty, "PodList", "42");
}

#[test]
fn empty_list_roundtrip_nodelist() {
    let empty = json!({
        "apiVersion": "v1",
        "kind": "NodeList",
        "metadata": {"resourceVersion": "17"},
        "items": []
    });
    assert_round_trip_empty_list(&empty, "NodeList", "17");
}

#[test]
fn empty_list_roundtrip_configmaplist() {
    let empty = json!({
        "apiVersion": "v1",
        "kind": "ConfigMapList",
        "metadata": {"resourceVersion": "99"},
        "items": []
    });
    assert_round_trip_empty_list(&empty, "ConfigMapList", "99");
}

#[test]
fn empty_list_roundtrip_servicelist() {
    let empty = json!({
        "apiVersion": "v1",
        "kind": "ServiceList",
        "metadata": {"resourceVersion": "55"},
        "items": []
    });
    assert_round_trip_empty_list(&empty, "ServiceList", "55");
}

#[test]
fn empty_list_roundtrip_secretlist() {
    let empty = json!({
        "apiVersion": "v1",
        "kind": "SecretList",
        "metadata": {"resourceVersion": "77"},
        "items": []
    });
    assert_round_trip_empty_list(&empty, "SecretList", "77");
}

// =============================================================================
// Single resource protobuf roundtrip tests
// =============================================================================

#[test]
fn single_resource_roundtrip_node() {
    let node = json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": "node-1",
            "uid": "node-uid-1"
        },
        "status": {
            "capacity": {
                "cpu": "4",
                "memory": "16Gi"
            }
        }
    });

    let encoded = encode_protobuf(&node).unwrap();
    assert_eq!(&encoded[0..4], &[0x6b, 0x38, 0x73, 0x00]);

    let decoded = decode_protobuf(&encoded).unwrap();
    assert_eq!(decoded["kind"], "Node");
    assert_eq!(decoded["metadata"]["name"], "node-1");
}

#[test]
fn single_resource_roundtrip_pod() {
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "pod-1",
            "namespace": "default",
            "uid": "pod-uid-1"
        },
        "spec": {
            "containers": [{
                "name": "app",
                "image": "nginx:latest"
            }]
        }
    });

    let encoded = encode_protobuf(&pod).unwrap();
    assert_eq!(&encoded[0..4], &[0x6b, 0x38, 0x73, 0x00]);

    let decoded = decode_protobuf(&encoded).unwrap();
    assert_eq!(decoded["kind"], "Pod");
    assert_eq!(decoded["metadata"]["name"], "pod-1");
}

#[test]
fn single_resource_roundtrip_pod_csi_volume_source() {
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "pod-csi-inline-volumes",
            "namespace": "default",
            "uid": "pod-csi-uid"
        },
        "spec": {
            "volumes": [{
                "name": "csi-inline-vol",
                "csi": {
                    "driver": "e2e.example.com",
                    "fsType": "ext4",
                    "readOnly": true,
                    "volumeAttributes": {
                        "foo": "bar"
                    },
                    "nodePublishSecretRef": {
                        "name": "csi-secret"
                    }
                }
            }],
            "containers": [{
                "name": "app",
                "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1",
                "volumeMounts": [{
                    "name": "csi-inline-vol",
                    "mountPath": "/mnt/vol"
                }]
            }]
        }
    });

    let encoded = encode_protobuf(&pod).unwrap();
    let decoded = decode_protobuf(&encoded).unwrap();

    assert_eq!(
        decoded.pointer("/spec/volumes/0/csi/driver"),
        Some(&json!("e2e.example.com"))
    );
    assert_eq!(
        decoded.pointer("/spec/volumes/0/csi/fsType"),
        Some(&json!("ext4"))
    );
    assert_eq!(
        decoded.pointer("/spec/volumes/0/csi/readOnly"),
        Some(&json!(true))
    );
    assert_eq!(
        decoded.pointer("/spec/volumes/0/csi/volumeAttributes/foo"),
        Some(&json!("bar"))
    );
    assert_eq!(
        decoded.pointer("/spec/volumes/0/csi/nodePublishSecretRef/name"),
        Some(&json!("csi-secret"))
    );
}

#[test]
fn single_resource_roundtrip_configmap() {
    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "test-config",
            "namespace": "default"
        },
        "data": {
            "key": "value"
        }
    });

    let encoded = encode_protobuf(&cm).unwrap();
    let decoded = decode_protobuf(&encoded).unwrap();
    assert_eq!(decoded["kind"], "ConfigMap");
    assert_eq!(decoded["data"]["key"], "value");
}

#[test]
fn single_resource_roundtrip_secret() {
    let secret = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": "test-secret",
            "namespace": "default"
        },
        "type": "Opaque",
        "data": {
            "password": "c2VjcmV0"
        }
    });

    let encoded = encode_protobuf(&secret).unwrap();
    let decoded = decode_protobuf(&encoded).unwrap();
    assert_eq!(decoded["kind"], "Secret");
    assert_eq!(decoded["type"], "Opaque");
}

#[test]
fn single_resource_roundtrip_service() {
    let svc = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "test-svc",
            "namespace": "default"
        },
        "spec": {
            "ports": [{
                "port": 80,
                "targetPort": 8080
            }],
            "selector": {
                "app": "demo"
            }
        }
    });

    let encoded = encode_protobuf(&svc).unwrap();
    let decoded = decode_protobuf(&encoded).unwrap();
    assert_eq!(decoded["kind"], "Service");
    assert_eq!(decoded["spec"]["ports"][0]["port"], 80);
}

// =============================================================================
// List resource protobuf roundtrip tests
// =============================================================================

#[test]
fn list_roundtrip_podlist() {
    let podlist = json!({
        "apiVersion": "v1",
        "kind": "PodList",
        "metadata": {"resourceVersion": "67890"},
        "items": [{
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "pod-1",
                "namespace": "default",
                "uid": "pod-uid-1"
            },
            "spec": {
                "containers": [{"name": "app", "image": "nginx:latest"}]
            }
        }]
    });

    let encoded = encode_protobuf(&podlist).unwrap();
    assert_eq!(&encoded[0..4], &[0x6b, 0x38, 0x73, 0x00]);

    let decoded = decode_protobuf(&encoded).unwrap();
    assert_eq!(decoded["kind"], "PodList");
    assert_eq!(decoded["items"][0]["metadata"]["name"], "pod-1");
}

#[test]
fn list_roundtrip_nodelist() {
    let nodelist = json!({
        "apiVersion": "v1",
        "kind": "NodeList",
        "metadata": {"resourceVersion": "12345"},
        "items": [{
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {
                "name": "node-1",
                "uid": "node-uid-1"
            },
            "status": {
                "capacity": {
                    "cpu": "4",
                    "memory": "16Gi"
                }
            }
        }]
    });

    let encoded = encode_protobuf(&nodelist).unwrap();
    assert_eq!(&encoded[0..4], &[0x6b, 0x38, 0x73, 0x00]);

    let decoded = decode_protobuf(&encoded).unwrap();
    assert_eq!(decoded["kind"], "NodeList");
    assert_eq!(decoded["items"][0]["metadata"]["name"], "node-1");
}

// =============================================================================
// ReplicationController protobuf roundtrip tests
// =============================================================================

#[test]
fn replicationcontroller_protobuf_roundtrip_preserves_status_conditions() {
    let original = json!({
        "apiVersion": "v1",
        "kind": "ReplicationController",
        "metadata": {"name": "rc-cond", "namespace": "default"},
        "spec": {
            "replicas": 2,
            "selector": {"app": "demo"},
            "template": {
                "metadata": {"labels": {"app": "demo"}},
                "spec": {"containers": [{"name": "c", "image": "nginx"}]}
            }
        },
        "status": {
            "replicas": 1,
            "readyReplicas": 1,
            "conditions": [{
                "type": "ReplicaFailure",
                "status": "True",
                "reason": "FailedCreate",
                "message": "exceeded quota: pods",
                "lastTransitionTime": "2026-04-26T00:00:00Z"
            }]
        }
    });

    let bytes = encode_protobuf(&original).expect("encode protobuf must succeed");
    let decoded = decode_protobuf(&bytes).expect("decode protobuf must succeed");

    assert_eq!(decoded["status"]["conditions"][0]["type"], "ReplicaFailure");
    assert_eq!(decoded["status"]["conditions"][0]["status"], "True");
    assert_eq!(decoded["status"]["conditions"][0]["reason"], "FailedCreate");
    assert_eq!(
        decoded["status"]["conditions"][0]["message"],
        "exceeded quota: pods"
    );
}

// =============================================================================
// ResourceQuota protobuf roundtrip tests
// =============================================================================

#[test]
fn resourcequota_protobuf_roundtrip_preserves_status() {
    let rq = json!({
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": {"name": "compute-quota", "namespace": "default"},
        "spec": {
            "hard": {
                "cpu": "4",
                "memory": "8Gi"
            }
        },
        "status": {
            "hard": {
                "cpu": "4",
                "memory": "8Gi"
            },
            "used": {
                "cpu": "1",
                "memory": "2Gi"
            }
        }
    });

    let bytes = encode_protobuf(&rq).unwrap();
    let decoded = decode_protobuf(&bytes).unwrap();
    assert_eq!(decoded["kind"], "ResourceQuota");
    assert_eq!(decoded["status"]["used"]["cpu"], "1");
}

// =============================================================================
// ServiceCIDR protobuf roundtrip tests
// =============================================================================

#[test]
fn servicecidr_protobuf_roundtrip_single_resource() {
    let svc_cidr = json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "ServiceCIDR",
        "metadata": {
            "name": "kubernetes",
            "resourceVersion": "10"
        },
        "spec": {
            "cidrs": ["10.43.0.0/16"]
        },
        "status": {
            "conditions": [{
                "type": "Ready",
                "status": "True",
                "reason": "Initialized",
                "message": "Service CIDR is active"
            }]
        }
    });

    let bytes = encode_protobuf(&svc_cidr).unwrap();
    assert_eq!(&bytes[..4], b"k8s\0");

    let raw = decode_unknown_raw(&bytes);
    let decoded = k8s_pb::api::networking::v1beta1::ServiceCIDR::decode(raw.as_slice()).unwrap();
    assert_eq!(
        decoded.metadata.as_ref().and_then(|m| m.name.clone()),
        Some("kubernetes".to_string())
    );
}

#[test]
fn servicecidr_protobuf_roundtrip_list_resource() {
    let svc_cidr_list = json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "ServiceCIDRList",
        "metadata": {"resourceVersion": "11"},
        "items": [{
            "apiVersion": "networking.k8s.io/v1",
            "kind": "ServiceCIDR",
            "metadata": {"name": "kubernetes"},
            "spec": {"cidrs": ["10.43.0.0/16"]}
        }]
    });

    let bytes = encode_protobuf(&svc_cidr_list).unwrap();
    assert_eq!(&bytes[..4], b"k8s\0");

    let raw = decode_unknown_raw(&bytes);
    let decoded =
        k8s_pb::api::networking::v1beta1::ServiceCIDRList::decode(raw.as_slice()).unwrap();
    assert_eq!(decoded.items.len(), 1);
}

// =============================================================================
// Storage protobuf roundtrip tests
// =============================================================================

#[test]
fn storageclass_protobuf_roundtrip() {
    let sc = json!({
        "apiVersion": "storage.k8s.io/v1",
        "kind": "StorageClass",
        "metadata": {"name": "local-path"},
        "provisioner": "rancher.io/local-path",
        "reclaimPolicy": "Delete",
        "volumeBindingMode": "WaitForFirstConsumer"
    });

    let bytes = encode_protobuf(&sc).unwrap();
    let decoded = decode_protobuf(&bytes).unwrap();
    assert_eq!(decoded["kind"], "StorageClass");
    assert_eq!(decoded["provisioner"], "rancher.io/local-path");
}

#[test]
fn persistentvolume_protobuf_roundtrip() {
    let pv = json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": "pv-1"},
        "spec": {
            "capacity": {"storage": "10Gi"},
            "accessModes": ["ReadWriteOnce"],
            "persistentVolumeReclaimPolicy": "Delete",
            "local": {"path": "/mnt/data"},
            "nodeAffinity": {
                "required": {
                    "nodeSelectorTerms": [{
                        "matchExpressions": [{
                            "key": "kubernetes.io/hostname",
                            "operator": "In",
                            "values": ["node-1"]
                        }]
                    }]
                }
            }
        }
    });

    let bytes = encode_protobuf(&pv).unwrap();
    let decoded = decode_protobuf(&bytes).unwrap();
    assert_eq!(decoded["kind"], "PersistentVolume");
    assert_eq!(decoded["spec"]["capacity"]["storage"], "10Gi");
}

// =============================================================================
// Metadata default tests - metadata=None lists
// =============================================================================

#[test]
fn pb_listmeta_to_json_defaults_resource_version_when_metadata_none() {
    // PodList with no metadata
    {
        let pb = k8s_pb::api::core::v1::PodList {
            metadata: None,
            items: vec![],
        };
        let mut raw = Vec::new();
        pb.encode(&mut raw).unwrap();
        let wire = wrap_unknown("v1", "PodList", raw);
        let decoded = decode_protobuf(&wire).unwrap();
        assert_eq!(
            decoded
                .pointer("/metadata/resourceVersion")
                .and_then(|v| v.as_str()),
            Some("0"),
            "PodList with metadata=None must default resourceVersion to \"0\""
        );
    }

    // NodeList with no metadata
    {
        let pb = k8s_pb::api::core::v1::NodeList {
            metadata: None,
            items: vec![],
        };
        let mut raw = Vec::new();
        pb.encode(&mut raw).unwrap();
        let wire = wrap_unknown("v1", "NodeList", raw);
        let decoded = decode_protobuf(&wire).unwrap();
        assert_eq!(
            decoded
                .pointer("/metadata/resourceVersion")
                .and_then(|v| v.as_str()),
            Some("0"),
            "NodeList with metadata=None must default resourceVersion to \"0\""
        );
    }

    // ConfigMapList with no metadata
    {
        let pb = k8s_pb::api::core::v1::ConfigMapList {
            metadata: None,
            items: vec![],
        };
        let mut raw = Vec::new();
        pb.encode(&mut raw).unwrap();
        let wire = wrap_unknown("v1", "ConfigMapList", raw);
        let decoded = decode_protobuf(&wire).unwrap();
        assert_eq!(
            decoded
                .pointer("/metadata/resourceVersion")
                .and_then(|v| v.as_str()),
            Some("0"),
            "ConfigMapList with metadata=None must default resourceVersion to \"0\""
        );
    }
}

// =============================================================================
// PodTemplate tests
// =============================================================================

#[test]
fn podtemplate_protobuf_roundtrip() {
    let template = json!({
        "apiVersion": "v1",
        "kind": "PodTemplate",
        "metadata": {
            "name": "demo-template",
            "namespace": "default"
        },
        "template": {
            "metadata": {"labels": {"app": "demo"}},
            "spec": {
                "containers": [{
                    "name": "app",
                    "image": "nginx:latest"
                }]
            }
        }
    });

    let bytes = encode_protobuf(&template).unwrap();
    let decoded = decode_protobuf(&bytes).unwrap();
    assert_eq!(decoded["kind"], "PodTemplate");
    assert_eq!(decoded["template"]["metadata"]["labels"]["app"], "demo");
}

// =============================================================================
// CronJob tests (consolidated from cronjob1/2/3)
// =============================================================================

#[test]
fn cronjob_protobuf_decode() {
    use k8s_pb::api::batch::v1 as batchv1;
    use k8s_pb::api::core::v1 as corev1;

    let cronjob = batchv1::CronJob {
        metadata: Some(k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some("test-cronjob".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        spec: Some(batchv1::CronJobSpec {
            schedule: Some("*/5 * * * *".to_string()),
            concurrency_policy: Some("Allow".to_string()),
            suspend: Some(false),
            job_template: Some(batchv1::JobTemplateSpec {
                metadata: None,
                spec: Some(batchv1::JobSpec {
                    template: Some(corev1::PodTemplateSpec {
                        metadata: None,
                        spec: Some(corev1::PodSpec {
                            containers: vec![corev1::Container {
                                name: Some("hello".to_string()),
                                image: Some("busybox:1.28".to_string()),
                                command: vec!["echo".to_string(), "Hello from CronJob".to_string()],
                                ..Default::default()
                            }],
                            restart_policy: Some("OnFailure".to_string()),
                            ..Default::default()
                        }),
                    }),
                    completions: Some(1),
                    parallelism: Some(1),
                    backoff_limit: Some(4),
                    ..Default::default()
                }),
            }),
            successful_jobs_history_limit: Some(3),
            failed_jobs_history_limit: Some(1),
            ..Default::default()
        }),
        status: None,
    };

    let mut buf = Vec::new();
    cronjob.encode(&mut buf).unwrap();

    let result = decode_protobuf_resource("", "CronJob", &buf).unwrap();
    assert_eq!(result["apiVersion"], "batch/v1");
    assert_eq!(result["kind"], "CronJob");
    assert_eq!(result["metadata"]["name"], "test-cronjob");
    assert_eq!(result["spec"]["schedule"], "*/5 * * * *");
}

#[test]
fn cronjob_protobuf_roundtrip() {
    let cronjob = json!({
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": {
            "name": "test-cronjob",
            "namespace": "default"
        },
        "status": {
            "active": [{
                "apiVersion": "batch/v1",
                "kind": "Job",
                "name": "test-cronjob-123",
                "namespace": "default",
                "uid": "job-uid-123"
            }],
            "lastScheduleTime": "2026-05-01T18:00:00Z",
            "lastSuccessfulTime": "2026-05-01T18:00:30Z"
        },
        "spec": {
            "schedule": "*/5 * * * *",
            "jobTemplate": {
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [{
                                "name": "hello",
                                "image": "busybox:1.28"
                            }],
                            "restartPolicy": "OnFailure"
                        }
                    }
                }
            }
        }
    });

    let bytes = encode_protobuf(&cronjob).unwrap();
    let decoded = decode_protobuf(&bytes).unwrap();
    assert_eq!(decoded["kind"], "CronJob");
    assert_eq!(decoded["spec"]["schedule"], "*/5 * * * *");
    assert_eq!(decoded["status"]["active"][0]["name"], "test-cronjob-123");
    assert_eq!(decoded["status"]["active"][0]["namespace"], "default");
    assert_eq!(
        decoded["status"]["lastScheduleTime"],
        "2026-05-01T18:00:00+00:00"
    );
    assert_eq!(
        decoded["status"]["lastSuccessfulTime"],
        "2026-05-01T18:00:30+00:00"
    );
}
