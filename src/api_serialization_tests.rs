use rstest::rstest;
use serde_json::{Value, json};

/// Helper to create test bytes in the specified format
fn create_test_bytes(resource_json: &Value, format: &str) -> anyhow::Result<Vec<u8>> {
    match format {
        "json" => Ok(serde_json::to_vec(resource_json)?),
        "protobuf" => {
            // For protobuf, manually construct k8s-pb types and encode them
            json_to_protobuf_bytes(resource_json)
        }
        _ => anyhow::bail!("Unknown format: {}", format),
    }
}

/// Manually construct k8s-pb types from test data and encode to protobuf
fn json_to_protobuf_bytes(value: &Value) -> anyhow::Result<Vec<u8>> {
    use k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use prost::Message;
    use std::collections::BTreeMap;

    let kind = value
        .get("kind")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing kind"))?;

    let api_version = value
        .get("apiVersion")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing apiVersion"))?;

    // Helper to extract metadata
    let extract_metadata = |v: &Value| -> ObjectMeta {
        let meta = v.get("metadata");
        ObjectMeta {
            name: meta
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
                .map(|s| s.to_string()),
            namespace: meta
                .and_then(|m| m.get("namespace"))
                .and_then(|n| n.as_str())
                .map(|s| s.to_string()),
            labels: meta
                .and_then(|m| m.get("labels"))
                .and_then(|l| l.as_object())
                .map(|obj| {
                    obj.iter()
                        .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                        .collect::<BTreeMap<String, String>>()
                })
                .unwrap_or_default(),
            ..Default::default()
        }
    };

    // Encode to protobuf based on kind
    let mut buf = Vec::new();
    match (api_version, kind) {
        ("v1", "ConfigMap") => {
            let meta = extract_metadata(value);
            let data = value
                .get("data")
                .and_then(|d| d.as_object())
                .map(|obj| {
                    obj.iter()
                        .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                        .collect::<BTreeMap<String, String>>()
                })
                .unwrap_or_default();

            let cm = k8s_pb::api::core::v1::ConfigMap {
                metadata: Some(meta),
                data,
                ..Default::default()
            };
            cm.encode(&mut buf)?;
        }
        ("v1", "Secret") => {
            let meta = extract_metadata(value);
            let data = value
                .get("data")
                .and_then(|d| d.as_object())
                .map(|obj| {
                    obj.iter()
                        .map(|(k, v)| {
                            let bytes = v.as_str().unwrap_or("").as_bytes().to_vec();
                            (k.clone(), bytes)
                        })
                        .collect::<BTreeMap<String, Vec<u8>>>()
                })
                .unwrap_or_default();

            let secret = k8s_pb::api::core::v1::Secret {
                metadata: Some(meta),
                data,
                ..Default::default()
            };
            secret.encode(&mut buf)?;
        }
        ("v1", "Pod") => {
            use k8s_pb::api::core::v1::{Container, PodSpec};

            let meta = extract_metadata(value);
            let spec_val = value.get("spec");
            let node_name = spec_val
                .and_then(|s| s.get("nodeName"))
                .and_then(|n| n.as_str())
                .map(|s| s.to_string());

            let containers = spec_val
                .and_then(|s| s.get("containers"))
                .and_then(|c| c.as_array())
                .map(|arr| {
                    arr.iter()
                        .map(|c| Container {
                            name: c
                                .get("name")
                                .and_then(|n| n.as_str())
                                .map(|s| s.to_string()),
                            image: c
                                .get("image")
                                .and_then(|i| i.as_str())
                                .map(|s| s.to_string()),
                            ..Default::default()
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            let pod = k8s_pb::api::core::v1::Pod {
                metadata: Some(meta),
                spec: Some(PodSpec {
                    node_name,
                    containers,
                    ..Default::default()
                }),
                ..Default::default()
            };
            pod.encode(&mut buf)?;
        }
        ("apps/v1", "Deployment") => {
            use k8s_pb::api::apps::v1::DeploymentSpec;
            use k8s_pb::api::core::v1::{Container, PodSpec, PodTemplateSpec};
            use k8s_pb::apimachinery::pkg::apis::meta::v1::LabelSelector;

            let meta = extract_metadata(value);
            let spec_val = value.get("spec");

            let replicas = spec_val
                .and_then(|s| s.get("replicas"))
                .and_then(|r| r.as_i64())
                .map(|r| r as i32);

            let selector = spec_val
                .and_then(|s| s.get("selector"))
                .and_then(|sel| sel.get("matchLabels"))
                .and_then(|ml| ml.as_object())
                .map(|obj| LabelSelector {
                    match_labels: obj
                        .iter()
                        .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                        .collect(),
                    ..Default::default()
                });

            let template = spec_val.and_then(|s| s.get("template")).map(|t| {
                let tmpl_meta = t.get("metadata").map(|m| ObjectMeta {
                    labels: m
                        .get("labels")
                        .and_then(|l| l.as_object())
                        .map(|obj| {
                            obj.iter()
                                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                                .collect()
                        })
                        .unwrap_or_default(),
                    ..Default::default()
                });

                let containers = t
                    .get("spec")
                    .and_then(|s| s.get("containers"))
                    .and_then(|c| c.as_array())
                    .map(|arr| {
                        arr.iter()
                            .map(|c| Container {
                                name: c
                                    .get("name")
                                    .and_then(|n| n.as_str())
                                    .map(|s| s.to_string()),
                                image: c
                                    .get("image")
                                    .and_then(|i| i.as_str())
                                    .map(|s| s.to_string()),
                                ..Default::default()
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                PodTemplateSpec {
                    metadata: tmpl_meta,
                    spec: Some(PodSpec {
                        containers,
                        ..Default::default()
                    }),
                }
            });

            let deploy = k8s_pb::api::apps::v1::Deployment {
                metadata: Some(meta),
                spec: Some(DeploymentSpec {
                    replicas,
                    selector,
                    template,
                    ..Default::default()
                }),
                ..Default::default()
            };
            deploy.encode(&mut buf)?;
        }
        ("v1", "Service") => {
            use k8s_pb::api::core::v1::{ServicePort, ServiceSpec};

            let meta = extract_metadata(value);
            let spec_val = value.get("spec");

            let cluster_ip = spec_val
                .and_then(|s| s.get("clusterIP"))
                .and_then(|c| c.as_str())
                .map(|s| s.to_string());

            let ports = spec_val
                .and_then(|s| s.get("ports"))
                .and_then(|p| p.as_array())
                .map(|arr| {
                    arr.iter()
                        .map(|p| ServicePort {
                            port: p.get("port").and_then(|prt| prt.as_i64()).map(|n| n as i32),
                            target_port: p.get("targetPort").and_then(|tp| {
                                if let Some(i) = tp.as_i64() {
                                    Some(k8s_pb::apimachinery::pkg::util::intstr::IntOrString {
                                        r#type: Some(0), // 0 = int
                                        int_val: Some(i as i32),
                                        str_val: None,
                                    })
                                } else {
                                    tp.as_str().map(|s| {
                                        k8s_pb::apimachinery::pkg::util::intstr::IntOrString {
                                            r#type: Some(1), // 1 = string
                                            int_val: None,
                                            str_val: Some(s.to_string()),
                                        }
                                    })
                                }
                            }),
                            protocol: p
                                .get("protocol")
                                .and_then(|pr| pr.as_str())
                                .map(|s| s.to_string()),
                            ..Default::default()
                        })
                        .collect()
                })
                .unwrap_or_default();

            let svc = k8s_pb::api::core::v1::Service {
                metadata: Some(meta),
                spec: Some(ServiceSpec {
                    cluster_ip,
                    ports,
                    ..Default::default()
                }),
                ..Default::default()
            };
            svc.encode(&mut buf)?;
        }
        ("v1", "Namespace") => {
            let meta = extract_metadata(value);
            let ns = k8s_pb::api::core::v1::Namespace {
                metadata: Some(meta),
                ..Default::default()
            };
            ns.encode(&mut buf)?;
        }
        ("v1", "Node") => {
            use k8s_pb::api::core::v1::{NodeCondition, NodeStatus};

            let meta = extract_metadata(value);
            let status_val = value.get("status");

            let conditions = status_val
                .and_then(|s| s.get("conditions"))
                .and_then(|c| c.as_array())
                .map(|arr| {
                    arr.iter()
                        .map(|c| NodeCondition {
                            r#type: c
                                .get("type")
                                .and_then(|t| t.as_str())
                                .map(|s| s.to_string()),
                            status: c
                                .get("status")
                                .and_then(|st| st.as_str())
                                .map(|s| s.to_string()),
                            reason: c
                                .get("reason")
                                .and_then(|r| r.as_str())
                                .map(|s| s.to_string()),
                            message: c
                                .get("message")
                                .and_then(|m| m.as_str())
                                .map(|s| s.to_string()),
                            ..Default::default()
                        })
                        .collect()
                })
                .unwrap_or_default();

            let node = k8s_pb::api::core::v1::Node {
                metadata: Some(meta),
                status: Some(NodeStatus {
                    conditions,
                    ..Default::default()
                }),
                ..Default::default()
            };
            node.encode(&mut buf)?;
        }
        ("coordination.k8s.io/v1", "Lease") => {
            use k8s_pb::api::coordination::v1::LeaseSpec;
            use k8s_pb::apimachinery::pkg::apis::meta::v1::MicroTime;

            let meta = extract_metadata(value);
            let spec_val = value.get("spec");

            let holder_identity = spec_val
                .and_then(|s| s.get("holderIdentity"))
                .and_then(|h| h.as_str())
                .map(|s| s.to_string());

            let lease_duration_seconds = spec_val
                .and_then(|s| s.get("leaseDurationSeconds"))
                .and_then(|l| l.as_i64())
                .map(|n| n as i32);

            let lease_transitions = spec_val
                .and_then(|s| s.get("leaseTransitions"))
                .and_then(|l| l.as_i64())
                .map(|n| n as i32);

            let parse_microtime = |_s: &str| -> Option<MicroTime> {
                // For testing, just store a placeholder timestamp
                // (Full RFC3339 parsing would require additional dependencies)
                Some(MicroTime {
                    seconds: Some(0),
                    nanos: Some(0),
                })
            };

            let acquire_time = spec_val
                .and_then(|s| s.get("acquireTime"))
                .and_then(|t| t.as_str())
                .and_then(parse_microtime);

            let renew_time = spec_val
                .and_then(|s| s.get("renewTime"))
                .and_then(|t| t.as_str())
                .and_then(parse_microtime);

            let lease = k8s_pb::api::coordination::v1::Lease {
                metadata: Some(meta),
                spec: Some(LeaseSpec {
                    holder_identity,
                    lease_duration_seconds,
                    acquire_time,
                    renew_time,
                    lease_transitions,
                    preferred_holder: None,
                    strategy: None,
                }),
            };
            lease.encode(&mut buf)?;
        }
        _ => anyhow::bail!("Unsupported kind: {}/{}", api_version, kind),
    }

    // Wrap in Unknown envelope
    let unknown = k8s_pb::apimachinery::pkg::runtime::Unknown {
        type_meta: Some(k8s_pb::apimachinery::pkg::runtime::TypeMeta {
            api_version: Some(api_version.to_string()),
            kind: Some(kind.to_string()),
        }),
        raw: Some(buf),
        content_encoding: Some(String::new()),
        content_type: Some(String::new()),
    };

    let mut result = Vec::new();
    unknown.encode(&mut result)?;
    Ok(result)
}

/// Helper to decode based on format
fn decode_resource(bytes: &[u8], format: &str) -> anyhow::Result<Value> {
    match format {
        "json" => Ok(serde_json::from_slice(bytes)?),
        "protobuf" => crate::protobuf::decode_protobuf(bytes),
        _ => anyhow::bail!("Unknown format: {}", format),
    }
}

#[rstest]
#[case("json")]
#[case("protobuf")]
fn test_configmap_data_preserved(#[case] format: &str) {
    let original = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "test-config",
            "namespace": "default"
        },
        "data": {
            "key1": "value1",
            "key2": "value2"
        }
    });

    let bytes = create_test_bytes(&original, format).expect("Failed to create test bytes");
    let decoded = decode_resource(&bytes, format).expect("decode failed");

    assert_eq!(decoded["apiVersion"], "v1");
    assert_eq!(decoded["kind"], "ConfigMap");
    assert_eq!(decoded["metadata"]["name"], "test-config");
    assert_eq!(decoded["metadata"]["namespace"], "default");
    assert_eq!(decoded["data"]["key1"], "value1");
    assert_eq!(decoded["data"]["key2"], "value2");
}

#[rstest]
#[case("json")]
#[case("protobuf")]
fn test_secret_data_field(#[case] format: &str) {
    let original = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": "test-secret",
            "namespace": "default"
        },
        "data": {
            "username": "YWRtaW4=",
            "password": "c2VjcmV0MTIz"
        }
    });

    let bytes = create_test_bytes(&original, format).expect("Failed to create test bytes");
    let decoded = decode_resource(&bytes, format).expect("decode failed");

    assert_eq!(decoded["apiVersion"], "v1");
    assert_eq!(decoded["kind"], "Secret");
    assert_eq!(decoded["metadata"]["name"], "test-secret");
    assert!(decoded.get("data").is_some());
}

#[rstest]
#[case("json")]
#[case("protobuf")]
fn test_pod_spec_containers(#[case] format: &str) {
    let original = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "test-pod",
            "namespace": "default"
        },
        "spec": {
            "nodeName": "node-1",
            "containers": [
                {
                    "name": "nginx",
                    "image": "nginx:latest"
                }
            ]
        }
    });

    let bytes = create_test_bytes(&original, format).expect("Failed to create test bytes");
    let decoded = decode_resource(&bytes, format).expect("decode failed");

    assert_eq!(decoded["apiVersion"], "v1");
    assert_eq!(decoded["kind"], "Pod");
    assert_eq!(decoded["spec"]["nodeName"], "node-1");
    assert_eq!(decoded["spec"]["containers"][0]["name"], "nginx");
    assert_eq!(decoded["spec"]["containers"][0]["image"], "nginx:latest");
}

#[rstest]
#[case("json")]
#[case("protobuf")]
fn test_deployment_spec_replicas(#[case] format: &str) {
    let original = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "test-deploy",
            "namespace": "default"
        },
        "spec": {
            "replicas": 3,
            "selector": {
                "matchLabels": {
                    "app": "test"
                }
            },
            "template": {
                "metadata": {
                    "labels": {
                        "app": "test"
                    }
                },
                "spec": {
                    "containers": [
                        {
                            "name": "nginx",
                            "image": "nginx:latest"
                        }
                    ]
                }
            }
        }
    });

    let bytes = create_test_bytes(&original, format).expect("Failed to create test bytes");
    let decoded = decode_resource(&bytes, format).expect("decode failed");

    assert_eq!(decoded["apiVersion"], "apps/v1");
    assert_eq!(decoded["kind"], "Deployment");
    assert_eq!(decoded["spec"]["replicas"], 3);
    assert_eq!(decoded["spec"]["selector"]["matchLabels"]["app"], "test");
}

#[rstest]
#[case("json")]
#[case("protobuf")]
fn test_service_spec_cluster_ip(#[case] format: &str) {
    let original = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "test-svc",
            "namespace": "default"
        },
        "spec": {
            "clusterIP": "10.43.128.100",
            "ports": [
                {
                    "port": 80,
                    "targetPort": 8080,
                    "protocol": "TCP"
                }
            ]
        }
    });

    let bytes = create_test_bytes(&original, format).expect("Failed to create test bytes");
    let decoded = decode_resource(&bytes, format).expect("decode failed");

    assert_eq!(decoded["apiVersion"], "v1");
    assert_eq!(decoded["kind"], "Service");
    assert_eq!(decoded["spec"]["clusterIP"], "10.43.128.100");
    assert_eq!(decoded["spec"]["ports"][0]["port"], 80);
    // targetPort might be int or intOrString, accept both
    let tp = &decoded["spec"]["ports"][0]["targetPort"];
    assert!(tp == &json!(8080) || tp == &json!("8080"));
}

#[rstest]
#[case("json")]
#[case("protobuf")]
fn test_namespace_metadata_only(#[case] format: &str) {
    let original = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": "test-ns",
            "labels": {
                "env": "test"
            }
        }
    });

    let bytes = create_test_bytes(&original, format).expect("Failed to create test bytes");
    let decoded = decode_resource(&bytes, format).expect("decode failed");

    assert_eq!(decoded["apiVersion"], "v1");
    assert_eq!(decoded["kind"], "Namespace");
    assert_eq!(decoded["metadata"]["name"], "test-ns");
    assert_eq!(decoded["metadata"]["labels"]["env"], "test");
}

#[rstest]
#[case("json")]
#[case("protobuf")]
fn test_node_status_conditions(#[case] format: &str) {
    let original = json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": "test-node"
        },
        "status": {
            "conditions": [
                {
                    "type": "Ready",
                    "status": "True",
                    "reason": "KubeletReady",
                    "message": "kubelet is posting ready status"
                }
            ]
        }
    });

    let bytes = create_test_bytes(&original, format).expect("Failed to create test bytes");
    let decoded = decode_resource(&bytes, format).expect("decode failed");

    assert_eq!(decoded["apiVersion"], "v1");
    assert_eq!(decoded["kind"], "Node");
    assert_eq!(decoded["status"]["conditions"][0]["type"], "Ready");
    assert_eq!(decoded["status"]["conditions"][0]["status"], "True");
}

#[rstest]
#[case("json")]
#[case("protobuf")]
fn test_lease_spec_fields(#[case] format: &str) {
    let original = json!({
        "apiVersion": "coordination.k8s.io/v1",
        "kind": "Lease",
        "metadata": {
            "name": "test-lease",
            "namespace": "kube-system"
        },
        "spec": {
            "holderIdentity": "cert-manager-1",
            "leaseDurationSeconds": 15,
            "acquireTime": "2026-04-03T00:00:00Z",
            "renewTime": "2026-04-03T00:00:15Z",
            "leaseTransitions": 0
        }
    });

    let bytes = create_test_bytes(&original, format).expect("Failed to create test bytes");
    let decoded = decode_resource(&bytes, format).expect("decode failed");

    assert_eq!(decoded["apiVersion"], "coordination.k8s.io/v1");
    assert_eq!(decoded["kind"], "Lease");
    assert_eq!(decoded["spec"]["holderIdentity"], "cert-manager-1");
    assert_eq!(decoded["spec"]["leaseDurationSeconds"], 15);
}
