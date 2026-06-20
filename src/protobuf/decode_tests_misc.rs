use super::decode_tests_helpers::build_unknown_envelope;
use crate::protobuf::*;
use prost::Message;
use serde_json::json;

#[test]
pub fn test_protobuf_decode_full_path() {
    use k8s_pb::api::apps::v1::{Deployment, DeploymentSpec};
    use k8s_pb::api::apps::v1::{StatefulSet, StatefulSetSpec};
    use k8s_pb::api::core::v1::{
        Container, Pod, PodSpec, PodTemplateSpec, Secret, Service, ServicePort, ServiceSpec,
    };
    use k8s_pb::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
    use prost::Message;

    struct Case {
        api_version: &'static str,
        kind: &'static str,
        build_pb: Box<dyn Fn() -> Vec<u8>>,
        assertions: Box<dyn Fn(Value)>,
    }

    let cases = vec![
        Case {
            api_version: "apps/v1",
            kind: "Deployment",
            build_pb: Box::new(|| {
                let d = Deployment {
                    metadata: Some(ObjectMeta {
                        name: Some("nginx".to_string()),
                        namespace: Some("default".to_string()),
                        ..Default::default()
                    }),
                    spec: Some(DeploymentSpec {
                        replicas: Some(3),
                        selector: Some(LabelSelector {
                            match_labels: vec![("app".to_string(), "nginx".to_string())]
                                .into_iter()
                                .collect(),
                            ..Default::default()
                        }),
                        template: Some(PodTemplateSpec {
                            metadata: Some(ObjectMeta {
                                labels: vec![("app".to_string(), "nginx".to_string())]
                                    .into_iter()
                                    .collect(),
                                ..Default::default()
                            }),
                            spec: Some(PodSpec {
                                containers: vec![Container {
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
                d.encode(&mut buf).unwrap();
                buf
            }),
            assertions: Box::new(|result| {
                assert_eq!(result["kind"], "Deployment");
                assert_eq!(result["metadata"]["name"], "nginx");
                assert_eq!(result["spec"]["replicas"], 3);
                assert_eq!(
                    result["spec"]["template"]["spec"]["containers"][0]["image"],
                    "nginx:latest"
                );
            }),
        },
        Case {
            api_version: "v1",
            kind: "Pod",
            build_pb: Box::new(|| {
                let p = Pod {
                    metadata: Some(ObjectMeta {
                        name: Some("test-pod".to_string()),
                        namespace: Some("default".to_string()),
                        ..Default::default()
                    }),
                    spec: Some(PodSpec {
                        containers: vec![Container {
                            name: Some("app".to_string()),
                            image: Some("app:v1".to_string()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                let mut buf = Vec::new();
                p.encode(&mut buf).unwrap();
                buf
            }),
            assertions: Box::new(|result| {
                assert_eq!(result["kind"], "Pod");
                assert_eq!(result["metadata"]["name"], "test-pod");
                assert_eq!(result["spec"]["containers"][0]["image"], "app:v1");
            }),
        },
        Case {
            api_version: "v1",
            kind: "Service",
            build_pb: Box::new(|| {
                let s = Service {
                    metadata: Some(ObjectMeta {
                        name: Some("web".to_string()),
                        namespace: Some("default".to_string()),
                        ..Default::default()
                    }),
                    spec: Some(ServiceSpec {
                        ports: vec![ServicePort {
                            port: Some(80),
                            target_port: Some(
                                k8s_pb::apimachinery::pkg::util::intstr::IntOrString {
                                    int_val: Some(8080),
                                    ..Default::default()
                                },
                            ),
                            ..Default::default()
                        }],
                        selector: vec![("app".to_string(), "web".to_string())]
                            .into_iter()
                            .collect(),
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                let mut buf = Vec::new();
                s.encode(&mut buf).unwrap();
                buf
            }),
            assertions: Box::new(|result| {
                assert_eq!(result["kind"], "Service");
                assert_eq!(result["spec"]["ports"][0]["port"], 80);
                assert_eq!(result["spec"]["ports"][0]["targetPort"], 8080);
            }),
        },
        Case {
            api_version: "v1",
            kind: "Secret",
            build_pb: Box::new(|| {
                let s = Secret {
                    metadata: Some(ObjectMeta {
                        name: Some("creds".to_string()),
                        namespace: Some("default".to_string()),
                        ..Default::default()
                    }),
                    data: vec![("password".to_string(), b"secret123".to_vec())]
                        .into_iter()
                        .collect(),
                    ..Default::default()
                };
                let mut buf = Vec::new();
                s.encode(&mut buf).unwrap();
                buf
            }),
            assertions: Box::new(|result| {
                assert_eq!(result["kind"], "Secret");
                assert_eq!(result["data"]["password"], "c2VjcmV0MTIz"); // base64("secret123")
            }),
        },
        Case {
            api_version: "apps/v1",
            kind: "StatefulSet",
            build_pb: Box::new(|| {
                let sts = StatefulSet {
                    metadata: Some(ObjectMeta {
                        name: Some("db".to_string()),
                        namespace: Some("default".to_string()),
                        ..Default::default()
                    }),
                    spec: Some(StatefulSetSpec {
                        replicas: Some(2),
                        service_name: Some("db-svc".to_string()),
                        selector: Some(LabelSelector {
                            match_labels: vec![("app".to_string(), "db".to_string())]
                                .into_iter()
                                .collect(),
                            ..Default::default()
                        }),
                        template: Some(PodTemplateSpec {
                            metadata: Some(ObjectMeta {
                                labels: vec![("app".to_string(), "db".to_string())]
                                    .into_iter()
                                    .collect(),
                                ..Default::default()
                            }),
                            spec: Some(PodSpec {
                                containers: vec![Container {
                                    name: Some("postgres".to_string()),
                                    image: Some("postgres:14".to_string()),
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
                sts.encode(&mut buf).unwrap();
                buf
            }),
            assertions: Box::new(|result| {
                assert_eq!(result["kind"], "StatefulSet");
                assert_eq!(result["spec"]["replicas"], 2);
                assert_eq!(result["spec"]["serviceName"], "db-svc");
            }),
        },
    ];

    for case in cases {
        let pb_bytes = (case.build_pb)();

        // Build Unknown envelope
        let envelope = Unknown {
            type_meta: Some(TypeMeta {
                api_version: case.api_version.to_string(),
                kind: case.kind.to_string(),
            }),
            raw: pb_bytes,
            content_encoding: String::new(),
            content_type: "application/vnd.kubernetes.protobuf".to_string(),
        };

        let mut wire = Vec::new();
        envelope.encode(&mut wire).unwrap();

        // This tests the full decode_protobuf() path (not just pb_*_to_json helpers)
        let result = decode_protobuf(&wire).unwrap();

        (case.assertions)(result);
    }
}

#[test]
pub fn test_decode_protobuf_omits_empty_objectmeta_namespace() {
    use k8s_pb::api::admissionregistration::v1::{
        ValidatingAdmissionPolicyBinding, ValidatingAdmissionPolicyBindingSpec,
    };
    use k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    let binding = ValidatingAdmissionPolicyBinding {
        metadata: Some(ObjectMeta {
            name: Some("vapb-empty-namespace".to_string()),
            namespace: Some(String::new()),
            labels: vec![("example-e2e-vapb-label".to_string(), "decode".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        }),
        spec: Some(ValidatingAdmissionPolicyBindingSpec {
            policy_name: Some("missing-policy.example.com".to_string()),
            validation_actions: vec!["Deny".to_string()],
            ..Default::default()
        }),
    };
    let mut raw = Vec::new();
    binding.encode(&mut raw).unwrap();
    let envelope = build_unknown_envelope(
        "admissionregistration.k8s.io/v1",
        "ValidatingAdmissionPolicyBinding",
        &raw,
    );
    let decoded = decode_protobuf(&envelope).unwrap();

    assert_eq!(decoded["metadata"]["name"], "vapb-empty-namespace");
    assert!(
        decoded.pointer("/metadata/namespace").is_none(),
        "empty protobuf ObjectMeta.namespace must not be persisted as a cluster-scoped JSON namespace"
    );
    assert_eq!(decoded["spec"]["validationActions"][0], "Deny");
}

#[test]
pub fn test_protobuf_decode_json_in_envelope() {
    use prost::Message;

    // kubectl sometimes sends JSON inside the protobuf envelope
    let json_data = br#"{"apiVersion":"v1","kind":"Pod","metadata":{"name":"json-pod"},"spec":{"containers":[{"name":"c1","image":"alpine"}]}}"#;

    let envelope = Unknown {
        type_meta: Some(TypeMeta {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
        }),
        raw: json_data.to_vec(),
        content_encoding: String::new(),
        content_type: "application/json".to_string(),
    };

    let mut wire = Vec::new();
    envelope.encode(&mut wire).unwrap();

    let result = decode_protobuf(&wire).unwrap();

    assert_eq!(result["kind"], "Pod");
    assert_eq!(result["metadata"]["name"], "json-pod");
    assert_eq!(result["spec"]["containers"][0]["image"], "alpine");
}

#[test]
pub fn test_protobuf_decode_unsupported_kind() {
    use k8s_pb::api::core::v1::Pod;
    use k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use prost::Message;

    let pod = Pod {
        metadata: Some(ObjectMeta {
            name: Some("test".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    };

    let mut pb_bytes = Vec::new();
    pod.encode(&mut pb_bytes).unwrap();

    let envelope = Unknown {
        type_meta: Some(TypeMeta {
            api_version: "v1".to_string(),
            kind: "UnsupportedKind".to_string(), // Not in registry
        }),
        raw: pb_bytes,
        content_encoding: String::new(),
        content_type: "application/vnd.kubernetes.protobuf".to_string(),
    };

    let mut wire = Vec::new();
    envelope.encode(&mut wire).unwrap();

    // Unknown kinds are handled by the generic fallback decoder which decodes ObjectMeta
    // from field 1.  The result is a best-effort JSON with kind/apiVersion set.
    let result = decode_protobuf(&wire);
    assert!(
        result.is_ok(),
        "Generic fallback should handle unknown kinds: {:?}",
        result.err()
    );
    let json = result.unwrap();
    // Generic decoder populates kind and apiVersion from the envelope type_meta
    assert_eq!(json["kind"], "UnsupportedKind");
    assert_eq!(json["metadata"]["name"], "test");
}

#[test]
pub fn test_protobuf_decode_malformed_envelope() {
    // Invalid protobuf data
    let malformed = b"not valid protobuf data";
    let result = decode_protobuf(malformed);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Failed to decode Unknown envelope"));
}

#[test]
pub fn test_encode_protobuf_pod() {
    use serde_json::json;

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "test-pod",
            "namespace": "default"
        },
        "spec": {
            "containers": [{
                "name": "nginx",
                "image": "nginx:1.14.2"
            }]
        }
    });

    let encoded = encode_protobuf(&pod).unwrap();
    assert!(!encoded.is_empty());

    // Verify K8s magic prefix
    assert_eq!(
        &encoded[0..4],
        &[0x6b, 0x38, 0x73, 0x00],
        "Missing k8s magic prefix"
    );

    // Decode and verify round-trip (decode_protobuf expects data without magic prefix)
    let decoded = decode_protobuf(&encoded[4..]).unwrap();
    assert_eq!(decoded["apiVersion"], "v1");
    assert_eq!(decoded["kind"], "Pod");
    assert_eq!(decoded["metadata"]["name"], "test-pod");
}

/// Regression test: protobuf-encoded Deployment response must include spec.template.
/// Without this, the Go client's GetNewReplicaSet (EqualIgnoreHash) sees an empty
/// template in the deployment and can never match any RS — the webhook deployment
/// loops forever with "new replicaset is yet to be created".
#[test]
pub fn test_encode_protobuf_deployment_includes_template() {
    use serde_json::json;

    let deployment = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "sample-webhook-deployment",
            "namespace": "webhook-1234",
            "uid": "deploy-uid-001"
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "sample-webhook", "webhook": "true"}},
            "strategy": {"type": "RollingUpdate"},
            "template": {
                "metadata": {"labels": {"app": "sample-webhook", "webhook": "true"}},
                "spec": {
                    "terminationGracePeriodSeconds": 0,
                    "containers": [{
                        "name": "sample-webhook",
                        "image": "registry.k8s.io/e2e-test-images/agnhost:2.56",
                        "args": ["webhook", "--tls-cert-file=/certs/tls.crt"],
                        "ports": [{"containerPort": 8444}],
                        "volumeMounts": [{"name": "webhook-certs", "readOnly": true, "mountPath": "/certs"}]
                    }],
                    "volumes": [{"name": "webhook-certs", "secret": {"secretName": "sample-webhook-secret"}}]
                }
            }
        }
    });

    // Encode to protobuf
    let encoded = encode_protobuf(&deployment).unwrap();
    // Decode back to JSON
    let decoded = decode_protobuf(&encoded[4..]).unwrap();

    // Template must survive the round-trip
    assert!(
        decoded.pointer("/spec/template").is_some(),
        "Deployment protobuf encode must include spec.template"
    );
    assert!(
        decoded.pointer("/spec/template/spec/containers").is_some(),
        "Deployment protobuf encode must include spec.template.spec.containers"
    );

    let containers = decoded["spec"]["template"]["spec"]["containers"]
        .as_array()
        .expect("containers must be an array");
    assert_eq!(containers.len(), 1);
    assert_eq!(containers[0]["name"], "sample-webhook");
    assert_eq!(
        containers[0]["image"],
        "registry.k8s.io/e2e-test-images/agnhost:2.56"
    );

    // Template labels must survive
    let labels = decoded["spec"]["template"]["metadata"]["labels"]
        .as_object()
        .expect("template metadata labels must be an object");
    assert_eq!(
        labels.get("app").and_then(|v| v.as_str()),
        Some("sample-webhook")
    );
    assert_eq!(labels.get("webhook").and_then(|v| v.as_str()), Some("true"));
}

/// Regression test: protobuf-encoded ReplicaSet response must include spec.template.
#[test]
pub fn test_encode_protobuf_replicaset_includes_template() {
    use serde_json::json;

    let rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": "test-rs-abc123",
            "namespace": "default",
            "uid": "rs-uid-001",
            "labels": {"app": "test", "pod-template-hash": "abc123"},
            "ownerReferences": [{"apiVersion": "apps/v1", "kind": "Deployment", "name": "test", "uid": "deploy-uid", "controller": true}]
        },
        "spec": {
            "replicas": 2,
            "selector": {"matchLabels": {"app": "test", "pod-template-hash": "abc123"}},
            "template": {
                "metadata": {"labels": {"app": "test", "pod-template-hash": "abc123"}},
                "spec": {
                    "containers": [{"name": "nginx", "image": "nginx:1.25"}]
                }
            }
        }
    });

    let encoded = encode_protobuf(&rs).unwrap();
    let decoded = decode_protobuf(&encoded[4..]).unwrap();

    assert!(
        decoded.pointer("/spec/template/spec/containers").is_some(),
        "ReplicaSet protobuf encode must include spec.template.spec.containers"
    );
    let containers = decoded["spec"]["template"]["spec"]["containers"]
        .as_array()
        .expect("containers must be an array");
    assert_eq!(containers[0]["image"], "nginx:1.25");
}

/// End-to-end test: simulate the full GetNewReplicaSet flow.
/// Protobuf-decoded deployment → stored in DB → reconcile creates RS →
/// deployment re-read via protobuf GET → RS read via JSON LIST →
/// templates compared like EqualIgnoreHash (must match).
#[tokio::test]
async fn test_deployment_rs_template_match_full_flow() {
    use serde_json::json;

    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

    // Webhook deployment with readinessProbe, volumeMounts, terminationGracePeriodSeconds
    let original_deployment = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "sample-webhook-deployment",
            "namespace": "default",
            "uid": "deploy-webhook-uid-001",
            "labels": {"app": "sample-webhook", "webhook": "true"}
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "sample-webhook", "webhook": "true"}},
            "strategy": {"type": "RollingUpdate"},
            "template": {
                "metadata": {"labels": {"app": "sample-webhook", "webhook": "true"}},
                "spec": {
                    "terminationGracePeriodSeconds": 0,
                    "containers": [{
                        "name": "sample-webhook",
                        "image": "registry.k8s.io/e2e-test-images/agnhost:2.56",
                        "args": ["webhook", "--tls-cert-file=/certs/tls.crt"],
                        "readinessProbe": {
                            "httpGet": {"scheme": "HTTPS", "port": 8444, "path": "/readyz"},
                            "periodSeconds": 1, "successThreshold": 1, "failureThreshold": 30
                        },
                        "ports": [{"containerPort": 8444}],
                        "volumeMounts": [{"name": "webhook-certs", "readOnly": true, "mountPath": "/certs"}]
                    }],
                    "volumes": [{"name": "webhook-certs", "secret": {"secretName": "sample-webhook-secret"}}]
                }
            }
        }
    });

    // Simulate protobuf round-trip (what LenientJson does for protobuf bodies)
    let pb_bytes = encode_protobuf(&original_deployment).unwrap();
    let decoded_deployment = decode_protobuf(&pb_bytes[4..]).unwrap();

    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "sample-webhook-deployment",
            decoded_deployment,
        )
        .await
        .unwrap();

    // Reconcile to create RS
    let deploy_with_rv =
        crate::api::inject_resource_version(created.data, created.resource_version);
    crate::controllers::deployment::reconcile_deployment(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &deploy_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    // Simulate GET deployment → protobuf response → decode (what Go client sees)
    let stored_deploy = db
        .get_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "sample-webhook-deployment",
        )
        .await
        .unwrap()
        .unwrap();
    let deploy_json =
        crate::api::inject_resource_version(stored_deploy.data, stored_deploy.resource_version);
    let deploy_pb = encode_protobuf(&deploy_json).unwrap();
    let deploy_as_seen_by_client = decode_protobuf(&deploy_pb[4..]).unwrap();

    // Simulate LIST RS → JSON response (what Go client sees)
    let rs_list = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(rs_list.items.len(), 1, "Should have 1 RS after reconcile");
    let rs_json = &rs_list.items[0].data;

    // Compare templates like EqualIgnoreHash: strip pod-template-hash, then compare
    let deploy_template = &deploy_as_seen_by_client["spec"]["template"];
    let rs_template = &rs_json["spec"]["template"];
    let mut rs_template_stripped = rs_template.clone();
    if let Some(labels) = rs_template_stripped
        .pointer_mut("/metadata/labels")
        .and_then(|l| l.as_object_mut())
    {
        labels.remove("pod-template-hash");
    }

    assert_eq!(
        deploy_template,
        &rs_template_stripped,
        "Deploy template (protobuf GET) must equal RS template (JSON LIST) \
         after stripping pod-template-hash — this is what EqualIgnoreHash checks.\n\
         Deploy: {}\nRS: {}",
        serde_json::to_string_pretty(deploy_template).unwrap(),
        serde_json::to_string_pretty(&rs_template_stripped).unwrap()
    );
}

#[test]
pub fn test_encode_protobuf_missing_apiversion() {
    use serde_json::json;

    let invalid = json!({
        "kind": "Pod",
        "metadata": {"name": "test"}
    });

    let result = encode_protobuf(&invalid);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Missing apiVersion"));
}

#[test]
pub fn test_encode_protobuf_missing_kind() {
    use serde_json::json;

    let invalid = json!({
        "apiVersion": "v1",
        "metadata": {"name": "test"}
    });

    let result = encode_protobuf(&invalid);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Missing kind"));
}

// ========================
// Secret protobuf encode/decode tests
// ========================

/// Helper: build a Secret protobuf with given data, string_data, and type
pub fn build_secret_pb(
    name: &str,
    data: Vec<(&str, &[u8])>,
    string_data: Vec<(&str, &str)>,
    secret_type: Option<&str>,
) -> k8s_pb::api::core::v1::Secret {
    k8s_pb::api::core::v1::Secret {
        metadata: Some(k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        data: data
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_vec()))
            .collect(),
        string_data: string_data
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        r#type: secret_type.map(|t| t.to_string()),
        ..Default::default()
    }
}

#[test]
pub fn test_secret_protobuf_full_path_stringdata_roundtrip() {
    let secret = build_secret_pb(
        "string-secret",
        vec![],
        vec![("api-key", "sk-12345"), ("endpoint", "https://example.com")],
        Some("Opaque"),
    );

    let mut pb_bytes = Vec::new();
    secret.encode(&mut pb_bytes).unwrap();

    let envelope = build_unknown_envelope("v1", "Secret", &pb_bytes);
    let result = decode_protobuf(&envelope).unwrap();

    assert_eq!(result["apiVersion"], "v1");
    assert_eq!(result["kind"], "Secret");
    assert_eq!(result["metadata"]["name"], "string-secret");
    assert_eq!(result["metadata"]["namespace"], "default");
    assert_eq!(result["stringData"]["api-key"], "sk-12345");
    assert_eq!(result["stringData"]["endpoint"], "https://example.com");
    assert_eq!(result["type"], "Opaque");
    // stringData-only secret should not have data field
    assert!(
        result.get("data").is_none(),
        "data field should be absent when only stringData is set"
    );
}

#[test]
pub fn test_secret_protobuf_full_path_base64_data_roundtrip() {
    let secret = build_secret_pb(
        "binary-secret",
        vec![("username", b"admin"), ("password", b"s3cret!")],
        vec![],
        Some("Opaque"),
    );

    let mut pb_bytes = Vec::new();
    secret.encode(&mut pb_bytes).unwrap();

    let envelope = build_unknown_envelope("v1", "Secret", &pb_bytes);
    let result = decode_protobuf(&envelope).unwrap();

    assert_eq!(result["apiVersion"], "v1");
    assert_eq!(result["kind"], "Secret");
    assert_eq!(result["metadata"]["name"], "binary-secret");
    // data values must be base64-encoded
    assert_eq!(result["data"]["username"], "YWRtaW4=");
    assert_eq!(result["data"]["password"], "czNjcmV0IQ==");
    assert_eq!(result["type"], "Opaque");
    // data-only secret should not have stringData field
    assert!(
        result.get("stringData").is_none(),
        "stringData field should be absent when only data is set"
    );
}

#[test]
pub fn test_secret_protobuf_full_path_type_preserved() {
    // Table-driven: test multiple secret types through full decode path
    let secret_types = vec![
        "Opaque",
        "kubernetes.io/tls",
        "kubernetes.io/dockerconfigjson",
        "kubernetes.io/service-account-token",
        "bootstrap.kubernetes.io/token",
    ];

    for secret_type in secret_types {
        let secret = build_secret_pb(
            "typed-secret",
            vec![("key", b"value")],
            vec![],
            Some(secret_type),
        );

        let mut pb_bytes = Vec::new();
        secret.encode(&mut pb_bytes).unwrap();

        let envelope = build_unknown_envelope("v1", "Secret", &pb_bytes);
        let result = decode_protobuf(&envelope)
            .unwrap_or_else(|e| panic!("Failed to decode Secret with type {}: {}", secret_type, e));

        assert_eq!(
            result["type"], secret_type,
            "Secret type '{}' not preserved through protobuf roundtrip",
            secret_type
        );
    }
}

#[test]
pub fn test_secret_protobuf_full_path_mixed_data_and_stringdata() {
    let secret = build_secret_pb(
        "mixed-secret",
        vec![("cert", b"\x30\x82\x01\x22")],
        vec![("config", "debug=true")],
        Some("Opaque"),
    );

    let mut pb_bytes = Vec::new();
    secret.encode(&mut pb_bytes).unwrap();

    let envelope = build_unknown_envelope("v1", "Secret", &pb_bytes);
    let result = decode_protobuf(&envelope).unwrap();

    // Binary data is base64-encoded
    let expected_b64 = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        b"\x30\x82\x01\x22",
    );
    assert_eq!(result["data"]["cert"], expected_b64);
    // stringData preserved as-is
    assert_eq!(result["stringData"]["config"], "debug=true");
    assert_eq!(result["type"], "Opaque");
}

#[test]
pub fn test_secret_protobuf_encode_decode_roundtrip() {
    // Test the encode path: JSON Secret -> encode_protobuf -> decode_protobuf -> verify
    let secret_json = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "roundtrip-secret", "namespace": "default"},
        "data": {"token": "dG9rZW4xMjM="},
        "type": "Opaque"
    });

    let encoded = encode_protobuf(&secret_json).unwrap();

    // Verify K8s magic prefix
    assert_eq!(
        &encoded[0..4],
        &[0x6b, 0x38, 0x73, 0x00],
        "Missing k8s magic prefix"
    );

    // Decode (strip magic prefix)
    let decoded = decode_protobuf(&encoded[4..]).unwrap();
    assert_eq!(decoded["apiVersion"], "v1");
    assert_eq!(decoded["kind"], "Secret");
    assert_eq!(decoded["metadata"]["name"], "roundtrip-secret");
    assert_eq!(decoded["data"]["token"], "dG9rZW4xMjM=");
    assert_eq!(decoded["type"], "Opaque");
}

#[test]
pub fn test_secret_protobuf_no_type_field() {
    // Secret with no type should not have type in output
    let secret = build_secret_pb("untyped-secret", vec![("key", b"val")], vec![], None);

    let mut pb_bytes = Vec::new();
    secret.encode(&mut pb_bytes).unwrap();

    let envelope = build_unknown_envelope("v1", "Secret", &pb_bytes);
    let result = decode_protobuf(&envelope).unwrap();

    assert_eq!(result["metadata"]["name"], "untyped-secret");
    assert!(
        result.get("type").is_none(),
        "type field should be absent when not set"
    );
    assert_eq!(result["data"]["key"], "dmFs");
}

// ========================
// Secret protobuf encode→decode roundtrip tests (manager-spec)
// JSON → encode_protobuf → decode_protobuf → verify fields preserved
// ========================

#[test]
pub fn test_secret_protobuf_data_roundtrip() {
    let secret_json = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "data-secret", "namespace": "default"},
        "data": {"key": "dmFsdWU="},
        "type": "Opaque"
    });

    let encoded = encode_protobuf(&secret_json).unwrap();
    let decoded = decode_protobuf(&encoded[4..]).unwrap();

    assert_eq!(decoded["apiVersion"], "v1");
    assert_eq!(decoded["kind"], "Secret");
    assert_eq!(decoded["metadata"]["name"], "data-secret");
    assert_eq!(decoded["data"]["key"], "dmFsdWU=");
}

#[test]
pub fn test_secret_protobuf_stringdata_roundtrip() {
    let secret_json = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "stringdata-secret", "namespace": "default"},
        "stringData": {"key": "value"},
        "type": "Opaque"
    });

    let encoded = encode_protobuf(&secret_json).unwrap();
    let decoded = decode_protobuf(&encoded[4..]).unwrap();

    assert_eq!(decoded["apiVersion"], "v1");
    assert_eq!(decoded["kind"], "Secret");
    assert_eq!(decoded["metadata"]["name"], "stringdata-secret");
    assert_eq!(decoded["stringData"]["key"], "value");
}

#[test]
pub fn test_secret_protobuf_type_preserved() {
    let secret_json = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "tls-secret", "namespace": "default"},
        "data": {"tls.crt": "Y2VydA==", "tls.key": "a2V5"},
        "type": "kubernetes.io/tls"
    });

    let encoded = encode_protobuf(&secret_json).unwrap();
    let decoded = decode_protobuf(&encoded[4..]).unwrap();

    assert_eq!(decoded["apiVersion"], "v1");
    assert_eq!(decoded["kind"], "Secret");
    assert_eq!(decoded["type"], "kubernetes.io/tls");
    assert_eq!(decoded["data"]["tls.crt"], "Y2VydA==");
    assert_eq!(decoded["data"]["tls.key"], "a2V5");
}
