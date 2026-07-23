use crate::protobuf::*;
use prost::Message;

/// Helper: wrap raw protobuf bytes in a K8s Unknown envelope
pub fn build_unknown_envelope(api_version: &str, kind: &str, raw: &[u8]) -> Vec<u8> {
    let envelope = Unknown {
        type_meta: Some(TypeMeta {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
        }),
        raw: raw.to_vec(),
        content_encoding: String::new(),
        content_type: String::new(),
    };
    let mut buf = Vec::new();
    envelope.encode(&mut buf).unwrap();
    buf
}

/// Helper: build a standard Deployment protobuf message
pub fn build_deployment_pb() -> Vec<u8> {
    let pb = k8s_pb::api::apps::v1::Deployment {
        metadata: Some(k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some("nginx".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        spec: Some(k8s_pb::api::apps::v1::DeploymentSpec {
            replicas: Some(3),
            selector: Some(k8s_pb::apimachinery::pkg::apis::meta::v1::LabelSelector {
                match_labels: vec![("app".to_string(), "nginx".to_string())]
                    .into_iter()
                    .collect(),
                ..Default::default()
            }),
            template: Some(k8s_pb::api::core::v1::PodTemplateSpec {
                metadata: Some(k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta {
                    labels: vec![("app".to_string(), "nginx".to_string())]
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
    buf
}

/// Helper: build a standard Pod protobuf message
pub fn build_pod_pb() -> Vec<u8> {
    let pb = k8s_pb::api::core::v1::Pod {
        metadata: Some(k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some("test-pod".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        spec: Some(k8s_pb::api::core::v1::PodSpec {
            containers: vec![k8s_pb::api::core::v1::Container {
                name: Some("app".to_string()),
                image: Some("busybox:1.36".to_string()),
                command: vec!["/bin/sh".to_string()],
                ..Default::default()
            }],
            restart_policy: Some("Never".to_string()),
            active_deadline_seconds: Some(3600),
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut buf = Vec::new();
    pb.encode(&mut buf).unwrap();
    buf
}

/// Helper: build a ConfigMap protobuf message
pub fn build_configmap_pb() -> Vec<u8> {
    let pb = k8s_pb::api::core::v1::ConfigMap {
        metadata: Some(k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some("my-config".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        data: vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]
        .into_iter()
        .collect(),
        ..Default::default()
    };
    let mut buf = Vec::new();
    pb.encode(&mut buf).unwrap();
    buf
}

/// Helper: build a Service protobuf message
pub fn build_service_pb() -> Vec<u8> {
    let pb = k8s_pb::api::core::v1::Service {
        metadata: Some(k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some("my-svc".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        spec: Some(k8s_pb::api::core::v1::ServiceSpec {
            ports: vec![k8s_pb::api::core::v1::ServicePort {
                port: Some(80),
                protocol: Some("TCP".to_string()),
                ..Default::default()
            }],
            selector: vec![("app".to_string(), "nginx".to_string())]
                .into_iter()
                .collect(),
            r#type: Some("ClusterIP".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut buf = Vec::new();
    pb.encode(&mut buf).unwrap();
    buf
}

/// Helper: build a ClusterRole protobuf message
pub fn build_clusterrole_pb() -> Vec<u8> {
    let pb = k8s_pb::api::rbac::v1::ClusterRole {
        metadata: Some(k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some("pod-reader".to_string()),
            ..Default::default()
        }),
        rules: vec![k8s_pb::api::rbac::v1::PolicyRule {
            verbs: vec!["get".to_string(), "list".to_string()],
            api_groups: vec!["".to_string()],
            resources: vec!["pods".to_string()],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut buf = Vec::new();
    pb.encode(&mut buf).unwrap();
    buf
}

// ========================
// Full-path round-trip tests (table-driven)
// Tests decode_protobuf() through the Unknown envelope — the actual ingestion path
// ========================
