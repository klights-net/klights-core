use super::decode_tests_helpers::*;
use crate::protobuf::*;
use prost::Message;
use serde_json::json;

#[test]
pub fn test_protobuf_full_path_round_trip() {
    struct Case {
        name: &'static str,
        kind: &'static str,
        api_version: &'static str,
        build_pb: fn() -> Vec<u8>,
        assertions: fn(serde_json::Value),
    }

    let cases = vec![
        Case {
            name: "Deployment",
            kind: "Deployment",
            api_version: "apps/v1",
            build_pb: build_deployment_pb,
            assertions: |json| {
                assert_eq!(json["apiVersion"], "apps/v1");
                assert_eq!(json["kind"], "Deployment");
                assert_eq!(json["metadata"]["name"], "nginx");
                assert_eq!(json["spec"]["replicas"], 3);
                let containers = json["spec"]["template"]["spec"]["containers"]
                    .as_array()
                    .unwrap();
                assert_eq!(containers[0]["image"], "nginx:latest");
            },
        },
        Case {
            name: "Pod",
            kind: "Pod",
            api_version: "v1",
            build_pb: build_pod_pb,
            assertions: |json| {
                assert_eq!(json["apiVersion"], "v1");
                assert_eq!(json["kind"], "Pod");
                assert_eq!(json["metadata"]["name"], "test-pod");
                assert_eq!(json["spec"]["containers"][0]["image"], "busybox:1.36");
                assert_eq!(json["spec"]["containers"][0]["command"][0], "/bin/sh");
                assert_eq!(json["spec"]["restartPolicy"], "Never");
                assert_eq!(json["spec"]["activeDeadlineSeconds"], 3600);
            },
        },
        Case {
            name: "ConfigMap",
            kind: "ConfigMap",
            api_version: "v1",
            build_pb: build_configmap_pb,
            assertions: |json| {
                assert_eq!(json["apiVersion"], "v1");
                assert_eq!(json["kind"], "ConfigMap");
                assert_eq!(json["metadata"]["name"], "my-config");
                assert_eq!(json["data"]["key1"], "value1");
                assert_eq!(json["data"]["key2"], "value2");
            },
        },
        Case {
            name: "Service",
            kind: "Service",
            api_version: "v1",
            build_pb: build_service_pb,
            assertions: |json| {
                assert_eq!(json["apiVersion"], "v1");
                assert_eq!(json["kind"], "Service");
                assert_eq!(json["spec"]["ports"][0]["port"], 80);
                assert_eq!(json["spec"]["selector"]["app"], "nginx");
                assert_eq!(json["spec"]["type"], "ClusterIP");
            },
        },
        Case {
            name: "ClusterRole",
            kind: "ClusterRole",
            api_version: "rbac.authorization.k8s.io/v1",
            build_pb: build_clusterrole_pb,
            assertions: |json| {
                assert_eq!(json["apiVersion"], "rbac.authorization.k8s.io/v1");
                assert_eq!(json["kind"], "ClusterRole");
                assert_eq!(json["rules"][0]["verbs"], json!(["get", "list"]));
                assert_eq!(json["rules"][0]["resources"], json!(["pods"]));
            },
        },
    ];

    for case in &cases {
        let pb_bytes = (case.build_pb)();
        let envelope = build_unknown_envelope(case.api_version, case.kind, &pb_bytes);
        let result = decode_protobuf(&envelope)
            .unwrap_or_else(|e| panic!("Failed to decode {} via full path: {}", case.name, e));
        (case.assertions)(result);
    }
}

#[test]
pub fn test_protobuf_json_in_envelope() {
    // kubectl sometimes sends JSON inside the protobuf Unknown envelope
    let json_body = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "test", "namespace": "default"},
        "data": {"foo": "bar"}
    });
    let json_bytes = serde_json::to_vec(&json_body).unwrap();

    let envelope = build_unknown_envelope("v1", "ConfigMap", &json_bytes);
    let result = decode_protobuf(&envelope).unwrap();

    // Should pass through JSON directly without protobuf decode
    assert_eq!(result["kind"], "ConfigMap");
    assert_eq!(result["data"]["foo"], "bar");
}

#[test]
pub fn test_protobuf_unsupported_kind_returns_error() {
    // Build a fake protobuf payload for an unsupported kind
    let envelope = build_unknown_envelope("v1", "UnsupportedKind", &[0x08, 0x01]);
    let result = decode_protobuf(&envelope);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("UnsupportedKind"),
        "Error should mention the unsupported kind, got: {}",
        err_msg
    );
}

#[test]
pub fn test_protobuf_malformed_envelope_returns_error() {
    // Garbage bytes that aren't valid protobuf
    let result = decode_protobuf(&[0xFF, 0xFF, 0xFF, 0xFF]);
    // Should return an error, not panic
    assert!(result.is_err());
}

// ========================
// Existing per-type tests (kept for deeper field coverage)
// ========================

#[test]
pub fn test_deployment_protobuf_decode_preserves_spec() {
    // Create a Deployment protobuf message with spec
    let pb_deployment = k8s_pb::api::apps::v1::Deployment {
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

    // Encode to protobuf bytes
    let mut pb_bytes = Vec::new();
    pb_deployment.encode(&mut pb_bytes).unwrap();

    // Decode via decode_protobuf_resource
    let result = decode_protobuf_resource("", "Deployment", &pb_bytes).unwrap();

    // Verify spec is present
    assert!(result.get("spec").is_some(), "spec field must be present");
    assert_eq!(result["spec"]["replicas"], 3);

    // Verify spec.template.spec.containers[0].image is present
    assert!(result["spec"]["template"]["spec"]["containers"].is_array());
    let containers = result["spec"]["template"]["spec"]["containers"]
        .as_array()
        .unwrap();
    assert_eq!(containers.len(), 1);
    assert_eq!(containers[0]["name"], "nginx");
    assert_eq!(containers[0]["image"], "nginx:latest");
}

#[test]
pub fn test_deployment_protobuf_decode_preserves_status_conditions() {
    let pb_deployment = k8s_pb::api::apps::v1::Deployment {
        metadata: Some(k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some("nginx".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        status: Some(k8s_pb::api::apps::v1::DeploymentStatus {
            conditions: vec![k8s_pb::api::apps::v1::DeploymentCondition {
                r#type: Some("StatusUpdate".to_string()),
                status: Some("True".to_string()),
                reason: Some("E2E".to_string()),
                message: Some("Set from e2e test".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    };

    let mut pb_bytes = Vec::new();
    pb_deployment.encode(&mut pb_bytes).unwrap();

    let result = decode_protobuf_resource("apps/v1", "Deployment", &pb_bytes).unwrap();

    assert_eq!(
        result["status"]["conditions"][0]["type"], "StatusUpdate",
        "Deployment status.conditions[].type must survive protobuf decode"
    );
    assert_eq!(result["status"]["conditions"][0]["status"], "True");
    assert_eq!(result["status"]["conditions"][0]["reason"], "E2E");
    assert_eq!(
        result["status"]["conditions"][0]["message"],
        "Set from e2e test"
    );
}

#[test]
pub fn test_secret_protobuf_decode_preserves_data() {
    use k8s_pb::api::core::v1::Secret;
    use k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use prost::Message;

    let secret = Secret {
        metadata: Some(ObjectMeta {
            name: Some("my-secret".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        data: vec![
            ("username".to_string(), b"admin".to_vec()),
            ("password".to_string(), b"secret123".to_vec()),
        ]
        .into_iter()
        .collect(),
        string_data: vec![("plain".to_string(), "plaintext".to_string())]
            .into_iter()
            .collect(),
        r#type: Some("Opaque".to_string()),
        ..Default::default()
    };

    let mut buf = Vec::new();
    secret.encode(&mut buf).unwrap();

    let result = pb_secret_to_json(&secret).unwrap();

    // Verify data fields are base64 encoded
    assert_eq!(result["data"]["username"], "YWRtaW4=");
    assert_eq!(result["data"]["password"], "c2VjcmV0MTIz");
    // Verify stringData preserved
    assert_eq!(result["stringData"]["plain"], "plaintext");
    assert_eq!(result["type"], "Opaque");
}

/// P0-E2E-20260424b-09: immutable field must survive protobuf decode so the
/// immutable-enforcement check in the update handler fires on proto bodies.
#[test]
pub fn test_secret_protobuf_decode_preserves_immutable_flag() {
    use k8s_pb::api::core::v1::Secret;
    use k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    let secret = Secret {
        metadata: Some(ObjectMeta {
            name: Some("imm-secret".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        data: vec![("key".to_string(), b"val".to_vec())]
            .into_iter()
            .collect(),
        immutable: Some(true),
        ..Default::default()
    };

    let result = pb_secret_to_json(&secret).unwrap();
    assert_eq!(
        result["immutable"], true,
        "immutable: true must survive proto → JSON decode"
    );
}

/// P0-E2E-20260424b-09: immutable field must survive ConfigMap protobuf decode.
#[test]
pub fn test_configmap_protobuf_decode_preserves_immutable_flag() {
    use k8s_pb::api::core::v1::ConfigMap;
    use k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    let cm = ConfigMap {
        metadata: Some(ObjectMeta {
            name: Some("imm-cm".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        data: vec![("key".to_string(), "val".to_string())]
            .into_iter()
            .collect(),
        immutable: Some(true),
        ..Default::default()
    };

    let result = pb_configmap_to_json(&cm).unwrap();
    assert_eq!(
        result["immutable"], true,
        "immutable: true must survive proto → JSON decode for ConfigMap"
    );
}

#[test]
pub fn test_statefulset_protobuf_decode_preserves_spec() {
    use k8s_pb::api::apps::v1::{
        RollingUpdateStatefulSetStrategy, StatefulSet, StatefulSetSpec, StatefulSetUpdateStrategy,
    };
    use k8s_pb::api::core::v1::{Container, PodSpec, PodTemplateSpec};
    use k8s_pb::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
    use prost::Message;

    let sts = StatefulSet {
        metadata: Some(ObjectMeta {
            name: Some("web".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        spec: Some(StatefulSetSpec {
            replicas: Some(3),
            service_name: Some("nginx".to_string()),
            selector: Some(LabelSelector {
                match_labels: vec![("app".to_string(), "nginx".to_string())]
                    .into_iter()
                    .collect(),
                ..Default::default()
            }),
            pod_management_policy: Some("OrderedReady".to_string()),
            update_strategy: Some(StatefulSetUpdateStrategy {
                r#type: Some("RollingUpdate".to_string()),
                rolling_update: Some(RollingUpdateStatefulSetStrategy {
                    partition: Some(3),
                    ..Default::default()
                }),
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
                        image: Some("nginx:1.21".to_string()),
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

    let result = pb_statefulset_to_json(&sts).unwrap();

    assert_eq!(result["spec"]["replicas"], 3);
    assert_eq!(result["spec"]["serviceName"], "nginx");
    assert_eq!(result["spec"]["podManagementPolicy"], "OrderedReady");
    assert_eq!(result["spec"]["updateStrategy"]["type"], "RollingUpdate");
    assert_eq!(
        result["spec"]["updateStrategy"]["rollingUpdate"]["partition"],
        3
    );
    assert_eq!(
        result["spec"]["template"]["spec"]["containers"][0]["image"],
        "nginx:1.21"
    );
}

#[test]
pub fn test_controllerrevision_protobuf_decode_preserves_data_and_revision() {
    let cr = k8s_pb::api::apps::v1::ControllerRevision {
        metadata: None,
        data: Some(k8s_pb::apimachinery::pkg::runtime::RawExtension {
            raw: Some(
                serde_json::to_vec(&json!({
                    "spec": {"template": {"$patch": "replace"}}
                }))
                .unwrap(),
            ),
        }),
        revision: Some(2),
    };
    let mut raw = Vec::new();
    cr.encode(&mut raw).unwrap();

    let decoded = decode_protobuf_resource("apps/v1", "ControllerRevision", &raw).unwrap();
    assert_eq!(decoded["apiVersion"], "apps/v1");
    assert_eq!(decoded["kind"], "ControllerRevision");
    assert_eq!(decoded["revision"], 2);
    assert_eq!(decoded["data"]["spec"]["template"]["$patch"], "replace");
}

#[test]
pub fn test_daemonset_protobuf_decode_preserves_spec_and_status_conditions() {
    use k8s_pb::api::apps::v1::{DaemonSet, DaemonSetCondition, DaemonSetSpec, DaemonSetStatus};
    use k8s_pb::api::core::v1::{Container, PodSpec, PodTemplateSpec};
    use k8s_pb::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
    use prost::Message;

    let ds = DaemonSet {
        metadata: Some(ObjectMeta {
            name: Some("fluentd".to_string()),
            namespace: Some("kube-system".to_string()),
            ..Default::default()
        }),
        status: Some(DaemonSetStatus {
            current_number_scheduled: Some(1),
            desired_number_scheduled: Some(1),
            number_ready: Some(1),
            conditions: vec![DaemonSetCondition {
                r#type: Some("StatusUpdate".to_string()),
                status: Some("True".to_string()),
                reason: Some("E2E".to_string()),
                message: Some("Set from e2e test".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        }),
        spec: Some(DaemonSetSpec {
            selector: Some(LabelSelector {
                match_labels: vec![("app".to_string(), "fluentd".to_string())]
                    .into_iter()
                    .collect(),
                ..Default::default()
            }),
            template: Some(PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: vec![("app".to_string(), "fluentd".to_string())]
                        .into_iter()
                        .collect(),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: Some("fluentd".to_string()),
                        image: Some("fluent/fluentd:v1.14".to_string()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            }),
            ..Default::default()
        }),
    };

    let mut buf = Vec::new();
    ds.encode(&mut buf).unwrap();

    let result = pb_daemonset_to_json(&ds).unwrap();

    assert_eq!(
        result["spec"]["template"]["spec"]["containers"][0]["image"],
        "fluent/fluentd:v1.14"
    );
    assert_eq!(result["status"]["conditions"][0]["type"], "StatusUpdate");
    assert_eq!(result["status"]["conditions"][0]["reason"], "E2E");
}

#[test]
pub fn test_job_protobuf_decode_preserves_spec() {
    use k8s_pb::api::batch::v1::{Job, JobSpec, JobStatus, SuccessPolicy, SuccessPolicyRule};
    use k8s_pb::api::core::v1::{Container, PodSpec, PodTemplateSpec};
    use k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use prost::Message;

    let job = Job {
        metadata: Some(ObjectMeta {
            name: Some("backup".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        spec: Some(JobSpec {
            completions: Some(1),
            parallelism: Some(1),
            backoff_limit: Some(3),
            completion_mode: Some("Indexed".to_string()),
            backoff_limit_per_index: Some(0),
            max_failed_indexes: Some(0),
            success_policy: Some(SuccessPolicy {
                rules: vec![SuccessPolicyRule {
                    succeeded_indexes: Some("0".to_string()),
                    succeeded_count: Some(1),
                }],
            }),
            template: Some(PodTemplateSpec {
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: Some("backup".to_string()),
                        image: Some("backup:latest".to_string()),
                        ..Default::default()
                    }],
                    restart_policy: Some("Never".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }),
        status: Some(JobStatus {
            failed_indexes: Some("0".to_string()),
            completed_indexes: Some("1,3".to_string()),
            ..Default::default()
        }),
    };

    let mut buf = Vec::new();
    job.encode(&mut buf).unwrap();

    let result = pb_job_to_json(&job).unwrap();

    assert_eq!(result["spec"]["completions"], 1);
    assert_eq!(result["spec"]["parallelism"], 1);
    assert_eq!(result["spec"]["backoffLimit"], 3);
    assert_eq!(result["spec"]["completionMode"], "Indexed");
    assert_eq!(result["spec"]["backoffLimitPerIndex"], 0);
    assert_eq!(result["spec"]["maxFailedIndexes"], 0);
    assert_eq!(
        result["spec"]["successPolicy"]["rules"][0]["succeededIndexes"],
        "0"
    );
    assert_eq!(
        result["spec"]["successPolicy"]["rules"][0]["succeededCount"],
        1
    );
    assert_eq!(result["status"]["failedIndexes"], "0");
    assert_eq!(result["status"]["completedIndexes"], "1,3");
    assert_eq!(
        result["spec"]["template"]["spec"]["containers"][0]["image"],
        "backup:latest"
    );
    assert_eq!(result["spec"]["template"]["spec"]["restartPolicy"], "Never");
}

#[test]
pub fn test_job_protobuf_roundtrip_preserves_success_policy() {
    let original = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "job-success-policy", "namespace": "default"},
        "spec": {
            "completionMode": "Indexed",
            "parallelism": 2,
            "completions": 2,
            "successPolicy": {
                "rules": [{
                    "succeededIndexes": "0-1",
                    "succeededCount": 2
                }]
            },
            "template": {
                "spec": {
                    "restartPolicy": "Never",
                    "containers": [{"name": "c", "image": "nginx"}]
                }
            }
        }
    });

    let bytes = encode_protobuf(&original).expect("job encode protobuf must succeed");
    let decoded = decode_protobuf(&bytes).expect("job decode protobuf must succeed");

    assert_eq!(
        decoded["spec"]["successPolicy"]["rules"][0]["succeededIndexes"],
        "0-1"
    );
    assert_eq!(
        decoded["spec"]["successPolicy"]["rules"][0]["succeededCount"],
        2
    );
}

#[test]
pub fn test_job_protobuf_roundtrip_preserves_status_conditions() {
    let original = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "job-cond", "namespace": "default"},
        "spec": {
            "parallelism": 1,
            "completions": 1,
            "template": {
                "spec": {
                    "restartPolicy": "Never",
                    "containers": [{"name": "c", "image": "nginx"}]
                }
            }
        },
        "status": {
            "active": 1,
            "conditions": [{
                "type": "CustomConditionType",
                "status": "True",
                "reason": "Patched",
                "message": "status condition from patch",
                "lastTransitionTime": "2026-04-26T00:00:00Z"
            }]
        }
    });

    let bytes = encode_protobuf(&original).expect("job encode protobuf must succeed");
    let decoded = decode_protobuf(&bytes).expect("job decode protobuf must succeed");

    assert_eq!(
        decoded["status"]["conditions"][0]["type"],
        "CustomConditionType"
    );
    assert_eq!(decoded["status"]["conditions"][0]["status"], "True");
    assert_eq!(decoded["status"]["conditions"][0]["reason"], "Patched");
    assert_eq!(
        decoded["status"]["conditions"][0]["message"],
        "status condition from patch"
    );
}

#[test]
pub fn test_clusterrole_protobuf_decode_preserves_rules() {
    use k8s_pb::api::rbac::v1::{ClusterRole, PolicyRule};
    use k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use prost::Message;

    let cr = ClusterRole {
        metadata: Some(ObjectMeta {
            name: Some("pod-reader".to_string()),
            ..Default::default()
        }),
        rules: vec![PolicyRule {
            verbs: vec!["get".to_string(), "list".to_string()],
            api_groups: vec!["".to_string()],
            resources: vec!["pods".to_string()],
            ..Default::default()
        }],
        ..Default::default()
    };

    let mut buf = Vec::new();
    cr.encode(&mut buf).unwrap();

    let result = pb_clusterrole_to_json(&cr).unwrap();

    assert_eq!(result["rules"][0]["verbs"], json!(["get", "list"]));
    assert_eq!(result["rules"][0]["apiGroups"], json!([""]));
    assert_eq!(result["rules"][0]["resources"], json!(["pods"]));
}

#[test]
pub fn test_rolebinding_protobuf_decode_preserves_roleref_and_subjects() {
    use k8s_pb::api::rbac::v1::{RoleBinding, RoleRef, Subject};
    use k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use prost::Message;

    let rb = RoleBinding {
        metadata: Some(ObjectMeta {
            name: Some("read-pods".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        role_ref: Some(RoleRef {
            api_group: Some("rbac.authorization.k8s.io".to_string()),
            kind: Some("Role".to_string()),
            name: Some("pod-reader".to_string()),
        }),
        subjects: vec![Subject {
            kind: Some("ServiceAccount".to_string()),
            name: Some("default".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }],
    };

    let mut buf = Vec::new();
    rb.encode(&mut buf).unwrap();

    let result = pb_rolebinding_to_json(&rb).unwrap();

    assert_eq!(result["roleRef"]["kind"], "Role");
    assert_eq!(result["roleRef"]["name"], "pod-reader");
    assert_eq!(result["subjects"][0]["kind"], "ServiceAccount");
    assert_eq!(result["subjects"][0]["name"], "default");
}

#[test]
pub fn test_tokenreview_protobuf_decode_preserves_spec_fields() {
    use k8s_pb::api::authentication::v1::{TokenReview, TokenReviewSpec};
    use prost::Message;

    let tr = TokenReview {
        spec: Some(TokenReviewSpec {
            token: Some("test-sa-token".to_string()),
            audiences: vec!["api".to_string(), "metrics".to_string()],
        }),
        ..Default::default()
    };

    let mut buf = Vec::new();
    tr.encode(&mut buf).unwrap();

    let result = decode_protobuf_resource("authentication.k8s.io/v1", "TokenReview", &buf)
        .expect("tokenreview decode must succeed");

    assert_eq!(result["kind"], "TokenReview");
    assert_eq!(result["apiVersion"], "authentication.k8s.io/v1");
    assert_eq!(result["spec"]["token"], "test-sa-token");
    assert_eq!(result["spec"]["audiences"], json!(["api", "metrics"]));
}

#[test]
pub fn test_serviceaccount_protobuf_decode_preserves_secrets() {
    use k8s_pb::api::core::v1::{ObjectReference, ServiceAccount};
    use k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use prost::Message;

    let sa = ServiceAccount {
        metadata: Some(ObjectMeta {
            name: Some("default".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        secrets: vec![ObjectReference {
            name: Some("default-token-abc".to_string()),
            ..Default::default()
        }],
        automount_service_account_token: Some(true),
        image_pull_secrets: vec![k8s_pb::api::core::v1::LocalObjectReference {
            name: Some("regcred".to_string()),
        }],
    };

    let mut buf = Vec::new();
    sa.encode(&mut buf).unwrap();

    let result = pb_serviceaccount_to_json(&sa).unwrap();

    assert_eq!(result["secrets"][0]["name"], "default-token-abc");
    assert_eq!(result["automountServiceAccountToken"], true);
    assert_eq!(result["imagePullSecrets"][0]["name"], "regcred");
}

#[test]
pub fn test_serviceaccount_protobuf_encode_decode_preserves_image_pull_secrets() {
    let sa_json = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {"name": "default", "namespace": "default"},
        "imagePullSecrets": [{"name": "regcred"}]
    });

    let wire = encode_protobuf(&sa_json).expect("encode serviceaccount");
    let decoded = decode_protobuf(&wire[4..]).expect("decode serviceaccount");

    assert_eq!(decoded["metadata"]["name"], "default");
    assert_eq!(decoded["imagePullSecrets"][0]["name"], "regcred");
}

#[test]
pub fn test_endpoints_protobuf_decode_preserves_subsets() {
    use k8s_pb::api::core::v1::{
        EndpointAddress, EndpointPort, EndpointSubset, Endpoints, ObjectReference,
    };
    use k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use prost::Message;

    let ep = Endpoints {
        metadata: Some(ObjectMeta {
            name: Some("kubernetes".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        subsets: vec![EndpointSubset {
            addresses: vec![EndpointAddress {
                ip: Some("10.0.0.1".to_string()),
                target_ref: Some(ObjectReference {
                    kind: Some("Pod".to_string()),
                    name: Some("pod-1".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ports: vec![EndpointPort {
                port: Some(443),
                protocol: Some("TCP".to_string()),
                name: Some("https".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    let mut buf = Vec::new();
    ep.encode(&mut buf).unwrap();

    let result = pb_endpoints_to_json(&ep).unwrap();

    assert_eq!(result["subsets"][0]["addresses"][0]["ip"], "10.0.0.1");
    assert_eq!(
        result["subsets"][0]["addresses"][0]["targetRef"]["kind"],
        "Pod"
    );
    assert_eq!(result["subsets"][0]["ports"][0]["port"], 443);
}

#[test]
pub fn test_endpoints_protobuf_encode_decode_preserves_subsets() {
    let ep_json = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {"name": "kubernetes", "namespace": "default"},
        "subsets": [{
            "addresses": [{"ip": "10.0.0.1"}],
            "ports": [{"port": 443, "protocol": "TCP", "name": "https"}]
        }]
    });

    let wire = encode_protobuf(&ep_json).expect("encode endpoints");
    let decoded = decode_protobuf(&wire[4..]).expect("decode endpoints");

    assert_eq!(decoded["subsets"][0]["addresses"][0]["ip"], "10.0.0.1");
    assert_eq!(decoded["subsets"][0]["ports"][0]["port"], 443);
}

#[test]
pub fn test_service_protobuf_encode_decode_preserves_target_port() {
    let svc_json = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "svc", "namespace": "default"},
        "spec": {
            "selector": {"app": "demo"},
            "ports": [{
                "port": 80,
                "protocol": "TCP",
                "targetPort": 8080
            }]
        }
    });

    let wire = encode_protobuf(&svc_json).expect("encode service");
    let decoded = decode_protobuf(&wire[4..]).expect("decode service");

    assert_eq!(decoded["spec"]["ports"][0]["port"], 80);
    assert_eq!(decoded["spec"]["ports"][0]["targetPort"], 8080);
}

#[test]
pub fn test_service_protobuf_decode_prefers_string_target_port_when_type_is_string() {
    use k8s_pb::api::core::v1::{Service, ServicePort, ServiceSpec};
    use k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use k8s_pb::apimachinery::pkg::util::intstr::IntOrString;

    let svc = Service {
        metadata: Some(ObjectMeta {
            name: Some("svc".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        spec: Some(ServiceSpec {
            ports: vec![ServicePort {
                name: Some("http".to_string()),
                port: Some(80),
                // Go clients may still carry intVal=0 on string-typed values.
                target_port: Some(IntOrString {
                    r#type: Some(1),
                    int_val: Some(0),
                    str_val: Some("portname1".to_string()),
                }),
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    };

    let decoded = pb_service_to_json(&svc).unwrap();

    assert_eq!(decoded["spec"]["ports"][0]["targetPort"], "portname1");
}

#[test]
pub fn test_pod_protobuf_encode_decode_preserves_host_network() {
    let pod_json = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "hostnet-pod", "namespace": "default"},
        "spec": {
            "hostNetwork": true,
            "containers": [{"name": "app", "image": "nginx"}]
        }
    });

    let wire = encode_protobuf(&pod_json).expect("encode pod");
    let decoded = decode_protobuf(&wire[4..]).expect("decode pod");

    assert_eq!(decoded["spec"]["hostNetwork"], true);
}

#[test]
pub fn test_pod_protobuf_decode_omits_empty_hostname_and_subdomain() {
    use k8s_pb::api::core::v1::{Container, Pod, PodSpec};
    use k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    let pod = Pod {
        metadata: Some(ObjectMeta {
            name: Some("p".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        spec: Some(PodSpec {
            hostname: Some(String::new()),
            subdomain: Some(String::new()),
            containers: vec![Container {
                name: Some("app".to_string()),
                image: Some("nginx".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    };

    let decoded = pb_pod_to_json(&pod).unwrap();
    assert!(decoded.pointer("/spec/hostname").is_none());
    assert!(decoded.pointer("/spec/subdomain").is_none());
}

#[test]
pub fn test_service_protobuf_encode_decode_preserves_external_name() {
    let svc_json = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "external-svc", "namespace": "default"},
        "spec": {
            "type": "ExternalName",
            "externalName": "example.com"
        }
    });

    let wire = encode_protobuf(&svc_json).expect("encode externalname service");
    let decoded = decode_protobuf(&wire[4..]).expect("decode externalname service");

    assert_eq!(decoded["spec"]["type"], "ExternalName");
    assert_eq!(decoded["spec"]["externalName"], "example.com");
}

#[test]
pub fn test_service_protobuf_encode_decode_preserves_session_affinity() {
    let svc_json = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "svc", "namespace": "default"},
        "spec": {
            "ports": [{"port": 80, "targetPort": 8080}],
            "sessionAffinity": "None"
        }
    });

    let wire = encode_protobuf(&svc_json).expect("encode service");
    let decoded = decode_protobuf(&wire[4..]).expect("decode service");

    assert_eq!(decoded["spec"]["sessionAffinity"], "None");
}

#[test]
pub fn test_service_protobuf_encode_defaults_session_affinity_none_when_missing() {
    let svc_json = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "svc-default", "namespace": "default"},
        "spec": {
            "ports": [{"port": 80, "targetPort": 8080}]
        }
    });

    let wire = encode_protobuf(&svc_json).expect("encode service");
    let decoded = decode_protobuf(&wire[4..]).expect("decode service");

    assert_eq!(decoded["spec"]["sessionAffinity"], "None");
}
