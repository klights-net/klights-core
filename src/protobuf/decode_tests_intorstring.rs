use crate::protobuf::*;

#[test]
pub fn test_pod_protobuf_encode_decode_preserves_overhead() {
    let pod_json = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "pod-overhead", "namespace": "default"},
        "spec": {
            "runtimeClassName": "with-overhead",
            "overhead": {
                "cpu": "10m",
                "memory": "16Mi"
            },
            "containers": [{
                "name": "c",
                "image": "busybox"
            }]
        }
    });

    let wire = encode_protobuf(&pod_json).expect("encode pod");
    let decoded = decode_protobuf(&wire[4..]).expect("decode pod");

    assert_eq!(decoded["spec"]["runtimeClassName"], "with-overhead");
    assert_eq!(decoded["spec"]["overhead"]["cpu"], "10m");
    assert_eq!(decoded["spec"]["overhead"]["memory"], "16Mi");
}

#[test]
pub fn test_service_protobuf_decode_preserves_status_conditions() {
    use k8s_pb::api::core::v1::{LoadBalancerStatus, Service, ServiceStatus};
    use k8s_pb::apimachinery::pkg::apis::meta::v1::{Condition, ObjectMeta, Time};
    use prost::Message;

    let svc = Service {
        metadata: Some(ObjectMeta {
            name: Some("svc-status".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        status: Some(ServiceStatus {
            load_balancer: Some(LoadBalancerStatus {
                ingress: vec![k8s_pb::api::core::v1::LoadBalancerIngress {
                    ip: Some("203.0.113.1".to_string()),
                    ..Default::default()
                }],
            }),
            conditions: vec![Condition {
                r#type: Some("LoadBalancerReady".to_string()),
                status: Some("True".to_string()),
                reason: Some("E2EPatched".to_string()),
                message: Some("patched status condition".to_string()),
                observed_generation: Some(2),
                last_transition_time: Some(Time {
                    seconds: Some(1_777_100_000),
                    nanos: Some(0),
                }),
            }],
        }),
        ..Default::default()
    };

    let mut buf = Vec::new();
    svc.encode(&mut buf).unwrap();

    let decoded = pb_service_to_json(&svc).unwrap();
    assert_eq!(
        decoded["status"]["loadBalancer"]["ingress"][0]["ip"],
        "203.0.113.1"
    );
    assert_eq!(
        decoded["status"]["conditions"][0]["type"],
        "LoadBalancerReady"
    );
    assert_eq!(decoded["status"]["conditions"][0]["status"], "True");
    assert_eq!(decoded["status"]["conditions"][0]["reason"], "E2EPatched");
    assert_eq!(decoded["status"]["conditions"][0]["observedGeneration"], 2);
}

#[test]
pub fn test_json_service_to_pb_preserves_status_conditions() {
    use k8s_openapi::api::core::v1::{LoadBalancerStatus, Service, ServiceStatus};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, ObjectMeta, Time};
    use std::str::FromStr;

    let svc = Service {
        metadata: ObjectMeta {
            name: Some("svc-status-encode".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        status: Some(ServiceStatus {
            load_balancer: Some(LoadBalancerStatus {
                ingress: Some(vec![k8s_openapi::api::core::v1::LoadBalancerIngress {
                    ip: Some("203.0.113.5".to_string()),
                    ..Default::default()
                }]),
            }),
            conditions: Some(vec![Condition {
                type_: "LoadBalancerReady".to_string(),
                status: "True".to_string(),
                reason: "E2EPatched".to_string(),
                message: "persist condition".to_string(),
                observed_generation: Some(3),
                last_transition_time: Time(
                    chrono::DateTime::from_str("2026-04-25T16:30:00Z").unwrap(),
                ),
            }]),
        }),
        ..Default::default()
    };

    let raw = serde_json::to_value(&svc).expect("serialize service");
    let pb = json_service_to_pb(&svc, &raw).unwrap();
    let status = pb.status.as_ref().expect("status must be present");
    assert_eq!(
        status
            .load_balancer
            .as_ref()
            .and_then(|lb| lb.ingress.first())
            .and_then(|i| i.ip.as_deref()),
        Some("203.0.113.5")
    );
    assert_eq!(status.conditions.len(), 1);
    let cond = &status.conditions[0];
    assert_eq!(cond.r#type.as_deref(), Some("LoadBalancerReady"));
    assert_eq!(cond.status.as_deref(), Some("True"));
    assert_eq!(cond.reason.as_deref(), Some("E2EPatched"));
    assert_eq!(cond.observed_generation, Some(3));
    assert!(cond.last_transition_time.is_some());
}

#[test]
pub fn test_rbac_protobuf_encode_decode_preserves_rules_subjects_and_role_ref() {
    let cluster_role = serde_json::json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRole",
        "metadata": {"name": "pod-reader"},
        "rules": [{
            "apiGroups": [""],
            "resources": ["pods"],
            "verbs": ["get", "list"]
        }]
    });
    let wire_cr = encode_protobuf(&cluster_role).expect("encode clusterrole");
    let decoded_cr = decode_protobuf(&wire_cr[4..]).expect("decode clusterrole");
    assert_eq!(decoded_cr["rules"][0]["resources"][0], "pods");
    assert_eq!(decoded_cr["rules"][0]["verbs"][0], "get");

    let role_binding = serde_json::json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": {"name": "rb", "namespace": "default"},
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "Role",
            "name": "pod-reader"
        },
        "subjects": [{
            "kind": "ServiceAccount",
            "name": "default",
            "namespace": "default"
        }]
    });
    let wire_rb = encode_protobuf(&role_binding).expect("encode rolebinding");
    let decoded_rb = decode_protobuf(&wire_rb[4..]).expect("decode rolebinding");
    assert_eq!(decoded_rb["roleRef"]["kind"], "Role");
    assert_eq!(decoded_rb["roleRef"]["name"], "pod-reader");
    assert_eq!(decoded_rb["subjects"][0]["kind"], "ServiceAccount");
    assert_eq!(decoded_rb["subjects"][0]["name"], "default");
}

#[test]
pub fn test_persistentvolume_protobuf_decode_preserves_spec() {
    use k8s_pb::api::core::v1::{PersistentVolume, PersistentVolumeSpec, PersistentVolumeStatus};
    use k8s_pb::apimachinery::pkg::api::resource::Quantity;
    use k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use prost::Message;

    let mut capacity = std::collections::BTreeMap::new();
    capacity.insert(
        "storage".to_string(),
        Quantity {
            string: Some("10Gi".to_string()),
        },
    );

    let pv = PersistentVolume {
        metadata: Some(ObjectMeta {
            name: Some("pv-test".to_string()),
            ..Default::default()
        }),
        spec: Some(PersistentVolumeSpec {
            capacity,
            access_modes: vec!["ReadWriteOnce".to_string()],
            persistent_volume_reclaim_policy: Some("Retain".to_string()),
            storage_class_name: Some("manual".to_string()),
            ..Default::default()
        }),
        status: Some(PersistentVolumeStatus {
            phase: Some("Available".to_string()),
            message: Some("StatusUpdated".to_string()),
            reason: Some("E2E updateStatus".to_string()),
            ..Default::default()
        }),
    };

    let mut buf = Vec::new();
    pv.encode(&mut buf).unwrap();

    let result = pb_persistentvolume_to_json(&pv).unwrap();

    assert_eq!(result["spec"]["capacity"]["storage"], "10Gi");
    assert_eq!(result["spec"]["accessModes"][0], "ReadWriteOnce");
    assert_eq!(result["spec"]["storageClassName"], "manual");
    assert_eq!(result["status"]["reason"], "E2E updateStatus");
    assert_eq!(result["status"]["message"], "StatusUpdated");
}

#[test]
pub fn test_persistentvolumeclaim_protobuf_decode_preserves_spec() {
    use k8s_pb::api::core::v1::{
        PersistentVolumeClaim, PersistentVolumeClaimSpec, VolumeResourceRequirements,
    };
    use k8s_pb::apimachinery::pkg::api::resource::Quantity;
    use k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use prost::Message;

    let mut requests = std::collections::BTreeMap::new();
    requests.insert(
        "storage".to_string(),
        Quantity {
            string: Some("5Gi".to_string()),
        },
    );

    let pvc = PersistentVolumeClaim {
        metadata: Some(ObjectMeta {
            name: Some("pvc-test".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        spec: Some(PersistentVolumeClaimSpec {
            access_modes: vec!["ReadWriteOnce".to_string()],
            resources: Some(VolumeResourceRequirements {
                requests,
                ..Default::default()
            }),
            storage_class_name: Some("standard".to_string()),
            volume_name: Some("pv-test".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    };

    let mut buf = Vec::new();
    pvc.encode(&mut buf).unwrap();

    let result = pb_persistentvolumeclaim_to_json(&pvc).unwrap();

    assert_eq!(result["spec"]["accessModes"][0], "ReadWriteOnce");
    assert_eq!(result["spec"]["resources"]["requests"]["storage"], "5Gi");
    assert_eq!(result["spec"]["volumeName"], "pv-test");
}

#[test]
pub fn test_persistentvolumeclaim_protobuf_decode_preserves_status_conditions() {
    use k8s_pb::api::core::v1::{
        PersistentVolumeClaim, PersistentVolumeClaimCondition, PersistentVolumeClaimStatus,
    };
    use k8s_pb::apimachinery::pkg::apis::meta::v1::{ObjectMeta, Time};
    use prost::Message;

    let pvc = PersistentVolumeClaim {
        metadata: Some(ObjectMeta {
            name: Some("pvc-status".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        status: Some(PersistentVolumeClaimStatus {
            phase: Some("Bound".to_string()),
            conditions: vec![PersistentVolumeClaimCondition {
                r#type: Some("StatusUpdated".to_string()),
                status: Some("True".to_string()),
                reason: Some("ControllerCheck".to_string()),
                message: Some("updated via status subresource".to_string()),
                last_transition_time: Some(Time {
                    seconds: Some(1_777_000_000),
                    nanos: Some(123_000_000),
                }),
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    };

    let mut buf = Vec::new();
    pvc.encode(&mut buf).unwrap();

    let result = pb_persistentvolumeclaim_to_json(&pvc).unwrap();
    assert_eq!(result["status"]["phase"], "Bound");
    assert_eq!(result["status"]["conditions"][0]["type"], "StatusUpdated");
    assert_eq!(result["status"]["conditions"][0]["status"], "True");
    assert_eq!(
        result["status"]["conditions"][0]["reason"],
        "ControllerCheck"
    );
    assert_eq!(
        result["status"]["conditions"][0]["message"],
        "updated via status subresource"
    );
}

#[test]
pub fn test_json_persistentvolumeclaim_to_pb_preserves_status_conditions() {
    use k8s_openapi::api::core::v1::{
        PersistentVolumeClaim, PersistentVolumeClaimCondition, PersistentVolumeClaimStatus,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, Time};
    use std::str::FromStr;

    let pvc = PersistentVolumeClaim {
        metadata: ObjectMeta {
            name: Some("pvc-status-encode".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        status: Some(PersistentVolumeClaimStatus {
            phase: Some("Bound".to_string()),
            conditions: Some(vec![PersistentVolumeClaimCondition {
                type_: "StatusUpdated".to_string(),
                status: "True".to_string(),
                reason: Some("ControllerCheck".to_string()),
                message: Some("persist me".to_string()),
                last_probe_time: Some(Time(
                    chrono::DateTime::from_str("2026-04-25T16:00:00Z").unwrap(),
                )),
                last_transition_time: Some(Time(
                    chrono::DateTime::from_str("2026-04-25T16:01:00Z").unwrap(),
                )),
            }]),
            ..Default::default()
        }),
        ..Default::default()
    };

    let pb = json_persistentvolumeclaim_to_pb(&pvc).unwrap();
    let status = pb.status.as_ref().expect("status must be encoded");
    assert_eq!(status.phase.as_deref(), Some("Bound"));
    assert_eq!(status.conditions.len(), 1);
    let cond = &status.conditions[0];
    assert_eq!(cond.r#type.as_deref(), Some("StatusUpdated"));
    assert_eq!(cond.status.as_deref(), Some("True"));
    assert_eq!(cond.reason.as_deref(), Some("ControllerCheck"));
    assert_eq!(cond.message.as_deref(), Some("persist me"));
    assert!(cond.last_probe_time.is_some());
    assert!(cond.last_transition_time.is_some());
}

#[test]
pub fn test_event_protobuf_decode_preserves_fields() {
    use k8s_pb::api::core::v1::{Event, EventSource, ObjectReference};
    use k8s_pb::apimachinery::pkg::apis::meta::v1::{ObjectMeta, Time};
    use prost::Message;

    let event = Event {
        metadata: Some(ObjectMeta {
            name: Some("event-1".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        involved_object: Some(ObjectReference {
            kind: Some("Pod".to_string()),
            name: Some("test-pod".to_string()),
            namespace: Some("default".to_string()),
            uid: Some("pod-123".to_string()),
            ..Default::default()
        }),
        reason: Some("Created".to_string()),
        message: Some("Pod created".to_string()),
        source: Some(EventSource {
            component: Some("kubelet".to_string()),
            host: Some("node-1".to_string()),
        }),
        r#type: Some("Normal".to_string()),
        count: Some(1),
        first_timestamp: Some(Time {
            seconds: Some(1609459200),
            ..Default::default()
        }),
        ..Default::default()
    };

    let mut buf = Vec::new();
    event.encode(&mut buf).unwrap();

    let result = pb_event_to_json(&event).unwrap();

    assert_eq!(result["involvedObject"]["kind"], "Pod");
    assert_eq!(result["reason"], "Created");
    assert_eq!(result["message"], "Pod created");
    assert_eq!(result["source"]["component"], "kubelet");
    assert_eq!(result["type"], "Normal");
}

#[test]
pub fn test_events_k8s_io_v1_event_protobuf_decode_preserves_fields() {
    use k8s_pb::api::events::v1::Event;
    use k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    let event = Event {
        metadata: Some(ObjectMeta {
            name: Some("ev-1".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        reason: Some("BackOff".to_string()),
        note: Some("Back-off restarting failed container".to_string()),
        r#type: Some("Warning".to_string()),
        reporting_controller: Some("kubelet".to_string()),
        reporting_instance: Some("node-1".to_string()),
        action: Some("BackOff".to_string()),
        ..Default::default()
    };

    let result = pb_events_v1_event_to_json(&event).unwrap();

    assert_eq!(result["apiVersion"], "events.k8s.io/v1");
    assert_eq!(result["kind"], "Event");
    assert_eq!(result["reason"], "BackOff");
    assert_eq!(result["note"], "Back-off restarting failed container");
    assert_eq!(result["type"], "Warning");
    assert_eq!(result["reportingController"], "kubelet");
    assert_eq!(result["reportingInstance"], "node-1");
    assert_eq!(result["action"], "BackOff");
    assert_eq!(result["metadata"]["name"], "ev-1");
}

#[test]
pub fn test_decode_protobuf_resource_dispatches_events_k8s_io_v1() {
    use k8s_pb::api::events::v1::Event;
    use k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use prost::Message;

    let event = Event {
        metadata: Some(ObjectMeta {
            name: Some("ev-dispatch".to_string()),
            namespace: Some("kube-system".to_string()),
            ..Default::default()
        }),
        reason: Some("ScalingReplicaSet".to_string()),
        r#type: Some("Normal".to_string()),
        reporting_controller: Some("deployment-controller".to_string()),
        ..Default::default()
    };

    let mut buf = Vec::new();
    event.encode(&mut buf).unwrap();

    let result = decode_protobuf_resource("events.k8s.io/v1", "Event", &buf).unwrap();

    assert_eq!(result["apiVersion"], "events.k8s.io/v1");
    assert_eq!(result["kind"], "Event");
    assert_eq!(result["reason"], "ScalingReplicaSet");
    assert_eq!(result["reportingController"], "deployment-controller");
    assert_eq!(result["metadata"]["name"], "ev-dispatch");
    assert_eq!(result["metadata"]["namespace"], "kube-system");
}

#[test]
pub fn test_decode_protobuf_event_with_empty_apiversion_prefers_events_v1_shape() {
    use k8s_pb::api::events::v1::Event;
    use k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use prost::Message;

    let event = Event {
        metadata: Some(ObjectMeta {
            name: Some("event-test".to_string()),
            ..Default::default()
        }),
        reason: Some("Test".to_string()),
        r#type: Some("Normal".to_string()),
        reporting_controller: Some("test-controller".to_string()),
        reporting_instance: Some("test-node".to_string()),
        action: Some("Do".to_string()),
        ..Default::default()
    };
    let mut raw = Vec::new();
    event.encode(&mut raw).unwrap();
    let envelope = Unknown {
        type_meta: Some(TypeMeta {
            api_version: String::new(),
            kind: "Event".to_string(),
        }),
        raw,
        content_encoding: String::new(),
        content_type: "application/vnd.kubernetes.protobuf".to_string(),
    };
    let mut wire = Vec::new();
    envelope.encode(&mut wire).unwrap();

    let result = decode_protobuf(&wire).unwrap();

    assert_eq!(result["apiVersion"], "events.k8s.io/v1");
    assert_eq!(result["kind"], "Event");
    assert_eq!(result["metadata"]["name"], "event-test");
    assert_eq!(result["reason"], "Test");
    assert_eq!(result["type"], "Normal");
    assert_eq!(result["reportingController"], "test-controller");
    assert_eq!(result["reportingInstance"], "test-node");
    assert_eq!(result["action"], "Do");
}

#[test]
pub fn test_decode_protobuf_event_with_v1_apiversion_prefers_events_v1_shape_when_fields_match() {
    use k8s_pb::api::events::v1::Event;
    use k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use prost::Message;

    let event = Event {
        metadata: Some(ObjectMeta {
            name: Some("event-test".to_string()),
            ..Default::default()
        }),
        reason: Some("Test".to_string()),
        r#type: Some("Normal".to_string()),
        reporting_controller: Some("test-controller".to_string()),
        reporting_instance: Some("test-node".to_string()),
        action: Some("Do".to_string()),
        ..Default::default()
    };
    let mut raw = Vec::new();
    event.encode(&mut raw).unwrap();
    let envelope = Unknown {
        type_meta: Some(TypeMeta {
            api_version: "v1".to_string(),
            kind: "Event".to_string(),
        }),
        raw,
        content_encoding: String::new(),
        content_type: "application/vnd.kubernetes.protobuf".to_string(),
    };
    let mut wire = Vec::new();
    envelope.encode(&mut wire).unwrap();

    let result = decode_protobuf(&wire).unwrap();

    assert_eq!(result["apiVersion"], "events.k8s.io/v1");
    assert_eq!(result["kind"], "Event");
    assert_eq!(result["metadata"]["name"], "event-test");
    assert_eq!(result["reportingController"], "test-controller");
    assert_eq!(result["reportingInstance"], "test-node");
}

#[test]
pub fn test_decode_protobuf_core_v1_event_with_v1_apiversion_stays_core_shape() {
    use k8s_pb::api::core::v1::{Event, EventSource, ObjectReference};
    use k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use prost::Message;

    let event = Event {
        metadata: Some(ObjectMeta {
            name: Some("core-event".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        involved_object: Some(ObjectReference {
            name: Some("pod-a".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        reason: Some("Started".to_string()),
        source: Some(EventSource {
            component: Some("kubelet".to_string()),
            ..Default::default()
        }),
        r#type: Some("Normal".to_string()),
        ..Default::default()
    };
    let mut raw = Vec::new();
    event.encode(&mut raw).unwrap();
    let envelope = Unknown {
        type_meta: Some(TypeMeta {
            api_version: "v1".to_string(),
            kind: "Event".to_string(),
        }),
        raw,
        content_encoding: String::new(),
        content_type: "application/vnd.kubernetes.protobuf".to_string(),
    };
    let mut wire = Vec::new();
    envelope.encode(&mut wire).unwrap();

    let result = decode_protobuf(&wire).unwrap();

    assert_eq!(result["apiVersion"], "v1");
    assert_eq!(result["kind"], "Event");
    assert_eq!(result["metadata"]["name"], "core-event");
    assert_eq!(result["source"]["component"], "kubelet");
    assert!(result.get("reportingController").is_none());
}

#[test]
pub fn test_events_v1_event_time_is_iso8601_not_epoch() {
    use k8s_pb::api::events::v1::{Event, EventSeries};
    use k8s_pb::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};

    let event = Event {
        metadata: Some(ObjectMeta {
            name: Some("ev-time".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        // 2024-01-15T10:30:00Z = 1705312200 epoch seconds
        event_time: Some(MicroTime {
            seconds: Some(1705312200),
            nanos: Some(123456000),
        }),
        series: Some(EventSeries {
            count: Some(3),
            last_observed_time: Some(MicroTime {
                seconds: Some(1705312500),
                nanos: Some(0),
            }),
        }),
        ..Default::default()
    };

    let result = pb_events_v1_event_to_json(&event).unwrap();

    // eventTime must be ISO 8601, NOT epoch like "1705312200.123456000Z"
    let event_time = result["eventTime"].as_str().unwrap();
    assert!(
        event_time.starts_with("2024-01-15"),
        "eventTime should be ISO 8601 datetime, got: {}",
        event_time
    );
    assert!(
        !event_time.starts_with("1705"),
        "eventTime must not be epoch seconds, got: {}",
        event_time
    );
    assert!(
        event_time.ends_with('Z') && event_time.contains('.'),
        "eventTime must be canonical MicroTime with Z + fractional seconds, got: {}",
        event_time
    );

    // series.lastObservedTime must also be ISO 8601
    let last_observed = result["series"]["lastObservedTime"].as_str().unwrap();
    assert!(
        last_observed.starts_with("2024-01-15"),
        "lastObservedTime should be ISO 8601 datetime, got: {}",
        last_observed
    );
    assert!(
        last_observed.ends_with('Z') && last_observed.contains('.'),
        "lastObservedTime must be canonical MicroTime with Z + fractional seconds, got: {}",
        last_observed
    );
    assert_eq!(result["series"]["count"], 3);
}

#[test]
pub fn test_decode_protobuf_resource_core_v1_event_still_works() {
    use k8s_pb::api::core::v1::{Event, EventSource, ObjectReference};
    use k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use prost::Message;

    let event = Event {
        metadata: Some(ObjectMeta {
            name: Some("core-event".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        }),
        involved_object: Some(ObjectReference {
            kind: Some("Pod".to_string()),
            name: Some("test-pod".to_string()),
            ..Default::default()
        }),
        reason: Some("Pulled".to_string()),
        source: Some(EventSource {
            component: Some("kubelet".to_string()),
            ..Default::default()
        }),
        r#type: Some("Normal".to_string()),
        ..Default::default()
    };

    let mut buf = Vec::new();
    event.encode(&mut buf).unwrap();

    // Empty api_version should route to core v1 Event decoder
    let result = decode_protobuf_resource("", "Event", &buf).unwrap();

    assert_eq!(result["apiVersion"], "v1");
    assert_eq!(result["kind"], "Event");
    assert_eq!(result["reason"], "Pulled");
    assert_eq!(result["involvedObject"]["kind"], "Pod");
    assert_eq!(result["source"]["component"], "kubelet");
}

#[test]
pub fn test_node_protobuf_decode_preserves_spec_and_status() {
    use k8s_pb::api::core::v1::{
        Node, NodeAddress, NodeCondition, NodeSpec, NodeStatus, NodeSystemInfo, Taint,
    };
    use k8s_pb::apimachinery::pkg::api::resource::Quantity;
    use k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use prost::Message;

    let mut capacity = std::collections::BTreeMap::new();
    capacity.insert(
        "cpu".to_string(),
        Quantity {
            string: Some("4".to_string()),
        },
    );

    let node = Node {
        metadata: Some(ObjectMeta {
            name: Some("node-1".to_string()),
            ..Default::default()
        }),
        spec: Some(NodeSpec {
            pod_cidr: Some("10.244.0.0/24".to_string()),
            unschedulable: Some(true),
            taints: vec![Taint {
                key: Some("node-role.kubernetes.io/control-plane".to_string()),
                value: Some("".to_string()),
                effect: Some("NoSchedule".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        }),
        status: Some(NodeStatus {
            conditions: vec![NodeCondition {
                r#type: Some("Ready".to_string()),
                status: Some("True".to_string()),
                ..Default::default()
            }],
            addresses: vec![NodeAddress {
                r#type: Some("InternalIP".to_string()),
                address: Some("192.168.1.10".to_string()),
            }],
            capacity,
            node_info: Some(NodeSystemInfo {
                machine_id: Some("abc123".to_string()),
                kernel_version: Some("5.10.0".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }),
    };

    let mut buf = Vec::new();
    node.encode(&mut buf).unwrap();

    let result = pb_node_to_json(&node).unwrap();

    assert_eq!(result["spec"]["podCIDR"], "10.244.0.0/24");
    assert_eq!(
        result["spec"]["unschedulable"], true,
        "protobuf Node decode must preserve spec.unschedulable so fake ready nodes stay unschedulable"
    );
    assert_eq!(
        result["spec"]["taints"][0]["key"],
        "node-role.kubernetes.io/control-plane"
    );
    assert_eq!(result["status"]["conditions"][0]["type"], "Ready");
    assert_eq!(result["status"]["capacity"]["cpu"], "4");
}

#[test]
pub fn test_crd_protobuf_decode_preserves_spec() {
    use k8s_pb::apiextensions_apiserver::pkg::apis::apiextensions::v1::{
        CustomResourceDefinition, CustomResourceDefinitionNames, CustomResourceDefinitionSpec,
        CustomResourceDefinitionVersion, SelectableField,
    };
    use k8s_pb::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use prost::Message;

    let crd = CustomResourceDefinition {
        metadata: Some(ObjectMeta {
            name: Some("applications.argoproj.io".to_string()),
            ..Default::default()
        }),
        spec: Some(CustomResourceDefinitionSpec {
            group: Some("argoproj.io".to_string()),
            names: Some(CustomResourceDefinitionNames {
                plural: Some("applications".to_string()),
                singular: Some("application".to_string()),
                kind: Some("Application".to_string()),
                short_names: vec!["app".to_string(), "apps".to_string()],
                ..Default::default()
            }),
            scope: Some("Namespaced".to_string()),
            versions: vec![CustomResourceDefinitionVersion {
                name: Some("v1alpha1".to_string()),
                served: Some(true),
                storage: Some(true),
                selectable_fields: vec![SelectableField {
                    json_path: Some(".host".to_string()),
                }],
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    };

    let mut buf = Vec::new();
    crd.encode(&mut buf).unwrap();

    let result = pb_crd_to_json(&crd).unwrap();

    assert_eq!(result["spec"]["group"], "argoproj.io");
    assert_eq!(result["spec"]["names"]["plural"], "applications");
    assert_eq!(result["spec"]["names"]["kind"], "Application");
    assert_eq!(result["spec"]["versions"][0]["name"], "v1alpha1");
    assert_eq!(
        result["spec"]["versions"][0]["selectableFields"][0]["jsonPath"],
        ".host"
    );
}
