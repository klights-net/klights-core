use super::*;

#[test]
fn probe_runtime_methods_require_uid() {
    use crate::runtime::probes::ProbeRuntime;
    fn assert_send_sync<T: ?Sized + Send + Sync>() {}
    assert_send_sync::<dyn ProbeRuntime>();
}

#[tokio::test]
async fn mock_probe_runtime_stops_by_uid() {
    let probe = MockProbeRuntime::new();
    let key = PodRuntimeKey::new("ns", "pod", "uid-1");

    probe
        .start_probes(&key, "sb-1", &serde_json::json!({}))
        .await
        .unwrap();
    probe.stop_probes(&key).await.unwrap();

    let calls = probe.recorded_calls();
    assert_eq!(calls.len(), 2);
    assert_eq!(
        calls[0],
        MockProbeCall::Start {
            namespace: "ns".into(),
            name: "pod".into(),
            uid: "uid-1".into(),
            sandbox_id: "sb-1".into(),
        }
    );
    assert_eq!(
        calls[1],
        MockProbeCall::Stop {
            namespace: "ns".into(),
            name: "pod".into(),
            uid: "uid-1".into(),
        }
    );
}

#[tokio::test]
async fn real_runtime_stop_pod_stops_probes_by_uid() {
    let harness = PodRuntimeHarness::new().await;
    let key = PodRuntimeKey::new("ns", "stop-probe", "uid-sp");

    harness
        .runtime
        .stop_pod(key.clone(), None, Some("sb-1".into()))
        .await
        .unwrap();

    let probe_calls = harness.probes.recorded_calls();
    assert_eq!(probe_calls.len(), 1, "expected exactly one probe call");
    assert_eq!(
        probe_calls[0],
        MockProbeCall::Stop {
            namespace: "ns".into(),
            name: "stop-probe".into(),
            uid: "uid-sp".into(),
        },
        "probes must be stopped with exact UID"
    );
}

#[tokio::test]
async fn mock_dependency_matrix_probe() {
    let mock = MockProbeRuntime::new();
    let key = PodRuntimeKey::new("ns", "probe-pod", "uid-probe");
    let pod = serde_json::json!({"metadata": {"name": "probe-pod"}});

    mock.start_probes(&key, "sb-probe", &pod).await.unwrap();
    mock.stop_probes(&key).await.unwrap();

    let calls = mock.recorded_calls();
    assert_eq!(calls.len(), 2);
    match &calls[0] {
        MockProbeCall::Start {
            namespace,
            name,
            uid,
            ..
        } => {
            assert_eq!(namespace, "ns");
            assert_eq!(name, "probe-pod");
            assert_eq!(*uid, "uid-probe");
        }
        other => panic!("expected Start, got {:?}", other),
    }
    match &calls[1] {
        MockProbeCall::Stop {
            namespace,
            name,
            uid,
        } => {
            assert_eq!(namespace, "ns");
            assert_eq!(name, "probe-pod");
            assert_eq!(*uid, "uid-probe");
        }
        other => panic!("expected Stop, got {:?}", other),
    }
}

#[tokio::test]
async fn readiness_lifecycle_command_persists_probe_result_with_parity() {
    use crate::pod_repository::PodStatusUpdate;

    let harness = PodRuntimeHarness::new().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "pod-network-test",
            "name": "netserver-0",
            "uid": "uid-netserver-0",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "containers": [{
                "name": "webserver",
                "image": "registry.k8s.io/e2e-test-images/agnhost:2.56",
                "imagePullPolicy": "Never",
                "readinessProbe": {
                    "httpGet": {"path": "/healthz", "port": 8083},
                    "periodSeconds": 10,
                    "timeoutSeconds": 30
                }
            }]
        },
        "status": {
            "phase": "Running",
            "podIP": "10.50.0.3",
            "containerStatuses": [{
                "name": "webserver",
                "containerID": "containerd://ctr-netserver",
                "image": "registry.k8s.io/e2e-test-images/agnhost:2.56",
                "imageID": "registry.k8s.io/e2e-test-images/agnhost@sha256:test",
                "ready": false,
                "started": true,
                "restartCount": 0,
                "state": {"running": {"startedAt": "2026-05-19T23:12:00Z"}}
            }],
            "conditions": [
                {"type": "ContainersReady", "status": "False", "lastTransitionTime": "2026-05-19T23:12:00Z"},
                {"type": "Ready", "status": "False", "lastTransitionTime": "2026-05-19T23:12:00Z"}
            ]
        }
    });
    let key = PodRuntimeKey::new("pod-network-test", "netserver-0", "uid-netserver-0");
    harness.create_runtime_pod(pod.clone()).await;
    harness
        .repo
        .pod_status_writer
        .set_pod_status_for_uid(
            "pod-network-test",
            "netserver-0",
            "uid-netserver-0",
            PodStatusUpdate {
                phase: "Running".to_string(),
                pod_ip: "10.50.0.3".to_string(),
                host_ip: String::new(),
                container_statuses: pod
                    .pointer("/status/containerStatuses")
                    .and_then(|value| value.as_array())
                    .cloned()
                    .unwrap(),
                init_container_statuses: None,
                qos_class: None,
            },
            None,
        )
        .await
        .unwrap();

    let cmd = crate::lifecycle::LifecycleCommand::ReadinessChanged {
        pod_uid: "uid-netserver-0".into(),
        namespace: "pod-network-test".into(),
        pod_name: "netserver-0".into(),
        container_name: "webserver".into(),
        ready: true,
    };
    harness.runtime.handle_lifecycle_command(cmd).await.unwrap();

    let stored = harness.stored_pod(&key).await;
    assert_eq!(
        stored
            .pointer("/status/containerStatuses/0/ready")
            .and_then(|value| value.as_bool()),
        Some(true),
        "a successful readiness probe must mark the probed container ready"
    );
    for condition_type in ["ContainersReady", "Ready"] {
        let condition = stored
            .pointer("/status/conditions")
            .and_then(|value| value.as_array())
            .and_then(|conditions| {
                conditions.iter().find(|condition| {
                    condition.pointer("/type").and_then(|value| value.as_str())
                        == Some(condition_type)
                })
            })
            .unwrap_or_else(|| panic!("{condition_type} condition must exist"));
        assert_eq!(
            condition
                .pointer("/status")
                .and_then(|value| value.as_str()),
            Some("True"),
            "{condition_type} must become True after the probe succeeds"
        );
    }
}

#[tokio::test]
async fn liveness_restart_uses_runtime_container_id_with_parity() {
    use crate::lifecycle::{LifecycleCommand, RestartReason};
    use crate::pod_repository::PodStatusUpdate;
    use crate::runtime::cri::ContainerRuntimeState;

    let harness = PodRuntimeHarness::new().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "container-probe",
            "name": "grpc-liveness-pod",
            "uid": "uid-grpc-liveness",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "restartPolicy": "Always",
            "containers": [{
                "name": "agnhost",
                "image": "registry.k8s.io/e2e-test-images/agnhost:2.56",
                "imagePullPolicy": "Never",
                "livenessProbe": {
                    "grpc": {"port": 8080},
                    "periodSeconds": 1,
                    "failureThreshold": 1
                }
            }]
        },
        "status": {
            "phase": "Running",
            "podIP": "10.50.1.4",
            "hostIP": "10.99.0.11",
            "containerStatuses": [{
                "name": "agnhost",
                "containerID": "containerd://old-grpc-container",
                "image": "registry.k8s.io/e2e-test-images/agnhost:2.56",
                "imageID": "registry.k8s.io/e2e-test-images/agnhost@sha256:test",
                "ready": true,
                "started": true,
                "restartCount": 0,
                "state": {"running": {"startedAt": "2026-05-19T22:49:26Z"}}
            }]
        }
    });
    let key = PodRuntimeKey::new("container-probe", "grpc-liveness-pod", "uid-grpc-liveness");

    harness.create_runtime_pod(pod.clone()).await;
    harness
        .repo
        .pod_status_writer
        .set_pod_status_for_uid(
            "container-probe",
            "grpc-liveness-pod",
            "uid-grpc-liveness",
            PodStatusUpdate {
                phase: "Running".to_string(),
                pod_ip: "10.50.1.4".to_string(),
                host_ip: "10.99.0.11".to_string(),
                container_statuses: pod
                    .pointer("/status/containerStatuses")
                    .and_then(|value| value.as_array())
                    .cloned()
                    .unwrap(),
                init_container_statuses: None,
                qos_class: None,
            },
            None,
        )
        .await
        .unwrap();
    harness
        .store
        .record_sandbox(&key, "sandbox-grpc-liveness")
        .await
        .unwrap();
    harness.container_control.set_container_states(vec![(
        "old-grpc-container".to_string(),
        ContainerRuntimeState::Running,
    )]);
    harness.cri.set_container_exit_code(137);

    harness
        .runtime
        .handle_lifecycle_command(LifecycleCommand::RestartRequested {
            pod_uid: "uid-grpc-liveness".into(),
            namespace: "container-probe".into(),
            pod_name: "grpc-liveness-pod".into(),
            container_name: "agnhost".into(),
            reason: RestartReason::LivenessProbe,
        })
        .await
        .unwrap();

    let calls = harness.cri.recorded_calls();
    assert!(
        calls.iter().any(|call| {
            matches!(
                &call.operation,
                MockCriOperation::StopContainer(container_id, 10)
                    if container_id == "old-grpc-container"
            )
        }),
        "liveness restart must stop the runtime container ID from status"
    );
    assert!(
        calls.iter().any(|call| {
            matches!(
                &call.operation,
                MockCriOperation::RemoveContainer(container_id)
                    if container_id == "old-grpc-container"
            )
        }),
        "liveness restart must remove the old runtime container ID"
    );

    let create_configs = harness.cri.recorded_create_configs();
    let restart_config = create_configs
        .last()
        .expect("restart must create a replacement container");
    assert_eq!(
        restart_config
            .metadata
            .as_ref()
            .map(|metadata| metadata.name.as_str()),
        Some("agnhost")
    );
    assert_eq!(
        restart_config
            .image
            .as_ref()
            .map(|image| image.image.as_str()),
        Some("registry.k8s.io/e2e-test-images/agnhost:2.56"),
        "replacement container config must be rebuilt from the pod spec"
    );

    let stored = harness.stored_pod(&key).await;
    let status = stored
        .pointer("/status/containerStatuses/0")
        .expect("container status must remain present after restart note");
    assert_eq!(status.pointer("/restartCount"), Some(&serde_json::json!(1)));
    assert!(
        status.pointer("/lastState/terminated").is_some(),
        "restart note must preserve the terminated lastState"
    );
}

#[tokio::test]
async fn liveness_restart_publishes_replacement_container_status_immediately() {
    use crate::lifecycle::{LifecycleCommand, RestartReason};
    use crate::pod_repository::PodStatusUpdate;
    use crate::runtime::cri::ContainerRuntimeState;

    let harness = PodRuntimeHarness::new().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "container-probe",
            "name": "liveness-status-pod",
            "uid": "uid-liveness-status",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "restartPolicy": "Always",
            "containers": [{
                "name": "agnhost",
                "image": "registry.k8s.io/e2e-test-images/agnhost:2.56",
                "imagePullPolicy": "Never",
                "livenessProbe": {
                    "grpc": {"port": 8080},
                    "periodSeconds": 1,
                    "failureThreshold": 1
                }
            }]
        },
        "status": {
            "phase": "Running",
            "podIP": "10.50.1.5",
            "hostIP": "10.99.0.11",
            "conditions": [
                {"type": "ContainersReady", "status": "True"},
                {"type": "Ready", "status": "True"}
            ],
            "containerStatuses": [{
                "name": "agnhost",
                "containerID": "containerd://old-liveness-container",
                "image": "registry.k8s.io/e2e-test-images/agnhost:2.56",
                "imageID": "registry.k8s.io/e2e-test-images/agnhost@sha256:test",
                "ready": true,
                "started": true,
                "restartCount": 0,
                "state": {"running": {"startedAt": "2026-05-19T22:49:26Z"}}
            }]
        }
    });
    let key = PodRuntimeKey::new(
        "container-probe",
        "liveness-status-pod",
        "uid-liveness-status",
    );

    harness.create_runtime_pod(pod.clone()).await;
    harness
        .repo
        .pod_status_writer
        .set_pod_status_for_uid(
            "container-probe",
            "liveness-status-pod",
            "uid-liveness-status",
            PodStatusUpdate {
                phase: "Running".to_string(),
                pod_ip: "10.50.1.5".to_string(),
                host_ip: "10.99.0.11".to_string(),
                container_statuses: pod
                    .pointer("/status/containerStatuses")
                    .and_then(|value| value.as_array())
                    .cloned()
                    .unwrap(),
                init_container_statuses: None,
                qos_class: None,
            },
            None,
        )
        .await
        .unwrap();
    harness
        .store
        .record_sandbox(&key, "sandbox-liveness-status")
        .await
        .unwrap();
    harness.container_control.set_container_states(vec![(
        "old-liveness-container".to_string(),
        ContainerRuntimeState::Running,
    )]);
    harness.cri.set_container_exit_code(137);

    harness
        .runtime
        .handle_lifecycle_command(LifecycleCommand::RestartRequested {
            pod_uid: "uid-liveness-status".into(),
            namespace: "container-probe".into(),
            pod_name: "liveness-status-pod".into(),
            container_name: "agnhost".into(),
            reason: RestartReason::LivenessProbe,
        })
        .await
        .unwrap();

    let stored = harness.stored_pod(&key).await;
    let status = stored
        .pointer("/status/containerStatuses/0")
        .expect("container status must be stored after liveness restart");
    assert_eq!(
        status.get("containerID").and_then(|value| value.as_str()),
        Some("containerd://container-sandbox-liveness-status"),
        "probe-triggered restart must publish the replacement container id immediately"
    );
    assert_eq!(
        status.get("restartCount").and_then(|value| value.as_i64()),
        Some(1),
        "probe-triggered restart must increment restartCount with the replacement status"
    );
    assert!(
        status.pointer("/lastState/terminated").is_some(),
        "probe-triggered restart must preserve the terminated lastState"
    );
    assert!(
        status.pointer("/state/running/startedAt").is_some(),
        "probe-triggered restart must publish the replacement as running"
    );
}

#[tokio::test]
async fn real_runtime_start_pod_does_not_register_readiness_probes_before_finalize_startup() {
    let harness = PodRuntimeHarness::new().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "namespace": "ns", "name": "probe-pod", "uid": "uid-probe", "resourceVersion": "1" },
        "spec": {
            "containers": [{
                "name": "app",
                "image": "nginx",
                "imagePullPolicy": "Never",
                "readinessProbe": {
                    "httpGet": { "path": "/ready", "port": 8080 },
                    "initialDelaySeconds": 1
                }
            }],
            "nodeName": "test-node"
        },
        "status": {"phase": "Pending"}
    });
    harness
        .repo
        .test_create_pod("ns", "probe-pod", "test-node", pod.clone())
        .await
        .unwrap();
    let key = PodRuntimeKey::new("ns", "probe-pod", "uid-probe");
    let result = harness
        .runtime
        .start_pod(key.clone(), Some(pod), CancellationToken::new())
        .await
        .unwrap();

    assert!(
        matches!(result, PodStartResult::Started { .. }),
        "pod must start, got {:?}",
        result
    );

    // start_pod must NOT register probes (readiness/startup probes deferred to finalize_startup)
    let probe_calls = harness.probes.recorded_calls();
    let start_calls: Vec<_> = probe_calls
        .iter()
        .filter(|c| matches!(c, MockProbeCall::Start { .. }))
        .collect();
    assert!(
        start_calls.is_empty(),
        "start_pod must not register probes before finalize_startup"
    );

    // Update pod status to Running with podIP so finalize_startup confirms.
    harness
        .repo
        .pod_status_writer
        .set_pod_status_for_uid(
            "ns",
            "probe-pod",
            "uid-probe",
            PodStatusUpdate {
                phase: "Running".into(),
                pod_ip: "10.0.0.1".into(),
                host_ip: String::new(),
                container_statuses: Vec::new(),
                init_container_statuses: None,
                qos_class: None,
            },
            None,
        )
        .await
        .unwrap();

    // finalize_startup must register probes once Running + podIP is confirmed.
    harness
        .runtime
        .finalize_startup(key.clone(), None, None)
        .await
        .unwrap();

    let probe_calls = harness.probes.recorded_calls();
    let start_calls: Vec<_> = probe_calls
        .iter()
        .filter(|c| matches!(c, MockProbeCall::Start { .. }))
        .collect();
    assert_eq!(
        start_calls.len(),
        1,
        "finalize_startup must register probes once pod is Running with podIP"
    );
}

#[tokio::test]
async fn readiness_probe_reconcile_path_with_parity() {
    let harness = PodRuntimeHarness::new().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "default",
            "name": "ready-gated",
            "uid": "uid-ready-gated",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "containers": [{
                "name": "web",
                "image": "nginx:1.25",
                "imagePullPolicy": "Never",
                "readinessProbe": {"httpGet": {"path": "/", "port": 80}}
            }]
        },
        "status": {"phase": "Pending"}
    });
    let key = PodRuntimeKey::new("default", "ready-gated", "uid-ready-gated");

    harness.create_runtime_pod(pod.clone()).await;
    let start = harness
        .start_pod_through_runtime(key.clone(), pod.clone())
        .await;
    assert!(matches!(start, PodStartResult::Started { .. }));

    harness.simulate_running_containers(vec!["container-ready-gated".into()]);
    harness.reconcile_runtime(key.clone()).await;

    let resource = harness.stored_pod(&key).await;
    assert_eq!(
        resource
            .pointer("/status/containerStatuses/0/name")
            .and_then(|v| v.as_str()),
        Some("web")
    );
    assert_eq!(
        resource
            .pointer("/status/containerStatuses/0/ready")
            .and_then(|v| v.as_bool()),
        Some(false),
        "main keeps readiness-probe containers unready until the probe manager reports success"
    );
    assert_eq!(
        resource
            .pointer("/status/conditions")
            .and_then(|v| v.as_array())
            .and_then(|conditions| {
                conditions.iter().find(|condition| {
                    condition.pointer("/type").and_then(|v| v.as_str()) == Some("Ready")
                })
            })
            .and_then(|condition| condition.pointer("/status"))
            .and_then(|v| v.as_str()),
        Some("False")
    );
}
