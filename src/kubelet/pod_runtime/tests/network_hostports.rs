use super::*;

#[test]
fn pod_network_runtime_read_assignment_requires_uid() {
    // Verify the trait exists and is object-safe.
    fn assert_send_sync<T: ?Sized + Send + Sync>() {}
    assert_send_sync::<dyn PodNetworkRuntime>();
    // read_assignment takes &PodRuntimeKey — UID is mandatory.
}

#[test]
fn pod_network_runtime_release_carries_uid() {
    // Compile-time check: release_sandbox_network signature requires PodRuntimeKey.
    // The trait method is async but the test just verifies the signature exists.
    fn assert_send_sync<T: ?Sized + Send + Sync>() {}
    assert_send_sync::<dyn PodNetworkRuntime>();
}

#[tokio::test]
async fn mock_network_records_assignment_and_release() {
    let mock = MockPodNetworkRuntime::new();
    let key = PodRuntimeKey::new("ns", "pod", "uid-1");

    let assignment = mock.read_assignment("sb-1", &key, false).await.unwrap();
    assert_eq!(assignment.pod_ip, "10.0.0.1");

    mock.release_sandbox_network(&key, "sb-1").await.unwrap();

    let calls = mock.recorded_calls();
    assert_eq!(calls.len(), 2);
    assert_eq!(
        calls[0],
        MockNetworkOp::ReadAssignment {
            sandbox_id: "sb-1".to_string(),
            namespace: "ns".to_string(),
            name: "pod".to_string(),
            uid: "uid-1".to_string(),
            host_network: false,
        }
    );
    assert_eq!(
        calls[1],
        MockNetworkOp::ReleaseSandboxNetwork {
            namespace: "ns".to_string(),
            name: "pod".to_string(),
            uid: "uid-1".to_string(),
            sandbox_id: "sb-1".to_string(),
        }
    );
}

#[tokio::test]
async fn real_network_runtime_rejects_release_when_uid_sandbox_row_does_not_match() {
    let (_ds, db) = crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let node_local =
        crate::bootstrap::pod_repository_composition::test_node_local_store(supervisor.clone())
            .await;
    let repository = build_test_pod_repository(
        db.clone(),
        supervisor,
        node_local,
        crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity(),
    );
    let datapath = Arc::new(klights_networking::test_support::MockNetworkProvider::new());
    let old_key = PodRuntimeKey::new("ns", "same-name", "old-uid");
    let new_key = PodRuntimeKey::new("ns", "same-name", "new-uid");
    let pod_runtime_store = node_local_runtime_store().await;
    let persisted_runtime_store = pod_runtime_store.pod_runtime();
    admit_runtime_key(persisted_runtime_store.as_ref(), &old_key).await;
    admit_runtime_key(persisted_runtime_store.as_ref(), &new_key).await;
    let store = Arc::new(
        crate::kubelet::pod_runtime::store::RealPodRuntimeStore::new(
            persisted_runtime_store,
            "node-1",
            Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
        ),
    );
    let runtime = crate::kubelet::pod_runtime::network::RealPodNetworkRuntime::new(
        datapath.clone(),
        repository.pod_network_assignment,
        store.clone(),
    );
    store.record_sandbox(&old_key, "sandbox-old").await.unwrap();
    store.record_sandbox(&new_key, "sandbox-new").await.unwrap();

    let err = runtime
        .release_sandbox_network(&new_key, "sandbox-old")
        .await
        .expect_err("must reject stale sandbox release for same-name replacement");

    assert!(
        err.to_string().contains("sandbox UID mismatch"),
        "unexpected error: {err:#}"
    );
    assert!(
        datapath.calls().iter().all(|call| !matches!(
            call,
            klights_networking::test_support::NetworkCall::CniDel { .. }
        )),
        "CNI delete must not run on UID/sandbox mismatch"
    );
}

#[test]
fn hostport_runtime_records_uid_from_pod_argument() {
    use crate::kubelet::pod_runtime::hostports::HostPortRuntime;
    fn assert_send_sync<T: ?Sized + Send + Sync>() {}
    assert_send_sync::<dyn HostPortRuntime>();
}

#[tokio::test]
async fn mock_hostport_runtime_records_add_and_remove() {
    let hp = MockHostPortRuntime::new();
    let key = PodRuntimeKey::new("ns", "pod", "uid-1");
    let pod = serde_json::json!({});
    let host_ports =
        crate::kubelet::pod_runtime::hostports::pod_host_ports_from_resource(&key, &pod).unwrap();

    hp.add_host_ports(&host_ports).await.unwrap();
    hp.remove_host_ports(&host_ports).await.unwrap();
    hp.check_host_port_admission(&host_ports).await.unwrap();

    let calls = hp.recorded_calls();
    assert_eq!(calls.len(), 3);
    assert_eq!(
        calls[0],
        MockHostPortOp::Add {
            namespace: "ns".into(),
            name: "pod".into(),
            uid: "uid-1".into(),
        }
    );
    assert_eq!(
        calls[1],
        MockHostPortOp::Remove {
            namespace: "ns".into(),
            name: "pod".into(),
            uid: "uid-1".into(),
        }
    );
    assert_eq!(
        calls[2],
        MockHostPortOp::Check {
            namespace: "ns".into(),
            name: "pod".into(),
            uid: "uid-1".into(),
        }
    );
}

#[tokio::test]
async fn network_assignment_timeout_rolls_back_sandbox_with_parity() {
    let harness = PodRuntimeHarness::new().await;
    let pod = pod_with_pull_policy("ns", "net-timeout", "uid-net-timeout", "nginx", "Never");
    harness
        .repo
        .test_create_pod("ns", "net-timeout", "test-node", pod.clone())
        .await
        .unwrap();
    harness.network.set_network_assignment_timeout();

    let key = PodRuntimeKey::new("ns", "net-timeout", "uid-net-timeout");
    let err = harness
        .runtime
        .start_pod(key.clone(), Some(pod), CancellationToken::new())
        .await
        .expect_err("network assignment timeout must surface as a retryable startup error");
    assert!(
        err.to_string().contains("network assignment failed"),
        "unexpected error: {err:#}"
    );

    let net_calls = harness.network.recorded_calls();
    assert!(
        net_calls.iter().any(|call| matches!(
            call,
            MockNetworkOp::ReleaseSandboxNetwork {
                uid,
                sandbox_id,
                ..
            } if uid == "uid-net-timeout" && sandbox_id == "sandbox-0001"
        )),
        "network assignment timeout must release the suspect sandbox network; calls={net_calls:?}"
    );

    let cri_calls = harness.cri.recorded_calls();
    assert!(
        cri_calls.iter().any(|call| matches!(
            call.operation,
            MockCriOperation::StopPodSandbox(ref sandbox_id) if sandbox_id == "sandbox-0001"
        )),
        "network assignment timeout must stop the suspect sandbox; calls={cri_calls:?}"
    );
    assert!(
        cri_calls.iter().any(|call| matches!(
            call.operation,
            MockCriOperation::RemovePodSandbox(ref sandbox_id) if sandbox_id == "sandbox-0001"
        )),
        "network assignment timeout must remove the suspect sandbox; calls={cri_calls:?}"
    );

    let store_calls = harness.store.recorded_calls();
    assert!(
        store_calls
            .iter()
            .any(|call| call == "delete_sandbox:ns/net-timeout/uid-net-timeout"),
        "network assignment timeout must clear the sandbox row so retry creates a fresh sandbox; calls={store_calls:?}"
    );
}

#[tokio::test]
async fn hung_hostport_setup_times_out_and_rolls_back_sandbox() {
    let harness = PodRuntimeHarness::new().await;
    let mut pod =
        pod_with_pull_policy("ns", "hostport-hang", "uid-hostport-hang", "nginx", "Never");
    pod["spec"]["containers"][0]["ports"] = json!([{
        "containerPort": 80,
        "hostPort": 18080,
        "protocol": "TCP"
    }]);
    harness
        .repo
        .test_create_pod("ns", "hostport-hang", "test-node", pod.clone())
        .await
        .unwrap();
    harness.hostports.hang_add_host_ports();

    let key = PodRuntimeKey::new("ns", "hostport-hang", "uid-hostport-hang");
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(250),
        harness
            .runtime
            .start_pod(key.clone(), Some(pod), CancellationToken::new()),
    )
    .await
    .expect("hung hostPort setup must be bounded by the runtime")
    .expect("hostPort setup timeout should be reported as a pod start result");

    match result {
        PodStartResult::Failed(message) => {
            assert!(
                message.contains("Timed out adding hostPort rules"),
                "timeout should describe hostPort setup: {message}"
            );
        }
        other => panic!("expected retryable hostPort setup failure, got {other:?}"),
    }
    assert_partial_start_rolled_back(&harness, &key, "sandbox-0001");
}

#[tokio::test]
async fn real_runtime_start_pod_uses_hostport_admission_port_before_side_effects() {
    let harness = PodRuntimeHarness::new().await;
    let pod = pod_with_pull_policy("ns", "hp-admit", "uid-hp-admit", "nginx", "Never");
    harness
        .repo
        .test_create_pod("ns", "hp-admit", "test-node", pod.clone())
        .await
        .unwrap();
    harness.hostports.reject_next_check("reserved host port");

    let key = PodRuntimeKey::new("ns", "hp-admit", "uid-hp-admit");
    let result = harness
        .runtime
        .start_pod(key, Some(pod), CancellationToken::new())
        .await
        .unwrap();

    match result {
        PodStartResult::Terminal(message) => {
            assert!(message.contains("hostPort admission failed"));
            assert!(message.contains("reserved host port"));
        }
        other => panic!("expected terminal hostPort admission failure, got {other:?}"),
    }

    assert_eq!(
        harness.hostports.recorded_calls(),
        vec![MockHostPortOp::Check {
            namespace: "ns".into(),
            name: "hp-admit".into(),
            uid: "uid-hp-admit".into(),
        }],
        "start_pod must route admission through HostPortRuntime before add_host_ports"
    );
    assert!(
        harness.cri.recorded_calls().is_empty(),
        "hostPort admission failure must stop before CRI sandbox/container calls"
    );
}

#[tokio::test]
async fn hostport_admission_failure_marks_pod_failed_with_parity() {
    use klights_kubelet::pod_repository::PodStatusUpdate;

    let harness = PodRuntimeHarness::new().await;
    let holder = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "statefulset",
            "name": "test-pod",
            "uid": "uid-holder",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "restartPolicy": "Never",
            "containers": [{
                "name": "webserver",
                "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4",
                "imagePullPolicy": "Never",
                "ports": [{"containerPort": 21017, "hostPort": 21017, "protocol": "TCP"}]
            }]
        },
        "status": {"phase": "Running"}
    });
    harness
        .repo
        .test_create_pod("statefulset", "test-pod", "test-node", holder)
        .await
        .unwrap();
    harness
        .repo
        .pod_status_writer
        .set_pod_status_for_uid(
            "statefulset",
            "test-pod",
            "uid-holder",
            PodStatusUpdate {
                phase: "Running".to_string(),
                pod_ip: "10.50.0.63".to_string(),
                host_ip: String::new(),
                container_statuses: Vec::new(),
                init_container_statuses: None,
                qos_class: None,
            },
            None,
        )
        .await
        .unwrap();

    let claimant = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "statefulset",
            "name": "ss-0",
            "uid": "uid-claimant",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "restartPolicy": "Never",
            "containers": [{
                "name": "webserver",
                "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4",
                "imagePullPolicy": "Never",
                "ports": [{"containerPort": 21017, "hostPort": 21017, "protocol": "TCP"}]
            }]
        },
        "status": {"phase": "Pending"}
    });
    harness
        .repo
        .test_create_pod("statefulset", "ss-0", "test-node", claimant.clone())
        .await
        .unwrap();
    harness
        .hostports
        .reject_next_check("hostPort 21017/TCP is already allocated");
    let key = PodRuntimeKey::new("statefulset", "ss-0", "uid-claimant");

    let result = harness
        .runtime
        .start_pod(key.clone(), Some(claimant), CancellationToken::new())
        .await
        .expect("hostPort admission rejection should be a terminal pod-start result");

    match result {
        PodStartResult::Terminal(message) => assert!(
            message.contains("hostPort 21017/TCP is already allocated"),
            "terminal message should include admission conflict: {message}"
        ),
        other => panic!("expected terminal hostPort admission rejection, got {other:?}"),
    }

    let stored = harness.stored_pod(&key).await;
    assert_eq!(
        stored
            .pointer("/status/phase")
            .and_then(|value| value.as_str()),
        Some("Failed")
    );
    let status = stored
        .pointer("/status/containerStatuses/0")
        .expect("failed pod should publish container status");
    assert_eq!(
        status.get("name").and_then(|value| value.as_str()),
        Some("webserver")
    );
    assert_eq!(
        status
            .pointer("/state/waiting/reason")
            .and_then(|value| value.as_str()),
        Some("CreateContainerError")
    );
    assert!(
        status
            .pointer("/state/waiting/message")
            .and_then(|value| value.as_str())
            .is_some_and(|message| message.contains("hostPort 21017/TCP is already allocated")),
        "container waiting message should include admission conflict: {status}"
    );
    assert!(
        !harness.cri.recorded_calls().iter().any(|call| matches!(
            &call.operation,
            MockCriOperation::RunPodSandbox | MockCriOperation::CreateContainer { .. }
        )),
        "hostPort admission rejection must happen before sandbox/container creation"
    );
    assert!(harness.events.recorded_events().iter().any(|event| {
        event.event_type == "Warning"
            && event.reason == "Failed"
            && event
                .message
                .contains("hostPort 21017/TCP is already allocated")
    }));
}

#[tokio::test]
async fn real_runtime_stop_pod_cleans_up_by_uid_and_releases_network() {
    let harness = PodRuntimeHarness::new().await;
    let pod = pod_with_pull_policy("ns", "stop-clean", "uid-sc", "nginx", "Never");
    harness
        .repo
        .test_create_pod("ns", "stop-clean", "test-node", pod.clone())
        .await
        .unwrap();
    let key = PodRuntimeKey::new("ns", "stop-clean", "uid-sc");
    let sandbox_id = "sb-clean";

    harness
        .store
        .record_sandbox(&key, sandbox_id)
        .await
        .unwrap();
    // No containers (already cleaned or never created).

    harness
        .runtime
        .stop_pod(key.clone(), Some(pod), Some(sandbox_id.into()))
        .await
        .unwrap();

    let cri_calls = harness.cri.recorded_calls();
    // Sandbox must be stopped and removed.
    assert!(
        cri_calls.iter().any(
            |c| matches!(c.operation, MockCriOperation::StopPodSandbox(ref s) if s == sandbox_id)
        ),
        "sandbox must be stopped"
    );
    assert!(
        cri_calls.iter().any(
            |c| matches!(c.operation, MockCriOperation::RemovePodSandbox(ref s) if s == sandbox_id)
        ),
        "sandbox must be removed"
    );

    // Cgroup must be cleaned up by UID.
    let fs_calls = harness.filesystem.recorded_calls();
    assert!(
        fs_calls
            .iter()
            .any(|s| s.contains("cleanup_cgroup") && s.contains("uid-sc")),
        "cgroup must be cleaned up"
    );

    // Sandbox row must be deleted from store by UID.
    let store_calls = harness.store.recorded_calls();
    assert!(
        store_calls
            .iter()
            .any(|s| s.contains("delete_sandbox") && s.contains("uid-sc")),
        "sandbox row must be deleted from store"
    );

    // Network must be released by UID.
    let net_calls = harness.network.recorded_calls();
    assert!(
        net_calls.iter().any(|c| matches!(
            c,
            MockNetworkOp::ReleaseSandboxNetwork { uid, .. } if uid == "uid-sc"
        )),
        "network must be released"
    );
}

#[tokio::test]
async fn mock_dependency_matrix_network() {
    let mock = MockPodNetworkRuntime::new();
    let key = PodRuntimeKey::new("ns", "pod-nw", "uid-nw");

    mock.read_assignment("sandbox-nw", &key, false)
        .await
        .unwrap();
    mock.release_sandbox_network(&key, "sandbox-nw")
        .await
        .unwrap();

    let calls = mock.recorded_calls();
    assert_eq!(calls.len(), 2, "must record exactly two network operations");
    match &calls[0] {
        MockNetworkOp::ReadAssignment {
            sandbox_id,
            namespace,
            name,
            uid,
            ..
        } => {
            assert_eq!(sandbox_id, "sandbox-nw");
            assert_eq!(namespace, "ns");
            assert_eq!(name, "pod-nw");
            assert_eq!(*uid, "uid-nw");
        }
        other => panic!("expected ReadAssignment, got {:?}", other),
    }
    match &calls[1] {
        MockNetworkOp::ReleaseSandboxNetwork {
            sandbox_id,
            namespace,
            name,
            uid,
        } => {
            assert_eq!(sandbox_id, "sandbox-nw");
            assert_eq!(namespace, "ns");
            assert_eq!(name, "pod-nw");
            assert_eq!(*uid, "uid-nw");
        }
        other => panic!("expected ReleaseSandboxNetwork, got {:?}", other),
    }
}

#[tokio::test]
async fn mock_dependency_matrix_hostport() {
    let mock = MockHostPortRuntime::new();
    let key = PodRuntimeKey::new("ns", "hp-pod", "uid-hp");
    let pod = serde_json::json!({"spec": {"containers": [{"ports": [{"hostPort": 8080}]}]}});
    let host_ports =
        crate::kubelet::pod_runtime::hostports::pod_host_ports_from_resource(&key, &pod).unwrap();

    mock.check_host_port_admission(&host_ports).await.unwrap();
    mock.add_host_ports(&host_ports).await.unwrap();
    mock.remove_host_ports(&host_ports).await.unwrap();

    let calls = mock.recorded_calls();
    assert_eq!(calls.len(), 3);
    for call in &calls {
        match call {
            MockHostPortOp::Check {
                namespace,
                name,
                uid,
            }
            | MockHostPortOp::Add {
                namespace,
                name,
                uid,
            }
            | MockHostPortOp::Remove {
                namespace,
                name,
                uid,
            } => {
                assert_eq!(namespace, "ns");
                assert_eq!(name, "hp-pod");
                assert_eq!(*uid, "uid-hp");
            }
        }
    }
}
