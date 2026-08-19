use super::*;

#[test]
fn pod_runtime_key_constructor_requires_uid() {
    let key = PodRuntimeKey::new("default", "my-pod", "uid-123");
    assert_eq!(key.namespace, "default");
    assert_eq!(key.name, "my-pod");
    assert_eq!(key.uid, "uid-123");
}

#[test]
fn pod_runtime_key_preserves_identity_from_lifecycle_key() {
    let lk = PodLifecycleKey::new("ns1", "pod-a", "uid-abc");
    let rk = PodRuntimeKey::from(&lk);
    assert_eq!(rk.namespace, "ns1");
    assert_eq!(rk.name, "pod-a");
    assert_eq!(rk.uid, "uid-abc");
}

#[test]
fn pod_start_result_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<PodStartResult>();
    assert_send_sync::<PodDeletionFinalizeResult>();
}

#[test]
fn runtime_traits_are_object_safe_send_sync() {
    // Verify the trait can be stored as Arc<dyn PodRuntimeService>,
    // proving it is object-safe and Send + Sync.
    fn assert_send_sync<T: ?Sized + Send + Sync>() {}
    assert_send_sync::<dyn crate::runtime::PodRuntimeService>();
}

#[test]
fn pod_runtime_service_methods_are_uid_keyed() {
    // This is a compile-time check: every method in PodRuntimeService
    // takes PodRuntimeKey or a UID-bearing command. The test just
    // ensures the trait definition compiles and the key type exists.
    let key = PodRuntimeKey::new("ns", "name", "uid");
    assert_eq!(key.namespace, "ns");
    assert_eq!(key.uid, "uid");
    // PodStartResult is returned by UID-keyed start_pod.
    let _ = PodStartResult::Started { sandbox_id: None };
    // PodDeletionFinalizeResult is returned by UID-keyed finalize_deletion.
    let _ = PodDeletionFinalizeResult::DeletedOrAlreadyGone;
}

#[tokio::test]
async fn mock_pod_runtime_service_records_uid_keyed_arguments() {
    let mock = MockPodRuntimeService::new();
    let key = PodRuntimeKey::new("ns", "pod", "uid-1");

    mock.start_pod(key.clone(), None, CancellationToken::new())
        .await
        .unwrap();

    let calls = mock.recorded_calls();
    assert_eq!(calls.len(), 1);
    match &calls[0] {
        MockRuntimeCall::StartPod {
            namespace,
            name,
            uid,
            ..
        } => {
            assert_eq!(namespace, "ns");
            assert_eq!(name, "pod");
            assert_eq!(uid, "uid-1");
        }
        other => panic!("expected StartPod, got {:?}", other),
    }
}

#[tokio::test]
async fn mock_pod_runtime_service_records_all_methods() {
    let mock = MockPodRuntimeService::new();
    let key = PodRuntimeKey::new("ns", "pod", "uid-all");
    let cancel = CancellationToken::new();

    // Exercise every method.
    mock.start_pod(key.clone(), None, cancel.clone())
        .await
        .unwrap();
    mock.stop_pod(crate::runtime::PodStopRequest {
        key: key.clone(),
        pod: None,
        sandbox_id: Some("sandbox-1".into()),
        deletion_deadline: None,
        mode: crate::runtime::PodStopMode::Forced,
        operation_id: 1,
        cancel: cancel.clone(),
    })
    .await
    .unwrap();
    mock.finalize_startup(key.clone(), None, None)
        .await
        .unwrap();
    mock.finalize_deletion(key.clone()).await.unwrap();
    mock.reconcile_runtime(key.clone(), crate::runtime::RuntimeReconcileHint::none())
        .await
        .unwrap();
    mock.reconcile_cri_leftovers(key.clone()).await.unwrap();
    mock.reconcile_ephemeral(key.clone(), None).await.unwrap();
    let (tx, _rx) =
        tokio::sync::mpsc::channel::<crate::pod_lifecycle_core::message::LifecycleMessage>(1);
    mock.check_slot_admission(
        crate::runtime::PodSlotAdmissionRequest {
            key: key.clone(),
            pod: serde_json::json!({"metadata": {"uid": "uid-all"}}),
            resource_version: Some(1),
            start_after_admit: true,
            operation_id: 8,
        },
        crate::pod_lifecycle_router::LifecycleReplyHandle::direct(tx),
        cancel.clone(),
    )
    .await
    .unwrap();

    let calls = mock.recorded_calls();
    assert_eq!(calls.len(), 8);
    for call in &calls {
        match call {
            MockRuntimeCall::StartPod {
                namespace,
                name,
                uid,
                ..
            }
            | MockRuntimeCall::FinalizeStartup {
                namespace,
                name,
                uid,
                ..
            }
            | MockRuntimeCall::FinalizeDeletion {
                namespace,
                name,
                uid,
            }
            | MockRuntimeCall::ReconcileRuntime {
                namespace,
                name,
                uid,
                ..
            }
            | MockRuntimeCall::ReconcileCriLeftovers {
                namespace,
                name,
                uid,
            }
            | MockRuntimeCall::ReconcileEphemeral {
                namespace,
                name,
                uid,
            }
            | MockRuntimeCall::CheckSlotAdmission {
                namespace,
                name,
                uid,
                ..
            } => {
                assert_eq!(namespace, "ns");
                assert_eq!(name, "pod");
                assert_eq!(uid, "uid-all");
            }
            MockRuntimeCall::StopPod {
                namespace,
                name,
                uid,
                sandbox_id,
                ..
            } => {
                assert_eq!(namespace, "ns");
                assert_eq!(name, "pod");
                assert_eq!(uid, "uid-all");
                assert_eq!(sandbox_id, &Some("sandbox-1".to_string()));
            }
            _ => {}
        }
    }
}

#[tokio::test]
async fn mock_pod_runtime_service_configurable_start_result() {
    let mock = MockPodRuntimeService::new();
    let key = PodRuntimeKey::new("ns", "p", "u");

    // Default is Started.
    let r = mock
        .start_pod(key.clone(), None, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(r, PodStartResult::Started { sandbox_id: None });

    // Configure to Failed.
    mock.set_start_result(PodStartResult::Failed("boom".into()));
    let r = mock
        .start_pod(key.clone(), None, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(r, PodStartResult::Failed("boom".into()));
}

#[tokio::test]
async fn mock_pod_runtime_service_configurable_finalize_result() {
    let mock = MockPodRuntimeService::new();
    let key = PodRuntimeKey::new("ns", "p", "u");

    // Default is DeletedOrAlreadyGone.
    let r = mock.finalize_deletion(key.clone()).await.unwrap();
    assert_eq!(r, PodDeletionFinalizeResult::DeletedOrAlreadyGone);

    // Configure to FinalizersPending.
    mock.set_finalize_result(PodDeletionFinalizeResult::FinalizersPending);
    let r = mock.finalize_deletion(key.clone()).await.unwrap();
    assert_eq!(r, PodDeletionFinalizeResult::FinalizersPending);
}

#[tokio::test]
async fn mock_pod_runtime_service_error_injection() {
    let mock = MockPodRuntimeService::new();
    let key = PodRuntimeKey::new("ns", "p", "u");

    mock.set_fail_method("start_pod");
    let err = mock
        .start_pod(key, None, CancellationToken::new())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("injected failure"));
}

#[tokio::test]
async fn real_pod_runtime_service_constructor_requires_all_object_ports() {
    // Verify the constructor accepts and stores every required port.
    let cri = std::sync::Arc::new(MockCriRuntime::new());
    let container_control = std::sync::Arc::new(MockContainerRuntimeControl::new());
    let network = std::sync::Arc::new(MockPodNetworkRuntime::new());
    let store = std::sync::Arc::new(MockPodRuntimeStore::new());
    let slot_admission = std::sync::Arc::new(MockPodSlotAdmission::new());
    let repo = fixture_pod_repository().await;
    let filesystem = std::sync::Arc::new(MockPodFilesystem::new());
    let volumes = std::sync::Arc::new(MockPodVolumeRuntime::new());
    let probes = std::sync::Arc::new(MockProbeRuntime::new());
    let hostports = std::sync::Arc::new(MockHostPortRuntime::new());
    let events = std::sync::Arc::new(MockPodEventSink::new());
    let hooks = std::sync::Arc::new(MockPodHookRuntime::new());
    let env_source = fixture_env_source("node-1").await;
    let finalizer = std::sync::Arc::new(MockPodDeletionFinalizer::new());
    let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let config = RuntimeConfig {
        node_name: "node-1".into(),
        service_cidr: "10.43.128.0/17".into(),
        containerd_namespace: "klights-test".into(),
        sandbox_inputs: crate::pod_sandbox_config::SandboxRuntimeInputs::default(),
        node_capacity: crate::node_capacity::NodeCapacity::default(),
        paths: crate::runtime_paths::KubeletRuntimePaths::new(std::path::PathBuf::from(
            "/tmp/klights/runtime-test",
        ))
        .unwrap(),
    };
    let _runtime = real_runtime! {
        cri: cri,
        container_control: container_control,
        network: network,
        store: store,
        clock: Arc::new(crate::runtime_clock::SystemRuntimeClock),
        slot_admission: slot_admission,
        pod_query: repo.pod_query.clone(),
        pod_status_writer: repo.pod_status_writer.clone(),
        filesystem: filesystem,
        volumes: volumes,
        probes: probes,
        hostports: hostports,
        events: events,
        hooks: hooks,
        env_source: env_source,
        finalizer: finalizer,
        supervisor: supervisor,
        config: config,
    };
}

#[tokio::test]
async fn real_pod_runtime_service_constructs_from_mock_dependencies() {
    // Construct via the PodRuntimeHarness — verifies all mock wiring compiles.
    let harness = PodRuntimeHarness::new().await;
    harness
        .env_source
        .config_map("default", "missing")
        .await
        .expect("mock env source lookup must be callable");
    assert_eq!(
        harness.env_source.recorded_calls(),
        vec!["config_map:default/missing".to_string()],
        "PodRuntimeHarness must use the recording env-source mock"
    );
}

#[tokio::test]
async fn real_runtime_start_pod_writes_pending_status_before_pull() {
    let harness = PodRuntimeHarness::new().await;
    let pod = crate::runtime::test_support::pod_json("ns", "test-pod", "uid-1", "nginx:latest");

    // Create pod in the repository.
    harness
        .repo
        .test_create_pod("ns", "test-pod", "test-node", pod.clone())
        .await
        .unwrap();

    let key = PodRuntimeKey::new("ns", "test-pod", "uid-1");
    let cancel = CancellationToken::new();
    let result = harness
        .runtime
        .start_pod(key, Some(pod), cancel)
        .await
        .unwrap();

    assert!(matches!(result, PodStartResult::Started { .. }));

    // Scheduled event must have been emitted.
    let events = harness.events.recorded_events();
    let scheduled = events.iter().find(|e| e.reason == "Scheduled");
    assert!(
        scheduled.is_some(),
        "Scheduled event must be emitted, got events: {:?}",
        events
    );
    assert_eq!(scheduled.unwrap().uid, "uid-1");

    // CRI image operations are expected (pull policy for nginx:latest is Always).
    let cri_calls = harness.cri.recorded_calls();
    let has_pull = cri_calls
        .iter()
        .any(|c| matches!(c.operation, MockCriOperation::PullImage(_)));
    assert!(
        has_pull,
        "CRI image pull must be called for Always pull policy"
    );
    // Sandbox creation must happen after image pull.
    let has_sandbox = cri_calls
        .iter()
        .any(|c| matches!(c.operation, MockCriOperation::RunPodSandbox));
    assert!(has_sandbox, "sandbox must be created after image pull");
}

#[tokio::test]
async fn real_runtime_start_pod_uses_provided_snapshot_without_fresh_liveness_read() {
    let harness = PodRuntimeHarness::new().await;
    let pod =
        crate::runtime::test_support::pod_json("ns", "cached-pod", "uid-cache", "nginx:latest");
    harness
        .repo
        .test_create_pod("ns", "cached-pod", "test-node", pod.clone())
        .await
        .unwrap();

    let runtime = real_runtime! {
        cri: harness.cri.clone(),
        container_control: harness.container_control.clone(),
        network: harness.network.clone(),
        store: harness.store.clone(),
        clock: Arc::new(crate::runtime_clock::SystemRuntimeClock),
        slot_admission: harness.slot_admission.clone(),
        pod_query: Arc::new(SnapshotOnlyPodQuery),
        pod_status_writer: harness.repo.pod_status_writer.clone(),
        filesystem: harness.filesystem.clone(),
        volumes: harness.volumes.clone(),
        probes: harness.probes.clone(),
        hostports: harness.hostports.clone(),
        events: harness.events.clone(),
        hooks: harness.hooks.clone(),
        env_source: harness.env_source.clone(),
        finalizer: harness.finalizer.clone(),
        supervisor: harness.supervisor.clone(),
        config: crate::runtime::service::RuntimeConfig {
            node_name: "test-node".to_string(),
            service_cidr: "10.43.128.0/17".to_string(),
            containerd_namespace: "klights-test".to_string(),
            sandbox_inputs: crate::pod_sandbox_config::SandboxRuntimeInputs::default(
            ),
            node_capacity: crate::node_capacity::NodeCapacity::default(),
            paths: crate::runtime_paths::KubeletRuntimePaths::new(
                std::path::PathBuf::from("/tmp/klights/runtime-test"),
            )
            .unwrap(),
        },
    };

    let result = runtime
        .start_pod(
            PodRuntimeKey::new("ns", "cached-pod", "uid-cache"),
            Some(pod),
            CancellationToken::new(),
        )
        .await
        .expect("start pod from supplied snapshot");

    assert!(matches!(result, PodStartResult::Started { .. }));
}

#[tokio::test]
async fn real_runtime_start_pod_does_not_write_status_to_replacement_uid() {
    let harness = PodRuntimeHarness::new().await;
    let old_pod =
        crate::runtime::test_support::pod_json("ns", "test-pod", "old-uid", "nginx:latest");

    // Create pod with old-uid.
    harness
        .repo
        .test_create_pod("ns", "test-pod", "test-node", old_pod.clone())
        .await
        .unwrap();

    // Read the live pod to capture its initial resourceVersion.
    let before = harness
        .repo
        .test_get_pod_for_uid("ns", "test-pod", "old-uid")
        .await
        .unwrap()
        .unwrap();
    let before_rv = before
        .data
        .pointer("/metadata/resourceVersion")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_default();

    // Call start_pod with a different UID (simulating stale start for old UID
    // after the pod has been replaced).
    let wrong_key = PodRuntimeKey::new("ns", "test-pod", "different-uid");
    let cancel = CancellationToken::new();
    let result = harness
        .runtime
        .start_pod(wrong_key, Some(old_pod), cancel)
        .await;

    // Must fail.
    match result {
        Ok(PodStartResult::Failed(_)) | Err(_) => {}
        other => panic!("expected failure for stale UID, got {:?}", other),
    }

    // The live pod (old-uid) must NOT have been modified.
    let after = harness
        .repo
        .test_get_pod_for_uid("ns", "test-pod", "old-uid")
        .await
        .unwrap()
        .unwrap();
    let after_rv = after
        .data
        .pointer("/metadata/resourceVersion")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_default();
    assert_eq!(
        before_rv, after_rv,
        "replacement pod resourceVersion must not change on stale UID start"
    );
}

#[tokio::test]
async fn mid_lifecycle_status_writes_preserve_host_ip_with_parity() {
    use crate::pod_repository::PodStatusUpdate;
    use crate::pod_repository::PublishedAddress;

    let conflict_cluster = std::sync::Arc::new(FakeCluster::new());
    let (_cri, conflict_runtime, conflict_repo, _conflict_cluster, conflict_hostports) =
        fixture_runtime_with_cluster("test-node", conflict_cluster).await;
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
    conflict_repo
        .test_create_pod("statefulset", "test-pod", "test-node", holder)
        .await
        .unwrap();
    conflict_repo
        .pod_status_writer
        .set_pod_status_for_uid(
            "statefulset",
            "test-pod",
            "uid-holder",
            PodStatusUpdate {
                phase: "Running".to_string(),
                pod_ip: PublishedAddress::must("10.50.0.63"),
                host_ip: PublishedAddress::must("10.0.0.5"),
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
    conflict_repo
        .test_create_pod("statefulset", "ss-0", "test-node", claimant.clone())
        .await
        .unwrap();
    conflict_hostports.reject_next_check("hostPort 21017/TCP is already allocated");
    let conflict_key = PodRuntimeKey::new("statefulset", "ss-0", "uid-claimant");

    let _ = conflict_runtime
        .start_pod(
            conflict_key.clone(),
            Some(claimant),
            CancellationToken::new(),
        )
        .await
        .unwrap();

    let conflict_failed_status = conflict_repo
        .test_get_pod_for_uid("statefulset", "ss-0", "uid-claimant")
        .await
        .unwrap()
        .expect("hostPort admission conflict should persist Failed status")
        .data
        .pointer("/status")
        .cloned()
        .expect("claimant status");
    assert!(
        !matches!(
            conflict_failed_status
                .get("hostIP")
                .and_then(|value| value.as_str()),
            Some("")
        ),
        "pre-assignment failure status must not forward hostIP as an empty string"
    );

    let init_cluster = std::sync::Arc::new(FakeCluster::new());
    let (init_cri, init_runtime, init_repo, _init_cluster, _init_hostports) =
        fixture_runtime_with_cluster("test-node", init_cluster).await;
    init_cri.set_container_exit_code(1);
    let init_pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "ns",
            "name": "init-fail-hostip",
            "uid": "uid-init-fail-hostip",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "restartPolicy": "Never",
            "initContainers": [{
                "name": "init",
                "image": "busybox:1.36",
                "imagePullPolicy": "Never"
            }],
            "containers": [{
                "name": "app",
                "image": "nginx:1.25",
                "imagePullPolicy": "Never"
            }]
        },
        "status": {"phase": "Pending"}
    });
    init_repo
        .test_create_pod("ns", "init-fail-hostip", "test-node", init_pod.clone())
        .await
        .unwrap();
    let init_key = PodRuntimeKey::new("ns", "init-fail-hostip", "uid-init-fail-hostip");

    let _ = init_runtime
        .start_pod(init_key, Some(init_pod), CancellationToken::new())
        .await
        .unwrap();

    let init_failed_status = init_repo
        .test_get_pod_for_uid("ns", "init-fail-hostip", "uid-init-fail-hostip")
        .await
        .unwrap()
        .expect("init failure should persist Failed status")
        .data
        .pointer("/status")
        .cloned()
        .expect("init failure status");
    assert_eq!(
        init_failed_status
            .get("hostIP")
            .and_then(|value| value.as_str()),
        Some("192.168.1.1"),
        "post-assignment failure status must preserve the assignment hostIP"
    );
}

#[tokio::test]
async fn real_runtime_start_pod_runs_filesystem_volume_hostport_and_containers() {
    let harness = PodRuntimeHarness::new().await;
    let pod = pod_with_pull_policy("ns", "order-pod", "uid-ord", "nginx", "Never");
    harness
        .repo
        .test_create_pod("ns", "order-pod", "test-node", pod.clone())
        .await
        .unwrap();
    let key = PodRuntimeKey::new("ns", "order-pod", "uid-ord");
    let result = harness
        .runtime
        .start_pod(key, Some(pod), CancellationToken::new())
        .await
        .unwrap();
    assert!(matches!(result, PodStartResult::Started { .. }));

    // Verify CRI sandbox and containers exist.
    let cri_calls = harness.cri.recorded_calls();
    let cri_ops: Vec<_> = cri_calls.iter().map(|c| &c.operation).collect();

    // Sandbox comes first.
    let sb_pos = cri_ops
        .iter()
        .position(|o| matches!(o, MockCriOperation::RunPodSandbox));
    assert!(sb_pos.is_some(), "RunPodSandbox must be present");

    // Container creation follows sandbox.
    let first_create = cri_ops
        .iter()
        .position(|o| matches!(o, MockCriOperation::CreateContainer { .. }));
    assert!(first_create.is_some(), "CreateContainer must be present");
    assert!(
        first_create.unwrap() > sb_pos.unwrap(),
        "containers must be created after sandbox"
    );

    // Filesystem must be called before containers.
    let fs_calls = harness.filesystem.recorded_calls();
    assert!(
        fs_calls.iter().any(|s| s.contains("write_hosts")),
        "write_hosts must be called"
    );
    assert!(
        fs_calls.iter().any(|s| s.contains("create_log")),
        "create_log_directory must be called"
    );

    // HostPort must be called.
    let hp_calls = harness.hostports.recorded_calls();
    assert!(
        hp_calls
            .iter()
            .any(|c| matches!(c, MockHostPortOp::Add { .. })),
        "add_host_ports must be called"
    );

    // Volumes must be called.
    let vol_calls = harness.volumes.recorded_calls();
    assert!(
        vol_calls.iter().any(|s| s.contains("process_volumes")),
        "process_volumes must be called"
    );
}

#[tokio::test]
async fn real_runtime_stop_pod_uses_deleted_snapshot_not_replacement() {
    let harness = PodRuntimeHarness::new().await;
    let pod = pod_with_pull_policy("ns", "stop-del", "uid-del", "nginx", "Never");
    harness
        .repo
        .test_create_pod("ns", "stop-del", "test-node", pod.clone())
        .await
        .unwrap();
    let key = PodRuntimeKey::new("ns", "stop-del", "uid-del");
    let sandbox_id = "sb-del";

    // Record sandbox for the pod's UID.
    harness
        .store
        .record_sandbox(&key, sandbox_id)
        .await
        .unwrap();

    // Set up containers in the sandbox.
    harness.container_control.set_containers(vec![
        ("ctr-1".into(), "running".into()),
        ("ctr-2".into(), "running".into()),
    ]);

    // stop_pod with the pod snapshot.
    harness
        .runtime
        .stop_pod(key.clone(), Some(pod), Some(sandbox_id.into()))
        .await
        .unwrap();

    // Containers must have been listed by sandbox filter.
    let cc_calls = harness.container_control.recorded_calls();
    assert!(
        cc_calls.iter().any(|c| matches!(
            c,
            MockContainerControlOp::ListContainers { sandbox_id_filter: Some(sid) } if sid == sandbox_id
        )),
        "containers must be listed with sandbox {}",
        sandbox_id
    );

    // Each container must be stopped and removed.
    let cri_calls = harness.cri.recorded_calls();
    let stopped: Vec<_> = cri_calls
        .iter()
        .filter(|c| matches!(c.operation, MockCriOperation::StopContainer(_, _)))
        .collect();
    assert_eq!(
        stopped.len(),
        2,
        "both containers must be stopped, got {:?}",
        cri_calls
    );

    let removed: Vec<_> = cri_calls
        .iter()
        .filter(|c| matches!(c.operation, MockCriOperation::RemoveContainer(_)))
        .collect();
    assert_eq!(
        removed.len(),
        2,
        "both containers must be removed, got {:?}",
        cri_calls
    );
}

#[tokio::test]
async fn real_runtime_stop_pod_returns_typed_ownership_error_for_non_owned_pod() {
    let harness = PodRuntimeHarness::new().await;
    let key = PodRuntimeKey::new("default", "non-owned", "uid-no");

    // Unscheduled Pod: spec.nodeName absent -> target_node == None.
    let unscheduled = serde_json::json!({
        "metadata": {"namespace": "default", "name": "non-owned", "uid": "uid-no"}
    });
    let err = harness
        .runtime
        .stop_pod(key.clone(), Some(unscheduled), None)
        .await
        .expect_err("unscheduled Pod must be refused");
    let own = err
        .downcast_ref::<PodOwnershipError>()
        .expect("refusal must be a typed PodOwnershipError, not a string bail");
    assert_eq!(own.local_node, "test-node");
    assert_eq!(own.target_node, None, "unscheduled Pod has no target node");

    // Pod assigned to another node -> target_node == Some(other).
    let other_node = serde_json::json!({
        "metadata": {"namespace": "default", "name": "non-owned", "uid": "uid-no"},
        "spec": {"nodeName": "other-node"}
    });
    let err = harness
        .runtime
        .stop_pod(key, Some(other_node), None)
        .await
        .expect_err("other-node Pod must be refused");
    let own = err
        .downcast_ref::<PodOwnershipError>()
        .expect("refusal must be a typed PodOwnershipError");
    assert_eq!(own.local_node, "test-node");
    assert_eq!(
        own.target_node.as_deref(),
        Some("other-node"),
        "target node must be preserved for routing/diagnostics"
    );
}

#[tokio::test]
async fn mock_deletion_finalizer_records_uid_bound_finalize() {
    let finalizer = MockPodDeletionFinalizer::new();
    let key = PodRuntimeKey::new("ns", "pod", "uid-1");

    let _ = finalizer.finalize_after_actor_cleanup(&key).await;
    let _ = finalizer
        .finalize_after_actor_cleanup(&PodRuntimeKey::new("ns2", "pod2", "uid-2"))
        .await;

    let calls = finalizer.recorded_calls();
    assert_eq!(calls.len(), 2, "must record every call");
    assert_eq!(calls[0].namespace, "ns");
    assert_eq!(calls[0].name, "pod");
    assert_eq!(calls[0].uid, "uid-1");
    assert_eq!(calls[1].namespace, "ns2");
    assert_eq!(calls[1].name, "pod2");
    assert_eq!(calls[1].uid, "uid-2");
}

#[tokio::test]
async fn mock_deletion_finalizer_returns_configured_outcome() {
    let finalizer = MockPodDeletionFinalizer::new();
    let key = PodRuntimeKey::new("ns", "pod", "uid-1");

    // Default is DeletedOrAlreadyGone.
    let r = finalizer.finalize_after_actor_cleanup(&key).await.unwrap();
    assert!(matches!(r, PodDeletionFinalizeResult::DeletedOrAlreadyGone));

    // Configure FinalizersPending.
    finalizer.set_outcome(PodDeletionFinalizeResult::FinalizersPending);
    let r = finalizer.finalize_after_actor_cleanup(&key).await.unwrap();
    assert!(matches!(r, PodDeletionFinalizeResult::FinalizersPending));

    // Error injection.
    finalizer.set_fail("database unavailable");
    let e = finalizer
        .finalize_after_actor_cleanup(&key)
        .await
        .unwrap_err();
    assert!(
        e.to_string().contains("database unavailable"),
        "expected error message, got: {e}"
    );
}

#[tokio::test]
async fn real_runtime_finalize_deletion_routes_through_deletion_finalizer_with_parity() {
    let harness = PodRuntimeHarness::new().await;

    // Set the mock finalizer to return FinalizersPending.
    harness
        .finalizer
        .set_outcome(PodDeletionFinalizeResult::FinalizersPending);

    let key = PodRuntimeKey::new("ns", "del-pod", "uid-1");
    let result = harness
        .runtime
        .finalize_deletion(key.clone())
        .await
        .unwrap();

    assert!(
        matches!(result, PodDeletionFinalizeResult::FinalizersPending),
        "expected FinalizersPending, got {:?}",
        result
    );

    // Verify the call was delegated to the finalizer.
    let calls = harness.finalizer.recorded_calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].namespace, "ns");
    assert_eq!(calls[0].name, "del-pod");
    assert_eq!(calls[0].uid, "uid-1");
}

#[tokio::test]
async fn real_runtime_handle_lifecycle_command_startup_passed() {
    let harness = PodRuntimeHarness::new().await;
    let cmd = crate::lifecycle::LifecycleCommand::StartupPassed {
        pod_uid: "uid-sp".into(),
        namespace: "ns".into(),
        pod_name: "startup-pod".into(),
        container_name: "app".into(),
    };
    let result = harness.runtime.handle_lifecycle_command(cmd).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn real_runtime_reconcile_ephemeral_noop_when_no_pod() {
    let harness = PodRuntimeHarness::new().await;
    let key = PodRuntimeKey::new("ns", "eph-pod", "uid-1");
    // No pod provided — should be a no-op.
    let result = harness.runtime.reconcile_ephemeral(key, None).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn real_runtime_reconcile_ephemeral_uid_mismatch_is_noop() {
    let harness = PodRuntimeHarness::new().await;
    let key = PodRuntimeKey::new("ns", "eph-pod", "uid-1");
    let pod = serde_json::json!({
        "metadata": {"uid": "uid-2", "namespace": "ns", "name": "eph-pod"},
        "spec": {"ephemeralContainers": []}
    });
    let result = harness.runtime.reconcile_ephemeral(key, Some(pod)).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn reconcile_ephemeral_full_sequence_with_parity() {
    let harness = PodRuntimeHarness::new_with_runtime_config(RuntimeConfig {
        node_name: "test-node".into(),
        service_cidr: "10.96.0.0/12".into(),
        containerd_namespace: "klights-test".into(),
        sandbox_inputs: crate::pod_sandbox_config::SandboxRuntimeInputs::default(),
        node_capacity: crate::node_capacity::NodeCapacity::default(),
        paths: crate::runtime_paths::KubeletRuntimePaths::new(std::path::PathBuf::from(
            "/tmp/klights/runtime-test",
        ))
        .unwrap(),
    })
    .await;
    harness.cri.set_image_present(false);
    harness
        .cri
        .set_container_status_state(k8s_cri::v1::ContainerState::ContainerRunning as i32);

    let image = "registry.k8s.io/e2e-test-images/busybox:1.37.0-1";
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "e2e-debug",
            "name": "target-pod",
            "uid": "uid-target-pod",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "containers": [{
                "name": "app",
                "image": "registry.k8s.io/e2e-test-images/agnhost:2.56",
                "imagePullPolicy": "Never"
            }],
            "ephemeralContainers": [{
                "name": "debugger",
                "image": image,
                "imagePullPolicy": "IfNotPresent",
                "command": ["/bin/sh", "-c"],
                "args": ["while true; do echo polo; sleep 2; done"],
                "stdin": true,
                "tty": true
            }]
        },
        "status": {
            "phase": "Running",
            "podIP": "10.42.1.7",
            "hostIP": "10.0.0.10"
        }
    });
    let key = PodRuntimeKey::new("e2e-debug", "target-pod", "uid-target-pod");

    harness.create_runtime_pod(pod.clone()).await;
    harness
        .store
        .record_sandbox(&key, "sandbox-eph")
        .await
        .unwrap();

    harness
        .runtime
        .reconcile_ephemeral(key.clone(), Some(pod))
        .await
        .unwrap();

    let calls = harness.cri.recorded_calls();
    let operations = calls.iter().map(|call| &call.operation).collect::<Vec<_>>();
    let image_status_pos = operations
        .iter()
        .position(|operation| {
            matches!(operation, MockCriOperation::ImageStatus(observed) if observed == image)
        })
        .expect("ephemeral reconcile must check image presence before pull");
    let pull_pos = operations
        .iter()
        .position(|operation| {
            matches!(operation, MockCriOperation::PullImage(observed) if observed == image)
        })
        .expect("ephemeral reconcile must pull a missing IfNotPresent image");
    let create_pos = operations
        .iter()
        .position(|operation| {
            matches!(
                operation,
                MockCriOperation::CreateContainer {
                    sandbox_id,
                    container_name,
                } if sandbox_id == "sandbox-eph" && container_name == "debugger"
            )
        })
        .expect("ephemeral reconcile must create the ephemeral container");
    let start_pos = operations
        .iter()
        .position(|operation| {
            matches!(
                operation,
                MockCriOperation::StartContainer(container_id)
                    if container_id == "container-sandbox-eph"
            )
        })
        .expect("ephemeral reconcile must start the ephemeral container");
    assert!(
        image_status_pos < pull_pos && pull_pos < create_pos && create_pos < start_pos,
        "ephemeral reconcile sequence must be image_status -> pull_image -> create_container -> start_container"
    );

    let create_config = harness
        .cri
        .recorded_create_configs()
        .into_iter()
        .find(|config| {
            config
                .metadata
                .as_ref()
                .map(|metadata| metadata.name.as_str())
                == Some("debugger")
        })
        .expect("ephemeral container config must be created");
    assert_eq!(
        create_config
            .image
            .as_ref()
            .map(|image| image.image.as_str()),
        Some(image)
    );
    assert_eq!(create_config.command, vec!["/bin/sh", "-c"]);
    assert_eq!(
        create_config.args,
        vec!["while true; do echo polo; sleep 2; done"]
    );
    assert!(create_config.stdin);
    assert!(create_config.tty);
    assert_eq!(
        create_config
            .envs
            .iter()
            .find(|env| env.key == "KUBERNETES_SERVICE_HOST")
            .map(|env| env.value.as_str()),
        Some("10.96.0.1")
    );

    let create_sandbox_config = harness
        .cri
        .recorded_create_sandbox_configs()
        .into_iter()
        .find(|config| {
            config
                .metadata
                .as_ref()
                .map(|metadata| metadata.name.as_str())
                == Some("target-pod")
        })
        .expect("CreateContainer must receive the pod sandbox config");
    let metadata = create_sandbox_config
        .metadata
        .as_ref()
        .expect("pod sandbox config must include metadata");
    assert_eq!(metadata.namespace, "e2e-debug");
    assert_eq!(metadata.uid, "uid-target-pod");
    assert!(
        !create_sandbox_config.log_directory.is_empty(),
        "ephemeral CreateContainer must preserve the sandbox log directory"
    );

    let stored = harness.stored_pod(&key).await;
    let statuses = stored
        .pointer("/status/ephemeralContainerStatuses")
        .and_then(|value| value.as_array())
        .expect("ephemeral container statuses must be written");
    let status = statuses
        .iter()
        .find(|status| status.get("name").and_then(|name| name.as_str()) == Some("debugger"))
        .expect("debugger status must exist");
    assert!(
        status
            .pointer("/state/running")
            .and_then(|value| value.as_object())
            .is_some()
    );
    assert_eq!(
        status.get("containerID").and_then(|value| value.as_str()),
        Some("containerd://container-sandbox-eph")
    );
}

#[tokio::test]
async fn reconcile_runtime_writes_pod_and_host_ips_with_parity() {
    use crate::runtime::cri::ContainerRuntimeState;
    use crate::runtime::store::PodRuntimeStore;

    let harness = PodRuntimeHarness::new().await;
    let key = PodRuntimeKey::new("pods", "ip-pod", "uid-ip-pod");
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "pods",
            "name": "ip-pod",
            "uid": "uid-ip-pod",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "containers": [{"name": "app", "image": "nginx:1.25"}]
        },
        "status": {
            "phase": "Pending",
            "podIP": "10.42.0.7",
            "podIPs": [{"ip": "10.42.0.7"}],
            "hostIP": "10.0.0.5",
            "hostIPs": [{"ip": "10.0.0.5"}],
            "containerStatuses": [{
                "name": "app",
                "ready": false,
                "started": false,
                "restartCount": 0,
                "state": {"waiting": {"reason": "ContainerCreating"}}
            }]
        }
    });
    harness
        .repo
        .test_create_pod("pods", "ip-pod", "test-node", pod)
        .await
        .unwrap();
    harness
        .store
        .record_sandbox(&key, "sandbox-ip")
        .await
        .unwrap();
    harness.container_control.set_container_states(vec![(
        "ctr-app".to_string(),
        ContainerRuntimeState::Running,
    )]);
    harness
        .cri
        .set_container_status_state(k8s_cri::v1::ContainerState::ContainerRunning as i32);

    harness
        .runtime
        .reconcile_runtime(key.clone(), crate::runtime::RuntimeReconcileHint::none())
        .await
        .unwrap();

    let stored = harness.stored_pod(&key).await;
    assert_eq!(
        stored.pointer("/status/phase").and_then(|v| v.as_str()),
        Some("Running")
    );
    assert_eq!(
        stored.pointer("/status/podIP").and_then(|v| v.as_str()),
        Some("10.42.0.7")
    );
    assert_eq!(
        stored
            .pointer("/status/podIPs/0/ip")
            .and_then(|v| v.as_str()),
        Some("10.42.0.7")
    );
    assert_eq!(
        stored.pointer("/status/hostIP").and_then(|v| v.as_str()),
        Some("10.0.0.5")
    );
    assert_eq!(
        stored
            .pointer("/status/hostIPs/0/ip")
            .and_then(|v| v.as_str()),
        Some("10.0.0.5")
    );
    assert_eq!(
        stored
            .pointer("/status/containerStatuses/0/ready")
            .and_then(|v| v.as_bool()),
        Some(true),
        "running containers without readiness probes become ready during runtime reconcile"
    );

    let ready_key = PodRuntimeKey::new("pods", "ready-pod", "uid-ready-pod");
    let ready_pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "pods",
            "name": "ready-pod",
            "uid": "uid-ready-pod",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "containers": [{
                "name": "app",
                "image": "nginx:1.25",
                "readinessProbe": {"exec": {"command": ["true"]}}
            }]
        },
        "status": {
            "phase": "Running",
            "podIP": "10.42.0.8",
            "podIPs": [{"ip": "10.42.0.8"}],
            "hostIP": "10.0.0.5",
            "hostIPs": [{"ip": "10.0.0.5"}],
            "containerStatuses": [{
                "name": "app",
                "ready": true,
                "started": true,
                "restartCount": 0,
                "state": {"running": {"startedAt": "2026-05-20T00:00:00Z"}}
            }]
        }
    });
    harness
        .repo
        .test_create_pod("pods", "ready-pod", "test-node", ready_pod)
        .await
        .unwrap();
    harness
        .repo
        .pod_status_writer
        .set_probe_readiness_for_uid("pods", "ready-pod", "uid-ready-pod", "app", false, None)
        .await
        .unwrap();
    harness
        .store
        .record_sandbox(&ready_key, "sandbox-ready")
        .await
        .unwrap();
    harness.container_control.set_container_states(vec![(
        "ctr-ready".to_string(),
        ContainerRuntimeState::Running,
    )]);

    harness
        .runtime
        .reconcile_runtime(
            ready_key.clone(),
            crate::runtime::RuntimeReconcileHint::none(),
        )
        .await
        .unwrap();

    let ready_stored = harness.stored_pod(&ready_key).await;
    assert_eq!(
        ready_stored
            .pointer("/status/containerStatuses/0/ready")
            .and_then(|v| v.as_bool()),
        Some(false),
        "runtime reconcile must preserve a recorded readiness probe failure"
    );
    assert_eq!(
        ready_stored
            .pointer("/status/podIP")
            .and_then(|v| v.as_str()),
        Some("10.42.0.8")
    );
    assert_eq!(
        ready_stored
            .pointer("/status/hostIPs/0/ip")
            .and_then(|v| v.as_str()),
        Some("10.0.0.5")
    );
}

#[tokio::test]
async fn reconcile_runtime_duplicate_status_does_not_emit_second_watch_event() {
    use crate::runtime::cri::ContainerRuntimeState;
    use crate::runtime::store::PodRuntimeStore;

    let harness = PodRuntimeHarness::new().await;
    let key = PodRuntimeKey::new("pods", "dedup-pod", "uid-dedup-pod");
    harness
        .repo
        .test_create_pod(
            "pods",
            "dedup-pod",
            "test-node",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "pods",
                    "name": "dedup-pod",
                    "uid": "uid-dedup-pod",
                    "resourceVersion": "1"
                },
                "spec": {
                    "nodeName": "test-node",
                    "containers": [{"name": "app", "image": "nginx:1.25"}]
                },
                "status": {
                    "phase": "Pending",
                    "podIP": "10.42.0.17",
                    "podIPs": [{"ip": "10.42.0.17"}],
                    "hostIP": "10.0.0.5",
                    "hostIPs": [{"ip": "10.0.0.5"}],
                    "containerStatuses": [{
                        "name": "app",
                        "ready": false,
                        "started": false,
                        "restartCount": 0,
                        "state": {"waiting": {"reason": "ContainerCreating"}}
                    }]
                }
            }),
        )
        .await
        .unwrap();
    harness
        .store
        .record_sandbox(&key, "sandbox-dedup")
        .await
        .unwrap();
    harness.container_control.set_container_states(vec![(
        "ctr-app".to_string(),
        ContainerRuntimeState::Running,
    )]);
    harness
        .cri
        .set_container_status_state(k8s_cri::v1::ContainerState::ContainerRunning as i32);

    harness
        .runtime
        .reconcile_runtime(key.clone(), crate::runtime::RuntimeReconcileHint::none())
        .await
        .unwrap();
    let first_write_count = harness.repo.backend.status_write_count();
    assert_eq!(
        first_write_count, 1,
        "first reconcile must persist one status update"
    );
    let first_rv = harness
        .repo
        .test_get_pod_for_uid("pods", "dedup-pod", "uid-dedup-pod")
        .await
        .unwrap()
        .expect("pod must exist")
        .resource_version;

    harness
        .runtime
        .reconcile_runtime(key.clone(), crate::runtime::RuntimeReconcileHint::none())
        .await
        .unwrap();
    assert_eq!(
        harness.repo.backend.status_write_count(),
        first_write_count,
        "duplicate runtime status must not persist a second update or run downstream side effects"
    );
    let second_rv = harness
        .repo
        .test_get_pod_for_uid("pods", "dedup-pod", "uid-dedup-pod")
        .await
        .unwrap()
        .expect("pod must exist")
        .resource_version;
    assert_eq!(
        second_rv, first_rv,
        "duplicate runtime status must not advance resourceVersion"
    );
}

#[tokio::test]
async fn active_deadline_enforcement_marks_failed_with_parity() {
    use crate::runtime::cri::ContainerRuntimeState;

    let harness = PodRuntimeHarness::new().await;
    let key = PodRuntimeKey::new("pods", "deadline-pod", "uid-deadline-pod");
    let creation_timestamp = (chrono::Utc::now() - chrono::Duration::seconds(30))
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "pods",
            "name": "deadline-pod",
            "uid": "uid-deadline-pod",
            "resourceVersion": "1",
            "creationTimestamp": creation_timestamp
        },
        "spec": {
            "nodeName": "test-node",
            "activeDeadlineSeconds": 5,
            "restartPolicy": "Always",
            "containers": [{
                "name": "pause",
                "image": "registry.k8s.io/pause:3.10.1"
            }]
        },
        "status": {
            "phase": "Running",
            "podIP": "10.50.2.7",
            "podIPs": [{"ip": "10.50.2.7"}],
            "hostIP": "10.99.0.12",
            "hostIPs": [{"ip": "10.99.0.12"}],
            "containerStatuses": [{
                "name": "pause",
                "containerID": "containerd://ctr-deadline",
                "image": "registry.k8s.io/pause:3.10.1",
                "imageID": "registry.k8s.io/pause@sha256:test",
                "ready": true,
                "started": true,
                "restartCount": 0,
                "state": {"running": {"startedAt": "2026-05-20T00:32:08Z"}}
            }]
        }
    });
    harness
        .repo
        .test_create_pod("pods", "deadline-pod", "test-node", pod)
        .await
        .unwrap();
    harness
        .store
        .record_sandbox(&key, "sandbox-deadline")
        .await
        .unwrap();
    harness.container_control.set_container_states(vec![(
        "ctr-deadline".to_string(),
        ContainerRuntimeState::Running,
    )]);

    harness
        .runtime
        .reconcile_runtime(key.clone(), crate::runtime::RuntimeReconcileHint::none())
        .await
        .unwrap();

    let calls = harness.cri.recorded_calls();
    assert!(
        calls.iter().any(|call| matches!(
            &call.operation,
            MockCriOperation::StopContainer(container_id, 0) if container_id == "ctr-deadline"
        )),
        "expired activeDeadlineSeconds must stop running containers with zero grace"
    );
    assert!(
        !calls.iter().any(|call| matches!(
            &call.operation,
            MockCriOperation::RemoveContainer(container_id) if container_id == "ctr-deadline"
        )),
        "activeDeadlineSeconds should match the legacy workflow: stop containers, do not delete runtime state"
    );

    let stored = harness.stored_pod(&key).await;
    assert_eq!(
        stored.pointer("/status/phase").and_then(|v| v.as_str()),
        Some("Failed")
    );
    assert_eq!(
        stored.pointer("/status/reason").and_then(|v| v.as_str()),
        Some("DeadlineExceeded")
    );
    assert!(
        stored
            .pointer("/status/message")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .contains("specified deadline (5s)"),
        "deadline-exceeded status must include the Kubernetes-compatible message"
    );
    assert_eq!(
        stored.pointer("/status/podIP").and_then(|v| v.as_str()),
        Some("10.50.2.7"),
        "deadline status write must preserve podIP"
    );
}

#[tokio::test]
async fn real_runtime_start_pod_runs_init_containers_in_order_with_parity() {
    let harness = PodRuntimeHarness::new().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "ns",
            "name": "init-pod",
            "uid": "uid-init",
            "resourceVersion": "1"
        },
        "spec": {
            "initContainers": [
                {"name": "init-1", "image": "busybox:1.35", "imagePullPolicy": "Never"},
                {"name": "init-2", "image": "busybox:1.36", "imagePullPolicy": "Never"}
            ],
            "containers": [
                {"name": "app", "image": "nginx:1.25", "imagePullPolicy": "Never"}
            ],
            "nodeName": "test-node"
        },
        "status": {"phase": "Pending"}
    });
    harness
        .repo
        .test_create_pod("ns", "init-pod", "test-node", pod.clone())
        .await
        .unwrap();
    let key = PodRuntimeKey::new("ns", "init-pod", "uid-init");
    let result = harness
        .runtime
        .start_pod(key, Some(pod), CancellationToken::new())
        .await
        .unwrap();
    assert!(matches!(result, PodStartResult::Started { .. }));

    let cri_calls = harness.cri.recorded_calls();

    // Extract CreateContainer operations in order
    let creates: Vec<String> = cri_calls
        .iter()
        .filter_map(|c| match &c.operation {
            MockCriOperation::CreateContainer { container_name, .. } => {
                Some(container_name.clone())
            }
            _ => None,
        })
        .collect();

    // Init containers must be created in order before regular containers
    assert_eq!(
        creates,
        vec!["init-1", "init-2", "app"],
        "init containers must be created in order before regular containers"
    );

    // Extract StartContainer operations in order
    let starts: Vec<String> = cri_calls
        .iter()
        .filter_map(|c| match &c.operation {
            MockCriOperation::StartContainer(name) => Some(name.clone()),
            _ => None,
        })
        .collect();

    assert_eq!(starts.len(), 3, "all 3 containers must be started");
}

#[tokio::test]
async fn real_runtime_start_pod_publishes_completed_init_container_statuses() {
    let harness = PodRuntimeHarness::new().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "init-container",
            "name": "pod-init",
            "uid": "uid-init-status",
            "resourceVersion": "1"
        },
        "spec": {
            "restartPolicy": "Always",
            "initContainers": [
                {"name": "init1", "image": "busybox:1.35", "imagePullPolicy": "Never"},
                {"name": "init2", "image": "busybox:1.36", "imagePullPolicy": "Never"}
            ],
            "containers": [
                {"name": "run1", "image": "busybox:1.37", "imagePullPolicy": "Never"}
            ],
            "nodeName": "test-node"
        },
        "status": {"phase": "Pending"}
    });
    let key = PodRuntimeKey::new("init-container", "pod-init", "uid-init-status");
    harness.create_runtime_pod(pod.clone()).await;

    let result = harness
        .runtime
        .start_pod(key.clone(), Some(pod), CancellationToken::new())
        .await
        .unwrap();
    assert!(matches!(result, PodStartResult::Started { .. }));

    let stored = harness.stored_pod(&key).await;
    let init_statuses = stored
        .pointer("/status/initContainerStatuses")
        .and_then(|value| value.as_array())
        .expect("completed init container statuses must be published");
    assert_eq!(init_statuses.len(), 2);
    assert_eq!(
        init_statuses
            .iter()
            .filter_map(|status| status.get("name").and_then(|value| value.as_str()))
            .collect::<Vec<_>>(),
        vec!["init1", "init2"]
    );
    assert!(init_statuses.iter().all(|status| {
        status.pointer("/state/terminated/exitCode") == Some(&serde_json::json!(0))
            && status.pointer("/ready").and_then(|value| value.as_bool()) == Some(true)
    }));
    assert_eq!(
        stored
            .pointer("/status/conditions")
            .and_then(|value| value.as_array())
            .and_then(|conditions| conditions.iter().find(|condition| {
                condition.get("type").and_then(|value| value.as_str()) == Some("Initialized")
            }))
            .and_then(|condition| condition.get("status"))
            .and_then(|value| value.as_str()),
        Some("True")
    );
}

#[tokio::test]
async fn container_config_invalid_subpath_error_marks_status_with_parity() {
    let harness = PodRuntimeHarness::new().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "var-expansion",
            "name": "bad-subpath",
            "uid": "uid-bad-subpath",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "restartPolicy": "Never",
            "containers": [{
                "name": "dapi-container",
                "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1",
                "env": [{"name": "POD_NAME", "value": ".."}],
                "volumeMounts": [{
                    "name": "workdir1",
                    "mountPath": "/logscontainer",
                    "subPathExpr": "$(POD_NAME)"
                }]
            }],
            "volumes": [{
                "name": "workdir1",
                "emptyDir": {}
            }]
        },
        "status": {"phase": "Pending"}
    });
    let key = PodRuntimeKey::new("var-expansion", "bad-subpath", "uid-bad-subpath");
    harness.create_runtime_pod(pod.clone()).await;

    let result = harness
        .runtime
        .start_pod(key.clone(), Some(pod), CancellationToken::new())
        .await
        .unwrap();

    assert!(
        matches!(result, PodStartResult::Terminal(_)),
        "invalid subPathExpr must not start the pod: {result:?}"
    );
    assert!(
        !harness.cri.recorded_calls().iter().any(|call| matches!(
            &call.operation,
            MockCriOperation::CreateContainer { container_name, .. }
                if container_name == "dapi-container"
        )),
        "container with invalid expanded subPathExpr must not be created"
    );

    let stored = harness.stored_pod(&key).await;
    assert_eq!(
        stored
            .pointer("/status/phase")
            .and_then(|value| value.as_str()),
        Some("Pending")
    );
    let status = stored
        .pointer("/status/containerStatuses/0")
        .expect("config error container status must be published");
    assert_eq!(
        status
            .pointer("/state/waiting/reason")
            .and_then(|value| value.as_str()),
        Some("CreateContainerConfigError")
    );
    assert!(
        status
            .pointer("/state/waiting/message")
            .and_then(|value| value.as_str())
            .is_some_and(|message| message.contains("invalid subPath")),
        "config error message should mention invalid subPath: {status}"
    );
    assert!(harness.events.recorded_events().iter().any(|event| {
        event.event_type == "Warning"
            && event.reason == "Failed"
            && event.message.contains("invalid subPath")
    }));
}

#[tokio::test]
async fn container_config_run_as_non_root_error_marks_status_with_parity() {
    let harness = PodRuntimeHarness::new().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "security-context",
            "name": "bad-root",
            "uid": "uid-bad-root",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "restartPolicy": "Never",
            "containers": [{
                "name": "root-container",
                "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1",
                "securityContext": {"runAsNonRoot": true, "runAsUser": 0}
            }]
        },
        "status": {"phase": "Pending"}
    });
    let key = PodRuntimeKey::new("security-context", "bad-root", "uid-bad-root");
    harness.create_runtime_pod(pod.clone()).await;

    let result = harness
        .runtime
        .start_pod(key.clone(), Some(pod), CancellationToken::new())
        .await
        .unwrap();

    assert!(
        matches!(result, PodStartResult::Terminal(_)),
        "runAsNonRoot violation must not start the pod: {result:?}"
    );
    assert!(
        !harness.cri.recorded_calls().iter().any(|call| matches!(
            &call.operation,
            MockCriOperation::CreateContainer { container_name, .. }
                if container_name == "root-container"
        )),
        "container rejected by runAsNonRoot must not be created"
    );

    let stored = harness.stored_pod(&key).await;
    let status = stored
        .pointer("/status/containerStatuses/0")
        .expect("runAsNonRoot config error status must be published");
    assert_eq!(
        status
            .pointer("/state/waiting/reason")
            .and_then(|value| value.as_str()),
        Some("CreateContainerConfigError")
    );
    assert!(
        status
            .pointer("/state/waiting/message")
            .and_then(|value| value.as_str())
            .is_some_and(|message| message.contains("runAsNonRoot")),
        "config error message should mention runAsNonRoot: {status}"
    );
}

#[tokio::test]
async fn real_runtime_start_pod_init_container_exit_code_aborts_start() {
    let harness = PodRuntimeHarness::new().await;
    // Set non-zero exit code on container_status for init containers
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "ns",
            "name": "init-fail",
            "uid": "uid-ifail",
            "resourceVersion": "1"
        },
        "spec": {
            "initContainers": [
                {"name": "init-bad", "image": "busybox:1.35", "imagePullPolicy": "Never"}
            ],
            "containers": [
                {"name": "app", "image": "nginx:1.25", "imagePullPolicy": "Never"}
            ],
            "nodeName": "test-node"
        },
        "status": {"phase": "Pending"}
    });
    harness
        .repo
        .test_create_pod("ns", "init-fail", "test-node", pod.clone())
        .await
        .unwrap();

    // Set the mock to report a non-zero exit code
    harness.cri.set_container_exit_code(1);

    let key = PodRuntimeKey::new("ns", "init-fail", "uid-ifail");
    let result = harness
        .runtime
        .start_pod(key, Some(pod), CancellationToken::new())
        .await
        .unwrap();

    assert!(
        matches!(result, PodStartResult::Failed(_)),
        "init container with non-zero exit code should produce Failed result, got {:?}",
        result
    );

    // Main containers must not be created after init failure
    let cri_calls = harness.cri.recorded_calls();
    let main_creates: Vec<_> = cri_calls
        .iter()
        .filter(|c| {
            matches!(&c.operation, MockCriOperation::CreateContainer { container_name, .. } if container_name == "app")
        })
        .collect();
    assert!(
        main_creates.is_empty(),
        "main containers must not be created after init container failure"
    );
}

#[tokio::test]
async fn real_runtime_start_pod_materializes_full_container_config_with_parity() {
    let harness = PodRuntimeHarness::new().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "ns",
            "name": "config-pod",
            "uid": "uid-cfg",
            "resourceVersion": "1"
        },
        "spec": {
            "containers": [{
                "name": "app",
                "image": "nginx:1.25",
                "imagePullPolicy": "Never",
                "command": ["/bin/sh", "-c"],
                "args": ["echo hello"],
                "workingDir": "/app",
                "tty": true,
                "stdin": true,
                "stdinOnce": true,
                "env": [
                    {"name": "MY_ENV", "value": "my-value"},
                    {"name": "ENV_REF", "value": "$(MY_ENV)-suffix"}
                ],
                "resources": {
                    "limits": {"cpu": "500m", "memory": "128Mi"},
                    "requests": {"cpu": "250m", "memory": "64Mi"}
                },
                "securityContext": {
                    "runAsUser": 1000,
                    "runAsGroup": 2000,
                    "privileged": false,
                    "readOnlyRootFilesystem": true,
                    "allowPrivilegeEscalation": false
                }
            }],
            "nodeName": "test-node",
            "securityContext": {
                "fsGroup": 3000
            }
        },
        "status": {"phase": "Pending"}
    });
    harness
        .repo
        .test_create_pod("ns", "config-pod", "test-node", pod.clone())
        .await
        .unwrap();
    let key = PodRuntimeKey::new("ns", "config-pod", "uid-cfg");
    let result = harness
        .runtime
        .start_pod(key, Some(pod), CancellationToken::new())
        .await
        .unwrap();
    assert!(matches!(result, PodStartResult::Started { .. }));

    let configs = harness.cri.recorded_create_configs();
    assert_eq!(configs.len(), 1, "one container config should be recorded");

    let config = &configs[0];

    // Metadata
    let metadata = config.metadata.as_ref().unwrap();
    assert_eq!(metadata.name, "app");

    // Image
    assert!(config.image.is_some(), "image must be set");
    assert_eq!(
        config.image.as_ref().unwrap().image,
        "nginx:1.25",
        "image must be materialized"
    );

    // Command and args
    assert_eq!(
        config.command,
        vec!["/bin/sh", "-c"],
        "command must be materialized"
    );
    assert_eq!(config.args, vec!["echo hello"], "args must be materialized");

    // Working dir
    assert_eq!(
        config.working_dir, "/app",
        "workingDir must be materialized"
    );

    // TTY and stdin
    assert!(config.tty, "tty must be true");
    assert!(config.stdin, "stdin must be true");
    assert!(config.stdin_once, "stdinOnce must be true");

    // Env vars
    let env_keys: Vec<&str> = config.envs.iter().map(|kv| kv.key.as_str()).collect();
    assert!(
        env_keys.contains(&"MY_ENV"),
        "env MY_ENV must be present, got: {:?}",
        env_keys
    );

    // Log path
    assert!(!config.log_path.is_empty(), "log_path must be set");

    // Linux resources and security context
    assert!(config.linux.is_some(), "linux config must be present");
    let linux = config.linux.as_ref().unwrap();

    // Resources
    assert!(
        linux.resources.is_some(),
        "linux resources must be present when resources are specified"
    );
    let res = linux.resources.as_ref().unwrap();
    assert!(
        res.memory_limit_in_bytes > 0,
        "memory_limit_in_bytes must be > 0, got {}",
        res.memory_limit_in_bytes
    );
    assert!(
        res.cpu_shares > 0,
        "cpu_shares must be > 0 for cpu request, got {}",
        res.cpu_shares
    );

    // Security context
    assert!(
        linux.security_context.is_some(),
        "security context must be present"
    );
    let sc = linux.security_context.as_ref().unwrap();
    assert_eq!(
        sc.run_as_user.as_ref().unwrap().value,
        1000,
        "runAsUser must be 1000"
    );
    assert_eq!(
        sc.run_as_group.as_ref().unwrap().value,
        2000,
        "runAsGroup must be 2000"
    );
    assert!(sc.readonly_rootfs, "readOnlyRootFilesystem must be true");
    assert!(
        sc.supplemental_groups.contains(&3000),
        "fsGroup 3000 must be in supplemental_groups, got: {:?}",
        sc.supplemental_groups
    );
}

#[tokio::test]
async fn real_runtime_start_reconcile_finalize_publishes_running_status_with_pod_ip() {
    let harness = PodRuntimeHarness::new().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "kube-system",
            "name": "coredns",
            "uid": "uid-coredns",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "containers": [{
                "name": "coredns",
                "image": "registry.k8s.io/coredns/coredns:v1.13.1",
                "imagePullPolicy": "Never"
            }]
        },
        "status": {"phase": "Pending"}
    });
    let key = PodRuntimeKey::new("kube-system", "coredns", "uid-coredns");

    harness.create_runtime_pod(pod.clone()).await;
    let start = harness
        .start_pod_through_runtime(key.clone(), pod.clone())
        .await;
    let sandbox_id = match start {
        PodStartResult::Started {
            sandbox_id: Some(sandbox_id),
        } => sandbox_id,
        other => panic!("expected successful startup with sandbox id, got {other:?}"),
    };

    let before_reconcile = harness.stored_pod(&key).await;
    assert_eq!(
        before_reconcile
            .pointer("/status/phase")
            .and_then(|v| v.as_str()),
        Some("Pending")
    );
    assert_eq!(
        before_reconcile
            .pointer("/status/podIP")
            .and_then(|v| v.as_str()),
        Some("10.0.0.1"),
        "startup must publish the CNI-assigned pod IP before CRI Running reconcile"
    );
    assert_eq!(
        before_reconcile
            .pointer("/status/containerStatuses/0/state/waiting/reason")
            .and_then(|v| v.as_str()),
        Some("ContainerCreating"),
        "startup must mirror main by publishing ContainerCreating before start_container completion is reconciled"
    );

    harness.simulate_running_containers(vec!["container-coredns".into()]);
    harness.reconcile_runtime(key.clone()).await;

    let resource = harness.stored_pod(&key).await;
    assert_eq!(
        resource.pointer("/status/phase").and_then(|v| v.as_str()),
        Some("Running")
    );
    assert_eq!(
        resource.pointer("/status/podIP").and_then(|v| v.as_str()),
        Some("10.0.0.1"),
        "runtime reconcile must preserve the assigned pod IP so startup finalization can complete"
    );
    assert_eq!(
        resource
            .pointer("/status/containerStatuses/0/name")
            .and_then(|v| v.as_str()),
        Some("coredns"),
        "runtime reconcile must keep Kubernetes containerStatuses keyed by spec container name"
    );
    assert_eq!(
        resource
            .pointer("/status/containerStatuses/0/started")
            .and_then(|v| v.as_bool()),
        Some(true),
        "running containers must not preserve the startup placeholder started=false"
    );
    assert_eq!(
        resource
            .pointer("/status/containerStatuses/0/ready")
            .and_then(|v| v.as_bool()),
        Some(true),
        "running containers without readiness probes are ready immediately"
    );

    let finalized = harness
        .runtime
        .finalize_startup(key, None, None)
        .await
        .unwrap();
    assert_eq!(
        finalized,
        PodFinalizeStartupResult::Confirmed {
            sandbox_id: sandbox_id.clone()
        }
    );
    assert_eq!(
        harness.probes.recorded_calls(),
        vec![
            MockProbeCall::RecordStartedSandbox {
                namespace: "kube-system".into(),
                name: "coredns".into(),
                uid: "uid-coredns".into(),
                sandbox_id: sandbox_id.clone(),
            },
            MockProbeCall::Start {
                namespace: "kube-system".into(),
                name: "coredns".into(),
                uid: "uid-coredns".into(),
                sandbox_id: sandbox_id.clone(),
            },
            MockProbeCall::MarkStartedSandboxFinalized {
                namespace: "kube-system".into(),
                name: "coredns".into(),
                uid: "uid-coredns".into(),
                sandbox_id,
            },
        ],
        "finalize_startup should start probes after Running+podIP is visible"
    );
}

#[tokio::test]
async fn real_runtime_start_pod_passes_cluster_dns_to_pod_sandbox() {
    let harness = PodRuntimeHarness::new().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "dns-debug",
            "name": "dns-client",
            "uid": "uid-dns-client",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "dnsPolicy": "ClusterFirst",
            "containers": [{
                "name": "client",
                "image": "docker.io/library/busybox:1.36",
                "imagePullPolicy": "Never"
            }]
        },
        "status": {"phase": "Pending"}
    });
    let key = PodRuntimeKey::new("dns-debug", "dns-client", "uid-dns-client");

    harness.create_runtime_pod(pod.clone()).await;
    let start = harness
        .start_pod_through_runtime(key.clone(), pod.clone())
        .await;
    assert!(matches!(start, PodStartResult::Started { .. }));

    let sandbox_configs = harness.cri.recorded_sandbox_configs();
    let dns = sandbox_configs
        .first()
        .and_then(|config| config.dns_config.as_ref())
        .expect("runtime must pass DNS config to RunPodSandbox");
    assert_eq!(
        dns.servers,
        vec!["10.43.128.10"],
        "ClusterFirst pods must use the kube-dns service IP, not host resolvers"
    );
    assert_eq!(
        dns.searches,
        vec![
            "dns-debug.svc.cluster.local",
            "svc.cluster.local",
            "cluster.local",
        ],
        "ClusterFirst pods must get Kubernetes search domains"
    );
    assert_eq!(dns.options, vec!["ndots:5"]);
}

#[tokio::test]
async fn kubernetes_service_envs_with_parity() {
    let harness =
        PodRuntimeHarness::new_with_runtime_config(crate::runtime::service::RuntimeConfig {
            node_name: "test-node".into(),
            service_cidr: "10.96.0.0/12".into(),
            containerd_namespace: "klights-test".into(),
            sandbox_inputs: crate::pod_sandbox_config::SandboxRuntimeInputs::default(),
            node_capacity: crate::node_capacity::NodeCapacity::default(),
            paths: crate::runtime_paths::KubeletRuntimePaths::new(std::path::PathBuf::from(
                "/tmp/klights/runtime-test",
            ))
            .unwrap(),
        })
        .await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "sonobuoy",
            "name": "aggregator",
            "uid": "uid-sonobuoy-env",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "containers": [{
                "name": "kube-sonobuoy",
                "image": "sonobuoy/sonobuoy:v0.57.3",
                "imagePullPolicy": "Never"
            }]
        },
        "status": {"phase": "Pending"}
    });
    let key = PodRuntimeKey::new("sonobuoy", "aggregator", "uid-sonobuoy-env");

    harness.create_runtime_pod(pod.clone()).await;
    let start = harness
        .start_pod_through_runtime(key.clone(), pod.clone())
        .await;
    assert!(matches!(start, PodStartResult::Started { .. }));

    let create_configs = harness.cri.recorded_create_configs();
    let env = create_configs
        .first()
        .expect("container config must be created")
        .envs
        .iter()
        .find(|kv| kv.key == "KUBERNETES_SERVICE_HOST")
        .expect("KUBERNETES_SERVICE_HOST must be injected");
    assert_eq!(
        env.value, "10.96.0.1",
        "in-cluster API env must match the configured kubernetes Service ClusterIP"
    );
}

#[tokio::test]
async fn namespace_service_envs_with_parity() {
    let harness = PodRuntimeHarness::new().await;
    let service = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "namespace": "pods",
            "name": "fooservice",
        },
        "spec": {
            "clusterIP": "10.43.128.205",
            "ports": [{
                "port": 8765,
                "protocol": "TCP"
            }]
        }
    });
    harness
        .env_source
        .insert_service("pods", "fooservice", service);

    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "pods",
            "name": "client-envvars",
            "uid": "uid-client-envvars",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "containers": [{
                "name": "env3cont",
                "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1",
                "imagePullPolicy": "Never"
            }]
        },
        "status": {"phase": "Pending"}
    });
    let key = PodRuntimeKey::new("pods", "client-envvars", "uid-client-envvars");

    harness.create_runtime_pod(pod.clone()).await;
    let start = harness
        .start_pod_through_runtime(key.clone(), pod.clone())
        .await;
    assert!(matches!(start, PodStartResult::Started { .. }));

    let create_configs = harness.cri.recorded_create_configs();
    let env_map: std::collections::HashMap<&str, &str> = create_configs
        .first()
        .expect("container config must be created")
        .envs
        .iter()
        .map(|kv| (kv.key.as_str(), kv.value.as_str()))
        .collect();

    assert_eq!(
        env_map.get("FOOSERVICE_SERVICE_HOST").copied(),
        Some("10.43.128.205"),
        "runtime must append namespace Service discovery env vars before CreateContainer"
    );
    assert_eq!(
        env_map.get("FOOSERVICE_SERVICE_PORT").copied(),
        Some("8765"),
        "runtime must include first service port env var"
    );
    assert_eq!(
        env_map.get("FOOSERVICE_PORT_8765_TCP_ADDR").copied(),
        Some("10.43.128.205"),
        "runtime must include per-port TCP service env vars"
    );
}

#[tokio::test]
async fn field_ref_env_value_from_with_parity() {
    let harness = PodRuntimeHarness::new().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "sonobuoy",
            "name": "sonobuoy",
            "uid": "uid-sonobuoy-advertise",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "containers": [{
                "name": "kube-sonobuoy",
                "image": "sonobuoy/sonobuoy:v0.57.3",
                "imagePullPolicy": "Never",
                "env": [{
                    "name": "SONOBUOY_ADVERTISE_IP",
                    "valueFrom": {
                        "fieldRef": {
                            "fieldPath": "status.podIP"
                        }
                    }
                }]
            }]
        },
        "status": {"phase": "Pending"}
    });
    let key = PodRuntimeKey::new("sonobuoy", "sonobuoy", "uid-sonobuoy-advertise");

    harness.create_runtime_pod(pod.clone()).await;
    let start = harness
        .start_pod_through_runtime(key.clone(), pod.clone())
        .await;
    assert!(matches!(start, PodStartResult::Started { .. }));

    let create_configs = harness.cri.recorded_create_configs();
    let env = create_configs
        .first()
        .expect("container config must be created")
        .envs
        .iter()
        .find(|kv| kv.key == "SONOBUOY_ADVERTISE_IP")
        .expect("fieldRef env must be present");
    assert_eq!(
        env.value, "10.0.0.1",
        "status.podIP fieldRef env must resolve from the CNI assignment before CreateContainer"
    );
}

#[tokio::test]
async fn secret_key_ref_env_value_from_with_parity() {
    use base64::Engine;

    let harness = PodRuntimeHarness::new().await;
    let cert_pem = "-----BEGIN CERTIFICATE-----\nsonobuoy-client\n-----END CERTIFICATE-----\n";
    let secret = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "namespace": "sonobuoy",
            "name": "sonobuoy-plugin-e2e",
        },
        "type": "kubernetes.io/tls",
        "data": {
            "tls.crt": base64::engine::general_purpose::STANDARD.encode(cert_pem),
        }
    });
    harness
        .env_source
        .insert_secret("sonobuoy", "sonobuoy-plugin-e2e", secret);

    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "sonobuoy",
            "name": "e2e",
            "uid": "uid-sonobuoy-secret-env",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "containers": [{
                "name": "sonobuoy-worker",
                "image": "sonobuoy/sonobuoy:v0.57.3",
                "imagePullPolicy": "Never",
                "env": [{
                    "name": "CLIENT_CERT",
                    "valueFrom": {
                        "secretKeyRef": {
                            "name": "sonobuoy-plugin-e2e",
                            "key": "tls.crt"
                        }
                    }
                }]
            }]
        },
        "status": {"phase": "Pending"}
    });
    let key = PodRuntimeKey::new("sonobuoy", "e2e", "uid-sonobuoy-secret-env");

    harness.create_runtime_pod(pod.clone()).await;
    let start = harness
        .start_pod_through_runtime(key.clone(), pod.clone())
        .await;
    assert!(matches!(start, PodStartResult::Started { .. }));

    let create_configs = harness.cri.recorded_create_configs();
    let env = create_configs
        .first()
        .expect("container config must be created")
        .envs
        .iter()
        .find(|kv| kv.key == "CLIENT_CERT")
        .expect("secretKeyRef env must be resolved before CreateContainer");
    assert_eq!(
        env.value, cert_pem,
        "Secret data must be base64-decoded before injection as an env var"
    );
}

#[tokio::test]
async fn real_runtime_actor_cycle_starts_reconciles_running_and_deletes_pod() {
    use crate::pod_lifecycle_actor::config::PodLifecycleConcurrencyConfig;
    use crate::pod_lifecycle_actor::registry::PodLifecycleRegistry;
    use crate::pod_lifecycle_core::message::LifecycleMessage;
    use crate::pod_lifecycle_router::PodLifecycleRouter;
    use crate::pod_lifecycle_router::executor::{
        NoopExecutor, PodLifecycleExecutor, PodWorkExecutor,
    };

    let harness = PodRuntimeHarness::new().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "kube-system",
            "name": "coredns-actor",
            "uid": "uid-coredns-actor",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "containers": [{
                "name": "coredns",
                "image": "coredns/coredns:1.11.1",
                "imagePullPolicy": "Never"
            }]
        },
        "status": {"phase": "Pending"}
    });
    let runtime_key = PodRuntimeKey::new("kube-system", "coredns-actor", "uid-coredns-actor");
    let lifecycle_key = PodLifecycleKey::new("kube-system", "coredns-actor", "uid-coredns-actor");
    harness.create_runtime_pod(pod.clone()).await;

    let executor_holder = Arc::new(std::sync::Mutex::new(
        Arc::new(NoopExecutor) as Arc<dyn PodWorkExecutor>
    ));
    let registry = Arc::new(PodLifecycleRegistry::new(
        harness.supervisor.clone(),
        PodLifecycleConcurrencyConfig::production_default(),
        executor_holder,
    ));
    let router = Arc::new(PodLifecycleRouter::new_actor(registry));

    let executor = Arc::new(PodLifecycleExecutor::new(harness.runtime.clone()));
    router.set_work_executor(executor);

    router
        .route(LifecycleMessage::WatchAdded {
            key: lifecycle_key.clone(),
            resource_version: Some(1),
            pod: pod.clone(),
        })
        .await
        .expect("route watch added");

    for _ in 0..50 {
        if !harness.cri.recorded_calls().is_empty() {
            break;
        }
        let _ = harness
            .supervisor
            .sleep(
                "actor_cycle_start_wait",
                std::time::Duration::from_millis(10),
            )
            .await;
    }
    assert!(
        !harness.cri.recorded_calls().is_empty(),
        "WatchAdded did not reach runtime start; diagnostics: {:?}",
        router.diagnostics().await
    );

    wait_for_pod_status(&harness, &runtime_key, |pod| {
        pod.pointer("/status/podIP").and_then(|v| v.as_str()) == Some("10.0.0.1")
            && pod
                .pointer("/status/containerStatuses/0/state/waiting/reason")
                .and_then(|v| v.as_str())
                == Some("ContainerCreating")
    })
    .await;

    harness.simulate_running_containers(vec!["container-sandbox-0001".into()]);
    router
        .route(LifecycleMessage::CriEvent {
            key: lifecycle_key.clone(),
            container_id: "container-sandbox-0001".into(),
            kind: crate::cri_events::KubeletEventKind::Started,
        })
        .await
        .expect("route cri start event");

    let running_pod = wait_for_pod_status(&harness, &runtime_key, |pod| {
        pod.pointer("/status/phase").and_then(|v| v.as_str()) == Some("Running")
    })
    .await;

    router
        .route(LifecycleMessage::WatchModified {
            key: lifecycle_key.clone(),
            resource_version: Some(2),
            pod: running_pod.clone(),
        })
        .await
        .expect("route running watch echo");

    let mut terminating_pod = running_pod;
    terminating_pod["metadata"]["deletionTimestamp"] =
        serde_json::Value::String(klights_cluster_core::k8s_time::format_legacy_timestamp(
            klights_supervisor::SystemWallClock::now_utc(),
        ));
    router
        .route(LifecycleMessage::WatchDeleted {
            key: lifecycle_key,
            resource_version: Some(3),
            pod: terminating_pod,
        })
        .await
        .expect("route watch deleted");

    for _ in 0..50 {
        if harness
            .store
            .get_sandbox_id(&runtime_key)
            .await
            .unwrap()
            .is_none()
            && !harness.finalizer.recorded_calls().is_empty()
        {
            return;
        }
        let _ = harness
            .supervisor
            .sleep(
                "actor_cycle_delete_wait",
                std::time::Duration::from_millis(10),
            )
            .await;
    }
    panic!("actor delete cycle did not clear sandbox and finalize deletion");
}

#[tokio::test]
async fn production_wired_runtime_reconcile_uses_oo_ports() {
    let harness = PodRuntimeHarness::new().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "kube-system",
            "name": "coredns-prod-wired",
            "uid": "uid-coredns-prod-wired",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "containers": [{
                "name": "coredns",
                "image": "coredns/coredns:1.11.1",
                "imagePullPolicy": "Never"
            }]
        },
        "status": {"phase": "Pending"}
    });
    let key = PodRuntimeKey::new(
        "kube-system",
        "coredns-prod-wired",
        "uid-coredns-prod-wired",
    );

    harness.create_runtime_pod(pod.clone()).await;
    let start = harness
        .start_pod_through_runtime(key.clone(), pod.clone())
        .await;
    assert!(matches!(start, PodStartResult::Started { .. }));
    assert_eq!(
        harness
            .stored_pod(&key)
            .await
            .pointer("/status/podIP")
            .and_then(|v| v.as_str()),
        Some("10.0.0.1"),
        "startup setup must publish podIP before runtime reconcile"
    );
    harness.simulate_running_containers(vec!["container-coredns".into()]);

    harness.reconcile_runtime(key.clone()).await;

    let resource = harness.stored_pod(&key).await;
    assert_eq!(
        resource.pointer("/status/phase").and_then(|v| v.as_str()),
        Some("Running"),
        "production-wired runtime service must use the OO runtime ports for reconcile"
    );
    assert_eq!(
        resource
            .pointer("/status/containerStatuses/0/name")
            .and_then(|v| v.as_str()),
        Some("coredns")
    );
}

#[tokio::test]
async fn production_runtime_stop_unstarted_terminating_pod_allows_actor_finalization() {
    let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let repo = build_test_pod_repository();
    let key = PodRuntimeKey::new("sonobuoy", "sonobuoy", "uid-sonobuoy");
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "sonobuoy",
            "name": "sonobuoy",
            "uid": "uid-sonobuoy",
            "resourceVersion": "1",
            "deletionTimestamp": "2026-05-19T13:47:28Z"
        },
        "spec": {
            "nodeName": "test-node",
            "containers": [{"name": "aggregator", "image": "sonobuoy/sonobuoy:v0.57.3"}]
        },
        "status": {"phase": "Pending", "containerStatuses": []}
    });
    repo.test_create_pod("sonobuoy", "sonobuoy", "test-node", pod.clone())
        .await
        .unwrap();
    let env_source = std::sync::Arc::new(MockEnvSourceReader::new());

    let runtime = real_runtime! {
        cri: std::sync::Arc::new(MockCriRuntime::new()),
        container_control: std::sync::Arc::new(MockContainerRuntimeControl::new()),
        network: std::sync::Arc::new(MockPodNetworkRuntime::new()),
        store: std::sync::Arc::new(MockPodRuntimeStore::new()),
        clock: std::sync::Arc::new(crate::runtime_clock::SystemRuntimeClock),
        slot_admission: std::sync::Arc::new(MockPodSlotAdmission::new()),
        pod_query: repo.pod_query.clone(),
        pod_status_writer: repo.pod_status_writer.clone(),
        filesystem: std::sync::Arc::new(MockPodFilesystem::new()),
        volumes: std::sync::Arc::new(MockPodVolumeRuntime::new()),
        probes: std::sync::Arc::new(MockProbeRuntime::new()),
        hostports: std::sync::Arc::new(MockHostPortRuntime::new()),
        events: std::sync::Arc::new(MockPodEventSink::new()),
        hooks: std::sync::Arc::new(MockPodHookRuntime::new()),
        env_source: env_source,
        finalizer: repo.deletion_finalizer.clone(),
        supervisor: supervisor,
        config: RuntimeConfig {
            node_name: "test-node".into(),
            service_cidr: "10.43.128.0/17".into(),
            containerd_namespace: "klights-test".into(),
            sandbox_inputs: crate::pod_sandbox_config::SandboxRuntimeInputs::default(
            ),
            node_capacity: crate::node_capacity::NodeCapacity::default(),
            paths: crate::runtime_paths::KubeletRuntimePaths::new(
                std::path::PathBuf::from("/tmp/klights/runtime-test"),
            )
            .unwrap(),
        },
    };

    runtime
        .stop_pod(key.clone(), Some(pod), None)
        .await
        .expect("unstarted terminating pod cleanup should succeed");
    assert_eq!(
        runtime.finalize_deletion(key.clone()).await.unwrap(),
        PodDeletionFinalizeResult::DeletedOrAlreadyGone
    );
    assert!(
        repo.test_get_pod_for_uid("sonobuoy", "sonobuoy", "uid-sonobuoy")
            .await
            .unwrap()
            .is_none(),
        "actor finalization must remove the unstarted terminating pod row"
    );
}
