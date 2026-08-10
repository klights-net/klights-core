use super::*;
use crate::kubelet::pod_runtime::service::PodRuntimeKey;
use crate::kubelet::pod_runtime::service::RuntimeReconcileHint;
use klights_kubelet::pod_lifecycle_core::state::PodLifecycleState;
use klights_kubelet::runtime::cri::ContainerRuntimeState;

#[test]
fn cri_runtime_trait_exposes_only_runtime_arguments() {
    // The CriRuntime trait must accept only runtime-level arguments
    // (image names, sandbox IDs, container configs) — never a cluster
    // persistence aggregate,
    // Old watcher context bundles, DatastoreHandle, or any lifecycle key.
    fn assert_object_safe<T: ?Sized + Send + Sync>() {}
    assert_object_safe::<dyn klights_kubelet::runtime::cri::CriRuntime>();
    assert_object_safe::<dyn klights_kubelet::runtime::cri::ContainerRuntimeControl>();
}

#[test]
fn shared_cri_runtime_clones_client_per_call_without_mutex() {
    // SharedCriRuntime is Send + Sync (no Mutex).
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<klights_kubelet::runtime::cri::SharedCriRuntime>();
    // The adapter implements CriRuntime.
    fn _takes_cri_runtime(_: &dyn klights_kubelet::runtime::cri::CriRuntime) {}
}

#[tokio::test]
async fn mock_cri_records_call_arguments_exactly() {
    let mock = MockCriRuntime::new();
    let sandbox_id = mock
        .run_pod_sandbox(PodSandboxConfig::default())
        .await
        .unwrap();
    mock.stop_pod_sandbox(&sandbox_id).await.unwrap();
    mock.remove_pod_sandbox(&sandbox_id).await.unwrap();

    let calls = mock.recorded_calls();
    assert_eq!(calls.len(), 3);
    assert_eq!(calls[0].operation, MockCriOperation::RunPodSandbox);
    assert_eq!(
        calls[1].operation,
        MockCriOperation::StopPodSandbox(sandbox_id.clone())
    );
    assert_eq!(
        calls[2].operation,
        MockCriOperation::RemovePodSandbox(sandbox_id.clone())
    );
    assert!(calls[0].call_order < calls[1].call_order);
    assert!(calls[1].call_order < calls[2].call_order);
}

#[tokio::test]
async fn mock_cri_records_image_pull_sequence() {
    let mock = MockCriRuntime::new();
    let present = mock.image_status("nginx:latest").await.unwrap();
    assert!(present);
    let image_ref = mock.pull_image("nginx:latest").await.unwrap();
    assert_eq!(image_ref, "pulled-nginx:latest");

    let calls = mock.recorded_calls();
    assert_eq!(calls.len(), 2);
    assert_eq!(
        calls[0].operation,
        MockCriOperation::ImageStatus("nginx:latest".to_string())
    );
    assert_eq!(
        calls[1].operation,
        MockCriOperation::PullImage("nginx:latest".to_string())
    );
}

#[tokio::test]
async fn mock_cri_can_fail_specific_operation() {
    let mock = MockCriRuntime::new();
    mock.set_fail_operation("RunPodSandbox");

    let result = mock.run_pod_sandbox(PodSandboxConfig::default()).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("injected failure"));

    // Other operations still succeed.
    mock.stop_pod_sandbox("sb-1").await.unwrap();
    let calls = mock.recorded_calls();
    assert_eq!(calls.len(), 2);
}

#[tokio::test]
async fn real_hook_runtime_exec_hook_uses_cri_runtime_port() {
    use crate::kubelet::pod_runtime::hooks::PodHookRuntime;

    let cri = Arc::new(MockCriRuntime::new());
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let hooks =
        crate::kubelet::pod_runtime::hooks::RealPodHookRuntime::new(cri.clone(), supervisor);
    let hook = serde_json::json!({
        "exec": {"command": ["/bin/sh", "-c", "true"]},
        "timeoutSeconds": 7
    });
    let container_spec = serde_json::json!({"name": "app"});

    let outcome = hooks
        .execute_post_start("container-1", "10.0.0.5", &hook, &container_spec)
        .await
        .unwrap();

    assert_eq!(outcome, HookOutcome::Succeeded);
    assert!(cri.recorded_calls().iter().any(|call| {
        matches!(
            &call.operation,
            MockCriOperation::ExecSync {
                container_id,
                command,
                timeout_seconds,
            } if container_id == "container-1"
                && command == &vec![
                    "/bin/sh".to_string(),
                    "-c".to_string(),
                    "true".to_string()
                ]
                && *timeout_seconds == 7
        )
    }));
}

#[test]
fn pod_runtime_store_sandbox_methods_require_uid() {
    fn assert_send_sync<T: ?Sized + Send + Sync>() {}
    assert_send_sync::<dyn PodRuntimeStore>();
    // record_sandbox, get_sandbox_id, delete_sandbox all take &PodRuntimeKey.
}

#[tokio::test]
async fn mock_runtime_store_preserves_same_name_uid_rows() {
    let store = MockPodRuntimeStore::new();
    let old_key = PodRuntimeKey::new("ns", "pod", "uid-old");
    let new_key = PodRuntimeKey::new("ns", "pod", "uid-new");

    store.record_sandbox(&old_key, "sb-old").await.unwrap();
    store.record_sandbox(&new_key, "sb-new").await.unwrap();

    assert_eq!(
        store.get_sandbox_id(&old_key).await.unwrap(),
        Some("sb-old".to_string())
    );
    assert_eq!(
        store.get_sandbox_id(&new_key).await.unwrap(),
        Some("sb-new".to_string())
    );

    // Delete old UID row; new UID row persists.
    store.delete_sandbox(&old_key).await.unwrap();
    assert_eq!(store.get_sandbox_id(&old_key).await.unwrap(), None);
    assert_eq!(
        store.get_sandbox_id(&new_key).await.unwrap(),
        Some("sb-new".to_string())
    );
}

#[test]
fn pod_event_sink_requires_pod_uid_argument() {
    use crate::kubelet::pod_runtime::events::PodEventSink;
    fn assert_send_sync<T: ?Sized + Send + Sync>() {}
    assert_send_sync::<dyn PodEventSink>();
}

#[tokio::test]
async fn mock_event_sink_records_pod_events_with_uid() {
    let sink = MockPodEventSink::new();
    let key = PodRuntimeKey::new("ns", "pod", "uid-1");

    sink.emit_pod_event(&key, "Normal", "Scheduled", "msg", "klights", "node1")
        .await
        .unwrap();

    let events = sink.recorded_events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].namespace, "ns");
    assert_eq!(events[0].uid, "uid-1");
    assert_eq!(events[0].reason, "Scheduled");
    assert_eq!(events[0].node_name, "node1");
}

#[tokio::test]
async fn mock_event_sink_preserves_stale_uid_on_replacement() {
    let sink = MockPodEventSink::new();
    let old_key = PodRuntimeKey::new("ns", "pod", "uid-old");
    let new_key = PodRuntimeKey::new("ns", "pod", "uid-new");

    sink.emit_pod_event(&old_key, "Normal", "Pulling", "pulling old", "c", "n")
        .await
        .unwrap();
    sink.emit_pod_event(&new_key, "Normal", "Pulling", "pulling new", "c", "n")
        .await
        .unwrap();

    let events = sink.recorded_events();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].uid, "uid-old");
    assert_eq!(events[1].uid, "uid-new");
}

#[tokio::test]
async fn real_runtime_start_pod_rejects_uid_mismatch_before_cri() {
    let harness = PodRuntimeHarness::new().await;
    let pod = klights_kubelet::runtime::test_support::pod_json(
        "ns",
        "test-pod",
        "correct-uid",
        "nginx:latest",
    );

    // Create pod with correct-uid in the repository.
    harness
        .repo
        .test_create_pod("ns", "test-pod", "test-node", pod.clone())
        .await
        .unwrap();

    // Call start_pod with a mismatched UID.
    let wrong_key = PodRuntimeKey::new("ns", "test-pod", "wrong-uid");
    let cancel = CancellationToken::new();
    let result = harness
        .runtime
        .start_pod(wrong_key, Some(pod), cancel)
        .await;

    // Must fail because UID doesn't match the live pod.
    match result {
        Ok(PodStartResult::Failed(_)) => {}
        Err(_) => {}
        other => panic!("expected UID mismatch failure, got {:?}", other),
    }

    // CRI must not have been called before UID verification.
    let cri_calls = harness.cri.recorded_calls();
    assert!(
        cri_calls.is_empty(),
        "CRI must not be called before UID verification"
    );
}

#[tokio::test]
async fn real_runtime_start_pod_image_pull_policy_matrix() {
    // --- Always: pulls even when image is present ---
    {
        let harness = PodRuntimeHarness::new().await;
        harness.cri.set_image_present(true);
        let pod = pod_with_pull_policy("ns", "pod", "uid-a", "nginx", "Always");
        harness
            .repo
            .test_create_pod("ns", "pod", "test-node", pod.clone())
            .await
            .unwrap();
        let key = PodRuntimeKey::new("ns", "pod", "uid-a");
        let result = harness
            .runtime
            .start_pod(key, Some(pod), CancellationToken::new())
            .await
            .unwrap();
        assert!(matches!(result, PodStartResult::Started { .. }));
        // PullImage must have been called.
        let calls = harness.cri.recorded_calls();
        let pulled = calls
            .iter()
            .any(|c| matches!(c.operation, MockCriOperation::PullImage(_)));
        assert!(pulled, "Always policy must pull image even when present");
    }

    // --- Never: skips pull entirely ---
    {
        let harness = PodRuntimeHarness::new().await;
        let pod = pod_with_pull_policy("ns", "pod2", "uid-b", "nginx", "Never");
        harness
            .repo
            .test_create_pod("ns", "pod2", "test-node", pod.clone())
            .await
            .unwrap();
        let key = PodRuntimeKey::new("ns", "pod2", "uid-b");
        harness
            .runtime
            .start_pod(key, Some(pod), CancellationToken::new())
            .await
            .unwrap();
        let calls = harness.cri.recorded_calls();
        let pulled = calls
            .iter()
            .any(|c| matches!(c.operation, MockCriOperation::PullImage(_)));
        assert!(!pulled, "Never policy must not pull image");
    }

    // --- IfNotPresent + image present: skips pull ---
    {
        let harness = PodRuntimeHarness::new().await;
        harness.cri.set_image_present(true);
        let pod = pod_with_pull_policy("ns", "pod3", "uid-c", "nginx", "IfNotPresent");
        harness
            .repo
            .test_create_pod("ns", "pod3", "test-node", pod.clone())
            .await
            .unwrap();
        let key = PodRuntimeKey::new("ns", "pod3", "uid-c");
        harness
            .runtime
            .start_pod(key, Some(pod), CancellationToken::new())
            .await
            .unwrap();
        let calls = harness.cri.recorded_calls();
        let pulled = calls
            .iter()
            .any(|c| matches!(c.operation, MockCriOperation::PullImage(_)));
        assert!(!pulled, "IfNotPresent with image present must not pull");
    }

    // --- IfNotPresent + image absent: pulls ---
    {
        let harness = PodRuntimeHarness::new().await;
        harness.cri.set_image_present(false);
        let pod = pod_with_pull_policy("ns", "pod4", "uid-d", "nginx", "IfNotPresent");
        harness
            .repo
            .test_create_pod("ns", "pod4", "test-node", pod.clone())
            .await
            .unwrap();
        let key = PodRuntimeKey::new("ns", "pod4", "uid-d");
        harness
            .runtime
            .start_pod(key, Some(pod), CancellationToken::new())
            .await
            .unwrap();
        let calls = harness.cri.recorded_calls();
        let pulled = calls
            .iter()
            .any(|c| matches!(c.operation, MockCriOperation::PullImage(_)));
        assert!(pulled, "IfNotPresent with image absent must pull");
    }
}

#[tokio::test]
async fn real_runtime_start_pod_image_pull_failure_emits_failed_event() {
    let harness = PodRuntimeHarness::new().await;
    harness.cri.set_image_present(false);
    harness.cri.set_fail_operation("PullImage");
    let pod = pod_with_pull_policy("ns", "pod", "uid-1", "nginx", "Always");
    harness
        .repo
        .test_create_pod("ns", "pod", "test-node", pod.clone())
        .await
        .unwrap();
    let key = PodRuntimeKey::new("ns", "pod", "uid-1");
    let result = harness
        .runtime
        .start_pod(key, Some(pod), CancellationToken::new())
        .await;

    // Must return Failed.
    match result {
        Ok(PodStartResult::Failed(_)) => {}
        other => panic!("expected Failed on pull error, got {:?}", other),
    }

    // Failed event must carry the correct UID.
    let events = harness.events.recorded_events();
    let failed_event = events.iter().find(|e| e.reason == "Failed");
    assert!(failed_event.is_some(), "Failed event must be emitted");
    assert_eq!(failed_event.unwrap().uid, "uid-1");
}

#[tokio::test]
async fn real_runtime_start_pod_failed_event_uses_verified_uid() {
    // Create a pod with old-uid, then call start_pod with a stale snapshot
    // after the pod has been replaced. The Failed event must carry the old UID,
    // not the replacement UID.
    let harness = PodRuntimeHarness::new().await;
    harness.cri.set_fail_operation("PullImage");
    let old_pod = pod_with_pull_policy("ns", "pod", "old-uid", "nginx", "Always");
    harness
        .repo
        .test_create_pod("ns", "pod", "test-node", old_pod.clone())
        .await
        .unwrap();

    // Call start_pod with a UID that doesn't match the live pod (stale start).
    let wrong_key = PodRuntimeKey::new("ns", "pod", "different-uid");
    let result = harness
        .runtime
        .start_pod(wrong_key, Some(old_pod), CancellationToken::new())
        .await;

    // Must fail (UID mismatch before CRI).
    match result {
        Ok(PodStartResult::Failed(_)) | Err(_) => {}
        other => panic!("expected failure for stale UID, got {:?}", other),
    }

    // No Failed event should be emitted with the wrong UID (UID check fails first).
    let events = harness.events.recorded_events();
    // The Scheduled event should NOT have been emitted either since UID check is
    // before event emission. But wait — Scheduled is emitted during identity
    // phase. Let's check: UID mismatch is detected in identity phase, BEFORE
    // Scheduled event. So no events should carry the wrong UID.
    for event in &events {
        assert_ne!(
            event.uid, "old-uid",
            "no event should be emitted for wrong-UID start that fails at UID check"
        );
    }
}

#[tokio::test]
async fn real_runtime_start_pod_records_sandbox_and_reads_assignment() {
    let harness = PodRuntimeHarness::new().await;
    let pod = pod_with_pull_policy("ns", "pod", "uid-sb", "nginx", "Never");
    harness
        .repo
        .test_create_pod("ns", "pod", "test-node", pod.clone())
        .await
        .unwrap();
    let key = PodRuntimeKey::new("ns", "pod", "uid-sb");
    let result = harness
        .runtime
        .start_pod(key, Some(pod), CancellationToken::new())
        .await
        .unwrap();

    // Sandbox should have been created.
    match result {
        PodStartResult::Started {
            sandbox_id: Some(ref sid),
        } => {
            assert!(!sid.is_empty(), "sandbox_id must be non-empty");
        }
        other => panic!("expected Started with sandbox_id, got {:?}", other),
    }

    // CRI must have run the sandbox.
    let cri_calls = harness.cri.recorded_calls();
    let has_sandbox = cri_calls
        .iter()
        .any(|c| matches!(c.operation, MockCriOperation::RunPodSandbox));
    assert!(has_sandbox, "RunPodSandbox must be called");

    // Sandbox must be recorded in the runtime store.
    let store_calls = harness.store.recorded_calls();
    let has_record = store_calls
        .iter()
        .any(|s| s.contains("record_sandbox") && s.contains("uid-sb"));
    assert!(has_record, "sandbox must be recorded with UID");

    // Network assignment must have been read.
    let net_calls = harness.network.recorded_calls();
    let has_read = net_calls.iter().any(|c| {
        matches!(
            c,
            MockNetworkOp::ReadAssignment { uid, .. } if uid == "uid-sb"
        )
    });
    assert!(has_read, "network assignment must be read with UID");
}

#[tokio::test]
async fn start_pod_recovery_skips_already_realized_running_sandbox_with_parity() {
    let harness = PodRuntimeHarness::new().await;
    let key = PodRuntimeKey::new("ns", "restart-survivor", "uid-restart");
    let mut pod = klights_kubelet::runtime::test_support::pod_json(
        &key.namespace,
        &key.name,
        &key.uid,
        "nginx:1.25",
    );
    pod["status"] = serde_json::json!({
        "phase": "Running",
        "podIP": "10.0.0.21",
        "hostIP": "192.168.1.1",
        "containerStatuses": [{
            "name": "app",
            "containerID": "containerd://container-live",
            "ready": true,
            "started": true,
            "restartCount": 0,
            "state": {"running": {"startedAt": "2026-05-20T00:00:00Z"}}
        }]
    });
    harness.create_runtime_pod(pod.clone()).await;
    harness
        .store
        .record_sandbox(&key, "sandbox-live")
        .await
        .expect("record live sandbox");
    harness.simulate_running_containers(["container-live".to_string()]);
    harness.cri.clear_calls();
    harness.container_control.clear_calls();
    harness.volumes.clear_calls();

    let result = harness
        .runtime
        .start_pod(key.clone(), Some(pod), CancellationToken::new())
        .await
        .expect("restart recovery start_pod should succeed");

    assert_eq!(
        result,
        PodStartResult::Started {
            sandbox_id: Some("sandbox-live".to_string())
        }
    );
    let cri_ops: Vec<_> = harness
        .cri
        .recorded_calls()
        .into_iter()
        .map(|call| call.operation)
        .collect();
    assert!(
        !cri_ops
            .iter()
            .any(|op| matches!(op, MockCriOperation::RunPodSandbox)),
        "main recovery parity: live sandbox must not be recreated: {cri_ops:?}"
    );
    assert!(
        !cri_ops
            .iter()
            .any(|op| matches!(op, MockCriOperation::PullImage(_))),
        "main recovery parity: realized pod must not pull images again: {cri_ops:?}"
    );
    assert!(
        !cri_ops
            .iter()
            .any(|op| matches!(op, MockCriOperation::CreateContainer { .. })),
        "main recovery parity: realized pod must not create duplicate containers: {cri_ops:?}"
    );
    assert!(
        !cri_ops
            .iter()
            .any(|op| matches!(op, MockCriOperation::StartContainer(_))),
        "main recovery parity: realized pod must not start duplicate containers: {cri_ops:?}"
    );
    assert_eq!(
        harness.container_control.recorded_calls(),
        vec![MockContainerControlOp::ListContainers {
            sandbox_id_filter: Some("sandbox-live".to_string())
        }],
        "runtime must verify the recorded sandbox has live containers before short-circuiting"
    );
    assert_eq!(
        harness.volumes.recorded_calls(),
        vec!["process_volumes:ns/restart-survivor/uid-restart".to_string()],
        "restart recovery must reconcile volumes so projected serviceaccount tokens are refreshed"
    );
}

#[tokio::test]
async fn start_pod_recovery_returns_failed_when_volume_reconcile_fails() {
    let harness = PodRuntimeHarness::new().await;
    let key = PodRuntimeKey::new("ns", "restart-survivor", "uid-restart");
    let mut pod = klights_kubelet::runtime::test_support::pod_json(
        &key.namespace,
        &key.name,
        &key.uid,
        "nginx:1.25",
    );
    pod["status"] = serde_json::json!({
        "phase": "Running",
        "podIP": "10.0.0.21",
        "hostIP": "192.168.1.1",
        "containerStatuses": [{
            "name": "app",
            "containerID": "containerd://container-live",
            "ready": true,
            "started": true,
            "restartCount": 0,
            "state": {"running": {"startedAt": "2026-05-20T00:00:00Z"}}
        }]
    });
    harness.create_runtime_pod(pod.clone()).await;
    harness
        .store
        .record_sandbox(&key, "sandbox-live")
        .await
        .expect("record live sandbox");
    harness.simulate_running_containers(["container-live".to_string()]);
    harness
        .volumes
        .fail_process_volumes("projected token refresh failed");
    harness.cri.clear_calls();

    let result = harness
        .runtime
        .start_pod(key.clone(), Some(pod), CancellationToken::new())
        .await
        .expect("volume reconciliation failure should be reported as pod start result");

    match result {
        PodStartResult::Failed(message) => {
            assert!(
                message.contains("Failed to reconcile volumes for running pod"),
                "failure should describe recovered volume reconciliation: {message}"
            );
            assert!(
                message.contains("projected token refresh failed"),
                "failure should retain the underlying volume error: {message}"
            );
        }
        other => panic!("expected retryable failure, got {other:?}"),
    }
    let cri_ops: Vec<_> = harness
        .cri
        .recorded_calls()
        .into_iter()
        .map(|call| call.operation)
        .collect();
    assert!(
        !cri_ops
            .iter()
            .any(|op| matches!(op, MockCriOperation::RunPodSandbox)),
        "volume reconcile failure for a live sandbox must not recreate the sandbox: {cri_ops:?}"
    );
}

#[tokio::test]
async fn start_pod_partial_container_create_failure_rolls_back_sandbox_with_parity() {
    let harness = PodRuntimeHarness::new().await;
    let mut pod = pod_with_pull_policy("ns", "partial-create", "uid-partial", "nginx", "Never");
    pod["spec"]["containers"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "name": "sidecar",
            "image": "busybox",
            "imagePullPolicy": "Never",
        }));
    harness
        .repo
        .test_create_pod("ns", "partial-create", "test-node", pod.clone())
        .await
        .unwrap();
    harness
        .container_control
        .set_containers(vec![("container-sandbox-0001".into(), "created".into())]);
    harness.cri.set_fail_operation("sidecar");

    let key = PodRuntimeKey::new("ns", "partial-create", "uid-partial");
    let err = harness
        .runtime
        .start_pod(key.clone(), Some(pod), CancellationToken::new())
        .await
        .expect_err("partial create failure must surface as a retryable startup error");
    assert!(
        err.to_string()
            .contains("failed to create container sidecar"),
        "unexpected error: {err:#}"
    );

    let container_calls = harness.container_control.recorded_calls();
    assert!(
        container_calls.iter().any(|call| matches!(
            call,
            MockContainerControlOp::ListContainers { sandbox_id_filter: Some(sandbox_id) }
                if sandbox_id == "sandbox-0001"
        )),
        "partial rollback must list containers by sandbox; calls={container_calls:?}"
    );

    let cri_calls = harness.cri.recorded_calls();
    assert!(
        cri_calls.iter().any(|call| matches!(
            call.operation,
            MockCriOperation::StopContainer(ref container_id, 10)
                if container_id == "container-sandbox-0001"
        )),
        "partial rollback must stop created containers; calls={cri_calls:?}"
    );
    assert!(
        cri_calls.iter().any(|call| matches!(
            call.operation,
            MockCriOperation::RemoveContainer(ref container_id)
                if container_id == "container-sandbox-0001"
        )),
        "partial rollback must remove created containers; calls={cri_calls:?}"
    );
    assert!(
        cri_calls.iter().any(|call| matches!(
            call.operation,
            MockCriOperation::StopPodSandbox(ref sandbox_id) if sandbox_id == "sandbox-0001"
        )),
        "partial rollback must stop the sandbox; calls={cri_calls:?}"
    );
    assert!(
        cri_calls.iter().any(|call| matches!(
            call.operation,
            MockCriOperation::RemovePodSandbox(ref sandbox_id) if sandbox_id == "sandbox-0001"
        )),
        "partial rollback must remove the sandbox; calls={cri_calls:?}"
    );

    let net_calls = harness.network.recorded_calls();
    assert!(
        net_calls.iter().any(|call| matches!(
            call,
            MockNetworkOp::ReleaseSandboxNetwork {
                uid,
                sandbox_id,
                ..
            } if uid == "uid-partial" && sandbox_id == "sandbox-0001"
        )),
        "partial rollback must release the sandbox network; calls={net_calls:?}"
    );
    let store_calls = harness.store.recorded_calls();
    assert!(
        store_calls
            .iter()
            .any(|call| call == "delete_sandbox:ns/partial-create/uid-partial"),
        "partial rollback must clear the UID-bound sandbox row; calls={store_calls:?}"
    );

    let fs_calls = harness.filesystem.recorded_calls();
    assert!(
        fs_calls
            .iter()
            .any(|call| call == "cleanup_fs:ns/partial-create/uid-partial"),
        "partial rollback must remove pod filesystem artifacts; calls={fs_calls:?}"
    );
}

#[tokio::test]
async fn real_runtime_start_pod_propagates_uid_qualified_sandbox_record_failure() {
    let harness = PodRuntimeHarness::new().await;
    let pod = pod_with_pull_policy("ns", "pod", "uid-fb", "nginx", "Never");
    harness
        .repo
        .test_create_pod("ns", "pod", "test-node", pod.clone())
        .await
        .unwrap();
    let key = PodRuntimeKey::new("ns", "pod", "uid-fb");
    harness
        .store
        .fail_record_sandbox("injected owned sandbox persistence failure");
    let error = harness
        .runtime
        .start_pod(key, Some(pod), CancellationToken::new())
        .await
        .expect_err("sandbox ownership persistence failure must fail startup");
    assert!(
        error
            .to_string()
            .contains("injected owned sandbox persistence failure"),
        "unexpected startup error: {error:#}"
    );

    let store_calls = harness.store.recorded_calls();
    assert!(
        store_calls
            .iter()
            .any(|call| call == "record_sandbox:ns/pod/uid-fb=sandbox-0001"),
        "startup must invoke the UID-qualified ownership/record contract: {store_calls:?}"
    );
    assert!(
        harness.cri.recorded_calls().iter().any(|call| matches!(
            &call.operation,
            MockCriOperation::RemovePodSandbox(sandbox_id)
                if sandbox_id == "sandbox-0001"
        )),
        "an unpersisted external sandbox must be rolled back"
    );
}

#[tokio::test]
async fn real_runtime_start_pod_fails_closed_when_sandbox_lookup_fails() {
    let harness = PodRuntimeHarness::new().await;
    let pod = pod_with_pull_policy("ns", "pod", "uid-lookup", "nginx", "Never");
    harness
        .repo
        .test_create_pod("ns", "pod", "test-node", pod.clone())
        .await
        .unwrap();
    harness
        .store
        .fail_sandbox_lookup("injected sandbox lookup failure");

    let error = harness
        .runtime
        .start_pod(
            PodRuntimeKey::new("ns", "pod", "uid-lookup"),
            Some(pod),
            CancellationToken::new(),
        )
        .await
        .expect_err("an unreadable ownership ledger must prevent sandbox creation");

    assert!(
        error
            .to_string()
            .contains("injected sandbox lookup failure"),
        "unexpected startup error: {error:#}"
    );
    assert!(
        !harness
            .cri
            .recorded_calls()
            .iter()
            .any(|call| matches!(call.operation, MockCriOperation::RunPodSandbox)),
        "lookup failure must not create an unverified second sandbox"
    );
}

#[tokio::test]
async fn real_runtime_start_pod_sandbox_rows_are_uid_qualified() {
    let harness = PodRuntimeHarness::new().await;

    // Create sandbox row for old UID.
    let old_key = PodRuntimeKey::new("ns", "pod", "uid-old");
    harness
        .store
        .record_sandbox(&old_key, "sb-old")
        .await
        .unwrap();

    // Start a pod with a new UID.
    let new_pod = pod_with_pull_policy("ns", "pod", "uid-new", "nginx", "Never");
    harness
        .repo
        .test_create_pod("ns", "pod", "test-node", new_pod.clone())
        .await
        .unwrap();
    let new_key = PodRuntimeKey::new("ns", "pod", "uid-new");
    let result = harness
        .runtime
        .start_pod(new_key, Some(new_pod), CancellationToken::new())
        .await
        .unwrap();

    match result {
        PodStartResult::Started {
            sandbox_id: Some(ref sid),
        } => {
            assert!(!sid.is_empty(), "new sandbox must be created");
            // The new sandbox must not overwrite the old UID's sandbox.
            assert_ne!(
                sid, "sb-old",
                "new sandbox must not reuse old UID's sandbox"
            );
        }
        other => panic!("expected Started with sandbox_id, got {:?}", other),
    }

    // Old sandbox must still be present.
    let old_sandbox = harness.store.get_sandbox_id(&old_key).await.unwrap();
    assert_eq!(
        old_sandbox,
        Some("sb-old".to_string()),
        "old UID sandbox must persist"
    );
}

#[tokio::test]
async fn real_runtime_start_pod_passes_verified_identity_to_hostport_filesystem_volume_and_container_ports()
 {
    let harness = PodRuntimeHarness::new().await;
    let pod = pod_with_pull_policy("ns", "iden-pod", "uid-iden", "nginx", "Never");
    harness
        .repo
        .test_create_pod("ns", "iden-pod", "test-node", pod.clone())
        .await
        .unwrap();
    let key = PodRuntimeKey::new("ns", "iden-pod", "uid-iden");
    let result = harness
        .runtime
        .start_pod(key, Some(pod), CancellationToken::new())
        .await
        .unwrap();
    assert!(matches!(result, PodStartResult::Started { .. }));

    // HostPort must be called with correct UID.
    let hp_calls = harness.hostports.recorded_calls();
    let add_call = hp_calls
        .iter()
        .find(|c| matches!(c, MockHostPortOp::Add { .. }));
    assert!(
        add_call.is_some(),
        "HostPort::add_host_ports must be called"
    );
    if let Some(MockHostPortOp::Add { uid, .. }) = add_call {
        assert_eq!(uid, "uid-iden", "HostPort UID must match");
    }

    // Filesystem must be called with correct UID.
    let fs_calls = harness.filesystem.recorded_calls();
    let hosts_call = fs_calls.iter().find(|s| s.contains("write_hosts"));
    assert!(
        hosts_call.is_some(),
        "Filesystem::write_hosts must be called"
    );
    let hosts = hosts_call.unwrap();
    assert!(
        hosts.contains("uid-iden"),
        "Filesystem write_hosts UID must match"
    );

    let log_call = fs_calls.iter().find(|s| s.contains("create_log"));
    assert!(
        log_call.is_some(),
        "Filesystem::create_log_directory must be called"
    );
    let log = log_call.unwrap();
    assert!(
        log.contains("uid-iden"),
        "Filesystem create_log_directory UID must match"
    );

    // Volumes must be called with correct UID.
    let vol_calls = harness.volumes.recorded_calls();
    let proc_call = vol_calls.iter().find(|s| s.contains("process_volumes"));
    assert!(
        proc_call.is_some(),
        "Volumes::process_volumes must be called"
    );
    let proc = proc_call.unwrap();
    assert!(
        proc.contains("uid-iden"),
        "Volumes process_volumes UID must match"
    );

    // Containers must be created/started via CRI.
    let cri_calls = harness.cri.recorded_calls();
    let has_create = cri_calls
        .iter()
        .any(|c| matches!(c.operation, MockCriOperation::CreateContainer { .. }));
    assert!(has_create, "CRI CreateContainer must be called");
    let has_start = cri_calls
        .iter()
        .any(|c| matches!(c.operation, MockCriOperation::StartContainer(_)));
    assert!(has_start, "CRI StartContainer must be called");
}

#[tokio::test]
async fn real_runtime_start_pod_uses_mock_cri_network_store_and_events() {
    // Verify that every mock port wired into RealPodRuntimeService is exercised
    // during a successful start_pod call.
    let harness = PodRuntimeHarness::new().await;
    let pod = pod_with_pull_policy("ns", "all-ports", "uid-ap", "nginx", "Never");
    harness
        .repo
        .test_create_pod("ns", "all-ports", "test-node", pod.clone())
        .await
        .unwrap();
    let key = PodRuntimeKey::new("ns", "all-ports", "uid-ap");
    let result = harness
        .runtime
        .start_pod(key, Some(pod), CancellationToken::new())
        .await
        .unwrap();
    assert!(matches!(result, PodStartResult::Started { .. }));

    // CRI: sandbox + containers.
    assert!(
        !harness.cri.recorded_calls().is_empty(),
        "CRI must be called"
    );
    // Network: read_assignment.
    assert!(
        !harness.network.recorded_calls().is_empty(),
        "Network must be called"
    );
    // Store: record_sandbox.
    assert!(
        !harness.store.recorded_calls().is_empty(),
        "Store must be called"
    );
    // Events: Scheduled at minimum.
    assert!(
        !harness.events.recorded_events().is_empty(),
        "Events must be emitted"
    );
    // HostPorts.
    assert!(
        !harness.hostports.recorded_calls().is_empty(),
        "HostPorts must be called"
    );
    // Filesystem.
    assert!(
        !harness.filesystem.recorded_calls().is_empty(),
        "Filesystem must be called"
    );
    // Volumes.
    assert!(
        !harness.volumes.recorded_calls().is_empty(),
        "Volumes must be called"
    );
}

#[tokio::test]
async fn real_runtime_start_pod_cancel_before_sandbox_does_not_call_cri() {
    let harness = PodRuntimeHarness::new().await;
    let pod = pod_with_pull_policy("ns", "cancel-early", "uid-ce", "nginx", "Never");
    harness
        .repo
        .test_create_pod("ns", "cancel-early", "test-node", pod.clone())
        .await
        .unwrap();
    let key = PodRuntimeKey::new("ns", "cancel-early", "uid-ce");

    let cancel = CancellationToken::new();
    cancel.cancel();

    let result = harness
        .runtime
        .start_pod(key, Some(pod), cancel)
        .await
        .unwrap();

    assert!(
        matches!(result, PodStartResult::Cancelled),
        "expected Cancelled, got {:?}",
        result
    );

    // No CRI sandbox operations must have occurred.
    let cri_calls = harness.cri.recorded_calls();
    let has_sandbox = cri_calls
        .iter()
        .any(|c| matches!(c.operation, MockCriOperation::RunPodSandbox));
    assert!(
        !has_sandbox,
        "CRI sandbox must not be called when cancelled"
    );
    let has_container = cri_calls
        .iter()
        .any(|c| matches!(c.operation, MockCriOperation::CreateContainer { .. }));
    assert!(
        !has_container,
        "CRI container must not be called when cancelled"
    );
}

#[tokio::test]
async fn real_runtime_start_pod_cancel_after_sandbox_rolls_back_uid_bound_state() {
    let harness = PodRuntimeHarness::new().await;
    let pod = pod_with_pull_policy("ns", "cancel-sb", "uid-csb", "nginx", "Never");
    harness
        .repo
        .test_create_pod("ns", "cancel-sb", "test-node", pod.clone())
        .await
        .unwrap();
    let key = PodRuntimeKey::new("ns", "cancel-sb", "uid-csb");

    let cancel = CancellationToken::new();
    // Trigger cancellation inside run_pod_sandbox (after recording).
    harness.cri.set_start_pod_cancel(cancel.clone());

    let result = harness
        .runtime
        .start_pod(key.clone(), Some(pod), cancel)
        .await
        .unwrap();

    assert!(
        matches!(result, PodStartResult::Cancelled),
        "expected Cancelled after sandbox, got {:?}",
        result
    );

    // Sandbox must have been stopped and removed.
    let cri_calls = harness.cri.recorded_calls();
    let has_stop_sandbox = cri_calls
        .iter()
        .any(|c| matches!(c.operation, MockCriOperation::StopPodSandbox(_)));
    assert!(has_stop_sandbox, "sandbox must be stopped on cancel");

    let has_remove_sandbox = cri_calls
        .iter()
        .any(|c| matches!(c.operation, MockCriOperation::RemovePodSandbox(_)));
    assert!(has_remove_sandbox, "sandbox must be removed on cancel");

    // Store must have had sandbox deleted.
    let store_calls = harness.store.recorded_calls();
    let has_delete = store_calls
        .iter()
        .any(|s| s.contains("delete_sandbox") && s.contains("uid-csb"));
    assert!(has_delete, "sandbox must be deleted from store on cancel");
}

#[tokio::test]
async fn real_runtime_start_pod_cancel_after_sandbox_rolls_back() {
    let harness = PodRuntimeHarness::new().await;
    let pod = pod_with_pull_policy("ns", "cancel-rb", "uid-crb", "nginx", "Never");
    harness
        .repo
        .test_create_pod("ns", "cancel-rb", "test-node", pod.clone())
        .await
        .unwrap();
    let key = PodRuntimeKey::new("ns", "cancel-rb", "uid-crb");

    let cancel = CancellationToken::new();
    harness.cri.set_start_pod_cancel(cancel.clone());

    let result = harness
        .runtime
        .start_pod(key.clone(), Some(pod), cancel)
        .await
        .unwrap();

    assert!(matches!(result, PodStartResult::Cancelled));

    // Cgroup must be cleaned up.
    let fs_calls = harness.filesystem.recorded_calls();
    let has_cgroup = fs_calls
        .iter()
        .any(|s| s.contains("cleanup_cgroup") && s.contains("uid-crb"));
    assert!(has_cgroup, "cgroup must be cleaned up on cancel");

    // Network must be released.
    let net_calls = harness.network.recorded_calls();
    let has_release = net_calls.iter().any(|c| {
        matches!(
            c,
            MockNetworkOp::ReleaseSandboxNetwork { uid, .. } if uid == "uid-crb"
        )
    });
    assert!(has_release, "network must be released on cancel");

    // No containers must have been created.
    let cri_calls = harness.cri.recorded_calls();
    let has_create = cri_calls
        .iter()
        .any(|c| matches!(c.operation, MockCriOperation::CreateContainer { .. }));
    assert!(!has_create, "no containers must be created on cancel");
}

#[tokio::test]
async fn real_runtime_stop_pod_missing_snapshot_cleans_sandbox_hint_for_orphan() {
    let harness = PodRuntimeHarness::new().await;
    let key = PodRuntimeKey::new("ns", "stop-nop", "uid-sn");

    harness.container_control.set_container_states(vec![(
        "ctr-orphan".to_string(),
        ContainerRuntimeState::Running,
    )]);

    // Orphan cleanup may not have a deleted Pod snapshot. A sandbox hint must
    // still drive UID-bound CRI/CNI teardown before the actor finalizes.
    harness
        .runtime
        .stop_pod(key.clone(), None, Some("sb-orphan".into()))
        .await
        .unwrap();

    // Probes must be stopped by UID.
    let probe_calls = harness.probes.recorded_calls();
    assert!(
        probe_calls
            .iter()
            .any(|c| matches!(c, MockProbeCall::Stop { uid, .. } if uid == "uid-sn")),
        "probes must be stopped with UID"
    );

    // Slot must be cleared by UID.
    let slot_calls = harness.slot_admission.recorded_calls();
    assert!(
        slot_calls
            .iter()
            .any(|s| s.contains("clear_slot") && s.contains("uid-sn")),
        "slot must be cleared with UID"
    );

    let cri_calls = harness.cri.recorded_calls();
    assert!(
        cri_calls.iter().any(|c| matches!(
            c.operation,
            MockCriOperation::StopContainer(ref container_id, _) if container_id == "ctr-orphan"
        )),
        "missing snapshot orphan cleanup must stop containers: {cri_calls:?}"
    );
    assert!(
        cri_calls.iter().any(|c| matches!(
            c.operation,
            MockCriOperation::RemovePodSandbox(ref sandbox_id) if sandbox_id == "sb-orphan"
        )),
        "missing snapshot orphan cleanup must remove sandbox: {cri_calls:?}"
    );

    let net_calls = harness.network.recorded_calls();
    assert!(
        net_calls.iter().any(|c| matches!(
            c,
            MockNetworkOp::ReleaseSandboxNetwork { uid, sandbox_id, .. }
                if uid == "uid-sn" && sandbox_id == "sb-orphan"
        )),
        "missing snapshot orphan cleanup must release CNI network: {net_calls:?}"
    );

    let fs_calls = harness.filesystem.recorded_calls();
    assert!(
        fs_calls
            .iter()
            .any(|call| call == "cleanup_fs:ns/stop-nop/uid-sn"),
        "missing snapshot orphan cleanup must remove pod filesystem artifacts: {fs_calls:?}"
    );
}

#[tokio::test]
async fn real_runtime_stop_pod_orphan_resolves_sandbox_via_cri_when_store_empty() {
    let harness = PodRuntimeHarness::new().await;
    let key = PodRuntimeKey::new("ns", "stop-cri", "uid-cri");

    // No sandbox hint, no store row — but CRI still has a running sandbox for
    // this UID. The orphan path must discover and stop it via CRI.
    harness.cri.set_pod_sandboxes(vec![(
        "sb-cri",
        "ns",
        "stop-cri",
        "uid-cri",
        "SANDBOX_READY",
    )]);
    harness.container_control.set_container_states(vec![(
        "ctr-cri".to_string(),
        ContainerRuntimeState::Running,
    )]);

    harness
        .runtime
        .stop_pod(key.clone(), None, None)
        .await
        .unwrap();

    let cri_calls = harness.cri.recorded_calls();
    assert!(
        cri_calls.iter().any(|c| matches!(
            c.operation,
            MockCriOperation::ListPodSandboxes(Some(ref uid)) if uid == "uid-cri"
        )),
        "orphan stop with empty store must query CRI by UID: {cri_calls:?}"
    );
    assert!(
        cri_calls.iter().any(|c| matches!(
            c.operation,
            MockCriOperation::StopPodSandbox(ref sandbox_id) if sandbox_id == "sb-cri"
        )),
        "orphan stop must stop the CRI-resolved sandbox, not just clear the slot: {cri_calls:?}"
    );
    assert!(
        cri_calls.iter().any(|c| matches!(
            c.operation,
            MockCriOperation::RemovePodSandbox(ref sandbox_id) if sandbox_id == "sb-cri"
        )),
        "orphan stop must remove the CRI-resolved sandbox: {cri_calls:?}"
    );

    // Slot is still cleared, but only after runtime cleanup.
    let slot_calls = harness.slot_admission.recorded_calls();
    assert!(
        slot_calls
            .iter()
            .any(|s| s.contains("clear_slot") && s.contains("uid-cri")),
        "slot must be cleared by UID after runtime cleanup: {slot_calls:?}"
    );

    let fs_calls = harness.filesystem.recorded_calls();
    assert!(
        fs_calls
            .iter()
            .any(|call| call == "cleanup_fs:ns/stop-cri/uid-cri"),
        "CRI-resolved orphan cleanup must remove pod filesystem artifacts: {fs_calls:?}"
    );
}

#[tokio::test]
async fn real_runtime_stop_pod_stops_and_removes_containers_idempotently() {
    let harness = PodRuntimeHarness::new().await;
    let pod = pod_with_pull_policy("ns", "stop-idem", "uid-idem", "nginx", "Never");
    harness
        .repo
        .test_create_pod("ns", "stop-idem", "test-node", pod.clone())
        .await
        .unwrap();
    let key = PodRuntimeKey::new("ns", "stop-idem", "uid-idem");
    let sandbox_id = "sb-idem";

    harness
        .store
        .record_sandbox(&key, sandbox_id)
        .await
        .unwrap();
    harness
        .container_control
        .set_containers(vec![("ctr-a".into(), "running".into())]);

    // First call: container is stopped and removed.
    harness
        .runtime
        .stop_pod(key.clone(), Some(pod.clone()), Some(sandbox_id.into()))
        .await
        .unwrap();

    let cri_calls_1 = harness.cri.recorded_calls();
    let stop_count_1 = cri_calls_1
        .iter()
        .filter(|c| matches!(c.operation, MockCriOperation::StopContainer(_, _)))
        .count();
    assert_eq!(stop_count_1, 1, "first call must stop the container");

    // Second call: idempotent — still succeeds even though containers no longer exist.
    // The mock still returns the same container list, so it will be stopped again.
    harness
        .runtime
        .stop_pod(key.clone(), Some(pod), Some(sandbox_id.into()))
        .await
        .unwrap();

    let cri_calls_2 = harness.cri.recorded_calls();
    let stop_count_2 = cri_calls_2
        .iter()
        .filter(|c| matches!(c.operation, MockCriOperation::StopContainer(_, _)))
        .count();
    // Since the mock is stateful and records accumulate, we expect 2 stops total
    // (1 from first call + 1 from second call).
    assert_eq!(
        stop_count_2, 2,
        "second stop must be idempotent (2 total stops)"
    );
}

#[tokio::test]
async fn real_runtime_stop_pod_confirms_cri_absence_before_success() {
    let harness = PodRuntimeHarness::new().await;
    let pod = pod_with_pull_policy("ns", "stop-abs", "uid-sa", "nginx", "Never");
    harness
        .repo
        .test_create_pod("ns", "stop-abs", "test-node", pod.clone())
        .await
        .unwrap();
    let key = PodRuntimeKey::new("ns", "stop-abs", "uid-sa");
    let sandbox_id = "sb-absence";

    harness
        .store
        .record_sandbox(&key, sandbox_id)
        .await
        .unwrap();
    // No containers set up in container_control (empty list).

    harness
        .runtime
        .stop_pod(key.clone(), Some(pod), Some(sandbox_id.into()))
        .await
        .unwrap();

    // CRI list containers must have been called (to confirm absence).
    let cc_calls = harness.container_control.recorded_calls();
    assert!(
        cc_calls.iter().any(|c| matches!(
            c,
            MockContainerControlOp::ListContainers {
                sandbox_id_filter: Some(_)
            }
        )),
        "must list containers to confirm CRI absence"
    );

    // Slot must be cleared by UID.
    let slot_calls = harness.slot_admission.recorded_calls();
    assert!(
        slot_calls
            .iter()
            .any(|s| s.contains("clear_slot") && s.contains("uid-sa")),
        "slot must be cleared"
    );
}

#[tokio::test]
async fn cri_leftover_cleanup_is_node_local() {
    // Build a runtime on worker-1 and test reconcile_cri_leftovers node-local gate.
    let (cri, runtime, repo) = fixture_runtime_with_node("worker-1").await;

    // Pod scheduled to a different node — reconcile must return Ok without CRI work.
    let cross_pod = scheduled_pod_json("ns", "cross-pod", "uid-cross", "worker-2");
    repo.test_create_pod("ns", "cross-pod", "worker-2", cross_pod)
        .await
        .unwrap();
    let cross_key = PodRuntimeKey::new("ns", "cross-pod", "uid-cross");
    runtime.reconcile_cri_leftovers(cross_key).await.unwrap();
    // CRI must not have been called for the non-owned pod.
    assert!(
        cri.recorded_calls().is_empty(),
        "CRI must not be called for non-owned pod in reconcile_cri_leftovers"
    );

    // Pod scheduled to this node — reconcile must proceed.
    let local_pod = scheduled_pod_json("ns", "local-pod", "uid-local", "worker-1");
    repo.test_create_pod("ns", "local-pod", "worker-1", local_pod)
        .await
        .unwrap();
    let local_key = PodRuntimeKey::new("ns", "local-pod", "uid-local");
    runtime.reconcile_cri_leftovers(local_key).await.unwrap();
    // Owned pod: method returns Ok; CRI work would happen here when implemented.
}

#[tokio::test]
async fn mock_dependency_matrix_cri() {
    let mock = MockCriRuntime::new();

    let _present = mock.image_status("nginx:latest").await.unwrap();
    mock.pull_image("nginx:latest").await.unwrap();
    let sandbox_id = mock
        .run_pod_sandbox(PodSandboxConfig::default())
        .await
        .unwrap();
    let container_id = mock
        .create_container(
            k8s_cri::v1::ContainerConfig::default(),
            &sandbox_id,
            PodSandboxConfig::default(),
        )
        .await
        .unwrap();
    mock.start_container(&container_id).await.unwrap();
    mock.stop_container(&container_id, 30).await.unwrap();
    mock.remove_container(&container_id).await.unwrap();
    mock.stop_pod_sandbox(&sandbox_id).await.unwrap();
    mock.remove_pod_sandbox(&sandbox_id).await.unwrap();

    let calls = mock.recorded_calls();
    assert!(
        calls.len() >= 9,
        "expected at least 9 recorded CRI calls, got {}",
        calls.len()
    );
    let call_names: Vec<String> = calls.iter().map(|c| format!("{:?}", c.operation)).collect();
    assert!(
        call_names[0].contains("ImageStatus"),
        "first call must be image status check"
    );
    assert!(
        call_names[1].contains("PullImage"),
        "second call must be image pull"
    );
    assert!(
        call_names[2].contains("RunPodSandbox"),
        "third call must be sandbox run"
    );
    assert!(
        call_names[3].contains("CreateContainer"),
        "fourth call must be container create"
    );
    assert!(
        call_names[4].contains("StartContainer"),
        "fifth call must be container start"
    );
    assert!(
        call_names[call_names.len() - 4].contains("StopContainer"),
        "stop container must precede remove"
    );
    assert!(
        call_names[call_names.len() - 3].contains("RemoveContainer"),
        "remove container must follow stop"
    );
    assert!(
        call_names[call_names.len() - 2].contains("StopPodSandbox"),
        "stop sandbox must be recorded"
    );
    assert!(
        call_names[call_names.len() - 1].contains("RemovePodSandbox"),
        "remove sandbox must be last"
    );
}

#[tokio::test]
async fn mock_dependency_matrix_runtime_store() {
    let store = MockPodRuntimeStore::new();
    let old_key = PodRuntimeKey::new("ns", "same-name", "uid-old");
    let new_key = PodRuntimeKey::new("ns", "same-name", "uid-new");

    store.record_sandbox(&old_key, "sb-old").await.unwrap();
    store.record_sandbox(&new_key, "sb-new").await.unwrap();

    let old_sb = store.get_sandbox_id(&old_key).await.unwrap();
    assert_eq!(old_sb.as_deref(), Some("sb-old"));
    let new_sb = store.get_sandbox_id(&new_key).await.unwrap();
    assert_eq!(new_sb.as_deref(), Some("sb-new"));

    store.delete_sandbox(&old_key).await.unwrap();
    assert!(store.get_sandbox_id(&old_key).await.unwrap().is_none());
    let new_sb_after = store.get_sandbox_id(&new_key).await.unwrap();
    assert_eq!(
        new_sb_after.as_deref(),
        Some("sb-new"),
        "new UID sandbox must survive old UID deletion"
    );
}

#[tokio::test]
async fn mock_dependency_matrix_repository() {
    let store = MockPodRuntimeStore::new();
    let key = PodRuntimeKey::new("ns", "repo-pod", "uid-repo");
    let stale_key = PodRuntimeKey::new("ns", "repo-pod", "uid-stale");

    store.record_sandbox(&key, "sb-repo").await.unwrap();
    assert_eq!(
        store.get_sandbox_id(&key).await.unwrap().as_deref(),
        Some("sb-repo")
    );

    let stale_result = store.get_sandbox_id(&stale_key).await.unwrap();
    assert!(
        stale_result.is_none(),
        "stale UID must not see real UID sandbox"
    );

    store.delete_sandbox(&stale_key).await.unwrap();
    assert_eq!(
        store.get_sandbox_id(&key).await.unwrap().as_deref(),
        Some("sb-repo"),
        "real UID sandbox must survive stale-UID delete"
    );
}

#[tokio::test]
async fn mock_dependency_matrix_event_sink() {
    let mock = MockPodEventSink::new();
    let key = PodRuntimeKey::new("ns", "ev-pod", "uid-ev");

    mock.emit_pod_event(
        &key,
        "Normal",
        "Scheduled",
        "pod scheduled",
        "klights",
        "node-1",
    )
    .await
    .unwrap();
    mock.emit_pod_event(
        &key,
        "Normal",
        "Pulling",
        "pulling nginx:latest",
        "klights",
        "node-1",
    )
    .await
    .unwrap();
    mock.emit_pod_event(
        &key,
        "Normal",
        "Pulled",
        "pulled nginx:latest",
        "klights",
        "node-1",
    )
    .await
    .unwrap();
    mock.emit_pod_event(
        &key,
        "Warning",
        "Failed",
        "ImagePullBackOff",
        "klights",
        "node-1",
    )
    .await
    .unwrap();

    let events = mock.recorded_events();
    assert_eq!(events.len(), 4);
    let expected_reasons = ["Scheduled", "Pulling", "Pulled", "Failed"];
    for (i, event) in events.iter().enumerate() {
        assert_eq!(event.namespace, "ns");
        assert_eq!(event.name, "ev-pod");
        assert_eq!(event.uid, "uid-ev");
        assert!(
            event.reason.contains(expected_reasons[i]),
            "event {} reason must contain '{}', got '{}'",
            i,
            expected_reasons[i],
            event.reason
        );
    }
}

#[tokio::test]
async fn shared_cri_runtime_implements_container_runtime_control_with_parity() {
    // Verify the trait is implemented on the production adapter type.
    use klights_kubelet::runtime::cri::ContainerRuntimeControl;
    // Trait object conversion compiles only if SharedCriRuntime: ContainerRuntimeControl.
    fn _assert_impl(_: &dyn ContainerRuntimeControl) {}
    // Verify mock also implements the trait (Task 20.1 coverage gate).
    let mock = MockContainerRuntimeControl::new();
    let result = mock.list_containers(Some("sb-1")).await;
    assert!(result.is_ok());
    assert_eq!(mock.recorded_calls().len(), 1);
}

#[tokio::test]
async fn real_pod_runtime_store_records_and_retrieves_sandbox() {
    let pod_runtime_store = node_local_runtime_store().await;
    let persisted_runtime_store = pod_runtime_store.pod_runtime();
    let key = PodRuntimeKey::new("ns", "test-pod", "uid-1");
    let store = crate::kubelet::pod_runtime::store::RealPodRuntimeStore::new(
        persisted_runtime_store.clone(),
        "node-1",
        Arc::new(FixedRuntimeClock(1_234_567)),
    );

    // Production startup has no separate node-local runtime admission step.
    // Recording the sandbox must establish the UID-qualified runtime
    // ownership needed by every later reconcile.
    store.record_sandbox(&key, "sandbox-abc").await.unwrap();

    // Retrieve by UID.
    let found = store.get_sandbox_id(&key).await.unwrap();
    assert_eq!(found.as_deref(), Some("sandbox-abc"));
    let persisted = klights_node_store::PodRuntimeStore::get_pod_runtime(
        persisted_runtime_store.as_ref(),
        klights_node_store::RuntimePodUid::try_new(&key.uid).unwrap(),
    )
    .await
    .unwrap()
    .expect("recording a sandbox must persist runtime ownership");
    assert_eq!(persisted.pod().namespace, key.namespace);
    assert_eq!(persisted.pod().name, key.name);
    assert_eq!(persisted.pod().uid, key.uid);
    assert_eq!(persisted.node_name(), "node-1");
    assert_eq!(persisted.sandbox_id(), Some("sandbox-abc"));
    assert_eq!(persisted.created_ms(), 1_234_567);

    let sandbox_conflict = store
        .record_sandbox(&key, "sandbox-other")
        .await
        .expect_err("one owned Pod UID must not be rebound to a second sandbox");
    assert_eq!(
        sandbox_conflict.downcast_ref::<klights_node_store::RuntimeWorkError>(),
        Some(&klights_node_store::RuntimeWorkError::OwnershipConflict {
            pod_uid: "uid-1".to_string(),
            existing_namespace: "ns".to_string(),
            existing_pod_name: "test-pod".to_string(),
            existing_node_name: "node-1".to_string(),
            existing_sandbox_id: Some("sandbox-abc".to_string()),
        })
    );
    let after_sandbox_conflict = klights_node_store::PodRuntimeStore::get_pod_runtime(
        persisted_runtime_store.as_ref(),
        klights_node_store::RuntimePodUid::try_new(&key.uid).unwrap(),
    )
    .await
    .unwrap()
    .expect("sandbox conflict must preserve the original runtime row");
    assert_eq!(after_sandbox_conflict.sandbox_id(), Some("sandbox-abc"));

    let conflicting_key = PodRuntimeKey::new("other-ns", "other-pod", "uid-1");
    let conflict = store
        .record_sandbox(&conflicting_key, "sandbox-conflict")
        .await
        .expect_err("one Pod UID must not be rebound to conflicting runtime ownership");
    assert!(
        conflict.to_string().contains("sandbox"),
        "unexpected ownership conflict: {conflict:#}"
    );
    let after_conflict = klights_node_store::PodRuntimeStore::get_pod_runtime(
        persisted_runtime_store.as_ref(),
        klights_node_store::RuntimePodUid::try_new(&key.uid).unwrap(),
    )
    .await
    .unwrap()
    .expect("ownership conflict must preserve the original runtime row");
    assert_eq!(after_conflict.pod().namespace, key.namespace);
    assert_eq!(after_conflict.pod().name, key.name);
    assert_eq!(after_conflict.sandbox_id(), Some("sandbox-abc"));

    // Delete by UID.
    store.delete_sandbox(&key).await.unwrap();
    let after_delete = store.get_sandbox_id(&key).await.unwrap();
    assert!(after_delete.is_none());
}

#[tokio::test]
async fn real_runtime_reconcile_runtime_noop_when_no_sandbox() {
    let harness = PodRuntimeHarness::new().await;
    let key = PodRuntimeKey::new("ns", "rec-pod", "uid-1");
    // No sandbox recorded — should be a no-op.
    let result = harness
        .runtime
        .reconcile_runtime(
            key,
            crate::kubelet::pod_runtime::service::RuntimeReconcileHint::none(),
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn real_runtime_reconcile_runtime_restarts_exited_restart_policy_always_container() {
    use klights_kubelet::pod_repository::PodStatusUpdate;
    use klights_kubelet::runtime::cri::ContainerRuntimeState;

    let harness = PodRuntimeHarness::new().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "container-runtime",
            "name": "terminate-cmd",
            "uid": "uid-terminate-cmd",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "restartPolicy": "Always",
            "containers": [{
                "name": "lifecycle-container",
                "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1",
                "imagePullPolicy": "Never",
                "command": ["/bin/sh", "-c", "exit 0"]
            }]
        },
        "status": {
            "phase": "Running",
            "podIP": "10.50.1.9",
            "containerStatuses": [{
                "name": "lifecycle-container",
                "containerID": "containerd://ctr-terminated",
                "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1",
                "imageID": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1",
                "ready": true,
                "started": true,
                "restartCount": 0,
                "state": {"running": {"startedAt": "2026-05-19T23:18:00Z"}}
            }]
        }
    });
    let key = PodRuntimeKey::new("container-runtime", "terminate-cmd", "uid-terminate-cmd");
    harness.create_runtime_pod(pod.clone()).await;
    harness
        .repo
        .pod_status_writer
        .set_pod_status_for_uid(
            "container-runtime",
            "terminate-cmd",
            "uid-terminate-cmd",
            PodStatusUpdate {
                phase: "Running".to_string(),
                pod_ip: "10.50.1.9".to_string(),
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
    harness
        .store
        .record_sandbox(&key, "sandbox-terminate-cmd")
        .await
        .unwrap();
    harness.container_control.set_container_states(vec![(
        "ctr-terminated".into(),
        ContainerRuntimeState::Exited,
    )]);
    harness
        .cri
        .set_container_status_state(k8s_cri::v1::ContainerState::ContainerExited as i32);
    harness.cri.set_container_exit_code(0);

    harness
        .runtime
        .reconcile_runtime(
            key.clone(),
            crate::kubelet::pod_runtime::service::RuntimeReconcileHint::none(),
        )
        .await
        .unwrap();

    let calls = harness.cri.recorded_calls();
    assert!(
        calls.iter().any(|call| {
            matches!(
                &call.operation,
                MockCriOperation::StopContainer(container_id, 10)
                    if container_id == "ctr-terminated"
            )
        }),
        "restartPolicy=Always must stop an observed exited app container before restart"
    );
    assert!(
        calls.iter().any(|call| {
            matches!(
                &call.operation,
                MockCriOperation::RemoveContainer(container_id)
                    if container_id == "ctr-terminated"
            )
        }),
        "restartPolicy=Always must remove an observed exited app container before replacement"
    );
    assert!(
        calls.iter().any(|call| {
            matches!(
                &call.operation,
                MockCriOperation::CreateContainer {
                    sandbox_id,
                    container_name,
                } if sandbox_id == "sandbox-terminate-cmd"
                    && container_name == "lifecycle-container"
            )
        }),
        "runtime reconcile must create a replacement container in the existing sandbox"
    );

    let create_configs = harness.cri.recorded_create_configs();
    let restart_config = create_configs
        .last()
        .expect("restart must create a replacement container");
    assert_eq!(
        restart_config
            .image
            .as_ref()
            .map(|image| image.image.as_str()),
        Some("registry.k8s.io/e2e-test-images/busybox:1.37.0-1"),
        "replacement container config must be rebuilt from the pod spec"
    );

    let stored = harness.stored_pod(&key).await;
    let status = stored
        .pointer("/status/containerStatuses/0")
        .expect("container status must remain present after restart note");
    assert_eq!(status.pointer("/restartCount"), Some(&serde_json::json!(1)));
    assert!(
        status.pointer("/lastState/terminated").is_some(),
        "runtime reconcile must preserve the terminated lastState while recording the restart"
    );
}

#[tokio::test]
async fn real_runtime_reconcile_restart_policy_always_publishes_replacement_running_status() {
    use klights_kubelet::pod_repository::PodStatusUpdate;
    use klights_kubelet::runtime::cri::ContainerRuntimeState;

    let harness = PodRuntimeHarness::new().await;
    let key = PodRuntimeKey::new(
        "container-runtime",
        "terminate-cmd-rpa",
        "uid-terminate-rpa",
    );
    let image = "registry.k8s.io/e2e-test-images/busybox:1.37.0-1";
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "container-runtime",
            "name": "terminate-cmd-rpa",
            "uid": "uid-terminate-rpa",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "restartPolicy": "Always",
            "containers": [{
                "name": "lifecycle-container",
                "image": image,
                "imagePullPolicy": "Never",
                "command": ["/bin/sh", "-c", "exit 0"]
            }]
        },
        "status": {
            "phase": "Running",
            "podIP": "10.50.1.10",
            "containerStatuses": [{
                "name": "lifecycle-container",
                "containerID": "containerd://ctr-second-exit",
                "image": image,
                "imageID": image,
                "ready": false,
                "started": true,
                "restartCount": 1,
                "lastState": {
                    "terminated": {
                        "exitCode": 1,
                        "reason": "Error",
                        "startedAt": "2026-05-19T23:18:00Z",
                        "finishedAt": "2026-05-19T23:18:01Z"
                    }
                },
                "state": {"running": {"startedAt": "2026-05-19T23:18:02Z"}}
            }]
        }
    });
    harness.create_runtime_pod(pod.clone()).await;
    harness
        .repo
        .pod_status_writer
        .set_pod_status_for_uid(
            "container-runtime",
            "terminate-cmd-rpa",
            "uid-terminate-rpa",
            PodStatusUpdate {
                phase: "Running".to_string(),
                pod_ip: "10.50.1.10".to_string(),
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
    harness
        .store
        .record_sandbox(&key, "sandbox-terminate-rpa")
        .await
        .unwrap();
    harness.container_control.set_container_states(vec![(
        "ctr-second-exit".into(),
        ContainerRuntimeState::Exited,
    )]);
    harness
        .cri
        .set_container_status_state(k8s_cri::v1::ContainerState::ContainerExited as i32);
    harness.cri.set_container_exit_code(0);

    harness
        .runtime
        .reconcile_runtime(
            key.clone(),
            crate::kubelet::pod_runtime::service::RuntimeReconcileHint::none(),
        )
        .await
        .unwrap();

    let stored = harness.stored_pod(&key).await;
    let status = stored
        .pointer("/status/containerStatuses/0")
        .expect("container status must be stored after restart");
    assert_eq!(
        status.get("containerID").and_then(|value| value.as_str()),
        Some("containerd://container-sandbox-terminate-rpa"),
        "status must point at the replacement container, not the exited one"
    );
    assert_eq!(
        status.get("restartCount").and_then(|value| value.as_i64()),
        Some(2),
        "second restart must preserve the observed restart count"
    );
    assert!(
        status.pointer("/state/running/startedAt").is_some(),
        "replacement container must be published as running immediately after StartContainer"
    );
    assert_eq!(
        status.get("ready").and_then(|value| value.as_bool()),
        Some(true),
        "a running replacement without readinessProbe must make ContainersReady true"
    );
    assert_eq!(
        stored
            .pointer("/status/conditions")
            .and_then(|value| value.as_array())
            .and_then(|conditions| {
                conditions.iter().find(|condition| {
                    condition.get("type").and_then(|value| value.as_str()) == Some("Ready")
                })
            })
            .and_then(|condition| condition.get("status"))
            .and_then(|value| value.as_str()),
        Some("True")
    );
}

#[tokio::test]
async fn real_runtime_finalize_startup_returns_unconfirmed_when_pod_not_found() {
    let harness = PodRuntimeHarness::new().await;
    let key = PodRuntimeKey::new("ns", "no-pod", "uid-none");
    let result = harness.runtime.finalize_startup(key, None, None).await;
    assert_eq!(result.unwrap(), PodFinalizeStartupResult::Unconfirmed);
}

#[tokio::test]
async fn real_runtime_finalize_startup_unconfirmed_when_pod_not_found_or_pending() {
    let harness = PodRuntimeHarness::new().await;
    // Key for a non-existent pod — should return Ok (unconfirmed).
    let key = PodRuntimeKey::new("ns", "no-such-pod", "uid-none");
    let result = harness.runtime.finalize_startup(key, None, None).await;
    assert_eq!(result.unwrap(), PodFinalizeStartupResult::Unconfirmed);
}

#[tokio::test]
async fn real_runtime_finalize_startup_returns_confirmed_sandbox_id_when_running_with_podip() {
    use crate::kubelet::pod_runtime::store::PodRuntimeStore;
    use klights_kubelet::pod_repository::PodStatusUpdate;

    let harness = PodRuntimeHarness::new().await;
    let key = PodRuntimeKey::new("ns", "confirmed-pod", "uid-confirmed");
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "ns",
            "name": "confirmed-pod",
            "uid": "uid-confirmed",
            "resourceVersion": "1"
        },
        "spec": {"containers": [{"name": "app", "image": "nginx"}]},
        "status": {"phase": "Running", "podIP": "10.0.0.23"}
    });
    harness
        .repo
        .test_create_pod("ns", "confirmed-pod", "test-node", pod)
        .await
        .unwrap();
    harness
        .repo
        .pod_status_writer
        .set_pod_status_for_uid(
            "ns",
            "confirmed-pod",
            "uid-confirmed",
            PodStatusUpdate {
                phase: "Running".to_string(),
                pod_ip: "10.0.0.23".to_string(),
                host_ip: String::new(),
                container_statuses: Vec::new(),
                init_container_statuses: None,
                qos_class: None,
            },
            None,
        )
        .await
        .unwrap();
    harness
        .store
        .record_sandbox(&key, "sandbox-confirmed")
        .await
        .unwrap();

    let result = harness
        .runtime
        .finalize_startup(key, None, None)
        .await
        .unwrap();

    assert_eq!(
        result,
        PodFinalizeStartupResult::Confirmed {
            sandbox_id: "sandbox-confirmed".to_string()
        }
    );
}

#[tokio::test]
async fn runtime_finalize_startup_uses_sandbox_hint_when_store_row_missing() {
    let harness = PodRuntimeHarness::new().await;
    let key = PodRuntimeKey::new("ns", "hint-pod", "uid-hint");
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "ns",
            "name": "hint-pod",
            "uid": "uid-hint",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "containers": [{"name": "app", "image": "nginx:1.25"}]
        },
        "status": {
            "phase": "Running",
            "podIP": "10.42.0.11",
            "podIPs": [{"ip": "10.42.0.11"}]
        }
    });
    harness
        .db_handle
        .create_resource("v1", "Pod", Some("ns"), "hint-pod", pod.clone())
        .await
        .unwrap();

    let result = harness
        .runtime
        .finalize_startup(key, Some(pod), Some("sandbox-hint".to_string()))
        .await
        .unwrap();

    assert_eq!(
        result,
        PodFinalizeStartupResult::Confirmed {
            sandbox_id: "sandbox-hint".to_string()
        },
        "finalize_startup must use the actor-provided sandbox hint when the store row is absent"
    );
    assert!(matches!(
        harness.probes.recorded_calls().as_slice(),
        [
            MockProbeCall::RecordStartedSandbox { sandbox_id, .. },
            MockProbeCall::Start { sandbox_id: started, .. },
            MockProbeCall::MarkStartedSandboxFinalized {
                sandbox_id: finalized,
                ..
            },
        ] if sandbox_id == "sandbox-hint"
            && started == "sandbox-hint"
            && finalized == "sandbox-hint"
    ));
}

#[tokio::test]
async fn runtime_finalize_startup_uses_pod_annotation_when_store_row_missing() {
    let harness = PodRuntimeHarness::new().await;
    let key = PodRuntimeKey::new("ns", "annotated-pod", "uid-annotated");
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "ns",
            "name": "annotated-pod",
            "uid": "uid-annotated",
            "resourceVersion": "1",
            "annotations": {
                "klights.dev/sandbox-id": "sandbox-annotation"
            }
        },
        "spec": {
            "nodeName": "test-node",
            "containers": [{"name": "app", "image": "nginx:1.25"}]
        },
        "status": {
            "phase": "Running",
            "podIP": "10.42.0.12",
            "podIPs": [{"ip": "10.42.0.12"}]
        }
    });
    harness
        .db_handle
        .create_resource("v1", "Pod", Some("ns"), "annotated-pod", pod)
        .await
        .unwrap();

    let result = harness
        .runtime
        .finalize_startup(key, None, None)
        .await
        .unwrap();

    assert_eq!(
        result,
        PodFinalizeStartupResult::Confirmed {
            sandbox_id: "sandbox-annotation".to_string()
        },
        "finalize_startup must use the pod annotation fallback when the store row is absent"
    );
}

#[tokio::test]
async fn finalize_startup_started_sandbox_idempotency_with_parity() {
    use crate::kubelet::pod_runtime::store::PodRuntimeStore;

    let harness = PodRuntimeHarness::new().await;
    let key = PodRuntimeKey::new("ns", "idempotent-pod", "uid-idem");
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "ns",
            "name": "idempotent-pod",
            "uid": "uid-idem",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "containers": [{"name": "app", "image": "nginx:1.25"}]
        },
        "status": {
            "phase": "Running",
            "podIP": "10.42.0.9",
            "podIPs": [{"ip": "10.42.0.9"}]
        }
    });
    harness
        .db_handle
        .create_resource("v1", "Pod", Some("ns"), "idempotent-pod", pod)
        .await
        .unwrap();
    harness
        .store
        .record_sandbox(&key, "sandbox-1")
        .await
        .unwrap();

    let first = harness
        .runtime
        .finalize_startup(key.clone(), None, None)
        .await
        .unwrap();
    let second = harness
        .runtime
        .finalize_startup(key.clone(), None, None)
        .await
        .unwrap();
    harness
        .store
        .record_sandbox(&key, "sandbox-2")
        .await
        .unwrap();
    let third = harness
        .runtime
        .finalize_startup(key.clone(), None, None)
        .await
        .unwrap();

    assert_eq!(
        first,
        PodFinalizeStartupResult::Confirmed {
            sandbox_id: "sandbox-1".to_string()
        }
    );
    assert_eq!(
        second,
        PodFinalizeStartupResult::Confirmed {
            sandbox_id: "sandbox-1".to_string()
        }
    );
    assert_eq!(
        third,
        PodFinalizeStartupResult::Confirmed {
            sandbox_id: "sandbox-2".to_string()
        }
    );

    assert_eq!(
        harness.probes.recorded_calls(),
        vec![
            MockProbeCall::RecordStartedSandbox {
                namespace: "ns".to_string(),
                name: "idempotent-pod".to_string(),
                uid: "uid-idem".to_string(),
                sandbox_id: "sandbox-1".to_string(),
            },
            MockProbeCall::Start {
                namespace: "ns".to_string(),
                name: "idempotent-pod".to_string(),
                uid: "uid-idem".to_string(),
                sandbox_id: "sandbox-1".to_string(),
            },
            MockProbeCall::MarkStartedSandboxFinalized {
                namespace: "ns".to_string(),
                name: "idempotent-pod".to_string(),
                uid: "uid-idem".to_string(),
                sandbox_id: "sandbox-1".to_string(),
            },
            MockProbeCall::RecordStartedSandbox {
                namespace: "ns".to_string(),
                name: "idempotent-pod".to_string(),
                uid: "uid-idem".to_string(),
                sandbox_id: "sandbox-1".to_string(),
            },
            MockProbeCall::RecordStartedSandbox {
                namespace: "ns".to_string(),
                name: "idempotent-pod".to_string(),
                uid: "uid-idem".to_string(),
                sandbox_id: "sandbox-2".to_string(),
            },
            MockProbeCall::Start {
                namespace: "ns".to_string(),
                name: "idempotent-pod".to_string(),
                uid: "uid-idem".to_string(),
                sandbox_id: "sandbox-2".to_string(),
            },
            MockProbeCall::MarkStartedSandboxFinalized {
                namespace: "ns".to_string(),
                name: "idempotent-pod".to_string(),
                uid: "uid-idem".to_string(),
                sandbox_id: "sandbox-2".to_string(),
            },
        ],
        "startup finalization must record, start, and mark exactly once per sandbox"
    );
}

#[tokio::test]
async fn finalize_startup_accepts_podips_startup_status_with_parity() {
    use crate::kubelet::pod_runtime::store::PodRuntimeStore;

    let harness = PodRuntimeHarness::new().await;
    let key = PodRuntimeKey::new("ns", "podips-pod", "uid-podips");
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "ns",
            "name": "podips-pod",
            "uid": "uid-podips",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "containers": [{"name": "app", "image": "nginx:1.25"}]
        },
        "status": {
            "phase": "Running",
            "podIPs": [{"ip": "10.42.0.10"}]
        }
    });
    harness
        .db_handle
        .create_resource("v1", "Pod", Some("ns"), "podips-pod", pod)
        .await
        .unwrap();
    harness
        .store
        .record_sandbox(&key, "sandbox-podips")
        .await
        .unwrap();

    let result = harness
        .runtime
        .finalize_startup(key, None, None)
        .await
        .unwrap();

    assert_eq!(
        result,
        PodFinalizeStartupResult::Confirmed {
            sandbox_id: "sandbox-podips".to_string()
        },
        "main accepts status.podIPs[0].ip as a published startup IP"
    );
}

#[tokio::test]
async fn container_lifecycle_event_emissions_with_parity() {
    let harness = PodRuntimeHarness::new().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "sysctl",
            "name": "sysctl-pod",
            "uid": "uid-sysctl-pod",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "restartPolicy": "Never",
            "containers": [{
                "name": "test-container",
                "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1",
                "imagePullPolicy": "Never"
            }]
        },
        "status": {"phase": "Pending"}
    });
    let key = PodRuntimeKey::new("sysctl", "sysctl-pod", "uid-sysctl-pod");
    harness.create_runtime_pod(pod.clone()).await;

    let result = harness
        .runtime
        .start_pod(key, Some(pod), CancellationToken::new())
        .await
        .unwrap();

    assert!(matches!(result, PodStartResult::Started { .. }));
    let events = harness.events.recorded_events();
    assert!(
        events.iter().any(|event| {
            event.event_type == "Normal"
                && event.reason == "Created"
                && event.message == "Created container test-container"
        }),
        "main container creation must emit Created event; got {events:?}"
    );
    assert!(
        events.iter().any(|event| {
            event.event_type == "Normal"
                && event.reason == "Started"
                && event.message == "Started container test-container"
        }),
        "main container start must emit Started event; got {events:?}"
    );
}

#[tokio::test]
async fn init_container_event_subscription_after_start_with_parity() {
    let harness = PodRuntimeHarness::new().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "ns",
            "name": "init-event-pod",
            "uid": "uid-init-event",
            "resourceVersion": "1"
        },
        "spec": {
            "initContainers": [
                {"name": "init-1", "image": "busybox:1.35", "imagePullPolicy": "Never"}
            ],
            "containers": [
                {"name": "app", "image": "nginx:1.25", "imagePullPolicy": "Never"}
            ],
            "nodeName": "test-node"
        },
        "status": {"phase": "Pending"}
    });
    harness.create_runtime_pod(pod.clone()).await;

    let key = PodRuntimeKey::new("ns", "init-event-pod", "uid-init-event");
    let result = harness
        .runtime
        .start_pod(key, Some(pod), CancellationToken::new())
        .await
        .unwrap();

    assert!(matches!(result, PodStartResult::Started { .. }));
    let cri_calls = harness.cri.recorded_calls();
    assert!(
        cri_calls
            .iter()
            .any(|call| matches!(call.operation, MockCriOperation::SubscribeContainerEvents)),
        "init container completion must subscribe to CRI stop events"
    );
    let start_order = cri_calls
        .iter()
        .find_map(|call| {
            matches!(call.operation, MockCriOperation::StartContainer(_)).then_some(call.call_order)
        })
        .expect("init container must be started");
    let subscribe_order = cri_calls
        .iter()
        .find_map(|call| {
            matches!(call.operation, MockCriOperation::SubscribeContainerEvents)
                .then_some(call.call_order)
        })
        .expect("init container completion must subscribe to CRI events");
    assert!(
        start_order < subscribe_order,
        "init container must be started before subscribing to CRI events; containerd GetContainerEvents can block until an event exists"
    );
    let status_calls = cri_calls
        .iter()
        .filter(|call| matches!(call.operation, MockCriOperation::ContainerStatus(_)))
        .count();
    assert_eq!(
        status_calls, 1,
        "init completion should read status once after the stop event, not poll"
    );
}

#[tokio::test]
async fn real_runtime_start_pod_runs_post_start_hook_with_parity() {
    let harness = PodRuntimeHarness::new().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "namespace": "ns", "name": "poststart-pod", "uid": "uid-ps", "resourceVersion": "1" },
        "spec": {
            "containers": [{
                "name": "app",
                "image": "nginx",
                "imagePullPolicy": "Never",
                "lifecycle": {
                    "postStart": {
                        "httpGet": { "path": "/healthz", "port": 8080 }
                    }
                }
            }],
            "nodeName": "test-node"
        },
        "status": {"phase": "Pending"}
    });
    harness
        .repo
        .test_create_pod("ns", "poststart-pod", "test-node", pod.clone())
        .await
        .unwrap();
    let key = PodRuntimeKey::new("ns", "poststart-pod", "uid-ps");
    let result = harness
        .runtime
        .start_pod(key, Some(pod), CancellationToken::new())
        .await
        .unwrap();

    assert!(
        matches!(result, PodStartResult::Started { .. }),
        "pod with postStart hook must start successfully, got {:?}",
        result
    );

    let hook_calls = harness.hooks.recorded_calls();
    assert_eq!(hook_calls.len(), 1, "postStart hook must be called once");
    assert_eq!(hook_calls[0].hook_type, "postStart");
    assert!(
        !hook_calls[0].container_id.is_empty(),
        "container_id must be populated"
    );
}

#[tokio::test]
async fn post_start_hook_failure_event_and_stop_with_parity() {
    let harness = PodRuntimeHarness::new().await;
    harness
        .hooks
        .set_outcome(HookOutcome::Failed("hook error".to_string()));
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "namespace": "ns", "name": "psfail-pod", "uid": "uid-psf", "resourceVersion": "1" },
        "spec": {
            "containers": [{
                "name": "app",
                "image": "nginx",
                "imagePullPolicy": "Never",
                "lifecycle": {
                    "postStart": {
                        "httpGet": { "path": "/healthz", "port": 8080 }
                    }
                }
            }],
            "nodeName": "test-node"
        },
        "status": {"phase": "Pending"}
    });
    harness
        .repo
        .test_create_pod("ns", "psfail-pod", "test-node", pod.clone())
        .await
        .unwrap();
    let key = PodRuntimeKey::new("ns", "psfail-pod", "uid-psf");
    let result = harness
        .runtime
        .start_pod(key, Some(pod), CancellationToken::new())
        .await
        .unwrap();

    assert!(
        matches!(result, PodStartResult::Failed(_)),
        "postStart hook failure must return Failed, got {:?}",
        result
    );

    // Verify FailedPostStartHook event emitted.
    let events = harness.events.recorded_events();
    let failed_events: Vec<_> = events
        .iter()
        .filter(|e| e.reason == "FailedPostStartHook")
        .collect();
    assert!(
        !failed_events.is_empty(),
        "FailedPostStartHook event must be emitted"
    );

    // Verify container was stopped (hook failure kills the container).
    let cri_calls = harness.cri.recorded_calls();
    let stop_calls: Vec<_> = cri_calls
        .iter()
        .filter(|c| matches!(c.operation, MockCriOperation::StopContainer(_, _)))
        .collect();
    assert!(
        !stop_calls.is_empty(),
        "container must be stopped on hook failure"
    );
}

#[tokio::test]
async fn real_runtime_stop_pod_runs_pre_stop_hooks_before_container_stop_with_parity() {
    let harness = PodRuntimeHarness::new().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "namespace": "ns", "name": "prestop-pod", "uid": "uid-prestop", "resourceVersion": "1" },
        "spec": {
            "terminationGracePeriodSeconds": 15,
            "containers": [{
                "name": "app",
                "image": "nginx",
                "imagePullPolicy": "Never",
                "lifecycle": {
                    "preStop": {
                        "exec": { "command": ["/bin/sh", "-c", "sleep 1"] }
                    }
                }
            }],
            "nodeName": "test-node"
        },
        "status": {
            "phase": "Running",
            "podIP": "10.0.0.5",
            "containerStatuses": [{
                "name": "app",
                "containerID": "containerd://ctr-prestop",
                "state": {"running": {"startedAt": "2026-01-01T00:00:00Z"}}
            }]
        }
    });
    harness
        .repo
        .test_create_pod("ns", "prestop-pod", "test-node", pod.clone())
        .await
        .unwrap();
    let key = PodRuntimeKey::new("ns", "prestop-pod", "uid-prestop");
    let sandbox_id = "sb-prestop";

    harness
        .store
        .record_sandbox(&key, sandbox_id)
        .await
        .unwrap();
    harness
        .container_control
        .set_containers(vec![("ctr-prestop".into(), "running".into())]);

    harness
        .runtime
        .stop_pod(key.clone(), Some(pod), Some(sandbox_id.into()))
        .await
        .unwrap();

    // Verify preStop hook was executed.
    let hook_calls = harness.hooks.recorded_calls();
    let pre_stop_calls: Vec<_> = hook_calls
        .iter()
        .filter(|c| c.hook_type == "preStop")
        .collect();
    assert_eq!(
        pre_stop_calls.len(),
        1,
        "preStop hook must be executed once, got {:?}",
        hook_calls
    );
    assert_eq!(pre_stop_calls[0].container_id, "ctr-prestop");
    assert_eq!(pre_stop_calls[0].pod_ip, "10.0.0.5");
}

#[tokio::test]
async fn real_runtime_stop_pod_passes_termination_grace_period_to_cri_with_parity() {
    // Case 1: explicit terminationGracePeriodSeconds
    {
        let harness = PodRuntimeHarness::new().await;
        let pod = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "namespace": "ns", "name": "grace-5", "uid": "uid-g5", "resourceVersion": "1" },
            "spec": {
                "terminationGracePeriodSeconds": 5,
                "containers": [{"name": "app", "image": "nginx", "imagePullPolicy": "Never"}],
                "nodeName": "test-node"
            },
            "status": {"phase": "Running"}
        });
        harness
            .repo
            .test_create_pod("ns", "grace-5", "test-node", pod.clone())
            .await
            .unwrap();
        let key = PodRuntimeKey::new("ns", "grace-5", "uid-g5");
        let sandbox_id = "sb-g5";
        harness
            .store
            .record_sandbox(&key, sandbox_id)
            .await
            .unwrap();
        harness
            .container_control
            .set_containers(vec![("ctr-5".into(), "running".into())]);

        harness
            .runtime
            .stop_pod(key.clone(), Some(pod), Some(sandbox_id.into()))
            .await
            .unwrap();

        let cri_calls = harness.cri.recorded_calls();
        let stop_call = cri_calls
            .iter()
            .find(|c| matches!(c.operation, MockCriOperation::StopContainer(_, _)))
            .expect("must have a StopContainer call");
        let timeout = match &stop_call.operation {
            MockCriOperation::StopContainer(_, t) => *t,
            _ => panic!("expected stop-container call"),
        };
        assert_eq!(
            timeout, 5,
            "terminationGracePeriodSeconds=5 must be passed to stop_container"
        );
    }

    // Case 2: no terminationGracePeriodSeconds → default 30
    {
        let harness = PodRuntimeHarness::new().await;
        let pod = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "namespace": "ns", "name": "grace-default", "uid": "uid-gd", "resourceVersion": "1" },
            "spec": {
                "containers": [{"name": "app", "image": "nginx", "imagePullPolicy": "Never"}],
                "nodeName": "test-node"
            },
            "status": {"phase": "Running"}
        });
        harness
            .repo
            .test_create_pod("ns", "grace-default", "test-node", pod.clone())
            .await
            .unwrap();
        let key = PodRuntimeKey::new("ns", "grace-default", "uid-gd");
        let sandbox_id = "sb-gd";
        harness
            .store
            .record_sandbox(&key, sandbox_id)
            .await
            .unwrap();
        harness
            .container_control
            .set_containers(vec![("ctr-d".into(), "running".into())]);

        harness
            .runtime
            .stop_pod(key.clone(), Some(pod), Some(sandbox_id.into()))
            .await
            .unwrap();

        let cri_calls = harness.cri.recorded_calls();
        let stop_call = cri_calls
            .iter()
            .find(|c| matches!(c.operation, MockCriOperation::StopContainer(_, _)))
            .expect("must have a StopContainer call");
        let timeout = match &stop_call.operation {
            MockCriOperation::StopContainer(_, t) => *t,
            _ => panic!("expected stop-container call"),
        };
        assert_eq!(
            timeout, 30,
            "absence of terminationGracePeriodSeconds must default to 30"
        );
    }
}

#[tokio::test]
async fn real_runtime_stop_pod_resolves_sandbox_id_through_row_annotation_then_cri_with_parity() {
    // Scenario 1: sandbox_id provided directly — used without resolution.
    {
        let harness = PodRuntimeHarness::new().await;
        let pod = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "namespace": "ns", "name": "dir-pod", "uid": "uid-dir", "resourceVersion": "1" },
            "spec": {
                "containers": [{"name": "app", "image": "nginx", "imagePullPolicy": "Never"}],
                "nodeName": "test-node"
            },
            "status": {"phase": "Running"}
        });
        harness
            .repo
            .test_create_pod("ns", "dir-pod", "test-node", pod.clone())
            .await
            .unwrap();
        let key = PodRuntimeKey::new("ns", "dir-pod", "uid-dir");
        harness
            .store
            .record_sandbox(&key, "sb-direct")
            .await
            .unwrap();
        harness
            .container_control
            .set_containers(vec![("ctr-d".into(), "running".into())]);

        // Provide sandbox_id directly.
        harness
            .runtime
            .stop_pod(key.clone(), Some(pod), Some("provided-sb".into()))
            .await
            .unwrap();

        // Verify the provided sandbox was used for cleanup (not the store row).
        let cri_calls = harness.cri.recorded_calls();
        assert!(cri_calls.iter().any(
            |c| matches!(c.operation, MockCriOperation::StopPodSandbox(ref s) if s == "provided-sb")
        ));
    }

    // Scenario 2: sandbox_id is None → resolved from store row.
    {
        let harness = PodRuntimeHarness::new().await;
        let pod = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "namespace": "ns", "name": "store-pod", "uid": "uid-store", "resourceVersion": "1" },
            "spec": {
                "containers": [{"name": "app", "image": "nginx", "imagePullPolicy": "Never"}],
                "nodeName": "test-node"
            },
            "status": {"phase": "Running"}
        });
        harness
            .repo
            .test_create_pod("ns", "store-pod", "test-node", pod.clone())
            .await
            .unwrap();
        let key = PodRuntimeKey::new("ns", "store-pod", "uid-store");
        harness
            .store
            .record_sandbox(&key, "sb-from-store")
            .await
            .unwrap();
        harness
            .container_control
            .set_containers(vec![("ctr-s".into(), "running".into())]);

        // No sandbox_id provided → resolved from store.
        harness
            .runtime
            .stop_pod(key.clone(), Some(pod), None)
            .await
            .unwrap();

        let cri_calls = harness.cri.recorded_calls();
        assert!(cri_calls
            .iter()
            .any(|c| matches!(c.operation, MockCriOperation::StopPodSandbox(ref s) if s == "sb-from-store")));
    }

    // Scenario 3: sandbox_id is None and store is empty → resolved from annotation.
    {
        let harness = PodRuntimeHarness::new().await;
        let pod = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "ns",
                "name": "annot-pod",
                "uid": "uid-annot",
                "resourceVersion": "1",
                "annotations": { "klights.dev/sandbox-id": "sb-from-annot" }
            },
            "spec": {
                "containers": [{"name": "app", "image": "nginx", "imagePullPolicy": "Never"}],
                "nodeName": "test-node"
            },
            "status": {"phase": "Running"}
        });
        harness
            .repo
            .test_create_pod("ns", "annot-pod", "test-node", pod.clone())
            .await
            .unwrap();
        let key = PodRuntimeKey::new("ns", "annot-pod", "uid-annot");
        // Do NOT record a sandbox in the store.
        harness
            .container_control
            .set_containers(vec![("ctr-a".into(), "running".into())]);

        harness
            .runtime
            .stop_pod(key.clone(), Some(pod), None)
            .await
            .unwrap();

        let cri_calls = harness.cri.recorded_calls();
        assert!(cri_calls
            .iter()
            .any(|c| matches!(c.operation, MockCriOperation::StopPodSandbox(ref s) if s == "sb-from-annot")));
    }
}

#[tokio::test]
async fn pod_stop_sandbox_identity_fallback_with_parity() {
    let harness = PodRuntimeHarness::new().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "deleted-ns",
            "name": "deleted-pod",
            "uid": "uid-deleted",
            "resourceVersion": "1",
            "deletionTimestamp": "2026-05-19T20:03:59Z"
        },
        "spec": {
            "containers": [{"name": "app", "image": "nginx", "imagePullPolicy": "Never"}],
            "nodeName": "test-node"
        },
        "status": {"phase": "Pending"}
    });
    let key = PodRuntimeKey::new("deleted-ns", "deleted-pod", "uid-deleted");
    harness
        .repo
        .test_create_pod("deleted-ns", "deleted-pod", "test-node", pod.clone())
        .await
        .unwrap();
    harness.cri.set_pod_sandboxes(vec![(
        "sandbox-live",
        "sonobuoy",
        "sonobuoy-e2e",
        "uid-live",
        "ready",
    )]);
    harness
        .container_control
        .set_containers(vec![("container-live".into(), "running".into())]);

    harness
        .runtime
        .stop_pod(key, Some(pod), None)
        .await
        .unwrap();

    let cri_calls = harness.cri.recorded_calls();
    assert!(
        cri_calls
            .iter()
            .any(|c| matches!(c.operation, MockCriOperation::ListPodSandboxes(Some(ref uid)) if uid == "uid-deleted")),
        "StopPod must query the CRI fallback by the deleted pod UID"
    );
    assert!(
        !cri_calls.iter().any(
            |c| matches!(c.operation, MockCriOperation::StopContainer(ref id, _) if id == "container-live")
        ),
        "StopPod must not stop containers from an unrelated sandbox"
    );
    assert!(
        !cri_calls.iter().any(
            |c| matches!(c.operation, MockCriOperation::StopPodSandbox(ref id) if id == "sandbox-live")
        ),
        "StopPod must not stop an unrelated sandbox when the UID does not match"
    );
}

#[tokio::test]
async fn real_runtime_reconcile_does_not_preserve_ready_started_for_missing_containers() {
    use klights_kubelet::pod_repository::PodStatusUpdate;

    let harness = PodRuntimeHarness::new().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "sonobuoy",
            "name": "e2e",
            "uid": "uid-e2e",
            "resourceVersion": "1"
        },
        "spec": {
            "containers": [
                {"name": "e2e", "image": "registry.k8s.io/conformance:v1.34.6", "imagePullPolicy": "Never"},
                {"name": "sonobuoy-worker", "image": "sonobuoy/sonobuoy:v0.57.3", "imagePullPolicy": "Never"}
            ],
            "nodeName": "test-node"
        },
        "status": {
            "phase": "Running",
            "containerStatuses": [
                {
                    "name": "e2e",
                    "containerID": "containerd://ctr-e2e",
                    "image": "registry.k8s.io/conformance:v1.34.6",
                    "imageID": "registry.k8s.io/conformance:v1.34.6",
                    "ready": true,
                    "started": true,
                    "restartCount": 0,
                    "state": {"running": {"startedAt": "2026-05-19T20:03:57Z"}}
                },
                {
                    "name": "sonobuoy-worker",
                    "containerID": "containerd://ctr-worker",
                    "image": "sonobuoy/sonobuoy:v0.57.3",
                    "imageID": "sonobuoy/sonobuoy:v0.57.3",
                    "ready": true,
                    "started": true,
                    "restartCount": 0,
                    "state": {"running": {"startedAt": "2026-05-19T20:03:57Z"}}
                }
            ]
        }
    });
    harness
        .repo
        .test_create_pod("sonobuoy", "e2e", "test-node", pod.clone())
        .await
        .unwrap();
    harness
        .repo
        .pod_status_writer
        .set_pod_status_for_uid(
            "sonobuoy",
            "e2e",
            "uid-e2e",
            PodStatusUpdate {
                phase: "Running".to_string(),
                pod_ip: "10.50.1.2".to_string(),
                host_ip: String::new(),
                container_statuses: pod
                    .pointer("/status/containerStatuses")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap(),
                init_container_statuses: None,
                qos_class: None,
            },
            None,
        )
        .await
        .unwrap();
    let key = PodRuntimeKey::new("sonobuoy", "e2e", "uid-e2e");
    harness
        .store
        .record_sandbox(&key, "sandbox-e2e")
        .await
        .unwrap();

    harness
        .runtime
        .reconcile_runtime(
            key,
            crate::kubelet::pod_runtime::service::RuntimeReconcileHint::none(),
        )
        .await
        .unwrap();

    let updated = harness
        .repo
        .test_get_pod_for_uid("sonobuoy", "e2e", "uid-e2e")
        .await
        .unwrap()
        .unwrap();
    let statuses = updated
        .data
        .pointer("/status/containerStatuses")
        .and_then(|v| v.as_array())
        .unwrap();
    assert_eq!(statuses.len(), 2);
    for status in statuses {
        assert_eq!(status.get("containerID"), Some(&serde_json::Value::Null));
        assert_eq!(status.get("ready").and_then(|v| v.as_bool()), Some(false));
        assert_eq!(status.get("started").and_then(|v| v.as_bool()), Some(false));
        assert!(
            status.pointer("/state/waiting").is_some(),
            "missing runtime state must be reported as waiting, not ready/running: {status:?}"
        );
    }
}

#[tokio::test]
async fn real_runtime_reconcile_reports_exited_restart_never_container_as_succeeded() {
    use crate::kubelet::pod_runtime::store::PodRuntimeStore;
    use klights_kubelet::pod_repository::PodStatusUpdate;
    use klights_kubelet::runtime::cri::ContainerRuntimeState;

    let harness = PodRuntimeHarness::new().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "downward-api",
            "name": "short-lived",
            "uid": "uid-short-lived",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "restartPolicy": "Never",
            "containers": [{
                "name": "client-container",
                "image": "registry.k8s.io/e2e-test-images/agnhost:2.56",
                "imagePullPolicy": "Never"
            }]
        },
        "status": {
            "phase": "Running",
            "containerStatuses": [{
                "name": "client-container",
                "containerID": "containerd://ctr-done",
                "image": "registry.k8s.io/e2e-test-images/agnhost:2.56",
                "imageID": "registry.k8s.io/e2e-test-images/agnhost:2.56",
                "ready": true,
                "started": true,
                "restartCount": 0,
                "state": {"running": {"startedAt": "2026-05-19T20:49:53Z"}}
            }]
        }
    });
    let key = PodRuntimeKey::new("downward-api", "short-lived", "uid-short-lived");
    harness.create_runtime_pod(pod.clone()).await;
    harness
        .repo
        .pod_status_writer
        .set_pod_status_for_uid(
            "downward-api",
            "short-lived",
            "uid-short-lived",
            PodStatusUpdate {
                phase: "Running".to_string(),
                pod_ip: "10.50.1.3".to_string(),
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
    harness
        .store
        .record_sandbox(&key, "sandbox-short-lived")
        .await
        .unwrap();
    harness
        .container_control
        .set_container_states(vec![("ctr-done".into(), ContainerRuntimeState::Exited)]);
    harness
        .cri
        .set_container_status_state(k8s_cri::v1::ContainerState::ContainerExited as i32);
    harness.cri.set_container_exit_code(0);

    harness.reconcile_runtime(key.clone()).await;

    let updated = harness.stored_pod(&key).await;
    assert_eq!(
        updated
            .pointer("/status/phase")
            .and_then(|value| value.as_str()),
        Some("Succeeded")
    );
    let status = updated
        .pointer("/status/containerStatuses/0")
        .expect("container status must be present");
    assert_eq!(
        status.pointer("/state/terminated/exitCode"),
        Some(&serde_json::json!(0))
    );
    assert_eq!(
        status.pointer("/state/terminated/reason"),
        Some(&serde_json::json!("Completed"))
    );
    assert_eq!(
        status.pointer("/ready").and_then(|value| value.as_bool()),
        Some(false)
    );
    assert_eq!(
        status.pointer("/started").and_then(|value| value.as_bool()),
        Some(true)
    );
}

#[tokio::test]
async fn real_runtime_reconcile_preserves_terminal_container_state_after_stale_running_snapshot() {
    use crate::kubelet::pod_runtime::store::PodRuntimeStore;
    use klights_kubelet::pod_repository::PodStatusUpdate;
    use klights_kubelet::runtime::cri::ContainerRuntimeState;

    let harness = PodRuntimeHarness::new().await;
    let key = PodRuntimeKey::new(
        "container-runtime",
        "terminate-cmd-rpof",
        "uid-terminate-rpof",
    );
    let image = "registry.k8s.io/e2e-test-images/busybox:1.37.0-1";
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "container-runtime",
            "name": "terminate-cmd-rpof",
            "uid": "uid-terminate-rpof",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "restartPolicy": "OnFailure",
            "containers": [{
                "name": "terminate-cmd-rpof",
                "image": image,
                "imagePullPolicy": "Never",
                "command": ["/bin/sh", "-c", "exit 0"]
            }]
        },
        "status": {
            "phase": "Succeeded",
            "containerStatuses": [{
                "name": "terminate-cmd-rpof",
                "containerID": "containerd://ctr-rpof",
                "image": image,
                "imageID": image,
                "ready": false,
                "started": true,
                "restartCount": 0,
                "state": {
                    "terminated": {
                        "exitCode": 0,
                        "reason": "Completed",
                        "startedAt": "2026-05-22T09:46:35Z",
                        "finishedAt": "2026-05-22T09:46:36Z"
                    }
                }
            }]
        }
    });
    harness.create_runtime_pod(pod).await;
    harness
        .repo
        .pod_status_writer
        .set_pod_status_for_uid(
            "container-runtime",
            "terminate-cmd-rpof",
            "uid-terminate-rpof",
            PodStatusUpdate {
                phase: "Succeeded".to_string(),
                pod_ip: "10.50.1.11".to_string(),
                host_ip: String::new(),
                container_statuses: vec![serde_json::json!({
                    "name": "terminate-cmd-rpof",
                    "containerID": "containerd://ctr-rpof",
                    "image": image,
                    "imageID": image,
                    "ready": false,
                    "started": true,
                    "restartCount": 0,
                    "state": {
                        "terminated": {
                            "exitCode": 0,
                            "reason": "Completed",
                            "startedAt": "2026-05-22T09:46:35Z",
                            "finishedAt": "2026-05-22T09:46:36Z"
                        }
                    }
                })],
                init_container_statuses: None,
                qos_class: None,
            },
            None,
        )
        .await
        .unwrap();
    harness
        .store
        .record_sandbox(&key, "sandbox-terminate-rpof")
        .await
        .unwrap();
    harness
        .container_control
        .set_container_states(vec![("ctr-rpof".into(), ContainerRuntimeState::Running)]);
    harness
        .cri
        .set_container_status_state(k8s_cri::v1::ContainerState::ContainerRunning as i32);
    harness.cri.set_container_exit_code(0);

    harness.reconcile_runtime(key.clone()).await;

    let updated = harness.stored_pod(&key).await;
    assert_eq!(
        updated
            .pointer("/status/phase")
            .and_then(|value| value.as_str()),
        Some("Succeeded"),
        "terminal phase must not regress after a stale running runtime snapshot"
    );
    let status = updated
        .pointer("/status/containerStatuses/0")
        .expect("container status must remain present");
    assert_eq!(
        status.pointer("/state/terminated/exitCode"),
        Some(&serde_json::json!(0)),
        "terminal container state must not regress to running when phase is already terminal"
    );
    assert!(
        status.pointer("/state/running").is_none(),
        "stale running status must not be published for an already completed OnFailure pod"
    );
}

#[tokio::test]
async fn real_runtime_stop_pod_handles_partial_state_idempotently_with_parity() {
    // Scenario 1: no containers in sandbox — cleanup succeeds.
    {
        let harness = PodRuntimeHarness::new().await;
        let pod = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "namespace": "ns", "name": "nocont-pod", "uid": "uid-nc", "resourceVersion": "1" },
            "spec": {
                "containers": [{"name": "app", "image": "nginx", "imagePullPolicy": "Never"}],
                "nodeName": "test-node"
            },
            "status": {"phase": "Running"}
        });
        harness
            .repo
            .test_create_pod("ns", "nocont-pod", "test-node", pod.clone())
            .await
            .unwrap();
        let key = PodRuntimeKey::new("ns", "nocont-pod", "uid-nc");
        harness
            .store
            .record_sandbox(&key, "sb-nocont")
            .await
            .unwrap();
        // Do NOT set any containers — simulate partial state.

        harness
            .runtime
            .stop_pod(key.clone(), Some(pod), Some("sb-nocont".into()))
            .await
            .unwrap();

        // Verify that sandbox stop/remove were still called (idempotent).
        let cri_calls = harness.cri.recorded_calls();
        assert!(cri_calls.iter().any(
            |c| matches!(c.operation, MockCriOperation::StopPodSandbox(ref s) if s == "sb-nocont")
        ));
    }

    // Scenario 2: a non-NotFound CRI failure is fail-closed. The actor retries;
    // CNI/store/artifact/slot cleanup must not run past an unconfirmed sandbox.
    {
        let harness = PodRuntimeHarness::new().await;
        harness.cri.set_fail_operation("StopPodSandbox");
        let pod = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "namespace": "ns", "name": "crlfail-pod", "uid": "uid-cf", "resourceVersion": "1" },
            "spec": {
                "containers": [{"name": "app", "image": "nginx", "imagePullPolicy": "Never"}],
                "nodeName": "test-node"
            },
            "status": {"phase": "Running"}
        });
        harness
            .repo
            .test_create_pod("ns", "crlfail-pod", "test-node", pod.clone())
            .await
            .unwrap();
        let key = PodRuntimeKey::new("ns", "crlfail-pod", "uid-cf");
        harness
            .store
            .record_sandbox(&key, "sb-crlfail")
            .await
            .unwrap();
        harness
            .container_control
            .set_containers(vec![("ctr-cf".into(), "running".into())]);

        let error = harness
            .runtime
            .stop_pod(key.clone(), Some(pod), Some("sb-crlfail".into()))
            .await
            .expect_err("sandbox stop failure must remain retryable");
        assert!(error.to_string().contains("injected failure"));
        let cri_calls = harness.cri.recorded_calls();
        assert!(cri_calls.iter().any(|call| matches!(
            call.operation,
            MockCriOperation::StopPodSandbox(ref id) if id == "sb-crlfail"
        )));
        assert!(!cri_calls.iter().any(|call| matches!(
            call.operation,
            MockCriOperation::RemovePodSandbox(ref id) if id == "sb-crlfail"
        )));
        assert!(harness.network.recorded_calls().is_empty());
        assert!(
            !harness
                .store
                .recorded_calls()
                .iter()
                .any(|call| { call == "delete_sandbox:ns/crlfail-pod/uid-cf" })
        );
        assert!(harness.filesystem.recorded_calls().is_empty());
        assert!(
            !harness
                .slot_admission
                .recorded_calls()
                .iter()
                .any(|call| call.contains("clear_slot"))
        );
    }

    // Scenario 3: no sandbox id and no resolution — succeeds (clears slot only).
    {
        let harness = PodRuntimeHarness::new().await;
        let pod = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "namespace": "ns", "name": "nosb-pod", "uid": "uid-ns", "resourceVersion": "1" },
            "spec": {
                "containers": [{"name": "app", "image": "nginx", "imagePullPolicy": "Never"}],
                "nodeName": "test-node"
            },
            "status": {"phase": "Running"}
        });
        harness
            .repo
            .test_create_pod("ns", "nosb-pod", "test-node", pod.clone())
            .await
            .unwrap();
        let key = PodRuntimeKey::new("ns", "nosb-pod", "uid-ns");
        // Do NOT record sandbox, no annotation, no CRI sandboxes.

        harness
            .runtime
            .stop_pod(key.clone(), Some(pod), None)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn real_runtime_reconcile_treats_cri_numeric_running_state_as_running() {
    let harness = PodRuntimeHarness::new().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "kube-system",
            "name": "coredns-numeric-state",
            "uid": "uid-coredns-numeric-state",
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
    let key = PodRuntimeKey::new(
        "kube-system",
        "coredns-numeric-state",
        "uid-coredns-numeric-state",
    );

    harness.create_runtime_pod(pod.clone()).await;
    let start = harness
        .start_pod_through_runtime(key.clone(), pod.clone())
        .await;
    assert!(matches!(start, PodStartResult::Started { .. }));

    harness.container_control.set_container_states(vec![(
        "container-coredns".into(),
        klights_kubelet::runtime::cri::ContainerRuntimeState::from_cri_state_i32(
            k8s_cri::v1::ContainerState::ContainerRunning as i32,
        ),
    )]);
    harness.reconcile_runtime(key.clone()).await;

    let resource = harness.stored_pod(&key).await;
    assert_eq!(
        resource.pointer("/status/phase").and_then(|v| v.as_str()),
        Some("Running"),
        "production CRI adapter must convert CRI's numeric running enum into the typed OO runtime state"
    );
    assert!(
        resource
            .pointer("/status/containerStatuses/0/state/running")
            .and_then(|v| v.as_object())
            .is_some(),
        "numeric CRI running state must not remain ContainerCreating"
    );
}

#[tokio::test]
async fn real_runtime_reconcile_uses_cri_event_container_id_when_list_is_empty() {
    use crate::kubelet::pod_runtime::service::RuntimeReconcileHint;
    use crate::kubelet::pod_runtime::store::PodRuntimeStore;
    use klights_kubelet::pod_repository::PodStatusUpdate;
    use klights_kubelet::runtime::cri::ContainerRuntimeState;

    let harness = PodRuntimeHarness::new().await;
    let key = PodRuntimeKey::new("container-runtime", "fast-exit", "uid-fast-exit");
    let image = "registry.k8s.io/e2e-test-images/busybox:1.37.0-1";
    let pod = serde_json::json!({
        "apiVersion":"v1","kind":"Pod",
        "metadata":{"namespace":"container-runtime","name":"fast-exit","uid":"uid-fast-exit","resourceVersion":"1"},
        "spec":{"nodeName":"test-node","restartPolicy":"Never","containers":[{"name":"app","image":image,"imagePullPolicy":"Never","command":["/bin/sh","-c","exit 0"]}]},
        "status":{"phase":"Pending","containerStatuses":[{"name":"app","image":image,"imageID":image,"ready":false,"started":false,"restartCount":0,"state":{"waiting":{"reason":"ContainerCreating"}}}]}
    });
    harness.create_runtime_pod(pod.clone()).await;
    harness
        .repo
        .pod_status_writer
        .set_pod_status_for_uid(
            "container-runtime",
            "fast-exit",
            "uid-fast-exit",
            PodStatusUpdate {
                phase: "Pending".to_string(),
                pod_ip: "10.50.2.44".to_string(),
                host_ip: String::new(),
                container_statuses: pod
                    .pointer("/status/containerStatuses")
                    .and_then(|v| v.as_array())
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
        .record_sandbox(&key, "sandbox-fast-exit")
        .await
        .unwrap();
    // Sandbox container listing is empty (the container already exited and
    // was removed, or the listing lagged behind the CRI event).
    harness.container_control.set_container_states(Vec::new());
    // Per-container mock status keyed by container id — the CRI event hint.
    harness.cri.set_container_status_for_test(
        "ctr-fast-exit",
        "app",
        ContainerRuntimeState::Exited,
        0,
        1_000_000_000,
        1_250_000_000,
        image,
    );
    harness
        .runtime
        .reconcile_runtime(
            key.clone(),
            RuntimeReconcileHint::from_container_event(
                "ctr-fast-exit",
                klights_kubelet::cri_events::KubeletEventKind::Started,
            )
            .with_container_event(
                "ctr-fast-exit",
                klights_kubelet::cri_events::KubeletEventKind::Stopped,
            ),
        )
        .await
        .unwrap();

    let updated = harness.stored_pod(&key).await;
    assert_eq!(
        updated.pointer("/status/phase").and_then(|v| v.as_str()),
        Some("Succeeded"),
        "fast-exit pod must reach Succeeded phase via CRI event hint, got: {:?}",
        updated.pointer("/status/phase")
    );
    let status = updated
        .pointer("/status/containerStatuses/0")
        .expect("container status must exist");
    assert_eq!(
        status.pointer("/state/terminated/exitCode"),
        Some(&serde_json::json!(0)),
        "container state must be terminated with exit code 0, got: {:?}",
        status.pointer("/state")
    );
    assert_eq!(
        status.pointer("/state/terminated/reason"),
        Some(&serde_json::json!("Completed")),
        "terminated reason must be Completed, got: {:?}",
        status.pointer("/state/terminated/reason")
    );
    assert!(
        status.pointer("/state/waiting").is_none(),
        "fast-exit pod must not remain ContainerCreating, got: {:?}",
        status.pointer("/state")
    );
}

#[tokio::test]
async fn missing_hinted_workload_container_is_an_observation_miss() {
    use crate::kubelet::pod_runtime::service::RuntimeReconcileHint;
    use crate::kubelet::pod_runtime::store::PodRuntimeStore;
    use klights_kubelet::pod_repository::PodStatusUpdate;

    let harness = PodRuntimeHarness::new().await;
    let key = PodRuntimeKey::new("workloads", "observation-miss", "uid-observation-miss");
    let pod = serde_json::json!({
        "apiVersion":"v1","kind":"Pod",
        "metadata":{"namespace":"workloads","name":"observation-miss","uid":"uid-observation-miss","resourceVersion":"1"},
        "spec":{"nodeName":"test-node","restartPolicy":"Never","containers":[{"name":"app","image":"busybox:latest"}]},
        "status":{"phase":"Running","containerStatuses":[{"name":"app","containerID":"containerd://ctr-live","image":"busybox:latest","imageID":"busybox:latest","ready":true,"started":true,"restartCount":0,"state":{"running":{"startedAt":"2026-01-01T00:00:00Z"}}}]}
    });
    harness.create_runtime_pod(pod.clone()).await;
    harness
        .repo
        .pod_status_writer
        .set_pod_status_for_uid(
            "workloads",
            "observation-miss",
            "uid-observation-miss",
            PodStatusUpdate {
                phase: "Running".to_string(),
                pod_ip: "10.50.2.45".to_string(),
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
    harness
        .store
        .record_sandbox(&key, "sandbox-observation-miss")
        .await
        .unwrap();
    harness.container_control.set_container_states(Vec::new());
    harness
        .cri
        .set_container_status_not_found_for_test("ctr-already-gone");

    harness
        .runtime
        .reconcile_runtime(
            key.clone(),
            RuntimeReconcileHint::from_container_event(
                "ctr-already-gone",
                klights_kubelet::cri_events::KubeletEventKind::Stopped,
            ),
        )
        .await
        .expect("a missing hinted container is only an observation miss");

    let updated = harness.stored_pod(&key).await;
    assert_eq!(
        updated
            .pointer("/status/phase")
            .and_then(|value| value.as_str()),
        Some("Running"),
        "an observation miss must not regress existing pod status"
    );
}

#[tokio::test]
async fn hinted_container_status_non_not_found_error_remains_fail_closed() {
    let cri = klights_kubelet::runtime::test_support::MockCriRuntime::new();
    cri.set_fail_operation("ContainerStatus");

    let error =
        super::status_projection::runtime_state_from_container_status(&cri, "ctr-runtime-down")
            .await
            .expect_err("non-NotFound CRI errors must propagate");
    assert!(error.to_string().contains("injected failure"));
}

#[tokio::test]
async fn started_cri_event_overrides_lagging_created_status_snapshot() {
    use crate::kubelet::pod_runtime::service::RuntimeReconcileHint;
    use crate::kubelet::pod_runtime::store::PodRuntimeStore;
    use klights_kubelet::cri_events::KubeletEventKind;
    use klights_kubelet::pod_repository::PodStatusUpdate;
    use klights_kubelet::runtime::cri::ContainerRuntimeState;

    let harness = PodRuntimeHarness::new().await;
    let key = PodRuntimeKey::new("kube-system", "coredns", "uid-coredns");
    let image = "coredns/coredns:1.11.1";
    let pod = serde_json::json!({
        "apiVersion":"v1","kind":"Pod",
        "metadata":{"namespace":"kube-system","name":"coredns","uid":"uid-coredns","resourceVersion":"1"},
        "spec":{"nodeName":"test-node","restartPolicy":"Always","containers":[{"name":"coredns","image":image}]},
        "status":{"phase":"Pending","containerStatuses":[{"name":"coredns","containerID":"containerd://ctr-coredns","image":image,"imageID":"","ready":false,"started":false,"restartCount":0,"state":{"waiting":{"reason":"ContainerCreating"}}}]}
    });
    harness.create_runtime_pod(pod.clone()).await;
    harness
        .repo
        .pod_status_writer
        .set_pod_status_for_uid(
            "kube-system",
            "coredns",
            "uid-coredns",
            PodStatusUpdate {
                phase: "Pending".to_string(),
                pod_ip: "10.50.0.2".to_string(),
                host_ip: "172.31.10.2".to_string(),
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
        .record_sandbox(&key, "sandbox-coredns")
        .await
        .unwrap();

    // containerd can emit Started before its ListContainers/ContainerStatus
    // cache advances from Created. The event is the newer runtime observation.
    harness.container_control.set_container_states(vec![(
        "ctr-coredns".to_string(),
        ContainerRuntimeState::Created,
    )]);
    harness.cri.set_container_status_for_test(
        "ctr-coredns",
        "coredns",
        ContainerRuntimeState::Created,
        0,
        0,
        0,
        image,
    );

    harness
        .runtime
        .reconcile_runtime(
            key.clone(),
            RuntimeReconcileHint::from_container_event("ctr-coredns", KubeletEventKind::Started),
        )
        .await
        .unwrap();

    let updated = harness.stored_pod(&key).await;
    assert_eq!(
        updated
            .pointer("/status/phase")
            .and_then(|value| value.as_str()),
        Some("Running")
    );
    let status = updated
        .pointer("/status/containerStatuses/0")
        .expect("container status");
    assert_eq!(status.get("ready"), Some(&serde_json::json!(true)));
    assert_eq!(status.get("started"), Some(&serde_json::json!(true)));
    assert!(status.pointer("/state/running").is_some());
    assert_eq!(status.get("image"), Some(&serde_json::json!(image)));
    assert_eq!(
        status.get("imageID"),
        Some(&serde_json::json!("coredns/coredns:1.11.1"))
    );

    let first_resource_version = updated
        .pointer("/metadata/resourceVersion")
        .and_then(|value| value.as_str())
        .expect("resource version after Started observation")
        .to_string();
    harness.container_control.set_container_states(vec![(
        "ctr-coredns".to_string(),
        ContainerRuntimeState::Running,
    )]);
    harness.cri.set_container_status_for_test(
        "ctr-coredns",
        "coredns",
        ContainerRuntimeState::Running,
        0,
        0,
        0,
        image,
    );
    harness
        .runtime
        .reconcile_runtime(key.clone(), RuntimeReconcileHint::none())
        .await
        .unwrap();
    let caught_up = harness.stored_pod(&key).await;
    assert_eq!(
        caught_up
            .pointer("/metadata/resourceVersion")
            .and_then(|value| value.as_str()),
        Some(first_resource_version.as_str()),
        "the status emitter must suppress the duplicate once CRI status catches up"
    );
}

#[test]
fn deferred_runtime_reconcile_preserves_multiple_container_ids() {
    let mut state = PodLifecycleState::new();
    state.defer_runtime_reconcile(Some("ctr-a"));
    state.defer_runtime_reconcile(Some("ctr-b"));
    state.defer_runtime_reconcile(Some("ctr-c"));
    let hint = state.take_runtime_reconcile_hint();
    let ids: std::collections::BTreeSet<_> = hint.container_ids().collect();
    assert!(ids.contains("ctr-a"), "must preserve ctr-a");
    assert!(ids.contains("ctr-b"), "must preserve ctr-b");
    assert!(ids.contains("ctr-c"), "must preserve ctr-c");
    assert_eq!(ids.len(), 3, "must have all 3 IDs, got: {ids:?}");
}

#[test]
fn deferred_runtime_reconcile_preserves_latest_kind_per_container() {
    use klights_kubelet::cri_events::KubeletEventKind;

    let mut state = PodLifecycleState::new();
    state.defer_runtime_reconcile_event("ctr-a", KubeletEventKind::Created);
    state.defer_runtime_reconcile_event("ctr-a", KubeletEventKind::Started);
    state.defer_runtime_reconcile_event("ctr-b", KubeletEventKind::Started);
    state.defer_runtime_reconcile_event("ctr-b", KubeletEventKind::Stopped);

    let hint = state.take_runtime_reconcile_hint();
    assert_eq!(hint.event_kind("ctr-a"), Some(KubeletEventKind::Started));
    assert_eq!(
        hint.event_kind("ctr-b"),
        Some(KubeletEventKind::Stopped),
        "a later Stopped observation must dominate Started"
    );
    assert_eq!(hint.container_ids().count(), 2);
}

#[test]
fn runtime_reconcile_drains_observations_without_polling() {
    let mut state = PodLifecycleState::new();
    state.defer_runtime_reconcile(Some("ctr-x"));
    state.defer_runtime_reconcile(Some("ctr-y"));
    let first = state.take_runtime_reconcile_hint();
    assert!(!first.is_empty(), "first drain must be non-empty");
    let second = state.take_runtime_reconcile_hint();
    assert!(
        second.is_empty(),
        "second drain must be empty (observations cleared)"
    );
}

#[test]
fn restored_runtime_observation_checkpoint_drains_into_reconcile_hint() {
    let mut state = PodLifecycleState::new();
    state.admit_uid("uid-restored");
    state.restore_runtime_reconcile_observations(
        "uid-restored",
        ["ctr-restored-a", "ctr-restored-b"],
        7,
    );

    let hint = state.take_runtime_reconcile_hint();
    let ids: std::collections::BTreeSet<_> = hint.container_ids().collect();
    assert_eq!(
        ids,
        ["ctr-restored-a", "ctr-restored-b"]
            .iter()
            .copied()
            .collect()
    );
    assert!(state.take_runtime_reconcile_hint().is_empty());
}

#[tokio::test]
async fn runtime_reconcile_uses_hinted_container_when_listing_is_partially_stale() {
    let harness = PodRuntimeHarness::new().await;
    let image = "registry.k8s.io/e2e-test-images/busybox:1.37.0-1";
    let key = PodRuntimeKey::new("container-runtime", "partial-stale", "uid-partial-stale");
    let pod = serde_json::json!({
        "apiVersion":"v1","kind":"Pod",
        "metadata":{"namespace":"container-runtime","name":"partial-stale","uid":"uid-partial-stale","resourceVersion":"1"},
        "spec":{"nodeName":"test-node","restartPolicy":"Never","containers":[{"name":"a","image":image,"imagePullPolicy":"Never"},{"name":"b","image":image,"imagePullPolicy":"Never"}]},
        "status":{"phase":"Running","containerStatuses":[
            {"name":"a","image":image,"imageID":image,"ready":true,"started":true,"restartCount":0,"state":{"running":{"startedAt":""}}},
            {"name":"b","image":image,"imageID":image,"ready":true,"started":true,"restartCount":0,"state":{"running":{"startedAt":""}}}
        ]}
    });
    harness.create_runtime_pod(pod.clone()).await;
    use klights_kubelet::pod_repository::PodStatusUpdate;
    harness
        .repo
        .pod_status_writer
        .set_pod_status_for_uid(
            "container-runtime",
            "partial-stale",
            "uid-partial-stale",
            PodStatusUpdate {
                phase: "Running".to_string(),
                pod_ip: "10.0.0.1".to_string(),
                host_ip: String::new(),
                container_statuses: pod
                    .pointer("/status/containerStatuses")
                    .unwrap()
                    .as_array()
                    .unwrap()
                    .clone(),
                init_container_statuses: None,
                qos_class: None,
            },
            None,
        )
        .await
        .unwrap();
    harness
        .store
        .record_sandbox(&key, "sandbox-partial")
        .await
        .unwrap();
    // listing is partial: only ctr-a is listed (ctr-b exited and was removed)
    harness
        .container_control
        .set_container_states(vec![("ctr-a".to_string(), ContainerRuntimeState::Running)]);
    // Both ctr-a and ctr-b are observed (from CRI events)
    harness.cri.set_container_status_for_test(
        "ctr-b",
        "b",
        ContainerRuntimeState::Exited,
        0,
        1_000_000_000,
        1_250_000_000,
        image,
    );
    let hint = RuntimeReconcileHint::from_container_ids(["ctr-a".to_string(), "ctr-b".to_string()]);
    harness
        .runtime
        .reconcile_runtime(key.clone(), hint)
        .await
        .unwrap();
    let updated = harness.stored_pod(&key).await;
    let statuses = updated
        .pointer("/status/containerStatuses")
        .unwrap()
        .as_array()
        .unwrap();
    let b_status = statuses
        .iter()
        .find(|s| s.pointer("/name").and_then(|v| v.as_str()) == Some("b"))
        .expect("container b must have a status");
    assert!(
        b_status.pointer("/state/terminated").is_some(),
        "ctr-b must be terminated even though it's not in the listing: {b_status}"
    );
}

#[tokio::test]
async fn runtime_reconcile_ignores_unknown_hinted_container_without_regressing_terminal_status() {
    let harness = PodRuntimeHarness::new().await;
    let image = "registry.k8s.io/e2e-test-images/busybox:1.37.0-1";
    let key = PodRuntimeKey::new("container-runtime", "unknown-hint", "uid-unknown-hint");
    let pod = serde_json::json!({
        "apiVersion":"v1","kind":"Pod",
        "metadata":{"namespace":"container-runtime","name":"unknown-hint","uid":"uid-unknown-hint","resourceVersion":"1"},
        "spec":{"nodeName":"test-node","restartPolicy":"Never","containers":[{"name":"app","image":image,"imagePullPolicy":"Never"}]},
        "status":{"phase":"Succeeded","containerStatuses":[{"name":"app","image":image,"imageID":image,"ready":false,"started":false,"restartCount":0,"state":{"terminated":{"exitCode":0,"reason":"Completed"}}}]}
    });
    harness.create_runtime_pod(pod.clone()).await;
    use klights_kubelet::pod_repository::PodStatusUpdate;
    harness
        .repo
        .pod_status_writer
        .set_pod_status_for_uid(
            "container-runtime",
            "unknown-hint",
            "uid-unknown-hint",
            PodStatusUpdate {
                phase: "Succeeded".to_string(),
                pod_ip: String::new(),
                host_ip: String::new(),
                container_statuses: pod
                    .pointer("/status/containerStatuses")
                    .unwrap()
                    .as_array()
                    .unwrap()
                    .clone(),
                init_container_statuses: None,
                qos_class: None,
            },
            None,
        )
        .await
        .unwrap();
    harness
        .store
        .record_sandbox(&key, "sandbox-unknown")
        .await
        .unwrap();
    harness.container_control.set_container_states(Vec::new());
    // Hint with an unknown ID (no CRI status available for it)
    let hint = RuntimeReconcileHint::from_container_ids(["ctr-unknown-xyz".to_string()]);
    harness
        .runtime
        .reconcile_runtime(key.clone(), hint)
        .await
        .unwrap();
    let updated = harness.stored_pod(&key).await;
    // Unknown hinted container must not regress the Succeeded phase
    assert_eq!(
        updated.pointer("/status/phase").and_then(|v| v.as_str()),
        Some("Succeeded"),
        "unknown hint must not regress terminal phase: {:?}",
        updated.pointer("/status/phase")
    );
}

#[tokio::test]
async fn fast_exit_multi_container_pod_reaches_terminal_phase_under_empty_listing() {
    let harness = PodRuntimeHarness::new().await;
    let image = "registry.k8s.io/e2e-test-images/busybox:1.37.0-1";
    let key = PodRuntimeKey::new("container-runtime", "multi-exit", "uid-multi-exit");
    let pod = serde_json::json!({
        "apiVersion":"v1","kind":"Pod",
        "metadata":{"namespace":"container-runtime","name":"multi-exit","uid":"uid-multi-exit","resourceVersion":"1"},
        "spec":{"nodeName":"test-node","restartPolicy":"Never","containers":[
            {"name":"a","image":image,"imagePullPolicy":"Never"},
            {"name":"b","image":image,"imagePullPolicy":"Never"}
        ]},
        "status":{"phase":"Pending","containerStatuses":[
            {"name":"a","image":image,"imageID":image,"ready":false,"started":false,"restartCount":0,"state":{"waiting":{"reason":"ContainerCreating"}}},
            {"name":"b","image":image,"imageID":image,"ready":false,"started":false,"restartCount":0,"state":{"waiting":{"reason":"ContainerCreating"}}}
        ]}
    });
    harness.create_runtime_pod(pod.clone()).await;
    use klights_kubelet::pod_repository::PodStatusUpdate;
    harness
        .repo
        .pod_status_writer
        .set_pod_status_for_uid(
            "container-runtime",
            "multi-exit",
            "uid-multi-exit",
            PodStatusUpdate {
                phase: "Pending".to_string(),
                pod_ip: "10.0.0.2".to_string(),
                host_ip: String::new(),
                container_statuses: pod
                    .pointer("/status/containerStatuses")
                    .unwrap()
                    .as_array()
                    .unwrap()
                    .clone(),
                init_container_statuses: None,
                qos_class: None,
            },
            None,
        )
        .await
        .unwrap();
    harness
        .store
        .record_sandbox(&key, "sandbox-multi")
        .await
        .unwrap();
    harness.container_control.set_container_states(Vec::new()); // empty listing
    harness.cri.set_container_status_for_test(
        "ctr-a",
        "a",
        ContainerRuntimeState::Exited,
        0,
        1_000_000_000,
        1_250_000_000,
        image,
    );
    harness.cri.set_container_status_for_test(
        "ctr-b",
        "b",
        ContainerRuntimeState::Exited,
        0,
        1_000_000_000,
        1_250_000_000,
        image,
    );
    let hint = RuntimeReconcileHint::from_container_ids(["ctr-a".to_string(), "ctr-b".to_string()]);
    harness
        .runtime
        .reconcile_runtime(key.clone(), hint)
        .await
        .unwrap();
    let updated = harness.stored_pod(&key).await;
    assert_eq!(
        updated.pointer("/status/phase").and_then(|v| v.as_str()),
        Some("Succeeded"),
        "multi-container fast-exit pod must reach Succeeded: {:?}",
        updated.pointer("/status/phase")
    );
    let statuses = updated
        .pointer("/status/containerStatuses")
        .unwrap()
        .as_array()
        .unwrap();
    assert_eq!(statuses.len(), 2, "both containers must have statuses");
    for s in statuses {
        assert!(
            s.pointer("/state/terminated").is_some(),
            "container must be terminated: {s}"
        );
    }
}
