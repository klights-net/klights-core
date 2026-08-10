use super::*;

#[tokio::test]
async fn real_runtime_schedule_retry_emits_retry_due_after_delay() {
    let harness = PodRuntimeHarness::new().await;
    let key = PodRuntimeKey::new("default", "retry-pod", "uid-retry");
    let (tx, mut rx) = tokio::sync::mpsc::channel::<
        klights_kubelet::pod_lifecycle_core::message::LifecycleMessage,
    >(8);
    let reply_to = klights_kubelet::pod_lifecycle_router::LifecycleReplyHandle::direct(tx);

    harness
        .runtime
        .schedule_retry(key, std::time::Duration::from_millis(10), reply_to)
        .await
        .expect("schedule retry");

    let message = tokio::time::timeout(std::time::Duration::from_millis(250), rx.recv())
        .await
        .expect("retry wakeup must arrive")
        .expect("reply channel must stay open");
    match message {
        klights_kubelet::pod_lifecycle_core::message::LifecycleMessage::RetryDue { key } => {
            assert_eq!(key.namespace, "default");
            assert_eq!(key.name, "retry-pod");
            assert_eq!(key.uid, "uid-retry");
        }
        other => panic!("expected RetryDue, got {other:?}"),
    }
}

#[tokio::test]
async fn real_runtime_schedule_start_pod_retry_writes_status_event_and_wakeup() {
    let harness = PodRuntimeHarness::new().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"namespace": "default", "name": "runtime-retry", "uid": "uid-rr"},
        "spec": {
            "nodeName": "test-node",
            "containers": [{"name": "app", "image": "missing.example/app:1"}]
        },
        "status": {"phase": "Pending"}
    });
    harness.create_runtime_pod(pod).await;

    let key = PodRuntimeKey::new("default", "runtime-retry", "uid-rr");
    let (tx, mut rx) = tokio::sync::mpsc::channel::<
        klights_kubelet::pod_lifecycle_core::message::LifecycleMessage,
    >(8);
    let reply_to = klights_kubelet::pod_lifecycle_router::LifecycleReplyHandle::direct(tx);
    let error_message = "Failed to pull image missing.example/app:1".to_string();

    harness
        .runtime
        .schedule_start_pod_retry(
            key.clone(),
            std::time::Duration::from_millis(10),
            error_message.clone(),
            1,
            reply_to,
        )
        .await
        .expect("schedule start pod retry");

    let updated = __pod_query_get(&harness.repo, "default", "runtime-retry")
        .await
        .expect("pod exists");
    assert_eq!(
        updated
            .data
            .pointer("/status/containerStatuses/0/state/waiting/reason")
            .and_then(|v| v.as_str()),
        Some("ErrImagePull")
    );
    assert_eq!(
        updated
            .data
            .pointer("/status/phase")
            .and_then(|v| v.as_str()),
        Some("Pending")
    );

    let events = harness.events.recorded_events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type, "Warning");
    assert_eq!(events[0].reason, "Failed");
    assert_eq!(events[0].uid, "uid-rr");
    assert_eq!(events[0].message, error_message);

    let message = tokio::time::timeout(std::time::Duration::from_millis(250), rx.recv())
        .await
        .expect("retry wakeup must arrive")
        .expect("reply channel must stay open");
    match message {
        klights_kubelet::pod_lifecycle_core::message::LifecycleMessage::RetryDue { key } => {
            assert_eq!(key.uid, "uid-rr");
        }
        other => panic!("expected RetryDue, got {other:?}"),
    }
}

#[tokio::test]
async fn real_runtime_schedule_start_pod_retry_rejects_stale_uid_but_wakes() {
    let harness = PodRuntimeHarness::new().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"namespace": "default", "name": "runtime-stale", "uid": "uid-live"},
        "spec": {
            "nodeName": "test-node",
            "containers": [{"name": "app", "image": "missing.example/app:1"}]
        },
        "status": {"phase": "Pending"}
    });
    harness.create_runtime_pod(pod).await;
    let before = __pod_query_get(&harness.repo, "default", "runtime-stale")
        .await
        .expect("pod exists");

    let stale_key = PodRuntimeKey::new("default", "runtime-stale", "uid-stale");
    let (tx, mut rx) = tokio::sync::mpsc::channel::<
        klights_kubelet::pod_lifecycle_core::message::LifecycleMessage,
    >(8);
    let reply_to = klights_kubelet::pod_lifecycle_router::LifecycleReplyHandle::direct(tx);

    harness
        .runtime
        .schedule_start_pod_retry(
            stale_key,
            std::time::Duration::from_millis(10),
            "Failed to pull image".to_string(),
            1,
            reply_to,
        )
        .await
        .expect("stale retry still schedules wakeup");

    let after = __pod_query_get(&harness.repo, "default", "runtime-stale")
        .await
        .expect("pod exists");
    assert_eq!(after.uid, before.uid);
    assert_eq!(after.resource_version, before.resource_version);

    let message = tokio::time::timeout(std::time::Duration::from_millis(250), rx.recv())
        .await
        .expect("retry wakeup must arrive")
        .expect("reply channel must stay open");
    match message {
        klights_kubelet::pod_lifecycle_core::message::LifecycleMessage::RetryDue { key } => {
            assert_eq!(key.uid, "uid-stale");
        }
        other => panic!("expected RetryDue, got {other:?}"),
    }
}

#[test]
fn pod_slot_trait_is_object_safe() {
    use crate::kubelet::pod_runtime::store::PodSlotAdmission;
    fn assert_send_sync<T: ?Sized + Send + Sync>() {}
    assert_send_sync::<dyn PodSlotAdmission>();
}

#[tokio::test]
async fn mock_dependency_matrix_timer() {
    let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let fired = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let fired_clone = fired.clone();

    let _handle = supervisor
        .spawn_delay(
            "matrix_timer_test",
            std::time::Duration::from_millis(10),
            async move {
                fired_clone.store(true, std::sync::atomic::Ordering::SeqCst);
            },
        )
        .await
        .expect("spawn_delay must succeed");

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        fired.load(std::sync::atomic::Ordering::SeqCst),
        "spawn_delay must fire at least once"
    );
}

#[tokio::test]
async fn real_pod_slot_admission_admits_and_clears_slot() {
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let node_local =
        crate::bootstrap::pod_repository_composition::test_node_local_store(supervisor).await;
    let pod_slot_adapter = crate::bootstrap::kubelet_ports::DatastorePodSlotAdapter::new(
        node_local.pod_slots(),
        node_local.pod_slot_events(),
    );
    let admission = crate::kubelet::pod_runtime::store::RealPodSlotAdmission::new(
        pod_slot_adapter.clone(),
        pod_slot_adapter,
        "node-1".into(),
    );
    let key = PodRuntimeKey::new("ns", "slot-pod", "uid-1");

    // Try admit — should succeed on first attempt.
    let result = admission.try_admit(&key, "node-1").await.unwrap();
    assert!(
        matches!(
            result,
            klights_node_store::PodSlotAdmissionResult::Admitted { .. }
        ),
        "first admission should be Admitted, got {:?}",
        result
    );

    // Subscribe returns a receiver.
    let _rx = admission.subscribe();

    // Clear slot by UID.
    admission.clear_slot(&key).await.unwrap();
}

#[tokio::test]
async fn real_pod_slot_admission_blocks_duplicate_re_admit() {
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let node_local =
        crate::bootstrap::pod_repository_composition::test_node_local_store(supervisor).await;
    let pod_slot_adapter = crate::bootstrap::kubelet_ports::DatastorePodSlotAdapter::new(
        node_local.pod_slots(),
        node_local.pod_slot_events(),
    );
    let admission = crate::kubelet::pod_runtime::store::RealPodSlotAdmission::new(
        pod_slot_adapter.clone(),
        pod_slot_adapter,
        "node-1".into(),
    );
    let key = PodRuntimeKey::new("ns", "dup-pod", "uid-1");

    // First admission succeeds.
    let first = admission.try_admit(&key, "node-1").await.unwrap();
    assert!(matches!(
        first,
        klights_node_store::PodSlotAdmissionResult::Admitted { .. }
    ));

    // Second admission with different UID is blocked.
    let key2 = PodRuntimeKey::new("ns", "dup-pod", "uid-2");
    let second = admission.try_admit(&key2, "node-1").await.unwrap();
    assert!(
        matches!(
            second,
            klights_node_store::PodSlotAdmissionResult::Blocked { .. }
        ),
        "second admission with different UID should be Blocked, got {:?}",
        second
    );
}

#[tokio::test]
async fn real_runtime_start_pod_retrying_init_failure_publishes_pod_initializing_app_statuses() {
    let harness = PodRuntimeHarness::new().await;
    harness.cri.set_container_exit_code(1);
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "init-container",
            "name": "pod-init",
            "uid": "uid-pod-init",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "restartPolicy": "Always",
            "initContainers": [
                {"name": "init1", "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1", "imagePullPolicy": "Never"},
                {"name": "init2", "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1", "imagePullPolicy": "Never"}
            ],
            "containers": [
                {"name": "run1", "image": "registry.k8s.io/pause:3.10.1", "imagePullPolicy": "Never"}
            ]
        },
        "status": {"phase": "Pending"}
    });
    let key = PodRuntimeKey::new("init-container", "pod-init", "uid-pod-init");
    harness.create_runtime_pod(pod.clone()).await;

    let result = harness
        .runtime
        .start_pod(key.clone(), Some(pod), CancellationToken::new())
        .await
        .unwrap();

    assert!(
        matches!(result, PodStartResult::Failed(_)),
        "restartPolicy=Always init failure must be retryable, got {:?}",
        result
    );
    let stored = harness.stored_pod(&key).await;
    assert_eq!(
        stored
            .pointer("/status/phase")
            .and_then(|value| value.as_str()),
        Some("Pending")
    );
    let app_status = stored
        .pointer("/status/containerStatuses/0")
        .expect("app container status must be published after init failure");
    assert_eq!(
        app_status.pointer("/name").and_then(|value| value.as_str()),
        Some("run1")
    );
    assert_eq!(
        app_status
            .pointer("/state/waiting/reason")
            .and_then(|value| value.as_str()),
        Some("PodInitializing"),
        "app containers must stay PodInitializing while init containers are incomplete"
    );
    assert!(
        app_status.pointer("/state/waiting/message").is_none(),
        "PodInitializing app container statuses must not include the init failure message"
    );

    let init_statuses = stored
        .pointer("/status/initContainerStatuses")
        .and_then(|value| value.as_array())
        .expect("retrying init failure must publish initContainerStatuses");
    assert_eq!(init_statuses.len(), 2);
    assert_eq!(
        init_statuses[0]
            .pointer("/name")
            .and_then(|value| value.as_str()),
        Some("init1")
    );
    assert_eq!(
        init_statuses[0]
            .pointer("/state/waiting/reason")
            .and_then(|value| value.as_str()),
        Some("PodInitializing")
    );
    assert_eq!(
        init_statuses[0]
            .pointer("/lastState/terminated/exitCode")
            .and_then(|value| value.as_i64()),
        Some(1)
    );
    assert_eq!(
        init_statuses[0]
            .pointer("/restartCount")
            .and_then(|value| value.as_i64()),
        Some(1)
    );
    assert_eq!(
        init_statuses[1]
            .pointer("/name")
            .and_then(|value| value.as_str()),
        Some("init2")
    );
    assert_eq!(
        init_statuses[1]
            .pointer("/state/waiting/reason")
            .and_then(|value| value.as_str()),
        Some("PodInitializing")
    );

    let cri_calls = harness.cri.recorded_calls();
    assert!(
        cri_calls.iter().any(|call| {
            matches!(
                &call.operation,
                MockCriOperation::RemoveContainer(container_id)
                    if container_id == "container-sandbox-0001"
            )
        }),
        "retrying init failures must remove the failed init container before the actor retries"
    );
}

#[tokio::test]
async fn init_retry_preserves_restart_count_with_parity() {
    let harness = PodRuntimeHarness::new().await;
    harness.cri.set_container_exit_code(1);
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "init-container",
            "name": "pod-init-retry",
            "uid": "uid-pod-init-retry",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "restartPolicy": "Always",
            "initContainers": [
                {"name": "init1", "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1", "imagePullPolicy": "Never"},
                {"name": "init2", "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1", "imagePullPolicy": "Never"}
            ],
            "containers": [
                {"name": "run1", "image": "registry.k8s.io/pause:3.10.1", "imagePullPolicy": "Never"}
            ]
        },
        "status": {"phase": "Pending"}
    });
    let key = PodRuntimeKey::new("init-container", "pod-init-retry", "uid-pod-init-retry");
    harness.create_runtime_pod(pod.clone()).await;

    let first = harness
        .runtime
        .start_pod(key.clone(), Some(pod), CancellationToken::new())
        .await
        .unwrap();
    assert!(
        matches!(first, PodStartResult::Failed(_)),
        "first init failure should be retryable, got {:?}",
        first
    );

    let second = harness
        .runtime
        .start_pod(key.clone(), None, CancellationToken::new())
        .await
        .unwrap();
    assert!(
        matches!(second, PodStartResult::Failed(_)),
        "second init failure should still be retryable, got {:?}",
        second
    );

    let stored = harness.stored_pod(&key).await;
    assert_eq!(
        stored
            .pointer("/status/initContainerStatuses/0/name")
            .and_then(|value| value.as_str()),
        Some("init1")
    );
    assert_eq!(
        stored
            .pointer("/status/initContainerStatuses/0/restartCount")
            .and_then(|value| value.as_i64()),
        Some(2)
    );
    assert_eq!(
        stored
            .pointer("/status/containerStatuses/0/state/waiting/reason")
            .and_then(|value| value.as_str()),
        Some("PodInitializing"),
        "app containers must remain blocked while init retries continue"
    );
}

#[tokio::test]
async fn sandbox_reuse_on_init_retry_with_parity() {
    let harness = PodRuntimeHarness::new().await;
    harness.cri.set_container_exit_code(1);
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "init-container",
            "name": "pod-init-sandbox-retry",
            "uid": "uid-pod-init-sandbox-retry",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "restartPolicy": "Always",
            "initContainers": [
                {"name": "init1", "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1", "imagePullPolicy": "Never"}
            ],
            "containers": [
                {"name": "run1", "image": "registry.k8s.io/pause:3.10.1", "imagePullPolicy": "Never"}
            ]
        },
        "status": {"phase": "Pending"}
    });
    let key = PodRuntimeKey::new(
        "init-container",
        "pod-init-sandbox-retry",
        "uid-pod-init-sandbox-retry",
    );
    harness.create_runtime_pod(pod.clone()).await;

    let first = harness
        .runtime
        .start_pod(key.clone(), Some(pod), CancellationToken::new())
        .await
        .unwrap();
    assert!(
        matches!(first, PodStartResult::Failed(_)),
        "first init failure should be retryable, got {:?}",
        first
    );

    let second = harness
        .runtime
        .start_pod(key.clone(), None, CancellationToken::new())
        .await
        .unwrap();
    assert!(
        matches!(second, PodStartResult::Failed(_)),
        "second init failure should still be retryable, got {:?}",
        second
    );

    let cri_calls = harness.cri.recorded_calls();
    let sandbox_runs = cri_calls
        .iter()
        .filter(|call| matches!(call.operation, MockCriOperation::RunPodSandbox))
        .count();
    assert_eq!(
        sandbox_runs, 1,
        "init retry must reuse the already recorded pod sandbox instead of reserving a new one"
    );

    let created_sandbox_ids: Vec<_> = cri_calls
        .iter()
        .filter_map(|call| match &call.operation {
            MockCriOperation::CreateContainer { sandbox_id, .. } => Some(sandbox_id.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        created_sandbox_ids,
        vec!["sandbox-0001", "sandbox-0001"],
        "both init attempts should create the init container in the original sandbox"
    );
}

#[tokio::test]
async fn completed_init_container_skip_on_retry_with_parity() {
    let harness = PodRuntimeHarness::new().await;
    harness.cri.set_container_exit_code(1);
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "init-container",
            "name": "pod-init-later-retry",
            "uid": "uid-pod-init-later-retry",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "restartPolicy": "Always",
            "initContainers": [
                {"name": "init1", "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1", "imagePullPolicy": "Never"},
                {"name": "init2", "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1", "imagePullPolicy": "Never"}
            ],
            "containers": [
                {"name": "run1", "image": "registry.k8s.io/pause:3.10.1", "imagePullPolicy": "Never"}
            ]
        },
        "status": {
            "phase": "Pending",
            "initContainerStatuses": [
                {
                    "name": "init1",
                    "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1",
                    "imageID": "",
                    "ready": true,
                    "restartCount": 0,
                    "state": {
                        "terminated": {
                            "exitCode": 0,
                            "reason": "Completed",
                            "startedAt": "2026-05-20T00:00:00Z",
                            "finishedAt": "2026-05-20T00:00:01Z"
                        }
                    }
                },
                {
                    "name": "init2",
                    "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1",
                    "imageID": "",
                    "ready": false,
                    "started": false,
                    "restartCount": 1,
                    "state": {"waiting": {"reason": "PodInitializing"}},
                    "lastState": {
                        "terminated": {
                            "exitCode": 1,
                            "reason": "Error",
                            "startedAt": "2026-05-20T00:00:02Z",
                            "finishedAt": "2026-05-20T00:00:03Z"
                        }
                    }
                }
            ],
            "containerStatuses": [
                {
                    "name": "run1",
                    "image": "registry.k8s.io/pause:3.10.1",
                    "imageID": "",
                    "ready": false,
                    "started": false,
                    "restartCount": 0,
                    "state": {"waiting": {"reason": "PodInitializing"}}
                }
            ]
        }
    });
    let key = PodRuntimeKey::new(
        "init-container",
        "pod-init-later-retry",
        "uid-pod-init-later-retry",
    );
    harness.create_runtime_pod(pod.clone()).await;

    let result = harness
        .runtime
        .start_pod(key.clone(), Some(pod), CancellationToken::new())
        .await
        .unwrap();
    assert!(
        matches!(result, PodStartResult::Failed(_)),
        "later init failure should remain retryable, got {:?}",
        result
    );

    let create_calls: Vec<_> = harness
        .cri
        .recorded_calls()
        .into_iter()
        .filter_map(|call| match call.operation {
            MockCriOperation::CreateContainer { container_name, .. } => Some(container_name),
            _ => None,
        })
        .collect();
    assert!(
        !create_calls.iter().any(|name| name == "init1"),
        "completed init1 must not be recreated on an init2 retry; calls: {:?}",
        create_calls
    );
    assert!(
        create_calls.iter().any(|name| name == "init2"),
        "retry must resume at init2; calls: {:?}",
        create_calls
    );

    let stored = harness.stored_pod(&key).await;
    assert_eq!(
        stored
            .pointer("/status/initContainerStatuses/1/name")
            .and_then(|value| value.as_str()),
        Some("init2")
    );
    assert_eq!(
        stored
            .pointer("/status/initContainerStatuses/1/restartCount")
            .and_then(|value| value.as_i64()),
        Some(2)
    );
}

#[tokio::test]
async fn init_container_completed_matrix_with_parity() {
    let cases = [
        (
            "exit-code-int-zero",
            serde_json::json!({
                "name": "init1",
                "state": {
                    "terminated": {
                        "exitCode": 0,
                        "reason": "Completed"
                    }
                }
            }),
            true,
        ),
        (
            "exit-code-float-zero",
            serde_json::json!({
                "state": {
                    "terminated": {
                        "reason": "Completed",
                        "exitCode": 0.0,
                        "finishedAt": "2026-05-20T00:00:01Z"
                    }
                },
                "name": "init1",
                "restartCount": 0
            }),
            true,
        ),
        (
            "exit-code-missing",
            serde_json::json!({
                "name": "init1",
                "state": {"terminated": {"reason": "Completed"}}
            }),
            false,
        ),
        (
            "partial-waiting-state",
            serde_json::json!({
                "name": "init1",
                "state": {"waiting": {"reason": "PodInitializing"}}
            }),
            false,
        ),
        (
            "exit-code-float-nonzero",
            serde_json::json!({
                "name": "init1",
                "state": {"terminated": {"exitCode": 1.0, "reason": "Error"}}
            }),
            false,
        ),
    ];

    for (case_name, init_status, should_skip_init) in cases {
        let harness = PodRuntimeHarness::new().await;
        let pod_name = format!("init-matrix-{case_name}");
        let uid = format!("uid-{case_name}");
        let pod = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "init-container",
                "name": pod_name,
                "uid": uid,
                "resourceVersion": "1"
            },
            "spec": {
                "nodeName": "test-node",
                "restartPolicy": "Always",
                "initContainers": [{
                    "name": "init1",
                    "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1",
                    "imagePullPolicy": "Never"
                }],
                "containers": [{
                    "name": "app",
                    "image": "registry.k8s.io/pause:3.10.1",
                    "imagePullPolicy": "Never"
                }]
            },
            "status": {
                "phase": "Pending",
                "initContainerStatuses": [init_status]
            }
        });
        let key = PodRuntimeKey::new("init-container", &pod_name, &uid);
        harness.create_runtime_pod(pod.clone()).await;

        let _ = harness
            .runtime
            .start_pod(key, Some(pod), CancellationToken::new())
            .await
            .unwrap();

        let created_init = harness.cri.recorded_calls().into_iter().any(|call| {
            matches!(
                call.operation,
                MockCriOperation::CreateContainer { ref container_name, .. }
                    if container_name == "init1"
            )
        });
        assert_eq!(
            created_init, !should_skip_init,
            "{case_name}: completed init-container detection should match Kubernetes-compatible terminated exitCode semantics"
        );
    }
}

#[tokio::test]
async fn real_runtime_start_pod_classifies_retryable_vs_terminal_with_parity() {
    // Scenario 1: init container failure + restartPolicy Never → Terminal
    {
        let harness = PodRuntimeHarness::new().await;
        harness.cri.set_container_exit_code(1);
        let pod = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "namespace": "ns", "name": "terminal-pod", "uid": "uid-term", "resourceVersion": "1" },
            "spec": {
                "restartPolicy": "Never",
                "initContainers": [{"name": "init", "image": "busybox", "imagePullPolicy": "Never"}],
                "containers": [{"name": "app", "image": "nginx", "imagePullPolicy": "Never"}],
                "nodeName": "test-node"
            },
            "status": {"phase": "Pending"}
        });
        harness
            .repo
            .test_create_pod("ns", "terminal-pod", "test-node", pod.clone())
            .await
            .unwrap();
        let key = PodRuntimeKey::new("ns", "terminal-pod", "uid-term");
        let result = harness
            .runtime
            .start_pod(key, Some(pod), CancellationToken::new())
            .await
            .unwrap();
        assert!(
            matches!(result, PodStartResult::Terminal(_)),
            "restartPolicy=Never + init failure must produce Terminal, got {:?}",
            result
        );
    }

    // Scenario 2: init container failure + restartPolicy Always → Failed
    {
        let harness = PodRuntimeHarness::new().await;
        harness.cri.set_container_exit_code(1);
        let pod = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "namespace": "ns", "name": "retry-pod", "uid": "uid-retry", "resourceVersion": "1" },
            "spec": {
                "restartPolicy": "Always",
                "initContainers": [{"name": "init", "image": "busybox", "imagePullPolicy": "Never"}],
                "containers": [{"name": "app", "image": "nginx", "imagePullPolicy": "Never"}],
                "nodeName": "test-node"
            },
            "status": {"phase": "Pending"}
        });
        harness
            .repo
            .test_create_pod("ns", "retry-pod", "test-node", pod.clone())
            .await
            .unwrap();
        let key = PodRuntimeKey::new("ns", "retry-pod", "uid-retry");
        let result = harness
            .runtime
            .start_pod(key, Some(pod), CancellationToken::new())
            .await
            .unwrap();
        assert!(
            matches!(result, PodStartResult::Failed(_)),
            "restartPolicy=Always + init failure must produce Failed, got {:?}",
            result
        );
    }

    // Scenario 3: image pull failure → Failed (retryable)
    {
        let harness = PodRuntimeHarness::new().await;
        harness.cri.set_fail_operation("PullImage");
        let pod = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "namespace": "ns", "name": "imgfail-pod", "uid": "uid-img", "resourceVersion": "1" },
            "spec": {
                "containers": [{"name": "app", "image": "nginx", "imagePullPolicy": "Always"}],
                "nodeName": "test-node"
            },
            "status": {"phase": "Pending"}
        });
        harness
            .repo
            .test_create_pod("ns", "imgfail-pod", "test-node", pod.clone())
            .await
            .unwrap();
        let key = PodRuntimeKey::new("ns", "imgfail-pod", "uid-img");
        let result = harness
            .runtime
            .start_pod(key, Some(pod), CancellationToken::new())
            .await
            .unwrap();
        assert!(
            matches!(result, PodStartResult::Failed(_)),
            "image pull failure must be retryable (Failed), got {:?}",
            result
        );
    }
}

#[tokio::test]
async fn runtime_stop_uses_absolute_remaining_grace_and_recomputes_per_container() {
    let base_ms = 2_000_000_i64;
    let clock = Arc::new(AdvancingStopClock::new(base_ms, 2_000));
    let harness = PodRuntimeHarness::new_with_clock(clock).await;
    let key = PodRuntimeKey::new("ns", "deadline-pod", "uid-deadline");
    let mut pod = deadline_runtime_pod("deadline-pod", "uid-deadline", false);
    pod["spec"]["containers"] = serde_json::json!([
        {"name": "app", "image": "nginx"},
        {"name": "sidecar", "image": "busybox"}
    ]);
    harness.container_control.set_containers(vec![
        ("ctr-deadline".into(), "running".into()),
        ("ctr-sidecar".into(), "running".into()),
    ]);
    let deadline = chrono::DateTime::from_timestamp_millis(base_ms + 5_500).unwrap();
    let result = stop_with_deadline_request(
        &harness,
        key,
        pod,
        "sb-deadline",
        deadline,
        klights_kubelet::runtime::PodStopMode::Graceful,
        41,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(result, klights_kubelet::runtime::PodStopResult::Completed);
    let stop_graces = harness
        .cri
        .recorded_calls()
        .into_iter()
        .filter_map(|call| match call.operation {
            MockCriOperation::StopContainer(_, grace) => Some(grace),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(stop_graces, vec![6, 4]);
}

#[tokio::test]
async fn runtime_stop_grace_can_exceed_cri_transport_request_timeout() {
    let base_ms = 2_500_000_i64;
    let harness = PodRuntimeHarness::new_with_clock(Arc::new(FixedRuntimeClock(base_ms))).await;
    let key = PodRuntimeKey::new("ns", "long-grace", "uid-long-grace");
    let pod = deadline_runtime_pod("long-grace", "uid-long-grace", false);
    harness
        .container_control
        .set_containers(vec![("ctr-long-grace".into(), "running".into())]);
    let deadline = chrono::DateTime::from_timestamp_millis(base_ms + 300_000).unwrap();

    stop_with_deadline_request(
        &harness,
        key,
        pod,
        "sb-long-grace",
        deadline,
        klights_kubelet::runtime::PodStopMode::Graceful,
        43,
        CancellationToken::new(),
    )
    .await
    .unwrap();

    assert_eq!(
        klights_kubelet::cri::DEFAULT_CRI_REQUEST_TIMEOUT,
        std::time::Duration::from_secs(120)
    );
    assert!(harness.cri.recorded_calls().iter().any(|call| matches!(
        call.operation,
        MockCriOperation::StopContainer(ref id, 300) if id == "ctr-long-grace"
    )));
}

#[tokio::test]
async fn runtime_stop_elapsed_or_forced_deadline_passes_zero_and_skips_prestop() {
    for (mode, deadline_offset_ms) in [
        (klights_kubelet::runtime::PodStopMode::Graceful, -1),
        (klights_kubelet::runtime::PodStopMode::Forced, 30_000),
    ] {
        let base_ms = 3_000_000_i64;
        let harness = PodRuntimeHarness::new_with_clock(Arc::new(FixedRuntimeClock(base_ms))).await;
        let key = PodRuntimeKey::new("ns", "expired-pod", "uid-expired");
        let pod = deadline_runtime_pod("expired-pod", "uid-expired", true);
        harness
            .container_control
            .set_containers(vec![("ctr-deadline".into(), "running".into())]);
        let deadline =
            chrono::DateTime::from_timestamp_millis(base_ms + deadline_offset_ms).unwrap();
        let result = stop_with_deadline_request(
            &harness,
            key,
            pod,
            "sb-expired",
            deadline,
            mode,
            42,
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(result, klights_kubelet::runtime::PodStopResult::Completed);
        assert!(harness.hooks.recorded_calls().is_empty());
        assert!(
            harness
                .cri
                .recorded_calls()
                .iter()
                .any(|call| { matches!(call.operation, MockCriOperation::StopContainer(_, 0)) })
        );
    }
}

#[tokio::test]
async fn runtime_stop_bounds_prestop_by_deadline_and_honors_cancellation() {
    let harness = PodRuntimeHarness::new().await;
    let key = PodRuntimeKey::new("ns", "bounded-hook", "uid-hook");
    let pod = deadline_runtime_pod("bounded-hook", "uid-hook", true);
    harness
        .container_control
        .set_containers(vec![("ctr-deadline".into(), "running".into())]);
    harness
        .hooks
        .block_pre_stop_until(Arc::new(tokio::sync::Notify::new()));
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        stop_with_deadline_request(
            &harness,
            key,
            pod,
            "sb-hook",
            chrono::Utc::now() + chrono::Duration::milliseconds(50),
            klights_kubelet::runtime::PodStopMode::Graceful,
            43,
            CancellationToken::new(),
        ),
    )
    .await
    .expect("preStop must be bounded by the absolute deadline")
    .unwrap();
    assert_eq!(result, klights_kubelet::runtime::PodStopResult::Completed);
    assert_eq!(harness.hooks.recorded_calls().len(), 1);
    assert!(
        harness
            .cri
            .recorded_calls()
            .iter()
            .any(|call| { matches!(call.operation, MockCriOperation::StopContainer(_, 0)) })
    );

    let cancelled_harness = PodRuntimeHarness::new().await;
    let cancel = CancellationToken::new();
    cancel.cancel();
    let result = stop_with_deadline_request(
        &cancelled_harness,
        PodRuntimeKey::new("ns", "cancelled", "uid-cancelled"),
        deadline_runtime_pod("cancelled", "uid-cancelled", false),
        "sb-cancelled",
        chrono::Utc::now() + chrono::Duration::seconds(30),
        klights_kubelet::runtime::PodStopMode::Graceful,
        44,
        cancel,
    )
    .await
    .unwrap();
    assert_eq!(result, klights_kubelet::runtime::PodStopResult::Cancelled);
    assert!(cancelled_harness.cri.recorded_calls().is_empty());
}
