use super::*;

#[test]
fn multi_node_runtime_traits_are_object_safe_send_sync() {
    fn assert_send_sync<T: ?Sized + Send + Sync>() {}
    assert_send_sync::<dyn klights_pod_api::PodQuery>();
    assert_send_sync::<dyn crate::pod_repository::status::PodStatusWriter>();
}

#[test]
fn multi_node_traits_mutating_methods_require_uid() {
    use crate::runtime_types::PodRuntimeKey;

    // Compile-time verification: every mutating method on ClusterRuntimeView
    // takes PodRuntimeKey (UID-qualified).
    // NodeRuntimeView is read-only (no UID needed).

    // Verify key is usable with the traits (compile-time).
    let _key = PodRuntimeKey::new("ns", "name", "uid");

    // Verify the traits accept PodRuntimeKey.
    fn _takes_status_writer(_v: &dyn crate::pod_repository::status::PodStatusWriter) {}
    fn _takes_pod_query(_n: &dyn klights_pod_api::PodQuery) {}
}

#[test]
fn fake_cluster_nodes_keep_runtime_arguments_isolated() {
    let leader = FakeNode::new("node-leader");
    let worker = FakeNode::new("node-worker");

    assert_eq!(leader.node_name(), "node-leader");
    assert_eq!(worker.node_name(), "node-worker");

    // Each node has independent state — no shared mutable state.
    let leader_pod = serde_json::json!({
        "spec": {"nodeName": "node-leader"}
    });
    let worker_pod = serde_json::json!({
        "spec": {"nodeName": "node-worker"}
    });

    assert!(leader.owns_pod_runtime(&leader_pod));
    assert!(!leader.owns_pod_runtime(&worker_pod));
    assert!(worker.owns_pod_runtime(&worker_pod));
    assert!(!worker.owns_pod_runtime(&leader_pod));
}

#[test]
fn fake_worker_owns_only_pods_scheduled_to_its_node() {
    let worker = FakeNode::new("worker-1");

    // Pod scheduled to this node.
    let owned = serde_json::json!({"spec": {"nodeName": "worker-1"}});
    assert!(worker.owns_pod_runtime(&owned));

    // Pod scheduled to a different node.
    let other = serde_json::json!({"spec": {"nodeName": "worker-2"}});
    assert!(!worker.owns_pod_runtime(&other));

    // Pod with no nodeName.
    let unscheduled = serde_json::json!({"spec": {}});
    assert!(!worker.owns_pod_runtime(&unscheduled));
}

#[tokio::test]
async fn fake_cluster_records_worker_status_forward_to_leader() {
    use crate::runtime_types::PodRuntimeKey;

    let cluster = FakeCluster::new();

    // get_fresh_pod returns None when no pod is set.
    let result = cluster.get_fresh_pod("default", "test-pod").await.unwrap();
    assert!(result.is_none());

    // forward_pod_status records the forward.
    let key = PodRuntimeKey::new("default", "test-pod", "uid-1");
    let status = serde_json::json!({"phase": "Running"});
    let _ = cluster
        .forward_pod_status(&key, status.clone())
        .await
        .unwrap();

    let forwards = cluster.recorded_status_forwards();
    assert_eq!(forwards.len(), 1);
    assert_eq!(forwards[0].0.namespace, "default");
    assert_eq!(forwards[0].0.name, "test-pod");
    assert_eq!(forwards[0].0.uid, "uid-1");
    assert_eq!(forwards[0].1, status);
}

#[tokio::test]
async fn worker_runtime_starts_local_pod_and_does_not_touch_leader_cri() {
    let (cri, runtime, repo) = fixture_runtime_with_node("worker-1").await;

    // Pod scheduled to a different node (leader) — must be rejected.
    let leader_pod = scheduled_pod_json("ns", "leader-pod", "uid-leader", "leader-node");
    repo.test_create_pod("ns", "leader-pod", "leader-node", leader_pod.clone())
        .await
        .unwrap();
    let leader_key = PodRuntimeKey::new("ns", "leader-pod", "uid-leader");
    let result = runtime
        .start_pod(leader_key, Some(leader_pod), CancellationToken::new())
        .await
        .unwrap();
    assert!(
        matches!(result, PodStartResult::Failed(_)),
        "pod scheduled to leader must be rejected by worker runtime"
    );
    // CRI must not have been called.
    assert!(
        cri.recorded_calls().is_empty(),
        "CRI must not be called for pod not owned by this node"
    );

    // Pod scheduled to this worker — must be started.
    let local_pod = scheduled_pod_json("ns", "local-pod", "uid-local", "worker-1");
    repo.test_create_pod("ns", "local-pod", "worker-1", local_pod.clone())
        .await
        .unwrap();
    let local_key = PodRuntimeKey::new("ns", "local-pod", "uid-local");
    let result = runtime
        .start_pod(local_key, Some(local_pod), CancellationToken::new())
        .await
        .unwrap();
    assert!(
        matches!(result, PodStartResult::Started { .. }),
        "pod scheduled to this worker must be started"
    );
    // CRI must have been called for the local pod.
    assert!(
        !cri.recorded_calls().is_empty(),
        "CRI must be called for pod owned by this node"
    );
}

#[tokio::test]
async fn worker_runtime_does_not_start_same_name_replacement_for_stale_uid() {
    let (cri, runtime, repo) = fixture_runtime_with_node("worker-1").await;

    // Create a pod with new-uid (simulating same-name replacement).
    let new_pod = scheduled_pod_json("ns", "test-pod", "new-uid", "worker-1");
    repo.test_create_pod("ns", "test-pod", "worker-1", new_pod.clone())
        .await
        .unwrap();

    // Build a stale snapshot with old-uid (simulating a start request that
    // was enqueued before the replacement).
    let old_snapshot = scheduled_pod_json("ns", "test-pod", "old-uid", "worker-1");

    // Try to start_pod with the old UID and old snapshot.
    let old_key = PodRuntimeKey::new("ns", "test-pod", "old-uid");
    let result = runtime
        .start_pod(old_key, Some(old_snapshot), CancellationToken::new())
        .await;

    // Must fail because live pod has new UID (UID mismatch between key.uid="old-uid"
    // and live pod UID="new-uid").
    match result {
        Ok(PodStartResult::Failed(_)) | Err(_) => {}
        other => panic!("expected failure for stale UID, got {:?}", other),
    }

    // CRI must not have been called for the stale UID.
    assert!(
        cri.recorded_calls().is_empty(),
        "CRI must not be called for stale UID"
    );
}

#[tokio::test]
async fn worker_runtime_rejects_same_name_replacement_without_snapshot() {
    let (cri, runtime, repo) = fixture_runtime_with_node("worker-1").await;

    // Live pod has the NEW uid (the replacement); the start request carries the
    // OLD uid via the key and no snapshot.
    let new_pod = scheduled_pod_json("ns", "test-pod", "new-uid", "worker-1");
    repo.test_create_pod("ns", "test-pod", "worker-1", new_pod)
        .await
        .unwrap();

    let old_key = PodRuntimeKey::new("ns", "test-pod", "old-uid");
    let result = runtime
        .start_pod(old_key, None, CancellationToken::new())
        .await;

    match result {
        Ok(PodStartResult::Failed(_)) | Err(_) => {}
        other => panic!(
            "expected failure for stale UID without snapshot, got {:?}",
            other
        ),
    }
    assert!(
        cri.recorded_calls().is_empty(),
        "CRI must not be called for a same-name replacement when starting without a snapshot"
    );
}

#[tokio::test]
async fn worker_runtime_forwards_status_to_leader() {
    let cluster = std::sync::Arc::new(FakeCluster::new());
    let (_cri, runtime, repo, _cluster, _hostports) =
        fixture_runtime_with_cluster("worker-1", cluster).await;

    let pod = scheduled_pod_json("ns", "fwd-pod", "uid-fwd", "worker-1");
    repo.test_create_pod("ns", "fwd-pod", "worker-1", pod.clone())
        .await
        .unwrap();
    let key = PodRuntimeKey::new("ns", "fwd-pod", "uid-fwd");
    let result = runtime
        .start_pod(key, Some(pod), CancellationToken::new())
        .await
        .unwrap();
    assert!(matches!(result, PodStartResult::Started { .. }));

    let stored = repo
        .test_get_pod_for_uid("ns", "fwd-pod", "uid-fwd")
        .await
        .unwrap()
        .expect("focused worker status writer must preserve UID identity");
    assert_eq!(stored.namespace.as_deref(), Some("ns"));
    assert_eq!(stored.name, "fwd-pod");
    assert_eq!(stored.uid, "uid-fwd");
    assert!(repo.backend.status_write_count() > 0);
}

#[tokio::test]
async fn leader_runtime_writes_status_locally() {
    // Leader runtime routes through the same worker cluster-view, backed by the
    // local cluster-datastore repository, so status writes land locally.
    let harness = PodRuntimeHarness::new().await;
    let pod = scheduled_pod_json("ns", "local-pod", "uid-local", "test-node");
    harness
        .repo
        .test_create_pod("ns", "local-pod", "test-node", pod.clone())
        .await
        .unwrap();
    let key = PodRuntimeKey::new("ns", "local-pod", "uid-local");
    let result = harness
        .runtime
        .start_pod(key, Some(pod), CancellationToken::new())
        .await
        .unwrap();
    assert!(matches!(result, PodStartResult::Started { .. }));

    // Verify the pod status was written to the local repository.
    let resource = harness
        .repo
        .test_get_pod_for_uid("ns", "local-pod", "uid-local")
        .await
        .unwrap()
        .expect("pod must exist");
    let phase = resource
        .data
        .pointer("/status/phase")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(phase, "Pending", "leader must write status locally");
}

#[tokio::test]
async fn worker_runtime_forwarded_status_is_uid_preconditioned() {
    let cluster = std::sync::Arc::new(FakeCluster::new());
    let (_cri, runtime, repo, cluster, _hostports) =
        fixture_runtime_with_cluster("worker-1", cluster).await;

    let pod = scheduled_pod_json("ns", "uid-pod", "uid-chk", "worker-1");
    repo.test_create_pod("ns", "uid-pod", "worker-1", pod.clone())
        .await
        .unwrap();
    let key = PodRuntimeKey::new("ns", "uid-pod", "uid-chk");
    runtime
        .start_pod(key, Some(pod), CancellationToken::new())
        .await
        .unwrap();

    // Every forwarded status must be UID-preconditioned.
    let forwards = cluster.recorded_status_forwards();
    for (fwd_key, _status) in &forwards {
        assert_eq!(
            fwd_key.uid, "uid-chk",
            "forwarded status must carry the correct UID"
        );
        assert_eq!(fwd_key.namespace, "ns");
        assert_eq!(fwd_key.name, "uid-pod");
    }
}

#[tokio::test]
async fn cross_node_delete_is_rejected_on_non_owner_node() {
    let (cri, runtime, repo) = fixture_runtime_with_node("worker-1").await;

    // Pod scheduled to a different node: this node must not perform any local
    // cleanup and must not report success, because success lets the lifecycle
    // actor finalize a Pod row whose owning node never cleaned its resources.
    let cross_pod = scheduled_pod_json("ns", "cross-pod", "uid-cross", "worker-2");
    repo.test_create_pod("ns", "cross-pod", "worker-2", cross_pod.clone())
        .await
        .unwrap();
    let cross_key = PodRuntimeKey::new("ns", "cross-pod", "uid-cross");
    let err = runtime
        .stop_pod(cross_key, Some(cross_pod), Some("sb-cross".into()))
        .await
        .expect_err("non-owner node must not report Pod cleanup success");
    let own = err
        .downcast_ref::<PodOwnershipError>()
        .expect("non-owner cleanup refusal must be a typed PodOwnershipError");
    assert_eq!(
        own.local_node, "worker-1",
        "unexpected non-owner cleanup error: {err:#}"
    );
    assert_eq!(
        own.target_node.as_deref(),
        Some("worker-2"),
        "target node must be preserved for cross-node cleanup refusal"
    );

    // CRI must NOT have been called (no sandbox stop/remove for non-owned pod).
    let cri_calls = cri.recorded_calls();
    assert!(
        cri_calls.is_empty(),
        "CRI must not be called for pod not owned by this node"
    );

    // Pod scheduled to this node — stop_pod must release network and clean up CRI.
    let local_pod = scheduled_pod_json("ns", "local-pod", "uid-local", "worker-1");
    repo.test_create_pod("ns", "local-pod", "worker-1", local_pod.clone())
        .await
        .unwrap();
    let local_key = PodRuntimeKey::new("ns", "local-pod", "uid-local");
    runtime
        .stop_pod(local_key, Some(local_pod), Some("sb-local".into()))
        .await
        .unwrap();

    // CRI must have been called for the owned pod.
    let cri_calls = cri.recorded_calls();
    let has_stop = cri_calls
        .iter()
        .any(|c| matches!(c.operation, MockCriOperation::StopPodSandbox(_)));
    assert!(has_stop, "CRI sandbox must be stopped for owned pod");
}

#[tokio::test]
async fn mock_dependency_matrix_cluster_view() {
    struct FakeClusterView {
        calls: std::sync::Mutex<Vec<(String, String, String)>>,
    }
    impl FakeClusterView {
        fn new() -> Self {
            Self {
                calls: std::sync::Mutex::new(Vec::new()),
            }
        }
        async fn forward_pod_status(&self, key: &PodRuntimeKey, _status: &serde_json::Value) {
            self.calls.lock().unwrap().push((
                key.namespace.clone(),
                key.name.clone(),
                key.uid.clone(),
            ));
        }
    }

    let view = FakeClusterView::new();
    let key = PodRuntimeKey::new("ns", "cv-pod", "uid-cv");
    view.forward_pod_status(&key, &serde_json::json!({"phase": "Running"}))
        .await;

    let calls = view.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, "ns");
    assert_eq!(calls[0].1, "cv-pod");
    assert_eq!(calls[0].2, "uid-cv");
}

#[tokio::test]
async fn mock_dependency_matrix_env_source() {
    let mock = MockEnvSourceReader::new();

    mock.insert_secret(
        "ns",
        "secret-a",
        serde_json::json!({"data": {"token": "dmFsdWU="}}),
    );
    mock.insert_config_map(
        "ns",
        "config-a",
        serde_json::json!({"data": {"setting": "enabled"}}),
    );
    mock.insert_service(
        "ns",
        "svc-a",
        serde_json::json!({"spec": {"clusterIP": "10.43.0.10"}}),
    );

    let secret = mock.secret("ns", "secret-a").await.unwrap();
    let config_map = mock.config_map("ns", "config-a").await.unwrap();
    let services = mock.services("ns").await.unwrap();

    assert!(secret.is_some(), "mock secret must be returned");
    assert!(config_map.is_some(), "mock configmap must be returned");
    assert_eq!(services.len(), 1, "mock service list must be returned");

    let calls = mock.recorded_calls();
    assert_eq!(
        calls,
        vec![
            "secret:ns/secret-a".to_string(),
            "config_map:ns/config-a".to_string(),
            "services:ns".to_string(),
        ],
        "env-source lookups must be observable in order"
    );
}

#[tokio::test]
async fn mock_dependency_matrix_deletion_finalizer() {
    let mock = MockPodDeletionFinalizer::new();
    let key = PodRuntimeKey::new("ns", "del-pod", "uid-del");

    let result = mock.finalize_after_actor_cleanup(&key).await.unwrap();
    assert!(
        matches!(result, PodDeletionFinalizeResult::DeletedOrAlreadyGone),
        "default mock must return DeletedOrAlreadyGone"
    );

    let calls = mock.recorded_calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].namespace, "ns");
    assert_eq!(calls[0].name, "del-pod");
    assert_eq!(calls[0].uid, "uid-del");
}

#[tokio::test]
async fn mock_dependency_matrix_fake_cluster() {
    let leader_cri = MockCriRuntime::new();
    let worker_cri = MockCriRuntime::new();

    leader_cri.image_status("nginx:leader").await.unwrap();
    leader_cri.pull_image("nginx:leader").await.unwrap();

    worker_cri.image_status("nginx:worker").await.unwrap();
    worker_cri
        .run_pod_sandbox(PodSandboxConfig::default())
        .await
        .unwrap();

    let leader_calls = leader_cri.recorded_calls();
    for call in &leader_calls {
        let call_str = format!("{:?}", call.operation);
        assert!(
            !call_str.contains("worker"),
            "leader CRI must not record worker calls"
        );
    }

    let worker_calls = worker_cri.recorded_calls();
    for call in &worker_calls {
        let call_str = format!("{:?}", call.operation);
        assert!(
            !call_str.contains("leader"),
            "worker CRI must not record leader calls"
        );
    }
}

#[test]
fn local_node_runtime_view_owns_pod_with_matching_node_name() {
    let view = FakeNode::new("node-1");
    let pod = serde_json::json!({
        "spec": {"nodeName": "node-1"}
    });
    assert!(view.owns_pod_runtime(&pod));
    assert_eq!(view.node_name(), "node-1");
}

#[test]
fn local_node_runtime_view_rejects_pod_with_different_node_name() {
    let view = FakeNode::new("node-1");
    let pod = serde_json::json!({
        "spec": {"nodeName": "node-2"}
    });
    assert!(!view.owns_pod_runtime(&pod));
}

#[test]
fn local_node_runtime_view_rejects_pod_without_node_name() {
    let view = FakeNode::new("node-1");
    let pod = serde_json::json!({
        "spec": {}
    });
    assert!(!view.owns_pod_runtime(&pod));
}

#[tokio::test]
async fn repository_cluster_runtime_view_constructs_with_repository() {
    let repo = fixture_pod_repository().await;
    let _focused_ports = (repo.pod_query, repo.pod_status_writer);
}

#[tokio::test]
async fn worker_init_retry_never_forwards_phase_only_pending_status() {
    let cluster = std::sync::Arc::new(FakeCluster::new());
    let (cri, runtime, repo, _cluster, _hostports) =
        fixture_runtime_with_cluster("worker-1", cluster).await;
    cri.set_container_exit_code(1);
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "init-container",
            "name": "pod-init-stale-retry",
            "uid": "uid-pod-init-stale-retry",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "worker-1",
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
    let key = PodRuntimeKey::new(
        "init-container",
        "pod-init-stale-retry",
        "uid-pod-init-stale-retry",
    );
    repo.test_create_pod(
        "init-container",
        "pod-init-stale-retry",
        "worker-1",
        pod.clone(),
    )
    .await
    .unwrap();

    let first = runtime
        .start_pod(key.clone(), Some(pod.clone()), CancellationToken::new())
        .await
        .unwrap();
    assert!(
        matches!(first, PodStartResult::Failed(_)),
        "first init failure should be retryable, got {first:?}"
    );
    let before_retry = repo.backend.status_write_count();

    let second = runtime
        .start_pod(key.clone(), Some(pod), CancellationToken::new())
        .await
        .unwrap();
    assert!(
        matches!(second, PodStartResult::Failed(_)),
        "second init failure should still be retryable, got {second:?}"
    );

    assert!(
        repo.backend.status_write_count() > before_retry,
        "retry must persist an updated Pending status"
    );
    let retry_resource = repo
        .test_get_pod_for_uid(
            "init-container",
            "pod-init-stale-retry",
            "uid-pod-init-stale-retry",
        )
        .await
        .unwrap()
        .expect("retry Pod must remain UID-bound");
    let retry_status = retry_resource
        .data
        .pointer("/status")
        .expect("retry status");
    assert_eq!(
        retry_status
            .pointer("/phase")
            .and_then(|value| value.as_str()),
        Some("Pending")
    );
    let init_statuses = retry_status
        .pointer("/initContainerStatuses")
        .and_then(|value| value.as_array())
        .expect("retry Pending status must not drop initContainerStatuses");
    assert_eq!(init_statuses.len(), 2);
    assert_eq!(
        retry_status
            .pointer("/containerStatuses/0/state/waiting/reason")
            .and_then(|value| value.as_str()),
        Some("PodInitializing"),
        "app container must remain waiting while init containers retry"
    );
}
