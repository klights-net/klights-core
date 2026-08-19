use super::*;

#[test]
fn filesystem_and_volume_ports_record_pod_identity_arguments() {
    use crate::runtime::filesystem::PodFilesystem;
    use crate::runtime::volumes::PodVolumeRuntime;
    fn assert_send_sync<T: ?Sized + Send + Sync>() {}
    assert_send_sync::<dyn PodFilesystem>();
    assert_send_sync::<dyn PodVolumeRuntime>();
}

#[tokio::test]
async fn mock_filesystem_records_hosts_logs_cgroups_and_fsgroup() {
    let fs = MockPodFilesystem::new();
    let key = PodRuntimeKey::new("ns", "pod", "uid-1");
    let pod = crate::runtime::test_support::pod_json("ns", "pod", "uid-1", "img");

    fs.write_hosts(&key, &pod).await.unwrap();
    fs.create_log_directory(&key).await.unwrap();
    fs.ensure_termination_log_file(&key, "app").await;
    fs.set_termination_message(&key, "app", "done");
    assert_eq!(
        fs.read_termination_message(&key, "app", "File", 0).await,
        "done"
    );
    fs.cleanup_cgroup(&key).await.unwrap();
    fs.apply_fs_group(&key, &pod).await.unwrap();
    fs.cleanup_pod_filesystem(&key).await.unwrap();

    let calls = fs.recorded_calls();
    assert_eq!(calls.len(), 7);
    assert!(calls.iter().all(|c| c.contains("uid-1")));
}

#[tokio::test]
async fn real_filesystem_handles_termination_log_with_parity() {
    let runtime_namespace = "klights-term-real-fs-test";
    let runtime_paths = kubelet_runtime_paths_for_test(runtime_namespace);
    let _ = std::fs::remove_dir_all(runtime_paths.data_root());
    let fs = crate::runtime::filesystem::RealPodFilesystem::new(
        std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        )),
        runtime_namespace.to_string(),
        "test-node".to_string(),
        runtime_paths.clone(),
    );
    let key = PodRuntimeKey::new("ns", "pod", "uid-real-term");
    let expected_path = runtime_paths
        .containerd_termination_log("ns", "pod", "app")
        .to_string_lossy()
        .into_owned();

    let path = fs.ensure_termination_log_file(&key, "app").await;
    std::fs::write(&path, "real-message").unwrap();
    let message = fs.read_termination_message(&key, "app", "File", 0).await;

    assert_eq!(path, expected_path);
    assert_eq!(message, "real-message");
    let _ = std::fs::remove_dir_all(runtime_paths.data_root());
}

#[tokio::test]
async fn real_filesystem_cleanup_removes_entire_pod_root() {
    let runtime_namespace = "klights-pod-root-cleanup-test";
    let runtime_paths = kubelet_runtime_paths_for_test(runtime_namespace);
    let data_root = runtime_paths.data_root().to_path_buf();
    let _ = std::fs::remove_dir_all(&data_root);
    let fs = crate::runtime::filesystem::RealPodFilesystem::new(
        std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        )),
        runtime_namespace.to_string(),
        "test-node".to_string(),
        runtime_paths.clone(),
    );
    let key = PodRuntimeKey::new("ns", "pod", "uid-root-cleanup");
    let pod_root = runtime_paths
        .volumes_root()
        .join(format!("{}_{}_{}", key.namespace, key.name, key.uid));
    let pod_log_dir = runtime_paths.pod_log_dir(&key.namespace, &key.name, &key.uid);

    std::fs::create_dir_all(pod_root.join("volumes/empty-dir/cache"))
        .expect("create pod volume dir");
    std::fs::write(pod_root.join("volumes/empty-dir/cache/file.txt"), b"data")
        .expect("write pod volume file");
    std::fs::create_dir_all(pod_root.join("etc-hosts")).expect("create pod hosts dir");
    std::fs::write(pod_root.join("etc-hosts/hosts"), b"127.0.0.1 localhost")
        .expect("write hosts file");
    std::fs::create_dir_all(pod_log_dir.join("app")).expect("create pod log dir");
    std::fs::write(pod_log_dir.join("app/0.log"), b"container log").expect("write pod log");

    fs.cleanup_pod_filesystem(&key)
        .await
        .expect("cleanup pod filesystem");

    assert!(
        !pod_root.exists(),
        "pod root directory should be removed: {}",
        pod_root.display()
    );
    assert!(
        !pod_log_dir.exists(),
        "pod log directory should be removed: {}",
        pod_log_dir.display()
    );
    let _ = std::fs::remove_dir_all(data_root);
}

#[cfg(unix)]
#[tokio::test]
async fn fs_group_volume_ownership_with_parity() {
    use std::os::unix::fs::MetadataExt;

    let current_gid = std::fs::metadata(".").unwrap().gid();
    let Some(fs_group) = alternate_test_group(current_gid) else {
        eprintln!("skipping fsGroup ownership test: no alternate group available");
        return;
    };

    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let containerd_ns = format!("podfs-fsgroup-test-{suffix}");
    let runtime_paths = kubelet_runtime_paths_for_test(&containerd_ns);
    let data_root = runtime_paths.data_root().to_path_buf();
    let _ = std::fs::remove_dir_all(&data_root);

    let key = PodRuntimeKey::new("projected", "pod-projected-secrets", "uid-fsgroup");
    let volume_dir = runtime_paths
        .volumes_root()
        .join(format!("{}_{}_{}", key.namespace, key.name, key.uid))
        .join("volumes")
        .join("projected")
        .join("secret-vol");
    std::fs::create_dir_all(&volume_dir).unwrap();
    let projected_file = volume_dir.join("data-1");
    std::fs::write(&projected_file, "secret-data").unwrap();
    assert_ne!(
        std::fs::metadata(&projected_file).unwrap().gid(),
        fs_group,
        "test setup must start with a file outside the target fsGroup"
    );

    let fs = crate::runtime::filesystem::RealPodFilesystem::new(
        std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        )),
        containerd_ns.clone(),
        "test-node".to_string(),
        runtime_paths,
    );
    let pod = serde_json::json!({
        "spec": {
            "securityContext": {
                "fsGroup": fs_group
            }
        }
    });

    fs.apply_fs_group(&key, &pod).await.unwrap();
    let applied_gid = std::fs::metadata(&projected_file).unwrap().gid();
    let _ = std::fs::remove_dir_all(data_root);

    assert_eq!(
        applied_gid, fs_group,
        "projected volume files must be group-owned by pod fsGroup"
    );
}

#[tokio::test]
async fn mock_volume_runtime_records_process_and_cleanup() {
    let vol = MockPodVolumeRuntime::new();
    let key = PodRuntimeKey::new("ns", "pod", "uid-1");
    let pod = crate::runtime::test_support::pod_json("ns", "pod", "uid-1", "img");

    vol.process_volumes(&key, &pod).await.unwrap();
    vol.cleanup_volumes(&key).await.unwrap();

    let calls = vol.recorded_calls();
    assert_eq!(calls.len(), 2);
    assert!(calls[0].contains("process_volumes") && calls[0].contains("uid-1"));
    assert!(calls[1].contains("cleanup_volumes") && calls[1].contains("uid-1"));
}

#[tokio::test]
async fn hung_volume_setup_times_out_and_rolls_back_sandbox() {
    let harness = PodRuntimeHarness::new().await;
    let pod = pod_with_pull_policy("ns", "volume-hang", "uid-volume-hang", "nginx", "Never");
    harness
        .repo
        .test_create_pod("ns", "volume-hang", "test-node", pod.clone())
        .await
        .unwrap();
    harness.volumes.hang_process_volumes();

    let key = PodRuntimeKey::new("ns", "volume-hang", "uid-volume-hang");
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(250),
        harness
            .runtime
            .start_pod(key.clone(), Some(pod), CancellationToken::new()),
    )
    .await
    .expect("hung volume setup must be bounded by the runtime")
    .expect("volume setup timeout should be reported as a pod start result");

    match result {
        PodStartResult::Failed(message) => {
            assert!(
                message.contains("Timed out processing volumes"),
                "timeout should describe volume setup: {message}"
            );
        }
        other => panic!("expected retryable volume setup failure, got {other:?}"),
    }
    assert_partial_start_rolled_back(&harness, &key, "sandbox-0001");
}

#[tokio::test]
async fn real_runtime_start_pod_stops_before_containers_when_volume_processing_fails() {
    let harness = PodRuntimeHarness::new().await;
    let pod = pod_with_pull_policy("ns", "volume-fail", "uid-volume-fail", "nginx", "Never");
    harness
        .repo
        .test_create_pod("ns", "volume-fail", "test-node", pod.clone())
        .await
        .unwrap();
    harness
        .volumes
        .fail_process_volumes("projected ServiceAccount token request denied");

    let key = PodRuntimeKey::new("ns", "volume-fail", "uid-volume-fail");
    let result = harness
        .runtime
        .start_pod(key, Some(pod), CancellationToken::new())
        .await
        .unwrap();

    match result {
        PodStartResult::Failed(message) => {
            assert!(message.contains("Failed to process volumes"));
            assert!(message.contains("projected ServiceAccount token request denied"));
        }
        other => panic!("volume setup failure must fail startup, got {other:?}"),
    }

    let cri_calls = harness.cri.recorded_calls();
    assert!(
        cri_calls
            .iter()
            .any(|call| matches!(call.operation, MockCriOperation::RunPodSandbox)),
        "sandbox is created before volume processing in the current startup flow"
    );
    assert!(
        !cri_calls
            .iter()
            .any(|call| matches!(call.operation, MockCriOperation::CreateContainer { .. })),
        "volume processing failure must stop before container creation"
    );
}

#[tokio::test]
async fn real_runtime_stop_pod_releases_hostports_and_cleans_volumes() {
    let harness = PodRuntimeHarness::new().await;
    let pod = pod_with_pull_policy("ns", "stop-hv", "uid-hv", "nginx", "Never");
    harness
        .repo
        .test_create_pod("ns", "stop-hv", "test-node", pod.clone())
        .await
        .unwrap();
    let key = PodRuntimeKey::new("ns", "stop-hv", "uid-hv");
    let sandbox_id = "sb-hv";

    harness
        .store
        .record_sandbox(&key, sandbox_id)
        .await
        .unwrap();

    harness
        .runtime
        .stop_pod(key.clone(), Some(pod), Some(sandbox_id.into()))
        .await
        .unwrap();

    // HostPort rules must be removed by UID.
    let hp_calls = harness.hostports.recorded_calls();
    assert!(
        hp_calls
            .iter()
            .any(|c| matches!(c, MockHostPortOp::Remove { uid, .. } if uid == "uid-hv")),
        "hostPort rules must be removed"
    );

    // Volumes must be cleaned up by UID.
    let vol_calls = harness.volumes.recorded_calls();
    assert!(
        vol_calls
            .iter()
            .any(|s| s.contains("cleanup_volumes") && s.contains("uid-hv")),
        "volumes must be cleaned up"
    );

    // The pod root must be removed after volume unmount/removal so generated
    // host files and empty pod directories do not survive termination.
    let fs_calls = harness.filesystem.recorded_calls();
    assert!(
        fs_calls
            .iter()
            .any(|s| s.contains("cleanup_fs") && s.contains("uid-hv")),
        "pod filesystem root must be cleaned up"
    );
}

#[tokio::test]
async fn real_runtime_stop_orphan_pod_cleans_volumes() {
    let harness = PodRuntimeHarness::new().await;
    let key = PodRuntimeKey::new("ns", "orphan-vol", "uid-ov");
    let sandbox_id = "sb-ov";

    harness
        .store
        .record_sandbox(&key, sandbox_id)
        .await
        .unwrap();

    harness
        .runtime
        .stop_orphan_pod(&key, Some(sandbox_id.into()))
        .await
        .unwrap();

    // Volumes must be unmounted/removed on the orphan path too.
    let vol_calls = harness.volumes.recorded_calls();
    assert!(
        vol_calls
            .iter()
            .any(|s| s.contains("cleanup_volumes") && s.contains("uid-ov")),
        "orphan stop must clean up volumes (unmount before pod-root removal), got: {vol_calls:?}"
    );

    // The pod root must still be removed after the volume unmount/removal.
    let fs_calls = harness.filesystem.recorded_calls();
    assert!(
        fs_calls
            .iter()
            .any(|s| s.contains("cleanup_fs") && s.contains("uid-ov")),
        "orphan stop must clean up the pod filesystem root"
    );
}

#[tokio::test]
async fn real_runtime_stop_pod_cleans_cgroup_even_without_sandbox() {
    let harness = PodRuntimeHarness::new().await;
    let key = PodRuntimeKey::new("ns", "nocg", "uid-nocg");

    // No sandbox hint, no store row, CRI reports none.
    harness
        .runtime
        .stop_pod(key.clone(), None, None)
        .await
        .unwrap();

    let fs_calls = harness.filesystem.recorded_calls();
    assert!(
        fs_calls
            .iter()
            .any(|c| c.starts_with("cleanup_cgroup:ns/nocg/uid-nocg")),
        "cgroup must be cleaned even without a resolved sandbox: {fs_calls:?}"
    );
}

#[tokio::test]
async fn mock_dependency_matrix_filesystem() {
    let mock = MockPodFilesystem::new();
    let key = PodRuntimeKey::new("ns", "fs-pod", "uid-fs");
    let pod = serde_json::json!({"metadata": {"name": "fs-pod"}});

    mock.create_log_directory(&key).await.unwrap();
    mock.write_hosts(&key, &pod).await.unwrap();
    mock.cleanup_cgroup(&key).await.unwrap();
    mock.apply_fs_group(&key, &pod).await.unwrap();

    let calls = mock.recorded_calls();
    assert!(
        calls.len() >= 4,
        "expected at least 4 FS calls, got {}",
        calls.len()
    );
    for call in &calls {
        assert!(
            call.contains("ns") && call.contains("fs-pod") && call.contains("uid-fs"),
            "call '{}' must contain Pod identity",
            call
        );
    }
}

#[tokio::test]
async fn mock_dependency_matrix_volume() {
    let mock = MockPodVolumeRuntime::new();
    let key = PodRuntimeKey::new("ns", "vol-pod", "uid-vol");
    let pod = serde_json::json!({"spec": {"volumes": []}});

    mock.process_volumes(&key, &pod).await.unwrap();
    mock.cleanup_volumes(&key).await.unwrap();

    let calls = mock.recorded_calls();
    assert_eq!(calls.len(), 2);
    assert!(
        calls[0].contains("ns") && calls[0].contains("vol-pod") && calls[0].contains("uid-vol")
    );
    assert!(
        calls[1].contains("ns") && calls[1].contains("vol-pod") && calls[1].contains("uid-vol")
    );
}

#[tokio::test]
async fn mocked_runtime_does_not_create_termination_log_file_directly() {
    let runtime_namespace = "klights-term-mock-create-test";
    let _ = std::fs::remove_dir_all(kubelet_runtime_paths_for_test(runtime_namespace).data_root());
    let harness =
        PodRuntimeHarness::new_with_runtime_config(crate::runtime::service::RuntimeConfig {
            node_name: "test-node".into(),
            service_cidr: "10.43.128.0/17".into(),
            containerd_namespace: runtime_namespace.into(),
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
            "namespace": "container-runtime",
            "name": "termination-message-pod",
            "uid": "uid-termination-mock-create",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "containers": [{
                "name": "termination-message-container",
                "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1",
                "imagePullPolicy": "Never",
                "terminationMessagePath": "/tmp/termination-message"
            }]
        },
        "status": {"phase": "Pending"}
    });
    let key = PodRuntimeKey::new(
        "container-runtime",
        "termination-message-pod",
        "uid-termination-mock-create",
    );
    let direct_host_path = kubelet_runtime_paths_for_test(runtime_namespace)
        .containerd_termination_log(
            "container-runtime",
            "termination-message-pod",
            "termination-message-container",
        );

    harness.create_runtime_pod(pod.clone()).await;
    let start = harness.start_pod_through_runtime(key, pod).await;
    assert!(matches!(start, PodStartResult::Started { .. }));
    assert!(
        !direct_host_path.exists(),
        "RealPodRuntimeService must not create termination logs outside PodFilesystem"
    );

    let _ = std::fs::remove_dir_all(kubelet_runtime_paths_for_test(runtime_namespace).data_root());
}

#[tokio::test]
async fn mocked_runtime_does_not_read_termination_message_file_directly() {
    use crate::pod_repository::PodStatusUpdate;
    use crate::pod_repository::PublishedAddress;
    use crate::runtime::cri::ContainerRuntimeState;
    use crate::runtime::store::PodRuntimeStore;

    let runtime_namespace = "klights-term-mock-read-test";
    let _ = std::fs::remove_dir_all(kubelet_runtime_paths_for_test(runtime_namespace).data_root());
    let harness =
        PodRuntimeHarness::new_with_runtime_config(crate::runtime::service::RuntimeConfig {
            node_name: "test-node".into(),
            service_cidr: "10.43.128.0/17".into(),
            containerd_namespace: runtime_namespace.into(),
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
            "namespace": "container-runtime",
            "name": "termination-message-pod",
            "uid": "uid-termination-mock-read",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "restartPolicy": "Never",
            "containers": [{
                "name": "termination-message-container",
                "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1",
                "imagePullPolicy": "Never"
            }]
        },
        "status": {
            "phase": "Running",
            "containerStatuses": [{
                "name": "termination-message-container",
                "containerID": "containerd://ctr-term",
                "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1",
                "imageID": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1",
                "ready": true,
                "started": true,
                "restartCount": 0,
                "state": {"running": {"startedAt": "2026-05-19T21:13:36Z"}}
            }]
        }
    });
    let key = PodRuntimeKey::new(
        "container-runtime",
        "termination-message-pod",
        "uid-termination-mock-read",
    );
    harness.create_runtime_pod(pod.clone()).await;
    harness
        .repo
        .pod_status_writer
        .set_pod_status_for_uid(
            "container-runtime",
            "termination-message-pod",
            "uid-termination-mock-read",
            PodStatusUpdate {
                phase: "Running".to_string(),
                pod_ip: PublishedAddress::must("10.50.1.3"),
                host_ip: None,
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
        .record_sandbox(&key, "sandbox-termination")
        .await
        .unwrap();
    harness
        .container_control
        .set_container_states(vec![("ctr-term".into(), ContainerRuntimeState::Exited)]);
    harness
        .cri
        .set_container_status_state(k8s_cri::v1::ContainerState::ContainerExited as i32);
    harness.cri.set_container_exit_code(0);
    let direct_host_path = kubelet_runtime_paths_for_test(runtime_namespace)
        .containerd_termination_log(
            "container-runtime",
            "termination-message-pod",
            "termination-message-container",
        );
    std::fs::create_dir_all(direct_host_path.parent().unwrap()).unwrap();
    std::fs::write(&direct_host_path, "direct-fs-message").unwrap();

    harness.reconcile_runtime(key.clone()).await;

    let updated = harness.stored_pod(&key).await;
    assert_ne!(
        updated.pointer("/status/containerStatuses/0/state/terminated/message"),
        Some(&serde_json::json!("direct-fs-message")),
        "RealPodRuntimeService must read termination messages through PodFilesystem"
    );

    let _ = std::fs::remove_dir_all(kubelet_runtime_paths_for_test(runtime_namespace).data_root());
}

#[tokio::test]
async fn termination_message_mount_path_with_parity() {
    let runtime_namespace = "klights-term-mount-test";
    let harness =
        PodRuntimeHarness::new_with_runtime_config(crate::runtime::service::RuntimeConfig {
            node_name: "test-node".into(),
            service_cidr: "10.43.128.0/17".into(),
            containerd_namespace: runtime_namespace.into(),
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
            "namespace": "container-runtime",
            "name": "termination-message-pod",
            "uid": "uid-termination-mount",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "containers": [{
                "name": "termination-message-container",
                "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1",
                "imagePullPolicy": "Never",
                "terminationMessagePath": "/tmp/termination-message"
            }]
        },
        "status": {"phase": "Pending"}
    });
    let key = PodRuntimeKey::new(
        "container-runtime",
        "termination-message-pod",
        "uid-termination-mount",
    );

    harness.create_runtime_pod(pod.clone()).await;
    let start = harness
        .start_pod_through_runtime(key.clone(), pod.clone())
        .await;
    assert!(matches!(start, PodStartResult::Started { .. }));

    let create_configs = harness.cri.recorded_create_configs();
    let config = create_configs
        .first()
        .expect("container config must be created");
    let expected_host_path = format!(
        "mock://termination/{}/{}/{}/{}",
        key.namespace, key.name, key.uid, "termination-message-container"
    );
    assert!(
        config.mounts.iter().any(|mount| {
            mount.container_path == "/tmp/termination-message"
                && mount.host_path == expected_host_path
                && !mount.readonly
        }),
        "terminationMessagePath must be backed by a host termination log mount"
    );
    assert!(harness.filesystem.recorded_calls().iter().any(|call| {
        call == "ensure_termination_log:container-runtime/termination-message-pod/uid-termination-mount/termination-message-container"
    }));

    let _ = std::fs::remove_dir_all(kubelet_runtime_paths_for_test(runtime_namespace).data_root());
}

#[tokio::test]
async fn hosts_file_mount_path_with_parity() {
    let runtime_namespace = "klights-hosts-mount-test";
    let harness =
        PodRuntimeHarness::new_with_runtime_config(crate::runtime::service::RuntimeConfig {
            node_name: "test-node".into(),
            service_cidr: "10.43.128.0/17".into(),
            containerd_namespace: runtime_namespace.into(),
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
            "namespace": "kubelet-test",
            "name": "host-alias-pod",
            "uid": "uid-host-alias",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "hostAliases": [{
                "ip": "203.0.113.89",
                "hostnames": ["foo", "bar"]
            }],
            "containers": [{
                "name": "agnhost-container",
                "image": "registry.k8s.io/e2e-test-images/agnhost:2.54",
                "imagePullPolicy": "Never"
            }]
        },
        "status": {"phase": "Pending"}
    });
    let key = PodRuntimeKey::new("kubelet-test", "host-alias-pod", "uid-host-alias");

    harness.create_runtime_pod(pod.clone()).await;
    let start = harness
        .start_pod_through_runtime(key.clone(), pod.clone())
        .await;
    assert!(matches!(start, PodStartResult::Started { .. }));

    let create_configs = harness.cri.recorded_create_configs();
    let config = create_configs
        .first()
        .expect("container config must be created");
    let expected_host_path = crate::runtime_paths::KubeletRuntimePaths::new(
        std::path::PathBuf::from("/tmp/klights/runtime-test"),
    )
    .unwrap()
    .containerd_hosts_dir("kubelet-test", "host-alias-pod")
    .join("hosts")
    .to_string_lossy()
    .into_owned();
    assert!(
        config.mounts.iter().any(|mount| {
            mount.container_path == "/etc/hosts"
                && mount.host_path == expected_host_path
                && !mount.readonly
        }),
        "managed /etc/hosts must be mounted into containers so HostAliases are visible"
    );

    let _ = std::fs::remove_dir_all(kubelet_runtime_paths_for_test(runtime_namespace).data_root());
}

#[tokio::test]
async fn termination_message_file_handling_with_parity() {
    use crate::pod_repository::PodStatusUpdate;
    use crate::pod_repository::PublishedAddress;
    use crate::runtime::cri::ContainerRuntimeState;
    use crate::runtime::store::PodRuntimeStore;

    let runtime_namespace = "klights-term-read-test";
    let harness =
        PodRuntimeHarness::new_with_runtime_config(crate::runtime::service::RuntimeConfig {
            node_name: "test-node".into(),
            service_cidr: "10.43.128.0/17".into(),
            containerd_namespace: runtime_namespace.into(),
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
            "namespace": "container-runtime",
            "name": "termination-message-pod",
            "uid": "uid-termination-read",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "restartPolicy": "Never",
            "containers": [{
                "name": "termination-message-container",
                "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1",
                "imagePullPolicy": "Never",
                "terminationMessagePolicy": "FallbackToLogsOnError"
            }]
        },
        "status": {
            "phase": "Running",
            "containerStatuses": [{
                "name": "termination-message-container",
                "containerID": "containerd://ctr-term",
                "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1",
                "imageID": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1",
                "ready": true,
                "started": true,
                "restartCount": 0,
                "state": {"running": {"startedAt": "2026-05-19T21:13:36Z"}}
            }]
        }
    });
    let key = PodRuntimeKey::new(
        "container-runtime",
        "termination-message-pod",
        "uid-termination-read",
    );
    harness.create_runtime_pod(pod.clone()).await;
    harness
        .repo
        .pod_status_writer
        .set_pod_status_for_uid(
            "container-runtime",
            "termination-message-pod",
            "uid-termination-read",
            PodStatusUpdate {
                phase: "Running".to_string(),
                pod_ip: PublishedAddress::must("10.50.1.3"),
                host_ip: None,
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
        .record_sandbox(&key, "sandbox-termination")
        .await
        .unwrap();
    harness
        .container_control
        .set_container_states(vec![("ctr-term".into(), ContainerRuntimeState::Exited)]);
    harness
        .cri
        .set_container_status_state(k8s_cri::v1::ContainerState::ContainerExited as i32);
    harness.cri.set_container_exit_code(0);

    harness
        .filesystem
        .set_termination_message(&key, "termination-message-container", "OK");

    harness.reconcile_runtime(key.clone()).await;

    let updated = harness.stored_pod(&key).await;
    assert_eq!(
        updated.pointer("/status/containerStatuses/0/state/terminated/message"),
        Some(&serde_json::json!("OK"))
    );
    assert!(harness.filesystem.recorded_calls().iter().any(|call| {
        call == "read_termination_message:container-runtime/termination-message-pod/uid-termination-read/termination-message-container:FallbackToLogsOnError:0"
    }));

    let _ = std::fs::remove_dir_all(kubelet_runtime_paths_for_test(runtime_namespace).data_root());
}

#[tokio::test]
async fn real_runtime_start_pod_passes_log_directory_to_create_container_sandbox_config() {
    let harness = PodRuntimeHarness::new().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "logs",
            "name": "logger",
            "uid": "uid-logger",
            "resourceVersion": "1"
        },
        "spec": {
            "nodeName": "test-node",
            "containers": [{
                "name": "app",
                "image": "docker.io/library/busybox:1.36",
                "imagePullPolicy": "Never"
            }]
        },
        "status": {"phase": "Pending"}
    });
    let key = PodRuntimeKey::new("logs", "logger", "uid-logger");

    harness.create_runtime_pod(pod.clone()).await;
    let start = harness
        .start_pod_through_runtime(key.clone(), pod.clone())
        .await;
    assert!(matches!(start, PodStartResult::Started { .. }));

    let run_sandbox_config = harness
        .cri
        .recorded_sandbox_configs()
        .first()
        .expect("RunPodSandbox config must be recorded")
        .clone();
    let create_sandbox_config = harness
        .cri
        .recorded_create_sandbox_configs()
        .first()
        .expect("CreateContainer sandbox config must be recorded")
        .clone();
    assert!(
        !create_sandbox_config.log_directory.is_empty(),
        "CreateContainer sandbox config must keep log_directory so containerd enables CRI logs"
    );
    assert_eq!(
        create_sandbox_config.log_directory, run_sandbox_config.log_directory,
        "CreateContainer must receive the same sandbox log directory used for RunPodSandbox"
    );
}
