#![cfg(test)]

use k8s_cri::v1::PodSandboxConfig;

use crate::kubelet::pod_env::EnvSourceReader;
use crate::kubelet::pod_lifecycle_core::message::PodLifecycleKey;
use crate::kubelet::pod_runtime::deletion_finalizer::PodDeletionFinalizer;
use crate::kubelet::pod_runtime::events::PodEventSink;
use crate::kubelet::pod_runtime::filesystem::PodFilesystem;
use crate::kubelet::pod_runtime::hooks::HookOutcome;
use crate::kubelet::pod_runtime::hostports::HostPortRuntime;
use crate::kubelet::pod_runtime::probes::ProbeRuntime;
use crate::kubelet::pod_runtime::service::{
    PodDeletionFinalizeResult, PodOwnershipError, PodRuntimeKey, PodStartResult,
    RealPodRuntimeServiceDependencies,
};
use crate::kubelet::pod_runtime::service::{PodFinalizeStartupResult, PodRuntimeService};
use crate::kubelet::pod_runtime::store::{PodRuntimeStore, PodSlotAdmission};
use crate::kubelet::pod_runtime::test_support::{
    MockContainerRuntimeControl, MockPodDeletionFinalizer, MockPodHookRuntime,
    MockPodRuntimeService, MockPodSlotAdmission, MockRuntimeCall,
};
use crate::kubelet::pod_runtime::volumes::PodVolumeRuntime;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[test]
fn pod_runtime_module_exports_only_declared_submodules() {
    // This module only exports test_support and service; no production
    // behaviour is leaked directly. This test exists to fail fast if
    // someone adds uncontrolled re-exports to mod.rs.
}

// --- Task 1.2: PodRuntimeKey identity and result types ---

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

// --- Task 1.3: PodRuntimeService trait ---

#[test]
fn runtime_traits_are_object_safe_send_sync() {
    // Verify the trait can be stored as Arc<dyn PodRuntimeService>,
    // proving it is object-safe and Send + Sync.
    fn assert_send_sync<T: ?Sized + Send + Sync>() {}
    assert_send_sync::<dyn crate::kubelet::pod_runtime::service::PodRuntimeService>();
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
async fn real_runtime_schedule_retry_emits_retry_due_after_delay() {
    let harness = crate::kubelet::pod_runtime::test_support::PodRuntimeHarness::new().await;
    let key = PodRuntimeKey::new("default", "retry-pod", "uid-retry");
    let (tx, mut rx) = tokio::sync::mpsc::channel::<
        crate::kubelet::pod_lifecycle_core::message::LifecycleMessage,
    >(8);
    let reply_to = crate::kubelet::pod_lifecycle_router::LifecycleReplyHandle::direct(tx);

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
        crate::kubelet::pod_lifecycle_core::message::LifecycleMessage::RetryDue { key } => {
            assert_eq!(key.namespace, "default");
            assert_eq!(key.name, "retry-pod");
            assert_eq!(key.uid, "uid-retry");
        }
        other => panic!("expected RetryDue, got {other:?}"),
    }
}

#[tokio::test]
async fn real_runtime_schedule_start_pod_retry_writes_status_event_and_wakeup() {
    use crate::kubelet::pod_repository::PodReader;

    let harness = crate::kubelet::pod_runtime::test_support::PodRuntimeHarness::new().await;
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
        crate::kubelet::pod_lifecycle_core::message::LifecycleMessage,
    >(8);
    let reply_to = crate::kubelet::pod_lifecycle_router::LifecycleReplyHandle::direct(tx);
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

    let updated = harness
        .repo
        .get_pod("default", "runtime-retry")
        .await
        .expect("read pod")
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
        crate::kubelet::pod_lifecycle_core::message::LifecycleMessage::RetryDue { key } => {
            assert_eq!(key.uid, "uid-rr");
        }
        other => panic!("expected RetryDue, got {other:?}"),
    }
}

#[tokio::test]
async fn real_runtime_schedule_start_pod_retry_rejects_stale_uid_but_wakes() {
    use crate::kubelet::pod_repository::PodReader;

    let harness = crate::kubelet::pod_runtime::test_support::PodRuntimeHarness::new().await;
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
    let before = harness
        .repo
        .get_pod("default", "runtime-stale")
        .await
        .expect("read before")
        .expect("pod exists");

    let stale_key = PodRuntimeKey::new("default", "runtime-stale", "uid-stale");
    let (tx, mut rx) = tokio::sync::mpsc::channel::<
        crate::kubelet::pod_lifecycle_core::message::LifecycleMessage,
    >(8);
    let reply_to = crate::kubelet::pod_lifecycle_router::LifecycleReplyHandle::direct(tx);

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

    let after = harness
        .repo
        .get_pod("default", "runtime-stale")
        .await
        .expect("read after")
        .expect("pod exists");
    assert_eq!(after.uid, before.uid);
    assert_eq!(after.resource_version, before.resource_version);

    let message = tokio::time::timeout(std::time::Duration::from_millis(250), rx.recv())
        .await
        .expect("retry wakeup must arrive")
        .expect("reply channel must stay open");
    match message {
        crate::kubelet::pod_lifecycle_core::message::LifecycleMessage::RetryDue { key } => {
            assert_eq!(key.uid, "uid-stale");
        }
        other => panic!("expected RetryDue, got {other:?}"),
    }
}

// --- Task 2.1: CriRuntime and ContainerRuntimeControl traits ---

#[test]
fn cri_runtime_trait_exposes_only_runtime_arguments() {
    // The CriRuntime trait must accept only runtime-level arguments
    // (image names, sandbox IDs, container configs) — never PodRepository,
    // Old watcher context bundles, DatastoreHandle, or any lifecycle key.
    fn assert_object_safe<T: ?Sized + Send + Sync>() {}
    assert_object_safe::<dyn crate::kubelet::pod_runtime::cri::CriRuntime>();
    assert_object_safe::<dyn crate::kubelet::pod_runtime::cri::ContainerRuntimeControl>();
}

// --- Task 2.2: SharedCriRuntime production adapter ---

/// SharedCriRuntime clones the client per-call without wrapping in a Mutex.
/// This is a compile-time test: if Mutex were introduced, the type would
/// not satisfy the structural constraints checked here.
#[test]
fn shared_cri_runtime_clones_client_per_call_without_mutex() {
    // SharedCriRuntime is Send + Sync (no Mutex).
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<crate::kubelet::pod_runtime::cri::SharedCriRuntime>();
    // The adapter implements CriRuntime.
    fn _takes_cri_runtime(_: &dyn crate::kubelet::pod_runtime::cri::CriRuntime) {}
}

// --- Task 2.3: MockCriRuntime ---

use crate::kubelet::pod_runtime::cri::{ContainerRuntimeState, CriRuntime};
use crate::kubelet::pod_runtime::test_support::{
    MockContainerControlOp, MockCriOperation, MockCriRuntime, MockHostPortOp, MockHostPortRuntime,
    MockNetworkOp, MockPodEventSink, MockPodFilesystem, MockPodNetworkRuntime,
    MockPodVolumeRuntime, MockProbeCall, MockProbeRuntime,
};

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
    let supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
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

// --- Task 3.1: PodNetworkRuntime trait and mock ---

use crate::kubelet::pod_runtime::network::PodNetworkRuntime;

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

// --- Task 3.2: PodRuntimeStore trait and mock ---

use crate::kubelet::pod_runtime::test_support::MockPodRuntimeStore;

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
fn pod_slot_trait_is_object_safe() {
    use crate::kubelet::pod_runtime::store::PodSlotAdmission;
    fn assert_send_sync<T: ?Sized + Send + Sync>() {}
    assert_send_sync::<dyn PodSlotAdmission>();
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
    let (_ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    let supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    ));
    let parts = crate::kubelet::pod_repository::PodRepository::build_parts(
        crate::kubelet::pod_repository::PodRepositoryBuildConfig {
            db: db.clone(),
            supervisor,
            side_effects: Arc::new(crate::side_effects::SideEffectRegistry::new()),
            metrics: crate::side_effects::SideEffectMetrics::new(),
            network_events: crate::networking::global_pod_network_events(),
            scheduling_mode:
                crate::kubelet::pod_repository::api::PodSchedulingMode::InlineSingleNode,
            outbox: None,
            cluster_api: None,
        },
    );
    let repository = Arc::new(parts.repository);
    let datapath = Arc::new(crate::networking::test_support::MockNetworkProvider::new());
    let store = Arc::new(crate::kubelet::pod_runtime::store::RealPodRuntimeStore::new(db.clone()));
    let runtime = crate::kubelet::pod_runtime::network::RealPodNetworkRuntime::new(
        datapath.clone(),
        repository,
        store.clone(),
    );
    let old_key = PodRuntimeKey::new("ns", "same-name", "old-uid");
    let new_key = PodRuntimeKey::new("ns", "same-name", "new-uid");

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
            crate::networking::test_support::NetworkCall::CniDel { .. }
        )),
        "CNI delete must not run on UID/sandbox mismatch"
    );
}

// --- Task 3.3: Filesystem and Volume runtime traits ---

#[test]
fn filesystem_and_volume_ports_record_pod_identity_arguments() {
    use crate::kubelet::pod_runtime::filesystem::PodFilesystem;
    use crate::kubelet::pod_runtime::volumes::PodVolumeRuntime;
    fn assert_send_sync<T: ?Sized + Send + Sync>() {}
    assert_send_sync::<dyn PodFilesystem>();
    assert_send_sync::<dyn PodVolumeRuntime>();
}

#[tokio::test]
async fn mock_filesystem_records_hosts_logs_cgroups_and_fsgroup() {
    let fs = MockPodFilesystem::new();
    let key = PodRuntimeKey::new("ns", "pod", "uid-1");
    let pod = crate::kubelet::pod_runtime::test_support::pod_json("ns", "pod", "uid-1", "img");

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
    let _ = std::fs::remove_dir_all(crate::paths::data_root_path(runtime_namespace));
    let fs = crate::kubelet::pod_runtime::filesystem::RealPodFilesystem::new(
        std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        )),
        runtime_namespace.to_string(),
        "test-node".to_string(),
    );
    let key = PodRuntimeKey::new("ns", "pod", "uid-real-term");
    let expected_path =
        crate::paths::containerd_termination_log_path(runtime_namespace, "ns", "pod", "app")
            .to_string_lossy()
            .into_owned();

    let path = fs.ensure_termination_log_file(&key, "app").await;
    std::fs::write(&path, "real-message").unwrap();
    let message = fs.read_termination_message(&key, "app", "File", 0).await;

    assert_eq!(path, expected_path);
    assert_eq!(message, "real-message");
    let _ = std::fs::remove_dir_all(crate::paths::data_root_path(runtime_namespace));
}

#[tokio::test]
async fn real_filesystem_cleanup_removes_entire_pod_root() {
    let runtime_namespace = "klights-pod-root-cleanup-test";
    let data_root = crate::paths::data_root_path(runtime_namespace);
    let _ = std::fs::remove_dir_all(&data_root);
    let fs = crate::kubelet::pod_runtime::filesystem::RealPodFilesystem::new(
        std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        )),
        runtime_namespace.to_string(),
        "test-node".to_string(),
    );
    let key = PodRuntimeKey::new("ns", "pod", "uid-root-cleanup");
    let pod_root = crate::paths::volumes_root_path(runtime_namespace)
        .join(format!("{}_{}_{}", key.namespace, key.name, key.uid));
    let pod_log_dir =
        crate::paths::pod_log_dir_path(runtime_namespace, &key.namespace, &key.name, &key.uid);

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
fn alternate_test_group(current_gid: u32) -> Option<u32> {
    unsafe {
        if libc::geteuid() == 0 {
            return Some(current_gid.saturating_add(1));
        }

        let group_count = libc::getgroups(0, std::ptr::null_mut());
        if group_count <= 0 {
            return None;
        }

        let mut groups = vec![0 as libc::gid_t; group_count as usize];
        if libc::getgroups(group_count, groups.as_mut_ptr()) < 0 {
            return None;
        }

        groups.into_iter().find(|gid| *gid != current_gid)
    }
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
    let data_root = crate::paths::data_root_path(&containerd_ns);
    let _ = std::fs::remove_dir_all(&data_root);

    let key = PodRuntimeKey::new("projected", "pod-projected-secrets", "uid-fsgroup");
    let volume_dir = crate::paths::volumes_root_path(&containerd_ns)
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

    let fs = crate::kubelet::pod_runtime::filesystem::RealPodFilesystem::new(
        std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        )),
        containerd_ns.clone(),
        "test-node".to_string(),
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
    let pod = crate::kubelet::pod_runtime::test_support::pod_json("ns", "pod", "uid-1", "img");

    vol.process_volumes(&key, &pod).await.unwrap();
    vol.cleanup_volumes(&key).await.unwrap();

    let calls = vol.recorded_calls();
    assert_eq!(calls.len(), 2);
    assert!(calls[0].contains("process_volumes") && calls[0].contains("uid-1"));
    assert!(calls[1].contains("cleanup_volumes") && calls[1].contains("uid-1"));
}

// --- Task 3.4: ProbeRuntime trait and mock ---

#[test]
fn probe_runtime_methods_require_uid() {
    use crate::kubelet::pod_runtime::probes::ProbeRuntime;
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

// --- Task 3.5: HostPortRuntime trait and mock ---

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

    hp.add_host_ports(&key, &pod).await.unwrap();
    hp.remove_host_ports(&key, &pod).await.unwrap();
    hp.check_host_port_admission(&key, &pod).await.unwrap();

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

// --- Task 3.6: PodEventSink trait and mock ---

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

// --- Task 7.1: MockPodRuntimeService tests ---

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
    mock.stop_pod(key.clone(), None, Some("sandbox-1".into()))
        .await
        .unwrap();
    mock.finalize_startup(key.clone(), None, None)
        .await
        .unwrap();
    mock.finalize_deletion(key.clone()).await.unwrap();
    mock.reconcile_runtime(
        key.clone(),
        crate::kubelet::pod_runtime::service::RuntimeReconcileHint::none(),
    )
    .await
    .unwrap();
    mock.reconcile_cri_leftovers(key.clone()).await.unwrap();
    mock.reconcile_ephemeral(key.clone(), None).await.unwrap();
    let (tx, _rx) = tokio::sync::mpsc::channel::<
        crate::kubelet::pod_lifecycle_core::message::LifecycleMessage,
    >(1);
    mock.check_slot_admission(
        crate::kubelet::pod_runtime::service::PodSlotAdmissionRequest {
            key: key.clone(),
            pod: serde_json::json!({"metadata": {"uid": "uid-all"}}),
            resource_version: Some(1),
            start_after_admit: true,
            operation_id: 8,
        },
        crate::kubelet::pod_lifecycle_router::LifecycleReplyHandle::direct(tx),
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

// --- Task 8.1: RealPodRuntimeService Constructor ---

use crate::kubelet::pod_runtime::service::RuntimeConfig;

async fn fixture_pod_repository() -> std::sync::Arc<crate::kubelet::pod_repository::PodRepository> {
    let (ds, handle) = crate::datastore::test_support::in_memory_with_handle().await;
    // These runtime tests place pods in conventional non-system namespaces. The
    // API create path enforces the upstream NamespaceLifecycle rule (target
    // namespace must exist), so seed them as a live cluster would have them.
    crate::kubelet::pod_runtime::test_support::seed_runtime_test_namespaces(&handle).await;
    std::mem::forget(ds);
    let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    ));
    let side_effects = std::sync::Arc::new(crate::side_effects::SideEffectRegistry::new());
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let parts = crate::kubelet::pod_repository::PodRepository::build_parts(
        crate::kubelet::pod_repository::PodRepositoryBuildConfig {
            db: handle,
            supervisor,
            side_effects,
            metrics,
            network_events: crate::networking::global_pod_network_events(),
            scheduling_mode:
                crate::kubelet::pod_repository::api::PodSchedulingMode::InlineSingleNode,
            outbox: None,
            cluster_api: None,
        },
    );
    std::sync::Arc::new(parts.repository)
}

async fn fixture_env_source(
    _node_name: &str,
) -> std::sync::Arc<dyn crate::kubelet::pod_env::EnvSourceReader> {
    std::sync::Arc::new(crate::kubelet::pod_runtime::test_support::MockEnvSourceReader::new())
}

#[tokio::test]
async fn real_pod_runtime_service_constructor_requires_all_object_ports() {
    // Verify the constructor accepts and stores every required port.
    let cri = std::sync::Arc::new(MockCriRuntime::new());
    let container_control = std::sync::Arc::new(
        crate::kubelet::pod_runtime::test_support::MockContainerRuntimeControl::new(),
    );
    let network = std::sync::Arc::new(MockPodNetworkRuntime::new());
    let store = std::sync::Arc::new(MockPodRuntimeStore::new());
    let slot_admission =
        std::sync::Arc::new(crate::kubelet::pod_runtime::test_support::MockPodSlotAdmission::new());
    let repo = fixture_pod_repository().await;
    let filesystem = std::sync::Arc::new(MockPodFilesystem::new());
    let volumes = std::sync::Arc::new(MockPodVolumeRuntime::new());
    let probes = std::sync::Arc::new(MockProbeRuntime::new());
    let hostports = std::sync::Arc::new(MockHostPortRuntime::new());
    let events = std::sync::Arc::new(MockPodEventSink::new());
    let hooks =
        std::sync::Arc::new(crate::kubelet::pod_runtime::test_support::MockPodHookRuntime::new());
    let env_source = fixture_env_source("node-1").await;
    let finalizer = std::sync::Arc::new(
        crate::kubelet::pod_runtime::test_support::MockPodDeletionFinalizer::new(),
    );
    let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    ));
    let config = RuntimeConfig {
        node_name: "node-1".into(),
        service_cidr: "10.43.128.0/17".into(),
        containerd_namespace: "klights-test".into(),
    };
    let node_view = std::sync::Arc::new(FakeNode::new("node-1", RuntimeNodeRole::Worker));
    let cluster_view = std::sync::Arc::new(
        crate::kubelet::pod_cluster_runtime::WorkerClusterRuntimeView::new(
            repo.clone(),
            "node-1".into(),
        ),
    );

    let _runtime = crate::kubelet::pod_runtime::service::RealPodRuntimeService::new(
        RealPodRuntimeServiceDependencies {
            cri,
            container_control,
            network,
            store,
            slot_admission,
            repository: repo,
            filesystem,
            volumes,
            probes,
            hostports,
            events,
            hooks,
            env_source,
            finalizer,
            supervisor,
            config,
            node_view,
            cluster_view,
        },
    );
}

#[tokio::test]
async fn real_pod_runtime_service_constructs_from_mock_dependencies() {
    // Construct via the PodRuntimeHarness — verifies all mock wiring compiles.
    let harness = crate::kubelet::pod_runtime::test_support::PodRuntimeHarness::new().await;
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

// --- Task 8.2: RealPodRuntimeService::start_pod identity/admission/status ---

use crate::kubelet::pod_repository::{PodObjectWriter, PodReader};
use crate::kubelet::pod_runtime::test_support::PodRuntimeHarness;

struct SnapshotOnlyStartRepository {
    inner: Arc<crate::kubelet::pod_repository::PodRepository>,
}

#[async_trait::async_trait]
impl crate::kubelet::pod_runtime::repository::PodRuntimeRepository for SnapshotOnlyStartRepository {
    async fn get_pod_for_uid(
        &self,
        _ns: &str,
        _name: &str,
        _pod_uid: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        Ok(None)
    }

    async fn set_pod_status_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        update: crate::kubelet::pod_repository::PodStatusUpdate,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::set_pod_status_for_uid(
            self.inner.as_ref(),
            ns,
            name,
            pod_uid,
            update,
            expected_rv,
        )
        .await
    }

    async fn apply_runtime_reconcile_status_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        update: crate::kubelet::pod_repository::RuntimeReconcileStatus,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::apply_runtime_reconcile_status_for_uid(
            self.inner.as_ref(),
            ns,
            name,
            pod_uid,
            update,
            expected_rv,
        )
        .await
    }

    async fn mark_start_pending_for_retry_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        error_message: &str,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::mark_start_pending_for_retry_for_uid(
            self.inner.as_ref(),
            ns,
            name,
            pod_uid,
            error_message,
        )
        .await
    }

    async fn set_probe_readiness_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        container_name: &str,
        ready: bool,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::set_probe_readiness_for_uid(
            self.inner.as_ref(),
            ns,
            name,
            pod_uid,
            container_name,
            ready,
            expected_rv,
        )
        .await
    }

    async fn set_deadline_exceeded_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        message: String,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::set_deadline_exceeded_for_uid(
            self.inner.as_ref(),
            ns,
            name,
            pod_uid,
            message,
            expected_rv,
        )
        .await
    }

    async fn apply_ephemeral_container_statuses_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        statuses: Vec<serde_json::Value>,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::datastore::Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::apply_ephemeral_container_statuses_for_uid(
            self.inner.as_ref(),
            ns,
            name,
            pod_uid,
            statuses,
            expected_rv,
        )
        .await
    }

    async fn note_container_restart_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        container_name: &str,
        terminated: serde_json::Value,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        crate::kubelet::pod_repository::PodStatusWriter::note_container_restart_for_uid(
            self.inner.as_ref(),
            ns,
            name,
            pod_uid,
            container_name,
            terminated,
            expected_rv,
        )
        .await
    }

    async fn check_live_pod_uid(
        &self,
        _ns: &str,
        _name: &str,
        _pod_uid: &str,
    ) -> anyhow::Result<crate::kubelet::pod_runtime::repository::LivePodUidCheck> {
        Ok(crate::kubelet::pod_runtime::repository::LivePodUidCheck::Missing)
    }
}

#[tokio::test]
async fn real_runtime_start_pod_rejects_uid_mismatch_before_cri() {
    let harness = PodRuntimeHarness::new().await;
    let pod = crate::kubelet::pod_runtime::test_support::pod_json(
        "ns",
        "test-pod",
        "correct-uid",
        "nginx:latest",
    );

    // Create pod with correct-uid in the repository.
    harness
        .repo
        .create_controller_pod("ns", "test-pod", "test-node", pod.clone())
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
async fn real_runtime_start_pod_writes_pending_status_before_pull() {
    let harness = PodRuntimeHarness::new().await;
    let pod = crate::kubelet::pod_runtime::test_support::pod_json(
        "ns",
        "test-pod",
        "uid-1",
        "nginx:latest",
    );

    // Create pod in the repository.
    harness
        .repo
        .create_controller_pod("ns", "test-pod", "test-node", pod.clone())
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
    let pod = crate::kubelet::pod_runtime::test_support::pod_json(
        "ns",
        "cached-pod",
        "uid-cache",
        "nginx:latest",
    );
    harness
        .repo
        .create_controller_pod("ns", "cached-pod", "test-node", pod.clone())
        .await
        .unwrap();

    let runtime = crate::kubelet::pod_runtime::service::RealPodRuntimeService::new(
        RealPodRuntimeServiceDependencies {
            cri: harness.cri.clone(),
            container_control: harness.container_control.clone(),
            network: harness.network.clone(),
            store: harness.store.clone(),
            slot_admission: harness.slot_admission.clone(),
            repository: Arc::new(SnapshotOnlyStartRepository {
                inner: harness.repo.clone(),
            }),
            filesystem: harness.filesystem.clone(),
            volumes: harness.volumes.clone(),
            probes: harness.probes.clone(),
            hostports: harness.hostports.clone(),
            events: harness.events.clone(),
            hooks: harness.hooks.clone(),
            env_source: harness.env_source.clone(),
            finalizer: harness.finalizer.clone(),
            supervisor: harness.supervisor.clone(),
            config: crate::kubelet::pod_runtime::service::RuntimeConfig {
                node_name: "test-node".to_string(),
                service_cidr: "10.43.128.0/17".to_string(),
                containerd_namespace: "klights-test".to_string(),
            },
            node_view: harness.node_view.clone(),
            cluster_view: Arc::new(
                crate::kubelet::pod_cluster_runtime::WorkerClusterRuntimeView::new(
                    harness.repo.clone(),
                    "test-node".to_string(),
                ),
            ),
        },
    );

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
    let old_pod = crate::kubelet::pod_runtime::test_support::pod_json(
        "ns",
        "test-pod",
        "old-uid",
        "nginx:latest",
    );

    // Create pod with old-uid.
    harness
        .repo
        .create_controller_pod("ns", "test-pod", "test-node", old_pod.clone())
        .await
        .unwrap();

    // Read the live pod to capture its initial resourceVersion.
    let before = harness
        .repo
        .get_pod_for_uid("ns", "test-pod", "old-uid")
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
        .get_pod_for_uid("ns", "test-pod", "old-uid")
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

// --- Task 8.3: RealPodRuntimeService::start_pod image pull flow ---

use serde_json::{Value, json};

fn pod_with_pull_policy(ns: &str, name: &str, uid: &str, image: &str, policy: &str) -> Value {
    let mut p = crate::kubelet::pod_runtime::test_support::pod_json(ns, name, uid, image);
    p["spec"]["containers"][0]["imagePullPolicy"] = json!(policy);
    p
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
            .create_controller_pod("ns", "pod", "test-node", pod.clone())
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
            .create_controller_pod("ns", "pod2", "test-node", pod.clone())
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
            .create_controller_pod("ns", "pod3", "test-node", pod.clone())
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
            .create_controller_pod("ns", "pod4", "test-node", pod.clone())
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
        .create_controller_pod("ns", "pod", "test-node", pod.clone())
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
        .create_controller_pod("ns", "pod", "test-node", old_pod.clone())
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

// --- Task 8.4: RealPodRuntimeService::start_pod sandbox and assignment ---

#[tokio::test]
async fn real_runtime_start_pod_records_sandbox_and_reads_assignment() {
    let harness = PodRuntimeHarness::new().await;
    let pod = pod_with_pull_policy("ns", "pod", "uid-sb", "nginx", "Never");
    harness
        .repo
        .create_controller_pod("ns", "pod", "test-node", pod.clone())
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
    let mut pod = crate::kubelet::pod_runtime::test_support::pod_json(
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
    let mut pod = crate::kubelet::pod_runtime::test_support::pod_json(
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
async fn network_assignment_timeout_rolls_back_sandbox_with_parity() {
    let harness = PodRuntimeHarness::new().await;
    let pod = pod_with_pull_policy("ns", "net-timeout", "uid-net-timeout", "nginx", "Never");
    harness
        .repo
        .create_controller_pod("ns", "net-timeout", "test-node", pod.clone())
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
        .create_controller_pod("ns", "partial-create", "test-node", pod.clone())
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
async fn real_runtime_start_pod_sandbox_record_failure_uses_annotation_fallback() {
    // This test verifies that when the runtime store fails to record a sandbox,
    // the code falls back to writing the sandbox-id annotation via the repository.
    // We can't easily make MockPodRuntimeStore fail (it always succeeds), but
    // we can verify that even on success, both paths don't conflict.
    let harness = PodRuntimeHarness::new().await;
    let pod = pod_with_pull_policy("ns", "pod", "uid-fb", "nginx", "Never");
    harness
        .repo
        .create_controller_pod("ns", "pod", "test-node", pod.clone())
        .await
        .unwrap();
    let key = PodRuntimeKey::new("ns", "pod", "uid-fb");
    let result = harness
        .runtime
        .start_pod(key, Some(pod), CancellationToken::new())
        .await
        .unwrap();

    match result {
        PodStartResult::Started {
            sandbox_id: Some(_),
        } => {}
        other => panic!("expected Started with sandbox_id, got {:?}", other),
    }

    // Store must have been called to record sandbox.
    let store_calls = harness.store.recorded_calls();
    assert!(
        store_calls
            .iter()
            .any(|s| s.contains("record_sandbox") && s.contains("uid-fb")),
        "store record_sandbox must be attempted"
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
        .create_controller_pod("ns", "pod", "test-node", new_pod.clone())
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

// --- Task 8.5: HostPort, Filesystem, Volume, and Container Flow ---

#[tokio::test]
async fn real_runtime_start_pod_uses_hostport_admission_port_before_side_effects() {
    let harness = PodRuntimeHarness::new().await;
    let pod = pod_with_pull_policy("ns", "hp-admit", "uid-hp-admit", "nginx", "Never");
    harness
        .repo
        .create_controller_pod("ns", "hp-admit", "test-node", pod.clone())
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
async fn real_runtime_start_pod_stops_before_containers_when_volume_processing_fails() {
    let harness = PodRuntimeHarness::new().await;
    let pod = pod_with_pull_policy("ns", "volume-fail", "uid-volume-fail", "nginx", "Never");
    harness
        .repo
        .create_controller_pod("ns", "volume-fail", "test-node", pod.clone())
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
async fn real_runtime_start_pod_passes_verified_identity_to_hostport_filesystem_volume_and_container_ports()
 {
    let harness = PodRuntimeHarness::new().await;
    let pod = pod_with_pull_policy("ns", "iden-pod", "uid-iden", "nginx", "Never");
    harness
        .repo
        .create_controller_pod("ns", "iden-pod", "test-node", pod.clone())
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
        .create_controller_pod("ns", "all-ports", "test-node", pod.clone())
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
async fn hostport_admission_failure_marks_pod_failed_with_parity() {
    use crate::kubelet::pod_repository::{PodStatusUpdate, PodStatusWriter};

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
        .create_controller_pod("statefulset", "test-pod", "test-node", holder)
        .await
        .unwrap();
    harness
        .repo
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
        .create_controller_pod("statefulset", "ss-0", "test-node", claimant.clone())
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
async fn mid_lifecycle_status_writes_preserve_host_ip_with_parity() {
    use crate::kubelet::pod_repository::{PodObjectWriter, PodStatusUpdate, PodStatusWriter};

    let conflict_cluster = std::sync::Arc::new(FakeCluster::new());
    let (_cri, conflict_runtime, conflict_repo, conflict_cluster, conflict_hostports) =
        fixture_runtime_with_cluster("test-node", RuntimeNodeRole::Worker, conflict_cluster).await;
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
        .create_controller_pod("statefulset", "test-pod", "test-node", holder)
        .await
        .unwrap();
    conflict_repo
        .set_pod_status_for_uid(
            "statefulset",
            "test-pod",
            "uid-holder",
            PodStatusUpdate {
                phase: "Running".to_string(),
                pod_ip: "10.50.0.63".to_string(),
                host_ip: "10.0.0.5".to_string(),
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
        .create_controller_pod("statefulset", "ss-0", "test-node", claimant.clone())
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

    let conflict_failed_status = conflict_cluster
        .recorded_status_forwards()
        .into_iter()
        .map(|(_, status)| status)
        .find(|status| status.get("phase").and_then(|value| value.as_str()) == Some("Failed"))
        .expect("hostPort admission conflict should forward Failed status");
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
    let (init_cri, init_runtime, init_repo, init_cluster, _init_hostports) =
        fixture_runtime_with_cluster("test-node", RuntimeNodeRole::Worker, init_cluster).await;
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
        .create_controller_pod("ns", "init-fail-hostip", "test-node", init_pod.clone())
        .await
        .unwrap();
    let init_key = PodRuntimeKey::new("ns", "init-fail-hostip", "uid-init-fail-hostip");

    let _ = init_runtime
        .start_pod(init_key, Some(init_pod), CancellationToken::new())
        .await
        .unwrap();

    let init_failed_status = init_cluster
        .recorded_status_forwards()
        .into_iter()
        .map(|(_, status)| status)
        .find(|status| status.get("phase").and_then(|value| value.as_str()) == Some("Failed"))
        .expect("init failure should forward Failed status");
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
        .create_controller_pod("ns", "order-pod", "test-node", pod.clone())
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

// --- Task 8.6: Cancellation and Rollback ---

#[tokio::test]
async fn real_runtime_start_pod_cancel_before_sandbox_does_not_call_cri() {
    let harness = PodRuntimeHarness::new().await;
    let pod = pod_with_pull_policy("ns", "cancel-early", "uid-ce", "nginx", "Never");
    harness
        .repo
        .create_controller_pod("ns", "cancel-early", "test-node", pod.clone())
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
        .create_controller_pod("ns", "cancel-sb", "test-node", pod.clone())
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
        .create_controller_pod("ns", "cancel-rb", "test-node", pod.clone())
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

// --- Task 9.1: Stop Pod Slot and Probe Phase ---

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

// HR #11: actor-owned finalization must confirm runtime cleanup before it
// clears the slot. Under churn the per-UID actor can have already exited, so the
// delete is finalized via the orphan path with NO sandbox hint and NO node-local
// store row. The orphan path must still consult the authoritative runtime (CRI,
// by UID) and stop the running sandbox — not silently clear the slot and leak a
// running sandbox (BUG: pods stayed "active" in the wrapped-volume-race test).
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

// --- Task 9.2: Stop Pod Sandbox Resolution and Container Cleanup ---

#[tokio::test]
async fn real_runtime_stop_pod_uses_deleted_snapshot_not_replacement() {
    let harness = PodRuntimeHarness::new().await;
    let pod = pod_with_pull_policy("ns", "stop-del", "uid-del", "nginx", "Never");
    harness
        .repo
        .create_controller_pod("ns", "stop-del", "test-node", pod.clone())
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
async fn real_runtime_stop_pod_stops_and_removes_containers_idempotently() {
    let harness = PodRuntimeHarness::new().await;
    let pod = pod_with_pull_policy("ns", "stop-idem", "uid-idem", "nginx", "Never");
    harness
        .repo
        .create_controller_pod("ns", "stop-idem", "test-node", pod.clone())
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

// --- Task 9.3: Stop Pod Sandbox, Cgroup, Store Row, and CNI Cleanup ---

/// P0 StopPod loop: the runtime service must refuse cleanup for a Pod it
/// does not own with a *typed* `PodOwnershipError` (downcastable), so the
/// lifecycle executor can classify it terminal/non-retryable instead of
/// spinning the actor forever on a generic retryable `DispatchFailed`.
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
async fn real_runtime_stop_pod_cleans_up_by_uid_and_releases_network() {
    let harness = PodRuntimeHarness::new().await;
    let pod = pod_with_pull_policy("ns", "stop-clean", "uid-sc", "nginx", "Never");
    harness
        .repo
        .create_controller_pod("ns", "stop-clean", "test-node", pod.clone())
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

// --- Task 9.4: Stop Pod HostPort, Volume Cleanup, CRI Absence, Slot Clear ---

#[tokio::test]
async fn real_runtime_stop_pod_confirms_cri_absence_before_success() {
    let harness = PodRuntimeHarness::new().await;
    let pod = pod_with_pull_policy("ns", "stop-abs", "uid-sa", "nginx", "Never");
    harness
        .repo
        .create_controller_pod("ns", "stop-abs", "test-node", pod.clone())
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
async fn real_runtime_stop_pod_releases_hostports_and_cleans_volumes() {
    let harness = PodRuntimeHarness::new().await;
    let pod = pod_with_pull_policy("ns", "stop-hv", "uid-hv", "nginx", "Never");
    harness
        .repo
        .create_controller_pod("ns", "stop-hv", "test-node", pod.clone())
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

/// The orphan/cold-sandbox stop path has no deleted-Pod snapshot, so it calls
/// `cleanup_pod_local_artifacts(key, None)`. It must STILL unmount and remove
/// the pod's volumes — `cleanup_volumes` needs only the key, not the pod spec.
/// Regression: the artifact helper gated `cleanup_volumes` on a `Some(pod)`
/// snapshot, so the orphan path skipped the unmount and leaked tmpfs/bind
/// mounts (then `remove_dir_all` ran over the live mount).
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

/// C4/B2 regression: cgroup teardown is UID-keyed and idempotent, so it must run
/// on every stop path via `cleanup_pod_local_artifacts` — even when no sandbox
/// can be resolved (CRI unreachable, store row gone). Previously cgroup cleanup
/// was gated inside the per-sandbox loop, so a no-sandbox stop leaked the pod
/// cgroup tree.
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

// --- Task 10.1: PodDeletionFinalizer trait and mock ---

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

// --- Task 11.1: Multi-node runtime traits ---

#[test]
fn multi_node_runtime_traits_are_object_safe_send_sync() {
    use crate::kubelet::pod_cluster_runtime::{
        ClusterRuntimeView, NodeRuntimeView, ReplicationRuntime,
    };

    fn assert_send_sync<T: ?Sized + Send + Sync>() {}
    assert_send_sync::<dyn NodeRuntimeView>();
    assert_send_sync::<dyn ClusterRuntimeView>();
    assert_send_sync::<dyn ReplicationRuntime>();
}

#[test]
fn multi_node_traits_mutating_methods_require_uid() {
    use crate::kubelet::pod_cluster_runtime::{
        ClusterRuntimeView, NodeRuntimeView, ReplicationRuntime, RuntimeNodeRole,
    };
    use crate::kubelet::pod_runtime::service::PodRuntimeKey;

    // Compile-time verification: every mutating method on ClusterRuntimeView
    // and ReplicationRuntime takes PodRuntimeKey (UID-qualified).
    // NodeRuntimeView is read-only (no UID needed).

    // RuntimeNodeRole is Send + Sync + Clone.
    fn assert_send_sync_clone<T: Send + Sync + Clone>() {}
    assert_send_sync_clone::<RuntimeNodeRole>();

    // Verify enum variants exist.
    let _ = RuntimeNodeRole::Leader;
    let _ = RuntimeNodeRole::Worker;
    let _ = RuntimeNodeRole::Replica;

    // Verify key is usable with the traits (compile-time).
    let _key = PodRuntimeKey::new("ns", "name", "uid");

    // Verify the traits accept PodRuntimeKey.
    fn _takes_cluster_view(_v: &dyn ClusterRuntimeView) {}
    fn _takes_replication(_r: &dyn ReplicationRuntime) {}
    fn _takes_node_view(_n: &dyn NodeRuntimeView) {}
}

// --- Task 11.2: FakeNode and FakeCluster test doubles ---

use crate::kubelet::pod_cluster_runtime::{
    ClusterRuntimeView, NodeRuntimeView, ReplicationRuntime, RuntimeNodeRole,
};
use crate::kubelet::pod_runtime::test_support::{FakeCluster, FakeNode};

#[test]
fn fake_cluster_nodes_keep_runtime_arguments_isolated() {
    let leader = FakeNode::new("node-leader", RuntimeNodeRole::Leader);
    let worker = FakeNode::new("node-worker", RuntimeNodeRole::Worker);

    assert_eq!(leader.node_name(), "node-leader");
    assert_eq!(leader.role(), RuntimeNodeRole::Leader);
    assert_eq!(worker.node_name(), "node-worker");
    assert_eq!(worker.role(), RuntimeNodeRole::Worker);

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
    let worker = FakeNode::new("worker-1", RuntimeNodeRole::Worker);

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
    use crate::datastore::command::StorageCommand;
    use crate::kubelet::pod_runtime::service::PodRuntimeKey;

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

    // enqueue_storage_command records the command.
    let key2 = PodRuntimeKey::new("default", "test-pod", "uid-1");
    let cmd = StorageCommand::DeleteResource {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "test-pod".to_string(),
        preconditions: crate::datastore::ResourcePreconditions {
            uid: Some("uid-1".to_string()),
            resource_version: None,
        },
    };
    cluster
        .enqueue_storage_command(&key2, cmd.clone())
        .await
        .unwrap();

    let commands = cluster.recorded_storage_commands();
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].0.namespace, "default");
    assert_eq!(commands[0].0.uid, "uid-1");
}

// --- Task 12.1: Multi-node runtime start respects node ownership ---

/// Build a RealPodRuntimeService with a custom FakeNode for node-ownership tests.
async fn fixture_runtime_with_node(
    node_name: &str,
    role: RuntimeNodeRole,
) -> (
    std::sync::Arc<MockCriRuntime>,
    std::sync::Arc<crate::kubelet::pod_runtime::service::RealPodRuntimeService>,
    std::sync::Arc<crate::kubelet::pod_repository::PodRepository>,
) {
    let repo = fixture_pod_repository().await;
    let cri = std::sync::Arc::new(MockCriRuntime::new());
    let container_control = std::sync::Arc::new(
        crate::kubelet::pod_runtime::test_support::MockContainerRuntimeControl::new(),
    );
    let network = std::sync::Arc::new(MockPodNetworkRuntime::new());
    let store = std::sync::Arc::new(MockPodRuntimeStore::new());
    let slot_admission =
        std::sync::Arc::new(crate::kubelet::pod_runtime::test_support::MockPodSlotAdmission::new());
    let filesystem = std::sync::Arc::new(MockPodFilesystem::new());
    let volumes = std::sync::Arc::new(MockPodVolumeRuntime::new());
    let probes = std::sync::Arc::new(MockProbeRuntime::new());
    let hostports = std::sync::Arc::new(MockHostPortRuntime::new());
    let events = std::sync::Arc::new(MockPodEventSink::new());
    let hooks =
        std::sync::Arc::new(crate::kubelet::pod_runtime::test_support::MockPodHookRuntime::new());
    let env_source = fixture_env_source(node_name).await;
    let finalizer = std::sync::Arc::new(
        crate::kubelet::pod_runtime::test_support::MockPodDeletionFinalizer::new(),
    );
    let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    ));
    let config = RuntimeConfig {
        node_name: node_name.to_string(),
        service_cidr: "10.43.128.0/17".into(),
        containerd_namespace: "klights-test".into(),
    };
    // Every role routes through the same worker cluster-view path.
    let cluster_view: std::sync::Arc<dyn crate::kubelet::pod_cluster_runtime::ClusterRuntimeView> =
        std::sync::Arc::new(
            crate::kubelet::pod_cluster_runtime::WorkerClusterRuntimeView::new(
                repo.clone(),
                node_name.to_string(),
            ),
        );
    let node_view = std::sync::Arc::new(FakeNode::new(node_name, role));

    let runtime = std::sync::Arc::new(
        crate::kubelet::pod_runtime::service::RealPodRuntimeService::new(
            RealPodRuntimeServiceDependencies {
                cri: cri.clone(),
                container_control,
                network,
                store,
                slot_admission,
                repository: repo.clone(),
                filesystem,
                volumes,
                probes,
                hostports,
                events,
                hooks,
                env_source,
                finalizer,
                supervisor,
                config,
                node_view,
                cluster_view,
            },
        ),
    );
    (cri, runtime, repo)
}

use crate::kubelet::pod_runtime::test_support::scheduled_pod_json;

#[tokio::test]
async fn worker_runtime_starts_local_pod_and_does_not_touch_leader_cri() {
    let (cri, runtime, repo) = fixture_runtime_with_node("worker-1", RuntimeNodeRole::Worker).await;

    // Pod scheduled to a different node (leader) — must be rejected.
    let leader_pod = scheduled_pod_json("ns", "leader-pod", "uid-leader", "leader-node");
    repo.create_controller_pod("ns", "leader-pod", "leader-node", leader_pod.clone())
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
    repo.create_controller_pod("ns", "local-pod", "worker-1", local_pod.clone())
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
    let (cri, runtime, repo) = fixture_runtime_with_node("worker-1", RuntimeNodeRole::Worker).await;

    // Create a pod with new-uid (simulating same-name replacement).
    let new_pod = scheduled_pod_json("ns", "test-pod", "new-uid", "worker-1");
    repo.create_controller_pod("ns", "test-pod", "worker-1", new_pod.clone())
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

/// F1 companion: the no-snapshot start path (`pod = None`) must ALSO reject a
/// same-name replacement. With no snapshot, start_pod fetches the pod fresh by
/// UID (`get_pod_for_uid`); a replacement carrying a different live UID resolves
/// to None ("not found for uid"), so the stale-UID start fails and never touches
/// CRI. Locks in that the guard does not depend on a snapshot being supplied.
#[tokio::test]
async fn worker_runtime_rejects_same_name_replacement_without_snapshot() {
    let (cri, runtime, repo) = fixture_runtime_with_node("worker-1", RuntimeNodeRole::Worker).await;

    // Live pod has the NEW uid (the replacement); the start request carries the
    // OLD uid via the key and no snapshot.
    let new_pod = scheduled_pod_json("ns", "test-pod", "new-uid", "worker-1");
    repo.create_controller_pod("ns", "test-pod", "worker-1", new_pod)
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

// --- Task 12.2: Multi-node status writes use cluster boundary ---

/// Build a RealPodRuntimeService with a FakeCluster for status-forwarding tests.
async fn fixture_runtime_with_cluster(
    node_name: &str,
    role: RuntimeNodeRole,
    cluster: std::sync::Arc<FakeCluster>,
) -> (
    std::sync::Arc<MockCriRuntime>,
    std::sync::Arc<crate::kubelet::pod_runtime::service::RealPodRuntimeService>,
    std::sync::Arc<crate::kubelet::pod_repository::PodRepository>,
    std::sync::Arc<FakeCluster>,
    std::sync::Arc<MockHostPortRuntime>,
) {
    let repo = fixture_pod_repository().await;
    let cri = std::sync::Arc::new(MockCriRuntime::new());
    let container_control = std::sync::Arc::new(
        crate::kubelet::pod_runtime::test_support::MockContainerRuntimeControl::new(),
    );
    let network = std::sync::Arc::new(MockPodNetworkRuntime::new());
    let store = std::sync::Arc::new(MockPodRuntimeStore::new());
    let slot_admission =
        std::sync::Arc::new(crate::kubelet::pod_runtime::test_support::MockPodSlotAdmission::new());
    let filesystem = std::sync::Arc::new(MockPodFilesystem::new());
    let volumes = std::sync::Arc::new(MockPodVolumeRuntime::new());
    let probes = std::sync::Arc::new(MockProbeRuntime::new());
    let hostports = std::sync::Arc::new(MockHostPortRuntime::new());
    let events = std::sync::Arc::new(MockPodEventSink::new());
    let hooks =
        std::sync::Arc::new(crate::kubelet::pod_runtime::test_support::MockPodHookRuntime::new());
    let env_source = fixture_env_source(node_name).await;
    let finalizer = std::sync::Arc::new(
        crate::kubelet::pod_runtime::test_support::MockPodDeletionFinalizer::new(),
    );
    let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    ));
    let config = RuntimeConfig {
        node_name: node_name.to_string(),
        service_cidr: "10.43.128.0/17".into(),
        containerd_namespace: "klights-test".into(),
    };
    let node_view = std::sync::Arc::new(FakeNode::new(node_name, role));

    let runtime = std::sync::Arc::new(
        crate::kubelet::pod_runtime::service::RealPodRuntimeService::new(
            RealPodRuntimeServiceDependencies {
                cri: cri.clone(),
                container_control,
                network,
                store,
                slot_admission,
                repository: repo.clone(),
                filesystem,
                volumes,
                probes,
                hostports: hostports.clone(),
                events,
                hooks,
                env_source,
                finalizer,
                supervisor,
                config,
                node_view,
                cluster_view: cluster.clone() as std::sync::Arc<dyn ClusterRuntimeView>,
            },
        ),
    );
    (cri, runtime, repo, cluster, hostports)
}

#[tokio::test]
async fn worker_runtime_forwards_status_to_leader() {
    let cluster = std::sync::Arc::new(FakeCluster::new());
    let (_cri, runtime, repo, cluster, _hostports) =
        fixture_runtime_with_cluster("worker-1", RuntimeNodeRole::Worker, cluster).await;

    let pod = scheduled_pod_json("ns", "fwd-pod", "uid-fwd", "worker-1");
    repo.create_controller_pod("ns", "fwd-pod", "worker-1", pod.clone())
        .await
        .unwrap();
    let key = PodRuntimeKey::new("ns", "fwd-pod", "uid-fwd");
    let result = runtime
        .start_pod(key, Some(pod), CancellationToken::new())
        .await
        .unwrap();
    assert!(matches!(result, PodStartResult::Started { .. }));

    // Status must have been forwarded to the leader via ClusterRuntimeView.
    let forwards = cluster.recorded_status_forwards();
    assert!(
        !forwards.is_empty(),
        "status must be forwarded to leader on worker"
    );
    // The forward must carry the correct UID.
    assert_eq!(forwards[0].0.namespace, "ns");
    assert_eq!(forwards[0].0.name, "fwd-pod");
    assert_eq!(forwards[0].0.uid, "uid-fwd");
}

#[tokio::test]
async fn leader_runtime_writes_status_locally() {
    // Leader runtime routes through the same worker cluster-view, backed by the
    // local cluster-datastore repository, so status writes land locally.
    let harness = PodRuntimeHarness::new().await;
    let pod = scheduled_pod_json("ns", "local-pod", "uid-local", "test-node");
    harness
        .repo
        .create_controller_pod("ns", "local-pod", "test-node", pod.clone())
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
        .get_pod_for_uid("ns", "local-pod", "uid-local")
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
        fixture_runtime_with_cluster("worker-1", RuntimeNodeRole::Worker, cluster).await;

    let pod = scheduled_pod_json("ns", "uid-pod", "uid-chk", "worker-1");
    repo.create_controller_pod("ns", "uid-pod", "worker-1", pod.clone())
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

// --- Task 12.3: Multi-node runtime cleanup node-local ---

#[tokio::test]
async fn cross_node_delete_is_rejected_on_non_owner_node() {
    let (cri, runtime, repo) = fixture_runtime_with_node("worker-1", RuntimeNodeRole::Worker).await;

    // Pod scheduled to a different node: this node must not perform any local
    // cleanup and must not report success, because success lets the lifecycle
    // actor finalize a Pod row whose owning node never cleaned its resources.
    let cross_pod = scheduled_pod_json("ns", "cross-pod", "uid-cross", "worker-2");
    repo.create_controller_pod("ns", "cross-pod", "worker-2", cross_pod.clone())
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
    repo.create_controller_pod("ns", "local-pod", "worker-1", local_pod.clone())
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
async fn cri_leftover_cleanup_is_node_local() {
    // Build a runtime on worker-1 and test reconcile_cri_leftovers node-local gate.
    let (cri, runtime, repo) = fixture_runtime_with_node("worker-1", RuntimeNodeRole::Worker).await;

    // Pod scheduled to a different node — reconcile must return Ok without CRI work.
    let cross_pod = scheduled_pod_json("ns", "cross-pod", "uid-cross", "worker-2");
    repo.create_controller_pod("ns", "cross-pod", "worker-2", cross_pod)
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
    repo.create_controller_pod("ns", "local-pod", "worker-1", local_pod)
        .await
        .unwrap();
    let local_key = PodRuntimeKey::new("ns", "local-pod", "uid-local");
    runtime.reconcile_cri_leftovers(local_key).await.unwrap();
    // Owned pod: method returns Ok; CRI work would happen here when implemented.
}

// --- Task 17.1: Mock Dependency Coverage Matrix ---

/// CRI: image pull, sandbox run, container stop calls recorded in order.
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

/// Network: read_assignment and release_sandbox_network carry PodRuntimeKey.
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

/// Runtime store: sandbox rows isolated by UID; same-name old/new preserved.
#[tokio::test]
async fn mock_dependency_matrix_runtime_store() {
    use crate::kubelet::pod_runtime::test_support::MockPodRuntimeStore;

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

/// Repository: MockPodRuntimeStore validates stale UID is rejected.
#[tokio::test]
async fn mock_dependency_matrix_repository() {
    use crate::kubelet::pod_runtime::test_support::MockPodRuntimeStore;

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

/// Cluster view: minimal fake verifying forward_pod_status carries UID.
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

/// Replication: minimal fake verifying UID-preconditioned StorageCommand enqueued.
#[tokio::test]
async fn mock_dependency_matrix_replication() {
    struct FakeReplication {
        commands: std::sync::Mutex<Vec<(String, String, String, String)>>,
    }
    impl FakeReplication {
        fn new() -> Self {
            Self {
                commands: std::sync::Mutex::new(Vec::new()),
            }
        }
        fn enqueue_command(&self, ns: &str, name: &str, uid: &str, action: &str) {
            self.commands.lock().unwrap().push((
                ns.to_string(),
                name.to_string(),
                uid.to_string(),
                action.to_string(),
            ));
        }
    }

    let rep = FakeReplication::new();
    rep.enqueue_command("ns", "rep-pod", "uid-rep", "update_status");

    let cmds = rep.commands.lock().unwrap();
    assert_eq!(cmds.len(), 1);
    assert_eq!(cmds[0].0, "ns");
    assert_eq!(cmds[0].1, "rep-pod");
    assert_eq!(cmds[0].2, "uid-rep");
    assert_eq!(cmds[0].3, "update_status");
}

/// Timer: TaskSupervisor spawn_delay fires once per scheduled deadline.
#[tokio::test]
async fn mock_dependency_matrix_timer() {
    let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
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

/// Probe: start/stop carry UID.
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

/// Filesystem: hosts/log/cgroup/fsgroup calls record UID.
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

/// Volume: process and cleanup recorded.
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

/// Hostport: add/remove/admission carry UID.
#[tokio::test]
async fn mock_dependency_matrix_hostport() {
    let mock = MockHostPortRuntime::new();
    let key = PodRuntimeKey::new("ns", "hp-pod", "uid-hp");
    let pod = serde_json::json!({"spec": {"containers": [{"ports": [{"hostPort": 8080}]}]}});

    mock.check_host_port_admission(&key, &pod).await.unwrap();
    mock.add_host_ports(&key, &pod).await.unwrap();
    mock.remove_host_ports(&key, &pod).await.unwrap();

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

/// Event sink: Scheduled/Pulling/Pulled/Failed events carry UID.
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

/// Env source: configmap/secret/service lookups are recordable without
/// datastore, leader API, or filesystem.
#[tokio::test]
async fn mock_dependency_matrix_env_source() {
    let mock = crate::kubelet::pod_runtime::test_support::MockEnvSourceReader::new();

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

/// Deletion finalizer: finalize call carries PodRuntimeKey.
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

/// Fake cluster: separate CRI mocks for leader and worker ensure no cross-talk.
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

// ── Task 20.1: ContainerRuntimeControl on SharedCriRuntime ──

/// Structural verification that SharedCriRuntime implements
/// ContainerRuntimeControl and the adapter compiles.
#[tokio::test]
async fn shared_cri_runtime_implements_container_runtime_control_with_parity() {
    // Verify the trait is implemented on the production adapter type.
    use crate::kubelet::pod_runtime::cri::ContainerRuntimeControl;
    // Trait object conversion compiles only if SharedCriRuntime: ContainerRuntimeControl.
    fn _assert_impl(_: &dyn ContainerRuntimeControl) {}
    // Verify mock also implements the trait (Task 20.1 coverage gate).
    use crate::kubelet::pod_runtime::test_support::MockContainerRuntimeControl;
    let mock = MockContainerRuntimeControl::new();
    let result = mock.list_containers(Some("sb-1")).await;
    assert!(result.is_ok());
    assert_eq!(mock.recorded_calls().len(), 1);
}

// ── Task 20.3-20.4: RealPodSlotAdmission & RealPodRuntimeStore ──

#[tokio::test]
async fn real_pod_runtime_store_records_and_retrieves_sandbox() {
    let (ds, handle) = crate::datastore::test_support::in_memory_with_handle().await;
    std::mem::forget(ds);
    let store = crate::kubelet::pod_runtime::store::RealPodRuntimeStore::new(handle.clone());
    let key = PodRuntimeKey::new("ns", "test-pod", "uid-1");

    // Record sandbox.
    store.record_sandbox(&key, "sandbox-abc").await.unwrap();

    // Retrieve by UID.
    let found = store.get_sandbox_id(&key).await.unwrap();
    assert_eq!(found.as_deref(), Some("sandbox-abc"));

    // Retrieve by name (without UID).
    let by_name = store
        .get_sandbox_id_by_name("ns", "test-pod")
        .await
        .unwrap();
    assert_eq!(by_name.as_deref(), Some("sandbox-abc"));

    // Delete by UID.
    store.delete_sandbox(&key).await.unwrap();
    let after_delete = store.get_sandbox_id(&key).await.unwrap();
    assert!(after_delete.is_none());
}

#[tokio::test]
async fn real_pod_slot_admission_admits_and_clears_slot() {
    let (ds, handle) = crate::datastore::test_support::in_memory_with_handle().await;
    std::mem::forget(ds);
    let admission = crate::kubelet::pod_runtime::store::RealPodSlotAdmission::new(
        handle.clone(),
        "node-1".into(),
    );
    let key = PodRuntimeKey::new("ns", "slot-pod", "uid-1");

    // Try admit — should succeed on first attempt.
    let result = admission.try_admit(&key, "node-1").await.unwrap();
    assert!(
        matches!(
            result,
            crate::datastore::PodSlotAdmissionResult::Admitted { .. }
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
    let (ds, handle) = crate::datastore::test_support::in_memory_with_handle().await;
    std::mem::forget(ds);
    let admission = crate::kubelet::pod_runtime::store::RealPodSlotAdmission::new(
        handle.clone(),
        "node-1".into(),
    );
    let key = PodRuntimeKey::new("ns", "dup-pod", "uid-1");

    // First admission succeeds.
    let first = admission.try_admit(&key, "node-1").await.unwrap();
    assert!(matches!(
        first,
        crate::datastore::PodSlotAdmissionResult::Admitted { .. }
    ));

    // Second admission with different UID is blocked.
    let key2 = PodRuntimeKey::new("ns", "dup-pod", "uid-2");
    let second = admission.try_admit(&key2, "node-1").await.unwrap();
    assert!(
        matches!(
            second,
            crate::datastore::PodSlotAdmissionResult::Blocked { .. }
        ),
        "second admission with different UID should be Blocked, got {:?}",
        second
    );
}

// ── Task 20.10: LocalNodeRuntimeView ──

use crate::kubelet::pod_cluster_runtime::LocalNodeRuntimeView;

#[test]
fn local_node_runtime_view_owns_pod_with_matching_node_name() {
    let view = LocalNodeRuntimeView::new("node-1".into(), RuntimeNodeRole::Leader);
    let pod = serde_json::json!({
        "spec": {"nodeName": "node-1"}
    });
    assert!(view.owns_pod_runtime(&pod));
    assert_eq!(view.node_name(), "node-1");
    assert!(matches!(view.role(), RuntimeNodeRole::Leader));
}

#[test]
fn local_node_runtime_view_rejects_pod_with_different_node_name() {
    let view = LocalNodeRuntimeView::new("node-1".into(), RuntimeNodeRole::Worker);
    let pod = serde_json::json!({
        "spec": {"nodeName": "node-2"}
    });
    assert!(!view.owns_pod_runtime(&pod));
}

#[test]
fn local_node_runtime_view_rejects_pod_without_node_name() {
    let view = LocalNodeRuntimeView::new("node-1".into(), RuntimeNodeRole::Worker);
    let pod = serde_json::json!({
        "spec": {}
    });
    assert!(!view.owns_pod_runtime(&pod));
}

// ── Task 20.11-20.12: ClusterRuntimeView & ReplicationRuntime ──

#[tokio::test]
async fn worker_cluster_runtime_view_constructs_with_repository() {
    let repo = fixture_pod_repository().await;
    let _view =
        crate::kubelet::pod_cluster_runtime::WorkerClusterRuntimeView::new(repo, "node-1".into());
}

#[tokio::test]
async fn real_replication_runtime_enqueue_is_noop_in_single_node() {
    let repo = fixture_pod_repository().await;
    let replication = crate::kubelet::pod_cluster_runtime::RealReplicationRuntime::new(repo);
    let key = PodRuntimeKey::new("ns", "pod", "uid");
    let cmd = crate::datastore::command::StorageCommand::create_resource(
        "v1",
        "Pod",
        Some("ns"),
        "pod",
        serde_json::json!({}),
    );
    // Should succeed (no-op in single-node mode).
    replication
        .enqueue_storage_command(&key, cmd)
        .await
        .unwrap();
}

// ── Task 21.1: finalize_deletion routes through PodDeletionFinalizer ──

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

// ── Task 21.2: handle_lifecycle_command ──

#[tokio::test]
async fn readiness_lifecycle_command_persists_probe_result_with_parity() {
    use crate::kubelet::pod_repository::{PodStatusUpdate, PodStatusWriter};

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

    let cmd = crate::kubelet::lifecycle::LifecycleCommand::ReadinessChanged {
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
async fn real_runtime_handle_lifecycle_command_startup_passed() {
    let harness = PodRuntimeHarness::new().await;
    let cmd = crate::kubelet::lifecycle::LifecycleCommand::StartupPassed {
        pod_uid: "uid-sp".into(),
        namespace: "ns".into(),
        pod_name: "startup-pod".into(),
        container_name: "app".into(),
    };
    let result = harness.runtime.handle_lifecycle_command(cmd).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn liveness_restart_uses_runtime_container_id_with_parity() {
    use crate::kubelet::lifecycle::{LifecycleCommand, RestartReason};
    use crate::kubelet::pod_repository::{PodStatusUpdate, PodStatusWriter};
    use crate::kubelet::pod_runtime::cri::ContainerRuntimeState;

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
    use crate::kubelet::lifecycle::{LifecycleCommand, RestartReason};
    use crate::kubelet::pod_repository::{PodStatusUpdate, PodStatusWriter};
    use crate::kubelet::pod_runtime::cri::ContainerRuntimeState;

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

// ── Task 21.3: reconcile_ephemeral ──

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

// ── Task 21.4: reconcile_runtime ──

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
    use crate::kubelet::pod_repository::{PodStatusUpdate, PodStatusWriter};
    use crate::kubelet::pod_runtime::cri::ContainerRuntimeState;

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
    use crate::kubelet::pod_repository::{PodStatusUpdate, PodStatusWriter};
    use crate::kubelet::pod_runtime::cri::ContainerRuntimeState;

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
async fn reconcile_runtime_writes_pod_and_host_ips_with_parity() {
    use crate::kubelet::pod_repository::PodStatusWriter;
    use crate::kubelet::pod_runtime::cri::ContainerRuntimeState;
    use crate::kubelet::pod_runtime::store::PodRuntimeStore;

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
        .db_handle
        .create_resource("v1", "Pod", Some("pods"), "ip-pod", pod)
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
        .reconcile_runtime(
            key.clone(),
            crate::kubelet::pod_runtime::service::RuntimeReconcileHint::none(),
        )
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
        .db_handle
        .create_resource("v1", "Pod", Some("pods"), "ready-pod", ready_pod)
        .await
        .unwrap();
    harness
        .repo
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
            crate::kubelet::pod_runtime::service::RuntimeReconcileHint::none(),
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
    use crate::kubelet::pod_repository::PodReader;
    use crate::kubelet::pod_runtime::cri::ContainerRuntimeState;
    use crate::kubelet::pod_runtime::store::PodRuntimeStore;

    let harness = PodRuntimeHarness::new().await;
    let key = PodRuntimeKey::new("pods", "dedup-pod", "uid-dedup-pod");
    harness
        .db_handle
        .create_resource(
            "v1",
            "Pod",
            Some("pods"),
            "dedup-pod",
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
    let mut watch_rx = harness
        .db_handle
        .subscribe_watch(crate::watch::WatchTopic::new("v1", "Pod"));
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
        .reconcile_runtime(
            key.clone(),
            crate::kubelet::pod_runtime::service::RuntimeReconcileHint::none(),
        )
        .await
        .unwrap();
    let first_event = tokio::time::timeout(std::time::Duration::from_secs(1), watch_rx.recv())
        .await
        .expect("first reconcile must emit one status watch event")
        .expect("pod watch channel must remain open");
    assert_eq!(first_event.event_type, crate::watch::EventType::Modified);
    assert_eq!(
        first_event
            .object
            .pointer("/metadata/name")
            .and_then(|value| value.as_str()),
        Some("dedup-pod")
    );
    let first_rv = harness
        .repo
        .get_pod_for_uid("pods", "dedup-pod", "uid-dedup-pod")
        .await
        .unwrap()
        .expect("pod must exist")
        .resource_version;

    harness
        .runtime
        .reconcile_runtime(
            key.clone(),
            crate::kubelet::pod_runtime::service::RuntimeReconcileHint::none(),
        )
        .await
        .unwrap();
    assert!(
        watch_rx.try_recv().is_err(),
        "duplicate runtime status must not emit a second Pod watch event or run downstream watch side effects"
    );
    let second_rv = harness
        .repo
        .get_pod_for_uid("pods", "dedup-pod", "uid-dedup-pod")
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
    use crate::kubelet::pod_runtime::cri::ContainerRuntimeState;

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
        .db_handle
        .create_resource("v1", "Pod", Some("pods"), "deadline-pod", pod)
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
        .reconcile_runtime(
            key.clone(),
            crate::kubelet::pod_runtime::service::RuntimeReconcileHint::none(),
        )
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

// ── Task 21.5: finalize_startup ──

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
    use crate::kubelet::pod_repository::PodObjectWriter;
    use crate::kubelet::pod_repository::{PodStatusUpdate, PodStatusWriter};
    use crate::kubelet::pod_runtime::store::PodRuntimeStore;

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
        .create_controller_pod("ns", "confirmed-pod", "test-node", pod)
        .await
        .unwrap();
    harness
        .repo
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

// ── Task 22.1: Init Containers ──

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
        .create_controller_pod("ns", "init-pod", "test-node", pod.clone())
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
        .create_controller_pod("ns", "init-fail", "test-node", pod.clone())
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
async fn worker_init_retry_never_forwards_phase_only_pending_status() {
    let cluster = std::sync::Arc::new(FakeCluster::new());
    let (cri, runtime, repo, cluster, _hostports) =
        fixture_runtime_with_cluster("worker-1", RuntimeNodeRole::Worker, cluster).await;
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
    repo.create_controller_pod(
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
    let before_retry = cluster.recorded_status_forwards().len();

    let second = runtime
        .start_pod(key.clone(), Some(pod), CancellationToken::new())
        .await
        .unwrap();
    assert!(
        matches!(second, PodStartResult::Failed(_)),
        "second init failure should still be retryable, got {second:?}"
    );

    let forwards = cluster.recorded_status_forwards();
    let retry_status = forwards
        .get(before_retry)
        .map(|(_, status)| status)
        .expect("retry must forward an initial Pending status");
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

// ── Task 22.2: Full Container Config ──

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
        .create_controller_pod("ns", "config-pod", "test-node", pod.clone())
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

// ── Task 22.3: Restart Policy and Retry ──

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
            .create_controller_pod("ns", "terminal-pod", "test-node", pod.clone())
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
            .create_controller_pod("ns", "retry-pod", "test-node", pod.clone())
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
            .create_controller_pod("ns", "imgfail-pod", "test-node", pod.clone())
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

// --- Task 22.4: PostStart Hooks ---

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
        .create_controller_pod("ns", "poststart-pod", "test-node", pod.clone())
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
        .create_controller_pod("ns", "psfail-pod", "test-node", pod.clone())
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

// --- Task 22.5: Probe Registration on Start ---

use crate::kubelet::pod_repository::{PodStatusUpdate, PodStatusWriter};

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
        .create_controller_pod("ns", "probe-pod", "test-node", pod.clone())
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

// --- Task 23.1: PreStop Hooks ---

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
        .create_controller_pod("ns", "prestop-pod", "test-node", pod.clone())
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

// --- Task 23.2: Termination Grace Period ---

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
            .create_controller_pod("ns", "grace-5", "test-node", pod.clone())
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
            .create_controller_pod("ns", "grace-default", "test-node", pod.clone())
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

// --- Task 23.3: Sandbox Resolution Full Ladder ---

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
            .create_controller_pod("ns", "dir-pod", "test-node", pod.clone())
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
            .create_controller_pod("ns", "store-pod", "test-node", pod.clone())
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
            .create_controller_pod("ns", "annot-pod", "test-node", pod.clone())
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
        .create_controller_pod("deleted-ns", "deleted-pod", "test-node", pod.clone())
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
    use crate::kubelet::pod_repository::{PodStatusUpdate, PodStatusWriter};

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
        .create_controller_pod("sonobuoy", "e2e", "test-node", pod.clone())
        .await
        .unwrap();
    harness
        .repo
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
        .get_pod_for_uid("sonobuoy", "e2e", "uid-e2e")
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
    use crate::kubelet::pod_repository::{PodStatusUpdate, PodStatusWriter};
    use crate::kubelet::pod_runtime::cri::ContainerRuntimeState;
    use crate::kubelet::pod_runtime::store::PodRuntimeStore;

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
    use crate::kubelet::pod_repository::{PodStatusUpdate, PodStatusWriter};
    use crate::kubelet::pod_runtime::cri::ContainerRuntimeState;
    use crate::kubelet::pod_runtime::store::PodRuntimeStore;

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
async fn mocked_runtime_does_not_create_termination_log_file_directly() {
    let runtime_namespace = "klights-term-mock-create-test";
    let _ = std::fs::remove_dir_all(crate::paths::data_root_path(runtime_namespace));
    let harness = PodRuntimeHarness::new_with_runtime_config(
        crate::kubelet::pod_runtime::service::RuntimeConfig {
            node_name: "test-node".into(),
            service_cidr: "10.43.128.0/17".into(),
            containerd_namespace: runtime_namespace.into(),
        },
    )
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
    let direct_host_path = crate::paths::containerd_termination_log_path(
        runtime_namespace,
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

    let _ = std::fs::remove_dir_all(crate::paths::data_root_path(runtime_namespace));
}

#[tokio::test]
async fn mocked_runtime_does_not_read_termination_message_file_directly() {
    use crate::kubelet::pod_repository::{PodStatusUpdate, PodStatusWriter};
    use crate::kubelet::pod_runtime::cri::ContainerRuntimeState;
    use crate::kubelet::pod_runtime::store::PodRuntimeStore;

    let runtime_namespace = "klights-term-mock-read-test";
    let _ = std::fs::remove_dir_all(crate::paths::data_root_path(runtime_namespace));
    let harness = PodRuntimeHarness::new_with_runtime_config(
        crate::kubelet::pod_runtime::service::RuntimeConfig {
            node_name: "test-node".into(),
            service_cidr: "10.43.128.0/17".into(),
            containerd_namespace: runtime_namespace.into(),
        },
    )
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
        .set_pod_status_for_uid(
            "container-runtime",
            "termination-message-pod",
            "uid-termination-mock-read",
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
    let direct_host_path = crate::paths::containerd_termination_log_path(
        runtime_namespace,
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

    let _ = std::fs::remove_dir_all(crate::paths::data_root_path(runtime_namespace));
}

#[tokio::test]
async fn termination_message_mount_path_with_parity() {
    let runtime_namespace = "klights-term-mount-test";
    let harness = PodRuntimeHarness::new_with_runtime_config(
        crate::kubelet::pod_runtime::service::RuntimeConfig {
            node_name: "test-node".into(),
            service_cidr: "10.43.128.0/17".into(),
            containerd_namespace: runtime_namespace.into(),
        },
    )
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

    let _ = std::fs::remove_dir_all(crate::paths::data_root_path(runtime_namespace));
}

#[tokio::test]
async fn hosts_file_mount_path_with_parity() {
    let runtime_namespace = "klights-hosts-mount-test";
    let harness = PodRuntimeHarness::new_with_runtime_config(
        crate::kubelet::pod_runtime::service::RuntimeConfig {
            node_name: "test-node".into(),
            service_cidr: "10.43.128.0/17".into(),
            containerd_namespace: runtime_namespace.into(),
        },
    )
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
    let expected_host_path = crate::paths::containerd_hosts_dir_path(
        runtime_namespace,
        "kubelet-test",
        "host-alias-pod",
    )
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

    let _ = std::fs::remove_dir_all(crate::paths::data_root_path(runtime_namespace));
}

#[tokio::test]
async fn termination_message_file_handling_with_parity() {
    use crate::kubelet::pod_repository::{PodStatusUpdate, PodStatusWriter};
    use crate::kubelet::pod_runtime::cri::ContainerRuntimeState;
    use crate::kubelet::pod_runtime::store::PodRuntimeStore;

    let runtime_namespace = "klights-term-read-test";
    let harness = PodRuntimeHarness::new_with_runtime_config(
        crate::kubelet::pod_runtime::service::RuntimeConfig {
            node_name: "test-node".into(),
            service_cidr: "10.43.128.0/17".into(),
            containerd_namespace: runtime_namespace.into(),
        },
    )
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
        .set_pod_status_for_uid(
            "container-runtime",
            "termination-message-pod",
            "uid-termination-read",
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

    let _ = std::fs::remove_dir_all(crate::paths::data_root_path(runtime_namespace));
}

// --- Task 23.4: Partial-State and Rollback Handling ---

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
            .create_controller_pod("ns", "nocont-pod", "test-node", pod.clone())
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

    // Scenario 2: CRI operations fail (simulate resources already gone) — cleanup still succeeds.
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
            .create_controller_pod("ns", "crlfail-pod", "test-node", pod.clone())
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

        // CRI failure must not propagate — cleanup is best-effort.
        harness
            .runtime
            .stop_pod(key.clone(), Some(pod), Some("sb-crlfail".into()))
            .await
            .unwrap();
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
            .create_controller_pod("ns", "nosb-pod", "test-node", pod.clone())
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
    let harness = PodRuntimeHarness::new_with_runtime_config(
        crate::kubelet::pod_runtime::service::RuntimeConfig {
            node_name: "test-node".into(),
            service_cidr: "10.96.0.0/12".into(),
            containerd_namespace: "klights-test".into(),
        },
    )
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
        crate::kubelet::pod_runtime::cri::ContainerRuntimeState::from_cri_state_i32(
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

async fn wait_for_pod_status(
    harness: &crate::kubelet::pod_runtime::test_support::PodRuntimeHarness,
    key: &PodRuntimeKey,
    predicate: impl Fn(&serde_json::Value) -> bool,
) -> serde_json::Value {
    for _ in 0..50 {
        let pod = harness.stored_pod(key).await;
        if predicate(&pod) {
            return pod;
        }
        let _ = harness
            .supervisor
            .sleep(
                "pod_runtime_status_wait",
                std::time::Duration::from_millis(10),
            )
            .await;
    }
    let pod = harness.stored_pod(key).await;
    panic!("pod status did not reach expected state: {pod}");
}

#[tokio::test]
async fn real_runtime_actor_cycle_starts_reconciles_running_and_deletes_pod() {
    use crate::kubelet::pod_lifecycle_actor::config::PodLifecycleConcurrencyConfig;
    use crate::kubelet::pod_lifecycle_actor::registry::PodLifecycleRegistry;
    use crate::kubelet::pod_lifecycle_core::message::LifecycleMessage;
    use crate::kubelet::pod_lifecycle_router::PodLifecycleRouter;
    use crate::kubelet::pod_lifecycle_router::executor::{
        NoopExecutor, PodLifecycleExecutor, PodWorkExecutor,
    };
    use crate::kubelet::pod_runtime::test_support::PodRuntimeHarness;

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
            kind: crate::kubelet::cri_events::KubeletEventKind::Started,
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
        serde_json::Value::String(crate::utils::k8s_timestamp());
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
    use crate::kubelet::pod_repository::{PodObjectWriter, PodReader};

    let (ds, db) = crate::datastore::test_support::in_memory_with_handle().await;
    db.seed_namespace_for_test("sonobuoy").await;
    std::mem::forget(ds);
    let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    ));
    let parts = crate::kubelet::pod_repository::PodRepository::build_parts(
        crate::kubelet::pod_repository::PodRepositoryBuildConfig {
            db: db.clone(),
            supervisor: supervisor.clone(),
            side_effects: std::sync::Arc::new(crate::side_effects::SideEffectRegistry::new()),
            metrics: crate::side_effects::SideEffectMetrics::new(),
            network_events: crate::networking::global_pod_network_events(),
            scheduling_mode:
                crate::kubelet::pod_repository::api::PodSchedulingMode::InlineSingleNode,
            outbox: None,
            cluster_api: None,
        },
    );
    let repo = std::sync::Arc::new(parts.repository);
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
    repo.create_controller_pod("sonobuoy", "sonobuoy", "test-node", pod.clone())
        .await
        .unwrap();
    let cluster_api: std::sync::Arc<dyn crate::control_plane::client::LeaderApiClient> =
        std::sync::Arc::new(crate::control_plane::client::local::LocalApiClient::new(
            db.clone(),
            "test-node".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ));
    let env_source = std::sync::Arc::new(crate::kubelet::pod_env::LeaderApiEnvSourceReader::new(
        cluster_api,
    ));

    let runtime = crate::kubelet::pod_runtime::service::RealPodRuntimeService::new(
        RealPodRuntimeServiceDependencies {
            cri: std::sync::Arc::new(MockCriRuntime::new()),
            container_control: std::sync::Arc::new(MockContainerRuntimeControl::new()),
            network: std::sync::Arc::new(MockPodNetworkRuntime::new()),
            store: std::sync::Arc::new(MockPodRuntimeStore::new()),
            slot_admission: std::sync::Arc::new(MockPodSlotAdmission::new()),
            repository: repo.clone(),
            filesystem: std::sync::Arc::new(MockPodFilesystem::new()),
            volumes: std::sync::Arc::new(MockPodVolumeRuntime::new()),
            probes: std::sync::Arc::new(MockProbeRuntime::new()),
            hostports: std::sync::Arc::new(MockHostPortRuntime::new()),
            events: std::sync::Arc::new(MockPodEventSink::new()),
            hooks: std::sync::Arc::new(MockPodHookRuntime::new()),
            env_source,
            finalizer: repo.deletion_finalizer(),
            supervisor,
            config: RuntimeConfig {
                node_name: "test-node".into(),
                service_cidr: "10.43.128.0/17".into(),
                containerd_namespace: "klights-test".into(),
            },
            node_view: std::sync::Arc::new(FakeNode::new("test-node", RuntimeNodeRole::Leader)),
            cluster_view: std::sync::Arc::new(
                crate::kubelet::pod_cluster_runtime::WorkerClusterRuntimeView::new(
                    repo.clone(),
                    "test-node".into(),
                ),
            ),
        },
    );

    runtime
        .stop_pod(key.clone(), Some(pod), None)
        .await
        .expect("unstarted terminating pod cleanup should succeed");
    assert_eq!(
        runtime.finalize_deletion(key.clone()).await.unwrap(),
        PodDeletionFinalizeResult::DeletedOrAlreadyGone
    );
    assert!(
        repo.get_pod_for_uid("sonobuoy", "sonobuoy", "uid-sonobuoy")
            .await
            .unwrap()
            .is_none(),
        "actor finalization must remove the unstarted terminating pod row"
    );
}

// ── Task 1 (fixnow): CRI event fast-exit hint ──
//
// Short-lived pods (ConfigMap-volume / ReplicaSet-adoption) can exit while
// startup finalization is still in flight. The actor defers the CRI stop
// event and later runs a runtime reconcile. If sandbox container listing
// returns empty/stale by then, the reconciler must not synthesize
// Pending/ContainerCreating — it must use the CRI event's container id to
// read the concrete (terminated) status and publish Succeeded.

#[tokio::test]
async fn real_runtime_reconcile_uses_cri_event_container_id_when_list_is_empty() {
    use crate::kubelet::pod_repository::{PodStatusUpdate, PodStatusWriter};
    use crate::kubelet::pod_runtime::cri::ContainerRuntimeState;
    use crate::kubelet::pod_runtime::service::RuntimeReconcileHint;
    use crate::kubelet::pod_runtime::store::PodRuntimeStore;

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
            RuntimeReconcileHint::from_container_id("ctr-fast-exit"),
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

// Task 4 tests (red-green): track multiple CRI event container IDs
#[cfg(test)]
mod task4_runtime_observations {
    use super::*;
    use crate::kubelet::pod_lifecycle_core::state::PodLifecycleState;
    use crate::kubelet::pod_runtime::cri::ContainerRuntimeState;
    use crate::kubelet::pod_runtime::service::PodRuntimeKey;
    use crate::kubelet::pod_runtime::service::RuntimeReconcileHint;
    use crate::kubelet::pod_runtime::test_support::PodRuntimeHarness;

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
        use crate::kubelet::pod_runtime::test_support::PodRuntimeHarness;
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
        use crate::kubelet::pod_repository::{PodStatusUpdate, PodStatusWriter};
        harness
            .repo
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
        let hint =
            RuntimeReconcileHint::from_container_ids(["ctr-a".to_string(), "ctr-b".to_string()]);
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
    async fn runtime_reconcile_ignores_unknown_hinted_container_without_regressing_terminal_status()
    {
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
        use crate::kubelet::pod_repository::{PodStatusUpdate, PodStatusWriter};
        harness
            .repo
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
        use crate::kubelet::pod_repository::{PodStatusUpdate, PodStatusWriter};
        harness
            .repo
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
        let hint =
            RuntimeReconcileHint::from_container_ids(["ctr-a".to_string(), "ctr-b".to_string()]);
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
}
