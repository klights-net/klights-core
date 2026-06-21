use super::*;
use crate::kubelet::pod_sandbox_config::build_sandbox_config_with_dns_policy;

#[test]
fn pod_start_retry_uses_one_shot_delay_not_interval_scan() {
    // R4: invariant now enforced by check_kubelet_invariants.sh
}

#[test]
fn pod_watcher_serializes_lifecycle_commands_with_cri_and_watch_events() {
    // R4: invariant now enforced by check_kubelet_invariants.sh
}

#[test]
fn pod_watcher_does_not_call_remove_actor_on_watch_deleted() {
    // R4: invariant now enforced by check_kubelet_invariants.sh
}

#[test]
fn pod_watcher_limits_pod_events_to_local_node_field_selector() {
    // R4: invariant now enforced by check_kubelet_invariants.sh
}

#[test]
fn pod_watcher_node_event_filter_matches_only_local_pods() {
    let filter = pod_watcher_node_event_filter("node-a");
    let local_pod = crate::watch::WatchEvent::added(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "local-pod", "namespace": "default"},
        "spec": {"nodeName": "node-a"}
    }));
    let remote_pod = crate::watch::WatchEvent::added(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "remote-pod", "namespace": "default"},
        "spec": {"nodeName": "node-b"}
    }));
    let configmap = crate::watch::WatchEvent::modified(serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "mounted-config", "namespace": "default"}
    }));

    assert!(filter.matches(&local_pod));
    assert!(!filter.matches(&remote_pod));
    assert!(filter.matches(&configmap));
}

#[tokio::test]
async fn app_state_pod_watcher_disables_cluster_reconciliation_on_raft_follower() {
    let mut state = crate::api::test_support::build_test_app_state().await;
    let (_lifecycle_tx, lifecycle_rx) = tokio::sync::mpsc::channel(1);
    state.pod_lifecycle_rx = Some(std::sync::Arc::new(tokio::sync::Mutex::new(Some(
        lifecycle_rx,
    ))));
    state.pod_lifecycle_router = Some(std::sync::Arc::new(
        crate::kubelet::pod_lifecycle_router::PodLifecycleRouter::from_env(
            state.task_supervisor.clone(),
            crate::kubelet::pod_lifecycle_actor::config::PodLifecycleConcurrencyConfig::production_default(),
        ),
    ));

    let (_is_leader_tx, is_leader_rx) = tokio::sync::watch::channel(false);
    let (_leader_addr_tx, leader_addr_rx) =
        tokio::sync::watch::channel(Some("https://10.99.0.10:7679".to_string()));
    state.is_raft_leader_rx = Some(std::sync::Arc::new(
        crate::api::raft_proxy::RaftLeaderProxy::new(is_leader_rx, leader_addr_rx, None),
    ));

    let context = PodWatcherRuntimeContext::from_app_state(&state);

    assert!(
        !context.cluster_reconciliation_enabled,
        "raft followers must not run leader-owned PVC/PV reconciliation from the AppState watcher"
    );
}

#[test]
fn deprecated_lifecycle_helpers_removed() {
    // R4: invariant now enforced by check_kubelet_invariants.sh
}

#[tokio::test]
async fn lifecycle_message_from_command_uses_command_uid_not_live_pod_uid() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "same-name",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "same-name",
                "uid": "uid-new"
            },
            "spec": {"containers": [{"name": "app", "image": "nginx"}]},
            "status": {"phase": "Running"}
        }),
    )
    .await
    .expect("create replacement pod");
    let pod_repo = super::fixture_pod_repository(&db);

    let message = lifecycle_message_from_command(
        &pod_repo,
        crate::kubelet::lifecycle::LifecycleCommand::ReadinessChanged {
            pod_uid: "uid-old".to_string(),
            namespace: "default".to_string(),
            pod_name: "same-name".to_string(),
            container_name: "app".to_string(),
            ready: true,
        },
    )
    .await
    .expect("command should route without a live name lookup");

    let LifecycleMessage::LifecycleCommand { key, .. } = message else {
        panic!("expected lifecycle command message");
    };
    assert_eq!(key.uid, "uid-old");
}

fn make_container(name: &str, state: i32, exit_code: i32) -> (String, ContainerInfo) {
    (
        name.to_string(),
        ContainerInfo {
            container_id: format!("{}-id", name),
            image: format!("{}:latest", name),
            image_ref: format!("docker.io/library/{}:latest", name),
            state,
            exit_code,
            started_at: 1_600_000_000_000_000_000,
            finished_at: if state == 2 {
                1_600_000_010_000_000_000
            } else {
                0
            },
            termination_message: String::new(),
        },
    )
}

#[test]
fn latest_container_infos_uses_runtime_timestamps_when_created_at_ties() {
    let (name, mut failed_first) = make_container("app", 2, 1);
    failed_first.container_id = "failed-first".to_string();
    failed_first.started_at = 1_000;
    failed_first.finished_at = 2_000;

    let (_, mut succeeded_second) = make_container("app", 2, 0);
    succeeded_second.container_id = "succeeded-second".to_string();
    succeeded_second.started_at = 3_000;
    succeeded_second.finished_at = 4_000;

    let latest = latest_container_infos_by_name(vec![
        (name, failed_first, 0),
        ("app".to_string(), succeeded_second, 0),
    ]);
    let (_, selected) = latest
        .iter()
        .find(|(container_name, _)| container_name == "app")
        .expect("latest app container selected");

    assert_eq!(selected.container_id, "succeeded-second");
    assert_eq!(selected.exit_code, 0);
    assert!(
        !should_restart("OnFailure", selected.exit_code),
        "OnFailure must not restart after the latest attempt exits 0"
    );
}

#[test]
fn test_container_ready_should_respect_readiness_probe() {
    use std::collections::{HashMap, HashSet};

    // Create a running container (state=1)
    let containers = vec![(
        "app".to_string(),
        ContainerInfo {
            container_id: "container1".to_string(),
            image: "nginx:latest".to_string(),
            image_ref: "docker.io/library/nginx:latest".to_string(),
            state: 1, // Running
            exit_code: 0,
            started_at: 1_600_000_000_000_000_000,
            finished_at: 0,
            termination_message: String::new(),
        },
    )];

    let restart_counts = HashMap::new();

    // Test 1: Container running but readiness probe not yet succeeded
    // Simulate: Ready condition is False (probe hasn't succeeded yet)
    let ready_containers_empty = HashSet::new();
    let statuses = build_container_statuses(&containers, &restart_counts, &ready_containers_empty);
    let status = &statuses[0];
    let ready = status.get("ready").and_then(|r| r.as_bool()).unwrap();

    // FIX VERIFICATION: ready should be false when readiness probe hasn't succeeded
    assert!(
        !(ready),
        "Container should NOT be ready when readiness probe hasn't succeeded"
    );

    // Test 2: Container running AND readiness probe succeeded
    // Simulate: Ready condition is True (probe succeeded)
    let mut ready_containers = HashSet::new();
    ready_containers.insert("app".to_string());
    let statuses = build_container_statuses(&containers, &restart_counts, &ready_containers);
    let status = &statuses[0];
    let ready = status.get("ready").and_then(|r| r.as_bool()).unwrap();

    // FIX VERIFICATION: ready should be true when readiness probe succeeded
    assert!(
        ready,
        "Container should be ready when readiness probe succeeded"
    );
}

#[test]
fn test_sandbox_hostname_uses_spec_hostname() {
    // Verify PodSandboxConfig.hostname is set from spec.hostname, not pod name
    let pod_spec = serde_json::json!({
        "hostname": "my-custom-hostname",
        "containers": []
    });
    let config = build_sandbox_config_with_dns_policy(
        "pod-name-123",
        "default",
        "10.43.0.5",
        "uid-abc",
        "klights-test",
        "10.43.128.10",
        &pod_spec,
    );
    assert_eq!(
        config.hostname, "my-custom-hostname",
        "PodSandboxConfig.hostname should use spec.hostname, not pod name"
    );
}

#[test]
fn test_sandbox_hostname_defaults_to_pod_name() {
    // Verify PodSandboxConfig.hostname falls back to pod name when spec.hostname is absent
    let pod_spec = serde_json::json!({
        "containers": []
    });
    let config = build_sandbox_config_with_dns_policy(
        "my-pod",
        "default",
        "10.43.0.5",
        "uid-abc",
        "klights-test",
        "10.43.128.10",
        &pod_spec,
    );
    assert_eq!(
        config.hostname, "my-pod",
        "PodSandboxConfig.hostname should fall back to pod name"
    );
}

#[test]
fn test_sandbox_hostname_empty_for_host_network_pods() {
    // For hostNetwork pods, hostname must stay empty to avoid sandbox creation
    // failures on runtimes without private UTS namespace support.
    let pod_spec = serde_json::json!({
        "hostNetwork": true,
        "hostname": "my-custom-hostname",
        "containers": [{"name":"app","image":"nginx"}]
    });
    let config = build_sandbox_config_with_dns_policy(
        "host-net-pod",
        "default",
        "10.43.0.5",
        "uid-abc",
        "klights-test",
        "10.43.128.10",
        &pod_spec,
    );
    assert_eq!(
        config.hostname, "",
        "hostNetwork pods should leave PodSandboxConfig.hostname empty"
    );
}

#[test]
fn test_sandbox_hostname_empty_for_host_network_pods_without_spec_hostname() {
    // hostNetwork pods must also avoid hostname fallback even when spec.hostname
    // is not set (would otherwise default to pod name).
    let pod_spec = serde_json::json!({
        "hostNetwork": true,
        "containers": [{"name":"app","image":"nginx"}]
    });
    let config = build_sandbox_config_with_dns_policy(
        "host-net-no-hostname",
        "default",
        "10.43.0.5",
        "uid-abc",
        "klights-test",
        "10.43.128.10",
        &pod_spec,
    );
    assert_eq!(
        config.hostname, "",
        "hostNetwork pods should not fall back to pod name for PodSandboxConfig.hostname"
    );
}

#[test]
fn test_sandbox_namespace_options_default_to_pod() {
    let pod_spec = serde_json::json!({
        "containers": [{"name":"app","image":"nginx"}]
    });
    let config = build_sandbox_config_with_dns_policy(
        "pod-default-ns",
        "default",
        "10.43.0.5",
        "uid-default",
        "klights-test",
        "10.43.128.10",
        &pod_spec,
    );
    let ns = config
        .linux
        .and_then(|l| l.security_context)
        .and_then(|sc| sc.namespace_options)
        .expect("namespace options must be present");

    assert_eq!(ns.network, 0, "default network namespace should be POD");
    assert_eq!(ns.pid, 0, "default PID namespace should be POD");
    assert_eq!(ns.ipc, 0, "default IPC namespace should be POD");
}

#[test]
fn test_sandbox_namespace_options_respect_host_flags() {
    let pod_spec = serde_json::json!({
        "hostNetwork": true,
        "hostPID": true,
        "hostIPC": true,
        "containers": [{"name":"app","image":"nginx"}]
    });
    let config = build_sandbox_config_with_dns_policy(
        "pod-host-ns",
        "default",
        "10.43.0.5",
        "uid-host",
        "klights-test",
        "10.43.128.10",
        &pod_spec,
    );
    let ns = config
        .linux
        .and_then(|l| l.security_context)
        .and_then(|sc| sc.namespace_options)
        .expect("namespace options must be present");

    assert_eq!(ns.network, 2, "hostNetwork must map to NODE namespace");
    assert_eq!(ns.pid, 2, "hostPID must map to NODE namespace");
    assert_eq!(ns.ipc, 2, "hostIPC must map to NODE namespace");
    assert_eq!(
        config.hostname, "",
        "hostNetwork pods should leave sandbox hostname empty"
    );
}

#[test]
fn test_build_creation_error_statuses_sets_waiting_with_error() {
    let pod = serde_json::json!({
        "spec": {
            "containers": [
                {"name": "test-container", "image": "busybox"},
                {"name": "sidecar", "image": "nginx"}
            ]
        }
    });
    let error_msg = "Secret default/my-secret not found";
    let statuses = build_creation_error_statuses(&pod, error_msg);

    assert_eq!(statuses.len(), 2);
    assert_eq!(statuses[0]["name"], "test-container");
    assert_eq!(statuses[0]["image"], "busybox");
    assert_eq!(statuses[0]["ready"], false);
    assert_eq!(
        statuses[0]["state"]["waiting"]["reason"],
        "CreateContainerError"
    );
    assert!(
        statuses[0]["state"]["waiting"]["message"]
            .as_str()
            .unwrap()
            .contains("my-secret not found")
    );

    assert_eq!(statuses[1]["name"], "sidecar");
    assert_eq!(statuses[1]["image"], "nginx");
}

#[test]
fn test_build_creation_error_statuses_with_incomplete_init_uses_pod_initializing() {
    let pod = serde_json::json!({
        "spec": {
            "initContainers": [
                {"name": "init", "image": "busybox"}
            ],
            "containers": [
                {"name": "app", "image": "nginx"}
            ]
        },
        "status": {
            "initContainerStatuses": []
        }
    });
    let error_msg = "temporary startup error";
    let statuses = build_creation_error_statuses(&pod, error_msg);

    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0]["name"], "app");
    assert_eq!(statuses[0]["state"]["waiting"]["reason"], "PodInitializing");
    assert!(statuses[0]["state"]["waiting"]["message"].is_null());
}

#[test]
fn test_build_creation_error_statuses_with_complete_init_keeps_create_container_error() {
    let pod = serde_json::json!({
        "spec": {
            "initContainers": [
                {"name": "init", "image": "busybox"}
            ],
            "containers": [
                {"name": "app", "image": "nginx"}
            ]
        },
        "status": {
            "initContainerStatuses": [
                {
                    "name": "init",
                    "ready": true,
                    "state": {
                        "terminated": {
                            "reason": "Completed",
                            "exitCode": 0,
                        }
                    }
                }
            ]
        }
    });
    let error_msg = "temporary startup error";
    let statuses = build_creation_error_statuses(&pod, error_msg);

    assert_eq!(statuses.len(), 1);
    assert_eq!(
        statuses[0]["state"]["waiting"]["reason"],
        "CreateContainerError"
    );
    assert_eq!(
        statuses[0]["state"]["waiting"]["message"],
        "temporary startup error"
    );
}

#[test]
fn test_build_creation_error_statuses_empty_containers_returns_empty() {
    let pod = serde_json::json!({"spec": {}});
    let statuses = build_creation_error_statuses(&pod, "error");
    assert!(statuses.is_empty());
}

#[test]
fn test_recovery_starts_pending_pod_without_runtime_state() {
    let pod = serde_json::json!({
        "metadata": {
            "creationTimestamp": (chrono::Utc::now() - chrono::Duration::seconds(30)).to_rfc3339(),
        },
        "spec": {"nodeName": "node"},
        "status": {
            "phase": "Pending",
        },
    });

    assert!(matches!(
        decide_startup_action(
            &pod,
            &PodRuntimeState::NotStarted,
            PodStartSource::Recovery,
            "node",
        ),
        StartupDecision::StartFresh
    ));
}

#[test]
fn test_recovery_skips_pod_already_realized_by_runtime() {
    let pod = serde_json::json!({
        "spec": {"nodeName": "node"},
        "status": {
            "phase": "Pending",
        },
    });

    assert!(matches!(
        decide_startup_action(
            &pod,
            &PodRuntimeState::Running,
            PodStartSource::Recovery,
            "node",
        ),
        StartupDecision::Skip
    ));
}

#[test]
fn test_watch_startup_reconciliation_skips_realized_pod_with_pod_ip() {
    let pod = serde_json::json!({
        "spec": {"nodeName": "node"},
        "status": {
            "phase": "Pending",
            "podIP": "10.43.0.5",
        },
    });
    let runtime_state = PodRuntimeState::StartingWithContainers {
        has_running_or_created: false,
    };

    assert!(matches!(
        decide_startup_action(&pod, &runtime_state, PodStartSource::WatchAdded, "node"),
        StartupDecision::Skip
    ));
    assert!(matches!(
        decide_startup_action(&pod, &runtime_state, PodStartSource::Recovery, "node"),
        StartupDecision::RollbackThenStart
    ));
}

#[tokio::test]
async fn test_mark_pod_start_pending_for_retry_keeps_image_pull_non_terminal() {
    use crate::kubelet::pod_repository::PodStatusWriter;
    let db = crate::datastore::test_support::in_memory().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "coredns",
            "namespace": "kube-system",
            "uid": "pod-uid-1",
        },
        "spec": {
            "containers": [{
                "name": "coredns",
                "image": "coredns/coredns:1.11.1"
            }]
        },
        "status": {
            "phase": "Pending"
        }
    });
    db.create_resource("v1", "Pod", Some("kube-system"), "coredns", pod.clone())
        .await
        .unwrap();

    let error_msg = "Failed to pull image docker.io/coredns/coredns:1.11.1: CRI pull_image failed: 429 Too Many Requests";
    super::fixture_pod_repository(&db)
        .mark_start_pending_for_retry_for_uid("kube-system", "coredns", "pod-uid-1", error_msg)
        .await
        .expect("retry-status write must succeed");

    let updated = db
        .get_resource("v1", "Pod", Some("kube-system"), "coredns")
        .await
        .unwrap()
        .unwrap()
        .data;

    assert_eq!(
        updated.pointer("/status/phase").and_then(|v| v.as_str()),
        Some("Pending")
    );
    assert_eq!(
        updated
            .pointer("/status/containerStatuses/0/state/waiting/reason")
            .and_then(|v| v.as_str()),
        Some("ErrImagePull")
    );
    assert!(
        updated.pointer("/status/podIP").and_then(|v| v.as_str()) == Some(""),
        "image-pull backoff before sandbox creation must not invent a pod IP"
    );
}

#[tokio::test]
async fn test_mark_pod_start_pending_for_retry_replaces_existing_pull_status_with_image_error() {
    use crate::kubelet::pod_repository::PodStatusWriter;
    let db = crate::datastore::test_support::in_memory().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "private-image",
            "namespace": "default",
            "uid": "pod-uid-image-pull",
        },
        "spec": {
            "containers": [{
                "name": "app",
                "image": "registry.example.invalid/klights/test-image:1"
            }]
        },
        "status": {
            "phase": "Pending",
            "containerStatuses": [{
                "name": "app",
                "image": "registry.example.invalid/klights/test-image:1",
                "imageID": "",
                "ready": false,
                "started": false,
                "restartCount": 0,
                "state": {
                    "waiting": {
                        "reason": "ContainerCreating",
                        "message": "Pulling image \"registry.example.invalid/klights/test-image:1\""
                    }
                }
            }]
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "private-image", pod.clone())
        .await
        .unwrap();

    let error_msg = "Failed to pull image registry.example.invalid/klights/test-image:1: CRI pull_image failed: pull access denied";
    super::fixture_pod_repository(&db)
        .mark_start_pending_for_retry_for_uid(
            "default",
            "private-image",
            "pod-uid-image-pull",
            error_msg,
        )
        .await
        .expect("retry-status write must succeed");

    let updated = db
        .get_resource("v1", "Pod", Some("default"), "private-image")
        .await
        .unwrap()
        .unwrap()
        .data;

    let waiting = updated
        .pointer("/status/containerStatuses/0/state/waiting")
        .expect("container must still be waiting");
    assert_eq!(
        waiting.pointer("/reason").and_then(|v| v.as_str()),
        Some("ErrImagePull"),
        "image pull failure must replace the old ContainerCreating status"
    );
    assert!(
        waiting
            .pointer("/message")
            .and_then(|v| v.as_str())
            .is_some_and(|message| message.contains("pull access denied")),
        "image pull failure message should preserve the CRI error"
    );
}

#[tokio::test]
async fn test_mark_pod_start_pending_for_retry_rebuilds_status_for_retrying_init_failure() {
    use crate::kubelet::pod_repository::PodStatusWriter;
    let db = crate::datastore::test_support::in_memory().await;
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "init-retry",
            "namespace": "default",
            "uid": "pod-uid-init-retry",
        },
        "spec": {
            "restartPolicy": "Always",
            "initContainers": [
                {"name": "init1", "image": "busybox"},
                {"name": "init2", "image": "busybox"}
            ],
            "containers": [{"name": "run1", "image": "pause"}]
        },
        "status": {
            "phase": "Pending",
            "containerStatuses": [{
                "name": "init1",
                "image": "busybox",
                "ready": false,
                "restartCount": 0,
                "state": {
                    "terminated": {
                        "exitCode": 1,
                        "reason": "Error"
                    }
                }
            }]
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "init-retry", pod.clone())
        .await
        .unwrap();

    let error_msg = "Init container init1 failed with exit code 1";
    super::fixture_pod_repository(&db)
        .mark_start_pending_for_retry_for_uid(
            "default",
            "init-retry",
            "pod-uid-init-retry",
            error_msg,
        )
        .await
        .expect("retry-status write must succeed");

    let updated = db
        .get_resource("v1", "Pod", Some("default"), "init-retry")
        .await
        .unwrap()
        .unwrap()
        .data;

    assert_eq!(
        updated.pointer("/status/phase").and_then(|v| v.as_str()),
        Some("Pending")
    );
    let app_status = updated
        .pointer("/status/containerStatuses/0")
        .expect("app container status must be present");
    assert_eq!(
        app_status.pointer("/name").and_then(|v| v.as_str()),
        Some("run1")
    );
    assert_eq!(
        app_status
            .pointer("/state/waiting/reason")
            .and_then(|v| v.as_str()),
        Some("PodInitializing")
    );

    let init_statuses = updated
        .pointer("/status/initContainerStatuses")
        .and_then(|v| v.as_array())
        .expect("init statuses must be present");
    assert_eq!(init_statuses.len(), 2);
    assert_eq!(
        init_statuses[0].pointer("/name").and_then(|v| v.as_str()),
        Some("init1")
    );
    assert_eq!(
        init_statuses[0]
            .pointer("/state/waiting/reason")
            .and_then(|v| v.as_str()),
        Some("PodInitializing")
    );
    assert_eq!(
        init_statuses[0]
            .pointer("/lastState/terminated/exitCode")
            .and_then(|v| v.as_i64()),
        Some(1)
    );
    assert_eq!(
        init_statuses[0]
            .pointer("/restartCount")
            .and_then(|v| v.as_i64()),
        Some(1)
    );
    assert_eq!(
        init_statuses[1].pointer("/name").and_then(|v| v.as_str()),
        Some("init2")
    );
    assert_eq!(
        init_statuses[1]
            .pointer("/state/waiting/reason")
            .and_then(|v| v.as_str()),
        Some("PodInitializing")
    );
}

// ========================
// P0-9 — phase sync end-to-end (Datastore round-trip)
// ========================
//
// `compute_pod_phase` itself is already covered by 23+ existing tests
// (test_compute_pod_phase_*, test_pod_phase_succeeded, test_pod_phase_failed).
// The bug fixed by P0-9 is NOT in `compute_pod_phase` — it is in the
// monitor loop: when a container exited before `update_pod_status("Running")`
// was called, no further CRI event fired and the pod stayed Running forever.
//
// The fix has two parts: (1) extract `apply_pod_phase_update` so the per-pod
// sync path is testable without CRI, and (2) add a 5s `phase_sync_interval`
// tick in `run_pod_watcher` that calls `monitor_running_pods` periodically.
// This test exercises the extracted function end-to-end against an
// in-memory Datastore, proving the Running → Succeeded write actually lands.

/// P0-9 regression: a Pod stuck in `phase: Running` must transition to
/// `phase: Succeeded` once `apply_pod_phase_update` is invoked with all
/// containers in state Exited (state=2), exit_code=0, and
/// `restartPolicy: Never`. This is the exact scenario triggered by the new
/// `phase_sync_interval` arm in `run_pod_watcher` when the original
/// ContainerStoppedEvent was missed.
#[tokio::test]
async fn test_apply_pod_phase_update_preserves_create_container_config_error_when_cri_empty() {
    let db = crate::datastore::test_support::in_memory().await;

    let initial_pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "bad-subpath",
            "namespace": "default",
            "uid": "uid-bad-subpath",
        },
        "spec": {
            "restartPolicy": "Never",
            "containers": [{
                "name": "dapi-container",
                "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1"
            }],
        },
        "status": {
            "phase": "Pending",
            "containerStatuses": [{
                "name": "dapi-container",
                "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1",
                "imageID": "",
                "ready": false,
                "started": false,
                "state": {
                    "waiting": {
                        "reason": "CreateContainerConfigError",
                        "message": "invalid subPath in container dapi-container"
                    }
                }
            }],
        },
    });
    db.create_resource("v1", "Pod", Some("default"), "bad-subpath", initial_pod)
        .await
        .unwrap();

    let pod_resource = db
        .get_resource("v1", "Pod", Some("default"), "bad-subpath")
        .await
        .unwrap()
        .unwrap();

    let container_infos = Vec::new();
    let restart_counts = std::collections::HashMap::new();
    apply_pod_phase_update(
        &super::fixture_pod_repository(&db),
        PodPhaseUpdateRequest {
            pod_resource: &pod_resource,
            container_infos: &container_infos,
            restart_counts: &restart_counts,
            current_phase: Some("Pending"),
            new_phase: "Pending",
            namespace: "default",
            pod_name: "bad-subpath",
        },
    )
    .await;

    let after = db
        .get_resource("v1", "Pod", Some("default"), "bad-subpath")
        .await
        .unwrap()
        .unwrap();
    let statuses = after
        .data
        .pointer("/status/containerStatuses")
        .and_then(|v| v.as_array())
        .expect("containerStatuses must remain present");
    assert_eq!(statuses.len(), 1);
    assert_eq!(
        statuses[0]
            .pointer("/state/waiting/reason")
            .and_then(|v| v.as_str()),
        Some("CreateContainerConfigError"),
        "runtime reconcile with no CRI containers must not erase the config error"
    );
}

#[tokio::test]
async fn test_apply_pod_phase_update_preserves_live_create_container_config_error_when_snapshot_stale()
 {
    let db = crate::datastore::test_support::in_memory().await;

    let initial_pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "stale-bad-subpath",
            "namespace": "default",
            "uid": "uid-stale-bad-subpath",
        },
        "spec": {
            "restartPolicy": "Never",
            "containers": [{
                "name": "dapi-container",
                "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1"
            }],
        },
        "status": {
            "phase": "Pending",
        },
    });
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "stale-bad-subpath",
        initial_pod,
    )
    .await
    .unwrap();

    let stale_snapshot = db
        .get_resource("v1", "Pod", Some("default"), "stale-bad-subpath")
        .await
        .unwrap()
        .unwrap();

    db.update_status_only(
        "v1",
        "Pod",
        Some("default"),
        "stale-bad-subpath",
        serde_json::json!({
            "phase": "Pending",
            "containerStatuses": [{
                "name": "dapi-container",
                "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1",
                "imageID": "",
                "ready": false,
                "started": false,
                "state": {
                    "waiting": {
                        "reason": "CreateContainerConfigError",
                        "message": "invalid subPath in container dapi-container"
                    }
                }
            }],
        }),
        None,
    )
    .await
    .unwrap();

    let container_infos = Vec::new();
    let restart_counts = std::collections::HashMap::new();
    apply_pod_phase_update(
        &super::fixture_pod_repository(&db),
        PodPhaseUpdateRequest {
            pod_resource: &stale_snapshot,
            container_infos: &container_infos,
            restart_counts: &restart_counts,
            current_phase: Some("Pending"),
            new_phase: "Pending",
            namespace: "default",
            pod_name: "stale-bad-subpath",
        },
    )
    .await;

    let after = db
        .get_resource("v1", "Pod", Some("default"), "stale-bad-subpath")
        .await
        .unwrap()
        .unwrap();
    let statuses = after
        .data
        .pointer("/status/containerStatuses")
        .and_then(|v| v.as_array())
        .expect("containerStatuses must remain present");
    assert_eq!(statuses.len(), 1);
    assert_eq!(
        statuses[0]
            .pointer("/state/waiting/reason")
            .and_then(|v| v.as_str()),
        Some("CreateContainerConfigError"),
        "stale runtime reconcile with no CRI containers must not erase a live config error"
    );
}

#[tokio::test]
async fn test_apply_pod_phase_update_running_to_succeeded_on_clean_exit_never() {
    let db = crate::datastore::test_support::in_memory().await;

    // Seed a Running pod in the datastore (mirrors what create_pod writes
    // after starting the container).
    let initial_pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "log-test",
            "namespace": "default",
            "annotations": {
                "klights.dev/sandbox-id": "sandbox-abc",
            },
        },
        "spec": {
            "restartPolicy": "Never",
            "containers": [{ "name": "app", "image": "registry.example.invalid/klights/test-image:1" }],
        },
        "status": {
            "phase": "Running",
            "containerStatuses": [{
                "name": "app",
                "ready": true,
                "started": true,
                "restartCount": 0,
                "state": { "running": { "startedAt": "2026-04-18T00:00:00Z" } },
            }],
        },
    });
    db.create_resource("v1", "Pod", Some("default"), "log-test", initial_pod)
        .await
        .unwrap();

    // Re-fetch to capture the resource_version assigned by the datastore.
    let pod_resource = db
        .get_resource("v1", "Pod", Some("default"), "log-test")
        .await
        .unwrap()
        .unwrap();

    // Container has now exited cleanly — this is what CRI would report
    // after the container completed (state=2 Exited, exit_code=0).
    let container_infos = vec![make_container("app", 2, 0)];
    let restart_counts = std::collections::HashMap::new();

    // Sanity-check the precondition: phase computation says Succeeded.
    assert_eq!(
        compute_pod_phase(&container_infos, "Never"),
        "Succeeded",
        "precondition: compute_pod_phase agrees the pod should be Succeeded"
    );

    // Drive the extracted phase-sync path that the new
    // `phase_sync_interval` arm calls each tick.
    apply_pod_phase_update(
        &super::fixture_pod_repository(&db),
        PodPhaseUpdateRequest {
            pod_resource: &pod_resource,
            container_infos: &container_infos,
            restart_counts: &restart_counts,
            current_phase: Some("Running"),
            new_phase: "Succeeded",
            namespace: "default",
            pod_name: "log-test",
        },
    )
    .await;

    // The pod in the datastore must now report Succeeded — the fix's
    // observable contract for the integration test pod_logs.sh.
    let after = db
        .get_resource("v1", "Pod", Some("default"), "log-test")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after.data.pointer("/status/phase").and_then(|p| p.as_str()),
        Some("Succeeded"),
        "Pod must transition from Running to Succeeded after apply_pod_phase_update"
    );

    // Container status reflects the terminated state with exit_code 0.
    let cs = after
        .data
        .pointer("/status/containerStatuses/0")
        .expect("containerStatuses[0] present");
    assert_eq!(
        cs.pointer("/state/terminated/exitCode")
            .and_then(|c| c.as_i64()),
        Some(0),
        "containerStatuses[0].state.terminated.exitCode must be 0"
    );
    assert_eq!(
        cs.pointer("/state/terminated/reason")
            .and_then(|r| r.as_str()),
        Some("Completed"),
        "containerStatuses[0].state.terminated.reason must be Completed for exit 0"
    );
}

#[tokio::test]
async fn test_apply_pod_phase_update_keeps_failed_init_pod_terminal() {
    let db = crate::datastore::test_support::in_memory().await;

    let initial_pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "init-failed",
            "namespace": "default",
            "annotations": {"klights.dev/sandbox-id": "sandbox-init-failed"},
        },
        "spec": {
            "restartPolicy": "Never",
            "initContainers": [
                {"name": "init1", "image": "busybox"},
                {"name": "init2", "image": "busybox"}
            ],
            "containers": [{"name": "run1", "image": "busybox"}],
        },
        "status": {
            "phase": "Failed",
            "conditions": [
                {"type": "PodScheduled", "status": "True"},
                {
                    "type": "Initialized",
                    "status": "False",
                    "reason": "ContainersNotInitialized",
                    "message": "containers with incomplete status: [init2]"
                },
                {"type": "ContainersReady", "status": "False"},
                {"type": "Ready", "status": "False"}
            ],
            "initContainerStatuses": [
                {
                    "name": "init1",
                    "image": "busybox",
                    "ready": true,
                    "restartCount": 0,
                    "state": {"terminated": {"exitCode": 0, "reason": "Completed"}}
                },
                {
                    "name": "init2",
                    "image": "busybox",
                    "ready": false,
                    "restartCount": 0,
                    "state": {"terminated": {"exitCode": 1, "reason": "Error"}}
                }
            ],
            "containerStatuses": [{
                "name": "run1",
                "image": "busybox",
                "ready": false,
                "started": false,
                "restartCount": 0,
                "state": {"waiting": {"reason": "PodInitializing"}}
            }]
        },
    });
    db.create_resource("v1", "Pod", Some("default"), "init-failed", initial_pod)
        .await
        .unwrap();
    let pod_resource = db
        .get_resource("v1", "Pod", Some("default"), "init-failed")
        .await
        .unwrap()
        .unwrap();

    apply_pod_phase_update(
        &super::fixture_pod_repository(&db),
        PodPhaseUpdateRequest {
            pod_resource: &pod_resource,
            container_infos: &[],
            restart_counts: &std::collections::HashMap::new(),
            current_phase: Some("Failed"),
            new_phase: "Pending",
            namespace: "default",
            pod_name: "init-failed",
        },
    )
    .await;

    let after = db
        .get_resource("v1", "Pod", Some("default"), "init-failed")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after.data.pointer("/status/phase").and_then(|p| p.as_str()),
        Some("Failed"),
        "runtime reconciliation must not downgrade a terminal init-container failure"
    );
    assert_eq!(
        after
            .data
            .pointer("/status/initContainerStatuses/1/state/terminated/exitCode")
            .and_then(|v| v.as_i64()),
        Some(1),
        "failed init container status must survive CRI events with no app containers"
    );
    assert_eq!(
        after
            .data
            .pointer("/status/containerStatuses/0/state/waiting/reason")
            .and_then(|v| v.as_str()),
        Some("PodInitializing"),
        "app containers must remain waiting after the init container failure"
    );
}

/// Pins the "kubelet status writes preserve unrelated status fields" contract:
/// any status field this code path doesn't explicitly touch (qosClass, startTime,
/// message, reason) must survive the update. Catches regressions where switching
/// to a status-subresource-style write accidentally drops fields the kubelet
/// doesn't own.
#[tokio::test]
async fn test_apply_pod_phase_update_preserves_unrelated_status_fields() {
    let db = crate::datastore::test_support::in_memory().await;

    let initial_pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "preserve-test",
            "namespace": "default",
            "annotations": {"klights.dev/sandbox-id": "sandbox-xyz"},
        },
        "spec": {
            "restartPolicy": "Never",
            "containers": [{ "name": "app", "image": "registry.example.invalid/klights/test-image:1" }],
        },
        "status": {
            "phase": "Running",
            "qosClass": "Burstable",
            "startTime": "2026-04-29T00:00:00Z",
            "message": "kubelet-set message",
            "reason": "kubelet-set reason",
            "containerStatuses": [{
                "name": "app",
                "ready": true,
                "started": true,
                "restartCount": 0,
                "state": { "running": { "startedAt": "2026-04-29T00:00:00Z" } },
            }],
        },
    });
    db.create_resource("v1", "Pod", Some("default"), "preserve-test", initial_pod)
        .await
        .unwrap();
    let pod_resource = db
        .get_resource("v1", "Pod", Some("default"), "preserve-test")
        .await
        .unwrap()
        .unwrap();

    let container_infos = vec![make_container("app", 2, 0)];
    let restart_counts = std::collections::HashMap::new();

    apply_pod_phase_update(
        &super::fixture_pod_repository(&db),
        PodPhaseUpdateRequest {
            pod_resource: &pod_resource,
            container_infos: &container_infos,
            restart_counts: &restart_counts,
            current_phase: Some("Running"),
            new_phase: "Succeeded",
            namespace: "default",
            pod_name: "preserve-test",
        },
    )
    .await;

    let after = db
        .get_resource("v1", "Pod", Some("default"), "preserve-test")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after.data.pointer("/status/phase").and_then(|p| p.as_str()),
        Some("Succeeded"),
        "phase must update to Succeeded"
    );
    assert_eq!(
        after
            .data
            .pointer("/status/qosClass")
            .and_then(|q| q.as_str()),
        Some("Burstable"),
        "qosClass must survive a kubelet status write"
    );
    assert_eq!(
        after
            .data
            .pointer("/status/startTime")
            .and_then(|q| q.as_str()),
        Some("2026-04-29T00:00:00Z"),
        "startTime must survive a kubelet status write"
    );
    assert_eq!(
        after
            .data
            .pointer("/status/message")
            .and_then(|q| q.as_str()),
        Some("kubelet-set message"),
        "message must survive a kubelet status write"
    );
    assert_eq!(
        after
            .data
            .pointer("/status/reason")
            .and_then(|q| q.as_str()),
        Some("kubelet-set reason"),
        "reason must survive a kubelet status write"
    );
}

#[tokio::test]
async fn test_apply_pod_phase_update_never_decreases_restart_count() {
    let db = crate::datastore::test_support::in_memory().await;

    let initial_pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "restart-monotonic",
            "namespace": "default",
            "annotations": {"klights.dev/sandbox-id": "sandbox-restart"},
        },
        "spec": {
            "restartPolicy": "Always",
            "containers": [{ "name": "app", "image": "agnhost" }],
        },
        "status": {
            "phase": "Running",
            "containerStatuses": [{
                "name": "app",
                "ready": true,
                "started": true,
                "restartCount": 1,
                "lastState": {
                    "terminated": {
                        "exitCode": 137,
                        "reason": "Error",
                    },
                },
                "state": { "running": { "startedAt": "2026-04-30T00:00:00Z" } },
            }],
        },
    });
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "restart-monotonic",
        initial_pod,
    )
    .await
    .unwrap();

    let pod_resource = db
        .get_resource("v1", "Pod", Some("default"), "restart-monotonic")
        .await
        .unwrap()
        .unwrap();

    let container_infos = vec![make_container("app", 1, 0)];
    let restart_counts = std::collections::HashMap::new();

    apply_pod_phase_update(
        &super::fixture_pod_repository(&db),
        PodPhaseUpdateRequest {
            pod_resource: &pod_resource,
            container_infos: &container_infos,
            restart_counts: &restart_counts,
            current_phase: Some("Running"),
            new_phase: "Running",
            namespace: "default",
            pod_name: "restart-monotonic",
        },
    )
    .await;

    let after = db
        .get_resource("v1", "Pod", Some("default"), "restart-monotonic")
        .await
        .unwrap()
        .unwrap();
    let status = after
        .data
        .pointer("/status/containerStatuses/0")
        .expect("containerStatuses[0] present");
    assert_eq!(
        status.get("restartCount").and_then(|v| v.as_i64()),
        Some(1),
        "restartCount must not decrease when kubelet rebuilds containerStatuses"
    );
    assert!(
        status.pointer("/lastState/terminated").is_some(),
        "lastState must still be preserved alongside restartCount"
    );
}

#[tokio::test]
async fn test_runtime_restart_status_increment_uses_live_pod_with_stale_snapshot() {
    let db = crate::datastore::test_support::in_memory().await;

    let initial_pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "runtime-restart-live-count",
            "namespace": "default",
            "annotations": {"klights.dev/sandbox-id": "sandbox-restart"},
        },
        "spec": {
            "restartPolicy": "Always",
            "containers": [{ "name": "app", "image": "agnhost" }],
        },
        "status": {
            "phase": "Running",
            "podIP": "10.43.0.99",
            "podIPs": [{"ip": "10.43.0.99"}],
            "containerStatuses": [{
                "name": "app",
                "containerID": "containerd://initial-id",
                "ready": true,
                "started": true,
                "restartCount": 0,
                "state": { "running": { "startedAt": "2026-04-30T00:00:00Z" } },
            }],
        },
    });
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "runtime-restart-live-count",
        initial_pod,
    )
    .await
    .unwrap();

    let stale_snapshot = db
        .get_resource("v1", "Pod", Some("default"), "runtime-restart-live-count")
        .await
        .unwrap()
        .unwrap();
    let pod_repo = super::fixture_pod_repository(&db);
    let (_, mut exited) = make_container("app", 2, 1);

    persist_runtime_restart_status(
        &pod_repo,
        &stale_snapshot,
        "default",
        "runtime-restart-live-count",
        "app",
        &exited,
    )
    .await
    .unwrap();

    exited.exit_code = 2;
    exited.finished_at += 1_000_000_000;
    persist_runtime_restart_status(
        &pod_repo,
        &stale_snapshot,
        "default",
        "runtime-restart-live-count",
        "app",
        &exited,
    )
    .await
    .unwrap();

    let after = db
        .get_resource("v1", "Pod", Some("default"), "runtime-restart-live-count")
        .await
        .unwrap()
        .unwrap();
    let status = after
        .data
        .pointer("/status/containerStatuses/0")
        .expect("containerStatuses[0] present");
    assert_eq!(
        status.get("restartCount").and_then(|v| v.as_i64()),
        Some(2),
        "restartCount must be incremented from the live pod, not a stale runtime event snapshot"
    );
    assert_eq!(
        status.pointer("/lastState/terminated/exitCode"),
        Some(&serde_json::json!(2)),
        "lastState should describe the most recent failed attempt"
    );
}

#[tokio::test]
async fn test_apply_pod_phase_update_preserves_same_container_started_at() {
    let db = crate::datastore::test_support::in_memory().await;

    let initial_started_at = "2026-04-30T00:00:00Z";
    let initial_pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "stable-started-at",
            "namespace": "default",
            "annotations": {"klights.dev/sandbox-id": "sandbox-stable-start"},
        },
        "spec": {
            "restartPolicy": "Never",
            "containers": [{ "name": "app", "image": "app:latest" }],
        },
        "status": {
            "phase": "Running",
            "conditions": [
                {"type": "Ready", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"}
            ],
            "containerStatuses": [{
                "name": "app",
                "containerID": "containerd://app-id",
                "image": "app:latest",
                "imageID": "docker.io/library/app:latest",
                "ready": true,
                "started": true,
                "restartCount": 0,
                "state": { "running": { "startedAt": initial_started_at } },
            }],
        },
    });
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "stable-started-at",
        initial_pod,
    )
    .await
    .unwrap();

    let pod_resource = db
        .get_resource("v1", "Pod", Some("default"), "stable-started-at")
        .await
        .unwrap()
        .unwrap();
    let before_rv = pod_resource.resource_version;
    let container_infos = vec![make_container("app", 1, 0)];
    let restart_counts = std::collections::HashMap::new();

    apply_pod_phase_update(
        &super::fixture_pod_repository(&db),
        PodPhaseUpdateRequest {
            pod_resource: &pod_resource,
            container_infos: &container_infos,
            restart_counts: &restart_counts,
            current_phase: Some("Running"),
            new_phase: "Running",
            namespace: "default",
            pod_name: "stable-started-at",
        },
    )
    .await;

    let after = db
        .get_resource("v1", "Pod", Some("default"), "stable-started-at")
        .await
        .unwrap()
        .unwrap();
    // After the runtime-reconcile dedup gate was removed (sonobuoy "Container
    // Runtime … should run with the expected status"), every reconcile now
    // bumps RV — even when the only delta is the same-container startedAt —
    // because skipping was masking real terminated→running transitions. The
    // invariant that still holds is the startedAt preservation:
    // `preserve_published_container_started_at` must keep the prior wall-clock
    // value when the containerID is unchanged.
    assert!(
        after.resource_version >= before_rv,
        "runtime reconcile must not regress RV"
    );
    assert_eq!(
        after
            .data
            .pointer("/status/containerStatuses/0/state/running/startedAt")
            .and_then(|v| v.as_str()),
        Some(initial_started_at),
        "same-container startedAt must still be preserved across reconciles"
    );
}

#[tokio::test]
async fn test_apply_pod_phase_update_reuses_running_started_at_when_terminal() {
    let db = crate::datastore::test_support::in_memory().await;

    let initial_started_at = "2026-04-30T00:00:00Z";
    let initial_pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "terminal-started-at",
            "namespace": "default",
            "annotations": {"klights.dev/sandbox-id": "sandbox-terminal-start"},
        },
        "spec": {
            "restartPolicy": "Never",
            "containers": [{ "name": "app", "image": "app:latest" }],
        },
        "status": {
            "phase": "Running",
            "conditions": [
                {"type": "Ready", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"}
            ],
            "containerStatuses": [{
                "name": "app",
                "containerID": "containerd://app-id",
                "image": "app:latest",
                "imageID": "docker.io/library/app:latest",
                "ready": true,
                "started": true,
                "restartCount": 0,
                "state": { "running": { "startedAt": initial_started_at } },
            }],
        },
    });
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "terminal-started-at",
        initial_pod,
    )
    .await
    .unwrap();

    let pod_resource = db
        .get_resource("v1", "Pod", Some("default"), "terminal-started-at")
        .await
        .unwrap()
        .unwrap();
    let container_infos = vec![make_container("app", 2, 0)];
    let restart_counts = std::collections::HashMap::new();

    apply_pod_phase_update(
        &super::fixture_pod_repository(&db),
        PodPhaseUpdateRequest {
            pod_resource: &pod_resource,
            container_infos: &container_infos,
            restart_counts: &restart_counts,
            current_phase: Some("Running"),
            new_phase: "Succeeded",
            namespace: "default",
            pod_name: "terminal-started-at",
        },
    )
    .await;

    let after = db
        .get_resource("v1", "Pod", Some("default"), "terminal-started-at")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after
            .data
            .pointer("/status/containerStatuses/0/state/terminated/startedAt")
            .and_then(|v| v.as_str()),
        Some(initial_started_at),
        "terminal status must preserve the start time already published for the same container"
    );
}

#[tokio::test]
async fn test_apply_pod_phase_update_survives_status_conflict() {
    let db = crate::datastore::test_support::in_memory().await;

    let initial_pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "restart-conflict",
            "namespace": "default",
            "annotations": {"klights.dev/sandbox-id": "sandbox-restart"},
        },
        "spec": {
            "restartPolicy": "Always",
            "containers": [{ "name": "app", "image": "agnhost" }],
        },
        "status": {
            "phase": "Running",
            // Seed podIP so the runtime-reconcile guard
            // (no-podIP-while-Running deferral) doesn't fire and the
            // test exercises its actual subject (status conflict
            // survival).
            "podIP": "10.43.0.99",
            "podIPs": [{"ip": "10.43.0.99"}],
            "conditions": [
                {"type": "Ready", "status": "True", "lastTransitionTime": "2026-04-30T00:00:00Z"}
            ],
            "containerStatuses": [{
                "name": "app",
                "containerID": "containerd://old-id",
                "ready": true,
                "started": true,
                "restartCount": 1,
                "state": { "running": { "startedAt": "2026-04-30T00:00:00Z" } },
            }],
        },
    });
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "restart-conflict",
        initial_pod,
    )
    .await
    .unwrap();

    let stale_snapshot = db
        .get_resource("v1", "Pod", Some("default"), "restart-conflict")
        .await
        .unwrap()
        .unwrap();

    let mut concurrent_status = stale_snapshot
        .data
        .get("status")
        .cloned()
        .expect("status present");
    concurrent_status["qosClass"] = serde_json::json!("BestEffort");
    db.update_status_only(
        "v1",
        "Pod",
        Some("default"),
        "restart-conflict",
        concurrent_status,
        Some(stale_snapshot.resource_version),
    )
    .await
    .unwrap();

    let container_infos = vec![(
        "app".to_string(),
        ContainerInfo {
            container_id: "new-id".to_string(),
            image: "agnhost:latest".to_string(),
            image_ref: "registry.k8s.io/e2e-test-images/agnhost:latest".to_string(),
            state: 1,
            exit_code: 0,
            started_at: 1_600_000_020_000_000_000,
            finished_at: 0,
            termination_message: String::new(),
        },
    )];
    let restart_counts = std::collections::HashMap::from([("app".to_string(), 2)]);

    apply_pod_phase_update(
        &super::fixture_pod_repository(&db),
        PodPhaseUpdateRequest {
            pod_resource: &stale_snapshot,
            container_infos: &container_infos,
            restart_counts: &restart_counts,
            current_phase: Some("Running"),
            new_phase: "Running",
            namespace: "default",
            pod_name: "restart-conflict",
        },
    )
    .await;

    let after = db
        .get_resource("v1", "Pod", Some("default"), "restart-conflict")
        .await
        .unwrap()
        .unwrap();
    let status = after
        .data
        .pointer("/status/containerStatuses/0")
        .expect("containerStatuses[0] present");
    assert_eq!(
        status.get("containerID").and_then(|v| v.as_str()),
        Some("containerd://new-id"),
        "runtime reconcile must not drop the latest container ID after a status conflict"
    );
    assert_eq!(
        status.get("restartCount").and_then(|v| v.as_i64()),
        Some(2),
        "runtime reconcile must publish the latest restart count after a status conflict"
    );
    assert_eq!(
        after
            .data
            .pointer("/status/qosClass")
            .and_then(|v| v.as_str()),
        Some("BestEffort"),
        "runtime reconcile must preserve concurrent status fields"
    );
}

#[tokio::test]
async fn test_apply_pod_phase_update_reconciles_pdb_on_ready_transition() {
    let db = crate::datastore::test_support::in_memory().await;

    let pdb = serde_json::json!({
        "apiVersion": "policy/v1",
        "kind": "PodDisruptionBudget",
        "metadata": {"name": "pdb-a", "namespace": "default"},
        "spec": {
            "minAvailable": 0,
            "selector": {"matchLabels": {"app": "myapp"}}
        }
    });
    db.create_resource(
        "policy/v1",
        "PodDisruptionBudget",
        Some("default"),
        "pdb-a",
        pdb.clone(),
    )
    .await
    .unwrap();

    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "pod-a",
            "namespace": "default",
            "labels": {"app": "myapp"}
        },
        "spec": {
            "restartPolicy": "Always",
            "containers": [{"name": "app", "image": "busybox"}]
        },
        "status": {
            "phase": "Pending",
            // Seed podIP so the runtime-reconcile guard
            // (no-podIP-while-Running deferral) doesn't fire when the
            // test transitions phase Pending → Running.
            "podIP": "10.43.0.42",
            "podIPs": [{"ip": "10.43.0.42"}],
            "conditions": [{"type": "Ready", "status": "False"}]
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "pod-a", pod)
        .await
        .unwrap();

    let pod_resource = db
        .get_resource("v1", "Pod", Some("default"), "pod-a")
        .await
        .unwrap()
        .unwrap();

    crate::controllers::pdb::reconcile_pdb(
        &db,
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        &pdb,
    )
    .await
    .unwrap();
    let before = db
        .get_resource("policy/v1", "PodDisruptionBudget", Some("default"), "pdb-a")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        before
            .data
            .pointer("/status/disruptionsAllowed")
            .and_then(|v| v.as_i64()),
        Some(0)
    );

    let container_infos = vec![make_container("app", 1, 0)];
    let restart_counts = std::collections::HashMap::new();
    apply_pod_phase_update(
        &super::fixture_pod_repository(&db),
        PodPhaseUpdateRequest {
            pod_resource: &pod_resource,
            container_infos: &container_infos,
            restart_counts: &restart_counts,
            current_phase: Some("Pending"),
            new_phase: "Running",
            namespace: "default",
            pod_name: "pod-a",
        },
    )
    .await;

    // PDB reconciliation is now async (spawned via TaskSupervisor).
    // Wait for the spawned task to complete before asserting.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let after = db
        .get_resource("policy/v1", "PodDisruptionBudget", Some("default"), "pdb-a")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after
            .data
            .pointer("/status/currentHealthy")
            .and_then(|v| v.as_i64()),
        Some(1)
    );
    assert_eq!(
        after
            .data
            .pointer("/status/disruptionsAllowed")
            .and_then(|v| v.as_i64()),
        Some(1),
        "PDB status must be reconciled after kubelet marks pod ready"
    );
}

#[tokio::test]
async fn test_apply_pod_phase_update_repairs_pod_ips_arrays() {
    let db = crate::datastore::test_support::in_memory().await;

    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "pod-ip-repair",
            "namespace": "default"
        },
        "spec": {
            "restartPolicy": "Always",
            "containers": [{"name": "app", "image": "busybox"}]
        },
        "status": {
            "phase": "Running",
            "podIP": "10.43.0.9",
            "hostIP": "10.206.0.5",
            "conditions": [{"type": "Ready", "status": "True"}],
            "containerStatuses": [{
                "name": "app",
                "ready": true,
                "state": {"running": {"startedAt": "2026-01-01T00:00:00Z"}}
            }]
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "pod-ip-repair", pod)
        .await
        .unwrap();

    let pod_resource = db
        .get_resource("v1", "Pod", Some("default"), "pod-ip-repair")
        .await
        .unwrap()
        .unwrap();

    let container_infos = vec![make_container("app", 1, 0)];
    let restart_counts = std::collections::HashMap::new();
    apply_pod_phase_update(
        &super::fixture_pod_repository(&db),
        PodPhaseUpdateRequest {
            pod_resource: &pod_resource,
            container_infos: &container_infos,
            restart_counts: &restart_counts,
            current_phase: Some("Running"),
            new_phase: "Running",
            namespace: "default",
            pod_name: "pod-ip-repair",
        },
    )
    .await;

    let after = db
        .get_resource("v1", "Pod", Some("default"), "pod-ip-repair")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after
            .data
            .pointer("/status/podIPs/0/ip")
            .and_then(|v| v.as_str()),
        Some("10.43.0.9")
    );
    assert_eq!(
        after
            .data
            .pointer("/status/hostIPs/0/ip")
            .and_then(|v| v.as_str()),
        Some("10.206.0.5")
    );
}

#[tokio::test]
async fn test_apply_pod_phase_update_repairs_scalar_pod_ips_from_arrays() {
    let db = crate::datastore::test_support::in_memory().await;

    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "pod-ip-scalar-repair",
            "namespace": "default"
        },
        "spec": {
            "restartPolicy": "Always",
            "containers": [{"name": "app", "image": "busybox"}]
        },
        "status": {
            "phase": "Running",
            "podIPs": [{"ip": "10.43.0.14"}],
            "hostIPs": [{"ip": "10.206.0.5"}],
            "conditions": [{"type": "Ready", "status": "True"}],
            "containerStatuses": [{
                "name": "app",
                "ready": true,
                "state": {"running": {"startedAt": "2026-01-01T00:00:00Z"}}
            }]
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "pod-ip-scalar-repair", pod)
        .await
        .unwrap();

    let pod_resource = db
        .get_resource("v1", "Pod", Some("default"), "pod-ip-scalar-repair")
        .await
        .unwrap()
        .unwrap();

    let container_infos = vec![make_container("app", 1, 0)];
    let restart_counts = std::collections::HashMap::new();
    apply_pod_phase_update(
        &super::fixture_pod_repository(&db),
        PodPhaseUpdateRequest {
            pod_resource: &pod_resource,
            container_infos: &container_infos,
            restart_counts: &restart_counts,
            current_phase: Some("Running"),
            new_phase: "Running",
            namespace: "default",
            pod_name: "pod-ip-scalar-repair",
        },
    )
    .await;

    let after = db
        .get_resource("v1", "Pod", Some("default"), "pod-ip-scalar-repair")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after.data.pointer("/status/podIP").and_then(|v| v.as_str()),
        Some("10.43.0.14")
    );
    assert_eq!(
        after
            .data
            .pointer("/status/hostIP")
            .and_then(|v| v.as_str()),
        Some("10.206.0.5")
    );
}

// --- enqueue_job_reconcile_for_pod tests ---
//
// These tests verify the async-enqueue replacement for the old synchronous
// reconcile_job_for_pod_owner path.

#[tokio::test]
async fn test_enqueue_job_reconcile_no_owner_is_noop() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let pod = serde_json::json!({
        "metadata": {"name": "pod", "namespace": "default"},
        "spec": {"nodeName": "node"},
        "status": {"phase": "Succeeded"}
    });
    // No ownerReferences — must not panic and must not enqueue anything.
    pod_repo.enqueue_job_reconcile_for_pod(&pod).await;
}

#[tokio::test]
async fn test_enqueue_job_reconcile_non_job_owner_is_noop() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let pod = serde_json::json!({
        "metadata": {
            "name": "pod",
            "namespace": "default",
            "ownerReferences": [{"kind": "ReplicaSet", "name": "my-rs", "uid": "rs-uid"}]
        },
        "spec": {"nodeName": "node"},
        "status": {"phase": "Succeeded"}
    });
    // Non-Job owner — must be a no-op.
    pod_repo.enqueue_job_reconcile_for_pod(&pod).await;
}

#[tokio::test]
async fn test_enqueue_job_reconcile_enqueues_job_key_via_dispatcher() {
    let (db, db_handle) = crate::datastore::test_support::in_memory_with_handle().await;
    let service_ipam = std::sync::Arc::new(crate::controllers::service::ServiceIpam::new(
        "10.43.128.0/17",
    ));
    let dispatcher = std::sync::Arc::new(crate::controller_dispatcher::ControllerDispatcher::new(
        service_ipam,
    ));
    let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    ));
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = std::sync::Arc::new(crate::side_effects::SideEffectRegistry::new());
    side_effects.set_controller_dispatcher(dispatcher.clone());
    let pod_repo = std::sync::Arc::new(crate::kubelet::pod_repository::PodRepository::new(
        db_handle,
        supervisor,
        side_effects,
        metrics,
    ));

    let job = serde_json::json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "my-job", "namespace": "default", "uid": "job-uid"},
        "spec": {
            "completions": 1,
            "parallelism": 1,
            "template": {
                "spec": {
                    "containers": [{"name": "w", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    });
    db.create_resource("batch/v1", "Job", Some("default"), "my-job", job)
        .await
        .unwrap();

    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "my-job-pod",
            "namespace": "default",
            "ownerReferences": [{
                "apiVersion": "batch/v1",
                "kind": "Job",
                "name": "my-job",
                "uid": "job-uid",
                "controller": true
            }]
        },
        "spec": {
            "nodeName": "test-node",
            "containers": [{"name": "w", "image": "busybox"}]
        },
        "status": {"phase": "Succeeded"}
    });
    db.create_resource("v1", "Pod", Some("default"), "my-job-pod", pod.clone())
        .await
        .unwrap();

    dispatcher
        .enqueue_reconcile_key(crate::controllers::workqueue::ReconcileKey::namespaced(
            "apps/v1",
            "Deployment",
            "default",
            "normal-backlog",
        ))
        .await;
    pod_repo.enqueue_job_reconcile_for_pod(&pod).await;

    let keys = dispatcher.queued_reconcile_keys_for_test().await;
    assert!(
        keys.contains(&crate::controllers::workqueue::ReconcileKey::namespaced(
            "batch/v1", "Job", "default", "my-job"
        )),
        "terminal Job pod reconcile must enqueue the owning Job"
    );
    assert!(
        keys.contains(&crate::controllers::workqueue::ReconcileKey::namespaced(
            "apps/v1",
            "Deployment",
            "default",
            "normal-backlog",
        )),
        "normal backlog must remain queued"
    );
}

#[tokio::test]
async fn test_terminal_watch_modified_pod_enqueues_job_reconcile() {
    let (db, db_handle) = crate::datastore::test_support::in_memory_with_handle().await;
    let service_ipam = std::sync::Arc::new(crate::controllers::service::ServiceIpam::new(
        "10.43.128.0/17",
    ));
    let dispatcher = std::sync::Arc::new(crate::controller_dispatcher::ControllerDispatcher::new(
        service_ipam,
    ));
    let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    ));
    let metrics = crate::side_effects::SideEffectMetrics::new();
    let side_effects = std::sync::Arc::new(crate::side_effects::SideEffectRegistry::new());
    side_effects.set_controller_dispatcher(dispatcher.clone());
    let pod_repo = std::sync::Arc::new(crate::kubelet::pod_repository::PodRepository::new(
        db_handle,
        supervisor,
        side_effects,
        metrics,
    ));

    let job = serde_json::json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "indexed-job", "namespace": "default", "uid": "job-uid"},
        "spec": {
            "completionMode": "Indexed",
            "completions": 2,
            "parallelism": 2,
            "template": {
                "spec": {
                    "containers": [{"name": "w", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    });
    db.create_resource("batch/v1", "Job", Some("default"), "indexed-job", job)
        .await
        .unwrap();

    let terminal_watch_pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "indexed-job-0",
            "namespace": "default",
            "ownerReferences": [{
                "apiVersion": "batch/v1",
                "kind": "Job",
                "name": "indexed-job",
                "uid": "job-uid",
                "controller": true
            }]
        },
        "spec": {
            "nodeName": "test-node",
            "containers": [{"name": "w", "image": "busybox"}]
        },
        "status": {"phase": "Succeeded"}
    });

    dispatcher
        .enqueue_reconcile_key(crate::controllers::workqueue::ReconcileKey::namespaced(
            "apps/v1",
            "Deployment",
            "default",
            "normal-backlog",
        ))
        .await;
    event_handlers::enqueue_job_reconcile_for_terminal_watch_pod(&pod_repo, &terminal_watch_pod)
        .await;

    let keys = dispatcher.queued_reconcile_keys_for_test().await;
    assert!(
        keys.contains(&crate::controllers::workqueue::ReconcileKey::namespaced(
            "batch/v1",
            "Job",
            "default",
            "indexed-job",
        )),
        "terminal Pod watch events must enqueue the owning indexed Job"
    );
    assert!(
        keys.contains(&crate::controllers::workqueue::ReconcileKey::namespaced(
            "apps/v1",
            "Deployment",
            "default",
            "normal-backlog",
        )),
        "normal backlog must remain queued"
    );
}

#[tokio::test]
async fn test_enqueue_job_reconcile_skips_when_dispatcher_not_bound() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let pod = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "my-job-pod",
            "namespace": "default",
            "ownerReferences": [{
                "apiVersion": "batch/v1",
                "kind": "Job",
                "name": "my-job",
                "uid": "job-uid",
                "controller": true
            }]
        },
        "spec": {"nodeName": "test-node", "containers": [{"name": "w", "image": "busybox"}]},
        "status": {"phase": "Succeeded"}
    });
    // Dispatcher not bound — must not panic.
    pod_repo.enqueue_job_reconcile_for_pod(&pod).await;
}

#[test]
fn test_parse_deadline_timer_delay_secs_uses_creation_timestamp_when_start_time_missing() {
    let now = chrono::Utc::now();
    let pod = serde_json::json!({
        "metadata": {
            "namespace": "default",
            "name": "ads-pod",
            "creationTimestamp": (now - chrono::Duration::seconds(2)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        },
        "spec": {
            "activeDeadlineSeconds": 5
        },
        "status": {
            "phase": "Running"
        }
    });

    let parsed = parse_deadline_timer_delay_secs(&pod).expect("deadline timer metadata");
    assert_eq!(parsed.0, "default");
    assert_eq!(parsed.1, "ads-pod");
    assert!(
        parsed.2 <= 3 && parsed.2 >= 1,
        "remaining seconds should be around 3s, got {}",
        parsed.2
    );
}

#[test]
fn test_parse_deadline_timer_delay_secs_skips_terminal_pods() {
    let pod = serde_json::json!({
        "metadata": {
            "namespace": "default",
            "name": "done-pod",
            "creationTimestamp": "2026-04-28T00:00:00Z"
        },
        "spec": {
            "activeDeadlineSeconds": 5
        },
        "status": {
            "phase": "Succeeded"
        }
    });
    assert!(
        parse_deadline_timer_delay_secs(&pod).is_none(),
        "terminal pods should not schedule deadline timers"
    );
}
