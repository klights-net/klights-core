use crate::pod_sandbox_config::build_sandbox_config_with_dns_policy;
use crate::pod_status_builders::build_container_statuses;
use crate::pod_status_builders::build_creation_error_statuses;
use crate::pod_status_logic::{ContainerInfo, compute_pod_phase, should_restart};

#[test]
fn test_pod_phase_succeeded() {
    // All containers exit with code 0 and restart policy is Never or OnFailure → Succeeded
    let containers = vec![
        (
            "app".to_string(),
            ContainerInfo {
                state: 2, // Exited
                exit_code: 0,
                finished_at: 2000000000,
                started_at: 1000000000,
                image: "app:latest".to_string(),
                image_ref: "docker.io/library/app:latest".to_string(),
                container_id: "aaa".to_string(),
                termination_message: String::new(),
            },
        ),
        (
            "sidecar".to_string(),
            ContainerInfo {
                state: 2, // Exited
                exit_code: 0,
                finished_at: 2100000000,
                started_at: 1000000000,
                image: "sidecar:latest".to_string(),
                image_ref: "docker.io/library/sidecar:latest".to_string(),
                container_id: "bbb".to_string(),
                termination_message: String::new(),
            },
        ),
    ];

    assert_eq!(
        compute_pod_phase(&containers, "Never"),
        "Succeeded",
        "Never: all exit 0 → Succeeded"
    );
    assert_eq!(
        compute_pod_phase(&containers, "OnFailure"),
        "Succeeded",
        "OnFailure: all exit 0 → Succeeded"
    );
}

#[test]
fn test_pod_phase_failed() {
    // Any container exits with non-zero and restart policy is Never → Failed
    let containers = vec![(
        "app".to_string(),
        ContainerInfo {
            state: 2, // Exited
            exit_code: 1,
            finished_at: 2000000000,
            started_at: 1000000000,
            image: "app:latest".to_string(),
            image_ref: "docker.io/library/app:latest".to_string(),
            container_id: "aaa".to_string(),
            termination_message: String::new(),
        },
    )];

    assert_eq!(
        compute_pod_phase(&containers, "Never"),
        "Failed",
        "Never: exit 1 → Failed"
    );

    // Multiple containers, one failed
    let containers_mixed = vec![
        (
            "app".to_string(),
            ContainerInfo {
                state: 2, // Exited
                exit_code: 0,
                finished_at: 2000000000,
                started_at: 1000000000,
                image: "app:latest".to_string(),
                image_ref: "docker.io/library/app:latest".to_string(),
                container_id: "aaa".to_string(),
                termination_message: String::new(),
            },
        ),
        (
            "sidecar".to_string(),
            ContainerInfo {
                state: 2,       // Exited
                exit_code: 137, // e.g., SIGKILL
                finished_at: 2100000000,
                started_at: 1000000000,
                image: "sidecar:latest".to_string(),
                image_ref: "docker.io/library/sidecar:latest".to_string(),
                container_id: "bbb".to_string(),
                termination_message: String::new(),
            },
        ),
    ];

    assert_eq!(
        compute_pod_phase(&containers_mixed, "Never"),
        "Failed",
        "Never: any non-zero exit → Failed"
    );
}

#[test]
fn test_sandbox_reservation_error_detection_failed_precondition() {
    // The error message from containerd when a sandbox name is already reserved
    let err_msg = "status: FailedPrecondition, message: \"failed to reserve sandbox name\"";
    assert!(
        err_msg.contains("failed to reserve sandbox name")
            || err_msg.contains("FailedPrecondition"),
        "Should detect sandbox name reservation error"
    );
}

#[test]
fn test_sandbox_reservation_error_detection_other_error() {
    // Other errors should NOT be treated as sandbox reservation conflicts
    let err_msg = "status: Internal, message: \"failed to create sandbox\"";
    assert!(
        !(err_msg.contains("failed to reserve sandbox name")
            || err_msg.contains("FailedPrecondition")),
        "Should NOT match non-reservation errors"
    );
}

#[test]
fn test_sandbox_reservation_error_detection_containerd_format() {
    // Actual containerd error format
    let err_msg = "rpc error: code = FailedPrecondition desc = failed to reserve sandbox name \"test-pod_default_abc-123_0\": name is reserved";
    assert!(
        err_msg.contains("failed to reserve sandbox name")
            || err_msg.contains("FailedPrecondition"),
        "Should detect actual containerd error format"
    );
}

// ========================
// S1.2: Pod restart policy tests
// ========================

#[test]
fn test_restart_policy_always_restarts_on_zero_exit() {
    // Always policy: restart even if exit code is 0
    assert!(
        should_restart("Always", 0),
        "Always policy should restart on exit code 0"
    );
}

#[test]
fn test_restart_policy_always_restarts_on_nonzero_exit() {
    // Always policy: restart on any non-zero exit code
    assert!(
        should_restart("Always", 1),
        "Always policy should restart on exit code 1"
    );
    assert!(
        should_restart("Always", 137),
        "Always policy should restart on exit code 137"
    );
}

#[test]
fn test_restart_policy_onfailure_no_restart_on_zero() {
    // OnFailure policy: do NOT restart if exit code is 0
    assert!(
        !should_restart("OnFailure", 0),
        "OnFailure policy should NOT restart on exit code 0"
    );
}

#[test]
fn test_restart_policy_onfailure_restarts_on_nonzero() {
    // OnFailure policy: restart only on non-zero exit code
    assert!(
        should_restart("OnFailure", 1),
        "OnFailure policy should restart on exit code 1"
    );
    assert!(
        should_restart("OnFailure", 137),
        "OnFailure policy should restart on exit code 137"
    );
}

#[test]
fn test_restart_policy_never_no_restart_on_zero() {
    // Never policy: never restart, even on exit code 0
    assert!(
        !should_restart("Never", 0),
        "Never policy should NOT restart on exit code 0"
    );
}

#[test]
fn test_restart_policy_never_no_restart_on_nonzero() {
    // Never policy: never restart, even on non-zero exit
    assert!(
        !should_restart("Never", 1),
        "Never policy should NOT restart on exit code 1"
    );
    assert!(
        !should_restart("Never", 137),
        "Never policy should NOT restart on exit code 137"
    );
}

#[test]
fn test_restart_policy_unknown_defaults_to_no_restart() {
    // Unknown policy: default to no restart (safe fallback)
    assert!(
        !should_restart("InvalidPolicy", 0),
        "Unknown policy should NOT restart on exit code 0"
    );
    assert!(
        !should_restart("InvalidPolicy", 1),
        "Unknown policy should NOT restart on exit code 1"
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
    let statuses = build_container_statuses(
        &containers,
        &restart_counts,
        &ready_containers_empty,
        chrono::DateTime::UNIX_EPOCH,
    );
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
    let statuses = build_container_statuses(
        &containers,
        &restart_counts,
        &ready_containers,
        chrono::DateTime::UNIX_EPOCH,
    );
    let status = &statuses[0];
    let ready = status.get("ready").and_then(|r| r.as_bool()).unwrap();

    // FIX VERIFICATION: ready should be true when readiness probe succeeded
    assert!(
        ready,
        "Container should be ready when readiness probe succeeded"
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
