use serde_json::Value;

/// K8s standard waiting reasons (see kubernetes/kubernetes
/// pkg/kubelet/images/types.go and pkg/kubelet/container/sync_result.go).
pub const REASON_ERR_IMAGE_PULL: &str = "ErrImagePull";
pub const REASON_IMAGE_PULL_BACK_OFF: &str = "ImagePullBackOff";
pub const REASON_CREATE_CONTAINER_ERROR: &str = "CreateContainerError";
pub const REASON_POD_INITIALIZING: &str = "PodInitializing";

/// Information about a container's CRI state
#[derive(Default)]
#[cfg(test)]
pub struct ContainerInfo {
    pub state: i32, // CRI ContainerState enum value (0=Created, 1=Running, 2=Exited, 3=Unknown)
    pub exit_code: i32,
    pub finished_at: i64, // nanoseconds
    pub started_at: i64,  // nanoseconds
    pub image: String,
    pub image_ref: String,
    pub container_id: String,
    pub termination_message: String, // content of terminationMessagePath file after exit, max 4096 bytes
}

/// Tracks restart backoff state for a container
#[derive(Debug, Clone)]
#[cfg(test)]
pub struct ContainerBackoffState {
    pub next_restart_time: i64, // Unix timestamp (seconds)
}

pub fn is_image_pull_error_msg(error_msg: &str) -> bool {
    let lower = error_msg.to_ascii_lowercase();
    lower.contains("pull image") || lower.contains("pull_image") || lower.contains("failed to pull")
}

/// Classify an error chain into the appropriate K8s waiting reason.
/// Heuristic: anyhow with_context strings include "pull image" / "pull_image"
/// for image-pull failures (set in pod_manager.rs). Init container failures
/// use PodInitializing so main containers reflect the correct state.
pub fn classify_failure_reason(error_msg: &str) -> &'static str {
    let lower = error_msg.to_ascii_lowercase();
    if lower.starts_with("init container ") && lower.contains(" failed with exit code ") {
        // Main containers are waiting because init containers haven't completed.
        REASON_POD_INITIALIZING
    } else if is_image_pull_error_msg(error_msg) {
        REASON_ERR_IMAGE_PULL
    } else {
        REASON_CREATE_CONTAINER_ERROR
    }
}

/// Extract init container name and exit code from an init container failure message.
/// Parses "Init container {name} failed with exit code {N}".
pub fn parse_init_container_failure(error_msg: &str) -> Option<(&str, i32)> {
    let msg = error_msg.strip_prefix("Init container ")?;
    let failed_pos = msg.find(" failed with exit code ")?;
    let name = &msg[..failed_pos];
    let exit_str = &msg[failed_pos + " failed with exit code ".len()..];
    let exit_code_end = exit_str
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(exit_str.len());
    let exit_code = exit_str[..exit_code_end].parse().ok()?;
    Some((name, exit_code))
}

/// Compute the Initialized condition for a pod.
///
/// Returns `(initialized, incomplete_message)`:
/// - `initialized = true` when there are no init containers or all completed.
/// - `initialized = false` with a message listing incomplete init container names otherwise.
pub fn compute_initialized_condition(
    pod: &Value,
    init_container_statuses: &[Value],
) -> (bool, Option<String>) {
    let spec_init_names: Vec<&str> = pod
        .pointer("/spec/initContainers")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|c| c.get("name").and_then(|n| n.as_str()))
                .collect()
        })
        .unwrap_or_default();

    if spec_init_names.is_empty() {
        return (true, None);
    }

    let completed_set: std::collections::HashSet<&str> = init_container_statuses
        .iter()
        .filter(|s| init_container_status_completed(s))
        .filter_map(|s| s.get("name").and_then(|n| n.as_str()))
        .collect();

    let incomplete: Vec<&str> = spec_init_names
        .into_iter()
        .filter(|name| !completed_set.contains(name))
        .collect();

    if incomplete.is_empty() {
        (true, None)
    } else {
        let msg = format!(
            "containers with incomplete status: [{}]",
            incomplete.join(" ")
        );
        (false, Some(msg))
    }
}

fn init_container_status_completed(status: &Value) -> bool {
    status
        .get("ready")
        .and_then(|ready| ready.as_bool())
        .unwrap_or(false)
        || status
            .pointer("/state/terminated/exitCode")
            .is_some_and(|exit_code| exit_code.as_i64() == Some(0) || exit_code.as_u64() == Some(0))
}

/// Preserve lastTransitionTime when updating pod conditions.
/// Returns the existing lastTransitionTime if a condition with the same type
/// and status already exists (preventing unnecessary watch MODIFIED events),
/// otherwise returns the provided new timestamp.
///
/// This is the core logic that prevents spurious watch events when condition
/// status hasn't actually changed between status updates.
pub fn get_condition_last_transition_time(
    existing_conditions: &[serde_json::Value],
    condition_type: &str,
    condition_status: &str,
    new_timestamp: &str,
) -> String {
    existing_conditions
        .iter()
        .find(|c| {
            c.get("type").and_then(|t| t.as_str()) == Some(condition_type)
                && c.get("status").and_then(|s| s.as_str()) == Some(condition_status)
        })
        .and_then(|c| c.get("lastTransitionTime"))
        .and_then(|t| t.as_str())
        .map(String::from)
        .unwrap_or_else(|| new_timestamp.to_string())
}

/// Determine pod phase from container states.
/// All containers exited with code 0 -> Succeeded
/// Any container exited with non-zero code -> Failed
/// Any container still running -> Running
/// All containers created but not started -> Pending
#[cfg(test)]
pub fn compute_pod_phase(
    container_states: &[(String, ContainerInfo)],
    restart_policy: &str,
) -> String {
    let restart_policy = effective_restart_policy(restart_policy);

    if container_states.is_empty() {
        // No container state observed yet: pod startup is still in progress.
        // Do not classify this as terminal for restartPolicy=Never/OnFailure.
        return "Pending".to_string();
    }

    let mut any_running = false;
    let mut any_exited_nonzero = false;
    let mut all_exited_zero = true;

    for (_, container) in container_states {
        match container.state {
            1 => {
                // Running
                any_running = true;
                all_exited_zero = false;
            }
            2 => {
                // Exited
                if container.exit_code != 0 {
                    any_exited_nonzero = true;
                    all_exited_zero = false;
                }
            }
            _ => {
                // Created, Unknown, or other
                all_exited_zero = false;
            }
        }
    }

    // If any container is still running, pod is Running
    if any_running {
        return "Running".to_string();
    }

    // If restart policy is Always or OnFailure and containers exited, phase stays Running (will restart)
    if restart_policy == "Always" {
        return "Running".to_string();
    }

    if restart_policy == "OnFailure" && any_exited_nonzero {
        return "Running".to_string();
    }

    // All containers exited with code 0 and restart policy is Never or OnFailure -> Succeeded
    if all_exited_zero && (restart_policy == "Never" || restart_policy == "OnFailure") {
        return "Succeeded".to_string();
    }

    // Any container exited with non-zero code and restart policy is Never -> Failed
    if any_exited_nonzero && restart_policy == "Never" {
        return "Failed".to_string();
    }

    "Pending".to_string()
}

/// Decide whether to restart a container based on restart policy and exit code
#[cfg(test)]
pub fn should_restart(restart_policy: &str, exit_code: i32) -> bool {
    match effective_restart_policy(restart_policy) {
        "Always" => true,
        "OnFailure" => exit_code != 0,
        "Never" => false,
        _ => false,
    }
}

#[cfg(test)]
pub fn effective_restart_policy(restart_policy: &str) -> &str {
    if restart_policy.is_empty() {
        "Always"
    } else {
        restart_policy
    }
}

/// Calculate exponential backoff delay in seconds for container restarts.
/// Spec: 10s, 20s, 40s, 80s, 160s, capped at 300s (5 min)
#[cfg(test)]
pub fn calculate_backoff_delay(restart_count: i32) -> u64 {
    if restart_count == 0 {
        return 0; // First restart is immediate
    }
    // Cap exponent to prevent overflow: 2^9 = 512, 10 * 512 = 5120 > 300
    let exponent = ((restart_count - 1) as u32).min(9);
    let delay = 10 * 2_u64.pow(exponent);
    delay.min(300) // Cap at 5 minutes
}

/// Extract set of containers that should be marked ready.
/// A container is ready if:
/// 1. It has no readiness probe (ready as soon as running), OR
/// 2. It has a readiness probe AND the existing containerStatuses show it as ready
///    (set by the probe manager after the probe succeeds)
#[cfg(test)]
pub fn extract_ready_containers_from_pod_condition(
    pod: &Value,
) -> std::collections::HashSet<String> {
    let mut ready_containers = std::collections::HashSet::new();

    let containers = pod.pointer("/spec/containers").and_then(|c| c.as_array());
    let existing_statuses = pod
        .pointer("/status/containerStatuses")
        .and_then(|cs| cs.as_array());

    if let Some(containers) = containers {
        for container in containers {
            let name = match container.get("name").and_then(|n| n.as_str()) {
                Some(n) => n,
                None => continue,
            };

            let has_readiness_probe = container.get("readinessProbe").is_some();

            if has_readiness_probe {
                // Container has a readiness probe — only ready if probe manager
                // has already marked it ready in existing containerStatuses
                let already_ready = existing_statuses
                    .and_then(|statuses| {
                        statuses
                            .iter()
                            .find(|cs| cs.get("name").and_then(|n| n.as_str()) == Some(name))
                    })
                    .and_then(|cs| cs.get("ready"))
                    .and_then(|r| r.as_bool())
                    .unwrap_or(false);

                if already_ready {
                    ready_containers.insert(name.to_string());
                }
            } else {
                // No readiness probe — ready as soon as running
                ready_containers.insert(name.to_string());
            }
        }
    }

    ready_containers
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_container(name: &str, state: i32, exit_code: i32) -> (String, ContainerInfo) {
        (
            name.to_string(),
            ContainerInfo {
                state,
                exit_code,
                finished_at: 0,
                started_at: 0,
                image: String::new(),
                image_ref: String::new(),
                container_id: String::new(),
                termination_message: String::new(),
            },
        )
    }

    #[test]
    fn parse_init_container_failure_extracts_name_and_exit_code() {
        let s = "Init container init2 failed with exit code 1";
        let result = super::parse_init_container_failure(s);
        assert_eq!(result, Some(("init2", 1)));
    }

    #[test]
    fn parse_init_container_failure_accepts_anyhow_context_chain() {
        let s =
            "Init container init1 failed with exit code 1: init container failed with exit code 1";
        let result = super::parse_init_container_failure(s);
        assert_eq!(result, Some(("init1", 1)));
    }

    #[test]
    fn parse_init_container_failure_returns_none_for_non_init() {
        let s = "failed to start container: exec: sh: not found";
        assert!(super::parse_init_container_failure(s).is_none());
    }

    #[test]
    fn compute_initialized_condition_no_init_containers_is_true() {
        use serde_json::json;
        let pod = json!({"spec": {"containers": [{"name": "app"}]}});
        let (initialized, msg) = super::compute_initialized_condition(&pod, &[]);
        assert!(initialized);
        assert!(msg.is_none());
    }

    #[test]
    fn compute_initialized_condition_all_completed_is_true() {
        use serde_json::json;
        let pod = json!({
            "spec": {
                "initContainers": [
                    {"name": "init1"},
                    {"name": "init2"}
                ]
            }
        });
        let statuses = vec![
            json!({"name": "init1", "ready": true}),
            json!({"name": "init2", "ready": true}),
        ];
        let (initialized, msg) = super::compute_initialized_condition(&pod, &statuses);
        assert!(initialized);
        assert!(msg.is_none());
    }

    #[test]
    fn compute_initialized_condition_terminated_zero_init_is_true_even_when_ready_false() {
        use serde_json::json;
        let pod = json!({
            "spec": {
                "initContainers": [
                    {"name": "init1"},
                    {"name": "init2"}
                ]
            }
        });
        let statuses = vec![
            json!({
                "name": "init1",
                "ready": false,
                "state": {"terminated": {"exitCode": 0, "reason": "Completed"}}
            }),
            json!({
                "name": "init2",
                "ready": false,
                "state": {"terminated": {"exitCode": 0, "reason": "Completed"}}
            }),
        ];
        let (initialized, msg) = super::compute_initialized_condition(&pod, &statuses);
        assert!(initialized);
        assert!(msg.is_none());
    }

    #[test]
    fn compute_initialized_condition_failed_init_is_false_with_message() {
        use serde_json::json;
        // Pod with init1 (success) and init2 (failed — not in statuses as ready)
        let pod = json!({
            "spec": {
                "initContainers": [
                    {"name": "init1"},
                    {"name": "init2"}
                ]
            }
        });
        // init1 completed, init2 failed (ready: false)
        let statuses = vec![
            json!({"name": "init1", "ready": true}),
            json!({"name": "init2", "ready": false}),
        ];
        let (initialized, msg) = super::compute_initialized_condition(&pod, &statuses);
        assert!(!initialized, "should be not initialized when init2 failed");
        assert_eq!(
            msg.as_deref(),
            Some("containers with incomplete status: [init2]"),
            "message must list incomplete init containers"
        );
    }

    #[test]
    fn compute_initialized_condition_never_ran_init_is_false() {
        use serde_json::json;
        // Pod with init1 and init2; only init1 ran (init2 never started)
        let pod = json!({
            "spec": {
                "initContainers": [
                    {"name": "init1"},
                    {"name": "init2"}
                ]
            }
        });
        let statuses = vec![json!({"name": "init1", "ready": true})];
        let (initialized, msg) = super::compute_initialized_condition(&pod, &statuses);
        assert!(!initialized);
        assert_eq!(
            msg.as_deref(),
            Some("containers with incomplete status: [init2]")
        );
    }

    #[test]
    fn compute_initialized_condition_multiple_incomplete_uses_k8s_message_shape() {
        use serde_json::json;
        let pod = json!({
            "spec": {
                "initContainers": [
                    {"name": "init1"},
                    {"name": "init2"}
                ]
            }
        });
        let statuses = vec![
            json!({"name": "init1", "ready": false}),
            json!({"name": "init2", "ready": false}),
        ];
        let (initialized, msg) = super::compute_initialized_condition(&pod, &statuses);
        assert!(!initialized);
        assert_eq!(
            msg.as_deref(),
            Some("containers with incomplete status: [init1 init2]")
        );
    }

    #[test]
    fn test_calculate_backoff_delay_first_restart() {
        let delay = calculate_backoff_delay(0);
        assert_eq!(delay, 0, "First restart should be immediate (0s delay)");
    }

    #[test]
    fn test_calculate_backoff_delay_exponential() {
        assert_eq!(calculate_backoff_delay(1), 10, "Second restart: 10s");
        assert_eq!(calculate_backoff_delay(2), 20, "Third restart: 20s");
        assert_eq!(calculate_backoff_delay(3), 40, "Fourth restart: 40s");
        assert_eq!(calculate_backoff_delay(4), 80, "Fifth restart: 80s");
        assert_eq!(calculate_backoff_delay(5), 160, "Sixth restart: 160s");
    }

    #[test]
    fn test_calculate_backoff_delay_capped_at_300s() {
        assert_eq!(calculate_backoff_delay(6), 300, "Cap at 300s");
        assert_eq!(calculate_backoff_delay(10), 300, "Stay capped at 300s");
        assert_eq!(calculate_backoff_delay(100), 300, "Stay capped at 300s");
    }

    #[test]
    fn test_compute_pod_phase_all_running() {
        let containers = vec![(
            "nginx".to_string(),
            ContainerInfo {
                state: 1, // Running
                exit_code: 0,
                finished_at: 0,
                started_at: 1000000000,
                image: "nginx:latest".to_string(),
                image_ref: "docker.io/library/nginx:latest".to_string(),
                container_id: "abc123".to_string(),
                termination_message: String::new(),
            },
        )];

        let phase = compute_pod_phase(&containers, "Always");
        assert_eq!(phase, "Running");
    }

    #[test]
    fn test_compute_pod_phase_all_exited_zero_never() {
        let containers = vec![(
            "nginx".to_string(),
            ContainerInfo {
                state: 2, // Exited
                exit_code: 0,
                finished_at: 2000000000,
                started_at: 1000000000,
                image: "nginx:latest".to_string(),
                image_ref: "docker.io/library/nginx:latest".to_string(),
                container_id: "abc123".to_string(),
                termination_message: String::new(),
            },
        )];

        let phase = compute_pod_phase(&containers, "Never");
        assert_eq!(phase, "Succeeded");
    }

    #[test]
    fn test_compute_pod_phase_all_exited_zero_always() {
        let containers = vec![(
            "nginx".to_string(),
            ContainerInfo {
                state: 2, // Exited
                exit_code: 0,
                finished_at: 2000000000,
                started_at: 1000000000,
                image: "nginx:latest".to_string(),
                image_ref: "docker.io/library/nginx:latest".to_string(),
                container_id: "abc123".to_string(),
                termination_message: String::new(),
            },
        )];

        let phase = compute_pod_phase(&containers, "Always");
        assert_eq!(phase, "Running"); // Will be restarted
    }

    #[test]
    fn test_compute_pod_phase_one_exited_nonzero_never() {
        let containers = vec![(
            "nginx".to_string(),
            ContainerInfo {
                state: 2, // Exited
                exit_code: 1,
                finished_at: 2000000000,
                started_at: 1000000000,
                image: "nginx:latest".to_string(),
                image_ref: "docker.io/library/nginx:latest".to_string(),
                container_id: "abc123".to_string(),
                termination_message: String::new(),
            },
        )];

        let phase = compute_pod_phase(&containers, "Never");
        assert_eq!(phase, "Failed");
    }

    #[test]
    fn test_compute_pod_phase_one_exited_nonzero_on_failure() {
        let containers = vec![(
            "nginx".to_string(),
            ContainerInfo {
                state: 2, // Exited
                exit_code: 1,
                finished_at: 2000000000,
                started_at: 1000000000,
                image: "nginx:latest".to_string(),
                image_ref: "docker.io/library/nginx:latest".to_string(),
                container_id: "abc123".to_string(),
                termination_message: String::new(),
            },
        )];

        let phase = compute_pod_phase(&containers, "OnFailure");
        assert_eq!(phase, "Running"); // Will be restarted
    }

    #[test]
    fn test_should_restart_always_exit_zero() {
        assert!(should_restart("Always", 0));
    }

    #[test]
    fn test_should_restart_always_exit_nonzero() {
        assert!(should_restart("Always", 1));
        assert!(should_restart("Always", 255));
    }

    #[test]
    fn test_should_restart_never_exit_zero() {
        assert!(!(should_restart("Never", 0)));
    }

    #[test]
    fn test_should_restart_never_exit_nonzero() {
        assert!(!(should_restart("Never", 1)));
        assert!(!(should_restart("Never", 255)));
    }

    #[test]
    fn test_should_restart_on_failure_exit_zero() {
        assert!(!(should_restart("OnFailure", 0)));
    }

    #[test]
    fn test_should_restart_on_failure_exit_nonzero() {
        assert!(should_restart("OnFailure", 1));
        assert!(should_restart("OnFailure", 255));
    }

    #[test]
    fn test_should_restart_empty_policy_defaults_to_always() {
        assert!(should_restart("", 0));
        assert!(should_restart("", 1));
    }

    #[test]
    fn test_compute_pod_phase_empty_restart_policy_defaults_to_always() {
        let containers = vec![(
            "nginx".to_string(),
            ContainerInfo {
                state: 2, // Exited
                exit_code: 1,
                finished_at: 2000000000,
                started_at: 1000000000,
                image: "nginx:latest".to_string(),
                image_ref: "docker.io/library/nginx:latest".to_string(),
                container_id: "abc123".to_string(),
                termination_message: String::new(),
            },
        )];

        let phase = compute_pod_phase(&containers, "");
        assert_eq!(phase, "Running");
    }

    #[test]
    fn test_compute_pod_phase_multiple_containers_mixed_running_and_exited() {
        // One container running, one exited with error — pod should be Running
        let containers = vec![
            (
                "web".to_string(),
                ContainerInfo {
                    state: 1, // Running
                    exit_code: 0,
                    finished_at: 0,
                    started_at: 1000000000,
                    image: "nginx:latest".to_string(),
                    image_ref: "docker.io/library/nginx:latest".to_string(),
                    container_id: "aaa".to_string(),
                    termination_message: String::new(),
                },
            ),
            (
                "sidecar".to_string(),
                ContainerInfo {
                    state: 2, // Exited
                    exit_code: 1,
                    finished_at: 2000000000,
                    started_at: 1000000000,
                    image: "busybox:latest".to_string(),
                    image_ref: "docker.io/library/busybox:latest".to_string(),
                    container_id: "bbb".to_string(),
                    termination_message: String::new(),
                },
            ),
        ];

        assert_eq!(compute_pod_phase(&containers, "Never"), "Running");
        assert_eq!(compute_pod_phase(&containers, "Always"), "Running");
        assert_eq!(compute_pod_phase(&containers, "OnFailure"), "Running");
    }

    #[test]
    fn test_compute_pod_phase_on_failure_all_exit_zero() {
        // OnFailure with all containers exit 0 → Succeeded (not restarted)
        let containers = vec![(
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
        )];

        assert_eq!(
            compute_pod_phase(&containers, "OnFailure"),
            "Succeeded",
            "OnFailure: exit 0 → Succeeded"
        );
    }

    #[test]
    fn test_compute_pod_phase_empty_containers() {
        // Edge case: no container state observed yet.
        let containers: Vec<(String, ContainerInfo)> = vec![];

        assert_eq!(
            compute_pod_phase(&containers, "Always"),
            "Pending",
            "Empty + Always → Pending"
        );
        assert_eq!(
            compute_pod_phase(&containers, "OnFailure"),
            "Pending",
            "Empty + OnFailure → Pending"
        );
        assert_eq!(
            compute_pod_phase(&containers, "Never"),
            "Pending",
            "Empty + Never → Pending"
        );
    }

    #[test]
    fn test_compute_pod_phase_all_running_returns_running() {
        let containers = vec![make_container("app", 1, 0), make_container("sidecar", 1, 0)];
        assert_eq!(compute_pod_phase(&containers, "Always"), "Running");
        assert_eq!(compute_pod_phase(&containers, "Never"), "Running");
        assert_eq!(compute_pod_phase(&containers, "OnFailure"), "Running");
    }

    #[test]
    fn test_compute_pod_phase_all_exited_zero_never_returns_succeeded() {
        let containers = vec![make_container("worker", 2, 0), make_container("init", 2, 0)];
        assert_eq!(compute_pod_phase(&containers, "Never"), "Succeeded");
    }

    #[test]
    fn test_compute_pod_phase_all_exited_zero_onfailure_returns_succeeded() {
        let containers = vec![make_container("job", 2, 0)];
        assert_eq!(compute_pod_phase(&containers, "OnFailure"), "Succeeded");
    }

    #[test]
    fn test_compute_pod_phase_all_exited_zero_always_returns_running() {
        // restartPolicy: Always means containers will be restarted, so pod stays Running
        let containers = vec![make_container("app", 2, 0)];
        assert_eq!(compute_pod_phase(&containers, "Always"), "Running");
    }

    #[test]
    fn test_compute_pod_phase_exited_nonzero_never_returns_failed() {
        let containers = vec![make_container("app", 2, 1)];
        assert_eq!(compute_pod_phase(&containers, "Never"), "Failed");
    }

    #[test]
    fn test_compute_pod_phase_exited_nonzero_onfailure_returns_running() {
        // restartPolicy: OnFailure means failed containers will restart, pod stays Running
        let containers = vec![make_container("worker", 2, 137)];
        assert_eq!(compute_pod_phase(&containers, "OnFailure"), "Running");
    }

    #[test]
    fn test_compute_pod_phase_mix_running_and_exited() {
        // One container running, one exited — pod is Running
        let containers = vec![make_container("app", 1, 0), make_container("sidecar", 2, 0)];
        assert_eq!(compute_pod_phase(&containers, "Always"), "Running");
        assert_eq!(compute_pod_phase(&containers, "Never"), "Running");
    }

    #[test]
    fn test_compute_pod_phase_mix_exited_zero_and_nonzero_never() {
        // One succeeded, one failed — with Never policy → Failed
        let containers = vec![make_container("app", 2, 0), make_container("sidecar", 2, 1)];
        assert_eq!(compute_pod_phase(&containers, "Never"), "Failed");
    }

    #[test]
    fn test_compute_pod_phase_no_containers() {
        // Edge case: no containers observed yet -> startup still pending.
        let containers: Vec<(String, ContainerInfo)> = vec![];
        assert_eq!(compute_pod_phase(&containers, "Always"), "Pending");
        assert_eq!(compute_pod_phase(&containers, "Never"), "Pending");
        assert_eq!(compute_pod_phase(&containers, "OnFailure"), "Pending");
    }

    #[test]
    fn test_compute_pod_phase_created_not_started_returns_pending() {
        // State 0 = Created (not yet started)
        let containers = vec![make_container("app", 0, 0)];
        // With Always, returns Running (will restart)
        assert_eq!(compute_pod_phase(&containers, "Always"), "Running");
        // With Never, returns Pending (not yet started, not exited)
        assert_eq!(compute_pod_phase(&containers, "Never"), "Pending");
    }

    #[test]
    fn test_compute_pod_phase_unknown_state() {
        // State 3 = Unknown
        let containers = vec![make_container("app", 3, 0)];
        assert_eq!(compute_pod_phase(&containers, "Never"), "Pending");
    }

    #[test]
    fn test_compute_pod_phase_multiple_exited_nonzero_never() {
        // All failed with different exit codes
        let containers = vec![
            make_container("app", 2, 1),
            make_container("sidecar", 2, 137),
            make_container("logger", 2, 2),
        ];
        assert_eq!(compute_pod_phase(&containers, "Never"), "Failed");
    }

    #[test]
    fn test_compute_pod_phase_running_overrides_exited_nonzero() {
        // If any container is running, phase is Running regardless of others
        let containers = vec![make_container("app", 1, 0), make_container("crashed", 2, 1)];
        assert_eq!(compute_pod_phase(&containers, "Never"), "Running");
    }

    #[test]
    fn test_extract_ready_containers_no_readiness_probe_always_ready() {
        let pod = serde_json::json!({
            "spec": {
                "containers": [
                    {"name": "app", "image": "nginx"}
                ]
            },
            "status": {
                "containerStatuses": [
                    {"name": "app", "ready": false}
                ]
            }
        });
        let ready = extract_ready_containers_from_pod_condition(&pod);
        assert!(
            ready.contains("app"),
            "Container without readiness probe should be in ready set"
        );
    }

    #[test]
    fn test_extract_ready_containers_with_readiness_probe_not_yet_ready() {
        let pod = serde_json::json!({
            "spec": {
                "containers": [
                    {"name": "app", "image": "nginx", "readinessProbe": {"httpGet": {"path": "/", "port": 80}}}
                ]
            },
            "status": {
                "containerStatuses": [
                    {"name": "app", "ready": false}
                ]
            }
        });
        let ready = extract_ready_containers_from_pod_condition(&pod);
        assert!(
            !ready.contains("app"),
            "Container with readiness probe not yet passed should NOT be in ready set"
        );
    }

    #[test]
    fn test_extract_ready_containers_with_readiness_probe_succeeded() {
        let pod = serde_json::json!({
            "spec": {
                "containers": [
                    {"name": "app", "image": "nginx", "readinessProbe": {"httpGet": {"path": "/", "port": 80}}}
                ]
            },
            "status": {
                "containerStatuses": [
                    {"name": "app", "ready": true}
                ]
            }
        });
        let ready = extract_ready_containers_from_pod_condition(&pod);
        assert!(
            ready.contains("app"),
            "Container with readiness probe that succeeded should be in ready set"
        );
    }

    #[test]
    fn test_extract_ready_containers_mixed_probes() {
        let pod = serde_json::json!({
            "spec": {
                "containers": [
                    {"name": "web", "image": "nginx", "readinessProbe": {"httpGet": {"path": "/", "port": 80}}},
                    {"name": "sidecar", "image": "busybox"}
                ]
            },
            "status": {
                "containerStatuses": [
                    {"name": "web", "ready": false},
                    {"name": "sidecar", "ready": false}
                ]
            }
        });
        let ready = extract_ready_containers_from_pod_condition(&pod);
        assert!(
            !ready.contains("web"),
            "Container with failing readiness probe should NOT be ready"
        );
        assert!(
            ready.contains("sidecar"),
            "Container without readiness probe should be ready"
        );
    }
}
