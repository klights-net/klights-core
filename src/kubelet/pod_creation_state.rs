#[cfg(test)]
use crate::kubelet::pod_startup_error::{PodStartupErrorKind, PodStartupRetryPolicy};
#[cfg(test)]
use crate::kubelet::pod_status_logic::is_image_pull_error_msg;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;

pub type PodCreationTracker = Arc<Mutex<HashSet<String>>>;
pub type PodStartRetryTracker = Arc<Mutex<PodStartRetryState>>;

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PodStartSource {
    WatchAdded,
    WatchModified,
    Recovery,
}

pub fn pod_creation_key(namespace: &str, pod_name: &str) -> String {
    format!("{}/{}", namespace, pod_name)
}

#[cfg(test)]
pub fn parse_pod_creation_key(key: &str) -> Option<(&str, &str)> {
    let (namespace, pod_name) = key.split_once('/')?;
    if namespace.is_empty() || pod_name.is_empty() {
        return None;
    }
    Some((namespace, pod_name))
}

pub fn retry_backoff(attempts: u32) -> std::time::Duration {
    // 2s, 4s, 8s, 16s, 32s, 60s, 60s, ...
    // Base=2s ensures the watcher loop stays responsive even when
    // several pods are in tight retry cycles (image pull, CNI race).
    let secs = 2u64
        .saturating_mul(2u64.saturating_pow(attempts.saturating_sub(1)))
        .min(60);
    std::time::Duration::from_secs(secs.max(2))
}

#[derive(Clone, Debug, Default)]
pub struct PodStartRetryState {
    attempts: HashMap<String, u32>,
}

impl PodStartRetryState {
    pub fn new() -> Self {
        Self {
            attempts: HashMap::new(),
        }
    }

    #[cfg(test)]
    pub fn next_delay(&mut self, namespace: &str, pod_name: &str) -> std::time::Duration {
        let key = pod_creation_key(namespace, pod_name);
        let attempts = self.attempts.get(&key).copied().unwrap_or(0) + 1;
        self.attempts.insert(key, attempts);
        retry_backoff(attempts)
    }

    pub fn clear(&mut self, namespace: &str, pod_name: &str) {
        self.attempts.remove(&pod_creation_key(namespace, pod_name));
    }

    pub fn pending_key_pairs(&self) -> Vec<(String, String)> {
        self.attempts
            .iter()
            .filter_map(|(key, attempts)| {
                let (namespace, pod_name) = key.split_once('/')?;
                if namespace.is_empty() || pod_name.is_empty() {
                    return None;
                }
                Some((
                    namespace.to_string(),
                    format!("{} (attempts={})", pod_name, attempts),
                ))
            })
            .collect()
    }

    #[cfg(test)]
    fn attempts_for(&self, namespace: &str, pod_name: &str) -> Option<u32> {
        self.attempts
            .get(&pod_creation_key(namespace, pod_name))
            .copied()
    }
}

pub async fn clear_pod_creation_inflight(
    tracker: &PodCreationTracker,
    namespace: &str,
    pod_name: &str,
) {
    tracker
        .lock()
        .await
        .remove(&pod_creation_key(namespace, pod_name));
}

pub fn should_clear_pod_creation_inflight(pod: &Value) -> bool {
    if pod
        .pointer("/metadata/deletionTimestamp")
        .and_then(|ts| ts.as_str())
        .is_some()
    {
        return true;
    }

    let phase = pod
        .pointer("/status/phase")
        .and_then(|p| p.as_str())
        .unwrap_or("Pending");
    if phase != "Pending" {
        return true;
    }

    let has_completed_container_config_error = pod
        .pointer("/status/containerStatuses")
        .and_then(|v| v.as_array())
        .map(|statuses| {
            statuses.iter().any(|status| {
                matches!(
                    status
                        .pointer("/state/waiting/reason")
                        .and_then(|reason| reason.as_str()),
                    Some("CreateContainerConfigError")
                )
            })
        })
        .unwrap_or(false);
    if has_completed_container_config_error {
        return true;
    }

    false
}

#[cfg(test)]
pub fn classify_legacy_startup_error(err: &anyhow::Error) -> PodStartupErrorKind {
    if let Some(kind) = err.downcast_ref::<PodStartupErrorKind>() {
        return kind.clone();
    }

    let err_msg = format!("{:#}", err);
    let lower = err_msg.to_ascii_lowercase();
    if lower.contains("pod not found") {
        return PodStartupErrorKind::PodDisappeared;
    }
    // create_run bails out with this marker after publishing
    // CreateContainerConfigError container statuses to the pod. Treating
    // it as Skip prevents the upstream mark_pod_failed path from
    // overwriting that status with phase=Failed +
    // reason=CreateContainerError, which causes conformance
    // `WaitForPodContainerToFail` (matches Pending + reason ==
    // CreateContainerConfigError) to time out.
    if lower.contains("createcontainerconfigerror; pod cannot start") {
        return PodStartupErrorKind::ContainerConfigError;
    }
    if is_image_pull_error_msg(&err_msg) {
        return PodStartupErrorKind::ImagePull;
    }
    if lower.starts_with("init container ") && lower.contains(" failed with exit code ") {
        return PodStartupErrorKind::InitContainerFailed {
            exit_code: parse_init_exit_code(&lower).unwrap_or(1),
        };
    }
    let is_missing_projected_source = lower.contains("failed to process volumes")
        && lower.contains(" not found")
        && (lower.contains("configmap ") || lower.contains("secret "));
    if is_missing_projected_source {
        return PodStartupErrorKind::MissingProjectedSource;
    }
    if lower.contains("cni plugin not initialized") {
        return PodStartupErrorKind::CniUnavailable;
    }
    if lower.contains("pod network assignment wait timed out") {
        return PodStartupErrorKind::NetworkAssignmentTimedOut;
    }
    if lower.contains("connection refused")
        || lower.contains("deadline exceeded")
        || lower.contains("replication stream closed")
        || lower.contains("replication forward response timed out")
        || lower.contains("replication stream error")
        || lower.contains("forwarded command rejected")
        || lower.contains("already reserved")
        || lower.contains("already in use")
    {
        return PodStartupErrorKind::CriUnavailable;
    }
    PodStartupErrorKind::InvalidPodSpec
}

#[cfg(test)]
fn parse_init_exit_code(lower_error: &str) -> Option<i32> {
    let marker = " failed with exit code ";
    let (_, tail) = lower_error.split_once(marker)?;
    tail.split_whitespace().next()?.parse::<i32>().ok()
}

#[cfg(test)]
pub fn pod_startup_retry_policy_for_error(
    pod: &Value,
    err: &anyhow::Error,
) -> PodStartupRetryPolicy {
    let restart_policy = pod
        .pointer("/spec/restartPolicy")
        .and_then(|v| v.as_str())
        .unwrap_or("Always");
    classify_legacy_startup_error(err).retry_policy(restart_policy)
}

#[cfg(test)]
pub fn is_transient_pod_start_error(err: &anyhow::Error) -> bool {
    classify_legacy_startup_error(err).retry_policy("Always") == PodStartupRetryPolicy::Retry
}

#[cfg(test)]
pub fn should_fail_pod_for_start_error(pod: &Value, err: &anyhow::Error) -> bool {
    pod_startup_retry_policy_for_error(pod, err) == PodStartupRetryPolicy::FailPod
}

#[cfg(test)]
pub fn is_pod_disappeared_during_start_error(err: &anyhow::Error) -> bool {
    classify_legacy_startup_error(err) == PodStartupErrorKind::PodDisappeared
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_clear_pod_creation_inflight_after_pod_leaves_startup_state() {
        let running_pod = serde_json::json!({
            "status": {
                "phase": "Running",
                "podIP": "10.42.0.10",
            },
        });
        assert!(
            should_clear_pod_creation_inflight(&running_pod),
            "a pod that has an IP and non-Pending phase should release the in-flight guard"
        );

        let pending_pod = serde_json::json!({
            "status": {
                "phase": "Pending",
            },
        });
        assert!(
            !should_clear_pod_creation_inflight(&pending_pod),
            "a pod still in Pending without a podIP must keep the in-flight guard"
        );

        let pending_pod_with_ip = serde_json::json!({
            "status": {
                "phase": "Pending",
                "podIP": "10.42.0.10",
            },
        });
        assert!(
            !should_clear_pod_creation_inflight(&pending_pod_with_ip),
            "a pod with a sandbox IP but still Pending must keep the in-flight guard until the phase changes"
        );
    }

    #[test]
    fn pending_pod_with_config_error_has_completed_creation_attempt() {
        let pod = serde_json::json!({
            "metadata": {"namespace": "default", "name": "subpath-retry"},
            "status": {
                "phase": "Pending",
                "podIP": "10.43.0.5",
                "containerStatuses": [{
                    "name": "dapi-container",
                    "state": {
                        "waiting": {
                            "reason": "CreateContainerConfigError",
                            "message": "invalid subPath in container dapi-container"
                        }
                    }
                }]
            }
        });

        assert!(
            should_clear_pod_creation_inflight(&pod),
            "a completed Pending start attempt with a runtime config error must not block a later MODIFIED retry"
        );
    }

    #[test]
    fn test_parse_pod_creation_key_validates_shape() {
        assert_eq!(
            parse_pod_creation_key("default/nginx"),
            Some(("default", "nginx"))
        );
        assert_eq!(parse_pod_creation_key("default"), None);
        assert_eq!(parse_pod_creation_key("/nginx"), None);
        assert_eq!(parse_pod_creation_key("default/"), None);
    }

    #[test]
    fn test_is_transient_pod_start_error_matches_known_patterns() {
        assert!(is_transient_pod_start_error(&anyhow::anyhow!(
            "cni plugin not initialized"
        )));
        assert!(is_transient_pod_start_error(&anyhow::anyhow!(
            "failed: connection refused"
        )));
        assert!(is_transient_pod_start_error(&anyhow::anyhow!(
            "replication stream closed before forward send"
        )));
        assert!(is_transient_pod_start_error(&anyhow::anyhow!(
            "replication forward response timed out after 30s"
        )));
        assert!(!is_transient_pod_start_error(&anyhow::anyhow!(
            "invalid pod spec"
        )));
    }

    #[test]
    fn typed_startup_errors_define_retry_policy_without_string_matching() {
        use crate::kubelet::pod_startup_error::{PodStartupErrorKind, PodStartupRetryPolicy};

        assert_eq!(
            PodStartupErrorKind::NetworkAssignmentTimedOut.retry_policy("Always"),
            PodStartupRetryPolicy::Retry
        );
        assert_eq!(
            PodStartupErrorKind::InitContainerFailed { exit_code: 1 }.retry_policy("Never"),
            PodStartupRetryPolicy::FailPod
        );
        assert_eq!(
            PodStartupErrorKind::InvalidPodSpec.retry_policy("Always"),
            PodStartupRetryPolicy::FailPod
        );
    }

    #[test]
    fn typed_startup_error_context_takes_precedence_over_message_text() {
        let err = anyhow::Error::new(PodStartupErrorKind::NetworkAssignmentTimedOut)
            .context("message text does not contain a legacy network timeout pattern");

        assert_eq!(
            classify_legacy_startup_error(&err),
            PodStartupErrorKind::NetworkAssignmentTimedOut
        );
    }

    #[test]
    fn test_is_transient_pod_start_error_does_not_string_match_missing_pod_network_assignment() {
        assert!(
            !is_transient_pod_start_error(&anyhow::anyhow!(
                "failed to resolve pod IP for ns/pod (sandbox sandbox-1): no pod_network for sandbox sandbox-1 or pod ns/pod uid uid-1 after 25 retries"
            )),
            "pod network assignment retry policy must be driven by typed startup errors, not brittle text matching"
        );
    }

    #[test]
    fn test_is_transient_pod_start_error_retries_image_pull_failures() {
        assert!(is_transient_pod_start_error(&anyhow::anyhow!(
            "Failed to pull image docker.io/coredns/coredns:1.11.1: CRI pull_image failed: 429 Too Many Requests"
        )));
    }

    #[test]
    fn test_is_transient_pod_start_error_retries_missing_projected_sources() {
        assert!(is_transient_pod_start_error(&anyhow::anyhow!(
            "Failed to process volumes: ConfigMap job-1223/kube-root-ca.crt not found"
        )));
        assert!(is_transient_pod_start_error(&anyhow::anyhow!(
            "Failed to process volumes: Secret default/api-token not found"
        )));
    }

    #[test]
    fn test_is_transient_pod_start_error_does_not_whole_pod_retry_init_container_nonzero_exit() {
        assert!(!is_transient_pod_start_error(&anyhow::anyhow!(
            "Init container init1 failed with exit code 1"
        )));
    }

    #[test]
    fn restartable_init_failure_is_not_whole_pod_start_retry() {
        let pod = serde_json::json!({
            "spec": {
                "restartPolicy": "Always",
                "initContainers": [{"name": "init1", "image": "busybox"}],
                "containers": [{"name": "run1", "image": "pause"}]
            }
        });

        assert_ne!(
            pod_startup_retry_policy_for_error(
                &pod,
                &anyhow::anyhow!("Init container init1 failed with exit code 1")
            ),
            PodStartupRetryPolicy::Retry,
            "restartable init-container failures must be retried by pod manager inside the existing pod, not by the whole-pod startup retry path"
        );
    }

    #[test]
    fn test_should_fail_pod_for_start_error_with_restart_policy_never_init_failure() {
        let pod = serde_json::json!({
            "spec": {
                "restartPolicy": "Never",
                "initContainers": [{"name": "init1", "image": "busybox"}],
                "containers": [{"name": "run1", "image": "busybox"}]
            }
        });
        assert!(should_fail_pod_for_start_error(
            &pod,
            &anyhow::anyhow!("Init container init1 failed with exit code 1")
        ));
    }

    #[test]
    fn test_should_fail_pod_for_start_error_keeps_restartable_init_failures_retryable() {
        for restart_policy in ["Always", "OnFailure"] {
            let pod = serde_json::json!({
                "spec": {
                    "restartPolicy": restart_policy,
                    "initContainers": [{"name": "init1", "image": "busybox"}],
                    "containers": [{"name": "run1", "image": "busybox"}]
                }
            });
            assert!(
                !should_fail_pod_for_start_error(
                    &pod,
                    &anyhow::anyhow!("Init container init1 failed with exit code 1")
                ),
                "{restart_policy} init failure should stay retryable"
            );
        }
    }

    #[test]
    fn test_is_pod_disappeared_during_start_error() {
        assert!(is_pod_disappeared_during_start_error(&anyhow::anyhow!(
            "update pod status failed: Pod not found"
        )));
        assert!(!is_pod_disappeared_during_start_error(&anyhow::anyhow!(
            "failed to pull image"
        )));
    }

    #[test]
    fn test_retry_backoff_is_exponential_and_capped() {
        assert_eq!(retry_backoff(1), std::time::Duration::from_secs(2));
        assert_eq!(retry_backoff(2), std::time::Duration::from_secs(4));
        assert_eq!(retry_backoff(3), std::time::Duration::from_secs(8));
        assert_eq!(retry_backoff(10), std::time::Duration::from_secs(60));
    }

    #[test]
    fn pod_start_retry_state_tracks_attempts_without_due_scan() {
        let mut state = PodStartRetryState::new();

        assert_eq!(
            state.next_delay("default", "retry-pod"),
            std::time::Duration::from_secs(2)
        );
        assert_eq!(
            state.next_delay("default", "retry-pod"),
            std::time::Duration::from_secs(4)
        );
        assert_eq!(state.attempts_for("default", "retry-pod"), Some(2));

        state.clear("default", "retry-pod");

        assert_eq!(state.attempts_for("default", "retry-pod"), None);
        assert_eq!(
            state.next_delay("default", "retry-pod"),
            std::time::Duration::from_secs(2)
        );
    }
}
