use crate::kubelet::pod_creation_state::PodStartSource;
use serde_json::Value;

#[derive(Debug)]
pub enum PodRuntimeState {
    NotStarted,
    StartingNoContainers,
    StartingWithContainers { has_running_or_created: bool },
    Running,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartupDecision {
    StartFresh,
    RollbackThenStart,
    Skip,
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn modified_pending_pod_with_ip_and_empty_sandbox_restarts_partial_startup() {
        let pod = json!({
            "metadata": {"namespace": "default", "name": "subpath-retry"},
            "spec": {"nodeName": "test-node"},
            "status": {"phase": "Pending", "podIP": "10.42.0.25"}
        });

        assert_eq!(
            decide_startup_action(
                &pod,
                &PodRuntimeState::StartingNoContainers,
                PodStartSource::WatchModified,
                "test-node",
            ),
            StartupDecision::RollbackThenStart
        );
    }

    #[test]
    fn modified_pending_pod_waiting_for_retry_does_not_bypass_retry_backoff() {
        let pod = json!({
            "metadata": {"namespace": "default", "name": "missing-config"},
            "spec": {"nodeName": "test-node"},
            "status": {
                "phase": "Pending",
                "podIP": "10.42.0.25",
                "containerStatuses": [{
                    "name": "c",
                    "state": {"waiting": {
                        "reason": "CreateContainerError",
                        "message": "Failed to process volumes: ConfigMap default/kube-root-ca.crt not found"
                    }}
                }]
            }
        });

        assert_eq!(
            decide_startup_action(
                &pod,
                &PodRuntimeState::StartingNoContainers,
                PodStartSource::WatchModified,
                "test-node",
            ),
            StartupDecision::Skip,
            "status-only MODIFIED events after a retryable start failure must not immediately retry"
        );
    }

    #[test]
    fn modified_pending_pod_with_ip_and_live_containers_is_not_rolled_back() {
        let pod = json!({
            "metadata": {"namespace": "default", "name": "starting-pod"},
            "spec": {"nodeName": "test-node"},
            "status": {"phase": "Pending", "podIP": "10.42.0.25"}
        });

        assert_eq!(
            decide_startup_action(
                &pod,
                &PodRuntimeState::StartingWithContainers {
                    has_running_or_created: true,
                },
                PodStartSource::WatchModified,
                "test-node",
            ),
            StartupDecision::Skip
        );
    }

    #[test]
    fn modified_pending_pod_without_ip_and_live_containers_is_not_rolled_back() {
        let pod = json!({
            "metadata": {"namespace": "default", "name": "starting-pod"},
            "spec": {"nodeName": "test-node"},
            "status": {"phase": "Pending"}
        });

        assert_eq!(
            decide_startup_action(
                &pod,
                &PodRuntimeState::StartingWithContainers {
                    has_running_or_created: true,
                },
                PodStartSource::WatchModified,
                "test-node",
            ),
            StartupDecision::Skip
        );
    }

    #[test]
    fn modified_running_pod_with_exited_containers_waits_for_lifecycle_reconcile() {
        let pod = json!({
            "metadata": {"namespace": "default", "name": "short-job-pod"},
            "spec": {"nodeName": "test-node", "restartPolicy": "Never"},
            "status": {"phase": "Running", "podIP": "10.42.0.25"}
        });

        assert_eq!(
            decide_startup_action(
                &pod,
                &PodRuntimeState::StartingWithContainers {
                    has_running_or_created: false,
                },
                PodStartSource::WatchModified,
                "test-node",
            ),
            StartupDecision::Skip,
            "a status MODIFIED echo must not rollback/restart short-lived containers that exited before the CRI stopped event is processed"
        );
    }

    #[test]
    fn modified_pending_pod_not_started_does_not_start_from_status_echo() {
        let pod = json!({
            "metadata": {"namespace": "default", "name": "image-pull-retry"},
            "spec": {"nodeName": "test-node"},
            "status": {"phase": "Pending", "containerStatuses": []}
        });

        assert_eq!(
            decide_startup_action(
                &pod,
                &PodRuntimeState::NotStarted,
                PodStartSource::WatchModified,
                "test-node",
            ),
            StartupDecision::Skip,
            "initial/status-only MODIFIED events must not bypass the retry queue"
        );
    }

    #[test]
    fn recovery_restarts_running_pod_when_runtime_is_missing() {
        let pod = json!({
            "metadata": {"namespace": "kube-system", "name": "coredns"},
            "spec": {"nodeName": "test-node"},
            "status": {"phase": "Running", "podIP": "10.43.0.2"}
        });

        assert_eq!(
            decide_startup_action(
                &pod,
                &PodRuntimeState::NotStarted,
                PodStartSource::Recovery,
                "test-node",
            ),
            StartupDecision::StartFresh,
            "on kubelet/containerd restart, persisted Running status must not mask missing CRI runtime"
        );
    }

    #[test]
    fn pending_pod_without_node_assignment_is_not_started() {
        let pod = json!({
            "metadata": {"namespace": "default", "name": "restricted-pod"},
            "status": {"phase": "Pending"}
        });

        assert_eq!(
            decide_startup_action(
                &pod,
                &PodRuntimeState::NotStarted,
                PodStartSource::WatchAdded,
                "test-node",
            ),
            StartupDecision::Skip
        );
    }

    #[test]
    fn pending_pod_assigned_to_another_node_is_not_started() {
        let pod = json!({
            "metadata": {"namespace": "default", "name": "remote-pod"},
            "spec": {"nodeName": "other-node"},
            "status": {"phase": "Pending"}
        });

        assert_eq!(
            decide_startup_action(
                &pod,
                &PodRuntimeState::NotStarted,
                PodStartSource::WatchAdded,
                "test-node",
            ),
            StartupDecision::Skip
        );
    }
}

pub fn decide_startup_action(
    pod: &Value,
    runtime_state: &PodRuntimeState,
    source: PodStartSource,
    node_name: &str,
) -> StartupDecision {
    if pod
        .pointer("/metadata/deletionTimestamp")
        .and_then(|ts| ts.as_str())
        .is_some()
    {
        return StartupDecision::Skip;
    }

    if pod.pointer("/spec/nodeName").and_then(|n| n.as_str()) != Some(node_name) {
        return StartupDecision::Skip;
    }

    let phase = pod
        .pointer("/status/phase")
        .and_then(|p| p.as_str())
        .unwrap_or("Pending");
    let has_pod_ip = pod
        .pointer("/status/podIP")
        .and_then(|ip| ip.as_str())
        .map(|ip| !ip.is_empty())
        .unwrap_or(false);

    if phase == "Running" && !has_pod_ip {
        return StartupDecision::Skip;
    }
    if source == PodStartSource::WatchAdded && has_pod_ip {
        return StartupDecision::Skip;
    }
    if source == PodStartSource::WatchModified
        && phase == "Pending"
        && has_retry_waiting_status(pod)
    {
        return StartupDecision::Skip;
    }

    match runtime_state {
        PodRuntimeState::NotStarted => {
            if (phase == "Pending" && !has_pod_ip && source != PodStartSource::WatchModified)
                || (source == PodStartSource::Recovery && phase == "Running" && has_pod_ip)
            {
                StartupDecision::StartFresh
            } else {
                StartupDecision::Skip
            }
        }
        PodRuntimeState::StartingNoContainers => StartupDecision::RollbackThenStart,
        PodRuntimeState::StartingWithContainers {
            has_running_or_created,
        } => {
            if (phase == "Pending" && *has_running_or_created)
                || (source == PodStartSource::WatchModified && phase == "Running")
            {
                StartupDecision::Skip
            } else {
                StartupDecision::RollbackThenStart
            }
        }
        PodRuntimeState::Running => StartupDecision::Skip,
    }
}

fn has_retry_waiting_status(pod: &Value) -> bool {
    pod.pointer("/status/containerStatuses")
        .and_then(|v| v.as_array())
        .is_some_and(|statuses| {
            statuses.iter().any(|status| {
                let reason = status
                    .pointer("/state/waiting/reason")
                    .and_then(|v| v.as_str());
                let message = status
                    .pointer("/state/waiting/message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_ascii_lowercase();

                matches!(reason, Some("ErrImagePull") | Some("ImagePullBackOff"))
                    || (reason == Some("CreateContainerError")
                        && message.contains("failed to process volumes")
                        && message.contains(" not found")
                        && (message.contains("configmap ") || message.contains("secret ")))
            })
        })
}
