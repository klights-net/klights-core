//! Init-container status JSON builders for the pod runtime service.
//!
//! Extracted from the `service.rs` hub to keep it under its size cap. These
//! are pure functions over the pod spec and CRI state — they synthesize the
//! init-container `containerStatuses` entries the kubelet emits during start,
//! retry, and failure paths.

use crate::kubelet::pod_runtime::status_helpers::json_number_as_i64;

/// CRI-derived stop record for a finished init container.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct InitContainerStop {
    pub exit_code: i32,
    pub finished_at: i64,
}

/// Read a finished init-container stop record from a CRI status response.
/// Returns `None` unless the container is in the `ContainerExited` state.
pub(super) fn init_container_stop_from_status(
    status: &k8s_cri::v1::ContainerStatusResponse,
) -> Option<InitContainerStop> {
    let status = status.status.as_ref()?;
    if status.state != k8s_cri::v1::ContainerState::ContainerExited as i32 {
        return None;
    }
    Some(InitContainerStop {
        exit_code: status.exit_code,
        finished_at: unix_seconds_from_cri_ns(status.finished_at),
    })
}

fn unix_seconds_from_cri_ns(ns: i64) -> i64 {
    if ns > 0 {
        ns / 1_000_000_000
    } else {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64
    }
}

/// Truncate the init-container status list up to (and including) any prior
/// entry for `container_name`, then append the fresh `completed` status so
/// only the latest completion for that name is retained.
pub(super) fn record_completed_init_container_status(
    statuses: &mut Vec<serde_json::Value>,
    container_name: &str,
    completed: serde_json::Value,
) {
    if let Some(pos) = statuses
        .iter()
        .position(|status| status.get("name").and_then(|v| v.as_str()) == Some(container_name))
    {
        statuses.truncate(pos);
    }
    statuses.push(completed);
}

/// True when the init-container status list already records a successful
/// (exit code 0) termination for `container_name`.
pub(super) fn init_container_completed(
    statuses: &[serde_json::Value],
    container_name: &str,
) -> bool {
    statuses.iter().any(|status| {
        status.get("name").and_then(|v| v.as_str()) == Some(container_name)
            && status
                .pointer("/state/terminated/exitCode")
                .and_then(json_number_as_i64)
                == Some(0)
    })
}

/// Build a completed init-container `containerStatuses` entry.
pub(super) fn build_completed_init_container_status(
    name: &str,
    image: &str,
    container_id: &str,
    exit_code: i32,
    started_at: i64,
    finished_at: i64,
) -> serde_json::Value {
    let timestamp_from_seconds = |seconds: i64| {
        chrono::DateTime::from_timestamp(seconds, 0)
            .map(|dt| dt.to_rfc3339())
            .unwrap_or_else(crate::utils::k8s_timestamp)
    };
    serde_json::json!({
        "name": name,
        "state": {
            "terminated": {
                "exitCode": exit_code,
                "reason": if exit_code == 0 { "Completed" } else { "Error" },
                "startedAt": timestamp_from_seconds(started_at),
                "finishedAt": timestamp_from_seconds(finished_at),
            }
        },
        "ready": exit_code == 0,
        "restartCount": 0,
        "image": image,
        "imageID": image,
        "containerID": format!("containerd://{}", container_id),
    })
}

fn init_failure_timestamp_from_seconds(seconds: i64) -> String {
    chrono::DateTime::from_timestamp(seconds, 0)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_else(crate::utils::k8s_timestamp)
}

/// Build the `terminated` state object for a failed init container.
pub(super) fn build_init_failure_terminated_state(
    exit_code: i32,
    started_at: i64,
    finished_at: i64,
) -> serde_json::Value {
    serde_json::json!({
        "exitCode": exit_code,
        "reason": if exit_code == 0 { "Completed" } else { "Error" },
        "startedAt": init_failure_timestamp_from_seconds(started_at),
        "finishedAt": init_failure_timestamp_from_seconds(finished_at),
    })
}

/// Build `containerStatuses` for every app container when pod start fails
/// before any container runs — each entry waits with `CreateContainerError`.
pub(super) fn build_pod_start_failure_app_statuses(
    pod: &serde_json::Value,
    message: &str,
) -> Vec<serde_json::Value> {
    pod.pointer("/spec/containers")
        .and_then(|value| value.as_array())
        .map(|containers| {
            containers
                .iter()
                .map(|container| {
                    let name = container
                        .get("name")
                        .and_then(|value| value.as_str())
                        .unwrap_or("unknown");
                    let image = container
                        .get("image")
                        .and_then(|value| value.as_str())
                        .unwrap_or("unknown");
                    serde_json::json!({
                        "name": name,
                        "image": image,
                        "imageID": "",
                        "ready": false,
                        "started": false,
                        "restartCount": 0,
                        "state": {
                            "waiting": {
                                "reason": REASON_CREATE_CONTAINER_ERROR,
                                "message": message
                            }
                        }
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Rebuild init-container statuses around a retried failed init container,
/// preserving prior terminated state in `lastState` and bumping the restart
/// count. Containers after the failed one wait with `PodInitializing`.
pub(super) fn build_retrying_init_container_statuses(
    pod: &serde_json::Value,
    failed_name: &str,
    existing_statuses: &[serde_json::Value],
    fallback_terminated: serde_json::Value,
) -> Vec<serde_json::Value> {
    let Some(init_containers) = pod
        .pointer("/spec/initContainers")
        .and_then(|value| value.as_array())
    else {
        return Vec::new();
    };

    let mut statuses = Vec::new();
    let mut saw_failed = false;
    for container in init_containers {
        let name = container
            .get("name")
            .and_then(|value| value.as_str())
            .unwrap_or("unknown");
        let image = container
            .get("image")
            .and_then(|value| value.as_str())
            .unwrap_or("unknown");
        let existing = existing_statuses
            .iter()
            .find(|status| status.get("name").and_then(|value| value.as_str()) == Some(name));

        if name == failed_name {
            saw_failed = true;
            let restart_count = existing
                .and_then(|status| status.get("restartCount"))
                .and_then(|value| value.as_i64())
                .unwrap_or(0)
                + 1;
            let terminated = existing
                .and_then(|status| status.pointer("/state/terminated"))
                .cloned()
                .or_else(|| {
                    existing
                        .and_then(|status| status.pointer("/lastState/terminated"))
                        .cloned()
                })
                .unwrap_or_else(|| fallback_terminated.clone());
            statuses.push(serde_json::json!({
                "name": name,
                "image": image,
                "imageID": existing
                    .and_then(|status| status.get("imageID"))
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!("")),
                "ready": false,
                "started": false,
                "restartCount": restart_count,
                "state": {
                    "waiting": {
                        "reason": REASON_POD_INITIALIZING
                    }
                },
                "lastState": {
                    "terminated": terminated
                }
            }));
        } else if saw_failed {
            statuses.push(serde_json::json!({
                "name": name,
                "image": image,
                "imageID": "",
                "ready": false,
                "started": false,
                "restartCount": 0,
                "state": {
                    "waiting": {
                        "reason": REASON_POD_INITIALIZING
                    }
                }
            }));
        } else if let Some(existing) = existing {
            statuses.push(existing.clone());
        } else {
            statuses.push(serde_json::json!({
                "name": name,
                "image": image,
                "imageID": "",
                "ready": true,
                "restartCount": 0,
                "state": {
                    "terminated": {
                        "exitCode": 0,
                        "reason": "Completed",
                        "startedAt": crate::utils::k8s_timestamp(),
                        "finishedAt": crate::utils::k8s_timestamp()
                    }
                }
            }));
        }
    }
    statuses
}

/// Build init-container statuses for a terminal init failure: the failed
/// container gets a terminated state, prior init containers stay completed.
pub(super) fn build_failed_init_container_statuses(
    pod: &serde_json::Value,
    failed_name: &str,
    exit_code: i32,
    started_at: i64,
    finished_at: i64,
) -> Vec<serde_json::Value> {
    let Some(init_containers) = pod
        .pointer("/spec/initContainers")
        .and_then(|value| value.as_array())
    else {
        return Vec::new();
    };

    let mut statuses = Vec::new();
    for container in init_containers {
        let name = container
            .get("name")
            .and_then(|value| value.as_str())
            .unwrap_or("unknown");
        let image = container
            .get("image")
            .and_then(|value| value.as_str())
            .unwrap_or("unknown");
        if name == failed_name {
            statuses.push(serde_json::json!({
                "name": name,
                "image": image,
                "imageID": "",
                "ready": false,
                "restartCount": 0,
                "state": {
                    "terminated": build_init_failure_terminated_state(
                        exit_code,
                        started_at,
                        finished_at,
                    )
                }
            }));
            break;
        }
        statuses.push(serde_json::json!({
            "name": name,
            "image": image,
            "imageID": "",
            "ready": true,
            "restartCount": 0,
            "state": {
                "terminated": {
                    "exitCode": 0,
                    "reason": "Completed",
                    "startedAt": crate::utils::k8s_timestamp(),
                    "finishedAt": crate::utils::k8s_timestamp()
                }
            }
        }));
    }
    statuses
}

const REASON_CREATE_CONTAINER_ERROR: &str = "CreateContainerError";
const REASON_POD_INITIALIZING: &str = "PodInitializing";
