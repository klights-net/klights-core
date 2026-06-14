#[cfg(test)]
use crate::kubelet::pod_status_logic::{ContainerBackoffState, ContainerInfo};
use crate::kubelet::pod_status_logic::{
    REASON_ERR_IMAGE_PULL, REASON_IMAGE_PULL_BACK_OFF, REASON_POD_INITIALIZING,
    classify_failure_reason,
};
use serde_json::Value;

#[cfg(test)]
pub fn cri_timestamp_from_ns(ns: i64) -> String {
    if ns <= 0 {
        return crate::utils::k8s_timestamp();
    }
    let secs = ns / 1_000_000_000;
    let sub_ns = (ns % 1_000_000_000) as u32;
    // Match k8s_timestamp() microsecond precision so status comparisons
    // detect no-change and avoid infinite write-loops.
    chrono::DateTime::from_timestamp(secs, sub_ns)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S.%fZ").to_string())
        .unwrap_or_else(crate::utils::k8s_timestamp)
}

/// Build status object for completed init container
#[cfg(test)]
pub fn build_init_container_status(
    name: &str,
    image: &str,
    container_id: &str,
    exit_code: i32,
    started_at: i64,
    finished_at: i64,
) -> Value {
    // Format timestamps as RFC3339
    let started_at_str = chrono::DateTime::from_timestamp(started_at, 0)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_default();
    let finished_at_str = chrono::DateTime::from_timestamp(finished_at, 0)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_default();

    serde_json::json!({
        "name": name,
        "state": {
            "terminated": {
                "exitCode": exit_code,
                "reason": if exit_code == 0 { "Completed" } else { "Error" },
                "startedAt": started_at_str,
                "finishedAt": finished_at_str,
            }
        },
        "ready": true,
        "restartCount": 0,
        "image": image,
        "imageID": format!("docker.io/library/{}", image),
        "containerID": format!("containerd://{}", container_id),
    })
}

#[cfg(test)]
pub struct EphemeralContainerStatusFixture<'a> {
    pub container_name: &'a str,
    pub container_id: Option<&'a str>,
    pub state: i32,
    pub started_at_ns: i64,
    pub finished_at_ns: i64,
    pub exit_code: i32,
    pub image: &'a str,
    pub image_ref: &'a str,
}

#[cfg(test)]
pub fn build_ephemeral_container_status(fixture: EphemeralContainerStatusFixture<'_>) -> Value {
    let EphemeralContainerStatusFixture {
        container_name,
        container_id,
        state,
        started_at_ns,
        finished_at_ns,
        exit_code,
        image,
        image_ref,
    } = fixture;
    let state_obj = match state {
        1 => serde_json::json!({
            "running": {
                "startedAt": cri_timestamp_from_ns(started_at_ns)
            }
        }),
        2 => serde_json::json!({
            "terminated": {
                "exitCode": exit_code,
                "reason": if exit_code == 0 { "Completed" } else { "Error" },
                "startedAt": cri_timestamp_from_ns(started_at_ns),
                "finishedAt": cri_timestamp_from_ns(finished_at_ns),
            }
        }),
        _ => serde_json::json!({
            "waiting": {
                "reason": "ContainerCreating"
            }
        }),
    };

    let mut status = serde_json::json!({
        "name": container_name,
        "state": state_obj,
        "ready": state == 1,
        "started": state == 1 || state == 2,
        "restartCount": 0,
        "image": image,
        "imageID": image_ref,
    });
    if let Some(id) = container_id {
        status["containerID"] = serde_json::json!(format!("containerd://{}", id));
    }
    status
}

#[cfg(test)]
pub fn build_container_statuses(
    containers: &[(String, ContainerInfo)],
    restart_counts: &std::collections::HashMap<String, i32>,
    ready_containers: &std::collections::HashSet<String>,
) -> Vec<Value> {
    // Call backoff-aware version with empty backoff state for backward compatibility
    let empty_backoff = std::collections::HashMap::new();
    build_container_statuses_with_backoff(
        containers,
        restart_counts,
        &empty_backoff,
        ready_containers,
    )
}

/// Build container statuses with backoff state awareness for CrashLoopBackOff detection
#[cfg(test)]
pub fn build_container_statuses_with_backoff(
    containers: &[(String, ContainerInfo)],
    restart_counts: &std::collections::HashMap<String, i32>,
    backoff_state: &std::collections::HashMap<String, ContainerBackoffState>,
    ready_containers: &std::collections::HashSet<String>,
) -> Vec<Value> {
    let now = chrono::Utc::now().timestamp();

    containers.iter().map(|(name, info)| {
        let restart_count = restart_counts.get(name).copied().unwrap_or(0);
        let backoff = backoff_state.get(name);

        // Check if container is in backoff period
        let in_backoff = if let Some(state) = backoff {
            now < state.next_restart_time
        } else {
            false
        };

        let state = if in_backoff && info.state == 2 {
            // Container is exited and waiting for backoff delay
            let backoff_seconds = backoff.map(|s| s.next_restart_time - now).unwrap_or(0);
            serde_json::json!({
                "waiting": {
                    "reason": "CrashLoopBackOff",
                    "message": format!("back-off {}s restarting failed container", backoff_seconds)
                }
            })
        } else {
            match info.state {
                1 => {
                    // Running
                    serde_json::json!({
                        "running": {
                            "startedAt": cri_timestamp_from_ns(info.started_at)
                        }
                    })
                }
                2 => {
                    // Exited/Terminated
                    let reason = if info.exit_code == 0 { "Completed" } else { "Error" };
                    let mut terminated = serde_json::json!({
                        "exitCode": info.exit_code,
                        "finishedAt": cri_timestamp_from_ns(info.finished_at),
                        "startedAt": cri_timestamp_from_ns(info.started_at),
                        "reason": reason
                    });
                    if !info.termination_message.is_empty() {
                        terminated["message"] = serde_json::json!(info.termination_message);
                    }
                    serde_json::json!({ "terminated": terminated })
            }
            _ => {
                // Created or Unknown -> Waiting
                serde_json::json!({
                    "waiting": {
                        "reason": "ContainerCreating"
                    }
                })
            }
        }
        };

        serde_json::json!({
            "name": name,
            "containerID": format!("containerd://{}", info.container_id),
            "image": info.image,
            "imageID": info.image_ref,
            "state": state,
            // Container is ready only if:
            // 1. It's running (state == 1), AND
            // 2. It's in the ready_containers set (meaning readiness probe succeeded or no probe)
            "ready": info.state == 1 && ready_containers.contains(name),
            // started is true if the container has been started at least once
            // (state 1=Running or 2=Exited means it was started)
            "started": info.state == 1 || info.state == 2,
            "restartCount": restart_count,
        })
    }).collect()
}

/// Build container statuses for a pod that failed during creation.
/// Each container gets a `waiting` state with the error reason/message.
/// For init container failures, main containers use PodInitializing (no message).
pub fn build_creation_error_statuses(pod: &Value, error_msg: &str) -> Vec<Value> {
    let containers = pod.pointer("/spec/containers").and_then(|c| c.as_array());
    let reason = classify_failure_reason(error_msg);
    let init_containers = pod
        .pointer("/spec/initContainers")
        .and_then(|c| c.as_array())
        .map_or(0usize, |c| c.len());

    let init_complete = if init_containers > 0 {
        let init_statuses = pod
            .pointer("/status/initContainerStatuses")
            .and_then(|c| c.as_array())
            .map_or(&[] as &[Value], |v| v) as &[Value];
        init_statuses.len() >= init_containers
            && init_statuses
                .iter()
                .filter_map(|s| {
                    s.get("ready")
                        .and_then(|ready| ready.as_bool())
                        .map(|ready| {
                            if ready {
                                s.pointer("/state/terminated").is_some()
                                    || s.pointer("/lastState/terminated").is_some()
                            } else {
                                false
                            }
                        })
                })
                .count()
                == init_containers
    } else {
        true
    };

    let is_pod_initializing =
        reason == REASON_POD_INITIALIZING || (!init_complete && init_containers > 0);

    match containers {
        Some(containers) => containers
            .iter()
            .map(|c| {
                let name = c.get("name").and_then(|n| n.as_str()).unwrap_or("unknown");
                let image = c.get("image").and_then(|i| i.as_str()).unwrap_or("unknown");
                // PodInitializing: main containers haven't started — no message field.
                let waiting = if is_pod_initializing {
                    serde_json::json!({"reason": REASON_POD_INITIALIZING})
                } else {
                    serde_json::json!({"reason": reason, "message": error_msg})
                };
                serde_json::json!({
                    "name": name,
                    "image": image,
                    "imageID": "",
                    "ready": false,
                    "started": false,
                    "restartCount": 0,
                    "state": {"waiting": waiting}
                })
            })
            .collect(),
        None => vec![],
    }
}

pub fn build_pod_initializing_app_statuses(pod: &Value) -> Vec<Value> {
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
                                "reason": REASON_POD_INITIALIZING
                            }
                        }
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn build_pod_initializing_init_statuses(pod: &Value) -> Vec<Value> {
    let Some(init_containers) = pod
        .pointer("/spec/initContainers")
        .and_then(|value| value.as_array())
    else {
        return Vec::new();
    };
    let existing_statuses = pod
        .pointer("/status/initContainerStatuses")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();

    init_containers
        .iter()
        .map(|container| {
            let name = container
                .get("name")
                .and_then(|value| value.as_str())
                .unwrap_or("unknown");
            if let Some(existing) = existing_statuses
                .iter()
                .find(|status| status.get("name").and_then(|value| value.as_str()) == Some(name))
            {
                return existing.clone();
            }
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
                        "reason": REASON_POD_INITIALIZING
                    }
                }
            })
        })
        .collect()
}

pub fn build_initial_pending_status(pod: &Value) -> Value {
    let has_init_containers = pod
        .pointer("/spec/initContainers")
        .and_then(|value| value.as_array())
        .is_some_and(|containers| !containers.is_empty());
    if !has_init_containers {
        return serde_json::json!({"phase": "Pending"});
    }

    serde_json::json!({
        "phase": "Pending",
        "containerStatuses": build_pod_initializing_app_statuses(pod),
        "initContainerStatuses": build_pod_initializing_init_statuses(pod),
    })
}

fn normalize_image_name_for_status(image: &str) -> String {
    let normalized = if !image.contains('/') {
        format!("docker.io/library/{}", image)
    } else if !image.contains('.') && image.split('/').count() == 2 {
        format!("docker.io/{}", image)
    } else {
        image.to_string()
    };

    if !normalized.contains(':') && !normalized.contains('@') {
        format!("{}:latest", normalized)
    } else {
        normalized
    }
}

pub fn build_image_pull_error_statuses(pod: &Value, error_msg: &str) -> Vec<Value> {
    let Some(containers) = pod.pointer("/spec/containers").and_then(|c| c.as_array()) else {
        return vec![];
    };
    let existing_statuses = pod
        .pointer("/status/containerStatuses")
        .and_then(|s| s.as_array());

    let failed_index = containers.iter().position(|container| {
        let image = container
            .get("image")
            .and_then(|i| i.as_str())
            .unwrap_or("");
        let normalized = normalize_image_name_for_status(image);
        (!image.is_empty() && error_msg.contains(image))
            || (!normalized.is_empty() && error_msg.contains(&normalized))
    });

    containers
        .iter()
        .enumerate()
        .map(|(idx, container)| {
            let name = container
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or("unknown");
            let image = container
                .get("image")
                .and_then(|i| i.as_str())
                .unwrap_or("unknown");
            let existing = existing_statuses.and_then(|statuses| {
                statuses
                    .iter()
                    .find(|status| status.get("name").and_then(|v| v.as_str()) == Some(name))
            });

            if failed_index == Some(idx) || failed_index.is_none() {
                let previous_reason = existing
                    .and_then(|status| status.pointer("/state/waiting/reason"))
                    .and_then(|reason| reason.as_str());
                let reason = if matches!(
                    previous_reason,
                    Some(REASON_ERR_IMAGE_PULL) | Some(REASON_IMAGE_PULL_BACK_OFF)
                ) {
                    REASON_IMAGE_PULL_BACK_OFF
                } else {
                    REASON_ERR_IMAGE_PULL
                };
                let message = if reason == REASON_IMAGE_PULL_BACK_OFF {
                    format!(
                        "Back-off pulling image \"{}\": {}",
                        normalize_image_name_for_status(image),
                        error_msg
                    )
                } else {
                    error_msg.to_string()
                };
                let restart_count = existing
                    .and_then(|status| status.get("restartCount"))
                    .and_then(|count| count.as_i64())
                    .unwrap_or(0);
                serde_json::json!({
                    "name": name,
                    "image": image,
                    "imageID": "",
                    "ready": false,
                    "started": false,
                    "restartCount": restart_count,
                    "state": {
                        "waiting": {
                            "reason": reason,
                            "message": message
                        }
                    }
                })
            } else {
                existing.cloned().unwrap_or_else(|| {
                    serde_json::json!({
                        "name": name,
                        "image": image,
                        "imageID": "",
                        "ready": false,
                        "started": false,
                        "restartCount": 0,
                        "state": {
                            "waiting": {
                                "reason": "ContainerCreating"
                            }
                        }
                    })
                })
            }
        })
        .collect()
}

/// Build init container statuses when an init container failed.
/// The failed container shows terminated.reason=Error; prior ones show Completed.
#[cfg(test)]
pub fn build_failed_init_container_statuses(
    pod: &Value,
    failed_name: &str,
    exit_code: i32,
) -> Vec<Value> {
    let init_containers = pod
        .pointer("/spec/initContainers")
        .and_then(|v| v.as_array());
    let Some(init_containers) = init_containers else {
        return vec![];
    };

    let now = crate::utils::k8s_timestamp();
    let mut statuses = Vec::new();
    for c in init_containers {
        let name = c.get("name").and_then(|n| n.as_str()).unwrap_or("unknown");
        let image = c.get("image").and_then(|i| i.as_str()).unwrap_or("unknown");
        if name == failed_name {
            statuses.push(serde_json::json!({
                "name": name,
                "image": image,
                "imageID": "",
                "ready": false,
                "restartCount": 0,
                "state": {
                    "terminated": {
                        "exitCode": exit_code,
                        "reason": "Error",
                        "startedAt": now,
                        "finishedAt": now
                    }
                }
            }));
            break; // Stop — containers after the failed one never ran
        } else {
            // Prior init containers completed successfully
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
                        "startedAt": now,
                        "finishedAt": now
                    }
                }
            }));
        }
    }
    statuses
}

pub fn build_retrying_init_container_statuses(
    pod: &Value,
    failed_name: &str,
    exit_code: i32,
    existing_statuses: &[Value],
) -> Vec<Value> {
    let init_containers = pod
        .pointer("/spec/initContainers")
        .and_then(|v| v.as_array());
    let Some(init_containers) = init_containers else {
        return vec![];
    };

    let now = crate::utils::k8s_timestamp();
    let mut statuses = Vec::new();
    let mut saw_failed = false;
    for c in init_containers {
        let name = c.get("name").and_then(|n| n.as_str()).unwrap_or("unknown");
        let image = c.get("image").and_then(|i| i.as_str()).unwrap_or("unknown");
        let existing = existing_statuses
            .iter()
            .find(|s| s.get("name").and_then(|n| n.as_str()) == Some(name));

        if name == failed_name {
            saw_failed = true;
            let restart_count = existing
                .and_then(|s| s.get("restartCount"))
                .and_then(|v| v.as_i64())
                .unwrap_or(0)
                + 1;
            let terminated = existing
                .and_then(|s| s.pointer("/state/terminated"))
                .cloned()
                .or_else(|| {
                    existing
                        .and_then(|s| s.pointer("/lastState/terminated"))
                        .cloned()
                })
                .unwrap_or_else(|| {
                    serde_json::json!({
                        "exitCode": exit_code,
                        "reason": "Error",
                        "startedAt": now,
                        "finishedAt": now
                    })
                });

            statuses.push(serde_json::json!({
                "name": name,
                "image": image,
                "imageID": existing.and_then(|s| s.get("imageID")).cloned().unwrap_or_else(|| serde_json::json!("")),
                "ready": false,
                "started": false,
                "restartCount": restart_count,
                "state": {"waiting": {"reason": REASON_POD_INITIALIZING}},
                "lastState": {"terminated": terminated}
            }));
        } else if saw_failed {
            statuses.push(serde_json::json!({
                "name": name,
                "image": image,
                "imageID": "",
                "ready": false,
                "started": false,
                "restartCount": 0,
                "state": {"waiting": {"reason": REASON_POD_INITIALIZING}}
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
                        "startedAt": now,
                        "finishedAt": now
                    }
                }
            }));
        }
    }
    statuses
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_build_container_statuses_crashloopbackoff_waiting() {
        let containers = vec![(
            "nginx".to_string(),
            ContainerInfo {
                container_id: "abc123".to_string(),
                image: "nginx:latest".to_string(),
                image_ref: "sha256:abc".to_string(),
                state: 2, // Exited
                exit_code: 1,
                started_at: 0,
                finished_at: 0,
                termination_message: String::new(),
            },
        )];

        let mut restart_counts = HashMap::new();
        restart_counts.insert("nginx".to_string(), 3);

        let now = chrono::Utc::now().timestamp();
        let mut backoff_state = HashMap::new();
        backoff_state.insert(
            "nginx".to_string(),
            ContainerBackoffState {
                next_restart_time: now + 30, // Still in backoff
            },
        );

        let statuses = build_container_statuses_with_backoff(
            &containers,
            &restart_counts,
            &backoff_state,
            &std::collections::HashSet::new(),
        );
        assert_eq!(statuses.len(), 1);

        let status = &statuses[0];
        assert_eq!(status["name"], "nginx");
        assert_eq!(status["restartCount"], 3);

        // Should have waiting state with CrashLoopBackOff reason
        assert!(status["state"]["waiting"].is_object());
        assert_eq!(status["state"]["waiting"]["reason"], "CrashLoopBackOff");
        assert!(
            status["state"]["waiting"]["message"]
                .as_str()
                .unwrap()
                .contains("back-off")
        );
    }

    #[test]
    fn test_build_container_statuses_running() {
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
        let restart_counts = HashMap::new();

        let statuses = build_container_statuses(
            &containers,
            &restart_counts,
            &std::collections::HashSet::new(),
        );
        assert_eq!(statuses.len(), 1);

        let status = &statuses[0];
        assert_eq!(status.get("name").unwrap().as_str().unwrap(), "nginx");
        assert!(status.get("state").unwrap().get("running").is_some());
        assert_eq!(status.get("restartCount").unwrap().as_i64().unwrap(), 0);
    }

    #[test]
    fn test_build_container_statuses_terminated() {
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
        let restart_counts = HashMap::new();

        let statuses = build_container_statuses(
            &containers,
            &restart_counts,
            &std::collections::HashSet::new(),
        );
        assert_eq!(statuses.len(), 1);

        let status = &statuses[0];
        assert_eq!(status.get("name").unwrap().as_str().unwrap(), "nginx");
        let terminated = status.get("state").unwrap().get("terminated").unwrap();
        assert_eq!(terminated.get("exitCode").unwrap().as_i64().unwrap(), 1);
    }

    #[test]
    fn test_container_status_has_running_state_after_start() {
        // After a container starts, state.running.startedAt must be a valid timestamp.
        // An empty state {} is never acceptable — kubectl needs this to show READY/STATUS.
        let containers = vec![(
            "test".to_string(),
            ContainerInfo {
                state: 1, // Running
                exit_code: 0,
                finished_at: 0,
                started_at: 1_712_700_000_000_000_000, // 2024-04-10T00:00:00Z in nanos
                image: "busybox:latest".to_string(),
                image_ref: "docker.io/library/busybox:latest".to_string(),
                container_id: "container123".to_string(),
                termination_message: String::new(),
            },
        )];
        let statuses = build_container_statuses(
            &containers,
            &HashMap::new(),
            &std::collections::HashSet::new(),
        );
        assert_eq!(statuses.len(), 1);
        let status = &statuses[0];

        // state must NOT be empty
        let state = status.get("state").expect("state field must exist");
        assert!(
            state.as_object().map(|o| !o.is_empty()).unwrap_or(false),
            "state must not be empty {{}}, got: {:?}",
            state
        );

        // state.running must exist with startedAt
        let running = state.get("running").expect("state.running must exist");
        let started_at = running.get("startedAt").expect("startedAt must exist");
        assert!(started_at.as_str().is_some(), "startedAt must be a string");
        assert!(
            !started_at.as_str().unwrap().is_empty(),
            "startedAt must not be empty"
        );

        // Other required fields
        assert_eq!(status["name"], "test");
        assert_eq!(status["ready"], false); // not in ready set
        assert_eq!(status["containerID"], "containerd://container123");
    }

    #[test]
    fn test_container_status_has_terminated_state_after_exit() {
        // After a container exits, state.terminated must have exitCode, reason, startedAt, finishedAt.
        // An empty state {} is never acceptable.
        let containers = vec![(
            "test".to_string(),
            ContainerInfo {
                state: 2, // Exited
                exit_code: 1,
                finished_at: 1_712_700_060_000_000_000, // 60s after start
                started_at: 1_712_700_000_000_000_000,
                image: "busybox:latest".to_string(),
                image_ref: "docker.io/library/busybox:latest".to_string(),
                container_id: "container456".to_string(),
                termination_message: String::new(),
            },
        )];
        let statuses = build_container_statuses(
            &containers,
            &HashMap::new(),
            &std::collections::HashSet::new(),
        );
        assert_eq!(statuses.len(), 1);
        let status = &statuses[0];

        // state must NOT be empty
        let state = status.get("state").expect("state field must exist");
        assert!(
            state.as_object().map(|o| !o.is_empty()).unwrap_or(false),
            "state must not be empty {{}}, got: {:?}",
            state
        );

        // state.terminated must exist with exitCode, reason, startedAt, finishedAt
        let terminated = state
            .get("terminated")
            .expect("state.terminated must exist");
        assert_eq!(terminated["exitCode"], 1);
        assert_eq!(terminated["reason"], "Error");
        assert!(
            terminated
                .get("startedAt")
                .and_then(|v| v.as_str())
                .is_some(),
            "startedAt must be set"
        );
        assert!(
            terminated
                .get("finishedAt")
                .and_then(|v| v.as_str())
                .is_some(),
            "finishedAt must be set"
        );

        // Verify timestamps are non-empty and valid
        let started = terminated["startedAt"].as_str().unwrap();
        let finished = terminated["finishedAt"].as_str().unwrap();
        assert!(!started.is_empty(), "startedAt must not be empty");
        assert!(!finished.is_empty(), "finishedAt must not be empty");
        assert!(started.contains('T'), "startedAt must be RFC3339 format");
        assert!(finished.contains('T'), "finishedAt must be RFC3339 format");
    }

    #[test]
    fn test_container_status_exited_zero_shows_completed_reason() {
        let containers = vec![(
            "test".to_string(),
            ContainerInfo {
                state: 2,
                exit_code: 0,
                finished_at: 1_712_700_060_000_000_000,
                started_at: 1_712_700_000_000_000_000,
                image: "busybox".to_string(),
                image_ref: "docker.io/library/busybox".to_string(),
                container_id: "c789".to_string(),
                termination_message: String::new(),
            },
        )];
        let statuses = build_container_statuses(
            &containers,
            &HashMap::new(),
            &std::collections::HashSet::new(),
        );
        let terminated = &statuses[0]["state"]["terminated"];
        assert_eq!(terminated["exitCode"], 0);
        assert_eq!(terminated["reason"], "Completed");
    }

    #[test]
    fn test_build_container_statuses_waiting() {
        // Waiting state is indicated by state=0 (Created) or when we're about to restart
        let containers = vec![(
            "nginx".to_string(),
            ContainerInfo {
                state: 0, // Created (waiting to start)
                exit_code: 0,
                finished_at: 0,
                started_at: 0,
                image: "nginx:latest".to_string(),
                image_ref: "docker.io/library/nginx:latest".to_string(),
                container_id: "abc123".to_string(),
                termination_message: String::new(),
            },
        )];
        let restart_counts = HashMap::new();

        let statuses = build_container_statuses(
            &containers,
            &restart_counts,
            &std::collections::HashSet::new(),
        );
        assert_eq!(statuses.len(), 1);

        let status = &statuses[0];
        assert_eq!(status.get("name").unwrap().as_str().unwrap(), "nginx");
        assert!(status.get("state").unwrap().get("waiting").is_some());
    }

    #[test]
    fn test_build_ephemeral_container_status_running() {
        let status = build_ephemeral_container_status(EphemeralContainerStatusFixture {
            container_name: "debugger",
            container_id: Some("abc123"),
            state: 1,
            started_at_ns: 1_712_700_000_000_000_000,
            finished_at_ns: 0,
            exit_code: 0,
            image: "busybox",
            image_ref: "docker.io/library/busybox:latest",
        });
        assert_eq!(status["name"], "debugger");
        assert_eq!(status["containerID"], "containerd://abc123");
        assert!(status.pointer("/state/running/startedAt").is_some());
        assert_eq!(status["ready"], true);
        assert_eq!(status["started"], true);
    }

    #[test]
    fn test_build_ephemeral_container_status_waiting_without_runtime_id() {
        let status = build_ephemeral_container_status(EphemeralContainerStatusFixture {
            container_name: "debugger",
            container_id: None,
            state: 0,
            started_at_ns: 0,
            finished_at_ns: 0,
            exit_code: 0,
            image: "busybox",
            image_ref: "",
        });
        assert_eq!(status["name"], "debugger");
        assert!(status.get("containerID").is_none());
        assert_eq!(
            status
                .pointer("/state/waiting/reason")
                .and_then(|v| v.as_str()),
            Some("ContainerCreating")
        );
        assert_eq!(status["ready"], false);
        assert_eq!(status["started"], false);
    }

    #[test]
    fn test_init_container_status_format() {
        let status = build_init_container_status(
            "copyutil",
            "busybox:latest",
            "abc123",
            0,
            1000000000,
            2000000000,
        );

        assert_eq!(status["name"], "copyutil");
        assert_eq!(status["image"], "busybox:latest");
        assert_eq!(status["containerID"], "containerd://abc123");
        assert_eq!(status["ready"], true);
        assert_eq!(status["restartCount"], 0);

        // Verify terminated state structure
        let state = status["state"].as_object().unwrap();
        assert!(state.contains_key("terminated"));
        let terminated = state["terminated"].as_object().unwrap();
        assert_eq!(terminated["exitCode"], 0);
        assert_eq!(terminated["reason"], "Completed");
        assert!(terminated.contains_key("startedAt"));
        assert!(terminated.contains_key("finishedAt"));
    }
}
