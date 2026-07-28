use crate::kubelet::pod_termination::find_pod_container_spec;

pub(super) fn pod_status_container_name_by_id(
    pod: &serde_json::Value,
) -> std::collections::HashMap<String, String> {
    let mut name_by_id = std::collections::HashMap::new();
    let Some(statuses) = pod
        .pointer("/status/containerStatuses")
        .and_then(|v| v.as_array())
    else {
        return name_by_id;
    };

    for status in statuses {
        let id = status
            .get("containerID")
            .and_then(|id| id.as_str())
            .map(|id| id.strip_prefix("containerd://").unwrap_or(id).to_string());
        let name = status
            .get("name")
            .and_then(|name| name.as_str())
            .map(str::to_string);
        if let (Some(id), Some(name)) = (id, name) {
            name_by_id.insert(id, name);
        }
    }
    name_by_id
}

pub(super) fn pod_status_container_id_by_name(
    pod: &serde_json::Value,
    container_name: &str,
) -> Option<String> {
    pod.pointer("/status/containerStatuses")
        .and_then(|v| v.as_array())
        .and_then(|statuses| {
            statuses.iter().find(|status| {
                status.get("name").and_then(|name| name.as_str()) == Some(container_name)
            })
        })
        .and_then(|status| status.get("containerID"))
        .and_then(|id| id.as_str())
        .map(|id| id.strip_prefix("containerd://").unwrap_or(id).to_string())
        .filter(|id| !id.is_empty())
}

pub(super) fn pod_status_ip(pod: &serde_json::Value) -> &str {
    pod.pointer("/status/podIP")
        .and_then(|v| v.as_str())
        .or_else(|| {
            pod.pointer("/status/podIPs")
                .and_then(|v| v.as_array())
                .and_then(|ips| ips.first())
                .and_then(|entry| entry.get("ip"))
                .and_then(|v| v.as_str())
        })
        .unwrap_or("")
}

pub(super) fn pod_status_host_ip(pod: &serde_json::Value) -> Option<&str> {
    pod.pointer("/status/hostIP")
        .and_then(|v| v.as_str())
        .or_else(|| {
            pod.pointer("/status/hostIPs")
                .and_then(|v| v.as_array())
                .and_then(|ips| ips.first())
                .and_then(|entry| entry.get("ip"))
                .and_then(|v| v.as_str())
        })
        .filter(|ip| !ip.trim().is_empty())
}

pub fn build_create_container_config_error_status(
    container: &serde_json::Value,
    container_name: &str,
    message: &str,
) -> serde_json::Value {
    let image = container
        .get("image")
        .and_then(|i| i.as_str())
        .unwrap_or("unknown");
    serde_json::json!({
        "name": container_name,
        "ready": false,
        "started": false,
        "state": {
            "waiting": {
                "reason": "CreateContainerConfigError",
                "message": message
            }
        },
        "image": image,
        "imageID": "",
        "restartCount": 0,
    })
}

pub fn pod_restart_policy(pod: &serde_json::Value) -> &str {
    pod.pointer("/spec/restartPolicy")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("Always")
}

pub fn should_restart_exited_container(restart_policy: &str, exit_code: i32) -> bool {
    match restart_policy {
        "Always" => true,
        "OnFailure" => exit_code != 0,
        "Never" => false,
        _ => false,
    }
}

pub fn restart_last_state_from_runtime_status(
    status: Option<&k8s_cri::v1::ContainerStatus>,
) -> serde_json::Value {
    let exit_code = status.map(|status| status.exit_code).unwrap_or(137);
    let mut terminated = serde_json::json!({
        "exitCode": exit_code,
        "reason": if exit_code == 0 { "Completed" } else { "Error" },
        "startedAt": cri_timestamp_from_ns(status.map(|status| status.started_at).unwrap_or(0)),
        "finishedAt": cri_timestamp_from_ns(status.map(|status| status.finished_at).unwrap_or(0)),
    });
    if let Some(message) =
        status.and_then(|status| (!status.message.is_empty()).then_some(status.message.as_str()))
    {
        terminated["message"] = serde_json::json!(message);
    }
    serde_json::json!({ "terminated": terminated })
}

pub fn restart_last_state_from_reconciled_status(
    status: &serde_json::Value,
) -> Option<serde_json::Value> {
    status
        .pointer("/state/terminated")
        .cloned()
        .map(|terminated| serde_json::json!({ "terminated": terminated }))
}

pub fn runtime_status_container_id(status: &serde_json::Value) -> Option<String> {
    status
        .get("containerID")
        .and_then(|value| value.as_str())
        .map(|id| id.strip_prefix("containerd://").unwrap_or(id).to_string())
        .filter(|id| !id.is_empty())
}

pub fn restarted_running_container_status(
    pod: &serde_json::Value,
    container_name: &str,
    new_container_id: &str,
    observed_status: &serde_json::Value,
    last_state: &serde_json::Value,
) -> Option<serde_json::Value> {
    let container = find_pod_container_spec(pod, container_name)?;
    let image = container
        .get("image")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .or_else(|| {
            observed_status
                .get("image")
                .and_then(|value| value.as_str())
                .filter(|value| !value.is_empty())
        })
        .unwrap_or("nginx:latest");
    let image_id = observed_status
        .get("imageID")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .unwrap_or(image);
    let restart_count = observed_status
        .get("restartCount")
        .and_then(|value| value.as_i64())
        .filter(|value| *value >= 0)
        .unwrap_or(0)
        .saturating_add(1);
    let ready = container.get("readinessProbe").is_none();

    Some(serde_json::json!({
        "name": container_name,
        "containerID": format!("containerd://{}", new_container_id),
        "ready": ready,
        "started": true,
        "restartCount": restart_count,
        "lastState": last_state.clone(),
        "state": {
            "running": {
                "startedAt": crate::k8s_time::now_legacy_timestamp()
            }
        },
        "image": image,
        "imageID": image_id,
    }))
}

pub fn replace_container_status(
    statuses: &mut Vec<serde_json::Value>,
    container_name: &str,
    replacement: serde_json::Value,
) {
    if let Some(status) = statuses
        .iter_mut()
        .find(|status| status.get("name").and_then(|value| value.as_str()) == Some(container_name))
    {
        *status = replacement;
    } else {
        statuses.push(replacement);
    }
}

pub fn json_number_as_i64(value: &serde_json::Value) -> Option<i64> {
    value.as_i64().or_else(|| {
        let number = value.as_f64()?;
        if number.is_finite()
            && number.fract() == 0.0
            && number >= i64::MIN as f64
            && number <= i64::MAX as f64
        {
            Some(number as i64)
        } else {
            None
        }
    })
}

pub(super) struct EphemeralContainerStatusInput<'a> {
    pub(super) container_name: &'a str,
    pub(super) container_id: Option<&'a str>,
    pub(super) state: i32,
    pub(super) started_at_ns: i64,
    pub(super) finished_at_ns: i64,
    pub(super) exit_code: i32,
    pub(super) image: &'a str,
    pub(super) image_ref: &'a str,
}

pub(super) fn build_ephemeral_container_status(
    input: EphemeralContainerStatusInput<'_>,
) -> serde_json::Value {
    let EphemeralContainerStatusInput {
        container_name,
        container_id,
        state,
        started_at_ns,
        finished_at_ns,
        exit_code,
        image,
        image_ref,
    } = input;
    let state_obj = match state {
        state if state == k8s_cri::v1::ContainerState::ContainerRunning as i32 => {
            serde_json::json!({
                "running": {
                    "startedAt": cri_timestamp_from_ns(started_at_ns)
                }
            })
        }
        state if state == k8s_cri::v1::ContainerState::ContainerExited as i32 => {
            serde_json::json!({
                "terminated": {
                    "exitCode": exit_code,
                    "reason": if exit_code == 0 { "Completed" } else { "Error" },
                    "startedAt": cri_timestamp_from_ns(started_at_ns),
                    "finishedAt": cri_timestamp_from_ns(finished_at_ns),
                }
            })
        }
        _ => serde_json::json!({
            "waiting": {
                "reason": "ContainerCreating"
            }
        }),
    };

    let mut status = serde_json::json!({
        "name": container_name,
        "state": state_obj,
        "ready": state == k8s_cri::v1::ContainerState::ContainerRunning as i32,
        "started": state == k8s_cri::v1::ContainerState::ContainerRunning as i32
            || state == k8s_cri::v1::ContainerState::ContainerExited as i32,
        "restartCount": 0,
        "image": image,
        "imageID": image_ref,
    });
    if let Some(id) = container_id {
        status["containerID"] = serde_json::json!(format!("containerd://{}", id));
    }
    status
}

pub(super) fn cri_timestamp_from_ns(ns: i64) -> String {
    if ns <= 0 {
        return crate::k8s_time::now_legacy_timestamp();
    }
    let secs = ns / 1_000_000_000;
    let sub_ns = (ns % 1_000_000_000) as u32;
    chrono::DateTime::from_timestamp(secs, sub_ns)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S.%fZ").to_string())
        .unwrap_or_else(crate::k8s_time::now_legacy_timestamp)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pod_status_container_indexes_strip_containerd_prefix() {
        let pod = serde_json::json!({
            "status": {
                "containerStatuses": [
                    {
                        "name": "app",
                        "containerID": "containerd://abc123"
                    },
                    {
                        "name": "sidecar",
                        "containerID": "raw456"
                    }
                ]
            }
        });

        let by_id = pod_status_container_name_by_id(&pod);
        assert_eq!(by_id.get("abc123").map(String::as_str), Some("app"));
        assert_eq!(by_id.get("raw456").map(String::as_str), Some("sidecar"));
        assert_eq!(
            pod_status_container_id_by_name(&pod, "app").as_deref(),
            Some("abc123")
        );
        assert_eq!(
            pod_status_container_id_by_name(&pod, "sidecar").as_deref(),
            Some("raw456")
        );
    }

    #[test]
    fn pod_status_ip_helpers_use_scalar_then_list_fallbacks() {
        let scalar = serde_json::json!({
            "status": {
                "podIP": "10.1.0.2",
                "podIPs": [{"ip": "10.1.0.3"}],
                "hostIP": "192.0.2.10",
                "hostIPs": [{"ip": "192.0.2.11"}]
            }
        });
        assert_eq!(pod_status_ip(&scalar), "10.1.0.2");
        assert_eq!(pod_status_host_ip(&scalar), Some("192.0.2.10"));

        let list_only = serde_json::json!({
            "status": {
                "podIPs": [{"ip": "10.1.0.3"}],
                "hostIPs": [{"ip": "192.0.2.11"}]
            }
        });
        assert_eq!(pod_status_ip(&list_only), "10.1.0.3");
        assert_eq!(pod_status_host_ip(&list_only), Some("192.0.2.11"));
    }

    #[test]
    fn ephemeral_container_status_projects_cri_states() {
        let running = build_ephemeral_container_status(EphemeralContainerStatusInput {
            container_name: "debug",
            container_id: Some("cid-running"),
            state: k8s_cri::v1::ContainerState::ContainerRunning as i32,
            started_at_ns: 1_700_000_000_000_000_000,
            finished_at_ns: 0,
            exit_code: 0,
            image: "busybox",
            image_ref: "sha256:busybox",
        });
        assert_eq!(running["name"], "debug");
        assert_eq!(running["containerID"], "containerd://cid-running");
        assert_eq!(running["ready"], true);
        assert!(running.pointer("/state/running/startedAt").is_some());

        let exited = build_ephemeral_container_status(EphemeralContainerStatusInput {
            container_name: "debug",
            container_id: Some("cid-exited"),
            state: k8s_cri::v1::ContainerState::ContainerExited as i32,
            started_at_ns: 1_700_000_000_000_000_000,
            finished_at_ns: 1_700_000_001_000_000_000,
            exit_code: 2,
            image: "busybox",
            image_ref: "sha256:busybox",
        });
        assert_eq!(
            exited.pointer("/state/terminated/exitCode"),
            Some(&serde_json::json!(2))
        );
        assert_eq!(
            exited.pointer("/state/terminated/reason"),
            Some(&serde_json::json!("Error"))
        );

        let waiting = build_ephemeral_container_status(EphemeralContainerStatusInput {
            container_name: "debug",
            container_id: None,
            state: k8s_cri::v1::ContainerState::ContainerCreated as i32,
            started_at_ns: 0,
            finished_at_ns: 0,
            exit_code: 0,
            image: "busybox",
            image_ref: "",
        });
        assert!(waiting.get("containerID").is_none());
        assert_eq!(
            waiting.pointer("/state/waiting/reason"),
            Some(&serde_json::json!("ContainerCreating"))
        );
    }
}
