use crate::kubelet::pod_termination::find_pod_container_spec;

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
                "startedAt": crate::utils::k8s_timestamp()
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

fn cri_timestamp_from_ns(ns: i64) -> String {
    if ns <= 0 {
        return crate::utils::k8s_timestamp();
    }
    let secs = ns / 1_000_000_000;
    let sub_ns = (ns % 1_000_000_000) as u32;
    chrono::DateTime::from_timestamp(secs, sub_ns)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S.%fZ").to_string())
        .unwrap_or_else(crate::utils::k8s_timestamp)
}
