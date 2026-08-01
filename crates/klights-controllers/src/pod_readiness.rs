//! Controller-neutral Pod readiness projection.

use serde_json::Value;

pub(crate) fn is_ready(pod: &Value) -> bool {
    let Some(status) = pod.get("status") else {
        return false;
    };
    if status
        .get("conditions")
        .and_then(Value::as_array)
        .is_some_and(|conditions| {
            conditions.iter().any(|condition| {
                condition.get("type").and_then(Value::as_str) == Some("Ready")
                    && condition.get("status").and_then(Value::as_str) == Some("True")
            })
        })
    {
        return true;
    }

    let is_running = status.get("phase").and_then(Value::as_str) == Some("Running");
    let all_containers_ready = status
        .get("containerStatuses")
        .and_then(Value::as_array)
        .filter(|statuses| !statuses.is_empty())
        .is_some_and(|statuses| {
            statuses.iter().all(|status| {
                status
                    .get("ready")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
            })
        });
    is_running && all_containers_ready
}
