use super::*;

pub(super) fn deadline_timer_schedule_set()
-> &'static std::sync::Mutex<std::collections::HashSet<String>> {
    static SCHEDULED: std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<String>>> =
        std::sync::OnceLock::new();
    SCHEDULED.get_or_init(|| std::sync::Mutex::new(std::collections::HashSet::new()))
}

pub(super) fn parse_deadline_timer_delay_secs(
    pod: &serde_json::Value,
) -> Option<(String, String, u64, String)> {
    let deadline_secs = pod
        .pointer("/spec/activeDeadlineSeconds")
        .and_then(|v| v.as_i64())
        .filter(|v| *v > 0)?;

    let namespace = pod
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        .unwrap_or("default")
        .to_string();
    let pod_name = pod
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if pod_name.is_empty() {
        return None;
    }

    let phase = pod
        .pointer("/status/phase")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if phase == "Succeeded" || phase == "Failed" {
        return None;
    }

    let start_time_raw = pod
        .pointer("/status/startTime")
        .and_then(|v| v.as_str())
        .or_else(|| {
            pod.pointer("/metadata/creationTimestamp")
                .and_then(|v| v.as_str())
        })?;

    let start_ts = chrono::DateTime::parse_from_rfc3339(start_time_raw)
        .ok()
        .map(|dt| dt.timestamp())?;
    let now = chrono::Utc::now().timestamp();
    let elapsed = std::cmp::max(0, now - start_ts);
    let remaining = std::cmp::max(0, deadline_secs - elapsed) as u64;
    let schedule_key = format!("{}/{}@{}:{}", namespace, pod_name, start_ts, deadline_secs);
    Some((namespace, pod_name, remaining, schedule_key))
}

pub(super) async fn schedule_active_deadline_timer_for_modified_pod(
    pod: &serde_json::Value,
    task_supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
    pod_lifecycle_router: std::sync::Arc<crate::kubelet::pod_lifecycle_router::PodLifecycleRouter>,
) {
    let Some((namespace, pod_name, delay_secs, schedule_key)) =
        parse_deadline_timer_delay_secs(pod)
    else {
        return;
    };
    let Some(key) = pod_lifecycle_key_from_pod(pod) else {
        tracing::warn!(
            "cannot schedule active deadline for pod without lifecycle identity {}/{}",
            namespace,
            pod_name
        );
        return;
    };

    let schedule_set = deadline_timer_schedule_set();
    {
        let mut guard = schedule_set.lock().unwrap_or_else(|p| p.into_inner());
        if guard.contains(&schedule_key) {
            return;
        }
        guard.insert(schedule_key.clone());
    }

    let schedule_key_for_timer = schedule_key.clone();
    if let Err(err) = task_supervisor
        .spawn_delay(
            "pod_active_deadline_timer",
            std::time::Duration::from_secs(delay_secs),
            async move {
                let _ = pod_lifecycle_router
                    .route(LifecycleMessage::ActiveDeadlineDue { key })
                    .await;
                let mut guard = deadline_timer_schedule_set()
                    .lock()
                    .unwrap_or_else(|p| p.into_inner());
                guard.remove(&schedule_key_for_timer);
            },
        )
        .await
    {
        tracing::warn!(
            "Failed to schedule activeDeadlineSeconds timer for {}/{}: {}",
            namespace,
            pod_name,
            err
        );
        let mut guard = deadline_timer_schedule_set()
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        guard.remove(&schedule_key);
    }
}
