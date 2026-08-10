use super::*;

#[derive(Clone, Default)]
pub(super) struct DeadlineTimerRegistry {
    scheduled: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<PodLifecycleKey, String>>>,
}

impl DeadlineTimerRegistry {
    fn replace(&self, key: &PodLifecycleKey, schedule: &str) -> bool {
        let mut scheduled = self
            .scheduled
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if scheduled.get(key).map(String::as_str) == Some(schedule) {
            return false;
        }
        scheduled.insert(key.clone(), schedule.to_string());
        true
    }

    fn invalidate(&self, key: &PodLifecycleKey) {
        self.scheduled
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(key);
    }

    fn take_if_current(&self, key: &PodLifecycleKey, schedule: &str) -> bool {
        let mut scheduled = self
            .scheduled
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if scheduled.get(key).map(String::as_str) != Some(schedule) {
            return false;
        }
        scheduled.remove(key);
        true
    }
}

pub(super) fn parse_deadline_timer_delay_secs_at(
    pod: &serde_json::Value,
    now_unix_seconds: i64,
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
    let elapsed = std::cmp::max(0, now_unix_seconds - start_ts);
    let remaining = std::cmp::max(0, deadline_secs - elapsed) as u64;
    let schedule_key = format!("{}/{}@{}:{}", namespace, pod_name, start_ts, deadline_secs);
    Some((namespace, pod_name, remaining, schedule_key))
}

pub(super) async fn schedule_active_deadline_timer_for_modified_pod(
    pod: &serde_json::Value,
    now_unix_seconds: i64,
    registry: DeadlineTimerRegistry,
    task_supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
    pod_lifecycle_router: std::sync::Arc<crate::pod_lifecycle_router::PodLifecycleRouter>,
) {
    let lifecycle_key = pod_lifecycle_key_from_pod(pod);
    let Some((namespace, pod_name, delay_secs, schedule_key)) =
        parse_deadline_timer_delay_secs_at(pod, now_unix_seconds)
    else {
        if let Some(key) = lifecycle_key.as_ref() {
            registry.invalidate(key);
        }
        return;
    };
    let Some(key) = lifecycle_key else {
        tracing::warn!(
            "cannot schedule active deadline for pod without lifecycle identity {}/{}",
            namespace,
            pod_name
        );
        return;
    };

    if !registry.replace(&key, &schedule_key) {
        return;
    }

    let schedule_key_for_timer = schedule_key.clone();
    let key_for_timer = key.clone();
    let delivery_key = key.clone();
    let timer_registry = registry.clone();
    if let Err(err) = task_supervisor
        .spawn_delay(
            "pod_active_deadline_timer",
            std::time::Duration::from_secs(delay_secs),
            async move {
                if timer_registry.take_if_current(&key_for_timer, &schedule_key_for_timer) {
                    let _ = pod_lifecycle_router
                        .route(LifecycleMessage::ActiveDeadlineDue { key: delivery_key })
                        .await;
                }
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
        if registry.take_if_current(&key, &schedule_key) {
            tracing::debug!("removed rejected active deadline schedule");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> PodLifecycleKey {
        PodLifecycleKey::new("workloads", "deadline-pod", "deadline-uid")
    }

    #[test]
    fn changed_deadline_supersedes_stale_timer_delivery() {
        let registry = DeadlineTimerRegistry::default();
        let key = key();

        assert!(registry.replace(&key, "old-schedule"));
        assert!(registry.replace(&key, "new-schedule"));
        assert!(!registry.take_if_current(&key, "old-schedule"));
        assert!(registry.take_if_current(&key, "new-schedule"));
    }

    #[test]
    fn terminal_update_invalidates_pending_deadline_delivery() {
        let registry = DeadlineTimerRegistry::default();
        let key = key();

        assert!(registry.replace(&key, "active-schedule"));
        registry.invalidate(&key);
        assert!(!registry.take_if_current(&key, "active-schedule"));
    }
}
