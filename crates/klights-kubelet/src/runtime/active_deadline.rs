use std::sync::Arc;

use serde_json::Value;

use crate::pod_repository::status::PodStatusWriter;
use crate::runtime::cri::{ContainerRuntimeControl, CriRuntime};
use crate::runtime_types::PodRuntimeKey;

/// Owns the activeDeadlineSeconds exceeded transition for a Pod.
pub(super) struct ActiveDeadlineEnforcer {
    cri: Arc<dyn CriRuntime>,
    container_control: Arc<dyn ContainerRuntimeControl>,
    pod_status_writer: Arc<dyn PodStatusWriter>,
}

impl ActiveDeadlineEnforcer {
    pub(super) fn new(
        cri: Arc<dyn CriRuntime>,
        container_control: Arc<dyn ContainerRuntimeControl>,
        pod_status_writer: Arc<dyn PodStatusWriter>,
    ) -> Self {
        Self {
            cri,
            container_control,
            pod_status_writer,
        }
    }

    pub(super) async fn enforce_exceeded(
        &self,
        key: &PodRuntimeKey,
        resource_version: i64,
        deadline_secs: i64,
        sandbox_id: Option<&str>,
    ) -> anyhow::Result<bool> {
        tracing::info!(
            namespace = key.namespace,
            name = key.name,
            uid = key.uid,
            deadline_secs,
            "pod exceeded activeDeadlineSeconds, terminating containers"
        );

        if let Some(sandbox_id) = sandbox_id {
            let containers = self
                .container_control
                .list_containers(Some(sandbox_id))
                .await?;
            for (container_id, _) in containers {
                self.cri.stop_container(&container_id, 0).await?;
            }
        }

        let message = format!(
            "Pod was active on the node longer than the specified deadline ({}s)",
            deadline_secs
        );
        if let Err(e) = self
            .pod_status_writer
            .set_deadline_exceeded_for_uid(
                &key.namespace,
                &key.name,
                &key.uid,
                message,
                Some(resource_version),
            )
            .await
        {
            tracing::warn!(
                namespace = key.namespace,
                name = key.name,
                uid = key.uid,
                "Failed to mark pod as DeadlineExceeded: {e:#}"
            );
        }

        Ok(true)
    }
}

pub(super) fn exceeded_active_deadline_seconds_at(pod: &Value, now: i64) -> Option<i64> {
    let deadline_secs = pod
        .pointer("/spec/activeDeadlineSeconds")
        .and_then(|v| v.as_i64())?;

    let start_ts = pod
        .pointer("/status/startTime")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.timestamp())
        .or_else(|| {
            pod.pointer("/metadata/creationTimestamp")
                .and_then(|v| v.as_str())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.timestamp())
        })?;

    if now - start_ts >= deadline_secs {
        Some(deadline_secs)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_deadline_uses_start_time_before_creation_timestamp() {
        let pod = serde_json::json!({
            "metadata": {"creationTimestamp": "2026-05-20T00:00:00Z"},
            "spec": {"activeDeadlineSeconds": 60},
            "status": {"startTime": "2026-05-20T00:10:00Z"}
        });

        assert_eq!(
            exceeded_active_deadline_seconds_at(&pod, 1_779_235_859),
            None
        );
        assert_eq!(
            exceeded_active_deadline_seconds_at(&pod, 1_779_235_860),
            Some(60)
        );
    }
}
