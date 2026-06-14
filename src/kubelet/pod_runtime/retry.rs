use std::sync::Arc;

use crate::kubelet::pod_lifecycle_core::message::{LifecycleMessage, PodLifecycleKey};
use crate::kubelet::pod_lifecycle_router::LifecycleReplyHandle;
use crate::kubelet::pod_runtime::events::PodEventSink;
use crate::kubelet::pod_runtime::repository::PodRuntimeRepository;
use crate::kubelet::pod_runtime::service::PodRuntimeKey;
use crate::task_supervisor::TaskSupervisor;

fn lifecycle_key_from_runtime_key(key: &PodRuntimeKey) -> PodLifecycleKey {
    PodLifecycleKey::new(&key.namespace, &key.name, &key.uid)
}

pub struct RetryRuntimeContext<'a> {
    pub repository: &'a dyn PodRuntimeRepository,
    pub events: &'a dyn PodEventSink,
    pub supervisor: &'a Arc<TaskSupervisor>,
    pub node_name: &'a str,
}

pub struct StartPodRetryRequest {
    pub key: PodRuntimeKey,
    pub delay: std::time::Duration,
    pub error_message: String,
    pub attempt: u32,
}

pub async fn schedule_retry(
    supervisor: &Arc<TaskSupervisor>,
    key: PodRuntimeKey,
    delay: std::time::Duration,
    reply_to: LifecycleReplyHandle,
) -> anyhow::Result<()> {
    let lifecycle_key = lifecycle_key_from_runtime_key(&key);
    let _ = supervisor
        .spawn_delay("runtime_schedule_retry", delay, async move {
            let _ = reply_to
                .route(LifecycleMessage::RetryDue { key: lifecycle_key })
                .await;
        })
        .await;
    Ok(())
}

pub async fn schedule_start_pod_retry(
    context: RetryRuntimeContext<'_>,
    request: StartPodRetryRequest,
    reply_to: LifecycleReplyHandle,
) -> anyhow::Result<()> {
    let StartPodRetryRequest {
        key,
        delay,
        error_message,
        attempt,
    } = request;

    if let Err(e) = context
        .repository
        .mark_start_pending_for_retry_for_uid(&key.namespace, &key.name, &key.uid, &error_message)
        .await
    {
        tracing::warn!(
            namespace = %key.namespace,
            pod = %key.name,
            uid = %key.uid,
            attempt,
            error = %e,
            "runtime: failed to write retry status for pod start failure"
        );
    }

    if let Err(e) = context
        .events
        .emit_pod_event(
            &key,
            "Warning",
            "Failed",
            &error_message,
            "kubelet",
            context.node_name,
        )
        .await
    {
        tracing::warn!(
            namespace = %key.namespace,
            pod = %key.name,
            uid = %key.uid,
            attempt,
            error = %e,
            "runtime: failed to emit Warning Failed event for pod start failure"
        );
    }

    let lifecycle_key = lifecycle_key_from_runtime_key(&key);
    let _ = context
        .supervisor
        .spawn_delay("runtime_schedule_start_pod_retry", delay, async move {
            let _ = reply_to
                .route(LifecycleMessage::RetryDue { key: lifecycle_key })
                .await;
        })
        .await;
    Ok(())
}
