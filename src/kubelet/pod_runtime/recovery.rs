use crate::kubelet::pod_runtime::cri::{ContainerRuntimeControl, ContainerRuntimeState};
use crate::kubelet::pod_runtime::service::PodRuntimeKey;
use crate::kubelet::pod_runtime::store::PodRuntimeStore;

pub async fn already_realized_running_sandbox(
    store: &dyn PodRuntimeStore,
    container_control: &dyn ContainerRuntimeControl,
    key: &PodRuntimeKey,
    pod: &serde_json::Value,
) -> Option<String> {
    let phase = pod
        .pointer("/status/phase")
        .and_then(|v| v.as_str())
        .unwrap_or("Pending");
    let has_pod_ip = pod
        .pointer("/status/podIP")
        .and_then(|v| v.as_str())
        .is_some_and(|ip| !ip.trim().is_empty());
    if phase != "Running" || !has_pod_ip {
        return None;
    }

    let sandbox_id = match store.get_sandbox_id(key).await {
        Ok(Some(sandbox_id)) if !sandbox_id.trim().is_empty() => sandbox_id,
        Ok(_) => return None,
        Err(err) => {
            tracing::warn!(
                namespace = key.namespace,
                name = key.name,
                uid = key.uid,
                "failed to inspect recorded sandbox before startup recovery: {err:#}"
            );
            return None;
        }
    };

    let containers = match container_control.list_containers(Some(&sandbox_id)).await {
        Ok(containers) => containers,
        Err(err) => {
            tracing::warn!(
                namespace = key.namespace,
                name = key.name,
                uid = key.uid,
                sandbox_id = %sandbox_id,
                "failed to inspect sandbox containers before startup recovery: {err:#}"
            );
            return None;
        }
    };

    containers
        .iter()
        .any(|(_, state)| {
            matches!(
                state,
                ContainerRuntimeState::Created | ContainerRuntimeState::Running
            )
        })
        .then_some(sandbox_id)
}
