use klights_pod_api::{PodGetRequest, PodQuery};

use crate::pod_repository::status::PodStatusWriter;
use crate::pod_repository::{PodStatusUpdate, RuntimeReconcileStatus};
use crate::runtime_types::PodRuntimeKey;

pub(super) fn owns_pod_runtime(node_name: &str, pod: &serde_json::Value) -> bool {
    pod.pointer("/spec/nodeName")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|assigned| assigned == node_name)
}

fn status_array(status: &serde_json::Value, field: &str) -> Vec<serde_json::Value> {
    status
        .get(field)
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn optional_status_array(
    status: &serde_json::Value,
    field: &str,
) -> Option<Vec<serde_json::Value>> {
    status
        .get(field)
        .and_then(serde_json::Value::as_array)
        .cloned()
}

fn optional_status_string(status: &serde_json::Value, field: &str) -> Option<String> {
    status
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

fn live_status_string(
    resource: Option<&klights_cluster_core::Resource>,
    field: &str,
) -> Option<String> {
    resource
        .and_then(|resource| resource.data.pointer("/status"))
        .and_then(|status| status.get(field))
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub(super) async fn apply_forwarded_status(
    pod_query: &dyn PodQuery,
    pod_status: &dyn PodStatusWriter,
    key: &PodRuntimeKey,
    status: serde_json::Value,
) -> anyhow::Result<klights_cluster_core::Resource> {
    let phase = status
        .get("phase")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("Pending")
        .to_string();
    let container_statuses = status_array(&status, "containerStatuses");
    let init_container_statuses = optional_status_array(&status, "initContainerStatuses");

    if status.get("podIP").is_none()
        && status.get("hostIP").is_none()
        && init_container_statuses.is_none()
    {
        return pod_status
            .apply_runtime_reconcile_status_for_uid(
                &key.namespace,
                &key.name,
                &key.uid,
                RuntimeReconcileStatus {
                    phase,
                    container_statuses,
                },
                None,
            )
            .await
            .map_err(|error| anyhow::anyhow!("{error:#}"));
    }

    let live = pod_query
        .get_pod(PodGetRequest::try_by_identity(
            klights_types::PodIdentity::new(&key.namespace, &key.name, &key.uid),
        )?)
        .await
        .map_err(|error| anyhow::anyhow!("{error:#}"))?;
    pod_status
        .set_pod_status_for_uid(
            &key.namespace,
            &key.name,
            &key.uid,
            PodStatusUpdate {
                phase,
                pod_ip: optional_status_string(&status, "podIP")
                    .or_else(|| live_status_string(live.as_ref(), "podIP"))
                    .unwrap_or_default(),
                host_ip: optional_status_string(&status, "hostIP")
                    .or_else(|| live_status_string(live.as_ref(), "hostIP"))
                    .unwrap_or_default(),
                container_statuses,
                init_container_statuses,
                qos_class: None,
            },
            None,
        )
        .await
        .map_err(|error| anyhow::anyhow!("{error:#}"))
}
