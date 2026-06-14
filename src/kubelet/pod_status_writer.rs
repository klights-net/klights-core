//! Legacy pod-status writer kept ONLY for unit-test fixtures that exercise
//! the historical full-pod-update path.
//!
//! Production lifecycle code writes pod status through
//! `crate::kubelet::pod_repository::PodRepository` (`set_pod_status`,
//! `apply_runtime_reconcile_status`, `set_probe_readiness`,
//! `set_deadline_exceeded`). Pod→Service endpoint reconcile is owned by the
//! leader's controllers (driven by `side_effects::service_pod` and
//! `replication::grpc::server::enqueue_forwarded_pod_status_effects`) — the
//! kubelet no longer reconciles endpoints directly.
//!
//! This helper is `#[cfg(test)]`-only so the existing tests under
//! `kubelet::pod_manager::tests::tests_mounts_and_create` keep passing without
//! growing a parallel rebuild of the status JSON.

#[cfg(test)]
use crate::datastore::DatastoreBackend;
#[cfg(test)]
use crate::kubelet::pod_endpoints::reconcile_endpoints_for_pod;
#[cfg(test)]
use crate::kubelet::pod_manager::get_cached_host_ip;
#[cfg(test)]
use crate::kubelet::pod_status_logic::{
    compute_initialized_condition, get_condition_last_transition_time,
};
#[cfg(test)]
use anyhow::Result;
#[cfg(test)]
use serde_json::Value;

/// Build a pod status condition Value with consistent shape:
/// `type`, `status`, `lastTransitionTime` (carried over when status hasn't
/// changed), and an optional `reason`/`message` pair.
#[cfg(test)]
fn build_pod_condition(
    existing_conditions: &[Value],
    now: &str,
    cond_type: &str,
    status: &str,
    reason: Option<&str>,
    message: Option<&str>,
) -> Value {
    let mut obj = serde_json::Map::new();
    obj.insert("type".to_string(), serde_json::json!(cond_type));
    obj.insert("status".to_string(), serde_json::json!(status));
    obj.insert(
        "lastTransitionTime".to_string(),
        serde_json::json!(get_condition_last_transition_time(
            existing_conditions,
            cond_type,
            status,
            now,
        )),
    );
    if let Some(r) = reason {
        obj.insert("reason".to_string(), serde_json::json!(r));
    }
    if let Some(m) = message {
        obj.insert("message".to_string(), serde_json::json!(m));
    }
    Value::Object(obj)
}

/// Pod status update parameters (legacy test-only shape).
#[cfg(test)]
pub struct PodStatusUpdate {
    pub phase: String,
    pub pod_ip: String,
    pub sandbox_id: String,
    pub container_statuses: Vec<Value>,
    pub init_container_statuses: Vec<Value>,
}

/// Test-only helper that mirrors the legacy production behaviour: rebuild
/// the pod status from scratch, persist it via a full-object update, and
/// reconcile endpoints afterward.
#[cfg(test)]
pub async fn update_pod_status(
    db: &dyn DatastoreBackend,
    pod_reader: &dyn crate::kubelet::pod_repository::PodReader,
    pod_name: &str,
    namespace: &str,
    status: PodStatusUpdate,
    services: Option<&dyn crate::networking::ServiceRouter>,
) -> Result<()> {
    let PodStatusUpdate {
        phase,
        pod_ip,
        sandbox_id,
        container_statuses,
        init_container_statuses,
    } = status;
    let pod_resource = db
        .get_resource("v1", "Pod", Some(namespace), pod_name)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Pod not found"))?;

    let mut pod: serde_json::Value = std::sync::Arc::unwrap_or_clone(pod_resource.data);

    let existing_conditions = pod
        .get("status")
        .and_then(|s| s.get("conditions"))
        .and_then(|c| c.as_array())
        .cloned()
        .unwrap_or_default();

    let (init_initialized, init_not_ready_message) =
        compute_initialized_condition(&pod, &init_container_statuses);

    let mut preserved_status_fields: std::collections::HashMap<
        String,
        (Option<Value>, Option<Value>),
    > = std::collections::HashMap::new();
    if let Some(existing_statuses) = pod
        .pointer("/status/containerStatuses")
        .and_then(|s| s.as_array())
    {
        for existing in existing_statuses {
            let Some(name) = existing.get("name").and_then(|n| n.as_str()) else {
                continue;
            };
            let restart_count = existing.get("restartCount").cloned();
            let last_state = existing.get("lastState").cloned();
            preserved_status_fields.insert(name.to_string(), (restart_count, last_state));
        }
    }

    let mut container_statuses = container_statuses;
    for status in &mut container_statuses {
        let Some(name) = status.get("name").and_then(|n| n.as_str()) else {
            continue;
        };
        let Some((restart_count, last_state)) = preserved_status_fields.get(name) else {
            continue;
        };
        if let Some(obj) = status.as_object_mut() {
            if let Some(v) = restart_count {
                let preserved_count = v.as_i64().unwrap_or(0);
                let current_count = obj
                    .get("restartCount")
                    .and_then(|value| value.as_i64())
                    .unwrap_or(0);
                if !obj.contains_key("restartCount") || preserved_count > current_count {
                    obj.insert("restartCount".to_string(), v.clone());
                }
            }
            if obj.get("lastState").is_none()
                && let Some(v) = last_state
            {
                obj.insert("lastState".to_string(), v.clone());
            }
        }
    }

    if let Some(obj) = pod.as_object_mut() {
        let metadata = obj.entry("metadata").or_insert(serde_json::json!({}));
        if let Some(meta_obj) = metadata.as_object_mut() {
            let annotations = meta_obj
                .entry("annotations")
                .or_insert(serde_json::json!({}));
            if let Some(ann_obj) = annotations.as_object_mut() {
                ann_obj.insert(
                    "klights.dev/sandbox-id".to_string(),
                    serde_json::json!(sandbox_id),
                );
            }
        }

        let now = crate::utils::k8s_timestamp();
        let all_containers_ready = if container_statuses.is_empty() {
            phase == "Running"
        } else {
            container_statuses
                .iter()
                .all(|c| c.get("ready").and_then(|r| r.as_bool()).unwrap_or(false))
        };

        let bool_status = |b: bool| if b { "True" } else { "False" };
        let initialized_condition = if init_initialized {
            build_pod_condition(
                &existing_conditions,
                &now,
                "Initialized",
                "True",
                None,
                None,
            )
        } else {
            build_pod_condition(
                &existing_conditions,
                &now,
                "Initialized",
                "False",
                Some("ContainersNotInitialized"),
                Some(init_not_ready_message.as_deref().unwrap_or("")),
            )
        };
        let containers_ready = bool_status(all_containers_ready);
        let ready_status = bool_status(phase == "Running" && all_containers_ready);
        let conditions = vec![
            build_pod_condition(
                &existing_conditions,
                &now,
                "PodScheduled",
                "True",
                Some("PodScheduled"),
                None,
            ),
            initialized_condition,
            build_pod_condition(
                &existing_conditions,
                &now,
                "ContainersReady",
                containers_ready,
                None,
                None,
            ),
            build_pod_condition(
                &existing_conditions,
                &now,
                "Ready",
                ready_status,
                None,
                None,
            ),
        ];

        let mut status_obj = serde_json::json!({
            "phase": phase,
            "podIP": pod_ip,
            "podIPs": [{ "ip": pod_ip }],
            "hostIP": get_cached_host_ip(),
            "hostIPs": [{ "ip": get_cached_host_ip() }],
            "conditions": conditions,
            "containerStatuses": container_statuses,
        });

        if !init_container_statuses.is_empty() {
            status_obj["initContainerStatuses"] = serde_json::json!(init_container_statuses);
        }

        if let Some(qos) = obj.get("status").and_then(|s| s.get("qosClass")).cloned() {
            status_obj["qosClass"] = qos;
        }

        obj.insert("status".to_string(), status_obj);
    }

    let current_rv = pod_resource.resource_version;
    let updated_pod = db
        .update_resource("v1", "Pod", Some(namespace), pod_name, pod, current_rv)
        .await?;

    if let Err(e) = reconcile_endpoints_for_pod(db, pod_reader, &updated_pod.data, services).await {
        tracing::warn!(
            "Failed to reconcile endpoints after pod status update: {}",
            e
        );
    }

    Ok(())
}
