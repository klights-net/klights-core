use crate::pod_status_builders::canonical_cri_timestamp_from_ns;
use crate::pod_termination::{find_pod_container_spec, termination_message_policy};
use crate::runtime::cri::{ContainerRuntimeState, CriRuntime};
use crate::runtime::filesystem::PodFilesystem;
use crate::runtime_types::PodRuntimeKey;

#[derive(Clone, Debug)]
pub(crate) struct ReconcileContainerInfo {
    container_id: String,
    state: ContainerRuntimeState,
    exit_code: i32,
    started_at: i64,
    finished_at: i64,
    created_at: i64,
    image: String,
    image_ref: String,
    termination_message: String,
}

pub(crate) async fn runtime_state_from_container_status(
    cri: &dyn CriRuntime,
    container_id: &str,
) -> anyhow::Result<Option<ContainerRuntimeState>> {
    let response = match cri.container_status(container_id).await {
        Ok(response) => response,
        Err(error)
            if error
                .downcast_ref::<tonic::Status>()
                .is_some_and(|status| status.code() == tonic::Code::NotFound) =>
        {
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    let state = response
        .status
        .map(|status| ContainerRuntimeState::from_cri_state_i32(status.state));
    Ok(state)
}

pub(crate) async fn reconcile_container_statuses_from_pod_spec(
    cri: &dyn CriRuntime,
    filesystem: &dyn PodFilesystem,
    key: &PodRuntimeKey,
    pod: &serde_json::Value,
    observed: &[(String, ContainerRuntimeState)],
    operation_now: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<(String, Vec<serde_json::Value>)> {
    let spec_containers = pod
        .pointer("/spec/containers")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let existing_statuses = pod
        .pointer("/status/containerStatuses")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let spec_names: std::collections::HashSet<String> = spec_containers
        .iter()
        .filter_map(|container| {
            container
                .get("name")
                .and_then(|v| v.as_str())
                .map(ToString::to_string)
        })
        .collect();

    let mut infos_by_name: std::collections::HashMap<String, ReconcileContainerInfo> =
        std::collections::HashMap::new();
    for (idx, (container_id, observed_state)) in observed.iter().enumerate() {
        let status = cri.container_status(container_id).await?.status;
        let fallback_spec = spec_containers.get(idx);
        // Prefer the CRI metadata name, then the existing status entry
        // whose containerID references this container (so a CRI event
        // container is never assigned to the wrong spec container when
        // CRI omits the metadata name), then the spec index.
        let cri_name = status
            .as_ref()
            .and_then(|status| status.metadata.as_ref())
            .map(|metadata| metadata.name.as_str())
            .filter(|name| !name.is_empty());
        let existing_status_name = existing_statuses.iter().find_map(|existing| {
            let matches_id = existing
                .get("containerID")
                .and_then(|id| id.as_str())
                .map(|id| id.strip_prefix("containerd://").unwrap_or(id) == container_id)
                .unwrap_or(false);
            if matches_id {
                existing
                    .get("name")
                    .and_then(|name| name.as_str())
                    .filter(|name| !name.is_empty())
            } else {
                None
            }
        });
        let spec_index_name = fallback_spec
            .and_then(|container| container.get("name").and_then(|name| name.as_str()));
        let container_name = cri_name
            .or(existing_status_name)
            .or(spec_index_name)
            .unwrap_or("");
        if container_name.is_empty() || !spec_names.contains(container_name) {
            continue;
        }

        let image = status
            .as_ref()
            .and_then(|status| status.image.as_ref())
            .map(|image| image.image.as_str())
            .filter(|image| !image.is_empty())
            .or_else(|| {
                fallback_spec
                    .and_then(|container| container.get("image").and_then(|image| image.as_str()))
            })
            .unwrap_or("nginx:latest")
            .to_string();
        let image_ref = status
            .as_ref()
            .map(|status| {
                if !status.image_ref.is_empty() {
                    status.image_ref.clone()
                } else if !status.image_id.is_empty() {
                    status.image_id.clone()
                } else {
                    image.clone()
                }
            })
            .unwrap_or_else(|| image.clone());
        let state = *observed_state;
        let termination_message = match status.as_ref() {
            Some(status) if !status.message.is_empty() => status.message.clone(),
            _ if state == ContainerRuntimeState::Exited => {
                read_termination_message_for_container(
                    filesystem,
                    key,
                    pod,
                    container_name,
                    status.as_ref().map(|status| status.exit_code).unwrap_or(0),
                )
                .await
            }
            _ => String::new(),
        };
        let info = ReconcileContainerInfo {
            container_id: container_id.clone(),
            state,
            exit_code: status.as_ref().map(|status| status.exit_code).unwrap_or(0),
            started_at: status.as_ref().map(|status| status.started_at).unwrap_or(0),
            finished_at: status
                .as_ref()
                .map(|status| status.finished_at)
                .unwrap_or(0),
            created_at: status
                .as_ref()
                .map(|status| status.created_at)
                .unwrap_or(idx as i64),
            image,
            image_ref,
            termination_message,
        };

        match infos_by_name.get(container_name) {
            Some(existing) if existing.created_at > info.created_at => {}
            _ => {
                infos_by_name.insert(container_name.to_string(), info);
            }
        }
    }

    let container_statuses = build_reconciled_container_statuses(
        &spec_containers,
        &existing_statuses,
        &infos_by_name,
        operation_now,
    );
    let phase = compute_reconciled_phase(&spec_containers, &infos_by_name, pod);
    Ok((phase, container_statuses))
}

async fn read_termination_message_for_container(
    filesystem: &dyn PodFilesystem,
    key: &PodRuntimeKey,
    pod: &serde_json::Value,
    container_name: &str,
    exit_code: i32,
) -> String {
    let container_spec = find_pod_container_spec(pod, container_name);
    let policy = termination_message_policy(container_spec);
    filesystem
        .read_termination_message(key, container_name, policy, exit_code)
        .await
}

fn build_reconciled_container_statuses(
    spec_containers: &[serde_json::Value],
    existing_statuses: &[serde_json::Value],
    infos_by_name: &std::collections::HashMap<String, ReconcileContainerInfo>,
    operation_now: chrono::DateTime<chrono::Utc>,
) -> Vec<serde_json::Value> {
    spec_containers
        .iter()
        .filter_map(|container| {
            let container_name = container.get("name").and_then(|v| v.as_str())?;
            let image = container
                .get("image")
                .and_then(|v| v.as_str())
                .unwrap_or("nginx:latest");
            let existing = existing_statuses
                .iter()
                .find(|status| status.get("name").and_then(|v| v.as_str()) == Some(container_name));
            let has_readiness_probe = container.get("readinessProbe").is_some();
            let info = infos_by_name.get(container_name);
            let running = info.map(|info| info.state.is_running()).unwrap_or(false);
            let ready = running
                && if !has_readiness_probe {
                    true
                } else {
                    existing
                        .and_then(|status| status.get("ready"))
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false)
                };
            let started = info.map(|info| info.state.has_started()).unwrap_or(false);
            let state_obj = match info {
                Some(info) if info.state == ContainerRuntimeState::Running => {
                    let started_at = if info.started_at > 0 {
                        canonical_cri_timestamp_from_ns(info.started_at, operation_now)
                    } else {
                        existing
                            .and_then(|status| status.pointer("/state/running/startedAt"))
                            .and_then(|value| value.as_str())
                            .map(ToString::to_string)
                            .unwrap_or_else(|| {
                                klights_cluster_core::k8s_time::format_legacy_timestamp(
                                    operation_now,
                                )
                            })
                    };
                    serde_json::json!({ "running": { "startedAt": started_at } })
                }
                Some(info) if info.state == ContainerRuntimeState::Exited => {
                    let mut terminated = serde_json::json!({
                        "exitCode": info.exit_code,
                        "reason": if info.exit_code == 0 { "Completed" } else { "Error" },
                        "startedAt": canonical_cri_timestamp_from_ns(info.started_at, operation_now),
                        "finishedAt": canonical_cri_timestamp_from_ns(info.finished_at, operation_now),
                    });
                    if !info.termination_message.is_empty() {
                        terminated["message"] = serde_json::json!(info.termination_message.clone());
                    }
                    serde_json::json!({ "terminated": terminated })
                }
                _ => serde_json::json!({ "waiting": { "reason": "ContainerCreating" } }),
            };
            let mut status = serde_json::json!({
                "name": container_name,
                "containerID": info
                    .map(|info| serde_json::json!(format!("containerd://{}", info.container_id)))
                    .unwrap_or(serde_json::Value::Null),
                "ready": ready,
                "started": started,
                "restartCount": existing
                    .and_then(|status| status.get("restartCount"))
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0),
                "state": state_obj,
                "image": info.map(|info| info.image.as_str()).unwrap_or(image),
                "imageID": info.map(|info| info.image_ref.as_str()).unwrap_or(image),
            });
            if let Some(last_state) = existing.and_then(|status| status.get("lastState"))
                && let Some(obj) = status.as_object_mut()
            {
                obj.insert("lastState".to_string(), last_state.clone());
            }
            Some(status)
        })
        .collect()
}

pub(crate) fn compute_reconciled_phase(
    spec_containers: &[serde_json::Value],
    infos_by_name: &std::collections::HashMap<String, ReconcileContainerInfo>,
    pod: &serde_json::Value,
) -> String {
    let restart_policy = pod
        .pointer("/spec/restartPolicy")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    crate::pod_status_logic::canonical_container_phase(
        spec_containers.iter().map(|container| {
            let info = container
                .get("name")
                .and_then(|value| value.as_str())
                .and_then(|name| infos_by_name.get(name));
            let state = match info.map(|info| info.state) {
                Some(ContainerRuntimeState::Running) => 1,
                Some(ContainerRuntimeState::Exited) => 2,
                _ => 0,
            };
            (state, info.map(|info| info.exit_code).unwrap_or(0))
        }),
        restart_policy,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconciled_phase_is_stale_if_container_exits_nonzero() {
        let pod = serde_json::json!({
            "spec": {
                "restartPolicy": "OnFailure",
                "containers": [
                    { "name": "app" }
                ]
            }
        });
        let mut infos = std::collections::HashMap::new();
        infos.insert(
            "app".to_string(),
            ReconcileContainerInfo {
                container_id: "cid".to_string(),
                state: ContainerRuntimeState::Exited,
                exit_code: 1,
                started_at: 0,
                finished_at: 0,
                created_at: 0,
                image: "nginx:latest".to_string(),
                image_ref: "nginx:latest".to_string(),
                termination_message: String::new(),
            },
        );
        let containers = pod
            .pointer("/spec/containers")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let phase = compute_reconciled_phase(&containers, &infos, &pod);
        assert_eq!(phase, "Running");
    }

    #[test]
    fn reconciled_phase_returns_failed_when_never_and_nonzero_exit() {
        // Kubernetes phase contract: a Pod with spec.restartPolicy=Never whose
        // container terminates with a non-zero exit code transitions to phase
        // "Failed". This is required for the Conformance test
        // "[sig-node] Container Runtime blackbox test on terminated container
        //  should report termination message from log output if
        //  TerminationMessagePolicy FallbackToLogsOnError is set".
        let pod = serde_json::json!({
            "spec": {
                "restartPolicy": "Never",
                "containers": [
                    { "name": "app" }
                ]
            }
        });
        let mut infos = std::collections::HashMap::new();
        infos.insert(
            "app".to_string(),
            ReconcileContainerInfo {
                container_id: "cid".to_string(),
                state: ContainerRuntimeState::Exited,
                exit_code: 1,
                started_at: 0,
                finished_at: 0,
                created_at: 0,
                image: "nginx:latest".to_string(),
                image_ref: "nginx:latest".to_string(),
                termination_message: String::new(),
            },
        );
        let containers = pod
            .pointer("/spec/containers")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let phase = compute_reconciled_phase(&containers, &infos, &pod);
        assert_eq!(phase, "Failed");
    }

    #[test]
    fn reconciled_phase_returns_failed_when_never_and_mixed_exits_has_nonzero() {
        // Even when some containers exited 0, a Never pod with any non-zero
        // container exit and no running container must be "Failed".
        let pod = serde_json::json!({
            "spec": {
                "restartPolicy": "Never",
                "containers": [
                    { "name": "app" },
                    { "name": "sidecar" },
                ]
            }
        });
        let containers = pod
            .pointer("/spec/containers")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let mut infos = std::collections::HashMap::new();
        infos.insert(
            "app".to_string(),
            ReconcileContainerInfo {
                container_id: "cid-app".to_string(),
                state: ContainerRuntimeState::Exited,
                exit_code: 0,
                started_at: 0,
                finished_at: 0,
                created_at: 0,
                image: "nginx:latest".to_string(),
                image_ref: "nginx:latest".to_string(),
                termination_message: String::new(),
            },
        );
        infos.insert(
            "sidecar".to_string(),
            ReconcileContainerInfo {
                container_id: "cid-sidecar".to_string(),
                state: ContainerRuntimeState::Exited,
                exit_code: 2,
                started_at: 0,
                finished_at: 0,
                created_at: 0,
                image: "busybox:latest".to_string(),
                image_ref: "busybox:latest".to_string(),
                termination_message: String::new(),
            },
        );
        let phase = compute_reconciled_phase(&containers, &infos, &pod);
        assert_eq!(phase, "Failed");
    }

    #[test]
    fn reconciled_phase_returns_succeeded_when_all_exited_zero_never() {
        let pod = serde_json::json!({
            "spec": {
                "restartPolicy": "Never",
                "containers": [
                    { "name": "app" },
                    { "name": "sidecar" },
                ]
            }
        });
        let containers = pod
            .pointer("/spec/containers")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let mut infos = std::collections::HashMap::new();
        infos.insert(
            "app".to_string(),
            ReconcileContainerInfo {
                container_id: "cid-app".to_string(),
                state: ContainerRuntimeState::Exited,
                exit_code: 0,
                started_at: 0,
                finished_at: 0,
                created_at: 0,
                image: "nginx:latest".to_string(),
                image_ref: "nginx:latest".to_string(),
                termination_message: String::new(),
            },
        );
        infos.insert(
            "sidecar".to_string(),
            ReconcileContainerInfo {
                container_id: "cid-helper".to_string(),
                state: ContainerRuntimeState::Exited,
                exit_code: 0,
                started_at: 0,
                finished_at: 0,
                created_at: 0,
                image: "nginx:latest".to_string(),
                image_ref: "nginx:latest".to_string(),
                termination_message: String::new(),
            },
        );
        let phase = compute_reconciled_phase(&containers, &infos, &pod);
        assert_eq!(phase, "Succeeded");
    }

    #[test]
    fn container_status_projection_keeps_last_state() {
        let spec_containers = vec![serde_json::json!({
            "name": "app",
            "readinessProbe": { "periodSeconds": 1 },
        })];
        let existing_statuses = vec![serde_json::json!({
            "name": "app",
            "ready": false,
            "lastState": { "terminated": { "reason": "OOMKilled" } },
            "restartCount": 3,
            "containerID": "containerd://old-id",
            "state": {"waiting": {"reason": "ContainerCreating"}}
        })];
        let infos_by_name = [(
            "app".to_string(),
            ReconcileContainerInfo {
                container_id: "new-id".to_string(),
                state: ContainerRuntimeState::Running,
                exit_code: 0,
                started_at: 0,
                finished_at: 0,
                created_at: 100,
                image: "busybox".to_string(),
                image_ref: "busybox:latest".to_string(),
                termination_message: String::new(),
            },
        )]
        .into_iter()
        .collect();

        let statuses = build_reconciled_container_statuses(
            &spec_containers,
            &existing_statuses,
            &infos_by_name,
            chrono::DateTime::from_timestamp(1_700_000_000, 0)
                .expect("fixed status projection test timestamp"),
        );
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].get("ready"), Some(&serde_json::json!(false)));
        assert_eq!(
            statuses[0].get("containerID"),
            Some(&serde_json::json!("containerd://new-id"))
        );
        assert!(statuses[0].get("lastState").is_some());
        assert_eq!(statuses[0]["containerID"], "containerd://new-id");
    }
}
