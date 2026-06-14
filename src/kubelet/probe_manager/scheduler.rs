use crate::kubelet::lifecycle::{LifecycleCommand, RestartReason};
use crate::kubelet::pod_repository::PodReader;
use crate::kubelet::probe_manager::{exec, grpc, http, tcp};
use crate::kubelet::probes::Probe;
use anyhow::{Result, anyhow};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
use tokio::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeType {
    Readiness,
    Liveness,
    Startup,
}

pub struct ProbeTaskRuntime {
    pub task_supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
    pub pod_reader: Arc<dyn PodReader>,
    pub cri: Option<Arc<dyn crate::kubelet::pod_runtime::cri::CriRuntime>>,
    pub startup_completed: Arc<RwLock<HashSet<String>>>,
    pub lifecycle_tx: mpsc::Sender<LifecycleCommand>,
}

pub struct ProbeTaskTiming {
    pub initial_delay_secs: u64,
    pub interval_secs: u64,
    pub timeout_secs: u64,
    pub failure_threshold: u32,
    pub success_threshold: u32,
}

pub struct ProbeTaskSpec {
    pub pod_key: String,
    pub pod_uid: String,
    pub container_name: String,
    pub container_id: String,
    pub pod_ip: String,
    pub probe: Probe,
    pub timing: ProbeTaskTiming,
    pub probe_type: ProbeType,
    pub has_startup_probe: bool,
}

fn container_started_at(
    statuses: &[serde_json::Value],
    container_name: &str,
) -> Option<chrono::DateTime<chrono::Utc>> {
    statuses
        .iter()
        .find(|status| status.get("name").and_then(|n| n.as_str()) == Some(container_name))
        .and_then(|status| status.pointer("/state/running/startedAt"))
        .and_then(|started_at| started_at.as_str())
        .and_then(|started_at| chrono::DateTime::parse_from_rfc3339(started_at).ok())
        .map(|started_at| started_at.with_timezone(&chrono::Utc))
}

fn probe_initial_delay_elapsed(
    statuses: &[serde_json::Value],
    container_name: &str,
    initial_delay_secs: u64,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    if initial_delay_secs == 0 {
        return true;
    }

    let Some(started_at) = container_started_at(statuses, container_name) else {
        return false;
    };

    now.signed_duration_since(started_at) >= chrono::Duration::seconds(initial_delay_secs as i64)
}

fn container_ready_status(statuses: &[serde_json::Value], container_name: &str) -> Option<bool> {
    statuses
        .iter()
        .find(|status| status.get("name").and_then(|n| n.as_str()) == Some(container_name))
        .and_then(|status| status.get("ready"))
        .and_then(|ready| ready.as_bool())
}

pub async fn spawn_probe_task_with_params(
    runtime: ProbeTaskRuntime,
    spec: ProbeTaskSpec,
) -> Result<crate::task_supervisor::SupervisedJoinHandle<()>> {
    let ProbeTaskRuntime {
        task_supervisor,
        pod_reader,
        cri,
        startup_completed,
        lifecycle_tx,
    } = runtime;
    let ProbeTaskSpec {
        pod_key,
        pod_uid,
        container_name,
        container_id,
        pod_ip,
        probe,
        timing,
        probe_type,
        has_startup_probe,
    } = spec;
    let ProbeTaskTiming {
        initial_delay_secs,
        interval_secs,
        timeout_secs,
        failure_threshold,
        success_threshold,
    } = timing;
    let mut split = pod_key.splitn(2, '/');
    let namespace = split.next().unwrap_or("").to_string();
    let pod_name = split.next().unwrap_or("").to_string();

    let startup_gate_key = if pod_uid.is_empty() {
        format!("{}/{}", pod_key, container_name)
    } else {
        format!("{}/{}/{}", pod_key, pod_uid, container_name)
    };

    let task_supervisor_for_probe = task_supervisor.clone();
    task_supervisor
        .spawn_async(
            crate::task_supervisor::TaskCategory::PodProbe,
            format!("probe_task_{probe_type:?}_{pod_key}_{container_name}"),
            async move {
                let http_client = crate::kubelet::probes::build_probe_http_client().ok();

                let interval_duration = Duration::from_secs(interval_secs);
                let mut consecutive_failures = 0u32;
                let mut consecutive_successes = 0u32;
                let mut container_id = container_id;
                let mut first_iteration = true;

                loop {
                    if first_iteration {
                        first_iteration = false;
                    } else if let Err(err) = task_supervisor_for_probe
                        .sleep("probe_periodic_interval", interval_duration)
                        .await
                    {
                        tracing::debug!("probe periodic timer interrupted: {err}");
                        break;
                    }

                    if has_startup_probe
                        && probe_type != ProbeType::Startup
                        && !startup_completed.read().await.contains(&startup_gate_key)
                    {
                        continue;
                    }

                    let res = match pod_reader.get_pod(&namespace, &pod_name).await {
                        Ok(Some(res)) => res,
                        Ok(None) => {
                            tracing::debug!(
                                "stopping probe task for deleted pod {}/{}",
                                namespace,
                                pod_name
                            );
                            break;
                        }
                        Err(err) => {
                            tracing::debug!(
                                "probe task could not read pod {}/{}: {err}",
                                namespace,
                                pod_name
                            );
                            continue;
                        }
                    };

                    if !pod_uid.is_empty() {
                        let current_uid = res
                            .data
                            .pointer("/metadata/uid")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        if current_uid != pod_uid {
                            tracing::debug!(
                                "stopping stale probe task for {}/{} uid={} current_uid={}",
                                namespace,
                                pod_name,
                                pod_uid,
                                current_uid
                            );
                            break;
                        }
                    }

                    if let Some(statuses) = res
                        .data
                        .pointer("/status/containerStatuses")
                        .and_then(|s| s.as_array())
                    {
                        if !probe_initial_delay_elapsed(
                            statuses,
                            &container_name,
                            initial_delay_secs,
                            chrono::Utc::now(),
                        ) {
                            continue;
                        }

                        for status in statuses {
                            if status.get("name").and_then(|n| n.as_str()) == Some(&container_name)
                                && let Some(cid) =
                                    status.get("containerID").and_then(|c| c.as_str())
                                {
                                    let new_id = cid.strip_prefix("containerd://").unwrap_or(cid);
                                    if !new_id.is_empty() && new_id != container_id {
                                        container_id = new_id.to_string();
                                    }
                                }
                        }
                    } else if initial_delay_secs > 0 {
                        continue;
                    }

                    let timeout = Duration::from_secs(timeout_secs);
                    let success = match &probe {
                        Probe::Http(http_probe) => {
                            http::check_http_probe(
                                http_client.as_ref(),
                                &pod_ip,
                                http_probe,
                                timeout,
                            )
                            .await
                        }
                        Probe::Tcp(tcp_probe) => {
                            tcp::check_tcp_probe(
                                &pod_ip,
                                tcp_probe,
                                timeout,
                                task_supervisor_for_probe.as_ref(),
                            )
                            .await
                        }
                        Probe::Grpc(grpc_probe) => {
                            grpc::check_grpc_probe(
                                &pod_ip,
                                grpc_probe,
                                timeout,
                                task_supervisor_for_probe.as_ref(),
                            )
                            .await
                        }
                        Probe::Exec(exec_probe) => {
                            if let Some(cri) = cri.as_ref() {
                                exec::check_exec_probe(
                                    cri.as_ref(),
                                    &container_id,
                                    exec_probe,
                                    timeout_secs,
                                )
                                .await
                            } else {
                                false
                            }
                        }
                    };

                    if success {
                        consecutive_failures = 0;
                        consecutive_successes += 1;
                    } else {
                        consecutive_successes = 0;
                        consecutive_failures += 1;
                    }

                    match probe_type {
                        ProbeType::Startup => {
                            if consecutive_successes >= success_threshold {
                                startup_completed
                                    .write()
                                    .await
                                    .insert(startup_gate_key.clone());
                                let _ = lifecycle_tx
                                    .send(LifecycleCommand::StartupPassed {
                                        pod_uid: pod_uid.clone(),
                                        namespace: namespace.clone(),
                                        pod_name: pod_name.clone(),
                                        container_name: container_name.clone(),
                                    })
                                    .await;
                                break;
                            }

                            if consecutive_failures >= failure_threshold {
                                let _ = lifecycle_tx
                                    .send(LifecycleCommand::RestartRequested {
                                        pod_uid: pod_uid.clone(),
                                        namespace: namespace.clone(),
                                        pod_name: pod_name.clone(),
                                        container_name: container_name.clone(),
                                        reason: RestartReason::StartupProbe,
                                    })
                                    .await;
                                consecutive_failures = 0;
                                consecutive_successes = 0;
                            }
                        }
                        ProbeType::Readiness => {
                            let is_ready = consecutive_successes >= success_threshold;
                            let current_ready = res
                                .data
                                .pointer("/status/containerStatuses")
                                .and_then(|s| s.as_array())
                                .and_then(|statuses| {
                                    container_ready_status(statuses, &container_name)
                                });
                            if current_ready == Some(is_ready) {
                                tracing::debug!(
                                    target: "klights::probe",
                                    namespace = %namespace,
                                    pod = %pod_name,
                                    container = %container_name,
                                    ready = is_ready,
                                    "readiness probe result matches current pod status; skipping lifecycle command"
                                );
                                continue;
                            }
                            let _ = lifecycle_tx
                                .send(LifecycleCommand::ReadinessChanged {
                                    pod_uid: pod_uid.clone(),
                                    namespace: namespace.clone(),
                                    pod_name: pod_name.clone(),
                                    container_name: container_name.clone(),
                                    ready: is_ready,
                                })
                                .await;
                        }
                        ProbeType::Liveness => {
                            if consecutive_failures >= failure_threshold {
                                let _ = lifecycle_tx
                                    .send(LifecycleCommand::RestartRequested {
                                        pod_uid: pod_uid.clone(),
                                        namespace: namespace.clone(),
                                        pod_name: pod_name.clone(),
                                        container_name: container_name.clone(),
                                        reason: RestartReason::LivenessProbe,
                                    })
                                    .await;
                                consecutive_failures = 0;
                                consecutive_successes = 0;
                            }
                        }
                    }
                }
            },
        )
        .await
        .map_err(|e| anyhow!("failed to spawn probe task: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kubelet::probes::TcpProbe;
    use serde_json::json;
    use std::sync::Arc;

    #[test]
    fn initial_delay_elapsed_uses_container_started_at() {
        let statuses = vec![json!({
            "name": "app",
            "state": {
                "running": {
                    "startedAt": "2026-05-01T05:12:39Z"
                }
            }
        })];
        let before = chrono::DateTime::parse_from_rfc3339("2026-05-01T05:12:53Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let at_delay = chrono::DateTime::parse_from_rfc3339("2026-05-01T05:12:54Z")
            .unwrap()
            .with_timezone(&chrono::Utc);

        assert!(
            !probe_initial_delay_elapsed(&statuses, "app", 15, before),
            "probe must stay disabled before the container has run for initialDelaySeconds"
        );
        assert!(
            probe_initial_delay_elapsed(&statuses, "app", 15, at_delay),
            "probe may run once the container has run for initialDelaySeconds"
        );
    }

    #[test]
    fn initial_delay_without_running_status_keeps_probe_disabled() {
        let statuses = vec![json!({
            "name": "app",
            "state": {"waiting": {"reason": "ContainerCreating"}}
        })];
        let now = chrono::DateTime::parse_from_rfc3339("2026-05-01T05:12:54Z")
            .unwrap()
            .with_timezone(&chrono::Utc);

        assert!(
            !probe_initial_delay_elapsed(&statuses, "app", 15, now),
            "kubelet does not probe until the target container has a running startedAt"
        );
        assert!(
            probe_initial_delay_elapsed(&statuses, "app", 0, now),
            "zero initialDelaySeconds does not impose a startedAt timing gate"
        );
    }

    #[tokio::test]
    async fn probe_task_exits_without_command_when_pod_uid_changes() {
        let db = crate::datastore::test_support::in_memory().await;
        let pod_reader: Arc<dyn PodReader> =
            crate::controllers::test_utils::pod_repository_for_test(&db);

        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "probed",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "probed",
                    "uid": "new-uid"
                },
                "spec": {"containers": [{"name": "app", "image": "registry.k8s.io/pause:3.10.1"}]},
                "status": {
                    "phase": "Running",
                    "podIP": "127.0.0.1",
                    "conditions": [{"type": "Ready", "status": "True"}],
                    "containerStatuses": [{
                        "name": "app",
                        "containerID": "containerd://new-container",
                        "ready": true,
                        "state": {"running": {"startedAt": "2026-05-01T05:12:39Z"}}
                    }]
                }
            }),
        )
        .await
        .unwrap();

        let stored = pod_reader
            .get_pod("default", "probed")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stored
                .data
                .pointer("/metadata/uid")
                .and_then(|v| v.as_str()),
            Some("new-uid")
        );

        let supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let startup_completed = Arc::new(RwLock::new(HashSet::new()));
        let (tx, mut rx) = mpsc::channel(4);

        let handle = spawn_probe_task_with_params(
            ProbeTaskRuntime {
                task_supervisor: supervisor,
                pod_reader,
                cri: None,
                startup_completed,
                lifecycle_tx: tx,
            },
            ProbeTaskSpec {
                pod_key: "default/probed".to_string(),
                pod_uid: "old-uid".to_string(),
                container_name: "app".to_string(),
                container_id: "old-container".to_string(),
                pod_ip: "127.0.0.1".to_string(),
                probe: Probe::Tcp(TcpProbe { port: 9 }),
                timing: ProbeTaskTiming {
                    initial_delay_secs: 0,
                    interval_secs: 1,
                    timeout_secs: 1,
                    failure_threshold: 1,
                    success_threshold: 1,
                },
                probe_type: ProbeType::Readiness,
                has_startup_probe: false,
            },
        )
        .await
        .unwrap();

        let received = tokio::time::timeout(Duration::from_millis(150), rx.recv()).await;
        handle.abort();

        assert!(
            matches!(received, Ok(None) | Err(_)),
            "stale probe task must exit without sending a lifecycle command"
        );
    }

    #[tokio::test]
    async fn readiness_probe_does_not_emit_command_when_status_is_already_ready() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accept_task = tokio::spawn(async move { while listener.accept().await.is_ok() {} });

        let db = crate::datastore::test_support::in_memory().await;
        let pod_reader: Arc<dyn PodReader> =
            crate::controllers::test_utils::pod_repository_for_test(&db);

        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "probed",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "probed",
                    "uid": "uid-ready"
                },
                "spec": {"containers": [{"name": "app", "image": "registry.k8s.io/pause:3.10.1"}]},
                "status": {
                    "phase": "Running",
                    "podIP": "127.0.0.1",
                    "conditions": [{"type": "Ready", "status": "True"}],
                    "containerStatuses": [{
                        "name": "app",
                        "containerID": "containerd://ready-container",
                        "ready": true,
                        "state": {"running": {"startedAt": "2026-05-01T05:12:39Z"}}
                    }]
                }
            }),
        )
        .await
        .unwrap();

        let supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let startup_completed = Arc::new(RwLock::new(HashSet::new()));
        let (tx, mut rx) = mpsc::channel(4);

        let handle = spawn_probe_task_with_params(
            ProbeTaskRuntime {
                task_supervisor: supervisor,
                pod_reader,
                cri: None,
                startup_completed,
                lifecycle_tx: tx,
            },
            ProbeTaskSpec {
                pod_key: "default/probed".to_string(),
                pod_uid: "uid-ready".to_string(),
                container_name: "app".to_string(),
                container_id: "ready-container".to_string(),
                pod_ip: "127.0.0.1".to_string(),
                probe: Probe::Tcp(TcpProbe { port }),
                timing: ProbeTaskTiming {
                    initial_delay_secs: 0,
                    interval_secs: 60,
                    timeout_secs: 1,
                    failure_threshold: 1,
                    success_threshold: 1,
                },
                probe_type: ProbeType::Readiness,
                has_startup_probe: false,
            },
        )
        .await
        .unwrap();

        let received = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await;
        handle.abort();
        accept_task.abort();

        assert!(
            matches!(received, Ok(None) | Err(_)),
            "readiness probe should not send an unchanged ready=true signal"
        );
    }
}
