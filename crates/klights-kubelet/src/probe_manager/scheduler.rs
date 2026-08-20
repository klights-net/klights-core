use crate::lifecycle::{LifecycleCommand, RestartReason};
use crate::probe_manager::{exec, grpc, http, tcp};
use crate::probes::Probe;
use anyhow::{Result, anyhow};
use klights_pod_api::{PodGetRequest, PodQuery};
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
    pub task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    pub pod_reader: Arc<dyn PodQuery>,
    pub cri: Option<Arc<dyn crate::runtime::cri::CriRuntime>>,
    pub startup_completed: Arc<RwLock<HashSet<String>>>,
    pub lifecycle_tx: mpsc::Sender<LifecycleCommand>,
    pub wall_clock: Arc<dyn crate::runtime_clock::RuntimeClock>,
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
) -> Result<klights_supervisor::SupervisedJoinHandle<()>> {
    let ProbeTaskRuntime {
        task_supervisor,
        pod_reader,
        cri,
        startup_completed,
        lifecycle_tx,
        wall_clock,
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
    let pod_request = PodGetRequest::try_by_name(&namespace, &pod_name)?;

    let startup_gate_key = if pod_uid.is_empty() {
        format!("{}/{}", pod_key, container_name)
    } else {
        format!("{}/{}/{}", pod_key, pod_uid, container_name)
    };

    let task_supervisor_for_probe = task_supervisor.clone();
    task_supervisor
        .spawn_async(
            klights_supervisor::TaskCategory::PodProbe,
            format!("probe_task_{probe_type:?}_{pod_key}_{container_name}"),
            async move {
                let http_client = crate::probes::build_probe_http_client().ok();

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

                    let res = match pod_reader.get_pod(pod_request.clone()).await {
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
                            wall_clock.now_utc(),
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
    use crate::probes::TcpProbe;
    use klights_pod_api::{
        PodListRequest, PodListResult, PodOwnerListRequest, PodRepositoryError, PodRepositoryFuture,
    };
    use serde_json::json;
    use std::sync::Arc;

    struct StaticPodQuery(klights_cluster_core::Resource);

    impl PodQuery for StaticPodQuery {
        fn get_pod(
            &self,
            _request: PodGetRequest,
        ) -> PodRepositoryFuture<'_, Option<klights_cluster_core::Resource>> {
            Box::pin(async { Ok(Some(self.0.clone())) })
        }

        fn list_pods(&self, _request: PodListRequest) -> PodRepositoryFuture<'_, PodListResult> {
            Box::pin(async { Err(PodRepositoryError::unavailable("unused list operation")) })
        }

        fn list_pods_by_owner_uid(
            &self,
            _request: PodOwnerListRequest,
        ) -> PodRepositoryFuture<'_, Vec<klights_cluster_core::Resource>> {
            Box::pin(async { Err(PodRepositoryError::unavailable("unused owner query")) })
        }
    }

    fn static_pod_query(pod: serde_json::Value) -> Arc<dyn PodQuery> {
        Arc::new(StaticPodQuery(
            klights_cluster_core::Resource::try_from_data(Arc::new(pod)).unwrap(),
        ))
    }

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
        let pod_reader = static_pod_query(json!({
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
        }));

        let stored = pod_reader
            .get_pod(PodGetRequest::try_by_name("default", "probed").unwrap())
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

        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
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
                wall_clock: Arc::new(crate::runtime_clock::SystemRuntimeClock),
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

        let pod_reader = static_pod_query(json!({
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
        }));

        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
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
                wall_clock: Arc::new(crate::runtime_clock::SystemRuntimeClock),
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

    #[tokio::test]
    async fn readiness_probe_retries_after_pending_status_is_published_running() {
        // The first successful probe observes a Pending/ContainerCreating
        // status. The lifecycle handler deliberately ignores that readiness
        // write until the runtime publishes Running. Once that publication is
        // visible, the next successful probe must still enqueue the same
        // ReadinessChanged(true) command; scheduler-side current_ready
        // optimization must not turn the ignored first command into a
        // permanent suppression.
        let pending = klights_cluster_core::Resource::try_from_data(Arc::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "latency-probed",
                "uid": "uid-latency-probed"
            },
            "spec": {"containers": [{"name": "app", "image": "nginx"}]},
            "status": {
                "phase": "Pending",
                "podIP": "127.0.0.1",
                "containerStatuses": [{
                    "name": "app",
                    "ready": false,
                    "state": {"waiting": {"reason": "ContainerCreating"}}
                }]
            }
        })))
        .unwrap();
        let running = klights_cluster_core::Resource::try_from_data(Arc::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "latency-probed",
                "uid": "uid-latency-probed"
            },
            "spec": {"containers": [{"name": "app", "image": "nginx"}]},
            "status": {
                "phase": "Running",
                "podIP": "127.0.0.1",
                "containerStatuses": [{
                    "name": "app",
                    "ready": false,
                    "state": {"running": {"startedAt": "2026-05-01T05:12:39Z"}}
                }]
            }
        })))
        .unwrap();

        struct LatencyPodQuery {
            pending: klights_cluster_core::Resource,
            running: klights_cluster_core::Resource,
            calls: std::sync::atomic::AtomicUsize,
            running_published: Arc<tokio::sync::Notify>,
        }

        impl PodQuery for LatencyPodQuery {
            fn get_pod(
                &self,
                _request: PodGetRequest,
            ) -> PodRepositoryFuture<'_, Option<klights_cluster_core::Resource>> {
                Box::pin(async move {
                    let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if call == 0 {
                        return Ok(Some(self.pending.clone()));
                    }
                    self.running_published.notified().await;
                    Ok(Some(self.running.clone()))
                })
            }

            fn list_pods(
                &self,
                _request: PodListRequest,
            ) -> PodRepositoryFuture<'_, PodListResult> {
                Box::pin(async { Err(PodRepositoryError::unavailable("unused list operation")) })
            }

            fn list_pods_by_owner_uid(
                &self,
                _request: PodOwnerListRequest,
            ) -> PodRepositoryFuture<'_, Vec<klights_cluster_core::Resource>> {
                Box::pin(async { Err(PodRepositoryError::unavailable("unused owner query")) })
            }
        }

        let pod_reader = Arc::new(LatencyPodQuery {
            pending,
            running,
            calls: std::sync::atomic::AtomicUsize::new(0),
            running_published: Arc::new(tokio::sync::Notify::new()),
        });
        let published = pod_reader.running_published.clone();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accept_task = tokio::spawn(async move { while listener.accept().await.is_ok() {} });

        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
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
                wall_clock: Arc::new(crate::runtime_clock::SystemRuntimeClock),
            },
            ProbeTaskSpec {
                pod_key: "default/latency-probed".to_string(),
                pod_uid: "uid-latency-probed".to_string(),
                container_name: "app".to_string(),
                container_id: "container-latency-probed".to_string(),
                pod_ip: "127.0.0.1".to_string(),
                probe: Probe::Tcp(TcpProbe { port }),
                timing: ProbeTaskTiming {
                    initial_delay_secs: 0,
                    interval_secs: 0,
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

        let early = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("early readiness command should arrive")
            .expect("probe task should remain alive after early command");
        assert!(matches!(
            early,
            LifecycleCommand::ReadinessChanged { ready: true, .. }
        ));

        // Model the delayed runtime status publication after the lifecycle
        // handler ignored the early command while the Pod was Pending.
        published.notify_one();

        let retry = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("readiness command must be retried after Running publication")
            .expect("probe task should emit the retry");
        assert!(matches!(
            retry,
            LifecycleCommand::ReadinessChanged { ready: true, .. }
        ));

        handle.abort();
        accept_task.abort();
    }
}
