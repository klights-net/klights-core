use crate::lifecycle::LifecycleCommand;
use crate::probes::parse_probe_for_container;
use anyhow::{Context, Result};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
mod exec;
mod grpc;
mod http;
mod scheduler;
mod tcp;

pub use scheduler::ProbeType;

/// Probe parameters parsed from pod spec
struct ProbeParams {
    initial_delay: u64,
    interval_secs: u64,
    timeout_secs: u64,
    failure_threshold: u32,
    success_threshold: u32,
}

/// Parse probe timing parameters from probe spec with K8s defaults
fn parse_probe_params(probe_spec: &Value) -> ProbeParams {
    // Protobuf decode paths may materialize omitted numeric fields as 0.
    // For probes, 0 is not a valid configured value for period/timeout/thresholds,
    // so treat 0 as "unset" and apply Kubernetes defaults.
    let period = probe_spec
        .get("periodSeconds")
        .and_then(|p| p.as_u64())
        .filter(|v| *v > 0)
        .unwrap_or(10);
    let timeout = probe_spec
        .get("timeoutSeconds")
        .and_then(|t| t.as_u64())
        .filter(|v| *v > 0)
        .unwrap_or(1);
    let failure_threshold = probe_spec
        .get("failureThreshold")
        .and_then(|f| f.as_u64())
        .filter(|v| *v > 0)
        .unwrap_or(3) as u32;
    let success_threshold = probe_spec
        .get("successThreshold")
        .and_then(|s| s.as_u64())
        .filter(|v| *v > 0)
        .unwrap_or(1) as u32;

    ProbeParams {
        initial_delay: probe_spec
            .get("initialDelaySeconds")
            .and_then(|d| d.as_u64())
            .unwrap_or(0),
        interval_secs: period.max(1),
        timeout_secs: timeout.max(1),
        failure_threshold: failure_threshold.max(1),
        success_threshold: success_threshold.max(1),
    }
}

/// Manages health probe timers for all pods
pub struct ProbeManager {
    /// Map of probe task key (namespace/name/uid) to probe task handles.
    tasks: Arc<RwLock<HashMap<String, Vec<klights_supervisor::SupervisedJoinHandle<()>>>>>,
    /// Tracks readiness/liveness gating once startup probe has passed.
    /// Key format: namespace/name/uid/container when UID is known.
    startup_completed: Arc<RwLock<HashSet<String>>>,
    pod_reader: Arc<dyn klights_pod_api::PodQuery>,
    cri: Option<Arc<dyn crate::runtime::cri::CriRuntime>>,
    lifecycle_tx: mpsc::Sender<LifecycleCommand>,
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    wall_clock: Arc<dyn crate::runtime_clock::RuntimeClock>,
}

impl ProbeManager {
    pub fn new_with_lifecycle(
        task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
        pod_reader: Arc<dyn klights_pod_api::PodQuery>,
        cri: Option<Arc<dyn crate::runtime::cri::CriRuntime>>,
        lifecycle_tx: mpsc::Sender<LifecycleCommand>,
        wall_clock: Arc<dyn crate::runtime_clock::RuntimeClock>,
    ) -> Self {
        Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            startup_completed: Arc::new(RwLock::new(HashSet::new())),
            pod_reader,
            cri,
            lifecycle_tx,
            task_supervisor,
            wall_clock,
        }
    }

    /// Start probe timers for a pod
    pub async fn start_probes(&self, pod: &Value) -> Result<()> {
        let metadata = pod.get("metadata").context("Missing metadata")?;
        let spec = pod.get("spec").context("Missing spec")?;

        let namespace = metadata
            .get("namespace")
            .and_then(|n| n.as_str())
            .context("Missing namespace")?;
        let name = metadata
            .get("name")
            .and_then(|n| n.as_str())
            .context("Missing name")?;
        let pod_uid = metadata
            .get("uid")
            .and_then(|u| u.as_str())
            .unwrap_or("")
            .to_string();

        let pod_key = format!("{}/{}", namespace, name);
        let task_key = probe_task_key(namespace, name, &pod_uid);

        // Get pod IP from status
        let pod_ip = pod
            .get("status")
            .and_then(|s| s.get("podIP"))
            .and_then(|ip| ip.as_str())
            .context("Missing podIP")?
            .to_string();

        // Ensure startup gates are reset whenever probes are (re)started for a pod.
        {
            let mut startup = self.startup_completed.write().await;
            startup.retain(|k| !k.starts_with(&format!("{}/", pod_key)));
        }

        // Parse containers and their probes
        let containers = spec
            .get("containers")
            .and_then(|c| c.as_array())
            .context("Missing containers")?;

        let mut handles = vec![];

        // Get container statuses for container IDs
        let container_statuses = pod
            .get("status")
            .and_then(|s| s.get("containerStatuses"))
            .and_then(|cs| cs.as_array());

        for container in containers {
            let container_name = container
                .get("name")
                .and_then(|n| n.as_str())
                .context("Missing container name")?
                .to_string();

            let has_startup_probe = container.get("startupProbe").is_some();

            // Find container ID from status
            let container_id = container_statuses
                .and_then(|statuses| {
                    statuses
                        .iter()
                        .find(|cs| cs.get("name").and_then(|n| n.as_str()) == Some(&container_name))
                })
                .and_then(|cs| cs.get("containerID"))
                .and_then(|id| id.as_str())
                .and_then(|id| id.strip_prefix("containerd://"))
                .unwrap_or("")
                .to_string();

            // Startup probe (must run first, gates liveness/readiness)
            if let Some(startup_spec) = container.get("startupProbe")
                && let Ok(probe) = parse_probe_for_container(startup_spec, container)
            {
                let params = parse_probe_params(startup_spec);

                let handle = self
                    .spawn_probe_task_with_params(scheduler::ProbeTaskSpec {
                        pod_key: pod_key.clone(),
                        pod_uid: pod_uid.clone(),
                        container_name: container_name.clone(),
                        container_id: container_id.clone(),
                        pod_ip: pod_ip.clone(),
                        probe,
                        timing: scheduler::ProbeTaskTiming {
                            initial_delay_secs: params.initial_delay,
                            interval_secs: params.interval_secs,
                            timeout_secs: params.timeout_secs,
                            failure_threshold: params.failure_threshold,
                            success_threshold: params.success_threshold,
                        },
                        probe_type: ProbeType::Startup,
                        has_startup_probe,
                    })
                    .await?;
                handles.push(handle);
            }

            // Readiness probe
            if let Some(readiness_spec) = container.get("readinessProbe")
                && let Ok(probe) = parse_probe_for_container(readiness_spec, container)
            {
                let params = parse_probe_params(readiness_spec);

                let handle = self
                    .spawn_probe_task_with_params(scheduler::ProbeTaskSpec {
                        pod_key: pod_key.clone(),
                        pod_uid: pod_uid.clone(),
                        container_name: container_name.clone(),
                        container_id: container_id.clone(),
                        pod_ip: pod_ip.clone(),
                        probe,
                        timing: scheduler::ProbeTaskTiming {
                            initial_delay_secs: params.initial_delay,
                            interval_secs: params.interval_secs,
                            timeout_secs: params.timeout_secs,
                            failure_threshold: params.failure_threshold,
                            success_threshold: params.success_threshold,
                        },
                        probe_type: ProbeType::Readiness,
                        has_startup_probe,
                    })
                    .await?;
                handles.push(handle);
            }

            // Liveness probe
            if let Some(liveness_spec) = container.get("livenessProbe")
                && let Ok(probe) = parse_probe_for_container(liveness_spec, container)
            {
                let params = parse_probe_params(liveness_spec);

                let handle = self
                    .spawn_probe_task_with_params(scheduler::ProbeTaskSpec {
                        pod_key: pod_key.clone(),
                        pod_uid: pod_uid.clone(),
                        container_name: container_name.clone(),
                        container_id: container_id.clone(),
                        pod_ip: pod_ip.clone(),
                        probe,
                        timing: scheduler::ProbeTaskTiming {
                            initial_delay_secs: params.initial_delay,
                            interval_secs: params.interval_secs,
                            timeout_secs: params.timeout_secs,
                            failure_threshold: params.failure_threshold,
                            success_threshold: params.success_threshold,
                        },
                        probe_type: ProbeType::Liveness,
                        has_startup_probe,
                    })
                    .await?;
                handles.push(handle);
            }
        }

        // Store handles
        let mut tasks = self.tasks.write().await;
        if let Some(old_handles) = tasks.insert(task_key.clone(), handles) {
            for handle in old_handles {
                handle.abort();
            }
        }

        tracing::debug!("Started probe timers for pod {}", task_key);
        Ok(())
    }

    /// Stop probe timers for a pod
    pub async fn stop_probes(&self, namespace: &str, name: &str) {
        let pod_key = format!("{}/{}", namespace, name);

        let mut tasks = self.tasks.write().await;
        let prefix = format!("{}/", pod_key);
        let keys: Vec<String> = tasks
            .keys()
            .filter(|key| *key == &pod_key || key.starts_with(&prefix))
            .cloned()
            .collect();
        for key in keys {
            if let Some(handles) = tasks.remove(&key) {
                for handle in handles {
                    handle.abort();
                }
            }
        }
        tracing::debug!("Stopped probe timers for pod {}", pod_key);
        let mut startup = self.startup_completed.write().await;
        startup.retain(|k| !k.starts_with(&format!("{}/", pod_key)));
    }

    pub async fn stop_probes_for_uid(&self, namespace: &str, name: &str, uid: &str) {
        if uid.is_empty() {
            self.stop_probes(namespace, name).await;
            return;
        }

        let task_key = probe_task_key(namespace, name, uid);
        let mut tasks = self.tasks.write().await;
        if let Some(handles) = tasks.remove(&task_key) {
            for handle in handles {
                handle.abort();
            }
            tracing::debug!("Stopped probe timers for pod {}", task_key);
        }
        let mut startup = self.startup_completed.write().await;
        startup.retain(|k| !k.starts_with(&format!("{}/{}/{}/", namespace, name, uid)));
    }

    async fn spawn_probe_task_with_params(
        &self,
        spec: scheduler::ProbeTaskSpec,
    ) -> Result<klights_supervisor::SupervisedJoinHandle<()>> {
        scheduler::spawn_probe_task_with_params(
            scheduler::ProbeTaskRuntime {
                task_supervisor: self.task_supervisor.clone(),
                pod_reader: self.pod_reader.clone(),
                cri: self.cri.clone(),
                startup_completed: self.startup_completed.clone(),
                lifecycle_tx: self.lifecycle_tx.clone(),
                wall_clock: self.wall_clock.clone(),
            },
            spec,
        )
        .await
    }

    #[cfg(any(test, feature = "test-support"))]
    pub async fn task_count_for_test(&self, key: &str) -> Option<usize> {
        self.tasks.read().await.get(key).map(Vec::len)
    }
}

fn probe_task_key(namespace: &str, name: &str, uid: &str) -> String {
    if uid.is_empty() {
        format!("{}/{}", namespace, name)
    } else {
        format!("{}/{}/{}", namespace, name, uid)
    }
}

#[cfg(test)]
mod tests;
