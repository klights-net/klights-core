use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::kubelet::pod_runtime::service::PodRuntimeKey;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartupFinalizationAction {
    RunFinalizers,
    AlreadyFinalized,
}

/// Probe lifecycle port used by startup finalization and stop cleanup.
#[async_trait::async_trait]
pub trait ProbeRuntime: Send + Sync {
    /// Record a visible started sandbox and decide whether startup
    /// finalizers should run for it.
    async fn record_started_sandbox(
        &self,
        key: &PodRuntimeKey,
        sandbox_id: &str,
    ) -> anyhow::Result<StartupFinalizationAction>;

    /// Start probes for a running pod.
    async fn start_probes(
        &self,
        key: &PodRuntimeKey,
        sandbox_id: &str,
        pod: &serde_json::Value,
    ) -> anyhow::Result<()>;

    /// Mark startup finalizers as completed for a started sandbox.
    async fn mark_started_sandbox_finalized(
        &self,
        key: &PodRuntimeKey,
        sandbox_id: &str,
    ) -> anyhow::Result<()>;

    /// Stop probes for a terminating pod.
    async fn stop_probes(&self, key: &PodRuntimeKey) -> anyhow::Result<()>;
}

// --- Production adapter ---

#[derive(Clone, Debug)]
struct StartedSandboxFinalization {
    sandbox_id: String,
    finalized: bool,
}

/// Production probe runtime adapter wrapping `ProbeManager`.
pub struct RealProbeRuntime {
    probe_manager: Arc<crate::kubelet::ProbeManager>,
    started_sandboxes: Mutex<HashMap<PodRuntimeKey, StartedSandboxFinalization>>,
}

impl RealProbeRuntime {
    pub fn new(probe_manager: Arc<crate::kubelet::ProbeManager>) -> Self {
        Self {
            probe_manager,
            started_sandboxes: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait::async_trait]
impl ProbeRuntime for RealProbeRuntime {
    async fn record_started_sandbox(
        &self,
        key: &PodRuntimeKey,
        sandbox_id: &str,
    ) -> anyhow::Result<StartupFinalizationAction> {
        let mut started_sandboxes = self.started_sandboxes.lock().unwrap();
        match started_sandboxes.get_mut(key) {
            Some(existing) if existing.sandbox_id == sandbox_id && existing.finalized => {
                Ok(StartupFinalizationAction::AlreadyFinalized)
            }
            Some(existing) => {
                existing.sandbox_id = sandbox_id.to_string();
                existing.finalized = false;
                Ok(StartupFinalizationAction::RunFinalizers)
            }
            None => {
                started_sandboxes.insert(
                    key.clone(),
                    StartedSandboxFinalization {
                        sandbox_id: sandbox_id.to_string(),
                        finalized: false,
                    },
                );
                Ok(StartupFinalizationAction::RunFinalizers)
            }
        }
    }

    async fn start_probes(
        &self,
        _key: &PodRuntimeKey,
        _sandbox_id: &str,
        pod: &serde_json::Value,
    ) -> anyhow::Result<()> {
        self.probe_manager.start_probes(pod).await
    }

    async fn mark_started_sandbox_finalized(
        &self,
        key: &PodRuntimeKey,
        sandbox_id: &str,
    ) -> anyhow::Result<()> {
        if let Some(existing) = self.started_sandboxes.lock().unwrap().get_mut(key)
            && existing.sandbox_id == sandbox_id
        {
            existing.finalized = true;
        }
        Ok(())
    }

    async fn stop_probes(&self, key: &PodRuntimeKey) -> anyhow::Result<()> {
        self.probe_manager
            .stop_probes_for_uid(&key.namespace, &key.name, &key.uid)
            .await;
        self.started_sandboxes.lock().unwrap().remove(key);
        Ok(())
    }
}
