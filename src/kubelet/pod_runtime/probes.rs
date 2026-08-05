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
    probe_manager: Arc<klights_kubelet::probe_manager::ProbeManager>,
    started_sandboxes: Mutex<HashMap<PodRuntimeKey, StartedSandboxFinalization>>,
}

impl RealProbeRuntime {
    pub fn new(probe_manager: Arc<klights_kubelet::probe_manager::ProbeManager>) -> Self {
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

#[cfg(test)]
pub(crate) mod test_support {
    use crate::kubelet::pod_runtime::service::PodRuntimeKey;
    use std::sync::Mutex;

    // --- MockProbeRuntime ---

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) enum MockProbeCall {
        RecordStartedSandbox {
            namespace: String,
            name: String,
            uid: String,
            sandbox_id: String,
        },
        Start {
            namespace: String,
            name: String,
            uid: String,
            sandbox_id: String,
        },
        MarkStartedSandboxFinalized {
            namespace: String,
            name: String,
            uid: String,
            sandbox_id: String,
        },
        Stop {
            namespace: String,
            name: String,
            uid: String,
        },
    }

    pub(crate) struct MockProbeRuntime {
        calls: Mutex<Vec<MockProbeCall>>,
        started_sandboxes: Mutex<std::collections::HashMap<PodRuntimeKey, (String, bool)>>,
    }

    impl Default for MockProbeRuntime {
        fn default() -> Self {
            Self::new()
        }
    }

    impl MockProbeRuntime {
        pub(crate) fn new() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                started_sandboxes: Mutex::new(std::collections::HashMap::new()),
            }
        }

        pub(crate) fn clear_calls(&self) {
            self.calls.lock().unwrap().clear();
            self.started_sandboxes.lock().unwrap().clear();
        }

        pub(crate) fn recorded_calls(&self) -> Vec<MockProbeCall> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl crate::kubelet::pod_runtime::probes::ProbeRuntime for MockProbeRuntime {
        async fn record_started_sandbox(
            &self,
            key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
            sandbox_id: &str,
        ) -> anyhow::Result<crate::kubelet::pod_runtime::probes::StartupFinalizationAction>
        {
            self.calls
                .lock()
                .unwrap()
                .push(MockProbeCall::RecordStartedSandbox {
                    namespace: key.namespace.clone(),
                    name: key.name.clone(),
                    uid: key.uid.clone(),
                    sandbox_id: sandbox_id.to_string(),
                });
            let mut started_sandboxes = self.started_sandboxes.lock().unwrap();
            match started_sandboxes.get_mut(key) {
                Some((existing_sandbox_id, finalized))
                    if existing_sandbox_id == sandbox_id && *finalized =>
                {
                    Ok(crate::kubelet::pod_runtime::probes::StartupFinalizationAction::AlreadyFinalized)
                }
                Some((existing_sandbox_id, finalized)) => {
                    *existing_sandbox_id = sandbox_id.to_string();
                    *finalized = false;
                    Ok(crate::kubelet::pod_runtime::probes::StartupFinalizationAction::RunFinalizers)
                }
                None => {
                    started_sandboxes.insert(key.clone(), (sandbox_id.to_string(), false));
                    Ok(crate::kubelet::pod_runtime::probes::StartupFinalizationAction::RunFinalizers)
                }
            }
        }

        async fn start_probes(
            &self,
            key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
            sandbox_id: &str,
            _pod: &serde_json::Value,
        ) -> anyhow::Result<()> {
            self.calls.lock().unwrap().push(MockProbeCall::Start {
                namespace: key.namespace.clone(),
                name: key.name.clone(),
                uid: key.uid.clone(),
                sandbox_id: sandbox_id.to_string(),
            });
            Ok(())
        }

        async fn mark_started_sandbox_finalized(
            &self,
            key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
            sandbox_id: &str,
        ) -> anyhow::Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(MockProbeCall::MarkStartedSandboxFinalized {
                    namespace: key.namespace.clone(),
                    name: key.name.clone(),
                    uid: key.uid.clone(),
                    sandbox_id: sandbox_id.to_string(),
                });
            if let Some((existing_sandbox_id, finalized)) =
                self.started_sandboxes.lock().unwrap().get_mut(key)
                && existing_sandbox_id == sandbox_id
            {
                *finalized = true;
            }
            Ok(())
        }

        async fn stop_probes(
            &self,
            key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
        ) -> anyhow::Result<()> {
            self.calls.lock().unwrap().push(MockProbeCall::Stop {
                namespace: key.namespace.clone(),
                name: key.name.clone(),
                uid: key.uid.clone(),
            });
            self.started_sandboxes.lock().unwrap().remove(key);
            Ok(())
        }
    }
}
