#![cfg(any(test, feature = "test-support"))]
use k8s_cri::v1::{ContainerConfig, PodSandboxConfig};
use serde_json::{Value, json};
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use tokio_util::sync::CancellationToken;

use super::cri::{CriRuntimeContainerEvent, CriRuntimeContainerEventKind};

type MockSandboxRecord = (String, String, String, String, String);

/// Minimal valid Pod spec for unit tests. Has metadata + one container.
/// Defaults `spec.nodeName` to "test-node" so it passes node-ownership
/// checks against the default harness FakeNode.
pub fn pod_json(ns: &str, name: &str, uid: &str, image: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": ns,
            "name": name,
            "uid": uid,
            "resourceVersion": "1"
        },
        "spec": {
            "containers": [{"name": "app", "image": image}],
            "nodeName": "test-node"
        },
        "status": {"phase": "Pending"}
    })
}

/// Pod with `spec.nodeName` already set (Task 11-12 multi-node tests).
pub fn scheduled_pod_json(ns: &str, name: &str, uid: &str, node_name: &str) -> Value {
    let mut p = pod_json(ns, name, uid, "nginx:1.25");
    p["spec"]["nodeName"] = json!(node_name);
    p
}

/// Pod with deletionTimestamp set + recorded sandbox (Task 9 stop tests).
pub fn terminating_pod_json(ns: &str, name: &str, uid: &str, sandbox_id: &str) -> Value {
    let mut p = pod_json(ns, name, uid, "nginx:1.25");
    p["metadata"]["deletionTimestamp"] = json!("2026-01-01T00:00:00Z");
    p["status"]["sandboxId"] = json!(sandbox_id);
    p
}

// --- MockCriRuntime ---

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MockCriOperation {
    ImageStatus(String),
    PullImage(String),
    RunPodSandbox,
    StopPodSandbox(String),
    RemovePodSandbox(String),
    ListPodSandboxes(Option<String>),
    CreateContainer {
        sandbox_id: String,
        container_name: String,
    },
    StartContainer(String),
    StopContainer(String, i64),
    RemoveContainer(String),
    ContainerStatus(String),
    ExecSync {
        container_id: String,
        command: Vec<String>,
        timeout_seconds: i64,
    },
    SubscribeContainerEvents,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MockCriCall {
    pub operation: MockCriOperation,
    pub call_order: u64,
}

/// Recording mock for the CRI runtime port.
/// Operations succeed by default; inject failures via `fail_operation`.
pub struct MockCriRuntime {
    calls: Mutex<Vec<MockCriCall>>,
    call_count: Mutex<u64>,
    fail_operation: Mutex<Option<String>>,
    sandbox_counter: Mutex<u64>,
    image_present: Mutex<bool>,
    /// If set, `run_pod_sandbox` cancels this token after recording.
    start_pod_cancel: Mutex<Option<CancellationToken>>,
    /// Exit code reported by `container_status` when no per-container status
    /// has been configured for the queried container id.
    container_exit_code: Mutex<i32>,
    /// State reported by `container_status` when no per-container status
    /// has been configured for the queried container id.
    container_status_state: Mutex<i32>,
    /// Exit code reported by `exec_sync`.
    exec_exit_code: Mutex<i32>,
    /// Pod sandboxes returned by the CRI fallback path.
    pod_sandboxes: Mutex<Vec<MockSandboxRecord>>,
    /// Recorded ContainerConfig from create_container calls.
    create_configs: Mutex<Vec<ContainerConfig>>,
    /// Recorded PodSandboxConfig from run_pod_sandbox calls.
    sandbox_configs: Mutex<Vec<PodSandboxConfig>>,
    /// Recorded PodSandboxConfig from create_container calls.
    create_sandbox_configs: Mutex<Vec<PodSandboxConfig>>,
    event_sender: tokio::sync::broadcast::Sender<CriRuntimeContainerEvent>,
    /// Per-container mock status keyed by container id. When an entry exists
    /// for the queried id, `container_status` returns its values (including a
    /// populated `metadata.name`), so tests can drive fast-exit/CRI-event
    /// scenarios where the global scalar fields do not apply.
    container_status_overrides: Mutex<HashMap<String, MockContainerStatus>>,
}

/// Per-container mock status record used by `container_status` overrides.
#[derive(Clone, Debug)]
struct MockContainerStatus {
    name: String,
    state: i32,
    exit_code: i32,
    started_at: i64,
    finished_at: i64,
    image: String,
}

impl Default for MockCriRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl MockCriRuntime {
    pub fn new() -> Self {
        let (event_sender, _) = tokio::sync::broadcast::channel(64);
        Self {
            calls: Mutex::new(Vec::new()),
            call_count: Mutex::new(0),
            fail_operation: Mutex::new(None),
            sandbox_counter: Mutex::new(0),
            image_present: Mutex::new(true),
            start_pod_cancel: Mutex::new(None),
            container_exit_code: Mutex::new(0),
            container_status_state: Mutex::new(k8s_cri::v1::ContainerState::ContainerExited as i32),
            exec_exit_code: Mutex::new(0),
            pod_sandboxes: Mutex::new(Vec::new()),
            create_configs: Mutex::new(Vec::new()),
            sandbox_configs: Mutex::new(Vec::new()),
            create_sandbox_configs: Mutex::new(Vec::new()),
            event_sender,
            container_status_overrides: Mutex::new(HashMap::new()),
        }
    }

    /// Cause the next call whose operation debug string contains `op_name`
    /// to return an error.
    pub fn set_fail_operation(&self, op_name: &str) {
        *self.fail_operation.lock().unwrap() = Some(op_name.to_string());
    }

    /// Control whether `image_status` reports the image as present.
    pub fn set_image_present(&self, present: bool) {
        *self.image_present.lock().unwrap() = present;
    }

    /// Set the exit code returned by `container_status`.
    pub fn set_container_exit_code(&self, exit_code: i32) {
        *self.container_exit_code.lock().unwrap() = exit_code;
    }

    /// Set the CRI state returned by `container_status`.
    pub fn set_container_status_state(&self, state: i32) {
        *self.container_status_state.lock().unwrap() = state;
    }

    /// Configure a per-container status override. `container_status` for this
    /// container id returns a `ContainerStatusResponse` populated with the
    /// given name/state/exit-code/timestamps/image and a `metadata.name` so
    /// runtime reconcile can match the container to a spec entry by name.
    /// Used by fast-exit / CRI-event tests where the global scalar fields
    /// cannot describe an individual container.
    #[allow(clippy::too_many_arguments)]
    pub fn set_container_status_for_test(
        &self,
        container_id: &str,
        name: &str,
        state: crate::runtime::cri::ContainerRuntimeState,
        exit_code: i32,
        started_at: i64,
        finished_at: i64,
        image: &str,
    ) {
        let cri_state = match state {
            crate::runtime::cri::ContainerRuntimeState::Created => {
                k8s_cri::v1::ContainerState::ContainerCreated as i32
            }
            crate::runtime::cri::ContainerRuntimeState::Running => {
                k8s_cri::v1::ContainerState::ContainerRunning as i32
            }
            crate::runtime::cri::ContainerRuntimeState::Exited => {
                k8s_cri::v1::ContainerState::ContainerExited as i32
            }
            crate::runtime::cri::ContainerRuntimeState::Unknown => {
                k8s_cri::v1::ContainerState::ContainerUnknown as i32
            }
        };
        self.container_status_overrides.lock().unwrap().insert(
            container_id.to_string(),
            MockContainerStatus {
                name: name.to_string(),
                state: cri_state,
                exit_code,
                started_at,
                finished_at,
                image: image.to_string(),
            },
        );
    }

    pub fn set_exec_exit_code(&self, exit_code: i32) {
        *self.exec_exit_code.lock().unwrap() = exit_code;
    }

    /// Configure CRI pod sandboxes as (id, namespace, name, uid, state).
    pub fn set_pod_sandboxes(&self, sandboxes: Vec<(&str, &str, &str, &str, &str)>) {
        *self.pod_sandboxes.lock().unwrap() = sandboxes
            .into_iter()
            .map(|(id, namespace, name, uid, state)| {
                (
                    id.to_string(),
                    namespace.to_string(),
                    name.to_string(),
                    uid.to_string(),
                    state.to_string(),
                )
            })
            .collect();
    }

    /// Return all ContainerConfig objects recorded from create_container calls.
    pub fn recorded_create_configs(&self) -> Vec<ContainerConfig> {
        self.create_configs.lock().unwrap().clone()
    }

    /// Return all PodSandboxConfig objects recorded from run_pod_sandbox calls.
    pub fn recorded_sandbox_configs(&self) -> Vec<PodSandboxConfig> {
        self.sandbox_configs.lock().unwrap().clone()
    }

    /// Return all PodSandboxConfig objects recorded from create_container calls.
    pub fn recorded_create_sandbox_configs(&self) -> Vec<PodSandboxConfig> {
        self.create_sandbox_configs.lock().unwrap().clone()
    }

    /// Set a CancellationToken that will be cancelled inside `run_pod_sandbox`
    /// (after recording). Used to test cancellation after sandbox creation.
    pub fn set_start_pod_cancel(&self, cancel: CancellationToken) {
        *self.start_pod_cancel.lock().unwrap() = Some(cancel);
    }

    pub fn clear_calls(&self) {
        self.calls.lock().unwrap().clear();
        *self.call_count.lock().unwrap() = 0;
    }

    pub fn recorded_calls(&self) -> Vec<MockCriCall> {
        self.calls.lock().unwrap().clone()
    }

    fn record(&self, operation: MockCriOperation) -> anyhow::Result<()> {
        let mut count = self.call_count.lock().unwrap();
        *count += 1;
        let order = *count;
        self.calls.lock().unwrap().push(MockCriCall {
            operation: operation.clone(),
            call_order: order,
        });
        // Check failure injection
        let op_debug = format!("{:?}", operation);
        if let Some(ref fail) = *self.fail_operation.lock().unwrap()
            && op_debug.contains(fail.as_str())
        {
            return Err(anyhow::anyhow!("injected failure for: {}", op_debug));
        }
        Ok(())
    }

    fn next_sandbox_id(&self) -> String {
        let mut counter = self.sandbox_counter.lock().unwrap();
        *counter += 1;
        format!("sandbox-{:04}", *counter)
    }

    fn emit_container_event(&self, container_id: &str, kind: CriRuntimeContainerEventKind) {
        let _ = self.event_sender.send(CriRuntimeContainerEvent {
            container_id: container_id.to_string(),
            kind,
        });
    }
}

pub struct MockCriEventStream {
    receiver: tokio::sync::broadcast::Receiver<CriRuntimeContainerEvent>,
    buffered: VecDeque<CriRuntimeContainerEvent>,
}

#[async_trait::async_trait]
impl crate::runtime::cri::CriRuntimeContainerEventStream for MockCriEventStream {
    async fn next_event(&mut self) -> anyhow::Result<Option<CriRuntimeContainerEvent>> {
        if let Some(event) = self.buffered.pop_front() {
            return Ok(Some(event));
        }
        loop {
            match self.receiver.recv().await {
                Ok(event) => return Ok(Some(event)),
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(None),
            }
        }
    }
}

#[async_trait::async_trait]
impl crate::runtime::cri::CriRuntime for MockCriRuntime {
    async fn image_status(&self, image: &str) -> anyhow::Result<bool> {
        self.record(MockCriOperation::ImageStatus(image.to_string()))?;
        Ok(*self.image_present.lock().unwrap())
    }

    async fn pull_image(&self, image: &str) -> anyhow::Result<String> {
        self.record(MockCriOperation::PullImage(image.to_string()))?;
        Ok(format!("pulled-{}", image))
    }

    async fn run_pod_sandbox(&self, sandbox_config: PodSandboxConfig) -> anyhow::Result<String> {
        self.record(MockCriOperation::RunPodSandbox)?;
        self.sandbox_configs.lock().unwrap().push(sandbox_config);
        let sandbox_id = self.next_sandbox_id();
        if let Some(cancel) = self.start_pod_cancel.lock().unwrap().take() {
            cancel.cancel();
        }
        Ok(sandbox_id)
    }

    async fn stop_pod_sandbox(&self, sandbox_id: &str) -> anyhow::Result<()> {
        self.record(MockCriOperation::StopPodSandbox(sandbox_id.to_string()))?;
        Ok(())
    }

    async fn remove_pod_sandbox(&self, sandbox_id: &str) -> anyhow::Result<()> {
        self.record(MockCriOperation::RemovePodSandbox(sandbox_id.to_string()))?;
        Ok(())
    }

    async fn list_pod_sandboxes(
        &self,
        pod_uid_filter: Option<&str>,
    ) -> anyhow::Result<Vec<(String, String)>> {
        self.record(MockCriOperation::ListPodSandboxes(
            pod_uid_filter.map(|s| s.to_string()),
        ))?;
        Ok(self
            .pod_sandboxes
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, _, _, uid, _)| {
                pod_uid_filter
                    .filter(|filter| !filter.trim().is_empty())
                    .map(|filter| uid == filter)
                    .unwrap_or(true)
            })
            .map(|(id, _, _, _, state)| (id.clone(), state.clone()))
            .collect())
    }

    async fn list_pod_sandbox_summaries(
        &self,
    ) -> anyhow::Result<Vec<crate::runtime::cri::CriPodSandboxSummary>> {
        self.record(MockCriOperation::ListPodSandboxes(None))?;
        Ok(self
            .pod_sandboxes
            .lock()
            .unwrap()
            .iter()
            .map(
                |(id, namespace, name, uid, _state)| crate::runtime::cri::CriPodSandboxSummary {
                    sandbox_id: id.clone(),
                    namespace: namespace.clone(),
                    name: name.clone(),
                    uid: uid.clone(),
                },
            )
            .collect())
    }

    async fn create_container(
        &self,
        container_config: ContainerConfig,
        sandbox_id: &str,
        sandbox_config: PodSandboxConfig,
    ) -> anyhow::Result<String> {
        let container_name = container_config
            .metadata
            .as_ref()
            .map(|m| m.name.clone())
            .unwrap_or_default();
        self.record(MockCriOperation::CreateContainer {
            sandbox_id: sandbox_id.to_string(),
            container_name,
        })?;
        self.create_configs.lock().unwrap().push(container_config);
        self.create_sandbox_configs
            .lock()
            .unwrap()
            .push(sandbox_config);
        Ok(format!("container-{}", sandbox_id))
    }

    async fn start_container(&self, container_id: &str) -> anyhow::Result<()> {
        self.record(MockCriOperation::StartContainer(container_id.to_string()))?;
        self.emit_container_event(container_id, CriRuntimeContainerEventKind::Stopped);
        Ok(())
    }

    async fn stop_container(&self, container_id: &str, timeout_seconds: i64) -> anyhow::Result<()> {
        self.record(MockCriOperation::StopContainer(
            container_id.to_string(),
            timeout_seconds,
        ))?;
        Ok(())
    }

    async fn remove_container(&self, container_id: &str) -> anyhow::Result<()> {
        self.record(MockCriOperation::RemoveContainer(container_id.to_string()))?;
        Ok(())
    }

    async fn container_status(
        &self,
        container_id: &str,
    ) -> anyhow::Result<k8s_cri::v1::ContainerStatusResponse> {
        self.record(MockCriOperation::ContainerStatus(container_id.to_string()))?;
        if let Some(override_status) = self
            .container_status_overrides
            .lock()
            .unwrap()
            .get(container_id)
            .cloned()
        {
            return Ok(k8s_cri::v1::ContainerStatusResponse {
                status: Some(k8s_cri::v1::ContainerStatus {
                    id: container_id.to_string(),
                    metadata: Some(k8s_cri::v1::ContainerMetadata {
                        name: override_status.name,
                        attempt: 0,
                    }),
                    state: override_status.state,
                    exit_code: override_status.exit_code,
                    started_at: override_status.started_at,
                    finished_at: override_status.finished_at,
                    image: Some(k8s_cri::v1::ImageSpec {
                        image: override_status.image.clone(),
                        ..Default::default()
                    }),
                    image_ref: override_status.image,
                    ..Default::default()
                }),
                ..Default::default()
            });
        }
        let exit_code = *self.container_exit_code.lock().unwrap();
        let state = *self.container_status_state.lock().unwrap();
        Ok(k8s_cri::v1::ContainerStatusResponse {
            status: Some(k8s_cri::v1::ContainerStatus {
                id: container_id.to_string(),
                state,
                exit_code,
                started_at: if state == k8s_cri::v1::ContainerState::ContainerRunning as i32 {
                    1_000_000_000
                } else {
                    0
                },
                image_ref: format!("mock-image-ref-{}", container_id),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    async fn exec_sync(
        &self,
        container_id: &str,
        command: &[String],
        timeout_seconds: i64,
    ) -> anyhow::Result<k8s_cri::v1::ExecSyncResponse> {
        self.record(MockCriOperation::ExecSync {
            container_id: container_id.to_string(),
            command: command.to_vec(),
            timeout_seconds,
        })?;
        Ok(k8s_cri::v1::ExecSyncResponse {
            exit_code: *self.exec_exit_code.lock().unwrap(),
            ..Default::default()
        })
    }

    async fn subscribe_container_events(
        &self,
    ) -> anyhow::Result<Box<dyn crate::runtime::cri::CriRuntimeContainerEventStream>> {
        self.record(MockCriOperation::SubscribeContainerEvents)?;
        Ok(Box::new(MockCriEventStream {
            receiver: self.event_sender.subscribe(),
            buffered: VecDeque::new(),
        }))
    }
}

// --- MockContainerRuntimeControl ---

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MockContainerControlOp {
    ListContainers { sandbox_id_filter: Option<String> },
    PodMetadataForContainer { container_id: String },
}

pub struct MockContainerRuntimeControl {
    calls: Mutex<Vec<MockContainerControlOp>>,
    containers: Mutex<Vec<(String, crate::runtime::cri::ContainerRuntimeState)>>,
}

impl Default for MockContainerRuntimeControl {
    fn default() -> Self {
        Self::new()
    }
}

impl MockContainerRuntimeControl {
    pub fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            containers: Mutex::new(Vec::new()),
        }
    }

    pub fn clear_calls(&self) {
        self.calls.lock().unwrap().clear();
    }

    pub fn recorded_calls(&self) -> Vec<MockContainerControlOp> {
        self.calls.lock().unwrap().clone()
    }

    pub fn set_containers(&self, containers: Vec<(String, String)>) {
        *self.containers.lock().unwrap() = containers
            .into_iter()
            .map(|(id, state)| (id, state.into()))
            .collect();
    }

    pub fn set_container_states(
        &self,
        containers: Vec<(String, crate::runtime::cri::ContainerRuntimeState)>,
    ) {
        *self.containers.lock().unwrap() = containers;
    }
}

#[async_trait::async_trait]
impl crate::runtime::cri::ContainerRuntimeControl for MockContainerRuntimeControl {
    async fn list_containers(
        &self,
        sandbox_id_filter: Option<&str>,
    ) -> anyhow::Result<Vec<(String, crate::runtime::cri::ContainerRuntimeState)>> {
        self.calls
            .lock()
            .unwrap()
            .push(MockContainerControlOp::ListContainers {
                sandbox_id_filter: sandbox_id_filter.map(|s| s.to_string()),
            });
        Ok(self.containers.lock().unwrap().clone())
    }

    async fn pod_metadata_for_container(
        &self,
        container_id: &str,
    ) -> anyhow::Result<Option<(String, String)>> {
        self.calls
            .lock()
            .unwrap()
            .push(MockContainerControlOp::PodMetadataForContainer {
                container_id: container_id.to_string(),
            });
        Ok(None)
    }
}

#[test]
fn cri_runtime_fakes_compile_without_root_adapters() {
    let runtime = MockCriRuntime::new();
    let control = MockContainerRuntimeControl::new();
    assert!(runtime.recorded_calls().is_empty());
    assert!(control.recorded_calls().is_empty());
}
