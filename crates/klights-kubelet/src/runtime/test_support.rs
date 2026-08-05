#![cfg(any(test, feature = "test-support"))]
use k8s_cri::v1::{ContainerConfig, PodSandboxConfig};
use serde_json::{Value, json};
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use tokio_util::sync::CancellationToken;

use super::cri::{CriRuntimeContainerEvent, CriRuntimeContainerEventKind};

use crate::lifecycle::LifecycleCommand;
use crate::runtime::{
    PodDeletionFinalizeResult, PodFinalizeStartupResult, PodRuntimeKey, PodRuntimeService,
    PodStartResult,
};

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

// --- MockPodRuntimeStore ---

pub struct MockPodRuntimeStore {
    sandboxes: Mutex<std::collections::HashMap<(String, String, String), String>>,
    calls: Mutex<Vec<String>>,
    record_failure: Mutex<Option<String>>,
    lookup_failure: Mutex<Option<String>>,
}

impl Default for MockPodRuntimeStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MockPodRuntimeStore {
    pub fn new() -> Self {
        Self {
            sandboxes: Mutex::new(std::collections::HashMap::new()),
            calls: Mutex::new(Vec::new()),
            record_failure: Mutex::new(None),
            lookup_failure: Mutex::new(None),
        }
    }

    pub fn clear_calls(&self) {
        self.calls.lock().unwrap().clear();
    }

    pub fn recorded_calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }

    pub fn fail_record_sandbox(&self, message: impl Into<String>) {
        *self.record_failure.lock().unwrap() = Some(message.into());
    }

    pub fn fail_sandbox_lookup(&self, message: impl Into<String>) {
        *self.lookup_failure.lock().unwrap() = Some(message.into());
    }
}

#[async_trait::async_trait]
impl crate::runtime::store::PodRuntimeStore for MockPodRuntimeStore {
    async fn record_sandbox(
        &self,
        key: &crate::runtime::PodRuntimeKey,
        sandbox_id: &str,
    ) -> anyhow::Result<()> {
        self.calls.lock().unwrap().push(format!(
            "record_sandbox:{}/{}/{}={}",
            key.namespace, key.name, key.uid, sandbox_id
        ));
        if let Some(message) = self.record_failure.lock().unwrap().clone() {
            anyhow::bail!("{message}");
        }
        self.sandboxes.lock().unwrap().insert(
            (key.namespace.clone(), key.name.clone(), key.uid.clone()),
            sandbox_id.to_string(),
        );
        Ok(())
    }

    async fn get_sandbox_id(
        &self,
        key: &crate::runtime::PodRuntimeKey,
    ) -> anyhow::Result<Option<String>> {
        self.calls.lock().unwrap().push(format!(
            "get_sandbox_id:{}/{}/{}",
            key.namespace, key.name, key.uid
        ));
        if let Some(message) = self.lookup_failure.lock().unwrap().clone() {
            anyhow::bail!("{message}");
        }
        Ok(self
            .sandboxes
            .lock()
            .unwrap()
            .get(&(key.namespace.clone(), key.name.clone(), key.uid.clone()))
            .cloned())
    }

    async fn delete_sandbox(&self, key: &crate::runtime::PodRuntimeKey) -> anyhow::Result<()> {
        self.calls.lock().unwrap().push(format!(
            "delete_sandbox:{}/{}/{}",
            key.namespace, key.name, key.uid
        ));
        self.sandboxes.lock().unwrap().remove(&(
            key.namespace.clone(),
            key.name.clone(),
            key.uid.clone(),
        ));
        Ok(())
    }
}

// --- MockPodSlotAdmission ---

pub struct MockPodSlotAdmission {
    calls: Mutex<Vec<String>>,
    slot_tx: tokio::sync::broadcast::Sender<klights_node_store::PodSlotAdmissionEvent>,
    admitted: Mutex<bool>,
}

struct MockPodSlotSubscription {
    receiver: tokio::sync::broadcast::Receiver<klights_node_store::PodSlotAdmissionEvent>,
}

impl klights_node_store::PodSlotEventSubscription for MockPodSlotSubscription {
    fn next_event(
        &mut self,
    ) -> klights_node_store::RuntimeWorkFuture<'_, Option<klights_node_store::PodSlotAdmissionEvent>>
    {
        Box::pin(async move {
            match self.receiver.recv().await {
                Ok(event) => Ok(Some(event)),
                Err(tokio::sync::broadcast::error::RecvError::Closed) => Ok(None),
                Err(error) => Err(klights_node_store::RuntimeWorkError::retryable(
                    error.to_string(),
                )),
            }
        })
    }
}

impl Default for MockPodSlotAdmission {
    fn default() -> Self {
        Self::new()
    }
}

impl MockPodSlotAdmission {
    pub fn new() -> Self {
        let (slot_tx, _) = tokio::sync::broadcast::channel(16);
        Self {
            calls: Mutex::new(Vec::new()),
            slot_tx,
            admitted: Mutex::new(true),
        }
    }

    pub fn set_admitted(&self, admitted: bool) {
        *self.admitted.lock().unwrap() = admitted;
    }

    pub fn clear_calls(&self) {
        self.calls.lock().unwrap().clear();
    }

    pub fn recorded_calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl crate::runtime::store::PodSlotAdmission for MockPodSlotAdmission {
    fn subscribe(&self) -> Box<dyn klights_node_store::PodSlotEventSubscription> {
        Box::new(MockPodSlotSubscription {
            receiver: self.slot_tx.subscribe(),
        })
    }

    async fn try_admit(
        &self,
        key: &crate::runtime::PodRuntimeKey,
        node_name: &str,
    ) -> anyhow::Result<klights_node_store::PodSlotAdmissionResult> {
        self.calls.lock().unwrap().push(format!(
            "try_admit:{}/{}/{}@{}",
            key.namespace, key.name, key.uid, node_name
        ));
        if *self.admitted.lock().unwrap() {
            Ok(klights_node_store::PodSlotAdmissionResult::Admitted {
                observed_pod_version: klights_node_store::ObservedPodVersion::try_new(1)?,
            })
        } else {
            Ok(klights_node_store::PodSlotAdmissionResult::Blocked {
                blocking_uid: "blocker-uid".into(),
                blocking_node: "blocker-node".into(),
                state: klights_node_store::PodSlotAdmissionState::Terminating,
                observed_pod_version: klights_node_store::ObservedPodVersion::try_new(1)?,
            })
        }
    }

    async fn clear_slot(&self, key: &crate::runtime::PodRuntimeKey) -> anyhow::Result<()> {
        self.calls.lock().unwrap().push(format!(
            "clear_slot:{}/{}/{}",
            key.namespace, key.name, key.uid
        ));
        Ok(())
    }
}
// --- MockPodRuntimeService ---

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MockRuntimeCall {
    StartPod {
        namespace: String,
        name: String,
        uid: String,
        /// Pod snapshot passed to start_pod; None when no pod was carried.
        has_pod: bool,
        /// Whether the cancellation token was triggered (cancel was signalled).
        cancelled: bool,
    },
    StopPod {
        namespace: String,
        name: String,
        uid: String,
        sandbox_id: Option<String>,
    },
    FinalizeStartup {
        namespace: String,
        name: String,
        uid: String,
        has_pod: bool,
        sandbox_id_hint: Option<String>,
    },
    FinalizeDeletion {
        namespace: String,
        name: String,
        uid: String,
    },
    ReconcileRuntime {
        namespace: String,
        name: String,
        uid: String,
        hint_container_ids: Vec<String>,
    },
    ReconcileCriLeftovers {
        namespace: String,
        name: String,
        uid: String,
    },
    ReconcileEphemeral {
        namespace: String,
        name: String,
        uid: String,
    },
    CheckSlotAdmission {
        namespace: String,
        name: String,
        uid: String,
        has_pod: bool,
        resource_version: Option<i64>,
        start_after_admit: bool,
        operation_id: u64,
        cancelled: bool,
    },
    HandleCommand {
        command_name: String,
    },
    ScheduleRetry {
        namespace: String,
        name: String,
        uid: String,
        delay_ms: u128,
    },
    ScheduleStartPodRetry {
        namespace: String,
        name: String,
        uid: String,
        delay_ms: u128,
        attempt: u32,
        error_message: String,
    },
}

impl MockRuntimeCall {
    fn from_key(op: &str, key: &PodRuntimeKey) -> Self {
        let (namespace, name, uid) = (key.namespace.clone(), key.name.clone(), key.uid.clone());
        match op {
            "start_pod" => MockRuntimeCall::StartPod {
                namespace,
                name,
                uid,
                has_pod: false,
                cancelled: false,
            },
            "stop_pod" => MockRuntimeCall::StopPod {
                namespace,
                name,
                uid,
                sandbox_id: None,
            },
            "finalize_startup" => MockRuntimeCall::FinalizeStartup {
                namespace,
                name,
                uid,
                has_pod: false,
                sandbox_id_hint: None,
            },
            "finalize_deletion" => MockRuntimeCall::FinalizeDeletion {
                namespace,
                name,
                uid,
            },
            "reconcile_runtime" => MockRuntimeCall::ReconcileRuntime {
                namespace,
                name,
                uid,
                hint_container_ids: vec![],
            },
            "reconcile_cri_leftovers" => MockRuntimeCall::ReconcileCriLeftovers {
                namespace,
                name,
                uid,
            },
            "reconcile_ephemeral" => MockRuntimeCall::ReconcileEphemeral {
                namespace,
                name,
                uid,
            },
            _ => panic!("unknown runtime call kind"),
        }
    }
}

/// Recording mock for `PodRuntimeService`. Every method records its call
/// with UID-keyed arguments. Start and deletion-finalize results are
/// configurable; per-method error injection is supported.
pub struct MockPodRuntimeService {
    calls: Mutex<Vec<MockRuntimeCall>>,
    start_result: Mutex<PodStartResult>,
    finalize_startup_result: Mutex<PodFinalizeStartupResult>,
    finalize_result: Mutex<PodDeletionFinalizeResult>,
    fail_method: Mutex<Option<String>>,
    /// When set, `stop_pod` returns a typed `PodOwnershipError` instead of
    /// the generic injected failure. Used to exercise the lifecycle
    /// executor's ownership-mismatch classification (P0 StopPod loop).
    stop_ownership_error: Mutex<Option<crate::runtime::PodOwnershipError>>,
    /// CancellationToken captured from the last `start_pod` call; cloned
    /// into the mock so tests can signal cancellation externally.
    start_pod_cancel: Mutex<Option<CancellationToken>>,
}

impl Default for MockPodRuntimeService {
    fn default() -> Self {
        Self::new()
    }
}

impl MockPodRuntimeService {
    pub fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            start_result: Mutex::new(PodStartResult::Started { sandbox_id: None }),
            finalize_startup_result: Mutex::new(PodFinalizeStartupResult::Unconfirmed),
            finalize_result: Mutex::new(PodDeletionFinalizeResult::DeletedOrAlreadyGone),
            fail_method: Mutex::new(None),
            stop_ownership_error: Mutex::new(None),
            start_pod_cancel: Mutex::new(None),
        }
    }

    pub fn set_start_result(&self, result: PodStartResult) {
        *self.start_result.lock().unwrap() = result;
    }

    pub fn set_finalize_result(&self, result: PodDeletionFinalizeResult) {
        *self.finalize_result.lock().unwrap() = result;
    }

    pub fn set_finalize_startup_result(&self, result: PodFinalizeStartupResult) {
        *self.finalize_startup_result.lock().unwrap() = result;
    }

    /// Cause the next call to the named method to return an error.
    pub fn set_fail_method(&self, method_name: &str) {
        *self.fail_method.lock().unwrap() = Some(method_name.to_string());
    }

    /// Cause the next `stop_pod` call to fail with a typed `PodOwnershipError`
    /// (terminal/non-retryable) instead of a generic injected failure.
    pub fn set_stop_pod_ownership_error(&self, local_node: &str, target_node: Option<&str>) {
        *self.stop_ownership_error.lock().unwrap() = Some(crate::runtime::PodOwnershipError {
            local_node: local_node.to_string(),
            target_node: target_node.map(|s| s.to_string()),
        });
    }

    pub fn recorded_calls(&self) -> Vec<MockRuntimeCall> {
        self.calls.lock().unwrap().clone()
    }

    /// Take the `CancellationToken` captured from the last `start_pod` call.
    pub fn take_start_pod_cancel(&self) -> Option<CancellationToken> {
        self.start_pod_cancel.lock().unwrap().take()
    }

    fn check_fail(&self, method: &str) -> anyhow::Result<()> {
        if let Some(ref f) = *self.fail_method.lock().unwrap()
            && f == method
        {
            anyhow::bail!("injected failure for: {}", method);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl PodRuntimeService for MockPodRuntimeService {
    async fn start_pod(
        &self,
        key: PodRuntimeKey,
        pod: Option<serde_json::Value>,
        cancel: CancellationToken,
    ) -> anyhow::Result<PodStartResult> {
        self.check_fail("start_pod")?;
        // Store cancel token so tests can signal cancellation.
        *self.start_pod_cancel.lock().unwrap() = Some(cancel.clone());
        self.calls.lock().unwrap().push(MockRuntimeCall::StartPod {
            namespace: key.namespace.clone(),
            name: key.name.clone(),
            uid: key.uid.clone(),
            has_pod: pod.is_some(),
            cancelled: cancel.is_cancelled(),
        });
        Ok(self.start_result.lock().unwrap().clone())
    }

    async fn stop_pod(
        &self,
        key: PodRuntimeKey,
        _pod: Option<serde_json::Value>,
        sandbox_id: Option<String>,
    ) -> anyhow::Result<()> {
        if let Some(own) = self.stop_ownership_error.lock().unwrap().take() {
            self.calls.lock().unwrap().push(MockRuntimeCall::StopPod {
                namespace: key.namespace.clone(),
                name: key.name.clone(),
                uid: key.uid.clone(),
                sandbox_id,
            });
            return Err(anyhow::Error::new(own));
        }
        self.check_fail("stop_pod")?;
        self.calls.lock().unwrap().push(MockRuntimeCall::StopPod {
            namespace: key.namespace.clone(),
            name: key.name.clone(),
            uid: key.uid.clone(),
            sandbox_id,
        });
        Ok(())
    }

    async fn finalize_startup(
        &self,
        key: PodRuntimeKey,
        pod: Option<serde_json::Value>,
        sandbox_id_hint: Option<String>,
    ) -> anyhow::Result<PodFinalizeStartupResult> {
        self.check_fail("finalize_startup")?;
        self.calls
            .lock()
            .unwrap()
            .push(MockRuntimeCall::FinalizeStartup {
                namespace: key.namespace.clone(),
                name: key.name.clone(),
                uid: key.uid.clone(),
                has_pod: pod.is_some(),
                sandbox_id_hint,
            });
        Ok(self.finalize_startup_result.lock().unwrap().clone())
    }

    async fn finalize_deletion(
        &self,
        key: PodRuntimeKey,
    ) -> anyhow::Result<PodDeletionFinalizeResult> {
        self.check_fail("finalize_deletion")?;
        self.calls
            .lock()
            .unwrap()
            .push(MockRuntimeCall::from_key("finalize_deletion", &key));
        Ok(self.finalize_result.lock().unwrap().clone())
    }

    async fn reconcile_runtime(
        &self,
        key: PodRuntimeKey,
        hint: crate::runtime::RuntimeReconcileHint,
    ) -> anyhow::Result<()> {
        self.check_fail("reconcile_runtime")?;
        self.calls
            .lock()
            .unwrap()
            .push(MockRuntimeCall::ReconcileRuntime {
                namespace: key.namespace.clone(),
                name: key.name.clone(),
                uid: key.uid.clone(),
                hint_container_ids: hint.container_ids().map(str::to_string).collect(),
            });
        Ok(())
    }

    async fn reconcile_cri_leftovers(&self, key: PodRuntimeKey) -> anyhow::Result<()> {
        self.check_fail("reconcile_cri_leftovers")?;
        self.calls
            .lock()
            .unwrap()
            .push(MockRuntimeCall::from_key("reconcile_cri_leftovers", &key));
        Ok(())
    }

    async fn reconcile_ephemeral(
        &self,
        key: PodRuntimeKey,
        _pod: Option<serde_json::Value>,
    ) -> anyhow::Result<()> {
        self.check_fail("reconcile_ephemeral")?;
        self.calls
            .lock()
            .unwrap()
            .push(MockRuntimeCall::from_key("reconcile_ephemeral", &key));
        Ok(())
    }

    async fn handle_lifecycle_command(&self, command: LifecycleCommand) -> anyhow::Result<()> {
        self.check_fail("handle_lifecycle_command")?;
        self.calls
            .lock()
            .unwrap()
            .push(MockRuntimeCall::HandleCommand {
                command_name: format!("{:?}", command).chars().take(60).collect(),
            });
        Ok(())
    }

    async fn check_slot_admission(
        &self,
        request: crate::runtime::PodSlotAdmissionRequest,
        reply_to: crate::pod_lifecycle_router::LifecycleReplyHandle,
        cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        self.check_fail("check_slot_admission")?;
        let crate::runtime::PodSlotAdmissionRequest {
            key,
            pod,
            resource_version,
            start_after_admit,
            operation_id,
        } = request;
        self.calls
            .lock()
            .unwrap()
            .push(MockRuntimeCall::CheckSlotAdmission {
                namespace: key.namespace.clone(),
                name: key.name.clone(),
                uid: key.uid.clone(),
                has_pod: !pod.is_null(),
                resource_version,
                start_after_admit,
                operation_id,
                cancelled: cancel.is_cancelled(),
            });
        let _ = reply_to
            .route(
                crate::pod_lifecycle_core::message::LifecycleMessage::SlotAdmissionGranted {
                    key: crate::pod_lifecycle_core::message::PodLifecycleKey::new(
                        &key.namespace,
                        &key.name,
                        &key.uid,
                    ),
                    operation_id,
                    pod,
                    resource_version,
                    start_after_admit,
                },
            )
            .await;
        Ok(())
    }

    async fn schedule_retry(
        &self,
        key: PodRuntimeKey,
        delay: std::time::Duration,
        _reply_to: crate::pod_lifecycle_router::LifecycleReplyHandle,
    ) -> anyhow::Result<()> {
        self.check_fail("schedule_retry")?;
        self.calls
            .lock()
            .unwrap()
            .push(MockRuntimeCall::ScheduleRetry {
                namespace: key.namespace,
                name: key.name,
                uid: key.uid,
                delay_ms: delay.as_millis(),
            });
        Ok(())
    }

    async fn schedule_start_pod_retry(
        &self,
        key: PodRuntimeKey,
        delay: std::time::Duration,
        error_message: String,
        attempt: u32,
        _reply_to: crate::pod_lifecycle_router::LifecycleReplyHandle,
    ) -> anyhow::Result<()> {
        self.check_fail("schedule_start_pod_retry")?;
        self.calls
            .lock()
            .unwrap()
            .push(MockRuntimeCall::ScheduleStartPodRetry {
                namespace: key.namespace,
                name: key.name,
                uid: key.uid,
                delay_ms: delay.as_millis(),
                attempt,
                error_message,
            });
        Ok(())
    }
}
// --- MockPodDeletionFinalizer ---

/// Mock deletion finalizer for testing actor finalization paths.
pub struct MockPodDeletionFinalizer {
    calls: Mutex<Vec<PodRuntimeKey>>,
    pub outcome: Mutex<PodDeletionFinalizeResult>,
    pub fail: Mutex<Option<String>>,
}

impl Default for MockPodDeletionFinalizer {
    fn default() -> Self {
        Self::new()
    }
}

impl MockPodDeletionFinalizer {
    pub fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            outcome: Mutex::new(PodDeletionFinalizeResult::DeletedOrAlreadyGone),
            fail: Mutex::new(None),
        }
    }

    pub fn recorded_calls(&self) -> Vec<PodRuntimeKey> {
        self.calls.lock().unwrap().clone()
    }

    pub fn set_outcome(&self, outcome: PodDeletionFinalizeResult) {
        *self.outcome.lock().unwrap() = outcome;
    }

    pub fn set_fail(&self, msg: &str) {
        *self.fail.lock().unwrap() = Some(msg.to_string());
    }
}

#[async_trait::async_trait]
impl crate::pod_deletion_finalizer::PodDeletionFinalizer for MockPodDeletionFinalizer {
    async fn finalize_after_actor_cleanup(
        &self,
        key: &PodRuntimeKey,
    ) -> anyhow::Result<PodDeletionFinalizeResult> {
        self.calls.lock().unwrap().push(key.clone());
        if let Some(ref msg) = *self.fail.lock().unwrap() {
            anyhow::bail!("{}", msg);
        }
        Ok(self.outcome.lock().unwrap().clone())
    }
}
// --- MockEnvSourceReader ---

type EnvSourceKey = (String, String);

/// Recording mock for env source lookups. Backed only by in-memory maps.
pub struct MockEnvSourceReader {
    calls: Mutex<Vec<String>>,
    secrets: Mutex<HashMap<EnvSourceKey, klights_cluster_core::Resource>>,
    config_maps: Mutex<HashMap<EnvSourceKey, klights_cluster_core::Resource>>,
    services: Mutex<HashMap<String, Vec<klights_cluster_core::Resource>>>,
}

impl Default for MockEnvSourceReader {
    fn default() -> Self {
        Self::new()
    }
}

impl MockEnvSourceReader {
    pub fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            secrets: Mutex::new(HashMap::new()),
            config_maps: Mutex::new(HashMap::new()),
            services: Mutex::new(HashMap::new()),
        }
    }

    pub fn recorded_calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }

    pub fn insert_secret(&self, namespace: &str, name: &str, data: Value) {
        self.secrets.lock().unwrap().insert(
            (namespace.to_string(), name.to_string()),
            Self::resource("v1", "Secret", namespace, name, data),
        );
    }

    pub fn insert_config_map(&self, namespace: &str, name: &str, data: Value) {
        self.config_maps.lock().unwrap().insert(
            (namespace.to_string(), name.to_string()),
            Self::resource("v1", "ConfigMap", namespace, name, data),
        );
    }

    pub fn insert_service(&self, namespace: &str, name: &str, data: Value) {
        self.services
            .lock()
            .unwrap()
            .entry(namespace.to_string())
            .or_default()
            .push(Self::resource("v1", "Service", namespace, name, data));
    }

    fn resource(
        api_version: &str,
        kind: &str,
        namespace: &str,
        name: &str,
        mut data: Value,
    ) -> klights_cluster_core::Resource {
        if let Some(obj) = data.as_object_mut() {
            obj.entry("apiVersion".to_string())
                .or_insert_with(|| json!(api_version));
            obj.entry("kind".to_string()).or_insert_with(|| json!(kind));
            let metadata = obj
                .entry("metadata".to_string())
                .or_insert_with(|| json!({}));
            if let Some(meta) = metadata.as_object_mut() {
                meta.entry("namespace".to_string())
                    .or_insert_with(|| json!(namespace));
                meta.entry("name".to_string())
                    .or_insert_with(|| json!(name));
            }
        }
        klights_cluster_core::Resource {
            id: 0,
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: Some(namespace.to_string()),
            name: name.to_string(),
            uid: format!("{namespace}-{name}-uid"),
            data: std::sync::Arc::new(data),
            resource_version: 1,
        }
    }
}

#[async_trait::async_trait]
impl crate::pod_env::EnvSourceReader for MockEnvSourceReader {
    async fn secret(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("secret:{namespace}/{name}"));
        Ok(self
            .secrets
            .lock()
            .unwrap()
            .get(&(namespace.to_string(), name.to_string()))
            .cloned())
    }

    async fn config_map(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("config_map:{namespace}/{name}"));
        Ok(self
            .config_maps
            .lock()
            .unwrap()
            .get(&(namespace.to_string(), name.to_string()))
            .cloned())
    }
}

#[async_trait::async_trait]
impl crate::pod_service_envs::ServiceEnvSource for MockEnvSourceReader {
    async fn services(
        &self,
        namespace: &str,
    ) -> anyhow::Result<Vec<klights_cluster_core::Resource>> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("services:{namespace}"));
        Ok(self
            .services
            .lock()
            .unwrap()
            .get(namespace)
            .cloned()
            .unwrap_or_default())
    }
}
// --- MockPodHookRuntime ---

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MockHookCall {
    pub hook_type: String,
    pub container_id: String,
    pub pod_ip: String,
}

pub struct MockPodHookRuntime {
    calls: Mutex<Vec<MockHookCall>>,
    outcome: Mutex<crate::runtime::hooks::HookOutcome>,
}

impl Default for MockPodHookRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl MockPodHookRuntime {
    pub fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            outcome: Mutex::new(crate::runtime::hooks::HookOutcome::Succeeded),
        }
    }

    pub fn set_outcome(&self, outcome: crate::runtime::hooks::HookOutcome) {
        *self.outcome.lock().unwrap() = outcome;
    }

    pub fn recorded_calls(&self) -> Vec<MockHookCall> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl crate::runtime::hooks::PodHookRuntime for MockPodHookRuntime {
    async fn execute_post_start(
        &self,
        container_id: &str,
        pod_ip: &str,
        _hook: &serde_json::Value,
        _container_spec: &serde_json::Value,
    ) -> anyhow::Result<crate::runtime::hooks::HookOutcome> {
        self.calls.lock().unwrap().push(MockHookCall {
            hook_type: "postStart".to_string(),
            container_id: container_id.to_string(),
            pod_ip: pod_ip.to_string(),
        });
        Ok(self.outcome.lock().unwrap().clone())
    }

    async fn execute_pre_stop(
        &self,
        container_id: &str,
        pod_ip: &str,
        _hook: &serde_json::Value,
        _container_spec: &serde_json::Value,
    ) -> anyhow::Result<crate::runtime::hooks::HookOutcome> {
        self.calls.lock().unwrap().push(MockHookCall {
            hook_type: "preStop".to_string(),
            container_id: container_id.to_string(),
            pod_ip: pod_ip.to_string(),
        });
        Ok(self.outcome.lock().unwrap().clone())
    }
}

#[test]
fn cri_runtime_fakes_compile_without_root_adapters() {
    let runtime = MockCriRuntime::new();
    let control = MockContainerRuntimeControl::new();
    assert!(runtime.recorded_calls().is_empty());
    assert!(control.recorded_calls().is_empty());
}
