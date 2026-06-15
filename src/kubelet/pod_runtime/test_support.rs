#![cfg(test)]
use k8s_cri::v1::{ContainerConfig, PodSandboxConfig};
use serde_json::{Value, json};
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use tokio_util::sync::CancellationToken;

use super::cri::{CriRuntimeContainerEvent, CriRuntimeContainerEventKind};
use super::service::{
    PodDeletionFinalizeResult, PodFinalizeStartupResult, PodRuntimeKey, PodRuntimeService,
    PodStartResult, RealPodRuntimeServiceDependencies,
};
use crate::kubelet::lifecycle::LifecycleCommand;

type MockSandboxRecord = (String, String, String, String, String);

/// Conventional non-system namespaces these runtime tests place pods in. The
/// API create path enforces the upstream NamespaceLifecycle rule (target
/// namespace must exist), so harnesses seed these as a live cluster would.
/// (System namespaces like `default`/`kube-system` are always considered
/// present, so they need not be listed here.)
pub(crate) const RUNTIME_TEST_NAMESPACES: &[&str] = &[
    "ns",
    "statefulset",
    "sonobuoy",
    "deleted-ns",
    "init-container",
    "container-probe",
    "container-runtime",
    "dns-debug",
    "downward-api",
    "e2e-debug",
    "kubelet-test",
    "logs",
    "pod-network-test",
    "pods",
    "security-context",
    "sysctl",
    "var-expansion",
];

/// Seed every conventional runtime-test namespace into `handle`.
pub(crate) async fn seed_runtime_test_namespaces(handle: &crate::datastore::DatastoreHandle) {
    for ns in RUNTIME_TEST_NAMESPACES {
        crate::datastore::DatastoreBackend::seed_namespace_for_test(handle.as_ref(), ns).await;
    }
}

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
    /// Exit code reported by `container_status`.
    container_exit_code: Mutex<i32>,
    /// State reported by `container_status`.
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
impl crate::kubelet::pod_runtime::cri::CriRuntimeContainerEventStream for MockCriEventStream {
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
impl crate::kubelet::pod_runtime::cri::CriRuntime for MockCriRuntime {
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
    ) -> anyhow::Result<Vec<crate::kubelet::pod_runtime::cri::CriPodSandboxSummary>> {
        self.record(MockCriOperation::ListPodSandboxes(None))?;
        Ok(self
            .pod_sandboxes
            .lock()
            .unwrap()
            .iter()
            .map(|(id, namespace, name, uid, _state)| {
                crate::kubelet::pod_runtime::cri::CriPodSandboxSummary {
                    sandbox_id: id.clone(),
                    namespace: namespace.clone(),
                    name: name.clone(),
                    uid: uid.clone(),
                }
            })
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
    ) -> anyhow::Result<Box<dyn crate::kubelet::pod_runtime::cri::CriRuntimeContainerEventStream>>
    {
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
    containers: Mutex<
        Vec<(
            String,
            crate::kubelet::pod_runtime::cri::ContainerRuntimeState,
        )>,
    >,
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
        containers: Vec<(
            String,
            crate::kubelet::pod_runtime::cri::ContainerRuntimeState,
        )>,
    ) {
        *self.containers.lock().unwrap() = containers;
    }
}

#[async_trait::async_trait]
impl crate::kubelet::pod_runtime::cri::ContainerRuntimeControl for MockContainerRuntimeControl {
    async fn list_containers(
        &self,
        sandbox_id_filter: Option<&str>,
    ) -> anyhow::Result<
        Vec<(
            String,
            crate::kubelet::pod_runtime::cri::ContainerRuntimeState,
        )>,
    > {
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

// --- MockPodNetworkRuntime ---

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MockNetworkOp {
    ReadAssignment {
        sandbox_id: String,
        namespace: String,
        name: String,
        uid: String,
        host_network: bool,
    },
    ReleaseSandboxNetwork {
        namespace: String,
        name: String,
        uid: String,
        sandbox_id: String,
    },
}

pub struct MockPodNetworkRuntime {
    calls: Mutex<Vec<MockNetworkOp>>,
    fail: Mutex<Option<String>>,
}

impl Default for MockPodNetworkRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl MockPodNetworkRuntime {
    pub fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            fail: Mutex::new(None),
        }
    }

    pub fn set_fail(&self, op_name: &str) {
        *self.fail.lock().unwrap() = Some(op_name.to_string());
    }

    pub fn set_network_assignment_timeout(&self) {
        *self.fail.lock().unwrap() = Some("network_assignment_timeout".to_string());
    }

    pub fn clear_calls(&self) {
        self.calls.lock().unwrap().clear();
    }

    pub fn recorded_calls(&self) -> Vec<MockNetworkOp> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl crate::kubelet::pod_runtime::network::PodNetworkRuntime for MockPodNetworkRuntime {
    async fn read_assignment(
        &self,
        sandbox_id: &str,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
        host_network: bool,
    ) -> anyhow::Result<crate::kubelet::pod_repository::PodNetworkAssignment> {
        if let Some(ref f) = *self.fail.lock().unwrap() {
            if f == "read_assignment" {
                anyhow::bail!("injected failure");
            }
            if f == "network_assignment_timeout" {
                return Err(anyhow::Error::new(
                    crate::kubelet::pod_startup_error::PodStartupErrorKind::NetworkAssignmentTimedOut,
                )
                .context("pod network assignment wait timed out for sandbox"));
            }
        }
        self.calls
            .lock()
            .unwrap()
            .push(MockNetworkOp::ReadAssignment {
                sandbox_id: sandbox_id.to_string(),
                namespace: key.namespace.clone(),
                name: key.name.clone(),
                uid: key.uid.clone(),
                host_network,
            });
        Ok(crate::kubelet::pod_repository::PodNetworkAssignment {
            pod_ip: "10.0.0.1".to_string(),
            host_ip: "192.168.1.1".to_string(),
        })
    }

    async fn release_sandbox_network(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
        sandbox_id: &str,
    ) -> anyhow::Result<()> {
        if let Some(ref f) = *self.fail.lock().unwrap()
            && f == "release_sandbox_network"
        {
            anyhow::bail!("injected failure");
        }
        self.calls
            .lock()
            .unwrap()
            .push(MockNetworkOp::ReleaseSandboxNetwork {
                namespace: key.namespace.clone(),
                name: key.name.clone(),
                uid: key.uid.clone(),
                sandbox_id: sandbox_id.to_string(),
            });
        Ok(())
    }
}

// --- MockPodRuntimeStore ---

pub struct MockPodRuntimeStore {
    sandboxes: Mutex<std::collections::HashMap<(String, String, String), String>>,
    calls: Mutex<Vec<String>>,
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
        }
    }

    pub fn clear_calls(&self) {
        self.calls.lock().unwrap().clear();
    }

    pub fn recorded_calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl crate::kubelet::pod_runtime::store::PodRuntimeStore for MockPodRuntimeStore {
    async fn record_sandbox(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
        sandbox_id: &str,
    ) -> anyhow::Result<()> {
        self.calls.lock().unwrap().push(format!(
            "record_sandbox:{}/{}/{}={}",
            key.namespace, key.name, key.uid, sandbox_id
        ));
        self.sandboxes.lock().unwrap().insert(
            (key.namespace.clone(), key.name.clone(), key.uid.clone()),
            sandbox_id.to_string(),
        );
        Ok(())
    }

    async fn get_sandbox_id(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
    ) -> anyhow::Result<Option<String>> {
        self.calls.lock().unwrap().push(format!(
            "get_sandbox_id:{}/{}/{}",
            key.namespace, key.name, key.uid
        ));
        Ok(self
            .sandboxes
            .lock()
            .unwrap()
            .get(&(key.namespace.clone(), key.name.clone(), key.uid.clone()))
            .cloned())
    }

    async fn delete_sandbox(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
    ) -> anyhow::Result<()> {
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

    async fn get_sandbox_id_by_name(
        &self,
        namespace: &str,
        pod_name: &str,
    ) -> anyhow::Result<Option<String>> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("get_sandbox_id_by_name:{}/{}", namespace, pod_name));
        // Return the first match by namespace/name (for name-only lookup tests)
        let sb = self.sandboxes.lock().unwrap();
        for ((ns, name, _uid), sid) in sb.iter() {
            if ns == namespace && name == pod_name {
                return Ok(Some(sid.clone()));
            }
        }
        Ok(None)
    }
}

// --- MockPodSlotAdmission ---

pub struct MockPodSlotAdmission {
    calls: Mutex<Vec<String>>,
    slot_tx: tokio::sync::broadcast::Sender<crate::datastore::PodSlotAdmissionEvent>,
    admitted: Mutex<bool>,
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
impl crate::kubelet::pod_runtime::store::PodSlotAdmission for MockPodSlotAdmission {
    fn subscribe(
        &self,
    ) -> tokio::sync::broadcast::Receiver<crate::datastore::PodSlotAdmissionEvent> {
        self.slot_tx.subscribe()
    }

    async fn try_admit(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
        node_name: &str,
    ) -> anyhow::Result<crate::datastore::PodSlotAdmissionResult> {
        self.calls.lock().unwrap().push(format!(
            "try_admit:{}/{}/{}@{}",
            key.namespace, key.name, key.uid, node_name
        ));
        if *self.admitted.lock().unwrap() {
            Ok(crate::datastore::PodSlotAdmissionResult::Admitted {
                resource_version: 1,
            })
        } else {
            Ok(crate::datastore::PodSlotAdmissionResult::Blocked {
                blocking_uid: "blocker-uid".into(),
                blocking_node: "blocker-node".into(),
                state: crate::datastore::PodSlotAdmissionState::Terminating,
                resource_version: 1,
            })
        }
    }

    async fn clear_slot(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
    ) -> anyhow::Result<()> {
        self.calls.lock().unwrap().push(format!(
            "clear_slot:{}/{}/{}",
            key.namespace, key.name, key.uid
        ));
        Ok(())
    }
}

// --- MockPodFilesystem ---

pub struct MockPodFilesystem {
    calls: Mutex<Vec<String>>,
    termination_messages: Mutex<HashMap<String, String>>,
}

impl Default for MockPodFilesystem {
    fn default() -> Self {
        Self::new()
    }
}

impl MockPodFilesystem {
    pub fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            termination_messages: Mutex::new(HashMap::new()),
        }
    }

    pub fn clear_calls(&self) {
        self.calls.lock().unwrap().clear();
    }

    pub fn recorded_calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }

    pub fn set_termination_message(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
        container_name: &str,
        message: &str,
    ) {
        self.termination_messages.lock().unwrap().insert(
            Self::termination_key(key, container_name),
            message.to_string(),
        );
    }

    fn termination_key(
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
        container_name: &str,
    ) -> String {
        format!(
            "{}/{}/{}/{}",
            key.namespace, key.name, key.uid, container_name
        )
    }
}

#[async_trait::async_trait]
impl crate::kubelet::pod_runtime::filesystem::PodFilesystem for MockPodFilesystem {
    async fn write_hosts(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
        _pod: &serde_json::Value,
    ) -> anyhow::Result<()> {
        self.calls.lock().unwrap().push(format!(
            "write_hosts:{}/{}/{}",
            key.namespace, key.name, key.uid
        ));
        Ok(())
    }

    async fn create_log_directory(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
    ) -> anyhow::Result<()> {
        self.calls.lock().unwrap().push(format!(
            "create_log:{}/{}/{}",
            key.namespace, key.name, key.uid
        ));
        Ok(())
    }

    async fn ensure_termination_log_file(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
        container_name: &str,
    ) -> String {
        self.calls.lock().unwrap().push(format!(
            "ensure_termination_log:{}/{}/{}/{}",
            key.namespace, key.name, key.uid, container_name
        ));
        format!(
            "mock://termination/{}/{}/{}/{}",
            key.namespace, key.name, key.uid, container_name
        )
    }

    async fn read_termination_message(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
        container_name: &str,
        policy: &str,
        exit_code: i32,
    ) -> String {
        self.calls.lock().unwrap().push(format!(
            "read_termination_message:{}/{}/{}/{}:{}:{}",
            key.namespace, key.name, key.uid, container_name, policy, exit_code
        ));
        self.termination_messages
            .lock()
            .unwrap()
            .get(&Self::termination_key(key, container_name))
            .cloned()
            .unwrap_or_default()
    }

    async fn cleanup_cgroup(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
        sandbox_id: &str,
    ) -> anyhow::Result<()> {
        self.calls.lock().unwrap().push(format!(
            "cleanup_cgroup:{}/{}/{}/{}",
            key.namespace, key.name, key.uid, sandbox_id
        ));
        Ok(())
    }

    async fn apply_fs_group(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
        _pod: &serde_json::Value,
    ) -> anyhow::Result<()> {
        self.calls.lock().unwrap().push(format!(
            "apply_fs_group:{}/{}/{}",
            key.namespace, key.name, key.uid
        ));
        Ok(())
    }

    async fn cleanup_pod_filesystem(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
    ) -> anyhow::Result<()> {
        self.calls.lock().unwrap().push(format!(
            "cleanup_fs:{}/{}/{}",
            key.namespace, key.name, key.uid
        ));
        Ok(())
    }
}

// --- MockPodVolumeRuntime ---

pub struct MockPodVolumeRuntime {
    calls: Mutex<Vec<String>>,
    process_error: Mutex<Option<String>>,
}

impl Default for MockPodVolumeRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl MockPodVolumeRuntime {
    pub fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            process_error: Mutex::new(None),
        }
    }

    pub fn clear_calls(&self) {
        self.calls.lock().unwrap().clear();
    }

    pub fn fail_process_volumes(&self, message: impl Into<String>) {
        *self.process_error.lock().unwrap() = Some(message.into());
    }

    pub fn recorded_calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl crate::kubelet::pod_runtime::volumes::PodVolumeRuntime for MockPodVolumeRuntime {
    async fn process_volumes(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
        _pod: &serde_json::Value,
    ) -> anyhow::Result<std::collections::HashMap<String, String>> {
        self.calls.lock().unwrap().push(format!(
            "process_volumes:{}/{}/{}",
            key.namespace, key.name, key.uid
        ));
        if let Some(message) = self.process_error.lock().unwrap().take() {
            anyhow::bail!("{message}");
        }
        Ok(std::collections::HashMap::new())
    }

    async fn cleanup_volumes(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
        _pod: &serde_json::Value,
    ) -> anyhow::Result<()> {
        self.calls.lock().unwrap().push(format!(
            "cleanup_volumes:{}/{}/{}",
            key.namespace, key.name, key.uid
        ));
        Ok(())
    }
}

// --- MockProbeRuntime ---

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MockProbeCall {
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

pub struct MockProbeRuntime {
    calls: Mutex<Vec<MockProbeCall>>,
    started_sandboxes: Mutex<std::collections::HashMap<PodRuntimeKey, (String, bool)>>,
}

impl Default for MockProbeRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl MockProbeRuntime {
    pub fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            started_sandboxes: Mutex::new(std::collections::HashMap::new()),
        }
    }

    pub fn clear_calls(&self) {
        self.calls.lock().unwrap().clear();
        self.started_sandboxes.lock().unwrap().clear();
    }

    pub fn recorded_calls(&self) -> Vec<MockProbeCall> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl crate::kubelet::pod_runtime::probes::ProbeRuntime for MockProbeRuntime {
    async fn record_started_sandbox(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
        sandbox_id: &str,
    ) -> anyhow::Result<crate::kubelet::pod_runtime::probes::StartupFinalizationAction> {
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

// --- MockHostPortRuntime ---

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MockHostPortOp {
    Add {
        namespace: String,
        name: String,
        uid: String,
    },
    Remove {
        namespace: String,
        name: String,
        uid: String,
    },
    Check {
        namespace: String,
        name: String,
        uid: String,
    },
}

pub struct MockHostPortRuntime {
    calls: Mutex<Vec<MockHostPortOp>>,
    check_error: Mutex<Option<String>>,
}

impl Default for MockHostPortRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl MockHostPortRuntime {
    pub fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            check_error: Mutex::new(None),
        }
    }

    pub fn clear_calls(&self) {
        self.calls.lock().unwrap().clear();
    }

    pub fn recorded_calls(&self) -> Vec<MockHostPortOp> {
        self.calls.lock().unwrap().clone()
    }

    pub fn reject_next_check(&self, message: &str) {
        *self.check_error.lock().unwrap() = Some(message.to_string());
    }
}

#[async_trait::async_trait]
impl crate::kubelet::pod_runtime::hostports::HostPortRuntime for MockHostPortRuntime {
    async fn add_host_ports(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
        _pod: &serde_json::Value,
    ) -> anyhow::Result<()> {
        self.calls.lock().unwrap().push(MockHostPortOp::Add {
            namespace: key.namespace.clone(),
            name: key.name.clone(),
            uid: key.uid.clone(),
        });
        Ok(())
    }

    async fn remove_host_ports(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
        _pod: &serde_json::Value,
    ) -> anyhow::Result<()> {
        self.calls.lock().unwrap().push(MockHostPortOp::Remove {
            namespace: key.namespace.clone(),
            name: key.name.clone(),
            uid: key.uid.clone(),
        });
        Ok(())
    }

    async fn check_host_port_admission(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
        _pod: &serde_json::Value,
    ) -> anyhow::Result<()> {
        self.calls.lock().unwrap().push(MockHostPortOp::Check {
            namespace: key.namespace.clone(),
            name: key.name.clone(),
            uid: key.uid.clone(),
        });
        if let Some(message) = self.check_error.lock().unwrap().take() {
            anyhow::bail!("{message}");
        }
        Ok(())
    }
}

// --- MockPodEventSink ---

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MockPodEvent {
    pub namespace: String,
    pub name: String,
    pub uid: String,
    pub event_type: String,
    pub reason: String,
    pub message: String,
    pub reporting_component: String,
    pub node_name: String,
}

pub struct MockPodEventSink {
    events: Mutex<Vec<MockPodEvent>>,
}

impl Default for MockPodEventSink {
    fn default() -> Self {
        Self::new()
    }
}

impl MockPodEventSink {
    pub fn new() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
        }
    }

    pub fn clear_events(&self) {
        self.events.lock().unwrap().clear();
    }

    pub fn recorded_events(&self) -> Vec<MockPodEvent> {
        self.events.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl crate::kubelet::pod_runtime::events::PodEventSink for MockPodEventSink {
    async fn emit_pod_event(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
        event_type: &str,
        reason: &str,
        message: &str,
        reporting_component: &str,
        node_name: &str,
    ) -> anyhow::Result<()> {
        self.events.lock().unwrap().push(MockPodEvent {
            namespace: key.namespace.clone(),
            name: key.name.clone(),
            uid: key.uid.clone(),
            event_type: event_type.to_string(),
            reason: reason.to_string(),
            message: message.to_string(),
            reporting_component: reporting_component.to_string(),
            node_name: node_name.to_string(),
        });
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

    async fn reconcile_runtime(&self, key: PodRuntimeKey) -> anyhow::Result<()> {
        self.check_fail("reconcile_runtime")?;
        self.calls
            .lock()
            .unwrap()
            .push(MockRuntimeCall::from_key("reconcile_runtime", &key));
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
        request: super::service::PodSlotAdmissionRequest,
        reply_to: crate::kubelet::pod_lifecycle_router::LifecycleReplyHandle,
        cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        self.check_fail("check_slot_admission")?;
        let super::service::PodSlotAdmissionRequest {
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
            .route(crate::kubelet::pod_lifecycle_core::message::LifecycleMessage::SlotAdmissionGranted {
                key: crate::kubelet::pod_lifecycle_core::message::PodLifecycleKey::new(
                    &key.namespace,
                    &key.name,
                    &key.uid,
                ),
                operation_id,
                pod,
                resource_version,
                start_after_admit,
            })
            .await;
        Ok(())
    }

    async fn schedule_retry(
        &self,
        key: PodRuntimeKey,
        delay: std::time::Duration,
        _reply_to: crate::kubelet::pod_lifecycle_router::LifecycleReplyHandle,
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
        _reply_to: crate::kubelet::pod_lifecycle_router::LifecycleReplyHandle,
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
impl crate::kubelet::pod_runtime::deletion_finalizer::PodDeletionFinalizer
    for MockPodDeletionFinalizer
{
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
    secrets: Mutex<HashMap<EnvSourceKey, crate::datastore::Resource>>,
    config_maps: Mutex<HashMap<EnvSourceKey, crate::datastore::Resource>>,
    services: Mutex<HashMap<String, Vec<crate::datastore::Resource>>>,
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
    ) -> crate::datastore::Resource {
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
        crate::datastore::Resource {
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
impl crate::kubelet::pod_env::EnvSourceReader for MockEnvSourceReader {
    async fn secret(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
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
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
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

    async fn services(&self, namespace: &str) -> anyhow::Result<Vec<crate::datastore::Resource>> {
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
    outcome: Mutex<super::hooks::HookOutcome>,
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
            outcome: Mutex::new(super::hooks::HookOutcome::Succeeded),
        }
    }

    pub fn set_outcome(&self, outcome: super::hooks::HookOutcome) {
        *self.outcome.lock().unwrap() = outcome;
    }

    pub fn recorded_calls(&self) -> Vec<MockHookCall> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl super::hooks::PodHookRuntime for MockPodHookRuntime {
    async fn execute_post_start(
        &self,
        container_id: &str,
        pod_ip: &str,
        _hook: &serde_json::Value,
        _container_spec: &serde_json::Value,
    ) -> anyhow::Result<super::hooks::HookOutcome> {
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
    ) -> anyhow::Result<super::hooks::HookOutcome> {
        self.calls.lock().unwrap().push(MockHookCall {
            hook_type: "preStop".to_string(),
            container_id: container_id.to_string(),
            pod_ip: pod_ip.to_string(),
        });
        Ok(self.outcome.lock().unwrap().clone())
    }
}

// --- PodRuntimeHarness ---

/// Wires every mockable port for `RealPodRuntimeService` unit tests.
/// Extended task-by-task; starts with all ports needed by Task 8.1.
pub struct PodRuntimeHarness {
    pub cri: std::sync::Arc<MockCriRuntime>,
    pub container_control: std::sync::Arc<MockContainerRuntimeControl>,
    pub network: std::sync::Arc<MockPodNetworkRuntime>,
    pub store: std::sync::Arc<MockPodRuntimeStore>,
    pub slot_admission: std::sync::Arc<MockPodSlotAdmission>,
    pub db_handle: crate::datastore::DatastoreHandle,
    pub repo: std::sync::Arc<crate::kubelet::pod_repository::PodRepository>,
    pub filesystem: std::sync::Arc<MockPodFilesystem>,
    pub volumes: std::sync::Arc<MockPodVolumeRuntime>,
    pub probes: std::sync::Arc<MockProbeRuntime>,
    pub hostports: std::sync::Arc<MockHostPortRuntime>,
    pub events: std::sync::Arc<MockPodEventSink>,
    pub hooks: std::sync::Arc<MockPodHookRuntime>,
    pub env_source: std::sync::Arc<MockEnvSourceReader>,
    pub finalizer: std::sync::Arc<MockPodDeletionFinalizer>,
    pub supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
    pub node_view: std::sync::Arc<FakeNode>,
    pub runtime: std::sync::Arc<crate::kubelet::pod_runtime::service::RealPodRuntimeService>,
}

impl PodRuntimeHarness {
    /// Construct with all-default mocks and an in-memory repository.
    pub async fn new() -> Self {
        Self::new_with_runtime_config(crate::kubelet::pod_runtime::service::RuntimeConfig {
            node_name: "test-node".into(),
            service_cidr: "10.43.128.0/17".into(),
            containerd_namespace: "klights-test".into(),
        })
        .await
    }

    pub async fn new_with_runtime_config(
        config: crate::kubelet::pod_runtime::service::RuntimeConfig,
    ) -> Self {
        let (ds, handle) = crate::datastore::test_support::in_memory_with_handle().await;
        // The API create path enforces the upstream NamespaceLifecycle rule
        // (target namespace must exist). Seed the conventional namespaces these
        // runtime tests place pods in, mirroring a live cluster.
        seed_runtime_test_namespaces(&handle).await;
        // Keep ds alive so the handle stays valid.
        std::mem::forget(ds);
        let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let side_effects = std::sync::Arc::new(crate::side_effects::SideEffectRegistry::new());
        let metrics: std::sync::Arc<crate::side_effects::SideEffectMetrics> =
            crate::side_effects::SideEffectMetrics::new();
        let parts = crate::kubelet::pod_repository::PodRepository::build_parts(
            crate::kubelet::pod_repository::PodRepositoryBuildConfig {
                db: handle.clone(),
                supervisor: supervisor.clone(),
                side_effects,
                metrics,
                network_events: crate::networking::global_pod_network_events(),
                scheduling_mode:
                    crate::kubelet::pod_repository::api::PodSchedulingMode::InlineSingleNode,
                outbox: None,
                cluster_api: None,
            },
        );
        let repo = std::sync::Arc::new(parts.repository);
        let cri = std::sync::Arc::new(MockCriRuntime::new());
        let container_control = std::sync::Arc::new(MockContainerRuntimeControl::new());
        let network = std::sync::Arc::new(MockPodNetworkRuntime::new());
        let store = std::sync::Arc::new(MockPodRuntimeStore::new());
        let slot_admission = std::sync::Arc::new(MockPodSlotAdmission::new());
        let filesystem = std::sync::Arc::new(MockPodFilesystem::new());
        let volumes = std::sync::Arc::new(MockPodVolumeRuntime::new());
        let probes = std::sync::Arc::new(MockProbeRuntime::new());
        let hostports = std::sync::Arc::new(MockHostPortRuntime::new());
        let events = std::sync::Arc::new(MockPodEventSink::new());
        let hooks = std::sync::Arc::new(MockPodHookRuntime::new());
        let env_source = std::sync::Arc::new(MockEnvSourceReader::new());
        let finalizer = std::sync::Arc::new(MockPodDeletionFinalizer::new());

        let node_view = std::sync::Arc::new(FakeNode::new(
            &config.node_name,
            super::super::pod_cluster_runtime::RuntimeNodeRole::Worker,
        ));
        let cluster_view = std::sync::Arc::new(
            super::super::pod_cluster_runtime::WorkerClusterRuntimeView::new(
                repo.clone(),
                config.node_name.clone(),
            ),
        );

        let runtime = std::sync::Arc::new(
            crate::kubelet::pod_runtime::service::RealPodRuntimeService::new(
                RealPodRuntimeServiceDependencies {
                    cri: cri.clone(),
                    container_control: container_control.clone(),
                    network: network.clone(),
                    store: store.clone(),
                    slot_admission: slot_admission.clone(),
                    repository: repo.clone(),
                    filesystem: filesystem.clone(),
                    volumes: volumes.clone(),
                    probes: probes.clone(),
                    hostports: hostports.clone(),
                    events: events.clone(),
                    hooks: hooks.clone(),
                    env_source: env_source.clone(),
                    finalizer: finalizer.clone(),
                    supervisor: supervisor.clone(),
                    config,
                    node_view: node_view.clone(),
                    cluster_view,
                },
            ),
        );

        Self {
            cri,
            container_control,
            network,
            store,
            slot_admission,
            db_handle: handle,
            repo,
            filesystem,
            volumes,
            probes,
            hostports,
            events,
            hooks,
            env_source,
            finalizer,
            supervisor,
            node_view,
            runtime,
        }
    }

    pub async fn create_runtime_pod(&self, pod: Value) {
        use crate::kubelet::pod_repository::PodObjectWriter;

        let namespace = pod
            .pointer("/metadata/namespace")
            .and_then(|v| v.as_str())
            .unwrap_or("default")
            .to_string();
        let name = pod
            .pointer("/metadata/name")
            .and_then(|v| v.as_str())
            .expect("test pod must have metadata.name")
            .to_string();
        let node_name = pod
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str())
            .unwrap_or("test-node")
            .to_string();

        // The API create path enforces the upstream NamespaceLifecycle rule
        // (target namespace must exist). Ensure the pod's namespace is present,
        // mirroring a live cluster where the namespace always pre-exists.
        crate::datastore::sqlite::test_support::ensure_namespace(
            self.db_handle.as_ref(),
            &namespace,
        )
        .await;

        self.repo
            .create_controller_pod(&namespace, &name, &node_name, pod)
            .await
            .expect("create runtime test pod");
    }

    pub async fn stored_pod(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
    ) -> Value {
        use crate::kubelet::pod_repository::PodReader;

        self.repo
            .get_pod_for_uid(&key.namespace, &key.name, &key.uid)
            .await
            .expect("read runtime test pod")
            .expect("runtime test pod should exist")
            .data
            .as_ref()
            .clone()
    }

    pub async fn start_pod_through_runtime(
        &self,
        key: crate::kubelet::pod_runtime::service::PodRuntimeKey,
        pod: Value,
    ) -> crate::kubelet::pod_runtime::service::PodStartResult {
        self.runtime
            .start_pod(key, Some(pod), CancellationToken::new())
            .await
            .expect("start pod through runtime")
    }

    pub fn simulate_running_containers(&self, containers: impl IntoIterator<Item = String>) {
        self.container_control.set_container_states(
            containers
                .into_iter()
                .map(|container_id| {
                    (
                        container_id,
                        crate::kubelet::pod_runtime::cri::ContainerRuntimeState::Running,
                    )
                })
                .collect(),
        );
    }

    pub async fn reconcile_runtime(
        &self,
        key: crate::kubelet::pod_runtime::service::PodRuntimeKey,
    ) {
        self.runtime
            .reconcile_runtime(key)
            .await
            .expect("reconcile runtime");
    }
}

// --- FakeNode ---

/// Fake node implementing `NodeRuntimeView` for multi-node tests.
pub struct FakeNode {
    node_name: String,
    role: super::super::pod_cluster_runtime::RuntimeNodeRole,
}

impl FakeNode {
    pub fn new(node_name: &str, role: super::super::pod_cluster_runtime::RuntimeNodeRole) -> Self {
        Self {
            node_name: node_name.to_string(),
            role,
        }
    }
}

impl super::super::pod_cluster_runtime::NodeRuntimeView for FakeNode {
    fn node_name(&self) -> &str {
        &self.node_name
    }

    fn role(&self) -> super::super::pod_cluster_runtime::RuntimeNodeRole {
        self.role.clone()
    }

    fn owns_pod_runtime(&self, pod: &serde_json::Value) -> bool {
        pod.pointer("/spec/nodeName")
            .and_then(|v| v.as_str())
            .is_some_and(|n| n == self.node_name)
    }
}

// --- FakeCluster ---

/// Records forwarded status updates for multi-node tests.
type StatusForward = (PodRuntimeKey, serde_json::Value);
/// Records enqueued storage commands for multi-node tests.
type StorageCommandRecord = (PodRuntimeKey, crate::datastore::command::StorageCommand);

/// Fake cluster implementing `ClusterRuntimeView` and `ReplicationRuntime`
/// for multi-node tests.
pub struct FakeCluster {
    fresh_pods: Mutex<std::collections::HashMap<(String, String), crate::datastore::Resource>>,
    status_forwards: Mutex<Vec<StatusForward>>,
    storage_commands: Mutex<Vec<StorageCommandRecord>>,
}

impl Default for FakeCluster {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeCluster {
    pub fn new() -> Self {
        Self {
            fresh_pods: Mutex::new(std::collections::HashMap::new()),
            status_forwards: Mutex::new(Vec::new()),
            storage_commands: Mutex::new(Vec::new()),
        }
    }

    pub fn set_fresh_pod(&self, ns: &str, name: &str, pod: crate::datastore::Resource) {
        self.fresh_pods
            .lock()
            .unwrap()
            .insert((ns.to_string(), name.to_string()), pod);
    }

    pub fn recorded_status_forwards(&self) -> Vec<StatusForward> {
        self.status_forwards.lock().unwrap().clone()
    }

    pub fn recorded_storage_commands(&self) -> Vec<StorageCommandRecord> {
        self.storage_commands.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl super::super::pod_cluster_runtime::ClusterRuntimeView for FakeCluster {
    async fn get_fresh_pod(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        Ok(self
            .fresh_pods
            .lock()
            .unwrap()
            .get(&(namespace.to_string(), name.to_string()))
            .cloned())
    }

    async fn forward_pod_status(
        &self,
        key: &PodRuntimeKey,
        status: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.status_forwards
            .lock()
            .unwrap()
            .push((key.clone(), status));
        Ok(crate::datastore::Resource {
            id: 0,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some(key.namespace.clone()),
            name: key.name.clone(),
            uid: key.uid.clone(),
            data: std::sync::Arc::new(serde_json::json!({
                "metadata": {
                    "namespace": key.namespace,
                    "name": key.name,
                    "uid": key.uid,
                },
            })),
            resource_version: 1,
        })
    }
}

#[async_trait::async_trait]
impl super::super::pod_cluster_runtime::ReplicationRuntime for FakeCluster {
    async fn enqueue_storage_command(
        &self,
        key: &PodRuntimeKey,
        command: crate::datastore::command::StorageCommand,
    ) -> anyhow::Result<()> {
        self.storage_commands
            .lock()
            .unwrap()
            .push((key.clone(), command));
        Ok(())
    }
}

/// Harness for actor-owned deletion finalizer tests (Task 10).
pub struct PodDeletionFinalizerHarness {
    pub repo: std::sync::Arc<crate::kubelet::pod_repository::PodRepository>,
    pub finalizer:
        std::sync::Arc<dyn crate::kubelet::pod_runtime::deletion_finalizer::PodDeletionFinalizer>,
}

/// Harness for `PodApiFacade` tests (Task 6).
pub struct PodApiFacadeHarness {
    pub repo: std::sync::Arc<crate::kubelet::pod_repository::PodRepository>,
    pub api: std::sync::Arc<crate::kubelet::pod_api::PodApiFacade>,
}

#[test]
fn pod_runtime_test_fixtures_compile() {
    let _ = pod_json("a", "b", "c", "d");
}
