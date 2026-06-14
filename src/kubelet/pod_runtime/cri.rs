use k8s_cri::v1::{ContainerConfig, ContainerStatusResponse, ExecSyncResponse, PodSandboxConfig};

/// Container state as observed at the CRI boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContainerRuntimeState {
    Created,
    Running,
    Exited,
    Unknown,
}

impl ContainerRuntimeState {
    pub fn from_cri_state_i32(state: i32) -> Self {
        match k8s_cri::v1::ContainerState::try_from(state)
            .unwrap_or(k8s_cri::v1::ContainerState::ContainerUnknown)
        {
            k8s_cri::v1::ContainerState::ContainerCreated => Self::Created,
            k8s_cri::v1::ContainerState::ContainerRunning => Self::Running,
            k8s_cri::v1::ContainerState::ContainerExited => Self::Exited,
            k8s_cri::v1::ContainerState::ContainerUnknown => Self::Unknown,
        }
    }

    pub fn is_running(self) -> bool {
        self == Self::Running
    }

    pub fn has_started(self) -> bool {
        matches!(self, Self::Running | Self::Exited)
    }
}

impl From<k8s_cri::v1::ContainerState> for ContainerRuntimeState {
    fn from(state: k8s_cri::v1::ContainerState) -> Self {
        Self::from_cri_state_i32(state as i32)
    }
}

#[cfg(test)]
impl From<&str> for ContainerRuntimeState {
    fn from(state: &str) -> Self {
        match state {
            "running" => Self::Running,
            "created" | "waiting" => Self::Created,
            "exited" => Self::Exited,
            "unknown" => Self::Unknown,
            numeric => numeric
                .parse::<i32>()
                .map(Self::from_cri_state_i32)
                .unwrap_or(Self::Unknown),
        }
    }
}

#[cfg(test)]
impl From<String> for ContainerRuntimeState {
    fn from(state: String) -> Self {
        Self::from(state.as_str())
    }
}

/// Container lifecycle event kind exposed by the CRI runtime port.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CriRuntimeContainerEventKind {
    Created,
    Started,
    Stopped,
    Deleted,
}

/// Container lifecycle event exposed by the CRI runtime port.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CriRuntimeContainerEvent {
    pub container_id: String,
    pub kind: CriRuntimeContainerEventKind,
}

/// Event stream returned by `CriRuntime::subscribe_container_events`.
#[async_trait::async_trait]
pub trait CriRuntimeContainerEventStream: Send {
    async fn next_event(&mut self) -> anyhow::Result<Option<CriRuntimeContainerEvent>>;
}

/// CRI-compatible container runtime port used by `PodRuntimeService`.
/// Production adapter wraps `SharedCriClient`; tests may model dockerd-like
/// behavior behind this port.
#[async_trait::async_trait]
pub trait CriRuntime: Send + Sync {
    /// Check whether an image is already present.
    async fn image_status(&self, image: &str) -> anyhow::Result<bool>;

    /// Pull an image. Returns the image reference.
    async fn pull_image(&self, image: &str) -> anyhow::Result<String>;

    /// Create and run a pod sandbox. Returns the sandbox id.
    async fn run_pod_sandbox(&self, sandbox_config: PodSandboxConfig) -> anyhow::Result<String>;

    /// Stop a pod sandbox.
    async fn stop_pod_sandbox(&self, sandbox_id: &str) -> anyhow::Result<()>;

    /// Remove a pod sandbox.
    async fn remove_pod_sandbox(&self, sandbox_id: &str) -> anyhow::Result<()>;

    /// List pod sandboxes, optionally filtered by pod UID label.
    /// Returns a list of (sandbox_id, state) tuples.
    async fn list_pod_sandboxes(
        &self,
        pod_uid_filter: Option<&str>,
    ) -> anyhow::Result<Vec<(String, String)>>;

    /// Create a container inside a sandbox. Returns the container id.
    async fn create_container(
        &self,
        container_config: ContainerConfig,
        sandbox_id: &str,
        sandbox_config: PodSandboxConfig,
    ) -> anyhow::Result<String>;

    /// Start a container.
    async fn start_container(&self, container_id: &str) -> anyhow::Result<()>;

    /// Stop a container.
    async fn stop_container(&self, container_id: &str, timeout_seconds: i64) -> anyhow::Result<()>;

    /// Remove a container.
    async fn remove_container(&self, container_id: &str) -> anyhow::Result<()>;

    /// Get container status.
    async fn container_status(&self, container_id: &str)
    -> anyhow::Result<ContainerStatusResponse>;

    /// Execute a command synchronously in a container.
    async fn exec_sync(
        &self,
        container_id: &str,
        command: &[String],
        timeout_seconds: i64,
    ) -> anyhow::Result<ExecSyncResponse>;

    /// Subscribe to CRI container lifecycle events.
    async fn subscribe_container_events(
        &self,
    ) -> anyhow::Result<Box<dyn CriRuntimeContainerEventStream>>;
}

/// Container runtime control extension for low-level operations.
#[async_trait::async_trait]
pub trait ContainerRuntimeControl: Send + Sync {
    /// List containers, optionally filtered by sandbox id.
    async fn list_containers(
        &self,
        sandbox_id_filter: Option<&str>,
    ) -> anyhow::Result<Vec<(String, ContainerRuntimeState)>>;

    /// Resolve a container id to owning Pod namespace/name through CRI metadata.
    async fn pod_metadata_for_container(
        &self,
        container_id: &str,
    ) -> anyhow::Result<Option<(String, String)>>;
}

// --- Production adapter: SharedCriRuntime ---

use crate::kubelet::cri::SharedCriClient;
use crate::kubelet::cri_events::{KubeletEvent, KubeletEventKind};

/// Production CRI adapter implementing `CriRuntime`.
/// Each method clones `SharedCriClient::client()` and calls the
/// existing concrete CRI method. No Mutex.
pub struct SharedCriRuntime {
    shared: SharedCriClient,
}

impl SharedCriRuntime {
    pub fn new(shared: SharedCriClient) -> Self {
        Self { shared }
    }
}

struct SharedCriRuntimeEventStream {
    inner: tonic::codec::Streaming<crate::kubelet::cri_events::CriContainerEventResponse>,
}

#[async_trait::async_trait]
impl CriRuntimeContainerEventStream for SharedCriRuntimeEventStream {
    async fn next_event(&mut self) -> anyhow::Result<Option<CriRuntimeContainerEvent>> {
        loop {
            let Some(raw) = self.inner.message().await? else {
                return Ok(None);
            };
            let Some(event) = KubeletEvent::from_cri(raw) else {
                continue;
            };
            let kind = match event.kind {
                KubeletEventKind::Created => CriRuntimeContainerEventKind::Created,
                KubeletEventKind::Started => CriRuntimeContainerEventKind::Started,
                KubeletEventKind::Stopped => CriRuntimeContainerEventKind::Stopped,
                KubeletEventKind::Deleted => CriRuntimeContainerEventKind::Deleted,
            };
            return Ok(Some(CriRuntimeContainerEvent {
                container_id: event.container_id,
                kind,
            }));
        }
    }
}

#[async_trait::async_trait]
impl CriRuntime for SharedCriRuntime {
    async fn image_status(&self, image: &str) -> anyhow::Result<bool> {
        self.shared.client().image_status(image).await
    }

    async fn pull_image(&self, image: &str) -> anyhow::Result<String> {
        self.shared.client().pull_image(image).await
    }

    async fn run_pod_sandbox(&self, sandbox_config: PodSandboxConfig) -> anyhow::Result<String> {
        self.shared.client().run_pod_sandbox(sandbox_config).await
    }

    async fn stop_pod_sandbox(&self, sandbox_id: &str) -> anyhow::Result<()> {
        self.shared.client().stop_pod_sandbox(sandbox_id).await
    }

    async fn remove_pod_sandbox(&self, sandbox_id: &str) -> anyhow::Result<()> {
        self.shared.client().remove_pod_sandbox(sandbox_id).await
    }

    async fn list_pod_sandboxes(
        &self,
        pod_uid_filter: Option<&str>,
    ) -> anyhow::Result<Vec<(String, String)>> {
        let mut client = self.shared.client();
        let mut items =
            crate::kubelet::cri::CriClient::list_pod_sandboxes(&mut client, None).await?;
        if let Some(pod_uid) = pod_uid_filter.filter(|uid| !uid.trim().is_empty()) {
            items.retain(|sb| {
                sb.metadata
                    .as_ref()
                    .map(|metadata| metadata.uid == pod_uid)
                    .unwrap_or(false)
            });
        }
        Ok(items
            .into_iter()
            .map(|sb| (sb.id, format!("{}", sb.state)))
            .collect())
    }

    async fn create_container(
        &self,
        container_config: ContainerConfig,
        sandbox_id: &str,
        sandbox_config: PodSandboxConfig,
    ) -> anyhow::Result<String> {
        let mut client = self.shared.client();
        crate::kubelet::cri::CriClient::create_container(
            &mut client,
            sandbox_id,
            container_config,
            sandbox_config,
        )
        .await
    }

    async fn start_container(&self, container_id: &str) -> anyhow::Result<()> {
        self.shared.client().start_container(container_id).await
    }

    async fn stop_container(&self, container_id: &str, timeout_seconds: i64) -> anyhow::Result<()> {
        self.shared
            .client()
            .stop_container(container_id, timeout_seconds)
            .await
    }

    async fn remove_container(&self, container_id: &str) -> anyhow::Result<()> {
        self.shared.client().remove_container(container_id).await
    }

    async fn container_status(
        &self,
        container_id: &str,
    ) -> anyhow::Result<ContainerStatusResponse> {
        self.shared.client().container_status(container_id).await
    }

    async fn exec_sync(
        &self,
        container_id: &str,
        command: &[String],
        timeout_seconds: i64,
    ) -> anyhow::Result<ExecSyncResponse> {
        self.shared
            .client()
            .exec_sync(container_id, command, timeout_seconds)
            .await
    }

    async fn subscribe_container_events(
        &self,
    ) -> anyhow::Result<Box<dyn CriRuntimeContainerEventStream>> {
        let stream = self.shared.client().subscribe_container_events().await?;
        Ok(Box::new(SharedCriRuntimeEventStream { inner: stream }))
    }
}

// --- Production adapter: ContainerRuntimeControl on SharedCriRuntime ---

#[async_trait::async_trait]
impl ContainerRuntimeControl for SharedCriRuntime {
    async fn list_containers(
        &self,
        sandbox_id_filter: Option<&str>,
    ) -> anyhow::Result<Vec<(String, ContainerRuntimeState)>> {
        if let Some(sandbox_id) = sandbox_id_filter {
            let containers = self
                .shared
                .client()
                .list_containers_by_sandbox(sandbox_id)
                .await?;
            Ok(containers
                .into_iter()
                .map(|c| (c.id, ContainerRuntimeState::from_cri_state_i32(c.state)))
                .collect())
        } else {
            let response = self.shared.client().list_containers(None).await?;
            Ok(response
                .containers
                .into_iter()
                .map(|c| (c.id, ContainerRuntimeState::from_cri_state_i32(c.state)))
                .collect())
        }
    }

    async fn pod_metadata_for_container(
        &self,
        container_id: &str,
    ) -> anyhow::Result<Option<(String, String)>> {
        let filter = k8s_cri::v1::ContainerFilter {
            id: container_id.to_string(),
            state: None,
            pod_sandbox_id: String::new(),
            label_selector: std::collections::HashMap::new(),
        };
        let mut client = self.shared.client();
        let containers = crate::kubelet::cri::CriClient::list_containers(&mut client, Some(filter))
            .await?
            .containers;
        let Some(container) = containers.into_iter().next() else {
            return Ok(None);
        };
        let sandbox_filter = k8s_cri::v1::PodSandboxFilter {
            id: container.pod_sandbox_id,
            state: None,
            label_selector: std::collections::HashMap::new(),
        };
        let mut client = self.shared.client();
        let sandboxes =
            crate::kubelet::cri::CriClient::list_pod_sandboxes(&mut client, Some(sandbox_filter))
                .await?;
        let Some(sandbox) = sandboxes.into_iter().next() else {
            return Ok(None);
        };
        let Some(meta) = sandbox.metadata else {
            return Ok(None);
        };
        if meta.namespace.is_empty() || meta.name.is_empty() {
            return Ok(None);
        }
        Ok(Some((meta.namespace, meta.name)))
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl CriRuntime for crate::kubelet::cri::CriClient {
    async fn image_status(&self, image: &str) -> anyhow::Result<bool> {
        self.clone().image_status(image).await
    }

    async fn pull_image(&self, image: &str) -> anyhow::Result<String> {
        self.clone().pull_image(image).await
    }

    async fn run_pod_sandbox(&self, sandbox_config: PodSandboxConfig) -> anyhow::Result<String> {
        self.clone().run_pod_sandbox(sandbox_config).await
    }

    async fn stop_pod_sandbox(&self, sandbox_id: &str) -> anyhow::Result<()> {
        self.clone().stop_pod_sandbox(sandbox_id).await
    }

    async fn remove_pod_sandbox(&self, sandbox_id: &str) -> anyhow::Result<()> {
        self.clone().remove_pod_sandbox(sandbox_id).await
    }

    async fn list_pod_sandboxes(
        &self,
        pod_uid_filter: Option<&str>,
    ) -> anyhow::Result<Vec<(String, String)>> {
        let mut client = self.clone();
        let mut items =
            crate::kubelet::cri::CriClient::list_pod_sandboxes(&mut client, None).await?;
        if let Some(pod_uid) = pod_uid_filter.filter(|uid| !uid.trim().is_empty()) {
            items.retain(|sb| {
                sb.metadata
                    .as_ref()
                    .map(|metadata| metadata.uid == pod_uid)
                    .unwrap_or(false)
            });
        }
        Ok(items
            .into_iter()
            .map(|sb| (sb.id, format!("{}", sb.state)))
            .collect())
    }

    async fn create_container(
        &self,
        container_config: ContainerConfig,
        sandbox_id: &str,
        sandbox_config: PodSandboxConfig,
    ) -> anyhow::Result<String> {
        let mut client = self.clone();
        crate::kubelet::cri::CriClient::create_container(
            &mut client,
            sandbox_id,
            container_config,
            sandbox_config,
        )
        .await
    }

    async fn start_container(&self, container_id: &str) -> anyhow::Result<()> {
        self.clone().start_container(container_id).await
    }

    async fn stop_container(&self, container_id: &str, timeout_seconds: i64) -> anyhow::Result<()> {
        self.clone()
            .stop_container(container_id, timeout_seconds)
            .await
    }

    async fn remove_container(&self, container_id: &str) -> anyhow::Result<()> {
        self.clone().remove_container(container_id).await
    }

    async fn container_status(
        &self,
        container_id: &str,
    ) -> anyhow::Result<ContainerStatusResponse> {
        self.clone().container_status(container_id).await
    }

    async fn exec_sync(
        &self,
        container_id: &str,
        command: &[String],
        timeout_seconds: i64,
    ) -> anyhow::Result<ExecSyncResponse> {
        self.clone()
            .exec_sync(container_id, command, timeout_seconds)
            .await
    }

    async fn subscribe_container_events(
        &self,
    ) -> anyhow::Result<Box<dyn CriRuntimeContainerEventStream>> {
        let stream = self.clone().subscribe_container_events().await?;
        Ok(Box::new(SharedCriRuntimeEventStream { inner: stream }))
    }
}
