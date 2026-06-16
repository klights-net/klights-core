use crate::kubelet::cri_events::{CriContainerEventCodec, CriContainerEventResponse};
use crate::replication::grpc::transport_policy::{ChannelKind, GrpcTransportPolicy};
use anyhow::{Context, Result};
use k8s_cri::v1::{
    AttachRequest, AttachResponse, ContainerConfig, ContainerFilter, ContainerStatusRequest,
    ContainerStatusResponse, CreateContainerRequest, ExecRequest, ExecResponse, ExecSyncRequest,
    ExecSyncResponse, GetEventsRequest, ImageSpec, ImageStatusRequest, ListContainersRequest,
    ListContainersResponse, ListPodSandboxRequest, PodSandboxConfig, PullImageRequest,
    RemoveContainerRequest, RemovePodSandboxRequest, RunPodSandboxRequest, StartContainerRequest,
    StopContainerRequest, StopPodSandboxRequest, image_service_client::ImageServiceClient,
    runtime_service_client::RuntimeServiceClient,
};
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

// bug-grpc A1: CRI no longer defines its own message-size constant; it
// inherits `max_message_bytes` (and the dial tunables) from the shared
// `GrpcTransportPolicy`, so the kubelet→containerd channel cannot drift from
// the worker→leader channels.
// CRI PullImage is a unary RPC that only returns once the whole image is
// pulled, so the request timeout is effectively a TOTAL pull deadline. The
// default stays conservative; environments with slow links or large
// on-demand pulls raise it via KLIGHTS_IMAGE_PULL_RESPONSE_TIMEOUT_SECS (the
// multinode netns harness does this) and/or preload the image.
const DEFAULT_IMAGE_PULL_RESPONSE_TIMEOUT_SECS: u64 = 30;
const KLIGHTS_IMAGE_PULL_RESPONSE_TIMEOUT_SECS: &str = "KLIGHTS_IMAGE_PULL_RESPONSE_TIMEOUT_SECS";

fn image_pull_response_timeout() -> std::time::Duration {
    std::env::var(KLIGHTS_IMAGE_PULL_RESPONSE_TIMEOUT_SECS)
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .map(std::time::Duration::from_secs)
        .unwrap_or_else(|| std::time::Duration::from_secs(DEFAULT_IMAGE_PULL_RESPONSE_TIMEOUT_SECS))
}

#[derive(Clone)]
pub struct CriClient {
    runtime: RuntimeServiceClient<Channel>,
    image: ImageServiceClient<Channel>,
    channel: Channel,
    /// bug-grpc A1: message-size limit from the injected policy, retained so
    /// per-call client builders (e.g. `subscribe_container_events`) reuse it.
    max_message_bytes: usize,
}

/// Cloneable CRI handle for pod lifecycle work.
///
/// Tonic clients and channels are cheap clone handles over the same transport,
/// so each pod operation takes its own client clone instead of waiting behind a
/// global lock.
#[derive(Clone)]
pub struct SharedCriClient {
    inner: std::sync::Arc<CriClient>,
}

impl SharedCriClient {
    pub fn new(client: CriClient) -> Self {
        Self {
            inner: std::sync::Arc::new(client),
        }
    }

    pub fn client(&self) -> CriClient {
        self.inner.as_ref().clone()
    }
}

impl CriClient {
    /// Test helper that connects using the default transport policy.
    #[cfg(test)]
    pub async fn connect(socket_path: &str, namespace: &str) -> Result<Self> {
        Self::connect_with_policy(socket_path, namespace, &GrpcTransportPolicy::default()).await
    }

    /// bug-grpc A1: connect to the containerd CRI Unix socket using the
    /// injected [`GrpcTransportPolicy`]. The Unix-socket connector is
    /// CRI-specific, but the message-size limits and dial tunables come from
    /// the same policy object that the worker→leader and raft channels use.
    pub async fn connect_with_policy(
        socket_path: &str,
        _namespace: &str,
        policy: &GrpcTransportPolicy,
    ) -> Result<Self> {
        // Connect to containerd Unix socket
        let socket_path = socket_path.to_string();
        let channel = policy
            .configure_endpoint(
                Endpoint::try_from("http://[::]:50051")?,
                ChannelKind::ContainerdUds,
            )
            .connect_with_connector(service_fn(move |_: Uri| {
                let path = socket_path.clone();
                async move {
                    let stream = tokio::net::UnixStream::connect(path).await?;
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
                }
            }))
            .await?;

        let max_message_bytes = policy.max_message_bytes;
        let runtime = RuntimeServiceClient::new(channel.clone())
            .max_decoding_message_size(max_message_bytes)
            .max_encoding_message_size(max_message_bytes);
        let image = ImageServiceClient::new(channel.clone())
            .max_decoding_message_size(max_message_bytes)
            .max_encoding_message_size(max_message_bytes);

        Ok(Self {
            runtime,
            image,
            channel,
            max_message_bytes,
        })
    }

    /// Returns true if the named image is already present in the local CRI image store.
    /// Honors `imagePullPolicy: IfNotPresent` — caller skips `pull_image` when this is true.
    pub async fn image_status(&mut self, image: &str) -> Result<bool> {
        let request = tonic::Request::new(ImageStatusRequest {
            image: Some(ImageSpec {
                image: image.to_string(),
                ..Default::default()
            }),
            verbose: false,
        });
        let response = self
            .image
            .image_status(request)
            .await
            .with_context(|| format!("CRI image_status failed for {}", image))?;
        Ok(response.into_inner().image.is_some())
    }

    pub async fn pull_image(&mut self, image: &str) -> Result<String> {
        let timeout = image_pull_response_timeout();
        let mut request = tonic::Request::new(PullImageRequest {
            image: Some(ImageSpec {
                image: image.to_string(),
                ..Default::default()
            }),
            ..Default::default()
        });
        request.set_timeout(timeout);

        let response = match self.image.pull_image(request).await {
            Ok(response) => response,
            Err(status) if status.code() == tonic::Code::DeadlineExceeded => {
                anyhow::bail!(
                    "pulling image {image} timed out after {}s without CRI response",
                    timeout.as_secs()
                );
            }
            Err(status) => {
                return Err(anyhow::Error::new(status)
                    .context(format!("CRI pull_image failed for {}", image)));
            }
        };
        Ok(response.into_inner().image_ref)
    }

    pub async fn run_pod_sandbox(&mut self, config: PodSandboxConfig) -> Result<String> {
        let request = tonic::Request::new(RunPodSandboxRequest {
            config: Some(config),
            runtime_handler: String::new(),
        });

        let response = self.runtime.run_pod_sandbox(request).await?;
        Ok(response.into_inner().pod_sandbox_id)
    }

    pub async fn stop_pod_sandbox(&mut self, sandbox_id: &str) -> Result<()> {
        let request = tonic::Request::new(StopPodSandboxRequest {
            pod_sandbox_id: sandbox_id.to_string(),
        });

        self.runtime.stop_pod_sandbox(request).await?;
        Ok(())
    }

    pub async fn remove_pod_sandbox(&mut self, sandbox_id: &str) -> Result<()> {
        let request = tonic::Request::new(RemovePodSandboxRequest {
            pod_sandbox_id: sandbox_id.to_string(),
        });

        self.runtime.remove_pod_sandbox(request).await?;
        Ok(())
    }

    /// List pod sandboxes, optionally filtered. Returns sandbox metadata including IDs.
    pub async fn list_pod_sandboxes(
        &mut self,
        filter: Option<k8s_cri::v1::PodSandboxFilter>,
    ) -> Result<Vec<k8s_cri::v1::PodSandbox>> {
        let request = tonic::Request::new(ListPodSandboxRequest { filter });

        let response = self.runtime.list_pod_sandbox(request).await?;
        Ok(response.into_inner().items)
    }

    pub async fn create_container(
        &mut self,
        sandbox_id: &str,
        config: ContainerConfig,
        sandbox_config: PodSandboxConfig,
    ) -> Result<String> {
        let request = tonic::Request::new(CreateContainerRequest {
            pod_sandbox_id: sandbox_id.to_string(),
            config: Some(config),
            sandbox_config: Some(sandbox_config),
        });

        let response = self.runtime.create_container(request).await?;
        Ok(response.into_inner().container_id)
    }

    pub async fn start_container(&mut self, container_id: &str) -> Result<()> {
        let request = tonic::Request::new(StartContainerRequest {
            container_id: container_id.to_string(),
        });

        self.runtime.start_container(request).await?;
        Ok(())
    }

    pub async fn stop_container(&mut self, container_id: &str, timeout: i64) -> Result<()> {
        let request = tonic::Request::new(StopContainerRequest {
            container_id: container_id.to_string(),
            timeout,
        });

        self.runtime.stop_container(request).await?;
        Ok(())
    }

    pub async fn remove_container(&mut self, container_id: &str) -> Result<()> {
        let request = tonic::Request::new(RemoveContainerRequest {
            container_id: container_id.to_string(),
        });

        self.runtime.remove_container(request).await?;
        Ok(())
    }

    pub async fn container_status(
        &mut self,
        container_id: &str,
    ) -> Result<ContainerStatusResponse> {
        self.container_status_verbose(container_id, false).await
    }

    pub async fn container_status_verbose(
        &mut self,
        container_id: &str,
        verbose: bool,
    ) -> Result<ContainerStatusResponse> {
        let request = tonic::Request::new(ContainerStatusRequest {
            container_id: container_id.to_string(),
            verbose,
        });

        let response = self.runtime.container_status(request).await?;
        Ok(response.into_inner())
    }

    pub async fn list_containers(
        &mut self,
        filter: Option<ContainerFilter>,
    ) -> Result<ListContainersResponse> {
        let request = tonic::Request::new(ListContainersRequest { filter });

        let response = self.runtime.list_containers(request).await?;
        Ok(response.into_inner())
    }

    pub async fn list_containers_by_sandbox(
        &mut self,
        sandbox_id: &str,
    ) -> Result<Vec<k8s_cri::v1::Container>> {
        let filter = Some(ContainerFilter {
            id: String::new(),
            state: None,
            pod_sandbox_id: sandbox_id.to_string(),
            label_selector: std::collections::HashMap::new(),
        });

        let response = self.list_containers(filter).await?;
        // Defensive filtering: some CRI implementations may ignore pod_sandbox_id
        // in ListContainers filters under load. Always enforce sandbox match client-side.
        Ok(response
            .containers
            .into_iter()
            .filter(|c| c.pod_sandbox_id == sandbox_id)
            .collect())
    }

    pub async fn exec_sync(
        &mut self,
        container_id: &str,
        cmd: &[String],
        timeout: i64,
    ) -> Result<ExecSyncResponse> {
        let request = tonic::Request::new(ExecSyncRequest {
            container_id: container_id.to_string(),
            cmd: cmd.to_vec(),
            timeout,
        });

        let response = self.runtime.exec_sync(request).await?;
        Ok(response.into_inner())
    }

    /// Subscribe to container lifecycle events from containerd.
    /// Returns a streaming response of ContainerEventResponse (created/started/stopped/deleted).
    /// Uses a cloned RuntimeServiceClient so the caller retains use of the original CriClient.
    pub async fn subscribe_container_events(
        &self,
    ) -> Result<tonic::codec::Streaming<CriContainerEventResponse>> {
        let mut grpc = tonic::client::Grpc::new(self.channel.clone())
            .max_decoding_message_size(self.max_message_bytes)
            .max_encoding_message_size(self.max_message_bytes);
        let request = tonic::Request::new(GetEventsRequest {});
        let request = request;
        let path = tonic::codegen::http::uri::PathAndQuery::from_static(
            "/runtime.v1.RuntimeService/GetContainerEvents",
        );
        let codec = CriContainerEventCodec;
        grpc.ready()
            .await
            .map_err(|e| anyhow::anyhow!("CRI runtime service was not ready: {}", e))?;
        let response = grpc
            .server_streaming(request, path, codec)
            .await
            .context("CRI get_container_events failed")?;
        Ok(response.into_inner())
    }

    pub async fn exec(
        &mut self,
        container_id: &str,
        cmd: &[String],
        tty: bool,
        stdin: bool,
        stdout: bool,
        stderr: bool,
    ) -> Result<ExecResponse> {
        let request = tonic::Request::new(ExecRequest {
            container_id: container_id.to_string(),
            cmd: cmd.to_vec(),
            tty,
            stdin,
            stdout,
            stderr,
        });

        let response = self.runtime.exec(request).await?;
        Ok(response.into_inner())
    }

    pub async fn attach(
        &mut self,
        container_id: &str,
        tty: bool,
        stdin: bool,
        stdout: bool,
        stderr: bool,
    ) -> Result<AttachResponse> {
        let request = tonic::Request::new(AttachRequest {
            container_id: container_id.to_string(),
            tty,
            stdin,
            stdout,
            stderr,
        });

        let response = self.runtime.attach(request).await?;
        Ok(response.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn image_pull_timeout_is_env_overridable_for_slow_links() {
        // The default stays conservative (pinned by check_kubelet_invariants),
        // but slow-link environments raise it via the env override. CRI
        // PullImage is unary, so this timeout is effectively a TOTAL pull
        // deadline — a ~94 MB etcd image over the harness link takes ~36s.
        let _lock = crate::TEST_ENV_LOCK.lock().unwrap();
        // SAFETY: env mutation serialized by TEST_ENV_LOCK for the test body.
        unsafe { std::env::remove_var(KLIGHTS_IMAGE_PULL_RESPONSE_TIMEOUT_SECS) };
        assert_eq!(
            image_pull_response_timeout(),
            std::time::Duration::from_secs(DEFAULT_IMAGE_PULL_RESPONSE_TIMEOUT_SECS)
        );
        unsafe { std::env::set_var(KLIGHTS_IMAGE_PULL_RESPONSE_TIMEOUT_SECS, "600") };
        assert_eq!(
            image_pull_response_timeout(),
            std::time::Duration::from_secs(600)
        );
        unsafe { std::env::remove_var(KLIGHTS_IMAGE_PULL_RESPONSE_TIMEOUT_SECS) };
    }

    #[tokio::test]
    #[ignore] // Only run with: cargo test -- --ignored
    async fn test_cri_connect() {
        let sock = crate::paths::test_data_root_path("klights")
            .join("containerd.sock")
            .to_string_lossy()
            .into_owned();
        let mut client = CriClient::connect(&sock, "klights-test")
            .await
            .expect("Failed to connect to containerd");

        let response = client
            .list_containers(None)
            .await
            .expect("Failed to list containers");

        // Should return a list (may be empty if no klights containers exist)
        tracing::info!("Found {} containers", response.containers.len());
    }

    #[tokio::test]
    #[ignore] // Only run with real containerd: cargo test -- --ignored
    async fn test_cri_subscribe_container_events() {
        // Verify that subscribe_container_events returns a valid stream.
        // The stream blocks until a container event occurs, so we just verify
        // the subscription succeeds (stream is established).
        let sock = crate::paths::test_data_root_path("klights")
            .join("containerd.sock")
            .to_string_lossy()
            .into_owned();
        let client = CriClient::connect(&sock, "klights-test")
            .await
            .expect("Failed to connect to containerd");

        let stream = client
            .subscribe_container_events()
            .await
            .expect("Failed to subscribe to container events");

        // Stream is established — drop it (we don't wait for events in this test)
        drop(stream);
    }

    #[tokio::test]
    #[ignore] // Only run with real containerd and KLIGHTS_RUN_CRI_MUTATING_SMOKE=1.
    async fn test_cri_mutating_runtime_methods_smoke() {
        if std::env::var_os("KLIGHTS_RUN_CRI_MUTATING_SMOKE").is_none() {
            return;
        }

        let sock = crate::paths::test_data_root_path("klights")
            .join("containerd.sock")
            .to_string_lossy()
            .into_owned();
        let mut client = CriClient::connect(&sock, "klights-test")
            .await
            .expect("Failed to connect to containerd");

        let _ = client.image_status("busybox:latest").await;
        let _ = client.pull_image("busybox:latest").await;
        let sandbox_id = client
            .run_pod_sandbox(PodSandboxConfig::default())
            .await
            .expect("CRI RunPodSandbox failed");
        let _ = client.container_status("missing-container-id").await;
        let _ = client
            .container_status_verbose("missing-container-id", true)
            .await;
        let _ = client.start_container("missing-container-id").await;
        let _ = client.stop_container("missing-container-id", 0).await;
        let _ = client.remove_container("missing-container-id").await;
        let _ = client.stop_pod_sandbox(&sandbox_id).await;
        let _ = client.remove_pod_sandbox(&sandbox_id).await;
    }

    #[tokio::test]
    #[ignore] // Only run with real containerd: cargo test -- --ignored
    async fn test_cri_exec_returns_streaming_url() {
        // This test verifies that CRI Exec() returns an ExecResponse with a streaming URL
        // The URL format should be: http://localhost:PORT/exec/TOKEN

        let sock = crate::paths::test_data_root_path("klights")
            .join("containerd.sock")
            .to_string_lossy()
            .into_owned();
        let mut client = CriClient::connect(&sock, "klights-test")
            .await
            .expect("Failed to connect to containerd");

        // First, we need a running container. List containers and pick the first one.
        let list_response = client
            .list_containers(None)
            .await
            .expect("Failed to list containers");

        if list_response.containers.is_empty() {
            eprintln!("No containers found for test. Skipping.");
            return;
        }

        let container_id = &list_response.containers[0].id;
        let command = vec!["echo".to_string(), "hello".to_string()];

        // Call CRI Exec
        let exec_response = client
            .exec(container_id, &command, false, false, true, true)
            .await
            .expect("CRI Exec failed");

        // Verify response has a URL field
        assert!(
            !exec_response.url.is_empty(),
            "ExecResponse.url should not be empty"
        );
        assert!(
            exec_response.url.starts_with("http://"),
            "Streaming URL should start with http://, got: {}",
            exec_response.url
        );

        tracing::info!("CRI Exec streaming URL: {}", exec_response.url);
    }
}
