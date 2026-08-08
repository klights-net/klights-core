use crate::cri_events::{CriContainerEventCodec, CriContainerEventResponse};
use anyhow::{Context, Result};
use k8s_cri::v1::{
    AttachRequest, AttachResponse, ContainerConfig, ContainerFilter, ContainerStatusRequest,
    ContainerStatusResponse, CreateContainerRequest, ExecRequest, ExecResponse, ExecSyncRequest,
    ExecSyncResponse, GetEventsRequest, ImageSpec, ImageStatusRequest, ListContainersRequest,
    ListContainersResponse, ListPodSandboxRequest, ListPodSandboxStatsRequest, PodSandboxConfig,
    PodSandboxStats, PodSandboxStatsFilter, PullImageRequest, RemoveContainerRequest,
    RemovePodSandboxRequest, RunPodSandboxRequest, StartContainerRequest, StopContainerRequest,
    StopPodSandboxRequest, image_service_client::ImageServiceClient,
    runtime_service_client::RuntimeServiceClient,
};
use klights_node_api::CriTransportPolicy;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

// bug-grpc A1: CRI no longer defines its own message-size constant; it
// inherits `max_message_bytes` (and the dial tunables) from the shared
// `GrpcTransportPolicy`, so the kubelet→containerd channel cannot drift from
// the worker→leader channels.
// CRI PullImage is a unary RPC that only returns once the whole image is
// pulled, so the request timeout is effectively a TOTAL pull deadline. The
// default stays conservative; root construction may inject a larger deadline
// for slow links or large on-demand pulls.
pub(crate) const DEFAULT_IMAGE_PULL_RESPONSE_TIMEOUT_SECS: u64 = 30;
pub const DEFAULT_IMAGE_PULL_RESPONSE_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(DEFAULT_IMAGE_PULL_RESPONSE_TIMEOUT_SECS);
pub const DEFAULT_CRI_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CriRequestTimeout {
    operation: &'static str,
    timeout: std::time::Duration,
}

impl CriRequestTimeout {
    pub fn new(operation: &'static str, timeout: std::time::Duration) -> Self {
        Self { operation, timeout }
    }

    pub fn operation(&self) -> &'static str {
        self.operation
    }

    pub fn timeout(&self) -> std::time::Duration {
        self.timeout
    }
}

impl std::fmt::Display for CriRequestTimeout {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "CRI {} request exceeded its {:?} transport deadline",
            self.operation, self.timeout
        )
    }
}

impl std::error::Error for CriRequestTimeout {}

async fn await_unary_response<T, F>(
    supervisor: &klights_supervisor::TaskSupervisor,
    request_timeout: std::time::Duration,
    operation: &'static str,
    future: F,
) -> Result<tonic::Response<T>>
where
    F: std::future::Future<Output = std::result::Result<tonic::Response<T>, tonic::Status>>,
{
    let result = supervisor
        .timeout(
            format!("cri_unary_{}", operation.to_ascii_lowercase()),
            request_timeout,
            future,
        )
        .await
        .with_context(|| format!("supervise CRI {operation} request"))?;
    match result {
        Err(_) => Err(anyhow::Error::new(CriRequestTimeout::new(
            operation,
            request_timeout,
        ))),
        Ok(Err(status)) if status.code() == tonic::Code::DeadlineExceeded => Err(
            anyhow::Error::new(CriRequestTimeout::new(operation, request_timeout))
                .context(status.to_string()),
        ),
        Ok(Err(status)) => Err(anyhow::Error::new(status)),
        Ok(Ok(response)) => Ok(response),
    }
}

fn is_not_found(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<tonic::Status>()
        .is_some_and(|status| status.code() == tonic::Code::NotFound)
}

fn request_with_timeout<T>(message: T, timeout: std::time::Duration) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    request.set_timeout(timeout);
    request
}

#[derive(Clone)]
pub struct CriClient {
    runtime: RuntimeServiceClient<Channel>,
    image: ImageServiceClient<Channel>,
    channel: Channel,
    /// bug-grpc A1: message-size limit from the injected policy, retained so
    /// per-call client builders (e.g. `subscribe_container_events`) reuse it.
    max_message_bytes: usize,
    image_pull_response_timeout: std::time::Duration,
    request_timeout: std::time::Duration,
    supervisor: klights_supervisor::TaskSupervisor,
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
    #[cfg(any(test, feature = "test-support"))]
    pub async fn connect(
        socket_path: &str,
        namespace: &str,
        supervisor: klights_supervisor::TaskSupervisor,
    ) -> Result<Self> {
        Self::connect_with_policy(
            socket_path,
            namespace,
            &CriTransportPolicy::new(std::time::Duration::from_secs(10), 32 * 1024 * 1024),
            DEFAULT_IMAGE_PULL_RESPONSE_TIMEOUT,
            DEFAULT_CRI_REQUEST_TIMEOUT,
            supervisor,
        )
        .await
    }

    /// bug-grpc A1: connect to the containerd CRI Unix socket using the
    /// injected [`CriTransportPolicy`].
    pub async fn connect_with_policy(
        socket_path: &str,
        _namespace: &str,
        policy: &CriTransportPolicy,
        image_pull_response_timeout: std::time::Duration,
        request_timeout: std::time::Duration,
        supervisor: klights_supervisor::TaskSupervisor,
    ) -> Result<Self> {
        // Connect to containerd Unix socket
        let socket_path = socket_path.to_string();
        let channel = Endpoint::try_from("http://[::]:50051")?
            .connect_timeout(policy.connect_timeout())
            .connect_with_connector(service_fn(move |_: Uri| {
                let path = socket_path.clone();
                async move {
                    let stream = tokio::net::UnixStream::connect(path).await?;
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
                }
            }))
            .await?;

        let max_message_bytes = policy.max_message_bytes();
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
            image_pull_response_timeout,
            request_timeout,
            supervisor,
        })
    }

    fn request<T>(&self, message: T) -> tonic::Request<T> {
        request_with_timeout(message, self.request_timeout)
    }

    /// Returns true if the named image is already present in the local CRI image store.
    /// Honors `imagePullPolicy: IfNotPresent` — caller skips `pull_image` when this is true.
    pub async fn image_status(&mut self, image: &str) -> Result<bool> {
        let request = self.request(ImageStatusRequest {
            image: Some(ImageSpec {
                image: image.to_string(),
                ..Default::default()
            }),
            verbose: false,
        });
        let response = await_unary_response(
            &self.supervisor,
            self.request_timeout,
            "ImageStatus",
            self.image.image_status(request),
        )
        .await
        .with_context(|| format!("CRI image_status failed for {}", image))?;
        Ok(response.into_inner().image.is_some())
    }

    pub async fn pull_image(&mut self, image: &str) -> Result<String> {
        let timeout = self.image_pull_response_timeout;
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
        let request = self.request(RunPodSandboxRequest {
            config: Some(config),
            runtime_handler: String::new(),
        });

        let response = await_unary_response(
            &self.supervisor,
            self.request_timeout,
            "RunPodSandbox",
            self.runtime.run_pod_sandbox(request),
        )
        .await?;
        Ok(response.into_inner().pod_sandbox_id)
    }

    pub async fn stop_pod_sandbox(&mut self, sandbox_id: &str) -> Result<()> {
        let request = self.request(StopPodSandboxRequest {
            pod_sandbox_id: sandbox_id.to_string(),
        });

        match await_unary_response(
            &self.supervisor,
            self.request_timeout,
            "StopPodSandbox",
            self.runtime.stop_pod_sandbox(request),
        )
        .await
        {
            Ok(_) => Ok(()),
            Err(error) if is_not_found(&error) => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub async fn remove_pod_sandbox(&mut self, sandbox_id: &str) -> Result<()> {
        let request = self.request(RemovePodSandboxRequest {
            pod_sandbox_id: sandbox_id.to_string(),
        });

        match await_unary_response(
            &self.supervisor,
            self.request_timeout,
            "RemovePodSandbox",
            self.runtime.remove_pod_sandbox(request),
        )
        .await
        {
            Ok(_) => Ok(()),
            Err(error) if is_not_found(&error) => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// List pod sandboxes, optionally filtered. Returns sandbox metadata including IDs.
    pub async fn list_pod_sandboxes(
        &mut self,
        filter: Option<k8s_cri::v1::PodSandboxFilter>,
    ) -> Result<Vec<k8s_cri::v1::PodSandbox>> {
        let request = self.request(ListPodSandboxRequest { filter });

        let response = await_unary_response(
            &self.supervisor,
            self.request_timeout,
            "ListPodSandbox",
            self.runtime.list_pod_sandbox(request),
        )
        .await?;
        Ok(response.into_inner().items)
    }

    /// List pod sandbox stats from the local CRI endpoint.
    ///
    /// This is intentionally a unary on-demand call. Metrics API handlers use
    /// it only while serving a metrics.k8s.io request; no sampler or cache
    /// warmer is started around it.
    pub async fn list_pod_sandbox_stats(
        &mut self,
        filter: Option<PodSandboxStatsFilter>,
    ) -> Result<Vec<PodSandboxStats>> {
        let request = self.request(ListPodSandboxStatsRequest { filter });

        let response = await_unary_response(
            &self.supervisor,
            self.request_timeout,
            "ListPodSandboxStats",
            self.runtime.list_pod_sandbox_stats(request),
        )
        .await?;
        Ok(response.into_inner().stats)
    }

    pub async fn create_container(
        &mut self,
        sandbox_id: &str,
        config: ContainerConfig,
        sandbox_config: PodSandboxConfig,
    ) -> Result<String> {
        let request = self.request(CreateContainerRequest {
            pod_sandbox_id: sandbox_id.to_string(),
            config: Some(config),
            sandbox_config: Some(sandbox_config),
        });

        let response = await_unary_response(
            &self.supervisor,
            self.request_timeout,
            "CreateContainer",
            self.runtime.create_container(request),
        )
        .await?;
        Ok(response.into_inner().container_id)
    }

    pub async fn start_container(&mut self, container_id: &str) -> Result<()> {
        let request = self.request(StartContainerRequest {
            container_id: container_id.to_string(),
        });

        await_unary_response(
            &self.supervisor,
            self.request_timeout,
            "StartContainer",
            self.runtime.start_container(request),
        )
        .await?;
        Ok(())
    }

    pub async fn stop_container(&mut self, container_id: &str, timeout: i64) -> Result<()> {
        let request = self.request(StopContainerRequest {
            container_id: container_id.to_string(),
            timeout,
        });

        match await_unary_response(
            &self.supervisor,
            self.request_timeout,
            "StopContainer",
            self.runtime.stop_container(request),
        )
        .await
        {
            Ok(_) => Ok(()),
            Err(error) if is_not_found(&error) => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub async fn remove_container(&mut self, container_id: &str) -> Result<()> {
        let request = self.request(RemoveContainerRequest {
            container_id: container_id.to_string(),
        });

        match await_unary_response(
            &self.supervisor,
            self.request_timeout,
            "RemoveContainer",
            self.runtime.remove_container(request),
        )
        .await
        {
            Ok(_) => Ok(()),
            Err(error) if is_not_found(&error) => Ok(()),
            Err(error) => Err(error),
        }
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
        let request = self.request(ContainerStatusRequest {
            container_id: container_id.to_string(),
            verbose,
        });

        let response = await_unary_response(
            &self.supervisor,
            self.request_timeout,
            "ContainerStatus",
            self.runtime.container_status(request),
        )
        .await?;
        Ok(response.into_inner())
    }

    pub async fn list_containers(
        &mut self,
        filter: Option<ContainerFilter>,
    ) -> Result<ListContainersResponse> {
        let request = self.request(ListContainersRequest { filter });

        let response = await_unary_response(
            &self.supervisor,
            self.request_timeout,
            "ListContainers",
            self.runtime.list_containers(request),
        )
        .await?;
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
        let request = self.request(ExecSyncRequest {
            container_id: container_id.to_string(),
            cmd: cmd.to_vec(),
            timeout,
        });

        let response = await_unary_response(
            &self.supervisor,
            self.request_timeout,
            "ExecSync",
            self.runtime.exec_sync(request),
        )
        .await?;
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
        let request = self.request(GetEventsRequest {});
        let path = tonic::codegen::http::uri::PathAndQuery::from_static(
            "/runtime.v1.RuntimeService/GetContainerEvents",
        );
        let codec = CriContainerEventCodec;
        let establish_stream = async move {
            grpc.ready().await.map_err(|error| {
                tonic::Status::unavailable(format!("CRI runtime service was not ready: {error}"))
            })?;
            grpc.server_streaming(request, path, codec).await
        };
        let response = await_unary_response(
            &self.supervisor,
            self.request_timeout,
            "GetContainerEvents",
            establish_stream,
        )
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
        let request = self.request(ExecRequest {
            container_id: container_id.to_string(),
            cmd: cmd.to_vec(),
            tty,
            stdin,
            stdout,
            stderr,
        });

        let response = await_unary_response(
            &self.supervisor,
            self.request_timeout,
            "Exec",
            self.runtime.exec(request),
        )
        .await?;
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
        let request = self.request(AttachRequest {
            container_id: container_id.to_string(),
            tty,
            stdin,
            stdout,
            stderr,
        });

        let response = await_unary_response(
            &self.supervisor,
            self.request_timeout,
            "Attach",
            self.runtime.attach(request),
        )
        .await?;
        Ok(response.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_supervisor() -> klights_supervisor::TaskSupervisor {
        klights_supervisor::TaskSupervisor::new(klights_supervisor::TaskCategoryConfig::default())
    }

    struct PendingUnary {
        dropped: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }

    impl std::future::Future for PendingUnary {
        type Output = std::result::Result<tonic::Response<()>, tonic::Status>;

        fn poll(
            self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            std::task::Poll::Pending
        }
    }

    impl Drop for PendingUnary {
        fn drop(&mut self) {
            self.dropped
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[test]
    fn image_pull_timeout_default_remains_conservative() {
        assert_eq!(
            DEFAULT_IMAGE_PULL_RESPONSE_TIMEOUT,
            std::time::Duration::from_secs(30)
        );
    }

    #[test]
    fn unary_request_timeout_is_typed_and_independent_from_pod_grace() {
        assert_eq!(
            DEFAULT_CRI_REQUEST_TIMEOUT,
            std::time::Duration::from_secs(120)
        );
        let error = CriRequestTimeout::new("StopContainer", std::time::Duration::from_secs(2));
        assert_eq!(error.operation(), "StopContainer");
        assert_eq!(error.timeout(), std::time::Duration::from_secs(2));
    }

    #[test]
    fn unary_request_carries_grpc_timeout_metadata() {
        let request = request_with_timeout((), std::time::Duration::from_secs(7));
        assert_eq!(
            request
                .metadata()
                .get("grpc-timeout")
                .and_then(|value| value.to_str().ok()),
            Some("7000000u")
        );
    }

    #[tokio::test]
    async fn supervisor_deadline_drops_hung_unary_and_returns_typed_error() {
        let supervisor = klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        );
        let dropped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let error = await_unary_response(
            &supervisor,
            std::time::Duration::from_millis(1),
            "StopContainer",
            PendingUnary {
                dropped: dropped.clone(),
            },
        )
        .await
        .expect_err("hung unary must reach the supervisor deadline");
        let typed = error
            .downcast_ref::<CriRequestTimeout>()
            .expect("deadline must retain its typed classification");
        assert_eq!(typed.operation(), "StopContainer");
        assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn tonic_deadline_is_normalized_to_typed_timeout() {
        let supervisor = klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        );
        let error = await_unary_response(
            &supervisor,
            std::time::Duration::from_secs(3),
            "ListContainers",
            std::future::ready(Err::<tonic::Response<()>, _>(
                tonic::Status::deadline_exceeded("server deadline"),
            )),
        )
        .await
        .expect_err("tonic deadline must be normalized");
        let typed = error
            .downcast_ref::<CriRequestTimeout>()
            .expect("normalized deadline must be downcastable");
        assert_eq!(typed.operation(), "ListContainers");
        assert_eq!(typed.timeout(), std::time::Duration::from_secs(3));
    }

    #[test]
    fn not_found_is_recognized_only_for_idempotent_operation_mapping() {
        assert!(is_not_found(&anyhow::Error::new(tonic::Status::not_found(
            "already absent"
        ))));
        assert!(!is_not_found(&anyhow::Error::new(
            tonic::Status::unavailable("runtime down")
        )));
    }

    #[tokio::test]
    #[ignore] // Only run with: cargo test -- --ignored
    async fn test_cri_connect() {
        let sock = crate::runtime_paths::KubeletRuntimePaths::for_test("klights")
            .data_root()
            .to_path_buf()
            .join("containerd.sock")
            .to_string_lossy()
            .into_owned();
        let mut client = CriClient::connect(&sock, "klights-test", test_supervisor())
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
        let sock = crate::runtime_paths::KubeletRuntimePaths::for_test("klights")
            .data_root()
            .to_path_buf()
            .join("containerd.sock")
            .to_string_lossy()
            .into_owned();
        let client = CriClient::connect(&sock, "klights-test", test_supervisor())
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

        let sock = crate::runtime_paths::KubeletRuntimePaths::for_test("klights")
            .data_root()
            .to_path_buf()
            .join("containerd.sock")
            .to_string_lossy()
            .into_owned();
        let mut client = CriClient::connect(&sock, "klights-test", test_supervisor())
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

        let sock = crate::runtime_paths::KubeletRuntimePaths::for_test("klights")
            .data_root()
            .to_path_buf()
            .join("containerd.sock")
            .to_string_lossy()
            .into_owned();
        let mut client = CriClient::connect(&sock, "klights-test", test_supervisor())
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
