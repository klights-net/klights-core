//! Cleanup helpers extracted from runtime.rs (R3 refactor).

use anyhow::Context;

use crate::bootstrap::{CliFlags, NodeMode};
use crate::{KlightsConfig, paths, pidfile, shutdown};
use klights_networking::cni_plugin;

/// Full teardown with the same immutable mode detection used by startup. This
/// keeps root/rootless cleanup dispatch centralized in `klights_networking::NetworkCleanup`.
pub async fn run_cleanup_with_flags(cli: CliFlags) -> anyhow::Result<()> {
    // Initialize tracing early
    let namespace = cli.namespace.as_deref().unwrap_or("klights");
    crate::bootstrap::logging::init_tracing_from_env(namespace);

    // Require root privileges for root-mode cleanup. Rootless cleanup is a
    // Phase-1 no-op for host bridge/veth state, but containerd/data cleanup
    // still expects the same privileges as before.
    // SAFETY: geteuid(2) is a thread-safe syscall with no preconditions and
    // returns the effective user id; it cannot fail or read invalid memory.
    if unsafe { libc::geteuid() } != 0 {
        anyhow::bail!("klights cleanup requires root privileges");
    }

    let config = std::sync::Arc::new(
        KlightsConfig::from_env_with_namespace_override(Some(namespace))
            .context("invalid klights configuration")?,
    );
    let cleanup_task_config = klights_supervisor::TaskCategoryConfig::from_env()
        .context("invalid task supervisor category limits")?;
    let cleanup_task_supervisor =
        std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(cleanup_task_config));
    let file_process =
        klights_supervisor::FileProcessExecutor::new(cleanup_task_supervisor.clone());
    let grpc_transport_policy =
        klights_leader_rpc::transport_policy::GrpcTransportPolicy::shared_default();
    let node_mode =
        NodeMode::detect(cli.rootless).context("failed to detect klights operating mode")?;
    let network_cleanup = crate::bootstrap::network_adapters::cleanup_config(&node_mode, &config)?
        .build_cleanup(file_process.clone());
    let cleanup_node_local =
        match open_cleanup_node_local(config.as_ref(), cleanup_task_supervisor.clone()).await {
            Ok(node_local) => Some(node_local),
            Err(e) => {
                tracing::warn!(
                    namespace = %namespace,
                    error = %e,
                    "Could not open node-local state for recorded network cleanup"
                );
                None
            }
        };
    let containerd_socket = paths::containerd_socket_path(namespace);
    let containerd_state_dir = paths::containerd_state_dir_path(namespace)
        .to_string_lossy()
        .into_owned();
    let runtime_paths =
        klights_kubelet::runtime_paths::KubeletRuntimePaths::new(paths::data_root_path(namespace))
            .context("invalid cleanup runtime path layout")?;
    let mut cleanup_cni_rpc = match start_cleanup_cni_rpc_server(
        namespace,
        &cleanup_task_supervisor,
        &file_process,
    )
    .await
    {
        Ok(server) => Some(server),
        Err(e) => {
            tracing::warn!(
                namespace = %namespace,
                error = %e,
                "Could not start cleanup CNI RPC server; continuing with fallback network cleanup"
            );
            None
        }
    };

    // Connect to containerd for sandbox teardown.
    let cri_transport_policy = klights_node_api::CriTransportPolicy::new(
        grpc_transport_policy.connect_timeout,
        grpc_transport_policy.max_message_bytes,
    );
    let mut cri = match klights_kubelet::cri::CriClient::connect_with_policy(
        containerd_socket.to_string_lossy().as_ref(),
        namespace,
        &cri_transport_policy,
        klights_kubelet::cri::DEFAULT_IMAGE_PULL_RESPONSE_TIMEOUT,
        klights_kubelet::cri::DEFAULT_CRI_REQUEST_TIMEOUT,
        cleanup_task_supervisor.as_ref().clone(),
    )
    .await
    {
        Ok(c) => {
            tracing::info!("Connected to containerd for cleanup");
            c
        }
        Err(e) => {
            tracing::warn!(
                "Could not connect to containerd (may already be stopped): {}",
                e
            );
            // Continue with directory cleanup even if containerd is down.
            stop_namespace_containerd_after_cleanup(
                namespace,
                &cleanup_task_supervisor,
                &file_process,
                &runtime_paths,
            )
            .await;
            if let Some(cleanup_cni_rpc) = cleanup_cni_rpc.take() {
                cleanup_cni_rpc.shutdown().await;
            }
            return cleanup_directories_and_network(
                &network_cleanup,
                cleanup_node_local.as_ref(),
                &containerd_state_dir,
                namespace,
                &cleanup_task_supervisor,
                &file_process,
            )
            .await;
        }
    };

    // Drain leftover CRI containers first (covers stale pause/infra
    // containers that can survive sandbox cleanup in rare crash windows).
    let runtime_cleanup = cri.cleanup_all_runtime_containers(5).await?;
    tracing::info!(
        to_stop = runtime_cleanup.stopped(),
        removed = runtime_cleanup.removed(),
        "Cleaned up lingering runtime containers during klights cleanup"
    );

    // Stop all pod sandboxes.
    tracing::info!("Stopping all pod sandboxes");
    let sandboxes = cri.list_pod_sandboxes(None).await?;
    for sb in &sandboxes {
        cri.stop_pod_sandbox(&sb.id).await?;
        cri.remove_pod_sandbox(&sb.id).await?;
        if let Some(uid) = sb
            .metadata
            .as_ref()
            .map(|meta| meta.uid.as_str())
            .filter(|uid| !uid.trim().is_empty())
            && let Err(e) =
                klights_kubelet::cgroup_cleanup::cleanup_pod_cgroup(&file_process, namespace, uid)
                    .await
        {
            tracing::warn!(
                sandbox_id = %sb.id,
                pod_uid = %uid,
                error = %e,
                "Failed to cleanup pod cgroup after sandbox removal"
            );
        }
    }
    tracing::info!("Stopped and removed {} sandboxes", sandboxes.len());

    stop_namespace_containerd_after_cleanup(
        namespace,
        &cleanup_task_supervisor,
        &file_process,
        &runtime_paths,
    )
    .await;
    if let Some(cleanup_cni_rpc) = cleanup_cni_rpc.take() {
        cleanup_cni_rpc.shutdown().await;
    }

    // Clean up networking and directories.
    cleanup_directories_and_network(
        &network_cleanup,
        cleanup_node_local.as_ref(),
        &containerd_state_dir,
        namespace,
        &cleanup_task_supervisor,
        &file_process,
    )
    .await
}

struct CleanupCniRpcServer {
    cancel: tokio_util::sync::CancellationToken,
    handle: klights_supervisor::SupervisedJoinHandle<()>,
}

impl CleanupCniRpcServer {
    async fn shutdown(self) {
        self.cancel.cancel();
        match self.handle.join().await {
            Ok(()) => {}
            Err(e) if e.is_cancelled() => {
                tracing::debug!("cleanup CNI RPC server task was cancelled");
            }
            Err(e) => {
                tracing::warn!("cleanup CNI RPC server task ended with error: {}", e);
            }
        }
    }
}

async fn start_cleanup_cni_rpc_server(
    namespace: &str,
    task_supervisor: &klights_supervisor::TaskSupervisor,
    file_process: &klights_supervisor::FileProcessExecutor,
) -> anyhow::Result<CleanupCniRpcServer> {
    let socket_path = cni_plugin::CniSocketPath::try_new(
        paths::cni_rpc_socket_path(namespace)
            .to_string_lossy()
            .into_owned(),
    )?;
    let socket_filesystem =
        crate::bootstrap::composition_adapters::cni_socket_adapter::RootCniSocketFilesystem::shared(
            file_process.clone(),
        );
    let server = cni_plugin::bind_cleanup_rpc_server(
        socket_path,
        socket_filesystem,
        task_supervisor.clone(),
    )
    .await?;
    let socket_path = server.socket_path().to_string();
    let cancel = tokio_util::sync::CancellationToken::new();
    let task_cancel = cancel.clone();
    let handle = match task_supervisor
        .spawn_async(
            klights_supervisor::TaskCategory::Background,
            "cleanup_cni_rpc_server",
            async move {
                if let Err(e) = server.serve(task_cancel).await {
                    tracing::warn!("cleanup CNI RPC server failed: {:#}", e);
                }
            },
        )
        .await
    {
        Ok(handle) => handle,
        Err(e) => {
            let _ = klights_supervisor::runtime_fs::remove_file_if_exists_async(
                file_process,
                &socket_path,
            )
            .await;
            return Err(e.into());
        }
    };
    Ok(CleanupCniRpcServer { cancel, handle })
}

pub async fn stop_namespace_containerd_after_cleanup(
    namespace: &str,
    task_supervisor: &klights_supervisor::TaskSupervisor,
    file_process: &klights_supervisor::FileProcessExecutor,
    paths: &klights_kubelet::runtime_paths::KubeletRuntimePaths,
) {
    match klights_kubelet::containerd_manager::ContainerdManager::stop_namespace_containerd(
        namespace,
        task_supervisor,
        file_process,
        paths,
    )
    .await
    {
        Ok(0) => tracing::debug!(
            namespace = %namespace,
            "No namespace containerd process remained after cleanup"
        ),
        Ok(stopped) => tracing::info!(
            namespace = %namespace,
            stopped,
            "Stopped namespace containerd process after cleanup"
        ),
        Err(e) => tracing::warn!(
            namespace = %namespace,
            error = %e,
            "Failed to stop namespace containerd after cleanup"
        ),
    }
}

async fn open_cleanup_node_local(
    config: &KlightsConfig,
    task_supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
) -> anyhow::Result<crate::bootstrap::node_store::NodeLocalStores> {
    let node_db_path: Option<&std::path::Path> = if config.in_memory {
        None
    } else {
        Some(config.node_db_path.as_path())
    };
    crate::bootstrap::node_store::open_node_local(
        config.node_local_backend,
        node_db_path,
        task_supervisor,
        "sqlite:node-local-cleanup",
    )
    .await
    .context("failed to open cleanup node-local datastore")
}

async fn cleanup_directories_and_network(
    network_cleanup: &klights_networking::NetworkCleanup,
    node_local: Option<&crate::bootstrap::node_store::NodeLocalStores>,
    containerd_state_dir: &str,
    namespace: &str,
    task_supervisor: &klights_supervisor::TaskSupervisor,
    file_process: &klights_supervisor::FileProcessExecutor,
) -> anyhow::Result<()> {
    if let Some(node_local) = node_local
        && let Err(e) = network_cleanup
            .cleanup_recorded_pod_networks(node_local.pod_network_cache().as_ref())
            .await
    {
        tracing::warn!("Failed to cleanup recorded pod networks: {}", e);
    }
    network_cleanup.cleanup_runtime_network_best_effort().await;

    // Unmount container shm mounts.
    if let Err(e) = shutdown::cleanup_shm_mounts(file_process, containerd_state_dir).await {
        tracing::warn!("Failed to cleanup shm mounts: {}", e);
    }

    // Unmount orphan overlay rootfs mounts (e.g. from crashed containerd).
    let containerd_base = crate::paths::containerd_root_dir_path(namespace)
        .to_string_lossy()
        .into_owned();
    if let Err(e) = shutdown::cleanup_overlay_rootfs_mounts(file_process, &containerd_base).await {
        tracing::warn!("Failed to cleanup overlay rootfs mounts: {}", e);
    }

    if let Err(e) = shutdown::cleanup_containerd_sandbox_mounts(
        file_process,
        containerd_state_dir,
        &containerd_base,
    )
    .await
    {
        tracing::warn!("Failed to cleanup containerd sandbox mounts: {}", e);
    }

    // Remove containerd runtime state. The data root contains image/content
    // metadata, so cleanup leaves it in place and relies on CRI sandbox removal
    // above to remove pod/container/snapshot references.
    tracing::info!("Removing containerd runtime state directories");
    if let Err(e) = shutdown::cleanup_containerd_state_dir(file_process, namespace).await {
        tracing::warn!("Failed to cleanup containerd state dir: {}", e);
    }
    if let Err(e) = shutdown::cleanup_containerd_auxiliary_dirs(file_process, namespace).await {
        tracing::warn!("Failed to cleanup containerd auxiliary dirs: {}", e);
    }

    // Remove CNI config directory.
    tracing::info!("Removing CNI config directory");
    if let Err(e) = shutdown::cleanup_cni_config_dir(file_process, namespace).await {
        tracing::warn!("Failed to cleanup CNI config dir: {}", e);
    }

    // Remove log directory.
    tracing::info!("Removing log directory");
    if let Err(e) = shutdown::cleanup_log_dir(file_process, namespace).await {
        tracing::warn!("Failed to cleanup log dir: {}", e);
    }

    // Remove pod volume directories.
    tracing::info!("Removing pod volume directories");
    if let Err(e) = shutdown::cleanup_volume_dirs(file_process, namespace).await {
        tracing::warn!("Failed to cleanup volume dirs: {}", e);
    }

    // Remove leftover cgroupfs directories for this klights containerd namespace.
    tracing::info!("Removing pod cgroup directories");
    match klights_kubelet::cgroup_cleanup::kill_namespace_cgroup_processes(
        namespace,
        task_supervisor,
        file_process,
    )
    .await
    {
        Ok(killed) if killed > 0 => {
            tracing::info!(killed, "Stopped leftover cgroup processes");
        }
        Ok(_) => {}
        Err(e) => tracing::warn!("Failed to stop leftover cgroup processes: {}", e),
    }
    match klights_kubelet::cgroup_cleanup::cleanup_namespace_cgroup_tree(file_process, namespace)
        .await
    {
        Ok(removed) if removed > 0 => {
            tracing::info!(removed, "Removed cgroup directories");
        }
        Ok(_) => {}
        Err(e) => tracing::warn!("Failed to cleanup cgroup directories: {}", e),
    }

    // Remove the pidfile.
    let pid_path = pidfile::default_pid_path(namespace);
    if let Err(e) = pidfile::remove(&pid_path) {
        tracing::warn!("Failed to remove pidfile: {}", e);
    }

    tracing::info!("Cleanup complete — all resources removed");
    Ok(())
}
