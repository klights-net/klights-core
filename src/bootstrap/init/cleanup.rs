//! Cleanup helpers extracted from runtime.rs (R3 refactor).

use anyhow::Context;

use crate::bootstrap::{CliFlags, NodeMode};
use crate::{KlightsConfig, cni_plugin, kubelet, networking, paths, pidfile, shutdown};

/// Full teardown with the same immutable mode detection used by startup. This
/// keeps root/rootless cleanup dispatch centralized in `networking::NetworkCleanup`.
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
    let cleanup_task_config = crate::task_supervisor::TaskCategoryConfig::from_env()
        .context("invalid task supervisor category limits")?;
    let cleanup_task_supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
        cleanup_task_config,
    ));
    let grpc_transport_policy =
        crate::replication::grpc::transport_policy::GrpcTransportPolicy::shared_default();
    let _ = crate::kubelet::file_blocking::init_file_blocking_supervisor(
        cleanup_task_supervisor.clone(),
    );
    let node_mode =
        NodeMode::detect(cli.rootless).context("failed to detect klights operating mode")?;
    let network_cleanup = networking::NetworkCleanup::from_config(&node_mode, &config);
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
    let mut cleanup_cni_rpc = match start_cleanup_cni_rpc_server(
        namespace,
        &cleanup_task_supervisor,
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
    let mut cri = match kubelet::CriClient::connect_with_policy(
        containerd_socket.to_string_lossy().as_ref(),
        namespace,
        grpc_transport_policy.as_ref(),
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
            stop_namespace_containerd_after_cleanup(namespace, &cleanup_task_supervisor).await;
            if let Some(cleanup_cni_rpc) = cleanup_cni_rpc.take() {
                cleanup_cni_rpc.shutdown().await;
            }
            return cleanup_directories_and_network(
                &network_cleanup,
                cleanup_node_local.as_deref(),
                &containerd_state_dir,
                namespace,
                &cleanup_task_supervisor,
            )
            .await;
        }
    };

    // Drain leftover CRI containers first (covers stale pause/infra
    // containers that can survive sandbox cleanup in rare crash windows).
    if let Err(e) = cleanup_all_runtime_containers(&mut cri).await {
        tracing::warn!("Failed to cleanup leftover runtime containers: {}", e);
    }

    // Stop all pod sandboxes.
    tracing::info!("Stopping all pod sandboxes");
    match cri.list_pod_sandboxes(None).await {
        Ok(sandboxes) => {
            for sb in &sandboxes {
                let _ = cri.stop_pod_sandbox(&sb.id).await;
                match cri.remove_pod_sandbox(&sb.id).await {
                    Ok(()) => {
                        if let Some(uid) = sb
                            .metadata
                            .as_ref()
                            .map(|meta| meta.uid.as_str())
                            .filter(|uid| !uid.trim().is_empty())
                            && let Err(e) =
                                kubelet::cgroup_cleanup::cleanup_pod_cgroup(namespace, uid).await
                        {
                            tracing::warn!(
                                sandbox_id = %sb.id,
                                pod_uid = %uid,
                                error = %e,
                                "Failed to cleanup pod cgroup after sandbox removal"
                            );
                        }
                    }
                    Err(e) => tracing::warn!(
                        sandbox_id = %sb.id,
                        error = %e,
                        "Failed to remove sandbox during cleanup"
                    ),
                }
            }
            tracing::info!("Stopped and removed {} sandboxes", sandboxes.len());
        }
        Err(e) => {
            tracing::warn!("Failed to list sandboxes during cleanup: {}", e);
        }
    }

    stop_namespace_containerd_after_cleanup(namespace, &cleanup_task_supervisor).await;
    if let Some(cleanup_cni_rpc) = cleanup_cni_rpc.take() {
        cleanup_cni_rpc.shutdown().await;
    }

    // Clean up networking and directories.
    cleanup_directories_and_network(
        &network_cleanup,
        cleanup_node_local.as_deref(),
        &containerd_state_dir,
        namespace,
        &cleanup_task_supervisor,
    )
    .await
}

async fn cleanup_all_runtime_containers(cri: &mut kubelet::CriClient) -> anyhow::Result<()> {
    let mut response = cri.list_containers(None).await?;
    if response.containers.is_empty() {
        return Ok(());
    }

    let mut to_stop = 0;
    for container in &response.containers {
        if container.id.trim().is_empty() {
            continue;
        }
        to_stop += 1;
        if let Err(err) = cri.stop_container(&container.id, 5).await {
            tracing::debug!(
                container_id = %container.id,
                error = %err,
                "Failed to stop runtime container during cleanup"
            );
        }
    }

    let mut removed = 0usize;
    for container in response.containers.drain(..) {
        if container.id.trim().is_empty() {
            continue;
        }
        if let Err(err) = cri.remove_container(&container.id).await {
            tracing::debug!(
                container_id = %container.id,
                error = %err,
                "Failed to remove runtime container during cleanup"
            );
            continue;
        }
        removed += 1;
    }
    tracing::info!(
        to_stop,
        removed,
        "Cleaned up lingering runtime containers during klights cleanup"
    );
    Ok(())
}

struct CleanupCniRpcServer {
    cancel: tokio_util::sync::CancellationToken,
    handle: crate::task_supervisor::SupervisedJoinHandle<()>,
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
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
) -> anyhow::Result<CleanupCniRpcServer> {
    let server = cni_plugin::bind_cleanup_rpc_server(namespace, task_supervisor.clone()).await?;
    let socket_path = server.socket_path().to_string();
    let cancel = tokio_util::sync::CancellationToken::new();
    let task_cancel = cancel.clone();
    let handle = match task_supervisor
        .spawn_async(
            crate::task_supervisor::TaskCategory::Background,
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
            let _ = crate::utils::remove_file_if_exists_async(&socket_path).await;
            return Err(e);
        }
    };
    Ok(CleanupCniRpcServer { cancel, handle })
}

pub async fn stop_namespace_containerd_after_cleanup(
    namespace: &str,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
) {
    match kubelet::ContainerdManager::stop_namespace_containerd(namespace, task_supervisor).await {
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
    task_supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
) -> anyhow::Result<crate::datastore::node_local::NodeLocalHandle> {
    let node_db_path: Option<&std::path::Path> = if config.in_memory {
        None
    } else {
        Some(config.node_db_path.as_path())
    };
    crate::datastore::node_local::selector::open_node_local(
        config.node_local_backend,
        node_db_path,
        task_supervisor,
        config.db_key_file.as_deref(),
        "sqlite:node-local-cleanup",
    )
    .await
    .context("failed to open cleanup node-local datastore")
}

async fn cleanup_directories_and_network(
    network_cleanup: &networking::NetworkCleanup,
    node_local: Option<&dyn crate::datastore::node_local::NodeLocalBackend>,
    containerd_state_dir: &str,
    namespace: &str,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
) -> anyhow::Result<()> {
    if let Some(node_local) = node_local
        && let Err(e) = network_cleanup
            .cleanup_recorded_pod_networks(node_local)
            .await
    {
        tracing::warn!("Failed to cleanup recorded pod networks: {}", e);
    }
    network_cleanup.cleanup_runtime_network_best_effort().await;

    // Unmount container shm mounts.
    if let Err(e) = shutdown::cleanup_shm_mounts(containerd_state_dir).await {
        tracing::warn!("Failed to cleanup shm mounts: {}", e);
    }

    // Unmount orphan overlay rootfs mounts (e.g. from crashed containerd).
    let containerd_base = crate::paths::containerd_root_dir_path(namespace)
        .to_string_lossy()
        .into_owned();
    if let Err(e) = shutdown::cleanup_overlay_rootfs_mounts(&containerd_base).await {
        tracing::warn!("Failed to cleanup overlay rootfs mounts: {}", e);
    }

    if let Err(e) =
        shutdown::cleanup_containerd_sandbox_mounts(containerd_state_dir, &containerd_base).await
    {
        tracing::warn!("Failed to cleanup containerd sandbox mounts: {}", e);
    }

    // Remove containerd runtime state. The data root contains image/content
    // metadata, so cleanup leaves it in place and relies on CRI sandbox removal
    // above to remove pod/container/snapshot references.
    tracing::info!("Removing containerd runtime state directories");
    if let Err(e) = shutdown::cleanup_containerd_state_dir(namespace).await {
        tracing::warn!("Failed to cleanup containerd state dir: {}", e);
    }
    if let Err(e) = shutdown::cleanup_containerd_auxiliary_dirs(namespace).await {
        tracing::warn!("Failed to cleanup containerd auxiliary dirs: {}", e);
    }

    // Remove CNI config directory.
    tracing::info!("Removing CNI config directory");
    if let Err(e) = shutdown::cleanup_cni_config_dir(namespace).await {
        tracing::warn!("Failed to cleanup CNI config dir: {}", e);
    }

    // Remove log directory.
    tracing::info!("Removing log directory");
    if let Err(e) = shutdown::cleanup_log_dir(namespace).await {
        tracing::warn!("Failed to cleanup log dir: {}", e);
    }

    // Remove pod volume directories.
    tracing::info!("Removing pod volume directories");
    if let Err(e) = shutdown::cleanup_volume_dirs(namespace).await {
        tracing::warn!("Failed to cleanup volume dirs: {}", e);
    }

    // Remove leftover cgroupfs directories for this klights containerd namespace.
    tracing::info!("Removing pod cgroup directories");
    match kubelet::cgroup_cleanup::kill_namespace_cgroup_processes(namespace, task_supervisor).await
    {
        Ok(killed) if killed > 0 => {
            tracing::info!(killed, "Stopped leftover cgroup processes");
        }
        Ok(_) => {}
        Err(e) => tracing::warn!("Failed to stop leftover cgroup processes: {}", e),
    }
    match kubelet::cgroup_cleanup::cleanup_namespace_cgroup_tree(namespace).await {
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
