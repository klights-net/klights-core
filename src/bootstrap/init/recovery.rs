//! Startup recovery extracted from runtime.rs (R3 refactor).

use anyhow::Context;

use crate::bootstrap::NodeMode;
use crate::{KlightsConfig, paths, shutdown};

use super::cleanup::stop_namespace_containerd_after_cleanup;

pub struct StartupRecoveryContext<'a> {
    pub config: &'a KlightsConfig,
    pub node_mode: &'a NodeMode,
    pub network_cleanup: &'a klights_networking::NetworkCleanup,
    pub containerd_state_dir: &'a str,
    pub runtime_paths: &'a klights_kubelet::runtime_paths::KubeletRuntimePaths,
    pub task_supervisor: &'a klights_supervisor::TaskSupervisor,
    pub file_process: &'a klights_supervisor::FileProcessExecutor,
    pub grpc_transport_policy: &'a klights_leader_rpc::transport_policy::GrpcTransportPolicy,
}

pub async fn run_startup_resource_recovery(
    context: StartupRecoveryContext<'_>,
) -> anyhow::Result<()> {
    let StartupRecoveryContext {
        config,
        node_mode,
        network_cleanup,
        containerd_state_dir,
        runtime_paths,
        task_supervisor,
        file_process,
        grpc_transport_policy,
    } = context;
    if config.containerd_socket.is_some() {
        tracing::debug!(
            "Skipping embedded startup recovery because KLIGHTS_CONTAINERD_SOCKET is set"
        );
        return Ok(());
    }

    let namespace = &config.containerd_namespace;
    let rootless = matches!(node_mode, NodeMode::Rootless { .. });
    let cri_transport_policy = klights_node_api::CriTransportPolicy::new(
        grpc_transport_policy.connect_timeout,
        grpc_transport_policy.max_message_bytes,
    );
    match klights_kubelet::containerd_manager::ContainerdManager::namespace_containerd_is_reusable(
        file_process,
        namespace,
        rootless,
        &klights_kubelet::containerd_manager::ContainerdCriConnectionConfig {
            transport_policy: &cri_transport_policy,
            image_pull_response_timeout: klights_kubelet::cri::DEFAULT_IMAGE_PULL_RESPONSE_TIMEOUT,
            request_timeout: klights_kubelet::cri::DEFAULT_CRI_REQUEST_TIMEOUT,
            supervisor: task_supervisor,
        },
        runtime_paths,
    )
    .await
    {
        Ok(true) => {
            tracing::info!(
                namespace = %namespace,
                "Reclaimed previous embedded containerd for startup"
            );
            return Ok(());
        }
        Ok(false) => {
            tracing::info!(
                namespace = %namespace,
                "No reclaimable embedded containerd found; cleaning stale startup resources"
            );
        }
        Err(e) => {
            tracing::warn!(
                namespace = %namespace,
                error = %e,
                "Previous embedded containerd is not reclaimable; cleaning stale startup resources"
            );
        }
    }

    stop_namespace_containerd_after_cleanup(
        namespace,
        task_supervisor,
        file_process,
        runtime_paths,
    )
    .await;
    network_cleanup.cleanup_startup_network_best_effort().await;

    if let Err(e) = shutdown::cleanup_shm_mounts(file_process, containerd_state_dir).await {
        tracing::warn!("Failed to cleanup stale startup shm mounts: {}", e);
    }

    let containerd_base = paths::containerd_root_dir_path(namespace)
        .to_string_lossy()
        .into_owned();
    if let Err(e) = shutdown::cleanup_overlay_rootfs_mounts(file_process, &containerd_base).await {
        tracing::warn!("Failed to cleanup stale startup overlay mounts: {}", e);
    }

    if let Err(e) = shutdown::cleanup_containerd_sandbox_mounts(
        file_process,
        containerd_state_dir,
        &containerd_base,
    )
    .await
    {
        tracing::warn!("Failed to cleanup stale startup sandbox mounts: {}", e);
    }

    match klights_kubelet::cgroup_cleanup::kill_namespace_cgroup_processes(
        namespace,
        task_supervisor,
        file_process,
    )
    .await
    {
        Ok(killed) if killed > 0 => {
            tracing::info!(namespace = %namespace, killed, "Stopped stale startup cgroup processes");
        }
        Ok(_) => {}
        Err(e) => tracing::warn!("Failed to stop stale startup cgroup processes: {}", e),
    }
    match klights_kubelet::cgroup_cleanup::cleanup_namespace_cgroup_tree(file_process, namespace)
        .await
    {
        Ok(removed) if removed > 0 => {
            tracing::info!(namespace = %namespace, removed, "Removed stale startup cgroups");
        }
        Ok(_) => {}
        Err(e) => tracing::warn!("Failed to cleanup stale startup cgroups: {}", e),
    }

    shutdown::cleanup_containerd_root_dir(file_process, namespace)
        .await
        .with_context(|| {
            format!("failed to remove unreclaimable embedded containerd root for {namespace}")
        })?;

    if let Err(e) = shutdown::cleanup_cni_config_dir(file_process, namespace).await {
        tracing::warn!("Failed to cleanup stale startup CNI config: {}", e);
    }

    Ok(())
}
