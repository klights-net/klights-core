//! Startup recovery extracted from runtime.rs (R3 refactor).

use anyhow::Context;

use crate::bootstrap::NodeMode;
use crate::{KlightsConfig, kubelet, networking, paths, shutdown};

use super::cleanup::stop_namespace_containerd_after_cleanup;

pub async fn run_startup_resource_recovery(
    config: &KlightsConfig,
    node_mode: &NodeMode,
    network_cleanup: &networking::NetworkCleanup,
    containerd_state_dir: &str,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
    grpc_transport_policy: &crate::replication::grpc::transport_policy::GrpcTransportPolicy,
) -> anyhow::Result<()> {
    if config.containerd_socket.is_some() {
        tracing::debug!(
            "Skipping embedded startup recovery because KLIGHTS_CONTAINERD_SOCKET is set"
        );
        return Ok(());
    }

    let namespace = &config.containerd_namespace;
    let rootless = matches!(node_mode, NodeMode::Rootless { .. });
    match kubelet::ContainerdManager::namespace_containerd_is_reusable(
        namespace,
        rootless,
        grpc_transport_policy,
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

    stop_namespace_containerd_after_cleanup(namespace, task_supervisor).await;
    network_cleanup.cleanup_startup_network_best_effort().await;

    if let Err(e) = shutdown::cleanup_shm_mounts(containerd_state_dir).await {
        tracing::warn!("Failed to cleanup stale startup shm mounts: {}", e);
    }

    let containerd_base = paths::containerd_root_dir_path(namespace)
        .to_string_lossy()
        .into_owned();
    if let Err(e) = shutdown::cleanup_overlay_rootfs_mounts(&containerd_base).await {
        tracing::warn!("Failed to cleanup stale startup overlay mounts: {}", e);
    }

    if let Err(e) =
        shutdown::cleanup_containerd_sandbox_mounts(containerd_state_dir, &containerd_base).await
    {
        tracing::warn!("Failed to cleanup stale startup sandbox mounts: {}", e);
    }

    match kubelet::cgroup_cleanup::kill_namespace_cgroup_processes(namespace, task_supervisor).await
    {
        Ok(killed) if killed > 0 => {
            tracing::info!(namespace = %namespace, killed, "Stopped stale startup cgroup processes");
        }
        Ok(_) => {}
        Err(e) => tracing::warn!("Failed to stop stale startup cgroup processes: {}", e),
    }
    match kubelet::cgroup_cleanup::cleanup_namespace_cgroup_tree(namespace).await {
        Ok(removed) if removed > 0 => {
            tracing::info!(namespace = %namespace, removed, "Removed stale startup cgroups");
        }
        Ok(_) => {}
        Err(e) => tracing::warn!("Failed to cleanup stale startup cgroups: {}", e),
    }

    shutdown::cleanup_containerd_root_dir(namespace)
        .await
        .with_context(|| {
            format!("failed to remove unreclaimable embedded containerd root for {namespace}")
        })?;

    if let Err(e) = shutdown::cleanup_cni_config_dir(namespace).await {
        tracing::warn!("Failed to cleanup stale startup CNI config: {}", e);
    }

    Ok(())
}
