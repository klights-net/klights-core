use anyhow::{Context, Result};
use std::collections::HashSet;
use tokio::process::Command;

/// Stop and remove all pod sandboxes on shutdown.
pub async fn cleanup_pod_sandboxes(
    cri: &mut crate::kubelet::CriClient,
    db: &dyn crate::datastore::DatastoreBackend,
    network: &dyn crate::networking::Datapath,
    containerd_ns: &str,
) -> Result<()> {
    let mut sandbox_ids = std::collections::HashSet::new();
    let sandboxes = db.list_sandboxes().await?;
    tracing::info!("Stopping {} recorded pod sandboxes", sandboxes.len());

    for sb in sandboxes {
        sandbox_ids.insert(sb.sandbox_id.clone());
        cleanup_one_pod_sandbox(
            cri,
            db,
            network,
            containerd_ns,
            &sb.sandbox_id,
            Some((&sb.namespace, &sb.pod_name, &sb.pod_uid)),
        )
        .await;
    }

    match cri.list_pod_sandboxes(None).await {
        Ok(runtime_sandboxes) => {
            for sandbox in runtime_sandboxes {
                if sandbox_ids.insert(sandbox.id.clone()) {
                    cleanup_one_pod_sandbox(
                        cri,
                        db,
                        network,
                        containerd_ns,
                        &sandbox.id,
                        sandbox.metadata.as_ref().map(|meta| {
                            (
                                meta.namespace.as_str(),
                                meta.name.as_str(),
                                meta.uid.as_str(),
                            )
                        }),
                    )
                    .await;
                }
            }
        }
        Err(e) => tracing::warn!("Failed to list runtime sandboxes during shutdown: {}", e),
    }

    Ok(())
}

async fn cleanup_one_pod_sandbox(
    cri: &mut crate::kubelet::CriClient,
    db: &dyn crate::datastore::DatastoreBackend,
    network: &dyn crate::networking::Datapath,
    containerd_ns: &str,
    sandbox_id: &str,
    owner: Option<(&str, &str, &str)>,
) {
    if let Err(e) = network.cni_del(sandbox_id).await {
        tracing::warn!(
            "Failed to clean pod network for sandbox {}: {}",
            sandbox_id,
            e
        );
    }

    if let Err(e) = cri.stop_pod_sandbox(sandbox_id).await {
        tracing::warn!("Failed to stop sandbox {}: {}", sandbox_id, e);
    }

    match cri.remove_pod_sandbox(sandbox_id).await {
        Ok(_) => {
            if let Some((_, _, pod_uid)) = owner
                && !pod_uid.trim().is_empty()
                && let Err(e) =
                    crate::kubelet::cgroup_cleanup::cleanup_pod_cgroup(containerd_ns, pod_uid).await
            {
                tracing::warn!(
                    "Failed to cleanup pod cgroup for sandbox {}: {}",
                    sandbox_id,
                    e
                );
            }
            if let Some((namespace, pod_name, pod_uid)) = owner
                && let Err(e) =
                    delete_shutdown_sandbox_row(db, namespace, pod_name, pod_uid, sandbox_id).await
            {
                tracing::warn!(
                    "Failed to delete sandbox SQLite row for {}/{}: {}",
                    namespace,
                    pod_name,
                    e
                );
            }
        }
        Err(e) => tracing::warn!("Failed to remove sandbox {}: {}", sandbox_id, e),
    }
}

async fn delete_shutdown_sandbox_row(
    db: &dyn crate::datastore::DatastoreBackend,
    namespace: &str,
    pod_name: &str,
    pod_uid: &str,
    sandbox_id: &str,
) -> Result<()> {
    if !pod_uid.trim().is_empty() {
        db.delete_sandbox_for_uid(namespace, pod_name, pod_uid, sandbox_id)
            .await
    } else {
        db.delete_sandbox(namespace, pod_name).await
    }
}

/// Extract shm mount points for a given containerd state directory from mount output.
/// Returns paths matching `{state_dir}/io.containerd.grpc.v1.cri/sandboxes/*/shm`.
fn find_shm_mount_points(mount_output: &str, state_dir: &str) -> Vec<String> {
    let prefix = format!("{}/io.containerd.grpc.v1.cri/sandboxes/", state_dir);
    mount_output
        .lines()
        .filter_map(|line| {
            // mount output format: "shm on {state_dir}/io.containerd.grpc.v1.cri/sandboxes/.../shm type tmpfs ..."
            let parts: Vec<&str> = line.splitn(6, ' ').collect();
            if parts.len() >= 3 && parts[2].starts_with(&prefix) && parts[2].ends_with("/shm") {
                Some(parts[2].to_string())
            } else {
                None
            }
        })
        .collect()
}

fn find_mount_points_under_roots(mount_output: &str, roots: &[&str]) -> Vec<String> {
    let normalized_roots: Vec<String> = roots
        .iter()
        .filter_map(|root| {
            let root = root.trim_end_matches('/');
            if root.is_empty() {
                None
            } else {
                Some(root.to_string())
            }
        })
        .collect();

    let mut seen = HashSet::new();
    let mut mount_points: Vec<String> = mount_output
        .lines()
        .filter_map(|line| {
            let parts: Vec<&str> = line.splitn(6, ' ').collect();
            let mount_point = parts.get(2)?.trim_end_matches('/');
            let under_root = normalized_roots
                .iter()
                .any(|root| mount_point == root || mount_point.starts_with(&format!("{root}/")));
            if under_root && seen.insert(mount_point.to_string()) {
                Some(mount_point.to_string())
            } else {
                None
            }
        })
        .collect();

    // Unmount children before parents so nested pod volume mounts do not keep
    // their parent busy during cleanup.
    mount_points.sort_by_key(|mount_point| std::cmp::Reverse(mount_point.len()));
    mount_points
}

async fn unmount_mount_points(mount_points: &[String], label: &str) -> Result<()> {
    let mut count = 0u32;
    for mount_point in mount_points {
        let result = Command::new("umount")
            .arg("-l")
            .arg(mount_point)
            .output()
            .await;

        match result {
            Ok(out) if out.status.success() => {
                count += 1;
            }
            Ok(out) => {
                let stderr = String::from_utf8_lossy(&out.stderr);
                tracing::warn!("Failed to unmount {}: {}", mount_point, stderr.trim());
            }
            Err(e) => {
                tracing::warn!("Failed to run umount for {}: {}", mount_point, e);
            }
        }
    }

    if count > 0 {
        tracing::info!("Unmounted {} {} mounts", count, label);
    }
    Ok(())
}

fn containerd_auxiliary_dir_paths(namespace: &str) -> Vec<std::path::PathBuf> {
    let root = crate::paths::containerd_root_dir_path(namespace);
    vec![root.join("hosts"), root.join("termination")]
}

/// Extract overlay rootfs mount points for a given containerd base directory
/// from mount output. Returns paths matching
/// `{containerd_base}/*/io.containerd.runtime.v2.task/k8s.io/*/rootfs`.
fn find_overlay_rootfs_mount_points(mount_output: &str, containerd_base: &str) -> Vec<String> {
    mount_output
        .lines()
        .filter_map(|line| {
            // mount output format:
            // "overlay on /path/.../io.containerd.runtime.v2.task/k8s.io/.../rootfs type overlay ..."
            let parts: Vec<&str> = line.splitn(6, ' ').collect();
            if parts.len() < 3 {
                return None;
            }
            if parts[0] != "overlay" {
                return None;
            }
            let mount_point = parts[2];
            if !mount_point.starts_with(containerd_base) {
                return None;
            }
            if !mount_point.contains("/io.containerd.runtime.v2.task/k8s.io/")
                || !mount_point.ends_with("/rootfs")
            {
                return None;
            }
            Some(mount_point.to_string())
        })
        .collect()
}

/// Unmount orphan overlay rootfs mounts left behind by containerd.
/// These are at `{containerd_base}/*/io.containerd.runtime.v2.task/k8s.io/*/rootfs`
/// and must be unmounted before containerd data/state directories can be removed.
pub async fn cleanup_overlay_rootfs_mounts(containerd_base: &str) -> Result<()> {
    let output = Command::new("mount")
        .args(["-t", "overlay"])
        .output()
        .await
        .context("Failed to run mount")?;

    let mount_output = String::from_utf8_lossy(&output.stdout);
    let mount_points = find_overlay_rootfs_mount_points(&mount_output, containerd_base);
    unmount_mount_points(&mount_points, "orphan overlay rootfs").await
}

/// Unmount container shm tmpfs mounts left behind by containerd sandboxes.
/// These are at {state_dir}/io.containerd.grpc.v1.cri/sandboxes/*/shm
/// and must be unmounted before containerd can cleanly shut down.
pub async fn cleanup_shm_mounts(state_dir: &str) -> Result<()> {
    let output = Command::new("mount")
        .output()
        .await
        .context("Failed to run mount")?;

    let mount_output = String::from_utf8_lossy(&output.stdout);
    let mount_points = find_shm_mount_points(&mount_output, state_dir);
    unmount_mount_points(&mount_points, "container shm").await
}

/// Unmount remaining sandbox/container-related mounts that may remain after
/// best-effort CRI cleanup.
///
/// These can appear as generic `containerd` mountpoints rooted under our
/// namespace-specific state/data paths and should be removed before removing
/// namespace directories.
pub async fn cleanup_containerd_sandbox_mounts(
    containerd_state_dir: &str,
    containerd_base_dir: &str,
) -> Result<()> {
    let output = Command::new("mount")
        .output()
        .await
        .context("Failed to run mount")?;

    let mount_output = String::from_utf8_lossy(&output.stdout);
    let sandbox_state_root = format!(
        "{}/io.containerd.grpc.v1.cri/sandboxes",
        containerd_state_dir
    );
    // containerd stores task mounts under its `data/` subdirectory:
    //   {containerd_base}/data/io.containerd.runtime.v2.task/k8s.io/.../rootfs
    // The `/data` prefix is required so find_mount_points_under_roots
    // matches these mount points.
    let task_root = format!(
        "{}/data/io.containerd.runtime.v2.task/k8s.io",
        containerd_base_dir
    );
    let mount_roots = vec![sandbox_state_root.as_str(), task_root.as_str()];
    let mount_points = find_mount_points_under_roots(&mount_output, &mount_roots);
    unmount_mount_points(&mount_points, "containerd sandbox").await
}

/// Remove CNI config files created by containerd_manager.
/// Config is namespace-scoped under KLIGHTS_DATA_ROOT.
pub async fn cleanup_cni_config_dir(namespace: &str) -> Result<()> {
    let cni_dir = crate::paths::cni_conf_dir_path(namespace);
    if crate::utils::remove_dir_all_if_exists_async(&cni_dir)
        .await
        .context(format!(
            "Failed to remove CNI config dir {}",
            cni_dir.display()
        ))?
    {
        tracing::info!("Removed CNI config dir: {}", cni_dir.display());
    }
    Ok(())
}

/// Remove containerd state directory.
pub async fn cleanup_containerd_state_dir(namespace: &str) -> Result<()> {
    let state_dir = crate::paths::containerd_state_dir_path(namespace);
    if crate::utils::remove_dir_all_if_exists_async(&state_dir)
        .await
        .context(format!(
            "Failed to remove containerd state dir {}",
            state_dir.display()
        ))?
    {
        tracing::info!("Removed containerd state dir: {}", state_dir.display());
    }
    // Also remove the socket file in the state dir.
    let socket_path = crate::paths::containerd_socket_path(namespace);
    let _ = crate::utils::remove_file_if_exists_async(&socket_path).await;
    // Remove parent if empty.
    if let Some(parent) = state_dir.parent() {
        let _ = crate::utils::remove_dir_if_exists_async(parent).await;
    }
    Ok(())
}

/// Remove the embedded containerd runtime root for an unreclaimable startup.
///
/// This is intentionally stronger than `cleanup_containerd_state_dir`: when a
/// previous embedded containerd cannot be contacted, its data metadata may no
/// longer be trustworthy. Persistent klights state (`state.db`, certs, volumes)
/// lives outside this root and is preserved.
pub async fn cleanup_containerd_root_dir(namespace: &str) -> Result<()> {
    let root = crate::paths::containerd_root_dir_path(namespace);
    let socket_path = crate::paths::containerd_socket_path(namespace);
    let key = root.to_string_lossy().into_owned();
    crate::kubelet::file_blocking::run_blocking_file_keyed(
        "cleanup_containerd_root_dir",
        key,
        move || {
            match std::fs::remove_file(&socket_path) {
                Ok(()) => {
                    tracing::info!("Removed containerd socket: {}", socket_path.display());
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(e)
                        .with_context(|| format!("Failed to remove {}", socket_path.display()));
                }
            }

            match std::fs::remove_dir_all(&root) {
                Ok(()) => {
                    tracing::info!("Removed containerd runtime root: {}", root.display());
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(e).with_context(|| format!("Failed to remove {}", root.display()));
                }
            }
            Ok(())
        },
    )
    .await
}

/// Remove containerd directories that live outside the data/state roots.
pub async fn cleanup_containerd_auxiliary_dirs(namespace: &str) -> Result<()> {
    for dir in containerd_auxiliary_dir_paths(namespace) {
        if crate::utils::remove_dir_all_if_exists_async(&dir)
            .await
            .context(format!(
                "Failed to remove containerd auxiliary dir {}",
                dir.display()
            ))?
        {
            tracing::info!("Removed containerd auxiliary dir: {}", dir.display());
        }
    }

    let root = crate::paths::containerd_root_dir_path(namespace);
    let _ = crate::utils::remove_dir_if_exists_async(&root).await;
    Ok(())
}

/// Remove namespace-specific log directory.
pub async fn cleanup_log_dir(namespace: &str) -> Result<()> {
    let log_dir = crate::paths::pod_logs_root_path(namespace);
    if crate::utils::remove_dir_all_if_exists_async(&log_dir)
        .await
        .context(format!("Failed to remove log dir {}", log_dir.display()))?
    {
        tracing::info!("Removed log dir: {}", log_dir.display());
    }
    Ok(())
}

fn volume_cleanup_roots(namespace: &str) -> Vec<std::path::PathBuf> {
    let mut roots = vec![crate::paths::volumes_root_path(namespace)];
    let legacy_root = std::path::PathBuf::from("/data/pods");
    if !roots.iter().any(|root| root == &legacy_root) {
        roots.push(legacy_root);
    }
    roots
}

/// Remove pod volume directories for the given containerd namespace.
pub async fn cleanup_volume_dirs(namespace: &str) -> Result<()> {
    for volumes_root in volume_cleanup_roots(namespace) {
        if crate::utils::path_exists_async(&volumes_root).await? {
            let mut roots = vec![volumes_root.to_string_lossy().into_owned()];
            if let Ok(canonical) = crate::utils::canonicalize_async(&volumes_root).await {
                roots.push(canonical.to_string_lossy().into_owned());
            }
            let root_refs: Vec<&str> = roots.iter().map(String::as_str).collect();
            let output = Command::new("mount")
                .output()
                .await
                .context("Failed to run mount")?;
            let mount_output = String::from_utf8_lossy(&output.stdout);
            let mount_points = find_mount_points_under_roots(&mount_output, &root_refs);
            unmount_mount_points(&mount_points, "pod volume").await?;

            crate::utils::remove_dir_all_if_exists_async(&volumes_root)
                .await
                .context(format!(
                    "Failed to remove volume dir {}",
                    volumes_root.display()
                ))?;
            tracing::info!("Removed volume dir: {}", volumes_root.display());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_shm_mount_points_matches_correct_namespace() {
        let ns = "klights";
        let state_dir = crate::paths::test_data_root_path(ns)
            .join("state")
            .to_string_lossy()
            .into_owned();
        let other_state_dir = crate::paths::test_data_root_path("klights-other-test")
            .join("state")
            .to_string_lossy()
            .into_owned();
        let mount_output = format!(
            "\
shm on {state_dir}/io.containerd.grpc.v1.cri/sandboxes/abc123/shm type tmpfs (rw,nosuid,nodev)
shm on {state_dir}/io.containerd.grpc.v1.cri/sandboxes/def456/shm type tmpfs (rw,nosuid,nodev)
shm on {other_state_dir}/io.containerd.grpc.v1.cri/sandboxes/other/shm type tmpfs (rw,nosuid,nodev)"
        );

        let results = find_shm_mount_points(&mount_output, &state_dir);
        assert_eq!(results.len(), 2);
        assert!(results[0].contains("abc123"));
        assert!(results[1].contains("def456"));
    }

    #[test]
    fn test_find_shm_mount_points_ignores_other_namespaces() {
        let ns = "klights";
        let state_dir = crate::paths::test_data_root_path(ns)
            .join("state")
            .to_string_lossy()
            .into_owned();
        let other_state_dir = crate::paths::test_data_root_path("klights-other-test")
            .join("state")
            .to_string_lossy()
            .into_owned();
        let mount_output = format!(
            "\
shm on {other_state_dir}/io.containerd.grpc.v1.cri/sandboxes/abc/shm type tmpfs (rw)
tmpfs on /dev/shm type tmpfs (rw,nosuid,nodev)"
        );

        let results = find_shm_mount_points(&mount_output, &state_dir);
        assert!(results.is_empty());
    }

    #[test]
    fn test_find_shm_mount_points_empty_output() {
        let ns = "klights";
        let state_dir = crate::paths::test_data_root_path(ns)
            .join("state")
            .to_string_lossy()
            .into_owned();
        let results = find_shm_mount_points("", &state_dir);
        assert!(results.is_empty());
    }

    #[test]
    fn test_find_shm_mount_points_ignores_non_shm_mounts() {
        let ns = "klights";
        let state_dir = crate::paths::test_data_root_path(ns)
            .join("state")
            .to_string_lossy()
            .into_owned();
        let mount_output = format!(
            "\
overlay on {state_dir}/io.containerd.grpc.v1.cri/sandboxes/abc/rootfs type overlay (rw)
shm on {state_dir}/io.containerd.grpc.v1.cri/sandboxes/abc/shm type tmpfs (rw)"
        );

        let results = find_shm_mount_points(&mount_output, &state_dir);
        assert_eq!(results.len(), 1);
        assert!(results[0].ends_with("/shm"));
    }

    #[test]
    fn test_cleanup_paths_derived_from_namespace() {
        let ns = "klights-architect";

        // CNI config dir
        assert_eq!(
            crate::paths::cni_conf_dir_path(ns)
                .to_string_lossy()
                .into_owned(),
            crate::paths::data_root_path(ns)
                .join("cni")
                .join("net.d")
                .join(ns)
                .to_string_lossy()
                .into_owned()
        );

        // Containerd data dir
        assert_eq!(
            crate::paths::containerd_data_dir_path(ns)
                .to_string_lossy()
                .into_owned(),
            crate::paths::data_root_path(ns)
                .join("containerd")
                .join("data")
                .to_string_lossy()
                .into_owned()
        );

        // Containerd state dir
        assert_eq!(
            crate::paths::containerd_state_dir_path(ns)
                .to_string_lossy()
                .into_owned(),
            crate::paths::data_root_path(ns)
                .join("containerd")
                .join("state")
                .to_string_lossy()
                .into_owned()
        );

        // Log dir
        assert_eq!(
            crate::paths::pod_logs_root_path(ns)
                .to_string_lossy()
                .into_owned(),
            crate::paths::data_root_path(ns)
                .join("logs")
                .join("pods")
                .to_string_lossy()
                .into_owned()
        );

        // Socket path
        assert_eq!(
            crate::paths::containerd_socket_path(ns)
                .to_string_lossy()
                .into_owned(),
            crate::paths::data_root_path(ns)
                .join("containerd.sock")
                .to_string_lossy()
                .into_owned()
        );
    }

    #[test]
    fn test_cleanup_paths_default_namespace() {
        let ns = "klights";

        assert_eq!(
            crate::paths::cni_conf_dir_path(ns)
                .to_string_lossy()
                .into_owned(),
            crate::paths::data_root_path(ns)
                .join("cni")
                .join("net.d")
                .join(ns)
                .to_string_lossy()
                .into_owned()
        );
        assert_eq!(
            crate::paths::containerd_data_dir_path(ns)
                .to_string_lossy()
                .into_owned(),
            crate::paths::data_root_path(ns)
                .join("containerd")
                .join("data")
                .to_string_lossy()
                .into_owned()
        );
        assert_eq!(
            crate::paths::containerd_state_dir_path(ns)
                .to_string_lossy()
                .into_owned(),
            crate::paths::data_root_path(ns)
                .join("containerd")
                .join("state")
                .to_string_lossy()
                .into_owned()
        );
        assert_eq!(
            crate::paths::pod_logs_root_path(ns)
                .to_string_lossy()
                .into_owned(),
            crate::paths::data_root_path(ns)
                .join("logs")
                .join("pods")
                .to_string_lossy()
                .into_owned()
        );
    }

    #[test]
    fn test_volume_cleanup_roots_include_legacy_data_pods() {
        let roots = volume_cleanup_roots("klights");
        assert!(
            roots
                .iter()
                .any(|root| root == std::path::Path::new("/data/pods")),
            "explicit cleanup must remove legacy /data/pods volume mounts"
        );
    }

    #[test]
    fn test_find_shm_mount_points_custom_namespace() {
        let ns = "klights-dev";
        let state_dir = crate::paths::test_data_root_path(ns)
            .join("state")
            .to_string_lossy()
            .into_owned();
        let mount_output = format!(
            "shm on {state_dir}/io.containerd.grpc.v1.cri/sandboxes/xyz/shm type tmpfs (rw)"
        );

        let results = find_shm_mount_points(&mount_output, &state_dir);
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0],
            format!("{state_dir}/io.containerd.grpc.v1.cri/sandboxes/xyz/shm")
        );
    }

    // --- overlay rootfs mount tests ---

    #[test]
    fn test_find_overlay_rootfs_matches_containerd_task_path() {
        let ns = "klights";
        let base = crate::paths::test_data_root_path(ns)
            .join("containerd")
            .to_string_lossy()
            .into_owned();
        let mount_output = format!(
            "overlay on {base}/data/io.containerd.runtime.v2.task/k8s.io/abc123/rootfs type overlay (rw,relatime,lowerdir=/a,upperdir=/b,workdir=/c)"
        );

        let results = find_overlay_rootfs_mount_points(&mount_output, &base);
        assert_eq!(results.len(), 1);
        assert!(results[0].ends_with("/rootfs"));
        assert!(results[0].contains("/io.containerd.runtime.v2.task/k8s.io/"));
    }

    #[test]
    fn test_find_overlay_rootfs_ignores_other_namespace() {
        let ns = "klights";
        let base = crate::paths::test_data_root_path(ns)
            .join("containerd")
            .to_string_lossy()
            .into_owned();
        let other_base = crate::paths::test_data_root_path("klights-other")
            .join("containerd")
            .to_string_lossy()
            .into_owned();
        let mount_output = format!(
            "overlay on {other_base}/data/io.containerd.runtime.v2.task/k8s.io/abc/rootfs type overlay (rw)"
        );

        let results = find_overlay_rootfs_mount_points(&mount_output, &base);
        assert!(results.is_empty());
    }

    #[test]
    fn test_find_overlay_rootfs_ignores_non_overlay_mounts() {
        let ns = "klights";
        let base = crate::paths::test_data_root_path(ns)
            .join("containerd")
            .to_string_lossy()
            .into_owned();
        let mount_output = format!(
            "shm on {base}/data/io.containerd.grpc.v1.cri/sandboxes/abc/shm type tmpfs (rw)\n\
             tmpfs on /dev/shm type tmpfs (rw,nosuid,nodev)"
        );

        let results = find_overlay_rootfs_mount_points(&mount_output, &base);
        assert!(results.is_empty());
    }

    #[test]
    fn test_find_overlay_rootfs_empty_output() {
        let ns = "klights";
        let base = crate::paths::test_data_root_path(ns)
            .join("containerd")
            .to_string_lossy()
            .into_owned();
        let results = find_overlay_rootfs_mount_points("", &base);
        assert!(results.is_empty());
    }

    #[test]
    fn test_find_overlay_rootfs_requires_rootfs_suffix() {
        let ns = "klights";
        let base = crate::paths::test_data_root_path(ns)
            .join("containerd")
            .to_string_lossy()
            .into_owned();
        let mount_output = format!(
            "overlay on {base}/data/io.containerd.runtime.v2.task/k8s.io/abc/notrootfs type overlay (rw)"
        );

        let results = find_overlay_rootfs_mount_points(&mount_output, &base);
        assert!(results.is_empty());
    }

    #[test]
    fn test_find_overlay_rootfs_multiple_mounts() {
        let ns = "klights";
        let base = crate::paths::test_data_root_path(ns)
            .join("containerd")
            .to_string_lossy()
            .into_owned();
        let mount_output = format!(
            "overlay on {base}/data/io.containerd.runtime.v2.task/k8s.io/id1/rootfs type overlay (rw)\n\
             overlay on {base}/data/io.containerd.runtime.v2.task/k8s.io/id2/rootfs type overlay (rw)\n\
             overlay on /some/other/rootfs type overlay (rw)"
        );

        let results = find_overlay_rootfs_mount_points(&mount_output, &base);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_cleanup_containerd_sandbox_mounts_collects_mounts_under_sandbox_roots() {
        let ns = "klights";
        let state = crate::paths::test_data_root_path(ns)
            .join("state")
            .to_string_lossy()
            .into_owned();
        let base = crate::paths::test_data_root_path(ns)
            .join("containerd")
            .to_string_lossy()
            .into_owned();
        let mount_output = format!(
            "overlay on {base}/data/io.containerd.runtime.v2.task/k8s.io/abc/rootfs type overlay (rw)\n\
tmpfs on {state}/io.containerd.grpc.v1.cri/sandboxes/pod-shm type tmpfs (rw)\n\
tmpfs on /data/other/state type tmpfs (rw)"
        );

        let roots = [
            format!("{state}/io.containerd.grpc.v1.cri/sandboxes"),
            format!("{base}/data/io.containerd.runtime.v2.task/k8s.io"),
        ];
        let root_refs: Vec<&str> = roots.iter().map(String::as_str).collect();
        let results = find_mount_points_under_roots(&mount_output, &root_refs);

        assert_eq!(results.len(), 2);
        assert!(results.iter().any(|m| m.contains("pod-shm")));
        assert!(
            results
                .iter()
                .any(|m| m.contains("/io.containerd.runtime.v2.task/k8s.io/abc/rootfs"))
        );
    }

    #[test]
    fn test_find_mount_points_under_roots_matches_symlink_target_and_raw_root() {
        let raw_root = "/root/klights/pods";
        let canonical_root = "/data/klights/pods";
        let mount_output = "\
tmpfs on /data/klights/pods/emptydir-1/volumes/empty-dir/cache type tmpfs (rw,relatime)\n\
tmpfs on /root/klights/pods/emptydir-2/volumes/empty-dir/cache type tmpfs (rw,relatime)\n\
tmpfs on /data/klights-other/pods/emptydir-3/volumes/empty-dir/cache type tmpfs (rw,relatime)";

        let results = find_mount_points_under_roots(mount_output, &[raw_root, canonical_root]);

        assert_eq!(
            results,
            vec![
                "/data/klights/pods/emptydir-1/volumes/empty-dir/cache".to_string(),
                "/root/klights/pods/emptydir-2/volumes/empty-dir/cache".to_string(),
            ]
        );
    }

    #[test]
    fn test_find_mount_points_under_roots_orders_children_before_parents() {
        let root = "/data/klights/pods";
        let mount_output = "\
tmpfs on /data/klights/pods/pod-a type tmpfs (rw,relatime)\n\
tmpfs on /data/klights/pods/pod-a/volumes/empty-dir/cache type tmpfs (rw,relatime)";

        let results = find_mount_points_under_roots(mount_output, &[root]);

        assert_eq!(
            results,
            vec![
                "/data/klights/pods/pod-a/volumes/empty-dir/cache".to_string(),
                "/data/klights/pods/pod-a".to_string(),
            ]
        );
    }

    #[test]
    fn test_containerd_auxiliary_cleanup_paths_cover_hosts_and_termination() {
        let ns = "klights";
        let paths = containerd_auxiliary_dir_paths(ns);

        assert_eq!(
            paths,
            vec![
                crate::paths::data_root_path(ns)
                    .join("containerd")
                    .join("hosts"),
                crate::paths::data_root_path(ns)
                    .join("containerd")
                    .join("termination"),
            ]
        );
    }

    #[tokio::test]
    async fn shutdown_cleanup_deletes_only_matching_sandbox_uid() {
        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();
        db.record_sandbox("default", "same-name", "uid-a", "sandbox-a")
            .await
            .unwrap();
        db.record_sandbox("default", "same-name", "uid-b", "sandbox-b")
            .await
            .unwrap();

        delete_shutdown_sandbox_row(&db, "default", "same-name", "uid-a", "sandbox-a")
            .await
            .unwrap();

        assert!(
            db.get_sandbox_for_uid("default", "same-name", "uid-a")
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            db.get_sandbox_for_uid("default", "same-name", "uid-b")
                .await
                .unwrap(),
            Some("sandbox-b".to_string())
        );
    }
}
