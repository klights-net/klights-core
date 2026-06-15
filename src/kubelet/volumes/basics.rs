use anyhow::{Context, Result};
use serde_json::Value;
use std::collections::BTreeSet;
use std::path::PathBuf;

pub fn volumes_root() -> String {
    let runtime_ns = crate::paths::runtime_namespace();
    volumes_root_for_namespace(&runtime_ns)
}

pub fn volumes_root_for_namespace(runtime_ns: &str) -> String {
    crate::paths::volumes_root_path(runtime_ns)
        .to_string_lossy()
        .into_owned()
}

pub fn empty_dir_volume_path(pod_name: &str, volume_name: &str) -> String {
    let runtime_ns = crate::paths::runtime_namespace();
    empty_dir_volume_path_for_namespace(&runtime_ns, pod_name, volume_name)
}

pub fn empty_dir_volume_path_for_namespace(
    runtime_ns: &str,
    pod_name: &str,
    volume_name: &str,
) -> String {
    format!(
        "{}/{}/volumes/empty-dir/{}",
        volumes_root_for_namespace(runtime_ns),
        pod_name,
        volume_name
    )
}
fn canonicalize_existing_path_for_mount(path: &str) -> Result<String> {
    std::fs::canonicalize(path)
        .with_context(|| format!("Failed to canonicalize volume path {}", path))
        .map(|p| p.to_string_lossy().into_owned())
}

/// Validate raw subPath and subPathExpr values in a pod spec.
/// K8s apiserver rejects values that are absolute paths or contain `..`.
/// subPathExpr expansion is kubelet runtime behavior and is validated after
/// environment and fieldRef values are resolved.
pub fn validate_volume_subpaths(pod: &Value) -> Result<(), String> {
    let containers_fields = ["containers", "initContainers"];
    let spec = match pod.get("spec") {
        Some(s) => s,
        None => return Ok(()),
    };

    for field in &containers_fields {
        let containers = match spec.get(field).and_then(|c| c.as_array()) {
            Some(c) => c,
            None => continue,
        };
        for container in containers {
            let container_name = container
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or("<unknown>");
            let volume_mounts = match container.get("volumeMounts").and_then(|v| v.as_array()) {
                Some(v) => v,
                None => continue,
            };
            for vm in volume_mounts {
                for sub_path_field in &["subPath", "subPathExpr"] {
                    if let Some(sub_path) = vm.get(sub_path_field).and_then(|s| s.as_str()) {
                        if sub_path.starts_with('/') {
                            return Err(format!(
                                "spec.{}.{}.volumeMounts.{}: Invalid value: \"{}\": must be a relative path",
                                field, container_name, sub_path_field, sub_path
                            ));
                        }
                        // Check for .. components in the path
                        for component in sub_path.split('/') {
                            if component == ".." {
                                return Err(format!(
                                    "spec.{}.{}.volumeMounts.{}: Invalid value: \"{}\": must not contain '..'",
                                    field, container_name, sub_path_field, sub_path
                                ));
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

/// Validate projection file paths in a pod spec.
///
/// The `path` of every `configMap`/`secret`/`downwardAPI`/`projected`
/// `items[]` entry (and `projected` `serviceAccountToken.path`) is rendered by
/// the kubelet relative to the volume directory. The upstream K8s apiserver
/// rejects absolute paths and any `..` component in these fields; klights must
/// do the same, otherwise a user who can create a Pod could escape the volume
/// directory and have the (root) kubelet write attacker-controlled bytes to an
/// arbitrary host path. See [`super::shared::projection_path_is_safe`] for the
/// last-line render-time guard.
pub fn validate_volume_projection_paths(pod: &Value) -> Result<(), String> {
    let Some(volumes) = pod
        .get("spec")
        .and_then(|s| s.get("volumes"))
        .and_then(|v| v.as_array())
    else {
        return Ok(());
    };

    for volume in volumes {
        let vol_name = volume
            .get("name")
            .and_then(|n| n.as_str())
            .unwrap_or("<unknown>");

        if let Some(cm) = volume.get("configMap") {
            check_projection_items(vol_name, "configMap", cm.get("items"))?;
        }
        if let Some(secret) = volume.get("secret") {
            check_projection_items(vol_name, "secret", secret.get("items"))?;
        }
        if let Some(dapi) = volume.get("downwardAPI") {
            check_projection_items(vol_name, "downwardAPI", dapi.get("items"))?;
        }
        if let Some(projected) = volume.get("projected")
            && let Some(sources) = projected.get("sources").and_then(|s| s.as_array())
        {
            for source in sources {
                if let Some(cm) = source.get("configMap") {
                    check_projection_items(vol_name, "projected.configMap", cm.get("items"))?;
                }
                if let Some(secret) = source.get("secret") {
                    check_projection_items(vol_name, "projected.secret", secret.get("items"))?;
                }
                if let Some(dapi) = source.get("downwardAPI") {
                    check_projection_items(vol_name, "projected.downwardAPI", dapi.get("items"))?;
                }
                if let Some(sat) = source.get("serviceAccountToken")
                    && let Some(path) = sat.get("path").and_then(|p| p.as_str())
                {
                    check_projection_path(vol_name, "projected.serviceAccountToken", path)?;
                }
            }
        }
    }
    Ok(())
}

fn check_projection_items(
    vol_name: &str,
    source: &str,
    items: Option<&Value>,
) -> Result<(), String> {
    let Some(items) = items.and_then(|i| i.as_array()) else {
        return Ok(());
    };
    for item in items {
        if let Some(path) = item.get("path").and_then(|p| p.as_str()) {
            check_projection_path(vol_name, source, path)?;
        }
    }
    Ok(())
}

fn check_projection_path(vol_name: &str, source: &str, path: &str) -> Result<(), String> {
    if path.is_empty() {
        return Err(format!(
            "spec.volumes.{}.{}.items.path: Required value: must not be empty",
            vol_name, source
        ));
    }
    if path.starts_with('/') {
        return Err(format!(
            "spec.volumes.{}.{}.items.path: Invalid value: \"{}\": must be a relative path",
            vol_name, source, path
        ));
    }
    if path.split('/').any(|component| component == "..") {
        return Err(format!(
            "spec.volumes.{}.{}.items.path: Invalid value: \"{}\": must not contain '..'",
            vol_name, source, path
        ));
    }
    Ok(())
}

/// Creates an emptyDir volume for a pod with world-writable permissions.
/// K8s spec requires emptyDir to be writable by the pod's fsGroup if set.
pub fn create_empty_dir(
    pod_name: &str,
    volume_name: &str,
    medium: Option<&str>,
    size_limit: Option<&str>,
) -> Result<String> {
    let runtime_ns = crate::paths::runtime_namespace();
    create_empty_dir_for_namespace(&runtime_ns, pod_name, volume_name, medium, size_limit)
}

pub fn create_empty_dir_for_namespace(
    runtime_ns: &str,
    pod_name: &str,
    volume_name: &str,
    medium: Option<&str>,
    size_limit: Option<&str>,
) -> Result<String> {
    let configured_path = empty_dir_volume_path_for_namespace(runtime_ns, pod_name, volume_name);
    std::fs::create_dir_all(&configured_path)
        .with_context(|| format!("Failed to create emptyDir volume at {}", configured_path))?;
    let path = canonicalize_existing_path_for_mount(&configured_path)?;

    // If medium is "Memory", create a tmpfs mount instead of using disk.
    // On container restart the same emptyDir path must be reused without
    // remounting, otherwise tmpfs would be reset and data would be lost.
    if medium == Some("Memory") {
        if !is_tmpfs_mounted_at_path(&path) {
            // Mount tmpfs at the path
            let mut cmd = std::process::Command::new("mount");
            cmd.arg("-t").arg("tmpfs");

            // Add size limit if provided (K8s uses bytes, tmpfs uses bytes)
            if let Some(limit) = size_limit {
                // Parse K8s size limit (e.g., "64Mi", "1Gi") to bytes
                let bytes = parse_k8s_quantity(limit)
                    .with_context(|| format!("Failed to parse sizeLimit: {}", limit))?;
                cmd.arg("-o").arg(format!("size={}", bytes));
            }

            cmd.arg("tmpfs").arg(&path);

            let output = cmd.output().with_context(|| {
                format!("Failed to execute mount command for tmpfs at {}", path)
            })?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                anyhow::bail!("Failed to mount tmpfs at {}: {}", path, stderr);
            }

            tracing::debug!("Mounted tmpfs at {} with size_limit={:?}", path, size_limit);
        } else {
            tracing::debug!("Reusing existing tmpfs mount at {}", path);
        }
    }

    // Set 0777 permissions so any user/group can write
    // This matches K8s behavior where emptyDir is writable by the container's fsGroup
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(&path)?.permissions();
    perms.set_mode(0o777);
    std::fs::set_permissions(&path, perms)
        .with_context(|| format!("Failed to set permissions on emptyDir volume at {}", path))?;

    Ok(path)
}

#[cfg(test)]
mod projection_path_tests {
    use super::*;
    use serde_json::json;

    fn pod_with_configmap_item_path(path: &str) -> Value {
        json!({
            "spec": {
                "volumes": [{
                    "name": "cfg",
                    "configMap": {"name": "cm", "items": [{"key": "k", "path": path}]}
                }]
            }
        })
    }

    #[test]
    fn rejects_parent_traversal_in_configmap_item_path() {
        let pod = pod_with_configmap_item_path("../../../../etc/cron.d/evil");
        let err = validate_volume_projection_paths(&pod).unwrap_err();
        assert!(err.contains(".."), "unexpected error: {err}");
    }

    #[test]
    fn rejects_absolute_configmap_item_path() {
        let pod = pod_with_configmap_item_path("/etc/cron.d/evil");
        let err = validate_volume_projection_paths(&pod).unwrap_err();
        assert!(err.contains("must be a relative path"), "unexpected: {err}");
    }

    #[test]
    fn rejects_empty_configmap_item_path() {
        let pod = pod_with_configmap_item_path("");
        assert!(validate_volume_projection_paths(&pod).is_err());
    }

    #[test]
    fn allows_safe_nested_relative_path() {
        let pod = pod_with_configmap_item_path("sub/dir/file.conf");
        assert!(validate_volume_projection_paths(&pod).is_ok());
    }

    #[test]
    fn rejects_traversal_in_secret_projected_downward_and_sa_token() {
        let pod = json!({
            "spec": {"volumes": [{
                "name": "p",
                "projected": {"sources": [
                    {"secret": {"name": "s", "items": [{"key": "k", "path": "../escape"}]}},
                    {"serviceAccountToken": {"path": "ok"}},
                ]}
            }]}
        });
        assert!(validate_volume_projection_paths(&pod).is_err());

        let pod = json!({
            "spec": {"volumes": [{
                "name": "p",
                "projected": {"sources": [
                    {"serviceAccountToken": {"path": "../../escape-token"}},
                ]}
            }]}
        });
        assert!(validate_volume_projection_paths(&pod).is_err());

        let pod = json!({
            "spec": {"volumes": [{
                "name": "d",
                "downwardAPI": {"items": [{"path": "../meta", "fieldRef": {"fieldPath": "metadata.name"}}]}
            }]}
        });
        assert!(validate_volume_projection_paths(&pod).is_err());
    }

    #[test]
    fn render_guard_rejects_unsafe_relative_path() {
        // Defense-in-depth: even if admission were bypassed, the writer refuses.
        let tmp = tempfile::tempdir().unwrap();
        let base = tmp.path().to_string_lossy().into_owned();
        let err =
            super::super::shared::write_projection_file_blocking(&base, "../escape", b"x", 0o644)
                .unwrap_err();
        assert!(err.to_string().contains("unsafe path"), "got: {err}");
        // And the file must not have been created outside the base dir.
        assert!(!tmp.path().parent().unwrap().join("escape").exists());
    }
}

#[cfg(test)]
mod empty_dir_path_tests {
    use super::*;

    #[test]
    fn canonicalize_existing_path_for_mount_resolves_symlinked_data_root() {
        let tmp = tempfile::tempdir().unwrap();
        let real_root = tmp.path().join("real");
        let link_root = tmp.path().join("link");
        std::fs::create_dir_all(real_root.join("pods/p/volumes/empty-dir/cache")).unwrap();
        std::os::unix::fs::symlink(&real_root, &link_root).unwrap();

        let configured = link_root.join("pods/p/volumes/empty-dir/cache");
        let canonicalized =
            canonicalize_existing_path_for_mount(configured.to_str().unwrap()).unwrap();

        assert_eq!(
            canonicalized,
            std::fs::canonicalize(real_root.join("pods/p/volumes/empty-dir/cache"))
                .unwrap()
                .to_string_lossy()
        );
    }
}

pub fn parse_mountinfo_entry(line: &str) -> Option<(&str, &str)> {
    let (pre, post) = line.split_once(" - ")?;
    let mount_point = pre.split_whitespace().nth(4)?;
    let fs_type = post.split_whitespace().next()?;
    Some((mount_point, fs_type))
}

fn is_tmpfs_mounted_at_path(path: &str) -> bool {
    let Ok(mountinfo) = crate::utils::read_utf8_file("/proc/self/mountinfo") else {
        return false;
    };
    mountinfo.lines().any(|line| {
        parse_mountinfo_entry(line)
            .map(|(mount_point, fs_type)| mount_point == path && fs_type == "tmpfs")
            .unwrap_or(false)
    })
}

fn normalize_mount_root(root: &str) -> String {
    let root = root.trim_end_matches('/').to_string();
    if root.is_empty() {
        "/".to_string()
    } else {
        root
    }
}

fn mount_target_roots(root: &str, canonical_root: Option<&str>) -> Vec<String> {
    let root = normalize_mount_root(root);
    let mut roots = vec![root.clone()];
    if let Some(canonical) = canonical_root {
        let canonical = normalize_mount_root(canonical);
        if canonical != root {
            roots.push(canonical);
        }
    }
    roots
}

fn mount_point_is_under_root(mount_point: &str, root: &str) -> bool {
    if mount_point == root {
        return true;
    }
    if root == "/" {
        return mount_point.starts_with('/');
    }
    let root_prefix = format!("{root}/");
    mount_point.starts_with(&root_prefix)
}

fn collect_mount_targets_under_roots(mountinfo: &str, roots: &[String]) -> Vec<String> {
    let mut targets: BTreeSet<String> = BTreeSet::new();
    for line in mountinfo.lines() {
        let Some((mount_point, _fs_type)) = parse_mountinfo_entry(line) else {
            continue;
        };
        if roots
            .iter()
            .any(|root| mount_point_is_under_root(mount_point, root))
        {
            targets.insert(mount_point.to_string());
        }
    }

    let mut sorted: Vec<String> = targets.into_iter().collect();
    // Deepest mount points first so parent unmount is not blocked by children.
    sorted.sort_by_key(|b| std::cmp::Reverse(b.matches('/').count()));
    sorted
}

#[cfg(test)]
fn collect_mount_targets_under(mountinfo: &str, root: &str) -> Vec<String> {
    let roots = mount_target_roots(root, None);
    collect_mount_targets_under_roots(mountinfo, &roots)
}

pub async fn unmount_volume_mounts_under(root: &str) -> Result<()> {
    let mountinfo = match crate::utils::read_utf8_file_async("/proc/self/mountinfo").await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(
                "Failed reading /proc/self/mountinfo while cleaning {}: {}",
                root,
                e
            );
            return Ok(());
        }
    };
    let canonical_root = crate::utils::canonicalize_async(root)
        .await
        .ok()
        .map(|path| path.to_string_lossy().to_string());
    let roots = mount_target_roots(root, canonical_root.as_deref());
    let targets = collect_mount_targets_under_roots(&mountinfo, &roots);
    for target in targets {
        let target_for_cmd = target.clone();
        let result = crate::kubelet::file_blocking::run_blocking_file_keyed(
            "kubelet_unmount_volume_target",
            target.clone(),
            move || {
                // INVARIANT (D4): the `-l` (lazy) flag is load-bearing for the
                // unmount-before-remove safety in pod cleanup. Even if a mount is
                // EBUSY, lazy detach removes it from the namespace immediately, so
                // the caller's subsequent `remove_dir_all` over the pod dir never
                // recurses into a still-live mount. `-R` recursively detaches
                // submounts. Do not drop either flag without re-introducing an
                // explicit "verify unmounted before remove" step. Guarded by
                // scripts/check_supervisor_spawn.sh.
                let output = std::process::Command::new("umount")
                    .arg("-R")
                    .arg("-l")
                    .arg(&target_for_cmd)
                    .output()
                    .with_context(|| format!("Failed to execute umount for {target_for_cmd}"))?;
                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                    if !stderr.contains("not mounted")
                        && !stderr.contains("no mount point specified")
                    {
                        anyhow::bail!("Failed to unmount {target_for_cmd}: {stderr}");
                    }
                }
                Ok(())
            },
        )
        .await;

        if let Err(e) = result {
            tracing::warn!("Unmount failed for {}: {}", target, e);
        }
    }
    Ok(())
}

/// Parse K8s quantity string (e.g., "64Mi", "1Gi", "512k") to bytes
pub fn parse_k8s_quantity(quantity: &str) -> Result<u64> {
    let quantity = quantity.trim();

    // Match suffixes
    let (value_str, multiplier) = if let Some(v) = quantity.strip_suffix("Ki") {
        (v, 1024u64)
    } else if let Some(v) = quantity.strip_suffix("Mi") {
        (v, 1024 * 1024)
    } else if let Some(v) = quantity.strip_suffix("Gi") {
        (v, 1024 * 1024 * 1024)
    } else if let Some(v) = quantity.strip_suffix("Ti") {
        (v, 1024 * 1024 * 1024 * 1024)
    } else if let Some(v) = quantity.strip_suffix('k') {
        (v, 1000u64)
    } else if let Some(v) = quantity.strip_suffix('M') {
        (v, 1000 * 1000)
    } else if let Some(v) = quantity.strip_suffix('G') {
        (v, 1000 * 1000 * 1000)
    } else if let Some(v) = quantity.strip_suffix('T') {
        (v, 1000 * 1000 * 1000 * 1000)
    } else {
        // No suffix - assume bytes
        (quantity, 1u64)
    };

    let value: u64 = value_str
        .parse()
        .with_context(|| format!("Failed to parse quantity value: {}", value_str))?;

    Ok(value * multiplier)
}

/// Resolves a hostPath volume with type validation
///
/// Supported types:
/// - DirectoryOrCreate: create directory if not exists
/// - FileOrCreate: create file if not exists
/// - Directory: must exist and be a directory
/// - File: must exist and be a file
/// - Socket, CharDevice, BlockDevice: validate type matches
pub fn resolve_host_path(host_path: &str, host_type: Option<&str>) -> Result<String> {
    let path = PathBuf::from(host_path);

    match host_type {
        Some("DirectoryOrCreate") => {
            if !path.exists() {
                std::fs::create_dir_all(&path)
                    .with_context(|| format!("Failed to create directory at {}", host_path))?;
            } else if !path.is_dir() {
                anyhow::bail!("hostPath {} exists but is not a directory", host_path);
            }
        }
        Some("FileOrCreate") => {
            if !path.exists() {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent).with_context(|| {
                        format!("Failed to create parent directory for {}", host_path)
                    })?;
                }
                std::fs::File::create(&path)
                    .with_context(|| format!("Failed to create file at {}", host_path))?;
            } else if !path.is_file() {
                anyhow::bail!("hostPath {} exists but is not a file", host_path);
            }
        }
        Some("Directory") => {
            if !path.exists() {
                anyhow::bail!("hostPath {} does not exist (type=Directory)", host_path);
            }
            if !path.is_dir() {
                anyhow::bail!("hostPath {} is not a directory", host_path);
            }
        }
        Some("File") => {
            if !path.exists() {
                anyhow::bail!("hostPath {} does not exist (type=File)", host_path);
            }
            if !path.is_file() {
                anyhow::bail!("hostPath {} is not a file", host_path);
            }
        }
        Some("Socket") => {
            if !path.exists() {
                anyhow::bail!("hostPath {} does not exist (type=Socket)", host_path);
            }
            // Check if it's a socket
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileTypeExt;
                let metadata = std::fs::metadata(&path)
                    .with_context(|| format!("Failed to get metadata for {}", host_path))?;
                if !metadata.file_type().is_socket() {
                    anyhow::bail!("hostPath {} is not a socket", host_path);
                }
            }
        }
        Some("CharDevice") => {
            if !path.exists() {
                anyhow::bail!("hostPath {} does not exist (type=CharDevice)", host_path);
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileTypeExt;
                let metadata = std::fs::metadata(&path)
                    .with_context(|| format!("Failed to get metadata for {}", host_path))?;
                if !metadata.file_type().is_char_device() {
                    anyhow::bail!("hostPath {} is not a character device", host_path);
                }
            }
        }
        Some("BlockDevice") => {
            if !path.exists() {
                anyhow::bail!("hostPath {} does not exist (type=BlockDevice)", host_path);
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileTypeExt;
                let metadata = std::fs::metadata(&path)
                    .with_context(|| format!("Failed to get metadata for {}", host_path))?;
                if !metadata.file_type().is_block_device() {
                    anyhow::bail!("hostPath {} is not a block device", host_path);
                }
            }
        }
        Some("") | None => {
            // No type check - just return the path as-is
        }
        Some(unknown) => {
            anyhow::bail!("Unsupported hostPath type: {}", unknown);
        }
    }

    Ok(host_path.to_string())
}

/// Cleans up volumes for a pod.
///
/// Async so recursive `remove_dir_all` on large emptyDir volumes does not
/// block the tokio runtime (HR2: the event loop must never block).
#[cfg(test)]
pub async fn cleanup_volumes(pod_name: &str) -> Result<()> {
    let volumes_root = volumes_root();
    let pod_volumes_path = format!("{}/{}/volumes", volumes_root, pod_name);
    // Best-effort unmount first to prevent recursive tmpfs stacking leaks
    // and remove_dir_all failures when mount points are still attached.
    unmount_volume_mounts_under(&pod_volumes_path).await?;
    crate::utils::remove_dir_all_if_exists_async(&pod_volumes_path)
        .await
        .with_context(|| format!("Failed to remove volumes at {}", pod_volumes_path))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serializes env-var mutation across the cleanup_volumes tests. Cargo
    /// runs `#[tokio::test]` cases in parallel; without this lock, two tests
    /// can race on KLIGHTS_DATA_ROOT and one observes the other's value.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Verifies cleanup_volumes is fully async — the tokio::time::timeout
    /// failsafe ensures the function never reverts to a sync recursive
    /// remove_dir_all that could block the runtime under load.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)] // ENV_LOCK serializes env-var-mutating tests; intentional
    async fn test_cleanup_volumes_removes_directory_async() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let temp = tempfile::tempdir().expect("tempdir");
        let test_ns = "rt01-cleanup";
        // Safety: serialized via ENV_LOCK so no concurrent reader observes
        // an inconsistent state.
        unsafe {
            std::env::set_var("KLIGHTS_DATA_ROOT", temp.path());
            std::env::set_var("KLIGHTS_CONTAINERD_NAMESPACE", test_ns);
        }

        // Build the directory tree cleanup_volumes expects:
        //   {data_root}/pods/{pod_id}/volumes/{kind}/{name}/file
        let pod_id = "default_my-pod";
        let pod_dir = std::path::PathBuf::from(volumes_root())
            .join(pod_id)
            .join("volumes");
        let nested = pod_dir.join("empty-dir").join("scratch");
        std::fs::create_dir_all(&nested).expect("create nested");
        std::fs::write(nested.join("a.txt"), b"hello").expect("write file");
        std::fs::write(nested.join("b.txt"), b"world").expect("write file");
        assert!(
            pod_dir.exists(),
            "setup: {} should exist",
            pod_dir.display()
        );

        // 1s timeout: catches any future regression that reintroduces a
        // blocking std::fs::remove_dir_all on the runtime worker.
        tokio::time::timeout(std::time::Duration::from_secs(1), cleanup_volumes(pod_id))
            .await
            .expect("cleanup_volumes did not complete within 1s — possible blocking regression")
            .expect("cleanup_volumes returned Err");

        assert!(
            !pod_dir.exists(),
            "expected pod volumes dir {} to be removed",
            pod_dir.display()
        );

        unsafe {
            std::env::remove_var("KLIGHTS_DATA_ROOT");
            std::env::remove_var("KLIGHTS_CONTAINERD_NAMESPACE");
        }
    }

    /// Idempotent: cleanup on a non-existent path returns Ok, not Err.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)] // ENV_LOCK serializes env-var-mutating tests; intentional
    async fn test_cleanup_volumes_missing_path_is_ok() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let temp = tempfile::tempdir().expect("tempdir");
        let test_ns = "rt01-missing";
        unsafe {
            std::env::set_var("KLIGHTS_DATA_ROOT", temp.path());
            std::env::set_var("KLIGHTS_CONTAINERD_NAMESPACE", test_ns);
        }

        let result = cleanup_volumes("nonexistent_pod").await;
        assert!(
            result.is_ok(),
            "expected Ok for missing path, got {:?}",
            result
        );

        unsafe {
            std::env::remove_var("KLIGHTS_DATA_ROOT");
            std::env::remove_var("KLIGHTS_CONTAINERD_NAMESPACE");
        }
    }

    #[test]
    fn test_collect_mount_targets_under_sorts_deepest_first_and_dedupes() {
        let root = format!(
            "{}/pods/default_pod/volumes",
            crate::paths::test_data_root_path("klights").display()
        );
        let mountinfo = format!(
            "10 1 0:1 / {root}/empty-dir/restart-count rw - tmpfs tmpfs rw\n\
             11 1 0:2 / {root}/empty-dir rw - tmpfs tmpfs rw\n\
             12 1 0:3 / {root}/empty-dir/restart-count rw - tmpfs tmpfs rw\n\
             13 1 0:4 / {root}/config-map/app rw - tmpfs tmpfs rw\n\
             14 1 0:5 / /unrelated rw - ext4 ext4 rw\n"
        );

        let targets = collect_mount_targets_under(&mountinfo, &root);
        assert_eq!(targets.len(), 3);
        assert_eq!(targets[2], format!("{root}/empty-dir"));
        assert!(targets.contains(&format!("{root}/empty-dir/restart-count")));
        assert!(targets.contains(&format!("{root}/config-map/app")));
    }

    #[test]
    fn test_collect_mount_targets_under_excludes_similar_prefixes() {
        let root = format!(
            "{}/pods/default_pod/volumes",
            crate::paths::test_data_root_path("klights").display()
        );
        let mountinfo = format!(
            "20 1 0:1 / {root} rw - tmpfs tmpfs rw\n\
             21 1 0:2 / {root}-backup rw - tmpfs tmpfs rw\n\
             22 1 0:3 / {root}/empty-dir/cache rw - tmpfs tmpfs rw\n"
        );

        let targets = collect_mount_targets_under(&mountinfo, &root);
        assert_eq!(
            targets,
            vec![format!("{root}/empty-dir/cache"), root.to_string()]
        );
    }

    #[test]
    fn test_collect_mount_targets_under_matches_canonical_symlink_root() {
        let temp = tempfile::tempdir().expect("tempdir");
        let canonical_root = temp
            .path()
            .join("data")
            .join("pods")
            .join("default_pod")
            .join("volumes");
        std::fs::create_dir_all(&canonical_root).expect("create canonical root");
        let symlink_parent = temp.path().join("links").join("pods").join("default_pod");
        std::fs::create_dir_all(&symlink_parent).expect("create symlink parent");
        let symlink_root = symlink_parent.join("volumes");
        std::os::unix::fs::symlink(&canonical_root, &symlink_root).expect("symlink root");

        let mountinfo = format!(
            "30 1 0:1 / {}/empty-dir/cache rw - tmpfs tmpfs rw\n",
            canonical_root.display()
        );

        let roots = mount_target_roots(
            symlink_root.to_str().unwrap(),
            Some(canonical_root.to_str().unwrap()),
        );
        let targets = collect_mount_targets_under_roots(&mountinfo, &roots);

        assert_eq!(
            targets,
            vec![format!("{}/empty-dir/cache", canonical_root.display())],
            "cleanup must find mounts even when the configured data root is a symlink"
        );
    }
}
