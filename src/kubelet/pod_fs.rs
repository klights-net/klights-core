use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

pub struct PodFs;

impl PodFs {
    /// Write /etc/hosts content to a host directory (creates dir + `dir/hosts`).
    pub async fn write_hosts_file(dir: PathBuf, content: String) -> Result<String> {
        crate::kubelet::file_blocking::run_blocking_file_keyed(
            "podfs_write_hosts_file",
            dir.to_string_lossy().into_owned(),
            move || {
                std::fs::create_dir_all(&dir).context("Failed to create hosts directory")?;
                let path = dir.join("hosts");
                std::fs::write(&path, &content).context("Failed to write /etc/hosts file")?;
                Ok(path.to_string_lossy().into_owned())
            },
        )
        .await
    }

    /// Ensure termination log file exists with mode 0o666. Always returns the path.
    pub async fn ensure_termination_log(path: PathBuf) -> String {
        let ret = path.to_string_lossy().into_owned();
        // safe-to-ignore: missing termination log file is non-fatal; container still runs
        let key = ret.clone();
        let _ = crate::kubelet::file_blocking::run_blocking_file_keyed(
            "podfs_ensure_termination_log",
            key,
            move || {
                ensure_termination_log_sync(&path);
                Ok(())
            },
        )
        .await;
        ret
    }

    /// Create pod log directory structure. Returns path to `0.log`.
    pub async fn create_log_dir(container_log_dir: PathBuf) -> Result<String> {
        crate::kubelet::file_blocking::run_blocking_file_keyed(
            "podfs_create_log_dir",
            container_log_dir.to_string_lossy().into_owned(),
            move || {
                std::fs::create_dir_all(&container_log_dir).with_context(|| {
                    format!(
                        "Failed to create log directory {}",
                        container_log_dir.display()
                    )
                })?;
                Ok(container_log_dir
                    .join("0.log")
                    .to_string_lossy()
                    .into_owned())
            },
        )
        .await
    }

    /// Apply fsGroup ownership to volume paths. Logs warnings per-volume.
    pub async fn apply_fs_group(volume_paths: Vec<String>, gid: u32) {
        let _ =
            crate::kubelet::file_blocking::run_blocking_file("podfs_apply_fs_group", move || {
                for path in &volume_paths {
                    if let Err(e) = apply_fs_group_sync(Path::new(path), gid) {
                        tracing::warn!("Failed to apply fsGroup {} to {}: {}", gid, path, e);
                    }
                }
                Ok(())
            })
            .await;
    }

    /// Rotate logs for all pods under the given root.
    pub async fn rotate_logs(root: PathBuf, max_size: u64, max_files: usize) {
        let _ = crate::kubelet::file_blocking::run_blocking_file_keyed(
            "podfs_rotate_logs",
            root.to_string_lossy().into_owned(),
            move || {
                rotate_logs_sync(&root, max_size, max_files);
                Ok(())
            },
        )
        .await;
    }

    /// Ensure directories exist (for subPath materialization).
    #[cfg(test)]
    pub async fn ensure_dirs(dirs: Vec<PathBuf>) {
        if dirs.is_empty() {
            return;
        }
        let key = dirs
            .first()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|| "podfs_ensure_dirs".to_string());
        let _ = crate::kubelet::file_blocking::run_blocking_file_keyed(
            "podfs_ensure_dirs",
            key,
            move || {
                for dir in &dirs {
                    if let Err(e) = std::fs::create_dir_all(dir) {
                        tracing::warn!(
                            "Failed to create subPath directory {}: {}",
                            dir.display(),
                            e
                        );
                    }
                }
                Ok(())
            },
        )
        .await;
    }
}

fn ensure_termination_log_sync(path: &Path) {
    if let Some(parent) = path.parent()
        && let Err(e) = std::fs::create_dir_all(parent)
    {
        tracing::warn!(
            "Failed to create termination log directory {}: {}",
            parent.display(),
            e
        );
        return;
    }

    match std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(path)
    {
        Ok(file) => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                match file.metadata() {
                    Ok(meta) => {
                        let mut perms = meta.permissions();
                        perms.set_mode(0o666);
                        if let Err(e) = std::fs::set_permissions(path, perms) {
                            tracing::warn!(
                                "Failed to chmod termination log file {}: {}",
                                path.display(),
                                e
                            );
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to stat termination log file {}: {}",
                            path.display(),
                            e
                        );
                    }
                }
            }
        }
        Err(e) => {
            tracing::warn!(
                "Failed to create termination log file {}: {}",
                path.display(),
                e
            );
        }
    }
}

fn apply_fs_group_sync(path: &Path, gid: u32) -> anyhow::Result<()> {
    use std::os::unix::ffi::OsStrExt;

    fn chown_gid(path: &Path, gid: u32) -> anyhow::Result<()> {
        let c_path =
            std::ffi::CString::new(path.as_os_str().as_bytes()).context("path to CString")?;
        // SAFETY: lchown(2) reads the path string up to its NUL terminator and
        // takes integer uid/gid by value. `c_path` is a freshly-built
        // CString that lives until the end of this expression, so the pointer
        // is valid and properly NUL-terminated for the duration of the call.
        let ret = unsafe { libc::lchown(c_path.as_ptr(), u32::MAX, gid) };
        if ret != 0 {
            return Err(anyhow::anyhow!(
                "lchown {:?} gid={}: {}",
                path,
                gid,
                std::io::Error::last_os_error()
            ));
        }
        Ok(())
    }

    fn apply_recursive(path: &Path, gid: u32) -> anyhow::Result<()> {
        if !path.exists() {
            return Ok(());
        }
        chown_gid(path, gid)?;
        if path.is_dir() {
            for entry in std::fs::read_dir(path)? {
                apply_recursive(&entry?.path(), gid)?;
            }
        }
        Ok(())
    }

    apply_recursive(path, gid)
}

fn rotate_logs_sync(root: &Path, max_size: u64, max_files: usize) {
    use crate::kubelet::log_rotation::{RotationPlan, build_rotation_plan};

    if !root.exists() {
        return;
    }

    let pod_dirs = match std::fs::read_dir(root) {
        Ok(dirs) => dirs,
        Err(e) => {
            tracing::debug!("Failed to read log root directory: {:#}", e);
            return;
        }
    };

    for pod_dir in pod_dirs.flatten() {
        if !pod_dir.path().is_dir() {
            continue;
        }

        let container_dirs = match std::fs::read_dir(pod_dir.path()) {
            Ok(dirs) => dirs,
            Err(e) => {
                tracing::debug!("Failed to read pod directory {:?}: {:#}", pod_dir.path(), e);
                continue;
            }
        };

        for container_dir in container_dirs.flatten() {
            if !container_dir.path().is_dir() {
                continue;
            }

            let log_file = container_dir.path().join("0.log");
            let metadata = match std::fs::metadata(&log_file) {
                Ok(m) => m,
                Err(_) => continue,
            };
            let Some(RotationPlan {
                remove_oldest,
                renames,
                current_to_one,
            }) = build_rotation_plan(&log_file, metadata.len(), max_size, max_files)
            else {
                continue;
            };
            let _ = std::fs::remove_file(&remove_oldest);
            for (src, dst) in renames {
                if src.exists()
                    && let Err(e) = std::fs::rename(&src, &dst)
                {
                    tracing::warn!("Failed to rename {:?} -> {:?}: {:#}", src, dst, e);
                }
            }
            let (cur, dst) = current_to_one;
            if let Err(e) = std::fs::rename(&cur, &dst) {
                tracing::warn!("Failed to rotate {:?} -> {:?}: {:#}", cur, dst, e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_write_hosts_file_creates_dir_and_file() {
        let dir = tempdir().unwrap();
        let hosts_dir = dir.path().join("hosts_dir");
        let content = "127.0.0.1\tlocalhost\n".to_string();

        let path = PodFs::write_hosts_file(hosts_dir.clone(), content.clone())
            .await
            .unwrap();

        assert_eq!(path, hosts_dir.join("hosts").to_string_lossy());
        assert_eq!(crate::utils::read_utf8_file(&path).unwrap(), content);
    }

    #[tokio::test]
    async fn test_ensure_termination_log_creates_file_with_permissions() {
        let dir = tempdir().unwrap();
        let log_path = dir
            .path()
            .join("term_logs")
            .join("container")
            .join("termination-log");

        let result = PodFs::ensure_termination_log(log_path.clone()).await;

        assert_eq!(result, log_path.to_string_lossy());
        assert!(log_path.exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::metadata(&log_path).unwrap().permissions();
            assert_eq!(perms.mode() & 0o777, 0o666);
        }
    }

    #[tokio::test]
    async fn test_ensure_termination_log_returns_path_on_failure() {
        let path = PathBuf::from("/proc/nonexistent/deep/path/term-log");
        let result = PodFs::ensure_termination_log(path.clone()).await;
        assert_eq!(result, path.to_string_lossy());
    }

    #[tokio::test]
    async fn test_create_log_dir_creates_structure() {
        let dir = tempdir().unwrap();
        let container_log_dir = dir.path().join("ns_pod_uid").join("container0");

        let log_path = PodFs::create_log_dir(container_log_dir.clone())
            .await
            .unwrap();

        assert!(container_log_dir.exists());
        assert_eq!(log_path, container_log_dir.join("0.log").to_string_lossy());
    }

    #[tokio::test]
    async fn test_rotate_logs_rotates_large_files() {
        let dir = tempdir().unwrap();
        let pod_dir = dir.path().join("ns_pod_uid");
        let container_dir = pod_dir.join("container0");
        std::fs::create_dir_all(&container_dir).unwrap();
        let log_file = container_dir.join("0.log");
        std::fs::write(&log_file, vec![0u8; 11 * 1024 * 1024]).unwrap();

        PodFs::rotate_logs(dir.path().to_path_buf(), 10 * 1024 * 1024, 5).await;

        assert!(!log_file.exists());
        assert!(container_dir.join("0.1.log").exists());
    }

    #[tokio::test]
    async fn test_rotate_logs_skips_under_threshold_files() {
        let dir = tempdir().unwrap();
        let container_dir = dir.path().join("ns_pod_uid").join("container0");
        std::fs::create_dir_all(&container_dir).unwrap();
        let log_file = container_dir.join("0.log");
        std::fs::write(&log_file, vec![0u8; 1024]).unwrap();

        PodFs::rotate_logs(dir.path().to_path_buf(), 10 * 1024 * 1024, 5).await;

        assert!(log_file.exists(), "small file must not be rotated");
        assert!(!container_dir.join("0.1.log").exists());
    }

    #[tokio::test]
    async fn test_rotate_logs_chain_deletes_oldest_and_renames_others() {
        let dir = tempdir().unwrap();
        let container_dir = dir.path().join("ns_pod_uid").join("container0");
        std::fs::create_dir_all(&container_dir).unwrap();
        let log_file = container_dir.join("0.log");
        for i in 1..=4 {
            std::fs::write(container_dir.join(format!("0.{i}.log")), format!("log {i}")).unwrap();
        }
        std::fs::write(&log_file, vec![0u8; 11 * 1024 * 1024]).unwrap();

        PodFs::rotate_logs(dir.path().to_path_buf(), 10 * 1024 * 1024, 5).await;

        assert!(!log_file.exists());
        assert!(container_dir.join("0.1.log").exists());
        assert!(container_dir.join("0.2.log").exists());
        assert!(container_dir.join("0.3.log").exists());
        assert!(container_dir.join("0.4.log").exists());
        assert!(!container_dir.join("0.5.log").exists());
        // Oldest content was discarded; the new 0.4.log holds what was 0.3.log.
        let after = std::fs::read_to_string(container_dir.join("0.4.log")).unwrap();
        assert_eq!(after, "log 3");
    }

    #[tokio::test]
    async fn test_rotate_logs_handles_missing_log_file() {
        let dir = tempdir().unwrap();
        let container_dir = dir.path().join("ns_pod_uid").join("container0");
        std::fs::create_dir_all(&container_dir).unwrap();
        // No 0.log present.

        PodFs::rotate_logs(dir.path().to_path_buf(), 10 * 1024 * 1024, 5).await;

        assert!(!container_dir.join("0.1.log").exists());
    }

    #[tokio::test]
    async fn test_ensure_dirs_creates_directories() {
        let dir = tempdir().unwrap();
        let d1 = dir.path().join("a").join("b");
        let d2 = dir.path().join("c").join("d");

        PodFs::ensure_dirs(vec![d1.clone(), d2.clone()]).await;

        assert!(d1.exists());
        assert!(d2.exists());
    }

    #[tokio::test]
    async fn test_ensure_dirs_noop_on_empty() {
        PodFs::ensure_dirs(vec![]).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_apply_fs_group_does_not_block_worker() {
        let dir = tempdir().unwrap();
        let base = dir.path().join("volumes");
        std::fs::create_dir_all(&base).unwrap();
        for i in 0..50 {
            std::fs::write(base.join(format!("file_{}", i)), "data").unwrap();
        }

        let paths = vec![base.to_string_lossy().into_owned()];
        let (_, async_result) = tokio::join!(PodFs::apply_fs_group(paths, 0), async {
            tokio::task::yield_now().await;
            42
        });

        assert_eq!(async_result, 42);
    }
}
