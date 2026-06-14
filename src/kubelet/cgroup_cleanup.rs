use anyhow::{Context, Result};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

const CGROUP_ROOT: &str = "/sys/fs/cgroup";

fn validate_cgroup_segment(label: &str, value: &str) -> Result<String> {
    let value = value.trim();
    anyhow::ensure!(!value.is_empty(), "{label} must not be empty");
    anyhow::ensure!(value != "." && value != "..", "{label} must not be . or ..");
    anyhow::ensure!(
        !value.contains('/') && !value.contains('\\'),
        "{label} must be a single path segment"
    );
    anyhow::ensure!(
        !value.contains(".."),
        "{label} must not contain parent-directory traversal"
    );
    anyhow::ensure!(
        value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.')),
        "{label} contains unsupported cgroup path characters"
    );
    Ok(value.to_string())
}

pub fn pod_cgroup_relative_path(containerd_ns: &str, pod_uid: &str) -> Result<PathBuf> {
    let namespace = validate_cgroup_segment("containerd namespace", containerd_ns)?;
    let pod_uid = validate_cgroup_segment("pod uid", pod_uid)?;
    Ok(PathBuf::from(namespace)
        .join("besteffort")
        .join(format!("pod{pod_uid}")))
}

fn namespace_cgroup_relative_path(containerd_ns: &str) -> Result<PathBuf> {
    let namespace = validate_cgroup_segment("containerd namespace", containerd_ns)?;
    Ok(PathBuf::from(namespace))
}

pub async fn cleanup_pod_cgroup(containerd_ns: &str, pod_uid: &str) -> Result<usize> {
    let relative = pod_cgroup_relative_path(containerd_ns, pod_uid)?;
    cleanup_cgroup_tree(relative, "cleanup_pod_cgroup").await
}

pub async fn cleanup_namespace_cgroup_tree(containerd_ns: &str) -> Result<usize> {
    let relative = namespace_cgroup_relative_path(containerd_ns)?;
    cleanup_cgroup_tree(relative, "cleanup_namespace_cgroup_tree").await
}

pub async fn kill_namespace_cgroup_processes(
    containerd_ns: &str,
    task_supervisor: &crate::task_supervisor::TaskSupervisor,
) -> Result<usize> {
    let relative = namespace_cgroup_relative_path(containerd_ns)?;
    let path = PathBuf::from(CGROUP_ROOT).join(relative);
    let key = path.display().to_string();
    let mut pids = crate::kubelet::file_blocking::run_blocking_file_keyed(
        "kill_namespace_cgroup_processes_collect",
        key,
        move || collect_cgroup_pids(&path),
    )
    .await?;

    if pids.is_empty() {
        let namespace = containerd_ns.to_string();
        let fallback_key = namespace.clone();
        pids = crate::kubelet::file_blocking::run_blocking_file_keyed(
            "kill_namespace_cgroup_processes_fallback_collect",
            fallback_key,
            move || collect_namespace_cgroup_pids(&namespace),
        )
        .await?;
    }

    if pids.is_empty() {
        return Ok(0);
    }

    for pid in &pids {
        crate::kubelet::containerd_manager::send_signal(*pid, libc::SIGTERM);
    }
    crate::kubelet::containerd_manager::wait_for_pids_to_exit(
        &pids,
        std::time::Duration::from_secs(5),
        task_supervisor,
    )
    .await;

    let remaining: Vec<libc::pid_t> = pids
        .iter()
        .copied()
        .filter(|pid| crate::kubelet::containerd_manager::process_exists(*pid))
        .collect();
    for pid in &remaining {
        crate::kubelet::containerd_manager::send_signal(*pid, libc::SIGKILL);
    }
    if !remaining.is_empty() {
        crate::kubelet::containerd_manager::wait_for_pids_to_exit(
            &remaining,
            std::time::Duration::from_secs(2),
            task_supervisor,
        )
        .await;
    }

    Ok(pids.len())
}

async fn cleanup_cgroup_tree(relative: PathBuf, label: &'static str) -> Result<usize> {
    let path = PathBuf::from(CGROUP_ROOT).join(relative);
    let key = path.display().to_string();
    crate::kubelet::file_blocking::run_blocking_file_keyed(label, key, move || {
        remove_empty_cgroup_tree(&path)
    })
    .await
}

pub fn remove_empty_cgroup_tree(root: &Path) -> Result<usize> {
    if !root.exists() {
        return Ok(0);
    }

    let mut removed = 0usize;
    remove_empty_cgroup_tree_inner(root, &mut removed)
        .with_context(|| format!("failed to remove cgroup tree {}", root.display()))?;
    Ok(removed)
}

fn remove_empty_cgroup_tree_inner(path: &Path, removed: &mut usize) -> Result<()> {
    let entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).with_context(|| format!("failed to read {}", path.display())),
    };

    for entry in entries {
        let entry =
            entry.with_context(|| format!("failed to read entry under {}", path.display()))?;
        let file_type = entry
            .file_type()
            .with_context(|| format!("failed to read type for {}", entry.path().display()))?;
        if file_type.is_dir() {
            remove_empty_cgroup_tree_inner(&entry.path(), removed)?;
        }
    }

    match std::fs::remove_dir(path) {
        Ok(()) => {
            *removed += 1;
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("failed to rmdir {}", path.display())),
    }
}

fn collect_cgroup_pids(root: &Path) -> Result<Vec<libc::pid_t>> {
    if !root.exists() {
        return Ok(Vec::new());
    }

    let mut pids = BTreeSet::new();
    collect_cgroup_pids_inner(root, &mut pids)
        .with_context(|| format!("failed to collect cgroup pids under {}", root.display()))?;
    Ok(pids.into_iter().collect())
}

fn collect_cgroup_pids_inner(path: &Path, pids: &mut BTreeSet<libc::pid_t>) -> Result<()> {
    let procs_path = path.join("cgroup.procs");
    match std::fs::read_to_string(&procs_path) {
        Ok(contents) => {
            for line in contents.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let pid = trimmed
                    .parse::<libc::pid_t>()
                    .with_context(|| format!("invalid pid in {}", procs_path.display()))?;
                pids.insert(pid);
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(e).with_context(|| format!("failed to read {}", procs_path.display()));
        }
    }

    let entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).with_context(|| format!("failed to read {}", path.display())),
    };

    for entry in entries {
        let entry =
            entry.with_context(|| format!("failed to read entry under {}", path.display()))?;
        let file_type = entry
            .file_type()
            .with_context(|| format!("failed to read type for {}", entry.path().display()))?;
        if file_type.is_dir() {
            collect_cgroup_pids_inner(&entry.path(), pids)?;
        }
    }

    Ok(())
}

fn collect_namespace_cgroup_pids(containerd_ns: &str) -> Result<Vec<libc::pid_t>> {
    let mut pids = BTreeSet::new();
    let exact = format!("/{containerd_ns}");
    let prefix = format!("{exact}/");

    let proc_root = Path::new("/proc");
    let entries = match std::fs::read_dir(proc_root) {
        Ok(entries) => entries,
        Err(e) => return Err(e).context("read /proc for fallback namespace process discovery"),
    };

    for entry in entries {
        let Ok(entry) = entry else {
            continue;
        };
        let file_name = entry.file_name();
        let Some(pid) = file_name
            .to_str()
            .and_then(|name| name.parse::<libc::pid_t>().ok())
        else {
            continue;
        };

        let cgroup_path = entry.path().join("cgroup");
        let Ok(content) = std::fs::read_to_string(&cgroup_path) else {
            continue;
        };
        if cgroup_path_targets_namespace(&content, &exact, &prefix) {
            pids.insert(pid);
        }
    }

    Ok(pids.into_iter().collect())
}

fn cgroup_path_targets_namespace(content: &str, exact: &str, prefix: &str) -> bool {
    for line in content.lines() {
        let mut parts = line.splitn(3, ':');
        let _ = parts.next();
        let _ = parts.next();
        let path = match parts.next() {
            Some(path) => path.trim(),
            None => continue,
        };
        if path == exact || path.starts_with(prefix) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pod_cgroup_relative_path_rejects_traversal_segments() {
        assert!(pod_cgroup_relative_path("../klights", "pod-uid").is_err());
        assert!(pod_cgroup_relative_path("klights", "../pod-uid").is_err());
        assert!(pod_cgroup_relative_path("klights/test", "pod-uid").is_err());
        assert!(pod_cgroup_relative_path("klights", "pod/uid").is_err());
    }

    #[test]
    fn pod_cgroup_path_uses_containerd_namespace_and_uid() {
        let path = pod_cgroup_relative_path("klights-tester-1", "uid-123").unwrap();
        assert_eq!(
            path,
            std::path::PathBuf::from("klights-tester-1")
                .join("besteffort")
                .join("poduid-123")
        );
    }

    #[test]
    fn remove_empty_cgroup_tree_removes_container_children_before_pod() {
        let tmp = tempfile::tempdir().unwrap();
        let pod_dir = tmp
            .path()
            .join("klights")
            .join("besteffort")
            .join("poduid-1");
        std::fs::create_dir_all(pod_dir.join("sandbox-container")).unwrap();
        std::fs::create_dir_all(pod_dir.join("app-container")).unwrap();

        let removed = remove_empty_cgroup_tree(&pod_dir).unwrap();

        assert_eq!(removed, 3);
        assert!(!pod_dir.exists());
        assert!(tmp.path().join("klights").join("besteffort").exists());
    }

    #[test]
    fn collect_cgroup_pids_recurses_through_pod_and_container_dirs() {
        let tmp = tempfile::tempdir().unwrap();
        let pod_dir = tmp
            .path()
            .join("klights")
            .join("besteffort")
            .join("poduid-1");
        let sandbox_dir = pod_dir.join("sandbox-container");
        let app_dir = pod_dir.join("app-container");
        std::fs::create_dir_all(&sandbox_dir).unwrap();
        std::fs::create_dir_all(&app_dir).unwrap();
        std::fs::write(pod_dir.join("cgroup.procs"), "100\n").unwrap();
        std::fs::write(sandbox_dir.join("cgroup.procs"), "101\n102\n").unwrap();
        std::fs::write(app_dir.join("cgroup.procs"), "102\n103\n").unwrap();

        let pids = collect_cgroup_pids(&pod_dir).unwrap();

        assert_eq!(pids, vec![100, 101, 102, 103]);
    }

    #[test]
    fn cgroup_path_targets_namespace_matches_exact_or_nested_path() {
        assert!(cgroup_path_targets_namespace("0::/k3s\n", "/k3s", "/k3s/",));
        assert!(cgroup_path_targets_namespace(
            "0::/k3s/besteffort/pod123\n",
            "/k3s",
            "/k3s/",
        ));
    }

    #[test]
    fn cgroup_path_targets_namespace_rejects_partial_prefix() {
        assert!(!cgroup_path_targets_namespace(
            "0::/k3s-other/workload\n",
            "/k3s",
            "/k3s/",
        ));
    }
}
