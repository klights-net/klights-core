//! PID file management for klights daemon lifecycle.
//!
//! Writes a PID file at `{data_root}/klights.pid` on `start` so `stop` and
//! `cleanup` can discover and signal the running daemon.
//!
//! The PID file is removed on soft shutdown (stop); `cleanup` refuses to run
//! while a daemon is still alive.

use std::path::{Path, PathBuf};

/// Write the current process PID to the pidfile.
pub fn write(pid_path: &Path) -> std::io::Result<()> {
    let pid = std::process::id();
    if let Some(parent) = pid_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(pid_path, format!("{}", pid))
}

/// Read the PID from the pidfile, returning `None` if the file doesn't exist
/// or contains garbage.
pub fn read(pid_path: &Path) -> Option<u32> {
    let contents = std::fs::read_to_string(pid_path).ok()?;
    contents.trim().parse().ok()
}

/// Remove the pidfile.
pub fn remove(pid_path: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(pid_path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Check whether the process identified by the pidfile is still running
/// AND is a klights binary (heuristic: reads `/proc/<pid>/comm`).
pub fn is_running(pid_path: &Path) -> bool {
    let Some(pid) = read(pid_path) else {
        return false;
    };
    is_pid_running(pid)
}

/// Check if a given PID is alive and is a klights process.
fn is_pid_running(pid: u32) -> bool {
    let comm_path = format!("/proc/{}/comm", pid);
    match std::fs::read_to_string(&comm_path) {
        Ok(comm) => comm.trim() == "klights",
        Err(_) => false,
    }
}

/// Return the default PID file path for a given namespace.
pub fn default_pid_path(namespace: &str) -> PathBuf {
    crate::paths::data_root_path(namespace).join("klights.pid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_and_read_pid() {
        let dir = crate::paths::test_data_root_path("pidfile-test");
        std::fs::create_dir_all(&dir).unwrap();
        let pid_path = dir.join("klights.pid");

        write(&pid_path).unwrap();
        let pid = read(&pid_path).unwrap();
        assert_eq!(pid, std::process::id());
    }

    #[test]
    fn read_returns_none_for_missing_file() {
        let dir = crate::paths::test_data_root_path("pidfile-test-missing");
        let pid_path = dir.join("klights.pid");
        assert_eq!(read(&pid_path), None);
    }

    #[test]
    fn read_returns_none_for_garbage() {
        let dir = crate::paths::test_data_root_path("pidfile-test-garbage");
        std::fs::create_dir_all(&dir).unwrap();
        let pid_path = dir.join("klights.pid");
        std::fs::write(&pid_path, "not-a-pid").unwrap();
        assert_eq!(read(&pid_path), None);
    }

    #[test]
    fn remove_cleans_up_file() {
        let dir = crate::paths::test_data_root_path("pidfile-test-remove");
        std::fs::create_dir_all(&dir).unwrap();
        let pid_path = dir.join("klights.pid");
        write(&pid_path).unwrap();
        assert!(pid_path.exists());
        remove(&pid_path).unwrap();
        assert!(!pid_path.exists());
    }

    #[test]
    fn remove_nonexistent_is_ok() {
        let dir = crate::paths::test_data_root_path("pidfile-test-remove-nonexistent");
        let pid_path = dir.join("klights.pid");
        remove(&pid_path).unwrap(); // should not panic
    }

    #[test]
    fn is_running_returns_false_for_live_non_klights_pid() {
        // Our own test process is alive but its /proc/<pid>/comm is not "klights",
        // so is_running should return false.
        let dir = crate::paths::test_data_root_path("pidfile-test-running");
        std::fs::create_dir_all(&dir).unwrap();
        let pid_path = dir.join("klights.pid");
        std::fs::write(&pid_path, format!("{}", std::process::id())).unwrap();
        assert!(!is_running(&pid_path));
    }

    #[test]
    fn is_running_returns_false_for_dead_pid() {
        let dir = crate::paths::test_data_root_path("pidfile-test-dead");
        std::fs::create_dir_all(&dir).unwrap();
        let pid_path = dir.join("klights.pid");
        // Write a PID that almost certainly doesn't exist
        std::fs::write(&pid_path, "99999999").unwrap();
        assert!(!is_running(&pid_path));
    }

    #[test]
    fn is_running_returns_false_for_missing_file() {
        let dir = crate::paths::test_data_root_path("pidfile-test-missing-file");
        let pid_path = dir.join("klights.pid");
        assert!(!is_running(&pid_path));
    }

    #[test]
    fn is_running_rejects_non_klights_process() {
        let dir = crate::paths::test_data_root_path("pidfile-test-non-klights");
        std::fs::create_dir_all(&dir).unwrap();
        let pid_path = dir.join("klights.pid");
        // PID 1 is almost always systemd/init, not klights
        std::fs::write(&pid_path, "1").unwrap();
        assert!(!is_running(&pid_path));
    }

    #[test]
    fn default_pid_path_is_under_data_root() {
        let path = default_pid_path("klights");
        assert!(path.ends_with("klights.pid"));
        assert!(path.to_string_lossy().contains("klights"));
    }
}
