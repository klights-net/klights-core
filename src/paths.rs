use std::path::PathBuf;
#[cfg(test)]
use std::sync::OnceLock;

fn absolute_path(path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("/"))
            .join(path)
    }
}

fn env_path(name: &str, default: impl FnOnce() -> PathBuf) -> PathBuf {
    let path = std::env::var_os(name)
        .map(PathBuf::from)
        .unwrap_or_else(default);
    absolute_path(path)
}

fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

pub fn data_root_path(namespace: &str) -> PathBuf {
    env_path("KLIGHTS_DATA_ROOT", || home_dir().join(namespace))
}

pub fn db_root_path(namespace: &str) -> PathBuf {
    env_path("KLIGHTS_DB_DIR", || data_root_path(namespace).join("db"))
}

fn backend_db_dir_path(namespace: &str, backend: &str) -> PathBuf {
    db_root_path(namespace).join(backend)
}

pub fn cluster_db_path(namespace: &str, backend: &str) -> PathBuf {
    let dir = backend_db_dir_path(namespace, backend);
    match backend {
        "redb" => dir.join("cluster.redb"),
        _ => dir.join("cluster.db"),
    }
}

pub fn node_db_path(namespace: &str, backend: &str) -> PathBuf {
    let dir = backend_db_dir_path(namespace, backend);
    match backend {
        "redb" => dir.join("node.redb"),
        _ => dir.join("node.db"),
    }
}

pub fn runtime_namespace() -> String {
    std::env::var("KLIGHTS_CONTAINERD_NAMESPACE").unwrap_or_else(|_| "klights".to_string())
}

/// Per-process unique test root, stable across all tests in a single `cargo test`
/// run.  Avoids `/tmp` collisions when multiple users or CI workers share the
/// same host.
///
/// Pattern: `/tmp/{namespace}-test-{pid}-{nanos}`
#[cfg(test)]
fn test_random_token() -> &'static str {
    static TOKEN: OnceLock<String> = OnceLock::new();
    TOKEN.get_or_init(|| {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos();
        format!("{pid}-{nanos}")
    })
}

#[cfg(test)]
pub fn test_data_root_path(namespace: &str) -> PathBuf {
    PathBuf::from(format!("/tmp/{}-test-{}", namespace, test_random_token()))
}

pub fn etc_dir_path(namespace: &str) -> PathBuf {
    data_root_path(namespace).join("etc")
}

pub fn containerd_root_dir_path(namespace: &str) -> PathBuf {
    data_root_path(namespace).join("containerd")
}

pub fn containerd_data_dir_path(namespace: &str) -> PathBuf {
    containerd_root_dir_path(namespace).join("data")
}

pub fn containerd_state_dir_path(namespace: &str) -> PathBuf {
    containerd_root_dir_path(namespace).join("state")
}

pub fn containerd_socket_path(namespace: &str) -> PathBuf {
    data_root_path(namespace).join("containerd.sock")
}

pub fn containerd_hosts_dir_path(namespace: &str, pod_namespace: &str, pod_name: &str) -> PathBuf {
    containerd_root_dir_path(namespace)
        .join("hosts")
        .join(pod_namespace)
        .join(pod_name)
}

pub fn containerd_termination_log_path(
    namespace: &str,
    pod_namespace: &str,
    pod_name: &str,
    container_name: &str,
) -> PathBuf {
    containerd_root_dir_path(namespace)
        .join("termination")
        .join(pod_namespace)
        .join(pod_name)
        .join(container_name)
}

pub fn volumes_root_path(namespace: &str) -> PathBuf {
    data_root_path(namespace).join("pods")
}

pub fn local_path_provisioner_root_path(namespace: &str) -> PathBuf {
    data_root_path(namespace).join("local-path-provisioner")
}

pub fn pod_logs_root_path(namespace: &str) -> PathBuf {
    data_root_path(namespace).join("logs").join("pods")
}

pub fn pod_log_dir_path(
    namespace: &str,
    pod_namespace: &str,
    pod_name: &str,
    pod_uid: &str,
) -> PathBuf {
    pod_logs_root_path(namespace).join(format!("{}_{}_{}", pod_namespace, pod_name, pod_uid))
}

pub fn kubeconfig_path(namespace: &str) -> PathBuf {
    etc_dir_path(namespace).join("kubeconfig.yaml")
}

fn etc_file_path(namespace: &str, file_name: &str) -> PathBuf {
    etc_dir_path(namespace).join(file_name)
}

pub fn ca_cert_path(namespace: &str) -> PathBuf {
    etc_file_path(namespace, "ca.crt")
}

pub fn ca_key_path(namespace: &str) -> PathBuf {
    etc_file_path(namespace, "ca.key")
}

pub fn service_account_signing_key_path(namespace: &str) -> PathBuf {
    etc_file_path(namespace, "service-account-signing.key")
}

pub fn server_cert_path(namespace: &str) -> PathBuf {
    etc_file_path(namespace, "server.crt")
}

pub fn server_key_path(namespace: &str) -> PathBuf {
    etc_file_path(namespace, "server.key")
}

pub fn api_proxy_cert_path(namespace: &str) -> PathBuf {
    etc_file_path(namespace, "api-proxy.crt")
}

pub fn api_proxy_key_path(namespace: &str) -> PathBuf {
    etc_file_path(namespace, "api-proxy.key")
}

pub fn apiservice_proxy_cert_path(namespace: &str) -> PathBuf {
    etc_file_path(namespace, "apiservice-proxy.crt")
}

pub fn apiservice_proxy_key_path(namespace: &str) -> PathBuf {
    etc_file_path(namespace, "apiservice-proxy.key")
}

pub fn admin_cert_path(namespace: &str) -> PathBuf {
    etc_file_path(namespace, "admin.crt")
}

pub fn admin_key_path(namespace: &str) -> PathBuf {
    etc_file_path(namespace, "admin.key")
}

pub fn cni_conf_dir_path(namespace: &str) -> PathBuf {
    data_root_path(namespace)
        .join("cni")
        .join("net.d")
        .join(namespace)
}

pub fn cni_bin_dir_path(namespace: &str) -> PathBuf {
    data_root_path(namespace).join("cni").join("bin")
}

pub fn cni_rpc_socket_path(namespace: &str) -> PathBuf {
    data_root_path(namespace)
        .join("cni")
        .join("klights-cni.sock")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvVarGuard {
        name: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl EnvVarGuard {
        fn set(name: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(name);
            // TODO: Audit that the environment access only happens in single-threaded code.
            unsafe { std::env::set_var(name, value) };
            Self { name, previous }
        }

        fn remove(name: &'static str) -> Self {
            let previous = std::env::var_os(name);
            // TODO: Audit that the environment access only happens in single-threaded code.
            unsafe { std::env::remove_var(name) };
            Self { name, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(value) = &self.previous {
                // TODO: Audit that the environment access only happens in single-threaded code.
                unsafe { std::env::set_var(self.name, value) };
            } else {
                // TODO: Audit that the environment access only happens in single-threaded code.
                unsafe { std::env::remove_var(self.name) };
            }
        }
    }

    #[test]
    fn default_data_root_resolves_to_absolute_paths() {
        let _guard = ENV_LOCK.lock().unwrap();
        let _env = EnvVarGuard::remove("KLIGHTS_DATA_ROOT");

        let root = data_root_path("klights");

        assert!(
            root.is_absolute(),
            "default containerd root/state paths must never be relative"
        );
        assert!(
            root.ends_with("klights"),
            "default must be ~/{{namespace}}, got: {}",
            root.display()
        );
    }

    #[test]
    fn test_data_root_is_stable_within_process() {
        let a = test_data_root_path("klights");
        let b = test_data_root_path("klights");
        assert_eq!(
            a, b,
            "test_data_root_path must be deterministic per process"
        );
    }

    #[test]
    fn test_data_root_is_namespace_scoped() {
        let a = test_data_root_path("klights");
        let b = test_data_root_path("klights-dev");
        assert_ne!(a, b, "different namespaces must have different roots");
    }

    #[test]
    fn test_data_root_lives_under_tmp() {
        let r = test_data_root_path("klights");
        assert!(
            r.starts_with("/tmp/"),
            "test root must be under /tmp, got: {}",
            r.display()
        );
    }

    #[test]
    fn relative_data_root_env_resolves_to_absolute_paths() {
        let _guard = ENV_LOCK.lock().unwrap();
        let _env = EnvVarGuard::set("KLIGHTS_DATA_ROOT", "relative-klights-root");

        let root = data_root_path("klights");

        assert!(
            root.is_absolute(),
            "containerd root/state paths must never be relative"
        );
        assert!(root.ends_with("relative-klights-root"));
        assert_eq!(
            containerd_state_dir_path("klights"),
            root.join("containerd").join("state")
        );
    }

    #[test]
    fn sqlite_cluster_and_node_db_paths_are_separate_files_under_db_root() {
        let _guard = ENV_LOCK.lock().unwrap();
        let _env = EnvVarGuard::set("KLIGHTS_DATA_ROOT", "/var/lib/klights-test");
        let _db_env = EnvVarGuard::remove("KLIGHTS_DB_DIR");

        assert_eq!(
            cluster_db_path("klights", "sqlite"),
            PathBuf::from("/var/lib/klights-test/db/sqlite/cluster.db")
        );
        assert_eq!(
            node_db_path("klights", "sqlite"),
            PathBuf::from("/var/lib/klights-test/db/sqlite/node.db")
        );
    }

    #[test]
    fn redb_cluster_and_node_db_paths_are_separate_files_under_db_root() {
        let _guard = ENV_LOCK.lock().unwrap();
        let _env = EnvVarGuard::set("KLIGHTS_DB_DIR", "/var/lib/klights-db");
        let _data_env = EnvVarGuard::remove("KLIGHTS_DATA_ROOT");

        assert_eq!(
            cluster_db_path("klights", "redb"),
            PathBuf::from("/var/lib/klights-db/redb/cluster.redb")
        );
        assert_eq!(
            node_db_path("klights", "redb"),
            PathBuf::from("/var/lib/klights-db/redb/node.redb")
        );
    }
}
