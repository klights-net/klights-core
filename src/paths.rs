use std::path::PathBuf;
#[cfg(any(test, feature = "native-api-test-support"))]
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
    env_path_value(std::env::var_os(name), default)
}

fn env_path_value(value: Option<std::ffi::OsString>, default: impl FnOnce() -> PathBuf) -> PathBuf {
    let path = value.map(PathBuf::from).unwrap_or_else(default);
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
    backend_db_dir_under(db_root_path(namespace), backend)
}

fn backend_db_dir_under(db_root: PathBuf, backend: &str) -> PathBuf {
    db_root.join(backend)
}

pub fn cluster_db_path(namespace: &str, backend: &str) -> PathBuf {
    cluster_db_path_under(backend_db_dir_path(namespace, backend), backend)
}

fn cluster_db_path_under(dir: PathBuf, backend: &str) -> PathBuf {
    match backend {
        "redb" => dir.join("cluster.redb"),
        _ => dir.join("cluster.db"),
    }
}

pub fn node_db_path(namespace: &str, backend: &str) -> PathBuf {
    node_db_path_under(backend_db_dir_path(namespace, backend), backend)
}

fn node_db_path_under(dir: PathBuf, backend: &str) -> PathBuf {
    match backend {
        "redb" => dir.join("node.redb"),
        _ => dir.join("node.db"),
    }
}

pub fn runtime_namespace() -> String {
    std::env::var("KLIGHTS_CONTAINERD_NAMESPACE").unwrap_or_else(|_| "klights".to_string())
}

/// Per-process fallback token for direct `cargo test` invocations that do not
/// provide the build-owned `KLIGHTS_TEST_DATA_ROOT`.
#[cfg(any(test, feature = "native-api-test-support"))]
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

#[cfg(any(test, feature = "native-api-test-support"))]
fn test_run_root_path() -> PathBuf {
    std::env::var_os("KLIGHTS_TEST_DATA_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(format!("/tmp/klights-test-run-{}", test_random_token())))
}

#[cfg(any(test, feature = "native-api-test-support"))]
fn test_path_component(raw: &str) -> String {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    raw.hash(&mut hasher);
    let readable = raw
        .rsplit("::")
        .next()
        .unwrap_or(raw)
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '_'
            }
        })
        .take(48)
        .collect::<String>();
    format!("{readable}-{:016x}", hasher.finish())
}

#[cfg(any(test, feature = "native-api-test-support"))]
fn test_case_root_path() -> PathBuf {
    let identity = std::thread::current()
        .name()
        .map(str::to_string)
        .unwrap_or_else(|| format!("{:?}", std::thread::current().id()));
    test_run_root_path().join(test_path_component(&identity))
}

/// Stable path for a namespace within the current test case.
///
/// `build.sh` owns the run root and removes it on exit. The test case component
/// prevents parallel tests that reuse a runtime namespace from sharing files.
#[cfg(any(test, feature = "native-api-test-support"))]
pub fn test_data_root_path(namespace: &str) -> PathBuf {
    test_case_root_path().join(test_path_component(namespace))
}

/// Create a unique test-owned data root below the current run and test case.
///
/// The returned `TempDir` removes the complete fixture subtree on drop;
/// `build.sh` removes the run root as a final safeguard after the suite exits.
#[cfg(test)]
pub fn test_data_root_fixture(namespace: &str) -> tempfile::TempDir {
    if std::env::var_os("KLIGHTS_TEST_DATA_ROOT").is_none() {
        return tempfile::Builder::new()
            .prefix(&format!("klights-test-{}-", test_path_component(namespace)))
            .tempdir()
            .expect("create isolated direct-test data fixture");
    }
    let test_case_root = test_case_root_path();
    std::fs::create_dir_all(&test_case_root).expect("create per-test data root");
    tempfile::Builder::new()
        .prefix(&format!("{}-", test_path_component(namespace)))
        .tempdir_in(test_case_root)
        .expect("create isolated test data fixture")
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

    #[test]
    fn default_data_root_resolves_to_absolute_paths() {
        let root = env_path_value(None, || home_dir().join("klights"));

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
    fn test_data_root_is_test_case_scoped() {
        let first = std::thread::Builder::new()
            .name("paths::first_parallel_test".to_string())
            .spawn(|| test_data_root_path("shared-namespace"))
            .unwrap()
            .join()
            .unwrap();
        let second = std::thread::Builder::new()
            .name("paths::second_parallel_test".to_string())
            .spawn(|| test_data_root_path("shared-namespace"))
            .unwrap()
            .join()
            .unwrap();

        assert_ne!(
            first, second,
            "parallel tests using the same runtime namespace need isolated roots"
        );
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
    fn test_data_root_fixture_removes_its_subtree_on_drop() {
        let path;
        {
            let fixture = test_data_root_fixture("cleanup");
            path = fixture.path().to_path_buf();
            std::fs::write(path.join("owned-file"), b"fixture").unwrap();
            assert!(path.exists());
        }
        assert!(
            !path.exists(),
            "dropping a test data fixture must remove its complete subtree"
        );
    }

    #[test]
    fn relative_data_root_env_resolves_to_absolute_paths() {
        let root = env_path_value(Some("relative-klights-root".into()), || {
            PathBuf::from("unused")
        });

        assert!(
            root.is_absolute(),
            "containerd root/state paths must never be relative"
        );
        assert!(root.ends_with("relative-klights-root"));
    }

    #[test]
    fn sqlite_cluster_and_node_db_paths_are_separate_files_under_db_root() {
        let directory = backend_db_dir_under(PathBuf::from("/var/lib/klights-test/db"), "sqlite");

        assert_eq!(
            cluster_db_path_under(directory.clone(), "sqlite"),
            PathBuf::from("/var/lib/klights-test/db/sqlite/cluster.db")
        );
        assert_eq!(
            node_db_path_under(directory, "sqlite"),
            PathBuf::from("/var/lib/klights-test/db/sqlite/node.db")
        );
    }

    #[test]
    fn redb_cluster_and_node_db_paths_are_separate_files_under_db_root() {
        let directory = backend_db_dir_under(PathBuf::from("/var/lib/klights-db"), "redb");

        assert_eq!(
            cluster_db_path_under(directory.clone(), "redb"),
            PathBuf::from("/var/lib/klights-db/redb/cluster.redb")
        );
        assert_eq!(
            node_db_path_under(directory, "redb"),
            PathBuf::from("/var/lib/klights-db/redb/node.redb")
        );
    }
}
