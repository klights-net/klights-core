use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Immutable node-local path layout resolved once by root construction.
///
/// Kubelet consumers derive child paths from this already-absolute root and
/// never inspect environment variables or the process working directory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KubeletRuntimePaths {
    data_root: Arc<PathBuf>,
}

impl KubeletRuntimePaths {
    pub fn new(data_root: PathBuf) -> Result<Self, KubeletRuntimePathError> {
        if !data_root.is_absolute() {
            return Err(KubeletRuntimePathError::NotAbsolute(data_root));
        }
        Ok(Self {
            data_root: Arc::new(data_root),
        })
    }

    pub fn data_root(&self) -> &Path {
        self.data_root.as_path()
    }

    pub fn etc_dir(&self) -> PathBuf {
        self.data_root.join("etc")
    }

    pub(crate) fn service_account_signing_key(&self) -> PathBuf {
        self.etc_dir().join("service-account-signing.key")
    }

    pub fn containerd_root(&self) -> PathBuf {
        self.data_root.join("containerd")
    }

    pub fn containerd_data_dir(&self) -> PathBuf {
        self.containerd_root().join("data")
    }

    pub fn containerd_state_dir(&self) -> PathBuf {
        self.containerd_root().join("state")
    }

    pub fn containerd_socket(&self) -> PathBuf {
        self.data_root.join("containerd.sock")
    }

    pub fn containerd_hosts_dir(&self, namespace: &str, pod_name: &str) -> PathBuf {
        self.containerd_root()
            .join("hosts")
            .join(namespace)
            .join(pod_name)
    }

    pub fn containerd_termination_log(
        &self,
        namespace: &str,
        pod_name: &str,
        container_name: &str,
    ) -> PathBuf {
        self.containerd_root()
            .join("termination")
            .join(namespace)
            .join(pod_name)
            .join(container_name)
    }

    pub fn volumes_root(&self) -> PathBuf {
        self.data_root.join("pods")
    }

    pub fn pod_logs_root(&self) -> PathBuf {
        self.data_root.join("logs").join("pods")
    }

    pub fn pod_log_dir(&self, namespace: &str, pod_name: &str, pod_uid: &str) -> PathBuf {
        self.pod_logs_root()
            .join(format!("{namespace}_{pod_name}_{pod_uid}"))
    }

    pub fn cni_bin_dir(&self) -> PathBuf {
        self.data_root.join("cni").join("bin")
    }

    pub fn cni_conf_dir(&self, runtime_namespace: &str) -> PathBuf {
        self.data_root
            .join("cni")
            .join("net.d")
            .join(runtime_namespace)
    }

    pub fn cni_rpc_socket(&self) -> PathBuf {
        self.data_root.join("cni").join("klights-cni.sock")
    }

    #[cfg(test)]
    pub(crate) fn for_test(namespace: &str) -> Self {
        use std::hash::{Hash, Hasher};

        let identity = std::thread::current()
            .name()
            .map(str::to_string)
            .unwrap_or_else(|| format!("{:?}", std::thread::current().id()));
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        identity.hash(&mut hasher);
        namespace.hash(&mut hasher);
        let run_root = std::env::var_os("KLIGHTS_TEST_DATA_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        Self::new(
            run_root
                .join("klights-kubelet-tests")
                .join(format!("{:016x}", hasher.finish())),
        )
        .expect("kubelet test runtime path must be absolute")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KubeletRuntimePathError {
    NotAbsolute(PathBuf),
}

impl std::fmt::Display for KubeletRuntimePathError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotAbsolute(path) => {
                write!(
                    formatter,
                    "kubelet data root must be absolute: {}",
                    path.display()
                )
            }
        }
    }
}

impl std::error::Error for KubeletRuntimePathError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_all_runtime_paths_from_the_injected_root() {
        let paths = KubeletRuntimePaths::new(PathBuf::from("/srv/klights")).unwrap();
        assert_eq!(
            paths.containerd_socket(),
            PathBuf::from("/srv/klights/containerd.sock")
        );
        assert_eq!(paths.volumes_root(), PathBuf::from("/srv/klights/pods"));
        assert_eq!(
            paths.service_account_signing_key(),
            PathBuf::from("/srv/klights/etc/service-account-signing.key")
        );
        assert_eq!(
            paths.pod_log_dir("default", "web", "uid-a"),
            PathBuf::from("/srv/klights/logs/pods/default_web_uid-a")
        );
    }

    #[test]
    fn rejects_relative_roots() {
        assert!(matches!(
            KubeletRuntimePaths::new(PathBuf::from("relative")),
            Err(KubeletRuntimePathError::NotAbsolute(_))
        ));
    }

    #[test]
    fn clones_share_one_validated_root_instance() {
        let paths = KubeletRuntimePaths::new(PathBuf::from("/srv/klights")).unwrap();
        let clone = paths.clone();

        assert!(Arc::ptr_eq(&paths.data_root, &clone.data_root));
        assert_eq!(paths.containerd_data_dir(), clone.containerd_data_dir());
        assert_eq!(paths.volumes_root(), clone.volumes_root());
    }

    #[test]
    fn test_fixture_is_stable_per_case_and_namespace() {
        let first = KubeletRuntimePaths::for_test("same");
        let second = KubeletRuntimePaths::for_test("same");
        let other = KubeletRuntimePaths::for_test("other");

        assert_eq!(first.data_root(), second.data_root());
        assert_ne!(first.data_root(), other.data_root());
    }
}
