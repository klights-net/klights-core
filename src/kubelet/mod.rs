pub mod context;
pub mod file_blocking;
pub mod pod_cluster_runtime;
pub mod pod_endpoints;
pub mod pod_fs;
pub mod pod_manager;
// pub mod pod_owner_reconcile; // removed — events flow top-down only
pub mod pod_repository;
pub mod pod_status_writer;
pub mod pod_subsystem;
pub mod pod_termination;
pub mod pod_watch_handlers;
pub mod pod_watch_source;
#[cfg(test)]
mod probe_manager_integration;
pub mod reconciler;
#[cfg(test)]
pub mod runtime_paths {
    use klights_kubelet::runtime_paths::KubeletRuntimePaths;

    pub(crate) fn for_test(namespace: &str) -> KubeletRuntimePaths {
        use std::hash::{Hash, Hasher};

        let identity = std::thread::current()
            .name()
            .map(str::to_string)
            .unwrap_or_else(|| format!("{:?}", std::thread::current().id()));
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        identity.hash(&mut hasher);
        namespace.hash(&mut hasher);
        let run_root = std::env::var_os("KLIGHTS_TEST_DATA_ROOT")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        KubeletRuntimePaths::new(
            run_root
                .join("klights-kubelet-tests")
                .join(format!("{:016x}", hasher.finish())),
        )
        .expect("kubelet test runtime path must be absolute")
    }
}
#[cfg(test)]
mod volume_integration_tests;

pub mod pod_runtime;
