pub mod cgroup_cleanup;
pub mod containerd_manager {
    pub use klights_kubelet::containerd_manager::*;
}
mod containerd_streaming;
pub mod context;
pub mod cri {
    pub use klights_kubelet::cri::*;
}
pub mod cri_events {
    pub use klights_kubelet::cri_events::*;
}
pub(crate) mod cri_exec;
pub mod file_blocking;
pub mod log_rotation {
    pub use klights_kubelet::log_rotation::*;
}
pub mod pod_cluster_runtime;
pub mod pod_container_config;
pub mod pod_creation_state {
    pub use klights_kubelet::pod_creation_state::*;
}
pub mod pod_dns {
    pub use klights_kubelet::pod_dns::*;
}
pub mod pod_endpoints;
pub mod pod_env;
pub mod pod_field_ref;
pub mod pod_fs;
pub mod pod_hosts {
    pub use klights_kubelet::pod_hosts::*;
}
pub mod pod_manager;
// pub mod pod_owner_reconcile; // removed — events flow top-down only
pub mod pod_repository;
pub mod pod_resources;
#[cfg(test)]
pub mod pod_runtime_state;
#[cfg(test)]
pub mod pod_sandbox {
    pub use klights_kubelet::pod_sandbox::*;
}
pub mod pod_sandbox_config {
    pub use klights_kubelet::pod_sandbox_config::*;
}
pub mod pod_service_envs;
pub mod pod_startup_error {
    pub use klights_kubelet::pod_startup_error::*;
}
pub mod pod_status_builders;
pub mod pod_status_logic {
    pub use klights_kubelet::pod_status_logic::*;
}
#[cfg(test)]
pub mod pod_status_test;
pub mod pod_status_writer;
pub mod pod_subsystem;
pub mod pod_termination;
pub mod pod_watch_handlers;
pub mod pod_watch_source;
#[cfg(test)]
mod probe_manager_integration;
pub mod reconciler;
pub mod registry_proxy {
    pub use klights_kubelet::registry_proxy::*;
}
pub(crate) mod remote_runtime;
pub mod rootless_runc_wrapper {
    pub use klights_kubelet::rootless_runc_wrapper::*;
}
pub mod runtime_paths {
    pub use klights_kubelet::runtime_paths::{KubeletRuntimePathError, KubeletRuntimePaths};

    #[cfg(test)]
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

pub use containerd_manager::ContainerdManager;
pub use cri::CriClient;
pub use klights_kubelet::probe_manager::ProbeManager;

pub mod pod_lifecycle_actor;
pub mod pod_lifecycle_core;
pub mod pod_lifecycle_router;
pub mod pod_lifecycle_service;
pub mod pod_runtime;
