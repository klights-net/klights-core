pub(crate) fn file_process_executor() -> klights_supervisor::FileProcessExecutor {
    klights_supervisor::FileProcessExecutor::new(std::sync::Arc::new(
        klights_supervisor::TaskSupervisor::new(klights_supervisor::TaskCategoryConfig::default()),
    ))
}

pub(crate) fn runtime_paths_for_test(namespace: &str) -> crate::runtime_paths::KubeletRuntimePaths {
    use std::hash::{Hash, Hasher};

    let identity = std::thread::current()
        .name()
        .map(str::to_string)
        .unwrap_or_else(|| format!("{:?}", std::thread::current().id()));
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    identity.hash(&mut hasher);
    namespace.hash(&mut hasher);
    let run_root = std::path::PathBuf::from("/tmp/klights")
        .join("klights-kubelet-tests")
        .join(std::process::id().to_string());
    crate::runtime_paths::KubeletRuntimePaths::new(
        run_root.join(format!("{:016x}", hasher.finish())),
    )
    .expect("kubelet test runtime path must be absolute")
}
