use std::sync::Arc;

use anyhow::Context;

use crate::runtime_types::PodRuntimeKey;
use klights_supervisor::TaskSupervisor;

/// Pod filesystem port for hosts files, logs, cgroups, fsGroup, and cleanup.
#[async_trait::async_trait]
pub trait PodFilesystem: Send + Sync {
    /// Write /etc/hosts for the pod.
    async fn write_hosts(&self, key: &PodRuntimeKey, pod: &serde_json::Value)
    -> anyhow::Result<()>;

    /// Create log directories for the pod.
    async fn create_log_directory(&self, key: &PodRuntimeKey) -> anyhow::Result<()>;

    /// Ensure the host-side termination log exists and return its host path.
    async fn ensure_termination_log_file(
        &self,
        key: &PodRuntimeKey,
        container_name: &str,
    ) -> String;

    /// Read the container termination message, including K8s log fallback policy.
    async fn read_termination_message(
        &self,
        key: &PodRuntimeKey,
        container_name: &str,
        policy: &str,
        exit_code: i32,
    ) -> String;

    /// Clean up the pod cgroup tree. UID-keyed and idempotent — derives the
    /// cgroup path purely from `key.uid`, so it is safe to run on every stop
    /// path regardless of whether a sandbox could be resolved.
    async fn cleanup_cgroup(&self, key: &PodRuntimeKey) -> anyhow::Result<()>;

    /// Apply fsGroup to pod volumes.
    async fn apply_fs_group(
        &self,
        key: &PodRuntimeKey,
        pod: &serde_json::Value,
    ) -> anyhow::Result<()>;

    /// Blocking filesystem cleanup for a terminated pod.
    async fn cleanup_pod_filesystem(&self, key: &PodRuntimeKey) -> anyhow::Result<()>;
}

// --- Production adapter ---

/// Production filesystem adapter delegating to PodFs helpers.
pub struct RealPodFilesystem {
    file_process: klights_supervisor::FileProcessExecutor,
    containerd_ns: String,
    _node_name: String,
    paths: crate::runtime_paths::KubeletRuntimePaths,
}

impl RealPodFilesystem {
    pub fn new(
        supervisor: Arc<TaskSupervisor>,
        containerd_ns: String,
        node_name: String,
        paths: crate::runtime_paths::KubeletRuntimePaths,
    ) -> Self {
        Self {
            file_process: klights_supervisor::FileProcessExecutor::new(supervisor),
            containerd_ns,
            _node_name: node_name,
            paths,
        }
    }
}

#[async_trait::async_trait]
impl PodFilesystem for RealPodFilesystem {
    async fn write_hosts(
        &self,
        key: &PodRuntimeKey,
        pod: &serde_json::Value,
    ) -> anyhow::Result<()> {
        if crate::pod_hosts::is_host_network(pod) {
            return Ok(());
        }

        let spec = pod.get("spec");
        let hostname = crate::pod_hosts::resolve_hostname(spec.unwrap_or(pod), &key.name);
        let pod_ip = pod
            .pointer("/status/podIP")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let subdomain = spec
            .and_then(|s| s.get("subdomain"))
            .and_then(|v| v.as_str());
        let host_aliases: Option<Vec<serde_json::Value>> = spec
            .and_then(|s| s.get("hostAliases"))
            .and_then(|v| v.as_array())
            .cloned();
        let host_aliases_ref: Option<&Vec<serde_json::Value>> = host_aliases.as_ref();

        let hosts_content = crate::pod_hosts::build_etc_hosts(
            &hostname,
            pod_ip,
            subdomain,
            &key.namespace,
            host_aliases_ref,
        );
        let hosts_dir = self.paths.containerd_hosts_dir(&key.namespace, &key.name);
        crate::pod_fs::PodFs::write_hosts_file(&self.file_process, hosts_dir, hosts_content)
            .await?;
        Ok(())
    }

    async fn create_log_directory(&self, key: &PodRuntimeKey) -> anyhow::Result<()> {
        let log_dir = self.paths.pod_log_dir(&key.namespace, &key.name, &key.uid);
        crate::pod_fs::PodFs::create_log_dir(&self.file_process, log_dir).await?;
        Ok(())
    }

    async fn ensure_termination_log_file(
        &self,
        key: &PodRuntimeKey,
        container_name: &str,
    ) -> String {
        crate::pod_termination::ensure_termination_log_host_file(
            &self.file_process,
            &self.paths,
            &key.namespace,
            &key.name,
            container_name,
        )
        .await
    }

    async fn read_termination_message(
        &self,
        key: &PodRuntimeKey,
        container_name: &str,
        policy: &str,
        exit_code: i32,
    ) -> String {
        let termination_path = crate::pod_termination::termination_log_host_path(
            &self.paths,
            &key.namespace,
            &key.name,
            container_name,
        );
        let log_path = crate::pod_termination::container_log_host_path(
            &self.paths,
            &key.namespace,
            &key.name,
            &key.uid,
            container_name,
        );
        crate::pod_termination::read_termination_message_with_fallback_async(
            &self.file_process,
            &termination_path,
            &log_path,
            policy,
            exit_code,
        )
        .await
    }

    async fn cleanup_cgroup(&self, key: &PodRuntimeKey) -> anyhow::Result<()> {
        crate::cgroup_cleanup::cleanup_pod_cgroup(
            &self.file_process,
            &self.containerd_ns,
            &key.uid,
        )
        .await?;
        Ok(())
    }

    async fn apply_fs_group(
        &self,
        key: &PodRuntimeKey,
        pod: &serde_json::Value,
    ) -> anyhow::Result<()> {
        let Some(fs_group) = pod
            .pointer("/spec/securityContext/fsGroup")
            .and_then(|v| v.as_u64())
        else {
            return Ok(());
        };
        let gid = u32::try_from(fs_group).context("pod fsGroup exceeds gid range")?;
        let pod_dir_id = key.volume_dir_id();
        let volume_root = self.paths.volumes_root().join(pod_dir_id).join("volumes");
        crate::pod_fs::PodFs::apply_fs_group(
            &self.file_process,
            vec![volume_root.to_string_lossy().into_owned()],
            gid,
        )
        .await;
        Ok(())
    }

    async fn cleanup_pod_filesystem(&self, key: &PodRuntimeKey) -> anyhow::Result<()> {
        let pod_dir_id = key.volume_dir_id();
        let pod_root = self.paths.volumes_root().join(&pod_dir_id);
        let pod_log_dir = self.paths.pod_log_dir(&key.namespace, &key.name, &key.uid);
        klights_supervisor::runtime_fs::remove_dir_all_if_exists_async(
            &self.file_process,
            &pod_root,
        )
        .await
        .with_context(|| format!("failed to remove pod filesystem dir {}", pod_root.display()))?;
        klights_supervisor::runtime_fs::remove_dir_all_if_exists_async(
            &self.file_process,
            &pod_log_dir,
        )
        .await
        .with_context(|| format!("failed to remove pod log dir {}", pod_log_dir.display()))?;
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::HashMap;
    use std::sync::Mutex;

    // --- MockPodFilesystem ---

    pub(crate) struct MockPodFilesystem {
        calls: Mutex<Vec<String>>,
        termination_messages: Mutex<HashMap<String, String>>,
    }

    impl Default for MockPodFilesystem {
        fn default() -> Self {
            Self::new()
        }
    }

    impl MockPodFilesystem {
        pub(crate) fn new() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                termination_messages: Mutex::new(HashMap::new()),
            }
        }

        #[allow(dead_code)]
        pub(crate) fn clear_calls(&self) {
            self.calls.lock().unwrap().clear();
        }

        pub(crate) fn recorded_calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }

        pub(crate) fn set_termination_message(
            &self,
            key: &crate::runtime_types::PodRuntimeKey,
            container_name: &str,
            message: &str,
        ) {
            self.termination_messages.lock().unwrap().insert(
                Self::termination_key(key, container_name),
                message.to_string(),
            );
        }

        fn termination_key(
            key: &crate::runtime_types::PodRuntimeKey,
            container_name: &str,
        ) -> String {
            format!(
                "{}/{}/{}/{}",
                key.namespace, key.name, key.uid, container_name
            )
        }
    }

    #[async_trait::async_trait]
    impl crate::runtime::filesystem::PodFilesystem for MockPodFilesystem {
        async fn write_hosts(
            &self,
            key: &crate::runtime_types::PodRuntimeKey,
            _pod: &serde_json::Value,
        ) -> anyhow::Result<()> {
            self.calls.lock().unwrap().push(format!(
                "write_hosts:{}/{}/{}",
                key.namespace, key.name, key.uid
            ));
            Ok(())
        }

        async fn create_log_directory(
            &self,
            key: &crate::runtime_types::PodRuntimeKey,
        ) -> anyhow::Result<()> {
            self.calls.lock().unwrap().push(format!(
                "create_log:{}/{}/{}",
                key.namespace, key.name, key.uid
            ));
            Ok(())
        }

        async fn ensure_termination_log_file(
            &self,
            key: &crate::runtime_types::PodRuntimeKey,
            container_name: &str,
        ) -> String {
            self.calls.lock().unwrap().push(format!(
                "ensure_termination_log:{}/{}/{}/{}",
                key.namespace, key.name, key.uid, container_name
            ));
            format!(
                "mock://termination/{}/{}/{}/{}",
                key.namespace, key.name, key.uid, container_name
            )
        }

        async fn read_termination_message(
            &self,
            key: &crate::runtime_types::PodRuntimeKey,
            container_name: &str,
            policy: &str,
            exit_code: i32,
        ) -> String {
            self.calls.lock().unwrap().push(format!(
                "read_termination_message:{}/{}/{}/{}:{}:{}",
                key.namespace, key.name, key.uid, container_name, policy, exit_code
            ));
            self.termination_messages
                .lock()
                .unwrap()
                .get(&Self::termination_key(key, container_name))
                .cloned()
                .unwrap_or_default()
        }

        async fn cleanup_cgroup(
            &self,
            key: &crate::runtime_types::PodRuntimeKey,
        ) -> anyhow::Result<()> {
            self.calls.lock().unwrap().push(format!(
                "cleanup_cgroup:{}/{}/{}",
                key.namespace, key.name, key.uid
            ));
            Ok(())
        }

        async fn apply_fs_group(
            &self,
            key: &crate::runtime_types::PodRuntimeKey,
            _pod: &serde_json::Value,
        ) -> anyhow::Result<()> {
            self.calls.lock().unwrap().push(format!(
                "apply_fs_group:{}/{}/{}",
                key.namespace, key.name, key.uid
            ));
            Ok(())
        }

        async fn cleanup_pod_filesystem(
            &self,
            key: &crate::runtime_types::PodRuntimeKey,
        ) -> anyhow::Result<()> {
            self.calls.lock().unwrap().push(format!(
                "cleanup_fs:{}/{}/{}",
                key.namespace, key.name, key.uid
            ));
            Ok(())
        }
    }
}
