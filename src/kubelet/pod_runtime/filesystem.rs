use std::sync::Arc;

use anyhow::Context;

use crate::kubelet::pod_runtime::service::PodRuntimeKey;
use crate::task_supervisor::TaskSupervisor;

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
    _supervisor: Arc<TaskSupervisor>,
    containerd_ns: String,
    _node_name: String,
}

impl RealPodFilesystem {
    pub fn new(supervisor: Arc<TaskSupervisor>, containerd_ns: String, node_name: String) -> Self {
        Self {
            _supervisor: supervisor,
            containerd_ns,
            _node_name: node_name,
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
        if crate::kubelet::pod_hosts::is_host_network(pod) {
            return Ok(());
        }

        let spec = pod.get("spec");
        let hostname = crate::kubelet::pod_hosts::resolve_hostname(spec.unwrap_or(pod), &key.name);
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

        let hosts_content = crate::kubelet::pod_hosts::build_etc_hosts(
            &hostname,
            pod_ip,
            subdomain,
            &key.namespace,
            host_aliases_ref,
        );
        let hosts_dir =
            crate::paths::containerd_hosts_dir_path(&self.containerd_ns, &key.namespace, &key.name);
        crate::kubelet::pod_fs::PodFs::write_hosts_file(hosts_dir, hosts_content).await?;
        Ok(())
    }

    async fn create_log_directory(&self, key: &PodRuntimeKey) -> anyhow::Result<()> {
        let log_dir = crate::paths::pod_log_dir_path(
            &self.containerd_ns,
            &key.namespace,
            &key.name,
            &key.uid,
        );
        crate::kubelet::pod_fs::PodFs::create_log_dir(log_dir).await?;
        Ok(())
    }

    async fn ensure_termination_log_file(
        &self,
        key: &PodRuntimeKey,
        container_name: &str,
    ) -> String {
        crate::kubelet::pod_termination::ensure_termination_log_host_file(
            &self.containerd_ns,
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
        let termination_path = crate::kubelet::pod_termination::termination_log_host_path(
            &self.containerd_ns,
            &key.namespace,
            &key.name,
            container_name,
        );
        let log_path = crate::kubelet::pod_termination::container_log_host_path(
            &self.containerd_ns,
            &key.namespace,
            &key.name,
            &key.uid,
            container_name,
        );
        crate::kubelet::pod_termination::read_termination_message_with_fallback_async(
            &termination_path,
            &log_path,
            policy,
            exit_code,
        )
        .await
    }

    async fn cleanup_cgroup(&self, key: &PodRuntimeKey) -> anyhow::Result<()> {
        crate::kubelet::cgroup_cleanup::cleanup_pod_cgroup(&self.containerd_ns, &key.uid).await?;
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
        let volume_root = crate::paths::volumes_root_path(&self.containerd_ns)
            .join(pod_dir_id)
            .join("volumes");
        crate::kubelet::pod_fs::PodFs::apply_fs_group(
            vec![volume_root.to_string_lossy().into_owned()],
            gid,
        )
        .await;
        Ok(())
    }

    async fn cleanup_pod_filesystem(&self, key: &PodRuntimeKey) -> anyhow::Result<()> {
        let pod_dir_id = key.volume_dir_id();
        let pod_root = crate::paths::volumes_root_path(&self.containerd_ns).join(&pod_dir_id);
        let pod_log_dir = crate::paths::pod_log_dir_path(
            &self.containerd_ns,
            &key.namespace,
            &key.name,
            &key.uid,
        );
        crate::utils::remove_dir_all_if_exists_async(&pod_root)
            .await
            .with_context(|| {
                format!("failed to remove pod filesystem dir {}", pod_root.display())
            })?;
        crate::utils::remove_dir_all_if_exists_async(&pod_log_dir)
            .await
            .with_context(|| format!("failed to remove pod log dir {}", pod_log_dir.display()))?;
        Ok(())
    }
}
