use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::kubelet::pod_runtime::service::PodRuntimeKey;
use crate::kubelet::volume_sources::VolumeSourceReader;
use crate::task_supervisor::TaskSupervisor;
use tokio_util::sync::CancellationToken;

/// Pod volume runtime port for processing and cleaning up volumes.
#[async_trait::async_trait]
pub trait PodVolumeRuntime: Send + Sync {
    /// Process (mount/setup) volumes for a pod.
    /// Returns a map of volume name → host path for use in container mount building.
    async fn process_volumes(
        &self,
        key: &PodRuntimeKey,
        pod: &serde_json::Value,
    ) -> anyhow::Result<HashMap<String, String>>;

    /// Clean up volumes for a terminated pod. Unmounts every mount under the
    /// pod's volumes dir and removes it. Derived entirely from `key`, so it
    /// runs unchanged on the orphan/cold-sandbox path that has no deleted-Pod
    /// snapshot.
    async fn cleanup_volumes(&self, key: &PodRuntimeKey) -> anyhow::Result<()>;
}

// --- Production adapter ---

/// Production volume runtime adapter delegating to [`PodVolumeManager`].
pub struct RealPodVolumeRuntime {
    sources: Arc<dyn VolumeSourceReader>,
    containerd_namespace: String,
    supervisor: Arc<TaskSupervisor>,
    projected_sa_refresh_cancellations: Arc<Mutex<HashMap<PodRuntimeKey, CancellationToken>>>,
}

impl RealPodVolumeRuntime {
    pub fn new(
        sources: Arc<dyn VolumeSourceReader>,
        containerd_namespace: String,
        supervisor: Arc<TaskSupervisor>,
    ) -> Self {
        Self {
            sources,
            containerd_namespace,
            supervisor,
            projected_sa_refresh_cancellations: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn cancel_projected_sa_refresh(&self, key: &PodRuntimeKey) {
        if let Some(cancel) = self
            .projected_sa_refresh_cancellations
            .lock()
            .expect("projected SA refresh cancellation map poisoned")
            .remove(key)
        {
            cancel.cancel();
        }
    }

    async fn schedule_projected_sa_refresh_if_needed(
        &self,
        key: &PodRuntimeKey,
        pod: &serde_json::Value,
    ) -> anyhow::Result<()> {
        if !crate::kubelet::projected_sa_token_refresh::pod_has_projected_service_account_tokens(
            pod,
        ) {
            self.cancel_projected_sa_refresh(key);
            return Ok(());
        }

        self.cancel_projected_sa_refresh(key);
        let cancel = self.supervisor.root_cancellation_token().child_token();
        self.projected_sa_refresh_cancellations
            .lock()
            .expect("projected SA refresh cancellation map poisoned")
            .insert(key.clone(), cancel.clone());
        let volumes_root = crate::paths::volumes_root_path(&self.containerd_namespace)
            .to_string_lossy()
            .into_owned();
        crate::kubelet::projected_sa_token_refresh::schedule_projected_service_account_token_refresh(
            crate::kubelet::projected_sa_token_refresh::ProjectedSaTokenRefreshRequest {
                sources: self.sources.clone(),
                volumes_root,
                key: key.clone(),
            },
            pod,
            self.supervisor.clone(),
            cancel,
        )
        .await
    }
}

#[async_trait::async_trait]
impl PodVolumeRuntime for RealPodVolumeRuntime {
    async fn process_volumes(
        &self,
        key: &PodRuntimeKey,
        pod: &serde_json::Value,
    ) -> anyhow::Result<HashMap<String, String>> {
        let pod_dir_id = format!("{}_{}", key.namespace, key.name);
        let manager = crate::kubelet::pod_volume_manager::PodVolumeManager::new(
            self.sources.as_ref(),
            &self.containerd_namespace,
        );
        let volume_paths = manager
            .process_volumes(&pod_dir_id, &key.name, &key.namespace, pod)
            .await?;
        self.schedule_projected_sa_refresh_if_needed(key, pod)
            .await?;
        Ok(volume_paths)
    }

    async fn cleanup_volumes(&self, key: &PodRuntimeKey) -> anyhow::Result<()> {
        self.cancel_projected_sa_refresh(key);
        let pod_dir_id = format!("{}_{}", key.namespace, key.name);
        let pod_volumes_dir = crate::paths::volumes_root_path(&self.containerd_namespace)
            .join(&pod_dir_id)
            .join("volumes");
        let pod_volumes_path = pod_volumes_dir.to_string_lossy().into_owned();
        crate::kubelet::volumes::unmount_volume_mounts_under(&pod_volumes_path).await?;
        crate::utils::remove_dir_all_if_exists_async(&pod_volumes_dir)
            .await
            .map(|_| ())
            .map_err(|e| {
                anyhow::anyhow!(
                    "failed to remove pod volume dir {}: {e}",
                    pod_volumes_dir.display()
                )
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn real_pod_volume_runtime_cleanup_removes_pod_volumes_directory() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|err| err.into_inner());
        let temp = tempfile::tempdir().expect("tempdir");
        let containerd_ns = "rt-volumes-test";
        unsafe {
            std::env::set_var("KLIGHTS_DATA_ROOT", temp.path());
            std::env::set_var("KLIGHTS_CONTAINERD_NAMESPACE", containerd_ns);
        }

        let runtime = RealPodVolumeRuntime::new(
            crate::kubelet::volume_sources::empty_volume_source_reader_for_tests(),
            containerd_ns.to_string(),
            Arc::new(crate::task_supervisor::TaskSupervisor::new(
                crate::task_supervisor::TaskCategoryConfig::default(),
            )),
        );
        let key = PodRuntimeKey {
            namespace: "default".to_string(),
            name: "web".to_string(),
            uid: "uid-1".to_string(),
        };
        let pod_volumes_dir = crate::paths::volumes_root_path(containerd_ns)
            .join(format!("{}_{}", key.namespace, key.name))
            .join("volumes");
        std::fs::create_dir_all(pod_volumes_dir.join("empty-dir")).expect("create volume dir");
        std::fs::write(pod_volumes_dir.join("empty-dir/file.txt"), b"test").expect("write file");

        runtime
            .cleanup_volumes(&key)
            .await
            .expect("cleanup volumes");

        assert!(
            !pod_volumes_dir.exists(),
            "volume directory should be removed"
        );

        unsafe {
            std::env::remove_var("KLIGHTS_DATA_ROOT");
            std::env::remove_var("KLIGHTS_CONTAINERD_NAMESPACE");
        }
    }
}
