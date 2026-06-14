use std::sync::Arc;

use crate::kubelet::pod_repository::PodNetworkReader;
use crate::kubelet::pod_runtime::service::PodRuntimeKey;
use crate::kubelet::pod_runtime::store::PodRuntimeStore;

/// Pod network runtime port wrapping CNI assignment reads and CNI cleanup.
#[async_trait::async_trait]
pub trait PodNetworkRuntime: Send + Sync {
    /// Read the CNI network assignment for a sandbox.
    async fn read_assignment(
        &self,
        sandbox_id: &str,
        key: &PodRuntimeKey,
        host_network: bool,
    ) -> anyhow::Result<crate::kubelet::pod_repository::PodNetworkAssignment>;

    /// Release sandbox network resources. `key` is the audit witness;
    /// sandbox_id is the CNI argument. The implementation must reject
    /// the call if the runtime store's UID-keyed sandbox lookup does
    /// not return this sandbox_id.
    async fn release_sandbox_network(
        &self,
        key: &PodRuntimeKey,
        sandbox_id: &str,
    ) -> anyhow::Result<()>;
}

// --- Production adapter ---

/// Production network runtime adapter over Datapath + PodRepository.
pub struct RealPodNetworkRuntime {
    datapath: Arc<dyn crate::networking::Datapath>,
    repository: Arc<crate::kubelet::pod_repository::PodRepository>,
    store: Arc<dyn PodRuntimeStore>,
}

impl RealPodNetworkRuntime {
    pub fn new(
        datapath: Arc<dyn crate::networking::Datapath>,
        repository: Arc<crate::kubelet::pod_repository::PodRepository>,
        store: Arc<dyn PodRuntimeStore>,
    ) -> Self {
        Self {
            datapath,
            repository,
            store,
        }
    }
}

#[async_trait::async_trait]
impl PodNetworkRuntime for RealPodNetworkRuntime {
    async fn read_assignment(
        &self,
        sandbox_id: &str,
        key: &PodRuntimeKey,
        host_network: bool,
    ) -> anyhow::Result<crate::kubelet::pod_repository::PodNetworkAssignment> {
        self.repository
            .read_pod_network_assignment(
                sandbox_id,
                &key.namespace,
                &key.name,
                &key.uid,
                host_network,
            )
            .await
    }

    async fn release_sandbox_network(
        &self,
        key: &PodRuntimeKey,
        sandbox_id: &str,
    ) -> anyhow::Result<()> {
        match self.store.get_sandbox_id(key).await? {
            Some(recorded) if recorded == sandbox_id => {}
            Some(recorded) => {
                anyhow::bail!(
                    "sandbox UID mismatch for {}/{} uid {}: requested {}, recorded {}",
                    key.namespace,
                    key.name,
                    key.uid,
                    sandbox_id,
                    recorded
                );
            }
            None => {
                anyhow::bail!(
                    "sandbox UID mismatch for {}/{} uid {}: requested {}, no UID-qualified sandbox row",
                    key.namespace,
                    key.name,
                    key.uid,
                    sandbox_id
                );
            }
        }
        self.datapath.cni_del(sandbox_id).await
    }
}
