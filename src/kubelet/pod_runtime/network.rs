use std::sync::Arc;

use crate::kubelet::pod_runtime::service::PodRuntimeKey;
use crate::kubelet::pod_runtime::store::PodRuntimeStore;
use klights_kubelet::pod_repository::{
    PodNetworkAssignment, PodNetworkAssignmentError, PodNetworkAssignmentQuery,
    PodNetworkAssignmentRequest,
};

/// Pod network runtime port wrapping CNI assignment reads and CNI cleanup.
#[async_trait::async_trait]
pub trait PodNetworkRuntime: Send + Sync {
    /// Read the CNI network assignment for a sandbox.
    async fn read_assignment(
        &self,
        sandbox_id: &str,
        key: &PodRuntimeKey,
        host_network: bool,
    ) -> anyhow::Result<PodNetworkAssignment>;

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
    datapath: Arc<dyn klights_network_api::Datapath>,
    repository: Arc<dyn PodNetworkAssignmentQuery>,
    store: Arc<dyn PodRuntimeStore>,
}

impl RealPodNetworkRuntime {
    pub fn new(
        datapath: Arc<dyn klights_network_api::Datapath>,
        repository: Arc<dyn PodNetworkAssignmentQuery>,
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
    ) -> anyhow::Result<PodNetworkAssignment> {
        self.repository
            .read_pod_network_assignment(PodNetworkAssignmentRequest::try_new(
                sandbox_id,
                klights_types::PodIdentity::new(&key.namespace, &key.name, &key.uid),
                host_network,
            )?)
            .await
            .map_err(|error| match error {
                PodNetworkAssignmentError::TimedOut(_) => anyhow::Error::new(
                    klights_kubelet::pod_startup_error::PodStartupErrorKind::NetworkAssignmentTimedOut,
                )
                .context(error.to_string()),
                other => anyhow::Error::new(other),
            })
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
        let sandbox_id = klights_network_api::SandboxId::try_new(sandbox_id)?;
        self.datapath.cni_del(&sandbox_id).await?;
        Ok(())
    }
}

#[cfg(test)]
mod focused_datapath_tests {
    use super::{PodRuntimeStore, RealPodNetworkRuntime};
    use std::sync::Arc;

    #[test]
    fn test_kubelet_caller_takes_only_datapath() {
        type Constructor = fn(
            Arc<dyn klights_network_api::Datapath>,
            Arc<dyn klights_kubelet::pod_repository::PodNetworkAssignmentQuery>,
            Arc<dyn PodRuntimeStore>,
        ) -> RealPodNetworkRuntime;
        let _constructor: Constructor = RealPodNetworkRuntime::new;
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::Mutex;

    // --- MockPodNetworkRuntime ---

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) enum MockNetworkOp {
        ReadAssignment {
            sandbox_id: String,
            namespace: String,
            name: String,
            uid: String,
            host_network: bool,
        },
        ReleaseSandboxNetwork {
            namespace: String,
            name: String,
            uid: String,
            sandbox_id: String,
        },
    }

    pub(crate) struct MockPodNetworkRuntime {
        calls: Mutex<Vec<MockNetworkOp>>,
        fail: Mutex<Option<String>>,
    }

    impl Default for MockPodNetworkRuntime {
        fn default() -> Self {
            Self::new()
        }
    }

    impl MockPodNetworkRuntime {
        pub(crate) fn new() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                fail: Mutex::new(None),
            }
        }

        pub(crate) fn set_network_assignment_timeout(&self) {
            *self.fail.lock().unwrap() = Some("network_assignment_timeout".to_string());
        }

        pub(crate) fn clear_calls(&self) {
            self.calls.lock().unwrap().clear();
        }

        pub(crate) fn recorded_calls(&self) -> Vec<MockNetworkOp> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl crate::kubelet::pod_runtime::network::PodNetworkRuntime for MockPodNetworkRuntime {
        async fn read_assignment(
            &self,
            sandbox_id: &str,
            key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
            host_network: bool,
        ) -> anyhow::Result<klights_kubelet::pod_repository::PodNetworkAssignment> {
            if let Some(ref f) = *self.fail.lock().unwrap() {
                if f == "read_assignment" {
                    anyhow::bail!("injected failure");
                }
                if f == "network_assignment_timeout" {
                    return Err(anyhow::Error::new(
                        klights_kubelet::pod_startup_error::PodStartupErrorKind::NetworkAssignmentTimedOut,
                    )
                    .context("pod network assignment wait timed out for sandbox"));
                }
            }
            self.calls
                .lock()
                .unwrap()
                .push(MockNetworkOp::ReadAssignment {
                    sandbox_id: sandbox_id.to_string(),
                    namespace: key.namespace.clone(),
                    name: key.name.clone(),
                    uid: key.uid.clone(),
                    host_network,
                });
            Ok(klights_kubelet::pod_repository::PodNetworkAssignment {
                pod_ip: "10.0.0.1".to_string(),
                host_ip: "192.168.1.1".to_string(),
            })
        }

        async fn release_sandbox_network(
            &self,
            key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
            sandbox_id: &str,
        ) -> anyhow::Result<()> {
            if let Some(ref f) = *self.fail.lock().unwrap()
                && f == "release_sandbox_network"
            {
                anyhow::bail!("injected failure");
            }
            self.calls
                .lock()
                .unwrap()
                .push(MockNetworkOp::ReleaseSandboxNetwork {
                    namespace: key.namespace.clone(),
                    name: key.name.clone(),
                    uid: key.uid.clone(),
                    sandbox_id: sandbox_id.to_string(),
                });
            Ok(())
        }
    }
}
