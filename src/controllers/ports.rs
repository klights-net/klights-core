use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;
#[cfg(not(test))]
use serde_json::Value;

use super::{
    apiservice::ApiServiceStore,
    csr_signer::CsrStatusStore,
    daemonset::{DaemonSetPodMutation, DaemonSetStore},
    deployment::{DeploymentPodMutation, DeploymentPodReader, DeploymentStore},
    job::{JobPodMutation, JobStore},
    pdb::PdbStore,
    pvc::PvcStore,
    replicaset::{ReplicaSetPodMutation, ReplicaSetStore},
    replicationcontroller::{ReplicationControllerPodMutation, ReplicationControllerStore},
    service::ServiceControllerStore,
    statefulset::{StatefulSetPodMutation, StatefulSetStore},
};

#[async_trait]
#[cfg_attr(test, allow(dead_code))]
pub(crate) trait ControllerLeaderPort: Send + Sync {
    async fn get_reconcile_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>>;

    async fn namespace_is_terminating(&self, namespace: &str) -> Result<bool>;

    fn deployment_store(&self) -> &dyn DeploymentStore;
    fn replicaset_store(&self) -> &dyn ReplicaSetStore;
    fn statefulset_store(&self) -> &dyn StatefulSetStore;
    fn daemonset_store(&self) -> &dyn DaemonSetStore;
    fn job_store(&self) -> &dyn JobStore;
    fn service_store(&self) -> &dyn ServiceControllerStore;
    fn pvc_store(&self) -> &dyn PvcStore;
    fn pdb_store(&self) -> &dyn PdbStore;
    fn replicationcontroller_store(&self) -> &dyn ReplicationControllerStore;
    fn apiservice_store(&self) -> &dyn ApiServiceStore;
    fn csr_status_store(&self) -> &dyn CsrStatusStore;
}

pub(crate) trait ControllerPodPort: Send + Sync {
    fn query(&self) -> &dyn klights_pod_api::PodQuery;
    fn pdb_reader(&self) -> &dyn super::pdb::PdbPodReader;
    fn deployment_reader(&self) -> &dyn DeploymentControllerPodReader;
    fn deployment_mutation(&self) -> &dyn DeploymentControllerPodMutation;
    fn replicaset_mutation(&self) -> &dyn ReplicaSetPodMutation;
    fn statefulset_mutation(&self) -> &dyn StatefulSetPodMutation;
    fn daemonset_mutation(&self) -> &dyn DaemonSetPodMutation;
    fn job_mutation(&self) -> &dyn JobPodMutation;
    fn replicationcontroller_mutation(&self) -> &dyn ReplicationControllerPodMutation;
    fn delete_sink(&self) -> &dyn klights_reconcile_api::GcPodDeleteSink;
}

pub(crate) trait DeploymentControllerPodMutation:
    DeploymentPodMutation + ReplicaSetPodMutation + Send + Sync
{
}

pub(crate) trait DeploymentControllerPodReader:
    DeploymentPodReader + klights_pod_api::PodQuery + Send + Sync
{
}

impl<T> DeploymentControllerPodReader for T where
    T: DeploymentPodReader + klights_pod_api::PodQuery + Send + Sync + ?Sized
{
}

impl<T> DeploymentControllerPodMutation for T where
    T: DeploymentPodMutation + ReplicaSetPodMutation + Send + Sync + ?Sized
{
}

pub(crate) trait ControllerReconcilePort: Send + Sync {
    fn non_pod_finalization(&self) -> &dyn klights_reconcile_api::GcNonPodFinalizationPort;
}

pub(crate) trait ControllerNetworkPort: Send + Sync {
    fn service_router(&self) -> &dyn klights_network_api::ServiceRouter;
}

pub(crate) trait ControllerEffectPort: Send + Sync {
    fn file_process(&self) -> &klights_supervisor::FileProcessExecutor;
    fn local_path_provisioner_root(&self) -> &std::path::Path;
}

#[derive(Clone)]
pub(crate) struct ControllerRuntimeDependencies {
    pub(crate) leader: Arc<dyn ControllerLeaderPort>,
    pub(crate) pods: Arc<dyn ControllerPodPort>,
    pub(crate) reconcile: Arc<dyn ControllerReconcilePort>,
    pub(crate) network: Arc<dyn ControllerNetworkPort>,
    pub(crate) effects: Arc<dyn ControllerEffectPort>,
    pub(crate) node_name: Arc<str>,
}

#[cfg(not(test))]
pub(crate) fn inject_resource_version(mut data: Value, resource_version: i64) -> Value {
    if let Some(metadata) = data.get_mut("metadata").and_then(Value::as_object_mut) {
        metadata.insert(
            "resourceVersion".to_string(),
            Value::String(resource_version.to_string()),
        );
    }
    data
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compose_fake_api(
        leader: Arc<dyn ControllerLeaderPort>,
        pods: Arc<dyn ControllerPodPort>,
        reconcile: Arc<dyn ControllerReconcilePort>,
        network: Arc<dyn ControllerNetworkPort>,
        effects: Arc<dyn ControllerEffectPort>,
    ) -> ControllerRuntimeDependencies {
        ControllerRuntimeDependencies {
            leader,
            pods,
            reconcile,
            network,
            effects,
            node_name: Arc::from("fake-node"),
        }
    }

    #[test]
    fn controller_ports_are_object_safe_and_fake_composable() {
        fn assert_object_safe(_: Option<Arc<dyn ControllerLeaderPort>>) {}
        fn assert_pod_object_safe(_: Option<Arc<dyn ControllerPodPort>>) {}
        fn assert_reconcile_object_safe(_: Option<Arc<dyn ControllerReconcilePort>>) {}
        fn assert_network_object_safe(_: Option<Arc<dyn ControllerNetworkPort>>) {}
        fn assert_effect_object_safe(_: Option<Arc<dyn ControllerEffectPort>>) {}

        assert_object_safe(None);
        assert_pod_object_safe(None);
        assert_reconcile_object_safe(None);
        assert_network_object_safe(None);
        assert_effect_object_safe(None);
        let _ = compose_fake_api;
    }
}
