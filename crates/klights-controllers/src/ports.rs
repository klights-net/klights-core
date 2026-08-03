use std::sync::Arc;

use async_trait::async_trait;
use klights_cluster_core::Resource;
use serde_json::Value;

use crate::service::ServiceControllerStore;
use crate::{
    apiservice::ApiServiceStore,
    csr_signer::CsrStatusStore,
    daemonset::{DaemonSetPodMutation, DaemonSetStore},
    deployment::{DeploymentPodMutation, DeploymentPodReader, DeploymentStore},
    job::{JobPodMutation, JobStore},
    pdb::PdbStore,
    pvc::PvcStore,
    replicaset::{ReplicaSetPodMutation, ReplicaSetStore},
    replicationcontroller::{ReplicationControllerPodMutation, ReplicationControllerStore},
    statefulset::{StatefulSetPodMutation, StatefulSetStore},
};

#[async_trait]
#[cfg_attr(test, allow(dead_code))]
pub trait ControllerResourceQuery: Send + Sync {
    async fn get_reconcile_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>, klights_leader_api::ResourceQueryError>;

    async fn namespace_is_terminating(
        &self,
        namespace: &str,
    ) -> Result<bool, klights_leader_api::ResourceQueryError>;
}

pub trait DeploymentControllerPodMutation:
    DeploymentPodMutation + ReplicaSetPodMutation + Send + Sync
{
}

pub trait DeploymentControllerPodReader:
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

pub trait ControllerReconcilePort: Send + Sync {
    fn non_pod_finalization(&self) -> &dyn klights_reconcile_api::GcNonPodFinalizationPort;
}

pub trait ControllerNetworkPort: Send + Sync {
    fn service_router(&self) -> &dyn klights_network_api::ServiceRouter;
}

pub trait ControllerEffectPort: Send + Sync {
    fn file_process(&self) -> &klights_supervisor::FileProcessExecutor;
    fn local_path_provisioner_root(&self) -> &std::path::Path;
}

#[derive(Clone)]
#[cfg_attr(test, allow(dead_code))]
pub struct ControllerRuntimeDependencies {
    pub wall_time: fn() -> chrono::DateTime<chrono::Utc>,
    pub resource_query: Arc<dyn ControllerResourceQuery>,
    pub deployment_store: Arc<dyn DeploymentStore>,
    pub replicaset_store: Arc<dyn ReplicaSetStore>,
    pub statefulset_store: Arc<dyn StatefulSetStore>,
    pub daemonset_store: Arc<dyn DaemonSetStore>,
    pub job_store: Arc<dyn JobStore>,
    pub service_store: Arc<dyn ServiceControllerStore>,
    pub pvc_store: Arc<dyn PvcStore>,
    pub pdb_store: Arc<dyn PdbStore>,
    pub replicationcontroller_store: Arc<dyn ReplicationControllerStore>,
    pub apiservice_store: Arc<dyn ApiServiceStore>,
    pub csr_status_store: Arc<dyn CsrStatusStore>,
    pub pod_query: Arc<dyn klights_pod_api::PodQuery>,
    pub pdb_pod_reader: Arc<dyn crate::pdb::PdbPodReader>,
    pub deployment_pod_reader: Arc<dyn DeploymentControllerPodReader>,
    pub deployment_pod_mutation: Arc<dyn DeploymentControllerPodMutation>,
    pub replicaset_pod_mutation: Arc<dyn ReplicaSetPodMutation>,
    pub statefulset_pod_mutation: Arc<dyn StatefulSetPodMutation>,
    pub daemonset_pod_mutation: Arc<dyn DaemonSetPodMutation>,
    pub job_pod_mutation: Arc<dyn JobPodMutation>,
    pub replicationcontroller_pod_mutation: Arc<dyn ReplicationControllerPodMutation>,
    pub pod_delete_sink: Arc<dyn klights_reconcile_api::GcPodDeleteSink>,
    pub reconcile: Arc<dyn ControllerReconcilePort>,
    pub network: Arc<dyn ControllerNetworkPort>,
    pub effects: Arc<dyn ControllerEffectPort>,
    pub coordination: Arc<crate::ControllerCoordination>,
    pub node_name: Arc<str>,
}

pub(crate) fn inject_resource_version(data: impl Into<Arc<Value>>, resource_version: i64) -> Value {
    let mut data = Arc::unwrap_or_clone(data.into());
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

    #[test]
    fn controller_ports_are_object_safe() {
        fn assert_object_safe(_: Option<Arc<dyn ControllerResourceQuery>>) {}
        fn assert_reconcile_object_safe(_: Option<Arc<dyn ControllerReconcilePort>>) {}
        fn assert_network_object_safe(_: Option<Arc<dyn ControllerNetworkPort>>) {}
        fn assert_effect_object_safe(_: Option<Arc<dyn ControllerEffectPort>>) {}

        assert_object_safe(None);
        assert_reconcile_object_safe(None);
        assert_network_object_safe(None);
        assert_effect_object_safe(None);
    }

    #[test]
    fn controller_projection_preserves_persisted_uid_without_api_fallback() {
        let projected = inject_resource_version(
            serde_json::json!({"metadata": {"uid": "persisted-api-object-uid"}}),
            42,
        );
        assert_eq!(projected["metadata"]["uid"], "persisted-api-object-uid");
        assert_eq!(projected["metadata"]["resourceVersion"], "42");

        let missing_uid = inject_resource_version(serde_json::json!({"metadata": {}}), 43);
        assert!(missing_uid["metadata"].get("uid").is_none());
    }
}
