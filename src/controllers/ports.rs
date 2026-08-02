use std::sync::Arc;

use async_trait::async_trait;
use klights_cluster_core::Resource;
use serde_json::Value;

use klights_controllers::service::ServiceControllerStore;
use klights_controllers::{
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
pub(crate) trait ControllerResourceQuery: Send + Sync {
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
#[cfg_attr(test, allow(dead_code))]
pub(crate) struct ControllerRuntimeDependencies {
    pub(crate) wall_time: fn() -> chrono::DateTime<chrono::Utc>,
    pub(crate) resource_query: Arc<dyn ControllerResourceQuery>,
    pub(crate) deployment_store: Arc<dyn DeploymentStore>,
    pub(crate) replicaset_store: Arc<dyn ReplicaSetStore>,
    pub(crate) statefulset_store: Arc<dyn StatefulSetStore>,
    pub(crate) daemonset_store: Arc<dyn DaemonSetStore>,
    pub(crate) job_store: Arc<dyn JobStore>,
    pub(crate) service_store: Arc<dyn ServiceControllerStore>,
    pub(crate) pvc_store: Arc<dyn PvcStore>,
    pub(crate) pdb_store: Arc<dyn PdbStore>,
    pub(crate) replicationcontroller_store: Arc<dyn ReplicationControllerStore>,
    pub(crate) apiservice_store: Arc<dyn ApiServiceStore>,
    pub(crate) csr_status_store: Arc<dyn CsrStatusStore>,
    pub(crate) pod_query: Arc<dyn klights_pod_api::PodQuery>,
    pub(crate) pdb_pod_reader: Arc<dyn klights_controllers::pdb::PdbPodReader>,
    pub(crate) deployment_pod_reader: Arc<dyn DeploymentControllerPodReader>,
    pub(crate) deployment_pod_mutation: Arc<dyn DeploymentControllerPodMutation>,
    pub(crate) replicaset_pod_mutation: Arc<dyn ReplicaSetPodMutation>,
    pub(crate) statefulset_pod_mutation: Arc<dyn StatefulSetPodMutation>,
    pub(crate) daemonset_pod_mutation: Arc<dyn DaemonSetPodMutation>,
    pub(crate) job_pod_mutation: Arc<dyn JobPodMutation>,
    pub(crate) replicationcontroller_pod_mutation: Arc<dyn ReplicationControllerPodMutation>,
    pub(crate) pod_delete_sink: Arc<dyn klights_reconcile_api::GcPodDeleteSink>,
    pub(crate) reconcile: Arc<dyn ControllerReconcilePort>,
    pub(crate) network: Arc<dyn ControllerNetworkPort>,
    pub(crate) effects: Arc<dyn ControllerEffectPort>,
    pub(crate) coordination: Arc<klights_controllers::ControllerCoordination>,
    pub(crate) node_name: Arc<str>,
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

    fn compose_fake_api(
        resource_query: Arc<dyn ControllerResourceQuery>,
        stores: Arc<crate::controller_runtime_adapter::RootControllerLeaderPort>,
        pod_query: Arc<dyn klights_pod_api::PodQuery>,
        pod_mutations: Arc<crate::controller_runtime_adapter::RootControllerPodPort>,
        pod_repository: Arc<crate::kubelet::pod_repository::PodRepository>,
        reconcile: Arc<dyn ControllerReconcilePort>,
        network: Arc<dyn ControllerNetworkPort>,
        effects: Arc<dyn ControllerEffectPort>,
        coordination: Arc<klights_controllers::ControllerCoordination>,
    ) -> ControllerRuntimeDependencies {
        ControllerRuntimeDependencies {
            wall_time: chrono::Utc::now,
            resource_query,
            deployment_store: stores.clone(),
            replicaset_store: stores.clone(),
            statefulset_store: stores.clone(),
            daemonset_store: stores.clone(),
            job_store: stores.clone(),
            service_store: stores.clone(),
            pvc_store: stores.clone(),
            pdb_store: stores.clone(),
            replicationcontroller_store: stores.clone(),
            apiservice_store: stores.clone(),
            csr_status_store: stores,
            pod_query,
            pdb_pod_reader: pod_repository.clone(),
            deployment_pod_reader: pod_repository.clone(),
            deployment_pod_mutation: pod_mutations.clone(),
            replicaset_pod_mutation: pod_mutations.clone(),
            statefulset_pod_mutation: pod_mutations.clone(),
            daemonset_pod_mutation: pod_mutations.clone(),
            job_pod_mutation: pod_mutations.clone(),
            replicationcontroller_pod_mutation: pod_mutations,
            pod_delete_sink: pod_repository,
            reconcile,
            network,
            effects,
            coordination,
            node_name: Arc::from("fake-node"),
        }
    }

    #[test]
    fn controller_ports_are_object_safe_and_fake_composable() {
        fn assert_object_safe(_: Option<Arc<dyn ControllerResourceQuery>>) {}
        fn assert_reconcile_object_safe(_: Option<Arc<dyn ControllerReconcilePort>>) {}
        fn assert_network_object_safe(_: Option<Arc<dyn ControllerNetworkPort>>) {}
        fn assert_effect_object_safe(_: Option<Arc<dyn ControllerEffectPort>>) {}

        assert_object_safe(None);
        assert_reconcile_object_safe(None);
        assert_network_object_safe(None);
        assert_effect_object_safe(None);
        let _ = compose_fake_api;
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
