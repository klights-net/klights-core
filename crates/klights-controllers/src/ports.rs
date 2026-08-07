use std::sync::Arc;

use async_trait::async_trait;
use klights_cluster_core::Resource;
use serde_json::Value;

fn map_pod_repository_error(
    error: klights_pod_api::PodRepositoryError,
) -> klights_reconcile_api::ControllerStoreError {
    use klights_pod_api::PodRepositoryError;
    let message = error.to_string();
    match error {
        PodRepositoryError::NotFound { .. } => {
            klights_reconcile_api::ControllerStoreError::not_found(message)
        }
        PodRepositoryError::AlreadyExists { .. } => {
            klights_reconcile_api::ControllerStoreError::already_exists(message)
        }
        PodRepositoryError::UidMismatch { .. } | PodRepositoryError::Conflict { .. } => {
            klights_reconcile_api::ControllerStoreError::conflict(message)
        }
        PodRepositoryError::Unavailable { .. }
        | PodRepositoryError::Timeout
        | PodRepositoryError::Cancelled => {
            klights_reconcile_api::ControllerStoreError::unavailable(message)
        }
        PodRepositoryError::InvalidRequest { .. }
        | PodRepositoryError::Forbidden { .. }
        | PodRepositoryError::Unprocessable { .. }
        | PodRepositoryError::Internal { .. }
        | PodRepositoryError::CorruptResponse { .. } => {
            klights_reconcile_api::ControllerStoreError::internal(message)
        }
    }
}

pub(crate) async fn create_pod_via_api(
    api: &dyn klights_pod_api::PodApiMutation,
    namespace: &str,
    name: &str,
    pod: Value,
) -> klights_reconcile_api::ControllerStoreResult<Resource> {
    api.create_pod(klights_pod_api::PodApiCreateRequest {
        namespace: namespace.to_string(),
        body: pod,
        dry_run: false,
    })
    .await
    .map_err(map_pod_repository_error)?
    .resource
    .ok_or_else(|| {
        klights_reconcile_api::ControllerStoreError::unavailable(format!(
            "controller Pod {namespace}/{name} create returned dry-run"
        ))
    })
}

pub(crate) async fn replace_pod_owner_references_via_update(
    update: &dyn klights_pod_api::PodUpdate,
    namespace: &str,
    name: &str,
    owner_references: Vec<Value>,
) -> klights_reconcile_api::ControllerStoreResult<Resource> {
    let owner_references = owner_references
        .into_iter()
        .map(|owner| {
            klights_pod_api::PodOwnerReference::try_new(
                owner
                    .get("apiVersion")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
                owner
                    .get("kind")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
                owner
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
                owner.get("uid").and_then(Value::as_str).unwrap_or_default(),
                owner.get("controller").and_then(Value::as_bool),
                owner.get("blockOwnerDeletion").and_then(Value::as_bool),
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            klights_reconcile_api::ControllerStoreError::internal(error.to_string())
        })?;
    let target =
        klights_pod_api::PodMutationTarget::try_by_name(namespace, name).map_err(|error| {
            klights_reconcile_api::ControllerStoreError::internal(error.to_string())
        })?;
    update
        .update_pod(klights_pod_api::PodUpdateRequest::replace_owner_references(
            target,
            owner_references,
        ))
        .await
        .map_err(map_pod_repository_error)
}

pub(crate) async fn merge_pod_labels_via_update(
    update: &dyn klights_pod_api::PodUpdate,
    namespace: &str,
    name: &str,
    labels: Vec<(String, String)>,
) -> klights_reconcile_api::ControllerStoreResult<Resource> {
    let labels = labels
        .into_iter()
        .map(|(key, value)| klights_pod_api::PodLabel::try_new(key, value))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            klights_reconcile_api::ControllerStoreError::internal(error.to_string())
        })?;
    let target =
        klights_pod_api::PodMutationTarget::try_by_name(namespace, name).map_err(|error| {
            klights_reconcile_api::ControllerStoreError::internal(error.to_string())
        })?;
    update
        .update_pod(klights_pod_api::PodUpdateRequest::merge_labels(
            target, labels,
        ))
        .await
        .map_err(map_pod_repository_error)
}

pub struct ControllerPodMutationAdapter {
    api: Arc<dyn klights_pod_api::PodApiMutation>,
    update: Arc<dyn klights_pod_api::PodUpdate>,
}

impl ControllerPodMutationAdapter {
    pub fn new(
        api: Arc<dyn klights_pod_api::PodApiMutation>,
        update: Arc<dyn klights_pod_api::PodUpdate>,
    ) -> Self {
        Self { api, update }
    }
}

macro_rules! impl_create_mutation {
    ($trait:path, $method:ident) => {
        #[async_trait]
        impl $trait for ControllerPodMutationAdapter {
            async fn $method(
                &self,
                namespace: &str,
                name: &str,
                _node_name: &str,
                pod: Value,
            ) -> klights_reconcile_api::ControllerStoreResult<Resource> {
                create_pod_via_api(self.api.as_ref(), namespace, name, pod).await
            }
        }
    };
}

impl_create_mutation!(crate::daemonset::DaemonSetPodMutation, create_daemonset_pod);
impl_create_mutation!(
    crate::statefulset::StatefulSetPodMutation,
    create_statefulset_pod
);

#[async_trait]
impl crate::job::JobPodMutation for ControllerPodMutationAdapter {
    async fn create_job_pod(
        &self,
        namespace: &str,
        name: &str,
        _node_name: &str,
        pod: Value,
    ) -> klights_reconcile_api::ControllerStoreResult<Resource> {
        create_pod_via_api(self.api.as_ref(), namespace, name, pod).await
    }

    async fn replace_job_pod_owner_references(
        &self,
        namespace: &str,
        name: &str,
        owner_references: Vec<Value>,
    ) -> klights_reconcile_api::ControllerStoreResult<Resource> {
        replace_pod_owner_references_via_update(
            self.update.as_ref(),
            namespace,
            name,
            owner_references,
        )
        .await
    }
}

#[async_trait]
impl crate::replicaset::ReplicaSetPodMutation for ControllerPodMutationAdapter {
    async fn create_replicaset_pod(
        &self,
        namespace: &str,
        name: &str,
        _node_name: &str,
        pod: Value,
    ) -> klights_reconcile_api::ControllerStoreResult<Resource> {
        create_pod_via_api(self.api.as_ref(), namespace, name, pod).await
    }

    async fn replace_replicaset_pod_owner_references(
        &self,
        namespace: &str,
        name: &str,
        owner_references: Vec<Value>,
    ) -> klights_reconcile_api::ControllerStoreResult<Resource> {
        replace_pod_owner_references_via_update(
            self.update.as_ref(),
            namespace,
            name,
            owner_references,
        )
        .await
    }
}

#[async_trait]
impl crate::replicationcontroller::ReplicationControllerPodMutation
    for ControllerPodMutationAdapter
{
    async fn create_replication_controller_pod(
        &self,
        namespace: &str,
        name: &str,
        _node_name: &str,
        pod: Value,
    ) -> klights_reconcile_api::ControllerStoreResult<Resource> {
        create_pod_via_api(self.api.as_ref(), namespace, name, pod).await
    }

    async fn replace_replication_controller_pod_owner_references(
        &self,
        namespace: &str,
        name: &str,
        owner_references: Vec<Value>,
    ) -> klights_reconcile_api::ControllerStoreResult<Resource> {
        replace_pod_owner_references_via_update(
            self.update.as_ref(),
            namespace,
            name,
            owner_references,
        )
        .await
    }
}

#[async_trait]
impl crate::deployment::DeploymentPodMutation for ControllerPodMutationAdapter {
    async fn merge_deployment_pod_labels(
        &self,
        namespace: &str,
        name: &str,
        labels: Vec<(String, String)>,
    ) -> klights_reconcile_api::ControllerStoreResult<Resource> {
        merge_pod_labels_via_update(self.update.as_ref(), namespace, name, labels).await
    }
}

use crate::service::ServiceControllerStore;
use crate::{
    apiservice::ApiServiceStore,
    csr_signer::CsrStatusStore,
    daemonset::{DaemonSetPodMutation, DaemonSetStore},
    deployment::{DeploymentPodMutation, DeploymentStore},
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

    #[test]
    fn pod_repository_errors_map_to_structural_controller_categories() {
        use klights_pod_api::PodRepositoryError as PodError;
        use klights_reconcile_api::ControllerStoreError as StoreError;

        let cases = [
            (PodError::not_found("default", "web"), "not_found"),
            (PodError::already_exists("exists"), "already_exists"),
            (PodError::uid_mismatch("old", "new"), "conflict"),
            (PodError::conflict("stale"), "conflict"),
            (PodError::unavailable("offline"), "unavailable"),
            (PodError::Timeout, "unavailable"),
            (PodError::Cancelled, "unavailable"),
            (PodError::invalid_request("pod.name", "empty"), "internal"),
            (PodError::forbidden("denied"), "internal"),
            (PodError::unprocessable("invalid"), "internal"),
            (PodError::internal("bug"), "internal"),
            (PodError::corrupt_response("broken"), "internal"),
        ];

        for (error, expected) in cases {
            let actual = map_pod_repository_error(error);
            let category = match actual {
                StoreError::NotFound(_) => "not_found",
                StoreError::AlreadyExists(_) => "already_exists",
                StoreError::Conflict(_) => "conflict",
                StoreError::Unavailable(_) => "unavailable",
                StoreError::Internal(_) => "internal",
            };
            assert_eq!(category, expected);
        }
    }
}
