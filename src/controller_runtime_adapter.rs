use std::sync::Arc;

use async_trait::async_trait;
use klights_cluster_core::{Resource, ResourceBatchOperation, ResourcePreconditions};
use klights_reconcile_api::{ControllerStoreError, ControllerStoreResult as Result};

use crate::controllers::{
    ControllerEffectPort, ControllerNetworkPort, ControllerReconcilePort, ControllerResourceQuery,
};
use crate::datastore::DatastoreHandle;
use crate::kubelet::pod_repository::PodRepository;

fn validate_controller_effect() -> Result<()> {
    klights_leader_api::validate_controller_lease_if_scoped().map_err(|error| {
        ControllerStoreError::unavailable(format!("controller authority rejected effect: {error}"))
    })
}

pub(crate) struct RootControllerLeaderPort {
    store: DatastoreHandle,
}

impl RootControllerLeaderPort {
    pub(crate) fn new(store: DatastoreHandle) -> Self {
        Self { store }
    }
}

#[async_trait]
impl ControllerResourceQuery for RootControllerLeaderPort {
    async fn get_reconcile_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> std::result::Result<Option<Resource>, klights_leader_api::ResourceQueryError> {
        self.store
            .get_resource(api_version, kind, namespace, name)
            .await
            .map_err(|error| {
                klights_leader_api::ResourceQueryError::retryable(format!(
                    "controller resource query failed: {error}"
                ))
            })
    }

    async fn namespace_is_terminating(
        &self,
        namespace: &str,
    ) -> std::result::Result<bool, klights_leader_api::ResourceQueryError> {
        Ok(self
            .store
            .get_namespace(namespace)
            .await
            .map_err(|error| {
                klights_leader_api::ResourceQueryError::retryable(format!(
                    "controller namespace query failed: {error}"
                ))
            })?
            .is_some_and(|resource| {
                resource
                    .data
                    .pointer("/metadata/deletionTimestamp")
                    .and_then(serde_json::Value::as_str)
                    .is_some()
            }))
    }
}

#[async_trait]
impl crate::controllers::gc::GcResourceStore for RootControllerLeaderPort {
    async fn list_custom_resource_definitions(&self) -> Result<Vec<Resource>> {
        crate::controllers::gc::GcResourceStore::list_custom_resource_definitions(
            self.store.as_ref(),
        )
        .await
    }

    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        crate::controllers::gc::GcResourceStore::get_resource(
            self.store.as_ref(),
            api_version,
            kind,
            namespace,
            name,
        )
        .await
    }

    async fn update_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        validate_controller_effect()?;
        crate::controllers::gc::GcResourceStore::update_resource_with_preconditions(
            self.store.as_ref(),
            api_version,
            kind,
            namespace,
            name,
            data,
            preconditions,
        )
        .await
    }

    async fn update_main_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        validate_controller_effect()?;
        crate::controllers::gc::GcResourceStore::update_main_resource_with_preconditions(
            self.store.as_ref(),
            api_version,
            kind,
            namespace,
            name,
            data,
            preconditions,
        )
        .await
    }

    async fn find_owned_resources(
        &self,
        owner_uid: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        crate::controllers::gc::GcResourceStore::find_owned_resources(
            self.store.as_ref(),
            owner_uid,
            namespace,
        )
        .await
    }

    async fn find_owned_by_name_kind_empty_uid(
        &self,
        owner_api_version: &str,
        owner_name: &str,
        owner_kind: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        crate::controllers::gc::GcResourceStore::find_owned_by_name_kind_empty_uid(
            self.store.as_ref(),
            owner_api_version,
            owner_name,
            owner_kind,
            namespace,
        )
        .await
    }
}

#[async_trait]
impl crate::controllers::replicaset::ReplicaSetStore for RootControllerLeaderPort {
    async fn get_replicaset(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        crate::controllers::replicaset::ReplicaSetStore::get_replicaset(
            self.store.as_ref(),
            namespace,
            name,
        )
        .await
    }

    async fn update_replicaset_status(
        &self,
        resource: &Resource,
        status: serde_json::Value,
    ) -> Result<()> {
        validate_controller_effect()?;
        crate::controllers::replicaset::ReplicaSetStore::update_replicaset_status(
            self.store.as_ref(),
            resource,
            status,
        )
        .await
    }
}

#[async_trait]
impl crate::controllers::deployment::DeploymentFinalizeStore for RootControllerLeaderPort {
    async fn get_deployment(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        crate::controllers::deployment::DeploymentFinalizeStore::get_deployment(
            self.store.as_ref(),
            namespace,
            name,
        )
        .await
    }

    async fn patch_deployment_revision(
        &self,
        namespace: &str,
        name: &str,
        revision: String,
        expected_uid: String,
    ) -> Result<()> {
        validate_controller_effect()?;
        crate::controllers::deployment::DeploymentFinalizeStore::patch_deployment_revision(
            self.store.as_ref(),
            namespace,
            name,
            revision,
            expected_uid,
        )
        .await
    }

    async fn delete_replicaset(
        &self,
        namespace: &str,
        name: &str,
        expected_uid: String,
    ) -> Result<()> {
        validate_controller_effect()?;
        crate::controllers::deployment::DeploymentFinalizeStore::delete_replicaset(
            self.store.as_ref(),
            namespace,
            name,
            expected_uid,
        )
        .await
    }
}

#[async_trait]
impl crate::controllers::deployment::DeploymentStore for RootControllerLeaderPort {
    async fn list_replicasets(&self, namespace: &str) -> Result<Vec<Resource>> {
        crate::controllers::deployment::DeploymentStore::list_replicasets(
            self.store.as_ref(),
            namespace,
        )
        .await
    }

    async fn create_replicaset(
        &self,
        namespace: &str,
        name: &str,
        replicaset: serde_json::Value,
    ) -> Result<Resource> {
        validate_controller_effect()?;
        crate::controllers::deployment::DeploymentStore::create_replicaset(
            self.store.as_ref(),
            namespace,
            name,
            replicaset,
        )
        .await
    }

    async fn patch_replicaset_scale(
        &self,
        namespace: &str,
        name: &str,
        patch: serde_json::Value,
        expected_uid: String,
    ) -> Result<Option<Resource>> {
        validate_controller_effect()?;
        crate::controllers::deployment::DeploymentStore::patch_replicaset_scale(
            self.store.as_ref(),
            namespace,
            name,
            patch,
            expected_uid,
        )
        .await
    }

    async fn update_deployment_status(
        &self,
        resource: &Resource,
        status: serde_json::Value,
    ) -> Result<()> {
        validate_controller_effect()?;
        crate::controllers::deployment::DeploymentStore::update_deployment_status(
            self.store.as_ref(),
            resource,
            status,
        )
        .await
    }
}

#[async_trait]
impl crate::controllers::statefulset::StatefulSetStore for RootControllerLeaderPort {
    async fn get_statefulset(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        crate::controllers::statefulset::StatefulSetStore::get_statefulset(
            self.store.as_ref(),
            namespace,
            name,
        )
        .await
    }

    async fn update_statefulset_status(
        &self,
        resource: &Resource,
        status: serde_json::Value,
    ) -> Result<()> {
        validate_controller_effect()?;
        crate::controllers::statefulset::StatefulSetStore::update_statefulset_status(
            self.store.as_ref(),
            resource,
            status,
        )
        .await
    }
}

#[async_trait]
impl crate::controllers::daemonset::DaemonSetStore for RootControllerLeaderPort {
    async fn list_controller_revisions(&self, namespace: &str) -> Result<Vec<Resource>> {
        crate::controllers::daemonset::DaemonSetStore::list_controller_revisions(
            self.store.as_ref(),
            namespace,
        )
        .await
    }

    async fn create_controller_revision(
        &self,
        namespace: &str,
        name: &str,
        revision: serde_json::Value,
    ) -> Result<Resource> {
        validate_controller_effect()?;
        crate::controllers::daemonset::DaemonSetStore::create_controller_revision(
            self.store.as_ref(),
            namespace,
            name,
            revision,
        )
        .await
    }

    async fn list_nodes(&self) -> Result<Vec<Resource>> {
        crate::controllers::daemonset::DaemonSetStore::list_nodes(self.store.as_ref()).await
    }

    async fn update_daemonset_status(
        &self,
        resource: &Resource,
        status: serde_json::Value,
    ) -> Result<()> {
        validate_controller_effect()?;
        crate::controllers::daemonset::DaemonSetStore::update_daemonset_status(
            self.store.as_ref(),
            resource,
            status,
        )
        .await
    }
}

#[async_trait]
impl crate::controllers::job::JobStore for RootControllerLeaderPort {
    async fn get_job(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        crate::controllers::job::JobStore::get_job(self.store.as_ref(), namespace, name).await
    }

    async fn update_job_status(
        &self,
        resource: &Resource,
        status: serde_json::Value,
    ) -> Result<Resource> {
        validate_controller_effect()?;
        crate::controllers::job::JobStore::update_job_status(self.store.as_ref(), resource, status)
            .await
    }
}

#[async_trait]
impl klights_controllers::service::ServiceReconcileStore for RootControllerLeaderPort {
    async fn list_services(&self) -> Result<Vec<Resource>> {
        klights_controllers::service::ServiceReconcileStore::list_services(self.store.as_ref())
            .await
    }

    async fn get_service(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        klights_controllers::service::ServiceReconcileStore::get_service(
            self.store.as_ref(),
            namespace,
            name,
        )
        .await
    }

    async fn update_service(
        &self,
        namespace: &str,
        name: &str,
        data: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        validate_controller_effect()?;
        klights_controllers::service::ServiceReconcileStore::update_service(
            self.store.as_ref(),
            namespace,
            name,
            data,
            preconditions,
        )
        .await
    }
}

#[async_trait]
impl klights_controllers::endpoints::EndpointReconcileStore for RootControllerLeaderPort {
    async fn endpoint_namespace_is_terminating(&self, namespace: &str) -> Result<bool> {
        klights_controllers::endpoints::EndpointReconcileStore::endpoint_namespace_is_terminating(
            self.store.as_ref(),
            namespace,
        )
        .await
    }

    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        klights_controllers::endpoints::EndpointReconcileStore::get_resource(
            self.store.as_ref(),
            api_version,
            kind,
            namespace,
            name,
        )
        .await
    }

    async fn list_service_endpoint_slices(
        &self,
        namespace: &str,
        service_name: &str,
    ) -> Result<Vec<Resource>> {
        klights_controllers::endpoints::EndpointReconcileStore::list_service_endpoint_slices(
            self.store.as_ref(),
            namespace,
            service_name,
        )
        .await
    }

    async fn create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: serde_json::Value,
    ) -> Result<Resource> {
        validate_controller_effect()?;
        klights_controllers::endpoints::EndpointReconcileStore::create_resource(
            self.store.as_ref(),
            api_version,
            kind,
            namespace,
            name,
            data,
        )
        .await
    }

    async fn update_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        validate_controller_effect()?;
        klights_controllers::endpoints::EndpointReconcileStore::update_resource_with_preconditions(
            self.store.as_ref(),
            api_version,
            kind,
            namespace,
            name,
            data,
            preconditions,
        )
        .await
    }

    async fn delete_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> Result<()> {
        validate_controller_effect()?;
        klights_controllers::endpoints::EndpointReconcileStore::delete_resource_with_preconditions(
            self.store.as_ref(),
            api_version,
            kind,
            namespace,
            name,
            preconditions,
        )
        .await
    }

    async fn apply_resource_batch(&self, operations: Vec<ResourceBatchOperation>) -> Result<()> {
        validate_controller_effect()?;
        klights_controllers::endpoints::EndpointReconcileStore::apply_resource_batch(
            self.store.as_ref(),
            operations,
        )
        .await
    }
}

#[async_trait]
impl crate::controllers::common::ControllerStatusStore for RootControllerLeaderPort {
    async fn get_status_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        crate::controllers::common::ControllerStatusStore::get_status_resource(
            self.store.as_ref(),
            api_version,
            kind,
            namespace,
            name,
        )
        .await
    }

    async fn update_status(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        validate_controller_effect()?;
        crate::controllers::common::ControllerStatusStore::update_status(
            self.store.as_ref(),
            api_version,
            kind,
            namespace,
            name,
            status,
            preconditions,
        )
        .await
    }

    fn log_noop_status_write(
        &self,
        operation: &'static str,
        resource: &Resource,
        reason: &'static str,
    ) {
        crate::controllers::common::ControllerStatusStore::log_noop_status_write(
            self.store.as_ref(),
            operation,
            resource,
            reason,
        );
    }
}

#[async_trait]
impl crate::controllers::pvc::PvcStore for RootControllerLeaderPort {
    async fn get_pvc(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        crate::controllers::pvc::PvcStore::get_pvc(self.store.as_ref(), namespace, name).await
    }

    async fn list_persistent_volumes(&self) -> Result<Vec<Resource>> {
        crate::controllers::pvc::PvcStore::list_persistent_volumes(self.store.as_ref()).await
    }

    async fn get_persistent_volume(&self, name: &str) -> Result<Option<Resource>> {
        crate::controllers::pvc::PvcStore::get_persistent_volume(self.store.as_ref(), name).await
    }

    async fn create_persistent_volume(
        &self,
        name: &str,
        value: serde_json::Value,
    ) -> Result<Resource> {
        validate_controller_effect()?;
        crate::controllers::pvc::PvcStore::create_persistent_volume(
            self.store.as_ref(),
            name,
            value,
        )
        .await
    }

    async fn update_persistent_volume(
        &self,
        name: &str,
        value: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        validate_controller_effect()?;
        crate::controllers::pvc::PvcStore::update_persistent_volume(
            self.store.as_ref(),
            name,
            value,
            preconditions,
        )
        .await
    }
}

#[async_trait]
impl crate::controllers::pdb::PdbStore for RootControllerLeaderPort {
    async fn list_pdbs(&self, namespace: &str) -> Result<Vec<Resource>> {
        crate::controllers::pdb::PdbStore::list_pdbs(self.store.as_ref(), namespace).await
    }
}

#[async_trait]
impl crate::controllers::replicationcontroller::ReplicationControllerStore
    for RootControllerLeaderPort
{
    async fn get_replication_controller(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Resource>> {
        crate::controllers::replicationcontroller::ReplicationControllerStore::get_replication_controller(
            self.store.as_ref(),
            namespace,
            name,
        )
        .await
    }

    async fn list_resource_quotas(&self, namespace: &str) -> Result<Vec<Resource>> {
        crate::controllers::replicationcontroller::ReplicationControllerStore::list_resource_quotas(
            self.store.as_ref(),
            namespace,
        )
        .await
    }

    async fn update_replication_controller_status(
        &self,
        resource: &Resource,
        status: serde_json::Value,
    ) -> Result<()> {
        validate_controller_effect()?;
        crate::controllers::replicationcontroller::ReplicationControllerStore::update_replication_controller_status(
            self.store.as_ref(),
            resource,
            status,
        )
        .await
    }
}

#[async_trait]
impl crate::controllers::apiservice::ApiServiceStore for RootControllerLeaderPort {
    async fn get_apiservice(&self, name: &str) -> Result<Option<Resource>> {
        crate::controllers::apiservice::ApiServiceStore::get_apiservice(self.store.as_ref(), name)
            .await
    }

    async fn service_exists(&self, namespace: &str, name: &str) -> Result<bool> {
        crate::controllers::apiservice::ApiServiceStore::service_exists(
            self.store.as_ref(),
            namespace,
            name,
        )
        .await
    }

    async fn list_endpoint_slices(
        &self,
        namespace: &str,
        service_name: &str,
    ) -> Result<Vec<Resource>> {
        crate::controllers::apiservice::ApiServiceStore::list_endpoint_slices(
            self.store.as_ref(),
            namespace,
            service_name,
        )
        .await
    }

    async fn get_endpoints(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        crate::controllers::apiservice::ApiServiceStore::get_endpoints(
            self.store.as_ref(),
            namespace,
            name,
        )
        .await
    }

    async fn update_apiservice_status(
        &self,
        current: &Resource,
        status: serde_json::Value,
    ) -> Result<()> {
        validate_controller_effect()?;
        crate::controllers::apiservice::ApiServiceStore::update_apiservice_status(
            self.store.as_ref(),
            current,
            status,
        )
        .await
    }
}

#[async_trait]
impl crate::controllers::csr_signer::CsrStatusStore for RootControllerLeaderPort {
    async fn get_csr(&self, name: &str) -> Result<Option<Resource>> {
        crate::controllers::csr_signer::CsrStatusStore::get_csr(self.store.as_ref(), name).await
    }

    async fn update_csr_status(
        &self,
        name: &str,
        uid: &str,
        resource_version: i64,
        status: serde_json::Value,
    ) -> Result<()> {
        validate_controller_effect()?;
        crate::controllers::csr_signer::CsrStatusStore::update_csr_status(
            self.store.as_ref(),
            name,
            uid,
            resource_version,
            status,
        )
        .await
    }
}

pub(crate) struct RootControllerPodPort {
    repository: Arc<PodRepository>,
    api: Arc<dyn klights_pod_api::PodApiMutation>,
    subresource: Arc<dyn klights_pod_api::PodSubresourceMutation>,
}

impl RootControllerPodPort {
    pub(crate) fn new(
        repository: Arc<PodRepository>,
        api: Arc<dyn klights_pod_api::PodApiMutation>,
        subresource: Arc<dyn klights_pod_api::PodSubresourceMutation>,
    ) -> Self {
        Self {
            repository,
            api,
            subresource,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(repository: Arc<PodRepository>) -> Self {
        let (api, subresource) = repository.test_root_api_services();
        Self::new(repository, api, subresource)
    }
}

#[async_trait]
impl crate::kubelet::pod_repository::PodObjectWriter for RootControllerPodPort {
    async fn create_controller_pod(
        &self,
        namespace: &str,
        name: &str,
        _node_name: &str,
        pod: serde_json::Value,
    ) -> anyhow::Result<Resource> {
        validate_controller_effect()?;
        let result = klights_pod_api::PodApiMutation::create_pod(
            self.api.as_ref(),
            klights_pod_api::PodApiCreateRequest {
                namespace: namespace.to_string(),
                body: pod,
                dry_run: false,
            },
        )
        .await
        .map_err(anyhow::Error::new)?;
        result.resource.ok_or_else(|| {
            anyhow::anyhow!("controller Pod {namespace}/{name} create returned dry-run")
        })
    }

    async fn delete_pod(&self, namespace: &str, name: &str) -> anyhow::Result<()> {
        validate_controller_effect()?;
        crate::kubelet::pod_repository::PodObjectWriter::delete_pod(
            self.repository.as_ref(),
            namespace,
            name,
        )
        .await
    }

    async fn update_pod_owner_references(
        &self,
        namespace: &str,
        name: &str,
        owner_refs: Vec<serde_json::Value>,
    ) -> anyhow::Result<Resource> {
        validate_controller_effect()?;
        crate::kubelet::pod_repository::PodObjectWriter::update_pod_owner_references(
            self.repository.as_ref(),
            namespace,
            name,
            owner_refs,
        )
        .await
    }

    async fn update_pod_owner_references_for_uid(
        &self,
        namespace: &str,
        name: &str,
        expected_uid: &str,
        owner_refs: Vec<serde_json::Value>,
    ) -> anyhow::Result<Resource> {
        validate_controller_effect()?;
        crate::kubelet::pod_repository::PodObjectWriter::update_pod_owner_references_for_uid(
            self.repository.as_ref(),
            namespace,
            name,
            expected_uid,
            owner_refs,
        )
        .await
    }

    async fn merge_pod_labels(
        &self,
        namespace: &str,
        name: &str,
        labels: Vec<(String, String)>,
    ) -> anyhow::Result<Resource> {
        validate_controller_effect()?;
        crate::kubelet::pod_repository::PodObjectWriter::merge_pod_labels(
            self.repository.as_ref(),
            namespace,
            name,
            labels,
        )
        .await
    }

    async fn merge_pod_labels_for_uid(
        &self,
        namespace: &str,
        name: &str,
        expected_uid: &str,
        labels: Vec<(String, String)>,
    ) -> anyhow::Result<Resource> {
        validate_controller_effect()?;
        crate::kubelet::pod_repository::PodObjectWriter::merge_pod_labels_for_uid(
            self.repository.as_ref(),
            namespace,
            name,
            expected_uid,
            labels,
        )
        .await
    }
}

#[async_trait]
impl crate::kubelet::pod_repository::PodSubresourceWriter for RootControllerPodPort {
    async fn replace_status_from_api(
        &self,
        namespace: &str,
        name: &str,
        status: serde_json::Value,
        expected_resource_version: i64,
    ) -> anyhow::Result<Resource> {
        validate_controller_effect()?;
        self.subresource
            .replace_status(klights_pod_api::PodStatusReplaceRequest {
                namespace: namespace.to_string(),
                name: name.to_string(),
                expected_uid: None,
                status,
                expected_resource_version,
            })
            .await
            .map_err(anyhow::Error::new)
    }

    async fn replace_status_from_api_for_uid(
        &self,
        namespace: &str,
        name: &str,
        pod_uid: &str,
        status: serde_json::Value,
        expected_resource_version: i64,
    ) -> anyhow::Result<Resource> {
        validate_controller_effect()?;
        self.subresource
            .replace_status(klights_pod_api::PodStatusReplaceRequest {
                namespace: namespace.to_string(),
                name: name.to_string(),
                expected_uid: Some(pod_uid.to_string()),
                status,
                expected_resource_version,
            })
            .await
            .map_err(anyhow::Error::new)
    }

    async fn patch_status_from_api(
        &self,
        namespace: &str,
        name: &str,
        patch: serde_json::Value,
        patch_type: crate::kubelet::pod_repository::PodStatusPatchType,
        expected_resource_version: i64,
    ) -> anyhow::Result<Resource> {
        validate_controller_effect()?;
        let patch_type = match patch_type {
            crate::kubelet::pod_repository::PodStatusPatchType::JsonPatch => {
                klights_pod_api::PodStatusPatchKind::JsonPatch
            }
            crate::kubelet::pod_repository::PodStatusPatchType::MergePatch => {
                klights_pod_api::PodStatusPatchKind::MergePatch
            }
            crate::kubelet::pod_repository::PodStatusPatchType::StrategicMerge => {
                klights_pod_api::PodStatusPatchKind::StrategicMerge
            }
            crate::kubelet::pod_repository::PodStatusPatchType::ApplyPatch => {
                klights_pod_api::PodStatusPatchKind::ApplyPatch
            }
        };
        self.subresource
            .patch_status(klights_pod_api::PodStatusPatchRequest {
                namespace: namespace.to_string(),
                name: name.to_string(),
                patch,
                patch_kind: patch_type,
                expected_resource_version,
            })
            .await
            .map_err(anyhow::Error::new)
    }

    async fn update_ephemeral_containers(
        &self,
        namespace: &str,
        name: &str,
        containers: Vec<serde_json::Value>,
        expected_resource_version: i64,
    ) -> anyhow::Result<Resource> {
        validate_controller_effect()?;
        self.subresource
            .update_ephemeral_containers(klights_pod_api::PodEphemeralContainersRequest {
                namespace: namespace.to_string(),
                name: name.to_string(),
                containers,
                expected_resource_version,
            })
            .await
            .map_err(anyhow::Error::new)
    }
}

#[async_trait]
impl crate::controllers::node_lifecycle::NodeLifecyclePodStore for RootControllerPodPort {
    async fn list_pods_bound_to_node(&self, node_name: &str) -> Result<Vec<Resource>> {
        let field_selector = format!("spec.nodeName={node_name}");
        Ok(crate::kubelet::pod_repository::PodReader::list_pods(
            self.repository.as_ref(),
            None,
            None,
            Some(&field_selector),
            None,
            None,
        )
        .await
        .map_err(crate::controller_store_error_adapter::map_controller_store_error)?
        .items)
    }

    async fn replace_pod_status_for_uid(
        &self,
        pod: &Resource,
        status: serde_json::Value,
    ) -> Result<Resource> {
        validate_controller_effect()?;
        self.subresource
            .replace_status(klights_pod_api::PodStatusReplaceRequest {
                namespace: pod.namespace.as_deref().unwrap_or("default").to_string(),
                name: pod.name.clone(),
                expected_uid: Some(pod.uid.clone()),
                status,
                expected_resource_version: pod.resource_version,
            })
            .await
            .map_err(anyhow::Error::new)
            .map_err(crate::controller_store_error_adapter::map_controller_store_error)
    }
}

pub(crate) struct RootControllerReconcilePort {
    finalization: Arc<dyn klights_reconcile_api::GcNonPodFinalizationPort>,
}

impl RootControllerReconcilePort {
    pub(crate) fn new(
        finalization: Arc<dyn klights_reconcile_api::GcNonPodFinalizationPort>,
    ) -> Self {
        Self { finalization }
    }
}

impl ControllerReconcilePort for RootControllerReconcilePort {
    fn non_pod_finalization(&self) -> &dyn klights_reconcile_api::GcNonPodFinalizationPort {
        self.finalization.as_ref()
    }
}

pub(crate) struct RootControllerNetworkPort {
    services: Arc<dyn klights_network_api::ServiceRouter>,
}

impl RootControllerNetworkPort {
    pub(crate) fn new(services: Arc<dyn klights_network_api::ServiceRouter>) -> Self {
        Self { services }
    }
}

impl ControllerNetworkPort for RootControllerNetworkPort {
    fn service_router(&self) -> &dyn klights_network_api::ServiceRouter {
        self.services.as_ref()
    }
}

pub(crate) struct RootControllerEffectPort {
    file_process: klights_supervisor::FileProcessExecutor,
    local_path_provisioner_root: std::path::PathBuf,
}

impl RootControllerEffectPort {
    pub(crate) fn new(
        file_process: klights_supervisor::FileProcessExecutor,
        local_path_provisioner_root: std::path::PathBuf,
    ) -> Self {
        Self {
            file_process,
            local_path_provisioner_root,
        }
    }
}

impl ControllerEffectPort for RootControllerEffectPort {
    fn file_process(&self) -> &klights_supervisor::FileProcessExecutor {
        &self.file_process
    }

    fn local_path_provisioner_root(&self) -> &std::path::Path {
        &self.local_path_provisioner_root
    }
}
