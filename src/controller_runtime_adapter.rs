use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::{Resource, ResourceBatchOperation, ResourcePreconditions};

use crate::controllers::{
    ControllerEffectPort, ControllerLeaderPort, ControllerNetworkPort, ControllerPodPort,
    ControllerReconcilePort,
};
use crate::datastore::DatastoreHandle;
use crate::kubelet::pod_repository::PodRepository;

pub(crate) struct RootControllerLeaderPort {
    store: DatastoreHandle,
}

impl RootControllerLeaderPort {
    pub(crate) fn new(store: DatastoreHandle) -> Self {
        Self { store }
    }
}

#[async_trait]
impl ControllerLeaderPort for RootControllerLeaderPort {
    async fn get_reconcile_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        self.store
            .get_resource(api_version, kind, namespace, name)
            .await
    }

    async fn namespace_is_terminating(&self, namespace: &str) -> Result<bool> {
        Ok(self
            .store
            .get_namespace(namespace)
            .await?
            .is_some_and(|resource| {
                resource
                    .data
                    .pointer("/metadata/deletionTimestamp")
                    .and_then(serde_json::Value::as_str)
                    .is_some()
            }))
    }

    fn deployment_store(&self) -> &dyn crate::controllers::deployment::DeploymentStore {
        self
    }

    fn replicaset_store(&self) -> &dyn crate::controllers::replicaset::ReplicaSetStore {
        self
    }

    fn statefulset_store(&self) -> &dyn crate::controllers::statefulset::StatefulSetStore {
        self
    }

    fn daemonset_store(&self) -> &dyn crate::controllers::daemonset::DaemonSetStore {
        self
    }

    fn job_store(&self) -> &dyn crate::controllers::job::JobStore {
        self
    }

    fn service_store(&self) -> &dyn crate::controllers::service::ServiceControllerStore {
        self
    }

    fn pvc_store(&self) -> &dyn crate::controllers::pvc::PvcStore {
        self
    }

    fn pdb_store(&self) -> &dyn crate::controllers::pdb::PdbStore {
        self
    }

    fn replicationcontroller_store(
        &self,
    ) -> &dyn crate::controllers::replicationcontroller::ReplicationControllerStore {
        self
    }

    fn apiservice_store(&self) -> &dyn crate::controllers::apiservice::ApiServiceStore {
        self
    }

    fn csr_status_store(&self) -> &dyn crate::controllers::csr_signer::CsrStatusStore {
        self
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

    fn gc_store_error_is_conflict(&self, error: &anyhow::Error) -> bool {
        crate::controllers::gc::GcResourceStore::gc_store_error_is_conflict(
            self.store.as_ref(),
            error,
        )
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
        crate::controllers::job::JobStore::update_job_status(self.store.as_ref(), resource, status)
            .await
    }
}

#[async_trait]
impl crate::controllers::service::ServiceReconcileStore for RootControllerLeaderPort {
    async fn list_services(&self) -> Result<Vec<Resource>> {
        crate::controllers::service::ServiceReconcileStore::list_services(self.store.as_ref()).await
    }

    async fn get_service(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        crate::controllers::service::ServiceReconcileStore::get_service(
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
        crate::controllers::service::ServiceReconcileStore::update_service(
            self.store.as_ref(),
            namespace,
            name,
            data,
            preconditions,
        )
        .await
    }

    fn service_store_error_is_conflict(&self, error: &anyhow::Error) -> bool {
        crate::controllers::service::ServiceReconcileStore::service_store_error_is_conflict(
            self.store.as_ref(),
            error,
        )
    }
}

#[async_trait]
impl crate::controllers::endpoints::EndpointReconcileStore for RootControllerLeaderPort {
    async fn endpoint_namespace_is_terminating(&self, namespace: &str) -> Result<bool> {
        crate::controllers::endpoints::EndpointReconcileStore::endpoint_namespace_is_terminating(
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
        crate::controllers::endpoints::EndpointReconcileStore::get_resource(
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
        crate::controllers::endpoints::EndpointReconcileStore::list_service_endpoint_slices(
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
        crate::controllers::endpoints::EndpointReconcileStore::create_resource(
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
        crate::controllers::endpoints::EndpointReconcileStore::update_resource_with_preconditions(
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
        crate::controllers::endpoints::EndpointReconcileStore::delete_resource_with_preconditions(
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
        crate::controllers::endpoints::EndpointReconcileStore::apply_resource_batch(
            self.store.as_ref(),
            operations,
        )
        .await
    }

    fn endpoint_store_error_is_conflict(&self, error: &anyhow::Error) -> bool {
        crate::controllers::endpoints::EndpointReconcileStore::endpoint_store_error_is_conflict(
            self.store.as_ref(),
            error,
        )
    }

    fn endpoint_store_error_is_already_exists(&self, error: &anyhow::Error) -> bool {
        crate::controllers::endpoints::EndpointReconcileStore::endpoint_store_error_is_already_exists(
            self.store.as_ref(),
            error,
        )
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

    fn is_conflict(&self, error: &anyhow::Error) -> bool {
        crate::controllers::common::ControllerStatusStore::is_conflict(self.store.as_ref(), error)
    }

    fn conflict_error(&self, message: &'static str) -> anyhow::Error {
        crate::controllers::common::ControllerStatusStore::conflict_error(
            self.store.as_ref(),
            message,
        )
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
    ) -> std::result::Result<(), crate::controllers::apiservice::ApiServiceStatusWriteError> {
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
}

impl RootControllerPodPort {
    pub(crate) fn new(repository: Arc<PodRepository>) -> Self {
        Self { repository }
    }
}

impl ControllerPodPort for RootControllerPodPort {
    fn query(&self) -> &dyn klights_pod_api::PodQuery {
        self.repository.as_ref()
    }

    fn pdb_reader(&self) -> &dyn crate::controllers::pdb::PdbPodReader {
        self.repository.as_ref()
    }

    fn deployment_reader(&self) -> &dyn crate::controllers::DeploymentControllerPodReader {
        self.repository.as_ref()
    }

    fn deployment_mutation(&self) -> &dyn crate::controllers::DeploymentControllerPodMutation {
        self.repository.as_ref()
    }

    fn replicaset_mutation(&self) -> &dyn crate::controllers::replicaset::ReplicaSetPodMutation {
        self.repository.as_ref()
    }

    fn statefulset_mutation(&self) -> &dyn crate::controllers::statefulset::StatefulSetPodMutation {
        self.repository.as_ref()
    }

    fn daemonset_mutation(&self) -> &dyn crate::controllers::daemonset::DaemonSetPodMutation {
        self.repository.as_ref()
    }

    fn job_mutation(&self) -> &dyn crate::controllers::job::JobPodMutation {
        self.repository.as_ref()
    }

    fn replicationcontroller_mutation(
        &self,
    ) -> &dyn crate::controllers::replicationcontroller::ReplicationControllerPodMutation {
        self.repository.as_ref()
    }

    fn delete_sink(&self) -> &dyn klights_reconcile_api::GcPodDeleteSink {
        self.repository.as_ref()
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
}

impl RootControllerEffectPort {
    pub(crate) fn new(file_process: klights_supervisor::FileProcessExecutor) -> Self {
        Self { file_process }
    }
}

impl ControllerEffectPort for RootControllerEffectPort {
    fn file_process(&self) -> &klights_supervisor::FileProcessExecutor {
        &self.file_process
    }
}
