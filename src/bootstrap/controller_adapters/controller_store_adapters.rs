use async_trait::async_trait;
use klights_cluster_core::{PatchKind, Resource, ResourcePreconditions};
use klights_cluster_datastore::diagnostics::{NoopResourceWrite, log_noop_resource_write};
use serde_json::json;

use crate::bootstrap::controller_adapters::controller_store_error_adapter::map_controller_store_error;
use crate::datastore::{DatastoreBackend, ResourceListQuery, ResourcePatchRequest};
use klights_controllers::apiservice::ApiServiceStore;
use klights_controllers::common::ControllerStatusStore;
use klights_controllers::cronjob::CronJobStore;
use klights_controllers::csr_signer::CsrStatusStore;
use klights_controllers::deployment::DeploymentFinalizeStore;
use klights_controllers::kube_service::KubernetesBootstrapStore;
use klights_controllers::namespace::NamespaceBootstrapStore;
use klights_controllers::pdb::PdbStore;
use klights_controllers::pvc::PvcStore;
use klights_controllers::rbac_reconcile::RbacPolicyStore;
use klights_reconcile_api::ControllerStoreResult;

#[cfg(test)]
#[path = "../../controller_policy_tests/kube_service.rs"]
mod kube_service_policy_tests;
#[cfg(test)]
#[path = "../../controller_policy_tests/namespace.rs"]
mod namespace_policy_tests;
#[cfg(test)]
#[path = "../../controller_policy_tests/rbac_reconcile.rs"]
mod rbac_reconcile_policy_tests;

#[async_trait]
impl ApiServiceStore for dyn DatastoreBackend + '_ {
    async fn get_apiservice(&self, name: &str) -> ControllerStoreResult<Option<Resource>> {
        self.get_resource("apiregistration.k8s.io/v1", "APIService", None, name)
            .await
            .map_err(map_controller_store_error)
    }

    async fn service_exists(&self, namespace: &str, name: &str) -> ControllerStoreResult<bool> {
        self.get_resource("v1", "Service", Some(namespace), name)
            .await
            .map(|service| service.is_some())
            .map_err(map_controller_store_error)
    }

    async fn list_endpoint_slices(
        &self,
        namespace: &str,
        service_name: &str,
    ) -> ControllerStoreResult<Vec<Resource>> {
        let selector = format!("kubernetes.io/service-name={service_name}");
        self.list_resources(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some(namespace),
            ResourceListQuery::new(Some(&selector), None, None, None),
        )
        .await
        .map(|listing| listing.items)
        .map_err(map_controller_store_error)
    }

    async fn get_endpoints(
        &self,
        namespace: &str,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        self.get_resource("v1", "Endpoints", Some(namespace), name)
            .await
            .map_err(map_controller_store_error)
    }

    async fn update_apiservice_status(
        &self,
        current: &Resource,
        status: serde_json::Value,
    ) -> ControllerStoreResult<()> {
        self.update_status_only_with_preconditions(
            "apiregistration.k8s.io/v1",
            "APIService",
            None,
            current.name.as_str(),
            status,
            ResourcePreconditions::from_resource(current),
        )
        .await
        .map(|_| ())
        .map_err(map_controller_store_error)
    }
}

#[async_trait]
impl DeploymentFinalizeStore for dyn DatastoreBackend + '_ {
    async fn get_deployment(
        &self,
        namespace: &str,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        self.get_resource("apps/v1", "Deployment", Some(namespace), name)
            .await
            .map_err(map_controller_store_error)
    }

    async fn patch_deployment_revision(
        &self,
        namespace: &str,
        name: &str,
        revision: String,
        expected_uid: String,
    ) -> ControllerStoreResult<()> {
        self.patch_resource_latest_with_preconditions(
            "apps/v1",
            "Deployment",
            Some(namespace),
            name,
            ResourcePatchRequest::new(
                PatchKind::Merge,
                json!({
                    "metadata": {
                        "annotations": {
                            "deployment.kubernetes.io/revision": revision
                        }
                    }
                }),
                ResourcePreconditions::uid(expected_uid),
            ),
        )
        .await
        .map(|_| ())
        .map_err(map_controller_store_error)
    }

    async fn delete_replicaset(
        &self,
        namespace: &str,
        name: &str,
        expected_uid: String,
    ) -> ControllerStoreResult<()> {
        self.delete_resource_with_preconditions(
            "apps/v1",
            "ReplicaSet",
            Some(namespace),
            name,
            ResourcePreconditions::uid(expected_uid),
        )
        .await
        .map(|_| ())
        .map_err(map_controller_store_error)
    }
}

#[async_trait]
impl KubernetesBootstrapStore for dyn DatastoreBackend + '_ {
    async fn get_bootstrap_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        self.get_resource(api_version, kind, namespace, name)
            .await
            .map_err(map_controller_store_error)
    }

    async fn create_bootstrap_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
    ) -> ControllerStoreResult<Resource> {
        self.create_resource(api_version, kind, namespace, name, value)
            .await
            .map_err(map_controller_store_error)
    }

    async fn update_bootstrap_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
        expected_resource_version: i64,
    ) -> ControllerStoreResult<Resource> {
        self.update_resource(
            api_version,
            kind,
            namespace,
            name,
            value,
            expected_resource_version,
        )
        .await
        .map_err(map_controller_store_error)
    }
}

#[async_trait]
impl CsrStatusStore for dyn DatastoreBackend + '_ {
    async fn get_csr(&self, name: &str) -> ControllerStoreResult<Option<Resource>> {
        self.get_resource(
            "certificates.k8s.io/v1",
            "CertificateSigningRequest",
            None,
            name,
        )
        .await
        .map_err(map_controller_store_error)
    }

    async fn update_csr_status(
        &self,
        name: &str,
        uid: &str,
        resource_version: i64,
        status: serde_json::Value,
    ) -> ControllerStoreResult<()> {
        self.update_status_only_with_preconditions(
            "certificates.k8s.io/v1",
            "CertificateSigningRequest",
            None,
            name,
            status,
            ResourcePreconditions {
                resource_version: Some(resource_version),
                uid: Some(uid.to_string()),
            },
        )
        .await
        .map(|_| ())
        .map_err(map_controller_store_error)
    }
}

#[async_trait]
impl ControllerStatusStore for dyn DatastoreBackend + '_ {
    async fn get_status_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        self.get_resource(api_version, kind, namespace, name)
            .await
            .map_err(map_controller_store_error)
    }

    async fn update_status(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> ControllerStoreResult<Resource> {
        self.update_status_only_with_preconditions(
            api_version,
            kind,
            namespace,
            name,
            status,
            preconditions,
        )
        .await
        .map_err(map_controller_store_error)
    }

    fn log_noop_status_write(
        &self,
        operation: &'static str,
        resource: &Resource,
        reason: &'static str,
    ) {
        log_noop_resource_write(NoopResourceWrite {
            operation,
            api_version: &resource.api_version,
            kind: &resource.kind,
            namespace: resource.namespace.as_deref(),
            name: &resource.name,
            uid: &resource.uid,
            resource_version: resource.resource_version,
            reason,
        });
    }
}

#[cfg(test)]
#[async_trait]
impl ControllerStatusStore for crate::datastore::sqlite::Datastore {
    async fn get_status_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        self.get_resource(api_version, kind, namespace, name)
            .await
            .map_err(map_controller_store_error)
    }

    async fn update_status(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> ControllerStoreResult<Resource> {
        self.update_status_only_with_preconditions(
            api_version,
            kind,
            namespace,
            name,
            status,
            preconditions,
        )
        .await
        .map_err(map_controller_store_error)
    }

    fn log_noop_status_write(
        &self,
        operation: &'static str,
        resource: &Resource,
        reason: &'static str,
    ) {
        let backend: &dyn DatastoreBackend = self;
        ControllerStatusStore::log_noop_status_write(backend, operation, resource, reason);
    }
}

#[async_trait]
impl RbacPolicyStore for dyn DatastoreBackend + '_ {
    async fn get_rbac_object(
        &self,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        self.get_resource("rbac.authorization.k8s.io/v1", kind, namespace, name)
            .await
            .map_err(map_controller_store_error)
    }

    async fn create_rbac_object(
        &self,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
    ) -> ControllerStoreResult<Resource> {
        self.create_resource("rbac.authorization.k8s.io/v1", kind, namespace, name, value)
            .await
            .map_err(map_controller_store_error)
    }

    async fn update_rbac_object(
        &self,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
        expected_resource_version: i64,
    ) -> ControllerStoreResult<Resource> {
        self.update_resource(
            "rbac.authorization.k8s.io/v1",
            kind,
            namespace,
            name,
            value,
            expected_resource_version,
        )
        .await
        .map_err(map_controller_store_error)
    }

    async fn list_cluster_roles(&self) -> ControllerStoreResult<Vec<Resource>> {
        self.list_resources_page(
            "rbac.authorization.k8s.io/v1",
            "ClusterRole",
            None,
            None,
            None,
            crate::datastore::types::ListPageRequest::unbounded(),
        )
        .await
        .map(|listing| listing.items)
        .map_err(map_controller_store_error)
    }
}

#[async_trait]
impl NamespaceBootstrapStore for dyn DatastoreBackend + '_ {
    async fn get_namespace(&self, name: &str) -> ControllerStoreResult<Option<Resource>> {
        DatastoreBackend::get_namespace(self, name)
            .await
            .map_err(map_controller_store_error)
    }

    async fn create_namespace(
        &self,
        name: &str,
        value: serde_json::Value,
    ) -> ControllerStoreResult<Resource> {
        DatastoreBackend::create_namespace(self, name, value)
            .await
            .map_err(map_controller_store_error)
    }

    async fn get_default_service_account(
        &self,
        namespace: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        self.get_resource("v1", "ServiceAccount", Some(namespace), "default")
            .await
            .map_err(map_controller_store_error)
    }

    async fn create_default_service_account(
        &self,
        namespace: &str,
        value: serde_json::Value,
    ) -> ControllerStoreResult<Resource> {
        self.create_resource("v1", "ServiceAccount", Some(namespace), "default", value)
            .await
            .map_err(map_controller_store_error)
    }

    async fn get_configmap(
        &self,
        namespace: &str,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        self.get_resource("v1", "ConfigMap", Some(namespace), name)
            .await
            .map_err(map_controller_store_error)
    }

    async fn create_configmap(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
    ) -> ControllerStoreResult<Resource> {
        self.create_resource("v1", "ConfigMap", Some(namespace), name, value)
            .await
            .map_err(map_controller_store_error)
    }

    async fn update_configmap(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
        expected_resource_version: i64,
    ) -> ControllerStoreResult<Resource> {
        self.update_resource(
            "v1",
            "ConfigMap",
            Some(namespace),
            name,
            value,
            expected_resource_version,
        )
        .await
        .map_err(map_controller_store_error)
    }
}

#[cfg(test)]
#[async_trait]
impl NamespaceBootstrapStore for crate::datastore::sqlite::Datastore {
    async fn get_namespace(&self, name: &str) -> ControllerStoreResult<Option<Resource>> {
        DatastoreBackend::get_namespace(self, name)
            .await
            .map_err(map_controller_store_error)
    }

    async fn create_namespace(
        &self,
        name: &str,
        value: serde_json::Value,
    ) -> ControllerStoreResult<Resource> {
        DatastoreBackend::create_namespace(self, name, value)
            .await
            .map_err(map_controller_store_error)
    }

    async fn get_default_service_account(
        &self,
        namespace: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        self.get_resource("v1", "ServiceAccount", Some(namespace), "default")
            .await
            .map_err(map_controller_store_error)
    }

    async fn create_default_service_account(
        &self,
        namespace: &str,
        value: serde_json::Value,
    ) -> ControllerStoreResult<Resource> {
        self.create_resource("v1", "ServiceAccount", Some(namespace), "default", value)
            .await
            .map_err(map_controller_store_error)
    }

    async fn get_configmap(
        &self,
        namespace: &str,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        self.get_resource("v1", "ConfigMap", Some(namespace), name)
            .await
            .map_err(map_controller_store_error)
    }

    async fn create_configmap(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
    ) -> ControllerStoreResult<Resource> {
        self.create_resource("v1", "ConfigMap", Some(namespace), name, value)
            .await
            .map_err(map_controller_store_error)
    }

    async fn update_configmap(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
        expected_resource_version: i64,
    ) -> ControllerStoreResult<Resource> {
        self.update_resource(
            "v1",
            "ConfigMap",
            Some(namespace),
            name,
            value,
            expected_resource_version,
        )
        .await
        .map_err(map_controller_store_error)
    }
}

#[async_trait]
impl CronJobStore for dyn DatastoreBackend + '_ {
    async fn get_cronjob(
        &self,
        namespace: &str,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        self.get_resource("batch/v1", "CronJob", Some(namespace), name)
            .await
            .map_err(map_controller_store_error)
    }

    async fn get_job(
        &self,
        namespace: &str,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        self.get_resource("batch/v1", "Job", Some(namespace), name)
            .await
            .map_err(map_controller_store_error)
    }

    async fn create_job(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
    ) -> ControllerStoreResult<Resource> {
        self.create_resource("batch/v1", "Job", Some(namespace), name, value)
            .await
            .map_err(map_controller_store_error)
    }

    async fn list_jobs(&self, namespace: &str) -> ControllerStoreResult<Vec<Resource>> {
        self.list_resources("batch/v1", "Job", Some(namespace), ResourceListQuery::all())
            .await
            .map(|listing| listing.items)
            .map_err(map_controller_store_error)
    }

    async fn delete_job(
        &self,
        namespace: &str,
        name: &str,
        uid: String,
    ) -> ControllerStoreResult<()> {
        self.delete_resource_with_preconditions(
            "batch/v1",
            "Job",
            Some(namespace),
            name,
            ResourcePreconditions::uid(uid),
        )
        .await
        .map(|_| ())
        .map_err(map_controller_store_error)
    }
}

#[cfg(test)]
#[async_trait]
impl CronJobStore for crate::datastore::sqlite::Datastore {
    async fn get_cronjob(
        &self,
        namespace: &str,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        self.get_resource("batch/v1", "CronJob", Some(namespace), name)
            .await
            .map_err(map_controller_store_error)
    }

    async fn get_job(
        &self,
        namespace: &str,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        self.get_resource("batch/v1", "Job", Some(namespace), name)
            .await
            .map_err(map_controller_store_error)
    }

    async fn create_job(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
    ) -> ControllerStoreResult<Resource> {
        self.create_resource("batch/v1", "Job", Some(namespace), name, value)
            .await
            .map_err(map_controller_store_error)
    }

    async fn list_jobs(&self, namespace: &str) -> ControllerStoreResult<Vec<Resource>> {
        self.list_resources("batch/v1", "Job", Some(namespace), ResourceListQuery::all())
            .await
            .map(|listing| listing.items)
            .map_err(map_controller_store_error)
    }

    async fn delete_job(
        &self,
        namespace: &str,
        name: &str,
        uid: String,
    ) -> ControllerStoreResult<()> {
        self.delete_resource_with_preconditions(
            "batch/v1",
            "Job",
            Some(namespace),
            name,
            ResourcePreconditions::uid(uid),
        )
        .await
        .map(|_| ())
        .map_err(map_controller_store_error)
    }
}

#[async_trait]
impl PvcStore for dyn DatastoreBackend + '_ {
    async fn get_pvc(
        &self,
        namespace: &str,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        self.get_resource("v1", "PersistentVolumeClaim", Some(namespace), name)
            .await
            .map_err(map_controller_store_error)
    }

    async fn list_persistent_volumes(&self) -> ControllerStoreResult<Vec<Resource>> {
        self.list_resources("v1", "PersistentVolume", None, ResourceListQuery::all())
            .await
            .map(|listing| listing.items)
            .map_err(map_controller_store_error)
    }

    async fn get_persistent_volume(&self, name: &str) -> ControllerStoreResult<Option<Resource>> {
        self.get_resource("v1", "PersistentVolume", None, name)
            .await
            .map_err(map_controller_store_error)
    }

    async fn create_persistent_volume(
        &self,
        name: &str,
        value: serde_json::Value,
    ) -> ControllerStoreResult<Resource> {
        self.create_resource("v1", "PersistentVolume", None, name, value)
            .await
            .map_err(map_controller_store_error)
    }

    async fn update_persistent_volume(
        &self,
        name: &str,
        value: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> ControllerStoreResult<Resource> {
        self.update_resource_with_preconditions(
            "v1",
            "PersistentVolume",
            None,
            name,
            value,
            preconditions,
        )
        .await
        .map_err(map_controller_store_error)
    }
}

#[cfg(test)]
#[async_trait]
impl PvcStore for crate::datastore::sqlite::Datastore {
    async fn get_pvc(
        &self,
        namespace: &str,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>> {
        self.get_resource("v1", "PersistentVolumeClaim", Some(namespace), name)
            .await
            .map_err(map_controller_store_error)
    }

    async fn list_persistent_volumes(&self) -> ControllerStoreResult<Vec<Resource>> {
        self.list_resources("v1", "PersistentVolume", None, ResourceListQuery::all())
            .await
            .map(|listing| listing.items)
            .map_err(map_controller_store_error)
    }

    async fn get_persistent_volume(&self, name: &str) -> ControllerStoreResult<Option<Resource>> {
        self.get_resource("v1", "PersistentVolume", None, name)
            .await
            .map_err(map_controller_store_error)
    }

    async fn create_persistent_volume(
        &self,
        name: &str,
        value: serde_json::Value,
    ) -> ControllerStoreResult<Resource> {
        self.create_resource("v1", "PersistentVolume", None, name, value)
            .await
            .map_err(map_controller_store_error)
    }

    async fn update_persistent_volume(
        &self,
        name: &str,
        value: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> ControllerStoreResult<Resource> {
        self.update_resource_with_preconditions(
            "v1",
            "PersistentVolume",
            None,
            name,
            value,
            preconditions,
        )
        .await
        .map_err(map_controller_store_error)
    }
}

#[async_trait]
impl PdbStore for dyn DatastoreBackend + '_ {
    async fn list_pdbs(&self, namespace: &str) -> ControllerStoreResult<Vec<Resource>> {
        self.list_resources(
            "policy/v1",
            "PodDisruptionBudget",
            Some(namespace),
            ResourceListQuery::all(),
        )
        .await
        .map(|listing| listing.items)
        .map_err(map_controller_store_error)
    }
}

#[cfg(test)]
#[async_trait]
impl PdbStore for crate::datastore::sqlite::Datastore {
    async fn list_pdbs(&self, namespace: &str) -> ControllerStoreResult<Vec<Resource>> {
        self.list_resources(
            "policy/v1",
            "PodDisruptionBudget",
            Some(namespace),
            ResourceListQuery::all(),
        )
        .await
        .map(|listing| listing.items)
        .map_err(map_controller_store_error)
    }
}
