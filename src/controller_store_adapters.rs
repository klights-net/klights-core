use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::{PatchKind, Resource, ResourcePreconditions};
use serde_json::json;

use crate::controllers::apiservice::{ApiServiceStatusWriteError, ApiServiceStore};
use crate::controllers::common::ControllerStatusStore;
use crate::controllers::cronjob::CronJobStore;
use crate::controllers::csr_signer::CsrStatusStore;
use crate::controllers::deployment::DeploymentFinalizeStore;
use crate::controllers::kube_service::KubernetesBootstrapStore;
use crate::controllers::namespace::NamespaceBootstrapStore;
use crate::controllers::pdb::PdbStore;
use crate::controllers::pvc::PvcStore;
use crate::controllers::rbac_reconcile::RbacPolicyStore;
use crate::datastore::{DatastoreBackend, ResourceListQuery, ResourcePatchRequest};

#[async_trait]
impl<T> ApiServiceStore for T
where
    T: DatastoreBackend + ?Sized,
{
    async fn get_apiservice(&self, name: &str) -> Result<Option<Resource>> {
        self.get_resource("apiregistration.k8s.io/v1", "APIService", None, name)
            .await
    }

    async fn service_exists(&self, namespace: &str, name: &str) -> Result<bool> {
        self.get_resource("v1", "Service", Some(namespace), name)
            .await
            .map(|service| service.is_some())
    }

    async fn list_endpoint_slices(
        &self,
        namespace: &str,
        service_name: &str,
    ) -> Result<Vec<Resource>> {
        let selector = format!("kubernetes.io/service-name={service_name}");
        self.list_resources(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some(namespace),
            ResourceListQuery::new(Some(&selector), None, None, None),
        )
        .await
        .map(|listing| listing.items)
    }

    async fn get_endpoints(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        self.get_resource("v1", "Endpoints", Some(namespace), name)
            .await
    }

    async fn update_apiservice_status(
        &self,
        current: &Resource,
        status: serde_json::Value,
    ) -> std::result::Result<(), ApiServiceStatusWriteError> {
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
        .map_err(|error| {
            if crate::datastore::errors::is_conflict_error(&error) {
                ApiServiceStatusWriteError::Conflict(error)
            } else {
                ApiServiceStatusWriteError::Other(error)
            }
        })
    }
}

#[async_trait]
impl<T> DeploymentFinalizeStore for T
where
    T: DatastoreBackend + ?Sized,
{
    async fn get_deployment(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        self.get_resource("apps/v1", "Deployment", Some(namespace), name)
            .await
    }

    async fn patch_deployment_revision(
        &self,
        namespace: &str,
        name: &str,
        revision: String,
        expected_uid: String,
    ) -> Result<()> {
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
    }

    async fn delete_replicaset(
        &self,
        namespace: &str,
        name: &str,
        expected_uid: String,
    ) -> Result<()> {
        self.delete_resource_with_preconditions(
            "apps/v1",
            "ReplicaSet",
            Some(namespace),
            name,
            ResourcePreconditions::uid(expected_uid),
        )
        .await
        .map(|_| ())
    }
}

#[async_trait]
impl<T> KubernetesBootstrapStore for T
where
    T: DatastoreBackend + ?Sized,
{
    async fn get_bootstrap_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        self.get_resource(api_version, kind, namespace, name).await
    }

    async fn create_bootstrap_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
    ) -> Result<Resource> {
        self.create_resource(api_version, kind, namespace, name, value)
            .await
    }

    async fn update_bootstrap_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
        expected_resource_version: i64,
    ) -> Result<Resource> {
        self.update_resource(
            api_version,
            kind,
            namespace,
            name,
            value,
            expected_resource_version,
        )
        .await
    }
}

#[async_trait]
impl<T> CsrStatusStore for T
where
    T: DatastoreBackend + ?Sized,
{
    async fn get_csr(&self, name: &str) -> Result<Option<Resource>> {
        self.get_resource(
            "certificates.k8s.io/v1",
            "CertificateSigningRequest",
            None,
            name,
        )
        .await
    }

    async fn update_csr_status(
        &self,
        name: &str,
        uid: &str,
        resource_version: i64,
        status: serde_json::Value,
    ) -> Result<()> {
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
    }
}

#[async_trait]
impl<T> ControllerStatusStore for T
where
    T: DatastoreBackend + ?Sized,
{
    async fn get_status_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        self.get_resource(api_version, kind, namespace, name).await
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
        self.update_status_only_with_preconditions(
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
        crate::datastore::errors::is_conflict_error(error)
    }

    fn conflict_error(&self, message: &'static str) -> anyhow::Error {
        crate::datastore::errors::DatastoreError::conflict(message).into()
    }

    fn log_noop_status_write(
        &self,
        operation: &'static str,
        resource: &Resource,
        reason: &'static str,
    ) {
        crate::datastore::diagnostics::log_noop_resource_write(
            crate::datastore::diagnostics::NoopResourceWrite {
                operation,
                api_version: &resource.api_version,
                kind: &resource.kind,
                namespace: resource.namespace.as_deref(),
                name: &resource.name,
                uid: &resource.uid,
                resource_version: resource.resource_version,
                reason,
            },
        );
    }
}

#[async_trait]
impl<T> RbacPolicyStore for T
where
    T: DatastoreBackend + ?Sized,
{
    async fn get_rbac_object(
        &self,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        self.get_resource("rbac.authorization.k8s.io/v1", kind, namespace, name)
            .await
    }

    async fn create_rbac_object(
        &self,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
    ) -> Result<Resource> {
        self.create_resource("rbac.authorization.k8s.io/v1", kind, namespace, name, value)
            .await
    }

    async fn update_rbac_object(
        &self,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
        expected_resource_version: i64,
    ) -> Result<Resource> {
        self.update_resource(
            "rbac.authorization.k8s.io/v1",
            kind,
            namespace,
            name,
            value,
            expected_resource_version,
        )
        .await
    }

    async fn list_cluster_roles(&self) -> Result<Vec<Resource>> {
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
    }
}

#[async_trait]
impl<T> NamespaceBootstrapStore for T
where
    T: DatastoreBackend + ?Sized,
{
    async fn get_namespace(&self, name: &str) -> Result<Option<Resource>> {
        DatastoreBackend::get_namespace(self, name).await
    }

    async fn create_namespace(&self, name: &str, value: serde_json::Value) -> Result<Resource> {
        DatastoreBackend::create_namespace(self, name, value).await
    }

    async fn get_default_service_account(&self, namespace: &str) -> Result<Option<Resource>> {
        self.get_resource("v1", "ServiceAccount", Some(namespace), "default")
            .await
    }

    async fn create_default_service_account(
        &self,
        namespace: &str,
        value: serde_json::Value,
    ) -> Result<Resource> {
        self.create_resource("v1", "ServiceAccount", Some(namespace), "default", value)
            .await
    }

    async fn get_configmap(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        self.get_resource("v1", "ConfigMap", Some(namespace), name)
            .await
    }

    async fn create_configmap(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
    ) -> Result<Resource> {
        self.create_resource("v1", "ConfigMap", Some(namespace), name, value)
            .await
    }

    async fn update_configmap(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
        expected_resource_version: i64,
    ) -> Result<Resource> {
        self.update_resource(
            "v1",
            "ConfigMap",
            Some(namespace),
            name,
            value,
            expected_resource_version,
        )
        .await
    }
}

#[async_trait]
impl<T> CronJobStore for T
where
    T: DatastoreBackend + ?Sized,
{
    async fn get_cronjob(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        self.get_resource("batch/v1", "CronJob", Some(namespace), name)
            .await
    }

    async fn get_job(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        self.get_resource("batch/v1", "Job", Some(namespace), name)
            .await
    }

    async fn create_job(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
    ) -> Result<Resource> {
        self.create_resource("batch/v1", "Job", Some(namespace), name, value)
            .await
    }

    async fn list_jobs(&self, namespace: &str) -> Result<Vec<Resource>> {
        self.list_resources("batch/v1", "Job", Some(namespace), ResourceListQuery::all())
            .await
            .map(|listing| listing.items)
    }

    async fn delete_job(&self, namespace: &str, name: &str, uid: String) -> Result<()> {
        self.delete_resource_with_preconditions(
            "batch/v1",
            "Job",
            Some(namespace),
            name,
            ResourcePreconditions::uid(uid),
        )
        .await
        .map(|_| ())
    }
}

#[async_trait]
impl<T> PvcStore for T
where
    T: DatastoreBackend + ?Sized,
{
    async fn get_pvc(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        self.get_resource("v1", "PersistentVolumeClaim", Some(namespace), name)
            .await
    }

    async fn list_persistent_volumes(&self) -> Result<Vec<Resource>> {
        self.list_resources("v1", "PersistentVolume", None, ResourceListQuery::all())
            .await
            .map(|listing| listing.items)
    }

    async fn get_persistent_volume(&self, name: &str) -> Result<Option<Resource>> {
        self.get_resource("v1", "PersistentVolume", None, name)
            .await
    }

    async fn create_persistent_volume(
        &self,
        name: &str,
        value: serde_json::Value,
    ) -> Result<Resource> {
        self.create_resource("v1", "PersistentVolume", None, name, value)
            .await
    }

    async fn update_persistent_volume(
        &self,
        name: &str,
        value: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        self.update_resource_with_preconditions(
            "v1",
            "PersistentVolume",
            None,
            name,
            value,
            preconditions,
        )
        .await
    }
}

#[async_trait]
impl<T> PdbStore for T
where
    T: DatastoreBackend + ?Sized,
{
    async fn list_pdbs(&self, namespace: &str) -> Result<Vec<Resource>> {
        self.list_resources(
            "policy/v1",
            "PodDisruptionBudget",
            Some(namespace),
            ResourceListQuery::all(),
        )
        .await
        .map(|listing| listing.items)
    }
}
