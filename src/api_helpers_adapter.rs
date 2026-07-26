use async_trait::async_trait;
use serde_json::Value;

use crate::api::{
    AdmissionResourceStore, AppError, NamespaceTerminationMetrics, NamespaceTerminationStore,
};
use crate::datastore::{DatastoreBackend, ResourceListQuery};

#[async_trait]
impl<T> NamespaceTerminationStore for T
where
    T: DatastoreBackend + ?Sized,
{
    async fn get_terminating_namespace(
        &self,
        namespace: &str,
    ) -> Result<Option<klights_cluster_core::Resource>, AppError> {
        Ok(self.get_namespace(namespace).await?)
    }

    async fn list_namespace_pods(
        &self,
        namespace: &str,
    ) -> Result<Vec<klights_cluster_core::Resource>, AppError> {
        Ok(self
            .list_namespace_resources_of_kind(namespace, "Pod")
            .await?)
    }

    async fn mark_namespace_pod_terminating(
        &self,
        pod: &klights_cluster_core::Resource,
        namespace: &str,
        body: Value,
    ) -> Result<(), AppError> {
        self.update_resource_with_preconditions(
            &pod.api_version,
            &pod.kind,
            Some(namespace),
            &pod.name,
            body,
            klights_cluster_core::ResourcePreconditions::from_resource(pod),
        )
        .await?;
        Ok(())
    }

    async fn update_terminating_namespace(
        &self,
        namespace: &str,
        body: Value,
        expected_resource_version: i64,
    ) -> Result<klights_cluster_core::Resource, AppError> {
        Ok(self
            .update_namespace(namespace, body, expected_resource_version)
            .await?)
    }

    async fn list_namespace_non_pod_resources(
        &self,
        namespace: &str,
    ) -> Result<Vec<klights_cluster_core::Resource>, AppError> {
        Ok(self
            .list_namespace_resources_excluding_kind(namespace, "Pod")
            .await?)
    }

    async fn delete_namespace_non_pod_resource(
        &self,
        resource: &klights_cluster_core::Resource,
        namespace: &str,
    ) -> Result<(), AppError> {
        self.delete_resource(
            &resource.api_version,
            &resource.kind,
            Some(namespace),
            &resource.name,
        )
        .await?;
        Ok(())
    }

    async fn count_namespace_resources(&self, namespace: &str) -> Result<i64, AppError> {
        Ok(DatastoreBackend::count_namespace_resources(self, namespace).await?)
    }

    async fn delete_terminating_namespace(&self, namespace: &str) -> anyhow::Result<()> {
        self.delete_namespace(namespace).await
    }
}

impl NamespaceTerminationMetrics for crate::side_effects::SideEffectMetrics {
    fn record_namespace_delete_failure(&self) {
        self.namespace_delete_failures_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

impl<T> NamespaceTerminationMetrics for std::sync::Arc<T>
where
    T: NamespaceTerminationMetrics + ?Sized,
{
    fn record_namespace_delete_failure(&self) {
        self.as_ref().record_namespace_delete_failure();
    }
}

#[async_trait]
impl<T> AdmissionResourceStore for T
where
    T: DatastoreBackend + ?Sized,
{
    async fn get_admission_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<klights_cluster_core::Resource>, AppError> {
        Ok(self
            .get_resource(api_version, kind, namespace, name)
            .await?)
    }

    async fn list_admission_resources(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<klights_cluster_core::Resource>, AppError> {
        Ok(self
            .list_resources(api_version, kind, namespace, ResourceListQuery::all())
            .await?
            .items)
    }
}
