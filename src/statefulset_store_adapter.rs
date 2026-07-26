use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;

use crate::controllers::statefulset::{StatefulSetPodMutation, StatefulSetStore};
use crate::datastore::DatastoreBackend;
use crate::kubelet::pod_repository::PodObjectWriter;

#[async_trait]
impl<T> StatefulSetPodMutation for T
where
    T: PodObjectWriter + Send + Sync + ?Sized,
{
    async fn create_statefulset_pod(
        &self,
        namespace: &str,
        name: &str,
        node_name: &str,
        pod: serde_json::Value,
    ) -> Result<Resource> {
        PodObjectWriter::create_controller_pod(self, namespace, name, node_name, pod).await
    }
}

#[async_trait]
impl<T> StatefulSetStore for T
where
    T: DatastoreBackend + Send + Sync + ?Sized,
{
    async fn get_statefulset(&self, namespace: &str, name: &str) -> Result<Option<Resource>> {
        DatastoreBackend::get_resource(self, "apps/v1", "StatefulSet", Some(namespace), name).await
    }

    async fn update_statefulset_status(
        &self,
        resource: &Resource,
        status: serde_json::Value,
    ) -> Result<()> {
        crate::controllers::common::write_status_for_resource(self, resource, &status)
            .await
            .map(|_| ())
    }
}
