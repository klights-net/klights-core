use anyhow::Result;
use async_trait::async_trait;

use crate::controllers::deployment::DeploymentPodReader;
use crate::controllers::pdb::PdbPodReader;
use crate::kubelet::pod_repository::PodReader;

#[async_trait]
impl<T> DeploymentPodReader for T
where
    T: PodReader + ?Sized,
{
    async fn list_pods_by_owner_uid(
        &self,
        namespace: &str,
        owner_uid: &str,
    ) -> Result<Vec<klights_cluster_core::Resource>> {
        PodReader::list_pods_by_owner_uid(self, namespace, owner_uid).await
    }

    async fn list_namespace_pods(
        &self,
        namespace: &str,
    ) -> Result<Vec<klights_cluster_core::Resource>> {
        PodReader::list_pods(self, Some(namespace), None, None, None, None)
            .await
            .map(|list| list.items)
    }
}

#[async_trait]
impl<T> PdbPodReader for T
where
    T: PodReader + ?Sized,
{
    async fn list_namespace_pods(
        &self,
        namespace: &str,
    ) -> Result<Vec<klights_cluster_core::Resource>> {
        PodReader::list_pods(self, Some(namespace), None, None, None, None)
            .await
            .map(|list| list.items)
    }
}
