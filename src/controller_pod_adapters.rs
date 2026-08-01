use async_trait::async_trait;

use crate::controller_store_error_adapter::map_controller_store_error;
use crate::kubelet::pod_repository::{PodReader, PodRepository};
use klights_controllers::deployment::DeploymentPodReader;
use klights_controllers::pdb::PdbPodReader;

#[async_trait]
impl DeploymentPodReader for PodRepository {
    async fn list_pods_by_owner_uid(
        &self,
        namespace: &str,
        owner_uid: &str,
    ) -> klights_reconcile_api::ControllerStoreResult<Vec<klights_cluster_core::Resource>> {
        PodReader::list_pods_by_owner_uid(self, namespace, owner_uid)
            .await
            .map_err(map_controller_store_error)
    }

    async fn list_namespace_pods(
        &self,
        namespace: &str,
    ) -> klights_reconcile_api::ControllerStoreResult<Vec<klights_cluster_core::Resource>> {
        PodReader::list_pods(self, Some(namespace), None, None, None, None)
            .await
            .map(|list| list.items)
            .map_err(map_controller_store_error)
    }
}

#[async_trait]
impl DeploymentPodReader for dyn PodReader + '_ {
    async fn list_pods_by_owner_uid(
        &self,
        namespace: &str,
        owner_uid: &str,
    ) -> klights_reconcile_api::ControllerStoreResult<Vec<klights_cluster_core::Resource>> {
        PodReader::list_pods_by_owner_uid(self, namespace, owner_uid)
            .await
            .map_err(map_controller_store_error)
    }

    async fn list_namespace_pods(
        &self,
        namespace: &str,
    ) -> klights_reconcile_api::ControllerStoreResult<Vec<klights_cluster_core::Resource>> {
        PodReader::list_pods(self, Some(namespace), None, None, None, None)
            .await
            .map(|list| list.items)
            .map_err(map_controller_store_error)
    }
}

#[async_trait]
impl PdbPodReader for PodRepository {
    async fn list_namespace_pods(
        &self,
        namespace: &str,
    ) -> klights_reconcile_api::ControllerStoreResult<Vec<klights_cluster_core::Resource>> {
        PodReader::list_pods(self, Some(namespace), None, None, None, None)
            .await
            .map(|list| list.items)
            .map_err(map_controller_store_error)
    }
}

#[async_trait]
impl PdbPodReader for dyn PodReader + '_ {
    async fn list_namespace_pods(
        &self,
        namespace: &str,
    ) -> klights_reconcile_api::ControllerStoreResult<Vec<klights_cluster_core::Resource>> {
        PodReader::list_pods(self, Some(namespace), None, None, None, None)
            .await
            .map(|list| list.items)
            .map_err(map_controller_store_error)
    }
}
