use async_trait::async_trait;
use klights_cluster_core::Resource;
use klights_reconcile_api::ControllerStoreResult as Result;

use crate::bootstrap::controller_adapters::controller_store_error_adapter::map_controller_store_error;
use klights_controllers::deployment::DeploymentPodMutation;

#[async_trait]
impl DeploymentPodMutation
    for crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerPodPort
{
    async fn merge_deployment_pod_labels(
        &self,
        namespace: &str,
        name: &str,
        labels: Vec<(String, String)>,
    ) -> Result<Resource> {
        self.merge_controller_pod_labels(namespace, name, None, labels)
            .await
            .map_err(map_controller_store_error)
    }
}
