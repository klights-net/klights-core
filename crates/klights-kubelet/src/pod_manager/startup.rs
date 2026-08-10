use super::*;

impl<'a> PodRecovery<'a> {
    pub fn new(
        pod_repo: Arc<dyn klights_pod_api::PodQuery>,
        node_name: &'a str,
        retry_state: &'a PodStartRetryTracker,
        pod_lifecycle_router: std::sync::Arc<crate::pod_lifecycle_router::PodLifecycleRouter>,
    ) -> Self {
        Self {
            pod_repo,
            node_name,
            retry_state,
            pod_lifecycle_router,
        }
    }

    pub(super) async fn recover_existing_pods(&mut self) -> Result<()> {
        // Route through PodQuery so the v1/Pod read boundary stays
        // inside `PodStore`.
        use crate::pod_lifecycle_core::message::LifecycleMessage;
        use klights_pod_api::PodListRequest;
        let field_selector = super::pod_watcher_node_field_selector(self.node_name);
        let pod_list = self
            .pod_repo
            .list_pods(PodListRequest::try_new(
                None,
                None,
                Some(field_selector),
                None,
                None,
            )?)
            .await?;

        for pod_resource in pod_list.into_parts().0 {
            let namespace = pod_resource
                .data
                .pointer("/metadata/namespace")
                .and_then(|n| n.as_str())
                .unwrap_or("default");
            let pod_name = pod_resource
                .data
                .pointer("/metadata/name")
                .and_then(|n| n.as_str())
                .unwrap_or("");
            let key = super::pod_lifecycle_key_from_pod(&pod_resource.data)
                .expect("pod must have metadata for recovery");
            self.pod_lifecycle_router
                .route(LifecycleMessage::WatchAdded {
                    key,
                    resource_version: Some(pod_resource.resource_version),
                    pod: pod_resource.data.as_ref().clone(),
                })
                .await
                .map_err(|e| anyhow::anyhow!("failed to route recovered pod: {e}"))?;
            clear_pod_start_retry_state(self.retry_state, namespace, pod_name).await;
        }

        Ok(())
    }
}
