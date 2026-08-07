use super::*;

impl<'a> PodRecovery<'a> {
    pub fn new(
        pod_repo: &'a Arc<crate::kubelet::pod_repository::PodRepository>,
        node_name: &'a str,
        retry_state: &'a PodStartRetryTracker,
        pod_lifecycle_router: std::sync::Arc<
            klights_kubelet::pod_lifecycle_router::PodLifecycleRouter,
        >,
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
        use klights_kubelet::pod_lifecycle_core::message::LifecycleMessage;
        use klights_pod_api::{PodListRequest, PodQuery};
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

#[cfg(test)]
mod tests {
    use super::*;

    use klights_kubelet::pod_lifecycle_core::action::PodAction;
    use klights_kubelet::pod_lifecycle_router::PodLifecycleRouter;
    use klights_kubelet::pod_lifecycle_router::executor::RecordingExecutor;
    use std::sync::Arc;

    async fn wait_for_recorded_action(
        recorder: &Arc<RecordingExecutor>,
        predicate: impl Fn(&PodAction) -> bool,
    ) {
        for _ in 0..20 {
            if recorder.actions.lock().unwrap().iter().any(&predicate) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("expected lifecycle action was not recorded");
    }

    #[tokio::test]
    async fn boot_recovery_routes_existing_pods_through_actor_startpod() {
        let (_db, db_handle) =
            crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let parts = crate::kubelet::pod_repository::PodRepository::build_parts(
            crate::kubelet::pod_repository::PodRepositoryBuildConfig {
                db: db_handle.clone(),
                pod_workqueue_store: None,
                supervisor: supervisor.clone(),
                side_effects: Arc::new(klights_controllers::side_effects::SideEffectRegistry::new()),
                metrics: klights_controllers::side_effects::SideEffectMetrics::new(),
                pod_network_cache: crate::kubelet::pod_repository::empty_test_pod_network_cache(),
                assignment_waiter: crate::kubelet::pod_repository::test_assignment_bus(),
                scheduling_mode:
                    crate::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
                outbox: None,
                cluster_api: None,
                remote_delivery_required: false,
                controller_identity:
                    crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity(),
                scheduler_bind_gate: None,
            },
        );
        let pod_repo = Arc::new(parts.repository);
        let pod = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "kube-system",
                "name": "coredns",
                "uid": "uid-coredns"
            },
            "spec": {
                "nodeName": "test-node",
                "containers": [{"name": "coredns", "image": "coredns/coredns:1.11.1"}]
            },
            "status": {"phase": "Pending"}
        });
        pod_repo
            .test_create_pod("kube-system", "coredns", "test-node", pod)
            .await
            .expect("create recovery pod");

        let recorder = RecordingExecutor::new();
        let registry = Arc::new(klights_kubelet::pod_lifecycle_actor::registry::PodLifecycleRegistry::new(
            supervisor,
            klights_kubelet::pod_lifecycle_actor::config::PodLifecycleConcurrencyConfig::production_default(),
            Arc::new(std::sync::Mutex::new(recorder.clone())),
        ));
        let router = Arc::new(PodLifecycleRouter::new_actor_with_executor(
            registry,
            recorder.clone(),
        ));
        let retry_state: klights_kubelet::pod_creation_state::PodStartRetryTracker = Arc::new(
            tokio::sync::Mutex::new(klights_kubelet::pod_creation_state::PodStartRetryState::new()),
        );
        let mut recovery = PodRecovery::new(&pod_repo, "test-node", &retry_state, router);

        recovery
            .recover_existing_pods()
            .await
            .expect("recover existing pods");

        wait_for_recorded_action(&recorder, |action| {
            matches!(
                action,
                PodAction::StartPod { key, .. }
                    if key.namespace == "kube-system"
                        && key.name == "coredns"
                        && key.uid == "uid-coredns"
            )
        })
        .await;
    }
}
