use super::*;

impl<'a> PodRecovery<'a> {
    pub fn new(
        pod_repo: &'a Arc<crate::kubelet::pod_repository::PodRepository>,
        node_name: &'a str,
        retry_state: &'a PodStartRetryTracker,
        pod_lifecycle_router: std::sync::Arc<
            crate::kubelet::pod_lifecycle_router::PodLifecycleRouter,
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
        // Route through PodReader so the v1/Pod read boundary stays
        // inside `PodStore`.
        use crate::kubelet::pod_lifecycle_core::message::LifecycleMessage;
        use crate::kubelet::pod_repository::PodReader;
        let field_selector = super::pod_watcher_node_field_selector(self.node_name);
        let pod_list = self
            .pod_repo
            .list_pods(None, None, Some(field_selector.as_str()), None, None)
            .await?;

        for pod_resource in pod_list.items {
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
    use crate::kubelet::pod_lifecycle_core::action::PodAction;
    use crate::kubelet::pod_lifecycle_router::PodLifecycleRouter;
    use crate::kubelet::pod_lifecycle_router::executor::RecordingExecutor;
    use crate::kubelet::pod_repository::PodObjectWriter;
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
        let (_db, db_handle) = crate::datastore::test_support::in_memory_with_handle().await;
        let supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let parts = crate::kubelet::pod_repository::PodRepository::build_parts(
            crate::kubelet::pod_repository::PodRepositoryBuildConfig {
                db: db_handle.clone(),
                supervisor: supervisor.clone(),
                side_effects: Arc::new(crate::side_effects::SideEffectRegistry::new()),
                metrics: crate::side_effects::SideEffectMetrics::new(),
                network_events: crate::networking::global_pod_network_events(),
                scheduling_mode:
                    crate::kubelet::pod_repository::api::PodSchedulingMode::InlineSingleNode,
                outbox: None,
                cluster_api: None,
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
            .create_controller_pod("kube-system", "coredns", "test-node", pod)
            .await
            .expect("create recovery pod");

        let recorder = RecordingExecutor::new();
        let registry = Arc::new(crate::kubelet::pod_lifecycle_actor::registry::PodLifecycleRegistry::new(
            supervisor,
            crate::kubelet::pod_lifecycle_actor::config::PodLifecycleConcurrencyConfig::production_default(),
            Arc::new(std::sync::Mutex::new(recorder.clone())),
        ));
        let router = Arc::new(PodLifecycleRouter::new_actor_with_executor(
            registry,
            recorder.clone(),
        ));
        let retry_state: crate::kubelet::pod_creation_state::PodStartRetryTracker = Arc::new(
            tokio::sync::Mutex::new(crate::kubelet::pod_creation_state::PodStartRetryState::new()),
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
