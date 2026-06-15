use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::mpsc;

use crate::control_plane::client::{CacheScope, LeaderApiClient};
use crate::datastore::node_local::NodeLocalHandle;
use crate::kubelet::pod_lifecycle_core::message::LifecycleMessage;
use crate::kubelet::pod_lifecycle_router::{PodLifecycleRouter, enqueue_orphan_finalize};
use crate::kubelet::pod_runtime::cri::{ContainerRuntimeControl, CriRuntime};
use crate::kubelet::reconciler::cri_inventory::{
    CriContainerInventory, CriInventoryAction, cleanup_cold_sandbox, diff_cri_inventory,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CriStreamLifecycle {
    Reconnected {
        generation: u64,
        disconnected_at_ms: i64,
        reconnected_at_ms: i64,
    },
}

pub struct CriReconnectReconciler {
    node_name: String,
    cluster_api: Arc<dyn LeaderApiClient>,
    node_local: NodeLocalHandle,
    cri: Arc<dyn CriRuntime>,
    container_control: Arc<dyn ContainerRuntimeControl>,
    router: Arc<PodLifecycleRouter>,
}

impl CriReconnectReconciler {
    pub fn new(
        node_name: String,
        cluster_api: Arc<dyn LeaderApiClient>,
        node_local: NodeLocalHandle,
        cri: Arc<dyn CriRuntime>,
        container_control: Arc<dyn ContainerRuntimeControl>,
        router: Arc<PodLifecycleRouter>,
    ) -> Self {
        Self {
            node_name,
            cluster_api,
            node_local,
            cri,
            container_control,
            router,
        }
    }

    pub async fn run_once(&self) -> Result<Vec<CriInventoryAction>> {
        self.cluster_api
            .wait_cache_ready(CacheScope::Resource {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: None,
            })
            .await
            .context("wait for pod cache before CRI reconnect reconcile")?;

        let runtime_rows = self.node_local.list_pod_runtime().await?;
        let leader_pods = self
            .cluster_api
            .list_pods_on_node(&self.node_name)
            .await?
            .into_iter()
            .map(|pod| (*pod.data).clone())
            .collect::<Vec<_>>();
        let sandboxes = self.cri.list_pod_sandbox_summaries().await?;
        let mut containers = Vec::new();
        for sandbox in &sandboxes {
            for (container_id, state) in self
                .container_control
                .list_containers(Some(&sandbox.sandbox_id))
                .await?
            {
                containers.push(CriContainerInventory {
                    sandbox_id: sandbox.sandbox_id.clone(),
                    container_id,
                    state,
                });
            }
        }

        let actions =
            diff_cri_inventory(true, &runtime_rows, &leader_pods, &sandboxes, &containers);
        self.apply_actions(&actions).await?;
        Ok(actions)
    }

    async fn apply_actions(&self, actions: &[CriInventoryAction]) -> Result<()> {
        for action in actions {
            match action {
                CriInventoryAction::FinalizeOrphan { key, reason } => {
                    enqueue_orphan_finalize(self.router.as_ref(), key.clone(), *reason).await?;
                }
                CriInventoryAction::KillColdSandbox { sandbox_id, key } => {
                    cleanup_cold_sandbox(
                        self.router.as_ref(),
                        self.cri.as_ref(),
                        sandbox_id,
                        key.as_ref(),
                    )
                    .await?;
                }
                CriInventoryAction::DropLocalRows { key } => {
                    self.node_local.delete_pod_runtime_for_uid(&key.uid).await?;
                    self.node_local.delete_endpoint_for_uid(&key.uid).await?;
                }
                CriInventoryAction::ReattachExistingSandbox { key, pod, .. }
                | CriInventoryAction::RecreateMissingSandbox { key, pod } => {
                    self.router
                        .route(LifecycleMessage::WatchAdded {
                            key: key.clone(),
                            resource_version: None,
                            pod: pod.clone(),
                        })
                        .await?;
                }
                CriInventoryAction::ReconcileRuntime { key } => {
                    self.router
                        .route(LifecycleMessage::CriEvent {
                            key: key.clone(),
                            container_id: String::new(),
                            kind: crate::kubelet::cri_events::KubeletEventKind::Stopped,
                        })
                        .await?;
                }
                CriInventoryAction::RefuseEmptyCache => {}
            }
        }
        Ok(())
    }

    pub async fn run_lifecycle_loop(
        self: Arc<Self>,
        mut rx: mpsc::Receiver<CriStreamLifecycle>,
        cancel_token: tokio_util::sync::CancellationToken,
    ) {
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => return,
                event = rx.recv() => {
                    let Some(CriStreamLifecycle::Reconnected { generation, .. }) = event else {
                        return;
                    };
                    let mut generation = generation;
                    loop {
                        match self.run_once().await {
                            Ok(actions) => tracing::warn!(
                                generation,
                                action_count = actions.len(),
                                "CRI reconnect inventory diff completed"
                            ),
                            Err(err) => tracing::warn!(
                                generation,
                                "CRI reconnect inventory diff failed: {err:#}"
                            ),
                        }
                        match rx.try_recv() {
                            Ok(CriStreamLifecycle::Reconnected { generation: next_generation, .. }) => {
                                generation = next_generation;
                                continue;
                            }
                            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => return,
                        }
                    }
                }
            }
        }
    }
}
