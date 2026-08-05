use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::mpsc;

use crate::kubelet::reconciler::cri_inventory::{
    CriContainerInventory, CriInventoryAction, cleanup_cold_sandbox, diff_cri_inventory,
};
use klights_kubelet::pod_lifecycle_core::message::LifecycleMessage;
use klights_kubelet::pod_lifecycle_router::{
    OrphanReason, PodLifecycleRouter, enqueue_orphan_finalize,
};
use klights_kubelet::runtime::cri::{ContainerRuntimeControl, CriRuntime};
use klights_leader_api::{CacheReadinessRequest, LeaderCacheReadiness, LeaderResourceQuery};

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
    resource_query: Arc<dyn LeaderResourceQuery>,
    cache_readiness: Arc<dyn LeaderCacheReadiness>,
    pod_runtime_store: Arc<dyn klights_node_store::PodRuntimeStore>,
    pod_endpoint_store: Arc<dyn klights_node_store::PodEndpointStore>,
    cri: Arc<dyn CriRuntime>,
    container_control: Arc<dyn ContainerRuntimeControl>,
    router: Arc<PodLifecycleRouter>,
}

pub struct CriReconnectDependencies {
    pub resource_query: Arc<dyn LeaderResourceQuery>,
    pub cache_readiness: Arc<dyn LeaderCacheReadiness>,
    pub pod_runtime_store: Arc<dyn klights_node_store::PodRuntimeStore>,
    pub pod_endpoint_store: Arc<dyn klights_node_store::PodEndpointStore>,
    pub cri: Arc<dyn CriRuntime>,
    pub container_control: Arc<dyn ContainerRuntimeControl>,
    pub router: Arc<PodLifecycleRouter>,
}

impl CriReconnectReconciler {
    pub fn new(node_name: String, dependencies: CriReconnectDependencies) -> Self {
        let CriReconnectDependencies {
            resource_query,
            cache_readiness,
            pod_runtime_store,
            pod_endpoint_store,
            cri,
            container_control,
            router,
        } = dependencies;
        Self {
            node_name,
            resource_query,
            cache_readiness,
            pod_runtime_store,
            pod_endpoint_store,
            cri,
            container_control,
            router,
        }
    }

    pub async fn run_once(&self) -> Result<Vec<CriInventoryAction>> {
        self.cache_readiness
            .wait_cache_ready(CacheReadinessRequest::try_new(
                "v1",
                "Pod",
                None,
                None,
                Some(format!("spec.nodeName={}", self.node_name)),
            )?)
            .await
            .context("wait for pod cache before CRI reconnect reconcile")?;

        let runtime_rows = self.pod_runtime_store.list_pod_runtime().await?;
        let leader_pods = self
            .resource_query
            .list_resources(klights_leader_api::pods_on_node_list_request(
                &self.node_name,
                klights_leader_api::ResourceQueryConsistency::Cached,
            )?)
            .await?
            .into_items()
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
                    // The sandbox is gone from CRI and the leader has no Pod, but
                    // on-disk artifacts (volumes, cgroup, pod dir) may still be
                    // present. Reclaim them via actor-owned orphan finalize — the
                    // cleanup is UID-keyed and idempotent, so it works whether or
                    // not the bookkeeping rows still exist — then drop the rows.
                    enqueue_orphan_finalize(
                        self.router.as_ref(),
                        key.clone(),
                        OrphanReason::LeaderDeletedWhileDown,
                    )
                    .await?;
                    self.pod_runtime_store
                        .delete_pod_runtime_for_uid(klights_node_store::RuntimePodUid::try_new(
                            key.uid.clone(),
                        )?)
                        .await?;
                    self.pod_endpoint_store
                        .delete_endpoint_for_uid(klights_node_store::PodUidKey::try_new(
                            key.uid.clone(),
                        )?)
                        .await?;
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
                            kind: klights_kubelet::cri_events::KubeletEventKind::Stopped,
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
