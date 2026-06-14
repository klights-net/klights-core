use std::sync::Arc;

use crate::control_plane::client::{CacheScope, LeaderApiClient};
use crate::datastore::POD_CLEANUP_REASON_NODE_LOST;
use crate::datastore::node_local::NodeLocalHandle;
use crate::kubelet::pod_lifecycle_core::message::{LifecycleMessage, PodLifecycleKey};
use crate::kubelet::pod_lifecycle_router::{
    OrphanReason, PodLifecycleRouter, enqueue_orphan_finalize,
};
use crate::kubelet::pod_runtime::cri::CriRuntime;
use anyhow::{Context, Result};

pub use crate::kubelet::reconciler::cri_inventory::{
    CriInventoryAction as StartupAction, diff_cri_inventory as plan_startup_actions,
};

pub struct StartupReconciler {
    node_name: String,
    cluster_api: Arc<dyn LeaderApiClient>,
    node_local: NodeLocalHandle,
    cri: Arc<dyn CriRuntime>,
    router: Arc<PodLifecycleRouter>,
}

impl StartupReconciler {
    pub fn new(
        node_name: String,
        cluster_api: Arc<dyn LeaderApiClient>,
        node_local: NodeLocalHandle,
        cri: Arc<dyn CriRuntime>,
        router: Arc<PodLifecycleRouter>,
    ) -> Self {
        Self {
            node_name,
            cluster_api,
            node_local,
            cri,
            router,
        }
    }

    pub async fn run_once(&self) -> Result<Vec<StartupAction>> {
        self.cluster_api
            .wait_cache_ready(CacheScope::Resource {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: None,
            })
            .await
            .context("wait for pod cache before startup reconcile")?;

        let runtime_rows = self
            .node_local
            .list_pod_runtime()
            .await
            .context("list node-local pod_runtime rows")?;
        let leader_pods = self
            .cluster_api
            .list_pods_on_node(&self.node_name)
            .await
            .context("list leader pods on node")?
            .into_iter()
            .map(|pod| (*pod.data).clone())
            .collect::<Vec<_>>();
        let sandbox_ids = self
            .cri
            .list_pod_sandboxes(None)
            .await
            .context("list CRI pod sandboxes")?
            .into_iter()
            .map(|(sandbox_id, _state)| sandbox_id)
            .collect::<Vec<_>>();

        let mut actions =
            plan_startup_actions(true, &runtime_rows, &leader_pods, &sandbox_ids, &[]);
        let cleanup_intents = self
            .cluster_api
            .list_pod_cleanup_intents_for_node(&self.node_name)
            .await
            .context("list pod cleanup intents for node")?;
        append_cleanup_intent_actions(&mut actions, &cleanup_intents);
        self.apply_actions(&actions).await?;
        for intent in cleanup_intents {
            if intent.reason == POD_CLEANUP_REASON_NODE_LOST {
                self.cluster_api
                    .delete_pod_cleanup_intent(
                        &intent.node_name,
                        &intent.namespace,
                        &intent.pod_name,
                        &intent.pod_uid,
                        &intent.reason,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "delete pod cleanup intent for {}/{} uid={}",
                            intent.namespace, intent.pod_name, intent.pod_uid
                        )
                    })?;
            }
        }
        Ok(actions)
    }

    async fn apply_actions(&self, actions: &[StartupAction]) -> Result<()> {
        for action in actions {
            match action {
                StartupAction::FinalizeOrphan { key, reason } => {
                    enqueue_orphan_finalize(self.router.as_ref(), key.clone(), *reason).await?;
                }
                StartupAction::KillColdSandbox { sandbox_id } => {
                    let _ = self.cri.stop_pod_sandbox(sandbox_id).await;
                    let _ = self.cri.remove_pod_sandbox(sandbox_id).await;
                }
                StartupAction::DropLocalRows { key } => {
                    self.node_local.delete_pod_runtime_for_uid(&key.uid).await?;
                    self.node_local.delete_endpoint_for_uid(&key.uid).await?;
                }
                StartupAction::ReattachExistingSandbox { key, pod, .. }
                | StartupAction::RecreateMissingSandbox { key, pod } => {
                    self.router
                        .route(LifecycleMessage::WatchAdded {
                            key: key.clone(),
                            resource_version: None,
                            pod: pod.clone(),
                        })
                        .await?;
                }
                StartupAction::ReconcileRuntime { key } => {
                    self.router
                        .route(LifecycleMessage::CriEvent {
                            key: key.clone(),
                            container_id: String::new(),
                            kind: crate::kubelet::cri_events::KubeletEventKind::Stopped,
                        })
                        .await?;
                }
                StartupAction::RefuseEmptyCache => {}
            }
        }
        Ok(())
    }
}

fn append_cleanup_intent_actions(
    actions: &mut Vec<StartupAction>,
    cleanup_intents: &[crate::datastore::PodCleanupIntent],
) {
    actions.extend(
        cleanup_intents
            .iter()
            .filter(|intent| intent.reason == POD_CLEANUP_REASON_NODE_LOST)
            .map(|intent| StartupAction::FinalizeOrphan {
                key: PodLifecycleKey::new(&intent.namespace, &intent.pod_name, &intent.pod_uid),
                reason: OrphanReason::NodeLost,
            }),
    );
}

#[cfg(test)]
mod tests {
    use crate::kubelet::reconciler::cri_inventory::tests::{pod, runtime_row};
    use serde_json::json;

    use super::*;

    #[test]
    fn stale_uid_orphan_finalized() {
        let actions = plan_startup_actions(
            true,
            &[runtime_row("old-uid", "default", "web", Some("sb-old"))],
            &[pod("default", "web", "new-uid")],
            &["sb-old".to_string()],
            &[],
        );

        assert_eq!(
            actions,
            vec![StartupAction::FinalizeOrphan {
                key: crate::kubelet::pod_lifecycle_core::message::PodLifecycleKey::new(
                    "default", "web", "old-uid",
                ),
                reason: crate::kubelet::pod_lifecycle_router::OrphanReason::UidChangedWhileDown,
            }]
        );
    }

    #[test]
    fn refuse_to_clean_with_empty_cache() {
        let actions = plan_startup_actions(
            false,
            &[runtime_row("uid-a", "default", "web", Some("sb-a"))],
            &[],
            &["sb-a".to_string()],
            &[],
        );

        assert_eq!(actions, vec![StartupAction::RefuseEmptyCache]);
    }

    #[test]
    fn node_lost_cleanup_intents_enqueue_uid_bound_orphan_finalization() {
        let mut actions = Vec::new();
        append_cleanup_intent_actions(
            &mut actions,
            &[
                crate::datastore::PodCleanupIntent {
                    node_name: "worker-a".to_string(),
                    namespace: "default".to_string(),
                    pod_name: "lost-pod".to_string(),
                    pod_uid: "uid-lost".to_string(),
                    reason: POD_CLEANUP_REASON_NODE_LOST.to_string(),
                    resource_version: 10,
                    created_at_ms: 1_700_000_000_000,
                    pod_data: json!({}),
                },
                crate::datastore::PodCleanupIntent {
                    node_name: "worker-a".to_string(),
                    namespace: "default".to_string(),
                    pod_name: "future-pod".to_string(),
                    pod_uid: "uid-future".to_string(),
                    reason: "FutureReason".to_string(),
                    resource_version: 11,
                    created_at_ms: 1_700_000_000_001,
                    pod_data: json!({}),
                },
            ],
        );

        assert_eq!(
            actions,
            vec![StartupAction::FinalizeOrphan {
                key: PodLifecycleKey::new("default", "lost-pod", "uid-lost"),
                reason: OrphanReason::NodeLost,
            }]
        );
    }
}
