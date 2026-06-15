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
    containerd_ns: String,
    cluster_api: Arc<dyn LeaderApiClient>,
    node_local: NodeLocalHandle,
    cri: Arc<dyn CriRuntime>,
    router: Arc<PodLifecycleRouter>,
}

impl StartupReconciler {
    pub fn new(
        node_name: String,
        containerd_ns: String,
        cluster_api: Arc<dyn LeaderApiClient>,
        node_local: NodeLocalHandle,
        cri: Arc<dyn CriRuntime>,
        router: Arc<PodLifecycleRouter>,
    ) -> Self {
        Self {
            node_name,
            containerd_ns,
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
        let sandboxes = self
            .cri
            .list_pod_sandbox_summaries()
            .await
            .context("list CRI pod sandboxes")?;

        // B3: reclaim leaked on-disk pod artifact dirs (volumes + root) whose
        // (namespace, name) slot belongs to no live pod — leader Pod, CRI
        // sandbox, or node-local runtime row. Safe to delete here because the
        // full live set is known and no new pods are being created yet.
        let live_slots: std::collections::HashSet<(String, String)> = runtime_rows
            .iter()
            .map(|row| (row.namespace.clone(), row.pod_name.clone()))
            .chain(
                sandboxes
                    .iter()
                    .map(|s| (s.namespace.clone(), s.name.clone())),
            )
            .chain(leader_pods.iter().filter_map(|pod| {
                let meta = pod.get("metadata")?;
                let ns = meta.get("namespace")?.as_str()?;
                let name = meta.get("name")?.as_str()?;
                Some((ns.to_string(), name.to_string()))
            }))
            .collect();
        match crate::kubelet::reconciler::cri_inventory::sweep_orphan_pod_artifacts(
            &self.containerd_ns,
            &live_slots,
        )
        .await
        {
            Ok(0) => {}
            Ok(removed) => {
                tracing::info!(removed, "startup reconcile swept leaked pod artifact dirs")
            }
            Err(err) => tracing::warn!("startup orphan pod artifact sweep failed: {err:#}"),
        }

        let mut actions = plan_startup_actions(true, &runtime_rows, &leader_pods, &sandboxes, &[]);
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
                StartupAction::KillColdSandbox { sandbox_id, key } => {
                    crate::kubelet::reconciler::cri_inventory::cleanup_cold_sandbox(
                        self.router.as_ref(),
                        self.cri.as_ref(),
                        sandbox_id,
                        key.as_ref(),
                    )
                    .await?;
                }
                StartupAction::DropLocalRows { key } => {
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
    use crate::kubelet::reconciler::cri_inventory::tests::{pod, runtime_row, sandbox};
    use serde_json::json;

    use super::*;

    #[test]
    fn stale_uid_orphan_finalized() {
        let actions = plan_startup_actions(
            true,
            &[runtime_row("old-uid", "default", "web", Some("sb-old"))],
            &[pod("default", "web", "new-uid")],
            &[sandbox("sb-old", "default", "web", "old-uid")],
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
            &[sandbox("sb-a", "default", "web", "uid-a")],
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
