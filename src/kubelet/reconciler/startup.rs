use std::sync::Arc;

use crate::kubelet::pod_lifecycle_core::message::{
    LifecycleMessage, POD_CLEANUP_REASON_NODE_LOST, PodLifecycleKey,
};
use crate::kubelet::pod_lifecycle_router::{
    OrphanReason, PodLifecycleRouter, enqueue_orphan_finalize,
};
use crate::kubelet::pod_runtime::cri::CriRuntime;
use anyhow::{Context, Result};
use klights_leader_api::{
    CacheReadinessRequest, LeaderCacheReadiness, LeaderPodCleanupIntents, LeaderResourceQuery,
    PodCleanupIntent, PodCleanupIntentListRequest,
};

pub use crate::kubelet::reconciler::cri_inventory::{
    CriInventoryAction as StartupAction, diff_cri_inventory as plan_startup_actions,
};

pub struct StartupReconciler {
    node_name: String,
    paths: crate::kubelet::runtime_paths::KubeletRuntimePaths,
    resource_query: Arc<dyn LeaderResourceQuery>,
    cache_readiness: Arc<dyn LeaderCacheReadiness>,
    pod_cleanup_intents: Arc<dyn LeaderPodCleanupIntents>,
    pod_runtime_store: Arc<dyn klights_node_store::PodRuntimeStore>,
    pod_endpoint_store: Arc<dyn klights_node_store::PodEndpointStore>,
    cri: Arc<dyn CriRuntime>,
    router: Arc<PodLifecycleRouter>,
    file_process: klights_supervisor::FileProcessExecutor,
}

pub struct StartupDependencies {
    pub resource_query: Arc<dyn LeaderResourceQuery>,
    pub cache_readiness: Arc<dyn LeaderCacheReadiness>,
    pub pod_cleanup_intents: Arc<dyn LeaderPodCleanupIntents>,
    pub pod_runtime_store: Arc<dyn klights_node_store::PodRuntimeStore>,
    pub pod_endpoint_store: Arc<dyn klights_node_store::PodEndpointStore>,
    pub cri: Arc<dyn CriRuntime>,
    pub router: Arc<PodLifecycleRouter>,
    pub file_process: klights_supervisor::FileProcessExecutor,
}

impl StartupReconciler {
    pub fn new(
        node_name: String,
        paths: crate::kubelet::runtime_paths::KubeletRuntimePaths,
        dependencies: StartupDependencies,
    ) -> Self {
        let StartupDependencies {
            resource_query,
            cache_readiness,
            pod_cleanup_intents,
            pod_runtime_store,
            pod_endpoint_store,
            cri,
            router,
            file_process,
        } = dependencies;
        Self {
            node_name,
            paths,
            resource_query,
            cache_readiness,
            pod_cleanup_intents,
            pod_runtime_store,
            pod_endpoint_store,
            cri,
            router,
            file_process,
        }
    }

    pub async fn run_once(&self) -> Result<Vec<StartupAction>> {
        self.cache_readiness
            .wait_cache_ready(CacheReadinessRequest::try_new(
                "v1",
                "Pod",
                None,
                None,
                Some(format!("spec.nodeName={}", self.node_name)),
            )?)
            .await
            .context("wait for pod cache before startup reconcile")?;

        let runtime_rows = self
            .pod_runtime_store
            .list_pod_runtime()
            .await
            .context("list node-local pod_runtime rows")?;
        let leader_pods = self
            .resource_query
            .list_resources(klights_leader_api::pods_on_node_list_request(
                &self.node_name,
                klights_leader_api::ResourceQueryConsistency::Cached,
            )?)
            .await
            .context("list leader pods on node")?
            .into_items()
            .into_iter()
            .map(|pod| (*pod.data).clone())
            .collect::<Vec<_>>();
        let sandboxes = self
            .cri
            .list_pod_sandbox_summaries()
            .await
            .context("list CRI pod sandboxes")?;

        // B3: reclaim leaked on-disk pod artifact dirs (volumes + root) whose
        // (namespace, name, uid) owner belongs to no live pod — leader Pod,
        // CRI sandbox, or node-local runtime row. Safe to delete here because
        // the full live set is known and no new pods are being created yet.
        let live_owners: std::collections::HashSet<(String, String, String)> = runtime_rows
            .iter()
            .filter_map(|row| {
                let pod = row.pod();
                crate::kubelet::reconciler::cri_inventory::pod_artifact_owner(
                    &pod.namespace,
                    &pod.name,
                    &pod.uid,
                )
            })
            .chain(sandboxes.iter().filter_map(|s| {
                crate::kubelet::reconciler::cri_inventory::pod_artifact_owner(
                    &s.namespace,
                    &s.name,
                    &s.uid,
                )
            }))
            .chain(leader_pods.iter().filter_map(
                crate::kubelet::reconciler::cri_inventory::pod_artifact_owner_from_value,
            ))
            .collect();
        match crate::kubelet::reconciler::cri_inventory::sweep_orphan_pod_artifacts(
            &self.file_process,
            self.paths.volumes_root(),
            &live_owners,
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
            .pod_cleanup_intents
            .list_pod_cleanup_intents(
                PodCleanupIntentListRequest::try_new(self.node_name.clone())
                    .map_err(anyhow::Error::new)?,
            )
            .await
            .context("list pod cleanup intents for node")?;
        append_cleanup_intent_actions(&mut actions, &cleanup_intents);
        self.apply_actions(&actions).await?;
        for intent in cleanup_intents {
            if intent.reason() == POD_CLEANUP_REASON_NODE_LOST {
                let namespace = intent.namespace().to_string();
                let pod_name = intent.pod_name().to_string();
                let pod_uid = intent.pod_uid().to_string();
                self.pod_cleanup_intents
                    .acknowledge_pod_cleanup_intent(
                        intent.ack_request().map_err(anyhow::Error::new)?,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "delete pod cleanup intent for {}/{} uid={}",
                            namespace, pod_name, pod_uid
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
    cleanup_intents: &[PodCleanupIntent],
) {
    actions.extend(
        cleanup_intents
            .iter()
            .filter(|intent| intent.reason() == POD_CLEANUP_REASON_NODE_LOST)
            .map(|intent| StartupAction::FinalizeOrphan {
                key: PodLifecycleKey::new(intent.namespace(), intent.pod_name(), intent.pod_uid()),
                reason: OrphanReason::NodeLost,
            }),
    );
}

#[cfg(test)]
mod tests {
    use crate::kubelet::reconciler::cri_inventory::tests::{pod, runtime_row, sandbox};
    use serde_json::json;

    use super::*;

    fn cleanup_intent(name: &str, uid: &str, reason: &str, rv: i64) -> PodCleanupIntent {
        PodCleanupIntent::try_new(
            "worker-a",
            "default",
            name,
            uid,
            reason,
            rv,
            1_700_000_000_000,
            klights_cluster_core::Resource::try_from_data(Arc::new(json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": name,
                    "uid": uid,
                    "resourceVersion": (rv - 1).to_string()
                },
                "spec": {"nodeName": "worker-a"}
            })))
            .unwrap(),
        )
        .unwrap()
    }

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
                cleanup_intent("lost-pod", "uid-lost", POD_CLEANUP_REASON_NODE_LOST, 10),
                cleanup_intent("future-pod", "uid-future", "FutureReason", 11),
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
