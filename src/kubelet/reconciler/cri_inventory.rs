use std::collections::{HashMap, HashSet};

use crate::datastore::node_local::PodRuntimeRow;
use crate::kubelet::pod_lifecycle_core::message::PodLifecycleKey;
use crate::kubelet::pod_lifecycle_router::{
    OrphanReason, PodLifecycleRouter, enqueue_orphan_finalize,
};
use crate::kubelet::pod_runtime::cri::{ContainerRuntimeState, CriPodSandboxSummary, CriRuntime};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CriContainerInventory {
    pub sandbox_id: String,
    pub container_id: String,
    pub state: ContainerRuntimeState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CriInventoryAction {
    ReattachExistingSandbox {
        key: PodLifecycleKey,
        sandbox_id: String,
        pod: serde_json::Value,
    },
    RecreateMissingSandbox {
        key: PodLifecycleKey,
        pod: serde_json::Value,
    },
    ReconcileRuntime {
        key: PodLifecycleKey,
    },
    FinalizeOrphan {
        key: PodLifecycleKey,
        reason: OrphanReason,
    },
    DropLocalRows {
        key: PodLifecycleKey,
    },
    KillColdSandbox {
        sandbox_id: String,
        /// Pod identity recovered from the cold sandbox's CRI metadata, when
        /// present. `Some` routes the teardown through actor-owned orphan
        /// finalize (reclaiming pod dir/cgroup/volumes via the shared helper);
        /// `None` (sandbox carries no identity) falls back to CRI-only teardown.
        key: Option<PodLifecycleKey>,
    },
    RefuseEmptyCache,
}

pub fn diff_cri_inventory(
    cache_primed: bool,
    runtime_rows: &[PodRuntimeRow],
    leader_pods: &[serde_json::Value],
    cri_sandboxes: &[CriPodSandboxSummary],
    cri_containers: &[CriContainerInventory],
) -> Vec<CriInventoryAction> {
    if !cache_primed {
        return vec![CriInventoryAction::RefuseEmptyCache];
    }

    let mut leader_by_slot = HashMap::<(String, String), (String, serde_json::Value)>::new();
    for pod in leader_pods {
        let Some(namespace) = pod.pointer("/metadata/namespace").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(name) = pod.pointer("/metadata/name").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(uid) = pod.pointer("/metadata/uid").and_then(|v| v.as_str()) else {
            continue;
        };
        leader_by_slot.insert(
            (namespace.to_string(), name.to_string()),
            (uid.to_string(), pod.clone()),
        );
    }

    let cri_sandbox_ids = cri_sandboxes
        .iter()
        .map(|sandbox| sandbox.sandbox_id.clone())
        .collect::<HashSet<_>>();
    let runtime_sandboxes = runtime_rows
        .iter()
        .filter_map(|row| row.sandbox_id.clone())
        .collect::<HashSet<_>>();
    let dirty_sandboxes = cri_containers
        .iter()
        .filter(|container| container.state != ContainerRuntimeState::Running)
        .map(|container| container.sandbox_id.clone())
        .collect::<HashSet<_>>();
    let mut actions = Vec::new();

    for row in runtime_rows {
        let key = PodLifecycleKey::new(&row.namespace, &row.pod_name, &row.pod_uid);
        match leader_by_slot.get(&(row.namespace.clone(), row.pod_name.clone())) {
            Some((uid, pod)) if uid == &row.pod_uid => match row.sandbox_id.as_deref() {
                Some(sandbox_id) if cri_sandbox_ids.contains(sandbox_id) => {
                    actions.push(CriInventoryAction::ReattachExistingSandbox {
                        key: key.clone(),
                        sandbox_id: sandbox_id.to_string(),
                        pod: pod.clone(),
                    });
                    if dirty_sandboxes.contains(sandbox_id) {
                        actions.push(CriInventoryAction::ReconcileRuntime { key });
                    }
                }
                _ => actions.push(CriInventoryAction::RecreateMissingSandbox {
                    key,
                    pod: pod.clone(),
                }),
            },
            Some(_) => actions.push(CriInventoryAction::FinalizeOrphan {
                key,
                reason: OrphanReason::UidChangedWhileDown,
            }),
            None if row
                .sandbox_id
                .as_ref()
                .is_some_and(|id| cri_sandbox_ids.contains(id)) =>
            {
                actions.push(CriInventoryAction::FinalizeOrphan {
                    key,
                    reason: OrphanReason::LeaderDeletedWhileDown,
                });
            }
            None => actions.push(CriInventoryAction::DropLocalRows { key }),
        }
    }

    for sandbox in cri_sandboxes {
        if !runtime_sandboxes.contains(&sandbox.sandbox_id) {
            // Recover the pod identity CRI stamped on the sandbox so the cold
            // teardown can run actor-owned orphan cleanup (pod dir, cgroup,
            // volumes) instead of leaking everything but the sandbox.
            let key = (!sandbox.namespace.trim().is_empty()
                && !sandbox.name.trim().is_empty()
                && !sandbox.uid.trim().is_empty())
            .then(|| PodLifecycleKey::new(&sandbox.namespace, &sandbox.name, &sandbox.uid));
            actions.push(CriInventoryAction::KillColdSandbox {
                sandbox_id: sandbox.sandbox_id.clone(),
                key,
            });
        }
    }

    actions
}

/// Tear down a cold (CRI-present but untracked) sandbox surfaced by
/// [`CriInventoryAction::KillColdSandbox`].
///
/// When the sandbox still carries a pod identity (`key`), route the teardown
/// through actor-owned orphan finalize (`OrphanReason::ColdCriOrphan`): the
/// per-UID actor's `stop_orphan_pod` re-resolves the sandbox by UID via CRI and
/// runs the shared cleanup, reclaiming the pod dir, cgroup, and volumes — not
/// just the CRI sandbox. With no identity to key an actor on, fall back to a
/// CRI-only sandbox teardown. Shared by the startup and reconnect reconcilers.
pub async fn cleanup_cold_sandbox(
    router: &PodLifecycleRouter,
    cri: &dyn CriRuntime,
    sandbox_id: &str,
    key: Option<&PodLifecycleKey>,
) -> anyhow::Result<()> {
    match key {
        Some(key) => {
            enqueue_orphan_finalize(router, key.clone(), OrphanReason::ColdCriOrphan).await?;
        }
        None => {
            let _ = cri.stop_pod_sandbox(sandbox_id).await;
            let _ = cri.remove_pod_sandbox(sandbox_id).await;
        }
    }
    Ok(())
}

#[cfg(test)]
pub mod tests {
    use super::*;

    pub fn runtime_row(
        uid: &str,
        namespace: &str,
        name: &str,
        sandbox_id: Option<&str>,
    ) -> PodRuntimeRow {
        PodRuntimeRow {
            pod_uid: uid.to_string(),
            namespace: namespace.to_string(),
            pod_name: name.to_string(),
            node_name: "worker-a".to_string(),
            sandbox_id: sandbox_id.map(str::to_string),
            cgroup_path: None,
            created_ms: 1,
            started_ms: Some(2),
        }
    }

    pub fn sandbox(id: &str, namespace: &str, name: &str, uid: &str) -> CriPodSandboxSummary {
        CriPodSandboxSummary {
            sandbox_id: id.to_string(),
            namespace: namespace.to_string(),
            name: name.to_string(),
            uid: uid.to_string(),
        }
    }

    pub fn pod(namespace: &str, name: &str, uid: &str) -> serde_json::Value {
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"namespace": namespace, "name": name, "uid": uid},
            "spec": {"nodeName": "worker-a", "containers": [{"name": "app", "image": "nginx"}]},
        })
    }

    #[test]
    fn reattach_existing_sandbox() {
        let pod = pod("default", "web", "uid-a");
        let actions = diff_cri_inventory(
            true,
            &[runtime_row("uid-a", "default", "web", Some("sb-a"))],
            std::slice::from_ref(&pod),
            &[sandbox("sb-a", "default", "web", "uid-a")],
            &[],
        );

        assert_eq!(
            actions,
            vec![CriInventoryAction::ReattachExistingSandbox {
                key: PodLifecycleKey::new("default", "web", "uid-a"),
                sandbox_id: "sb-a".to_string(),
                pod,
            }]
        );
    }

    #[test]
    fn recreate_missing_sandbox_idempotent() {
        let pod = pod("default", "web", "uid-a");
        let actions = diff_cri_inventory(
            true,
            &[runtime_row("uid-a", "default", "web", Some("sb-missing"))],
            std::slice::from_ref(&pod),
            &[],
            &[],
        );

        assert_eq!(
            actions,
            vec![CriInventoryAction::RecreateMissingSandbox {
                key: PodLifecycleKey::new("default", "web", "uid-a"),
                pod,
            }]
        );
    }

    #[test]
    fn cold_orphan_in_cri_killed() {
        let actions = diff_cri_inventory(
            true,
            &[],
            &[],
            &[sandbox("sb-cold", "default", "cold-pod", "uid-cold")],
            &[],
        );

        // A cold sandbox with pod identity routes through actor-owned orphan
        // cleanup (key carried), not a CRI-only teardown.
        assert_eq!(
            actions,
            vec![CriInventoryAction::KillColdSandbox {
                sandbox_id: "sb-cold".to_string(),
                key: Some(PodLifecycleKey::new("default", "cold-pod", "uid-cold")),
            }]
        );
    }

    #[test]
    fn cold_orphan_without_identity_falls_back_to_cri_teardown() {
        let actions = diff_cri_inventory(true, &[], &[], &[sandbox("sb-bare", "", "", "")], &[]);

        // No identity stamped on the sandbox: no key, so apply_actions does a
        // CRI-only teardown rather than routing through orphan finalize.
        assert_eq!(
            actions,
            vec![CriInventoryAction::KillColdSandbox {
                sandbox_id: "sb-bare".to_string(),
                key: None,
            }]
        );
    }

    #[test]
    fn exited_container_triggers_runtime_reconcile() {
        let pod = pod("default", "web", "uid-a");
        let actions = diff_cri_inventory(
            true,
            &[runtime_row("uid-a", "default", "web", Some("sb-a"))],
            std::slice::from_ref(&pod),
            &[sandbox("sb-a", "default", "web", "uid-a")],
            &[CriContainerInventory {
                sandbox_id: "sb-a".to_string(),
                container_id: "ctr-a".to_string(),
                state: ContainerRuntimeState::Exited,
            }],
        );

        assert!(
            actions
                .iter()
                .any(|action| matches!(action, CriInventoryAction::ReconcileRuntime { key } if key.uid == "uid-a")),
            "exited containers observed after reconnect must enqueue runtime reconcile"
        );
    }

    fn actor_router_with_recorder() -> (
        std::sync::Arc<PodLifecycleRouter>,
        std::sync::Arc<crate::kubelet::pod_lifecycle_router::executor::RecordingExecutor>,
    ) {
        use crate::kubelet::pod_lifecycle_actor::config::PodLifecycleConcurrencyConfig;
        use crate::kubelet::pod_lifecycle_actor::registry::PodLifecycleRegistry;
        use crate::kubelet::pod_lifecycle_router::executor::{PodWorkExecutor, RecordingExecutor};

        let recorder = RecordingExecutor::new();
        let supervisor = std::sync::Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let holder = std::sync::Arc::new(std::sync::Mutex::new(
            recorder.clone() as std::sync::Arc<dyn PodWorkExecutor>
        ));
        let registry = std::sync::Arc::new(PodLifecycleRegistry::new(
            supervisor,
            PodLifecycleConcurrencyConfig::production_default(),
            holder,
        ));
        let router = std::sync::Arc::new(PodLifecycleRouter::new_actor_with_executor(
            registry,
            recorder.clone(),
        ));
        (router, recorder)
    }

    /// A cold sandbox WITH a recovered pod identity must route through
    /// actor-owned orphan finalize (so the shared cleanup reclaims pod
    /// dir/cgroup/volumes), NOT a CRI-only sandbox teardown.
    #[tokio::test]
    async fn cleanup_cold_sandbox_with_identity_routes_to_orphan_finalize() {
        use crate::kubelet::pod_runtime::test_support::{MockCriOperation, MockCriRuntime};

        let (router, recorder) = actor_router_with_recorder();
        let cri = MockCriRuntime::new();
        let key = PodLifecycleKey::new("default", "cold-pod", "uid-cold");

        cleanup_cold_sandbox(&router, &cri, "sb-cold", Some(&key))
            .await
            .unwrap();

        // The orphan finalize is processed by the per-UID actor, which produces
        // a stop PodAction captured by the recording executor.
        let mut produced = false;
        for _ in 0..1000 {
            if recorder.action_count() >= 1 {
                produced = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        assert!(
            produced,
            "cold sandbox with identity must enqueue actor-owned orphan finalization"
        );
        // The reconciler must NOT short-circuit to a sandbox-only teardown.
        let cri_teardown = cri.recorded_calls().into_iter().any(|call| {
            matches!(
                call.operation,
                MockCriOperation::StopPodSandbox(_) | MockCriOperation::RemovePodSandbox(_)
            )
        });
        assert!(
            !cri_teardown,
            "identity-bearing cold sandbox must not be torn down CRI-only"
        );
    }

    /// A cold sandbox with NO identity has no UID to key an actor on, so it
    /// falls back to a direct CRI sandbox teardown.
    #[tokio::test]
    async fn cleanup_cold_sandbox_without_identity_tears_down_cri_only() {
        use crate::kubelet::pod_runtime::test_support::{MockCriOperation, MockCriRuntime};

        let (router, recorder) = actor_router_with_recorder();
        let cri = MockCriRuntime::new();

        cleanup_cold_sandbox(&router, &cri, "sb-bare", None)
            .await
            .unwrap();

        let ops: Vec<MockCriOperation> = cri
            .recorded_calls()
            .into_iter()
            .map(|call| call.operation)
            .collect();
        assert!(
            ops.iter()
                .any(|op| matches!(op, MockCriOperation::StopPodSandbox(id) if id == "sb-bare")),
            "identity-less cold sandbox must be stopped via CRI; saw {ops:?}"
        );
        assert!(
            ops.iter()
                .any(|op| matches!(op, MockCriOperation::RemovePodSandbox(id) if id == "sb-bare")),
            "identity-less cold sandbox must be removed via CRI; saw {ops:?}"
        );
        assert_eq!(
            recorder.action_count(),
            0,
            "identity-less cold sandbox must not enqueue actor work"
        );
    }
}
