use std::collections::{HashMap, HashSet};

use crate::datastore::node_local::PodRuntimeRow;
use crate::kubelet::pod_lifecycle_core::message::PodLifecycleKey;
use crate::kubelet::pod_lifecycle_router::OrphanReason;
use crate::kubelet::pod_runtime::cri::ContainerRuntimeState;

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
    },
    RefuseEmptyCache,
}

pub fn diff_cri_inventory(
    cache_primed: bool,
    runtime_rows: &[PodRuntimeRow],
    leader_pods: &[serde_json::Value],
    cri_sandbox_ids: &[String],
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

    let cri_sandboxes = cri_sandbox_ids.iter().cloned().collect::<HashSet<_>>();
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
                Some(sandbox_id) if cri_sandboxes.contains(sandbox_id) => {
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
                .is_some_and(|id| cri_sandboxes.contains(id)) =>
            {
                actions.push(CriInventoryAction::FinalizeOrphan {
                    key,
                    reason: OrphanReason::LeaderDeletedWhileDown,
                });
            }
            None => actions.push(CriInventoryAction::DropLocalRows { key }),
        }
    }

    for sandbox_id in cri_sandboxes {
        if !runtime_sandboxes.contains(&sandbox_id) {
            actions.push(CriInventoryAction::KillColdSandbox { sandbox_id });
        }
    }

    actions
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
            &["sb-a".to_string()],
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
        let actions = diff_cri_inventory(true, &[], &[], &["sb-cold".to_string()], &[]);

        assert_eq!(
            actions,
            vec![CriInventoryAction::KillColdSandbox {
                sandbox_id: "sb-cold".to_string()
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
            &["sb-a".to_string()],
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
}
