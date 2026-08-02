use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use klights_pod_api::{PodGetRequest, PodListRequest};
use klights_reconcile_api::GcPodDeleteRequest;
use klights_supervisor::TaskSupervisor;
use klights_types::PodIdentity;
use serde_json::Value;

use crate::datastore::DatastoreHandle;
use klights_controllers::side_effects::node_taint_manager::{
    EvictionAction, eviction_action_for_pod, noexecute_taints,
};
use klights_controllers::side_effects::{PodSideEffectPortsSlot, SideEffect};

pub fn node_taint_manager(
    pod_repository: PodSideEffectPortsSlot,
    task_supervisor: Option<Arc<TaskSupervisor>>,
    db: Option<DatastoreHandle>,
) -> Arc<dyn SideEffect> {
    Arc::new(NodeTaintManager {
        pod_repository,
        task_supervisor,
        db,
    })
}

struct NodeTaintManager {
    pod_repository: PodSideEffectPortsSlot,
    task_supervisor: Option<Arc<TaskSupervisor>>,
    db: Option<DatastoreHandle>,
}

#[async_trait]
impl SideEffect for NodeTaintManager {
    fn name(&self) -> &'static str {
        "node_taint_manager"
    }

    async fn apply(&self, node: &Value) -> Result<()> {
        reconcile_node_noexecute_taints(
            self.pod_repository.clone(),
            self.task_supervisor.clone(),
            self.db.clone(),
            node,
        )
        .await
    }
}

pub(crate) async fn reconcile_node_noexecute_taints(
    pod_slot: PodSideEffectPortsSlot,
    task_supervisor: Option<Arc<TaskSupervisor>>,
    db: Option<DatastoreHandle>,
    node: &Value,
) -> Result<()> {
    let node_name = node
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if node_name.is_empty() {
        return Ok(());
    }

    let taints = noexecute_taints(node);
    if taints.is_empty() {
        return Ok(());
    }

    let Some(pods) = pod_slot.query() else {
        tracing::debug!("node_taint_manager: pod repository is not bound yet");
        return Ok(());
    };

    let pod_list = pods
        .list_pods(PodListRequest::try_new(None, None, None, None, None)?)
        .await?;
    for pod in pod_list.into_parts().0 {
        if pod.data.pointer("/spec/nodeName").and_then(|v| v.as_str()) != Some(node_name) {
            continue;
        }
        if pod
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str())
            .is_some()
        {
            continue;
        }

        match eviction_action_for_pod(&pod.data, &taints) {
            EvictionAction::None => {}
            EvictionAction::Now => {
                evict_pod(pod_slot.clone(), Arc::unwrap_or_clone(pod.data)).await;
            }
            EvictionAction::After(delay) => {
                let Some(supervisor) = task_supervisor.clone() else {
                    continue;
                };
                let pod_slot_for_task = pod_slot.clone();
                let db_for_task = db.clone();
                let node_name_for_task = node_name.to_string();
                let namespace = pod
                    .data
                    .pointer("/metadata/namespace")
                    .and_then(|v| v.as_str())
                    .unwrap_or("default")
                    .to_string();
                let name = pod
                    .data
                    .pointer("/metadata/name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if name.is_empty() {
                    continue;
                }
                let _ = supervisor
                    .spawn_delay("node_taint_noexecute_eviction", delay, async move {
                        recheck_and_evict_pod(
                            pod_slot_for_task,
                            db_for_task,
                            node_name_for_task,
                            namespace,
                            name,
                        )
                        .await;
                    })
                    .await;
            }
        }
    }

    Ok(())
}

async fn recheck_and_evict_pod(
    pod_slot: PodSideEffectPortsSlot,
    db: Option<DatastoreHandle>,
    node_name: String,
    namespace: String,
    name: String,
) {
    let Some(pods) = pod_slot.query() else {
        return;
    };
    let Some(db) = db else {
        return;
    };
    let Ok(Some(node)) = db.get_resource("v1", "Node", None, &node_name).await else {
        return;
    };

    let Ok(request) = PodGetRequest::try_by_name(namespace.clone(), name.clone()) else {
        return;
    };
    let Ok(Some(pod)) = pods.get_pod(request).await else {
        return;
    };
    if pod
        .data
        .pointer("/metadata/deletionTimestamp")
        .and_then(|v| v.as_str())
        .is_some()
    {
        return;
    }

    let taints = noexecute_taints(&node.data);
    if !matches!(
        eviction_action_for_pod(&pod.data, &taints),
        EvictionAction::None
    ) {
        evict_pod(pod_slot, Arc::unwrap_or_clone(pod.data)).await;
    }
}

async fn evict_pod(pod_slot: PodSideEffectPortsSlot, pod: Value) {
    let Some(delete_sink) = pod_slot.delete() else {
        return;
    };
    let namespace = pod
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        .unwrap_or("default");
    let Some(name) = pod.pointer("/metadata/name").and_then(|v| v.as_str()) else {
        return;
    };
    let Some(uid) = pod.pointer("/metadata/uid").and_then(|v| v.as_str()) else {
        return;
    };
    if let Err(err) = delete_sink
        .request_gc_pod_delete(GcPodDeleteRequest::new(PodIdentity::new(
            namespace, name, uid,
        )))
        .await
    {
        tracing::warn!(namespace, name, error = %err, "node_taint_manager: pod eviction failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn node_noexecute_taint_deletes_untolerated_pod() {
        let (db, db_handle, slot, _supervisor) = fixture().await;
        let node = create_node(&db, vec![noexecute_taint()]).await;
        create_pod(&db, "untolerated", json!([])).await;

        reconcile_node_noexecute_taints(slot, None, Some(db_handle), &node.data)
            .await
            .unwrap();

        let pod = db
            .get_resource("v1", "Pod", Some("default"), "untolerated")
            .await
            .unwrap();
        let pod = pod.expect("untolerated NoExecute pod row remains until actor finalization");
        assert!(
            pod.data.pointer("/metadata/deletionTimestamp").is_some(),
            "untolerated NoExecute pod must be marked terminating for actor-owned eviction"
        );
    }

    #[tokio::test]
    async fn node_ready_unknown_deletes_untolerated_pod() {
        let (db, db_handle, slot, _supervisor) = fixture().await;
        let node = create_node_with_status(
            &db,
            vec![],
            json!({
                "conditions": [{
                    "type": "Ready",
                    "status": "Unknown",
                    "reason": "NodeStatusUnknown",
                    "message": "Kubelet stopped posting node status.",
                    "lastHeartbeatTime": "2026-05-13T06:34:15Z",
                    "lastTransitionTime": "2026-05-13T06:34:15Z"
                }]
            }),
        )
        .await;
        create_pod(&db, "ready-unknown", json!([])).await;

        reconcile_node_noexecute_taints(slot, None, Some(db_handle), &node.data)
            .await
            .unwrap();

        let pod = db
            .get_resource("v1", "Pod", Some("default"), "ready-unknown")
            .await
            .unwrap();
        let pod = pod.expect("ready-unknown pod row remains until actor finalization");
        assert!(
            pod.data.pointer("/metadata/deletionTimestamp").is_some(),
            "ready-unknown Node must evict untolerated pod"
        );
    }

    #[tokio::test]
    async fn delayed_noexecute_eviction_deletes_pod_when_taint_remains() {
        let (db, db_handle, slot, supervisor) = fixture().await;
        let node = create_node(&db, vec![noexecute_taint()]).await;
        create_pod(
            &db,
            "delayed-evict",
            json!([{
                "key": "kubernetes.io/e2e-evict-taint-key",
                "operator": "Equal",
                "value": "evictTaintVal",
                "effect": "NoExecute",
                "tolerationSeconds": 1
            }]),
        )
        .await;

        reconcile_node_noexecute_taints(
            slot,
            Some(supervisor.clone()),
            Some(db_handle),
            &node.data,
        )
        .await
        .unwrap();

        supervisor
            .sleep(
                "node_taint_manager_test_wait",
                std::time::Duration::from_millis(1200),
            )
            .await
            .unwrap();
        let pod = db
            .get_resource("v1", "Pod", Some("default"), "delayed-evict")
            .await
            .unwrap();
        let pod = pod.expect("delayed NoExecute pod row remains until actor finalization");
        assert!(
            pod.data.pointer("/metadata/deletionTimestamp").is_some(),
            "pod must be marked terminating when NoExecute taint remains after tolerationSeconds"
        );
    }

    #[tokio::test]
    async fn delayed_noexecute_eviction_rechecks_removed_taint_before_delete() {
        let (db, db_handle, slot, supervisor) = fixture().await;
        let node = create_node(&db, vec![noexecute_taint()]).await;
        create_pod(
            &db,
            "delayed",
            json!([{
                "key": "kubernetes.io/e2e-evict-taint-key",
                "operator": "Equal",
                "value": "evictTaintVal",
                "effect": "NoExecute",
                "tolerationSeconds": 1
            }]),
        )
        .await;

        reconcile_node_noexecute_taints(
            slot,
            Some(supervisor.clone()),
            Some(db_handle),
            &node.data,
        )
        .await
        .unwrap();

        let mut untainted_node: Value = Arc::unwrap_or_clone(node.data);
        untainted_node["spec"]["taints"] = json!([]);
        db.update_resource(
            "v1",
            "Node",
            None,
            "node-a",
            untainted_node,
            node.resource_version,
        )
        .await
        .unwrap();

        supervisor
            .sleep(
                "node_taint_manager_test_wait",
                std::time::Duration::from_millis(1200),
            )
            .await
            .unwrap();
        let pod = db
            .get_resource("v1", "Pod", Some("default"), "delayed")
            .await
            .unwrap();
        assert!(
            pod.is_some(),
            "pod must survive when NoExecute taint is removed before toleration expires"
        );
    }

    async fn fixture() -> (
        crate::datastore::sqlite::Datastore,
        crate::datastore::DatastoreHandle,
        PodSideEffectPortsSlot,
        Arc<TaskSupervisor>,
    ) {
        let db = crate::datastore::test_support::in_memory().await;
        let db_handle: crate::datastore::DatastoreHandle = Arc::new(db.clone());
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let metrics = klights_controllers::side_effects::SideEffectMetrics::new();
        let side_effects = Arc::new(klights_controllers::side_effects::SideEffectRegistry::new());
        let repository = Arc::new(crate::kubelet::pod_repository::PodRepository::new(
            db_handle.clone(),
            supervisor.clone(),
            side_effects,
            metrics,
        ));
        let slot = PodSideEffectPortsSlot::new();
        slot.set(repository.clone(), repository);
        (db, db_handle, slot, supervisor)
    }

    async fn create_node(
        db: &crate::datastore::sqlite::Datastore,
        taints: Vec<Value>,
    ) -> crate::datastore::Resource {
        create_node_with_status(
            db,
            taints,
            json!({
                "conditions": [{
                    "type": "Ready",
                    "status": "True",
                    "reason": "KubeletReady",
                    "message": "klights is ready",
                    "lastHeartbeatTime": "2026-05-13T06:34:15Z",
                    "lastTransitionTime": "2026-05-13T06:34:15Z"
                }]
            }),
        )
        .await
    }

    async fn create_node_with_status(
        db: &crate::datastore::sqlite::Datastore,
        taints: Vec<Value>,
        status: Value,
    ) -> crate::datastore::Resource {
        db.create_resource(
            "v1",
            "Node",
            None,
            "node-a",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "node-a"},
                "spec": {"taints": taints},
                "status": status
            }),
        )
        .await
        .unwrap()
    }

    async fn create_pod(db: &crate::datastore::sqlite::Datastore, name: &str, tolerations: Value) {
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            name,
            pod_with_tolerations(name, tolerations),
        )
        .await
        .unwrap();
    }

    fn noexecute_taint() -> Value {
        json!({
            "key": "kubernetes.io/e2e-evict-taint-key",
            "value": "evictTaintVal",
            "effect": "NoExecute"
        })
    }

    fn pod_with_tolerations(name: &str, tolerations: Value) -> Value {
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"namespace": "default", "name": name},
            "spec": {
                "nodeName": "node-a",
                "tolerations": tolerations,
                "containers": [{"name": "c", "image": "pause"}]
            }
        })
    }
}
