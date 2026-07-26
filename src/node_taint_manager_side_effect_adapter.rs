use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use klights_pod_api::{PodGetRequest, PodListRequest};
use klights_reconcile_api::GcPodDeleteRequest;
use klights_supervisor::TaskSupervisor;
use klights_types::PodIdentity;
use serde_json::Value;

use crate::datastore::DatastoreHandle;
use crate::side_effects::node_taint_manager::{
    EvictionAction, eviction_action_for_pod, noexecute_taints,
};
use crate::side_effects::{PodSideEffectPortsSlot, SideEffect};

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
