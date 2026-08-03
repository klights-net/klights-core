use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;
use klights_pod_api::{PodGetRequest, PodListRequest};
use klights_reconcile_api::GcPodDeleteRequest;
use klights_supervisor::TaskSupervisor;
use klights_types::PodIdentity;
use serde_json::Value;

use super::{PodSideEffectPortsSlot, SideEffect};

#[async_trait]
pub trait NodeTaintNodeStore: Send + Sync {
    async fn get_node(&self, name: &str) -> Result<Option<Resource>>;
}

struct NodeTaintManager {
    pod_repository: PodSideEffectPortsSlot,
    task_supervisor: Option<Arc<TaskSupervisor>>,
    node_store: Option<Arc<dyn NodeTaintNodeStore>>,
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
            self.node_store.clone(),
            node,
        )
        .await
    }
}

pub fn effect(
    pod_repository: PodSideEffectPortsSlot,
    task_supervisor: Option<Arc<TaskSupervisor>>,
    node_store: Option<Arc<dyn NodeTaintNodeStore>>,
) -> Arc<dyn SideEffect> {
    Arc::new(NodeTaintManager {
        pod_repository,
        task_supervisor,
        node_store,
    })
}

pub async fn reconcile_node_noexecute_taints(
    pod_slot: PodSideEffectPortsSlot,
    task_supervisor: Option<Arc<TaskSupervisor>>,
    node_store: Option<Arc<dyn NodeTaintNodeStore>>,
    node: &Value,
) -> Result<()> {
    let node_name = node
        .pointer("/metadata/name")
        .and_then(Value::as_str)
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
        if pod.data.pointer("/spec/nodeName").and_then(Value::as_str) != Some(node_name) {
            continue;
        }
        if pod
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(Value::as_str)
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
                let node_store_for_task = node_store.clone();
                let node_name_for_task = node_name.to_string();
                let namespace = pod
                    .data
                    .pointer("/metadata/namespace")
                    .and_then(Value::as_str)
                    .unwrap_or("default")
                    .to_string();
                let name = pod
                    .data
                    .pointer("/metadata/name")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                if name.is_empty() {
                    continue;
                }
                let _ = supervisor
                    .spawn_delay("node_taint_noexecute_eviction", delay, async move {
                        recheck_and_evict_pod(
                            pod_slot_for_task,
                            node_store_for_task,
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
    node_store: Option<Arc<dyn NodeTaintNodeStore>>,
    node_name: String,
    namespace: String,
    name: String,
) {
    let Some(pods) = pod_slot.query() else {
        return;
    };
    let Some(node_store) = node_store else {
        return;
    };
    let Ok(Some(node)) = node_store.get_node(&node_name).await else {
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
        .and_then(Value::as_str)
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
        .and_then(Value::as_str)
        .unwrap_or("default");
    let Some(name) = pod.pointer("/metadata/name").and_then(Value::as_str) else {
        return;
    };
    let Some(uid) = pod.pointer("/metadata/uid").and_then(Value::as_str) else {
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

pub const NODE_NOT_READY_TAINT_KEY: &str = "node.kubernetes.io/not-ready";
pub const NODE_NOT_READY_TAINT_EFFECT: &str = "NoExecute";
pub const NODE_NOT_READY_TAINT_VALUE: &str = "true";

#[derive(Debug, PartialEq, Eq)]
pub enum EvictionAction {
    None,
    Now,
    After(Duration),
}

pub fn eviction_action_for_pod(pod: &Value, taints: &[Value]) -> EvictionAction {
    if taints.is_empty() {
        return EvictionAction::None;
    }

    let tolerations = pod
        .pointer("/spec/tolerations")
        .and_then(|v| v.as_array())
        .map(Vec::as_slice)
        .unwrap_or(&[]);

    let mut shortest_delay: Option<Duration> = None;
    for taint in taints {
        let Some(toleration) = tolerations
            .iter()
            .find(|toleration| toleration_matches_taint(toleration, taint))
        else {
            return EvictionAction::Now;
        };

        if let Some(seconds) = toleration
            .get("tolerationSeconds")
            .and_then(|v| v.as_i64())
            .filter(|seconds| *seconds >= 0)
        {
            let delay = Duration::from_secs(seconds as u64);
            shortest_delay = Some(shortest_delay.map_or(delay, |current| current.min(delay)));
        }
    }

    shortest_delay.map_or(EvictionAction::None, |delay| {
        if delay.is_zero() {
            EvictionAction::Now
        } else {
            EvictionAction::After(delay)
        }
    })
}

pub fn noexecute_taints(node: &Value) -> Vec<Value> {
    let mut taints = node
        .pointer("/spec/taints")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
        .filter(|taint| taint.get("effect").and_then(|v| v.as_str()) == Some("NoExecute"))
        .cloned()
        .collect::<Vec<_>>();

    let has_not_ready_taint = taints.iter().any(|taint| {
        taint.get("key").and_then(|v| v.as_str()) == Some(NODE_NOT_READY_TAINT_KEY)
            && taint.get("effect").and_then(|v| v.as_str()) == Some(NODE_NOT_READY_TAINT_EFFECT)
    });

    if is_node_not_ready(node) && !has_not_ready_taint {
        taints.push(serde_json::json!({
            "key": NODE_NOT_READY_TAINT_KEY,
            "value": NODE_NOT_READY_TAINT_VALUE,
            "effect": NODE_NOT_READY_TAINT_EFFECT
        }));
    }

    taints
}

fn is_node_not_ready(node: &Value) -> bool {
    node.pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
        .any(|condition| {
            condition.get("type").and_then(|v| v.as_str()) == Some("Ready")
                && condition.get("status").and_then(|v| v.as_str()) != Some("True")
        })
}

fn toleration_matches_taint(toleration: &Value, taint: &Value) -> bool {
    let taint_key = taint.get("key").and_then(|v| v.as_str()).unwrap_or("");
    let taint_value = taint.get("value").and_then(|v| v.as_str()).unwrap_or("");
    let taint_effect = taint.get("effect").and_then(|v| v.as_str()).unwrap_or("");

    let toleration_key = toleration.get("key").and_then(|v| v.as_str()).unwrap_or("");
    let toleration_value = toleration
        .get("value")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let toleration_effect = toleration
        .get("effect")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let operator = toleration
        .get("operator")
        .and_then(|v| v.as_str())
        .unwrap_or("Equal");

    if !toleration_effect.is_empty() && toleration_effect != taint_effect {
        return false;
    }

    match operator {
        "Exists" => toleration_key.is_empty() || toleration_key == taint_key,
        _ => toleration_key == taint_key && toleration_value == taint_value,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn noexecute_taint_without_toleration_evicts_now() {
        let pod = pod_with_tolerations("p", json!([]));
        let taints = vec![noexecute_taint()];

        assert_eq!(eviction_action_for_pod(&pod, &taints), EvictionAction::Now);
    }

    #[test]
    fn noexecute_taint_with_indefinite_toleration_does_not_evict() {
        let pod = pod_with_tolerations(
            "p",
            json!([{
                "key": "kubernetes.io/e2e-evict-taint-key",
                "operator": "Equal",
                "value": "evictTaintVal",
                "effect": "NoExecute"
            }]),
        );
        let taints = vec![noexecute_taint()];

        assert_eq!(eviction_action_for_pod(&pod, &taints), EvictionAction::None);
    }

    #[test]
    fn noexecute_taint_with_toleration_seconds_delays_eviction() {
        let pod = pod_with_tolerations(
            "p",
            json!([{
                "key": "kubernetes.io/e2e-evict-taint-key",
                "operator": "Equal",
                "value": "evictTaintVal",
                "effect": "NoExecute",
                "tolerationSeconds": 1
            }]),
        );
        let taints = vec![noexecute_taint()];

        assert_eq!(
            eviction_action_for_pod(&pod, &taints),
            EvictionAction::After(Duration::from_secs(1))
        );
    }

    #[test]
    fn ready_unknown_taint_triggers_noexecute_eviction() {
        let pod = pod_with_tolerations("p", json!([]));
        let taints = noexecute_taints(&json!({
            "status": {
                "conditions": [{
                    "type": "Ready",
                    "status": "Unknown",
                    "reason": "NodeStatusUnknown",
                    "message": "Kubelet stopped posting node status.",
                    "lastHeartbeatTime": "2026-05-13T06:34:15Z",
                    "lastTransitionTime": "2026-05-13T06:34:15Z"
                }]
            }
        }));

        assert_eq!(eviction_action_for_pod(&pod, &taints), EvictionAction::Now);
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
