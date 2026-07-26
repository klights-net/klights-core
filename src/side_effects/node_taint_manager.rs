use std::time::Duration;

use serde_json::Value;

pub(crate) const NODE_NOT_READY_TAINT_KEY: &str = "node.kubernetes.io/not-ready";
pub(crate) const NODE_NOT_READY_TAINT_EFFECT: &str = "NoExecute";
pub(crate) const NODE_NOT_READY_TAINT_VALUE: &str = "true";

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum EvictionAction {
    None,
    Now,
    After(Duration),
}

pub(crate) fn eviction_action_for_pod(pod: &Value, taints: &[Value]) -> EvictionAction {
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

pub(crate) fn noexecute_taints(node: &Value) -> Vec<Value> {
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
    use crate::side_effects::PodSideEffectPortsSlot;
    use klights_supervisor::TaskSupervisor;
    use serde_json::json;
    use std::sync::Arc;

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
        let taints = node_ready_unknown_taints();

        assert_eq!(eviction_action_for_pod(&pod, &taints), EvictionAction::Now);
    }

    #[tokio::test]
    async fn node_noexecute_taint_deletes_untolerated_pod() {
        let (db, db_handle, slot, _supervisor) = fixture().await;
        let node = create_node(&db, vec![noexecute_taint()]).await;
        create_pod(&db, "untolerated", json!([])).await;

        crate::node_taint_manager_side_effect_adapter::reconcile_node_noexecute_taints(
            slot,
            None,
            Some(db_handle),
            &node.data,
        )
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

        crate::node_taint_manager_side_effect_adapter::reconcile_node_noexecute_taints(
            slot,
            None,
            Some(db_handle),
            &node.data,
        )
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

        crate::node_taint_manager_side_effect_adapter::reconcile_node_noexecute_taints(
            slot,
            Some(supervisor.clone()),
            Some(db_handle),
            &node.data,
        )
        .await
        .unwrap();

        supervisor
            .sleep("node_taint_manager_test_wait", Duration::from_millis(1200))
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

        crate::node_taint_manager_side_effect_adapter::reconcile_node_noexecute_taints(
            slot,
            Some(supervisor.clone()),
            Some(db_handle),
            &node.data,
        )
        .await
        .unwrap();

        let mut untainted_node: Value = std::sync::Arc::unwrap_or_clone(node.data);
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
            .sleep("node_taint_manager_test_wait", Duration::from_millis(1200))
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
        let metrics = crate::side_effects::SideEffectMetrics::new();
        let side_effects = Arc::new(crate::side_effects::SideEffectRegistry::new());
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

    fn node_ready_unknown_taints() -> Vec<Value> {
        noexecute_taints(&json!({
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
        }))
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
            "metadata": {
                "namespace": "default",
                "name": name
            },
            "spec": {
                "nodeName": "node-a",
                "tolerations": tolerations,
                "containers": [{"name": "c", "image": "pause"}]
            }
        })
    }
}
