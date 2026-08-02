use std::time::Duration;

use serde_json::Value;

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
