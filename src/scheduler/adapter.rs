//! Scheduler adapter — bridges `serde_json::Value` (K8s API) to typed scheduler structs.
//!
//! Pure functions that extract scheduling-relevant data from JSON pod/node
//! objects into the typed structs used by predicates/scoring/preemption.

use super::types::*;
use std::collections::HashMap;

/// Extract a `SchedulableNode` from a JSON Node object.
pub fn extract_schedulable_node(node_value: &serde_json::Value) -> SchedulableNode {
    let name = node_value
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let unschedulable = node_value
        .pointer("/spec/unschedulable")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let ready = node_value
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .is_some_and(|conditions| {
            conditions.iter().any(|condition| {
                condition.get("type").and_then(|v| v.as_str()) == Some("Ready")
                    && condition.get("status").and_then(|v| v.as_str()) == Some("True")
            })
        });

    let labels = node_value
        .pointer("/metadata/labels")
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();

    let taints = node_value
        .pointer("/spec/taints")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(extract_taint).collect())
        .unwrap_or_default();

    let allocatable = extract_node_resources(node_value, "/status/allocatable");
    let capacity = extract_node_resources(node_value, "/status/capacity");

    SchedulableNode {
        name,
        ready,
        unschedulable,
        taints,
        labels,
        allocatable,
        capacity,
    }
}

/// Extract `PodSchedulingConstraints` from a JSON Pod object.
pub fn extract_pod_constraints(pod_value: &serde_json::Value) -> PodSchedulingConstraints {
    let namespace = pod_value
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        .unwrap_or("default")
        .to_string();
    let labels = extract_labels(pod_value, "/metadata/labels");

    let node_selector = pod_value
        .pointer("/spec/nodeSelector")
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();

    let required_node_affinity = pod_value
        .pointer("/spec/affinity/nodeAffinity/requiredDuringSchedulingIgnoredDuringExecution/nodeSelectorTerms")
        .and_then(|v| v.as_array())
        .map(|terms| terms.iter().filter_map(extract_node_selector_term).collect())
        .unwrap_or_default();

    let required_pod_affinity = pod_value
        .pointer("/spec/affinity/podAffinity/requiredDuringSchedulingIgnoredDuringExecution")
        .and_then(|v| v.as_array())
        .map(|terms| terms.iter().filter_map(extract_pod_affinity_term).collect())
        .unwrap_or_default();

    let required_pod_anti_affinity = pod_value
        .pointer("/spec/affinity/podAntiAffinity/requiredDuringSchedulingIgnoredDuringExecution")
        .and_then(|v| v.as_array())
        .map(|terms| terms.iter().filter_map(extract_pod_affinity_term).collect())
        .unwrap_or_default();

    let topology_spread_constraints = pod_value
        .pointer("/spec/topologySpreadConstraints")
        .and_then(|v| v.as_array())
        .map(|constraints| {
            constraints
                .iter()
                .filter_map(extract_topology_spread_constraint)
                .collect()
        })
        .unwrap_or_default();

    let tolerations = pod_value
        .pointer("/spec/tolerations")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(extract_toleration).collect())
        .unwrap_or_default();

    let resources = extract_pod_resources(pod_value);

    let priority = pod_value
        .pointer("/spec/priority")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    let priority_class_name = pod_value
        .pointer("/spec/priorityClassName")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(ToString::to_string);

    let preemption_policy = pod_value
        .pointer("/spec/preemptionPolicy")
        .and_then(|v| v.as_str())
        .and_then(|s| match s {
            "PreemptLowerPriority" => Some(PreemptionPolicy::PreemptLowerPriority),
            "Never" => Some(PreemptionPolicy::Never),
            _ => None,
        });

    PodSchedulingConstraints {
        namespace,
        labels,
        node_selector,
        required_node_affinity,
        required_pod_affinity,
        required_pod_anti_affinity,
        topology_spread_constraints,
        tolerations,
        resources,
        host_port_requests: Vec::new(), // Not needed for scheduling predicates
        priority,
        priority_class_name,
        preemption_policy,
    }
}

/// Extract `PodResources` (effective requests including overhead) from a JSON Pod object.
pub fn extract_existing_pod_resources(pod_value: &serde_json::Value) -> PodResources {
    extract_pod_resources(pod_value)
}

/// Extract `ExistingPod` (for preemption) from a JSON Pod object.
pub fn extract_existing_pod(
    pod_value: &serde_json::Value,
) -> crate::scheduler::preemption::ExistingPod {
    let namespace = pod_value
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        .unwrap_or("default")
        .to_string();
    let name = pod_value
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let priority = pod_value
        .pointer("/spec/priority")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let resources = extract_pod_resources(pod_value);
    let labels = extract_labels(pod_value, "/metadata/labels");

    crate::scheduler::preemption::ExistingPod {
        namespace,
        name,
        priority,
        resources,
        labels,
    }
}

// ---- Internal helpers ----

fn extract_labels(pod_value: &serde_json::Value, pointer: &str) -> HashMap<String, String> {
    pod_value
        .pointer(pointer)
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

fn extract_taint(v: &serde_json::Value) -> Option<Taint> {
    let key = v.get("key")?.as_str()?.to_string();
    let value = v
        .get("value")
        .and_then(|v| v.as_str())
        .map(ToString::to_string);
    let effect = v.get("effect")?.as_str().and_then(|s| match s {
        "NoSchedule" => Some(TaintEffect::NoSchedule),
        "PreferNoSchedule" => Some(TaintEffect::PreferNoSchedule),
        "NoExecute" => Some(TaintEffect::NoExecute),
        _ => None,
    })?;
    Some(Taint { key, value, effect })
}

fn extract_toleration(v: &serde_json::Value) -> Option<Toleration> {
    let operator = v
        .get("operator")
        .and_then(|v| v.as_str())
        .map(|s| match s {
            "Exists" => TolerationOperator::Exists,
            _ => TolerationOperator::Equal,
        })
        .unwrap_or(TolerationOperator::Equal);
    let key = v
        .get("key")
        .and_then(|v| v.as_str())
        .map(ToString::to_string);
    let value = v
        .get("value")
        .and_then(|v| v.as_str())
        .map(ToString::to_string);
    let effect = v
        .get("effect")
        .and_then(|v| v.as_str())
        .and_then(|s| match s {
            "NoSchedule" => Some(TaintEffect::NoSchedule),
            "PreferNoSchedule" => Some(TaintEffect::PreferNoSchedule),
            "NoExecute" => Some(TaintEffect::NoExecute),
            _ => None,
        });
    Some(Toleration {
        key,
        value,
        operator,
        effect,
    })
}

fn extract_node_selector_term(v: &serde_json::Value) -> Option<NodeSelectorTerm> {
    let match_expressions = v
        .get("matchExpressions")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(extract_node_selector_requirement)
                .collect()
        })
        .unwrap_or_default();
    let match_fields = v
        .get("matchFields")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(extract_node_selector_requirement)
                .collect()
        })
        .unwrap_or_default();
    Some(NodeSelectorTerm {
        match_expressions,
        match_fields,
    })
}

fn extract_node_selector_requirement(v: &serde_json::Value) -> Option<NodeSelectorRequirement> {
    let key = v.get("key")?.as_str()?.to_string();
    let operator = v.get("operator")?.as_str().and_then(|s| match s {
        "In" => Some(NodeSelectorOperator::In),
        "NotIn" => Some(NodeSelectorOperator::NotIn),
        "Exists" => Some(NodeSelectorOperator::Exists),
        "DoesNotExist" => Some(NodeSelectorOperator::DoesNotExist),
        "Gt" => Some(NodeSelectorOperator::Gt),
        "Lt" => Some(NodeSelectorOperator::Lt),
        _ => None,
    })?;
    let values = v
        .get("values")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(ToString::to_string))
                .collect()
        })
        .unwrap_or_default();
    Some(NodeSelectorRequirement {
        key,
        operator,
        values,
    })
}

fn extract_pod_affinity_term(v: &serde_json::Value) -> Option<PodAffinityTerm> {
    let topology_key = v.get("topologyKey")?.as_str()?.to_string();
    let label_selector = v.get("labelSelector").and_then(extract_label_selector_term);
    let namespaces = v.get("namespaces").and_then(|value| {
        value.as_array().map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(ToString::to_string))
                .collect::<Vec<_>>()
        })
    });
    let namespace_selector = v
        .get("namespaceSelector")
        .and_then(extract_label_selector_term);
    Some(PodAffinityTerm {
        label_selector,
        namespaces,
        namespace_selector,
        topology_key,
    })
}

pub(crate) fn extract_label_selector_term(v: &serde_json::Value) -> Option<LabelSelectorTerm> {
    let mut match_labels = HashMap::new();
    if let Some(labels) = v.get("matchLabels").and_then(|v| v.as_object()) {
        for (key, value) in labels {
            if let Some(value) = value.as_str() {
                match_labels.insert(key.clone(), value.to_string());
            }
        }
    }

    let match_expressions = v
        .get("matchExpressions")
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(extract_label_selector_requirement)
                .collect()
        })
        .unwrap_or_default();

    Some(LabelSelectorTerm {
        match_labels,
        match_expressions,
    })
}

fn extract_label_selector_requirement(v: &serde_json::Value) -> Option<LabelSelectorRequirement> {
    let key = v.get("key")?.as_str()?.to_string();
    let operator = v
        .get("operator")?
        .as_str()
        .and_then(|operator| match operator {
            "In" => Some(LabelSelectorOperator::In),
            "NotIn" => Some(LabelSelectorOperator::NotIn),
            "Exists" => Some(LabelSelectorOperator::Exists),
            "DoesNotExist" => Some(LabelSelectorOperator::DoesNotExist),
            _ => None,
        })?;
    let values = v
        .get("values")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(ToString::to_string))
                .collect()
        })
        .unwrap_or_default();
    Some(LabelSelectorRequirement {
        key,
        operator,
        values,
    })
}

fn extract_topology_spread_constraint(v: &serde_json::Value) -> Option<TopologySpreadConstraint> {
    let max_skew = v.get("maxSkew")?.as_i64()?;
    let topology_key = v.get("topologyKey")?.as_str()?.to_string();
    let when_unsatisfiable = v
        .get("whenUnsatisfiable")
        .and_then(|value| value.as_str())
        .and_then(|value| match value {
            "DoNotSchedule" => Some(TopologySpreadUnsatisfiableAction::DoNotSchedule),
            "ScheduleAnyway" => Some(TopologySpreadUnsatisfiableAction::ScheduleAnyway),
            _ => None,
        })?;
    let min_domains = v.get("minDomains").and_then(|value| value.as_i64());
    let label_selector = v.get("labelSelector").and_then(extract_label_selector_term);
    let match_label_keys = v
        .get("matchLabelKeys")
        .and_then(|value| value.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(ToString::to_string))
                .collect()
        })
        .unwrap_or_default();
    let node_affinity_policy = extract_node_inclusion_policy(v.get("nodeAffinityPolicy"))
        .unwrap_or(NodeInclusionPolicy::Honor);
    let node_taints_policy = extract_node_inclusion_policy(v.get("nodeTaintsPolicy"))
        .unwrap_or(NodeInclusionPolicy::Ignore);

    Some(TopologySpreadConstraint {
        max_skew,
        min_domains,
        topology_key,
        when_unsatisfiable,
        label_selector,
        match_label_keys,
        node_affinity_policy,
        node_taints_policy,
    })
}

fn extract_node_inclusion_policy(v: Option<&serde_json::Value>) -> Option<NodeInclusionPolicy> {
    v.and_then(|value| value.as_str())
        .and_then(|value| match value {
            "Honor" => Some(NodeInclusionPolicy::Honor),
            "Ignore" => Some(NodeInclusionPolicy::Ignore),
            _ => None,
        })
}

fn extract_node_resources(node_value: &serde_json::Value, pointer: &str) -> NodeResources {
    let resources = node_value.pointer(pointer);
    let cpu_milli = resources
        .and_then(|v| v.get("cpu"))
        .and_then(|v| v.as_str())
        .map(super::types::parse_cpu_quantity)
        .unwrap_or(0);
    let memory_ki = resources
        .and_then(|v| v.get("memory"))
        .and_then(|v| v.as_str())
        .map(|q| {
            // Memory from K8s is in bytes; convert to KiB for our internal representation
            let bytes = super::types::parse_memory_quantity(q);
            bytes / 1024
        })
        .unwrap_or(0);
    let pods = resources
        .and_then(|v| v.get("pods"))
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);

    // Extract scalar/extended resources
    let mut extended = HashMap::new();
    if let Some(obj) = resources.and_then(|v| v.as_object()) {
        for (key, value) in obj {
            if matches!(key.as_str(), "cpu" | "memory" | "pods") {
                continue;
            }
            if let Some(q) = value.as_str() {
                let parsed = super::types::parse_scalar_quantity(key, q);
                if parsed > 0 {
                    extended.insert(key.clone(), parsed);
                }
            }
        }
    }

    NodeResources {
        cpu_milli,
        memory_ki,
        pods,
        extended,
    }
}

/// Extract pod resource requests including overhead.
fn extract_pod_resources(pod_value: &serde_json::Value) -> PodResources {
    let cpu_milli = crate::controllers::resource_quota::calculate_pod_effective_resource_for_key(
        pod_value, "requests", "cpu",
    );
    let memory_bytes = crate::controllers::resource_quota::calculate_pod_effective_resource_for_key(
        pod_value, "requests", "memory",
    );
    let memory_ki = memory_bytes / 1024;

    // Extract overhead
    let overhead_cpu_milli = pod_value
        .pointer("/spec/overhead/cpu")
        .and_then(|v| v.as_str())
        .map(super::types::parse_cpu_quantity)
        .unwrap_or(0);
    let overhead_memory_ki = pod_value
        .pointer("/spec/overhead/memory")
        .and_then(|v| v.as_str())
        .map(|q| {
            let bytes = super::types::parse_memory_quantity(q);
            bytes / 1024
        })
        .unwrap_or(0);

    // Extract extended/scalar resources
    let extended = extract_pod_extended_resources(pod_value);

    PodResources {
        cpu_milli,
        memory_ki,
        extended,
        overhead_cpu_milli,
        overhead_memory_ki,
    }
}

fn extract_pod_extended_resources(pod_value: &serde_json::Value) -> HashMap<String, i64> {
    let mut keys = HashMap::new();
    for container_path in &["/spec/containers", "/spec/initContainers"] {
        if let Some(containers) = pod_value.pointer(container_path).and_then(|v| v.as_array()) {
            for container in containers {
                if let Some(requests) = container
                    .pointer("/resources/requests")
                    .and_then(|v| v.as_object())
                {
                    for key in requests.keys() {
                        if !matches!(key.as_str(), "cpu" | "memory") {
                            keys.insert(key.clone(), ());
                        }
                    }
                }
            }
        }
    }

    keys.into_keys()
        .map(|key| {
            let value =
                crate::controllers::resource_quota::calculate_pod_effective_resource_for_key(
                    pod_value, "requests", &key,
                );
            (key, value)
        })
        .filter(|(_, value)| *value > 0)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn node_json(name: &str) -> serde_json::Value {
        json!({
            "metadata": {
                "name": name,
                "labels": { "zone": "us-west-1" }
            },
            "spec": {
                "taints": [
                    { "key": "dedicated", "value": "gpu", "effect": "NoSchedule" }
                ]
            },
            "status": {
                "conditions": [
                    { "type": "Ready", "status": "True" }
                ],
                "allocatable": {
                    "cpu": "4",
                    "memory": "8Gi",
                    "pods": "110",
                    "nvidia.com/gpu": "2"
                },
                "capacity": {
                    "cpu": "4",
                    "memory": "8Gi",
                    "pods": "110",
                    "nvidia.com/gpu": "2"
                }
            }
        })
    }

    fn pod_json(name: &str) -> serde_json::Value {
        json!({
            "metadata": { "name": name, "namespace": "default" },
            "spec": {
                "containers": [{
                    "name": "main",
                    "resources": {
                        "requests": { "cpu": "500m", "memory": "256Mi" }
                    }
                }],
                "nodeSelector": { "zone": "us-west-1" },
                "tolerations": [
                    { "key": "dedicated", "operator": "Equal", "value": "gpu", "effect": "NoSchedule" }
                ],
                "priority": 100,
                "priorityClassName": "high-priority",
                "preemptionPolicy": "PreemptLowerPriority"
            }
        })
    }

    #[test]
    fn extract_node_basic() {
        let node = extract_schedulable_node(&node_json("node-a"));
        assert_eq!(node.name, "node-a");
        assert!(node.ready);
        assert!(!node.unschedulable);
        assert_eq!(node.labels.get("zone"), Some(&"us-west-1".to_string()));
        assert_eq!(node.taints.len(), 1);
        assert_eq!(node.taints[0].key, "dedicated");
        assert_eq!(node.allocatable.cpu_milli, 4000);
        assert_eq!(node.allocatable.extended.get("nvidia.com/gpu"), Some(&2));
    }

    #[test]
    fn extract_node_unschedulable() {
        let mut node_val = node_json("node-a");
        node_val["spec"]["unschedulable"] = json!(true);
        let node = extract_schedulable_node(&node_val);
        assert!(node.unschedulable);
    }

    #[test]
    fn extract_node_not_ready() {
        let mut node_val = node_json("node-a");
        node_val["status"]["conditions"] = json!([
            { "type": "Ready", "status": "False" }
        ]);
        let node = extract_schedulable_node(&node_val);
        assert!(!node.ready);
    }

    #[test]
    fn extract_pod_basic() {
        let pod = extract_pod_constraints(&pod_json("test-pod"));
        assert_eq!(
            pod.node_selector.get("zone"),
            Some(&"us-west-1".to_string())
        );
        assert_eq!(pod.tolerations.len(), 1);
        assert_eq!(pod.resources.cpu_milli, 500);
        assert_eq!(pod.priority, 100);
        assert_eq!(pod.priority_class_name, Some("high-priority".to_string()));
        assert_eq!(
            pod.preemption_policy,
            Some(PreemptionPolicy::PreemptLowerPriority)
        );
    }

    #[test]
    fn extract_pod_with_overhead() {
        let mut pod_val = pod_json("test-pod");
        pod_val["spec"]["overhead"] = json!({
            "cpu": "100m",
            "memory": "64Mi"
        });
        let resources = extract_existing_pod_resources(&pod_val);
        assert_eq!(resources.overhead_cpu_milli, 100);
        assert!(resources.overhead_memory_ki > 0);
        assert_eq!(resources.effective_cpu_milli(), 600); // 500 + 100
    }

    #[test]
    fn extract_pod_affinity() {
        let mut pod_val = pod_json("test-pod");
        pod_val["spec"]["affinity"] = json!({
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [{
                        "matchExpressions": [{
                            "key": "zone",
                            "operator": "In",
                            "values": ["us-west-1"]
                        }],
                        "matchFields": [{
                            "key": "metadata.name",
                            "operator": "In",
                            "values": ["node-a"]
                        }]
                    }]
                }
            }
        });
        let pod = extract_pod_constraints(&pod_val);
        assert_eq!(pod.required_node_affinity.len(), 1);
        assert_eq!(pod.required_node_affinity[0].match_expressions.len(), 1);
        assert_eq!(pod.required_node_affinity[0].match_fields.len(), 1);
        assert_eq!(
            pod.required_node_affinity[0].match_fields[0].key,
            "metadata.name"
        );
    }

    #[test]
    fn test_extract_existing_pod() {
        let pod = extract_existing_pod(&pod_json("test-pod"));
        assert_eq!(pod.namespace, "default");
        assert_eq!(pod.name, "test-pod");
        assert_eq!(pod.priority, 100);
        assert_eq!(pod.resources.cpu_milli, 500);
    }

    #[test]
    fn extract_empty_node() {
        let node = extract_schedulable_node(&json!({}));
        assert_eq!(node.name, "");
        assert!(!node.ready);
        assert!(node.labels.is_empty());
        assert!(node.taints.is_empty());
    }

    #[test]
    fn extract_empty_pod() {
        let pod = extract_pod_constraints(&json!({}));
        assert!(pod.node_selector.is_empty());
        assert!(pod.tolerations.is_empty());
        assert_eq!(pod.priority, 0);
        assert_eq!(pod.preemption_policy, None);
    }
}
