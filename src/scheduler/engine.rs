//! Scheduler engine — full pipeline entry point (Step 8).
//!
//! Combines filter → score → preempt into a single `schedule_multi_node` call.

use super::adapter;
use super::predicates;
use super::preemption::{self, ExistingPod};
use super::scoring;
use super::types::*;
use std::collections::{HashMap, HashSet};

/// Run the full scheduling pipeline: filter → score → preempt.
///
/// 1. Filter: run predicates on each node.
/// 2. Score: rank passing nodes by resource balance + PreferNoSchedule penalty.
/// 3. Select: highest score, alphabetical tiebreak.
/// 4. Preempt: if no node fits and preemption is allowed, try preemption on
///    each node and pick the lowest-impact victim set.
/// 5. Return `SchedulingDecision`.
pub fn schedule_multi_node(
    nodes: &[SchedulableNode],
    pod: &PodSchedulingConstraints,
    existing_resources: &[NodeExistingPods],
) -> SchedulingDecision {
    schedule_multi_node_with_namespace_labels(nodes, pod, existing_resources, HashMap::new())
}

fn schedule_multi_node_with_namespace_labels(
    nodes: &[SchedulableNode],
    pod: &PodSchedulingConstraints,
    existing_resources: &[NodeExistingPods],
    namespace_labels_by_name: HashMap<String, HashMap<String, String>>,
) -> SchedulingDecision {
    // Build a map from node name to existing pods
    let existing_map: std::collections::HashMap<&str, &[ExistingPod]> = existing_resources
        .iter()
        .map(|nep| (nep.node_name.as_str(), nep.pods.as_slice()))
        .collect();
    let inter_pod_context =
        build_inter_pod_affinity_context(nodes, existing_resources, namespace_labels_by_name);

    // Step 1: Filter — run predicates on each node
    let mut fit_nodes = Vec::new();
    let mut all_reasons = Vec::new();
    let mut failed_nodes = Vec::new();

    for node in nodes {
        let existing = existing_map.get(node.name.as_str()).copied().unwrap_or(&[]);
        let existing_resources: Vec<PodResources> =
            existing.iter().map(|p| p.resources.clone()).collect();
        let reasons =
            predicates::node_fit_with_context(node, pod, &existing_resources, &inter_pod_context);
        if reasons.is_empty() {
            fit_nodes.push(node.clone());
        } else {
            for r in &reasons {
                if !all_reasons.contains(r) {
                    all_reasons.push(r.clone());
                }
            }
            failed_nodes.push(NodeFilterFailure {
                node: node.clone(),
                reasons,
            });
        }
    }

    if fit_nodes.is_empty() {
        // Step 4: Try preemption
        return try_preemption(&failed_nodes, pod, &existing_map, &all_reasons);
    }

    // Step 2-3: Score and select
    let scoring_existing: Vec<(&str, Vec<PodResources>)> = existing_resources
        .iter()
        .map(|nep| {
            let resources: Vec<PodResources> =
                nep.pods.iter().map(|p| p.resources.clone()).collect();
            (nep.node_name.as_str(), resources)
        })
        .collect();

    let selected = select_best_node_with_topology_spread(
        &fit_nodes,
        pod,
        &scoring_existing,
        &inter_pod_context,
    );
    match selected {
        Some(name) => SchedulingDecision::success(name),
        None => SchedulingDecision::failed(vec!["no node selected after scoring".into()]),
    }
}

fn build_inter_pod_affinity_context(
    nodes: &[SchedulableNode],
    existing_resources: &[NodeExistingPods],
    namespace_labels_by_name: HashMap<String, HashMap<String, String>>,
) -> InterPodAffinityContext {
    let nodes_by_name = nodes
        .iter()
        .map(|node| (node.name.clone(), node.clone()))
        .collect::<HashMap<_, _>>();
    let node_labels_by_name = nodes
        .iter()
        .map(|node| (node.name.clone(), node.labels.clone()))
        .collect::<HashMap<_, _>>();
    let existing_pods = existing_resources
        .iter()
        .flat_map(|node_existing| {
            node_existing.pods.iter().map(|pod| ScheduledPod {
                namespace: pod.namespace.clone(),
                name: pod.name.clone(),
                node_name: node_existing.node_name.clone(),
                labels: pod.labels.clone(),
            })
        })
        .collect();

    InterPodAffinityContext {
        existing_pods,
        nodes_by_name,
        node_labels_by_name,
        namespace_labels_by_name,
    }
}

fn select_best_node_with_topology_spread(
    nodes: &[SchedulableNode],
    pod: &PodSchedulingConstraints,
    existing_resources: &[(&str, Vec<PodResources>)],
    inter_pod_context: &InterPodAffinityContext,
) -> Option<String> {
    if pod.topology_spread_constraints.iter().all(|constraint| {
        constraint.when_unsatisfiable != TopologySpreadUnsatisfiableAction::ScheduleAnyway
    }) {
        return scoring::select_best_node(nodes, pod, existing_resources);
    }

    let resource_map: HashMap<&str, &[PodResources]> = existing_resources
        .iter()
        .map(|(name, resources)| (*name, resources.as_slice()))
        .collect();
    let mut best_nodes = Vec::new();
    let mut best_score = i64::MIN;

    for node in nodes {
        let existing = resource_map.get(node.name.as_str()).copied().unwrap_or(&[]);
        let score = scoring::score_node(node, pod, existing)
            + predicates::topology_spread_schedule_anyway_score(node, pod, inter_pod_context);
        match score.cmp(&best_score) {
            std::cmp::Ordering::Greater => {
                best_score = score;
                best_nodes.clear();
                best_nodes.push(node);
            }
            std::cmp::Ordering::Equal => best_nodes.push(node),
            std::cmp::Ordering::Less => {}
        }
    }

    best_nodes
        .into_iter()
        .min_by(|a, b| a.name.cmp(&b.name))
        .map(|node| node.name.clone())
}

/// Convenience struct for passing existing pods per node.
#[derive(Clone, Debug)]
pub struct NodeExistingPods {
    pub node_name: String,
    pub pods: Vec<ExistingPod>,
}

#[derive(Clone, Debug)]
struct NodeFilterFailure {
    node: SchedulableNode,
    reasons: Vec<String>,
}

/// Try preemption on all nodes, return the lowest-impact victim set.
fn try_preemption(
    failed_nodes: &[NodeFilterFailure],
    pod: &PodSchedulingConstraints,
    existing_map: &std::collections::HashMap<&str, &[ExistingPod]>,
    all_reasons: &[String],
) -> SchedulingDecision {
    let mut best: Option<PreemptionCandidate> = None;

    for failed in failed_nodes {
        if !failed
            .reasons
            .iter()
            .all(|reason| preemption_recoverable_reason(reason))
        {
            continue;
        }

        let node = &failed.node;
        let existing = existing_map.get(node.name.as_str()).copied().unwrap_or(&[]);
        if let Some(victims) = preemption::select_preemption_victims(node, pod, existing) {
            let remaining_resources = resources_after_preemption(existing, &victims);
            if !predicates::node_fit(node, pod, &remaining_resources).is_empty() {
                continue;
            }
            let candidate = PreemptionCandidate::new(node.name.clone(), victims, existing);
            if best
                .as_ref()
                .is_none_or(|current| candidate.is_better_than(current))
            {
                best = Some(candidate);
            }
        }
    }

    match best {
        Some(candidate) => {
            SchedulingDecision::preempt_with_victims(candidate.node_name, candidate.victims)
        }
        None => {
            let message = format!(
                "0/{} nodes are available: {}.",
                failed_nodes.len(),
                all_reasons.join(", ")
            );
            SchedulingDecision::failed_with_message(all_reasons.to_vec(), message)
        }
    }
}

fn preemption_recoverable_reason(reason: &str) -> bool {
    reason == "Too many pods" || reason.starts_with("Insufficient ")
}

fn resources_after_preemption(
    existing: &[ExistingPod],
    victims: &[PreemptionVictim],
) -> Vec<PodResources> {
    let victim_keys: HashSet<(&str, &str)> = victims
        .iter()
        .map(|victim| (victim.namespace.as_str(), victim.name.as_str()))
        .collect();
    existing
        .iter()
        .filter(|pod| !victim_keys.contains(&(pod.namespace.as_str(), pod.name.as_str())))
        .map(|pod| pod.resources.clone())
        .collect()
}

#[derive(Clone, Debug)]
struct PreemptionCandidate {
    node_name: String,
    victims: Vec<PreemptionVictim>,
    highest_victim_priority: i64,
    priority_sum: i64,
    victim_count: usize,
}

impl PreemptionCandidate {
    fn new(node_name: String, victims: Vec<PreemptionVictim>, existing: &[ExistingPod]) -> Self {
        let mut highest_victim_priority = i64::MIN;
        let mut priority_sum = 0_i64;

        for victim in &victims {
            let priority = existing
                .iter()
                .find(|pod| pod.namespace == victim.namespace && pod.name == victim.name)
                .map(|pod| pod.priority)
                .unwrap_or(i64::MAX);
            highest_victim_priority = highest_victim_priority.max(priority);
            priority_sum = priority_sum.saturating_add(priority);
        }

        let victim_count = victims.len();
        Self {
            node_name,
            victims,
            highest_victim_priority,
            priority_sum,
            victim_count,
        }
    }

    fn is_better_than(&self, other: &Self) -> bool {
        self.highest_victim_priority
            .cmp(&other.highest_victim_priority)
            .then_with(|| self.priority_sum.cmp(&other.priority_sum))
            .then_with(|| self.victim_count.cmp(&other.victim_count))
            .then_with(|| self.node_name.cmp(&other.node_name))
            .is_lt()
    }
}

/// High-level scheduling entry point that accepts raw JSON Values
/// and returns a `SchedulingDecision`. This is the primary call from
/// `PodApiService`.
pub fn schedule_from_json(
    nodes: &[&serde_json::Value],
    pod: &serde_json::Value,
    existing_pods_per_node: &[(&str, &[&serde_json::Value])],
) -> SchedulingDecision {
    schedule_from_json_with_namespaces(nodes, pod, existing_pods_per_node, &[])
}

/// High-level scheduling entry point with Namespace labels for namespaceSelector.
pub fn schedule_from_json_with_namespaces(
    nodes: &[&serde_json::Value],
    pod: &serde_json::Value,
    existing_pods_per_node: &[(&str, &[&serde_json::Value])],
    namespaces: &[&serde_json::Value],
) -> SchedulingDecision {
    let typed_nodes: Vec<SchedulableNode> = nodes
        .iter()
        .map(|n| adapter::extract_schedulable_node(n))
        .collect();
    let pod_constraints = adapter::extract_pod_constraints(pod);
    let namespace_labels_by_name = namespaces
        .iter()
        .filter_map(|namespace| {
            let name = namespace
                .pointer("/metadata/name")
                .and_then(|value| value.as_str())?;
            Some((name.to_string(), extract_metadata_labels(namespace)))
        })
        .collect::<HashMap<_, _>>();
    let existing: Vec<NodeExistingPods> = existing_pods_per_node
        .iter()
        .map(|(node_name, pods)| NodeExistingPods {
            node_name: node_name.to_string(),
            pods: pods
                .iter()
                .map(|p| adapter::extract_existing_pod(p))
                .collect(),
        })
        .collect();

    schedule_multi_node_with_namespace_labels(
        &typed_nodes,
        &pod_constraints,
        &existing,
        namespace_labels_by_name,
    )
}

fn extract_metadata_labels(value: &serde_json::Value) -> HashMap<String, String> {
    value
        .pointer("/metadata/labels")
        .and_then(|labels| labels.as_object())
        .map(|labels| {
            labels
                .iter()
                .filter_map(|(key, value)| value.as_str().map(|value| (key.clone(), value.into())))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    fn make_node(name: &str, cpu: i64, mem: i64) -> SchedulableNode {
        SchedulableNode {
            name: name.into(),
            ready: true,
            unschedulable: false,
            taints: Vec::new(),
            labels: HashMap::new(),
            allocatable: NodeResources {
                cpu_milli: cpu,
                memory_ki: mem,
                pods: 110,
                extended: HashMap::new(),
            },
            capacity: NodeResources {
                cpu_milli: cpu,
                memory_ki: mem,
                pods: 110,
                extended: HashMap::new(),
            },
        }
    }

    fn make_node_with_extended(name: &str, resource_name: &str, amount: i64) -> SchedulableNode {
        let mut node = make_node(name, 8000, 32 * 1024 * 1024);
        node.allocatable
            .extended
            .insert(resource_name.to_string(), amount);
        node.capacity
            .extended
            .insert(resource_name.to_string(), amount);
        node
    }

    fn make_constraints(cpu: i64, mem: i64, priority: i64) -> PodSchedulingConstraints {
        PodSchedulingConstraints {
            resources: PodResources {
                cpu_milli: cpu,
                memory_ki: mem,
                ..Default::default()
            },
            priority,
            ..Default::default()
        }
    }

    fn make_extended_constraints(
        resource_name: &str,
        amount: i64,
        priority: i64,
    ) -> PodSchedulingConstraints {
        let mut pod = make_constraints(0, 0, priority);
        pod.resources
            .extended
            .insert(resource_name.to_string(), amount);
        pod
    }

    fn make_existing(name: &str, priority: i64, cpu: i64, mem: i64) -> ExistingPod {
        ExistingPod {
            namespace: "default".into(),
            name: name.into(),
            priority,
            resources: PodResources {
                cpu_milli: cpu,
                memory_ki: mem,
                ..Default::default()
            },
            labels: HashMap::new(),
        }
    }

    fn make_extended_existing(
        name: &str,
        priority: i64,
        resource_name: &str,
        amount: i64,
    ) -> ExistingPod {
        let mut pod = make_existing(name, priority, 0, 0);
        pod.resources
            .extended
            .insert(resource_name.to_string(), amount);
        pod
    }

    #[test]
    fn one_fit_node_selected() {
        let node_a = make_node("node-a", 4000, 8192000);
        let pod = make_constraints(500, 512000, 0);
        let decision = schedule_multi_node(&[node_a], &pod, &[]);
        assert!(decision.is_success());
        assert_eq!(decision.selected_node, Some("node-a".into()));
    }

    #[test]
    fn schedule_from_json_rejects_ready_node_without_allocatable_pods() {
        let fake_node = json!({
            "metadata": {"name": "e2e-fake-node-xq5nq"},
            "status": {
                "conditions": [{
                    "type": "Ready",
                    "status": "True",
                    "reason": "E2E",
                    "message": "Set from e2e test"
                }],
                "allocatable": {},
                "capacity": {}
            }
        });
        let pod = json!({
            "metadata": {"name": "zero-request-pod", "namespace": "default"},
            "spec": {"containers": [{"name": "main", "image": "registry.k8s.io/pause:3.10"}]}
        });

        let decision = schedule_from_json(&[&fake_node], &pod, &[]);

        assert!(!decision.is_success());
        assert!(
            decision
                .failed_reasons
                .iter()
                .any(|reason| reason.contains("Too many pods")),
            "node without allocatable.pods must not accept even a zero-request pod: {decision:?}"
        );
    }

    #[test]
    fn schedule_from_json_enforces_required_pod_affinity_topology_key() {
        let node_a = json!({
            "metadata": {
                "name": "node-a",
                "labels": {"topology.kubernetes.io/zone": "zone-a"}
            },
            "status": {
                "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110"},
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        });
        let node_b = json!({
            "metadata": {
                "name": "node-b",
                "labels": {"topology.kubernetes.io/zone": "zone-b"}
            },
            "status": {
                "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110"},
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        });
        let existing_peer = json!({
            "metadata": {
                "namespace": "default",
                "name": "peer",
                "labels": {"app": "database"}
            },
            "spec": {
                "nodeName": "node-b",
                "containers": [{"name": "main", "image": "pause"}]
            }
        });
        let pod = json!({
            "metadata": {"namespace": "default", "name": "client"},
            "spec": {
                "containers": [{"name": "main", "image": "pause"}],
                "affinity": {
                    "podAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": [{
                            "labelSelector": {"matchLabels": {"app": "database"}},
                            "topologyKey": "topology.kubernetes.io/zone"
                        }]
                    }
                }
            }
        });

        let decision = schedule_from_json(
            &[&node_a, &node_b],
            &pod,
            &[("node-b", &[&existing_peer][..])],
        );

        assert!(decision.is_success(), "{decision:?}");
        assert_eq!(decision.selected_node, Some("node-b".into()));
    }

    #[test]
    fn schedule_from_json_enforces_required_pod_anti_affinity_topology_key() {
        let node_a = json!({
            "metadata": {
                "name": "node-a",
                "labels": {"kubernetes.io/hostname": "node-a"}
            },
            "status": {
                "allocatable": {"cpu": "64", "memory": "256Gi", "pods": "110"},
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        });
        let node_b = json!({
            "metadata": {
                "name": "node-b",
                "labels": {"kubernetes.io/hostname": "node-b"}
            },
            "spec": {
                "taints": [{
                    "key": "dedicated",
                    "value": "fallback",
                    "effect": "PreferNoSchedule"
                }]
            },
            "status": {
                "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110"},
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        });
        let existing_peer = json!({
            "metadata": {
                "namespace": "default",
                "name": "peer",
                "labels": {"app": "database"}
            },
            "spec": {
                "nodeName": "node-a",
                "containers": [{"name": "main", "image": "pause"}]
            }
        });
        let pod = json!({
            "metadata": {"namespace": "default", "name": "client"},
            "spec": {
                "containers": [{"name": "main", "image": "pause"}],
                "affinity": {
                    "podAntiAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": [{
                            "labelSelector": {"matchLabels": {"app": "database"}},
                            "topologyKey": "kubernetes.io/hostname"
                        }]
                    }
                }
            }
        });

        let decision = schedule_from_json(
            &[&node_a, &node_b],
            &pod,
            &[("node-a", &[&existing_peer][..])],
        );

        assert!(decision.is_success(), "{decision:?}");
        assert_eq!(decision.selected_node, Some("node-b".into()));
    }

    #[test]
    fn schedule_from_json_matches_pod_affinity_namespace_selector() {
        let node_a = json!({
            "metadata": {
                "name": "node-a",
                "labels": {"topology.kubernetes.io/zone": "zone-a"}
            },
            "status": {
                "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110"},
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        });
        let node_b = json!({
            "metadata": {
                "name": "node-b",
                "labels": {"topology.kubernetes.io/zone": "zone-b"}
            },
            "status": {
                "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110"},
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        });
        let peer_namespace = json!({
            "metadata": {
                "name": "peer-ns",
                "labels": {"team": "storage"}
            }
        });
        let existing_peer = json!({
            "metadata": {
                "namespace": "peer-ns",
                "name": "peer",
                "labels": {"app": "database"}
            },
            "spec": {
                "nodeName": "node-b",
                "containers": [{"name": "main", "image": "pause"}]
            }
        });
        let pod = json!({
            "metadata": {"namespace": "default", "name": "client"},
            "spec": {
                "containers": [{"name": "main", "image": "pause"}],
                "affinity": {
                    "podAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": [{
                            "labelSelector": {"matchLabels": {"app": "database"}},
                            "namespaceSelector": {"matchLabels": {"team": "storage"}},
                            "topologyKey": "topology.kubernetes.io/zone"
                        }]
                    }
                }
            }
        });

        let decision = schedule_from_json_with_namespaces(
            &[&node_a, &node_b],
            &pod,
            &[("node-b", &[&existing_peer][..])],
            &[&peer_namespace],
        );

        assert!(decision.is_success(), "{decision:?}");
        assert_eq!(decision.selected_node, Some("node-b".into()));
    }

    #[test]
    fn schedule_from_json_enforces_topology_spread_do_not_schedule() {
        let node_a = json!({
            "metadata": {
                "name": "node-a",
                "labels": {"topology.kubernetes.io/zone": "zone-a"}
            },
            "status": {
                "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110"},
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        });
        let node_b = json!({
            "metadata": {
                "name": "node-b",
                "labels": {"topology.kubernetes.io/zone": "zone-b"}
            },
            "spec": {
                "taints": [{
                    "key": "dedicated",
                    "value": "fallback",
                    "effect": "PreferNoSchedule"
                }]
            },
            "status": {
                "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110"},
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        });
        let existing_one = json!({
            "metadata": {
                "namespace": "default",
                "name": "peer-1",
                "labels": {"app": "web"}
            },
            "spec": {"nodeName": "node-a", "containers": [{"name": "main", "image": "pause"}]}
        });
        let existing_two = json!({
            "metadata": {
                "namespace": "default",
                "name": "peer-2",
                "labels": {"app": "web"}
            },
            "spec": {"nodeName": "node-a", "containers": [{"name": "main", "image": "pause"}]}
        });
        let pod = json!({
            "metadata": {
                "namespace": "default",
                "name": "client",
                "labels": {"app": "web"}
            },
            "spec": {
                "containers": [{"name": "main", "image": "pause"}],
                "topologySpreadConstraints": [{
                    "maxSkew": 1,
                    "topologyKey": "topology.kubernetes.io/zone",
                    "whenUnsatisfiable": "DoNotSchedule",
                    "labelSelector": {"matchLabels": {"app": "web"}}
                }]
            }
        });

        let decision = schedule_from_json(
            &[&node_a, &node_b],
            &pod,
            &[("node-a", &[&existing_one, &existing_two][..])],
        );

        assert!(decision.is_success(), "{decision:?}");
        assert_eq!(decision.selected_node, Some("node-b".into()));
    }

    #[test]
    fn schedule_from_json_prefers_topology_spread_schedule_anyway_domain() {
        let node_a = json!({
            "metadata": {
                "name": "node-a",
                "labels": {"topology.kubernetes.io/zone": "zone-a"}
            },
            "status": {
                "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110"},
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        });
        let node_b = json!({
            "metadata": {
                "name": "node-b",
                "labels": {"topology.kubernetes.io/zone": "zone-b"}
            },
            "spec": {
                "taints": [{
                    "key": "dedicated",
                    "value": "fallback",
                    "effect": "PreferNoSchedule"
                }]
            },
            "status": {
                "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110"},
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        });
        let existing_one = json!({
            "metadata": {
                "namespace": "default",
                "name": "peer-1",
                "labels": {"app": "web"}
            },
            "spec": {"nodeName": "node-a", "containers": [{"name": "main", "image": "pause"}]}
        });
        let existing_two = json!({
            "metadata": {
                "namespace": "default",
                "name": "peer-2",
                "labels": {"app": "web"}
            },
            "spec": {"nodeName": "node-a", "containers": [{"name": "main", "image": "pause"}]}
        });
        let pod = json!({
            "metadata": {
                "namespace": "default",
                "name": "client",
                "labels": {"app": "web"}
            },
            "spec": {
                "containers": [{"name": "main", "image": "pause"}],
                "topologySpreadConstraints": [{
                    "maxSkew": 1,
                    "topologyKey": "topology.kubernetes.io/zone",
                    "whenUnsatisfiable": "ScheduleAnyway",
                    "labelSelector": {"matchLabels": {"app": "web"}}
                }]
            }
        });

        let decision = schedule_from_json(
            &[&node_a, &node_b],
            &pod,
            &[("node-a", &[&existing_one, &existing_two][..])],
        );

        assert!(decision.is_success(), "{decision:?}");
        assert_eq!(decision.selected_node, Some("node-b".into()));
    }

    #[test]
    fn schedule_from_json_topology_spread_min_domains_uses_zero_global_minimum() {
        let node_a = json!({
            "metadata": {
                "name": "node-a",
                "labels": {"topology.kubernetes.io/zone": "zone-a"}
            },
            "status": {
                "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110"},
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        });
        let node_b = json!({
            "metadata": {
                "name": "node-b",
                "labels": {"topology.kubernetes.io/zone": "zone-b"}
            },
            "status": {
                "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110"},
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        });
        let existing_a = json!({
            "metadata": {
                "namespace": "default",
                "name": "peer-a",
                "labels": {"app": "web"}
            },
            "spec": {"nodeName": "node-a", "containers": [{"name": "main", "image": "pause"}]}
        });
        let existing_b = json!({
            "metadata": {
                "namespace": "default",
                "name": "peer-b",
                "labels": {"app": "web"}
            },
            "spec": {"nodeName": "node-b", "containers": [{"name": "main", "image": "pause"}]}
        });
        let pod = json!({
            "metadata": {
                "namespace": "default",
                "name": "client",
                "labels": {"app": "web"}
            },
            "spec": {
                "containers": [{"name": "main", "image": "pause"}],
                "topologySpreadConstraints": [{
                    "maxSkew": 1,
                    "minDomains": 3,
                    "topologyKey": "topology.kubernetes.io/zone",
                    "whenUnsatisfiable": "DoNotSchedule",
                    "labelSelector": {"matchLabels": {"app": "web"}}
                }]
            }
        });

        let decision = schedule_from_json(
            &[&node_a, &node_b],
            &pod,
            &[
                ("node-a", &[&existing_a][..]),
                ("node-b", &[&existing_b][..]),
            ],
        );

        assert!(!decision.is_success(), "{decision:?}");
        assert!(
            decision
                .failed_reasons
                .iter()
                .any(|reason| reason.contains("topology spread")),
            "{decision:?}"
        );
    }

    #[test]
    fn schedule_from_json_topology_spread_match_label_keys_selects_peer_pods() {
        let node_a = json!({
            "metadata": {
                "name": "node-a",
                "labels": {"topology.kubernetes.io/zone": "zone-a"}
            },
            "status": {
                "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110"},
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        });
        let node_b = json!({
            "metadata": {
                "name": "node-b",
                "labels": {"topology.kubernetes.io/zone": "zone-b"}
            },
            "spec": {
                "taints": [{
                    "key": "dedicated",
                    "value": "fallback",
                    "effect": "PreferNoSchedule"
                }]
            },
            "status": {
                "allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110"},
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        });
        let matching_peer = json!({
            "metadata": {
                "namespace": "default",
                "name": "peer-1",
                "labels": {"pod-template-hash": "abc123"}
            },
            "spec": {"nodeName": "node-a", "containers": [{"name": "main", "image": "pause"}]}
        });
        let other_peer = json!({
            "metadata": {
                "namespace": "default",
                "name": "peer-2",
                "labels": {"pod-template-hash": "other"}
            },
            "spec": {"nodeName": "node-b", "containers": [{"name": "main", "image": "pause"}]}
        });
        let pod = json!({
            "metadata": {
                "namespace": "default",
                "name": "client",
                "labels": {"pod-template-hash": "abc123"}
            },
            "spec": {
                "containers": [{"name": "main", "image": "pause"}],
                "topologySpreadConstraints": [{
                    "maxSkew": 1,
                    "topologyKey": "topology.kubernetes.io/zone",
                    "whenUnsatisfiable": "DoNotSchedule",
                    "matchLabelKeys": ["pod-template-hash"]
                }]
            }
        });

        let decision = schedule_from_json(
            &[&node_a, &node_b],
            &pod,
            &[
                ("node-a", &[&matching_peer][..]),
                ("node-b", &[&other_peer][..]),
            ],
        );

        assert!(decision.is_success(), "{decision:?}");
        assert_eq!(decision.selected_node, Some("node-b".into()));
    }

    #[test]
    fn all_nodes_fail_predicates() {
        let node_a = make_node("node-a", 1000, 1000);
        let pod = make_constraints(5000, 5000, 0);
        let decision = schedule_multi_node(&[node_a], &pod, &[]);
        assert!(!decision.is_success());
        assert!(
            decision
                .failed_reasons
                .iter()
                .any(|r| r.contains("Insufficient cpu"))
        );
        assert!(decision.unschedulable_message.is_some());
    }

    #[test]
    fn no_fit_but_preemption_succeeds() {
        let node = make_node("node-a", 1000, 10000);
        let pod = make_constraints(500, 0, 100);
        let existing = vec![NodeExistingPods {
            node_name: "node-a".into(),
            pods: vec![make_existing("low-pod", 50, 800, 0)],
        }];
        let decision = schedule_multi_node(&[node], &pod, &existing);
        assert!(decision.is_success());
        assert!(decision.selected_node.is_some());
        assert!(!decision.preemption_victims.is_empty());
    }

    #[test]
    fn preempt_fails_too() {
        let node = make_node("node-a", 1000, 10000);
        let pod = make_constraints(5000, 0, 100);
        let existing = vec![NodeExistingPods {
            node_name: "node-a".into(),
            pods: vec![make_existing("low-pod", 50, 800, 0)],
        }];
        let decision = schedule_multi_node(&[node], &pod, &existing);
        assert!(!decision.is_success());
        assert!(decision.unschedulable_message.is_some());
    }

    #[test]
    fn two_nodes_prefer_no_schedule_penalized() {
        let mut tainted = make_node("node-tainted", 4000, 8192000);
        tainted.taints.push(Taint {
            key: "dedicated".into(),
            value: Some("gpu".into()),
            effect: TaintEffect::PreferNoSchedule,
        });
        let clean = make_node("node-clean", 4000, 8192000);
        let pod = make_constraints(500, 512000, 0);
        let decision = schedule_multi_node(&[tainted, clean], &pod, &[]);
        assert!(decision.is_success());
        assert_eq!(decision.selected_node, Some("node-clean".into()));
    }

    #[test]
    fn spread_scoring_prefers_less_loaded() {
        let node_a = make_node("node-a", 4000, 8192000);
        let node_b = make_node("node-b", 4000, 8192000);
        let pod = make_constraints(500, 512000, 0);
        let existing = vec![NodeExistingPods {
            node_name: "node-a".into(),
            pods: vec![make_existing("existing", 0, 3000, 0)],
        }];
        let decision = schedule_multi_node(&[node_a, node_b], &pod, &existing);
        assert!(decision.is_success());
        assert_eq!(decision.selected_node, Some("node-b".into()));
    }

    #[test]
    fn preemption_picks_fewest_victims() {
        let node_a = make_node("node-a", 1000, 10000);
        let node_b = make_node("node-b", 1000, 10000);
        let pod = make_constraints(500, 0, 100);
        let existing = vec![
            NodeExistingPods {
                node_name: "node-a".into(),
                pods: vec![
                    make_existing("low-1", 50, 300, 0),
                    make_existing("low-2", 50, 300, 0),
                ],
            },
            NodeExistingPods {
                node_name: "node-b".into(),
                pods: vec![make_existing("low-3", 50, 800, 0)],
            },
        ];
        let decision = schedule_multi_node(&[node_a, node_b], &pod, &existing);
        assert!(decision.is_success());
        // node-a: 600m used, need 500m more = 1100 > 1000. Must remove both (2 victims)
        //   After removing one 300m: 300+500=800≤1000 → only 1 victim needed
        //   Wait, that means both nodes need 1 victim. Tiebreak by name → node-a.
        // Let's make node-a need 2 victims:
        //   node-a: 3 pods × 300m = 900m used. 900+500=1400>1000.
        //   Remove one 300m: 600+500=1100>1000. Still over. Need 2.
        //   Remove two 300m: 300+500=800≤1000. Need 2 victims.
        // vs node-b: 1 pod × 800m. 800+500=1300>1000. Remove 1 → 500≤1000. Need 1.
        // So node-b with 1 victim wins.
        let existing = vec![
            NodeExistingPods {
                node_name: "node-a".into(),
                pods: vec![
                    make_existing("low-1", 50, 300, 0),
                    make_existing("low-2", 50, 300, 0),
                    make_existing("low-3", 50, 300, 0),
                ],
            },
            NodeExistingPods {
                node_name: "node-b".into(),
                pods: vec![make_existing("low-4", 50, 800, 0)],
            },
        ];
        let decision = schedule_multi_node(
            &[
                make_node("node-a", 1000, 10000),
                make_node("node-b", 1000, 10000),
            ],
            &pod,
            &existing,
        );
        assert!(decision.is_success());
        assert_eq!(decision.selected_node, Some("node-b".into()));
        assert_eq!(decision.preemption_victims.len(), 1);
    }

    #[test]
    fn preemption_prefers_lowest_priority_victim_across_nodes() {
        let resource_name = "scheduling.k8s.io/foo";
        let leader = make_node_with_extended("mn-leader", resource_name, 5);
        let worker = make_node_with_extended("mn-worker", resource_name, 5);
        let pod = make_extended_constraints(resource_name, 2, 2_000_000_000);
        let existing = vec![
            NodeExistingPods {
                node_name: "mn-leader".into(),
                pods: vec![
                    make_extended_existing("leader-medium-0", 100, resource_name, 2),
                    make_extended_existing("leader-medium-1", 100, resource_name, 2),
                ],
            },
            NodeExistingPods {
                node_name: "mn-worker".into(),
                pods: vec![
                    make_extended_existing("worker-low", 1, resource_name, 2),
                    make_extended_existing("worker-medium", 100, resource_name, 2),
                ],
            },
        ];

        let decision = schedule_multi_node(&[leader, worker], &pod, &existing);

        assert!(decision.is_success());
        assert_eq!(decision.selected_node, Some("mn-worker".into()));
        assert_eq!(decision.preemption_victims, vec!["default/worker-low"]);
    }

    #[test]
    fn preemption_does_not_select_node_with_non_resource_predicate_failure() {
        struct Case {
            name: &'static str,
            configure_node: fn(&mut SchedulableNode),
            configure_pod: fn(&mut PodSchedulingConstraints),
            expected_reason: &'static str,
        }

        fn mark_unschedulable(node: &mut SchedulableNode) {
            node.unschedulable = true;
        }

        fn mark_not_ready(node: &mut SchedulableNode) {
            node.ready = false;
        }

        fn require_missing_label(pod: &mut PodSchedulingConstraints) {
            pod.node_selector.insert("disk".into(), "ssd".into());
        }

        fn add_untolerated_taint(node: &mut SchedulableNode) {
            node.taints.push(Taint {
                key: "dedicated".into(),
                value: Some("system".into()),
                effect: TaintEffect::NoSchedule,
            });
        }

        let cases = [
            Case {
                name: "unschedulable node",
                configure_node: mark_unschedulable,
                configure_pod: |_| {},
                expected_reason: "node(s) were unschedulable",
            },
            Case {
                name: "not ready node",
                configure_node: mark_not_ready,
                configure_pod: |_| {},
                expected_reason: "node(s) were not ready",
            },
            Case {
                name: "node selector mismatch",
                configure_node: |_| {},
                configure_pod: require_missing_label,
                expected_reason: "node(s) didn't match Pod's node affinity/selector",
            },
            Case {
                name: "untolerated NoSchedule taint",
                configure_node: add_untolerated_taint,
                configure_pod: |_| {},
                expected_reason: "taint dedicated/system not tolerated",
            },
        ];

        for case in cases {
            let mut node = make_node("node-a", 1000, 10000);
            (case.configure_node)(&mut node);
            let mut pod = make_constraints(500, 0, 100);
            (case.configure_pod)(&mut pod);
            let existing = vec![NodeExistingPods {
                node_name: "node-a".into(),
                pods: vec![make_existing("low-pod", 50, 800, 0)],
            }];

            let decision = schedule_multi_node(&[node], &pod, &existing);

            assert!(
                !decision.is_success(),
                "{} should remain unschedulable after preemption",
                case.name
            );
            assert!(
                decision
                    .failed_reasons
                    .iter()
                    .any(|reason| reason == case.expected_reason),
                "{} should report hard predicate failure, got {:?}",
                case.name,
                decision.failed_reasons
            );
            assert!(
                decision.preemption_victims.is_empty(),
                "{} should not preempt victims for an ineligible node",
                case.name
            );
        }
    }

    #[test]
    fn preemption_skips_ineligible_node_and_uses_recoverable_candidate() {
        let mut ineligible = make_node("node-a", 1000, 10000);
        ineligible.taints.push(Taint {
            key: "dedicated".into(),
            value: Some("system".into()),
            effect: TaintEffect::NoSchedule,
        });
        let eligible = make_node("node-b", 1000, 10000);
        let pod = make_constraints(500, 0, 100);
        let existing = vec![
            NodeExistingPods {
                node_name: "node-a".into(),
                pods: vec![make_existing("ineligible-low", 50, 800, 0)],
            },
            NodeExistingPods {
                node_name: "node-b".into(),
                pods: vec![make_existing("eligible-low", 50, 800, 0)],
            },
        ];

        let decision = schedule_multi_node(&[ineligible, eligible], &pod, &existing);

        assert!(decision.is_success());
        assert_eq!(decision.selected_node, Some("node-b".into()));
        assert_eq!(decision.preemption_victims, vec!["default/eligible-low"]);
    }
}
