//! Scheduler scoring (2A-8).
//!
//! Scores nodes that passed predicates and picks the best fit.
//! Higher score = better fit. Ties are broken by node name for determinism.

use super::types::*;

/// Penalty per un-tolerated PreferNoSchedule taint.
const PREFER_NO_SCHEDULE_PENALTY: i64 = -100;

/// Calculate the PreferNoSchedule soft penalty for a node.
/// Returns a negative score contribution for each un-tolerated PreferNoSchedule taint.
pub fn prefer_no_schedule_penalty(node: &SchedulableNode, pod: &PodSchedulingConstraints) -> i64 {
    let mut penalty = 0i64;
    for taint in &node.taints {
        if taint.effect != TaintEffect::PreferNoSchedule {
            continue;
        }
        let tolerated = pod.tolerations.iter().any(|tol| match tol.operator {
            TolerationOperator::Exists => {
                if tol.key.is_none() {
                    return true;
                }
                if tol.key.as_deref() == Some(&taint.key)
                    && (tol.effect.is_none()
                        || tol.effect.as_ref() == Some(&TaintEffect::PreferNoSchedule))
                {
                    return true;
                }
                false
            }
            TolerationOperator::Equal => {
                if tol.key.as_deref() == Some(&taint.key) {
                    let values_match = match (&tol.value, &taint.value) {
                        (Some(tv), Some(av)) => tv == av,
                        (None, None) => true,
                        _ => false,
                    };
                    if values_match
                        && (tol.effect.is_none()
                            || tol.effect.as_ref() == Some(&TaintEffect::PreferNoSchedule))
                    {
                        return true;
                    }
                }
                false
            }
        });
        if !tolerated {
            penalty += PREFER_NO_SCHEDULE_PENALTY;
        }
    }
    penalty
}

/// Score a node for a pod. Returns a numeric score (higher is better).
///
/// Scoring factors:
/// - Resource balance: prefer nodes with more remaining resources after the pod is placed.
/// - Pod occupancy: prefer nodes with fewer existing pods when resource requests tie.
/// - Equal-weight: CPU, memory, and pod slots are equally weighted.
pub fn score_node(
    node: &SchedulableNode,
    pod: &PodSchedulingConstraints,
    existing_pod_resources: &[PodResources],
) -> i64 {
    let mut remaining_cpu = node.allocatable.cpu_milli - pod.resources.effective_cpu_milli();
    let mut remaining_mem = node.allocatable.memory_ki - pod.resources.effective_memory_ki();
    let remaining_pods = (node.allocatable.pods - existing_pod_resources.len() as i64 - 1).max(0);

    for existing in existing_pod_resources {
        remaining_cpu -= existing.effective_cpu_milli();
        remaining_mem -= existing.effective_memory_ki();
    }

    // Prefer nodes with more remaining resources (spread strategy).
    // Use a simple sum of remaining percentages to avoid overflow.
    let cpu_pct = if node.allocatable.cpu_milli > 0 {
        (remaining_cpu * 100) / node.allocatable.cpu_milli
    } else {
        100
    };
    let mem_pct = if node.allocatable.memory_ki > 0 {
        (remaining_mem * 100) / node.allocatable.memory_ki
    } else {
        100
    };
    let pods_pct = if node.allocatable.pods > 0 {
        (remaining_pods * 100) / node.allocatable.pods
    } else {
        100
    };

    cpu_pct + mem_pct + pods_pct + prefer_no_schedule_penalty(node, pod)
}

/// Select the best node from a list of candidates.
///
/// Returns the node name with the highest score.
/// Ties are broken by node name for deterministic scheduling decisions.
pub fn select_best_node(
    nodes: &[SchedulableNode],
    pod: &PodSchedulingConstraints,
    existing_resources: &[(&str, Vec<PodResources>)],
) -> Option<String> {
    if nodes.is_empty() {
        return None;
    }

    // Build a map from node name to existing pod resources on that node
    let resource_map: std::collections::HashMap<&str, &[PodResources]> = existing_resources
        .iter()
        .map(|(name, res)| (*name, res.as_slice()))
        .collect();

    let mut best_nodes: Vec<&SchedulableNode> = Vec::new();
    let mut best_score: i64 = i64::MIN;

    for node in nodes {
        let existing = resource_map.get(node.name.as_str()).copied().unwrap_or(&[]);
        let score = score_node(node, pod, existing);

        match score.cmp(&best_score) {
            std::cmp::Ordering::Greater => {
                best_score = score;
                best_nodes.clear();
                best_nodes.push(node);
            }
            std::cmp::Ordering::Equal => {
                best_nodes.push(node);
            }
            std::cmp::Ordering::Less => {}
        }
    }

    if best_nodes.is_empty() {
        return None;
    }

    best_nodes
        .into_iter()
        .min_by(|a, b| a.name.cmp(&b.name))
        .map(|node| node.name.clone())
}

/// Run the full scheduling algorithm: filter + score.
///
/// Returns a scheduling decision with the selected node or failure reasons.
pub fn schedule(
    nodes: &[SchedulableNode],
    pod: &PodSchedulingConstraints,
    existing_resources: &[(&str, Vec<PodResources>)],
) -> SchedulingDecision {
    let resource_map: std::collections::HashMap<&str, &[PodResources]> = existing_resources
        .iter()
        .map(|(name, res)| (*name, res.as_slice()))
        .collect();

    // Filter: run predicates on each node
    let mut fit_nodes = Vec::new();
    let mut all_reasons = Vec::new();

    for node in nodes {
        let existing = resource_map.get(node.name.as_str()).copied().unwrap_or(&[]);
        let reasons = super::predicates::node_fit(node, pod, existing);
        if reasons.is_empty() {
            fit_nodes.push(node.clone());
        } else {
            // Collect unique failure reasons
            for r in &reasons {
                if !all_reasons.contains(r) {
                    all_reasons.push(r.clone());
                }
            }
        }
    }

    if fit_nodes.is_empty() {
        return SchedulingDecision::failed(all_reasons);
    }

    // Score and select
    let selected = select_best_node(&fit_nodes, pod, existing_resources);
    match selected {
        Some(name) => SchedulingDecision::success(name),
        None => SchedulingDecision::failed(vec!["no node selected after scoring".into()]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    fn make_pod(cpu: i64, mem: i64) -> PodSchedulingConstraints {
        PodSchedulingConstraints {
            resources: PodResources {
                cpu_milli: cpu,
                memory_ki: mem,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn score_prefers_more_remaining() {
        let big = make_node("big", 8000, 16384000);
        let small = make_node("small", 2000, 4096000);
        let pod = make_pod(500, 512000);

        let big_score = score_node(&big, &pod, &[]);
        let small_score = score_node(&small, &pod, &[]);

        assert!(
            big_score > small_score,
            "node with more remaining resources should score higher: big={} small={}",
            big_score,
            small_score
        );
    }

    #[test]
    fn select_best_node_picks_highest_score() {
        let big = make_node("big", 8000, 16384000);
        let small = make_node("small", 2000, 4096000);
        let pod = make_pod(500, 512000);

        let selected = select_best_node(&[big, small], &pod, &[]);
        assert_eq!(selected, Some("big".into()));
    }

    #[test]
    fn select_best_node_ties_broken_by_node_name_deterministically() {
        let node_a = make_node("node-a", 4000, 8192000);
        let node_b = make_node("node-b", 4000, 8192000);
        let pod = make_pod(500, 512000);

        for _ in 0..50 {
            let selected = select_best_node(&[node_b.clone(), node_a.clone()], &pod, &[]);
            assert_eq!(selected, Some("node-a".into()));
        }
    }

    #[test]
    fn schedule_selects_fit_node() {
        let node_a = make_node("node-a", 4000, 8192000);
        let pod = make_pod(500, 512000);

        let decision = schedule(&[node_a], &pod, &[]);
        assert!(decision.is_success());
        assert_eq!(decision.selected_node, Some("node-a".into()));
    }

    #[test]
    fn schedule_fails_when_no_node_fits() {
        let node_a = make_node("node-a", 1000, 1000);
        let pod = make_pod(5000, 5000);

        let decision = schedule(&[node_a], &pod, &[]);
        assert!(!decision.is_success());
        assert!(
            decision
                .failed_reasons
                .iter()
                .any(|r| r.contains("Insufficient cpu"))
        );
    }

    #[test]
    fn schedule_considers_existing_resources() {
        let node = make_node("node-a", 2000, 8192000);
        let pod = make_pod(1000, 512000);
        let existing = vec![(
            "node-a",
            vec![PodResources {
                cpu_milli: 500,
                ..Default::default()
            }],
        )];

        let decision = schedule(&[node], &pod, &existing);
        assert!(decision.is_success());
    }

    #[test]
    fn schedule_skips_unschedulable_node() {
        let mut node = make_node("node-a", 4000, 8192000);
        node.unschedulable = true;
        let pod = make_pod(500, 512000);

        let decision = schedule(&[node], &pod, &[]);
        assert!(!decision.is_success());
        assert!(
            decision
                .failed_reasons
                .iter()
                .any(|r| r.contains("unschedulable"))
        );
    }

    #[test]
    fn schedule_skips_notready_node() {
        let mut node = make_node("node-a", 4000, 8192000);
        node.ready = false;
        let pod = make_pod(500, 512000);

        let decision = schedule(&[node], &pod, &[]);
        assert!(!decision.is_success());
        assert!(
            decision
                .failed_reasons
                .iter()
                .any(|r| r.contains("not ready"))
        );
    }

    #[test]
    fn schedule_with_node_selector() {
        let mut node_a = make_node("node-a", 4000, 8192000);
        node_a.labels.insert("zone".into(), "us-west".into());
        let node_b = make_node("node-b", 4000, 8192000);
        let mut pod = make_pod(500, 512000);
        pod.node_selector.insert("zone".into(), "us-west".into());

        let decision = schedule(&[node_b, node_a], &pod, &[]);
        assert!(decision.is_success());
        assert_eq!(decision.selected_node, Some("node-a".into()));
    }

    #[test]
    fn schedule_prefers_spread() {
        // Node A has existing pods, node B is empty
        let node_a = make_node("node-a", 4000, 8192000);
        let node_b = make_node("node-b", 4000, 8192000);
        let pod = make_pod(500, 512000);
        let existing = vec![(
            "node-a",
            vec![PodResources {
                cpu_milli: 3000,
                ..Default::default()
            }],
        )];

        let decision = schedule(&[node_a, node_b], &pod, &existing);
        assert!(decision.is_success());
        assert_eq!(decision.selected_node, Some("node-b".into()));
    }

    #[test]
    fn schedule_prefers_fewer_existing_pods_when_resource_scores_tie() {
        let node_a = make_node("node-a", 4000, 8192000);
        let node_b = make_node("node-b", 4000, 8192000);
        let pod = make_pod(0, 0);
        let existing = vec![(
            "node-a",
            vec![
                PodResources::default(),
                PodResources::default(),
                PodResources::default(),
            ],
        )];

        let decision = schedule(&[node_a, node_b], &pod, &existing);
        assert!(decision.is_success());
        assert_eq!(decision.selected_node, Some("node-b".into()));
    }

    #[test]
    fn empty_nodes_returns_none() {
        let pod = make_pod(500, 512000);
        let selected = select_best_node(&[], &pod, &[]);
        assert!(selected.is_none());
    }

    // ---- PreferNoSchedule soft scoring ----

    #[test]
    fn prefer_no_schedule_penalty_without_toleration() {
        let mut node = make_node("node-a", 4000, 8192000);
        node.taints.push(Taint {
            key: "dedicated".into(),
            value: Some("gpu".into()),
            effect: TaintEffect::PreferNoSchedule,
        });
        let pod = make_pod(500, 512000);
        let penalty = prefer_no_schedule_penalty(&node, &pod);
        assert!(penalty < 0, "penalty should be negative: {}", penalty);
    }

    #[test]
    fn prefer_no_schedule_no_penalty_with_toleration() {
        let mut node = make_node("node-a", 4000, 8192000);
        node.taints.push(Taint {
            key: "dedicated".into(),
            value: Some("gpu".into()),
            effect: TaintEffect::PreferNoSchedule,
        });
        let mut pod = make_pod(500, 512000);
        pod.tolerations.push(Toleration {
            key: Some("dedicated".into()),
            value: Some("gpu".into()),
            operator: TolerationOperator::Equal,
            effect: Some(TaintEffect::PreferNoSchedule),
        });
        let penalty = prefer_no_schedule_penalty(&node, &pod);
        assert_eq!(penalty, 0, "penalty should be zero with toleration");
    }

    #[test]
    fn prefer_no_schedule_penalized_but_still_selected_when_only_option() {
        let mut node = make_node("node-a", 4000, 8192000);
        node.taints.push(Taint {
            key: "dedicated".into(),
            value: Some("gpu".into()),
            effect: TaintEffect::PreferNoSchedule,
        });
        let pod = make_pod(500, 512000);
        let decision = schedule(&[node], &pod, &[]);
        assert!(decision.is_success());
        assert_eq!(decision.selected_node, Some("node-a".into()));
    }

    #[test]
    fn prefer_no_schedule_loses_to_clean_node() {
        let mut tainted = make_node("node-tainted", 4000, 8192000);
        tainted.taints.push(Taint {
            key: "dedicated".into(),
            value: Some("gpu".into()),
            effect: TaintEffect::PreferNoSchedule,
        });
        let clean = make_node("node-clean", 4000, 8192000);
        let pod = make_pod(500, 512000);
        let decision = schedule(&[tainted, clean], &pod, &[]);
        assert!(decision.is_success());
        assert_eq!(decision.selected_node, Some("node-clean".into()));
    }
}
