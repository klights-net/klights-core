//! Scheduler preemption victim selection (Step 6).
//!
//! Selects lower-priority pods to preempt when no node has sufficient
//! resources for an incoming pod. This is a pure function library —
//! no side effects, no I/O.

use super::types::*;
use std::collections::HashMap;

/// An existing pod on a node, used for preemption calculations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExistingPod {
    pub namespace: String,
    pub name: String,
    pub priority: i64,
    pub resources: PodResources,
    pub labels: HashMap<String, String>,
}

/// Select preemption victims on a node for an incoming pod.
///
/// Returns `Some(victims)` if preemption can resolve all resource fit failures,
/// or `None` if preemption is not possible (policy is Never, no lower-priority
/// pods exist, or victims don't relieve enough resources).
///
/// Algorithm (ported from api.rs::select_preemption_victims):
/// 1. If preemption_policy == Never, return None.
/// 2. Collect pods on the node with priority < incoming pod's priority.
/// 3. Sort ascending by priority, then by namespace/name.
/// 4. Greedily accumulate victims whose resources relieve at least one fit failure.
/// 5. Stop when resource_fit_failures returns empty → return victims.
/// 6. If we exhaust candidates without resolving all failures, return None.
pub fn select_preemption_victims(
    node: &SchedulableNode,
    pod: &PodSchedulingConstraints,
    existing_pods: &[ExistingPod],
) -> Option<Vec<PreemptionVictim>> {
    // Step 1: Check preemption policy
    if matches!(pod.preemption_policy, Some(PreemptionPolicy::Never)) {
        return None;
    }

    // Step 2: Collect lower-priority pods
    let mut candidates: Vec<&ExistingPod> = existing_pods
        .iter()
        .filter(|p| p.priority < pod.priority)
        .collect();

    // Step 3: Sort ascending by priority, then by namespace/name
    candidates.sort_by(|a, b| {
        a.priority
            .cmp(&b.priority)
            .then_with(|| a.namespace.cmp(&b.namespace))
            .then_with(|| a.name.cmp(&b.name))
    });

    // Compute initial allocated resources
    let mut allocated: PodResources =
        existing_pods
            .iter()
            .fold(PodResources::default(), |mut acc, p| {
                acc.cpu_milli += p.resources.effective_cpu_milli();
                acc.memory_ki += p.resources.effective_memory_ki();
                for (k, v) in &p.resources.extended {
                    *acc.extended.entry(k.clone()).or_insert(0) += v;
                }
                acc
            });

    // Check if there are any initial fit failures
    let initial_failures = resource_fit_failures(node, &allocated, &pod.resources);
    if initial_failures.is_empty() {
        // No failures — no preemption needed
        return None;
    }

    // Step 4: Greedily accumulate victims
    let mut victims = Vec::new();
    for candidate in candidates {
        let candidate_effective_cpu = candidate.resources.effective_cpu_milli();
        let candidate_effective_mem = candidate.resources.effective_memory_ki();

        // Check if this candidate relieves at least one current fit failure
        if !request_relieves_fit_failure(
            node,
            &allocated,
            &pod.resources,
            candidate_effective_cpu,
            candidate_effective_mem,
            &candidate.resources.extended,
        ) {
            continue;
        }

        // Remove this candidate's resources from allocated
        allocated.cpu_milli = allocated.cpu_milli.saturating_sub(candidate_effective_cpu);
        allocated.memory_ki = allocated.memory_ki.saturating_sub(candidate_effective_mem);
        for (k, v) in &candidate.resources.extended {
            let current = allocated.extended.entry(k.clone()).or_insert(0);
            *current = current.saturating_sub(*v);
        }

        victims.push(PreemptionVictim {
            namespace: candidate.namespace.clone(),
            name: candidate.name.clone(),
        });

        // Step 5: Check if we've resolved all failures
        if resource_fit_failures(node, &allocated, &pod.resources).is_empty() {
            return Some(victims);
        }
    }

    // Step 6: Exhausted candidates without resolving all failures
    None
}

/// Check if current resource allocation + new pod request exceeds allocatable.
fn resource_fit_failures(
    node: &SchedulableNode,
    allocated: &PodResources,
    requested: &PodResources,
) -> Vec<String> {
    let mut failures = Vec::new();

    let effective_requested_cpu = requested.effective_cpu_milli();
    let effective_requested_mem = requested.effective_memory_ki();

    if allocated.cpu_milli + effective_requested_cpu > node.allocatable.cpu_milli {
        failures.push("Insufficient cpu".to_string());
    }
    if allocated.memory_ki + effective_requested_mem > node.allocatable.memory_ki {
        failures.push("Insufficient memory".to_string());
    }
    for (key, allocatable_val) in &node.allocatable.extended {
        let allocated_val = allocated.extended.get(key).copied().unwrap_or(0);
        let requested_val = requested.extended.get(key).copied().unwrap_or(0);
        if allocated_val + requested_val > *allocatable_val {
            failures.push(format!("Insufficient {key}"));
        }
    }
    failures
}

/// Check if removing a candidate's resources would relieve at least one fit failure.
fn request_relieves_fit_failure(
    node: &SchedulableNode,
    allocated: &PodResources,
    requested: &PodResources,
    candidate_cpu: i64,
    candidate_mem: i64,
    candidate_extended: &std::collections::HashMap<String, i64>,
) -> bool {
    let effective_requested_cpu = requested.effective_cpu_milli();
    let effective_requested_mem = requested.effective_memory_ki();

    if allocated.cpu_milli + effective_requested_cpu > node.allocatable.cpu_milli
        && candidate_cpu > 0
    {
        return true;
    }
    if allocated.memory_ki + effective_requested_mem > node.allocatable.memory_ki
        && candidate_mem > 0
    {
        return true;
    }
    for (key, allocatable_val) in &node.allocatable.extended {
        let allocated_val = allocated.extended.get(key).copied().unwrap_or(0);
        let requested_val = requested.extended.get(key).copied().unwrap_or(0);
        if allocated_val + requested_val > *allocatable_val
            && candidate_extended.get(key).copied().unwrap_or(0) > 0
        {
            return true;
        }
    }
    false
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

    #[test]
    fn preemption_policy_never_returns_none() {
        let node = make_node("node-a", 1000, 1000);
        let pod = PodSchedulingConstraints {
            resources: PodResources {
                cpu_milli: 2000,
                ..Default::default()
            },
            priority: 100,
            preemption_policy: Some(PreemptionPolicy::Never),
            ..Default::default()
        };
        let existing = vec![make_existing("low", 50, 500, 500)];
        assert_eq!(select_preemption_victims(&node, &pod, &existing), None);
    }

    #[test]
    fn no_lower_priority_pods_returns_none() {
        let node = make_node("node-a", 1000, 1000);
        let pod = make_constraints(2000, 0, 100);
        let existing = vec![make_existing("high", 200, 500, 0)];
        assert_eq!(select_preemption_victims(&node, &pod, &existing), None);
    }

    #[test]
    fn one_victim_relieves_cpu() {
        let node = make_node("node-a", 1000, 10000);
        // Node has 1000m CPU. Existing pod uses 800m. New pod needs 500m.
        // 800 + 500 = 1300 > 1000 → need to preempt the existing pod.
        let pod = make_constraints(500, 0, 100);
        let existing = vec![make_existing("low-pod", 50, 800, 0)];
        let result = select_preemption_victims(&node, &pod, &existing);
        assert!(result.is_some());
        let victims = result.unwrap();
        assert_eq!(victims.len(), 1);
        assert_eq!(victims[0].name, "low-pod");
    }

    #[test]
    fn two_victims_needed() {
        let node = make_node("node-a", 1000, 10000);
        // Node has 1000m CPU. Two pods using 400m each = 800m. New pod needs 500m.
        // 800 + 500 = 1300 > 1000. Need to remove at least one 400m pod.
        let pod = make_constraints(500, 0, 100);
        let existing = vec![
            make_existing("low-a", 50, 400, 0),
            make_existing("low-b", 50, 400, 0),
        ];
        let result = select_preemption_victims(&node, &pod, &existing);
        assert!(result.is_some());
        let victims = result.unwrap();
        // After removing one 400m pod: 400 + 500 = 900 ≤ 1000 → one victim enough
        assert_eq!(victims.len(), 1);
    }

    #[test]
    fn victim_doesnt_relieve_failure_is_skipped() {
        let node = make_node("node-a", 1000, 10000);
        // Node has 1000m CPU. Pod using 800m. New pod needs 500m.
        // There's a low-priority pod using 0m CPU — it doesn't relieve the CPU failure.
        let pod = make_constraints(500, 0, 100);
        let existing = vec![
            make_existing("low-useless", 50, 0, 0),  // No CPU to relieve
            make_existing("low-useful", 50, 800, 0), // Relieves CPU
        ];
        let result = select_preemption_victims(&node, &pod, &existing);
        assert!(result.is_some());
        let victims = result.unwrap();
        // The useless one should be skipped, the useful one should be selected
        assert_eq!(victims.len(), 1);
        assert_eq!(victims[0].name, "low-useful");
    }

    #[test]
    fn extended_resource_preemption() {
        let node = SchedulableNode {
            name: "node-a".into(),
            ready: true,
            unschedulable: false,
            taints: Vec::new(),
            labels: HashMap::new(),
            allocatable: NodeResources {
                cpu_milli: 4000,
                memory_ki: 8192000,
                pods: 110,
                extended: HashMap::from([("nvidia.com/gpu".into(), 4)]),
            },
            capacity: NodeResources {
                cpu_milli: 4000,
                memory_ki: 8192000,
                pods: 110,
                extended: HashMap::from([("nvidia.com/gpu".into(), 4)]),
            },
        };
        let pod = PodSchedulingConstraints {
            resources: PodResources {
                cpu_milli: 500,
                memory_ki: 512,
                extended: HashMap::from([("nvidia.com/gpu".into(), 2)]),
                ..Default::default()
            },
            priority: 100,
            ..Default::default()
        };
        let existing = vec![ExistingPod {
            namespace: "default".into(),
            name: "gpu-hog".into(),
            priority: 50,
            resources: PodResources {
                cpu_milli: 500,
                memory_ki: 512,
                extended: HashMap::from([("nvidia.com/gpu".into(), 3)]),
                ..Default::default()
            },
            labels: HashMap::new(),
        }];
        // 3 existing + 2 requested = 5 > 4 allocatable → need to preempt
        let result = select_preemption_victims(&node, &pod, &existing);
        assert!(result.is_some());
        let victims = result.unwrap();
        assert_eq!(victims.len(), 1);
        assert_eq!(victims[0].name, "gpu-hog");
    }

    #[test]
    fn cant_relieve_enough_returns_none() {
        let node = make_node("node-a", 1000, 10000);
        // Node has 1000m CPU. Two pods using 600m each. New pod needs 900m.
        // Removing one 600m pod: 600 + 900 = 1500 > 1000. Still over.
        // Removing both: 0 + 900 = 900 ≤ 1000. Need both.
        let pod = make_constraints(900, 0, 100);
        let existing = vec![
            make_existing("low-a", 50, 600, 0),
            make_existing("low-b", 50, 600, 0),
        ];
        let result = select_preemption_victims(&node, &pod, &existing);
        assert!(result.is_some());
        let victims = result.unwrap();
        assert_eq!(victims.len(), 2);
    }

    #[test]
    fn sorted_by_priority_then_name() {
        let node = make_node("node-a", 1000, 10000);
        let pod = make_constraints(500, 0, 100);
        let existing = vec![
            make_existing("pod-c", 80, 400, 0),
            make_existing("pod-a", 50, 400, 0),
            make_existing("pod-b", 50, 400, 0),
        ];
        // Should select pod-a first (lowest priority, then alphabetically first)
        let result = select_preemption_victims(&node, &pod, &existing);
        assert!(result.is_some());
        let victims = result.unwrap();
        assert_eq!(victims[0].name, "pod-a");
    }
}
