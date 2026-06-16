//! Scheduler predicates (2A-8).
//!
//! Filter predicates determine if a pod can be scheduled on a node.
//! Each predicate is a pure function with no side effects.

use super::types::*;
use std::collections::HashMap;

/// Check if a node passes all scheduling predicates for a pod.
/// Returns a list of failure reasons (empty if the node is fit).
pub fn node_fit(
    node: &SchedulableNode,
    pod: &PodSchedulingConstraints,
    existing_pod_resources: &[PodResources],
) -> Vec<String> {
    node_fit_with_context(
        node,
        pod,
        existing_pod_resources,
        &InterPodAffinityContext::default(),
    )
}

/// Check if a node passes all predicates, including inter-pod affinity.
pub fn node_fit_with_context(
    node: &SchedulableNode,
    pod: &PodSchedulingConstraints,
    existing_pod_resources: &[PodResources],
    inter_pod_context: &InterPodAffinityContext,
) -> Vec<String> {
    let mut reasons = Vec::new();

    // 1. Node is schedulable
    if node.unschedulable {
        reasons.push("node(s) were unschedulable".into());
    }

    // 2. Node is ready
    if !node.ready {
        reasons.push("node(s) were not ready".into());
    }

    // 3. Taint/toleration check
    if let Some(reason) = check_taints(node, pod) {
        reasons.push(reason);
    }

    // 4. nodeSelector + node affinity check (combined for K8s-compatible messages)
    if !node_selector_and_affinity_match(node, pod) {
        reasons.push("node(s) didn't match Pod's node affinity/selector".into());
    }

    if !required_pod_affinity_matches(node, pod, inter_pod_context) {
        reasons.push("node(s) didn't match Pod's pod affinity rules".into());
    }

    if !required_pod_anti_affinity_matches(node, pod, inter_pod_context) {
        reasons.push("node(s) didn't match Pod's pod anti-affinity rules".into());
    }

    if !topology_spread_constraints_match(node, pod, inter_pod_context) {
        reasons.push("node(s) didn't satisfy Pod's topology spread constraints".into());
    }

    // 6. Resource fit check
    if let Some(reason) = check_resource_fit(node, pod, existing_pod_resources) {
        reasons.push(reason);
    }

    reasons
}

/// Score ScheduleAnyway topology spread constraints. Higher is better.
pub fn topology_spread_schedule_anyway_score(
    node: &SchedulableNode,
    pod: &PodSchedulingConstraints,
    context: &InterPodAffinityContext,
) -> i64 {
    pod.topology_spread_constraints
        .iter()
        .filter(|constraint| {
            constraint.when_unsatisfiable == TopologySpreadUnsatisfiableAction::ScheduleAnyway
        })
        .filter_map(|constraint| evaluate_topology_spread(node, pod, constraint, context))
        .map(|evaluation| (evaluation.max_count - evaluation.candidate_count) * 100)
        .sum()
}

/// Score preferred node affinity terms. Higher is better.
pub fn preferred_node_affinity_score(
    node: &SchedulableNode,
    pod: &PodSchedulingConstraints,
) -> i64 {
    pod.preferred_node_affinity
        .iter()
        .filter(|term| term_matches(node, &term.preference))
        .map(|term| term.weight.clamp(1, 100))
        .sum()
}

fn topology_spread_constraints_match(
    node: &SchedulableNode,
    pod: &PodSchedulingConstraints,
    context: &InterPodAffinityContext,
) -> bool {
    pod.topology_spread_constraints
        .iter()
        .filter(|constraint| {
            constraint.when_unsatisfiable == TopologySpreadUnsatisfiableAction::DoNotSchedule
        })
        .all(|constraint| {
            evaluate_topology_spread(node, pod, constraint, context).is_some_and(|evaluation| {
                evaluation.candidate_count + 1 - evaluation.global_min <= constraint.max_skew
            })
        })
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TopologySpreadEvaluation {
    candidate_count: i64,
    global_min: i64,
    max_count: i64,
}

fn evaluate_topology_spread(
    node: &SchedulableNode,
    pod: &PodSchedulingConstraints,
    constraint: &TopologySpreadConstraint,
    context: &InterPodAffinityContext,
) -> Option<TopologySpreadEvaluation> {
    if constraint.topology_key.is_empty() || constraint.max_skew <= 0 {
        return None;
    }

    let candidate_domain = node.labels.get(&constraint.topology_key)?;
    let mut domain_counts = eligible_domain_counts(pod, constraint, context);
    if !domain_counts.contains_key(candidate_domain) {
        return None;
    }

    for existing in &context.existing_pods {
        if !pod_matches_topology_spread_selector(existing, pod, constraint) {
            continue;
        }
        let Some(existing_node) = context.nodes_by_name.get(&existing.node_name) else {
            continue;
        };
        if !node_included_for_topology_spread(existing_node, pod, constraint) {
            continue;
        }
        if let Some(domain) = existing_node.labels.get(&constraint.topology_key)
            && let Some(count) = domain_counts.get_mut(domain)
        {
            *count += 1;
        }
    }

    let min_domains = constraint.min_domains.unwrap_or(1);
    let global_min = if min_domains > 0 && (domain_counts.len() as i64) < min_domains {
        0
    } else {
        domain_counts.values().copied().min().unwrap_or(0)
    };
    let max_count = domain_counts.values().copied().max().unwrap_or(0);
    let candidate_count = domain_counts.get(candidate_domain).copied().unwrap_or(0);

    Some(TopologySpreadEvaluation {
        candidate_count,
        global_min,
        max_count,
    })
}

fn eligible_domain_counts(
    pod: &PodSchedulingConstraints,
    constraint: &TopologySpreadConstraint,
    context: &InterPodAffinityContext,
) -> HashMap<String, i64> {
    let mut domains = HashMap::new();
    for node in context.nodes_by_name.values() {
        if !node_included_for_topology_spread(node, pod, constraint) {
            continue;
        }
        if let Some(domain) = node.labels.get(&constraint.topology_key) {
            domains.entry(domain.clone()).or_insert(0);
        }
    }
    domains
}

fn node_included_for_topology_spread(
    node: &SchedulableNode,
    pod: &PodSchedulingConstraints,
    constraint: &TopologySpreadConstraint,
) -> bool {
    if constraint.node_affinity_policy == NodeInclusionPolicy::Honor
        && !node_selector_and_affinity_match(node, pod)
    {
        return false;
    }
    if constraint.node_taints_policy == NodeInclusionPolicy::Honor
        && check_taints(node, pod).is_some()
    {
        return false;
    }
    true
}

fn pod_matches_topology_spread_selector(
    existing: &ScheduledPod,
    pod: &PodSchedulingConstraints,
    constraint: &TopologySpreadConstraint,
) -> bool {
    if existing.namespace != pod.namespace {
        return false;
    }
    if constraint.label_selector.is_none() && constraint.match_label_keys.is_empty() {
        return false;
    }
    if let Some(selector) = &constraint.label_selector
        && !selector.matches(&existing.labels)
    {
        return false;
    }
    constraint.match_label_keys.iter().all(|key| {
        pod.labels
            .get(key)
            .map(|value| existing.labels.get(key) == Some(value))
            .unwrap_or(true)
    })
}

fn required_pod_affinity_matches(
    node: &SchedulableNode,
    pod: &PodSchedulingConstraints,
    context: &InterPodAffinityContext,
) -> bool {
    pod.required_pod_affinity.iter().all(|term| {
        matching_pods_in_topology(node, pod, term, context)
            .next()
            .is_some()
    })
}

fn required_pod_anti_affinity_matches(
    node: &SchedulableNode,
    pod: &PodSchedulingConstraints,
    context: &InterPodAffinityContext,
) -> bool {
    pod.required_pod_anti_affinity.iter().all(|term| {
        matching_pods_in_topology(node, pod, term, context)
            .next()
            .is_none()
    })
}

fn matching_pods_in_topology<'a>(
    candidate_node: &'a SchedulableNode,
    pod: &'a PodSchedulingConstraints,
    term: &'a PodAffinityTerm,
    context: &'a InterPodAffinityContext,
) -> impl Iterator<Item = &'a ScheduledPod> + 'a {
    context.existing_pods.iter().filter(move |existing| {
        pod_matches_affinity_term(existing, pod, term, context)
            && pods_share_topology(candidate_node, existing, term, context)
    })
}

fn pod_matches_affinity_term(
    existing: &ScheduledPod,
    pod: &PodSchedulingConstraints,
    term: &PodAffinityTerm,
    context: &InterPodAffinityContext,
) -> bool {
    label_selector_matches(term, &existing.labels)
        && namespace_matches_term(&existing.namespace, &pod.namespace, term, context)
}

fn label_selector_matches(term: &PodAffinityTerm, labels: &HashMap<String, String>) -> bool {
    term.label_selector
        .as_ref()
        .map(|selector| selector.matches(labels))
        .unwrap_or(false)
}

fn namespace_matches_term(
    existing_namespace: &str,
    pod_namespace: &str,
    term: &PodAffinityTerm,
    context: &InterPodAffinityContext,
) -> bool {
    let explicit_namespace_match = term.namespaces.as_ref().map(|namespaces| {
        namespaces
            .iter()
            .any(|namespace| namespace == existing_namespace)
    });
    let namespace_selector_match = term
        .namespace_selector
        .as_ref()
        .map(|selector| namespace_selector_matches(existing_namespace, selector, context));

    match (explicit_namespace_match, namespace_selector_match) {
        (Some(explicit), Some(selector)) => explicit && selector,
        (Some(explicit), None) => explicit,
        (None, Some(selector)) => selector,
        (None, None) => existing_namespace == pod_namespace,
    }
}

fn namespace_selector_matches(
    namespace: &str,
    selector: &LabelSelectorTerm,
    context: &InterPodAffinityContext,
) -> bool {
    if selector.is_empty() {
        return true;
    }
    context
        .namespace_labels_by_name
        .get(namespace)
        .is_some_and(|labels| selector.matches(labels))
}

fn pods_share_topology(
    candidate_node: &SchedulableNode,
    existing: &ScheduledPod,
    term: &PodAffinityTerm,
    context: &InterPodAffinityContext,
) -> bool {
    if term.topology_key.is_empty() {
        return false;
    }

    let Some(candidate_value) = candidate_node.labels.get(&term.topology_key) else {
        return false;
    };
    context
        .node_labels_by_name
        .get(&existing.node_name)
        .and_then(|labels| labels.get(&term.topology_key))
        == Some(candidate_value)
}

/// Check taints vs tolerations.
/// Only `NoSchedule` taints cause hard rejection.
/// `PreferNoSchedule` taints are handled in scoring (soft penalty).
/// `NoExecute` taints are not checked during scheduling.
pub(crate) fn check_taints(
    node: &SchedulableNode,
    pod: &PodSchedulingConstraints,
) -> Option<String> {
    for taint in &node.taints {
        // Only NoSchedule causes hard rejection during predicate filtering.
        if !matches!(taint.effect, TaintEffect::NoSchedule) {
            continue;
        }

        let tolerated = pod.tolerations.iter().any(|tol| {
            match tol.operator {
                TolerationOperator::Exists => {
                    // If key is None, tolerates all taints
                    if tol.key.is_none() {
                        return true;
                    }
                    // If key matches and effect matches (or effect is None)
                    if tol.key.as_deref() == Some(&taint.key)
                        && (tol.effect.is_none() || tol.effect.as_ref() == Some(&taint.effect))
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
                            && (tol.effect.is_none() || tol.effect.as_ref() == Some(&taint.effect))
                        {
                            return true;
                        }
                    }
                    false
                }
            }
        });

        if !tolerated {
            return Some(format!(
                "taint {}/{} not tolerated",
                taint.key,
                taint.value.as_deref().unwrap_or("")
            ));
        }
    }
    None
}

/// Check nodeSelector and node affinity together.
pub(crate) fn node_selector_and_affinity_match(
    node: &SchedulableNode,
    pod: &PodSchedulingConstraints,
) -> bool {
    // Check nodeSelector
    for (key, value) in &pod.node_selector {
        match node.labels.get(key) {
            Some(nv) if nv == value => continue,
            _ => return false,
        }
    }

    // Check required node affinity
    if !pod.required_node_affinity.is_empty() {
        let mut any_matches = false;
        for term in &pod.required_node_affinity {
            if term_matches(node, term) {
                any_matches = true;
                break;
            }
        }
        if !any_matches {
            return false;
        }
    }

    true
}

/// A node's schedulable fields (metadata.name, etc.).
/// Used by matchFields in node affinity.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NodeFields {
    pub metadata_name: Option<String>,
}

impl SchedulableNode {
    /// Get the node's fields for matchFields evaluation.
    pub fn fields(&self) -> NodeFields {
        NodeFields {
            metadata_name: Some(self.name.clone()),
        }
    }
}

fn term_matches(node: &SchedulableNode, term: &NodeSelectorTerm) -> bool {
    // Both match_expressions and match_fields must all pass (AND within term).
    // If both are empty, the term matches.

    for req in &term.match_expressions {
        if !requirement_matches_label(node, req) {
            return false;
        }
    }

    let node_fields = node.fields();
    for req in &term.match_fields {
        if !requirement_matches_field(&node_fields, req) {
            return false;
        }
    }

    true
}

/// Check a matchExpressions requirement against node labels.
fn requirement_matches_label(node: &SchedulableNode, req: &NodeSelectorRequirement) -> bool {
    let node_val = node.labels.get(&req.key).map(|s| s.as_str()).unwrap_or("");
    match req.operator {
        NodeSelectorOperator::In => {
            if node.labels.contains_key(&req.key) {
                req.values.iter().any(|v| v == node_val)
            } else {
                false
            }
        }
        NodeSelectorOperator::NotIn => !req.values.iter().any(|v| v == node_val),
        NodeSelectorOperator::Exists => node.labels.contains_key(&req.key),
        NodeSelectorOperator::DoesNotExist => !node.labels.contains_key(&req.key),
        NodeSelectorOperator::Gt => {
            let node_num: i64 = node_val.parse().unwrap_or(0);
            let req_num: i64 = req.values.first().and_then(|v| v.parse().ok()).unwrap_or(0);
            node_num > req_num
        }
        NodeSelectorOperator::Lt => {
            let node_num: i64 = node_val.parse().unwrap_or(0);
            let req_num: i64 = req.values.first().and_then(|v| v.parse().ok()).unwrap_or(0);
            node_num < req_num
        }
    }
}

/// Check a matchFields requirement against node fields.
/// Only `metadata.name` is supported. Unknown keys fail open (return true),
/// matching upstream K8s behavior for unrecognized fields.
fn requirement_matches_field(fields: &NodeFields, req: &NodeSelectorRequirement) -> bool {
    match req.key.as_str() {
        "metadata.name" => {
            let node_val = fields.metadata_name.as_deref().unwrap_or("");
            match req.operator {
                NodeSelectorOperator::In => req.values.iter().any(|v| v == node_val),
                NodeSelectorOperator::NotIn => !req.values.iter().any(|v| v == node_val),
                NodeSelectorOperator::Exists => fields.metadata_name.is_some(),
                NodeSelectorOperator::DoesNotExist => fields.metadata_name.is_none(),
                // Gt/Lt don't make semantic sense for metadata.name but match by
                // lexicographic comparison for correctness.
                NodeSelectorOperator::Gt => {
                    node_val > req.values.first().map(|s| s.as_str()).unwrap_or("")
                }
                NodeSelectorOperator::Lt => {
                    node_val < req.values.first().map(|s| s.as_str()).unwrap_or("")
                }
            }
        }
        // Unknown field keys: fail open (matches upstream K8s behavior).
        _ => true,
    }
}

/// Check resource fit (CPU, memory, pods, extended resources).
fn check_resource_fit(
    node: &SchedulableNode,
    pod: &PodSchedulingConstraints,
    existing_pod_resources: &[PodResources],
) -> Option<String> {
    let mut failures = Vec::new();

    // Sum new pod's effective request (request + overhead) with existing pods' effective requests.
    let mut requested_cpu = pod.resources.effective_cpu_milli();
    let mut requested_mem = pod.resources.effective_memory_ki();
    let mut requested_extended: HashMap<String, i64> = pod.resources.extended.clone();

    for existing in existing_pod_resources {
        requested_cpu += existing.effective_cpu_milli();
        requested_mem += existing.effective_memory_ki();
        for (key, val) in &existing.extended {
            *requested_extended.entry(key.clone()).or_insert(0) += val;
        }
    }

    if requested_cpu > node.allocatable.cpu_milli {
        failures.push("Insufficient cpu".to_string());
    }

    if requested_mem > node.allocatable.memory_ki {
        failures.push("Insufficient memory".to_string());
    }

    // Check pods count (existing + 1 for the new pod)
    let total_pods = existing_pod_resources.len() as i64 + 1;
    if total_pods > node.allocatable.pods {
        failures.push("Too many pods".to_string());
    }

    // Check extended resources
    for (key, requested_val) in &requested_extended {
        let available = node.allocatable.extended.get(key).copied().unwrap_or(0);
        if *requested_val > available {
            failures.push(format!("Insufficient {key}"));
        }
    }

    // Return the first failure (or None)
    failures.into_iter().next()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_node(name: &str) -> SchedulableNode {
        SchedulableNode {
            name: name.into(),
            ready: true,
            unschedulable: false,
            taints: Vec::new(),
            labels: HashMap::new(),
            allocatable: NodeResources {
                cpu_milli: 4000,
                memory_ki: 8192000,
                pods: 110,
                extended: HashMap::new(),
            },
            capacity: NodeResources {
                cpu_milli: 4000,
                memory_ki: 8192000,
                pods: 110,
                extended: HashMap::new(),
            },
        }
    }

    fn make_pod() -> PodSchedulingConstraints {
        PodSchedulingConstraints::default()
    }

    // ---- Two ready nodes ----

    #[test]
    fn two_ready_nodes_both_fit() {
        let node_a = make_node("node-a");
        let node_b = make_node("node-b");
        let pod = make_pod();

        assert!(node_fit(&node_a, &pod, &[]).is_empty());
        assert!(node_fit(&node_b, &pod, &[]).is_empty());
    }

    // ---- Unschedulable node ----

    #[test]
    fn unschedulable_node_rejected() {
        let mut node = make_node("node-a");
        node.unschedulable = true;
        let pod = make_pod();
        let reasons = node_fit(&node, &pod, &[]);
        assert!(reasons.iter().any(|r| r.contains("unschedulable")));
    }

    // ---- NotReady node ----

    #[test]
    fn notready_node_rejected() {
        let mut node = make_node("node-a");
        node.ready = false;
        let pod = make_pod();
        let reasons = node_fit(&node, &pod, &[]);
        assert!(reasons.iter().any(|r| r.contains("not ready")));
    }

    // ---- nodeSelector ----

    #[test]
    fn node_selector_match() {
        let mut node = make_node("node-a");
        node.labels.insert("zone".into(), "us-west-1".into());
        let mut pod = make_pod();
        pod.node_selector.insert("zone".into(), "us-west-1".into());
        assert!(node_fit(&node, &pod, &[]).is_empty());
    }

    #[test]
    fn node_selector_mismatch() {
        let mut node = make_node("node-a");
        node.labels.insert("zone".into(), "us-east-1".into());
        let mut pod = make_pod();
        pod.node_selector.insert("zone".into(), "us-west-1".into());
        let reasons = node_fit(&node, &pod, &[]);
        assert!(reasons.iter().any(|r| r.contains("node affinity/selector")));
    }

    #[test]
    fn node_selector_missing_label() {
        let node = make_node("node-a");
        let mut pod = make_pod();
        pod.node_selector.insert("zone".into(), "us-west-1".into());
        let reasons = node_fit(&node, &pod, &[]);
        assert!(reasons.iter().any(|r| r.contains("node affinity/selector")));
    }

    // ---- Required node affinity ----

    #[test]
    fn node_affinity_in_match() {
        let mut node = make_node("node-a");
        node.labels.insert("zone".into(), "us-west-1".into());
        let mut pod = make_pod();
        pod.required_node_affinity.push(NodeSelectorTerm {
            match_expressions: vec![NodeSelectorRequirement {
                key: "zone".into(),
                operator: NodeSelectorOperator::In,
                values: vec!["us-west-1".into(), "us-west-2".into()],
            }],
            match_fields: Vec::new(),
        });
        assert!(node_fit(&node, &pod, &[]).is_empty());
    }

    #[test]
    fn node_affinity_in_no_match() {
        let mut node = make_node("node-a");
        node.labels.insert("zone".into(), "us-east-1".into());
        let mut pod = make_pod();
        pod.required_node_affinity.push(NodeSelectorTerm {
            match_expressions: vec![NodeSelectorRequirement {
                key: "zone".into(),
                operator: NodeSelectorOperator::In,
                values: vec!["us-west-1".into()],
            }],
            match_fields: Vec::new(),
        });
        let reasons = node_fit(&node, &pod, &[]);
        assert!(reasons.iter().any(|r| r.contains("node affinity/selector")));
    }

    // ---- CPU/memory/pods fit ----

    #[test]
    fn resource_fit_cpu_exceeded() {
        let mut node = make_node("node-a");
        node.allocatable.cpu_milli = 1000;
        let mut pod = make_pod();
        pod.resources.cpu_milli = 1500;
        let reasons = node_fit(&node, &pod, &[]);
        assert!(reasons.iter().any(|r| r.contains("Insufficient cpu")));
    }

    #[test]
    fn resource_fit_memory_exceeded() {
        let mut node = make_node("node-a");
        node.allocatable.memory_ki = 1000;
        let mut pod = make_pod();
        pod.resources.memory_ki = 2000;
        let reasons = node_fit(&node, &pod, &[]);
        assert!(reasons.iter().any(|r| r.contains("Insufficient memory")));
    }

    #[test]
    fn resource_fit_pods_exceeded() {
        let mut node = make_node("node-a");
        node.allocatable.pods = 2;
        let pod = make_pod();
        let existing = vec![PodResources::default(), PodResources::default()];
        let reasons = node_fit(&node, &pod, &existing);
        assert!(reasons.iter().any(|r| r.contains("pods")));
    }

    #[test]
    fn resource_fit_with_existing_pods() {
        let mut node = make_node("node-a");
        node.allocatable.cpu_milli = 2000;
        let mut pod = make_pod();
        pod.resources.cpu_milli = 500;
        let existing = vec![PodResources {
            cpu_milli: 1000,
            ..Default::default()
        }];
        // 1000 (existing) + 500 (new) = 1500 < 2000 → fits
        assert!(node_fit(&node, &pod, &existing).is_empty());
    }

    // ---- Extended resources ----

    #[test]
    fn extended_resource_fit() {
        let mut node = make_node("node-a");
        node.allocatable.extended.insert("nvidia.com/gpu".into(), 4);
        let mut pod = make_pod();
        pod.resources.extended.insert("nvidia.com/gpu".into(), 2);
        assert!(node_fit(&node, &pod, &[]).is_empty());
    }

    #[test]
    fn extended_resource_exceeded() {
        let mut node = make_node("node-a");
        node.allocatable.extended.insert("nvidia.com/gpu".into(), 2);
        let mut pod = make_pod();
        pod.resources.extended.insert("nvidia.com/gpu".into(), 4);
        let reasons = node_fit(&node, &pod, &[]);
        assert!(
            reasons
                .iter()
                .any(|r| r.contains("Insufficient nvidia.com/gpu"))
        );
    }

    // ---- Taints/tolerations ----

    #[test]
    fn taint_not_tolerated() {
        let mut node = make_node("node-a");
        node.taints.push(Taint {
            key: "dedicated".into(),
            value: Some("gpu".into()),
            effect: TaintEffect::NoSchedule,
        });
        let pod = make_pod();
        let reasons = node_fit(&node, &pod, &[]);
        assert!(reasons.iter().any(|r| r.contains("not tolerated")));
    }

    #[test]
    fn taint_prefer_no_schedule_not_hard_reject() {
        // PreferNoSchedule should NOT cause hard rejection
        let mut node = make_node("node-a");
        node.taints.push(Taint {
            key: "dedicated".into(),
            value: Some("gpu".into()),
            effect: TaintEffect::PreferNoSchedule,
        });
        let pod = make_pod();
        // Predicate should pass
        assert!(node_fit(&node, &pod, &[]).is_empty());
    }

    #[test]
    fn taint_tolerated_equal() {
        let mut node = make_node("node-a");
        node.taints.push(Taint {
            key: "dedicated".into(),
            value: Some("gpu".into()),
            effect: TaintEffect::NoSchedule,
        });
        let mut pod = make_pod();
        pod.tolerations.push(Toleration {
            key: Some("dedicated".into()),
            value: Some("gpu".into()),
            operator: TolerationOperator::Equal,
            effect: Some(TaintEffect::NoSchedule),
        });
        assert!(node_fit(&node, &pod, &[]).is_empty());
    }

    #[test]
    fn taint_tolerated_exists() {
        let mut node = make_node("node-a");
        node.taints.push(Taint {
            key: "dedicated".into(),
            value: Some("gpu".into()),
            effect: TaintEffect::NoSchedule,
        });
        let mut pod = make_pod();
        pod.tolerations.push(Toleration {
            key: Some("dedicated".into()),
            value: None,
            operator: TolerationOperator::Exists,
            effect: None,
        });
        assert!(node_fit(&node, &pod, &[]).is_empty());
    }

    #[test]
    fn wildcard_toleration() {
        let mut node = make_node("node-a");
        node.taints.push(Taint {
            key: "special".into(),
            value: None,
            effect: TaintEffect::NoSchedule,
        });
        let mut pod = make_pod();
        pod.tolerations.push(Toleration {
            key: None,
            value: None,
            operator: TolerationOperator::Exists,
            effect: None,
        });
        assert!(node_fit(&node, &pod, &[]).is_empty());
    }

    // ---- HostPort same-node conflict ----

    #[test]
    fn host_port_conflict_same_node() {
        // HostPort conflicts are checked at a higher level (the scoring phase),
        // not at the predicate level. The predicate just checks resources.
        // This test documents that HostPort checking is NOT a predicate.
        let node = make_node("node-a");
        let mut pod = make_pod();
        pod.host_port_requests.push(8080);
        // Should still pass predicates
        assert!(node_fit(&node, &pod, &[]).is_empty());
    }

    // ---- matchFields (node affinity) ----

    #[test]
    fn match_fields_metadata_name_in_matches() {
        let node = make_node("node-a");
        let mut pod = make_pod();
        pod.required_node_affinity.push(NodeSelectorTerm {
            match_expressions: vec![],
            match_fields: vec![NodeSelectorRequirement {
                key: "metadata.name".into(),
                operator: NodeSelectorOperator::In,
                values: vec!["node-a".into()],
            }],
        });
        assert!(node_fit(&node, &pod, &[]).is_empty());
    }

    #[test]
    fn match_fields_metadata_name_in_no_match() {
        let node = make_node("node-a");
        let mut pod = make_pod();
        pod.required_node_affinity.push(NodeSelectorTerm {
            match_expressions: vec![],
            match_fields: vec![NodeSelectorRequirement {
                key: "metadata.name".into(),
                operator: NodeSelectorOperator::In,
                values: vec!["node-b".into()],
            }],
        });
        let reasons = node_fit(&node, &pod, &[]);
        assert!(reasons.iter().any(|r| r.contains("node affinity")));
    }

    #[test]
    fn match_fields_metadata_name_not_in() {
        let node = make_node("node-a");
        let mut pod = make_pod();
        pod.required_node_affinity.push(NodeSelectorTerm {
            match_expressions: vec![],
            match_fields: vec![NodeSelectorRequirement {
                key: "metadata.name".into(),
                operator: NodeSelectorOperator::NotIn,
                values: vec!["node-b".into()],
            }],
        });
        assert!(node_fit(&node, &pod, &[]).is_empty());
    }

    #[test]
    fn match_fields_metadata_name_exists() {
        let node = make_node("node-a");
        let mut pod = make_pod();
        pod.required_node_affinity.push(NodeSelectorTerm {
            match_expressions: vec![],
            match_fields: vec![NodeSelectorRequirement {
                key: "metadata.name".into(),
                operator: NodeSelectorOperator::Exists,
                values: vec![],
            }],
        });
        assert!(node_fit(&node, &pod, &[]).is_empty());
    }

    #[test]
    fn match_fields_metadata_name_does_not_exist() {
        let node = make_node("node-a");
        let mut pod = make_pod();
        pod.required_node_affinity.push(NodeSelectorTerm {
            match_expressions: vec![],
            match_fields: vec![NodeSelectorRequirement {
                key: "metadata.name".into(),
                operator: NodeSelectorOperator::DoesNotExist,
                values: vec![],
            }],
        });
        let reasons = node_fit(&node, &pod, &[]);
        assert!(reasons.iter().any(|r| r.contains("node affinity")));
    }

    #[test]
    fn match_fields_unknown_key_fails_open() {
        let node = make_node("node-a");
        let mut pod = make_pod();
        pod.required_node_affinity.push(NodeSelectorTerm {
            match_expressions: vec![],
            match_fields: vec![NodeSelectorRequirement {
                key: "unknown.field".into(),
                operator: NodeSelectorOperator::Exists,
                values: vec![],
            }],
        });
        // Unknown field keys fail open (match)
        assert!(node_fit(&node, &pod, &[]).is_empty());
    }

    #[test]
    fn match_fields_and_expressions_both_must_pass() {
        let mut node = make_node("node-a");
        node.labels.insert("zone".into(), "us-west".into());
        let mut pod = make_pod();
        pod.required_node_affinity.push(NodeSelectorTerm {
            match_expressions: vec![NodeSelectorRequirement {
                key: "zone".into(),
                operator: NodeSelectorOperator::In,
                values: vec!["us-west".into()],
            }],
            match_fields: vec![NodeSelectorRequirement {
                key: "metadata.name".into(),
                operator: NodeSelectorOperator::In,
                values: vec!["node-b".into()], // Wrong node name
            }],
        });
        // Expression passes but field doesn't → term fails
        let reasons = node_fit(&node, &pod, &[]);
        assert!(reasons.iter().any(|r| r.contains("node affinity")));
    }

    // ---- Pod overhead in resource fit ----

    #[test]
    fn pod_overhead_fits_within_allocatable() {
        let mut node = make_node("node-a");
        node.allocatable.cpu_milli = 2000;
        let mut pod = make_pod();
        pod.resources.cpu_milli = 500;
        pod.resources.overhead_cpu_milli = 200;
        // 500 + 200 = 700 < 2000 → fits
        assert!(node_fit(&node, &pod, &[]).is_empty());
    }

    #[test]
    fn pod_overhead_exceeds_allocatable() {
        let mut node = make_node("node-a");
        node.allocatable.cpu_milli = 1000;
        let mut pod = make_pod();
        pod.resources.cpu_milli = 800;
        pod.resources.overhead_cpu_milli = 300;
        // 800 + 300 = 1100 > 1000 → fails
        let reasons = node_fit(&node, &pod, &[]);
        assert!(reasons.iter().any(|r| r.contains("Insufficient cpu")));
    }

    #[test]
    fn pod_overhead_memory_exceeds_allocatable() {
        let mut node = make_node("node-a");
        node.allocatable.memory_ki = 1000;
        let mut pod = make_pod();
        pod.resources.memory_ki = 600;
        pod.resources.overhead_memory_ki = 500;
        // 600 + 500 = 1100 > 1000 → fails
        let reasons = node_fit(&node, &pod, &[]);
        assert!(reasons.iter().any(|r| r.contains("Insufficient memory")));
    }

    #[test]
    fn pod_overhead_with_existing_pods() {
        let mut node = make_node("node-a");
        node.allocatable.cpu_milli = 2000;
        let mut pod = make_pod();
        pod.resources.cpu_milli = 500;
        pod.resources.overhead_cpu_milli = 200;
        let existing = vec![PodResources {
            cpu_milli: 1000,
            overhead_cpu_milli: 100,
            ..Default::default()
        }];
        // existing effective: 1100, new effective: 700, total: 1800 < 2000 → fits
        assert!(node_fit(&node, &pod, &existing).is_empty());
    }

    #[test]
    fn pod_overhead_with_existing_pushes_over() {
        let mut node = make_node("node-a");
        node.allocatable.cpu_milli = 1500;
        let mut pod = make_pod();
        pod.resources.cpu_milli = 500;
        pod.resources.overhead_cpu_milli = 200;
        let existing = vec![PodResources {
            cpu_milli: 600,
            overhead_cpu_milli: 100,
            ..Default::default()
        }];
        // existing effective: 700, new effective: 700, total: 1400 < 1500 → fits
        assert!(node_fit(&node, &pod, &existing).is_empty());

        // But add one more existing pod and it fails
        let more_existing = vec![
            PodResources {
                cpu_milli: 600,
                overhead_cpu_milli: 100,
                ..Default::default()
            },
            PodResources {
                cpu_milli: 100,
                overhead_cpu_milli: 50,
                ..Default::default()
            },
        ];
        // existing effective: 750+150=850, new effective: 700, total: 1550 > 1500 → fails
        let reasons = node_fit(&node, &pod, &more_existing);
        assert!(reasons.iter().any(|r| r.contains("Insufficient cpu")));
    }
}
