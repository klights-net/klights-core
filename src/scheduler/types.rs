//! Scheduler types (2A-8).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A node's schedulable resources.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeResources {
    pub cpu_milli: i64,
    pub memory_ki: i64,
    pub pods: i64,
    /// Extended resources (e.g. "nvidia.com/gpu": 4).
    pub extended: HashMap<String, i64>,
}

/// A pod's resource requests.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PodResources {
    pub cpu_milli: i64,
    pub memory_ki: i64,
    /// Extended resource requests.
    pub extended: HashMap<String, i64>,
    /// Pod overhead (from spec.overhead) — added to effective request.
    pub overhead_cpu_milli: i64,
    /// Pod overhead memory (from spec.overhead) — added to effective request.
    pub overhead_memory_ki: i64,
}

impl PodResources {
    /// Effective CPU request including overhead.
    pub fn effective_cpu_milli(&self) -> i64 {
        self.cpu_milli + self.overhead_cpu_milli
    }

    /// Effective memory request including overhead.
    pub fn effective_memory_ki(&self) -> i64 {
        self.memory_ki + self.overhead_memory_ki
    }
}

/// A node's status for scheduling purposes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulableNode {
    pub name: String,
    pub ready: bool,
    pub unschedulable: bool,
    pub taints: Vec<Taint>,
    pub labels: HashMap<String, String>,
    pub allocatable: NodeResources,
    pub capacity: NodeResources,
}

/// A taint on a node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Taint {
    pub key: String,
    pub value: Option<String>,
    pub effect: TaintEffect,
}

/// Taint effect.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaintEffect {
    NoSchedule,
    PreferNoSchedule,
    NoExecute,
}

/// Preemption policy for a pod.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreemptionPolicy {
    #[default]
    PreemptLowerPriority,
    Never,
}

/// A pod's tolerations.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Toleration {
    pub key: Option<String>,
    pub value: Option<String>,
    pub operator: TolerationOperator,
    pub effect: Option<TaintEffect>,
}

/// Toleration operator.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TolerationOperator {
    Equal,
    Exists,
}

/// A pod's scheduling constraints.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PodSchedulingConstraints {
    pub namespace: String,
    pub labels: HashMap<String, String>,
    pub node_selector: HashMap<String, String>,
    pub required_node_affinity: Vec<NodeSelectorTerm>,
    pub required_pod_affinity: Vec<PodAffinityTerm>,
    pub required_pod_anti_affinity: Vec<PodAffinityTerm>,
    pub topology_spread_constraints: Vec<TopologySpreadConstraint>,
    pub tolerations: Vec<Toleration>,
    pub resources: PodResources,
    pub host_port_requests: Vec<u16>,
    /// Pod priority (from spec.priority or PriorityClass).
    pub priority: i64,
    /// Priority class name (from spec.priorityClassName).
    pub priority_class_name: Option<String>,
    /// Preemption policy.
    pub preemption_policy: Option<PreemptionPolicy>,
}

/// A Pod topology spread constraint.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopologySpreadConstraint {
    pub max_skew: i64,
    pub min_domains: Option<i64>,
    pub topology_key: String,
    pub when_unsatisfiable: TopologySpreadUnsatisfiableAction,
    pub label_selector: Option<LabelSelectorTerm>,
    pub match_label_keys: Vec<String>,
    pub node_affinity_policy: NodeInclusionPolicy,
    pub node_taints_policy: NodeInclusionPolicy,
}

/// Action when a topology spread constraint cannot be satisfied.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum TopologySpreadUnsatisfiableAction {
    #[default]
    DoNotSchedule,
    ScheduleAnyway,
}

/// Node inclusion policy for topology spread skew calculations.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeInclusionPolicy {
    #[default]
    Honor,
    Ignore,
}

/// A required pod affinity or anti-affinity term.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PodAffinityTerm {
    pub label_selector: Option<LabelSelectorTerm>,
    pub namespaces: Option<Vec<String>>,
    pub namespace_selector: Option<LabelSelectorTerm>,
    pub topology_key: String,
}

/// A Kubernetes label selector in structured form.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LabelSelectorTerm {
    pub match_labels: HashMap<String, String>,
    pub match_expressions: Vec<LabelSelectorRequirement>,
}

impl LabelSelectorTerm {
    pub fn is_empty(&self) -> bool {
        self.match_labels.is_empty() && self.match_expressions.is_empty()
    }

    pub fn matches(&self, labels: &HashMap<String, String>) -> bool {
        self.match_labels
            .iter()
            .all(|(key, value)| labels.get(key) == Some(value))
            && self
                .match_expressions
                .iter()
                .all(|requirement| requirement.matches(labels))
    }
}

/// A label selector matchExpression requirement.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LabelSelectorRequirement {
    pub key: String,
    pub operator: LabelSelectorOperator,
    pub values: Vec<String>,
}

impl LabelSelectorRequirement {
    fn matches(&self, labels: &HashMap<String, String>) -> bool {
        let label_value = labels.get(&self.key);
        match self.operator {
            LabelSelectorOperator::In => label_value
                .is_some_and(|value| self.values.iter().any(|candidate| candidate == value)),
            LabelSelectorOperator::NotIn => label_value
                .map(|value| self.values.iter().all(|candidate| candidate != value))
                .unwrap_or(true),
            LabelSelectorOperator::Exists => label_value.is_some(),
            LabelSelectorOperator::DoesNotExist => label_value.is_none(),
        }
    }
}

/// Label selector matchExpression operators.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LabelSelectorOperator {
    In,
    NotIn,
    Exists,
    DoesNotExist,
}

/// Existing cluster state needed by inter-pod affinity predicates.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InterPodAffinityContext {
    pub existing_pods: Vec<ScheduledPod>,
    pub nodes_by_name: HashMap<String, SchedulableNode>,
    pub node_labels_by_name: HashMap<String, HashMap<String, String>>,
    pub namespace_labels_by_name: HashMap<String, HashMap<String, String>>,
}

/// A pod already assigned to a node.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScheduledPod {
    pub namespace: String,
    pub name: String,
    pub node_name: String,
    pub labels: HashMap<String, String>,
}

/// Scheduling-time view of a PodDisruptionBudget.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PodDisruptionBudgetConstraint {
    pub namespace: String,
    pub name: String,
    pub selector: LabelSelectorTerm,
    pub disruptions_allowed: i64,
}

/// A node selector term for required node affinity.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSelectorTerm {
    pub match_expressions: Vec<NodeSelectorRequirement>,
    /// matchFields checks against node fields (not labels).
    pub match_fields: Vec<NodeSelectorRequirement>,
}

/// A node selector requirement.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSelectorRequirement {
    pub key: String,
    pub operator: NodeSelectorOperator,
    pub values: Vec<String>,
}

/// Node selector operators.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeSelectorOperator {
    In,
    NotIn,
    Exists,
    DoesNotExist,
    Gt,
    Lt,
}

/// A preemption victim.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreemptionVictim {
    pub namespace: String,
    pub name: String,
}

/// Result of a scheduling attempt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulingDecision {
    /// The selected node, if scheduling succeeded.
    pub selected_node: Option<String>,
    /// Reasons why scheduling failed (empty on success).
    pub failed_reasons: Vec<String>,
    /// Preemption victims (if preemption was attempted).
    pub preemption_victims: Vec<String>,
    /// Unschedulable message for the pod (human-readable summary).
    pub unschedulable_message: Option<String>,
}

impl SchedulingDecision {
    /// Create a successful scheduling decision.
    pub fn success(node: String) -> Self {
        Self {
            selected_node: Some(node),
            failed_reasons: Vec::new(),
            preemption_victims: Vec::new(),
            unschedulable_message: None,
        }
    }

    /// Create a failed scheduling decision.
    pub fn failed(reasons: Vec<String>) -> Self {
        Self {
            selected_node: None,
            failed_reasons: reasons,
            preemption_victims: Vec::new(),
            unschedulable_message: None,
        }
    }

    /// Create a failed scheduling decision with a human-readable message.
    pub fn failed_with_message(reasons: Vec<String>, message: String) -> Self {
        Self {
            selected_node: None,
            failed_reasons: reasons,
            preemption_victims: Vec::new(),
            unschedulable_message: Some(message),
        }
    }

    /// Create a preemption scheduling decision.
    pub fn preempt(node: String, victims: Vec<String>) -> Self {
        Self {
            selected_node: Some(node),
            failed_reasons: Vec::new(),
            preemption_victims: victims,
            unschedulable_message: None,
        }
    }

    /// Create a preemption scheduling decision with typed victims.
    pub fn preempt_with_victims(node: String, victims: Vec<PreemptionVictim>) -> Self {
        Self {
            selected_node: Some(node),
            failed_reasons: Vec::new(),
            preemption_victims: victims
                .iter()
                .map(|v| format!("{}/{}", v.namespace, v.name))
                .collect(),
            unschedulable_message: None,
        }
    }

    /// Returns true if scheduling succeeded.
    pub fn is_success(&self) -> bool {
        self.selected_node.is_some()
    }
}

/// Parse a CPU quantity string to millicores.
/// Delegates to `resource_quota::parse_resource_quantity`.
///
/// # Examples
/// - `"1"` → 1000m
/// - `"500m"` → 500m
/// - `"2"` → 2000m
pub fn parse_cpu_quantity(s: &str) -> i64 {
    crate::controllers::resource_quota::parse_resource_quantity("cpu", s).unwrap_or(0)
}

/// Parse a memory quantity string to bytes.
/// Delegates to `resource_quota::parse_resource_quantity`.
///
/// # Examples
/// - `"1Gi"` → 1073741824
/// - `"512Mi"` → 536870912
pub fn parse_memory_quantity(s: &str) -> i64 {
    crate::controllers::resource_quota::parse_resource_quantity("memory", s).unwrap_or(0)
}

/// Parse a scalar/extended resource quantity string to an integer.
/// Uses decimal SI parsing for non-CPU, non-memory resources.
pub fn parse_scalar_quantity(key: &str, s: &str) -> i64 {
    crate::controllers::resource_quota::parse_resource_quantity(key, s).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pod_resources_default_has_zero_overhead() {
        let res = PodResources::default();
        assert_eq!(res.overhead_cpu_milli, 0);
        assert_eq!(res.overhead_memory_ki, 0);
        assert_eq!(res.effective_cpu_milli(), 0);
        assert_eq!(res.effective_memory_ki(), 0);
    }

    #[test]
    fn pod_resources_effective_includes_overhead() {
        let res = PodResources {
            cpu_milli: 500,
            memory_ki: 1024,
            overhead_cpu_milli: 100,
            overhead_memory_ki: 256,
            ..Default::default()
        };
        assert_eq!(res.effective_cpu_milli(), 600);
        assert_eq!(res.effective_memory_ki(), 1280);
    }

    #[test]
    fn scheduling_constraints_default_zero_priority() {
        let c = PodSchedulingConstraints::default();
        assert_eq!(c.priority, 0);
        assert_eq!(c.priority_class_name, None);
        assert_eq!(c.preemption_policy, None);
    }

    #[test]
    fn node_selector_term_default_empty_match_fields() {
        let t = NodeSelectorTerm::default();
        assert!(t.match_expressions.is_empty());
        assert!(t.match_fields.is_empty());
    }

    #[test]
    fn preemption_policy_default_is_preempt_lower() {
        assert_eq!(
            PreemptionPolicy::default(),
            PreemptionPolicy::PreemptLowerPriority
        );
    }

    #[test]
    fn scheduling_decision_success_has_no_unschedulable_message() {
        let d = SchedulingDecision::success("node-a".into());
        assert!(d.is_success());
        assert_eq!(d.selected_node, Some("node-a".into()));
        assert_eq!(d.unschedulable_message, None);
    }

    #[test]
    fn scheduling_decision_failed_with_message() {
        let d = SchedulingDecision::failed_with_message(
            vec!["Insufficient cpu".into()],
            "0/1 nodes are available".into(),
        );
        assert!(!d.is_success());
        assert_eq!(
            d.unschedulable_message,
            Some("0/1 nodes are available".into())
        );
    }

    #[test]
    fn scheduling_decision_preempt_with_victims() {
        let d = SchedulingDecision::preempt_with_victims(
            "node-a".into(),
            vec![
                PreemptionVictim {
                    namespace: "ns1".into(),
                    name: "pod-a".into(),
                },
                PreemptionVictim {
                    namespace: "ns2".into(),
                    name: "pod-b".into(),
                },
            ],
        );
        assert!(d.is_success());
        assert_eq!(d.preemption_victims, vec!["ns1/pod-a", "ns2/pod-b"]);
    }

    #[test]
    fn types_round_trip_json() {
        let constraints = PodSchedulingConstraints {
            namespace: "default".into(),
            labels: HashMap::from([("app".into(), "web".into())]),
            node_selector: HashMap::from([("zone".into(), "us-west".into())]),
            required_node_affinity: vec![NodeSelectorTerm {
                match_expressions: vec![NodeSelectorRequirement {
                    key: "zone".into(),
                    operator: NodeSelectorOperator::In,
                    values: vec!["us-west".into()],
                }],
                match_fields: vec![NodeSelectorRequirement {
                    key: "metadata.name".into(),
                    operator: NodeSelectorOperator::In,
                    values: vec!["node-a".into()],
                }],
            }],
            required_pod_affinity: Vec::new(),
            required_pod_anti_affinity: Vec::new(),
            topology_spread_constraints: Vec::new(),
            tolerations: vec![Toleration {
                key: Some("dedicated".into()),
                value: Some("gpu".into()),
                operator: TolerationOperator::Equal,
                effect: Some(TaintEffect::NoSchedule),
            }],
            resources: PodResources {
                cpu_milli: 500,
                memory_ki: 1024,
                overhead_cpu_milli: 100,
                overhead_memory_ki: 256,
                ..Default::default()
            },
            host_port_requests: vec![8080],
            priority: 100,
            priority_class_name: Some("high-priority".into()),
            preemption_policy: Some(PreemptionPolicy::PreemptLowerPriority),
        };
        let json = serde_json::to_string(&constraints).unwrap();
        let back: PodSchedulingConstraints = serde_json::from_str(&json).unwrap();
        assert_eq!(constraints, back);
    }

    // ---- Quantity parsing ----

    #[test]
    fn parse_cpu_integer() {
        assert_eq!(parse_cpu_quantity("1"), 1000);
    }

    #[test]
    fn parse_cpu_milli() {
        assert_eq!(parse_cpu_quantity("500m"), 500);
    }

    #[test]
    fn parse_cpu_fractional() {
        assert_eq!(parse_cpu_quantity("2.5"), 2500);
    }

    #[test]
    fn parse_cpu_empty() {
        assert_eq!(parse_cpu_quantity(""), 0);
    }

    #[test]
    fn parse_memory_gi() {
        assert_eq!(parse_memory_quantity("1Gi"), 1_073_741_824);
    }

    #[test]
    fn parse_memory_mi() {
        assert_eq!(parse_memory_quantity("512Mi"), 536_870_912);
    }

    #[test]
    fn parse_memory_ki() {
        assert_eq!(parse_memory_quantity("1024Ki"), 1_048_576);
    }

    #[test]
    fn parse_memory_empty() {
        assert_eq!(parse_memory_quantity(""), 0);
    }

    #[test]
    fn parse_scalar_integer() {
        assert_eq!(parse_scalar_quantity("nvidia.com/gpu", "4"), 4);
    }

    #[test]
    fn parse_scalar_empty() {
        assert_eq!(parse_scalar_quantity("nvidia.com/gpu", ""), 0);
    }
}
