//! Adapter from the focused Pod placement port to the pure scheduler engine.

use klights_pod_api::{
    PodPlacement, PodPlacementDecision, PodPlacementRequest, PodRepositoryError,
};

#[derive(Default)]
pub struct SchedulerPlacement;

impl SchedulerPlacement {
    pub fn new() -> Self {
        Self
    }
}

impl PodPlacement for SchedulerPlacement {
    fn place_pod(
        &self,
        request: PodPlacementRequest,
    ) -> Result<PodPlacementDecision, PodRepositoryError> {
        let nodes: Vec<&serde_json::Value> = request.nodes.iter().map(AsRef::as_ref).collect();
        let namespaces: Vec<&serde_json::Value> =
            request.namespaces.iter().map(AsRef::as_ref).collect();
        let disruption_budgets: Vec<&serde_json::Value> = request
            .disruption_budgets
            .iter()
            .map(AsRef::as_ref)
            .collect();
        let existing: Vec<(&str, Vec<&serde_json::Value>)> = request
            .existing_pods_by_node
            .iter()
            .map(|(node, pods)| {
                (
                    node.as_str(),
                    pods.iter().map(AsRef::as_ref).collect::<Vec<_>>(),
                )
            })
            .collect();
        let existing_refs: Vec<(&str, &[&serde_json::Value])> = existing
            .iter()
            .map(|(node, pods)| (*node, pods.as_slice()))
            .collect();
        let decision = super::engine::schedule_from_json_with_policy(
            &nodes,
            request.incoming_pod.as_ref(),
            &existing_refs,
            &namespaces,
            &disruption_budgets,
        );
        Ok(PodPlacementDecision {
            selected_node: decision.selected_node,
            unschedulable_message: decision.unschedulable_message,
            preemption_victims: decision.preemption_victims,
        })
    }
}
