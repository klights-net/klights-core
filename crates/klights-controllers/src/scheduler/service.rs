//! Kubernetes-native Pod scheduling, bind, and preemption orchestration.
//!
//! The service owns policy and orchestration while all reads, writes, actor
//! wakeups, events, and reconcile effects arrive through focused ports.

use std::cmp::Ordering;
use std::sync::Arc;
use std::time::Duration;

use klights_cluster_core::Resource;
use klights_leader_api::{
    LeaderResourceQuery, ResourceListRequest, ResourceListScope, ResourceQueryConsistency,
};
use klights_pod_api::{
    PodControlPlaneEventRequest, PodControlPlaneEventSink, PodDeleteOrchestration, PodGetRequest,
    PodListRequest, PodPersistence, PodPersistenceReplaceRequest, PodPlacement, PodQuery,
    PodRepositoryError, PodScheduling, PodSchedulingFuture, PodStatusPersistence,
    PodStatusWriteRequest,
};
use klights_reconcile_api::{
    ResourceChange, ResourceMutationEffectsPort, ResourceMutationEffectsRequest,
};
use klights_supervisor::{TaskCategory, TaskSupervisor, WallClock};
use serde_json::{Value, json};

pub const SCHED_BIND_CONCURRENCY: usize = 8;

enum SchedulerReason {
    FailedScheduling,
    PreemptionByScheduler,
}

impl SchedulerReason {
    const fn as_str(&self) -> &'static str {
        match self {
            Self::FailedScheduling => "FailedScheduling",
            Self::PreemptionByScheduler => "PreemptionByScheduler",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PodSchedulingDecision {
    node_name: Option<String>,
    unschedulable_message: Option<String>,
    preemption_victims: Vec<PodPreemptionCandidate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PodPreemptionCandidate {
    namespace: String,
    name: String,
    resource_version: i64,
    data: Value,
}

struct PodPreemptionContext<'a> {
    query: &'a dyn PodQuery,
    persistence: &'a dyn PodPersistence,
    deletion: &'a dyn PodDeleteOrchestration,
    mutation_effects: &'a dyn ResourceMutationEffectsPort,
    preemptor_namespace: &'a str,
    preemptor_name: &'a str,
}

#[derive(Clone)]
pub struct SchedulerService {
    pod_query: Arc<dyn PodQuery>,
    persistence: Arc<dyn PodPersistence>,
    status_persistence: Arc<dyn PodStatusPersistence>,
    deletion: Arc<dyn PodDeleteOrchestration>,
    event_sink: Arc<dyn PodControlPlaneEventSink>,
    placement: Arc<dyn PodPlacement>,
    resource_query: Arc<dyn LeaderResourceQuery>,
    supervisor: Arc<TaskSupervisor>,
    mutation_effects: Arc<dyn ResourceMutationEffectsPort>,
    wall_clock: Arc<dyn WallClock>,
}

pub struct SchedulerServiceDependencies {
    pub pod_query: Arc<dyn PodQuery>,
    pub persistence: Arc<dyn PodPersistence>,
    pub status_persistence: Arc<dyn PodStatusPersistence>,
    pub deletion: Arc<dyn PodDeleteOrchestration>,
    pub event_sink: Arc<dyn PodControlPlaneEventSink>,
    pub placement: Arc<dyn PodPlacement>,
    pub resource_query: Arc<dyn LeaderResourceQuery>,
    pub supervisor: Arc<TaskSupervisor>,
    pub mutation_effects: Arc<dyn ResourceMutationEffectsPort>,
    pub wall_clock: Arc<dyn WallClock>,
}

impl SchedulerService {
    pub fn new(dependencies: SchedulerServiceDependencies) -> Arc<Self> {
        let SchedulerServiceDependencies {
            pod_query,
            persistence,
            status_persistence,
            deletion,
            event_sink,
            placement,
            resource_query,
            supervisor,
            mutation_effects,
            wall_clock,
        } = dependencies;
        Arc::new(Self {
            pod_query,
            persistence,
            status_persistence,
            deletion,
            event_sink,
            placement,
            resource_query,
            supervisor,
            mutation_effects,
            wall_clock,
        })
    }

    async fn schedule_all_unbound_pods(&self) -> Result<(), PodRepositoryError> {
        let initial = self
            .pod_query
            .list_pods(pod_list_request(None, None, None)?)
            .await?;
        let candidates = sorted_unbound_pods(initial.into_parts().0);

        for wave in candidates.chunks(SCHED_BIND_CONCURRENCY) {
            let snapshot = self.scheduler_snapshot().await?;
            let mut reservations = Vec::new();
            let mut handles = Vec::with_capacity(wave.len());

            for pod in wave {
                let namespace = pod_namespace(pod);
                let name = pod.name.clone();
                let decision = schedule_pod_from_snapshot(
                    self.pod_query.as_ref(),
                    self.placement.as_ref(),
                    &snapshot,
                    &pod.data,
                    &namespace,
                    &name,
                    &reservations,
                )
                .await?;
                if let Some(node_name) = decision.node_name.as_deref() {
                    reservations.push(reserved_pod_body(pod, node_name));
                }

                let service = self.clone();
                let handle = self
                    .supervisor
                    .spawn_async(
                        TaskCategory::Background,
                        format!("scheduler_bind/{namespace}/{name}"),
                        async move {
                            service
                                .schedule_pending_pod_with_decision(&namespace, &name, decision)
                                .await
                        },
                    )
                    .await
                    .map_err(|error| PodRepositoryError::internal(error.to_string()))?;
                handles.push(handle);
            }

            for handle in handles {
                handle.join().await.map_err(|error| {
                    PodRepositoryError::internal(format!("scheduler bind task failed: {error}"))
                })??;
            }
        }

        Ok(())
    }

    async fn scheduler_snapshot(&self) -> Result<PodSchedulingView, PodRepositoryError> {
        let nodes = list_controller_resources(
            self.resource_query.as_ref(),
            "v1",
            "Node",
            ResourceListScope::Cluster,
        )
        .await?;
        let pods = self
            .pod_query
            .list_pods(pod_list_request(None, None, None)?)
            .await?;
        let namespaces = list_controller_resources(
            self.resource_query.as_ref(),
            "v1",
            "Namespace",
            ResourceListScope::Cluster,
        )
        .await?;
        let pdbs = list_controller_resources(
            self.resource_query.as_ref(),
            "policy/v1",
            "PodDisruptionBudget",
            ResourceListScope::AllNamespaces,
        )
        .await?;
        Ok(PodSchedulingView {
            nodes,
            pods: pods.into_parts().0,
            namespaces,
            pdbs,
        })
    }

    async fn schedule_pending_pod(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Resource>, PodRepositoryError> {
        let Some(current) = self
            .pod_query
            .get_pod(pod_get_request(namespace, name)?)
            .await?
        else {
            return Ok(None);
        };
        if current
            .data
            .pointer("/spec/nodeName")
            .and_then(Value::as_str)
            .is_some_and(|node_name| !node_name.is_empty())
        {
            return Ok(Some(current));
        }

        let decision = schedule_pod_on_available_nodes(
            self.pod_query.as_ref(),
            self.resource_query.as_ref(),
            self.placement.as_ref(),
            &current.data,
            namespace,
            name,
        )
        .await?;

        self.apply_scheduling_decision_to_pod(namespace, name, current, decision)
            .await
    }

    async fn schedule_pending_pod_with_decision(
        &self,
        namespace: &str,
        name: &str,
        decision: PodSchedulingDecision,
    ) -> Result<Option<Resource>, PodRepositoryError> {
        let Some(current) = self
            .pod_query
            .get_pod(pod_get_request(namespace, name)?)
            .await?
        else {
            return Ok(None);
        };
        if current
            .data
            .pointer("/spec/nodeName")
            .and_then(Value::as_str)
            .is_some_and(|node_name| !node_name.is_empty())
        {
            return Ok(Some(current));
        }
        self.apply_scheduling_decision_to_pod(namespace, name, current, decision)
            .await
    }

    async fn apply_scheduling_decision_to_pod(
        &self,
        namespace: &str,
        name: &str,
        current: Resource,
        mut decision: PodSchedulingDecision,
    ) -> Result<Option<Resource>, PodRepositoryError> {
        let transition_time =
            klights_cluster_core::k8s_time::format_legacy_timestamp(self.wall_clock.now_utc());
        if let Some(node_name) = decision.node_name.as_deref()
            && !self
                .planned_node_still_fits(namespace, name, &current.data, node_name)
                .await?
        {
            decision = PodSchedulingDecision {
                node_name: None,
                unschedulable_message: Some(
                    "node allocation changed before scheduler bind".to_string(),
                ),
                preemption_victims: Vec::new(),
            };
        }

        let mut body = Arc::unwrap_or_clone(current.data.clone());
        if let Some(spec) = body.get_mut("spec").and_then(Value::as_object_mut) {
            match decision.node_name.as_deref() {
                Some(node_name) => {
                    spec.insert("nodeName".to_string(), json!(node_name));
                }
                None => {
                    spec.remove("nodeName");
                }
            }
        }
        if let Some(status) = body.get_mut("status").and_then(Value::as_object_mut) {
            let conditions = status
                .entry("conditions".to_string())
                .or_insert_with(|| json!([]));
            if let Some(conditions) = conditions.as_array_mut() {
                conditions.retain(|condition| {
                    condition.get("type").and_then(Value::as_str) != Some("PodScheduled")
                });
                conditions.push(
                    if let Some(message) = decision.unschedulable_message.as_deref() {
                        json!({
                            "type": "PodScheduled",
                            "status": "False",
                            "lastTransitionTime": transition_time.clone(),
                            "reason": "Unschedulable",
                            "message": message,
                        })
                    } else {
                        json!({
                            "type": "PodScheduled",
                            "status": "True",
                            "lastTransitionTime": transition_time.clone(),
                        })
                    },
                );
            }
        }
        let desired_status = body.get("status").cloned();
        let spec_changed = body.get("spec") != current.data.get("spec");
        let status_changed = desired_status
            .as_ref()
            .is_some_and(|status| pod_scheduled_condition_changed(&current.data, status));

        let mut final_resource = if spec_changed && status_changed {
            self.persistence
                .replace_pod_including_status(PodPersistenceReplaceRequest {
                    namespace: namespace.to_string(),
                    name: name.to_string(),
                    body,
                    expected_resource_version: current.resource_version,
                })
                .await?
        } else if spec_changed {
            self.persistence
                .replace_pod(PodPersistenceReplaceRequest {
                    namespace: namespace.to_string(),
                    name: name.to_string(),
                    body,
                    expected_resource_version: current.resource_version,
                })
                .await?
        } else {
            current
        };
        if status_changed && !spec_changed {
            let status = desired_status.expect("status_changed requires desired status");
            final_resource = self
                .status_persistence
                .write_pod_status(PodStatusWriteRequest {
                    namespace: namespace.to_string(),
                    name: name.to_string(),
                    status,
                    expected_resource_version: Some(final_resource.resource_version),
                })
                .await?;
        }
        apply_pod_preemption(
            PodPreemptionContext {
                query: self.pod_query.as_ref(),
                persistence: self.persistence.as_ref(),
                deletion: self.deletion.as_ref(),
                mutation_effects: self.mutation_effects.as_ref(),
                preemptor_namespace: namespace,
                preemptor_name: name,
            },
            &decision.preemption_victims,
            &transition_time,
        )
        .await?;
        if status_changed
            && let Some(message) = decision.unschedulable_message.as_deref()
            && let Err(error) = self
                .event_sink
                .emit_pod_event(PodControlPlaneEventRequest {
                    pod: final_resource.data.clone(),
                    reason: SchedulerReason::FailedScheduling.as_str().to_string(),
                    message: message.to_string(),
                    event_type: "Warning".to_string(),
                    reporting_component: "default-scheduler".to_string(),
                    reporting_instance: decision.node_name.clone().unwrap_or_default(),
                })
                .await
        {
            tracing::warn!(
                namespace,
                name,
                %error,
                "failed to emit FailedScheduling event during scheduler retry"
            );
        }
        Ok(Some(final_resource))
    }

    async fn planned_node_still_fits(
        &self,
        namespace: &str,
        name: &str,
        pod: &Value,
        planned_node: &str,
    ) -> Result<bool, PodRepositoryError> {
        let live_decision = schedule_pod_on_available_nodes(
            self.pod_query.as_ref(),
            self.resource_query.as_ref(),
            self.placement.as_ref(),
            pod,
            namespace,
            name,
        )
        .await?;
        Ok(live_decision.node_name.as_deref() == Some(planned_node))
    }
}

impl PodScheduling for SchedulerService {
    fn schedule_all_unbound_pods(&self) -> PodSchedulingFuture<'_, ()> {
        Box::pin(SchedulerService::schedule_all_unbound_pods(self))
    }

    fn schedule_pending_pod(
        &self,
        namespace: String,
        name: String,
    ) -> PodSchedulingFuture<'_, Option<Resource>> {
        Box::pin(
            async move { SchedulerService::schedule_pending_pod(self, &namespace, &name).await },
        )
    }
}

fn pod_get_request(namespace: &str, name: &str) -> Result<PodGetRequest, PodRepositoryError> {
    PodGetRequest::try_by_name(namespace.to_string(), name.to_string())
}

fn pod_list_request(
    namespace: Option<&str>,
    label_selector: Option<&str>,
    field_selector: Option<&str>,
) -> Result<PodListRequest, PodRepositoryError> {
    PodListRequest::try_new(
        namespace.map(ToString::to_string),
        label_selector.map(ToString::to_string),
        field_selector.map(ToString::to_string),
        None,
        None,
    )
}

async fn list_controller_resources(
    query: &dyn LeaderResourceQuery,
    api_version: &str,
    kind: &str,
    scope: ResourceListScope,
) -> Result<Vec<Resource>, PodRepositoryError> {
    let request = ResourceListRequest::try_new(
        api_version,
        kind,
        scope,
        None,
        None,
        None,
        None,
        ResourceQueryConsistency::LeaderFresh,
    )
    .map_err(|error| PodRepositoryError::unavailable(error.to_string()))?;
    query
        .list_resources(request)
        .await
        .map(|list| list.into_items())
        .map_err(|error| PodRepositoryError::unavailable(error.to_string()))
}

fn pod_scheduled_condition_changed(current_pod: &Value, desired_status: &Value) -> bool {
    pod_scheduled_condition_signature(current_pod.get("status"))
        != pod_scheduled_condition_signature(Some(desired_status))
}

fn pod_scheduled_condition_signature(
    status: Option<&Value>,
) -> Option<(String, Option<String>, Option<String>)> {
    let condition = status
        .and_then(|status| status.get("conditions"))
        .and_then(Value::as_array)
        .and_then(|conditions| {
            conditions.iter().find(|condition| {
                condition.get("type").and_then(Value::as_str) == Some("PodScheduled")
            })
        })?;

    Some((
        condition_string_field(condition, "status")?,
        condition_string_field(condition, "reason"),
        condition_string_field(condition, "message"),
    ))
}

fn condition_string_field(condition: &Value, field: &str) -> Option<String> {
    condition
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn sorted_unbound_pods(pods: Vec<Resource>) -> Vec<Resource> {
    let mut pods: Vec<Resource> = pods
        .into_iter()
        .filter(|pod| {
            pod.data
                .pointer("/spec/nodeName")
                .and_then(Value::as_str)
                .is_none_or(str::is_empty)
        })
        .collect();
    pods.sort_by(compare_pod_scheduling_order);
    pods
}

fn compare_pod_scheduling_order(a: &Resource, b: &Resource) -> Ordering {
    pod_priority(&b.data)
        .cmp(&pod_priority(&a.data))
        .then_with(|| pod_creation_timestamp(&a.data).cmp(pod_creation_timestamp(&b.data)))
        .then_with(|| pod_namespace(a).cmp(&pod_namespace(b)))
        .then_with(|| a.name.cmp(&b.name))
}

fn pod_creation_timestamp(pod: &Value) -> &str {
    pod.pointer("/metadata/creationTimestamp")
        .and_then(Value::as_str)
        .unwrap_or("")
}

fn pod_namespace(pod: &Resource) -> String {
    pod.namespace
        .clone()
        .or_else(|| {
            pod.data
                .pointer("/metadata/namespace")
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
        .unwrap_or_else(|| "default".to_string())
}

fn reserved_pod_body(pod: &Resource, node_name: &str) -> Value {
    let mut body = Arc::unwrap_or_clone(pod.data.clone());
    if let Some(object) = body.as_object_mut() {
        let spec = object
            .entry("spec".to_string())
            .or_insert_with(|| json!({}));
        if let Some(spec) = spec.as_object_mut() {
            spec.insert("nodeName".to_string(), json!(node_name));
        }
    }
    body
}

struct PodSchedulingView {
    nodes: Vec<Resource>,
    pods: Vec<Resource>,
    namespaces: Vec<Resource>,
    pdbs: Vec<Resource>,
}

async fn schedule_pod_from_snapshot(
    query: &dyn PodQuery,
    placement: &dyn PodPlacement,
    snapshot: &PodSchedulingView,
    pod: &Value,
    namespace: &str,
    pod_name: &str,
    reservations: &[Value],
) -> Result<PodSchedulingDecision, PodRepositoryError> {
    let explicit_node_name = pod
        .pointer("/spec/nodeName")
        .and_then(Value::as_str)
        .filter(|node_name| !node_name.is_empty());
    if let Some(explicit_node_name) = explicit_node_name {
        return Ok(PodSchedulingDecision {
            node_name: Some(explicit_node_name.to_string()),
            unschedulable_message: None,
            preemption_victims: Vec::new(),
        });
    }

    let mut node_names: Vec<String> = snapshot
        .nodes
        .iter()
        .map(|node| node.name.clone())
        .collect();
    node_names.sort();

    let node_values: Vec<Arc<Value>> = snapshot
        .nodes
        .iter()
        .map(|node| node.data.clone())
        .collect();
    let namespace_values: Vec<Arc<Value>> = snapshot
        .namespaces
        .iter()
        .map(|namespace| namespace.data.clone())
        .collect();
    let pdb_values: Vec<Arc<Value>> = snapshot.pdbs.iter().map(|pdb| pdb.data.clone()).collect();
    let existing_per_node: Vec<(String, Vec<Arc<Value>>)> = node_names
        .iter()
        .map(|node_name| {
            let mut pods_on_node: Vec<Arc<Value>> = snapshot
                .pods
                .iter()
                .filter(|pod| {
                    pod_counts_toward_node_allocated(&pod.data, node_name, namespace, pod_name)
                })
                .map(|pod| pod.data.clone())
                .collect();
            pods_on_node.extend(
                reservations
                    .iter()
                    .filter(|pod| {
                        pod_counts_toward_node_allocated(pod, node_name, namespace, pod_name)
                    })
                    .cloned()
                    .map(Arc::new),
            );
            (node_name.clone(), pods_on_node)
        })
        .collect();

    let decision = placement.place_pod(klights_pod_api::PodPlacementRequest {
        nodes: node_values,
        incoming_pod: Arc::new(pod.clone()),
        existing_pods_by_node: existing_per_node,
        namespaces: namespace_values,
        disruption_budgets: pdb_values,
    })?;

    hydrate_preemption_victims(query, scheduling_decision_to_api(decision), pod).await
}

async fn schedule_pod_on_available_nodes(
    query: &dyn PodQuery,
    resources: &dyn LeaderResourceQuery,
    placement: &dyn PodPlacement,
    pod: &Value,
    namespace: &str,
    pod_name: &str,
) -> Result<PodSchedulingDecision, PodRepositoryError> {
    let explicit_node_name = pod
        .pointer("/spec/nodeName")
        .and_then(Value::as_str)
        .filter(|node_name| !node_name.is_empty());
    if let Some(explicit_node_name) = explicit_node_name {
        return Ok(PodSchedulingDecision {
            node_name: Some(explicit_node_name.to_string()),
            unschedulable_message: None,
            preemption_victims: Vec::new(),
        });
    }

    let nodes =
        list_controller_resources(resources, "v1", "Node", ResourceListScope::Cluster).await?;
    let namespaces =
        list_controller_resources(resources, "v1", "Namespace", ResourceListScope::Cluster).await?;
    let pdbs = list_controller_resources(
        resources,
        "policy/v1",
        "PodDisruptionBudget",
        ResourceListScope::AllNamespaces,
    )
    .await?;
    let all_pods = query.list_pods(pod_list_request(None, None, None)?).await?;

    let mut node_names: Vec<String> = nodes.iter().map(|node| node.name.clone()).collect();
    node_names.sort();
    let node_values: Vec<Arc<Value>> = nodes.iter().map(|node| node.data.clone()).collect();
    let namespace_values: Vec<Arc<Value>> = namespaces
        .iter()
        .map(|namespace| namespace.data.clone())
        .collect();
    let pdb_values: Vec<Arc<Value>> = pdbs.iter().map(|pdb| pdb.data.clone()).collect();
    let existing_per_node: Vec<(String, Vec<Arc<Value>>)> = node_names
        .iter()
        .map(|node_name| {
            let pods_on_node = all_pods
                .items()
                .iter()
                .filter(|pod| {
                    pod_counts_toward_node_allocated(&pod.data, node_name, namespace, pod_name)
                })
                .map(|pod| pod.data.clone())
                .collect();
            (node_name.clone(), pods_on_node)
        })
        .collect();

    let decision = placement.place_pod(klights_pod_api::PodPlacementRequest {
        nodes: node_values,
        incoming_pod: Arc::new(pod.clone()),
        existing_pods_by_node: existing_per_node,
        namespaces: namespace_values,
        disruption_budgets: pdb_values,
    })?;

    hydrate_preemption_victims(query, scheduling_decision_to_api(decision), pod).await
}

fn scheduling_decision_to_api(
    decision: klights_pod_api::PodPlacementDecision,
) -> PodSchedulingDecision {
    PodSchedulingDecision {
        node_name: decision.selected_node,
        unschedulable_message: decision.unschedulable_message,
        preemption_victims: decision
            .preemption_victims
            .iter()
            .map(|victim| {
                let mut parts = victim.splitn(2, '/');
                PodPreemptionCandidate {
                    namespace: parts.next().unwrap_or("").to_string(),
                    name: parts.next().unwrap_or("").to_string(),
                    resource_version: 0,
                    data: Value::Null,
                }
            })
            .collect(),
    }
}

async fn hydrate_preemption_victims(
    query: &dyn PodQuery,
    mut decision: PodSchedulingDecision,
    incoming_pod: &Value,
) -> Result<PodSchedulingDecision, PodRepositoryError> {
    if decision.preemption_victims.is_empty() {
        return Ok(decision);
    }
    let victim_keys: Vec<String> = decision
        .preemption_victims
        .iter()
        .map(|victim| format!("{}/{}", victim.namespace, victim.name))
        .collect();
    if let Some(node_name) = decision.node_name.as_deref() {
        decision.preemption_victims =
            collect_preemption_victims_with_data(query, node_name, incoming_pod, &victim_keys)
                .await?;
    }
    Ok(decision)
}

async fn collect_preemption_victims_with_data(
    query: &dyn PodQuery,
    node_name: &str,
    incoming: &Value,
    victim_names: &[String],
) -> Result<Vec<PodPreemptionCandidate>, PodRepositoryError> {
    let incoming_priority = pod_priority(incoming);
    let pods = query.list_pods(pod_list_request(None, None, None)?).await?;
    let mut victims = Vec::new();
    for resource in pods.into_parts().0 {
        if !pod_counts_toward_node_allocated(&resource.data, node_name, "", "")
            || pod_priority(&resource.data) >= incoming_priority
        {
            continue;
        }
        let namespace = resource
            .namespace
            .clone()
            .or_else(|| {
                resource
                    .data
                    .pointer("/metadata/namespace")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
            })
            .unwrap_or_else(|| "default".to_string());
        let key = format!("{namespace}/{}", resource.name);
        if victim_names.contains(&key) {
            victims.push(PodPreemptionCandidate {
                namespace,
                name: resource.name,
                resource_version: resource.resource_version,
                data: Arc::unwrap_or_clone(resource.data),
            });
        }
    }
    Ok(victims)
}

fn pod_counts_toward_node_allocated(
    pod: &Value,
    node_name: &str,
    pending_namespace: &str,
    pending_name: &str,
) -> bool {
    if pod
        .pointer("/metadata/deletionTimestamp")
        .and_then(Value::as_str)
        .is_some()
        || pod.pointer("/spec/nodeName").and_then(Value::as_str) != Some(node_name)
    {
        return false;
    }
    let namespace = pod
        .pointer("/metadata/namespace")
        .and_then(Value::as_str)
        .unwrap_or("default");
    let name = pod.pointer("/metadata/name").and_then(Value::as_str);
    if namespace == pending_namespace && name == Some(pending_name) {
        return false;
    }
    !matches!(
        pod.pointer("/status/phase").and_then(Value::as_str),
        Some("Succeeded" | "Failed")
    )
}

async fn apply_pod_preemption(
    context: PodPreemptionContext<'_>,
    victims: &[PodPreemptionCandidate],
    transition_time: &str,
) -> Result<(), PodRepositoryError> {
    for victim in victims {
        let updated = mark_preemption_candidate(
            context.query,
            context.persistence,
            victim,
            context.preemptor_namespace,
            context.preemptor_name,
            transition_time,
        )
        .await?;
        let uid = updated.uid.clone();
        let hook_resource = Arc::unwrap_or_clone(updated.data);
        context
            .deletion
            .enqueue_marked_retry(klights_pod_api::PodMarkedRetryRequest {
                namespace: victim.namespace.clone(),
                name: victim.name.clone(),
                uid,
                run_after: Duration::ZERO,
                pod_data: hook_resource.clone(),
            })
            .await?;
        context
            .mutation_effects
            .dispatch_resource_mutation_effects(ResourceMutationEffectsRequest::new(
                ResourceChange::Updated,
                &hook_resource,
                Some(&victim.data),
                "pod_preemption_victim",
            ))
            .await;
    }
    Ok(())
}

async fn mark_preemption_candidate(
    query: &dyn PodQuery,
    persistence: &dyn PodPersistence,
    victim: &PodPreemptionCandidate,
    preemptor_namespace: &str,
    preemptor_name: &str,
    transition_time: &str,
) -> Result<Resource, PodRepositoryError> {
    const MAX_RETRIES: u32 = 5;
    let mut resource_version = victim.resource_version;
    let mut data = victim.data.clone();

    for attempt in 0..MAX_RETRIES {
        mark_pod_preempted_metadata(&mut data, transition_time);
        let mut status =
            preempted_status(&data, preemptor_namespace, preemptor_name, transition_time);
        klights_types::merge_pod_status_for_update(
            "v1",
            "Pod",
            &data,
            &mut status,
            klights_types::PodStatusOwner::Scheduler,
        );
        if let Some(object) = data.as_object_mut() {
            object.insert("status".to_string(), status);
        }
        match persistence
            .replace_pod_including_status(PodPersistenceReplaceRequest {
                namespace: victim.namespace.clone(),
                name: victim.name.clone(),
                body: data,
                expected_resource_version: resource_version,
            })
            .await
        {
            Ok(updated) => return Ok(updated),
            Err(error)
                if attempt + 1 < MAX_RETRIES
                    && matches!(error, PodRepositoryError::Conflict { .. }) =>
            {
                let current = query
                    .get_pod(pod_get_request(&victim.namespace, &victim.name)?)
                    .await?
                    .ok_or_else(|| {
                        PodRepositoryError::not_found(&victim.namespace, &victim.name)
                    })?;
                resource_version = current.resource_version;
                data = Arc::unwrap_or_clone(current.data);
            }
            Err(error) => return Err(error),
        }
    }

    unreachable!("preemption termination retry loop exhausted without returning")
}

fn mark_pod_preempted_metadata(data: &mut Value, transition_time: &str) {
    if let Some(metadata) = data.get_mut("metadata").and_then(Value::as_object_mut) {
        metadata
            .entry("deletionTimestamp".to_string())
            .or_insert_with(|| json!(transition_time));
        metadata
            .entry("deletionGracePeriodSeconds".to_string())
            .or_insert_with(|| json!(0));
    }
}

fn preempted_status(
    data: &Value,
    preemptor_namespace: &str,
    preemptor_name: &str,
    transition_time: &str,
) -> Value {
    let mut status = data.get("status").cloned().unwrap_or_else(|| json!({}));
    if !status.is_object() {
        status = json!({});
    }
    let has_bound_node = data
        .pointer("/spec/nodeName")
        .and_then(Value::as_str)
        .is_some_and(|node_name| !node_name.is_empty());
    let condition = json!({
        "type": "DisruptionTarget",
        "status": "True",
        "lastTransitionTime": transition_time,
        "reason": SchedulerReason::PreemptionByScheduler.as_str(),
        "message": format!(
            "Preempted by pod {preemptor_namespace}/{preemptor_name} on node"
        ),
    });
    if let Some(status) = status.as_object_mut() {
        let conditions = status
            .entry("conditions".to_string())
            .or_insert_with(|| json!([]));
        if let Some(conditions) = conditions.as_array_mut() {
            let pod_scheduled_true = if has_bound_node {
                conditions
                    .iter()
                    .find(|existing| {
                        existing.get("type").and_then(Value::as_str) == Some("PodScheduled")
                            && existing.get("status").and_then(Value::as_str) == Some("True")
                    })
                    .cloned()
                    .or_else(|| {
                        Some(json!({
                            "type": "PodScheduled",
                            "status": "True",
                            "lastTransitionTime": transition_time,
                        }))
                    })
            } else {
                None
            };
            conditions.retain(|existing| {
                let condition_type = existing.get("type").and_then(Value::as_str);
                condition_type != Some("DisruptionTarget")
                    && (!has_bound_node || condition_type != Some("PodScheduled"))
            });
            if let Some(pod_scheduled_true) = pod_scheduled_true {
                conditions.push(pod_scheduled_true);
            }
            conditions.push(condition);
        }
    }
    status
}

fn pod_priority(pod: &Value) -> i64 {
    pod.pointer("/spec/priority")
        .and_then(Value::as_i64)
        .unwrap_or(0)
}
